use std::fmt;

use async_trait::async_trait;
use thiserror::Error;

use crate::{CheckpointEnvelope, ExecutionId};

/// Opaque compare-and-swap revision for one execution's checkpoint.
///
/// Providers may use values such as Kubernetes `resourceVersion`. The token
/// supports equality but deliberately exposes no ordering or increment API.
///
/// ```compile_fail
/// use kuberic_dex::StorageRevision;
///
/// let earlier = StorageRevision::new("provider-token-a").unwrap();
/// let later = StorageRevision::new("provider-token-b").unwrap();
/// let _ordered = earlier < later;
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageRevision(String);

impl StorageRevision {
    /// Construct a revision from a provider-owned opaque token.
    pub fn new(value: impl Into<String>) -> Result<Self, StoreError> {
        let value = value.into();
        if value.is_empty() {
            return Err(StoreError::new(
                StoreErrorKind::MalformedResponse,
                "checkpoint storage returned an empty revision token",
            ));
        }
        Ok(Self(value))
    }

    /// Borrow the token for provider request round-tripping.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A checkpoint and the storage revision at which it was loaded.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredCheckpoint {
    revision: StorageRevision,
    checkpoint: CheckpointEnvelope,
}

impl StoredCheckpoint {
    pub const fn new(revision: StorageRevision, checkpoint: CheckpointEnvelope) -> Self {
        Self {
            revision,
            checkpoint,
        }
    }

    pub const fn revision(&self) -> &StorageRevision {
        &self.revision
    }

    pub const fn checkpoint(&self) -> &CheckpointEnvelope {
        &self.checkpoint
    }
}

/// Portable checkpoint-provider failure classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreErrorKind {
    Authorization,
    Unavailable,
    Timeout,
    MalformedResponse,
    Other,
}

impl fmt::Display for StoreErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Authorization => "authorization",
            Self::Unavailable => "unavailable or transport",
            Self::Timeout => "timeout",
            Self::MalformedResponse => "malformed response",
            Self::Other => "other",
        })
    }
}

/// Provider-neutral error retaining a useful provider description.
///
/// Provider-specific error types are intentionally not exposed through this
/// API. Implementations should preserve their useful message or source
/// description in `description`.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("checkpoint storage {kind}: {description}")]
pub struct StoreError {
    kind: StoreErrorKind,
    description: String,
}

impl StoreError {
    pub fn new(kind: StoreErrorKind, description: impl Into<String>) -> Self {
        Self {
            kind,
            description: description.into(),
        }
    }

    pub const fn kind(&self) -> StoreErrorKind {
        self.kind
    }

    pub fn description(&self) -> &str {
        &self.description
    }
}

/// Result of one atomic checkpoint compare-and-swap.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CasOutcome {
    Accepted(StorageRevision),
    Conflict,
    OutcomeUnknown,
}

/// Runtime-neutral asynchronous persistence boundary used by the host.
///
/// A provider must return [`CasOutcome::OutcomeUnknown`] whenever it cannot
/// prove whether a CAS was accepted. `StoreError` is reserved for definitely
/// local or pre-request failures that could not have applied the mutation.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn load(&self, execution_id: ExecutionId)
    -> Result<Option<StoredCheckpoint>, StoreError>;

    async fn compare_and_swap(
        &self,
        execution_id: ExecutionId,
        expected: Option<StorageRevision>,
        checkpoint: CheckpointEnvelope,
    ) -> Result<CasOutcome, StoreError>;
}
