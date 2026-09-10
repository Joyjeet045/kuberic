//! Optional Kubernetes-backed checkpoint persistence.
//!
//! One execution is stored as one namespaced `ConfigMap`. Confirmed-write
//! measurements count the canonical JSON bytes of the `CheckpointEnvelope` and
//! the canonical `serde_json` bytes of the typed, server-returned `ConfigMap`.
//! The latter includes server-assigned metadata and is not a raw HTTP-wire
//! measurement. Kubernetes watch traffic is intentionally outside the
//! `CheckpointStore` contract; validation measures each delivered typed
//! `WatchEvent<ConfigMap>` by canonical `serde_json` reserialization, excluding
//! HTTP framing and transport overhead.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use k8s_openapi::{
    api::core::v1::ConfigMap,
    apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference},
};
use kube::{Api, Client, Error, api::PostParams};

use crate::{
    CasOutcome, CheckpointEnvelope, CheckpointStore, ExecutionId, StorageRevision, StoreError,
    StoreErrorKind, StoredCheckpoint,
};

const CHECKPOINT_DATA_KEY: &str = "checkpoint.json";
const CHECKPOINT_NAME_PREFIX: &str = "kuberic-checkpoint-";

/// Default aggregate UTF-8 byte budget for ConfigMap data keys and values.
pub const DEFAULT_CONFIG_MAP_DATA_BUDGET_BYTES: u64 = 768 * 1024;

/// Largest configurable data budget, retaining 64 KiB below the 1 MiB ceiling.
pub const MAX_CONFIG_MAP_DATA_BUDGET_BYTES: u64 = 960 * 1024;

/// Caller assertion about the scope of a checkpoint owner.
///
/// Kubernetes does not serialize owner namespace or scope in an
/// [`OwnerReference`], so the provider needs this assertion to reject known
/// cross-namespace references before dispatch. The caller remains responsible
/// for asserting the actual scope of the referenced resource.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KubernetesCheckpointOwnerScope {
    /// A namespaced owner, which must share the checkpoint namespace.
    Namespaced(String),
    /// A cluster-scoped owner, which may own a namespaced checkpoint.
    ClusterScoped,
}

/// Optional Kubernetes garbage-collection owner for checkpoint ConfigMaps.
///
/// The provider accepts only non-controlling, non-blocking references. It does
/// not fetch the owner to prove its existence or scope.
#[derive(Clone, Debug, PartialEq)]
pub struct KubernetesCheckpointOwner {
    reference: OwnerReference,
    scope: KubernetesCheckpointOwnerScope,
}

impl KubernetesCheckpointOwner {
    /// Pair an owner reference with the caller's scope assertion.
    pub const fn new(reference: OwnerReference, scope: KubernetesCheckpointOwnerScope) -> Self {
        Self { reference, scope }
    }

    /// Borrow the owner reference emitted on checkpoint objects.
    pub const fn reference(&self) -> &OwnerReference {
        &self.reference
    }

    /// Borrow the caller-supplied owner scope assertion.
    pub const fn scope(&self) -> &KubernetesCheckpointOwnerScope {
        &self.scope
    }
}

/// Construction options for a Kubernetes checkpoint store.
#[derive(Clone, Debug, PartialEq)]
pub struct KubernetesCheckpointStoreOptions {
    data_budget_bytes: u64,
    owner: Option<KubernetesCheckpointOwner>,
}

impl Default for KubernetesCheckpointStoreOptions {
    fn default() -> Self {
        Self {
            data_budget_bytes: DEFAULT_CONFIG_MAP_DATA_BUDGET_BYTES,
            owner: None,
        }
    }
}

impl KubernetesCheckpointStoreOptions {
    /// Set the aggregate ConfigMap data-key and data-value budget in bytes.
    pub const fn with_data_budget_bytes(mut self, data_budget_bytes: u64) -> Self {
        self.data_budget_bytes = data_budget_bytes;
        self
    }

    /// Attach one optional, non-controlling garbage-collection owner.
    pub fn with_owner(mut self, owner: KubernetesCheckpointOwner) -> Self {
        self.owner = Some(owner);
        self
    }

    /// Return the configured aggregate ConfigMap data budget.
    pub const fn data_budget_bytes(&self) -> u64 {
        self.data_budget_bytes
    }

    /// Return the optional checkpoint owner.
    pub const fn owner(&self) -> Option<&KubernetesCheckpointOwner> {
        self.owner.as_ref()
    }
}

#[derive(Debug, Default)]
struct Metrics {
    accepted_writes: AtomicU64,
    checkpoint_bytes: AtomicU64,
    object_bytes: AtomicU64,
    measurement_failures: AtomicU64,
}

/// Clone-shared measurements recorded by a Kubernetes checkpoint store.
#[derive(Clone, Debug, Default)]
pub struct KubernetesCheckpointMetrics {
    inner: Arc<Metrics>,
}

impl KubernetesCheckpointMetrics {
    /// Return a point-in-time snapshot of cumulative provider measurements.
    pub fn snapshot(&self) -> KubernetesCheckpointMetricsSnapshot {
        KubernetesCheckpointMetricsSnapshot {
            accepted_writes: self.inner.accepted_writes.load(Ordering::Relaxed),
            checkpoint_bytes: self.inner.checkpoint_bytes.load(Ordering::Relaxed),
            object_bytes: self.inner.object_bytes.load(Ordering::Relaxed),
            measurement_failures: self.inner.measurement_failures.load(Ordering::Relaxed),
        }
    }

    fn record_accepted(&self, checkpoint_bytes: usize, object: &ConfigMap) {
        self.inner.accepted_writes.fetch_add(1, Ordering::Relaxed);
        self.inner
            .checkpoint_bytes
            .fetch_add(checkpoint_bytes as u64, Ordering::Relaxed);
        match serde_json::to_vec(object) {
            Ok(encoded) => {
                self.inner
                    .object_bytes
                    .fetch_add(encoded.len() as u64, Ordering::Relaxed);
            }
            Err(_) => {
                self.inner
                    .measurement_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Cumulative measurements for writes whose acceptance the provider proved.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct KubernetesCheckpointMetricsSnapshot {
    accepted_writes: u64,
    checkpoint_bytes: u64,
    object_bytes: u64,
    measurement_failures: u64,
}

impl KubernetesCheckpointMetricsSnapshot {
    pub const fn accepted_writes(self) -> u64 {
        self.accepted_writes
    }

    pub const fn checkpoint_bytes(self) -> u64 {
        self.checkpoint_bytes
    }

    pub const fn object_bytes(self) -> u64 {
        self.object_bytes
    }

    pub const fn measurement_failures(self) -> u64 {
        self.measurement_failures
    }
}

/// A namespaced ConfigMap-backed implementation of the portable checkpoint
/// store contract.
#[derive(Clone, Debug)]
pub struct KubernetesCheckpointStore {
    api: Api<ConfigMap>,
    namespace: String,
    data_budget_bytes: usize,
    owner_reference: Option<OwnerReference>,
    metrics: KubernetesCheckpointMetrics,
}

impl KubernetesCheckpointStore {
    /// Construct a store from a caller-owned Kubernetes client and namespace.
    pub fn new(client: Client, namespace: impl Into<String>) -> Result<Self, StoreError> {
        Self::with_options(
            client,
            namespace,
            KubernetesCheckpointStoreOptions::default(),
        )
    }

    /// Construct a store with an explicit data budget and optional owner.
    pub fn with_options(
        client: Client,
        namespace: impl Into<String>,
        options: KubernetesCheckpointStoreOptions,
    ) -> Result<Self, StoreError> {
        let namespace = namespace.into();
        validate_namespace(&namespace)?;
        let data_budget_bytes = validate_data_budget(options.data_budget_bytes)?;
        let owner_reference = options
            .owner
            .map(|owner| validate_owner(&namespace, owner))
            .transpose()?;
        Ok(Self {
            api: Api::namespaced(client, &namespace),
            namespace,
            data_budget_bytes,
            owner_reference,
            metrics: KubernetesCheckpointMetrics::default(),
        })
    }

    /// Return the clone-shared provider measurements.
    pub fn metrics(&self) -> KubernetesCheckpointMetrics {
        self.metrics.clone()
    }

    /// Return the deterministic ConfigMap name for an execution.
    pub fn object_name(execution_id: ExecutionId) -> String {
        let mut name = String::with_capacity(CHECKPOINT_NAME_PREFIX.len() + 32);
        name.push_str(CHECKPOINT_NAME_PREFIX);
        for byte in execution_id.as_bytes() {
            use std::fmt::Write as _;
            write!(&mut name, "{byte:02x}").expect("writing to a String cannot fail");
        }
        name
    }

    fn object(
        &self,
        execution_id: ExecutionId,
        expected: Option<&StorageRevision>,
        checkpoint_json: String,
    ) -> ConfigMap {
        let mut labels = BTreeMap::new();
        labels.insert(
            "kuberic.io/component".to_string(),
            "durable-checkpoint".to_string(),
        );
        let mut data = BTreeMap::new();
        data.insert(CHECKPOINT_DATA_KEY.to_string(), checkpoint_json);
        ConfigMap {
            metadata: ObjectMeta {
                name: Some(Self::object_name(execution_id)),
                namespace: Some(self.namespace.clone()),
                resource_version: expected.map(|revision| revision.as_str().to_string()),
                labels: Some(labels),
                owner_references: self.owner_reference.clone().map(|owner| vec![owner]),
                ..ObjectMeta::default()
            },
            data: Some(data),
            ..ConfigMap::default()
        }
    }
}

#[async_trait]
impl CheckpointStore for KubernetesCheckpointStore {
    async fn load(
        &self,
        execution_id: ExecutionId,
    ) -> Result<Option<StoredCheckpoint>, StoreError> {
        let name = Self::object_name(execution_id);
        let object = self
            .api
            .get_opt(&name)
            .await
            .map_err(|error| load_error("load", error))?;
        object
            .map(|object| {
                if !owner_relationship_matches(
                    self.owner_reference.as_ref(),
                    object.metadata.owner_references.as_deref(),
                ) {
                    return Err(StoreError::new(
                        StoreErrorKind::MalformedResponse,
                        "load rejected a checkpoint with a different owner relationship",
                    ));
                }
                decode_object(object)
            })
            .transpose()
    }

    async fn compare_and_swap(
        &self,
        execution_id: ExecutionId,
        expected: Option<StorageRevision>,
        checkpoint: CheckpointEnvelope,
    ) -> Result<CasOutcome, StoreError> {
        let checkpoint_json = serde_json::to_string(&checkpoint).map_err(|error| {
            StoreError::new(
                StoreErrorKind::MalformedResponse,
                format!("compare-and-swap request serialization failed: {error}"),
            )
        })?;
        let config_map_data_bytes = CHECKPOINT_DATA_KEY
            .len()
            .checked_add(checkpoint_json.len())
            .ok_or_else(|| {
                StoreError::new(
                    StoreErrorKind::Other,
                    "compare-and-swap ConfigMap data size overflowed",
                )
            })?;
        if config_map_data_bytes > self.data_budget_bytes {
            return Err(StoreError::new(
                StoreErrorKind::Other,
                format!(
                    "compare-and-swap ConfigMap data is {config_map_data_bytes} bytes, exceeding the configured ConfigMap data budget of {} bytes",
                    self.data_budget_bytes
                ),
            ));
        }

        let name = Self::object_name(execution_id);
        if let Some(expected_revision) = expected.as_ref() {
            let existing = self
                .api
                .get_opt(&name)
                .await
                .map_err(|error| load_error("compare-and-swap ownership preflight", error))?;
            let Some(existing) = existing else {
                return Ok(CasOutcome::Conflict);
            };
            let Some(existing_revision) = response_revision(&existing) else {
                return Err(StoreError::new(
                    StoreErrorKind::MalformedResponse,
                    "compare-and-swap ownership preflight returned a ConfigMap without a usable resourceVersion",
                ));
            };
            if &existing_revision != expected_revision {
                return Ok(CasOutcome::Conflict);
            }
            if !owner_relationship_matches(
                self.owner_reference.as_ref(),
                existing.metadata.owner_references.as_deref(),
            ) {
                return Err(StoreError::new(
                    StoreErrorKind::Other,
                    "compare-and-swap refused to change the checkpoint owner relationship",
                ));
            }
        }

        let checkpoint_bytes = checkpoint_json.len();
        let object = self.object(execution_id, expected.as_ref(), checkpoint_json);
        let result = if expected.is_some() {
            self.api
                .replace(&name, &PostParams::default(), &object)
                .await
        } else {
            self.api.create(&PostParams::default(), &object).await
        };

        match result {
            Ok(object) => {
                let Some(revision) = response_revision(&object) else {
                    return Ok(CasOutcome::OutcomeUnknown);
                };
                self.metrics.record_accepted(checkpoint_bytes, &object);
                Ok(CasOutcome::Accepted(revision))
            }
            Err(error) => classify_mutation_error(expected.is_some(), error),
        }
    }
}

fn owner_relationship_matches(
    configured: Option<&OwnerReference>,
    persisted: Option<&[OwnerReference]>,
) -> bool {
    match (configured, persisted.unwrap_or_default()) {
        (None, []) => true,
        (Some(configured), [persisted]) => {
            configured.api_version == persisted.api_version
                && configured.kind == persisted.kind
                && configured.name == persisted.name
                && configured.uid == persisted.uid
                && configured.controller.unwrap_or(false) == persisted.controller.unwrap_or(false)
                && configured.block_owner_deletion.unwrap_or(false)
                    == persisted.block_owner_deletion.unwrap_or(false)
        }
        _ => false,
    }
}

fn validate_data_budget(data_budget_bytes: u64) -> Result<usize, StoreError> {
    if !(1..=MAX_CONFIG_MAP_DATA_BUDGET_BYTES).contains(&data_budget_bytes) {
        return Err(StoreError::new(
            StoreErrorKind::Other,
            format!(
                "Kubernetes checkpoint ConfigMap data budget must be between 1 and {MAX_CONFIG_MAP_DATA_BUDGET_BYTES} bytes inclusive; received {data_budget_bytes}"
            ),
        ));
    }
    usize::try_from(data_budget_bytes).map_err(|_| {
        StoreError::new(
            StoreErrorKind::Other,
            format!(
                "Kubernetes checkpoint ConfigMap data budget {data_budget_bytes} cannot be represented on this platform"
            ),
        )
    })
}

fn validate_owner(
    checkpoint_namespace: &str,
    owner: KubernetesCheckpointOwner,
) -> Result<OwnerReference, StoreError> {
    for (field, value) in [
        ("apiVersion", owner.reference.api_version.as_str()),
        ("kind", owner.reference.kind.as_str()),
        ("name", owner.reference.name.as_str()),
        ("uid", owner.reference.uid.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(StoreError::new(
                StoreErrorKind::Other,
                format!("Kubernetes checkpoint owner {field} must be nonempty"),
            ));
        }
    }
    if owner.reference.controller == Some(true) {
        return Err(StoreError::new(
            StoreErrorKind::Other,
            "Kubernetes checkpoint owner controller must be absent or false; the checkpoint provider does not take controller ownership",
        ));
    }
    if owner.reference.block_owner_deletion == Some(true) {
        return Err(StoreError::new(
            StoreErrorKind::Other,
            "Kubernetes checkpoint owner blockOwnerDeletion must be absent or false; the checkpoint provider does not require delete permission on the owner",
        ));
    }
    if let KubernetesCheckpointOwnerScope::Namespaced(owner_namespace) = &owner.scope {
        validate_namespace(owner_namespace).map_err(|_| {
            StoreError::new(
                StoreErrorKind::Other,
                "Kubernetes checkpoint owner namespace must be a nonempty DNS label of at most 63 characters",
            )
        })?;
        if owner_namespace != checkpoint_namespace {
            return Err(StoreError::new(
                StoreErrorKind::Other,
                format!(
                    "Kubernetes checkpoint owner namespace {owner_namespace:?} must match checkpoint namespace {checkpoint_namespace:?}"
                ),
            ));
        }
    }
    Ok(owner.reference)
}

fn validate_namespace(namespace: &str) -> Result<(), StoreError> {
    let valid_length = !namespace.is_empty() && namespace.len() <= 63;
    let valid_bytes = namespace.bytes().enumerate().all(|(index, byte)| {
        byte.is_ascii_lowercase()
            || byte.is_ascii_digit()
            || (byte == b'-' && index > 0 && index + 1 < namespace.len())
    });
    if valid_length && valid_bytes {
        Ok(())
    } else {
        Err(StoreError::new(
            StoreErrorKind::Other,
            "Kubernetes checkpoint namespace must be a nonempty DNS label of at most 63 characters",
        ))
    }
}

fn decode_object(object: ConfigMap) -> Result<StoredCheckpoint, StoreError> {
    let revision = response_revision(&object).ok_or_else(|| {
        StoreError::new(
            StoreErrorKind::MalformedResponse,
            "load response omitted a nonempty metadata.resourceVersion",
        )
    })?;
    let checkpoint_json = object
        .data
        .as_ref()
        .and_then(|data| data.get(CHECKPOINT_DATA_KEY))
        .ok_or_else(|| {
            StoreError::new(
                StoreErrorKind::MalformedResponse,
                "load response omitted data[checkpoint.json]",
            )
        })?;
    let checkpoint = serde_json::from_str(checkpoint_json).map_err(|error| {
        StoreError::new(
            StoreErrorKind::MalformedResponse,
            format!("load response contained invalid checkpoint JSON: {error}"),
        )
    })?;
    Ok(StoredCheckpoint::new(revision, checkpoint))
}

fn response_revision(object: &ConfigMap) -> Option<StorageRevision> {
    object
        .metadata
        .resource_version
        .as_ref()
        .and_then(|revision| StorageRevision::new(revision.clone()).ok())
}

fn classify_mutation_error(replacing: bool, error: Error) -> Result<CasOutcome, StoreError> {
    match error {
        Error::Api(status)
            if status.is_conflict() || (!replacing && status.is_already_exists()) =>
        {
            Ok(CasOutcome::Conflict)
        }
        Error::Api(status) if replacing && status.is_not_found() => Ok(CasOutcome::Conflict),
        Error::Api(status) if status.code >= 500 || status.code == 408 => {
            Ok(CasOutcome::OutcomeUnknown)
        }
        Error::Api(status) => Err(api_status_error("compare-and-swap", &status)),
        Error::Auth(_) => Err(StoreError::new(
            StoreErrorKind::Authorization,
            "compare-and-swap authentication failed before dispatch",
        )),
        Error::BuildRequest(_) => Err(StoreError::new(
            StoreErrorKind::Other,
            "compare-and-swap request construction failed before dispatch",
        )),
        Error::HttpError(_) => Err(StoreError::new(
            StoreErrorKind::Other,
            "compare-and-swap HTTP request construction failed before dispatch",
        )),
        _ => Ok(CasOutcome::OutcomeUnknown),
    }
}

fn load_error(operation: &str, error: Error) -> StoreError {
    match error {
        Error::Api(status) => api_status_error(operation, &status),
        Error::Auth(_) => StoreError::new(
            StoreErrorKind::Authorization,
            format!("{operation} authentication failed"),
        ),
        Error::SerdeError(error) => StoreError::new(
            StoreErrorKind::MalformedResponse,
            format!("{operation} response decoding failed: {error}"),
        ),
        Error::FromUtf8(error) => StoreError::new(
            StoreErrorKind::MalformedResponse,
            format!("{operation} response was not UTF-8: {error}"),
        ),
        Error::BuildRequest(_) => StoreError::new(
            StoreErrorKind::Other,
            format!("{operation} request construction failed"),
        ),
        _ => StoreError::new(
            StoreErrorKind::Unavailable,
            format!("{operation} Kubernetes request was unavailable or failed in transport"),
        ),
    }
}

fn api_status_error(operation: &str, status: &kube::error::Status) -> StoreError {
    let kind = match status.code {
        401 | 403 => StoreErrorKind::Authorization,
        408 | 504 => StoreErrorKind::Timeout,
        500..=599 | 429 => StoreErrorKind::Unavailable,
        _ => StoreErrorKind::Other,
    };
    StoreError::new(
        kind,
        format!(
            "{operation} Kubernetes API rejected the request: code={}, reason={}",
            status.code, status.reason
        ),
    )
}
