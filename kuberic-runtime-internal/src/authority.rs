use async_trait::async_trait;
use bytes::Bytes;
pub use kuberic_protocol::types::{BuildAuthority, BuildAuthorityKind};
use kuberic_protocol::types::{
    ConfigurationDescriptor, ConfigurationId, EffectivePolicy, Epoch, OperationId, ReplicaIdentity,
    ReplicaRole, TransitionKind,
};
use kuberic_protocol::validation::{validate_configuration, validate_transition_relationship};
use serde::{Deserialize, Serialize};

use crate::transport::{CopyItem, ReplicationAck, ReplicationItem};
use crate::{ContractError, Result};

pub fn validate_build_envelope(authority: &BuildAuthority, envelope: &CopyItem) -> Result<()> {
    authority
        .validate()
        .map_err(|error| ContractError::AuthorityMismatch(error.to_string()))?;
    if envelope.build_id != authority.build_id
        || envelope.sender != authority.source
        || envelope.receiver != authority.target
        || envelope.epoch != authority.current_configuration.epoch
        || envelope.current_configuration_id != authority.current_configuration.configuration_id
        || envelope.replication_boundary_lsn != authority.replication_boundary_lsn
    {
        return Err(ContractError::AuthorityMismatch(
            "copy item does not match durable build authority".to_string(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AuthorityFence {
    pub epoch: Epoch,
    pub previous_configuration_id: Option<ConfigurationId>,
    pub current_configuration_id: ConfigurationId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationProgress {
    pub fence: AuthorityFence,
    pub verified_lsn: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableBuildProgress {
    pub authority: BuildAuthority,
    pub last_sequence: u64,
    pub durable_lsn: i64,
    pub completed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LocalWritePhase {
    Reserved,
    Registered,
    Committed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableLocalWrite {
    pub operation_id: OperationId,
    pub lsn: i64,
    #[serde(default)]
    pub committed_lsn: i64,
    pub data: Bytes,
    pub phase: LocalWritePhase,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmittedAuthority {
    pub local_identity: ReplicaIdentity,
    pub transition_kind: Option<TransitionKind>,
    pub previous_configuration: Option<ConfigurationDescriptor>,
    pub current_configuration: ConfigurationDescriptor,
}

impl AdmittedAuthority {
    pub fn is_current_only_completion_of(&self, existing: &Self) -> bool {
        self.local_identity == existing.local_identity
            && self.current_configuration == existing.current_configuration
            && existing.previous_configuration.is_some()
            && self.previous_configuration.is_none()
            && self.transition_kind.is_none()
    }

    pub fn fence(&self) -> AuthorityFence {
        AuthorityFence {
            epoch: self.current_configuration.epoch,
            previous_configuration_id: self
                .previous_configuration
                .as_ref()
                .map(|configuration| configuration.configuration_id.clone()),
            current_configuration_id: self.current_configuration.configuration_id.clone(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        validate_configuration(&self.current_configuration, None)
            .map_err(|error| ContractError::AuthorityMismatch(error.to_string()))?;
        let policy = EffectivePolicy::fixed(self.current_configuration.members.len() as u32, 0)
            .ok_or_else(|| {
                ContractError::AuthorityMismatch("configuration must contain members".to_string())
            })?;
        match (self.previous_configuration.as_ref(), self.transition_kind) {
            (
                Some(previous),
                Some(kind @ (TransitionKind::Replacement | TransitionKind::Failover)),
            ) => {
                validate_transition_relationship(
                    kind,
                    Some(previous),
                    &self.current_configuration,
                    &policy,
                )
                .map_err(|error| ContractError::AuthorityMismatch(error.to_string()))?;
            }
            (None, Some(TransitionKind::Bootstrap)) | (None, None) => {}
            (Some(_), _) => {
                return Err(ContractError::AuthorityMismatch(
                    "Previous Configuration requires replacement or failover authority".to_string(),
                ));
            }
            (None, Some(_)) => {
                return Err(ContractError::AuthorityMismatch(
                    "non-bootstrap transition requires a Previous Configuration".to_string(),
                ));
            }
        }
        if !self.contains_member(&self.local_identity) {
            return Err(ContractError::AuthorityMismatch(
                "local identity is outside admitted authority".to_string(),
            ));
        }
        Ok(())
    }

    pub fn primary_identity(&self) -> &ReplicaIdentity {
        &self
            .current_configuration
            .members
            .iter()
            .find(|member| {
                member.identity.replica_id == self.current_configuration.primary_id
                    && member.role == ReplicaRole::Primary
            })
            .expect("validated configuration has one primary")
            .identity
    }

    pub fn contains_member(&self, identity: &ReplicaIdentity) -> bool {
        self.current_configuration
            .members
            .iter()
            .any(|member| &member.identity == identity)
            || self
                .previous_configuration
                .as_ref()
                .is_some_and(|previous| {
                    previous
                        .members
                        .iter()
                        .any(|member| &member.identity == identity)
                })
    }

    pub fn local_role(&self) -> ReplicaRole {
        self.current_configuration
            .members
            .iter()
            .find(|member| member.identity == self.local_identity)
            .or_else(|| {
                self.previous_configuration.as_ref().and_then(|previous| {
                    previous
                        .members
                        .iter()
                        .find(|member| member.identity == self.local_identity)
                })
            })
            .expect("validated local identity belongs to authority")
            .role
    }

    pub fn validate_envelope(&self, envelope: &ReplicationItem) -> Result<()> {
        self.validate_fence(
            &envelope.sender,
            &envelope.receiver,
            envelope.epoch,
            envelope.previous_configuration_id.as_ref(),
            &envelope.current_configuration_id,
        )
    }

    pub fn validate_acknowledgement(&self, acknowledgement: &ReplicationAck) -> Result<()> {
        self.validate_fence(
            &acknowledgement.sender,
            &acknowledgement.receiver,
            acknowledgement.epoch,
            acknowledgement.previous_configuration_id.as_ref(),
            &acknowledgement.current_configuration_id,
        )
    }

    fn validate_fence(
        &self,
        sender: &ReplicaIdentity,
        receiver: &ReplicaIdentity,
        epoch: kuberic_protocol::types::Epoch,
        previous_configuration_id: Option<&kuberic_protocol::types::ConfigurationId>,
        current_configuration_id: &kuberic_protocol::types::ConfigurationId,
    ) -> Result<()> {
        if sender != self.primary_identity() {
            return Err(ContractError::AuthorityMismatch(
                "replication sender is not the exact admitted primary".to_string(),
            ));
        }
        if !self.contains_member(receiver) {
            return Err(ContractError::AuthorityMismatch(
                "replication receiver is outside admitted authority".to_string(),
            ));
        }
        if epoch != self.current_configuration.epoch
            || current_configuration_id != &self.current_configuration.configuration_id
            || previous_configuration_id
                != self
                    .previous_configuration
                    .as_ref()
                    .map(|configuration| &configuration.configuration_id)
        {
            return Err(ContractError::AuthorityMismatch(
                "replication epoch or configuration fence is stale".to_string(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
pub trait ReplicaAuthorityStore: Send + Sync {
    async fn load(&self) -> Result<Option<AdmittedAuthority>>;

    async fn admit(&self, authority: &AdmittedAuthority) -> Result<()>;
}

#[async_trait]
pub trait ReplicationProgressStore: Send + Sync {
    async fn load_replication_progress(
        &self,
        fence: &AuthorityFence,
    ) -> Result<Option<ReplicationProgress>>;

    async fn load_configuration_progress(
        &self,
        epoch: Epoch,
        current_configuration_id: &ConfigurationId,
    ) -> Result<Option<ReplicationProgress>>;

    async fn record_replication_progress(&self, progress: &ReplicationProgress) -> Result<()>;
}

#[async_trait]
pub trait LocalWriteJournal: Send + Sync {
    async fn load_local_write(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<DurableLocalWrite>>;

    async fn load_local_writes(&self) -> Result<Vec<DurableLocalWrite>>;

    async fn record_local_write(&self, write: &DurableLocalWrite) -> Result<()>;

    async fn reset_local_writes_after_data_loss(&self, committed_lsn: i64) -> Result<()>;
}

#[async_trait]
pub trait BuildAuthorityStore: Send + Sync {
    async fn load_build(&self, build_id: &OperationId) -> Result<Option<BuildAuthority>>;

    async fn admit_build(&self, authority: &BuildAuthority) -> Result<()>;
}

#[async_trait]
pub trait BuildProgressStore: Send + Sync {
    async fn load_build_progress(
        &self,
        build_id: &OperationId,
    ) -> Result<Option<DurableBuildProgress>>;

    async fn record_build_progress(&self, progress: &DurableBuildProgress) -> Result<()>;
}

pub trait AuthorityStore:
    ReplicaAuthorityStore
    + ReplicationProgressStore
    + LocalWriteJournal
    + BuildAuthorityStore
    + BuildProgressStore
{
}

impl<T> AuthorityStore for T where
    T: ReplicaAuthorityStore
        + ReplicationProgressStore
        + LocalWriteJournal
        + BuildAuthorityStore
        + BuildProgressStore
{
}
