//! Detached fixture records, never production authority capabilities.
use crate::protocol::types::*;
pub use crate::protocol::types::{BuildAuthority, BuildAuthorityKind};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
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
    #[serde(default)]
    pub catch_up_boundary_lsn: Option<i64>,
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
    #[serde(default)]
    pub switchover_handoff: Option<SwitchoverHandoff>,
    #[serde(default)]
    pub secondary_removal: Option<SecondaryRemovalEvidence>,
    #[serde(default)]
    pub scale_up: Option<Box<ScaleUpConfigurationEvidence>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetiredAuthority {
    pub committed: SecondaryScaleDownCleanup,
    pub report: ReplicaRetirementReport,
}
