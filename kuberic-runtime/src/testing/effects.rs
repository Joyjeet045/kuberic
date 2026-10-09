use crate::protocol::types::{
    AccessStatus, ConfigurationId, OperationId, ReplicaIdentity, ReplicaRole, SwitchoverRequestId,
};
use serde::{Deserialize, Serialize};

use super::authority::RetiredAuthority;
use super::authority::{AdmittedAuthority, BuildAuthority};
use crate::application::OpenMode;

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::deserialize(deserializer)
}
use crate::protocol::types::{
    ProcessSessionId, SecondaryRemovalPreparation, SecondaryRemovalWitness,
    SecondaryScaleDownCleanup, SecondaryScaleDownIntent,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleTransition {
    pub completed_role: ReplicaRole,
    pub target_role: ReplicaRole,
    pub replicator_completed: bool,
    pub epoch_completed: bool,
    pub application_completed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeEffect {
    pub operation_id: OperationId,
    pub sequence: u64,
    pub action: RuntimeEffectAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeEffectAction {
    Open(OpenMode),
    AdmitAuthority(Box<AdmittedAuthority>),
    PrepareSecondaryRemoval {
        intent: Box<SecondaryScaleDownIntent>,
        process_session_id: ProcessSessionId,
        report_sequence: u64,
    },
    RegisterPeerSession {
        identity: ReplicaIdentity,
        session: ProcessSessionId,
    },
    ObserveSecondaryRemovalWitness(Box<SecondaryRemovalWitness>),
    ObserveSecondaryRemovalProgress {
        witness: Box<SecondaryRemovalWitness>,
        committed: Box<SecondaryScaleDownCleanup>,
    },
    AcceptSecondaryRemovalCommit(Box<SecondaryScaleDownCleanup>),
    AcceptHistoricalSecondaryRemovalCommit(
        Box<crate::protocol::command::AcceptSecondaryRemovalCommit>,
    ),
    RetireReplica(Box<RetiredAuthority>),
    FenceRetirement(Box<RetiredAuthority>),
    CompleteRetirement(Box<RetiredAuthority>),
    AuthorizeFailoverPrefix(i64),
    AdmitBuildAuthority(Box<BuildAuthority>),
    ChangeRole(ReplicaRole),
    ChangeReplicatorRole(ReplicaRole),
    UpdateEpoch,
    ChangeApplicationRole(ReplicaRole),
    WaitForCatchup,
    SetAccessStatus {
        read: AccessStatus,
        write: AccessStatus,
    },
    SetReadStatus(AccessStatus),
    SetWriteStatus(AccessStatus),
    PrepareSwitchover {
        preparation_generation: u64,
        request_id: SwitchoverRequestId,
        source: ReplicaIdentity,
        target: ReplicaIdentity,
        starting_configuration_id: ConfigurationId,
        starting_epoch: crate::protocol::types::Epoch,
    },
    RefreshApplicationProgress,
    BuildReplica {
        build_id: OperationId,
        target: ReplicaIdentity,
        replication_address: String,
    },
    RetireBuild(OperationId),
    Close,
    Abort,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildPostcondition {
    pub authority: BuildAuthority,
    pub last_sequence: u64,
    pub durable_lsn: i64,
    pub completed: bool,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub catch_up_boundary_lsn: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSnapshot {
    pub identity: ReplicaIdentity,
    pub open: bool,
    pub replication_address: Option<String>,
    pub role: ReplicaRole,
    pub role_transition: Option<RoleTransition>,
    pub read_status: AccessStatus,
    pub write_status: AccessStatus,
    pub authority: Option<AdmittedAuthority>,
    pub prepared_secondary_removal: Option<SecondaryRemovalPreparation>,
    pub retired_authority: Option<RetiredAuthority>,
    pub accepted_secondary_removal: Option<SecondaryScaleDownCleanup>,
    pub current_progress: i64,
    pub verified_replication_lsn: Option<i64>,
    #[serde(default)]
    pub live_builds_only: bool,
    pub committed_lsn: i64,
    pub current_configuration_quorum_progress: i64,
    pub catch_up_boundary: Option<i64>,
    pub catch_up_complete: bool,
    pub builds: Vec<BuildPostcondition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeEffectResult {
    pub operation_id: OperationId,
    pub sequence: u64,
    pub outcome: serde_json::Value,
}
