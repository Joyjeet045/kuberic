use kuberic_protocol::types::{
    AccessStatus, ConfigurationId, OperationId, ReplicaIdentity, ReplicaRole, SwitchoverRequestId,
};
use serde::{Deserialize, Serialize};

use crate::authority::RetiredAuthority;
use crate::authority::{AdmittedAuthority, BuildAuthority};
use kuberic_protocol::types::{
    ProcessSessionId, SecondaryRemovalPreparation, SecondaryRemovalWitness,
    SecondaryScaleDownCleanup, SecondaryScaleDownIntent,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpenMode {
    New,
    Existing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleTransition {
    pub completed_role: ReplicaRole,
    pub target_role: ReplicaRole,
    pub replicator_completed: bool,
    #[serde(default)]
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
    ObserveReplicationAck {
        acknowledgement: Box<crate::transport::ReplicationAck>,
        session: ProcessSessionId,
    },
    AcceptSecondaryRemovalCommit(Box<SecondaryScaleDownCleanup>),
    AcceptHistoricalSecondaryRemovalCommit(
        Box<kuberic_protocol::command::AcceptSecondaryRemovalCommit>,
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
        starting_epoch: kuberic_protocol::types::Epoch,
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
    #[serde(default)]
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
    pub committed_lsn: i64,
    pub current_configuration_quorum_progress: i64,
    pub catch_up_boundary: Option<i64>,
    pub catch_up_complete: bool,
    pub builds: Vec<BuildPostcondition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimePostcondition {
    pub open: bool,
    pub role: ReplicaRole,
    pub role_transition: Option<RoleTransition>,
    pub read_status: AccessStatus,
    pub write_status: AccessStatus,
    pub authority: Option<AdmittedAuthority>,
    #[serde(default)]
    pub prepared_secondary_removal: Option<SecondaryRemovalPreparation>,
    #[serde(default)]
    pub retired_authority: Option<RetiredAuthority>,
    #[serde(default)]
    pub accepted_secondary_removal: Option<SecondaryScaleDownCleanup>,
    pub current_progress: i64,
    pub verified_replication_lsn: Option<i64>,
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
    pub postcondition: RuntimePostcondition,
}

impl From<RuntimeSnapshot> for RuntimePostcondition {
    fn from(snapshot: RuntimeSnapshot) -> Self {
        Self {
            open: snapshot.open,
            role: snapshot.role,
            role_transition: snapshot.role_transition,
            read_status: snapshot.read_status,
            write_status: snapshot.write_status,
            authority: snapshot.authority,
            prepared_secondary_removal: snapshot.prepared_secondary_removal,
            retired_authority: snapshot.retired_authority,
            accepted_secondary_removal: snapshot.accepted_secondary_removal,
            current_progress: snapshot.current_progress,
            verified_replication_lsn: snapshot.verified_replication_lsn,
            committed_lsn: snapshot.committed_lsn,
            current_configuration_quorum_progress: snapshot.current_configuration_quorum_progress,
            catch_up_boundary: snapshot.catch_up_boundary,
            catch_up_complete: snapshot.catch_up_complete,
            builds: snapshot.builds,
        }
    }
}
