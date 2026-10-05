use crate::protocol::types::{
    AccessStatus, ConfigurationId, OperationId, ReplicaIdentity, ReplicaRole, SwitchoverRequestId,
};
use serde::{Deserialize, Serialize};

use crate::authority::RetiredAuthority;
use crate::authority::{AdmittedAuthority, BuildAuthority};
use crate::protocol::types::{
    ProcessSessionId, SecondaryRemovalPreparation, SecondaryRemovalWitness,
    SecondaryScaleDownCleanup, SecondaryScaleDownIntent,
};
use crate::receipts::TopologyReceipt;

use crate::application::OpenMode;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RoleTransition {
    pub(crate) completed_role: ReplicaRole,
    pub(crate) target_role: ReplicaRole,
    pub(crate) replicator_completed: bool,
    #[serde(default)]
    pub(crate) epoch_completed: bool,
    pub(crate) application_completed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RuntimeEffect {
    pub(crate) operation_id: OperationId,
    pub(crate) sequence: u64,
    pub(crate) action: RuntimeEffectAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum RuntimeEffectAction {
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
pub(crate) struct BuildPostcondition {
    pub(crate) authority: BuildAuthority,
    pub(crate) last_sequence: u64,
    pub(crate) durable_lsn: i64,
    pub(crate) completed: bool,
    #[serde(default)]
    pub(crate) catch_up_boundary_lsn: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RuntimeSnapshot {
    pub(crate) identity: ReplicaIdentity,
    pub(crate) open: bool,
    pub(crate) replication_address: Option<String>,
    pub(crate) role: ReplicaRole,
    pub(crate) role_transition: Option<RoleTransition>,
    pub(crate) read_status: AccessStatus,
    pub(crate) write_status: AccessStatus,
    pub(crate) authority: Option<AdmittedAuthority>,
    pub(crate) prepared_secondary_removal: Option<SecondaryRemovalPreparation>,
    pub(crate) retired_authority: Option<RetiredAuthority>,
    pub(crate) accepted_secondary_removal: Option<SecondaryScaleDownCleanup>,
    pub(crate) current_progress: i64,
    pub(crate) verified_replication_lsn: Option<i64>,
    #[serde(default)]
    pub(crate) live_builds_only: bool,
    pub(crate) committed_lsn: i64,
    pub(crate) current_configuration_quorum_progress: i64,
    pub(crate) catch_up_boundary: Option<i64>,
    pub(crate) catch_up_complete: bool,
    pub(crate) builds: Vec<BuildPostcondition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RuntimePostcondition {
    pub(crate) open: bool,
    pub(crate) role: ReplicaRole,
    pub(crate) role_transition: Option<RoleTransition>,
    pub(crate) read_status: AccessStatus,
    pub(crate) write_status: AccessStatus,
    pub(crate) authority: Option<AdmittedAuthority>,
    #[serde(default)]
    pub(crate) prepared_secondary_removal: Option<SecondaryRemovalPreparation>,
    #[serde(default)]
    pub(crate) retired_authority: Option<RetiredAuthority>,
    #[serde(default)]
    pub(crate) accepted_secondary_removal: Option<SecondaryScaleDownCleanup>,
    pub(crate) current_progress: i64,
    pub(crate) verified_replication_lsn: Option<i64>,
    pub(crate) committed_lsn: i64,
    pub(crate) current_configuration_quorum_progress: i64,
    pub(crate) catch_up_boundary: Option<i64>,
    pub(crate) catch_up_complete: bool,
    pub(crate) builds: Vec<BuildPostcondition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RuntimeEffectResult {
    pub(crate) operation_id: OperationId,
    pub(crate) sequence: u64,
    #[serde(default)]
    pub(crate) topology_receipt: Option<Box<TopologyReceipt>>,
    pub(crate) postcondition: RuntimePostcondition,
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
