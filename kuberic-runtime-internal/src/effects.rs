use kuberic_protocol::types::{AccessStatus, OperationId, ReplicaIdentity, ReplicaRole};
use serde::{Deserialize, Serialize};

use crate::authority::{AdmittedAuthority, BuildAuthority};

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
