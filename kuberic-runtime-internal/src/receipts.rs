use kuberic_protocol::types::{
    AccessStatus, ConfigurationId, Epoch, ReplicaIdentity, SecondaryRemovalPreparation,
    SecondaryRemovalWitness, SecondaryScaleDownCleanup, SwitchoverRequestId,
};
use serde::{Deserialize, Serialize};

use crate::authority::AdmittedAuthority;
use crate::authority::RetiredAuthority;

/// Exact native engine identity captured before a public operation begins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeOperationToken {
    pub authority: Option<AdmittedAuthority>,
    pub engine_session_id: String,
    pub engine_generation: u64,
}

/// Narrow native progress observation used for effect postconditions and
/// recovery without mirroring the full runtime snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeProgressStatus {
    pub current_progress: i64,
    pub verified_replication_lsn: Option<i64>,
    pub committed_lsn: i64,
    pub current_configuration_quorum_progress: i64,
    pub catch_up_boundary: Option<i64>,
    pub catch_up_complete: bool,
}

/// Narrow native topology observation used to restore host reporting and
/// access gating without mirroring a full runtime snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeTopologyStatus {
    pub prepared_secondary_removal: Option<SecondaryRemovalPreparation>,
    pub accepted_secondary_removal: Option<SecondaryScaleDownCleanup>,
    pub retired_authority: Option<RetiredAuthority>,
}

/// Native access preparation. The exact value must be supplied back to the
/// engine for publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessPreparation {
    pub authority: Option<AdmittedAuthority>,
    pub engine_session_id: String,
    pub engine_generation: u64,
    pub read: AccessStatus,
    pub write: AccessStatus,
    pub current_progress: i64,
    pub committed_lsn: i64,
}

/// Durable certified-prefix settlement evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CertifiedPrefixReceipt {
    pub token: NativeOperationToken,
    pub verified_lsn: i64,
    pub settled_lsn: i64,
    pub committed_lsn: i64,
}

/// Exact native proof for one planned switchover preparation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwitchoverReceipt {
    pub token: NativeOperationToken,
    pub preparation_generation: u64,
    pub request_id: SwitchoverRequestId,
    pub source: ReplicaIdentity,
    pub target: ReplicaIdentity,
    pub starting_configuration_id: ConfigurationId,
    pub starting_epoch: Epoch,
    pub handoff_lsn: i64,
    pub committed_lsn: i64,
}

/// Native secondary-removal evidence returned at each durable boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryRemovalReceipt {
    pub token: NativeOperationToken,
    pub preparation: Option<SecondaryRemovalPreparation>,
    pub witness: Option<SecondaryRemovalWitness>,
    pub accepted: Option<SecondaryScaleDownCleanup>,
    pub verified_lsn: Option<i64>,
    pub committed_lsn: i64,
}

/// Native retirement start/completion evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetirementReceipt {
    pub engine_session_id: String,
    pub engine_generation: u64,
    pub retired: RetiredAuthority,
    pub completed: bool,
}

/// The canonical durable proof returned by Kuberic-specific native topology
/// operations. Public catch-up, build, and ordinary removal completion do not
/// use this private protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopologyReceipt {
    CertifiedPrefix(Box<CertifiedPrefixReceipt>),
    Switchover(Box<SwitchoverReceipt>),
    SecondaryRemoval(Box<SecondaryRemovalReceipt>),
    Retirement(Box<RetirementReceipt>),
}
