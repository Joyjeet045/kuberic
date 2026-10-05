use crate::protocol::types::{
    AccessStatus, ConfigurationId, Epoch, ReplicaIdentity, SecondaryRemovalPreparation,
    SecondaryRemovalWitness, SecondaryScaleDownCleanup, SwitchoverRequestId,
};
use serde::{Deserialize, Serialize};

use crate::authority::AdmittedAuthority;
use crate::authority::RetiredAuthority;

/// Exact native engine identity captured before a public operation begins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct NativeOperationToken {
    pub(crate) authority: Option<AdmittedAuthority>,
    pub(crate) engine_session_id: String,
    pub(crate) engine_generation: u64,
}

/// Narrow native progress observation used for effect postconditions and
/// recovery without mirroring the full runtime snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct NativeProgressStatus {
    pub(crate) current_progress: i64,
    pub(crate) verified_replication_lsn: Option<i64>,
    pub(crate) committed_lsn: i64,
    pub(crate) current_configuration_quorum_progress: i64,
    pub(crate) catch_up_boundary: Option<i64>,
    pub(crate) catch_up_complete: bool,
}

/// Narrow native topology observation used to restore host reporting and
/// access gating without mirroring a full runtime snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct NativeTopologyStatus {
    pub(crate) prepared_secondary_removal: Option<SecondaryRemovalPreparation>,
    pub(crate) accepted_secondary_removal: Option<SecondaryScaleDownCleanup>,
    pub(crate) retired_authority: Option<RetiredAuthority>,
}

/// Native access preparation. The exact value must be supplied back to the
/// engine for publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AccessPreparation {
    pub(crate) authority: Option<AdmittedAuthority>,
    pub(crate) engine_session_id: String,
    pub(crate) engine_generation: u64,
    pub(crate) read: AccessStatus,
    pub(crate) write: AccessStatus,
    pub(crate) current_progress: i64,
    pub(crate) committed_lsn: i64,
}

/// Durable certified-prefix settlement evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CertifiedPrefixReceipt {
    pub(crate) token: NativeOperationToken,
    pub(crate) verified_lsn: i64,
    pub(crate) settled_lsn: i64,
    pub(crate) committed_lsn: i64,
}

/// Exact native proof for one planned switchover preparation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SwitchoverReceipt {
    pub(crate) token: NativeOperationToken,
    pub(crate) preparation_generation: u64,
    pub(crate) request_id: SwitchoverRequestId,
    pub(crate) source: ReplicaIdentity,
    pub(crate) target: ReplicaIdentity,
    pub(crate) starting_configuration_id: ConfigurationId,
    pub(crate) starting_epoch: Epoch,
    pub(crate) handoff_lsn: i64,
    pub(crate) committed_lsn: i64,
}

/// Native secondary-removal evidence returned at each durable boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SecondaryRemovalReceipt {
    pub(crate) token: NativeOperationToken,
    pub(crate) preparation: Option<SecondaryRemovalPreparation>,
    pub(crate) witness: Option<SecondaryRemovalWitness>,
    pub(crate) accepted: Option<SecondaryScaleDownCleanup>,
    pub(crate) verified_lsn: Option<i64>,
    pub(crate) committed_lsn: i64,
}

/// Native retirement start/completion evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RetirementReceipt {
    pub(crate) engine_session_id: String,
    pub(crate) engine_generation: u64,
    pub(crate) retired: RetiredAuthority,
    pub(crate) completed: bool,
}

/// The canonical durable proof returned by Kuberic-specific native topology
/// operations. Public catch-up, build, and ordinary removal completion do not
/// use this private protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum TopologyReceipt {
    CertifiedPrefix(Box<CertifiedPrefixReceipt>),
    Switchover(Box<SwitchoverReceipt>),
    SecondaryRemoval(Box<SecondaryRemovalReceipt>),
    Retirement(Box<RetirementReceipt>),
}
