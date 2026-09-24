pub mod api;
pub mod attestation;
pub mod controller;
pub mod discovery;
pub mod observability;
pub mod placement;
pub mod preflight;
mod release;
pub mod safety;

pub use api::{
    AffectedKubericSetStatus, AffectedReplicaStatus, MaintenanceBlockedReason,
    MaintenanceDesiredState, MaintenancePhase, NodeMaintenanceRequest, NodeMaintenanceRequestSpec,
    NodeMaintenanceRequestStatus, NodeRecovery, PREPARED_CONDITION_TYPE, ReplicaRecovery,
};
pub use attestation::{
    Attestation, CommittedMember, CommittedTopology, Epoch, LiveMember, LiveObservation, attest,
};
pub use controller::{
    KubeMaintenanceApi, MaintenanceApi, ReconcileOutcome, RequestContext, reconcile_request,
};
pub use discovery::{Discovery, DiscoveryInput, MaintenancePod, NodeRef, reconcile_discovery};
pub use placement::{
    PlacementCandidate, explicit_target_is_eligible, is_eligible_primary,
    switchover_target_for_maintenance,
};
pub use preflight::{Preflight, preflight};
pub use safety::{SetEvaluation, SetPlacement, SetReadiness, evaluate_set, reconcile_preparation};
