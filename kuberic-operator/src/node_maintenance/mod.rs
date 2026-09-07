pub mod api;
pub mod controller;
pub mod discovery;
pub mod placement;
pub mod preflight;
pub mod safety;

pub use api::{
    AffectedKubericSetStatus, AffectedReplicaStatus, MaintenanceBlockedReason,
    MaintenanceDesiredState, MaintenanceOperation, MaintenancePhase, NodeMaintenanceRequest,
    NodeMaintenanceRequestSpec, NodeMaintenanceRequestStatus, PREPARED_CONDITION_TYPE,
};
pub use controller::{
    KubeMaintenanceApi, MaintenanceApi, ReconcileOutcome, RequestContext, reconcile_request,
};
pub use discovery::{Discovery, DiscoveryInput, MaintenancePod, NodeRef, reconcile_discovery};
pub use placement::{PlacementCandidate, switchover_target_for_maintenance};
pub use preflight::{Preflight, preflight};
pub use safety::{
    SetEvaluation, SetPlacement, SetReadiness, SetTopology, evaluate_set, reconcile_preparation,
};
