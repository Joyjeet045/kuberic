#![forbid(unsafe_code)]

use std::sync::Arc;

use kuberic_runtime::replicator::{
    PrimaryReplicator, Replicator, ReplicatorAttachment, ReplicatorCreationReservation,
    ReplicatorFactoryContext, ReplicatorInterfaces, ReplicatorRegistration, StateReplicator,
    StatefulServicePartition,
};
use kuberic_runtime::authority::AdmittedAuthority;
use kuberic_runtime::host::hosting::{
    BuildRuntime, OutboundRuntime, PeerDiscoveryRuntime, ReportRuntime,
};
use kuberic_runtime::host::hosting::lifecycle::{
    LifecycleWiring, RecoveryRuntime, TopologyRuntime,
};
use kuberic_runtime::host::hosting::custom::ReplicatorLifecycleRegistration;

pub fn extract_attachment(
    interfaces: &ReplicatorInterfaces,
    reservation: ReplicatorCreationReservation,
) {
    let _ = interfaces.prepare_attachment(reservation);
}

pub fn infer_host_token(
    registration: Arc<dyn ReplicatorRegistration>,
    context: ReplicatorFactoryContext,
) {
    let _ = StatefulServicePartition::new(Default::default(), registration, context);
}

pub fn assemble_split_bundle(
    control: Arc<dyn Replicator>,
    state: Arc<dyn StateReplicator>,
    primary: Arc<dyn PrimaryReplicator>,
) {
    let _ = ReplicatorInterfaces {
        replicator: control,
        state_replicator: Some(state),
        primary_replicator: Some(primary),
    };
}

pub fn forge_attachment(control: Arc<dyn Replicator>, primary: Arc<dyn PrimaryReplicator>) {
    let _ = ReplicatorAttachment {
        replicator: control,
        primary_replicator: Some(primary),
    };
}

pub fn disarm_bundle_guard(attachment: &ReplicatorAttachment) {
    attachment.disarm();
}

pub fn inspect_host_dependencies(context: &ReplicatorFactoryContext) {
    let _ = &context.default_dependencies;
}

pub fn obtain_authority(_: AdmittedAuthority) {}
