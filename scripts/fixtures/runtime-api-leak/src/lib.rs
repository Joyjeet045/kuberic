#![forbid(unsafe_code)]

use std::sync::Arc;

use kuberic_runtime::replicator::{
    PrimaryReplicator, Replicator, ReplicatorFactoryContext, ReplicatorInterfaces,
    ReplicatorRegistration, StateReplicator, StatefulServicePartition,
};
use kuberic_runtime::authority::AdmittedAuthority;

pub fn obtain_managed(interfaces: &ReplicatorInterfaces) {
    let _ = interfaces.managed_replicator();
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
    let _ = ReplicatorInterfaces::new(control, state, Some(primary));
}

pub async fn register_managed_directly(context: ReplicatorFactoryContext) {
    context.register_managed(panic!("no managed capability")).await;
}

pub fn inspect_host_dependencies(context: &ReplicatorFactoryContext) {
    let _ = &context.default_dependencies;
}

pub fn obtain_authority(_: AdmittedAuthority) {}
