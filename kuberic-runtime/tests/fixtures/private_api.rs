#![forbid(unsafe_code)]

use std::sync::Arc;

use kuberic_runtime::authority::AdmittedAuthority;
use kuberic_runtime::capabilities::{ReplicatorCreationIdentity, RuntimeHostToken};
use kuberic_runtime::effects::RuntimeEffect;
use kuberic_runtime::host::{
    command, coordinator, hosting, provisioning, recovery, report, runtime_adapter, service,
    session, sqlite_store, state, store, testing as host_testing, transport as host_transport,
};
use kuberic_runtime::host::hosting::{
    BuildRuntime, OutboundRuntime, PeerDiscoveryRuntime, ReportRuntime,
};
use kuberic_runtime::host::hosting::lifecycle::{
    LifecycleWiring, RecoveryRuntime, TopologyRuntime,
};
use kuberic_runtime::host::hosting::custom::ReplicatorLifecycleRegistration;
use kuberic_runtime::receipts::TopologyReceipt;
use kuberic_runtime::replicator::{
    DefaultReplicatorDependencies, ManagedReplicatorDataPlane, ManagedReplicatorLifecycle,
    PartitionAccessView, Replicator, ReplicatorAttachment, ReplicatorCreationReservation,
    ReplicatorFactoryContext, ReplicatorInterfaces, ReplicatorRegistration,
    StatefulServicePartition, copy::PreparedCopy, quorum::QuorumTracker, sender::ReliableSender,
};
use kuberic_runtime::runtime::PendingWrite;
use kuberic_runtime::transport::ReplicationItem;

fn infer_partition(context: ReplicatorFactoryContext) {
    let _ = StatefulServicePartition::new(Default::default(), Default::default(), context.clone());
    let _ = &context.default_dependencies;
}

fn forge_factory_context() {
    let _ = ReplicatorFactoryContext::new(
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
    );
}

fn forge_interfaces(control: Arc<dyn Replicator>) {
    let _ = ReplicatorInterfaces {
        replicator: control,
        state_replicator: None,
        primary_replicator: None,
    };
}

fn extract_fixture(
    runtime: kuberic_runtime::testing::hosting::PodRuntime,
    store: kuberic_runtime::testing::sqlite_store::SqliteStore,
) {
    let _ = runtime.inner;
    let _ = store.inner;
}

fn main() {}
