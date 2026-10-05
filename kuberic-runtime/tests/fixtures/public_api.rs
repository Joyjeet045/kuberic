#![forbid(unsafe_code)]

use std::sync::Arc;

use kuberic_runtime::application::{ClientWrite, OpenMode};
use kuberic_runtime::host::{
    ApplicationStorageState, KubernetesDnsResolver, ReplicaDiagnostics, ReplicaEndpointResolver,
    ReplicaHandle, ReplicaHost, ReplicaProcessConfig, RunningReplica,
};
use kuberic_runtime::protocol::types::{Epoch, OperationId, ReplicaRole};
use kuberic_runtime::replicator::{PrimaryReplicator, Replicator, ReplicatorInterfaces};

fn ordinary_application(control: Arc<dyn Replicator>, primary: Arc<dyn PrimaryReplicator>) {
    let _ = ReplicatorInterfaces::secondary(control, None);
    let _ = primary.change_role(Epoch::new(1, 2), ReplicaRole::Primary);
    let _ = ClientWrite {
        operation_id: OperationId::new("external-write"),
        data: Default::default(),
    };
    let _ = OpenMode::Existing;
}

fn host_application<A, R>(config: ReplicaProcessConfig, application: Arc<A>, resolver: Arc<R>)
where
    A: kuberic_runtime::StatefulServiceReplica + 'static,
    R: ReplicaEndpointResolver + 'static,
{
    let host = ReplicaHost::new(
        config,
        application,
        ApplicationStorageState::Established,
        resolver,
    )
    .with_application_storage_paths(Default::default());
    let _ = host.start();
    let _ = KubernetesDnsResolver::new(
        kuberic_runtime::protocol::types::ResourceUid::new("resource"),
        "namespace",
    );
    let _ = <KubernetesDnsResolver as ReplicaEndpointResolver>::control_endpoint;
}

fn isolated_fixture<A>(
    identity: kuberic_runtime::protocol::types::ReplicaIdentity,
    application: Arc<A>,
    store: Arc<kuberic_runtime::testing::sqlite_store::SqliteStore>,
) where
    A: kuberic_runtime::StatefulServiceReplica + 'static,
{
    let fixture = kuberic_runtime::testing::hosting::PodRuntime::new(
        identity,
        application,
        store,
    );
    let _ = fixture.snapshot();
    let _ = kuberic_runtime::testing::InProcessTransport::new();
}

async fn observe_host(replica: &mut RunningReplica, handle: ReplicaHandle) {
    let _: ReplicaHandle = replica.handle();
    let _ = replica.shutdown_signal();
    let _: kuberic_runtime::host::Result<ReplicaDiagnostics> = handle.diagnostics().await;
    replica.shutdown();
    let _: kuberic_runtime::host::Result<()> = replica.wait().await;
}

fn main() {}
