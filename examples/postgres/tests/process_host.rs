use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kuberic_agent::process::{ReplicaHost, ReplicaProcessConfig, RunningReplica};
use kuberic_agent::transport::ReplicaEndpointResolver;
use kuberic_agent::{sqlite_store::SqliteStore, store::AgentStore};
use kuberic_protocol::types::{
    ConfigurationDescriptor, ConfigurationMember, Epoch, FaultType, PodUid, PvcUid, ReplicaId,
    ReplicaIdentity, ReplicaInstanceId, ReplicaRole, ResourceUid, derive_agent_generation,
    derive_initialization_id,
};
use kuberic_runtime::application::{OpenContext, RoleChange};
use kuberic_runtime::replicator::ReplicaSetQuorumMode;
use kuberic_runtime::{PrimaryReplicator, Replicator, StatefulServiceReplica};
use kuberic_wire::proto::{self as wire, agent_control_client::AgentControlClient};
use postgres_replicated::data_service::PgDataServiceImpl;
use postgres_replicated::durable::{PgDurableIdentity, PgDurableStore, StorageMode};
use postgres_replicated::instance::PgInstanceManager;
use postgres_replicated::native::PgNativeObserver;
use postgres_replicated::proto::{self as pgwire, pg_data_service_client::PgDataServiceClient};
use postgres_replicated::testing::{
    ProcessProbe, TestDataDir, allocate_port, find_pg_bin, wrapped_pg_bin,
};
use postgres_replicated::{PgService, PgServiceConfig};
use tonic::{Request, transport::Channel};

const TOKEN: &str = "host-local-postgres-test";

#[path = "../src/testing/layout.rs"]
mod layout;
use layout::SINGLE_REPLICA_DIRECTORY;

struct LocalResolver {
    control: SocketAddr,
    replication: SocketAddr,
}
impl ReplicaEndpointResolver for LocalResolver {
    fn control_endpoint(&self, _: &ReplicaIdentity) -> String {
        format!("http://{}", self.control)
    }
    fn replication_endpoint(&self, _: &ReplicaIdentity) -> String {
        format!("http://{}", self.replication)
    }
}

fn identity() -> ReplicaIdentity {
    let initialization = derive_initialization_id(
        &ResourceUid::new("postgres-test"),
        ReplicaId::new(1),
        &PodUid::new("postgres-1"),
        &PvcUid::new("postgres-pvc-1"),
    );
    ReplicaIdentity {
        replica_id: ReplicaId::new(1),
        instance_id: ReplicaInstanceId::new("postgres-1"),
        agent_generation: derive_agent_generation(&initialization),
    }
}

fn configuration() -> ConfigurationDescriptor {
    ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        ReplicaId::new(1),
        vec![ConfigurationMember {
            identity: identity(),
            role: ReplicaRole::Primary,
        }],
        1,
    )
}

fn authorized<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
    request
}

fn execute(
    session: &str,
    command: wire::execute_command_request::Command,
) -> Request<wire::ExecuteCommandRequest> {
    authorized(wire::ExecuteCommandRequest {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: "postgres-test".into(),
        target: Some(identity().into()),
        expected_process_session_id: session.into(),
        command: Some(command),
    })
}

fn initialize(session: &str) -> Request<wire::ExecuteCommandRequest> {
    execute(
        session,
        wire::execute_command_request::Command::InitializeAgentStore(
            wire::InitializeAgentStoreCommand {
                initialization_id: derive_initialization_id(
                    &ResourceUid::new("postgres-test"),
                    ReplicaId::new(1),
                    &PodUid::new("postgres-1"),
                    &PvcUid::new("postgres-pvc-1"),
                )
                .to_string(),
                resource_uid: "postgres-test".into(),
                local_replica_id: 1,
                expected_instance_id: "postgres-1".into(),
                expected_pod_uid: "postgres-1".into(),
                expected_pvc_uid: "postgres-pvc-1".into(),
                assigned_agent_generation: identity().agent_generation.to_string(),
                effective_policy: Some(policy()),
                bootstrap_configuration: Some(configuration().into()),
                provisioning: None,
            },
        ),
    )
}

fn policy() -> wire::EffectivePolicy {
    wire::EffectivePolicy {
        replica_set_size: 1,
        write_quorum: 1,
        read_quorum: 1,
        failover_delay_seconds: 30,
    }
}

fn configure(
    session: &str,
    operation: &str,
    granted: bool,
) -> Request<wire::ExecuteCommandRequest> {
    execute(
        session,
        wire::execute_command_request::Command::EnsureConfiguration(Box::new(
            wire::EnsureConfigurationCommand {
                operation_id: operation.into(),
                current_configuration: Some(configuration().into()),
                current_epoch: Some(configuration().epoch.into()),
                effective_policy: Some(policy()),
                local_replica_id: 1,
                expected_instance_id: "postgres-1".into(),
                expected_agent_generation: identity().agent_generation.to_string(),
                transition_kind: wire::TransitionKind::Bootstrap as i32,
                primary_write_status: if granted {
                    wire::AccessStatus::Granted
                } else {
                    wire::AccessStatus::ReconfigurationPending
                } as i32,
                grant_write: granted,
                ..Default::default()
            },
        )),
    )
}

async fn status(address: SocketAddr) -> (AgentControlClient<Channel>, wire::AgentStatusReport) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Ok(mut client) = AgentControlClient::connect(format!("http://{address}")).await {
                let report = client
                    .get_status(authorized(wire::GetAgentStatusRequest {
                        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                        resource_uid: "postgres-test".into(),
                        replica_id: 1,
                        expected_instance_id: "postgres-1".into(),
                    }))
                    .await;
                match report {
                    Ok(report) => return (client, report.into_inner()),
                    Err(error) if error.code() == tonic::Code::Unavailable => {}
                    Err(error)
                        if error.code() == tonic::Code::Unknown
                            && error.message() == "transport error" => {}
                    Err(error) => panic!("reachable local agent reports status: {error}"),
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("local agent listener")
}

struct HostAttempt {
    address: SocketAddr,
    application: Arc<PgService>,
    task: tokio::task::JoinHandle<kuberic_agent::Result<RunningReplica>>,
}

struct OpenGate {
    application: Arc<PgService>,
    reject: bool,
}

#[async_trait]
impl StatefulServiceReplica for OpenGate {
    async fn open(
        self: Arc<Self>,
        context: OpenContext,
    ) -> kuberic_runtime::Result<Arc<dyn Replicator>> {
        if self.reject {
            return Err(kuberic_runtime::RuntimeError::Application(
                "injected pre-open interruption".into(),
            ));
        }
        self.application.clone().open(context).await
    }

    async fn change_role(&self, role: ReplicaRole) -> kuberic_runtime::Result<RoleChange> {
        self.application.change_role(role).await
    }

    async fn close(&self) -> kuberic_runtime::Result<()> {
        self.application.close().await
    }

    fn abort(&self) {
        self.application.abort();
    }
}

impl Drop for HostAttempt {
    fn drop(&mut self) {
        self.task.abort();
        self.application.abort();
    }
}

impl HostAttempt {
    async fn start(root: &Path) -> Self {
        Self::start_as(root, "postgres-1").await
    }

    async fn start_as(root: &Path, pod_uid: &str) -> Self {
        Self::start_with(root, pod_uid, |_| {}).await
    }

    async fn start_with(
        root: &Path,
        pod_uid: &str,
        configure: impl FnOnce(&mut PgServiceConfig),
    ) -> Self {
        Self::start_gated(root, pod_uid, false, configure).await
    }

    async fn start_gated(
        root: &Path,
        pod_uid: &str,
        reject_open: bool,
        configure: impl FnOnce(&mut PgServiceConfig),
    ) -> Self {
        let control = format!("127.0.0.1:{}", allocate_port().await)
            .parse()
            .unwrap();
        let replication = format!("127.0.0.1:{}", allocate_port().await)
            .parse()
            .unwrap();
        let service = PgServiceConfig {
            resource_uid: ResourceUid::new("postgres-test"),
            application_root: root.join("application"),
            pg_data: root.join("pgdata"),
            pg_bin: find_pg_bin(),
            pg_port: allocate_port().await,
            replication_address: format!("http://{replication}"),
        };
        // Restart retains the listener identity while repairing managed configuration.
        let port_file = root.join("port");
        let mut service = service;
        if port_file.exists() {
            service.pg_port = std::fs::read_to_string(&port_file)
                .unwrap()
                .parse()
                .unwrap();
        }
        configure(&mut service);
        let storage = PgService::storage_state(&service).unwrap();
        let storage_paths = PgService::storage_paths(&service);
        let application = Arc::new(PgService::deferred(service));
        let host = ReplicaHost::new(
            ReplicaProcessConfig {
                resource_uid: ResourceUid::new("postgres-test"),
                replica_id: ReplicaId::new(1),
                pod_uid: PodUid::new(pod_uid),
                pvc_uid: PvcUid::new("postgres-pvc-1"),
                data_root: root.to_path_buf(),
                control_address: control,
                replication_address: replication,
                bearer_token: TOKEN.into(),
                rpc_deadline: Duration::from_secs(5),
                transport_window_capacity: 16,
            },
            Arc::new(OpenGate {
                application: application.clone(),
                reject: reject_open,
            }),
            storage,
            Arc::new(LocalResolver {
                control,
                replication,
            }),
        )
        .with_application_storage_paths(storage_paths);
        Self {
            address: control,
            application,
            task: tokio::spawn(host.start()),
        }
    }

    async fn ready(&mut self) -> RunningReplica {
        tokio::time::timeout(Duration::from_secs(30), &mut self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    }

    async fn bootstrap(&mut self, root: &Path) -> RunningReplica {
        let (mut client, report) = status(self.address).await;
        client
            .execute(initialize(&report.process_session_id))
            .await
            .unwrap();
        let running = self.ready().await;
        std::fs::write(
            root.join("port"),
            self.application.instance().port().to_string(),
        )
        .unwrap();
        running
    }
}

async fn stop(running: &mut RunningReplica, application: &PgService) {
    running.shutdown();
    tokio::time::timeout(Duration::from_secs(10), running.wait())
        .await
        .unwrap()
        .unwrap();
    application.close().await.unwrap();
    assert!(!application.instance().is_running().await);
}

fn files(path: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    fn visit(base: &Path, path: &Path, result: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
        if !path.exists() {
            return;
        }
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                result.insert(entry.path().strip_prefix(base).unwrap().into(), None);
                visit(base, &entry.path(), result);
            } else if entry.file_type().unwrap().is_file() {
                result.insert(
                    entry.path().strip_prefix(base).unwrap().into(),
                    Some(std::fs::read(entry.path()).unwrap()),
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    visit(path, path, &mut result);
    result
}

async fn fresh_host_waits_without_touching_application_storage() {
    for existing_empty in [false, true] {
        let root = TestDataDir::new("host-wait");
        if existing_empty {
            std::fs::create_dir(root.path().join("application")).unwrap();
            std::fs::create_dir(root.path().join("pgdata")).unwrap();
        }
        let before = files(root.path());
        let first = HostAttempt::start(root.path()).await;
        let (_, report) = status(first.address).await;
        assert_eq!(
            report.storage_state,
            wire::AgentStorageState::Uninitialized as i32
        );
        assert!(!first.task.is_finished());
        assert_eq!(files(root.path()), before);
        assert_eq!(root.path().join("application").exists(), existing_empty);
        assert_eq!(root.path().join("pgdata").exists(), existing_empty);
        drop(first);
        let mut second = HostAttempt::start(root.path()).await;
        let (mut client, retry) = status(second.address).await;
        assert_ne!(report.process_session_id, retry.process_session_id);
        assert!(
            client
                .execute(initialize(&report.process_session_id))
                .await
                .is_err()
        );
        assert_eq!(files(root.path()), before);
        let mut running = second.bootstrap(root.path()).await;
        assert!(root.path().join("pgdata/PG_VERSION").is_file());
        assert!(
            second
                .application
                .instance()
                .connect_application()
                .await
                .is_err()
        );
        second
            .application
            .change_role(ReplicaRole::Primary)
            .await
            .unwrap();
        assert!(
            second
                .application
                .instance()
                .connect_application()
                .await
                .is_err()
        );
        stop(&mut running, &second.application).await;
    }
}

async fn closed_host_releases_the_partition_and_driver() {
    let root = TestDataDir::new("host-release");
    let mut host = HostAttempt::start(root.path()).await;
    let mut running = host.bootstrap(root.path()).await;
    let application = Arc::downgrade(&host.application);
    stop(&mut running, &host.application).await;
    drop(running);
    drop(host);
    tokio::task::yield_now().await;
    assert!(
        application.upgrade().is_none(),
        "fault reporting must not retain a host cycle"
    );
}

async fn singleton_bootstrap_fence_and_restart_use_fresh_sessions_and_preserve_sql() {
    let root = TestDataDir::new("host-singleton");
    let mut first = HostAttempt::start(root.path()).await;
    let mut running = first.bootstrap(root.path()).await;
    let (mut client, report) = status(first.address).await;
    client
        .execute(configure(&report.process_session_id, "install", false))
        .await
        .unwrap();
    assert!(
        first
            .application
            .instance()
            .connect_application()
            .await
            .is_err()
    );
    client
        .execute(configure(&report.process_session_id, "bootstrap", true))
        .await
        .unwrap();
    let (sql, connection) = first
        .application
        .instance()
        .connect_application()
        .await
        .unwrap();
    sql.batch_execute("CREATE TABLE receipt(id int primary key); INSERT INTO receipt VALUES (42)")
        .await
        .unwrap();
    client
        .execute(configure(&report.process_session_id, "fence", false))
        .await
        .unwrap();
    assert!(
        sql.simple_query("INSERT INTO receipt VALUES (99)")
            .await
            .is_err()
    );
    connection.await.unwrap();
    assert!(
        first
            .application
            .instance()
            .connect_application()
            .await
            .is_err()
    );
    stop(&mut running, &first.application).await;
    drop(running);
    drop(first);

    let mut restarted = HostAttempt::start(root.path()).await;
    let mut running = restarted.ready().await;
    let (mut client, fresh) = status(restarted.address).await;
    assert_ne!(report.process_session_id, fresh.process_session_id);
    assert!(
        restarted
            .application
            .instance()
            .connect_application()
            .await
            .is_err()
    );
    assert!(
        client
            .execute(configure(&report.process_session_id, "stale-grant", true))
            .await
            .is_err()
    );
    restarted
        .application
        .change_role(ReplicaRole::Primary)
        .await
        .unwrap();
    assert!(
        restarted
            .application
            .instance()
            .connect_application()
            .await
            .is_err()
    );
    client
        .execute(configure(&fresh.process_session_id, "restore", true))
        .await
        .unwrap();
    let (sql, _) = restarted
        .application
        .instance()
        .connect_application()
        .await
        .unwrap();
    assert_eq!(
        sql.query_one("SELECT id FROM receipt", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        42
    );
    sql.execute("INSERT INTO receipt VALUES (43)", &[])
        .await
        .unwrap();
    stop(&mut running, &restarted.application).await;
}

async fn establish_storage(root: &Path) {
    let mut host = HostAttempt::start(root).await;
    let mut running = host.bootstrap(root).await;
    let (mut client, report) = status(host.address).await;
    client
        .execute(configure(
            &report.process_session_id,
            "install-storage",
            false,
        ))
        .await
        .unwrap();
    client
        .execute(configure(&report.process_session_id, "grant-storage", true))
        .await
        .unwrap();
    let (sql, connection) = host
        .application
        .instance()
        .connect_application()
        .await
        .unwrap();
    sql.batch_execute(
        "CREATE TABLE retained_receipt(id int); INSERT INTO retained_receipt VALUES (42)",
    )
    .await
    .unwrap();
    client
        .execute(configure(
            &report.process_session_id,
            "fence-storage",
            false,
        ))
        .await
        .unwrap();
    assert!(sql.simple_query("SELECT 1").await.is_err());
    connection.await.unwrap();
    stop(&mut running, &host.application).await;
    let store =
        SqliteStore::open_existing(SqliteStore::metadata_database_path(root), None).unwrap();
    assert!(
        !store
            .load_state()
            .await
            .unwrap()
            .application_storage
            .unwrap()
            .initializing
    );
}

async fn assert_storage_rejected(
    root: &Path,
    application_root: PathBuf,
    pgdata: PathBuf,
    expected_error: &str,
) {
    let paths = [
        root.join("application"),
        root.join("pgdata"),
        application_root.clone(),
        pgdata.clone(),
    ];
    let before: Vec<_> = paths
        .iter()
        .map(|path| (path.exists(), files(path)))
        .collect();
    let mut host = HostAttempt::start_with(root, "postgres-1", |config| {
        config.application_root = application_root;
        config.pg_data = pgdata;
    })
    .await;
    let error = tokio::time::timeout(Duration::from_secs(10), &mut host.task)
        .await
        .unwrap()
        .unwrap()
        .err()
        .expect("storage loss must reject startup");
    assert!(error.to_string().contains(expected_error), "{error}");
    let after: Vec<_> = paths
        .iter()
        .map(|path| (path.exists(), files(path)))
        .collect();
    assert_eq!(
        before, after,
        "no application files or directories may be created or changed"
    );
    let store =
        SqliteStore::open_existing(SqliteStore::metadata_database_path(root), None).unwrap();
    assert_eq!(
        store.load_state().await.unwrap().reported_fault,
        Some(FaultType::Permanent)
    );
    assert!(!host.application.instance().is_running().await);
    assert!(
        host.application
            .instance()
            .connect_application()
            .await
            .is_err()
    );
    assert!(host.application.instance().connect().await.is_err());
    assert!(
        tokio::net::TcpStream::connect(("127.0.0.1", host.application.instance().port()))
            .await
            .is_err()
    );
}

async fn established_storage_loss_never_reinitializes_postgres() {
    for empty_directories in [false, true] {
        let root = TestDataDir::new("host-storage-loss");
        establish_storage(root.path()).await;
        for path in [root.path().join("application"), root.path().join("pgdata")] {
            std::fs::remove_dir_all(&path).unwrap();
            if empty_directories {
                std::fs::create_dir(&path).unwrap();
            }
        }
        assert_storage_rejected(
            root.path(),
            root.path().join("application"),
            root.path().join("pgdata"),
            "durable state is missing",
        )
        .await;
    }
}

async fn missing_pgdata_alone_persists_permanent_fault_before_start_returns() {
    for empty_directory in [false, true] {
        let root = TestDataDir::new("host-pgdata-loss");
        establish_storage(root.path()).await;
        let path = SqliteStore::metadata_database_path(root.path());
        assert_eq!(
            SqliteStore::open_existing(&path, None)
                .unwrap()
                .load_state()
                .await
                .unwrap()
                .reported_fault,
            None
        );
        std::fs::remove_dir_all(root.path().join("pgdata")).unwrap();
        if empty_directory {
            std::fs::create_dir(root.path().join("pgdata")).unwrap();
        }
        assert_storage_rejected(
            root.path(),
            root.path().join("application"),
            root.path().join("pgdata"),
            "cannot inspect PostgreSQL control data",
        )
        .await;
    }
}

async fn changed_application_root_never_initializes_or_mutates_storage() {
    for moved in [false, true] {
        let root = TestDataDir::new("host-changed-application");
        establish_storage(root.path()).await;
        let changed = root.path().join("other-application");
        if moved {
            std::fs::rename(root.path().join("application"), &changed).unwrap();
        }
        assert_storage_rejected(
            root.path(),
            changed,
            root.path().join("pgdata"),
            "application storage paths differ",
        )
        .await;
    }
}

async fn changed_pgdata_path_never_initializes_or_mutates_storage() {
    for moved in [false, true] {
        let root = TestDataDir::new("host-changed-pgdata");
        establish_storage(root.path()).await;
        let changed = root.path().join("other-pgdata");
        if moved {
            std::fs::rename(root.path().join("pgdata"), &changed).unwrap();
        }
        assert_storage_rejected(
            root.path(),
            root.path().join("application"),
            changed,
            "application storage paths differ",
        )
        .await;
    }
}

async fn authorized_initialization_retries_at_storage_boundaries() {
    for stage in [
        "agent-only",
        "metadata",
        "initdb",
        "raw-initdb",
        "raw-initdb-data",
    ] {
        let root = TestDataDir::new("host-initialization-retry");
        let mut interrupted =
            HostAttempt::start_gated(root.path(), "postgres-1", stage == "agent-only", |config| {
                config.pg_bin = root.path().join("unavailable-pg-bin");
            })
            .await;
        let (mut client, report) = status(interrupted.address).await;
        client
            .execute(initialize(&report.process_session_id))
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(10), &mut interrupted.task)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        let port = interrupted.application.instance().port();
        std::fs::write(root.path().join("port"), port.to_string()).unwrap();
        assert_eq!(
            root.path().join("application/state-v2.json").exists(),
            stage != "agent-only"
        );
        assert!(!root.path().join("pgdata").exists());
        let store =
            SqliteStore::open_existing(SqliteStore::metadata_database_path(root.path()), None)
                .unwrap();
        assert!(
            store
                .load_state()
                .await
                .unwrap()
                .application_storage
                .unwrap()
                .initializing
        );
        drop(interrupted);
        let mut expected_system = None;
        let raw = stage.starts_with("raw-initdb");
        if stage == "initdb" || raw {
            let initialized =
                PgInstanceManager::new(root.path().join("pgdata"), find_pg_bin(), port);
            if raw {
                // Reproduce subprocess success without entering write_initial.
                let output = tokio::process::Command::new(find_pg_bin().join("initdb"))
                    .args([
                        "--data-checksums",
                        "--auth=trust",
                        "--no-instructions",
                        "-D",
                    ])
                    .arg(initialized.data_dir())
                    .env("LC_ALL", "C")
                    .output()
                    .await
                    .unwrap();
                assert!(
                    output.status.success(),
                    "{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                let conf = std::fs::read(initialized.data_dir().join("postgresql.conf")).unwrap();
                assert!(
                    !String::from_utf8_lossy(&conf).contains("# --- kuberic required settings ---")
                );
                if stage == "raw-initdb-data" {
                    use tokio::io::AsyncWriteExt;
                    // Single-user mode adds a data-loss oracle without installing
                    // managed settings or exposing a listener.
                    let mut child = tokio::process::Command::new(find_pg_bin().join("postgres"))
                        .arg("--single")
                        .arg("-D")
                        .arg(initialized.data_dir())
                        .arg("postgres")
                        .stdin(std::process::Stdio::piped())
                        .stdout(std::process::Stdio::piped())
                        .stderr(std::process::Stdio::piped())
                        .kill_on_drop(true)
                        .spawn()
                        .unwrap();
                    child.stdin.take().unwrap().write_all(
                        b"CREATE TABLE raw_initdb_receipt(id int);\nINSERT INTO raw_initdb_receipt VALUES (42);\n"
                    ).await.unwrap();
                    let output = child.wait_with_output().await.unwrap();
                    assert!(
                        output.status.success(),
                        "{}",
                        String::from_utf8_lossy(&output.stderr)
                    );
                    assert_eq!(
                        std::fs::read(initialized.data_dir().join("postgresql.conf")).unwrap(),
                        conf
                    );
                }
            } else {
                initialized.init_db().await.unwrap();
            }
            expected_system = Some(initialized.control_identity().await.unwrap().0);
        }
        let mut retry = HostAttempt::start(root.path()).await;
        let mut running = retry.ready().await;
        assert!(
            !store
                .load_state()
                .await
                .unwrap()
                .application_storage
                .unwrap()
                .initializing
        );
        if let Some(expected) = expected_system {
            assert_eq!(
                retry
                    .application
                    .instance()
                    .control_identity()
                    .await
                    .unwrap()
                    .0,
                expected,
                "retry must not replace completed initdb"
            );
        }
        assert!(
            retry
                .application
                .instance()
                .connect_application()
                .await
                .is_err()
        );
        if stage == "raw-initdb-data" {
            let (sql, _) = retry.application.instance().connect().await.unwrap();
            assert_eq!(
                sql.query_one("SELECT id FROM raw_initdb_receipt", &[])
                    .await
                    .unwrap()
                    .get::<_, i32>(0),
                42
            );
        }
        if raw {
            let (sql, _) = retry.application.instance().connect().await.unwrap();
            assert_eq!(
                sql.query_one("SHOW port", &[])
                    .await
                    .unwrap()
                    .get::<_, String>(0),
                port.to_string()
            );
            assert_eq!(
                sql.query_one("SHOW unix_socket_directories", &[])
                    .await
                    .unwrap()
                    .get::<_, String>(0),
                retry.application.instance().socket_dir().to_str().unwrap()
            );
        }
        let (mut client, report) = status(retry.address).await;
        client
            .execute(configure(
                &report.process_session_id,
                "retry-install",
                false,
            ))
            .await
            .unwrap();
        client
            .execute(configure(&report.process_session_id, "retry-grant", true))
            .await
            .unwrap();
        let (sql, _) = retry
            .application
            .instance()
            .connect_application()
            .await
            .unwrap();
        sql.batch_execute(
            "CREATE TABLE retry_receipt(id int); INSERT INTO retry_receipt VALUES (1)",
        )
        .await
        .unwrap();
        stop(&mut running, &retry.application).await;
        if raw {
            let conf = std::fs::read(root.path().join("pgdata/postgresql.conf")).unwrap();
            drop(running);
            drop(retry);
            let mut reopened = HostAttempt::start(root.path()).await;
            let mut running = reopened.ready().await;
            assert_eq!(
                std::fs::read(root.path().join("pgdata/postgresql.conf")).unwrap(),
                conf
            );
            let (sql, _) = reopened
                .application
                .instance()
                .connect_application()
                .await
                .unwrap();
            assert_eq!(
                sql.query_one("SELECT id FROM retry_receipt", &[])
                    .await
                    .unwrap()
                    .get::<_, i32>(0),
                1
            );
            stop(&mut running, &reopened.application).await;
        }
    }
}

async fn orphaned_postgres_without_agent_metadata_is_unsafe_and_untouched() {
    let root = TestDataDir::new("host-orphan");
    let instance = PgInstanceManager::new(
        root.path().join("pgdata"),
        find_pg_bin(),
        allocate_port().await,
    );
    instance.init_db().await.unwrap();
    let before = files(root.path());
    let host = HostAttempt::start(root.path()).await;
    let (mut client, report) = status(host.address).await;
    assert_eq!(report.storage_state, wire::AgentStorageState::Unsafe as i32);
    assert_eq!(
        client
            .execute(initialize(&report.process_session_id))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(files(root.path()), before);
    assert!(!host.application.instance().is_running().await);
}

async fn established_application_identity_mismatch_rejects_before_pgdata_mutation() {
    let root = TestDataDir::new("host-mismatch");
    let mut host = HostAttempt::start(root.path()).await;
    let mut running = host.bootstrap(root.path()).await;
    stop(&mut running, &host.application).await;
    drop(running);
    drop(host);
    let durable = PgDurableStore::open(
        root.path().join("application"),
        PgDurableIdentity {
            resource_uid: ResourceUid::new("postgres-test"),
            replica: identity(),
        },
        StorageMode::Established,
    )
    .await
    .unwrap();
    // Write a valid envelope with another identity using a separate store, not corrupt JSON.
    let other = TestDataDir::new("other-identity");
    let mut mismatched = identity();
    mismatched.instance_id = ReplicaInstanceId::new("another-process");
    let other_store = PgDurableStore::open(
        other.path(),
        PgDurableIdentity {
            resource_uid: ResourceUid::new("postgres-test"),
            replica: mismatched,
        },
        StorageMode::Fresh,
    )
    .await
    .unwrap();
    let saved = durable.snapshot().await;
    other_store
        .update(|state| {
            let identity = state.identity.clone();
            *state = saved;
            state.identity = identity;
            Ok(())
        })
        .await
        .unwrap();
    std::fs::copy(
        other.path().join("state-v2.json"),
        root.path().join("application/state-v2.json"),
    )
    .unwrap();
    let before = files(&root.path().join("pgdata"));
    let mut rejected = HostAttempt::start(root.path()).await;
    let error = tokio::time::timeout(Duration::from_secs(10), &mut rejected.task)
        .await
        .unwrap()
        .unwrap()
        .err()
        .unwrap();
    assert!(error.to_string().contains("identity mismatch"), "{error}");
    assert_eq!(files(&root.path().join("pgdata")), before);
    assert!(!rejected.application.instance().is_running().await);
}

async fn unsupported_coordination_rpcs_preserve_every_pgdata_byte() {
    let root = TestDataDir::new("host-unsupported");
    let instance = PgInstanceManager::new(
        root.path().join("pgdata"),
        find_pg_bin(),
        allocate_port().await,
    );
    instance.init_db().await.unwrap();
    let before = files(root.path());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let cancellation = tokio_util::sync::CancellationToken::new();
    let stopped = cancellation.clone();
    let service = Arc::new(PgService::deferred(PgServiceConfig {
        resource_uid: ResourceUid::new("postgres-test"),
        application_root: root.path().join("application"),
        pg_data: instance.data_dir().to_path_buf(),
        pg_bin: find_pg_bin(),
        pg_port: instance.port(),
        replication_address: format!("http://{address}"),
    }));
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(PgDataServiceImpl::new(service, TOKEN.into()).into_server())
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                stopped.cancelled_owned(),
            )
            .await
            .unwrap();
    });
    let mut client = PgDataServiceClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    assert_eq!(
        client
            .build(pgwire::NativeBuildRequest::default())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        client
            .inspect_source(pgwire::NativeBuildRequest::default())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(files(root.path()), before);
    cancellation.cancel();
    server.await.unwrap();
}

async fn previously_promoted_database_restarts_closed_until_authority_restoration() {
    let root = TestDataDir::new("host-promoted");
    let mut initial = HostAttempt::start(root.path()).await;
    let mut running = initial.bootstrap(root.path()).await;
    stop(&mut running, &initial.application).await;
    let port = initial.application.instance().port();
    drop(running);
    drop(initial);

    // Model pre-existing promoted storage using real physical recovery, not a fabricated timeline.
    let source_root = TestDataDir::new("promotion-source");
    let source = PgInstanceManager::new(
        source_root.path().join("pgdata"),
        find_pg_bin(),
        allocate_port().await,
    );
    let (faults, _rx) = tokio::sync::mpsc::channel(8);
    source.init_db().await.unwrap();
    source.start_native(faults.clone()).await.unwrap();
    postgres_replicated::access::initialize_application_role(&source)
        .await
        .unwrap();
    std::fs::remove_dir_all(root.path().join("pgdata")).unwrap();
    let promoted = Arc::new(PgInstanceManager::new(
        root.path().join("pgdata"),
        find_pg_bin(),
        port,
    ));
    promoted
        .base_backup(source.listen_host(), source.port())
        .await
        .unwrap();
    promoted
        .config()
        .patch_after_clone_exact(promoted.data_dir(), "promotion_fixture")
        .await
        .unwrap();
    promoted.start_native(faults).await.unwrap();
    promoted.promote().await.unwrap();
    let (sql, _) = promoted.connect().await.unwrap();
    sql.batch_execute("CHECKPOINT").await.unwrap();
    // The fixture installs the already-promoted database's exact durable evidence.
    std::fs::remove_dir_all(root.path().join("application")).unwrap();
    let durable = Arc::new(
        PgDurableStore::open(
            root.path().join("application"),
            PgDurableIdentity {
                resource_uid: ResourceUid::new("postgres-test"),
                replica: identity(),
            },
            StorageMode::Fresh,
        )
        .await
        .unwrap(),
    );
    let evidence = PgNativeObserver::with_store(promoted.clone(), durable)
        .await
        .snapshot_and_persist()
        .await
        .unwrap()
        .evidence
        .unwrap();
    assert!(evidence.timeline_id > 1);
    assert!(!evidence.in_recovery);
    promoted.stop().await.unwrap();
    source.stop().await.unwrap();
    drop(promoted);

    let mut host = HostAttempt::start(root.path()).await;
    let mut running = host.ready().await;
    assert!(
        host.application
            .instance()
            .connect_application()
            .await
            .is_err()
    );
    host.application
        .change_role(ReplicaRole::Primary)
        .await
        .unwrap();
    assert!(
        host.application
            .instance()
            .connect_application()
            .await
            .is_err()
    );
    let (mut client, report) = status(host.address).await;
    client
        .execute(configure(
            &report.process_session_id,
            "install-promoted-singleton",
            false,
        ))
        .await
        .unwrap();
    assert!(
        host.application
            .instance()
            .connect_application()
            .await
            .is_err()
    );
    client
        .execute(configure(
            &report.process_session_id,
            "accept-promoted-singleton",
            true,
        ))
        .await
        .unwrap();
    let (sql, _) = host
        .application
        .instance()
        .connect_application()
        .await
        .unwrap();
    sql.batch_execute(
        "CREATE TABLE promoted_receipt(id int); INSERT INTO promoted_receipt VALUES (1)",
    )
    .await
    .unwrap();
    stop(&mut running, &host.application).await;
}

async fn unsupported_native_callbacks_and_mutated_evidence_never_change_pgdata() {
    use kuberic_protocol::types::{BuildAuthority, BuildAuthorityKind, OperationId};
    let root = TestDataDir::new("host-callbacks");
    let mut host = HostAttempt::start(root.path()).await;
    let mut running = host.bootstrap(root.path()).await;
    let driver = host.application.native_driver().clone();
    stop(&mut running, &host.application).await;
    let before = files(root.path());
    let mut target = identity();
    target.replica_id = ReplicaId::new(2);
    target.instance_id = ReplicaInstanceId::new("postgres-2");
    let build = BuildAuthority {
        build_id: OperationId::new("unsupported-build"),
        kind: BuildAuthorityKind::Provisioning,
        source: identity(),
        target: target.clone(),
        current_configuration: configuration(),
        replication_boundary_lsn: 0,
    };
    build.validate().unwrap();
    assert!(
        driver
            .build_replica(kuberic_runtime::replicator::ReplicaInformation::new(
                build.build_id.clone(),
                target.clone(),
                String::new(),
            ))
            .await
            .is_err()
    );
    assert!(
        driver
            .wait_for_catch_up_quorum(ReplicaSetQuorumMode::WriteQuorum)
            .await
            .is_err()
    );
    assert!(driver.remove_replica(target.replica_id).await.is_err());
    assert!(driver.on_data_loss().await.is_err());
    assert!(
        driver
            .update_catch_up_replica_set_configuration(
                configuration().into(),
                configuration().into()
            )
            .await
            .is_err()
    );
    let larger = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        ReplicaId::new(1),
        vec![
            ConfigurationMember {
                identity: identity(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: target,
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    );
    assert!(
        driver
            .update_current_replica_set_configuration(larger.into())
            .await
            .is_err()
    );
    assert_eq!(files(root.path()), before);

    let metadata = root.path().join("application/state-v2.json");
    std::fs::write(&metadata, b"corrupt evidence").unwrap();
    let pgdata = files(&root.path().join("pgdata"));
    assert!(driver.current_progress().await.is_err());
    assert!(driver.current_progress().await.is_err());
    assert_eq!(files(&root.path().join("pgdata")), pgdata);
    assert_eq!(std::fs::read(metadata).unwrap(), b"corrupt evidence");
}

struct BinaryHost {
    child: std::process::Child,
    pgdata: PathBuf,
    pgport: u16,
    control: SocketAddr,
    replication: SocketAddr,
    coordination: SocketAddr,
    log: PathBuf,
}

async fn established_agent_and_postgres_lineage_mismatches_are_non_mutating() {
    let root = TestDataDir::new("host-lineage");
    let mut host = HostAttempt::start(root.path()).await;
    let mut running = host.bootstrap(root.path()).await;
    stop(&mut running, &host.application).await;
    drop(running);
    drop(host);
    let before = files(root.path());
    let mut wrong_agent = HostAttempt::start_as(root.path(), "another-incarnation").await;
    assert!(
        tokio::time::timeout(Duration::from_secs(10), &mut wrong_agent.task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert_eq!(files(root.path()), before);
    drop(wrong_agent);

    std::fs::remove_dir_all(root.path().join("pgdata")).unwrap();
    let replacement = PgInstanceManager::new(
        root.path().join("pgdata"),
        find_pg_bin(),
        allocate_port().await,
    );
    replacement.init_db().await.unwrap();
    let before = (
        files(&root.path().join("application")),
        files(&root.path().join("pgdata")),
    );
    let mut wrong_database = HostAttempt::start(root.path()).await;
    let error = tokio::time::timeout(Duration::from_secs(10), &mut wrong_database.task)
        .await
        .unwrap()
        .unwrap()
        .err()
        .unwrap();
    assert!(
        error
            .to_string()
            .contains("system identity/timeline mismatch"),
        "{error}"
    );
    assert_eq!(
        (
            files(&root.path().join("application")),
            files(&root.path().join("pgdata")),
        ),
        before
    );
    assert_eq!(
        SqliteStore::open_existing(SqliteStore::metadata_database_path(root.path()), None)
            .unwrap()
            .load_state()
            .await
            .unwrap()
            .reported_fault,
        Some(FaultType::Permanent)
    );
    assert!(!wrong_database.application.instance().is_running().await);
}

async fn unsafe_evidence_reports_permanent_fault_and_stops_granted_sql() {
    use kuberic_agent::{sqlite_store::SqliteStore, store::AgentStore};
    use kuberic_protocol::types::FaultType;
    let root = TestDataDir::new("host-unsafe");
    let mut host = HostAttempt::start(root.path()).await;
    let mut running = host.bootstrap(root.path()).await;
    let (mut client, report) = status(host.address).await;
    client
        .execute(configure(
            &report.process_session_id,
            "install-before-corruption",
            false,
        ))
        .await
        .unwrap();
    client
        .execute(configure(
            &report.process_session_id,
            "grant-before-corruption",
            true,
        ))
        .await
        .unwrap();
    let (sql, _) = host
        .application
        .instance()
        .connect_application()
        .await
        .unwrap();
    sql.batch_execute("CREATE TABLE safe_receipt(id int); INSERT INTO safe_receipt VALUES (1)")
        .await
        .unwrap();
    let metadata = root.path().join("application/state-v2.json");
    let saved = std::fs::read(&metadata).unwrap();
    std::fs::write(&metadata, b"invalid evidence").unwrap();
    let driver = host.application.native_driver();
    assert!(driver.current_progress().await.is_err());
    assert!(
        sql.simple_query("INSERT INTO safe_receipt VALUES (2)")
            .await
            .is_err()
    );
    assert!(!host.application.instance().is_running().await);
    let store =
        SqliteStore::open_existing(SqliteStore::metadata_database_path(root.path()), None).unwrap();
    running.shutdown();
    tokio::time::timeout(Duration::from_secs(10), running.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store.load_state().await.unwrap().reported_fault,
        Some(FaultType::Permanent),
        "shutdown must persist the fault without polling a status RPC"
    );
    let error = tokio::time::timeout(Duration::from_secs(5), driver.current_progress())
        .await
        .expect("stopped fault consumer must not deadlock the driver")
        .unwrap_err();
    assert!(
        matches!(error, kuberic_runtime::RuntimeError::OperationCancelled)
            || error.to_string().contains("fault acknowledgement"),
        "{error}"
    );
    std::fs::write(metadata, saved).unwrap();
    host.application.close().await.unwrap();
}

impl Drop for BinaryHost {
    fn drop(&mut self) {
        if self.child.try_wait().unwrap().is_none() {
            let _ = std::process::Command::new("kill")
                .args(["-TERM", &self.child.id().to_string()])
                .status();
            for _ in 0..100 {
                if self.child.try_wait().unwrap().is_some() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            if self.child.try_wait().unwrap().is_none() {
                let _ = self.child.kill();
            }
            let _ = self.child.wait();
        }
        if self.pgdata.join("postmaster.pid").exists() {
            let _ = std::process::Command::new(find_pg_bin().join("pg_ctl"))
                .args([
                    "stop",
                    "-D",
                    self.pgdata.to_str().unwrap(),
                    "-m",
                    "immediate",
                    "-w",
                    "-t",
                    "5",
                ])
                .output();
        }
    }
}

async fn start_binary(root: &Path) -> (BinaryHost, AgentControlClient<Channel>) {
    start_binary_with(root, &find_pg_bin()).await
}

async fn start_binary_with(
    root: &Path,
    pg_bin: &Path,
) -> (BinaryHost, AgentControlClient<Channel>) {
    let binary = spawn_binary(root, pg_bin).await;
    initialize_binary(&binary, root).await;
    let client = grant_binary(&binary).await;
    (binary, client)
}

async fn grant_binary(binary: &BinaryHost) -> AgentControlClient<Channel> {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let (mut client, report) = status(binary.control).await;
            if report.storage_state != wire::AgentStorageState::Uninitialized as i32 {
                match client
                    .execute(configure(
                        &report.process_session_id,
                        "binary-install",
                        false,
                    ))
                    .await
                {
                    Ok(_) => {
                        client
                            .execute(configure(&report.process_session_id, "binary-grant", true))
                            .await
                            .unwrap();
                        break client;
                    }
                    Err(error) if error.code() == tonic::Code::Unavailable => {}
                    Err(error) => panic!("binary configuration: {error}"),
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|error| panic!("binary did not grant access: {error}: {}", binary.output()))
}

async fn spawn_binary(root: &Path, pg_bin: &Path) -> BinaryHost {
    let control = format!("127.0.0.1:{}", allocate_port().await);
    let replication = format!("127.0.0.1:{}", allocate_port().await);
    let coordination = format!("127.0.0.1:{}", allocate_port().await);
    let pgport = allocate_port().await;
    let log = root.join("binary.log");
    let output = std::fs::File::create(&log).unwrap();
    let child = std::process::Command::new(env!("CARGO_BIN_EXE_postgres-replicated"))
        .current_dir(root)
        .args([
            "--resource-uid",
            "postgres-test",
            "--replica-id",
            "1",
            "--pod-uid",
            "postgres-1",
            "--pvc-uid",
            "postgres-pvc-1",
            "--bearer-token",
            TOKEN,
            "--data-root",
            SINGLE_REPLICA_DIRECTORY,
            "--pg-bin",
            pg_bin.to_str().unwrap(),
            "--pg-port",
            &pgport.to_string(),
            "--control-address",
            &control,
            "--replication-address",
            &replication,
            "--application-address",
            &coordination,
            "--control-endpoint",
            &format!("http://{control}"),
            "--replication-endpoint",
            &format!("http://{replication}"),
        ])
        .stdout(output.try_clone().unwrap())
        .stderr(output)
        .process_group(0)
        .spawn()
        .unwrap();
    BinaryHost {
        child,
        pgdata: root.join(SINGLE_REPLICA_DIRECTORY).join("pgdata"),
        pgport,
        control: control.parse().unwrap(),
        replication: replication.parse().unwrap(),
        coordination: coordination.parse().unwrap(),
        log,
    }
}

async fn initialize_binary(binary: &BinaryHost, root: &Path) {
    let (mut client, waiting) = status(binary.control).await;
    assert!(!root.join(SINGLE_REPLICA_DIRECTORY).exists());
    client
        .execute(initialize(&waiting.process_session_id))
        .await
        .unwrap();
}

struct HelperGate {
    bin: PathBuf,
    entered: PathBuf,
    leaf: PathBuf,
    release: PathBuf,
    armed: PathBuf,
}

impl HelperGate {
    fn new(root: &Path, command: &str, stage: &str, daemon: bool) -> Self {
        let entered = root.join("helper-entered");
        let leaf = root.join("helper-leaf");
        let release = root.join("helper-release");
        let armed = root.join("helper-armed");
        let script = root.join("helper-gate.sh");
        let pgdata = root.join(SINGLE_REPLICA_DIRECTORY).join("pgdata");
        let real = find_pg_bin().join(command);
        let preparation = match stage {
            "partial" => format!(
                "mkdir -p '{}'\nprintf partial > '{}/PG_VERSION'\n",
                pgdata.display(),
                pgdata.display()
            ),
            "complete" => format!("'{}' \"$@\" || exit $?\n", real.display()),
            _ => String::new(),
        };
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\n\
                 trap '' INT TERM QUIT\n\
                 {preparation}\
                 setsid sh -c 'trap \"\" INT TERM QUIT; echo $$ > \"{leaf}\"; \
                 while [ ! -e \"{release}\" ]; do sleep 0.02; done; \
                 mkdir -p \"{pgdata}\"; echo escaped > \"{pgdata}/late-descendant\"' &\n\
                 echo $$ > '{entered}'\n\
                 while [ ! -e '{release}' ]; do sleep 0.02; done\n\
                 {finish}\n",
                leaf = leaf.display(),
                release = release.display(),
                pgdata = pgdata.display(),
                entered = entered.display(),
                finish = if stage == "complete" {
                    "exit 0".into()
                } else {
                    format!("exec '{}' \"$@\"", real.display())
                },
            ),
        )
        .unwrap();
        let bin = wrapped_pg_bin(
            root,
            command,
            &format!(
                "#!/bin/sh\n\
                 if [ ! -e '{}' ]; then exec '{}' \"$@\"; fi\n\
                 {} sh '{}' \"$@\"{}\n",
                armed.display(),
                real.display(),
                if daemon { "setsid" } else { "exec" },
                script.display(),
                if daemon { " &\nexit 0" } else { "" },
            ),
        );
        Self {
            bin,
            entered,
            leaf,
            release,
            armed,
        }
    }

    fn arm(&self) {
        std::fs::write(&self.armed, b"").unwrap();
    }

    async fn wait(&self, binary: &mut BinaryHost) -> u32 {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                assert!(
                    binary.child.try_wait().unwrap().is_none(),
                    "{}",
                    binary.output()
                );
                let pid = std::fs::read_to_string(&self.entered)
                    .ok()
                    .and_then(|text| text.trim().parse::<u32>().ok());
                if let Some(pid) = pid
                    && self.leaf.exists()
                {
                    let parent = process_parent_and_group(pid).0;
                    let (owner, group) = process_parent_and_group(parent);
                    if owner == binary.child.id() && group == parent {
                        return parent;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|error| panic!("helper gate: {error}: {}", binary.output()))
    }

    async fn assert_no_late_mutation(&self, binary: &BinaryHost) {
        let before = files(&binary.pgdata);
        std::fs::write(&self.release, b"").unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(files(&binary.pgdata), before, "post-exit PGDATA mutation");
        assert!(!binary.pgdata.join("late-descendant").exists());
    }
}

async fn executable_owned_helpers_cancel_reap_and_retry() {
    for (command, stage, daemon) in [
        ("initdb", "empty", false),
        ("initdb", "empty", true),
        ("initdb", "partial", true),
        ("initdb", "complete", true),
        ("pg_controldata", "startup", false),
        ("pg_controldata", "startup", true),
        ("pg_isready", "startup", false),
        ("pg_isready", "startup", true),
        ("pg_isready", "health", true),
    ] {
        let root = TestDataDir::new("helper-cancel");
        let gate = HelperGate::new(root.path(), command, stage, daemon);
        let mut binary = if stage == "health" {
            start_binary_with(root.path(), &gate.bin).await.0
        } else {
            gate.arm();
            let binary = spawn_binary(root.path(), &gate.bin).await;
            initialize_binary(&binary, root.path()).await;
            binary
        };
        gate.arm();
        let supervisor = ProcessProbe::process(gate.wait(&mut binary).await);
        let descendants = ProcessProbe::descendants(binary.child.id());
        assert!(
            descendants.len() >= 3,
            "supervisor, helper and setsid child"
        );
        let mut unrelated = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap();
        let unrelated_probe = ProcessProbe::process(unrelated.id());
        binary.signal(
            if daemon {
                rustix::process::Signal::TERM
            } else {
                rustix::process::Signal::INT
            },
            daemon,
        );
        let exit = binary.wait().await;
        assert!(
            exit.success(),
            "{command}/{stage}/{daemon}: {exit}: {}",
            binary.output()
        );
        supervisor.assert_reaped();
        descendants.assert_reaped();
        assert!(unrelated.try_wait().unwrap().is_none());
        unrelated_probe.signal(rustix::process::Signal::TERM);
        unrelated.wait().unwrap();
        gate.assert_no_late_mutation(&binary).await;
        binary.assert_stopped().await;
        if command == "initdb" && stage == "empty" {
            assert!(!binary.pgdata.join("PG_VERSION").exists());
            assert!(!binary.pgdata.join("global/pg_control").exists());
        }

        if stage == "health" {
            continue;
        }
        let before = files(&binary.pgdata);
        let expected_system = if stage != "partial" && stage != "empty" {
            Some(
                PgInstanceManager::new(binary.pgdata.clone(), find_pg_bin(), binary.pgport)
                    .control_identity()
                    .await
                    .unwrap()
                    .0,
            )
        } else {
            None
        };
        let mut retry = spawn_binary(root.path(), &find_pg_bin()).await;
        if stage == "partial" {
            let exit = retry.wait().await;
            assert!(!exit.success(), "{exit}: {}", retry.output());
            assert!(
                retry
                    .output()
                    .contains("cannot inspect PostgreSQL control data"),
                "{}",
                retry.output()
            );
            assert_eq!(
                files(&retry.pgdata),
                before,
                "partial initdb must fail closed"
            );
            retry.assert_stopped().await;
            continue;
        }
        grant_binary(&retry).await;
        let instance = PgInstanceManager::new(retry.pgdata.clone(), find_pg_bin(), retry.pgport);
        if let Some(expected) = expected_system {
            assert_eq!(instance.control_identity().await.unwrap().0, expected);
        }
        let (sql, _) = instance.connect_application().await.unwrap();
        assert_eq!(
            sql.query_one("SELECT 42::int", &[])
                .await
                .unwrap()
                .get::<_, i32>(0),
            42
        );
        let descendants = ProcessProbe::descendants(retry.child.id());
        retry.terminate();
        let exit = retry.wait().await;
        assert!(exit.success(), "{exit}: {}", retry.output());
        descendants.assert_reaped();
        retry.assert_stopped().await;
    }
}

async fn executable_owned_helpers_root_loss_is_failure() {
    const HELPER: &str = "KUBERIC_TEST_HELPER_ROOT_LOSS";
    if std::env::var_os(HELPER).is_none() {
        let output = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::executable_owned_helpers_root_loss_is_failure",
                "--nocapture",
            ])
            .env(HELPER, "1")
            .kill_on_drop(true)
            .output()
            .await
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        return;
    }
    rustix::process::set_child_subreaper(Some(rustix::process::getpid())).unwrap();
    struct Adopted(ProcessProbe);
    impl Drop for Adopted {
        fn drop(&mut self) {
            self.0
                .reap_adopted()
                .expect("reap lost helper root descendants");
        }
    }
    for command in ["initdb", "pg_controldata", "pg_isready"] {
        let root = TestDataDir::new("helper-loss");
        let gate = HelperGate::new(root.path(), command, "startup", true);
        gate.arm();
        let mut binary = spawn_binary(root.path(), &gate.bin).await;
        initialize_binary(&binary, root.path()).await;
        let supervisor = ProcessProbe::process(gate.wait(&mut binary).await);
        let descendants = Adopted(ProcessProbe::descendants(binary.child.id()));
        assert!(descendants.0.len() >= 3);
        supervisor.signal(rustix::process::Signal::KILL);
        supervisor.assert_exited().await;
        binary.terminate();
        let exit = binary.wait().await;
        assert!(!exit.success(), "{command}: {exit}: {}", binary.output());
        assert!(
            binary.output().contains("supervisor exited unexpectedly")
                && binary.output().contains("descendant reaping unproven")
                && binary.output().contains("Error:"),
            "{}",
            binary.output()
        );
        supervisor.assert_reaped();
        descendants.0.reap_adopted().unwrap();
        descendants.0.assert_reaped();
        gate.assert_no_late_mutation(&binary).await;
    }
}

async fn delayed_readiness_binary(root: &Path) -> BinaryHost {
    let marker = root.join("readiness-entered");
    let pg_bin = wrapped_pg_bin(
        root,
        "pg_isready",
        &format!(
            "#!/bin/sh\nprintf '%s' $$ > '{}'\nexec sleep 30\n",
            marker.display()
        ),
    );
    let mut binary = spawn_binary(root, &pg_bin).await;
    initialize_binary(&binary, root).await;
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            assert!(
                binary.child.try_wait().unwrap().is_none(),
                "{}",
                binary.output()
            );
            if marker.exists()
                && std::process::Command::new(find_pg_bin().join("pg_isready"))
                    .args([
                        "-h",
                        binary.pgdata.join("pg_stat_tmp").to_str().unwrap(),
                        "-p",
                        &binary.pgport.to_string(),
                    ])
                    .output()
                    .unwrap()
                    .status
                    .success()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|error| panic!("readiness gate not reached: {error}: {}", binary.output()));
    assert!(
        !binary.output().contains("PostgreSQL started"),
        "{}",
        binary.output()
    );
    assert!(
        tokio::net::TcpStream::connect(binary.coordination)
            .await
            .is_err(),
        "coordination is deliberately unavailable until host readiness"
    );
    binary
}

async fn executable_pre_readiness_signals_join_startup_and_reap() {
    for signal in [rustix::process::Signal::INT, rustix::process::Signal::TERM] {
        let root = TestDataDir::new("pre-init");
        let mut binary = spawn_binary(root.path(), &find_pg_bin()).await;
        let (client, _) = status(binary.control).await;
        drop(client);
        binary.signal(signal, true);
        let exit = binary.wait().await;
        assert!(exit.success(), "{exit}: {}", binary.output());
        assert!(!root.path().join(SINGLE_REPLICA_DIRECTORY).exists());
        binary.assert_stopped().await;

        let root = TestDataDir::new("pre-signal");
        let mut binary = delayed_readiness_binary(root.path()).await;
        let supervisor = ProcessProbe::process(supervisor_pid(&binary));
        let descendants = ProcessProbe::descendants(binary.child.id());
        assert!(
            descendants.len() >= 7,
            "include readiness command and PostgreSQL"
        );
        binary.signal(signal, true);
        let exit = binary.wait().await;
        assert!(exit.success(), "{exit}: {}", binary.output());
        assert!(
            !binary.output().contains("PostgreSQL started"),
            "{}",
            binary.output()
        );
        assert!(
            !binary.output().contains("cleanup failed"),
            "{}",
            binary.output()
        );
        descendants.assert_reaped();
        supervisor.assert_reaped();
        binary.assert_stopped().await;
    }
}

async fn executable_pre_readiness_startup_error_reaps() {
    let root = TestDataDir::new("pre-error");
    let mut binary = delayed_readiness_binary(root.path()).await;
    let descendants = ProcessProbe::descendants(binary.child.id());
    assert!(descendants.len() >= 7);
    let exit = binary.wait().await;
    assert!(!exit.success(), "{exit}: {}", binary.output());
    assert!(
        binary.output().contains("pg_isready command timeout"),
        "{}",
        binary.output()
    );
    descendants.assert_reaped();
    binary.assert_stopped().await;
}

async fn executable_pre_readiness_supervisor_loss_never_succeeds() {
    const HELPER: &str = "KUBERIC_TEST_PRE_READINESS_ROOT_LOSS_HELPER";
    if std::env::var_os(HELPER).is_none() {
        let output = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::executable_pre_readiness_supervisor_loss_never_succeeds",
                "--nocapture",
            ])
            .env(HELPER, "1")
            .kill_on_drop(true)
            .output()
            .await
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        return;
    }
    rustix::process::set_child_subreaper(Some(rustix::process::getpid())).unwrap();
    struct Adopted(ProcessProbe);
    impl Drop for Adopted {
        fn drop(&mut self) {
            self.0
                .reap_adopted()
                .expect("reap isolated root-loss descendants");
        }
    }
    for signal in [rustix::process::Signal::INT, rustix::process::Signal::TERM] {
        let root = TestDataDir::new("pre-loss");
        let mut binary = delayed_readiness_binary(root.path()).await;
        let supervisor = ProcessProbe::process(supervisor_pid(&binary));
        let descendants = Adopted(ProcessProbe::descendants(binary.child.id()));
        assert!(descendants.0.len() >= 7);
        supervisor.signal(rustix::process::Signal::KILL);
        supervisor.assert_exited().await;
        binary.signal(signal, true);
        let exit = binary.wait().await;
        assert!(!exit.success(), "{exit}: {}", binary.output());
        assert!(
            !binary.output().contains("PostgreSQL started"),
            "{}",
            binary.output()
        );
        assert!(
            binary.output().contains("supervisor exited unexpectedly")
                && binary.output().contains("descendant reaping unproven")
                && binary.output().contains("Error:"),
            "cleanup failure must reach the executable result, not just a drop log: {}",
            binary.output()
        );
        supervisor.assert_reaped();
        // Before readiness the lost root can orphan identities not yet retained by
        // the host. Only this isolated test subreaper can prove their final reaping.
        descendants.0.reap_adopted().unwrap();
        descendants.0.assert_reaped();
    }
}

impl BinaryHost {
    fn terminate(&self) {
        self.signal(rustix::process::Signal::TERM, false);
    }

    fn signal(&self, signal: rustix::process::Signal, group: bool) {
        use rustix::process::{Pid, kill_process, kill_process_group};
        let pid = Pid::from_raw(self.child.id() as i32).unwrap();
        if group {
            kill_process_group(pid, signal).unwrap();
        } else {
            kill_process(pid, signal).unwrap();
        }
    }

    async fn wait(&mut self) -> std::process::ExitStatus {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Some(exit) = self.child.try_wait().unwrap() {
                    break exit;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("binary did not exit: {}", self.output()))
    }

    fn output(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap()
    }

    async fn assert_stopped(&self) {
        assert!(!self.pgdata.join("postmaster.pid").exists());
        for address in [self.control, self.replication, self.coordination] {
            assert!(tokio::net::TcpStream::connect(address).await.is_err());
        }
        assert!(
            tokio::net::TcpStream::connect(("127.0.0.1", self.pgport))
                .await
                .is_err()
        );
    }
}

fn process_parent_and_group(pid: u32) -> (u32, u32) {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    let fields: Vec<_> = stat
        .rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .collect();
    (fields[1].parse().unwrap(), fields[2].parse().unwrap())
}

fn supervisor_pid(binary: &BinaryHost) -> u32 {
    let postmaster = std::fs::read_to_string(binary.pgdata.join("postmaster.pid"))
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let supervisor = process_parent_and_group(postmaster).0;
    let (parent, group) = process_parent_and_group(supervisor);
    assert_eq!(parent, binary.child.id());
    assert_eq!(group, supervisor, "ownership root needs its own group");
    assert_eq!(
        process_parent_and_group(binary.child.id()).1,
        binary.child.id()
    );
    supervisor
}

async fn executable_shutdown_signals_preserve_supervisor_reaping() {
    use rustix::process::Signal;
    let unrelated_dir = TestDataDir::new("signal-unrelated");
    let unrelated = PgInstanceManager::new(
        unrelated_dir.path().join("pgdata"),
        find_pg_bin(),
        allocate_port().await,
    );
    unrelated.init_db().await.unwrap();
    let (faults, mut unrelated_faults) = tokio::sync::mpsc::channel(8);
    unrelated.start_native(faults).await.unwrap();
    let unrelated_probe = ProcessProbe::postgres(unrelated.data_dir());
    let (unrelated_sql, _) = unrelated.connect().await.unwrap();
    unrelated_sql
        .batch_execute("CREATE TABLE isolation_receipt(id int)")
        .await
        .unwrap();

    for detached_launcher in [false, true] {
        for signal in [Signal::INT, Signal::TERM] {
            for target in ["host-pid", "host-group", "supervisor-pid"] {
                eprintln!("signal={signal:?}, target={target}, detached={detached_launcher}");
                let root = TestDataDir::new("host-signal");
                let pg_bin = if detached_launcher {
                    wrapped_pg_bin(
                        root.path(),
                        "postgres",
                        &format!(
                            "#!/bin/sh\nsetsid sh -c '\"{}/postgres\" \"$@\" &' sh \"$@\" &\n",
                            find_pg_bin().display()
                        ),
                    )
                } else {
                    find_pg_bin()
                };
                let (mut binary, control) = start_binary_with(root.path(), &pg_bin).await;
                drop(control);
                let instance =
                    PgInstanceManager::new(binary.pgdata.clone(), find_pg_bin(), binary.pgport);
                let (sql, connection) = instance.connect_application().await.unwrap();
                sql.batch_execute(
                    "CREATE TABLE signal_receipt(id int); INSERT INTO signal_receipt VALUES (1)",
                )
                .await
                .unwrap();
                let supervisor = ProcessProbe::process(supervisor_pid(&binary));
                let descendants = ProcessProbe::descendants(binary.child.id());
                assert!(
                    descendants.len() >= 7,
                    "include supervisor, postmaster and SQL backend"
                );
                let mut unrelated_child = std::process::Command::new("sh")
                    .args(["-c", "exit 23"])
                    .spawn()
                    .unwrap();
                if target == "supervisor-pid" {
                    for _ in 0..4 {
                        supervisor.signal(signal);
                    }
                    tokio::time::sleep(Duration::from_millis(600)).await;
                    assert!(
                        binary.child.try_wait().unwrap().is_none(),
                        "{}",
                        binary.output()
                    );
                    assert!(!binary.output().contains("PostgreSQL exited unexpectedly"));
                    sql.batch_execute("INSERT INTO signal_receipt VALUES (2)")
                        .await
                        .unwrap();
                }
                let started = std::time::Instant::now();
                binary.signal(signal, target != "host-pid");
                let exit = binary.wait().await;
                assert!(started.elapsed() < Duration::from_secs(15));
                assert!(
                    exit.success(),
                    "{signal:?}, {target}, detached={detached_launcher}: {exit}: {}",
                    binary.output()
                );
                descendants.assert_reaped();
                supervisor.assert_reaped();
                binary.assert_stopped().await;
                assert!(
                    tokio::time::timeout(
                        Duration::from_secs(2),
                        sql.batch_execute("INSERT INTO signal_receipt VALUES (3)")
                    )
                    .await
                    .unwrap()
                    .is_err()
                );
                assert!(instance.connect_application().await.is_err());
                let socket = binary
                    .pgdata
                    .join("pg_stat_tmp")
                    .join(format!(".s.PGSQL.{}", binary.pgport));
                assert!(tokio::net::UnixStream::connect(&socket).await.is_err());
                assert!(!socket.exists());
                tokio::time::timeout(Duration::from_secs(2), connection)
                    .await
                    .unwrap()
                    .unwrap();
                unrelated_sql
                    .batch_execute("INSERT INTO isolation_receipt VALUES (1)")
                    .await
                    .unwrap();
                assert!(unrelated.is_running().await);
                assert!(unrelated_faults.try_recv().is_err());
                assert_eq!(unrelated_child.wait().unwrap().code(), Some(23));
            }
        }
    }
    unrelated.stop().await.unwrap();
    unrelated_probe.assert_gone().await;
}

async fn executable_supervisor_loss_is_cleanup_failure() {
    const HELPER: &str = "KUBERIC_TEST_SUPERVISOR_LOSS_HELPER";
    if std::env::var_os(HELPER).is_none() {
        let output = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::executable_supervisor_loss_is_cleanup_failure",
                "--nocapture",
            ])
            .env(HELPER, "1")
            .kill_on_drop(true)
            .output()
            .await
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        return;
    }
    // Deliberate root loss cannot leave zombies under the machine's PID 1.
    // Only this isolated test subprocess adopts them, not the host or test runner.
    rustix::process::set_child_subreaper(Some(rustix::process::getpid())).unwrap();
    struct Adopted(ProcessProbe);
    impl Drop for Adopted {
        fn drop(&mut self) {
            if let Err(error) = self.0.reap_adopted() {
                eprintln!("isolated root-loss test cleanup failed: {error}");
            }
        }
    }
    let root = TestDataDir::new("host-root-loss");
    let (mut binary, control) = start_binary(root.path()).await;
    drop(control);
    let instance = PgInstanceManager::new(binary.pgdata.clone(), find_pg_bin(), binary.pgport);
    let (sql, connection) = instance.connect_application().await.unwrap();
    sql.batch_execute("CREATE TABLE root_loss_receipt(id int)")
        .await
        .unwrap();
    let supervisor = ProcessProbe::process(supervisor_pid(&binary));
    let descendants = Adopted(ProcessProbe::descendants(binary.child.id()));
    assert!(descendants.0.len() >= 7);
    supervisor.signal(rustix::process::Signal::KILL);
    tokio::time::timeout(Duration::from_secs(5), async {
        while !binary.output().contains("PostgreSQL exited unexpectedly") {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("root loss must be observed even while the postmaster survives");
    binary.terminate();
    let exit = binary.wait().await;
    assert!(!exit.success(), "{exit}: {}", binary.output());
    assert!(
        binary.output().contains("supervisor exited unexpectedly"),
        "{}",
        binary.output()
    );
    assert!(
        binary.output().contains("descendant reaping unproven"),
        "{}",
        binary.output()
    );
    binary.assert_stopped().await;
    assert!(
        sql.batch_execute("INSERT INTO root_loss_receipt VALUES (1)")
            .await
            .is_err()
    );
    assert!(instance.connect_application().await.is_err());
    tokio::time::timeout(Duration::from_secs(2), connection)
        .await
        .unwrap()
        .unwrap();
    // The host must terminate every retained identity, but cannot claim to have
    // reaped descendants orphaned outside its lost ownership root.
    descendants.0.assert_exited().await;
    descendants.0.reap_adopted().unwrap();
    descendants.0.assert_gone().await;
}

async fn executable_serves_initialization_and_singleton_sql_then_stops_its_child() {
    let root = TestDataDir::new("host-executable");
    let (mut binary, mut client) = start_binary(root.path()).await;
    let instance = PgInstanceManager::new(binary.pgdata.clone(), find_pg_bin(), binary.pgport);
    let (sql, _) = instance.connect_application().await.unwrap();
    sql.batch_execute("CREATE TABLE binary_receipt(id int); INSERT INTO binary_receipt VALUES (7)")
        .await
        .unwrap();
    let report = client
        .get_status(authorized(wire::GetAgentStatusRequest {
            protocol_version: kuberic_protocol::PROTOCOL_VERSION,
            resource_uid: "postgres-test".into(),
            replica_id: 1,
            expected_instance_id: "postgres-1".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    client
        .execute(configure(&report.process_session_id, "binary-fence", false))
        .await
        .unwrap();
    assert!(sql.simple_query("SELECT 1").await.is_err());
    let mut coordination_client =
        PgDataServiceClient::connect(format!("http://{}", binary.coordination))
            .await
            .unwrap();
    assert_eq!(
        coordination_client
            .build(pgwire::NativeBuildRequest::default())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    binary.terminate();
    let exit = binary.wait().await;
    assert!(exit.success(), "{exit}");
    binary.assert_stopped().await;
    assert!(instance.connect_application().await.is_err());
}

async fn executable_shutdown_requires_durable_fault_acknowledgement() {
    for release_lock in [false, true] {
        let root = TestDataDir::new("host-executable-fault");
        let pg_bin = wrapped_pg_bin(
            root.path(),
            "postgres",
            &format!(
                "#!/bin/sh\nsetsid sh -c '\"{}/postgres\" \"$@\" &' sh \"$@\" &\n",
                find_pg_bin().display()
            ),
        );
        let (mut binary, client) = start_binary_with(root.path(), &pg_bin).await;
        drop(client);
        let database =
            SqliteStore::metadata_database_path(&root.path().join(SINGLE_REPLICA_DIRECTORY));
        let store = SqliteStore::open_existing(&database, None).unwrap();
        assert_eq!(store.load_state().await.unwrap().reported_fault, None);
        let lock = rusqlite::Connection::open(&database).unwrap();
        lock.execute_batch("BEGIN IMMEDIATE").unwrap();

        let stopped = tokio::process::Command::new(find_pg_bin().join("pg_ctl"))
            .args(["stop", "-D"])
            .arg(&binary.pgdata)
            .args(["-m", "immediate", "-w", "-t", "5"])
            .output()
            .await
            .unwrap();
        assert!(stopped.status.success(), "{stopped:?}");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !binary.output().contains("PostgreSQL exited unexpectedly") {
                assert!(binary.child.try_wait().unwrap().is_none());
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("process monitor must observe the unexpected child exit");
        // Let the monitor's queued fault reach the partition before SIGTERM.
        tokio::time::sleep(Duration::from_millis(100)).await;
        if release_lock {
            lock.execute_batch("ROLLBACK").unwrap();
        }
        binary.terminate();
        let exit = binary.wait().await;
        if release_lock {
            assert!(exit.success(), "{exit}: {}", binary.output());
        } else {
            assert!(!exit.success(), "{exit}: {}", binary.output());
            assert!(
                binary.output().contains("database is locked"),
                "{}",
                binary.output()
            );
            lock.execute_batch("ROLLBACK").unwrap();
        }
        assert_eq!(
            store.load_state().await.unwrap().reported_fault,
            release_lock.then_some(FaultType::Permanent),
            "exit status must distinguish acknowledged from unpersisted faults"
        );
        binary.assert_stopped().await;
    }
}

async fn executable_shutdown_terminates_owned_tree_without_pid_file() {
    for scenario in [
        "normal",
        "hidden-pid",
        "launcher-hidden-pid",
        "stubborn-launcher",
    ] {
        let root = TestDataDir::new("host-owned-exit");
        let mut pg_bin = find_pg_bin();
        if matches!(scenario, "launcher-hidden-pid" | "stubborn-launcher") {
            let script = if scenario == "launcher-hidden-pid" {
                format!(
                    "#!/bin/sh\n\"{}/postgres\" \"$@\" &\nwait\n",
                    pg_bin.display()
                )
            } else {
                format!(
                    "#!/bin/sh\ntrap '' INT QUIT\n\"{}/postgres\" \"$@\" &\nwhile :; do :; done\n",
                    pg_bin.display()
                )
            };
            pg_bin = wrapped_pg_bin(root.path(), "postgres", &script);
        }
        let (mut binary, client) = start_binary_with(root.path(), &pg_bin).await;
        drop(client);
        let instance = PgInstanceManager::new(binary.pgdata.clone(), find_pg_bin(), binary.pgport);
        let (sql, connection) = instance.connect_application().await.unwrap();
        sql.batch_execute(
            "CREATE TABLE shutdown_receipt(id int); INSERT INTO shutdown_receipt VALUES (1)",
        )
        .await
        .unwrap();
        let descendants = ProcessProbe::descendants(binary.child.id());
        assert!(
            descendants.len() >= 6,
            "must track postmaster, retained backend and background workers"
        );
        if scenario.contains("hidden-pid") {
            std::fs::rename(
                binary.pgdata.join("postmaster.pid"),
                binary.pgdata.join("postmaster.pid.hidden"),
            )
            .unwrap();
        }
        let started = std::time::Instant::now();
        binary.terminate();
        let exit = binary.wait().await;
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "shutdown must be bounded"
        );
        assert_eq!(
            exit.success(),
            scenario == "normal",
            "{scenario}: {}",
            binary.output()
        );
        if scenario.contains("hidden-pid") {
            assert!(
                binary
                    .output()
                    .contains("PID file does not identify an owned live process"),
                "{}",
                binary.output()
            );
        }
        if scenario == "stubborn-launcher" {
            assert!(
                binary
                    .output()
                    .contains("fast PostgreSQL shutdown timed out"),
                "{}",
                binary.output()
            );
        }
        binary.assert_stopped().await;
        assert!(
            tokio::time::timeout(
                Duration::from_secs(2),
                sql.batch_execute("INSERT INTO shutdown_receipt VALUES (2)")
            )
            .await
            .unwrap()
            .is_err()
        );
        assert!(instance.connect_application().await.is_err());
        let socket = binary
            .pgdata
            .join("pg_stat_tmp")
            .join(format!(".s.PGSQL.{}", binary.pgport));
        assert!(tokio::net::UnixStream::connect(&socket).await.is_err());
        assert!(!socket.exists(), "PostgreSQL must remove its owned socket");
        tokio::time::timeout(Duration::from_secs(2), connection)
            .await
            .unwrap()
            .unwrap();
        descendants.assert_gone().await;
    }
}

async fn executable_launcher_exit_keeps_owned_postmaster_until_shutdown() {
    for early in [false, true] {
        for hidden in [false, true] {
            let root = TestDataDir::new("launcher-exited");
            let release = root.path().join("release");
            let launcher = root.path().join("launcher");
            let script = if early {
                // Both launch ancestors exit before PostgreSQL even starts.
                format!(
                    "#!/bin/sh\nsetsid sh -c 'sleep 0.2; \"{}/postgres\" \"$@\" &' sh \"$@\" &\n",
                    find_pg_bin().display()
                )
            } else {
                format!(
                    "#!/bin/sh\n\"{}/postgres\" \"$@\" &\necho $$ > '{}'\nwhile [ ! -f '{}' ]; do sleep 0.02; done\n",
                    find_pg_bin().display(),
                    launcher.display(),
                    release.display()
                )
            };
            let pg_bin = wrapped_pg_bin(root.path(), "postgres", &script);
            let (mut binary, mut control) = start_binary_with(root.path(), &pg_bin).await;
            let instance =
                PgInstanceManager::new(binary.pgdata.clone(), find_pg_bin(), binary.pgport);
            let (sql, connection) = instance.connect_application().await.unwrap();
            sql.batch_execute(
                "CREATE TABLE launcher_receipt(id int); INSERT INTO launcher_receipt VALUES (1)",
            )
            .await
            .unwrap();
            let descendants = ProcessProbe::descendants(binary.child.id());
            assert!(descendants.len() >= 7);
            if hidden {
                std::fs::rename(
                    binary.pgdata.join("postmaster.pid"),
                    binary.pgdata.join("postmaster.pid.hidden"),
                )
                .unwrap();
            }
            if !early {
                let pid = std::fs::read_to_string(launcher)
                    .unwrap()
                    .trim()
                    .parse()
                    .unwrap();
                let launcher = ProcessProbe::process(pid);
                std::fs::write(release, "").unwrap();
                launcher.assert_exited().await;
            }
            tokio::time::sleep(Duration::from_millis(750)).await;
            assert!(
                binary.child.try_wait().unwrap().is_none(),
                "{}",
                binary.output()
            );
            assert!(
                !binary.output().contains("PostgreSQL exited unexpectedly"),
                "{}",
                binary.output()
            );
            let report = control
                .get_status(authorized(wire::GetAgentStatusRequest {
                    protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                    resource_uid: "postgres-test".into(),
                    replica_id: 1,
                    expected_instance_id: "postgres-1".into(),
                }))
                .await
                .unwrap()
                .into_inner();
            assert!(!report.process_session_id.is_empty());
            sql.batch_execute("INSERT INTO launcher_receipt VALUES (2)")
                .await
                .unwrap();
            let started = std::time::Instant::now();
            binary.terminate();
            let exit = binary.wait().await;
            assert!(started.elapsed() < Duration::from_secs(15));
            assert_eq!(
                exit.success(),
                !hidden,
                "early={early}, hidden={hidden}: {}",
                binary.output()
            );
            assert!(
                tokio::time::timeout(
                    Duration::from_secs(2),
                    sql.batch_execute("INSERT INTO launcher_receipt VALUES (3)")
                )
                .await
                .unwrap()
                .is_err()
            );
            binary.assert_stopped().await;
            assert!(instance.connect_application().await.is_err());
            let socket = binary
                .pgdata
                .join("pg_stat_tmp")
                .join(format!(".s.PGSQL.{}", binary.pgport));
            assert!(tokio::net::UnixStream::connect(&socket).await.is_err());
            assert!(!socket.exists());
            tokio::time::timeout(Duration::from_secs(2), connection)
                .await
                .unwrap()
                .unwrap();
            descendants.assert_gone().await;
        }
    }
}

#[test]
fn executable_exposes_v2_host_configuration() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_postgres-replicated"))
        .arg("--help")
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    for argument in [
        "--resource-uid",
        "--replica-id",
        "--pod-uid",
        "--pvc-uid",
        "--data-root",
        "--application-root",
        "--pg-data",
        "--pg-bin",
        "--pg-port",
        "--control-address",
        "--replication-address",
        "--application-address",
        "--control-endpoint",
        "--replication-endpoint",
    ] {
        assert!(text.contains(argument), "{argument}");
    }
}

async fn retained_pre_authentication(port: u16) -> std::io::Result<tokio::net::TcpStream> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", port)).await?;
    socket.write_all(&[0, 0, 0, 8, 4, 210, 22, 47]).await?;
    let mut reply = [0];
    socket.read_exact(&mut reply).await?;
    assert_eq!(
        reply, *b"N",
        "the accepted PostgreSQL backend is awaiting startup, not authentication"
    );
    Ok(socket)
}

async fn preauthentication_sockets_are_drained_before_fence_receipt() {
    let root = TestDataDir::new("host-preauth");
    let mut host = HostAttempt::start(root.path()).await;
    let mut running = host.bootstrap(root.path()).await;
    let (mut control, report) = status(host.address).await;
    control
        .execute(configure(
            &report.process_session_id,
            "preauth-install",
            false,
        ))
        .await
        .unwrap();
    control
        .execute(configure(&report.process_session_id, "preauth-grant", true))
        .await
        .unwrap();
    let (sql, _) = host
        .application
        .instance()
        .connect_application()
        .await
        .unwrap();
    sql.batch_execute("CREATE TABLE preauth_receipts(id int)")
        .await
        .unwrap();
    drop(sql);
    for round in 0..8 {
        let retained = retained_pre_authentication(host.application.instance().port())
            .await
            .unwrap();
        let retained_through_regrant =
            retained_pre_authentication(host.application.instance().port())
                .await
                .unwrap();
        let port = host.application.instance().port();
        let accepting = async move {
            let mut attempts = tokio::task::JoinSet::new();
            for _ in 0..12 {
                attempts.spawn(async move {
                    tokio::time::timeout(Duration::from_secs(2), retained_pre_authentication(port))
                        .await
                        .ok()
                        .and_then(Result::ok)
                });
            }
            let mut accepted = Vec::new();
            while let Some(result) = attempts.join_next().await {
                accepted.push(result.unwrap());
            }
            accepted
        };
        let (closed, accepted) = tokio::join!(
            control.execute(configure(
                &report.process_session_id,
                &format!("preauth-close-{round}"),
                false
            )),
            accepting
        );
        closed.unwrap();
        for (index, socket) in std::iter::once(retained)
            .chain(accepted.into_iter().flatten())
            .enumerate()
        {
            let mut config = tokio_postgres::Config::new();
            config
                .user("kuberic_app")
                .dbname("kuberic")
                .ssl_mode(tokio_postgres::config::SslMode::Disable);
            let attempted = tokio::time::timeout(
                Duration::from_secs(2),
                config.connect_raw(socket, tokio_postgres::NoTls),
            )
            .await
            .unwrap();
            if let Ok((client, connection)) = attempted {
                let task = tokio::spawn(connection);
                let result = client
                    .batch_execute(&format!(
                        "INSERT INTO preauth_receipts VALUES ({})",
                        round * 100 + index
                    ))
                    .await;
                drop(client);
                task.abort();
                let _ = task.await;
                panic!("pre-authentication socket authenticated after closure: write={result:?}");
            }
        }
        assert!(
            host.application
                .instance()
                .connect_application()
                .await
                .is_err()
        );
        let (admin, _) = host.application.instance().connect().await.unwrap();
        // Internal administration remains available after the acknowledged fence.
        admin.simple_query("SELECT 1").await.unwrap();
        control
            .execute(configure(
                &report.process_session_id,
                &format!("preauth-grant-{round}"),
                true,
            ))
            .await
            .unwrap();
        let mut startup = tokio_postgres::Config::new();
        startup
            .user("kuberic_app")
            .dbname("kuberic")
            .ssl_mode(tokio_postgres::config::SslMode::Disable);
        assert!(
            tokio::time::timeout(
                Duration::from_secs(2),
                startup.connect_raw(retained_through_regrant, tokio_postgres::NoTls)
            )
            .await
            .unwrap()
            .is_err(),
            "a new grant must not revive a socket from before the fence"
        );
        let (client, _) = host
            .application
            .instance()
            .connect_application()
            .await
            .unwrap();
        assert_eq!(
            client
                .query_one("SELECT count(*) FROM preauth_receipts", &[])
                .await
                .unwrap()
                .get::<_, i64>(0),
            0
        );
    }
    stop(&mut running, &host.application).await;
}

async fn executable_shutdown_cancels_retained_transport_handshakes() {
    use tokio::io::AsyncWriteExt;
    for round in 0..4 {
        let root = TestDataDir::new("host-drain");
        let (mut binary, client) = start_binary(root.path()).await;
        let mut retained = Vec::new();
        for address in [binary.control, binary.replication, binary.coordination] {
            for _ in 0..4 {
                let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
                socket.write_all(b"PRI * HTTP/2.0\r\n").await.unwrap();
                retained.push(socket);
            }
        }
        let descendants = ProcessProbe::descendants(binary.child.id());
        binary.signal(rustix::process::Signal::TERM, round % 2 == 0);
        let exit = tokio::time::timeout(Duration::from_secs(5), binary.wait())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "shutdown depends on retained transport handshakes: {}",
                    binary.output()
                )
            });
        assert!(exit.success(), "{}", binary.output());
        descendants.assert_reaped();
        binary.assert_stopped().await;
        drop((retained, client));
    }
}

mod tests {
    // Native authority/effect futures use the same stack allowance as agent RPC fixtures.
    macro_rules! host_test {
        ($name:ident) => {
            #[test]
            fn $name() {
                std::thread::Builder::new()
                    .stack_size(16 * 1024 * 1024)
                    .spawn(|| {
                        tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .unwrap()
                            .block_on(super::$name());
                    })
                    .unwrap()
                    .join()
                    .unwrap();
            }
        };
    }
    host_test!(fresh_host_waits_without_touching_application_storage);
    host_test!(closed_host_releases_the_partition_and_driver);
    host_test!(singleton_bootstrap_fence_and_restart_use_fresh_sessions_and_preserve_sql);
    host_test!(established_storage_loss_never_reinitializes_postgres);
    host_test!(missing_pgdata_alone_persists_permanent_fault_before_start_returns);
    host_test!(changed_application_root_never_initializes_or_mutates_storage);
    host_test!(changed_pgdata_path_never_initializes_or_mutates_storage);
    host_test!(authorized_initialization_retries_at_storage_boundaries);
    host_test!(orphaned_postgres_without_agent_metadata_is_unsafe_and_untouched);
    host_test!(established_application_identity_mismatch_rejects_before_pgdata_mutation);
    host_test!(unsupported_coordination_rpcs_preserve_every_pgdata_byte);
    host_test!(previously_promoted_database_restarts_closed_until_authority_restoration);
    host_test!(unsupported_native_callbacks_and_mutated_evidence_never_change_pgdata);
    host_test!(executable_serves_initialization_and_singleton_sql_then_stops_its_child);
    host_test!(executable_shutdown_requires_durable_fault_acknowledgement);
    host_test!(executable_shutdown_terminates_owned_tree_without_pid_file);
    host_test!(executable_launcher_exit_keeps_owned_postmaster_until_shutdown);
    host_test!(executable_shutdown_signals_preserve_supervisor_reaping);
    host_test!(executable_supervisor_loss_is_cleanup_failure);
    host_test!(executable_pre_readiness_signals_join_startup_and_reap);
    host_test!(executable_pre_readiness_startup_error_reaps);
    host_test!(executable_pre_readiness_supervisor_loss_never_succeeds);
    host_test!(executable_owned_helpers_cancel_reap_and_retry);
    host_test!(executable_owned_helpers_root_loss_is_failure);
    host_test!(established_agent_and_postgres_lineage_mismatches_are_non_mutating);
    host_test!(unsafe_evidence_reports_permanent_fault_and_stops_granted_sql);
    host_test!(preauthentication_sockets_are_drained_before_fence_receipt);
    host_test!(executable_shutdown_cancels_retained_transport_handshakes);
}
