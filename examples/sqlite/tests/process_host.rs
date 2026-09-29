use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use kuberic_agent::process::{
    ApplicationStorageState, ReplicaHost, ReplicaProcessConfig, RunningReplica,
};
use kuberic_agent::sqlite_store::SqliteStore;
use kuberic_agent::transport::ReplicaEndpointResolver;
use kuberic_protocol::types::{
    PodUid, PvcUid, ReplicaId, ReplicaIdentity, ReplicaInstanceId, ResourceUid,
    derive_agent_generation, derive_initialization_id,
};
use kuberic_wire::proto as agent_proto;
use kuberic_wire::proto::agent_control_client::AgentControlClient;
use sqlite_replicated::proto::sqlite_store_server::SqliteStore as _;
use sqlite_replicated::server::SqliteServer;
use sqlite_replicated::service::SqliteService;
use sqlite_replicated::testing::{SqlitePod, configuration, scratch};
use sqlite_replicated::{SqlitePersistence, proto};
use tonic::{Request, transport::Channel};

struct SingletonResolver;
impl ReplicaEndpointResolver for SingletonResolver {
    fn control_endpoint(&self, _: &ReplicaIdentity) -> String {
        "http://127.0.0.1:1".into()
    }
    fn replication_endpoint(&self, _: &ReplicaIdentity) -> String {
        "http://127.0.0.1:1".into()
    }
}

fn config(root: &Path, control_address: SocketAddr) -> ReplicaProcessConfig {
    ReplicaProcessConfig {
        resource_uid: ResourceUid::new("sqlite-test"),
        replica_id: ReplicaId::new(1),
        pod_uid: PodUid::new("sqlite-1"),
        pvc_uid: PvcUid::new("sqlite-pvc-1"),
        data_root: root.to_path_buf(),
        control_address,
        replication_address: "127.0.0.1:0".parse().unwrap(),
        bearer_token: "in-process-test-token".into(),
        rpc_deadline: Duration::from_secs(2),
        transport_window_capacity: 16,
    }
}

fn application(root: &Path) -> (Arc<SqliteService>, ApplicationStorageState) {
    let path = root.join("application");
    let storage = if SqlitePersistence::is_fresh_empty(&path).unwrap() {
        ApplicationStorageState::FreshEmpty
    } else {
        ApplicationStorageState::Established
    };
    let application = Arc::new(SqliteService::deferred(path, "http://127.0.0.1:0".into()).unwrap());
    (application, storage)
}

fn start_host(
    root: &Path,
) -> (
    SocketAddr,
    Arc<SqliteService>,
    tokio::task::JoinHandle<kuberic_agent::Result<RunningReplica>>,
) {
    let address = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let (application, storage) = application(root);
    let host = ReplicaHost::new(
        config(root, address),
        application.clone(),
        storage,
        Arc::new(SingletonResolver),
    );
    (address, application, tokio::spawn(host.start()))
}

fn authorized<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        "Bearer in-process-test-token".parse().unwrap(),
    );
    request
}

async fn initialization_status(
    address: SocketAddr,
) -> (AgentControlClient<Channel>, agent_proto::AgentStatusReport) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut client = loop {
            match AgentControlClient::connect(format!("http://{address}")).await {
                Ok(client) => break client,
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        };
        let status = client
            .get_status(authorized(agent_proto::GetAgentStatusRequest {
                protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                resource_uid: "sqlite-test".into(),
                replica_id: 1,
                expected_instance_id: "sqlite-1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        (client, status)
    })
    .await
    .expect("initialization listener becomes reachable")
}

fn initialize(session: String) -> Request<agent_proto::ExecuteCommandRequest> {
    let initialization = derive_initialization_id(
        &ResourceUid::new("sqlite-test"),
        ReplicaId::new(1),
        &PodUid::new("sqlite-1"),
        &PvcUid::new("sqlite-pvc-1"),
    );
    let identity = ReplicaIdentity {
        replica_id: ReplicaId::new(1),
        instance_id: ReplicaInstanceId::new("sqlite-1"),
        agent_generation: derive_agent_generation(&initialization),
    };
    authorized(agent_proto::ExecuteCommandRequest {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: "sqlite-test".into(),
        target: Some(identity.clone().into()),
        expected_process_session_id: session,
        command: Some(
            agent_proto::execute_command_request::Command::InitializeAgentStore(
                agent_proto::InitializeAgentStoreCommand {
                    initialization_id: initialization.to_string(),
                    resource_uid: "sqlite-test".into(),
                    local_replica_id: 1,
                    expected_instance_id: "sqlite-1".into(),
                    expected_pod_uid: "sqlite-1".into(),
                    expected_pvc_uid: "sqlite-pvc-1".into(),
                    assigned_agent_generation: identity.agent_generation.to_string(),
                    effective_policy: Some(agent_proto::EffectivePolicy {
                        replica_set_size: 1,
                        write_quorum: 1,
                        read_quorum: 1,
                        failover_delay_seconds: 30,
                    }),
                    bootstrap_configuration: Some(configuration(&[identity], 0, 1).into()),
                    provisioning: None,
                },
            ),
        ),
    })
}

fn application_files(path: &Path) -> BTreeMap<std::ffi::OsString, Vec<u8>> {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (entry.file_name(), std::fs::read(entry.path()).unwrap())
        })
        .collect()
}

#[tokio::test]
async fn interrupted_initialization_keeps_sqlite_fresh_and_retry_opens_only_after_authorization() {
    for existing_empty_directory in [false, true] {
        let root = scratch();
        let path = root.path().join("application");
        let metadata = SqliteStore::metadata_database_path(root.path());
        if existing_empty_directory {
            std::fs::create_dir(&path).unwrap();
        }
        let (address, application, host) = start_host(root.path());
        assert_eq!(path.exists(), existing_empty_directory);
        assert_eq!(
            application.persistence().progress().unwrap_err().kind(),
            std::io::ErrorKind::NotConnected
        );
        let (client, first) = initialization_status(address).await;
        assert_eq!(
            first.storage_state,
            agent_proto::AgentStorageState::Uninitialized as i32
        );
        assert!(SqlitePersistence::is_fresh_empty(&path).unwrap());
        assert!(!metadata.exists());
        assert!(!host.is_finished());
        drop(client);
        host.abort();
        assert!(matches!(host.await, Err(error) if error.is_cancelled()));
        drop(application);
        assert_eq!(path.exists(), existing_empty_directory);
        assert!(SqlitePersistence::is_fresh_empty(&path).unwrap());
        assert!(!metadata.exists());

        let (address, application, host) = start_host(root.path());
        let (mut client, second) = initialization_status(address).await;
        assert_ne!(first.process_session_id, second.process_session_id);
        assert_eq!(
            second.storage_state,
            agent_proto::AgentStorageState::Uninitialized as i32
        );
        assert!(SqlitePersistence::is_fresh_empty(&path).unwrap());
        assert!(!metadata.exists());
        client
            .execute(initialize(second.process_session_id))
            .await
            .unwrap();
        drop(client);
        let mut running = tokio::time::timeout(Duration::from_secs(5), host)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(metadata.is_file());
        assert!(path.join("state-v2.json").is_file());
        assert!(!SqlitePersistence::is_fresh_empty(&path).unwrap());
        assert_eq!(application.persistence().progress().unwrap().applied_lsn, 0);
        assert_eq!(
            application.persistence().progress().unwrap().committed_lsn,
            0
        );
        assert_eq!(
            running.handle().diagnostics().await.unwrap().write_status,
            "NotPrimary"
        );
        running.shutdown();
        tokio::time::timeout(Duration::from_secs(5), running.wait())
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn established_sqlite_without_agent_metadata_stays_unsafe_and_untouched() {
    let root = scratch();
    let source = SqlitePod::singleton(root.path().join("source")).await;
    source
        .execute("CREATE TABLE data(id INTEGER)")
        .await
        .unwrap();
    source.execute("INSERT INTO data VALUES(42)").await.unwrap();
    let original = source.application.persistence().snapshot(2).unwrap();
    source.runtime.abort();
    let source_path = source.root.join("application");
    drop(source);
    let orphan = root.path().join("orphan");
    let path = orphan.join("application");
    std::fs::create_dir_all(&path).unwrap();
    for (name, bytes) in application_files(&source_path) {
        std::fs::write(path.join(name), bytes).unwrap();
    }
    let before = application_files(&path);
    let metadata = SqliteStore::metadata_database_path(&orphan);
    let (address, application, host) = start_host(&orphan);
    assert_eq!(application_files(&path), before);
    let (mut client, status) = initialization_status(address).await;
    assert_eq!(
        status.storage_state,
        agent_proto::AgentStorageState::Unsafe as i32
    );
    assert!(!status.healthy);
    assert_eq!(
        status.storage_error,
        "application state exists without matching Kuberic agent metadata"
    );
    let rejected = client
        .execute(initialize(status.process_session_id))
        .await
        .unwrap_err();
    assert_eq!(rejected.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        rejected.message(),
        "application state is not fresh and empty"
    );
    assert_eq!(
        application.persistence().progress().unwrap_err().kind(),
        std::io::ErrorKind::NotConnected
    );
    assert!(!metadata.exists());
    assert_eq!(application_files(&path), before);
    drop(client);
    host.abort();
    assert!(matches!(host.await, Err(error) if error.is_cancelled()));
    drop(application);
    let preserved = SqlitePersistence::open(path).unwrap();
    assert_eq!(preserved.progress().unwrap().committed_lsn, 2);
    assert_eq!(preserved.snapshot(2).unwrap(), original);
}

#[tokio::test]
async fn replica_host_reopens_sqlite_with_new_process_sessions_without_external_services() {
    let root = scratch();
    let pod = SqlitePod::singleton(root.path().join("replica")).await;
    pod.execute("CREATE TABLE data(id INTEGER)").await.unwrap();
    pod.execute("INSERT INTO data VALUES(42)").await.unwrap();
    let path = pod.root.clone();
    drop(pod);
    let mut last_session = None;
    for _ in 0..2 {
        let before = application_files(&path.join("application"));
        let (application, storage) = application(&path);
        assert_eq!(storage, ApplicationStorageState::Established);
        assert_eq!(application_files(&path.join("application")), before);
        assert_eq!(
            application.persistence().progress().unwrap_err().kind(),
            std::io::ErrorKind::NotConnected
        );
        let mut replica = ReplicaHost::new(
            config(&path, "127.0.0.1:0".parse().unwrap()),
            application.clone(),
            storage,
            Arc::new(SingletonResolver),
        )
        .start()
        .await
        .unwrap();
        let diagnostics = replica.handle().diagnostics().await.unwrap();
        assert_eq!(diagnostics.committed_lsn, 2);
        assert_eq!(application.barrier().last_lsn(), 2);
        assert_eq!(
            application.persistence().progress().unwrap().committed_lsn,
            2
        );
        assert_ne!(last_session.as_ref(), Some(&diagnostics.process_session));
        last_session = Some(diagnostics.process_session);
        let server = SqliteServer::new(application);
        let response = server
            .query(tonic::Request::new(proto::QueryRequest {
                sql: "SELECT id FROM data".into(),
                params: Vec::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response.rows[0].values[0].kind,
            Some(proto::value::Kind::IntegerValue(42))
        );
        replica.shutdown();
        tokio::time::timeout(Duration::from_secs(5), replica.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(
            server
                .query(tonic::Request::new(proto::QueryRequest {
                    sql: "SELECT id FROM data".into(),
                    params: Vec::new(),
                }))
                .await
                .is_err()
        );
    }
}
