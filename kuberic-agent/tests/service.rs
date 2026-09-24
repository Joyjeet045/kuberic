use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream;
use kuberic_agent::hosting::PodRuntime;
use kuberic_agent::provisioning::ObservedStorageIdentity;
use kuberic_agent::service::AgentService;
use kuberic_agent::service::InitializationService;
use kuberic_agent::sqlite_store::SqliteStore;
use kuberic_agent::state::{AgentState, SCHEMA_VERSION, StorageIdentity};
use kuberic_agent::store::AgentStore;
use kuberic_protocol::types::{
    EffectivePolicy, PodUid, PvcUid, ReplicaId, ReplicaIdentity, ReplicaInstanceId, ResourceUid,
    derive_agent_generation, derive_initialization_id,
};
use kuberic_runtime::application::{
    OpenContext, OperationDataStream, RoleChange, StateProvider, StatefulServiceReplica,
};
use kuberic_runtime::replicator::stream::OperationStream;
use kuberic_runtime::replicator::{
    Replicator, ReplicatorFactory, ReplicatorFactoryContext, ReplicatorInterfaces,
    ReplicatorSettings, StateReplicator,
};
use kuberic_runtime::{Result as RuntimeResult, RuntimeError};
use kuberic_runtime_internal::effects::{RuntimeEffect, RuntimeEffectAction};
use kuberic_wire::proto;
use tempfile::tempdir;
use tokio::sync::watch;
use tonic::{Code, Request};

struct NoopApplication {
    streams: Mutex<Vec<OperationStream>>,
}

struct NoopFactory;

struct NoopReplicator {
    replication: Mutex<Option<OperationStream>>,
    copy: Mutex<Option<OperationStream>>,
    _senders: Vec<kuberic_runtime::replicator::stream::OperationSender>,
}

#[async_trait]
impl Replicator for NoopReplicator {
    async fn open(&self) -> RuntimeResult<String> {
        Ok("127.0.0.1:0".into())
    }

    async fn change_role(
        &self,
        _epoch: kuberic_protocol::types::Epoch,
        _role: kuberic_protocol::types::ReplicaRole,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    async fn update_epoch(&self, _epoch: kuberic_protocol::types::Epoch) -> RuntimeResult<()> {
        Ok(())
    }

    async fn close(&self) -> RuntimeResult<()> {
        Ok(())
    }

    fn abort(&self) {}

    async fn current_progress(&self) -> RuntimeResult<i64> {
        Ok(0)
    }

    async fn catch_up_capability(&self) -> RuntimeResult<i64> {
        Ok(0)
    }
}

#[async_trait]
impl StateReplicator for NoopReplicator {
    async fn replicate(&self, _data: bytes::Bytes) -> RuntimeResult<i64> {
        Err(RuntimeError::NotPrimary)
    }

    async fn get_replication_stream(&self) -> RuntimeResult<OperationStream> {
        self.replication
            .lock()
            .unwrap()
            .take()
            .ok_or(RuntimeError::Closed)
    }

    async fn get_copy_stream(&self) -> RuntimeResult<OperationStream> {
        self.copy.lock().unwrap().take().ok_or(RuntimeError::Closed)
    }

    async fn update_replicator_settings(&self, _settings: ReplicatorSettings) -> RuntimeResult<()> {
        Ok(())
    }
}

#[async_trait]
impl ReplicatorFactory for NoopFactory {
    async fn create_replicator(
        &self,
        _context: ReplicatorFactoryContext,
        _state_provider: Arc<dyn StateProvider>,
        _settings: ReplicatorSettings,
    ) -> RuntimeResult<ReplicatorInterfaces> {
        let (replication_sender, replication) = OperationStream::channel(1);
        let (copy_sender, copy) = OperationStream::channel(1);
        let replicator = Arc::new(NoopReplicator {
            replication: Mutex::new(Some(replication)),
            copy: Mutex::new(Some(copy)),
            _senders: vec![replication_sender, copy_sender],
        });
        Ok(ReplicatorInterfaces::secondary(
            replicator.clone(),
            replicator,
        ))
    }
}

#[async_trait]
impl StatefulServiceReplica for NoopApplication {
    async fn open(self: Arc<Self>, context: OpenContext) -> RuntimeResult<Arc<dyn Replicator>> {
        let interfaces = context
            .partition
            .with_factory(Arc::new(NoopFactory))
            .create_replicator(self.clone(), None)
            .await?;
        let state = interfaces.state_replicator();
        *self.streams.lock().unwrap() = vec![
            state.get_replication_stream().await?,
            state.get_copy_stream().await?,
        ];
        Ok(interfaces.replicator())
    }

    async fn change_role(
        &self,
        _role: kuberic_protocol::types::ReplicaRole,
    ) -> RuntimeResult<RoleChange> {
        Ok(RoleChange {
            service_address: None,
        })
    }

    async fn close(&self) -> RuntimeResult<()> {
        Ok(())
    }

    fn abort(&self) {}
}

#[async_trait]
impl StateProvider for NoopApplication {
    async fn update_epoch(
        &self,
        _epoch: kuberic_protocol::types::Epoch,
        _previous_epoch_last_lsn: i64,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    async fn last_committed_lsn(&self) -> RuntimeResult<i64> {
        Ok(0)
    }

    async fn get_copy_context(&self) -> RuntimeResult<OperationDataStream> {
        Ok(Box::pin(stream::empty()))
    }

    async fn get_copy_state(
        &self,
        _up_to_lsn: i64,
        _copy_context: OperationDataStream,
    ) -> RuntimeResult<OperationDataStream> {
        Ok(Box::pin(stream::empty()))
    }

    async fn on_data_loss(&self) -> RuntimeResult<bool> {
        Ok(false)
    }
}

fn identity() -> ReplicaIdentity {
    let initialization_id = derive_initialization_id(
        &ResourceUid::new("resource-1"),
        ReplicaId::new(1),
        &PodUid::new("pod-1"),
        &PvcUid::new("pvc-1"),
    );
    ReplicaIdentity {
        replica_id: ReplicaId::new(1),
        instance_id: ReplicaInstanceId::new("pod-1"),
        agent_generation: derive_agent_generation(&initialization_id),
    }
}

fn free_address() -> SocketAddr {
    let listener = TcpListener::bind((IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    listener.local_addr().unwrap()
}

#[tokio::test]
async fn services_bind_separate_listeners_require_credentials_and_report_readiness() {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let storage_identity = StorageIdentity {
        schema_version: SCHEMA_VERSION,
        resource_uid: ResourceUid::new("resource-1"),
        pod_uid: PodUid::new("pod-1"),
        pvc_uid: PvcUid::new("pvc-1"),
        initialization_id: derive_initialization_id(
            &ResourceUid::new("resource-1"),
            ReplicaId::new(1),
            &PodUid::new("pod-1"),
            &PvcUid::new("pvc-1"),
        ),
        local_identity: identity(),
        effective_policy: EffectivePolicy::fixed(1, 30).unwrap(),
    };
    let store = Arc::new(
        SqliteStore::create_authorized(path, AgentState::new(storage_identity.clone())).unwrap(),
    );
    let runtime = Arc::new(PodRuntime::new_for_partition(
        kuberic_protocol::types::PartitionInformation {
            partition_id: kuberic_protocol::types::PartitionId::new("partition-1"),
        },
        identity(),
        Arc::new(NoopApplication {
            streams: Mutex::new(Vec::new()),
        }),
        store.clone(),
    ));
    store
        .begin_effect(&RuntimeEffect {
            operation_id: kuberic_protocol::types::OperationId::new("pending-before-service-start"),
            sequence: 1,
            action: RuntimeEffectAction::SetReadStatus(
                kuberic_protocol::types::AccessStatus::NotPrimary,
            ),
        })
        .await
        .unwrap();
    assert!(
        AgentService::new(
            store.clone(),
            runtime.clone(),
            runtime.clone(),
            Arc::<str>::from(""),
        )
        .is_err()
    );
    let control_address = free_address();
    let replication_address = free_address();
    assert_ne!(control_address, replication_address);
    let service = AgentService::new(
        store.clone(),
        runtime.clone(),
        runtime.clone(),
        Arc::<str>::from("secret"),
    )
    .unwrap();
    let (ready_tx, mut ready_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server =
        tokio::spawn(service.serve(control_address, replication_address, ready_tx, shutdown_rx));
    ready_rx.wait_for(|ready| *ready).await.unwrap();
    assert!(store.load_state().await.unwrap().pending_effect.is_none());

    let mut client = proto::agent_control_client::AgentControlClient::connect(format!(
        "http://{control_address}"
    ))
    .await
    .unwrap();
    let request = proto::GetAgentStatusRequest {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: "resource-1".into(),
        replica_id: 1,
        expected_instance_id: "pod-1".into(),
    };
    assert_eq!(
        client.get_status(request.clone()).await.unwrap_err().code(),
        Code::Unauthenticated
    );
    let mut incompatible = Request::new(proto::GetAgentStatusRequest {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION + 1,
        ..request.clone()
    });
    incompatible
        .metadata_mut()
        .insert("authorization", "Bearer secret".parse().unwrap());
    assert_eq!(
        client.get_status(incompatible).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    let mut valid = Request::new(request);
    valid
        .metadata_mut()
        .insert("authorization", "Bearer secret".parse().unwrap());
    let report = client.get_status(valid).await.unwrap().into_inner();
    assert_eq!(report.resource_uid, "resource-1");
    assert!(!report.process_session_id.is_empty());
    assert_eq!(report.read_status, proto::AccessStatus::NotPrimary as i32);
    assert_eq!(report.report_sequence, 1);

    let mut initialize = Request::new(proto::ExecuteCommandRequest {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: "resource-1".into(),
        target: Some(identity().into()),
        command: Some(
            proto::execute_command_request::Command::InitializeAgentStore(
                proto::InitializeAgentStoreCommand {
                    initialization_id: storage_identity.initialization_id.to_string(),
                    resource_uid: "resource-1".into(),
                    local_replica_id: 1,
                    expected_instance_id: "pod-1".into(),
                    expected_pod_uid: "pod-1".into(),
                    expected_pvc_uid: "pvc-1".into(),
                    assigned_agent_generation: identity().agent_generation.to_string(),
                    effective_policy: Some(proto::EffectivePolicy {
                        replica_set_size: storage_identity.effective_policy.replica_set_size,
                        write_quorum: storage_identity.effective_policy.write_quorum,
                        read_quorum: storage_identity.effective_policy.read_quorum,
                        failover_delay_seconds: storage_identity
                            .effective_policy
                            .failover_delay_seconds,
                    }),
                    bootstrap_configuration: Some(
                        kuberic_protocol::types::ConfigurationDescriptor::new(
                            kuberic_protocol::types::Epoch::new(0, 1),
                            kuberic_protocol::types::ReplicaId::new(1),
                            vec![kuberic_protocol::types::ConfigurationMember {
                                identity: identity(),
                                role: kuberic_protocol::types::ReplicaRole::Primary,
                            }],
                            1,
                        )
                        .into(),
                    ),
                    provisioning: None,
                },
            ),
        ),
    });
    initialize
        .metadata_mut()
        .insert("authorization", "Bearer secret".parse().unwrap());
    let response = client.execute(initialize).await.unwrap().into_inner();
    assert_eq!(response.observation.unwrap().report_sequence, 2);

    shutdown_tx.send_replace(true);
    server.await.unwrap().unwrap();
    assert!(!*ready_rx.borrow());
    assert!(!runtime.snapshot().await.open);
}

#[tokio::test]
async fn fresh_storage_reports_uninitialized_and_creates_exact_bootstrap_identity() {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let address = free_address();
    let observed = ObservedStorageIdentity {
        resource_uid: ResourceUid::new("resource-1"),
        pod_uid: PodUid::new("pod-1"),
        pvc_uid: PvcUid::new("pvc-1"),
        instance_id: ReplicaInstanceId::new("pod-1"),
    };
    let (initialized_tx, mut initialized_rx) = watch::channel(false);
    let service = InitializationService::new(
        observed,
        ReplicaId::new(1),
        path.clone(),
        "token",
        initialized_tx,
        true,
    )
    .unwrap();
    let (ready_tx, mut ready_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server = tokio::spawn(service.serve(address, ready_tx, shutdown_rx));
    ready_rx.wait_for(|ready| *ready).await.unwrap();

    let mut client =
        proto::agent_control_client::AgentControlClient::connect(format!("http://{address}"))
            .await
            .unwrap();
    let mut status = Request::new(proto::GetAgentStatusRequest {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: "resource-1".into(),
        replica_id: 1,
        expected_instance_id: "pod-1".into(),
    });
    status.metadata_mut().insert(
        "authorization",
        format!("{} {}", "Bearer", "token").parse().unwrap(),
    );
    let report = client.get_status(status).await.unwrap().into_inner();
    assert_eq!(
        report.storage_state,
        proto::AgentStorageState::Uninitialized as i32
    );

    let storage_identity = StorageIdentity {
        schema_version: SCHEMA_VERSION,
        resource_uid: ResourceUid::new("resource-1"),
        pod_uid: PodUid::new("pod-1"),
        pvc_uid: PvcUid::new("pvc-1"),
        initialization_id: derive_initialization_id(
            &ResourceUid::new("resource-1"),
            ReplicaId::new(1),
            &PodUid::new("pod-1"),
            &PvcUid::new("pvc-1"),
        ),
        local_identity: identity(),
        effective_policy: EffectivePolicy::fixed(1, 30).unwrap(),
    };
    let configuration = kuberic_protocol::types::ConfigurationDescriptor::new(
        kuberic_protocol::types::Epoch::new(0, 1),
        ReplicaId::new(1),
        vec![kuberic_protocol::types::ConfigurationMember {
            identity: identity(),
            role: kuberic_protocol::types::ReplicaRole::Primary,
        }],
        1,
    );
    let mut initialize = Request::new(proto::ExecuteCommandRequest {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: "resource-1".into(),
        target: Some(identity().into()),
        command: Some(
            proto::execute_command_request::Command::InitializeAgentStore(
                proto::InitializeAgentStoreCommand {
                    initialization_id: storage_identity.initialization_id.to_string(),
                    resource_uid: "resource-1".into(),
                    local_replica_id: 1,
                    expected_instance_id: "pod-1".into(),
                    expected_pod_uid: "pod-1".into(),
                    expected_pvc_uid: "pvc-1".into(),
                    assigned_agent_generation: identity().agent_generation.to_string(),
                    effective_policy: Some(proto::EffectivePolicy {
                        replica_set_size: 1,
                        write_quorum: 1,
                        read_quorum: 1,
                        failover_delay_seconds: 30,
                    }),
                    bootstrap_configuration: Some(configuration.into()),
                    provisioning: None,
                },
            ),
        ),
    });
    initialize.metadata_mut().insert(
        "authorization",
        format!("{} {}", "Bearer", "token").parse().unwrap(),
    );
    client.execute(initialize).await.unwrap();
    initialized_rx
        .wait_for(|initialized| *initialized)
        .await
        .unwrap();
    shutdown_tx.send_replace(true);
    server.await.unwrap().unwrap();

    let store = SqliteStore::open_existing(path, Some(&storage_identity)).unwrap();
    assert_eq!(store.identity().await.unwrap(), storage_identity);
}
