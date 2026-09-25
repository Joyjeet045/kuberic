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
    ConfigurationId, EffectivePolicy, OperationId, PodUid, PvcUid, ReplicaId, ReplicaIdentity,
    ReplicaInstanceId, ResourceUid, SwitchoverHandoff, SwitchoverRequestId,
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

struct ReplayApplication;

#[async_trait]
impl StatefulServiceReplica for ReplayApplication {
    async fn open(self: Arc<Self>, context: OpenContext) -> RuntimeResult<Arc<dyn Replicator>> {
        let provider = Arc::new(NoopApplication {
            streams: Mutex::new(Vec::new()),
        });
        let interfaces = context
            .partition
            .with_factory(Arc::new(
                kuberic_runtime::replicator::DefaultReplicatorFactory::new(self),
            ))
            .create_replicator(provider, None)
            .await?;
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
impl kuberic_runtime::engine::DurableState for ReplayApplication {
    async fn get_replication_operations(
        &self,
        _: i64,
        _: i64,
    ) -> RuntimeResult<kuberic_runtime::engine::RetainedOperationStream> {
        Ok(Box::pin(stream::empty()))
    }
    async fn apply_copy_chunk(
        &self,
        _: &OperationId,
        _: u64,
        _: kuberic_runtime::application::CopyChunk,
    ) -> RuntimeResult<()> {
        panic!("retained command replay must not copy")
    }
    async fn verify_copy_chunk(
        &self,
        _: &OperationId,
        _: u64,
        _: &kuberic_runtime::application::CopyChunk,
    ) -> RuntimeResult<bool> {
        panic!("retained command replay must not copy")
    }
    async fn finish_copy(
        &self,
        _: &OperationId,
        _: i64,
        _: i64,
    ) -> RuntimeResult<kuberic_runtime::application::DurableApplicationProgress> {
        panic!("retained command replay must not copy")
    }
    async fn apply(
        &self,
        _: kuberic_runtime::application::Operation,
    ) -> RuntimeResult<kuberic_runtime::application::DurableApplicationAck> {
        panic!("retained command replay must not write")
    }
    async fn durable_progress(
        &self,
    ) -> RuntimeResult<kuberic_runtime::application::DurableApplicationProgress> {
        Ok(Default::default())
    }
    async fn verify_applied(
        &self,
        _: &kuberic_runtime::application::Operation,
    ) -> RuntimeResult<bool> {
        panic!("retained command replay must not write")
    }
    async fn commit(
        &self,
        lsn: i64,
    ) -> RuntimeResult<kuberic_runtime::application::DurableApplicationProgress> {
        assert_eq!(lsn, 0);
        Ok(Default::default())
    }
}

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
        expected_process_session_id: report.process_session_id.clone(),
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
    let mut stale = Request::new(proto::ExecuteCommandRequest {
        expected_process_session_id: "stale-session".to_string(),
        ..initialize.get_ref().clone()
    });
    *stale.metadata_mut() = initialize.metadata().clone();
    assert_eq!(
        client.execute(stale).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    let response = client.execute(initialize).await.unwrap().into_inner();
    assert_eq!(response.observation.unwrap().report_sequence, 2);

    shutdown_tx.send_replace(true);
    server.await.unwrap().unwrap();
    assert!(!*ready_rx.borrow());
    assert!(!runtime.snapshot().await.open);
}

#[tokio::test]
async fn restarted_agent_rejects_old_session_commands_without_mutating_store() {
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
    let application = || {
        Arc::new(NoopApplication {
            streams: Mutex::new(Vec::new()),
        })
    };
    let runtime = Arc::new(PodRuntime::new_for_partition(
        kuberic_protocol::types::PartitionInformation {
            partition_id: kuberic_protocol::types::PartitionId::new("partition-1"),
        },
        identity(),
        application(),
        store.clone(),
    ));
    let first_control = free_address();
    let first_replication = free_address();
    let first_service = AgentService::new(
        store.clone(),
        runtime.clone(),
        runtime,
        Arc::<str>::from("token"),
    )
    .unwrap();
    let (first_ready_tx, mut first_ready_rx) = watch::channel(false);
    let (first_shutdown_tx, first_shutdown_rx) = watch::channel(false);
    let first_server = tokio::spawn(first_service.serve(
        first_control,
        first_replication,
        first_ready_tx,
        first_shutdown_rx,
    ));
    first_ready_rx.wait_for(|ready| *ready).await.unwrap();
    let mut first_client =
        proto::agent_control_client::AgentControlClient::connect(format!("http://{first_control}"))
            .await
            .unwrap();
    let mut first_status = Request::new(proto::GetAgentStatusRequest {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: "resource-1".to_string(),
        replica_id: 1,
        expected_instance_id: "pod-1".to_string(),
    });
    first_status.metadata_mut().insert(
        "authorization",
        format!("{} {}", "Bearer", "token").parse().unwrap(),
    );
    let old_session = first_client
        .get_status(first_status)
        .await
        .unwrap()
        .into_inner()
        .process_session_id;
    first_shutdown_tx.send_replace(true);
    first_server.await.unwrap().unwrap();

    let runtime = Arc::new(PodRuntime::new_for_partition(
        kuberic_protocol::types::PartitionInformation {
            partition_id: kuberic_protocol::types::PartitionId::new("partition-1"),
        },
        identity(),
        application(),
        store.clone(),
    ));
    let control = free_address();
    let replication = free_address();
    let service = AgentService::new(
        store.clone(),
        runtime.clone(),
        runtime,
        Arc::<str>::from("token"),
    )
    .unwrap();
    let (ready_tx, mut ready_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server = tokio::spawn(service.serve(control, replication, ready_tx, shutdown_rx));
    ready_rx.wait_for(|ready| *ready).await.unwrap();
    let mut client =
        proto::agent_control_client::AgentControlClient::connect(format!("http://{control}"))
            .await
            .unwrap();
    let mut status = Request::new(proto::GetAgentStatusRequest {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: "resource-1".to_string(),
        replica_id: 1,
        expected_instance_id: "pod-1".to_string(),
    });
    status.metadata_mut().insert(
        "authorization",
        format!("{} {}", "Bearer", "token").parse().unwrap(),
    );
    let new_session = client
        .get_status(status)
        .await
        .unwrap()
        .into_inner()
        .process_session_id;
    assert_ne!(old_session, new_session);

    let configuration = kuberic_protocol::types::ConfigurationDescriptor::new(
        kuberic_protocol::types::Epoch::new(0, 1),
        ReplicaId::new(1),
        vec![kuberic_protocol::types::ConfigurationMember {
            identity: identity(),
            role: kuberic_protocol::types::ReplicaRole::Primary,
        }],
        1,
    );
    let initialize_command = || proto::InitializeAgentStoreCommand {
        initialization_id: storage_identity.initialization_id.to_string(),
        resource_uid: "resource-1".to_string(),
        local_replica_id: 1,
        expected_instance_id: "pod-1".to_string(),
        expected_pod_uid: "pod-1".to_string(),
        expected_pvc_uid: "pvc-1".to_string(),
        assigned_agent_generation: identity().agent_generation.to_string(),
        effective_policy: Some(proto::EffectivePolicy {
            replica_set_size: 1,
            write_quorum: 1,
            read_quorum: 1,
            failover_delay_seconds: 30,
        }),
        bootstrap_configuration: Some(configuration.clone().into()),
        provisioning: None,
    };
    let request = |session: &str, command| {
        let mut request = Request::new(proto::ExecuteCommandRequest {
            protocol_version: kuberic_protocol::PROTOCOL_VERSION,
            resource_uid: "resource-1".to_string(),
            target: Some(identity().into()),
            expected_process_session_id: session.to_string(),
            command: Some(command),
        });
        request.metadata_mut().insert(
            "authorization",
            format!("{} {}", "Bearer", "token").parse().unwrap(),
        );
        request
    };
    let state_before = store.load_state().await.unwrap();
    let stale_commands = vec![
        proto::execute_command_request::Command::InitializeAgentStore(initialize_command()),
        proto::execute_command_request::Command::EnsureConfiguration(Box::new(
            proto::EnsureConfigurationCommand {
                operation_id: "configuration-1".to_string(),
                current_configuration: Some(configuration.clone().into()),
                current_epoch: Some(configuration.epoch.into()),
                effective_policy: Some(proto::EffectivePolicy {
                    replica_set_size: 1,
                    write_quorum: 1,
                    read_quorum: 1,
                    failover_delay_seconds: 30,
                }),
                local_replica_id: 1,
                expected_instance_id: "pod-1".to_string(),
                expected_agent_generation: identity().agent_generation.to_string(),
                transition_kind: proto::TransitionKind::Bootstrap as i32,
                primary_write_status: proto::AccessStatus::ReconfigurationPending as i32,
                ..Default::default()
            },
        )),
        proto::execute_command_request::Command::EnsureReplicaBuild(
            proto::EnsureReplicaBuildCommand {
                operation_id: "build-1".to_string(),
                local_replica_id: 1,
                expected_instance_id: "pod-1".to_string(),
                expected_agent_generation: identity().agent_generation.to_string(),
                target: Some(proto::ReplicaIdentity {
                    replica_id: 2,
                    instance_id: "pod-2".to_string(),
                    agent_generation: "generation-2".to_string(),
                }),
                authority: None,
                source_session_id: String::new(),
            },
        ),
    ];
    for command in stale_commands {
        assert_eq!(
            client
                .execute(request(&old_session, command))
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
        assert_eq!(store.load_state().await.unwrap(), state_before);
    }

    client
        .execute(request(
            &new_session,
            proto::execute_command_request::Command::InitializeAgentStore(initialize_command()),
        ))
        .await
        .unwrap();

    shutdown_tx.send_replace(true);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn status_reports_durable_switchover_preparation_after_reopen() {
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
    let target = ReplicaIdentity {
        replica_id: ReplicaId::new(2),
        instance_id: ReplicaInstanceId::new("pod-2"),
        agent_generation: kuberic_protocol::types::AgentGeneration::new("generation-2"),
    };
    let handoff = SwitchoverHandoff {
        preparation_generation: 1,
        preparation_operation_id: OperationId::new("prepare-1"),
        request_id: SwitchoverRequestId::new("request-1"),
        source: storage_identity.local_identity.clone(),
        target,
        starting_configuration_id: ConfigurationId::new("configuration-1"),
        starting_epoch: kuberic_protocol::types::Epoch::new(0, 1),
        handoff_lsn: 9,
    };
    let mut state = AgentState::new(storage_identity);
    state.prepared_switchover = Some(handoff.clone());
    drop(SqliteStore::create_authorized(&path, state).unwrap());
    let store = Arc::new(SqliteStore::open_existing(&path, None).unwrap());
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
    let control = free_address();
    let replication = free_address();
    let service =
        AgentService::new(store, runtime.clone(), runtime, Arc::<str>::from("token")).unwrap();
    let (ready_tx, mut ready_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server = tokio::spawn(service.serve(control, replication, ready_tx, shutdown_rx));
    ready_rx.wait_for(|ready| *ready).await.unwrap();

    let mut client =
        proto::agent_control_client::AgentControlClient::connect(format!("http://{control}"))
            .await
            .unwrap();
    let mut status = Request::new(proto::GetAgentStatusRequest {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: "resource-1".to_string(),
        replica_id: 1,
        expected_instance_id: "pod-1".to_string(),
    });
    status.metadata_mut().insert(
        "authorization",
        format!("{} {}", "Bearer", "token").parse().unwrap(),
    );
    let report = client.get_status(status).await.unwrap().into_inner();
    let prepared = report.prepared_switchover.unwrap();
    assert_eq!(prepared.preparation_operation_id, "prepare-1");
    assert_eq!(prepared.handoff_lsn, 9);

    shutdown_tx.send_replace(true);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn switchover_commands_revalidate_sessions_before_replaying_durable_evidence() {
    use kuberic_agent::state::RetainedCommandResult;
    use kuberic_protocol::command::ProtocolCommand;
    use kuberic_protocol::types::{
        AccessStatus, ConfigurationDescriptor, ConfigurationMember, Epoch, ReplicaRole,
    };
    for stage in [
        "prepare",
        "retired-prepare",
        "demote",
        "promote",
        "current-only",
        "restore",
        "compensate",
        "compensated-current-only",
    ] {
        let preparing = stage.ends_with("prepare");
        let source = identity();
        let target = ReplicaIdentity {
            replica_id: ReplicaId::new(2),
            instance_id: ReplicaInstanceId::new("pod-2"),
            agent_generation: kuberic_protocol::types::AgentGeneration::new("generation-2"),
        };
        let configuration = |number, primary: &ReplicaIdentity| {
            ConfigurationDescriptor::new(
                Epoch::new(0, number),
                primary.replica_id,
                [&source, &target]
                    .into_iter()
                    .map(|local| ConfigurationMember {
                        identity: local.clone(),
                        role: if local == primary {
                            ReplicaRole::Primary
                        } else {
                            ReplicaRole::ActiveSecondary
                        },
                    })
                    .collect(),
                2,
            )
        };
        let starting = configuration(1, &source);
        let requested = configuration(2, &target);
        let compensation = configuration(3, &source);
        let handoff = SwitchoverHandoff {
            preparation_generation: 1,
            preparation_operation_id:
                kuberic_protocol::types::derive_switchover_preparation_operation_id(
                    &ResourceUid::new("resource-1"),
                    &SwitchoverRequestId::new("session-request"),
                    1,
                    &starting.configuration_id,
                    &source,
                    &target,
                ),
            request_id: SwitchoverRequestId::new("session-request"),
            source: source.clone(),
            target: target.clone(),
            starting_configuration_id: starting.configuration_id.clone(),
            starting_epoch: starting.epoch,
            handoff_lsn: 0,
        };
        let local = if stage == "promote" {
            target.clone()
        } else {
            source.clone()
        };
        let restoring = stage == "restore";
        let compensating = stage.starts_with("compensat");
        let current_only = stage.ends_with("current-only");
        let current = if preparing || restoring {
            &starting
        } else if compensating {
            &compensation
        } else {
            &requested
        };
        let previous = (!current_only && !restoring && !preparing).then(|| {
            if compensating {
                requested.clone()
            } else {
                starting.clone()
            }
        });
        let command = if preparing {
            proto::execute_command_request::Command::PrepareSwitchover(
                proto::PrepareSwitchoverCommand {
                    preparation_generation: handoff.preparation_generation,
                    operation_id: handoff.preparation_operation_id.to_string(),
                    request_id: handoff.request_id.to_string(),
                    local_replica_id: local.replica_id.value(),
                    expected_instance_id: local.instance_id.to_string(),
                    expected_agent_generation: local.agent_generation.to_string(),
                    source: Some(source.clone().into()),
                    target: Some(target.clone().into()),
                    current_configuration: Some(starting.clone().into()),
                },
            )
        } else {
            proto::execute_command_request::Command::EnsureConfiguration(Box::new(
                proto::EnsureConfigurationCommand {
                    operation_id: format!("session-{stage}"),
                    previous_epoch: previous.as_ref().map(|cc| cc.epoch.into()),
                    previous_configuration: previous.clone().map(Into::into),
                    current_configuration: Some(current.clone().into()),
                    current_epoch: Some(current.epoch.into()),
                    effective_policy: Some(proto::EffectivePolicy {
                        replica_set_size: 2,
                        write_quorum: 2,
                        read_quorum: 1,
                        failover_delay_seconds: 30,
                    }),
                    local_replica_id: local.replica_id.value(),
                    expected_instance_id: local.instance_id.to_string(),
                    expected_agent_generation: local.agent_generation.to_string(),
                    transition_kind: proto::TransitionKind::PlannedSwitchover as i32,
                    primary_write_status: proto::AccessStatus::ReconfigurationPending as i32,
                    current_only,
                    switchover_handoff: Some(handoff.clone().into()),
                    retire_switchover_preparation_ids: if current_only || restoring {
                        vec![handoff.preparation().into()]
                    } else {
                        Vec::new()
                    },
                    ..Default::default()
                },
            ))
        };
        let envelope = proto::ExecuteCommandRequest {
            protocol_version: kuberic_protocol::PROTOCOL_VERSION,
            resource_uid: "resource-1".into(),
            target: Some(local.clone().into()),
            expected_process_session_id: "previous-process".into(),
            command: Some(command),
        };
        let mut state = AgentState::new(StorageIdentity {
            schema_version: SCHEMA_VERSION,
            resource_uid: ResourceUid::new("resource-1"),
            pod_uid: PodUid::new(local.instance_id.as_str()),
            pvc_uid: PvcUid::new("session-pvc"),
            initialization_id: derive_initialization_id(
                &ResourceUid::new("resource-1"),
                local.replica_id,
                &PodUid::new(local.instance_id.as_str()),
                &PvcUid::new("session-pvc"),
            ),
            local_identity: local.clone(),
            effective_policy: EffectivePolicy::fixed(2, 30).unwrap(),
        });
        state.highest_epoch = current.epoch;
        state.current_configuration = Some(current.clone());
        state.previous_configuration = previous;
        state.role = current
            .members
            .iter()
            .find(|m| m.identity == local)
            .unwrap()
            .role;
        state.write_status = AccessStatus::ReconfigurationPending;
        if current_only || restoring {
            state.retired_switchover = Some(handoff.clone());
            state.preparation_retirement = Some(kuberic_agent::state::PreparationRetirement {
                starting_configuration_id: handoff.starting_configuration_id.clone(),
                starting_epoch: handoff.starting_epoch,
                generation: handoff.preparation_generation,
            });
        } else if local == source {
            state.prepared_switchover = Some(handoff.clone());
        }
        if stage == "retired-prepare" {
            state.prepared_switchover = None;
            state.write_status = AccessStatus::Granted;
            state.preparation_retirement = Some(kuberic_agent::state::PreparationRetirement {
                starting_configuration_id: starting.configuration_id.clone(),
                starting_epoch: starting.epoch,
                generation: 3,
            });
        }
        if let ProtocolCommand::EnsureConfiguration(command) =
            kuberic_wire::normalize_execute_request(envelope.clone())
                .unwrap()
                .command
        {
            state.retained_command = Some(RetainedCommandResult {
                command: *command,
                role: state.role,
                epoch: state.highest_epoch,
            });
        }
        let directory = tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        drop(SqliteStore::create_authorized(&path, state).unwrap());
        let store = Arc::new(SqliteStore::open_existing(&path, None).unwrap());
        use kuberic_runtime_internal::authority::{AdmittedAuthority, ReplicaAuthorityStore};
        let persisted = store.load_state().await.unwrap();
        store
            .admit(&AdmittedAuthority {
                local_identity: local.clone(),
                transition_kind: persisted
                    .previous_configuration
                    .as_ref()
                    .map(|_| kuberic_protocol::types::TransitionKind::PlannedSwitchover),
                previous_configuration: persisted.previous_configuration.clone(),
                current_configuration: current.clone(),
                switchover_handoff: (current.epoch > starting.epoch).then_some(handoff.clone()),
            })
            .await
            .unwrap();
        let runtime = Arc::new(PodRuntime::new(
            local.clone(),
            Arc::new(ReplayApplication),
            store.clone(),
        ));
        let service = AgentService::new(
            store.clone(),
            runtime.clone(),
            runtime,
            Arc::<str>::from("token"),
        )
        .unwrap();
        let control = free_address();
        let replication = free_address();
        let (ready_tx, mut ready_rx) = watch::channel(false);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let server = tokio::spawn(service.serve(control, replication, ready_tx, shutdown_rx));
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            ready_rx.wait_for(|ready| *ready),
        )
        .await
        .unwrap()
        .unwrap();
        let mut client =
            proto::agent_control_client::AgentControlClient::connect(format!("http://{control}"))
                .await
                .unwrap();
        let authorize = |body| {
            let mut request = Request::new(body);
            request.metadata_mut().insert(
                "authorization",
                format!("{} {}", "Bearer", "token").parse().unwrap(),
            );
            request
        };
        let mut status = Request::new(proto::GetAgentStatusRequest {
            protocol_version: kuberic_protocol::PROTOCOL_VERSION,
            resource_uid: "resource-1".into(),
            replica_id: local.replica_id.value(),
            expected_instance_id: local.instance_id.to_string(),
        });
        status.metadata_mut().insert(
            "authorization",
            format!("{} {}", "Bearer", "token").parse().unwrap(),
        );
        let session = client
            .get_status(status)
            .await
            .unwrap()
            .into_inner()
            .process_session_id;
        let before = store.load_state().await.unwrap();
        assert_eq!(
            client
                .execute(authorize(envelope.clone()))
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition,
            "{stage}"
        );
        assert_eq!(store.load_state().await.unwrap(), before);
        if stage == "retired-prepare" {
            for generation in 1..=3 {
                let mut delayed = envelope.clone();
                delayed.expected_process_session_id = session.clone();
                let Some(proto::execute_command_request::Command::PrepareSwitchover(command)) =
                    delayed.command.as_mut()
                else {
                    unreachable!()
                };
                command.preparation_generation = generation;
                command.operation_id =
                    kuberic_protocol::types::derive_switchover_preparation_operation_id(
                        &ResourceUid::new("resource-1"),
                        &handoff.request_id,
                        generation,
                        &starting.configuration_id,
                        &source,
                        &target,
                    )
                    .to_string();
                assert_eq!(
                    client.execute(authorize(delayed)).await.unwrap_err().code(),
                    Code::FailedPrecondition
                );
                assert_eq!(store.load_state().await.unwrap(), before);
            }
            shutdown_tx.send_replace(true);
            server.await.unwrap().unwrap();
            continue;
        }
        for _ in 0..2 {
            let response = client
                .execute(authorize(proto::ExecuteCommandRequest {
                    expected_process_session_id: session.clone(),
                    ..envelope.clone()
                }))
                .await
                .unwrap_or_else(|error| panic!("{stage}: {error}"))
                .into_inner()
                .observation
                .unwrap();
            assert_eq!(response.process_session_id, session);
            assert_eq!(
                store.load_state().await.unwrap(),
                before,
                "exact replay must not allocate effects: {stage}"
            );
            assert_eq!(
                response.prepared_switchover,
                before.prepared_switchover.clone().map(Into::into)
            );
        }
        let mut conflicting = envelope.clone();
        conflicting.expected_process_session_id = session;
        match conflicting.command.as_mut().unwrap() {
            proto::execute_command_request::Command::PrepareSwitchover(command) => {
                command.request_id = "conflicting-request".into();
            }
            proto::execute_command_request::Command::EnsureConfiguration(command) => {
                command.switchover_handoff.as_mut().unwrap().handoff_lsn += 1;
            }
            _ => unreachable!(),
        }
        assert!(
            client.execute(authorize(conflicting)).await.is_err(),
            "{stage}"
        );
        assert_eq!(store.load_state().await.unwrap(), before, "{stage}");
        shutdown_tx.send_replace(true);
        server.await.unwrap().unwrap();
    }
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
        expected_process_session_id: report.process_session_id.clone(),
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
    let mut stale = Request::new(proto::ExecuteCommandRequest {
        expected_process_session_id: "stale-session".to_string(),
        ..initialize.get_ref().clone()
    });
    *stale.metadata_mut() = initialize.metadata().clone();
    assert_eq!(
        client.execute(stale).await.unwrap_err().code(),
        Code::FailedPrecondition
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
