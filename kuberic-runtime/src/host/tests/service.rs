use super::coordinator::scale_up_model;
use crate::protocol::command::{KubernetesChange, ProtocolCommand, ScaleDownResource};
use crate::protocol::observation::*;
use crate::protocol::types::{
    AcceptedStatus, AcceptedTopology, CleanupResourceIdentity, ReplicaCleanupIdentity,
    ScaleUpCleanup, ScaleUpReceipt, ScaleUpWitness, derive_replica_endpoint_name,
};
use crate::test_controller::Plan;
use crate::test_controller::evaluate;
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use super::tempdir;
use crate::Result as RuntimeResult;
use crate::RuntimeError;
use crate::application::{
    CopyChunk, DurableApplicationAck, DurableApplicationProgress, OpenContext, Operation,
    OperationDataStream, RoleChange, StateProvider, StatefulServiceReplica,
};
use crate::authority::{
    AdmittedAuthority, BuildAuthority, BuildAuthorityKind, BuildAuthorityStore, BuildProgressStore,
    DurableBuildProgress, ReplicaAuthorityStore,
};
use crate::control::proto;
use crate::effects::{RuntimeEffect, RuntimeEffectAction};
use crate::engine::{DurableState, RetainedOperationStream};
use crate::host::Result as AgentResult;
use crate::host::hosting::PodRuntime;
use crate::host::process::{ApplicationStorageState, ReplicaHost, ReplicaProcessConfig};
use crate::host::provisioning::ObservedStorageIdentity;
use crate::host::runtime_adapter::RuntimeAdapter;
use crate::host::service::AgentService;
use crate::host::service::InitializationService;
use crate::host::sqlite_store::SqliteStore;
use crate::host::state::{AgentState, SCHEMA_VERSION, StorageIdentity};
use crate::host::store::AgentStore;
use crate::host::transport::{
    GrpcOutboundDispatcher, ReliableTransport, ReplicaEndpointResolver, run_outbound,
};
use crate::protocol::command::{EnsureConfiguration, EnsureReplicaBuild};
use crate::protocol::types::{
    AccessStatus, ConfigurationDescriptor, ConfigurationId, ConfigurationMember, EffectivePolicy,
    Epoch, FaultType, OperationId, PodUid, ProvisioningIntent, ProvisioningPurpose, PvcUid,
    ReplicaId, ReplicaIdentity, ReplicaInstanceId, ReplicaRole, ResourceUid,
    ScaleUpConfigurationEvidence, ScaleUpIntent, ScaleUpProvisioning, ScaleUpStage,
    SwitchoverHandoff, SwitchoverRequestId, TransitionKind, derive_agent_generation,
    derive_initialization_id,
};
use crate::replicator::stream::OperationMetadata;
use crate::replicator::stream::OperationStream;
use crate::replicator::{
    Replicator, ReplicatorFactory, ReplicatorFactoryContext, ReplicatorInterfaces,
    ReplicatorSettings, StateReplicator,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use tokio::sync::{Mutex as AsyncMutex, Notify, watch};
use tonic::{Code, Request};

use crate::removal_fixture as scale_down_fixture;

#[test]
fn secondary_removal_rpc_replays_exact_receipts_in_new_sessions() {
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(secondary_removal_rpc_replay());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn secondary_removal_rpc_replay() {
    use crate::authority::{AdmittedAuthority, ReplicaAuthorityStore};
    use crate::protocol::types::{AccessStatus, ReplicaRole};
    let intent = scale_down_fixture::intent(&[1, 2], 1);
    for retiring in [false, true] {
        let local = if retiring {
            intent.target.clone()
        } else {
            intent.primary.clone()
        };
        let directory = tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let provenance = StorageIdentity {
            schema_version: SCHEMA_VERSION,
            resource_uid: intent.resource_uid.clone(),
            local_identity: local.clone(),
            pod_uid: PodUid::new(local.instance_id.as_str()),
            pvc_uid: PvcUid::new(format!("pvc-{}", local.replica_id)),
            initialization_id: crate::protocol::types::InitializationId::new(
                "original-initialization",
            ),
            effective_policy: intent.previous_policy.clone(),
        };
        let mut state = AgentState::new(provenance.clone());
        state.current_configuration = Some(intent.previous_configuration.clone());
        state.highest_epoch = intent.previous_configuration.epoch;
        state.role = if retiring {
            ReplicaRole::ActiveSecondary
        } else {
            ReplicaRole::Primary
        };
        state.read_status = AccessStatus::Granted;
        state.write_status = if retiring {
            AccessStatus::NotPrimary
        } else {
            AccessStatus::Granted
        };
        let store = SqliteStore::create_authorized(&path, state).unwrap();
        store
            .admit(&AdmittedAuthority {
                local_identity: local.clone(),
                transition_kind: None,
                previous_configuration: None,
                current_configuration: intent.previous_configuration.clone(),
                switchover_handoff: None,
                scale_up: None,
                secondary_removal: None,
            })
            .await
            .unwrap();
        drop(store);
        let mut old_session = String::new();
        let mut terminal_preparation = None;
        let mut terminal_retirement = None;
        let mut terminal_commit: Option<crate::protocol::types::SecondaryScaleDownCleanup> = None;
        for restart in 0..3 {
            let store = Arc::new(SqliteStore::open_existing(&path, Some(&provenance)).unwrap());
            let runtime = Arc::new(PodRuntime::new(
                local.clone(),
                Arc::new(ReplayApplication),
                store.clone(),
            ));
            let service =
                AgentService::new(store.clone(), runtime.clone(), runtime.clone(), "token")
                    .unwrap();
            let session = service.sessions().local_session().to_string();
            assert_ne!(session, old_session);
            let control = free_address();
            let replication = free_address();
            let (ready, mut ready_rx) = watch::channel(false);
            let (shutdown, shutdown_rx) = watch::channel(false);
            let server = tokio::spawn(service.serve(control, replication, ready, shutdown_rx));
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                ready_rx.wait_for(|ready| *ready),
            )
            .await
            .unwrap()
            .unwrap();
            let mut client = proto::agent_control_client::AgentControlClient::connect(format!(
                "http://{control}"
            ))
            .await
            .unwrap();
            let command = if let Some(committed) = &terminal_commit {
                proto::execute_command_request::Command::AcceptSecondaryRemovalCommit(Box::new(
                    crate::protocol::command::AcceptSecondaryRemovalCommit {
                        operation_id: intent.command_operation_id(
                            crate::protocol::types::SecondaryRemovalStage::AcceptCommit,
                            &local,
                        ),
                        target: local.clone(),
                        committed: committed.clone(),
                        local_recovery: false,
                    }
                    .into(),
                ))
            } else if retiring {
                proto::execute_command_request::Command::RetireReplica(Box::new(
                    scale_down_fixture::retire_command(&intent).into(),
                ))
            } else {
                proto::execute_command_request::Command::PrepareSecondaryRemoval(Box::new(
                    scale_down_fixture::prepare_command(&intent).into(),
                ))
            };
            let request = |session: &str, command| {
                let mut request = Request::new(proto::ExecuteCommandRequest {
                    protocol_version: crate::protocol::PROTOCOL_VERSION,
                    resource_uid: intent.resource_uid.to_string(),
                    target: Some(local.clone().into()),
                    expected_process_session_id: session.into(),
                    command: Some(command),
                });
                request.metadata_mut().insert(
                    "authorization",
                    format!("{} {}", "Bearer", "token").parse().unwrap(),
                );
                request
            };
            let before = store.load_state().await.unwrap();
            assert_eq!(
                client
                    .execute(request(&old_session, command.clone()))
                    .await
                    .unwrap_err()
                    .code(),
                if old_session.is_empty() {
                    Code::InvalidArgument
                } else {
                    Code::FailedPrecondition
                }
            );
            assert_eq!(store.load_state().await.unwrap(), before);
            let report = client
                .execute(request(&session, command.clone()))
                .await
                .unwrap()
                .into_inner()
                .observation
                .unwrap();
            crate::control::normalize_agent_status_report(report.clone()).unwrap();
            assert_eq!(report.process_session_id, session);
            if restart == 0 {
                terminal_preparation = report.prepared_secondary_removal.clone();
                terminal_retirement = report.retired_replica.clone();
            } else if terminal_commit.is_none() {
                assert_eq!(report.prepared_secondary_removal, terminal_preparation);
                assert_eq!(report.retired_replica, terminal_retirement);
            } else {
                assert_eq!(
                    report.accepted_secondary_removal,
                    terminal_commit.clone().map(Into::into)
                );
                assert!(report.prepared_secondary_removal.is_none());
            }
            let duplicate = client
                .execute(request(&session, command.clone()))
                .await
                .unwrap()
                .into_inner()
                .observation
                .unwrap();
            assert!(duplicate.report_sequence > report.report_sequence);
            if retiring {
                assert_eq!(report.role, proto::ReplicaRole::None as i32);
                assert!(!runtime.snapshot().await.open);
                assert!(report.retired_replica.as_ref().unwrap().application_closed);
            } else if restart == 1 {
                let preparation: crate::protocol::types::SecondaryRemovalPreparation = report
                    .prepared_secondary_removal
                    .unwrap()
                    .try_into()
                    .unwrap();
                let mut joint = scale_down_fixture::configuration_command(&intent, false);
                let evidence = joint.secondary_removal_evidence.as_mut().unwrap();
                evidence.preparation = preparation;
                evidence.reduced_write_quorum.clear();
                for witness in &mut evidence.previous_read_quorum {
                    witness.process_session_id = evidence.preparation.process_session_id.clone();
                    witness.report_sequence = evidence.preparation.report_sequence + 1;
                    witness.verified_replication_lsn = 0;
                }
                let mut reduced = joint.clone();
                reduced.current_only = true;
                reduced.operation_id = intent.command_operation_id(
                    crate::protocol::types::SecondaryRemovalStage::CurrentOnly,
                    &local,
                );
                reduced.previous_configuration = None;
                reduced.previous_epoch = None;
                reduced
                    .secondary_removal_evidence
                    .as_mut()
                    .unwrap()
                    .reduced_write_quorum = scale_down_fixture::witnesses(
                    &intent,
                    crate::protocol::types::SecondaryRemovalStage::PreviousCurrent,
                );
                for cmd in [joint, reduced] {
                    let command = proto::EnsureConfigurationCommand {
                        operation_id: cmd.operation_id.to_string(),
                        previous_configuration: cmd.previous_configuration.map(Into::into),
                        current_configuration: Some(cmd.current_configuration.into()),
                        previous_epoch: cmd.previous_epoch.map(Into::into),
                        current_epoch: Some(cmd.current_epoch.into()),
                        effective_policy: Some(cmd.effective_policy.into()),
                        previous_policy: cmd.previous_policy.map(Into::into),
                        secondary_removal_evidence: cmd.secondary_removal_evidence.map(Into::into),
                        scale_up_evidence: cmd.scale_up_evidence.map(|evidence| (*evidence).into()),
                        local_replica_id: cmd.local_replica_id.value(),
                        expected_instance_id: cmd.expected_instance_id.to_string(),
                        expected_agent_generation: cmd.expected_agent_generation.to_string(),
                        transition_kind: proto::TransitionKind::SecondaryScaleDown as i32,
                        primary_write_status: proto::AccessStatus::ReconfigurationPending as i32,
                        current_only: cmd.current_only,
                        ..Default::default()
                    };
                    let envelope = proto::execute_command_request::Command::EnsureConfiguration(
                        Box::new(command),
                    );
                    let before = store.load_state().await.unwrap();
                    assert_eq!(
                        client
                            .execute(request(&old_session, envelope.clone()))
                            .await
                            .unwrap_err()
                            .code(),
                        Code::FailedPrecondition,
                    );
                    assert_eq!(store.load_state().await.unwrap(), before);
                    let report = client
                        .execute(request(&session, envelope.clone()))
                        .await
                        .unwrap()
                        .into_inner()
                        .observation
                        .unwrap();
                    crate::control::normalize_agent_status_report(report.clone()).unwrap();
                    assert_ne!(report.write_status, proto::AccessStatus::Granted as i32);
                    let completed = store.load_state().await.unwrap();
                    client
                        .execute(request(&session, envelope.clone()))
                        .await
                        .unwrap();
                    assert_eq!(store.load_state().await.unwrap(), completed);
                    let mut conflict = envelope;
                    let proto::execute_command_request::Command::EnsureConfiguration(c) =
                        &mut conflict
                    else {
                        unreachable!()
                    };
                    c.primary_write_status = proto::AccessStatus::Granted as i32;
                    assert!(client.execute(request(&session, conflict)).await.is_err());
                    assert_eq!(store.load_state().await.unwrap(), completed);
                }
                assert_eq!(store.identity().await.unwrap(), provenance);
                assert_eq!(
                    store.load_state().await.unwrap().admitted_policy,
                    Some(intent.current_policy.clone())
                );
                let state = store.load_state().await.unwrap();
                let mut committed = scale_down_fixture::cleanup(&intent);
                committed.evidence = state.secondary_removal_evidence.clone().unwrap();
                let accept = crate::protocol::command::AcceptSecondaryRemovalCommit {
                    operation_id: intent.command_operation_id(
                        crate::protocol::types::SecondaryRemovalStage::AcceptCommit,
                        &local,
                    ),
                    target: local.clone(),
                    committed: committed.clone(),
                    local_recovery: false,
                };
                let command = proto::execute_command_request::Command::AcceptSecondaryRemovalCommit(
                    Box::new(accept.into()),
                );
                let before = store.load_state().await.unwrap();
                assert_eq!(
                    client
                        .execute(request(&old_session, command.clone()))
                        .await
                        .unwrap_err()
                        .code(),
                    Code::FailedPrecondition
                );
                assert_eq!(store.load_state().await.unwrap(), before);
                for _ in 0..2 {
                    let report = client
                        .execute(request(&session, command.clone()))
                        .await
                        .unwrap()
                        .into_inner()
                        .observation
                        .unwrap();
                    crate::control::normalize_agent_status_report(report.clone()).unwrap();
                    assert_eq!(
                        report.accepted_secondary_removal,
                        Some(committed.clone().into())
                    );
                    assert!(report.prepared_secondary_removal.is_none());
                }
                assert_eq!(
                    store.load_state().await.unwrap().accepted_secondary_removal,
                    Some(committed.clone())
                );
                terminal_commit = Some(committed);
            }
            old_session = session;
            shutdown.send_replace(true);
            server.await.unwrap().unwrap();
        }
    }
}

#[tokio::test]
async fn secondary_removal_contracts_are_rejected_without_durable_mutation() {
    use crate::authority::AdmittedAuthority;
    let intent = scale_down_fixture::intent(&[1, 2], 1);
    for retiring in [false, true] {
        let local = if retiring {
            intent.target.clone()
        } else {
            intent.primary.clone()
        };
        let state = AgentState::new(StorageIdentity {
            schema_version: SCHEMA_VERSION,
            resource_uid: intent.resource_uid.clone(),
            pod_uid: PodUid::new(local.instance_id.as_str()),
            pvc_uid: PvcUid::new("exact-pvc"),
            initialization_id: derive_initialization_id(
                &intent.resource_uid,
                local.replica_id,
                &PodUid::new(local.instance_id.as_str()),
                &PvcUid::new("exact-pvc"),
            ),
            local_identity: local.clone(),
            effective_policy: intent.previous_policy.clone(),
        });
        assert!(
            crate::host::command::admit_configuration(
                &scale_down_fixture::configuration_command(&intent, false),
                &state
            )
            .is_err()
        );
        assert!(
            AdmittedAuthority {
                scale_up: None,
                secondary_removal: None,
                local_identity: local.clone(),
                transition_kind: Some(crate::protocol::types::TransitionKind::SecondaryScaleDown),
                previous_configuration: Some(intent.previous_configuration.clone()),
                current_configuration: intent.current_configuration.clone(),
                switchover_handoff: None,
            }
            .validate()
            .is_err()
        );
        let directory = tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let store = Arc::new(SqliteStore::create_authorized(&path, state).unwrap());
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
        let mut status = Request::new(proto::GetAgentStatusRequest {
            protocol_version: crate::protocol::PROTOCOL_VERSION,
            resource_uid: intent.resource_uid.to_string(),
            replica_id: local.replica_id.value(),
            expected_instance_id: local.instance_id.to_string(),
        });
        status
            .metadata_mut()
            .insert("authorization", "Bearer token".parse().unwrap());
        let session = client
            .get_status(status)
            .await
            .unwrap()
            .into_inner()
            .process_session_id;
        let command = if retiring {
            proto::execute_command_request::Command::RetireReplica(Box::new(
                scale_down_fixture::retire_command(&intent).into(),
            ))
        } else {
            proto::execute_command_request::Command::PrepareSecondaryRemoval(Box::new(
                scale_down_fixture::prepare_command(&intent).into(),
            ))
        };
        let before = store.load_state().await.unwrap();
        for session in [session, "obsolete-session".into()] {
            let mut request = Request::new(proto::ExecuteCommandRequest {
                protocol_version: crate::protocol::PROTOCOL_VERSION,
                resource_uid: intent.resource_uid.to_string(),
                target: Some(local.clone().into()),
                expected_process_session_id: session,
                command: Some(command.clone()),
            });
            request
                .metadata_mut()
                .insert("authorization", "Bearer token".parse().unwrap());
            assert_eq!(
                client.execute(request).await.unwrap_err().code(),
                Code::FailedPrecondition
            );
            assert_eq!(store.load_state().await.unwrap(), before);
        }
        shutdown_tx.send(true).unwrap();
        server.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn bound_agent_listeners_remain_owned_until_shutdown() {
    let local = identity();
    let resource_uid = ResourceUid::new("resource-1");
    let pod_uid = PodUid::new("pod-1");
    let pvc_uid = PvcUid::new("pvc-1");
    let initialization_id =
        derive_initialization_id(&resource_uid, local.replica_id, &pod_uid, &pvc_uid);
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let store = Arc::new(
        SqliteStore::create_authorized(
            &path,
            AgentState::new(StorageIdentity {
                schema_version: SCHEMA_VERSION,
                resource_uid,
                local_identity: local.clone(),
                pod_uid,
                pvc_uid,
                initialization_id,
                effective_policy: EffectivePolicy::fixed(1, 0).unwrap(),
            }),
        )
        .unwrap(),
    );
    let runtime = Arc::new(PodRuntime::new(
        local,
        Arc::new(ReplayApplication),
        store.clone(),
    ));
    let service = AgentService::new(store, runtime.clone(), runtime, "token").unwrap();
    let control = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let replication = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let control_address = control.local_addr().unwrap();
    let replication_address = replication.local_addr().unwrap();
    let (ready, mut ready_rx) = watch::channel(false);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let server =
        tokio::spawn(service.serve_with_listeners(control, replication, ready, shutdown_rx));

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        ready_rx.wait_for(|ready| *ready),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::net::TcpStream::connect(control_address)
        .await
        .unwrap();
    assert_eq!(
        TcpListener::bind(control_address).unwrap_err().kind(),
        std::io::ErrorKind::AddrInUse
    );
    assert_eq!(
        TcpListener::bind(replication_address).unwrap_err().kind(),
        std::io::ErrorKind::AddrInUse
    );

    shutdown.send_replace(true);
    server.await.unwrap().unwrap();
    TcpListener::bind(control_address).unwrap();
    TcpListener::bind(replication_address).unwrap();
}

struct NoopApplication {
    streams: Mutex<Vec<OperationStream>>,
}

struct NoopFactory;

struct ReplayApplication;

struct LoseCancelledBuildReply {
    runtime: Arc<PodRuntime>,
    lose_once: AtomicBool,
}

#[derive(Clone, Copy, Debug)]
enum RetirementCut {
    BeforeConsume,
    AfterConsume,
    AfterRetire,
}

struct CutRetirementReply {
    runtime: Arc<PodRuntime>,
    cut: RetirementCut,
    lose_once: AtomicBool,
}

#[async_trait]
impl crate::host::runtime_adapter::RuntimeEffectExecutor for CutRetirementReply {
    async fn apply_runtime_effect(
        &self,
        effect: RuntimeEffect,
    ) -> AgentResult<crate::effects::RuntimeEffectResult> {
        let lose = matches!(effect.action, RuntimeEffectAction::RetireBuild(_))
            && matches!(self.cut, RetirementCut::AfterRetire)
            && self.lose_once.swap(false, Ordering::SeqCst);
        let result =
            <PodRuntime as crate::host::runtime_adapter::RuntimeEffectExecutor>::apply_runtime_effect(
                self.runtime.as_ref(),
                effect,
            )
            .await?;
        if lose {
            return Err(crate::host::HostError::SessionRejected(
                "injected lost build-retirement reply".into(),
            ));
        }
        Ok(result)
    }

    async fn consume_cancelled_build_effect(
        &self,
        effect: RuntimeEffect,
    ) -> AgentResult<crate::effects::RuntimeEffectResult> {
        if matches!(self.cut, RetirementCut::BeforeConsume)
            && self.lose_once.swap(false, Ordering::SeqCst)
        {
            return Err(crate::host::HostError::SessionRejected(
                "injected pre-consume build-cancellation interruption".into(),
            ));
        }
        let result = <PodRuntime as crate::host::runtime_adapter::RuntimeEffectExecutor>::consume_cancelled_build_effect(
            self.runtime.as_ref(),
            effect,
        )
        .await?;
        if matches!(self.cut, RetirementCut::AfterConsume)
            && self.lose_once.swap(false, Ordering::SeqCst)
        {
            return Err(crate::host::HostError::SessionRejected(
                "injected lost cancelled-build consumption reply".into(),
            ));
        }
        Ok(result)
    }

    async fn cancel_build(&self, build_id: &OperationId) -> AgentResult<()> {
        <PodRuntime as crate::host::runtime_adapter::RuntimeEffectExecutor>::cancel_build(
            self.runtime.as_ref(),
            build_id,
        )
        .await
    }
}

#[async_trait]
impl crate::host::runtime_adapter::RuntimeEffectExecutor for LoseCancelledBuildReply {
    async fn apply_runtime_effect(
        &self,
        effect: RuntimeEffect,
    ) -> AgentResult<crate::effects::RuntimeEffectResult> {
        <PodRuntime as crate::host::runtime_adapter::RuntimeEffectExecutor>::apply_runtime_effect(
            self.runtime.as_ref(),
            effect,
        )
        .await
    }

    async fn consume_cancelled_build_effect(
        &self,
        effect: RuntimeEffect,
    ) -> AgentResult<crate::effects::RuntimeEffectResult> {
        let result = <PodRuntime as crate::host::runtime_adapter::RuntimeEffectExecutor>::consume_cancelled_build_effect(
            self.runtime.as_ref(),
            effect,
        )
        .await?;
        if self.lose_once.swap(false, Ordering::SeqCst) {
            return Err(crate::host::HostError::SessionRejected(
                "injected lost cancelled-build consumption reply".into(),
            ));
        }
        Ok(result)
    }

    async fn cancel_build(&self, build_id: &OperationId) -> AgentResult<()> {
        <PodRuntime as crate::host::runtime_adapter::RuntimeEffectExecutor>::cancel_build(
            self.runtime.as_ref(),
            build_id,
        )
        .await
    }
}

#[async_trait]
impl StatefulServiceReplica for ReplayApplication {
    async fn open(self: Arc<Self>, context: OpenContext) -> RuntimeResult<Arc<dyn Replicator>> {
        let provider = Arc::new(NoopApplication {
            streams: Mutex::new(Vec::new()),
        });
        let interfaces = context
            .partition
            .with_factory(Arc::new(crate::replicator::DefaultReplicatorFactory::new(
                self,
            )))
            .create_replicator(Some(provider), None)
            .await?;
        Ok(interfaces.replicator())
    }

    async fn change_role(
        &self,
        _role: crate::protocol::types::ReplicaRole,
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
impl crate::engine::DurableState for ReplayApplication {
    async fn get_replication_operations(
        &self,
        _: i64,
        _: i64,
    ) -> RuntimeResult<crate::engine::RetainedOperationStream> {
        Ok(Box::pin(stream::empty()))
    }
    async fn apply_copy_chunk(
        &self,
        _: &OperationId,
        _: u64,
        _: crate::application::CopyChunk,
    ) -> RuntimeResult<()> {
        panic!("retained command replay must not copy")
    }
    async fn verify_copy_chunk(
        &self,
        _: &OperationId,
        _: u64,
        _: &crate::application::CopyChunk,
    ) -> RuntimeResult<bool> {
        panic!("retained command replay must not copy")
    }
    async fn finish_copy(
        &self,
        _: &OperationId,
        _: i64,
        _: i64,
    ) -> RuntimeResult<crate::application::DurableApplicationProgress> {
        panic!("retained command replay must not copy")
    }
    async fn apply(
        &self,
        _: crate::application::Operation,
    ) -> RuntimeResult<crate::application::DurableApplicationAck> {
        panic!("retained command replay must not write")
    }
    async fn durable_progress(
        &self,
    ) -> RuntimeResult<crate::application::DurableApplicationProgress> {
        Ok(Default::default())
    }
    async fn verify_applied(&self, _: &crate::application::Operation) -> RuntimeResult<bool> {
        panic!("retained command replay must not write")
    }
    async fn commit(
        &self,
        lsn: i64,
    ) -> RuntimeResult<crate::application::DurableApplicationProgress> {
        assert_eq!(lsn, 0);
        Ok(Default::default())
    }
}

#[derive(Default)]
struct ResumableCopyApplication {
    chunks: Mutex<BTreeMap<(String, u64), Bytes>>,
    progress: Mutex<DurableApplicationProgress>,
    first_chunk_applied: AtomicUsize,
    pause_second_enumeration: AtomicBool,
    first_chunk_notify: Arc<Notify>,
    resume_second_notify: Arc<Notify>,
}

impl ResumableCopyApplication {
    fn source() -> Self {
        Self {
            pause_second_enumeration: AtomicBool::new(true),
            ..Self::default()
        }
    }

    fn applied_chunks(&self, build_id: &OperationId) -> Vec<(u64, Bytes)> {
        let mut chunks = self
            .chunks
            .lock()
            .unwrap()
            .iter()
            .filter(|((id, _), _)| id == build_id.as_str())
            .map(|((_, sequence), bytes)| (*sequence, bytes.clone()))
            .collect::<Vec<_>>();
        chunks.sort_by_key(|(sequence, _)| *sequence);
        chunks
    }
}

#[async_trait]
impl StatefulServiceReplica for ResumableCopyApplication {
    async fn open(self: Arc<Self>, context: OpenContext) -> RuntimeResult<Arc<dyn Replicator>> {
        let interfaces = context
            .partition
            .with_factory(Arc::new(crate::replicator::DefaultReplicatorFactory::new(
                self.clone(),
            )))
            .create_replicator(Some(self.clone()), None)
            .await?;
        let state_replicator = interfaces
            .state_replicator()
            .expect("operation/copy capability");
        for mut stream in [
            state_replicator.get_replication_stream().await?,
            state_replicator.get_copy_stream().await?,
        ] {
            let application = Arc::downgrade(&self);
            tokio::spawn(async move {
                while let Some(operation) = stream.get_operation().await.unwrap() {
                    let Some(application) = application.upgrade() else {
                        break;
                    };
                    let result = match &operation.metadata {
                        OperationMetadata::Replication { lsn, committed_lsn } => {
                            application
                                .apply(Operation {
                                    lsn: *lsn,
                                    committed_lsn: *committed_lsn,
                                    data: operation.data.clone(),
                                })
                                .await
                        }
                        OperationMetadata::Copy { build_id, sequence } => {
                            match application
                                .apply_copy_chunk(
                                    build_id,
                                    *sequence,
                                    CopyChunk {
                                        data: operation.data.clone(),
                                    },
                                )
                                .await
                            {
                                Ok(()) => application.durable_progress().await,
                                Err(error) => Err(error),
                            }
                        }
                        OperationMetadata::CopyComplete {
                            build_id,
                            up_to_lsn,
                            committed_lsn,
                        } => {
                            application
                                .finish_copy(build_id, *up_to_lsn, *committed_lsn)
                                .await
                        }
                    };
                    match result {
                        Ok(progress) => {
                            let _ = operation.acknowledge(progress);
                        }
                        Err(error) => {
                            let _ = operation.reject(error);
                        }
                    }
                }
            });
        }
        Ok(interfaces.replicator())
    }

    async fn change_role(&self, _role: ReplicaRole) -> RuntimeResult<RoleChange> {
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
impl StateProvider for ResumableCopyApplication {
    async fn update_epoch(
        &self,
        _epoch: Epoch,
        _previous_epoch_last_lsn: i64,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    async fn last_committed_lsn(&self) -> RuntimeResult<i64> {
        Ok(self.progress.lock().unwrap().committed_lsn)
    }

    async fn get_copy_context(&self) -> RuntimeResult<OperationDataStream> {
        Ok(Box::pin(stream::empty()))
    }

    async fn get_copy_state(
        &self,
        _up_to_lsn: i64,
        _copy_context: OperationDataStream,
    ) -> RuntimeResult<OperationDataStream> {
        let pause = self.pause_second_enumeration.swap(false, Ordering::SeqCst);
        let resume = self.resume_second_notify.clone();
        Ok(Box::pin(stream::unfold(0_u8, move |index| {
            let resume = resume.clone();
            async move {
                match index {
                    0 => Some((Ok(Bytes::from_static(b"partial-copy-first")), 1)),
                    1 => {
                        if pause {
                            resume.notified().await;
                        }
                        Some((Ok(Bytes::from_static(b"partial-copy-second")), 2))
                    }
                    _ => None,
                }
            }
        })))
    }

    async fn on_data_loss(&self) -> RuntimeResult<bool> {
        Ok(false)
    }
}

#[async_trait]
impl DurableState for ResumableCopyApplication {
    async fn get_replication_operations(
        &self,
        _from_lsn: i64,
        _to_lsn: i64,
    ) -> RuntimeResult<RetainedOperationStream> {
        Ok(Box::pin(stream::empty()))
    }

    async fn apply_copy_chunk(
        &self,
        build_id: &OperationId,
        sequence: u64,
        chunk: CopyChunk,
    ) -> RuntimeResult<()> {
        let key = (build_id.to_string(), sequence);
        let mut chunks = self.chunks.lock().unwrap();
        if let Some(existing) = chunks.get(&key)
            && existing != &chunk.data
        {
            return Err(RuntimeError::Application(
                "copy retry changed durable bytes".into(),
            ));
        }
        chunks.insert(key, chunk.data);
        if self.first_chunk_applied.fetch_add(1, Ordering::SeqCst) == 0 {
            self.first_chunk_notify.notify_waiters();
        }
        Ok(())
    }

    async fn verify_copy_chunk(
        &self,
        build_id: &OperationId,
        sequence: u64,
        chunk: &CopyChunk,
    ) -> RuntimeResult<bool> {
        Ok(self
            .chunks
            .lock()
            .unwrap()
            .get(&(build_id.to_string(), sequence))
            .is_some_and(|stored| stored == &chunk.data))
    }

    async fn finish_copy(
        &self,
        _build_id: &OperationId,
        up_to_lsn: i64,
        committed_lsn: i64,
    ) -> RuntimeResult<DurableApplicationProgress> {
        let mut progress = self.progress.lock().unwrap();
        progress.applied_lsn = progress.applied_lsn.max(up_to_lsn);
        progress.committed_lsn = progress.committed_lsn.max(committed_lsn);
        Ok(*progress)
    }

    async fn apply(&self, operation: Operation) -> RuntimeResult<DurableApplicationAck> {
        let mut progress = self.progress.lock().unwrap();
        progress.applied_lsn = progress.applied_lsn.max(operation.lsn);
        progress.committed_lsn = progress.committed_lsn.max(operation.committed_lsn);
        Ok(*progress)
    }

    async fn durable_progress(&self) -> RuntimeResult<DurableApplicationProgress> {
        Ok(*self.progress.lock().unwrap())
    }

    async fn verify_applied(&self, operation: &Operation) -> RuntimeResult<bool> {
        Ok(self.progress.lock().unwrap().applied_lsn >= operation.lsn)
    }

    async fn commit(&self, committed_lsn: i64) -> RuntimeResult<DurableApplicationProgress> {
        let mut progress = self.progress.lock().unwrap();
        progress.committed_lsn = progress.committed_lsn.max(committed_lsn);
        Ok(*progress)
    }
}

struct NoopReplicator {
    replication: Mutex<Option<OperationStream>>,
    copy: Mutex<Option<OperationStream>>,
    _senders: Vec<crate::replicator::stream::OperationSender>,
}

#[async_trait]
impl Replicator for NoopReplicator {
    async fn open(&self) -> RuntimeResult<String> {
        Ok("127.0.0.1:0".into())
    }

    async fn change_role(
        &self,
        _epoch: crate::protocol::types::Epoch,
        _role: crate::protocol::types::ReplicaRole,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    async fn update_epoch(&self, _epoch: crate::protocol::types::Epoch) -> RuntimeResult<()> {
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
        _state_provider: Option<Arc<dyn StateProvider>>,
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
            Some(replicator),
        ))
    }
}

#[async_trait]
impl StatefulServiceReplica for NoopApplication {
    async fn open(self: Arc<Self>, context: OpenContext) -> RuntimeResult<Arc<dyn Replicator>> {
        let interfaces = context
            .partition
            .with_factory(Arc::new(NoopFactory))
            .create_replicator(Some(self.clone()), None)
            .await?;
        let state = interfaces
            .state_replicator()
            .expect("operation/copy capability");
        *self.streams.lock().unwrap() = vec![
            state.get_replication_stream().await?,
            state.get_copy_stream().await?,
        ];
        Ok(interfaces.replicator())
    }

    async fn change_role(
        &self,
        _role: crate::protocol::types::ReplicaRole,
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
        _epoch: crate::protocol::types::Epoch,
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

struct ScaleUpSourceFixture {
    store: Arc<SqliteStore>,
    primary: ReplicaIdentity,
    target: ReplicaIdentity,
    build_id: OperationId,
    provisioning: ProvisioningIntent,
    current_policy: EffectivePolicy,
    current_only: EnsureConfiguration,
}

async fn scale_up_source_fixture(
    data_root: &std::path::Path,
    pc_cc_installed: bool,
    build_completed: bool,
) -> ScaleUpSourceFixture {
    std::fs::create_dir_all(data_root).unwrap();
    let resource_uid = ResourceUid::new("resource-1");
    let primary = identity();
    let secondary_initialization = derive_initialization_id(
        &resource_uid,
        ReplicaId::new(2),
        &PodUid::new("pod-2"),
        &PvcUid::new("pvc-2"),
    );
    let secondary = ReplicaIdentity {
        replica_id: ReplicaId::new(2),
        instance_id: ReplicaInstanceId::new("pod-2"),
        agent_generation: derive_agent_generation(&secondary_initialization),
    };
    let previous_policy = EffectivePolicy::fixed(2, 30).unwrap();
    let current_policy = EffectivePolicy::fixed(3, 30).unwrap();
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        primary.replica_id,
        vec![
            ConfigurationMember {
                identity: primary.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: secondary,
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        previous_policy.write_quorum,
    );
    let mut provisioning = ProvisioningIntent {
        purpose: ProvisioningPurpose::scale_up(ScaleUpProvisioning {
            resource_uid: resource_uid.clone(),
            spec_generation: 2,
            desired_replicas: 3,
            previous_configuration: previous.clone(),
            previous_policy: previous_policy.clone(),
            current_policy: current_policy.clone(),
            target_replica_id: ReplicaId::new(3),
        }),
        pod_uid: PodUid::new("pod-3"),
        pvc_uid: PvcUid::new("pvc-3"),
        operation_id: OperationId::default(),
    };
    provisioning.operation_id = provisioning.expected_operation_id();
    let target = provisioning.target_identity(&resource_uid);
    let build_id = provisioning.scale_up_build_id(&resource_uid).unwrap();
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        primary.replica_id,
        previous
            .members
            .iter()
            .cloned()
            .chain(std::iter::once(ConfigurationMember {
                identity: target.clone(),
                role: ReplicaRole::ActiveSecondary,
            }))
            .collect(),
        current_policy.write_quorum,
    );
    let mut intent = ScaleUpIntent {
        operation_id: OperationId::default(),
        resource_uid: resource_uid.clone(),
        spec_generation: 2,
        desired_replicas: 3,
        previous_configuration: previous.clone(),
        current_configuration: current.clone(),
        previous_policy: previous_policy.clone(),
        current_policy: current_policy.clone(),
        primary: primary.clone(),
        target: target.clone(),
        build_id: build_id.clone(),
        snapshot_boundary_lsn: 0,
        catch_up_boundary_lsn: 0,
    };
    intent.operation_id = intent.expected_operation_id();
    let evidence = ScaleUpConfigurationEvidence::Admission {
        intent: intent.clone(),
    };
    let storage = StorageIdentity {
        schema_version: SCHEMA_VERSION,
        resource_uid,
        pod_uid: PodUid::new("pod-1"),
        pvc_uid: PvcUid::new("pvc-1"),
        initialization_id: derive_initialization_id(
            &ResourceUid::new("resource-1"),
            primary.replica_id,
            &PodUid::new("pod-1"),
            &PvcUid::new("pvc-1"),
        ),
        local_identity: primary.clone(),
        effective_policy: if pc_cc_installed {
            current_policy.clone()
        } else {
            previous_policy.clone()
        },
    };
    let build_command = EnsureReplicaBuild {
        operation_id: build_id.clone(),
        local_replica_id: primary.replica_id,
        expected_instance_id: primary.instance_id.clone(),
        expected_agent_generation: primary.agent_generation.clone(),
        target: target.clone(),
        authority: None,
        source_session_id: None,
        retire: false,
    };
    let mut state = AgentState::new(storage);
    state.highest_epoch = if pc_cc_installed {
        current.epoch
    } else {
        previous.epoch
    };
    state.current_configuration = Some(if pc_cc_installed {
        current.clone()
    } else {
        previous.clone()
    });
    state.previous_configuration = pc_cc_installed.then(|| previous.clone());
    state.role = ReplicaRole::Primary;
    state.read_status = AccessStatus::Granted;
    state.write_status = AccessStatus::Granted;
    state.admitted_policy = Some(if pc_cc_installed {
        current_policy.clone()
    } else {
        previous_policy.clone()
    });
    state.previous_policy = pc_cc_installed.then(|| previous_policy.clone());
    state.scale_up_evidence = pc_cc_installed.then(|| Box::new(evidence.clone()));
    state.build_commands.insert(build_id.clone(), build_command);
    let path = SqliteStore::metadata_database_path(data_root);
    let store = Arc::new(SqliteStore::create_authorized(&path, state).unwrap());
    let build_authority = BuildAuthority {
        build_id: build_id.clone(),
        kind: BuildAuthorityKind::Provisioning,
        source: primary.clone(),
        target: target.clone(),
        current_configuration: previous.clone(),
        replication_boundary_lsn: 0,
    };
    store.admit_build(&build_authority).await.unwrap();
    store
        .record_build_progress(&DurableBuildProgress {
            authority: build_authority,
            last_sequence: u64::from(build_completed),
            durable_lsn: 0,
            completed: build_completed,
            catch_up_boundary_lsn: build_completed.then_some(0),
        })
        .await
        .unwrap();
    store
        .admit(&AdmittedAuthority {
            local_identity: primary.clone(),
            transition_kind: pc_cc_installed.then_some(TransitionKind::ScaleUp),
            previous_configuration: pc_cc_installed.then(|| previous.clone()),
            current_configuration: if pc_cc_installed {
                current.clone()
            } else {
                previous
            },
            switchover_handoff: None,
            scale_up: pc_cc_installed.then(|| Box::new(evidence.clone())),
            secondary_removal: None,
        })
        .await
        .unwrap();
    let current_only = EnsureConfiguration {
        operation_id: intent.command_operation_id(ScaleUpStage::CurrentOnly, &primary, &current),
        previous_configuration: None,
        current_configuration: current.clone(),
        previous_epoch: None,
        current_epoch: current.epoch,
        effective_policy: current_policy.clone(),
        previous_policy: Some(previous_policy),
        secondary_removal_evidence: None,
        scale_up_evidence: Some(Box::new(evidence)),
        local_replica_id: primary.replica_id,
        expected_instance_id: primary.instance_id.clone(),
        expected_agent_generation: primary.agent_generation.clone(),
        transition_kind: TransitionKind::ScaleUp,
        failover_safe_lsn: None,
        primary_write_status: AccessStatus::Granted,
        current_only: true,
        retire_build_ids: vec![build_id.clone()],
        switchover_handoff: None,
        retire_switchover_preparation_ids: Vec::new(),
    };
    ScaleUpSourceFixture {
        store,
        primary,
        target,
        build_id,
        provisioning,
        current_policy: current_policy.clone(),
        current_only,
    }
}

fn authorized_request(
    resource_uid: &str,
    target: &ReplicaIdentity,
    session: &str,
    command: proto::execute_command_request::Command,
) -> Request<proto::ExecuteCommandRequest> {
    let mut request = Request::new(proto::ExecuteCommandRequest {
        protocol_version: crate::protocol::PROTOCOL_VERSION,
        resource_uid: resource_uid.into(),
        target: Some(target.clone().into()),
        expected_process_session_id: session.into(),
        command: Some(command),
    });
    request.metadata_mut().insert(
        "authorization",
        format!("{} {}", "Bearer", "token").parse().unwrap(),
    );
    request
}

fn free_address() -> SocketAddr {
    let listener = TcpListener::bind((IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    listener.local_addr().unwrap()
}

#[derive(Clone)]
struct UnreachableResolver;

impl ReplicaEndpointResolver for UnreachableResolver {
    fn control_endpoint(&self, _identity: &ReplicaIdentity) -> String {
        "http://127.0.0.1:9".into()
    }

    fn replication_endpoint(&self, _identity: &ReplicaIdentity) -> String {
        "http://127.0.0.1:9".into()
    }
}

#[derive(Clone, Default)]
struct MutableResolver {
    endpoints: Arc<RwLock<BTreeMap<ReplicaIdentity, (SocketAddr, SocketAddr)>>>,
}

impl MutableResolver {
    fn set(&self, identity: ReplicaIdentity, control: SocketAddr, replication: SocketAddr) {
        self.endpoints
            .write()
            .unwrap()
            .insert(identity, (control, replication));
    }
}

impl ReplicaEndpointResolver for MutableResolver {
    fn control_endpoint(&self, identity: &ReplicaIdentity) -> String {
        let endpoint = self.endpoints.read().unwrap()[identity].0;
        format!("http://{endpoint}")
    }

    fn replication_endpoint(&self, identity: &ReplicaIdentity) -> String {
        let endpoint = self.endpoints.read().unwrap()[identity].1;
        format!("http://{endpoint}")
    }
}

async fn service_status(control: SocketAddr, target: &ReplicaIdentity) -> proto::AgentStatusReport {
    let mut client =
        proto::agent_control_client::AgentControlClient::connect(format!("http://{control}"))
            .await
            .unwrap();
    let mut request = Request::new(proto::GetAgentStatusRequest {
        protocol_version: crate::protocol::PROTOCOL_VERSION,
        resource_uid: "resource-1".into(),
        replica_id: target.replica_id.value(),
        expected_instance_id: target.instance_id.to_string(),
    });
    request.metadata_mut().insert(
        "authorization",
        format!("{} {}", "Bearer", "token").parse().unwrap(),
    );
    client.get_status(request).await.unwrap().into_inner()
}

#[tokio::test]
async fn scale_up_source_service_startup_restores_completed_evidence_without_replaying_delivery() {
    let directory = tempdir().unwrap();
    let fixture = scale_up_source_fixture(directory.path(), true, true).await;
    let runtime = Arc::new(PodRuntime::new(
        fixture.primary.clone(),
        Arc::new(ReplayApplication),
        fixture.store.clone(),
    ));
    let service = AgentService::new(
        fixture.store.clone(),
        runtime.clone(),
        runtime.clone(),
        "token",
    )
    .unwrap();
    let control = free_address();
    let replication = free_address();
    let (ready, mut ready_rx) = watch::channel(false);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let server = tokio::spawn(service.serve(control, replication, ready, shutdown_rx));
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        ready_rx.wait_for(|ready| *ready),
    )
    .await
    .unwrap()
    .unwrap();

    let status = service_status(control, &fixture.primary).await;
    assert_eq!(status.write_status, proto::AccessStatus::Granted as i32);
    assert!(status.builds.iter().any(|build| {
        build.build_id == fixture.build_id.as_str()
            && build.completed
            && build.catch_up_boundary_lsn == Some(0)
    }));
    assert!(
        runtime.snapshot().await.builds.is_empty(),
        "completed source evidence must not reopen outbound delivery"
    );
    let mut client =
        proto::agent_control_client::AgentControlClient::connect(format!("http://{control}"))
            .await
            .unwrap();
    let response = client
        .execute(authorized_request(
            "resource-1",
            &fixture.primary,
            &status.process_session_id,
            proto::execute_command_request::Command::EnsureConfiguration(Box::new(
                crate::control::configuration_command_to_proto(fixture.current_only.clone()),
            )),
        ))
        .await
        .unwrap()
        .into_inner()
        .observation
        .unwrap();
    assert_eq!(response.write_status, proto::AccessStatus::Granted as i32);
    assert!(response.builds.iter().any(|build| {
        build.build_id == fixture.build_id.as_str()
            && build.completed
            && build.catch_up_boundary_lsn == Some(0)
    }));
    assert!(
        fixture
            .store
            .load_state()
            .await
            .unwrap()
            .retired_builds
            .contains(&fixture.build_id)
    );
    shutdown.send_replace(true);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn scale_up_source_startup_ignores_incomplete_abandoned_delivery_until_reissued() {
    let directory = tempdir().unwrap();
    let fixture = scale_up_source_fixture(directory.path(), false, false).await;
    let runtime = Arc::new(PodRuntime::new(
        fixture.primary.clone(),
        Arc::new(ReplayApplication),
        fixture.store.clone(),
    ));
    let service = AgentService::new(
        fixture.store.clone(),
        runtime.clone(),
        runtime.clone(),
        "token",
    )
    .unwrap();
    let control = free_address();
    let replication = free_address();
    let (ready, mut ready_rx) = watch::channel(false);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let server = tokio::spawn(service.serve(control, replication, ready, shutdown_rx));
    ready_rx.wait_for(|ready| *ready).await.unwrap();
    let status = service_status(control, &fixture.primary).await;
    assert!(status.builds.iter().any(|build| {
        build.build_id == fixture.build_id.as_str()
            && !build.completed
            && build.catch_up_boundary_lsn.is_none()
    }));
    assert!(
        runtime.snapshot().await.builds.is_empty(),
        "startup must not resurrect incomplete source delivery without a fresh controller command"
    );
    shutdown.send_replace(true);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn scale_up_transport_cancellation_reissues_exact_build_and_resumes_partial_copy() {
    let source_directory = tempdir().unwrap();
    let target_directory = tempdir().unwrap();
    let fixture = scale_up_source_fixture(source_directory.path(), false, false).await;

    let mut target_state = AgentState::new(StorageIdentity {
        schema_version: SCHEMA_VERSION,
        resource_uid: ResourceUid::new("resource-1"),
        pod_uid: fixture.provisioning.pod_uid.clone(),
        pvc_uid: fixture.provisioning.pvc_uid.clone(),
        initialization_id: fixture
            .provisioning
            .initialization_id(&ResourceUid::new("resource-1")),
        local_identity: fixture.target.clone(),
        effective_policy: fixture.current_policy.clone(),
    });
    target_state.scale_up_initialization = Some(fixture.provisioning.clone());
    let target_store = Arc::new(
        SqliteStore::create_authorized(
            SqliteStore::metadata_database_path(target_directory.path()),
            target_state,
        )
        .unwrap(),
    );

    let target_application = Arc::new(ResumableCopyApplication::default());
    let target_runtime = Arc::new(PodRuntime::new(
        fixture.target.clone(),
        target_application.clone(),
        target_store.clone(),
    ));
    let target_service = AgentService::new(
        target_store.clone(),
        target_runtime.clone(),
        target_runtime.clone(),
        "token",
    )
    .unwrap();
    let target_control = free_address();
    let target_replication = free_address();
    let (target_ready, mut target_ready_rx) = watch::channel(false);
    let (target_shutdown, target_shutdown_rx) = watch::channel(false);
    let target_server = tokio::spawn(target_service.serve(
        target_control,
        target_replication,
        target_ready,
        target_shutdown_rx,
    ));
    target_ready_rx.wait_for(|ready| *ready).await.unwrap();

    let source_application = Arc::new(ResumableCopyApplication::source());
    let source_runtime = Arc::new(PodRuntime::new(
        fixture.primary.clone(),
        source_application.clone(),
        fixture.store.clone(),
    ));
    let source_service = AgentService::new(
        fixture.store.clone(),
        source_runtime.clone(),
        source_runtime.clone(),
        "token",
    )
    .unwrap();
    let source_session = source_service.sessions().local_session().clone();
    let source_control = free_address();
    let source_replication = free_address();
    let (source_ready, mut source_ready_rx) = watch::channel(false);
    let (source_shutdown, source_shutdown_rx) = watch::channel(false);
    let source_server = tokio::spawn(source_service.serve(
        source_control,
        source_replication,
        source_ready,
        source_shutdown_rx,
    ));
    source_ready_rx.wait_for(|ready| *ready).await.unwrap();

    let resolver = Arc::new(MutableResolver::default());
    resolver.set(fixture.target.clone(), target_control, target_replication);
    let transport = Arc::new(AsyncMutex::new(
        ReliableTransport::new(source_session, 64).unwrap(),
    ));
    let dispatcher = Arc::new(
        GrpcOutboundDispatcher::new(
            source_runtime.build_runtime(),
            transport.clone(),
            resolver.clone(),
            "resource-1",
            "token",
            std::time::Duration::from_secs(2),
        )
        .unwrap(),
    );
    let (outbound_shutdown, outbound_shutdown_rx) = watch::channel(false);
    let outbound = tokio::spawn(run_outbound(
        Arc::new(source_runtime.outbound_runtime()),
        transport,
        dispatcher,
        outbound_shutdown_rx,
    ));

    let source_status = service_status(source_control, &fixture.primary).await;
    let exact_build = proto::EnsureReplicaBuildCommand {
        operation_id: fixture.build_id.to_string(),
        local_replica_id: fixture.primary.replica_id.value(),
        expected_instance_id: fixture.primary.instance_id.to_string(),
        expected_agent_generation: fixture.primary.agent_generation.to_string(),
        target: Some(fixture.target.clone().into()),
        authority: None,
        source_session_id: String::new(),
        retire: false,
    };
    let first_request = authorized_request(
        "resource-1",
        &fixture.primary,
        &source_status.process_session_id,
        proto::execute_command_request::Command::EnsureReplicaBuild(exact_build.clone()),
    );
    let first = tokio::spawn(async move {
        proto::agent_control_client::AgentControlClient::connect(format!("http://{source_control}"))
            .await
            .unwrap()
            .execute(first_request)
            .await
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        target_application.first_chunk_notify.notified(),
    )
    .await
    .expect("candidate durably accepted the first copy chunk");
    let partial = target_application.applied_chunks(&fixture.build_id);
    assert_eq!(partial.len(), 1);
    let first_bytes = partial[0].1.clone();
    resolver.set(
        fixture.target.clone(),
        "127.0.0.1:9".parse().unwrap(),
        "127.0.0.1:9".parse().unwrap(),
    );
    source_application.resume_second_notify.notify_waiters();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), first)
            .await
            .expect("asynchronous source build dispatch completed")
            .unwrap()
            .is_ok()
    );
    let cancelled = fixture.store.load_state().await.unwrap();
    assert!(
        cancelled.pending_effect.is_none(),
        "retryable transport cancellation must clear the active source effect"
    );
    assert!(!cancelled.abandoned_builds.contains(&fixture.build_id));
    let source_partial = service_status(source_control, &fixture.primary).await;
    let source_build = source_partial
        .builds
        .iter()
        .find(|build| build.build_id == fixture.build_id.as_str())
        .unwrap();
    let frozen_snapshot_boundary = source_build.replication_boundary_lsn;
    assert!(!source_build.completed);

    resolver.set(fixture.target.clone(), target_control, target_replication);
    let mut retry_client = proto::agent_control_client::AgentControlClient::connect(format!(
        "http://{source_control}"
    ))
    .await
    .unwrap();
    let retried = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        retry_client.execute(authorized_request(
            "resource-1",
            &fixture.primary,
            &source_status.process_session_id,
            proto::execute_command_request::Command::EnsureReplicaBuild(exact_build),
        )),
    )
    .await
    .expect("exact build retry completed")
    .unwrap()
    .into_inner()
    .observation
    .unwrap();
    assert!(
        retried
            .builds
            .iter()
            .any(|build| build.build_id == fixture.build_id.as_str())
    );
    let completed_source = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let status = service_status(source_control, &fixture.primary).await;
            if let Some(build) = status
                .builds
                .into_iter()
                .find(|build| build.build_id == fixture.build_id.as_str() && build.completed)
            {
                break build;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("source observed exact asynchronous build completion");
    assert!(completed_source.completed);
    assert_eq!(
        completed_source.replication_boundary_lsn,
        frozen_snapshot_boundary
    );
    assert_eq!(completed_source.catch_up_boundary_lsn, Some(0));

    let completed_target = service_status(target_control, &fixture.target).await;
    let target_build = completed_target
        .builds
        .iter()
        .find(|build| build.build_id == fixture.build_id.as_str())
        .unwrap();
    assert!(target_build.completed);
    assert_eq!(
        target_build.replication_boundary_lsn,
        frozen_snapshot_boundary
    );
    assert_eq!(
        target_build.catch_up_boundary_lsn,
        completed_source.catch_up_boundary_lsn
    );
    let copied = target_application.applied_chunks(&fixture.build_id);
    assert_eq!(copied.len(), 2);
    assert_eq!(copied[0].1, first_bytes);
    assert_eq!(copied[1].1, Bytes::from_static(b"partial-copy-second"));

    outbound_shutdown.send_replace(true);
    outbound.await.unwrap().unwrap();
    source_shutdown.send_replace(true);
    target_shutdown.send_replace(true);
    source_server.await.unwrap().unwrap();
    target_server.await.unwrap().unwrap();
}

#[tokio::test]
async fn scale_up_source_service_abandons_an_in_flight_copy_and_restarts_retired() {
    let directory = tempdir().unwrap();
    let fixture = scale_up_source_fixture(directory.path(), false, false).await;
    let start = |store: Arc<SqliteStore>| {
        let primary = fixture.primary.clone();
        async move {
            let runtime = Arc::new(PodRuntime::new(
                primary,
                Arc::new(ReplayApplication),
                store.clone(),
            ));
            let service =
                AgentService::new(store, runtime.clone(), runtime.clone(), "token").unwrap();
            let control = free_address();
            let replication = free_address();
            let (ready, ready_rx) = watch::channel(false);
            let (shutdown, shutdown_rx) = watch::channel(false);
            let server = tokio::spawn(service.serve(control, replication, ready, shutdown_rx));
            (control, ready_rx, shutdown, server, runtime)
        }
    };
    let (control, mut ready_rx, shutdown, server, runtime) = start(fixture.store.clone()).await;
    ready_rx.wait_for(|ready| *ready).await.unwrap();
    let prior_sequence = fixture
        .store
        .load_state()
        .await
        .unwrap()
        .next_effect_sequence;
    RuntimeAdapter::new(fixture.store.clone(), runtime.clone())
        .execute(RuntimeEffect {
            operation_id: OperationId::new("pre-abandonment-refresh"),
            sequence: prior_sequence,
            action: RuntimeEffectAction::RefreshApplicationProgress,
        })
        .await
        .unwrap();
    let status = service_status(control, &fixture.primary).await;
    let build_request = proto::EnsureReplicaBuildCommand {
        operation_id: fixture.build_id.to_string(),
        local_replica_id: fixture.primary.replica_id.value(),
        expected_instance_id: fixture.primary.instance_id.to_string(),
        expected_agent_generation: fixture.primary.agent_generation.to_string(),
        target: Some(fixture.target.clone().into()),
        authority: None,
        source_session_id: String::new(),
        retire: false,
    };
    let build_client =
        proto::agent_control_client::AgentControlClient::connect(format!("http://{control}"))
            .await
            .unwrap();
    let build_primary = fixture.primary.clone();
    let build_session = status.process_session_id.clone();
    let pending_build_request = build_request.clone();
    let build = tokio::spawn(async move {
        let mut build_client = build_client;
        build_client
            .execute(authorized_request(
                "resource-1",
                &build_primary,
                &build_session,
                proto::execute_command_request::Command::EnsureReplicaBuild(pending_build_request),
            ))
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if fixture
                .store
                .load_state()
                .await
                .unwrap()
                .pending_effect
                .as_ref()
                .is_some_and(|pending| {
                    matches!(
                        &pending.effect.action,
                        RuntimeEffectAction::BuildReplica { build_id, .. }
                            if build_id == &fixture.build_id
                    )
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("real source copy effect became pending");

    let mut retire_client =
        proto::agent_control_client::AgentControlClient::connect(format!("http://{control}"))
            .await
            .unwrap();
    let retirement = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        retire_client.execute(authorized_request(
            "resource-1",
            &fixture.primary,
            &status.process_session_id,
            proto::execute_command_request::Command::EnsureReplicaBuild(
                proto::EnsureReplicaBuildCommand {
                    retire: true,
                    ..build_request
                },
            ),
        )),
    )
    .await
    .expect("retirement must cancel rather than wait for copy completion")
    .unwrap();
    assert!(retirement.into_inner().observation.is_some());
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), build)
            .await
            .expect("cancelled copy RPC completed")
            .unwrap()
            .is_err()
    );
    let retired = fixture.store.load_state().await.unwrap();
    assert!(retired.pending_effect.is_none());
    assert!(retired.abandoned_builds.contains(&fixture.build_id));
    assert!(retired.retired_builds.contains(&fixture.build_id));
    let retirement = retired.retained_result.as_ref().unwrap();
    assert!(matches!(
        &retirement.effect.action,
        RuntimeEffectAction::RetireBuild(build_id) if build_id == &fixture.build_id
    ));
    assert_eq!(
        retired.next_effect_sequence,
        retirement.effect.sequence + 1,
        "cancelled build and retirement must consume contiguous durable sequences"
    );
    RuntimeAdapter::new(fixture.store.clone(), runtime)
        .execute(RuntimeEffect {
            operation_id: OperationId::new("post-abandonment-refresh"),
            sequence: retired.next_effect_sequence,
            action: RuntimeEffectAction::RefreshApplicationProgress,
        })
        .await
        .expect("future runtime work must follow the consumed cancellation sequence");
    shutdown.send_replace(true);
    server.await.unwrap().unwrap();

    let (control, mut ready_rx, shutdown, server, _runtime) = start(fixture.store.clone()).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        ready_rx.wait_for(|ready| *ready),
    )
    .await
    .expect("retired source restart became ready")
    .unwrap();
    let restarted = service_status(control, &fixture.primary).await;
    assert!(restarted.builds.is_empty());
    assert_eq!(restarted.write_status, proto::AccessStatus::Granted as i32);
    shutdown.send_replace(true);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn scale_up_retirement_replays_lost_runtime_cancellation_consumption_reply() {
    let directory = tempdir().unwrap();
    let fixture = scale_up_source_fixture(directory.path(), false, false).await;
    let runtime = Arc::new(PodRuntime::new(
        fixture.primary.clone(),
        Arc::new(ReplayApplication),
        fixture.store.clone(),
    ));
    let executor = Arc::new(LoseCancelledBuildReply {
        runtime: runtime.clone(),
        lose_once: AtomicBool::new(true),
    });
    let service =
        AgentService::new(fixture.store.clone(), runtime.clone(), executor, "token").unwrap();
    let control = free_address();
    let replication = free_address();
    let (ready, mut ready_rx) = watch::channel(false);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let server = tokio::spawn(service.serve(control, replication, ready, shutdown_rx));
    ready_rx.wait_for(|ready| *ready).await.unwrap();

    let prior_sequence = fixture
        .store
        .load_state()
        .await
        .unwrap()
        .next_effect_sequence;
    RuntimeAdapter::new(fixture.store.clone(), runtime)
        .execute(RuntimeEffect {
            operation_id: OperationId::new("pre-lost-cancellation-refresh"),
            sequence: prior_sequence,
            action: RuntimeEffectAction::RefreshApplicationProgress,
        })
        .await
        .unwrap();
    let status = service_status(control, &fixture.primary).await;
    let build_request = proto::EnsureReplicaBuildCommand {
        operation_id: fixture.build_id.to_string(),
        local_replica_id: fixture.primary.replica_id.value(),
        expected_instance_id: fixture.primary.instance_id.to_string(),
        expected_agent_generation: fixture.primary.agent_generation.to_string(),
        target: Some(fixture.target.clone().into()),
        authority: None,
        source_session_id: String::new(),
        retire: false,
    };
    let mut build_client =
        proto::agent_control_client::AgentControlClient::connect(format!("http://{control}"))
            .await
            .unwrap();
    let build_primary = fixture.primary.clone();
    let build_session = status.process_session_id.clone();
    let pending_build = build_request.clone();
    let build = tokio::spawn(async move {
        build_client
            .execute(authorized_request(
                "resource-1",
                &build_primary,
                &build_session,
                proto::execute_command_request::Command::EnsureReplicaBuild(pending_build),
            ))
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if fixture
                .store
                .load_state()
                .await
                .unwrap()
                .pending_effect
                .as_ref()
                .is_some_and(|pending| {
                    matches!(
                        &pending.effect.action,
                        RuntimeEffectAction::BuildReplica { build_id, .. }
                            if build_id == &fixture.build_id
                    )
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("source build became pending");

    let retirement_request = || {
        authorized_request(
            "resource-1",
            &fixture.primary,
            &status.process_session_id,
            proto::execute_command_request::Command::EnsureReplicaBuild(
                proto::EnsureReplicaBuildCommand {
                    retire: true,
                    ..build_request.clone()
                },
            ),
        )
    };
    let mut retire_client =
        proto::agent_control_client::AgentControlClient::connect(format!("http://{control}"))
            .await
            .unwrap();
    assert!(
        retire_client.execute(retirement_request()).await.is_err(),
        "the injected lost reply must leave retirement incomplete"
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), build)
            .await
            .expect("cancelled build request completed")
            .unwrap()
            .is_err()
    );
    let lost = fixture.store.load_state().await.unwrap();
    assert!(lost.abandoned_builds.contains(&fixture.build_id));
    assert!(lost.pending_effect.as_ref().is_some_and(|pending| {
        matches!(
            &pending.effect.action,
            RuntimeEffectAction::BuildReplica { build_id, .. }
                if build_id == &fixture.build_id
        )
    }));
    assert!(!lost.retired_builds.contains(&fixture.build_id));

    retire_client
        .execute(retirement_request())
        .await
        .expect("retry must replay the consumed runtime sequence")
        .into_inner();
    let retired = fixture.store.load_state().await.unwrap();
    assert!(retired.pending_effect.is_none());
    assert!(retired.retired_builds.contains(&fixture.build_id));
    assert_eq!(
        retired.next_effect_sequence,
        retired.retained_result.as_ref().unwrap().effect.sequence + 1
    );

    shutdown.send_replace(true);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn scale_up_source_startup_finishes_retirement_after_abandonment_reply_loss() {
    let directory = tempdir().unwrap();
    let fixture = scale_up_source_fixture(directory.path(), false, false).await;
    let pending = RuntimeEffect {
        operation_id: OperationId::new(format!("{}:build-replica", fixture.build_id)),
        sequence: fixture
            .store
            .load_state()
            .await
            .unwrap()
            .next_effect_sequence,
        action: RuntimeEffectAction::BuildReplica {
            build_id: fixture.build_id.clone(),
            target: fixture.target.clone(),
            replication_address: String::new(),
        },
    };
    fixture.store.begin_effect(&pending).await.unwrap();
    fixture
        .store
        .abandon_build(&EnsureReplicaBuild {
            operation_id: fixture.build_id.clone(),
            local_replica_id: fixture.primary.replica_id,
            expected_instance_id: fixture.primary.instance_id.clone(),
            expected_agent_generation: fixture.primary.agent_generation.clone(),
            target: fixture.target.clone(),
            authority: None,
            source_session_id: None,
            retire: true,
        })
        .await
        .unwrap();
    let abandoned = fixture.store.load_state().await.unwrap();
    assert_eq!(
        abandoned
            .pending_effect
            .as_ref()
            .map(|pending| &pending.effect),
        Some(&pending)
    );
    assert!(abandoned.abandoned_builds.contains(&fixture.build_id));
    assert!(!abandoned.retired_builds.contains(&fixture.build_id));

    let runtime = Arc::new(PodRuntime::new(
        fixture.primary.clone(),
        Arc::new(ReplayApplication),
        fixture.store.clone(),
    ));
    let service =
        AgentService::new(fixture.store.clone(), runtime.clone(), runtime, "token").unwrap();
    let control = free_address();
    let replication = free_address();
    let (ready, mut ready_rx) = watch::channel(false);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let server = tokio::spawn(service.serve(control, replication, ready, shutdown_rx));
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        ready_rx.wait_for(|ready| *ready),
    )
    .await
    .expect("abandoned source startup became ready")
    .unwrap();
    let state = fixture.store.load_state().await.unwrap();
    assert!(state.retired_builds.contains(&fixture.build_id));
    assert!(state.pending_effect.is_none());
    let status = service_status(control, &fixture.primary).await;
    assert!(status.builds.is_empty());
    shutdown.send_replace(true);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn scale_up_source_service_retirement_survives_pre_admission_restart() {
    let directory = tempdir().unwrap();
    let fixture = scale_up_source_fixture(directory.path(), false, true).await;
    let start = |store: Arc<SqliteStore>| {
        let primary = fixture.primary.clone();
        async move {
            let runtime = Arc::new(PodRuntime::new(
                primary,
                Arc::new(ReplayApplication),
                store.clone(),
            ));
            let service = AgentService::new(store, runtime.clone(), runtime, "token").unwrap();
            let control = free_address();
            let replication = free_address();
            let (ready, ready_rx) = watch::channel(false);
            let (shutdown, shutdown_rx) = watch::channel(false);
            let server = tokio::spawn(service.serve(control, replication, ready, shutdown_rx));
            (control, ready_rx, shutdown, server)
        }
    };
    let (control, mut ready_rx, shutdown, server) = start(fixture.store.clone()).await;
    ready_rx.wait_for(|ready| *ready).await.unwrap();
    let status = service_status(control, &fixture.primary).await;
    assert!(
        status
            .builds
            .iter()
            .any(|build| build.build_id == fixture.build_id.as_str())
    );
    let mut client =
        proto::agent_control_client::AgentControlClient::connect(format!("http://{control}"))
            .await
            .unwrap();
    client
        .execute(authorized_request(
            "resource-1",
            &fixture.primary,
            &status.process_session_id,
            proto::execute_command_request::Command::EnsureReplicaBuild(
                proto::EnsureReplicaBuildCommand {
                    operation_id: fixture.build_id.to_string(),
                    local_replica_id: fixture.primary.replica_id.value(),
                    expected_instance_id: fixture.primary.instance_id.to_string(),
                    expected_agent_generation: fixture.primary.agent_generation.to_string(),
                    target: Some(fixture.target.clone().into()),
                    authority: None,
                    source_session_id: String::new(),
                    retire: true,
                },
            ),
        ))
        .await
        .unwrap();
    shutdown.send_replace(true);
    server.await.unwrap().unwrap();

    let (control, mut ready_rx, shutdown, server) = start(fixture.store.clone()).await;
    ready_rx.wait_for(|ready| *ready).await.unwrap();
    let restarted = service_status(control, &fixture.primary).await;
    assert!(restarted.builds.is_empty());
    assert_eq!(restarted.write_status, proto::AccessStatus::Granted as i32);
    shutdown.send_replace(true);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn replica_host_scale_up_source_recovery_covers_pc_cc_and_abandoned_build_cuts() {
    let pc_cc_directory = tempdir().unwrap();
    let pc_cc = scale_up_source_fixture(pc_cc_directory.path(), true, true).await;
    let pc_cc_control = free_address();
    let mut owner = ReplicaHost::new(
        ReplicaProcessConfig {
            resource_uid: ResourceUid::new("resource-1"),
            replica_id: pc_cc.primary.replica_id,
            pod_uid: PodUid::new(pc_cc.primary.instance_id.as_str()),
            pvc_uid: PvcUid::new("pvc-1"),
            data_root: pc_cc_directory.path().to_path_buf(),
            control_address: pc_cc_control,
            replication_address: free_address(),
            bearer_token: "token".into(),
            rpc_deadline: std::time::Duration::from_millis(50),
            transport_window_capacity: 8,
        },
        Arc::new(ReplayApplication),
        ApplicationStorageState::Established,
        Arc::new(UnreachableResolver),
    )
    .start()
    .await
    .unwrap();
    let diagnostics = owner.handle().diagnostics().await.unwrap();
    assert_eq!(diagnostics.write_status, "Granted");
    assert!(diagnostics.builds.is_empty());
    let status = service_status(pc_cc_control, &pc_cc.primary).await;
    let mut client =
        proto::agent_control_client::AgentControlClient::connect(format!("http://{pc_cc_control}"))
            .await
            .unwrap();
    client
        .execute(authorized_request(
            "resource-1",
            &pc_cc.primary,
            &status.process_session_id,
            proto::execute_command_request::Command::EnsureConfiguration(Box::new(
                crate::control::configuration_command_to_proto(pc_cc.current_only.clone()),
            )),
        ))
        .await
        .unwrap();
    owner.shutdown();
    owner.wait().await.unwrap();

    let cleanup_directory = tempdir().unwrap();
    let cleanup = scale_up_source_fixture(cleanup_directory.path(), false, true).await;
    let cleanup_control = free_address();
    let config = |control_address| ReplicaProcessConfig {
        resource_uid: ResourceUid::new("resource-1"),
        replica_id: cleanup.primary.replica_id,
        pod_uid: PodUid::new(cleanup.primary.instance_id.as_str()),
        pvc_uid: PvcUid::new("pvc-1"),
        data_root: cleanup_directory.path().to_path_buf(),
        control_address,
        replication_address: free_address(),
        bearer_token: "token".into(),
        rpc_deadline: std::time::Duration::from_millis(50),
        transport_window_capacity: 8,
    };
    let mut owner = ReplicaHost::new(
        config(cleanup_control),
        Arc::new(ReplayApplication),
        ApplicationStorageState::Established,
        Arc::new(UnreachableResolver),
    )
    .start()
    .await
    .unwrap();
    let status = service_status(cleanup_control, &cleanup.primary).await;
    let mut client = proto::agent_control_client::AgentControlClient::connect(format!(
        "http://{cleanup_control}"
    ))
    .await
    .unwrap();
    client
        .execute(authorized_request(
            "resource-1",
            &cleanup.primary,
            &status.process_session_id,
            proto::execute_command_request::Command::EnsureReplicaBuild(
                proto::EnsureReplicaBuildCommand {
                    operation_id: cleanup.build_id.to_string(),
                    local_replica_id: cleanup.primary.replica_id.value(),
                    expected_instance_id: cleanup.primary.instance_id.to_string(),
                    expected_agent_generation: cleanup.primary.agent_generation.to_string(),
                    target: Some(cleanup.target.clone().into()),
                    authority: None,
                    source_session_id: String::new(),
                    retire: true,
                },
            ),
        ))
        .await
        .unwrap();
    owner.shutdown();
    owner.wait().await.unwrap();
    let mut restarted = ReplicaHost::new(
        config(free_address()),
        Arc::new(ReplayApplication),
        ApplicationStorageState::Established,
        Arc::new(UnreachableResolver),
    )
    .start()
    .await
    .unwrap();
    let diagnostics = restarted.handle().diagnostics().await.unwrap();
    assert_eq!(diagnostics.write_status, "Granted");
    assert!(diagnostics.builds.is_empty());
    assert!(
        cleanup
            .store
            .load_state()
            .await
            .unwrap()
            .retired_builds
            .contains(&cleanup.build_id)
    );
    restarted.shutdown();
    restarted.wait().await.unwrap();
}

#[tokio::test]
async fn replica_host_startup_cancellation_joins_and_preserves_acknowledgement_errors() {
    struct StartupGate {
        opened: Notify,
        release: Notify,
        fault: bool,
        reject: bool,
        aborts: AtomicUsize,
    }
    #[async_trait]
    impl StatefulServiceReplica for StartupGate {
        async fn open(self: Arc<Self>, context: OpenContext) -> RuntimeResult<Arc<dyn Replicator>> {
            if self.fault {
                context.partition.report_fault(FaultType::Permanent).await?;
            }
            self.opened.notify_one();
            self.release.notified().await;
            if self.reject {
                return Err(RuntimeError::Application("startup gate rejected".into()));
            }
            Arc::new(ReplayApplication).open(context).await
        }
        async fn change_role(&self, _: ReplicaRole) -> RuntimeResult<RoleChange> {
            Ok(RoleChange {
                service_address: None,
            })
        }
        async fn close(&self) -> RuntimeResult<()> {
            Ok(())
        }
        fn abort(&self) {
            self.aborts.fetch_add(1, Ordering::SeqCst);
        }
    }

    for mode in ["cancel", "locked", "reject", "readiness-race", "ready"] {
        let directory = tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let store = SqliteStore::create_authorized(
            &path,
            AgentState::new(StorageIdentity {
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
            }),
        )
        .unwrap();
        let application = Arc::new(StartupGate {
            opened: Notify::new(),
            release: Notify::new(),
            fault: matches!(mode, "cancel" | "locked" | "reject"),
            reject: mode == "reject",
            aborts: AtomicUsize::new(0),
        });
        let control = free_address();
        let replication = free_address();
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(
            ReplicaHost::new(
                ReplicaProcessConfig {
                    resource_uid: ResourceUid::new("resource-1"),
                    replica_id: ReplicaId::new(1),
                    pod_uid: PodUid::new("pod-1"),
                    pvc_uid: PvcUid::new("pvc-1"),
                    data_root: directory.path().to_owned(),
                    control_address: control,
                    replication_address: replication,
                    bearer_token: "token".into(),
                    rpc_deadline: std::time::Duration::from_millis(50),
                    transport_window_capacity: 8,
                },
                application.clone(),
                ApplicationStorageState::Established,
                Arc::new(UnreachableResolver),
            )
            .start_with_shutdown(receiver),
        );
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            application.opened.notified(),
        )
        .await
        .unwrap();
        let lock = if mode == "locked" {
            let lock = rusqlite::Connection::open(&path).unwrap();
            lock.execute_batch("BEGIN IMMEDIATE").unwrap();
            Some(lock)
        } else {
            None
        };
        if matches!(mode, "reject" | "readiness-race" | "ready") {
            application.release.notify_one();
        }
        if !matches!(mode, "reject" | "ready") {
            shutdown.send_replace(true);
        }
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap();
        drop(lock);
        match (mode, result) {
            ("locked", Err(error)) => assert!(
                error.to_string().contains("database is locked")
                    || error.to_string().contains("persistence timed out"),
                "{error}"
            ),
            ("reject", Err(error)) => assert!(error.to_string().contains("startup gate rejected")),
            ("cancel" | "readiness-race", Ok(None)) => {}
            ("ready" | "readiness-race", Ok(Some(mut replica))) => {
                shutdown.send_replace(true);
                replica.shutdown();
                tokio::time::timeout(std::time::Duration::from_secs(5), replica.wait())
                    .await
                    .unwrap()
                    .unwrap();
            }
            (_, Err(error)) => panic!("{mode}: {error}"),
            _ => panic!("unexpected startup outcome for {mode}"),
        }
        assert_eq!(application.aborts.load(Ordering::SeqCst), 1, "{mode}");
        assert!(
            TcpListener::bind(control).is_ok(),
            "{mode}: control listener leaked"
        );
        assert!(
            TcpListener::bind(replication).is_ok(),
            "{mode}: replication listener leaked"
        );
        if matches!(mode, "cancel" | "reject") {
            assert_eq!(
                store.load_state().await.unwrap().reported_fault,
                Some(FaultType::Permanent)
            );
        }
    }
}

#[tokio::test]
async fn startup_and_shutdown_acknowledge_durable_partition_faults() {
    struct FaultApplication {
        mode: &'static str,
        opened: Notify,
        partition: Mutex<Option<crate::replicator::StatefulServicePartition>>,
    }

    #[async_trait]
    impl StatefulServiceReplica for FaultApplication {
        async fn open(self: Arc<Self>, context: OpenContext) -> RuntimeResult<Arc<dyn Replicator>> {
            *self.partition.lock().unwrap() = Some(context.partition.clone());
            if self.mode != "running" {
                context.partition.report_fault(FaultType::Permanent).await?;
                context.partition.report_fault(FaultType::Transient).await?;
            }
            self.opened.notify_one();
            match self.mode {
                "reject" => Err(RuntimeError::Application(
                    "injected startup rejection".into(),
                )),
                "cancel" => std::future::pending().await,
                _ => Arc::new(ReplayApplication).open(context).await,
            }
        }
        async fn change_role(&self, _: ReplicaRole) -> RuntimeResult<RoleChange> {
            Ok(RoleChange {
                service_address: None,
            })
        }
        async fn close(&self) -> RuntimeResult<()> {
            Ok(())
        }
        fn abort(&self) {}
    }

    for mode in ["reject", "cancel", "running"] {
        let directory = tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let store = Arc::new(
            SqliteStore::create_authorized(
                &path,
                AgentState::new(StorageIdentity {
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
                }),
            )
            .unwrap(),
        );
        let application = Arc::new(FaultApplication {
            mode,
            opened: Notify::new(),
            partition: Mutex::new(None),
        });
        let runtime = Arc::new(PodRuntime::new(
            identity(),
            application.clone(),
            store.clone(),
        ));
        let service =
            AgentService::new(store.clone(), runtime.clone(), runtime.clone(), "token").unwrap();
        let (ready, mut ready_rx) = watch::channel(false);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(service.serve(free_address(), free_address(), ready, shutdown_rx));
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            application.opened.notified(),
        )
        .await
        .unwrap();
        let partition = application.partition.lock().unwrap().clone().unwrap();
        if mode == "running" {
            ready_rx.wait_for(|ready| *ready).await.unwrap();
            partition.report_fault(FaultType::Permanent).await.unwrap();
            partition.report_fault(FaultType::Transient).await.unwrap();
        }
        if mode != "reject" {
            shutdown.send_replace(true);
        }
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
        match mode {
            "reject" => assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("injected startup rejection")
            ),
            "cancel" => assert!(matches!(
                result,
                Err(crate::host::HostError::Runtime(
                    RuntimeError::OperationCancelled
                ))
            )),
            _ => result.unwrap(),
        }
        assert!(!*ready_rx.borrow());
        assert_eq!(
            runtime.partition_report().await.reported_fault,
            Some(FaultType::Permanent)
        );
        assert_eq!(
            SqliteStore::open_existing(&path, None)
                .unwrap()
                .load_state()
                .await
                .unwrap()
                .reported_fault,
            Some(FaultType::Permanent),
            "completion must acknowledge durable persistence, without a status RPC"
        );
        assert!(
            matches!(
                tokio::time::timeout(
                    std::time::Duration::from_secs(1),
                    partition.report_fault(FaultType::Permanent)
                )
                .await
                .unwrap(),
                Err(RuntimeError::Closed)
            ),
            "a stopped consumer must reject promptly rather than waiting for itself"
        );
    }
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
        crate::protocol::types::PartitionInformation {
            partition_id: crate::protocol::types::PartitionId::new("partition-1"),
        },
        identity(),
        Arc::new(NoopApplication {
            streams: Mutex::new(Vec::new()),
        }),
        store.clone(),
    ));
    store
        .begin_effect(&RuntimeEffect {
            operation_id: crate::protocol::types::OperationId::new("pending-before-service-start"),
            sequence: 1,
            action: RuntimeEffectAction::SetReadStatus(
                crate::protocol::types::AccessStatus::NotPrimary,
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
        protocol_version: crate::protocol::PROTOCOL_VERSION,
        resource_uid: "resource-1".into(),
        replica_id: 1,
        expected_instance_id: "pod-1".into(),
    };
    assert_eq!(
        client.get_status(request.clone()).await.unwrap_err().code(),
        Code::Unauthenticated
    );
    let mut incompatible = Request::new(proto::GetAgentStatusRequest {
        protocol_version: crate::protocol::PROTOCOL_VERSION + 1,
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
        protocol_version: crate::protocol::PROTOCOL_VERSION,
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
                        crate::protocol::types::ConfigurationDescriptor::new(
                            crate::protocol::types::Epoch::new(0, 1),
                            crate::protocol::types::ReplicaId::new(1),
                            vec![crate::protocol::types::ConfigurationMember {
                                identity: identity(),
                                role: crate::protocol::types::ReplicaRole::Primary,
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
        crate::protocol::types::PartitionInformation {
            partition_id: crate::protocol::types::PartitionId::new("partition-1"),
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
        protocol_version: crate::protocol::PROTOCOL_VERSION,
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
        crate::protocol::types::PartitionInformation {
            partition_id: crate::protocol::types::PartitionId::new("partition-1"),
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
        protocol_version: crate::protocol::PROTOCOL_VERSION,
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

    let configuration = crate::protocol::types::ConfigurationDescriptor::new(
        crate::protocol::types::Epoch::new(0, 1),
        ReplicaId::new(1),
        vec![crate::protocol::types::ConfigurationMember {
            identity: identity(),
            role: crate::protocol::types::ReplicaRole::Primary,
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
            protocol_version: crate::protocol::PROTOCOL_VERSION,
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
                retire: false,
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
        agent_generation: crate::protocol::types::AgentGeneration::new("generation-2"),
    };
    let handoff = SwitchoverHandoff {
        preparation_generation: 1,
        preparation_operation_id: OperationId::new("prepare-1"),
        request_id: SwitchoverRequestId::new("request-1"),
        source: storage_identity.local_identity.clone(),
        target,
        starting_configuration_id: ConfigurationId::new("configuration-1"),
        starting_epoch: crate::protocol::types::Epoch::new(0, 1),
        handoff_lsn: 9,
    };
    let mut state = AgentState::new(storage_identity);
    state.prepared_switchover = Some(handoff.clone());
    drop(SqliteStore::create_authorized(&path, state).unwrap());
    let store = Arc::new(SqliteStore::open_existing(&path, None).unwrap());
    let runtime = Arc::new(PodRuntime::new_for_partition(
        crate::protocol::types::PartitionInformation {
            partition_id: crate::protocol::types::PartitionId::new("partition-1"),
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
        protocol_version: crate::protocol::PROTOCOL_VERSION,
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
    use crate::host::state::RetainedCommandResult;
    use crate::protocol::command::ProtocolCommand;
    use crate::protocol::types::{
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
            agent_generation: crate::protocol::types::AgentGeneration::new("generation-2"),
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
                crate::protocol::types::derive_switchover_preparation_operation_id(
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
            protocol_version: crate::protocol::PROTOCOL_VERSION,
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
            state.preparation_retirement = Some(crate::host::state::PreparationRetirement {
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
            state.preparation_retirement = Some(crate::host::state::PreparationRetirement {
                starting_configuration_id: starting.configuration_id.clone(),
                starting_epoch: starting.epoch,
                generation: 3,
            });
        }
        if let ProtocolCommand::EnsureConfiguration(command) =
            crate::control::normalize_execute_request(envelope.clone())
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
        use crate::authority::{AdmittedAuthority, ReplicaAuthorityStore};
        let persisted = store.load_state().await.unwrap();
        store
            .admit(&AdmittedAuthority {
                scale_up: None,
                secondary_removal: None,
                local_identity: local.clone(),
                transition_kind: persisted
                    .previous_configuration
                    .as_ref()
                    .map(|_| crate::protocol::types::TransitionKind::PlannedSwitchover),
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
            protocol_version: crate::protocol::PROTOCOL_VERSION,
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
                    crate::protocol::types::derive_switchover_preparation_operation_id(
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
        protocol_version: crate::protocol::PROTOCOL_VERSION,
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
    let configuration = crate::protocol::types::ConfigurationDescriptor::new(
        crate::protocol::types::Epoch::new(0, 1),
        ReplicaId::new(1),
        vec![crate::protocol::types::ConfigurationMember {
            identity: identity(),
            role: crate::protocol::types::ReplicaRole::Primary,
        }],
        1,
    );
    let mut initialize = Request::new(proto::ExecuteCommandRequest {
        protocol_version: crate::protocol::PROTOCOL_VERSION,
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

#[tokio::test]
async fn evaluator_cleanup_retires_real_incomplete_build_across_restart_cuts() {
    fn cleanup_model(
        fixture: &ScaleUpSourceFixture,
        permanent_candidate_failure: bool,
        include_prior_receipt: bool,
    ) -> scale_up_model::Model {
        let scale_up = fixture.provisioning.scale_up().unwrap();
        let previous = scale_up.previous_configuration.clone();
        let previous_policy = scale_up.previous_policy.clone();
        let secondary = previous
            .members
            .iter()
            .find(|member| member.identity != fixture.primary)
            .unwrap()
            .identity
            .clone();
        let prior_policy = EffectivePolicy::fixed(1, 30).unwrap();
        let prior_configuration = ConfigurationDescriptor::new(
            Epoch::new(
                previous.epoch.data_loss_number,
                previous.epoch.configuration_number.saturating_sub(1),
            ),
            fixture.primary.replica_id,
            vec![ConfigurationMember {
                identity: fixture.primary.clone(),
                role: ReplicaRole::Primary,
            }],
            prior_policy.write_quorum,
        );
        let mut prior_intent = ScaleUpIntent {
            operation_id: OperationId::default(),
            resource_uid: ResourceUid::new("resource-1"),
            spec_generation: 2,
            desired_replicas: 2,
            previous_configuration: prior_configuration,
            current_configuration: previous.clone(),
            previous_policy: prior_policy,
            current_policy: previous_policy.clone(),
            primary: fixture.primary.clone(),
            target: secondary.clone(),
            build_id: OperationId::new("settled-prior-build"),
            snapshot_boundary_lsn: 0,
            catch_up_boundary_lsn: 0,
        };
        prior_intent.operation_id = prior_intent.expected_operation_id();
        let prior_witness = |identity: ReplicaIdentity, role, sequence| ScaleUpWitness {
            resource_uid: prior_intent.resource_uid.clone(),
            identity: identity.clone(),
            role,
            process_session_id: crate::protocol::types::ProcessSessionId::new(format!(
                "prior-session-{sequence}"
            )),
            report_sequence: sequence,
            epoch: previous.epoch,
            previous_configuration_id: None,
            current_configuration_id: previous.configuration_id.clone(),
            verified_replication_lsn: 0,
            write_status: if role == ReplicaRole::Primary {
                AccessStatus::Granted
            } else {
                AccessStatus::NotPrimary
            },
            pending_operation_id: None,
            retained_operation_id: Some(prior_intent.command_operation_id(
                ScaleUpStage::CurrentOnly,
                &identity,
                &previous,
            )),
        };
        let current_only_write_quorum = vec![
            prior_witness(fixture.primary.clone(), ReplicaRole::Primary, 1),
            prior_witness(secondary.clone(), ReplicaRole::ActiveSecondary, 2),
        ];
        let prior_receipt = ScaleUpReceipt {
            intent: prior_intent,
            accepted_configuration: previous.clone(),
            failover_evidence: None,
            failover_safe_lsn: None,
            current_only_write_quorum,
        };
        let report = |identity: ReplicaIdentity, role, write_status| AgentReport {
            protocol_version: crate::protocol::PROTOCOL_VERSION,
            resource_uid: ResourceUid::new("resource-1"),
            identity,
            process_session_id: crate::protocol::types::ProcessSessionId::new("synthetic"),
            report_sequence: 1,
            role,
            read_status: AccessStatus::Granted,
            write_status,
            healthy: true,
            epoch: previous.epoch,
            previous_configuration: None,
            current_configuration: Some(previous.clone()),
            current_progress: 0,
            verified_replication_lsn: Some(0),
            committed_lsn: 0,
            current_configuration_quorum_progress: 0,
            catch_up_complete: true,
            ..Default::default()
        };
        let endpoint_name =
            derive_replica_endpoint_name(&ResourceUid::new("resource-1"), &fixture.target);
        let cleanup = ScaleUpCleanup {
            provisioning: fixture.provisioning.clone(),
            target: fixture.target.clone(),
            resources: ReplicaCleanupIdentity {
                pod: CleanupResourceIdentity::Present {
                    name: "pod-3".into(),
                    uid: fixture.provisioning.pod_uid.to_string(),
                },
                pvc: CleanupResourceIdentity::Present {
                    name: "data-3".into(),
                    uid: fixture.provisioning.pvc_uid.to_string(),
                },
                endpoint: CleanupResourceIdentity::Present {
                    name: endpoint_name.clone(),
                    uid: "endpoint-3".into(),
                },
            },
        };
        let candidate = AgentReport {
            protocol_version: crate::protocol::PROTOCOL_VERSION,
            resource_uid: ResourceUid::new("resource-1"),
            identity: fixture.target.clone(),
            process_session_id: crate::protocol::types::ProcessSessionId::new(
                "candidate-synthetic",
            ),
            report_sequence: 1,
            role: ReplicaRole::IdleSecondary,
            read_status: AccessStatus::NotPrimary,
            write_status: AccessStatus::NotPrimary,
            healthy: !permanent_candidate_failure,
            reported_fault: permanent_candidate_failure.then_some(FaultType::Permanent),
            ..Default::default()
        };
        let snapshot = ObservationSnapshot {
            resource_uid: ResourceUid::new("resource-1"),
            resource_version: "1".into(),
            desired: DesiredState {
                generation: 3,
                replicas: if permanent_candidate_failure { 3 } else { 2 },
                image: "example:v1".into(),
                failover_delay_seconds: 30,
                switchover: None,
            },
            status: AcceptedStatus {
                initialized: true,
                observed_generation: 2,
                effective_policy: Some(previous_policy),
                topology: Some(AcceptedTopology {
                    configuration: previous.clone(),
                }),
                provisioning: Some(fixture.provisioning.clone()),
                last_scale_up: include_prior_receipt.then(|| Box::new(prior_receipt)),
                ..Default::default()
            },
            replicas: BTreeMap::from([
                (
                    ReplicaObservationKey::new(
                        fixture.primary.replica_id,
                        fixture.primary.instance_id.clone(),
                    ),
                    ReplicaObservation {
                        kubernetes: Some(KubernetesReplicaObservation {
                            replica_id: fixture.primary.replica_id,
                            pod_name: "pod-1".into(),
                            pod_uid: Some(PodUid::new("pod-1")),
                            pvc_name: "data-1".into(),
                            pvc_uid: Some(PvcUid::new("pvc-1")),
                            image: Some("example:v1".into()),
                            pod_ready: true,
                            peer_endpoint_ready: true,
                        }),
                        agent: AgentObservation::Report(Box::new(report(
                            fixture.primary.clone(),
                            ReplicaRole::Primary,
                            AccessStatus::Granted,
                        ))),
                    },
                ),
                (
                    ReplicaObservationKey::new(secondary.replica_id, secondary.instance_id.clone()),
                    ReplicaObservation {
                        kubernetes: Some(KubernetesReplicaObservation {
                            replica_id: secondary.replica_id,
                            pod_name: secondary.instance_id.to_string(),
                            pod_uid: Some(PodUid::new(secondary.instance_id.as_str())),
                            pvc_name: "data-2".into(),
                            pvc_uid: Some(PvcUid::new("pvc-2")),
                            image: Some("example:v1".into()),
                            pod_ready: true,
                            peer_endpoint_ready: true,
                        }),
                        agent: AgentObservation::Report(Box::new(report(
                            secondary,
                            ReplicaRole::ActiveSecondary,
                            AccessStatus::NotPrimary,
                        ))),
                    },
                ),
                (
                    ReplicaObservationKey::new(
                        fixture.target.replica_id,
                        fixture.target.instance_id.clone(),
                    ),
                    ReplicaObservation {
                        kubernetes: Some(KubernetesReplicaObservation {
                            replica_id: fixture.target.replica_id,
                            pod_name: "pod-3".into(),
                            pod_uid: Some(fixture.provisioning.pod_uid.clone()),
                            pvc_name: "data-3".into(),
                            pvc_uid: Some(fixture.provisioning.pvc_uid.clone()),
                            image: Some("example:v1".into()),
                            pod_ready: true,
                            peer_endpoint_ready: true,
                        }),
                        agent: AgentObservation::Report(Box::new(candidate)),
                    },
                ),
            ]),
            secondary_scale_down_resources: vec![SecondaryScaleDownResourceObservation {
                resource_uid: ResourceUid::new("resource-1"),
                target: fixture.target.clone(),
                identity: cleanup.resources.clone(),
                pod: ExactResourceObservation::FrozenUidPresent {
                    resource_version: "1".into(),
                },
                pod_allocation_operation_id: Some(fixture.provisioning.operation_id.clone()),
                pod_matches_allocation_metadata: true,
                pvc: ExactResourceObservation::FrozenUidPresent {
                    resource_version: "1".into(),
                },
                pvc_allocation_operation_id: Some(fixture.provisioning.operation_id.clone()),
                endpoint: ExactResourceObservation::FrozenUidPresent {
                    resource_version: "1".into(),
                },
            }],
            previous_report_watermarks: BTreeMap::new(),
            durable_storage_evidence: true,
            supporting_resources_ready: true,
            routing: RoutingObservation {
                service_present: true,
                unresolved_write_target: false,
                write_target: Some(fixture.primary.clone()),
            },
            observation_failures: Vec::new(),
            now_unix_seconds: 100,
        };
        scale_up_model::Model::from_snapshot(snapshot, vec![2])
    }

    fn install_source_report(
        model: &mut scale_up_model::Model,
        source: &ReplicaIdentity,
        status: proto::AgentStatusReport,
    ) {
        let report = crate::control::normalize_agent_status_report(status).unwrap();
        let key = ReplicaObservationKey::new(source.replica_id, source.instance_id.clone());
        model.snapshot.replicas.get_mut(&key).unwrap().agent = report;
    }

    fn persist_cleanup(model: &mut scale_up_model::Model) {
        let changes = match evaluate(&model.snapshot, &scale_up_model::config()) {
            Plan::Apply { changes } => changes,
            other => panic!("evaluator must persist exact abandoned-candidate cleanup: {other:?}"),
        };
        assert!(changes.iter().any(|change| matches!(
            change,
            KubernetesChange::PersistStatus { status }
                if status.scale_up_cleanup.is_some() && status.provisioning.is_none()
        )));
        for change in changes {
            model.apply(change);
        }
    }

    fn exact_retirement(plan: Plan, fixture: &ScaleUpSourceFixture) -> EnsureReplicaBuild {
        let command = match plan {
            Plan::Execute {
                command: ProtocolCommand::EnsureReplicaBuild(command),
            } => command,
            other => panic!("evaluator must issue exact source build retirement: {other:?}"),
        };
        assert!(command.retire);
        assert_eq!(command.operation_id, fixture.build_id);
        assert_eq!(command.local_replica_id, fixture.primary.replica_id);
        assert_eq!(command.expected_instance_id, fixture.primary.instance_id);
        assert_eq!(
            command.expected_agent_generation,
            fixture.primary.agent_generation
        );
        assert_eq!(command.target, fixture.target);
        assert!(command.authority.is_none());
        assert!(command.source_session_id.is_none());
        *command
    }

    for permanent_candidate_failure in [false, true] {
        for cut in [
            RetirementCut::BeforeConsume,
            RetirementCut::AfterConsume,
            RetirementCut::AfterRetire,
        ] {
            let directory = tempdir().unwrap();
            let fixture = scale_up_source_fixture(directory.path(), false, false).await;
            let runtime = Arc::new(PodRuntime::new(
                fixture.primary.clone(),
                Arc::new(ReplayApplication),
                fixture.store.clone(),
            ));
            let executor = Arc::new(CutRetirementReply {
                runtime: runtime.clone(),
                cut,
                lose_once: AtomicBool::new(true),
            });
            let service =
                AgentService::new(fixture.store.clone(), runtime.clone(), executor, "token")
                    .unwrap();
            let control = free_address();
            let replication = free_address();
            let (ready, mut ready_rx) = watch::channel(false);
            let (shutdown, shutdown_rx) = watch::channel(false);
            let server = tokio::spawn(service.serve(control, replication, ready, shutdown_rx));
            ready_rx.wait_for(|ready| *ready).await.unwrap();

            let source_status = service_status(control, &fixture.primary).await;
            let build_request = proto::EnsureReplicaBuildCommand {
                operation_id: fixture.build_id.to_string(),
                local_replica_id: fixture.primary.replica_id.value(),
                expected_instance_id: fixture.primary.instance_id.to_string(),
                expected_agent_generation: fixture.primary.agent_generation.to_string(),
                target: Some(fixture.target.clone().into()),
                authority: None,
                source_session_id: String::new(),
                retire: false,
            };
            let mut build_client = proto::agent_control_client::AgentControlClient::connect(
                format!("http://{control}"),
            )
            .await
            .unwrap();
            let build_primary = fixture.primary.clone();
            let build_session = source_status.process_session_id.clone();
            let pending_build = build_request.clone();
            let build = tokio::spawn(async move {
                build_client
                    .execute(authorized_request(
                        "resource-1",
                        &build_primary,
                        &build_session,
                        proto::execute_command_request::Command::EnsureReplicaBuild(pending_build),
                    ))
                    .await
            });
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    if fixture
                        .store
                        .load_state()
                        .await
                        .unwrap()
                        .pending_effect
                        .as_ref()
                        .is_some_and(|pending| {
                            matches!(
                                &pending.effect.action,
                                RuntimeEffectAction::BuildReplica { build_id, .. }
                                    if build_id == &fixture.build_id
                            )
                        })
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("real copy remained incomplete with a durable source effect");

            let mut model = cleanup_model(&fixture, permanent_candidate_failure, true);
            install_source_report(
                &mut model,
                &fixture.primary,
                service_status(control, &fixture.primary).await,
            );
            persist_cleanup(&mut model);
            let retirement = exact_retirement(
                evaluate(&model.snapshot, &scale_up_model::config()),
                &fixture,
            );
            let retirement_proto = proto::EnsureReplicaBuildCommand {
                operation_id: retirement.operation_id.to_string(),
                local_replica_id: retirement.local_replica_id.value(),
                expected_instance_id: retirement.expected_instance_id.to_string(),
                expected_agent_generation: retirement.expected_agent_generation.to_string(),
                target: Some(retirement.target.clone().into()),
                authority: None,
                source_session_id: String::new(),
                retire: true,
            };
            let mut retire_client = proto::agent_control_client::AgentControlClient::connect(
                format!("http://{control}"),
            )
            .await
            .unwrap();
            assert!(
                retire_client
                    .execute(authorized_request(
                        "resource-1",
                        &fixture.primary,
                        &source_status.process_session_id,
                        proto::execute_command_request::Command::EnsureReplicaBuild(
                            retirement_proto,
                        ),
                    ))
                    .await
                    .is_err(),
                "{cut:?} must interrupt retirement settlement"
            );
            assert!(
                tokio::time::timeout(std::time::Duration::from_secs(5), build)
                    .await
                    .expect("cancelled real copy request completed")
                    .unwrap()
                    .is_err()
            );
            let interrupted = fixture.store.load_state().await.unwrap();
            assert!(interrupted.abandoned_builds.contains(&fixture.build_id));
            assert!(!interrupted.retired_builds.contains(&fixture.build_id));

            let hidden = service_status(control, &fixture.primary).await;
            assert!(
                hidden
                    .builds
                    .iter()
                    .all(|build| build.build_id != fixture.build_id.as_str()),
                "durable abandonment must hide the stale build report"
            );
            install_source_report(&mut model, &fixture.primary, hidden);
            assert_eq!(
                exact_retirement(
                    evaluate(&model.snapshot, &scale_up_model::config()),
                    &fixture,
                ),
                retirement,
                "hidden abandonment evidence must replay the exact retirement command"
            );

            shutdown.send_replace(true);
            server.await.unwrap().unwrap();

            let (restarted_control, restarted_shutdown, restarted_server) = loop {
                let restarted_runtime = Arc::new(PodRuntime::new(
                    fixture.primary.clone(),
                    Arc::new(ReplayApplication),
                    fixture.store.clone(),
                ));
                let restarted_service = AgentService::new(
                    fixture.store.clone(),
                    restarted_runtime.clone(),
                    restarted_runtime,
                    "token",
                )
                .unwrap();
                let restarted_control = free_address();
                let (restarted_ready, mut restarted_ready_rx) = watch::channel(false);
                let (restarted_shutdown, restarted_shutdown_rx) = watch::channel(false);
                let mut restarted_server = tokio::spawn(restarted_service.serve(
                    restarted_control,
                    free_address(),
                    restarted_ready,
                    restarted_shutdown_rx,
                ));
                let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    tokio::select! {
                        ready = restarted_ready_rx.wait_for(|ready| *ready) => {
                            Ok(ready)
                        }
                        stopped = &mut restarted_server => {
                            Err(stopped)
                        }
                    }
                })
                .await
                .expect("restarted source owner settled abandoned build");
                match outcome {
                    Ok(Ok(_)) => {
                        break (restarted_control, restarted_shutdown, restarted_server);
                    }
                    Ok(Err(error)) => panic!("restarted readiness closed: {error}"),
                    Err(Ok(Err(error))) if error.to_string().contains("Address already in use") => {
                        continue;
                    }
                    Err(result) => panic!("restarted source owner failed: {result:?}"),
                }
            };
            let settled = fixture.store.load_state().await.unwrap();
            assert!(settled.pending_effect.is_none());
            assert!(settled.retired_builds.contains(&fixture.build_id));

            install_source_report(
                &mut model,
                &fixture.primary,
                service_status(restarted_control, &fixture.primary).await,
            );
            let old_attempt = model
                .snapshot
                .status
                .scale_up_cleanup
                .as_ref()
                .unwrap()
                .provisioning
                .operation_id
                .clone();
            let mut deleted = Vec::new();
            for _ in 0..12 {
                match evaluate(&model.snapshot, &scale_up_model::config()) {
                    Plan::Apply { changes } => {
                        for change in changes {
                            if let KubernetesChange::DeleteScaleDownResource { resource, .. } =
                                &change
                            {
                                deleted.push(*resource);
                            }
                            model.apply(change);
                        }
                    }
                    Plan::Wait { status, .. } => model.apply_wait(status),
                    Plan::Execute {
                        command: ProtocolCommand::EnsureReplicaBuild(command),
                    } => panic!("settled retirement was reissued: {command:?}"),
                    other => panic!("cleanup convergence after restart: {other:?}"),
                }
                if model.snapshot.status.scale_up_cleanup.is_none() {
                    break;
                }
            }
            assert_eq!(
                deleted,
                vec![
                    ScaleDownResource::Endpoint,
                    ScaleDownResource::Pod,
                    ScaleDownResource::Pvc,
                ]
            );
            assert!(model.snapshot.status.scale_up_cleanup.is_none());

            if !permanent_candidate_failure {
                model.snapshot.desired.replicas = 3;
                model.snapshot.desired.generation += 1;
            }
            for _ in 0..20 {
                let fresh = model
                    .snapshot
                    .status
                    .provisioning
                    .as_ref()
                    .map(|provisioning| &provisioning.operation_id)
                    .or_else(|| {
                        model
                            .snapshot
                            .status
                            .scale_up_allocation
                            .as_ref()
                            .map(|allocation| &allocation.operation_id)
                    });
                if fresh.is_some_and(|operation_id| operation_id != &old_attempt) {
                    break;
                }
                model.step();
            }
            let fresh = model
                .snapshot
                .status
                .provisioning
                .as_ref()
                .map(|provisioning| &provisioning.operation_id)
                .or_else(|| {
                    model
                        .snapshot
                        .status
                        .scale_up_allocation
                        .as_ref()
                        .map(|allocation| &allocation.operation_id)
                })
                .expect("cleanup must authorize a fresh retry");
            assert_ne!(fresh, &old_attempt);

            restarted_shutdown.send_replace(true);
            restarted_server.await.unwrap().unwrap();
        }
    }

    let directory = tempdir().unwrap();
    let fixture = scale_up_source_fixture(directory.path(), false, false).await;
    let runtime = Arc::new(PodRuntime::new(
        fixture.primary.clone(),
        Arc::new(ReplayApplication),
        fixture.store.clone(),
    ));
    let service =
        AgentService::new(fixture.store.clone(), runtime.clone(), runtime, "token").unwrap();
    let control = free_address();
    let (ready, mut ready_rx) = watch::channel(false);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let server = tokio::spawn(service.serve(control, free_address(), ready, shutdown_rx));
    ready_rx.wait_for(|ready| *ready).await.unwrap();
    let unrelated = RuntimeEffect {
        operation_id: OperationId::new("unrelated-pending"),
        sequence: fixture
            .store
            .load_state()
            .await
            .unwrap()
            .next_effect_sequence,
        action: RuntimeEffectAction::RefreshApplicationProgress,
    };
    fixture.store.begin_effect(&unrelated).await.unwrap();
    let mut model = cleanup_model(&fixture, false, false);
    install_source_report(
        &mut model,
        &fixture.primary,
        service_status(control, &fixture.primary).await,
    );
    persist_cleanup(&mut model);
    assert!(matches!(
        evaluate(&model.snapshot, &scale_up_model::config()),
        Plan::Wait { status, .. }
            if status.conditions.iter().any(|condition|
                condition.reason == "ScaleUpSourceBuildRetirementBlocked")
    ));
    let fenced = fixture.store.load_state().await.unwrap();
    assert_eq!(
        fenced
            .pending_effect
            .as_ref()
            .map(|pending| &pending.effect),
        Some(&unrelated)
    );
    assert!(!fenced.abandoned_builds.contains(&fixture.build_id));
    shutdown.send_replace(true);
    server.await.unwrap().unwrap();
}
