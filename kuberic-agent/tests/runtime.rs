use std::collections::{BTreeMap, VecDeque};
use std::env;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream};
use kuberic_agent::coordinator::Coordinator;
use kuberic_agent::hosting::{OutboundReplication, PodRuntime, PreparedCopy, RuntimeControlPlane};
use kuberic_agent::runtime_adapter::RuntimeAdapter;
use kuberic_agent::service::AgentService;
use kuberic_agent::sqlite_store::SqliteStore;
use kuberic_agent::state::{AgentState, CoordinatorStage, SCHEMA_VERSION, StorageIdentity};
use kuberic_agent::store::AgentStore;
use kuberic_protocol::command::EnsureConfiguration;
use kuberic_protocol::evaluator::{EvaluationConfig, evaluate};
use kuberic_protocol::observation::{
    AgentBuildReport, AgentObservation, AgentReport, DesiredState, KubernetesReplicaObservation,
    ObservationSnapshot, ReplicaObservation, ReplicaObservationKey, RoutingObservation,
};
use kuberic_protocol::plan::Plan;
use kuberic_protocol::types::{
    AcceptedStatus, AcceptedTopology, AccessStatus, AgentGeneration, ConfigurationDescriptor,
    ConfigurationMember, EffectivePolicy, Epoch, FaultType, InitializationId, LoadMetric,
    OperationId, PodUid, ProcessSessionId, ProvisioningIntent, ProvisioningPurpose, PvcUid,
    ReplicaId, ReplicaIdentity, ReplicaInstanceId, ReplicaRole, ResourceUid,
    ScaleUpConfigurationEvidence, ScaleUpIntent, ScaleUpProvisioning, SwitchoverHandoff,
    SwitchoverRequestId, TransitionKind,
};
use kuberic_runtime::application::{
    ClientWrite, CopyChunk, DurableApplicationAck, DurableApplicationProgress, OpenContext,
    OpenMode, Operation, OperationDataStream, RoleChange, StateProvider, StatefulServiceReplica,
};
use kuberic_runtime::engine::{DurableState, RetainedOperationStream};
use kuberic_runtime::replicator::copy::{BuildConfiguration, PrepareCopyRequest};
use kuberic_runtime::replicator::stream::{OperationMetadata, OperationStream};
use kuberic_runtime::replicator::{
    DefaultReplicatorFactory, PrimaryReplicator, ReplicaInformation, ReplicaSetQuorumMode,
    Replicator, ReplicatorFactory, ReplicatorFactoryContext, ReplicatorInterfaces,
    ReplicatorSettings, StateReplicator, StatefulServicePartition,
};
use kuberic_runtime::{Result, RuntimeError};
use kuberic_runtime_internal::authority::{
    AdmittedAuthority, AuthorityFence, BuildAuthority, BuildAuthorityKind, BuildAuthorityStore,
    BuildProgressStore, DurableBuildProgress, DurableLocalWrite, LocalWriteJournal,
    ReplicaAuthorityStore, ReplicationProgress, ReplicationProgressStore,
};
use kuberic_runtime_internal::effects::{RuntimeEffect, RuntimeEffectAction, RuntimeEffectResult};
use kuberic_runtime_internal::{ContractError, Result as ContractResult};
use kuberic_wire::proto;
use tokio::sync::Notify;
use tokio::time::{Duration, timeout};

#[allow(dead_code)]
#[path = "../../kuberic-protocol/tests/support/secondary_scale_down.rs"]
mod removal_fixture;

#[path = "support/removal_oracle.rs"]
mod removal_oracle;

#[cfg(feature = "testing")]
mod in_process_transport_tests {
    use super::*;
    use futures::FutureExt;
    use kuberic_agent::testing::{
        ControlOutput, InProcessTransport, Message, TransportError, TransportEvent,
    };

    async fn receiver(
        local: ReplicaIdentity,
        members: Vec<ReplicaIdentity>,
        manual: bool,
    ) -> (Arc<TestApplication>, Arc<PodRuntime>) {
        let application = Arc::new(TestApplication::default());
        application.manual_streams.store(manual, Ordering::SeqCst);
        let runtime = Arc::new(PodRuntime::new(
            local.clone(),
            application.clone(),
            Arc::new(MemoryAuthorityStore::default()),
        ));
        for (index, action) in [
            RuntimeEffectAction::Open(OpenMode::New),
            RuntimeEffectAction::AdmitAuthority(Box::new(authority(local, members))),
            RuntimeEffectAction::ChangeRole(ReplicaRole::ActiveSecondary),
        ]
        .into_iter()
        .enumerate()
        {
            runtime
                .apply_effect(effect(index as u64 + 1, action))
                .await
                .unwrap();
        }
        (application, runtime)
    }

    pub(super) async fn event(
        transport: &mut InProcessTransport,
        predicate: impl Fn(&TransportEvent) -> bool,
    ) -> TransportEvent {
        timeout(Duration::from_secs(2), async {
            loop {
                for event in transport.next().await.events {
                    if predicate(&event) {
                        return event;
                    }
                    if let TransportEvent::Rejected { error, .. } = event {
                        panic!("unexpected transport rejection: {error}");
                    }
                }
            }
        })
        .await
        .expect("expected transport event")
    }

    async fn persist(
        application: &TestApplication,
        operation: &kuberic_runtime::replicator::stream::StreamOperation,
    ) -> DurableApplicationProgress {
        let OperationMetadata::Replication { lsn, committed_lsn } = operation.metadata else {
            panic!("expected replication")
        };
        application
            .apply(Operation {
                lsn,
                committed_lsn,
                data: operation.data.clone(),
            })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn received_is_not_applied_and_idle_excludes_pending_acknowledgements() {
        let primary = identity(1, "primary");
        let secondary = identity(2, "secondary");
        let members = vec![primary.clone(), secondary.clone()];
        let source_app = Arc::new(TestApplication::default());
        let source = open_primary(source_app.clone(), members.clone()).await;
        let (target_app, target) = receiver(secondary, members, true).await;
        let mut transport = InProcessTransport::new();
        transport
            .register(source.clone(), ProcessSessionId::new("source-session"))
            .await
            .unwrap();
        transport
            .register(target.clone(), ProcessSessionId::new("target-session"))
            .await
            .unwrap();
        assert!(transport.pump().idle);
        let state = source_app.state_replicator.lock().unwrap().clone().unwrap();
        let write = tokio::spawn(async move { state.replicate(Bytes::from_static(b"one")).await });
        let received = event(&mut transport, |e| {
            matches!(e, TransportEvent::Received { .. })
        })
        .await;
        let TransportEvent::Received {
            acknowledgement, ..
        } = received
        else {
            unreachable!()
        };
        assert_eq!(acknowledgement.received_lsn, 1);
        assert_eq!(acknowledgement.applied_lsn, 0);
        assert_eq!(acknowledgement.receiver_session_id, "target-session");
        let report = transport.pump();
        assert!(!report.idle);
        assert_eq!(report.in_flight, 1);
        let mut stream = target_app.held_streams.lock().unwrap().remove(0);
        let operation = stream.get_operation().await.unwrap().unwrap();
        let progress = persist(&target_app, &operation).await;
        assert!(!write.is_finished());
        assert!(
            !transport.pump().idle,
            "persistence without a service ACK is insufficient"
        );
        assert!(
            transport.next().now_or_never().is_none(),
            "cancelling a wait must retain the pending delivery"
        );
        operation.acknowledge(progress).unwrap();
        let applied = event(&mut transport, |e| {
            matches!(e, TransportEvent::Applied { .. })
        })
        .await;
        let TransportEvent::Applied {
            acknowledgement, ..
        } = applied
        else {
            unreachable!()
        };
        assert_eq!(acknowledgement.applied_lsn, 1);
        assert_eq!(write.await.unwrap().unwrap(), 1);
        assert!(transport.pump().idle);
        assert_eq!(target.snapshot().await.verified_replication_lsn, Some(1));
    }

    #[tokio::test]
    async fn rejected_and_dropped_operations_never_earn_applied_credit() {
        for reject in [false, true] {
            let members = vec![identity(1, "primary"), identity(2, "secondary")];
            let source_app = Arc::new(TestApplication::default());
            let source = open_primary(source_app.clone(), members.clone()).await;
            let (target_app, target) = receiver(members[1].clone(), members, true).await;
            let mut transport = InProcessTransport::new();
            transport
                .register(source.clone(), ProcessSessionId::new("source"))
                .await
                .unwrap();
            transport
                .register(target, ProcessSessionId::new("target"))
                .await
                .unwrap();
            let state = source_app.state_replicator.lock().unwrap().clone().unwrap();
            let write =
                tokio::spawn(async move { state.replicate(Bytes::from_static(b"one")).await });
            event(&mut transport, |e| {
                matches!(e, TransportEvent::Received { .. })
            })
            .await;
            let mut stream = target_app.held_streams.lock().unwrap().remove(0);
            let operation = stream.get_operation().await.unwrap().unwrap();
            if reject {
                operation
                    .reject(RuntimeError::Application("explicit rejection".into()))
                    .unwrap();
            } else {
                drop(operation);
            }
            event(&mut transport, |e| {
                matches!(e, TransportEvent::Rejected { .. })
            })
            .await;
            assert_eq!(source_app.progress.lock().unwrap().committed_lsn, 0);
            assert!(!write.is_finished());
            assert!(
                transport.pump().idle,
                "transport idle does not imply client quorum"
            );
            source.abort();
            assert!(write.await.unwrap().is_err());
        }
    }

    #[tokio::test]
    async fn routing_uses_full_identity_and_rejects_stale_wire_sessions() {
        let primary = identity(1, "primary");
        let secondary = identity(2, "current");
        let source = open_primary(
            Arc::new(TestApplication::default()),
            vec![primary.clone(), secondary.clone()],
        )
        .await;
        let (current_app, current) =
            receiver(secondary.clone(), vec![primary, secondary], false).await;
        let old_app = Arc::new(TestApplication::default());
        let old = Arc::new(PodRuntime::new(
            identity(2, "old-incarnation"),
            old_app.clone(),
            Arc::new(MemoryAuthorityStore::default()),
        ));
        old.apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::New)))
            .await
            .unwrap();
        let mut transport = InProcessTransport::new();
        transport
            .register(source.clone(), ProcessSessionId::new("source"))
            .await
            .unwrap();
        transport
            .register(current, ProcessSessionId::new("current"))
            .await
            .unwrap();
        transport
            .register(old, ProcessSessionId::new("old"))
            .await
            .unwrap();
        let pending = source
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("exact-route"),
                data: Bytes::from_static(b"one"),
            })
            .await
            .unwrap();
        let raw = pending.replication_items[0].clone();
        assert!(matches!(
            transport.enqueue(Message::Replication(raw.clone())),
            Err(TransportError::StaleSession { .. })
        ));
        let mut absent = raw.clone();
        absent.receiver = Some(identity(2, "unregistered-incarnation").into());
        assert!(matches!(
            transport.bind(Message::Replication(absent)),
            Err(TransportError::Unregistered(_))
        ));
        let mut wrong_generation = raw.clone();
        wrong_generation.receiver.as_mut().unwrap().agent_generation = "obsolete-generation".into();
        assert!(matches!(
            transport.bind(Message::Replication(wrong_generation)),
            Err(TransportError::Unregistered(_))
        ));
        let bound = transport.bind(Message::Replication(raw)).unwrap();
        for sender in [false, true] {
            let Message::Replication(mut stale) = bound.clone() else {
                unreachable!()
            };
            if sender {
                stale.sender_session_id = "obsolete-source".into();
            } else {
                stale.receiver_session_id = "obsolete-target".into();
            }
            assert!(matches!(
                transport.bind(Message::Replication(stale.clone())),
                Err(TransportError::StaleSession { .. })
            ));
            assert!(matches!(
                transport.enqueue(Message::Replication(stale)),
                Err(TransportError::StaleSession { .. })
            ));
        }
        transport.enqueue(bound).unwrap();
        event(&mut transport, |e| {
            matches!(e, TransportEvent::Applied { .. })
        })
        .await;
        assert_eq!(pending.committed().await.unwrap().lsn, 1);
        assert_eq!(current_app.progress.lock().unwrap().applied_lsn, 1);
        assert_eq!(old_app.progress.lock().unwrap().applied_lsn, 0);
    }

    #[tokio::test]
    async fn replacing_a_session_cancels_old_delivery_and_rejects_delayed_replay() {
        let members = vec![identity(1, "primary"), identity(2, "secondary")];
        let source = open_primary(Arc::new(TestApplication::default()), members.clone()).await;
        let (old_app, old_target) = receiver(members[1].clone(), members.clone(), true).await;
        let mut transport = InProcessTransport::new();
        transport
            .register(source.clone(), ProcessSessionId::new("source"))
            .await
            .unwrap();
        transport
            .register(old_target.clone(), ProcessSessionId::new("old-session"))
            .await
            .unwrap();
        let pending = source
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("restart-route"),
                data: Bytes::from_static(b"one"),
            })
            .await
            .unwrap();
        let raw = Message::Replication(pending.replication_items[0].clone());
        let delayed = transport.bind(raw.clone()).unwrap();
        let ticket = transport.enqueue(delayed.clone()).unwrap();
        event(&mut transport, |e| {
            matches!(e, TransportEvent::Received { .. })
        })
        .await;
        let mut stream = old_app.held_streams.lock().unwrap().remove(0);
        let operation = stream.get_operation().await.unwrap().unwrap();
        let progress = persist(&old_app, &operation).await;
        let (new_app, new_target) = receiver(members[1].clone(), members, false).await;
        transport
            .register(new_target.clone(), ProcessSessionId::new("new-session"))
            .await
            .unwrap();
        assert!(
            operation.acknowledge(progress).is_err(),
            "old waiter was cancelled"
        );
        let rejected = transport.pump();
        assert!(rejected.events.iter().any(|event| matches!(event,
            TransportEvent::Rejected { delivery: Some(id), error: TransportError::StaleSession { .. }, .. } if *id == ticket)));
        assert_eq!(source.snapshot().await.committed_lsn, 0);
        assert!(matches!(
            transport.enqueue(delayed),
            Err(TransportError::StaleSession { .. })
        ));
        assert!(matches!(
            transport
                .register(old_target, ProcessSessionId::new("old-session"))
                .await,
            Err(TransportError::StaleSession { .. })
        ));
        transport.enqueue(transport.bind(raw).unwrap()).unwrap();
        let applied = event(&mut transport, |e| {
            matches!(e, TransportEvent::Applied { .. })
        })
        .await;
        let TransportEvent::Applied {
            acknowledgement, ..
        } = applied
        else {
            unreachable!()
        };
        assert_eq!(acknowledgement.receiver_session_id, "new-session");
        assert_eq!(pending.committed().await.unwrap().lsn, 1);
        assert_eq!(new_app.progress.lock().unwrap().applied_lsn, 1);
        let aliased = receiver(
            identity(2, "secondary"),
            vec![identity(1, "primary"), identity(2, "secondary")],
            false,
        )
        .await
        .1;
        assert!(matches!(
            transport
                .register(aliased, ProcessSessionId::new("new-session"))
                .await,
            Err(TransportError::Registration(_))
        ));
        assert!(transport.pump().idle);

        let unfinished = source
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("before-source-restart"),
                data: Bytes::from_static(b"two"),
            })
            .await
            .unwrap();
        let delayed = transport
            .bind(Message::Replication(
                unfinished.replication_items[0].clone(),
            ))
            .unwrap();
        let ticket = transport.enqueue(delayed.clone()).unwrap();
        let recovered_app = Arc::new(TestApplication::default());
        recovered_app
            .apply(Operation {
                lsn: 1,
                committed_lsn: 0,
                data: Bytes::from_static(b"one"),
            })
            .await
            .unwrap();
        recovered_app.commit(1).await.unwrap();
        let recovered_source = open_primary(
            recovered_app,
            vec![identity(1, "primary"), identity(2, "secondary")],
        )
        .await;
        transport
            .register(
                recovered_source.clone(),
                ProcessSessionId::new("source-restarted"),
            )
            .await
            .unwrap();
        assert!(transport.pump().events.iter().any(|event| matches!(event,
            TransportEvent::Rejected { delivery: Some(id), error: TransportError::StaleSession { .. }, .. } if *id == ticket)));
        assert!(matches!(
            transport.enqueue(delayed),
            Err(TransportError::StaleSession { .. })
        ));
        assert_eq!(recovered_source.snapshot().await.current_progress, 1);
        assert_eq!(new_app.progress.lock().unwrap().applied_lsn, 1);
        source.abort();
        assert!(unfinished.committed().await.is_err());
    }

    #[tokio::test]
    async fn copy_acknowledgements_follow_durable_service_acceptance_and_return_to_source() {
        let source_id = identity(1, "source");
        let target_id = identity(1, "replacement");
        let source = open_primary(Arc::new(TestApplication::default()), vec![source_id]).await;
        source
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("snapshot-row"),
                data: Bytes::from_static(b"snapshot"),
            })
            .await
            .unwrap()
            .committed()
            .await
            .unwrap();
        let mut prepared = prepare_copy_authorized(
            &source,
            PrepareCopyRequest {
                build_id: OperationId::new("transport-copy"),
                target: target_id.clone(),
                configuration: BuildConfiguration::Current,
                copy_context: empty_copy_context(),
            },
        )
        .await
        .unwrap();
        let items = copy_through_final(&mut prepared).await;
        let target_app = Arc::new(TestApplication::default());
        target_app.manual_streams.store(true, Ordering::SeqCst);
        let target = Arc::new(PodRuntime::new(
            target_id,
            target_app.clone(),
            Arc::new(MemoryAuthorityStore::default()),
        ));
        for (index, action) in [
            RuntimeEffectAction::Open(OpenMode::New),
            RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary),
            RuntimeEffectAction::AdmitBuildAuthority(Box::new(prepared.authority.clone())),
        ]
        .into_iter()
        .enumerate()
        {
            target
                .apply_effect(effect(index as u64 + 1, action))
                .await
                .unwrap();
        }
        let mut transport = InProcessTransport::new();
        transport
            .register(source.clone(), ProcessSessionId::new("copy-source"))
            .await
            .unwrap();
        transport
            .register(target, ProcessSessionId::new("copy-target"))
            .await
            .unwrap();
        let mut stream = target_app.held_streams.lock().unwrap().remove(1);
        for item in &items {
            let id = transport
                .enqueue(transport.bind(Message::Copy(item.clone())).unwrap())
                .unwrap();
            let report = transport.pump();
            assert_eq!(report.in_flight, 1);
            assert!(!report.idle);
            assert!(
                !report
                    .events
                    .iter()
                    .any(|e| matches!(e, TransportEvent::Copied { .. }))
            );
            let operation = stream.get_operation().await.unwrap().unwrap();
            let progress = match &operation.metadata {
                OperationMetadata::Copy { build_id, sequence } => {
                    target_app
                        .apply_copy_chunk(
                            build_id,
                            *sequence,
                            CopyChunk {
                                data: operation.data.clone(),
                            },
                        )
                        .await
                        .unwrap();
                    target_app.durable_progress().await.unwrap()
                }
                OperationMetadata::CopyComplete {
                    build_id,
                    up_to_lsn,
                    committed_lsn,
                } => target_app
                    .finish_copy(build_id, *up_to_lsn, *committed_lsn)
                    .await
                    .unwrap(),
                _ => panic!("expected copy stream"),
            };
            operation.acknowledge(progress).unwrap();
            let copied = event(
                &mut transport,
                |e| matches!(e, TransportEvent::Copied { delivery, .. } if *delivery == id),
            )
            .await;
            let TransportEvent::Copied {
                acknowledgement, ..
            } = copied
            else {
                unreachable!()
            };
            assert_eq!(acknowledgement.sequence, item.sequence);
            assert_eq!(acknowledgement.sender_session_id, "copy-source");
            assert_eq!(acknowledgement.receiver_session_id, "copy-target");
        }
        assert!(source.snapshot().await.builds[0].completed);
        assert_eq!(
            *target_app.progress.lock().unwrap(),
            DurableApplicationProgress {
                applied_lsn: 1,
                committed_lsn: 1
            }
        );
        assert!(transport.pump().idle);
    }

    #[tokio::test]
    async fn build_remove_and_evict_are_surfaced_without_automatic_authority_changes() {
        let source = open_primary(
            Arc::new(TestApplication::default()),
            vec![identity(1, "source")],
        )
        .await;
        let before = source.snapshot().await.authority;
        let primary = source.primary_replicator().await.unwrap();
        let target = identity(2, "replacement");
        let info = ReplicaInformation {
            build_id: OperationId::new("control-build"),
            identity: target.clone(),
            replication_address: "in-process://replacement".into(),
        };
        let mut transport = InProcessTransport::new();
        transport
            .register(source.clone(), ProcessSessionId::new("source"))
            .await
            .unwrap();
        let building = tokio::spawn({
            let primary = primary.clone();
            async move { primary.build_replica(info).await }
        });
        let output = event(&mut transport, |e| {
            matches!(
                e,
                TransportEvent::Control {
                    output: ControlOutput::Build(_),
                    ..
                }
            )
        })
        .await;
        assert!(
            matches!(output, TransportEvent::Control { output: ControlOutput::Build(endpoint), .. } if endpoint.identity == target)
        );
        assert!(!building.is_finished());
        assert_eq!(source.snapshot().await.authority, before);
        primary.remove_replica(target.replica_id).await.unwrap();
        event(&mut transport, |e| matches!(e, TransportEvent::Control { output: ControlOutput::Remove(id), .. } if *id == target.replica_id)).await;
        assert!(building.await.unwrap().is_err());
        assert!(transport.pump().idle);

        let intent = removal_fixture::intent(&[1, 2], 1);
        let reduced = open_removal_member(
            &intent,
            intent.primary.clone(),
            Arc::new(TestApplication::default()),
            Arc::new(MemoryAuthorityStore::default()),
        )
        .await;
        let prepared = reduced
            .apply_effect(effect(5, prepare_removal(&intent)))
            .await
            .unwrap();
        let evidence = removal_evidence(prepared.postcondition.prepared_secondary_removal.unwrap());
        let mut admitted = AdmittedAuthority {
            local_identity: intent.primary.clone(),
            transition_kind: Some(TransitionKind::SecondaryScaleDown),
            previous_configuration: Some(intent.previous_configuration.clone()),
            current_configuration: intent.current_configuration.clone(),
            switchover_handoff: None,
            scale_up: None,
            secondary_removal: Some(evidence),
        };
        reduced
            .apply_effect(effect(
                6,
                RuntimeEffectAction::AdmitAuthority(Box::new(admitted.clone())),
            ))
            .await
            .unwrap();
        admitted.previous_configuration = None;
        admitted.transition_kind = None;
        reduced
            .apply_effect(effect(
                7,
                RuntimeEffectAction::AdmitAuthority(Box::new(admitted.clone())),
            ))
            .await
            .unwrap();
        let mut transport = InProcessTransport::new();
        transport
            .register(reduced.clone(), ProcessSessionId::new("reduced"))
            .await
            .unwrap();
        event(&mut transport, |e| matches!(e, TransportEvent::Control { output: ControlOutput::Evict(id), .. } if *id == intent.target)).await;
        assert_eq!(reduced.snapshot().await.authority, Some(admitted));
        assert!(transport.pump().idle);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_snapshots_do_not_reacquire_read_locks_behind_queued_peer_eviction() {
    let intent = removal_fixture::intent(&[1, 2], 1);
    let runtime = open_removal_member(
        &intent,
        intent.primary.clone(),
        Arc::new(TestApplication::default()),
        Arc::new(MemoryAuthorityStore::default()),
    )
    .await;
    runtime
        .apply_effect(effect(5, prepare_removal(&intent)))
        .await
        .unwrap();
    let snapshots = {
        let runtime = runtime.clone();
        tokio::spawn(async move {
            for _ in 0..2000 {
                assert_eq!(runtime.snapshot().await.role, ReplicaRole::Primary);
                tokio::task::yield_now().await;
            }
        })
    };
    let plane = runtime.data_plane();
    let eviction_polling = tokio::spawn(async move { plane.next_outbound().await });
    let notifications = tokio::spawn(async move {
        for _ in 0..2000 {
            runtime.cancel_configuration_work().await.unwrap();
            tokio::task::yield_now().await;
        }
    });
    timeout(Duration::from_secs(3), async {
        snapshots.await.unwrap();
        notifications.await.unwrap();
    })
    .await
    .expect("snapshot must not deadlock with the pending-eviction writer");
    eviction_polling.abort();
}

async fn open_removal_member<S: kuberic_runtime_internal::authority::AuthorityStore + 'static>(
    intent: &kuberic_protocol::types::SecondaryScaleDownIntent,
    local: ReplicaIdentity,
    application: Arc<TestApplication>,
    store: Arc<S>,
) -> Arc<PodRuntime> {
    let primary = local == intent.primary;
    let runtime = Arc::new(PodRuntime::new(local.clone(), application, store));
    for (index, action) in [
        RuntimeEffectAction::Open(OpenMode::Existing),
        RuntimeEffectAction::AdmitAuthority(Box::new(AdmittedAuthority {
            local_identity: local,
            transition_kind: None,
            previous_configuration: None,
            current_configuration: intent.previous_configuration.clone(),
            switchover_handoff: None,
            scale_up: None,
            secondary_removal: None,
        })),
        RuntimeEffectAction::ChangeRole(if primary {
            ReplicaRole::Primary
        } else {
            ReplicaRole::ActiveSecondary
        }),
        RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::Granted,
            write: if primary {
                AccessStatus::Granted
            } else {
                AccessStatus::NotPrimary
            },
        },
    ]
    .into_iter()
    .enumerate()
    {
        runtime
            .apply_effect(effect(index as u64 + 1, action))
            .await
            .unwrap();
    }
    runtime
}

struct ScaleUpAgentBuildFixture {
    resource_uid: ResourceUid,
    source: ReplicaIdentity,
    target: ReplicaIdentity,
    previous_policy: EffectivePolicy,
    current_policy: EffectivePolicy,
    previous: ConfigurationDescriptor,
    provisioning: ProvisioningIntent,
    authority: BuildAuthority,
}

fn scale_up_agent_build_fixture() -> ScaleUpAgentBuildFixture {
    let source = ReplicaIdentity {
        replica_id: ReplicaId::new(1),
        instance_id: ReplicaInstanceId::new("source-pod"),
        agent_generation: AgentGeneration::new("source-generation"),
    };
    let previous_policy = EffectivePolicy::fixed(1, 30).unwrap();
    let current_policy = EffectivePolicy::fixed(2, 30).unwrap();
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        source.replica_id,
        vec![ConfigurationMember {
            identity: source.clone(),
            role: ReplicaRole::Primary,
        }],
        previous_policy.write_quorum,
    );
    let resource_uid = ResourceUid::new("scale-up-set");
    let mut provisioning = ProvisioningIntent {
        purpose: ProvisioningPurpose::scale_up(ScaleUpProvisioning {
            resource_uid: resource_uid.clone(),
            spec_generation: 2,
            desired_replicas: 2,
            previous_configuration: previous.clone(),
            previous_policy: previous_policy.clone(),
            current_policy: current_policy.clone(),
            target_replica_id: ReplicaId::new(2),
        }),
        pod_uid: PodUid::new("candidate-pod"),
        pvc_uid: PvcUid::new("candidate-pvc"),
        operation_id: OperationId::default(),
    };
    provisioning.operation_id = provisioning.expected_operation_id();
    let target = provisioning.target_identity(&resource_uid);
    let authority = BuildAuthority {
        build_id: provisioning.scale_up_build_id(&resource_uid).unwrap(),
        kind: BuildAuthorityKind::Provisioning,
        source: source.clone(),
        target: target.clone(),
        current_configuration: previous.clone(),
        replication_boundary_lsn: 4,
    };
    ScaleUpAgentBuildFixture {
        resource_uid,
        source,
        target,
        previous_policy,
        current_policy,
        previous,
        provisioning,
        authority,
    }
}

#[tokio::test]
async fn scale_up_receiver_replays_immutable_build_progress_without_reusing_source_session() {
    let source = ReplicaIdentity {
        replica_id: ReplicaId::new(1),
        instance_id: ReplicaInstanceId::new("source-pod"),
        agent_generation: AgentGeneration::new("source-generation"),
    };
    let previous_policy = EffectivePolicy::fixed(1, 30).unwrap();
    let current_policy = EffectivePolicy::fixed(2, 30).unwrap();
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        source.replica_id,
        vec![ConfigurationMember {
            identity: source.clone(),
            role: ReplicaRole::Primary,
        }],
        previous_policy.write_quorum,
    );
    let resource_uid = ResourceUid::new("scale-up-set");
    let mut provisioning = ProvisioningIntent {
        purpose: ProvisioningPurpose::scale_up(ScaleUpProvisioning {
            resource_uid: resource_uid.clone(),
            spec_generation: 2,
            desired_replicas: 2,
            previous_configuration: previous.clone(),
            previous_policy,
            current_policy: current_policy.clone(),
            target_replica_id: ReplicaId::new(2),
        }),
        pod_uid: PodUid::new("candidate-pod"),
        pvc_uid: PvcUid::new("candidate-pvc"),
        operation_id: OperationId::default(),
    };
    provisioning.operation_id = provisioning.expected_operation_id();
    let target = provisioning.target_identity(&resource_uid);
    let build_id = provisioning.scale_up_build_id(&resource_uid).unwrap();
    let authority = BuildAuthority {
        build_id: build_id.clone(),
        kind: BuildAuthorityKind::Provisioning,
        source: source.clone(),
        target: target.clone(),
        current_configuration: previous,
        replication_boundary_lsn: 4,
    };
    let command = kuberic_protocol::command::EnsureReplicaBuild {
        operation_id: build_id.clone(),
        local_replica_id: target.replica_id,
        expected_instance_id: target.instance_id.clone(),
        expected_agent_generation: target.agent_generation.clone(),
        target: target.clone(),
        authority: Some(authority.clone()),
        source_session_id: Some(kuberic_protocol::types::ProcessSessionId::new(
            "retired-source-session",
        )),
        retire: false,
    };
    let mut state = AgentState::new(StorageIdentity {
        schema_version: SCHEMA_VERSION,
        resource_uid: resource_uid.clone(),
        pod_uid: provisioning.pod_uid.clone(),
        pvc_uid: provisioning.pvc_uid.clone(),
        initialization_id: provisioning.initialization_id(&resource_uid),
        local_identity: target.clone(),
        effective_policy: current_policy,
    });
    state.scale_up_initialization = Some(provisioning);
    state.role = ReplicaRole::IdleSecondary;
    state.build_commands.insert(build_id.clone(), command);
    let directory = tempfile::tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let store = Arc::new(SqliteStore::create_authorized(&path, state).unwrap());
    store.admit_build(&authority).await.unwrap();
    store
        .record_build_progress(&DurableBuildProgress {
            authority: authority.clone(),
            last_sequence: 8,
            durable_lsn: 9,
            completed: true,
            catch_up_boundary_lsn: Some(9),
        })
        .await
        .unwrap();

    let application = Arc::new(TestApplication::default());
    let runtime = Arc::new(PodRuntime::new(target.clone(), application, store.clone()));
    let service =
        AgentService::new(store.clone(), runtime.clone(), runtime.clone(), "token").unwrap();
    service.reconstruct_runtime().await.unwrap();
    let snapshot = runtime.snapshot().await;
    assert_eq!(snapshot.role, ReplicaRole::IdleSecondary);
    assert_eq!(snapshot.builds.len(), 1);
    assert_eq!(snapshot.builds[0].authority, authority);
    assert_eq!(snapshot.builds[0].durable_lsn, 9);
    assert_eq!(snapshot.builds[0].catch_up_boundary_lsn, Some(9));
    assert!(
        service
            .sessions()
            .validate_peer(
                &source,
                "retired-source-session",
                service.sessions().local_session().as_str()
            )
            .await
            .is_err()
    );
    service
        .sessions()
        .register_peer(
            source.clone(),
            kuberic_protocol::types::ProcessSessionId::new("current-source-session"),
        )
        .await;
    assert!(
        service
            .sessions()
            .validate_peer(
                &source,
                "retired-source-session",
                service.sessions().local_session().as_str()
            )
            .await
            .is_err()
    );
    assert!(
        service
            .sessions()
            .validate_peer(
                &source,
                "current-source-session",
                service.sessions().local_session().as_str()
            )
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn scale_up_receiver_replays_every_durable_build_boundary_without_role_activation() {
    let boundaries = [
        ("authorized", None),
        ("snapshot-progress", Some((1_u64, 4_i64, false, None))),
        (
            "frozen-catch-up-boundary",
            Some((2_u64, 4_i64, true, Some(9_i64))),
        ),
        ("caught-up", Some((3_u64, 9_i64, true, Some(9_i64)))),
    ];
    for (name, progress) in boundaries {
        let fixture = scale_up_agent_build_fixture();
        let command = kuberic_protocol::command::EnsureReplicaBuild {
            operation_id: fixture.authority.build_id.clone(),
            local_replica_id: fixture.target.replica_id,
            expected_instance_id: fixture.target.instance_id.clone(),
            expected_agent_generation: fixture.target.agent_generation.clone(),
            target: fixture.target.clone(),
            authority: Some(fixture.authority.clone()),
            source_session_id: Some(ProcessSessionId::new("retired-source-session")),
            retire: false,
        };
        let mut state = AgentState::new(StorageIdentity {
            schema_version: SCHEMA_VERSION,
            resource_uid: fixture.resource_uid.clone(),
            pod_uid: fixture.provisioning.pod_uid.clone(),
            pvc_uid: fixture.provisioning.pvc_uid.clone(),
            initialization_id: fixture
                .provisioning
                .initialization_id(&fixture.resource_uid),
            local_identity: fixture.target.clone(),
            effective_policy: fixture.current_policy.clone(),
        });
        state.scale_up_initialization = Some(fixture.provisioning.clone());
        state.role = ReplicaRole::IdleSecondary;
        state
            .build_commands
            .insert(fixture.authority.build_id.clone(), command);
        let directory = tempfile::tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let store = Arc::new(SqliteStore::create_authorized(&path, state).unwrap());
        store.admit_build(&fixture.authority).await.unwrap();
        if let Some((last_sequence, durable_lsn, completed, catch_up_boundary_lsn)) = progress {
            store
                .record_build_progress(&DurableBuildProgress {
                    authority: fixture.authority.clone(),
                    last_sequence,
                    durable_lsn,
                    completed,
                    catch_up_boundary_lsn,
                })
                .await
                .unwrap();
        }
        drop(store);

        let store = Arc::new(SqliteStore::open_existing(&path, None).unwrap());
        let runtime = Arc::new(PodRuntime::new(
            fixture.target.clone(),
            Arc::new(TestApplication::default()),
            store.clone(),
        ));
        let service = AgentService::new(store, runtime.clone(), runtime.clone(), "token").unwrap();
        service.reconstruct_runtime().await.unwrap();
        let snapshot = runtime.snapshot().await;
        assert_eq!(snapshot.role, ReplicaRole::IdleSecondary, "{name}");
        assert_ne!(snapshot.read_status, AccessStatus::Granted, "{name}");
        assert_ne!(snapshot.write_status, AccessStatus::Granted, "{name}");
        assert_eq!(snapshot.builds.len(), 1, "{name}");
        let replayed = &snapshot.builds[0];
        assert_eq!(replayed.authority, fixture.authority, "{name}");
        match progress {
            None => {
                assert_eq!(replayed.last_sequence, 0, "{name}");
                assert_eq!(replayed.durable_lsn, 0, "{name}");
                assert!(!replayed.completed, "{name}");
                assert_eq!(replayed.catch_up_boundary_lsn, None, "{name}");
            }
            Some((last_sequence, durable_lsn, completed, boundary)) => {
                assert_eq!(replayed.last_sequence, last_sequence, "{name}");
                assert_eq!(replayed.durable_lsn, durable_lsn, "{name}");
                assert_eq!(replayed.completed, completed, "{name}");
                assert_eq!(replayed.catch_up_boundary_lsn, boundary, "{name}");
            }
        }
    }
}

#[tokio::test]
async fn scale_up_source_replays_each_durable_build_boundary_with_exact_authority() {
    let boundaries = [
        ("authorized", None),
        ("enumerating", Some((1_u64, 4_i64, false, None))),
        ("boundary-frozen", Some((2_u64, 4_i64, true, Some(9_i64)))),
        (
            "receiver-caught-up",
            Some((3_u64, 9_i64, true, Some(9_i64))),
        ),
    ];
    for (name, progress) in boundaries {
        let fixture = scale_up_agent_build_fixture();
        let command = kuberic_protocol::command::EnsureReplicaBuild {
            operation_id: fixture.authority.build_id.clone(),
            local_replica_id: fixture.source.replica_id,
            expected_instance_id: fixture.source.instance_id.clone(),
            expected_agent_generation: fixture.source.agent_generation.clone(),
            target: fixture.target.clone(),
            authority: None,
            source_session_id: None,
            retire: false,
        };
        let mut state = AgentState::new(StorageIdentity {
            schema_version: SCHEMA_VERSION,
            resource_uid: fixture.resource_uid.clone(),
            pod_uid: PodUid::new(fixture.source.instance_id.as_str()),
            pvc_uid: PvcUid::new("source-pvc"),
            initialization_id: InitializationId::new("source-initialization"),
            local_identity: fixture.source.clone(),
            effective_policy: fixture.previous_policy.clone(),
        });
        state.admitted_policy = Some(fixture.previous_policy.clone());
        state.highest_epoch = fixture.previous.epoch;
        state.current_configuration = Some(fixture.previous.clone());
        state.role = ReplicaRole::Primary;
        state.read_status = AccessStatus::Granted;
        state.write_status = AccessStatus::Granted;
        state
            .build_commands
            .insert(fixture.authority.build_id.clone(), command);
        let directory = tempfile::tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let store = Arc::new(SqliteStore::create_authorized(&path, state).unwrap());
        store
            .admit(&AdmittedAuthority {
                local_identity: fixture.source.clone(),
                transition_kind: None,
                previous_configuration: None,
                current_configuration: fixture.previous.clone(),
                switchover_handoff: None,
                secondary_removal: None,
                scale_up: None,
            })
            .await
            .unwrap();
        store.admit_build(&fixture.authority).await.unwrap();
        if let Some((last_sequence, durable_lsn, completed, catch_up_boundary_lsn)) = progress {
            store
                .record_build_progress(&DurableBuildProgress {
                    authority: fixture.authority.clone(),
                    last_sequence,
                    durable_lsn,
                    completed,
                    catch_up_boundary_lsn,
                })
                .await
                .unwrap();
        }
        drop(store);

        let store = Arc::new(SqliteStore::open_existing(&path, None).unwrap());
        let runtime = Arc::new(PodRuntime::new(
            fixture.source.clone(),
            Arc::new(TestApplication::default()),
            store.clone(),
        ));
        runtime
            .reconstruct(
                OpenMode::Existing,
                ReplicaRole::Primary,
                AccessStatus::Granted,
                AccessStatus::Granted,
                None,
            )
            .await
            .unwrap();
        let command = store
            .load_state()
            .await
            .unwrap()
            .build_commands
            .get(&fixture.authority.build_id)
            .cloned()
            .unwrap();
        let coordinator = Arc::new(Coordinator::new(store.clone(), runtime.clone()));
        let replay = {
            let coordinator = coordinator.clone();
            tokio::spawn(async move { coordinator.ensure_build(command).await })
        };
        let outbound = timeout(Duration::from_secs(1), runtime.data_plane().next_outbound())
            .await
            .unwrap()
            .unwrap();
        let OutboundReplication::Build(endpoint) = outbound else {
            panic!("{name}: expected replayed build, got {outbound:?}");
        };
        assert_eq!(endpoint.build_id, fixture.authority.build_id, "{name}");
        assert_eq!(endpoint.identity, fixture.target, "{name}");
        assert_eq!(endpoint.replication_address, "", "{name}");
        replay.abort();
        assert_eq!(
            store
                .load_build_progress(&fixture.authority.build_id)
                .await
                .unwrap(),
            progress.map(
                |(last_sequence, durable_lsn, completed, catch_up_boundary_lsn)| {
                    DurableBuildProgress {
                        authority: fixture.authority,
                        last_sequence,
                        durable_lsn,
                        completed,
                        catch_up_boundary_lsn,
                    }
                }
            ),
            "{name}"
        );
    }
}

#[tokio::test]
async fn sqlite_retirement_tombstone_precedes_host_open_and_cannot_be_reactivated() {
    use kuberic_agent::sqlite_store::SqliteStore;
    use kuberic_agent::state::{AgentState, SCHEMA_VERSION, StorageIdentity};
    use kuberic_protocol::types::{InitializationId, PodUid, PvcUid};
    let intent = removal_fixture::intent(&[1, 2, 3], 1);
    let local = intent.target.clone();
    let provenance = StorageIdentity {
        schema_version: SCHEMA_VERSION,
        resource_uid: intent.resource_uid.clone(),
        local_identity: local.clone(),
        pod_uid: PodUid::new(local.instance_id.as_str()),
        pvc_uid: PvcUid::new("pvc-3"),
        initialization_id: InitializationId::new("original"),
        effective_policy: intent.previous_policy.clone(),
    };
    let directory = tempfile::tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let store = Arc::new(
        SqliteStore::create_authorized(&path, AgentState::new(provenance.clone())).unwrap(),
    );
    let runtime = open_removal_member(
        &intent,
        local.clone(),
        Arc::new(TestApplication::default()),
        store.clone(),
    )
    .await;
    let retired = kuberic_runtime_internal::authority::RetiredAuthority {
        committed: removal_fixture::cleanup(&intent),
        report: removal_fixture::retirement(&intent),
    };
    runtime
        .apply_effect(effect(
            5,
            RuntimeEffectAction::RetireReplica(Box::new(retired.clone())),
        ))
        .await
        .unwrap();
    drop(runtime);
    drop(store);
    let store = Arc::new(SqliteStore::open_existing(&path, Some(&provenance)).unwrap());
    let application = Arc::new(TestApplication::default());
    let runtime = PodRuntime::new(local, application.clone(), store);
    runtime
        .reconstruct(
            OpenMode::Existing,
            ReplicaRole::ActiveSecondary,
            AccessStatus::Granted,
            AccessStatus::Granted,
            None,
        )
        .await
        .unwrap();
    assert!(application.events.lock().unwrap().is_empty());
    assert!(!runtime.snapshot().await.open);
    assert_eq!(
        runtime.snapshot().await.retired_authority,
        Some(retired.clone())
    );
    assert!(
        runtime
            .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
            .await
            .is_err()
    );
    let replay = runtime
        .apply_effect(effect(
            1,
            RuntimeEffectAction::RetireReplica(Box::new(retired)),
        ))
        .await
        .unwrap();
    assert_eq!(replay.postcondition.role, ReplicaRole::None);
    assert_eq!(replay.postcondition.write_status, AccessStatus::NotPrimary);
}

#[tokio::test]
async fn retirement_started_recovery_fails_closed_until_exact_tombstone_is_durable() {
    use kuberic_agent::sqlite_store::SqliteStore;
    use kuberic_agent::state::{AgentState, SCHEMA_VERSION, StorageIdentity};
    use kuberic_protocol::types::{InitializationId, PodUid, PvcUid};
    let intent = removal_fixture::intent(&[1, 2, 3], 1);
    let provenance = StorageIdentity {
        schema_version: SCHEMA_VERSION,
        resource_uid: intent.resource_uid.clone(),
        local_identity: intent.target.clone(),
        pod_uid: PodUid::new(intent.target.instance_id.as_str()),
        pvc_uid: PvcUid::new("pvc-3"),
        initialization_id: InitializationId::new("original"),
        effective_policy: intent.previous_policy.clone(),
    };
    let directory = tempfile::tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let store =
        Arc::new(SqliteStore::create_authorized(&path, AgentState::new(provenance)).unwrap());
    let original = open_removal_member(
        &intent,
        intent.target.clone(),
        Arc::new(TestApplication::default()),
        store.clone(),
    )
    .await;
    let active = original.snapshot().await.authority.unwrap();
    let retired = kuberic_runtime_internal::authority::RetiredAuthority {
        committed: removal_fixture::cleanup(&intent),
        report: removal_fixture::retirement(&intent),
    };
    store.record_retirement_started(&retired).await.unwrap();
    assert!(original.restore_authority().await.is_err());
    assert!(
        original
            .apply_effect(effect(
                5,
                RuntimeEffectAction::AdmitAuthority(Box::new(active))
            ))
            .await
            .is_err()
    );
    original.abort();
    let application = Arc::new(TestApplication::default());
    let runtime = PodRuntime::new(intent.target.clone(), application.clone(), store.clone());
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_recovery BEFORE INSERT ON runtime_lifecycle
         WHEN NEW.kind = 'retired' BEGIN SELECT RAISE(FAIL, 'failed recovery'); END;",
        )
        .unwrap();
    assert!(
        runtime
            .reconstruct(
                OpenMode::Existing,
                ReplicaRole::ActiveSecondary,
                AccessStatus::Granted,
                AccessStatus::Granted,
                None,
            )
            .await
            .is_err()
    );
    assert!(application.events.lock().unwrap().is_empty());
    assert!(store.load_retired_authority().await.unwrap().is_none());
    assert_eq!(
        store.load_retirement_started().await.unwrap(),
        Some(retired.clone())
    );
    assert!(
        runtime
            .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
            .await
            .is_err()
    );
    connection
        .execute_batch("DROP TRIGGER reject_recovery;")
        .unwrap();
    for _ in 0..2 {
        runtime
            .reconstruct(
                OpenMode::Existing,
                ReplicaRole::ActiveSecondary,
                AccessStatus::Granted,
                AccessStatus::Granted,
                None,
            )
            .await
            .unwrap();
        assert!(application.events.lock().unwrap().is_empty());
        let snapshot = runtime.snapshot().await;
        assert!(!snapshot.open);
        assert_eq!(snapshot.role, ReplicaRole::None);
        assert_eq!(snapshot.read_status, AccessStatus::NotPrimary);
        assert_eq!(snapshot.write_status, AccessStatus::NotPrimary);
        assert!(snapshot.authority.is_none());
        assert_eq!(snapshot.retired_authority, Some(retired.clone()));
    }
    assert!(store.load_retirement_started().await.unwrap().is_none());
    assert!(store.load().await.unwrap().is_none());
}

fn prepare_removal(
    intent: &kuberic_protocol::types::SecondaryScaleDownIntent,
) -> RuntimeEffectAction {
    RuntimeEffectAction::PrepareSecondaryRemoval {
        intent: Box::new(intent.clone()),
        process_session_id: removal_fixture::preparation(intent).process_session_id,
        report_sequence: 1,
    }
}

fn removal_evidence(
    preparation: kuberic_protocol::types::SecondaryRemovalPreparation,
) -> kuberic_protocol::types::SecondaryRemovalEvidence {
    let mut evidence = removal_fixture::evidence(&preparation.intent);
    for witness in evidence
        .previous_read_quorum
        .iter_mut()
        .chain(&mut evidence.reduced_write_quorum)
    {
        witness.verified_replication_lsn = preparation.boundary_lsn;
    }
    evidence.preparation = preparation;
    evidence
}

async fn converge_removal(runtime: &PodRuntime, sequence: &mut u64) -> AdmittedAuthority {
    converge_removal_with_peer_restart(runtime, sequence, false).await
}

async fn converge_removal_with_peer_restart(
    runtime: &PodRuntime,
    sequence: &mut u64,
    restart_peer: bool,
) -> AdmittedAuthority {
    use kuberic_protocol::types::SecondaryRemovalStage;
    let preparation = runtime.snapshot().await.prepared_secondary_removal.unwrap();
    let intent = preparation.intent.clone();
    let evidence = removal_evidence(preparation);
    let mut admitted = AdmittedAuthority {
        local_identity: intent.primary.clone(),
        transition_kind: Some(TransitionKind::SecondaryScaleDown),
        previous_configuration: Some(intent.previous_configuration.clone()),
        current_configuration: intent.current_configuration.clone(),
        switchover_handoff: None,
        scale_up: None,
        secondary_removal: Some(evidence.clone()),
    };
    recovery_action(
        runtime,
        sequence,
        RuntimeEffectAction::AdmitAuthority(Box::new(admitted.clone())),
    )
    .await;
    assert_eq!(
        runtime.snapshot().await.write_status,
        AccessStatus::ReconfigurationPending
    );
    assert!(
        runtime
            .apply_effect(effect(
                *sequence,
                RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted)
            ))
            .await
            .is_err()
    );
    assert!(
        runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("forbidden-pc-write"),
                data: Bytes::new()
            })
            .await
            .is_err()
    );
    for witness in evidence
        .reduced_write_quorum
        .iter()
        .filter(|w| w.identity != intent.primary)
    {
        recovery_action(
            runtime,
            sequence,
            RuntimeEffectAction::RegisterPeerSession {
                identity: witness.identity.clone(),
                session: witness.process_session_id.clone(),
            },
        )
        .await;
        recovery_action(
            runtime,
            sequence,
            RuntimeEffectAction::ObserveSecondaryRemovalWitness(Box::new(witness.clone())),
        )
        .await;
    }
    assert!(runtime.snapshot().await.catch_up_complete);
    admitted.previous_configuration = None;
    admitted.transition_kind = None;
    recovery_action(
        runtime,
        sequence,
        RuntimeEffectAction::AdmitAuthority(Box::new(admitted.clone())),
    )
    .await;
    assert!(matches!(
        timeout(Duration::from_secs(1), runtime.data_plane().next_outbound()).await.unwrap(),
        Some(OutboundReplication::Evict(identity)) if identity == intent.target
    ));
    assert!(
        runtime
            .data_plane()
            .accept_acknowledgement(acknowledgement(&admitted, intent.target.clone(), 100))
            .await
            .is_err()
    );
    assert!(
        runtime
            .apply_effect(effect(
                *sequence,
                RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted)
            ))
            .await
            .is_err()
    );
    let mut current_witnesses =
        removal_fixture::witnesses(&intent, SecondaryRemovalStage::CurrentOnly);
    for witness in &mut current_witnesses {
        witness.verified_replication_lsn = evidence.preparation.boundary_lsn;
        if witness.identity != intent.primary {
            recovery_action(
                runtime,
                sequence,
                RuntimeEffectAction::RegisterPeerSession {
                    identity: witness.identity.clone(),
                    session: witness.process_session_id.clone(),
                },
            )
            .await;
            let mut fresher = witness.clone();
            fresher.report_sequence += 100;
            recovery_action(
                runtime,
                sequence,
                RuntimeEffectAction::ObserveSecondaryRemovalWitness(Box::new(fresher)),
            )
            .await;
            if restart_peer {
                let session = kuberic_protocol::types::ProcessSessionId::new("restarted-peer");
                recovery_action(
                    runtime,
                    sequence,
                    RuntimeEffectAction::RegisterPeerSession {
                        identity: witness.identity.clone(),
                        session: session.clone(),
                    },
                )
                .await;
                assert!(!runtime.snapshot().await.catch_up_complete);
                for stale in [
                    RuntimeEffectAction::RegisterPeerSession {
                        identity: witness.identity.clone(),
                        session: witness.process_session_id.clone(),
                    },
                    RuntimeEffectAction::ObserveSecondaryRemovalWitness(Box::new(witness.clone())),
                    RuntimeEffectAction::ObserveReplicationAck {
                        acknowledgement: Box::new(session_ack(
                            &admitted,
                            witness.identity.clone(),
                            100,
                        )),
                        session: witness.process_session_id.clone(),
                    },
                ] {
                    assert!(
                        runtime
                            .apply_effect(effect(*sequence, stale))
                            .await
                            .is_err()
                    );
                    assert!(!runtime.snapshot().await.catch_up_complete);
                }
                let fresh = restarted_removal_peer(&admitted, witness, session).await;
                recovery_action(
                    runtime,
                    sequence,
                    RuntimeEffectAction::ObserveSecondaryRemovalWitness(Box::new(fresh)),
                )
                .await;
                assert!(runtime.snapshot().await.catch_up_complete);
            }
        }
    }
    let committed = kuberic_protocol::types::SecondaryScaleDownCleanup {
        evidence,
        current_only_write_quorum: current_witnesses,
        retirement: None,
    };
    recovery_action(
        runtime,
        sequence,
        RuntimeEffectAction::AcceptSecondaryRemovalCommit(Box::new(committed.clone())),
    )
    .await;
    recovery_action(
        runtime,
        sequence,
        RuntimeEffectAction::AcceptSecondaryRemovalCommit(Box::new(committed)),
    )
    .await;
    recovery_action(
        runtime,
        sequence,
        RuntimeEffectAction::AdmitAuthority(Box::new(admitted.clone())),
    )
    .await;
    recovery_action(
        runtime,
        sequence,
        RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
    )
    .await;
    admitted
}

async fn restarted_removal_peer(
    authority: &AdmittedAuthority,
    frozen: &kuberic_protocol::types::SecondaryRemovalWitness,
    session: kuberic_protocol::types::ProcessSessionId,
) -> kuberic_protocol::types::SecondaryRemovalWitness {
    use kuberic_agent::coordinator::Coordinator;
    use kuberic_agent::sqlite_store::SqliteStore;
    use kuberic_agent::state::{AgentState, SCHEMA_VERSION, StorageIdentity};
    use kuberic_agent::store::AgentStore;
    use kuberic_protocol::types::{InitializationId, PodUid, PvcUid, SecondaryRemovalStage};

    let mut authority = authority.clone();
    authority.local_identity = frozen.identity.clone();
    let evidence = authority.secondary_removal.clone().unwrap();
    let intent = &evidence.preparation.intent;
    let mut state = AgentState::new(StorageIdentity {
        schema_version: SCHEMA_VERSION,
        resource_uid: intent.resource_uid.clone(),
        local_identity: frozen.identity.clone(),
        pod_uid: PodUid::new(frozen.identity.instance_id.as_str()),
        pvc_uid: PvcUid::new("retained-peer-pvc"),
        initialization_id: InitializationId::new("retained-peer"),
        effective_policy: intent.previous_policy.clone(),
    });
    state.current_configuration = Some(authority.current_configuration.clone());
    state.highest_epoch = authority.current_configuration.epoch;
    state.role = ReplicaRole::ActiveSecondary;
    state.admitted_policy = Some(intent.current_policy.clone());
    state.secondary_removal_evidence = Some(evidence.clone());
    state.next_effect_sequence = 3;
    let directory = tempfile::tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let store = Arc::new(SqliteStore::create_authorized(&path, state).unwrap());
    store.admit(&authority).await.unwrap();
    store
        .record_replication_progress(&ReplicationProgress {
            fence: authority.fence(),
            verified_lsn: frozen.verified_replication_lsn,
        })
        .await
        .unwrap();
    let mut reopened = None;
    for restart in [false, true] {
        let runtime = Arc::new(PodRuntime::new(
            frozen.identity.clone(),
            Arc::new(TestApplication::default()),
            Arc::new(SqliteStore::open_existing(&path, None).unwrap()),
        ));
        runtime
            .reconstruct(
                OpenMode::Existing,
                ReplicaRole::ActiveSecondary,
                AccessStatus::Granted,
                AccessStatus::NotPrimary,
                None,
            )
            .await
            .unwrap();
        assert!(
            runtime
                .snapshot()
                .await
                .accepted_secondary_removal
                .is_none()
        );
        if restart {
            reopened = Some(runtime);
        } else {
            runtime.abort();
        }
    }
    let runtime = reopened.unwrap();
    let snapshot = runtime.snapshot().await;
    assert_eq!(snapshot.authority, Some(authority.clone()));
    let mut fresh = frozen.clone();
    fresh.process_session_id = session;
    fresh.report_sequence = 1;
    fresh.verified_replication_lsn = snapshot.verified_replication_lsn.unwrap();
    let mut committed = removal_fixture::cleanup(intent);
    committed.evidence = evidence.clone();
    for witness in &mut committed.current_only_write_quorum {
        witness.verified_replication_lsn = evidence.preparation.boundary_lsn;
    }
    let primary = committed
        .current_only_write_quorum
        .iter()
        .find(|w| w.identity == intent.primary)
        .unwrap();
    let mut sequence = 1;
    recovery_action(
        &runtime,
        &mut sequence,
        RuntimeEffectAction::RegisterPeerSession {
            identity: primary.identity.clone(),
            session: primary.process_session_id.clone(),
        },
    )
    .await;
    recovery_action(
        &runtime,
        &mut sequence,
        RuntimeEffectAction::ObserveSecondaryRemovalWitness(Box::new(primary.clone())),
    )
    .await;
    let coordinator = Coordinator::new(store.clone(), runtime);
    coordinator
        .accept_secondary_removal_commit(kuberic_protocol::command::AcceptSecondaryRemovalCommit {
            operation_id: intent
                .command_operation_id(SecondaryRemovalStage::AcceptCommit, &frozen.identity),
            target: frozen.identity.clone(),
            committed: committed.clone(),
            local_recovery: false,
        })
        .await
        .unwrap();
    assert_eq!(
        store.load_state().await.unwrap().accepted_secondary_removal,
        Some(committed)
    );
    fresh
}

#[tokio::test]
async fn removal_commit_replay_after_retained_peer_restart_uses_live_session_credit() {
    let intent = removal_fixture::intent(&[1, 2, 3], 1);
    let store = Arc::new(MemoryAuthorityStore::default());
    let runtime = open_removal_member(
        &intent,
        intent.primary.clone(),
        Arc::new(TestApplication::default()),
        store.clone(),
    )
    .await;
    let mut sequence = 5;
    recovery_action(&runtime, &mut sequence, prepare_removal(&intent)).await;
    let authority = converge_removal_with_peer_restart(&runtime, &mut sequence, true).await;
    let committed = runtime.snapshot().await.accepted_secondary_removal.unwrap();
    assert_eq!(
        store.load_secondary_removal_commit().await.unwrap(),
        Some(committed.clone())
    );
    runtime.restore_authority().await.unwrap();
    assert_eq!(
        runtime.snapshot().await.accepted_secondary_removal,
        Some(committed.clone())
    );
    let pending = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("fresh-after-peer-restart"),
            data: Bytes::from_static(b"fresh"),
        })
        .await
        .unwrap();
    let peer = intent.current_configuration.members[1].identity.clone();
    let stale = &committed.current_only_write_quorum[1];
    assert!(
        runtime
            .apply_effect(effect(
                sequence,
                RuntimeEffectAction::ObserveReplicationAck {
                    acknowledgement: Box::new(session_ack(&authority, peer.clone(), 1)),
                    session: stale.process_session_id.clone(),
                }
            ))
            .await
            .is_err()
    );
    assert_eq!(runtime.snapshot().await.committed_lsn, 0);
    recovery_action(
        &runtime,
        &mut sequence,
        RuntimeEffectAction::ObserveReplicationAck {
            acknowledgement: Box::new(session_ack(&authority, peer, 1)),
            session: kuberic_protocol::types::ProcessSessionId::new("restarted-peer"),
        },
    )
    .await;
    assert_eq!(pending.committed().await.unwrap().lsn, 1);
}

#[tokio::test]
async fn historical_removal_acceptance_requires_exact_verified_local_boundary() {
    let intent = removal_fixture::intent(&[1, 2, 3, 4, 5, 6], 1);
    let local = intent.current_configuration.members[4].identity.clone();
    let store = Arc::new(MemoryAuthorityStore::default());
    let runtime = open_removal_member(
        &intent,
        local.clone(),
        Arc::new(TestApplication::default()),
        store.clone(),
    )
    .await;
    let evidence = removal_fixture::evidence(&intent);
    let mut authority = AdmittedAuthority {
        local_identity: local.clone(),
        transition_kind: Some(TransitionKind::SecondaryScaleDown),
        previous_configuration: Some(intent.previous_configuration.clone()),
        current_configuration: intent.current_configuration.clone(),
        switchover_handoff: None,
        scale_up: None,
        secondary_removal: Some(evidence.clone()),
    };
    runtime
        .apply_effect(effect(
            5,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority.clone())),
        ))
        .await
        .unwrap();
    authority.previous_configuration = None;
    authority.transition_kind = None;
    runtime
        .apply_effect(effect(
            6,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority)),
        ))
        .await
        .unwrap();
    let command = kuberic_protocol::command::AcceptSecondaryRemovalCommit {
        operation_id: intent.command_operation_id(
            kuberic_protocol::types::SecondaryRemovalStage::AcceptCommit,
            &local,
        ),
        target: local,
        committed: removal_fixture::cleanup(&intent),
        local_recovery: true,
    };
    let before = runtime.snapshot().await;
    assert!(before.verified_replication_lsn.unwrap() < evidence.preparation.boundary_lsn);
    assert!(
        runtime
            .apply_effect(effect(
                7,
                RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(Box::new(command))
            ))
            .await
            .is_err()
    );
    assert_eq!(runtime.snapshot().await, before);
    assert!(
        store
            .load_secondary_removal_commit()
            .await
            .unwrap()
            .is_none()
    );
}

fn session_ack(
    authority: &AdmittedAuthority,
    receiver: ReplicaIdentity,
    lsn: i64,
) -> kuberic_runtime_internal::transport::ReplicationAck {
    kuberic_runtime_internal::transport::ReplicationAck {
        sender: authority.primary_identity().clone(),
        receiver,
        epoch: authority.current_configuration.epoch,
        previous_configuration_id: authority.fence().previous_configuration_id,
        current_configuration_id: authority.current_configuration.configuration_id.clone(),
        received_lsn: lsn,
        applied_lsn: lsn,
        committed_lsn: 0,
    }
}

#[tokio::test]
async fn sequential_removal_preparation_preserves_previous_commit_until_new_admission() {
    let first = removal_fixture::intent(&[1, 2, 3], 1);
    let runtime = open_removal_member(
        &first,
        first.primary.clone(),
        Arc::new(TestApplication::default()),
        Arc::new(MemoryAuthorityStore::default()),
    )
    .await;
    let mut sequence = 5;
    recovery_action(&runtime, &mut sequence, prepare_removal(&first)).await;
    converge_removal(&runtime, &mut sequence).await;
    let committed = runtime.snapshot().await.accepted_secondary_removal.unwrap();
    let mut next = removal_fixture::intent(&[1, 2], 1);
    next.previous_configuration = first.current_configuration.clone();
    next.current_configuration = ConfigurationDescriptor::new(
        Epoch::new(
            first.current_configuration.epoch.data_loss_number,
            first.current_configuration.epoch.configuration_number + 1,
        ),
        next.current_configuration.primary_id,
        next.current_configuration.members,
        next.current_configuration.write_quorum,
    );
    next.spec_generation += 1;
    next.operation_id = next.expected_operation_id();
    recovery_action(&runtime, &mut sequence, prepare_removal(&next)).await;
    let snapshot = runtime.snapshot().await;
    assert_eq!(snapshot.accepted_secondary_removal, Some(committed));
    assert_eq!(snapshot.prepared_secondary_removal.unwrap().intent, next);
    assert_eq!(snapshot.write_status, AccessStatus::ReconfigurationPending);
    for action in [
        RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::Granted,
            write: AccessStatus::Granted,
        },
        RuntimeEffectAction::AdmitAuthority(Box::new(snapshot.authority.unwrap())),
        RuntimeEffectAction::PrepareSwitchover {
            preparation_generation: 1,
            request_id: kuberic_protocol::types::SwitchoverRequestId::new("stale-handoff"),
            source: next.primary.clone(),
            target: next.target.clone(),
            starting_configuration_id: next.previous_configuration.configuration_id.clone(),
            starting_epoch: next.previous_configuration.epoch,
        },
    ] {
        assert!(
            runtime
                .apply_effect(effect(sequence, action))
                .await
                .is_err(),
            "a previous commit must not authorize reopening or replacing a newer preparation"
        );
    }
    converge_removal(&runtime, &mut sequence).await;
    assert_eq!(runtime.snapshot().await.write_status, AccessStatus::Granted);
}

#[tokio::test]
async fn secondary_removal_reaches_each_reduced_quorum_and_only_then_grants_fresh_writes() {
    for size in 2..=5 {
        let intent = removal_fixture::intent(&(1..=size).collect::<Vec<_>>(), 1);
        let application = Arc::new(TestApplication::default());
        let store = Arc::new(MemoryAuthorityStore::default());
        let runtime = open_removal_member(
            &intent,
            intent.primary.clone(),
            application.clone(),
            store.clone(),
        )
        .await;
        let old = runtime.snapshot().await.authority.unwrap();
        let pending = runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("successful-before-removal"),
                data: Bytes::from_static(b"retained"),
            })
            .await
            .unwrap();
        for member in intent.previous_configuration.members.iter().skip(1) {
            runtime
                .data_plane()
                .accept_acknowledgement(acknowledgement(&old, member.identity.clone(), pending.lsn))
                .await
                .unwrap();
        }
        assert_eq!(pending.committed().await.unwrap().lsn, 1);
        let prepared = runtime
            .apply_effect(effect(5, prepare_removal(&intent)))
            .await
            .unwrap();
        assert_eq!(
            prepared
                .postcondition
                .prepared_secondary_removal
                .as_ref()
                .unwrap()
                .boundary_lsn,
            1
        );
        assert_eq!(
            runtime
                .apply_effect(effect(5, prepare_removal(&intent)))
                .await
                .unwrap(),
            prepared
        );
        runtime
            .apply_effect(effect(6, prepare_removal(&intent)))
            .await
            .unwrap();
        let mut conflicting = intent.clone();
        conflicting.cleanup.pvc = kuberic_protocol::types::CleanupResourceIdentity::Absent {
            name: "conflicting-pvc".into(),
        };
        assert!(
            runtime
                .apply_effect(effect(7, prepare_removal(&conflicting)))
                .await
                .is_err()
        );
        let mut sequence = 7;
        let reduced = converge_removal(&runtime, &mut sequence).await;
        let fresh = runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("fresh-after-removal"),
                data: Bytes::from_static(b"new"),
            })
            .await
            .unwrap();
        assert_eq!(fresh.replication_items.len(), size as usize - 2);
        assert!(
            fresh
                .replication_items
                .iter()
                .all(|item| item.receiver.as_ref().unwrap().replica_id
                    != intent.target.replica_id.value())
        );
        for member in reduced.current_configuration.members.iter().skip(1) {
            runtime
                .data_plane()
                .accept_acknowledgement(acknowledgement(
                    &reduced,
                    member.identity.clone(),
                    fresh.lsn,
                ))
                .await
                .unwrap();
        }
        assert_eq!(fresh.committed().await.unwrap().lsn, 2);
        assert_eq!(application.applied.lock().unwrap().len(), 2);
        assert_eq!(
            store.load_secondary_removal().await.unwrap(),
            prepared.postcondition.prepared_secondary_removal
        );
        let json = serde_json::to_vec(&prepared).unwrap();
        assert_eq!(
            serde_json::from_slice::<RuntimeEffectResult>(&json).unwrap(),
            prepared
        );
    }
}

#[tokio::test]
async fn secondary_removal_preparation_fences_pending_ack_and_recovers_unknown_writes() {
    for failure in ["pending", "reserved", "applied", "registered"] {
        let intent = removal_fixture::intent(&[1, 2], 1);
        let application = Arc::new(TestApplication::default());
        let store = Arc::new(MemoryAuthorityStore::default());
        let runtime = open_removal_member(
            &intent,
            intent.primary.clone(),
            application.clone(),
            store.clone(),
        )
        .await;
        application
            .fail_apply
            .store(failure == "reserved", Ordering::SeqCst);
        application
            .fail_after_apply
            .store(failure == "applied", Ordering::SeqCst);
        store
            .fail_registered_write_once
            .store(failure == "registered", Ordering::SeqCst);
        let write = runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("unknown-outcome"),
                data: Bytes::from_static(b"unknown"),
            })
            .await;
        application.fail_apply.store(false, Ordering::SeqCst);
        let old = runtime.snapshot().await.authority.unwrap();
        let prepared = runtime
            .apply_effect(effect(5, prepare_removal(&intent)))
            .await
            .unwrap();
        assert_eq!(
            prepared
                .postcondition
                .prepared_secondary_removal
                .unwrap()
                .boundary_lsn,
            1
        );
        if let Ok(pending) = write {
            runtime
                .data_plane()
                .accept_acknowledgement(acknowledgement(&old, intent.target.clone(), 1))
                .await
                .unwrap();
            assert!(
                pending.committed().await.is_err(),
                "late ACK cannot turn a fenced completion into success"
            );
        }
        let mut sequence = 6;
        converge_removal(&runtime, &mut sequence).await;
        let replay = runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("unknown-outcome"),
                data: Bytes::from_static(b"unknown"),
            })
            .await
            .unwrap();
        assert_eq!(replay.committed().await.unwrap().lsn, 1);
        assert_eq!(
            application.applied.lock().unwrap().len(),
            1,
            "unknown original identity was not duplicated"
        );
    }
}

#[tokio::test]
async fn secondary_removal_preparation_serializes_apply_and_successful_commit_races() {
    for race in ["apply", "commit"] {
        let intent = removal_fixture::intent(&[1, 2], 1);
        let application = Arc::new(TestApplication::default());
        let store = Arc::new(MemoryAuthorityStore::default());
        let runtime = open_removal_member(
            &intent,
            intent.primary.clone(),
            application.clone(),
            store.clone(),
        )
        .await;
        application
            .pause_after_apply
            .store(race == "apply", Ordering::SeqCst);
        store
            .pause_committed_write
            .store(race == "commit", Ordering::SeqCst);
        let writer = {
            let runtime = runtime.clone();
            tokio::spawn(async move {
                runtime
                    .data_plane()
                    .begin_write(ClientWrite {
                        operation_id: OperationId::new("racing-write"),
                        data: Bytes::from_static(b"race"),
                    })
                    .await
                    .unwrap()
            })
        };
        if race == "apply" {
            application.applied_notify.notified().await;
        } else {
            let pending = writer.await.unwrap();
            let old = runtime.snapshot().await.authority.unwrap();
            let ack = acknowledgement(&old, intent.target.clone(), 1);
            let ack_runtime = runtime.clone();
            let ack_task =
                tokio::spawn(
                    async move { ack_runtime.data_plane().accept_acknowledgement(ack).await },
                );
            store.committed_write_notify.notified().await;
            // The writer handle is consumed only in this branch.
            let prepare = {
                let runtime = runtime.clone();
                let intent = intent.clone();
                tokio::spawn(async move {
                    runtime
                        .apply_effect(effect(5, prepare_removal(&intent)))
                        .await
                })
            };
            tokio::task::yield_now().await;
            assert!(!prepare.is_finished());
            store.resume_committed_write_notify.notify_one();
            ack_task.await.unwrap().unwrap();
            assert_eq!(pending.committed().await.unwrap().lsn, 1);
            assert_eq!(
                prepare
                    .await
                    .unwrap()
                    .unwrap()
                    .postcondition
                    .prepared_secondary_removal
                    .unwrap()
                    .boundary_lsn,
                1
            );
            continue;
        }
        let prepare = {
            let runtime = runtime.clone();
            let intent = intent.clone();
            tokio::spawn(async move {
                runtime
                    .apply_effect(effect(5, prepare_removal(&intent)))
                    .await
            })
        };
        tokio::task::yield_now().await;
        assert!(!prepare.is_finished());
        application.resume_notify.notify_one();
        let pending = writer.await.unwrap();
        assert_eq!(
            prepare
                .await
                .unwrap()
                .unwrap()
                .postcondition
                .prepared_secondary_removal
                .unwrap()
                .boundary_lsn,
            1
        );
        assert!(pending.committed().await.is_err());
    }
}

#[tokio::test]
async fn secondary_removal_retirement_closes_host_and_survives_reconstruction() {
    let intent = removal_fixture::intent(&[1, 2], 1);
    let app = Arc::new(TestApplication::default());
    let store = Arc::new(MemoryAuthorityStore::default());
    let target =
        open_removal_member(&intent, intent.target.clone(), app.clone(), store.clone()).await;
    let old_authority = target.snapshot().await.authority.unwrap();
    let retired = kuberic_runtime_internal::authority::RetiredAuthority {
        committed: removal_fixture::cleanup(&intent),
        report: removal_fixture::retirement(&intent),
    };
    let effect = effect(
        5,
        RuntimeEffectAction::RetireReplica(Box::new(retired.clone())),
    );
    let result = target.apply_effect(effect.clone()).await.unwrap();
    assert_eq!(target.apply_effect(effect).await.unwrap(), result);
    assert!(!result.postcondition.open);
    assert_eq!(result.postcondition.role, ReplicaRole::None);
    assert_eq!(result.postcondition.read_status, AccessStatus::NotPrimary);
    assert_eq!(result.postcondition.write_status, AccessStatus::NotPrimary);
    assert!(result.postcondition.authority.is_none());
    assert_eq!(
        result.postcondition.retired_authority,
        Some(retired.clone())
    );
    assert!(
        target
            .apply_effect(self::effect(
                2,
                RuntimeEffectAction::AdmitAuthority(Box::new(old_authority.clone()))
            ))
            .await
            .is_err(),
        "cached active effects cannot replay across terminal retirement"
    );
    let late = retry_item(
        &acknowledgement(&old_authority, intent.target.clone(), 1),
        intent.target.clone(),
    );
    assert!(target.data_plane().receive_replication(late).await.is_err());
    assert_eq!(
        store.load_retired_authority().await.unwrap(),
        Some(retired.clone())
    );
    assert!(
        app.events
            .lock()
            .unwrap()
            .iter()
            .any(|event| event == "service.close")
    );
    let retained_handle = app.state_replicator.lock().unwrap().clone().unwrap();
    assert!(retained_handle.replicate(Bytes::new()).await.is_err());
    assert!(
        target
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("retired-write"),
                data: Bytes::new()
            })
            .await
            .is_err()
    );
    assert!(
        target
            .apply_effect(self::effect(
                6,
                RuntimeEffectAction::SetReadStatus(AccessStatus::Granted)
            ))
            .await
            .is_err()
    );
    let mut conflict = retired.clone();
    conflict.report.process_session_id =
        kuberic_protocol::types::ProcessSessionId::new("conflicting-session");
    assert!(
        target
            .apply_effect(self::effect(
                6,
                RuntimeEffectAction::RetireReplica(Box::new(conflict))
            ))
            .await
            .is_err()
    );
    let json = serde_json::to_vec(&result).unwrap();
    assert_eq!(
        serde_json::from_slice::<RuntimeEffectResult>(&json).unwrap(),
        result
    );
    drop(target);
    let restarted_app = Arc::new(TestApplication::default());
    let restarted = PodRuntime::new(intent.target.clone(), restarted_app.clone(), store);
    restarted
        .reconstruct(
            OpenMode::Existing,
            ReplicaRole::ActiveSecondary,
            AccessStatus::Granted,
            AccessStatus::NotPrimary,
            None,
        )
        .await
        .unwrap();
    assert!(!restarted.snapshot().await.open);
    assert_eq!(restarted.snapshot().await.retired_authority, Some(retired));
    assert!(
        restarted_app.partition.lock().unwrap().is_none(),
        "tombstone checked before application Open"
    );
    assert!(
        restarted
            .apply_effect(self::effect(
                1,
                RuntimeEffectAction::Open(OpenMode::Existing)
            ))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn secondary_removal_retirement_cancels_unacknowledged_inbound_delivery() {
    let intent = removal_fixture::intent(&[1, 2], 1);
    let app = Arc::new(TestApplication::default());
    app.manual_streams.store(true, Ordering::SeqCst);
    let target = open_removal_member(
        &intent,
        intent.target.clone(),
        app.clone(),
        Arc::new(MemoryAuthorityStore::default()),
    )
    .await;
    let old_authority = target.snapshot().await.authority.unwrap();
    let pending = target
        .data_plane()
        .receive_replication(retry_item(
            &acknowledgement(&old_authority, intent.target.clone(), 1),
            intent.target.clone(),
        ))
        .await
        .unwrap();
    let applied = tokio::spawn(async move { pending.applied().await });
    let mut stream = app.held_streams.lock().unwrap().remove(0);
    let operation = stream.get_operation().await.unwrap().unwrap();
    let retired = kuberic_runtime_internal::authority::RetiredAuthority {
        committed: removal_fixture::cleanup(&intent),
        report: removal_fixture::retirement(&intent),
    };

    let result = timeout(
        Duration::from_secs(1),
        target.apply_effect(effect(
            5,
            RuntimeEffectAction::RetireReplica(Box::new(retired)),
        )),
    )
    .await
    .expect("retirement must not wait for the old application acknowledgement")
    .unwrap();

    assert!(!result.postcondition.open);
    assert_eq!(result.postcondition.role, ReplicaRole::None);
    assert!(applied.await.unwrap().is_err());
    assert!(
        operation
            .acknowledge(DurableApplicationProgress {
                applied_lsn: 1,
                committed_lsn: 0,
            })
            .is_err()
    );
    assert!(
        app.events
            .lock()
            .unwrap()
            .iter()
            .any(|event| event == "service.close")
    );
}

#[tokio::test]
async fn rejected_secondary_removal_retirement_keeps_inbound_delivery_open() {
    let intent = removal_fixture::intent(&[1, 2], 1);
    let app = Arc::new(TestApplication::default());
    app.manual_streams.store(true, Ordering::SeqCst);
    let store = Arc::new(MemoryAuthorityStore::default());
    let target =
        open_removal_member(&intent, intent.target.clone(), app.clone(), store.clone()).await;
    let old_authority = target.snapshot().await.authority.unwrap();
    let pending = target
        .data_plane()
        .receive_replication(retry_item(
            &acknowledgement(&old_authority, intent.target.clone(), 1),
            intent.target.clone(),
        ))
        .await
        .unwrap();
    let applied = tokio::spawn(async move { pending.applied().await });
    let mut stream = app.held_streams.lock().unwrap().remove(0);
    let operation = stream.get_operation().await.unwrap().unwrap();
    let mut conflicting_intent = intent.clone();
    conflicting_intent
        .previous_configuration
        .epoch
        .configuration_number += 10;
    conflicting_intent.previous_configuration.configuration_id =
        conflicting_intent.previous_configuration.expected_id();
    conflicting_intent
        .current_configuration
        .epoch
        .configuration_number += 10;
    conflicting_intent.current_configuration.configuration_id =
        conflicting_intent.current_configuration.expected_id();
    conflicting_intent.operation_id = conflicting_intent.expected_operation_id();
    let conflicting = kuberic_runtime_internal::authority::RetiredAuthority {
        committed: removal_fixture::cleanup(&conflicting_intent),
        report: removal_fixture::retirement(&conflicting_intent),
    };

    assert!(
        target
            .apply_effect(effect(
                5,
                RuntimeEffectAction::RetireReplica(Box::new(conflicting)),
            ))
            .await
            .is_err()
    );
    assert!(store.load_retirement_started().await.unwrap().is_none());
    assert!(!applied.is_finished());
    operation
        .acknowledge(DurableApplicationProgress {
            applied_lsn: 1,
            committed_lsn: 0,
        })
        .unwrap();
    assert_eq!(applied.await.unwrap().unwrap().applied_lsn, 1);
    assert!(target.snapshot().await.open);
    assert_eq!(target.snapshot().await.role, ReplicaRole::ActiveSecondary);
}

#[tokio::test]
async fn secondary_removal_preparation_failure_is_closed_and_exactly_replayable() {
    for ambiguous in [false, true] {
        let intent = removal_fixture::intent(&[1, 2], 1);
        let store = Arc::new(MemoryAuthorityStore::default());
        let runtime = open_removal_member(
            &intent,
            intent.primary.clone(),
            Arc::new(TestApplication::default()),
            store.clone(),
        )
        .await;
        store
            .fail_preparation_once
            .store(!ambiguous, Ordering::SeqCst);
        store
            .fail_after_preparation_once
            .store(ambiguous, Ordering::SeqCst);
        assert!(
            runtime
                .apply_effect(effect(5, prepare_removal(&intent)))
                .await
                .is_err()
        );
        assert_ne!(runtime.snapshot().await.write_status, AccessStatus::Granted);
        assert!(
            runtime
                .apply_effect(effect(
                    5,
                    RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted)
                ))
                .await
                .is_err()
        );
        let mut conflict = intent.clone();
        conflict.cleanup.pvc = kuberic_protocol::types::CleanupResourceIdentity::Absent {
            name: "other".into(),
        };
        assert!(
            runtime
                .apply_effect(effect(5, prepare_removal(&conflict)))
                .await
                .is_err()
        );
        let result = runtime
            .apply_effect(effect(5, prepare_removal(&intent)))
            .await
            .unwrap();
        assert_eq!(
            result
                .postcondition
                .prepared_secondary_removal
                .unwrap()
                .boundary_lsn,
            0
        );
    }
}

#[tokio::test]
async fn secondary_removal_never_certifies_raw_application_progress() {
    let intent = removal_fixture::intent(&[1, 2], 1);
    let app = Arc::new(TestApplication::default());
    *app.progress.lock().unwrap() = DurableApplicationProgress {
        applied_lsn: 100,
        committed_lsn: 100,
    };
    let runtime = open_removal_member(
        &intent,
        intent.primary.clone(),
        app,
        Arc::new(MemoryAuthorityStore::default()),
    )
    .await;
    assert!(
        runtime
            .apply_effect(effect(5, prepare_removal(&intent)))
            .await
            .is_err()
    );
    assert!(
        runtime
            .snapshot()
            .await
            .prepared_secondary_removal
            .is_none()
    );
    assert_ne!(runtime.snapshot().await.write_status, AccessStatus::Granted);
}

#[tokio::test]
async fn secondary_removal_reconstructs_successful_and_unknown_prefixes_and_singleton() {
    for successful in [false, true] {
        let intent = removal_fixture::intent(&[1, 2], 1);
        let store = Arc::new(MemoryAuthorityStore::default());
        let app = Arc::new(TestApplication::default());
        let runtime =
            open_removal_member(&intent, intent.primary.clone(), app.clone(), store.clone()).await;
        let pending = runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("before-restart"),
                data: Bytes::from_static(b"persistent"),
            })
            .await
            .unwrap();
        if successful {
            let old = runtime.snapshot().await.authority.unwrap();
            runtime
                .data_plane()
                .accept_acknowledgement(acknowledgement(&old, intent.target.clone(), 1))
                .await
                .unwrap();
            assert_eq!(pending.committed().await.unwrap().lsn, 1);
        } else {
            drop(pending);
        }
        let durable = *app.progress.lock().unwrap();
        let operations = app.applied.lock().unwrap().clone();
        drop(runtime);
        let reopened_app = Arc::new(TestApplication::default());
        *reopened_app.progress.lock().unwrap() = durable;
        *reopened_app.applied.lock().unwrap() = operations;
        let reopened = PodRuntime::new(intent.primary.clone(), reopened_app.clone(), store.clone());
        reopened
            .reconstruct(
                OpenMode::Existing,
                ReplicaRole::Primary,
                AccessStatus::ReconfigurationPending,
                AccessStatus::ReconfigurationPending,
                None,
            )
            .await
            .unwrap();
        reopened
            .apply_effect(effect(1, prepare_removal(&intent)))
            .await
            .unwrap();
        assert_eq!(
            reopened
                .snapshot()
                .await
                .prepared_secondary_removal
                .unwrap()
                .boundary_lsn,
            1
        );
        let mut sequence = 2;
        converge_removal(&reopened, &mut sequence).await;
        let committed = reopened
            .snapshot()
            .await
            .accepted_secondary_removal
            .unwrap();
        let durable = *reopened_app.progress.lock().unwrap();
        let operations = reopened_app.applied.lock().unwrap().clone();
        drop(reopened);
        let singleton_app = Arc::new(TestApplication::default());
        *singleton_app.progress.lock().unwrap() = durable;
        *singleton_app.applied.lock().unwrap() = operations;
        let singleton = PodRuntime::new(intent.primary.clone(), singleton_app, store);
        singleton
            .reconstruct(
                OpenMode::Existing,
                ReplicaRole::Primary,
                AccessStatus::ReconfigurationPending,
                AccessStatus::ReconfigurationPending,
                None,
            )
            .await
            .unwrap();
        assert!(
            singleton
                .apply_effect(effect(
                    1,
                    RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted)
                ))
                .await
                .is_err()
        );
        singleton
            .apply_effect(effect(
                1,
                RuntimeEffectAction::AcceptSecondaryRemovalCommit(Box::new(committed)),
            ))
            .await
            .unwrap();
        singleton
            .apply_effect(effect(
                2,
                RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
            ))
            .await
            .unwrap();
        let write = singleton
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("new-singleton-write"),
                data: Bytes::from_static(b"fresh"),
            })
            .await
            .unwrap();
        assert_eq!(write.committed().await.unwrap().lsn, 2);
    }
}

#[tokio::test]
async fn secondary_removal_retirement_publishes_tombstone_only_after_host_closes() {
    for ambiguous in [false, true] {
        secondary_removal_retirement_persistence_boundary(ambiguous).await;
    }
}

async fn secondary_removal_retirement_persistence_boundary(ambiguous: bool) {
    let intent = removal_fixture::intent(&[1, 2], 1);
    let store = Arc::new(MemoryAuthorityStore::default());
    let app = Arc::new(TestApplication::default());
    let target =
        open_removal_member(&intent, intent.target.clone(), app.clone(), store.clone()).await;
    let retired = kuberic_runtime_internal::authority::RetiredAuthority {
        committed: removal_fixture::cleanup(&intent),
        report: removal_fixture::retirement(&intent),
    };
    app.pause_close.store(true, Ordering::SeqCst);
    store
        .fail_retirement_once
        .store(!ambiguous, Ordering::SeqCst);
    store
        .fail_after_retirement_once
        .store(ambiguous, Ordering::SeqCst);
    let action = effect(
        5,
        RuntimeEffectAction::RetireReplica(Box::new(retired.clone())),
    );
    let task = {
        let target = target.clone();
        let action = action.clone();
        tokio::spawn(async move { target.apply_effect(action).await })
    };
    app.close_notify.notified().await;
    assert!(store.load_retired_authority().await.unwrap().is_none());
    assert!(target.snapshot().await.retired_authority.is_none());
    assert_ne!(target.snapshot().await.read_status, AccessStatus::Granted);
    assert_ne!(target.snapshot().await.write_status, AccessStatus::Granted);
    app.resume_close_notify.notify_one();
    assert!(
        task.await.unwrap().is_err(),
        "tombstone persistence failure must not acknowledge retirement"
    );
    assert!(target.snapshot().await.retired_authority.is_none());
    let result = target.apply_effect(action).await.unwrap();
    assert_eq!(result.postcondition.retired_authority, Some(retired));
    assert_eq!(
        app.events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.as_str() == "service.close")
            .count(),
        1
    );
}

#[tokio::test]
async fn secondary_removal_preparations_advance_only_after_the_previous_commit() {
    let first = removal_fixture::intent(&[1, 2, 3], 1);
    let runtime = open_removal_member(
        &first,
        first.primary.clone(),
        Arc::new(TestApplication::default()),
        Arc::new(MemoryAuthorityStore::default()),
    )
    .await;
    runtime
        .apply_effect(effect(5, prepare_removal(&first)))
        .await
        .unwrap();
    let mut sequence = 6;
    let reduced = converge_removal(&runtime, &mut sequence).await;
    let mut second = removal_fixture::intent(&[1, 2], 1);
    second.previous_configuration = reduced.current_configuration;
    second.current_configuration = ConfigurationDescriptor::new(
        Epoch::new(2, 12),
        second.current_configuration.primary_id,
        second.current_configuration.members.clone(),
        1,
    );
    second.operation_id = second.expected_operation_id();
    recovery_action(&runtime, &mut sequence, prepare_removal(&second)).await;
    let singleton = converge_removal(&runtime, &mut sequence).await;
    assert_eq!(singleton.current_configuration.members.len(), 1);
    assert!(
        runtime
            .apply_effect(effect(sequence, prepare_removal(&first)))
            .await
            .is_err()
    );
    let newer = AdmittedAuthority {
        current_configuration: ConfigurationDescriptor::new(
            Epoch::new(2, 13),
            singleton.current_configuration.primary_id,
            singleton.current_configuration.members.clone(),
            1,
        ),
        scale_up: None,
        secondary_removal: None,
        ..singleton
    };
    recovery_action(
        &runtime,
        &mut sequence,
        RuntimeEffectAction::AdmitAuthority(Box::new(newer)),
    )
    .await;
    recovery_action(
        &runtime,
        &mut sequence,
        RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
    )
    .await;
    recovery_action(
        &runtime,
        &mut sequence,
        RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
    )
    .await;
    assert!(
        runtime
            .snapshot()
            .await
            .prepared_secondary_removal
            .is_none()
    );
}

#[derive(Default)]
struct MemoryAuthorityStore {
    lifecycle: Mutex<()>,
    authority: Mutex<Option<AdmittedAuthority>>,
    prepared_secondary_removal: Mutex<Option<kuberic_protocol::types::SecondaryRemovalPreparation>>,
    retired_authority: Mutex<Option<kuberic_runtime_internal::authority::RetiredAuthority>>,
    retirement_started: Mutex<Option<kuberic_runtime_internal::authority::RetiredAuthority>>,
    accepted_removal: Mutex<Option<kuberic_protocol::types::SecondaryScaleDownCleanup>>,
    fail_preparation_once: AtomicBool,
    fail_after_preparation_once: AtomicBool,
    fail_retirement_once: AtomicBool,
    fail_after_retirement_once: AtomicBool,
    replication_progress: Mutex<BTreeMap<AuthorityFence, ReplicationProgress>>,
    local_writes: Mutex<BTreeMap<OperationId, DurableLocalWrite>>,
    builds: Mutex<BTreeMap<OperationId, BuildAuthority>>,
    build_progress: Mutex<BTreeMap<OperationId, DurableBuildProgress>>,
    admit_count: AtomicUsize,
    fail_after_admit: AtomicBool,
    fail_build_progress_once: AtomicBool,
    pause_build_progress: AtomicBool,
    build_progress_notify: Notify,
    resume_build_progress_notify: Notify,
    fail_registered_write_once: AtomicBool,
    pause_registered_write: AtomicBool,
    registered_write_notify: Notify,
    resume_registered_write_notify: Notify,
    pause_committed_write: AtomicBool,
    committed_write_notify: Notify,
    resume_committed_write_notify: Notify,
}

impl MemoryAuthorityStore {
    fn validate_retirement(
        &self,
        retired: &kuberic_runtime_internal::authority::RetiredAuthority,
    ) -> ContractResult<()> {
        let active = self.authority.lock().unwrap();
        retired.validate(
            active
                .as_ref()
                .map_or(&retired.report.intent.target, |a| &a.local_identity),
        )?;
        if active.as_ref().is_some_and(|a| {
            a.current_configuration != retired.report.intent.previous_configuration
                || a.previous_configuration.is_some()
        }) || self
            .retirement_started
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|old| old != retired)
            || self
                .retired_authority
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|old| old != retired)
        {
            return Err(ContractError::AuthorityMismatch(
                "conflicting retirement".into(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl ReplicaAuthorityStore for MemoryAuthorityStore {
    async fn load_secondary_removal_commit(
        &self,
    ) -> ContractResult<Option<kuberic_protocol::types::SecondaryScaleDownCleanup>> {
        Ok(self.accepted_removal.lock().unwrap().clone())
    }

    async fn record_secondary_removal_commit(
        &self,
        committed: &kuberic_protocol::types::SecondaryScaleDownCleanup,
    ) -> ContractResult<()> {
        *self.accepted_removal.lock().unwrap() = Some(committed.clone());
        Ok(())
    }

    async fn load_secondary_removal(
        &self,
    ) -> ContractResult<Option<kuberic_protocol::types::SecondaryRemovalPreparation>> {
        Ok(self.prepared_secondary_removal.lock().unwrap().clone())
    }

    async fn record_secondary_removal(
        &self,
        preparation: &kuberic_protocol::types::SecondaryRemovalPreparation,
    ) -> ContractResult<()> {
        if self.fail_preparation_once.swap(false, Ordering::SeqCst) {
            return Err(ContractError::Persistence(
                "injected preparation failure".into(),
            ));
        }
        let mut stored = self.prepared_secondary_removal.lock().unwrap();
        if stored.as_ref().is_some_and(|existing| {
            existing != preparation
                && (existing.intent == preparation.intent
                    || preparation.intent.previous_configuration.epoch
                        < existing.intent.current_configuration.epoch)
        }) {
            return Err(ContractError::AuthorityMismatch(
                "conflicting preparation".into(),
            ));
        }
        *stored = Some(preparation.clone());
        if self
            .fail_after_preparation_once
            .swap(false, Ordering::SeqCst)
        {
            return Err(ContractError::Persistence("ambiguous preparation".into()));
        }
        Ok(())
    }

    async fn load_retired_authority(
        &self,
    ) -> ContractResult<Option<kuberic_runtime_internal::authority::RetiredAuthority>> {
        Ok(self.retired_authority.lock().unwrap().clone())
    }

    async fn load_retirement_started(
        &self,
    ) -> ContractResult<Option<kuberic_runtime_internal::authority::RetiredAuthority>> {
        let _guard = self.lifecycle.lock().unwrap();
        Ok(self.retirement_started.lock().unwrap().clone())
    }

    async fn record_retirement_started(
        &self,
        retired: &kuberic_runtime_internal::authority::RetiredAuthority,
    ) -> ContractResult<()> {
        let _guard = self.lifecycle.lock().unwrap();
        self.validate_retirement(retired)?;
        if self.retired_authority.lock().unwrap().is_none() {
            *self.retirement_started.lock().unwrap() = Some(retired.clone());
        }
        Ok(())
    }

    async fn retire(
        &self,
        retired: &kuberic_runtime_internal::authority::RetiredAuthority,
    ) -> ContractResult<()> {
        let _guard = self.lifecycle.lock().unwrap();
        self.validate_retirement(retired)?;
        if self.retired_authority.lock().unwrap().is_some() {
            return Ok(());
        }
        if self.fail_retirement_once.swap(false, Ordering::SeqCst) {
            return Err(ContractError::Persistence(
                "injected tombstone failure".into(),
            ));
        }
        let mut stored = self.retired_authority.lock().unwrap();
        if stored.as_ref().is_some_and(|existing| existing != retired) {
            return Err(ContractError::AuthorityMismatch(
                "conflicting retirement".into(),
            ));
        }
        *stored = Some(retired.clone());
        *self.authority.lock().unwrap() = None;
        *self.retirement_started.lock().unwrap() = None;
        if self
            .fail_after_retirement_once
            .swap(false, Ordering::SeqCst)
        {
            return Err(ContractError::Persistence(
                "ambiguous tombstone persistence".into(),
            ));
        }
        Ok(())
    }

    async fn load(&self) -> ContractResult<Option<AdmittedAuthority>> {
        let _guard = self.lifecycle.lock().unwrap();
        Ok(self.authority.lock().unwrap().clone())
    }

    async fn admit(&self, authority: &AdmittedAuthority) -> ContractResult<()> {
        let _guard = self.lifecycle.lock().unwrap();
        if self.retired_authority.lock().unwrap().is_some()
            || self.retirement_started.lock().unwrap().is_some()
        {
            return Err(ContractError::AuthorityMismatch("retired identity".into()));
        }
        self.admit_count.fetch_add(1, Ordering::SeqCst);
        *self.authority.lock().unwrap() = Some(authority.clone());
        if self.fail_after_admit.swap(false, Ordering::SeqCst) {
            return Err(ContractError::Persistence(
                "injected ambiguous authority admission".into(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl ReplicationProgressStore for MemoryAuthorityStore {
    async fn load_replication_progress(
        &self,
        fence: &AuthorityFence,
    ) -> ContractResult<Option<ReplicationProgress>> {
        Ok(self
            .replication_progress
            .lock()
            .unwrap()
            .get(fence)
            .cloned())
    }

    async fn load_configuration_progress(
        &self,
        epoch: Epoch,
        current_configuration_id: &kuberic_protocol::types::ConfigurationId,
    ) -> ContractResult<Option<ReplicationProgress>> {
        Ok(self
            .replication_progress
            .lock()
            .unwrap()
            .values()
            .filter(|progress| {
                progress.fence.epoch == epoch
                    && &progress.fence.current_configuration_id == current_configuration_id
            })
            .max_by_key(|progress| progress.verified_lsn)
            .cloned())
    }

    async fn record_replication_progress(
        &self,
        progress: &ReplicationProgress,
    ) -> ContractResult<()> {
        self.replication_progress
            .lock()
            .unwrap()
            .insert(progress.fence.clone(), progress.clone());
        Ok(())
    }
}

#[async_trait]
impl LocalWriteJournal for MemoryAuthorityStore {
    async fn load_local_write(
        &self,
        operation_id: &OperationId,
    ) -> ContractResult<Option<DurableLocalWrite>> {
        Ok(self.local_writes.lock().unwrap().get(operation_id).cloned())
    }

    async fn load_local_writes(&self) -> ContractResult<Vec<DurableLocalWrite>> {
        Ok(self
            .local_writes
            .lock()
            .unwrap()
            .values()
            .filter(|write| {
                write.phase != kuberic_runtime_internal::authority::LocalWritePhase::Committed
            })
            .cloned()
            .collect())
    }

    async fn record_local_write(&self, write: &DurableLocalWrite) -> ContractResult<()> {
        if write.phase == kuberic_runtime_internal::authority::LocalWritePhase::Registered
            && self
                .fail_registered_write_once
                .swap(false, Ordering::SeqCst)
        {
            return Err(ContractError::Persistence(
                "injected registered-write persistence failure".to_string(),
            ));
        }
        self.local_writes
            .lock()
            .unwrap()
            .insert(write.operation_id.clone(), write.clone());
        if write.phase == kuberic_runtime_internal::authority::LocalWritePhase::Registered
            && self.pause_registered_write.load(Ordering::SeqCst)
        {
            self.registered_write_notify.notify_one();
            self.resume_registered_write_notify.notified().await;
        }
        if write.phase == kuberic_runtime_internal::authority::LocalWritePhase::Committed
            && self.pause_committed_write.load(Ordering::SeqCst)
        {
            self.committed_write_notify.notify_one();
            self.resume_committed_write_notify.notified().await;
        }
        Ok(())
    }

    async fn reset_local_writes_after_data_loss(&self, committed_lsn: i64) -> ContractResult<()> {
        self.local_writes.lock().unwrap().retain(|_, write| {
            write.phase == kuberic_runtime_internal::authority::LocalWritePhase::Committed
                && write.lsn <= committed_lsn
        });
        Ok(())
    }
}

#[async_trait]
impl BuildAuthorityStore for MemoryAuthorityStore {
    async fn load_build(&self, build_id: &OperationId) -> ContractResult<Option<BuildAuthority>> {
        Ok(self.builds.lock().unwrap().get(build_id).cloned())
    }

    async fn admit_build(&self, authority: &BuildAuthority) -> ContractResult<()> {
        let mut builds = self.builds.lock().unwrap();
        if let Some(existing) = builds.get(&authority.build_id)
            && existing != authority
        {
            return Err(ContractError::AuthorityMismatch(
                "test store rejected conflicting build authority".to_string(),
            ));
        }
        builds.insert(authority.build_id.clone(), authority.clone());
        Ok(())
    }
}

#[async_trait]
impl BuildProgressStore for MemoryAuthorityStore {
    async fn load_build_progress(
        &self,
        build_id: &OperationId,
    ) -> ContractResult<Option<DurableBuildProgress>> {
        Ok(self.build_progress.lock().unwrap().get(build_id).cloned())
    }

    async fn record_build_progress(&self, progress: &DurableBuildProgress) -> ContractResult<()> {
        if self.fail_build_progress_once.swap(false, Ordering::SeqCst) {
            return Err(ContractError::Persistence(
                "injected build progress failure".to_string(),
            ));
        }
        if self.pause_build_progress.load(Ordering::SeqCst) {
            self.build_progress_notify.notify_one();
            self.resume_build_progress_notify.notified().await;
        }
        let mut builds = self.build_progress.lock().unwrap();
        if builds
            .get(&progress.authority.build_id)
            .is_some_and(|existing| {
                progress.last_sequence < existing.last_sequence
                    || progress.durable_lsn < existing.durable_lsn
                    || (existing.completed && !progress.completed)
                    || existing.catch_up_boundary_lsn.is_some()
                        && progress.catch_up_boundary_lsn != existing.catch_up_boundary_lsn
            })
        {
            return Err(ContractError::AuthorityMismatch(
                "test store rejected regressing build progress".into(),
            ));
        }
        builds.insert(progress.authority.build_id.clone(), progress.clone());
        Ok(())
    }
}

#[derive(Default)]
struct TestApplication {
    disk_path: Option<std::path::PathBuf>,
    copy_boundaries: Mutex<Vec<i64>>,
    primary_progress: Mutex<Vec<DurableApplicationProgress>>,
    partition: Mutex<Option<StatefulServicePartition>>,
    factory: Mutex<Option<Arc<dyn ReplicatorFactory>>>,
    state_replicator: Mutex<Option<Arc<dyn StateReplicator>>>,
    returned_control: Mutex<Option<Arc<dyn Replicator>>>,
    streams_taken: AtomicUsize,
    manual_streams: AtomicBool,
    held_streams: Mutex<Vec<OperationStream>>,
    settings: Mutex<Option<ReplicatorSettings>>,
    pause_open: AtomicBool,
    report_fault_on_open: AtomicBool,
    fail_open: AtomicBool,
    open_notify: Notify,
    resume_open_notify: Notify,
    applied: Mutex<BTreeMap<i64, Operation>>,
    progress: Mutex<DurableApplicationProgress>,
    copy_chunks: Mutex<BTreeMap<(String, u64), Bytes>>,
    copy_context_items: Mutex<Vec<Bytes>>,
    copy_apply_count: AtomicUsize,
    pause_copy_enumeration: Arc<AtomicBool>,
    copy_enumeration_notify: Arc<Notify>,
    resume_copy_enumeration_notify: Arc<Notify>,
    pause_copy_completion: Arc<AtomicBool>,
    copy_completion_notify: Arc<Notify>,
    resume_copy_completion_notify: Arc<Notify>,
    pause_retained_enumeration: Arc<AtomicBool>,
    retained_enumeration_notify: Arc<Notify>,
    resume_retained_enumeration_notify: Arc<Notify>,
    fail_apply: AtomicBool,
    fail_after_apply: AtomicBool,
    pause_after_apply: AtomicBool,
    applied_notify: Notify,
    resume_notify: Notify,
    fail_commit: AtomicBool,
    pause_commit: AtomicBool,
    commit_notify: Notify,
    resume_commit_notify: Notify,
    events: Arc<Mutex<Vec<String>>>,
    pause_close: AtomicBool,
    fail_close: AtomicBool,
    fail_change_role: AtomicBool,
    close_notify: Notify,
    resume_close_notify: Notify,
    fail_update_epoch: AtomicBool,
    pause_update_epoch: AtomicBool,
    update_epoch_notify: Notify,
    resume_update_epoch_notify: Notify,
}

impl TestApplication {
    fn reopen(path: std::path::PathBuf) -> Self {
        let application = Self {
            disk_path: Some(path.clone()),
            ..Self::default()
        };
        if path.exists() {
            type DiskState = (PersistedApplicationState, Vec<(String, u64, Vec<u8>)>);
            let ((operations, applied, committed), chunks): DiskState =
                serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            *application.applied.lock().unwrap() = operations
                .into_iter()
                .map(|(lsn, committed_lsn, data)| {
                    (
                        lsn,
                        Operation {
                            lsn,
                            committed_lsn,
                            data: data.into(),
                        },
                    )
                })
                .collect();
            *application.progress.lock().unwrap() = DurableApplicationProgress {
                applied_lsn: applied,
                committed_lsn: committed,
            };
            *application.copy_chunks.lock().unwrap() = chunks
                .into_iter()
                .map(|(id, sequence, bytes)| ((id, sequence), bytes.into()))
                .collect();
        }
        application
    }

    fn persist(&self) {
        let Some(path) = &self.disk_path else {
            return;
        };
        use std::io::Write;
        let progress = *self.progress.lock().unwrap();
        let operations: Vec<_> = self
            .applied
            .lock()
            .unwrap()
            .values()
            .map(|op| (op.lsn, op.committed_lsn, op.data.to_vec()))
            .collect();
        let chunks: Vec<_> = self
            .copy_chunks
            .lock()
            .unwrap()
            .iter()
            .map(|((id, sequence), bytes)| (id.clone(), *sequence, bytes.to_vec()))
            .collect();
        let bytes = serde_json::to_vec(&(
            (operations, progress.applied_lsn, progress.committed_lsn),
            chunks,
        ))
        .unwrap();
        let tmp = path.with_extension("tmp");
        let mut file = std::fs::File::create(&tmp).unwrap();
        file.write_all(&bytes).unwrap();
        file.sync_all().unwrap();
        std::fs::rename(tmp, path).unwrap();
        std::fs::File::open(path.parent().unwrap())
            .unwrap()
            .sync_all()
            .unwrap();
    }

    fn seed_progress(&self, applied_lsn: i64) {
        let mut progress = self.progress.lock().unwrap();
        let mut operations = self.applied.lock().unwrap();
        for lsn in (progress.applied_lsn + 1)..=applied_lsn {
            operations.insert(
                lsn,
                Operation {
                    lsn,
                    committed_lsn: lsn,
                    data: Bytes::from(format!("seed-{lsn}")),
                },
            );
        }
        progress.applied_lsn = applied_lsn;
        progress.committed_lsn = progress.committed_lsn.max(applied_lsn);
    }

    fn seed_operation(&self, lsn: i64, data: Bytes) {
        let mut progress = self.progress.lock().unwrap();
        let mut operations = self.applied.lock().unwrap();
        assert_eq!(lsn, progress.applied_lsn + 1);
        operations.insert(
            lsn,
            Operation {
                lsn,
                committed_lsn: lsn,
                data,
            },
        );
        progress.applied_lsn = lsn;
        progress.committed_lsn = lsn;
    }
}

#[derive(Default)]
struct TestControlPlane {
    effects: VecDeque<RuntimeEffect>,
    published: Vec<RuntimeEffectResult>,
}

#[derive(Clone)]
struct CountingFactory {
    storage: std::sync::Weak<TestApplication>,
    opened: Arc<AtomicUsize>,
    role_changes: Arc<AtomicUsize>,
    epoch_updates: Arc<AtomicUsize>,
    events: Arc<Mutex<Vec<String>>>,
    fail_change_role: Arc<AtomicBool>,
    fail_close: Arc<AtomicBool>,
}

struct CountingReplicator {
    inner: Arc<dyn Replicator>,
    primary: Arc<dyn PrimaryReplicator>,
    counts: CountingFactory,
}

struct PausingFactory {
    storage: std::sync::Weak<TestApplication>,
    captured: Arc<Mutex<Option<Arc<dyn Replicator>>>>,
    created: Arc<Notify>,
    resume: Arc<Notify>,
}

#[async_trait]
impl ReplicatorFactory for PausingFactory {
    async fn create_replicator(
        &self,
        context: ReplicatorFactoryContext,
        provider: Arc<dyn StateProvider>,
        settings: ReplicatorSettings,
    ) -> Result<ReplicatorInterfaces> {
        let interfaces =
            DefaultReplicatorFactory::new(self.storage.upgrade().ok_or(RuntimeError::Closed)?)
                .create_replicator(context, provider, settings)
                .await?;
        *self.captured.lock().unwrap() = Some(interfaces.replicator());
        self.created.notify_one();
        self.resume.notified().await;
        Ok(interfaces)
    }
}

#[async_trait]
impl ReplicatorFactory for CountingFactory {
    async fn create_replicator(
        &self,
        context: ReplicatorFactoryContext,
        provider: Arc<dyn StateProvider>,
        settings: ReplicatorSettings,
    ) -> Result<ReplicatorInterfaces> {
        let interfaces =
            DefaultReplicatorFactory::new(self.storage.upgrade().ok_or(RuntimeError::Closed)?)
                .create_replicator(context, provider, settings)
                .await?;
        let state_replicator = interfaces.state_replicator();
        let replicator = Arc::new(CountingReplicator {
            inner: interfaces.replicator(),
            primary: interfaces
                .primary_replicator()
                .ok_or(RuntimeError::NotPrimary)?,
            counts: self.clone(),
        });
        Ok(ReplicatorInterfaces::primary(replicator, state_replicator))
    }
}

fn runtime_with_factory(
    identity: ReplicaIdentity,
    application: Arc<TestApplication>,
    store: Arc<MemoryAuthorityStore>,
    factory: CountingFactory,
) -> Result<PodRuntime> {
    *application.factory.lock().unwrap() = Some(Arc::new(factory));
    Ok(PodRuntime::new(identity, application, store))
}

#[async_trait]
impl Replicator for CountingReplicator {
    async fn open(&self) -> Result<String> {
        self.counts
            .events
            .lock()
            .unwrap()
            .push("replicator.open".to_string());
        self.counts.opened.fetch_add(1, Ordering::SeqCst);
        self.inner.open().await
    }

    async fn change_role(&self, epoch: Epoch, role: ReplicaRole) -> Result<()> {
        self.counts
            .events
            .lock()
            .unwrap()
            .push("replicator.change_role".to_string());
        self.counts.role_changes.fetch_add(1, Ordering::SeqCst);
        if self.counts.fail_change_role.swap(false, Ordering::SeqCst) {
            return Err(RuntimeError::Application(
                "injected replicator role failure".to_string(),
            ));
        }
        self.inner.change_role(epoch, role).await
    }

    async fn update_epoch(&self, epoch: Epoch) -> Result<()> {
        self.counts
            .events
            .lock()
            .unwrap()
            .push("replicator.update_epoch".to_string());
        self.counts.epoch_updates.fetch_add(1, Ordering::SeqCst);
        self.inner.update_epoch(epoch).await
    }

    async fn current_progress(&self) -> Result<i64> {
        self.inner.current_progress().await
    }

    async fn catch_up_capability(&self) -> Result<i64> {
        self.inner.catch_up_capability().await
    }

    async fn close(&self) -> Result<()> {
        self.counts
            .events
            .lock()
            .unwrap()
            .push("replicator.close".to_string());
        if self.counts.fail_close.swap(false, Ordering::SeqCst) {
            return Err(RuntimeError::Application(
                "injected replicator close failure".to_string(),
            ));
        }
        self.inner.close().await
    }

    fn abort(&self) {
        self.counts
            .events
            .lock()
            .unwrap()
            .push("replicator.abort".to_string());
        self.inner.abort();
    }
}

#[async_trait]
impl PrimaryReplicator for CountingReplicator {
    async fn on_data_loss(&self) -> Result<bool> {
        self.primary.on_data_loss().await
    }

    async fn update_catch_up_replica_set_configuration(
        &self,
        current: ConfigurationDescriptor,
        previous: ConfigurationDescriptor,
    ) -> Result<()> {
        self.primary
            .update_catch_up_replica_set_configuration(current, previous)
            .await
    }

    async fn wait_for_catch_up_quorum(&self, mode: ReplicaSetQuorumMode) -> Result<()> {
        self.primary.wait_for_catch_up_quorum(mode).await
    }

    async fn update_current_replica_set_configuration(
        &self,
        current: ConfigurationDescriptor,
    ) -> Result<()> {
        self.primary
            .update_current_replica_set_configuration(current)
            .await
    }

    async fn build_replica(&self, replica: ReplicaInformation) -> Result<()> {
        self.primary.build_replica(replica).await
    }

    async fn remove_replica(&self, replica_id: ReplicaId) -> Result<()> {
        self.primary.remove_replica(replica_id).await
    }
}

#[async_trait]
impl RuntimeControlPlane for TestControlPlane {
    async fn next_effect(&mut self) -> Result<Option<RuntimeEffect>> {
        Ok(self.effects.pop_front())
    }

    async fn publish(&mut self, result: RuntimeEffectResult) -> Result<()> {
        self.published.push(result);
        Ok(())
    }
}

#[async_trait]
impl StatefulServiceReplica for TestApplication {
    async fn open(self: Arc<Self>, context: OpenContext) -> Result<Arc<dyn Replicator>> {
        self.events.lock().unwrap().push("service.open".to_string());
        if self.report_fault_on_open.load(Ordering::SeqCst) {
            context.partition.report_fault(FaultType::Permanent).await?;
        }
        *self.partition.lock().unwrap() = Some(context.partition.clone());
        assert_eq!(
            context.partition.get_write_status().await?,
            AccessStatus::NotPrimary
        );
        let factory = self.factory.lock().unwrap().clone();
        let factory =
            factory.unwrap_or_else(|| Arc::new(DefaultReplicatorFactory::new(self.clone())));
        let partition = context.partition.with_factory(factory);
        let settings = self.settings.lock().unwrap().clone();
        let interfaces = partition.create_replicator(self.clone(), settings).await?;
        let state_replicator = interfaces.state_replicator();
        let replication = state_replicator.get_replication_stream().await?;
        let copy = state_replicator.get_copy_stream().await?;
        self.streams_taken.fetch_add(2, Ordering::SeqCst);
        for stream in [replication, copy] {
            if self.manual_streams.load(Ordering::SeqCst) {
                self.held_streams.lock().unwrap().push(stream);
            } else {
                tokio::spawn(consume_stream(Arc::downgrade(&self), stream));
            }
        }
        *self.state_replicator.lock().unwrap() = Some(state_replicator);
        *self.returned_control.lock().unwrap() = Some(interfaces.replicator());
        if self.pause_open.load(Ordering::SeqCst) {
            self.open_notify.notify_one();
            self.resume_open_notify.notified().await;
        }
        if self.fail_open.load(Ordering::SeqCst) {
            return Err(RuntimeError::Application("injected Open failure".into()));
        }
        Ok(interfaces.replicator())
    }

    async fn change_role(&self, role: ReplicaRole) -> Result<RoleChange> {
        if role == ReplicaRole::Primary {
            self.primary_progress
                .lock()
                .unwrap()
                .push(*self.progress.lock().unwrap());
        }
        self.events
            .lock()
            .unwrap()
            .push("service.change_role".to_string());
        if self.fail_change_role.swap(false, Ordering::SeqCst) {
            return Err(RuntimeError::Application(
                "injected role callback failure".to_string(),
            ));
        }
        Ok(RoleChange {
            service_address: None,
        })
    }

    async fn close(&self) -> Result<()> {
        self.events
            .lock()
            .unwrap()
            .push("service.close".to_string());
        if self.pause_close.load(Ordering::SeqCst) {
            self.close_notify.notify_one();
            self.resume_close_notify.notified().await;
        }
        if self.fail_close.swap(false, Ordering::SeqCst) {
            return Err(RuntimeError::Application(
                "injected close failure".to_string(),
            ));
        }
        Ok(())
    }

    fn abort(&self) {
        self.events
            .lock()
            .unwrap()
            .push("service.abort".to_string());
    }
}

#[tokio::test]
async fn application_can_report_a_rebuild_fault_during_open_without_reentering_effect_lock() {
    let application = Arc::new(TestApplication::default());
    application
        .report_fault_on_open
        .store(true, Ordering::SeqCst);
    let runtime = PodRuntime::new(
        identity(1, "open-fault"),
        application,
        Arc::new(MemoryAuthorityStore::default()),
    );
    timeout(
        Duration::from_secs(2),
        runtime.apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing))),
    )
    .await
    .expect("fault reporting must not deadlock Open")
    .unwrap();
    assert_eq!(
        runtime.partition_report().await.reported_fault,
        Some(FaultType::Permanent)
    );
}

#[async_trait]
impl StateProvider for TestApplication {
    async fn update_epoch(&self, _epoch: Epoch, _previous_epoch_last_lsn: i64) -> Result<()> {
        self.events
            .lock()
            .unwrap()
            .push("provider.update_epoch".to_string());
        if self.pause_update_epoch.load(Ordering::SeqCst) {
            self.update_epoch_notify.notify_one();
            self.resume_update_epoch_notify.notified().await;
        }
        if self.fail_update_epoch.swap(false, Ordering::SeqCst) {
            return Err(RuntimeError::Application(
                "injected epoch update failure".to_string(),
            ));
        }
        Ok(())
    }

    async fn last_committed_lsn(&self) -> Result<i64> {
        Ok(self.progress.lock().unwrap().committed_lsn)
    }

    async fn get_copy_context(&self) -> Result<OperationDataStream> {
        Ok(Box::pin(stream::empty()))
    }

    async fn get_copy_state(
        &self,
        up_to_lsn: i64,
        mut copy_context: OperationDataStream,
    ) -> Result<OperationDataStream> {
        self.copy_boundaries.lock().unwrap().push(up_to_lsn);
        let mut context_items = Vec::new();
        while let Some(item) = copy_context.next().await {
            context_items.push(item?);
        }
        self.copy_context_items
            .lock()
            .unwrap()
            .extend(context_items);
        let mut snapshot = Vec::new();
        for operation in self
            .applied
            .lock()
            .unwrap()
            .range(..=up_to_lsn)
            .map(|(_, operation)| operation)
        {
            snapshot.extend_from_slice(&operation.lsn.to_be_bytes());
            snapshot.extend_from_slice(&operation.committed_lsn.to_be_bytes());
            snapshot.extend_from_slice(&(operation.data.len() as u64).to_be_bytes());
            snapshot.extend_from_slice(&operation.data);
        }
        let chunks = if snapshot.is_empty() {
            Vec::new()
        } else {
            let split = snapshot.len().div_ceil(2);
            vec![
                Ok(Bytes::copy_from_slice(&snapshot[..split])),
                Ok(Bytes::copy_from_slice(&snapshot[split..])),
            ]
        };
        let pause = self.pause_copy_enumeration.clone();
        let paused = self.copy_enumeration_notify.clone();
        let resume = self.resume_copy_enumeration_notify.clone();
        let pause_completion = self.pause_copy_completion.clone();
        let completion_paused = self.copy_completion_notify.clone();
        let resume_completion = self.resume_copy_completion_notify.clone();
        Ok(Box::pin(stream::unfold(
            (chunks.into_iter(), true),
            move |(mut chunks, first)| {
                let pause = pause.clone();
                let paused = paused.clone();
                let resume = resume.clone();
                let pause_completion = pause_completion.clone();
                let completion_paused = completion_paused.clone();
                let resume_completion = resume_completion.clone();
                async move {
                    if first && pause.load(Ordering::SeqCst) {
                        paused.notify_one();
                        resume.notified().await;
                    }
                    if let Some(chunk) = std::iter::Iterator::next(&mut chunks) {
                        return Some((chunk, (chunks, false)));
                    }
                    if pause_completion.load(Ordering::SeqCst) {
                        completion_paused.notify_one();
                        resume_completion.notified().await;
                    }
                    None
                }
            },
        )))
    }

    async fn on_data_loss(&self) -> Result<bool> {
        self.events
            .lock()
            .unwrap()
            .push("provider.on_data_loss".into());
        Ok(false)
    }
}

#[async_trait]
impl DurableState for TestApplication {
    async fn get_replication_operations(
        &self,
        from_lsn: i64,
        to_lsn: i64,
    ) -> Result<RetainedOperationStream> {
        if from_lsn > to_lsn {
            return Ok(Box::pin(stream::empty()));
        }
        let operations = self
            .applied
            .lock()
            .unwrap()
            .range(from_lsn..=to_lsn)
            .map(|(_, operation)| Ok(operation.clone()))
            .collect::<Vec<_>>();
        let pause = self.pause_retained_enumeration.clone();
        let paused = self.retained_enumeration_notify.clone();
        let resume = self.resume_retained_enumeration_notify.clone();
        Ok(Box::pin(stream::unfold(
            (operations.into_iter(), true),
            move |(mut operations, first)| {
                let pause = pause.clone();
                let paused = paused.clone();
                let resume = resume.clone();
                async move {
                    if first && pause.load(Ordering::SeqCst) {
                        paused.notify_one();
                        resume.notified().await;
                    }
                    std::iter::Iterator::next(&mut operations)
                        .map(|operation| (operation, (operations, false)))
                }
            },
        )))
    }

    async fn apply_copy_chunk(
        &self,
        build_id: &OperationId,
        sequence: u64,
        chunk: CopyChunk,
    ) -> Result<()> {
        self.copy_apply_count.fetch_add(1, Ordering::SeqCst);
        let key = (build_id.to_string(), sequence);
        let mut chunks = self.copy_chunks.lock().unwrap();
        if let Some(existing) = chunks.get(&key)
            && existing != &chunk.data
        {
            return Err(RuntimeError::Application(
                "copy chunk retry changed contents".to_string(),
            ));
        }
        chunks.insert(key, chunk.data);
        drop(chunks);
        self.persist();
        Ok(())
    }

    async fn verify_copy_chunk(
        &self,
        build_id: &OperationId,
        sequence: u64,
        chunk: &CopyChunk,
    ) -> Result<bool> {
        Ok(self
            .copy_chunks
            .lock()
            .unwrap()
            .get(&(build_id.to_string(), sequence))
            .is_some_and(|stored| stored == &chunk.data))
    }

    async fn finish_copy(
        &self,
        build_id: &OperationId,
        up_to_lsn: i64,
        committed_lsn: i64,
    ) -> Result<DurableApplicationProgress> {
        let prefix = build_id.to_string();
        let chunks = self.copy_chunks.lock().unwrap();
        let mut copied = chunks
            .iter()
            .filter(|((id, _), _)| id == &prefix)
            .map(|((_, sequence), data)| (*sequence, data.clone()))
            .collect::<Vec<_>>();
        copied.sort_by_key(|(sequence, _)| *sequence);
        drop(chunks);
        let mut snapshot = Vec::new();
        for (_, chunk) in copied {
            snapshot.extend_from_slice(&chunk);
        }
        let mut operations = self.applied.lock().unwrap();
        let mut offset = 0;
        while offset < snapshot.len() {
            if snapshot.len() - offset < 24 {
                return Err(RuntimeError::Application(
                    "copy chunk is malformed".to_string(),
                ));
            }
            let lsn = i64::from_be_bytes(snapshot[offset..offset + 8].try_into().unwrap());
            let operation_committed_lsn =
                i64::from_be_bytes(snapshot[offset + 8..offset + 16].try_into().unwrap());
            let data_len =
                u64::from_be_bytes(snapshot[offset + 16..offset + 24].try_into().unwrap()) as usize;
            offset += 24;
            if snapshot.len() - offset < data_len {
                return Err(RuntimeError::Application(
                    "copy chunk data is truncated".to_string(),
                ));
            }
            operations.insert(
                lsn,
                Operation {
                    lsn,
                    committed_lsn: operation_committed_lsn,
                    data: Bytes::copy_from_slice(&snapshot[offset..offset + data_len]),
                },
            );
            offset += data_len;
        }
        drop(operations);
        let mut progress = self.progress.lock().unwrap();
        progress.applied_lsn = progress.applied_lsn.max(up_to_lsn);
        progress.committed_lsn = progress.committed_lsn.max(committed_lsn);
        let result = *progress;
        drop(progress);
        self.persist();
        Ok(result)
    }

    async fn apply(&self, operation: Operation) -> Result<DurableApplicationAck> {
        if self.fail_apply.load(Ordering::SeqCst) {
            return Err(RuntimeError::Application(
                "injected durable apply failure".to_string(),
            ));
        }
        let result = {
            let mut progress = self.progress.lock().unwrap();
            if operation.lsn != progress.applied_lsn + 1 {
                return Err(RuntimeError::Application(
                    "test application observed a replication gap".to_string(),
                ));
            }
            progress.applied_lsn = operation.lsn;
            progress.committed_lsn = progress.committed_lsn.max(operation.committed_lsn);
            self.applied
                .lock()
                .unwrap()
                .insert(operation.lsn, operation);
            *progress
        };
        self.persist();
        if self.pause_after_apply.load(Ordering::SeqCst) {
            self.applied_notify.notify_one();
            self.resume_notify.notified().await;
        }
        if self.fail_after_apply.swap(false, Ordering::SeqCst) {
            return Err(RuntimeError::Application(
                "injected ambiguous apply failure".to_string(),
            ));
        }
        Ok(result)
    }

    async fn durable_progress(&self) -> Result<DurableApplicationProgress> {
        Ok(*self.progress.lock().unwrap())
    }

    async fn verify_applied(&self, operation: &Operation) -> Result<bool> {
        Ok(self
            .applied
            .lock()
            .unwrap()
            .get(&operation.lsn)
            .is_some_and(|applied| applied.data == operation.data))
    }

    async fn commit(&self, committed_lsn: i64) -> Result<DurableApplicationProgress> {
        let result = {
            let mut progress = self.progress.lock().unwrap();
            if committed_lsn > progress.applied_lsn {
                return Err(RuntimeError::Application(
                    "commit exceeds applied progress".to_string(),
                ));
            }
            progress.committed_lsn = progress.committed_lsn.max(committed_lsn);
            *progress
        };
        self.persist();
        if self.pause_commit.load(Ordering::SeqCst) {
            self.commit_notify.notify_one();
            self.resume_commit_notify.notified().await;
        }
        if self.fail_commit.swap(false, Ordering::SeqCst) {
            return Err(RuntimeError::Application(
                "injected commit failure".to_string(),
            ));
        }
        Ok(result)
    }
}

async fn consume_stream(
    application: std::sync::Weak<TestApplication>,
    mut stream: OperationStream,
) {
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
}

fn empty_copy_context() -> OperationDataStream {
    Box::pin(stream::empty())
}

fn fresh_disk_store(root: &Path, local: ReplicaIdentity) -> Arc<SqliteStore> {
    Arc::new(
        SqliteStore::create_authorized(
            SqliteStore::metadata_database_path(root),
            AgentState::new(StorageIdentity {
                schema_version: SCHEMA_VERSION,
                resource_uid: ResourceUid::new("frozen-copy"),
                pod_uid: PodUid::new(local.instance_id.as_str()),
                pvc_uid: PvcUid::new(format!("pvc-{}", local.replica_id.value())),
                initialization_id: InitializationId::new(format!(
                    "init-{}",
                    local.replica_id.value()
                )),
                local_identity: local,
                effective_policy: EffectivePolicy::fixed(1, 30).unwrap(),
            }),
        )
        .unwrap(),
    )
}

async fn activate_test_primary(runtime: &PodRuntime, admitted: AdmittedAuthority, grant: bool) {
    for (index, action) in [
        RuntimeEffectAction::Open(OpenMode::Existing),
        RuntimeEffectAction::AdmitAuthority(Box::new(admitted)),
        RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
    ]
    .into_iter()
    .enumerate()
    {
        runtime
            .apply_effect(effect(index as u64 + 1, action))
            .await
            .unwrap();
    }
    if grant {
        runtime
            .apply_effect(effect(
                4,
                RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
            ))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn committed_snapshot_boundary_survives_disk_reopens_with_exact_applied_catchup() {
    for recover_during_snapshot in [false, true] {
        committed_snapshot_restart_trace(recover_during_snapshot).await;
    }
}

async fn committed_snapshot_restart_trace(recover_during_snapshot: bool) {
    let scratch = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/copy-unit");
    std::fs::create_dir_all(&scratch).unwrap();
    let dir = tempfile::tempdir_in(&scratch).unwrap();
    let source_root = dir.path().join("source");
    let target_root = dir.path().join("target");
    std::fs::create_dir_all(&source_root).unwrap();
    std::fs::create_dir_all(&target_root).unwrap();
    let source = identity(1, "frozen-source");
    let target = identity(1, "frozen-replacement");
    let source_store = fresh_disk_store(&source_root, source.clone());
    let target_store = fresh_disk_store(&target_root, target.clone());
    let source_app = Arc::new(TestApplication::reopen(
        source_root.join("application.json"),
    ));
    let target_app = Arc::new(TestApplication::reopen(
        target_root.join("application.json"),
    ));
    let source_runtime = PodRuntime::new(source.clone(), source_app.clone(), source_store.clone());
    let admitted = authority(source.clone(), vec![source.clone()]);
    activate_test_primary(&source_runtime, admitted.clone(), true).await;
    for lsn in 1..=9 {
        source_runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new(format!("write-{lsn}")),
                data: Bytes::from(format!("value-{lsn}")),
            })
            .await
            .unwrap()
            .committed()
            .await
            .unwrap();
    }
    let tenth = ClientWrite {
        operation_id: OperationId::new("tenth"),
        data: Bytes::from_static(b"tenth"),
    };
    source_app.fail_after_apply.store(true, Ordering::SeqCst);
    assert!(
        source_runtime
            .data_plane()
            .begin_write(tenth.clone())
            .await
            .is_err()
    );
    assert_eq!(
        source_app.durable_progress().await.unwrap(),
        DurableApplicationProgress {
            applied_lsn: 10,
            committed_lsn: 9
        }
    );
    let build_id = OperationId::new("frozen-ten-nine");
    let uncommitted_build_id = OperationId::new("reject-applied-boundary");
    source_store
        .admit_build(&BuildAuthority {
            build_id: uncommitted_build_id.clone(),
            kind: BuildAuthorityKind::Provisioning,
            source: source.clone(),
            target: target.clone(),
            current_configuration: admitted.current_configuration.clone(),
            replication_boundary_lsn: 10,
        })
        .await
        .unwrap();
    assert!(
        source_runtime
            .data_plane()
            .prepare_copy(PrepareCopyRequest {
                build_id: uncommitted_build_id,
                target: target.clone(),
                configuration: BuildConfiguration::Current,
                copy_context: empty_copy_context(),
            })
            .await
            .is_err()
    );
    let request = || PrepareCopyRequest {
        build_id: build_id.clone(),
        target: target.clone(),
        configuration: BuildConfiguration::Current,
        copy_context: empty_copy_context(),
    };
    source_app
        .pause_copy_enumeration
        .store(recover_during_snapshot, Ordering::SeqCst);
    let mut prepared = prepare_copy_authorized(&source_runtime, request())
        .await
        .unwrap();
    if recover_during_snapshot {
        source_runtime
            .data_plane()
            .begin_write(tenth.clone())
            .await
            .unwrap()
            .committed()
            .await
            .unwrap();
        source_app
            .pause_copy_enumeration
            .store(false, Ordering::SeqCst);
        source_app.resume_copy_enumeration_notify.notify_one();
    }
    let build = prepared.authority.clone();
    assert_eq!(build.replication_boundary_lsn, 9);
    let original = copy_through_final(&mut prepared).await;
    assert_eq!(original.last().unwrap().committed_lsn, 9);
    assert_eq!(original.last().unwrap().lsn, 9);
    assert_eq!(original.last().unwrap().catch_up_boundary_lsn, Some(10));
    let original_suffix = next_copy_item(&mut prepared).await;
    assert_eq!(original_suffix.lsn, 10);
    assert_eq!(original_suffix.committed_lsn, 9);
    assert_eq!(original_suffix.data, b"tenth");
    let target_runtime = PodRuntime::new(target.clone(), target_app.clone(), target_store.clone());
    for (index, action) in [
        RuntimeEffectAction::Open(OpenMode::Existing),
        RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary),
        RuntimeEffectAction::AdmitBuildAuthority(Box::new(build.clone())),
    ]
    .into_iter()
    .enumerate()
    {
        target_runtime
            .apply_effect(effect(index as u64 + 1, action))
            .await
            .unwrap();
    }
    let ack = target_runtime
        .data_plane()
        .receive_copy_item(original[0].clone())
        .await
        .unwrap();
    let mut wrong_watermark = ack.clone();
    wrong_watermark.replication_boundary_lsn = 10;
    assert!(
        source_runtime
            .data_plane()
            .accept_copy_acknowledgement(wrong_watermark)
            .await
            .is_err()
    );
    source_runtime
        .data_plane()
        .accept_copy_acknowledgement(ack)
        .await
        .unwrap();
    source_runtime
        .data_plane()
        .begin_write(tenth)
        .await
        .unwrap()
        .committed()
        .await
        .unwrap();
    source_runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("eleventh"),
            data: Bytes::from_static(b"eleventh"),
        })
        .await
        .unwrap()
        .committed()
        .await
        .unwrap();
    // Recovery must not emit operation 10 twice under different copy sequences.
    let live = next_copy_item(&mut prepared).await;
    assert_eq!(live.lsn, 11);
    assert_eq!(live.sequence, original_suffix.sequence + 1);
    drop(prepared);
    drop(source_runtime);
    drop(target_runtime);
    drop(source_app);
    drop(target_app);
    drop(source_store);
    drop(target_store);

    // Reopen all application bytes, copy staging and agent journals from disk.
    let source_store = Arc::new(
        SqliteStore::open_existing(SqliteStore::metadata_database_path(&source_root), None)
            .unwrap(),
    );
    let target_store = Arc::new(
        SqliteStore::open_existing(SqliteStore::metadata_database_path(&target_root), None)
            .unwrap(),
    );
    let source_app = Arc::new(TestApplication::reopen(
        source_root.join("application.json"),
    ));
    let target_app = Arc::new(TestApplication::reopen(
        target_root.join("application.json"),
    ));
    let source_runtime = PodRuntime::new(source.clone(), source_app.clone(), source_store);
    activate_test_primary(&source_runtime, admitted, false).await;
    let target_runtime = PodRuntime::new(target.clone(), target_app.clone(), target_store.clone());
    for (index, action) in [
        RuntimeEffectAction::Open(OpenMode::Existing),
        RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary),
        RuntimeEffectAction::AdmitBuildAuthority(Box::new(build.clone())),
    ]
    .into_iter()
    .enumerate()
    {
        target_runtime
            .apply_effect(effect(index as u64 + 1, action))
            .await
            .unwrap();
    }
    let mut resumed = prepare_copy_authorized(&source_runtime, request())
        .await
        .unwrap();
    let replay = copy_through_final(&mut resumed).await;
    assert_eq!(original, replay);
    assert_eq!(source_app.copy_boundaries.lock().unwrap().as_slice(), &[9]);
    for item in &replay {
        let ack = target_runtime
            .data_plane()
            .receive_copy_item(item.clone())
            .await
            .unwrap();
        source_runtime
            .data_plane()
            .accept_copy_acknowledgement(ack)
            .await
            .unwrap();
    }
    let final_item = replay.last().unwrap().clone();
    assert_eq!(
        target_app.durable_progress().await.unwrap(),
        DurableApplicationProgress {
            applied_lsn: 9,
            committed_lsn: 9
        }
    );
    target_runtime
        .data_plane()
        .receive_copy_item(final_item.clone())
        .await
        .unwrap();
    let suffix = next_copy_item(&mut resumed).await;
    assert_eq!(suffix, original_suffix);
    target_runtime
        .data_plane()
        .receive_copy_item(suffix)
        .await
        .unwrap();
    assert_eq!(
        target_app.durable_progress().await.unwrap(),
        DurableApplicationProgress {
            applied_lsn: 10,
            committed_lsn: 9
        }
    );
    let later = next_copy_item(&mut resumed).await;
    assert_eq!(later.lsn, 11);
    assert_eq!(later.committed_lsn, 10);
    assert_eq!(later, live);
    target_runtime
        .data_plane()
        .receive_copy_item(later)
        .await
        .unwrap();
    let before = target_app.durable_progress().await.unwrap();
    target_runtime
        .data_plane()
        .receive_copy_item(final_item.clone())
        .await
        .unwrap();
    let mut changed = final_item.clone();
    changed.committed_lsn = 10;
    assert!(
        target_runtime
            .data_plane()
            .receive_copy_item(changed.clone())
            .await
            .is_err()
    );
    assert_eq!(target_app.durable_progress().await.unwrap(), before);
    drop(target_runtime);
    drop(target_app);
    drop(target_store);
    let target_store = Arc::new(
        SqliteStore::open_existing(SqliteStore::metadata_database_path(&target_root), None)
            .unwrap(),
    );
    let target_app = Arc::new(TestApplication::reopen(
        target_root.join("application.json"),
    ));
    let target_runtime = PodRuntime::new(target.clone(), target_app.clone(), target_store);
    for (index, action) in [
        RuntimeEffectAction::Open(OpenMode::Existing),
        RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary),
        RuntimeEffectAction::AdmitBuildAuthority(Box::new(build)),
    ]
    .into_iter()
    .enumerate()
    {
        target_runtime
            .apply_effect(effect(index as u64 + 1, action))
            .await
            .unwrap();
    }
    target_runtime
        .data_plane()
        .receive_copy_item(final_item)
        .await
        .unwrap();
    assert!(
        target_runtime
            .data_plane()
            .receive_copy_item(changed)
            .await
            .is_err()
    );
    assert_eq!(target_app.durable_progress().await.unwrap(), before);
}

#[tokio::test]
async fn primary_callback_follows_durable_certified_commit_and_not_unverified_suffix() {
    for (verified, fail_commit) in [(9, false), (10, false), (11, false), (10, true)] {
        let source = identity(1, "certified-primary");
        let store = Arc::new(MemoryAuthorityStore::default());
        let app = Arc::new(TestApplication::default());
        for lsn in 1..=10 {
            app.apply(Operation {
                lsn,
                committed_lsn: (lsn - 1).min(9),
                data: Bytes::from(format!("op-{lsn}")),
            })
            .await
            .unwrap();
        }
        let admitted = authority(source.clone(), vec![source.clone()]);
        store.admit(&admitted).await.unwrap();
        store
            .record_replication_progress(&ReplicationProgress {
                fence: admitted.fence(),
                verified_lsn: verified,
            })
            .await
            .unwrap();
        let runtime = PodRuntime::new(source, app.clone(), store);
        runtime
            .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
            .await
            .unwrap();
        runtime
            .apply_effect(effect(
                2,
                RuntimeEffectAction::AdmitAuthority(Box::new(admitted)),
            ))
            .await
            .unwrap();
        app.fail_commit.store(fail_commit, Ordering::SeqCst);
        let result = runtime
            .apply_effect(effect(
                3,
                RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
            ))
            .await;
        assert_ne!(runtime.snapshot().await.write_status, AccessStatus::Granted);
        if verified > 10 || fail_commit {
            assert!(result.is_err());
            assert!(app.primary_progress.lock().unwrap().is_empty());
            assert!(
                runtime
                    .apply_effect(effect(
                        3,
                        RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted)
                    ))
                    .await
                    .is_err()
            );
            assert!(
                runtime
                    .apply_effect(effect(
                        3,
                        RuntimeEffectAction::SetReadStatus(AccessStatus::Granted)
                    ))
                    .await
                    .is_err()
            );
        } else {
            result.unwrap();
            assert_eq!(
                app.primary_progress.lock().unwrap().as_slice(),
                &[DurableApplicationProgress {
                    applied_lsn: 10,
                    committed_lsn: verified
                }]
            );
            assert_eq!(app.applied.lock().unwrap().len(), 10);
            if verified == 10 {
                runtime
                    .apply_effect(effect(
                        4,
                        RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
                    ))
                    .await
                    .unwrap();
                runtime
                    .data_plane()
                    .begin_write(ClientWrite {
                        operation_id: OperationId::new("after-certified-prefix"),
                        data: Bytes::from_static(b"eleventh"),
                    })
                    .await
                    .unwrap()
                    .committed()
                    .await
                    .unwrap();
                assert_eq!(
                    app.applied.lock().unwrap().get(&11).unwrap().committed_lsn,
                    10
                );
            }
        }
    }
    // Existing applied bytes without any durable authority cannot trigger promotion.
    let app = Arc::new(TestApplication::default());
    app.apply(Operation {
        lsn: 1,
        committed_lsn: 0,
        data: Bytes::from_static(b"unverified"),
    })
    .await
    .unwrap();
    let runtime = PodRuntime::new(
        identity(1, "no-authority"),
        app.clone(),
        Arc::new(MemoryAuthorityStore::default()),
    );
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    assert!(
        runtime
            .apply_effect(effect(
                2,
                RuntimeEffectAction::ChangeRole(ReplicaRole::Primary)
            ))
            .await
            .is_err()
    );
    assert!(app.primary_progress.lock().unwrap().is_empty());
}

async fn next_copy_item(prepared: &mut PreparedCopy) -> proto::CopyItem {
    timeout(Duration::from_secs(1), prepared.items.next())
        .await
        .unwrap()
        .expect("copy stream ended")
        .unwrap()
}

async fn copy_through_final(prepared: &mut PreparedCopy) -> Vec<proto::CopyItem> {
    let mut items = Vec::new();
    loop {
        let item = next_copy_item(prepared).await;
        let final_item = item.final_item;
        items.push(item);
        if final_item {
            return items;
        }
    }
}

async fn prepare_copy_authorized(
    runtime: &PodRuntime,
    request: PrepareCopyRequest,
) -> Result<PreparedCopy> {
    runtime
        .authorize_build(
            request.build_id.clone(),
            request.target.clone(),
            request.configuration.clone(),
        )
        .await?;
    runtime.data_plane().prepare_copy(request).await
}

async fn open_primary(
    application: Arc<TestApplication>,
    members: Vec<ReplicaIdentity>,
) -> Arc<PodRuntime> {
    let local = members[0].clone();
    let runtime = Arc::new(PodRuntime::new(
        local.clone(),
        application,
        Arc::new(MemoryAuthorityStore::default()),
    ));
    for (index, action) in [
        RuntimeEffectAction::Open(OpenMode::Existing),
        RuntimeEffectAction::AdmitAuthority(Box::new(authority(local, members))),
        RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
    ]
    .into_iter()
    .enumerate()
    {
        runtime
            .apply_effect(effect(index as u64 + 1, action))
            .await
            .unwrap();
    }
    runtime
}

#[tokio::test]
async fn existing_build_id_rejects_a_different_exact_target() {
    let runtime = open_primary(
        Arc::new(TestApplication::default()),
        vec![
            identity(1, "primary"),
            identity(2, "secondary-2"),
            identity(3, "secondary-3"),
        ],
    )
    .await;
    let build_id = OperationId::new("target-bound-build");
    let first = identity(4, "replacement-4");
    runtime
        .authorize_build(build_id.clone(), first.clone(), BuildConfiguration::Current)
        .await
        .unwrap();

    assert!(matches!(
        runtime
            .authorize_build(
                build_id,
                identity(5, "replacement-5"),
                BuildConfiguration::Current,
            )
            .await,
        Err(RuntimeError::AuthorityMismatch(_))
    ));
}

#[test]
fn public_trait_method_sets_match_sf_v1_com_divisions() {
    fn methods(source: &str, name: &str) -> Vec<String> {
        let definition = source
            .split_once(&format!("pub trait {name}:"))
            .unwrap()
            .1
            .split_once("\n}")
            .unwrap()
            .0;
        let mut names = definition
            .split("fn ")
            .skip(1)
            .map(|tail| tail.split('(').next().unwrap().trim().to_owned())
            .collect::<Vec<_>>();
        names.sort();
        names
    }
    let replication = include_str!("../../kuberic-runtime/src/replicator/mod.rs");
    let application = include_str!("../../kuberic-runtime/src/application.rs");
    let library = include_str!("../../kuberic-runtime/src/lib.rs");
    assert!(
        replication.contains("#[doc(hidden)]\npub trait ManagedReplicator"),
        "the cross-crate managed bridge must remain hidden from generated user documentation"
    );
    assert!(
        !replication.contains("record_durable_peer_progress"),
        "raw peer status must not cross the managed bridge into quorum credit"
    );
    assert!(
        !include_str!("../src/transport.rs").contains(".record_durable_peer_progress("),
        "peer discovery may use reported progress for repair, never commit quorum credit"
    );
    assert!(!replication.contains("fn managed_replicator("));
    assert!(!replication.contains("ReplicatorInterfaces::new"));
    for internal_module in ["authority", "effects", "runtime"] {
        assert!(
            !library.contains(&format!("pub mod {internal_module};")),
            "{internal_module} must not be a public runtime module"
        );
    }
    for (source, name, expected) in [
        (
            replication,
            "Replicator",
            vec![
                "open",
                "change_role",
                "update_epoch",
                "close",
                "abort",
                "current_progress",
                "catch_up_capability",
            ],
        ),
        (
            replication,
            "PrimaryReplicator",
            vec![
                "on_data_loss",
                "update_catch_up_replica_set_configuration",
                "wait_for_catch_up_quorum",
                "update_current_replica_set_configuration",
                "build_replica",
                "remove_replica",
            ],
        ),
        (
            replication,
            "StateReplicator",
            vec![
                "replicate",
                "get_replication_stream",
                "get_copy_stream",
                "update_replicator_settings",
            ],
        ),
        (
            application,
            "StateProvider",
            vec![
                "update_epoch",
                "last_committed_lsn",
                "get_copy_context",
                "get_copy_state",
                "on_data_loss",
            ],
        ),
        (
            application,
            "StatefulServiceReplica",
            vec!["open", "change_role", "close", "abort"],
        ),
    ] {
        let mut expected = expected;
        expected.sort();
        assert_eq!(methods(source, name), expected, "{name}");
    }
}

#[tokio::test]
async fn streams_require_explicit_acknowledgement_and_drop_is_not_success() {
    let (sender, mut stream) = OperationStream::channel(1);
    let delivery = {
        let sender = sender.clone();
        tokio::spawn(async move {
            sender
                .send(
                    OperationMetadata::Replication {
                        lsn: 1,
                        committed_lsn: 0,
                    },
                    Bytes::from_static(b"one"),
                )
                .await
        })
    };
    let operation = stream.get_operation().await.unwrap().unwrap();
    assert!(!delivery.is_finished());
    drop(operation);
    assert!(matches!(
        delivery.await.unwrap(),
        Err(RuntimeError::WriteCompletionClosed)
    ));

    let delivery = {
        let sender = sender.clone();
        tokio::spawn(async move {
            sender
                .send(
                    OperationMetadata::Replication {
                        lsn: 1,
                        committed_lsn: 0,
                    },
                    Bytes::from_static(b"one"),
                )
                .await
        })
    };
    let operation = stream.get_operation().await.unwrap().unwrap();
    assert!(!delivery.is_finished());
    operation
        .acknowledge(DurableApplicationProgress {
            applied_lsn: 1,
            committed_lsn: 0,
        })
        .unwrap();
    assert_eq!(delivery.await.unwrap().unwrap().applied_lsn, 1);
    sender.close();
    assert!(stream.get_operation().await.unwrap().is_none());
}

#[tokio::test]
async fn service_owned_state_handle_and_stream_drive_exact_durable_quorum() {
    let primary = identity(1, "primary");
    let secondary = identity(2, "secondary");
    let members = vec![primary.clone(), secondary.clone()];
    let primary_app = Arc::new(TestApplication::default());
    *primary_app.settings.lock().unwrap() = Some(ReplicatorSettings {
        replication_address: "replication://primary".into(),
    });
    let source = open_primary(primary_app.clone(), members.clone()).await;
    assert_eq!(
        source.snapshot().await.replication_address.as_deref(),
        Some("replication://primary")
    );
    let target_app = Arc::new(TestApplication::default());
    target_app.manual_streams.store(true, Ordering::SeqCst);
    let target = Arc::new(PodRuntime::new(
        secondary.clone(),
        target_app.clone(),
        Arc::new(MemoryAuthorityStore::default()),
    ));
    for (index, action) in [
        RuntimeEffectAction::Open(OpenMode::New),
        RuntimeEffectAction::AdmitAuthority(Box::new(authority(secondary, members))),
        RuntimeEffectAction::ChangeRole(ReplicaRole::ActiveSecondary),
    ]
    .into_iter()
    .enumerate()
    {
        target
            .apply_effect(effect(index as u64 + 1, action))
            .await
            .unwrap();
    }
    let state = primary_app
        .state_replicator
        .lock()
        .unwrap()
        .clone()
        .unwrap();
    state
        .update_replicator_settings(ReplicatorSettings {
            replication_address: "replication://updated".into(),
        })
        .await
        .unwrap();
    assert!(state.get_replication_stream().await.is_err());
    assert!(state.get_copy_stream().await.is_err());
    let write = tokio::spawn(async move { state.replicate(Bytes::from_static(b"one")).await });
    #[cfg(not(feature = "testing"))]
    let receive = {
        let OutboundReplication::Replication(item) =
            source.data_plane().next_outbound().await.unwrap()
        else {
            panic!("expected replication")
        };
        target.data_plane().receive_replication(item).await.unwrap()
    };
    #[cfg(feature = "testing")]
    let mut transport = {
        let mut transport = kuberic_agent::testing::InProcessTransport::new();
        transport
            .register(source.clone(), ProcessSessionId::new("service-source"))
            .await
            .unwrap();
        transport
            .register(target.clone(), ProcessSessionId::new("service-target"))
            .await
            .unwrap();
        let event = in_process_transport_tests::event(&mut transport, |e| {
            matches!(e, kuberic_agent::testing::TransportEvent::Received { .. })
        })
        .await;
        let kuberic_agent::testing::TransportEvent::Received {
            acknowledgement, ..
        } = event
        else {
            unreachable!()
        };
        assert_eq!(acknowledgement.received_lsn, 1);
        assert_eq!(acknowledgement.applied_lsn, 0);
        transport
    };
    let mut stream = target_app.held_streams.lock().unwrap().remove(0);
    let operation = stream.get_operation().await.unwrap().unwrap();
    #[cfg(not(feature = "testing"))]
    {
        assert_eq!(receive.received.received_lsn, 1);
        assert_eq!(receive.received.applied_lsn, 0);
        source
            .data_plane()
            .accept_acknowledgement(receive.received.clone())
            .await
            .unwrap();
    }
    assert!(!write.is_finished());
    let OperationMetadata::Replication { lsn, committed_lsn } = operation.metadata else {
        panic!("expected replication metadata")
    };
    let progress = target_app
        .apply(Operation {
            lsn,
            committed_lsn,
            data: operation.data.clone(),
        })
        .await
        .unwrap();
    assert!(!write.is_finished());
    operation.acknowledge(progress).unwrap();
    #[cfg(not(feature = "testing"))]
    source
        .data_plane()
        .accept_acknowledgement(receive.applied().await.unwrap())
        .await
        .unwrap();
    #[cfg(feature = "testing")]
    in_process_transport_tests::event(&mut transport, |e| {
        matches!(e, kuberic_agent::testing::TransportEvent::Applied { .. })
    })
    .await;
    assert_eq!(write.await.unwrap().unwrap(), 1);
    assert_eq!(target.snapshot().await.verified_replication_lsn, Some(1));
}

#[tokio::test]
async fn state_replicator_retries_failed_and_cancelled_operations_without_losing_identity() {
    let app = Arc::new(TestApplication::default());
    let runtime = open_primary(app.clone(), vec![identity(1, "primary")]).await;
    let state = app.state_replicator.lock().unwrap().clone().unwrap();
    app.fail_after_apply.store(true, Ordering::SeqCst);
    assert!(state.replicate(Bytes::from_static(b"one")).await.is_err());
    assert_eq!(
        state.replicate(Bytes::from_static(b"one")).await.unwrap(),
        1
    );
    app.pause_after_apply.store(true, Ordering::SeqCst);
    let write = {
        let state = state.clone();
        tokio::spawn(async move { state.replicate(Bytes::from_static(b"two")).await })
    };
    app.applied_notify.notified().await;
    write.abort();
    assert!(write.await.unwrap_err().is_cancelled());
    app.pause_after_apply.store(false, Ordering::SeqCst);
    assert_eq!(
        state.replicate(Bytes::from_static(b"two")).await.unwrap(),
        2
    );
    assert_eq!(runtime.snapshot().await.committed_lsn, 2);
}

#[tokio::test]
async fn restarted_state_replicator_resolves_original_identity_before_a_different_write() {
    let local = identity(1, "primary");
    let store = Arc::new(MemoryAuthorityStore::default());
    store.local_writes.lock().unwrap().insert(
        OperationId::new("sf:old-session:1"),
        DurableLocalWrite {
            operation_id: OperationId::new("sf:old-session:1"),
            lsn: 1,
            committed_lsn: 0,
            data: Bytes::from_static(b"pending-a"),
            phase: kuberic_runtime_internal::authority::LocalWritePhase::Registered,
        },
    );
    let application = Arc::new(TestApplication::default());
    let runtime = Arc::new(PodRuntime::new(
        local.clone(),
        application.clone(),
        store.clone(),
    ));
    for (index, action) in [
        RuntimeEffectAction::Open(OpenMode::Existing),
        RuntimeEffectAction::AdmitAuthority(Box::new(authority(local.clone(), vec![local]))),
        RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
    ]
    .into_iter()
    .enumerate()
    {
        runtime
            .apply_effect(effect(index as u64 + 1, action))
            .await
            .unwrap();
    }
    let state = application
        .state_replicator
        .lock()
        .unwrap()
        .clone()
        .unwrap();

    let original = store
        .load_local_write(&OperationId::new("sf:old-session:1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        original.phase,
        kuberic_runtime_internal::authority::LocalWritePhase::Committed
    );
    assert_eq!(original.data, Bytes::from_static(b"pending-a"));
    assert_eq!(
        state
            .replicate(Bytes::from_static(b"different-b"))
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: original.operation_id,
                data: original.data,
            })
            .await
            .unwrap()
            .committed()
            .await
            .unwrap()
            .committed_lsn,
        1
    );
}

#[tokio::test]
async fn reconstruction_republishes_the_original_registered_write_without_a_client() {
    let local = identity(1, "primary");
    let store = Arc::new(MemoryAuthorityStore::default());
    let admitted = authority(local.clone(), vec![local.clone()]);
    *store.authority.lock().unwrap() = Some(admitted);
    store.local_writes.lock().unwrap().insert(
        OperationId::new("sf:old-session:3"),
        DurableLocalWrite {
            operation_id: OperationId::new("sf:old-session:3"),
            lsn: 3,
            committed_lsn: 2,
            data: Bytes::from_static(b"pending-three"),
            phase: kuberic_runtime_internal::authority::LocalWritePhase::Registered,
        },
    );
    let application = Arc::new(TestApplication::default());
    application.applied.lock().unwrap().insert(
        3,
        Operation {
            lsn: 3,
            committed_lsn: 2,
            data: Bytes::from_static(b"pending-three"),
        },
    );
    *application.progress.lock().unwrap() = DurableApplicationProgress {
        applied_lsn: 3,
        committed_lsn: 2,
    };
    let runtime = PodRuntime::new(local, application, store.clone());
    runtime
        .reconstruct(
            OpenMode::Existing,
            ReplicaRole::Primary,
            AccessStatus::Granted,
            AccessStatus::Granted,
            None,
        )
        .await
        .unwrap();

    assert_eq!(runtime.snapshot().await.committed_lsn, 3);
    assert_eq!(
        store
            .local_writes
            .lock()
            .unwrap()
            .get(&OperationId::new("sf:old-session:3"))
            .unwrap()
            .phase,
        kuberic_runtime_internal::authority::LocalWritePhase::Committed
    );
}

#[tokio::test]
async fn cold_primary_replays_retained_history_to_a_lagging_current_peer() {
    let primary = identity(1, "primary");
    let secondary = identity(2, "secondary");
    let application = Arc::new(TestApplication::default());
    application.seed_operation(1, Bytes::from_static(b"one"));
    let runtime = open_primary(application, vec![primary.clone(), secondary.clone()]).await;

    runtime.repair_peer(secondary.clone(), 0).await.unwrap();
    let OutboundReplication::Replication(item) =
        runtime.data_plane().next_outbound().await.unwrap()
    else {
        panic!("expected retained replication repair");
    };
    assert_eq!(
        item.receiver.as_ref().map(|identity| identity.replica_id),
        Some(secondary.replica_id.value())
    );
    assert_eq!(item.lsn, 1);
    assert_eq!(item.data, b"one");
}

#[tokio::test]
async fn peer_repair_status_does_not_grant_commit_quorum() {
    let primary = identity(1, "primary");
    let returned = identity(2, "returned");
    let acknowledged = identity(3, "acknowledged");
    let fourth = identity(4, "fourth");
    let fifth = identity(5, "fifth");
    let members = vec![
        primary.clone(),
        returned.clone(),
        acknowledged.clone(),
        fourth.clone(),
        fifth,
    ];
    let admitted = authority(primary, members.clone());
    let application = Arc::new(TestApplication::default());
    application.seed_progress(100);
    let runtime = open_primary(application, members).await;

    let pending = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("post-failover-101"),
            data: Bytes::from_static(b"new-101"),
        })
        .await
        .unwrap();
    assert_eq!(pending.lsn, 101);

    runtime.repair_peer(returned, 120).await.unwrap();
    runtime
        .data_plane()
        .accept_acknowledgement(acknowledgement(&admitted, acknowledged, 101))
        .await
        .unwrap();

    let committed = tokio::spawn(pending.committed());
    tokio::task::yield_now().await;
    assert!(
        !committed.is_finished(),
        "raw repair progress must not provide the third quorum vote"
    );

    runtime
        .data_plane()
        .accept_acknowledgement(acknowledgement(&admitted, fourth, 101))
        .await
        .unwrap();
    assert_eq!(committed.await.unwrap().unwrap().committed_lsn, 101);
}

#[tokio::test]
async fn primary_control_build_waits_for_service_copy_ack_and_removal_is_fenced() {
    let primary = identity(1, "primary");
    let replacement = identity(2, "replacement");
    let source = open_primary(Arc::new(TestApplication::default()), vec![primary.clone()]).await;
    let control = source.primary_replicator().await.unwrap();
    let build = {
        let control = control.clone();
        let target = replacement.clone();
        tokio::spawn(async move {
            control
                .build_replica(kuberic_runtime::replicator::ReplicaInformation {
                    build_id: OperationId::new("sf-build"),
                    identity: target,
                    replication_address: "target".into(),
                })
                .await
        })
    };
    assert!(matches!(
        source.data_plane().next_outbound().await,
        Some(OutboundReplication::Build(_))
    ));
    let mut prepared = prepare_copy_authorized(
        &source,
        PrepareCopyRequest {
            build_id: OperationId::new("sf-build"),
            target: replacement.clone(),
            configuration: BuildConfiguration::Current,
            copy_context: empty_copy_context(),
        },
    )
    .await
    .unwrap();
    let target_app = Arc::new(TestApplication::default());
    target_app.manual_streams.store(true, Ordering::SeqCst);
    let target = Arc::new(PodRuntime::new(
        replacement.clone(),
        target_app.clone(),
        Arc::new(MemoryAuthorityStore::default()),
    ));
    for (index, action) in [
        RuntimeEffectAction::Open(OpenMode::New),
        RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary),
        RuntimeEffectAction::AdmitBuildAuthority(Box::new(prepared.authority.clone())),
    ]
    .into_iter()
    .enumerate()
    {
        target
            .apply_effect(effect(index as u64 + 1, action))
            .await
            .unwrap();
    }
    let mut copy = target_app.held_streams.lock().unwrap().remove(1);
    let delivery = {
        let target = target.clone();
        let item = next_copy_item(&mut prepared).await;
        tokio::spawn(async move { target.data_plane().receive_copy_item(item).await })
    };
    let operation = copy.get_operation().await.unwrap().unwrap();
    assert!(matches!(
        operation.metadata,
        OperationMetadata::CopyComplete { .. }
    ));
    assert!(!delivery.is_finished());
    assert!(!build.is_finished());
    operation
        .acknowledge(DurableApplicationProgress::default())
        .unwrap();
    source
        .data_plane()
        .accept_copy_acknowledgement(delivery.await.unwrap().unwrap())
        .await
        .unwrap();
    timeout(Duration::from_secs(1), build)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        control.remove_replica(primary.replica_id).await,
        Err(RuntimeError::AuthorityMismatch(_))
    ));
    control
        .remove_replica(replacement.replica_id)
        .await
        .unwrap();
    assert!(source.snapshot().await.builds.is_empty());
    assert!(matches!(
        source.data_plane().next_outbound().await,
        Some(OutboundReplication::Remove(_))
    ));
}

#[tokio::test]
async fn direct_control_epoch_fences_old_replication_and_abort_ends_pending_quorum_waits() {
    let app = Arc::new(TestApplication::default());
    let local = identity(1, "primary");
    let secondary = identity(2, "secondary");
    let runtime = open_primary(app.clone(), vec![local.clone(), secondary.clone()]).await;
    let pending = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("pending"),
            data: Bytes::from_static(b"one"),
        })
        .await
        .unwrap();
    let control = app.returned_control.lock().unwrap().clone().unwrap();
    control.update_epoch(Epoch::new(0, 2)).await.unwrap();
    assert!(matches!(
        pending.committed().await,
        Err(RuntimeError::WriteClosed(_))
    ));
    assert!(matches!(
        runtime
            .data_plane()
            .accept_acknowledgement(acknowledgement(
                &authority(local, vec![identity(1, "primary"), secondary.clone()]),
                secondary,
                1
            ))
            .await,
        Err(RuntimeError::AuthorityMismatch(_))
    ));
    assert!(control.update_epoch(Epoch::new(0, 1)).await.is_err());
    control.abort();
    let state = app.state_replicator.lock().unwrap().clone().unwrap();
    assert!(matches!(
        state.replicate(Bytes::from_static(b"two")).await,
        Err(RuntimeError::Closed)
    ));
}

#[tokio::test]
async fn failed_or_cancelled_service_open_aborts_created_interfaces() {
    for cancelled in [false, true] {
        let app = Arc::new(TestApplication::default());
        app.pause_open.store(cancelled, Ordering::SeqCst);
        app.fail_open.store(!cancelled, Ordering::SeqCst);
        let runtime = Arc::new(PodRuntime::new(
            identity(1, "primary"),
            app.clone(),
            Arc::new(MemoryAuthorityStore::default()),
        ));
        let open = {
            let runtime = runtime.clone();
            tokio::spawn(async move {
                runtime
                    .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
                    .await
            })
        };
        if cancelled {
            app.open_notify.notified().await;
            open.abort();
            assert!(open.await.unwrap_err().is_cancelled());
        } else {
            assert!(matches!(
                open.await.unwrap(),
                Err(RuntimeError::Application(_))
            ));
        }
        assert!(!runtime.snapshot().await.open);
        let state = app.state_replicator.lock().unwrap().clone().unwrap();
        assert!(matches!(
            state.replicate(Bytes::from_static(b"no")).await,
            Err(RuntimeError::Closed)
        ));
        assert!(
            app.events
                .lock()
                .unwrap()
                .iter()
                .any(|event| event == "service.abort")
        );
    }
}

#[tokio::test]
async fn cancelled_factory_creation_aborts_pending_managed_replicator() {
    let application = Arc::new(TestApplication::default());
    let captured = Arc::new(Mutex::new(None));
    let created = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    *application.factory.lock().unwrap() = Some(Arc::new(PausingFactory {
        storage: Arc::downgrade(&application),
        captured: captured.clone(),
        created: created.clone(),
        resume,
    }));
    let runtime = Arc::new(PodRuntime::new(
        identity(1, "cancelled-factory"),
        application,
        Arc::new(MemoryAuthorityStore::default()),
    ));
    let open_runtime = runtime.clone();
    let open = tokio::spawn(async move {
        open_runtime
            .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
            .await
    });
    created.notified().await;
    open.abort();
    assert!(matches!(open.await, Err(error) if error.is_cancelled()));
    let control = captured.lock().unwrap().clone().unwrap();
    assert!(matches!(control.open().await, Err(RuntimeError::Closed)));
}

#[tokio::test]
async fn quorum_modes_data_loss_and_configuration_methods_use_the_default_engine() {
    use kuberic_runtime::replicator::ReplicaSetQuorumMode;
    let members = vec![
        identity(1, "primary"),
        identity(2, "second"),
        identity(3, "third"),
    ];
    let app = Arc::new(TestApplication::default());
    app.seed_progress(1);
    let runtime = open_primary(app.clone(), members.clone()).await;
    let control = runtime.primary_replicator().await.unwrap();
    assert_eq!(control.current_progress().await.unwrap(), 1);
    assert_eq!(control.catch_up_capability().await.unwrap(), 1);
    let admitted = authority(members[0].clone(), members.clone());
    let mut invalid = admitted.current_configuration.clone();
    invalid.epoch = Epoch::new(0, 3);
    assert!(matches!(
        control
            .update_current_replica_set_configuration(invalid)
            .await,
        Err(RuntimeError::AuthorityMismatch(_))
    ));
    assert!(!control.on_data_loss().await.unwrap());
    assert!(
        app.events
            .lock()
            .unwrap()
            .iter()
            .any(|event| event == "provider.on_data_loss")
    );
    let quorum = {
        let control = control.clone();
        tokio::spawn(async move {
            control
                .wait_for_catch_up_quorum(ReplicaSetQuorumMode::WriteQuorum)
                .await
        })
    };
    let all = {
        let control = control.clone();
        tokio::spawn(async move {
            control
                .wait_for_catch_up_quorum(ReplicaSetQuorumMode::All)
                .await
        })
    };
    runtime
        .data_plane()
        .accept_acknowledgement(acknowledgement(&admitted, members[1].clone(), 1))
        .await
        .unwrap();
    timeout(Duration::from_secs(1), quorum)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!all.is_finished());
    control.abort();
    assert!(matches!(
        timeout(Duration::from_secs(1), all).await.unwrap().unwrap(),
        Err(RuntimeError::Closed)
    ));
}

#[tokio::test]
async fn lifecycle_orders_role_changes_and_close_like_service_fabric() {
    let app = Arc::new(TestApplication::default());
    let events = app.events.clone();
    *app.factory.lock().unwrap() = Some(Arc::new(CountingFactory {
        storage: Arc::downgrade(&app),
        opened: Arc::new(AtomicUsize::new(0)),
        role_changes: Arc::new(AtomicUsize::new(0)),
        epoch_updates: Arc::new(AtomicUsize::new(0)),
        events: events.clone(),
        fail_change_role: Arc::new(AtomicBool::new(false)),
        fail_close: Arc::new(AtomicBool::new(false)),
    }));
    let runtime = open_primary(app.clone(), vec![identity(1, "primary")]).await;
    events.lock().unwrap().clear();
    runtime
        .apply_effect(effect(
            5,
            RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            6,
            RuntimeEffectAction::ChangeRole(ReplicaRole::ActiveSecondary),
        ))
        .await
        .unwrap();
    let close = effect(7, RuntimeEffectAction::Close);
    let result = runtime.apply_effect(close.clone()).await.unwrap();
    assert_eq!(runtime.apply_effect(close).await.unwrap(), result);
    runtime
        .apply_effect(effect(8, RuntimeEffectAction::Abort))
        .await
        .unwrap();
    drop(runtime);
    assert_eq!(
        events.lock().unwrap().as_slice(),
        [
            "replicator.change_role",
            "service.change_role",
            "replicator.change_role",
            "service.change_role",
            "replicator.close",
            "service.close",
        ]
    );
}

#[tokio::test]
async fn primary_promotion_retries_epoch_stage_before_application_role() {
    let local = identity(1, "promotion-primary");
    let application = Arc::new(TestApplication::default());
    let events = application.events.clone();
    let role_changes = Arc::new(AtomicUsize::new(0));
    let epoch_updates = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with_factory(
        local.clone(),
        application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
        CountingFactory {
            storage: Arc::downgrade(&application),
            opened: Arc::new(AtomicUsize::new(0)),
            role_changes: role_changes.clone(),
            epoch_updates: epoch_updates.clone(),
            events: events.clone(),
            fail_change_role: Arc::new(AtomicBool::new(false)),
            fail_close: Arc::new(AtomicBool::new(false)),
        },
    )
    .unwrap();
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority(local.clone(), vec![local]))),
        ))
        .await
        .unwrap();
    events.lock().unwrap().clear();
    application.fail_update_epoch.store(true, Ordering::SeqCst);
    let promote = effect(3, RuntimeEffectAction::ChangeRole(ReplicaRole::Primary));
    assert!(matches!(
        runtime.apply_effect(promote.clone()).await,
        Err(RuntimeError::Application(_))
    ));
    assert_eq!(
        runtime.snapshot().await.role_transition,
        Some(kuberic_runtime_internal::effects::RoleTransition {
            completed_role: ReplicaRole::None,
            target_role: ReplicaRole::Primary,
            replicator_completed: true,
            epoch_completed: false,
            application_completed: false,
        })
    );
    runtime.apply_effect(promote).await.unwrap();

    assert_eq!(role_changes.load(Ordering::SeqCst), 1);
    assert_eq!(epoch_updates.load(Ordering::SeqCst), 2);
    assert_eq!(
        events.lock().unwrap().as_slice(),
        [
            "replicator.change_role",
            "replicator.update_epoch",
            "provider.update_epoch",
            "replicator.update_epoch",
            "provider.update_epoch",
            "service.change_role",
        ]
    );
}

#[tokio::test]
async fn newer_primary_authority_stays_write_closed_until_epoch_stage_completes() {
    let local = identity(1, "same-primary-new-epoch");
    let application = Arc::new(TestApplication::default());
    let runtime = open_primary(application.clone(), vec![local.clone()]).await;
    let newer = AdmittedAuthority {
        scale_up: None,
        secondary_removal: None,
        local_identity: local.clone(),
        transition_kind: None,
        previous_configuration: None,
        current_configuration: ConfigurationDescriptor::new(
            Epoch::new(0, 2),
            local.replica_id,
            vec![ConfigurationMember {
                identity: local,
                role: ReplicaRole::Primary,
            }],
            1,
        ),
        switchover_handoff: None,
    };
    runtime
        .apply_effect(effect(
            5,
            RuntimeEffectAction::AdmitAuthority(Box::new(newer)),
        ))
        .await
        .unwrap();
    assert_eq!(
        runtime.snapshot().await.write_status,
        AccessStatus::ReconfigurationPending
    );
    assert!(matches!(
        runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("blocked-before-epoch"),
                data: Bytes::from_static(b"blocked"),
            })
            .await,
        Err(RuntimeError::WriteClosed(
            AccessStatus::ReconfigurationPending
        ))
    ));
    runtime
        .apply_effect(effect(
            6,
            RuntimeEffectAction::ChangeReplicatorRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    application.pause_update_epoch.store(true, Ordering::SeqCst);
    let epoch_runtime = runtime.clone();
    let update = tokio::spawn(async move {
        epoch_runtime
            .apply_effect(effect(7, RuntimeEffectAction::UpdateEpoch))
            .await
    });
    application.update_epoch_notify.notified().await;
    assert!(matches!(
        runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("blocked-during-epoch"),
                data: Bytes::from_static(b"blocked"),
            })
            .await,
        Err(RuntimeError::WriteClosed(
            AccessStatus::ReconfigurationPending
        ))
    ));
    application
        .pause_update_epoch
        .store(false, Ordering::SeqCst);
    application.resume_update_epoch_notify.notify_waiters();
    update.await.unwrap().unwrap();
    runtime
        .apply_effect(effect(
            8,
            RuntimeEffectAction::ChangeApplicationRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            9,
            RuntimeEffectAction::SetAccessStatus {
                read: AccessStatus::Granted,
                write: AccessStatus::Granted,
            },
        ))
        .await
        .unwrap();
    runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("allowed-after-epoch"),
            data: Bytes::from_static(b"allowed"),
        })
        .await
        .unwrap()
        .committed()
        .await
        .unwrap();
}

#[tokio::test]
async fn explicit_abort_stops_replicator_before_application() {
    let application = Arc::new(TestApplication::default());
    let events = application.events.clone();
    let runtime = runtime_with_factory(
        identity(1, "explicit-abort"),
        application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
        CountingFactory {
            storage: Arc::downgrade(&application),
            opened: Arc::new(AtomicUsize::new(0)),
            role_changes: Arc::new(AtomicUsize::new(0)),
            epoch_updates: Arc::new(AtomicUsize::new(0)),
            events: events.clone(),
            fail_change_role: Arc::new(AtomicBool::new(false)),
            fail_close: Arc::new(AtomicBool::new(false)),
        },
    )
    .unwrap();
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    events.lock().unwrap().clear();
    runtime
        .apply_effect(effect(2, RuntimeEffectAction::Abort))
        .await
        .unwrap();
    assert_eq!(
        events.lock().unwrap().as_slice(),
        ["replicator.abort", "service.abort"]
    );
}

#[tokio::test]
async fn ambiguous_primary_authority_admission_fences_pending_writes_and_old_acks() {
    let local = identity(1, "primary");
    let secondary = identity(2, "secondary");
    let admitted = authority(local.clone(), vec![local.clone(), secondary.clone()]);
    let store = Arc::new(MemoryAuthorityStore::default());
    let runtime = PodRuntime::new(local, Arc::new(TestApplication::default()), store.clone());
    for (index, action) in [
        RuntimeEffectAction::Open(OpenMode::Existing),
        RuntimeEffectAction::AdmitAuthority(Box::new(admitted.clone())),
        RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
    ]
    .into_iter()
    .enumerate()
    {
        runtime
            .apply_effect(effect(index as u64 + 1, action))
            .await
            .unwrap();
    }
    let pending = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("old-epoch"),
            data: Bytes::from_static(b"one"),
        })
        .await
        .unwrap();
    let next = AdmittedAuthority {
        current_configuration: ConfigurationDescriptor::new(
            Epoch::new(0, 2),
            admitted.current_configuration.primary_id,
            admitted.current_configuration.members.clone(),
            admitted.current_configuration.write_quorum,
        ),
        ..admitted.clone()
    };
    store.fail_after_admit.store(true, Ordering::SeqCst);
    assert!(matches!(
        runtime
            .apply_effect(effect(
                5,
                RuntimeEffectAction::AdmitAuthority(Box::new(next))
            ))
            .await,
        Err(RuntimeError::Application(_))
    ));
    assert!(matches!(
        pending.committed().await,
        Err(RuntimeError::WriteClosed(_))
    ));
    assert!(matches!(
        runtime
            .data_plane()
            .accept_acknowledgement(acknowledgement(&admitted, secondary, 1))
            .await,
        Err(RuntimeError::AuthorityMismatch(_))
    ));
    let snapshot = runtime.snapshot().await;
    assert_eq!(snapshot.committed_lsn, 0);
    assert_eq!(snapshot.write_status, AccessStatus::ReconfigurationPending);
}

#[tokio::test]
async fn removing_a_replica_terminates_its_pending_build_wait() {
    let runtime = open_primary(
        Arc::new(TestApplication::default()),
        vec![identity(1, "primary")],
    )
    .await;
    let control = runtime.primary_replicator().await.unwrap();
    let target = identity(2, "target");
    let wait = {
        let control = control.clone();
        let target = target.clone();
        tokio::spawn(async move {
            control
                .build_replica(kuberic_runtime::replicator::ReplicaInformation {
                    build_id: OperationId::new("removed-build"),
                    identity: target,
                    replication_address: "target".into(),
                })
                .await
        })
    };
    assert!(matches!(
        runtime.data_plane().next_outbound().await,
        Some(OutboundReplication::Build(_))
    ));
    control.remove_replica(target.replica_id).await.unwrap();
    assert!(matches!(
        timeout(Duration::from_secs(1), wait)
            .await
            .unwrap()
            .unwrap(),
        Err(RuntimeError::ReplicaRemoved(2))
    ));
}

#[tokio::test]
async fn cancelling_an_exact_outbound_build_terminates_only_its_pending_wait() {
    let runtime = open_primary(
        Arc::new(TestApplication::default()),
        vec![identity(1, "primary")],
    )
    .await;
    let control = runtime.primary_replicator().await.unwrap();
    let cancelled_id = OperationId::new("cancelled-build");
    let cancelled = {
        let control = control.clone();
        let build_id = cancelled_id.clone();
        tokio::spawn(async move {
            control
                .build_replica(kuberic_runtime::replicator::ReplicaInformation {
                    build_id,
                    identity: identity(2, "cancelled-target"),
                    replication_address: "cancelled-target".into(),
                })
                .await
        })
    };
    assert!(matches!(
        runtime.data_plane().next_outbound().await,
        Some(OutboundReplication::Build(endpoint))
            if endpoint.build_id == cancelled_id
    ));
    runtime.cancel_outbound_build(&cancelled_id).await.unwrap();
    assert!(matches!(
        timeout(Duration::from_secs(1), cancelled)
            .await
            .unwrap()
            .unwrap(),
        Err(RuntimeError::OperationCancelled)
    ));
}

#[tokio::test]
async fn bounded_outbound_build_queue_is_cancelled_by_abort() {
    let runtime = open_primary(
        Arc::new(TestApplication::default()),
        vec![identity(1, "primary")],
    )
    .await;
    let control = runtime.primary_replicator().await.unwrap();
    let mut builds = Vec::new();
    for id in 2..=66 {
        let control = control.clone();
        builds.push(tokio::spawn(async move {
            control
                .build_replica(kuberic_runtime::replicator::ReplicaInformation {
                    build_id: OperationId::new(format!("bounded-build-{id}")),
                    identity: identity(id, &format!("target-{id}")),
                    replication_address: format!("target-{id}"),
                })
                .await
        }));
    }
    tokio::task::yield_now().await;
    assert!(builds.iter().any(|build| !build.is_finished()));
    control.abort();
    for build in builds {
        assert!(matches!(
            timeout(Duration::from_secs(1), build)
                .await
                .unwrap()
                .unwrap(),
            Err(RuntimeError::Closed)
        ));
    }
}

#[tokio::test]
async fn dropping_runtime_always_aborts_application_and_selected_control() {
    let application = Arc::new(TestApplication::default());
    let events = application.events.clone();
    let runtime = runtime_with_factory(
        identity(1, "drop-abort"),
        application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
        CountingFactory {
            storage: Arc::downgrade(&application),
            opened: Arc::new(AtomicUsize::new(0)),
            role_changes: Arc::new(AtomicUsize::new(0)),
            epoch_updates: Arc::new(AtomicUsize::new(0)),
            events: events.clone(),
            fail_change_role: Arc::new(AtomicBool::new(false)),
            fail_close: Arc::new(AtomicBool::new(false)),
        },
    )
    .unwrap();
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    drop(runtime);
    assert_eq!(
        events.lock().unwrap().as_slice(),
        [
            "service.open",
            "replicator.open",
            "replicator.abort",
            "service.abort",
        ]
    );
}

#[tokio::test]
async fn dropping_runtime_aborts_retained_state_handles_streams_and_pending_writes() {
    let app = Arc::new(TestApplication::default());
    app.manual_streams.store(true, Ordering::SeqCst);
    let runtime = open_primary(
        app.clone(),
        vec![identity(1, "primary"), identity(2, "secondary")],
    )
    .await;
    let state = app.state_replicator.lock().unwrap().clone().unwrap();
    let write = {
        let state = state.clone();
        tokio::spawn(async move { state.replicate(Bytes::from_static(b"pending")).await })
    };
    assert!(matches!(
        runtime.data_plane().next_outbound().await,
        Some(OutboundReplication::Replication(_))
    ));
    drop(runtime);
    assert!(matches!(
        timeout(Duration::from_secs(1), write)
            .await
            .unwrap()
            .unwrap(),
        Err(RuntimeError::Closed)
    ));
    assert!(matches!(
        state.replicate(Bytes::from_static(b"closed")).await,
        Err(RuntimeError::Closed)
    ));
    let streams = std::mem::take(&mut *app.held_streams.lock().unwrap());
    for mut stream in streams {
        assert!(stream.get_operation().await.unwrap().is_none());
    }
}

fn identity(id: i64, instance: &str) -> ReplicaIdentity {
    ReplicaIdentity {
        replica_id: ReplicaId::new(id),
        instance_id: ReplicaInstanceId::new(instance),
        agent_generation: AgentGeneration::new(format!("generation-{instance}")),
    }
}

struct ExternalReplicator {
    replication: Mutex<Option<OperationStream>>,
    copy: Mutex<Option<OperationStream>>,
    senders: Vec<kuberic_runtime::replicator::stream::OperationSender>,
    settings: Mutex<ReplicatorSettings>,
    closed: AtomicBool,
}

#[async_trait]
impl Replicator for ExternalReplicator {
    async fn open(&self) -> Result<String> {
        Ok("external://replica".into())
    }
    async fn change_role(&self, _epoch: Epoch, _role: ReplicaRole) -> Result<()> {
        Ok(())
    }
    async fn update_epoch(&self, _epoch: Epoch) -> Result<()> {
        Ok(())
    }
    async fn close(&self) -> Result<()> {
        self.abort();
        Ok(())
    }
    fn abort(&self) {
        self.closed.store(true, Ordering::SeqCst);
        for sender in &self.senders {
            sender.close();
        }
    }
    async fn current_progress(&self) -> Result<i64> {
        Ok(7)
    }
    async fn catch_up_capability(&self) -> Result<i64> {
        Ok(3)
    }
}

#[async_trait]
impl StateReplicator for ExternalReplicator {
    async fn replicate(&self, _data: Bytes) -> Result<i64> {
        Err(RuntimeError::NotPrimary)
    }
    async fn get_replication_stream(&self) -> Result<OperationStream> {
        self.replication
            .lock()
            .unwrap()
            .take()
            .ok_or(RuntimeError::Closed)
    }
    async fn get_copy_stream(&self) -> Result<OperationStream> {
        self.copy.lock().unwrap().take().ok_or(RuntimeError::Closed)
    }
    async fn update_replicator_settings(&self, settings: ReplicatorSettings) -> Result<()> {
        *self.settings.lock().unwrap() = settings;
        Ok(())
    }
}

struct ExternalFactory;

#[async_trait]
impl ReplicatorFactory for ExternalFactory {
    async fn create_replicator(
        &self,
        _context: ReplicatorFactoryContext,
        _provider: Arc<dyn StateProvider>,
        settings: ReplicatorSettings,
    ) -> Result<ReplicatorInterfaces> {
        let (replication_tx, replication) = OperationStream::channel(1);
        let (copy_tx, copy) = OperationStream::channel(1);
        let replicator = Arc::new(ExternalReplicator {
            replication: Mutex::new(Some(replication)),
            copy: Mutex::new(Some(copy)),
            senders: vec![replication_tx, copy_tx],
            settings: Mutex::new(settings),
            closed: AtomicBool::new(false),
        });
        Ok(ReplicatorInterfaces::secondary(
            replicator.clone(),
            replicator,
        ))
    }
}

struct CountingExternalFactory {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl ReplicatorFactory for CountingExternalFactory {
    async fn create_replicator(
        &self,
        context: ReplicatorFactoryContext,
        provider: Arc<dyn StateProvider>,
        settings: ReplicatorSettings,
    ) -> Result<ReplicatorInterfaces> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        ExternalFactory
            .create_replicator(context, provider, settings)
            .await
    }
}

#[derive(Default)]
struct ExternalService {
    state: Mutex<Option<Arc<dyn StateReplicator>>>,
    streams: Mutex<Vec<OperationStream>>,
}

struct DoubleCreateService {
    factory_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl StatefulServiceReplica for DoubleCreateService {
    async fn open(self: Arc<Self>, context: OpenContext) -> Result<Arc<dyn Replicator>> {
        let partition = context
            .partition
            .with_factory(Arc::new(CountingExternalFactory {
                calls: self.factory_calls.clone(),
            }));
        let first = partition
            .create_replicator(Arc::new(TestApplication::default()), None)
            .await?;
        assert!(matches!(
            partition
                .create_replicator(Arc::new(TestApplication::default()), None)
                .await,
            Err(RuntimeError::Application(_))
        ));
        Ok(first.replicator())
    }

    async fn change_role(&self, _role: ReplicaRole) -> Result<RoleChange> {
        Ok(RoleChange {
            service_address: None,
        })
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    fn abort(&self) {}
}

#[async_trait]
impl StatefulServiceReplica for ExternalService {
    async fn open(self: Arc<Self>, context: OpenContext) -> Result<Arc<dyn Replicator>> {
        let interfaces = context
            .partition
            .with_factory(Arc::new(ExternalFactory))
            .create_replicator(Arc::new(TestApplication::default()), None)
            .await?;
        let state_replicator = interfaces.state_replicator();
        let streams = vec![
            state_replicator.get_replication_stream().await?,
            state_replicator.get_copy_stream().await?,
        ];
        *self.streams.lock().unwrap() = streams;
        *self.state.lock().unwrap() = Some(state_replicator);
        Ok(interfaces.replicator())
    }
    async fn change_role(&self, _role: ReplicaRole) -> Result<RoleChange> {
        Ok(RoleChange {
            service_address: None,
        })
    }
    async fn close(&self) -> Result<()> {
        Ok(())
    }
    fn abort(&self) {}
}

#[tokio::test]
async fn custom_factory_does_not_require_the_default_engine_or_service_storage_traits() {
    let service = Arc::new(ExternalService::default());
    let runtime = PodRuntime::new(
        identity(1, "external"),
        service.clone(),
        Arc::new(MemoryAuthorityStore::default()),
    );
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::New)))
        .await
        .unwrap();
    let snapshot = runtime.snapshot().await;
    assert_eq!(
        snapshot.replication_address.as_deref(),
        Some("external://replica")
    );
    assert_eq!(snapshot.current_progress, 7);
    assert!(service.state.lock().unwrap().is_some());
    assert_eq!(service.streams.lock().unwrap().len(), 2);
    runtime
        .apply_effect(effect(2, RuntimeEffectAction::RefreshApplicationProgress))
        .await
        .unwrap();
    assert!(matches!(
        runtime
            .apply_effect(effect(
                3,
                RuntimeEffectAction::PrepareSwitchover {
                    preparation_generation: 1,
                    request_id: SwitchoverRequestId::new("custom-request"),
                    source: identity(1, "external"),
                    target: identity(2, "target"),
                    starting_configuration_id: kuberic_protocol::types::ConfigurationId::new(
                        "configuration",
                    ),
                    starting_epoch: Epoch::new(0, 1),
                },
            ))
            .await,
        Err(RuntimeError::Application(_))
    ));
    runtime
        .apply_effect(effect(3, RuntimeEffectAction::Close))
        .await
        .unwrap();
    let streams = std::mem::take(&mut *service.streams.lock().unwrap());
    for mut stream in streams {
        assert!(stream.get_operation().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn create_replicator_reserves_one_shot_ownership_before_factory_invocation() {
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let runtime = PodRuntime::new(
        identity(1, "double-create"),
        Arc::new(DoubleCreateService {
            factory_calls: factory_calls.clone(),
        }),
        Arc::new(MemoryAuthorityStore::default()),
    );
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::New)))
        .await
        .unwrap();
    assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
}

fn authority(local: ReplicaIdentity, members: Vec<ReplicaIdentity>) -> AdmittedAuthority {
    let write_quorum = members.len() as u32 / 2 + 1;
    let configuration = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        members[0].replica_id,
        members
            .into_iter()
            .enumerate()
            .map(|(index, identity)| ConfigurationMember {
                identity,
                role: if index == 0 {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        write_quorum,
    );
    AdmittedAuthority {
        scale_up: None,
        secondary_removal: None,
        local_identity: local,
        transition_kind: None,
        previous_configuration: None,
        current_configuration: configuration,
        switchover_handoff: None,
    }
}

fn effect(sequence: u64, action: RuntimeEffectAction) -> RuntimeEffect {
    RuntimeEffect {
        operation_id: OperationId::new(format!("effect-{sequence}")),
        sequence,
        action,
    }
}

fn acknowledgement(
    authority: &AdmittedAuthority,
    receiver: ReplicaIdentity,
    lsn: i64,
) -> proto::ReplicationAck {
    proto::ReplicationAck {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        sender: Some(authority.primary_identity().clone().into()),
        receiver: Some(receiver.into()),
        epoch: Some(authority.current_configuration.epoch.into()),
        previous_configuration_id: authority
            .previous_configuration
            .as_ref()
            .map_or_else(String::new, |configuration| {
                configuration.configuration_id.to_string()
            }),
        current_configuration_id: authority.current_configuration.configuration_id.to_string(),
        received_lsn: lsn,
        applied_lsn: lsn,
        committed_lsn: 0,
        ..Default::default()
    }
}

#[tokio::test]
async fn direct_writes_require_primary_role_and_explicit_write_grant() {
    let local = identity(1, "single");
    let application = Arc::new(TestApplication::default());
    let store = Arc::new(MemoryAuthorityStore::default());
    let runtime = PodRuntime::new(local.clone(), application.clone(), store);

    assert!(matches!(
        runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("closed"),
                data: Bytes::from_static(b"closed")
            })
            .await,
        Err(RuntimeError::NotOpen)
    ));
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority(
                local.clone(),
                vec![local.clone()],
            ))),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    let primary_snapshot = runtime.snapshot().await;
    assert_eq!(primary_snapshot.role, ReplicaRole::Primary);
    assert_eq!(
        primary_snapshot.write_status,
        AccessStatus::ReconfigurationPending
    );

    assert!(matches!(
        runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("still-closed"),
                data: Bytes::from_static(b"still-closed")
            })
            .await,
        Err(RuntimeError::WriteClosed(
            AccessStatus::ReconfigurationPending
        ))
    ));

    runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let pending = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("committed"),
            data: Bytes::from_static(b"committed"),
        })
        .await
        .unwrap();
    assert!(pending.replication_items.is_empty());
    assert_eq!(
        application.durable_progress().await.unwrap().committed_lsn,
        1
    );
    assert_eq!(pending.committed().await.unwrap().committed_lsn, 1);

    runtime
        .apply_effect(effect(
            5,
            RuntimeEffectAction::SetAccessStatus {
                read: AccessStatus::Granted,
                write: AccessStatus::NoWriteQuorum,
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("no-quorum"),
                data: Bytes::from_static(b"no-quorum"),
            })
            .await,
        Err(RuntimeError::WriteClosed(AccessStatus::NoWriteQuorum))
    ));
}

#[tokio::test]
async fn partition_contract_reports_independent_access_load_and_fault() {
    let application = Arc::new(TestApplication::default());
    let runtime = PodRuntime::new(
        identity(1, "partition-contract"),
        application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
    );
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    let partition = application.partition.lock().unwrap().clone().unwrap();
    assert!(
        !partition
            .get_partition_information()
            .partition_id
            .is_empty()
    );
    assert_eq!(
        partition.get_read_status().await.unwrap(),
        AccessStatus::NotPrimary
    );
    assert_eq!(
        partition.get_write_status().await.unwrap(),
        AccessStatus::NotPrimary
    );
    partition
        .report_load(vec![LoadMetric {
            name: "queue-depth".into(),
            value: 3,
        }])
        .await
        .unwrap();
    partition.report_fault(FaultType::Transient).await.unwrap();
    let report = runtime.partition_report().await;
    assert_eq!(report.load_metrics[0].name, "queue-depth");
    assert_eq!(report.reported_fault, Some(FaultType::Transient));
    assert!(
        partition
            .report_load(vec![
                LoadMetric {
                    name: "duplicate".into(),
                    value: 1,
                },
                LoadMetric {
                    name: "duplicate".into(),
                    value: 2,
                },
            ])
            .await
            .is_err()
    );
    runtime
        .apply_effect(effect(2, RuntimeEffectAction::Close))
        .await
        .unwrap();
    assert!(partition.report_load(Vec::new()).await.is_err());
    assert!(partition.report_fault(FaultType::Permanent).await.is_err());
}

#[tokio::test]
async fn local_write_retries_the_same_lsn_after_definite_or_ambiguous_failure() {
    let local = identity(1, "single");
    let application = Arc::new(TestApplication::default());
    let store = Arc::new(MemoryAuthorityStore::default());
    let runtime = Arc::new(PodRuntime::new(
        local.clone(),
        application.clone(),
        store.clone(),
    ));
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority(local.clone(), vec![local]))),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();

    let first = ClientWrite {
        operation_id: OperationId::new("first"),
        data: Bytes::from_static(b"first"),
    };
    application.fail_apply.store(true, Ordering::SeqCst);
    assert!(matches!(
        runtime.data_plane().begin_write(first.clone()).await,
        Err(RuntimeError::Application(_))
    ));
    application.fail_apply.store(false, Ordering::SeqCst);
    let retry = runtime.data_plane().begin_write(first).await.unwrap();
    assert_eq!(retry.lsn, 1);
    retry.committed().await.unwrap();

    let second = ClientWrite {
        operation_id: OperationId::new("second"),
        data: Bytes::from_static(b"second"),
    };
    application.fail_after_apply.store(true, Ordering::SeqCst);
    assert!(matches!(
        runtime.data_plane().begin_write(second.clone()).await,
        Err(RuntimeError::Application(_))
    ));
    assert_eq!(application.durable_progress().await.unwrap().applied_lsn, 2);
    let retry = runtime.data_plane().begin_write(second).await.unwrap();
    assert_eq!(retry.lsn, 2);
    retry.committed().await.unwrap();

    let third = ClientWrite {
        operation_id: OperationId::new("third"),
        data: Bytes::from_static(b"third"),
    };
    application.pause_after_apply.store(true, Ordering::SeqCst);
    let runtime_task = runtime.clone();
    let third_task =
        tokio::spawn(async move { runtime_task.data_plane().begin_write(third.clone()).await });
    application.applied_notify.notified().await;
    third_task.abort();
    assert!(matches!(third_task.await, Err(error) if error.is_cancelled()));
    application.pause_after_apply.store(false, Ordering::SeqCst);
    application.resume_notify.notify_waiters();
    let retry = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("third"),
            data: Bytes::from_static(b"third"),
        })
        .await
        .unwrap();
    assert_eq!(retry.lsn, 3);
    retry.committed().await.unwrap();

    let fourth = ClientWrite {
        operation_id: OperationId::new("fourth"),
        data: Bytes::from_static(b"fourth"),
    };
    application.fail_commit.store(true, Ordering::SeqCst);
    assert!(matches!(
        runtime.data_plane().begin_write(fourth.clone()).await,
        Err(RuntimeError::Application(_))
    ));
    let retry = runtime.data_plane().begin_write(fourth).await.unwrap();
    assert_eq!(retry.lsn, 4);
    retry.committed().await.unwrap();

    let fifth = ClientWrite {
        operation_id: OperationId::new("fifth"),
        data: Bytes::from_static(b"fifth"),
    };
    application.pause_commit.store(true, Ordering::SeqCst);
    let runtime_task = runtime.clone();
    let fifth_task =
        tokio::spawn(async move { runtime_task.data_plane().begin_write(fifth).await });
    application.commit_notify.notified().await;
    fifth_task.abort();
    assert!(matches!(fifth_task.await, Err(error) if error.is_cancelled()));
    application.pause_commit.store(false, Ordering::SeqCst);
    application.resume_commit_notify.notify_waiters();
    let retry = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("fifth"),
            data: Bytes::from_static(b"fifth"),
        })
        .await
        .unwrap();
    assert_eq!(retry.lsn, 5);
    retry.committed().await.unwrap();

    let sixth = ClientWrite {
        operation_id: OperationId::new("sixth"),
        data: Bytes::from_static(b"sixth"),
    };
    store
        .fail_registered_write_once
        .store(true, Ordering::SeqCst);
    assert!(matches!(
        runtime.data_plane().begin_write(sixth.clone()).await,
        Err(RuntimeError::Application(_))
    ));
    assert!(matches!(
        runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("blocked-by-sixth"),
                data: Bytes::from_static(b"blocked"),
            })
            .await,
        Err(RuntimeError::LocalWritePending(_))
    ));
    let retry = runtime.data_plane().begin_write(sixth).await.unwrap();
    assert_eq!(retry.lsn, 6);
    retry.committed().await.unwrap();

    let seventh = ClientWrite {
        operation_id: OperationId::new("seventh"),
        data: Bytes::from_static(b"seventh"),
    };
    store.pause_registered_write.store(true, Ordering::SeqCst);
    let runtime_task = runtime.clone();
    let seventh_task =
        tokio::spawn(async move { runtime_task.data_plane().begin_write(seventh).await });
    store.registered_write_notify.notified().await;
    seventh_task.abort();
    assert!(matches!(seventh_task.await, Err(error) if error.is_cancelled()));
    store.pause_registered_write.store(false, Ordering::SeqCst);
    store.resume_registered_write_notify.notify_waiters();
    let retry = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("seventh"),
            data: Bytes::from_static(b"seventh"),
        })
        .await
        .unwrap();
    assert_eq!(retry.lsn, 7);
    retry.committed().await.unwrap();

    let eighth = ClientWrite {
        operation_id: OperationId::new("eighth"),
        data: Bytes::from_static(b"eighth"),
    };
    store.pause_committed_write.store(true, Ordering::SeqCst);
    let runtime_task = runtime.clone();
    let eighth_task =
        tokio::spawn(async move { runtime_task.data_plane().begin_write(eighth).await });
    store.committed_write_notify.notified().await;
    eighth_task.abort();
    assert!(matches!(eighth_task.await, Err(error) if error.is_cancelled()));
    store.pause_committed_write.store(false, Ordering::SeqCst);
    store.resume_committed_write_notify.notify_waiters();
    let retry = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("eighth"),
            data: Bytes::from_static(b"eighth"),
        })
        .await
        .unwrap();
    assert_eq!(retry.lsn, 8);
    retry.committed().await.unwrap();
    let ninth = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("ninth"),
            data: Bytes::from_static(b"ninth"),
        })
        .await
        .unwrap();
    assert_eq!(ninth.lsn, 9);
    ninth.committed().await.unwrap();
}

#[tokio::test]
async fn receiver_ack_requires_durable_authority_and_application_acceptance() {
    let primary = identity(1, "primary");
    let local = identity(2, "secondary");
    let admitted = authority(
        local.clone(),
        vec![primary.clone(), local.clone(), identity(3, "third")],
    );
    let application = Arc::new(TestApplication::default());
    let store = Arc::new(MemoryAuthorityStore::default());
    let runtime = PodRuntime::new(local.clone(), application.clone(), store.clone());
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    let item = proto::ReplicationItem {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        sender: Some(primary.into()),
        epoch: Some(admitted.current_configuration.epoch.into()),
        previous_configuration_id: String::new(),
        current_configuration_id: admitted.current_configuration.configuration_id.to_string(),
        lsn: 1,
        committed_lsn: 0,
        data: b"value".to_vec(),
        receiver: Some(local.clone().into()),
        ..Default::default()
    };

    assert!(matches!(
        runtime.data_plane().receive_replication(item.clone()).await,
        Err(RuntimeError::AuthorityNotAdmitted)
    ));
    assert!(application.applied.lock().unwrap().is_empty());

    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(admitted)),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::ActiveSecondary),
        ))
        .await
        .unwrap();
    assert_eq!(store.admit_count.load(Ordering::SeqCst), 1);

    let mut gap = item.clone();
    gap.lsn = 2;
    gap.data = b"gap".to_vec();
    assert!(matches!(
        runtime.data_plane().receive_replication(gap).await,
        Err(RuntimeError::InvalidReplication(_))
    ));
    assert!(application.applied.lock().unwrap().is_empty());

    application.fail_apply.store(true, Ordering::SeqCst);
    assert!(matches!(
        runtime
            .data_plane()
            .receive_replication(item.clone())
            .await
            .unwrap()
            .applied()
            .await,
        Err(RuntimeError::Application(_))
    ));
    application.fail_apply.store(false, Ordering::SeqCst);

    let delivery = runtime
        .data_plane()
        .receive_replication(item.clone())
        .await
        .unwrap();
    assert_eq!(delivery.received.received_lsn, 1);
    assert_eq!(delivery.received.applied_lsn, 0);
    let acknowledgement = delivery.applied().await.unwrap();
    assert_eq!(acknowledgement.applied_lsn, 1);
    assert_eq!(application.applied.lock().unwrap().len(), 1);

    let retry = runtime
        .data_plane()
        .receive_replication(item.clone())
        .await
        .unwrap()
        .applied()
        .await
        .unwrap();
    assert_eq!(retry.applied_lsn, 1);
    assert_eq!(application.applied.lock().unwrap().len(), 1);

    let mut conflicting = item;
    conflicting.data = b"different".to_vec();
    assert!(matches!(
        runtime
            .data_plane()
            .receive_replication(conflicting)
            .await
            .unwrap()
            .applied()
            .await,
        Err(RuntimeError::InvalidReplication(_))
    ));

    let mut committed = retry_item(&retry, local.clone());
    committed.lsn = 2;
    committed.committed_lsn = 1;
    committed.data = b"next".to_vec();
    runtime
        .data_plane()
        .receive_replication(committed)
        .await
        .unwrap()
        .applied()
        .await
        .unwrap();
    let snapshot = runtime.snapshot().await;
    assert_eq!(snapshot.current_progress, 2);
    assert_eq!(snapshot.committed_lsn, 1);
    assert_eq!(
        application.durable_progress().await.unwrap().committed_lsn,
        1
    );
    let older = retry_item(&retry, local);
    let acknowledgement = runtime
        .data_plane()
        .receive_replication(older)
        .await
        .unwrap()
        .applied()
        .await
        .unwrap();
    assert_eq!(acknowledgement.received_lsn, 2);
    assert_eq!(acknowledgement.applied_lsn, 2);
    kuberic_wire::validate_replication_ack(&acknowledgement).unwrap();
    assert_eq!(application.applied.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn new_authority_ack_does_not_credit_an_unverified_old_suffix() {
    let primary = identity(1, "primary");
    let members = vec![
        primary.clone(),
        identity(2, "second"),
        identity(3, "third"),
        identity(4, "fourth"),
        identity(5, "fifth"),
    ];
    let primary_authority = authority(primary.clone(), members.clone());
    let primary_application = Arc::new(TestApplication::default());
    primary_application.seed_operation(1, Bytes::from_static(b"one"));
    let primary_runtime = PodRuntime::new(
        primary,
        primary_application,
        Arc::new(MemoryAuthorityStore::default()),
    );
    primary_runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    primary_runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(primary_authority.clone())),
        ))
        .await
        .unwrap();
    primary_runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    primary_runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let pending = primary_runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("new-two"),
            data: Bytes::from_static(b"new-two"),
        })
        .await
        .unwrap();
    let pending_items = pending.replication_items.clone();

    for secondary in members.iter().skip(1).take(2) {
        let application = Arc::new(TestApplication::default());
        application.seed_operation(1, Bytes::from_static(b"one"));
        application.seed_operation(2, Bytes::from_static(b"old-two"));
        let local_authority = AdmittedAuthority {
            local_identity: secondary.clone(),
            ..primary_authority.clone()
        };
        let runtime = PodRuntime::new(
            secondary.clone(),
            application,
            Arc::new(MemoryAuthorityStore::default()),
        );
        runtime
            .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
            .await
            .unwrap();
        runtime
            .apply_effect(effect(
                2,
                RuntimeEffectAction::AdmitAuthority(Box::new(local_authority)),
            ))
            .await
            .unwrap();
        runtime
            .apply_effect(effect(
                3,
                RuntimeEffectAction::ChangeRole(ReplicaRole::ActiveSecondary),
            ))
            .await
            .unwrap();
        let mut replay = pending_items
            .iter()
            .find(|item| {
                item.receiver
                    .as_ref()
                    .is_some_and(|receiver| receiver.replica_id == secondary.replica_id.value())
            })
            .unwrap()
            .clone();
        replay.lsn = 1;
        replay.committed_lsn = 1;
        replay.data = b"one".to_vec();
        let ack = runtime
            .data_plane()
            .receive_replication(replay)
            .await
            .unwrap()
            .applied()
            .await
            .unwrap();
        assert_eq!(ack.applied_lsn, 1);
        primary_runtime
            .data_plane()
            .accept_acknowledgement(ack)
            .await
            .unwrap();

        let conflicting = pending_items
            .iter()
            .find(|item| {
                item.receiver
                    .as_ref()
                    .is_some_and(|receiver| receiver.replica_id == secondary.replica_id.value())
            })
            .unwrap()
            .clone();
        assert!(matches!(
            runtime
                .data_plane()
                .receive_replication(conflicting)
                .await
                .unwrap()
                .applied()
                .await,
            Err(RuntimeError::InvalidReplication(_))
        ));
    }

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), pending.committed())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn failover_does_not_inherit_previous_primary_verification_credit() {
    let old_primary = identity(1, "old-primary");
    let local = identity(2, "local");
    let new_primary = identity(3, "new-primary");
    let fourth = identity(4, "fourth");
    let fifth = identity(5, "fifth");
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        old_primary.replica_id,
        vec![
            ConfigurationMember {
                identity: old_primary.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: local.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: new_primary.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: fourth.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: fifth.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        3,
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        new_primary.replica_id,
        vec![
            ConfigurationMember {
                identity: old_primary,
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: local.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: new_primary,
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: fourth,
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: fifth,
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        3,
    );
    let store = Arc::new(MemoryAuthorityStore::default());
    store
        .record_replication_progress(&ReplicationProgress {
            fence: AuthorityFence {
                epoch: previous.epoch,
                previous_configuration_id: None,
                current_configuration_id: previous.configuration_id.clone(),
            },
            verified_lsn: 2,
        })
        .await
        .unwrap();
    let application = Arc::new(TestApplication::default());
    application.seed_operation(1, Bytes::from_static(b"one"));
    application.seed_operation(2, Bytes::from_static(b"old-two"));
    let runtime = PodRuntime::new(local.clone(), application, store);
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(AdmittedAuthority {
                scale_up: None,
                secondary_removal: None,
                local_identity: local,
                transition_kind: Some(TransitionKind::Failover),
                previous_configuration: Some(previous),
                current_configuration: current,
                switchover_handoff: None,
            })),
        ))
        .await
        .unwrap();
    assert_eq!(runtime.snapshot().await.verified_replication_lsn, Some(0));
    runtime
        .apply_effect(effect(3, RuntimeEffectAction::AuthorizeFailoverPrefix(1)))
        .await
        .unwrap();
    assert_eq!(runtime.snapshot().await.verified_replication_lsn, Some(1));
}

fn retry_item(
    acknowledgement: &proto::ReplicationAck,
    receiver: ReplicaIdentity,
) -> proto::ReplicationItem {
    proto::ReplicationItem {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        sender: acknowledgement.sender.clone(),
        receiver: Some(receiver.into()),
        epoch: acknowledgement.epoch,
        previous_configuration_id: acknowledgement.previous_configuration_id.clone(),
        current_configuration_id: acknowledgement.current_configuration_id.clone(),
        lsn: acknowledgement.received_lsn,
        committed_lsn: acknowledgement.committed_lsn,
        data: b"value".to_vec(),
        ..Default::default()
    }
}

#[tokio::test]
async fn effects_are_ordered_and_idempotent() {
    let local = identity(1, "single");
    let runtime = PodRuntime::new(
        local,
        Arc::new(TestApplication::default()),
        Arc::new(MemoryAuthorityStore::default()),
    );
    let open = effect(1, RuntimeEffectAction::Open(OpenMode::Existing));
    let first = runtime.apply_effect(open.clone()).await.unwrap();
    let duplicate = runtime.apply_effect(open).await.unwrap();
    assert_eq!(duplicate, first);
    assert!(matches!(
        runtime
            .apply_effect(effect(1, RuntimeEffectAction::Close))
            .await,
        Err(RuntimeError::EffectConflict { sequence: 1 })
    ));

    let close = effect(2, RuntimeEffectAction::Close);
    runtime.apply_effect(close).await.unwrap();
    assert_eq!(
        runtime
            .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing),))
            .await
            .unwrap(),
        first
    );
    assert!(matches!(
        runtime
            .apply_effect(effect(4, RuntimeEffectAction::Open(OpenMode::Existing),))
            .await,
        Err(RuntimeError::EffectOutOfOrder {
            expected: 3,
            observed: 4
        })
    ));
}

#[tokio::test]
async fn caller_supplies_the_control_plane() {
    let local = identity(1, "single");
    let runtime = PodRuntime::new(
        local,
        Arc::new(TestApplication::default()),
        Arc::new(MemoryAuthorityStore::default()),
    );
    let mut control_plane = TestControlPlane {
        effects: VecDeque::from([effect(1, RuntimeEffectAction::Open(OpenMode::Existing))]),
        published: Vec::new(),
    };

    runtime.serve(&mut control_plane).await.unwrap();
    assert_eq!(control_plane.published.len(), 1);
    assert!(control_plane.published[0].postcondition.open);
}

#[tokio::test]
async fn runtime_accepts_custom_primary_replicator_and_exposes_state_replicator_trait() {
    let local = identity(1, "custom");
    let opened = Arc::new(AtomicUsize::new(0));
    let role_changes = Arc::new(AtomicUsize::new(0));
    let epoch_updates = Arc::new(AtomicUsize::new(0));
    let application = Arc::new(TestApplication::default());
    let events = application.events.clone();
    let runtime = runtime_with_factory(
        local.clone(),
        application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
        CountingFactory {
            storage: Arc::downgrade(&application),
            opened: opened.clone(),
            role_changes: role_changes.clone(),
            epoch_updates,
            events: events.clone(),
            fail_change_role: Arc::new(AtomicBool::new(false)),
            fail_close: Arc::new(AtomicBool::new(false)),
        },
    )
    .unwrap();
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::New)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority(local.clone(), vec![local]))),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();

    let state_replicator = application
        .state_replicator
        .lock()
        .unwrap()
        .clone()
        .unwrap();
    assert_eq!(
        state_replicator
            .replicate(Bytes::from_static(b"custom"))
            .await
            .unwrap(),
        1
    );
    assert_eq!(application.streams_taken.load(Ordering::SeqCst), 2);
    assert_eq!(opened.load(Ordering::SeqCst), 1);
    assert_eq!(role_changes.load(Ordering::SeqCst), 1);
    assert_eq!(
        events.lock().unwrap().as_slice(),
        [
            "service.open",
            "replicator.open",
            "replicator.change_role",
            "replicator.update_epoch",
            "provider.update_epoch",
            "service.change_role",
        ]
    );
}

#[tokio::test]
async fn secondary_authority_dispatches_replicator_and_provider_epoch_before_role() {
    let primary = identity(1, "primary");
    let local = identity(2, "secondary");
    let application = Arc::new(TestApplication::default());
    let events = application.events.clone();
    let epoch_updates = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with_factory(
        local.clone(),
        application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
        CountingFactory {
            storage: Arc::downgrade(&application),
            opened: Arc::new(AtomicUsize::new(0)),
            role_changes: Arc::new(AtomicUsize::new(0)),
            epoch_updates: epoch_updates.clone(),
            events: events.clone(),
            fail_change_role: Arc::new(AtomicBool::new(false)),
            fail_close: Arc::new(AtomicBool::new(false)),
        },
    )
    .unwrap();
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::New)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority(
                local,
                vec![primary, identity(2, "secondary"), identity(3, "third")],
            ))),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::ActiveSecondary),
        ))
        .await
        .unwrap();

    assert_eq!(epoch_updates.load(Ordering::SeqCst), 1);
    assert_eq!(
        events.lock().unwrap().as_slice(),
        [
            "service.open",
            "replicator.open",
            "replicator.update_epoch",
            "provider.update_epoch",
            "replicator.change_role",
            "service.change_role",
        ]
    );
}

#[tokio::test]
async fn failed_or_cancelled_secondary_epoch_admission_stays_write_fenced() {
    let old_primary = identity(1, "old-primary");
    let new_primary = identity(2, "new-primary");
    let third = identity(3, "third");
    let previous = authority(
        old_primary.clone(),
        vec![old_primary.clone(), new_primary.clone(), third.clone()],
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        new_primary.replica_id,
        vec![
            ConfigurationMember {
                identity: old_primary.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: new_primary.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: third,
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    );
    let demoted_authority = AdmittedAuthority {
        scale_up: None,
        secondary_removal: None,
        local_identity: old_primary.clone(),
        transition_kind: Some(TransitionKind::Failover),
        previous_configuration: Some(previous.current_configuration.clone()),
        current_configuration: current,
        switchover_handoff: None,
    };
    let application = Arc::new(TestApplication::default());
    let runtime = Arc::new(PodRuntime::new(
        old_primary,
        application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
    ));
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(previous)),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let pending = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("old-authority-pending"),
            data: Bytes::from_static(b"pending"),
        })
        .await
        .unwrap();
    let delayed_item = pending
        .replication_items
        .iter()
        .find(|item| {
            item.receiver
                .as_ref()
                .is_some_and(|receiver| receiver.instance_id == new_primary.instance_id.as_str())
        })
        .unwrap()
        .clone();

    application.pause_update_epoch.store(true, Ordering::SeqCst);
    let runtime_task = runtime.clone();
    let authority_for_task = demoted_authority.clone();
    let admission = tokio::spawn(async move {
        runtime_task
            .apply_effect(effect(
                5,
                RuntimeEffectAction::AdmitAuthority(Box::new(authority_for_task)),
            ))
            .await
    });
    application.update_epoch_notify.notified().await;
    assert!(matches!(
        pending.committed().await,
        Err(RuntimeError::WriteClosed(
            AccessStatus::ReconfigurationPending
        ))
    ));
    admission.abort();
    assert!(matches!(admission.await, Err(error) if error.is_cancelled()));
    application
        .pause_update_epoch
        .store(false, Ordering::SeqCst);
    application.resume_update_epoch_notify.notify_waiters();

    application.fail_update_epoch.store(true, Ordering::SeqCst);
    assert!(matches!(
        runtime
            .apply_effect(effect(
                5,
                RuntimeEffectAction::AdmitAuthority(Box::new(demoted_authority)),
            ))
            .await,
        Err(RuntimeError::Application(_))
    ));
    assert!(matches!(
        runtime
            .data_plane()
            .accept_acknowledgement(proto::ReplicationAck {
                protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                sender: delayed_item.sender,
                receiver: delayed_item.receiver,
                epoch: delayed_item.epoch,
                previous_configuration_id: delayed_item.previous_configuration_id,
                current_configuration_id: delayed_item.current_configuration_id,
                received_lsn: delayed_item.lsn,
                applied_lsn: delayed_item.lsn,
                committed_lsn: 0,
                ..Default::default()
            })
            .await,
        Err(RuntimeError::AuthorityMismatch(_))
    ));
    assert_eq!(
        runtime.snapshot().await.write_status,
        AccessStatus::ReconfigurationPending
    );
}

#[tokio::test]
async fn close_fences_pending_client_writes_before_ack_processing() {
    let local = identity(1, "primary");
    let secondary = identity(2, "secondary");
    let third = identity(3, "third");
    let application = Arc::new(TestApplication::default());
    let runtime = Arc::new(PodRuntime::new(
        local.clone(),
        application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
    ));
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority(
                local,
                vec![identity(1, "primary"), secondary, third],
            ))),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let pending = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("pending"),
            data: Bytes::from_static(b"pending"),
        })
        .await
        .unwrap();
    let item = pending.replication_items[0].clone();

    application.pause_close.store(true, Ordering::SeqCst);
    let runtime_task = runtime.clone();
    let close_task = tokio::spawn(async move {
        runtime_task
            .apply_effect(effect(5, RuntimeEffectAction::Close))
            .await
    });
    application.close_notify.notified().await;
    assert!(matches!(
        pending.committed().await,
        Err(RuntimeError::WriteClosed(
            AccessStatus::ReconfigurationPending
        ))
    ));
    application.pause_close.store(false, Ordering::SeqCst);
    application.resume_close_notify.notify_waiters();
    close_task.await.unwrap().unwrap();
    assert!(matches!(
        runtime
            .data_plane()
            .accept_acknowledgement(proto::ReplicationAck {
                protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                sender: item.sender,
                receiver: item.receiver,
                epoch: item.epoch,
                previous_configuration_id: item.previous_configuration_id,
                current_configuration_id: item.current_configuration_id,
                received_lsn: item.lsn,
                applied_lsn: item.lsn,
                committed_lsn: 0,
                ..Default::default()
            })
            .await,
        Err(RuntimeError::Closed)
    ));
}

#[tokio::test]
async fn switchover_preparation_fences_pending_writes_and_returns_applied_boundary() {
    let local = identity(1, "primary");
    let secondary = identity(2, "secondary");
    let third = identity(3, "third");
    let application = Arc::new(TestApplication::default());
    let store = Arc::new(MemoryAuthorityStore::default());
    let runtime = Arc::new(PodRuntime::new(
        local.clone(),
        application.clone(),
        store.clone(),
    ));
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority(
                local.clone(),
                vec![local.clone(), secondary.clone(), third],
            ))),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let pending = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("pending-switchover"),
            data: Bytes::from_static(b"pending-switchover"),
        })
        .await
        .unwrap();
    let pending_lsn = pending.lsn;
    let starting_configuration = runtime
        .snapshot()
        .await
        .authority
        .unwrap()
        .current_configuration;
    let prepare = effect(
        5,
        RuntimeEffectAction::PrepareSwitchover {
            preparation_generation: 1,
            request_id: SwitchoverRequestId::new("request-1"),
            source: local.clone(),
            target: secondary.clone(),
            starting_configuration_id: starting_configuration.configuration_id,
            starting_epoch: starting_configuration.epoch,
        },
    );
    let result = runtime.apply_effect(prepare.clone()).await.unwrap();

    assert_eq!(
        result.postcondition.write_status,
        AccessStatus::ReconfigurationPending
    );
    assert!(result.postcondition.current_progress >= pending_lsn);
    assert!(matches!(
        pending.committed().await,
        Err(RuntimeError::WriteClosed(
            AccessStatus::ReconfigurationPending
        ))
    ));
    assert!(matches!(
        runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("after-switchover"),
                data: Bytes::from_static(b"after-switchover"),
            })
            .await,
        Err(RuntimeError::WriteClosed(
            AccessStatus::ReconfigurationPending
        ))
    ));
    let restarted = PodRuntime::new(local, application, store);
    restarted
        .reconstruct(
            OpenMode::Existing,
            ReplicaRole::Primary,
            AccessStatus::Granted,
            AccessStatus::ReconfigurationPending,
            None,
        )
        .await
        .unwrap();
    let replayed = restarted.apply_effect(prepare.clone()).await.unwrap();
    assert_eq!(
        replayed.postcondition.current_progress,
        result.postcondition.current_progress
    );
    assert_eq!(runtime.apply_effect(prepare).await.unwrap(), result);
}

#[tokio::test]
async fn switchover_preparation_boundary_covers_acknowledged_writes() {
    let local = identity(1, "primary");
    let secondary = identity(2, "secondary");
    let third = identity(3, "third");
    let admitted = authority(local.clone(), vec![local.clone(), secondary.clone(), third]);
    let application = Arc::new(TestApplication::default());
    let runtime = Arc::new(PodRuntime::new(
        local.clone(),
        application,
        Arc::new(MemoryAuthorityStore::default()),
    ));
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(admitted.clone())),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let pending = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("committed-before-switchover"),
            data: Bytes::from_static(b"committed-before-switchover"),
        })
        .await
        .unwrap();
    let lsn = pending.lsn;
    runtime
        .data_plane()
        .accept_acknowledgement(acknowledgement(&admitted, secondary.clone(), lsn))
        .await
        .unwrap();
    let receipt = pending.committed().await.unwrap();
    assert_eq!(receipt.lsn, lsn);
    assert!(receipt.committed_lsn >= lsn);

    let result = runtime
        .apply_effect(effect(
            5,
            RuntimeEffectAction::PrepareSwitchover {
                preparation_generation: 1,
                request_id: SwitchoverRequestId::new("request-committed"),
                source: local,
                target: secondary,
                starting_configuration_id: admitted.current_configuration.configuration_id.clone(),
                starting_epoch: admitted.current_configuration.epoch,
            },
        ))
        .await
        .unwrap();
    assert!(result.postcondition.current_progress >= lsn);
    assert!(result.postcondition.committed_lsn >= lsn);
    assert_eq!(
        result.postcondition.write_status,
        AccessStatus::ReconfigurationPending
    );
}

async fn recovery_action(runtime: &PodRuntime, sequence: &mut u64, action: RuntimeEffectAction) {
    runtime
        .apply_effect(effect(*sequence, action))
        .await
        .unwrap();
    *sequence += 1;
}

async fn replicate_recovery_write(
    runtimes: &[Arc<PodRuntime>],
    primary: usize,
    write: ClientWrite,
    acknowledge: bool,
) -> kuberic_agent::hosting::PendingWrite {
    let pending = runtimes[primary]
        .data_plane()
        .begin_write(write)
        .await
        .unwrap();
    for item in &pending.replication_items {
        let index = item.receiver.as_ref().unwrap().replica_id as usize - 1;
        let ack = runtimes[index]
            .data_plane()
            .receive_replication(item.clone())
            .await
            .unwrap()
            .applied()
            .await
            .unwrap();
        if acknowledge {
            runtimes[primary]
                .data_plane()
                .accept_acknowledgement(ack)
                .await
                .unwrap();
        }
    }
    pending
}

#[tokio::test]
async fn switchover_preparation_materializes_reserved_identity_before_certifying_handoff() {
    let local = identity(1, "source");
    let target = identity(2, "target");
    let application = Arc::new(TestApplication::default());
    let store = Arc::new(MemoryAuthorityStore::default());
    let runtime = Arc::new(PodRuntime::new(
        local.clone(),
        application.clone(),
        store.clone(),
    ));
    let authority = authority(local.clone(), vec![local.clone(), target.clone()]);
    let mut sequence = 1;
    for action in [
        RuntimeEffectAction::Open(OpenMode::Existing),
        RuntimeEffectAction::AdmitAuthority(Box::new(authority.clone())),
        RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
    ] {
        recovery_action(&runtime, &mut sequence, action).await;
    }
    let original = ClientWrite {
        operation_id: OperationId::new("reserved-original"),
        data: Bytes::from_static(b"reserved-original-data"),
    };
    application.fail_apply.store(true, Ordering::SeqCst);
    assert!(
        runtime
            .data_plane()
            .begin_write(original.clone())
            .await
            .is_err()
    );
    assert_eq!(
        store
            .load_local_write(&original.operation_id)
            .await
            .unwrap()
            .unwrap()
            .phase,
        kuberic_runtime_internal::authority::LocalWritePhase::Reserved
    );
    application.fail_apply.store(false, Ordering::SeqCst);
    recovery_action(
        &runtime,
        &mut sequence,
        RuntimeEffectAction::PrepareSwitchover {
            preparation_generation: 1,
            request_id: SwitchoverRequestId::new("reserved-handoff"),
            source: local,
            target: target.clone(),
            starting_configuration_id: authority.current_configuration.configuration_id.clone(),
            starting_epoch: authority.current_configuration.epoch,
        },
    )
    .await;
    assert_eq!(runtime.snapshot().await.current_progress, 1);
    assert_eq!(runtime.snapshot().await.committed_lsn, 0);
    assert_eq!(application.applied.lock().unwrap()[&1].data, original.data);
    let plane = runtime.data_plane();
    plane.next_outbound().await.unwrap();
    let granting = runtime.clone();
    let grant = tokio::spawn(async move {
        granting
            .apply_effect(effect(
                sequence,
                RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
            ))
            .await
    });
    let Some(OutboundReplication::Replication(replay)) = plane.next_outbound().await else {
        panic!("recovery replay")
    };
    assert_eq!(replay.data, original.data);
    assert_ne!(runtime.snapshot().await.write_status, AccessStatus::Granted);
    plane
        .accept_acknowledgement(acknowledgement(&authority, target.clone(), 1))
        .await
        .unwrap();
    timeout(Duration::from_secs(1), grant)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let fresh = plane
        .begin_write(ClientWrite {
            operation_id: OperationId::new("fresh-after-reserved"),
            data: Bytes::from_static(b"fresh"),
        })
        .await
        .unwrap();
    plane
        .accept_acknowledgement(acknowledgement(&authority, target, 2))
        .await
        .unwrap();
    assert_eq!(fresh.committed().await.unwrap().committed_lsn, 2);
}

#[tokio::test]
async fn switchover_recovery_commits_different_writes_after_restoration_compensation_and_return() {
    for movement in ["restore", "compensate", "return"] {
        for restart in [false, true] {
            let identities = vec![
                identity(1, "source"),
                identity(2, "target"),
                identity(3, "third"),
            ];
            let starting =
                authority(identities[0].clone(), identities.clone()).current_configuration;
            let mut runtimes = Vec::new();
            let mut stores = Vec::new();
            let mut applications = Vec::new();
            let mut sequences = [1; 3];
            for index in 0..3 {
                let application = Arc::new(TestApplication::default());
                let store = Arc::new(MemoryAuthorityStore::default());
                let runtime = Arc::new(PodRuntime::new(
                    identities[index].clone(),
                    application.clone(),
                    store.clone(),
                ));
                for action in [
                    RuntimeEffectAction::Open(OpenMode::Existing),
                    RuntimeEffectAction::AdmitAuthority(Box::new(AdmittedAuthority {
                        scale_up: None,
                        secondary_removal: None,
                        local_identity: identities[index].clone(),
                        transition_kind: None,
                        previous_configuration: None,
                        current_configuration: starting.clone(),
                        switchover_handoff: None,
                    })),
                    RuntimeEffectAction::ChangeRole(starting.members[index].role),
                    RuntimeEffectAction::SetWriteStatus(if index == 0 {
                        AccessStatus::Granted
                    } else {
                        AccessStatus::NotPrimary
                    }),
                ] {
                    recovery_action(&runtime, &mut sequences[index], action).await;
                }
                runtimes.push(runtime);
                stores.push(store);
                applications.push(application);
            }
            let original = ClientWrite {
                operation_id: OperationId::new("interrupted-original"),
                data: Bytes::from_static(b"original-data"),
            };
            let pending = replicate_recovery_write(&runtimes, 0, original.clone(), false).await;
            let mut handoff = SwitchoverHandoff {
                preparation_generation: 1,
                preparation_operation_id: OperationId::new("prepare-original"),
                request_id: SwitchoverRequestId::new("move-out"),
                source: identities[0].clone(),
                target: identities[1].clone(),
                starting_configuration_id: starting.configuration_id.clone(),
                starting_epoch: starting.epoch,
                handoff_lsn: 1,
            };
            recovery_action(
                &runtimes[0],
                &mut sequences[0],
                RuntimeEffectAction::PrepareSwitchover {
                    preparation_generation: 1,
                    request_id: handoff.request_id.clone(),
                    source: handoff.source.clone(),
                    target: handoff.target.clone(),
                    starting_configuration_id: starting.configuration_id.clone(),
                    starting_epoch: starting.epoch,
                },
            )
            .await;
            assert!(matches!(
                pending.committed().await,
                Err(RuntimeError::WriteClosed(_))
            ));
            assert_eq!(
                stores[0]
                    .load_local_write(&original.operation_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .phase,
                kuberic_runtime_internal::authority::LocalWritePhase::Registered
            );

            let mut selected = starting.clone();
            if movement != "restore" {
                for leg in 0..2 {
                    let primary = if leg == 0 { 1 } else { 0 };
                    let next = ConfigurationDescriptor::new(
                        Epoch::new(
                            starting.epoch.data_loss_number,
                            starting.epoch.configuration_number + leg + 1,
                        ),
                        identities[primary].replica_id,
                        identities
                            .iter()
                            .enumerate()
                            .map(|(index, identity)| ConfigurationMember {
                                identity: identity.clone(),
                                role: if index == primary {
                                    ReplicaRole::Primary
                                } else {
                                    ReplicaRole::ActiveSecondary
                                },
                            })
                            .collect(),
                        2,
                    );
                    for current_only in [false, true] {
                        let order = if primary == 1 { [0, 2, 1] } else { [1, 2, 0] };
                        for index in order {
                            recovery_action(
                                &runtimes[index],
                                &mut sequences[index],
                                RuntimeEffectAction::AdmitAuthority(Box::new(AdmittedAuthority {
                                    scale_up: None,
                                    secondary_removal: None,
                                    local_identity: identities[index].clone(),
                                    transition_kind: (!current_only)
                                        .then_some(TransitionKind::PlannedSwitchover),
                                    previous_configuration: (!current_only)
                                        .then(|| selected.clone()),
                                    current_configuration: next.clone(),
                                    switchover_handoff: Some(handoff.clone()),
                                })),
                            )
                            .await;
                            if !current_only {
                                recovery_action(
                                    &runtimes[index],
                                    &mut sequences[index],
                                    RuntimeEffectAction::ChangeRole(next.members[index].role),
                                )
                                .await;
                            }
                        }
                    }
                    selected = next;
                    if leg == 0 && movement == "return" {
                        recovery_action(
                            &runtimes[1],
                            &mut sequences[1],
                            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
                        )
                        .await;
                        let moved = replicate_recovery_write(
                            &runtimes,
                            1,
                            ClientWrite {
                                operation_id: OperationId::new("on-requested-target"),
                                data: Bytes::from_static(b"target-data"),
                            },
                            true,
                        )
                        .await;
                        assert_eq!(moved.committed().await.unwrap().committed_lsn, 2);
                        handoff = SwitchoverHandoff {
                            preparation_generation: 2,
                            preparation_operation_id: OperationId::new("prepare-return"),
                            request_id: SwitchoverRequestId::new("move-back"),
                            source: identities[1].clone(),
                            target: identities[0].clone(),
                            starting_configuration_id: selected.configuration_id.clone(),
                            starting_epoch: selected.epoch,
                            handoff_lsn: 2,
                        };
                        recovery_action(
                            &runtimes[1],
                            &mut sequences[1],
                            RuntimeEffectAction::PrepareSwitchover {
                                preparation_generation: 2,
                                request_id: handoff.request_id.clone(),
                                source: handoff.source.clone(),
                                target: handoff.target.clone(),
                                starting_configuration_id: selected.configuration_id.clone(),
                                starting_epoch: selected.epoch,
                            },
                        )
                        .await;
                    }
                }
            }
            if restart {
                runtimes[0].abort();
                let cold = Arc::new(PodRuntime::new(
                    identities[0].clone(),
                    applications[0].clone(),
                    stores[0].clone(),
                ));
                cold.reconstruct(
                    OpenMode::Existing,
                    ReplicaRole::Primary,
                    AccessStatus::ReconfigurationPending,
                    AccessStatus::ReconfigurationPending,
                    None,
                )
                .await
                .unwrap();
                runtimes[0] = cold;
            }
            let grant = effect(
                sequences[0],
                RuntimeEffectAction::SetAccessStatus {
                    read: AccessStatus::Granted,
                    write: AccessStatus::Granted,
                },
            );
            if movement != "return" {
                // Cancel after re-registration but before quorum; retry the same effect.
                assert!(
                    timeout(
                        Duration::from_millis(20),
                        runtimes[0].apply_effect(grant.clone())
                    )
                    .await
                    .is_err()
                );
                assert_ne!(
                    runtimes[0].snapshot().await.write_status,
                    AccessStatus::Granted
                );
                assert!(
                    runtimes[0]
                        .data_plane()
                        .begin_write(ClientWrite {
                            operation_id: OperationId::new("must-remain-closed"),
                            data: Bytes::new(),
                        })
                        .await
                        .is_err()
                );
            }
            let source = runtimes[0].clone();
            let mut grant_task = tokio::spawn(async move { source.apply_effect(grant).await });
            let source_data = runtimes[0].data_plane();
            timeout(Duration::from_secs(2), async {
                loop {
                    tokio::select! {
                        result = &mut grant_task => { result.unwrap().unwrap(); break; }
                        outbound = source_data.next_outbound() => {
                            let Some(OutboundReplication::Replication(item)) = outbound else { panic!("expected recovery replication") };
                            if item.current_configuration_id != selected.configuration_id.as_str() { continue; }
                            assert_eq!(item.lsn, 1);
                            assert_eq!(item.data, original.data);
                            assert_ne!(runtimes[0].snapshot().await.write_status, AccessStatus::Granted);
                            let index = item.receiver.as_ref().unwrap().replica_id as usize - 1;
                            let ack = runtimes[index].data_plane().receive_replication(item).await.unwrap().applied().await.unwrap();
                            runtimes[0].data_plane().accept_acknowledgement(ack).await.unwrap();
                        }
                    }
                }
            }).await.unwrap();
            for runtime in &runtimes[1..] {
                assert_ne!(runtime.snapshot().await.write_status, AccessStatus::Granted);
            }
            let committed = stores[0]
                .load_local_write(&original.operation_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                committed.phase,
                kuberic_runtime_internal::authority::LocalWritePhase::Committed
            );
            assert_eq!(committed.data, original.data);
            assert_eq!(committed.lsn, 1);
            let fresh = replicate_recovery_write(
                &runtimes,
                0,
                ClientWrite {
                    operation_id: OperationId::new("fresh-after-recovery"),
                    data: Bytes::from_static(b"new-different-data"),
                },
                true,
            )
            .await;
            assert_eq!(
                fresh.committed().await.unwrap().committed_lsn,
                if movement == "return" { 3 } else { 2 }
            );
            assert!(
                runtimes[0]
                    .data_plane()
                    .begin_write(ClientWrite {
                        data: Bytes::from_static(b"changed-original"),
                        ..original
                    })
                    .await
                    .is_err()
            );
        }
    }
}

#[tokio::test]
async fn switchover_preparation_races_ack_without_success_outside_boundary() {
    let local = identity(1, "primary");
    let secondary = identity(2, "secondary");
    let third = identity(3, "third");
    let admitted = authority(local.clone(), vec![local.clone(), secondary.clone(), third]);
    let runtime = Arc::new(PodRuntime::new(
        local.clone(),
        Arc::new(TestApplication::default()),
        Arc::new(MemoryAuthorityStore::default()),
    ));
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(admitted.clone())),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let pending = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("racing-switchover"),
            data: Bytes::from_static(b"racing-switchover"),
        })
        .await
        .unwrap();
    let lsn = pending.lsn;
    let prepare_runtime = runtime.clone();
    let prepare = effect(
        5,
        RuntimeEffectAction::PrepareSwitchover {
            preparation_generation: 1,
            request_id: SwitchoverRequestId::new("request-race"),
            source: local,
            target: secondary.clone(),
            starting_configuration_id: admitted.current_configuration.configuration_id.clone(),
            starting_epoch: admitted.current_configuration.epoch,
        },
    );
    let ack_runtime = runtime.clone();
    let acknowledgement = acknowledgement(&admitted, secondary, lsn);
    let (prepared, acknowledged) = tokio::join!(
        async move { prepare_runtime.apply_effect(prepare).await },
        async move {
            ack_runtime
                .data_plane()
                .accept_acknowledgement(acknowledgement)
                .await
        }
    );
    let prepared = prepared.unwrap();
    acknowledged.unwrap();
    match pending.committed().await {
        Ok(receipt) => {
            assert!(prepared.postcondition.current_progress >= receipt.lsn);
            assert!(prepared.postcondition.committed_lsn >= receipt.committed_lsn);
        }
        Err(RuntimeError::WriteClosed(AccessStatus::ReconfigurationPending)) => {}
        other => panic!("unexpected racing write result: {other:?}"),
    }
}

#[tokio::test]
async fn switchover_drain_serializes_durable_boundaries_and_delayed_direct_clients() {
    for boundary in ["apply", "registered", "commit", "committed-journal"] {
        let source = identity(1, "source");
        let target = identity(2, "target");
        let starting = authority(
            source.clone(),
            vec![source.clone(), target.clone(), identity(3, "third")],
        );
        let application = Arc::new(TestApplication::default());
        let store = Arc::new(MemoryAuthorityStore::default());
        let runtime = Arc::new(PodRuntime::new(
            source.clone(),
            application.clone(),
            store.clone(),
        ));
        for (index, action) in [
            RuntimeEffectAction::Open(OpenMode::Existing),
            RuntimeEffectAction::AdmitAuthority(Box::new(starting.clone())),
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ]
        .into_iter()
        .enumerate()
        {
            runtime
                .apply_effect(effect(index as u64 + 1, action))
                .await
                .unwrap();
        }
        let retained_client = runtime.data_plane();
        let (pause, entered, resume) = match boundary {
            "apply" => (
                &application.pause_after_apply,
                &application.applied_notify,
                &application.resume_notify,
            ),
            "registered" => (
                &store.pause_registered_write,
                &store.registered_write_notify,
                &store.resume_registered_write_notify,
            ),
            "commit" => (
                &application.pause_commit,
                &application.commit_notify,
                &application.resume_commit_notify,
            ),
            _ => (
                &store.pause_committed_write,
                &store.committed_write_notify,
                &store.resume_committed_write_notify,
            ),
        };
        pause.store(true, Ordering::SeqCst);
        let writer_runtime = runtime.clone();
        let ack = acknowledgement(&starting, target.clone(), 1);
        let delayed_ack = ack.clone();
        let commit_first = matches!(boundary, "commit" | "committed-journal");
        let writer = tokio::spawn(async move {
            let pending = writer_runtime
                .data_plane()
                .begin_write(ClientWrite {
                    operation_id: OperationId::new("drain-race"),
                    data: Bytes::from_static(b"durable-before-revocation"),
                })
                .await
                .unwrap();
            if commit_first {
                writer_runtime
                    .data_plane()
                    .accept_acknowledgement(ack)
                    .await
                    .unwrap();
            }
            pending
        });
        timeout(Duration::from_secs(3), entered.notified())
            .await
            .unwrap();
        let preparation = effect(
            5,
            RuntimeEffectAction::PrepareSwitchover {
                preparation_generation: 1,
                request_id: SwitchoverRequestId::new("drain-race"),
                source: source.clone(),
                target: target.clone(),
                starting_configuration_id: starting.current_configuration.configuration_id.clone(),
                starting_epoch: starting.current_configuration.epoch,
            },
        );
        let mut prepare = Box::pin(runtime.apply_effect(preparation.clone()));
        // Polling registers the preparation behind the blocked durable operation,
        // before a retained client can queue another admission.
        assert!(futures::poll!(prepare.as_mut()).is_pending());
        let mut late_write = Box::pin(retained_client.begin_write(ClientWrite {
            operation_id: OperationId::new("outside-certificate"),
            data: Bytes::from_static(b"must-not-apply"),
        }));
        assert!(futures::poll!(late_write.as_mut()).is_pending());
        pause.store(false, Ordering::SeqCst);
        resume.notify_one();
        let (prepared, late_result) = timeout(Duration::from_secs(3), async {
            tokio::join!(prepare, late_write)
        })
        .await
        .unwrap();
        let prepared = prepared.unwrap();
        assert!(late_result.is_err(), "{boundary}");
        let pending = writer.await.unwrap();
        let handoff = SwitchoverHandoff {
            preparation_generation: 1,
            preparation_operation_id: preparation.operation_id.clone(),
            request_id: SwitchoverRequestId::new("drain-race"),
            source: source.clone(),
            target: target.clone(),
            starting_configuration_id: starting.current_configuration.configuration_id.clone(),
            starting_epoch: starting.current_configuration.epoch,
            handoff_lsn: prepared.postcondition.current_progress,
        };
        assert_eq!(handoff.handoff_lsn, 1);
        let current = ConfigurationDescriptor::new(
            Epoch::new(0, 2),
            target.replica_id,
            starting
                .current_configuration
                .members
                .iter()
                .map(|member| ConfigurationMember {
                    identity: member.identity.clone(),
                    role: if member.identity == target {
                        ReplicaRole::Primary
                    } else {
                        ReplicaRole::ActiveSecondary
                    },
                })
                .collect(),
            2,
        );
        runtime
            .apply_effect(effect(
                6,
                RuntimeEffectAction::AdmitAuthority(Box::new(AdmittedAuthority {
                    scale_up: None,
                    secondary_removal: None,
                    local_identity: source,
                    transition_kind: Some(TransitionKind::PlannedSwitchover),
                    previous_configuration: Some(starting.current_configuration),
                    current_configuration: current,
                    switchover_handoff: Some(handoff.clone()),
                })),
            ))
            .await
            .unwrap();
        runtime
            .apply_effect(effect(
                7,
                RuntimeEffectAction::ChangeRole(ReplicaRole::ActiveSecondary),
            ))
            .await
            .unwrap();
        assert!(
            retained_client
                .accept_acknowledgement(delayed_ack)
                .await
                .is_err()
        );
        // Deliberately deliver the client result only after configuration and role change.
        match pending.committed().await {
            Ok(receipt) => {
                assert!(commit_first, "{boundary}");
                assert!(receipt.lsn <= handoff.handoff_lsn);
                assert!(receipt.committed_lsn <= prepared.postcondition.committed_lsn);
            }
            Err(RuntimeError::WriteClosed(AccessStatus::ReconfigurationPending)) => {
                assert!(!commit_first, "{boundary}");
            }
            other => panic!("unexpected delayed completion at {boundary}: {other:?}"),
        }
        assert!(
            retained_client
                .begin_write(ClientWrite {
                    operation_id: OperationId::new("retained-after-demotion"),
                    data: Bytes::from_static(b"must-not-apply"),
                })
                .await
                .is_err()
        );
        assert_eq!(application.applied.lock().unwrap().len(), 1);
        assert_eq!(
            application.applied.lock().unwrap()[&1].data,
            Bytes::from_static(b"durable-before-revocation")
        );
        assert_eq!(runtime.apply_effect(preparation).await.unwrap(), prepared);
        assert_eq!(runtime.snapshot().await.role, ReplicaRole::ActiveSecondary);
        assert_ne!(runtime.snapshot().await.write_status, AccessStatus::Granted);
        assert!(
            !application
                .events
                .lock()
                .unwrap()
                .iter()
                .any(|event| event == "provider.on_data_loss")
        );
    }
}

#[tokio::test]
async fn failed_demotion_callback_preserves_completed_role_and_transition_stage() {
    let local = identity(1, "primary");
    let application = Arc::new(TestApplication::default());
    let runtime = PodRuntime::new(
        local.clone(),
        application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
    );
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority(
                local,
                vec![
                    identity(1, "primary"),
                    identity(2, "secondary"),
                    identity(3, "third"),
                ],
            ))),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let pending = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("demotion-pending"),
            data: Bytes::from_static(b"pending"),
        })
        .await
        .unwrap();
    application.fail_change_role.store(true, Ordering::SeqCst);
    assert!(matches!(
        runtime
            .apply_effect(effect(
                5,
                RuntimeEffectAction::ChangeRole(ReplicaRole::ActiveSecondary),
            ))
            .await,
        Err(RuntimeError::Application(_))
    ));
    assert!(matches!(
        pending.committed().await,
        Err(RuntimeError::WriteClosed(
            AccessStatus::ReconfigurationPending
        ))
    ));
    let snapshot = runtime.snapshot().await;
    assert_eq!(snapshot.role, ReplicaRole::Primary);
    assert_eq!(
        snapshot.role_transition,
        Some(kuberic_runtime_internal::effects::RoleTransition {
            completed_role: ReplicaRole::Primary,
            target_role: ReplicaRole::ActiveSecondary,
            replicator_completed: true,
            epoch_completed: true,
            application_completed: false,
        })
    );
    assert_eq!(snapshot.write_status, AccessStatus::ReconfigurationPending);
}

#[tokio::test]
async fn injected_replicator_failures_cannot_leave_pending_writes_live() {
    for fail_close in [false, true] {
        let local = identity(
            1,
            if fail_close {
                "close-primary"
            } else {
                "role-primary"
            },
        );
        let fail_change_role = Arc::new(AtomicBool::new(false));
        let fail_close_flag = Arc::new(AtomicBool::new(false));
        let application = Arc::new(TestApplication::default());
        let runtime = runtime_with_factory(
            local.clone(),
            application.clone(),
            Arc::new(MemoryAuthorityStore::default()),
            CountingFactory {
                storage: Arc::downgrade(&application),
                opened: Arc::new(AtomicUsize::new(0)),
                role_changes: Arc::new(AtomicUsize::new(0)),
                epoch_updates: Arc::new(AtomicUsize::new(0)),
                events: Arc::new(Mutex::new(Vec::new())),
                fail_change_role: fail_change_role.clone(),
                fail_close: fail_close_flag.clone(),
            },
        )
        .unwrap();
        runtime
            .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
            .await
            .unwrap();
        runtime
            .apply_effect(effect(
                2,
                RuntimeEffectAction::AdmitAuthority(Box::new(authority(
                    local.clone(),
                    vec![local, identity(2, "secondary"), identity(3, "third")],
                ))),
            ))
            .await
            .unwrap();
        runtime
            .apply_effect(effect(
                3,
                RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
            ))
            .await
            .unwrap();
        runtime
            .apply_effect(effect(
                4,
                RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
            ))
            .await
            .unwrap();
        let pending = runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new(if fail_close {
                    "close-failure"
                } else {
                    "role-failure"
                }),
                data: Bytes::from_static(b"pending"),
            })
            .await
            .unwrap();
        let result = if fail_close {
            fail_close_flag.store(true, Ordering::SeqCst);
            runtime
                .apply_effect(effect(5, RuntimeEffectAction::Close))
                .await
        } else {
            fail_change_role.store(true, Ordering::SeqCst);
            runtime
                .apply_effect(effect(
                    5,
                    RuntimeEffectAction::ChangeRole(ReplicaRole::ActiveSecondary),
                ))
                .await
        };
        assert!(matches!(result, Err(RuntimeError::Application(_))));
        if fail_close {
            assert!(matches!(
                pending.committed().await,
                Err(RuntimeError::Closed)
            ));
        } else {
            assert!(matches!(
                pending.committed().await,
                Err(RuntimeError::WriteClosed(
                    AccessStatus::ReconfigurationPending
                ))
            ));
        }
    }
}

#[tokio::test]
async fn exact_target_copy_closes_the_replication_gap_before_completion() {
    let source = identity(1, "source");
    let target = identity(1, "replacement");
    let source_runtime = PodRuntime::new(
        source.clone(),
        Arc::new(TestApplication::default()),
        Arc::new(MemoryAuthorityStore::default()),
    );
    source_runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    source_runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority(
                source.clone(),
                vec![source.clone()],
            ))),
        ))
        .await
        .unwrap();
    source_runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    source_runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    source_runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("one"),
            data: Bytes::from_static(b"one"),
        })
        .await
        .unwrap()
        .committed()
        .await
        .unwrap();
    let mut prepared = prepare_copy_authorized(
        &source_runtime,
        PrepareCopyRequest {
            build_id: OperationId::new("build"),
            target: target.clone(),
            configuration: BuildConfiguration::Current,
            copy_context: empty_copy_context(),
        },
    )
    .await
    .unwrap();
    assert_eq!(prepared.authority.replication_boundary_lsn, 1);
    let items = copy_through_final(&mut prepared).await;
    assert_eq!(items.len(), 3);
    assert_eq!(
        items.last().unwrap().catch_up_boundary_lsn,
        Some(prepared.authority.replication_boundary_lsn)
    );
    let live_write = source_runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("two"),
            data: Bytes::from_static(b"two"),
        })
        .await
        .unwrap();
    assert!(live_write.build_items.is_empty());
    let live_build_item = next_copy_item(&mut prepared).await;
    live_write.committed().await.unwrap();

    let target_application = Arc::new(TestApplication::default());
    let target_runtime = PodRuntime::new(
        target,
        target_application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
    );
    target_runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    target_runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary),
        ))
        .await
        .unwrap();
    target_runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::AdmitBuildAuthority(Box::new(prepared.authority.clone())),
        ))
        .await
        .unwrap();

    assert!(matches!(
        target_runtime
            .data_plane()
            .receive_copy_item(items[2].clone())
            .await,
        Err(RuntimeError::InvalidReplication(_))
    ));
    let first_ack = target_runtime
        .data_plane()
        .receive_copy_item(items[0].clone())
        .await
        .unwrap();
    source_runtime
        .data_plane()
        .accept_copy_acknowledgement(first_ack.clone())
        .await
        .unwrap();
    assert!(!first_ack.final_item);
    assert_eq!(
        target_application.copy_apply_count.load(Ordering::SeqCst),
        1
    );
    target_runtime
        .data_plane()
        .receive_copy_item(items[0].clone())
        .await
        .unwrap();
    assert_eq!(
        target_application.copy_apply_count.load(Ordering::SeqCst),
        1
    );
    assert_eq!(target_application.applied.lock().unwrap().len(), 0);
    target_runtime
        .data_plane()
        .receive_copy_item(items[1].clone())
        .await
        .unwrap();
    let final_ack = target_runtime
        .data_plane()
        .receive_copy_item(items[2].clone())
        .await
        .unwrap();
    assert_eq!(target_application.applied.lock().unwrap().len(), 1);
    assert!(final_ack.final_item);
    assert_eq!(final_ack.durable_lsn, 1);
    let live_ack = target_runtime
        .data_plane()
        .receive_copy_item(live_build_item)
        .await
        .unwrap();
    let mut forged_ack = live_ack.clone();
    forged_ack.durable_lsn = 100;
    assert!(matches!(
        source_runtime
            .data_plane()
            .accept_copy_acknowledgement(forged_ack)
            .await,
        Err(RuntimeError::InvalidReplication(_))
    ));
    source_runtime
        .data_plane()
        .accept_copy_acknowledgement(live_ack.clone())
        .await
        .unwrap();
    assert!(!source_runtime.snapshot().await.builds[0].completed);
    source_runtime
        .data_plane()
        .accept_copy_acknowledgement(final_ack)
        .await
        .unwrap();
    let retried_final = target_runtime
        .data_plane()
        .receive_copy_item(items[2].clone())
        .await
        .unwrap();
    assert_eq!(retried_final.durable_lsn, 1);
    source_runtime
        .data_plane()
        .accept_copy_acknowledgement(retried_final)
        .await
        .unwrap();
    let retried_chunk = target_runtime
        .data_plane()
        .receive_copy_item(items[0].clone())
        .await
        .unwrap();
    assert!(retried_chunk.snapshot_chunk);
    assert_eq!(retried_chunk.durable_lsn, 0);
    source_runtime
        .data_plane()
        .accept_copy_acknowledgement(retried_chunk)
        .await
        .unwrap();
    let mut conflicting_chunk = items[0].clone();
    conflicting_chunk.data.push(0xff);
    assert!(matches!(
        target_runtime
            .data_plane()
            .receive_copy_item(conflicting_chunk)
            .await,
        Err(RuntimeError::InvalidReplication(_))
    ));
    assert_eq!(
        target_application.copy_apply_count.load(Ordering::SeqCst),
        2
    );
    let snapshot = target_runtime.snapshot().await;
    assert_eq!(snapshot.current_progress, 2);
    assert!(snapshot.builds[0].completed);
    let source_snapshot = source_runtime.snapshot().await;
    assert!(source_snapshot.builds[0].completed);
    assert_eq!(source_snapshot.builds[0].durable_lsn, 2);
}

#[tokio::test]
async fn copy_context_and_snapshot_stream_without_blocking_primary_writes() {
    let application = Arc::new(TestApplication::default());
    application.seed_operation(1, Bytes::from_static(b"seed"));
    application
        .pause_copy_enumeration
        .store(true, Ordering::SeqCst);
    let runtime = open_primary(application.clone(), vec![identity(1, "source")]).await;
    let mut prepared = prepare_copy_authorized(
        &runtime,
        PrepareCopyRequest {
            build_id: OperationId::new("streaming-copy"),
            target: identity(1, "replacement"),
            configuration: BuildConfiguration::Current,
            copy_context: Box::pin(stream::iter([
                Ok(Bytes::from_static(b"context-1")),
                Ok(Bytes::from_static(b"context-2")),
            ])),
        },
    )
    .await
    .unwrap();
    application.copy_enumeration_notify.notified().await;
    let pending = timeout(
        Duration::from_secs(1),
        runtime.data_plane().begin_write(ClientWrite {
            operation_id: OperationId::new("write-during-copy"),
            data: Bytes::from_static(b"live"),
        }),
    )
    .await
    .unwrap()
    .unwrap();
    pending.committed().await.unwrap();
    assert_eq!(
        application.copy_context_items.lock().unwrap().as_slice(),
        [
            Bytes::from_static(b"context-1"),
            Bytes::from_static(b"context-2")
        ]
    );
    application
        .pause_copy_enumeration
        .store(false, Ordering::SeqCst);
    application.resume_copy_enumeration_notify.notify_waiters();
    let snapshot = copy_through_final(&mut prepared).await;
    assert_eq!(
        snapshot.iter().filter(|item| item.snapshot_chunk).count(),
        2
    );
    let final_item = snapshot.iter().find(|item| item.final_item).unwrap();
    assert_eq!(final_item.catch_up_boundary_lsn, Some(2));
    assert_eq!(
        runtime.snapshot().await.builds[0].catch_up_boundary_lsn,
        Some(2)
    );
    let live = next_copy_item(&mut prepared).await;
    assert_eq!(live.lsn, 2);
    assert_eq!(live.catch_up_boundary_lsn, None);
    assert!(!live.snapshot_chunk);
    assert!(!live.final_item);
}

async fn exercise_copy_boundary_demotion_overlap(iteration: usize) {
    let source = identity(1, &format!("boundary-source-{iteration}"));
    let new_primary = identity(2, &format!("boundary-primary-{iteration}"));
    let target = identity(3, &format!("boundary-target-{iteration}"));
    let previous = authority(source.clone(), vec![source.clone(), new_primary.clone()]);
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        new_primary.replica_id,
        vec![
            ConfigurationMember {
                identity: source.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: new_primary,
                role: ReplicaRole::Primary,
            },
        ],
        2,
    );
    let demoted = AdmittedAuthority {
        local_identity: source.clone(),
        transition_kind: Some(TransitionKind::Failover),
        previous_configuration: Some(previous.current_configuration.clone()),
        current_configuration: current,
        switchover_handoff: None,
        secondary_removal: None,
        scale_up: None,
    };
    let application = Arc::new(TestApplication::default());
    application.seed_operation(1, Bytes::from_static(b"seed"));
    application
        .pause_copy_completion
        .store(true, Ordering::SeqCst);
    let store = Arc::new(MemoryAuthorityStore::default());
    let runtime = Arc::new(PodRuntime::new(source, application.clone(), store.clone()));
    for (sequence, action) in [
        RuntimeEffectAction::Open(OpenMode::Existing),
        RuntimeEffectAction::AdmitAuthority(Box::new(previous)),
        RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
    ]
    .into_iter()
    .enumerate()
    {
        runtime
            .apply_effect(effect(sequence as u64 + 1, action))
            .await
            .unwrap();
    }

    let build_id = OperationId::new(format!("boundary-build-{iteration}"));
    let mut prepared = prepare_copy_authorized(
        &runtime,
        PrepareCopyRequest {
            build_id: build_id.clone(),
            target,
            configuration: BuildConfiguration::Current,
            copy_context: empty_copy_context(),
        },
    )
    .await
    .unwrap();
    let first = next_copy_item(&mut prepared).await;
    assert!(first.snapshot_chunk);
    application.copy_completion_notify.notified().await;

    store.pause_build_progress.store(true, Ordering::SeqCst);
    let ack_runtime = runtime.clone();
    let acknowledgement = proto::CopyAck {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        build_id: first.build_id.clone(),
        sender: first.sender.clone(),
        receiver: first.receiver.clone(),
        epoch: first.epoch,
        current_configuration_id: first.current_configuration_id.clone(),
        sequence: first.sequence,
        durable_lsn: first.lsn,
        replication_boundary_lsn: first.replication_boundary_lsn,
        catch_up_boundary_lsn: first.catch_up_boundary_lsn,
        final_item: first.final_item,
        snapshot_chunk: first.snapshot_chunk,
        ..Default::default()
    };
    let acknowledgement = tokio::spawn(async move {
        ack_runtime
            .data_plane()
            .accept_copy_acknowledgement(acknowledgement)
            .await
    });
    store.build_progress_notify.notified().await;

    application
        .pause_copy_completion
        .store(false, Ordering::SeqCst);
    application.resume_copy_completion_notify.notify_waiters();
    tokio::task::yield_now().await;

    let demotion_runtime = runtime.clone();
    let expected_demoted = demoted.clone();
    let demotion = tokio::spawn(async move {
        demotion_runtime
            .apply_effect(effect(
                5,
                RuntimeEffectAction::AdmitAuthority(Box::new(demoted)),
            ))
            .await
    });
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    drop(prepared);
    store.pause_build_progress.store(false, Ordering::SeqCst);
    store.resume_build_progress_notify.notify_waiters();

    timeout(Duration::from_secs(2), async {
        acknowledgement.await.unwrap().unwrap();
        demotion.await.unwrap().unwrap();
        loop {
            if runtime.snapshot().await.builds.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let snapshot = runtime.snapshot().await;
    assert_eq!(snapshot.authority, Some(expected_demoted));
    assert_eq!(snapshot.write_status, AccessStatus::ReconfigurationPending);
    assert!(snapshot.builds.is_empty());
    let progress = store.load_build_progress(&build_id).await.unwrap().unwrap();
    assert_eq!(progress.last_sequence, first.sequence);
    assert_eq!(progress.catch_up_boundary_lsn, None);
}

#[tokio::test]
async fn cancelled_copy_boundary_cannot_deadlock_source_demotion() {
    for iteration in 0..8 {
        exercise_copy_boundary_demotion_overlap(iteration).await;
    }
}

#[tokio::test]
async fn copy_boundary_persists_without_concurrent_authority_change() {
    let source = identity(1, "boundary-control-source");
    let target = identity(2, "boundary-control-target");
    let application = Arc::new(TestApplication::default());
    application.seed_operation(1, Bytes::from_static(b"seed"));
    let store = Arc::new(MemoryAuthorityStore::default());
    let runtime = Arc::new(PodRuntime::new(source.clone(), application, store.clone()));
    for (sequence, action) in [
        RuntimeEffectAction::Open(OpenMode::Existing),
        RuntimeEffectAction::AdmitAuthority(Box::new(authority(source.clone(), vec![source]))),
        RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
    ]
    .into_iter()
    .enumerate()
    {
        runtime
            .apply_effect(effect(sequence as u64 + 1, action))
            .await
            .unwrap();
    }
    let build_id = OperationId::new("boundary-control-build");
    let mut prepared = prepare_copy_authorized(
        &runtime,
        PrepareCopyRequest {
            build_id: build_id.clone(),
            target,
            configuration: BuildConfiguration::Current,
            copy_context: empty_copy_context(),
        },
    )
    .await
    .unwrap();
    let final_item = copy_through_final(&mut prepared)
        .await
        .into_iter()
        .find(|item| item.final_item)
        .unwrap();
    assert_eq!(final_item.catch_up_boundary_lsn, Some(1));
    assert_eq!(
        store
            .load_build_progress(&build_id)
            .await
            .unwrap()
            .unwrap()
            .catch_up_boundary_lsn,
        Some(1)
    );
}

#[tokio::test]
async fn source_copy_requires_agent_admitted_build_authority() {
    let runtime = open_primary(
        Arc::new(TestApplication::default()),
        vec![identity(1, "unauthorized-copy-source")],
    )
    .await;
    assert!(matches!(
        runtime
            .data_plane()
            .prepare_copy(PrepareCopyRequest {
                build_id: OperationId::new("unauthorized-copy"),
                target: identity(1, "unauthorized-copy-target"),
                configuration: BuildConfiguration::Current,
                copy_context: empty_copy_context(),
            })
            .await,
        Err(RuntimeError::AuthorityNotAdmitted)
    ));
}

#[tokio::test]
async fn retained_gap_enumeration_does_not_block_primary_writes() {
    let local = identity(1, "retained-gap-source");
    let target = identity(1, "retained-gap-target");
    let application = Arc::new(TestApplication::default());
    application.seed_operation(1, Bytes::from_static(b"seed"));
    let store = Arc::new(MemoryAuthorityStore::default());
    let admitted = authority(local.clone(), vec![local.clone()]);
    let runtime = Arc::new(PodRuntime::new(
        local.clone(),
        application.clone(),
        store.clone(),
    ));
    for (index, action) in [
        RuntimeEffectAction::Open(OpenMode::Existing),
        RuntimeEffectAction::AdmitAuthority(Box::new(admitted.clone())),
        RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
    ]
    .into_iter()
    .enumerate()
    {
        runtime
            .apply_effect(effect(index as u64 + 1, action))
            .await
            .unwrap();
    }
    let build_id = OperationId::new("retained-gap-copy");
    store
        .admit_build(&BuildAuthority {
            build_id: build_id.clone(),
            kind: BuildAuthorityKind::Provisioning,
            source: local,
            target: target.clone(),
            current_configuration: admitted.current_configuration,
            replication_boundary_lsn: 0,
        })
        .await
        .unwrap();
    application
        .pause_retained_enumeration
        .store(true, Ordering::SeqCst);
    let cancelled_runtime = runtime.clone();
    let cancelled_build_id = build_id.clone();
    let cancelled_target = target.clone();
    let cancelled = tokio::spawn(async move {
        prepare_copy_authorized(
            &cancelled_runtime,
            PrepareCopyRequest {
                build_id: cancelled_build_id,
                target: cancelled_target,
                configuration: BuildConfiguration::Current,
                copy_context: empty_copy_context(),
            },
        )
        .await
    });
    application.retained_enumeration_notify.notified().await;
    cancelled.abort();
    assert!(matches!(cancelled.await, Err(error) if error.is_cancelled()));
    tokio::task::yield_now().await;
    assert!(runtime.snapshot().await.builds.is_empty());

    let runtime_for_copy = runtime.clone();
    let prepare = tokio::spawn(async move {
        prepare_copy_authorized(
            &runtime_for_copy,
            PrepareCopyRequest {
                build_id,
                target,
                configuration: BuildConfiguration::Current,
                copy_context: empty_copy_context(),
            },
        )
        .await
    });
    application.retained_enumeration_notify.notified().await;
    let pending = timeout(
        Duration::from_secs(1),
        runtime.data_plane().begin_write(ClientWrite {
            operation_id: OperationId::new("write-during-gap-scan"),
            data: Bytes::from_static(b"live"),
        }),
    )
    .await
    .unwrap()
    .unwrap();
    pending.committed().await.unwrap();
    application
        .pause_retained_enumeration
        .store(false, Ordering::SeqCst);
    application
        .resume_retained_enumeration_notify
        .notify_waiters();

    let mut prepared = prepare.await.unwrap().unwrap();
    let _snapshot = copy_through_final(&mut prepared).await;
    assert_eq!(next_copy_item(&mut prepared).await.lsn, 1);
    assert_eq!(next_copy_item(&mut prepared).await.lsn, 2);
}

#[tokio::test]
async fn dropping_returned_copy_stream_cancels_paused_provider_and_releases_build() {
    let application = Arc::new(TestApplication::default());
    application.seed_operation(1, Bytes::from_static(b"seed"));
    application
        .pause_copy_enumeration
        .store(true, Ordering::SeqCst);
    let runtime = open_primary(application.clone(), vec![identity(1, "copy-cancel")]).await;
    let request = || PrepareCopyRequest {
        build_id: OperationId::new("cancel-returned-copy"),
        target: identity(1, "copy-cancel-target"),
        configuration: BuildConfiguration::Current,
        copy_context: empty_copy_context(),
    };
    let prepared = prepare_copy_authorized(&runtime, request()).await.unwrap();
    application.copy_enumeration_notify.notified().await;
    drop(prepared);
    timeout(Duration::from_secs(1), async {
        loop {
            if runtime.snapshot().await.builds.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    application
        .pause_copy_enumeration
        .store(false, Ordering::SeqCst);
    application.resume_copy_enumeration_notify.notify_waiters();
    let _retry = prepare_copy_authorized(&runtime, request()).await.unwrap();
}

#[tokio::test]
async fn completed_copy_hands_off_to_new_authority_and_normal_quorum_replication() {
    let source = identity(1, "source");
    let secondary = identity(2, "secondary");
    let old = identity(3, "old");
    let replacement = identity(3, "replacement");
    let previous_authority =
        authority(source.clone(), vec![source.clone(), secondary.clone(), old]);
    let source_application = Arc::new(TestApplication::default());
    source_application.seed_operation(1, Bytes::from_static(b"one"));
    let source_runtime = PodRuntime::new(
        source.clone(),
        source_application,
        Arc::new(MemoryAuthorityStore::default()),
    );
    source_runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    source_runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(previous_authority.clone())),
        ))
        .await
        .unwrap();
    source_runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    let mut prepared = prepare_copy_authorized(
        &source_runtime,
        PrepareCopyRequest {
            build_id: OperationId::new("handoff-build"),
            target: replacement.clone(),
            configuration: BuildConfiguration::Current,
            copy_context: empty_copy_context(),
        },
    )
    .await
    .unwrap();
    let items = copy_through_final(&mut prepared).await;

    let target_application = Arc::new(TestApplication::default());
    let target_store = Arc::new(MemoryAuthorityStore::default());
    let target_runtime = PodRuntime::new(replacement.clone(), target_application, target_store);
    target_runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::New)))
        .await
        .unwrap();
    target_runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary),
        ))
        .await
        .unwrap();
    target_runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::AdmitBuildAuthority(Box::new(prepared.authority.clone())),
        ))
        .await
        .unwrap();
    for item in items {
        target_runtime
            .data_plane()
            .receive_copy_item(item)
            .await
            .unwrap();
    }

    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        source.replica_id,
        vec![
            ConfigurationMember {
                identity: source.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: secondary.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: replacement.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    );
    let new_source_authority = AdmittedAuthority {
        scale_up: None,
        secondary_removal: None,
        local_identity: source.clone(),
        transition_kind: Some(TransitionKind::Replacement),
        previous_configuration: Some(previous_authority.current_configuration.clone()),
        current_configuration: current.clone(),
        switchover_handoff: None,
    };
    let new_target_authority = AdmittedAuthority {
        local_identity: replacement.clone(),
        ..new_source_authority.clone()
    };
    target_runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::AdmitAuthority(Box::new(new_target_authority)),
        ))
        .await
        .unwrap();
    target_runtime
        .apply_effect(effect(
            5,
            RuntimeEffectAction::ChangeRole(ReplicaRole::ActiveSecondary),
        ))
        .await
        .unwrap();
    assert_eq!(
        target_runtime.snapshot().await.verified_replication_lsn,
        Some(1)
    );

    let secondary_application = Arc::new(TestApplication::default());
    secondary_application.seed_operation(1, Bytes::from_static(b"one"));
    let secondary_store = Arc::new(MemoryAuthorityStore::default());
    secondary_store
        .record_replication_progress(&ReplicationProgress {
            fence: previous_authority.fence(),
            verified_lsn: 1,
        })
        .await
        .unwrap();
    let secondary_runtime =
        PodRuntime::new(secondary.clone(), secondary_application, secondary_store);
    secondary_runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    secondary_runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(AdmittedAuthority {
                scale_up: None,
                secondary_removal: None,
                local_identity: secondary.clone(),
                ..new_source_authority.clone()
            })),
        ))
        .await
        .unwrap();
    secondary_runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::ActiveSecondary),
        ))
        .await
        .unwrap();

    source_runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::AdmitAuthority(Box::new(new_source_authority.clone())),
        ))
        .await
        .unwrap();
    source_runtime
        .apply_effect(effect(
            5,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let pending = source_runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("two"),
            data: Bytes::from_static(b"two"),
        })
        .await
        .unwrap();
    for item in pending.replication_items.clone() {
        let receiver = item.receiver.as_ref().unwrap();
        let ack = if receiver.instance_id == replacement.instance_id.as_str() {
            target_runtime
                .data_plane()
                .receive_replication(item)
                .await
                .unwrap()
                .applied()
                .await
                .unwrap()
        } else if receiver.instance_id == secondary.instance_id.as_str() {
            secondary_runtime
                .data_plane()
                .receive_replication(item)
                .await
                .unwrap()
                .applied()
                .await
                .unwrap()
        } else {
            continue;
        };
        source_runtime
            .data_plane()
            .accept_acknowledgement(ack)
            .await
            .unwrap();
    }
    assert_eq!(pending.committed().await.unwrap().committed_lsn, 2);

    target_runtime
        .apply_effect(effect(
            6,
            RuntimeEffectAction::AdmitAuthority(Box::new(AdmittedAuthority {
                scale_up: None,
                secondary_removal: None,
                local_identity: replacement.clone(),
                transition_kind: None,
                previous_configuration: None,
                current_configuration: current.clone(),
                switchover_handoff: None,
            })),
        ))
        .await
        .unwrap();
    assert_eq!(
        target_runtime.snapshot().await.verified_replication_lsn,
        Some(2)
    );
    source_runtime
        .apply_effect(effect(
            6,
            RuntimeEffectAction::AdmitAuthority(Box::new(AdmittedAuthority {
                scale_up: None,
                secondary_removal: None,
                local_identity: source,
                transition_kind: None,
                previous_configuration: None,
                current_configuration: current,
                switchover_handoff: None,
            })),
        ))
        .await
        .unwrap();
    source_runtime
        .apply_effect(effect(
            7,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let pending = source_runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("three"),
            data: Bytes::from_static(b"three"),
        })
        .await
        .unwrap();
    let item = pending
        .replication_items
        .iter()
        .find(|item| {
            item.receiver
                .as_ref()
                .is_some_and(|receiver| receiver.instance_id == replacement.instance_id.as_str())
        })
        .unwrap()
        .clone();
    let ack = target_runtime
        .data_plane()
        .receive_replication(item)
        .await
        .unwrap()
        .applied()
        .await
        .unwrap();
    source_runtime
        .data_plane()
        .accept_acknowledgement(ack)
        .await
        .unwrap();
    assert_eq!(pending.committed().await.unwrap().committed_lsn, 3);
}

#[tokio::test]
async fn bootstrap_primary_builds_full_genesis_members_before_configuration_admission() {
    let primary = identity(1, "primary");
    let secondary = identity(2, "secondary");
    let genesis = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        primary.replica_id,
        vec![
            ConfigurationMember {
                identity: primary.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: secondary.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: identity(3, "third"),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    );
    let runtime = PodRuntime::new(
        primary,
        Arc::new(TestApplication::default()),
        Arc::new(MemoryAuthorityStore::default()),
    );
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::New)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();

    let mut prepared = prepare_copy_authorized(
        &runtime,
        PrepareCopyRequest {
            build_id: OperationId::new("bootstrap-secondary"),
            target: secondary,
            configuration: BuildConfiguration::Bootstrap(genesis),
            copy_context: empty_copy_context(),
        },
    )
    .await
    .unwrap();
    assert_eq!(prepared.authority.kind, BuildAuthorityKind::Bootstrap);
    assert_eq!(prepared.authority.replication_boundary_lsn, 0);
    let items = copy_through_final(&mut prepared).await;
    assert_eq!(items.len(), 1);
    assert!(items[0].final_item);
}

#[tokio::test]
async fn build_identity_is_immutable_and_recoverable_on_source_restart() {
    let source = identity(1, "source");
    let target = identity(1, "replacement");
    let admitted = authority(source.clone(), vec![source.clone()]);
    let application = Arc::new(TestApplication::default());
    let store = Arc::new(MemoryAuthorityStore::default());
    let runtime = PodRuntime::new(source.clone(), application.clone(), store.clone());
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(admitted.clone())),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let request = |target| PrepareCopyRequest {
        build_id: OperationId::new("immutable-build"),
        target,
        configuration: BuildConfiguration::Current,
        copy_context: empty_copy_context(),
    };
    let mut first = prepare_copy_authorized(&runtime, request(target.clone()))
        .await
        .unwrap();
    assert_eq!(first.authority.replication_boundary_lsn, 0);
    let _ = copy_through_final(&mut first).await;
    assert_eq!(
        runtime.snapshot().await.builds[0].catch_up_boundary_lsn,
        Some(0)
    );
    runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("after-boundary"),
            data: Bytes::from_static(b"after-boundary"),
        })
        .await
        .unwrap()
        .committed()
        .await
        .unwrap();
    let live = next_copy_item(&mut first).await;
    assert_eq!(live.lsn, 1);
    assert!(!live.final_item);
    assert!(matches!(
        prepare_copy_authorized(&runtime, request(target.clone())).await,
        Err(RuntimeError::ReconfigurationPending)
    ));

    assert!(matches!(
        prepare_copy_authorized(&runtime, request(identity(2, "different-target"))).await,
        Err(RuntimeError::AuthorityMismatch(_))
    ));

    let restarted = PodRuntime::new(source, application, store);
    restarted
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    restarted
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(admitted)),
        ))
        .await
        .unwrap();
    restarted
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    let mut resumed = prepare_copy_authorized(&restarted, request(target))
        .await
        .unwrap();
    assert_eq!(resumed.authority.replication_boundary_lsn, 0);
    let _ = copy_through_final(&mut resumed).await;
    assert_eq!(
        restarted.snapshot().await.builds[0].catch_up_boundary_lsn,
        Some(0)
    );
    let resumed_operation = next_copy_item(&mut resumed).await;
    assert_eq!(resumed_operation.lsn, 1);
    assert!(!resumed_operation.final_item);
}

#[tokio::test]
async fn retrying_accepted_write_does_not_overwrite_build_final_sequence() {
    let source = identity(1, "source");
    let target = identity(1, "replacement");
    let application = Arc::new(TestApplication::default());
    let runtime = PodRuntime::new(
        source.clone(),
        application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
    );
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority(source.clone(), vec![source]))),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let write = ClientWrite {
        operation_id: OperationId::new("ambiguous-before-build"),
        data: Bytes::from_static(b"value"),
    };
    application.fail_after_apply.store(true, Ordering::SeqCst);
    assert!(matches!(
        runtime.data_plane().begin_write(write.clone()).await,
        Err(RuntimeError::Application(_))
    ));
    let mut prepared = prepare_copy_authorized(
        &runtime,
        PrepareCopyRequest {
            build_id: OperationId::new("final-sequence"),
            target,
            configuration: BuildConfiguration::Current,
            copy_context: empty_copy_context(),
        },
    )
    .await
    .unwrap();
    let final_item = copy_through_final(&mut prepared)
        .await
        .into_iter()
        .find(|item| item.final_item)
        .unwrap();
    let retry = runtime.data_plane().begin_write(write).await.unwrap();
    assert!(retry.build_items.is_empty());
    retry.committed().await.unwrap();
    runtime
        .data_plane()
        .accept_copy_acknowledgement(proto::CopyAck {
            protocol_version: kuberic_protocol::PROTOCOL_VERSION,
            build_id: final_item.build_id,
            sender: final_item.sender,
            receiver: final_item.receiver,
            epoch: final_item.epoch,
            current_configuration_id: final_item.current_configuration_id,
            sequence: final_item.sequence,
            durable_lsn: final_item.replication_boundary_lsn,
            replication_boundary_lsn: final_item.replication_boundary_lsn,
            catch_up_boundary_lsn: final_item.catch_up_boundary_lsn,
            final_item: true,
            snapshot_chunk: false,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(runtime.snapshot().await.builds[0].completed);
}

#[tokio::test]
async fn copy_receiver_recovers_sequence_and_final_completion_after_restart() {
    let source = identity(1, "source");
    let target = identity(1, "replacement");
    let source_runtime = PodRuntime::new(
        source.clone(),
        Arc::new(TestApplication::default()),
        Arc::new(MemoryAuthorityStore::default()),
    );
    source_runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    source_runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority(source.clone(), vec![source]))),
        ))
        .await
        .unwrap();
    source_runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    source_runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    source_runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("copy-value"),
            data: Bytes::from_static(b"copy-value"),
        })
        .await
        .unwrap()
        .committed()
        .await
        .unwrap();
    let mut prepared = prepare_copy_authorized(
        &source_runtime,
        PrepareCopyRequest {
            build_id: OperationId::new("restartable-build"),
            target: target.clone(),
            configuration: BuildConfiguration::Current,
            copy_context: empty_copy_context(),
        },
    )
    .await
    .unwrap();
    let items = copy_through_final(&mut prepared).await;

    let application = Arc::new(TestApplication::default());
    let store = Arc::new(MemoryAuthorityStore::default());
    let first_runtime = PodRuntime::new(target.clone(), application.clone(), store.clone());
    first_runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::New)))
        .await
        .unwrap();
    first_runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary),
        ))
        .await
        .unwrap();
    first_runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::AdmitBuildAuthority(Box::new(prepared.authority.clone())),
        ))
        .await
        .unwrap();
    first_runtime
        .data_plane()
        .receive_copy_item(items[0].clone())
        .await
        .unwrap();

    let restarted = PodRuntime::new(target.clone(), application.clone(), store.clone());
    restarted
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    restarted
        .apply_effect(effect(
            2,
            RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary),
        ))
        .await
        .unwrap();
    restarted
        .apply_effect(effect(
            3,
            RuntimeEffectAction::AdmitBuildAuthority(Box::new(prepared.authority.clone())),
        ))
        .await
        .unwrap();
    restarted
        .data_plane()
        .receive_copy_item(items[0].clone())
        .await
        .unwrap();
    restarted
        .data_plane()
        .receive_copy_item(items[1].clone())
        .await
        .unwrap();
    store.fail_build_progress_once.store(true, Ordering::SeqCst);
    assert!(matches!(
        restarted
            .data_plane()
            .receive_copy_item(items[2].clone())
            .await,
        Err(RuntimeError::Application(_))
    ));

    let after_final_crash = PodRuntime::new(target, application, store);
    after_final_crash
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    after_final_crash
        .apply_effect(effect(
            2,
            RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary),
        ))
        .await
        .unwrap();
    after_final_crash
        .apply_effect(effect(
            3,
            RuntimeEffectAction::AdmitBuildAuthority(Box::new(prepared.authority)),
        ))
        .await
        .unwrap();
    let final_ack = after_final_crash
        .data_plane()
        .receive_copy_item(items[2].clone())
        .await
        .unwrap();
    assert!(final_ack.final_item);
    assert_eq!(final_ack.catch_up_boundary_lsn, Some(1));
    let recovered = after_final_crash.snapshot().await;
    assert!(recovered.builds[0].completed);
    assert_eq!(recovered.builds[0].catch_up_boundary_lsn, Some(1));
}

#[tokio::test]
async fn completed_scale_up_copy_seeds_exact_candidate_progress_for_pc_cc() {
    let source = identity(1, "scale-up-source");
    let candidate = identity(2, "scale-up-candidate");
    let previous_authority = authority(source.clone(), vec![source.clone()]);
    let source_application = Arc::new(TestApplication::default());
    source_application.seed_operation(1, Bytes::from_static(b"one"));
    let source_runtime = PodRuntime::new(
        source.clone(),
        source_application,
        Arc::new(MemoryAuthorityStore::default()),
    );
    source_runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    source_runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(previous_authority.clone())),
        ))
        .await
        .unwrap();
    source_runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    let mut prepared = prepare_copy_authorized(
        &source_runtime,
        PrepareCopyRequest {
            build_id: OperationId::new("scale-up-build"),
            target: candidate.clone(),
            configuration: BuildConfiguration::Current,
            copy_context: empty_copy_context(),
        },
    )
    .await
    .unwrap();
    let items = copy_through_final(&mut prepared).await;
    assert_eq!(items.last().unwrap().catch_up_boundary_lsn, Some(1));

    let target_application = Arc::new(TestApplication::default());
    let target_store = Arc::new(MemoryAuthorityStore::default());
    let target_runtime = PodRuntime::new(
        candidate.clone(),
        target_application.clone(),
        target_store.clone(),
    );
    target_runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::New)))
        .await
        .unwrap();
    target_runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary),
        ))
        .await
        .unwrap();
    target_runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::AdmitBuildAuthority(Box::new(prepared.authority.clone())),
        ))
        .await
        .unwrap();
    for item in items {
        let acknowledgement = target_runtime
            .data_plane()
            .receive_copy_item(item)
            .await
            .unwrap();
        source_runtime
            .data_plane()
            .accept_copy_acknowledgement(acknowledgement)
            .await
            .unwrap();
    }

    let previous_policy = kuberic_protocol::types::EffectivePolicy::fixed(1, 30).unwrap();
    let current_policy = kuberic_protocol::types::EffectivePolicy::fixed(2, 30).unwrap();
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        source.replica_id,
        vec![
            ConfigurationMember {
                identity: source.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: candidate.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        current_policy.write_quorum,
    );
    let mut intent = ScaleUpIntent {
        operation_id: OperationId::default(),
        resource_uid: ResourceUid::new("set"),
        spec_generation: 2,
        desired_replicas: 2,
        previous_configuration: previous_authority.current_configuration.clone(),
        current_configuration: current.clone(),
        previous_policy,
        current_policy,
        primary: source.clone(),
        target: candidate.clone(),
        build_id: prepared.authority.build_id.clone(),
        snapshot_boundary_lsn: prepared.authority.replication_boundary_lsn,
        catch_up_boundary_lsn: 1,
    };
    intent.operation_id = intent.expected_operation_id();
    let source_authority = AdmittedAuthority {
        scale_up: Some(Box::new(ScaleUpConfigurationEvidence::Admission { intent })),
        secondary_removal: None,
        local_identity: source.clone(),
        transition_kind: Some(TransitionKind::ScaleUp),
        previous_configuration: Some(previous_authority.current_configuration),
        current_configuration: current,
        switchover_handoff: None,
    };
    let target_authority = AdmittedAuthority {
        local_identity: candidate.clone(),
        ..source_authority.clone()
    };
    target_runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::AdmitAuthority(Box::new(target_authority.clone())),
        ))
        .await
        .unwrap();
    target_runtime
        .apply_effect(effect(
            5,
            RuntimeEffectAction::ChangeRole(ReplicaRole::ActiveSecondary),
        ))
        .await
        .unwrap();
    assert_eq!(
        target_runtime.snapshot().await.verified_replication_lsn,
        Some(1)
    );

    let restarted_target = PodRuntime::new(candidate, target_application, target_store);
    restarted_target
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    restarted_target
        .apply_effect(effect(
            2,
            RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary),
        ))
        .await
        .unwrap();
    restarted_target
        .apply_effect(effect(
            3,
            RuntimeEffectAction::AdmitBuildAuthority(Box::new(prepared.authority.clone())),
        ))
        .await
        .unwrap();
    restarted_target
        .apply_effect(effect(
            4,
            RuntimeEffectAction::AdmitAuthority(Box::new(target_authority)),
        ))
        .await
        .unwrap();
    restarted_target
        .apply_effect(effect(
            5,
            RuntimeEffectAction::ChangeRole(ReplicaRole::ActiveSecondary),
        ))
        .await
        .unwrap();
    assert_eq!(
        restarted_target.snapshot().await.verified_replication_lsn,
        Some(1)
    );

    source_runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::AdmitAuthority(Box::new(source_authority)),
        ))
        .await
        .unwrap();
    assert!(source_runtime.snapshot().await.catch_up_complete);
}

#[tokio::test]
async fn runtime_exposes_cc_boundary_catch_up_evidence() {
    let primary = identity(1, "primary");
    let secondary = identity(2, "secondary");
    let old = identity(3, "old");
    let replacement = identity(3, "replacement");
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 5),
        ReplicaId::new(1),
        vec![
            ConfigurationMember {
                identity: primary.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: secondary.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: old,
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 6),
        ReplicaId::new(1),
        vec![
            ConfigurationMember {
                identity: primary.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: secondary.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: replacement,
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    );
    let admitted = AdmittedAuthority {
        scale_up: None,
        secondary_removal: None,
        local_identity: primary.clone(),
        transition_kind: Some(TransitionKind::Replacement),
        previous_configuration: Some(previous),
        current_configuration: current,
        switchover_handoff: None,
    };
    let application = Arc::new(TestApplication::default());
    application.seed_progress(10);
    let runtime = PodRuntime::new(
        primary,
        application,
        Arc::new(MemoryAuthorityStore::default()),
    );
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(admitted.clone())),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let waiting = runtime.snapshot().await;
    assert_eq!(waiting.catch_up_boundary, Some(10));
    assert!(!waiting.catch_up_complete);
    assert!(matches!(
        runtime
            .apply_effect(effect(
                5,
                RuntimeEffectAction::SetAccessStatus {
                    read: AccessStatus::Granted,
                    write: AccessStatus::ReconfigurationPending,
                },
            ))
            .await,
        Err(RuntimeError::ReconfigurationPending)
    ));
    let pending_after_configuration = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("after-catch-up-boundary"),
            data: Bytes::from_static(b"newer-write"),
        })
        .await
        .unwrap();

    let runtime_for_wait = Arc::new(runtime);
    let wait_runtime = runtime_for_wait.clone();
    let managed_wait = tokio::spawn(async move {
        wait_runtime
            .apply_effect(effect(5, RuntimeEffectAction::WaitForCatchup))
            .await
    });
    tokio::task::yield_now().await;
    runtime_for_wait
        .data_plane()
        .accept_acknowledgement(acknowledgement(&admitted, secondary, 10))
        .await
        .unwrap();
    timeout(Duration::from_secs(1), managed_wait)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let complete = runtime_for_wait.snapshot().await;
    assert_eq!(complete.current_configuration_quorum_progress, 10);
    assert!(complete.catch_up_complete);
    runtime_for_wait
        .apply_effect(effect(
            6,
            RuntimeEffectAction::SetAccessStatus {
                read: AccessStatus::Granted,
                write: AccessStatus::ReconfigurationPending,
            },
        ))
        .await
        .unwrap();
    timeout(
        Duration::from_secs(1),
        runtime_for_wait
            .primary_replicator()
            .await
            .unwrap()
            .wait_for_catch_up_quorum(
                kuberic_runtime::replicator::ReplicaSetQuorumMode::WriteQuorum,
            ),
    )
    .await
    .unwrap()
    .unwrap();
    drop(pending_after_configuration);
}

#[test]
fn evaluator_scale_up_command_uses_reopened_sqlite_copy_and_preserves_live_write() {
    if env::var("KUBERIC_SQLITE_SCALE_UP_WRITER").as_deref() == Ok("1") {
        std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(evaluator_scale_up_sqlite_trace());
            })
            .unwrap()
            .join()
            .unwrap();
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let output = Command::new(env::current_exe().unwrap())
        .args([
            "--exact",
            "evaluator_scale_up_command_uses_reopened_sqlite_copy_and_preserves_live_write",
        ])
        .env("KUBERIC_SQLITE_SCALE_UP_WRITER", "1")
        .env("KUBERIC_SQLITE_SCALE_UP_ROOT", directory.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn({
            let root = directory.path().to_path_buf();
            move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(verify_persisted_scale_up_after_process_exit(&root));
            }
        })
        .unwrap()
        .join()
        .unwrap();
}

type PersistedApplicationState = (Vec<(i64, i64, Vec<u8>)>, i64, i64);

async fn evaluator_scale_up_sqlite_trace() {
    let root = std::path::PathBuf::from(env::var("KUBERIC_SQLITE_SCALE_UP_ROOT").unwrap());
    let source_root = root.join("source");
    let target_root = root.join("target");
    std::fs::create_dir_all(&source_root).unwrap();
    std::fs::create_dir_all(&target_root).unwrap();
    let resource_uid = ResourceUid::new("sqlite-evaluator-scale-up");
    let source = identity(1, "sqlite-source");
    let previous_policy = EffectivePolicy::fixed(1, 30).unwrap();
    let current_policy = EffectivePolicy::fixed(2, 30).unwrap();
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        source.replica_id,
        vec![ConfigurationMember {
            identity: source.clone(),
            role: ReplicaRole::Primary,
        }],
        previous_policy.write_quorum,
    );
    let mut provisioning = ProvisioningIntent {
        purpose: ProvisioningPurpose::scale_up(ScaleUpProvisioning {
            resource_uid: resource_uid.clone(),
            spec_generation: 2,
            desired_replicas: 2,
            previous_configuration: previous.clone(),
            previous_policy: previous_policy.clone(),
            current_policy: current_policy.clone(),
            target_replica_id: ReplicaId::new(2),
        }),
        pod_uid: PodUid::new("sqlite-target"),
        pvc_uid: PvcUid::new("sqlite-target-pvc"),
        operation_id: OperationId::default(),
    };
    provisioning.operation_id = provisioning.expected_operation_id();
    let target = provisioning.target_identity(&resource_uid);
    let build_id = provisioning.scale_up_build_id(&resource_uid).unwrap();

    let source_path = SqliteStore::metadata_database_path(&source_root);
    let mut source_state = AgentState::new(StorageIdentity {
        schema_version: SCHEMA_VERSION,
        resource_uid: resource_uid.clone(),
        pod_uid: PodUid::new(source.instance_id.as_str()),
        pvc_uid: PvcUid::new("sqlite-source-pvc"),
        initialization_id: InitializationId::new("sqlite-source-init"),
        local_identity: source.clone(),
        effective_policy: previous_policy.clone(),
    });
    source_state.admitted_policy = Some(previous_policy.clone());
    source_state.highest_epoch = previous.epoch;
    source_state.current_configuration = Some(previous.clone());
    source_state.role = ReplicaRole::Primary;
    source_state.read_status = AccessStatus::Granted;
    source_state.write_status = AccessStatus::Granted;
    let source_store =
        Arc::new(SqliteStore::create_authorized(&source_path, source_state).unwrap());
    let previous_authority = AdmittedAuthority {
        local_identity: source.clone(),
        transition_kind: None,
        previous_configuration: None,
        current_configuration: previous.clone(),
        switchover_handoff: None,
        secondary_removal: None,
        scale_up: None,
    };
    source_store.admit(&previous_authority).await.unwrap();

    let target_path = SqliteStore::metadata_database_path(&target_root);
    let mut target_state = AgentState::new(StorageIdentity {
        schema_version: SCHEMA_VERSION,
        resource_uid: resource_uid.clone(),
        pod_uid: provisioning.pod_uid.clone(),
        pvc_uid: provisioning.pvc_uid.clone(),
        initialization_id: provisioning.initialization_id(&resource_uid),
        local_identity: target.clone(),
        effective_policy: current_policy.clone(),
    });
    target_state.scale_up_initialization = Some(provisioning.clone());
    target_state.role = ReplicaRole::IdleSecondary;
    let target_store =
        Arc::new(SqliteStore::create_authorized(&target_path, target_state).unwrap());

    let source_application = Arc::new(TestApplication::default());
    source_application.seed_operation(1, Bytes::from_static(b"seed"));
    source_application
        .pause_copy_enumeration
        .store(true, Ordering::SeqCst);
    let source_runtime = Arc::new(PodRuntime::new(
        source.clone(),
        source_application.clone(),
        source_store.clone(),
    ));
    for (sequence, action) in [
        RuntimeEffectAction::Open(OpenMode::Existing),
        RuntimeEffectAction::AdmitAuthority(Box::new(previous_authority)),
        RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::Granted,
            write: AccessStatus::Granted,
        },
    ]
    .into_iter()
    .enumerate()
    {
        source_runtime
            .apply_effect(effect(sequence as u64 + 1, action))
            .await
            .unwrap();
    }
    let mut prepared = prepare_copy_authorized(
        &source_runtime,
        PrepareCopyRequest {
            build_id: build_id.clone(),
            target: target.clone(),
            configuration: BuildConfiguration::Current,
            copy_context: empty_copy_context(),
        },
    )
    .await
    .unwrap();
    source_application.copy_enumeration_notify.notified().await;
    let live_write = source_runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("write-during-sqlite-copy"),
            data: Bytes::from_static(b"live-value"),
        })
        .await
        .unwrap();
    live_write.committed().await.unwrap();
    let source_enumeration_cut = source_runtime.snapshot().await;
    source_application
        .pause_copy_enumeration
        .store(false, Ordering::SeqCst);
    source_application
        .resume_copy_enumeration_notify
        .notify_waiters();
    let snapshot_items = copy_through_final(&mut prepared).await;
    let live_item = next_copy_item(&mut prepared).await;
    assert_eq!(live_item.lsn, 2);

    let target_application = Arc::new(TestApplication::default());
    let target_runtime = Arc::new(PodRuntime::new(
        target.clone(),
        target_application.clone(),
        target_store.clone(),
    ));
    target_runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::New)))
        .await
        .unwrap();
    target_runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary),
        ))
        .await
        .unwrap();
    target_runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::AdmitBuildAuthority(Box::new(prepared.authority.clone())),
        ))
        .await
        .unwrap();
    target_store
        .journal_build(&kuberic_protocol::command::EnsureReplicaBuild {
            operation_id: build_id.clone(),
            local_replica_id: target.replica_id,
            expected_instance_id: target.instance_id.clone(),
            expected_agent_generation: target.agent_generation.clone(),
            target: target.clone(),
            authority: Some(prepared.authority.clone()),
            source_session_id: Some(ProcessSessionId::new("sqlite-source-session")),
            retire: false,
        })
        .await
        .unwrap();

    let catch_up_boundary = snapshot_items
        .iter()
        .find_map(|item| item.catch_up_boundary_lsn)
        .unwrap();
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        source.replica_id,
        vec![
            ConfigurationMember {
                identity: source.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: target.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        current_policy.write_quorum,
    );
    let mut intent = ScaleUpIntent {
        operation_id: OperationId::default(),
        resource_uid: resource_uid.clone(),
        spec_generation: 2,
        desired_replicas: 2,
        previous_configuration: previous.clone(),
        current_configuration: current.clone(),
        previous_policy: previous_policy.clone(),
        current_policy: current_policy.clone(),
        primary: source.clone(),
        target: target.clone(),
        build_id: build_id.clone(),
        snapshot_boundary_lsn: prepared.authority.replication_boundary_lsn,
        catch_up_boundary_lsn: catch_up_boundary,
    };
    intent.operation_id = intent.expected_operation_id();
    let before_copy = target_store.load_state().await.unwrap();

    for item in snapshot_items {
        let acknowledgement = target_runtime
            .data_plane()
            .receive_copy_item(item)
            .await
            .unwrap();
        source_runtime
            .data_plane()
            .accept_copy_acknowledgement(acknowledgement)
            .await
            .unwrap();
    }
    let source_receiver_cut = source_runtime.snapshot().await;
    let target_receiver_cut = target_runtime.snapshot().await;
    let acknowledgement = target_runtime
        .data_plane()
        .receive_copy_item(live_item)
        .await
        .unwrap();
    source_runtime
        .data_plane()
        .accept_copy_acknowledgement(acknowledgement)
        .await
        .unwrap();
    let application_path = target_root.join("application-state.json");
    let persisted_operations = target_application
        .applied
        .lock()
        .unwrap()
        .values()
        .map(|operation| {
            (
                operation.lsn,
                operation.committed_lsn,
                operation.data.to_vec(),
            )
        })
        .collect::<Vec<_>>();
    let persisted_progress = *target_application.progress.lock().unwrap();
    std::fs::write(
        &application_path,
        serde_json::to_vec(&(
            persisted_operations,
            persisted_progress.applied_lsn,
            persisted_progress.committed_lsn,
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        target_application
            .durable_progress()
            .await
            .unwrap()
            .applied_lsn,
        2
    );
    let source_snapshot = source_runtime.snapshot().await;
    let target_snapshot = target_runtime.snapshot().await;
    let report_builds = |snapshot: &kuberic_runtime_internal::effects::RuntimeSnapshot| {
        snapshot
            .builds
            .iter()
            .map(|build| AgentBuildReport {
                build_id: build.authority.build_id.clone(),
                target: build.authority.target.clone(),
                last_sequence: build.last_sequence,
                replication_boundary_lsn: build.authority.replication_boundary_lsn,
                durable_lsn: build.durable_lsn,
                completed: build.completed,
                catch_up_boundary_lsn: build.catch_up_boundary_lsn,
            })
            .collect::<Vec<_>>()
    };
    let expected_transition = kuberic_protocol::types::TransitionIntent {
        transition_id: intent.transition_id(TransitionKind::ScaleUp, &current),
        kind: TransitionKind::ScaleUp,
        spec_generation: intent.spec_generation,
        effective_policy: current_policy.clone(),
        previous_configuration_id: Some(previous.configuration_id.clone()),
        current_configuration: current.clone(),
        election_lsn: None,
        build_id: Some(build_id.clone()),
        repair: None,
        switchover: None,
        secondary_scale_down: None,
        secondary_removal_evidence: None,
        scale_up: Some(Box::new(intent.clone())),
        scale_up_failover: None,
    };
    let snapshot = ObservationSnapshot {
        resource_uid: resource_uid.clone(),
        resource_version: "1".into(),
        desired: DesiredState {
            generation: 2,
            replicas: 2,
            image: "example:v1".into(),
            failover_delay_seconds: 30,
            switchover: None,
        },
        status: AcceptedStatus {
            initialized: true,
            observed_generation: 1,
            effective_policy: Some(previous_policy.clone()),
            topology: Some(AcceptedTopology {
                configuration: previous.clone(),
            }),
            provisioning: Some(provisioning.clone()),
            ..Default::default()
        },
        replicas: BTreeMap::from([
            (
                ReplicaObservationKey::new(source.replica_id, source.instance_id.clone()),
                ReplicaObservation {
                    kubernetes: Some(KubernetesReplicaObservation {
                        replica_id: source.replica_id,
                        pod_name: source.instance_id.to_string(),
                        pod_uid: Some(PodUid::new(source.instance_id.as_str())),
                        pvc_name: "sqlite-source-data".into(),
                        pvc_uid: Some(PvcUid::new("sqlite-source-pvc")),
                        image: Some("example:v1".into()),
                        pod_ready: true,
                        peer_endpoint_ready: true,
                    }),
                    agent: AgentObservation::Report(Box::new(AgentReport {
                        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                        resource_uid: resource_uid.clone(),
                        identity: source.clone(),
                        process_session_id: ProcessSessionId::new("source-report"),
                        report_sequence: 1,
                        role: ReplicaRole::Primary,
                        read_status: AccessStatus::Granted,
                        write_status: AccessStatus::Granted,
                        healthy: true,
                        epoch: previous.epoch,
                        previous_configuration: None,
                        current_configuration: Some(previous.clone()),
                        current_progress: source_snapshot.current_progress,
                        verified_replication_lsn: source_snapshot.verified_replication_lsn,
                        committed_lsn: source_snapshot.committed_lsn,
                        builds: report_builds(&source_snapshot),
                        ..Default::default()
                    })),
                },
            ),
            (
                ReplicaObservationKey::new(target.replica_id, target.instance_id.clone()),
                ReplicaObservation {
                    kubernetes: Some(KubernetesReplicaObservation {
                        replica_id: target.replica_id,
                        pod_name: target.instance_id.to_string(),
                        pod_uid: Some(provisioning.pod_uid.clone()),
                        pvc_name: "sqlite-target-data".into(),
                        pvc_uid: Some(provisioning.pvc_uid.clone()),
                        image: Some("example:v1".into()),
                        pod_ready: true,
                        peer_endpoint_ready: true,
                    }),
                    agent: AgentObservation::Report(Box::new(AgentReport {
                        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                        resource_uid: resource_uid.clone(),
                        identity: target.clone(),
                        process_session_id: ProcessSessionId::new("target-report"),
                        report_sequence: 1,
                        role: ReplicaRole::IdleSecondary,
                        read_status: AccessStatus::NotPrimary,
                        write_status: AccessStatus::NotPrimary,
                        healthy: true,
                        epoch: Epoch::default(),
                        current_progress: target_snapshot.current_progress,
                        verified_replication_lsn: None,
                        committed_lsn: target_snapshot.committed_lsn,
                        builds: report_builds(&target_snapshot),
                        ..Default::default()
                    })),
                },
            ),
        ]),
        secondary_scale_down_resources: Vec::new(),
        previous_report_watermarks: BTreeMap::new(),
        durable_storage_evidence: true,
        supporting_resources_ready: true,
        routing: RoutingObservation {
            service_present: true,
            unresolved_write_target: false,
            write_target: Some(source.clone()),
        },
        observation_failures: Vec::new(),
        now_unix_seconds: 100,
    };
    let config = EvaluationConfig {
        allow_scale_up: true,
        ..Default::default()
    };
    for (source_cut, target_cut) in [
        (&source_enumeration_cut, None),
        (&source_receiver_cut, Some(&target_receiver_cut)),
    ] {
        let mut incomplete = snapshot.clone();
        let source_report = incomplete
            .replicas
            .values_mut()
            .find_map(|observation| match &mut observation.agent {
                AgentObservation::Report(report)
                    if report.identity.replica_id == source.replica_id =>
                {
                    Some(report)
                }
                _ => None,
            })
            .unwrap();
        source_report.builds = report_builds(source_cut);
        source_report.current_progress = source_cut.current_progress;
        source_report.committed_lsn = source_cut.committed_lsn;
        let target_report = incomplete
            .replicas
            .values_mut()
            .find_map(|observation| match &mut observation.agent {
                AgentObservation::Report(report)
                    if report.identity.replica_id == target.replica_id =>
                {
                    Some(report)
                }
                _ => None,
            })
            .unwrap();
        target_report.builds = target_cut.map_or_else(Vec::new, report_builds);
        target_report.current_progress = target_cut.map_or(0, |cut| cut.current_progress);
        target_report.committed_lsn = target_cut.map_or(0, |cut| cut.committed_lsn);
        for _ in 0..3 {
            match evaluate(&incomplete, &config) {
                Plan::Execute {
                    command: kuberic_protocol::command::ProtocolCommand::EnsureConfiguration(_),
                } => panic!("incomplete runtime copy produced configuration command"),
                Plan::Apply { changes } => {
                    for change in changes {
                        if let kuberic_protocol::command::KubernetesChange::PersistStatus {
                            status,
                        } = change
                        {
                            assert!(status.transition.is_none());
                            assert!(status.scale_up_admission_started.is_none());
                            incomplete.status = *status;
                        }
                    }
                }
                Plan::Wait { status, .. } => {
                    assert!(status.transition.is_none());
                    assert!(status.scale_up_admission_started.is_none());
                    incomplete.status = status;
                }
                _ => {}
            }
        }
    }
    let mut snapshot = snapshot;
    let Plan::Apply { changes } = evaluate(&snapshot, &config) else {
        panic!("completed durable copy must persist evaluator transition")
    };
    snapshot.status = changes
        .into_iter()
        .find_map(|change| match change {
            kuberic_protocol::command::KubernetesChange::PersistStatus { status } => Some(*status),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        snapshot.status.transition.as_ref(),
        Some(&expected_transition)
    );
    assert!(snapshot.status.scale_up_admission_started.is_none());
    let Plan::Apply { changes } = evaluate(&snapshot, &config) else {
        panic!("evaluator must persist admission fence")
    };
    snapshot.status = changes
        .into_iter()
        .find_map(|change| match change {
            kuberic_protocol::command::KubernetesChange::PersistStatus { status } => Some(*status),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        snapshot.status.scale_up_admission_started.as_ref(),
        Some(&intent.operation_id)
    );
    let Plan::Execute {
        command: kuberic_protocol::command::ProtocolCommand::EnsureConfiguration(target_command),
    } = evaluate(&snapshot, &config)
    else {
        panic!("completed durable copy must produce evaluator configuration command")
    };
    assert_eq!(target_command.local_replica_id, target.replica_id);
    std::fs::write(
        root.join("configuration-command.json"),
        serde_json::to_vec(target_command.as_ref()).unwrap(),
    )
    .unwrap();
    assert!(kuberic_agent::command::admit_configuration(&target_command, &before_copy).is_err());
    target_runtime
        .apply_effect(effect(4, RuntimeEffectAction::Close))
        .await
        .unwrap();
    target_runtime.abort();
    *target_application.partition.lock().unwrap() = None;
    *target_application.factory.lock().unwrap() = None;
    *target_application.state_replicator.lock().unwrap() = None;
    *target_application.returned_control.lock().unwrap() = None;
    target_application.held_streams.lock().unwrap().clear();
    drop(target_runtime);
    let original_application = Arc::as_ptr(&target_application) as usize;
    drop(target_application);
    drop(target_store);
    if env::var("KUBERIC_SQLITE_SCALE_UP_WRITER").as_deref() == Ok("1") {
        return;
    }

    let (persisted_operations, applied_lsn, committed_lsn): PersistedApplicationState =
        serde_json::from_slice(&std::fs::read(&application_path).unwrap()).unwrap();
    let reopened_application = Arc::new(TestApplication::default());
    assert_ne!(
        Arc::as_ptr(&reopened_application) as usize,
        original_application
    );
    {
        let mut applied = reopened_application.applied.lock().unwrap();
        for (lsn, committed_lsn, data) in persisted_operations {
            applied.insert(
                lsn,
                Operation {
                    lsn,
                    committed_lsn,
                    data: Bytes::from(data),
                },
            );
        }
        *reopened_application.progress.lock().unwrap() = DurableApplicationProgress {
            applied_lsn,
            committed_lsn,
        };
    }

    let reopened_store = Arc::new(SqliteStore::open_existing(&target_path, None).unwrap());
    let reopened_runtime = Arc::new(PodRuntime::new(
        target.clone(),
        reopened_application.clone(),
        reopened_store.clone(),
    ));
    let service = AgentService::new(
        reopened_store.clone(),
        reopened_runtime.clone(),
        reopened_runtime.clone(),
        "token",
    )
    .unwrap();
    service.reconstruct_runtime().await.unwrap();
    let reopened_state = reopened_store.load_state().await.unwrap();
    assert!(kuberic_agent::command::admit_configuration(&target_command, &reopened_state).is_ok());
    Coordinator::new(reopened_store.clone(), reopened_runtime.clone())
        .ensure_configuration(*target_command)
        .await
        .unwrap();
    assert_eq!(
        reopened_store.load_state().await.unwrap().role,
        ReplicaRole::ActiveSecondary
    );
    assert_eq!(
        reopened_application
            .durable_progress()
            .await
            .unwrap()
            .applied_lsn,
        2
    );
    assert_eq!(
        reopened_application
            .applied
            .lock()
            .unwrap()
            .get(&2)
            .unwrap()
            .data,
        Bytes::from_static(b"live-value")
    );
}

async fn verify_persisted_scale_up_after_process_exit(root: &Path) {
    let target_root = root.join("target");
    let target_path = SqliteStore::metadata_database_path(&target_root);
    let command: EnsureConfiguration =
        serde_json::from_slice(&std::fs::read(root.join("configuration-command.json")).unwrap())
            .unwrap();
    let (persisted_operations, applied_lsn, committed_lsn): PersistedApplicationState =
        serde_json::from_slice(&std::fs::read(target_root.join("application-state.json")).unwrap())
            .unwrap();
    let application = Arc::new(TestApplication::default());
    {
        let mut applied = application.applied.lock().unwrap();
        for (lsn, committed_lsn, data) in persisted_operations {
            applied.insert(
                lsn,
                Operation {
                    lsn,
                    committed_lsn,
                    data: Bytes::from(data),
                },
            );
        }
        *application.progress.lock().unwrap() = DurableApplicationProgress {
            applied_lsn,
            committed_lsn,
        };
    }
    let store = Arc::new(SqliteStore::open_existing(&target_path, None).unwrap());
    let identity = store.identity().await.unwrap().local_identity;
    let runtime = Arc::new(PodRuntime::new(
        identity,
        application.clone(),
        store.clone(),
    ));
    let service =
        AgentService::new(store.clone(), runtime.clone(), runtime.clone(), "token").unwrap();
    service.reconstruct_runtime().await.unwrap();
    Coordinator::new(store.clone(), runtime)
        .ensure_configuration(command)
        .await
        .unwrap();
    assert_eq!(
        store.load_state().await.unwrap().role,
        ReplicaRole::ActiveSecondary
    );
    assert_eq!(application.durable_progress().await.unwrap().applied_lsn, 2);
    assert_eq!(
        application.applied.lock().unwrap().get(&2).unwrap().data,
        Bytes::from_static(b"live-value")
    );
}

#[tokio::test]
async fn scale_up_runtime_requires_the_built_candidate_through_the_frozen_boundary() {
    let primary = identity(1, "primary");
    let candidate = identity(2, "candidate");
    let previous_policy = kuberic_protocol::types::EffectivePolicy::fixed(1, 30).unwrap();
    let current_policy = kuberic_protocol::types::EffectivePolicy::fixed(2, 30).unwrap();
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        primary.replica_id,
        vec![ConfigurationMember {
            identity: primary.clone(),
            role: ReplicaRole::Primary,
        }],
        previous_policy.write_quorum,
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        primary.replica_id,
        vec![
            ConfigurationMember {
                identity: primary.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: candidate.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        current_policy.write_quorum,
    );
    let mut intent = ScaleUpIntent {
        operation_id: OperationId::default(),
        resource_uid: ResourceUid::new("set"),
        spec_generation: 2,
        desired_replicas: 2,
        previous_configuration: previous.clone(),
        current_configuration: current.clone(),
        previous_policy,
        current_policy,
        primary: primary.clone(),
        target: candidate.clone(),
        build_id: OperationId::new("build"),
        snapshot_boundary_lsn: 0,
        catch_up_boundary_lsn: 2,
    };
    intent.operation_id = intent.expected_operation_id();
    let admitted = AdmittedAuthority {
        scale_up: Some(Box::new(ScaleUpConfigurationEvidence::Admission { intent })),
        secondary_removal: None,
        local_identity: primary.clone(),
        transition_kind: Some(TransitionKind::ScaleUp),
        previous_configuration: Some(previous),
        current_configuration: current,
        switchover_handoff: None,
    };
    let application = Arc::new(TestApplication::default());
    application.seed_progress(2);
    let runtime = PodRuntime::new(
        primary,
        application,
        Arc::new(MemoryAuthorityStore::default()),
    );
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(admitted.clone())),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    let waiting = runtime.snapshot().await;
    assert_eq!(waiting.catch_up_boundary, Some(2));
    assert!(!waiting.catch_up_complete);
    runtime
        .data_plane()
        .accept_acknowledgement(acknowledgement(&admitted, candidate.clone(), 2))
        .await
        .unwrap();
    assert!(runtime.snapshot().await.catch_up_complete);
    let mut completed = admitted;
    completed.previous_configuration = None;
    completed.transition_kind = None;
    runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::AdmitAuthority(Box::new(completed)),
        ))
        .await
        .unwrap();
    assert!(runtime.snapshot().await.catch_up_complete);
}

#[tokio::test]
async fn same_primary_scale_up_preserves_granted_access_and_fences_old_completions() {
    let primary = identity(1, "primary");
    let secondary = identity(2, "secondary");
    let candidate = identity(3, "candidate");
    let previous_authority = authority(primary.clone(), vec![primary.clone(), secondary.clone()]);
    let previous_policy = kuberic_protocol::types::EffectivePolicy::fixed(2, 30).unwrap();
    let current_policy = kuberic_protocol::types::EffectivePolicy::fixed(3, 30).unwrap();
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        primary.replica_id,
        vec![
            ConfigurationMember {
                identity: primary.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: secondary.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: candidate.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        current_policy.write_quorum,
    );
    let mut intent = ScaleUpIntent {
        operation_id: OperationId::default(),
        resource_uid: ResourceUid::new("set"),
        spec_generation: 2,
        desired_replicas: 3,
        previous_configuration: previous_authority.current_configuration.clone(),
        current_configuration: current.clone(),
        previous_policy,
        current_policy,
        primary: primary.clone(),
        target: candidate,
        build_id: OperationId::new("build"),
        snapshot_boundary_lsn: 0,
        catch_up_boundary_lsn: 0,
    };
    intent.operation_id = intent.expected_operation_id();
    let scale_up = AdmittedAuthority {
        scale_up: Some(Box::new(ScaleUpConfigurationEvidence::Admission { intent })),
        secondary_removal: None,
        local_identity: primary.clone(),
        transition_kind: Some(TransitionKind::ScaleUp),
        previous_configuration: Some(previous_authority.current_configuration.clone()),
        current_configuration: current,
        switchover_handoff: None,
    };
    let runtime = Arc::new(PodRuntime::new(
        primary,
        Arc::new(TestApplication::default()),
        Arc::new(MemoryAuthorityStore::default()),
    ));
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(previous_authority)),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let pending = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("old-authority-write"),
            data: Bytes::from_static(b"old"),
        })
        .await
        .unwrap();
    let admission_runtime = runtime.clone();
    let admitted = scale_up.clone();
    let admission = tokio::spawn(async move {
        admission_runtime
            .apply_effect(effect(
                5,
                RuntimeEffectAction::AdmitAuthority(Box::new(admitted)),
            ))
            .await
    });
    loop {
        let Some(OutboundReplication::Replication(item)) =
            runtime.data_plane().next_outbound().await
        else {
            continue;
        };
        let receiver: ReplicaIdentity = item.receiver.clone().unwrap().try_into().unwrap();
        if receiver == secondary {
            runtime
                .data_plane()
                .accept_acknowledgement(acknowledgement(&scale_up, secondary.clone(), item.lsn))
                .await
                .unwrap();
            break;
        }
    }
    admission.await.unwrap().unwrap();
    assert_eq!(runtime.snapshot().await.write_status, AccessStatus::Granted);
    let old_completion = pending.committed().await;
    assert!(
        matches!(old_completion, Err(RuntimeError::AuthorityMismatch(_))),
        "{old_completion:?}"
    );
    let next = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("new-authority-write"),
            data: Bytes::from_static(b"new"),
        })
        .await
        .unwrap();
    runtime
        .data_plane()
        .accept_acknowledgement(acknowledgement(&scale_up, secondary, 2))
        .await
        .unwrap();
    next.committed().await.unwrap();
}

#[tokio::test]
async fn same_primary_scale_up_accepts_real_writes_after_every_durable_agent_effect() {
    let primary = identity(1, "durable-primary");
    let secondary = identity(2, "durable-secondary");
    let candidate = identity(3, "durable-candidate");
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
                identity: secondary.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        previous_policy.write_quorum,
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        primary.replica_id,
        vec![
            ConfigurationMember {
                identity: primary.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: secondary.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: candidate.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        current_policy.write_quorum,
    );
    let build = BuildAuthority {
        build_id: OperationId::new("durable-scale-up-build"),
        kind: BuildAuthorityKind::Provisioning,
        source: primary.clone(),
        target: candidate.clone(),
        current_configuration: previous.clone(),
        replication_boundary_lsn: 0,
    };
    let mut intent = ScaleUpIntent {
        operation_id: OperationId::default(),
        resource_uid: ResourceUid::new("durable-scale-up-set"),
        spec_generation: 2,
        desired_replicas: 3,
        previous_configuration: previous.clone(),
        current_configuration: current.clone(),
        previous_policy: previous_policy.clone(),
        current_policy: current_policy.clone(),
        primary: primary.clone(),
        target: candidate.clone(),
        build_id: build.build_id.clone(),
        snapshot_boundary_lsn: 0,
        catch_up_boundary_lsn: 0,
    };
    intent.operation_id = intent.expected_operation_id();
    let evidence = ScaleUpConfigurationEvidence::Admission {
        intent: intent.clone(),
    };
    let configuration = |current_only: bool| EnsureConfiguration {
        operation_id: intent.command_operation_id(
            if current_only {
                kuberic_protocol::types::ScaleUpStage::CurrentOnly
            } else {
                kuberic_protocol::types::ScaleUpStage::PreviousCurrent
            },
            &primary,
            &current,
        ),
        previous_configuration: (!current_only).then_some(previous.clone()),
        current_configuration: current.clone(),
        previous_epoch: (!current_only).then_some(previous.epoch),
        current_epoch: current.epoch,
        effective_policy: current_policy.clone(),
        previous_policy: Some(previous_policy.clone()),
        secondary_removal_evidence: None,
        scale_up_evidence: Some(Box::new(evidence.clone())),
        local_replica_id: primary.replica_id,
        expected_instance_id: primary.instance_id.clone(),
        expected_agent_generation: primary.agent_generation.clone(),
        transition_kind: TransitionKind::ScaleUp,
        failover_safe_lsn: None,
        primary_write_status: AccessStatus::Granted,
        current_only,
        retire_build_ids: current_only
            .then_some(vec![build.build_id.clone()])
            .unwrap_or_default(),
        switchover_handoff: None,
        retire_switchover_preparation_ids: Vec::new(),
    };
    let mut state = AgentState::new(StorageIdentity {
        schema_version: SCHEMA_VERSION,
        resource_uid: intent.resource_uid.clone(),
        pod_uid: PodUid::new(primary.instance_id.as_str()),
        pvc_uid: PvcUid::new("durable-primary-pvc"),
        initialization_id: InitializationId::new("durable-primary-initialization"),
        local_identity: primary.clone(),
        effective_policy: previous_policy.clone(),
    });
    state.admitted_policy = Some(previous_policy.clone());
    state.highest_epoch = previous.epoch;
    state.current_configuration = Some(previous.clone());
    state.role = ReplicaRole::Primary;
    state.read_status = AccessStatus::Granted;
    state.write_status = AccessStatus::Granted;
    state.next_effect_sequence = 5;
    state.build_commands.insert(
        build.build_id.clone(),
        kuberic_protocol::command::EnsureReplicaBuild {
            operation_id: build.build_id.clone(),
            local_replica_id: primary.replica_id,
            expected_instance_id: primary.instance_id.clone(),
            expected_agent_generation: primary.agent_generation.clone(),
            target: candidate.clone(),
            authority: None,
            source_session_id: None,
            retire: false,
        },
    );
    let directory = tempfile::tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let store = Arc::new(SqliteStore::create_authorized(&path, state).unwrap());
    let previous_authority = AdmittedAuthority {
        local_identity: primary.clone(),
        transition_kind: None,
        previous_configuration: None,
        current_configuration: previous.clone(),
        switchover_handoff: None,
        secondary_removal: None,
        scale_up: None,
    };
    store.admit(&previous_authority).await.unwrap();
    store.admit_build(&build).await.unwrap();
    store
        .record_build_progress(&DurableBuildProgress {
            authority: build.clone(),
            last_sequence: 1,
            durable_lsn: 0,
            completed: true,
            catch_up_boundary_lsn: Some(0),
        })
        .await
        .unwrap();
    let runtime = Arc::new(PodRuntime::new(
        primary.clone(),
        Arc::new(TestApplication::default()),
        store.clone(),
    ));
    for (sequence, action) in [
        RuntimeEffectAction::Open(OpenMode::Existing),
        RuntimeEffectAction::AdmitAuthority(Box::new(previous_authority)),
        RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::Granted,
            write: AccessStatus::Granted,
        },
    ]
    .into_iter()
    .enumerate()
    {
        runtime
            .apply_effect(effect(sequence as u64 + 1, action))
            .await
            .unwrap();
    }
    let adapter = RuntimeAdapter::new(store.clone(), runtime.clone());
    let mut effect_sequence = store.load_state().await.unwrap().next_effect_sequence;
    let mut write_sequence = 0_u64;
    let write_after = |label: &'static str,
                       authority: AdmittedAuthority,
                       runtime: Arc<PodRuntime>,
                       secondary: ReplicaIdentity,
                       candidate: ReplicaIdentity,
                       operation: u64| async move {
        let pending = runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new(format!("durable-scale-up-{label}-{operation}")),
                data: Bytes::from(format!("{label}-{operation}")),
            })
            .await
            .unwrap();
        runtime
            .data_plane()
            .accept_acknowledgement(acknowledgement(&authority, secondary, pending.lsn))
            .await
            .unwrap();
        runtime
            .data_plane()
            .accept_acknowledgement(acknowledgement(&authority, candidate, pending.lsn))
            .await
            .unwrap();
        pending.committed().await.unwrap();
    };

    let pc_cc = configuration(false);
    store.begin_configuration(&pc_cc).await.unwrap();
    let pc_cc_authority =
        kuberic_agent::command::admit_configuration(&pc_cc, &store.load_state().await.unwrap())
            .unwrap();
    adapter
        .execute(RuntimeEffect {
            operation_id: OperationId::new(format!("{}:admit-authority", pc_cc.operation_id)),
            sequence: effect_sequence,
            action: RuntimeEffectAction::AdmitAuthority(Box::new(pc_cc_authority.clone())),
        })
        .await
        .unwrap();
    effect_sequence += 1;
    store
        .advance_configuration(
            &pc_cc.operation_id,
            CoordinatorStage::AdmitAuthority,
            CoordinatorStage::Activate,
            None,
        )
        .await
        .unwrap();
    write_sequence += 1;
    write_after(
        "pc-cc-authority",
        pc_cc_authority.clone(),
        runtime.clone(),
        secondary.clone(),
        candidate.clone(),
        write_sequence,
    )
    .await;
    adapter
        .execute(RuntimeEffect {
            operation_id: OperationId::new(format!("{}:activate", pc_cc.operation_id)),
            sequence: effect_sequence,
            action: RuntimeEffectAction::SetAccessStatus {
                read: AccessStatus::Granted,
                write: AccessStatus::Granted,
            },
        })
        .await
        .unwrap();
    effect_sequence += 1;
    store
        .advance_configuration(
            &pc_cc.operation_id,
            CoordinatorStage::Activate,
            CoordinatorStage::Complete,
            None,
        )
        .await
        .unwrap();
    write_sequence += 1;
    write_after(
        "pc-cc-access",
        pc_cc_authority.clone(),
        runtime.clone(),
        secondary.clone(),
        candidate.clone(),
        write_sequence,
    )
    .await;
    store
        .complete_configuration(&pc_cc.operation_id)
        .await
        .unwrap();
    write_sequence += 1;
    write_after(
        "pc-cc-completion",
        pc_cc_authority,
        runtime.clone(),
        secondary.clone(),
        candidate.clone(),
        write_sequence,
    )
    .await;

    let current_only = configuration(true);
    store.begin_configuration(&current_only).await.unwrap();
    let current_only_authority = kuberic_agent::command::admit_configuration(
        &current_only,
        &store.load_state().await.unwrap(),
    )
    .unwrap();
    adapter
        .execute(RuntimeEffect {
            operation_id: OperationId::new(format!(
                "{}:admit-authority",
                current_only.operation_id
            )),
            sequence: effect_sequence,
            action: RuntimeEffectAction::AdmitAuthority(Box::new(current_only_authority.clone())),
        })
        .await
        .unwrap();
    effect_sequence += 1;
    store
        .advance_configuration(
            &current_only.operation_id,
            CoordinatorStage::AdmitAuthority,
            CoordinatorStage::Activate,
            None,
        )
        .await
        .unwrap();
    write_sequence += 1;
    write_after(
        "current-only-authority",
        current_only_authority.clone(),
        runtime.clone(),
        secondary.clone(),
        candidate.clone(),
        write_sequence,
    )
    .await;
    adapter
        .execute(RuntimeEffect {
            operation_id: OperationId::new(format!("{}:activate", current_only.operation_id)),
            sequence: effect_sequence,
            action: RuntimeEffectAction::SetAccessStatus {
                read: AccessStatus::Granted,
                write: AccessStatus::Granted,
            },
        })
        .await
        .unwrap();
    effect_sequence += 1;
    store
        .advance_configuration(
            &current_only.operation_id,
            CoordinatorStage::Activate,
            CoordinatorStage::RetireBuild,
            None,
        )
        .await
        .unwrap();
    write_sequence += 1;
    write_after(
        "current-only-access",
        current_only_authority.clone(),
        runtime.clone(),
        secondary.clone(),
        candidate.clone(),
        write_sequence,
    )
    .await;
    adapter
        .execute(RuntimeEffect {
            operation_id: OperationId::new(format!("{}:retire-build-0", current_only.operation_id)),
            sequence: effect_sequence,
            action: RuntimeEffectAction::RetireBuild(build.build_id.clone()),
        })
        .await
        .unwrap();
    store
        .advance_configuration(
            &current_only.operation_id,
            CoordinatorStage::RetireBuild,
            CoordinatorStage::Complete,
            None,
        )
        .await
        .unwrap();
    write_sequence += 1;
    write_after(
        "build-retirement",
        current_only_authority.clone(),
        runtime.clone(),
        secondary.clone(),
        candidate.clone(),
        write_sequence,
    )
    .await;
    store
        .complete_configuration(&current_only.operation_id)
        .await
        .unwrap();
    write_sequence += 1;
    write_after(
        "current-only-completion",
        current_only_authority,
        runtime,
        secondary,
        candidate,
        write_sequence,
    )
    .await;
}

#[tokio::test]
async fn planned_handoff_roles_converge_closed_and_catchup_uses_certified_boundary() {
    for compensate in [false, true] {
        planned_handoff_role_recovery(compensate).await;
    }
}

async fn planned_handoff_role_recovery(compensate: bool) {
    let identities = [
        identity(1, "source"),
        identity(2, "target"),
        identity(3, "third"),
    ];
    let starting = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        identities[0].replica_id,
        identities
            .iter()
            .enumerate()
            .map(|(index, identity)| ConfigurationMember {
                identity: identity.clone(),
                role: if index == 0 {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        2,
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        identities[1].replica_id,
        identities
            .iter()
            .enumerate()
            .map(|(index, identity)| ConfigurationMember {
                identity: identity.clone(),
                role: if index == 1 {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        2,
    );
    let handoff = SwitchoverHandoff {
        preparation_generation: 1,
        preparation_operation_id: OperationId::new("planned-prepare"),
        request_id: SwitchoverRequestId::new("planned-request"),
        source: identities[0].clone(),
        target: identities[1].clone(),
        starting_configuration_id: starting.configuration_id.clone(),
        starting_epoch: starting.epoch,
        handoff_lsn: 7,
    };
    let mut runtimes = Vec::new();
    let mut applications = Vec::new();
    for (index, identity) in identities.iter().enumerate() {
        let store = Arc::new(MemoryAuthorityStore::default());
        let application = Arc::new(TestApplication::default());
        application.seed_progress(if index == 1 { 9 } else { 7 });
        let authority = AdmittedAuthority {
            scale_up: None,
            secondary_removal: None,
            local_identity: identity.clone(),
            transition_kind: None,
            previous_configuration: None,
            current_configuration: starting.clone(),
            switchover_handoff: None,
        };
        store.replication_progress.lock().unwrap().insert(
            authority.fence(),
            ReplicationProgress {
                fence: authority.fence(),
                verified_lsn: 7,
            },
        );
        applications.push(application.clone());
        let runtime = PodRuntime::new(identity.clone(), application, store);
        runtime
            .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
            .await
            .unwrap();
        runtime
            .apply_effect(effect(
                2,
                RuntimeEffectAction::AdmitAuthority(Box::new(authority)),
            ))
            .await
            .unwrap();
        runtime
            .apply_effect(effect(
                3,
                RuntimeEffectAction::ChangeRole(starting.members[index].role),
            ))
            .await
            .unwrap();
        runtime
            .apply_effect(effect(
                4,
                RuntimeEffectAction::SetWriteStatus(if index == 0 {
                    AccessStatus::Granted
                } else {
                    AccessStatus::NotPrimary
                }),
            ))
            .await
            .unwrap();
        runtimes.push(runtime);
    }
    let retained_clients: Vec<_> = runtimes.iter().map(PodRuntime::data_plane).collect();
    let prepared = runtimes[0]
        .apply_effect(RuntimeEffect {
            operation_id: handoff.preparation_operation_id.clone(),
            sequence: 5,
            action: RuntimeEffectAction::PrepareSwitchover {
                preparation_generation: handoff.preparation_generation,
                request_id: handoff.request_id.clone(),
                source: handoff.source.clone(),
                target: handoff.target.clone(),
                starting_configuration_id: handoff.starting_configuration_id.clone(),
                starting_epoch: handoff.starting_epoch,
            },
        })
        .await
        .unwrap();
    assert_eq!(prepared.postcondition.current_progress, 7);
    let mut sequences = [0; 3];
    for index in [0, 2, 1] {
        let runtime = &runtimes[index];
        let authority = AdmittedAuthority {
            scale_up: None,
            secondary_removal: None,
            local_identity: identities[index].clone(),
            transition_kind: Some(TransitionKind::PlannedSwitchover),
            previous_configuration: Some(starting.clone()),
            current_configuration: current.clone(),
            switchover_handoff: Some(handoff.clone()),
        };
        let first_sequence = if index == 0 { 6 } else { 5 };
        let mut actions = vec![
            RuntimeEffectAction::AdmitAuthority(Box::new(authority.clone())),
            RuntimeEffectAction::SetReadStatus(AccessStatus::ReconfigurationPending),
            RuntimeEffectAction::RefreshApplicationProgress,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::ReconfigurationPending),
            RuntimeEffectAction::ChangeReplicatorRole(current.members[index].role),
        ];
        if index == 1 {
            actions.push(RuntimeEffectAction::UpdateEpoch);
        }
        actions.push(RuntimeEffectAction::ChangeApplicationRole(
            current.members[index].role,
        ));
        let mut next = first_sequence;
        for action in actions {
            runtime.apply_effect(effect(next, action)).await.unwrap();
            next += 1;
            for participant in &runtimes {
                assert_ne!(
                    participant.snapshot().await.write_status,
                    AccessStatus::Granted
                );
            }
            for client in &retained_clients {
                assert!(
                    client
                        .begin_write(ClientWrite {
                            operation_id: OperationId::new("direct-during-role-convergence"),
                            data: Bytes::from_static(b"must-not-commit"),
                        })
                        .await
                        .is_err()
                );
            }
        }
        if index == 1 {
            assert_eq!(runtime.snapshot().await.catch_up_boundary, Some(7));
            assert_eq!(runtime.snapshot().await.current_progress, 9);
            assert_eq!(runtime.snapshot().await.verified_replication_lsn, Some(7));
            assert!(!runtime.snapshot().await.catch_up_complete);
            runtime
                .data_plane()
                .accept_acknowledgement(acknowledgement(&authority, identities[0].clone(), 7))
                .await
                .unwrap();
            timeout(
                Duration::from_secs(1),
                runtime.apply_effect(effect(next, RuntimeEffectAction::WaitForCatchup)),
            )
            .await
            .unwrap()
            .unwrap();
            next += 1;
        }
        runtime
            .apply_effect(effect(
                next,
                RuntimeEffectAction::SetAccessStatus {
                    read: AccessStatus::Granted,
                    write: if index == 1 {
                        AccessStatus::ReconfigurationPending
                    } else {
                        AccessStatus::NotPrimary
                    },
                },
            ))
            .await
            .unwrap();
        next += 1;
        assert!(
            retained_clients[index]
                .begin_write(ClientWrite {
                    operation_id: OperationId::new(format!("before-acceptance-{index}")),
                    data: Bytes::from_static(b"must-not-commit"),
                })
                .await
                .is_err()
        );
        sequences[index] = next;
    }
    for index in [0, 2, 1] {
        let runtime = &runtimes[index];
        runtime
            .apply_effect(effect(
                sequences[index],
                RuntimeEffectAction::AdmitAuthority(Box::new(AdmittedAuthority {
                    scale_up: None,
                    secondary_removal: None,
                    local_identity: identities[index].clone(),
                    transition_kind: None,
                    previous_configuration: None,
                    current_configuration: current.clone(),
                    switchover_handoff: Some(handoff.clone()),
                })),
            ))
            .await
            .unwrap();
        assert_ne!(runtime.snapshot().await.write_status, AccessStatus::Granted);
    }
    if compensate {
        let compensation = ConfigurationDescriptor::new(
            Epoch::new(0, 3),
            identities[0].replica_id,
            starting.members.clone(),
            starting.write_quorum,
        );
        for index in [1, 2, 0] {
            let runtime = &runtimes[index];
            let authority = AdmittedAuthority {
                scale_up: None,
                secondary_removal: None,
                local_identity: identities[index].clone(),
                transition_kind: Some(TransitionKind::PlannedSwitchover),
                previous_configuration: Some(current.clone()),
                current_configuration: compensation.clone(),
                switchover_handoff: Some(handoff.clone()),
            };
            let mut next = sequences[index] + 1;
            for action in [
                RuntimeEffectAction::AdmitAuthority(Box::new(authority.clone())),
                RuntimeEffectAction::ChangeRole(starting.members[index].role),
            ] {
                runtime.apply_effect(effect(next, action)).await.unwrap();
                next += 1;
            }
            if index == 0 {
                assert_eq!(runtime.snapshot().await.verified_replication_lsn, Some(7));
                runtime
                    .data_plane()
                    .accept_acknowledgement(acknowledgement(&authority, identities[2].clone(), 7))
                    .await
                    .unwrap();
                timeout(
                    Duration::from_secs(1),
                    runtime.apply_effect(effect(next, RuntimeEffectAction::WaitForCatchup)),
                )
                .await
                .unwrap()
                .unwrap();
                next += 1;
            }
            runtime
                .apply_effect(effect(
                    next,
                    RuntimeEffectAction::AdmitAuthority(Box::new(AdmittedAuthority {
                        previous_configuration: None,
                        transition_kind: None,
                        ..authority
                    })),
                ))
                .await
                .unwrap();
            sequences[index] = next + 1;
            for participant in &runtimes {
                assert_ne!(
                    participant.snapshot().await.write_status,
                    AccessStatus::Granted
                );
            }
            for client in &retained_clients {
                assert!(
                    client
                        .begin_write(ClientWrite {
                            operation_id: OperationId::new("direct-during-compensation"),
                            data: Bytes::from_static(b"must-not-commit"),
                        })
                        .await
                        .is_err()
                );
            }
        }
        runtimes[0]
            .apply_effect(effect(
                sequences[0],
                RuntimeEffectAction::SetAccessStatus {
                    read: AccessStatus::Granted,
                    write: AccessStatus::Granted,
                },
            ))
            .await
            .unwrap();
        assert_eq!(runtimes[0].snapshot().await.role, ReplicaRole::Primary);
        assert_eq!(
            runtimes[0].snapshot().await.write_status,
            AccessStatus::Granted
        );
        assert!(
            retained_clients[1]
                .begin_write(ClientWrite {
                    operation_id: OperationId::new("compensated-target-direct-client"),
                    data: Bytes::from_static(b"must-not-commit"),
                })
                .await
                .is_err()
        );
        for lsn in 1..=handoff.handoff_lsn {
            assert_eq!(
                applications[0].applied.lock().unwrap()[&lsn].data,
                Bytes::from(format!("seed-{lsn}"))
            );
        }
        assert!(applications.iter().all(|application| {
            !application
                .events
                .lock()
                .unwrap()
                .iter()
                .any(|event| event == "provider.on_data_loss")
        }));
        return;
    }
    runtimes[1]
        .apply_effect(effect(
            sequences[1] + 1,
            RuntimeEffectAction::SetAccessStatus {
                read: AccessStatus::Granted,
                write: AccessStatus::Granted,
            },
        ))
        .await
        .unwrap();
    assert_eq!(
        runtimes[1].snapshot().await.write_status,
        AccessStatus::Granted
    );
    assert_eq!(
        runtimes[0].snapshot().await.role,
        ReplicaRole::ActiveSecondary
    );
    assert!(
        retained_clients[0]
            .begin_write(ClientWrite {
                operation_id: OperationId::new("retained-source-client"),
                data: Bytes::from_static(b"stale-client"),
            })
            .await
            .is_err()
    );
    assert_eq!(runtimes[1].snapshot().await.role, ReplicaRole::Primary);
    assert_eq!(
        runtimes[2].snapshot().await.role,
        ReplicaRole::ActiveSecondary
    );
    for lsn in 1..=handoff.handoff_lsn {
        assert_eq!(
            applications[1].applied.lock().unwrap()[&lsn].data,
            applications[0].applied.lock().unwrap()[&lsn].data
        );
    }
    assert!(applications.iter().all(|application| {
        !application
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|event| event == "provider.on_data_loss")
    }));
}

#[tokio::test]
async fn switchover_certificate_transfers_only_the_verified_prefix_across_restart_and_compensation()
{
    let source = identity(1, "source");
    let target = identity(2, "target");
    let third = identity(3, "third");
    let starting = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        source.replica_id,
        vec![
            ConfigurationMember {
                identity: source.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: target.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: third.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    );
    let requested = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        target.replica_id,
        vec![
            ConfigurationMember {
                identity: source.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: target.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: third.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    );
    let handoff = SwitchoverHandoff {
        preparation_generation: 1,
        preparation_operation_id: OperationId::new("prepare-1"),
        request_id: SwitchoverRequestId::new("request-1"),
        source: source.clone(),
        target: target.clone(),
        starting_configuration_id: starting.configuration_id.clone(),
        starting_epoch: starting.epoch,
        handoff_lsn: 7,
    };
    let target_store = Arc::new(MemoryAuthorityStore::default());
    let starting_fence = AuthorityFence {
        epoch: starting.epoch,
        previous_configuration_id: None,
        current_configuration_id: starting.configuration_id.clone(),
    };
    target_store.replication_progress.lock().unwrap().insert(
        starting_fence.clone(),
        ReplicationProgress {
            fence: starting_fence,
            verified_lsn: 9,
        },
    );
    let target_application = Arc::new(TestApplication::default());
    target_application.seed_progress(9);
    let requested_authority = AdmittedAuthority {
        scale_up: None,
        secondary_removal: None,
        local_identity: target.clone(),
        transition_kind: Some(TransitionKind::PlannedSwitchover),
        previous_configuration: Some(starting.clone()),
        current_configuration: requested.clone(),
        switchover_handoff: Some(handoff.clone()),
    };
    let uncertified_application = Arc::new(TestApplication::default());
    uncertified_application.seed_progress(9);
    let uncertified_runtime = PodRuntime::new(
        target.clone(),
        uncertified_application,
        Arc::new(MemoryAuthorityStore::default()),
    );
    uncertified_runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    assert!(matches!(
        uncertified_runtime
            .apply_effect(effect(
                2,
                RuntimeEffectAction::AdmitAuthority(Box::new(requested_authority.clone())),
            ))
            .await,
        Err(RuntimeError::AuthorityMismatch(_))
    ));
    let target_runtime = PodRuntime::new(
        target.clone(),
        target_application.clone(),
        target_store.clone(),
    );
    target_runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    target_runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(requested_authority)),
        ))
        .await
        .unwrap();
    assert_eq!(
        target_runtime.snapshot().await.verified_replication_lsn,
        Some(7)
    );

    let restarted_target =
        PodRuntime::new(target.clone(), target_application, target_store.clone());
    restarted_target
        .reconstruct(
            OpenMode::Existing,
            ReplicaRole::Primary,
            AccessStatus::ReconfigurationPending,
            AccessStatus::ReconfigurationPending,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        restarted_target.snapshot().await.verified_replication_lsn,
        Some(7)
    );
    restarted_target
        .apply_effect(effect(
            1,
            RuntimeEffectAction::AdmitAuthority(Box::new(AdmittedAuthority {
                scale_up: None,
                secondary_removal: None,
                local_identity: target.clone(),
                transition_kind: None,
                previous_configuration: None,
                current_configuration: requested.clone(),
                switchover_handoff: Some(handoff.clone()),
            })),
        ))
        .await
        .unwrap();
    let current_only_target = restarted_target.snapshot().await;
    assert!(
        current_only_target
            .authority
            .as_ref()
            .unwrap()
            .previous_configuration
            .is_none()
    );
    assert_eq!(current_only_target.verified_replication_lsn, Some(7));
    assert_eq!(
        current_only_target.read_status,
        AccessStatus::ReconfigurationPending
    );

    let compensation = ConfigurationDescriptor::new(
        Epoch::new(0, 3),
        source.replica_id,
        vec![
            ConfigurationMember {
                identity: source.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: target.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: third,
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    );
    let source_store = Arc::new(MemoryAuthorityStore::default());
    let source_application = Arc::new(TestApplication::default());
    source_application.seed_progress(9);
    let source_runtime = PodRuntime::new(
        source.clone(),
        source_application.clone(),
        source_store.clone(),
    );
    source_runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    source_runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(AdmittedAuthority {
                scale_up: None,
                secondary_removal: None,
                local_identity: source.clone(),
                transition_kind: Some(TransitionKind::PlannedSwitchover),
                previous_configuration: Some(requested),
                current_configuration: compensation,
                switchover_handoff: Some(handoff),
            })),
        ))
        .await
        .unwrap();
    assert_eq!(
        source_runtime.snapshot().await.verified_replication_lsn,
        Some(7)
    );
    let restarted_source = PodRuntime::new(source, source_application, source_store);
    restarted_source
        .reconstruct(
            OpenMode::Existing,
            ReplicaRole::Primary,
            AccessStatus::ReconfigurationPending,
            AccessStatus::ReconfigurationPending,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        restarted_source.snapshot().await.verified_replication_lsn,
        Some(7)
    );
}

#[tokio::test]
async fn runtime_exposes_derived_must_catch_up_evidence() {
    let old_primary = identity(1, "old-primary");
    let new_primary = identity(2, "new-primary");
    let third = identity(3, "third");
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 5),
        ReplicaId::new(1),
        vec![
            ConfigurationMember {
                identity: old_primary.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: new_primary.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: third.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 6),
        ReplicaId::new(2),
        vec![
            ConfigurationMember {
                identity: old_primary.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: new_primary.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: third.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    );
    let admitted = AdmittedAuthority {
        scale_up: None,
        secondary_removal: None,
        local_identity: new_primary.clone(),
        transition_kind: Some(TransitionKind::Failover),
        previous_configuration: Some(previous),
        current_configuration: current,
        switchover_handoff: None,
    };
    let application = Arc::new(TestApplication::default());
    application.seed_progress(5);
    let runtime = PodRuntime::new(
        new_primary,
        application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
    );
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(admitted.clone())),
        ))
        .await
        .unwrap();
    runtime
        .data_plane()
        .accept_acknowledgement(acknowledgement(&admitted, old_primary, 10))
        .await
        .unwrap();
    runtime
        .data_plane()
        .accept_acknowledgement(acknowledgement(&admitted, third, 10))
        .await
        .unwrap();
    let waiting = runtime.snapshot().await;
    assert_eq!(waiting.current_configuration_quorum_progress, 10);
    assert!(!waiting.catch_up_complete);

    application.seed_progress(10);
    runtime
        .apply_effect(effect(3, RuntimeEffectAction::RefreshApplicationProgress))
        .await
        .unwrap();
    assert!(runtime.snapshot().await.catch_up_complete);
}
