use super::*;
use crate::control::proto;
use crate::protocol::command::{AcceptSecondaryRemovalCommit, ProtocolCommand};
use crate::protocol::observation::{AgentObservation, ReplicaObservationKey};
use crate::protocol::types::{AcceptedTopology, SecondaryRemovalReceipt};
use crate::test_controller::Plan;
use tokio::sync::watch;
use tonic::{Code, Request};

fn crash_child(root: &Path, boundary: &str) {
    let output = Command::new(env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "host::tests::crash_boundaries::removal_crashes::quorum_crashes::history::pending_acceptance_writer",
            "--nocapture",
        ])
        .env("KUBERIC_HISTORY_PATH", root)
        .env("KUBERIC_HISTORY_BOUNDARY", boundary)
        .env("RUST_MIN_STACK", "16777216")
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(73),
        "{boundary}: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
#[ignore]
async fn pending_acceptance_writer() {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let root = PathBuf::from(env::var("KUBERIC_HISTORY_PATH").unwrap());
        let boundary = env::var("KUBERIC_HISTORY_BOUNDARY").unwrap();
        if boundary == "ordinary" {
            prepare_history(&root, true).await;
            unreachable!();
        }
        let command: AcceptSecondaryRemovalCommit =
            serde_json::from_slice(&std::fs::read(root.join("recovery-command")).unwrap()).unwrap();
        let intent = &command.committed.evidence.preparation.intent;
        let late = member(&root, intent, 4, false).await;
        let service = crate::host::service::AgentService::new(
            Arc::new(CrashStore {
                inner: late.store.clone(),
                boundary: boundary.clone(),
            }),
            late.runtime.clone(),
            Arc::new(CrashRuntime {
                runtime: late.runtime.clone(),
                boundary,
            }),
            "token",
        )
        .unwrap();
        let session = service.sessions().local_session().to_string();
        std::fs::write(root.join("conversion-session"), &session).unwrap();
        let (mut client, _shutdown, _server) = serve(service).await;
        client
            .execute(request(
                intent,
                &command.target,
                &session,
                wire_command(ProtocolCommand::AcceptSecondaryRemovalCommit(Box::new(
                    command.clone(),
                ))),
            ))
            .await
            .unwrap();
        panic!("historical acceptance crash was not reached");
    })
    .await
    .unwrap();
}

async fn serve<S, E>(
    service: crate::host::service::AgentService<S, E>,
) -> (
    proto::agent_control_client::AgentControlClient<tonic::transport::Channel>,
    watch::Sender<bool>,
    tokio::task::JoinHandle<Result<()>>,
)
where
    S: AgentStore + 'static,
    E: RuntimeEffectExecutor + 'static,
{
    let address = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let replication = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let (ready, mut ready_rx) = watch::channel(false);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let server = tokio::spawn(service.serve(address, replication, ready, shutdown_rx));
    tokio::time::timeout(std::time::Duration::from_secs(5), ready_rx.wait_for(|r| *r))
        .await
        .unwrap()
        .unwrap();
    let client =
        proto::agent_control_client::AgentControlClient::connect(format!("http://{address}"))
            .await
            .unwrap();
    (client, shutdown, server)
}

#[test]
fn historical_local_acceptance_survives_sqlite_service_restart_and_newer_authority() {
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    tokio::time::timeout(
                        std::time::Duration::from_secs(90),
                        recover_history(false),
                    )
                    .await
                    .unwrap();
                });
        })
        .unwrap()
        .join()
        .unwrap();
}

fn request(
    intent: &SecondaryScaleDownIntent,
    local: &ReplicaIdentity,
    session: &str,
    command: proto::execute_command_request::Command,
) -> Request<proto::ExecuteCommandRequest> {
    let mut request = Request::new(proto::ExecuteCommandRequest {
        protocol_version: crate::protocol::PROTOCOL_VERSION,
        resource_uid: intent.resource_uid.to_string(),
        target: Some(local.clone().into()),
        expected_process_session_id: session.into(),
        command: Some(command),
    });
    request
        .metadata_mut()
        .insert("authorization", "Bearer token".parse().unwrap());
    request
}

fn wire_command(command: ProtocolCommand) -> proto::execute_command_request::Command {
    match command {
        ProtocolCommand::AcceptSecondaryRemovalCommit(c) => {
            proto::execute_command_request::Command::AcceptSecondaryRemovalCommit(Box::new(
                (*c).into(),
            ))
        }
        ProtocolCommand::EnsureConfiguration(c) => {
            assert_eq!(c.transition_kind, TransitionKind::Failover);
            proto::execute_command_request::Command::EnsureConfiguration(Box::new(
                proto::EnsureConfigurationCommand {
                    operation_id: c.operation_id.to_string(),
                    local_replica_id: c.local_replica_id.value(),
                    expected_instance_id: c.expected_instance_id.to_string(),
                    expected_agent_generation: c.expected_agent_generation.to_string(),
                    previous_configuration: c.previous_configuration.map(Into::into),
                    current_configuration: Some(c.current_configuration.into()),
                    previous_epoch: c.previous_epoch.map(Into::into),
                    current_epoch: Some(c.current_epoch.into()),
                    effective_policy: Some(c.effective_policy.into()),
                    transition_kind: proto::TransitionKind::Failover as i32,
                    primary_write_status: proto::AccessStatus::ReconfigurationPending as i32,
                    current_only: c.current_only,
                    failover_safe_lsn: c.failover_safe_lsn,
                    ..Default::default()
                },
            ))
        }
        other => panic!("unexpected recovery {other:?}"),
    }
}

#[test]
fn pending_ordinary_acceptance_recovers_after_cleanup_and_newer_authority() {
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    tokio::time::timeout(std::time::Duration::from_secs(90), recover_history(true))
                        .await
                        .unwrap();
                });
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn prepare_history(
    root: &Path,
    pending_ordinary: bool,
) -> (SecondaryScaleDownCleanup, String) {
    let mut intent = fixture::intent(&[1, 2, 3, 4, 5, 6], 1);
    intent.desired_replicas = 5;
    intent.operation_id = intent.expected_operation_id();
    let mut members = Vec::new();
    for index in 0..5 {
        members.push(member(root, &intent, index, true).await);
    }
    let primary = &members[0];
    for peer in &members[1..] {
        register(primary, peer).await;
    }
    let pending = primary
        .runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("before-six-to-five"),
            data: Bytes::from_static(b"historical-prefix"),
        })
        .await
        .unwrap();
    for peer in &members[1..] {
        let identity = peer.runtime.snapshot().await.identity;
        let item = pending
            .replication_items
            .iter()
            .find(|item| item.receiver.as_ref() == Some(&identity.clone().into()))
            .unwrap()
            .clone();
        let mut ack = peer
            .runtime
            .data_plane()
            .receive_replication(item)
            .await
            .unwrap()
            .applied()
            .await
            .unwrap();
        ack.receiver_session_id = peer.session.to_string();
        primary
            .runtime
            .data_plane()
            .accept_acknowledgement(ack)
            .await
            .unwrap();
    }
    assert_eq!(pending.committed().await.unwrap().lsn, 1);
    let preparation = Coordinator::new(primary.store.clone(), primary.runtime.clone())
        .ensure_secondary_removal_prepared(
            fixture::prepare_command(&intent),
            primary.session.clone(),
            1,
        )
        .await
        .unwrap();
    assert_eq!(preparation.boundary_lsn, 1);
    let mut evidence = SecondaryRemovalEvidence {
        preparation,
        previous_read_quorum: Vec::new(),
        reduced_write_quorum: Vec::new(),
    };
    for peer in &members[..3] {
        evidence
            .previous_read_quorum
            .push(witness(peer, &intent, 2).await);
    }
    for peer in members[1..].iter().chain(&members[..1]) {
        configure(peer, &evidence, false).await;
    }
    for peer in &members[..3] {
        evidence
            .reduced_write_quorum
            .push(witness(peer, &intent, 3).await);
    }
    for peer in members[1..].iter().chain(&members[..1]) {
        configure(peer, &evidence, true).await;
    }
    let mut committed = SecondaryScaleDownCleanup {
        evidence,
        current_only_write_quorum: Vec::new(),
        retirement: None,
    };
    for peer in &members[..3] {
        committed
            .current_only_write_quorum
            .push(witness(peer, &intent, 4).await);
    }
    for peer in &members[..4] {
        for source in &members[..3] {
            if peer.session != source.session {
                register(peer, source).await;
            }
        }
        let local = peer.runtime.snapshot().await.identity;
        Coordinator::new(peer.store.clone(), peer.runtime.clone())
            .accept_secondary_removal_commit(AcceptSecondaryRemovalCommit {
                operation_id: intent
                    .command_operation_id(SecondaryRemovalStage::AcceptCommit, &local),
                target: local,
                committed: committed.clone(),
                local_recovery: false,
            })
            .await
            .unwrap();
    }
    let old_session = members[4].session.to_string();
    assert!(
        members[4]
            .store
            .load_state()
            .await
            .unwrap()
            .accepted_secondary_removal
            .is_none()
    );
    if pending_ordinary {
        let late = &members[4];
        let local = late.runtime.snapshot().await.identity;
        std::fs::write(root.join("ordinary-session"), &old_session).unwrap();
        Coordinator::new(
            Arc::new(CrashStore {
                inner: late.store.clone(),
                boundary: "intent".into(),
            }),
            late.runtime.clone(),
        )
        .accept_secondary_removal_commit(AcceptSecondaryRemovalCommit {
            operation_id: intent.command_operation_id(SecondaryRemovalStage::AcceptCommit, &local),
            target: local,
            committed: committed.clone(),
            local_recovery: false,
        })
        .await
        .unwrap();
        panic!("ordinary intent crash was not reached");
    }
    for peer in &members {
        peer.runtime.abort();
    }
    drop(members);
    (committed, old_session)
}

async fn recover_history(pending_ordinary: bool) {
    for (replacement, cut) in [false, true].into_iter().flat_map(|replacement| {
        (0..if pending_ordinary { 4 } else { 3 }).map(move |cut| (replacement, cut))
    }) {
        let directory = tempdir().unwrap();
        let (committed, mut old_session) = if pending_ordinary {
            crash_child(directory.path(), "ordinary");
            let store = SqliteStore::open_existing(
                SqliteStore::metadata_database_path(&directory.path().join("replica-5")),
                None,
            )
            .unwrap();
            let pending = store.load_state().await.unwrap().pending_effect.unwrap();
            assert_eq!(
                pending.stage,
                crate::host::state::EffectStage::IntentCommitted
            );
            let RuntimeEffectAction::AcceptSecondaryRemovalCommit(committed) =
                pending.effect.action
            else {
                panic!("ordinary intent was not persisted")
            };
            (
                *committed,
                std::fs::read_to_string(directory.path().join("ordinary-session")).unwrap(),
            )
        } else {
            prepare_history(directory.path(), false).await
        };
        let intent = committed.evidence.preparation.intent.clone();
        // Cleanup has completed and a quorum has accepted newer authority while 5 is absent.
        let receipt = SecondaryRemovalReceipt {
            evidence: committed.evidence.clone(),
            current_only_write_quorum: committed.current_only_write_quorum.clone(),
        };
        let mut cluster = model::Model::new(&[1, 2, 3, 4, 5, 6], 1, 5);
        let mut newer = intent.current_configuration.members.clone();
        if replacement {
            newer[3].identity.instance_id = ReplicaInstanceId::new("replacement-4");
            newer[3].identity.agent_generation = AgentGeneration::new("replacement-generation");
        } else {
            newer[0].role = ReplicaRole::ActiveSecondary;
            newer[1].role = ReplicaRole::Primary;
        }
        let accepted = ConfigurationDescriptor::new(
            Epoch::new(2, 12),
            ReplicaId::new(if replacement { 1 } else { 2 }),
            newer,
            intent.current_policy.write_quorum,
        );
        let removed = cluster.key(6);
        cluster.snapshot.replicas.remove(&removed);
        for member in &accepted.members {
            let key = cluster.key(member.identity.replica_id.value());
            let mut observation = cluster.snapshot.replicas.remove(&key).unwrap();
            let AgentObservation::Report(report) = &mut observation.agent else {
                unreachable!()
            };
            report.identity = member.identity.clone();
            report.epoch = accepted.epoch;
            report.current_configuration = Some(accepted.clone());
            report.role = member.role;
            report.write_status = if member.role == ReplicaRole::Primary {
                AccessStatus::Granted
            } else {
                AccessStatus::NotPrimary
            };
            observation.kubernetes.as_mut().unwrap().pod_uid =
                Some(PodUid::new(member.identity.instance_id.as_str()));
            cluster.snapshot.replicas.insert(
                ReplicaObservationKey::new(
                    member.identity.replica_id,
                    member.identity.instance_id.clone(),
                ),
                observation,
            );
        }
        cluster.snapshot.status.topology = Some(AcceptedTopology {
            configuration: accepted.clone(),
        });
        cluster.snapshot.status.effective_policy = Some(intent.current_policy.clone());
        cluster.snapshot.status.last_secondary_removal = Some(receipt.clone());
        cluster.snapshot.routing.write_target = Some(
            accepted
                .members
                .iter()
                .find(|m| m.role == ReplicaRole::Primary)
                .unwrap()
                .identity
                .clone(),
        );
        let local = intent.current_configuration.members[4].identity.clone();
        let mut recovery = None;
        for restart in 0..3 {
            let late = member(directory.path(), &intent, 4, false).await;
            assert_ne!(late.session.as_str(), old_session);
            assert_eq!(
                late.application.state.lock().unwrap().operations[&1],
                b"historical-prefix"
            );
            let (mut client, shutdown, server) = serve(late.service.clone()).await;
            let mut status = Request::new(proto::GetAgentStatusRequest {
                protocol_version: crate::protocol::PROTOCOL_VERSION,
                resource_uid: intent.resource_uid.to_string(),
                replica_id: local.replica_id.value(),
                expected_instance_id: local.instance_id.to_string(),
            });
            status
                .metadata_mut()
                .insert("authorization", "Bearer token".parse().unwrap());
            let report = client.get_status(status).await.unwrap().into_inner();
            let AgentObservation::Report(report) =
                crate::control::normalize_agent_status_report(report).unwrap()
            else {
                panic!("initialized")
            };
            *cluster.report(5) = *report;
            if restart == 0 {
                let Plan::Execute {
                    command: ProtocolCommand::AcceptSecondaryRemovalCommit(command),
                } = cluster.plan()
                else {
                    panic!("expected exact local recovery: {:?}", cluster.plan());
                };
                assert!(command.local_recovery);
                assert_eq!(command.committed, committed);
                recovery = Some(*command);
            }
            let command = recovery.clone().unwrap();
            let before = late.store.load_state().await.unwrap();
            let envelope = wire_command(ProtocolCommand::AcceptSecondaryRemovalCommit(Box::new(
                command.clone(),
            )));
            assert_eq!(
                client
                    .execute(request(&intent, &local, &old_session, envelope.clone()))
                    .await
                    .unwrap_err()
                    .code(),
                Code::FailedPrecondition
            );
            assert_eq!(late.store.load_state().await.unwrap(), before);
            if restart == 0 {
                for mutation in 0..7 {
                    let mut wrong = command.clone();
                    match mutation {
                        0 => wrong.operation_id = OperationId::new("different-operation"),
                        1 => wrong.committed.evidence.preparation.boundary_lsn = 2,
                        2 => wrong.committed.current_only_write_quorum.clear(),
                        3 => {
                            wrong.target.agent_generation = AgentGeneration::new("wrong-generation")
                        }
                        4 => wrong.target = intent.target.clone(),
                        5 => wrong.target = intent.primary.clone(),
                        _ => {
                            wrong.committed.evidence.preparation.intent.resource_uid =
                                ResourceUid::new("other-resource")
                        }
                    }
                    assert!(
                        late.store
                            .begin_effect(&RuntimeEffect {
                                operation_id: wrong.operation_id.clone(),
                                sequence: before.next_effect_sequence,
                                action: RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(
                                    Box::new(wrong.clone())
                                ),
                            })
                            .await
                            .is_err()
                    );
                    assert!(
                        client
                            .execute(request(
                                &intent,
                                &local,
                                late.session.as_str(),
                                wire_command(ProtocolCommand::AcceptSecondaryRemovalCommit(
                                    Box::new(wrong)
                                ))
                            ))
                            .await
                            .is_err()
                    );
                    assert_eq!(late.store.load_state().await.unwrap(), before);
                }
                // The normal correction really is fenced before local acceptance.
                let mut assumed = cluster.clone();
                assumed.report(5).accepted_secondary_removal = Some(committed.clone());
                let Plan::Execute {
                    command: correction,
                } = assumed.plan()
                else {
                    panic!("correction")
                };
                assert_eq!(
                    client
                        .execute(request(
                            &intent,
                            &local,
                            late.session.as_str(),
                            wire_command(correction)
                        ))
                        .await
                        .unwrap_err()
                        .code(),
                    Code::FailedPrecondition
                );
                assert_eq!(late.store.load_state().await.unwrap(), before);
            }
            if restart < 2 {
                if restart == 0 && cut > 0 {
                    let effect = RuntimeEffect {
                        operation_id: command.operation_id.clone(),
                        sequence: before.next_effect_sequence,
                        action: RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(
                            Box::new(command.clone()),
                        ),
                    };
                    if pending_ordinary {
                        shutdown.send_replace(true);
                        server.await.unwrap().unwrap();
                        std::fs::write(
                            directory.path().join("recovery-command"),
                            serde_json::to_vec(&command).unwrap(),
                        )
                        .unwrap();
                        crash_child(
                            directory.path(),
                            match cut {
                                1 => "historical-intent",
                                2 => "historical-runtime",
                                3 => "historical-applied",
                                _ => unreachable!(),
                            },
                        );
                        let converted = late.store.load_state().await.unwrap();
                        let mut expected = before.clone();
                        let pending = expected.pending_effect.as_mut().unwrap();
                        pending.effect = effect;
                        if cut == 3 {
                            pending.stage = crate::host::state::EffectStage::EffectApplied;
                        }
                        assert_eq!(converted, expected, "conversion changes only mode/stage");
                        assert!(
                            late.store
                                .load_secondary_removal_commit()
                                .await
                                .unwrap()
                                .is_none()
                        );
                        old_session =
                            std::fs::read_to_string(directory.path().join("conversion-session"))
                                .unwrap();
                        assert_ne!(old_session, late.session.as_str());
                        continue;
                    }
                    late.store.begin_effect(&effect).await.unwrap();
                    if cut == 2 {
                        late.runtime.apply_effect(effect.clone()).await.unwrap();
                        late.store.mark_effect_applied(&effect).await.unwrap();
                    }
                    assert!(
                        late.store
                            .load_state()
                            .await
                            .unwrap()
                            .accepted_secondary_removal
                            .is_none()
                    );
                    old_session = late.session.to_string();
                    shutdown.send_replace(true);
                    server.await.unwrap().unwrap();
                    continue;
                }
                let before_runtime = late.runtime.snapshot().await;
                let report = client
                    .execute(request(&intent, &local, late.session.as_str(), envelope))
                    .await
                    .unwrap()
                    .into_inner()
                    .observation
                    .unwrap();
                let AgentObservation::Report(report) =
                    crate::control::normalize_agent_status_report(report).unwrap()
                else {
                    panic!("initialized")
                };
                *cluster.report(5) = *report;
                let after_runtime = late.runtime.snapshot().await;
                assert_eq!(after_runtime.authority, before_runtime.authority);
                assert_eq!(after_runtime.write_status, before_runtime.write_status);
                assert!(
                    !after_runtime.catch_up_complete,
                    "no historical quorum/session credit"
                );
                assert_eq!(
                    after_runtime.current_configuration_quorum_progress,
                    before_runtime.current_configuration_quorum_progress
                );
                assert_eq!(
                    late.store.load_secondary_removal_commit().await.unwrap(),
                    None
                );
                assert_eq!(
                    late.store
                        .load_state()
                        .await
                        .unwrap()
                        .accepted_secondary_removal,
                    Some(committed.clone())
                );
                let mut wrong = command.clone();
                wrong.committed.current_only_write_quorum[0].report_sequence += 1;
                assert!(
                    client
                        .execute(request(
                            &intent,
                            &local,
                            late.session.as_str(),
                            wire_command(ProtocolCommand::AcceptSecondaryRemovalCommit(Box::new(
                                wrong
                            )))
                        ))
                        .await
                        .is_err()
                );
                for current_only in [false, true] {
                    if restart == 0 {
                        break;
                    }
                    let Plan::Execute {
                        command: correction,
                    } = cluster.plan()
                    else {
                        panic!("correction: {:?}", cluster.plan())
                    };
                    assert!(
                        matches!(&correction, ProtocolCommand::EnsureConfiguration(c) if c.current_only == current_only && c.current_configuration == accepted)
                    );
                    let wire = wire_command(correction);
                    assert_eq!(
                        client
                            .execute(request(&intent, &local, &old_session, wire.clone()))
                            .await
                            .unwrap_err()
                            .code(),
                        Code::FailedPrecondition
                    );
                    let report = client
                        .execute(request(&intent, &local, late.session.as_str(), wire))
                        .await
                        .unwrap()
                        .into_inner()
                        .observation
                        .unwrap();
                    let AgentObservation::Report(report) =
                        crate::control::normalize_agent_status_report(report).unwrap()
                    else {
                        panic!("initialized")
                    };
                    *cluster.report(5) = *report;
                }
            } else {
                assert!(
                    late.store
                        .begin_effect(&RuntimeEffect {
                            operation_id: command.operation_id.clone(),
                            sequence: before.next_effect_sequence,
                            action: RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(
                                Box::new(command.clone())
                            ),
                        })
                        .await
                        .is_err(),
                    "store cannot regress newer authority"
                );
                assert!(
                    client
                        .execute(request(&intent, &local, late.session.as_str(), envelope))
                        .await
                        .is_err(),
                    "historical replay cannot regress newer authority"
                );
                assert_eq!(
                    cluster.report(5).current_configuration,
                    Some(accepted.clone())
                );
                cluster.finish();
                assert!(matches!(cluster.plan(), Plan::Stable { .. }));
            }
            assert_eq!(
                cluster.snapshot.status.last_secondary_removal,
                Some(receipt.clone())
            );
            old_session = late.session.to_string();
            shutdown.send_replace(true);
            server.await.unwrap().unwrap();
        }
    }
}
