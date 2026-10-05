use kuberic_agent::service::SessionRegistry;
use kuberic_agent::transport::{
    ReliableTransport, ReliableWindow, ResumeWindow, RoleTransportState,
};
use kuberic_protocol::types::{
    AgentGeneration, ConfigurationId, Epoch, ProcessSessionId, ReplicaId, ReplicaIdentity,
    ReplicaInstanceId, ReplicaRole,
};
use kuberic_runtime::replicator::sender::SenderOutbound;
use kuberic_runtime_internal::transport::OutboundOperation;
use kuberic_runtime_internal::transport::ReplicationItem;

#[test]
fn hosted_build_dispatch_uses_one_shared_public_execution_coordinator() {
    let transport = include_str!("../src/transport.rs");
    assert_eq!(
        transport.matches(".execute_admitted_build(").count(),
        1,
        "production transport must have one admitted-build coordinator call"
    );
    assert!(
        !transport.contains(".build_replica("),
        "transport must not invoke the public primary through a parallel path"
    );
    assert!(
        transport.contains("let (decision, completion) = tokio::sync::oneshot::channel()")
            && transport.contains("if completion.await != Ok(true)")
            && !transport.contains("impl Drop for BuildDispatchCancellation"),
        "dispatch cleanup must be an explicit transaction, not asynchronous Drop work"
    );
    assert_eq!(
        transport
            .matches("QueuedOutbound::Build(endpoint) => self.dispatch_build(endpoint).await")
            .count(),
        1,
        "one common Build control branch must own hosted dispatch"
    );
    assert_eq!(
        transport
            .matches(
                "QueuedOutbound::Remove(replica_id) => {\n                let receiver = self.transport",
            )
            .count(),
        1,
        "one common Remove control branch must own hosted dispatch"
    );
}

fn identity(id: i64, instance: &str) -> ReplicaIdentity {
    ReplicaIdentity {
        replica_id: ReplicaId::new(id),
        instance_id: ReplicaInstanceId::new(instance),
        agent_generation: AgentGeneration::new(format!("generation-{instance}")),
    }
}

fn item(lsn: i64) -> ReplicationItem {
    ReplicationItem {
        sender: identity(1, "primary"),
        receiver: identity(2, "secondary"),
        epoch: Epoch::new(0, 1),
        previous_configuration_id: None,
        current_configuration_id: ConfigurationId::new("configuration"),
        lsn,
        committed_lsn: lsn - 1,
        data: format!("value-{lsn}").into(),
    }
}

#[tokio::test]
async fn reduction_evicts_exact_streams_and_delayed_discovery_cannot_restore_sessions() {
    let removed = identity(2, "secondary");
    let replacement = identity(2, "new-incarnation");
    let registry = SessionRegistry::new(ProcessSessionId::new("primary-session"));
    registry
        .register_peer(removed.clone(), ProcessSessionId::new("old-session"))
        .await;
    let mut transport =
        ReliableTransport::new(ProcessSessionId::new("primary-session"), 4).unwrap();
    transport
        .admit_peer(removed.clone(), ProcessSessionId::new("old-session"))
        .unwrap();
    transport
        .queue(OutboundOperation::Replication(item(1)))
        .unwrap();
    transport
        .queue(OutboundOperation::Evict(removed.clone()))
        .unwrap();
    registry.retire_peer(&removed).await;
    for session in ["old-session", "restarted-target-session"] {
        assert!(
            transport
                .admit_peer(removed.clone(), ProcessSessionId::new(session))
                .is_err()
        );
        registry
            .register_peer(removed.clone(), ProcessSessionId::new(session))
            .await;
        assert!(
            registry
                .validate_peer(&removed, session, "primary-session")
                .await
                .is_err()
        );
    }
    assert!(
        transport
            .queue(OutboundOperation::Replication(item(2)))
            .is_err()
    );
    assert!(transport.acknowledge_replication(&removed, 1).is_err());
    transport
        .admit_peer(
            replacement.clone(),
            ProcessSessionId::new("replacement-session"),
        )
        .unwrap();
    registry
        .register_peer(
            replacement.clone(),
            ProcessSessionId::new("replacement-session"),
        )
        .await;
    assert!(
        registry
            .validate_peer(&replacement, "replacement-session", "primary-session")
            .await
            .is_ok()
    );
    transport
        .queue(OutboundOperation::Replication(ReplicationItem {
            receiver: replacement.clone(),
            ..item(3)
        }))
        .unwrap();
    for _ in 0..3 {
        transport
            .queue(OutboundOperation::Evict(removed.clone()))
            .unwrap();
        registry.retire_peer(&removed).await;
        assert!(
            registry
                .validate_peer(&replacement, "replacement-session", "primary-session")
                .await
                .is_ok()
        );
        let ResumeWindow::Retained(items) =
            transport.reconnect_replication(&replacement, 3).unwrap()
        else {
            panic!("late exact eviction must preserve the replacement stream")
        };
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].payload.receiver, replacement);
        assert_eq!(items[0].payload.data, item(3).data);
    }
}

#[test]
fn reliable_window_preserves_order_backpressure_reconnect_and_cancellation() {
    let mut window = ReliableWindow::new(2).unwrap();
    assert_eq!(window.enqueue(item(1)).unwrap().sequence, 1);
    assert_eq!(window.enqueue(item(2)).unwrap().sequence, 2);
    assert!(window.enqueue(item(3)).is_err());
    assert_eq!(window.catch_up_capability(), Some(1));
    assert!(matches!(
        window.reconnect_from_lsn(0),
        ResumeWindow::FullCopyRequired
    ));
    let ResumeWindow::Retained(retained) = window.reconnect_from_lsn(1) else {
        panic!("retained operations expected");
    };
    assert_eq!(
        retained
            .iter()
            .map(|message| message.payload.lsn)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    window.acknowledge_through(1).unwrap();
    assert_eq!(window.catch_up_capability(), Some(2));
    assert_eq!(window.enqueue(item(3)).unwrap().sequence, 3);
    window.cancel();
    assert!(window.retained().is_empty());
    assert!(window.enqueue(item(4)).is_err());
    assert!(matches!(
        window.reconnect_from_lsn(2),
        ResumeWindow::FullCopyRequired
    ));
    assert!(matches!(
        ReliableWindow::<ReplicationItem>::new(2)
            .unwrap()
            .reconnect_from_lsn(1),
        ResumeWindow::FullCopyRequired
    ));
}

#[test]
fn role_transition_cancels_primary_sessions_and_requires_secondary_source() {
    let mut state = RoleTransportState::Primary {
        sessions: [(identity(2, "secondary"), ReliableWindow::new(4).unwrap())]
            .into_iter()
            .collect(),
    };
    state
        .transition(ReplicaRole::ActiveSecondary, Some(identity(1, "primary")))
        .unwrap();
    assert!(matches!(state, RoleTransportState::Secondary { .. }));
    assert!(state.transition(ReplicaRole::IdleSecondary, None).is_err());
    state.transition(ReplicaRole::None, None).unwrap();
    assert!(matches!(state, RoleTransportState::None));
}

#[tokio::test]
async fn session_registry_rejects_retired_sender_and_receiver_sessions() {
    let sender = identity(1, "primary");
    let registry = SessionRegistry::new(ProcessSessionId::new("receiver-current"));
    registry
        .register_peer(sender.clone(), ProcessSessionId::new("sender-current"))
        .await;
    let lease = registry
        .validate_peer(&sender, "sender-current", "receiver-current")
        .await
        .unwrap();
    let registry = std::sync::Arc::new(registry);
    let replacement_registry = registry.clone();
    let replacement_sender = sender.clone();
    let replacement = tokio::spawn(async move {
        replacement_registry
            .register_peer(
                replacement_sender,
                ProcessSessionId::new("sender-replacement"),
            )
            .await;
    });
    tokio::task::yield_now().await;
    assert!(!replacement.is_finished());
    drop(lease);
    replacement.await.unwrap();
    assert!(
        registry
            .validate_peer(&sender, "sender-current", "receiver-current")
            .await
            .is_err()
    );
    drop(
        registry
            .validate_peer(&sender, "sender-replacement", "receiver-current")
            .await
            .unwrap(),
    );
    assert!(
        registry
            .validate_peer(&sender, "sender-replacement", "receiver-retired")
            .await
            .is_err()
    );
}

#[test]
fn reliable_transport_attaches_sessions_retires_acks_and_falls_back_to_copy() {
    let local_session = ProcessSessionId::new("primary-session");
    let receiver = identity(2, "secondary");
    let mut transport = ReliableTransport::new(local_session, 2).unwrap();
    transport
        .admit_peer(receiver.clone(), ProcessSessionId::new("secondary-session"))
        .unwrap();
    let SenderOutbound::Replication {
        sender_session,
        receiver_session,
        message,
        ..
    } = transport
        .queue(OutboundOperation::Replication(item(1)))
        .unwrap()
    else {
        panic!("replication dispatch expected");
    };
    assert_eq!(message.sequence, 1);
    assert_eq!(sender_session.as_str(), "primary-session");
    assert_eq!(receiver_session.as_str(), "secondary-session");
    transport
        .queue(OutboundOperation::Replication(item(2)))
        .unwrap();
    transport
        .admit_peer(
            receiver.clone(),
            ProcessSessionId::new("secondary-restarted"),
        )
        .unwrap();
    assert!(
        transport
            .queue(OutboundOperation::Replication(item(3)))
            .is_err()
    );
    transport.acknowledge_replication(&receiver, 1).unwrap();
    let SenderOutbound::Replication {
        receiver_session, ..
    } = transport
        .queue(OutboundOperation::Replication(item(3)))
        .unwrap()
    else {
        panic!("replication dispatch expected");
    };
    assert_eq!(receiver_session.as_str(), "secondary-restarted");
    assert!(matches!(
        transport.reconnect_replication(&receiver, 1).unwrap(),
        ResumeWindow::FullCopyRequired
    ));
    transport.retire_peer(&receiver);
    assert!(
        transport
            .queue(OutboundOperation::Replication(item(4)))
            .is_err()
    );
}

#[tokio::test]
async fn switchover_peer_session_matrix_preserves_exact_replay_across_role_reversal() {
    for (sender, receiver) in [
        (identity(1, "source"), identity(2, "target")),
        (identity(2, "target"), identity(1, "source")),
    ] {
        for epoch in [2, 3] {
            let receiver_registry = SessionRegistry::new(ProcessSessionId::new("receiver-new"));
            receiver_registry
                .register_peer(sender.clone(), ProcessSessionId::new("sender-old"))
                .await;
            receiver_registry
                .register_peer(sender.clone(), ProcessSessionId::new("sender-new"))
                .await;
            let mut sender_transport =
                ReliableTransport::new(ProcessSessionId::new("sender-new"), 2).unwrap();
            sender_transport
                .admit_peer(receiver.clone(), ProcessSessionId::new("receiver-new"))
                .unwrap();
            let payload = ReplicationItem {
                sender: sender.clone(),
                receiver: receiver.clone(),
                epoch: Epoch::new(0, epoch),
                previous_configuration_id: Some(ConfigurationId::new(format!(
                    "configuration-{}",
                    epoch - 1
                ))),
                current_configuration_id: ConfigurationId::new(format!("configuration-{epoch}")),
                ..item(10)
            };
            sender_transport
                .queue(OutboundOperation::Replication(payload.clone()))
                .unwrap();
            for sender_session in ["sender-old", "sender-new"] {
                for receiver_session in ["receiver-old", "receiver-new"] {
                    let lease = receiver_registry
                        .validate_peer(&sender, sender_session, receiver_session)
                        .await;
                    assert_eq!(
                        lease.is_ok(),
                        sender_session == "sender-new" && receiver_session == "receiver-new"
                    );
                    // Failed session validation cannot consume the retained operation.
                    let ResumeWindow::Retained(replayed) = sender_transport
                        .reconnect_replication(&receiver, 10)
                        .unwrap()
                    else {
                        panic!("handoff-boundary operation must remain replayable")
                    };
                    assert_eq!(replayed.len(), 1);
                    assert_eq!(replayed[0].payload, payload);
                }
            }
            for _ in 0..2 {
                drop(
                    receiver_registry
                        .validate_peer(&sender, "sender-new", "receiver-new")
                        .await
                        .unwrap(),
                );
                let ResumeWindow::Retained(replayed) = sender_transport
                    .reconnect_replication(&receiver, 10)
                    .unwrap()
                else {
                    panic!("current-session replay")
                };
                assert_eq!(replayed[0].payload, payload);
            }
            let impostor = identity(sender.replica_id.value(), "replacement-incarnation");
            assert!(
                receiver_registry
                    .validate_peer(&impostor, "sender-new", "receiver-new")
                    .await
                    .is_err()
            );
            sender_transport
                .acknowledge_replication(&receiver, 10)
                .unwrap();
            assert!(matches!(
                sender_transport
                    .reconnect_replication(&receiver, 10)
                    .unwrap(),
                ResumeWindow::FullCopyRequired
            ));
        }
    }
}
