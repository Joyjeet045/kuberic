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
