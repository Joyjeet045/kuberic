use kuberic_protocol::types::{
    AgentGeneration, ConfigurationDescriptor, ConfigurationMember, Epoch, ReplicaId,
    ReplicaIdentity, ReplicaInstanceId, ReplicaRole, TransitionKind,
};
use kuberic_runtime::internal::QuorumTracker;
use kuberic_runtime_internal::authority::AdmittedAuthority;
use kuberic_runtime_internal::transport::ReplicationAck;

fn identity(id: i64, instance: &str) -> ReplicaIdentity {
    ReplicaIdentity {
        replica_id: ReplicaId::new(id),
        instance_id: ReplicaInstanceId::new(instance),
        agent_generation: AgentGeneration::new(format!("generation-{instance}")),
    }
}

fn member(identity: ReplicaIdentity, role: ReplicaRole) -> ConfigurationMember {
    ConfigurationMember { identity, role }
}

fn acknowledgement(
    authority: &AdmittedAuthority,
    receiver: ReplicaIdentity,
    lsn: i64,
) -> ReplicationAck {
    ReplicationAck {
        sender: authority.primary_identity().clone(),
        receiver,
        epoch: authority.current_configuration.epoch,
        previous_configuration_id: authority
            .previous_configuration
            .as_ref()
            .map(|configuration| configuration.configuration_id.clone()),
        current_configuration_id: authority.current_configuration.configuration_id.clone(),
        received_lsn: lsn,
        applied_lsn: lsn,
        committed_lsn: 0,
    }
}

#[tokio::test]
async fn two_incarnations_of_one_replica_receive_distinct_quorum_credit() {
    let primary = identity(1, "primary");
    let secondary = identity(2, "secondary");
    let old = identity(3, "old");
    let replacement = identity(3, "replacement");
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        ReplicaId::new(1),
        vec![
            member(primary.clone(), ReplicaRole::Primary),
            member(secondary.clone(), ReplicaRole::ActiveSecondary),
            member(old.clone(), ReplicaRole::ActiveSecondary),
        ],
        2,
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        ReplicaId::new(1),
        vec![
            member(primary.clone(), ReplicaRole::Primary),
            member(secondary, ReplicaRole::ActiveSecondary),
            member(replacement.clone(), ReplicaRole::ActiveSecondary),
        ],
        2,
    );
    let authority = AdmittedAuthority {
        local_identity: primary,
        transition_kind: Some(TransitionKind::Replacement),
        previous_configuration: Some(previous),
        current_configuration: current,
    };
    let mut tracker = QuorumTracker::default();
    tracker.configure(authority.clone(), 0).unwrap();
    tracker.record_local_progress(1).unwrap();
    let mut completion = tracker.register_write(1).unwrap();

    tracker
        .acknowledge(&acknowledgement(&authority, old, 1))
        .unwrap();
    assert!(completion.try_recv().is_err());
    assert_eq!(tracker.committed_lsn(), 0);

    tracker
        .acknowledge(&acknowledgement(&authority, replacement, 1))
        .unwrap();
    assert_eq!(tracker.ready_commit_lsn(), Some(1));
    tracker.finalize_commit(1).unwrap();
    assert_eq!(completion.await.unwrap().unwrap(), 1);
    assert_eq!(tracker.committed_lsn(), 1);
}

#[tokio::test]
async fn stale_authority_ack_cannot_advance_progress_or_commit() {
    let primary = identity(1, "primary");
    let secondary = identity(2, "secondary");
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        ReplicaId::new(1),
        vec![
            member(primary.clone(), ReplicaRole::Primary),
            member(secondary.clone(), ReplicaRole::ActiveSecondary),
            member(identity(3, "third"), ReplicaRole::ActiveSecondary),
        ],
        2,
    );
    let authority = AdmittedAuthority {
        local_identity: primary,
        transition_kind: None,
        previous_configuration: None,
        current_configuration: current,
    };
    let mut tracker = QuorumTracker::default();
    tracker.configure(authority.clone(), 0).unwrap();
    tracker.record_local_progress(1).unwrap();
    let mut completion = tracker.register_write(1).unwrap();

    let mut stale = acknowledgement(&authority, secondary.clone(), 1);
    stale.epoch = Epoch::new(0, 1);
    assert!(tracker.acknowledge(&stale).is_err());
    assert_eq!(tracker.current_configuration_quorum_progress(), 0);
    assert_eq!(tracker.committed_lsn(), 0);
    assert!(completion.try_recv().is_err());

    let mut wrong_configuration = acknowledgement(&authority, secondary.clone(), 1);
    wrong_configuration.current_configuration_id =
        kuberic_protocol::types::ConfigurationId::new("stale");
    assert!(tracker.acknowledge(&wrong_configuration).is_err());
    assert_eq!(tracker.current_configuration_quorum_progress(), 0);

    tracker
        .acknowledge(&acknowledgement(&authority, secondary, 1))
        .unwrap();
    assert_eq!(tracker.ready_commit_lsn(), Some(1));
    tracker.finalize_commit(1).unwrap();
    assert_eq!(completion.await.unwrap().unwrap(), 1);
}

#[tokio::test]
async fn authority_change_fails_pending_writes_instead_of_rebinding_them() {
    let primary = identity(1, "primary");
    let secondary = identity(2, "secondary");
    let members = vec![
        member(primary.clone(), ReplicaRole::Primary),
        member(secondary, ReplicaRole::ActiveSecondary),
        member(identity(3, "third"), ReplicaRole::ActiveSecondary),
    ];
    let initial = AdmittedAuthority {
        local_identity: primary.clone(),
        transition_kind: None,
        previous_configuration: None,
        current_configuration: ConfigurationDescriptor::new(
            Epoch::new(0, 1),
            ReplicaId::new(1),
            members.clone(),
            2,
        ),
    };
    let advanced = AdmittedAuthority {
        local_identity: primary,
        transition_kind: None,
        previous_configuration: None,
        current_configuration: ConfigurationDescriptor::new(
            Epoch::new(0, 2),
            ReplicaId::new(1),
            members,
            2,
        ),
    };
    let mut tracker = QuorumTracker::default();
    tracker.configure(initial, 0).unwrap();
    tracker.record_local_progress(1).unwrap();
    let completion = tracker.register_write(1).unwrap();

    tracker.configure(advanced, 1).unwrap();
    assert!(completion.await.unwrap().is_err());
    assert_eq!(tracker.committed_lsn(), 0);
}

#[test]
fn authority_change_discards_remote_acknowledgement_credit() {
    let primary = identity(1, "primary");
    let secondary = identity(2, "secondary");
    let third = identity(3, "third");
    let initial = AdmittedAuthority {
        local_identity: primary.clone(),
        transition_kind: None,
        previous_configuration: None,
        current_configuration: ConfigurationDescriptor::new(
            Epoch::new(0, 1),
            ReplicaId::new(1),
            vec![
                member(primary.clone(), ReplicaRole::Primary),
                member(secondary.clone(), ReplicaRole::ActiveSecondary),
                member(third.clone(), ReplicaRole::ActiveSecondary),
            ],
            2,
        ),
    };
    let replacement = identity(3, "replacement");
    let previous = initial.current_configuration.clone();
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        ReplicaId::new(1),
        vec![
            member(primary.clone(), ReplicaRole::Primary),
            member(secondary.clone(), ReplicaRole::ActiveSecondary),
            member(replacement, ReplicaRole::ActiveSecondary),
        ],
        2,
    );
    let advanced = AdmittedAuthority {
        local_identity: primary,
        transition_kind: Some(TransitionKind::Replacement),
        previous_configuration: Some(previous),
        current_configuration: current,
    };
    let mut tracker = QuorumTracker::default();
    tracker.configure(initial.clone(), 10).unwrap();
    tracker
        .acknowledge(&acknowledgement(&initial, secondary, 10))
        .unwrap();
    assert_eq!(tracker.current_configuration_quorum_progress(), 10);

    tracker.configure(advanced, 10).unwrap();
    assert_eq!(tracker.current_configuration_quorum_progress(), 0);
    assert!(!tracker.catch_up_complete());
}

#[test]
fn catch_up_requires_recorded_cc_boundary() {
    let primary = identity(1, "primary");
    let second = identity(2, "second");
    let old = identity(3, "old");
    let replacement = identity(3, "replacement");
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 5),
        ReplicaId::new(1),
        vec![
            member(primary.clone(), ReplicaRole::Primary),
            member(second.clone(), ReplicaRole::ActiveSecondary),
            member(old, ReplicaRole::ActiveSecondary),
        ],
        2,
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 6),
        ReplicaId::new(1),
        vec![
            member(primary.clone(), ReplicaRole::Primary),
            member(second.clone(), ReplicaRole::ActiveSecondary),
            member(replacement.clone(), ReplicaRole::ActiveSecondary),
        ],
        2,
    );
    let authority = AdmittedAuthority {
        local_identity: primary,
        transition_kind: Some(TransitionKind::Replacement),
        previous_configuration: Some(previous),
        current_configuration: current,
    };
    let mut tracker = QuorumTracker::default();
    tracker.configure(authority.clone(), 10).unwrap();
    assert_eq!(tracker.catch_up_boundary(), Some(10));
    assert!(!tracker.catch_up_complete());

    tracker
        .acknowledge(&acknowledgement(&authority, replacement, 9))
        .unwrap();
    assert!(!tracker.catch_up_complete());
    tracker
        .acknowledge(&acknowledgement(&authority, second, 10))
        .unwrap();
    assert!(tracker.catch_up_complete());
}

#[test]
fn catch_up_requires_each_derived_must_catch_up_member() {
    let old_primary = identity(1, "one");
    let new_primary = identity(2, "two");
    let third = identity(3, "three");
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 5),
        ReplicaId::new(1),
        vec![
            member(old_primary.clone(), ReplicaRole::Primary),
            member(new_primary.clone(), ReplicaRole::ActiveSecondary),
            member(third.clone(), ReplicaRole::ActiveSecondary),
        ],
        2,
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 6),
        ReplicaId::new(2),
        vec![
            member(old_primary.clone(), ReplicaRole::ActiveSecondary),
            member(new_primary.clone(), ReplicaRole::Primary),
            member(third.clone(), ReplicaRole::ActiveSecondary),
        ],
        2,
    );
    let authority = AdmittedAuthority {
        local_identity: new_primary,
        transition_kind: Some(TransitionKind::Failover),
        previous_configuration: Some(previous),
        current_configuration: current,
    };
    let mut tracker = QuorumTracker::default();
    tracker.configure(authority.clone(), 5).unwrap();
    tracker
        .acknowledge(&acknowledgement(&authority, old_primary, 10))
        .unwrap();
    tracker
        .acknowledge(&acknowledgement(&authority, third, 10))
        .unwrap();

    assert_eq!(tracker.current_configuration_quorum_progress(), 10);
    assert!(!tracker.catch_up_complete());
    tracker.record_local_progress(10).unwrap();
    assert!(tracker.catch_up_complete());
}
