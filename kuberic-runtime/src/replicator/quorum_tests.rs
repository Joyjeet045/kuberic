use super::QuorumTracker;
use crate::authority::AdmittedAuthority;
use crate::protocol::types::{
    AgentGeneration, ConfigurationDescriptor, ConfigurationMember, Epoch, ReplicaId,
    ReplicaIdentity, ReplicaInstanceId, ReplicaRole, TransitionKind,
};
use crate::removal_fixture;
use crate::transport::ReplicationAck;

fn removal_authority(size: i64, current_only: bool) -> AdmittedAuthority {
    let intent = removal_fixture::intent(&(1..=size).collect::<Vec<_>>(), 1);
    AdmittedAuthority {
        local_identity: intent.primary.clone(),
        transition_kind: (!current_only).then_some(TransitionKind::SecondaryScaleDown),
        previous_configuration: (!current_only).then(|| intent.previous_configuration.clone()),
        current_configuration: intent.current_configuration.clone(),
        switchover_handoff: None,
        secondary_removal: Some(removal_fixture::evidence(&intent)),
        scale_up: None,
    }
}

#[test]
fn reduction_quorums_require_session_bound_verified_cc_progress_not_raw_acks() {
    use crate::protocol::types::{ProcessSessionId, SecondaryRemovalStage};
    for size in 2..=5 {
        let authority = removal_authority(size, false);
        let intent = authority
            .secondary_removal
            .as_ref()
            .unwrap()
            .preparation
            .intent
            .clone();
        let mut tracker = QuorumTracker::default();
        tracker.configure(authority.clone(), 100).unwrap();
        assert_eq!(tracker.catch_up_boundary(), Some(10));
        assert!(
            !tracker.catch_up_complete(),
            "raw local progress is not a prefix proof"
        );
        tracker.record_verified_local_progress(10);
        tracker
            .acknowledge(&acknowledgement(&authority, intent.target.clone(), 100))
            .unwrap();
        for member in &intent.current_configuration.members {
            tracker
                .acknowledge(&acknowledgement(&authority, member.identity.clone(), 100))
                .unwrap();
        }
        assert_eq!(tracker.catch_up_complete(), size == 2);
        let witnesses = removal_fixture::witnesses(&intent, SecondaryRemovalStage::PreviousCurrent);
        for (index, witness) in witnesses.iter().skip(1).enumerate() {
            assert!(
                tracker.observe_secondary_removal(witness).is_err(),
                "session must be registered"
            );
            tracker
                .register_peer_session(witness.identity.clone(), witness.process_session_id.clone())
                .unwrap();
            let mut stale = witness.clone();
            stale.identity.agent_generation = AgentGeneration::new("stale");
            assert!(tracker.observe_secondary_removal(&stale).is_err());
            stale = witness.clone();
            stale.process_session_id = ProcessSessionId::new("old-session");
            assert!(tracker.observe_secondary_removal(&stale).is_err());
            stale = witness.clone();
            stale.verified_replication_lsn = 9;
            assert!(tracker.observe_secondary_removal(&stale).is_err());
            stale = witness.clone();
            stale.identity = intent.target.clone();
            assert!(tracker.observe_secondary_removal(&stale).is_err());
            tracker.observe_secondary_removal(witness).unwrap();
            tracker.observe_secondary_removal(witness).unwrap();
            let mut conflicting = witness.clone();
            conflicting.verified_replication_lsn += 1;
            assert!(
                tracker.observe_secondary_removal(&conflicting).is_err(),
                "same sequence cannot change evidence"
            );
            assert_eq!(
                tracker.catch_up_complete(),
                index + 2 >= intent.current_policy.write_quorum as usize
            );
        }
        assert!(tracker.catch_up_complete());
        if size > 2 {
            for witness in witnesses.iter().skip(1) {
                tracker
                    .register_peer_session(
                        witness.identity.clone(),
                        ProcessSessionId::new("restarted"),
                    )
                    .unwrap();
                assert!(tracker.observe_secondary_removal(witness).is_err());
            }
            assert!(
                !tracker.catch_up_complete(),
                "all old-session credit was discarded"
            );
        }
        tracker
            .configure(removal_authority(size, true), 100)
            .unwrap();
        assert!(
            !tracker.catch_up_complete(),
            "authority change drops all verified credit"
        );
        tracker.record_verified_local_progress(10);
        assert_eq!(tracker.catch_up_complete(), size == 2);
        assert!(
            tracker
                .acknowledge(&acknowledgement(&authority, intent.target, 100))
                .is_err()
        );
    }
}

#[tokio::test]
async fn reduction_evidence_never_relaxes_general_dual_write_quorum_commit() {
    for size in [2, 4] {
        let authority = removal_authority(size, false);
        let mut tracker = QuorumTracker::default();
        tracker.configure(authority.clone(), 10).unwrap();
        let completion = tracker.register_write(11).unwrap();
        tracker.record_local_progress(11).unwrap();
        for member in authority
            .current_configuration
            .members
            .iter()
            .skip(1)
            .take(authority.current_configuration.write_quorum as usize - 1)
        {
            tracker
                .acknowledge(&acknowledgement(&authority, member.identity.clone(), 11))
                .unwrap();
        }
        assert_eq!(tracker.current_configuration_quorum_progress(), 11);
        assert_eq!(
            tracker.ready_commit_lsn(),
            None,
            "PC read quorum is not PC client write quorum"
        );
        let target = authority
            .secondary_removal
            .as_ref()
            .unwrap()
            .preparation
            .intent
            .target
            .clone();
        tracker
            .acknowledge(&acknowledgement(&authority, target, 11))
            .unwrap();
        assert_eq!(tracker.ready_commit_lsn(), Some(11));
        tracker.finalize_commit(11).unwrap();
        assert_eq!(completion.await.unwrap().unwrap(), 11);
    }
}

#[test]
fn reduced_authority_rejects_arbitrary_membership_and_conflicting_completion() {
    let authority = removal_authority(3, false);
    authority.validate().unwrap();
    let complete = removal_authority(3, true);
    assert!(complete.is_current_only_completion_of(&authority));
    let mut wrong = complete.clone();
    wrong
        .secondary_removal
        .as_mut()
        .unwrap()
        .preparation
        .boundary_lsn += 1;
    assert!(!wrong.is_current_only_completion_of(&authority));
    assert!(wrong.validate().is_err());
    let mut missing = authority.clone();
    missing.secondary_removal = None;
    assert!(missing.validate().is_err());
    let mut target = authority.clone();
    target.local_identity = target
        .secondary_removal
        .as_ref()
        .unwrap()
        .preparation
        .intent
        .target
        .clone();
    assert!(target.validate().is_err());
    let mut unrelated = authority.clone();
    unrelated.current_configuration = complete
        .secondary_removal
        .as_ref()
        .unwrap()
        .preparation
        .intent
        .previous_configuration
        .clone();
    assert!(unrelated.validate().is_err());
}

#[test]
fn exact_peer_eviction_discards_both_windows_and_cannot_reconnect() {
    use crate::protocol::types::ProcessSessionId;
    use crate::replicator::sender::{ReliableSender, ResumeWindow};
    use crate::transport::{OutboundOperation, ReplicationItem};
    let authority = removal_authority(2, false);
    let target = authority
        .secondary_removal
        .as_ref()
        .unwrap()
        .preparation
        .intent
        .target
        .clone();
    let mut sender = ReliableSender::new(ProcessSessionId::new("local"), 8).unwrap();
    sender
        .admit_peer(target.clone(), ProcessSessionId::new("target"))
        .unwrap();
    let item = ReplicationItem {
        sender: authority.local_identity.clone(),
        receiver: target.clone(),
        epoch: authority.current_configuration.epoch,
        previous_configuration_id: authority.fence().previous_configuration_id,
        current_configuration_id: authority.current_configuration.configuration_id.clone(),
        lsn: 1,
        committed_lsn: 0,
        data: bytes::Bytes::new(),
    };
    sender
        .queue(OutboundOperation::Replication(item.clone()))
        .unwrap();
    let copy = crate::transport::CopyItem {
        build_id: crate::protocol::types::OperationId::new("old-build"),
        sender: authority.local_identity.clone(),
        receiver: target.clone(),
        epoch: item.epoch,
        current_configuration_id: item.current_configuration_id.clone(),
        sequence: 1,
        lsn: 1,
        committed_lsn: 0,
        replication_boundary_lsn: 1,
        catch_up_boundary_lsn: Some(1),
        final_item: true,
        snapshot_chunk: true,
        data: bytes::Bytes::new(),
    };
    sender.queue(OutboundOperation::Copy(copy.clone())).unwrap();
    assert!(matches!(
        sender.reconnect_replication(&target, 1).unwrap(),
        ResumeWindow::Retained(_)
    ));
    sender
        .queue(OutboundOperation::Evict(target.clone()))
        .unwrap();
    assert!(sender.reconnect_replication(&target, 1).is_err());
    assert!(sender.queue(OutboundOperation::Replication(item)).is_err());
    assert!(sender.queue(OutboundOperation::Copy(copy)).is_err());
    assert!(
        sender
            .admit_peer(target.clone(), ProcessSessionId::new("late"))
            .is_err()
    );
    let mut replacement = target;
    replacement.instance_id = ReplicaInstanceId::new("different-incarnation");
    sender
        .admit_peer(replacement, ProcessSessionId::new("replacement"))
        .unwrap();
}

#[tokio::test]
async fn session_change_invalidates_ordinary_client_quorum_credit_and_stale_acks() {
    use crate::protocol::types::ProcessSessionId;
    let mut authority = removal_authority(4, true);
    authority.secondary_removal = None;
    let peer = authority.current_configuration.members[1].identity.clone();
    let mut tracker = QuorumTracker::default();
    tracker.configure(authority.clone(), 10).unwrap();
    tracker
        .register_peer_session(peer.clone(), ProcessSessionId::new("old"))
        .unwrap();
    let ack = acknowledgement(&authority, peer.clone(), 10);
    tracker
        .acknowledge_in_session(&ack, &ProcessSessionId::new("old"))
        .unwrap();
    let completion = tracker.register_write(10).unwrap();
    assert_eq!(tracker.ready_commit_lsn(), Some(10));
    tracker
        .register_peer_session(peer, ProcessSessionId::new("new"))
        .unwrap();
    assert!(
        tracker
            .register_peer_session(ack.receiver.clone(), ProcessSessionId::new("old"))
            .is_err()
    );
    assert!(
        tracker
            .acknowledge_in_session(&ack, &ProcessSessionId::new("old"))
            .is_err()
    );
    assert_eq!(tracker.ready_commit_lsn(), None);
    tracker
        .acknowledge_in_session(&ack, &ProcessSessionId::new("new"))
        .unwrap();
    tracker.finalize_commit(10).unwrap();
    assert_eq!(completion.await.unwrap().unwrap(), 10);
}

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
        secondary_removal: None,
        scale_up: None,
        local_identity: primary,
        transition_kind: Some(TransitionKind::Replacement),
        previous_configuration: Some(previous),
        current_configuration: current,
        switchover_handoff: None,
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
        secondary_removal: None,
        scale_up: None,
        local_identity: primary,
        transition_kind: None,
        previous_configuration: None,
        current_configuration: current,
        switchover_handoff: None,
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
        crate::protocol::types::ConfigurationId::new("stale");
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
        secondary_removal: None,
        scale_up: None,
        local_identity: primary.clone(),
        transition_kind: None,
        previous_configuration: None,
        current_configuration: ConfigurationDescriptor::new(
            Epoch::new(0, 1),
            ReplicaId::new(1),
            members.clone(),
            2,
        ),
        switchover_handoff: None,
    };
    let advanced = AdmittedAuthority {
        secondary_removal: None,
        scale_up: None,
        local_identity: primary,
        transition_kind: None,
        previous_configuration: None,
        current_configuration: ConfigurationDescriptor::new(
            Epoch::new(0, 2),
            ReplicaId::new(1),
            members,
            2,
        ),
        switchover_handoff: None,
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
        secondary_removal: None,
        scale_up: None,
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
        switchover_handoff: None,
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
        secondary_removal: None,
        scale_up: None,
        local_identity: primary,
        transition_kind: Some(TransitionKind::Replacement),
        previous_configuration: Some(previous),
        current_configuration: current,
        switchover_handoff: None,
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
        secondary_removal: None,
        scale_up: None,
        local_identity: primary,
        transition_kind: Some(TransitionKind::Replacement),
        previous_configuration: Some(previous),
        current_configuration: current,
        switchover_handoff: None,
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
        secondary_removal: None,
        scale_up: None,
        local_identity: new_primary,
        transition_kind: Some(TransitionKind::Failover),
        previous_configuration: Some(previous),
        current_configuration: current,
        switchover_handoff: None,
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
