use super::*;
use crate::testing::{PgGroup, native_configuration, run_pg_test};
use kuberic_protocol::types::{AccessStatus, TransitionKind};
use kuberic_runtime_internal::authority::AdmittedAuthority;
use kuberic_runtime_internal::effects::RuntimeEffectAction;

async fn group() -> PgGroup {
    let mut group = PgGroup::singleton().await;
    for id in 2..=5 {
        let pod = group.candidate(id).await;
        group.build_candidate(&pod, &format!("subset-{id}")).await;
        group.pods.insert(id, pod);
    }
    group.link().await;
    let members = group
        .pods
        .values()
        .map(|pod| pod.identity.clone())
        .collect::<Vec<_>>();
    let configuration = native_configuration(&members, 0, 2);
    for id in [2, 3, 4, 5, 1] {
        let pod = group.pod(id);
        pod.admit(configuration.clone()).await;
        if id != 1 {
            pod.effect(RuntimeEffectAction::ChangeRole(
                ReplicaRole::ActiveSecondary,
            ))
            .await
            .unwrap();
        }
    }
    group.configuration = configuration;
    for id in 1..=5 {
        group
            .pod(id)
            .effect(RuntimeEffectAction::SetAccessStatus {
                read: AccessStatus::Granted,
                write: if id == 1 {
                    AccessStatus::Granted
                } else {
                    AccessStatus::NotPrimary
                },
            })
            .await
            .unwrap();
    }
    group.write("all eligible members").await;
    group.assert_contents().await;
    for id in [4, 5] {
        group.pod(id).disconnect_receiver().await.unwrap();
    }
    group
        .write("acknowledged by the two freshest exact responders")
        .await;
    group
        .pod(1)
        .effect(RuntimeEffectAction::ChangeRole(ReplicaRole::None))
        .await
        .unwrap();
    group
}

async fn stale(group: &mut PgGroup, id: i64) {
    let old = group.pods.remove(&id).unwrap();
    let session = old.session.clone();
    let replacement = old.reopen().await;
    assert_ne!(replacement.session, session);
    group.pods.insert(id, replacement);
    for other in 2..=5 {
        if other != id {
            group.pod(other).peer(group.pod(id)).await;
            group.pod(id).peer(group.pod(other)).await;
        }
    }
}

async fn admit(group: &mut PgGroup) {
    let previous = group.configuration.clone();
    let members = group
        .pods
        .values()
        .map(|pod| pod.identity.clone())
        .collect::<Vec<_>>();
    let current = native_configuration(&members, 1, 3);
    for id in [3, 4, 5, 2] {
        group
            .pod(id)
            .effect(RuntimeEffectAction::AdmitAuthority(Box::new(
                AdmittedAuthority {
                    local_identity: group.pod(id).identity.clone(),
                    current_configuration: current.clone(),
                    previous_configuration: Some(previous.clone()),
                    transition_kind: Some(TransitionKind::Failover),
                    switchover_handoff: None,
                    secondary_removal: None,
                    scale_up: None,
                },
            )))
            .await
            .unwrap();
    }
    group.configuration = current;
}

async fn accepted(group: &PgGroup) -> Policy {
    let state = group
        .pod(2)
        .application
        .native_driver()
        .durable_state()
        .await;
    let policy = state.recovery.unwrap().accepted_policy.unwrap();
    assert_eq!(policy.policy.eligible_standbys.len(), 4);
    assert_eq!(policy.policy.write_acknowledgements, 2);
    policy
}

#[test_log::test]
fn stale_optional_responder_preserves_sufficient_exact_subset_and_acknowledged_data() {
    run_pg_test(|| async {
        let mut group = group().await;
        let policy = accepted(&group).await;
        stale(&mut group, 5).await;
        admit(&mut group).await;
        assert_eq!(accepted(&group).await, policy);
        group
            .pod(2)
            .effect(RuntimeEffectAction::ChangeRole(ReplicaRole::Primary))
            .await
            .unwrap();
        group
            .pod(2)
            .effect(RuntimeEffectAction::SetAccessStatus {
                read: AccessStatus::Granted,
                write: AccessStatus::Granted,
            })
            .await
            .unwrap();
        let state = group
            .pod(2)
            .application
            .native_driver()
            .durable_state()
            .await;
        let election = state.recovery.unwrap().pending.unwrap();
        assert_eq!(election.policy, policy);
        assert_eq!(election.responders.len(), 3);
        assert_eq!(election.final_observations.len(), 3);
        assert!(election.ready && election.promoted);
        for (peer, observation) in election.responders.iter().zip(&election.final_observations) {
            assert_eq!(&observation.peer, peer);
            assert_eq!(
                peer.session,
                group.pod(peer.identity.replica_id.value()).session
            );
            validate_observation(&policy, observation, true).unwrap();
            assert_ne!(peer.identity.replica_id.value(), 5);
        }
        let native = group
            .pod(2)
            .application
            .native_driver()
            .observer
            .snapshot()
            .await
            .unwrap()
            .evidence
            .unwrap();
        let synchronous = native.synchronous.unwrap();
        assert_eq!(synchronous, state.synchronous.unwrap());
        assert_eq!(synchronous.write_acknowledgements, 2);
        assert_eq!(synchronous.eligible_standbys.len(), 2);
        assert!(
            synchronous.eligible_standbys.iter().all(|peer| [3, 4]
                .contains(&peer.identity.replica_id.value())
                && peer.process_session_id == group.pod(peer.identity.replica_id.value()).session)
        );
        let admin = group.session(2, true).await;
        let setting: String = admin
            .client()
            .query_one("SHOW synchronous_standby_names", &[])
            .await
            .unwrap()
            .get(0);
        let names = synchronous
            .eligible_standbys
            .iter()
            .map(|peer| {
                crate::native::replication_application_name(
                    &peer.identity,
                    &peer.process_session_id,
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        assert_eq!(setting, format!("ANY 2 ({names})"));
        assert_eq!(group.contents(2).await, group.expected);
        group.write("recovered subset synchronous write").await;
        for id in [2, 3, 4] {
            assert_eq!(group.contents(id).await, group.expected);
        }
        group.shutdown().await;
    });
}

#[test_log::test]
fn stale_optional_responders_do_not_reduce_persisted_quorum_requirement() {
    run_pg_test(|| async {
        let mut group = group().await;
        let policy = accepted(&group).await;
        stale(&mut group, 4).await;
        stale(&mut group, 5).await;
        admit(&mut group).await;
        assert!(
            group
                .pod(2)
                .effect(RuntimeEffectAction::ChangeRole(ReplicaRole::Primary))
                .await
                .is_err()
        );
        let state = group
            .pod(2)
            .application
            .native_driver()
            .durable_state()
            .await;
        let recovery = state.recovery.unwrap();
        assert_eq!(recovery.accepted_policy.as_ref(), Some(&policy));
        assert!(recovery.pending.is_none());
        assert_ne!(
            group.pod(2).runtime.snapshot().await.write_status,
            AccessStatus::Granted
        );
        assert!(
            group
                .pod(2)
                .application
                .instance()
                .connect_application()
                .await
                .is_err()
        );
        let evidence =
            crate::native::PgNativeObserver::new(group.pod(2).application.instance().clone())
                .snapshot()
                .await
                .unwrap()
                .evidence
                .unwrap();
        assert!(evidence.in_recovery);
        assert_eq!(group.contents(2).await, group.expected);
        group.shutdown().await;
    });
}

#[test_log::test]
fn selected_subset_policy_loss_after_initial_round_cannot_promote() {
    run_pg_test(|| async {
        let mut group = group().await;
        stale(&mut group, 5).await;
        admit(&mut group).await;
        {
            let gate = group
                .pod(2)
                .application
                .native_driver()
                .pause_recovery(RecoveryStage::InitialRound);
            let work = group
                .pod(2)
                .effect(RuntimeEffectAction::ChangeRole(ReplicaRole::Primary));
            tokio::pin!(work);
            tokio::select! {
                result = &mut work => panic!("election missed initial-round checkpoint: {result:?}"),
                entered = tokio::time::timeout(Duration::from_secs(20), gate.entered.notified()) => entered.unwrap(),
            }
            let before = group
                .pod(2)
                .application
                .native_driver()
                .durable_state()
                .await
                .recovery
                .unwrap()
                .pending
                .unwrap();
            assert_eq!(before.responders.len(), 3);
            group
                .pod(3)
                .application
                .native_driver()
                .durable
                .update(|state| {
                    state.recovery.as_mut().unwrap().accepted_policy = None;
                    Ok(())
                })
                .await
                .unwrap();
            gate.release.notify_one();
            assert!(work.await.is_err());
            let after = group
                .pod(2)
                .application
                .native_driver()
                .durable_state()
                .await
                .recovery
                .unwrap()
                .pending
                .unwrap();
            assert_eq!(after.responders, before.responders);
            assert!(after.boundary.is_none() && !after.promoted && !after.ready);
            assert_ne!(
                group.pod(2).runtime.snapshot().await.write_status,
                AccessStatus::Granted
            );
            assert_eq!(group.contents(3).await, group.expected);
        }
        group.shutdown().await;
    });
}
