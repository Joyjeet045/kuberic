use super::*;
use crate::testing::{PgGroup, ProcessProbe, native_configuration, run_pg_test};
use kuberic_runtime::protocol::types::{AccessStatus, FaultType, TransitionKind};
use kuberic_runtime::replicator::Replicator;
use kuberic_runtime::testing::report::AgentReporter;
use kuberic_runtime::testing::{authority::AdmittedAuthority, effects::RuntimeEffectAction};

async fn followed_candidate() -> PgGroup {
    let mut group = PgGroup::singleton().await;
    group.add(2).await;
    group.add(3).await;
    assert_eq!(group.change_primary(2, true).await, 1);
    group.rejoin(1).await;
    group.write("acknowledged after first handoff").await;
    let durable = group
        .pod(3)
        .application
        .native_driver()
        .durable_state()
        .await;
    assert_eq!(durable.role, PgDurableRole::Standby);
    assert!(durable.native_build.is_none());
    assert!(durable.recovery.unwrap().followed.is_some());

    group.pod(1).disconnect_receiver().await.unwrap();
    group.write("candidate's exclusive acknowledgement").await;
    group
        .pod(2)
        .effect(RuntimeEffectAction::ChangeRole(ReplicaRole::None))
        .await
        .unwrap();
    let previous = group.configuration.clone();
    let current = native_configuration(
        &[
            group.pod(3).identity.clone(),
            group.pod(2).identity.clone(),
            group.pod(1).identity.clone(),
        ],
        0,
        previous.epoch.configuration_number + 1,
    );
    for id in [1, 3] {
        group
            .pod(id)
            .effect(RuntimeEffectAction::AdmitAuthority(Box::new(
                AdmittedAuthority {
                    local_identity: group.pod(id).identity.clone(),
                    previous_configuration: Some(previous.clone()),
                    current_configuration: current.clone(),
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
    group
}

async fn assert_promotion_gap(group: &PgGroup) -> u64 {
    let pod = group.pod(3);
    let instance = pod.application.instance();
    let durable = pod.application.native_driver().durable_state().await;
    assert_eq!(durable.role, PgDurableRole::Standby);
    assert!(durable.native_build.is_none());
    assert!(durable.external_access_closed);
    assert!(!instance.data_dir().join("standby.signal").exists());
    let recovery = durable.recovery.unwrap();
    let election = recovery.pending.unwrap();
    assert!(!election.promoted);
    assert!(!election.ready);
    assert!(election.boundary.is_some());
    let intent = election.promotion.unwrap();
    assert_eq!(intent.candidate.identity, pod.identity);
    assert_eq!(intent.candidate.session, pod.session);
    assert_eq!(intent.process_generation, instance.generation_id());
    assert!(instance.is_running().await);
    assert!(instance.connect_application().await.is_err());
    assert_ne!(
        pod.runtime.snapshot().await.write_status,
        AccessStatus::Granted
    );
    assert_eq!(pod.runtime.partition_report().await.reported_fault, None);
    assert_eq!(pod.store.load_state().await.unwrap().reported_fault, None);
    assert_eq!(group.contents(3).await, group.expected);
    instance.generation_id()
}

async fn complete_and_fence(mut group: PgGroup, generation: u64) {
    let pod = group.pod(3);
    let instance = pod.application.instance();
    let durable = pod.application.native_driver().durable_state().await;
    assert_eq!(durable.role, PgDurableRole::Primary);
    assert!(durable.recovery.unwrap().pending.unwrap().ready);
    assert_eq!(instance.generation_id(), generation);
    assert!(instance.is_running().await);
    assert_eq!(pod.runtime.partition_report().await.reported_fault, None);
    assert!(instance.connect_application().await.is_err());
    assert_ne!(
        pod.runtime.snapshot().await.write_status,
        AccessStatus::Granted
    );
    assert_eq!(group.contents(3).await, group.expected);
    pod.effect(RuntimeEffectAction::SetAccessStatus {
        read: AccessStatus::Granted,
        write: AccessStatus::Granted,
    })
    .await
    .unwrap();
    group.write("acknowledged after second promotion").await;
    assert_eq!(group.contents(3).await, group.expected);

    let ordinary = group.session(3, false).await;
    let admin = group.session(3, true).await;
    let ordinary_probe = group.next_probe();
    let admin_probe = group.next_probe();
    let pod = group.pod(3);
    let processes = ProcessProbe::postgres(pod.application.instance().data_dir());
    // A completed election is not a blanket exemption for subsequent corruption.
    std::fs::write(
        pod.application.instance().data_dir().join("standby.signal"),
        b"",
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        assert!(
            pod.application
                .native_driver()
                .current_progress()
                .await
                .is_err()
        );
        assert!(!pod.status().await.unwrap().healthy);
    })
    .await
    .unwrap();
    assert!(!pod.application.instance().is_running().await);
    assert_eq!(
        pod.runtime.partition_report().await.reported_fault,
        Some(FaultType::Permanent)
    );
    processes.assert_reaped();
    ordinary.rejected(ordinary_probe).await;
    admin.rejected(admin_probe).await;
    ordinary.disconnected().await;
    admin.disconnected().await;
    std::fs::remove_file(pod.application.instance().data_dir().join("standby.signal")).unwrap();
    let old = group.pods.remove(&3).unwrap();
    group.pods.insert(3, old.reopen().await);
    assert_eq!(group.contents(3).await, group.expected);
    group.shutdown().await;
}

async fn concurrent_status() {
    let group = followed_candidate().await;
    let pod = group.pod(3);
    let driver = pod.application.native_driver();
    let gate = driver.pause_recovery(RecoveryStage::Promoted);
    let mut promotion = Box::pin(pod.effect(RuntimeEffectAction::ChangeRole(ReplicaRole::Primary)));
    tokio::time::timeout(Duration::from_secs(30), async {
        tokio::select! {
            result = &mut promotion => panic!("promotion missed its cut: {result:?}"),
            entered = tokio::time::timeout(Duration::from_secs(30), gate.entered.notified()) => entered.unwrap(),
        }
    })
    .await
    .unwrap();
    let generation = assert_promotion_gap(&group).await;
    let reporter = AgentReporter::new(pod.store.clone());
    let mut progress = Box::pin(driver.current_progress());
    let mut report = Box::pin(reporter.report(&pod.runtime));
    let report_result = tokio::time::timeout(Duration::from_millis(250), &mut report)
        .await
        .expect("read-only status must not wait for promotion callbacks")
        .unwrap();
    assert_ne!(
        report_result.write_status,
        kuberic_runtime::control::proto::AccessStatus::Granted as i32
    );
    drop(report);
    assert!(
        tokio::time::timeout(Duration::from_millis(250), &mut progress)
            .await
            .is_err(),
        "progress remains serialized with role publication"
    );
    assert_eq!(assert_promotion_gap(&group).await, generation);
    gate.release.notify_one();
    tokio::time::timeout(Duration::from_secs(30), async {
        let (promoted, progress) = tokio::join!(&mut promotion, &mut progress);
        promoted.unwrap();
        progress.unwrap();
    })
    .await
    .unwrap();
    drop(progress);
    drop(promotion);
    complete_and_fence(group, generation).await;
}

#[test_log::test]
fn authorized_promotion_serializes_progress_and_agent_reporter_without_native_build() {
    run_pg_test(concurrent_status);
}

#[test_log::test]
fn interrupted_authorized_promotion_retries_without_native_build() {
    run_pg_test(|| async {
        let group = followed_candidate().await;
        let pod = group.pod(3);
        let driver = pod.application.native_driver();
        let gate = driver.pause_recovery(RecoveryStage::Promoted);
        let mut promotion =
            Box::pin(pod.effect(RuntimeEffectAction::ChangeRole(ReplicaRole::Primary)));
        tokio::select! {
            result = &mut promotion => panic!("promotion missed its cut: {result:?}"),
            entered = tokio::time::timeout(Duration::from_secs(30), gate.entered.notified()) => entered.unwrap(),
        }
        let generation = assert_promotion_gap(&group).await;
        drop(promotion);
        driver.current_progress().await.unwrap();
        pod.status().await.unwrap();
        assert_eq!(assert_promotion_gap(&group).await, generation);
        pod.effect(RuntimeEffectAction::ChangeRole(ReplicaRole::Primary))
            .await
            .unwrap();
        complete_and_fence(group, generation).await;
    });
}

#[test_log::test]
fn interrupted_promotion_reconciles_stopped_storage_but_rejects_new_session() {
    run_pg_test(|| async {
        let group = followed_candidate().await;
        let pod = group.pod(3);
        let driver = pod.application.native_driver();
        let gate = driver.pause_recovery(RecoveryStage::Promoted);
        let mut promotion =
            Box::pin(pod.effect(RuntimeEffectAction::ChangeRole(ReplicaRole::Primary)));
        tokio::select! {
            result = &mut promotion => panic!("promotion missed its cut: {result:?}"),
            entered = tokio::time::timeout(Duration::from_secs(30), gate.entered.notified()) => entered.unwrap(),
        }
        let generation = assert_promotion_gap(&group).await;
        drop(promotion);
        let processes = ProcessProbe::postgres(pod.application.instance().data_dir());
        pod.application.instance().stop().await.unwrap();
        processes.assert_reaped();
        assert_ne!(pod.application.instance().generation_id(), generation);
        let mut installed = driver.configuration.write().await.take().unwrap();
        {
            let _state = driver.state.lock().await;
            let reconciled = driver.validate().await.unwrap();
            assert_eq!(reconciled.role, PgDurableRole::Primary);
            let election = reconciled.recovery.unwrap().pending.unwrap();
            assert!(election.promoted);
            assert!(!election.ready);
        }
        assert!(!pod.application.instance().is_running().await);
        installed
            .replicas
            .iter_mut()
            .find(|replica| replica.identity == pod.identity)
            .unwrap()
            .process_session_id = ProcessSessionId::new("reconstructed-candidate-session");
        let previous = driver.durable.snapshot().await.recovery.unwrap().previous;
        driver
            .install_configuration_pair(installed, previous)
            .await
            .unwrap();
        assert!(matches!(
            driver
                .change_role(group.configuration.epoch, ReplicaRole::Primary)
                .await,
            Err(RuntimeError::AuthorityNotAdmitted)
        ));
        assert_eq!(pod.runtime.partition_report().await.reported_fault, None);
        assert_eq!(pod.store.load_state().await.unwrap().reported_fault, None);
        assert!(
            pod.application
                .instance()
                .connect_application()
                .await
                .is_err()
        );
        assert_eq!(group.contents(3).await, group.expected);
        group.shutdown().await;
    });
}

#[derive(Clone, Copy, Debug)]
enum InvalidIntent {
    Missing,
    Generation,
    Policy,
    ReceiverEpoch,
    Session,
}

#[test_log::test]
fn interrupted_promotion_rejects_nonexact_authorization() {
    run_pg_test(|| async {
        let group = followed_candidate().await;
        let pod = group.pod(3);
        let driver = pod.application.native_driver();
        let gate = driver.pause_recovery(RecoveryStage::Promoted);
        let mut promotion =
            Box::pin(pod.effect(RuntimeEffectAction::ChangeRole(ReplicaRole::Primary)));
        tokio::select! {
            result = &mut promotion => panic!("promotion missed its cut: {result:?}"),
            entered = tokio::time::timeout(Duration::from_secs(30), gate.entered.notified()) => entered.unwrap(),
        }
        let generation = assert_promotion_gap(&group).await;
        drop(promotion);
        let durable = driver.durable.snapshot().await;
        let installed = driver.configuration.read().await.clone().unwrap();
        for invalid in [
            InvalidIntent::Missing,
            InvalidIntent::Generation,
            InvalidIntent::Policy,
            InvalidIntent::ReceiverEpoch,
            InvalidIntent::Session,
        ] {
            tracing::info!(?invalid, "rejecting nonexact promotion intent");
            let mut candidate = durable.clone();
            let recovery = candidate.recovery.as_mut().unwrap();
            let election = recovery.pending.as_mut().unwrap();
            match invalid {
                InvalidIntent::Missing => election.promotion = None,
                InvalidIntent::Generation => {
                    election.promotion.as_mut().unwrap().process_generation += 1;
                }
                InvalidIntent::Policy => {
                    recovery.accepted_policy.as_mut().unwrap().generation += 1;
                }
                InvalidIntent::ReceiverEpoch => recovery.receiver_epoch = None,
                InvalidIntent::Session => {
                    let mut mismatched = installed.clone();
                    mismatched
                        .replicas
                        .iter_mut()
                        .find(|replica| replica.identity == pod.identity)
                        .unwrap()
                        .process_session_id = ProcessSessionId::new("different-candidate-session");
                    *driver.configuration.write().await = Some(mismatched);
                }
            }
            assert!(
                !driver
                    .interrupted_promotion(&candidate, generation, false)
                    .await
                    .unwrap(),
                "{invalid:?}"
            );
            *driver.configuration.write().await = Some(installed.clone());
        }

        let processes = ProcessProbe::postgres(pod.application.instance().data_dir());
        driver
            .durable
            .update(|state| {
                state
                    .recovery
                    .as_mut()
                    .unwrap()
                    .pending
                    .as_mut()
                    .unwrap()
                    .promotion = None;
                Ok(())
            })
            .await
            .unwrap();
        assert!(driver.current_progress().await.is_err());
        assert!(!pod.application.instance().is_running().await);
        assert_eq!(
            pod.runtime.partition_report().await.reported_fault,
            Some(FaultType::Permanent)
        );
        processes.assert_reaped();
        group.shutdown().await;
    });
}
