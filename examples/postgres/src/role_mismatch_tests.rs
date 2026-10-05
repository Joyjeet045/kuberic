use super::*;
use crate::testing::{PgGroup, ProcessProbe, native_configuration, native_identity, run_pg_test};

use std::time::Duration;

#[derive(Clone, Copy, Debug)]
enum Mismatch {
    StandbySignal,
    RecoverySignal,
    BothSignals,
    MissingStandbySignal,
    MissingRole,
    IncompleteRecovery,
    OrphanAcceptedBuild,
}

const MISMATCHES: [Mismatch; 7] = [
    Mismatch::StandbySignal,
    Mismatch::RecoverySignal,
    Mismatch::BothSignals,
    Mismatch::MissingStandbySignal,
    Mismatch::MissingRole,
    Mismatch::IncompleteRecovery,
    Mismatch::OrphanAcceptedBuild,
];

async fn inject(group: &PgGroup, mismatch: Mismatch) {
    let pod = group.pod(1);
    let data = pod.application.instance().data_dir();
    if matches!(mismatch, Mismatch::StandbySignal | Mismatch::BothSignals) {
        std::fs::write(data.join("standby.signal"), b"").unwrap();
    }
    if matches!(mismatch, Mismatch::RecoverySignal | Mismatch::BothSignals) {
        std::fs::write(data.join("recovery.signal"), b"").unwrap();
    }
    pod.application
        .native_driver()
        .durable
        .update(|state| {
            match mismatch {
                Mismatch::MissingStandbySignal => state.role = PgDurableRole::Standby,
                Mismatch::MissingRole => state.role = PgDurableRole::None,
                Mismatch::IncompleteRecovery => state.recovery_state = PgRecoveryState::Rebuilding,
                Mismatch::OrphanAcceptedBuild => {
                    state.accepted_build = Some(BuildAuthority {
                        build_id: OperationId::new("orphan"),
                        kind: kuberic_runtime::protocol::types::BuildAuthorityKind::Failover,
                        source: native_identity(2, "source"),
                        target: pod.identity.clone(),
                        current_configuration: native_configuration(
                            &[native_identity(2, "source"), pod.identity.clone()],
                            0,
                            2,
                        ),
                        replication_boundary_lsn: 0,
                    });
                }
                _ => {}
            }
            Ok(())
        })
        .await
        .unwrap();
}

async fn repair(group: &PgGroup) {
    let pod = group.pod(1);
    for signal in ["standby.signal", "recovery.signal"] {
        let path = pod.application.instance().data_dir().join(signal);
        if path.exists() {
            std::fs::remove_file(path).unwrap();
        }
    }
    pod.application
        .native_driver()
        .durable
        .update(|state| {
            state.role = PgDurableRole::Primary;
            state.recovery_state = PgRecoveryState::Ready;
            state.accepted_build = None;
            Ok(())
        })
        .await
        .unwrap();
}

async fn exercise(status: bool) {
    for mismatch in MISMATCHES {
        tracing::info!(?mismatch, status, "granted-primary mismatch fence");
        let mut group = PgGroup::singleton().await;
        let ordinary = group.session(1, false).await;
        let admin = group.session(1, true).await;
        let ordinary_probe = group.next_probe();
        let admin_probe = group.next_probe();
        let pod = group.pod(1);
        assert_eq!(
            pod.runtime.snapshot().await.write_status,
            AccessStatus::Granted
        );
        let processes = ProcessProbe::postgres(pod.application.instance().data_dir());
        inject(&group, mismatch).await;
        tokio::time::timeout(Duration::from_secs(10), async {
            if status {
                assert!(pod.status().await.is_err());
            } else {
                assert!(
                    pod.runtime
                        .primary_replicator()
                        .await
                        .unwrap()
                        .current_progress()
                        .await
                        .is_err()
                );
            }
        })
        .await
        .expect("mismatch fence deadline");
        assert_eq!(
            pod.runtime.partition_report().await.reported_fault,
            Some(FaultType::Permanent)
        );
        assert!(!pod.application.instance().is_running().await);
        processes.assert_reaped();
        ordinary.rejected(ordinary_probe).await;
        admin.rejected(admin_probe).await;
        ordinary.disconnected().await;
        admin.disconnected().await;
        // Repair only the injected inconsistency, then inspect the same PGDATA
        // through the closed, reconstructed host's administrative connection.
        repair(&group).await;
        let old = group.pods.remove(&1).unwrap();
        group.pods.insert(1, old.reopen().await);
        assert_eq!(group.contents(1).await, group.expected);
        assert!(
            group
                .pod(1)
                .application
                .instance()
                .connect_application()
                .await
                .is_err()
        );
        group.shutdown().await;
    }
}

#[test_log::test]
fn role_mismatches_fence_granted_primary_before_direct_progress_errors() {
    run_pg_test(|| exercise(false));
}

#[test_log::test]
fn role_mismatches_fence_granted_primary_before_agent_status_errors() {
    run_pg_test(|| exercise(true));
}

#[test_log::test]
fn retired_role_mismatch_cannot_fence_or_fault_successor() {
    run_pg_test(|| async {
        for mismatch in [Mismatch::StandbySignal, Mismatch::IncompleteRecovery] {
            let mut group = PgGroup::singleton().await;
            inject(&group, mismatch).await;
            let pod = group.pod(1);
            let gate = pod.application.instance().pause_error_handling();
            let control = pod.runtime.primary_replicator().await.unwrap();
            let old = tokio::spawn(async move { control.current_progress().await });
            tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
                .await
                .unwrap();
            repair(&group).await;
            let access = PgAccessController::new(pod.application.instance());
            access.close_external().await.unwrap();
            access.grant_role_access().await.unwrap();
            gate.release.notify_one();
            assert!(matches!(
                old.await.unwrap(),
                Err(RuntimeError::OperationCancelled)
            ));
            assert!(pod.application.instance().is_running().await);
            assert_eq!(pod.runtime.partition_report().await.reported_fault, None);
            assert_eq!(pod.store.load_state().await.unwrap().reported_fault, None);
            group.write("successor remains writable").await;
            assert_eq!(group.contents(1).await, group.expected);
            group.shutdown().await;
        }
    });
}
