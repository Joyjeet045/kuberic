use futures::FutureExt;
use kuberic_agent::store::AgentStore;
use kuberic_protocol::types::{AccessStatus, ReplicaRole};
use kuberic_runtime_internal::authority::LocalWriteJournal;
use kuberic_runtime_internal::effects::RuntimeEffectAction;
use sqlite_replicated::proto::sqlite_store_server::SqliteStore as _;
use sqlite_replicated::testing::*;
use sqlite_replicated::{RecoveryState, proto};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Cut {
    BeforeReservation,
    AfterApply,
    AfterQuorum,
}

#[tokio::test]
async fn authority_revocation_at_three_sql_cuts_preserves_exact_outcome_semantics() {
    for cut in [Cut::BeforeReservation, Cut::AfterApply, Cut::AfterQuorum] {
        let root = scratch();
        let source = SqlitePod::new(1, root.path().join("source"), 3).await;
        let target = SqlitePod::new(2, root.path().join("target"), 3).await;
        let witness = SqlitePod::new(3, root.path().join("witness"), 3).await;
        let previous = bootstrap(&[&source, &target, &witness]).await;
        let routes = route(&source, &[&target, &witness]).await;
        create_data(&source).await;
        let baseline = write_receipt(&source, 1).await;
        wait_applied(&[&target, &witness], baseline.lsn).await;
        let mut routes = Some(routes);
        if cut == Cut::AfterApply {
            routes.take().unwrap().stop().await;
        }
        let gate = match cut {
            Cut::BeforeReservation => &source.application.barrier().before_dispatch,
            Cut::AfterApply => &source.application.persistence().after_apply,
            Cut::AfterQuorum => &source.application.barrier().after_quorum,
        };
        gate.arm();
        let server = source.server.clone();
        let sql = tokio::spawn(async move {
            server
                .execute(tonic::Request::new(proto::ExecuteRequest {
                    sql: "INSERT INTO data VALUES(99,'interrupted')".into(),
                    params: Vec::new(),
                }))
                .await
        });
        gate.wait_entered().await;
        let before = source.application.persistence().progress().unwrap();
        match cut {
            Cut::BeforeReservation => {
                assert_eq!(
                    (before.applied_lsn, before.committed_lsn),
                    (baseline.lsn, baseline.lsn)
                );
                assert!(source.store.load_local_writes().await.unwrap().is_empty());
            }
            Cut::AfterApply => {
                assert_eq!(
                    (before.applied_lsn, before.committed_lsn),
                    (baseline.lsn + 1, baseline.lsn)
                );
                assert_eq!(source.store.load_local_writes().await.unwrap().len(), 1);
            }
            Cut::AfterQuorum => {
                assert_eq!(
                    (before.applied_lsn, before.committed_lsn),
                    (baseline.lsn + 1, baseline.lsn + 1)
                );
                assert!(source.store.load_local_writes().await.unwrap().is_empty());
                wait_applied(&[&target, &witness], before.applied_lsn).await;
            }
        }
        assert!(!sql.is_finished());
        let mut revoke = Box::pin(source.effect(RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::ReconfigurationPending,
            write: AccessStatus::ReconfigurationPending,
        }));
        if cut == Cut::AfterApply {
            // Authority changes serialize behind application acceptance. Record
            // the actual durable pending effect before releasing that boundary.
            assert!(revoke.as_mut().now_or_never().is_none());
            assert!(
                source
                    .store
                    .load_state()
                    .await
                    .unwrap()
                    .pending_effect
                    .is_some()
            );
            gate.release();
            revoke.await.unwrap();
        } else {
            tokio::time::timeout(std::time::Duration::from_secs(5), revoke.as_mut())
                .await
                .unwrap()
                .unwrap();
            assert_closed(&source).await;
            gate.release();
        }
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), sql)
            .await
            .unwrap()
            .unwrap();
        let boundary = match cut {
            Cut::BeforeReservation => {
                assert_eq!(outcome.unwrap_err().code(), tonic::Code::Unavailable);
                assert!(source.store.load_local_writes().await.unwrap().is_empty());
                assert_eq!(
                    source.application.persistence().recovery_state().unwrap(),
                    RecoveryState::Healthy
                );
                baseline.lsn
            }
            Cut::AfterApply => {
                assert_eq!(outcome.unwrap_err().code(), tonic::Code::Unknown);
                assert!(source.application.barrier().is_fenced());
                assert!(matches!(
                    source.application.persistence().recovery_state().unwrap(),
                    RecoveryState::ReconciliationRequired(_)
                ));
                // Deliver the exact accepted WAL suffix while the old writer is
                // closed; this is recovery evidence, not a successful old RPC.
                routes = Some(route(&source, &[&target, &witness]).await);
                wait_applied(&[&target, &witness], baseline.lsn + 1).await;
                baseline.lsn + 1
            }
            Cut::AfterQuorum => {
                let delayed = outcome.unwrap().into_inner();
                assert_eq!(delayed.lsn, baseline.lsn + 1);
                assert!(!source.application.barrier().is_fenced());
                delayed.lsn
            }
        };
        assert_closed(&source).await;
        if let Some(routes) = routes {
            routes.stop().await;
        }
        let _current = change_primary(
            &[&source, &target, &witness],
            &previous,
            &target,
            true,
            boundary,
        )
        .await;
        let mut expected_transactions = vec![baseline];
        if cut != Cut::BeforeReservation {
            // AfterApply was unknown to its caller but exactly recovered by the
            // handoff. AfterQuorum was a delayed success, not a new stale write.
            expected_transactions.push(SqlReceipt {
                id: 99,
                value: "interrupted".into(),
                lsn: boundary,
            });
        }
        assert_receipts(&target, &expected_transactions).await;
        assert_durable_receipts(&witness, &expected_transactions);
        assert_closed(&source).await;
        assert_closed(&witness).await;
        let target = target.reopen().await;
        assert_receipts(&target, &expected_transactions).await;
        assert_durable_receipts(&witness, &expected_transactions);
    }
}

#[tokio::test]
async fn quorum_loss_and_fresh_session_restoration_preserve_acknowledged_sql_without_data_loss() {
    let root = scratch();
    let source = SqlitePod::new(1, root.path().join("source"), 3).await;
    let second = SqlitePod::new(2, root.path().join("second"), 3).await;
    let third = SqlitePod::new(3, root.path().join("third"), 3).await;
    bootstrap(&[&source, &second, &third]).await;
    let routes = route(&source, &[&second, &third]).await;
    create_data(&source).await;
    let receipt = write_receipt(&source, 1).await;
    wait_applied(&[&second, &third], receipt.lsn).await;
    routes.stop().await;
    second.runtime.abort();
    third.runtime.abort();
    source
        .effect(RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::Granted,
            write: AccessStatus::NoWriteQuorum,
        })
        .await
        .unwrap();
    assert_closed(&source).await;
    assert_receipts(&source, std::slice::from_ref(&receipt)).await;
    assert!(source.store.load_local_writes().await.unwrap().is_empty());
    let second = second.reopen().await;
    let third = third.reopen().await;
    let mut routes = route(&source, &[&second, &third]).await;
    for pod in [&second, &third] {
        source
            .runtime
            .repair_peer(pod.identity.clone(), receipt.lsn - 1)
            .await
            .unwrap();
    }
    routes
        .wait_for_applied(&[&second, &third], receipt.lsn)
        .await;
    source.grant().await;
    let next = write_receipt(&source, 2).await;
    wait_applied(&[&second, &third], next.lsn).await;
    let before = source.application.persistence().progress().unwrap();
    assert!(
        !source
            .runtime
            .primary_replicator()
            .await
            .unwrap()
            .on_data_loss()
            .await
            .unwrap()
    );
    assert_eq!(source.application.persistence().progress().unwrap(), before);
    assert_eq!(source.runtime.snapshot().await.role, ReplicaRole::Primary);
    routes.stop().await;
    let source = source.reopen().await;
    assert_receipts(&source, &[receipt, next]).await;
    assert_closed(&second).await;
    assert_closed(&third).await;
}
