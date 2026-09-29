use kuberic_protocol::types::AccessStatus;
use kuberic_runtime_internal::effects::RuntimeEffectAction;
use sqlite_replicated::proto;
use sqlite_replicated::testing::*;

#[test]
fn fence_classifier_rejects_unknown_constraints_and_arbitrary_errors() {
    for status in [
        tonic::Status::unknown("outcome unknown after replication dispatch"),
        tonic::Status::invalid_argument("SQL rejected: UNIQUE constraint failed"),
        tonic::Status::failed_precondition("SQL is fenced for this service instance"),
        tonic::Status::unavailable("transaction rejected before dispatch: no replication barrier"),
        tonic::Status::unavailable("network failure"),
        tonic::Status::internal("disk error"),
    ] {
        assert!(definitive_fence(&Err(status)).is_err());
    }
    assert!(
        definitive_fence(&Ok(proto::ExecuteResponse {
            rows_affected: 1,
            last_insert_rowid: 1,
            lsn: 2,
        }))
        .is_err()
    );
    for reason in [
        "runtime is not primary",
        "writes are closed with status ReconfigurationPending",
        "writes are closed with status NoWriteQuorum",
    ] {
        assert!(definitive_fence(&Err(tonic::Status::unavailable(reason))).is_ok());
    }
}

#[tokio::test]
async fn unique_stale_probes_cannot_hide_success_behind_a_previous_probe_constraint() {
    let root = scratch();
    let pod = SqlitePod::singleton(root.path().join("one")).await;
    create_data(&pod).await;
    // The previous oracle reused this ID and could accept its UNIQUE error.
    let old_probe = write_receipt(&pod, -999).await;
    let before = pod.application.persistence().progress().unwrap();
    let failure = probe_closed(&pod).await.unwrap_err();
    assert!(failure.contains("succeeded"));
    assert_eq!(
        pod.application
            .persistence()
            .progress()
            .unwrap()
            .applied_lsn,
        before.applied_lsn + 1
    );
    // The exact-set oracle must reject this additional row, even though every
    // acknowledged expected row can still be found.
    assert!(verify_contents(&pod, std::slice::from_ref(&old_probe)).is_err());
    close_access(&pod).await;
    let first = assert_closed(&pod).await;
    let second = assert_closed(&pod).await;
    assert_ne!(first.id, second.id);
    assert_ne!(first.id, -999);
    assert_ne!(second.id, -999);
    assert_eq!(
        pod.application
            .persistence()
            .progress()
            .unwrap()
            .applied_lsn,
        before.applied_lsn + 1
    );
    let pod = pod.reopen().await;
    let third = assert_closed(&pod).await;
    assert_ne!(second.id, third.id);
}

#[tokio::test]
async fn exact_history_oracle_rejects_extra_rows_and_writes_that_leave_no_extra_row() {
    let root = scratch();
    let pod = SqlitePod::singleton(root.path().join("one")).await;
    create_data(&pod).await;
    let receipt = write_receipt(&pod, 1).await;
    assert_receipts(&pod, std::slice::from_ref(&receipt)).await;
    pod.execute("INSERT INTO data VALUES(77,'unexpected')")
        .await
        .unwrap();
    assert!(verify_contents(&pod, std::slice::from_ref(&receipt)).is_err());
    pod.execute("DELETE FROM data WHERE id=77").await.unwrap();
    // Final rows alone look correct again, but exact history still rejects.
    let error = verify_contents(&pod, std::slice::from_ref(&receipt)).unwrap_err();
    assert!(error.contains("history"));
    let pod = pod.reopen().await;
    assert!(verify_contents(&pod, std::slice::from_ref(&receipt)).is_err());
    pod.effect(RuntimeEffectAction::SetWriteStatus(
        AccessStatus::NoWriteQuorum,
    ))
    .await
    .unwrap();
    assert_closed(&pod).await;
}

#[tokio::test]
async fn probe_rejects_real_post_dispatch_unknown_and_missing_barrier_errors() {
    let root = scratch();
    let unknown = SqlitePod::singleton(root.path().join("unknown")).await;
    create_data(&unknown).await;
    unknown.application.barrier().fail_after_quorum_once();
    let reason = probe_closed(&unknown).await.unwrap_err();
    assert!(reason.contains("not a definitive authority/access fence"));
    assert_eq!(
        unknown
            .application
            .persistence()
            .progress()
            .unwrap()
            .committed_lsn,
        2
    );
    assert!(unknown.application.barrier().is_fenced());

    let unavailable = SqlitePod::singleton(root.path().join("unavailable")).await;
    create_data(&unavailable).await;
    unavailable.application.barrier().uninstall();
    let reason = probe_closed(&unavailable).await.unwrap_err();
    assert!(reason.contains("not a definitive authority/access fence"));
    assert_eq!(
        unavailable
            .application
            .persistence()
            .progress()
            .unwrap()
            .applied_lsn,
        1
    );
    assert!(!unavailable.application.barrier().is_fenced());
}
