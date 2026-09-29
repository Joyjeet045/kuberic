use kuberic_protocol::types::{AccessStatus, FaultType, OperationId, ReplicaRole};
use kuberic_runtime::engine::DurableState;
use kuberic_runtime_internal::authority::{LocalWriteJournal, LocalWritePhase};
use kuberic_runtime_internal::effects::RuntimeEffectAction;
use sqlite_replicated::proto::sqlite_store_server::SqliteStore as _;
use sqlite_replicated::state::PersistenceFault;
use sqlite_replicated::testing::{SqlitePod, scratch};
use sqlite_replicated::testing::{bootstrap, route, wait_applied};
use sqlite_replicated::{RecoveryState, SqlitePersistence};
use tonic::Code;

#[tokio::test]
async fn successful_batch_is_one_durable_replication_and_survives_reopen() {
    let root = scratch();
    let pod = SqlitePod::singleton(root.path().join("replica")).await;
    let schema = pod
        .execute("CREATE TABLE data(id INTEGER PRIMARY KEY, value TEXT)")
        .await
        .unwrap();
    let batch = pod
        .batch(&[
            "INSERT INTO data VALUES(1,'one')",
            "INSERT INTO data VALUES(2,'two')",
        ])
        .await
        .unwrap();
    assert_eq!(batch.lsn, schema.lsn + 1);
    assert_eq!(pod.count().await, 2);
    let old_vfs = pod.application.vfs_name().to_owned();
    let pod = pod.reopen().await;
    assert_ne!(old_vfs, pod.application.vfs_name());
    assert_eq!(pod.count().await, 2);
    assert_eq!(
        pod.application
            .persistence()
            .progress()
            .unwrap()
            .committed_lsn,
        batch.lsn
    );
}

#[tokio::test]
async fn waiting_without_durable_quorum_cannot_publish_sqlite_commit_bytes() {
    let root = scratch();
    let first = SqlitePod::new(1, root.path().join("first"), 2).await;
    let second = SqlitePod::new(2, root.path().join("second"), 2).await;
    bootstrap(&[&first, &second]).await;
    let routes = route(&first, &[&second]).await;
    first
        .execute("CREATE TABLE data(id INTEGER)")
        .await
        .unwrap();
    wait_applied(&[&second], 1).await;
    routes.stop().await;
    let writer = first.server.clone();
    let pending = tokio::spawn(async move {
        writer
            .execute(tonic::Request::new(
                sqlite_replicated::proto::ExecuteRequest {
                    sql: "INSERT INTO data VALUES(42)".into(),
                    params: Vec::new(),
                },
            ))
            .await
    });
    let item = first.runtime.data_plane().next_outbound().await.unwrap();
    let kuberic_agent::hosting::OutboundReplication::Replication(item) = item else {
        panic!("expected WAL replication")
    };
    assert_eq!(item.lsn, 2);
    assert_eq!(
        first
            .application
            .persistence()
            .progress()
            .unwrap()
            .committed_lsn,
        1
    );
    assert!(!pending.is_finished());
    let captured = root.path().join("captured");
    std::fs::create_dir_all(&captured).unwrap();
    for name in ["db.sqlite", "db.sqlite-wal"] {
        std::fs::copy(
            first.root.join("application").join(name),
            captured.join(name),
        )
        .unwrap();
    }
    let disk = rusqlite::Connection::open(captured.join("db.sqlite")).unwrap();
    assert_eq!(
        disk.query_row("SELECT COUNT(*) FROM data", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    first.runtime.abort();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.unwrap_err().code(), Code::Unknown);
    assert_eq!(
        second
            .application
            .persistence()
            .progress()
            .unwrap()
            .applied_lsn,
        1
    );
    assert!(matches!(
        first.application.persistence().recovery_state().unwrap(),
        RecoveryState::ReconciliationRequired(_)
    ));
}

#[tokio::test]
async fn write_closed_and_missing_barrier_are_definitive_and_leave_no_reservation() {
    let root = scratch();
    let pod = SqlitePod::singleton(root.path().join("replica")).await;
    pod.execute("CREATE TABLE data(id INTEGER)").await.unwrap();
    let before = pod.application.persistence().snapshot(1).unwrap();
    pod.effect(RuntimeEffectAction::SetAccessStatus {
        read: AccessStatus::Granted,
        write: AccessStatus::NoWriteQuorum,
    })
    .await
    .unwrap();
    assert_eq!(
        pod.execute("INSERT INTO data VALUES(1)")
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    assert_eq!(pod.count().await, 0);
    pod.grant().await;
    pod.application.barrier().uninstall();
    assert_eq!(
        pod.execute("INSERT INTO data VALUES(2)")
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    assert_eq!(pod.count().await, 0);
    assert!(!pod.application.barrier().is_fenced());
    assert!(pod.store.load_local_writes().await.unwrap().is_empty());
    assert_eq!(pod.application.persistence().snapshot(1).unwrap(), before);
    let pod = pod.reopen().await;
    assert_eq!(pod.count().await, 0);
}

#[tokio::test]
async fn dispatched_failure_cuts_recover_exact_reserved_bytes_and_fence_same_process_sql() {
    for (fault, applied, committed) in [
        (PersistenceFault::BeforeApply, 1, 1),
        (PersistenceFault::AfterApply, 2, 1),
        (PersistenceFault::AfterCommit, 2, 2),
    ] {
        let root = scratch();
        let pod = SqlitePod::singleton(root.path().join("replica")).await;
        pod.execute("CREATE TABLE data(id INTEGER)").await.unwrap();
        let original = pod.application.persistence().snapshot(1).unwrap();
        pod.application.persistence().fail_once(fault);
        assert_eq!(
            pod.execute("INSERT INTO data VALUES(42)")
                .await
                .unwrap_err()
                .code(),
            Code::Unknown
        );
        let progress = pod.application.persistence().progress().unwrap();
        assert_eq!(
            (progress.applied_lsn, progress.committed_lsn),
            (applied, committed)
        );
        assert!(matches!(
            pod.application.persistence().recovery_state().unwrap(),
            RecoveryState::ReconciliationRequired(_)
        ));
        assert!(pod.application.barrier().is_fenced());
        assert_eq!(
            pod.query("SELECT * FROM data").await.unwrap_err().code(),
            Code::FailedPrecondition
        );
        assert_eq!(
            pod.execute("INSERT INTO data VALUES(99)")
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
        let pending = pod.store.load_local_writes().await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].lsn, 2);
        assert_eq!(pod.application.persistence().snapshot(1).unwrap(), original);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while pod.runtime.partition_report().await.reported_fault != Some(FaultType::Transient)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let operation_id = pending[0].operation_id.clone();
        let exact = pending[0].data.clone();
        pod.runtime.abort();
        for companion in ["db.sqlite-wal", "db.sqlite-shm"] {
            std::fs::write(
                pod.root.join("application").join(companion),
                b"stale unpublished bytes",
            )
            .unwrap();
        }
        let pod = pod.reopen().await;
        // The first request after grant must rematerialize journal recovery, not
        // read the pre-grant connection opened at the preceding committed LSN.
        assert_eq!(pod.count().await, 1);
        assert_eq!(
            pod.application.persistence().recovery_state().unwrap(),
            RecoveryState::Healthy
        );
        assert!(pod.store.load_local_writes().await.unwrap().is_empty());
        let recovered = pod
            .store
            .load_local_write(&operation_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recovered.data, exact);
        assert_eq!(recovered.phase, LocalWritePhase::Committed);
        assert_eq!(
            pod.application
                .persistence()
                .progress()
                .unwrap()
                .committed_lsn,
            2
        );
        assert_eq!(
            pod.execute("INSERT INTO data VALUES(43)")
                .await
                .unwrap()
                .lsn,
            3
        );
        assert_eq!(pod.count().await, 2);
    }
}

#[tokio::test]
async fn quorum_committed_local_unpublished_is_unknown_but_reconcilable_not_rebuild_loss() {
    let root = scratch();
    let pod = SqlitePod::singleton(root.path().join("replica")).await;
    pod.execute("CREATE TABLE data(id INTEGER)").await.unwrap();
    pod.application.barrier().fail_after_quorum_once();
    assert_eq!(
        pod.execute("INSERT INTO data VALUES(7)")
            .await
            .unwrap_err()
            .code(),
        Code::Unknown
    );
    assert_eq!(
        pod.application
            .persistence()
            .progress()
            .unwrap()
            .committed_lsn,
        2
    );
    assert!(pod.store.load_local_writes().await.unwrap().is_empty());
    assert!(matches!(
        pod.application.persistence().recovery_state().unwrap(),
        RecoveryState::ReconciliationRequired(_)
    ));
    assert!(pod.query("SELECT * FROM data").await.is_err());
    pod.runtime.abort();
    {
        let disk = rusqlite::Connection::open(pod.root.join("application/db.sqlite")).unwrap();
        assert_eq!(
            disk.query_row("SELECT COUNT(*) FROM data", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
    let pod = pod.reopen().await;
    assert_eq!(pod.count().await, 1);
    assert_eq!(
        pod.application.persistence().recovery_state().unwrap(),
        RecoveryState::Healthy
    );
}

#[tokio::test]
async fn acknowledged_history_loss_stays_rebuild_fenced_across_reopen_until_copy() {
    let root = scratch();
    let pod = SqlitePod::singleton(root.path().join("replica")).await;
    pod.execute("CREATE TABLE data(id INTEGER)").await.unwrap();
    pod.execute("INSERT INTO data VALUES(5)").await.unwrap();
    let snapshot = pod.application.persistence().snapshot(2).unwrap();
    let path = pod.root.clone();
    pod.runtime.abort();
    drop(pod);
    std::fs::remove_file(path.join("application/history-0.log")).unwrap();
    for _ in 0..2 {
        let pod = SqlitePod::new(1, path.clone(), 1).await;
        pod.open().await;
        assert!(matches!(
            pod.application.persistence().recovery_state().unwrap(),
            RecoveryState::RebuildRequired(_)
        ));
        assert!(
            pod.application
                .persistence()
                .complete_reconciliation()
                .is_err()
        );
        assert!(pod.query("SELECT * FROM data").await.is_err());
        assert_eq!(
            pod.runtime.partition_report().await.reported_fault,
            Some(FaultType::Permanent)
        );
    }
    let persistence = SqlitePersistence::open(path.join("application")).unwrap();
    let build = OperationId::new("accepted-rebuild");
    persistence
        .apply_copy_chunk(
            &build,
            1,
            kuberic_runtime::application::CopyChunk {
                data: snapshot.into(),
            },
        )
        .await
        .unwrap();
    persistence.finish_copy(&build, 2, 2).await.unwrap();
    assert_eq!(
        persistence.recovery_state().unwrap(),
        RecoveryState::Healthy
    );
    let connection =
        rusqlite::Connection::open(persistence.materialize_committed().unwrap()).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM data", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn sql_cannot_disable_the_barrier_or_mutate_through_query() {
    let root = scratch();
    let pod = SqlitePod::singleton(root.path().join("replica")).await;
    pod.execute("CREATE TABLE data(id INTEGER)").await.unwrap();
    for statement in [
        "PRAGMA journal_mode=OFF",
        "ATTACH ':memory:' AS other",
        "BEGIN",
        "COMMIT",
        "CREATE TEMP TABLE scratch(id)",
    ] {
        assert!(pod.execute(statement).await.is_err(), "{statement}");
    }
    assert!(pod.query("INSERT INTO data VALUES(1)").await.is_err());
    assert_eq!(
        pod.application
            .persistence()
            .progress()
            .unwrap()
            .applied_lsn,
        1
    );
    assert_eq!(pod.count().await, 0);
}

#[tokio::test]
async fn open_role_access_close_and_abort_stay_write_closed_until_authorized() {
    let root = scratch();
    let pod = SqlitePod::new(1, root.path().join("replica"), 1).await;
    pod.open().await;
    assert_eq!(
        pod.execute("CREATE TABLE data(id)")
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    pod.effect(RuntimeEffectAction::AdmitAuthority(Box::new(
        sqlite_replicated::testing::authority(
            pod.identity.clone(),
            sqlite_replicated::testing::configuration(std::slice::from_ref(&pod.identity), 0, 1),
        ),
    )))
    .await
    .unwrap();
    pod.effect(RuntimeEffectAction::ChangeRole(ReplicaRole::Primary))
        .await
        .unwrap();
    assert_eq!(
        pod.execute("CREATE TABLE data(id)")
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    pod.grant().await;
    pod.execute("CREATE TABLE data(id)").await.unwrap();
    pod.effect(RuntimeEffectAction::Close).await.unwrap();
    assert!(pod.query("SELECT * FROM data").await.is_err());
    let pod = pod.reopen().await;
    assert!(pod.execute("INSERT INTO data VALUES(1)").await.is_err());
    pod.runtime.abort();
    assert!(pod.query("SELECT * FROM data").await.is_err());
}

#[tokio::test]
async fn instances_have_independent_barriers_fences_receipts_and_directories() {
    let root = scratch();
    let first = SqlitePod::singleton(root.path().join("first")).await;
    let second = SqlitePod::singleton(root.path().join("second")).await;
    assert_ne!(first.application.vfs_name(), second.application.vfs_name());
    first
        .execute("CREATE TABLE data(id INTEGER)")
        .await
        .unwrap();
    assert_eq!(
        second
            .application
            .persistence()
            .progress()
            .unwrap()
            .applied_lsn,
        0
    );
    second
        .execute("CREATE TABLE data(id INTEGER)")
        .await
        .unwrap();
    first
        .application
        .persistence()
        .fail_once(PersistenceFault::BeforeApply);
    assert_eq!(
        first
            .execute("INSERT INTO data VALUES(1)")
            .await
            .unwrap_err()
            .code(),
        Code::Unknown
    );
    assert!(!second.application.barrier().is_fenced());
    assert_eq!(
        second
            .execute("INSERT INTO data VALUES(2)")
            .await
            .unwrap()
            .lsn,
        2
    );
    assert_eq!(second.count().await, 1);
    assert_eq!(
        first
            .application
            .persistence()
            .progress()
            .unwrap()
            .applied_lsn,
        1
    );
}
