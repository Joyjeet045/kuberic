use futures::TryStreamExt;
use kuberic_agent::testing::{InProcessTransport, TransportError, TransportEvent};
use kuberic_protocol::types::{AccessStatus, ReplicaRole, SwitchoverRequestId, TransitionKind};
use kuberic_runtime::engine::DurableState;
use kuberic_runtime_internal::effects::RuntimeEffectAction;
use sqlite_replicated::state::PersistenceFault;
use sqlite_replicated::testing::{
    SqlitePod, authority, bootstrap, configuration, route, scratch, wait_applied,
};

#[tokio::test]
async fn v2_sqlite_replication_covers_multi_page_schema_and_secondary_restart() {
    let root = scratch();
    let first = SqlitePod::new(1, root.path().join("one"), 3).await;
    let second = SqlitePod::new(2, root.path().join("two"), 3).await;
    let third = SqlitePod::new(3, root.path().join("three"), 3).await;
    bootstrap(&[&first, &second, &third]).await;
    let routes = route(&first, &[&second, &third]).await;
    first
        .execute("CREATE TABLE data(id INTEGER PRIMARY KEY,value TEXT)")
        .await
        .unwrap();
    let statements: Vec<_> = (0..50)
        .map(|id| format!("INSERT INTO data VALUES({id},'{}')", "x".repeat(2048)))
        .collect();
    let batch = first
        .batch(&statements.iter().map(String::as_str).collect::<Vec<_>>())
        .await
        .unwrap();
    assert_eq!(batch.lsn, 2);
    let altered = first
        .execute("ALTER TABLE data ADD COLUMN extra TEXT DEFAULT 'v2'")
        .await
        .unwrap();
    wait_applied(&[&second, &third], altered.lsn).await;
    assert_eq!(first.count().await, 50);
    assert!(second.query("SELECT * FROM data").await.is_err());
    routes.stop().await;
    let second = second.reopen().await;
    assert_eq!(
        second
            .application
            .persistence()
            .progress()
            .unwrap()
            .applied_lsn,
        altered.lsn
    );
    let _routes = route(&first, &[&second, &third]).await;
    let next = first
        .execute("INSERT INTO data(id,value) VALUES(50,'after-restart')")
        .await
        .unwrap();
    wait_applied(&[&second, &third], next.lsn).await;
    assert_eq!(first.count().await, 51);
    let committed = rusqlite::Connection::open(
        second
            .application
            .persistence()
            .materialize_committed()
            .unwrap(),
    )
    .unwrap();
    // The newest insert remains applied-only here, while the preceding
    // multi-page batch and schema change are already durably committed.
    assert_eq!(
        committed
            .query_row("SELECT COUNT(*) FROM data WHERE extra='v2'", [], |row| row
                .get::<_, i64>(
                0
            ))
            .unwrap(),
        50
    );
    assert_ne!(first.application.vfs_name(), second.application.vfs_name());
    assert_ne!(second.application.vfs_name(), third.application.vfs_name());
}

#[tokio::test]
async fn stream_ack_requires_durable_sqlite_acceptance() {
    let root = scratch();
    let first = SqlitePod::new(1, root.path().join("one"), 2).await;
    let second = SqlitePod::new(2, root.path().join("two"), 2).await;
    bootstrap(&[&first, &second]).await;
    let mut transport = InProcessTransport::new();
    for pod in [&first, &second] {
        transport
            .register(pod.runtime.clone(), pod.session.id().clone())
            .await
            .unwrap();
    }
    second
        .application
        .persistence()
        .fail_once(PersistenceFault::BeforeApply);
    let writer = first.server.clone();
    let pending = tokio::spawn(async move {
        use sqlite_replicated::proto::sqlite_store_server::SqliteStore;
        writer
            .execute(tonic::Request::new(
                sqlite_replicated::proto::ExecuteRequest {
                    sql: "CREATE TABLE data(id INTEGER)".into(),
                    params: Vec::new(),
                },
            ))
            .await
    });
    let mut received = false;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            for event in transport.next().await.events {
                match event {
                    TransportEvent::Received {
                        acknowledgement, ..
                    } => {
                        assert_eq!(acknowledgement.applied_lsn, 0);
                        received = true;
                    }
                    TransportEvent::Rejected {
                        error: TransportError::Runtime(_),
                        ..
                    } => return,
                    other => panic!("unexpected event: {other:?}"),
                }
            }
        }
    })
    .await
    .unwrap();
    assert!(received);
    assert!(!pending.is_finished());
    assert_eq!(
        second
            .application
            .persistence()
            .progress()
            .unwrap()
            .applied_lsn,
        0
    );
    assert!(transport.pump().idle);
    first
        .runtime
        .repair_peer(second.identity.clone(), 0)
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            for event in transport.next().await.events {
                match event {
                    TransportEvent::Applied {
                        acknowledgement, ..
                    } => {
                        assert_eq!(acknowledgement.applied_lsn, 1);
                        return;
                    }
                    TransportEvent::Received { .. } => {}
                    other => panic!("unexpected event: {other:?}"),
                }
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        second
            .application
            .persistence()
            .progress()
            .unwrap()
            .applied_lsn,
        1
    );
    assert_eq!(pending.await.unwrap().unwrap().into_inner().lsn, 1);
    let old_session = second.session.id().clone();
    transport
        .unregister(&second.identity, &old_session)
        .unwrap();
    let second = second.reopen().await;
    transport
        .register(second.runtime.clone(), second.session.id().clone())
        .await
        .unwrap();
    assert!(matches!(
        transport
            .register(second.runtime.clone(), old_session)
            .await,
        Err(TransportError::StaleSession { .. })
    ));
    assert_eq!(
        second
            .application
            .persistence()
            .progress()
            .unwrap()
            .applied_lsn,
        1
    );
}

#[tokio::test]
async fn unverified_applied_suffix_is_not_visible_or_extended_by_new_sql() {
    let root = scratch();
    let source = SqlitePod::singleton(root.path().join("source")).await;
    source
        .execute("CREATE TABLE data(id INTEGER)")
        .await
        .unwrap();
    source.execute("INSERT INTO data VALUES(42)").await.unwrap();
    let operations = source
        .application
        .persistence()
        .get_replication_operations(1, 2)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let candidate = SqlitePod::new(2, root.path().join("candidate"), 1).await;
    // An inbound-only suffix has no primary-local reservation journal. The test
    // authority certifies only its first LSN, not all physically present bytes.
    for operation in operations {
        candidate
            .application
            .persistence()
            .apply(operation)
            .await
            .unwrap();
    }
    candidate.open().await;
    candidate
        .effect(RuntimeEffectAction::AdmitAuthority(Box::new(authority(
            candidate.identity.clone(),
            configuration(std::slice::from_ref(&candidate.identity), 0, 2),
        ))))
        .await
        .unwrap();
    candidate
        .effect(RuntimeEffectAction::AuthorizeFailoverPrefix(1))
        .await
        .unwrap();
    candidate
        .effect(RuntimeEffectAction::ChangeRole(ReplicaRole::Primary))
        .await
        .unwrap();
    candidate.grant().await;
    assert_eq!(candidate.count().await, 0);
    assert_eq!(
        candidate
            .execute("INSERT INTO data VALUES(99)")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        candidate
            .application
            .persistence()
            .progress()
            .unwrap()
            .applied_lsn,
        2
    );
    let candidate = candidate.reopen().await;
    assert_eq!(candidate.count().await, 0);
    assert_eq!(
        candidate
            .execute("INSERT INTO data VALUES(99)")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    candidate
        .effect(RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::ReconfigurationPending,
            write: AccessStatus::ReconfigurationPending,
        })
        .await
        .unwrap();
    candidate
        .effect(RuntimeEffectAction::AuthorizeFailoverPrefix(2))
        .await
        .unwrap();
    candidate
        .effect(RuntimeEffectAction::ChangeRole(ReplicaRole::Primary))
        .await
        .unwrap();
    candidate.grant().await;
    assert_eq!(candidate.count().await, 1);
    candidate
        .execute("INSERT INTO data VALUES(43)")
        .await
        .unwrap();
    assert_eq!(candidate.count().await, 2);
}

#[tokio::test]
async fn failover_and_planned_handoff_materialize_last_acknowledged_write_before_new_sql() {
    for planned in [false, true] {
        let root = scratch();
        let first = SqlitePod::new(1, root.path().join("one"), 3).await;
        let second = SqlitePod::new(2, root.path().join("two"), 3).await;
        let third = SqlitePod::new(3, root.path().join("three"), 3).await;
        let previous = bootstrap(&[&first, &second, &third]).await;
        let routes = route(&first, &[&second, &third]).await;
        first
            .execute("CREATE TABLE data(id INTEGER)")
            .await
            .unwrap();
        let receipt = first.execute("INSERT INTO data VALUES(42)").await.unwrap();
        wait_applied(&[&second, &third], receipt.lsn).await;
        assert_eq!(
            second
                .application
                .persistence()
                .progress()
                .unwrap()
                .committed_lsn,
            receipt.lsn - 1
        );
        let handoff = if planned {
            first
                .effect(RuntimeEffectAction::PrepareSwitchover {
                    preparation_generation: 1,
                    request_id: SwitchoverRequestId::new("sqlite-handoff"),
                    source: first.identity.clone(),
                    target: second.identity.clone(),
                    starting_configuration_id: previous.configuration_id.clone(),
                    starting_epoch: previous.epoch,
                })
                .await
                .unwrap();
            use kuberic_agent::store::AgentStore;
            Some(
                first
                    .store
                    .load_state()
                    .await
                    .unwrap()
                    .prepared_switchover
                    .unwrap(),
            )
        } else {
            first.runtime.abort();
            None
        };
        routes.stop().await;
        let members = vec![
            first.identity.clone(),
            second.identity.clone(),
            third.identity.clone(),
        ];
        let current = configuration(&members, 1, 2);
        let survivors = if planned {
            vec![&first, &second, &third]
        } else {
            vec![&second, &third]
        };
        for pod in &survivors {
            let mut admitted = authority(pod.identity.clone(), current.clone());
            admitted.previous_configuration = Some(previous.clone());
            admitted.transition_kind = Some(if planned {
                TransitionKind::PlannedSwitchover
            } else {
                TransitionKind::Failover
            });
            admitted.switchover_handoff = handoff.clone();
            pod.effect(RuntimeEffectAction::AdmitAuthority(Box::new(admitted)))
                .await
                .unwrap();
            if !planned {
                pod.effect(RuntimeEffectAction::AuthorizeFailoverPrefix(receipt.lsn))
                    .await
                    .unwrap();
            }
            pod.effect(RuntimeEffectAction::ChangeRole(
                if pod.identity == second.identity {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            ))
            .await
            .unwrap();
        }
        let targets = if planned {
            vec![&first, &third]
        } else {
            vec![&third]
        };
        let _routes = route(&second, &targets).await;
        for target in &targets {
            second
                .runtime
                .repair_peer(target.identity.clone(), 0)
                .await
                .unwrap();
        }
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            second.effect(RuntimeEffectAction::WaitForCatchup),
        )
        .await
        .unwrap()
        .unwrap();
        second.grant().await;
        assert_eq!(
            second.count().await,
            1,
            "acknowledged row must be visible before any new SQL write"
        );
        assert_eq!(
            second
                .application
                .persistence()
                .progress()
                .unwrap()
                .committed_lsn,
            receipt.lsn
        );
        assert!(first.execute("INSERT INTO data VALUES(99)").await.is_err());
        let old = first.runtime.snapshot().await;
        if planned {
            assert_ne!(old.write_status, AccessStatus::Granted);
        } else {
            assert!(
                !old.open,
                "aborted hosts cannot serve even if their last status was Granted"
            );
        }
        // Finish PC/CC convergence before testing an ordinary process restart.
        // A mid-transition restart correctly requires fresh catch-up evidence.
        for pod in &survivors {
            let mut completed = authority(pod.identity.clone(), current.clone());
            completed.switchover_handoff = handoff.clone();
            pod.effect(RuntimeEffectAction::AdmitAuthority(Box::new(completed)))
                .await
                .unwrap();
        }
        second.grant().await;
        // Restart the promoted replica using both reopened durable stores.
        _routes.stop().await;
        let second = second.reopen().await;
        assert_eq!(second.count().await, 1);
        let _routes = route(&second, &targets).await;
        second.execute("INSERT INTO data VALUES(43)").await.unwrap();
        assert_eq!(second.count().await, 2);
    }
}
