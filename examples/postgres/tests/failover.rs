macro_rules! host_test {
    ($name:ident) => {
        #[test_log::test]
        fn $name() {
            std::thread::Builder::new()
                .name(stringify!($name).into())
                .stack_size(16 * 1024 * 1024)
                .spawn(|| {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap()
                        .block_on(scenarios::$name());
                })
                .unwrap()
                .join()
                .unwrap();
        }
    };
}

mod scenarios {

    use kuberic_runtime::protocol::types::{
        AccessStatus, ReplicaRole, SwitchoverRequestId, TransitionKind,
    };
    use kuberic_runtime::testing::authority::AdmittedAuthority;
    use kuberic_runtime::testing::effects::RuntimeEffectAction;
    use postgres_replicated::testing::{
        PgPod, RecoveryStage, TestDataDir, definitive_fence_error, native_configuration,
        native_identity,
    };
    use std::time::Duration;
    use tokio_postgres::error::SqlState;

    #[derive(Clone, Copy)]
    enum Failure {
        Promotion,
        Observation,
        Shutdown,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum PlannedRestart {
        SourcePreFence,
        SourceAuthorityInstalled,
        SourceFenceIntent,
        SourceFenced,
        SourceDemoted,
        TargetAuthorityInstalled,
        TargetPromotion,
        TargetActivated,
        TargetGranted,
    }

    #[derive(Default)]
    struct Case {
        planned: bool,
        cut: Option<RecoveryStage>,
        policy_cut: Option<RecoveryStage>,
        live_gap: bool,
        quorum_loss: bool,
        changed_session: bool,
        reopen_cut: bool,
        failure: Option<Failure>,
        joint_gap: bool,
        explicit_demote: bool,
        alternate_candidate: bool,
        planned_restart: Option<PlannedRestart>,
    }

    async fn write_rejected(client: &tokio_postgres::Client, statement: &str) {
        let error = tokio::time::timeout(Duration::from_secs(5), client.simple_query(statement))
            .await
            .expect("a timeout is not a fencing proof")
            .expect_err("stale write succeeded");
        assert!(
            definitive_fence_error(&error),
            "not a definitive fence: {error:?}"
        );
    }

    async fn disconnected(client: &tokio_postgres::Client) {
        let error = tokio::time::timeout(Duration::from_secs(5), client.simple_query("SELECT 1"))
            .await
            .expect("disconnect oracle cannot time out")
            .expect_err("fenced SQL session remained connected");
        assert!(
            error.is_closed()
                || error.code().is_some_and(|code| matches!(
                    *code,
                    SqlState::ADMIN_SHUTDOWN | SqlState::CRASH_SHUTDOWN
                )),
            "not a completed disconnect: {error:?}"
        );
    }

    async fn rows_absent(pod: &PgPod, ids: &[i64]) {
        let (client, connection) = admin(pod).await;
        for id in ids {
            let count = tokio::time::timeout(
                Duration::from_secs(5),
                client.query_one("SELECT count(*) FROM recovered WHERE id=$1::bigint", &[id]),
            )
            .await
            .expect("absence oracle cannot time out")
            .unwrap()
            .get::<_, i64>(0);
            assert_eq!(count, 0, "fenced write {id} became visible");
        }
        drop(client);
        connection.await.unwrap().unwrap();
    }

    async fn exercise_primary_change(case: Case) {
        let Case {
            planned,
            cut,
            policy_cut,
            live_gap,
            quorum_loss,
            changed_session,
            reopen_cut,
            failure,
            joint_gap,
            explicit_demote,
            alternate_candidate,
            planned_restart,
        } = case;
        let root = TestDataDir::new("p5-elect");
        let source = PgPod::new(root.path().join("s"), native_identity(1, "source")).await;
        let fault_marker = root.path().join("helper-fault");
        let bin = match failure {
            Some(Failure::Promotion) => postgres_replicated::testing::wrapped_pg_bin(
                root.path(),
                "pg_ctl",
                &format!(
                    "#!/bin/sh\nif test \"$1\" = promote; then echo 'injected promotion failure' >&2; exit 9; fi\nexec '{}/pg_ctl' \"$@\"\n",
                    postgres_replicated::testing::find_pg_bin().display()
                ),
            ),
            Some(Failure::Observation) => postgres_replicated::testing::wrapped_pg_bin(
                root.path(),
                "pg_controldata",
                &format!(
                    "#!/bin/sh\nif test -f '{}'; then echo malformed; exit 0; fi\nexec '{}/pg_controldata' \"$@\"\n",
                    fault_marker.display(),
                    postgres_replicated::testing::find_pg_bin().display()
                ),
            ),
            None | Some(Failure::Shutdown) => postgres_replicated::testing::find_pg_bin(),
        };
        let target =
            PgPod::with_bin(root.path().join("t"), native_identity(2, "target"), bin).await;
        let other = PgPod::new(root.path().join("o"), native_identity(3, "other")).await;
        source.singleton().await;
        let (sql, connection) = source
            .application
            .instance()
            .connect_application()
            .await
            .unwrap();
        sql.batch_execute("CREATE TABLE recovered(id int primary key)")
            .await
            .unwrap();
        let build = source.authorize(&target, "initial").await;
        source.build(&target, &build).await.unwrap();
        let build = source.authorize(&other, "other").await;
        source.build(&other, &build).await.unwrap();
        target.peer(&other).await;
        other.peer(&target).await;
        let mut previous = native_configuration(
            &[
                source.identity.clone(),
                target.identity.clone(),
                other.identity.clone(),
            ],
            0,
            2,
        );
        let accepted_previous = previous.clone();
        other.admit(previous.clone()).await;
        other
            .effect(RuntimeEffectAction::ChangeRole(
                ReplicaRole::ActiveSecondary,
            ))
            .await
            .unwrap();
        target.admit(previous.clone()).await;
        target
            .effect(RuntimeEffectAction::ChangeRole(
                ReplicaRole::ActiveSecondary,
            ))
            .await
            .unwrap();
        source.admit(previous.clone()).await;
        source
            .effect(RuntimeEffectAction::SetAccessStatus {
                read: AccessStatus::Granted,
                write: AccessStatus::Granted,
            })
            .await
            .unwrap();
        if joint_gap {
            let next = native_configuration(
                &[
                    source.identity.clone(),
                    target.identity.clone(),
                    other.identity.clone(),
                ],
                0,
                3,
            );
            for pod in [&target, &other, &source] {
                pod.effect(RuntimeEffectAction::AdmitAuthority(Box::new(
                    AdmittedAuthority {
                        local_identity: pod.identity.clone(),
                        previous_configuration: Some(previous.clone()),
                        current_configuration: next.clone(),
                        transition_kind: Some(TransitionKind::Failover),
                        switchover_handoff: None,
                        scale_up: None,
                        secondary_removal: None,
                    },
                )))
                .await
                .unwrap();
            }
            source
                .effect(RuntimeEffectAction::SetAccessStatus {
                    read: AccessStatus::Granted,
                    write: AccessStatus::Granted,
                })
                .await
                .unwrap();
            previous = next;
        }
        drop(sql);
        connection.await.unwrap();
        let (sql, _) = source
            .application
            .instance()
            .connect_application()
            .await
            .unwrap();
        if !planned {
            request_replay_pause(&other).await;
        }
        let published_progress = source.runtime.snapshot().await.current_progress;
        if joint_gap {
            assert!(
                source
                    .runtime
                    .snapshot()
                    .await
                    .authority
                    .unwrap()
                    .previous_configuration
                    .is_some()
            );
        }
        tokio::time::timeout(
            Duration::from_secs(10),
            sql.batch_execute("INSERT INTO recovered VALUES(1)"),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            source.runtime.snapshot().await.current_progress,
            published_progress
        );
        if !planned {
            let (lagging, task) = admin(&other).await;
            assert_eq!(
                lagging
                    .query_one("SELECT count(*) FROM recovered WHERE id=1", &[])
                    .await
                    .unwrap()
                    .get::<_, i64>(0),
                0
            );
            drop(lagging);
            task.await.unwrap().unwrap();
        }
        let (old_admin, old_admin_connection) = admin(&source).await;
        if planned_restart.is_some() {
            assert!(planned);
            source
                .runtime
                .primary_replicator()
                .await
                .unwrap()
                .wait_for_catch_up_quorum(kuberic_runtime::replicator::ReplicaSetQuorumMode::All)
                .await
                .unwrap();
        }
        if planned_restart == Some(PlannedRestart::SourcePreFence) {
            reopen_planned_host(source, &other, PlannedRestart::SourcePreFence).await;
            write_rejected(&sql, "INSERT INTO recovered VALUES(99)").await;
            disconnected(&sql).await;
            write_rejected(&old_admin, "INSERT INTO recovered VALUES(98)").await;
            disconnected(&old_admin).await;
            assert!(
                target
                    .application
                    .instance()
                    .connect_application()
                    .await
                    .is_err()
            );
            return;
        }
        if let Some(stage) = policy_cut {
            postgres_replicated::access::PgAccessController::new(source.application.instance())
                .close_external()
                .await
                .unwrap();
            let gate = source.application.native_driver().pause_recovery(stage);
            {
                let grant = source.effect(RuntimeEffectAction::SetAccessStatus {
                    read: AccessStatus::Granted,
                    write: AccessStatus::Granted,
                });
                tokio::pin!(grant);
                tokio::select! {
                    result = &mut grant => panic!("grant finished before {stage:?}: {result:?}"),
                    entered = tokio::time::timeout(Duration::from_secs(10), gate.entered.notified()) => entered.unwrap(),
                }
            }
            gate.release.notify_one();
            assert!(
                source
                    .application
                    .instance()
                    .connect_application()
                    .await
                    .is_err()
            );
        }
        let mut current = native_configuration(
            &[
                target.identity.clone(),
                source.identity.clone(),
                other.identity.clone(),
            ],
            0,
            previous.epoch.configuration_number + 1,
        );
        let handoff = if planned {
            source
                .effect(RuntimeEffectAction::PrepareSwitchover {
                    preparation_generation: 1,
                    request_id: SwitchoverRequestId::new("handoff"),
                    source: source.identity.clone(),
                    target: target.identity.clone(),
                    starting_configuration_id: previous.configuration_id.clone(),
                    starting_epoch: previous.epoch,
                })
                .await
                .unwrap();
            let handoff = source.store.load_state().await.unwrap().prepared_switchover;
            source
                .effect(RuntimeEffectAction::AdmitAuthority(Box::new(
                    AdmittedAuthority {
                        local_identity: source.identity.clone(),
                        previous_configuration: Some(previous.clone()),
                        current_configuration: current.clone(),
                        transition_kind: Some(TransitionKind::PlannedSwitchover),
                        switchover_handoff: handoff.clone(),
                        scale_up: None,
                        secondary_removal: None,
                    },
                )))
                .await
                .unwrap();
            if planned_restart == Some(PlannedRestart::SourceAuthorityInstalled) {
                reopen_planned_host(source, &other, PlannedRestart::SourceAuthorityInstalled).await;
                write_rejected(&sql, "INSERT INTO recovered VALUES(99)").await;
                disconnected(&sql).await;
                write_rejected(&old_admin, "INSERT INTO recovered VALUES(98)").await;
                disconnected(&old_admin).await;
                rows_absent(&other, &[98, 99]).await;
                assert!(
                    target
                        .application
                        .instance()
                        .connect_application()
                        .await
                        .is_err()
                );
                return;
            }
            if let Some(
                point @ (PlannedRestart::SourceFenceIntent | PlannedRestart::SourceFenced),
            ) = planned_restart
            {
                let stage = if point == PlannedRestart::SourceFenceIntent {
                    RecoveryStage::SourceFenceIntent
                } else {
                    RecoveryStage::SourceStopped
                };
                cancel_role_at(&source, ReplicaRole::ActiveSecondary, stage).await;
                reopen_planned_host(source, &other, point).await;
                write_rejected(&sql, "INSERT INTO recovered VALUES(99)").await;
                disconnected(&sql).await;
                write_rejected(&old_admin, "INSERT INTO recovered VALUES(98)").await;
                disconnected(&old_admin).await;
                rows_absent(&other, &[98, 99]).await;
                assert!(
                    target
                        .application
                        .instance()
                        .connect_application()
                        .await
                        .is_err()
                );
                return;
            }
            if let Some(stage @ (RecoveryStage::SourceFenceIntent | RecoveryStage::SourceStopped)) =
                cut
            {
                cancel_role_at(&source, ReplicaRole::ActiveSecondary, stage).await;
            }
            if matches!(failure, Some(Failure::Shutdown)) {
                let processes = postgres_replicated::testing::ProcessProbe::postgres(
                    source.application.instance().data_dir(),
                );
                processes.signal(rustix::process::Signal::QUIT);
                processes.assert_exited().await;
                assert!(
                    source
                        .effect(RuntimeEffectAction::ChangeRole(
                            ReplicaRole::ActiveSecondary
                        ))
                        .await
                        .is_err()
                );
                assert!(!source.application.instance().is_running().await);
                assert_eq!(
                    source.runtime.partition_report().await.reported_fault,
                    Some(kuberic_runtime::protocol::types::FaultType::Permanent)
                );
                write_rejected(&sql, "INSERT INTO recovered VALUES(99)").await;
                disconnected(&sql).await;
                write_rejected(&old_admin, "INSERT INTO recovered VALUES(98)").await;
                disconnected(&old_admin).await;
                assert!(
                    target
                        .application
                        .instance()
                        .connect_application()
                        .await
                        .is_err()
                );
                return;
            }
            source
                .effect(RuntimeEffectAction::ChangeRole(
                    ReplicaRole::ActiveSecondary,
                ))
                .await
                .unwrap();
            assert!(!source.application.instance().is_running().await);
            if planned_restart == Some(PlannedRestart::SourceDemoted) {
                reopen_planned_host(source, &other, PlannedRestart::SourceDemoted).await;
                write_rejected(&sql, "INSERT INTO recovered VALUES(99)").await;
                disconnected(&sql).await;
                write_rejected(&old_admin, "INSERT INTO recovered VALUES(98)").await;
                disconnected(&old_admin).await;
                rows_absent(&other, &[98, 99]).await;
                assert!(
                    target
                        .application
                        .instance()
                        .connect_application()
                        .await
                        .is_err()
                );
                return;
            }
            handoff
        } else {
            if explicit_demote {
                source
                    .effect(RuntimeEffectAction::ChangeRole(ReplicaRole::None))
                    .await
                    .unwrap();
            } else if !live_gap {
                source.application.instance().stop().await.unwrap();
            }
            None
        };
        if !live_gap {
            write_rejected(&sql, "INSERT INTO recovered VALUES(99)").await;
            disconnected(&sql).await;
        }
        let transition = if planned {
            TransitionKind::PlannedSwitchover
        } else {
            TransitionKind::Failover
        };
        if joint_gap {
            previous = accepted_previous;
        }
        if alternate_candidate {
            let provisional = native_configuration(
                &[
                    other.identity.clone(),
                    source.identity.clone(),
                    target.identity.clone(),
                ],
                0,
                current.epoch.configuration_number,
            );
            for pod in [&target, &other] {
                pod.effect(RuntimeEffectAction::AdmitAuthority(Box::new(
                    AdmittedAuthority {
                        local_identity: pod.identity.clone(),
                        previous_configuration: Some(previous.clone()),
                        current_configuration: provisional.clone(),
                        transition_kind: Some(TransitionKind::Failover),
                        switchover_handoff: None,
                        scale_up: None,
                        secondary_removal: None,
                    },
                )))
                .await
                .unwrap();
            }
            let rejected = other
                .runtime
                .primary_replicator()
                .await
                .unwrap()
                .change_role(provisional.epoch, ReplicaRole::Primary)
                .await;
            assert!(matches!(
                rejected,
                Err(kuberic_runtime::RuntimeError::ReconfigurationPending)
            ));
            assert!(
                other
                    .application
                    .instance()
                    .connect_application()
                    .await
                    .is_err()
            );
            previous = provisional;
            current = native_configuration(
                &[
                    target.identity.clone(),
                    source.identity.clone(),
                    other.identity.clone(),
                ],
                0,
                previous.epoch.configuration_number + 1,
            );
        }
        other
            .effect(RuntimeEffectAction::AdmitAuthority(Box::new(
                AdmittedAuthority {
                    local_identity: other.identity.clone(),
                    previous_configuration: Some(previous.clone()),
                    current_configuration: current.clone(),
                    transition_kind: Some(transition),
                    switchover_handoff: handoff.clone(),
                    scale_up: None,
                    secondary_removal: None,
                },
            )))
            .await
            .unwrap();
        target
            .effect(RuntimeEffectAction::AdmitAuthority(Box::new(
                AdmittedAuthority {
                    local_identity: target.identity.clone(),
                    previous_configuration: Some(previous),
                    current_configuration: current,
                    transition_kind: Some(transition),
                    switchover_handoff: handoff,
                    scale_up: None,
                    secondary_removal: None,
                },
            )))
            .await
            .unwrap();
        if planned_restart == Some(PlannedRestart::TargetAuthorityInstalled) {
            reopen_planned_host(target, &other, PlannedRestart::TargetAuthorityInstalled).await;
            write_rejected(&old_admin, "INSERT INTO recovered VALUES(98)").await;
            disconnected(&old_admin).await;
            rows_absent(&other, &[98, 99]).await;
            assert!(!source.application.instance().is_running().await);
            return;
        }
        if planned_restart == Some(PlannedRestart::TargetPromotion) {
            cancel_role_at(&target, ReplicaRole::Primary, RecoveryStage::Promoted).await;
            reopen_planned_host(target, &other, PlannedRestart::TargetPromotion).await;
            write_rejected(&old_admin, "INSERT INTO recovered VALUES(98)").await;
            disconnected(&old_admin).await;
            rows_absent(&other, &[98, 99]).await;
            assert!(!source.application.instance().is_running().await);
            return;
        }
        if let Some(stage) = cut.filter(|stage| {
            !matches!(
                stage,
                RecoveryStage::SourceFenceIntent | RecoveryStage::SourceStopped
            )
        }) {
            cancel_role_at(&target, ReplicaRole::Primary, stage).await;
            if reopen_cut {
                let saved = target.application.native_driver().durable_state().await;
                target.runtime.abort();
                drop(target);
                let service = std::sync::Arc::new(
                    postgres_replicated::PgService::deferred(
                        postgres_replicated::PgServiceConfig {
                            resource_uid: saved.identity.resource_uid.clone(),
                            application_root: root.path().join("t/application"),
                            pg_data: root.path().join("t/pgdata"),
                            pg_bin: postgres_replicated::testing::find_pg_bin(),
                            pg_port: postgres_replicated::testing::allocate_port().await,
                            replication_address: "http://127.0.0.1:1".into(),
                        },
                    )
                    .with_coordination_token("restart-test".into()),
                );
                let store = std::sync::Arc::new(
                    kuberic_runtime::testing::sqlite_store::SqliteStore::open_existing(
                        kuberic_runtime::testing::sqlite_store::SqliteStore::metadata_database_path(
                            &root.path().join("t"),
                        ),
                        None,
                    )
                    .unwrap(),
                );
                let runtime = kuberic_runtime::testing::hosting::PodRuntime::new(
                    saved.identity.replica.clone(),
                    service.clone(),
                    store.clone(),
                );
                runtime
                    .bind_replica_session(
                        saved.identity.resource_uid.clone(),
                        kuberic_runtime::protocol::types::ProcessSessionId::new("reopened-session"),
                    )
                    .unwrap();
                let result = runtime
                    .reconstruct(
                        kuberic_runtime::application::OpenMode::Existing,
                        ReplicaRole::Primary,
                        AccessStatus::ReconfigurationPending,
                        AccessStatus::ReconfigurationPending,
                        Some((ReplicaRole::Primary, false, false)),
                    )
                    .await;
                if stage != RecoveryStage::Ready {
                    assert!(
                        result.is_err(),
                        "incomplete election must not reuse a retired session: {stage:?}"
                    );
                }
                assert!(service.instance().connect_application().await.is_err());
                assert_ne!(
                    runtime.partition_report().await.write_status,
                    AccessStatus::Granted
                );
                assert!(store.load_state().await.unwrap().pending_effect.is_some());
                if result.is_ok() {
                    let sequence = store.load_state().await.unwrap().next_effect_sequence;
                    let grant = runtime
                        .apply_effect(kuberic_runtime::testing::effects::RuntimeEffect {
                            operation_id: kuberic_runtime::protocol::types::OperationId::new(
                                "reopened-grant",
                            ),
                            sequence,
                            action: RuntimeEffectAction::SetAccessStatus {
                                read: AccessStatus::Granted,
                                write: AccessStatus::Granted,
                            },
                        })
                        .await;
                    assert!(grant.is_err());
                    assert!(service.instance().connect_application().await.is_err());
                }
                runtime.abort();
                write_rejected(&old_admin, "INSERT INTO recovered VALUES(98)").await;
                disconnected(&old_admin).await;
                rows_absent(&other, &[98, 99]).await;
                drop(old_admin);
                let _ = old_admin_connection.await;
                return;
            }
        }
        if changed_session {
            target
                .effect(RuntimeEffectAction::RegisterPeerSession {
                    identity: other.identity.clone(),
                    session: kuberic_runtime::protocol::types::ProcessSessionId::new(
                        "replaced-responder",
                    ),
                })
                .await
                .unwrap();
        }
        if matches!(failure, Some(Failure::Observation)) {
            std::fs::write(&fault_marker, b"").unwrap();
        }
        if quorum_loss {
            other.application.instance().stop().await.unwrap();
        }
        let activated = if live_gap {
            let initial = target
                .application
                .native_driver()
                .pause_recovery(RecoveryStage::InitialRound);
            let role = target.effect(RuntimeEffectAction::ChangeRole(ReplicaRole::Primary));
            tokio::pin!(role);
            tokio::select! {
                result = &mut role => panic!("election passed initial cut: {result:?}"),
                entered = tokio::time::timeout(Duration::from_secs(10), initial.entered.notified()) => entered.unwrap(),
            }
            tokio::time::timeout(
                Duration::from_secs(10),
                sql.batch_execute("INSERT INTO recovered VALUES(3)"),
            )
            .await
            .unwrap()
            .unwrap();
            for pod in [&target, &other] {
                request_replay_pause(pod).await;
            }
            let accepted_unknown = tokio::time::timeout(
                Duration::from_millis(250),
                sql.batch_execute("INSERT INTO recovered VALUES(4)"),
            )
            .await;
            assert!(
                accepted_unknown.is_err(),
                "already accepted work must wait for synchronous replay"
            );
            let drained = target
                .application
                .native_driver()
                .pause_recovery(RecoveryStage::ReceiversDrained);
            initial.release.notify_one();
            tokio::select! {
                result = &mut role => panic!("election passed drain cut: {result:?}"),
                entered = tokio::time::timeout(Duration::from_secs(15), drained.entered.notified()) => entered.unwrap(),
            }
            let (late, _) = source
                .application
                .instance()
                .connect_application()
                .await
                .unwrap();
            let unknown = tokio::time::timeout(
                Duration::from_millis(250),
                late.batch_execute("INSERT INTO recovered VALUES(5)"),
            )
            .await;
            assert!(
                unknown.is_err(),
                "no supported synchronous commit may finish after receiver drain"
            );
            source.application.instance().stop().await.unwrap();
            write_rejected(&sql, "INSERT INTO recovered VALUES(99)").await;
            disconnected(&sql).await;
            drained.release.notify_one();
            role.await
        } else {
            target
                .effect(RuntimeEffectAction::ChangeRole(ReplicaRole::Primary))
                .await
        };
        write_rejected(&old_admin, "INSERT INTO recovered VALUES(98)").await;
        disconnected(&old_admin).await;
        drop(old_admin);
        let _ = old_admin_connection.await.unwrap();
        if failure.is_some()
            || changed_session
            || quorum_loss
            || policy_cut.is_some_and(|stage| stage != RecoveryStage::PolicyAccepted)
        {
            assert!(activated.is_err());
            assert!(
                target
                    .application
                    .instance()
                    .connect_application()
                    .await
                    .is_err()
            );
            assert_ne!(
                target.runtime.snapshot().await.write_status,
                AccessStatus::Granted
            );
            if let Some(failure) = failure {
                let expected = match failure {
                    Failure::Promotion => kuberic_runtime::protocol::types::FaultType::Permanent,
                    Failure::Observation => kuberic_runtime::protocol::types::FaultType::Permanent,
                    Failure::Shutdown => kuberic_runtime::protocol::types::FaultType::Permanent,
                };
                assert_eq!(
                    target.runtime.partition_report().await.reported_fault,
                    Some(expected)
                );
            }
            return;
        }
        activated.unwrap();
        if planned_restart == Some(PlannedRestart::TargetActivated) {
            reopen_planned_host(target, &other, PlannedRestart::TargetActivated).await;
            assert!(!source.application.instance().is_running().await);
            return;
        }
        assert!(
            target
                .application
                .instance()
                .connect_application()
                .await
                .is_err()
        );
        let (admin, admin_connection) = admin(&target).await;
        assert_eq!(
            admin
                .query_one("SELECT count(*) FROM recovered WHERE id=1", &[])
                .await
                .unwrap()
                .get::<_, i64>(0),
            1
        );
        assert_eq!(
            admin
                .query_one("SELECT count(*) FROM recovered WHERE id=99", &[])
                .await
                .unwrap()
                .get::<_, i64>(0),
            0
        );
        assert_eq!(
            admin
                .query_one("SELECT count(*) FROM recovered WHERE id=98", &[])
                .await
                .unwrap()
                .get::<_, i64>(0),
            0
        );
        if live_gap {
            assert_eq!(
                admin
                    .query_one("SELECT count(*) FROM recovered WHERE id=3", &[])
                    .await
                    .unwrap()
                    .get::<_, i64>(0),
                1
            );
        }
        target
            .effect(RuntimeEffectAction::SetAccessStatus {
                read: AccessStatus::Granted,
                write: AccessStatus::Granted,
            })
            .await
            .unwrap();
        let (sql, _) = target
            .application
            .instance()
            .connect_application()
            .await
            .unwrap();
        sql.batch_execute("INSERT INTO recovered VALUES(2)")
            .await
            .unwrap();
        if planned_restart == Some(PlannedRestart::TargetGranted) {
            reopen_planned_host(target, &other, PlannedRestart::TargetGranted).await;
            write_rejected(&sql, "INSERT INTO recovered VALUES(99)").await;
            disconnected(&sql).await;
            write_rejected(&admin, "INSERT INTO recovered VALUES(98)").await;
            disconnected(&admin).await;
            assert!(!source.application.instance().is_running().await);
            return;
        }
        if alternate_candidate {
            other.refresh().await;
            assert_eq!(other.runtime.partition_report().await.reported_fault, None);
        }
        if planned && cut.is_none() {
            other.application.instance().stop().await.unwrap();
            assert!(
                target
                    .effect(RuntimeEffectAction::RefreshApplicationProgress)
                    .await
                    .is_err()
            );
            assert_ne!(
                target.runtime.partition_report().await.write_status,
                AccessStatus::Granted
            );
            write_rejected(&sql, "INSERT INTO recovered VALUES(77)").await;
            disconnected(&sql).await;
            write_rejected(&admin, "INSERT INTO recovered VALUES(78)").await;
            disconnected(&admin).await;
            let (check, task) = self::admin(&target).await;
            assert_eq!(
                check
                    .query_one("SELECT count(*) FROM recovered WHERE id IN (77,78)", &[])
                    .await
                    .unwrap()
                    .get::<_, i64>(0),
                0
            );
            assert_eq!(
                check
                    .query_one("SELECT count(*) FROM recovered WHERE id=2", &[])
                    .await
                    .unwrap()
                    .get::<_, i64>(0),
                1
            );
            drop(check);
            task.await.unwrap().unwrap();
        }
        drop(admin);
        let _ = admin_connection.await.unwrap();
        if explicit_demote {
            let reopened = source.reopen().await;
            assert!(!reopened.application.instance().is_running().await);
            assert!(
                reopened
                    .application
                    .instance()
                    .connect_application()
                    .await
                    .is_err()
            );
        }
    }

    async fn admin(
        pod: &PgPod,
    ) -> (
        tokio_postgres::Client,
        tokio::task::JoinHandle<std::result::Result<(), tokio_postgres::Error>>,
    ) {
        let (client, connection) = tokio_postgres::connect(
            &pod.application
                .instance()
                .connection_string()
                .replace("dbname=postgres", "dbname=kuberic"),
            tokio_postgres::NoTls,
        )
        .await
        .unwrap();
        (client, tokio::spawn(connection))
    }

    struct ReopenedHost {
        runtime: std::sync::Arc<kuberic_runtime::testing::hosting::PodRuntime>,
        server: tokio::task::JoinHandle<()>,
    }

    impl Drop for ReopenedHost {
        fn drop(&mut self) {
            self.runtime.abort();
            self.server.abort();
        }
    }

    async fn reopen_planned_host(pod: PgPod, survivor: &PgPod, point: PlannedRestart) {
        use kuberic_runtime::protocol::types::{FaultType, OperationId};
        use kuberic_runtime::testing::effects::RuntimeEffect;
        use kuberic_runtime::testing::{
            runtime_adapter::RuntimeAdapter, service::AgentService, sqlite_store::SqliteStore,
        };
        use postgres_replicated::testing::{ProcessProbe, allocate_port, find_pg_bin};
        use postgres_replicated::{PgService, PgServiceConfig, data_service::PgDataServiceImpl};
        use std::sync::Arc;

        let before = pod.store.load_state().await.unwrap();
        let snapshot = pod.runtime.snapshot().await;
        let metadata = pod.application.native_driver().durable_state().await;
        let encoded = serde_json::to_value(&metadata).unwrap();
        assert_eq!(metadata.identity.replica, snapshot.identity);
        let granted = matches!(
            point,
            PlannedRestart::SourcePreFence | PlannedRestart::TargetGranted
        );
        assert_eq!(
            snapshot.write_status == AccessStatus::Granted,
            granted,
            "{point:?}"
        );
        match point {
            PlannedRestart::SourcePreFence => assert!(before.prepared_switchover.is_none()),
            PlannedRestart::SourceAuthorityInstalled => {
                assert!(before.prepared_switchover.is_some());
                assert!(matches!(
                    before.retained_result.as_ref().unwrap().effect.action,
                    RuntimeEffectAction::AdmitAuthority(_)
                ));
            }
            PlannedRestart::SourceFenceIntent | PlannedRestart::SourceFenced => {
                assert!(matches!(
                    before.pending_effect.as_ref().unwrap().effect.action,
                    RuntimeEffectAction::ChangeRole(ReplicaRole::ActiveSecondary)
                ));
                assert_eq!(
                    encoded["recovery"]["source_fence"]["stopped"],
                    point == PlannedRestart::SourceFenced
                );
            }
            PlannedRestart::SourceDemoted => {
                assert_eq!(before.role, ReplicaRole::ActiveSecondary);
                assert!(before.pending_effect.is_none());
                assert_eq!(encoded["recovery"]["source_fence"]["stopped"], true);
                assert!(!pod.application.instance().is_running().await);
            }
            PlannedRestart::TargetAuthorityInstalled => {
                assert_eq!(before.role, ReplicaRole::ActiveSecondary);
                assert!(matches!(
                    before.retained_result.as_ref().unwrap().effect.action,
                    RuntimeEffectAction::AdmitAuthority(_)
                ));
                assert!(
                    snapshot
                        .authority
                        .as_ref()
                        .unwrap()
                        .switchover_handoff
                        .is_some()
                );
            }
            PlannedRestart::TargetPromotion => {
                assert!(matches!(
                    before.pending_effect.as_ref().unwrap().effect.action,
                    RuntimeEffectAction::ChangeRole(ReplicaRole::Primary)
                ));
                let (sql, _) = pod.application.instance().connect().await.unwrap();
                assert!(
                    !sql.query_one("SELECT pg_is_in_recovery()", &[])
                        .await
                        .unwrap()
                        .get::<_, bool>(0)
                );
                assert_eq!(encoded["recovery"]["source_fence"]["stopped"], true);
            }
            PlannedRestart::TargetActivated | PlannedRestart::TargetGranted => {
                assert_eq!(before.role, ReplicaRole::Primary);
                assert!(before.pending_effect.is_none());
                assert_eq!(encoded["recovery"]["pending"]["ready"], true);
                assert_eq!(encoded["recovery"]["source_fence"]["stopped"], true);
            }
        }
        let ordinary = if granted {
            Some(
                pod.application
                    .instance()
                    .connect_application()
                    .await
                    .unwrap(),
            )
        } else {
            assert!(
                pod.application
                    .instance()
                    .connect_application()
                    .await
                    .is_err(),
                "{point:?}"
            );
            None
        };
        let running = pod.application.instance().is_running().await;
        let administrative = if running {
            Some(admin(&pod).await)
        } else {
            None
        };
        let processes =
            running.then(|| ProcessProbe::postgres(pod.application.instance().data_dir()));
        let root = pod.root.clone();
        let old_session = pod.session.clone();
        let old_service = Arc::downgrade(&pod.application);
        pod.runtime.abort();
        drop(pod);
        if let Some(processes) = processes {
            processes.assert_reaped();
        }
        if let Some((sql, task)) = ordinary {
            write_rejected(&sql, "INSERT INTO recovered VALUES(9101)").await;
            disconnected(&sql).await;
            drop(sql);
            let _ = task.await;
        }
        if let Some((sql, task)) = administrative {
            write_rejected(&sql, "INSERT INTO recovered VALUES(9102)").await;
            disconnected(&sql).await;
            drop(sql);
            let _ = task.await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while old_service.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("old service and metadata owner must be gone before reopen");

        let store = Arc::new(
            SqliteStore::open_existing(SqliteStore::metadata_database_path(&root), None).unwrap(),
        );
        assert_eq!(store.load_state().await.unwrap(), before);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let token = "host-local-native-build";
        let application = Arc::new(
            PgService::deferred(PgServiceConfig {
                resource_uid: metadata.identity.resource_uid.clone(),
                application_root: root.join("application"),
                pg_data: root.join("pgdata"),
                pg_bin: find_pg_bin(),
                pg_port: allocate_port().await,
                replication_address: endpoint,
            })
            .with_coordination_token(token.into()),
        );
        let runtime = Arc::new(kuberic_runtime::testing::hosting::PodRuntime::new(
            metadata.identity.replica.clone(),
            application.clone(),
            store.clone(),
        ));
        let agent =
            AgentService::new(store.clone(), runtime.clone(), runtime.clone(), token).unwrap();
        assert_ne!(agent.sessions().local_session(), &old_session);
        let data = PgDataServiceImpl::new(application.clone(), token.into());
        let host = ReopenedHost {
            runtime: runtime.clone(),
            server: tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(data.into_server())
                    .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                    .await
                    .unwrap();
            }),
        };
        let reconstructed =
            tokio::time::timeout(Duration::from_secs(15), agent.reconstruct_runtime())
                .await
                .expect("planned restart must finish or explicitly remain fenced");
        let observed = runtime.snapshot().await;
        assert!(
            observed.open,
            "{point:?}: valid storage must open, not fail through a broken fixture: {reconstructed:?}"
        );
        assert_eq!(observed.authority, snapshot.authority, "{point:?}");
        assert_ne!(
            runtime.partition_report().await.reported_fault,
            Some(FaultType::Permanent),
            "{point:?}: {reconstructed:?}"
        );
        assert_eq!(
            store.load_state().await.unwrap().pending_effect,
            before.pending_effect,
            "{point:?}"
        );
        assert_ne!(
            runtime.partition_report().await.write_status,
            AccessStatus::Granted,
            "{point:?}"
        );
        assert!(
            application.instance().connect_application().await.is_err(),
            "{point:?}"
        );
        if matches!(
            point,
            PlannedRestart::SourceFenceIntent
                | PlannedRestart::SourceFenced
                | PlannedRestart::SourceDemoted
        ) {
            assert!(!application.instance().is_running().await, "{point:?}");
            let reopened =
                serde_json::to_value(application.native_driver().durable_state().await).unwrap();
            assert_eq!(reopened["recovery"]["former_primary"], true);
            assert_eq!(
                reopened["recovery"]["source_fence"]["boundary"],
                encoded["recovery"]["source_fence"]["boundary"]
            );
        }
        let replay = RuntimeAdapter::new(store.clone(), runtime.clone())
            .resume_pending()
            .await;
        assert_ne!(
            runtime.partition_report().await.write_status,
            AccessStatus::Granted,
            "{point:?}: {replay:?}"
        );
        assert!(
            application.instance().connect_application().await.is_err(),
            "{point:?}: {replay:?}"
        );
        let sequence = store.load_state().await.unwrap().next_effect_sequence;
        assert!(
            runtime
                .apply_effect(RuntimeEffect {
                    operation_id: OperationId::new("fresh-session-handoff-grant"),
                    sequence,
                    action: RuntimeEffectAction::SetAccessStatus {
                        read: AccessStatus::Granted,
                        write: AccessStatus::Granted
                    },
                })
                .await
                .is_err(),
            "{point:?}: stale-session proof must not grant fresh access"
        );
        assert!(
            application.instance().connect_application().await.is_err(),
            "{point:?}"
        );
        let (reader, task) = admin(survivor).await;
        assert_eq!(
            reader
                .query_one("SELECT count(*) FROM recovered WHERE id=1", &[])
                .await
                .unwrap()
                .get::<_, i64>(0),
            1,
            "{point:?}"
        );
        if point == PlannedRestart::TargetGranted {
            assert_eq!(
                reader
                    .query_one("SELECT count(*) FROM recovered WHERE id=2", &[])
                    .await
                    .unwrap()
                    .get::<_, i64>(0),
                1
            );
        }
        assert_eq!(
            reader
                .query_one(
                    "SELECT count(*) FROM recovered WHERE id IN (9101,9102)",
                    &[]
                )
                .await
                .unwrap()
                .get::<_, i64>(0),
            0,
            "{point:?}"
        );
        drop(reader);
        task.await.unwrap().unwrap();
        drop(host);
    }

    async fn request_replay_pause(pod: &PgPod) {
        let (client, task) = pod.application.instance().connect().await.unwrap();
        client
            .simple_query("SELECT pg_wal_replay_pause()")
            .await
            .unwrap();
        assert!(
            client
                .query_one("SELECT pg_is_wal_replay_paused()", &[])
                .await
                .unwrap()
                .get::<_, bool>(0)
        );
        drop(client);
        task.await.unwrap();
    }

    async fn cancel_role_at(pod: &PgPod, role: ReplicaRole, stage: RecoveryStage) {
        let gate = pod.application.native_driver().pause_recovery(stage);
        {
            let change = pod.effect(RuntimeEffectAction::ChangeRole(role));
            tokio::pin!(change);
            tokio::select! {
                result = &mut change => panic!("role finished before {stage:?}: {result:?}"),
                entered = tokio::time::timeout(Duration::from_secs(30), gate.entered.notified()) => entered.unwrap(),
            }
        }
        gate.release.notify_one();
        assert!(
            pod.store
                .load_state()
                .await
                .unwrap()
                .pending_effect
                .is_some()
        );
        assert_ne!(
            pod.runtime.partition_report().await.write_status,
            AccessStatus::Granted
        );
        assert!(
            pod.application
                .instance()
                .connect_application()
                .await
                .is_err()
        );
    }

    pub(super) async fn acknowledged_row_is_replayed_before_candidate_primary_callback_completes() {
        exercise_primary_change(Case::default()).await;
    }

    pub(super) async fn planned_switchover_requires_source_shutdown_before_target_writes() {
        exercise_primary_change(Case {
            planned: true,
            ..Default::default()
        })
        .await;
    }

    async fn planned_restart(point: PlannedRestart) {
        exercise_primary_change(Case {
            planned: true,
            planned_restart: Some(point),
            ..Default::default()
        })
        .await;
    }

    async fn cancelled_election(stage: RecoveryStage, reopen_cut: bool) {
        exercise_primary_change(Case {
            cut: Some(stage),
            reopen_cut,
            ..Default::default()
        })
        .await;
    }

    macro_rules! primary_change_scenario {
        ($name:ident, $body:expr) => {
            pub(super) async fn $name() {
                $body.await;
            }
        };
    }

    primary_change_scenario!(
        planned_source_restart_before_fence,
        planned_restart(PlannedRestart::SourcePreFence)
    );
    primary_change_scenario!(
        planned_source_restart_after_authority_install,
        planned_restart(PlannedRestart::SourceAuthorityInstalled)
    );
    primary_change_scenario!(
        planned_source_restart_after_fence_intent,
        planned_restart(PlannedRestart::SourceFenceIntent)
    );
    primary_change_scenario!(
        planned_source_restart_after_fence,
        planned_restart(PlannedRestart::SourceFenced)
    );
    primary_change_scenario!(
        planned_source_restart_after_demotion,
        planned_restart(PlannedRestart::SourceDemoted)
    );
    primary_change_scenario!(
        planned_target_restart_after_authority_install,
        planned_restart(PlannedRestart::TargetAuthorityInstalled)
    );
    primary_change_scenario!(
        planned_target_restart_during_promotion,
        planned_restart(PlannedRestart::TargetPromotion)
    );
    primary_change_scenario!(
        planned_target_restart_after_activation,
        planned_restart(PlannedRestart::TargetActivated)
    );
    primary_change_scenario!(
        planned_target_restart_after_access_grant,
        planned_restart(PlannedRestart::TargetGranted)
    );
    primary_change_scenario!(
        cancelled_election_resumes_initial_round,
        cancelled_election(RecoveryStage::InitialRound, false)
    );
    primary_change_scenario!(
        cancelled_election_resumes_receiver_drain,
        cancelled_election(RecoveryStage::ReceiversDrained, false)
    );
    primary_change_scenario!(
        cancelled_election_resumes_final_round,
        cancelled_election(RecoveryStage::FinalRound, false)
    );
    primary_change_scenario!(
        cancelled_election_resumes_promotion,
        cancelled_election(RecoveryStage::Promoted, false)
    );
    primary_change_scenario!(
        cancelled_election_resumes_ready_publication,
        cancelled_election(RecoveryStage::Ready, false)
    );
    primary_change_scenario!(
        fresh_session_reopen_at_initial_round_remains_write_closed,
        cancelled_election(RecoveryStage::InitialRound, true)
    );
    primary_change_scenario!(
        fresh_session_reopen_at_receiver_drain_remains_write_closed,
        cancelled_election(RecoveryStage::ReceiversDrained, true)
    );
    primary_change_scenario!(
        fresh_session_reopen_at_final_round_remains_write_closed,
        cancelled_election(RecoveryStage::FinalRound, true)
    );
    primary_change_scenario!(
        fresh_session_reopen_at_promotion_remains_write_closed,
        cancelled_election(RecoveryStage::Promoted, true)
    );
    primary_change_scenario!(
        fresh_session_reopen_at_ready_publication_remains_write_closed,
        cancelled_election(RecoveryStage::Ready, true)
    );
    primary_change_scenario!(
        cancelled_source_demotion_resumes_fence_intent,
        exercise_primary_change(Case {
            planned: true,
            cut: Some(RecoveryStage::SourceFenceIntent),
            ..Default::default()
        })
    );
    primary_change_scenario!(
        cancelled_source_demotion_resumes_stopped_receipt,
        exercise_primary_change(Case {
            planned: true,
            cut: Some(RecoveryStage::SourceStopped),
            ..Default::default()
        })
    );
    primary_change_scenario!(
        interrupted_policy_invalidation_stays_write_closed,
        exercise_primary_change(Case {
            policy_cut: Some(RecoveryStage::PolicyInvalidated),
            ..Default::default()
        })
    );
    primary_change_scenario!(
        interrupted_policy_application_stays_write_closed,
        exercise_primary_change(Case {
            policy_cut: Some(RecoveryStage::PolicyApplied),
            ..Default::default()
        })
    );
    primary_change_scenario!(
        interrupted_policy_acceptance_resumes_matched_generation,
        exercise_primary_change(Case {
            policy_cut: Some(RecoveryStage::PolicyAccepted),
            ..Default::default()
        })
    );
    primary_change_scenario!(
        promotion_failure_stays_closed_with_permanent_fault,
        exercise_primary_change(Case {
            failure: Some(Failure::Promotion),
            ..Default::default()
        })
    );
    primary_change_scenario!(
        observation_failure_stays_closed_with_permanent_fault,
        exercise_primary_change(Case {
            failure: Some(Failure::Observation),
            ..Default::default()
        })
    );
    primary_change_scenario!(
        planned_shutdown_failure_stays_closed_with_permanent_fault,
        exercise_primary_change(Case {
            planned: true,
            failure: Some(Failure::Shutdown),
            ..Default::default()
        })
    );

    pub(super) async fn final_round_includes_between_round_commit_and_draining_revokes_old_acknowledgements()
     {
        exercise_primary_change(Case {
            live_gap: true,
            ..Default::default()
        })
        .await;
    }

    pub(super) async fn insufficient_exact_responders_keep_the_sf_role_and_partition_write_closed()
    {
        exercise_primary_change(Case {
            quorum_loss: true,
            ..Default::default()
        })
        .await;
    }

    pub(super) async fn stale_required_responder_session_cannot_activate_candidate() {
        exercise_primary_change(Case {
            changed_session: true,
            ..Default::default()
        })
        .await;
    }

    pub(super) async fn pc_cc_policy_change_and_report_gap_preserve_the_between_round_acknowledgement()
     {
        exercise_primary_change(Case {
            joint_gap: true,
            live_gap: true,
            ..Default::default()
        })
        .await;
    }

    pub(super) async fn explicit_demotion_fences_retained_clients_and_former_primary_restart() {
        exercise_primary_change(Case {
            explicit_demote: true,
            ..Default::default()
        })
        .await;
    }

    pub(super) async fn absent_accepted_policy_cannot_activate_a_durably_built_candidate() {
        let root = TestDataDir::new("p5-missing");
        let source = PgPod::new(root.path().join("s"), native_identity(1, "source")).await;
        let target = PgPod::new(root.path().join("t"), native_identity(2, "target")).await;
        source.singleton().await;
        let build = source.authorize(&target, "missing-policy").await;
        source.build(&target, &build).await.unwrap();
        let previous =
            native_configuration(&[source.identity.clone(), target.identity.clone()], 0, 2);
        target.admit(previous.clone()).await;
        target
            .effect(RuntimeEffectAction::ChangeRole(
                ReplicaRole::ActiveSecondary,
            ))
            .await
            .unwrap();
        source.admit(previous.clone()).await;
        source.application.instance().stop().await.unwrap();
        let current =
            native_configuration(&[target.identity.clone(), source.identity.clone()], 0, 3);
        target
            .effect(RuntimeEffectAction::AdmitAuthority(Box::new(
                AdmittedAuthority {
                    local_identity: target.identity.clone(),
                    previous_configuration: Some(previous),
                    current_configuration: current,
                    transition_kind: Some(TransitionKind::Failover),
                    switchover_handoff: None,
                    scale_up: None,
                    secondary_removal: None,
                },
            )))
            .await
            .unwrap();
        let error = target
            .effect(RuntimeEffectAction::ChangeRole(ReplicaRole::Primary))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("missing accepted PostgreSQL synchronous policy"),
            "{error}"
        );
        assert_ne!(
            target.runtime.partition_report().await.write_status,
            AccessStatus::Granted
        );
        assert!(
            target
                .application
                .instance()
                .connect_application()
                .await
                .is_err()
        );
        let (admin, _) = target.application.instance().connect().await.unwrap();
        assert!(
            admin
                .query_one("SELECT pg_is_in_recovery()", &[])
                .await
                .unwrap()
                .get::<_, bool>(0)
        );
    }

    pub(super) async fn refused_candidate_can_follow_the_exact_higher_epoch_winner_without_old_receipt_poisoning()
     {
        exercise_primary_change(Case {
            alternate_candidate: true,
            ..Default::default()
        })
        .await;
    }
}

host_test!(acknowledged_row_is_replayed_before_candidate_primary_callback_completes);
host_test!(planned_switchover_requires_source_shutdown_before_target_writes);
host_test!(planned_source_restart_before_fence);
host_test!(planned_source_restart_after_authority_install);
host_test!(planned_source_restart_after_fence_intent);
host_test!(planned_source_restart_after_fence);
host_test!(planned_source_restart_after_demotion);
host_test!(planned_target_restart_after_authority_install);
host_test!(planned_target_restart_during_promotion);
host_test!(planned_target_restart_after_activation);
host_test!(planned_target_restart_after_access_grant);
host_test!(cancelled_election_resumes_initial_round);
host_test!(cancelled_election_resumes_receiver_drain);
host_test!(cancelled_election_resumes_final_round);
host_test!(cancelled_election_resumes_promotion);
host_test!(cancelled_election_resumes_ready_publication);
host_test!(cancelled_source_demotion_resumes_fence_intent);
host_test!(cancelled_source_demotion_resumes_stopped_receipt);
host_test!(interrupted_policy_invalidation_stays_write_closed);
host_test!(interrupted_policy_application_stays_write_closed);
host_test!(interrupted_policy_acceptance_resumes_matched_generation);
host_test!(final_round_includes_between_round_commit_and_draining_revokes_old_acknowledgements);
host_test!(insufficient_exact_responders_keep_the_sf_role_and_partition_write_closed);
host_test!(stale_required_responder_session_cannot_activate_candidate);
host_test!(fresh_session_reopen_at_initial_round_remains_write_closed);
host_test!(fresh_session_reopen_at_receiver_drain_remains_write_closed);
host_test!(fresh_session_reopen_at_final_round_remains_write_closed);
host_test!(fresh_session_reopen_at_promotion_remains_write_closed);
host_test!(fresh_session_reopen_at_ready_publication_remains_write_closed);
host_test!(promotion_failure_stays_closed_with_permanent_fault);
host_test!(observation_failure_stays_closed_with_permanent_fault);
host_test!(planned_shutdown_failure_stays_closed_with_permanent_fault);
host_test!(pc_cc_policy_change_and_report_gap_preserve_the_between_round_acknowledgement);
host_test!(explicit_demotion_fences_retained_clients_and_former_primary_restart);
host_test!(absent_accepted_policy_cannot_activate_a_durably_built_candidate);
host_test!(
    refused_candidate_can_follow_the_exact_higher_epoch_winner_without_old_receipt_poisoning
);
