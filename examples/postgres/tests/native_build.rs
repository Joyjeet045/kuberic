use std::time::Duration;

use kuberic_protocol::types::AccessStatus;
use kuberic_runtime_internal::effects::RuntimeEffectAction;
use postgres_replicated::{
    build::{PgBuildMethod, PgBuildStage, PgLineage, decode, encode},
    native::PgNativeObserver,
    testing::{PgPod, TestDataDir, native_identity},
};

#[test_log::test(tokio::test)]
async fn former_primary_rejoins_by_rewind_or_explicit_fresh_fallback() {
    for fallback in [false, true] {
        let root = TestDataDir::new("nr");
        let bin = if fallback {
            postgres_replicated::testing::wrapped_pg_bin(
                root.path(),
                "pg_rewind",
                "#!/bin/sh\nprintf 'missing retained WAL for rewind' >&2\nexit 1\n",
            )
        } else {
            postgres_replicated::testing::find_pg_bin()
        };
        let former =
            PgPod::with_bin(root.path().join("f"), native_identity(1, "former"), bin).await;
        let successor = PgPod::new(root.path().join("s"), native_identity(2, "successor")).await;
        former.singleton().await;
        write_source(&former).await;
        let build = former.authorize(&successor, "initial").await;
        former.build(&successor, &build).await.unwrap();
        former
            .effect(RuntimeEffectAction::SetAccessStatus {
                read: AccessStatus::ReconfigurationPending,
                write: AccessStatus::ReconfigurationPending,
            })
            .await
            .unwrap();
        former.application.instance().stop().await.unwrap();
        let successor = successor.promoted_fixture().await;
        successor
            .admit(postgres_replicated::testing::native_configuration(
                std::slice::from_ref(&successor.identity),
                0,
                3,
            ))
            .await;
        successor
            .effect(RuntimeEffectAction::ChangeRole(
                kuberic_protocol::types::ReplicaRole::Primary,
            ))
            .await
            .unwrap();
        let authority = successor.authorize(&former, "rejoin").await;
        successor.build(&former, &authority).await.unwrap();
        assert_data(&former).await;
        let progress = former
            .application
            .native_driver()
            .durable_state()
            .await
            .native_build
            .unwrap();
        assert_eq!(
            progress.method,
            if fallback {
                PgBuildMethod::Fresh
            } else {
                PgBuildMethod::Rewind
            }
        );
        assert!(progress.request.lineage.timeline > 1);
        former.refresh().await;
        assert!(former.runtime.snapshot().await.builds[0].completed);
    }
}

#[tokio::test]
async fn agent_dispatch_admits_and_builds_through_exact_native_route() {
    let root = TestDataDir::new("nw");
    let source = PgPod::new(root.path().join("s"), native_identity(1, "source")).await;
    let mut target = PgPod::new(root.path().join("t"), native_identity(2, "target")).await;
    source.singleton().await;
    write_source(&source).await;
    use kuberic_runtime::PrimaryReplicator;
    let error = source
        .application
        .native_driver()
        .build_replica(kuberic_runtime::replicator::ReplicaInformation::new(
            kuberic_protocol::types::OperationId::new("wire-native"),
            target.identity.clone(),
            String::new(),
        ))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("missing exact target replication endpoint"),
        "{error}"
    );
    assert!(
        source
            .application
            .native_driver()
            .durable_state()
            .await
            .outbound_builds
            .is_empty()
    );
    let control = target.start_control().await;
    source
        .dispatch_native(&target, control, "wire-native")
        .await;
    target.refresh().await;
    assert_data(&target).await;
    assert_eq!(target.runtime.snapshot().await.builds.len(), 1);
}

#[tokio::test]
async fn replacement_and_retired_sessions_cannot_publish_old_completion() {
    let root = TestDataDir::new("ns");
    let source = PgPod::new(root.path().join("s"), native_identity(1, "source")).await;
    let old = PgPod::new(root.path().join("old"), native_identity(2, "old")).await;
    source.singleton().await;
    write_source(&source).await;
    let authority = source.authorize(&old, "old-build").await;
    source.build(&old, &authority).await.unwrap();
    let progress = old
        .application
        .native_driver()
        .durable_state()
        .await
        .native_build
        .unwrap();
    let replacement = PgPod::new(root.path().join("new"), native_identity(2, "replacement")).await;
    assert!(replacement.inject(&progress.request).await.is_err());
    let replacement_authority = source.authorize(&replacement, "replacement-build").await;
    source
        .build(&replacement, &replacement_authority)
        .await
        .unwrap();
    assert_data(&replacement).await;
    let old = old.reopen().await;
    old.peer(&source).await;
    assert!(old.inject(&progress.request).await.is_err());
    old.refresh().await;
    assert!(old.runtime.snapshot().await.builds.is_empty());
    source.peer(&old).await;
    source.refresh().await;
    assert!(
        !source
            .runtime
            .snapshot()
            .await
            .builds
            .iter()
            .any(|b| b.authority.build_id == authority.build_id)
    );
    let request = replacement
        .application
        .native_driver()
        .durable_state()
        .await
        .native_build
        .unwrap()
        .request;
    replacement
        .effect(RuntimeEffectAction::RetireBuild(
            replacement_authority.build_id.clone(),
        ))
        .await
        .unwrap();
    assert!(replacement.inject(&request).await.is_err());
    replacement.refresh().await;
    assert!(replacement.runtime.snapshot().await.builds.is_empty());
}

#[tokio::test]
async fn delayed_completion_cannot_publish_after_target_session_replacement() {
    let root = TestDataDir::new("ndelay");
    let source = PgPod::new(root.path().join("s"), native_identity(1, "source")).await;
    let target = PgPod::new(root.path().join("t"), native_identity(2, "target")).await;
    source.singleton().await;
    write_source(&source).await;
    let authority = source.authorize(&target, "delayed").await;
    let gate = target
        .application
        .native_driver()
        .pause_build(PgBuildStage::Complete);
    let work = source.build(&target, &authority);
    tokio::pin!(work);
    tokio::select! {
        result = &mut work => panic!("completed before publication gate: {result:?}"),
        entered = tokio::time::timeout(Duration::from_secs(30), gate.entered.notified()) => { entered.unwrap(); }
    }
    source
        .effect(RuntimeEffectAction::RegisterPeerSession {
            identity: target.identity.clone(),
            session: kuberic_protocol::types::ProcessSessionId::new("replacement-process"),
        })
        .await
        .unwrap();
    gate.release.notify_one();
    assert!(work.await.is_err());
    source.refresh().await;
    assert!(source.runtime.snapshot().await.builds.is_empty());
}

#[tokio::test]
async fn cancelling_owned_basebackup_reaps_helpers_and_rebuilds_partial_stage() {
    let root = TestDataDir::new("nh");
    let marker = root.path().join("backup.pid");
    let bin = postgres_replicated::testing::wrapped_pg_bin(
        root.path(),
        "pg_basebackup",
        &format!(
            "#!/bin/sh\nprintf '%s' $$ > '{}'\nsleep 60\n",
            marker.display()
        ),
    );
    let source = PgPod::new(root.path().join("s"), native_identity(1, "source")).await;
    let target = PgPod::with_bin(root.path().join("t"), native_identity(2, "target"), bin).await;
    source.singleton().await;
    write_source(&source).await;
    let authority = source.authorize(&target, "cancel-helper").await;
    {
        let work = source.build(&target, &authority);
        tokio::pin!(work);
        tokio::select! {
            result = &mut work => panic!("backup exited before cancellation: {result:?}"),
            _ = async {
                tokio::time::timeout(Duration::from_secs(15), async {
                    while !marker.exists() { tokio::time::sleep(Duration::from_millis(20)).await; }
                }).await.unwrap();
            } => {}
        }
        let pid = std::fs::read_to_string(&marker).unwrap().parse().unwrap();
        let helper = postgres_replicated::testing::ProcessProbe::process(pid);
        let children = postgres_replicated::testing::ProcessProbe::descendants(pid);
        assert_eq!(
            target
                .application
                .native_driver()
                .durable_state()
                .await
                .native_build
                .unwrap()
                .stage,
            PgBuildStage::Copying
        );
        target.runtime.abort();
        assert!(work.await.is_err());
        helper.assert_reaped();
        children.assert_reaped();
    }
    let target = target.reopen().await;
    let authority = source.authorize(&target, "cancel-helper").await;
    source.build(&target, &authority).await.unwrap();
    assert_data(&target).await;
}

#[tokio::test]
async fn incompatible_real_timeline_is_rejected_without_replacing_data() {
    let root = TestDataDir::new("ntli");
    let source = PgPod::new(root.path().join("s"), native_identity(1, "source")).await;
    let target = PgPod::new(root.path().join("t"), native_identity(2, "target")).await;
    source.singleton().await;
    write_source(&source).await;
    let authority = source.authorize(&target, "initial").await;
    source.build(&target, &authority).await.unwrap();
    let target = target.promoted_fixture().await;
    let before = target
        .application
        .instance()
        .control_identity()
        .await
        .unwrap();
    assert!(before.1 > 1);
    let authority = source.authorize(&target, "wrong-timeline").await;
    let error = source.build(&target, &authority).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("incompatible PostgreSQL system identity/timeline"),
        "{error}"
    );
    assert_eq!(
        target
            .application
            .instance()
            .control_identity()
            .await
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn source_restart_rebinds_streaming_without_reusing_old_sessions() {
    let root = TestDataDir::new("nsrc");
    let source = PgPod::new(root.path().join("s"), native_identity(1, "source")).await;
    let target = PgPod::new(root.path().join("t"), native_identity(2, "target")).await;
    source.singleton().await;
    write_source(&source).await;
    let authority = source.authorize(&target, "restart-source").await;
    source.build(&target, &authority).await.unwrap();
    let old_request = target
        .application
        .native_driver()
        .durable_state()
        .await
        .native_build
        .unwrap()
        .request;
    let source = source.reopen().await;
    target.peer(&source).await;
    assert!(target.inject(&old_request).await.is_err());
    source.refresh().await;
    assert!(source.runtime.snapshot().await.builds.is_empty());
    let restored = source.authorize(&target, "restart-source").await;
    assert_eq!(restored, authority);
    source.build(&target, &restored).await.unwrap();
    assert_data(&target).await;
    assert_ne!(
        old_request.source_session,
        target
            .application
            .native_driver()
            .durable_state()
            .await
            .native_build
            .unwrap()
            .request
            .source_session
    );
}

#[tokio::test]
async fn real_receive_and_flush_without_replay_do_not_complete_frozen_boundary() {
    let root = TestDataDir::new("nbound");
    let source = PgPod::new(root.path().join("s"), native_identity(1, "source")).await;
    let target = PgPod::new(root.path().join("t"), native_identity(2, "target")).await;
    source.singleton().await;
    write_source(&source).await;
    let build = source.authorize(&target, "first").await;
    source.build(&target, &build).await.unwrap();
    let (control, _) = target.application.instance().connect().await.unwrap();
    control
        .simple_query("SELECT pg_wal_replay_pause()")
        .await
        .unwrap();
    let (sql, _) = source
        .application
        .instance()
        .connect_application()
        .await
        .unwrap();
    sql.batch_execute("INSERT INTO build_receipts VALUES (3, 'frozen')")
        .await
        .unwrap();
    source.refresh().await;
    // Freeze a later boundary without replacing the target's installed build.
    // Re-admitting B would correctly revoke A and stop its old receiver.
    let authority = source
        .runtime
        .authorize_build(
            kuberic_protocol::types::OperationId::new("frozen"),
            target.identity.clone(),
            kuberic_runtime::replicator::copy::BuildConfiguration::Current,
        )
        .await
        .unwrap();
    let boundary = authority.replication_boundary_lsn;
    let observer = PgNativeObserver::new(target.application.instance().clone());
    let mut progress = target
        .application
        .native_driver()
        .durable_state()
        .await
        .native_build
        .unwrap();
    progress.request.authority = authority;
    progress.stage = PgBuildStage::Recovering;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            progress.evidence = observer.snapshot().await.unwrap().evidence;
            if progress.evidence.as_ref().unwrap().flush_lsn >= boundary {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(progress.evidence.as_ref().unwrap().received_lsn.unwrap() >= boundary);
    assert!(progress.evidence.as_ref().unwrap().replay_lsn.unwrap() < boundary);
    assert!(!progress.recovered());
    progress.stage = PgBuildStage::Complete;
    assert!(progress.validate().is_err());
    control
        .simple_query("SELECT pg_wal_replay_resume()")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            progress.evidence = observer.snapshot().await.unwrap().evidence;
            if progress.recovered() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(progress.evidence.as_ref().unwrap().replay_lsn.unwrap() >= boundary);
    progress.validate().unwrap();
    let mut at_boundary = progress.clone();
    at_boundary.evidence.as_mut().unwrap().replay_lsn = Some(boundary);
    assert!(at_boundary.recovered());
    at_boundary.validate().unwrap();
    let bytes = encode(&progress).unwrap();
    assert_eq!(
        decode::<postgres_replicated::build::PgBuildProgress>(&bytes).unwrap(),
        progress
    );
    let json = String::from_utf8(bytes).unwrap();
    for malformed in [
        json.replacen("\"sequence\":", "\"sequence\":1,\"sequence\":", 1),
        json.replacen("\"stage\":\"Complete\",", "", 1),
        json.replacen("{", "{\"unknown\":1,", 1),
        format!(" {json}"),
    ] {
        assert!(
            decode::<postgres_replicated::build::PgBuildProgress>(malformed.as_bytes()).is_err()
        );
    }
    progress.evidence.as_mut().unwrap().timeline_id += 1;
    assert!(!progress.recovered());
    assert!(progress.validate().is_err());
}

async fn write_source(source: &PgPod) {
    let (sql, _) = source
        .application
        .instance()
        .connect_application()
        .await
        .unwrap();
    sql.batch_execute("CREATE TABLE build_receipts(id int PRIMARY KEY, value text); INSERT INTO build_receipts VALUES (1, 'native-copy'), (2, 'durable-boundary')")
        .await.unwrap();
}

#[tokio::test]
async fn all_wait_cancellation_reconciles_committed_metadata_without_permanent_fault() {
    use kuberic_runtime::replicator::ReplicaSetQuorumMode;
    use postgres_replicated::durable::CommitStage;
    let root = TestDataDir::new("all-commit");
    let source = PgPod::new(root.path().join("s"), native_identity(1, "source")).await;
    source.singleton().await;
    write_source(&source).await;
    for (index, stage) in [
        CommitStage::BeforeRename,
        CommitStage::CommittedBeforePublish,
        CommitStage::Published,
    ]
    .into_iter()
    .enumerate()
    {
        source
            .admit(postgres_replicated::testing::native_configuration(
                std::slice::from_ref(&source.identity),
                0,
                index as i64 + 2,
            ))
            .await;
        let gate = source
            .application
            .native_driver()
            .pause_catch_up_commit(stage);
        let before = source
            .application
            .native_driver()
            .durable_state()
            .await
            .catch_up;
        let primary = source.runtime.primary_replicator().await.unwrap();
        let waiter = tokio::spawn({
            let primary = primary.clone();
            async move {
                primary
                    .wait_for_catch_up_quorum(ReplicaSetQuorumMode::All)
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        waiter.abort();
        assert!(
            tokio::time::timeout(Duration::from_secs(2), waiter)
                .await
                .unwrap()
                .unwrap_err()
                .is_cancelled()
        );
        gate.release();
        let state = source.application.native_driver().durable_state().await;
        if stage == CommitStage::BeforeRename {
            assert_eq!(state.catch_up, before);
        } else {
            assert!(state.catch_up.is_some());
            assert_ne!(state.catch_up, before);
        }
        source.refresh().await;
        assert_ne!(
            source.runtime.partition_report().await.reported_fault,
            Some(kuberic_protocol::types::FaultType::Permanent)
        );
        tokio::time::timeout(
            Duration::from_secs(5),
            primary.wait_for_catch_up_quorum(ReplicaSetQuorumMode::All),
        )
        .await
        .unwrap()
        .unwrap();
    }
}

#[tokio::test]
async fn more_than_sixteen_terminal_builds_reclaim_capacity_and_slots_across_reopen() {
    use kuberic_protocol::types::OperationId;
    use kuberic_runtime::replicator::ReplicaInformation;
    let root = TestDataDir::new("many-builds");
    let mut source = PgPod::new(root.path().join("s"), native_identity(1, "source")).await;
    let target = PgPod::new(root.path().join("t"), native_identity(2, "target")).await;
    source.singleton().await;
    write_source(&source).await;
    let mut old_request = None;
    for index in 0..20 {
        let authority = source.authorize(&target, &format!("bounded-{index}")).await;
        if [0, 16, 19].contains(&index) {
            source.build(&target, &authority).await.unwrap();
            if index == 0 {
                old_request = Some(
                    target
                        .application
                        .native_driver()
                        .durable_state()
                        .await
                        .native_build
                        .unwrap()
                        .request,
                );
            }
        } else {
            let result = kuberic_agent::testing::execute_build(
                &source.runtime,
                ReplicaInformation::new(
                    authority.build_id.clone(),
                    target.identity.clone(),
                    "http://127.0.0.1:0".into(),
                ),
            )
            .await;
            assert!(result.is_err());
        }
        source
            .runtime
            .cancel_outbound_build(&authority.build_id)
            .await
            .unwrap();
        let state = source.application.native_driver().durable_state().await;
        assert!(state.outbound_builds.is_empty());
        assert!(state.outbound_attempts.is_empty());
        assert_eq!(state.suspended_builds.len(), 1);
        if index % 2 == 1 || index == 0 {
            source
                .effect(RuntimeEffectAction::RetireBuild(authority.build_id.clone()))
                .await
                .unwrap();
            target
                .effect(RuntimeEffectAction::RetireBuild(authority.build_id.clone()))
                .await
                .unwrap();
            let (sql, _) = source.application.instance().connect().await.unwrap();
            assert_eq!(
                sql.query_one(
                    "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'kuberic_%'",
                    &[]
                )
                .await
                .unwrap()
                .get::<_, i64>(0),
                0
            );
        }
        if index == 10 {
            source = source.reopen().await;
        }
    }
    let source = source.reopen().await;
    let state = source.application.native_driver().durable_state().await;
    assert!(state.outbound_builds.is_empty());
    assert!(state.suspended_builds.is_empty());
    assert_eq!(state.retired_builds.len(), 20);
    assert!(
        kuberic_agent::testing::execute_build(
            &source.runtime,
            ReplicaInformation::new(
                OperationId::new("bounded-0"),
                target.identity.clone(),
                target.endpoint.clone()
            ),
        )
        .await
        .is_err()
    );
    let target = target.reopen().await;
    assert!(target.inject(&old_request.unwrap()).await.is_err());
}

#[tokio::test]
async fn cancelled_attempt_cannot_publish_after_same_build_retry() {
    let root = TestDataDir::new("retry-attempt");
    let source = PgPod::new(root.path().join("s"), native_identity(1, "source")).await;
    let target = PgPod::new(root.path().join("t"), native_identity(2, "target")).await;
    source.singleton().await;
    write_source(&source).await;
    let authority = source.authorize(&target, "same-build").await;
    let gate = target
        .application
        .native_driver()
        .pause_build(PgBuildStage::Complete);
    let first = source.build(&target, &authority);
    tokio::pin!(first);
    tokio::select! {
        result = &mut first => panic!("unexpected early completion: {result:?}"),
        entered = tokio::time::timeout(Duration::from_secs(30), gate.entered.notified()) => entered.unwrap(),
    }
    source
        .runtime
        .cancel_outbound_build(&authority.build_id)
        .await
        .unwrap();
    assert!(
        source
            .application
            .native_driver()
            .durable_state()
            .await
            .outbound_builds
            .is_empty()
    );
    gate.release.notify_one();
    assert!(first.await.is_err());
    gate.release.notify_one();
    source.build(&target, &authority).await.unwrap();
    assert_eq!(
        source
            .application
            .native_driver()
            .durable_state()
            .await
            .outbound_builds
            .len(),
        1
    );
    source
        .effect(RuntimeEffectAction::RetireBuild(authority.build_id.clone()))
        .await
        .unwrap();
    target
        .effect(RuntimeEffectAction::RetireBuild(authority.build_id.clone()))
        .await
        .unwrap();
    assert!(
        source
            .application
            .native_driver()
            .durable_state()
            .await
            .retired_builds
            .contains(&authority.build_id)
    );
}

#[tokio::test]
async fn completion_is_exact_to_the_selected_build_across_reopen() {
    use kuberic_runtime_internal::authority::BuildAuthorityStore;
    let root = TestDataDir::new("exact-receipt");
    let source = PgPod::new(root.path().join("s"), native_identity(1, "source")).await;
    let target = PgPod::new(root.path().join("t"), native_identity(2, "target")).await;
    source.singleton().await;
    write_source(&source).await;
    let a = source.authorize(&target, "build-a").await;
    source.build(&target, &a).await.unwrap();
    target.refresh().await;
    assert_eq!(target.runtime.snapshot().await.builds[0].authority, a);
    let old = target
        .application
        .native_driver()
        .durable_state()
        .await
        .native_build
        .unwrap()
        .request;
    let b = source.authorize(&target, "build-b").await;
    target.refresh().await;
    source.refresh().await;
    assert!(
        target.runtime.snapshot().await.builds.is_empty(),
        "A's replay cannot certify B"
    );
    assert!(source.runtime.snapshot().await.builds.is_empty());
    assert!(
        source
            .store
            .load_build(&a.build_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(target.inject(&old).await.is_err());
    assert_eq!(
        target
            .application
            .native_driver()
            .durable_state()
            .await
            .native_build
            .unwrap()
            .request
            .authority,
        a
    );

    let target = target.reopen().await;
    assert!(
        target
            .store
            .load_build(&a.build_id)
            .await
            .unwrap()
            .is_none()
    );
    let resumed = source.authorize(&target, "build-b").await;
    assert_eq!(resumed, b);
    target.refresh().await;
    assert!(target.runtime.snapshot().await.builds.is_empty());
    source.build(&target, &b).await.unwrap();
    target.refresh().await;
    let completed = target.runtime.snapshot().await.builds;
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].authority, b);
    assert!(completed[0].completed);
}

#[tokio::test]
async fn overlapping_build_authority_cancels_and_rejects_the_old_completion() {
    let root = TestDataDir::new("overlap");
    let source = PgPod::new(root.path().join("s"), native_identity(1, "source")).await;
    let target = PgPod::new(root.path().join("t"), native_identity(2, "target")).await;
    source.singleton().await;
    write_source(&source).await;
    let a = source.authorize(&target, "overlap-a").await;
    let gate = target
        .application
        .native_driver()
        .pause_build(PgBuildStage::Complete);
    let old = source.build(&target, &a);
    tokio::pin!(old);
    tokio::select! {
        result = &mut old => panic!("unexpected early completion {result:?}"),
        entered = tokio::time::timeout(Duration::from_secs(30), gate.entered.notified()) => entered.unwrap(),
    }
    let b = tokio::time::timeout(
        Duration::from_secs(5),
        source.authorize(&target, "overlap-b"),
    )
    .await
    .unwrap();
    gate.release.notify_one();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), &mut old)
            .await
            .unwrap()
            .is_err()
    );
    target.refresh().await;
    source.refresh().await;
    assert!(target.runtime.snapshot().await.builds.is_empty());
    assert!(source.runtime.snapshot().await.builds.is_empty());
    source.build(&target, &b).await.unwrap();
    target.refresh().await;
    assert_eq!(target.runtime.snapshot().await.builds[0].authority, b);
}

#[tokio::test]
async fn all_catch_up_requires_every_current_session_and_is_cancellable() {
    use kuberic_protocol::types::ReplicaRole;
    use kuberic_runtime::replicator::ReplicaSetQuorumMode;
    let root = TestDataDir::new("all-quorum");
    let source = PgPod::new(root.path().join("s"), native_identity(1, "source")).await;
    let fast = PgPod::new(root.path().join("f"), native_identity(2, "fast")).await;
    let slow = PgPod::new(root.path().join("t"), native_identity(3, "slow")).await;
    source.singleton().await;
    write_source(&source).await;
    for (target, id) in [(&fast, "fast"), (&slow, "slow")] {
        let build = source.authorize(target, id).await;
        source.build(target, &build).await.unwrap();
    }
    let configuration = postgres_replicated::testing::native_configuration(
        &[
            source.identity.clone(),
            fast.identity.clone(),
            slow.identity.clone(),
        ],
        0,
        2,
    );
    for target in [&fast, &slow] {
        target.admit(configuration.clone()).await;
        target
            .effect(RuntimeEffectAction::ChangeRole(
                ReplicaRole::ActiveSecondary,
            ))
            .await
            .unwrap();
        target
            .effect(RuntimeEffectAction::SetAccessStatus {
                read: AccessStatus::Granted,
                write: AccessStatus::NotPrimary,
            })
            .await
            .unwrap();
    }
    source.admit(configuration.clone()).await;
    source
        .effect(RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::Granted,
            write: AccessStatus::Granted,
        })
        .await
        .unwrap();
    let primary = source.runtime.primary_replicator().await.unwrap();
    let (replica, _) = slow.application.instance().connect().await.unwrap();
    replica
        .simple_query("SELECT pg_wal_replay_pause()")
        .await
        .unwrap();
    let (writer, _) = source
        .application
        .instance()
        .connect_application()
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        writer.batch_execute("INSERT INTO build_receipts VALUES (3, 'quorum-only')"),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        primary.wait_for_catch_up_quorum(ReplicaSetQuorumMode::WriteQuorum),
    )
    .await
    .unwrap()
    .unwrap();
    let spawn_all = || {
        let primary = primary.clone();
        tokio::spawn(async move {
            primary
                .wait_for_catch_up_quorum(ReplicaSetQuorumMode::All)
                .await
        })
    };
    let mut all = spawn_all();
    assert!(
        tokio::time::timeout(Duration::from_millis(250), &mut all)
            .await
            .is_err()
    );
    primary.update_epoch(configuration.epoch).await.unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), &mut all)
            .await
            .unwrap()
            .unwrap(),
        Err(kuberic_runtime::RuntimeError::OperationCancelled)
    ));
    let mut all = spawn_all();
    assert!(
        tokio::time::timeout(Duration::from_millis(250), &mut all)
            .await
            .is_err()
    );
    let next = postgres_replicated::testing::native_configuration(
        &[
            source.identity.clone(),
            fast.identity.clone(),
            slow.identity.clone(),
        ],
        0,
        3,
    );
    source.admit(next.clone()).await;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), &mut all)
            .await
            .unwrap()
            .unwrap(),
        Err(kuberic_runtime::RuntimeError::OperationCancelled)
    ));
    for target in [&fast, &slow] {
        target.admit(next.clone()).await;
    }
    // Admission closes access and restarts a previously readable standby. Pause
    // the new process before creating the next frozen replay boundary.
    let (replica, _) = slow.application.instance().connect().await.unwrap();
    replica
        .simple_query("SELECT pg_wal_replay_pause()")
        .await
        .unwrap();
    source
        .effect(RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::Granted,
            write: AccessStatus::Granted,
        })
        .await
        .unwrap();
    let (writer, _) = source
        .application
        .instance()
        .connect_application()
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        writer.batch_execute("INSERT INTO build_receipts VALUES (4, 'all-boundary')"),
    )
    .await
    .unwrap()
    .unwrap();
    let mut all = spawn_all();
    assert!(
        tokio::time::timeout(Duration::from_millis(250), &mut all)
            .await
            .is_err()
    );
    replica
        .simple_query("SELECT pg_wal_replay_resume()")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), &mut all)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        !replica
            .query_one("SELECT pg_is_wal_replay_paused()", &[])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
}

#[tokio::test]
async fn admitted_standby_restart_and_build_retirement_preserve_readable_data() {
    use kuberic_protocol::types::ReplicaRole;
    use kuberic_runtime::application::OpenMode;
    let root = TestDataDir::new("sf-restart");
    let source = PgPod::new(root.path().join("s"), native_identity(1, "source")).await;
    let target = PgPod::new(root.path().join("t"), native_identity(2, "target")).await;
    source.singleton().await;
    write_source(&source).await;
    let build = source.authorize(&target, "accepted-build").await;
    source.build(&target, &build).await.unwrap();
    let configuration = postgres_replicated::testing::native_configuration(
        &[source.identity.clone(), target.identity.clone()],
        0,
        2,
    );
    target.admit(configuration.clone()).await;
    target
        .effect(RuntimeEffectAction::ChangeRole(
            ReplicaRole::ActiveSecondary,
        ))
        .await
        .unwrap();
    target
        .effect(RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::Granted,
            write: AccessStatus::NotPrimary,
        })
        .await
        .unwrap();
    source.admit(configuration).await;
    target
        .effect(RuntimeEffectAction::RetireBuild(build.build_id.clone()))
        .await
        .unwrap();
    source
        .effect(RuntimeEffectAction::RetireBuild(build.build_id.clone()))
        .await
        .unwrap();
    assert!(target.application.instance().is_running().await);
    let (reader, _) = target
        .application
        .instance()
        .connect_application()
        .await
        .unwrap();
    assert_eq!(
        reader
            .query("SELECT id FROM build_receipts", &[])
            .await
            .unwrap()
            .len(),
        2
    );
    drop(reader);

    let target = target.reopen().await;
    target
        .runtime
        .reconstruct(
            OpenMode::Existing,
            ReplicaRole::ActiveSecondary,
            AccessStatus::Granted,
            AccessStatus::NotPrimary,
            None,
        )
        .await
        .unwrap();
    assert!(!target.application.instance().is_running().await);
    target.peer(&source).await;
    target.refresh().await;
    source.peer(&target).await;
    assert!(target.runtime.snapshot().await.builds.is_empty());
    let (reader, _) = target
        .application
        .instance()
        .connect_application()
        .await
        .unwrap();
    assert_eq!(
        reader
            .query("SELECT id FROM build_receipts", &[])
            .await
            .unwrap()
            .len(),
        2
    );
    source
        .effect(RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::Granted,
            write: AccessStatus::Granted,
        })
        .await
        .unwrap();
    let (writer, _) = source
        .application
        .instance()
        .connect_application()
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        writer.batch_execute("INSERT INTO build_receipts VALUES (3, 'fresh-session')"),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        reader
            .query("SELECT id FROM build_receipts", &[])
            .await
            .unwrap()
            .len(),
        3
    );
}

async fn assert_data(target: &PgPod) {
    let (sql, _) = target.application.instance().connect().await.unwrap();
    assert!(
        sql.query_one("SELECT pg_is_in_recovery()", &[])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let connection = format!(
        "host={} port={} dbname=kuberic",
        target.application.instance().socket_dir().display(),
        target.application.instance().port()
    );
    let (sql, connection) = tokio_postgres::connect(&connection, tokio_postgres::NoTls)
        .await
        .unwrap();
    let task = tokio::spawn(connection);
    let rows = sql
        .query("SELECT id, value FROM build_receipts ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[1].get::<_, String>(1), "durable-boundary");
    drop(sql);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn fresh_native_build_requires_durable_replay_and_exact_lineage() {
    let root = TestDataDir::new("nb");
    let source = PgPod::new(root.path().join("s"), native_identity(1, "s")).await;
    let target = PgPod::new(root.path().join("t"), native_identity(2, "t")).await;
    source.singleton().await;
    write_source(&source).await;
    let authority = source.authorize(&target, "fresh").await;
    assert!(authority.replication_boundary_lsn > 0);
    source.build(&target, &authority).await.unwrap();
    target.refresh().await;
    assert_data(&target).await;
    let source_state = source.application.native_driver().durable_state().await;
    let target_state = target.application.native_driver().durable_state().await;
    assert_eq!(
        source_state.system_identifier,
        target_state.system_identifier
    );
    assert_eq!(source_state.timeline_id, target_state.timeline_id);
    assert_eq!(
        source_state.timeline_history_digest,
        target_state.timeline_history_digest
    );
    let report = target.runtime.snapshot().await;
    assert_eq!(report.builds.len(), 1);
    assert!(report.builds[0].completed);
    assert!(target_state.replay_lsn.unwrap() >= authority.replication_boundary_lsn);
    assert_ne!(report.read_status, AccessStatus::Granted);
    assert!(
        target
            .application
            .instance()
            .connect_application()
            .await
            .is_err()
    );
    let configuration = postgres_replicated::testing::native_configuration(
        &[source.identity.clone(), target.identity.clone()],
        0,
        2,
    );
    target.admit(configuration.clone()).await;
    target.runtime.cancel_configuration_work().await.unwrap();
    target
        .effect(RuntimeEffectAction::ChangeRole(
            kuberic_protocol::types::ReplicaRole::ActiveSecondary,
        ))
        .await
        .unwrap();
    target
        .effect(RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::Granted,
            write: AccessStatus::NotPrimary,
        })
        .await
        .unwrap();
    let (reader, _) = target
        .application
        .instance()
        .connect_application()
        .await
        .unwrap();
    assert_eq!(
        reader
            .query("SELECT id FROM build_receipts", &[])
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        reader
            .batch_execute("INSERT INTO build_receipts VALUES (99, 'forbidden')")
            .await
            .unwrap_err()
            .as_db_error()
            .unwrap()
            .code(),
        &tokio_postgres::error::SqlState::READ_ONLY_SQL_TRANSACTION
    );
    source.admit(configuration).await;
    source
        .effect(RuntimeEffectAction::WaitForCatchup)
        .await
        .unwrap();
    source.refresh().await;
    assert!(source.runtime.snapshot().await.catch_up_complete);
    source
        .effect(RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::Granted,
            write: AccessStatus::Granted,
        })
        .await
        .unwrap();
    let (writer, _) = source
        .application
        .instance()
        .connect_application()
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        writer.batch_execute("INSERT INTO build_receipts VALUES (3, 'remote-apply')"),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        reader
            .query_one("SELECT value FROM build_receipts WHERE id = 3", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "remote-apply"
    );
}

#[tokio::test]
async fn interrupted_native_stages_reopen_with_fresh_sessions() {
    for stage in [
        PgBuildStage::Intent,
        PgBuildStage::Installed,
        PgBuildStage::Recovering,
        PgBuildStage::Complete,
    ] {
        let root = TestDataDir::new("nc");
        let source = PgPod::new(root.path().join("s"), native_identity(1, "s")).await;
        let target = PgPod::new(root.path().join("t"), native_identity(2, "t")).await;
        source.singleton().await;
        write_source(&source).await;
        let authority = source.authorize(&target, "crash").await;
        let gate = target.application.native_driver().pause_build(stage);
        let before = target
            .application
            .native_driver()
            .durable_state()
            .await
            .system_identifier;
        let old_request = {
            let build = source.build(&target, &authority);
            tokio::pin!(build);
            tokio::select! {
                result = &mut build => panic!("build completed before {stage:?}: {result:?}"),
                entered = tokio::time::timeout(Duration::from_secs(30), gate.entered.notified()) => { entered.unwrap(); }
            }
            let durable = target.application.native_driver().durable_state().await;
            assert_eq!(durable.native_build.as_ref().unwrap().stage, stage);
            assert!(
                target.runtime.snapshot().await.builds.is_empty(),
                "publication preceded durable completion release"
            );
            if stage == PgBuildStage::Intent {
                let (system, _) = target
                    .application
                    .instance()
                    .control_identity()
                    .await
                    .unwrap();
                assert_eq!(Some(system), before);
            }
            let old_request = durable.native_build.unwrap().request;
            let processes = target.application.instance().is_running().await.then(|| {
                postgres_replicated::testing::ProcessProbe::postgres(
                    target.application.instance().data_dir(),
                )
            });
            target.runtime.abort();
            assert!(build.await.is_err());
            if let Some(processes) = processes {
                processes.assert_reaped();
            }
            old_request
        };
        let target = target.reopen().await;
        assert!(target.inject(&old_request).await.is_err());
        let authority = source.authorize(&target, "crash").await;
        source.build(&target, &authority).await.unwrap();
        target.refresh().await;
        assert_data(&target).await;
        assert_eq!(target.runtime.snapshot().await.builds.len(), 1);
    }
}

#[tokio::test]
async fn native_lineage_validation_precedes_scalar_boundary_comparison() {
    let source = PgLineage {
        system_identifier: "123".into(),
        timeline: 1,
        history: vec![],
        history_text: String::new(),
    };
    let other_system = PgLineage {
        system_identifier: "456".into(),
        ..source.clone()
    };
    assert!(!other_system.can_rewind_from(&source));
    let root = TestDataDir::new("nl");
    let source = PgPod::new(root.path().join("s"), native_identity(1, "s")).await;
    let target = PgPod::new(root.path().join("t"), native_identity(2, "t")).await;
    source.singleton().await;
    target.singleton().await;
    source
        .admit(postgres_replicated::testing::native_configuration(
            std::slice::from_ref(&source.identity),
            0,
            3,
        ))
        .await;
    let authority = source.authorize(&target, "incompatible").await;
    target.refresh().await;
    assert!(
        target.runtime.snapshot().await.builds.is_empty(),
        "an unrelated pre-existing primary's scalar progress is not build completion"
    );
    let before = target
        .application
        .instance()
        .control_identity()
        .await
        .unwrap();
    let error = source.build(&target, &authority).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("incompatible PostgreSQL system identity/timeline"),
        "{error}"
    );
    assert_eq!(
        target
            .application
            .instance()
            .control_identity()
            .await
            .unwrap(),
        before
    );
    assert!(
        target
            .application
            .native_driver()
            .durable_state()
            .await
            .native_build
            .is_none()
    );
    let evidence = PgNativeObserver::new(target.application.instance().clone())
        .snapshot()
        .await;
    // Rejection may stop an in-flight build, but must never replace its data.
    if let Ok(evidence) = evidence {
        assert_eq!(evidence.evidence.unwrap().system_identifier, before.0);
    }
    source
        .effect(RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::ReconfigurationPending,
            write: AccessStatus::ReconfigurationPending,
        })
        .await
        .unwrap();
}
