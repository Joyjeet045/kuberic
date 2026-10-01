use super::*;
use crate::testing::{PgPod, ProcessProbe, TestDataDir, native_identity};
use std::path::{Path, PathBuf};
use std::time::Duration;

tokio::task_local! {
    pub(super) static BUILD_TIMEOUT: Duration;
}

async fn admitted_request(source: &PgPod, target: &PgPod) -> PgBuildRequest {
    source.singleton().await;
    let (sql, connection) = source
        .application
        .instance()
        .connect_application()
        .await
        .unwrap();
    sql.batch_execute(
        "CREATE TABLE deletion_receipts(id int PRIMARY KEY, value text);
         INSERT INTO deletion_receipts VALUES (1, 'old-authority'), (2, 'successor-data')",
    )
    .await
    .unwrap();
    drop(sql);
    connection.await.unwrap();
    let authority = source.authorize(target, "owned-deletion").await;
    let driver = target.application.native_driver();
    let gate = driver.pause_build(PgBuildStage::Intent);
    let work = source.build(target, &authority);
    tokio::pin!(work);
    tokio::select! {
        result = &mut work => panic!("build missed intent gate: {result:?}"),
        entered = tokio::time::timeout(Duration::from_secs(15), gate.entered.notified()) => entered.unwrap(),
    }
    let request = driver.durable_state().await.native_build.unwrap().request;
    driver.build_cancellation.lock().unwrap().cancel();
    assert!(work.await.is_err());
    *driver.build_cancellation.lock().unwrap() = CancellationToken::new();
    *driver.build_gate.lock().unwrap() = None;
    request
}

fn deletion_gate(target: &PgPod, entry: &str) -> PathBuf {
    let marker = target.root.join("deletion-entered");
    assert!(!marker.exists());
    *target
        .application
        .instance()
        .clear_pgdata_hook
        .lock()
        .unwrap() = Some((entry.into(), marker.clone()));
    marker
}

async fn deletion_entered(marker: &Path) -> ProcessProbe {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(pid) = std::fs::read_to_string(marker)
                && let Ok(pid) = pid.parse()
            {
                return ProcessProbe::process(pid);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("actual deletion helper did not enter")
}

async fn assert_copying(target: &PgPod, request: &PgBuildRequest) {
    let durable = target.application.native_driver().durable_state().await;
    let progress = durable.native_build.unwrap();
    assert_eq!(progress.request, *request);
    assert_eq!(progress.stage, PgBuildStage::Copying);
    assert_eq!(progress.method, PgBuildMethod::Fresh);
    assert!(progress.evidence.is_none());
    assert_eq!(durable.recovery_state, PgRecoveryState::Rebuilding);
    assert!(durable.external_access_closed);
    assert!(!target.application.instance().is_running().await);
    assert!(target.runtime.snapshot().await.builds.is_empty());
}

async fn assert_successor(target: &PgPod, request: &PgBuildRequest) {
    let driver = target.application.native_driver();
    let progress = driver.durable_state().await.native_build.unwrap();
    assert_eq!(progress.request, *request);
    assert_eq!(progress.stage, PgBuildStage::Complete);
    assert_eq!(progress.method, PgBuildMethod::Fresh);
    assert!(progress.recovered());
    assert_eq!(driver.lineage().await.unwrap(), request.lineage);
    let evidence = driver.observer.snapshot().await.unwrap().evidence.unwrap();
    assert!(evidence.in_recovery);
    assert_eq!(
        evidence.system_identifier,
        request.lineage.system_identifier
    );
    assert_eq!(evidence.timeline_id, request.lineage.timeline);
    let boundary = request.authority.replication_boundary_lsn;
    assert!(evidence.flush_lsn >= boundary);
    assert!(evidence.received_lsn.unwrap() >= boundary);
    assert!(evidence.replay_lsn.unwrap() >= boundary);

    let instance = target.application.instance();
    let (sql, connection) = tokio_postgres::connect(
        &format!(
            "host={} port={} dbname=kuberic",
            instance.socket_dir().display(),
            instance.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    let connection = tokio::spawn(connection);
    let rows = sql
        .query("SELECT id, value FROM deletion_receipts ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| (row.get::<_, i32>(0), row.get::<_, String>(1)))
            .collect::<Vec<_>>(),
        [(1, "old-authority".into()), (2, "successor-data".into())]
    );
    drop(sql);
    connection.await.unwrap().unwrap();
}

async fn release_after_successor(
    target: &PgPod,
    request: &PgBuildRequest,
    marker: &Path,
    old: &ProcessProbe,
) {
    old.assert_reaped();
    assert!(
        marker.exists(),
        "cleanup required the paused deletion to cooperate"
    );
    assert_successor(target, request).await;
    let instance = target.application.instance();
    let generation = instance.generation_id();
    let progress = target
        .application
        .native_driver()
        .durable_state()
        .await
        .native_build;
    let version = std::fs::read(instance.data_dir().join("PG_VERSION")).unwrap();
    let sentinel = instance.data_dir().join("base/successor-sentinel");
    std::fs::write(&sentinel, b"successor-owned-directory").unwrap();
    std::fs::remove_file(marker).unwrap();
    // Reaping, not a delay, proves the old worker cannot resume after release.
    old.assert_reaped();
    assert_eq!(
        std::fs::read(&sentinel).unwrap(),
        b"successor-owned-directory"
    );
    assert_eq!(
        std::fs::read(instance.data_dir().join("PG_VERSION")).unwrap(),
        version
    );
    assert_eq!(instance.generation_id(), generation);
    assert_eq!(
        target
            .application
            .native_driver()
            .durable_state()
            .await
            .native_build,
        progress
    );
    assert_successor(target, request).await;
    std::fs::remove_file(sentinel).unwrap();
}

#[tokio::test]
async fn pgdata_deletion_supersession_reaps_before_successor_configuration() {
    for entry in ["base", "PG_VERSION"] {
        let root = TestDataDir::new("delete-supersession");
        let mut source = PgPod::new(root.path().join("s"), native_identity(1, "source")).await;
        let mut target = PgPod::new(root.path().join("t"), native_identity(2, "target")).await;
        let request = admitted_request(&source, &target).await;
        {
            let driver = target.application.native_driver();
            let marker = deletion_gate(&target, entry);
            let work = driver.receive_build(request.clone());
            tokio::pin!(work);
            let old = tokio::select! {
                result = &mut work => panic!("build missed deletion gate: {result:?}"),
                old = deletion_entered(&marker) => old,
            };
            let old_generation = target.application.instance().generation_id();
            source = source.reopen().await;
            source.peer(&target).await;
            // Install a real successor source session while deletion is paused.
            let (result, ()) = tokio::time::timeout(Duration::from_secs(10), async {
                tokio::join!(&mut work, target.peer(&source))
            })
            .await
            .unwrap();
            assert!(matches!(result, Err(RuntimeError::OperationCancelled)));
            old.assert_reaped();
            assert_eq!(
                driver.peer_session(&source.identity).await.unwrap(),
                source.session
            );
            assert_copying(&target, &request).await;
            assert!(matches!(
                driver.receive_build(request.clone()).await,
                Err(RuntimeError::AuthorityNotAdmitted)
            ));
            let authority = source
                .authorize(&target, request.authority.build_id.as_str())
                .await;
            source.build(&target, &authority).await.unwrap();
            let successor = driver.durable_state().await.native_build.unwrap().request;
            assert_ne!(successor.source_session, request.source_session);
            assert_eq!(successor.target_session, request.target_session);
            assert!(successor.same_work(&request));
            assert!(target.application.instance().generation_id() > old_generation);
            release_after_successor(&target, &successor, &marker, &old).await;
        }
        target.shutdown().await.unwrap();
        source.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn pgdata_deletion_timeout_reaps_before_same_build_retry() {
    for entry in ["base", "PG_VERSION"] {
        let root = TestDataDir::new("delete-timeout");
        let mut source = PgPod::new(root.path().join("s"), native_identity(1, "source")).await;
        let mut target = PgPod::new(root.path().join("t"), native_identity(2, "target")).await;
        let request = admitted_request(&source, &target).await;
        let marker = deletion_gate(&target, entry);
        {
            let work = BUILD_TIMEOUT.scope(
                Duration::from_secs(2),
                target
                    .application
                    .native_driver()
                    .receive_build(request.clone()),
            );
            tokio::pin!(work);
            let old = tokio::select! {
                result = &mut work => panic!("build missed deletion gate: {result:?}"),
                old = deletion_entered(&marker) => old,
            };
            let error = work.await.unwrap_err();
            assert!(
                error.to_string().contains("native build timed out"),
                "{error}"
            );
            old.assert_reaped();
            assert_copying(&target, &request).await;
            source.build(&target, &request.authority).await.unwrap();
            release_after_successor(&target, &request, &marker, &old).await;
        }
        target.shutdown().await.unwrap();
        source.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn pgdata_deletion_dropped_build_reopens_with_fresh_session() {
    for entry in ["base", "PG_VERSION"] {
        let root = TestDataDir::new("delete-drop");
        let mut source = PgPod::new(root.path().join("s"), native_identity(1, "source")).await;
        let target = PgPod::new(root.path().join("t"), native_identity(2, "target")).await;
        let request = admitted_request(&source, &target).await;
        let marker = deletion_gate(&target, entry);
        let old = {
            let work = target
                .application
                .native_driver()
                .receive_build(request.clone());
            tokio::pin!(work);
            tokio::select! {
                result = &mut work => panic!("build missed deletion gate: {result:?}"),
                old = deletion_entered(&marker) => old,
            }
        };
        old.assert_reaped();
        assert_copying(&target, &request).await;
        let mut target = target.reopen().await;
        assert!(target.inject(&request).await.is_err());
        let authority = source
            .authorize(&target, request.authority.build_id.as_str())
            .await;
        source.build(&target, &authority).await.unwrap();
        let successor = target
            .application
            .native_driver()
            .durable_state()
            .await
            .native_build
            .unwrap()
            .request;
        assert_ne!(successor.target_session, request.target_session);
        assert_eq!(successor.authority.build_id, request.authority.build_id);
        release_after_successor(&target, &successor, &marker, &old).await;
        target.shutdown().await.unwrap();
        source.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn pgdata_deletion_io_failure_is_reported_and_partial_copy_retried() {
    for entry in ["base", "PG_VERSION"] {
        let root = TestDataDir::new("delete-failure");
        let mut source = PgPod::new(root.path().join("s"), native_identity(1, "source")).await;
        let mut target = PgPod::new(root.path().join("t"), native_identity(2, "target")).await;
        let request = admitted_request(&source, &target).await;
        let marker = deletion_gate(&target, entry);
        {
            let work = target
                .application
                .native_driver()
                .receive_build(request.clone());
            tokio::pin!(work);
            let old = tokio::select! {
                result = &mut work => panic!("build missed deletion gate: {result:?}"),
                old = deletion_entered(&marker) => old,
            };
            // Force a real unlink/rmdir error after the helper has read its entry.
            let original = target.application.instance().data_dir().join(entry);
            let saved = target.root.join("removed-entry");
            std::fs::rename(&original, &saved).unwrap();
            std::fs::remove_file(&marker).unwrap();
            let error = work.await.unwrap_err();
            assert!(error.to_string().contains("clear PGDATA failed"), "{error}");
            assert!(
                error.to_string().contains("No such file or directory"),
                "{error}"
            );
            old.assert_reaped();
            assert_copying(&target, &request).await;
            std::fs::rename(saved, original).unwrap();
            source.build(&target, &request.authority).await.unwrap();
            assert_successor(&target, &request).await;
        }
        target.shutdown().await.unwrap();
        source.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn pgdata_deletion_release_executes_removals_without_following_symlinks() {
    for directory in [false, true] {
        let root = TestDataDir::new("delete-release");
        let data = root.path().join("pgdata");
        let entry = data.join("remove-me");
        std::fs::create_dir(&data).unwrap();
        if directory {
            std::fs::create_dir(&entry).unwrap();
            std::fs::write(entry.join("nested"), b"old-data").unwrap();
        } else {
            std::fs::write(&entry, b"old-data").unwrap();
        }
        let outside = root.path().join("keep-me");
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("retained"), b"not-pgdata").unwrap();
        std::os::unix::fs::symlink(&outside, data.join("external-link")).unwrap();
        let instance = PgInstanceManager::new(data.clone(), crate::testing::find_pg_bin(), 1);
        let marker = root.path().join("deletion-entered");
        *instance.clear_pgdata_hook.lock().unwrap() = Some(("remove-me".into(), marker.clone()));
        let work = instance.clear_pgdata();
        tokio::pin!(work);
        let helper = tokio::select! {
            result = &mut work => panic!("clear missed deletion gate: {result:?}"),
            helper = deletion_entered(&marker) => helper,
        };
        assert!(entry.exists());
        std::fs::remove_file(marker).unwrap();
        work.await.unwrap();
        helper.assert_reaped();
        assert_eq!(std::fs::read_dir(&data).unwrap().count(), 0);
        assert_eq!(
            std::fs::read(outside.join("retained")).unwrap(),
            b"not-pgdata"
        );
        instance.stop().await.unwrap();
    }
}
