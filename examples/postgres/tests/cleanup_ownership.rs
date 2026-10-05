use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use postgres_replicated::access::{PgAccessController, initialize_application_role};
use postgres_replicated::instance::{PgInstanceManager, PgProcessState};
use postgres_replicated::testing::{
    PgPod, ProcessProbe, TestDataDir, allocate_port, find_pg_bin, native_identity, wrapped_pg_bin,
};

struct QueuedCleanup {
    release: std::sync::mpsc::Sender<()>,
    runtime: Option<std::thread::JoinHandle<()>>,
}

impl QueuedCleanup {
    fn drain(mut self) {
        let _ = self.release.send(());
        self.runtime.take().unwrap().join().unwrap();
    }
}

impl Drop for QueuedCleanup {
    fn drop(&mut self) {
        let _ = self.release.send(());
        if let Some(runtime) = self.runtime.take() {
            runtime.join().unwrap();
        }
    }
}

async fn cancel_queued<F: Future<Output = ()> + Send + 'static>(
    instance: Arc<PgInstanceManager>,
    action: F,
    entered: Option<PathBuf>,
) -> QueuedCleanup {
    let (release, released) = std::sync::mpsc::channel();
    let (cancelled, joined) = tokio::sync::oneshot::channel();
    let runtime = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap()
            .block_on(async {
                let mut action = Some(action);
                let mut task = None;
                if let Some(entered) = entered {
                    task = Some(tokio::spawn(action.take().unwrap()));
                    tokio::time::timeout(Duration::from_secs(5), async {
                        while !entered.exists() {
                            tokio::time::sleep(Duration::from_millis(2)).await;
                        }
                    })
                    .await
                    .unwrap();
                }
                let (occupied, occupancy) = tokio::sync::oneshot::channel();
                let blocker = tokio::task::spawn_blocking(move || {
                    occupied.send(()).unwrap();
                    released.recv_timeout(Duration::from_secs(30)).unwrap();
                });
                occupancy.await.unwrap();
                let task = task.unwrap_or_else(|| tokio::spawn(action.take().unwrap()));
                tokio::time::timeout(Duration::from_secs(5), async {
                    while instance.process_state().await != PgProcessState::Stopping {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                assert!(!task.is_finished());
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
                cancelled.send(()).unwrap();
                blocker.await.unwrap();
            });
    });
    tokio::time::timeout(Duration::from_secs(10), joined)
        .await
        .unwrap()
        .unwrap();
    QueuedCleanup {
        release,
        runtime: Some(runtime),
    }
}

async fn manager(
    root: &Path,
) -> (
    Arc<PgInstanceManager>,
    tokio::sync::mpsc::Sender<kuberic_runtime::protocol::types::FaultType>,
) {
    let instance = Arc::new(PgInstanceManager::new(
        root.join("pgdata"),
        find_pg_bin(),
        allocate_port().await,
    ));
    instance.init_db().await.unwrap();
    let (faults, _reported) = tokio::sync::mpsc::channel(8);
    instance.start_native(faults.clone()).await.unwrap();
    (instance, faults)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_cancelled_stop_cannot_touch_a_restarted_generation() {
    let root = TestDataDir::new("queue-stop");
    let (instance, faults) = manager(root.path()).await;
    let queued = cancel_queued(
        instance.clone(),
        {
            let instance = instance.clone();
            async move {
                instance.stop().await.unwrap();
            }
        },
        None,
    )
    .await;
    assert_eq!(instance.process_state().await, PgProcessState::Running);
    instance.stop().await.unwrap();
    instance.stop().await.unwrap();
    instance.start_native(faults).await.unwrap();
    let replacement = ProcessProbe::postgres(instance.data_dir());
    queued.drain();
    assert!(instance.is_running().await);
    assert_eq!(instance.process_state().await, PgProcessState::Running);
    instance
        .connect()
        .await
        .unwrap()
        .0
        .simple_query("SELECT 1")
        .await
        .unwrap();
    instance.stop().await.unwrap();
    instance.stop().await.unwrap();
    replacement.assert_reaped();
}

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn queued_cancelled_fence_cannot_touch_regranted_sql() {
    let root = TestDataDir::new("queue-fence");
    let (instance, _) = manager(root.path()).await;
    initialize_application_role(&instance).await.unwrap();
    PgAccessController::new(&instance)
        .grant_role_access()
        .await
        .unwrap();
    let queued = cancel_queued(
        instance.clone(),
        {
            let instance = instance.clone();
            async move {
                PgAccessController::new(&instance)
                    .close_external()
                    .await
                    .unwrap();
            }
        },
        None,
    )
    .await;
    let access = PgAccessController::new(&instance);
    access.close_external().await.unwrap();
    access.grant_role_access().await.unwrap();
    let (sql, _) = instance.connect_application().await.unwrap();
    sql.simple_query("SELECT 1").await.unwrap();
    queued.drain();
    assert!(instance.is_running().await);
    sql.simple_query("SELECT 1").await.unwrap();
    instance.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_cancelled_sf_timeout_cannot_poison_its_successful_retry() {
    let root = TestDataDir::new("queue-sf");
    let armed = root.path().join("armed");
    let entered = root.path().join("entered");
    let bin = wrapped_pg_bin(
        root.path(),
        "pg_controldata",
        &format!(
            "#!/bin/sh\nif test -f '{}'; then\n echo started > '{}'\n sleep 30\nelse\n exec '{}/pg_controldata' \"$@\"\nfi\n",
            armed.display(),
            entered.display(),
            find_pg_bin().display()
        ),
    );
    let pod = PgPod::with_bin(root.path().join("p"), native_identity(1, "retry"), bin).await;
    pod.singleton().await;
    let primary = pod.runtime.primary_replicator().await.unwrap();
    let instance = pod.application.instance().clone();
    let (sql, _) = instance.connect_application().await.unwrap();
    std::fs::write(&armed, b"").unwrap();
    let queued = cancel_queued(
        instance.clone(),
        {
            let primary = primary.clone();
            async move {
                let _ = primary.current_progress().await;
            }
        },
        Some(entered),
    )
    .await;
    std::fs::remove_file(armed).unwrap();
    assert!(primary.current_progress().await.unwrap() > 0);
    sql.simple_query("SELECT 1").await.unwrap();
    queued.drain();
    assert!(instance.is_running().await);
    assert_eq!(instance.process_state().await, PgProcessState::Running);
    sql.simple_query("SELECT 1").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inflight_cleanup_cannot_touch_replacement_helpers_or_publish_old_state() {
    let root = TestDataDir::new("cleanup-live");
    let armed = root.path().join("armed");
    let marker = root.path().join("helper-pid");
    let bin = wrapped_pg_bin(
        root.path(),
        "pg_controldata",
        &format!(
            "#!/bin/sh\nif test -f '{}'; then\n echo $$ > '{}'\n while test -f '{}'; do sleep 0.02; done\nfi\nexec '{}/pg_controldata' \"$@\"\n",
            armed.display(),
            marker.display(),
            armed.display(),
            find_pg_bin().display()
        ),
    );
    let instance = Arc::new(PgInstanceManager::new(
        root.path().join("pgdata"),
        bin,
        allocate_port().await,
    ));
    instance.init_db().await.unwrap();
    let (faults, _rx) = tokio::sync::mpsc::channel(8);
    instance.start_native(faults.clone()).await.unwrap();
    let identity = instance.control_identity().await.unwrap();
    let gate = instance.pause_cleanup();
    let stopping = tokio::spawn({
        let instance = instance.clone();
        async move { instance.stop().await }
    });
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    stopping.abort();
    assert!(stopping.await.unwrap_err().is_cancelled());
    // start itself must finish the retired generation, not mistake its still
    // live postmaster for an already-ready replacement.
    instance.start_native(faults).await.unwrap();
    let replacement = ProcessProbe::postgres(instance.data_dir());
    std::fs::write(&armed, b"").unwrap();
    let helper = tokio::spawn({
        let instance = instance.clone();
        async move { instance.control_identity().await }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !marker.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    gate.release();
    tokio::time::timeout(Duration::from_secs(5), gate.finished.notified())
        .await
        .unwrap();
    std::fs::remove_file(armed).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), helper)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        identity
    );
    assert!(instance.is_running().await);
    assert_eq!(instance.process_state().await, PgProcessState::Running);
    instance.stop().await.unwrap();
    replacement.assert_reaped();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_cleanup_cannot_affect_a_reopened_sf_store() {
    let root = TestDataDir::new("cleanup-open");
    let pod = PgPod::new(root.path().join("p"), native_identity(1, "reopen")).await;
    pod.singleton().await;
    let old_instance = pod.application.instance().clone();
    let gate = old_instance.pause_cleanup();
    let stopping = tokio::spawn({
        let instance = old_instance.clone();
        async move { instance.stop().await }
    });
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    stopping.abort();
    assert!(stopping.await.unwrap_err().is_cancelled());
    old_instance.stop().await.unwrap();
    let reopened = pod.reopen().await;
    reopened.singleton().await;
    let before = reopened.application.native_driver().durable_state().await;
    let (sql, _) = reopened
        .application
        .instance()
        .connect_application()
        .await
        .unwrap();
    gate.release();
    tokio::time::timeout(Duration::from_secs(5), gate.finished.notified())
        .await
        .unwrap();
    old_instance.stop().await.unwrap();
    assert!(reopened.application.instance().is_running().await);
    sql.simple_query("SELECT 1").await.unwrap();
    let after = reopened.application.native_driver().durable_state().await;
    assert_eq!(after.identity, before.identity);
    assert_eq!(after.system_identifier, before.system_identifier);
    assert_eq!(after.generation, before.generation);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn captured_fatal_cleanup_cannot_be_cleared_by_a_replacement_attempt() {
    let root = TestDataDir::new("cleanup-error");
    let (instance, faults) = manager(root.path()).await;
    let owned = ProcessProbe::postgres(instance.data_dir());
    let gate = instance.pause_cleanup();
    let stopping = tokio::spawn({
        let instance = instance.clone();
        async move { instance.stop().await }
    });
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    stopping.abort();
    assert!(stopping.await.unwrap_err().is_cancelled());
    std::fs::remove_file(instance.data_dir().join("postmaster.pid")).unwrap();
    let error = instance.stop().await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("PID file does not identify an owned live process")
    );
    assert!(instance.start_native(faults.clone()).await.is_err());
    gate.release();
    tokio::time::timeout(Duration::from_secs(5), gate.finished.notified())
        .await
        .unwrap();
    assert!(!instance.is_running().await);
    assert_eq!(instance.process_state().await, PgProcessState::Faulted);
    assert!(instance.start_native(faults).await.is_err());
    owned.assert_reaped();
}
