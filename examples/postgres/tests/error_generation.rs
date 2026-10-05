use std::path::PathBuf;
use std::time::Duration;

use kuberic_runtime::protocol::types::{AccessStatus, FaultType};
use kuberic_runtime::testing::effects::RuntimeEffectAction;
use postgres_replicated::access::PgAccessController;
use postgres_replicated::testing::{
    PgPod, ProcessProbe, TestDataDir, find_pg_bin, native_identity, wrapped_pg_bin,
};

struct PausedCallback {
    resume: std::sync::mpsc::Sender<()>,
    completed: Option<tokio::sync::oneshot::Receiver<kuberic_runtime::Result<i64>>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl PausedCallback {
    async fn finish(mut self) -> kuberic_runtime::Result<i64> {
        self.resume.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(10), self.completed.take().unwrap())
            .await
            .unwrap()
            .unwrap();
        self.thread.take().unwrap().join().unwrap();
        result
    }
}

impl Drop for PausedCallback {
    fn drop(&mut self) {
        let _ = self.resume.send(());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

async fn pause_callback(pod: &PgPod, entered: PathBuf) -> PausedCallback {
    let primary = pod.runtime.primary_replicator().await.unwrap();
    let (paused, pause_complete) = tokio::sync::oneshot::channel();
    let (resume, resumed) = std::sync::mpsc::channel();
    let (completed, completion) = tokio::sync::oneshot::channel();
    let thread = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let old = tokio::spawn(async move { primary.current_progress().await });
                tokio::time::timeout(Duration::from_secs(5), async {
                    while !entered.exists() {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                })
                .await
                .unwrap();
                paused.send(()).unwrap();
                // No application lock is held: only delivery of the old helper
                // result is delayed while another executor replaces its generation.
                resumed.recv_timeout(Duration::from_secs(30)).unwrap();
                let _ = completed.send(old.await.unwrap());
            });
    });
    tokio::time::timeout(Duration::from_secs(5), pause_complete)
        .await
        .unwrap()
        .unwrap();
    PausedCallback {
        resume,
        completed: Some(completion),
        thread: Some(thread),
    }
}

async fn replace_access(pod: &PgPod, through_sf: bool) {
    if through_sf {
        pod.effect(RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::ReconfigurationPending,
            write: AccessStatus::ReconfigurationPending,
        })
        .await
        .unwrap();
        pod.effect(RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::Granted,
            write: AccessStatus::Granted,
        })
        .await
        .unwrap();
    } else {
        let access = PgAccessController::new(pod.application.instance());
        access.close_external().await.unwrap();
        access.grant_role_access().await.unwrap();
    }
}

async fn exercise_retired_error(through_sf: bool) {
    let root = TestDataDir::new("late-error");
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
    let pod = PgPod::with_bin(root.path().join("p"), native_identity(1, "old-error"), bin).await;
    pod.singleton().await;
    for round in 0..3 {
        std::fs::write(&armed, b"").unwrap();
        let old = pause_callback(&pod, entered.clone()).await;
        std::fs::remove_file(&armed).unwrap();
        replace_access(&pod, through_sf).await;
        assert!(
            pod.application.instance().is_running().await,
            "replacement was not running before old result: round={round} sf={through_sf} fault={:?}",
            pod.runtime.partition_report().await.reported_fault
        );
        let before = pod.runtime.partition_report().await.reported_fault;
        let (sql, _) = pod
            .application
            .instance()
            .connect_application()
            .await
            .unwrap();
        sql.simple_query("SELECT 1").await.unwrap();
        assert!(old.finish().await.is_err());
        assert!(pod.application.instance().is_running().await);
        sql.simple_query("SELECT 1").await.unwrap();
        assert_eq!(pod.runtime.partition_report().await.reported_fault, before);
        std::fs::remove_file(&entered).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retired_helper_error_preserves_direct_regrant_and_fault_status() {
    exercise_retired_error(false).await;
}

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn retired_helper_error_preserves_acknowledged_sf_regrant_and_fault_status() {
    exercise_retired_error(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn derived_helper_error_is_revalidated_after_a_replacement_race() {
    let root = TestDataDir::new("error-ready");
    let armed = root.path().join("bad-output");
    let bin = wrapped_pg_bin(
        root.path(),
        "pg_controldata",
        &format!(
            "#!/bin/sh\nif test -f '{}'; then\n echo malformed\nelse\n exec '{}/pg_controldata' \"$@\"\nfi\n",
            armed.display(),
            find_pg_bin().display()
        ),
    );
    let pod = PgPod::with_bin(
        root.path().join("p"),
        native_identity(1, "ready-error"),
        bin,
    )
    .await;
    pod.singleton().await;
    std::fs::write(&armed, b"").unwrap();
    let gate = pod.application.instance().pause_error_handling();
    let control = pod.runtime.primary_replicator().await.unwrap();
    let old = tokio::spawn(async move { control.current_progress().await });
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    std::fs::remove_file(armed).unwrap();
    replace_access(&pod, true).await;
    let before = pod.runtime.partition_report().await.reported_fault;
    let (sql, _) = pod
        .application
        .instance()
        .connect_application()
        .await
        .unwrap();
    gate.release.notify_one();
    let result = old.await.unwrap().unwrap_err();
    assert!(matches!(
        result,
        kuberic_runtime::RuntimeError::OperationCancelled
    ));
    sql.simple_query("SELECT 1").await.unwrap();
    assert!(pod.application.instance().is_running().await);
    assert_eq!(pod.runtime.partition_report().await.reported_fault, before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn old_service_error_does_not_affect_reopened_store_or_new_partition_faults() {
    let root = TestDataDir::new("error-open");
    let armed = root.path().join("bad-output");
    let bin = wrapped_pg_bin(
        root.path(),
        "pg_controldata",
        &format!(
            "#!/bin/sh\nif test -f '{}'; then\n echo malformed\nelse\n exec '{}/pg_controldata' \"$@\"\nfi\n",
            armed.display(),
            find_pg_bin().display()
        ),
    );
    let pod = PgPod::with_bin(root.path().join("p"), native_identity(1, "reopen"), bin).await;
    pod.singleton().await;
    std::fs::write(&armed, b"").unwrap();
    let instance = pod.application.instance().clone();
    let gate = instance.pause_error_handling();
    let control = pod.runtime.primary_replicator().await.unwrap();
    let old = tokio::spawn(async move { control.current_progress().await });
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    std::fs::remove_file(armed).unwrap();
    // The old service retains its metadata owner until its callback returns,
    // but the old error must be discarded against its retired process first.
    instance.stop().await.unwrap();
    gate.release.notify_one();
    assert!(old.await.unwrap().is_err());
    let reopened = pod.reopen().await;
    reopened.singleton().await;
    let before = reopened.application.native_driver().durable_state().await;
    let (sql, _) = reopened
        .application
        .instance()
        .connect_application()
        .await
        .unwrap();
    instance.stop().await.unwrap();
    sql.simple_query("SELECT 1").await.unwrap();
    assert_eq!(
        reopened.runtime.partition_report().await.reported_fault,
        None
    );
    assert_eq!(
        reopened
            .application
            .native_driver()
            .durable_state()
            .await
            .identity,
        before.identity
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn current_generation_malformed_control_data_remains_fatal_and_acknowledged() {
    let root = TestDataDir::new("live-error");
    let armed = root.path().join("bad-output");
    let bin = wrapped_pg_bin(
        root.path(),
        "pg_controldata",
        &format!(
            "#!/bin/sh\nif test -f '{}'; then\n echo malformed\nelse\n exec '{}/pg_controldata' \"$@\"\nfi\n",
            armed.display(),
            find_pg_bin().display()
        ),
    );
    let pod = PgPod::with_bin(root.path().join("p"), native_identity(1, "fatal"), bin).await;
    pod.singleton().await;
    let (sql, _) = pod
        .application
        .instance()
        .connect_application()
        .await
        .unwrap();
    let owned = ProcessProbe::postgres(pod.application.instance().data_dir());
    std::fs::write(&armed, b"").unwrap();
    assert!(
        pod.effect(RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::Granted,
            write: AccessStatus::Granted,
        })
        .await
        .is_err()
    );
    assert_eq!(
        pod.runtime.partition_report().await.reported_fault,
        Some(FaultType::Permanent)
    );
    assert_eq!(
        pod.runtime.partition_report().await.write_status,
        AccessStatus::ReconfigurationPending
    );
    assert!(!pod.application.instance().is_running().await);
    assert!(sql.simple_query("SELECT 1").await.is_err());
    owned.assert_reaped();
    std::fs::remove_file(armed).unwrap();
    assert!(
        pod.runtime
            .primary_replicator()
            .await
            .unwrap()
            .current_progress()
            .await
            .is_err()
    );
    assert_eq!(
        pod.runtime.partition_report().await.reported_fault,
        Some(FaultType::Permanent)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fault_sink_rechecks_queued_generation_and_preserves_current_fatal_reports() {
    for replace in [false, true] {
        let root = TestDataDir::new("fault-origin");
        let pod = PgPod::new(root.path().join("p"), native_identity(1, "sink")).await;
        pod.singleton().await;
        let instance = pod.application.instance();
        let gate = instance.pause_fault_delivery();
        ProcessProbe::postgres(instance.data_dir()).signal(rustix::process::Signal::QUIT);
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        assert_eq!(pod.runtime.partition_report().await.reported_fault, None);
        if replace {
            let (faults, _receiver) = tokio::sync::mpsc::channel(8);
            instance.start_native(faults).await.unwrap();
            PgAccessController::new(instance)
                .grant_role_access()
                .await
                .unwrap();
        }
        gate.release.notify_one();
        if replace {
            // Barrier through a second generation-tagged report: its delivery
            // follows the retired notice on the same sequential fault sink.
            let current = instance.pause_fault_delivery();
            ProcessProbe::postgres(instance.data_dir()).signal(rustix::process::Signal::QUIT);
            tokio::time::timeout(Duration::from_secs(5), current.entered.notified())
                .await
                .unwrap();
            assert_eq!(pod.runtime.partition_report().await.reported_fault, None);
            current.release.notify_one();
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while pod.runtime.partition_report().await.reported_fault != Some(FaultType::Permanent)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
