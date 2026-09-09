//! The commit barrier must make a locally committed transaction impossible
//! unless it reached durable quorum first.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use kuberic_core::handles::{PartitionHandle, PartitionState};
use kuberic_core::types::{AccessStatus, FaultType};
use serial_test::serial;
use sqlite_commit_barrier::CommitBarrier;
use sqlite_replicated::barrier::barrier;
use sqlite_replicated::server::SqliteServer;
use sqlite_replicated::state::SqliteState;
use tokio::sync::mpsc;

struct Sink {
    accept: Arc<AtomicBool>,
    lsn: Arc<AtomicI64>,
    replicated: Arc<AtomicI64>,
}

fn sink() -> Sink {
    Sink {
        accept: Arc::new(AtomicBool::new(true)),
        lsn: Arc::new(AtomicI64::new(0)),
        replicated: Arc::new(AtomicI64::new(0)),
    }
}

fn install(sink: &Sink) {
    let accept = sink.accept.clone();
    let lsn = sink.lsn.clone();
    let replicated = sink.replicated.clone();
    barrier().install_sink(move |payload| {
        if !accept.load(Ordering::SeqCst) {
            return Err("quorum unavailable".to_string());
        }
        assert!(!payload.is_empty(), "a published commit must carry frames");
        replicated.fetch_add(1, Ordering::SeqCst);
        Ok(lsn.fetch_add(1, Ordering::SeqCst) + 1)
    });
}

async fn primary(dir: &std::path::Path) -> SqliteState {
    let mut state = SqliteState::open(dir.to_path_buf()).await.expect("open");
    state.open_as_primary().expect("open as primary");
    state
}

/// The barrier is process-global, so each test rebinds it to its own directory.
fn fresh(dir: &std::path::Path) {
    barrier().arm(dir.to_path_buf(), mpsc::channel(1).0);
    barrier().clear_fence_after_rebuild(dir);
}

fn count(state: &SqliteState) -> i64 {
    let (_, rows) = state
        .query_sql("SELECT count(*) FROM t", &[])
        .expect("query");
    rows[0][0].as_i64().expect("count")
}

/// Issue #42 asks for a fault injected after the local commit and before
/// quorum completes. The process cannot be killed inside a test, so the data
/// directory is copied at the moment the barrier is holding the transaction,
/// which is exactly what a crashed primary would have left on disk.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn a_crash_while_awaiting_quorum_leaves_nothing_behind() {
    let dir = tempfile::tempdir().expect("tempdir");
    let salvage = dir.path().join("salvage");
    std::fs::create_dir_all(&salvage).expect("mkdir");
    fresh(dir.path());

    let sink = sink();
    let captured = Arc::new(AtomicBool::new(false));
    {
        let source = dir.path().to_path_buf();
        let target = salvage.clone();
        let captured = captured.clone();
        let lsn = sink.lsn.clone();
        let replicated = sink.replicated.clone();
        let accept = sink.accept.clone();
        barrier().install_sink(move |_payload| {
            if !accept.load(Ordering::SeqCst) {
                for name in ["db.sqlite", "db.sqlite-wal"] {
                    let from = source.join(name);
                    if from.exists() {
                        std::fs::copy(&from, target.join(name)).expect("copy");
                    }
                }
                captured.store(true, Ordering::SeqCst);
                return Err("crashed before quorum".to_string());
            }
            replicated.fetch_add(1, Ordering::SeqCst);
            Ok(lsn.fetch_add(1, Ordering::SeqCst) + 1)
        });
    }

    let mut state = primary(dir.path()).await;
    state
        .execute_batch_sql(&["CREATE TABLE t(v INTEGER)".to_string()])
        .expect("create");
    state
        .execute_sql("INSERT INTO t VALUES (1)", &[])
        .expect("replicated insert");

    sink.accept.store(false, Ordering::SeqCst);
    assert!(state.execute_sql("INSERT INTO t VALUES (2)", &[]).is_err());
    assert!(
        captured.load(Ordering::SeqCst),
        "the barrier must have held the transaction"
    );

    sink.accept.store(true, Ordering::SeqCst);
    install(&sink);

    let recovered = primary(&salvage).await;
    assert_eq!(
        count(&recovered),
        1,
        "a transaction still awaiting quorum must not survive a crash"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn a_replicated_write_is_visible_and_durable() {
    let dir = tempfile::tempdir().expect("tempdir");
    fresh(dir.path());
    let sink = sink();
    install(&sink);

    {
        let mut state = primary(dir.path()).await;
        state
            .execute_batch_sql(&["CREATE TABLE t(v INTEGER)".to_string()])
            .expect("create");
        state
            .execute_sql("INSERT INTO t VALUES (1)", &[])
            .expect("insert");
        assert_eq!(count(&state), 1);
        state.close();
    }

    assert!(sink.replicated.load(Ordering::SeqCst) >= 2);

    let reopened = primary(dir.path()).await;
    assert_eq!(count(&reopened), 1);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn a_write_that_loses_quorum_never_commits_locally() {
    let dir = tempfile::tempdir().expect("tempdir");
    fresh(dir.path());
    let sink = sink();
    install(&sink);

    {
        let mut state = primary(dir.path()).await;
        state
            .execute_batch_sql(&["CREATE TABLE t(v INTEGER)".to_string()])
            .expect("create");

        sink.accept.store(false, Ordering::SeqCst);
        let rejected = state.execute_sql("INSERT INTO t VALUES (1)", &[]);
        assert!(
            rejected.is_err(),
            "a write must fail when quorum is unavailable"
        );

        sink.accept.store(true, Ordering::SeqCst);
        assert_eq!(
            count(&state),
            0,
            "an unreplicated write must not be visible on the primary"
        );
        state.close();
    }

    let reopened = primary(dir.path()).await;
    assert_eq!(
        count(&reopened),
        0,
        "an unreplicated write must not survive a restart"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn an_unreplicated_write_cannot_escape_through_a_snapshot() {
    let dir = tempfile::tempdir().expect("tempdir");
    fresh(dir.path());
    let sink = sink();
    install(&sink);

    let mut state = primary(dir.path()).await;
    state
        .execute_batch_sql(&["CREATE TABLE t(v INTEGER)".to_string()])
        .expect("create");

    sink.accept.store(false, Ordering::SeqCst);
    assert!(state.execute_sql("INSERT INTO t VALUES (1)", &[]).is_err());
    sink.accept.store(true, Ordering::SeqCst);

    let snapshot = state.snapshot_db().expect("snapshot");
    let restored = dir.path().join("restored");
    tokio::fs::create_dir_all(&restored).await.expect("mkdir");
    let mut copy = SqliteState::open(restored.clone())
        .await
        .expect("open copy");
    copy.restore_from_snapshot(&snapshot)
        .await
        .expect("restore");
    copy.open_as_primary().expect("open restored");

    assert_eq!(
        count(&copy),
        0,
        "a snapshot must not carry an unreplicated write to another replica"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn a_primary_without_a_barrier_cannot_write() {
    let dir = tempfile::tempdir().expect("tempdir");
    fresh(dir.path());
    let sink = sink();
    install(&sink);

    let mut state = primary(dir.path()).await;
    state
        .execute_batch_sql(&["CREATE TABLE t(v INTEGER)".to_string()])
        .expect("create");

    barrier().uninstall();
    assert!(
        state.execute_sql("INSERT INTO t VALUES (1)", &[]).is_err(),
        "a demoted replica must not commit"
    );

    install(&sink);
    assert_eq!(count(&state), 0);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn a_batch_reaches_quorum_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    fresh(dir.path());
    let sink = sink();
    install(&sink);

    let mut state = primary(dir.path()).await;
    state
        .execute_batch_sql(&["CREATE TABLE t(v INTEGER)".to_string()])
        .expect("create");
    let before = sink.replicated.load(Ordering::SeqCst);

    state
        .execute_batch_sql(&[
            "INSERT INTO t VALUES (1)".to_string(),
            "INSERT INTO t VALUES (2)".to_string(),
        ])
        .expect("batch");

    assert_eq!(
        sink.replicated.load(Ordering::SeqCst) - before,
        1,
        "a batch must replicate as one transaction"
    );
    assert_eq!(count(&state), 2);
}

/// A replica missing a transaction the cluster kept must stop serving entirely.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn a_replica_that_loses_a_committed_transaction_is_fenced_until_rebuild() {
    let dir = tempfile::tempdir().expect("tempdir");
    fresh(dir.path());
    let (fault_tx, mut fault_rx) = mpsc::channel(4);
    barrier().arm(dir.path().to_path_buf(), fault_tx);
    let sink = sink();
    install(&sink);

    let mut state = primary(dir.path()).await;
    state
        .execute_batch_sql(&["CREATE TABLE t(v INTEGER)".to_string()])
        .expect("create");
    state
        .execute_sql("INSERT INTO t VALUES (1)", &[])
        .expect("insert");
    let snapshot = state.snapshot_db().expect("snapshot");
    state.close();

    CommitBarrier::abandon(&**barrier(), "write failed after quorum");

    assert!(barrier().is_fenced(), "the barrier must fence the replica");
    assert!(
        sqlite_replicated::barrier::is_fenced_on_disk(dir.path()),
        "the fence must survive a restart"
    );
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(5), fault_rx.recv())
            .await
            .expect("a permanent fault must be reported"),
        Some(FaultType::Permanent)
    );

    let granted = Arc::new(PartitionState::new());
    granted.set_read_status(AccessStatus::Granted);
    granted.set_write_status(AccessStatus::Granted);
    let partition = Arc::new(PartitionHandle::new(granted, mpsc::channel(1).0));
    let server = SqliteServer::new(
        Arc::new(tokio::sync::Mutex::new(
            SqliteState::open(dir.path().to_path_buf())
                .await
                .expect("open"),
        )),
        partition,
    );
    assert!(
        server.check_read_access().is_err(),
        "a fenced replica must not serve reads even while access is granted"
    );
    assert!(
        server.check_write_access().is_err(),
        "a fenced replica must not serve writes even while access is granted"
    );

    let mut reopened = SqliteState::open(dir.path().to_path_buf())
        .await
        .expect("open");
    assert!(
        reopened.open_as_primary().is_err(),
        "a fenced replica must not be reusable as primary"
    );

    reopened
        .restore_from_snapshot(&snapshot)
        .await
        .expect("rebuild");

    assert!(!barrier().is_fenced(), "a rebuild must clear the fence");
    assert!(!sqlite_replicated::barrier::is_fenced_on_disk(dir.path()));

    install(&sink);
    reopened
        .open_as_primary()
        .expect("a rebuilt replica must serve again");
    reopened
        .execute_sql("INSERT INTO t VALUES (2)", &[])
        .expect("write after rebuild");
    assert_eq!(count(&reopened), 2);
}
