//! The commit barrier must make a locally committed transaction impossible
//! unless it reached durable quorum first.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use serial_test::serial;
use sqlite_replicated::barrier::barrier;
use sqlite_replicated::state::SqliteState;

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

fn count(state: &SqliteState) -> i64 {
    let (_, rows) = state
        .query_sql("SELECT count(*) FROM t", &[])
        .expect("query");
    rows[0][0].as_i64().expect("count")
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn a_replicated_write_is_visible_and_durable() {
    let dir = tempfile::tempdir().expect("tempdir");
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
