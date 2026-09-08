use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rusqlite::{Connection, OpenFlags};
use sqlite_commit_barrier::{BarrierError, CommitBarrier, Transaction, register};

#[derive(Default)]
struct RecordingBarrier {
    accept: AtomicBool,
    published: AtomicUsize,
    last: Mutex<Vec<u8>>,
    last_pages: AtomicUsize,
}

impl RecordingBarrier {
    fn accepting() -> Arc<Self> {
        let barrier = Arc::new(Self::default());
        barrier.accept.store(true, Ordering::SeqCst);
        barrier
    }
}

impl CommitBarrier for RecordingBarrier {
    fn publish(&self, transaction: &Transaction<'_>) -> Result<(), BarrierError> {
        if !self.accept.load(Ordering::SeqCst) {
            return Err(BarrierError::new("rejected by test"));
        }
        self.published.fetch_add(1, Ordering::SeqCst);
        self.last_pages
            .store(transaction.database_pages as usize, Ordering::SeqCst);
        *self.last.lock().unwrap() = transaction.frames.to_vec();
        Ok(())
    }
}

fn open(path: &std::path::Path, vfs: &str) -> Connection {
    let connection = Connection::open_with_flags_and_vfs(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        vfs,
    )
    .expect("open");
    connection
        .pragma_update(None, "locking_mode", "EXCLUSIVE")
        .expect("locking_mode");
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .expect("journal_mode");
    connection
        .pragma_update(None, "synchronous", "FULL")
        .expect("synchronous");
    connection
}

fn row_count(connection: &Connection) -> i64 {
    connection
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .expect("count")
}

#[test]
fn an_accepted_transaction_is_visible_and_survives_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("accept.sqlite");
    let barrier = RecordingBarrier::accepting();
    register("barrier-accept", barrier.clone()).expect("register");

    {
        let connection = open(&path, "barrier-accept");
        connection
            .execute_batch("CREATE TABLE t(v INTEGER); INSERT INTO t VALUES (1);")
            .expect("write");
        assert_eq!(row_count(&connection), 1);
    }

    assert!(barrier.published.load(Ordering::SeqCst) > 0);
    assert!(barrier.last_pages.load(Ordering::SeqCst) > 0);
    assert!(!barrier.last.lock().unwrap().is_empty());

    let reopened = open(&path, "barrier-accept");
    assert_eq!(row_count(&reopened), 1);
}

#[test]
fn a_rejected_transaction_is_not_recoverable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("reject.sqlite");
    let barrier = RecordingBarrier::accepting();
    register("barrier-reject", barrier.clone()).expect("register");

    {
        let connection = open(&path, "barrier-reject");
        connection
            .execute_batch("CREATE TABLE t(v INTEGER); INSERT INTO t VALUES (1);")
            .expect("seed");
        assert_eq!(row_count(&connection), 1);

        barrier.accept.store(false, Ordering::SeqCst);
        let rejected = connection.execute("INSERT INTO t VALUES (2)", []);
        assert!(
            rejected.is_err(),
            "commit must fail when the barrier rejects"
        );

        barrier.accept.store(true, Ordering::SeqCst);
    }

    let reopened = open(&path, "barrier-reject");
    assert_eq!(
        row_count(&reopened),
        1,
        "a rejected transaction must not survive"
    );
}

#[test]
fn a_rejected_transaction_does_not_block_later_writes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("resume.sqlite");
    let barrier = RecordingBarrier::accepting();
    register("barrier-resume", barrier.clone()).expect("register");

    let connection = open(&path, "barrier-resume");
    connection
        .execute_batch("CREATE TABLE t(v INTEGER);")
        .expect("create");

    barrier.accept.store(false, Ordering::SeqCst);
    assert!(connection.execute("INSERT INTO t VALUES (1)", []).is_err());

    barrier.accept.store(true, Ordering::SeqCst);
    connection
        .execute("INSERT INTO t VALUES (2)", [])
        .expect("later write");

    assert_eq!(row_count(&connection), 1);
    let value: i64 = connection
        .query_row("SELECT v FROM t", [], |row| row.get(0))
        .expect("value");
    assert_eq!(value, 2);
}

#[test]
fn every_committed_transaction_reaches_the_barrier() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("count.sqlite");
    let barrier = RecordingBarrier::accepting();
    register("barrier-count", barrier.clone()).expect("register");

    let connection = open(&path, "barrier-count");
    connection
        .execute_batch("CREATE TABLE t(v INTEGER);")
        .expect("create");
    let after_create = barrier.published.load(Ordering::SeqCst);

    for value in 0..5 {
        connection
            .execute("INSERT INTO t VALUES (?1)", [value])
            .expect("insert");
    }

    assert_eq!(
        barrier.published.load(Ordering::SeqCst) - after_create,
        5,
        "each commit must be offered to the barrier exactly once"
    );
    assert_eq!(row_count(&connection), 5);
}

#[test]
fn a_multi_statement_transaction_is_published_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("batch.sqlite");
    let barrier = RecordingBarrier::accepting();
    register("barrier-batch", barrier.clone()).expect("register");

    let connection = open(&path, "barrier-batch");
    connection
        .execute_batch("CREATE TABLE t(v INTEGER);")
        .expect("create");
    let after_create = barrier.published.load(Ordering::SeqCst);

    connection
        .execute_batch("BEGIN; INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); COMMIT;")
        .expect("batch");

    assert_eq!(
        barrier.published.load(Ordering::SeqCst) - after_create,
        1,
        "one transaction must produce one publish"
    );
    assert_eq!(row_count(&connection), 2);
}

#[test]
fn a_rolled_back_transaction_is_never_published() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("rollback.sqlite");
    let barrier = RecordingBarrier::accepting();
    register("barrier-rollback", barrier.clone()).expect("register");

    let connection = open(&path, "barrier-rollback");
    connection
        .execute_batch("CREATE TABLE t(v INTEGER);")
        .expect("create");
    let after_create = barrier.published.load(Ordering::SeqCst);

    connection
        .execute_batch("BEGIN; INSERT INTO t VALUES (1); ROLLBACK;")
        .expect("rollback");

    assert_eq!(barrier.published.load(Ordering::SeqCst), after_create);
    assert_eq!(row_count(&connection), 0);
}
