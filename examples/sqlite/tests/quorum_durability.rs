use std::path::{Path, PathBuf};

use sqlite_replicated::state::SqliteState;

fn scratch_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("sqlite-durability")
        .join(format!("{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn crash_snapshot(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for name in ["db.sqlite", "db.sqlite-wal", "meta.json"] {
        let src = from.join(name);
        if src.exists() {
            std::fs::copy(&src, to.join(name)).unwrap();
        }
    }
}

fn column(rows: &[Vec<serde_json::Value>]) -> Vec<String> {
    rows.iter()
        .map(|row| row[0].as_str().unwrap_or_default().to_string())
        .collect()
}

async fn seed_confirmed(dir: &Path) -> SqliteState {
    let mut state = SqliteState::open(dir.to_path_buf()).await.unwrap();
    state.open_as_primary().unwrap();

    state
        .execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &[])
        .unwrap();
    let (_, offset) = state
        .capture_wal_frames()
        .unwrap()
        .expect("schema change produces frames");
    state.mark_confirmed(1, offset).await.unwrap();

    state
        .execute_sql("INSERT INTO t VALUES (1, 'confirmed')", &[])
        .unwrap();
    let (_, offset) = state
        .capture_wal_frames()
        .unwrap()
        .expect("insert produces frames");
    state.mark_confirmed(2, offset).await.unwrap();

    state
}

#[tokio::test]
async fn unconfirmed_commit_is_discarded_on_restart() {
    let dir = scratch_dir("unconfirmed");
    let state = seed_confirmed(&dir).await;

    state
        .execute_sql("INSERT INTO t VALUES (2, 'unconfirmed')", &[])
        .unwrap();

    let recovered_dir = scratch_dir("unconfirmed-recovered");
    crash_snapshot(&dir, &recovered_dir);

    let mut recovered = SqliteState::open(recovered_dir).await.unwrap();
    recovered.open_as_primary().unwrap();
    let (_, rows) = recovered
        .query_sql("SELECT v FROM t ORDER BY id", &[])
        .unwrap();

    assert_eq!(
        column(&rows),
        vec!["confirmed".to_string()],
        "a locally committed write that never reached quorum must not survive restart"
    );
    assert_eq!(recovered.committed_lsn, 2);
}

#[tokio::test]
async fn confirmed_commit_survives_restart() {
    let dir = scratch_dir("confirmed");
    let mut state = seed_confirmed(&dir).await;

    state
        .execute_sql("INSERT INTO t VALUES (2, 'also confirmed')", &[])
        .unwrap();
    let (_, offset) = state
        .capture_wal_frames()
        .unwrap()
        .expect("insert produces frames");
    state.mark_confirmed(3, offset).await.unwrap();

    let recovered_dir = scratch_dir("confirmed-recovered");
    crash_snapshot(&dir, &recovered_dir);

    let mut recovered = SqliteState::open(recovered_dir).await.unwrap();
    recovered.open_as_primary().unwrap();
    let (_, rows) = recovered
        .query_sql("SELECT v FROM t ORDER BY id", &[])
        .unwrap();

    assert_eq!(
        column(&rows),
        vec!["confirmed".to_string(), "also confirmed".to_string()],
        "recovery must not discard writes that reached quorum"
    );
    assert_eq!(recovered.committed_lsn, 3);
}

#[tokio::test]
async fn capture_resumes_after_wal_restart() {
    let dir = scratch_dir("wal-restart");
    let mut state = seed_confirmed(&dir).await;

    state.close();
    state.open_as_primary().unwrap();

    state
        .execute_sql("INSERT INTO t VALUES (2, 'after restart')", &[])
        .unwrap();

    let captured = state.capture_wal_frames().unwrap();
    assert!(
        captured.is_some(),
        "frames written after a WAL restart must still be captured"
    );
}

#[tokio::test]
async fn unconfirmed_commit_after_checkpoint_is_discarded() {
    let dir = scratch_dir("unconfirmed-after-checkpoint");
    let mut state = seed_confirmed(&dir).await;

    state.close();
    state.open_as_primary().unwrap();

    state
        .execute_sql("INSERT INTO t VALUES (2, 'unconfirmed')", &[])
        .unwrap();

    let recovered_dir = scratch_dir("unconfirmed-after-checkpoint-recovered");
    crash_snapshot(&dir, &recovered_dir);

    let mut recovered = SqliteState::open(recovered_dir).await.unwrap();
    recovered.open_as_primary().unwrap();
    let (_, rows) = recovered
        .query_sql("SELECT v FROM t ORDER BY id", &[])
        .unwrap();

    assert_eq!(
        column(&rows),
        vec!["confirmed".to_string()],
        "an unconfirmed write in a post-checkpoint WAL generation must not survive"
    );
}
