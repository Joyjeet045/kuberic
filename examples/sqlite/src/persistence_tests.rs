use std::fs::{self, OpenOptions};
use std::io::Write;

use futures::TryStreamExt;
use kuberic_runtime::application::{CopyChunk, DurableApplicationProgress, Operation};
use kuberic_runtime::engine::DurableState;
use kuberic_runtime::protocol::types::OperationId;
use rusqlite::Connection;
use tempfile::TempDir;

use crate::frames::{WalFrameSet, frames_from_wal_bytes};
use crate::{RecoveryState, SqlitePersistence};

fn directory() -> TempDir {
    let scratch = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/sqlite-unit");
    fs::create_dir_all(&scratch).unwrap();
    tempfile::tempdir_in(scratch).unwrap()
}

fn open(dir: &TempDir) -> SqlitePersistence {
    SqlitePersistence::open(dir.path().to_owned()).unwrap()
}

#[test]
fn startup_freshness_does_not_confuse_empty_sql_state_with_missing_metadata() {
    let root = directory();
    let application = root.path().join("application");
    assert!(SqlitePersistence::is_fresh_empty(&application).unwrap());
    let state = SqlitePersistence::open(application.clone()).unwrap();
    assert!(!SqlitePersistence::is_fresh_empty(&application).unwrap());
    drop(state);
    std::fs::remove_file(application.join("state-v2.json")).unwrap();
    assert!(!SqlitePersistence::is_fresh_empty(&application).unwrap());
    assert!(SqlitePersistence::open(application).is_err());
}

fn progress(applied_lsn: i64, committed_lsn: i64) -> DurableApplicationProgress {
    DurableApplicationProgress {
        applied_lsn,
        committed_lsn,
    }
}

/// Capture real SQLite WAL transactions (schema, multi-page insert, schema change).
fn operations() -> Vec<Operation> {
    let dir = directory();
    let conn = Connection::open(dir.path().join("producer.sqlite")).unwrap();
    conn.execute_batch(
        "PRAGMA page_size=512; PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;",
    )
    .unwrap();
    let mut offset = 0;
    [
        "CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)",
        "INSERT INTO data VALUES(1, printf('%02000d',1))",
        "ALTER TABLE data ADD COLUMN extra TEXT; INSERT INTO data(id,value) VALUES(2,'second')",
    ]
    .into_iter()
    .enumerate()
    .map(|(index, sql)| {
        conn.execute_batch(&format!("BEGIN; {sql}; COMMIT;"))
            .unwrap();
        let wal = fs::read(dir.path().join("producer.sqlite-wal")).unwrap();
        let frames = frames_from_wal_bytes(offset as u64, &wal[offset..], 512);
        offset = wal.len();
        let frame_set = WalFrameSet {
            checksum: WalFrameSet::compute_checksum(&frames),
            frames,
            db_size_pages: conn
                .pragma_query_value(None, "page_count", |r| r.get(0))
                .unwrap(),
        };
        frame_set.page_size().unwrap();
        Operation {
            lsn: index as i64 + 1,
            committed_lsn: index as i64,
            data: serde_json::to_vec(&frame_set).unwrap().into(),
        }
    })
    .collect()
}

#[tokio::test]
async fn shrinking_wal_transaction_retains_exact_bytes_and_reconstructs_final_image() {
    let producer_dir = directory();
    let producer_path = producer_dir.path().join("producer.sqlite");
    let wal_path = producer_dir.path().join("producer.sqlite-wal");
    let conn = Connection::open(&producer_path).unwrap();
    conn.execute_batch(
        "PRAGMA page_size=512;
         PRAGMA auto_vacuum=FULL;
         PRAGMA journal_mode=WAL;
         PRAGMA wal_autocheckpoint=0;
         PRAGMA cache_size=8;
         PRAGMA cache_spill=ON;",
    )
    .unwrap();
    let capture = |offset: usize, lsn, committed_lsn| {
        let wal = fs::read(&wal_path).unwrap();
        let frames = frames_from_wal_bytes(offset as u64, &wal[offset..], 512);
        let frame_set = WalFrameSet {
            checksum: WalFrameSet::compute_checksum(&frames),
            frames,
            db_size_pages: conn
                .pragma_query_value(None, "page_count", |row| row.get(0))
                .unwrap(),
        };
        let operation = Operation {
            lsn,
            committed_lsn,
            data: serde_json::to_vec(&frame_set).unwrap().into(),
        };
        (wal.len(), frame_set, operation)
    };
    conn.execute_batch(
        "BEGIN;
         CREATE TABLE data(id INTEGER PRIMARY KEY, value BLOB NOT NULL);
         WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<100)
         INSERT INTO data SELECT i, zeroblob(1800) FROM n;
         COMMIT;",
    )
    .unwrap();
    let (offset, initial_frames, initial) = capture(0, 1, 0);
    conn.execute_batch(
        "BEGIN;
         UPDATE data SET value=zeroblob(1801);
         DELETE FROM data;
         COMMIT;",
    )
    .unwrap();
    let (_, shrinking_frames, shrinking) = capture(offset, 2, 1);
    assert!(shrinking_frames.db_size_pages < initial_frames.db_size_pages);
    assert!(
        shrinking_frames
            .frames
            .iter()
            .any(|frame| frame.page_number > shrinking_frames.db_size_pages),
        "the real transaction must spill pages subsequently removed by auto-vacuum"
    );
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    let expected_image = fs::read(&producer_path).unwrap();
    assert_eq!(
        expected_image.len(),
        shrinking_frames.db_size_pages as usize * 512
    );

    let state_dir = directory();
    let state = open(&state_dir);
    state.apply(initial).await.unwrap();
    state.commit(1).await.unwrap();
    assert_eq!(rows(&state), 100);
    let original_snapshot = state.snapshot(1).unwrap();
    assert_eq!(
        state.apply(shrinking.clone()).await.unwrap(),
        progress(2, 1)
    );
    assert_eq!(
        state.apply(shrinking.clone()).await.unwrap(),
        progress(2, 1)
    );
    assert_eq!(rows(&state), 100);
    // Reopen while the shrinking transaction is still applied-only.
    drop(state);
    let state = open(&state_dir);
    assert_eq!(state.recovery_state().unwrap(), RecoveryState::Healthy);
    assert_eq!(state.durable_progress().await.unwrap(), progress(2, 1));
    assert!(state.verify_applied(&shrinking).await.unwrap());
    let retained = state
        .get_replication_operations(2, 2)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(retained, vec![shrinking]);
    state.commit(2).await.unwrap();
    assert_eq!(rows(&state), 0);
    assert_eq!(
        fs::read(state.materialize_committed().unwrap()).unwrap(),
        expected_image
    );
    assert_eq!(state.snapshot(1).unwrap(), original_snapshot);
    let snapshot = state.snapshot(2).unwrap();
    drop(state);
    let state = open(&state_dir);
    assert_eq!(state.snapshot(2).unwrap(), snapshot);

    let target_dir = directory();
    let target = open(&target_dir);
    let build = OperationId::new("shrinking-copy");
    stage(&target, &build, &snapshot).await;
    target.finish_copy(&build, 2, 2).await.unwrap();
    drop(target);
    let target = open(&target_dir);
    assert_eq!(target.durable_progress().await.unwrap(), progress(2, 2));
    assert_eq!(target.snapshot(2).unwrap(), snapshot);
    let materialized = target.materialize_committed().unwrap();
    assert_eq!(fs::read(&materialized).unwrap(), expected_image);
    let copied = Connection::open(materialized).unwrap();
    assert_eq!(
        copied
            .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
    assert_eq!(
        copied
            .query_row("SELECT COUNT(*) FROM data", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

fn rows(state: &SqlitePersistence) -> i64 {
    let conn = Connection::open(state.materialize_committed().unwrap()).unwrap();
    conn.query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0))
        .unwrap()
}

async fn stage(state: &SqlitePersistence, build: &OperationId, bytes: &[u8]) {
    let middle = bytes.len() / 2;
    for (index, chunk) in [&bytes[..middle], &bytes[middle..]].into_iter().enumerate() {
        state
            .apply_copy_chunk(
                build,
                index as u64 + 1,
                CopyChunk {
                    data: chunk.to_vec().into(),
                },
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn exact_apply_commit_restart_and_retained_ranges() {
    let dir = directory();
    let state = open(&dir);
    let ops = operations();
    assert!(state.commit(1).await.is_err());
    assert!(state.apply(ops[1].clone()).await.is_err());
    assert_eq!(state.apply(ops[0].clone()).await.unwrap(), progress(1, 0));
    assert_eq!(state.apply(ops[0].clone()).await.unwrap(), progress(1, 0));
    let mut conflicting = ops[0].clone();
    conflicting.data = [conflicting.data.as_ref(), b" "].concat().into();
    assert!(state.apply(conflicting).await.is_err());
    let mut watermark_conflict = ops[0].clone();
    watermark_conflict.committed_lsn = 1;
    assert!(!state.verify_applied(&watermark_conflict).await.unwrap());
    assert!(state.apply(watermark_conflict).await.is_err());
    state.apply(ops[1].clone()).await.unwrap();
    assert_eq!(rows(&state), 0);
    drop(state);
    let state = open(&dir);
    assert_eq!(state.durable_progress().await.unwrap(), progress(2, 1));
    assert!(state.verify_applied(&ops[1]).await.unwrap());
    let retained = state
        .get_replication_operations(1, 2)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(retained, ops[..2]);
    assert!(state.get_replication_operations(0, 2).await.is_err());
    assert!(state.get_replication_operations(1, 3).await.is_err());
    assert!(
        state
            .get_replication_operations(3, 2)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    state.commit(2).await.unwrap();
    assert_eq!(rows(&state), 1);
    drop(state);
    assert_eq!(rows(&open(&dir)), 1);
}

#[tokio::test]
async fn committed_snapshot_then_retained_catchup_preserves_uncommitted_suffix() {
    let source_dir = directory();
    let source = open(&source_dir);
    let ops = operations();
    source.apply(ops[0].clone()).await.unwrap();
    source.apply(ops[1].clone()).await.unwrap();
    assert!(source.snapshot(2).is_err());
    let bytes = source.snapshot(1).unwrap();
    source.apply(ops[2].clone()).await.unwrap();
    source.commit(3).await.unwrap();
    assert_eq!(bytes, source.snapshot(1).unwrap());
    drop(source);
    let source = open(&source_dir);
    assert_eq!(bytes, source.snapshot(1).unwrap());
    let target_dir = directory();
    let target = open(&target_dir);
    let build = OperationId::new("exact-copy");
    stage(&target, &build, &bytes).await;
    assert_eq!(target.durable_progress().await.unwrap(), progress(0, 0));
    assert_eq!(
        target.finish_copy(&build, 1, 1).await.unwrap(),
        progress(1, 1)
    );
    assert_eq!(rows(&target), 0);
    assert!(!target.verify_applied(&ops[1]).await.unwrap());
    assert!(target.snapshot(0).is_err());
    assert!(target.snapshot(4).is_err());
    let suffix = source
        .get_replication_operations(2, 2)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(suffix, ops[1..2]);
    target.apply(suffix[0].clone()).await.unwrap();
    assert_eq!(rows(&target), 0);
    assert!(target.snapshot(2).is_err());
    drop(target);
    let target = open(&target_dir);
    assert_eq!(target.durable_progress().await.unwrap(), progress(2, 1));
    target.commit(2).await.unwrap();
    assert_eq!(rows(&target), 1);
    let catchup = source
        .get_replication_operations(3, 3)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(catchup, ops[2..]);
    target.apply(catchup[0].clone()).await.unwrap();
    target.commit(3).await.unwrap();
    assert_eq!(rows(&target), 2);
    let conn = Connection::open(target.materialize_committed().unwrap()).unwrap();
    assert!(conn.prepare("SELECT extra FROM data").is_ok());
}

#[tokio::test]
async fn committed_boundary_zero_transfers_entire_applied_database_as_catchup() {
    let source_dir = directory();
    let source = open(&source_dir);
    let empty = source.snapshot(0).unwrap();
    let ops = operations();
    source.apply(ops[0].clone()).await.unwrap();
    assert_eq!(empty, source.snapshot(0).unwrap());
    assert!(source.snapshot(1).is_err());
    let suffix = source
        .get_replication_operations(1, 1)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let target_dir = directory();
    let target = open(&target_dir);
    let build = OperationId::new("empty-committed-base");
    stage(&target, &build, &empty).await;
    assert_eq!(
        target.finish_copy(&build, 0, 0).await.unwrap(),
        progress(0, 0)
    );
    target.apply(suffix[0].clone()).await.unwrap();
    assert_eq!(target.durable_progress().await.unwrap(), progress(1, 0));
    assert_eq!(
        fs::metadata(target.materialize_committed().unwrap())
            .unwrap()
            .len(),
        0
    );
    drop(target);
    let target = open(&target_dir);
    target.commit(1).await.unwrap();
    assert_eq!(rows(&target), 0);
}

#[tokio::test]
async fn copy_retries_incomplete_restart_install_cleanup_and_completion_conflicts() {
    let source_dir = directory();
    let source = open(&source_dir);
    let ops = operations();
    source.apply(ops[0].clone()).await.unwrap();
    source.apply(ops[1].clone()).await.unwrap();
    let bytes = source.snapshot(1).unwrap();
    let target_dir = directory();
    let target = open(&target_dir);
    let build = OperationId::new("../opaque-build-is-not-a-path");
    let split = bytes.len() / 2;
    let chunk = CopyChunk {
        data: bytes[..split].to_vec().into(),
    };
    assert!(
        target
            .apply_copy_chunk(&build, 2, chunk.clone())
            .await
            .is_err()
    );
    target
        .apply_copy_chunk(&build, 1, chunk.clone())
        .await
        .unwrap();
    target
        .apply_copy_chunk(&build, 1, chunk.clone())
        .await
        .unwrap();
    assert!(
        target
            .apply_copy_chunk(
                &build,
                1,
                CopyChunk {
                    data: b"conflict".to_vec().into()
                }
            )
            .await
            .is_err()
    );
    assert!(target.finish_copy(&build, 1, 1).await.is_err());
    drop(target);
    let target = open(&target_dir);
    assert_eq!(target.durable_progress().await.unwrap(), progress(0, 0));
    assert!(target.verify_copy_chunk(&build, 1, &chunk).await.unwrap());
    target
        .apply_copy_chunk(
            &build,
            2,
            CopyChunk {
                data: bytes[split..].to_vec().into(),
            },
        )
        .await
        .unwrap();
    assert!(target.finish_copy(&build, 2, 2).await.is_err());
    assert!(target.finish_copy(&build, 2, 1).await.is_err());
    target.finish_copy(&build, 1, 1).await.unwrap();
    // Crash after application install but before agent acknowledgement.
    drop(target);
    let target = open(&target_dir);
    assert_eq!(
        target.finish_copy(&build, 1, 1).await.unwrap(),
        progress(1, 1)
    );
    assert!(target.verify_copy_chunk(&build, 1, &chunk).await.unwrap());
    target.apply(ops[1].clone()).await.unwrap();
    target.apply(ops[2].clone()).await.unwrap();
    target.commit(3).await.unwrap();
    assert_eq!(
        target.finish_copy(&build, 1, 1).await.unwrap(),
        progress(3, 3)
    );
    assert!(target.finish_copy(&build, 2, 2).await.is_err());
    assert!(target.finish_copy(&build, 1, 0).await.is_err());
    assert_eq!(rows(&target), 2);
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(target_dir.path().join("state-v2.json")).unwrap())
            .unwrap();
    assert!(manifest["state"]["staging"].as_object().unwrap().is_empty());
    assert!(!target_dir.path().join("history-0.log").exists());
    drop(target);
    let target = open(&target_dir);
    assert_eq!(
        target.finish_copy(&build, 1, 1).await.unwrap(),
        progress(3, 3)
    );
    assert!(target.finish_copy(&build, 2, 0).await.is_err());
    assert_eq!(rows(&target), 2);
}

#[tokio::test]
async fn unrecorded_torn_tail_is_discarded_but_acknowledged_corruption_requires_rebuild() {
    let ops = operations();
    for corrupt in [false, true] {
        let dir = directory();
        let state = open(&dir);
        state.apply(ops[0].clone()).await.unwrap();
        state.apply(ops[1].clone()).await.unwrap();
        state.commit(2).await.unwrap();
        drop(state);
        let history = dir.path().join("history-0.log");
        if corrupt {
            let mut bytes = fs::read(&history).unwrap();
            bytes[8] ^= 1;
            fs::write(&history, bytes).unwrap();
        } else {
            let recorded_len = fs::metadata(&history).unwrap().len();
            OpenOptions::new()
                .append(true)
                .open(&history)
                .unwrap()
                .write_all(b"torn unaccepted")
                .unwrap();
            let reopened = open(&dir);
            assert_eq!(reopened.recovery_state().unwrap(), RecoveryState::Healthy);
            assert_eq!(fs::metadata(&history).unwrap().len(), recorded_len);
            assert_eq!(rows(&reopened), 1);
            drop(reopened);
            OpenOptions::new()
                .write(true)
                .open(&history)
                .unwrap()
                .set_len(recorded_len - 1)
                .unwrap();
        }
        let state = open(&dir);
        assert_eq!(state.durable_progress().await.unwrap(), progress(2, 2));
        assert!(matches!(
            state.recovery_state().unwrap(),
            RecoveryState::RebuildRequired(_)
        ));
        assert!(state.commit(2).await.is_err());
        assert!(state.apply(ops[2].clone()).await.is_err());
        assert!(state.snapshot(2).is_err());
        assert!(state.materialize_committed().is_err());
        assert!(state.complete_reconciliation().is_err());
        drop(state);
        let state = open(&dir);
        assert!(matches!(
            state.recovery_state().unwrap(),
            RecoveryState::RebuildRequired(_)
        ));
        let source_dir = directory();
        let source = open(&source_dir);
        for op in &ops[..2] {
            source.apply(op.clone()).await.unwrap();
        }
        let bytes = source.snapshot(1).unwrap();
        let build = OperationId::new("rebuild");
        stage(&state, &build, &bytes).await;
        state.finish_copy(&build, 1, 1).await.unwrap();
        assert!(matches!(
            state.recovery_state().unwrap(),
            RecoveryState::Rebuilding { .. }
        ));
        assert!(state.materialize_committed().is_err());
        assert!(state.complete_reconciliation().is_err());
        drop(state);
        let state = open(&dir);
        assert!(matches!(
            state.recovery_state().unwrap(),
            RecoveryState::Rebuilding { .. }
        ));
        state.apply(ops[1].clone()).await.unwrap();
        assert!(state.materialize_committed().is_err());
        assert!(
            state
                .require_reconciliation("cannot override rebuilding".into())
                .is_err()
        );
        state.commit(2).await.unwrap();
        assert_eq!(state.recovery_state().unwrap(), RecoveryState::Healthy);
        assert_eq!(rows(&state), 1);
    }
}

#[tokio::test]
async fn reconciliation_and_stale_sqlite_companions() {
    let dir = directory();
    let state = open(&dir);
    let ops = operations();
    for op in &ops[..2] {
        state.apply(op.clone()).await.unwrap();
    }
    state
        .require_reconciliation("dispatched write outcome unknown".into())
        .unwrap();
    drop(state);
    let state = open(&dir);
    assert!(matches!(
        state.recovery_state().unwrap(),
        RecoveryState::ReconciliationRequired(_)
    ));
    // Authority recovery commits the original bytes, not a replacement transaction.
    state.commit(2).await.unwrap();
    for name in ["db.sqlite-wal", "db.sqlite-shm", "db.sqlite"] {
        fs::write(dir.path().join(name), b"stale").unwrap();
    }
    state.complete_reconciliation().unwrap();
    assert_eq!(state.recovery_state().unwrap(), RecoveryState::Healthy);
    assert!(!dir.path().join("db.sqlite-wal").exists());
    assert!(!dir.path().join("db.sqlite-shm").exists());
    assert_eq!(rows(&state), 1);
}

#[tokio::test]
async fn malformed_frames_progress_and_missing_base_fail_closed() {
    let dir = directory();
    let state = open(&dir);
    let op = operations().remove(0);
    let frames: WalFrameSet = serde_json::from_slice(&op.data).unwrap();
    for variant in 0..4 {
        let mut bad = frames.clone();
        match variant {
            0 => bad.frames[0].page_number = 0,
            1 => bad.frames[0].data.pop().map(|_| ()).unwrap(),
            2 => bad.checksum ^= 1,
            _ => bad.db_size_pages = 0,
        }
        if variant != 2 {
            bad.checksum = WalFrameSet::compute_checksum(&bad.frames);
        }
        assert!(
            state
                .apply(Operation {
                    data: serde_json::to_vec(&bad).unwrap().into(),
                    ..op.clone()
                })
                .await
                .is_err()
        );
    }
    assert!(
        state
            .apply(Operation {
                committed_lsn: 2,
                ..op.clone()
            })
            .await
            .is_err()
    );
    assert_eq!(state.durable_progress().await.unwrap(), progress(0, 0));
    state.apply(op).await.unwrap();
    drop(state);
    fs::remove_file(dir.path().join("base-0.sqlite")).unwrap();
    let state = open(&dir);
    assert!(matches!(
        state.recovery_state().unwrap(),
        RecoveryState::RebuildRequired(_)
    ));
    assert_eq!(state.durable_progress().await.unwrap(), progress(1, 0));
}

#[test]
fn classic_or_missing_metadata_is_not_imported() {
    let dir = directory();
    fs::write(dir.path().join("meta.json"), b"{\"committed_lsn\":1}").unwrap();
    assert!(SqlitePersistence::open(dir.path().to_owned()).is_err());
}

#[tokio::test]
async fn metadata_corruption_cannot_silently_regress_watermarks() {
    let dir = directory();
    let state = open(&dir);
    state.apply(operations().remove(0)).await.unwrap();
    state.commit(1).await.unwrap();
    drop(state);
    let path = dir.path().join("state-v2.json");
    let mut json: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    json["state"]["committed_lsn"] = 0.into();
    fs::write(path, serde_json::to_vec(&json).unwrap()).unwrap();
    assert!(SqlitePersistence::open(dir.path().to_owned()).is_err());
}

#[tokio::test]
async fn installing_older_copy_preserves_later_contiguous_history_and_geometry() {
    for snapshot_boundary in [0, 1] {
        let source_dir = directory();
        let source = open(&source_dir);
        let ops = operations();
        let empty = source.snapshot(0).unwrap();
        for op in &ops[..2] {
            source.apply(op.clone()).await.unwrap();
        }
        let bytes = if snapshot_boundary == 0 {
            empty
        } else {
            source.snapshot(snapshot_boundary).unwrap()
        };
        let target_dir = directory();
        let target = open(&target_dir);
        for op in &ops {
            target.apply(op.clone()).await.unwrap();
        }
        target.commit(3).await.unwrap();
        let build = OperationId::new("older-build");
        stage(&target, &build, &bytes).await;
        assert_eq!(
            target
                .finish_copy(&build, snapshot_boundary, snapshot_boundary)
                .await
                .unwrap(),
            progress(3, 3)
        );
        assert!(target.verify_applied(&ops[2]).await.unwrap());
        assert_eq!(rows(&target), 2);
        drop(target);
        let target = open(&target_dir);
        assert_eq!(target.recovery_state().unwrap(), RecoveryState::Healthy);
        assert_eq!(rows(&target), 2);
    }
}
