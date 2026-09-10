//! File-based persistence for KvState.
//!
//! Two files:
//! - `state.json` — periodic full snapshot (atomic write-rename)
//! - `wal.log` — append-only NDJSON log of KvOps since last snapshot
//!
//! Recovery: load snapshot + replay WAL. Crash-safe via:
//! - WAL: append + flush before ACK (process-crash durability)
//! - Snapshot: write-tmp + fdatasync + rename (atomic)
//! - WAL truncation after replay (removes corrupt trailing bytes)

use std::path::Path;

use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, BufReader, BufWriter};
use tracing::{info, warn};

use crate::state::{KvOp, KvState};
use kuberic_core::types::Lsn;

async fn atomic_write(path: &Path, contents: Vec<u8>) -> std::io::Result<()> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        use std::io::Write;
        let temporary = path.with_extension(format!(
            "{}.tmp",
            path.extension().unwrap().to_string_lossy()
        ));
        let mut file = std::fs::File::create(&temporary)?;
        file.write_all(&contents)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(temporary, path)
    })
    .await
    .map_err(std::io::Error::other)?
}

/// A single WAL entry: one operation at one LSN.
#[derive(Serialize, Deserialize)]
pub struct WalEntry {
    pub lsn: Lsn,
    pub op: KvOp,
}

async fn read_wal_entry(
    reader: &mut BufReader<fs::File>,
) -> std::io::Result<Option<(WalEntry, Vec<u8>)>> {
    let mut record = Vec::new();
    if reader.read_until(b'\n', &mut record).await? == 0 {
        return Ok(None);
    }
    match serde_json::from_slice(&record) {
        Ok(entry) => {
            if record.last() != Some(&b'\n') {
                record.push(b'\n');
            }
            Ok(Some((entry, record)))
        }
        Err(error) => {
            warn!(%error, "truncated WAL entry, stopping replay");
            Ok(None)
        }
    }
}

/// Snapshot format: full HashMap + LSN.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct SnapshotData {
    pub last_applied_lsn: Lsn,
    pub data: std::collections::HashMap<String, String>,
    #[serde(default)]
    pub key_versions: std::collections::HashMap<String, Lsn>,
    #[serde(default)]
    pub committed_transactions: std::collections::HashMap<String, Lsn>,
}

/// Load a snapshot from disk, or return empty state if no snapshot exists.
pub async fn load_snapshot(dir: &Path) -> std::io::Result<SnapshotData> {
    let path = dir.join("state.json");
    match fs::read_to_string(&path).await {
        Ok(contents) => {
            let snap: SnapshotData = serde_json::from_str(&contents)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            info!(
                lsn = snap.last_applied_lsn,
                keys = snap.data.len(),
                "loaded snapshot"
            );
            Ok(snap)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            info!("no snapshot found, starting empty");
            Ok(SnapshotData::default())
        }
        Err(e) => Err(e),
    }
}

/// Replay WAL entries from disk, applying only entries with LSN > current.
/// Returns the number of entries replayed.
pub async fn replay_wal(state: &mut KvState, dir: &Path) -> std::io::Result<u64> {
    replay_wal_up_to(state, dir, Lsn::MAX).await
}

pub async fn replay_wal_up_to(
    state: &mut KvState,
    dir: &Path,
    max_lsn: Lsn,
) -> std::io::Result<u64> {
    let path = dir.join("wal.log");
    let file = match fs::File::open(&path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    let mut reader = BufReader::new(file);
    let mut replayed = 0u64;
    while let Some((entry, _)) = read_wal_entry(&mut reader).await? {
        if entry.lsn <= max_lsn
            && (entry.lsn > state.last_applied_lsn || matches!(entry.op, KvOp::Snapshot(_)))
        {
            state.apply_op_in_memory(entry.lsn, &entry.op);
            replayed += 1;
        }
    }
    info!(
        replayed,
        lsn = state.last_applied_lsn,
        "WAL replay complete"
    );
    Ok(replayed)
}

/// After replay, rewrite WAL with only valid entries to remove any
/// corrupt trailing bytes. Without this, new entries appended after
/// the corrupt line would be lost on next recovery.
pub async fn truncate_wal_to_valid(dir: &Path) -> std::io::Result<()> {
    let wal_path = dir.join("wal.log");

    let mut contents = Vec::new();
    let file = match fs::File::open(&wal_path).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    let mut reader = BufReader::new(file);
    while let Some((_, record)) = read_wal_entry(&mut reader).await? {
        contents.extend_from_slice(&record);
    }
    drop(reader);
    atomic_write(&wal_path, contents).await
}

/// Open WAL file for appending.
pub async fn open_wal_append(dir: &Path) -> std::io::Result<BufWriter<fs::File>> {
    let file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("wal.log"))
        .await?;
    Ok(BufWriter::new(file))
}

/// Write a snapshot and truncate the WAL.
/// Only call when committed_lsn == last_applied_lsn (no uncommitted ops).
pub async fn write_checkpoint(state: &KvState, dir: &Path) -> std::io::Result<()> {
    let snapshot = state.snapshot();

    let dst = dir.join("state.json");
    let json = serde_json::to_vec(&snapshot).map_err(std::io::Error::other)?;
    atomic_write(&dst, json).await?;

    info!(
        lsn = state.last_applied_lsn,
        keys = state.data.len(),
        "checkpoint written"
    );
    Ok(())
}

/// Rewrite WAL keeping only entries up to max_lsn.
/// Returns a new BufWriter for the rewritten WAL.
pub async fn rewrite_wal_up_to(dir: &Path, max_lsn: Lsn) -> std::io::Result<BufWriter<fs::File>> {
    let wal_path = dir.join("wal.log");

    let mut contents = Vec::new();
    let file = fs::File::open(&wal_path).await?;
    let mut reader = BufReader::new(file);
    while let Some((entry, record)) = read_wal_entry(&mut reader).await? {
        if entry.lsn <= max_lsn {
            contents.extend_from_slice(&record);
        }
    }
    drop(reader);
    atomic_write(&wal_path, contents).await?;

    // Reopen for append
    open_wal_append(dir).await
}
