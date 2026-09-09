use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use kuberic_core::types::{CancellationToken, Lsn, OperationStream};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufWriter};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::persistence;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum KvOp {
    Put {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
    Transaction {
        transaction_id: String,
        mutations: Vec<KvMutation>,
    },
    Snapshot(persistence::SnapshotData),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum KvMutation {
    Put { key: String, value: String },
    Delete { key: String },
}

#[derive(Serialize, Deserialize)]
pub(crate) struct CopyChunk {
    pub index: usize,
    pub data: Vec<u8>,
    pub last: bool,
}

pub struct KvState {
    pub data: HashMap<String, String>,
    pub last_applied_lsn: Lsn,
    pub committed_lsn: Lsn,
    pub(crate) generation: u64,
    pub(crate) applied: Arc<tokio::sync::Notify>,
    key_versions: HashMap<String, Lsn>,
    committed_transactions: HashMap<String, Lsn>,
    wal_failed: bool,
    wal_writer: BufWriter<tokio::fs::File>,
    data_dir: PathBuf,
}

pub const COMMIT_HISTORY_LIMIT: usize = 1024;

impl KvState {
    /// Open with persistence. Loads snapshot + replays WAL.
    /// This is the only constructor.
    pub async fn open(data_dir: PathBuf) -> std::io::Result<Self> {
        tokio::fs::create_dir_all(&data_dir).await?;
        let snapshot = persistence::load_snapshot(&data_dir).await?;

        let mut state = Self::from_snapshot(snapshot, data_dir.clone()).await?;

        persistence::replay_wal(&mut state, &data_dir).await?;
        persistence::truncate_wal_to_valid(&data_dir).await?;

        state.wal_writer = persistence::open_wal_append(&data_dir).await?;
        Ok(state)
    }

    async fn from_snapshot(
        snapshot: persistence::SnapshotData,
        data_dir: PathBuf,
    ) -> std::io::Result<Self> {
        let mut state = Self {
            data: HashMap::new(),
            last_applied_lsn: 0,
            committed_lsn: snapshot.last_applied_lsn,
            generation: 0,
            applied: Arc::new(tokio::sync::Notify::new()),
            key_versions: HashMap::new(),
            committed_transactions: HashMap::new(),
            wal_failed: false,
            // Temporary — replaced after replay
            wal_writer: persistence::open_wal_append(&data_dir).await?,
            data_dir: data_dir.clone(),
        };
        state.install_snapshot(&snapshot);
        Ok(state)
    }

    /// Apply an operation to in-memory state AND persist to WAL.
    /// Returns Err if WAL write fails — caller must NOT acknowledge.
    pub async fn apply_op(&mut self, lsn: Lsn, op: &KvOp) -> std::io::Result<()> {
        if self.wal_failed {
            return Err(std::io::Error::other("WAL requires recovery"));
        }
        if lsn <= self.last_applied_lsn && !matches!(op, KvOp::Snapshot(_)) {
            return Ok(());
        }
        // Persist to WAL
        let entry = persistence::WalEntry {
            lsn,
            op: op.clone(),
        };
        let line = serde_json::to_string(&entry).map_err(std::io::Error::other)?;
        self.wal_failed = true;
        self.wal_writer.write_all(line.as_bytes()).await?;
        self.wal_writer.write_all(b"\n").await?;
        self.wal_writer.flush().await?;
        // fdatasync for power-failure durability. Disabled by default
        // for performance (process-crash safe via flush alone).
        // Enable with: self.wal_writer.get_ref().sync_data().await?;
        self.apply_op_in_memory(lsn, op);
        self.wal_failed = false;
        self.applied.notify_waiters();
        Ok(())
    }

    /// Apply an operation to in-memory state only (no WAL write).
    /// Used during WAL replay and copy stream processing.
    pub fn apply_op_in_memory(&mut self, lsn: Lsn, op: &KvOp) {
        match op {
            KvOp::Put { key, value } => {
                self.data.insert(key.clone(), value.clone());
                self.key_versions.insert(key.clone(), lsn);
            }
            KvOp::Delete { key } => {
                self.data.remove(key);
                self.key_versions.insert(key.clone(), lsn);
            }
            KvOp::Transaction {
                transaction_id,
                mutations,
            } => {
                if self.committed_transactions.contains_key(transaction_id) {
                    self.last_applied_lsn = self.last_applied_lsn.max(lsn);
                    return;
                }
                for mutation in mutations {
                    match mutation {
                        KvMutation::Put { key, value } => {
                            self.data.insert(key.clone(), value.clone());
                            self.key_versions.insert(key.clone(), lsn);
                        }
                        KvMutation::Delete { key } => {
                            self.data.remove(key);
                            self.key_versions.insert(key.clone(), lsn);
                        }
                    }
                }
                self.committed_transactions
                    .insert(transaction_id.clone(), lsn);
                if self.committed_transactions.len() > COMMIT_HISTORY_LIMIT {
                    let oldest = self
                        .committed_transactions
                        .iter()
                        .min_by_key(|(_, committed_lsn)| **committed_lsn)
                        .map(|(transaction_id, _)| transaction_id.clone())
                        .unwrap();
                    self.committed_transactions.remove(&oldest);
                }
            }
            KvOp::Snapshot(snapshot) => self.install_snapshot(snapshot),
        }
        if lsn > self.last_applied_lsn {
            self.last_applied_lsn = lsn;
        }
    }

    pub fn key_version(&self, key: &str) -> Lsn {
        self.key_versions.get(key).copied().unwrap_or(0)
    }

    pub fn transaction_lsn(&self, transaction_id: &str) -> Option<Lsn> {
        self.committed_transactions.get(transaction_id).copied()
    }

    pub fn snapshot(&self) -> persistence::SnapshotData {
        persistence::SnapshotData {
            last_applied_lsn: self.last_applied_lsn,
            data: self.data.clone(),
            key_versions: self.key_versions.clone(),
            committed_transactions: self.committed_transactions.clone(),
        }
    }

    fn install_snapshot(&mut self, snapshot: &persistence::SnapshotData) {
        self.data = snapshot.data.clone();
        self.key_versions = snapshot.key_versions.clone();
        for key in self.data.keys() {
            self.key_versions
                .entry(key.clone())
                .or_insert(snapshot.last_applied_lsn);
        }
        self.committed_transactions = snapshot.committed_transactions.clone();
        self.last_applied_lsn = snapshot.last_applied_lsn;
    }

    pub async fn snapshot_at(&self, lsn: Lsn) -> std::io::Result<persistence::SnapshotData> {
        if lsn == self.last_applied_lsn {
            return Ok(self.snapshot());
        }
        let snapshot = persistence::load_snapshot(&self.data_dir).await?;
        if lsn < snapshot.last_applied_lsn || lsn > self.last_applied_lsn {
            return Err(std::io::Error::other(
                "copy LSN is outside retained history",
            ));
        }
        let mut recovered = Self::from_snapshot(snapshot, self.data_dir.clone()).await?;
        persistence::replay_wal_up_to(&mut recovered, &self.data_dir, lsn).await?;
        Ok(recovered.snapshot())
    }

    /// Update committed_lsn when the replicator confirms advancement.
    pub fn set_committed_lsn(&mut self, lsn: Lsn) {
        if lsn > self.committed_lsn {
            self.committed_lsn = lsn;
        }
    }

    /// Write a snapshot and truncate the WAL.
    /// Only safe when committed_lsn == last_applied_lsn.
    pub async fn checkpoint(&mut self) -> std::io::Result<()> {
        if self.wal_failed {
            return Err(std::io::Error::other("WAL requires recovery"));
        }
        if self.committed_lsn < self.last_applied_lsn {
            warn!(
                committed = self.committed_lsn,
                applied = self.last_applied_lsn,
                "skipping checkpoint: uncommitted ops present"
            );
            return Ok(());
        }

        persistence::write_checkpoint(self, &self.data_dir.clone()).await?;

        // Truncate WAL
        let wal_path = self.data_dir.join("wal.log");
        let file = tokio::fs::File::create(&wal_path).await?;
        self.wal_writer = BufWriter::new(file);
        Ok(())
    }

    /// Rollback state to target_lsn by reloading snapshot + partial WAL replay.
    /// Call AFTER cancelling drain tasks to avoid races.
    pub async fn rollback_to(&mut self, target_lsn: Lsn) -> std::io::Result<()> {
        let dir = self.data_dir.clone();

        // Reload from snapshot (always at committed_lsn)
        let snapshot = persistence::load_snapshot(&dir).await?;
        if snapshot.last_applied_lsn > target_lsn {
            return Err(std::io::Error::other("rollback precedes checkpoint"));
        }
        self.install_snapshot(&snapshot);

        // Replay WAL only up to target_lsn
        let wal_path = dir.join("wal.log");
        if let Ok(file) = tokio::fs::File::open(&wal_path).await {
            let mut lines = tokio::io::BufReader::new(file).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let Ok(entry) = serde_json::from_str::<persistence::WalEntry>(&line) else {
                    break;
                };
                if entry.lsn > self.last_applied_lsn && entry.lsn <= target_lsn {
                    self.apply_op_in_memory(entry.lsn, &entry.op);
                }
            }
        }

        // Rewrite WAL with only entries up to target_lsn
        self.wal_writer = persistence::rewrite_wal_up_to(&dir, target_lsn).await?;
        self.committed_lsn = self.committed_lsn.min(self.last_applied_lsn);
        self.wal_failed = false;

        info!(lsn = self.last_applied_lsn, "rollback complete");
        Ok(())
    }

    /// Get the data directory path.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }
}

use std::path::Path;

pub type SharedState = Arc<RwLock<KvState>>;

pub async fn drain_copy_stream(
    state: SharedState,
    mut stream: OperationStream,
    token: CancellationToken,
) -> std::io::Result<()> {
    let mut contents = Vec::new();
    let mut expected_index = 0;
    let mut snapshot_lsn = None;
    loop {
        let operation = tokio::select! {
            biased;
            _ = token.cancelled() => return Err(std::io::Error::other("copy cancelled")),
            operation = stream.get_operation() => operation,
        }
        .ok_or_else(|| std::io::Error::other("incomplete copy snapshot"))?;
        let chunk: CopyChunk =
            serde_json::from_slice(&operation.data).map_err(std::io::Error::other)?;
        let lsn = *snapshot_lsn.get_or_insert(operation.lsn);
        if chunk.index != expected_index || operation.lsn != lsn {
            return Err(std::io::Error::other("out-of-order copy snapshot"));
        }
        contents.extend_from_slice(&chunk.data);
        expected_index += 1;
        if chunk.last {
            let snapshot: persistence::SnapshotData =
                serde_json::from_slice(&contents).map_err(std::io::Error::other)?;
            if snapshot.last_applied_lsn != lsn {
                return Err(std::io::Error::other("copy snapshot LSN mismatch"));
            }
            state
                .write()
                .await
                .apply_op(lsn, &KvOp::Snapshot(snapshot))
                .await?;
            operation.acknowledge();
            return Ok(());
        }
        operation.acknowledge();
    }
}

/// Drain a copy or replication stream, applying each operation to shared state.
/// Stops when the stream ends or the cancellation token fires.
///
/// Each operation is applied to in-memory state AND persisted to WAL before
/// acknowledging. The acknowledge gates the secondary's ACK back to the
/// primary, so WAL persistence is on the quorum path.
pub async fn drain_stream(
    state: SharedState,
    mut stream: OperationStream,
    token: CancellationToken,
    label: &'static str,
) -> std::io::Result<()> {
    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => {
                info!(label, "stream drain cancelled");
                break;
            }
            item = stream.get_operation() => {
                let Some(op) = item else { break };
                let lsn = op.lsn;
                match serde_json::from_slice::<KvOp>(&op.data) {
                    Ok(kv_op) => {
                        if let Err(e) = state.write().await.apply_op(lsn, &kv_op).await {
                            warn!(lsn, error = %e, label, "WAL write failed, not acknowledging");
                            return Err(e);
                        }
                        debug!(lsn, ?kv_op, label, "applied from stream");
                        op.acknowledge();
                    }
                    Err(e) => {
                        warn!(lsn, error = %e, label, "failed to deserialize stream op");
                        return Err(std::io::Error::other(e));
                    }
                }
            }
        }
    }
    info!(label, "stream drained");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn transaction_wal_failure_and_recovery_are_atomic() {
        let dir = std::env::temp_dir().join(format!(
            "kvstore-transaction-wal-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut state = KvState::open(dir.clone()).await.unwrap();
        let operation = KvOp::Transaction {
            transaction_id: "wal-test".into(),
            mutations: vec![
                KvMutation::Put {
                    key: "first".into(),
                    value: "one".into(),
                },
                KvMutation::Put {
                    key: "second".into(),
                    value: "two".into(),
                },
                KvMutation::Delete {
                    key: "first".into(),
                },
            ],
        };
        state.wal_writer =
            BufWriter::new(tokio::fs::File::open(dir.join("wal.log")).await.unwrap());
        assert!(state.apply_op(1, &operation).await.is_err());
        assert!(state.data.is_empty());
        assert_eq!(state.last_applied_lsn, 0);
        assert!(state.apply_op(1, &operation).await.is_err());
        drop(state);
        let mut state = KvState::open(dir.clone()).await.unwrap();
        state.apply_op(1, &operation).await.unwrap();
        assert_eq!(state.data.len(), 1);
        assert_eq!(state.data["second"], "two");
        drop(state);
        let recovered = KvState::open(dir.clone()).await.unwrap();
        assert_eq!(recovered.data.len(), 1);
        assert_eq!(recovered.data["second"], "two");
        assert_eq!(recovered.last_applied_lsn, 1);
        assert_eq!(recovered.transaction_lsn("wal-test"), Some(1));
        assert_eq!(recovered.key_version("first"), 1);
        drop(recovered);
        tokio::fs::remove_dir_all(dir).await.unwrap();
    }

    #[tokio::test]
    async fn truncated_transaction_wal_and_copy_never_publish_partial_state() {
        let directory =
            std::env::temp_dir().join(format!("kv-truncated-{:032x}", rand::random::<u128>()));
        let mut state = KvState::open(directory.clone()).await.unwrap();
        state
            .apply_op(
                1,
                &KvOp::Put {
                    key: "old".into(),
                    value: "preserved".into(),
                },
            )
            .await
            .unwrap();
        let entry = persistence::WalEntry {
            lsn: 2,
            op: KvOp::Transaction {
                transaction_id: "partial".into(),
                mutations: vec![
                    KvMutation::Delete { key: "old".into() },
                    KvMutation::Put {
                        key: "new".into(),
                        value: "hidden".into(),
                    },
                ],
            },
        };
        let encoded = serde_json::to_vec(&entry).unwrap();
        state
            .wal_writer
            .write_all(&encoded[..encoded.len() - 3])
            .await
            .unwrap();
        state.wal_writer.flush().await.unwrap();
        drop(state);
        let recovered = KvState::open(directory.clone()).await.unwrap();
        assert_eq!(recovered.last_applied_lsn, 1);
        assert_eq!(recovered.data.len(), 1);
        assert_eq!(recovered.data["old"], "preserved");
        assert_eq!(recovered.transaction_lsn("partial"), None);
        let state = Arc::new(RwLock::new(recovered));
        let (sender, stream) = OperationStream::channel(1);
        let chunk = CopyChunk {
            index: 0,
            data: b"{\"data\":".to_vec(),
            last: false,
        };
        sender
            .send(kuberic_core::types::Operation::new(
                2,
                serde_json::to_vec(&chunk).unwrap().into(),
                None,
            ))
            .await
            .unwrap();
        drop(sender);
        assert!(
            drain_copy_stream(state.clone(), stream, CancellationToken::new())
                .await
                .is_err()
        );
        assert_eq!(state.read().await.last_applied_lsn, 1);
        assert_eq!(state.read().await.data["old"], "preserved");
        state.write().await.apply_op(2, &entry.op).await.unwrap();
        drop(state);
        let recovered = KvState::open(directory.clone()).await.unwrap();
        assert_eq!(recovered.last_applied_lsn, 2);
        assert_eq!(recovered.data.len(), 1);
        assert_eq!(recovered.data["new"], "hidden");
        drop(recovered);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }
}
