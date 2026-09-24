use std::collections::BTreeMap;
use std::io::Write;
use std::ops::Bound::{Excluded, Included};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Mutex;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, stream};
use kuberic_protocol::types::{Epoch, OperationId};
use kuberic_runtime::application::{
    CopyChunk, DurableApplicationAck, DurableApplicationProgress, Operation,
};
use kuberic_runtime::engine::{DurableState, RetainedOperationStream};
use kuberic_runtime::{Result, RuntimeError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Mutation {
    Put { key: String, value: String },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PersistedState {
    values: BTreeMap<String, String>,
    #[serde(default)]
    base_values: BTreeMap<String, String>,
    #[serde(default)]
    base_lsn: i64,
    operations: BTreeMap<i64, OperationRecord>,
    applied_lsn: i64,
    committed_lsn: i64,
    epoch: Epoch,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OperationRecord {
    committed_lsn: i64,
    data: Vec<u8>,
}

pub struct KvPersistence {
    root: PathBuf,
    state_path: PathBuf,
    state: Mutex<PersistedState>,
    #[cfg(test)]
    fail_next_persist: AtomicBool,
}

impl KvPersistence {
    pub fn is_fresh_empty(root: impl AsRef<Path>) -> Result<bool> {
        let root = root.as_ref();
        if !root.exists() {
            return Ok(true);
        }
        if !root.is_dir() {
            return Err(RuntimeError::Application(
                "application data root is not a directory".into(),
            ));
        }
        Ok(std::fs::read_dir(root)
            .map_err(|error| RuntimeError::Application(error.to_string()))?
            .next()
            .is_none())
    }

    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)
            .map_err(|error| RuntimeError::Application(error.to_string()))?;
        let state_path = root.join("state.json");
        let state = if state_path.is_file() {
            serde_json::from_slice(
                &std::fs::read(&state_path)
                    .map_err(|error| RuntimeError::Application(error.to_string()))?,
            )
            .map_err(|error| RuntimeError::Application(error.to_string()))?
        } else {
            PersistedState::default()
        };
        Ok(Self {
            root,
            state_path,
            state: Mutex::new(state),
            #[cfg(test)]
            fail_next_persist: AtomicBool::new(false),
        })
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.state.lock().unwrap().values.get(key).cloned()
    }

    pub fn snapshot_at(&self, up_to_lsn: i64) -> Result<BTreeMap<String, String>> {
        let state = self.state.lock().unwrap();
        if up_to_lsn < state.base_lsn {
            return Err(RuntimeError::Application(format!(
                "snapshot boundary {up_to_lsn} predates retained base {}",
                state.base_lsn
            )));
        }
        let mut values = state.base_values.clone();
        for record in state
            .operations
            .range((Excluded(state.base_lsn), Included(up_to_lsn)))
            .map(|(_, record)| record)
        {
            match serde_json::from_slice::<Mutation>(&record.data)
                .map_err(|error| RuntimeError::Application(error.to_string()))?
            {
                Mutation::Put { key, value } => {
                    values.insert(key, value);
                }
            }
        }
        Ok(values)
    }

    pub fn encode_put(key: String, value: String) -> Result<Bytes> {
        serde_json::to_vec(&Mutation::Put { key, value })
            .map(Bytes::from)
            .map_err(|error| RuntimeError::Application(error.to_string()))
    }

    pub fn update_epoch(&self, epoch: Epoch) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if epoch < state.epoch {
            return Err(RuntimeError::AuthorityMismatch(
                "application epoch regressed".into(),
            ));
        }
        let mut candidate = state.clone();
        candidate.epoch = epoch;
        self.persist(&candidate)?;
        *state = candidate;
        Ok(())
    }

    fn persist(&self, state: &PersistedState) -> Result<()> {
        #[cfg(test)]
        if self.fail_next_persist.swap(false, Ordering::SeqCst) {
            return Err(RuntimeError::Application(
                "injected persistence failure".into(),
            ));
        }
        let temporary = self.state_path.with_extension("json.tmp");
        let bytes = serde_json::to_vec(state)
            .map_err(|error| RuntimeError::Application(error.to_string()))?;
        std::fs::write(&temporary, bytes)
            .map_err(|error| RuntimeError::Application(error.to_string()))?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(&temporary)
            .map_err(|error| RuntimeError::Application(error.to_string()))?;
        file.sync_all()
            .map_err(|error| RuntimeError::Application(error.to_string()))?;
        std::fs::rename(&temporary, &self.state_path)
            .map_err(|error| RuntimeError::Application(error.to_string()))?;
        let directory = std::fs::File::open(&self.root)
            .map_err(|error| RuntimeError::Application(error.to_string()))?;
        directory
            .sync_all()
            .map_err(|error| RuntimeError::Application(error.to_string()))
    }

    fn copy_chunk_path(&self, build_id: &OperationId, sequence: u64) -> PathBuf {
        self.root
            .join("copy")
            .join(build_id.as_str())
            .join(format!("{sequence:020}.chunk"))
    }

    fn copy_chunk_staging_path(&self, build_id: &OperationId, sequence: u64) -> PathBuf {
        self.root
            .join("copy")
            .join(build_id.as_str())
            .join(format!("{sequence:020}.chunk.tmp"))
    }
}

#[async_trait]
impl DurableState for KvPersistence {
    async fn get_replication_operations(
        &self,
        from_lsn: i64,
        to_lsn: i64,
    ) -> Result<RetainedOperationStream> {
        if from_lsn > to_lsn {
            return Ok(Box::pin(stream::empty()));
        }
        let operations = self
            .state
            .lock()
            .unwrap()
            .operations
            .range(from_lsn..=to_lsn)
            .map(|(lsn, record)| {
                Ok(Operation {
                    lsn: *lsn,
                    committed_lsn: record.committed_lsn,
                    data: Bytes::copy_from_slice(&record.data),
                })
            })
            .collect::<Vec<_>>();
        Ok(Box::pin(stream::iter(operations)))
    }

    async fn apply_copy_chunk(
        &self,
        build_id: &OperationId,
        sequence: u64,
        chunk: CopyChunk,
    ) -> Result<()> {
        let path = self.copy_chunk_path(build_id, sequence);
        let staging = self.copy_chunk_staging_path(build_id, sequence);
        let parent = path
            .parent()
            .ok_or_else(|| RuntimeError::Application("copy chunk path has no parent".into()))?;
        std::fs::create_dir_all(parent)
            .map_err(|error| RuntimeError::Application(error.to_string()))?;
        if path.is_file() {
            if std::fs::read(&path).map_err(|error| RuntimeError::Application(error.to_string()))?
                != chunk.data
            {
                return Err(RuntimeError::AuthorityMismatch(
                    "copy sequence was reused with different bytes".into(),
                ));
            }
            std::fs::File::open(&path)
                .and_then(|file| file.sync_all())
                .map_err(|error| RuntimeError::Application(error.to_string()))?;
        } else {
            match std::fs::remove_file(&staging) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(RuntimeError::Application(error.to_string())),
            }
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&staging)
                .map_err(|error| RuntimeError::Application(error.to_string()))?;
            file.write_all(&chunk.data)
                .map_err(|error| RuntimeError::Application(error.to_string()))?;
            file.sync_all()
                .map_err(|error| RuntimeError::Application(error.to_string()))?;
            drop(file);
            std::fs::rename(&staging, &path)
                .map_err(|error| RuntimeError::Application(error.to_string()))?;
        }
        let mut directory = Some(parent);
        while let Some(path) = directory {
            std::fs::File::open(path)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| RuntimeError::Application(error.to_string()))?;
            if path == self.root {
                break;
            }
            directory = path.parent();
        }
        Ok(())
    }

    async fn verify_copy_chunk(
        &self,
        build_id: &OperationId,
        sequence: u64,
        chunk: &CopyChunk,
    ) -> Result<bool> {
        let path = self.copy_chunk_path(build_id, sequence);
        Ok(path.is_file()
            && std::fs::read(path).map_err(|error| RuntimeError::Application(error.to_string()))?
                == chunk.data)
    }

    async fn finish_copy(
        &self,
        build_id: &OperationId,
        up_to_lsn: i64,
        committed_lsn: i64,
    ) -> Result<DurableApplicationProgress> {
        let directory = self.root.join("copy").join(build_id.as_str());
        let mut paths = std::fs::read_dir(&directory)
            .map_err(|error| RuntimeError::Application(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| RuntimeError::Application(error.to_string()))?;
        paths.retain(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "chunk")
        });
        paths.sort_by_key(|entry| entry.file_name());
        let mut bytes = Vec::new();
        for entry in paths {
            bytes.extend(
                std::fs::read(entry.path())
                    .map_err(|error| RuntimeError::Application(error.to_string()))?,
            );
        }
        let values = if bytes.is_empty() {
            BTreeMap::new()
        } else {
            serde_json::from_slice(&bytes)
                .map_err(|error| RuntimeError::Application(error.to_string()))?
        };
        let mut state = self.state.lock().unwrap();
        let mut candidate = state.clone();
        candidate.values = values.clone();
        candidate.base_values = values;
        candidate.base_lsn = up_to_lsn;
        candidate.operations.clear();
        candidate.applied_lsn = up_to_lsn;
        candidate.committed_lsn = committed_lsn;
        self.persist(&candidate)?;
        *state = candidate;
        Ok(DurableApplicationProgress {
            applied_lsn: up_to_lsn,
            committed_lsn,
        })
    }

    async fn apply(&self, operation: Operation) -> Result<DurableApplicationAck> {
        let mutation: Mutation = serde_json::from_slice(&operation.data)
            .map_err(|error| RuntimeError::Application(error.to_string()))?;
        let mut state = self.state.lock().unwrap();
        if let Some(existing) = state.operations.get(&operation.lsn) {
            if existing.data == operation.data && existing.committed_lsn == operation.committed_lsn
            {
                return Ok(DurableApplicationProgress {
                    applied_lsn: state.applied_lsn,
                    committed_lsn: state.committed_lsn,
                });
            }
            return Err(RuntimeError::AuthorityMismatch(
                "LSN was reused with different application data".into(),
            ));
        }
        let mut candidate = state.clone();
        match mutation {
            Mutation::Put { key, value } => {
                candidate.values.insert(key, value);
            }
        }
        candidate.operations.insert(
            operation.lsn,
            OperationRecord {
                committed_lsn: operation.committed_lsn,
                data: operation.data.to_vec(),
            },
        );
        candidate.applied_lsn = candidate.applied_lsn.max(operation.lsn);
        candidate.committed_lsn = candidate.committed_lsn.max(operation.committed_lsn);
        self.persist(&candidate)?;
        *state = candidate;
        Ok(DurableApplicationProgress {
            applied_lsn: state.applied_lsn,
            committed_lsn: state.committed_lsn,
        })
    }

    async fn durable_progress(&self) -> Result<DurableApplicationProgress> {
        let state = self.state.lock().unwrap();
        Ok(DurableApplicationProgress {
            applied_lsn: state.applied_lsn,
            committed_lsn: state.committed_lsn,
        })
    }

    async fn verify_applied(&self, operation: &Operation) -> Result<bool> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .operations
            .get(&operation.lsn)
            .is_some_and(|record| {
                record.data == operation.data && record.committed_lsn == operation.committed_lsn
            }))
    }

    async fn commit(&self, committed_lsn: i64) -> Result<DurableApplicationProgress> {
        let mut state = self.state.lock().unwrap();
        if committed_lsn > state.applied_lsn {
            return Err(RuntimeError::Application(
                "cannot commit beyond applied progress".into(),
            ));
        }
        let mut candidate = state.clone();
        candidate.committed_lsn = candidate.committed_lsn.max(committed_lsn);
        self.persist(&candidate)?;
        *state = candidate;
        Ok(DurableApplicationProgress {
            applied_lsn: state.applied_lsn,
            committed_lsn: state.committed_lsn,
        })
    }
}

pub type SnapshotStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>;

pub fn snapshot_stream(values: BTreeMap<String, String>) -> Result<SnapshotStream> {
    let bytes = serde_json::to_vec(&values)
        .map(Bytes::from)
        .map_err(|error| RuntimeError::Application(error.to_string()))?;
    Ok(Box::pin(stream::iter(
        (!bytes.is_empty()).then_some(Ok(bytes)),
    )))
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

    #[tokio::test]
    async fn put_is_durable_idempotent_and_reopens() {
        let directory = tempfile::tempdir().unwrap();
        let store = KvPersistence::open(directory.path()).unwrap();
        let operation = Operation {
            lsn: 1,
            committed_lsn: 0,
            data: KvPersistence::encode_put("key".into(), "value".into()).unwrap(),
        };
        store.apply(operation.clone()).await.unwrap();
        store.apply(operation).await.unwrap();
        store.commit(1).await.unwrap();
        assert_eq!(store.get("key").as_deref(), Some("value"));
        drop(store);

        let reopened = KvPersistence::open(directory.path()).unwrap();
        assert_eq!(reopened.get("key").as_deref(), Some("value"));
        assert_eq!(reopened.durable_progress().await.unwrap().committed_lsn, 1);
    }

    #[tokio::test]
    async fn failed_persistence_does_not_publish_in_memory_durability() {
        let directory = tempfile::tempdir().unwrap();
        let store = KvPersistence::open(directory.path()).unwrap();
        let operation = Operation {
            lsn: 1,
            committed_lsn: 0,
            data: KvPersistence::encode_put("key".into(), "value".into()).unwrap(),
        };
        store.fail_next_persist.store(true, Ordering::SeqCst);
        assert!(matches!(
            store.apply(operation.clone()).await,
            Err(RuntimeError::Application(_))
        ));
        assert_eq!(store.get("key"), None);
        assert_eq!(
            store.durable_progress().await.unwrap(),
            DurableApplicationProgress::default()
        );
        assert!(!store.verify_applied(&operation).await.unwrap());

        store.apply(operation.clone()).await.unwrap();
        assert!(store.verify_applied(&operation).await.unwrap());
        drop(store);
        let reopened = KvPersistence::open(directory.path()).unwrap();
        assert_eq!(reopened.get("key").as_deref(), Some("value"));
    }

    #[tokio::test]
    async fn empty_replication_gap_returns_an_empty_stream() {
        let directory = tempfile::tempdir().unwrap();
        let store = KvPersistence::open(directory.path()).unwrap();
        let mut operations = store.get_replication_operations(2, 1).await.unwrap();
        assert!(operations.next().await.is_none());
    }

    #[tokio::test]
    async fn snapshot_at_reconstructs_the_frozen_build_boundary() {
        let directory = tempfile::tempdir().unwrap();
        let store = KvPersistence::open(directory.path()).unwrap();
        store
            .apply(Operation {
                lsn: 1,
                committed_lsn: 0,
                data: KvPersistence::encode_put("key".into(), "old".into()).unwrap(),
            })
            .await
            .unwrap();
        store
            .apply(Operation {
                lsn: 2,
                committed_lsn: 1,
                data: KvPersistence::encode_put("key".into(), "new".into()).unwrap(),
            })
            .await
            .unwrap();

        assert_eq!(
            store.snapshot_at(1).unwrap().get("key").map(String::as_str),
            Some("old")
        );
        assert_eq!(
            store.snapshot_at(2).unwrap().get("key").map(String::as_str),
            Some("new")
        );
    }

    #[tokio::test]
    async fn copied_baseline_survives_reopen_and_later_copy() {
        let first_directory = tempfile::tempdir().unwrap();
        let first = KvPersistence::open(first_directory.path()).unwrap();
        let first_build = OperationId::new("first-copy");
        first
            .apply_copy_chunk(
                &first_build,
                0,
                CopyChunk {
                    data: Bytes::from(
                        serde_json::to_vec(&BTreeMap::from([(
                            "copied".to_string(),
                            "baseline".to_string(),
                        )]))
                        .unwrap(),
                    ),
                },
            )
            .await
            .unwrap();
        first.finish_copy(&first_build, 7, 7).await.unwrap();
        first
            .apply(Operation {
                lsn: 8,
                committed_lsn: 7,
                data: KvPersistence::encode_put("later".into(), "value".into()).unwrap(),
            })
            .await
            .unwrap();
        first.commit(8).await.unwrap();

        assert_eq!(
            first.snapshot_at(7).unwrap(),
            BTreeMap::from([("copied".to_string(), "baseline".to_string())])
        );
        let expected = BTreeMap::from([
            ("copied".to_string(), "baseline".to_string()),
            ("later".to_string(), "value".to_string()),
        ]);
        assert_eq!(first.snapshot_at(8).unwrap(), expected);
        drop(first);

        let reopened = KvPersistence::open(first_directory.path()).unwrap();
        assert_eq!(reopened.snapshot_at(8).unwrap(), expected);

        let second_directory = tempfile::tempdir().unwrap();
        let second = KvPersistence::open(second_directory.path()).unwrap();
        let second_build = OperationId::new("second-copy");
        second
            .apply_copy_chunk(
                &second_build,
                0,
                CopyChunk {
                    data: Bytes::from(serde_json::to_vec(&expected).unwrap()),
                },
            )
            .await
            .unwrap();
        second.finish_copy(&second_build, 8, 8).await.unwrap();
        assert_eq!(second.snapshot_at(8).unwrap(), expected);
    }

    #[tokio::test]
    async fn incomplete_staging_chunk_is_replaced_and_not_installed() {
        let directory = tempfile::tempdir().unwrap();
        let store = KvPersistence::open(directory.path()).unwrap();
        let build = OperationId::new("interrupted-copy");
        let staging = store.copy_chunk_staging_path(&build, 0);
        std::fs::create_dir_all(staging.parent().unwrap()).unwrap();
        std::fs::write(&staging, b"partial").unwrap();

        let expected = BTreeMap::from([("key".to_string(), "value".to_string())]);
        let bytes = serde_json::to_vec(&expected).unwrap();
        store
            .apply_copy_chunk(
                &build,
                0,
                CopyChunk {
                    data: Bytes::from(bytes.clone()),
                },
            )
            .await
            .unwrap();
        assert!(!staging.exists());
        assert_eq!(
            std::fs::read(store.copy_chunk_path(&build, 0)).unwrap(),
            bytes
        );

        std::fs::write(store.copy_chunk_staging_path(&build, 1), b"ignored").unwrap();
        store.finish_copy(&build, 4, 4).await.unwrap();
        assert_eq!(store.snapshot_at(4).unwrap(), expected);
    }

    #[test]
    fn fresh_empty_proof_rejects_surviving_application_files() {
        let directory = tempfile::tempdir().unwrap();
        let application = directory.path().join("application");
        assert!(KvPersistence::is_fresh_empty(&application).unwrap());
        std::fs::create_dir_all(&application).unwrap();
        assert!(KvPersistence::is_fresh_empty(&application).unwrap());
        std::fs::write(application.join("state.json"), b"{}").unwrap();
        assert!(!KvPersistence::is_fresh_empty(&application).unwrap());
    }
}
