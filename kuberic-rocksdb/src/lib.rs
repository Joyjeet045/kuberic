use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use rocksdb::{DB, Options, WriteBatch, WriteOptions};

mod replica;
pub use replica::RocksReplica;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("RocksDB: {0}")]
    Database(#[from] rocksdb::Error),
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid record: {0}")]
    Invalid(String),
    #[error("serialization: {0}")]
    Encoding(#[from] postcard::Error),
    #[error("replication: {0}")]
    Replication(#[from] kuberic_core::KubericError),
    #[error("replica requires recovery")]
    RecoveryRequired,
    #[error("write access is not granted")]
    NotPrimary,
    #[error("too many pending writes")]
    Busy,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum Mutation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Merge { key: Vec<u8>, value: Vec<u8> },
}

const LSN_KEY: &[u8] = b"\0lsn";
const PROFILE_KEY: &[u8] = b"\0profile";
const PROFILE: &str = "kuberic-rocksdb/1;rocksdb=10.4.2;cf=default;comparator=bytewise;merge=append-v1;compression=none";
const MAX_BATCH: usize = 1024 * 1024;
const MAX_COPY: usize = 256 * 1024 * 1024;

#[derive(serde::Serialize, serde::Deserialize)]
struct Envelope {
    profile: String,
    mutations: Vec<Mutation>,
    batch: Vec<u8>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct History {
    record: Vec<u8>,
    previous: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct CopyState {
    profile: String,
    lsn: i64,
    files: BTreeMap<String, Vec<u8>>,
}

fn encode<T: serde::Serialize>(value: &T, limit: usize) -> Result<Vec<u8>> {
    let payload = postcard::to_allocvec(value)?;
    if payload.len() > limit {
        return Err(Error::Invalid("record exceeds size limit".into()));
    }
    let mut record = crc32fast::hash(&payload).to_le_bytes().to_vec();
    record.extend_from_slice(&payload);
    Ok(record)
}

fn decode<T: serde::de::DeserializeOwned>(record: &[u8], limit: usize) -> Result<T> {
    if record.len() < 4 || record.len() > limit + 4 {
        return Err(Error::Invalid("invalid record length".into()));
    }
    let checksum = u32::from_le_bytes(record[..4].try_into().unwrap());
    if crc32fast::hash(&record[4..]) != checksum {
        return Err(Error::Invalid("record checksum mismatch".into()));
    }
    Ok(postcard::from_bytes(&record[4..])?)
}

fn make_batch(mutations: &[Mutation]) -> Result<WriteBatch> {
    if mutations.len() > 1024 {
        return Err(Error::Invalid("batch exceeds 1024 mutations".into()));
    }
    let mut batch = WriteBatch::default();
    for mutation in mutations {
        match mutation {
            Mutation::Put { key, value } => batch.put(user_key(key), value),
            Mutation::Delete { key } => batch.delete(user_key(key)),
            Mutation::Merge { key, value } => batch.merge(user_key(key), value),
        }
        if batch.size_in_bytes() > MAX_BATCH / 2 {
            return Err(Error::Invalid("batch exceeds payload limit".into()));
        }
    }
    Ok(batch)
}

fn record(mutations: Vec<Mutation>) -> Result<Vec<u8>> {
    let batch = make_batch(&mutations)?.data().to_vec();
    encode(
        &Envelope {
            profile: PROFILE.into(),
            mutations,
            batch,
        },
        MAX_BATCH,
    )
}

fn history_key(lsn: i64) -> Vec<u8> {
    let mut key = b"\0history".to_vec();
    key.extend_from_slice(&lsn.to_be_bytes());
    key
}

fn sync_write(database: &DB, batch: WriteBatch) -> Result<()> {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options.disable_wal(false);
    database.write_opt(batch, &options)?;
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    std::fs::File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

struct Store {
    database: DB,
    lsn: i64,
    path: PathBuf,
}

fn user_key(key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(key.len() + 1);
    encoded.push(1);
    encoded.extend_from_slice(key);
    encoded
}

impl Store {
    fn open(path: &Path) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_compression_type(rocksdb::DBCompressionType::None);
        options.set_merge_operator_associative("append-v1", |_key, existing, operands| {
            let mut value = existing.unwrap_or_default().to_vec();
            for operand in operands {
                value.extend_from_slice(operand);
            }
            Some(value)
        });
        let database = DB::open(&options, path)?;
        match database.get(PROFILE_KEY)? {
            Some(profile) if profile != PROFILE.as_bytes() => {
                return Err(Error::Invalid("database configuration mismatch".into()));
            }
            Some(_) => {}
            None => {
                if database
                    .iterator(rocksdb::IteratorMode::Start)
                    .next()
                    .is_some()
                {
                    return Err(Error::Invalid(
                        "database was not created by this adapter".into(),
                    ));
                }
                let mut batch = WriteBatch::default();
                batch.put(PROFILE_KEY, PROFILE.as_bytes());
                sync_write(&database, batch)?;
            }
        }
        let lsn = match database.get(LSN_KEY)? {
            Some(bytes) => i64::from_le_bytes(
                bytes
                    .try_into()
                    .map_err(|_| Error::Invalid("invalid persisted LSN".into()))?,
            ),
            None => 0,
        };
        if lsn < 0 {
            return Err(Error::Invalid("negative persisted LSN".into()));
        }
        Ok(Self {
            database,
            lsn,
            path: path.into(),
        })
    }

    #[cfg(test)]
    fn apply(&mut self, lsn: i64, mutations: &[Mutation]) -> Result<()> {
        self.apply_record(lsn, &record(mutations.to_vec())?)
    }

    fn apply_record(&mut self, lsn: i64, record: &[u8]) -> Result<()> {
        let envelope: Envelope = decode(record, MAX_BATCH)?;
        if envelope.profile != PROFILE || make_batch(&envelope.mutations)?.data() != envelope.batch
        {
            return Err(Error::Invalid(
                "unsupported batch or configuration mismatch".into(),
            ));
        }
        if lsn <= self.lsn {
            let history = self
                .database
                .get(history_key(lsn))?
                .ok_or_else(|| Error::Invalid("duplicate LSN has no retained identity".into()))?;
            let history: History = postcard::from_bytes(&history)?;
            return if history.record == record {
                Ok(())
            } else {
                Err(Error::Invalid(
                    "conflicting record at an applied LSN".into(),
                ))
            };
        }
        if lsn != self.lsn + 1 {
            return Err(Error::Invalid("noncontiguous LSN".into()));
        }
        let mut previous = BTreeMap::new();
        for mutation in &envelope.mutations {
            let key = match mutation {
                Mutation::Put { key, .. }
                | Mutation::Delete { key }
                | Mutation::Merge { key, .. } => user_key(key),
            };
            if !previous.contains_key(&key) {
                previous.insert(key.clone(), self.database.get(&key)?);
            }
        }
        let mut batch = WriteBatch::from_data(&envelope.batch);
        batch.put(LSN_KEY, lsn.to_le_bytes());
        batch.put(
            history_key(lsn),
            postcard::to_allocvec(&History {
                record: record.into(),
                previous,
            })?,
        );
        sync_write(&self.database, batch)?;
        self.lsn = lsn;
        Ok(())
    }

    fn rollback(&mut self, target: i64) -> Result<()> {
        if target < 0 || target > self.lsn {
            return Err(Error::Invalid("invalid rollback boundary".into()));
        }
        let mut batch = WriteBatch::default();
        for lsn in (target + 1..=self.lsn).rev() {
            let history = self.database.get(history_key(lsn))?.ok_or_else(|| {
                Error::Invalid("rollback history unavailable; checkpoint rebuild required".into())
            })?;
            let history: History = postcard::from_bytes(&history)?;
            for (key, value) in history.previous {
                match value {
                    Some(value) => batch.put(key, value),
                    None => batch.delete(key),
                }
            }
            batch.delete(history_key(lsn));
        }
        batch.put(LSN_KEY, target.to_le_bytes());
        sync_write(&self.database, batch)?;
        self.lsn = target;
        Ok(())
    }

    fn copy_at(&self, target: i64) -> Result<Vec<u8>> {
        let temporary = tempfile::tempdir_in(self.path.parent().unwrap())?;
        let path = temporary.path().join("checkpoint");
        rocksdb::checkpoint::Checkpoint::new(&self.database)?.create_checkpoint(&path)?;
        let mut checkpoint = Self::open(&path)?;
        checkpoint.rollback(target)?;
        checkpoint.database.flush()?;
        drop(checkpoint);
        let mut files = BTreeMap::new();
        let mut total = 0usize;
        for entry in std::fs::read_dir(&path)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                return Err(Error::Invalid("checkpoint contains non-file entry".into()));
            }
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| Error::Invalid("invalid checkpoint filename".into()))?;
            if name == "LOCK" {
                continue;
            }
            total = total
                .checked_add(entry.metadata()?.len() as usize)
                .ok_or_else(|| Error::Invalid("checkpoint too large".into()))?;
            if total > MAX_COPY {
                return Err(Error::Invalid("checkpoint exceeds 256 MiB limit".into()));
            }
            files.insert(name, std::fs::read(entry.path())?);
        }
        encode(
            &CopyState {
                profile: PROFILE.into(),
                lsn: target,
                files,
            },
            MAX_COPY,
        )
    }
}

fn publish_database(root: &Path, name: &str) -> Result<()> {
    use std::io::Write;
    let mut file = tempfile::NamedTempFile::new_in(root)?;
    file.write_all(name.as_bytes())?;
    file.as_file().sync_all()?;
    file.persist(root.join("active"))
        .map_err(|error| error.error)?;
    sync_directory(root)
}

fn filename(name: &str) -> Result<()> {
    if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\\', ':']) {
        return Err(Error::Invalid("unsafe checkpoint filename".into()));
    }
    Ok(())
}

fn restore(root: &Path, record: &[u8], expected_lsn: i64) -> Result<Store> {
    let copy: CopyState = decode(record, MAX_COPY)?;
    if copy.profile != PROFILE || copy.lsn != expected_lsn {
        return Err(Error::Invalid(
            "checkpoint configuration or LSN mismatch".into(),
        ));
    }
    let temporary = tempfile::Builder::new()
        .prefix("database-")
        .tempdir_in(root)?;
    for (name, data) in copy.files {
        filename(&name)?;
        let path = temporary.path().join(name);
        use std::io::Write;
        let mut file = std::fs::File::create(&path)?;
        file.write_all(&data)?;
        file.sync_all()?;
    }
    if !temporary.path().join("CURRENT").is_file() {
        return Err(Error::Invalid(
            "checkpoint is missing RocksDB CURRENT".into(),
        ));
    }
    let store = Store::open(temporary.path())?;
    if store.lsn != expected_lsn {
        return Err(Error::Invalid("checkpoint LSN metadata mismatch".into()));
    }
    sync_directory(temporary.path())?;
    let directory = temporary.keep();
    publish_database(root, directory.file_name().unwrap().to_str().unwrap())?;
    Ok(store)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_copy_merge_rollback_and_corruption() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("source")).unwrap();
        store
            .apply(
                1,
                &[Mutation::Put {
                    key: b"key".to_vec(),
                    value: b"base".to_vec(),
                }],
            )
            .unwrap();
        store
            .apply(
                2,
                &[Mutation::Merge {
                    key: b"key".to_vec(),
                    value: b"+tail".to_vec(),
                }],
            )
            .unwrap();
        let copy = store.copy_at(1).unwrap();
        let destination = directory.path().join("destination");
        std::fs::create_dir(&destination).unwrap();
        let mut restored = restore(&destination, &copy, 1).unwrap();
        assert_eq!(restored.lsn, 1);
        assert_eq!(
            restored.database.get(user_key(b"key")).unwrap(),
            Some(b"base".to_vec())
        );
        restored
            .apply(
                2,
                &[Mutation::Merge {
                    key: b"key".to_vec(),
                    value: b"+tail".to_vec(),
                }],
            )
            .unwrap();
        assert_eq!(
            restored.database.get(user_key(b"key")).unwrap(),
            Some(b"base+tail".to_vec())
        );
        restored.rollback(0).unwrap();
        assert_eq!(restored.database.get(user_key(b"key")).unwrap(), None);
        let mut invalid = copy.clone();
        invalid[5] ^= 1;
        assert!(restore(&destination, &invalid, 1).is_err());
        assert!(restore(&destination, &copy, 2).is_err());
        let mut mismatch: CopyState = decode(&copy, MAX_COPY).unwrap();
        mismatch.profile = "incompatible".into();
        assert!(restore(&destination, &encode(&mismatch, MAX_COPY).unwrap(), 1).is_err());
        let mut malicious: CopyState = decode(&copy, MAX_COPY).unwrap();
        malicious.files.insert("../outside".into(), Vec::new());
        assert!(restore(&destination, &encode(&malicious, MAX_COPY).unwrap(), 1).is_err());
        let mut batch: Envelope = decode(&record(vec![]).unwrap(), MAX_BATCH).unwrap();
        batch.batch = vec![0];
        assert!(
            store
                .apply_record(3, &encode(&batch, MAX_BATCH).unwrap())
                .is_err()
        );
        assert_eq!(store.lsn, 2);
    }

    #[test]
    fn crash_writer() {
        let Some(path) = std::env::var_os("KUBERIC_ROCKSDB_CRASH_PATH") else {
            return;
        };
        let mut store = Store::open(Path::new(&path)).unwrap();
        store
            .apply(
                1,
                &[
                    Mutation::Put {
                        key: b"left".to_vec(),
                        value: b"durable".to_vec(),
                    },
                    Mutation::Put {
                        key: b"right".to_vec(),
                        value: b"durable".to_vec(),
                    },
                ],
            )
            .unwrap();
        std::process::exit(0);
    }

    #[test]
    fn abrupt_process_exit_recovers_the_entire_batch() {
        let directory = tempfile::tempdir().unwrap();
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "tests::crash_writer"])
            .env("KUBERIC_ROCKSDB_CRASH_PATH", directory.path())
            .status()
            .unwrap();
        assert!(child.success());
        let store = Store::open(directory.path()).unwrap();
        assert_eq!(store.lsn, 1);
        for key in [b"left".as_slice(), b"right".as_slice()] {
            assert_eq!(
                store.database.get(user_key(key)).unwrap(),
                Some(b"durable".to_vec())
            );
        }
    }

    #[test]
    fn batch_and_replication_lsn_recover_atomically() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = Store::open(directory.path()).unwrap();
        let mutations = vec![
            Mutation::Put {
                key: b"first".to_vec(),
                value: b"one".to_vec(),
            },
            Mutation::Put {
                key: b"second".to_vec(),
                value: b"two".to_vec(),
            },
            Mutation::Delete {
                key: b"first".to_vec(),
            },
        ];
        store.apply(1, &mutations).unwrap();
        drop(store);
        let mut store = Store::open(directory.path()).unwrap();
        assert_eq!(store.lsn, 1);
        assert_eq!(store.database.get(user_key(b"first")).unwrap(), None);
        assert_eq!(
            store.database.get(user_key(b"second")).unwrap(),
            Some(b"two".to_vec())
        );
        store.apply(1, &mutations).unwrap();
        assert!(
            store
                .apply(
                    1,
                    &[Mutation::Delete {
                        key: b"second".to_vec()
                    }]
                )
                .is_err()
        );
        assert_eq!(
            store.database.get(user_key(b"second")).unwrap(),
            Some(b"two".to_vec())
        );
        assert!(store.apply(3, &mutations).is_err());
    }
}
