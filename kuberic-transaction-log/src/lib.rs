use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 4] = b"KTL1";
pub const MAX_RECORD: usize = 64 * 1024 * 1024;
pub const MAX_RETAINED_LOG: u64 = 64 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub lsn: i64,
    pub payload: Vec<u8>,
}

pub struct TransactionLog {
    root: PathBuf,
    generation: PathBuf,
    file: Option<File>,
    records: Vec<Record>,
    checkpoint: Option<Record>,
    failed: bool,
    _lock: File,
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub fn encode(record: &Record) -> io::Result<Vec<u8>> {
    if record.payload.len() > MAX_RECORD || record.lsn < 0 {
        return Err(invalid("invalid transaction log record"));
    }
    let mut bytes = Vec::with_capacity(record.payload.len() + 24);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&(record.payload.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&record.lsn.to_le_bytes());
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
    bytes.extend_from_slice(&record.payload);
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
    Ok(bytes)
}

fn read_record(file: &mut File) -> io::Result<Option<Record>> {
    let mut header = [0; 20];
    match file.read_exact(&mut header) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    if &header[..4] != MAGIC {
        return Err(invalid("transaction log format mismatch"));
    }
    if crc32fast::hash(&header[..16]) != u32::from_le_bytes(header[16..].try_into().unwrap()) {
        return Err(invalid("transaction log header checksum mismatch"));
    }
    let length = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
    if length > MAX_RECORD {
        return Err(invalid("transaction log record exceeds limit"));
    }
    let mut payload = vec![0; length + 4];
    match file.read_exact(&mut payload) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let expected = u32::from_le_bytes(payload[length..].try_into().unwrap());
    payload.truncate(length);
    let mut checksum = crc32fast::Hasher::new();
    checksum.update(&header);
    checksum.update(&payload);
    if checksum.finalize() != expected {
        return Err(invalid("transaction log checksum mismatch"));
    }
    let lsn = i64::from_le_bytes(header[8..16].try_into().unwrap());
    if lsn < 0 {
        return Err(invalid("negative transaction LSN"));
    }
    Ok(Some(Record { lsn, payload }))
}

pub fn atomic_write(path: &Path, contents: &[u8]) -> io::Result<()> {
    let root = path.parent().ok_or_else(|| invalid("path has no parent"))?;
    let mut temporary = tempfile::NamedTempFile::new_in(root)?;
    temporary.write_all(contents)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    #[cfg(unix)]
    File::open(root)?.sync_all()?;
    Ok(())
}

impl TransactionLog {
    pub fn open(root: PathBuf) -> io::Result<Self> {
        std::fs::create_dir_all(&root)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("lock"))?;
        fs2::FileExt::try_lock_exclusive(&lock)?;
        let generation = match std::fs::read_to_string(root.join("active")) {
            Ok(name) => {
                if !name.starts_with("generation-") || name.contains(['/', '\\', ':']) {
                    return Err(invalid("invalid log generation"));
                }
                let path = root.join(name);
                if !path.join("transactions.log").is_file() || !path.join("checkpoint").is_file() {
                    return Err(invalid("active log generation is incomplete"));
                }
                path
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => root.clone(),
            Err(error) => return Err(error),
        };
        let checkpoint = match File::open(generation.join("checkpoint")) {
            Ok(mut file) => {
                let checkpoint =
                    read_record(&mut file)?.ok_or_else(|| invalid("incomplete checkpoint"))?;
                if file.stream_position()? != file.metadata()?.len() {
                    return Err(invalid("trailing checkpoint bytes"));
                }
                Some(checkpoint)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(generation.join("transactions.log"))?;
        let mut records = Vec::new();
        let mut valid = 0;
        let mut lsn = checkpoint.as_ref().map_or(0, |record| record.lsn);
        while let Some(record) = read_record(&mut file)? {
            valid = file.stream_position()?;
            if record.lsn <= checkpoint.as_ref().map_or(0, |record| record.lsn) {
                continue;
            }
            if record.lsn != lsn + 1 {
                return Err(invalid("noncontiguous transaction log"));
            }
            lsn = record.lsn;
            records.push(record);
        }
        file.set_len(valid)?;
        file.sync_all()?;
        file.seek(SeekFrom::End(0))?;
        #[cfg(unix)]
        File::open(&generation)?.sync_all()?;
        Ok(Self {
            root,
            generation,
            file: Some(file),
            records,
            checkpoint,
            failed: false,
            _lock: lock,
        })
    }

    pub fn checkpoint_record(&self) -> Option<&Record> {
        self.checkpoint.as_ref()
    }
    pub fn records(&self) -> &[Record] {
        &self.records
    }
    pub fn last_lsn(&self) -> i64 {
        self.records
            .last()
            .or(self.checkpoint.as_ref())
            .map_or(0, |record| record.lsn)
    }

    pub fn append(&mut self, record: Record) -> io::Result<()> {
        if self.failed {
            return Err(invalid("log requires recovery"));
        }
        if record.lsn != self.last_lsn() + 1 {
            return Err(invalid("noncontiguous append"));
        }
        let bytes = encode(&record)?;
        self.check_capacity(record.payload.len())?;
        self.failed = true;
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| invalid("log requires recovery"))?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        self.records.push(record);
        self.failed = false;
        Ok(())
    }

    pub fn check_capacity(&self, payload_length: usize) -> io::Result<()> {
        if self.failed {
            return Err(invalid("log requires recovery"));
        }
        let length = self.retained_bytes()?;
        if payload_length > MAX_RECORD
            || length
                .saturating_add(payload_length as u64)
                .saturating_add(24)
                > MAX_RETAINED_LOG
        {
            return Err(io::Error::other(
                "retained log limit reached; checkpoint required",
            ));
        }
        Ok(())
    }

    pub fn retained_bytes(&self) -> io::Result<u64> {
        Ok(self
            .file
            .as_ref()
            .ok_or_else(|| invalid("log requires recovery"))?
            .metadata()?
            .len())
    }

    pub fn checkpoint(&mut self, snapshot: Record) -> io::Result<()> {
        if self.failed
            || snapshot.lsn < self.checkpoint.as_ref().map_or(0, |record| record.lsn)
            || snapshot.lsn > self.last_lsn()
        {
            return Err(invalid("invalid checkpoint boundary"));
        }
        let suffix = self
            .records
            .iter()
            .filter(|record| record.lsn > snapshot.lsn)
            .cloned()
            .collect();
        self.publish_generation(snapshot, suffix)
    }

    pub fn install_checkpoint(&mut self, snapshot: Record) -> io::Result<()> {
        self.publish_generation(snapshot, Vec::new())
    }

    fn publish_generation(&mut self, snapshot: Record, suffix: Vec<Record>) -> io::Result<()> {
        let bytes = encode(&snapshot)?;
        let temporary = tempfile::Builder::new()
            .prefix("generation-")
            .tempdir_in(&self.root)?;
        atomic_write(&temporary.path().join("checkpoint"), &bytes)?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(temporary.path().join("transactions.log"))?;
        for record in &suffix {
            file.write_all(&encode(record)?)?;
        }
        file.sync_all()?;
        #[cfg(unix)]
        File::open(temporary.path())?.sync_all()?;
        let generation = temporary.keep();
        self.failed = true;
        let name = generation
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| invalid("invalid log generation"))?;
        atomic_write(&self.root.join("active"), name.as_bytes())?;
        self.file = Some(file);
        let previous = std::mem::replace(&mut self.generation, generation);
        self.records = suffix;
        self.checkpoint = Some(snapshot);
        self.failed = false;
        if previous != self.root {
            let _ = std::fs::remove_dir_all(previous);
        } else {
            let _ = std::fs::remove_file(previous.join("transactions.log"));
        }
        Ok(())
    }

    pub fn rollback(&mut self, lsn: i64) -> io::Result<()> {
        if self.failed
            || lsn < self.checkpoint.as_ref().map_or(0, |record| record.lsn)
            || lsn > self.last_lsn()
        {
            return Err(invalid("rollback outside retained history"));
        }
        let mut bytes = Vec::new();
        for record in self.records.iter().filter(|record| record.lsn <= lsn) {
            bytes.extend_from_slice(&encode(record)?);
        }
        self.failed = true;
        drop(self.file.take());
        atomic_write(&self.generation.join("transactions.log"), &bytes)?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.generation.join("transactions.log"))?;
        file.seek(SeekFrom::End(0))?;
        self.file = Some(file);
        self.records.retain(|record| record.lsn <= lsn);
        self.failed = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_discards_only_torn_tail_and_checkpoint_reclaims_log() {
        let directory = tempfile::tempdir().unwrap();
        let mut log = TransactionLog::open(directory.path().into()).unwrap();
        log.append(Record {
            lsn: 1,
            payload: b"transaction".to_vec(),
        })
        .unwrap();
        log.file
            .as_mut()
            .unwrap()
            .write_all(
                &encode(&Record {
                    lsn: 2,
                    payload: b"incomplete".to_vec(),
                })
                .unwrap()[..18],
            )
            .unwrap();
        log.file.as_ref().unwrap().sync_all().unwrap();
        drop(log);
        let mut log = TransactionLog::open(directory.path().into()).unwrap();
        assert_eq!(log.last_lsn(), 1);
        assert_eq!(log.records().len(), 1);
        log.append(Record {
            lsn: 2,
            payload: b"complete".to_vec(),
        })
        .unwrap();
        log.rollback(1).unwrap();
        log.checkpoint(Record {
            lsn: 1,
            payload: b"snapshot".to_vec(),
        })
        .unwrap();
        assert!(log.records().is_empty());
        drop(log);
        let log = TransactionLog::open(directory.path().into()).unwrap();
        assert_eq!(log.checkpoint_record().unwrap().payload, b"snapshot");
        assert_eq!(log.file.as_ref().unwrap().metadata().unwrap().len(), 0);
    }

    #[test]
    fn corrupt_complete_record_fails_without_truncating_history() {
        let directory = tempfile::tempdir().unwrap();
        let mut bytes = encode(&Record {
            lsn: 1,
            payload: b"important".to_vec(),
        })
        .unwrap();
        bytes[20] ^= 1;
        std::fs::write(directory.path().join("transactions.log"), &bytes).unwrap();
        assert!(TransactionLog::open(directory.path().into()).is_err());
        assert_eq!(
            std::fs::read(directory.path().join("transactions.log")).unwrap(),
            bytes
        );
    }

    #[test]
    fn corrupt_length_is_not_treated_as_a_torn_tail() {
        let directory = tempfile::tempdir().unwrap();
        let mut bytes = encode(&Record {
            lsn: 1,
            payload: b"important".to_vec(),
        })
        .unwrap();
        bytes[4] = 120;
        std::fs::write(directory.path().join("transactions.log"), &bytes).unwrap();
        assert!(TransactionLog::open(directory.path().into()).is_err());
        assert_eq!(
            std::fs::read(directory.path().join("transactions.log")).unwrap(),
            bytes
        );
    }

    #[test]
    fn checkpoint_install_never_replays_an_old_generation_suffix() {
        let directory = tempfile::tempdir().unwrap();
        let mut log = TransactionLog::open(directory.path().into()).unwrap();
        for lsn in 1..=3 {
            log.append(Record {
                lsn,
                payload: vec![lsn as u8],
            })
            .unwrap();
        }
        let previous = std::fs::read(directory.path().join("transactions.log")).unwrap();
        log.install_checkpoint(Record {
            lsn: 1,
            payload: b"copied".to_vec(),
        })
        .unwrap();
        std::fs::write(directory.path().join("transactions.log"), previous).unwrap();
        drop(log);
        let log = TransactionLog::open(directory.path().into()).unwrap();
        assert_eq!(log.last_lsn(), 1);
        assert!(log.records().is_empty());
        assert_eq!(log.checkpoint_record().unwrap().payload, b"copied");
    }

    #[test]
    fn prefix_checkpoint_retains_unconfirmed_suffix_across_restart() {
        let directory = tempfile::tempdir().unwrap();
        let mut log = TransactionLog::open(directory.path().into()).unwrap();
        for lsn in 1..=3 {
            log.append(Record {
                lsn,
                payload: vec![lsn as u8],
            })
            .unwrap();
        }
        log.checkpoint(Record {
            lsn: 2,
            payload: b"confirmed".to_vec(),
        })
        .unwrap();
        assert_eq!(
            log.records(),
            &[Record {
                lsn: 3,
                payload: vec![3]
            }]
        );
        drop(log);
        let mut log = TransactionLog::open(directory.path().into()).unwrap();
        assert_eq!(log.checkpoint_record().unwrap().lsn, 2);
        assert_eq!(log.last_lsn(), 3);
        log.rollback(2).unwrap();
        assert!(log.records().is_empty());
    }

    #[test]
    fn failed_append_and_failed_checkpoint_do_not_discard_durable_history() {
        let directory = tempfile::tempdir().unwrap();
        let mut log = TransactionLog::open(directory.path().into()).unwrap();
        log.append(Record {
            lsn: 1,
            payload: b"durable".to_vec(),
        })
        .unwrap();
        log.file = Some(File::open(directory.path().join("transactions.log")).unwrap());
        assert!(
            log.append(Record {
                lsn: 2,
                payload: b"failed".to_vec()
            })
            .is_err()
        );
        assert_eq!(log.last_lsn(), 1);
        assert!(
            log.append(Record {
                lsn: 2,
                payload: vec![]
            })
            .is_err()
        );
        drop(log);
        let mut log = TransactionLog::open(directory.path().into()).unwrap();
        std::fs::create_dir(directory.path().join("active")).unwrap();
        assert!(
            log.checkpoint(Record {
                lsn: 1,
                payload: b"snapshot".to_vec()
            })
            .is_err()
        );
        assert_eq!(log.records().len(), 1);
        std::fs::remove_dir(directory.path().join("active")).unwrap();
        drop(log);
        let log = TransactionLog::open(directory.path().into()).unwrap();
        assert_eq!(log.records()[0].payload, b"durable");
        assert_eq!(log.last_lsn(), 1);
    }
}
