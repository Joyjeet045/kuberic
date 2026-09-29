//! Versioned base plus exact history. The fsynced manifest is the only publication
//! point: log tails outside its length are unaccepted, never inferred as progress.
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use kuberic_runtime::application::{DurableApplicationProgress, Operation};
use serde::{Deserialize, Serialize};

use crate::frames::WalFrameSet;

const VERSION: u32 = 2;
const MANIFEST: &str = "state-v2.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum RecoveryState {
    #[default]
    Healthy,
    ReconciliationRequired(String),
    RebuildRequired(String),
    /// A committed snapshot is installed, but the accepted rebuild must still
    /// recover the old durable suffix before SQL can be materialized.
    Rebuilding {
        required_applied_lsn: i64,
        required_committed_lsn: i64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Record {
    pub lsn: i64,
    pub committed_lsn: i64,
    pub data: Vec<u8>,
}

impl Record {
    pub fn operation(&self) -> Operation {
        Operation {
            lsn: self.lsn,
            committed_lsn: self.committed_lsn,
            data: self.data.clone().into(),
        }
    }

    fn frames(&self) -> io::Result<WalFrameSet> {
        let frames: WalFrameSet = serde_json::from_slice(&self.data).map_err(io::Error::other)?;
        frames.page_size()?;
        if self.lsn <= 0
            || self.lsn == i64::MAX
            || self.committed_lsn < 0
            || self.committed_lsn > self.lsn
        {
            return Err(io::Error::other("invalid operation progress"));
        }
        Ok(frames)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CopyImage {
    version: u32,
    up_to_lsn: i64,
    page_size: usize,
    image: Vec<u8>,
}

#[derive(Clone, Serialize, Deserialize)]
struct CompletedCopy {
    up_to_lsn: i64,
    // Exact retry evidence survives staging cleanup and subsequent catch-up.
    chunks: BTreeMap<u64, Vec<u8>>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Manifest {
    version: u32,
    generation: u64,
    base_lsn: i64,
    base_checksum: u32,
    page_size: usize,
    applied_lsn: i64,
    committed_lsn: i64,
    history_len: u64,
    recovery: RecoveryState,
    staging: BTreeMap<String, BTreeMap<u64, Vec<u8>>>,
    completed: BTreeMap<String, CompletedCopy>,
}

#[derive(Serialize, Deserialize)]
struct CheckedManifest {
    state: Manifest,
    checksum: u32,
}

fn manifest_bytes(meta: &Manifest) -> io::Result<Vec<u8>> {
    let bytes = serde_json::to_vec(meta).map_err(io::Error::other)?;
    serde_json::to_vec(&CheckedManifest {
        state: meta.clone(),
        checksum: crc32fast::hash(&bytes),
    })
    .map_err(io::Error::other)
}

pub(crate) struct DurableFrameLog {
    root: PathBuf,
    meta: Manifest,
    base: Vec<u8>,
    operations: BTreeMap<i64, Record>,
}

pub(crate) fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        // Path::parent("data") is the empty path, which denotes the caller's cwd.
        let directory = if path.as_os_str().is_empty() {
            Path::new(".")
        } else {
            path
        };
        File::open(directory)?.sync_all()?;
    }
    Ok(())
}

pub(crate) fn replace(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let temporary = path.with_extension("tmp");
    let mut file = File::create(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(temporary, path)?;
    sync_directory(
        path.parent()
            .ok_or_else(|| io::Error::other("missing parent"))?,
    )
}

impl DurableFrameLog {
    pub fn open(root: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&root)?;
        sync_directory(&root)?;
        if let Some(parent) = root.parent() {
            sync_directory(parent)?;
        }
        let path = root.join(MANIFEST);
        let meta: Manifest = if path.exists() {
            let checked: CheckedManifest =
                serde_json::from_slice(&fs::read(&path)?).map_err(io::Error::other)?;
            let bytes = serde_json::to_vec(&checked.state).map_err(io::Error::other)?;
            if crc32fast::hash(&bytes) != checked.checksum {
                return Err(io::Error::other("v2 metadata checksum mismatch"));
            }
            checked.state
        } else {
            // Missing metadata on established storage is not a fresh replica.
            if fs::read_dir(&root)?.next().is_some() {
                return Err(io::Error::other(
                    "established or classic storage lacks v2 metadata",
                ));
            }
            let meta = Manifest {
                version: VERSION,
                generation: 0,
                base_lsn: 0,
                base_checksum: crc32fast::hash(&[]),
                page_size: 0,
                applied_lsn: 0,
                committed_lsn: 0,
                history_len: 0,
                recovery: RecoveryState::Healthy,
                staging: BTreeMap::new(),
                completed: BTreeMap::new(),
            };
            replace(&root.join("base-0.sqlite"), &[])?;
            replace(&root.join("history-0.log"), &[])?;
            replace(&path, &manifest_bytes(&meta)?)?;
            meta
        };
        if meta.version != VERSION
            || meta.base_lsn < 0
            || meta.committed_lsn < meta.base_lsn
            || meta.applied_lsn < meta.committed_lsn
            || meta.applied_lsn == i64::MAX
        {
            return Err(io::Error::other("invalid v2 storage version or progress"));
        }
        let mut log = Self {
            root,
            meta,
            base: Vec::new(),
            operations: BTreeMap::new(),
        };
        if let Err(error) = log.recover() {
            let mut meta = log.meta.clone();
            if let RecoveryState::Rebuilding {
                required_applied_lsn,
                required_committed_lsn,
            } = meta.recovery
            {
                meta.applied_lsn = meta.applied_lsn.max(required_applied_lsn);
                meta.committed_lsn = meta.committed_lsn.max(required_committed_lsn);
            }
            meta.recovery = RecoveryState::RebuildRequired(error.to_string());
            log.publish(meta)?;
        }
        Ok(log)
    }

    fn base_path(&self) -> PathBuf {
        self.root
            .join(format!("base-{}.sqlite", self.meta.generation))
    }
    fn history_path(&self) -> PathBuf {
        self.root
            .join(format!("history-{}.log", self.meta.generation))
    }

    fn recover(&mut self) -> io::Result<()> {
        self.base = fs::read(self.base_path())?;
        if crc32fast::hash(&self.base) != self.meta.base_checksum
            || (!self.base.is_empty()
                && (self.meta.page_size == 0
                    || !self.base.len().is_multiple_of(self.meta.page_size)))
        {
            return Err(io::Error::other("durable base corruption"));
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.history_path())?;
        if file.metadata()?.len() < self.meta.history_len {
            return Err(io::Error::other("acknowledged history is missing"));
        }
        let mut remaining = self.meta.history_len;
        let mut expected = self.meta.base_lsn + 1;
        while remaining > 0 {
            if remaining < 8 {
                return Err(io::Error::other("truncated acknowledged record"));
            }
            let mut number = [0; 4];
            file.read_exact(&mut number)?;
            let len = u32::from_le_bytes(number) as u64;
            if len > remaining - 8 {
                return Err(io::Error::other("truncated acknowledged payload"));
            }
            let mut bytes = vec![0; len as usize];
            file.read_exact(&mut bytes)?;
            file.read_exact(&mut number)?;
            if crc32fast::hash(&bytes) != u32::from_le_bytes(number) {
                return Err(io::Error::other("acknowledged history checksum mismatch"));
            }
            let record: Record = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
            if record.lsn != expected || record.frames()?.page_size()? != self.meta.page_size {
                return Err(io::Error::other(
                    "acknowledged history gap or geometry conflict",
                ));
            }
            self.operations.insert(expected, record);
            expected += 1;
            remaining -= len + 8;
        }
        if expected - 1 != self.meta.applied_lsn {
            return Err(io::Error::other(
                "history does not cover durable applied progress",
            ));
        }
        // Only bytes beyond the recorded applied boundary may be discarded.
        file.set_len(self.meta.history_len)?;
        file.sync_all()?;
        Ok(())
    }

    fn publish(&mut self, meta: Manifest) -> io::Result<()> {
        replace(&self.root.join(MANIFEST), &manifest_bytes(&meta)?)?;
        self.meta = meta;
        Ok(())
    }

    pub fn progress(&self) -> DurableApplicationProgress {
        DurableApplicationProgress {
            applied_lsn: self.meta.applied_lsn,
            committed_lsn: self.meta.committed_lsn,
        }
    }

    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn base_lsn_for_test(&self) -> i64 {
        self.meta.base_lsn
    }

    pub fn recovery(&self) -> RecoveryState {
        self.meta.recovery.clone()
    }

    fn intact(&self) -> io::Result<()> {
        if matches!(self.meta.recovery, RecoveryState::RebuildRequired(_)) {
            Err(io::Error::other("replica requires a complete rebuild"))
        } else {
            Ok(())
        }
    }

    pub fn reconcile(&mut self, reason: String) -> io::Result<()> {
        self.intact()?;
        if matches!(self.meta.recovery, RecoveryState::Rebuilding { .. }) {
            return Err(io::Error::other("rebuild catch-up is not complete"));
        }
        let mut meta = self.meta.clone();
        meta.recovery = RecoveryState::ReconciliationRequired(reason);
        self.publish(meta)
    }

    pub fn clear_reconciliation(&mut self) -> io::Result<()> {
        self.intact()?;
        if matches!(self.meta.recovery, RecoveryState::Rebuilding { .. }) {
            return Err(io::Error::other("rebuild catch-up is not complete"));
        }
        let mut meta = self.meta.clone();
        meta.recovery = RecoveryState::Healthy;
        self.publish(meta)
    }

    pub fn verify(&self, operation: &Operation) -> io::Result<bool> {
        self.intact()?;
        Ok(self.operations.get(&operation.lsn).is_some_and(|r| {
            r.committed_lsn == operation.committed_lsn && r.data == operation.data
        }))
    }

    pub fn apply(&mut self, operation: Operation) -> io::Result<DurableApplicationProgress> {
        self.intact()?;
        if operation.lsn <= self.meta.applied_lsn {
            if !self.verify(&operation)? {
                return Err(io::Error::other("conflicting operation retry"));
            }
            return Ok(self.progress());
        }
        if operation.lsn != self.meta.applied_lsn + 1 {
            return Err(io::Error::other("operation gap"));
        }
        let record = Record {
            lsn: operation.lsn,
            committed_lsn: operation.committed_lsn,
            data: operation.data.to_vec(),
        };
        let page_size = record.frames()?.page_size()?;
        if self.meta.page_size != 0 && self.meta.page_size != page_size {
            return Err(io::Error::other("operation page size changed"));
        }
        let bytes = encode_record(&record)?;
        let file = OpenOptions::new().write(true).open(self.history_path())?;
        file.set_len(self.meta.history_len)?;
        drop(file);
        let mut file = OpenOptions::new().append(true).open(self.history_path())?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        let mut meta = self.meta.clone();
        meta.page_size = page_size;
        meta.applied_lsn = record.lsn;
        meta.committed_lsn = meta.committed_lsn.max(record.committed_lsn);
        meta.history_len += bytes.len() as u64;
        settle_rebuild(&mut meta);
        self.publish(meta)?;
        self.operations.insert(record.lsn, record);
        Ok(self.progress())
    }

    pub fn commit(&mut self, lsn: i64) -> io::Result<DurableApplicationProgress> {
        self.intact()?;
        if lsn < 0 || lsn > self.meta.applied_lsn {
            return Err(io::Error::other("invalid committed progress"));
        }
        if lsn > self.meta.committed_lsn {
            let mut meta = self.meta.clone();
            meta.committed_lsn = lsn;
            settle_rebuild(&mut meta);
            self.publish(meta)?;
        }
        Ok(self.progress())
    }

    pub fn retained(&self, from: i64, to: i64) -> io::Result<Vec<Operation>> {
        self.intact()?;
        if from > to {
            return Ok(Vec::new());
        }
        if from <= self.meta.base_lsn || to > self.meta.applied_lsn {
            return Err(io::Error::other("requested history is not retained"));
        }
        (from..=to)
            .map(|lsn| {
                self.operations
                    .get(&lsn)
                    .map(Record::operation)
                    .ok_or_else(|| io::Error::other("retained history gap"))
            })
            .collect()
    }

    pub fn image_at(&self, lsn: i64) -> io::Result<Vec<u8>> {
        self.intact()?;
        if matches!(self.meta.recovery, RecoveryState::Rebuilding { .. }) {
            return Err(io::Error::other("rebuild catch-up is not complete"));
        }
        if lsn < self.meta.base_lsn || lsn > self.meta.applied_lsn {
            return Err(io::Error::other(
                "snapshot boundary outside retained history",
            ));
        }
        let mut image = self.base.clone();
        for operation in self.retained(self.meta.base_lsn + 1, lsn)? {
            let frames: WalFrameSet =
                serde_json::from_slice(&operation.data).map_err(io::Error::other)?;
            frames.apply_to_image(&mut image, self.meta.page_size)?;
        }
        Ok(image)
    }

    pub fn snapshot(&self, up_to_lsn: i64) -> io::Result<Vec<u8>> {
        if up_to_lsn > self.meta.committed_lsn {
            return Err(io::Error::other("snapshot boundary is not committed"));
        }
        let image = self.image_at(up_to_lsn)?;
        serde_json::to_vec(&CopyImage {
            version: VERSION,
            up_to_lsn,
            page_size: if up_to_lsn == 0 {
                0
            } else {
                self.meta.page_size
            },
            image,
        })
        .map_err(io::Error::other)
    }

    pub fn stage(&mut self, build: &str, sequence: u64, bytes: Vec<u8>) -> io::Result<()> {
        if build.is_empty() || sequence == 0 {
            return Err(io::Error::other("invalid copy sequence"));
        }
        if let Some(existing) = self.chunks(build).and_then(|chunks| chunks.get(&sequence)) {
            return if *existing == bytes {
                Ok(())
            } else {
                Err(io::Error::other("conflicting copy chunk"))
            };
        }
        if self.meta.completed.contains_key(build) {
            return Err(io::Error::other("completed build cannot be extended"));
        }
        let mut meta = self.meta.clone();
        let chunks = meta.staging.entry(build.to_owned()).or_default();
        if sequence != chunks.len() as u64 + 1 {
            return Err(io::Error::other("copy sequence gap"));
        }
        chunks.insert(sequence, bytes);
        self.publish(meta)
    }

    fn chunks(&self, build: &str) -> Option<&BTreeMap<u64, Vec<u8>>> {
        self.meta
            .staging
            .get(build)
            .or_else(|| self.meta.completed.get(build).map(|c| &c.chunks))
    }

    pub fn verify_chunk(&self, build: &str, sequence: u64, bytes: &[u8]) -> bool {
        self.chunks(build)
            .and_then(|c| c.get(&sequence))
            .is_some_and(|b| b == bytes)
    }

    pub fn finish(
        &mut self,
        build: &str,
        up_to_lsn: i64,
        committed_lsn: i64,
    ) -> io::Result<DurableApplicationProgress> {
        if up_to_lsn != committed_lsn {
            return Err(io::Error::other(
                "copy completion must equal the committed snapshot boundary",
            ));
        }
        if let Some(completed) = self.meta.completed.get(build) {
            if completed.up_to_lsn != up_to_lsn {
                return Err(io::Error::other("conflicting copy completion"));
            }
            self.intact()?;
            return Ok(self.progress());
        }
        let chunks = self
            .meta
            .staging
            .get(build)
            .ok_or_else(|| io::Error::other("copy has no staged snapshot"))?
            .clone();
        let bytes: Vec<u8> = chunks.values().flatten().copied().collect();
        let snapshot: CopyImage = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
        if snapshot.version != VERSION
            || snapshot.up_to_lsn != up_to_lsn
            || up_to_lsn < 0
            || up_to_lsn == i64::MAX
            || (up_to_lsn == 0) != snapshot.image.is_empty()
            || (up_to_lsn == 0 && snapshot.page_size != 0)
            || (up_to_lsn > 0
                && (!(512..=65536).contains(&snapshot.page_size)
                    || !snapshot.page_size.is_power_of_two()))
            || (!snapshot.image.is_empty()
                && (snapshot.page_size == 0
                    || !snapshot.image.len().is_multiple_of(snapshot.page_size)))
        {
            return Err(io::Error::other(
                "invalid copy representation or completion boundary",
            ));
        }
        let mut operations = BTreeMap::new();
        let mut expected = up_to_lsn + 1;
        // A replay may arrive after catch-up but before durable agent completion.
        // Keep later contiguous operations, never overwrite conflicts or regress commitment.
        let intact = !matches!(
            self.meta.recovery,
            RecoveryState::RebuildRequired(_) | RecoveryState::Rebuilding { .. }
        );
        if intact {
            if self.meta.base_lsn <= up_to_lsn
                && self.meta.applied_lsn >= up_to_lsn
                && self.image_at(up_to_lsn)? != snapshot.image
            {
                return Err(io::Error::other("copy conflicts with committed base"));
            }
            for (lsn, record) in self.operations.range((up_to_lsn + 1)..) {
                if *lsn != expected {
                    return Err(io::Error::other("post-copy history gap"));
                }
                operations.insert(*lsn, record.clone());
                expected += 1;
            }
            if self.meta.committed_lsn > expected - 1 {
                return Err(io::Error::other("copy would regress committed progress"));
            }
        }
        let mut meta = self.meta.clone();
        meta.generation = meta
            .generation
            .checked_add(1)
            .ok_or_else(|| io::Error::other("generation exhausted"))?;
        meta.base_lsn = up_to_lsn;
        meta.base_checksum = crc32fast::hash(&snapshot.image);
        // A boundary-zero snapshot carries no geometry. Later accepted history
        // still owns its original page size when reinstalling that empty base.
        meta.page_size = if up_to_lsn == 0 && !operations.is_empty() {
            self.meta.page_size
        } else {
            snapshot.page_size
        };
        if operations.values().any(|record| {
            record.frames().and_then(|frames| frames.page_size()).ok() != Some(meta.page_size)
        }) {
            return Err(io::Error::other(
                "retained suffix differs from copy page size",
            ));
        }
        meta.applied_lsn = expected - 1;
        meta.committed_lsn = if intact {
            self.meta.committed_lsn.max(up_to_lsn)
        } else {
            up_to_lsn
        };
        let history: Vec<u8> = operations
            .values()
            .map(encode_record)
            .collect::<io::Result<Vec<_>>>()?
            .concat();
        meta.history_len = history.len() as u64;
        meta.recovery = if intact {
            RecoveryState::Healthy
        } else {
            match &self.meta.recovery {
                RecoveryState::Rebuilding { .. } => self.meta.recovery.clone(),
                _ => RecoveryState::Rebuilding {
                    required_applied_lsn: self.meta.applied_lsn,
                    required_committed_lsn: self.meta.committed_lsn,
                },
            }
        };
        settle_rebuild(&mut meta);
        meta.staging.remove(build);
        meta.completed
            .insert(build.to_owned(), CompletedCopy { up_to_lsn, chunks });
        replace(
            &self.root.join(format!("base-{}.sqlite", meta.generation)),
            &snapshot.image,
        )?;
        replace(
            &self.root.join(format!("history-{}.log", meta.generation)),
            &history,
        )?;
        let old_base = self.base_path();
        let old_history = self.history_path();
        self.publish(meta)?;
        self.base = snapshot.image;
        self.operations = operations;
        // Cleanup is not the commit point; old generations are never loaded again.
        let _ = fs::remove_file(old_base);
        let _ = fs::remove_file(old_history);
        sync_directory(&self.root)?;
        Ok(self.progress())
    }
}

fn settle_rebuild(meta: &mut Manifest) {
    if let RecoveryState::Rebuilding {
        required_applied_lsn,
        required_committed_lsn,
    } = meta.recovery
        && meta.applied_lsn >= required_applied_lsn
        && meta.committed_lsn >= required_committed_lsn
    {
        meta.recovery = RecoveryState::Healthy;
    }
}

fn encode_record(record: &Record) -> io::Result<Vec<u8>> {
    let payload = serde_json::to_vec(record).map_err(io::Error::other)?;
    let len = u32::try_from(payload.len()).map_err(io::Error::other)?;
    let mut bytes = Vec::with_capacity(payload.len() + 8);
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(&payload);
    bytes.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    Ok(bytes)
}
