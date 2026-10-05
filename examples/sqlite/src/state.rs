//! Application-owned durable history, independent of the live SQL connection.
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use kuberic_runtime::application::{
    CopyChunk, DurableApplicationAck, DurableApplicationProgress, Operation,
};
use kuberic_runtime::engine::{DurableState, RetainedOperationStream};
use kuberic_runtime::protocol::types::OperationId;

pub use crate::framelog::durable::RecoveryState;
use crate::framelog::durable::{DurableFrameLog, replace, sync_directory};

pub struct SqlitePersistence {
    root: PathBuf,
    log: OnceLock<Mutex<DurableFrameLog>>,
    opening: Mutex<()>,
    #[cfg(any(test, feature = "testing"))]
    fault: std::sync::atomic::AtomicU8,
    #[cfg(any(test, feature = "testing"))]
    pub after_apply: crate::testing::PauseGate,
}

/// Application acceptance fault cuts; absent from production builds.
#[cfg(any(test, feature = "testing"))]
#[derive(Clone, Copy)]
pub enum PersistenceFault {
    BeforeApply = 1,
    AfterApply = 2,
    AfterCommit = 3,
    AfterCopyInstall = 4,
}

#[cfg(any(test, feature = "testing"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistenceInspection {
    pub progress: DurableApplicationProgress,
    pub recovery: RecoveryState,
    pub base_lsn: i64,
    pub operations: Vec<Operation>,
    pub image: Vec<u8>,
}

impl SqlitePersistence {
    /// Prove freshness before opening storage or asking ReplicaHost to initialize authority.
    pub fn is_fresh_empty(root: &Path) -> io::Result<bool> {
        match std::fs::read_dir(root) {
            Ok(mut entries) => Ok(entries.next().transpose()?.is_none()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(true),
            Err(error) => Err(error),
        }
    }

    /// Open fresh-v2 storage; classic data and missing established metadata are never imported.
    pub fn open(root: PathBuf) -> io::Result<Self> {
        let persistence = Self::deferred(root)?;
        persistence.initialize()?;
        Ok(persistence)
    }

    pub(crate) fn deferred(root: PathBuf) -> io::Result<Self> {
        Ok(Self {
            root: std::path::absolute(root)?,
            log: OnceLock::new(),
            opening: Mutex::new(()),
            #[cfg(any(test, feature = "testing"))]
            fault: std::sync::atomic::AtomicU8::new(0),
            #[cfg(any(test, feature = "testing"))]
            after_apply: crate::testing::PauseGate::default(),
        })
    }

    pub(crate) fn initialize(&self) -> io::Result<()> {
        let _opening = self
            .opening
            .lock()
            .map_err(|_| io::Error::other("SQLite initialization mutex poisoned"))?;
        if self.log.get().is_none() {
            let log = DurableFrameLog::open(self.root.clone())?;
            self.log
                .set(Mutex::new(log))
                .map_err(|_| io::Error::other("SQLite persistence already initialized"))?;
        }
        Ok(())
    }

    fn lock(&self) -> io::Result<std::sync::MutexGuard<'_, DurableFrameLog>> {
        self.log
            .get()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotConnected,
                    "SQLite persistence is not open",
                )
            })?
            .lock()
            .map_err(|_| io::Error::other("SQLite persistence mutex poisoned"))
    }

    pub fn progress(&self) -> io::Result<DurableApplicationProgress> {
        Ok(self.lock()?.progress())
    }

    pub fn recovery_state(&self) -> io::Result<RecoveryState> {
        Ok(self.lock()?.recovery())
    }

    pub fn require_reconciliation(&self, reason: String) -> io::Result<()> {
        self.lock()?.reconcile(reason)
    }

    pub fn snapshot(&self, up_to_lsn: i64) -> io::Result<Vec<u8>> {
        self.lock()?.snapshot(up_to_lsn)
    }

    /// Offline test oracle for retained applied data; never changes commitment
    /// or exposes that suffix through the production SQL connection.
    #[cfg(any(test, feature = "testing"))]
    pub fn applied_image_for_test(&self) -> io::Result<Vec<u8>> {
        let log = self.lock()?;
        log.image_at(log.progress().applied_lsn)
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn image_at_for_test(&self, lsn: i64) -> io::Result<Vec<u8>> {
        self.lock()?.image_at(lsn)
    }

    /// One coherent read of the application evidence used by fencing oracles.
    #[cfg(any(test, feature = "testing"))]
    pub fn inspect_for_test(&self) -> io::Result<PersistenceInspection> {
        let log = self.lock()?;
        let progress = log.progress();
        let base_lsn = log.base_lsn_for_test();
        Ok(PersistenceInspection {
            progress,
            recovery: log.recovery(),
            base_lsn,
            operations: log.retained(base_lsn + 1, progress.applied_lsn)?,
            image: log.image_at(progress.applied_lsn)?,
        })
    }

    /// Requires that the caller has closed every connection to the materialized database.
    pub fn materialize_committed(&self) -> io::Result<PathBuf> {
        let log = self.lock()?;
        self.materialize(&log)
    }

    fn materialize(&self, log: &DurableFrameLog) -> io::Result<PathBuf> {
        let image = log.image_at(log.progress().committed_lsn)?;
        for companion in ["db.sqlite-wal", "db.sqlite-shm"] {
            match std::fs::remove_file(self.root.join(companion)) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        sync_directory(&self.root)?;
        let path = self.root.join("db.sqlite");
        replace(&path, &image)?;
        Ok(path)
    }

    /// Recovery may clear reconciliation only after v2 has resolved its exact journal.
    pub fn complete_reconciliation(&self) -> io::Result<()> {
        let mut log = self.lock()?;
        self.materialize(&log)?;
        log.clear_reconciliation()
    }

    /// Called under SQL serialization after SQLite returned a successful commit.
    pub(crate) fn confirm_publication(&self, lsn: i64) -> io::Result<()> {
        let mut log = self.lock()?;
        if log.progress().committed_lsn != lsn {
            return Err(io::Error::other(
                "publication does not match durable commitment",
            ));
        }
        log.clear_reconciliation()
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn fail_once(&self, fault: PersistenceFault) {
        self.fault
            .store(fault as u8, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(any(test, feature = "testing"))]
    fn fault_at(&self, fault: PersistenceFault) -> kuberic_runtime::Result<()> {
        use std::sync::atomic::Ordering;
        if self
            .fault
            .compare_exchange(fault as u8, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return Err(kuberic_runtime::RuntimeError::Application(
                "injected persistence cut".into(),
            ));
        }
        Ok(())
    }
}

pub(crate) fn persistence_error(error: io::Error) -> kuberic_runtime::RuntimeError {
    kuberic_runtime::RuntimeError::Application(error.to_string())
}

#[async_trait::async_trait]
impl DurableState for SqlitePersistence {
    async fn get_replication_operations(
        &self,
        from_lsn: i64,
        to_lsn: i64,
    ) -> kuberic_runtime::Result<RetainedOperationStream> {
        let operations = self
            .lock()
            .and_then(|log| log.retained(from_lsn, to_lsn))
            .map_err(persistence_error)?;
        Ok(Box::pin(futures::stream::iter(
            operations.into_iter().map(Ok),
        )))
    }

    async fn apply_copy_chunk(
        &self,
        build_id: &OperationId,
        sequence: u64,
        chunk: CopyChunk,
    ) -> kuberic_runtime::Result<()> {
        self.lock()
            .and_then(|mut log| log.stage(build_id.as_str(), sequence, chunk.data.to_vec()))
            .map_err(persistence_error)
    }

    async fn verify_copy_chunk(
        &self,
        build_id: &OperationId,
        sequence: u64,
        chunk: &CopyChunk,
    ) -> kuberic_runtime::Result<bool> {
        self.lock()
            .map(|log| log.verify_chunk(build_id.as_str(), sequence, &chunk.data))
            .map_err(persistence_error)
    }

    async fn finish_copy(
        &self,
        build_id: &OperationId,
        up_to_lsn: i64,
        committed_lsn: i64,
    ) -> kuberic_runtime::Result<DurableApplicationProgress> {
        let progress = self
            .lock()
            .and_then(|mut log| log.finish(build_id.as_str(), up_to_lsn, committed_lsn))
            .map_err(persistence_error)?;
        #[cfg(any(test, feature = "testing"))]
        self.fault_at(PersistenceFault::AfterCopyInstall)?;
        Ok(progress)
    }

    async fn apply(&self, operation: Operation) -> kuberic_runtime::Result<DurableApplicationAck> {
        #[cfg(any(test, feature = "testing"))]
        self.fault_at(PersistenceFault::BeforeApply)?;
        let progress = self
            .lock()
            .and_then(|mut log| log.apply(operation))
            .map_err(persistence_error)?;
        #[cfg(any(test, feature = "testing"))]
        self.after_apply.pause().await;
        #[cfg(any(test, feature = "testing"))]
        self.fault_at(PersistenceFault::AfterApply)?;
        Ok(progress)
    }

    async fn durable_progress(&self) -> kuberic_runtime::Result<DurableApplicationProgress> {
        self.progress().map_err(persistence_error)
    }

    async fn verify_applied(&self, operation: &Operation) -> kuberic_runtime::Result<bool> {
        self.lock()
            .and_then(|log| log.verify(operation))
            .map_err(persistence_error)
    }

    async fn commit(
        &self,
        committed_lsn: i64,
    ) -> kuberic_runtime::Result<DurableApplicationProgress> {
        let progress = self
            .lock()
            .and_then(|mut log| log.commit(committed_lsn))
            .map_err(persistence_error)?;
        #[cfg(any(test, feature = "testing"))]
        self.fault_at(PersistenceFault::AfterCommit)?;
        Ok(progress)
    }
}
