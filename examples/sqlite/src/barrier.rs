use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;
use kuberic_core::handles::StateReplicatorHandle;
use kuberic_core::types::{CancellationToken, Lsn};
use sqlite_commit_barrier::{BarrierError, CommitBarrier, Transaction};
use tokio::sync::mpsc;
use tracing::warn;

use crate::frames::{WalFrameSet, frames_from_wal_bytes};

pub const VFS_NAME: &str = "kuberic-quorum";

struct Request {
    payload: Vec<u8>,
    reply: std::sync::mpsc::Sender<Result<Lsn, String>>,
}

/// Blocks a SQLite commit until the transaction reaches durable quorum.
///
/// SQLite calls [`CommitBarrier::publish`] from the thread driving the commit,
/// so the request is handed to a Tokio task and the caller waits on a plain
/// channel rather than depending on executor progress from inside SQLite.
#[derive(Default)]
pub struct ReplicationBarrier {
    sender: Mutex<Option<mpsc::UnboundedSender<Request>>>,
    last_lsn: AtomicI64,
    abandoned: AtomicBool,
}

impl ReplicationBarrier {
    /// Starts replicating commits. Writes fail until this is called.
    pub fn install(&self, replicator: StateReplicatorHandle, token: CancellationToken) {
        let (sender, mut receiver) = mpsc::unbounded_channel::<Request>();
        tokio::spawn(async move {
            while let Some(request) = receiver.recv().await {
                let outcome = replicator
                    .replicate(Bytes::from(request.payload), token.clone())
                    .await
                    .map_err(|error| error.to_string());
                let _ = request.reply.send(outcome);
            }
        });
        *self.sender.lock().expect("barrier sender") = Some(sender);
    }

    /// Stops accepting commits. Any later commit fails rather than proceeding
    /// unreplicated.
    pub fn uninstall(&self) {
        *self.sender.lock().expect("barrier sender") = None;
    }

    /// Replaces quorum replication with `sink`, so the commit path can be
    /// exercised without a cluster.
    #[cfg(any(test, feature = "testing"))]
    pub fn install_sink<F>(&self, sink: F)
    where
        F: Fn(Vec<u8>) -> Result<Lsn, String> + Send + 'static,
    {
        let (sender, mut receiver) = mpsc::unbounded_channel::<Request>();
        tokio::spawn(async move {
            while let Some(request) = receiver.recv().await {
                let outcome = sink(request.payload);
                let _ = request.reply.send(outcome);
            }
        });
        *self.sender.lock().expect("barrier sender") = Some(sender);
    }

    pub fn last_lsn(&self) -> Lsn {
        self.last_lsn.load(Ordering::SeqCst)
    }
}

impl CommitBarrier for ReplicationBarrier {
    fn publish(&self, transaction: &Transaction<'_>) -> Result<(), BarrierError> {
        if self.abandoned.load(Ordering::SeqCst) {
            return Err(BarrierError::new(
                "replica is behind the cluster and must be rebuilt",
            ));
        }
        let frames = frames_from_wal_bytes(
            transaction.wal_offset,
            transaction.frames,
            transaction.page_size,
        );
        if frames.is_empty() {
            return Ok(());
        }

        let frame_set = WalFrameSet {
            checksum: WalFrameSet::compute_checksum(&frames),
            frames,
            db_size_pages: transaction.database_pages,
        };
        let payload = serde_json::to_vec(&frame_set)
            .map_err(|error| BarrierError::new(format!("frame serialization failed: {error}")))?;

        let sender = self
            .sender
            .lock()
            .expect("barrier sender")
            .clone()
            .ok_or_else(|| BarrierError::new("replica is not accepting writes"))?;

        let (reply, wait) = std::sync::mpsc::channel();
        sender
            .send(Request { payload, reply })
            .map_err(|_| BarrierError::new("replication task is gone"))?;

        match wait.recv() {
            Ok(Ok(lsn)) => {
                self.last_lsn.store(lsn, Ordering::SeqCst);
                Ok(())
            }
            Ok(Err(error)) => Err(BarrierError::new(error)),
            Err(_) => Err(BarrierError::new("replication task dropped the commit")),
        }
    }

    fn abandon(&self, error: &str) {
        self.abandoned.store(true, Ordering::SeqCst);
        self.uninstall();
        tracing::error!(error, "replica lost a replicated transaction locally");
    }
}

static BARRIER: OnceLock<Arc<ReplicationBarrier>> = OnceLock::new();

/// Returns the process-wide barrier, registering the VFS on first use.
///
/// SQLite keeps the registration for the life of the process, so it happens
/// once and is shared by every connection opened against [`VFS_NAME`].
pub fn barrier() -> &'static Arc<ReplicationBarrier> {
    BARRIER.get_or_init(|| {
        let barrier = Arc::new(ReplicationBarrier::default());
        if !sqlite_commit_barrier::is_registered(VFS_NAME)
            && let Err(error) = sqlite_commit_barrier::register(VFS_NAME, barrier.clone())
        {
            warn!(%error, "commit barrier vfs registration failed");
        }
        barrier
    })
}
