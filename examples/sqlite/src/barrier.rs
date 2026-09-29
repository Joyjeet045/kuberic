//! Per-instance bridge from synchronous WAL publication to the public v2 replicator.
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
};

use bytes::Bytes;
use kuberic_protocol::types::{AccessStatus, FaultType};
use kuberic_runtime::replicator::{StateReplicator, StatefulServicePartition};
use sqlite_commit_barrier::{BarrierError, CommitBarrier, Transaction};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::frames::{WalFrameSet, frames_from_wal_bytes};
use crate::state::SqlitePersistence;

#[derive(Debug, Clone)]
pub enum CommitFailure {
    Definitive(String),
    Unknown(String),
}

struct Request {
    payload: Bytes,
    reply: std::sync::mpsc::Sender<Result<i64, CommitFailure>>,
}

struct Worker {
    sender: mpsc::UnboundedSender<Request>,
    cancel: CancellationToken,
}

pub struct ReplicationBarrier {
    persistence: Arc<SqlitePersistence>,
    worker: Mutex<Option<Worker>>,
    fault_context: Mutex<Option<(tokio::runtime::Handle, StatefulServicePartition)>>,
    last_lsn: AtomicI64,
    fenced: AtomicBool,
    failure: Mutex<Option<CommitFailure>>,
    #[cfg(any(test, feature = "testing"))]
    fail_after_quorum: AtomicBool,
    #[cfg(any(test, feature = "testing"))]
    pub before_dispatch: crate::testing::PauseGate,
    #[cfg(any(test, feature = "testing"))]
    pub after_quorum: crate::testing::PauseGate,
}

static NEXT_VFS: AtomicU64 = AtomicU64::new(1);

impl ReplicationBarrier {
    pub fn register(persistence: Arc<SqlitePersistence>) -> std::io::Result<(Arc<Self>, String)> {
        let committed_lsn = persistence.progress()?.committed_lsn;
        Self::register_with_lsn(persistence, committed_lsn)
    }

    pub(crate) fn register_with_lsn(
        persistence: Arc<SqlitePersistence>,
        committed_lsn: i64,
    ) -> std::io::Result<(Arc<Self>, String)> {
        let id = NEXT_VFS
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |id| id.checked_add(1))
            .map_err(|_| std::io::Error::other("VFS instance counter exhausted"))?;
        let name = format!("kuberic-sqlite-{}-{id}", std::process::id());
        let barrier = Arc::new(Self {
            last_lsn: AtomicI64::new(committed_lsn),
            persistence,
            worker: Mutex::new(None),
            fault_context: Mutex::new(None),
            fenced: AtomicBool::new(false),
            failure: Mutex::new(None),
            #[cfg(any(test, feature = "testing"))]
            fail_after_quorum: AtomicBool::new(false),
            #[cfg(any(test, feature = "testing"))]
            before_dispatch: crate::testing::PauseGate::default(),
            #[cfg(any(test, feature = "testing"))]
            after_quorum: crate::testing::PauseGate::default(),
        });
        sqlite_commit_barrier::register(&name, barrier.clone()).map_err(std::io::Error::other)?;
        Ok((barrier, name))
    }

    pub fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::SeqCst)
    }
    pub fn last_lsn(&self) -> i64 {
        self.last_lsn.load(Ordering::SeqCst)
    }
    pub(crate) fn reset_receipt(&self, lsn: i64) {
        self.last_lsn.store(lsn, Ordering::SeqCst);
        self.failure.lock().expect("barrier failure").take();
    }
    pub fn failure(&self) -> Option<CommitFailure> {
        self.failure.lock().expect("barrier failure").clone()
    }

    pub fn install(
        self: &Arc<Self>,
        replicator: Arc<dyn StateReplicator>,
        partition: StatefulServicePartition,
    ) {
        self.uninstall();
        let (sender, mut receiver) = mpsc::unbounded_channel::<Request>();
        let cancel = CancellationToken::new();
        *self.fault_context.lock().expect("fault context") =
            Some((tokio::runtime::Handle::current(), partition.clone()));
        *self.worker.lock().expect("barrier worker") = Some(Worker {
            sender,
            cancel: cancel.clone(),
        });
        let barrier = self.clone();
        tokio::spawn(async move {
            loop {
                let request = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    request = receiver.recv() => match request { Some(request) => request, None => break },
                };
                #[cfg(any(test, feature = "testing"))]
                barrier.before_dispatch.pause().await;
                let outcome = if cancel.is_cancelled()
                    || barrier.is_fenced()
                    || !matches!(
                        partition.get_write_status().await,
                        Ok(AccessStatus::Granted)
                    ) {
                    Err(CommitFailure::Definitive(
                        "replica is not accepting writes".into(),
                    ))
                } else if let Err(error) = barrier.persistence.require_reconciliation(
                    "dispatched SQL transaction awaiting publication".into(),
                ) {
                    Err(CommitFailure::Definitive(format!(
                        "cannot durably prepare publication: {error}"
                    )))
                } else {
                    // From this call onward even application absence cannot prove
                    // rejection: the agent may already hold a durable reservation.
                    let result = tokio::select! {
                        biased;
                        _ = cancel.cancelled() => Err(kuberic_runtime::RuntimeError::OperationCancelled),
                        result = replicator.replicate(request.payload) => result,
                    };
                    match result {
                        Ok(lsn) => {
                            #[cfg(any(test, feature = "testing"))]
                            barrier.after_quorum.pause().await;
                            #[cfg(any(test, feature = "testing"))]
                            if barrier.fail_after_quorum.swap(false, Ordering::SeqCst) {
                                let reason =
                                    "injected quorum-committed/local-unpublished cut".to_owned();
                                barrier.fence_unknown(&reason);
                                let _ = request.reply.send(Err(CommitFailure::Unknown(reason)));
                                continue;
                            }
                            Ok(lsn)
                        }
                        Err(error) => {
                            let reason = error.to_string();
                            barrier.fence_unknown(&reason);
                            Err(CommitFailure::Unknown(reason))
                        }
                    }
                };
                let _ = request.reply.send(outcome);
            }
        });
    }

    pub fn uninstall(&self) {
        if let Some(worker) = self.worker.lock().expect("barrier worker").take() {
            worker.cancel.cancel();
        }
        self.fault_context.lock().expect("fault context").take();
    }

    /// A sticky same-process fence. Only constructing a new service instance can
    /// reopen SQL, after agent recovery and committed rematerialization.
    pub(crate) fn fence_unknown(&self, reason: &str) {
        self.fenced.store(true, Ordering::SeqCst);
        *self.failure.lock().expect("barrier failure") =
            Some(CommitFailure::Unknown(reason.to_owned()));
        if let Err(error) = self.persistence.require_reconciliation(reason.to_owned()) {
            tracing::error!(%error, "unable to persist reconciliation fence");
        }
        if let Some((handle, partition)) = self.fault_context.lock().expect("fault context").clone()
        {
            handle.spawn(async move {
                let _ = partition.report_fault(FaultType::Transient).await;
            });
        }
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn fail_after_quorum_once(&self) {
        self.fail_after_quorum.store(true, Ordering::SeqCst);
    }
}

impl CommitBarrier for ReplicationBarrier {
    fn publish(&self, transaction: &Transaction<'_>) -> Result<(), BarrierError> {
        let outcome = (|| {
            if self.is_fenced() {
                return Err(CommitFailure::Unknown(
                    "replica requires reopen and reconciliation".into(),
                ));
            }
            let frames = frames_from_wal_bytes(
                transaction.wal_offset,
                transaction.frames,
                transaction.page_size,
            );
            let frame_set = WalFrameSet {
                checksum: WalFrameSet::compute_checksum(&frames),
                frames,
                db_size_pages: transaction.database_pages,
            };
            frame_set
                .page_size()
                .map_err(|e| CommitFailure::Definitive(e.to_string()))?;
            let payload = serde_json::to_vec(&frame_set)
                .map_err(|e| CommitFailure::Definitive(e.to_string()))?;
            let sender = self
                .worker
                .lock()
                .expect("barrier worker")
                .as_ref()
                .map(|w| w.sender.clone())
                .ok_or_else(|| {
                    CommitFailure::Definitive("no replication barrier is installed".into())
                })?;
            let (reply, wait) = std::sync::mpsc::channel();
            sender
                .send(Request {
                    payload: payload.into(),
                    reply,
                })
                .map_err(|_| {
                    CommitFailure::Definitive("replication worker is unavailable".into())
                })?;
            wait.recv().unwrap_or_else(|_| {
                self.fence_unknown("replication worker lost the response");
                Err(CommitFailure::Unknown(
                    "replication worker lost the response".into(),
                ))
            })
        })();
        match outcome {
            Ok(lsn) => {
                self.last_lsn.store(lsn, Ordering::SeqCst);
                Ok(())
            }
            Err(failure) => {
                let message = match &failure {
                    CommitFailure::Definitive(s) | CommitFailure::Unknown(s) => s.clone(),
                };
                *self.failure.lock().expect("barrier failure") = Some(failure);
                Err(BarrierError::new(message))
            }
        }
    }

    fn abandon(&self, reason: &str) {
        // The committed history is intact; losing live WAL publication is
        // reconcilable, not proof that acknowledged application history was lost.
        self.fence_unknown(reason);
        self.uninstall();
    }
}
