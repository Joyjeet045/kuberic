//! Public v2 lifecycle/provider adapter. Authority stays in ReplicaHost.
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream};
use kuberic_protocol::types::{AccessStatus, Epoch, FaultType, ReplicaRole};
use kuberic_runtime::application::{OpenContext, OperationDataStream, RoleChange};
use kuberic_runtime::engine::DurableState;
use kuberic_runtime::replicator::stream::{OperationMetadata, OperationStream};
use kuberic_runtime::replicator::{
    DefaultReplicatorFactory, Replicator, ReplicatorSettings, StateReplicator,
    StatefulServicePartition,
};
use kuberic_runtime::{Result, RuntimeError, StateProvider, StatefulServiceReplica};

use crate::barrier::ReplicationBarrier;
use crate::connection::SqliteConnection;
use crate::state::{RecoveryState, SqlitePersistence, persistence_error};

pub struct SqliteProvider {
    persistence: Arc<SqlitePersistence>,
}

#[async_trait]
impl StateProvider for SqliteProvider {
    async fn update_epoch(&self, _epoch: Epoch, previous_epoch_last_lsn: i64) -> Result<()> {
        // Agent authority fences epochs; immutable application history must not
        // discard an unresolved reservation or an acknowledged prefix here.
        if previous_epoch_last_lsn < 0 {
            return Err(RuntimeError::Application(
                "negative previous epoch boundary".into(),
            ));
        }
        Ok(())
    }
    async fn last_committed_lsn(&self) -> Result<i64> {
        Ok(self.persistence.durable_progress().await?.committed_lsn)
    }
    async fn get_copy_context(&self) -> Result<OperationDataStream> {
        Ok(Box::pin(stream::empty()))
    }
    async fn get_copy_state(
        &self,
        up_to_lsn: i64,
        mut context: OperationDataStream,
    ) -> Result<OperationDataStream> {
        if context.next().await.is_some() {
            return Err(RuntimeError::Application(
                "SQLite does not use copy context".into(),
            ));
        }
        let data = self
            .persistence
            .snapshot(up_to_lsn)
            .map_err(persistence_error)?;
        Ok(Box::pin(stream::iter([Ok(Bytes::from(data))])))
    }
    async fn on_data_loss(&self) -> Result<bool> {
        Ok(false)
    }
}

pub struct SqliteService {
    pub(crate) sql: Arc<Mutex<SqliteConnection>>,
    pub(crate) request_gate: Arc<tokio::sync::Mutex<()>>,
    pub(crate) active: Arc<AtomicBool>,
    pub(crate) vfs_name: String,
    persistence: Arc<SqlitePersistence>,
    barrier: Arc<ReplicationBarrier>,
    provider: Arc<SqliteProvider>,
    partition: Mutex<Option<StatefulServicePartition>>,
    replicator: Mutex<Option<Arc<dyn StateReplicator>>>,
    streams: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    replication_address: String,
}

impl SqliteService {
    pub fn new(
        persistence: Arc<SqlitePersistence>,
        replication_address: String,
    ) -> std::io::Result<Self> {
        let committed_lsn = persistence.progress()?.committed_lsn;
        Self::with_persistence(persistence, replication_address, committed_lsn)
    }

    /// Construct without touching application files. ReplicaHost must authorize
    /// agent storage before invoking Open, which opens application persistence.
    pub fn deferred(root: PathBuf, replication_address: String) -> std::io::Result<Self> {
        Self::with_persistence(
            Arc::new(SqlitePersistence::deferred(root)?),
            replication_address,
            0,
        )
    }

    fn with_persistence(
        persistence: Arc<SqlitePersistence>,
        replication_address: String,
        committed_lsn: i64,
    ) -> std::io::Result<Self> {
        let (barrier, vfs_name) =
            ReplicationBarrier::register_with_lsn(persistence.clone(), committed_lsn)?;
        Ok(Self {
            sql: Arc::new(Mutex::new(SqliteConnection::default())),
            request_gate: Arc::new(tokio::sync::Mutex::new(())),
            active: Arc::new(AtomicBool::new(false)),
            vfs_name,
            provider: Arc::new(SqliteProvider {
                persistence: persistence.clone(),
            }),
            persistence,
            barrier,
            partition: Mutex::new(None),
            replicator: Mutex::new(None),
            streams: Mutex::new(Vec::new()),
            replication_address,
        })
    }
    pub fn persistence(&self) -> &Arc<SqlitePersistence> {
        &self.persistence
    }
    pub fn barrier(&self) -> &Arc<ReplicationBarrier> {
        &self.barrier
    }
    pub fn provider(&self) -> &Arc<SqliteProvider> {
        &self.provider
    }
    pub fn vfs_name(&self) -> &str {
        &self.vfs_name
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn primary_application_active_for_test(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }
    pub fn partition(&self) -> Result<StatefulServicePartition> {
        self.partition
            .lock()
            .expect("partition")
            .clone()
            .ok_or(RuntimeError::NotOpen)
    }

    pub(crate) async fn access(&self, write: bool) -> Result<(StatefulServicePartition, bool)> {
        if !self.active.load(Ordering::SeqCst) {
            return Err(RuntimeError::NotPrimary);
        }
        let partition = self.partition()?;
        let status = if write {
            partition.get_write_status().await?
        } else {
            partition.get_read_status().await?
        };
        if status != AccessStatus::Granted {
            return Err(if write {
                RuntimeError::WriteClosed(status)
            } else {
                RuntimeError::ReadClosed(status)
            });
        }
        // A write grant follows the runtime's exact local-journal recovery. Reads
        // alone must not clear a reservation-only reconciliation marker.
        let recovered = partition.get_write_status().await? == AccessStatus::Granted;
        Ok((partition, recovered))
    }

    /// Grant-time journal recovery can advance commitment after the Primary
    /// callback. Under the request/connection locks, refresh that materialization
    /// before the first SQL request can observe it or issue a different write.
    pub(crate) fn prepare_sql(
        &self,
        sql: &mut SqliteConnection,
        recovered: bool,
    ) -> std::io::Result<()> {
        if !self.active.load(Ordering::SeqCst) || self.barrier.is_fenced() {
            return Err(std::io::Error::other(
                "SQL is fenced for this service instance",
            ));
        }
        let recovery = self.persistence.recovery_state()?;
        if matches!(
            recovery,
            RecoveryState::RebuildRequired(_) | RecoveryState::Rebuilding { .. }
        ) {
            return Err(std::io::Error::other(
                "acknowledged history requires rebuild",
            ));
        }
        let committed = self.persistence.progress()?.committed_lsn;
        if !sql.is_open() || sql.visible_lsn != committed || recovery != RecoveryState::Healthy {
            sql.close();
            if recovery != RecoveryState::Healthy {
                if !recovered {
                    return Err(std::io::Error::other(
                        "agent journal reconciliation is incomplete",
                    ));
                }
                self.persistence.complete_reconciliation()?;
            }
            let path = self.persistence.materialize_committed()?;
            self.barrier.reset_receipt(committed);
            sql.open(&path, &self.vfs_name, committed)
                .map_err(std::io::Error::other)?;
        }
        Ok(())
    }
}

#[async_trait]
impl StatefulServiceReplica for SqliteService {
    async fn open(self: Arc<Self>, context: OpenContext) -> Result<Arc<dyn Replicator>> {
        self.persistence.initialize().map_err(persistence_error)?;
        self.barrier.reset_receipt(
            self.persistence
                .progress()
                .map_err(persistence_error)?
                .committed_lsn,
        );
        *self.partition.lock().expect("partition") = Some(context.partition.clone());
        let interfaces = context
            .partition
            .with_factory(Arc::new(DefaultReplicatorFactory::new(
                self.persistence.clone(),
            )))
            .create_replicator(
                self.provider.clone(),
                Some(ReplicatorSettings {
                    replication_address: self.replication_address.clone(),
                }),
            )
            .await?;
        let replicator = interfaces.state_replicator();
        for stream in [
            replicator.get_copy_stream().await?,
            replicator.get_replication_stream().await?,
        ] {
            let task = tokio::spawn(consume_stream(self.persistence.clone(), stream));
            self.streams.lock().expect("streams").push(task);
        }
        *self.replicator.lock().expect("replicator") = Some(replicator);
        if matches!(
            self.persistence
                .recovery_state()
                .map_err(persistence_error)?,
            RecoveryState::RebuildRequired(_) | RecoveryState::Rebuilding { .. }
        ) {
            self.partition()?.report_fault(FaultType::Permanent).await?;
        }
        Ok(interfaces.replicator())
    }

    async fn change_role(&self, role: ReplicaRole) -> Result<RoleChange> {
        self.active.store(false, Ordering::SeqCst);
        self.barrier.uninstall();
        let state = self.sql.clone();
        tokio::task::spawn_blocking(move || state.lock().expect("SQL connection").close())
            .await
            .map_err(|e| RuntimeError::Application(e.to_string()))?;
        if role == ReplicaRole::Primary {
            if self.barrier.is_fenced() {
                return Err(RuntimeError::Application(
                    "reopen the fenced service before promotion".into(),
                ));
            }
            let replicator = self
                .replicator
                .lock()
                .expect("replicator")
                .clone()
                .ok_or(RuntimeError::NotOpen)?;
            self.barrier.install(replicator, self.partition()?);
            let state = self.sql.clone();
            let persistence = self.persistence.clone();
            let vfs = self.vfs_name.clone();
            let barrier = self.barrier.clone();
            let opened = tokio::task::spawn_blocking(move || {
                let mut state = state.lock().expect("SQL connection");
                let path = persistence.materialize_committed()?;
                let committed = persistence.progress()?.committed_lsn;
                barrier.reset_receipt(committed);
                state
                    .open(&path, &vfs, committed)
                    .map_err(std::io::Error::other)
            })
            .await
            .map_err(|e| RuntimeError::Application(e.to_string()))?;
            if let Err(error) = opened {
                self.barrier.uninstall();
                return Err(persistence_error(error));
            }
            self.active.store(true, Ordering::SeqCst);
        }
        Ok(RoleChange {
            service_address: None,
        })
    }

    async fn close(&self) -> Result<()> {
        self.abort();
        let state = self.sql.clone();
        tokio::task::spawn_blocking(move || state.lock().expect("SQL connection").close())
            .await
            .map_err(|e| RuntimeError::Application(e.to_string()))?;
        Ok(())
    }

    fn abort(&self) {
        self.active.store(false, Ordering::SeqCst);
        self.barrier.uninstall();
        self.partition.lock().expect("partition").take();
        self.replicator.lock().expect("replicator").take();
        for task in self.streams.lock().expect("streams").drain(..) {
            task.abort();
        }
        if let Ok(mut state) = self.sql.try_lock() {
            state.close();
        }
    }
}

async fn consume_stream(persistence: Arc<SqlitePersistence>, mut stream: OperationStream) {
    while let Ok(Some(operation)) = stream.get_operation().await {
        let result = match &operation.metadata {
            OperationMetadata::Replication { lsn, committed_lsn } => {
                persistence
                    .apply(kuberic_runtime::application::Operation {
                        lsn: *lsn,
                        committed_lsn: *committed_lsn,
                        data: operation.data.clone(),
                    })
                    .await
            }
            OperationMetadata::Copy { build_id, sequence } => match persistence
                .apply_copy_chunk(
                    build_id,
                    *sequence,
                    kuberic_runtime::application::CopyChunk {
                        data: operation.data.clone(),
                    },
                )
                .await
            {
                Ok(()) => persistence.durable_progress().await,
                Err(error) => Err(error),
            },
            OperationMetadata::CopyComplete {
                build_id,
                up_to_lsn,
                committed_lsn,
            } => {
                persistence
                    .finish_copy(build_id, *up_to_lsn, *committed_lsn)
                    .await
            }
        };
        match result {
            Ok(progress) => {
                let _ = operation.acknowledge(progress);
            }
            Err(error) => {
                let _ = operation.reject(error);
            }
        }
    }
}
