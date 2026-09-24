use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kuberic_protocol::types::ReplicaRole;
use kuberic_runtime::application::{OpenContext, RoleChange, StatefulServiceReplica};
use kuberic_runtime::replicator::stream::{OperationMetadata, OperationStream};
use kuberic_runtime::replicator::{
    DefaultReplicatorFactory, Replicator, ReplicatorSettings, StateReplicator,
    StatefulServicePartition,
};
use kuberic_runtime::{Result, RuntimeError};
use tokio::sync::RwLock;

use crate::persistence::KvPersistence;
use crate::state::KvStateProvider;

pub struct KvService {
    provider: Arc<KvStateProvider>,
    replication_address: String,
    state_replicator: RwLock<Option<Arc<dyn StateReplicator>>>,
    partition: RwLock<Option<StatefulServicePartition>>,
    role: Mutex<ReplicaRole>,
}

impl KvService {
    pub fn new(persistence: Arc<KvPersistence>, replication_address: String) -> Self {
        Self {
            provider: Arc::new(KvStateProvider::new(persistence)),
            replication_address,
            state_replicator: RwLock::new(None),
            partition: RwLock::new(None),
            role: Mutex::new(ReplicaRole::None),
        }
    }

    pub fn persistence(&self) -> &Arc<KvPersistence> {
        self.provider.persistence()
    }

    pub async fn replicate_put(&self, key: String, value: String) -> Result<i64> {
        let data = KvPersistence::encode_put(key, value)?;
        self.state_replicator
            .read()
            .await
            .as_ref()
            .ok_or(RuntimeError::NotOpen)?
            .replicate(data)
            .await
    }

    pub async fn get(&self, key: &str) -> Result<Option<String>> {
        let partition = self
            .partition
            .read()
            .await
            .clone()
            .ok_or(RuntimeError::NotOpen)?;
        let status = partition.get_read_status().await?;
        require_read_access(status)?;
        Ok(self.persistence().get(key))
    }
}

fn require_read_access(status: kuberic_protocol::types::AccessStatus) -> Result<()> {
    if status == kuberic_protocol::types::AccessStatus::Granted {
        Ok(())
    } else {
        Err(RuntimeError::ReadClosed(status))
    }
}

#[async_trait]
impl StatefulServiceReplica for KvService {
    async fn open(self: Arc<Self>, context: OpenContext) -> Result<Arc<dyn Replicator>> {
        *self.partition.write().await = Some(context.partition.clone());
        let interfaces = context
            .partition
            .with_factory(Arc::new(DefaultReplicatorFactory::new(
                self.provider.persistence().clone(),
            )))
            .create_replicator(
                self.provider.clone(),
                Some(ReplicatorSettings {
                    replication_address: self.replication_address.clone(),
                }),
            )
            .await?;
        let state_replicator = interfaces.state_replicator();
        let replication = state_replicator.get_replication_stream().await?;
        let copy = state_replicator.get_copy_stream().await?;
        tokio::spawn(consume_stream(
            self.provider.persistence().clone(),
            replication,
        ));
        tokio::spawn(consume_stream(self.provider.persistence().clone(), copy));
        *self.state_replicator.write().await = Some(state_replicator);
        Ok(interfaces.replicator())
    }

    async fn change_role(&self, role: ReplicaRole) -> Result<RoleChange> {
        *self.role.lock().unwrap() = role;
        Ok(RoleChange {
            service_address: (role == ReplicaRole::Primary)
                .then(|| "http://0.0.0.0:8080".to_string()),
        })
    }

    async fn close(&self) -> Result<()> {
        self.state_replicator.write().await.take();
        self.partition.write().await.take();
        *self.role.lock().unwrap() = ReplicaRole::None;
        Ok(())
    }

    fn abort(&self) {
        if let Ok(mut partition) = self.partition.try_write() {
            partition.take();
        }
        if let Ok(mut role) = self.role.lock() {
            *role = ReplicaRole::None;
        }
    }
}

async fn consume_stream(persistence: Arc<KvPersistence>, mut stream: OperationStream) {
    use kuberic_runtime::engine::DurableState;
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

            OperationMetadata::Copy { build_id, sequence } => {
                match persistence
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
                }
            }
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
