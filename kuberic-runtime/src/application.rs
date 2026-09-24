use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use kuberic_protocol::types::{Epoch, OperationId, ReplicaIdentity, ReplicaRole};

use crate::Result;
use crate::replicator::{Replicator, StatefulServicePartition};
pub use kuberic_runtime_internal::effects::OpenMode;

pub type Lsn = i64;
pub type OperationData = Bytes;
pub type OperationDataStream = Pin<Box<dyn Stream<Item = Result<OperationData>> + Send>>;

#[derive(Clone)]
pub struct OpenContext {
    pub identity: ReplicaIdentity,
    pub mode: OpenMode,
    pub partition: StatefulServicePartition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleChange {
    pub service_address: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Operation {
    pub lsn: Lsn,
    pub committed_lsn: Lsn,
    pub data: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyChunk {
    pub data: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DurableApplicationProgress {
    pub applied_lsn: Lsn,
    pub committed_lsn: Lsn,
}

pub type DurableApplicationAck = DurableApplicationProgress;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientWrite {
    pub operation_id: OperationId,
    pub data: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteReceipt {
    pub lsn: Lsn,
    pub committed_lsn: Lsn,
}

#[async_trait]
pub trait StatefulServiceReplica: Send + Sync {
    async fn open(self: Arc<Self>, context: OpenContext) -> Result<Arc<dyn Replicator>>;

    async fn change_role(&self, role: ReplicaRole) -> Result<RoleChange>;

    async fn close(&self) -> Result<()>;

    fn abort(&self);
}

#[async_trait]
pub trait StateProvider: Send + Sync {
    async fn update_epoch(&self, epoch: Epoch, previous_epoch_last_lsn: Lsn) -> Result<()>;

    async fn last_committed_lsn(&self) -> Result<Lsn>;

    async fn get_copy_context(&self) -> Result<OperationDataStream>;

    /// Returns copy bytes frozen at `up_to_lsn`. Repeating an authorized build
    /// at the same boundary must reproduce identical ordered bytes.
    async fn get_copy_state(
        &self,
        up_to_lsn: Lsn,
        copy_context: OperationDataStream,
    ) -> Result<OperationDataStream>;

    async fn on_data_loss(&self) -> Result<bool>;
}
