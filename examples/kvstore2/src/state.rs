use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use kuberic_protocol::types::Epoch;
use kuberic_runtime::application::{OperationDataStream, StateProvider};
use kuberic_runtime::{Result, RuntimeError};

use crate::persistence::{KvPersistence, snapshot_stream};

pub struct KvStateProvider {
    persistence: Arc<KvPersistence>,
}

impl KvStateProvider {
    pub fn new(persistence: Arc<KvPersistence>) -> Self {
        Self { persistence }
    }

    pub fn persistence(&self) -> &Arc<KvPersistence> {
        &self.persistence
    }
}

#[async_trait]
impl StateProvider for KvStateProvider {
    async fn update_epoch(&self, epoch: Epoch, _previous_epoch_last_lsn: i64) -> Result<()> {
        self.persistence.update_epoch(epoch)
    }

    async fn last_committed_lsn(&self) -> Result<i64> {
        use kuberic_runtime::engine::DurableState;
        Ok(self.persistence.durable_progress().await?.committed_lsn)
    }

    async fn get_copy_context(&self) -> Result<OperationDataStream> {
        Ok(Box::pin(stream::empty()))
    }

    async fn get_copy_state(
        &self,
        up_to_lsn: i64,
        mut copy_context: OperationDataStream,
    ) -> Result<OperationDataStream> {
        use futures::StreamExt;
        if copy_context.next().await.is_some() {
            return Err(RuntimeError::Application(
                "kvstore2 does not use a copy context".into(),
            ));
        }
        snapshot_stream(self.persistence.snapshot_at(up_to_lsn)?)
    }

    async fn on_data_loss(&self) -> Result<bool> {
        Ok(false)
    }
}
