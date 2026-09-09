use tonic::Status;
use tonic::transport::Channel;

use crate::proto::{self, kv_store_client::KvStoreClient};

pub struct KvTransaction {
    client: KvStoreClient<Channel>,
    transaction_id: String,
}

impl KvTransaction {
    pub async fn begin(mut client: KvStoreClient<Channel>) -> Result<Self, Status> {
        let transaction_id = client
            .begin_transaction(proto::BeginTransactionRequest::default())
            .await?
            .into_inner()
            .transaction_id;
        Ok(Self::from_id(client, transaction_id))
    }

    pub fn from_id(client: KvStoreClient<Channel>, transaction_id: String) -> Self {
        Self {
            client,
            transaction_id,
        }
    }

    pub fn id(&self) -> &str {
        &self.transaction_id
    }

    pub async fn get(&mut self, key: impl Into<String>) -> Result<Option<String>, Status> {
        let response = self
            .client
            .transaction_get(proto::TransactionGetRequest {
                transaction_id: self.transaction_id.clone(),
                key: key.into(),
            })
            .await?
            .into_inner();
        Ok(response.found.then_some(response.value))
    }

    pub async fn put(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), Status> {
        self.client
            .transaction_put(proto::TransactionPutRequest {
                transaction_id: self.transaction_id.clone(),
                key: key.into(),
                value: value.into(),
            })
            .await?;
        Ok(())
    }

    pub async fn delete(&mut self, key: impl Into<String>) -> Result<(), Status> {
        self.client
            .transaction_delete(proto::TransactionDeleteRequest {
                transaction_id: self.transaction_id.clone(),
                key: key.into(),
            })
            .await?;
        Ok(())
    }

    pub async fn commit(&mut self) -> Result<i64, Status> {
        Ok(self
            .client
            .commit_transaction(proto::TransactionRequest {
                transaction_id: self.transaction_id.clone(),
            })
            .await?
            .into_inner()
            .lsn)
    }

    pub async fn abort(&mut self) -> Result<(), Status> {
        self.client
            .abort_transaction(proto::TransactionRequest {
                transaction_id: self.transaction_id.clone(),
            })
            .await?;
        Ok(())
    }
}
