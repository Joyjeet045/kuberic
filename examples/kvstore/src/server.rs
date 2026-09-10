use std::sync::Arc;

use bytes::Bytes;
use kuberic_core::handles::{PartitionHandle, StateReplicatorHandle};
use kuberic_core::types::{AccessStatus, CancellationToken};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use crate::proto;
use crate::state::{KvMutation, KvOp, SharedState};
use crate::transactions::{Transaction, Transactions};

#[derive(Clone)]
pub struct KvServer {
    pub state: SharedState,
    pub partition: Arc<PartitionHandle>,
    pub replicator: StateReplicatorHandle,
    pub token: CancellationToken,
    transactions: Arc<Mutex<Transactions>>,
    commit_gate: Arc<Mutex<bool>>,
}

enum Write {
    Put(proto::PutRequest),
    Delete(proto::DeleteRequest),
    Commit(String),
    Execute(proto::ExecuteTransactionRequest),
}

impl KvServer {
    pub fn new(
        state: SharedState,
        partition: Arc<PartitionHandle>,
        replicator: StateReplicatorHandle,
        token: CancellationToken,
    ) -> Self {
        Self {
            state,
            partition,
            replicator,
            token,
            transactions: Arc::new(Mutex::new(Transactions::default())),
            commit_gate: Arc::new(Mutex::new(false)),
        }
    }

    fn check_write_access(&self) -> Result<(), Status> {
        if self.token.is_cancelled() || self.partition.write_status() != AccessStatus::Granted {
            return Err(Status::unavailable("primary write access is not granted"));
        }
        Ok(())
    }

    async fn write(&self, write: Write) -> Result<(i64, bool), Status> {
        let server = self.clone();
        tokio::spawn(async move { server.commit(write).await })
            .await
            .map_err(|error| Status::internal(error.to_string()))?
    }

    async fn commit(&self, write: Write) -> Result<(i64, bool), Status> {
        let mut requires_recovery = self.commit_gate.lock().await;
        self.check_write_access()?;
        let mut transactions = self.transactions.lock().await;
        let state = self.state.read().await;
        let generation = state.generation;
        if *requires_recovery {
            return Err(Status::unavailable(
                "an uncertain write requires replica recovery",
            ));
        }
        let mut existed = false;
        let operation = match write {
            Write::Put(request) => KvOp::Put {
                key: request.key,
                value: request.value,
            },
            Write::Delete(request) => {
                existed = state.data.contains_key(&request.key);
                KvOp::Delete { key: request.key }
            }
            Write::Commit(transaction_id) => {
                if let Some(lsn) = state.transaction_lsn(&transaction_id) {
                    return Ok((lsn, false));
                }
                let transaction = transactions.take(&state, &transaction_id)?;
                transaction.validate(&state)?;
                KvOp::Transaction {
                    transaction_id,
                    mutations: transaction.mutations,
                }
            }
            Write::Execute(request) => {
                if request.transaction_id.is_empty() || request.transaction_id.len() > 128 {
                    return Err(Status::invalid_argument(
                        "transaction ID must be 1-128 bytes",
                    ));
                }
                if let Some(lsn) = state.transaction_lsn(&request.transaction_id) {
                    return Ok((lsn, false));
                }
                let mut transaction = Transaction::new(&state, 0)?;
                for mutation in request.mutations {
                    let mutation = match mutation.mutation {
                        Some(proto::transaction_mutation::Mutation::Put(request)) => {
                            KvMutation::Put {
                                key: request.key,
                                value: request.value,
                            }
                        }
                        Some(proto::transaction_mutation::Mutation::Delete(request)) => {
                            KvMutation::Delete { key: request.key }
                        }
                        None => return Err(Status::invalid_argument("mutation is required")),
                    };
                    transaction.stage(&state, mutation)?;
                }
                transaction.validate(&state)?;
                KvOp::Transaction {
                    transaction_id: request.transaction_id,
                    mutations: transaction.mutations,
                }
            }
        };
        drop(state);
        drop(transactions);
        let data =
            serde_json::to_vec(&operation).map_err(|error| Status::internal(error.to_string()))?;
        if matches!(operation, KvOp::Transaction { .. }) && data.len() > 3 * 1024 * 1024 {
            return Err(Status::resource_exhausted(
                "encoded transaction exceeds 3 MiB",
            ));
        }
        let lsn = match self
            .replicator
            .replicate(Bytes::from(data), self.token.clone())
            .await
        {
            Ok(lsn) => lsn,
            Err(error) => {
                *requires_recovery = true;
                self.partition
                    .report_fault(kuberic_core::types::FaultType::Transient);
                return Err(Status::unavailable(error.to_string()));
            }
        };
        let mut state = self.state.write().await;
        if state.generation != generation {
            *requires_recovery = true;
            self.partition
                .report_fault(kuberic_core::types::FaultType::Transient);
            return Err(Status::unavailable(
                "primary epoch changed during commit; retry the ID on the primary",
            ));
        }
        if let Err(error) = state.apply_op(lsn, &operation).await {
            *requires_recovery = true;
            self.partition
                .report_fault(kuberic_core::types::FaultType::Transient);
            return Err(Status::unavailable(format!(
                "WAL requires recovery: {error}"
            )));
        }
        state.set_committed_lsn(lsn);
        debug!(lsn, "replicated + applied");
        Ok((lsn, existed))
    }
}

#[tonic::async_trait]
impl proto::kv_store_server::KvStore for KvServer {
    async fn get(
        &self,
        request: Request<proto::GetRequest>,
    ) -> Result<Response<proto::GetResponse>, Status> {
        match self.partition.read_status() {
            AccessStatus::Granted => {}
            AccessStatus::NotPrimary => {
                return Err(Status::unavailable("not primary — redirect to primary"));
            }
            AccessStatus::ReconfigurationPending => {
                return Err(Status::unavailable("reconfiguration in progress"));
            }
            AccessStatus::NoWriteQuorum => {
                // Reads still OK on primary without write quorum
            }
        }

        let key = &request.get_ref().key;
        let state = self.state.read().await;
        match state.data.get(key) {
            Some(value) => Ok(Response::new(proto::GetResponse {
                found: true,
                value: value.clone(),
            })),
            None => Ok(Response::new(proto::GetResponse {
                found: false,
                value: String::new(),
            })),
        }
    }

    async fn put(
        &self,
        request: Request<proto::PutRequest>,
    ) -> Result<Response<proto::PutResponse>, Status> {
        let (lsn, _) = self.write(Write::Put(request.into_inner())).await?;
        Ok(Response::new(proto::PutResponse { lsn }))
    }

    async fn delete(
        &self,
        request: Request<proto::DeleteRequest>,
    ) -> Result<Response<proto::DeleteResponse>, Status> {
        let (lsn, existed) = self.write(Write::Delete(request.into_inner())).await?;
        Ok(Response::new(proto::DeleteResponse { existed, lsn }))
    }

    async fn begin_transaction(
        &self,
        request: Request<proto::BeginTransactionRequest>,
    ) -> Result<Response<proto::BeginTransactionResponse>, Status> {
        self.check_write_access()?;
        let mut transactions = self.transactions.lock().await;
        let state = self.state.read().await;
        let transaction_id = transactions.begin(&state, request.into_inner().timeout_ms)?;
        Ok(Response::new(proto::BeginTransactionResponse {
            transaction_id,
        }))
    }

    async fn transaction_get(
        &self,
        request: Request<proto::TransactionGetRequest>,
    ) -> Result<Response<proto::GetResponse>, Status> {
        self.check_write_access()?;
        let request = request.into_inner();
        let mut transactions = self.transactions.lock().await;
        let state = self.state.read().await;
        let value = transactions
            .get_mut(&state, &request.transaction_id)?
            .get(&state, &request.key)?;
        Ok(Response::new(proto::GetResponse {
            found: value.is_some(),
            value: value.unwrap_or_default(),
        }))
    }

    async fn transaction_put(
        &self,
        request: Request<proto::TransactionPutRequest>,
    ) -> Result<Response<proto::TransactionResponse>, Status> {
        self.check_write_access()?;
        let request = request.into_inner();
        let mut transactions = self.transactions.lock().await;
        let state = self.state.read().await;
        transactions
            .get_mut(&state, &request.transaction_id)?
            .stage(
                &state,
                KvMutation::Put {
                    key: request.key,
                    value: request.value,
                },
            )?;
        Ok(Response::new(proto::TransactionResponse {}))
    }

    async fn transaction_delete(
        &self,
        request: Request<proto::TransactionDeleteRequest>,
    ) -> Result<Response<proto::TransactionResponse>, Status> {
        self.check_write_access()?;
        let request = request.into_inner();
        let mut transactions = self.transactions.lock().await;
        let state = self.state.read().await;
        transactions
            .get_mut(&state, &request.transaction_id)?
            .stage(&state, KvMutation::Delete { key: request.key })?;
        Ok(Response::new(proto::TransactionResponse {}))
    }

    async fn commit_transaction(
        &self,
        request: Request<proto::TransactionRequest>,
    ) -> Result<Response<proto::CommitTransactionResponse>, Status> {
        let (lsn, _) = self
            .write(Write::Commit(request.into_inner().transaction_id))
            .await?;
        Ok(Response::new(proto::CommitTransactionResponse { lsn }))
    }

    async fn abort_transaction(
        &self,
        request: Request<proto::TransactionRequest>,
    ) -> Result<Response<proto::TransactionResponse>, Status> {
        self.check_write_access()?;
        let _gate = self.commit_gate.lock().await;
        let transaction_id = request.into_inner().transaction_id;
        let mut transactions = self.transactions.lock().await;
        if self
            .state
            .read()
            .await
            .transaction_lsn(&transaction_id)
            .is_some()
        {
            return Err(Status::failed_precondition("transaction already committed"));
        }
        transactions.abort(&transaction_id);
        Ok(Response::new(proto::TransactionResponse {}))
    }

    async fn execute_transaction(
        &self,
        request: Request<proto::ExecuteTransactionRequest>,
    ) -> Result<Response<proto::CommitTransactionResponse>, Status> {
        let (lsn, _) = self.write(Write::Execute(request.into_inner())).await?;
        Ok(Response::new(proto::CommitTransactionResponse { lsn }))
    }
}

/// Start the client-facing KV gRPC server.
pub async fn run_client_server(
    bind: String,
    state: SharedState,
    partition: Arc<PartitionHandle>,
    replicator: StateReplicatorHandle,
    token: CancellationToken,
    shutdown: CancellationToken,
) {
    let listener = match tokio::net::TcpListener::bind(&bind).await {
        Ok(l) => l,
        Err(e) => {
            warn!(error = %e, "failed to bind client server");
            return;
        }
    };
    let addr = listener.local_addr().unwrap();
    info!(%addr, "client KV gRPC server started");

    let server = KvServer::new(state, partition, replicator, token);
    let cleanup_server = server.clone();
    let cleanup = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            let mut transactions = cleanup_server.transactions.lock().await;
            let generation = cleanup_server.state.read().await.generation;
            transactions.cleanup(generation, tokio::time::Instant::now());
        }
    });

    let _ = tonic::transport::Server::builder()
        .add_service(proto::kv_store_server::KvStoreServer::new(server))
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
            shutdown.cancelled(),
        )
        .await;

    cleanup.abort();
    let _ = cleanup.await;
    info!("client KV gRPC server stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::kv_store_server::KvStore;
    use crate::state::KvState;
    use kuberic_core::handles::PartitionState;
    use tokio::sync::{RwLock, mpsc};

    #[tokio::test]
    async fn uncertain_commit_remains_fenced_across_epoch_changes() {
        for quorum_succeeded in [false, true] {
            let directory =
                std::env::temp_dir().join(format!("kv-uncertain-{:032x}", rand::random::<u128>()));
            let state = Arc::new(RwLock::new(KvState::open(directory.clone()).await.unwrap()));
            let partition_state = Arc::new(PartitionState::new());
            partition_state.set_write_status(AccessStatus::Granted);
            let (fault_tx, mut faults) = mpsc::channel(1);
            let partition = Arc::new(PartitionHandle::new(partition_state.clone(), fault_tx));
            let (request_tx, mut requests) = mpsc::channel(1);
            let server = KvServer::new(
                state.clone(),
                partition,
                StateReplicatorHandle::new(request_tx, partition_state),
                CancellationToken::new(),
            );
            let transaction_id = server
                .begin_transaction(Request::new(proto::BeginTransactionRequest::default()))
                .await
                .unwrap()
                .into_inner()
                .transaction_id;
            server
                .transaction_put(Request::new(proto::TransactionPutRequest {
                    transaction_id: transaction_id.clone(),
                    key: "pending".into(),
                    value: "value".into(),
                }))
                .await
                .unwrap();
            let committing = server.clone();
            let commit = tokio::spawn(async move {
                committing
                    .commit_transaction(Request::new(proto::TransactionRequest { transaction_id }))
                    .await
            });
            let operation = requests.recv().await.unwrap();
            if quorum_succeeded {
                state.write().await.generation += 1;
                operation.reply.send(Ok(1)).unwrap();
            } else {
                operation
                    .reply
                    .send(Err(kuberic_core::KubericError::NoWriteQuorum))
                    .unwrap();
            }
            assert_eq!(
                commit.await.unwrap().unwrap_err().code(),
                tonic::Code::Unavailable
            );
            assert_eq!(
                faults.try_recv().unwrap(),
                kuberic_core::types::FaultType::Transient
            );
            state.write().await.generation += 1;
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                server.put(Request::new(proto::PutRequest {
                    key: "later".into(),
                    value: "must-not-pass".into(),
                })),
            )
            .await
            .expect("fenced write must not reach replication");
            assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);
            assert!(requests.try_recv().is_err());
            assert!(state.read().await.data.is_empty());
            drop(server);
            drop(state);
            tokio::fs::remove_dir_all(directory).await.unwrap();
        }
    }

    #[tokio::test]
    async fn cancelled_commit_finishes_once_and_serializes_ordinary_writes() {
        let directory =
            std::env::temp_dir().join(format!("kv-cancelled-{:032x}", rand::random::<u128>()));
        let state = Arc::new(RwLock::new(KvState::open(directory.clone()).await.unwrap()));
        let partition_state = Arc::new(PartitionState::new());
        partition_state.set_read_status(AccessStatus::Granted);
        partition_state.set_write_status(AccessStatus::Granted);
        let (fault_tx, _fault_rx) = mpsc::channel(1);
        let partition = Arc::new(PartitionHandle::new(partition_state.clone(), fault_tx));
        let (request_tx, mut requests) = mpsc::channel(1);
        let server = KvServer::new(
            state.clone(),
            partition,
            StateReplicatorHandle::new(request_tx, partition_state),
            CancellationToken::new(),
        );
        let transaction_id = server
            .begin_transaction(Request::new(proto::BeginTransactionRequest::default()))
            .await
            .unwrap()
            .into_inner()
            .transaction_id;
        server
            .transaction_put(Request::new(proto::TransactionPutRequest {
                transaction_id: transaction_id.clone(),
                key: "transaction".into(),
                value: "committed".into(),
            }))
            .await
            .unwrap();
        let request = proto::TransactionRequest {
            transaction_id: transaction_id.clone(),
        };
        let committing = server.clone();
        let rpc =
            tokio::spawn(async move { committing.commit_transaction(Request::new(request)).await });
        let operation = requests.recv().await.unwrap();
        let writing = server.clone();
        let write = tokio::spawn(async move {
            writing
                .put(Request::new(proto::PutRequest {
                    key: "ordinary".into(),
                    value: "next".into(),
                }))
                .await
        });
        rpc.abort();
        let _ = rpc.await;
        assert!(requests.try_recv().is_err());
        operation.reply.send(Ok(1)).unwrap();
        let operation = tokio::time::timeout(std::time::Duration::from_secs(5), requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(state.read().await.transaction_lsn(&transaction_id), Some(1));
        operation.reply.send(Ok(2)).unwrap();
        assert_eq!(write.await.unwrap().unwrap().into_inner().lsn, 2);
        assert_eq!(
            server
                .commit_transaction(Request::new(proto::TransactionRequest { transaction_id }))
                .await
                .unwrap()
                .into_inner()
                .lsn,
            1
        );
        assert!(requests.try_recv().is_err());
        assert_eq!(state.read().await.data.len(), 2);
        drop(server);
        drop(state);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }
}
