//! Client-facing SQL gRPC server (primary only).

use std::sync::Arc;

use kuberic_core::handles::PartitionHandle;
use kuberic_core::types::{AccessStatus, CancellationToken};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use crate::proto;
use crate::state::SharedState;

pub struct SqliteServer {
    pub state: SharedState,
    pub partition: Arc<PartitionHandle>,
    write_gate: Arc<Mutex<()>>,
}

#[tonic::async_trait]
impl proto::sqlite_store_server::SqliteStore for SqliteServer {
    async fn execute(
        &self,
        request: Request<proto::ExecuteRequest>,
    ) -> Result<Response<proto::ExecuteResponse>, Status> {
        self.check_write_access()?;

        let _write = self.write_gate.lock().await;

        let req = request.into_inner();
        let params = convert_params(&req.params);
        let sql = req.sql;

        // Execute SQL on blocking thread (rusqlite is synchronous)
        let state = self.state.clone();
        let (rows_affected, last_insert_rowid) = tokio::task::spawn_blocking(move || {
            let state = state.blocking_lock();
            state.execute_sql(&sql, &params)
        })
        .await
        .map_err(|e| Status::internal(format!("task join error: {e}")))?
        .map_err(|e| Status::internal(format!("SQL error: {e}")))?;

        // The commit blocked on durable quorum inside the barrier VFS.
        let lsn = self.confirm_committed().await?;

        debug!(lsn, rows_affected, "execute complete");
        Ok(Response::new(proto::ExecuteResponse {
            rows_affected: rows_affected as i64,
            last_insert_rowid,
            lsn,
        }))
    }

    async fn query(
        &self,
        request: Request<proto::QueryRequest>,
    ) -> Result<Response<proto::QueryResponse>, Status> {
        self.check_read_access()?;

        let req = request.into_inner();
        let params = convert_params(&req.params);
        let sql = req.sql;

        let state = self.state.clone();
        let (columns, rows) = tokio::task::spawn_blocking(move || {
            let state = state.blocking_lock();
            state.query_sql(&sql, &params)
        })
        .await
        .map_err(|e| Status::internal(format!("task join error: {e}")))?
        .map_err(|e| Status::internal(format!("SQL error: {e}")))?;

        let proto_rows: Vec<proto::Row> = rows
            .into_iter()
            .map(|row| proto::Row {
                values: row.into_iter().map(json_to_proto_value).collect(),
            })
            .collect();

        Ok(Response::new(proto::QueryResponse {
            columns,
            rows: proto_rows,
        }))
    }

    async fn execute_batch(
        &self,
        request: Request<proto::ExecuteBatchRequest>,
    ) -> Result<Response<proto::ExecuteBatchResponse>, Status> {
        self.check_write_access()?;

        let _write = self.write_gate.lock().await;

        let req = request.into_inner();
        let statements = req.statements;

        let state = self.state.clone();
        let rows_affected = tokio::task::spawn_blocking(move || {
            let mut state = state.blocking_lock();
            state.execute_batch_sql(&statements)
        })
        .await
        .map_err(|e| Status::internal(format!("task join error: {e}")))?
        .map_err(|e| Status::internal(format!("SQL error: {e}")))?;

        // The commit blocked on durable quorum inside the barrier VFS.
        let lsn = self.confirm_committed().await?;

        debug!(
            lsn,
            statements_count = rows_affected.len(),
            "batch complete"
        );
        Ok(Response::new(proto::ExecuteBatchResponse {
            rows_affected: rows_affected.into_iter().map(|r| r as i64).collect(),
            lsn,
        }))
    }
}

impl SqliteServer {
    fn check_write_access(&self) -> Result<(), Status> {
        match self.partition.read_status() {
            AccessStatus::Granted => Ok(()),
            AccessStatus::NotPrimary => {
                Err(Status::unavailable("not primary — redirect to primary"))
            }
            AccessStatus::ReconfigurationPending => {
                Err(Status::unavailable("reconfiguration in progress"))
            }
            AccessStatus::NoWriteQuorum => Err(Status::unavailable("no write quorum available")),
        }
    }

    fn check_read_access(&self) -> Result<(), Status> {
        match self.partition.read_status() {
            AccessStatus::Granted | AccessStatus::NoWriteQuorum => Ok(()),
            AccessStatus::NotPrimary => {
                Err(Status::unavailable("not primary — redirect to primary"))
            }
            AccessStatus::ReconfigurationPending => {
                Err(Status::unavailable("reconfiguration in progress"))
            }
        }
    }

    /// The commit itself waits for durable quorum inside the barrier VFS, so a
    /// returned statement has already been replicated. The LSN is whatever the
    /// barrier recorded for that commit.
    async fn confirm_committed(&self) -> Result<i64, Status> {
        let lsn = crate::barrier::barrier().last_lsn();
        self.state
            .lock()
            .await
            .mark_confirmed(lsn)
            .await
            .map_err(|e| Status::internal(format!("confirmation record failed: {e}")))?;
        Ok(lsn)
    }
}

/// Start the client-facing SQL gRPC server.
pub async fn run_client_server(
    bind: String,
    state: SharedState,
    partition: Arc<PartitionHandle>,
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
    info!(%addr, "client SQL gRPC server started");

    let server = SqliteServer {
        state,
        partition,
        write_gate: Arc::new(Mutex::new(())),
    };

    let _ = tonic::transport::Server::builder()
        .add_service(proto::sqlite_store_server::SqliteStoreServer::new(server))
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
            shutdown.cancelled(),
        )
        .await;

    info!("client SQL gRPC server stopped");
}

fn convert_params(params: &[proto::Value]) -> Vec<rusqlite::types::Value> {
    params
        .iter()
        .map(|v| {
            if v.is_null {
                return rusqlite::types::Value::Null;
            }
            match &v.kind {
                Some(proto::value::Kind::IntegerValue(i)) => rusqlite::types::Value::Integer(*i),
                Some(proto::value::Kind::RealValue(f)) => rusqlite::types::Value::Real(*f),
                Some(proto::value::Kind::TextValue(s)) => rusqlite::types::Value::Text(s.clone()),
                Some(proto::value::Kind::BlobValue(b)) => rusqlite::types::Value::Blob(b.clone()),
                None => rusqlite::types::Value::Null,
            }
        })
        .collect()
}

fn json_to_proto_value(val: serde_json::Value) -> proto::Value {
    match val {
        serde_json::Value::Null => proto::Value {
            kind: None,
            is_null: true,
        },
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                proto::Value {
                    kind: Some(proto::value::Kind::IntegerValue(i)),
                    is_null: false,
                }
            } else if let Some(f) = n.as_f64() {
                proto::Value {
                    kind: Some(proto::value::Kind::RealValue(f)),
                    is_null: false,
                }
            } else {
                proto::Value {
                    kind: None,
                    is_null: true,
                }
            }
        }
        serde_json::Value::String(s) => proto::Value {
            kind: Some(proto::value::Kind::TextValue(s)),
            is_null: false,
        },
        _ => proto::Value {
            kind: Some(proto::value::Kind::TextValue(val.to_string())),
            is_null: false,
        },
    }
}
