//! SQL RPCs use asynchronous v2 access checks and one serialized connection.
use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::barrier::CommitFailure;
use crate::connection::Statement;
use crate::proto;
use crate::service::SqliteService;

#[derive(Clone)]
pub struct SqliteServer {
    application: Arc<SqliteService>,
}

impl SqliteServer {
    pub fn new(application: Arc<SqliteService>) -> Self {
        Self { application }
    }

    async fn write(&self, statements: Vec<Statement>) -> Result<(Vec<usize>, i64, i64), Status> {
        self.application.access(true).await.map_err(unavailable)?;
        let permit = self.application.request_gate.clone().lock_owned().await;
        let (_, recovered) = self.application.access(true).await.map_err(unavailable)?;
        let application = self.application.clone();
        let result = tokio::task::spawn_blocking(move || {
            // A cancelled RPC must not release serialization while SQLite is still
            // waiting at the barrier or recording successful publication.
            let _permit = permit;
            let mut sql = application.sql.lock().expect("SQL connection");
            application
                .prepare_sql(&mut sql, recovered)
                .map_err(|e| Status::failed_precondition(e.to_string()))?;
            let progress = application
                .persistence()
                .progress()
                .map_err(|e| Status::internal(e.to_string()))?;
            if progress.applied_lsn != progress.committed_lsn {
                return Err(Status::failed_precondition(
                    "an applied suffix must be settled by authority before new SQL writes",
                ));
            }
            let before = progress.committed_lsn;
            application.barrier().reset_receipt(before);
            let executed = sql.execute(&statements);
            let lsn = application.barrier().last_lsn();
            match executed {
                Ok((rows, rowid)) => {
                    if application.barrier().is_fenced() {
                        return Err(unknown("local publication was interrupted"));
                    }
                    if let Err(error) = application.persistence().confirm_publication(lsn) {
                        application.barrier().fence_unknown(&error.to_string());
                        return Err(unknown(&error.to_string()));
                    }
                    sql.visible_lsn = lsn;
                    Ok((rows, rowid, lsn))
                }
                Err(error) => match application.barrier().failure() {
                    Some(CommitFailure::Unknown(reason)) => Err(unknown(&reason)),
                    Some(CommitFailure::Definitive(reason)) => Err(Status::unavailable(format!(
                        "transaction rejected before dispatch: {reason}"
                    ))),
                    None if lsn > before => {
                        application.barrier().fence_unknown(&error.to_string());
                        Err(unknown(&error.to_string()))
                    }
                    None => Err(Status::invalid_argument(format!("SQL rejected: {error}"))),
                },
            }
        })
        .await;
        match result {
            Ok(result) => result,
            Err(error) => {
                self.application.barrier().fence_unknown(&error.to_string());
                Err(unknown(&error.to_string()))
            }
        }
    }
}

fn unavailable(error: kuberic_runtime::RuntimeError) -> Status {
    Status::unavailable(error.to_string())
}

fn unknown(reason: &str) -> Status {
    Status::unknown(format!(
        "outcome unknown after replication dispatch: {reason}; reopen/reconcile and verify the transaction before retrying"
    ))
}

#[tonic::async_trait]
impl proto::sqlite_store_server::SqliteStore for SqliteServer {
    async fn execute(
        &self,
        request: Request<proto::ExecuteRequest>,
    ) -> Result<Response<proto::ExecuteResponse>, Status> {
        let request = request.into_inner();
        let (rows, rowid, lsn) = self
            .write(vec![(request.sql, convert_params(&request.params))])
            .await?;
        Ok(Response::new(proto::ExecuteResponse {
            rows_affected: rows[0] as i64,
            last_insert_rowid: rowid,
            lsn,
        }))
    }

    async fn execute_batch(
        &self,
        request: Request<proto::ExecuteBatchRequest>,
    ) -> Result<Response<proto::ExecuteBatchResponse>, Status> {
        let statements = request
            .into_inner()
            .statements
            .into_iter()
            .map(|sql| (sql, Vec::new()))
            .collect();
        let (rows, _, lsn) = self.write(statements).await?;
        Ok(Response::new(proto::ExecuteBatchResponse {
            rows_affected: rows.into_iter().map(|n| n as i64).collect(),
            lsn,
        }))
    }

    async fn query(
        &self,
        request: Request<proto::QueryRequest>,
    ) -> Result<Response<proto::QueryResponse>, Status> {
        self.application.access(false).await.map_err(unavailable)?;
        let permit = self.application.request_gate.clone().lock_owned().await;
        let (_, recovered) = self.application.access(false).await.map_err(unavailable)?;
        let request = request.into_inner();
        let application = self.application.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut sql = application.sql.lock().expect("SQL connection");
            application
                .prepare_sql(&mut sql, recovered)
                .map_err(|e| Status::failed_precondition(e.to_string()))?;
            let (columns, rows) = sql
                .query(&request.sql, &convert_params(&request.params))
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            Ok(Response::new(proto::QueryResponse {
                columns,
                rows: rows
                    .into_iter()
                    .map(|values| proto::Row {
                        values: values.into_iter().map(proto_value).collect(),
                    })
                    .collect(),
            }))
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?
    }
}

fn convert_params(params: &[proto::Value]) -> Vec<rusqlite::types::Value> {
    params
        .iter()
        .map(|value| {
            if value.is_null {
                return rusqlite::types::Value::Null;
            }
            match &value.kind {
                Some(proto::value::Kind::IntegerValue(i)) => rusqlite::types::Value::Integer(*i),
                Some(proto::value::Kind::RealValue(f)) => rusqlite::types::Value::Real(*f),
                Some(proto::value::Kind::TextValue(s)) => rusqlite::types::Value::Text(s.clone()),
                Some(proto::value::Kind::BlobValue(b)) => rusqlite::types::Value::Blob(b.clone()),
                None => rusqlite::types::Value::Null,
            }
        })
        .collect()
}

fn proto_value(value: rusqlite::types::Value) -> proto::Value {
    let kind = match value {
        rusqlite::types::Value::Null => None,
        rusqlite::types::Value::Integer(i) => Some(proto::value::Kind::IntegerValue(i)),
        rusqlite::types::Value::Real(f) => Some(proto::value::Kind::RealValue(f)),
        rusqlite::types::Value::Text(s) => Some(proto::value::Kind::TextValue(s)),
        rusqlite::types::Value::Blob(b) => Some(proto::value::Kind::BlobValue(b)),
    };
    proto::Value {
        is_null: kind.is_none(),
        kind,
    }
}
