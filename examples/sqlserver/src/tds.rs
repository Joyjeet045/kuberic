mod panic_boundary;

use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use futures::TryStreamExt;
use tiberius::{AuthMethod, Client, Column, ColumnType, Config, EncryptionLevel, QueryItem};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use zeroize::Zeroizing;

use crate::executor::{QueryRow, SqlExecutor, SqlSession};
use crate::query::ReadQuery;
use crate::runtime_config::{ConnectionSettings, read_bounded};
use crate::runtime_error::RuntimeError;
use crate::{AvailabilityGroupName, ObservationFailureKind};

const MAX_SECRET_BYTES: u64 = 4096;
const MAX_QUERY_ROWS: usize = 4096;
const MAX_CELL_BYTES: usize = 4096;

pub struct TdsExecutor {
    settings: ConnectionSettings,
}

impl TdsExecutor {
    pub fn new(settings: ConnectionSettings) -> Self {
        Self { settings }
    }
}

struct TdsSession {
    client: Option<Client<Compat<TcpStream>>>,
    query_timeout: Duration,
}

#[async_trait]
impl SqlExecutor for TdsExecutor {
    async fn connect(&self) -> Result<Box<dyn SqlSession>, RuntimeError> {
        timeout(self.settings.connect_timeout, async {
            let username = read_secret(&self.settings.username_file, "observer username").await?;
            let password = read_secret(&self.settings.password_file, "observer password").await?;
            let config = connection_config(&self.settings, &username, &password);
            let tcp =
                TcpStream::connect((self.settings.endpoint.host(), self.settings.endpoint.port()))
                    .await
                    .map_err(|_| {
                        RuntimeError::new(
                            ObservationFailureKind::Unreachable,
                            "TCP connect",
                            "cannot reach the configured SQL Server endpoint",
                        )
                    })?;
            tcp.set_nodelay(true).map_err(|_| {
                RuntimeError::new(
                    ObservationFailureKind::Unreachable,
                    "TCP connect",
                    "cannot configure the SQL Server connection",
                )
            })?;
            let client = panic_boundary::contain("TLS/TDS login", async {
                Client::connect(config, tcp.compat_write())
                    .await
                    .map_err(|error| driver_error("TLS/TDS login", error))
            })
            .await?;
            Ok(Box::new(TdsSession {
                client: Some(client),
                query_timeout: self.settings.query_timeout,
            }) as Box<dyn SqlSession>)
        })
        .await
        .map_err(|_| timed_out("TLS/TDS connect"))?
    }
}

fn connection_config(settings: &ConnectionSettings, username: &str, password: &str) -> Config {
    let mut config = Config::new();
    config.host(settings.endpoint.host());
    config.port(settings.endpoint.port());
    config.database("master");
    config.application_name("kuberic-sqlserver-observer");
    // Required avoids both plaintext fallback and the driver's On/Off panic.
    config.encryption(EncryptionLevel::Required);
    if let Some(path) = &settings.ca_certificate_file {
        // Config validation rejects non-UTF-8 paths before this point.
        config.trust_cert_ca(path.to_string_lossy());
    }
    config.authentication(AuthMethod::sql_server(username, password));
    // ApplicationIntent is not a write fence and can trigger replica routing.
    // Connect to the named instance directly; only ReadQuery statements are exposed.
    config
}

#[async_trait]
impl SqlSession for TdsSession {
    async fn query(
        &mut self,
        query: ReadQuery,
        availability_group: &AvailabilityGroupName,
    ) -> Result<Vec<QueryRow>, RuntimeError> {
        // The future owns the client, so an error, panic, or cancellation cannot
        // leave a partially decoded session available for another query.
        let mut client = self.client.take().ok_or_else(|| {
            malformed(
                query.label(),
                "TDS session is closed; reconnect before observing",
            )
        })?;
        let (client, rows) = timeout(
            self.query_timeout,
            panic_boundary::contain(query.label(), async move {
                let name = availability_group.as_str();
                let mut stream = client
                    .query(query.sql(), &[&name])
                    .await
                    .map_err(|error| driver_error(query.label(), error))?;
                let mut rows = Vec::new();
                let mut result_sets = 0;
                while let Some(item) = stream
                    .try_next()
                    .await
                    .map_err(|error| driver_error(query.label(), error))?
                {
                    match item {
                        QueryItem::Metadata(metadata) => {
                            result_sets += 1;
                            if result_sets != 1 {
                                return Err(malformed(
                                    query.label(),
                                    "unexpected multiple result sets",
                                ));
                            }
                            validate_columns(query, metadata.columns())?;
                        }
                        QueryItem::Row(row) => {
                            if rows.len() == MAX_QUERY_ROWS {
                                return Err(malformed(
                                    query.label(),
                                    "query exceeds the 4096-row observation limit",
                                ));
                            }
                            let mut values = QueryRow::new();
                            for (index, column) in row.columns().iter().enumerate() {
                                let value = row.try_get::<&str, _>(index).map_err(|_| {
                                    malformed(query.label(), "expected a text or NULL DMV column")
                                })?;
                                if value.is_some_and(|text| text.len() > MAX_CELL_BYTES) {
                                    return Err(malformed(
                                        query.label(),
                                        "DMV column exceeds the size limit",
                                    ));
                                }
                                if values
                                    .insert(column.name().to_owned(), value.map(str::to_owned))
                                    .is_some()
                                {
                                    return Err(malformed(
                                        query.label(),
                                        "duplicate DMV column name",
                                    ));
                                }
                            }
                            rows.push(values);
                        }
                    }
                }
                if result_sets != 1 {
                    return Err(malformed(query.label(), "missing DMV result set"));
                }
                drop(stream);
                Ok((client, rows))
            }),
        )
        .await
        .map_err(|_| timed_out(query.label()))??;
        self.client = Some(client);
        Ok(rows)
    }
}

fn validate_columns(query: ReadQuery, columns: &[Column]) -> Result<(), RuntimeError> {
    if columns.len() != query.columns().len()
        || columns
            .iter()
            .zip(query.columns())
            .any(|(actual, expected)| {
                actual.name() != *expected || actual.column_type() != ColumnType::NVarchar
            })
    {
        return Err(malformed(
            query.label(),
            "DMV result schema does not match the predefined query",
        ));
    }
    Ok(())
}

async fn read_secret(path: &Path, stage: &'static str) -> Result<Zeroizing<String>, RuntimeError> {
    let bytes = Zeroizing::new(read_bounded(path, MAX_SECRET_BYTES, stage).await?);
    let text =
        std::str::from_utf8(&bytes).map_err(|_| malformed(stage, "Secret must be UTF-8 text"))?;
    if text.is_empty() || text.contains(['\0', '\r', '\n']) {
        return Err(malformed(
            stage,
            "Secret must be nonempty text without NUL or line endings",
        ));
    }
    Ok(Zeroizing::new(text.to_owned()))
}

fn driver_error(stage: &'static str, error: tiberius::error::Error) -> RuntimeError {
    use tiberius::error::Error;

    let code = error.code();
    let (kind, message) = match &error {
        Error::Server(_) => server_error(code),
        Error::Tls(_) => (
            ObservationFailureKind::Tls,
            "TLS certificate or handshake validation failed",
        ),
        Error::Io { kind, .. } if *kind == std::io::ErrorKind::TimedOut => (
            ObservationFailureKind::TimedOut,
            "SQL Server transport timed out",
        ),
        Error::Io { kind, .. }
            if stage == "TLS/TDS login"
                && matches!(
                    kind,
                    std::io::ErrorKind::InvalidData | std::io::ErrorKind::InvalidInput
                ) =>
        {
            (
                ObservationFailureKind::Tls,
                "TLS certificate or handshake validation failed",
            )
        }
        Error::Io { .. } => (
            ObservationFailureKind::Unreachable,
            "SQL Server transport failed",
        ),
        Error::Routing { .. } => (
            ObservationFailureKind::Unsupported,
            "SQL Server routing is not allowed for an instance-bound observer",
        ),
        _ => (
            ObservationFailureKind::Malformed,
            "invalid TDS response or unsupported column encoding",
        ),
    };
    let mut failure = RuntimeError::new(kind, stage, message);
    failure.server_code = code;
    failure
}

fn server_error(code: Option<u32>) -> (ObservationFailureKind, &'static str) {
    match code {
        Some(18452 | 18456 | 18470 | 18486 | 18487 | 18488) => (
            ObservationFailureKind::Authentication,
            "SQL Server authentication failed",
        ),
        Some(229 | 230 | 262 | 297 | 300 | 916) => (
            ObservationFailureKind::PermissionDenied,
            "SQL Server denied the observation permission",
        ),
        _ => (
            ObservationFailureKind::Unsupported,
            "SQL Server rejected the observation query",
        ),
    }
}

fn timed_out(stage: &'static str) -> RuntimeError {
    RuntimeError::new(
        ObservationFailureKind::TimedOut,
        stage,
        "observation deadline exceeded",
    )
}

fn malformed(stage: &'static str, message: &'static str) -> RuntimeError {
    RuntimeError::new(ObservationFailureKind::Malformed, stage, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn driver_errors_never_echo_server_text() {
        let marker = "secret-password-and-connection-string";
        let errors = [
            tiberius::error::Error::Protocol(marker.into()),
            tiberius::error::Error::Tls(marker.to_owned()),
            tiberius::error::Error::Io {
                kind: std::io::ErrorKind::ConnectionReset,
                message: marker.to_owned(),
            },
            tiberius::error::Error::Routing {
                host: marker.to_owned(),
                port: 1433,
            },
        ];
        for error in errors {
            let sanitized = driver_error("test", error);
            assert!(!sanitized.to_string().contains(marker));
            assert!(!format!("{sanitized:?}").contains(marker));
        }
    }

    #[test]
    fn permission_and_login_failures_are_distinct() {
        assert_eq!(
            server_error(Some(297)).0,
            ObservationFailureKind::PermissionDenied
        );
        assert_eq!(
            server_error(Some(18456)).0,
            ObservationFailureKind::Authentication
        );
        assert_eq!(
            server_error(Some(208)).0,
            ObservationFailureKind::Unsupported
        );
    }

    #[tokio::test]
    async fn discarded_tds_sessions_require_a_new_connection() {
        let mut session = TdsSession {
            client: None,
            query_timeout: Duration::from_secs(1),
        };
        let error = session
            .query(
                ReadQuery::Permissions,
                &AvailabilityGroupName::new("test-ag").unwrap(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind, ObservationFailureKind::Malformed);
        assert_eq!(
            error.message,
            "TDS session is closed; reconnect before observing"
        );
    }

    #[test]
    fn result_metadata_is_validated_even_when_no_rows_are_returned() {
        for query in ReadQuery::ALL {
            let expected: Vec<Column> = query
                .columns()
                .iter()
                .map(|name| Column::new((*name).to_owned(), ColumnType::NVarchar))
                .collect();
            validate_columns(query, &expected).unwrap();
            assert!(validate_columns(query, &[]).is_err());
            assert!(validate_columns(query, &expected[1..]).is_err());
            let mut reordered = expected.clone();
            reordered.swap(0, 1);
            assert!(validate_columns(query, &reordered).is_err());
            let mut wrong_type = expected.clone();
            wrong_type[0] = Column::new(query.columns()[0].to_owned(), ColumnType::Int8);
            assert!(validate_columns(query, &wrong_type).is_err());
            let mut duplicate = expected.clone();
            duplicate.push(expected[0].clone());
            assert!(validate_columns(query, &duplicate).is_err());
        }
    }
}
