use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use tokio::io::AsyncReadExt;

use crate::observation::ObservationTarget;
use crate::runtime_error::RuntimeError;
use crate::{AvailabilityGroupName, Endpoint, ObservationFailureKind, ReplicaIdentity, ServerName};

const MAX_CONFIG_BYTES: u64 = 65_536;
const MAX_DURATION_MILLIS: u64 = 300_000;

#[derive(Debug, Clone)]
pub struct ConnectionSettings {
    pub(crate) endpoint: Endpoint,
    pub(crate) username_file: PathBuf,
    pub(crate) password_file: PathBuf,
    pub(crate) ca_certificate_file: Option<PathBuf>,
    pub(crate) connect_timeout: Duration,
    pub(crate) query_timeout: Duration,
}

impl ConnectionSettings {
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
}

#[derive(Debug, Clone)]
pub struct ObserverConfig {
    connection: ConnectionSettings,
    target: ObservationTarget,
    sample_timeout: Duration,
    poll_interval: Duration,
    max_age_millis: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigFile {
    #[serde(default)]
    mode: RuntimeMode,
    host: String,
    #[serde(default = "default_port")]
    port: u16,
    availability_group: String,
    expected_server_name: String,
    replica_id: String,
    incarnation: String,
    observer_username_file: PathBuf,
    observer_password_file: PathBuf,
    ca_certificate_file: Option<PathBuf>,
    #[serde(default = "default_connect_timeout")]
    connect_timeout_ms: u64,
    #[serde(default = "default_query_timeout")]
    query_timeout_ms: u64,
    #[serde(default = "default_sample_timeout")]
    sample_timeout_ms: u64,
    #[serde(default = "default_poll_interval")]
    poll_interval_ms: u64,
    #[serde(default = "default_max_age")]
    max_age_ms: u64,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RuntimeMode {
    #[default]
    ObserveOnly,
}

impl ObserverConfig {
    pub async fn read(path: &Path) -> Result<Self, RuntimeError> {
        let bytes = read_bounded(path, MAX_CONFIG_BYTES, "configuration").await?;
        Self::from_json(&bytes)
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, RuntimeError> {
        if bytes.len() as u64 > MAX_CONFIG_BYTES {
            return Err(invalid("configuration exceeds 64 KiB"));
        }
        let file: ConfigFile = serde_json::from_slice(bytes).map_err(|_| {
            invalid("expected a valid observe-only configuration with no unknown fields")
        })?;
        let RuntimeMode::ObserveOnly = file.mode;
        let endpoint = Endpoint::new(file.host, file.port).map_err(|_| {
            invalid("host must be a DNS name or IPv4 address and port must be nonzero")
        })?;
        let target = ObservationTarget {
            availability_group: AvailabilityGroupName::new(file.availability_group)
                .map_err(|_| invalid("invalid availability group name"))?,
            expected_server_name: ServerName::new(file.expected_server_name)
                .map_err(|_| invalid("invalid expected server name"))?,
            replica: ReplicaIdentity::desired(file.replica_id, file.incarnation)
                .map_err(|_| invalid("invalid logical replica ID or incarnation"))?,
        };
        validate_path(&file.observer_username_file)?;
        validate_path(&file.observer_password_file)?;
        if file.observer_username_file == file.observer_password_file {
            return Err(invalid(
                "username and password must use distinct Secret files",
            ));
        }
        if let Some(path) = &file.ca_certificate_file {
            validate_path(path)?;
            let supported = path
                .extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| {
                    ["pem", "crt", "der"]
                        .iter()
                        .any(|allowed| extension.eq_ignore_ascii_case(allowed))
                });
            if !supported {
                return Err(invalid(
                    "CA certificate must have a .pem, .crt or .der extension",
                ));
            }
        }
        let connect_timeout = duration(file.connect_timeout_ms)?;
        let query_timeout = duration(file.query_timeout_ms)?;
        let sample_timeout = duration(file.sample_timeout_ms)?;
        if connect_timeout > sample_timeout || query_timeout > sample_timeout {
            return Err(invalid(
                "connect and query timeouts must not exceed the sample timeout",
            ));
        }
        duration(file.max_age_ms)?;
        Ok(Self {
            connection: ConnectionSettings {
                endpoint,
                username_file: file.observer_username_file,
                password_file: file.observer_password_file,
                ca_certificate_file: file.ca_certificate_file,
                connect_timeout,
                query_timeout,
            },
            target,
            sample_timeout,
            poll_interval: duration(file.poll_interval_ms)?,
            max_age_millis: file.max_age_ms,
        })
    }

    pub fn connection(&self) -> &ConnectionSettings {
        &self.connection
    }

    pub fn target(&self) -> &ObservationTarget {
        &self.target
    }

    pub fn sample_timeout(&self) -> Duration {
        self.sample_timeout
    }

    pub fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    pub fn max_age(&self) -> Duration {
        Duration::from_millis(self.max_age_millis)
    }

    pub fn max_age_millis(&self) -> u64 {
        self.max_age_millis
    }
}

pub(crate) async fn read_bounded(
    path: &Path,
    max_bytes: u64,
    stage: &'static str,
) -> Result<Vec<u8>, RuntimeError> {
    if !tokio::fs::metadata(path)
        .await
        .map_err(|error| file_error(stage, error.kind()))?
        .is_file()
    {
        return Err(RuntimeError::new(
            ObservationFailureKind::Malformed,
            stage,
            "expected a regular mounted file",
        ));
    }
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|error| file_error(stage, error.kind()))?;
    if !file
        .metadata()
        .await
        .map_err(|error| file_error(stage, error.kind()))?
        .is_file()
    {
        return Err(RuntimeError::new(
            ObservationFailureKind::Malformed,
            stage,
            "expected a regular mounted file",
        ));
    }
    let mut bytes = Vec::new();
    file.take(max_bytes + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|error| file_error(stage, error.kind()))?;
    if bytes.len() as u64 > max_bytes {
        return Err(RuntimeError::new(
            ObservationFailureKind::Malformed,
            stage,
            "mounted file exceeds the size limit",
        ));
    }
    Ok(bytes)
}

fn file_error(stage: &'static str, kind: std::io::ErrorKind) -> RuntimeError {
    RuntimeError::new(
        if kind == std::io::ErrorKind::PermissionDenied {
            ObservationFailureKind::PermissionDenied
        } else {
            ObservationFailureKind::Unreachable
        },
        stage,
        "cannot read mounted file; check its path and permissions",
    )
}

fn validate_path(path: &Path) -> Result<(), RuntimeError> {
    if !path.is_absolute()
        || path
            .to_str()
            .is_none_or(|value| value.chars().any(char::is_control))
    {
        return Err(invalid(
            "mounted file paths must be absolute UTF-8 paths without control characters",
        ));
    }
    Ok(())
}

fn invalid(message: &'static str) -> RuntimeError {
    RuntimeError::new(ObservationFailureKind::Malformed, "configuration", message)
}

fn duration(millis: u64) -> Result<Duration, RuntimeError> {
    if !(1..=MAX_DURATION_MILLIS).contains(&millis) {
        return Err(invalid(
            "durations must be between 1 and 300000 milliseconds",
        ));
    }
    Ok(Duration::from_millis(millis))
}

fn default_port() -> u16 {
    1433
}
fn default_connect_timeout() -> u64 {
    5_000
}
fn default_query_timeout() -> u64 {
    5_000
}
fn default_sample_timeout() -> u64 {
    30_000
}
fn default_poll_interval() -> u64 {
    1_000
}
fn default_max_age() -> u64 {
    60_000
}
