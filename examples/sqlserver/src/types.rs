use std::fmt;

use crate::error::ContractError;

/// `sysname` is `nvarchar(128)`, so SQL identifiers are bounded in UTF-16 code
/// units rather than bytes. Opaque Kuberic-side identifiers never reach SQL
/// Server, so they are bounded in bytes instead.
const MAX_SQL_IDENTIFIER_UTF16: usize = 128;
const MAX_OPAQUE_ID_BYTES: usize = 256;

/// `CREATE AVAILABILITY GROUP` documents a narrower limit for the cluster types
/// that have no WSFC behind them: "The maximum length for an availability group
/// name is 128 characters for `cluster_type = WSFC` and 64 characters for
/// `cluster_type = NONE` and `EXTERNAL`."
///
/// The engine enforces this as error 19544. Before SQL Server 2022 CU23 an
/// over-length name raised an assertion failure instead, so rejecting it here
/// guards against a crash-class failure on older builds rather than merely
/// anticipating an error.
///
/// Microsoft states the bound in "characters" without naming a unit. Counting
/// UTF-16 code units is never more permissive than counting scalar values, so
/// this is the fail-closed reading.
///
/// <https://learn.microsoft.com/en-us/sql/t-sql/statements/create-availability-group-transact-sql>
const MAX_EXTERNAL_AG_NAME_UTF16: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SqlIdentifier(String);

impl SqlIdentifier {
    pub fn new(value: impl Into<String>) -> Result<Self, ContractError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ContractError::MissingField {
                field: "SQL identifier",
            });
        }
        if value.chars().any(char::is_control) {
            return Err(ContractError::InvalidCharacter {
                field: "SQL identifier",
            });
        }
        if value.trim() != value {
            return Err(ContractError::InvalidCharacter {
                field: "SQL identifier",
            });
        }
        if value.encode_utf16().count() > MAX_SQL_IDENTIFIER_UTF16 {
            return Err(ContractError::InvalidLength {
                field: "SQL identifier",
                max: MAX_SQL_IDENTIFIER_UTF16,
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn quoted(&self) -> String {
        format!("[{}]", self.0.replace(']', "]]"))
    }
}

impl fmt::Display for SqlIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AvailabilityGroupName(SqlIdentifier);

impl AvailabilityGroupName {
    pub fn new(value: impl Into<String>) -> Result<Self, ContractError> {
        let identifier = SqlIdentifier::new(value)?;
        if identifier.as_str().encode_utf16().count() > MAX_EXTERNAL_AG_NAME_UTF16 {
            return Err(ContractError::InvalidLength {
                field: "EXTERNAL availability group name",
                max: MAX_EXTERNAL_AG_NAME_UTF16,
            });
        }
        Ok(Self(identifier))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn quoted(&self) -> String {
        self.0.quoted()
    }
}

impl fmt::Display for AvailabilityGroupName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A SQL Server instance name as reported by `@@SERVERNAME`.
///
/// SQL Server instance names are case-insensitive, so the value is lowercased at
/// construction. Normalizing here rather than at each use keeps duplicate
/// detection and canonical operation encoding from disagreeing about whether two
/// spellings name the same instance.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServerName(SqlIdentifier);

impl ServerName {
    pub fn new(value: impl Into<String>) -> Result<Self, ContractError> {
        let mut value = value.into();
        value.make_ascii_lowercase();
        Ok(Self(SqlIdentifier::new(value)?))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn quoted(&self) -> String {
        self.0.quoted()
    }
}

impl fmt::Display for ServerName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OpaqueId(String);

impl OpaqueId {
    pub fn new(field: &'static str, value: impl Into<String>) -> Result<Self, ContractError> {
        let value = value.into();
        validate_text(field, &value, MAX_OPAQUE_ID_BYTES)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for OpaqueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Guid(String);

impl Guid {
    pub fn parse(field: &'static str, value: impl AsRef<str>) -> Result<Self, ContractError> {
        let value = value.as_ref();
        let valid = value.len() == 36
            && value.bytes().enumerate().all(|(index, byte)| match index {
                8 | 13 | 18 | 23 => byte == b'-',
                _ => byte.is_ascii_hexdigit(),
            });
        if !valid {
            return Err(ContractError::InvalidGuid { field });
        }

        let canonical = value.to_ascii_lowercase();
        if canonical.bytes().all(|byte| byte == b'0' || byte == b'-') {
            return Err(ContractError::NilGuid { field });
        }
        Ok(Self(canonical))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Guid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DecimalProgress(u128);

impl DecimalProgress {
    pub fn parse(value: &str) -> Result<Self, ContractError> {
        if value.is_empty() || value.len() > 25 || !value.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(ContractError::InvalidProgress);
        }
        let value = value
            .parse::<u128>()
            .map_err(|_| ContractError::InvalidProgress)?;
        Ok(Self(value))
    }

    pub fn value(self) -> u128 {
        self.0
    }
}

impl fmt::Display for DecimalProgress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Endpoint {
    host: String,
    port: u16,
}

impl Endpoint {
    pub fn new(host: impl Into<String>, port: u16) -> Result<Self, ContractError> {
        let mut host = host.into();
        if !is_dns_name(&host) {
            return Err(ContractError::InvalidEndpointHost);
        }
        if port == 0 {
            return Err(ContractError::InvalidPort);
        }
        host.make_ascii_lowercase();
        Ok(Self { host, port })
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tcp://{}:{}", self.host, self.port)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PinnedImage(String);

impl PinnedImage {
    pub fn new(value: impl Into<String>) -> Result<Self, ContractError> {
        let value = value.into();
        let Some((repository, digest)) = value.rsplit_once("@sha256:") else {
            return Err(ContractError::InvalidImageDigest);
        };
        if repository.is_empty()
            || repository.contains('@')
            || repository.chars().any(char::is_whitespace)
            || digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            || value.chars().any(char::is_control)
        {
            return Err(ContractError::InvalidImageDigest);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PinnedImage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SecretRef {
    name: String,
    key: String,
}

impl SecretRef {
    pub fn new(
        field: &'static str,
        name: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Self, ContractError> {
        let name = name.into();
        let key = key.into();
        if !is_dns_subdomain(&name)
            || key.is_empty()
            || key.len() > 253
            || !key
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
        {
            return Err(ContractError::InvalidSecretReference { field });
        }
        Ok(Self { name, key })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReplicaIdentity {
    logical_id: OpaqueId,
    native_replica_id: Option<Guid>,
    incarnation: OpaqueId,
}

impl ReplicaIdentity {
    pub fn desired(
        logical_id: impl Into<String>,
        incarnation: impl Into<String>,
    ) -> Result<Self, ContractError> {
        Ok(Self {
            logical_id: OpaqueId::new("logical replica ID", logical_id)?,
            native_replica_id: None,
            incarnation: OpaqueId::new("replica incarnation", incarnation)?,
        })
    }

    pub fn observed(
        logical_id: impl Into<String>,
        native_replica_id: Guid,
        incarnation: impl Into<String>,
    ) -> Result<Self, ContractError> {
        Ok(Self {
            logical_id: OpaqueId::new("logical replica ID", logical_id)?,
            native_replica_id: Some(native_replica_id),
            incarnation: OpaqueId::new("replica incarnation", incarnation)?,
        })
    }

    pub fn logical_id(&self) -> &str {
        self.logical_id.as_str()
    }

    pub fn native_replica_id(&self) -> Option<&Guid> {
        self.native_replica_id.as_ref()
    }

    pub fn incarnation(&self) -> &str {
        self.incarnation.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaDescriptor {
    pub identity: ReplicaIdentity,
    pub server_name: ServerName,
    pub endpoint: Endpoint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvailabilityGroupIdentity {
    pub name: AvailabilityGroupName,
    pub group_id: Guid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseIdentity {
    pub name: SqlIdentifier,
    pub group_database_id: Guid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseLineage {
    pub database: DatabaseIdentity,
    pub recovery_fork_id: Guid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeRole {
    Primary,
    Secondary,
    Resolving,
    NotJoined,
    Unknown(OpaqueId),
}

impl NativeRole {
    /// Classifies a role string reported by SQL Server.
    ///
    /// Unrecognized roles are retained rather than discarded so that an
    /// unsupported engine state stays distinguishable from a known role, but the
    /// server-supplied text is validated like any other external input.
    pub fn parse(value: &str) -> Result<Self, ContractError> {
        Ok(match value {
            "PRIMARY" => Self::Primary,
            "SECONDARY" => Self::Secondary,
            "RESOLVING" => Self::Resolving,
            "NOT_JOINED" => Self::NotJoined,
            other => Self::Unknown(OpaqueId::new("native replica role", other)?),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeProgress {
    pub hardened_block: Option<DecimalProgress>,
    pub redone_record: Option<DecimalProgress>,
    pub committed_record: Option<DecimalProgress>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservationFailureKind {
    Unreachable,
    PermissionDenied,
    TimedOut,
    Malformed,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservationFailure {
    pub kind: ObservationFailureKind,
    pub message: String,
    pub observed_at_unix_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Observation<T> {
    Present {
        value: T,
        observed_at_unix_millis: u64,
    },
    Absent {
        observed_at_unix_millis: u64,
    },
    Failed(ObservationFailure),
}

impl<T> Observation<T> {
    /// Returns when the observation attempt was made, including failed attempts.
    ///
    /// A failed attempt still carries a timestamp so that callers can reason
    /// about how long evidence has been unavailable, which lease and fencing
    /// decisions depend on.
    pub fn observed_at_unix_millis(&self) -> u64 {
        match self {
            Self::Present {
                observed_at_unix_millis,
                ..
            }
            | Self::Absent {
                observed_at_unix_millis,
            } => *observed_at_unix_millis,
            Self::Failed(failure) => failure.observed_at_unix_millis,
        }
    }

    /// Reports whether this observation is usable evidence at `now_unix_millis`.
    ///
    /// A failed observation is never fresh: it carries no evidence about the
    /// native state. An observation stamped in the future is also reported as
    /// not fresh, so clock skew degrades toward refusing to act rather than
    /// toward trusting an unverifiable sample.
    pub fn is_fresh_at(&self, now_unix_millis: u64, max_age_millis: u64) -> bool {
        match self {
            Self::Present {
                observed_at_unix_millis,
                ..
            }
            | Self::Absent {
                observed_at_unix_millis,
            } => now_unix_millis
                .checked_sub(*observed_at_unix_millis)
                .is_some_and(|age| age <= max_age_millis),
            Self::Failed(_) => false,
        }
    }
}

pub(crate) fn validate_text(
    field: &'static str,
    value: &str,
    max: usize,
) -> Result<(), ContractError> {
    if value.is_empty() {
        return Err(ContractError::MissingField { field });
    }
    if value.len() > max {
        return Err(ContractError::InvalidLength { field, max });
    }
    if value.chars().any(char::is_control) {
        return Err(ContractError::InvalidCharacter { field });
    }
    Ok(())
}

fn is_dns_name(value: &str) -> bool {
    if value.is_empty() || value.len() > 253 || value.ends_with('.') {
        return false;
    }
    value.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            && label
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphanumeric)
            && label
                .as_bytes()
                .last()
                .is_some_and(u8::is_ascii_alphanumeric)
    })
}

fn is_dns_subdomain(value: &str) -> bool {
    is_dns_name(value)
        && value
            .bytes()
            .all(|byte| !byte.is_ascii_alphabetic() || byte.is_ascii_lowercase())
}
