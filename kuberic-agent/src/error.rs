use thiserror::Error;

#[derive(Debug, Error)]
pub enum AgentError {
    #[error("agent store is uninitialized")]
    Uninitialized,
    #[error("established agent metadata is missing")]
    MissingEstablishedStore,
    #[error("agent store identity mismatch: {0}")]
    IdentityMismatch(String),
    #[error("agent store schema {observed} is incompatible; expected {expected}")]
    SchemaMismatch { expected: u32, observed: u32 },
    #[error("agent store is corrupt: {0}")]
    Corrupt(String),
    #[error("initialization is not authorized: {0}")]
    InitializationNotAuthorized(String),
    #[error("durable effect conflict: {0}")]
    EffectConflict(String),
    #[error("command rejected: {0}")]
    CommandRejected(String),
    #[error("transport session rejected: {0}")]
    SessionRejected(String),
    #[error("transport backpressure: {0}")]
    Backpressure(String),
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("runtime error: {0}")]
    Runtime(#[from] kuberic_runtime::RuntimeError),
    #[error("internal contract error: {0}")]
    Contract(#[from] kuberic_runtime_internal::ContractError),
}

pub type Result<T> = std::result::Result<T, AgentError>;
