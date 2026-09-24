use kuberic_protocol::types::AccessStatus;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("runtime is closed")]
    Closed,
    #[error("runtime is not open")]
    NotOpen,
    #[error("runtime is not primary")]
    NotPrimary,
    #[error("operation was cancelled")]
    OperationCancelled,
    #[error("replica {0} was removed")]
    ReplicaRemoved(i64),
    #[error("runtime reconfiguration is still pending")]
    ReconfigurationPending,
    #[error("bounded operation queue is full")]
    QueueFull,
    #[error("writes are closed with status {0:?}")]
    WriteClosed(AccessStatus),
    #[error("reads are closed with status {0:?}")]
    ReadClosed(AccessStatus),
    #[error("durable authority has not been admitted")]
    AuthorityNotAdmitted,
    #[error("authority mismatch: {0}")]
    AuthorityMismatch(String),
    #[error("invalid replication evidence: {0}")]
    InvalidReplication(String),
    #[error("application error: {0}")]
    Application(String),
    #[error("control-plane error: {0}")]
    ControlPlane(String),
    #[error("effect sequence {observed} is out of order; expected {expected}")]
    EffectOutOfOrder { expected: u64, observed: u64 },
    #[error("effect sequence {sequence} was reused with a different operation ID")]
    EffectConflict { sequence: u64 },
    #[error("write completion channel closed")]
    WriteCompletionClosed,
    #[error("local write {0} must be resolved before another write can be assigned")]
    LocalWritePending(String),
    #[error("local write was fenced by data-loss recovery")]
    DataLossFenced,
}

pub type Result<T> = std::result::Result<T, RuntimeError>;

impl From<kuberic_runtime_internal::ContractError> for RuntimeError {
    fn from(error: kuberic_runtime_internal::ContractError) -> Self {
        match error {
            kuberic_runtime_internal::ContractError::AuthorityMismatch(message) => {
                Self::AuthorityMismatch(message)
            }
            kuberic_runtime_internal::ContractError::Persistence(message) => {
                Self::Application(message)
            }
        }
    }
}

impl From<kuberic_protocol::validation::ValidationError> for RuntimeError {
    fn from(error: kuberic_protocol::validation::ValidationError) -> Self {
        Self::AuthorityMismatch(error.to_string())
    }
}
