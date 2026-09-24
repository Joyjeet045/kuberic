//! Unpublished contracts shared by the level-triggered runtime and replica agent.

pub mod authority;
pub mod effects;
pub mod transport;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ContractError {
    #[error("authority mismatch: {0}")]
    AuthorityMismatch(String),
    #[error("persistence error: {0}")]
    Persistence(String),
}

pub type Result<T> = std::result::Result<T, ContractError>;

#[derive(Debug, Clone, Copy)]
pub struct RuntimeHostToken {
    _private: (),
}

#[allow(clippy::new_without_default)]
impl RuntimeHostToken {
    pub fn new() -> Self {
        Self { _private: () }
    }
}
