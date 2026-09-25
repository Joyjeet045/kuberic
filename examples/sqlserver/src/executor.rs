use std::collections::BTreeMap;

use async_trait::async_trait;

use crate::AvailabilityGroupName;
use crate::query::ReadQuery;
use crate::runtime_error::RuntimeError;

pub type QueryRow = BTreeMap<String, Option<String>>;

/// One connection is retained for a complete, bracketed observation attempt.
#[async_trait]
pub trait SqlSession: Send {
    async fn query(
        &mut self,
        query: ReadQuery,
        availability_group: &AvailabilityGroupName,
    ) -> Result<Vec<QueryRow>, RuntimeError>;
}

#[async_trait]
pub trait SqlExecutor: Send + Sync {
    async fn connect(&self) -> Result<Box<dyn SqlSession>, RuntimeError>;
}
