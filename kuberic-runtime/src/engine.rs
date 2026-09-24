//! Non-COM persistence integration for the default replication engine.
//!
//! These are storage/recovery hooks, not additions to IFabricStateProvider.
//! Secondary delivery is through service-owned, explicitly acknowledged streams.

use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use kuberic_protocol::types::OperationId;

use crate::Result;
use crate::application::{
    CopyChunk, DurableApplicationAck, DurableApplicationProgress, Lsn, Operation,
};

pub type RetainedOperationStream = Pin<Box<dyn Stream<Item = Result<Operation>> + Send>>;

#[async_trait]
pub trait DurableState: Send + Sync {
    async fn get_replication_operations(
        &self,
        from_lsn: Lsn,
        to_lsn: Lsn,
    ) -> Result<RetainedOperationStream>;
    /// Returns only after the chunk and required directory entries are durable.
    /// An exact retry must verify identical bytes for the same build sequence.
    async fn apply_copy_chunk(
        &self,
        build_id: &OperationId,
        sequence: u64,
        chunk: CopyChunk,
    ) -> Result<()>;
    async fn verify_copy_chunk(
        &self,
        build_id: &OperationId,
        sequence: u64,
        chunk: &CopyChunk,
    ) -> Result<bool>;
    async fn finish_copy(
        &self,
        build_id: &OperationId,
        up_to_lsn: Lsn,
        committed_lsn: Lsn,
    ) -> Result<DurableApplicationProgress>;
    async fn apply(&self, operation: Operation) -> Result<DurableApplicationAck>;
    async fn durable_progress(&self) -> Result<DurableApplicationProgress>;
    async fn verify_applied(&self, operation: &Operation) -> Result<bool>;
    async fn commit(&self, committed_lsn: Lsn) -> Result<DurableApplicationProgress>;
}
