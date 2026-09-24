//! Narrow agent store operations.

use async_trait::async_trait;
use kuberic_protocol::command::EnsureConfiguration;
use kuberic_protocol::types::{FaultType, LoadMetric, OperationId};
use kuberic_runtime_internal::effects::{RuntimeEffect, RuntimeEffectResult};

use crate::Result;
use crate::state::{
    AgentState, CoordinatorStage, ReconfigurationRecord, RetainedCommandResult, RetainedResult,
    StorageIdentity,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BeginEffect {
    Execute(RuntimeEffect),
    Pending(RuntimeEffect),
    Completed(Box<RuntimeEffectResult>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BeginConfiguration {
    Execute(ReconfigurationRecord),
    Pending(ReconfigurationRecord),
    Superseded(ReconfigurationRecord),
    Completed(RetainedCommandResult),
}

#[async_trait]
pub trait AgentStore: Send + Sync {
    async fn identity(&self) -> Result<StorageIdentity>;

    async fn load_state(&self) -> Result<AgentState>;

    async fn begin_effect(&self, effect: &RuntimeEffect) -> Result<BeginEffect>;

    async fn mark_effect_applied(&self, effect: &RuntimeEffect) -> Result<()>;

    async fn complete_effect(&self, result: &RuntimeEffectResult) -> Result<()>;

    async fn cancel_effect(&self, effect: &RuntimeEffect) -> Result<()>;

    async fn begin_configuration(
        &self,
        command: &EnsureConfiguration,
    ) -> Result<BeginConfiguration>;

    async fn advance_configuration(
        &self,
        operation_id: &OperationId,
        expected: CoordinatorStage,
        next: CoordinatorStage,
        observed_lsn: Option<i64>,
    ) -> Result<ReconfigurationRecord>;

    async fn complete_configuration(
        &self,
        operation_id: &OperationId,
    ) -> Result<RetainedCommandResult>;

    async fn retained_result(&self) -> Result<Option<RetainedResult>>;

    async fn set_reconfiguration(&self, data: Option<String>) -> Result<()>;

    async fn clear_reconfiguration(&self) -> Result<()>;

    async fn migrate_schema(&self, expected_version: u32, target_version: u32) -> Result<()>;

    async fn record_partition_reports(
        &self,
        load_metrics: Vec<LoadMetric>,
        reported_fault: Option<FaultType>,
    ) -> Result<()>;
}
