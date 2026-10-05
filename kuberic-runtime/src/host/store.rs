//! Narrow agent store operations.

use crate::effects::{RuntimeEffect, RuntimeEffectResult};
use crate::protocol::command::{EnsureConfiguration, EnsureReplicaBuild};
use crate::protocol::types::{FaultType, LoadMetric, OperationId};
use async_trait::async_trait;

use crate::host::Result;
#[cfg(test)]
use crate::host::state::RetainedResult;
use crate::host::state::{
    AgentState, CoordinatorStage, ReconfigurationRecord, RetainedCommandResult, StorageIdentity,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BeginEffect {
    Execute(RuntimeEffect),
    Pending(RuntimeEffect),
    Completed(Box<RuntimeEffectResult>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BeginConfiguration {
    Execute(ReconfigurationRecord),
    Pending(ReconfigurationRecord),
    Superseded(ReconfigurationRecord),
    Completed(RetainedCommandResult),
}

#[async_trait]
pub(crate) trait AgentStore: Send + Sync {
    async fn identity(&self) -> Result<StorageIdentity>;

    async fn load_state(&self) -> Result<AgentState>;

    async fn complete_application_initialization(&self) -> Result<()>;

    async fn begin_effect(&self, effect: &RuntimeEffect) -> Result<BeginEffect>;

    async fn mark_effect_applied(&self, effect: &RuntimeEffect) -> Result<()>;

    async fn complete_effect(&self, result: &RuntimeEffectResult) -> Result<()>;

    async fn cancel_effect(&self, effect: &RuntimeEffect) -> Result<()>;

    async fn begin_configuration(
        &self,
        command: &EnsureConfiguration,
    ) -> Result<BeginConfiguration>;

    async fn journal_build(&self, command: &EnsureReplicaBuild) -> Result<EnsureReplicaBuild>;

    async fn abandon_build(&self, command: &EnsureReplicaBuild) -> Result<()>;

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

    #[cfg(test)]
    async fn retained_result(&self) -> Result<Option<RetainedResult>>;

    #[cfg(test)]
    async fn set_reconfiguration(&self, data: Option<String>) -> Result<()>;

    #[cfg(test)]
    async fn clear_reconfiguration(&self) -> Result<()>;

    #[cfg(test)]
    async fn migrate_schema(&self, expected_version: u32, target_version: u32) -> Result<()>;

    async fn record_partition_reports(
        &self,
        load_metrics: Vec<LoadMetric>,
        reported_fault: Option<FaultType>,
    ) -> Result<()>;
}
