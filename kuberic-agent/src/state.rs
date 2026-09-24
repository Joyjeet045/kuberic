//! Durable replica-agent state.

use kuberic_protocol::command::EnsureConfiguration;
use kuberic_protocol::types::{
    AccessStatus, ConfigurationDescriptor, EffectivePolicy, Epoch, FaultType, InitializationId,
    LoadMetric, OperationId, PodUid, PvcUid, ReplicaIdentity, ReplicaRole, ResourceUid,
};
use kuberic_runtime_internal::effects::{RuntimeEffect, RuntimeEffectResult};
use serde::{Deserialize, Serialize};

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageIdentity {
    pub schema_version: u32,
    pub resource_uid: ResourceUid,
    pub pod_uid: PodUid,
    pub pvc_uid: PvcUid,
    pub initialization_id: InitializationId,
    pub local_identity: ReplicaIdentity,
    pub effective_policy: EffectivePolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeactivationState {
    pub epoch: Epoch,
    pub deactivated_lsn: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EffectStage {
    IntentCommitted,
    EffectApplied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingEffect {
    pub effect: RuntimeEffect,
    pub stage: EffectStage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetainedResult {
    pub operation_id: OperationId,
    pub effect: RuntimeEffect,
    pub result: RuntimeEffectResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CoordinatorStage {
    AdmitAuthority,
    FailoverPrefix,
    Demote,
    GetLsn,
    Catchup,
    Deactivate,
    ReplicatorRole,
    Epoch,
    ApplicationRole,
    Activate,
    RetireBuild,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReconfigurationRecord {
    pub command: EnsureConfiguration,
    pub stage: CoordinatorStage,
    pub observed_lsn: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetainedCommandResult {
    pub command: EnsureConfiguration,
    pub role: ReplicaRole,
    pub epoch: Epoch,
}

fn initial_effect_sequence() -> u64 {
    1
}

fn denied_access() -> AccessStatus {
    AccessStatus::NotPrimary
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentState {
    pub identity: StorageIdentity,
    pub highest_epoch: Epoch,
    pub previous_configuration: Option<ConfigurationDescriptor>,
    pub current_configuration: Option<ConfigurationDescriptor>,
    pub role: ReplicaRole,
    #[serde(default = "denied_access")]
    pub read_status: AccessStatus,
    pub write_status: AccessStatus,
    pub deactivation: Option<DeactivationState>,
    pub reconfiguration_data: Option<String>,
    #[serde(default)]
    pub reconfiguration: Option<ReconfigurationRecord>,
    #[serde(default)]
    pub retained_command: Option<RetainedCommandResult>,
    #[serde(default = "initial_effect_sequence")]
    pub next_effect_sequence: u64,
    #[serde(default)]
    pub load_metrics: Vec<LoadMetric>,
    #[serde(default)]
    pub reported_fault: Option<FaultType>,
    pub pending_effect: Option<PendingEffect>,
    pub retained_result: Option<RetainedResult>,
}

impl AgentState {
    pub fn new(identity: StorageIdentity) -> Self {
        Self {
            identity,
            highest_epoch: Epoch::default(),
            previous_configuration: None,
            current_configuration: None,
            role: ReplicaRole::None,
            read_status: AccessStatus::NotPrimary,
            write_status: AccessStatus::NotPrimary,
            deactivation: None,
            reconfiguration_data: None,
            reconfiguration: None,
            retained_command: None,
            next_effect_sequence: 1,
            load_metrics: Vec::new(),
            reported_fault: None,
            pending_effect: None,
            retained_result: None,
        }
    }
}
