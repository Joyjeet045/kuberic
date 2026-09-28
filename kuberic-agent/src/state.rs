//! Durable replica-agent state.

use kuberic_protocol::command::EnsureConfiguration;
use kuberic_protocol::command::EnsureReplicaBuild;
use kuberic_protocol::types::{
    AccessStatus, ConfigurationDescriptor, ConfigurationId, EffectivePolicy, Epoch, FaultType,
    InitializationId, LoadMetric, OperationId, PodUid, ProvisioningIntent, PvcUid, ReplicaIdentity,
    ReplicaRole, ResourceUid, ScaleUpConfigurationEvidence, SecondaryRemovalEvidence,
    SecondaryRemovalPreparation, SecondaryScaleDownCleanup, SwitchoverHandoff,
};
use kuberic_runtime_internal::authority::{DurableBuildProgress, RetiredAuthority};
use kuberic_runtime_internal::effects::{RuntimeEffect, RuntimeEffectResult};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const SCHEMA_VERSION: u32 = 3;

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
pub struct PreparationRetirement {
    pub starting_configuration_id: ConfigurationId,
    pub starting_epoch: Epoch,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentState {
    pub identity: StorageIdentity,
    #[serde(default)]
    pub scale_up_initialization: Option<ProvisioningIntent>,
    pub admitted_policy: Option<EffectivePolicy>,
    pub previous_policy: Option<EffectivePolicy>,
    pub prepared_secondary_removal: Option<SecondaryRemovalPreparation>,
    pub secondary_removal_evidence: Option<SecondaryRemovalEvidence>,
    pub accepted_secondary_removal: Option<SecondaryScaleDownCleanup>,
    pub retired_authority: Option<RetiredAuthority>,
    pub removal_effects: BTreeMap<OperationId, RetainedResult>,
    pub removal_commands: BTreeMap<OperationId, RetainedCommandResult>,
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
    #[serde(default)]
    pub completed_scale_up: Option<Box<RetainedCommandResult>>,
    #[serde(default)]
    pub scale_up_evidence: Option<Box<ScaleUpConfigurationEvidence>>,
    #[serde(default)]
    pub build_commands: BTreeMap<OperationId, EnsureReplicaBuild>,
    #[serde(default)]
    pub build_progress: BTreeMap<OperationId, DurableBuildProgress>,
    #[serde(default)]
    pub retired_builds: BTreeSet<OperationId>,
    #[serde(default)]
    pub abandoned_builds: BTreeSet<OperationId>,
    #[serde(default = "initial_effect_sequence")]
    pub next_effect_sequence: u64,
    #[serde(default)]
    pub load_metrics: Vec<LoadMetric>,
    #[serde(default)]
    pub reported_fault: Option<FaultType>,
    pub pending_effect: Option<PendingEffect>,
    pub retained_result: Option<RetainedResult>,
    #[serde(default)]
    pub prepared_switchover: Option<SwitchoverHandoff>,
    #[serde(default)]
    pub retired_switchover: Option<SwitchoverHandoff>,
    #[serde(default)]
    pub preparation_retirement: Option<PreparationRetirement>,
}

impl AgentState {
    pub fn new(identity: StorageIdentity) -> Self {
        Self {
            identity,
            scale_up_initialization: None,
            admitted_policy: None,
            previous_policy: None,
            prepared_secondary_removal: None,
            secondary_removal_evidence: None,
            accepted_secondary_removal: None,
            retired_authority: None,
            removal_effects: BTreeMap::new(),
            removal_commands: BTreeMap::new(),
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
            completed_scale_up: None,
            scale_up_evidence: None,
            build_commands: BTreeMap::new(),
            build_progress: BTreeMap::new(),
            retired_builds: BTreeSet::new(),
            abandoned_builds: BTreeSet::new(),
            next_effect_sequence: 1,
            load_metrics: Vec::new(),
            reported_fault: None,
            pending_effect: None,
            retained_result: None,
            prepared_switchover: None,
            retired_switchover: None,
            preparation_retirement: None,
        }
    }

    pub(crate) fn removal_pending(&self) -> bool {
        self.prepared_secondary_removal.is_some()
            || self.pending_effect.as_ref().is_some_and(|p| matches!(
                p.effect.action,
                kuberic_runtime_internal::effects::RuntimeEffectAction::PrepareSecondaryRemoval { .. }
                    | kuberic_runtime_internal::effects::RuntimeEffectAction::RetireReplica(_)
            ))
            || self.secondary_removal_evidence.as_ref().is_some_and(|e| {
                self.accepted_secondary_removal
                    .as_ref()
                    .is_none_or(|c| &c.evidence != e)
            })
    }
}
