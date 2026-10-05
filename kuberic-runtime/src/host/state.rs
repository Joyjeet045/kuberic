//! Durable replica-agent state.

use crate::authority::{DurableBuildProgress, RetiredAuthority};
use crate::effects::{RuntimeEffect, RuntimeEffectResult};
use crate::protocol::command::EnsureConfiguration;
use crate::protocol::command::EnsureReplicaBuild;
use crate::protocol::types::{
    AccessStatus, ConfigurationDescriptor, ConfigurationId, EffectivePolicy, Epoch, FaultType,
    InitializationId, LoadMetric, OperationId, PodUid, ProvisioningIntent, PvcUid, ReplicaIdentity,
    ReplicaRole, ResourceUid, ScaleUpConfigurationEvidence, SecondaryRemovalEvidence,
    SecondaryRemovalPreparation, SecondaryScaleDownCleanup, SwitchoverHandoff,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

// Fresh schema-5 stores bind application paths and initialization permission.
pub(crate) const SCHEMA_VERSION: u32 = 5;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StorageIdentity {
    pub(crate) schema_version: u32,
    pub(crate) resource_uid: ResourceUid,
    pub(crate) pod_uid: PodUid,
    pub(crate) pvc_uid: PvcUid,
    pub(crate) initialization_id: InitializationId,
    pub(crate) local_identity: ReplicaIdentity,
    pub(crate) effective_policy: EffectivePolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeactivationState {
    pub(crate) epoch: Epoch,
    pub(crate) deactivated_lsn: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum EffectStage {
    IntentCommitted,
    EffectApplied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PendingEffect {
    pub(crate) effect: RuntimeEffect,
    pub(crate) stage: EffectStage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RetainedResult {
    pub(crate) operation_id: OperationId,
    pub(crate) effect: RuntimeEffect,
    pub(crate) result: RuntimeEffectResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum CoordinatorStage {
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
pub(crate) struct ReconfigurationRecord {
    pub(crate) command: EnsureConfiguration,
    pub(crate) stage: CoordinatorStage,
    pub(crate) observed_lsn: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RetainedCommandResult {
    pub(crate) command: EnsureConfiguration,
    pub(crate) role: ReplicaRole,
    pub(crate) epoch: Epoch,
}

fn initial_effect_sequence() -> u64 {
    1
}

fn denied_access() -> AccessStatus {
    AccessStatus::NotPrimary
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PreparationRetirement {
    pub(crate) starting_configuration_id: ConfigurationId,
    pub(crate) starting_epoch: Epoch,
    pub(crate) generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ApplicationStorageBinding {
    pub(crate) paths: BTreeMap<String, PathBuf>,
    pub(crate) initializing: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AgentState {
    pub(crate) identity: StorageIdentity,
    #[serde(default)]
    pub(crate) application_storage: Option<ApplicationStorageBinding>,
    #[serde(default)]
    pub(crate) scale_up_initialization: Option<ProvisioningIntent>,
    pub(crate) admitted_policy: Option<EffectivePolicy>,
    pub(crate) previous_policy: Option<EffectivePolicy>,
    pub(crate) prepared_secondary_removal: Option<SecondaryRemovalPreparation>,
    pub(crate) secondary_removal_evidence: Option<SecondaryRemovalEvidence>,
    pub(crate) accepted_secondary_removal: Option<SecondaryScaleDownCleanup>,
    pub(crate) retired_authority: Option<RetiredAuthority>,
    pub(crate) removal_effects: BTreeMap<OperationId, RetainedResult>,
    pub(crate) removal_commands: BTreeMap<OperationId, RetainedCommandResult>,
    pub(crate) highest_epoch: Epoch,
    pub(crate) previous_configuration: Option<ConfigurationDescriptor>,
    pub(crate) current_configuration: Option<ConfigurationDescriptor>,
    pub(crate) role: ReplicaRole,
    #[serde(default = "denied_access")]
    pub(crate) read_status: AccessStatus,
    pub(crate) write_status: AccessStatus,
    pub(crate) deactivation: Option<DeactivationState>,
    pub(crate) reconfiguration_data: Option<String>,
    #[serde(default)]
    pub(crate) reconfiguration: Option<ReconfigurationRecord>,
    #[serde(default)]
    pub(crate) retained_command: Option<RetainedCommandResult>,
    #[serde(default)]
    pub(crate) completed_scale_up: Option<Box<RetainedCommandResult>>,
    #[serde(default)]
    pub(crate) scale_up_evidence: Option<Box<ScaleUpConfigurationEvidence>>,
    #[serde(default)]
    pub(crate) build_commands: BTreeMap<OperationId, EnsureReplicaBuild>,
    #[serde(default)]
    pub(crate) build_progress: BTreeMap<OperationId, DurableBuildProgress>,
    #[serde(default)]
    pub(crate) retired_builds: BTreeSet<OperationId>,
    #[serde(default)]
    pub(crate) abandoned_builds: BTreeSet<OperationId>,
    #[serde(default = "initial_effect_sequence")]
    pub(crate) next_effect_sequence: u64,
    #[serde(default)]
    pub(crate) load_metrics: Vec<LoadMetric>,
    #[serde(default)]
    pub(crate) reported_fault: Option<FaultType>,
    pub(crate) pending_effect: Option<PendingEffect>,
    pub(crate) retained_result: Option<RetainedResult>,
    #[serde(default)]
    pub(crate) prepared_switchover: Option<SwitchoverHandoff>,
    #[serde(default)]
    pub(crate) retired_switchover: Option<SwitchoverHandoff>,
    #[serde(default)]
    pub(crate) preparation_retirement: Option<PreparationRetirement>,
}

impl AgentState {
    pub(crate) fn new(identity: StorageIdentity) -> Self {
        Self {
            identity,
            application_storage: None,
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
            || self.pending_effect.as_ref().is_some_and(|p| {
                matches!(
                    p.effect.action,
                    crate::effects::RuntimeEffectAction::PrepareSecondaryRemoval { .. }
                        | crate::effects::RuntimeEffectAction::RetireReplica(_)
                )
            })
            || self.secondary_removal_evidence.as_ref().is_some_and(|e| {
                self.accepted_secondary_removal
                    .as_ref()
                    .is_none_or(|c| &c.evidence != e)
            })
    }
}
