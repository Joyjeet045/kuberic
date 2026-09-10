//! Compact versioned durable contract for framework-native replica removal.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::ResourceExt;
use kuberic_core::driver::ReplicaHandle;
use kuberic_core::error::KubericError;
use kuberic_core::remove_replica::{
    RemoveReplicaCoordinatorPhase, RemoveReplicaProgress, RemoveReplicaTerminalResult,
    TargetRetirementObservation,
};
use kuberic_core::types::{
    AccessStatus, AgentControlVersion, AgentGeneration, CorrelatedActionObservation,
    DurableActionErrorClass, DurableActionObservation, DurableActionResult, DurableActionState,
    DurableReplicaAction, Epoch, ReplicaAgentStatus, ReplicaConfigurationMemberStatus,
    ReplicaConfigurationMode, ReplicaConfigurationStatus, ReplicaConnectionStatus,
    ReplicaInstanceId, ReplicaStatusInfo, Role,
};
use kuberic_durable_execution::{
    ActivityName, ActivityObservation, ActivityOptions, ActivityRecord, ActivitySequence,
    ActivitySpec, ActivityState, CheckpointEnvelope, CheckpointError, CheckpointLimits,
    CheckpointPayload, DurableActivity, ExactBytes, ExecutionContract, ExecutionId, ExecutionSpec,
    InMemoryCheckpointStore, KubernetesCheckpointOwner, KubernetesCheckpointOwnerScope,
    KubernetesCheckpointStore, KubernetesCheckpointStoreOptions, LogicalActivityId,
    PreparedActivityError, PreparedActivityResolver, TerminalOutcome, Workflow, WorkflowContext,
    decode_activity_input, decode_activity_result, encode_activity_input, encode_activity_result,
};
use rand::random;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::cluster_api::ClusterApi;
use crate::crd::{
    DurableOperationPhase, DurableOperationStatus, DurableRemoveMode, EpochStatus, KubericSet,
    RemoveReplicaAdmissionInputStatus, RemoveReplicaAdmissionTargetStatus,
    RemoveReplicaCleanupStatus, RemoveReplicaCommitEvidenceStatus,
    RemoveReplicaCoordinatorPhaseStatus, RemoveReplicaDispositionStatus,
    RemoveReplicaExecutionStatus, RemoveReplicaTerminalResultStatus, StablePartitionSnapshotStatus,
    TargetRetirementObservationStatus,
};

use super::checkpoint_store::{
    CheckpointMeasurementDecoder, DurableActivityAccounting, DurableActivityClass,
};
use super::effects::{
    DeleteEffectCommand, DispatchFailureDisposition, DurableEffectOutcome,
    DurableEffectPreparationError, LabelEffectCommand, ReplicaEffectCommand,
    classify_dispatch_failure, clear_dispatch_evidence, dispatch_rejection_requires_refresh,
    execute_delete_command, execute_label_command, execute_replica_command,
    generation_change_proves_no_admission, prepare_lifecycle_replica_effect_command,
    prepare_remove_delete_effect_command, prepare_remove_label_effect_command,
    remove_delete_postcondition_satisfied, remove_label_postcondition_satisfied,
    validate_remove_replica_action_kind, validate_remove_replica_dispatch_authority,
};
use super::remove_replica::{core_intent, validate_remove_replica_operation};
use super::runner::{
    DurableAdapterBoundary, DurableAdapterWait, DurableCheckpointDisposition,
    DurableOperationAdapter,
};
use super::workflow_host::{DurableOperatorHost, DurablePermitGuard, DurableWorkflowRuntime};
use super::{
    Decision, OperationObservations, OperationPodIdentities, RemoveReplicaTarget,
    ReplicaObservation, correlated_action_observation, decide_remove_replica,
    record_activity_error, start_remove_replica,
};

pub const REMOVE_REPLICA_CONTRACT_VERSION: u32 = 3;
pub const REMOVE_REPLICA_MAX_MEMBERS: usize = 3;
pub const REMOVE_REPLICA_MAX_ACTIVITY_RECORDS: usize = 16;
pub const REMOVE_REPLICA_MAX_WORKFLOW_INPUT_BYTES: usize = 2_048;
pub const REMOVE_REPLICA_MAX_BOUNDARY_INPUT_BYTES: u64 = 4_096;
pub const REMOVE_REPLICA_MAX_BOUNDARY_RESULT_BYTES: u64 = 2_048;
pub const REMOVE_REPLICA_MAX_ACTIVE_ENCODED_BYTES: usize = 262_144;
pub const REMOVE_REPLICA_MAX_TERMINAL_ENCODED_BYTES: usize = 12_288;
pub const REMOVE_REPLICA_MAX_TERMINAL_PAYLOAD_BYTES: u64 = 4_096;
pub const REMOVE_REPLICA_MAX_TRANSITION_FUEL: usize = 48;

const REMOVE_REPLICA_ACTIVITY_NAME: &str = "kuberic.remove-replica.native-boundary";
const REMOVE_REPLICA_ACTIVITY_VERSION: u32 = 2;
const MAX_TERMINAL_MESSAGE_BYTES: usize = 512;
const PROJECTED_FAULT_HISTORY_RECORDS: usize = 16;

pub type RemoveReplicaExecution = RemoveReplicaExecutionStatus;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoveReplicaWorkflowInput {
    pub contract_version: u32,
    pub execution_id: String,
    pub admission: RemoveReplicaAdmissionInputStatus,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "k", deny_unknown_fields)]
pub enum RemoveReplicaBoundaryInput {
    #[serde(rename = "o")]
    Observe {
        #[serde(rename = "p")]
        phase: DurableOperationPhase,
        #[serde(rename = "a")]
        attempt: u32,
    },
    #[serde(rename = "r")]
    ReplicaCommand {
        #[serde(rename = "c")]
        command: CompactReplicaEffectCommand,
    },
    #[serde(rename = "l")]
    LabelCommand {
        #[serde(rename = "c")]
        command: LabelEffectCommand,
    },
    #[serde(rename = "d")]
    DeleteCommand {
        #[serde(rename = "c")]
        command: DeleteEffectCommand,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactReplicaEffectCommand {
    #[serde(rename = "g")]
    pub expected_agent_generation: String,
    #[serde(rename = "c")]
    pub expected_control_version: u64,
    #[serde(rename = "r")]
    pub observed_runtime_epoch: [i64; 2],
    #[serde(rename = "d")]
    pub action_payload: ExactBytes,
}

impl From<ReplicaEffectCommand> for CompactReplicaEffectCommand {
    fn from(command: ReplicaEffectCommand) -> Self {
        Self {
            expected_agent_generation: command.expected_agent_generation,
            expected_control_version: command.expected_control_version,
            observed_runtime_epoch: [
                command.observed_runtime_epoch.data_loss_number,
                command.observed_runtime_epoch.configuration_number,
            ],
            action_payload: compact_action_payload(&command.action_payload),
        }
    }
}

impl CompactReplicaEffectCommand {
    pub fn effect_command(&self) -> Result<ReplicaEffectCommand, String> {
        let action_payload = expand_action_payload(&self.action_payload);
        let action = kuberic_core::grpc::convert::decode_correlated_action_payload(&action_payload)
            .map_err(|error| format!("decode compact native remove command: {error}"))?;
        let DurableReplicaAction::RemoveReplicaIntent { intent } = &action else {
            return Err("compact native remove command contains another action".to_string());
        };
        Ok(ReplicaEffectCommand {
            action_id: intent.action_id.clone(),
            action_signature: action.signature(),
            target_id: intent.primary_replica_id,
            target_instance_id: intent.primary_instance_id.to_string(),
            expected_epoch: EpochStatus {
                data_loss_number: intent.epoch.data_loss_number,
                configuration_number: intent.epoch.configuration_number,
            },
            desired_postcondition: crate::crd::DurablePostconditionStatus {
                kind: crate::crd::DurablePostconditionKind::RemoveReplicaCoordinated,
                role: None,
            },
            expected_agent_generation: self.expected_agent_generation.clone(),
            expected_control_version: self.expected_control_version,
            observed_runtime_epoch: EpochStatus {
                data_loss_number: self.observed_runtime_epoch[0],
                configuration_number: self.observed_runtime_epoch[1],
            },
            action_payload,
        })
    }
}

fn compact_action_payload(encoded: &str) -> ExactBytes {
    let bytes: Vec<u8> = encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).expect("generated action payload is ASCII");
            u8::from_str_radix(pair, 16).expect("generated action payload is hexadecimal")
        })
        .collect();
    ExactBytes::new(bytes)
}

fn expand_action_payload(encoded: &ExactBytes) -> String {
    encoded
        .as_slice()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

impl RemoveReplicaBoundaryInput {
    fn class(&self) -> DurableActivityClass {
        match self {
            Self::Observe { .. } => DurableActivityClass::PassiveObservation,
            Self::ReplicaCommand { .. }
            | Self::LabelCommand { .. }
            | Self::DeleteCommand { .. } => DurableActivityClass::ExternalEffect,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum RemoveReplicaBoundaryResult {
    Observation {
        evidence: RemoveReplicaObservationEvidence,
    },
    Effect {
        outcome: DurableEffectOutcome<RemoveReplicaObservationEvidence>,
    },
    ProvenNoAdmission {
        action_id: String,
        redelivery: u8,
        evidence: RemoveReplicaObservationEvidence,
    },
}

pub struct RemoveReplicaBoundary;

impl DurableActivity for RemoveReplicaBoundary {
    type Input = RemoveReplicaBoundaryInput;
    type Output = RemoveReplicaBoundaryResult;

    const NAME: &'static str = REMOVE_REPLICA_ACTIVITY_NAME;
    const VERSION: u32 = REMOVE_REPLICA_ACTIVITY_VERSION;
    const MAX_INPUT_BYTES: u64 = REMOVE_REPLICA_MAX_BOUNDARY_INPUT_BYTES;
    const MAX_RESULT_BYTES: u64 = REMOVE_REPLICA_MAX_BOUNDARY_RESULT_BYTES;
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoveReplicaActivityAccounting {
    pub external_effect_count: u64,
    pub passive_observation_count: u64,
}

impl RemoveReplicaActivityAccounting {
    fn record(&mut self, class: DurableActivityClass) -> Result<(), String> {
        match class {
            DurableActivityClass::ExternalEffect => {
                self.external_effect_count = self
                    .external_effect_count
                    .checked_add(1)
                    .ok_or_else(|| "remove activity accounting overflowed".to_string())?;
            }
            DurableActivityClass::PassiveObservation => {
                self.passive_observation_count = self
                    .passive_observation_count
                    .checked_add(1)
                    .ok_or_else(|| "remove activity accounting overflowed".to_string())?;
            }
        }
        Ok(())
    }

    fn total(self) -> Option<u64> {
        self.external_effect_count
            .checked_add(self.passive_observation_count)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum RemoveReplicaTerminal {
    Completed {
        commit_evidence: RemoveReplicaCommitEvidenceStatus,
        cleanup: RemoveReplicaCleanupStatus,
        accounting: RemoveReplicaActivityAccounting,
    },
    Compensated {
        message: String,
        accounting: RemoveReplicaActivityAccounting,
    },
    Unsafe {
        committed: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        commit_evidence: Option<RemoveReplicaCommitEvidenceStatus>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cleanup: Option<RemoveReplicaCleanupStatus>,
        disposition: RemoveReplicaDispositionStatus,
        message: String,
        accounting: RemoveReplicaActivityAccounting,
    },
    Rejected {
        message: String,
        accounting: RemoveReplicaActivityAccounting,
    },
    IncompatibleContract {
        source: String,
        found_version: u32,
        supported_version: u32,
        message: String,
        accounting: RemoveReplicaActivityAccounting,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoveReplicaObservationEvidence {
    #[serde(rename = "t")]
    pub observed_unix_seconds: i64,
    #[serde(rename = "r", default, skip_serializing_if = "Vec::is_empty")]
    pub replicas: Vec<RemoveReplicaReplicaEvidence>,
    #[serde(rename = "p", default, skip_serializing_if = "Vec::is_empty")]
    pub pods: Vec<RemoveReplicaPodEvidence>,
    #[serde(rename = "l", skip_serializing_if = "Option::is_none")]
    pub target_role_label: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoveReplicaPodEvidence {
    #[serde(rename = "i")]
    pub replica_id: i64,
    #[serde(rename = "u")]
    pub uid: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoveReplicaReplicaEvidence {
    #[serde(rename = "i")]
    pub replica_id: i64,
    #[serde(rename = "n")]
    pub instance_id: String,
    #[serde(rename = "r")]
    pub role: RemoveReplicaRoleEvidence,
    #[serde(rename = "e")]
    pub epoch: EpochStatus,
    #[serde(rename = "p")]
    pub current_progress: i64,
    #[serde(rename = "c", skip_serializing_if = "Option::is_none")]
    pub catch_up_capability: Option<i64>,
    #[serde(rename = "h")]
    pub healthy: bool,
    #[serde(rename = "w")]
    pub write_status: RemoveReplicaWriteStatusEvidence,
    #[serde(rename = "f", skip_serializing_if = "Option::is_none")]
    pub configuration: Option<RemoveReplicaConfigurationEvidence>,
    #[serde(rename = "x", default, skip_serializing_if = "Vec::is_empty")]
    pub active_connections: Vec<RemoveReplicaConnectionEvidence>,
    #[serde(rename = "a")]
    pub agent: RemoveReplicaAgentEvidence,
    #[serde(rename = "o")]
    pub control_address: String,
    #[serde(rename = "d")]
    pub replicator_address: String,
    #[serde(rename = "m")]
    pub pod_name: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoveReplicaRoleEvidence {
    Primary,
    ActiveSecondary,
    None,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoveReplicaWriteStatusEvidence {
    Granted,
    ReconfigurationPending,
    NotPrimary,
    NoWriteQuorum,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoveReplicaConfigurationEvidence {
    #[serde(rename = "o")]
    pub mode: RemoveReplicaConfigurationModeEvidence,
    #[serde(rename = "m")]
    pub members: Vec<RemoveReplicaConfigurationMemberEvidence>,
    #[serde(rename = "q")]
    pub write_quorum: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoveReplicaConfigurationModeEvidence {
    CatchUp,
    Current,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoveReplicaConfigurationMemberEvidence {
    #[serde(rename = "i")]
    pub replica_id: i64,
    #[serde(rename = "n")]
    pub instance_id: String,
    #[serde(rename = "r")]
    pub role: RemoveReplicaRoleEvidence,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoveReplicaConnectionEvidence {
    #[serde(rename = "i")]
    pub replica_id: i64,
    #[serde(rename = "n")]
    pub instance_id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoveReplicaAgentEvidence {
    #[serde(rename = "p")]
    pub protocol_version: u32,
    #[serde(rename = "l")]
    pub lifecycle_peer_protocol_version: u32,
    #[serde(rename = "g")]
    pub generation: String,
    #[serde(rename = "v")]
    pub control_version: u64,
    #[serde(rename = "a", skip_serializing_if = "Option::is_none")]
    pub action: Option<RemoveReplicaActionEvidence>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoveReplicaActionEvidence {
    #[serde(rename = "t")]
    pub state: RemoveReplicaActionStateEvidence,
    #[serde(rename = "c", skip_serializing_if = "Option::is_none")]
    pub error_class: Option<RemoveReplicaActionErrorClassEvidence>,
    #[serde(rename = "e", skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(rename = "r", skip_serializing_if = "Option::is_none")]
    pub result: Option<RemoveReplicaTerminalResultStatus>,
    #[serde(rename = "p", skip_serializing_if = "Option::is_none")]
    pub progress: Option<RemoveReplicaProgressEvidence>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoveReplicaActionStateEvidence {
    Scheduled,
    InProgress,
    Completed,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoveReplicaActionErrorClassEvidence {
    Internal,
    NotPrimary,
    NoWriteQuorum,
    ReconfigurationPending,
    StaleEpoch,
    Cancelled,
    Closed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoveReplicaProgressEvidence {
    #[serde(rename = "p")]
    pub phase: RemoveReplicaCoordinatorPhaseStatus,
    #[serde(rename = "c")]
    pub commit_observed: bool,
    #[serde(rename = "t", skip_serializing_if = "Option::is_none")]
    pub commit_observed_unix_seconds: Option<i64>,
    #[serde(rename = "x")]
    pub connection_absent: bool,
    #[serde(rename = "r")]
    pub target_retirement: TargetRetirementObservationStatus,
    #[serde(rename = "e", skip_serializing_if = "Option::is_none")]
    pub retirement_expiry_unix_seconds: Option<i64>,
    #[serde(rename = "m", skip_serializing_if = "Option::is_none")]
    pub compensation_expiry_unix_seconds: Option<i64>,
    #[serde(rename = "z", skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(rename = "d")]
    pub current_install_dispatched: bool,
}

impl RemoveReplicaObservationEvidence {
    pub fn capture(
        operation: &DurableOperationStatus,
        observations: &OperationObservations,
        pods: &OperationPodIdentities,
        target_role_label: Option<&str>,
        observed_unix_seconds: i64,
    ) -> Result<Self, String> {
        let action_id = operation
            .pending_action
            .as_ref()
            .map(|pending| pending.action_id.as_str())
            .or_else(|| {
                operation
                    .remove_intent
                    .as_ref()
                    .map(|intent| intent.action_id.as_str())
            });
        let replicas = observations
            .iter()
            .map(|(replica_id, observation)| {
                RemoveReplicaReplicaEvidence::capture(*replica_id, observation, action_id)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let pods = pods
            .iter()
            .map(|(replica_id, uid)| RemoveReplicaPodEvidence {
                replica_id: *replica_id,
                uid: uid.clone(),
            })
            .collect();
        let evidence = Self {
            observed_unix_seconds,
            replicas,
            pods,
            target_role_label: target_role_label.map(ToOwned::to_owned),
        };
        evidence.validate()?;
        Ok(evidence)
    }

    pub fn reconstruct(
        &self,
        operation: &DurableOperationStatus,
    ) -> Result<(OperationObservations, OperationPodIdentities), String> {
        self.validate()?;
        let observations = self
            .replicas
            .iter()
            .map(|replica| Ok((replica.replica_id, replica.reconstruct(operation)?)))
            .collect::<Result<OperationObservations, String>>()?;
        let pods = self
            .pods
            .iter()
            .map(|pod| (pod.replica_id, pod.uid.clone()))
            .collect();
        Ok((observations, pods))
    }

    fn validate(&self) -> Result<(), String> {
        if self.observed_unix_seconds <= 0 {
            return Err("remove evidence requires a positive observation time".to_string());
        }
        if self.replicas.len() > REMOVE_REPLICA_MAX_MEMBERS
            || self.pods.len() > REMOVE_REPLICA_MAX_MEMBERS
        {
            return Err(format!(
                "remove evidence supports at most {REMOVE_REPLICA_MAX_MEMBERS} members"
            ));
        }
        let replica_ids = self
            .replicas
            .iter()
            .map(|replica| replica.replica_id)
            .collect::<BTreeSet<_>>();
        let pod_ids = self
            .pods
            .iter()
            .map(|pod| pod.replica_id)
            .collect::<BTreeSet<_>>();
        if replica_ids.len() != self.replicas.len()
            || pod_ids.len() != self.pods.len()
            || self.pods.iter().any(|pod| pod.uid.is_empty())
        {
            return Err("remove evidence contains duplicate or empty identity".to_string());
        }
        Ok(())
    }
}

impl RemoveReplicaReplicaEvidence {
    fn capture(
        replica_id: i64,
        observation: &ReplicaObservation,
        action_id: Option<&str>,
    ) -> Result<Self, String> {
        let status = &observation.status;
        let action = action_id
            .and_then(|action_id| correlated_action_observation(status, action_id))
            .map(RemoveReplicaActionEvidence::capture)
            .transpose()?;
        Ok(Self {
            replica_id,
            instance_id: status.instance_id.to_string(),
            role: status.role.into(),
            epoch: EpochStatus {
                data_loss_number: status.epoch.data_loss_number,
                configuration_number: status.epoch.configuration_number,
            },
            current_progress: status.current_progress,
            catch_up_capability: status.catch_up_capability,
            healthy: status.healthy,
            write_status: status.write_status.into(),
            configuration: status
                .configuration
                .as_ref()
                .map(RemoveReplicaConfigurationEvidence::capture),
            active_connections: status
                .active_replica_connections
                .iter()
                .map(|connection| RemoveReplicaConnectionEvidence {
                    replica_id: connection.id,
                    instance_id: connection.instance_id.to_string(),
                })
                .collect(),
            agent: RemoveReplicaAgentEvidence {
                protocol_version: status.agent.protocol_version,
                lifecycle_peer_protocol_version: status.agent.lifecycle_peer_protocol_version,
                generation: status.agent.generation.to_string(),
                control_version: status.agent.control_version.value(),
                action,
            },
            control_address: observation.control_address.clone(),
            replicator_address: observation.replicator_address.clone(),
            pod_name: observation.pod_name.clone(),
        })
    }

    fn reconstruct(
        &self,
        operation: &DurableOperationStatus,
    ) -> Result<ReplicaObservation, String> {
        let generation = AgentGeneration::parse(self.agent.generation.clone())?;
        let control_version = AgentControlVersion::new(self.agent.control_version);
        let action = self
            .agent
            .action
            .as_ref()
            .map(|action| action.reconstruct(operation))
            .transpose()?;
        let current_action = action.map(|action| CorrelatedActionObservation {
            generation: generation.clone(),
            control_version,
            action,
        });
        Ok(ReplicaObservation {
            status: ReplicaStatusInfo {
                instance_id: ReplicaInstanceId::new(self.instance_id.clone()),
                role: self.role.into(),
                epoch: Epoch::new(self.epoch.data_loss_number, self.epoch.configuration_number),
                current_progress: self.current_progress,
                catch_up_capability: self.catch_up_capability,
                committed_lsn: self.current_progress,
                healthy: self.healthy,
                write_status: self.write_status.into(),
                configuration: self
                    .configuration
                    .as_ref()
                    .map(RemoveReplicaConfigurationEvidence::reconstruct)
                    .transpose()?,
                election_configuration: None,
                deactivation_info: None,
                active_replica_connections: self
                    .active_connections
                    .iter()
                    .map(|connection| ReplicaConnectionStatus {
                        id: connection.replica_id,
                        instance_id: ReplicaInstanceId::new(connection.instance_id.clone()),
                    })
                    .collect(),
                build_observation: None,
                agent: ReplicaAgentStatus {
                    protocol_version: self.agent.protocol_version,
                    lifecycle_peer_protocol_version: self.agent.lifecycle_peer_protocol_version,
                    generation,
                    control_version,
                    current_action,
                    retained_terminal_actions: Vec::new(),
                    local_faults: Vec::new(),
                },
            },
            control_address: self.control_address.clone(),
            replicator_address: self.replicator_address.clone(),
            pod_name: self.pod_name.clone(),
            pod_role_label: None,
        })
    }
}

impl RemoveReplicaConfigurationEvidence {
    fn capture(configuration: &ReplicaConfigurationStatus) -> Self {
        Self {
            mode: configuration.mode.into(),
            members: configuration
                .members
                .iter()
                .map(|member| RemoveReplicaConfigurationMemberEvidence {
                    replica_id: member.id,
                    instance_id: member.instance_id.to_string(),
                    role: member.role.into(),
                })
                .collect(),
            write_quorum: configuration.write_quorum,
        }
    }

    fn reconstruct(&self) -> Result<ReplicaConfigurationStatus, String> {
        if self.members.len() > REMOVE_REPLICA_MAX_MEMBERS {
            return Err("remove configuration evidence exceeds member bound".to_string());
        }
        Ok(ReplicaConfigurationStatus {
            mode: self.mode.into(),
            members: self
                .members
                .iter()
                .map(|member| ReplicaConfigurationMemberStatus {
                    id: member.replica_id,
                    instance_id: ReplicaInstanceId::new(member.instance_id.clone()),
                    role: member.role.into(),
                })
                .collect(),
            write_quorum: self.write_quorum,
        })
    }
}

impl RemoveReplicaActionEvidence {
    fn capture(action: &DurableActionObservation) -> Result<Self, String> {
        let result = match action.result {
            Some(DurableActionResult::RemoveReplica(result)) => Some(result.into()),
            Some(_) => return Err("remove evidence contains another action result".to_string()),
            None => None,
        };
        Ok(Self {
            state: action.state.into(),
            error_class: action.error_class.map(Into::into),
            error: action.error.clone(),
            result,
            progress: action
                .remove_replica_progress
                .as_ref()
                .map(RemoveReplicaProgressEvidence::capture),
        })
    }

    fn reconstruct(
        &self,
        operation: &DurableOperationStatus,
    ) -> Result<DurableActionObservation, String> {
        let intent = operation
            .remove_intent
            .as_ref()
            .ok_or_else(|| "remove action evidence has no frozen intent".to_string())?;
        Ok(DurableActionObservation {
            action_id: intent.action_id.clone(),
            signature: intent.input_signature.clone(),
            state: self.state.into(),
            error_class: self.error_class.map(Into::into),
            error: self.error.clone(),
            result: self
                .result
                .map(|result| DurableActionResult::RemoveReplica(result.into())),
            add_replica_progress: None,
            remove_replica_progress: self
                .progress
                .as_ref()
                .map(|progress| progress.reconstruct(operation))
                .transpose()?,
        })
    }
}

impl RemoveReplicaProgressEvidence {
    fn capture(progress: &RemoveReplicaProgress) -> Self {
        Self {
            phase: progress.phase.into(),
            commit_observed: progress.commit_observed,
            commit_observed_unix_seconds: progress.commit_observed_unix_seconds,
            connection_absent: progress.connection_absent,
            target_retirement: progress.target_retirement.into(),
            retirement_expiry_unix_seconds: progress.retirement_expiry_unix_seconds,
            compensation_expiry_unix_seconds: progress.compensation_expiry_unix_seconds,
            error: progress.error.clone(),
            current_install_dispatched: progress.current_install_dispatched,
        }
    }

    fn reconstruct(
        &self,
        operation: &DurableOperationStatus,
    ) -> Result<RemoveReplicaProgress, String> {
        let intent = operation
            .remove_intent
            .as_ref()
            .ok_or_else(|| "remove progress evidence has no frozen intent".to_string())?;
        let progress = RemoveReplicaProgress {
            phase: self.phase.into(),
            attempt_id: intent.attempt_id.clone(),
            commit_observed: self.commit_observed,
            commit_observed_unix_seconds: self.commit_observed_unix_seconds,
            connection_absent: self.connection_absent,
            target_retirement: self.target_retirement.into(),
            retirement_expiry_unix_seconds: self.retirement_expiry_unix_seconds,
            compensation_expiry_unix_seconds: self.compensation_expiry_unix_seconds,
            error: self.error.clone(),
            current_install_dispatched: self.current_install_dispatched,
        };
        progress.validate()?;
        Ok(progress)
    }
}

impl From<Role> for RemoveReplicaRoleEvidence {
    fn from(value: Role) -> Self {
        match value {
            Role::Primary => Self::Primary,
            Role::ActiveSecondary => Self::ActiveSecondary,
            Role::None => Self::None,
            Role::Unknown | Role::IdleSecondary => Self::Unknown,
        }
    }
}

impl From<RemoveReplicaRoleEvidence> for Role {
    fn from(value: RemoveReplicaRoleEvidence) -> Self {
        match value {
            RemoveReplicaRoleEvidence::Primary => Self::Primary,
            RemoveReplicaRoleEvidence::ActiveSecondary => Self::ActiveSecondary,
            RemoveReplicaRoleEvidence::None => Self::None,
            RemoveReplicaRoleEvidence::Unknown => Self::Unknown,
        }
    }
}

impl From<AccessStatus> for RemoveReplicaWriteStatusEvidence {
    fn from(value: AccessStatus) -> Self {
        match value {
            AccessStatus::Granted => Self::Granted,
            AccessStatus::ReconfigurationPending => Self::ReconfigurationPending,
            AccessStatus::NotPrimary => Self::NotPrimary,
            AccessStatus::NoWriteQuorum => Self::NoWriteQuorum,
        }
    }
}

impl From<RemoveReplicaWriteStatusEvidence> for AccessStatus {
    fn from(value: RemoveReplicaWriteStatusEvidence) -> Self {
        match value {
            RemoveReplicaWriteStatusEvidence::Granted => Self::Granted,
            RemoveReplicaWriteStatusEvidence::ReconfigurationPending => {
                Self::ReconfigurationPending
            }
            RemoveReplicaWriteStatusEvidence::NotPrimary => Self::NotPrimary,
            RemoveReplicaWriteStatusEvidence::NoWriteQuorum => Self::NoWriteQuorum,
        }
    }
}

impl From<ReplicaConfigurationMode> for RemoveReplicaConfigurationModeEvidence {
    fn from(value: ReplicaConfigurationMode) -> Self {
        match value {
            ReplicaConfigurationMode::CatchUp => Self::CatchUp,
            ReplicaConfigurationMode::Current => Self::Current,
        }
    }
}

impl From<RemoveReplicaConfigurationModeEvidence> for ReplicaConfigurationMode {
    fn from(value: RemoveReplicaConfigurationModeEvidence) -> Self {
        match value {
            RemoveReplicaConfigurationModeEvidence::CatchUp => Self::CatchUp,
            RemoveReplicaConfigurationModeEvidence::Current => Self::Current,
        }
    }
}

impl From<DurableActionState> for RemoveReplicaActionStateEvidence {
    fn from(value: DurableActionState) -> Self {
        match value {
            DurableActionState::Scheduled => Self::Scheduled,
            DurableActionState::InProgress => Self::InProgress,
            DurableActionState::Completed => Self::Completed,
            DurableActionState::Failed => Self::Failed,
        }
    }
}

impl From<RemoveReplicaActionStateEvidence> for DurableActionState {
    fn from(value: RemoveReplicaActionStateEvidence) -> Self {
        match value {
            RemoveReplicaActionStateEvidence::Scheduled => Self::Scheduled,
            RemoveReplicaActionStateEvidence::InProgress => Self::InProgress,
            RemoveReplicaActionStateEvidence::Completed => Self::Completed,
            RemoveReplicaActionStateEvidence::Failed => Self::Failed,
        }
    }
}

impl From<DurableActionErrorClass> for RemoveReplicaActionErrorClassEvidence {
    fn from(value: DurableActionErrorClass) -> Self {
        match value {
            DurableActionErrorClass::Internal => Self::Internal,
            DurableActionErrorClass::NotPrimary => Self::NotPrimary,
            DurableActionErrorClass::NoWriteQuorum => Self::NoWriteQuorum,
            DurableActionErrorClass::ReconfigurationPending => Self::ReconfigurationPending,
            DurableActionErrorClass::StaleEpoch => Self::StaleEpoch,
            DurableActionErrorClass::Cancelled => Self::Cancelled,
            DurableActionErrorClass::Closed => Self::Closed,
        }
    }
}

impl From<RemoveReplicaActionErrorClassEvidence> for DurableActionErrorClass {
    fn from(value: RemoveReplicaActionErrorClassEvidence) -> Self {
        match value {
            RemoveReplicaActionErrorClassEvidence::Internal => Self::Internal,
            RemoveReplicaActionErrorClassEvidence::NotPrimary => Self::NotPrimary,
            RemoveReplicaActionErrorClassEvidence::NoWriteQuorum => Self::NoWriteQuorum,
            RemoveReplicaActionErrorClassEvidence::ReconfigurationPending => {
                Self::ReconfigurationPending
            }
            RemoveReplicaActionErrorClassEvidence::StaleEpoch => Self::StaleEpoch,
            RemoveReplicaActionErrorClassEvidence::Cancelled => Self::Cancelled,
            RemoveReplicaActionErrorClassEvidence::Closed => Self::Closed,
        }
    }
}

impl From<RemoveReplicaTerminalResultStatus> for RemoveReplicaTerminalResult {
    fn from(value: RemoveReplicaTerminalResultStatus) -> Self {
        match value {
            RemoveReplicaTerminalResultStatus::CommittedClean => Self::CommittedClean,
            RemoveReplicaTerminalResultStatus::CommittedDegraded => Self::CommittedDegraded,
            RemoveReplicaTerminalResultStatus::Compensated => Self::Compensated,
            RemoveReplicaTerminalResultStatus::CompensationIncomplete => {
                Self::CompensationIncomplete
            }
        }
    }
}

impl From<RemoveReplicaCoordinatorPhaseStatus> for RemoveReplicaCoordinatorPhase {
    fn from(value: RemoveReplicaCoordinatorPhaseStatus) -> Self {
        match value {
            RemoveReplicaCoordinatorPhaseStatus::Validating => Self::Validating,
            RemoveReplicaCoordinatorPhaseStatus::InstallingCatchUpConfiguration => {
                Self::InstallingCatchUpConfiguration
            }
            RemoveReplicaCoordinatorPhaseStatus::WaitingForCatchUpQuorum => {
                Self::WaitingForCatchUpQuorum
            }
            RemoveReplicaCoordinatorPhaseStatus::InstallingCurrentConfiguration => {
                Self::InstallingCurrentConfiguration
            }
            RemoveReplicaCoordinatorPhaseStatus::RemovingConnection => Self::RemovingConnection,
            RemoveReplicaCoordinatorPhaseStatus::RetiringTarget => Self::RetiringTarget,
            RemoveReplicaCoordinatorPhaseStatus::Attesting => Self::Attesting,
            RemoveReplicaCoordinatorPhaseStatus::Compensating => Self::Compensating,
        }
    }
}

impl From<TargetRetirementObservationStatus> for TargetRetirementObservation {
    fn from(value: TargetRetirementObservationStatus) -> Self {
        match value {
            TargetRetirementObservationStatus::NotAttempted => Self::NotAttempted,
            TargetRetirementObservationStatus::InProgress => Self::InProgress,
            TargetRetirementObservationStatus::Completed => Self::Completed,
            TargetRetirementObservationStatus::Unavailable => Self::Unavailable,
            TargetRetirementObservationStatus::Stale => Self::Stale,
            TargetRetirementObservationStatus::Failed => Self::Failed,
        }
    }
}

pub struct RemoveReplicaWorkflow;

#[async_trait]
impl Workflow for RemoveReplicaWorkflow {
    async fn run(&self, context: &mut WorkflowContext<'_>, input: ExactBytes) -> TerminalOutcome {
        let input = match decode_workflow_input(input.as_slice()) {
            Ok(input) => input,
            Err(WorkflowInputError::Incompatible {
                source,
                found_version,
                message,
            }) => {
                return incompatible_terminal(source, found_version, message);
            }
            Err(WorkflowInputError::Rejected(message)) => {
                return rejected_terminal(message, Default::default());
            }
        };
        if input.execution_id != encode_execution_id(context.execution_id()) {
            return rejected_terminal(
                "native remove workflow execution identity mismatch".to_string(),
                Default::default(),
            );
        }
        let mut operation = match reconstruct_initial_operation(&input.admission) {
            Ok(operation) => operation,
            Err(error) => return rejected_terminal(error, Default::default()),
        };
        let mut accounting = RemoveReplicaActivityAccounting::default();
        let mut redeliveries = BTreeMap::<String, u8>::new();

        for _ in 0..REMOVE_REPLICA_MAX_TRANSITION_FUEL {
            match advance_without_evidence(operation, accounting) {
                Ok(Advance::State(next)) => operation = next,
                Ok(Advance::Terminal(terminal)) => return encode_terminal(terminal),
                Ok(Advance::Boundary(current)) => {
                    operation = current;
                    let logical = logical_boundary(&operation);
                    let result = match context.call::<RemoveReplicaBoundary>(logical).await {
                        Ok(result) => result,
                        Err(error) => {
                            return rejected_terminal(
                                format!("decode native remove boundary result: {error}"),
                                accounting,
                            );
                        }
                    };
                    match apply_boundary_result(
                        &operation,
                        result,
                        &mut accounting,
                        &mut redeliveries,
                    ) {
                        Ok(next) => operation = next,
                        Err(error) => return rejected_terminal(error, accounting),
                    }
                }
                Err(error) => return rejected_terminal(error, accounting),
            }
        }
        rejected_terminal(
            format!(
                "native remove exhausted its {REMOVE_REPLICA_MAX_TRANSITION_FUEL}-transition fuel"
            ),
            accounting,
        )
    }
}

enum Advance {
    State(DurableOperationStatus),
    Boundary(DurableOperationStatus),
    Terminal(RemoveReplicaTerminal),
}

fn advance_without_evidence(
    operation: DurableOperationStatus,
    accounting: RemoveReplicaActivityAccounting,
) -> Result<Advance, String> {
    use DurableOperationPhase as Phase;
    if matches!(
        operation.phase,
        Phase::Completed | Phase::Failed | Phase::Poisoned
    ) {
        return Ok(Advance::Terminal(terminal_from_operation(
            &operation, accounting,
        )?));
    }
    if !matches!(
        operation.phase,
        Phase::RemoveDispatchIntent
            | Phase::RemoveRecordCommit
            | Phase::RemovePublishTopology
            | Phase::RemoveFinalize
            | Phase::RemoveCompensateFinalize
    ) {
        return Ok(Advance::Boundary(operation));
    }
    match decide_remove_replica(
        &operation,
        &OperationObservations::new(),
        &OperationPodIdentities::new(),
        None,
        operation.phase_deadline_unix_seconds,
    )? {
        Decision::Persist(next) => {
            validate_transition(&operation, &next)?;
            Ok(Advance::State(next))
        }
        Decision::CommitSnapshot {
            operation: next,
            snapshot,
        } => {
            if snapshot != operation.target_snapshot {
                return Err("native remove publication changed reduced snapshot".to_string());
            }
            validate_transition(&operation, &next)?;
            Ok(Advance::State(next))
        }
        Decision::Complete {
            operation: completed,
            snapshot,
            compensated,
        } => {
            let expected = if compensated {
                operation.previous_snapshot.as_ref()
            } else {
                Some(&operation.target_snapshot)
            };
            if expected != Some(&snapshot) {
                return Err("native remove terminal changed frozen snapshot".to_string());
            }
            validate_transition(&operation, &completed)?;
            Ok(Advance::Terminal(terminal_from_operation(
                &completed, accounting,
            )?))
        }
        other => Err(format!(
            "deterministic native remove transition produced external decision: {other:?}"
        )),
    }
}

fn apply_boundary_result(
    operation: &DurableOperationStatus,
    result: RemoveReplicaBoundaryResult,
    accounting: &mut RemoveReplicaActivityAccounting,
    redeliveries: &mut BTreeMap<String, u8>,
) -> Result<DurableOperationStatus, String> {
    match result {
        RemoveReplicaBoundaryResult::Observation { evidence } => {
            accounting.record(DurableActivityClass::PassiveObservation)?;
            fold_evidence(operation, &evidence)
        }

        RemoveReplicaBoundaryResult::Effect { outcome } => {
            accounting.record(DurableActivityClass::ExternalEffect)?;
            match outcome {
                DurableEffectOutcome::Applied(evidence) => fold_evidence(operation, &evidence),
                DurableEffectOutcome::DefiniteFailure { class, message } => {
                    let next = record_activity_error(operation, &format!("{class}: {message}"));
                    validate_transition(operation, &next)?;
                    Ok(next)
                }
                DurableEffectOutcome::ProvenNoAdmission { .. }
                | DurableEffectOutcome::Pending
                | DurableEffectOutcome::Unknown { .. } => {
                    Err("native remove persisted a non-authoritative effect outcome".to_string())
                }
            }
        }
        RemoveReplicaBoundaryResult::ProvenNoAdmission {
            action_id,
            redelivery,
            evidence,
        } => {
            accounting.record(DurableActivityClass::ExternalEffect)?;
            if redelivery != 1
                || operation
                    .pending_action
                    .as_ref()
                    .is_none_or(|pending| pending.action_id != action_id)
            {
                return Err("invalid native remove proven-no-admission evidence".to_string());
            }
            let count = redeliveries.entry(action_id).or_default();
            *count = count.saturating_add(1);
            if *count > 1 {
                return Err(
                    "native remove action exceeded one proven-no-admission redelivery".to_string(),
                );
            }
            let mut next = operation.clone();
            if let Some(pending) = next.pending_action.as_mut() {
                clear_dispatch_evidence(pending);
                pending.last_error = Some("dispatch was proven not admitted".to_string());
            }
            validate_transition(operation, &next)?;
            fold_evidence(&next, &evidence)
        }
    }
}

fn advance_to_boundary(
    mut operation: DurableOperationStatus,
) -> Result<DurableOperationStatus, String> {
    for _ in 0..REMOVE_REPLICA_MAX_TRANSITION_FUEL {
        if matches!(
            operation.phase,
            DurableOperationPhase::Completed
                | DurableOperationPhase::Failed
                | DurableOperationPhase::Poisoned
        ) {
            return Ok(operation);
        }
        match advance_without_evidence(operation, Default::default())? {
            Advance::State(next) => operation = next,
            Advance::Boundary(current) => return Ok(current),
            Advance::Terminal(_) => unreachable!("terminal phases return before advancement"),
        }
    }
    Err("native remove active replay exhausted transition fuel".to_string())
}

pub fn replay_active_operation(
    reference: &RemoveReplicaExecution,
    activities: &[ActivityRecord],
) -> Result<DurableOperationStatus, String> {
    replay_active_operation_history(reference, activities).map(|(operation, _)| operation)
}

fn replay_active_operation_history(
    reference: &RemoveReplicaExecution,
    activities: &[ActivityRecord],
) -> Result<(DurableOperationStatus, Vec<DurableOperationStatus>), String> {
    let mut operation = reconstruct_initial_operation(admission_input(reference)?)?;
    let mut accounting = RemoveReplicaActivityAccounting::default();
    let mut redeliveries = BTreeMap::new();
    let mut history = Vec::with_capacity(activities.len().saturating_add(1));
    for record in activities {
        operation = advance_to_boundary(operation)?;
        history.push(operation.clone());
        let input = decode_boundary_input(record.input())?;
        if input != logical_boundary(&operation) {
            validate_prepared_activity(&operation, &input)?;
        }
        match record.state() {
            ActivityState::Completed { result } => {
                let result = decode_boundary_result(result)?;
                operation =
                    apply_boundary_result(&operation, result, &mut accounting, &mut redeliveries)?;
            }
            ActivityState::Scheduled | ActivityState::DispatchExposed { .. } => {
                return Ok((operation, history));
            }
        }
    }
    operation = advance_to_boundary(operation)?;
    history.push(operation.clone());
    Ok((operation, history))
}

fn operation_after_boundary_result(
    operation: &DurableOperationStatus,
    result: &RemoveReplicaBoundaryResult,
) -> Result<DurableOperationStatus, String> {
    let mut accounting = RemoveReplicaActivityAccounting::default();
    let mut redeliveries = BTreeMap::new();
    let next = apply_boundary_result(
        operation,
        result.clone(),
        &mut accounting,
        &mut redeliveries,
    )?;
    advance_to_boundary(next)
}

fn fold_evidence(
    operation: &DurableOperationStatus,
    evidence: &RemoveReplicaObservationEvidence,
) -> Result<DurableOperationStatus, String> {
    let (observations, pods) = evidence.reconstruct(operation)?;
    let mut current = operation.clone();
    for _ in 0..REMOVE_REPLICA_MAX_TRANSITION_FUEL {
        match decide_remove_replica(
            &current,
            &observations,
            &pods,
            evidence.target_role_label.as_deref(),
            evidence.observed_unix_seconds,
        )? {
            Decision::Persist(next) | Decision::RecordCommitEvidence(next) => {
                validate_transition(&current, &next)?;
                if next == current {
                    return Ok(current);
                }
                current = next;
            }
            Decision::CommitSnapshot {
                operation: next,
                snapshot,
            } => {
                if snapshot != current.target_snapshot {
                    return Err("native remove evidence changed reduced snapshot".to_string());
                }
                validate_transition(&current, &next)?;
                current = next;
            }
            Decision::Complete {
                operation: next,
                snapshot,
                compensated,
            } => {
                let expected = if compensated {
                    current.previous_snapshot.as_ref()
                } else {
                    Some(&current.target_snapshot)
                };
                if expected != Some(&snapshot) {
                    return Err("native remove evidence changed terminal snapshot".to_string());
                }
                validate_transition(&current, &next)?;
                return Ok(next);
            }
            Decision::Wait
            | Decision::Execute { .. }
            | Decision::PatchPodRoleExactUid { .. }
            | Decision::DeletePod { .. } => return Ok(current),
            Decision::PatchPodRole { .. } => {
                return Err("native remove requires exact-UID label commands".to_string());
            }
            other => {
                return Err(format!(
                    "native remove evidence reached unrelated decision: {other:?}"
                ));
            }
        }
    }
    Err("native remove evidence folding exhausted transition fuel".to_string())
}

fn logical_boundary(operation: &DurableOperationStatus) -> RemoveReplicaBoundaryInput {
    RemoveReplicaBoundaryInput::Observe {
        phase: operation.phase,
        attempt: operation
            .remove_intent
            .as_ref()
            .map_or(0, |intent| intent.attempt),
    }
}

pub struct RemoveReplicaPreparedActivityResolver {
    operation: DurableOperationStatus,
    history: Vec<DurableOperationStatus>,
    force_observation: bool,
    observations: OperationObservations,
    pod_identities: OperationPodIdentities,
    target_role_label: Option<String>,
    addressed_instances: BTreeMap<i64, ReplicaInstanceId>,
    now: i64,
}

impl RemoveReplicaPreparedActivityResolver {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        operation: &DurableOperationStatus,
        observations: &OperationObservations,
        pod_identities: &OperationPodIdentities,
        target_role_label: Option<&str>,
        addressed_instances: &BTreeMap<i64, ReplicaInstanceId>,
        now: i64,
    ) -> Self {
        Self {
            operation: operation.clone(),
            history: vec![operation.clone()],
            force_observation: false,
            observations: observations.clone(),
            pod_identities: pod_identities.clone(),
            target_role_label: target_role_label.map(ToOwned::to_owned),
            addressed_instances: addressed_instances.clone(),
            now,
        }
    }

    fn with_history(mut self, history: Vec<DurableOperationStatus>) -> Self {
        self.history = history;
        self
    }

    fn force_observation(mut self, force_observation: bool) -> Self {
        self.force_observation = force_observation;
        self
    }

    fn prepare(
        &self,
        logical: &RemoveReplicaBoundaryInput,
    ) -> Result<RemoveReplicaBoundaryInput, PreparedActivityError> {
        if logical != &logical_boundary(&self.operation) {
            return Err(PreparedActivityError::Validation);
        }
        if self.force_observation {
            return Ok(logical.clone());
        }
        match decide_remove_replica(
            &self.operation,
            &self.observations,
            &self.pod_identities,
            self.target_role_label.as_deref(),
            self.now,
        )
        .map_err(|_| PreparedActivityError::Derivation)?
        {
            Decision::Execute {
                target_id,
                action_id,
                action,
            } => {
                let pending = self
                    .operation
                    .pending_action
                    .as_ref()
                    .ok_or(PreparedActivityError::Validation)?;
                if pending.target_id != target_id
                    || pending.action_id != action_id
                    || !validate_remove_replica_action_kind(pending.kind, &action)
                {
                    return Err(PreparedActivityError::Validation);
                }
                let observed = self
                    .observations
                    .get(&target_id)
                    .ok_or(PreparedActivityError::Derivation)?;
                let addressed = self
                    .addressed_instances
                    .get(&target_id)
                    .ok_or(PreparedActivityError::Derivation)?;
                let (_, command) = prepare_lifecycle_replica_effect_command(
                    pending,
                    &observed.status,
                    addressed,
                    &action,
                )
                .map_err(preparation_error)?;
                if !validate_remove_replica_dispatch_authority(
                    &self.operation,
                    &observed.status,
                    addressed,
                    &action,
                ) {
                    return Err(PreparedActivityError::Validation);
                }
                Ok(RemoveReplicaBoundaryInput::ReplicaCommand {
                    command: command.into(),
                })
            }
            Decision::PatchPodRoleExactUid {
                target_id,
                expected_uid,
                role,
            } => prepare_remove_label_effect_command(
                &self.operation,
                target_id,
                &expected_uid,
                &role,
                &self.pod_identities,
            )
            .map(|command| RemoveReplicaBoundaryInput::LabelCommand { command })
            .map_err(|_| PreparedActivityError::Derivation),
            Decision::DeletePod {
                pod_name,
                expected_uid,
            } => prepare_remove_delete_effect_command(
                &self.operation,
                &pod_name,
                &expected_uid,
                &self.pod_identities,
            )
            .map(|command| RemoveReplicaBoundaryInput::DeleteCommand { command })
            .map_err(|_| PreparedActivityError::Derivation),
            Decision::Persist(_)
            | Decision::RecordCommitEvidence(_)
            | Decision::Wait
            | Decision::CommitSnapshot { .. }
            | Decision::Complete { .. } => Ok(logical.clone()),
            _ => Err(PreparedActivityError::Validation),
        }
    }
}

impl PreparedActivityResolver for RemoveReplicaPreparedActivityResolver {
    fn resolve(
        &self,
        logical: &ActivitySpec,
        recorded: Option<&ActivitySpec>,
    ) -> Result<ActivitySpec, PreparedActivityError> {
        let logical_input = decode_activity_input::<RemoveReplicaBoundary>(logical.input())
            .map_err(|_| PreparedActivityError::Encoding)?;
        if let Some(recorded) = recorded {
            if recorded.name() != logical.name()
                || recorded.max_result_bytes() != logical.max_result_bytes()
            {
                return Ok(logical.clone());
            }
            let Ok(recorded_input) =
                decode_activity_input::<RemoveReplicaBoundary>(recorded.input())
            else {
                return Ok(logical.clone());
            };
            let Some(historical_operation) = self
                .history
                .iter()
                .find(|operation| logical_input == logical_boundary(operation))
            else {
                return Ok(logical.clone());
            };
            if validate_prepared_activity(historical_operation, &recorded_input).is_err() {
                return Ok(logical.clone());
            }
            return Ok(recorded.clone());
        }
        activity_spec(&self.prepare(&logical_input)?).map_err(|_| PreparedActivityError::Encoding)
    }
}

fn preparation_error(error: DurableEffectPreparationError) -> PreparedActivityError {
    match error {
        DurableEffectPreparationError::WaitForExactIncarnation
        | DurableEffectPreparationError::WaitForSupportedProtocol => {
            PreparedActivityError::Derivation
        }
        DurableEffectPreparationError::InvalidCommand => PreparedActivityError::Validation,
    }
}

pub fn validate_prepared_activity(
    operation: &DurableOperationStatus,
    input: &RemoveReplicaBoundaryInput,
) -> Result<(), String> {
    match input {
        RemoveReplicaBoundaryInput::Observe { .. } if input == &logical_boundary(operation) => {
            Ok(())
        }
        RemoveReplicaBoundaryInput::ReplicaCommand { command } => {
            let exact = command.effect_command()?;
            let pending = operation
                .pending_action
                .as_ref()
                .ok_or_else(|| "prepared remove command has no pending action".to_string())?;
            let action = kuberic_core::grpc::convert::decode_correlated_action_payload(
                &exact.action_payload,
            )
            .map_err(|error| format!("decode prepared remove command: {error}"))?;
            let expected_action = DurableReplicaAction::RemoveReplicaIntent {
                intent: Box::new(core_intent(operation)?),
            };
            let intent = operation
                .remove_intent
                .as_ref()
                .ok_or_else(|| "prepared remove command has no frozen intent".to_string())?;
            let expected = ReplicaEffectCommand {
                action_id: pending.action_id.clone(),
                action_signature: expected_action.signature(),
                target_id: pending.target_id,
                target_instance_id: pending.target_instance_id.clone(),
                expected_epoch: pending.expected_epoch.clone(),
                desired_postcondition: pending.desired_postcondition.clone(),
                expected_agent_generation: intent.primary_agent_generation.clone(),
                expected_control_version: intent.primary_agent_control_version,
                observed_runtime_epoch: pending.expected_epoch.clone(),
                action_payload: kuberic_core::grpc::convert::encode_correlated_action_payload(
                    &expected_action,
                )
                .map_err(|error| format!("encode exact prepared remove command: {error}"))?,
            };
            if exact != expected
                || !validate_remove_replica_action_kind(pending.kind, &action)
                || action.signature() != expected_action.signature()
                || exact.action_signature != expected_action.signature()
            {
                return Err("prepared remove command changed exact authority".to_string());
            }
            Ok(())
        }
        RemoveReplicaBoundaryInput::LabelCommand { command } => {
            if operation.target_replica_id != Some(command.target_id)
                || operation.target_pod_name.as_deref() != Some(command.pod_name.as_str())
                || operation.target_pod_uid.as_deref() != Some(command.expected_uid.as_str())
                || command.role != "retired"
                || !command.has_valid_identity_signature()
            {
                return Err("prepared remove label changed exact target identity".to_string());
            }
            Ok(())
        }
        RemoveReplicaBoundaryInput::DeleteCommand { command } => {
            if operation.target_replica_id != Some(command.target_id)
                || operation.target_pod_name.as_deref() != Some(command.pod_name.as_str())
                || operation.target_pod_uid.as_deref() != Some(command.expected_uid.as_str())
                || !command.has_valid_identity_signature()
            {
                return Err("prepared remove delete changed exact target identity".to_string());
            }
            Ok(())
        }
        RemoveReplicaBoundaryInput::Observe { .. } => {
            Err("logical remove boundary changed replay identity".to_string())
        }
    }
}

pub fn encode_boundary_result(result: &RemoveReplicaBoundaryResult) -> Result<ExactBytes, String> {
    encode_activity_result::<RemoveReplicaBoundary>(result)
        .map_err(|error| format!("serialize native remove boundary result: {error}"))
}

pub fn decode_boundary_result(result: &ExactBytes) -> Result<RemoveReplicaBoundaryResult, String> {
    decode_activity_result::<RemoveReplicaBoundary>(result)
        .map_err(|error| format!("decode native remove boundary result: {error}"))
}

pub fn decode_boundary_input(input: &ExactBytes) -> Result<RemoveReplicaBoundaryInput, String> {
    decode_activity_input::<RemoveReplicaBoundary>(input)
        .map_err(|error| format!("decode native remove boundary input: {error}"))
}

pub fn activity_spec(input: &RemoveReplicaBoundaryInput) -> Result<ActivitySpec, String> {
    let input = encode_activity_input::<RemoveReplicaBoundary>(input)
        .map_err(|error| format!("serialize native remove boundary input: {error}"))?;
    Ok(ActivitySpec::with_bounds_and_options(
        ActivityName::new(
            REMOVE_REPLICA_ACTIVITY_NAME,
            REMOVE_REPLICA_ACTIVITY_VERSION,
        )
        .map_err(|error| format!("construct native remove activity name: {error}"))?,
        input,
        REMOVE_REPLICA_MAX_BOUNDARY_INPUT_BYTES,
        REMOVE_REPLICA_MAX_BOUNDARY_RESULT_BYTES,
        ActivityOptions::default(),
    ))
}

pub type RemoveReplicaHost = DurableOperatorHost;

pub struct FrameworkNativeRemoveReplicaRuntime {
    inner: Arc<DurableWorkflowRuntime>,
}

impl FrameworkNativeRemoveReplicaRuntime {
    pub fn kubernetes(client: kube::Client) -> Self {
        Self {
            inner: Arc::new(DurableWorkflowRuntime::kubernetes(client)),
        }
    }

    pub fn in_memory(store: InMemoryCheckpointStore) -> Self {
        Self {
            inner: Arc::new(DurableWorkflowRuntime::in_memory(store)),
        }
    }

    pub fn shared(inner: Arc<DurableWorkflowRuntime>) -> Self {
        Self { inner }
    }

    pub async fn host(
        &self,
        namespace: &str,
        set_name: &str,
        set_uid: &str,
        reference: &RemoveReplicaExecution,
    ) -> Result<Arc<Mutex<RemoveReplicaHost>>, String> {
        let execution_id = execution_id(reference)?;
        self.inner
            .host(
                namespace,
                set_name,
                set_uid,
                "remove-replica-native",
                execution_id,
                &reference.execution_id,
                &reference.checkpoint_name,
                checkpoint_store_options(namespace, set_name, set_uid)?,
                checkpoint_limits(),
                checkpoint_measurement_decoder(),
            )
            .await
    }

    pub async fn forget(&self, namespace: &str, set_name: &str, set_uid: &str, execution_id: &str) {
        self.inner
            .forget(
                namespace,
                set_name,
                set_uid,
                "remove-replica-native",
                execution_id,
            )
            .await;
    }

    pub async fn measurements(
        &self,
        namespace: &str,
        set_name: &str,
        set_uid: &str,
        execution_id: &str,
    ) -> Option<super::checkpoint_store::DurableCheckpointMeasurementsSnapshot> {
        self.inner
            .measurements(
                namespace,
                set_name,
                set_uid,
                "remove-replica-native",
                execution_id,
            )
            .await
    }
}

pub fn checkpoint_store_options(
    namespace: &str,
    set_name: &str,
    set_uid: &str,
) -> Result<KubernetesCheckpointStoreOptions, String> {
    if namespace.is_empty() || set_name.is_empty() || set_uid.is_empty() {
        return Err("native remove checkpoint owner requires namespace, name, and UID".to_string());
    }
    Ok(
        KubernetesCheckpointStoreOptions::default().with_owner(KubernetesCheckpointOwner::new(
            OwnerReference {
                api_version: "kuberic.io/v1".to_string(),
                kind: "KubericSet".to_string(),
                name: set_name.to_string(),
                uid: set_uid.to_string(),
                controller: Some(false),
                block_owner_deletion: Some(false),
            },
            KubernetesCheckpointOwnerScope::Namespaced(namespace.to_string()),
        )),
    )
}

pub fn new_execution(
    set_uid: &str,
    previous_snapshot: StablePartitionSnapshotStatus,
    target: RemoveReplicaTarget,
    mode: DurableRemoveMode,
    minimum_replicas: usize,
    now: i64,
) -> Result<RemoveReplicaExecution, String> {
    if set_uid.is_empty() {
        return Err("native remove requires the KubericSet UID".to_string());
    }
    let execution_id = ExecutionId::from_bytes(random());
    let execution_hex = encode_execution_id(execution_id);
    let operation_authority = format!("{set_uid}:native-remove:{execution_hex}");
    let operation = start_remove_replica(
        &operation_authority,
        previous_snapshot.clone(),
        RemoveReplicaTarget {
            replica_id: target.replica_id,
            pod_name: target.pod_name.clone(),
            pod_uid: target.pod_uid.clone(),
            replicator_address: target.replicator_address.clone(),
            agent_generation: target.agent_generation.clone(),
        },
        mode,
        minimum_replicas,
        now,
    )?;
    let input = RemoveReplicaAdmissionInputStatus {
        operation_authority,
        operation_id: operation.operation_id,
        mode,
        previous_snapshot,
        target: RemoveReplicaAdmissionTargetStatus {
            replica_id: target.replica_id,
            instance_id: target.pod_uid.clone(),
            pod_name: target.pod_name,
            pod_uid: target.pod_uid,
            replicator_address: target.replicator_address,
            agent_generation: target
                .agent_generation
                .map(|generation| generation.to_string()),
        },
        minimum_committed_replicas: u32::try_from(minimum_replicas.max(1))
            .map_err(|_| "native remove minimum replicas are not representable".to_string())?,
        accepted_unix_seconds: now,
        overall_deadline_unix_seconds: operation.phase_deadline_unix_seconds,
    };
    let reference = RemoveReplicaExecution {
        contract_version: REMOVE_REPLICA_CONTRACT_VERSION,
        execution_id: execution_hex,
        checkpoint_name: KubernetesCheckpointStore::object_name(execution_id),
        input: Some(input),
        incompatibility: None,
    };
    execution_spec(&reference)?;
    Ok(reference)
}

pub fn execution_id(reference: &RemoveReplicaExecution) -> Result<ExecutionId, String> {
    if let Some(marker) = &reference.incompatibility {
        return Err(format!(
            "incompatible remove execution from {:?} contract {} identity {} fingerprint {}",
            marker.source,
            marker.legacy_contract_version,
            marker.legacy_execution_id,
            marker.fingerprint
        ));
    }
    if reference.contract_version != REMOVE_REPLICA_CONTRACT_VERSION {
        return Err(format!(
            "incompatible native remove contract version {}; supported {}",
            reference.contract_version, REMOVE_REPLICA_CONTRACT_VERSION
        ));
    }
    let execution_id = ExecutionId::from_bytes(decode_execution_id(&reference.execution_id)?);
    let expected_name = KubernetesCheckpointStore::object_name(execution_id);
    if reference.checkpoint_name != expected_name {
        return Err(format!(
            "native remove checkpoint name mismatch: expected {expected_name}, found {}",
            reference.checkpoint_name
        ));
    }
    Ok(execution_id)
}

pub fn admission_input(
    reference: &RemoveReplicaExecution,
) -> Result<&RemoveReplicaAdmissionInputStatus, String> {
    reference
        .input
        .as_ref()
        .ok_or_else(|| "native remove execution has no immutable admission input".to_string())
}

pub fn execution_spec(reference: &RemoveReplicaExecution) -> Result<ExecutionSpec, String> {
    let execution_id = execution_id(reference)?;
    let admission = admission_input(reference)?;
    validate_admission(admission)?;
    let input = RemoveReplicaWorkflowInput {
        contract_version: reference.contract_version,
        execution_id: reference.execution_id.clone(),
        admission: admission.clone(),
    };
    let encoded = serde_json::to_vec(&input)
        .map_err(|error| format!("serialize native remove workflow input: {error}"))?;
    if encoded.len() > REMOVE_REPLICA_MAX_WORKFLOW_INPUT_BYTES {
        return Err(format!(
            "native remove immutable input is {} bytes; maximum is {}",
            encoded.len(),
            REMOVE_REPLICA_MAX_WORKFLOW_INPUT_BYTES
        ));
    }
    Ok(ExecutionSpec::new(
        execution_id,
        ExactBytes::new(encoded),
        REMOVE_REPLICA_MAX_TERMINAL_PAYLOAD_BYTES,
    ))
}

pub fn reconstruct_initial_operation(
    input: &RemoveReplicaAdmissionInputStatus,
) -> Result<DurableOperationStatus, String> {
    let operation = start_remove_replica(
        &input.operation_authority,
        input.previous_snapshot.clone(),
        RemoveReplicaTarget {
            replica_id: input.target.replica_id,
            pod_name: input.target.pod_name.clone(),
            pod_uid: input.target.pod_uid.clone(),
            replicator_address: input.target.replicator_address.clone(),
            agent_generation: input
                .target
                .agent_generation
                .clone()
                .map(AgentGeneration::parse)
                .transpose()?,
        },
        input.mode,
        usize::try_from(input.minimum_committed_replicas)
            .map_err(|_| "native remove minimum replicas are not representable".to_string())?,
        input.accepted_unix_seconds,
    )?;
    if operation.operation_id != input.operation_id
        || operation.target_instance_id.as_deref() != Some(input.target.instance_id.as_str())
        || operation.phase_deadline_unix_seconds != input.overall_deadline_unix_seconds
    {
        return Err("native remove immutable admission authority changed".to_string());
    }
    Ok(operation)
}

pub fn validate_admission(input: &RemoveReplicaAdmissionInputStatus) -> Result<(), String> {
    if input.operation_authority.is_empty()
        || input.operation_id.is_empty()
        || input.accepted_unix_seconds <= 0
        || input.overall_deadline_unix_seconds <= input.accepted_unix_seconds
        || input.target.instance_id != input.target.pod_uid
    {
        return Err("native remove immutable admission input is invalid".to_string());
    }
    if input.previous_snapshot.members.len() > REMOVE_REPLICA_MAX_MEMBERS {
        return Err(format!(
            "native remove supports at most {REMOVE_REPLICA_MAX_MEMBERS} members"
        ));
    }
    let operation = reconstruct_initial_operation(input)?;
    validate_remove_replica_operation(&operation)?;
    if PROJECTED_FAULT_HISTORY_RECORDS > REMOVE_REPLICA_MAX_ACTIVITY_RECORDS {
        return Err("native remove projected fault history exceeds record bound".to_string());
    }
    let maximum_active = maximum_active_checkpoint()?
        .encoded_len()
        .map_err(|error| format!("measure native remove active checkpoint: {error}"))?;
    if maximum_active > REMOVE_REPLICA_MAX_ACTIVE_ENCODED_BYTES {
        return Err(format!(
            "native remove projected fault history is {maximum_active} bytes; maximum is {REMOVE_REPLICA_MAX_ACTIVE_ENCODED_BYTES}"
        ));
    }
    let maximum_terminal = maximum_terminal_checkpoint()?
        .encoded_len()
        .map_err(|error| format!("measure native remove terminal checkpoint: {error}"))?;
    if maximum_terminal > REMOVE_REPLICA_MAX_TERMINAL_ENCODED_BYTES {
        return Err(format!(
            "native remove projected terminal is {maximum_terminal} bytes; maximum is {REMOVE_REPLICA_MAX_TERMINAL_ENCODED_BYTES}"
        ));
    }
    Ok(())
}

pub fn checkpoint_limits() -> CheckpointLimits {
    CheckpointLimits::new(
        REMOVE_REPLICA_MAX_ACTIVITY_RECORDS,
        REMOVE_REPLICA_MAX_ACTIVE_ENCODED_BYTES,
        REMOVE_REPLICA_MAX_TERMINAL_ENCODED_BYTES,
    )
    .expect("native remove checkpoint limits are nonzero")
}

fn validate_active_record_bytes(actual: usize) -> Result<(), String> {
    if actual > REMOVE_REPLICA_MAX_ACTIVE_ENCODED_BYTES {
        Err(format!(
            "native remove active record is {actual} bytes; maximum is {REMOVE_REPLICA_MAX_ACTIVE_ENCODED_BYTES}"
        ))
    } else {
        Ok(())
    }
}

fn validate_terminal_record_bytes(actual: usize) -> Result<(), String> {
    if actual > REMOVE_REPLICA_MAX_TERMINAL_ENCODED_BYTES {
        Err(format!(
            "native remove terminal record is {actual} bytes; maximum is {REMOVE_REPLICA_MAX_TERMINAL_ENCODED_BYTES}"
        ))
    } else {
        Ok(())
    }
}

fn validate_terminal_payload_bytes(actual: usize) -> Result<(), String> {
    if actual > REMOVE_REPLICA_MAX_TERMINAL_PAYLOAD_BYTES as usize {
        Err(format!(
            "native remove terminal payload is {actual} bytes; maximum is {REMOVE_REPLICA_MAX_TERMINAL_PAYLOAD_BYTES}"
        ))
    } else {
        Ok(())
    }
}

pub fn maximum_active_checkpoint() -> Result<CheckpointEnvelope, String> {
    let execution_id = ExecutionId::from_bytes([u8::MAX; 16]);
    let execution = ExecutionSpec::new(
        execution_id,
        ExactBytes::new(vec![u8::MAX; REMOVE_REPLICA_MAX_WORKFLOW_INPUT_BYTES]),
        REMOVE_REPLICA_MAX_TERMINAL_PAYLOAD_BYTES,
    );
    let contract = ExecutionContract::with_encoded_limits(
        execution,
        REMOVE_REPLICA_MAX_ACTIVE_ENCODED_BYTES as u64,
        REMOVE_REPLICA_MAX_TERMINAL_ENCODED_BYTES as u64,
    );
    let name = ActivityName::new(
        REMOVE_REPLICA_ACTIVITY_NAME,
        REMOVE_REPLICA_ACTIVITY_VERSION,
    )
    .map_err(|error| format!("construct native remove activity name: {error}"))?;
    let activities = (0..PROJECTED_FAULT_HISTORY_RECORDS)
        .map(|sequence| {
            ActivityRecord::completed(
                ActivitySequence::new(sequence as u64),
                ActivitySpec::new(
                    name.clone(),
                    ExactBytes::new(vec![
                        u8::MAX;
                        REMOVE_REPLICA_MAX_BOUNDARY_INPUT_BYTES as usize
                    ]),
                    REMOVE_REPLICA_MAX_BOUNDARY_RESULT_BYTES,
                ),
                ExactBytes::new(vec![
                    u8::MAX;
                    REMOVE_REPLICA_MAX_BOUNDARY_RESULT_BYTES as usize
                ]),
            )
        })
        .collect();
    let checkpoint = CheckpointEnvelope::encode_with_limits(
        &CheckpointPayload::active(contract, activities),
        checkpoint_limits(),
    )
    .map_err(|error| format!("project native remove active checkpoint: {error}"))?;
    validate_active_record_bytes(
        checkpoint
            .encoded_len()
            .map_err(|error| format!("measure native remove active checkpoint: {error}"))?,
    )?;
    Ok(checkpoint)
}

pub fn maximum_terminal_checkpoint() -> Result<CheckpointEnvelope, String> {
    let execution_id = ExecutionId::from_bytes([u8::MAX; 16]);
    let execution = ExecutionSpec::new(
        execution_id,
        ExactBytes::new(vec![u8::MAX; REMOVE_REPLICA_MAX_WORKFLOW_INPUT_BYTES]),
        REMOVE_REPLICA_MAX_TERMINAL_PAYLOAD_BYTES,
    );
    let contract = ExecutionContract::with_encoded_limits(
        execution,
        REMOVE_REPLICA_MAX_ACTIVE_ENCODED_BYTES as u64,
        REMOVE_REPLICA_MAX_TERMINAL_ENCODED_BYTES as u64,
    );
    let checkpoint = CheckpointEnvelope::encode_with_limits(
        &CheckpointPayload::terminal(
            contract,
            TerminalOutcome::failed(vec![
                u8::MAX;
                REMOVE_REPLICA_MAX_TERMINAL_PAYLOAD_BYTES as usize
            ]),
            REMOVE_REPLICA_MAX_ACTIVITY_RECORDS as u64,
        ),
        checkpoint_limits(),
    )
    .map_err(|error| format!("project native remove terminal checkpoint: {error}"))?;
    validate_terminal_record_bytes(
        checkpoint
            .encoded_len()
            .map_err(|error| format!("measure native remove terminal checkpoint: {error}"))?,
    )?;
    Ok(checkpoint)
}

fn validate_transition(
    current: &DurableOperationStatus,
    next: &DurableOperationStatus,
) -> Result<(), String> {
    use DurableOperationPhase as Phase;
    validate_remove_replica_operation(next)?;
    let mut normalized = next.clone();
    normalized.phase = current.phase;
    normalized.committed_snapshot = current.committed_snapshot.clone();
    normalized.phase_deadline_unix_seconds = current.phase_deadline_unix_seconds;
    normalized.pending_action = current.pending_action.clone();
    normalized.last_error = current.last_error.clone();
    normalized.remove_intent = current.remove_intent.clone();
    normalized.remove_commit_evidence = current.remove_commit_evidence.clone();
    normalized.remove_cleanup = current.remove_cleanup.clone();
    normalized.removal_disposition = current.removal_disposition.clone();
    if normalized != *current {
        return Err("native remove transition changed immutable authority".to_string());
    }
    if current.remove_commit_evidence.is_some()
        && (next.remove_commit_evidence != current.remove_commit_evidence
            || next.committed_snapshot != current.committed_snapshot)
    {
        return Err("native remove transition regressed commit evidence".to_string());
    }
    if current.remove_commit_evidence.is_none()
        && next.remove_commit_evidence.is_some()
        && !matches!(
            (current.phase, next.phase),
            (Phase::RemoveAwaitCoordination, Phase::RemoveRecordCommit)
        )
    {
        return Err("native remove transition invented commit evidence".to_string());
    }
    if next.remove_cleanup.is_some() && next.remove_commit_evidence.is_none() {
        return Err("native remove cleanup preceded commit evidence".to_string());
    }
    if let Some(current_cleanup) = current.remove_cleanup.as_ref() {
        let next_cleanup = next
            .remove_cleanup
            .as_ref()
            .ok_or_else(|| "native remove transition regressed cleanup evidence".to_string())?;
        if (current_cleanup.connection_absent && !next_cleanup.connection_absent)
            || (current_cleanup.target_labels_fenced && !next_cleanup.target_labels_fenced)
            || (current_cleanup.target_pod_deleted && !next_cleanup.target_pod_deleted)
            || (current_cleanup.target_retirement.is_some()
                && next_cleanup.target_retirement.is_none())
            || (current_cleanup.target_retirement
                == Some(TargetRetirementObservationStatus::Completed)
                && next_cleanup.target_retirement
                    != Some(TargetRetirementObservationStatus::Completed))
        {
            return Err("native remove transition regressed cleanup evidence".to_string());
        }
    }
    if current
        .remove_intent
        .as_ref()
        .is_some_and(|intent| intent.current_install_dispatched)
        && next
            .remove_intent
            .as_ref()
            .is_none_or(|intent| !intent.current_install_dispatched)
    {
        return Err("native remove regressed current-install evidence".to_string());
    }
    if next.remove_commit_evidence.is_some()
        && next.committed_snapshot.as_ref() != Some(&next.target_snapshot)
    {
        return Err("native remove commit lacks exact reduced snapshot".to_string());
    }
    let allowed = current.phase == next.phase
        || matches!(
            (current.phase, next.phase),
            (Phase::RemoveFreezeIntent, Phase::RemoveDispatchIntent)
                | (Phase::RemoveFreezeIntent, Phase::Poisoned)
                | (Phase::RemoveDispatchIntent, Phase::RemoveAwaitCoordination)
                | (Phase::RemoveAwaitCoordination, Phase::RemoveFreezeIntent)
                | (Phase::RemoveAwaitCoordination, Phase::RemoveRecordCommit)
                | (
                    Phase::RemoveAwaitCoordination,
                    Phase::RemoveCompensateFinalize
                )
                | (Phase::RemoveAwaitCoordination, Phase::Poisoned)
                | (Phase::RemoveRecordCommit, Phase::RemoveAwaitCleanup)
                | (Phase::RemoveAwaitCleanup, Phase::RemoveDeleteTargetPod)
                | (Phase::RemoveAwaitCleanup, Phase::Poisoned)
                | (Phase::RemoveDeleteTargetPod, Phase::RemovePublishTopology)
                | (Phase::RemovePublishTopology, Phase::RemoveFinalize)
                | (Phase::RemovePublishTopology, Phase::Poisoned)
                | (Phase::RemoveFinalize, Phase::Completed)
                | (Phase::RemoveCompensateFinalize, Phase::Failed)
        );
    if !allowed {
        return Err(format!(
            "invalid native remove phase transition {:?} -> {:?}",
            current.phase, next.phase
        ));
    }
    Ok(())
}

fn terminal_from_operation(
    operation: &DurableOperationStatus,
    accounting: RemoveReplicaActivityAccounting,
) -> Result<RemoveReplicaTerminal, String> {
    match operation.phase {
        DurableOperationPhase::Completed => {
            let commit_evidence = operation
                .remove_commit_evidence
                .clone()
                .ok_or_else(|| "completed native remove has no commit evidence".to_string())?;
            let cleanup = operation
                .remove_cleanup
                .clone()
                .ok_or_else(|| "completed native remove has no cleanup evidence".to_string())?;
            validate_completed_cleanup(&cleanup)?;
            Ok(RemoveReplicaTerminal::Completed {
                commit_evidence,
                cleanup,
                accounting,
            })
        }
        DurableOperationPhase::Failed if operation.remove_commit_evidence.is_none() => {
            Ok(RemoveReplicaTerminal::Compensated {
                message: bounded_message(
                    operation
                        .last_error
                        .as_deref()
                        .unwrap_or("remove-replica pre-commit work was compensated"),
                ),
                accounting,
            })
        }
        DurableOperationPhase::Poisoned => Ok(RemoveReplicaTerminal::Unsafe {
            committed: operation.remove_commit_evidence.is_some(),
            commit_evidence: operation.remove_commit_evidence.clone(),
            cleanup: operation.remove_cleanup.clone(),
            disposition: operation
                .removal_disposition
                .clone()
                .ok_or_else(|| "unsafe native remove has no typed disposition".to_string())?,
            message: bounded_message(
                operation
                    .last_error
                    .as_deref()
                    .unwrap_or("remove-replica cannot advance safely"),
            ),
            accounting,
        }),
        _ => Err(format!(
            "native remove terminal cannot be constructed from {:?}",
            operation.phase
        )),
    }
}

pub fn decode_terminal(outcome: &TerminalOutcome) -> Result<RemoveReplicaTerminal, String> {
    let terminal: RemoveReplicaTerminal = serde_json::from_slice(outcome.payload().as_slice())
        .map_err(|error| format!("decode native remove terminal: {error}"))?;
    match (outcome, &terminal) {
        (TerminalOutcome::Succeeded(_), RemoveReplicaTerminal::Completed { cleanup, .. }) => {
            validate_completed_cleanup(cleanup)?
        }
        (TerminalOutcome::Succeeded(_), RemoveReplicaTerminal::Compensated { .. })
        | (TerminalOutcome::Failed(_), RemoveReplicaTerminal::Unsafe { .. })
        | (TerminalOutcome::Failed(_), RemoveReplicaTerminal::Rejected { .. })
        | (TerminalOutcome::Failed(_), RemoveReplicaTerminal::IncompatibleContract { .. }) => {}
        _ => return Err("native remove terminal kind disagrees with kernel outcome".to_string()),
    }
    Ok(terminal)
}

pub fn validate_loaded_terminal(
    reference: &RemoveReplicaExecution,
    outcome: &TerminalOutcome,
    completed_activity_count: u64,
) -> Result<RemoveReplicaTerminal, String> {
    let initial = reconstruct_initial_operation(admission_input(reference)?)?;
    let terminal = decode_terminal(outcome)?;
    let accounting = terminal_accounting(&terminal);
    if accounting.total() != Some(completed_activity_count)
        || completed_activity_count > REMOVE_REPLICA_MAX_ACTIVITY_RECORDS as u64
    {
        return Err("native remove terminal accounting is not authoritative".to_string());
    }
    match &terminal {
        RemoveReplicaTerminal::Completed {
            commit_evidence,
            cleanup,
            ..
        } => {
            let attempt = commit_evidence
                .attempt_id
                .strip_prefix(&format!("{}:attempt-", initial.operation_id))
                .and_then(|value| value.parse::<u32>().ok())
                .filter(|attempt| {
                    (1..=kuberic_core::remove_replica::MAX_REMOVE_REPLICA_PRE_COMMIT_ATTEMPTS)
                        .contains(attempt)
                })
                .ok_or_else(|| "completed native remove changed attempt identity".to_string())?;
            if commit_evidence.action_id
                != format!(
                    "{}:attempt-{attempt}:RemoveReplicaIntent",
                    initial.operation_id
                )
                || AgentGeneration::parse(commit_evidence.primary_agent_generation.clone()).is_err()
            {
                return Err("completed native remove changed action authority".to_string());
            }
            validate_exact_configuration_membership(
                &commit_evidence.configuration_signature,
                &initial.target_snapshot,
            )?;
            validate_completed_cleanup(cleanup)?;
        }
        RemoveReplicaTerminal::Unsafe {
            committed,
            commit_evidence,
            ..
        } if *committed != commit_evidence.is_some() => {
            return Err("unsafe native remove has inconsistent commit evidence".to_string());
        }
        RemoveReplicaTerminal::IncompatibleContract {
            supported_version, ..
        } if *supported_version != REMOVE_REPLICA_CONTRACT_VERSION => {
            return Err(
                "native remove incompatible terminal changed supported version".to_string(),
            );
        }
        _ => {}
    }
    Ok(terminal)
}

fn validate_exact_configuration_membership(
    signature: &str,
    snapshot: &StablePartitionSnapshotStatus,
) -> Result<(), String> {
    let body = signature
        .strip_prefix(&format!("q{}[", snapshot.write_quorum))
        .and_then(|value| value.strip_suffix(']'))
        .ok_or_else(|| "completed native remove changed configuration authority".to_string())?;
    let members = if body.is_empty() {
        Vec::new()
    } else {
        body.split(',').collect::<Vec<_>>()
    };
    if !members.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err("completed native remove configuration is not canonical".to_string());
    }
    let member_count = members.len();
    let actual = members
        .into_iter()
        .map(|member| {
            let (replica_id, remainder) = member.split_once('@').ok_or_else(|| {
                "completed native remove configuration member is malformed".to_string()
            })?;
            let (instance_id, _) = remainder.split_once(':').ok_or_else(|| {
                "completed native remove configuration member is malformed".to_string()
            })?;
            let replica_id = replica_id.parse::<i64>().map_err(|_| {
                "completed native remove configuration member is malformed".to_string()
            })?;
            Ok((replica_id, instance_id.to_string()))
        })
        .collect::<Result<BTreeSet<_>, String>>()?;
    let expected = snapshot
        .members
        .iter()
        .filter(|member| member.id != snapshot.primary_id)
        .map(|member| (member.id, member.instance_id.clone()))
        .collect::<BTreeSet<_>>();
    if member_count != expected.len() || actual.len() != expected.len() || actual != expected {
        return Err("completed native remove changed configuration authority".to_string());
    }
    Ok(())
}

fn validate_completed_cleanup(cleanup: &RemoveReplicaCleanupStatus) -> Result<(), String> {
    if cleanup.connection_absent
        && matches!(
            cleanup.target_retirement,
            Some(
                TargetRetirementObservationStatus::Completed
                    | TargetRetirementObservationStatus::Unavailable
                    | TargetRetirementObservationStatus::Stale
                    | TargetRetirementObservationStatus::Failed
            )
        )
        && cleanup.target_labels_fenced
        && cleanup.target_pod_deleted
    {
        Ok(())
    } else {
        Err("completed native remove lacks exact cleanup evidence".to_string())
    }
}

fn encode_terminal(terminal: RemoveReplicaTerminal) -> TerminalOutcome {
    let succeeded = matches!(
        terminal,
        RemoveReplicaTerminal::Completed { .. } | RemoveReplicaTerminal::Compensated { .. }
    );
    match serde_json::to_vec(&terminal) {
        Ok(encoded) if validate_terminal_payload_bytes(encoded.len()).is_ok() => {
            if succeeded {
                TerminalOutcome::succeeded(encoded)
            } else {
                TerminalOutcome::failed(encoded)
            }
        }
        _ => TerminalOutcome::failed(
            br#"{"status":"rejected","message":"native remove terminal payload exceeded its bound","accounting":{"externalEffectCount":0,"passiveObservationCount":0}}"#
                .to_vec(),
        ),
    }
}

fn rejected_terminal(
    message: String,
    accounting: RemoveReplicaActivityAccounting,
) -> TerminalOutcome {
    encode_terminal(RemoveReplicaTerminal::Rejected {
        message: bounded_message(&message),
        accounting,
    })
}

fn incompatible_terminal(source: String, found_version: u32, message: String) -> TerminalOutcome {
    encode_terminal(RemoveReplicaTerminal::IncompatibleContract {
        source: bounded_message(&source),
        found_version,
        supported_version: REMOVE_REPLICA_CONTRACT_VERSION,
        message: bounded_message(&message),
        accounting: Default::default(),
    })
}

fn bounded_message(message: &str) -> String {
    message.chars().take(MAX_TERMINAL_MESSAGE_BYTES).collect()
}

enum WorkflowInputError {
    Incompatible {
        source: String,
        found_version: u32,
        message: String,
    },
    Rejected(String),
}

fn decode_workflow_input(bytes: &[u8]) -> Result<RemoveReplicaWorkflowInput, WorkflowInputError> {
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|error| {
        WorkflowInputError::Rejected(format!("decode native remove input: {error}"))
    })?;
    let version = value
        .get("contractVersion")
        .or_else(|| value.get("version"))
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u32::try_from(value).ok());
    if let Some(found_version) = version
        && found_version != REMOVE_REPLICA_CONTRACT_VERSION
    {
        let source = if value.get("initialOperation").is_some() {
            "legacy_pilot"
        } else {
            "native"
        };
        return Err(WorkflowInputError::Incompatible {
            source: source.to_string(),
            found_version,
            message: format!(
                "unsupported remove contract version {found_version}; supported {REMOVE_REPLICA_CONTRACT_VERSION}"
            ),
        });
    }
    serde_json::from_value(value).map_err(|error| {
        WorkflowInputError::Rejected(format!("decode native remove input: {error}"))
    })
}

fn classify_checkpoint_activity(activity: &ActivitySpec) -> Option<DurableActivityClass> {
    if activity.name().name() != REMOVE_REPLICA_ACTIVITY_NAME
        || activity.name().version() != REMOVE_REPLICA_ACTIVITY_VERSION
        || activity.max_result_bytes() != REMOVE_REPLICA_MAX_BOUNDARY_RESULT_BYTES
    {
        return None;
    }
    decode_boundary_input(activity.input())
        .ok()
        .map(|input| input.class())
}

fn decode_checkpoint_terminal_accounting(
    outcome: &TerminalOutcome,
    completed_activity_count: u64,
) -> Option<DurableActivityAccounting> {
    let terminal = decode_terminal(outcome).ok()?;
    let accounting = terminal_accounting(&terminal);
    (accounting.total() == Some(completed_activity_count)).then_some(DurableActivityAccounting {
        external_effect_count: accounting.external_effect_count,
        passive_observation_count: accounting.passive_observation_count,
    })
}

fn terminal_accounting(terminal: &RemoveReplicaTerminal) -> RemoveReplicaActivityAccounting {
    match terminal {
        RemoveReplicaTerminal::Completed { accounting, .. }
        | RemoveReplicaTerminal::Compensated { accounting, .. }
        | RemoveReplicaTerminal::Unsafe { accounting, .. }
        | RemoveReplicaTerminal::Rejected { accounting, .. }
        | RemoveReplicaTerminal::IncompatibleContract { accounting, .. } => *accounting,
    }
}

pub fn checkpoint_measurement_decoder() -> CheckpointMeasurementDecoder {
    CheckpointMeasurementDecoder::new(
        "remove-replica-native",
        classify_checkpoint_activity,
        decode_checkpoint_terminal_accounting,
    )
}

struct RemoveReplicaRunnerContext {
    observations: OperationObservations,
    pod_identities: OperationPodIdentities,
    target_role_label: Option<String>,
    handles: BTreeMap<i64, Box<dyn ReplicaHandle>>,
    addressed_instances: BTreeMap<i64, ReplicaInstanceId>,
}

async fn collect_runner_context(
    operation: &DurableOperationStatus,
    set: &KubericSet,
    api: &dyn ClusterApi,
    current_pods: &[(i64, ReplicaInstanceId, &Pod)],
) -> Result<RemoveReplicaRunnerContext, String> {
    let pod_identities = current_pods
        .iter()
        .map(|(replica_id, instance_id, _)| (*replica_id, instance_id.to_string()))
        .collect::<OperationPodIdentities>();
    let target_role_label = operation.target_replica_id.and_then(|target_id| {
        current_pods
            .iter()
            .find(|(replica_id, instance_id, _)| {
                *replica_id == target_id
                    && operation.target_pod_uid.as_deref() == Some(instance_id.as_str())
            })
            .and_then(|(_, _, pod)| {
                pod.metadata
                    .labels
                    .as_ref()
                    .and_then(|labels| labels.get("kuberic.io/role"))
            })
            .cloned()
    });
    let mut handles = BTreeMap::new();
    let mut observations = OperationObservations::new();
    for (replica_id, _, pod) in current_pods {
        if !operation
            .previous_snapshot
            .members
            .iter()
            .any(|member| member.id == *replica_id)
        {
            continue;
        }
        let Ok(handle) = api.create_replica_handle(*replica_id, pod, &set.spec).await else {
            continue;
        };
        match handle.get_status().await {
            Ok(status) => {
                observations.insert(
                    *replica_id,
                    ReplicaObservation {
                        status,
                        control_address: handle.control_address(),
                        replicator_address: handle.replicator_address(),
                        pod_name: pod.name_any(),
                        pod_role_label: pod
                            .metadata
                            .labels
                            .as_ref()
                            .and_then(|labels| labels.get("kuberic.io/role"))
                            .cloned(),
                    },
                );
            }
            Err(
                error @ (KubericError::RemoteControlProtocolUnsupported(_)
                | KubericError::RemoteAgentRequestRejected(_)),
            ) => {
                return Err(format!(
                    "replica {replica_id} has unsupported or malformed control status during native remove: {error}"
                ));
            }
            Err(_) => {}
        }
        handles.insert(*replica_id, handle);
    }
    let addressed_instances = handles
        .iter()
        .map(|(replica_id, handle)| (*replica_id, handle.instance_id()))
        .collect();
    Ok(RemoveReplicaRunnerContext {
        observations,
        pod_identities,
        target_role_label,
        handles,
        addressed_instances,
    })
}

pub struct FrameworkNativeRemoveReplicaAdapter<'a> {
    reference: &'a RemoveReplicaExecution,
    set: &'a KubericSet,
    current_pods: &'a [(i64, ReplicaInstanceId, &'a Pod)],
    api: &'a dyn ClusterApi,
    namespace: String,
    operation: DurableOperationStatus,
    operation_history: Vec<DurableOperationStatus>,
    resolver: RemoveReplicaPreparedActivityResolver,
    context: Option<RemoveReplicaRunnerContext>,
    now: i64,
}

impl<'a> FrameworkNativeRemoveReplicaAdapter<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        reference: &'a RemoveReplicaExecution,
        set: &'a KubericSet,
        current_pods: &'a [(i64, ReplicaInstanceId, &'a Pod)],
        api: &'a dyn ClusterApi,
        now: i64,
    ) -> Result<Self, String> {
        let operation =
            advance_to_boundary(reconstruct_initial_operation(admission_input(reference)?)?)?;
        let empty_observations = OperationObservations::new();
        let empty_pods = OperationPodIdentities::new();
        let empty_instances = BTreeMap::new();
        Ok(Self {
            reference,
            set,
            current_pods,
            api,
            namespace: set.namespace().unwrap_or_default(),
            resolver: RemoveReplicaPreparedActivityResolver::new(
                &operation,
                &empty_observations,
                &empty_pods,
                None,
                &empty_instances,
                now,
            ),
            operation,
            operation_history: Vec::new(),
            context: None,
            now,
        })
    }

    fn context(&self) -> Result<&RemoveReplicaRunnerContext, String> {
        self.context
            .as_ref()
            .ok_or_else(|| "native remove adapter was not prepared".to_string())
    }

    fn evidence(&self) -> Result<RemoveReplicaObservationEvidence, String> {
        let context = self.context()?;
        RemoveReplicaObservationEvidence::capture(
            &self.operation,
            &context.observations,
            &context.pod_identities,
            context.target_role_label.as_deref(),
            self.now,
        )
    }

    fn observation(
        &mut self,
        activity: LogicalActivityId,
        result: RemoveReplicaBoundaryResult,
        force_next_observation: bool,
        stop_for_fresh_authority: bool,
    ) -> DurableAdapterBoundary {
        let encoded = match encode_boundary_result(&result) {
            Ok(encoded) => encoded,
            Err(error) => return DurableAdapterBoundary::Isolated(error),
        };
        match operation_after_boundary_result(&self.operation, &result) {
            Ok(operation) => self.operation = operation,
            Err(error) => return DurableAdapterBoundary::Isolated(error),
        }
        self.operation_history.push(self.operation.clone());
        let context = match self.context() {
            Ok(context) => context,
            Err(error) => return DurableAdapterBoundary::Isolated(error),
        };
        self.resolver = RemoveReplicaPreparedActivityResolver::new(
            &self.operation,
            &context.observations,
            &context.pod_identities,
            context.target_role_label.as_deref(),
            &context.addressed_instances,
            self.now,
        )
        .with_history(self.operation_history.clone())
        .force_observation(force_next_observation);
        let observation = ActivityObservation::new(activity, encoded);
        if force_next_observation {
            DurableAdapterBoundary::ObserveAndProgressThenWait {
                observation: Box::new(observation),
                reason: "RefreshingAuthority".to_string(),
                detail: "native remove persisted effect evidence and exposed a passive boundary for fresh authority before another command".to_string(),
                requeue_after_seconds: 1,
            }
        } else if stop_for_fresh_authority {
            DurableAdapterBoundary::ObserveAndWait {
                observation: Box::new(observation),
                reason: "RefreshingAuthority".to_string(),
                detail: "native remove persisted effect evidence and requires a fresh authority observation before another command".to_string(),
                requeue_after_seconds: 1,
            }
        } else {
            DurableAdapterBoundary::Observed(Box::new(observation))
        }
    }

    fn wait(&self, reason: &str, detail: &str) -> DurableAdapterBoundary {
        DurableAdapterBoundary::Wait {
            reason: reason.to_string(),
            detail: detail.to_string(),
        }
    }

    fn validate_activity(
        &self,
        activity: &LogicalActivityId,
    ) -> Result<RemoveReplicaBoundaryInput, String> {
        let input = decode_boundary_input(activity.input())?;
        if input == logical_boundary(&self.operation) {
            return Ok(input);
        }
        validate_prepared_activity(&self.operation, &input)?;
        Ok(input)
    }
}

#[async_trait]
impl DurableOperationAdapter for FrameworkNativeRemoveReplicaAdapter<'_> {
    type Resolver = RemoveReplicaPreparedActivityResolver;
    type Terminal = RemoveReplicaTerminal;
    type Publication = RemoveReplicaTerminal;

    fn resolver(&self) -> &Self::Resolver {
        &self.resolver
    }

    fn restore(
        &mut self,
        checkpoint: Option<&CheckpointPayload>,
    ) -> Result<(), DurableAdapterBoundary> {
        let (operation, history) = match checkpoint {
            Some(payload) => replay_active_operation_history(
                self.reference,
                payload.active_activities().unwrap_or_default(),
            )
            .map_err(DurableAdapterBoundary::Isolated)?,
            None => {
                let admission =
                    admission_input(self.reference).map_err(DurableAdapterBoundary::Rejected)?;
                let operation = advance_to_boundary(
                    reconstruct_initial_operation(admission)
                        .map_err(DurableAdapterBoundary::Rejected)?,
                )
                .map_err(DurableAdapterBoundary::Isolated)?;
                (operation.clone(), vec![operation])
            }
        };
        self.operation = operation;
        self.operation_history = history;
        Ok(())
    }

    async fn prepare(&mut self) -> Result<(), DurableAdapterBoundary> {
        let context =
            collect_runner_context(&self.operation, self.set, self.api, self.current_pods)
                .await
                .map_err(DurableAdapterBoundary::Isolated)?;
        self.resolver = RemoveReplicaPreparedActivityResolver::new(
            &self.operation,
            &context.observations,
            &context.pod_identities,
            context.target_role_label.as_deref(),
            &context.addressed_instances,
            self.now,
        )
        .with_history(self.operation_history.clone());
        self.context = Some(context);
        Ok(())
    }

    async fn observe_or_dispatch(
        &mut self,
        activity: &LogicalActivityId,
        attempt_id: kuberic_durable_execution::AttemptId,
        permit: &mut DurablePermitGuard,
    ) -> DurableAdapterBoundary {
        let prepared = match self.validate_activity(activity) {
            Ok(prepared) => prepared,
            Err(error) => return DurableAdapterBoundary::Isolated(error),
        };
        let expected = match activity_spec(&prepared) {
            Ok(expected) => expected,
            Err(error) => return DurableAdapterBoundary::Isolated(error),
        };
        if let Err(error) = permit.consume(&expected, activity, attempt_id, "remove-replica-native")
        {
            return DurableAdapterBoundary::Isolated(error);
        }
        match prepared {
            RemoveReplicaBoundaryInput::Observe { .. } => {
                let evidence = match self.evidence() {
                    Ok(evidence) => evidence,
                    Err(error) => return DurableAdapterBoundary::Isolated(error),
                };
                self.observation(
                    activity.clone(),
                    RemoveReplicaBoundaryResult::Observation { evidence },
                    false,
                    false,
                )
            }
            RemoveReplicaBoundaryInput::ReplicaCommand { command } => {
                let exact = match command.effect_command() {
                    Ok(exact) => exact,
                    Err(error) => return DurableAdapterBoundary::Isolated(error),
                };
                let context = match self.context() {
                    Ok(context) => context,
                    Err(error) => return DurableAdapterBoundary::Isolated(error),
                };
                let Some(handle) = context.handles.get(&exact.target_id) else {
                    return self.wait(
                        "AwaitingAuthoritativeObservation",
                        "native remove exact replica target is not currently addressable",
                    );
                };
                if handle.instance_id().as_str() != exact.target_instance_id {
                    return self.wait(
                        "AwaitingFreshAuthority",
                        "native remove exact replica incarnation changed before dispatch",
                    );
                }
                match execute_replica_command(handle.as_ref(), &exact).await {
                    Ok(()) => self.wait(
                        "EffectExposed",
                        "native remove replica effect was exposed and awaits authoritative observation",
                    ),
                    Err(error)
                        if classify_dispatch_failure(&error)
                            == DispatchFailureDisposition::ProvenNoAdmission =>
                    {
                        let evidence = match self.evidence() {
                            Ok(evidence) => evidence,
                            Err(message) => return DurableAdapterBoundary::Isolated(message),
                        };
                        self.observation(
                            activity.clone(),
                            RemoveReplicaBoundaryResult::ProvenNoAdmission {
                                action_id: exact.action_id,
                                redelivery: 1,
                                evidence,
                            },
                            false,
                            dispatch_rejection_requires_refresh(&error),
                        )
                    }
                    Err(error)
                        if classify_dispatch_failure(&error)
                            == DispatchFailureDisposition::DefiniteFailure =>
                    {
                        let result = RemoveReplicaBoundaryResult::Effect {
                            outcome: DurableEffectOutcome::definite_failure(
                                "dispatch",
                                &error.to_string(),
                            ),
                        };
                        self.observation(activity.clone(), result, false, true)
                    }
                    Err(_) => self.wait(
                        "EffectExposed",
                        "native remove replica effect outcome is uncertain and requires authoritative observation",
                    ),
                }
            }
            RemoveReplicaBoundaryInput::LabelCommand { command } => {
                execute_label_command(self.api, &self.namespace, &command).await;
                self.wait(
                    "EffectExposed",
                    "native remove UID-fenced label effect was exposed and awaits observation",
                )
            }
            RemoveReplicaBoundaryInput::DeleteCommand { command } => {
                execute_delete_command(self.api, &self.namespace, &command).await;
                self.wait(
                    "EffectExposed",
                    "native remove UID-fenced delete effect was exposed and awaits observation",
                )
            }
        }
    }

    async fn resolve_quarantine(
        &mut self,
        activity: LogicalActivityId,
        _attempt_id: kuberic_durable_execution::AttemptId,
    ) -> DurableAdapterBoundary {
        let prepared = match self.validate_activity(&activity) {
            Ok(prepared) => prepared,
            Err(error) => return DurableAdapterBoundary::Isolated(error),
        };
        let evidence = match self.evidence() {
            Ok(evidence) => evidence,
            Err(error) => return DurableAdapterBoundary::Isolated(error),
        };
        match prepared {
            RemoveReplicaBoundaryInput::Observe { .. } => self.observation(
                activity,
                RemoveReplicaBoundaryResult::Observation { evidence },
                false,
                false,
            ),
            RemoveReplicaBoundaryInput::ReplicaCommand { command } => {
                let exact = match command.effect_command() {
                    Ok(exact) => exact,
                    Err(error) => return DurableAdapterBoundary::Isolated(error),
                };
                let context = match self.context() {
                    Ok(context) => context,
                    Err(error) => return DurableAdapterBoundary::Isolated(error),
                };
                if context
                    .observations
                    .get(&exact.target_id)
                    .and_then(|observation| {
                        correlated_action_observation(&observation.status, exact.action_id.as_str())
                    })
                    .is_some()
                {
                    return self.observation(
                        activity,
                        RemoveReplicaBoundaryResult::Effect {
                            outcome: DurableEffectOutcome::Applied(evidence),
                        },
                        true,
                        false,
                    );
                }
                if generation_change_proves_no_admission(
                    &self.operation,
                    exact.target_id,
                    &context.observations,
                ) {
                    return self.observation(
                        activity,
                        RemoveReplicaBoundaryResult::ProvenNoAdmission {
                            action_id: exact.action_id,
                            redelivery: 1,
                            evidence,
                        },
                        false,
                        true,
                    );
                }
                self.wait(
                    "Quarantined",
                    "native remove exposed replica effect remains quarantined pending exact correlated evidence",
                )
            }
            RemoveReplicaBoundaryInput::LabelCommand { command } => {
                let context = match self.context() {
                    Ok(context) => context,
                    Err(error) => return DurableAdapterBoundary::Isolated(error),
                };
                if remove_label_postcondition_satisfied(
                    &command,
                    &context.pod_identities,
                    context.target_role_label.as_deref(),
                ) {
                    self.observation(
                        activity,
                        RemoveReplicaBoundaryResult::Effect {
                            outcome: DurableEffectOutcome::Applied(evidence),
                        },
                        false,
                        false,
                    )
                } else {
                    self.wait(
                        "Quarantined",
                        "native remove UID-fenced label effect awaits its exact postcondition",
                    )
                }
            }
            RemoveReplicaBoundaryInput::DeleteCommand { command } => {
                let context = match self.context() {
                    Ok(context) => context,
                    Err(error) => return DurableAdapterBoundary::Isolated(error),
                };
                if remove_delete_postcondition_satisfied(&command, &context.pod_identities) {
                    self.observation(
                        activity,
                        RemoveReplicaBoundaryResult::Effect {
                            outcome: DurableEffectOutcome::Applied(evidence),
                        },
                        false,
                        false,
                    )
                } else {
                    self.wait(
                        "Quarantined",
                        "native remove UID-fenced delete effect awaits exact UID absence",
                    )
                }
            }
        }
    }

    fn deadline_unix_seconds(&self) -> i64 {
        self.operation
            .pending_action
            .as_ref()
            .map(|pending| pending.deadline_unix_seconds)
            .unwrap_or(self.operation.phase_deadline_unix_seconds)
    }

    fn preparation_wait(&self, error: &CheckpointError) -> DurableAdapterWait {
        DurableAdapterWait {
            reason: "AwaitingFreshAuthority".to_string(),
            detail: format!(
                "native remove cannot prepare the next exact effect until epoch, incarnation, UID, role, control-version, generation, and configuration authority are current: {error}"
            ),
            requeue_after_seconds: Some(1),
        }
    }

    fn checkpoint_disposition(&self, error: &CheckpointError) -> DurableCheckpointDisposition {
        match error {
            CheckpointError::UnsupportedFormat { .. }
            | CheckpointError::WorkflowInputMismatch { .. }
            | CheckpointError::TerminalPayloadBoundMismatch { .. }
            | CheckpointError::ConfiguredCapacityBelowAdmission { .. }
            | CheckpointError::ConfiguredCapacityAboveAdmission { .. }
            | CheckpointError::TerminalEncodedCheckpointCapacityMismatch { .. } => {
                DurableCheckpointDisposition::Incompatible
            }
            CheckpointError::ExecutionMismatch { .. } => DurableCheckpointDisposition::Isolated,
            _ => DurableCheckpointDisposition::Rejected,
        }
    }

    fn validate_terminal(
        &mut self,
        outcome: TerminalOutcome,
        completed_activity_count: u64,
    ) -> Result<Self::Terminal, DurableAdapterBoundary> {
        validate_loaded_terminal(self.reference, &outcome, completed_activity_count)
            .map_err(DurableAdapterBoundary::Rejected)
    }

    fn publication_handoff(&mut self, terminal: Self::Terminal) -> Self::Publication {
        terminal
    }
}

fn encode_execution_id(execution_id: ExecutionId) -> String {
    let mut encoded = String::with_capacity(32);
    for byte in execution_id.as_bytes() {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn decode_execution_id(value: &str) -> Result<[u8; 16], String> {
    if value.len() != 32 {
        return Err(
            "native remove execution ID must contain 32 hexadecimal characters".to_string(),
        );
    }
    let mut bytes = [0_u8; 16];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (decode_hex(pair[0])? << 4) | decode_hex(pair[1])?;
    }
    Ok(bytes)
}

fn decode_hex(value: u8) -> Result<u8, String> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err("native remove execution ID must be lowercase hexadecimal".to_string()),
    }
}

#[cfg(test)]
mod remove_replica_execution_tests {
    use std::sync::{Arc, Mutex as StdMutex};

    use async_trait::async_trait;
    use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod, Service};
    use kube::api::ObjectMeta;
    use kuberic_core::driver::ReplicaHandle;
    use kuberic_core::error::KubericError;
    use kuberic_core::replica_lifecycle::REPLICA_LIFECYCLE_PEER_PROTOCOL_VERSION;
    use kuberic_core::types::{
        CorrelatedControlActionAcknowledgement, CorrelatedControlActionRequest, Lsn, ReplicaId,
    };
    use kuberic_durable_execution::{
        ActivityCallError, ActivityState, CasOutcome, CheckpointError, CheckpointState,
        CheckpointStore, DispatchPermit, Evaluation, HostEpoch, HostOutcome,
        InMemoryCheckpointStore, PersistenceBoundary, PreparedActivityError,
        encode_activity_result, evaluate,
    };
    use serde::de::DeserializeOwned;

    use crate::crd::{
        KubericSetSpec, KubericSetStatus, MemberStatus, Phase, PvcRetentionPolicy,
        StableReplicaElectionMetadataStatus, StableReplicaRoleStatus, StableReplicaSnapshotStatus,
    };

    use super::super::checkpoint_store::{DurableCheckpointStore, MeasuredDurableCheckpointStore};
    use super::super::runner::{
        DurableActiveReason, DurableOperationAdapter, DurableRunner, DurableRunnerOutcome,
    };
    use super::*;

    fn generation(id: i64) -> AgentGeneration {
        AgentGeneration::parse(format!("{id:032x}")).unwrap()
    }

    fn snapshot() -> StablePartitionSnapshotStatus {
        StablePartitionSnapshotStatus {
            epoch: EpochStatus {
                data_loss_number: 1,
                configuration_number: 7,
            },
            primary_id: 1,
            members: vec![
                StableReplicaSnapshotStatus {
                    id: 1,
                    instance_id: "one".to_string(),
                    role: StableReplicaRoleStatus::Primary,
                    election_metadata: None,
                },
                StableReplicaSnapshotStatus {
                    id: 2,
                    instance_id: "two".to_string(),
                    role: StableReplicaRoleStatus::ActiveSecondary,
                    election_metadata: None,
                },
                StableReplicaSnapshotStatus {
                    id: 3,
                    instance_id: "three".to_string(),
                    role: StableReplicaRoleStatus::ActiveSecondary,
                    election_metadata: None,
                },
            ],
            write_quorum: 2,
        }
    }

    fn target() -> RemoveReplicaTarget {
        RemoveReplicaTarget {
            replica_id: 3,
            pod_name: "set-2".to_string(),
            pod_uid: "three".to_string(),
            replicator_address: "http://three:9091".to_string(),
            agent_generation: Some(generation(3)),
        }
    }

    fn reference() -> RemoveReplicaExecution {
        new_execution(
            "set-uid",
            snapshot(),
            target(),
            DurableRemoveMode::ScaleDown,
            2,
            10,
        )
        .unwrap()
    }

    fn force_reference() -> RemoveReplicaExecution {
        let mut target = target();
        target.agent_generation = None;
        new_execution(
            "set-uid",
            snapshot(),
            target,
            DurableRemoveMode::Force,
            2,
            10,
        )
        .unwrap()
    }

    fn configuration(
        snapshot: &StablePartitionSnapshotStatus,
        mode: ReplicaConfigurationMode,
    ) -> ReplicaConfigurationStatus {
        ReplicaConfigurationStatus {
            mode,
            members: snapshot
                .members
                .iter()
                .filter(|member| member.id != snapshot.primary_id)
                .map(|member| ReplicaConfigurationMemberStatus {
                    id: member.id,
                    instance_id: ReplicaInstanceId::new(member.instance_id.clone()),
                    role: Role::ActiveSecondary,
                })
                .collect(),
            write_quorum: snapshot.write_quorum,
        }
    }

    fn observation(
        id: i64,
        instance: &str,
        role: Role,
        configuration: Option<ReplicaConfigurationStatus>,
    ) -> ReplicaObservation {
        ReplicaObservation {
            status: ReplicaStatusInfo {
                instance_id: ReplicaInstanceId::new(instance),
                role,
                epoch: Epoch::new(1, 7),
                current_progress: 10,
                catch_up_capability: Some(10),
                committed_lsn: 10,
                healthy: true,
                write_status: if role == Role::Primary {
                    AccessStatus::Granted
                } else {
                    AccessStatus::NotPrimary
                },
                configuration,
                election_configuration: None,
                deactivation_info: None,
                active_replica_connections: if role == Role::Primary {
                    vec![
                        ReplicaConnectionStatus {
                            id: 2,
                            instance_id: ReplicaInstanceId::new("two"),
                        },
                        ReplicaConnectionStatus {
                            id: 3,
                            instance_id: ReplicaInstanceId::new("three"),
                        },
                    ]
                } else {
                    Vec::new()
                },
                build_observation: None,
                agent: ReplicaAgentStatus {
                    protocol_version:
                        kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION,
                    lifecycle_peer_protocol_version: REPLICA_LIFECYCLE_PEER_PROTOCOL_VERSION,
                    generation: generation(id),
                    control_version: AgentControlVersion::new(11),
                    current_action: None,
                    retained_terminal_actions: Vec::new(),
                    local_faults: Vec::new(),
                },
            },
            control_address: format!("http://{instance}:9090"),
            replicator_address: format!("http://{instance}:9091"),
            pod_name: format!("set-{}", id - 1),
            pod_role_label: Some(
                if role == Role::Primary {
                    "primary"
                } else {
                    "secondary"
                }
                .to_string(),
            ),
        }
    }

    fn observations(operation: &DurableOperationStatus) -> OperationObservations {
        BTreeMap::from([
            (
                1,
                observation(
                    1,
                    "one",
                    Role::Primary,
                    Some(configuration(
                        &operation.previous_snapshot,
                        ReplicaConfigurationMode::Current,
                    )),
                ),
            ),
            (2, observation(2, "two", Role::ActiveSecondary, None)),
            (3, observation(3, "three", Role::ActiveSecondary, None)),
        ])
    }

    fn pod_identities() -> OperationPodIdentities {
        BTreeMap::from([
            (1, "one".to_string()),
            (2, "two".to_string()),
            (3, "three".to_string()),
        ])
    }

    fn freeze_and_dispatch() -> (DurableOperationStatus, OperationObservations) {
        freeze_and_dispatch_for(&reference())
    }

    fn freeze_and_dispatch_for(
        reference: &RemoveReplicaExecution,
    ) -> (DurableOperationStatus, OperationObservations) {
        let initial = reconstruct_initial_operation(reference.input.as_ref().unwrap()).unwrap();
        let observations = observations(&initial);
        let Decision::Persist(frozen) = decide_remove_replica(
            &initial,
            &observations,
            &pod_identities(),
            Some("secondary"),
            10,
        )
        .unwrap() else {
            panic!("expected frozen remove intent");
        };
        let Decision::Persist(dispatched) = decide_remove_replica(
            &frozen,
            &observations,
            &pod_identities(),
            Some("secondary"),
            10,
        )
        .unwrap() else {
            panic!("expected remove dispatch state");
        };
        (dispatched, observations)
    }

    fn completed_action(
        operation: &DurableOperationStatus,
        action_id: String,
    ) -> CorrelatedActionObservation {
        let intent = operation.remove_intent.as_ref().unwrap();
        CorrelatedActionObservation {
            generation: generation(1),
            control_version: AgentControlVersion::new(11),
            action: DurableActionObservation {
                action_id,
                signature: intent.input_signature.clone(),
                state: DurableActionState::Completed,
                error_class: None,
                error: None,
                result: Some(DurableActionResult::RemoveReplica(
                    RemoveReplicaTerminalResult::CommittedClean,
                )),
                add_replica_progress: None,
                remove_replica_progress: Some(RemoveReplicaProgress {
                    phase: RemoveReplicaCoordinatorPhase::Attesting,
                    attempt_id: intent.attempt_id.clone(),
                    commit_observed: true,
                    commit_observed_unix_seconds: Some(10),
                    connection_absent: true,
                    target_retirement: TargetRetirementObservation::Completed,
                    retirement_expiry_unix_seconds: Some(
                        core_intent(operation)
                            .unwrap()
                            .retirement_expiry(10)
                            .unwrap(),
                    ),
                    compensation_expiry_unix_seconds: None,
                    error: None,
                    current_install_dispatched: true,
                }),
            },
        }
    }

    fn committed_operation() -> DurableOperationStatus {
        committed_operation_for(&reference())
    }

    fn committed_operation_for(reference: &RemoveReplicaExecution) -> DurableOperationStatus {
        let (mut operation, _) = freeze_and_dispatch_for(reference);
        let intent = operation.remove_intent.as_ref().unwrap();
        operation.phase = DurableOperationPhase::Completed;
        operation.pending_action = None;
        operation.committed_snapshot = Some(operation.target_snapshot.clone());
        operation.remove_commit_evidence = Some(RemoveReplicaCommitEvidenceStatus {
            attempt_id: intent.attempt_id.clone(),
            action_id: intent.action_id.clone(),
            primary_agent_generation: intent.primary_agent_generation.clone(),
            configuration_signature: core_intent(&operation)
                .unwrap()
                .reduced_current_configuration
                .signature(),
            observed_unix_seconds: 10,
        });
        operation.remove_cleanup = Some(RemoveReplicaCleanupStatus {
            connection_absent: true,
            target_retirement: Some(TargetRetirementObservationStatus::Completed),
            target_labels_fenced: true,
            target_pod_deleted: true,
        });
        operation
    }

    fn assert_unknown_field_rejected<T>(value: &serde_json::Value, pointer: &str)
    where
        T: DeserializeOwned,
    {
        let mut changed = value.clone();
        changed
            .pointer_mut(pointer)
            .unwrap_or_else(|| panic!("missing JSON pointer {pointer}"))
            .as_object_mut()
            .unwrap_or_else(|| panic!("JSON pointer {pointer} is not an object"))
            .insert("unknown".to_string(), serde_json::Value::Bool(true));
        assert!(
            serde_json::from_value::<T>(changed).is_err(),
            "{pointer} accepted an unknown field"
        );
    }

    fn payload(spec: &ExecutionSpec, activities: Vec<ActivityRecord>) -> CheckpointEnvelope {
        CheckpointEnvelope::encode_with_limits(
            &CheckpointPayload::active(
                ExecutionContract::with_encoded_limits(
                    spec.clone(),
                    REMOVE_REPLICA_MAX_ACTIVE_ENCODED_BYTES as u64,
                    REMOVE_REPLICA_MAX_TERMINAL_ENCODED_BYTES as u64,
                ),
                activities,
            ),
            checkpoint_limits(),
        )
        .unwrap()
    }

    #[derive(Clone, Copy)]
    enum DispatchResult {
        Busy,
        Ambiguous,
    }

    struct AdapterTestHandle {
        replica_id: ReplicaId,
        status: ReplicaStatusInfo,
        requests: Arc<StdMutex<Vec<CorrelatedControlActionRequest>>>,
        dispatch_result: DispatchResult,
    }

    #[async_trait]
    impl ReplicaHandle for AdapterTestHandle {
        fn id(&self) -> ReplicaId {
            self.replica_id
        }

        fn instance_id(&self) -> ReplicaInstanceId {
            self.status.instance_id.clone()
        }

        fn current_progress(&self) -> Lsn {
            self.status.current_progress
        }

        fn catch_up_capability(&self) -> Lsn {
            self.status.catch_up_capability.unwrap_or_default()
        }

        fn control_address(&self) -> String {
            format!("http://{}:9090", self.status.instance_id)
        }

        fn replicator_address(&self) -> String {
            format!("http://{}:9091", self.status.instance_id)
        }

        async fn get_status(&self) -> kuberic_core::Result<ReplicaStatusInfo> {
            Ok(self.status.clone())
        }

        async fn execute_correlated_control_action(
            &self,
            request: CorrelatedControlActionRequest,
        ) -> kuberic_core::Result<CorrelatedControlActionAcknowledgement> {
            self.requests.lock().unwrap().push(request);
            match self.dispatch_result {
                DispatchResult::Busy => Err(KubericError::AgentBusy),
                DispatchResult::Ambiguous => Err(KubericError::Internal(Box::new(
                    std::io::Error::other("ambiguous test dispatch"),
                ))),
            }
        }
    }

    struct AdapterTestApi {
        statuses: Arc<StdMutex<BTreeMap<ReplicaId, ReplicaStatusInfo>>>,
        requests: Arc<StdMutex<Vec<CorrelatedControlActionRequest>>>,
        dispatch_result: DispatchResult,
    }

    #[async_trait]
    impl ClusterApi for AdapterTestApi {
        async fn list_pods(&self, _: &str, _: &str) -> Result<Vec<Pod>, String> {
            unreachable!()
        }

        async fn create_pod(&self, _: &str, _: &Pod) -> Result<(), String> {
            unreachable!()
        }

        async fn delete_pod(&self, _: &str, _: &str, _: &str) -> Result<(), String> {
            Ok(())
        }

        async fn patch_pod_labels(
            &self,
            _: &str,
            _: &str,
            _: BTreeMap<String, String>,
        ) -> Result<(), String> {
            Ok(())
        }

        async fn patch_pod_labels_if_uid(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: BTreeMap<String, String>,
        ) -> Result<(), String> {
            Ok(())
        }

        async fn patch_set_status(
            &self,
            _: &str,
            _: &str,
            _: &KubericSetStatus,
            _: Option<&str>,
        ) -> Result<(), String> {
            unreachable!()
        }

        async fn create_replica_handle(
            &self,
            replica_id: ReplicaId,
            _: &Pod,
            _: &KubericSetSpec,
        ) -> Result<Box<dyn ReplicaHandle>, String> {
            Ok(Box::new(AdapterTestHandle {
                replica_id,
                status: self
                    .statuses
                    .lock()
                    .unwrap()
                    .get(&replica_id)
                    .unwrap()
                    .clone(),
                requests: self.requests.clone(),
                dispatch_result: self.dispatch_result,
            }))
        }

        async fn get_pvc(&self, _: &str, _: &str) -> Result<PersistentVolumeClaim, String> {
            unreachable!()
        }

        async fn create_pvc(&self, _: &str, _: &PersistentVolumeClaim) -> Result<(), String> {
            unreachable!()
        }

        async fn list_pvcs(&self, _: &str, _: &str) -> Result<Vec<PersistentVolumeClaim>, String> {
            unreachable!()
        }

        async fn delete_pvc(&self, _: &str, _: &str) -> Result<(), String> {
            unreachable!()
        }

        async fn get_service(&self, _: &str, _: &str) -> Result<Service, String> {
            unreachable!()
        }

        async fn create_service(&self, _: &str, _: &Service) -> Result<(), String> {
            unreachable!()
        }

        async fn delete_service(&self, _: &str, _: &str) -> Result<(), String> {
            unreachable!()
        }
    }

    fn adapter_set(reference: &RemoveReplicaExecution) -> KubericSet {
        KubericSet {
            metadata: ObjectMeta {
                name: Some("set".to_string()),
                namespace: Some("default".to_string()),
                uid: Some("set-uid".to_string()),
                ..Default::default()
            },
            spec: KubericSetSpec {
                replicas: 2,
                min_replicas: 1,
                image: "test:latest".to_string(),
                failover_delay: 0,
                switchover_delay: 30,
                port: 8080,
                control_port: 9090,
                data_port: 9091,
                storage: "256Mi".to_string(),
                pvc_retention_policy: PvcRetentionPolicy::Delete,
            },
            status: Some(KubericSetStatus {
                phase: Phase::RemovingReplica,
                members: vec![
                    MemberStatus {
                        name: "set-0".to_string(),
                        id: 1,
                        instance_id: "one".to_string(),
                        role: "primary".to_string(),
                        current_progress: 10,
                        healthy: true,
                        control_address: "http://one:9090".to_string(),
                        data_address: "http://one:9091".to_string(),
                    },
                    MemberStatus {
                        name: "set-1".to_string(),
                        id: 2,
                        instance_id: "two".to_string(),
                        role: "secondary".to_string(),
                        current_progress: 10,
                        healthy: true,
                        control_address: "http://two:9090".to_string(),
                        data_address: "http://two:9091".to_string(),
                    },
                    MemberStatus {
                        name: "set-2".to_string(),
                        id: 3,
                        instance_id: "three".to_string(),
                        role: "secondary".to_string(),
                        current_progress: 10,
                        healthy: true,
                        control_address: "http://three:9090".to_string(),
                        data_address: "http://three:9091".to_string(),
                    },
                ],
                stable_snapshot: Some(reference.input.as_ref().unwrap().previous_snapshot.clone()),
                remove_replica_execution: Some(reference.clone()),
                ..Default::default()
            }),
        }
    }

    fn adapter_pods() -> Vec<Pod> {
        [
            (1, "one", "primary"),
            (2, "two", "secondary"),
            (3, "three", "secondary"),
        ]
        .into_iter()
        .map(|(id, uid, role)| Pod {
            metadata: ObjectMeta {
                name: Some(format!("set-{}", id - 1)),
                namespace: Some("default".to_string()),
                uid: Some(uid.to_string()),
                labels: Some(BTreeMap::from([
                    ("kuberic.io/pod-index".to_string(), (id - 1).to_string()),
                    ("kuberic.io/role".to_string(), role.to_string()),
                ])),
                ..Default::default()
            },
            ..Default::default()
        })
        .collect()
    }

    fn adapter_current_pods(pods: &[Pod]) -> Vec<(i64, ReplicaInstanceId, &Pod)> {
        pods.iter()
            .enumerate()
            .map(|(index, pod)| {
                (
                    i64::try_from(index).unwrap() + 1,
                    ReplicaInstanceId::new(pod.metadata.uid.clone().unwrap()),
                    pod,
                )
            })
            .collect()
    }

    fn adapter_api(operation: &DurableOperationStatus, result: DispatchResult) -> AdapterTestApi {
        AdapterTestApi {
            statuses: Arc::new(StdMutex::new(
                observations(operation)
                    .into_iter()
                    .map(|(id, observation)| (id, observation.status))
                    .collect(),
            )),
            requests: Arc::new(StdMutex::new(Vec::new())),
            dispatch_result: result,
        }
    }

    fn adapter_store(
        execution: &ExecutionSpec,
        backend: InMemoryCheckpointStore,
    ) -> MeasuredDurableCheckpointStore {
        MeasuredDurableCheckpointStore::with_native_remove_decoder(
            execution.execution_id(),
            DurableCheckpointStore::InMemory(backend),
        )
    }

    fn native_host(
        execution: &ExecutionSpec,
        backend: InMemoryCheckpointStore,
        epoch: u8,
    ) -> RemoveReplicaHost {
        RemoveReplicaHost::new(
            adapter_store(execution, backend),
            HostEpoch::from_bytes([epoch; 16]),
            checkpoint_limits(),
        )
    }

    async fn active_history(
        backend: &InMemoryCheckpointStore,
        execution: &ExecutionSpec,
    ) -> Vec<ActivityRecord> {
        let stored = backend
            .load(execution.execution_id())
            .await
            .unwrap()
            .unwrap();
        stored
            .checkpoint()
            .decode_and_validate(execution, checkpoint_limits())
            .unwrap()
            .active_activities()
            .unwrap()
            .to_vec()
    }

    async fn assert_native_exposed_history(
        backend: &InMemoryCheckpointStore,
        execution: &ExecutionSpec,
        permit: &DispatchPermit,
        expected_specs: &[ActivitySpec],
    ) {
        let activities = active_history(backend, execution).await;
        assert_eq!(activities.len(), expected_specs.len());
        for (record, expected) in activities.iter().zip(expected_specs) {
            assert_eq!(record.spec(), expected);
        }
        assert!(
            activities[..activities.len() - 1]
                .iter()
                .all(|record| matches!(record.state(), ActivityState::Completed { .. }))
        );
        let exposed = activities.last().unwrap();
        assert_eq!(
            exposed.logical_id(execution.execution_id()),
            *permit.activity()
        );
        assert!(matches!(
            exposed.state(),
            ActivityState::DispatchExposed { attempt_id }
                if *attempt_id == permit.attempt_id()
        ));
    }

    async fn assert_native_completed_history(
        backend: &InMemoryCheckpointStore,
        execution: &ExecutionSpec,
        expected_specs: &[ActivitySpec],
    ) -> usize {
        let stored = backend
            .load(execution.execution_id())
            .await
            .unwrap()
            .unwrap();
        let encoded_len = stored.checkpoint().encoded_len().unwrap();
        let activities = stored
            .checkpoint()
            .decode_and_validate(execution, checkpoint_limits())
            .unwrap()
            .active_activities()
            .unwrap()
            .to_vec();
        assert_eq!(activities.len(), expected_specs.len());
        for (record, expected) in activities.iter().zip(expected_specs) {
            assert_eq!(record.spec(), expected);
            assert!(matches!(record.state(), ActivityState::Completed { .. }));
        }
        encoded_len
    }

    async fn restart_native_into_quarantine(
        backend: &InMemoryCheckpointStore,
        execution: &ExecutionSpec,
        epoch: u8,
        permit: &DispatchPermit,
    ) -> RemoveReplicaHost {
        let before = backend.load(execution.execution_id()).await.unwrap();
        let mut restarted = native_host(execution, backend.clone(), epoch);
        let HostOutcome::Quarantined {
            activity,
            attempt_id,
            ..
        } = restarted
            .turn(&RemoveReplicaWorkflow, execution.clone())
            .await
        else {
            panic!("expected exposed native remove activity to be quarantined after restart");
        };
        assert_eq!(activity, *permit.activity());
        assert_eq!(attempt_id, permit.attempt_id());
        assert_eq!(
            backend.load(execution.execution_id()).await.unwrap(),
            before
        );
        restarted
    }

    async fn replayed_native_boundary(
        backend: &InMemoryCheckpointStore,
        execution: &ExecutionSpec,
        reference: &RemoveReplicaExecution,
    ) -> (DurableOperationStatus, Vec<DurableOperationStatus>) {
        replay_active_operation_history(reference, &active_history(backend, execution).await)
            .unwrap()
    }

    fn resolver_for(
        operation: &DurableOperationStatus,
        history: Vec<DurableOperationStatus>,
        observations: &OperationObservations,
        pods: &OperationPodIdentities,
        target_role_label: Option<&str>,
        now: i64,
    ) -> RemoveReplicaPreparedActivityResolver {
        let addressed = observations
            .iter()
            .map(|(id, observation)| (*id, observation.status.instance_id.clone()))
            .collect();
        RemoveReplicaPreparedActivityResolver::new(
            operation,
            observations,
            pods,
            target_role_label,
            &addressed,
            now,
        )
        .with_history(history)
    }

    fn record_exact_effect(
        effects: &mut BTreeMap<Vec<u8>, usize>,
        permit: &DispatchPermit,
    ) -> RemoveReplicaBoundaryInput {
        let input = decode_boundary_input(permit.activity().spec().input()).unwrap();
        assert!(matches!(
            input,
            RemoveReplicaBoundaryInput::ReplicaCommand { .. }
                | RemoveReplicaBoundaryInput::LabelCommand { .. }
                | RemoveReplicaBoundaryInput::DeleteCommand { .. }
        ));
        let count = effects
            .entry(permit.activity().spec().input().as_slice().to_vec())
            .or_default();
        *count += 1;
        assert_eq!(*count, 1, "an exact durable effect was dispatched twice");
        input
    }

    #[test]
    fn remove_replica_execution_admission_is_structured_compact_and_derives_reduced_topology() {
        let reference = reference();
        let operation = reconstruct_initial_operation(reference.input.as_ref().unwrap()).unwrap();
        assert_eq!(reference.contract_version, REMOVE_REPLICA_CONTRACT_VERSION);
        assert_eq!(operation.previous_snapshot.members.len(), 3);
        assert_eq!(operation.target_snapshot.members.len(), 2);
        assert!(
            operation
                .target_snapshot
                .members
                .iter()
                .all(|member| member.id != 3)
        );
        let encoded = serde_json::to_vec(reference.input.as_ref().unwrap()).unwrap();
        assert!(encoded.len() < REMOVE_REPLICA_MAX_BOUNDARY_INPUT_BYTES as usize);
        assert!(
            !String::from_utf8(encoded)
                .unwrap()
                .contains("targetSnapshot")
        );
    }

    #[test]
    fn framework_native_remove_replica_force_mode_is_immutable_and_replays_without_target() {
        let reference = force_reference();
        assert_eq!(
            reference.input.as_ref().unwrap().mode,
            DurableRemoveMode::Force
        );
        assert!(
            reference
                .input
                .as_ref()
                .unwrap()
                .target
                .agent_generation
                .is_none()
        );

        let initial = reconstruct_initial_operation(reference.input.as_ref().unwrap()).unwrap();
        assert_eq!(initial.remove_mode, Some(DurableRemoveMode::Force));
        assert!(initial.remove_target_agent_generation.is_none());
        let mut changed_mode = initial.clone();
        changed_mode.remove_mode = Some(DurableRemoveMode::ScaleDown);
        changed_mode.remove_target_agent_generation = Some(generation(3).to_string());
        assert!(
            validate_transition(&initial, &changed_mode)
                .unwrap_err()
                .contains("immutable authority")
        );

        let mut force_observations = observations(&initial);
        force_observations.remove(&3);
        let mut force_pods = pod_identities();
        force_pods.remove(&3);
        let evidence = RemoveReplicaObservationEvidence::capture(
            &initial,
            &force_observations,
            &force_pods,
            None,
            10,
        )
        .unwrap();
        let activity = ActivityRecord::completed(
            ActivitySequence::new(0),
            activity_spec(&logical_boundary(&initial)).unwrap(),
            encode_boundary_result(&RemoveReplicaBoundaryResult::Observation { evidence }).unwrap(),
        );
        let replayed = replay_active_operation(&reference, &[activity]).unwrap();
        let intent = core_intent(&replayed).unwrap();
        assert_eq!(replayed.remove_mode, Some(DurableRemoveMode::Force));
        assert_eq!(
            intent.mode,
            kuberic_core::remove_replica::RemoveReplicaMode::Force
        );
        assert!(intent.expected_target_agent_generation.is_none());
        assert!(intent.target_control_address.is_none());
        assert!(intent.target_lifecycle_peer_protocol_version.is_none());
    }

    #[test]
    fn framework_native_remove_replica_force_requires_primary_target_and_topology_fences() {
        let reference = force_reference();
        let initial = reconstruct_initial_operation(reference.input.as_ref().unwrap()).unwrap();
        let pods = pod_identities();
        let base = observations(&initial);

        let mut missing_target = base.clone();
        missing_target.remove(&3);
        let Decision::Persist(frozen) =
            decide_remove_replica(&initial, &missing_target, &pods, None, 10).unwrap()
        else {
            panic!("Force must freeze from exact primary and retained topology without a target")
        };
        assert_eq!(frozen.remove_mode, Some(DurableRemoveMode::Force));
        let frozen_intent = core_intent(&frozen).unwrap();
        assert_eq!(
            frozen_intent.mode,
            kuberic_core::remove_replica::RemoveReplicaMode::Force
        );
        assert!(frozen_intent.expected_target_agent_generation.is_none());

        for mutate in [
            |observations: &mut OperationObservations| {
                observations.get_mut(&1).unwrap().status.instance_id =
                    ReplicaInstanceId::new("replacement-primary");
            },
            |observations: &mut OperationObservations| {
                observations.get_mut(&1).unwrap().status.epoch = Epoch::new(1, 8);
            },
            |observations: &mut OperationObservations| {
                observations.get_mut(&1).unwrap().status.role = Role::ActiveSecondary;
            },
        ] as [fn(&mut OperationObservations); 3]
        {
            let mut drifted = missing_target.clone();
            mutate(&mut drifted);
            assert!(
                decide_remove_replica(&initial, &drifted, &pods, None, 10).is_err(),
                "Force accepted drifted primary authority"
            );
        }

        let mut wrong_target = base.clone();
        wrong_target.get_mut(&3).unwrap().status.instance_id =
            ReplicaInstanceId::new("replacement-target");
        assert!(
            decide_remove_replica(&initial, &wrong_target, &pods, Some("secondary"), 10)
                .unwrap_err()
                .contains("another incarnation")
        );

        let mut missing_retained = missing_target.clone();
        missing_retained.remove(&2);
        assert!(
            decide_remove_replica(&initial, &missing_retained, &pods, None, 10)
                .unwrap_err()
                .contains("retained replica 2 is unavailable")
        );

        let mut changed_topology = missing_target;
        changed_topology
            .get_mut(&1)
            .unwrap()
            .status
            .configuration
            .as_mut()
            .unwrap()
            .members
            .retain(|member| member.id != 2);
        assert!(
            decide_remove_replica(&initial, &changed_topology, &pods, None, 10)
                .unwrap_err()
                .contains("primary configuration is not previous")
        );

        let Decision::Persist(dispatched) =
            decide_remove_replica(&frozen, &base, &pods, Some("secondary"), 10).unwrap()
        else {
            panic!("Force intent must advance to its exact primary command")
        };
        for mutate in [
            |observations: &mut OperationObservations| {
                observations.get_mut(&1).unwrap().status.agent.generation = generation(9);
            },
            |observations: &mut OperationObservations| {
                observations
                    .get_mut(&1)
                    .unwrap()
                    .status
                    .agent
                    .control_version = AgentControlVersion::new(12);
            },
        ] as [fn(&mut OperationObservations); 2]
        {
            let mut drifted = base.clone();
            mutate(&mut drifted);
            let resolver = resolver_for(
                &dispatched,
                vec![dispatched.clone()],
                &drifted,
                &pods,
                Some("secondary"),
                10,
            );
            assert!(
                !matches!(
                    resolver.prepare(&logical_boundary(&dispatched)),
                    Ok(RemoveReplicaBoundaryInput::ReplicaCommand { .. })
                ),
                "Force prepared a command after primary generation/control authority drift"
            );
        }
    }

    #[test]
    fn remove_replica_execution_boundaries_are_tagged_and_never_store_mutable_operation_state() {
        let reference = reference();
        let operation = reconstruct_initial_operation(reference.input.as_ref().unwrap()).unwrap();
        let evidence = RemoveReplicaObservationEvidence::capture(
            &operation,
            &observations(&operation),
            &pod_identities(),
            Some("secondary"),
            10,
        )
        .unwrap();
        let input = serde_json::to_string(&logical_boundary(&operation)).unwrap();
        let result =
            serde_json::to_string(&RemoveReplicaBoundaryResult::Observation { evidence }).unwrap();
        assert!(input.len() <= REMOVE_REPLICA_MAX_BOUNDARY_INPUT_BYTES as usize);
        assert!(result.len() <= REMOVE_REPLICA_MAX_BOUNDARY_RESULT_BYTES as usize);
        eprintln!(
            "native remove compact sample: immutable_input={}, logical_input={}, observation_result={}",
            execution_spec(&reference)
                .unwrap()
                .workflow_input()
                .as_slice()
                .len(),
            input.len(),
            result.len()
        );
        for encoded in [&input, &result] {
            assert!(!encoded.contains("DurableOperationStatus"));
            assert!(!encoded.contains("pendingAction"));
            assert!(!encoded.contains("removeIntent"));
            assert!(!encoded.contains("reducedCatchUpConfiguration"));
            assert!(!encoded.contains("reducedCurrentConfiguration"));
        }
    }

    #[test]
    fn remove_replica_execution_replays_evidence_deterministically() {
        let reference = reference();
        let spec = execution_spec(&reference).unwrap();
        let operation = reconstruct_initial_operation(reference.input.as_ref().unwrap()).unwrap();
        let evidence = RemoveReplicaObservationEvidence::capture(
            &operation,
            &observations(&operation),
            &pod_identities(),
            Some("secondary"),
            10,
        )
        .unwrap();
        let activity = ActivityRecord::completed(
            ActivitySequence::new(0),
            activity_spec(&logical_boundary(&operation)).unwrap(),
            encode_boundary_result(&RemoveReplicaBoundaryResult::Observation { evidence }).unwrap(),
        );
        let checkpoint = payload(&spec, vec![activity]);
        let first = evaluate(
            &RemoveReplicaWorkflow,
            &spec,
            Some(&checkpoint),
            checkpoint_limits(),
        );
        let second = evaluate(
            &RemoveReplicaWorkflow,
            &spec,
            Some(&checkpoint),
            checkpoint_limits(),
        );
        assert_eq!(first, second);
        let Evaluation::Scheduled { activity, .. } = first else {
            panic!("replay did not advance to the exact next boundary: {first:?}");
        };
        assert_eq!(activity.sequence(), ActivitySequence::new(1));
    }

    #[tokio::test]
    async fn framework_native_remove_replica_restarts_at_every_durable_boundary_without_duplicate_effects()
     {
        let reference = reference();
        let execution = execution_spec(&reference).unwrap();
        let initial = advance_to_boundary(
            reconstruct_initial_operation(reference.input.as_ref().unwrap()).unwrap(),
        )
        .unwrap();
        let initial_observations = observations(&initial);
        let pods = pod_identities();
        let backend = InMemoryCheckpointStore::new();
        let workflow = RemoveReplicaWorkflow;
        let mut history_specs = Vec::new();
        let mut effects = BTreeMap::new();

        let mut host = native_host(&execution, backend.clone(), 61);
        let HostOutcome::ScheduleAccepted {
            activity: freeze_activity,
            ..
        } = host.turn(&workflow, execution.clone()).await
        else {
            panic!("expected accepted native freeze-observation schedule");
        };
        let scheduled = active_history(&backend, &execution).await;
        assert_eq!(scheduled.len(), 1);
        assert_eq!(
            scheduled[0].logical_id(execution.execution_id()),
            freeze_activity
        );
        assert!(matches!(scheduled[0].state(), ActivityState::Scheduled));

        let mut host = native_host(&execution, backend.clone(), 62);
        let HostOutcome::DispatchPermitted {
            permit: freeze_permit,
            boundary: PersistenceBoundary::Exposure,
            ..
        } = host.turn(&workflow, execution.clone()).await
        else {
            panic!("expected accepted native freeze-observation exposure");
        };
        assert_eq!(*freeze_permit.activity(), freeze_activity);
        assert!(matches!(
            decode_boundary_input(freeze_permit.activity().spec().input()).unwrap(),
            RemoveReplicaBoundaryInput::Observe {
                phase: DurableOperationPhase::RemoveFreezeIntent,
                attempt: 0
            }
        ));
        history_specs.push(freeze_permit.activity().spec().clone());
        assert_native_exposed_history(&backend, &execution, &freeze_permit, &history_specs).await;

        let _ = restart_native_into_quarantine(&backend, &execution, 63, &freeze_permit).await;
        let host = restart_native_into_quarantine(&backend, &execution, 64, &freeze_permit).await;
        let freeze_result = RemoveReplicaBoundaryResult::Observation {
            evidence: RemoveReplicaObservationEvidence::capture(
                &initial,
                &initial_observations,
                &pods,
                Some("secondary"),
                10,
            )
            .unwrap(),
        };
        assert!(matches!(
            host.observe(
                &execution,
                ActivityObservation::new(
                    freeze_permit.activity().clone(),
                    encode_boundary_result(&freeze_result).unwrap(),
                ),
            )
            .await,
            HostOutcome::ObservationAccepted { activity, .. }
                if activity == *freeze_permit.activity()
        ));
        assert_native_completed_history(&backend, &execution, &history_specs).await;

        let (replica_operation, replica_history) =
            replayed_native_boundary(&backend, &execution, &reference).await;
        assert_eq!(
            replica_operation.phase,
            DurableOperationPhase::RemoveAwaitCoordination
        );
        assert!(replica_operation.remove_commit_evidence.is_none());
        let replica_resolver = resolver_for(
            &replica_operation,
            replica_history,
            &initial_observations,
            &pods,
            Some("secondary"),
            10,
        );
        let mut host = native_host(&execution, backend.clone(), 65);
        let HostOutcome::DispatchPermitted {
            permit: replica_permit,
            boundary: PersistenceBoundary::ScheduleExposure,
            ..
        } = host
            .turn_and_expose_with(&workflow, execution.clone(), &replica_resolver)
            .await
        else {
            panic!("expected exact native replica-command exposure after restart");
        };
        let RemoveReplicaBoundaryInput::ReplicaCommand {
            command: replica_command,
        } = record_exact_effect(&mut effects, &replica_permit)
        else {
            unreachable!()
        };
        let exact_replica_command = replica_command.effect_command().unwrap();
        let intent = replica_operation.remove_intent.as_ref().unwrap();
        assert_eq!(exact_replica_command.target_id, initial.old_primary_id);
        assert_eq!(
            exact_replica_command.target_instance_id,
            intent.primary_instance_id
        );
        assert_eq!(exact_replica_command.action_id, intent.action_id);
        assert_eq!(
            exact_replica_command.action_signature,
            intent.input_signature
        );
        history_specs.push(replica_permit.activity().spec().clone());
        assert_native_exposed_history(&backend, &execution, &replica_permit, &history_specs).await;

        let _ = restart_native_into_quarantine(&backend, &execution, 66, &replica_permit).await;
        let host = restart_native_into_quarantine(&backend, &execution, 67, &replica_permit).await;
        let mut admitted_observations = observations(&replica_operation);
        let mut admitted_action = completed_action(&replica_operation, intent.action_id.clone());
        admitted_action.action.state = DurableActionState::Scheduled;
        admitted_action.action.result = None;
        admitted_action.action.remove_replica_progress = None;
        admitted_observations
            .get_mut(&replica_operation.old_primary_id)
            .unwrap()
            .status
            .agent
            .current_action = Some(admitted_action);
        let admitted_result = RemoveReplicaBoundaryResult::Effect {
            outcome: DurableEffectOutcome::Applied(
                RemoveReplicaObservationEvidence::capture(
                    &replica_operation,
                    &admitted_observations,
                    &pods,
                    Some("secondary"),
                    11,
                )
                .unwrap(),
            ),
        };
        assert!(matches!(
            host.observe(
                &execution,
                ActivityObservation::new(
                    replica_permit.activity().clone(),
                    encode_boundary_result(&admitted_result).unwrap(),
                ),
            )
            .await,
            HostOutcome::ObservationAccepted { activity, .. }
                if activity == *replica_permit.activity()
        ));
        assert_native_completed_history(&backend, &execution, &history_specs).await;

        let (commit_operation, commit_history) =
            replayed_native_boundary(&backend, &execution, &reference).await;
        assert_eq!(
            commit_operation.phase,
            DurableOperationPhase::RemoveAwaitCoordination
        );
        assert!(commit_operation.remove_commit_evidence.is_none());
        let commit_resolver = resolver_for(
            &commit_operation,
            commit_history,
            &admitted_observations,
            &pods,
            Some("secondary"),
            11,
        );
        let mut host = native_host(&execution, backend.clone(), 68);
        let HostOutcome::DispatchPermitted {
            permit: commit_permit,
            boundary: PersistenceBoundary::ScheduleExposure,
            ..
        } = host
            .turn_and_expose_with(&workflow, execution.clone(), &commit_resolver)
            .await
        else {
            panic!("expected post-effect authoritative observation after restart");
        };
        assert!(matches!(
            decode_boundary_input(commit_permit.activity().spec().input()).unwrap(),
            RemoveReplicaBoundaryInput::Observe {
                phase: DurableOperationPhase::RemoveAwaitCoordination,
                attempt: 1
            }
        ));
        history_specs.push(commit_permit.activity().spec().clone());
        assert_native_exposed_history(&backend, &execution, &commit_permit, &history_specs).await;

        let _ = restart_native_into_quarantine(&backend, &execution, 69, &commit_permit).await;
        let host = restart_native_into_quarantine(&backend, &execution, 70, &commit_permit).await;
        let mut committed_observations = observations(&commit_operation);
        {
            let primary = committed_observations
                .get_mut(&commit_operation.old_primary_id)
                .unwrap();
            primary.status.configuration = Some(configuration(
                &commit_operation.target_snapshot,
                ReplicaConfigurationMode::Current,
            ));
            primary.status.active_replica_connections.clear();
            primary.status.agent.current_action = Some(completed_action(
                &commit_operation,
                commit_operation
                    .remove_intent
                    .as_ref()
                    .unwrap()
                    .action_id
                    .clone(),
            ));
        }
        let commit_result = RemoveReplicaBoundaryResult::Observation {
            evidence: RemoveReplicaObservationEvidence::capture(
                &commit_operation,
                &committed_observations,
                &pods,
                Some("secondary"),
                20,
            )
            .unwrap(),
        };
        assert!(matches!(
            host.observe(
                &execution,
                ActivityObservation::new(
                    commit_permit.activity().clone(),
                    encode_boundary_result(&commit_result).unwrap(),
                ),
            )
            .await,
            HostOutcome::ObservationAccepted { activity, .. }
                if activity == *commit_permit.activity()
        ));
        assert_native_completed_history(&backend, &execution, &history_specs).await;

        let (label_operation, label_history) =
            replayed_native_boundary(&backend, &execution, &reference).await;
        assert_eq!(
            label_operation.phase,
            DurableOperationPhase::RemoveAwaitCleanup
        );
        assert_eq!(
            label_operation.committed_snapshot,
            Some(initial.target_snapshot.clone())
        );
        assert!(label_operation.remove_commit_evidence.is_some());
        let label_resolver = resolver_for(
            &label_operation,
            label_history,
            &committed_observations,
            &pods,
            Some("secondary"),
            20,
        );
        let mut host = native_host(&execution, backend.clone(), 71);
        let HostOutcome::DispatchPermitted {
            permit: label_permit,
            boundary: PersistenceBoundary::ScheduleExposure,
            ..
        } = host
            .turn_and_expose_with(&workflow, execution.clone(), &label_resolver)
            .await
        else {
            panic!("expected exact UID-fenced label exposure after commit restart");
        };
        let RemoveReplicaBoundaryInput::LabelCommand {
            command: label_command,
        } = record_exact_effect(&mut effects, &label_permit)
        else {
            unreachable!()
        };
        assert_eq!(label_command.target_id, 3);
        assert_eq!(label_command.pod_name, "set-2");
        assert_eq!(label_command.expected_uid, "three");
        assert_eq!(label_command.role, "retired");
        assert!(label_command.has_valid_identity_signature());
        history_specs.push(label_permit.activity().spec().clone());
        assert_native_exposed_history(&backend, &execution, &label_permit, &history_specs).await;

        let _ = restart_native_into_quarantine(&backend, &execution, 72, &label_permit).await;
        let host = restart_native_into_quarantine(&backend, &execution, 73, &label_permit).await;
        let label_result = RemoveReplicaBoundaryResult::Effect {
            outcome: DurableEffectOutcome::Applied(
                RemoveReplicaObservationEvidence::capture(
                    &label_operation,
                    &committed_observations,
                    &pods,
                    Some("retired"),
                    20,
                )
                .unwrap(),
            ),
        };
        assert!(matches!(
            host.observe(
                &execution,
                ActivityObservation::new(
                    label_permit.activity().clone(),
                    encode_boundary_result(&label_result).unwrap(),
                ),
            )
            .await,
            HostOutcome::ObservationAccepted { activity, .. }
                if activity == *label_permit.activity()
        ));
        assert_native_completed_history(&backend, &execution, &history_specs).await;

        let (delete_operation, delete_history) =
            replayed_native_boundary(&backend, &execution, &reference).await;
        assert_eq!(
            delete_operation.phase,
            DurableOperationPhase::RemoveDeleteTargetPod
        );
        let delete_resolver = resolver_for(
            &delete_operation,
            delete_history,
            &committed_observations,
            &pods,
            Some("retired"),
            20,
        );
        let mut host = native_host(&execution, backend.clone(), 74);
        let HostOutcome::DispatchPermitted {
            permit: delete_permit,
            boundary: PersistenceBoundary::ScheduleExposure,
            ..
        } = host
            .turn_and_expose_with(&workflow, execution.clone(), &delete_resolver)
            .await
        else {
            panic!("expected exact UID-fenced delete exposure after restart");
        };
        let RemoveReplicaBoundaryInput::DeleteCommand {
            command: delete_command,
        } = record_exact_effect(&mut effects, &delete_permit)
        else {
            unreachable!()
        };
        assert_eq!(delete_command.target_id, 3);
        assert_eq!(delete_command.pod_name, "set-2");
        assert_eq!(delete_command.expected_uid, "three");
        assert!(delete_command.has_valid_identity_signature());
        history_specs.push(delete_permit.activity().spec().clone());
        assert_native_exposed_history(&backend, &execution, &delete_permit, &history_specs).await;

        let _ = restart_native_into_quarantine(&backend, &execution, 75, &delete_permit).await;
        let host = restart_native_into_quarantine(&backend, &execution, 76, &delete_permit).await;
        let mut deleted_pods = pods;
        deleted_pods.remove(&3);
        let delete_result = RemoveReplicaBoundaryResult::Effect {
            outcome: DurableEffectOutcome::Applied(
                RemoveReplicaObservationEvidence::capture(
                    &delete_operation,
                    &committed_observations,
                    &deleted_pods,
                    None,
                    20,
                )
                .unwrap(),
            ),
        };
        assert!(matches!(
            host.observe(
                &execution,
                ActivityObservation::new(
                    delete_permit.activity().clone(),
                    encode_boundary_result(&delete_result).unwrap(),
                ),
            )
            .await,
            HostOutcome::ObservationAccepted { activity, .. }
                if activity == *delete_permit.activity()
        ));
        let active_encoded_len =
            assert_native_completed_history(&backend, &execution, &history_specs).await;

        assert_eq!(history_specs.len(), 5);
        assert_eq!(effects.len(), 3);
        assert!(effects.values().all(|count| *count == 1));

        let (completed_operation, completed_history) =
            replayed_native_boundary(&backend, &execution, &reference).await;
        assert_eq!(completed_operation.phase, DurableOperationPhase::Completed);
        let completed_resolver = resolver_for(
            &completed_operation,
            completed_history,
            &committed_observations,
            &deleted_pods,
            None,
            20,
        );
        let mut host = native_host(&execution, backend.clone(), 77);
        let HostOutcome::WorkflowCompleted {
            outcome,
            boundary: PersistenceBoundary::Completion,
            ..
        } = host
            .turn_and_expose_with(&workflow, execution.clone(), &completed_resolver)
            .await
        else {
            panic!("expected compact terminal after the final durable-boundary restart");
        };
        assert!(matches!(
            validate_loaded_terminal(&reference, &outcome, 5).unwrap(),
            RemoveReplicaTerminal::Completed {
                cleanup: RemoveReplicaCleanupStatus {
                    connection_absent: true,
                    target_retirement: Some(TargetRetirementObservationStatus::Completed),
                    target_labels_fenced: true,
                    target_pod_deleted: true,
                },
                accounting: RemoveReplicaActivityAccounting {
                    external_effect_count: 3,
                    passive_observation_count: 2,
                },
                ..
            }
        ));
        let terminal = backend
            .load(execution.execution_id())
            .await
            .unwrap()
            .unwrap();
        assert!(terminal.checkpoint().encoded_len().unwrap() < active_encoded_len);
        let terminal_payload = terminal
            .checkpoint()
            .decode_and_validate(&execution, checkpoint_limits())
            .unwrap();
        assert!(terminal_payload.active_activities().is_none());
        assert!(matches!(
            terminal_payload.state(),
            CheckpointState::Terminal {
                completed_activity_count: 5,
                ..
            }
        ));

        let effects_before_terminal_reload = effects.clone();
        let mut restarted = native_host(&execution, backend, 78);
        assert!(matches!(
            restarted.turn(&workflow, execution).await,
            HostOutcome::WorkflowCompleted {
                checkpoint_status: kuberic_durable_execution::TerminalCheckpointStatus::Reloaded,
                ..
            }
        ));
        assert_eq!(effects, effects_before_terminal_reload);
    }

    #[test]
    fn remove_replica_execution_captures_exact_retained_terminal_evidence() {
        let (operation, mut observations) = freeze_and_dispatch();
        let action_id = operation.remove_intent.as_ref().unwrap().action_id.clone();
        let primary = observations.get_mut(&operation.old_primary_id).unwrap();
        let matching = completed_action(&operation, action_id.clone());
        primary.status.agent.current_action = Some(completed_action(
            &operation,
            "another-current-action".to_string(),
        ));
        primary.status.agent.retained_terminal_actions = vec![
            matching,
            completed_action(&operation, "newer-unrelated-terminal".to_string()),
        ];

        let evidence = RemoveReplicaObservationEvidence::capture(
            &operation,
            &observations,
            &pod_identities(),
            Some("secondary"),
            10,
        )
        .unwrap();
        let captured = evidence
            .replicas
            .iter()
            .find(|replica| replica.replica_id == operation.old_primary_id)
            .and_then(|replica| replica.agent.action.as_ref())
            .unwrap();
        assert_eq!(captured.state, RemoveReplicaActionStateEvidence::Completed);
        assert_eq!(
            captured.result,
            Some(RemoveReplicaTerminalResultStatus::CommittedClean)
        );

        let (reconstructed, _) = evidence.reconstruct(&operation).unwrap();
        let reconstructed = reconstructed.get(&operation.old_primary_id).unwrap();
        let correlated = correlated_action_observation(&reconstructed.status, &action_id).unwrap();
        assert_eq!(correlated.state, DurableActionState::Completed);
        assert!(matches!(
            correlated.result,
            Some(DurableActionResult::RemoveReplica(
                RemoveReplicaTerminalResult::CommittedClean
            ))
        ));
    }

    #[test]
    fn remove_replica_execution_reuses_the_exact_prepared_command() {
        let (operation, observations) = freeze_and_dispatch();
        let addressed = observations
            .iter()
            .map(|(id, observation)| (*id, observation.status.instance_id.clone()))
            .collect();
        let pods = pod_identities();
        let resolver = RemoveReplicaPreparedActivityResolver::new(
            &operation,
            &observations,
            &pods,
            Some("secondary"),
            &addressed,
            10,
        );
        let logical = activity_spec(&logical_boundary(&operation)).unwrap();
        let prepared_input = resolver.prepare(&logical_boundary(&operation)).unwrap();
        eprintln!(
            "native remove prepared replica input bytes={}",
            serde_json::to_vec(&prepared_input).unwrap().len()
        );
        let prepared = activity_spec(&prepared_input).unwrap();
        let prepared_input = decode_boundary_input(prepared.input()).unwrap();
        let RemoveReplicaBoundaryInput::ReplicaCommand { command } = &prepared_input else {
            panic!("remove command was not prepared");
        };
        assert!(
            prepared.input().as_slice().len() <= REMOVE_REPLICA_MAX_BOUNDARY_INPUT_BYTES as usize
        );
        assert_eq!(
            resolver.resolve(&logical, Some(&prepared)).unwrap(),
            prepared
        );

        let mut changed_commands = Vec::new();
        let mut changed = command.clone();
        changed.expected_agent_generation = generation(9).to_string();
        changed_commands.push(changed);
        let mut changed = command.clone();
        changed.expected_control_version = changed.expected_control_version.saturating_add(1);
        changed_commands.push(changed);
        let mut changed = command.clone();
        changed.observed_runtime_epoch[1] = changed.observed_runtime_epoch[1].saturating_add(1);
        changed_commands.push(changed);
        let mut changed = command.clone();
        changed.action_payload = ExactBytes::new(b"invalid");
        changed_commands.push(changed);
        let (other_operation, _) = freeze_and_dispatch();
        let other_action = DurableReplicaAction::RemoveReplicaIntent {
            intent: Box::new(core_intent(&other_operation).unwrap()),
        };
        let mut changed = command.clone();
        changed.action_payload = compact_action_payload(
            &kuberic_core::grpc::convert::encode_correlated_action_payload(&other_action).unwrap(),
        );
        changed_commands.push(changed);
        for changed in changed_commands {
            let changed =
                activity_spec(&RemoveReplicaBoundaryInput::ReplicaCommand { command: changed })
                    .unwrap();
            assert_eq!(resolver.resolve(&logical, Some(&changed)).unwrap(), logical);
        }

        for exact in [
            RemoveReplicaBoundaryInput::LabelCommand {
                command: LabelEffectCommand::new(
                    3,
                    "set-2".to_string(),
                    "three".to_string(),
                    "retired".to_string(),
                ),
            },
            RemoveReplicaBoundaryInput::DeleteCommand {
                command: DeleteEffectCommand::new(3, "set-2".to_string(), "three".to_string()),
            },
        ] {
            assert!(
                activity_spec(&exact).unwrap().input().as_slice().len()
                    <= REMOVE_REPLICA_MAX_BOUNDARY_INPUT_BYTES as usize
            );
        }
    }

    #[test]
    fn remove_replica_execution_transition_validation_is_monotonic() {
        let reference = reference();
        let initial = reconstruct_initial_operation(reference.input.as_ref().unwrap()).unwrap();
        let mut changed = initial.clone();
        changed.target_pod_uid = Some("replacement".to_string());
        assert!(validate_transition(&initial, &changed).is_err());

        let mut invalid_cleanup = initial.clone();
        invalid_cleanup.remove_cleanup = Some(RemoveReplicaCleanupStatus::default());
        assert!(validate_transition(&initial, &invalid_cleanup).is_err());

        let current = committed_operation();
        for regress in [
            |cleanup: &mut RemoveReplicaCleanupStatus| cleanup.connection_absent = false,
            |cleanup: &mut RemoveReplicaCleanupStatus| cleanup.target_retirement = None,
            |cleanup: &mut RemoveReplicaCleanupStatus| {
                cleanup.target_retirement = Some(TargetRetirementObservationStatus::Unavailable)
            },
            |cleanup: &mut RemoveReplicaCleanupStatus| cleanup.target_labels_fenced = false,
            |cleanup: &mut RemoveReplicaCleanupStatus| cleanup.target_pod_deleted = false,
        ] {
            let mut next = current.clone();
            regress(next.remove_cleanup.as_mut().unwrap());
            assert!(validate_transition(&current, &next).is_err());
        }

        let mut current = current;
        current.remove_cleanup.as_mut().unwrap().target_retirement =
            Some(TargetRetirementObservationStatus::Unavailable);
        let mut next = current.clone();
        next.remove_cleanup.as_mut().unwrap().target_retirement = None;
        assert!(validate_transition(&current, &next).is_err());
    }

    #[test]
    fn remove_replica_execution_distinguishes_incompatible_and_malformed_contracts() {
        let legacy = ExactBytes::new(
            br#"{"version":1,"executionId":"legacy","initialOperation":{}}"#.to_vec(),
        );
        let legacy_spec = ExecutionSpec::new(
            ExecutionId::from_bytes([7; 16]),
            legacy,
            REMOVE_REPLICA_MAX_TERMINAL_PAYLOAD_BYTES,
        );
        let Evaluation::Complete { outcome, .. } = evaluate(
            &RemoveReplicaWorkflow,
            &legacy_spec,
            None,
            checkpoint_limits(),
        ) else {
            panic!("legacy contract did not terminate");
        };
        assert!(matches!(
            decode_terminal(&outcome).unwrap(),
            RemoveReplicaTerminal::IncompatibleContract {
                source,
                found_version: 1,
                ..
            } if source == "legacy_pilot"
        ));

        let reference = reference();
        let spec = execution_spec(&reference).unwrap();
        let mut unknown: serde_json::Value =
            serde_json::from_slice(spec.workflow_input().as_slice()).unwrap();
        unknown
            .as_object_mut()
            .unwrap()
            .insert("unknown".to_string(), serde_json::Value::Bool(true));
        let malformed_spec = ExecutionSpec::new(
            spec.execution_id(),
            serde_json::to_vec(&unknown).unwrap().into(),
            REMOVE_REPLICA_MAX_TERMINAL_PAYLOAD_BYTES,
        );
        let Evaluation::Complete { outcome, .. } = evaluate(
            &RemoveReplicaWorkflow,
            &malformed_spec,
            None,
            checkpoint_limits(),
        ) else {
            panic!("unknown-field contract did not terminate");
        };
        assert!(matches!(
            decode_terminal(&outcome).unwrap(),
            RemoveReplicaTerminal::Rejected { .. }
        ));
    }

    struct InputBound;

    impl DurableActivity for InputBound {
        type Input = String;
        type Output = ();

        const NAME: &'static str = "native-remove-input-bound";
        const VERSION: u32 = 1;
        const MAX_INPUT_BYTES: u64 = REMOVE_REPLICA_MAX_BOUNDARY_INPUT_BYTES;
        const MAX_RESULT_BYTES: u64 = 1;
    }

    struct ResultBound;

    impl DurableActivity for ResultBound {
        type Input = ();
        type Output = String;

        const NAME: &'static str = "native-remove-result-bound";
        const VERSION: u32 = 1;
        const MAX_INPUT_BYTES: u64 = 1;
        const MAX_RESULT_BYTES: u64 = REMOVE_REPLICA_MAX_BOUNDARY_RESULT_BYTES;
    }

    #[test]
    fn remove_replica_execution_rejects_all_six_one_byte_over_bounds() {
        let reference = reference();
        let spec = execution_spec(&reference).unwrap();
        let logical = activity_spec(&logical_boundary(
            &reconstruct_initial_operation(reference.input.as_ref().unwrap()).unwrap(),
        ))
        .unwrap();
        let activities = (0..=REMOVE_REPLICA_MAX_ACTIVITY_RECORDS)
            .map(|sequence| {
                ActivityRecord::completed(
                    ActivitySequence::new(sequence as u64),
                    logical.clone(),
                    ExactBytes::new(b"{}".to_vec()),
                )
            })
            .collect();
        assert!(matches!(
            CheckpointEnvelope::encode_with_limits(
                &CheckpointPayload::active(
                    ExecutionContract::with_encoded_limits(
                        spec,
                        REMOVE_REPLICA_MAX_ACTIVE_ENCODED_BYTES as u64,
                        REMOVE_REPLICA_MAX_TERMINAL_ENCODED_BYTES as u64,
                    ),
                    activities,
                ),
                checkpoint_limits(),
            ),
            Err(CheckpointError::ActivityRecordLimitExceeded {
                actual: 17,
                maximum: 16
            })
        ));
        assert!(matches!(
            encode_activity_input::<InputBound>(&"x".repeat(4_095)),
            Err(ActivityCallError::InputTooLarge {
                actual_bytes: 4_097,
                max_bytes: 4_096
            })
        ));
        assert!(matches!(
            encode_activity_result::<ResultBound>(&"x".repeat(2_047)),
            Err(ActivityCallError::ResultTooLarge {
                actual_bytes: 2_049,
                max_bytes: 2_048
            })
        ));
        assert!(validate_active_record_bytes(262_145).is_err());
        assert!(validate_terminal_record_bytes(12_289).is_err());
        assert!(validate_terminal_payload_bytes(4_097).is_err());
    }

    #[test]
    fn remove_replica_execution_maximum_fault_history_and_terminal_fit_independent_bounds() {
        let active = maximum_active_checkpoint().unwrap();
        let terminal = maximum_terminal_checkpoint().unwrap();
        let active_bytes = active.encoded_len().unwrap();
        let terminal_bytes = terminal.encoded_len().unwrap();
        assert!(active_bytes <= REMOVE_REPLICA_MAX_ACTIVE_ENCODED_BYTES);
        assert!(terminal_bytes <= REMOVE_REPLICA_MAX_TERMINAL_ENCODED_BYTES);
        assert_eq!(
            checkpoint_limits().max_activity_records(),
            REMOVE_REPLICA_MAX_ACTIVITY_RECORDS
        );
        eprintln!(
            "native remove projected bounds: records={}, input={}, result={}, active={}, terminal={}, terminal_payload={}",
            REMOVE_REPLICA_MAX_ACTIVITY_RECORDS,
            REMOVE_REPLICA_MAX_BOUNDARY_INPUT_BYTES,
            REMOVE_REPLICA_MAX_BOUNDARY_RESULT_BYTES,
            active_bytes,
            terminal_bytes,
            REMOVE_REPLICA_MAX_TERMINAL_PAYLOAD_BYTES
        );
    }

    #[test]
    fn remove_replica_execution_terminal_has_no_duplicate_commit_authority() {
        let terminal = RemoveReplicaTerminal::Completed {
            commit_evidence: RemoveReplicaCommitEvidenceStatus {
                attempt_id: "operation:attempt-1".to_string(),
                action_id: "operation:attempt-1:RemoveReplicaIntent".to_string(),
                primary_agent_generation: generation(1).to_string(),
                configuration_signature: "q2[2@two:]".to_string(),
                observed_unix_seconds: 10,
            },
            cleanup: RemoveReplicaCleanupStatus {
                connection_absent: true,
                target_retirement: Some(TargetRetirementObservationStatus::Completed),
                target_labels_fenced: true,
                target_pod_deleted: true,
            },
            accounting: Default::default(),
        };
        let encoded = serde_json::to_string(&terminal).unwrap();
        assert!(!encoded.contains("authority"));
        assert!(!encoded.contains("inputSignature"));
        assert!(encoded.len() <= REMOVE_REPLICA_MAX_TERMINAL_PAYLOAD_BYTES as usize);
    }

    #[test]
    fn remove_replica_execution_validates_completed_terminal_against_immutable_admission() {
        let reference = reference();
        let (operation, _) = freeze_and_dispatch_for(&reference);
        let intent = operation.remove_intent.as_ref().unwrap();
        let terminal = RemoveReplicaTerminal::Completed {
            commit_evidence: RemoveReplicaCommitEvidenceStatus {
                attempt_id: intent.attempt_id.clone(),
                action_id: intent.action_id.clone(),
                primary_agent_generation: intent.primary_agent_generation.clone(),
                configuration_signature: core_intent(&operation)
                    .unwrap()
                    .reduced_current_configuration
                    .signature(),
                observed_unix_seconds: 10,
            },
            cleanup: RemoveReplicaCleanupStatus {
                connection_absent: true,
                target_retirement: Some(TargetRetirementObservationStatus::Completed),
                target_labels_fenced: true,
                target_pod_deleted: true,
            },
            accounting: Default::default(),
        };
        let outcome = encode_terminal(terminal.clone());
        assert_eq!(
            validate_loaded_terminal(&reference, &outcome, 0).unwrap(),
            terminal
        );

        let RemoveReplicaTerminal::Completed {
            mut commit_evidence,
            cleanup,
            accounting,
        } = terminal.clone()
        else {
            unreachable!()
        };
        commit_evidence.configuration_signature = "q2[replacement]".to_string();
        let changed = encode_terminal(RemoveReplicaTerminal::Completed {
            commit_evidence,
            cleanup,
            accounting,
        });
        assert!(validate_loaded_terminal(&reference, &changed, 0).is_err());

        let RemoveReplicaTerminal::Completed {
            mut commit_evidence,
            cleanup,
            accounting,
        } = terminal
        else {
            unreachable!()
        };
        commit_evidence.configuration_signature.push_str(",forged");
        let forged = encode_terminal(RemoveReplicaTerminal::Completed {
            commit_evidence,
            cleanup,
            accounting,
        });
        assert!(validate_loaded_terminal(&reference, &forged, 0).is_err());
    }

    #[tokio::test]
    async fn remove_replica_execution_measurement_decoder_classifies_native_boundaries() {
        let execution_id = ExecutionId::from_bytes([9; 16]);
        let spec = ExecutionSpec::new(
            execution_id,
            b"measurement".to_vec().into(),
            REMOVE_REPLICA_MAX_TERMINAL_PAYLOAD_BYTES,
        );
        let observe = activity_spec(&RemoveReplicaBoundaryInput::Observe {
            phase: DurableOperationPhase::RemoveFreezeIntent,
            attempt: 0,
        })
        .unwrap();
        let label = activity_spec(&RemoveReplicaBoundaryInput::LabelCommand {
            command: LabelEffectCommand::new(
                3,
                "set-2".to_string(),
                "three".to_string(),
                "retired".to_string(),
            ),
        })
        .unwrap();
        let contract = ExecutionContract::with_encoded_limits(
            spec.clone(),
            REMOVE_REPLICA_MAX_ACTIVE_ENCODED_BYTES as u64,
            REMOVE_REPLICA_MAX_TERMINAL_ENCODED_BYTES as u64,
        );
        let active = CheckpointEnvelope::encode_with_limits(
            &CheckpointPayload::active(
                contract.clone(),
                vec![
                    ActivityRecord::completed(
                        ActivitySequence::new(0),
                        observe,
                        ExactBytes::new(b"{}".to_vec()),
                    ),
                    ActivityRecord::completed(
                        ActivitySequence::new(1),
                        label,
                        ExactBytes::new(b"{}".to_vec()),
                    ),
                ],
            ),
            checkpoint_limits(),
        )
        .unwrap();
        let terminal = encode_terminal(RemoveReplicaTerminal::Compensated {
            message: "safe".to_string(),
            accounting: RemoveReplicaActivityAccounting {
                external_effect_count: 1,
                passive_observation_count: 1,
            },
        });
        let terminal = CheckpointEnvelope::encode_with_limits(
            &CheckpointPayload::terminal(contract, terminal, 2),
            checkpoint_limits(),
        )
        .unwrap();
        let store = MeasuredDurableCheckpointStore::with_native_remove_decoder(
            execution_id,
            DurableCheckpointStore::InMemory(InMemoryCheckpointStore::new()),
        );
        assert!(matches!(
            store
                .compare_and_swap(execution_id, None, active)
                .await
                .unwrap(),
            CasOutcome::Accepted(_)
        ));
        let loaded = store.load(execution_id).await.unwrap().unwrap();
        assert!(matches!(
            store
                .compare_and_swap(execution_id, Some(loaded.revision().clone()), terminal)
                .await
                .unwrap(),
            CasOutcome::Accepted(_)
        ));
        let measurements = store.measurements();
        assert_eq!(measurements.accepted_writes, 2);
        assert_eq!(measurements.completed_external_effect_count, Some(1));
        assert_eq!(measurements.completed_passive_observation_count, Some(1));
        assert!(measurements.latest_active_checkpoint_bytes.is_some());
        assert!(measurements.latest_terminal_checkpoint_bytes.is_some());
    }

    #[test]
    fn remove_replica_execution_persisted_shapes_deny_unknown_fields() {
        let mut reference = reference();
        reference.input.as_mut().unwrap().previous_snapshot.members[0].election_metadata =
            Some(StableReplicaElectionMetadataStatus {
                current_lsn: 10,
                committed_lsn: 9,
                first_retained_lsn: 1,
                deactivation_epoch: EpochStatus {
                    data_loss_number: 1,
                    configuration_number: 6,
                },
                deactivation_catch_up_lsn: 8,
            });
        let execution = serde_json::to_value(&reference).unwrap();
        for pointer in [
            "",
            "/input",
            "/input/target",
            "/input/previousSnapshot",
            "/input/previousSnapshot/epoch",
            "/input/previousSnapshot/members/0",
            "/input/previousSnapshot/members/0/electionMetadata",
            "/input/previousSnapshot/members/0/electionMetadata/deactivationEpoch",
        ] {
            assert_unknown_field_rejected::<RemoveReplicaExecution>(&execution, pointer);
        }
        let workflow_input = RemoveReplicaWorkflowInput {
            contract_version: reference.contract_version,
            execution_id: reference.execution_id.clone(),
            admission: reference.input.clone().unwrap(),
        };
        assert_unknown_field_rejected::<RemoveReplicaWorkflowInput>(
            &serde_json::to_value(workflow_input).unwrap(),
            "",
        );

        let boundary = RemoveReplicaBoundaryInput::Observe {
            phase: DurableOperationPhase::RemoveFreezeIntent,
            attempt: 0,
        };
        assert_unknown_field_rejected::<RemoveReplicaBoundaryInput>(
            &serde_json::to_value(boundary).unwrap(),
            "",
        );

        let (operation, observations) = freeze_and_dispatch();
        let pods = pod_identities();
        let addressed = observations
            .iter()
            .map(|(id, observation)| (*id, observation.status.instance_id.clone()))
            .collect();
        let resolver = RemoveReplicaPreparedActivityResolver::new(
            &operation,
            &observations,
            &pods,
            Some("secondary"),
            &addressed,
            10,
        );
        let RemoveReplicaBoundaryInput::ReplicaCommand { command } =
            resolver.prepare(&logical_boundary(&operation)).unwrap()
        else {
            panic!("expected prepared replica command")
        };
        assert_unknown_field_rejected::<CompactReplicaEffectCommand>(
            &serde_json::to_value(command).unwrap(),
            "",
        );
        assert_unknown_field_rejected::<LabelEffectCommand>(
            &serde_json::to_value(LabelEffectCommand::new(
                3,
                "set-2".to_string(),
                "three".to_string(),
                "retired".to_string(),
            ))
            .unwrap(),
            "",
        );
        assert_unknown_field_rejected::<DeleteEffectCommand>(
            &serde_json::to_value(DeleteEffectCommand::new(
                3,
                "set-2".to_string(),
                "three".to_string(),
            ))
            .unwrap(),
            "",
        );
    }

    #[test]
    fn remove_replica_execution_nested_evidence_denies_unknown_fields() {
        let (operation, mut observations) = freeze_and_dispatch();
        let action_id = operation.remove_intent.as_ref().unwrap().action_id.clone();
        observations
            .get_mut(&operation.old_primary_id)
            .unwrap()
            .status
            .agent
            .retained_terminal_actions
            .push(completed_action(&operation, action_id));
        let evidence = RemoveReplicaObservationEvidence::capture(
            &operation,
            &observations,
            &pod_identities(),
            Some("secondary"),
            10,
        )
        .unwrap();
        let result = serde_json::to_value(RemoveReplicaBoundaryResult::Observation {
            evidence: evidence.clone(),
        })
        .unwrap();
        for pointer in [
            "",
            "/evidence",
            "/evidence/r/0",
            "/evidence/r/0/e",
            "/evidence/r/0/f",
            "/evidence/r/0/f/m/0",
            "/evidence/r/0/x/0",
            "/evidence/r/0/a",
            "/evidence/r/0/a/a",
            "/evidence/r/0/a/a/p",
            "/evidence/p/0",
        ] {
            assert_unknown_field_rejected::<RemoveReplicaBoundaryResult>(&result, pointer);
        }

        let effect = serde_json::to_value(RemoveReplicaBoundaryResult::Effect {
            outcome: DurableEffectOutcome::Applied(evidence),
        })
        .unwrap();
        for pointer in ["", "/outcome", "/outcome/detail"] {
            assert_unknown_field_rejected::<RemoveReplicaBoundaryResult>(&effect, pointer);
        }
    }

    #[test]
    fn remove_replica_execution_nested_terminal_denies_unknown_fields() {
        let operation = committed_operation();
        let terminal =
            terminal_from_operation(&operation, RemoveReplicaActivityAccounting::default())
                .unwrap();
        let terminal = serde_json::to_value(terminal).unwrap();
        for pointer in ["", "/commit_evidence", "/cleanup", "/accounting"] {
            assert_unknown_field_rejected::<RemoveReplicaTerminal>(&terminal, pointer);
        }

        let unsafe_terminal = serde_json::to_value(RemoveReplicaTerminal::Unsafe {
            committed: false,
            commit_evidence: None,
            cleanup: None,
            disposition: RemoveReplicaDispositionStatus::FailedPreCommitIncomplete {
                attempt: 1,
                last_observed_phase: Some(RemoveReplicaCoordinatorPhaseStatus::Validating),
                reason: "failed safely".to_string(),
            },
            message: "failed safely".to_string(),
            accounting: Default::default(),
        })
        .unwrap();
        assert_unknown_field_rejected::<RemoveReplicaTerminal>(
            &unsafe_terminal,
            "/disposition/failedPreCommitIncomplete",
        );
    }

    #[test]
    fn framework_native_remove_replica_fr017_classifies_actual_checkpoint_failures() {
        let reference = reference();
        let set = adapter_set(&reference);
        let pods = adapter_pods();
        let current_pods = adapter_current_pods(&pods);
        let execution = execution_spec(&reference).unwrap();
        let operation = reconstruct_initial_operation(reference.input.as_ref().unwrap()).unwrap();
        let api = adapter_api(&operation, DispatchResult::Busy);
        let adapter =
            FrameworkNativeRemoveReplicaAdapter::new(&reference, &set, &current_pods, &api, 10)
                .unwrap();

        assert_eq!(
            adapter.checkpoint_disposition(&CheckpointError::UnsupportedFormat {
                actual: 2,
                supported: 3,
            }),
            DurableCheckpointDisposition::Incompatible
        );
        assert_eq!(
            adapter.checkpoint_disposition(&CheckpointError::InvalidJson("bad".to_string())),
            DurableCheckpointDisposition::Rejected
        );
        assert_eq!(
            adapter.checkpoint_disposition(&CheckpointError::ExecutionMismatch {
                expected: execution.execution_id(),
                actual: ExecutionId::from_bytes([99; 16]),
            }),
            DurableCheckpointDisposition::Isolated
        );
        let wait = adapter.preparation_wait(&CheckpointError::PreparedActivityRejected(
            PreparedActivityError::Derivation,
        ));
        assert_eq!(wait.reason, "AwaitingFreshAuthority");
        assert_eq!(wait.requeue_after_seconds, Some(1));
    }

    #[tokio::test]
    async fn framework_native_remove_replica_fr019_observation_collection() {
        let reference = reference();
        let set = adapter_set(&reference);
        let pods = adapter_pods();
        let current_pods = adapter_current_pods(&pods);
        let operation = reconstruct_initial_operation(reference.input.as_ref().unwrap()).unwrap();
        let api = adapter_api(&operation, DispatchResult::Busy);
        let mut adapter =
            FrameworkNativeRemoveReplicaAdapter::new(&reference, &set, &current_pods, &api, 10)
                .unwrap();

        adapter.prepare().await.unwrap();
        let evidence = adapter.evidence().unwrap();
        let (reconstructed, pods) = evidence.reconstruct(&adapter.operation).unwrap();
        assert_eq!(reconstructed.len(), 3);
        assert_eq!(pods, pod_identities());
        assert_eq!(adapter.context().unwrap().handles.len(), 3);
    }

    #[tokio::test]
    async fn framework_native_remove_replica_fr019_authority_and_preparation() {
        let reference = reference();
        let set = adapter_set(&reference);
        let pods = adapter_pods();
        let current_pods = adapter_current_pods(&pods);
        let operation = reconstruct_initial_operation(reference.input.as_ref().unwrap()).unwrap();
        let api = adapter_api(&operation, DispatchResult::Busy);
        let mut adapter =
            FrameworkNativeRemoveReplicaAdapter::new(&reference, &set, &current_pods, &api, 10)
                .unwrap();

        adapter.prepare().await.unwrap();
        let logical = activity_spec(&logical_boundary(&adapter.operation)).unwrap();
        let prepared = adapter.resolver().resolve(&logical, None).unwrap();
        let RemoveReplicaBoundaryInput::Observe { .. } =
            decode_boundary_input(prepared.input()).unwrap()
        else {
            panic!("first prepared boundary must collect authoritative observations");
        };
    }

    #[tokio::test]
    async fn framework_native_remove_replica_fr019_exact_effect_dispatch_and_quarantine() {
        let reference = reference();
        let set = adapter_set(&reference);
        let pods = adapter_pods();
        let current_pods = adapter_current_pods(&pods);
        let execution = execution_spec(&reference).unwrap();
        let operation = reconstruct_initial_operation(reference.input.as_ref().unwrap()).unwrap();
        let api = adapter_api(&operation, DispatchResult::Ambiguous);
        let backend = InMemoryCheckpointStore::new();
        let store = adapter_store(&execution, backend.clone());
        let mut host = RemoveReplicaHost::new(
            store.clone(),
            HostEpoch::from_bytes([44; 16]),
            checkpoint_limits(),
        );
        let mut adapter =
            FrameworkNativeRemoveReplicaAdapter::new(&reference, &set, &current_pods, &api, 10)
                .unwrap();

        let outcome = DurableRunner::new(REMOVE_REPLICA_MAX_ACTIVITY_RECORDS)
            .unwrap()
            .run(
                &mut host,
                &RemoveReplicaWorkflow,
                execution.clone(),
                &mut adapter,
                10,
            )
            .await;
        assert!(matches!(
            outcome,
            DurableRunnerOutcome::Active {
                reason: DurableActiveReason::Adapter,
                ref condition_reason,
                ..
            } if condition_reason == "EffectExposed"
        ));
        assert_eq!(api.requests.lock().unwrap().len(), 1);
        let stored = backend
            .load(execution.execution_id())
            .await
            .unwrap()
            .unwrap();
        let payload = stored
            .checkpoint()
            .decode_and_validate(&execution, checkpoint_limits())
            .unwrap();
        let last = payload.active_activities().unwrap().last().unwrap();
        assert!(matches!(
            last.state(),
            kuberic_durable_execution::ActivityState::DispatchExposed { .. }
        ));
        assert!(matches!(
            decode_boundary_input(last.input()).unwrap(),
            RemoveReplicaBoundaryInput::ReplicaCommand { .. }
        ));

        let exposed_operation =
            replay_active_operation(&reference, payload.active_activities().unwrap()).unwrap();
        let action_id = exposed_operation
            .remove_intent
            .as_ref()
            .unwrap()
            .action_id
            .clone();
        {
            let mut statuses = api.statuses.lock().unwrap();
            let primary = statuses.get_mut(&exposed_operation.old_primary_id).unwrap();
            primary.configuration = Some(
                core_intent(&exposed_operation)
                    .unwrap()
                    .reduced_current_status(),
            );
            primary
                .active_replica_connections
                .retain(|connection| connection.id != exposed_operation.target_replica_id.unwrap());
            primary.agent.current_action = Some(completed_action(&exposed_operation, action_id));
        }
        let reloaded_store = adapter_store(&execution, backend);
        let mut reloaded_host = RemoveReplicaHost::new(
            reloaded_store.clone(),
            HostEpoch::from_bytes([45; 16]),
            checkpoint_limits(),
        );
        let mut reloaded_adapter =
            FrameworkNativeRemoveReplicaAdapter::new(&reference, &set, &current_pods, &api, 10)
                .unwrap();
        let recovered = DurableRunner::new(REMOVE_REPLICA_MAX_ACTIVITY_RECORDS)
            .unwrap()
            .run(
                &mut reloaded_host,
                &RemoveReplicaWorkflow,
                execution,
                &mut reloaded_adapter,
                10,
            )
            .await;
        assert!(
            matches!(
                &recovered,
                DurableRunnerOutcome::Active {
                    reason: DurableActiveReason::Adapter,
                    condition_reason,
                    ..
                } if condition_reason == "RefreshingAuthority"
            ),
            "unexpected quarantine recovery outcome: {recovered:?}"
        );
        assert_eq!(
            api.requests.lock().unwrap().len(),
            1,
            "quarantine recovery must not redispatch the exposed command"
        );
    }

    #[test]
    fn framework_native_remove_replica_fr019_deadline_policy() {
        let reference = reference();
        let set = adapter_set(&reference);
        let pods = adapter_pods();
        let current_pods = adapter_current_pods(&pods);
        let (operation, _) = freeze_and_dispatch_for(&reference);
        let api = adapter_api(&operation, DispatchResult::Busy);
        let mut adapter =
            FrameworkNativeRemoveReplicaAdapter::new(&reference, &set, &current_pods, &api, 10)
                .unwrap();
        adapter.operation = operation;
        let wait = adapter.preparation_wait(&CheckpointError::PreparedActivityRejected(
            PreparedActivityError::Validation,
        ));
        assert_eq!(wait.reason, "AwaitingFreshAuthority");
        assert_eq!(wait.requeue_after_seconds, Some(1));
        assert_eq!(
            adapter.deadline_unix_seconds(),
            adapter
                .operation
                .pending_action
                .as_ref()
                .map(|pending| pending.deadline_unix_seconds)
                .unwrap_or(adapter.operation.phase_deadline_unix_seconds)
        );
    }

    #[test]
    fn framework_native_remove_replica_fr019_terminal_validation() {
        let reference = reference();
        let set = adapter_set(&reference);
        let pods = adapter_pods();
        let current_pods = adapter_current_pods(&pods);
        let operation = committed_operation_for(&reference);
        let api = adapter_api(&operation, DispatchResult::Busy);
        let mut adapter =
            FrameworkNativeRemoveReplicaAdapter::new(&reference, &set, &current_pods, &api, 10)
                .unwrap();
        let terminal = terminal_from_operation(&operation, Default::default()).unwrap();
        let outcome = encode_terminal(terminal);
        assert!(matches!(
            adapter.validate_terminal(outcome.clone(), 0),
            Ok(RemoveReplicaTerminal::Completed { .. })
        ));
        assert!(matches!(
            adapter.validate_terminal(outcome, 1),
            Err(DurableAdapterBoundary::Rejected(_))
        ));
    }

    #[tokio::test]
    async fn framework_native_remove_replica_fr019_publication_handoff() {
        let reference = reference();
        let set = adapter_set(&reference);
        let pods = adapter_pods();
        let current_pods = adapter_current_pods(&pods);
        let execution = execution_spec(&reference).unwrap();
        let operation = committed_operation_for(&reference);
        let terminal = terminal_from_operation(&operation, Default::default()).unwrap();
        let outcome = encode_terminal(terminal);
        let checkpoint = CheckpointEnvelope::encode_with_limits(
            &CheckpointPayload::terminal(
                ExecutionContract::with_encoded_limits(
                    execution.clone(),
                    REMOVE_REPLICA_MAX_ACTIVE_ENCODED_BYTES as u64,
                    REMOVE_REPLICA_MAX_TERMINAL_ENCODED_BYTES as u64,
                ),
                outcome,
                0,
            ),
            checkpoint_limits(),
        )
        .unwrap();
        let backend = InMemoryCheckpointStore::new();
        assert!(matches!(
            backend
                .compare_and_swap(execution.execution_id(), None, checkpoint)
                .await
                .unwrap(),
            CasOutcome::Accepted(_)
        ));
        let store = adapter_store(&execution, backend);
        let mut host = RemoveReplicaHost::new(
            store.clone(),
            HostEpoch::from_bytes([45; 16]),
            checkpoint_limits(),
        );
        let api = adapter_api(&operation, DispatchResult::Busy);
        let mut adapter =
            FrameworkNativeRemoveReplicaAdapter::new(&reference, &set, &current_pods, &api, 10)
                .unwrap();
        let published = DurableRunner::new(REMOVE_REPLICA_MAX_ACTIVITY_RECORDS)
            .unwrap()
            .run(
                &mut host,
                &RemoveReplicaWorkflow,
                execution,
                &mut adapter,
                10,
            )
            .await;
        assert!(matches!(
            published,
            DurableRunnerOutcome::Terminal(RemoveReplicaTerminal::Completed { .. })
        ));
        assert!(api.requests.lock().unwrap().is_empty());
    }

    #[test]
    fn framework_native_remove_replica_checkpoint_owner_is_exact_and_non_controlling() {
        let options = checkpoint_store_options("default", "database", "set-uid").unwrap();
        let owner = options.owner().unwrap();
        assert_eq!(
            owner.scope(),
            &KubernetesCheckpointOwnerScope::Namespaced("default".to_string())
        );
        assert_eq!(owner.reference().api_version, "kuberic.io/v1");
        assert_eq!(owner.reference().kind, "KubericSet");
        assert_eq!(owner.reference().name, "database");
        assert_eq!(owner.reference().uid, "set-uid");
        assert_eq!(owner.reference().controller, Some(false));
        assert_eq!(owner.reference().block_owner_deletion, Some(false));
    }

    #[test]
    fn framework_native_remove_replica_uid_fenced_label_and_delete_commands() {
        let reference = reference();
        let operation = reconstruct_initial_operation(reference.input.as_ref().unwrap()).unwrap();
        let label = prepare_remove_label_effect_command(
            &operation,
            3,
            "three",
            "retired",
            &pod_identities(),
        )
        .unwrap();
        let delete =
            prepare_remove_delete_effect_command(&operation, "set-2", "three", &pod_identities())
                .unwrap();
        validate_prepared_activity(
            &operation,
            &RemoveReplicaBoundaryInput::LabelCommand { command: label },
        )
        .unwrap();
        validate_prepared_activity(
            &operation,
            &RemoveReplicaBoundaryInput::DeleteCommand { command: delete },
        )
        .unwrap();
    }
}
