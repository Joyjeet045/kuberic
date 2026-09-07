//! Bounded durable-execution workflow for remove-replica.

use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock},
};

use async_trait::async_trait;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kuberic_core::types::{DurableReplicaAction, ReplicaInstanceId};
use kuberic_durable_execution::{
    ActivityName, ActivityRecord, ActivitySequence, ActivitySpec, CheckpointEnvelope,
    CheckpointLimits, CheckpointPayload, DispatchPermit, DurableActivity, ExactBytes,
    ExecutionContract, ExecutionId, ExecutionSpec, InMemoryCheckpointStore,
    KubernetesCheckpointOwner, KubernetesCheckpointOwnerScope, KubernetesCheckpointStore,
    KubernetesCheckpointStoreOptions, LogicalActivityId, PreparedActivityError,
    PreparedActivityResolver, TerminalOutcome, Workflow, WorkflowContext, decode_activity_input,
    decode_activity_result, encode_activity_input, encode_activity_result,
};
use rand::random;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::crd::{
    DurableOperationPhase, DurableOperationStatus, DurableRemoveMode,
    DurableRemoveReplicaPilotStatus, PendingActionStatus, RemoveReplicaCleanupStatus,
    RemoveReplicaCommitEvidenceStatus, RemoveReplicaDispositionStatus, RemoveReplicaIntentStatus,
    StablePartitionSnapshotStatus, TargetRetirementObservationStatus,
};

use super::effects::{
    DeleteEffectCommand, DurableEffectPreparationError, LabelEffectCommand, ReplicaEffectCommand,
    prepare_lifecycle_replica_effect_command, prepare_remove_delete_effect_command,
    prepare_remove_label_effect_command, validate_remove_replica_action_kind,
    validate_remove_replica_dispatch_authority,
};
use super::pilot_store::{
    CheckpointMeasurementDecoder, DurableActivityAccounting, DurableActivityClass,
    DurableCheckpointMeasurementsSnapshot,
};
use super::remove_replica::{core_intent, validate_remove_replica_operation};
use super::workflow_host::{DurableOperatorHost, DurablePermitGuard, DurableWorkflowRuntime};
use super::{
    Decision, OperationObservations, OperationPodIdentities, RemoveReplicaTarget,
    decide_remove_replica, start_remove_replica,
};

// COMPLEXITY-BOUNDARY: remove-replica-pilot-module:start
pub const REMOVE_REPLICA_PILOT_VERSION: u32 = 1;
pub const REMOVE_REPLICA_PILOT_MAX_REPLICAS: usize = 3;
pub const REMOVE_REPLICA_PILOT_MAX_ACTIVITY_RECORDS: usize = 16;
pub const REMOVE_REPLICA_PILOT_MAX_TRANSITION_FUEL: usize = 48;
pub const REMOVE_REPLICA_PILOT_MAX_WORKFLOW_INPUT_BYTES: usize = 8_192;
pub const REMOVE_REPLICA_PILOT_MAX_ACTIVITY_INPUT_BYTES: usize = 16_384;
pub const REMOVE_REPLICA_PILOT_MAX_ACTIVITY_RESULT_BYTES: usize = 8_192;
pub const REMOVE_REPLICA_PILOT_MAX_TERMINAL_BYTES: u64 = 4_096;
pub const REMOVE_REPLICA_PILOT_MAX_ENCODED_CHECKPOINT_BYTES: usize = 752 * 1_024;

const REMOVE_REPLICA_ACTIVITY_NAME: &str = "kuberic.remove-replica.effect-boundary";
const REMOVE_REPLICA_ACTIVITY_VERSION: u32 = 1;
const MAX_TERMINAL_MESSAGE_BYTES: usize = 512;
const PROJECTED_MAX_ACTIVITY_RECORDS: usize = 16;
const PROJECTED_MAX_DETERMINISTIC_TRANSITIONS: usize = 8;

pub type RemoveReplicaPilotHost = DurableOperatorHost;

pub type DurableRemoveReplicaPilotExecution = DurableRemoveReplicaPilotStatus;

pub struct DurableRemoveReplicaPilotRuntime {
    inner: Arc<DurableWorkflowRuntime>,
}

impl DurableRemoveReplicaPilotRuntime {
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
        reference: &DurableRemoveReplicaPilotExecution,
    ) -> Result<Arc<Mutex<RemoveReplicaPilotHost>>, String> {
        let execution_id = execution_id(reference)?;
        self.inner
            .host(
                namespace,
                set_name,
                set_uid,
                "remove-replica",
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
            .forget(namespace, set_name, set_uid, "remove-replica", execution_id)
            .await;
    }

    pub async fn measurements(
        &self,
        namespace: &str,
        set_name: &str,
        set_uid: &str,
        execution_id: &str,
    ) -> Option<DurableCheckpointMeasurementsSnapshot> {
        self.inner
            .measurements(namespace, set_name, set_uid, "remove-replica", execution_id)
            .await
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DurableRemoveReplicaPilotInput {
    pub version: u32,
    pub execution_id: String,
    pub initial_operation: DurableOperationStatus,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DurableRemoveReplicaState {
    pub phase: DurableOperationPhase,
    pub committed_snapshot: Option<StablePartitionSnapshotStatus>,
    pub phase_deadline_unix_seconds: i64,
    pub pending_action: Option<PendingActionStatus>,
    pub last_error: Option<String>,
    pub remove_intent: Option<Box<RemoveReplicaIntentStatus>>,
    pub remove_commit_evidence: Option<RemoveReplicaCommitEvidenceStatus>,
    pub remove_cleanup: Option<RemoveReplicaCleanupStatus>,
    pub removal_disposition: Option<RemoveReplicaDispositionStatus>,
}

impl DurableRemoveReplicaState {
    pub fn from_operation(operation: &DurableOperationStatus) -> Self {
        Self {
            phase: operation.phase,
            committed_snapshot: operation.committed_snapshot.clone(),
            phase_deadline_unix_seconds: operation.phase_deadline_unix_seconds,
            pending_action: operation.pending_action.clone(),
            last_error: operation.last_error.clone(),
            remove_intent: operation.remove_intent.clone(),
            remove_commit_evidence: operation.remove_commit_evidence.clone(),
            remove_cleanup: operation.remove_cleanup.clone(),
            removal_disposition: operation.removal_disposition.clone(),
        }
    }

    pub fn apply_to(
        &self,
        initial: &DurableOperationStatus,
    ) -> Result<DurableOperationStatus, String> {
        let mut operation = initial.clone();
        operation.phase = self.phase;
        operation.committed_snapshot = self.committed_snapshot.clone();
        operation.phase_deadline_unix_seconds = self.phase_deadline_unix_seconds;
        operation.pending_action = self.pending_action.clone();
        operation.last_error = self.last_error.clone();
        operation.remove_intent = self.remove_intent.clone();
        operation.remove_commit_evidence = self.remove_commit_evidence.clone();
        operation.remove_cleanup = self.remove_cleanup.clone();
        operation.removal_disposition = self.removal_disposition.clone();
        validate_remove_replica_operation(&operation)?;
        validate_immutable_authority(initial, &operation)?;
        Ok(operation)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DurableRemoveReplicaActivityInput {
    pub version: u32,
    pub state: DurableRemoveReplicaState,
    #[serde(default)]
    pub kind: RemoveReplicaActivityKind,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RemoveReplicaActivityKind {
    #[default]
    PassiveObservation,
    PreparedReplica {
        command: ReplicaEffectCommand,
    },
    PreparedLabel {
        command: LabelEffectCommand,
    },
    PreparedDelete {
        command: DeleteEffectCommand,
    },
}

impl RemoveReplicaActivityKind {
    pub(crate) fn completion_class(&self) -> RemoveReplicaActivityCompletion {
        match self {
            Self::PassiveObservation => RemoveReplicaActivityCompletion::PassiveObservation,
            Self::PreparedReplica { .. }
            | Self::PreparedLabel { .. }
            | Self::PreparedDelete { .. } => RemoveReplicaActivityCompletion::ExternalEffect,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoveReplicaActivityCompletion {
    ExternalEffect,
    PassiveObservation,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoveReplicaActivityAccounting {
    pub external_effect_count: u64,
    pub passive_observation_count: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoveReplicaTerminalCommitAuthority {
    pub attempt: u32,
    pub attempt_id: String,
    pub action_id: String,
    pub input_signature: String,
    pub primary_agent_generation: String,
    pub configuration_signature: String,
    pub binding_signature: String,
}

impl RemoveReplicaActivityAccounting {
    fn record(&mut self, completion: RemoveReplicaActivityCompletion) -> Result<(), String> {
        match completion {
            RemoveReplicaActivityCompletion::ExternalEffect => {
                self.external_effect_count = self
                    .external_effect_count
                    .checked_add(1)
                    .ok_or_else(|| "remove activity accounting overflowed".to_string())?;
            }
            RemoveReplicaActivityCompletion::PassiveObservation => {
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
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum DurableRemoveReplicaStepResult {
    Advance {
        operation: DurableRemoveReplicaState,
        completion: RemoveReplicaActivityCompletion,
    },
    ProvenNoAdmission {
        operation: DurableRemoveReplicaState,
        action_id: String,
        redelivery: u8,
    },
}

pub struct DurableRemoveReplicaActivity;

impl DurableActivity for DurableRemoveReplicaActivity {
    type Input = DurableRemoveReplicaActivityInput;
    type Output = DurableRemoveReplicaStepResult;

    const NAME: &'static str = REMOVE_REPLICA_ACTIVITY_NAME;
    const VERSION: u32 = REMOVE_REPLICA_ACTIVITY_VERSION;
    const MAX_INPUT_BYTES: u64 = REMOVE_REPLICA_PILOT_MAX_ACTIVITY_INPUT_BYTES as u64;
    const MAX_RESULT_BYTES: u64 = REMOVE_REPLICA_PILOT_MAX_ACTIVITY_RESULT_BYTES as u64;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum DurableRemoveReplicaPilotTerminal {
    Completed {
        authority: RemoveReplicaTerminalCommitAuthority,
        commit_evidence: RemoveReplicaCommitEvidenceStatus,
        cleanup: RemoveReplicaCleanupStatus,
        accounting: RemoveReplicaActivityAccounting,
    },
    Compensated {
        message: String,
        accounting: RemoveReplicaActivityAccounting,
    },
    UnsafeAmbiguity {
        committed: bool,
        commit_evidence: Option<RemoveReplicaCommitEvidenceStatus>,
        cleanup: Option<RemoveReplicaCleanupStatus>,
        disposition: RemoveReplicaDispositionStatus,
        message: String,
        accounting: RemoveReplicaActivityAccounting,
    },
    Rejected {
        message: String,
        accounting: RemoveReplicaActivityAccounting,
    },
}

// COMPLEXITY-BOUNDARY: remove-replica-pilot-workflow:start
pub struct DurableRemoveReplicaWorkflow;

enum DeterministicAdvance {
    State(Box<DurableOperationStatus>),
    Terminal(Box<DurableRemoveReplicaPilotTerminal>),
}

// COMPLEXITY-BOUNDARY: remove-replica-pilot-workflow-body:start
#[async_trait]
impl Workflow for DurableRemoveReplicaWorkflow {
    async fn run(&self, context: &mut WorkflowContext<'_>, input: ExactBytes) -> TerminalOutcome {
        let input: DurableRemoveReplicaPilotInput = match serde_json::from_slice(input.as_slice()) {
            Ok(input) => input,
            Err(error) => {
                return rejected_terminal(
                    format!("decode durable remove workflow input: {error}"),
                    RemoveReplicaActivityAccounting::default(),
                );
            }
        };
        if input.version != REMOVE_REPLICA_PILOT_VERSION {
            return rejected_terminal(
                format!(
                    "unsupported durable remove workflow version {}",
                    input.version
                ),
                RemoveReplicaActivityAccounting::default(),
            );
        }
        if input.execution_id != encode_execution_id(context.execution_id()) {
            return rejected_terminal(
                "durable remove workflow execution identity mismatch".to_string(),
                RemoveReplicaActivityAccounting::default(),
            );
        }
        let initial = input.initial_operation;
        if let Err(error) = validate_remove_replica_admission(&initial) {
            return rejected_terminal(error, RemoveReplicaActivityAccounting::default());
        }

        let mut state = DurableRemoveReplicaState::from_operation(&initial);
        let mut accounting = RemoveReplicaActivityAccounting::default();
        let mut no_admission_redeliveries = BTreeMap::<String, u8>::new();

        for _ in 0..REMOVE_REPLICA_PILOT_MAX_TRANSITION_FUEL {
            let operation = match state.apply_to(&initial) {
                Ok(operation) => operation,
                Err(error) => return rejected_terminal(error, accounting),
            };
            match advance_deterministic(&operation, accounting) {
                Ok(Some(DeterministicAdvance::State(next))) => {
                    if let Err(error) = validate_remove_transition(&operation, &next) {
                        return rejected_terminal(error, accounting);
                    }
                    state = DurableRemoveReplicaState::from_operation(next.as_ref());
                    continue;
                }
                Ok(Some(DeterministicAdvance::Terminal(terminal))) => {
                    return encode_terminal(*terminal);
                }
                Ok(None) => {}
                Err(error) => return rejected_terminal(error, accounting),
            }

            let result = match context
                .call::<DurableRemoveReplicaActivity>(DurableRemoveReplicaActivityInput {
                    version: REMOVE_REPLICA_PILOT_VERSION,
                    state: state.clone(),
                    kind: RemoveReplicaActivityKind::PassiveObservation,
                })
                .await
            {
                Ok(result) => result,
                Err(error) => {
                    return rejected_terminal(
                        format!("decode durable remove activity result: {error}"),
                        accounting,
                    );
                }
            };
            match result {
                DurableRemoveReplicaStepResult::Advance {
                    operation: next_state,
                    completion,
                } => {
                    accounting.record(completion).unwrap_or_else(|error| {
                        panic!("validated bounded activity count overflowed: {error}")
                    });
                    let next = match next_state.apply_to(&initial) {
                        Ok(next) => next,
                        Err(error) => return rejected_terminal(error, accounting),
                    };
                    if next_state == state {
                        return rejected_terminal(
                            "durable remove activity made no protocol progress".to_string(),
                            accounting,
                        );
                    }
                    if let Err(error) = validate_remove_transition(&operation, &next) {
                        return rejected_terminal(error, accounting);
                    }
                    state = next_state;
                }
                DurableRemoveReplicaStepResult::ProvenNoAdmission {
                    operation: next_state,
                    action_id,
                    redelivery,
                } => {
                    accounting
                        .record(RemoveReplicaActivityCompletion::ExternalEffect)
                        .unwrap_or_else(|error| {
                            panic!("validated bounded activity count overflowed: {error}")
                        });
                    let next = match next_state.apply_to(&initial) {
                        Ok(next) => next,
                        Err(error) => return rejected_terminal(error, accounting),
                    };
                    if let Err(error) =
                        validate_no_admission_transition(&operation, &next, &action_id)
                    {
                        return rejected_terminal(error, accounting);
                    }
                    if redelivery != 1 {
                        return rejected_terminal(
                            "invalid remove proven-no-admission redelivery evidence".to_string(),
                            accounting,
                        );
                    }
                    let count = no_admission_redeliveries.entry(action_id).or_default();
                    *count = count.saturating_add(1);
                    if *count > 1 {
                        return rejected_terminal(
                            "remove action exceeded one proven-no-admission redelivery".to_string(),
                            accounting,
                        );
                    }
                    state = next_state;
                }
            }
        }

        rejected_terminal(
            format!(
                "durable remove exhausted its \
                 {REMOVE_REPLICA_PILOT_MAX_TRANSITION_FUEL}-transition fuel"
            ),
            accounting,
        )
    }
}
// COMPLEXITY-BOUNDARY: remove-replica-pilot-workflow-body:end

fn advance_deterministic(
    operation: &DurableOperationStatus,
    accounting: RemoveReplicaActivityAccounting,
) -> Result<Option<DeterministicAdvance>, String> {
    use DurableOperationPhase as Phase;
    if operation.phase == Phase::Poisoned {
        return Ok(Some(DeterministicAdvance::Terminal(Box::new(
            terminal_from_operation(operation, accounting)?,
        ))));
    }
    if !matches!(
        operation.phase,
        Phase::RemoveDispatchIntent
            | Phase::RemoveRecordCommit
            | Phase::RemovePublishTopology
            | Phase::RemoveFinalize
            | Phase::RemoveCompensateFinalize
            | Phase::Completed
            | Phase::Failed
    ) {
        return Ok(None);
    }
    match decide_remove_replica(
        operation,
        &OperationObservations::new(),
        &OperationPodIdentities::new(),
        None,
        operation.phase_deadline_unix_seconds,
    )? {
        Decision::Persist(next) => Ok(Some(DeterministicAdvance::State(Box::new(next)))),
        Decision::CommitSnapshot {
            operation: next,
            snapshot,
        } => {
            if snapshot != operation.target_snapshot {
                return Err(
                    "remove publication decision changed the frozen reduced snapshot".to_string(),
                );
            }
            Ok(Some(DeterministicAdvance::State(Box::new(next))))
        }
        Decision::Complete {
            operation: completed,
            snapshot,
            compensated,
        } => {
            let expected = if compensated {
                &operation.previous_snapshot
            } else {
                &operation.target_snapshot
            };
            if &snapshot != expected {
                return Err("remove terminal decision changed the frozen snapshot".to_string());
            }
            Ok(Some(DeterministicAdvance::Terminal(Box::new(
                terminal_from_operation(&completed, accounting)?,
            ))))
        }
        Decision::Wait if matches!(operation.phase, Phase::Completed | Phase::Failed) => {
            Ok(Some(DeterministicAdvance::Terminal(Box::new(
                terminal_from_operation(operation, accounting)?,
            ))))
        }
        other => Err(format!(
            "deterministic remove transition produced external decision: {other:?}"
        )),
    }
}

fn terminal_from_operation(
    operation: &DurableOperationStatus,
    accounting: RemoveReplicaActivityAccounting,
) -> Result<DurableRemoveReplicaPilotTerminal, String> {
    use DurableOperationPhase as Phase;
    match operation.phase {
        Phase::Completed => {
            let commit_evidence = operation
                .remove_commit_evidence
                .clone()
                .ok_or_else(|| "completed remove terminal has no commit evidence".to_string())?;
            let intent = operation
                .remove_intent
                .as_deref()
                .ok_or_else(|| "completed remove terminal has no frozen intent".to_string())?;
            let cleanup = operation
                .remove_cleanup
                .clone()
                .ok_or_else(|| "completed remove terminal has no cleanup evidence".to_string())?;
            validate_completed_cleanup(&cleanup)?;
            let mut authority = RemoveReplicaTerminalCommitAuthority {
                attempt: intent.attempt,
                attempt_id: intent.attempt_id.clone(),
                action_id: intent.action_id.clone(),
                input_signature: intent.input_signature.clone(),
                primary_agent_generation: commit_evidence.primary_agent_generation.clone(),
                configuration_signature: commit_evidence.configuration_signature.clone(),
                binding_signature: String::new(),
            };
            authority.binding_signature = terminal_commit_authority_binding(operation, &authority)?;
            Ok(DurableRemoveReplicaPilotTerminal::Completed {
                authority,
                commit_evidence,
                cleanup,
                accounting,
            })
        }
        Phase::Failed if operation.remove_commit_evidence.is_none() => {
            Ok(DurableRemoveReplicaPilotTerminal::Compensated {
                message: bounded_terminal_message(
                    operation
                        .last_error
                        .as_deref()
                        .unwrap_or("remove-replica pre-commit work was compensated"),
                ),
                accounting,
            })
        }
        Phase::Poisoned => {
            let disposition = operation
                .removal_disposition
                .clone()
                .ok_or_else(|| "poisoned remove terminal has no typed disposition".to_string())?;
            Ok(DurableRemoveReplicaPilotTerminal::UnsafeAmbiguity {
                committed: operation.committed_snapshot.is_some(),
                commit_evidence: operation.remove_commit_evidence.clone(),
                cleanup: operation.remove_cleanup.clone(),
                disposition,
                message: bounded_terminal_message(
                    operation
                        .last_error
                        .as_deref()
                        .unwrap_or("remove-replica cannot advance safely"),
                ),
                accounting,
            })
        }
        _ => Err(format!(
            "remove terminal cannot be constructed from phase {:?}",
            operation.phase
        )),
    }
}

fn validate_completed_cleanup(cleanup: &RemoveReplicaCleanupStatus) -> Result<(), String> {
    let terminal_retirement = matches!(
        cleanup.target_retirement,
        Some(
            TargetRetirementObservationStatus::Completed
                | TargetRetirementObservationStatus::Unavailable
                | TargetRetirementObservationStatus::Stale
                | TargetRetirementObservationStatus::Failed
        )
    );
    if cleanup.connection_absent
        && terminal_retirement
        && cleanup.target_labels_fenced
        && cleanup.target_pod_deleted
    {
        Ok(())
    } else {
        Err("completed remove terminal lacks exact cleanup evidence".to_string())
    }
}

pub fn encode_step_result(result: &DurableRemoveReplicaStepResult) -> Result<ExactBytes, String> {
    encode_activity_result::<DurableRemoveReplicaActivity>(result)
        .map_err(|error| format!("serialize durable remove activity result: {error}"))
}

pub fn decode_activity_step_result(
    result: &ExactBytes,
) -> Result<DurableRemoveReplicaStepResult, String> {
    decode_activity_result::<DurableRemoveReplicaActivity>(result)
        .map_err(|error| format!("decode completed durable remove activity: {error}"))
}

pub fn decode_remove_activity_input(
    input: &ExactBytes,
) -> Result<DurableRemoveReplicaActivityInput, String> {
    let input = decode_activity_input::<DurableRemoveReplicaActivity>(input)
        .map_err(|error| format!("decode current durable remove activity: {error}"))?;
    if input.version != REMOVE_REPLICA_PILOT_VERSION {
        return Err(format!(
            "unsupported durable remove activity version {}",
            input.version
        ));
    }
    Ok(input)
}

pub fn completed_step(
    decision: RemoveReplicaAdapterDecision,
    prepared: &RemoveReplicaActivityKind,
) -> Result<DurableRemoveReplicaStepResult, String> {
    let RemoveReplicaAdapterDecision::Advance(operation) = decision else {
        return Err("remove activity has no authoritative completed result".to_string());
    };
    Ok(DurableRemoveReplicaStepResult::Advance {
        operation: *operation,
        completion: prepared.completion_class(),
    })
}

fn classify_checkpoint_activity(input: &ExactBytes) -> Option<DurableActivityClass> {
    let input = decode_remove_activity_input(input).ok()?;
    Some(match input.kind {
        RemoveReplicaActivityKind::PassiveObservation => DurableActivityClass::PassiveObservation,
        RemoveReplicaActivityKind::PreparedReplica { .. }
        | RemoveReplicaActivityKind::PreparedLabel { .. }
        | RemoveReplicaActivityKind::PreparedDelete { .. } => DurableActivityClass::ExternalEffect,
    })
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

pub fn checkpoint_measurement_decoder() -> CheckpointMeasurementDecoder {
    CheckpointMeasurementDecoder::new(
        "remove-replica",
        classify_checkpoint_activity,
        decode_checkpoint_terminal_accounting,
    )
}

pub fn decode_terminal(
    outcome: &TerminalOutcome,
) -> Result<DurableRemoveReplicaPilotTerminal, String> {
    let terminal: DurableRemoveReplicaPilotTerminal =
        serde_json::from_slice(outcome.payload().as_slice())
            .map_err(|error| format!("decode durable remove terminal outcome: {error}"))?;
    match (outcome, &terminal) {
        (
            TerminalOutcome::Succeeded(_),
            DurableRemoveReplicaPilotTerminal::Completed { cleanup, .. },
        ) => validate_completed_cleanup(cleanup)?,
        (TerminalOutcome::Succeeded(_), DurableRemoveReplicaPilotTerminal::Compensated { .. })
        | (TerminalOutcome::Failed(_), DurableRemoveReplicaPilotTerminal::UnsafeAmbiguity { .. })
        | (TerminalOutcome::Failed(_), DurableRemoveReplicaPilotTerminal::Rejected { .. }) => {}
        _ => {
            return Err(
                "kernel terminal outcome kind does not match durable remove payload".to_string(),
            );
        }
    }
    Ok(terminal)
}

pub fn validate_loaded_terminal(
    reference: &DurableRemoveReplicaPilotExecution,
    outcome: &TerminalOutcome,
    completed_activity_count: u64,
) -> Result<DurableRemoveReplicaPilotTerminal, String> {
    let initial = initial_operation(reference)?;
    validate_remove_replica_admission(&initial)?;
    let terminal = decode_terminal(outcome)?;
    let accounting = terminal_accounting(&terminal);
    if accounting.total() != Some(completed_activity_count)
        || completed_activity_count
            > u64::try_from(REMOVE_REPLICA_PILOT_MAX_ACTIVITY_RECORDS)
                .expect("activity bound fits u64")
    {
        return Err("remove terminal activity accounting is not authoritative".to_string());
    }
    match &terminal {
        DurableRemoveReplicaPilotTerminal::Completed {
            authority,
            commit_evidence,
            cleanup,
            ..
        } => {
            let expected_attempt_id =
                format!("{}:attempt-{}", initial.operation_id, authority.attempt);
            let expected_action_id = format!("{}:RemoveReplicaIntent", authority.attempt_id);
            let expected_quorum_prefix = format!("q{}[", initial.target_snapshot.write_quorum);
            let retained_identities_match = initial
                .target_snapshot
                .members
                .iter()
                .filter(|member| member.id != initial.target_snapshot.primary_id)
                .all(|member| {
                    authority
                        .configuration_signature
                        .contains(&format!("{}@{}:", member.id, member.instance_id))
                });
            let exact_binding = terminal_commit_authority_binding(&initial, authority)?
                == authority.binding_signature;
            if authority.attempt == 0
                || authority.attempt
                    > kuberic_core::remove_replica::MAX_REMOVE_REPLICA_PRE_COMMIT_ATTEMPTS
                || authority.attempt_id != expected_attempt_id
                || authority.action_id != expected_action_id
            {
                return Err("completed remove terminal changed action identity".to_string());
            }
            if authority.input_signature.is_empty()
                || kuberic_core::types::AgentGeneration::parse(
                    authority.primary_agent_generation.clone(),
                )
                .is_err()
            {
                return Err("completed remove terminal changed dispatch authority".to_string());
            }
            if !authority
                .configuration_signature
                .starts_with(&expected_quorum_prefix)
                || !retained_identities_match
                || !exact_binding
            {
                return Err("completed remove terminal changed reduced configuration".to_string());
            }
            if commit_evidence.attempt_id != authority.attempt_id
                || commit_evidence.action_id != authority.action_id
                || commit_evidence.primary_agent_generation != authority.primary_agent_generation
                || commit_evidence.configuration_signature != authority.configuration_signature
            {
                return Err(
                    "completed remove terminal has inconsistent exact commit authority".to_string(),
                );
            }
            validate_completed_cleanup(cleanup)?;
        }
        DurableRemoveReplicaPilotTerminal::Compensated { .. } => {}
        DurableRemoveReplicaPilotTerminal::UnsafeAmbiguity {
            committed,
            commit_evidence,
            ..
        } => {
            if *committed != commit_evidence.is_some() {
                return Err("unsafe remove terminal has inconsistent commit evidence".to_string());
            }
        }
        DurableRemoveReplicaPilotTerminal::Rejected { .. } => {}
    }
    Ok(terminal)
}

fn terminal_accounting(
    terminal: &DurableRemoveReplicaPilotTerminal,
) -> RemoveReplicaActivityAccounting {
    match terminal {
        DurableRemoveReplicaPilotTerminal::Completed { accounting, .. }
        | DurableRemoveReplicaPilotTerminal::Compensated { accounting, .. }
        | DurableRemoveReplicaPilotTerminal::UnsafeAmbiguity { accounting, .. }
        | DurableRemoveReplicaPilotTerminal::Rejected { accounting, .. } => *accounting,
    }
}

fn terminal_commit_authority_binding(
    operation: &DurableOperationStatus,
    authority: &RemoveReplicaTerminalCommitAuthority,
) -> Result<String, String> {
    let encoded = serde_json::to_vec(&(
        &operation.operation_id,
        operation.remove_mode,
        operation.old_primary_id,
        operation.target_replica_id,
        &operation.target_instance_id,
        &operation.target_pod_name,
        &operation.target_pod_uid,
        &operation.target_snapshot,
        authority.attempt,
        &authority.attempt_id,
        &authority.action_id,
        &authority.input_signature,
        &authority.primary_agent_generation,
        &authority.configuration_signature,
    ))
    .map_err(|error| format!("encode remove terminal commit authority: {error}"))?;
    let hash = encoded.iter().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    });
    Ok(format!("{hash:016x}"))
}

fn encode_terminal(terminal: DurableRemoveReplicaPilotTerminal) -> TerminalOutcome {
    let succeeded = matches!(
        terminal,
        DurableRemoveReplicaPilotTerminal::Completed { .. }
            | DurableRemoveReplicaPilotTerminal::Compensated { .. }
    );
    match serde_json::to_vec(&terminal) {
        Ok(encoded) if encoded.len() <= REMOVE_REPLICA_PILOT_MAX_TERMINAL_BYTES as usize => {
            if succeeded {
                TerminalOutcome::succeeded(ExactBytes::new(encoded))
            } else {
                TerminalOutcome::failed(ExactBytes::new(encoded))
            }
        }
        _ => TerminalOutcome::failed(ExactBytes::new(
            br#"{"status":"rejected","message":"remove terminal payload exceeded its bound","accounting":{"externalEffectCount":0,"passiveObservationCount":0}}"#,
        )),
    }
}

fn rejected_terminal(
    message: String,
    accounting: RemoveReplicaActivityAccounting,
) -> TerminalOutcome {
    encode_terminal(DurableRemoveReplicaPilotTerminal::Rejected {
        message: bounded_terminal_message(&message),
        accounting,
    })
}

fn bounded_terminal_message(message: &str) -> String {
    message.chars().take(MAX_TERMINAL_MESSAGE_BYTES).collect()
}

#[derive(Debug)]
pub enum RemoveReplicaAdapterDecision {
    Advance(Box<DurableRemoveReplicaState>),
    AwaitEvidence,
    External(Box<Decision>),
}

pub fn evaluate_adapter_step(
    operation: &DurableOperationStatus,
    observations: &OperationObservations,
    pod_identities: &OperationPodIdentities,
    target_pod_role_label: Option<&str>,
    now: i64,
) -> Result<RemoveReplicaAdapterDecision, String> {
    Ok(
        match decide_remove_replica(
            operation,
            observations,
            pod_identities,
            target_pod_role_label,
            now,
        )? {
            Decision::Persist(operation) | Decision::RecordCommitEvidence(operation) => {
                RemoveReplicaAdapterDecision::Advance(Box::new(
                    DurableRemoveReplicaState::from_operation(&operation),
                ))
            }
            Decision::Wait => RemoveReplicaAdapterDecision::AwaitEvidence,
            external @ (Decision::Execute { .. }
            | Decision::PatchPodRoleExactUid { .. }
            | Decision::DeletePod { .. }) => {
                RemoveReplicaAdapterDecision::External(Box::new(external))
            }
            Decision::PatchPodRole { .. } => {
                return Err("remove workflow requires exact-UID role mutation".to_string());
            }
            other => {
                return Err(format!(
                    "deterministic remove decision reached activity adapter: {other:?}"
                ));
            }
        },
    )
}

pub struct RemoveReplicaPreparedActivityResolver<'a> {
    initial: &'a DurableOperationStatus,
    observations: &'a OperationObservations,
    pod_identities: &'a OperationPodIdentities,
    target_pod_role_label: Option<&'a str>,
    addressed_instances: &'a BTreeMap<i64, ReplicaInstanceId>,
    now: i64,
}

impl<'a> RemoveReplicaPreparedActivityResolver<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        initial: &'a DurableOperationStatus,
        observations: &'a OperationObservations,
        pod_identities: &'a OperationPodIdentities,
        target_pod_role_label: Option<&'a str>,
        addressed_instances: &'a BTreeMap<i64, ReplicaInstanceId>,
        now: i64,
    ) -> Self {
        Self {
            initial,
            observations,
            pod_identities,
            target_pod_role_label,
            addressed_instances,
            now,
        }
    }

    fn prepare(
        &self,
        logical: &DurableRemoveReplicaActivityInput,
    ) -> Result<DurableRemoveReplicaActivityInput, PreparedActivityError> {
        if logical.version != REMOVE_REPLICA_PILOT_VERSION
            || logical.kind != RemoveReplicaActivityKind::PassiveObservation
        {
            return Err(PreparedActivityError::Validation);
        }
        let operation = logical
            .state
            .apply_to(self.initial)
            .map_err(|_| PreparedActivityError::Validation)?;
        let decision = evaluate_adapter_step(
            &operation,
            self.observations,
            self.pod_identities,
            self.target_pod_role_label,
            self.now,
        )
        .map_err(|_| PreparedActivityError::Derivation)?;
        let mut prepared = logical.clone();
        match decision {
            RemoveReplicaAdapterDecision::Advance(_) => {}
            RemoveReplicaAdapterDecision::AwaitEvidence => {
                if operation.pending_action.is_some() {
                    return Err(PreparedActivityError::Derivation);
                }
            }
            RemoveReplicaAdapterDecision::External(decision) => match *decision {
                Decision::Execute {
                    target_id,
                    action_id,
                    action,
                } => {
                    let pending = operation
                        .pending_action
                        .as_ref()
                        .ok_or(PreparedActivityError::Validation)?;
                    if pending.action_id != action_id
                        || pending.target_id != target_id
                        || !validate_remove_replica_action_kind(pending.kind, &action)
                        || action.signature()
                            != (DurableReplicaAction::RemoveReplicaIntent {
                                intent: Box::new(
                                    core_intent(&operation)
                                        .map_err(|_| PreparedActivityError::Validation)?,
                                ),
                            })
                            .signature()
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
                    if !validate_remove_replica_dispatch_authority(
                        &operation,
                        &observed.status,
                        addressed,
                        &action,
                    ) {
                        return Err(PreparedActivityError::Derivation);
                    }
                    let (planned, command) = prepare_lifecycle_replica_effect_command(
                        pending,
                        &observed.status,
                        addressed,
                        &action,
                    )
                    .map_err(preparation_error)?;
                    let mut operation = operation;
                    operation.pending_action = Some(planned);
                    prepared.state = DurableRemoveReplicaState::from_operation(&operation);
                    prepared.kind = RemoveReplicaActivityKind::PreparedReplica { command };
                }
                Decision::PatchPodRoleExactUid {
                    target_id,
                    expected_uid,
                    role,
                } => {
                    let command = prepare_remove_label_effect_command(
                        &operation,
                        target_id,
                        &expected_uid,
                        &role,
                        self.pod_identities,
                    )
                    .map_err(|_| PreparedActivityError::Derivation)?;
                    prepared.kind = RemoveReplicaActivityKind::PreparedLabel { command };
                }
                Decision::DeletePod {
                    pod_name,
                    expected_uid,
                } => {
                    let command = prepare_remove_delete_effect_command(
                        &operation,
                        &pod_name,
                        &expected_uid,
                        self.pod_identities,
                    )
                    .map_err(|_| PreparedActivityError::Derivation)?;
                    prepared.kind = RemoveReplicaActivityKind::PreparedDelete { command };
                }
                _ => return Err(PreparedActivityError::Validation),
            },
        }
        Ok(prepared)
    }

    fn validate_recorded(
        &self,
        logical: &DurableRemoveReplicaActivityInput,
        recorded: &DurableRemoveReplicaActivityInput,
    ) -> Result<(), PreparedActivityError> {
        if logical.version != REMOVE_REPLICA_PILOT_VERSION
            || recorded.version != REMOVE_REPLICA_PILOT_VERSION
            || logical.kind != RemoveReplicaActivityKind::PassiveObservation
        {
            return Err(PreparedActivityError::Validation);
        }
        let mut predecessor = recorded.state.clone();
        if let Some(pending) = predecessor.pending_action.as_mut() {
            pending.dispatch_agent_generation = None;
            pending.dispatch_agent_control_version = None;
            pending.dispatch_observed_runtime_epoch = None;
            pending.dispatch_action_payload.clear();
        }
        if recorded.state != logical.state && predecessor != logical.state {
            return Err(PreparedActivityError::Validation);
        }
        let operation = recorded
            .state
            .apply_to(self.initial)
            .map_err(|_| PreparedActivityError::Validation)?;
        if recorded.kind == RemoveReplicaActivityKind::PassiveObservation
            && recorded.state != logical.state
        {
            return Err(PreparedActivityError::Validation);
        }
        validate_prepared_activity(&operation, &recorded.kind)
            .map_err(|_| PreparedActivityError::Validation)
    }
}

impl PreparedActivityResolver for RemoveReplicaPreparedActivityResolver<'_> {
    fn resolve(
        &self,
        logical: &ActivitySpec,
        recorded: Option<&ActivitySpec>,
    ) -> Result<ActivitySpec, PreparedActivityError> {
        let logical_input = decode_activity_input::<DurableRemoveReplicaActivity>(logical.input())
            .map_err(|_| PreparedActivityError::Encoding)?;
        if let Some(recorded) = recorded {
            if recorded.name() != logical.name()
                || recorded.max_result_bytes() != logical.max_result_bytes()
            {
                return Ok(logical.clone());
            }
            let Ok(recorded_input) =
                decode_activity_input::<DurableRemoveReplicaActivity>(recorded.input())
            else {
                return Ok(logical.clone());
            };
            if self
                .validate_recorded(&logical_input, &recorded_input)
                .is_err()
            {
                return Ok(logical.clone());
            }
            return Ok(recorded.clone());
        }
        let prepared = self.prepare(&logical_input)?;
        activity_spec(&prepared).map_err(|_| PreparedActivityError::Encoding)
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
    kind: &RemoveReplicaActivityKind,
) -> Result<(), String> {
    match kind {
        RemoveReplicaActivityKind::PassiveObservation => Ok(()),
        RemoveReplicaActivityKind::PreparedReplica { command } => {
            let pending = operation
                .pending_action
                .as_ref()
                .ok_or_else(|| "prepared remove command has no pending action".to_string())?;
            let expected = ReplicaEffectCommand::from_lifecycle_pending(pending)?;
            let action = kuberic_core::grpc::convert::decode_correlated_action_payload(
                &command.action_payload,
            )
            .map_err(|error| format!("decode prepared remove command: {error}"))?;
            let expected_action = DurableReplicaAction::RemoveReplicaIntent {
                intent: Box::new(core_intent(operation)?),
            };
            if command != &expected
                || !validate_remove_replica_action_kind(pending.kind, &action)
                || action.signature() != expected_action.signature()
                || command.action_signature != expected_action.signature()
            {
                return Err("prepared remove command changed coarse action identity".to_string());
            }
            Ok(())
        }
        RemoveReplicaActivityKind::PreparedLabel { command } => {
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
        RemoveReplicaActivityKind::PreparedDelete { command } => {
            if operation.target_replica_id != Some(command.target_id)
                || operation.target_pod_name.as_deref() != Some(command.pod_name.as_str())
                || operation.target_pod_uid.as_deref() != Some(command.expected_uid.as_str())
                || !command.has_valid_identity_signature()
            {
                return Err("prepared remove delete changed exact target identity".to_string());
            }
            Ok(())
        }
    }
}

pub struct RemoveReplicaPermitGuard {
    inner: DurablePermitGuard,
}

impl RemoveReplicaPermitGuard {
    pub fn new(permit: DispatchPermit) -> Self {
        Self {
            inner: DurablePermitGuard::new(permit),
        }
    }

    pub fn consume_for(
        &mut self,
        operation: &DurableOperationStatus,
        prepared: &RemoveReplicaActivityKind,
        expected_activity: &LogicalActivityId,
        attempt_id: kuberic_durable_execution::AttemptId,
    ) -> Result<DispatchPermit, String> {
        let expected = activity_spec(&DurableRemoveReplicaActivityInput {
            version: REMOVE_REPLICA_PILOT_VERSION,
            state: DurableRemoveReplicaState::from_operation(operation),
            kind: prepared.clone(),
        })?;
        self.inner
            .consume(&expected, expected_activity, attempt_id, "remove-replica")
    }

    pub fn activity(&self) -> Option<&LogicalActivityId> {
        self.inner.activity()
    }
}

fn activity_spec(input: &DurableRemoveReplicaActivityInput) -> Result<ActivitySpec, String> {
    let encoded = encode_activity_input::<DurableRemoveReplicaActivity>(input)
        .map_err(|error| format!("serialize durable remove activity input: {error}"))?;
    Ok(ActivitySpec::new(
        ActivityName::new(
            REMOVE_REPLICA_ACTIVITY_NAME,
            REMOVE_REPLICA_ACTIVITY_VERSION,
        )
        .map_err(|error| format!("construct durable remove activity name: {error}"))?,
        encoded,
        REMOVE_REPLICA_PILOT_MAX_ACTIVITY_RESULT_BYTES as u64,
    ))
}

fn validate_immutable_authority(
    initial: &DurableOperationStatus,
    operation: &DurableOperationStatus,
) -> Result<(), String> {
    let mut normalized = operation.clone();
    normalized.phase = initial.phase;
    normalized.committed_snapshot = initial.committed_snapshot.clone();
    normalized.phase_deadline_unix_seconds = initial.phase_deadline_unix_seconds;
    normalized.pending_action = initial.pending_action.clone();
    normalized.last_error = initial.last_error.clone();
    normalized.remove_intent = initial.remove_intent.clone();
    normalized.remove_commit_evidence = initial.remove_commit_evidence.clone();
    normalized.remove_cleanup = initial.remove_cleanup.clone();
    normalized.removal_disposition = initial.removal_disposition.clone();
    if &normalized != initial {
        return Err("durable remove activity changed immutable operation authority".to_string());
    }
    Ok(())
}

fn validate_remove_transition(
    current: &DurableOperationStatus,
    next: &DurableOperationStatus,
) -> Result<(), String> {
    use DurableOperationPhase as Phase;
    validate_immutable_authority(current, next)?;
    if current.remove_commit_evidence.is_some()
        && (next.remove_commit_evidence != current.remove_commit_evidence
            || next.committed_snapshot != current.committed_snapshot)
    {
        return Err("durable remove transition changed committed evidence".to_string());
    }
    if current.remove_commit_evidence.is_none()
        && next.remove_commit_evidence.is_some()
        && !matches!(
            (current.phase, next.phase),
            (Phase::RemoveAwaitCoordination, Phase::RemoveRecordCommit)
        )
    {
        return Err("durable remove transition invented commit evidence".to_string());
    }
    if current.remove_commit_evidence.is_none()
        && let Some(evidence) = &next.remove_commit_evidence
    {
        let intent = next
            .remove_intent
            .as_ref()
            .ok_or_else(|| "durable remove commit evidence has no frozen intent".to_string())?;
        if evidence.attempt_id != intent.attempt_id
            || evidence.action_id != intent.action_id
            || kuberic_core::types::AgentGeneration::parse(
                evidence.primary_agent_generation.clone(),
            )
            .is_err()
            || evidence.configuration_signature
                != core_intent(next)?.reduced_current_configuration.signature()
        {
            return Err(
                "durable remove commit transition changed frozen commit authority".to_string(),
            );
        }
    }
    if next.remove_cleanup.is_some() && next.remove_commit_evidence.is_none() {
        return Err("durable remove cleanup preceded commit evidence".to_string());
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
        return Err(
            "durable remove transition regressed current-install dispatch evidence".to_string(),
        );
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
            "invalid durable remove phase transition {:?} -> {:?}",
            current.phase, next.phase
        ));
    }
    if next.remove_commit_evidence.is_some()
        && next.committed_snapshot.as_ref() != Some(&next.target_snapshot)
    {
        return Err("durable remove commit evidence lacks exact reduced snapshot".to_string());
    }
    Ok(())
}

fn validate_no_admission_transition(
    current: &DurableOperationStatus,
    next: &DurableOperationStatus,
    action_id: &str,
) -> Result<(), String> {
    if current.phase != next.phase {
        return Err("proven-no-admission changed remove phase".to_string());
    }
    let Some(current_pending) = current.pending_action.as_ref() else {
        return Err("proven-no-admission has no pending remove action".to_string());
    };
    let Some(next_pending) = next.pending_action.as_ref() else {
        return Err("proven-no-admission removed the pending remove action".to_string());
    };
    if current_pending.action_id != action_id || next_pending.action_id != action_id {
        return Err("proven-no-admission changed remove action identity".to_string());
    }
    let mut normalized_current = current.clone();
    let mut normalized_next = next.clone();
    normalized_current.last_error = None;
    normalized_next.last_error = None;
    for pending in [
        normalized_current.pending_action.as_mut().unwrap(),
        normalized_next.pending_action.as_mut().unwrap(),
    ] {
        pending.last_error = None;
        pending.dispatch_agent_generation = None;
        pending.dispatch_agent_control_version = None;
        pending.dispatch_observed_runtime_epoch = None;
        pending.dispatch_action_payload.clear();
    }
    if normalized_current != normalized_next {
        return Err("proven-no-admission changed non-dispatch remove state".to_string());
    }
    Ok(())
}

pub fn new_pilot_execution(
    set_uid: &str,
    previous_snapshot: StablePartitionSnapshotStatus,
    target: RemoveReplicaTarget,
    mode: DurableRemoveMode,
    minimum_replicas: usize,
    now: i64,
) -> Result<DurableRemoveReplicaPilotExecution, String> {
    if set_uid.is_empty() {
        return Err("durable remove pilot requires the KubericSet UID".to_string());
    }
    let execution_id = ExecutionId::from_bytes(random());
    let execution_hex = encode_execution_id(execution_id);
    let operation_authority = format!("{set_uid}:durable-remove:{execution_hex}");
    let initial_operation = start_remove_replica(
        &operation_authority,
        previous_snapshot,
        target,
        mode,
        minimum_replicas,
        now,
    )?;
    validate_remove_replica_admission(&initial_operation)?;
    let reference = DurableRemoveReplicaPilotExecution {
        version: REMOVE_REPLICA_PILOT_VERSION,
        execution_id: execution_hex,
        checkpoint_name: KubernetesCheckpointStore::object_name(execution_id),
        initial_operation_json: serde_json::to_string(&initial_operation)
            .map_err(|error| format!("serialize initial durable remove operation: {error}"))?,
    };
    execution_spec(&reference)?;
    Ok(reference)
}

pub fn initial_operation(
    reference: &DurableRemoveReplicaPilotExecution,
) -> Result<DurableOperationStatus, String> {
    serde_json::from_str(&reference.initial_operation_json)
        .map_err(|error| format!("decode initial durable remove operation: {error}"))
}

pub fn execution_id(reference: &DurableRemoveReplicaPilotExecution) -> Result<ExecutionId, String> {
    if reference.version != REMOVE_REPLICA_PILOT_VERSION {
        return Err(format!(
            "unsupported durable remove pilot version {}",
            reference.version
        ));
    }
    let execution_id = ExecutionId::from_bytes(decode_execution_id(&reference.execution_id)?);
    let expected_name = KubernetesCheckpointStore::object_name(execution_id);
    if reference.checkpoint_name != expected_name {
        return Err(format!(
            "durable remove checkpoint name mismatch: expected {expected_name}, found {}",
            reference.checkpoint_name
        ));
    }
    Ok(execution_id)
}

pub fn execution_spec(
    reference: &DurableRemoveReplicaPilotExecution,
) -> Result<ExecutionSpec, String> {
    let execution_id = execution_id(reference)?;
    let initial_operation = initial_operation(reference)?;
    validate_remove_replica_admission(&initial_operation)?;
    let input = DurableRemoveReplicaPilotInput {
        version: reference.version,
        execution_id: reference.execution_id.clone(),
        initial_operation,
    };
    let input = serde_json::to_vec(&input)
        .map_err(|error| format!("serialize durable remove pilot input: {error}"))?;
    if input.len() > REMOVE_REPLICA_PILOT_MAX_WORKFLOW_INPUT_BYTES {
        return Err(format!(
            "durable remove pilot input is {} bytes; maximum is {}",
            input.len(),
            REMOVE_REPLICA_PILOT_MAX_WORKFLOW_INPUT_BYTES
        ));
    }
    Ok(ExecutionSpec::new(
        execution_id,
        ExactBytes::new(input),
        REMOVE_REPLICA_PILOT_MAX_TERMINAL_BYTES,
    ))
}

pub fn checkpoint_limits() -> CheckpointLimits {
    CheckpointLimits::new(
        REMOVE_REPLICA_PILOT_MAX_ACTIVITY_RECORDS,
        REMOVE_REPLICA_PILOT_MAX_ENCODED_CHECKPOINT_BYTES,
    )
    .expect("durable remove pilot limits are nonzero")
}

pub fn checkpoint_store_options(
    namespace: &str,
    set_name: &str,
    set_uid: &str,
) -> Result<KubernetesCheckpointStoreOptions, String> {
    if namespace.is_empty() || set_name.is_empty() || set_uid.is_empty() {
        return Err(
            "durable remove checkpoint owner requires namespace, name, and UID".to_string(),
        );
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

pub fn validate_remove_replica_admission(operation: &DurableOperationStatus) -> Result<(), String> {
    validate_remove_replica_operation(operation)?;
    if operation.phase != DurableOperationPhase::RemoveFreezeIntent
        || operation.committed_snapshot.is_some()
        || operation.pending_action.is_some()
        || operation.remove_intent.is_some()
        || operation.remove_commit_evidence.is_some()
        || operation.remove_cleanup.is_some()
        || operation.removal_disposition.is_some()
    {
        return Err("durable remove admission requires a fresh accepted operation".to_string());
    }
    if operation.previous_snapshot.members.len() > REMOVE_REPLICA_PILOT_MAX_REPLICAS {
        return Err(format!(
            "durable remove pilot supports at most {REMOVE_REPLICA_PILOT_MAX_REPLICAS} replicas; found {}",
            operation.previous_snapshot.members.len()
        ));
    }
    let minimum = usize::try_from(operation.minimum_committed_replicas.unwrap_or(0))
        .map_err(|_| "remove minimum replica count is not representable".to_string())?;
    if minimum == 0
        || operation.target_snapshot.members.len() < minimum
        || operation.target_snapshot.members.len()
            < usize::try_from(operation.previous_snapshot.write_quorum)
                .map_err(|_| "remove write quorum is not representable".to_string())?
    {
        return Err("durable remove admission detected minimum or quorum drift".to_string());
    }
    if operation.phase_deadline_unix_seconds <= 0 {
        return Err("durable remove admission requires a positive overall deadline".to_string());
    }
    if PROJECTED_MAX_ACTIVITY_RECORDS > REMOVE_REPLICA_PILOT_MAX_ACTIVITY_RECORDS {
        return Err(format!(
            "durable remove requires {PROJECTED_MAX_ACTIVITY_RECORDS} projected activities; \
             maximum is {REMOVE_REPLICA_PILOT_MAX_ACTIVITY_RECORDS}"
        ));
    }
    if PROJECTED_MAX_ACTIVITY_RECORDS + PROJECTED_MAX_DETERMINISTIC_TRANSITIONS
        > REMOVE_REPLICA_PILOT_MAX_TRANSITION_FUEL
    {
        return Err(format!(
            "durable remove requires {} projected transitions; maximum is {}",
            PROJECTED_MAX_ACTIVITY_RECORDS + PROJECTED_MAX_DETERMINISTIC_TRANSITIONS,
            REMOVE_REPLICA_PILOT_MAX_TRANSITION_FUEL
        ));
    }
    validate_variant_bounds(operation)?;
    if maximum_projected_checkpoint_bytes()? > REMOVE_REPLICA_PILOT_MAX_ENCODED_CHECKPOINT_BYTES {
        return Err("durable remove projected checkpoint exceeds its admitted ceiling".to_string());
    }
    Ok(())
}

fn validate_variant_bounds(operation: &DurableOperationStatus) -> Result<(), String> {
    let state = DurableRemoveReplicaState::from_operation(operation);
    let maximum = "x".repeat(MAX_TERMINAL_MESSAGE_BYTES);
    let attempt_id = format!("{}:attempt-3", operation.operation_id);
    let action_id = format!("{attempt_id}:RemoveReplicaIntent");
    let configuration_signature = serde_json::to_string(&operation.target_snapshot)
        .map_err(|error| format!("serialize remove terminal configuration: {error}"))?;
    let cleanup = RemoveReplicaCleanupStatus {
        connection_absent: true,
        target_retirement: Some(TargetRetirementObservationStatus::Unavailable),
        target_labels_fenced: true,
        target_pod_deleted: true,
    };
    let commit_evidence = RemoveReplicaCommitEvidenceStatus {
        attempt_id: attempt_id.clone(),
        action_id: action_id.clone(),
        primary_agent_generation: "f".repeat(32),
        configuration_signature: configuration_signature.clone(),
        observed_unix_seconds: i64::MAX,
    };
    let authority = RemoveReplicaTerminalCommitAuthority {
        attempt: u32::MAX,
        attempt_id,
        action_id,
        input_signature: maximum.clone(),
        primary_agent_generation: "f".repeat(32),
        configuration_signature: configuration_signature.clone(),
        binding_signature: "f".repeat(16),
    };
    let accounting = RemoveReplicaActivityAccounting {
        external_effect_count: u64::MAX,
        passive_observation_count: u64::MAX,
    };
    for result in [
        DurableRemoveReplicaStepResult::Advance {
            operation: state.clone(),
            completion: RemoveReplicaActivityCompletion::PassiveObservation,
        },
        DurableRemoveReplicaStepResult::ProvenNoAdmission {
            operation: state,
            action_id: "x".repeat(512),
            redelivery: 1,
        },
    ] {
        encode_step_result(&result)?;
    }
    for terminal in [
        DurableRemoveReplicaPilotTerminal::Completed {
            authority,
            commit_evidence: commit_evidence.clone(),
            cleanup: cleanup.clone(),
            accounting,
        },
        DurableRemoveReplicaPilotTerminal::Compensated {
            message: maximum.clone(),
            accounting,
        },
        DurableRemoveReplicaPilotTerminal::UnsafeAmbiguity {
            committed: true,
            commit_evidence: Some(commit_evidence),
            cleanup: Some(cleanup),
            disposition: RemoveReplicaDispositionStatus::InvalidRemovalState {
                attempt: u32::MAX,
                last_observed_phase: None,
                previous_configuration_signature: serde_json::to_string(
                    &*operation.previous_snapshot,
                )
                .map_err(|error| format!("serialize previous remove configuration: {error}"))?,
                reduced_configuration_signature: configuration_signature,
                reason: maximum.clone(),
            },
            message: maximum.clone(),
            accounting,
        },
        DurableRemoveReplicaPilotTerminal::Rejected {
            message: maximum,
            accounting,
        },
    ] {
        let encoded = serde_json::to_vec(&terminal)
            .map_err(|error| format!("serialize durable remove terminal variant: {error}"))?;
        if encoded.len() > REMOVE_REPLICA_PILOT_MAX_TERMINAL_BYTES as usize {
            return Err(format!(
                "durable remove terminal variant is {} bytes; maximum is {}",
                encoded.len(),
                REMOVE_REPLICA_PILOT_MAX_TERMINAL_BYTES
            ));
        }
    }
    Ok(())
}

pub fn maximum_active_checkpoint() -> Result<CheckpointEnvelope, String> {
    let payload = maximum_active_payload(REMOVE_REPLICA_PILOT_MAX_ENCODED_CHECKPOINT_BYTES)?;
    CheckpointEnvelope::encode_with_limits(&payload, checkpoint_limits())
        .map_err(|error| format!("project maximum durable remove checkpoint: {error}"))
}

fn maximum_active_payload(
    admitted_max_encoded_checkpoint_bytes: usize,
) -> Result<CheckpointPayload, String> {
    let execution_id = ExecutionId::from_bytes([u8::MAX; 16]);
    let execution = ExecutionSpec::new(
        execution_id,
        ExactBytes::new(vec![u8::MAX; REMOVE_REPLICA_PILOT_MAX_WORKFLOW_INPUT_BYTES]),
        REMOVE_REPLICA_PILOT_MAX_TERMINAL_BYTES,
    );
    let contract = ExecutionContract::new(
        execution,
        u64::try_from(admitted_max_encoded_checkpoint_bytes)
            .map_err(|_| "remove checkpoint limit does not fit u64".to_string())?,
    );
    let name = ActivityName::new(
        REMOVE_REPLICA_ACTIVITY_NAME,
        REMOVE_REPLICA_ACTIVITY_VERSION,
    )
    .map_err(|error| format!("construct durable remove activity name: {error}"))?;
    let activities = (0..REMOVE_REPLICA_PILOT_MAX_ACTIVITY_RECORDS)
        .map(|sequence| {
            ActivityRecord::completed(
                ActivitySequence::new(
                    u64::try_from(sequence).expect("remove activity count fits u64"),
                ),
                ActivitySpec::new(
                    name.clone(),
                    ExactBytes::new(vec![u8::MAX; REMOVE_REPLICA_PILOT_MAX_ACTIVITY_INPUT_BYTES]),
                    REMOVE_REPLICA_PILOT_MAX_ACTIVITY_RESULT_BYTES as u64,
                ),
                ExactBytes::new(vec![
                    u8::MAX;
                    REMOVE_REPLICA_PILOT_MAX_ACTIVITY_RESULT_BYTES
                ]),
            )
        })
        .collect();
    Ok(CheckpointPayload::active(contract, activities))
}

fn maximum_projected_checkpoint_bytes() -> Result<usize, String> {
    static PROJECTED_BYTES: OnceLock<Result<usize, String>> = OnceLock::new();
    PROJECTED_BYTES
        .get_or_init(|| {
            maximum_active_checkpoint()?
                .encoded_len()
                .map_err(|error| format!("measure maximum durable remove checkpoint: {error}"))
        })
        .clone()
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
        return Err(format!(
            "durable remove execution ID must contain 32 lowercase hexadecimal characters; found {}",
            value.len()
        ));
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
        _ => Err("durable remove execution ID must be lowercase hexadecimal".to_string()),
    }
}

// COMPLEXITY-BOUNDARY: remove-replica-pilot-workflow:end
// COMPLEXITY-BOUNDARY: remove-replica-pilot-module:end

#[cfg(test)]
mod remove_replica_pilot_tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use kuberic_core::{
        error::KubericError,
        remove_replica::{
            MAX_REMOVE_REPLICA_PRE_COMMIT_ATTEMPTS, RemoveReplicaCoordinatorPhase,
            RemoveReplicaProgress, RemoveReplicaTerminalResult, TargetRetirementObservation,
        },
        replica_lifecycle::REPLICA_LIFECYCLE_PEER_PROTOCOL_VERSION,
        types::{
            AccessStatus, AgentControlVersion, AgentGeneration, CorrelatedActionObservation,
            DurableActionObservation, DurableActionResult, DurableActionState, Epoch,
            ReplicaAgentStatus, ReplicaConfigurationMemberStatus, ReplicaConfigurationMode,
            ReplicaConfigurationStatus, ReplicaConnectionStatus, ReplicaStatusInfo, Role,
        },
    };
    use kuberic_durable_execution::{
        ActivityObservation, ActivityState, CasOutcome, CheckpointState, CheckpointStore,
        DurableHost, HostEpoch, HostOutcome, InMemoryFault, PersistenceBoundary, ReloadReason,
    };

    use crate::{
        crd::{EpochStatus, StableReplicaRoleStatus, StableReplicaSnapshotStatus},
        durable::{
            ReplicaObservation,
            effects::{
                DispatchFailureDisposition, DurableEffectBridgeOutcome, classify_dispatch_failure,
                clear_dispatch_evidence, operation_after_dispatch_error,
                remove_delete_postcondition_satisfied, remove_label_postcondition_satisfied,
                resolve_quarantined_remove_delete_effect, resolve_quarantined_remove_label_effect,
            },
        },
    };

    use super::super::pilot_store::MeasuredPilotCheckpointStore;
    use super::*;

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

    fn target(mode: DurableRemoveMode) -> RemoveReplicaTarget {
        RemoveReplicaTarget {
            replica_id: 3,
            pod_name: "set-2".to_string(),
            pod_uid: "three".to_string(),
            replicator_address: "http://three:9091".to_string(),
            agent_generation: (mode == DurableRemoveMode::ScaleDown).then(|| generation(3)),
        }
    }

    fn generation(id: i64) -> AgentGeneration {
        AgentGeneration::parse(format!("{id:032x}")).unwrap()
    }

    fn configuration(
        snapshot: &StablePartitionSnapshotStatus,
        mode: ReplicaConfigurationMode,
    ) -> ReplicaConfigurationStatus {
        let mut members = snapshot
            .members
            .iter()
            .filter(|member| member.id != snapshot.primary_id)
            .map(|member| ReplicaConfigurationMemberStatus {
                id: member.id,
                instance_id: ReplicaInstanceId::new(member.instance_id.clone()),
                role: Role::ActiveSecondary,
            })
            .collect::<Vec<_>>();
        members.sort_by_key(|member| member.id);
        ReplicaConfigurationStatus {
            mode,
            members,
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
        let mut observations = OperationObservations::new();
        observations.insert(
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
        );
        observations.insert(2, observation(2, "two", Role::ActiveSecondary, None));
        observations.insert(3, observation(3, "three", Role::ActiveSecondary, None));
        observations
    }

    fn pod_identities() -> OperationPodIdentities {
        BTreeMap::from([
            (1, "one".to_string()),
            (2, "two".to_string()),
            (3, "three".to_string()),
        ])
    }

    fn reference(mode: DurableRemoveMode) -> DurableRemoveReplicaPilotExecution {
        new_pilot_execution("set-uid", snapshot(), target(mode), mode, 2, 10).unwrap()
    }

    fn initial(mode: DurableRemoveMode) -> DurableOperationStatus {
        start_remove_replica("set-fixed", snapshot(), target(mode), mode, 2, 10).unwrap()
    }

    fn freeze_and_dispatch(
        mode: DurableRemoveMode,
    ) -> (DurableOperationStatus, OperationObservations) {
        let initial = initial(mode);
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
            panic!("expected persisted coarse remove dispatch");
        };
        (dispatched, observations)
    }

    fn set_remove_action_progress(
        operation: &DurableOperationStatus,
        observations: &mut OperationObservations,
        progress: RemoveReplicaProgress,
        state: DurableActionState,
        result: Option<RemoveReplicaTerminalResult>,
    ) {
        let intent = operation.remove_intent.as_ref().unwrap();
        let primary = observations.get_mut(&operation.old_primary_id).unwrap();
        primary.status.agent.generation =
            AgentGeneration::parse(intent.primary_agent_generation.clone()).unwrap();
        primary.status.agent.control_version =
            AgentControlVersion::new(intent.primary_agent_control_version);
        primary.status.agent.current_action = Some(CorrelatedActionObservation {
            generation: primary.status.agent.generation.clone(),
            control_version: primary.status.agent.control_version,
            action: DurableActionObservation {
                action_id: intent.action_id.clone(),
                signature: intent.input_signature.clone(),
                state,
                error_class: None,
                error: None,
                result: result.map(DurableActionResult::RemoveReplica),
                add_replica_progress: None,
                remove_replica_progress: Some(progress),
            },
        });
    }

    fn committed_operation() -> DurableOperationStatus {
        let (operation, mut observations) = freeze_and_dispatch(DurableRemoveMode::ScaleDown);
        let intent = operation.remove_intent.as_ref().unwrap().clone();
        let progress = RemoveReplicaProgress {
            phase: RemoveReplicaCoordinatorPhase::Attesting,
            attempt_id: intent.attempt_id.clone(),
            commit_observed: true,
            commit_observed_unix_seconds: Some(123),
            connection_absent: true,
            target_retirement: TargetRetirementObservation::Completed,
            retirement_expiry_unix_seconds: Some(183),
            compensation_expiry_unix_seconds: None,
            error: None,
            current_install_dispatched: true,
        };
        let primary = observations.get_mut(&1).unwrap();
        primary.status.configuration = Some(configuration(
            &operation.target_snapshot,
            ReplicaConfigurationMode::Current,
        ));
        primary.status.active_replica_connections.clear();
        set_remove_action_progress(
            &operation,
            &mut observations,
            progress,
            DurableActionState::Completed,
            Some(RemoveReplicaTerminalResult::CommittedClean),
        );
        let Decision::Persist(observed) = decide_remove_replica(
            &operation,
            &observations,
            &pod_identities(),
            Some("secondary"),
            200,
        )
        .unwrap() else {
            panic!("expected progress persistence");
        };
        let Decision::RecordCommitEvidence(committed) = decide_remove_replica(
            &observed,
            &observations,
            &pod_identities(),
            Some("secondary"),
            200,
        )
        .unwrap() else {
            panic!("expected commit evidence");
        };
        committed
    }

    struct CompletedStages {
        committed: DurableOperationStatus,
        awaiting_cleanup: DurableOperationStatus,
        deleting: DurableOperationStatus,
        publishing: DurableOperationStatus,
        finalizing: DurableOperationStatus,
        completed: DurableOperationStatus,
    }

    fn completed_stages() -> CompletedStages {
        let committed = committed_operation();
        let Decision::Persist(awaiting_cleanup) = decide_remove_replica(
            &committed,
            &OperationObservations::new(),
            &OperationPodIdentities::new(),
            None,
            200,
        )
        .unwrap() else {
            panic!("expected cleanup transition");
        };
        let mut cleanup_observations = observations(&awaiting_cleanup);
        let primary = cleanup_observations.get_mut(&1).unwrap();
        primary.status.configuration = Some(configuration(
            &awaiting_cleanup.target_snapshot,
            ReplicaConfigurationMode::Current,
        ));
        primary.status.active_replica_connections.clear();
        let target = cleanup_observations.get_mut(&3).unwrap();
        target.status.role = Role::None;
        target.status.healthy = false;
        let Decision::Persist(deleting) = decide_remove_replica(
            &awaiting_cleanup,
            &cleanup_observations,
            &pod_identities(),
            Some("retired"),
            200,
        )
        .unwrap() else {
            panic!("expected delete transition");
        };
        let mut deleted_pods = pod_identities();
        deleted_pods.remove(&3);
        let Decision::Persist(publishing) =
            decide_remove_replica(&deleting, &cleanup_observations, &deleted_pods, None, 200)
                .unwrap()
        else {
            panic!("expected publication transition");
        };
        let Decision::CommitSnapshot {
            operation: finalizing,
            ..
        } = decide_remove_replica(
            &publishing,
            &OperationObservations::new(),
            &OperationPodIdentities::new(),
            None,
            200,
        )
        .unwrap()
        else {
            panic!("expected terminal publication decision");
        };
        let Decision::Complete {
            operation: completed,
            ..
        } = decide_remove_replica(
            &finalizing,
            &OperationObservations::new(),
            &OperationPodIdentities::new(),
            None,
            200,
        )
        .unwrap()
        else {
            panic!("expected completed removal");
        };
        CompletedStages {
            committed,
            awaiting_cleanup,
            deleting,
            publishing,
            finalizing,
            completed,
        }
    }

    fn compensated_operation() -> DurableOperationStatus {
        let (operation, mut observations) = freeze_and_dispatch(DurableRemoveMode::ScaleDown);
        let intent = operation.remove_intent.as_ref().unwrap().clone();
        set_remove_action_progress(
            &operation,
            &mut observations,
            RemoveReplicaProgress {
                phase: RemoveReplicaCoordinatorPhase::Compensating,
                attempt_id: intent.attempt_id,
                commit_observed: false,
                commit_observed_unix_seconds: None,
                connection_absent: false,
                target_retirement: TargetRetirementObservation::NotAttempted,
                retirement_expiry_unix_seconds: None,
                compensation_expiry_unix_seconds: Some(50),
                error: Some("pre-commit quorum loss".to_string()),
                current_install_dispatched: false,
            },
            DurableActionState::Completed,
            Some(RemoveReplicaTerminalResult::Compensated),
        );
        let Decision::Persist(observed) = decide_remove_replica(
            &operation,
            &observations,
            &pod_identities(),
            Some("secondary"),
            20,
        )
        .unwrap() else {
            panic!("expected compensation observation");
        };
        let Decision::Persist(finalizing) = decide_remove_replica(
            &observed,
            &observations,
            &pod_identities(),
            Some("secondary"),
            20,
        )
        .unwrap() else {
            panic!("expected compensation finalization");
        };
        let Decision::Complete {
            operation: failed,
            compensated: true,
            ..
        } = decide_remove_replica(
            &finalizing,
            &OperationObservations::new(),
            &OperationPodIdentities::new(),
            None,
            20,
        )
        .unwrap()
        else {
            panic!("expected safe compensated terminal");
        };
        failed
    }

    fn unsafe_operation() -> DurableOperationStatus {
        let (mut operation, mut observations) = freeze_and_dispatch(DurableRemoveMode::ScaleDown);
        operation
            .remove_intent
            .as_mut()
            .unwrap()
            .current_install_dispatched = true;
        observations.get_mut(&1).unwrap().status.agent.generation =
            AgentGeneration::parse("ffffffffffffffffffffffffffffffff").unwrap();
        let Decision::Persist(poisoned) = decide_remove_replica(
            &operation,
            &observations,
            &pod_identities(),
            Some("secondary"),
            20,
        )
        .unwrap() else {
            panic!("expected unsafe ambiguity");
        };
        assert_eq!(poisoned.phase, DurableOperationPhase::Poisoned);
        poisoned
    }

    fn addressed(observations: &OperationObservations) -> BTreeMap<i64, ReplicaInstanceId> {
        observations
            .iter()
            .map(|(id, observed)| (*id, observed.status.instance_id.clone()))
            .collect()
    }

    fn resolve_kind(
        initial: &DurableOperationStatus,
        operation: &DurableOperationStatus,
        observations: &OperationObservations,
        pods: &OperationPodIdentities,
        role: Option<&str>,
    ) -> Result<RemoveReplicaActivityKind, PreparedActivityError> {
        let addressed = addressed(observations);
        let resolver = RemoveReplicaPreparedActivityResolver::new(
            initial,
            observations,
            pods,
            role,
            &addressed,
            200,
        );
        let logical = activity_spec(&DurableRemoveReplicaActivityInput {
            version: REMOVE_REPLICA_PILOT_VERSION,
            state: DurableRemoveReplicaState::from_operation(operation),
            kind: RemoveReplicaActivityKind::PassiveObservation,
        })
        .unwrap();
        resolver.resolve(&logical, None).and_then(|spec| {
            decode_remove_activity_input(spec.input())
                .map(|input| input.kind)
                .map_err(|_| PreparedActivityError::Encoding)
        })
    }

    fn measured_host(
        execution: &ExecutionSpec,
        backend: InMemoryCheckpointStore,
        epoch: u8,
    ) -> RemoveReplicaPilotHost {
        DurableHost::new(
            MeasuredPilotCheckpointStore::with_decoder(
                execution.execution_id(),
                super::super::pilot_store::DurableCheckpointStore::InMemory(backend),
                checkpoint_measurement_decoder(),
            ),
            HostEpoch::from_bytes([epoch; 16]),
            checkpoint_limits(),
        )
    }

    async fn assert_exposed_history(
        backend: &InMemoryCheckpointStore,
        execution: &ExecutionSpec,
        permit: &DispatchPermit,
        expected_specs: &[ActivitySpec],
    ) {
        let stored = backend
            .load(execution.execution_id())
            .await
            .unwrap()
            .unwrap();
        let payload = stored
            .checkpoint()
            .decode_and_validate(execution, checkpoint_limits())
            .unwrap();
        let activities = payload.active_activities().unwrap();
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

    async fn assert_completed_history(
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
        let payload = stored
            .checkpoint()
            .decode_and_validate(execution, checkpoint_limits())
            .unwrap();
        let activities = payload.active_activities().unwrap();
        assert_eq!(activities.len(), expected_specs.len());
        for (record, expected) in activities.iter().zip(expected_specs) {
            assert_eq!(record.spec(), expected);
            assert!(matches!(record.state(), ActivityState::Completed { .. }));
        }
        encoded_len
    }

    async fn restart_into_quarantine(
        backend: &InMemoryCheckpointStore,
        execution: &ExecutionSpec,
        epoch: u8,
        permit: &DispatchPermit,
    ) -> RemoveReplicaPilotHost {
        let before = backend.load(execution.execution_id()).await.unwrap();
        let mut restarted = measured_host(execution, backend.clone(), epoch);
        let HostOutcome::Quarantined {
            activity,
            attempt_id,
        } = restarted
            .turn(&DurableRemoveReplicaWorkflow, execution.clone())
            .await
        else {
            panic!("expected exposed remove activity to be quarantined after restart");
        };
        assert_eq!(activity, *permit.activity());
        assert_eq!(attempt_id, permit.attempt_id());
        assert_eq!(
            backend.load(execution.execution_id()).await.unwrap(),
            before
        );
        restarted
    }

    #[test]
    fn remove_replica_pilot_rejects_primary_nonmember_uid_incarnation_minimum_and_quorum_drift() {
        let primary = RemoveReplicaTarget {
            replica_id: 1,
            ..target(DurableRemoveMode::Force)
        };
        assert!(
            new_pilot_execution("set", snapshot(), primary, DurableRemoveMode::Force, 2, 10)
                .unwrap_err()
                .contains("primary")
        );
        let nonmember = RemoveReplicaTarget {
            replica_id: 4,
            pod_name: "set-3".to_string(),
            pod_uid: "four".to_string(),
            replicator_address: "http://four:9091".to_string(),
            agent_generation: None,
        };
        assert!(
            new_pilot_execution(
                "set",
                snapshot(),
                nonmember,
                DurableRemoveMode::Force,
                2,
                10
            )
            .unwrap_err()
            .contains("not committed")
        );
        let mut wrong_uid = target(DurableRemoveMode::ScaleDown);
        wrong_uid.pod_uid = "replacement".to_string();
        assert!(
            new_pilot_execution(
                "set",
                snapshot(),
                wrong_uid,
                DurableRemoveMode::ScaleDown,
                2,
                10
            )
            .unwrap_err()
            .contains("incarnation")
        );
        assert!(
            new_pilot_execution(
                "set",
                snapshot(),
                target(DurableRemoveMode::ScaleDown),
                DurableRemoveMode::ScaleDown,
                3,
                10
            )
            .unwrap_err()
            .contains("minReplicas")
        );
        let mut drift = initial(DurableRemoveMode::ScaleDown);
        drift.minimum_committed_replicas = Some(3);
        assert!(
            validate_remove_replica_admission(&drift)
                .unwrap_err()
                .contains("minimum or quorum drift")
        );
    }

    #[test]
    fn remove_replica_pilot_rejects_generation_control_version_runtime_epoch_protocol_and_configuration_drift()
     {
        let initial = initial(DurableRemoveMode::ScaleDown);
        let (operation, observations) = freeze_and_dispatch(DurableRemoveMode::ScaleDown);
        assert!(matches!(
            resolve_kind(
                &initial,
                &operation,
                &observations,
                &pod_identities(),
                Some("secondary")
            )
            .unwrap(),
            RemoveReplicaActivityKind::PreparedReplica { .. }
        ));
        let mut variants = Vec::new();
        let mut changed = observations.clone();
        changed.get_mut(&1).unwrap().status.agent.generation =
            AgentGeneration::parse("ffffffffffffffffffffffffffffffff").unwrap();
        variants.push(changed);
        let mut changed = observations.clone();
        changed.get_mut(&1).unwrap().status.agent.control_version = AgentControlVersion::new(12);
        variants.push(changed);
        let mut changed = observations.clone();
        changed.get_mut(&1).unwrap().status.epoch = Epoch::new(1, 8);
        variants.push(changed);
        let mut changed = observations.clone();
        changed.get_mut(&1).unwrap().status.agent.protocol_version = 0;
        variants.push(changed);
        let mut changed = observations;
        changed.get_mut(&1).unwrap().status.configuration = None;
        variants.push(changed);
        for drifted in variants {
            assert!(
                !matches!(
                    resolve_kind(
                        &initial,
                        &operation,
                        &drifted,
                        &pod_identities(),
                        Some("secondary")
                    ),
                    Ok(RemoveReplicaActivityKind::PreparedReplica { .. })
                ),
                "drifted authority must not prepare a replica command"
            );
        }
    }

    #[test]
    fn remove_replica_pilot_preserves_scale_down_authority_and_never_converts_to_force() {
        let operation = initial(DurableRemoveMode::ScaleDown);
        let mut missing_target = observations(&operation);
        missing_target.remove(&3);
        let error = decide_remove_replica(
            &operation,
            &missing_target,
            &pod_identities(),
            Some("secondary"),
            10,
        )
        .unwrap_err();
        assert!(error.contains("ScaleDown target is unavailable"));
        assert_eq!(operation.remove_mode, Some(DurableRemoveMode::ScaleDown));
    }

    #[test]
    fn remove_replica_pilot_force_retains_topology_and_primary_fences() {
        let operation = initial(DurableRemoveMode::Force);
        let mut without_target = observations(&operation);
        without_target.remove(&3);
        let Decision::Persist(frozen) =
            decide_remove_replica(&operation, &without_target, &pod_identities(), None, 10)
                .unwrap()
        else {
            panic!("Force should freeze without target authority");
        };
        assert_eq!(frozen.remove_mode, Some(DurableRemoveMode::Force));
        assert_eq!(frozen.target_snapshot, operation.target_snapshot);
        let mut stale_primary = without_target;
        stale_primary.get_mut(&1).unwrap().status.epoch = Epoch::new(9, 9);
        assert!(
            decide_remove_replica(&operation, &stale_primary, &pod_identities(), None, 10)
                .unwrap_err()
                .contains("exact writable runtime")
        );
    }

    #[test]
    fn remove_replica_pilot_replay_preserves_exact_coarse_action_identity() {
        let initial = initial(DurableRemoveMode::ScaleDown);
        let (operation, observations) = freeze_and_dispatch(DurableRemoveMode::ScaleDown);
        let pods = pod_identities();
        let addressed = addressed(&observations);
        let resolver = RemoveReplicaPreparedActivityResolver::new(
            &initial,
            &observations,
            &pods,
            Some("secondary"),
            &addressed,
            10,
        );
        let logical = activity_spec(&DurableRemoveReplicaActivityInput {
            version: REMOVE_REPLICA_PILOT_VERSION,
            state: DurableRemoveReplicaState::from_operation(&operation),
            kind: RemoveReplicaActivityKind::PassiveObservation,
        })
        .unwrap();
        let prepared = resolver.resolve(&logical, None).unwrap();
        assert_eq!(
            resolver.resolve(&logical, Some(&prepared)).unwrap(),
            prepared
        );
        let input = decode_remove_activity_input(prepared.input()).unwrap();
        let RemoveReplicaActivityKind::PreparedReplica { command } = input.kind else {
            panic!("expected exact coarse remove command");
        };
        assert_eq!(
            command.action_id,
            operation.remove_intent.as_ref().unwrap().action_id
        );
        assert_eq!(
            command.action_signature,
            operation.remove_intent.as_ref().unwrap().input_signature
        );
    }

    #[test]
    fn remove_replica_lifecycle_payload_does_not_weaken_direct_command_boundary() {
        let initial = initial(DurableRemoveMode::ScaleDown);
        let (operation, observations) = freeze_and_dispatch(DurableRemoveMode::ScaleDown);
        let prepared = resolve_kind(
            &initial,
            &operation,
            &observations,
            &pod_identities(),
            Some("secondary"),
        )
        .unwrap();
        let RemoveReplicaActivityKind::PreparedReplica { command } = prepared else {
            panic!("expected prepared lifecycle command");
        };
        let mut pending = operation.pending_action.as_ref().unwrap().clone();
        pending.dispatch_agent_generation = Some(command.expected_agent_generation.clone());
        pending.dispatch_agent_control_version = Some(command.expected_control_version);
        pending.dispatch_observed_runtime_epoch = Some(command.observed_runtime_epoch.clone());
        pending.dispatch_action_payload = command.action_payload.clone();
        assert!(ReplicaEffectCommand::from_pending(&pending).is_err());
        assert_eq!(
            ReplicaEffectCommand::from_lifecycle_pending(&pending).unwrap(),
            command
        );
    }

    #[test]
    fn remove_replica_pilot_requires_correlated_primary_connection_lifecycle_uid_and_role_evidence()
    {
        let initial = initial(DurableRemoveMode::ScaleDown);
        assert!(
            resolve_kind(
                &initial,
                &initial,
                &OperationObservations::new(),
                &pod_identities(),
                None
            )
            .is_err()
        );
        let stages = completed_stages();
        let mut pods = pod_identities();
        pods.insert(3, "replacement".to_string());
        assert!(
            resolve_kind(
                &initial,
                &stages.awaiting_cleanup,
                &OperationObservations::new(),
                &pods,
                None
            )
            .is_err()
                || !matches!(
                    resolve_kind(
                        &initial,
                        &stages.awaiting_cleanup,
                        &OperationObservations::new(),
                        &pods,
                        None
                    ),
                    Ok(RemoveReplicaActivityKind::PreparedLabel { .. })
                )
        );
        assert!(
            stages
                .completed
                .remove_cleanup
                .as_ref()
                .is_some_and(|cleanup| cleanup.connection_absent
                    && cleanup.target_retirement.is_some()
                    && cleanup.target_labels_fenced
                    && cleanup.target_pod_deleted)
        );
    }

    #[test]
    fn remove_replica_pilot_distinguishes_refresh_busy_conflict_unknown_and_proven_no_admission() {
        let (mut operation, _) = freeze_and_dispatch(DurableRemoveMode::ScaleDown);
        let pending = operation.pending_action.as_mut().unwrap();
        pending.dispatch_agent_generation = Some(generation(1).to_string());
        pending.dispatch_agent_control_version = Some(11);
        pending.dispatch_observed_runtime_epoch = Some(EpochStatus {
            data_loss_number: 1,
            configuration_number: 7,
        });
        pending.dispatch_action_payload = "frozen".to_string();

        let refreshed = operation_after_dispatch_error(
            &operation,
            &KubericError::RemoteAgentPreconditionRejected("stale".into()),
        );
        assert!(
            refreshed
                .pending_action
                .as_ref()
                .unwrap()
                .dispatch_action_payload
                .is_empty()
        );
        assert_eq!(
            classify_dispatch_failure(&KubericError::AgentBusy),
            DispatchFailureDisposition::ProvenNoAdmission
        );
        assert_eq!(
            classify_dispatch_failure(&KubericError::RemoteAgentConflict("conflict".into())),
            DispatchFailureDisposition::DefiniteFailure
        );
        assert_eq!(
            classify_dispatch_failure(&KubericError::Closed),
            DispatchFailureDisposition::Unknown
        );
        let mut cleared = operation.pending_action.clone().unwrap();
        clear_dispatch_evidence(&mut cleared);
        assert!(cleared.dispatch_action_payload.is_empty());
    }

    #[test]
    fn remove_replica_pilot_redrives_only_from_safe_configuration_within_attempt_bound() {
        let (operation, mut observations) = freeze_and_dispatch(DurableRemoveMode::ScaleDown);
        observations.get_mut(&1).unwrap().status.agent.generation =
            AgentGeneration::parse("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let mut current = operation;
        for (attempt, generation_value) in [
            (2, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            (3, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        ] {
            observations.get_mut(&1).unwrap().status.agent.generation =
                AgentGeneration::parse(generation_value).unwrap();
            let Decision::Persist(redrive) = decide_remove_replica(
                &current,
                &observations,
                &pod_identities(),
                Some("secondary"),
                20,
            )
            .unwrap() else {
                panic!("expected redrive toward attempt {attempt}");
            };
            assert_eq!(redrive.phase, DurableOperationPhase::RemoveFreezeIntent);
            let Decision::Persist(frozen) = decide_remove_replica(
                &redrive,
                &observations,
                &pod_identities(),
                Some("secondary"),
                20,
            )
            .unwrap() else {
                panic!("expected attempt {attempt} freeze");
            };
            assert_eq!(frozen.remove_intent.as_ref().unwrap().attempt, attempt);
            let Decision::Persist(dispatched) = decide_remove_replica(
                &frozen,
                &observations,
                &pod_identities(),
                Some("secondary"),
                20,
            )
            .unwrap() else {
                panic!("expected attempt {attempt} dispatch");
            };
            current = dispatched;
        }
        assert_eq!(
            current.remove_intent.as_ref().unwrap().attempt,
            MAX_REMOVE_REPLICA_PRE_COMMIT_ATTEMPTS
        );
        observations.get_mut(&1).unwrap().status.agent.generation =
            AgentGeneration::parse("cccccccccccccccccccccccccccccccc").unwrap();
        let Decision::Persist(stopped) = decide_remove_replica(
            &current,
            &observations,
            &pod_identities(),
            Some("secondary"),
            20,
        )
        .unwrap() else {
            panic!("expected bounded failure");
        };
        assert_eq!(stopped.phase, DurableOperationPhase::Poisoned);
    }

    #[test]
    fn remove_replica_pilot_persists_commit_before_cleanup_and_never_rolls_back_after_dispatch_ambiguity()
     {
        let committed = committed_operation();
        assert_eq!(committed.phase, DurableOperationPhase::RemoveRecordCommit);
        assert_eq!(
            committed.committed_snapshot.as_ref(),
            Some(&committed.target_snapshot)
        );
        assert!(committed.remove_commit_evidence.is_some());
        assert!(
            !committed
                .remove_cleanup
                .as_ref()
                .unwrap()
                .target_pod_deleted
        );
        let unsafe_operation = unsafe_operation();
        assert_eq!(unsafe_operation.phase, DurableOperationPhase::Poisoned);
        assert_ne!(
            unsafe_operation.phase,
            DurableOperationPhase::RemoveCompensateFinalize
        );
    }

    #[test]
    fn remove_replica_pilot_rejects_commit_evidence_that_changes_frozen_authority() {
        let (awaiting_commit, _) = freeze_and_dispatch(DurableRemoveMode::ScaleDown);
        let committed = committed_operation();
        validate_remove_transition(&awaiting_commit, &committed).unwrap();

        let mut changed = committed;
        changed
            .remove_commit_evidence
            .as_mut()
            .unwrap()
            .configuration_signature
            .push_str("-changed");
        assert!(
            validate_remove_transition(&awaiting_commit, &changed)
                .unwrap_err()
                .contains("commit authority")
        );
    }

    #[test]
    fn remove_replica_pilot_requires_connection_retirement_uid_label_and_uid_delete_before_publication()
     {
        let stages = completed_stages();
        assert!(stages.committed.remove_commit_evidence.is_some());
        assert_eq!(
            stages.awaiting_cleanup.phase,
            DurableOperationPhase::RemoveAwaitCleanup
        );
        let cleanup = stages.deleting.remove_cleanup.as_ref().unwrap();
        assert!(cleanup.connection_absent);
        assert_eq!(
            cleanup.target_retirement,
            Some(TargetRetirementObservationStatus::Completed)
        );
        assert!(cleanup.target_labels_fenced);
        assert!(!cleanup.target_pod_deleted);
        assert!(
            stages
                .publishing
                .remove_cleanup
                .as_ref()
                .unwrap()
                .target_pod_deleted
        );
        assert_eq!(
            stages.finalizing.phase,
            DurableOperationPhase::RemoveFinalize
        );
    }

    #[test]
    fn remove_replica_pilot_maps_success_compensation_and_unsafe_ambiguity() {
        assert!(matches!(
            terminal_from_operation(
                &completed_stages().completed,
                RemoveReplicaActivityAccounting::default()
            )
            .unwrap(),
            DurableRemoveReplicaPilotTerminal::Completed { .. }
        ));
        assert!(matches!(
            terminal_from_operation(
                &compensated_operation(),
                RemoveReplicaActivityAccounting::default()
            )
            .unwrap(),
            DurableRemoveReplicaPilotTerminal::Compensated { .. }
        ));
        assert!(matches!(
            terminal_from_operation(
                &unsafe_operation(),
                RemoveReplicaActivityAccounting::default()
            )
            .unwrap(),
            DurableRemoveReplicaPilotTerminal::UnsafeAmbiguity { .. }
        ));
    }

    #[test]
    fn completed_terminal_rejects_inexact_commit_authority_and_nonterminal_retirement() {
        let initial = initial(DurableRemoveMode::ScaleDown);
        let execution_id = ExecutionId::from_bytes([121; 16]);
        let reference = DurableRemoveReplicaPilotExecution {
            version: REMOVE_REPLICA_PILOT_VERSION,
            execution_id: encode_execution_id(execution_id),
            checkpoint_name: KubernetesCheckpointStore::object_name(execution_id),
            initial_operation_json: serde_json::to_string(&initial).unwrap(),
        };
        let terminal = terminal_from_operation(
            &completed_stages().completed,
            RemoveReplicaActivityAccounting::default(),
        )
        .unwrap();
        let outcome = encode_terminal(terminal.clone());
        let loaded = validate_loaded_terminal(&reference, &outcome, 0);
        assert!(loaded.is_ok(), "{loaded:?}");

        let mut inexact = terminal.clone();
        let DurableRemoveReplicaPilotTerminal::Completed { authority, .. } = &mut inexact else {
            unreachable!();
        };
        authority.action_id.push_str("-drift");
        assert!(
            validate_loaded_terminal(&reference, &encode_terminal(inexact), 0)
                .unwrap_err()
                .contains("action identity")
        );

        let mut inexact_signature = terminal.clone();
        let DurableRemoveReplicaPilotTerminal::Completed { authority, .. } = &mut inexact_signature
        else {
            unreachable!();
        };
        authority.input_signature.push_str("-drift");
        assert!(
            validate_loaded_terminal(&reference, &encode_terminal(inexact_signature), 0)
                .unwrap_err()
                .contains("reduced configuration")
        );

        let mut inexact_configuration = terminal.clone();
        let DurableRemoveReplicaPilotTerminal::Completed { authority, .. } =
            &mut inexact_configuration
        else {
            unreachable!();
        };
        authority.configuration_signature.push_str("-drift");
        assert!(
            validate_loaded_terminal(&reference, &encode_terminal(inexact_configuration), 0)
                .unwrap_err()
                .contains("reduced configuration")
        );

        let mut nonterminal = terminal;
        let DurableRemoveReplicaPilotTerminal::Completed { cleanup, .. } = &mut nonterminal else {
            unreachable!();
        };
        cleanup.target_retirement = Some(TargetRetirementObservationStatus::InProgress);
        assert!(
            validate_loaded_terminal(&reference, &encode_terminal(nonterminal), 0)
                .unwrap_err()
                .contains("cleanup evidence")
        );
    }

    #[tokio::test]
    async fn remove_replica_pilot_runs_a_deterministic_success_transcript() {
        let reference = reference(DurableRemoveMode::ScaleDown);
        let execution = execution_spec(&reference).unwrap();
        let initial = initial_operation(&reference).unwrap();
        let pods = pod_identities();
        let initial_observations = observations(&initial);
        let initial_addressed = addressed(&initial_observations);
        let initial_resolver = RemoveReplicaPreparedActivityResolver::new(
            &initial,
            &initial_observations,
            &pods,
            Some("secondary"),
            &initial_addressed,
            10,
        );
        let backend = InMemoryCheckpointStore::new();
        let mut host = measured_host(&execution, backend, 61);
        let workflow = DurableRemoveReplicaWorkflow;

        let HostOutcome::DispatchPermitted {
            permit: freeze_permit,
            ..
        } = host
            .turn_and_expose_with(&workflow, execution.clone(), &initial_resolver)
            .await
        else {
            panic!("expected freeze observation");
        };
        let freeze_input =
            decode_remove_activity_input(freeze_permit.activity().spec().input()).unwrap();
        let freeze_decision = evaluate_adapter_step(
            &initial,
            &initial_observations,
            &pods,
            Some("secondary"),
            10,
        )
        .unwrap();
        let HostOutcome::DispatchPermitted {
            permit: replica_permit,
            ..
        } = host
            .observe_and_turn_with(
                &workflow,
                &execution,
                ActivityObservation::new(
                    freeze_permit.activity().clone(),
                    encode_step_result(
                        &completed_step(freeze_decision, &freeze_input.kind).unwrap(),
                    )
                    .unwrap(),
                ),
                &initial_resolver,
            )
            .await
        else {
            panic!("expected coarse remove exposure");
        };
        let replica_input =
            decode_remove_activity_input(replica_permit.activity().spec().input()).unwrap();
        let replica_operation = replica_input.state.apply_to(&initial).unwrap();
        let intent = replica_operation.remove_intent.as_ref().unwrap().clone();
        let mut committed_observations = observations(&replica_operation);
        {
            let primary = committed_observations.get_mut(&1).unwrap();
            primary.status.configuration = Some(configuration(
                &replica_operation.target_snapshot,
                ReplicaConfigurationMode::Current,
            ));
            primary.status.active_replica_connections.clear();
        }
        set_remove_action_progress(
            &replica_operation,
            &mut committed_observations,
            RemoveReplicaProgress {
                phase: RemoveReplicaCoordinatorPhase::Attesting,
                attempt_id: intent.attempt_id.clone(),
                commit_observed: true,
                commit_observed_unix_seconds: Some(123),
                connection_absent: true,
                target_retirement: TargetRetirementObservation::Completed,
                retirement_expiry_unix_seconds: Some(183),
                compensation_expiry_unix_seconds: None,
                error: None,
                current_install_dispatched: true,
            },
            DurableActionState::Completed,
            Some(RemoveReplicaTerminalResult::CommittedClean),
        );
        let committed_addressed = addressed(&committed_observations);
        let committed_resolver = RemoveReplicaPreparedActivityResolver::new(
            &initial,
            &committed_observations,
            &pods,
            Some("secondary"),
            &committed_addressed,
            200,
        );
        let replica_decision = evaluate_adapter_step(
            &replica_operation,
            &committed_observations,
            &pods,
            Some("secondary"),
            200,
        )
        .unwrap();
        let HostOutcome::DispatchPermitted {
            permit: commit_permit,
            ..
        } = host
            .observe_and_turn_with(
                &workflow,
                &execution,
                ActivityObservation::new(
                    replica_permit.activity().clone(),
                    encode_step_result(
                        &completed_step(replica_decision, &replica_input.kind).unwrap(),
                    )
                    .unwrap(),
                ),
                &committed_resolver,
            )
            .await
        else {
            panic!("expected commit-evidence observation");
        };
        let commit_input =
            decode_remove_activity_input(commit_permit.activity().spec().input()).unwrap();
        let commit_operation = commit_input.state.apply_to(&initial).unwrap();
        let commit_decision = evaluate_adapter_step(
            &commit_operation,
            &committed_observations,
            &pods,
            Some("secondary"),
            200,
        )
        .unwrap();

        let mut cleanup_observations = committed_observations;
        let target = cleanup_observations.get_mut(&3).unwrap();
        target.status.role = Role::None;
        target.status.healthy = false;
        let cleanup_addressed = addressed(&cleanup_observations);
        let cleanup_resolver = RemoveReplicaPreparedActivityResolver::new(
            &initial,
            &cleanup_observations,
            &pods,
            Some("secondary"),
            &cleanup_addressed,
            200,
        );
        let HostOutcome::DispatchPermitted {
            permit: label_permit,
            ..
        } = host
            .observe_and_turn_with(
                &workflow,
                &execution,
                ActivityObservation::new(
                    commit_permit.activity().clone(),
                    encode_step_result(
                        &completed_step(commit_decision, &commit_input.kind).unwrap(),
                    )
                    .unwrap(),
                ),
                &cleanup_resolver,
            )
            .await
        else {
            panic!("expected exact-UID retired-label exposure");
        };
        let label_input =
            decode_remove_activity_input(label_permit.activity().spec().input()).unwrap();
        assert!(matches!(
            label_input.kind,
            RemoveReplicaActivityKind::PreparedLabel { .. }
        ));
        let label_operation = label_input.state.apply_to(&initial).unwrap();
        let label_decision = evaluate_adapter_step(
            &label_operation,
            &cleanup_observations,
            &pods,
            Some("retired"),
            200,
        )
        .unwrap();
        let retired_resolver = RemoveReplicaPreparedActivityResolver::new(
            &initial,
            &cleanup_observations,
            &pods,
            Some("retired"),
            &cleanup_addressed,
            200,
        );
        let HostOutcome::DispatchPermitted {
            permit: delete_permit,
            ..
        } = host
            .observe_and_turn_with(
                &workflow,
                &execution,
                ActivityObservation::new(
                    label_permit.activity().clone(),
                    encode_step_result(&completed_step(label_decision, &label_input.kind).unwrap())
                        .unwrap(),
                ),
                &retired_resolver,
            )
            .await
        else {
            panic!("expected exact-UID delete exposure");
        };
        let delete_input =
            decode_remove_activity_input(delete_permit.activity().spec().input()).unwrap();
        assert!(matches!(
            delete_input.kind,
            RemoveReplicaActivityKind::PreparedDelete { .. }
        ));
        let delete_operation = delete_input.state.apply_to(&initial).unwrap();
        let mut deleted_pods = pods;
        deleted_pods.remove(&3);
        let delete_decision = evaluate_adapter_step(
            &delete_operation,
            &cleanup_observations,
            &deleted_pods,
            None,
            200,
        )
        .unwrap();
        let deleted_resolver = RemoveReplicaPreparedActivityResolver::new(
            &initial,
            &cleanup_observations,
            &deleted_pods,
            None,
            &cleanup_addressed,
            200,
        );
        let HostOutcome::WorkflowCompleted { outcome, .. } = host
            .observe_and_turn_with(
                &workflow,
                &execution,
                ActivityObservation::new(
                    delete_permit.activity().clone(),
                    encode_step_result(
                        &completed_step(delete_decision, &delete_input.kind).unwrap(),
                    )
                    .unwrap(),
                ),
                &deleted_resolver,
            )
            .await
        else {
            panic!("expected compact completed terminal");
        };
        let terminal = validate_loaded_terminal(&reference, &outcome, 5).unwrap();
        assert!(matches!(
            terminal,
            DurableRemoveReplicaPilotTerminal::Completed {
                accounting: RemoveReplicaActivityAccounting {
                    external_effect_count: 3,
                    passive_observation_count: 2
                },
                ..
            }
        ));
    }

    #[tokio::test]
    async fn remove_replica_pilot_runs_a_deterministic_safe_compensation_transcript() {
        let reference = reference(DurableRemoveMode::ScaleDown);
        let execution = execution_spec(&reference).unwrap();
        let initial = initial_operation(&reference).unwrap();
        let pods = pod_identities();
        let mut observations = observations(&initial);
        let initial_addressed = addressed(&observations);
        let initial_resolver = RemoveReplicaPreparedActivityResolver::new(
            &initial,
            &observations,
            &pods,
            Some("secondary"),
            &initial_addressed,
            10,
        );
        let backend = InMemoryCheckpointStore::new();
        let mut host = measured_host(&execution, backend, 62);
        let workflow = DurableRemoveReplicaWorkflow;
        let HostOutcome::DispatchPermitted {
            permit: freeze_permit,
            ..
        } = host
            .turn_and_expose_with(&workflow, execution.clone(), &initial_resolver)
            .await
        else {
            panic!("expected freeze observation");
        };
        let freeze_input =
            decode_remove_activity_input(freeze_permit.activity().spec().input()).unwrap();
        let freeze_decision =
            evaluate_adapter_step(&initial, &observations, &pods, Some("secondary"), 10).unwrap();
        let HostOutcome::DispatchPermitted {
            permit: replica_permit,
            ..
        } = host
            .observe_and_turn_with(
                &workflow,
                &execution,
                ActivityObservation::new(
                    freeze_permit.activity().clone(),
                    encode_step_result(
                        &completed_step(freeze_decision, &freeze_input.kind).unwrap(),
                    )
                    .unwrap(),
                ),
                &initial_resolver,
            )
            .await
        else {
            panic!("expected coarse remove exposure");
        };
        let replica_input =
            decode_remove_activity_input(replica_permit.activity().spec().input()).unwrap();
        let replica_operation = replica_input.state.apply_to(&initial).unwrap();
        let intent = replica_operation.remove_intent.as_ref().unwrap().clone();
        set_remove_action_progress(
            &replica_operation,
            &mut observations,
            RemoveReplicaProgress {
                phase: RemoveReplicaCoordinatorPhase::Compensating,
                attempt_id: intent.attempt_id,
                commit_observed: false,
                commit_observed_unix_seconds: None,
                connection_absent: false,
                target_retirement: TargetRetirementObservation::NotAttempted,
                retirement_expiry_unix_seconds: None,
                compensation_expiry_unix_seconds: Some(50),
                error: Some("pre-commit quorum loss".to_string()),
                current_install_dispatched: false,
            },
            DurableActionState::Completed,
            Some(RemoveReplicaTerminalResult::Compensated),
        );
        let compensated_addressed = addressed(&observations);
        let compensated_resolver = RemoveReplicaPreparedActivityResolver::new(
            &initial,
            &observations,
            &pods,
            Some("secondary"),
            &compensated_addressed,
            20,
        );
        let replica_decision = evaluate_adapter_step(
            &replica_operation,
            &observations,
            &pods,
            Some("secondary"),
            20,
        )
        .unwrap();
        let HostOutcome::DispatchPermitted {
            permit: compensation_permit,
            ..
        } = host
            .observe_and_turn_with(
                &workflow,
                &execution,
                ActivityObservation::new(
                    replica_permit.activity().clone(),
                    encode_step_result(
                        &completed_step(replica_decision, &replica_input.kind).unwrap(),
                    )
                    .unwrap(),
                ),
                &compensated_resolver,
            )
            .await
        else {
            panic!("expected compensation observation");
        };
        let compensation_input =
            decode_remove_activity_input(compensation_permit.activity().spec().input()).unwrap();
        let compensation_operation = compensation_input.state.apply_to(&initial).unwrap();
        let compensation_decision = evaluate_adapter_step(
            &compensation_operation,
            &observations,
            &pods,
            Some("secondary"),
            20,
        )
        .unwrap();
        let HostOutcome::WorkflowCompleted { outcome, .. } = host
            .observe_and_turn_with(
                &workflow,
                &execution,
                ActivityObservation::new(
                    compensation_permit.activity().clone(),
                    encode_step_result(
                        &completed_step(compensation_decision, &compensation_input.kind).unwrap(),
                    )
                    .unwrap(),
                ),
                &compensated_resolver,
            )
            .await
        else {
            panic!("expected compact compensated terminal");
        };
        assert!(matches!(
            validate_loaded_terminal(&reference, &outcome, 3).unwrap(),
            DurableRemoveReplicaPilotTerminal::Compensated {
                accounting: RemoveReplicaActivityAccounting {
                    external_effect_count: 1,
                    passive_observation_count: 2
                },
                ..
            }
        ));
    }

    #[tokio::test]
    async fn remove_replica_pilot_restarts_at_every_durable_boundary_without_command_drift() {
        let reference = reference(DurableRemoveMode::ScaleDown);
        let execution = execution_spec(&reference).unwrap();
        let initial = initial_operation(&reference).unwrap();
        let pods = pod_identities();
        let initial_observations = observations(&initial);
        let initial_addressed = addressed(&initial_observations);
        let initial_resolver = RemoveReplicaPreparedActivityResolver::new(
            &initial,
            &initial_observations,
            &pods,
            Some("secondary"),
            &initial_addressed,
            10,
        );
        let backend = InMemoryCheckpointStore::new();
        let workflow = DurableRemoveReplicaWorkflow;
        let mut history_specs = Vec::new();

        let mut host = measured_host(&execution, backend.clone(), 61);
        let HostOutcome::ScheduleAccepted {
            activity: freeze_activity,
            ..
        } = host.turn(&workflow, execution.clone()).await
        else {
            panic!("expected accepted freeze-observation schedule");
        };
        let scheduled = backend
            .load(execution.execution_id())
            .await
            .unwrap()
            .unwrap();
        let scheduled_payload = scheduled
            .checkpoint()
            .decode_and_validate(&execution, checkpoint_limits())
            .unwrap();
        let scheduled_records = scheduled_payload.active_activities().unwrap();
        assert_eq!(scheduled_records.len(), 1);
        assert_eq!(
            scheduled_records[0].logical_id(execution.execution_id()),
            freeze_activity
        );
        assert!(matches!(
            scheduled_records[0].state(),
            ActivityState::Scheduled
        ));

        let mut host = measured_host(&execution, backend.clone(), 62);
        let HostOutcome::DispatchPermitted {
            permit: freeze_permit,
            boundary: PersistenceBoundary::Exposure,
            ..
        } = host.turn(&workflow, execution.clone()).await
        else {
            panic!("expected accepted freeze-observation exposure");
        };
        assert_eq!(*freeze_permit.activity(), freeze_activity);
        let freeze_input =
            decode_remove_activity_input(freeze_permit.activity().spec().input()).unwrap();
        assert_eq!(
            freeze_input,
            DurableRemoveReplicaActivityInput {
                version: REMOVE_REPLICA_PILOT_VERSION,
                state: DurableRemoveReplicaState::from_operation(&initial),
                kind: RemoveReplicaActivityKind::PassiveObservation,
            }
        );
        history_specs.push(freeze_permit.activity().spec().clone());
        assert_exposed_history(&backend, &execution, &freeze_permit, &history_specs).await;

        let host = restart_into_quarantine(&backend, &execution, 63, &freeze_permit).await;
        let freeze_decision = evaluate_adapter_step(
            &initial,
            &initial_observations,
            &pods,
            Some("secondary"),
            10,
        )
        .unwrap();
        assert!(matches!(
            host.observe(
                &execution,
                ActivityObservation::new(
                    freeze_permit.activity().clone(),
                    encode_step_result(
                        &completed_step(freeze_decision, &freeze_input.kind).unwrap()
                    )
                    .unwrap(),
                ),
            )
            .await,
            HostOutcome::ObservationAccepted { activity, .. }
                if activity == *freeze_permit.activity()
        ));
        assert_completed_history(&backend, &execution, &history_specs).await;

        let mut host = measured_host(&execution, backend.clone(), 64);
        let HostOutcome::DispatchPermitted {
            permit: replica_permit,
            boundary: PersistenceBoundary::ScheduleExposure,
            ..
        } = host
            .turn_and_expose_with(&workflow, execution.clone(), &initial_resolver)
            .await
        else {
            panic!("expected exact coarse remove exposure after observation restart");
        };
        let replica_input =
            decode_remove_activity_input(replica_permit.activity().spec().input()).unwrap();
        let RemoveReplicaActivityKind::PreparedReplica {
            command: replica_command,
        } = &replica_input.kind
        else {
            panic!("expected exact prepared replica command");
        };
        let replica_operation = replica_input.state.apply_to(&initial).unwrap();
        let intent = replica_operation.remove_intent.as_ref().unwrap().clone();
        assert_eq!(replica_command.target_id, initial.old_primary_id);
        assert_eq!(
            replica_command.target_instance_id,
            intent.primary_instance_id
        );
        assert_eq!(replica_command.action_id, intent.action_id);
        assert_eq!(replica_command.action_signature, intent.input_signature);
        history_specs.push(replica_permit.activity().spec().clone());
        assert_exposed_history(&backend, &execution, &replica_permit, &history_specs).await;

        let host = restart_into_quarantine(&backend, &execution, 65, &replica_permit).await;
        let mut committed_observations = observations(&replica_operation);
        {
            let primary = committed_observations.get_mut(&1).unwrap();
            primary.status.configuration = Some(configuration(
                &replica_operation.target_snapshot,
                ReplicaConfigurationMode::Current,
            ));
            primary.status.active_replica_connections.clear();
        }
        set_remove_action_progress(
            &replica_operation,
            &mut committed_observations,
            RemoveReplicaProgress {
                phase: RemoveReplicaCoordinatorPhase::Attesting,
                attempt_id: intent.attempt_id.clone(),
                commit_observed: true,
                commit_observed_unix_seconds: Some(123),
                connection_absent: true,
                target_retirement: TargetRetirementObservation::Completed,
                retirement_expiry_unix_seconds: Some(183),
                compensation_expiry_unix_seconds: None,
                error: None,
                current_install_dispatched: true,
            },
            DurableActionState::Completed,
            Some(RemoveReplicaTerminalResult::CommittedClean),
        );
        let replica_decision = evaluate_adapter_step(
            &replica_operation,
            &committed_observations,
            &pods,
            Some("secondary"),
            200,
        )
        .unwrap();
        assert!(matches!(
            host.observe(
                &execution,
                ActivityObservation::new(
                    replica_permit.activity().clone(),
                    encode_step_result(
                        &completed_step(replica_decision, &replica_input.kind).unwrap()
                    )
                    .unwrap(),
                ),
            )
            .await,
            HostOutcome::ObservationAccepted { activity, .. }
                if activity == *replica_permit.activity()
        ));
        assert_completed_history(&backend, &execution, &history_specs).await;

        let committed_addressed = addressed(&committed_observations);
        let committed_resolver = RemoveReplicaPreparedActivityResolver::new(
            &initial,
            &committed_observations,
            &pods,
            Some("secondary"),
            &committed_addressed,
            200,
        );
        let mut host = measured_host(&execution, backend.clone(), 66);
        let HostOutcome::DispatchPermitted {
            permit: commit_permit,
            boundary: PersistenceBoundary::ScheduleExposure,
            ..
        } = host
            .turn_and_expose_with(&workflow, execution.clone(), &committed_resolver)
            .await
        else {
            panic!("expected post-commit cleanup observation after restart");
        };
        let commit_input =
            decode_remove_activity_input(commit_permit.activity().spec().input()).unwrap();
        let commit_operation = commit_input.state.apply_to(&initial).unwrap();
        assert_eq!(
            commit_operation.phase,
            DurableOperationPhase::RemoveAwaitCoordination
        );
        assert!(commit_operation.remove_commit_evidence.is_none());
        assert_eq!(
            commit_input.kind,
            RemoveReplicaActivityKind::PassiveObservation
        );
        history_specs.push(commit_permit.activity().spec().clone());
        assert_exposed_history(&backend, &execution, &commit_permit, &history_specs).await;

        let host = restart_into_quarantine(&backend, &execution, 67, &commit_permit).await;
        let commit_decision = evaluate_adapter_step(
            &commit_operation,
            &committed_observations,
            &pods,
            Some("secondary"),
            200,
        )
        .unwrap();
        let RemoveReplicaAdapterDecision::Advance(commit_state) = &commit_decision else {
            panic!("expected authoritative observation to record commit evidence");
        };
        assert_eq!(
            commit_state.phase,
            DurableOperationPhase::RemoveRecordCommit
        );
        assert_eq!(
            commit_state.committed_snapshot,
            Some(initial.target_snapshot.clone())
        );
        assert!(commit_state.remove_commit_evidence.is_some());
        let mut cleanup_observations = committed_observations;
        let target = cleanup_observations.get_mut(&3).unwrap();
        target.status.role = Role::None;
        target.status.healthy = false;
        assert!(matches!(
            host.observe(
                &execution,
                ActivityObservation::new(
                    commit_permit.activity().clone(),
                    encode_step_result(
                        &completed_step(commit_decision, &commit_input.kind).unwrap()
                    )
                    .unwrap(),
                ),
            )
            .await,
            HostOutcome::ObservationAccepted { activity, .. }
                if activity == *commit_permit.activity()
        ));
        assert_completed_history(&backend, &execution, &history_specs).await;

        let cleanup_addressed = addressed(&cleanup_observations);
        let cleanup_resolver = RemoveReplicaPreparedActivityResolver::new(
            &initial,
            &cleanup_observations,
            &pods,
            Some("secondary"),
            &cleanup_addressed,
            200,
        );
        let mut host = measured_host(&execution, backend.clone(), 68);
        let HostOutcome::DispatchPermitted {
            permit: label_permit,
            boundary: PersistenceBoundary::ScheduleExposure,
            ..
        } = host
            .turn_and_expose_with(&workflow, execution.clone(), &cleanup_resolver)
            .await
        else {
            panic!("expected exact-UID retired-label exposure after restart");
        };
        let label_input =
            decode_remove_activity_input(label_permit.activity().spec().input()).unwrap();
        let RemoveReplicaActivityKind::PreparedLabel {
            command: label_command,
        } = &label_input.kind
        else {
            panic!("expected exact-UID retired-label command");
        };
        let label_operation = label_input.state.apply_to(&initial).unwrap();
        assert_eq!(
            label_operation.phase,
            DurableOperationPhase::RemoveAwaitCleanup
        );
        assert_eq!(
            label_operation.committed_snapshot,
            Some(initial.target_snapshot.clone())
        );
        assert!(label_operation.remove_commit_evidence.is_some());
        assert_eq!(label_command.target_id, 3);
        assert_eq!(label_command.pod_name, "set-2");
        assert_eq!(label_command.expected_uid, "three");
        assert_eq!(label_command.role, "retired");
        assert!(label_command.has_valid_identity_signature());
        assert!(!remove_label_postcondition_satisfied(
            label_command,
            &pods,
            Some("secondary")
        ));
        assert!(remove_label_postcondition_satisfied(
            label_command,
            &pods,
            Some("retired")
        ));
        let mut replacement_pods = pods.clone();
        replacement_pods.insert(3, "replacement".to_string());
        assert!(remove_label_postcondition_satisfied(
            label_command,
            &replacement_pods,
            Some("secondary")
        ));
        history_specs.push(label_permit.activity().spec().clone());
        assert_exposed_history(&backend, &execution, &label_permit, &history_specs).await;
        let host = restart_into_quarantine(&backend, &execution, 69, &label_permit).await;
        let label_decision = evaluate_adapter_step(
            &label_operation,
            &cleanup_observations,
            &pods,
            Some("retired"),
            200,
        )
        .unwrap();
        assert!(matches!(
            host.observe(
                &execution,
                ActivityObservation::new(
                    label_permit.activity().clone(),
                    encode_step_result(
                        &completed_step(label_decision, &label_input.kind).unwrap()
                    )
                    .unwrap(),
                ),
            )
            .await,
            HostOutcome::ObservationAccepted { activity, .. }
                if activity == *label_permit.activity()
        ));
        assert_completed_history(&backend, &execution, &history_specs).await;

        let retired_resolver = RemoveReplicaPreparedActivityResolver::new(
            &initial,
            &cleanup_observations,
            &pods,
            Some("retired"),
            &cleanup_addressed,
            200,
        );
        let mut host = measured_host(&execution, backend.clone(), 70);
        let HostOutcome::DispatchPermitted {
            permit: delete_permit,
            boundary: PersistenceBoundary::ScheduleExposure,
            ..
        } = host
            .turn_and_expose_with(&workflow, execution.clone(), &retired_resolver)
            .await
        else {
            panic!("expected exact-UID delete exposure after restart");
        };
        let delete_input =
            decode_remove_activity_input(delete_permit.activity().spec().input()).unwrap();
        let RemoveReplicaActivityKind::PreparedDelete {
            command: delete_command,
        } = &delete_input.kind
        else {
            panic!("expected exact-UID delete command");
        };
        assert_eq!(delete_command.target_id, 3);
        assert_eq!(delete_command.pod_name, "set-2");
        assert_eq!(delete_command.expected_uid, "three");
        assert!(delete_command.has_valid_identity_signature());
        assert!(!remove_delete_postcondition_satisfied(
            delete_command,
            &pods
        ));
        assert!(remove_delete_postcondition_satisfied(
            delete_command,
            &replacement_pods
        ));
        history_specs.push(delete_permit.activity().spec().clone());
        assert_exposed_history(&backend, &execution, &delete_permit, &history_specs).await;

        let host = restart_into_quarantine(&backend, &execution, 71, &delete_permit).await;
        let delete_operation = delete_input.state.apply_to(&initial).unwrap();
        let mut deleted_pods = pods.clone();
        deleted_pods.remove(&3);
        let delete_decision = evaluate_adapter_step(
            &delete_operation,
            &cleanup_observations,
            &deleted_pods,
            None,
            200,
        )
        .unwrap();
        assert!(matches!(
            host.observe(
                &execution,
                ActivityObservation::new(
                    delete_permit.activity().clone(),
                    encode_step_result(
                        &completed_step(delete_decision, &delete_input.kind).unwrap()
                    )
                    .unwrap(),
                ),
            )
            .await,
            HostOutcome::ObservationAccepted { activity, .. }
                if activity == *delete_permit.activity()
        ));
        let active_encoded_len =
            assert_completed_history(&backend, &execution, &history_specs).await;

        let deleted_resolver = RemoveReplicaPreparedActivityResolver::new(
            &initial,
            &cleanup_observations,
            &deleted_pods,
            None,
            &cleanup_addressed,
            200,
        );
        let mut host = measured_host(&execution, backend.clone(), 72);
        let HostOutcome::WorkflowCompleted {
            outcome,
            boundary: PersistenceBoundary::Completion,
            ..
        } = host
            .turn_and_expose_with(&workflow, execution.clone(), &deleted_resolver)
            .await
        else {
            panic!("expected compact completed terminal after observation restart");
        };
        assert!(matches!(
            validate_loaded_terminal(&reference, &outcome, 5).unwrap(),
            DurableRemoveReplicaPilotTerminal::Completed {
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
        let terminal_stored = backend
            .load(execution.execution_id())
            .await
            .unwrap()
            .unwrap();
        assert!(terminal_stored.checkpoint().encoded_len().unwrap() < active_encoded_len);
        let terminal_payload = terminal_stored
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

        let polls = Arc::new(AtomicUsize::new(0));
        let mut reloaded = measured_host(&execution, backend, 73);
        let HostOutcome::WorkflowCompleted {
            outcome: reloaded_outcome,
            boundary: PersistenceBoundary::Completion,
            ..
        } = reloaded
            .turn(
                &PollCountingWorkflow {
                    polls: polls.clone(),
                },
                execution,
            )
            .await
        else {
            panic!("expected terminal reload without workflow polling");
        };
        assert_eq!(reloaded_outcome, outcome);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn remove_replica_pilot_rejects_oversized_typed_input_and_result() {
        let operation = initial(DurableRemoveMode::ScaleDown);
        let mut state = DurableRemoveReplicaState::from_operation(&operation);
        state.last_error = Some("x".repeat(REMOVE_REPLICA_PILOT_MAX_ACTIVITY_INPUT_BYTES));
        assert!(
            encode_activity_input::<DurableRemoveReplicaActivity>(
                &DurableRemoveReplicaActivityInput {
                    version: REMOVE_REPLICA_PILOT_VERSION,
                    state: state.clone(),
                    kind: RemoveReplicaActivityKind::PassiveObservation,
                }
            )
            .is_err()
        );
        state.last_error = Some("x".repeat(REMOVE_REPLICA_PILOT_MAX_ACTIVITY_RESULT_BYTES));
        assert!(
            encode_step_result(&DurableRemoveReplicaStepResult::Advance {
                operation: state,
                completion: RemoveReplicaActivityCompletion::PassiveObservation,
            })
            .is_err()
        );
    }

    #[test]
    fn remove_replica_pilot_rejects_semantic_replay_mismatch() {
        let initial = initial(DurableRemoveMode::ScaleDown);
        let (operation, observations) = freeze_and_dispatch(DurableRemoveMode::ScaleDown);
        let pods = pod_identities();
        let addressed = addressed(&observations);
        let resolver = RemoveReplicaPreparedActivityResolver::new(
            &initial,
            &observations,
            &pods,
            Some("secondary"),
            &addressed,
            10,
        );
        let logical = activity_spec(&DurableRemoveReplicaActivityInput {
            version: REMOVE_REPLICA_PILOT_VERSION,
            state: DurableRemoveReplicaState::from_operation(&operation),
            kind: RemoveReplicaActivityKind::PassiveObservation,
        })
        .unwrap();
        let prepared = resolver.resolve(&logical, None).unwrap();
        let mut input = decode_remove_activity_input(prepared.input()).unwrap();
        let RemoveReplicaActivityKind::PreparedReplica { command } = &mut input.kind else {
            panic!("expected replica command");
        };
        command.action_signature.push_str("-drift");
        let drifted = activity_spec(&input).unwrap();
        assert_eq!(resolver.resolve(&logical, Some(&drifted)).unwrap(), logical);
    }

    async fn expose_replica_command() -> (
        RemoveReplicaPilotHost,
        InMemoryCheckpointStore,
        ExecutionSpec,
        DurableOperationStatus,
        DispatchPermit,
        DurableRemoveReplicaActivityInput,
    ) {
        let reference = reference(DurableRemoveMode::ScaleDown);
        let execution = execution_spec(&reference).unwrap();
        let initial = initial_operation(&reference).unwrap();
        let observations = observations(&initial);
        let pods = pod_identities();
        let addressed = addressed(&observations);
        let resolver = RemoveReplicaPreparedActivityResolver::new(
            &initial,
            &observations,
            &pods,
            Some("secondary"),
            &addressed,
            10,
        );
        let backend = InMemoryCheckpointStore::new();
        let mut host = measured_host(&execution, backend.clone(), 7);
        let workflow = DurableRemoveReplicaWorkflow;
        let HostOutcome::DispatchPermitted { permit: first, .. } = host
            .turn_and_expose_with(&workflow, execution.clone(), &resolver)
            .await
        else {
            panic!("expected passive observation exposure");
        };
        let first_input = decode_remove_activity_input(first.activity().spec().input()).unwrap();
        assert_eq!(
            first_input.kind,
            RemoveReplicaActivityKind::PassiveObservation
        );
        let decision =
            evaluate_adapter_step(&initial, &observations, &pods, Some("secondary"), 10).unwrap();
        let first_result = completed_step(decision, &first_input.kind).unwrap();
        let outcome = host
            .observe_and_turn_with(
                &workflow,
                &execution,
                ActivityObservation::new(
                    first.activity().clone(),
                    encode_step_result(&first_result).unwrap(),
                ),
                &resolver,
            )
            .await;
        let HostOutcome::DispatchPermitted { permit, .. } = outcome else {
            panic!("expected exact replica command exposure, found {outcome:?}");
        };
        let input = decode_remove_activity_input(permit.activity().spec().input()).unwrap();
        assert!(matches!(
            input.kind,
            RemoveReplicaActivityKind::PreparedReplica { .. }
        ));
        (host, backend, execution, initial, permit, input)
    }

    #[tokio::test]
    async fn remove_replica_pilot_exposes_exact_command_before_dispatch() {
        let (host, backend, execution, _, permit, input) = expose_replica_command().await;
        let loaded = backend
            .load(execution.execution_id())
            .await
            .unwrap()
            .unwrap();
        let payload = loaded
            .checkpoint()
            .decode_and_validate(&execution, checkpoint_limits())
            .unwrap();
        let record = payload.active_activities().unwrap().last().unwrap();
        assert_eq!(
            record.logical_id(execution.execution_id()),
            *permit.activity()
        );
        assert_eq!(record.spec(), permit.activity().spec());
        assert!(matches!(
            input.kind,
            RemoveReplicaActivityKind::PreparedReplica { .. }
        ));
        assert_eq!(host.store().measurements().accepted_writes, 2);
    }

    struct OneActivityWorkflow {
        input: DurableRemoveReplicaActivityInput,
    }

    #[async_trait]
    impl Workflow for OneActivityWorkflow {
        async fn run(&self, context: &mut WorkflowContext<'_>, _: ExactBytes) -> TerminalOutcome {
            let _ = context
                .call::<DurableRemoveReplicaActivity>(self.input.clone())
                .await;
            TerminalOutcome::succeeded(ExactBytes::new(b"done"))
        }
    }

    async fn permit_for_input(
        input: DurableRemoveReplicaActivityInput,
        seed: u8,
    ) -> DispatchPermit {
        let execution = ExecutionSpec::new(
            ExecutionId::from_bytes([seed; 16]),
            ExactBytes::new(b"one"),
            REMOVE_REPLICA_PILOT_MAX_TERMINAL_BYTES,
        );
        let backend = InMemoryCheckpointStore::new();
        let mut host = measured_host(&execution, backend, seed);
        let HostOutcome::DispatchPermitted { permit, .. } = host
            .turn_and_expose(&OneActivityWorkflow { input }, execution)
            .await
        else {
            panic!("expected prepared permit");
        };
        permit
    }

    #[tokio::test]
    async fn remove_replica_pilot_consumes_delete_and_replica_permits_once() {
        let (_, _, _, initial, replica_permit, replica_input) = expose_replica_command().await;
        let replica_operation = replica_input.state.apply_to(&initial).unwrap();
        let replica_activity = replica_permit.activity().clone();
        let replica_attempt = replica_permit.attempt_id();
        let mut guard = RemoveReplicaPermitGuard::new(replica_permit);
        assert!(
            guard
                .consume_for(
                    &replica_operation,
                    &replica_input.kind,
                    &replica_activity,
                    replica_attempt
                )
                .is_ok()
        );
        assert!(
            guard
                .consume_for(
                    &replica_operation,
                    &replica_input.kind,
                    &replica_activity,
                    replica_attempt
                )
                .unwrap_err()
                .contains("already consumed")
        );

        let stages = completed_stages();
        let delete = DeleteEffectCommand::new(3, "set-2".to_string(), "three".to_string());
        let delete_input = DurableRemoveReplicaActivityInput {
            version: REMOVE_REPLICA_PILOT_VERSION,
            state: DurableRemoveReplicaState::from_operation(&stages.deleting),
            kind: RemoveReplicaActivityKind::PreparedDelete { command: delete },
        };
        let delete_permit = permit_for_input(delete_input.clone(), 21).await;
        let delete_activity = delete_permit.activity().clone();
        let delete_attempt = delete_permit.attempt_id();
        let mut delete_guard = RemoveReplicaPermitGuard::new(delete_permit);
        assert!(
            delete_guard
                .consume_for(
                    &stages.deleting,
                    &delete_input.kind,
                    &delete_activity,
                    delete_attempt
                )
                .is_ok()
        );
        assert!(
            delete_guard
                .consume_for(
                    &stages.deleting,
                    &delete_input.kind,
                    &delete_activity,
                    delete_attempt
                )
                .unwrap_err()
                .contains("already consumed")
        );
    }

    #[test]
    fn remove_replica_pilot_rejects_preparation_after_primary_target_or_uid_drift() {
        let initial_operation = initial(DurableRemoveMode::ScaleDown);
        let (operation, observations) = freeze_and_dispatch(DurableRemoveMode::ScaleDown);
        let mut primary_drift = observations.clone();
        primary_drift.get_mut(&1).unwrap().status.instance_id =
            ReplicaInstanceId::new("replacement-primary");
        assert!(!matches!(
            resolve_kind(
                &initial_operation,
                &operation,
                &primary_drift,
                &pod_identities(),
                Some("secondary")
            ),
            Ok(RemoveReplicaActivityKind::PreparedReplica { .. })
        ));
        let mut target_drift = observations;
        target_drift.get_mut(&3).unwrap().status.instance_id =
            ReplicaInstanceId::new("replacement-target");
        let initial_target = initial(DurableRemoveMode::ScaleDown);
        assert!(
            resolve_kind(
                &initial_target,
                &initial_target,
                &target_drift,
                &pod_identities(),
                Some("secondary")
            )
            .is_err()
        );
        let stages = completed_stages();
        let mut uid_drift = pod_identities();
        uid_drift.insert(3, "replacement".to_string());
        assert!(
            prepare_remove_delete_effect_command(&stages.deleting, "set-2", "three", &uid_drift)
                .is_err()
        );
    }

    async fn assert_exposed_checkpoint_is_quarantined() {
        let (_, backend, execution, _, permit, _) = expose_replica_command().await;
        let mut restarted = measured_host(&execution, backend, 8);
        match restarted
            .turn(&DurableRemoveReplicaWorkflow, execution)
            .await
        {
            HostOutcome::Quarantined { activity, .. } => {
                assert_eq!(activity, *permit.activity());
            }
            other => panic!("expected quarantine, found {other:?}"),
        }
    }

    #[tokio::test]
    async fn remove_replica_pilot_reload_of_exposed_checkpoint_is_quarantined() {
        assert_exposed_checkpoint_is_quarantined().await;
    }

    #[tokio::test]
    async fn remove_replica_pilot_restart_before_dispatch_preserves_command() {
        let (_, backend, execution, _, permit, input) = expose_replica_command().await;
        let first_command = input.kind;
        let loaded = backend
            .load(execution.execution_id())
            .await
            .unwrap()
            .unwrap();
        let payload = loaded
            .checkpoint()
            .decode_and_validate(&execution, checkpoint_limits())
            .unwrap();
        let recorded = payload.active_activities().unwrap().last().unwrap();
        assert_eq!(recorded.spec(), permit.activity().spec());
        assert_eq!(
            decode_remove_activity_input(recorded.spec().input())
                .unwrap()
                .kind,
            first_command
        );
    }

    #[tokio::test]
    async fn remove_replica_pilot_restart_after_exposure_is_quarantined_without_second_permit() {
        assert_exposed_checkpoint_is_quarantined().await;
    }

    #[tokio::test]
    async fn remove_replica_pilot_restart_after_observation_replays_result() {
        let reference = reference(DurableRemoveMode::ScaleDown);
        let execution = execution_spec(&reference).unwrap();
        let initial = initial_operation(&reference).unwrap();
        let observations = observations(&initial);
        let pods = pod_identities();
        let addressed = addressed(&observations);
        let resolver = RemoveReplicaPreparedActivityResolver::new(
            &initial,
            &observations,
            &pods,
            Some("secondary"),
            &addressed,
            10,
        );
        let backend = InMemoryCheckpointStore::new();
        let mut host = measured_host(&execution, backend.clone(), 31);
        let HostOutcome::DispatchPermitted { permit, .. } = host
            .turn_and_expose_with(&DurableRemoveReplicaWorkflow, execution.clone(), &resolver)
            .await
        else {
            panic!("expected first observation");
        };
        let first_input = decode_remove_activity_input(permit.activity().spec().input()).unwrap();
        let decision =
            evaluate_adapter_step(&initial, &observations, &pods, Some("secondary"), 10).unwrap();
        assert!(matches!(
            host.observe(
                &execution,
                ActivityObservation::new(
                    permit.activity().clone(),
                    encode_step_result(&completed_step(decision, &first_input.kind).unwrap())
                        .unwrap()
                )
            )
            .await,
            HostOutcome::ObservationAccepted { .. }
        ));
        let mut restarted = measured_host(&execution, backend, 32);
        let HostOutcome::DispatchPermitted { permit, .. } = restarted
            .turn_and_expose_with(&DurableRemoveReplicaWorkflow, execution, &resolver)
            .await
        else {
            panic!("expected replay to exact next command");
        };
        assert!(matches!(
            decode_remove_activity_input(permit.activity().spec().input())
                .unwrap()
                .kind,
            RemoveReplicaActivityKind::PreparedReplica { .. }
        ));
    }

    #[test]
    fn remove_replica_pilot_restart_after_commit_rolls_forward() {
        let stages = completed_stages();
        let state = DurableRemoveReplicaState::from_operation(&stages.committed);
        let replayed = state
            .apply_to(&initial(DurableRemoveMode::ScaleDown))
            .unwrap();
        let Some(DeterministicAdvance::State(next)) =
            advance_deterministic(&replayed, RemoveReplicaActivityAccounting::default()).unwrap()
        else {
            panic!("expected committed removal to roll forward");
        };
        assert_eq!(next.phase, DurableOperationPhase::RemoveAwaitCleanup);
        assert!(next.remove_commit_evidence.is_some());
    }

    #[test]
    fn remove_replica_pilot_restart_during_cleanup_preserves_uid_fences() {
        let stages = completed_stages();
        let command = prepare_remove_delete_effect_command(
            &stages.deleting,
            "set-2",
            "three",
            &pod_identities(),
        )
        .unwrap();
        let encoded = serde_json::to_vec(&command).unwrap();
        let replayed: DeleteEffectCommand = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(replayed, command);
        assert!(replayed.has_valid_identity_signature());
        assert!(!remove_delete_postcondition_satisfied(
            &replayed,
            &pod_identities()
        ));
        let mut replaced = pod_identities();
        replaced.insert(3, "replacement".to_string());
        assert!(remove_delete_postcondition_satisfied(&replayed, &replaced));
    }

    struct PollCountingWorkflow {
        polls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Workflow for PollCountingWorkflow {
        async fn run(
            &self,
            context: &mut WorkflowContext<'_>,
            input: ExactBytes,
        ) -> TerminalOutcome {
            self.polls.fetch_add(1, Ordering::SeqCst);
            DurableRemoveReplicaWorkflow.run(context, input).await
        }
    }

    #[tokio::test]
    async fn remove_replica_pilot_terminal_reload_skips_workflow_polling() {
        let reference = reference(DurableRemoveMode::ScaleDown);
        let execution = execution_spec(&reference).unwrap();
        let terminal = terminal_from_operation(
            &completed_stages().completed,
            RemoveReplicaActivityAccounting::default(),
        )
        .unwrap();
        let outcome = encode_terminal(terminal);
        let payload = CheckpointPayload::terminal(
            ExecutionContract::new(
                execution.clone(),
                REMOVE_REPLICA_PILOT_MAX_ENCODED_CHECKPOINT_BYTES as u64,
            ),
            outcome,
            0,
        );
        let checkpoint =
            CheckpointEnvelope::encode_with_limits(&payload, checkpoint_limits()).unwrap();
        let backend = InMemoryCheckpointStore::new();
        assert!(matches!(
            backend
                .compare_and_swap(execution.execution_id(), None, checkpoint)
                .await
                .unwrap(),
            CasOutcome::Accepted(_)
        ));
        let polls = Arc::new(AtomicUsize::new(0));
        let mut host = measured_host(&execution, backend, 41);
        assert!(matches!(
            host.turn(
                &PollCountingWorkflow {
                    polls: polls.clone()
                },
                execution
            )
            .await,
            HostOutcome::WorkflowCompleted { .. }
        ));
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn remove_replica_pilot_completes_only_proven_safe_compensation() {
        let safe = compensated_operation();
        assert_eq!(safe.phase, DurableOperationPhase::Failed);
        assert!(safe.remove_commit_evidence.is_none());
        assert!(matches!(
            terminal_from_operation(&safe, RemoveReplicaActivityAccounting::default()).unwrap(),
            DurableRemoveReplicaPilotTerminal::Compensated { .. }
        ));
        let unsafe_operation = unsafe_operation();
        assert!(matches!(
            terminal_from_operation(
                &unsafe_operation,
                RemoveReplicaActivityAccounting::default()
            )
            .unwrap(),
            DurableRemoveReplicaPilotTerminal::UnsafeAmbiguity { .. }
        ));
    }

    #[tokio::test]
    async fn remove_replica_pilot_terminal_checkpoint_discards_active_history_and_retains_accounting()
     {
        let reference = reference(DurableRemoveMode::ScaleDown);
        let execution = execution_spec(&reference).unwrap();
        let accounting = RemoveReplicaActivityAccounting {
            external_effect_count: 3,
            passive_observation_count: 5,
        };
        let outcome = encode_terminal(
            terminal_from_operation(&completed_stages().completed, accounting).unwrap(),
        );
        let payload = CheckpointPayload::terminal(
            ExecutionContract::new(
                execution.clone(),
                REMOVE_REPLICA_PILOT_MAX_ENCODED_CHECKPOINT_BYTES as u64,
            ),
            outcome.clone(),
            8,
        );
        let checkpoint =
            CheckpointEnvelope::encode_with_limits(&payload, checkpoint_limits()).unwrap();
        let decoded = checkpoint
            .decode_and_validate(&execution, checkpoint_limits())
            .unwrap();
        assert!(decoded.active_activities().is_none());
        assert!(matches!(decoded.state(), CheckpointState::Terminal { .. }));
        assert_eq!(
            decode_checkpoint_terminal_accounting(&outcome, 8),
            Some(DurableActivityAccounting {
                external_effect_count: 3,
                passive_observation_count: 5,
            })
        );
        assert!(
            checkpoint.encoded_len().unwrap()
                < maximum_active_checkpoint().unwrap().encoded_len().unwrap()
        );
    }

    #[tokio::test]
    async fn remove_replica_pilot_distinguishes_lost_reply_conflict_and_safe_redelivery() {
        let reference = reference(DurableRemoveMode::ScaleDown);
        let execution = execution_spec(&reference).unwrap();
        let initial = initial_operation(&reference).unwrap();
        let observations = observations(&initial);
        let pods = pod_identities();
        let addressed = addressed(&observations);
        let resolver = RemoveReplicaPreparedActivityResolver::new(
            &initial,
            &observations,
            &pods,
            Some("secondary"),
            &addressed,
            10,
        );

        let unknown_store = InMemoryCheckpointStore::new();
        unknown_store.fail_next_compare_and_swap(InMemoryFault::OutcomeUnknownAfterApply);
        let mut host = measured_host(&execution, unknown_store.clone(), 51);
        assert!(matches!(
            host.turn_and_expose_with(&DurableRemoveReplicaWorkflow, execution.clone(), &resolver)
                .await,
            HostOutcome::ReloadRequired {
                reason: ReloadReason::OutcomeUnknown,
                ..
            }
        ));
        assert!(matches!(
            measured_host(&execution, unknown_store, 52)
                .turn(&DurableRemoveReplicaWorkflow, execution.clone())
                .await,
            HostOutcome::Quarantined { .. }
        ));

        let conflict_store = InMemoryCheckpointStore::new();
        conflict_store.fail_next_compare_and_swap(InMemoryFault::ConflictWithoutApply);
        let mut host = measured_host(&execution, conflict_store, 53);
        assert!(matches!(
            host.turn_and_expose_with(&DurableRemoveReplicaWorkflow, execution, &resolver)
                .await,
            HostOutcome::ReloadRequired {
                reason: ReloadReason::Conflict,
                ..
            }
        ));
    }

    #[test]
    fn remove_replica_pilot_exact_label_and_delete_postconditions_are_uid_fenced() {
        let stages = completed_stages();
        let label = prepare_remove_label_effect_command(
            &stages.awaiting_cleanup,
            3,
            "three",
            "retired",
            &pod_identities(),
        )
        .unwrap();
        assert!(!remove_label_postcondition_satisfied(
            &label,
            &pod_identities(),
            Some("secondary")
        ));
        assert!(matches!(
            resolve_quarantined_remove_label_effect(
                &label,
                &pod_identities(),
                Some("secondary"),
                || ()
            ),
            DurableEffectBridgeOutcome::AwaitEvidence
        ));
        assert!(remove_label_postcondition_satisfied(
            &label,
            &pod_identities(),
            Some("retired")
        ));
        assert!(matches!(
            resolve_quarantined_remove_label_effect(
                &label,
                &pod_identities(),
                Some("retired"),
                || ()
            ),
            DurableEffectBridgeOutcome::Observe(())
        ));
        let delete = prepare_remove_delete_effect_command(
            &stages.deleting,
            "set-2",
            "three",
            &pod_identities(),
        )
        .unwrap();
        assert_eq!(delete.target_id, 3);
        assert_eq!(delete.pod_name, "set-2");
        assert_eq!(delete.expected_uid, "three");
        assert!(matches!(
            resolve_quarantined_remove_delete_effect(&delete, &pod_identities(), || ()),
            DurableEffectBridgeOutcome::AwaitEvidence
        ));
        let mut deleted = pod_identities();
        deleted.remove(&3);
        assert!(matches!(
            resolve_quarantined_remove_delete_effect(&delete, &deleted, || ()),
            DurableEffectBridgeOutcome::Observe(())
        ));
    }

    #[test]
    fn remove_replica_pilot_maximum_projected_history_result_reservation_active_checkpoint_and_terminal_payload_admission()
     {
        let reference = reference(DurableRemoveMode::ScaleDown);
        assert!(execution_spec(&reference).is_ok());
        let maximum = maximum_active_checkpoint().unwrap();
        assert!(
            maximum.encoded_len().unwrap() <= REMOVE_REPLICA_PILOT_MAX_ENCODED_CHECKPOINT_BYTES
        );
        let completed = completed_stages().completed;
        let terminal = terminal_from_operation(
            &completed,
            RemoveReplicaActivityAccounting {
                external_effect_count: 5,
                passive_observation_count: 10,
            },
        )
        .unwrap();
        assert!(
            serde_json::to_vec(&terminal).unwrap().len()
                <= REMOVE_REPLICA_PILOT_MAX_TERMINAL_BYTES as usize
        );
        assert_eq!(
            checkpoint_limits().max_activity_records(),
            REMOVE_REPLICA_PILOT_MAX_ACTIVITY_RECORDS
        );
    }
}
