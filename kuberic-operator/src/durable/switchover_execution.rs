//! Compact versioned durable contract for framework-native switchover.
//!
//! The workflow reuses the deterministic switchover operation as its
//! workflow input. Per-turn workflow and reconciliation behavior is added by
//! later phases; this module owns only identity, bounds, and Kubernetes
//! lifecycle policy.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicI64, Ordering},
    },
};

use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::ResourceExt;
use kuberic_core::driver::ReplicaHandle;
#[cfg(test)]
use kuberic_durable_execution::{
    ActivityCallError, CasOutcome, CheckpointError, CheckpointStore, DurableHost, HostEpoch,
    StorageRevision, StoreError, StoredCheckpoint,
};
use kuberic_durable_execution::{
    ActivityName, ActivityObservation, ActivityRecord, ActivitySequence, ActivitySpec,
    CheckpointEnvelope, CheckpointLimits, CheckpointPayload, DispatchPermit, DurableActivity,
    ExactBytes, ExecutionContract, ExecutionId, ExecutionSpec, InMemoryCheckpointStore,
    KubernetesCheckpointOwner, KubernetesCheckpointOwnerScope, KubernetesCheckpointStore,
    KubernetesCheckpointStoreOptions, LogicalActivityId, PreparedActivityError,
    PreparedActivityResolver, TerminalOutcome, Workflow, WorkflowContext, decode_activity_input,
    decode_activity_result, encode_activity_input, encode_activity_result,
};
use rand::random;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::crd::{
    DurableActionKind, DurableOperationPhase, DurableOperationStatus, KubericSet,
    PendingActionStatus, StablePartitionSnapshotStatus, SwitchoverAdmissionInputStatus,
    SwitchoverExecutionStatus,
};
use crate::{cluster_api::ClusterApi, reconciler::snapshot_with_observed_metadata};

pub use super::checkpoint_store::DurableCheckpointStore;
use super::checkpoint_store::{
    CheckpointMeasurementDecoder, DurableActivityAccounting, DurableActivityClass,
    MeasuredDurableCheckpointStore,
};
use super::effects::{
    DurableEffectPreparationError, LabelEffectCommand, ReplicaEffectCommand,
    SwitchoverEffectBridgeOutcome, bridge_switchover_runner_step, exact_label_command,
    prepare_replica_effect_command, resolve_switchover_quarantine,
    validate_switchover_replica_action_kind,
};
use super::runner::{
    DurableAdapterBoundary, DurableAdapterWait, DurableCheckpointDisposition,
    DurableOperationAdapter,
};
use super::workflow_host::{DurableOperatorHost, DurablePermitGuard, DurableWorkflowRuntime};
use super::{
    Decision, OperationObservations, decide, start_switchover,
    switchover::{
        is_legal_switchover_phase_transition, validate_switchover_operation,
        validate_switchover_terminal as validate_terminal,
    },
};

pub const SWITCHOVER_CONTRACT_VERSION: u32 = 3;
pub const SWITCHOVER_MAX_REPLICAS: usize = crate::crd::KUBERIC_MAX_REPLICAS as usize;
pub const SWITCHOVER_MAX_ACTIVITY_RECORDS: usize = 2 * SWITCHOVER_MAX_REPLICAS + 15;
pub const SWITCHOVER_MAX_TRANSITION_FUEL: usize = 64;
pub const SWITCHOVER_MAX_RUNNER_FUEL: usize = 32;
pub const SWITCHOVER_MAX_WORKFLOW_INPUT_BYTES: usize = 4_096;
pub const SWITCHOVER_MAX_ACTIVITY_INPUT_BYTES: usize = 8_192;
pub const SWITCHOVER_MAX_ACTIVITY_RESULT_BYTES: usize = 4_096;
pub const SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES: usize = 752 * 1_024;
pub const SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES: usize = 16 * 1_024;
pub const SWITCHOVER_MAX_TERMINAL_PAYLOAD_BYTES: u64 = 4_096;
pub const SWITCHOVER_MAX_ERROR_BYTES: usize = 512;

const SWITCHOVER_ACTIVITY_NAME: &str = "kuberic.switchover.native-boundary";
const SWITCHOVER_ACTIVITY_VERSION: u32 = 2;
pub type SwitchoverHost = DurableOperatorHost;
pub type SwitchoverExecution = SwitchoverExecutionStatus;

/// Process-local host cache. Checkpoints, rather than this cache, remain the
/// recovery authority; retaining hosts preserves monotonic attempt counters
/// within one process epoch.
pub struct DurableSwitchoverRuntime {
    inner: Arc<DurableWorkflowRuntime>,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TestSwitchoverReference {
    pub(crate) version: u32,
    pub(crate) execution_id: String,
    pub(crate) checkpoint_name: String,
    pub(crate) initial_operation_json: String,
}

impl DurableSwitchoverRuntime {
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

    #[cfg(test)]
    pub(crate) async fn host(
        &self,
        namespace: &str,
        set_name: &str,
        set_uid: &str,
        reference: &TestSwitchoverReference,
    ) -> Result<Arc<Mutex<SwitchoverHost>>, String> {
        let execution_id = test_execution_id(reference)?;
        self.inner
            .host(
                namespace,
                set_name,
                set_uid,
                "switchover",
                execution_id,
                &reference.execution_id,
                &reference.checkpoint_name,
                checkpoint_store_options(namespace, set_name, set_uid)?,
                checkpoint_limits(),
                checkpoint_measurement_decoder(),
            )
            .await
    }

    pub async fn native_host(
        &self,
        namespace: &str,
        set_name: &str,
        set_uid: &str,
        reference: &SwitchoverExecutionStatus,
    ) -> Result<Arc<Mutex<SwitchoverHost>>, String> {
        let execution_id = native_execution_id(reference)?;
        self.inner
            .host(
                namespace,
                set_name,
                set_uid,
                "switchover",
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
            .forget(namespace, set_name, set_uid, "switchover", execution_id)
            .await;
    }

    pub async fn host_count(&self) -> usize {
        self.inner.host_count().await
    }

    pub async fn measurements(
        &self,
        namespace: &str,
        set_name: &str,
        set_uid: &str,
        execution_id: &str,
    ) -> Option<super::checkpoint_store::DurableCheckpointMeasurementsSnapshot> {
        self.inner
            .measurements(namespace, set_name, set_uid, "switchover", execution_id)
            .await
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwitchoverWorkflowInput {
    pub version: u32,
    pub execution_id: String,
    pub initial_operation: DurableOperationStatus,
}

/// Compact mutable protocol projection replayed over the immutable initial
/// operation in the workflow input.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DurableSwitchoverState {
    pub phase: DurableOperationPhase,
    pub frozen_lsn: Option<i64>,
    pub next_secondary_index: u32,
    pub phase_deadline_unix_seconds: i64,
    pub pending_action: Option<PendingActionStatus>,
    pub last_error: Option<String>,
}

impl DurableSwitchoverState {
    pub fn from_operation(operation: &DurableOperationStatus) -> Self {
        Self {
            phase: operation.phase,
            frozen_lsn: operation.frozen_lsn,
            next_secondary_index: operation.next_secondary_index,
            phase_deadline_unix_seconds: operation.phase_deadline_unix_seconds,
            pending_action: operation.pending_action.clone(),
            last_error: operation.last_error.clone(),
        }
    }

    pub fn apply_to(
        &self,
        initial: &DurableOperationStatus,
    ) -> Result<DurableOperationStatus, String> {
        let mut operation = initial.clone();
        operation.phase = self.phase;
        operation.frozen_lsn = self.frozen_lsn;
        operation.next_secondary_index = self.next_secondary_index;
        operation.phase_deadline_unix_seconds = self.phase_deadline_unix_seconds;
        operation.pending_action = self.pending_action.clone();
        operation.last_error = self.last_error.clone();
        validate_transition(initial, &operation)?;
        Ok(operation)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DurableSwitchoverActivityInput {
    pub version: u32,
    pub state: DurableSwitchoverState,
    pub kind: SwitchoverActivityKind,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SwitchoverActivityKind {
    PassiveObservation,
    PreparedReplica { command: ReplicaEffectCommand },
    PreparedLabel { command: LabelEffectCommand },
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwitchoverActivityAccounting {
    pub external_effect_count: u64,
    pub passive_observation_count: u64,
}

impl SwitchoverActivityAccounting {
    pub const fn new(external_effect_count: u64, passive_observation_count: u64) -> Self {
        Self {
            external_effect_count,
            passive_observation_count,
        }
    }

    pub fn matches_completed_activity_count(self, completed_activity_count: u64) -> bool {
        self.external_effect_count
            .checked_add(self.passive_observation_count)
            == Some(completed_activity_count)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum DurableSwitchoverStepResult {
    Advance {
        operation: DurableSwitchoverState,
    },
    ProvenNoAdmission {
        operation: DurableSwitchoverState,
        action_id: String,
        redelivery: u8,
    },
    Complete {
        operation: DurableSwitchoverState,
        snapshot: StablePartitionSnapshotStatus,
        compensated: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        accounting: Option<SwitchoverActivityAccounting>,
    },
    Stopped {
        operation: DurableSwitchoverState,
        message: String,
    },
}

pub struct DurableSwitchoverActivity;

impl DurableActivity for DurableSwitchoverActivity {
    type Input = DurableSwitchoverActivityInput;
    type Output = DurableSwitchoverStepResult;

    const NAME: &'static str = SWITCHOVER_ACTIVITY_NAME;
    const VERSION: u32 = SWITCHOVER_ACTIVITY_VERSION;
    const MAX_INPUT_BYTES: u64 = SWITCHOVER_MAX_ACTIVITY_INPUT_BYTES as u64;
    const MAX_RESULT_BYTES: u64 = SWITCHOVER_MAX_ACTIVITY_RESULT_BYTES as u64;
}

#[derive(Clone, Debug, PartialEq)]
pub enum SwitchoverTerminal {
    Complete {
        operation: DurableOperationStatus,
        snapshot: StablePartitionSnapshotStatus,
        compensated: bool,
        accounting: SwitchoverActivityAccounting,
    },
    Stopped {
        operation: Option<DurableOperationStatus>,
        message: String,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum DurableSwitchoverTerminalRecord {
    Complete {
        state: DurableSwitchoverState,
        snapshot: StablePartitionSnapshotStatus,
        compensated: bool,
        #[serde(default)]
        accounting: SwitchoverActivityAccounting,
    },
    Stopped {
        state: Option<DurableSwitchoverState>,
        message: String,
    },
}

#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum DurableSwitchoverTerminalAccountingRecord {
    Complete {
        state: Box<DurableSwitchoverState>,
        snapshot: StablePartitionSnapshotStatus,
        #[serde(default)]
        compensated: bool,
        #[serde(default)]
        accounting: Option<SwitchoverActivityAccounting>,
    },
    Stopped {},
}

pub struct DurableSwitchoverWorkflow;

#[async_trait]
impl Workflow for DurableSwitchoverWorkflow {
    async fn run(&self, context: &mut WorkflowContext<'_>, input: ExactBytes) -> TerminalOutcome {
        let input: SwitchoverWorkflowInput = match serde_json::from_slice(input.as_slice()) {
            Ok(input) => input,
            Err(error) => {
                return terminal_failure(
                    None,
                    format!("decode switchover workflow input: {error}"),
                );
            }
        };
        if input.version != SWITCHOVER_CONTRACT_VERSION {
            return terminal_failure(
                Some(input.initial_operation),
                format!("unsupported switchover workflow version {}", input.version),
            );
        }
        if input.execution_id != encode_execution_id(context.execution_id()) {
            return terminal_failure(
                Some(input.initial_operation),
                "switchover workflow execution identity mismatch".to_string(),
            );
        }
        let initial = input.initial_operation;
        if let Err(error) = validate_switchover_operation_contract(&initial) {
            return terminal_failure(Some(initial), error);
        }
        if let Err(error) = validate_switchover_operation(&initial) {
            return terminal_failure(Some(initial), error);
        }
        let mut state = DurableSwitchoverState::from_operation(&initial);
        let mut no_admission_redeliveries = std::collections::BTreeMap::<String, u8>::new();

        for _ in 0..SWITCHOVER_MAX_TRANSITION_FUEL {
            let operation = match state.apply_to(&initial) {
                Ok(operation) => operation,
                Err(error) => return terminal_failure(None, error),
            };
            match advance_effect_free(&operation) {
                Ok(Some(next)) => {
                    if let Err(error) = validate_phase_transition(&operation, &next) {
                        return terminal_failure(Some(operation), error);
                    }
                    if let Err(error) = validate_cursor_transition(&operation, &next) {
                        return terminal_failure(Some(operation), error);
                    }
                    if next.phase == DurableOperationPhase::Poisoned {
                        return terminal_failure(
                            Some(next.clone()),
                            next.last_error.clone().unwrap_or_else(|| {
                                "durable switchover entered poisoned state".to_string()
                            }),
                        );
                    }
                    state = DurableSwitchoverState::from_operation(&next);
                    continue;
                }
                Ok(None) => {}
                Err(error) => return terminal_failure(Some(operation), error),
            }
            let result = match context
                .call::<DurableSwitchoverActivity>(DurableSwitchoverActivityInput {
                    version: SWITCHOVER_CONTRACT_VERSION,
                    state: state.clone(),
                    kind: SwitchoverActivityKind::PassiveObservation,
                })
                .await
            {
                Ok(result) => result,
                Err(error) => {
                    return terminal_failure(
                        Some(operation),
                        format!("decode switchover activity result: {error}"),
                    );
                }
            };
            match result {
                DurableSwitchoverStepResult::Advance {
                    operation: next_state,
                } => {
                    if next_state == state {
                        return terminal_failure(
                            Some(operation),
                            "durable switchover activity made no protocol progress".to_string(),
                        );
                    }
                    let next = match next_state.apply_to(&initial) {
                        Ok(next) => next,
                        Err(error) => return terminal_failure(Some(operation), error),
                    };
                    if let Err(error) = validate_phase_transition(&operation, &next) {
                        return terminal_failure(Some(operation), error);
                    }
                    if let Err(error) = validate_cursor_transition(&operation, &next) {
                        return terminal_failure(Some(operation), error);
                    }
                    state = next_state;
                }
                DurableSwitchoverStepResult::ProvenNoAdmission {
                    operation: next_state,
                    action_id,
                    redelivery,
                } => {
                    let next = match next_state.apply_to(&initial) {
                        Ok(next) => next,
                        Err(error) => return terminal_failure(Some(operation), error),
                    };
                    if let Err(error) =
                        validate_no_admission_transition(&operation, &next, &action_id)
                    {
                        return terminal_failure(Some(operation), error);
                    }
                    let expected_action_id = operation
                        .pending_action
                        .as_ref()
                        .map(|pending| pending.action_id.as_str());
                    if expected_action_id != Some(action_id.as_str()) || redelivery != 1 {
                        return terminal_failure(
                            Some(operation),
                            "invalid proven-no-admission redelivery evidence".to_string(),
                        );
                    }
                    let count = no_admission_redeliveries.entry(action_id).or_default();
                    *count = count.saturating_add(1);
                    if *count > 1 {
                        return terminal_failure(
                            Some(operation),
                            "correlated action exceeded one proven-no-admission redelivery"
                                .to_string(),
                        );
                    }
                    state = next_state;
                }
                DurableSwitchoverStepResult::Complete {
                    operation: completed_state,
                    snapshot,
                    compensated,
                    accounting,
                } => {
                    let completed = match completed_state.apply_to(&initial) {
                        Ok(completed) => completed,
                        Err(error) => return terminal_failure(Some(operation), error),
                    };
                    if let Err(error) =
                        validate_completion_transition(&operation, &completed, compensated)
                    {
                        return terminal_failure(Some(operation), error);
                    }
                    if let Err(error) = validate_terminal(&completed, &snapshot, compensated) {
                        return terminal_failure(Some(operation), error);
                    }
                    let Some(accounting) = accounting else {
                        return terminal_failure(
                            Some(operation),
                            "switchover completion is missing authoritative activity accounting"
                                .to_string(),
                        );
                    };
                    let Some(completed_activity_count) = accounting
                        .external_effect_count
                        .checked_add(accounting.passive_observation_count)
                    else {
                        return terminal_failure(
                            Some(operation),
                            "switchover terminal activity accounting overflowed".to_string(),
                        );
                    };
                    if let Err(error) = validate_terminal_activity_accounting(
                        &initial,
                        &completed,
                        compensated,
                        accounting,
                        completed_activity_count,
                    ) {
                        return terminal_failure(Some(operation), error);
                    }
                    return terminal_success(SwitchoverTerminal::Complete {
                        operation: completed,
                        snapshot,
                        compensated,
                        accounting,
                    });
                }
                DurableSwitchoverStepResult::Stopped {
                    operation: stopped_state,
                    message,
                } => {
                    let stopped = match stopped_state.apply_to(&initial) {
                        Ok(stopped) => stopped,
                        Err(error) => return terminal_failure(Some(operation), error),
                    };
                    if stopped.phase != crate::crd::DurableOperationPhase::Poisoned
                        || stopped.pending_action.is_some()
                    {
                        return terminal_failure(
                            Some(operation),
                            "stopped durable switchover is not poisoned and effect-free"
                                .to_string(),
                        );
                    }
                    return terminal_failure(Some(stopped), message);
                }
            }
        }

        terminal_failure(
            state.apply_to(&initial).ok(),
            format!(
                "durable switchover exhausted its {SWITCHOVER_MAX_TRANSITION_FUEL}-transition fuel"
            ),
        )
    }
}

fn advance_effect_free(
    operation: &DurableOperationStatus,
) -> Result<Option<DurableOperationStatus>, String> {
    if operation.pending_action.is_some() {
        return Ok(None);
    }
    use DurableOperationPhase as Phase;
    if !matches!(
        operation.phase,
        Phase::Revoke
            | Phase::DemoteOldPrimary
            | Phase::PromoteTarget
            | Phase::DistributeEpoch
            | Phase::UpdateCatchUpConfiguration
            | Phase::WaitForCatchUpQuorum
            | Phase::UpdateCurrentConfiguration
            | Phase::LabelTargetPrimary
            | Phase::LabelOldSecondary
            | Phase::RestorePreviousConfiguration
            | Phase::CompensatePromoteOldPrimary
            | Phase::CompensateDistributeEpoch
            | Phase::CompensateCatchUpConfiguration
            | Phase::CompensateCurrentConfiguration
            | Phase::CompensateLabelOldPrimary
            | Phase::CompensateLabelTargetSecondary
    ) {
        return Ok(None);
    }
    let deterministic_now = operation
        .phase_deadline_unix_seconds
        .saturating_sub(super::ACTION_DEADLINE_SECONDS);
    match decide(operation, &OperationObservations::new(), deterministic_now)? {
        Decision::Persist(next) => Ok(Some(next)),
        Decision::Wait => Ok(None),
        other => Err(format!(
            "effect-free switchover transition produced external decision: {other:?}"
        )),
    }
}

pub fn encode_step_result(result: &DurableSwitchoverStepResult) -> Result<ExactBytes, String> {
    encode_activity_result::<DurableSwitchoverActivity>(result)
        .map_err(|error| format!("serialize switchover activity result: {error}"))
}

pub fn decode_activity_input_state(input: &ExactBytes) -> Result<DurableSwitchoverState, String> {
    Ok(decode_switchover_activity_input(input)?.state)
}

pub fn decode_switchover_activity_input(
    input: &ExactBytes,
) -> Result<DurableSwitchoverActivityInput, String> {
    let input = decode_activity_input::<DurableSwitchoverActivity>(input)
        .map_err(|error| format!("decode current durable switchover activity: {error}"))?;
    if input.version != SWITCHOVER_CONTRACT_VERSION {
        return Err(format!(
            "unsupported durable switchover activity version {}",
            input.version
        ));
    }
    Ok(input)
}

fn classify_checkpoint_activity(input: &ExactBytes) -> Option<DurableActivityClass> {
    let input = decode_switchover_activity_input(input).ok()?;
    Some(match input.kind {
        SwitchoverActivityKind::PassiveObservation => DurableActivityClass::PassiveObservation,
        SwitchoverActivityKind::PreparedReplica { .. }
        | SwitchoverActivityKind::PreparedLabel { .. } => DurableActivityClass::ExternalEffect,
    })
}

fn decode_checkpoint_terminal_accounting(
    outcome: &TerminalOutcome,
    completed_activity_count: u64,
) -> Option<DurableActivityAccounting> {
    decode_terminal_activity_accounting(outcome, completed_activity_count)
        .ok()
        .flatten()
        .map(|accounting| DurableActivityAccounting {
            external_effect_count: accounting.external_effect_count,
            passive_observation_count: accounting.passive_observation_count,
        })
}

pub fn checkpoint_measurement_decoder() -> CheckpointMeasurementDecoder {
    CheckpointMeasurementDecoder::new(
        "switchover",
        classify_checkpoint_activity,
        decode_checkpoint_terminal_accounting,
    )
}

pub fn validate_prepared_activity(
    operation: &DurableOperationStatus,
    kind: &SwitchoverActivityKind,
) -> Result<(), String> {
    match kind {
        SwitchoverActivityKind::PassiveObservation => Ok(()),
        SwitchoverActivityKind::PreparedReplica { command } => {
            let pending = operation
                .pending_action
                .as_ref()
                .ok_or_else(|| "prepared replica activity has no pending action".to_string())?;
            let expected = ReplicaEffectCommand::from_pending(pending)?;
            if &expected != command {
                return Err(
                    "prepared replica activity differs from frozen pending action".to_string(),
                );
            }
            let action = kuberic_core::grpc::convert::decode_direct_correlated_action_payload(
                &command.action_payload,
            )
            .map_err(|error| format!("decode prepared replica activity: {error}"))?;
            let signature_matches = action.signature() == command.action_signature;
            let kind_matches = validate_switchover_replica_action_kind(pending.kind, &action);
            let fixed_semantics_match =
                action_matches_fixed_operation_semantics(operation, pending, &action);
            if !signature_matches || !kind_matches || !fixed_semantics_match {
                return Err(format!(
                    "prepared replica activity has invalid action identity \
                     (kind={:?}, signature_matches={signature_matches}, \
                     kind_matches={kind_matches}, \
                     fixed_semantics_match={fixed_semantics_match})",
                    pending.kind
                ));
            }
            Ok(())
        }
        SwitchoverActivityKind::PreparedLabel { command } => {
            let pending = operation
                .pending_action
                .as_ref()
                .ok_or_else(|| "prepared label activity has no pending action".to_string())?;
            let expected_uid = operation
                .previous_snapshot
                .members
                .iter()
                .chain(operation.target_snapshot.members.iter())
                .find(|member| member.id == command.target_id)
                .map(|member| member.instance_id.as_str());
            let expected_role = match pending.kind {
                crate::crd::DurableActionKind::LabelTargetPrimary
                | crate::crd::DurableActionKind::CompensateLabelOldPrimary => Some("primary"),
                crate::crd::DurableActionKind::LabelOldSecondary
                | crate::crd::DurableActionKind::CompensateLabelTargetSecondary => {
                    Some("secondary")
                }
                _ => None,
            };
            if command.target_id != pending.target_id
                || command.expected_uid != pending.target_instance_id
                || expected_uid != Some(command.expected_uid.as_str())
                || command.pod_name.is_empty()
                || expected_role != Some(command.role.as_str())
                || !command.has_valid_identity_signature()
            {
                return Err("prepared label activity has invalid UID-fenced identity".to_string());
            }
            Ok(())
        }
    }
}

fn action_matches_fixed_operation_semantics(
    operation: &DurableOperationStatus,
    pending: &PendingActionStatus,
    action: &kuberic_core::types::DurableReplicaAction,
) -> bool {
    use kuberic_core::types::{DurableReplicaAction, Role};
    let target_epoch = &operation.target_snapshot.epoch;
    let has_target_epoch = |epoch: &kuberic_core::types::Epoch| {
        epoch.data_loss_number == target_epoch.data_loss_number
            && epoch.configuration_number == target_epoch.configuration_number
    };
    match action {
        DurableReplicaAction::ChangeRole { epoch, role } => {
            let expected_role =
                pending
                    .desired_postcondition
                    .role
                    .as_ref()
                    .map(|role| match role {
                        crate::crd::StableReplicaRoleStatus::Primary => Role::Primary,
                        crate::crd::StableReplicaRoleStatus::ActiveSecondary => {
                            Role::ActiveSecondary
                        }
                    });
            has_target_epoch(epoch) && expected_role.as_ref() == Some(role)
        }
        DurableReplicaAction::UpdateEpoch { epoch } => has_target_epoch(epoch),
        _ => true,
    }
}

pub fn decode_activity_step_result(
    result: &ExactBytes,
) -> Result<DurableSwitchoverStepResult, String> {
    decode_activity_result::<DurableSwitchoverActivity>(result)
        .map_err(|error| format!("decode completed durable switchover activity: {error}"))
}

pub fn decode_terminal(
    outcome: &TerminalOutcome,
    initial: &DurableOperationStatus,
) -> Result<SwitchoverTerminal, String> {
    let record: DurableSwitchoverTerminalRecord =
        serde_json::from_slice(outcome.payload().as_slice())
            .map_err(|error| format!("decode durable switchover terminal outcome: {error}"))?;
    Ok(match record {
        DurableSwitchoverTerminalRecord::Complete {
            state,
            snapshot,
            compensated,
            accounting,
        } => SwitchoverTerminal::Complete {
            operation: state.apply_to(initial)?,
            snapshot,
            compensated,
            accounting,
        },
        DurableSwitchoverTerminalRecord::Stopped { state, message } => {
            SwitchoverTerminal::Stopped {
                operation: state.map(|state| state.apply_to(initial)).transpose()?,
                message,
            }
        }
    })
}

pub fn decode_terminal_activity_accounting(
    outcome: &TerminalOutcome,
    completed_activity_count: u64,
) -> Result<Option<SwitchoverActivityAccounting>, String> {
    let record: DurableSwitchoverTerminalAccountingRecord =
        serde_json::from_slice(outcome.payload().as_slice()).map_err(|error| {
            format!("decode durable switchover terminal activity accounting: {error}")
        })?;
    Ok(match (outcome, record) {
        (
            TerminalOutcome::Succeeded(_),
            DurableSwitchoverTerminalAccountingRecord::Complete {
                state,
                snapshot,
                compensated,
                accounting: Some(accounting),
            },
        ) if validate_terminal_accounting_shape(
            TerminalAccountingContext::from_state(&state),
            snapshot.members.len(),
            compensated,
            accounting,
            completed_activity_count,
        )
        .is_ok() =>
        {
            Some(accounting)
        }
        _ => None,
    })
}

#[cfg(test)]
fn validate_test_loaded_terminal(
    reference: &TestSwitchoverReference,
    outcome: &TerminalOutcome,
    completed_activity_count: u64,
) -> Result<SwitchoverTerminal, String> {
    let initial = test_initial_operation(reference)?;
    validate_loaded_terminal_for_initial(&initial, outcome, completed_activity_count)
}

pub fn validate_loaded_terminal_for_initial(
    initial: &DurableOperationStatus,
    outcome: &TerminalOutcome,
    completed_activity_count: u64,
) -> Result<SwitchoverTerminal, String> {
    validate_switchover_admission(initial)?;
    let terminal = decode_terminal(outcome, initial)?;
    match (outcome, &terminal) {
        (
            TerminalOutcome::Succeeded(_),
            SwitchoverTerminal::Complete {
                operation,
                snapshot,
                compensated,
                accounting,
            },
        ) => {
            validate_transition(initial, operation)?;
            validate_terminal(operation, snapshot, *compensated)?;
            validate_terminal_activity_accounting(
                initial,
                operation,
                *compensated,
                *accounting,
                completed_activity_count,
            )?;
        }
        (
            TerminalOutcome::Failed(_),
            SwitchoverTerminal::Stopped {
                operation: Some(operation),
                ..
            },
        ) => {
            validate_transition(initial, operation)?;
        }
        (TerminalOutcome::Failed(_), SwitchoverTerminal::Stopped { .. }) => {}
        _ => {
            return Err(
                "kernel terminal outcome kind does not match durable switchover payload"
                    .to_string(),
            );
        }
    }
    Ok(terminal)
}

pub enum SwitchoverAdapterDecision {
    Observe(Box<DurableSwitchoverStepResult>),
    AwaitEvidence,
    External(Box<Decision>),
}

pub fn evaluate_adapter_step(
    operation: &DurableOperationStatus,
    observations: &OperationObservations,
    now: i64,
) -> Result<SwitchoverAdapterDecision, String> {
    Ok(match decide(operation, observations, now)? {
        Decision::Persist(operation)
            if operation.phase == crate::crd::DurableOperationPhase::Poisoned =>
        {
            let message = operation
                .last_error
                .clone()
                .unwrap_or_else(|| "durable switchover entered poisoned state".to_string());
            SwitchoverAdapterDecision::Observe(Box::new(DurableSwitchoverStepResult::Stopped {
                operation: DurableSwitchoverState::from_operation(&operation),
                message,
            }))
        }
        Decision::Persist(operation) => {
            SwitchoverAdapterDecision::Observe(Box::new(DurableSwitchoverStepResult::Advance {
                operation: DurableSwitchoverState::from_operation(&operation),
            }))
        }
        Decision::Complete {
            operation,
            snapshot,
            compensated,
        } => terminal_adapter_decision(operation, snapshot, compensated),
        Decision::Wait => SwitchoverAdapterDecision::AwaitEvidence,
        external @ (Decision::Execute { .. }
        | Decision::PatchPodRole { .. }
        | Decision::PatchPodRoleExactUid { .. }) => {
            SwitchoverAdapterDecision::External(Box::new(external))
        }
        other => {
            return Err(format!(
                "unsupported switchover decision reached the durable adapter: {other:?}"
            ));
        }
    })
}

fn terminal_adapter_decision(
    operation: DurableOperationStatus,
    snapshot: StablePartitionSnapshotStatus,
    compensated: bool,
) -> SwitchoverAdapterDecision {
    let terminal_state = DurableSwitchoverState::from_operation(&operation);
    if compensated
        && projected_compensation_transcripts(
            TerminalAccountingContext::from_state(&terminal_state),
            operation.previous_snapshot.members.len(),
        )
        .is_empty()
    {
        let message = operation.last_error.clone().unwrap_or_else(|| {
            "durable switchover failed before a compensation path completed".to_string()
        });
        let mut stopped = operation;
        stopped.phase = DurableOperationPhase::Poisoned;
        stopped.pending_action = None;
        stopped.last_error = Some(message.clone());
        SwitchoverAdapterDecision::Observe(Box::new(DurableSwitchoverStepResult::Stopped {
            operation: DurableSwitchoverState::from_operation(&stopped),
            message,
        }))
    } else {
        SwitchoverAdapterDecision::Observe(Box::new(DurableSwitchoverStepResult::Complete {
            operation: terminal_state,
            snapshot,
            compensated,
            accounting: None,
        }))
    }
}

pub struct SwitchoverPreparedActivityResolver {
    initial: DurableOperationStatus,
    observations: OperationObservations,
    addressed_instances: BTreeMap<i64, kuberic_core::types::ReplicaInstanceId>,
    now: i64,
    deadline: Option<Arc<AtomicI64>>,
}

impl SwitchoverPreparedActivityResolver {
    pub fn new(
        initial: &DurableOperationStatus,
        observations: &OperationObservations,
        addressed_instances: &BTreeMap<i64, kuberic_core::types::ReplicaInstanceId>,
        now: i64,
    ) -> Self {
        Self {
            initial: initial.clone(),
            observations: observations.clone(),
            addressed_instances: addressed_instances.clone(),
            now,
            deadline: None,
        }
    }

    fn with_deadline(
        initial: &DurableOperationStatus,
        observations: &OperationObservations,
        addressed_instances: &BTreeMap<i64, kuberic_core::types::ReplicaInstanceId>,
        now: i64,
        deadline: Arc<AtomicI64>,
    ) -> Self {
        Self {
            initial: initial.clone(),
            observations: observations.clone(),
            addressed_instances: addressed_instances.clone(),
            now,
            deadline: Some(deadline),
        }
    }

    fn record_deadline(&self, operation: &DurableOperationStatus) {
        if let Some(deadline) = &self.deadline {
            deadline.store(
                switchover_deadline_unix_seconds(operation),
                Ordering::Relaxed,
            );
        }
    }

    fn prepare(
        &self,
        logical: &DurableSwitchoverActivityInput,
    ) -> Result<DurableSwitchoverActivityInput, PreparedActivityError> {
        if logical.version != SWITCHOVER_CONTRACT_VERSION
            || logical.kind != SwitchoverActivityKind::PassiveObservation
        {
            return Err(PreparedActivityError::Validation);
        }
        let operation = logical
            .state
            .apply_to(&self.initial)
            .map_err(|_| PreparedActivityError::Validation)?;
        self.record_deadline(&operation);
        let decision = evaluate_adapter_step(&operation, &self.observations, self.now)
            .map_err(|_| PreparedActivityError::Derivation)?;
        let mut prepared = logical.clone();
        match decision {
            SwitchoverAdapterDecision::Observe(_) => {}
            SwitchoverAdapterDecision::AwaitEvidence => {
                if operation.pending_action.is_some() {
                    return Err(PreparedActivityError::Derivation);
                }
            }
            SwitchoverAdapterDecision::External(decision) => match *decision {
                Decision::Execute {
                    target_id,
                    action_id,
                    action,
                } => {
                    let pending = operation
                        .pending_action
                        .as_ref()
                        .ok_or(PreparedActivityError::Validation)?;
                    if pending.action_id != action_id || pending.target_id != target_id {
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
                    let (planned, command) = prepare_replica_effect_command(
                        pending,
                        &observed.status,
                        addressed,
                        &action,
                    )
                    .map_err(preparation_error)?;
                    let mut operation = operation;
                    operation.pending_action = Some(planned);
                    prepared.state = DurableSwitchoverState::from_operation(&operation);
                    prepared.kind = SwitchoverActivityKind::PreparedReplica { command };
                }
                Decision::PatchPodRole { target_id, role } => {
                    let command =
                        exact_label_command(&operation, target_id, &role, &self.observations)
                            .map_err(|_| PreparedActivityError::Derivation)?;
                    prepared.kind = SwitchoverActivityKind::PreparedLabel { command };
                }
                Decision::PatchPodRoleExactUid {
                    target_id,
                    expected_uid,
                    role,
                } => {
                    let command =
                        exact_label_command(&operation, target_id, &role, &self.observations)
                            .map_err(|_| PreparedActivityError::Derivation)?;
                    if command.expected_uid != expected_uid {
                        return Err(PreparedActivityError::Validation);
                    }
                    prepared.kind = SwitchoverActivityKind::PreparedLabel { command };
                }
                _ => return Err(PreparedActivityError::Validation),
            },
        }
        Ok(prepared)
    }

    fn validate_recorded(
        &self,
        logical: &DurableSwitchoverActivityInput,
        recorded: &DurableSwitchoverActivityInput,
    ) -> Result<(), PreparedActivityError> {
        if logical.version != SWITCHOVER_CONTRACT_VERSION
            || recorded.version != SWITCHOVER_CONTRACT_VERSION
            || logical.kind != SwitchoverActivityKind::PassiveObservation
        {
            return Err(PreparedActivityError::Validation);
        }
        let mut logical_predecessor = recorded.state.clone();
        if let Some(pending) = logical_predecessor.pending_action.as_mut() {
            pending.dispatch_agent_generation = None;
            pending.dispatch_agent_control_version = None;
            pending.dispatch_observed_runtime_epoch = None;
            pending.dispatch_action_payload.clear();
        }
        if recorded.state != logical.state && logical_predecessor != logical.state {
            return Err(PreparedActivityError::Validation);
        }
        let operation = recorded
            .state
            .apply_to(&self.initial)
            .map_err(|_| PreparedActivityError::Validation)?;
        self.record_deadline(&operation);
        if matches!(recorded.kind, SwitchoverActivityKind::PassiveObservation)
            && recorded.state != logical.state
        {
            return Err(PreparedActivityError::Validation);
        }
        validate_prepared_activity(&operation, &recorded.kind)
            .map_err(|_| PreparedActivityError::Validation)
    }
}

impl PreparedActivityResolver for SwitchoverPreparedActivityResolver {
    fn resolve(
        &self,
        logical: &ActivitySpec,
        recorded: Option<&ActivitySpec>,
    ) -> Result<ActivitySpec, PreparedActivityError> {
        let logical_input = decode_activity_input::<DurableSwitchoverActivity>(logical.input())
            .map_err(|_| PreparedActivityError::Encoding)?;
        if let Some(recorded) = recorded {
            if recorded.name() != logical.name()
                || recorded.max_result_bytes() != logical.max_result_bytes()
            {
                return Ok(logical.clone());
            }
            let Ok(recorded_input) =
                decode_activity_input::<DurableSwitchoverActivity>(recorded.input())
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
        let spec = activity_spec(&prepared).map_err(|_| PreparedActivityError::Encoding)?;
        let actual_bytes = u64::try_from(spec.input().as_slice().len())
            .map_err(|_| PreparedActivityError::Encoding)?;
        if actual_bytes > DurableSwitchoverActivity::MAX_INPUT_BYTES {
            return Err(PreparedActivityError::InputTooLarge {
                actual_bytes,
                max_bytes: DurableSwitchoverActivity::MAX_INPUT_BYTES,
            });
        }
        Ok(spec)
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

pub struct SwitchoverRunnerContext {
    pub observations: OperationObservations,
    pub handles: BTreeMap<i64, Box<dyn ReplicaHandle>>,
    pub addressed_instances: BTreeMap<i64, kuberic_core::types::ReplicaInstanceId>,
}

pub async fn collect_switchover_runner_context(
    initial: &DurableOperationStatus,
    set: &KubericSet,
    api: &dyn ClusterApi,
    current_pods: &[(i64, kuberic_core::types::ReplicaInstanceId, &Pod)],
) -> Result<SwitchoverRunnerContext, String> {
    for member in &initial.previous_snapshot.members {
        let Some((_, instance_id, _)) = current_pods.iter().find(|(id, _, _)| *id == member.id)
        else {
            return Err(format!(
                "durable switchover replica {} has no current pod",
                member.id
            ));
        };
        if instance_id.as_str() != member.instance_id {
            return Err(format!(
                "durable switchover replica {} incarnation changed",
                member.id
            ));
        }
    }

    let mut handles: BTreeMap<i64, Box<dyn ReplicaHandle>> = BTreeMap::new();
    let mut observations = OperationObservations::new();
    for (replica_id, _, pod) in current_pods {
        if !initial
            .target_snapshot
            .members
            .iter()
            .any(|member| member.id == *replica_id)
        {
            continue;
        }
        let Ok(handle) = api.create_replica_handle(*replica_id, pod, &set.spec).await else {
            continue;
        };
        if let Ok(status) = handle.get_status().await {
            observations.insert(
                *replica_id,
                super::ReplicaObservation {
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
        handles.insert(*replica_id, handle);
    }
    let addressed_instances = handles
        .iter()
        .map(|(replica_id, handle)| (*replica_id, handle.instance_id()))
        .collect();
    Ok(SwitchoverRunnerContext {
        observations,
        handles,
        addressed_instances,
    })
}

pub struct SwitchoverRunnerAdapter<'a> {
    initial: &'a DurableOperationStatus,
    set: &'a KubericSet,
    current_pods: &'a [(i64, kuberic_core::types::ReplicaInstanceId, &'a Pod)],
    resolver: SwitchoverPreparedActivityResolver,
    context: Option<SwitchoverRunnerContext>,
    api: &'a dyn ClusterApi,
    namespace: String,
    store: MeasuredDurableCheckpointStore,
    now: i64,
    deadline: Arc<AtomicI64>,
}

impl<'a> SwitchoverRunnerAdapter<'a> {
    pub fn new(
        initial: &'a DurableOperationStatus,
        set: &'a KubericSet,
        current_pods: &'a [(i64, kuberic_core::types::ReplicaInstanceId, &'a Pod)],
        api: &'a dyn ClusterApi,
        store: MeasuredDurableCheckpointStore,
        now: i64,
    ) -> Self {
        let deadline = Arc::new(AtomicI64::new(switchover_deadline_unix_seconds(initial)));
        let observations = OperationObservations::new();
        let addressed_instances = BTreeMap::new();
        Self {
            initial,
            set,
            current_pods,
            resolver: SwitchoverPreparedActivityResolver::with_deadline(
                initial,
                &observations,
                &addressed_instances,
                now,
                deadline.clone(),
            ),
            context: None,
            api,
            namespace: set.namespace().unwrap_or_default(),
            store,
            now,
            deadline,
        }
    }

    fn context(&self) -> Result<&SwitchoverRunnerContext, String> {
        self.context
            .as_ref()
            .ok_or_else(|| "durable switchover adapter was not prepared".to_string())
    }

    fn decode_operation(
        &self,
        activity: &LogicalActivityId,
    ) -> Result<(DurableOperationStatus, SwitchoverActivityKind), String> {
        let prepared = decode_switchover_activity_input(activity.input())?;
        let operation = prepared.state.apply_to(self.initial)?;
        validate_prepared_activity(&operation, &prepared.kind)?;
        self.deadline.store(
            switchover_deadline_unix_seconds(&operation),
            Ordering::Relaxed,
        );
        Ok((operation, prepared.kind))
    }

    fn observation(
        &self,
        activity: LogicalActivityId,
        result: DurableSwitchoverStepResult,
    ) -> Result<ActivityObservation, String> {
        let result = enrich_runner_result(
            result,
            &self.context()?.observations,
            self.store.measurements(),
        );
        Ok(ActivityObservation::new(
            activity,
            encode_step_result(&result)?,
        ))
    }

    fn bridge_boundary(
        &self,
        activity: LogicalActivityId,
        outcome: SwitchoverEffectBridgeOutcome,
        quarantine: bool,
    ) -> DurableAdapterBoundary {
        match outcome {
            SwitchoverEffectBridgeOutcome::Observe(result) => self
                .observation(activity, *result)
                .map(DurableAdapterBoundary::Observed)
                .unwrap_or_else(DurableAdapterBoundary::Isolated),
            SwitchoverEffectBridgeOutcome::ObserveAfterFenceRefresh(result) if !quarantine => self
                .observation(activity, *result)
                .map(|observation| DurableAdapterBoundary::ObserveAndWait {
                    observation: Box::new(observation),
                    reason: "RefreshingReplicaObservation".to_string(),
                    detail: "proven non-admission was persisted; retry awaits refreshed replica observations".to_string(),
                    requeue_after_seconds: 1,
                })
                .unwrap_or_else(DurableAdapterBoundary::Isolated),
            SwitchoverEffectBridgeOutcome::ObserveAfterFenceRefresh(result) => self
                .observation(activity, *result)
                .map(DurableAdapterBoundary::Observed)
                .unwrap_or_else(DurableAdapterBoundary::Isolated),
            SwitchoverEffectBridgeOutcome::Exposed if quarantine => DurableAdapterBoundary::Wait {
                reason: "Quarantined".to_string(),
                detail: "exposed switchover activity remains quarantined pending authoritative evidence"
                    .to_string(),
            },
            SwitchoverEffectBridgeOutcome::AwaitEvidence if quarantine => {
                DurableAdapterBoundary::Wait {
                    reason: "Quarantined".to_string(),
                    detail: "exposed switchover activity remains quarantined pending authoritative evidence"
                        .to_string(),
                }
            }
            SwitchoverEffectBridgeOutcome::Exposed => DurableAdapterBoundary::Wait {
                reason: "EffectExposed".to_string(),
                detail:
                    "correlated effect was exposed and awaits authoritative observation".to_string(),
            },
            SwitchoverEffectBridgeOutcome::AwaitEvidence => DurableAdapterBoundary::Wait {
                reason: "AwaitingReplicaObservation".to_string(),
                detail: "switchover activity is waiting for an exact replica or authoritative effect evidence"
                    .to_string(),
            },
        }
    }
}

#[async_trait]
impl DurableOperationAdapter for SwitchoverRunnerAdapter<'_> {
    type Resolver = SwitchoverPreparedActivityResolver;
    type Terminal = SwitchoverTerminal;
    type Publication = SwitchoverTerminal;

    fn resolver(&self) -> &Self::Resolver {
        &self.resolver
    }

    async fn prepare(&mut self) -> Result<(), DurableAdapterBoundary> {
        let context =
            collect_switchover_runner_context(self.initial, self.set, self.api, self.current_pods)
                .await
                .map_err(DurableAdapterBoundary::Isolated)?;
        self.resolver = SwitchoverPreparedActivityResolver::with_deadline(
            self.initial,
            &context.observations,
            &context.addressed_instances,
            self.now,
            self.deadline.clone(),
        );
        self.context = Some(context);
        Ok(())
    }

    async fn observe_or_dispatch(
        &mut self,
        activity: &LogicalActivityId,
        attempt_id: kuberic_durable_execution::AttemptId,
        permit: &mut DurablePermitGuard,
    ) -> DurableAdapterBoundary {
        let (operation, prepared) = match self.decode_operation(activity) {
            Ok(decoded) => decoded,
            Err(error) => return DurableAdapterBoundary::Isolated(error),
        };
        if prepared == SwitchoverActivityKind::PassiveObservation {
            if let Err(error) = permit.consume(activity.spec(), activity, attempt_id, "switchover")
            {
                return DurableAdapterBoundary::Isolated(error);
            }
            let observations = match self.context() {
                Ok(context) => &context.observations,
                Err(error) => return DurableAdapterBoundary::Isolated(error),
            };
            return match evaluate_adapter_step(&operation, observations, self.now) {
                Ok(SwitchoverAdapterDecision::Observe(result)) => self
                    .observation(activity.clone(), *result)
                    .map(DurableAdapterBoundary::Observed)
                    .unwrap_or_else(DurableAdapterBoundary::Isolated),
                Ok(SwitchoverAdapterDecision::AwaitEvidence) => DurableAdapterBoundary::Wait {
                    reason: "AwaitingReplicaObservation".to_string(),
                    detail: "switchover activity is waiting for authoritative observation"
                        .to_string(),
                },
                Ok(SwitchoverAdapterDecision::External(_)) => DurableAdapterBoundary::Isolated(
                    "switchover resolver exposed an external effect as a passive observation"
                        .to_string(),
                ),
                Err(error) => DurableAdapterBoundary::Isolated(error),
            };
        }
        let context = match self.context() {
            Ok(context) => context,
            Err(error) => return DurableAdapterBoundary::Isolated(error),
        };
        match bridge_switchover_runner_step(
            permit,
            &operation,
            &prepared,
            activity,
            attempt_id,
            &context.observations,
            &context.handles,
            self.api,
            &self.namespace,
        )
        .await
        {
            Ok(outcome) => self.bridge_boundary(activity.clone(), outcome, false),
            Err(error) => DurableAdapterBoundary::Isolated(error),
        }
    }

    async fn resolve_quarantine(
        &mut self,
        activity: LogicalActivityId,
        _attempt_id: kuberic_durable_execution::AttemptId,
    ) -> DurableAdapterBoundary {
        let (operation, prepared) = match self.decode_operation(&activity) {
            Ok(decoded) => decoded,
            Err(error) => return DurableAdapterBoundary::Isolated(error),
        };
        let observations = match self.context() {
            Ok(context) => &context.observations,
            Err(error) => return DurableAdapterBoundary::Isolated(error),
        };
        let decision = match evaluate_adapter_step(&operation, observations, self.now) {
            Ok(decision) => decision,
            Err(error) => return DurableAdapterBoundary::Isolated(error),
        };
        match resolve_switchover_quarantine(&operation, &prepared, decision, observations) {
            Ok(outcome) => self.bridge_boundary(activity, outcome, true),
            Err(error) => DurableAdapterBoundary::Isolated(error),
        }
    }

    fn deadline_unix_seconds(&self) -> i64 {
        self.deadline.load(Ordering::Relaxed)
    }

    fn preparation_wait(
        &self,
        _error: &kuberic_durable_execution::CheckpointError,
    ) -> DurableAdapterWait {
        DurableAdapterWait {
            reason: "AwaitingEffectPreparation".to_string(),
            detail:
                "switchover effect could not be prepared from the current authoritative observations"
                    .to_string(),
            requeue_after_seconds: Some(1),
        }
    }

    fn checkpoint_disposition(
        &self,
        error: &kuberic_durable_execution::CheckpointError,
    ) -> DurableCheckpointDisposition {
        if matches!(
            error,
            kuberic_durable_execution::CheckpointError::UnsupportedFormat { .. }
        ) {
            DurableCheckpointDisposition::Incompatible
        } else {
            DurableCheckpointDisposition::Rejected
        }
    }

    fn validate_terminal(
        &mut self,
        outcome: TerminalOutcome,
        completed_activity_count: u64,
    ) -> Result<Self::Terminal, DurableAdapterBoundary> {
        validate_loaded_terminal_for_initial(self.initial, &outcome, completed_activity_count)
            .map_err(DurableAdapterBoundary::Rejected)
    }

    fn publication_handoff(&mut self, terminal: Self::Terminal) -> Self::Publication {
        terminal
    }
}

fn enrich_runner_result(
    result: DurableSwitchoverStepResult,
    observations: &OperationObservations,
    measurements: super::checkpoint_store::DurableCheckpointMeasurementsSnapshot,
) -> DurableSwitchoverStepResult {
    match result {
        DurableSwitchoverStepResult::Complete {
            operation,
            snapshot,
            compensated,
            accounting,
        } => DurableSwitchoverStepResult::Complete {
            operation,
            snapshot: snapshot_with_observed_metadata(snapshot, observations),
            compensated,
            accounting: measurements
                .completed_external_effect_count
                .zip(measurements.completed_passive_observation_count)
                .map(|(external, passive)| SwitchoverActivityAccounting::new(external, passive))
                .or(accounting),
        },
        other => other,
    }
}

pub fn switchover_deadline_unix_seconds(operation: &DurableOperationStatus) -> i64 {
    operation
        .pending_action
        .as_ref()
        .map(|pending| pending.deadline_unix_seconds)
        .unwrap_or(operation.phase_deadline_unix_seconds)
}

pub struct SwitchoverPermitGuard {
    inner: DurablePermitGuard,
}

impl SwitchoverPermitGuard {
    pub fn new(permit: DispatchPermit) -> Self {
        Self {
            inner: DurablePermitGuard::new(permit),
        }
    }

    pub fn consume_for(
        &mut self,
        operation: &DurableOperationStatus,
        prepared: &SwitchoverActivityKind,
        expected_activity: &LogicalActivityId,
        attempt_id: kuberic_durable_execution::AttemptId,
    ) -> Result<DispatchPermit, String> {
        let expected = activity_spec(&DurableSwitchoverActivityInput {
            version: SWITCHOVER_CONTRACT_VERSION,
            state: DurableSwitchoverState::from_operation(operation),
            kind: prepared.clone(),
        })?;
        self.inner
            .consume(&expected, expected_activity, attempt_id, "switchover")
    }

    pub fn activity(&self) -> Option<&LogicalActivityId> {
        self.inner.activity()
    }
}

fn activity_spec(input: &DurableSwitchoverActivityInput) -> Result<ActivitySpec, String> {
    Ok(ActivitySpec::new(
        ActivityName::new(SWITCHOVER_ACTIVITY_NAME, SWITCHOVER_ACTIVITY_VERSION)
            .map_err(|error| format!("construct switchover activity name: {error}"))?,
        encode_activity_input::<DurableSwitchoverActivity>(input)
            .map_err(|error| format!("serialize switchover activity input: {error}"))?,
        SWITCHOVER_MAX_ACTIVITY_RESULT_BYTES as u64,
    ))
}

pub(crate) fn prepared_activity_spec(
    operation: &DurableOperationStatus,
    prepared: &SwitchoverActivityKind,
) -> Result<ActivitySpec, String> {
    activity_spec(&DurableSwitchoverActivityInput {
        version: SWITCHOVER_CONTRACT_VERSION,
        state: DurableSwitchoverState::from_operation(operation),
        kind: prepared.clone(),
    })
}

fn validate_transition(
    initial: &DurableOperationStatus,
    next: &DurableOperationStatus,
) -> Result<(), String> {
    if next.operation_id != initial.operation_id
        || next.execution_id != initial.execution_id
        || next.kind != initial.kind
        || next.previous_snapshot != initial.previous_snapshot
        || next.target_snapshot != initial.target_snapshot
        || next.old_primary_id != initial.old_primary_id
        || next.target_primary_id != initial.target_primary_id
    {
        return Err("durable switchover activity changed immutable operation identity".to_string());
    }
    validate_switchover_operation_contract(next)
}

fn validate_phase_transition(
    current: &DurableOperationStatus,
    next: &DurableOperationStatus,
) -> Result<(), String> {
    if is_legal_switchover_phase_transition(current.phase, next.phase) {
        Ok(())
    } else {
        Err(format!(
            "invalid durable switchover phase transition {:?} -> {:?}",
            current.phase, next.phase
        ))
    }
}

fn validate_cursor_transition(
    current: &DurableOperationStatus,
    next: &DurableOperationStatus,
) -> Result<(), String> {
    if current.phase != next.phase {
        let member_count = current
            .previous_snapshot
            .as_ref()
            .map_or(0, |snapshot| snapshot.members.len());
        let completed_distribution = match (current.phase, next.phase) {
            (
                DurableOperationPhase::DistributeEpoch,
                DurableOperationPhase::UpdateCatchUpConfiguration,
            ) => Some(member_count.saturating_sub(2)),
            (
                DurableOperationPhase::CompensateDistributeEpoch,
                DurableOperationPhase::CompensateCatchUpConfiguration,
            ) => Some(member_count.saturating_sub(1)),
            _ => None,
        };
        if let Some(expected) = completed_distribution {
            let current_cursor = usize::try_from(current.next_secondary_index).ok();
            let next_cursor = usize::try_from(next.next_secondary_index).ok();
            return if current_cursor == Some(expected) && next_cursor == Some(expected) {
                Ok(())
            } else {
                Err(format!(
                    "durable switchover advanced from {:?} before completing {expected} \
                     secondary epoch updates",
                    current.phase
                ))
            };
        }
        if matches!(
            (current.phase, next.phase),
            (
                DurableOperationPhase::PromoteTarget,
                DurableOperationPhase::DistributeEpoch
            ) | (
                DurableOperationPhase::CompensatePromoteOldPrimary,
                DurableOperationPhase::CompensateDistributeEpoch
            )
        ) {
            return if next.next_secondary_index == 0 {
                Ok(())
            } else {
                Err("durable switchover did not reset the secondary cursor".to_string())
            };
        }
        return if next.next_secondary_index == current.next_secondary_index {
            Ok(())
        } else {
            Err(format!(
                "invalid durable switchover secondary cursor transition {} -> {}",
                current.next_secondary_index, next.next_secondary_index
            ))
        };
    }
    if next.next_secondary_index == current.next_secondary_index {
        return Ok(());
    }
    let permitted = match current.pending_action.as_ref().map(|pending| pending.kind) {
        Some(DurableActionKind::UpdateSecondaryEpoch) => {
            current.phase == DurableOperationPhase::DistributeEpoch
                && next.phase == DurableOperationPhase::DistributeEpoch
                && current
                    .next_secondary_index
                    .checked_add(1)
                    .is_some_and(|expected| next.next_secondary_index == expected)
        }
        Some(DurableActionKind::CompensateUpdateSecondaryEpoch) => {
            current.phase == DurableOperationPhase::CompensateDistributeEpoch
                && next.phase == DurableOperationPhase::CompensateDistributeEpoch
                && current
                    .next_secondary_index
                    .checked_add(1)
                    .is_some_and(|expected| next.next_secondary_index == expected)
        }
        Some(DurableActionKind::PromoteTarget) => {
            next.phase == DurableOperationPhase::DistributeEpoch && next.next_secondary_index == 0
        }
        Some(DurableActionKind::CompensatePromoteOldPrimary) => {
            next.phase == DurableOperationPhase::CompensateDistributeEpoch
                && next.next_secondary_index == 0
        }
        _ => false,
    };
    if permitted {
        Ok(())
    } else {
        Err(format!(
            "invalid durable switchover secondary cursor transition {} -> {}",
            current.next_secondary_index, next.next_secondary_index
        ))
    }
}

fn validate_no_admission_transition(
    current: &DurableOperationStatus,
    next: &DurableOperationStatus,
    action_id: &str,
) -> Result<(), String> {
    if current.phase != next.phase {
        return Err("proven-no-admission changed switchover phase".to_string());
    }
    let Some(current_pending) = current.pending_action.as_ref() else {
        return Err("proven-no-admission has no current pending action".to_string());
    };
    let Some(next_pending) = next.pending_action.as_ref() else {
        return Err("proven-no-admission removed the pending action".to_string());
    };
    if current_pending.action_id != action_id || next_pending.action_id != action_id {
        return Err("proven-no-admission changed correlated action identity".to_string());
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
        return Err("proven-no-admission changed non-dispatch operation state".to_string());
    }
    Ok(())
}

fn validate_completion_transition(
    current: &DurableOperationStatus,
    completed: &DurableOperationStatus,
    compensated: bool,
) -> Result<(), String> {
    use crate::crd::DurableOperationPhase as Phase;
    let allowed = if compensated {
        matches!(
            (current.phase, completed.phase),
            (Phase::CompensateFinalize, Phase::Failed) | (Phase::Failed, Phase::Failed)
        )
    } else {
        (current.phase, completed.phase) == (Phase::Finalize, Phase::Completed)
    };
    if allowed {
        Ok(())
    } else {
        Err(format!(
            "invalid durable switchover completion transition {:?} -> {:?}",
            current.phase, completed.phase
        ))
    }
}

fn terminal_success(terminal: SwitchoverTerminal) -> TerminalOutcome {
    match encode_terminal(&terminal) {
        Ok(payload) => TerminalOutcome::succeeded(payload),
        Err(error) => terminal_failure(None, error),
    }
}

pub(crate) fn encode_terminal(terminal: &SwitchoverTerminal) -> Result<ExactBytes, String> {
    let record = match terminal {
        SwitchoverTerminal::Complete {
            operation,
            snapshot,
            compensated,
            accounting,
        } => DurableSwitchoverTerminalRecord::Complete {
            state: DurableSwitchoverState::from_operation(operation),
            snapshot: snapshot.clone(),
            compensated: *compensated,
            accounting: *accounting,
        },
        SwitchoverTerminal::Stopped { operation, message } => {
            DurableSwitchoverTerminalRecord::Stopped {
                state: operation
                    .as_ref()
                    .map(DurableSwitchoverState::from_operation),
                message: message.clone(),
            }
        }
    };
    encode_terminal_record(&record)
}

fn terminal_failure(operation: Option<DurableOperationStatus>, message: String) -> TerminalOutcome {
    let bounded_message = bounded_utf8(&message, SWITCHOVER_MAX_ERROR_BYTES);
    let terminal = DurableSwitchoverTerminalRecord::Stopped {
        state: operation
            .as_ref()
            .map(DurableSwitchoverState::from_operation),
        message: bounded_message,
    };
    let payload = encode_terminal_record(&terminal).unwrap_or_else(|_| {
        ExactBytes::new(br#"{"status":"stopped","state":null,"message":"switchover terminal payload exceeded its bound"}"#)
    });
    TerminalOutcome::failed(payload)
}

fn bounded_utf8(value: &str, maximum_bytes: usize) -> String {
    if value.len() <= maximum_bytes {
        return value.to_string();
    }
    let mut boundary = maximum_bytes;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value[..boundary].to_string()
}

fn encode_terminal_record(
    terminal: &DurableSwitchoverTerminalRecord,
) -> Result<ExactBytes, String> {
    let encoded = serde_json::to_vec(terminal)
        .map_err(|error| format!("serialize durable switchover terminal outcome: {error}"))?;
    validate_terminal_payload_bytes(encoded.len())?;
    Ok(ExactBytes::new(encoded))
}

#[cfg(test)]
pub(crate) fn new_test_reference(
    set_uid: &str,
    previous_snapshot: StablePartitionSnapshotStatus,
    target_primary_id: i64,
    now: i64,
) -> Result<TestSwitchoverReference, String> {
    if set_uid.is_empty() {
        return Err("switchover test reference requires the KubericSet UID".to_string());
    }
    let execution_id = ExecutionId::from_bytes(random());
    let execution_hex = encode_execution_id(execution_id);
    let operation_identity = format!("{set_uid}:test-contract:{execution_hex}");
    let initial_operation = start_switchover(
        &operation_identity,
        previous_snapshot,
        target_primary_id,
        now,
    )?;
    validate_switchover_admission(&initial_operation)?;
    let initial_operation_json = serde_json::to_string(&initial_operation)
        .map_err(|error| format!("serialize initial durable switchover operation: {error}"))?;

    let reference = TestSwitchoverReference {
        version: SWITCHOVER_CONTRACT_VERSION,
        execution_id: execution_hex,
        checkpoint_name: KubernetesCheckpointStore::object_name(execution_id),
        initial_operation_json,
    };
    test_execution_spec(&reference)?;
    Ok(reference)
}

pub fn new_switchover_execution(
    operation_authority: &str,
    previous_snapshot: StablePartitionSnapshotStatus,
    target_primary_id: i64,
    now: i64,
) -> Result<SwitchoverExecutionStatus, String> {
    if operation_authority.is_empty() {
        return Err("framework-native switchover requires operation authority".to_string());
    }
    let execution_id = ExecutionId::from_bytes(random());
    let execution_hex = encode_execution_id(execution_id);
    let reference = SwitchoverExecutionStatus {
        contract_version: SWITCHOVER_CONTRACT_VERSION,
        execution_id: execution_hex,
        checkpoint_name: KubernetesCheckpointStore::object_name(execution_id),
        input: SwitchoverAdmissionInputStatus {
            operation_authority: operation_authority.to_string(),
            previous_snapshot,
            target_primary_id,
            accepted_unix_seconds: now,
        },
    };
    native_execution_spec(&reference)?;
    Ok(reference)
}

pub fn native_execution_id(reference: &SwitchoverExecutionStatus) -> Result<ExecutionId, String> {
    if reference.contract_version != SWITCHOVER_CONTRACT_VERSION {
        return Err(format!(
            "unsupported framework-native switchover contract version {}",
            reference.contract_version
        ));
    }
    let bytes = decode_execution_id(&reference.execution_id)?;
    let execution_id = ExecutionId::from_bytes(bytes);
    let expected_name = KubernetesCheckpointStore::object_name(execution_id);
    if reference.checkpoint_name != expected_name {
        return Err(format!(
            "framework-native switchover checkpoint name mismatch: expected {expected_name}, found {}",
            reference.checkpoint_name
        ));
    }
    Ok(execution_id)
}

pub fn native_initial_operation(
    reference: &SwitchoverExecutionStatus,
) -> Result<DurableOperationStatus, String> {
    let input = &reference.input;
    if input.operation_authority.is_empty()
        || input.accepted_unix_seconds <= 0
        || input
            .accepted_unix_seconds
            .checked_add(super::ACTION_DEADLINE_SECONDS)
            .is_none()
    {
        return Err("framework-native switchover immutable admission input is invalid".to_string());
    }
    start_switchover(
        &format!(
            "{}:framework-native:{}",
            input.operation_authority, reference.execution_id
        ),
        input.previous_snapshot.clone(),
        input.target_primary_id,
        input.accepted_unix_seconds,
    )
}

pub fn validate_native_operation_authority(
    reference: &SwitchoverExecutionStatus,
    expected_set_uid: &str,
) -> Result<(), String> {
    if expected_set_uid.is_empty() || reference.input.operation_authority != expected_set_uid {
        return Err(
            "framework-native switchover operation authority does not match KubericSet UID"
                .to_string(),
        );
    }
    Ok(())
}

pub fn native_execution_spec(
    reference: &SwitchoverExecutionStatus,
) -> Result<ExecutionSpec, String> {
    let execution_id = native_execution_id(reference)?;
    let initial_operation = native_initial_operation(reference)?;
    validate_switchover_admission(&initial_operation)?;
    let input = SwitchoverWorkflowInput {
        version: reference.contract_version,
        execution_id: reference.execution_id.clone(),
        initial_operation,
    };
    let input = serde_json::to_vec(&input)
        .map_err(|error| format!("serialize framework-native switchover input: {error}"))?;
    validate_workflow_input_bytes(input.len())?;
    Ok(ExecutionSpec::new(
        execution_id,
        ExactBytes::new(input),
        SWITCHOVER_MAX_TERMINAL_PAYLOAD_BYTES,
    ))
}

#[cfg(test)]
pub(crate) fn test_execution_id(
    reference: &TestSwitchoverReference,
) -> Result<ExecutionId, String> {
    if reference.version != SWITCHOVER_CONTRACT_VERSION {
        return Err(format!(
            "unsupported switchover test contract version {}",
            reference.version
        ));
    }
    let bytes = decode_execution_id(&reference.execution_id)?;
    let execution_id = ExecutionId::from_bytes(bytes);
    let expected_name = KubernetesCheckpointStore::object_name(execution_id);
    if reference.checkpoint_name != expected_name {
        return Err(format!(
            "durable switchover checkpoint name mismatch: expected {expected_name}, found {}",
            reference.checkpoint_name
        ));
    }
    Ok(execution_id)
}

#[cfg(test)]
pub(crate) fn test_execution_spec(
    reference: &TestSwitchoverReference,
) -> Result<ExecutionSpec, String> {
    let execution_id = test_execution_id(reference)?;
    let initial_operation = test_initial_operation(reference)?;
    validate_switchover_admission(&initial_operation)?;
    let input = SwitchoverWorkflowInput {
        version: reference.version,
        execution_id: reference.execution_id.clone(),
        initial_operation,
    };
    let input = serde_json::to_vec(&input)
        .map_err(|error| format!("serialize switchover test input: {error}"))?;
    if input.len() > SWITCHOVER_MAX_WORKFLOW_INPUT_BYTES {
        return Err(format!(
            "switchover test input is {} bytes; maximum is {}",
            input.len(),
            SWITCHOVER_MAX_WORKFLOW_INPUT_BYTES
        ));
    }
    Ok(ExecutionSpec::new(
        execution_id,
        ExactBytes::new(input),
        SWITCHOVER_MAX_TERMINAL_PAYLOAD_BYTES,
    ))
}

#[cfg(test)]
pub(crate) fn test_initial_operation(
    reference: &TestSwitchoverReference,
) -> Result<DurableOperationStatus, String> {
    serde_json::from_str(&reference.initial_operation_json)
        .map_err(|error| format!("decode initial durable switchover operation: {error}"))
}

pub fn checkpoint_limits() -> CheckpointLimits {
    CheckpointLimits::new(
        SWITCHOVER_MAX_ACTIVITY_RECORDS,
        SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES,
        SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES,
    )
    .expect("framework-native switchover limits are nonzero")
}

fn validate_workflow_input_bytes(actual: usize) -> Result<(), String> {
    if actual > SWITCHOVER_MAX_WORKFLOW_INPUT_BYTES {
        Err(format!(
            "framework-native switchover workflow input is {actual} bytes; maximum is {SWITCHOVER_MAX_WORKFLOW_INPUT_BYTES}"
        ))
    } else {
        Ok(())
    }
}

fn validate_active_checkpoint_bytes(actual: usize) -> Result<(), String> {
    if actual > SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES {
        Err(format!(
            "framework-native switchover active checkpoint is {actual} bytes; maximum is {SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES}"
        ))
    } else {
        Ok(())
    }
}

fn validate_terminal_checkpoint_bytes(actual: usize) -> Result<(), String> {
    if actual > SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES {
        Err(format!(
            "framework-native switchover terminal checkpoint is {actual} bytes; maximum is {SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES}"
        ))
    } else {
        Ok(())
    }
}

fn validate_terminal_payload_bytes(actual: usize) -> Result<(), String> {
    if actual > SWITCHOVER_MAX_TERMINAL_PAYLOAD_BYTES as usize {
        Err(format!(
            "framework-native switchover terminal payload is {actual} bytes; maximum is {SWITCHOVER_MAX_TERMINAL_PAYLOAD_BYTES}"
        ))
    } else {
        Ok(())
    }
}

fn validate_error_bytes(actual: usize) -> Result<(), String> {
    if actual > SWITCHOVER_MAX_ERROR_BYTES {
        Err(format!(
            "framework-native switchover error is {actual} bytes; maximum is {SWITCHOVER_MAX_ERROR_BYTES}"
        ))
    } else {
        Ok(())
    }
}

fn validate_transition_fuel(actual: usize) -> Result<(), String> {
    if actual > SWITCHOVER_MAX_TRANSITION_FUEL {
        Err(format!(
            "framework-native switchover requires {actual} transitions; maximum is {SWITCHOVER_MAX_TRANSITION_FUEL}"
        ))
    } else {
        Ok(())
    }
}

pub(crate) fn validate_runner_fuel(actual: usize) -> Result<(), String> {
    if actual == 0 || actual > SWITCHOVER_MAX_RUNNER_FUEL {
        Err(format!(
            "framework-native switchover runner fuel is {actual}; allowed range is 1..={SWITCHOVER_MAX_RUNNER_FUEL}"
        ))
    } else {
        Ok(())
    }
}

pub fn checkpoint_store_options(
    namespace: &str,
    set_name: &str,
    set_uid: &str,
) -> Result<KubernetesCheckpointStoreOptions, String> {
    if namespace.is_empty() || set_name.is_empty() || set_uid.is_empty() {
        return Err(
            "durable switchover checkpoint owner requires namespace, name, and UID".to_string(),
        );
    }
    let owner = KubernetesCheckpointOwner::new(
        OwnerReference {
            api_version: "kuberic.io/v1".to_string(),
            kind: "KubericSet".to_string(),
            name: set_name.to_string(),
            uid: set_uid.to_string(),
            controller: Some(false),
            block_owner_deletion: Some(false),
        },
        KubernetesCheckpointOwnerScope::Namespaced(namespace.to_string()),
    );
    Ok(KubernetesCheckpointStoreOptions::default().with_owner(owner))
}

pub fn validate_switchover_admission(operation: &DurableOperationStatus) -> Result<(), String> {
    validate_switchover_operation_contract(operation)?;
    validate_variant_bounds(operation)?;
    let previous_snapshot = operation
        .previous_snapshot
        .as_ref()
        .ok_or_else(|| "durable switchover has no previous snapshot".to_string())?;
    let success = projected_success_transcript(previous_snapshot.members.len());
    let rollback = projected_rollback_transcript(previous_snapshot.members.len());
    let projected_steps = success
        .maximum_activity_count()
        .max(rollback.maximum_activity_count());
    if projected_steps > SWITCHOVER_MAX_ACTIVITY_RECORDS {
        return Err(format!(
            "durable switchover requires {projected_steps} projected activities; maximum is {SWITCHOVER_MAX_ACTIVITY_RECORDS}"
        ));
    }
    let projected_transitions = (success.maximum_activity_count()
        + projected_success_pure_transitions())
    .max(rollback.maximum_activity_count() + projected_rollback_pure_transitions());
    validate_transition_fuel(projected_transitions)?;
    let projected_bytes = maximum_projected_checkpoint_bytes()?;
    validate_active_checkpoint_bytes(projected_bytes)?;
    let projected_terminal = maximum_terminal_checkpoint()?
        .encoded_len()
        .map_err(|error| format!("measure maximum switchover terminal checkpoint: {error}"))?;
    validate_terminal_checkpoint_bytes(projected_terminal)?;
    Ok(())
}

pub fn validate_switchover_operation_contract(
    operation: &DurableOperationStatus,
) -> Result<(), String> {
    validate_switchover_operation(operation)?;
    let previous_snapshot = operation
        .previous_snapshot
        .as_ref()
        .ok_or_else(|| "durable switchover has no previous snapshot".to_string())?;
    if previous_snapshot.members.len() > SWITCHOVER_MAX_REPLICAS {
        return Err(format!(
            "durable switchover supports at most {SWITCHOVER_MAX_REPLICAS} replicas; found {}",
            previous_snapshot.members.len()
        ));
    }
    let cursor = usize::try_from(operation.next_secondary_index)
        .map_err(|_| "durable switchover secondary cursor is not representable".to_string())?;
    let maximum_cursor = previous_snapshot.members.len().saturating_sub(1);
    if cursor > maximum_cursor {
        return Err(format!(
            "durable switchover secondary cursor {cursor} exceeds maximum {maximum_cursor}"
        ));
    }
    if let Some(pending) = operation.pending_action.as_ref() {
        let available = match pending.kind {
            DurableActionKind::UpdateSecondaryEpoch => {
                previous_snapshot.members.len().saturating_sub(2)
            }
            DurableActionKind::CompensateUpdateSecondaryEpoch => maximum_cursor,
            _ => maximum_cursor.saturating_add(1),
        };
        if cursor >= available {
            return Err(format!(
                "durable switchover pending {:?} has out-of-range secondary cursor {cursor}",
                pending.kind
            ));
        }
    }
    let operation_bytes = serde_json::to_vec(operation)
        .map_err(|error| format!("serialize durable switchover operation: {error}"))?
        .len();
    if operation_bytes > SWITCHOVER_MAX_WORKFLOW_INPUT_BYTES {
        return Err(format!(
            "durable switchover operation is {operation_bytes} bytes; maximum is {SWITCHOVER_MAX_WORKFLOW_INPUT_BYTES}"
        ));
    }
    if let Some(error) = operation.last_error.as_deref() {
        validate_error_bytes(error.len())?;
    }
    if let Some(error) = operation
        .pending_action
        .as_ref()
        .and_then(|pending| pending.last_error.as_deref())
    {
        validate_error_bytes(error.len())?;
    }
    Ok(())
}

fn maximum_projected_checkpoint_bytes() -> Result<usize, String> {
    static PROJECTED_BYTES: OnceLock<Result<usize, String>> = OnceLock::new();
    PROJECTED_BYTES
        .get_or_init(|| {
            maximum_active_checkpoint()?
                .encoded_len()
                .map_err(|error| format!("measure maximum switchover checkpoint: {error}"))
        })
        .clone()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProjectedActivityKind {
    ReplicaEffect,
    OtherExternalEffect,
    PassiveObservation,
    ExternalOrPassive,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProjectedActivity {
    name: &'static str,
    kind: ProjectedActivityKind,
}

impl ProjectedActivity {
    const fn replica_effect(name: &'static str) -> Self {
        Self {
            name,
            kind: ProjectedActivityKind::ReplicaEffect,
        }
    }

    const fn external_effect(name: &'static str) -> Self {
        Self {
            name,
            kind: ProjectedActivityKind::OtherExternalEffect,
        }
    }

    const fn passive(name: &'static str) -> Self {
        Self {
            name,
            kind: ProjectedActivityKind::PassiveObservation,
        }
    }

    const fn external_or_passive(name: &'static str) -> Self {
        Self {
            name,
            kind: ProjectedActivityKind::ExternalOrPassive,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProjectedTranscript {
    activities: Vec<ProjectedActivity>,
}

impl ProjectedTranscript {
    fn new(activities: Vec<ProjectedActivity>) -> Self {
        Self { activities }
    }

    #[cfg(test)]
    fn external_effect_slot_count(&self) -> usize {
        self.activities
            .iter()
            .filter(|activity| {
                matches!(
                    activity.kind,
                    ProjectedActivityKind::ReplicaEffect
                        | ProjectedActivityKind::OtherExternalEffect
                        | ProjectedActivityKind::ExternalOrPassive
                )
            })
            .count()
    }

    fn redelivery_slot_count(&self) -> usize {
        self.activities
            .iter()
            .filter(|activity| activity.kind == ProjectedActivityKind::ReplicaEffect)
            .count()
    }

    fn maximum_activity_count(&self) -> usize {
        self.activities.len() + self.redelivery_slot_count()
    }

    fn required_external_effect_count(&self) -> usize {
        self.activities
            .iter()
            .filter(|activity| {
                matches!(
                    activity.kind,
                    ProjectedActivityKind::ReplicaEffect
                        | ProjectedActivityKind::OtherExternalEffect
                )
            })
            .count()
    }

    fn required_passive_observation_count(&self) -> usize {
        self.activities
            .iter()
            .filter(|activity| activity.kind == ProjectedActivityKind::PassiveObservation)
            .count()
    }

    fn flexible_activity_count(&self) -> usize {
        self.activities
            .iter()
            .filter(|activity| activity.kind == ProjectedActivityKind::ExternalOrPassive)
            .count()
    }

    #[cfg(test)]
    fn contains(&self, name: &str) -> bool {
        self.activities.iter().any(|activity| activity.name == name)
    }

    /// Returns whether the accounting is produced by one valid base
    /// transcript plus zero or one proven-no-admission redelivery for each
    /// activity that was actually exposed as an external effect.
    fn contains_accounting(&self, accounting: SwitchoverActivityAccounting) -> bool {
        let required_external = self.required_external_effect_count();
        let required_passive = self.required_passive_observation_count();
        let flexible = self.flexible_activity_count();
        (0..=flexible).any(|external_flexible| {
            let base_external = required_external + external_flexible;
            let passive = required_passive + flexible - external_flexible;
            let maximum_external = base_external + self.redelivery_slot_count();
            usize::try_from(accounting.passive_observation_count) == Ok(passive)
                && usize::try_from(accounting.external_effect_count)
                    .is_ok_and(|external| (base_external..=maximum_external).contains(&external))
        })
    }
}

fn projected_success_transcript(member_count: usize) -> ProjectedTranscript {
    let mut activities = vec![
        ProjectedActivity::replica_effect("revoke.effect"),
        ProjectedActivity::passive("capture-lsn"),
        ProjectedActivity::passive("target-catch-up"),
        ProjectedActivity::replica_effect("demote.effect"),
        ProjectedActivity::replica_effect("promote.effect"),
    ];
    activities.extend(std::iter::repeat_n(
        ProjectedActivity::replica_effect("epoch.effect"),
        member_count.saturating_sub(2),
    ));
    activities.extend([
        ProjectedActivity::replica_effect("catch-up-config.effect"),
        ProjectedActivity::replica_effect("quorum.effect"),
        ProjectedActivity::replica_effect("current-config.effect"),
        ProjectedActivity::external_effect("target-label.effect"),
        ProjectedActivity::external_effect("old-label.effect"),
        ProjectedActivity::passive("final-attestation"),
    ]);
    ProjectedTranscript::new(activities)
}

fn projected_pre_catch_up_restore_transcript() -> ProjectedTranscript {
    ProjectedTranscript::new(vec![
        ProjectedActivity::replica_effect("revoke.effect"),
        ProjectedActivity::passive("capture-lsn"),
        ProjectedActivity::passive("target-catch-up"),
        ProjectedActivity::replica_effect("restore-previous.effect"),
        ProjectedActivity::passive("rollback-final-attestation"),
    ])
}

fn projected_demote_restore_transcript() -> ProjectedTranscript {
    ProjectedTranscript::new(vec![
        ProjectedActivity::replica_effect("revoke.effect"),
        ProjectedActivity::passive("capture-lsn"),
        ProjectedActivity::passive("target-catch-up"),
        ProjectedActivity::replica_effect("demote.effect"),
        ProjectedActivity::replica_effect("restore-previous.effect"),
        ProjectedActivity::passive("rollback-final-attestation"),
    ])
}

fn is_full_switchover_start(initial: &DurableOperationStatus) -> bool {
    initial.phase == DurableOperationPhase::Revoke
        && initial.pending_action.is_none()
        && initial.frozen_lsn.is_none()
        && initial.next_secondary_index == 0
        && (2..=SWITCHOVER_MAX_REPLICAS).contains(&initial.previous_snapshot.members.len())
}

fn validate_terminal_activity_accounting(
    initial: &DurableOperationStatus,
    terminal: &DurableOperationStatus,
    compensated: bool,
    accounting: SwitchoverActivityAccounting,
    completed_activity_count: u64,
) -> Result<(), String> {
    if !is_full_switchover_start(initial) {
        return Ok(());
    }
    validate_terminal_accounting_shape(
        TerminalAccountingContext::from_state(&DurableSwitchoverState::from_operation(terminal)),
        initial.previous_snapshot.members.len(),
        compensated,
        accounting,
        completed_activity_count,
    )
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct TerminalAccountingContext {
    phase: DurableOperationPhase,
    frozen_lsn: Option<i64>,
    next_secondary_index: u32,
    has_pending_action: bool,
    has_last_error: bool,
}

impl TerminalAccountingContext {
    fn from_state(state: &DurableSwitchoverState) -> Self {
        Self {
            phase: state.phase,
            frozen_lsn: state.frozen_lsn,
            next_secondary_index: state.next_secondary_index,
            has_pending_action: state.pending_action.is_some(),
            has_last_error: state.last_error.is_some(),
        }
    }
}

fn validate_terminal_accounting_shape(
    terminal: TerminalAccountingContext,
    member_count: usize,
    compensated: bool,
    accounting: SwitchoverActivityAccounting,
    completed_activity_count: u64,
) -> Result<(), String> {
    if !accounting.matches_completed_activity_count(completed_activity_count) {
        return Err(format!(
            "switchover terminal activity accounting {}/{} does not total the authoritative \
             completed activity count {completed_activity_count}",
            accounting.external_effect_count, accounting.passive_observation_count,
        ));
    }

    if !compensated {
        let projection = projected_success_transcript(member_count);
        let minimum_external = projection.required_external_effect_count();
        let maximum_external =
            projection.required_external_effect_count() + projection.redelivery_slot_count();
        if terminal.phase != DurableOperationPhase::Completed
            || terminal.has_pending_action
            || !projection.contains_accounting(accounting)
        {
            return Err(format!(
                "successful {member_count}-member switchover requires exactly three passive \
                 observations and {minimum_external}..={maximum_external} external effects \
                 (at most one projected redelivery per ReplicaAgent effect), got {}/{}",
                accounting.external_effect_count, accounting.passive_observation_count,
            ));
        }
        return Ok(());
    }

    let compensation_projections = projected_compensation_transcripts(terminal, member_count);
    if compensation_projections
        .iter()
        .all(|projection| !projection.contains_accounting(accounting))
    {
        return Err(format!(
            "compensated switchover activity accounting {}/{} is not reachable from the terminal \
             operation's compensation transcript and its per-effect bounded redelivery slots",
            accounting.external_effect_count, accounting.passive_observation_count,
        ));
    }
    Ok(())
}

fn projected_compensation_transcripts(
    terminal: TerminalAccountingContext,
    member_count: usize,
) -> Vec<ProjectedTranscript> {
    if terminal.phase != DurableOperationPhase::Failed
        || terminal.has_pending_action
        || terminal.frozen_lsn.is_none()
        || terminal.has_last_error
    {
        return Vec::new();
    }
    if terminal.next_secondary_index == 0 {
        return vec![
            projected_pre_catch_up_restore_transcript(),
            projected_demote_restore_transcript(),
        ];
    }
    if usize::try_from(terminal.next_secondary_index) == Ok(member_count.saturating_sub(1)) {
        return vec![projected_rollback_transcript(member_count)];
    }
    Vec::new()
}

#[cfg(test)]
fn projected_label_effect_count(transcript: &ProjectedTranscript) -> usize {
    transcript
        .activities
        .iter()
        .filter(|activity| activity.name.contains("label.effect"))
        .count()
}

fn validate_variant_bounds(operation: &DurableOperationStatus) -> Result<(), String> {
    let previous = operation
        .previous_snapshot
        .cloned()
        .ok_or_else(|| "durable switchover has no previous snapshot".to_string())?;
    let mut stopped = operation.clone();
    stopped.phase = crate::crd::DurableOperationPhase::Poisoned;
    stopped.pending_action = None;
    let state = DurableSwitchoverState::from_operation(operation);
    let variants = [
        DurableSwitchoverStepResult::Advance {
            operation: state.clone(),
        },
        DurableSwitchoverStepResult::ProvenNoAdmission {
            operation: state.clone(),
            action_id: operation
                .pending_action
                .as_ref()
                .map(|pending| pending.action_id.clone())
                .unwrap_or_else(|| format!("{}:maximum-action", operation.operation_id)),
            redelivery: 1,
        },
        DurableSwitchoverStepResult::Complete {
            operation: state.clone(),
            snapshot: operation.target_snapshot.clone(),
            compensated: false,
            accounting: Some(SwitchoverActivityAccounting::new(u64::MAX, u64::MAX)),
        },
        DurableSwitchoverStepResult::Complete {
            operation: state,
            snapshot: previous.clone(),
            compensated: true,
            accounting: Some(SwitchoverActivityAccounting::new(u64::MAX, u64::MAX)),
        },
        DurableSwitchoverStepResult::Stopped {
            operation: DurableSwitchoverState::from_operation(&stopped),
            message: "x".repeat(512),
        },
    ];
    for variant in &variants {
        encode_step_result(variant)?;
    }
    for terminal in [
        DurableSwitchoverTerminalRecord::Complete {
            state: DurableSwitchoverState::from_operation(operation),
            snapshot: operation.target_snapshot.clone(),
            compensated: false,
            accounting: SwitchoverActivityAccounting::new(u64::MAX, u64::MAX),
        },
        DurableSwitchoverTerminalRecord::Complete {
            state: DurableSwitchoverState::from_operation(operation),
            snapshot: previous,
            compensated: true,
            accounting: SwitchoverActivityAccounting::new(u64::MAX, u64::MAX),
        },
        DurableSwitchoverTerminalRecord::Stopped {
            state: Some(DurableSwitchoverState::from_operation(&stopped)),
            message: "x".repeat(512),
        },
    ] {
        encode_terminal_record(&terminal)?;
    }
    Ok(())
}

fn projected_rollback_transcript(member_count: usize) -> ProjectedTranscript {
    let mut activities = vec![
        ProjectedActivity::replica_effect("revoke.effect"),
        ProjectedActivity::passive("capture-lsn"),
        ProjectedActivity::passive("target-catch-up"),
        ProjectedActivity::replica_effect("demote.effect"),
        ProjectedActivity::replica_effect("promote.effect"),
        ProjectedActivity::replica_effect("rollback-promote.effect"),
    ];
    activities.extend(std::iter::repeat_n(
        ProjectedActivity::replica_effect("rollback-epoch.effect"),
        member_count.saturating_sub(1),
    ));
    activities.extend([
        ProjectedActivity::replica_effect("rollback-catch-up.effect"),
        ProjectedActivity::replica_effect("rollback-current.effect"),
        ProjectedActivity::external_or_passive("rollback-old-label.effect"),
        ProjectedActivity::external_or_passive("rollback-target-label.effect"),
        ProjectedActivity::passive("rollback-final-attestation"),
    ]);
    ProjectedTranscript::new(activities)
}

const fn projected_success_pure_transitions() -> usize {
    10
}

const fn projected_rollback_pure_transitions() -> usize {
    11
}

pub fn maximum_active_checkpoint() -> Result<CheckpointEnvelope, String> {
    let payload = maximum_active_payload(SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES)?;
    CheckpointEnvelope::encode_with_limits(&payload, checkpoint_limits())
        .map_err(|error| format!("project maximum switchover checkpoint: {error}"))
}

fn maximum_active_payload(
    admitted_max_encoded_checkpoint_bytes: usize,
) -> Result<CheckpointPayload, String> {
    active_payload_with_records(
        admitted_max_encoded_checkpoint_bytes,
        SWITCHOVER_MAX_ACTIVITY_RECORDS,
    )
}

fn active_payload_with_records(
    admitted_max_encoded_checkpoint_bytes: usize,
    record_count: usize,
) -> Result<CheckpointPayload, String> {
    let execution_id = ExecutionId::from_bytes([u8::MAX; 16]);
    let execution = ExecutionSpec::new(
        execution_id,
        ExactBytes::new(vec![u8::MAX; SWITCHOVER_MAX_WORKFLOW_INPUT_BYTES]),
        SWITCHOVER_MAX_TERMINAL_PAYLOAD_BYTES,
    );
    let admitted = u64::try_from(admitted_max_encoded_checkpoint_bytes)
        .map_err(|_| "switchover checkpoint limit does not fit u64".to_string())?;
    let contract = ExecutionContract::with_encoded_limits(
        execution,
        admitted,
        SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES as u64,
    );
    let name = ActivityName::new(SWITCHOVER_ACTIVITY_NAME, SWITCHOVER_ACTIVITY_VERSION)
        .map_err(|error| format!("construct switchover activity name: {error}"))?;
    let activities = (0..record_count)
        .map(|sequence| {
            let spec = ActivitySpec::new(
                name.clone(),
                ExactBytes::new(vec![u8::MAX; SWITCHOVER_MAX_ACTIVITY_INPUT_BYTES]),
                u64::try_from(SWITCHOVER_MAX_ACTIVITY_RESULT_BYTES)
                    .expect("switchover result bound fits u64"),
            );
            ActivityRecord::completed(
                ActivitySequence::new(
                    u64::try_from(sequence).expect("switchover activity count fits u64"),
                ),
                spec,
                ExactBytes::new(vec![u8::MAX; SWITCHOVER_MAX_ACTIVITY_RESULT_BYTES]),
            )
        })
        .collect();
    Ok(CheckpointPayload::active(contract, activities))
}

#[cfg(test)]
fn projected_active_checkpoint_bytes(record_count: usize) -> Result<usize, String> {
    CheckpointEnvelope::encode_with_limits(
        &active_payload_with_records(SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES, record_count)?,
        checkpoint_limits(),
    )
    .map_err(|error| format!("encode projected switchover checkpoint: {error}"))?
    .encoded_len()
    .map_err(|error| format!("measure projected switchover checkpoint: {error}"))
}

#[cfg(test)]
fn projected_unadmitted_active_checkpoint_bytes(record_count: usize) -> Result<usize, String> {
    let projection_capacity = SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES * 2;
    let measurement_limits = CheckpointLimits::new(
        record_count,
        projection_capacity,
        SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES,
    )
    .map_err(|error| format!("construct projection-only checkpoint limits: {error}"))?;
    CheckpointEnvelope::encode_with_limits(
        &active_payload_with_records(projection_capacity, record_count)?,
        measurement_limits,
    )
    .map_err(|error| format!("encode projection-only switchover checkpoint: {error}"))?
    .encoded_len()
    .map_err(|error| format!("measure projection-only switchover checkpoint: {error}"))
}

pub fn maximum_terminal_checkpoint() -> Result<CheckpointEnvelope, String> {
    let execution_id = ExecutionId::from_bytes([u8::MAX; 16]);
    let execution = ExecutionSpec::new(
        execution_id,
        ExactBytes::new(vec![u8::MAX; SWITCHOVER_MAX_WORKFLOW_INPUT_BYTES]),
        SWITCHOVER_MAX_TERMINAL_PAYLOAD_BYTES,
    );
    let contract = ExecutionContract::with_encoded_limits(
        execution,
        SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES as u64,
        SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES as u64,
    );
    CheckpointEnvelope::encode_with_limits(
        &CheckpointPayload::terminal(
            contract,
            TerminalOutcome::failed(vec![
                u8::MAX;
                SWITCHOVER_MAX_TERMINAL_PAYLOAD_BYTES as usize
            ]),
            SWITCHOVER_MAX_ACTIVITY_RECORDS as u64,
        ),
        checkpoint_limits(),
    )
    .map_err(|error| format!("project maximum framework-native switchover terminal: {error}"))
}

#[cfg(test)]
fn maximum_prepared_activity_input() -> Result<ExactBytes, String> {
    let maximum_fence = "x".repeat(512);
    let input = DurableSwitchoverActivityInput {
        version: SWITCHOVER_CONTRACT_VERSION,
        state: DurableSwitchoverState {
            phase: DurableOperationPhase::Poisoned,
            frozen_lsn: Some(i64::MAX),
            next_secondary_index: u32::MAX,
            phase_deadline_unix_seconds: i64::MAX,
            pending_action: None,
            last_error: Some(maximum_fence.clone()),
        },
        kind: SwitchoverActivityKind::PreparedReplica {
            command: ReplicaEffectCommand {
                action_id: maximum_fence.clone(),
                action_signature: maximum_fence.clone(),
                target_id: i64::MAX,
                target_instance_id: maximum_fence.clone(),
                expected_epoch: crate::crd::EpochStatus {
                    data_loss_number: i64::MAX,
                    configuration_number: i64::MAX,
                },
                desired_postcondition: crate::crd::DurablePostconditionStatus {
                    kind: crate::crd::DurablePostconditionKind::CurrentConfiguration,
                    role: Some(crate::crd::StableReplicaRoleStatus::Primary),
                },
                expected_agent_generation: maximum_fence.clone(),
                expected_control_version: u64::MAX,
                observed_runtime_epoch: crate::crd::EpochStatus {
                    data_loss_number: i64::MAX,
                    configuration_number: i64::MAX,
                },
                action_payload: "x".repeat(4_096),
            },
        },
    };
    let encoded = encode_activity_input::<DurableSwitchoverActivity>(&input)
        .map_err(|error| format!("encode maximum prepared switchover activity: {error}"))?;
    if encoded.as_slice().len() > SWITCHOVER_MAX_ACTIVITY_INPUT_BYTES {
        return Err(format!(
            "maximum prepared switchover activity is {} bytes; maximum is {}",
            encoded.as_slice().len(),
            SWITCHOVER_MAX_ACTIVITY_INPUT_BYTES
        ));
    }
    Ok(encoded)
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
            "durable switchover execution ID must contain 32 lowercase hexadecimal characters; found {}",
            value.len()
        ));
    }
    let mut bytes = [0_u8; 16];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = decode_hex(pair[0])?;
        let low = decode_hex(pair[1])?;
        bytes[index] = (high << 4) | low;
    }
    Ok(bytes)
}

fn decode_hex(value: u8) -> Result<u8, String> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err("durable switchover execution ID must be lowercase hexadecimal".to_string()),
    }
}

#[cfg(test)]
mod framework_native_switchover_tests {
    use super::*;
    use super::{
        new_test_reference as test_reference, test_execution_id as execution_id,
        test_execution_spec as execution_spec, test_initial_operation as initial_operation,
        validate_test_loaded_terminal as validate_loaded_terminal,
    };
    use crate::crd::{EpochStatus, StableReplicaRoleStatus, StableReplicaSnapshotStatus};
    use crate::durable::{Decision, decide};
    use kuberic_durable_execution::ActivityObservation;
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::{AtomicBool, Ordering};

    type TestActivityKind = SwitchoverActivityKind;
    type TestActivityAccounting = SwitchoverActivityAccounting;
    type TestAdapterDecision = SwitchoverAdapterDecision;
    type TestPreparedActivityResolver = SwitchoverPreparedActivityResolver;
    type TestPermitGuard = SwitchoverPermitGuard;
    type TestTerminal = SwitchoverTerminal;

    fn snapshot(member_count: usize) -> StablePartitionSnapshotStatus {
        StablePartitionSnapshotStatus {
            epoch: EpochStatus {
                data_loss_number: 4,
                configuration_number: 8,
            },
            primary_id: 1,
            members: (1..=member_count)
                .map(|id| StableReplicaSnapshotStatus {
                    id: i64::try_from(id).unwrap(),
                    instance_id: format!("pod-{id}-uid"),
                    role: if id == 1 {
                        StableReplicaRoleStatus::Primary
                    } else {
                        StableReplicaRoleStatus::ActiveSecondary
                    },
                    election_metadata: None,
                })
                .collect(),
            write_quorum: u32::try_from(member_count / 2 + 1).unwrap(),
        }
    }

    fn compact(operation: &DurableOperationStatus) -> DurableSwitchoverState {
        DurableSwitchoverState::from_operation(operation)
    }

    #[test]
    fn framework_native_reference_is_immutable_and_reconstructs_admission() {
        let previous = snapshot(3);
        let first = new_switchover_execution("set-uid", previous.clone(), 2, 100).unwrap();
        let second = new_switchover_execution("set-uid", previous.clone(), 2, 100).unwrap();
        assert_eq!(first.contract_version, SWITCHOVER_CONTRACT_VERSION);
        assert_ne!(first.execution_id, second.execution_id);
        assert_eq!(
            first.checkpoint_name,
            KubernetesCheckpointStore::object_name(native_execution_id(&first).unwrap())
        );
        let operation = native_initial_operation(&first).unwrap();
        assert_eq!(operation.previous_snapshot.as_ref(), Some(&previous));
        assert_eq!(operation.target_primary_id, 2);
        assert!(operation.operation_id.contains(&first.execution_id));
        assert!(native_execution_spec(&first).is_ok());
    }

    #[test]
    fn framework_native_reference_schema_requires_current_admission_input() {
        let reference = new_switchover_execution("set-uid", snapshot(3), 2, 100).unwrap();
        let mut missing = serde_json::to_value(&reference).unwrap();
        missing.as_object_mut().unwrap().remove("input");
        assert!(serde_json::from_value::<SwitchoverExecutionStatus>(missing).is_err());

        let mut unknown = serde_json::to_value(&reference).unwrap();
        unknown
            .as_object_mut()
            .unwrap()
            .insert("unknownState".to_string(), serde_json::json!({}));
        assert!(serde_json::from_value::<SwitchoverExecutionStatus>(unknown).is_err());
    }

    #[test]
    fn framework_native_active_and_terminal_projections_fit_independent_bounds() {
        let active = maximum_active_checkpoint().unwrap();
        let terminal = maximum_terminal_checkpoint().unwrap();
        let active_bytes = active.encoded_len().unwrap();
        let terminal_bytes = terminal.encoded_len().unwrap();
        assert!(active_bytes <= SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES);
        assert!(terminal_bytes <= SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES);
        assert_ne!(
            SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES,
            SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES
        );
        eprintln!(
            "framework-native switchover projected bounds: records={}, workflow_input={}, activity_input={}, result={}, active={}, terminal={}, terminal_payload={}, transition_fuel={}, runner_fuel={}",
            SWITCHOVER_MAX_ACTIVITY_RECORDS,
            SWITCHOVER_MAX_WORKFLOW_INPUT_BYTES,
            SWITCHOVER_MAX_ACTIVITY_INPUT_BYTES,
            SWITCHOVER_MAX_ACTIVITY_RESULT_BYTES,
            active_bytes,
            terminal_bytes,
            SWITCHOVER_MAX_TERMINAL_PAYLOAD_BYTES,
            SWITCHOVER_MAX_TRANSITION_FUEL,
            SWITCHOVER_MAX_RUNNER_FUEL
        );
    }

    #[test]
    fn framework_native_error_limit_is_utf8_byte_bounded() {
        let bounded = bounded_utf8(
            &"é".repeat(SWITCHOVER_MAX_ERROR_BYTES),
            SWITCHOVER_MAX_ERROR_BYTES,
        );
        assert_eq!(bounded.len(), SWITCHOVER_MAX_ERROR_BYTES);
        assert!(bounded.is_char_boundary(bounded.len()));
        assert_eq!(bounded, "é".repeat(SWITCHOVER_MAX_ERROR_BYTES / 2));
    }

    struct NativeInputBound;

    impl DurableActivity for NativeInputBound {
        type Input = String;
        type Output = ();

        const NAME: &'static str = "native-switchover-input-bound";
        const VERSION: u32 = 1;
        const MAX_INPUT_BYTES: u64 = SWITCHOVER_MAX_ACTIVITY_INPUT_BYTES as u64;
        const MAX_RESULT_BYTES: u64 = 1;
    }

    struct NativeResultBound;

    impl DurableActivity for NativeResultBound {
        type Input = ();
        type Output = String;

        const NAME: &'static str = "native-switchover-result-bound";
        const VERSION: u32 = 1;
        const MAX_INPUT_BYTES: u64 = 1;
        const MAX_RESULT_BYTES: u64 = SWITCHOVER_MAX_ACTIVITY_RESULT_BYTES as u64;
    }

    #[test]
    fn framework_native_switchover_rejects_every_independent_one_over_bound() {
        let execution = ExecutionSpec::new(
            ExecutionId::from_bytes([9; 16]),
            ExactBytes::new(vec![9; SWITCHOVER_MAX_WORKFLOW_INPUT_BYTES]),
            SWITCHOVER_MAX_TERMINAL_PAYLOAD_BYTES,
        );
        let logical = ActivitySpec::new(
            ActivityName::new(SWITCHOVER_ACTIVITY_NAME, SWITCHOVER_ACTIVITY_VERSION).unwrap(),
            ExactBytes::new(b"{}".to_vec()),
            SWITCHOVER_MAX_ACTIVITY_RESULT_BYTES as u64,
        );
        let activities = (0..=SWITCHOVER_MAX_ACTIVITY_RECORDS)
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
                        execution,
                        SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES as u64,
                        SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES as u64,
                    ),
                    activities,
                ),
                checkpoint_limits(),
            ),
            Err(CheckpointError::ActivityRecordLimitExceeded { .. })
        ));
        assert!(matches!(
            encode_activity_input::<NativeInputBound>(
                &"x".repeat(SWITCHOVER_MAX_ACTIVITY_INPUT_BYTES - 1)
            ),
            Err(ActivityCallError::InputTooLarge { .. })
        ));
        assert!(matches!(
            encode_activity_result::<NativeResultBound>(
                &"x".repeat(SWITCHOVER_MAX_ACTIVITY_RESULT_BYTES - 1)
            ),
            Err(ActivityCallError::ResultTooLarge { .. })
        ));
        assert!(validate_workflow_input_bytes(SWITCHOVER_MAX_WORKFLOW_INPUT_BYTES + 1).is_err());
        assert!(validate_active_checkpoint_bytes(SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES + 1).is_err());
        assert!(
            validate_terminal_checkpoint_bytes(SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES + 1).is_err()
        );
        assert!(
            validate_terminal_payload_bytes(SWITCHOVER_MAX_TERMINAL_PAYLOAD_BYTES as usize + 1)
                .is_err()
        );
        assert!(validate_transition_fuel(SWITCHOVER_MAX_TRANSITION_FUEL + 1).is_err());
        assert!(validate_runner_fuel(SWITCHOVER_MAX_RUNNER_FUEL + 1).is_err());
        assert!(validate_error_bytes(SWITCHOVER_MAX_ERROR_BYTES + 1).is_err());
        assert!(
            new_switchover_execution("set-uid", snapshot(SWITCHOVER_MAX_REPLICAS + 1), 2, 100)
                .is_err()
        );
        let bounded = bounded_utf8(
            &"é".repeat(SWITCHOVER_MAX_ERROR_BYTES),
            SWITCHOVER_MAX_ERROR_BYTES,
        );
        assert_eq!(bounded.len(), SWITCHOVER_MAX_ERROR_BYTES);
        assert!("é".repeat(SWITCHOVER_MAX_ERROR_BYTES).len() > SWITCHOVER_MAX_ERROR_BYTES);
    }

    #[test]
    fn references_are_stable_but_repeated_requests_are_distinct() {
        let first = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let second = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        assert_ne!(first.execution_id, second.execution_id);
        assert_ne!(
            initial_operation(&first).unwrap().operation_id,
            initial_operation(&second).unwrap().operation_id
        );
        let first_action =
            match decide(&initial_operation(&first).unwrap(), &BTreeMap::new(), 100).unwrap() {
                Decision::Persist(operation) => operation.pending_action.unwrap().action_id,
                other => panic!("expected initial pending action, found {other:?}"),
            };
        let second_action =
            match decide(&initial_operation(&second).unwrap(), &BTreeMap::new(), 100).unwrap() {
                Decision::Persist(operation) => operation.pending_action.unwrap().action_id,
                other => panic!("expected initial pending action, found {other:?}"),
            };
        assert_ne!(first_action, second_action);
        assert_eq!(
            execution_spec(&first).unwrap(),
            execution_spec(&first).unwrap()
        );
        assert_eq!(
            first.checkpoint_name,
            KubernetesCheckpointStore::object_name(execution_id(&first).unwrap())
        );
    }

    #[test]
    fn owner_is_same_namespace_non_controlling_and_non_blocking() {
        let options = checkpoint_store_options("tenant-a", "database", "set-uid").unwrap();
        let owner = options.owner().unwrap();
        assert_eq!(
            owner.scope(),
            &KubernetesCheckpointOwnerScope::Namespaced("tenant-a".to_string())
        );
        assert_eq!(owner.reference().api_version, "kuberic.io/v1");
        assert_eq!(owner.reference().kind, "KubericSet");
        assert_eq!(owner.reference().name, "database");
        assert_eq!(owner.reference().uid, "set-uid");
        assert_eq!(owner.reference().controller, Some(false));
        assert_eq!(owner.reference().block_owner_deletion, Some(false));
    }

    #[test]
    fn admission_accepts_four_and_product_max_members_and_rejects_one_over() {
        assert!(new_switchover_execution("set-uid", snapshot(4), 2, 100).is_ok());
        assert!(
            new_switchover_execution("set-uid", snapshot(SWITCHOVER_MAX_REPLICAS), 2, 100).is_ok()
        );
        let error =
            new_switchover_execution("set-uid", snapshot(SWITCHOVER_MAX_REPLICAS + 1), 2, 100)
                .unwrap_err();
        assert!(
            error.contains(&format!("at most {SWITCHOVER_MAX_REPLICAS} replicas")),
            "{error}"
        );
    }

    #[test]
    fn maximum_projected_history_fits_both_budgets() {
        let prepared_input = maximum_prepared_activity_input().unwrap();
        assert!(
            prepared_input.as_slice().len() > SWITCHOVER_MAX_ACTIVITY_INPUT_BYTES / 2,
            "prepared fixture must exercise large commands and fences"
        );
        assert!(matches!(
            decode_switchover_activity_input(&prepared_input)
                .unwrap()
                .kind,
            TestActivityKind::PreparedReplica { .. }
        ));
        let checkpoint = maximum_active_checkpoint().unwrap();
        let encoded = checkpoint.encoded_len().unwrap();
        assert!(encoded <= SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES);
        assert!(
            encoded + "checkpoint.json".len()
                <= usize::try_from(kuberic_durable_execution::DEFAULT_CONFIG_MAP_DATA_BUDGET_BYTES)
                    .unwrap()
        );

        let payload = maximum_active_payload(encoded).unwrap();
        let exact = CheckpointLimits::new(
            SWITCHOVER_MAX_ACTIVITY_RECORDS,
            encoded,
            SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES,
        )
        .unwrap();
        assert!(CheckpointEnvelope::encode_with_limits(&payload, exact).is_ok());
        let one_byte_short = CheckpointLimits::new(
            SWITCHOVER_MAX_ACTIVITY_RECORDS,
            encoded - 1,
            SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES,
        )
        .unwrap();
        assert!(CheckpointEnvelope::encode_with_limits(&payload, one_byte_short).is_err());
    }

    #[test]
    fn malformed_or_mismatched_execution_identity_is_rejected() {
        let mut reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        reference.execution_id = "ABC".to_string();
        assert!(execution_id(&reference).is_err());

        let mut reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        reference.checkpoint_name.push_str("-other");
        assert!(execution_id(&reference).is_err());
    }

    #[test]
    fn workflow_error_is_bounded_before_checkpoint_creation() {
        let mut reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let mut operation = initial_operation(&reference).unwrap();
        operation.last_error = Some("x".repeat(SWITCHOVER_MAX_ERROR_BYTES + 1));
        reference.initial_operation_json = serde_json::to_string(&operation).unwrap();
        let error = execution_spec(&reference).unwrap_err();
        assert!(error.contains("switchover error is 513 bytes"), "{error}");
    }

    #[test]
    fn missing_previous_snapshot_is_rejected_without_panic() {
        let mut reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let mut value: serde_json::Value =
            serde_json::from_str(&reference.initial_operation_json).unwrap();
        value.as_object_mut().unwrap().remove("previousSnapshot");
        reference.initial_operation_json = serde_json::to_string(&value).unwrap();
        let error = execution_spec(&reference).unwrap_err();
        assert!(error.contains("previous stable snapshot"), "{error}");

        let mut operation =
            initial_operation(&test_reference("set-uid", snapshot(3), 2, 100).unwrap()).unwrap();
        operation.previous_snapshot = Default::default();
        assert!(validate_switchover_admission(&operation).is_err());
    }

    #[tokio::test]
    async fn runtime_reuses_one_host_per_process_execution() {
        let runtime = DurableSwitchoverRuntime::in_memory(InMemoryCheckpointStore::new());
        let reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let first = runtime
            .host("tenant-a", "database", "set-uid", &reference)
            .await
            .unwrap();
        let second = runtime
            .host("tenant-a", "database", "set-uid", &reference)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        runtime
            .forget("tenant-a", "database", "set-uid", &reference.execution_id)
            .await;
        let replacement = runtime
            .host("tenant-a", "database", "set-uid", &reference)
            .await
            .unwrap();
        assert!(!Arc::ptr_eq(&first, &replacement));
    }

    #[tokio::test]
    async fn host_cache_identity_includes_owner_coordinates() {
        let runtime = DurableSwitchoverRuntime::in_memory(InMemoryCheckpointStore::new());
        let reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let first = runtime
            .host("tenant-a", "database", "set-uid", &reference)
            .await
            .unwrap();
        let other_owner = runtime
            .host("tenant-b", "database", "other-uid", &reference)
            .await
            .unwrap();
        assert!(!Arc::ptr_eq(&first, &other_owner));
    }

    struct PollCountingSwitchoverWorkflow {
        polls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Workflow for PollCountingSwitchoverWorkflow {
        async fn run(
            &self,
            context: &mut WorkflowContext<'_>,
            input: ExactBytes,
        ) -> TerminalOutcome {
            self.polls.fetch_add(1, Ordering::SeqCst);
            DurableSwitchoverWorkflow.run(context, input).await
        }
    }

    async fn expose_next<W: Workflow>(
        host: &mut SwitchoverHost,
        workflow: &W,
        execution: &ExecutionSpec,
    ) -> kuberic_durable_execution::DispatchPermit {
        assert!(matches!(
            {
                let outcome = host.turn(workflow, execution.clone()).await;
                host.store().correlate_host_outcome(&outcome);
                outcome
            },
            kuberic_durable_execution::HostOutcome::ScheduleAccepted { .. }
        ));
        let outcome = host.turn(workflow, execution.clone()).await;
        host.store().correlate_host_outcome(&outcome);
        match outcome {
            kuberic_durable_execution::HostOutcome::DispatchPermitted { permit, .. } => permit,
            other => panic!("expected dispatch permit, found {other:?}"),
        }
    }

    #[tokio::test]
    async fn workflow_replays_steps_and_reloads_terminal_without_polling() {
        let mut reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let mut initial = initial_operation(&reference).unwrap();
        initial.phase = crate::crd::DurableOperationPhase::LabelOldSecondary;
        reference.initial_operation_json = serde_json::to_string(&initial).unwrap();
        let execution = execution_spec(&reference).unwrap();
        let store = InMemoryCheckpointStore::new();
        let measured = MeasuredDurableCheckpointStore::new(
            execution.execution_id(),
            DurableCheckpointStore::InMemory(store),
        );
        let mut host = DurableHost::new(
            measured,
            HostEpoch::from_bytes([7; 16]),
            checkpoint_limits(),
        );
        let polls = Arc::new(AtomicUsize::new(0));
        let workflow = PollCountingSwitchoverWorkflow {
            polls: polls.clone(),
        };

        let first_permit = expose_next(&mut host, &workflow, &execution).await;
        let mut advanced = initial.clone();
        advanced.phase = crate::crd::DurableOperationPhase::Finalize;
        let outcome = host
            .observe(
                &execution,
                ActivityObservation::new(
                    first_permit.activity().clone(),
                    encode_step_result(&DurableSwitchoverStepResult::Advance {
                        operation: compact(&advanced),
                    })
                    .unwrap(),
                ),
            )
            .await;
        host.store().correlate_host_outcome(&outcome);
        assert!(matches!(
            outcome,
            kuberic_durable_execution::HostOutcome::ObservationAccepted { .. }
        ));

        let second_permit = expose_next(&mut host, &workflow, &execution).await;
        advanced.phase = crate::crd::DurableOperationPhase::Completed;
        let terminal_snapshot = advanced.target_snapshot.clone();
        let observed = host
            .observe(
                &execution,
                ActivityObservation::new(
                    second_permit.activity().clone(),
                    encode_step_result(&DurableSwitchoverStepResult::Complete {
                        operation: compact(&advanced),
                        snapshot: terminal_snapshot.clone(),
                        compensated: false,
                        accounting: Some(TestActivityAccounting::new(1, 1)),
                    })
                    .unwrap(),
                ),
            )
            .await;
        host.store().correlate_host_outcome(&observed);
        assert!(matches!(
            observed,
            kuberic_durable_execution::HostOutcome::ObservationAccepted { .. }
        ));

        let completed = host.turn(&workflow, execution.clone()).await;
        host.store().correlate_host_outcome(&completed);
        let kuberic_durable_execution::HostOutcome::WorkflowCompleted { outcome, .. } = completed
        else {
            panic!("expected workflow completion, found {completed:?}");
        };
        assert_eq!(
            decode_terminal(&outcome, &initial).unwrap(),
            TestTerminal::Complete {
                operation: advanced,
                snapshot: terminal_snapshot,
                compensated: false,
                accounting: TestActivityAccounting::new(1, 1),
            }
        );
        let polls_before_terminal_reload = polls.load(Ordering::SeqCst);
        assert!(matches!(
            host.turn(&workflow, execution).await,
            kuberic_durable_execution::HostOutcome::WorkflowCompleted { .. }
        ));
        assert_eq!(
            polls.load(Ordering::SeqCst),
            polls_before_terminal_reload,
            "terminal reload must not poll workflow code"
        );
        let measurements = host.store().measurements();
        assert_eq!(measurements.accepted_writes, 7);
        assert!(measurements.latest_authoritative_checkpoint_bytes.is_some());
        assert!(
            host.store()
                .collector()
                .events()
                .iter()
                .all(|event| event.boundary.is_some())
        );
    }

    #[tokio::test]
    async fn exposed_activity_is_quarantined_after_host_restart() {
        let reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let execution = execution_spec(&reference).unwrap();
        let backend = InMemoryCheckpointStore::new();
        let mut first_host = DurableHost::new(
            MeasuredDurableCheckpointStore::new(
                execution.execution_id(),
                DurableCheckpointStore::InMemory(backend.clone()),
            ),
            HostEpoch::from_bytes([3; 16]),
            checkpoint_limits(),
        );
        let workflow = DurableSwitchoverWorkflow;
        let permit = expose_next(&mut first_host, &workflow, &execution).await;

        let mut restarted_host = DurableHost::new(
            MeasuredDurableCheckpointStore::new(
                execution.execution_id(),
                DurableCheckpointStore::InMemory(backend),
            ),
            HostEpoch::from_bytes([4; 16]),
            checkpoint_limits(),
        );
        match restarted_host.turn(&workflow, execution).await {
            kuberic_durable_execution::HostOutcome::Quarantined { activity, .. } => {
                assert_eq!(&activity, permit.activity());
            }
            other => panic!("expected quarantine after restart, found {other:?}"),
        }
    }

    #[tokio::test]
    async fn stopped_step_becomes_failed_terminal() {
        let reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let execution = execution_spec(&reference).unwrap();
        let operation = initial_operation(&reference).unwrap();
        let store = InMemoryCheckpointStore::new();
        let mut host = DurableHost::new(
            MeasuredDurableCheckpointStore::new(
                execution.execution_id(),
                DurableCheckpointStore::InMemory(store),
            ),
            HostEpoch::from_bytes([8; 16]),
            checkpoint_limits(),
        );
        let workflow = DurableSwitchoverWorkflow;
        let permit = expose_next(&mut host, &workflow, &execution).await;
        let mut stopped = operation.clone();
        stopped.phase = crate::crd::DurableOperationPhase::Poisoned;
        stopped.pending_action = None;
        assert!(matches!(
            host.observe(
                &execution,
                ActivityObservation::new(
                    permit.activity().clone(),
                    encode_step_result(&DurableSwitchoverStepResult::Stopped {
                        operation: compact(&stopped),
                        message: "ambiguous effect remains quarantined".to_string(),
                    })
                    .unwrap(),
                ),
            )
            .await,
            kuberic_durable_execution::HostOutcome::ObservationAccepted { .. }
        ));
        let completed = host.turn(&workflow, execution).await;
        let kuberic_durable_execution::HostOutcome::WorkflowCompleted { outcome, .. } = completed
        else {
            panic!("expected stopped terminal, found {completed:?}");
        };
        assert!(matches!(outcome, TerminalOutcome::Failed(_)));
        assert_eq!(
            decode_terminal(&outcome, &operation).unwrap(),
            TestTerminal::Stopped {
                operation: Some(stopped),
                message: "ambiguous effect remains quarantined".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn permit_guard_consumes_matching_permit_once() {
        let reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let execution = execution_spec(&reference).unwrap();
        let operation = initial_operation(&reference).unwrap();
        let mut host = DurableHost::new(
            MeasuredDurableCheckpointStore::new(
                execution.execution_id(),
                DurableCheckpointStore::InMemory(InMemoryCheckpointStore::new()),
            ),
            HostEpoch::from_bytes([12; 16]),
            checkpoint_limits(),
        );
        let permit = expose_next(&mut host, &DurableSwitchoverWorkflow, &execution).await;
        let prepared = decode_switchover_activity_input(permit.activity().input()).unwrap();
        let current = prepared.state.apply_to(&operation).unwrap();
        let activity = permit.activity().clone();
        let attempt_id = permit.attempt_id();
        let mut guard = TestPermitGuard::new(permit);
        assert!(guard.activity().is_some());
        let wrong_attempt =
            kuberic_durable_execution::AttemptId::new(HostEpoch::from_bytes([99; 16]), 1).unwrap();
        assert!(
            guard
                .consume_for(&current, &prepared.kind, &activity, wrong_attempt)
                .is_err()
        );
        let wrong_kind = TestActivityKind::PreparedLabel {
            command: LabelEffectCommand::new(
                1,
                "database-0".to_string(),
                "pod-1-uid".to_string(),
                "primary".to_string(),
            ),
        };
        assert!(
            guard
                .consume_for(&current, &wrong_kind, &activity, attempt_id)
                .is_err()
        );
        let consumed = guard
            .consume_for(&current, &prepared.kind, &activity, attempt_id)
            .unwrap();
        assert_eq!(
            consumed.activity().input().as_slice(),
            encode_activity_input::<DurableSwitchoverActivity>(&DurableSwitchoverActivityInput {
                version: SWITCHOVER_CONTRACT_VERSION,
                state: compact(&current),
                kind: prepared.kind.clone(),
            })
            .unwrap()
            .as_slice()
        );
        assert!(
            guard
                .consume_for(&current, &prepared.kind, &activity, attempt_id)
                .is_err()
        );
    }

    #[test]
    fn prepared_replica_spec_binds_every_dispatch_fence() {
        let reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let operation = initial_operation(&reference).unwrap();
        let command = ReplicaEffectCommand {
            action_id: "execution:1:RevokeWrite".to_string(),
            action_signature: kuberic_core::types::DurableReplicaAction::RevokeWriteStatus
                .signature(),
            target_id: 1,
            target_instance_id: "pod-1-uid".to_string(),
            expected_epoch: EpochStatus {
                data_loss_number: 4,
                configuration_number: 8,
            },
            desired_postcondition: crate::crd::DurablePostconditionStatus {
                kind: crate::crd::DurablePostconditionKind::WriteRevoked,
                role: None,
            },
            expected_agent_generation: "0123456789abcdef0123456789abcdef".to_string(),
            expected_control_version: 7,
            observed_runtime_epoch: EpochStatus {
                data_loss_number: 4,
                configuration_number: 8,
            },
            action_payload: kuberic_core::grpc::convert::encode_direct_correlated_action_payload(
                &kuberic_core::types::DurableReplicaAction::RevokeWriteStatus,
            )
            .unwrap(),
        };
        let input = |command| DurableSwitchoverActivityInput {
            version: SWITCHOVER_CONTRACT_VERSION,
            state: compact(&operation),
            kind: TestActivityKind::PreparedReplica { command },
        };
        let baseline = activity_spec(&input(command.clone())).unwrap();
        let mut variants = Vec::new();
        let mut changed = command.clone();
        changed.action_id.push_str("-other");
        variants.push(changed);
        let mut changed = command.clone();
        changed.action_signature.push_str("-other");
        variants.push(changed);
        let mut changed = command.clone();
        changed.target_id = 2;
        variants.push(changed);
        let mut changed = command.clone();
        changed.target_instance_id = "replacement-uid".to_string();
        variants.push(changed);
        let mut changed = command.clone();
        changed.expected_epoch.configuration_number += 1;
        variants.push(changed);
        let mut changed = command.clone();
        changed.desired_postcondition.kind = crate::crd::DurablePostconditionKind::Role;
        variants.push(changed);
        let mut changed = command.clone();
        changed.expected_agent_generation = "fedcba9876543210fedcba9876543210".to_string();
        variants.push(changed);
        let mut changed = command.clone();
        changed.expected_control_version += 1;
        variants.push(changed);
        let mut changed = command.clone();
        changed.observed_runtime_epoch.configuration_number += 1;
        variants.push(changed);
        let mut changed = command;
        changed.action_payload =
            kuberic_core::grpc::convert::encode_direct_correlated_action_payload(
                &kuberic_core::types::DurableReplicaAction::Close,
            )
            .unwrap();
        variants.push(changed);

        for changed in variants {
            assert_ne!(activity_spec(&input(changed)).unwrap(), baseline);
        }
    }

    #[test]
    fn restart_validation_rejects_consistently_rewritten_role_and_epoch_payloads() {
        use kuberic_core::types::{DurableReplicaAction, Epoch, Role};

        let reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let mut operation = initial_operation(&reference).unwrap();
        operation.phase = DurableOperationPhase::PromoteTarget;
        let target = operation
            .target_snapshot
            .members
            .iter()
            .find(|member| member.id == operation.target_primary_id)
            .unwrap();
        let target_epoch = Epoch {
            data_loss_number: operation.target_snapshot.epoch.data_loss_number,
            configuration_number: operation.target_snapshot.epoch.configuration_number,
        };
        let mut pending = PendingActionStatus {
            action_id: "execution:3:PromoteTarget".to_string(),
            sequence: 3,
            kind: crate::crd::DurableActionKind::PromoteTarget,
            target_id: target.id,
            target_instance_id: target.instance_id.clone(),
            expected_epoch: operation.previous_snapshot.epoch.clone(),
            desired_postcondition: crate::crd::DurablePostconditionStatus {
                kind: crate::crd::DurablePostconditionKind::Role,
                role: Some(crate::crd::StableReplicaRoleStatus::Primary),
            },
            attempts: 0,
            deadline_unix_seconds: 100,
            last_error: None,
            dispatch_authorized: true,
            dispatch_agent_generation: Some("0123456789abcdef0123456789abcdef".to_string()),
            dispatch_agent_control_version: Some(7),
            dispatch_observed_runtime_epoch: Some(operation.previous_snapshot.epoch.clone()),
            dispatch_action_payload:
                kuberic_core::grpc::convert::encode_direct_correlated_action_payload(
                    &DurableReplicaAction::ChangeRole {
                        epoch: target_epoch,
                        role: Role::Primary,
                    },
                )
                .unwrap(),
        };
        operation.pending_action = Some(pending.clone());
        validate_prepared_activity(
            &operation,
            &TestActivityKind::PreparedReplica {
                command: ReplicaEffectCommand::from_pending(&pending).unwrap(),
            },
        )
        .unwrap();

        for rewritten in [
            DurableReplicaAction::ChangeRole {
                epoch: Epoch {
                    configuration_number: target_epoch.configuration_number + 1,
                    ..target_epoch
                },
                role: Role::Primary,
            },
            DurableReplicaAction::ChangeRole {
                epoch: target_epoch,
                role: Role::ActiveSecondary,
            },
        ] {
            pending.dispatch_action_payload =
                kuberic_core::grpc::convert::encode_direct_correlated_action_payload(&rewritten)
                    .unwrap();
            operation.pending_action = Some(pending.clone());
            assert!(
                validate_prepared_activity(
                    &operation,
                    &TestActivityKind::PreparedReplica {
                        command: ReplicaEffectCommand::from_pending(&pending).unwrap(),
                    },
                )
                .is_err(),
                "restart validation accepted rewritten fixed role/epoch semantics"
            );
        }
    }

    #[test]
    fn replay_rejects_recorded_command_semantic_drift_as_activity_mismatch() {
        let reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let initial = initial_operation(&reference).unwrap();
        let TestAdapterDecision::Observe(result) =
            evaluate_adapter_step(&initial, &OperationObservations::new(), 100).unwrap()
        else {
            panic!("expected revoke intent");
        };
        let DurableSwitchoverStepResult::Advance { operation: state } = *result else {
            panic!("expected pending revoke state");
        };
        let logical = activity_spec(&DurableSwitchoverActivityInput {
            version: SWITCHOVER_CONTRACT_VERSION,
            state: state.clone(),
            kind: TestActivityKind::PassiveObservation,
        })
        .unwrap();
        let mut prepared_operation = state.apply_to(&initial).unwrap();
        let pending = prepared_operation.pending_action.as_mut().unwrap();
        pending.dispatch_agent_generation = Some("0123456789abcdef0123456789abcdef".to_string());
        pending.dispatch_agent_control_version = Some(7);
        pending.dispatch_observed_runtime_epoch = Some(EpochStatus {
            data_loss_number: 4,
            configuration_number: 8,
        });
        pending.dispatch_action_payload =
            kuberic_core::grpc::convert::encode_direct_correlated_action_payload(
                &kuberic_core::types::DurableReplicaAction::RevokeWriteStatus,
            )
            .unwrap();
        let command = ReplicaEffectCommand::from_pending(pending).unwrap();
        let prepared = activity_spec(&DurableSwitchoverActivityInput {
            version: SWITCHOVER_CONTRACT_VERSION,
            state: compact(&prepared_operation),
            kind: TestActivityKind::PreparedReplica {
                command: command.clone(),
            },
        })
        .unwrap();
        validate_prepared_activity(
            &prepared_operation,
            &TestActivityKind::PreparedReplica {
                command: command.clone(),
            },
        )
        .unwrap();
        let observations = OperationObservations::new();
        let addressed = BTreeMap::new();
        let resolver = TestPreparedActivityResolver::new(&initial, &observations, &addressed, 100);
        assert_eq!(
            resolver.resolve(&logical, Some(&prepared)).unwrap(),
            prepared
        );

        let execution = execution_spec(&reference).unwrap();
        let mut variants = Vec::new();
        let mut changed = command.clone();
        changed.action_id.push_str("-other");
        variants.push(changed);
        let mut changed = command.clone();
        changed.action_signature.push_str("-other");
        variants.push(changed);
        let mut changed = command.clone();
        changed.target_id = 2;
        variants.push(changed);
        let mut changed = command.clone();
        changed.target_instance_id = "replacement-uid".to_string();
        variants.push(changed);
        let mut changed = command.clone();
        changed.expected_epoch.configuration_number += 1;
        variants.push(changed);
        let mut changed = command.clone();
        changed.desired_postcondition.kind = crate::crd::DurablePostconditionKind::Role;
        variants.push(changed);
        let mut changed = command.clone();
        changed.expected_agent_generation = "fedcba9876543210fedcba9876543210".to_string();
        variants.push(changed);
        let mut changed = command.clone();
        changed.expected_control_version += 1;
        variants.push(changed);
        let mut changed = command.clone();
        changed.observed_runtime_epoch.configuration_number += 1;
        variants.push(changed);
        let mut changed = command;
        changed.action_payload =
            kuberic_core::grpc::convert::encode_direct_correlated_action_payload(
                &kuberic_core::types::DurableReplicaAction::Close,
            )
            .unwrap();
        variants.push(changed);

        for changed in variants {
            assert!(
                validate_prepared_activity(
                    &prepared_operation,
                    &TestActivityKind::PreparedReplica {
                        command: changed.clone(),
                    },
                )
                .is_err(),
                "restart validation must reject any changed prepared command"
            );
            let drifted = activity_spec(&DurableSwitchoverActivityInput {
                version: SWITCHOVER_CONTRACT_VERSION,
                state: compact(&prepared_operation),
                kind: TestActivityKind::PreparedReplica { command: changed },
            })
            .unwrap();
            assert_eq!(resolver.resolve(&logical, Some(&drifted)).unwrap(), logical);
            let payload = CheckpointPayload::active(
                ExecutionContract::with_encoded_limits(
                    execution.clone(),
                    u64::try_from(SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES).unwrap(),
                    u64::try_from(SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES).unwrap(),
                ),
                vec![ActivityRecord::scheduled(ActivitySequence::new(0), drifted)],
            );
            let checkpoint =
                CheckpointEnvelope::encode_with_limits(&payload, checkpoint_limits()).unwrap();
            assert!(matches!(
                kuberic_durable_execution::evaluate_prepared(
                    &DurableSwitchoverWorkflow,
                    &execution,
                    Some(&checkpoint),
                    checkpoint_limits(),
                    &resolver,
                ),
                kuberic_durable_execution::Evaluation::Nondeterminism(
                    kuberic_durable_execution::Nondeterminism::ActivityMismatch { .. }
                )
            ));
        }
    }

    #[test]
    fn replay_rejects_changed_activity_name_version_and_result_bound() {
        let reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let initial = initial_operation(&reference).unwrap();
        let logical = activity_spec(&DurableSwitchoverActivityInput {
            version: SWITCHOVER_CONTRACT_VERSION,
            state: compact(&initial),
            kind: TestActivityKind::PassiveObservation,
        })
        .unwrap();
        let observations = OperationObservations::new();
        let addressed = BTreeMap::new();
        let resolver = TestPreparedActivityResolver::new(&initial, &observations, &addressed, 100);
        assert_eq!(resolver.resolve(&logical, Some(&logical)).unwrap(), logical);

        let changed_name = ActivitySpec::new(
            ActivityName::new("kuberic.switchover.changed-name", logical.name().version()).unwrap(),
            logical.input().clone(),
            logical.max_result_bytes(),
        );
        let changed_version = ActivitySpec::new(
            ActivityName::new(
                logical.name().name(),
                logical.name().version().saturating_add(1),
            )
            .unwrap(),
            logical.input().clone(),
            logical.max_result_bytes(),
        );
        let changed_result_bound = ActivitySpec::new(
            logical.name().clone(),
            logical.input().clone(),
            logical.max_result_bytes().saturating_add(1),
        );
        let execution = execution_spec(&reference).unwrap();

        for changed in [changed_name, changed_version, changed_result_bound] {
            assert_eq!(resolver.resolve(&logical, Some(&changed)).unwrap(), logical);
            let payload = CheckpointPayload::active(
                ExecutionContract::with_encoded_limits(
                    execution.clone(),
                    u64::try_from(SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES).unwrap(),
                    u64::try_from(SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES).unwrap(),
                ),
                vec![ActivityRecord::scheduled(ActivitySequence::new(0), changed)],
            );
            let checkpoint =
                CheckpointEnvelope::encode_with_limits(&payload, checkpoint_limits()).unwrap();
            assert!(matches!(
                kuberic_durable_execution::evaluate_prepared(
                    &DurableSwitchoverWorkflow,
                    &execution,
                    Some(&checkpoint),
                    checkpoint_limits(),
                    &resolver,
                ),
                kuberic_durable_execution::Evaluation::Nondeterminism(
                    kuberic_durable_execution::Nondeterminism::ActivityMismatch { .. }
                )
            ));
        }
    }

    #[test]
    fn adapter_invokes_existing_switchover_decision_engine() {
        let reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let operation = initial_operation(&reference).unwrap();
        let observations = OperationObservations::new();
        let TestAdapterDecision::Observe(result) =
            evaluate_adapter_step(&operation, &observations, 100).unwrap()
        else {
            panic!("initial switchover decision must persist the correlated revoke action");
        };
        let DurableSwitchoverStepResult::Advance { operation: pending } = *result else {
            panic!("initial switchover decision must advance compact state");
        };
        let pending = pending.apply_to(&operation).unwrap();
        assert!(pending.pending_action.is_some());
        assert!(matches!(
            evaluate_adapter_step(&pending, &observations, 100).unwrap(),
            TestAdapterDecision::AwaitEvidence
        ));
    }

    #[test]
    fn adapter_turns_effect_free_poison_into_stopped_result() {
        let reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let initial = initial_operation(&reference).unwrap();
        let TestAdapterDecision::Observe(result) =
            evaluate_adapter_step(&initial, &OperationObservations::new(), 100).unwrap()
        else {
            panic!("expected persisted revoke intent");
        };
        let DurableSwitchoverStepResult::Advance { operation: pending } = *result else {
            panic!("expected compact revoke state");
        };
        let pending = pending.apply_to(&initial).unwrap();
        let TestAdapterDecision::Observe(result) =
            evaluate_adapter_step(&pending, &OperationObservations::new(), 1_000).unwrap()
        else {
            panic!("expired operation did not produce an observation");
        };
        assert!(matches!(
            *result,
            DurableSwitchoverStepResult::Stopped { .. }
        ));
    }

    #[tokio::test]
    async fn proven_no_admission_redelivery_is_bounded_per_action() {
        let mut reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let initial = initial_operation(&reference).unwrap();
        let TestAdapterDecision::Observe(result) =
            evaluate_adapter_step(&initial, &OperationObservations::new(), 100).unwrap()
        else {
            panic!("expected persisted revoke action");
        };
        let DurableSwitchoverStepResult::Advance { operation: pending } = *result else {
            panic!("expected compact revoke state");
        };
        let pending = pending.apply_to(&initial).unwrap();
        let action_id = pending.pending_action.as_ref().unwrap().action_id.clone();
        reference.initial_operation_json = serde_json::to_string(&pending).unwrap();
        let execution = execution_spec(&reference).unwrap();
        let mut host = DurableHost::new(
            MeasuredDurableCheckpointStore::new(
                execution.execution_id(),
                DurableCheckpointStore::InMemory(InMemoryCheckpointStore::new()),
            ),
            HostEpoch::from_bytes([13; 16]),
            checkpoint_limits(),
        );
        let workflow = DurableSwitchoverWorkflow;

        for _ in 0..2 {
            let permit = expose_next(&mut host, &workflow, &execution).await;
            let observed = host
                .observe(
                    &execution,
                    ActivityObservation::new(
                        permit.activity().clone(),
                        encode_step_result(&DurableSwitchoverStepResult::ProvenNoAdmission {
                            operation: compact(&pending),
                            action_id: action_id.clone(),
                            redelivery: 1,
                        })
                        .unwrap(),
                    ),
                )
                .await;
            assert!(matches!(
                observed,
                kuberic_durable_execution::HostOutcome::ObservationAccepted { .. }
            ));
        }
        let completed = host.turn(&workflow, execution).await;
        let kuberic_durable_execution::HostOutcome::WorkflowCompleted { outcome, .. } = completed
        else {
            panic!("second no-admission redelivery must stop, found {completed:?}");
        };
        assert!(matches!(outcome, TerminalOutcome::Failed(_)));
    }

    #[tokio::test]
    async fn compensation_branch_terminalizes_failed_old_primary_topology() {
        let mut reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let mut failed = initial_operation(&reference).unwrap();
        failed.phase = crate::crd::DurableOperationPhase::Failed;
        reference.initial_operation_json = serde_json::to_string(&failed).unwrap();
        let execution = execution_spec(&reference).unwrap();
        let mut host = DurableHost::new(
            MeasuredDurableCheckpointStore::new(
                execution.execution_id(),
                DurableCheckpointStore::InMemory(InMemoryCheckpointStore::new()),
            ),
            HostEpoch::from_bytes([14; 16]),
            checkpoint_limits(),
        );
        let workflow = DurableSwitchoverWorkflow;
        let permit = expose_next(&mut host, &workflow, &execution).await;
        let previous = failed.previous_snapshot.cloned().unwrap();
        assert!(matches!(
            host.observe(
                &execution,
                ActivityObservation::new(
                    permit.activity().clone(),
                    encode_step_result(&DurableSwitchoverStepResult::Complete {
                        operation: compact(&failed),
                        snapshot: previous.clone(),
                        compensated: true,
                        accounting: Some(TestActivityAccounting::new(0, 1)),
                    })
                    .unwrap(),
                ),
            )
            .await,
            kuberic_durable_execution::HostOutcome::ObservationAccepted { .. }
        ));
        let kuberic_durable_execution::HostOutcome::WorkflowCompleted { outcome, .. } =
            host.turn(&workflow, execution).await
        else {
            panic!("expected compensated terminal");
        };
        assert_eq!(
            decode_terminal(&outcome, &failed).unwrap(),
            TestTerminal::Complete {
                operation: failed,
                snapshot: previous,
                compensated: true,
                accounting: TestActivityAccounting::new(0, 1),
            }
        );
    }

    #[tokio::test]
    async fn malformed_workflow_input_fails_without_scheduling_activity() {
        let execution = ExecutionSpec::new(
            ExecutionId::from_bytes([11; 16]),
            ExactBytes::new(b"not-json"),
            SWITCHOVER_MAX_TERMINAL_PAYLOAD_BYTES,
        );
        let mut host = DurableHost::new(
            MeasuredDurableCheckpointStore::new(
                execution.execution_id(),
                DurableCheckpointStore::InMemory(InMemoryCheckpointStore::new()),
            ),
            HostEpoch::from_bytes([9; 16]),
            checkpoint_limits(),
        );
        let outcome = host.turn(&DurableSwitchoverWorkflow, execution).await;
        let kuberic_durable_execution::HostOutcome::WorkflowCompleted { outcome, .. } = outcome
        else {
            panic!("malformed input must fail terminally, found {outcome:?}");
        };
        assert!(matches!(outcome, TerminalOutcome::Failed(_)));
        assert!(
            serde_json::from_slice::<serde_json::Value>(outcome.payload().as_slice()).unwrap()["status"]
                == "stopped"
        );
    }

    #[tokio::test]
    async fn malformed_distribution_cursor_cannot_skip_epoch_updates() {
        let mut reference = test_reference("set-uid", snapshot(4), 2, 100).unwrap();
        let mut initial = initial_operation(&reference).unwrap();
        initial.phase = DurableOperationPhase::DistributeEpoch;
        reference.initial_operation_json = serde_json::to_string(&initial).unwrap();
        let execution = execution_spec(&reference).unwrap();
        let mut host = DurableHost::new(
            MeasuredDurableCheckpointStore::new(
                execution.execution_id(),
                DurableCheckpointStore::InMemory(InMemoryCheckpointStore::new()),
            ),
            HostEpoch::from_bytes([29; 16]),
            checkpoint_limits(),
        );
        let workflow = DurableSwitchoverWorkflow;
        let permit = expose_next(&mut host, &workflow, &execution).await;
        let mut skipped = decode_activity_input_state(permit.activity().input()).unwrap();
        assert_eq!(skipped.phase, DurableOperationPhase::DistributeEpoch);
        assert_eq!(skipped.next_secondary_index, 0);
        skipped.phase = DurableOperationPhase::UpdateCatchUpConfiguration;
        skipped.pending_action = None;
        assert!(matches!(
            host.observe(
                &execution,
                ActivityObservation::new(
                    permit.activity().clone(),
                    encode_step_result(&DurableSwitchoverStepResult::Advance {
                        operation: skipped,
                    })
                    .unwrap(),
                ),
            )
            .await,
            kuberic_durable_execution::HostOutcome::ObservationAccepted { .. }
        ));
        let kuberic_durable_execution::HostOutcome::WorkflowCompleted { outcome, .. } =
            host.turn(&workflow, execution).await
        else {
            panic!("malformed cursor replay must fail terminally");
        };
        assert!(matches!(outcome, TerminalOutcome::Failed(_)));
        assert!(
            String::from_utf8_lossy(outcome.payload().as_slice())
                .contains("before completing 2 secondary epoch updates")
        );
    }

    #[test]
    fn structurally_invalid_initial_operation_fails_before_activity_schedule() {
        let mut reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let mut invalid = initial_operation(&reference).unwrap();
        invalid.version = u32::MAX;
        reference.initial_operation_json = serde_json::to_string(&invalid).unwrap();
        let error = execution_spec(&reference).unwrap_err();
        assert!(
            error.contains("unsupported durable operation version"),
            "{error}"
        );
    }

    #[test]
    fn native_reference_rejects_invalid_authority_and_acceptance_time() {
        let reference = new_switchover_execution("set-uid", snapshot(3), 2, 100).unwrap();
        assert!(validate_native_operation_authority(&reference, "set-uid").is_ok());
        assert!(validate_native_operation_authority(&reference, "other-uid").is_err());

        let mut malformed = reference.clone();
        malformed.input.operation_authority.clear();
        assert!(native_execution_spec(&malformed).is_err());

        for accepted_unix_seconds in [0, -1, i64::MAX] {
            let mut malformed = reference.clone();
            malformed.input.accepted_unix_seconds = accepted_unix_seconds;
            assert!(
                native_execution_spec(&malformed).is_err(),
                "accepted invalid timestamp {accepted_unix_seconds}"
            );
        }
    }

    #[test]
    fn malformed_secondary_cursors_fail_before_effects() {
        let reference = new_switchover_execution("set-uid", snapshot(3), 2, 100).unwrap();
        let initial = native_initial_operation(&reference).unwrap();

        let mut jumped = initial.clone();
        jumped.next_secondary_index = 1;
        assert!(validate_cursor_transition(&initial, &jumped).is_err());

        let mut skipped_distribution = initial.clone();
        skipped_distribution.phase = DurableOperationPhase::DistributeEpoch;
        let mut skipped_to_configuration = skipped_distribution.clone();
        skipped_to_configuration.phase = DurableOperationPhase::UpdateCatchUpConfiguration;
        assert!(
            validate_cursor_transition(&skipped_distribution, &skipped_to_configuration).is_err()
        );

        let mut unrepresentable = initial.clone();
        unrepresentable.next_secondary_index = u32::MAX;
        assert!(validate_switchover_operation_contract(&unrepresentable).is_err());

        let mut distributing = initial.clone();
        distributing.phase = DurableOperationPhase::DistributeEpoch;
        let Decision::Persist(mut pending_distribution) =
            decide(&distributing, &OperationObservations::default(), 100).unwrap()
        else {
            panic!("distribution must prepare a retained-member epoch action");
        };
        pending_distribution.next_secondary_index = 1;
        assert!(validate_switchover_operation_contract(&pending_distribution).is_err());

        let mut compensating = initial;
        compensating.phase = DurableOperationPhase::CompensateDistributeEpoch;
        let Decision::Persist(mut pending_compensation) =
            decide(&compensating, &OperationObservations::default(), 100).unwrap()
        else {
            panic!("compensation must prepare a secondary epoch action");
        };
        pending_compensation.next_secondary_index = 2;
        assert!(validate_switchover_operation_contract(&pending_compensation).is_err());
    }

    #[test]
    fn accounting_diagnostic_uses_member_specific_redelivery_range() {
        let terminal = TerminalAccountingContext {
            phase: DurableOperationPhase::Completed,
            frozen_lsn: Some(10),
            next_secondary_index: 2,
            has_pending_action: false,
            has_last_error: false,
        };
        let error = validate_terminal_accounting_shape(
            terminal,
            4,
            false,
            SwitchoverActivityAccounting::new(20, 3),
            23,
        )
        .unwrap_err();
        assert!(error.contains("10..=18 external effects"), "{error}");
    }

    #[test]
    fn codecs_reject_identity_drift_and_oversized_results() {
        let reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let initial = initial_operation(&reference).unwrap();
        let mut drifted = initial.clone();
        drifted.operation_id.push_str("-other");
        assert!(validate_transition(&initial, &drifted).is_err());

        let mut oversized = initial;
        oversized.last_error = Some("x".repeat(SWITCHOVER_MAX_ACTIVITY_RESULT_BYTES));
        let error = encode_step_result(&DurableSwitchoverStepResult::Advance {
            operation: compact(&oversized),
        })
        .unwrap_err();
        assert!(error.contains("activity result is"), "{error}");
    }

    #[test]
    fn compact_activity_payloads_exclude_immutable_operation_snapshots() {
        let reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let operation = initial_operation(&reference).unwrap();
        let state = compact(&operation);
        let input =
            encode_activity_input::<DurableSwitchoverActivity>(&DurableSwitchoverActivityInput {
                version: SWITCHOVER_CONTRACT_VERSION,
                state: state.clone(),
                kind: TestActivityKind::PassiveObservation,
            })
            .unwrap();
        let result =
            encode_step_result(&DurableSwitchoverStepResult::Advance { operation: state }).unwrap();
        for payload in [input, result] {
            let text = std::str::from_utf8(payload.as_slice()).unwrap();
            assert!(!text.contains("previousSnapshot"));
            assert!(!text.contains("targetSnapshot"));
            assert!(!text.contains("operationId"));
            assert!(!text.contains("executionId"));
        }
    }

    #[test]
    fn effect_free_pending_creation_advances_without_an_activity() {
        let reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let operation = initial_operation(&reference).unwrap();
        let pending = advance_effect_free(&operation).unwrap().unwrap();
        assert_eq!(pending.phase, operation.phase);
        assert!(pending.pending_action.is_some());
        assert!(advance_effect_free(&pending).unwrap().is_none());
    }

    #[tokio::test]
    async fn same_state_activity_result_stops_before_consuming_history_capacity() {
        let mut reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let mut initial = initial_operation(&reference).unwrap();
        initial.phase = DurableOperationPhase::CaptureLsn;
        reference.initial_operation_json = serde_json::to_string(&initial).unwrap();
        let execution = execution_spec(&reference).unwrap();
        let mut host = DurableHost::new(
            MeasuredDurableCheckpointStore::new(
                execution.execution_id(),
                DurableCheckpointStore::InMemory(InMemoryCheckpointStore::new()),
            ),
            HostEpoch::from_bytes([31; 16]),
            checkpoint_limits(),
        );
        let permit = expose_next(&mut host, &DurableSwitchoverWorkflow, &execution).await;
        let current = decode_activity_input_state(permit.activity().input()).unwrap();
        assert!(matches!(
            host.observe(
                &execution,
                ActivityObservation::new(
                    permit.activity().clone(),
                    encode_step_result(&DurableSwitchoverStepResult::Advance {
                        operation: current,
                    })
                    .unwrap(),
                ),
            )
            .await,
            kuberic_durable_execution::HostOutcome::ObservationAccepted { .. }
        ));
        let kuberic_durable_execution::HostOutcome::WorkflowCompleted { outcome, .. } =
            host.turn(&DurableSwitchoverWorkflow, execution).await
        else {
            panic!("same-state transcript did not stop");
        };
        let TestTerminal::Stopped { message, .. } = decode_terminal(&outcome, &initial).unwrap()
        else {
            panic!("no-progress result did not produce stopped terminal");
        };
        assert!(message.contains("no protocol progress"));
    }

    #[test]
    fn compensated_terminal_round_trips_exactly() {
        let reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let operation = initial_operation(&reference).unwrap();
        let terminal = TestTerminal::Complete {
            snapshot: operation.previous_snapshot.cloned().unwrap(),
            operation: operation.clone(),
            compensated: true,
            accounting: TestActivityAccounting::new(10, 3),
        };
        let outcome = TerminalOutcome::succeeded(encode_terminal(&terminal).unwrap());
        assert_eq!(decode_terminal(&outcome, &operation).unwrap(), terminal);
    }

    #[test]
    fn loaded_terminal_revalidates_identity_topology_and_outcome_kind() {
        let reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let mut operation = initial_operation(&reference).unwrap();
        operation.phase = crate::crd::DurableOperationPhase::Completed;
        let valid = TestTerminal::Complete {
            snapshot: operation.target_snapshot.clone(),
            operation: operation.clone(),
            compensated: false,
            accounting: TestActivityAccounting::new(9, 3),
        };
        let success = TerminalOutcome::succeeded(encode_terminal(&valid).unwrap());
        assert_eq!(
            validate_loaded_terminal(&reference, &success, 12).unwrap(),
            valid
        );
        for accounting in [
            TestActivityAccounting::new(10, 3),
            TestActivityAccounting::new(16, 3),
        ] {
            let redelivered_success = TerminalOutcome::succeeded(
                encode_terminal(&TestTerminal::Complete {
                    snapshot: operation.target_snapshot.clone(),
                    operation: operation.clone(),
                    compensated: false,
                    accounting,
                })
                .unwrap(),
            );
            assert_eq!(
                validate_loaded_terminal(
                    &reference,
                    &redelivered_success,
                    accounting.external_effect_count + accounting.passive_observation_count,
                )
                .unwrap(),
                TestTerminal::Complete {
                    snapshot: operation.target_snapshot.clone(),
                    operation: operation.clone(),
                    compensated: false,
                    accounting,
                }
            );
        }
        for accounting in [
            TestActivityAccounting::new(10, 2),
            TestActivityAccounting::new(8, 3),
            TestActivityAccounting::new(17, 3),
        ] {
            let invalid_accounting = TerminalOutcome::succeeded(
                encode_terminal(&TestTerminal::Complete {
                    snapshot: operation.target_snapshot.clone(),
                    operation: operation.clone(),
                    compensated: false,
                    accounting,
                })
                .unwrap(),
            );
            assert!(
                validate_loaded_terminal(
                    &reference,
                    &invalid_accounting,
                    accounting.external_effect_count + accounting.passive_observation_count,
                )
                .is_err(),
                "successful terminal accepted invalid accounting {accounting:?}"
            );
        }

        let failed_kind = TerminalOutcome::failed(encode_terminal(&valid).unwrap());
        assert!(validate_loaded_terminal(&reference, &failed_kind, 12).is_err());

        let mut wrong_identity = operation.clone();
        wrong_identity.operation_id.push_str("-other");
        let wrong_identity = TerminalOutcome::succeeded(
            encode_terminal(&TestTerminal::Complete {
                snapshot: wrong_identity.target_snapshot.clone(),
                operation: wrong_identity,
                compensated: false,
                accounting: TestActivityAccounting::new(9, 3),
            })
            .unwrap(),
        );
        let canonical = validate_loaded_terminal(&reference, &wrong_identity, 12).unwrap();
        let TestTerminal::Complete {
            operation: canonical,
            ..
        } = canonical
        else {
            panic!("expected compact completed terminal");
        };
        assert_eq!(
            canonical.operation_id,
            initial_operation(&reference).unwrap().operation_id,
            "immutable identity comes only from the authoritative initial reference"
        );

        let mut wrong_snapshot = operation.target_snapshot.clone();
        wrong_snapshot.primary_id = operation.old_primary_id;
        let wrong_snapshot = TerminalOutcome::succeeded(
            encode_terminal(&TestTerminal::Complete {
                operation,
                snapshot: wrong_snapshot,
                compensated: false,
                accounting: TestActivityAccounting::new(9, 3),
            })
            .unwrap(),
        );
        assert!(validate_loaded_terminal(&reference, &wrong_snapshot, 12).is_err());

        let mut failed = initial_operation(&reference).unwrap();
        failed.phase = crate::crd::DurableOperationPhase::Failed;
        failed.frozen_lsn = Some(42);
        failed.next_secondary_index =
            u32::try_from(failed.previous_snapshot.members.len().saturating_sub(1)).unwrap();
        failed.last_error = None;
        let compensated = failed.previous_snapshot.cloned().unwrap();
        let valid_compensation = TestTerminal::Complete {
            operation: failed.clone(),
            snapshot: compensated.clone(),
            compensated: true,
            accounting: TestActivityAccounting::new(8, 5),
        };
        assert!(
            validate_loaded_terminal(
                &reference,
                &TerminalOutcome::succeeded(encode_terminal(&valid_compensation).unwrap()),
                13,
            )
            .is_ok()
        );
        let invalid_compensation = TerminalOutcome::succeeded(
            encode_terminal(&TestTerminal::Complete {
                operation: failed.clone(),
                snapshot: compensated.clone(),
                compensated: true,
                accounting: TestActivityAccounting::new(21, 3),
            })
            .unwrap(),
        );
        assert!(validate_loaded_terminal(&reference, &invalid_compensation, 24).is_err());
        let impossible_passive_split = TerminalOutcome::succeeded(
            encode_terminal(&TestTerminal::Complete {
                operation: failed.clone(),
                snapshot: compensated.clone(),
                compensated: true,
                accounting: TestActivityAccounting::new(1, 22),
            })
            .unwrap(),
        );
        assert!(validate_loaded_terminal(&reference, &impossible_passive_split, 23).is_err());
        let impossible_redelivery_split = TerminalOutcome::succeeded(
            encode_terminal(&TestTerminal::Complete {
                operation: failed.clone(),
                snapshot: compensated.clone(),
                compensated: true,
                accounting: TestActivityAccounting::new(10, 13),
            })
            .unwrap(),
        );
        assert!(validate_loaded_terminal(&reference, &impossible_redelivery_split, 23).is_err());

        let mut malformed = Vec::new();
        let mut missing = compensated.clone();
        missing.members.pop();
        malformed.push(missing);
        let mut unique_extra = compensated.clone();
        let mut foreign = unique_extra.members[0].clone();
        foreign.id = 99;
        foreign.instance_id = "foreign".to_string();
        unique_extra.members.push(foreign.clone());
        malformed.push(unique_extra);
        let mut same_length_duplicate = compensated.clone();
        same_length_duplicate.members[2] = same_length_duplicate.members[0].clone();
        malformed.push(same_length_duplicate);
        let mut same_length_unknown = compensated.clone();
        same_length_unknown.members[2] = foreign;
        malformed.push(same_length_unknown);
        let mut wrong_incarnation = compensated.clone();
        wrong_incarnation.members[0].instance_id = "replacement".to_string();
        malformed.push(wrong_incarnation);
        let mut wrong_quorum = compensated.clone();
        wrong_quorum.write_quorum = wrong_quorum.write_quorum.saturating_add(1);
        malformed.push(wrong_quorum);
        let mut wrong_epoch = compensated;
        wrong_epoch.epoch.configuration_number += 9;
        malformed.push(wrong_epoch);
        for snapshot in malformed {
            let outcome = TerminalOutcome::succeeded(
                encode_terminal(&TestTerminal::Complete {
                    operation: failed.clone(),
                    snapshot,
                    compensated: true,
                    accounting: TestActivityAccounting::new(8, 5),
                })
                .unwrap(),
            );
            assert!(validate_loaded_terminal(&reference, &outcome, 13).is_err());
        }

        assert!(
            validate_loaded_terminal(&reference, &success, 11).is_err(),
            "loaded terminal validation must compare accounting with the authoritative count"
        );
    }

    #[test]
    fn terminal_accounting_matches_exact_reachable_projection_pairs() {
        let successful = TerminalAccountingContext {
            phase: DurableOperationPhase::Completed,
            frozen_lsn: Some(42),
            next_secondary_index: 1,
            has_pending_action: false,
            has_last_error: false,
        };
        let restored = TerminalAccountingContext {
            phase: DurableOperationPhase::Failed,
            frozen_lsn: Some(42),
            next_secondary_index: 0,
            has_pending_action: false,
            has_last_error: false,
        };
        let fully_compensated = TerminalAccountingContext {
            next_secondary_index: 2,
            ..restored
        };
        let incomplete_compensation = TerminalAccountingContext {
            next_secondary_index: 1,
            ..restored
        };
        let uncompensated_failure = TerminalAccountingContext {
            frozen_lsn: None,
            has_last_error: true,
            ..restored
        };
        let cases = [
            ("success base", successful, false, 9, 3, true),
            ("success one redelivery", successful, false, 10, 3, true),
            ("success all redeliveries", successful, false, 16, 3, true),
            (
                "success too many redeliveries",
                successful,
                false,
                17,
                3,
                false,
            ),
            (
                "success wrong passive split",
                successful,
                false,
                10,
                2,
                false,
            ),
            ("pre-catch-up restore", restored, true, 2, 3, true),
            (
                "pre-catch-up restore redeliveries",
                restored,
                true,
                4,
                3,
                true,
            ),
            ("demote restore", restored, true, 3, 3, true),
            ("demote restore redeliveries", restored, true, 6, 3, true),
            (
                "restore path excess redelivery",
                restored,
                true,
                7,
                3,
                false,
            ),
            (
                "failed-promotion compensation",
                fully_compensated,
                true,
                8,
                5,
                true,
            ),
            (
                "failed-promotion compensation redeliveries",
                fully_compensated,
                true,
                16,
                5,
                true,
            ),
            (
                "failed-promotion one label effect",
                fully_compensated,
                true,
                9,
                4,
                true,
            ),
            (
                "failed-promotion all effects",
                fully_compensated,
                true,
                10,
                3,
                true,
            ),
            (
                "failed-promotion all redeliveries",
                fully_compensated,
                true,
                18,
                3,
                true,
            ),
            (
                "failed-promotion one label and all replica redeliveries",
                fully_compensated,
                true,
                17,
                4,
                true,
            ),
            (
                "failed-promotion excess redelivery",
                fully_compensated,
                true,
                17,
                5,
                false,
            ),
            (
                "unreachable low split",
                fully_compensated,
                true,
                1,
                1,
                false,
            ),
            (
                "phase-inconsistent restore accounting",
                fully_compensated,
                true,
                2,
                3,
                false,
            ),
            (
                "incomplete compensation phase",
                incomplete_compensation,
                true,
                8,
                5,
                false,
            ),
            (
                "failure without compensation",
                uncompensated_failure,
                true,
                1,
                1,
                false,
            ),
        ];
        for (name, terminal, compensated, external, passive, expected) in cases {
            let accounting = TestActivityAccounting::new(external, passive);
            assert_eq!(
                validate_terminal_accounting_shape(
                    terminal,
                    3,
                    compensated,
                    accounting,
                    external + passive,
                )
                .is_ok(),
                expected,
                "{name}: {accounting:?}",
            );
        }
    }

    #[test]
    fn failure_before_compensation_stops_instead_of_emitting_unreachable_accounting() {
        let reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let mut failed = initial_operation(&reference).unwrap();
        failed.phase = DurableOperationPhase::Failed;
        failed.last_error = Some("revoke failed before compensation".to_string());
        let previous = failed.previous_snapshot.cloned().unwrap();
        let TestAdapterDecision::Observe(result) =
            terminal_adapter_decision(failed, previous, true)
        else {
            panic!("early safe failure must become a stopped switchover result");
        };
        let DurableSwitchoverStepResult::Stopped { operation, message } = *result else {
            panic!("early safe failure must not claim compensated completion");
        };
        assert_eq!(operation.phase, DurableOperationPhase::Poisoned);
        assert!(operation.pending_action.is_none());
        assert_eq!(message, "revoke failed before compensation");
    }

    #[test]
    fn two_member_terminal_accounting_uses_its_authoritative_projection() {
        let reference = test_reference("set-uid", snapshot(2), 2, 100).unwrap();
        let mut completed = initial_operation(&reference).unwrap();
        completed.phase = DurableOperationPhase::Completed;
        completed.frozen_lsn = Some(42);
        let valid = TerminalOutcome::succeeded(
            encode_terminal(&TestTerminal::Complete {
                snapshot: completed.target_snapshot.clone(),
                operation: completed.clone(),
                compensated: false,
                accounting: TestActivityAccounting::new(8, 3),
            })
            .unwrap(),
        );
        assert!(validate_loaded_terminal(&reference, &valid, 11).is_ok());

        let excessive_two_member_redeliveries = TerminalOutcome::succeeded(
            encode_terminal(&TestTerminal::Complete {
                snapshot: completed.target_snapshot.clone(),
                operation: completed,
                compensated: false,
                accounting: TestActivityAccounting::new(15, 3),
            })
            .unwrap(),
        );
        assert!(
            validate_loaded_terminal(&reference, &excessive_two_member_redeliveries, 18).is_err()
        );
    }

    #[test]
    fn transition_and_terminal_validation_rejects_shortcuts() {
        let reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let initial = initial_operation(&reference).unwrap();
        let mut skipped = initial.clone();
        skipped.phase = crate::crd::DurableOperationPhase::PromoteTarget;
        assert!(validate_phase_transition(&initial, &skipped).is_err());

        let mut completed = initial.clone();
        completed.phase = crate::crd::DurableOperationPhase::Completed;
        assert!(validate_completion_transition(&initial, &completed, false).is_err());
        assert!(
            validate_terminal(
                &completed,
                &completed.previous_snapshot.cloned().unwrap(),
                false
            )
            .is_err()
        );
        assert!(validate_terminal(&completed, &completed.target_snapshot.clone(), true).is_err());
    }

    #[tokio::test]
    async fn schedule_unknown_outcomes_reload_before_any_permit() {
        for (fault, applied) in [
            (
                kuberic_durable_execution::InMemoryFault::OutcomeUnknownWithoutApply,
                false,
            ),
            (
                kuberic_durable_execution::InMemoryFault::OutcomeUnknownAfterApply,
                true,
            ),
        ] {
            let reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
            let execution = execution_spec(&reference).unwrap();
            let backend = InMemoryCheckpointStore::new();
            backend.fail_next_compare_and_swap(fault);
            let mut host = DurableHost::new(
                MeasuredDurableCheckpointStore::new(
                    execution.execution_id(),
                    DurableCheckpointStore::InMemory(backend),
                ),
                HostEpoch::from_bytes([15; 16]),
                checkpoint_limits(),
            );
            let first = host
                .turn(&DurableSwitchoverWorkflow, execution.clone())
                .await;
            assert!(matches!(
                first,
                kuberic_durable_execution::HostOutcome::ReloadRequired {
                    boundary: kuberic_durable_execution::PersistenceBoundary::Schedule,
                    reason: kuberic_durable_execution::ReloadReason::OutcomeUnknown,
                }
            ));
            let second = host.turn(&DurableSwitchoverWorkflow, execution).await;
            assert_eq!(
                matches!(
                    &second,
                    kuberic_durable_execution::HostOutcome::DispatchPermitted { .. }
                ),
                applied
            );
            assert_eq!(
                matches!(
                    &second,
                    kuberic_durable_execution::HostOutcome::ScheduleAccepted { .. }
                ),
                !applied
            );
        }
    }

    #[tokio::test]
    async fn observation_faults_reload_effect_free_steps_without_dispatch() {
        use kuberic_durable_execution::{PersistenceBoundary, StoreOperation};

        for (fault, applied) in [
            (
                kuberic_durable_execution::InMemoryFault::ConflictWithoutApply,
                false,
            ),
            (
                kuberic_durable_execution::InMemoryFault::OutcomeUnknownWithoutApply,
                false,
            ),
            (
                kuberic_durable_execution::InMemoryFault::OutcomeUnknownAfterApply,
                true,
            ),
            (
                kuberic_durable_execution::InMemoryFault::FailBeforeRequest(
                    kuberic_durable_execution::StoreErrorKind::Unavailable,
                ),
                false,
            ),
        ] {
            let reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
            let execution = execution_spec(&reference).unwrap();
            let backend = InMemoryCheckpointStore::new();
            let mut host = DurableHost::new(
                MeasuredDurableCheckpointStore::new(
                    execution.execution_id(),
                    DurableCheckpointStore::InMemory(backend.clone()),
                ),
                HostEpoch::from_bytes([18; 16]),
                checkpoint_limits(),
            );
            let permit = expose_next(&mut host, &DurableSwitchoverWorkflow, &execution).await;
            let mut next = decode_activity_input_state(permit.activity().input()).unwrap();
            next.last_error = Some("authoritative observation advanced state".to_string());
            let result = DurableSwitchoverStepResult::Advance { operation: next };
            backend.fail_next_compare_and_swap(fault);
            let observed = host
                .observe(
                    &execution,
                    ActivityObservation::new(
                        permit.activity().clone(),
                        encode_step_result(&result).unwrap(),
                    ),
                )
                .await;
            assert!(matches!(
                observed,
                kuberic_durable_execution::HostOutcome::ReloadRequired {
                    boundary: PersistenceBoundary::Observation,
                    ..
                } | kuberic_durable_execution::HostOutcome::StoreFailed {
                    operation: StoreOperation::CompareAndSwap(PersistenceBoundary::Observation),
                    ..
                }
            ));

            let mut restarted = DurableHost::new(
                MeasuredDurableCheckpointStore::new(
                    execution.execution_id(),
                    DurableCheckpointStore::InMemory(backend),
                ),
                HostEpoch::from_bytes([19; 16]),
                checkpoint_limits(),
            );
            let recovered = restarted.turn(&DurableSwitchoverWorkflow, execution).await;
            assert_eq!(
                matches!(
                    recovered,
                    kuberic_durable_execution::HostOutcome::ScheduleAccepted { .. }
                ),
                applied
            );
            assert_eq!(
                matches!(
                    recovered,
                    kuberic_durable_execution::HostOutcome::Quarantined { .. }
                ),
                !applied
            );
        }
    }

    #[tokio::test]
    async fn exposure_faults_reload_without_false_permits() {
        use kuberic_durable_execution::{PersistenceBoundary, StoreOperation};

        for (fault, applied) in [
            (
                kuberic_durable_execution::InMemoryFault::ConflictWithoutApply,
                false,
            ),
            (
                kuberic_durable_execution::InMemoryFault::OutcomeUnknownWithoutApply,
                false,
            ),
            (
                kuberic_durable_execution::InMemoryFault::OutcomeUnknownAfterApply,
                true,
            ),
            (
                kuberic_durable_execution::InMemoryFault::FailBeforeRequest(
                    kuberic_durable_execution::StoreErrorKind::Unavailable,
                ),
                false,
            ),
        ] {
            let reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
            let execution = execution_spec(&reference).unwrap();
            let backend = InMemoryCheckpointStore::new();
            let mut host = DurableHost::new(
                MeasuredDurableCheckpointStore::new(
                    execution.execution_id(),
                    DurableCheckpointStore::InMemory(backend.clone()),
                ),
                HostEpoch::from_bytes([20; 16]),
                checkpoint_limits(),
            );
            assert!(matches!(
                host.turn(&DurableSwitchoverWorkflow, execution.clone())
                    .await,
                kuberic_durable_execution::HostOutcome::ScheduleAccepted { .. }
            ));
            backend.fail_next_compare_and_swap(fault);
            let exposure = host
                .turn(&DurableSwitchoverWorkflow, execution.clone())
                .await;
            assert!(matches!(
                exposure,
                kuberic_durable_execution::HostOutcome::ReloadRequired {
                    boundary: PersistenceBoundary::Exposure,
                    ..
                } | kuberic_durable_execution::HostOutcome::StoreFailed {
                    operation: StoreOperation::CompareAndSwap(PersistenceBoundary::Exposure),
                    ..
                }
            ));

            let mut restarted = DurableHost::new(
                MeasuredDurableCheckpointStore::new(
                    execution.execution_id(),
                    DurableCheckpointStore::InMemory(backend),
                ),
                HostEpoch::from_bytes([21; 16]),
                checkpoint_limits(),
            );
            let recovered = restarted.turn(&DurableSwitchoverWorkflow, execution).await;
            assert_eq!(
                matches!(
                    recovered,
                    kuberic_durable_execution::HostOutcome::Quarantined { .. }
                ),
                applied
            );
            assert_eq!(
                matches!(
                    recovered,
                    kuberic_durable_execution::HostOutcome::DispatchPermitted { .. }
                ),
                !applied
            );
        }
    }

    #[derive(Clone)]
    struct ConflictOnceStore {
        inner: InMemoryCheckpointStore,
        conflict: Arc<AtomicBool>,
    }

    #[async_trait]
    impl CheckpointStore for ConflictOnceStore {
        async fn load(
            &self,
            execution_id: ExecutionId,
        ) -> Result<Option<StoredCheckpoint>, StoreError> {
            self.inner.load(execution_id).await
        }

        async fn compare_and_swap(
            &self,
            execution_id: ExecutionId,
            expected: Option<StorageRevision>,
            checkpoint: CheckpointEnvelope,
        ) -> Result<CasOutcome, StoreError> {
            if self.conflict.swap(false, Ordering::SeqCst) {
                Ok(CasOutcome::Conflict)
            } else {
                self.inner
                    .compare_and_swap(execution_id, expected, checkpoint)
                    .await
            }
        }
    }

    #[tokio::test]
    async fn schedule_conflict_reloads_before_any_permit() {
        let reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let execution = execution_spec(&reference).unwrap();
        let mut host = DurableHost::new(
            ConflictOnceStore {
                inner: InMemoryCheckpointStore::new(),
                conflict: Arc::new(AtomicBool::new(true)),
            },
            HostEpoch::from_bytes([16; 16]),
            checkpoint_limits(),
        );
        assert!(matches!(
            host.turn(&DurableSwitchoverWorkflow, execution.clone())
                .await,
            kuberic_durable_execution::HostOutcome::ReloadRequired {
                boundary: kuberic_durable_execution::PersistenceBoundary::Schedule,
                reason: kuberic_durable_execution::ReloadReason::Conflict,
            }
        ));
        assert!(matches!(
            host.turn(&DurableSwitchoverWorkflow, execution).await,
            kuberic_durable_execution::HostOutcome::ScheduleAccepted { .. }
        ));
    }

    struct MeasuredSwitchoverWorkflow {
        inputs: Vec<DurableSwitchoverActivityInput>,
        terminal: TerminalOutcome,
    }

    #[async_trait]
    impl Workflow for MeasuredSwitchoverWorkflow {
        async fn run(
            &self,
            context: &mut WorkflowContext<'_>,
            _input: ExactBytes,
        ) -> TerminalOutcome {
            for input in &self.inputs {
                if let Err(error) = context
                    .call::<DurableSwitchoverActivity>(input.clone())
                    .await
                {
                    return TerminalOutcome::failed(error.to_string().into_bytes());
                }
            }
            self.terminal.clone()
        }
    }

    struct MeasuredSwitchoverResolver;

    impl PreparedActivityResolver for MeasuredSwitchoverResolver {
        fn resolve(
            &self,
            logical: &ActivitySpec,
            _recorded: Option<&ActivitySpec>,
        ) -> Result<ActivitySpec, PreparedActivityError> {
            Ok(logical.clone())
        }
    }

    struct MeasuredSwitchoverAdapter {
        resolver: MeasuredSwitchoverResolver,
        result: DurableSwitchoverStepResult,
    }

    #[async_trait]
    impl DurableOperationAdapter for MeasuredSwitchoverAdapter {
        type Resolver = MeasuredSwitchoverResolver;
        type Terminal = TerminalOutcome;
        type Publication = TerminalOutcome;

        fn resolver(&self) -> &Self::Resolver {
            &self.resolver
        }

        async fn observe_or_dispatch(
            &mut self,
            activity: &LogicalActivityId,
            attempt_id: kuberic_durable_execution::AttemptId,
            permit: &mut DurablePermitGuard,
        ) -> DurableAdapterBoundary {
            if let Err(error) = permit.consume(activity.spec(), activity, attempt_id, "switchover")
            {
                return DurableAdapterBoundary::Isolated(error);
            }
            DurableAdapterBoundary::Observed(ActivityObservation::new(
                activity.clone(),
                encode_step_result(&self.result).unwrap(),
            ))
        }

        async fn resolve_quarantine(
            &mut self,
            activity: LogicalActivityId,
            _attempt_id: kuberic_durable_execution::AttemptId,
        ) -> DurableAdapterBoundary {
            DurableAdapterBoundary::Observed(ActivityObservation::new(
                activity,
                encode_step_result(&self.result).unwrap(),
            ))
        }

        fn deadline_unix_seconds(&self) -> i64 {
            100
        }

        fn validate_terminal(
            &mut self,
            outcome: TerminalOutcome,
            _completed_activity_count: u64,
        ) -> Result<Self::Terminal, DurableAdapterBoundary> {
            Ok(outcome)
        }

        fn publication_handoff(&mut self, terminal: Self::Terminal) -> Self::Publication {
            terminal
        }
    }

    #[tokio::test]
    async fn measurements_switchover_runner_preserves_happy_path_accounting() {
        use crate::durable::runner::{DurableRunner, DurableRunnerOutcome};

        let reference = test_reference("set-uid", snapshot(3), 2, 100).unwrap();
        let initial = initial_operation(&reference).unwrap();
        let state = compact(&initial);
        let label = LabelEffectCommand {
            target_id: 2,
            pod_name: "set-1".to_string(),
            expected_uid: "pod-2-uid".to_string(),
            role: "secondary".to_string(),
            identity_signature: "measurement".to_string(),
        };
        let mut inputs = vec![
            DurableSwitchoverActivityInput {
                version: SWITCHOVER_CONTRACT_VERSION,
                state: state.clone(),
                kind: TestActivityKind::PassiveObservation,
            };
            3
        ];
        inputs.extend((0..9).map(|_| DurableSwitchoverActivityInput {
            version: SWITCHOVER_CONTRACT_VERSION,
            state: state.clone(),
            kind: TestActivityKind::PreparedLabel {
                command: label.clone(),
            },
        }));

        let mut completed = initial.clone();
        completed.phase = DurableOperationPhase::Completed;
        completed.frozen_lsn = Some(42);
        let terminal = terminal_success(TestTerminal::Complete {
            operation: completed.clone(),
            snapshot: completed.target_snapshot.clone(),
            compensated: false,
            accounting: TestActivityAccounting::new(9, 3),
        });
        let workflow = MeasuredSwitchoverWorkflow { inputs, terminal };
        let execution = ExecutionSpec::new(
            ExecutionId::from_bytes([44; 16]),
            ExactBytes::new(b"switchover-runner-measurement"),
            SWITCHOVER_MAX_TERMINAL_PAYLOAD_BYTES,
        );
        let measured = MeasuredDurableCheckpointStore::with_decoder(
            execution.execution_id(),
            DurableCheckpointStore::InMemory(InMemoryCheckpointStore::new()),
            checkpoint_measurement_decoder(),
        );
        let mut host = DurableOperatorHost::new(
            measured,
            HostEpoch::from_bytes([45; 16]),
            checkpoint_limits(),
        );
        let mut adapter = MeasuredSwitchoverAdapter {
            resolver: MeasuredSwitchoverResolver,
            result: DurableSwitchoverStepResult::Advance { operation: state },
        };

        let outcome = DurableRunner::new(SWITCHOVER_MAX_ACTIVITY_RECORDS)
            .unwrap()
            .run(&mut host, &workflow, execution, &mut adapter, 0)
            .await;
        assert!(matches!(
            outcome,
            DurableRunnerOutcome::Terminal(TerminalOutcome::Succeeded(_))
        ));

        let measurements = host.store().measurements();
        assert_eq!(measurements.completed_external_effect_count, Some(9));
        assert_eq!(measurements.completed_passive_observation_count, Some(3));
        assert_eq!(measurements.completed_activity_count, Some(12));
        assert_eq!(measurements.accepted_writes, 13);
        assert!(measurements.maximum_active_checkpoint_bytes > 0);
        assert!(measurements.maximum_terminal_checkpoint_bytes > 0);
    }

    #[test]
    fn success_and_rollback_transcripts_fit_with_redelivery_headroom() {
        let canonical_success = projected_success_transcript(3);
        let canonical_rollback = projected_rollback_transcript(3);
        assert_eq!(canonical_success.maximum_activity_count(), 19);
        assert_eq!(canonical_rollback.maximum_activity_count(), 21);
        assert_eq!(canonical_success.external_effect_slot_count(), 9);
        assert_eq!(canonical_rollback.external_effect_slot_count(), 10);
        assert_eq!(canonical_success.redelivery_slot_count(), 7);
        assert_eq!(canonical_rollback.redelivery_slot_count(), 8);

        let success = projected_success_transcript(SWITCHOVER_MAX_REPLICAS);
        let rollback = projected_rollback_transcript(SWITCHOVER_MAX_REPLICAS);
        assert_eq!(
            success.maximum_activity_count(),
            2 * SWITCHOVER_MAX_REPLICAS + 13
        );
        assert_eq!(
            rollback.maximum_activity_count(),
            2 * SWITCHOVER_MAX_REPLICAS + 15
        );
        assert_eq!(
            success.external_effect_slot_count(),
            SWITCHOVER_MAX_REPLICAS + 6
        );
        assert_eq!(
            rollback.external_effect_slot_count(),
            SWITCHOVER_MAX_REPLICAS + 7
        );
        assert_eq!(success.redelivery_slot_count(), SWITCHOVER_MAX_REPLICAS + 4);
        assert_eq!(
            rollback.redelivery_slot_count(),
            SWITCHOVER_MAX_REPLICAS + 5
        );
        let success_label_effects = projected_label_effect_count(&success);
        assert_eq!(success_label_effects, 2);
        assert_eq!(
            success.external_effect_slot_count() - success_label_effects,
            SWITCHOVER_MAX_REPLICAS + 4
        );
        assert_eq!(success.required_passive_observation_count(), 3);
        assert_eq!(success.flexible_activity_count(), 0);
        assert_eq!(rollback.required_passive_observation_count(), 3);
        assert_eq!(rollback.flexible_activity_count(), 2);
        assert!(success.maximum_activity_count() <= SWITCHOVER_MAX_ACTIVITY_RECORDS);
        assert!(rollback.maximum_activity_count() <= SWITCHOVER_MAX_ACTIVITY_RECORDS);
        assert!(
            success.maximum_activity_count() + projected_success_pure_transitions()
                <= SWITCHOVER_MAX_TRANSITION_FUEL
        );
        assert!(
            rollback.maximum_activity_count() + projected_rollback_pure_transitions()
                <= SWITCHOVER_MAX_TRANSITION_FUEL
        );
        let success_bytes =
            projected_active_checkpoint_bytes(success.maximum_activity_count()).unwrap();
        let rollback_bytes =
            projected_active_checkpoint_bytes(rollback.maximum_activity_count()).unwrap();
        assert!(success_bytes <= SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES);
        assert!(rollback_bytes <= SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES);
        let one_over_rollback =
            projected_rollback_transcript(SWITCHOVER_MAX_REPLICAS.saturating_add(1));
        assert_eq!(
            one_over_rollback.maximum_activity_count(),
            SWITCHOVER_MAX_ACTIVITY_RECORDS + 2
        );
        assert!(
            projected_unadmitted_active_checkpoint_bytes(
                one_over_rollback.maximum_activity_count()
            )
            .unwrap()
                > SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES
        );
        eprintln!(
            "framework-native switchover history projections: success_records={} success_bytes={} rollback_records={} rollback_bytes={}",
            success.maximum_activity_count(),
            success_bytes,
            rollback.maximum_activity_count(),
            rollback_bytes
        );
        assert!(success.contains("revoke.effect"));
        assert!(rollback.contains("rollback-promote.effect"));
    }
}
