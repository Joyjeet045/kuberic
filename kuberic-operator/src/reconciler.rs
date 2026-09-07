use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use k8s_openapi::api::core::v1::{
    Container, ContainerPort, EnvVarSource, ObjectFieldSelector, PersistentVolumeClaim,
    PersistentVolumeClaimSpec, Pod, PodSpec, Probe, Service, ServicePort, ServiceSpec,
    TCPSocketAction, VolumeResourceRequirements,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::ResourceExt;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use kuberic_core::driver::{PartitionDriver, ReplicaHandle};
use kuberic_core::error::KubericError;
use kuberic_core::remove_replica::{RemoveReplicaClock, SystemRemoveReplicaClock};
#[cfg(all(test, feature = "durable-switchover-pilot"))]
use kuberic_core::types::CorrelatedControlActionRequest;
#[cfg(test)]
use kuberic_core::types::DurableActionState;
#[cfg(test)]
use kuberic_core::types::{AgentControlVersion, CorrelatedControlActionAcknowledgement};
use kuberic_core::types::{
    AgentGeneration, DurableReplicaAction, ReplicaConfigurationMemberStatus,
    ReplicaConfigurationMode, ReplicaConfigurationStatus, ReplicaElectionConfiguration,
    ReplicaStatusInfo,
};
use kuberic_core::types::{Epoch, ReplicaId, ReplicaInstanceId, StablePartitionSnapshot};
// COMPLEXITY-BOUNDARY: shared-durable-runtime-wiring:start
#[cfg(any(
    feature = "durable-switchover-pilot",
    feature = "durable-remove-replica-pilot"
))]
use kuberic_durable_execution::{ActivityObservation, ActivityState, CheckpointStore, HostOutcome};

use crate::cluster_api::ClusterApi;
use crate::crd::{
    DurableAddMode, DurableOperationKind, DurableOperationPhase, DurableOperationStatus,
    DurableRemoveMode, KubericSet, KubericSetSpec, KubericSetStatus, MemberStatus,
    PendingActionStatus, Phase, ReconfigurationPhase, RemoveReplicaExecutionMode,
    StablePartitionSnapshotStatus, StableReplicaElectionMetadataStatus, StableReplicaRoleStatus,
    StableReplicaSnapshotStatus, StatusCondition,
};
#[cfg(feature = "durable-switchover-pilot")]
use crate::durable::pilot::DurableSwitchoverPilotRuntime;
#[cfg(feature = "durable-switchover-pilot")]
use crate::durable::pilot::{
    DurableSwitchoverPilotTerminal, DurableSwitchoverStepResult, DurableSwitchoverWorkflow,
    PilotActivityAccounting, PilotActivityKind, PilotAdapterDecision, PilotPermitGuard,
    PilotPreparedActivityResolver, decode_pilot_activity_input, encode_step_result,
    evaluate_adapter_step, execution_spec, initial_operation, new_pilot_reference,
    validate_loaded_terminal, validate_pilot_operation, validate_prepared_activity,
};
#[cfg(feature = "durable-remove-replica-pilot")]
use crate::durable::remove_replica_pilot as remove_pilot;
#[cfg(feature = "durable-remove-replica-pilot")]
use crate::durable::remove_replica_pilot::{
    DurableRemoveReplicaPilotRuntime, DurableRemoveReplicaPilotTerminal,
    DurableRemoveReplicaStepResult, DurableRemoveReplicaWorkflow, RemoveReplicaActivityKind,
    RemoveReplicaAdapterDecision, RemoveReplicaPermitGuard, RemoveReplicaPreparedActivityResolver,
};
#[cfg(any(
    feature = "durable-switchover-pilot",
    feature = "durable-remove-replica-pilot"
))]
use crate::durable::workflow_host::DurableWorkflowRuntime;
use crate::durable::{
    CreatePartitionTarget, Decision, OperationObservations, OperationPodIdentities,
    RemoveReplicaTarget, ReplicaObservation, adopt_replacement_before_confirmation,
    attest_add_replica, correlated_action_observation, decide, decide_add_replica,
    decide_create_partition, decide_failover, decide_remove_replica, fail_closed,
    failover_action_for, failover_pending_label, operation_condition, record_activity_error,
    record_observation, start_add_replica, start_create_partition, start_failover,
    start_remove_replica, start_switchover,
};

/// Shared state across reconciliation loops.
pub struct ReconcilerState {
    /// Per-set partition drivers, keyed by "{namespace}/{name}".
    pub drivers: Mutex<HashMap<String, PartitionDriver>>,
    /// Stable statuses whose first persistence attempt failed after the
    /// corresponding runtime topology had already committed.
    pending_statuses: Mutex<HashMap<String, PendingCommittedStatus>>,
    removal_clock: Arc<dyn RemoveReplicaClock>,
    #[cfg(feature = "durable-switchover-pilot")]
    pub durable_switchover_pilot: Option<Arc<DurableSwitchoverPilotRuntime>>,
    #[cfg(feature = "durable-remove-replica-pilot")]
    pub durable_remove_replica_pilot: Option<Arc<DurableRemoveReplicaPilotRuntime>>,
}

#[derive(Clone)]
struct PendingCommittedStatus {
    owner_uid: Option<String>,
    status: KubericSetStatus,
}

impl Default for ReconcilerState {
    fn default() -> Self {
        Self {
            drivers: Mutex::new(HashMap::new()),
            pending_statuses: Mutex::new(HashMap::new()),
            removal_clock: Arc::new(SystemRemoveReplicaClock),
            #[cfg(feature = "durable-switchover-pilot")]
            durable_switchover_pilot: None,
            #[cfg(feature = "durable-remove-replica-pilot")]
            durable_remove_replica_pilot: None,
        }
    }
}

impl ReconcilerState {
    pub fn with_removal_clock(clock: Arc<dyn RemoveReplicaClock>) -> Self {
        Self {
            drivers: Mutex::new(HashMap::new()),
            pending_statuses: Mutex::new(HashMap::new()),
            removal_clock: clock,
            #[cfg(feature = "durable-switchover-pilot")]
            durable_switchover_pilot: None,
            #[cfg(feature = "durable-remove-replica-pilot")]
            durable_remove_replica_pilot: None,
        }
    }

    #[cfg(any(
        feature = "durable-switchover-pilot",
        feature = "durable-remove-replica-pilot"
    ))]
    pub fn with_durable_client(client: kube::Client) -> Self {
        let runtime = Arc::new(DurableWorkflowRuntime::kubernetes(client));
        Self {
            drivers: Mutex::new(HashMap::new()),
            pending_statuses: Mutex::new(HashMap::new()),
            removal_clock: Arc::new(SystemRemoveReplicaClock),
            #[cfg(feature = "durable-switchover-pilot")]
            durable_switchover_pilot: Some(Arc::new(DurableSwitchoverPilotRuntime::shared(
                runtime.clone(),
            ))),
            #[cfg(feature = "durable-remove-replica-pilot")]
            durable_remove_replica_pilot: Some(Arc::new(DurableRemoveReplicaPilotRuntime::shared(
                runtime,
            ))),
        }
    }
    // COMPLEXITY-BOUNDARY: shared-durable-runtime-wiring:end

    #[cfg(feature = "durable-switchover-pilot")]
    pub fn with_durable_switchover_client(client: kube::Client) -> Self {
        Self::with_durable_client(client)
    }

    #[cfg(feature = "durable-switchover-pilot")]
    pub fn with_durable_switchover_store(
        store: kuberic_durable_execution::InMemoryCheckpointStore,
    ) -> Self {
        Self {
            drivers: Mutex::new(HashMap::new()),
            pending_statuses: Mutex::new(HashMap::new()),
            removal_clock: Arc::new(SystemRemoveReplicaClock),
            durable_switchover_pilot: Some(Arc::new(DurableSwitchoverPilotRuntime::in_memory(
                store,
            ))),
            #[cfg(feature = "durable-remove-replica-pilot")]
            durable_remove_replica_pilot: None,
        }
    }

    #[cfg(feature = "durable-remove-replica-pilot")]
    pub fn with_durable_remove_replica_store(
        store: kuberic_durable_execution::InMemoryCheckpointStore,
    ) -> Self {
        Self {
            drivers: Mutex::new(HashMap::new()),
            pending_statuses: Mutex::new(HashMap::new()),
            removal_clock: Arc::new(SystemRemoveReplicaClock),
            #[cfg(feature = "durable-switchover-pilot")]
            durable_switchover_pilot: None,
            durable_remove_replica_pilot: Some(Arc::new(
                DurableRemoveReplicaPilotRuntime::in_memory(store),
            )),
        }
    }
}

/// Result of a reconciliation — either requeue after a duration, or done.
pub enum ReconcileAction {
    Requeue(Duration),
}

type DispatchEvidencePlan = crate::durable::effects::DispatchEvidencePlan;

fn authoritative_topology_snapshot(
    status: &KubericSetStatus,
) -> Option<&StablePartitionSnapshotStatus> {
    let operation = status.operation.as_ref();
    if let Some(operation) = operation
        && status.phase == Phase::RemovingReplica
        && operation.kind == DurableOperationKind::RemoveReplica
        && operation.remove_commit_evidence.is_some()
        && matches!(
            operation.phase,
            DurableOperationPhase::RemoveRecordCommit
                | DurableOperationPhase::RemoveAwaitCleanup
                | DurableOperationPhase::RemoveDeleteTargetPod
                | DurableOperationPhase::RemovePublishTopology
        )
    {
        return operation.committed_snapshot.as_ref();
    }
    status.stable_snapshot.as_ref()
}

fn validate_active_operation_phase(
    status: Option<&KubericSetStatus>,
    current_phase: &Phase,
) -> Result<(), String> {
    let Some(operation) = status.and_then(|status| status.operation.as_ref()) else {
        return Ok(());
    };
    if matches!(
        operation.phase,
        DurableOperationPhase::Completed
            | DurableOperationPhase::Failed
            | DurableOperationPhase::Poisoned
    ) {
        return Ok(());
    }

    let expected_phase = match operation.kind {
        DurableOperationKind::CreatePartition => Phase::Creating,
        DurableOperationKind::Switchover => Phase::Switchover,
        DurableOperationKind::AddReplica => Phase::AddingReplica,
        DurableOperationKind::RemoveReplica => Phase::RemovingReplica,
        DurableOperationKind::Failover => Phase::FailingOver,
    };
    if current_phase != &expected_phase {
        return Err(format!(
            "active durable operation {} requires {expected_phase:?} phase",
            operation.operation_id
        ));
    }
    Ok(())
}

fn validate_new_switchover_engine(mode: crate::crd::SwitchoverExecutionMode) -> Result<(), String> {
    match mode {
        crate::crd::SwitchoverExecutionMode::Explicit => Ok(()),
        crate::crd::SwitchoverExecutionMode::DurablePilot => {
            #[cfg(feature = "durable-switchover-pilot")]
            {
                Ok(())
            }
            #[cfg(not(feature = "durable-switchover-pilot"))]
            {
                Err(
                    "durable switchover pilot requires the operator durable-switchover-pilot build feature"
                        .to_string(),
                )
            }
        }
    }
}

// COMPLEXITY-BOUNDARY: remove-replica-routing-integration:start
fn validate_new_remove_replica_engine(mode: RemoveReplicaExecutionMode) -> Result<(), String> {
    match mode {
        RemoveReplicaExecutionMode::Explicit => Ok(()),
        RemoveReplicaExecutionMode::DurablePilot => {
            #[cfg(feature = "durable-remove-replica-pilot")]
            {
                Ok(())
            }
            #[cfg(not(feature = "durable-remove-replica-pilot"))]
            {
                Err(
                    "durable remove-replica pilot requires the operator durable-remove-replica-pilot build feature"
                        .to_string(),
                )
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn accept_remove_replica(
    set: &KubericSet,
    state: &ReconcilerState,
    previous_snapshot: StablePartitionSnapshotStatus,
    target: RemoveReplicaTarget,
    mode: DurableRemoveMode,
    minimum_replicas: usize,
    now: i64,
) -> Result<KubericSetStatus, String> {
    validate_new_remove_replica_engine(set.spec.remove_replica_execution_mode)?;
    let mut status = KubericSetStatus {
        phase: Phase::RemovingReplica,
        ..set.status.clone().unwrap_or_default()
    };
    match set.spec.remove_replica_execution_mode {
        RemoveReplicaExecutionMode::Explicit => {
            let fallback_identity =
                format!("{}/{}", set.namespace().unwrap_or_default(), set.name_any());
            let operation = start_remove_replica(
                set.metadata.uid.as_deref().unwrap_or(&fallback_identity),
                previous_snapshot,
                target,
                mode,
                minimum_replicas,
                now,
            )?;
            status.operation = Some(operation.clone());
            status.durable_remove_replica_pilot = None;
            set_operation_condition(&mut status, operation_condition(&operation, now));
        }
        RemoveReplicaExecutionMode::DurablePilot => {
            #[cfg(feature = "durable-remove-replica-pilot")]
            {
                if state.durable_remove_replica_pilot.is_none() {
                    return Err(
                        "durable remove-replica pilot runtime is not configured".to_string()
                    );
                }
                let reference = remove_pilot::new_pilot_execution(
                    set.metadata.uid.as_deref().ok_or_else(|| {
                        "durable remove-replica pilot requires KubericSet UID".to_string()
                    })?,
                    previous_snapshot,
                    target,
                    mode,
                    minimum_replicas,
                    now,
                )?;
                status.operation = None;
                status.durable_remove_replica_pilot = Some(reference);
                set_remove_pilot_condition(
                    &mut status,
                    "Accepted",
                    "durable remove reference persisted before checkpoint creation",
                    now,
                );
            }
            #[cfg(not(feature = "durable-remove-replica-pilot"))]
            {
                let _ = (
                    state,
                    previous_snapshot,
                    target,
                    mode,
                    minimum_replicas,
                    now,
                );
                return Err(
                    "durable remove-replica pilot requires the operator durable-remove-replica-pilot build feature"
                        .to_string(),
                );
            }
        }
    }
    Ok(status)
}
// COMPLEXITY-BOUNDARY: remove-replica-routing-integration:end

fn durable_identity_members(
    status: &KubericSetStatus,
    operation: &DurableOperationStatus,
) -> Result<Vec<StableReplicaSnapshotStatus>, String> {
    Ok(match operation.kind {
        DurableOperationKind::CreatePartition => operation
            .committed_snapshot
            .as_ref()
            .map(|snapshot| snapshot.members.clone())
            .unwrap_or_default(),
        DurableOperationKind::Switchover => operation.previous_snapshot.members.clone(),
        DurableOperationKind::AddReplica => operation
            .target_snapshot
            .members
            .iter()
            .filter(|member| {
                Some(member.id) != operation.target_replica_id
                    && member.id != operation.old_primary_id
            })
            .cloned()
            .collect(),
        DurableOperationKind::RemoveReplica if operation.remove_commit_evidence.is_some() => {
            Vec::new()
        }
        DurableOperationKind::RemoveReplica => authoritative_topology_snapshot(status)
            .ok_or_else(|| "durable removal has no authoritative topology snapshot".to_string())?
            .members
            .iter()
            .filter(|member| Some(member.id) != operation.target_replica_id)
            .cloned()
            .collect(),
        DurableOperationKind::Failover => Vec::new(),
    })
}

async fn preadmit_scale_down_target(
    set: &KubericSet,
    api: &dyn ClusterApi,
    current_pods: &[(ReplicaId, ReplicaInstanceId, &Pod)],
    target: &StableReplicaSnapshotStatus,
) -> Result<Option<AgentGeneration>, String> {
    let capability = current_pods
        .iter()
        .find(|(id, instance_id, pod)| {
            *id == target.id && instance_id.as_str() == target.instance_id && is_pod_ready(pod)
        })
        .map(|(_, _, pod)| async {
            let handle = api.create_replica_handle(target.id, pod, &set.spec).await?;
            handle.get_status().await.map_err(|error| error.to_string())
        });
    let capability = match capability {
        Some(check) => tokio::time::timeout(Duration::from_secs(10), check)
            .await
            .ok()
            .and_then(Result::ok),
        None => None,
    };
    let generation = capability.as_ref().and_then(|status| {
        (status.instance_id.as_str() == target.instance_id
            && status.agent.lifecycle_peer_protocol_version
                == kuberic_core::replica_lifecycle::REPLICA_LIFECYCLE_PEER_PROTOCOL_VERSION
            && status.healthy)
            .then(|| status.agent.generation.clone())
    });
    if generation.is_some() {
        return Ok(generation);
    }

    let now = unix_seconds();
    let mut status = set.status.clone().unwrap_or_default();
    let condition = StatusCondition {
        type_: "ScaleDownTargetUnavailable".to_string(),
        status: "True".to_string(),
        reason: "ScaleDownTargetUnavailable".to_string(),
        message: format!(
            "replica {}@{} did not prove lifecycle-peer-v2 capability within 10 seconds",
            target.id, target.instance_id
        ),
        last_transition_time: now.to_string(),
    };
    let unchanged = status.conditions.iter().any(|existing| {
        existing.type_ == condition.type_
            && existing.status == condition.status
            && existing.reason == condition.reason
            && existing.message == condition.message
    });
    if !unchanged {
        status
            .conditions
            .retain(|existing| existing.type_ != condition.type_);
        status.conditions.push(condition);
        api.patch_set_status(
            &set.namespace().unwrap_or_default(),
            &set.name_any(),
            &status,
            set.metadata.resource_version.as_deref(),
        )
        .await?;
    }
    Ok(None)
}

fn plan_dispatch_evidence(
    pending: &PendingActionStatus,
    observed: &ReplicaStatusInfo,
    addressed_instance: &ReplicaInstanceId,
    action: &DurableReplicaAction,
    persist_action_payload: bool,
) -> DispatchEvidencePlan {
    crate::durable::effects::plan_dispatch_evidence(
        pending,
        observed,
        addressed_instance,
        action,
        persist_action_payload,
    )
}

async fn execute_planned_control_action(
    handle: &dyn ReplicaHandle,
    pending: &PendingActionStatus,
    authoritative_action: Option<DurableReplicaAction>,
) -> kuberic_core::Result<()> {
    crate::durable::effects::execute_planned_control_action(handle, pending, authoritative_action)
        .await
}

#[cfg(test)]
fn correlated_acknowledgement_result(
    acknowledgement: CorrelatedControlActionAcknowledgement,
    expected_action_id: &str,
    expected_signature: &str,
    expected_generation: &AgentGeneration,
    expected_control_version: AgentControlVersion,
) -> kuberic_core::Result<()> {
    crate::durable::effects::correlated_acknowledgement_result(
        acknowledgement,
        expected_action_id,
        expected_signature,
        expected_generation,
        expected_control_version,
    )
}

fn dispatch_rejection_requires_refresh(error: &KubericError) -> bool {
    crate::durable::effects::dispatch_rejection_requires_refresh(error)
}

fn dispatch_rejection_is_retryable_without_execution(error: &KubericError) -> bool {
    crate::durable::effects::dispatch_rejection_is_retryable_without_execution(error)
}

fn clear_dispatch_evidence(pending: &mut PendingActionStatus) {
    crate::durable::effects::clear_dispatch_evidence(pending);
}

fn operation_after_dispatch_error(
    operation: &DurableOperationStatus,
    error: &KubericError,
) -> DurableOperationStatus {
    crate::durable::effects::operation_after_dispatch_error(operation, error)
}

// COMPLEXITY-BOUNDARY: pilot-effect-bridge:start
#[cfg(feature = "durable-switchover-pilot")]
/// Result of handling one dispatch-permitted durable pilot activity.
pub type PilotEffectBridgeOutcome = crate::durable::effects::PilotEffectBridgeOutcome;

#[cfg(feature = "durable-switchover-pilot")]
/// Consume one permit and either persist adapter evidence, issue one existing
/// correlated effect, or leave the activity exposed for observation.
#[allow(clippy::too_many_arguments)]
pub async fn bridge_pilot_permitted_step(
    guard: &mut PilotPermitGuard,
    operation: &DurableOperationStatus,
    prepared: &PilotActivityKind,
    accepted_activity: &kuberic_durable_execution::LogicalActivityId,
    accepted_attempt: kuberic_durable_execution::AttemptId,
    observations: &OperationObservations,
    handles: &BTreeMap<ReplicaId, Box<dyn ReplicaHandle>>,
    api: &dyn ClusterApi,
    namespace: &str,
) -> Result<PilotEffectBridgeOutcome, String> {
    crate::durable::effects::bridge_pilot_permitted_step(
        guard,
        operation,
        prepared,
        accepted_activity,
        accepted_attempt,
        observations,
        handles,
        api,
        namespace,
    )
    .await
}

#[cfg(all(test, feature = "durable-switchover-pilot"))]
fn pilot_result_after_dispatch_error(
    operation: &DurableOperationStatus,
    action_id: String,
    error: &KubericError,
) -> Option<DurableSwitchoverStepResult> {
    if crate::durable::effects::classify_dispatch_failure(error)
        == crate::durable::effects::DispatchFailureDisposition::ProvenNoAdmission
    {
        return Some(DurableSwitchoverStepResult::ProvenNoAdmission {
            operation: crate::durable::pilot::DurableSwitchoverState::from_operation(
                &operation_after_dispatch_error(operation, error),
            ),
            action_id,
            redelivery: 1,
        });
    }
    matches!(error, KubericError::RemoteAgentConflict(_)).then(|| {
        DurableSwitchoverStepResult::Stopped {
            operation: crate::durable::pilot::DurableSwitchoverState::from_operation(&fail_closed(
                operation,
                &error.to_string(),
            )),
            message: error.to_string(),
        }
    })
}

#[cfg(feature = "durable-switchover-pilot")]
/// Resolve an exposed activity only from authoritative replica observations.
pub fn resolve_pilot_quarantine(
    operation: &DurableOperationStatus,
    prepared: &PilotActivityKind,
    decision: PilotAdapterDecision,
    observations: &OperationObservations,
) -> Result<PilotEffectBridgeOutcome, String> {
    crate::durable::effects::resolve_pilot_quarantine(operation, prepared, decision, observations)
}

#[cfg(all(test, feature = "durable-switchover-pilot"))]
fn exact_pilot_label_target(
    operation: &DurableOperationStatus,
    target_id: ReplicaId,
    observations: &OperationObservations,
) -> Result<(String, String), String> {
    let command =
        crate::durable::effects::exact_label_command(operation, target_id, "", observations)?;
    Ok((command.pod_name, command.expected_uid))
}

// COMPLEXITY-BOUNDARY: pilot-effect-bridge:end
/// Main reconciliation logic, decoupled from kube-runtime.
/// Takes a ClusterApi trait object so it can be tested without a real cluster.
pub async fn reconcile_set(
    set: &KubericSet,
    api: &dyn ClusterApi,
    state: &ReconcilerState,
) -> Result<ReconcileAction, String> {
    let name = set.name_any();
    let namespace = set.namespace().unwrap_or_default();
    let set_key = format!("{}/{}", namespace, name);

    info!(name, namespace, "reconciling KubericSet");

    // A topology operation is not complete until its stable snapshot is
    // durable. Retry a failed post-commit status write before observing pods
    // or selecting any further health/topology action.
    let pending_status = { state.pending_statuses.lock().await.get(&set_key).cloned() };
    if let Some(pending) = pending_status {
        if pending.owner_uid.as_deref() != set.metadata.uid.as_deref() {
            state.pending_statuses.lock().await.remove(&set_key);
        } else {
            if set.status.as_ref() == Some(&pending.status) {
                state.pending_statuses.lock().await.remove(&set_key);
                #[cfg(any(
                    feature = "durable-switchover-pilot",
                    feature = "durable-remove-replica-pilot"
                ))]
                cleanup_persisted_durable_execution(state, set, &pending.status, &set_key).await;
                return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
            }
            if api
                .patch_set_status(
                    &namespace,
                    &name,
                    &pending.status,
                    set.metadata.resource_version.as_deref(),
                )
                .await
                .is_err()
            {
                return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
            }
            state.pending_statuses.lock().await.remove(&set_key);
            #[cfg(any(
                feature = "durable-switchover-pilot",
                feature = "durable-remove-replica-pilot"
            ))]
            cleanup_persisted_durable_execution(state, set, &pending.status, &set_key).await;
            return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
        }
    }

    let label_selector = format!("kuberic.io/set={}", name);
    let pods = match api.list_pods(&namespace, &label_selector).await {
        Ok(pods) => pods,
        Err(error) => {
            #[cfg(feature = "durable-switchover-pilot")]
            if set.status.as_ref().is_some_and(|status| {
                status.phase == Phase::Switchover
                    && status.operation.is_none()
                    && status.durable_switchover_pilot.is_some()
            }) {
                record_pilot_wait_condition(
                    set,
                    api,
                    "ObservationUnavailable",
                    &format!("pod observation failed: {error}"),
                    unix_seconds(),
                )
                .await;
                return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
            }
            #[cfg(feature = "durable-remove-replica-pilot")]
            if set.status.as_ref().is_some_and(|status| {
                status.phase == Phase::RemovingReplica
                    && status.operation.is_none()
                    && status.durable_remove_replica_pilot.is_some()
            }) {
                record_remove_pilot_wait_condition(
                    set,
                    api,
                    "ObservationUnavailable",
                    &format!("pod observation failed: {error}"),
                    state.removal_clock.unix_seconds(),
                )
                .await;
                return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
            }
            return Err(error);
        }
    };

    let ready_pods: Vec<&Pod> = pods.iter().filter(|p| is_pod_ready(p)).collect();

    let current_phase = set
        .status
        .as_ref()
        .map(|s| s.phase.clone())
        .unwrap_or_default();
    validate_active_operation_phase(set.status.as_ref(), &current_phase)?;

    match current_phase {
        Phase::Pending => {
            info!(name, "creating partition pods and services");
            create_services(api, set, &namespace).await?;
            create_pods(api, set, &namespace).await?;
            let status = KubericSetStatus {
                phase: Phase::Creating,
                ..Default::default()
            };
            persist_committed_status(api, state, &set_key, set, &status).await?;
            Ok(ReconcileAction::Requeue(Duration::from_secs(5)))
        }

        Phase::Creating => {
            if let Some(operation) = set
                .status
                .as_ref()
                .and_then(|status| status.operation.as_ref())
                .filter(|operation| operation.kind == DurableOperationKind::CreatePartition)
            {
                match operation.phase {
                    DurableOperationPhase::Failed => {}
                    DurableOperationPhase::Poisoned => {
                        return Ok(ReconcileAction::Requeue(Duration::from_secs(10)));
                    }
                    DurableOperationPhase::Completed => {
                        return Err(
                            "completed creation operation is inconsistent with Creating phase"
                                .to_string(),
                        );
                    }
                    _ => {
                        return Box::pin(reconcile_durable_operation(
                            set,
                            api,
                            state,
                            &pods,
                            &ready_pods,
                            &set_key,
                        ))
                        .await;
                    }
                }
            }
            let desired = set.spec.replicas as usize;
            if desired == 0 {
                return Err("partition creation requires at least one replica".to_string());
            }
            if set.spec.min_replicas <= 0 || set.spec.min_replicas as usize > desired {
                return Err(format!(
                    "minReplicas must be between 1 and desired replicas ({desired})"
                ));
            }
            if pods.len() < desired {
                for index in 0..desired {
                    ensure_pvc(api, set, &namespace, index as i32).await?;
                    ensure_pod(api, set, &namespace, index as i32).await?;
                }
                return Ok(ReconcileAction::Requeue(Duration::from_secs(5)));
            }
            if ready_pods.len() < desired {
                info!(name, ready = ready_pods.len(), desired, "waiting for pods");
                return Ok(ReconcileAction::Requeue(Duration::from_secs(5)));
            }

            debug!(
                name,
                "all pods ready, checkpointing durable partition creation"
            );
            let current_pods = checked_pods_by_id(&pods)?;
            validate_creation_ids(&current_pods, desired)?;
            let targets = current_pods
                .iter()
                .map(|(replica_id, instance_id, pod)| CreatePartitionTarget {
                    replica_id: *replica_id,
                    instance_id: instance_id.to_string(),
                    pod_name: pod.name_any(),
                })
                .collect();
            let committed_snapshot = set
                .status
                .as_ref()
                .and_then(|status| status.operation.as_ref())
                .filter(|operation| {
                    operation.kind == DurableOperationKind::CreatePartition
                        && operation.phase == DurableOperationPhase::Failed
                })
                .and_then(|operation| operation.committed_snapshot.clone());
            let now = unix_seconds();
            let operation = start_create_partition(
                set.metadata.uid.as_deref().unwrap_or(&set_key),
                targets,
                committed_snapshot,
                set.spec.min_replicas as usize,
                now,
            )?;
            let mut status = KubericSetStatus {
                phase: Phase::Creating,
                operation: Some(operation.clone()),
                ..set.status.clone().unwrap_or_default()
            };
            status.stable_snapshot = None;
            status.current_primary = None;
            status.target_primary = None;
            set_operation_condition(&mut status, operation_condition(&operation, now));
            api.patch_set_status(
                &namespace,
                &name,
                &status,
                set.metadata.resource_version.as_deref(),
            )
            .await?;
            state.drivers.lock().await.remove(&set_key);
            Ok(ReconcileAction::Requeue(Duration::from_secs(1)))
        }

        Phase::Healthy => {
            let desired = set.spec.replicas as usize;
            let actual = pods.len();
            let current_pods = checked_pods_by_id(&pods)?;
            let persisted_snapshot = set
                .status
                .as_ref()
                .and_then(authoritative_topology_snapshot)
                .ok_or_else(|| {
                    format!("cannot recover {namespace}/{name}: stable snapshot is absent")
                })?;
            let stable_primary = persisted_snapshot
                .members
                .iter()
                .find(|member| member.id == persisted_snapshot.primary_id)
                .ok_or_else(|| "stable snapshot primary member is missing".to_string())?;
            let primary_stale_before_topology = match current_pods
                .iter()
                .find(|(id, _, _)| *id == stable_primary.id)
            {
                None => true,
                Some((_, instance_id, _)) if instance_id.as_str() != stable_primary.instance_id => {
                    true
                }
                Some((_, _, pod)) if !is_pod_ready(pod) => true,
                Some((_, _, pod)) => match api
                    .create_replica_handle(stable_primary.id, pod, &set.spec)
                    .await
                {
                    Ok(handle) => match handle.get_status().await {
                        Ok(status) => {
                            status.instance_id.as_str() != stable_primary.instance_id
                                || status.epoch
                                    != Epoch::new(
                                        persisted_snapshot.epoch.data_loss_number,
                                        persisted_snapshot.epoch.configuration_number,
                                    )
                                || status.role != kuberic_core::types::Role::Primary
                        }
                        Err(
                            error @ (KubericError::RemoteControlProtocolUnsupported(_)
                            | KubericError::RemoteAgentRequestRejected(_)),
                        ) => {
                            return Err(format!(
                                "stable primary has unsupported or malformed control status: {error}"
                            ));
                        }
                        Err(_) => true,
                    },
                    Err(_) => true,
                },
            };
            if primary_stale_before_topology {
                if set.spec.failover_delay < 0 {
                    return Err("failoverDelay must be non-negative".to_string());
                }
                let now = unix_seconds();
                let failing_since = set
                    .status
                    .as_ref()
                    .and_then(|status| status.primary_failing_since.as_deref())
                    .and_then(|value| value.parse::<i64>().ok());
                if set.spec.failover_delay > 0
                    && failing_since.is_none_or(|since| {
                        now.saturating_sub(since) < i64::from(set.spec.failover_delay)
                    })
                {
                    let status = KubericSetStatus {
                        primary_failing_since: Some(failing_since.unwrap_or(now).to_string()),
                        ..set.status.clone().unwrap_or_default()
                    };
                    api.patch_set_status(
                        &namespace,
                        &name,
                        &status,
                        set.metadata.resource_version.as_deref(),
                    )
                    .await?;
                    return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
                }
                let operation = start_failover(
                    set.metadata.uid.as_deref().unwrap_or(&set_key),
                    persisted_snapshot.clone(),
                    stable_primary.id,
                    set.spec.min_replicas as usize,
                    now,
                )?;
                let mut status = KubericSetStatus {
                    phase: Phase::FailingOver,
                    operation: Some(operation.clone()),
                    ..set.status.clone().unwrap_or_default()
                };
                set_operation_condition(&mut status, operation_condition(&operation, now));
                api.patch_set_status(
                    &namespace,
                    &name,
                    &status,
                    set.metadata.resource_version.as_deref(),
                )
                .await?;
                state.drivers.lock().await.remove(&set_key);
                return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
            }

            // Rebuild compensation deletes the failed candidate while keeping
            // the old stable identity. Recreate that ordinal so the durable
            // rebuild can be attempted again instead of interpreting the
            // intentional cleanup as permanent eviction.
            if actual < desired
                && set
                    .status
                    .as_ref()
                    .and_then(|status| status.operation.as_ref())
                    .is_some_and(|operation| {
                        operation.kind == DurableOperationKind::AddReplica
                            && operation.add_mode == Some(DurableAddMode::Rebuild)
                            && operation.phase == DurableOperationPhase::Failed
                    })
            {
                for i in 0..desired {
                    ensure_pvc(api, set, &namespace, i as i32).await?;
                    ensure_pod(api, set, &namespace, i as i32).await?;
                }
                return Ok(ReconcileAction::Requeue(Duration::from_secs(5)));
            }

            // A missing stable secondary has no replacement incarnation to
            // rebuild. Evict it durably before normal scale-up recreates
            // desired capacity.
            if let Some(target) = persisted_snapshot
                .members
                .iter()
                .filter(|member| member.role == StableReplicaRoleStatus::ActiveSecondary)
                .find(|member| !current_pods.iter().any(|(id, _, _)| *id == member.id))
            {
                let now = state.removal_clock.unix_seconds();
                let status = accept_remove_replica(
                    set,
                    state,
                    persisted_snapshot.clone(),
                    RemoveReplicaTarget {
                        replica_id: target.id,
                        pod_name: format!("{}-{}", name, target.id - 1),
                        pod_uid: target.instance_id.clone(),
                        replicator_address: historical_remove_replicator_address(set, target.id),
                        agent_generation: None,
                    },
                    DurableRemoveMode::Force,
                    set.spec.min_replicas as usize,
                    now,
                )?;
                api.patch_set_status(
                    &namespace,
                    &name,
                    &status,
                    set.metadata.resource_version.as_deref(),
                )
                .await?;
                state.drivers.lock().await.remove(&set_key);
                return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
            }

            // A replacement incarnation cannot reconstruct the previous
            // stable driver. Rebuild it when ready; otherwise evict the old
            // committed incarnation without deleting the replacement UID.
            if let Some((target_id, target_instance, target_pod, old_instance)) = persisted_snapshot
                .members
                .iter()
                .filter(|member| member.role == StableReplicaRoleStatus::ActiveSecondary)
                .find_map(|member| {
                    current_pods
                        .iter()
                        .find(|(id, instance, _)| {
                            *id == member.id && instance.as_str() != member.instance_id
                        })
                        .map(|(id, instance, pod)| {
                            (*id, instance.clone(), *pod, member.instance_id.clone())
                        })
                })
            {
                if !is_pod_ready(target_pod) {
                    let now = state.removal_clock.unix_seconds();
                    let status = accept_remove_replica(
                        set,
                        state,
                        persisted_snapshot.clone(),
                        RemoveReplicaTarget {
                            replica_id: target_id,
                            pod_name: target_pod.name_any(),
                            pod_uid: old_instance,
                            replicator_address: historical_remove_replicator_address(
                                set, target_id,
                            ),
                            agent_generation: None,
                        },
                        DurableRemoveMode::Force,
                        set.spec.min_replicas as usize,
                        now,
                    )?;
                    api.patch_set_status(
                        &namespace,
                        &name,
                        &status,
                        set.metadata.resource_version.as_deref(),
                    )
                    .await?;
                    state.drivers.lock().await.remove(&set_key);
                    return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
                }
                let now = unix_seconds();
                let operation = start_add_replica(
                    set.metadata.uid.as_deref().unwrap_or(&set_key),
                    persisted_snapshot.clone(),
                    target_id,
                    target_instance.to_string(),
                    target_pod.name_any(),
                    DurableAddMode::Rebuild,
                    set.spec.min_replicas as usize,
                    now,
                )?;
                let mut status = KubericSetStatus {
                    phase: Phase::AddingReplica,
                    operation: Some(operation.clone()),
                    ..set.status.clone().unwrap_or_default()
                };
                set_operation_condition(&mut status, operation_condition(&operation, now));
                api.patch_set_status(
                    &namespace,
                    &name,
                    &status,
                    set.metadata.resource_version.as_deref(),
                )
                .await?;
                state.drivers.lock().await.remove(&set_key);
                return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
            }

            // Ensure every desired ordinal exists after missing committed
            // members have been evicted and replacement incarnations handled.
            if actual < desired {
                info!(name, actual, desired, "scale-up: creating pods");
                for i in 0..desired {
                    ensure_pvc(api, set, &namespace, i as i32).await?;
                    ensure_pod(api, set, &namespace, i as i32).await?;
                }
                return Ok(ReconcileAction::Requeue(Duration::from_secs(5)));
            }

            // Rebuild process-local driver state before making any Healthy
            // health or topology decision. Legacy observational fields are
            // never used as recovery input.
            let needs_recovery = !state.drivers.lock().await.contains_key(&set_key);
            if needs_recovery {
                let primary_member = persisted_snapshot
                    .members
                    .iter()
                    .find(|member| member.id == persisted_snapshot.primary_id)
                    .ok_or_else(|| "stable snapshot primary member is missing".to_string())?;
                let primary_stale = match current_pods
                    .iter()
                    .find(|(id, _, _)| *id == primary_member.id)
                {
                    None => true,
                    Some((_, instance_id, pod))
                        if instance_id.as_str() != primary_member.instance_id =>
                    {
                        true
                    }
                    Some((_, _, pod)) if !is_pod_ready(pod) => true,
                    Some((_, _, pod)) => {
                        match api
                            .create_replica_handle(primary_member.id, pod, &set.spec)
                            .await
                        {
                            Ok(handle) => match handle.get_status().await {
                                Ok(status) => {
                                    status.instance_id.as_str() != primary_member.instance_id
                                        || status.epoch
                                            != Epoch::new(
                                                persisted_snapshot.epoch.data_loss_number,
                                                persisted_snapshot.epoch.configuration_number,
                                            )
                                        || status.role != kuberic_core::types::Role::Primary
                                }
                                Err(
                                    error @ (KubericError::RemoteControlProtocolUnsupported(_)
                                    | KubericError::RemoteAgentRequestRejected(_)),
                                ) => {
                                    return Err(format!(
                                        "stable primary has unsupported or malformed control status: {error}"
                                    ));
                                }
                                Err(_) => true,
                            },
                            Err(_) => true,
                        }
                    }
                };
                if primary_stale {
                    if set.spec.failover_delay < 0 {
                        return Err("failoverDelay must be non-negative".to_string());
                    }
                    let now = unix_seconds();
                    let failing_since = set
                        .status
                        .as_ref()
                        .and_then(|status| status.primary_failing_since.as_deref())
                        .and_then(|value| value.parse::<i64>().ok());
                    if set.spec.failover_delay > 0
                        && failing_since.is_none_or(|since| {
                            now.saturating_sub(since) < i64::from(set.spec.failover_delay)
                        })
                    {
                        let status = KubericSetStatus {
                            primary_failing_since: Some(failing_since.unwrap_or(now).to_string()),
                            ..set.status.clone().unwrap_or_default()
                        };
                        api.patch_set_status(
                            &namespace,
                            &name,
                            &status,
                            set.metadata.resource_version.as_deref(),
                        )
                        .await?;
                        return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
                    }
                    let operation = start_failover(
                        set.metadata.uid.as_deref().unwrap_or(&set_key),
                        persisted_snapshot.clone(),
                        primary_member.id,
                        set.spec.min_replicas as usize,
                        now,
                    )?;
                    let mut status = KubericSetStatus {
                        phase: Phase::FailingOver,
                        operation: Some(operation.clone()),
                        ..set.status.clone().unwrap_or_default()
                    };
                    set_operation_condition(&mut status, operation_condition(&operation, now));
                    api.patch_set_status(
                        &namespace,
                        &name,
                        &status,
                        set.metadata.resource_version.as_deref(),
                    )
                    .await?;
                    return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
                }
                // A container restart can retain the Kubernetes Pod UID while
                // losing all process-local replica and correlation state.
                // Detect that case before strict driver recovery and route it
                // through the existing durable force-remove/rebuild protocol.
                let mut restarted_secondary = None;
                for member in persisted_snapshot
                    .members
                    .iter()
                    .filter(|member| member.role == StableReplicaRoleStatus::ActiveSecondary)
                {
                    let Some((_, current_instance, pod)) =
                        current_pods.iter().find(|(id, _, _)| *id == member.id)
                    else {
                        continue;
                    };
                    if current_instance.as_str() != member.instance_id || !is_pod_ready(pod) {
                        continue;
                    }
                    let Ok(handle) = api.create_replica_handle(member.id, pod, &set.spec).await
                    else {
                        continue;
                    };
                    let Ok(observed) = handle.get_status().await else {
                        continue;
                    };
                    let expected_epoch = Epoch::new(
                        persisted_snapshot.epoch.data_loss_number,
                        persisted_snapshot.epoch.configuration_number,
                    );
                    if observed.instance_id.as_str() == member.instance_id
                        && (observed.epoch != expected_epoch
                            || observed.role != kuberic_core::types::Role::ActiveSecondary)
                    {
                        restarted_secondary =
                            Some((member.id, pod.name_any(), member.instance_id.clone()));
                        break;
                    }
                }
                if let Some((replica_id, pod_name, pod_uid)) = restarted_secondary {
                    let now = state.removal_clock.unix_seconds();
                    let status = accept_remove_replica(
                        set,
                        state,
                        persisted_snapshot.clone(),
                        RemoveReplicaTarget {
                            replica_id,
                            pod_name,
                            pod_uid,
                            replicator_address: historical_remove_replicator_address(
                                set, replica_id,
                            ),
                            agent_generation: None,
                        },
                        DurableRemoveMode::Force,
                        set.spec.min_replicas as usize,
                        now,
                    )?;
                    api.patch_set_status(
                        &namespace,
                        &name,
                        &status,
                        set.metadata.resource_version.as_deref(),
                    )
                    .await?;
                    state.drivers.lock().await.remove(&set_key);
                    return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
                }
                let snapshot = StablePartitionSnapshot::try_from(persisted_snapshot)
                    .map_err(|error| format!("invalid stable snapshot: {error}"))?;
                let mut handles: Vec<Box<dyn ReplicaHandle>> = Vec::new();
                for (replica_id, _, pod) in &current_pods {
                    if !persisted_snapshot
                        .members
                        .iter()
                        .any(|member| member.id == *replica_id)
                    {
                        continue;
                    }
                    handles.push(
                        api.create_replica_handle(*replica_id, pod, &set.spec)
                            .await
                            .map_err(|error| {
                                format!(
                                    "cannot construct recovery handle for replica {replica_id}: {error}"
                                )
                            })?,
                    );
                }
                let driver = PartitionDriver::recover(snapshot, handles)
                    .await
                    .map_err(|error| format!("stable driver recovery failed: {error}"))?;
                state
                    .drivers
                    .lock()
                    .await
                    .entry(set_key.clone())
                    .or_insert(driver);
            }
            {
                let drivers = state.drivers.lock().await;
                let driver = drivers.get(&set_key).unwrap();
                validate_pod_handle_identities(driver, &current_pods, desired)?;
            }

            let (current_primary_id, current_epoch, replica_ids) = {
                let drivers = state.drivers.lock().await;
                let driver = drivers.get(&set_key).unwrap();
                (driver.primary_id(), driver.epoch(), driver.replica_ids())
            };
            let current_primary =
                current_primary_id.and_then(|id| pod_name_for_id(&current_pods, id));

            // --- Replica health check (primary + secondaries) ---
            // Probe all replicas via get_status to detect crashed/restarted pods.
            // Must run BEFORE switchover — don't switchover to a stale target.
            let mut stale_ids: Vec<ReplicaId> = Vec::new();
            let mut primary_stale = false;
            for &replica_id in &replica_ids {
                let Some((_, expected_instance, pod)) =
                    current_pods.iter().find(|(id, _, _)| *id == replica_id)
                else {
                    if Some(replica_id) == current_primary_id {
                        primary_stale = true;
                    } else {
                        stale_ids.push(replica_id);
                    }
                    continue;
                };
                let is_stale = match api.create_replica_handle(replica_id, pod, &set.spec).await {
                    Ok(handle) => match handle.get_status().await {
                        Ok(s) if s.instance_id != *expected_instance => {
                            warn!(
                                name,
                                replica_id,
                                expected_instance_id = %expected_instance,
                                actual_instance_id = %s.instance_id,
                                "replica incarnation mismatch"
                            );
                            true
                        }
                        Ok(s) if s.epoch != current_epoch => {
                            warn!(name, replica_id, ?current_epoch,
                                actual_epoch = ?s.epoch,
                                "replica epoch mismatch — pod restarted");
                            true
                        }
                        Ok(s) if s.role == kuberic_core::types::Role::Unknown => {
                            warn!(name, replica_id, "replica role=Unknown — pod restarted");
                            true
                        }
                        Err(
                            e @ (KubericError::RemoteControlProtocolUnsupported(_)
                            | KubericError::RemoteAgentRequestRejected(_)),
                        ) => {
                            return Err(format!(
                                "replica {replica_id} has unsupported or malformed control status: {e}"
                            ));
                        }
                        Err(e) => {
                            warn!(name, replica_id, error = %e,
                                "replica unreachable — stale handle");
                            true
                        }
                        Ok(_) => false,
                    },
                    Err(error) => {
                        warn!(
                            name,
                            replica_id,
                            error = %error,
                            "failed to construct health-check handle"
                        );
                        true
                    }
                };

                if is_stale {
                    if Some(replica_id) == current_primary_id {
                        primary_stale = true;
                    } else {
                        stale_ids.push(replica_id);
                    }
                }
            }

            // Also check K8s-level readiness for primary
            if !primary_stale && let Some(ref primary_name) = current_primary {
                let primary_ready = pods
                    .iter()
                    .find(|p| p.name_any() == *primary_name)
                    .map(is_pod_ready)
                    .unwrap_or(false);
                if !primary_ready {
                    primary_stale = true;
                }
            }

            // Primary stale → failover (takes priority over everything)
            if primary_stale {
                warn!(name, "primary unhealthy, initiating failover");
                if set.spec.failover_delay < 0 {
                    return Err("failoverDelay must be non-negative".to_string());
                }
                let now = unix_seconds();
                let failing_since = set
                    .status
                    .as_ref()
                    .and_then(|status| status.primary_failing_since.as_deref())
                    .and_then(|value| value.parse::<i64>().ok());
                if set.spec.failover_delay > 0
                    && failing_since.is_none_or(|since| {
                        now.saturating_sub(since) < i64::from(set.spec.failover_delay)
                    })
                {
                    let status = KubericSetStatus {
                        primary_failing_since: Some(failing_since.unwrap_or(now).to_string()),
                        ..set.status.clone().unwrap_or_default()
                    };
                    api.patch_set_status(
                        &namespace,
                        &name,
                        &status,
                        set.metadata.resource_version.as_deref(),
                    )
                    .await?;
                    return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
                }
                let failed_primary_id = current_primary_id
                    .ok_or_else(|| "stale partition has no primary ID".to_string())?;
                let operation = start_failover(
                    set.metadata.uid.as_deref().unwrap_or(&set_key),
                    persisted_snapshot.clone(),
                    failed_primary_id,
                    set.spec.min_replicas as usize,
                    now,
                )?;
                let mut status = KubericSetStatus {
                    phase: Phase::FailingOver,
                    operation: Some(operation.clone()),
                    ..set.status.clone().unwrap_or_default()
                };
                set_operation_condition(&mut status, operation_condition(&operation, now));
                api.patch_set_status(
                    &namespace,
                    &name,
                    &status,
                    set.metadata.resource_version.as_deref(),
                )
                .await?;
                state.drivers.lock().await.remove(&set_key);
                return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
            }

            if set
                .status
                .as_ref()
                .and_then(|status| status.primary_failing_since.as_ref())
                .is_some()
            {
                let status = KubericSetStatus {
                    primary_failing_since: None,
                    ..set.status.clone().unwrap_or_default()
                };
                api.patch_set_status(
                    &namespace,
                    &name,
                    &status,
                    set.metadata.resource_version.as_deref(),
                )
                .await?;
                return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
            }

            if !stale_ids.is_empty() {
                let target_id = *stale_ids.iter().max().unwrap();
                let target_member = persisted_snapshot
                    .members
                    .iter()
                    .find(|member| member.id == target_id)
                    .ok_or_else(|| {
                        format!("stale secondary {target_id} is absent from stable snapshot")
                    })?;
                let target_pod_name = current_pods
                    .iter()
                    .find(|(id, _, _)| *id == target_id)
                    .map(|(_, _, pod)| pod.name_any())
                    .unwrap_or_else(|| format!("{}-{}", name, target_id - 1));
                let now = state.removal_clock.unix_seconds();
                let status = accept_remove_replica(
                    set,
                    state,
                    persisted_snapshot.clone(),
                    RemoveReplicaTarget {
                        replica_id: target_id,
                        pod_name: target_pod_name,
                        pod_uid: target_member.instance_id.clone(),
                        replicator_address: historical_remove_replicator_address(set, target_id),
                        agent_generation: None,
                    },
                    DurableRemoveMode::Force,
                    set.spec.min_replicas as usize,
                    now,
                )?;
                api.patch_set_status(
                    &namespace,
                    &name,
                    &status,
                    set.metadata.resource_version.as_deref(),
                )
                .await?;
                state.drivers.lock().await.remove(&set_key);
                return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
            }

            // --- Switchover check (only when all replicas are healthy) ---
            let target_primary = set.status.as_ref().and_then(|s| s.target_primary.clone());
            info!(
                name,
                ?current_primary,
                ?target_primary,
                "evaluating switchover request"
            );
            if let (Some(current), Some(target)) = (&current_primary, &target_primary)
                && current != target
            {
                validate_new_switchover_engine(set.spec.switchover_execution_mode)?;
                let target_id = current_pods
                    .iter()
                    .find(|(_, _, pod)| pod.name_any() == *target)
                    .map(|(id, _, _)| *id)
                    .ok_or_else(|| format!("switchover target pod {target} is not current"))?;
                let drivers = state.drivers.lock().await;
                if drivers
                    .get(&set_key)
                    .and_then(|driver| driver.handle(target_id))
                    .is_none()
                {
                    return Err(format!(
                        "switchover target replica {target_id} is not in the committed driver topology"
                    ));
                }
                let previous_snapshot = snapshot_status(drivers.get(&set_key).unwrap())?;
                drop(drivers);
                info!(name, current = %current, target = %target, "switchover requested");
                let now = unix_seconds();
                #[cfg(feature = "durable-switchover-pilot")]
                if set.spec.switchover_execution_mode
                    == crate::crd::SwitchoverExecutionMode::DurablePilot
                {
                    if state.durable_switchover_pilot.is_none() {
                        return Err(
                            "durable switchover pilot runtime is not configured".to_string()
                        );
                    }
                    let reference = new_pilot_reference(
                        set.metadata.uid.as_deref().ok_or_else(|| {
                            "durable switchover pilot requires KubericSet UID".to_string()
                        })?,
                        previous_snapshot,
                        target_id,
                        now,
                    )?;
                    let mut status = KubericSetStatus {
                        phase: Phase::Switchover,
                        operation: None,
                        durable_switchover_pilot: Some(reference),
                        ..set.status.clone().unwrap_or_default()
                    };
                    set_pilot_condition(
                        &mut status,
                        "Accepted",
                        "pilot reference persisted before checkpoint creation",
                        now,
                    );
                    api.patch_set_status(
                        &namespace,
                        &name,
                        &status,
                        set.metadata.resource_version.as_deref(),
                    )
                    .await?;
                    state.drivers.lock().await.remove(&set_key);
                    return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
                }
                let operation = start_switchover(
                    set.metadata.uid.as_deref().unwrap_or(&set_key),
                    previous_snapshot,
                    target_id,
                    now,
                )?;
                let mut status = KubericSetStatus {
                    phase: Phase::Switchover,
                    operation: Some(operation.clone()),
                    durable_switchover_pilot: None,
                    ..set.status.clone().unwrap_or_default()
                };
                set_operation_condition(&mut status, operation_condition(&operation, now));
                api.patch_set_status(
                    &namespace,
                    &name,
                    &status,
                    set.metadata.resource_version.as_deref(),
                )
                .await?;
                state.drivers.lock().await.remove(&set_key);
                return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
            }

            // Scale-up: checkpoint exactly one ready pod before any runtime
            // mutation. Later reconciles operate only from this checkpoint.
            if persisted_snapshot.members.len() < desired
                && let Some((replica_id, instance_id, pod)) =
                    current_pods.iter().find(|(id, _, pod)| {
                        is_pod_ready(pod)
                            && !persisted_snapshot
                                .members
                                .iter()
                                .any(|member| member.id == *id)
                    })
            {
                let now = unix_seconds();
                let operation = start_add_replica(
                    set.metadata.uid.as_deref().unwrap_or(&set_key),
                    persisted_snapshot.clone(),
                    *replica_id,
                    instance_id.to_string(),
                    pod.name_any(),
                    DurableAddMode::ScaleUp,
                    set.spec.min_replicas as usize,
                    now,
                )?;
                let mut status = KubericSetStatus {
                    phase: Phase::AddingReplica,
                    operation: Some(operation.clone()),
                    ..set.status.clone().unwrap_or_default()
                };
                set_operation_condition(&mut status, operation_condition(&operation, now));
                api.patch_set_status(
                    &namespace,
                    &name,
                    &status,
                    set.metadata.resource_version.as_deref(),
                )
                .await?;
                state.drivers.lock().await.remove(&set_key);
                return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
            }

            // Scale-down: checkpoint one exact stable secondary before any
            // runtime or Kubernetes mutation.
            if persisted_snapshot.members.len() > desired {
                let target = persisted_snapshot
                    .members
                    .iter()
                    .filter(|member| member.id != persisted_snapshot.primary_id)
                    .max_by_key(|member| member.id)
                    .ok_or_else(|| "scale-down has no removable secondary".to_string())?;
                let pod_name = current_pods
                    .iter()
                    .find(|(id, _, _)| *id == target.id)
                    .map(|(_, _, pod)| pod.name_any())
                    .unwrap_or_else(|| format!("{}-{}", name, target.id - 1));
                let Some(target_generation) =
                    Box::pin(preadmit_scale_down_target(set, api, &current_pods, target)).await?
                else {
                    return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
                };
                let now = state.removal_clock.unix_seconds();
                let mut status = accept_remove_replica(
                    set,
                    state,
                    persisted_snapshot.clone(),
                    RemoveReplicaTarget {
                        replica_id: target.id,
                        pod_name,
                        pod_uid: target.instance_id.clone(),
                        replicator_address: historical_remove_replicator_address(set, target.id),
                        agent_generation: Some(target_generation),
                    },
                    DurableRemoveMode::ScaleDown,
                    set.spec.min_replicas as usize,
                    now,
                )?;
                status
                    .conditions
                    .retain(|condition| condition.type_ != "ScaleDownTargetUnavailable");
                api.patch_set_status(
                    &namespace,
                    &name,
                    &status,
                    set.metadata.resource_version.as_deref(),
                )
                .await?;
                state.drivers.lock().await.remove(&set_key);
                return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
            }

            let degraded_add = set.status.as_ref().and_then(|status| {
                status
                    .conditions
                    .iter()
                    .any(|condition| condition.reason == "CommittedDegraded")
                    .then_some(status.operation.as_ref())
                    .flatten()
                    .filter(|operation| operation.kind == DurableOperationKind::AddReplica)
            });
            if let Some(operation) = degraded_add {
                let mut add_observations = OperationObservations::new();
                for member in &persisted_snapshot.members {
                    let Some((_, _, pod)) = current_pods.iter().find(|(id, _, _)| *id == member.id)
                    else {
                        continue;
                    };
                    let Ok(handle) = api.create_replica_handle(member.id, pod, &set.spec).await
                    else {
                        continue;
                    };
                    let Ok(status) = handle.get_status().await else {
                        continue;
                    };
                    add_observations.insert(
                        member.id,
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
                if attest_add_replica(operation, &add_observations).is_ok() {
                    let target_id = operation
                        .target_replica_id
                        .ok_or_else(|| "degraded add has no target replica ID".to_string())?;
                    let target = add_observations
                        .get(&target_id)
                        .ok_or_else(|| "degraded add target is unavailable".to_string())?;
                    if target.pod_role_label.as_deref() != Some("secondary") {
                        let mut labels = BTreeMap::new();
                        labels.insert("kuberic.io/role".to_string(), "secondary".to_string());
                        api.patch_pod_labels(&namespace, &target.pod_name, labels)
                            .await?;
                    } else {
                        let mut status = set.status.clone().unwrap_or_default();
                        status
                            .conditions
                            .retain(|condition| condition.reason != "CommittedDegraded");
                        api.patch_set_status(
                            &namespace,
                            &name,
                            &status,
                            set.metadata.resource_version.as_deref(),
                        )
                        .await?;
                    }
                    return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
                }
            }

            if let Some(action) =
                reconcile_stable_election_metadata(set, api, persisted_snapshot, &current_pods)
                    .await?
            {
                return Ok(action);
            }

            Ok(ReconcileAction::Requeue(Duration::from_secs(30)))
        }

        Phase::FailingOver => {
            Box::pin(reconcile_failover_operation(
                set,
                api,
                state,
                &pods,
                &ready_pods,
                &set_key,
            ))
            .await
        }

        Phase::Switchover => {
            #[cfg(feature = "durable-switchover-pilot")]
            if set.status.as_ref().is_some_and(|status| {
                status.operation.is_none() && status.durable_switchover_pilot.is_some()
            }) {
                return match Box::pin(reconcile_durable_switchover_pilot(
                    set,
                    api,
                    state,
                    &pods,
                    &ready_pods,
                ))
                .await
                {
                    Ok(action) => Ok(action),
                    Err(error) => {
                        warn!(name, error, "durable switchover pilot reconcile blocked");
                        record_pilot_wait_condition(set, api, "Blocked", &error, unix_seconds())
                            .await;
                        Ok(ReconcileAction::Requeue(Duration::from_secs(1)))
                    }
                };
            }
            Box::pin(reconcile_durable_operation(
                set,
                api,
                state,
                &pods,
                &ready_pods,
                &set_key,
            ))
            .await
        }

        Phase::RemovingReplica => {
            if set.status.as_ref().is_some_and(|status| {
                status.operation.is_none() && status.durable_remove_replica_pilot.is_some()
            }) {
                #[cfg(feature = "durable-remove-replica-pilot")]
                return match Box::pin(reconcile_durable_remove_replica_pilot(
                    set,
                    api,
                    state,
                    &pods,
                    &ready_pods,
                ))
                .await
                {
                    Ok(action) => Ok(action),
                    Err(error) => {
                        warn!(
                            name,
                            error, "durable remove-replica pilot reconcile blocked"
                        );
                        record_remove_pilot_wait_condition(
                            set,
                            api,
                            "Blocked",
                            &error,
                            state.removal_clock.unix_seconds(),
                        )
                        .await;
                        Ok(ReconcileAction::Requeue(Duration::from_secs(1)))
                    }
                };
                #[cfg(not(feature = "durable-remove-replica-pilot"))]
                return Err(
                    "durable remove-replica pilot requires the operator durable-remove-replica-pilot build feature"
                        .to_string(),
                );
            }
            Box::pin(reconcile_durable_operation(
                set,
                api,
                state,
                &pods,
                &ready_pods,
                &set_key,
            ))
            .await
        }

        Phase::AddingReplica => {
            Box::pin(reconcile_durable_operation(
                set,
                api,
                state,
                &pods,
                &ready_pods,
                &set_key,
            ))
            .await
        }

        Phase::Deleting => Ok(ReconcileAction::Requeue(Duration::from_secs(10))),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type CurrentPod<'a> = (ReplicaId, ReplicaInstanceId, &'a Pod);

async fn reconcile_stable_election_metadata(
    set: &KubericSet,
    api: &dyn ClusterApi,
    snapshot: &StablePartitionSnapshotStatus,
    current_pods: &[CurrentPod<'_>],
) -> Result<Option<ReconcileAction>, String> {
    let existing = set
        .status
        .as_ref()
        .and_then(|status| status.stable_election_metadata_refresh.clone());
    if existing.is_none()
        && snapshot
            .members
            .iter()
            .all(|member| member.election_metadata.is_some())
    {
        return Ok(None);
    }
    let mut refresh = existing.unwrap_or(crate::crd::StableElectionMetadataRefreshStatus {
        snapshot_epoch: snapshot.epoch.clone(),
        next_member_index: 0,
        completed_members: Vec::new(),
        pending_action: None,
    });
    if refresh.snapshot_epoch != snapshot.epoch {
        refresh = crate::crd::StableElectionMetadataRefreshStatus {
            snapshot_epoch: snapshot.epoch.clone(),
            next_member_index: 0,
            completed_members: Vec::new(),
            pending_action: None,
        };
    }
    if snapshot.members.is_empty() {
        return Ok(None);
    }
    let index = refresh.next_member_index as usize % snapshot.members.len();
    refresh.next_member_index = index as u32;
    let member = &snapshot.members[index];
    let Some((_, instance_id, pod)) = current_pods.iter().find(|(id, _, _)| *id == member.id)
    else {
        return advance_metadata_refresh(set, api, refresh, snapshot.members.len()).await;
    };
    if instance_id.as_str() != member.instance_id {
        return advance_metadata_refresh(set, api, refresh, snapshot.members.len()).await;
    }
    let Ok(handle) = api.create_replica_handle(member.id, pod, &set.spec).await else {
        return advance_metadata_refresh(set, api, refresh, snapshot.members.len()).await;
    };
    let observed = match handle.get_status().await {
        Ok(observed) => observed,
        Err(
            error @ (KubericError::RemoteControlProtocolUnsupported(_)
            | KubericError::RemoteAgentRequestRejected(_)),
        ) => {
            return Err(format!(
                "replica {} has unsupported or malformed control status during metadata refresh: {error}",
                member.id
            ));
        }
        Err(_) => {
            return advance_metadata_refresh(set, api, refresh, snapshot.members.len()).await;
        }
    };
    let expected = ReplicaElectionConfiguration {
        previous: None,
        current: ReplicaConfigurationStatus {
            mode: ReplicaConfigurationMode::Current,
            members: snapshot
                .members
                .iter()
                .map(|member| ReplicaConfigurationMemberStatus {
                    id: member.id,
                    instance_id: ReplicaInstanceId::new(member.instance_id.clone()),
                    role: match member.role {
                        StableReplicaRoleStatus::Primary => kuberic_core::types::Role::Primary,
                        StableReplicaRoleStatus::ActiveSecondary => {
                            kuberic_core::types::Role::ActiveSecondary
                        }
                    },
                })
                .collect(),
            write_quorum: snapshot.write_quorum,
        },
    };
    let evidence_complete = observed.instance_id.as_str() == member.instance_id
        && observed.epoch
            == Epoch::new(
                snapshot.epoch.data_loss_number,
                snapshot.epoch.configuration_number,
            )
        && observed.election_configuration.as_ref() == Some(&expected)
        && observed.catch_up_capability.is_some()
        && observed.deactivation_info.is_some();
    if evidence_complete {
        let deactivation = observed.deactivation_info.unwrap();
        let mut status = set.status.clone().unwrap_or_default();
        let stable = status
            .stable_snapshot
            .as_mut()
            .and_then(|snapshot| {
                snapshot
                    .members
                    .iter_mut()
                    .find(|stable| stable.id == member.id)
            })
            .ok_or_else(|| "stable metadata refresh member disappeared".to_string())?;
        stable.election_metadata = Some(StableReplicaElectionMetadataStatus {
            current_lsn: observed.current_progress,
            committed_lsn: observed.committed_lsn,
            first_retained_lsn: observed.catch_up_capability.unwrap(),
            deactivation_epoch: crate::crd::EpochStatus {
                data_loss_number: deactivation.epoch.data_loss_number,
                configuration_number: deactivation.epoch.configuration_number,
            },
            deactivation_catch_up_lsn: deactivation.catch_up_lsn,
        });
        if !refresh.completed_members.contains(&member.id) {
            refresh.completed_members.push(member.id);
            refresh.completed_members.sort_unstable();
        }
        refresh.next_member_index = (refresh.next_member_index + 1) % snapshot.members.len() as u32;
        refresh.pending_action = None;
        status.stable_election_metadata_refresh =
            (refresh.completed_members.len() < snapshot.members.len()).then_some(refresh);
        api.patch_set_status(
            &set.namespace().unwrap_or_default(),
            &set.name_any(),
            &status,
            set.metadata.resource_version.as_deref(),
        )
        .await?;
        return Ok(Some(ReconcileAction::Requeue(Duration::from_secs(1))));
    }

    if let Some(pending) = &refresh.pending_action
        && pending.dispatch_authorized
    {
        let terminal_action = correlated_action_observation(&observed, &pending.action_id)
            .is_some_and(|action| {
                matches!(
                    action.state,
                    kuberic_core::types::DurableActionState::Completed
                        | kuberic_core::types::DurableActionState::Failed
                )
            });
        if terminal_action
            || observed.election_configuration.as_ref() == Some(&expected)
            || unix_seconds() >= pending.deadline_unix_seconds
        {
            return advance_metadata_refresh(set, api, refresh, snapshot.members.len()).await;
        }
    }

    if refresh.pending_action.is_none() {
        let topology_key = snapshot
            .members
            .iter()
            .map(|member| format!("{}@{}", member.id, member.instance_id))
            .collect::<Vec<_>>()
            .join(",");
        refresh.pending_action = Some(crate::crd::PendingActionStatus {
            action_id: format!(
                "stable-election-metadata:{}:{}:{}:{}",
                snapshot.epoch.data_loss_number,
                snapshot.epoch.configuration_number,
                topology_key,
                member.id
            ),
            sequence: refresh.next_member_index,
            kind: crate::crd::DurableActionKind::FailoverRecordElectionConfiguration,
            target_id: member.id,
            target_instance_id: member.instance_id.clone(),
            expected_epoch: snapshot.epoch.clone(),
            desired_postcondition: crate::crd::DurablePostconditionStatus {
                kind: crate::crd::DurablePostconditionKind::ElectionConfiguration,
                role: Some(member.role),
            },
            attempts: 0,
            deadline_unix_seconds: unix_seconds() + crate::durable::ACTION_DEADLINE_SECONDS,
            last_error: None,
            dispatch_authorized: false,
            dispatch_agent_generation: None,
            dispatch_agent_control_version: None,
            dispatch_observed_runtime_epoch: None,
            dispatch_action_payload: String::new(),
        });
        let mut status = set.status.clone().unwrap_or_default();
        status.stable_election_metadata_refresh = Some(refresh);
        api.patch_set_status(
            &set.namespace().unwrap_or_default(),
            &set.name_any(),
            &status,
            set.metadata.resource_version.as_deref(),
        )
        .await?;
        return Ok(Some(ReconcileAction::Requeue(Duration::from_secs(1))));
    }
    if !refresh.pending_action.as_ref().unwrap().dispatch_authorized {
        let pending = refresh.pending_action.as_mut().unwrap();
        pending.dispatch_authorized = true;
        pending.attempts = pending.attempts.saturating_add(1);
        let mut status = set.status.clone().unwrap_or_default();
        status.stable_election_metadata_refresh = Some(refresh);
        api.patch_set_status(
            &set.namespace().unwrap_or_default(),
            &set.name_any(),
            &status,
            set.metadata.resource_version.as_deref(),
        )
        .await?;
        return Ok(Some(ReconcileAction::Requeue(Duration::from_secs(1))));
    }
    let pending = refresh.pending_action.as_ref().unwrap();
    let action = DurableReplicaAction::RecordElectionConfiguration {
        configuration: expected,
    };
    match plan_dispatch_evidence(pending, &observed, &handle.instance_id(), &action, true) {
        DispatchEvidencePlan::Persist(planned) => {
            refresh.pending_action = Some(*planned);
            let mut status = set.status.clone().unwrap_or_default();
            status.stable_election_metadata_refresh = Some(refresh);
            api.patch_set_status(
                &set.namespace().unwrap_or_default(),
                &set.name_any(),
                &status,
                set.metadata.resource_version.as_deref(),
            )
            .await?;
            return Ok(Some(ReconcileAction::Requeue(Duration::from_secs(1))));
        }
        DispatchEvidencePlan::WaitForExactIncarnation
        | DispatchEvidencePlan::WaitForSupportedProtocol => {
            return Ok(Some(ReconcileAction::Requeue(Duration::from_secs(1))));
        }
        DispatchEvidencePlan::Ready => {}
    }
    let pending = refresh.pending_action.as_ref().unwrap();
    let result = execute_planned_control_action(handle.as_ref(), pending, None).await;
    if let Err(error) = result {
        let pending = refresh.pending_action.as_mut().unwrap();
        if dispatch_rejection_requires_refresh(&error) {
            clear_dispatch_evidence(pending);
        } else if !dispatch_rejection_is_retryable_without_execution(&error) {
            pending.attempts = pending.attempts.saturating_add(1);
        }
        pending.last_error = Some(error.to_string().chars().take(512).collect());
        let mut status = set.status.clone().unwrap_or_default();
        status.stable_election_metadata_refresh = Some(refresh);
        api.patch_set_status(
            &set.namespace().unwrap_or_default(),
            &set.name_any(),
            &status,
            set.metadata.resource_version.as_deref(),
        )
        .await?;
    }
    Ok(Some(ReconcileAction::Requeue(Duration::from_secs(1))))
}

async fn advance_metadata_refresh(
    set: &KubericSet,
    api: &dyn ClusterApi,
    mut refresh: crate::crd::StableElectionMetadataRefreshStatus,
    member_count: usize,
) -> Result<Option<ReconcileAction>, String> {
    refresh.pending_action = None;
    refresh.next_member_index = (refresh.next_member_index + 1) % member_count as u32;
    let mut status = set.status.clone().unwrap_or_default();
    status.stable_election_metadata_refresh = Some(refresh);
    api.patch_set_status(
        &set.namespace().unwrap_or_default(),
        &set.name_any(),
        &status,
        set.metadata.resource_version.as_deref(),
    )
    .await?;
    Ok(Some(ReconcileAction::Requeue(Duration::from_secs(1))))
}

async fn reconcile_failover_operation(
    set: &KubericSet,
    api: &dyn ClusterApi,
    state: &ReconcilerState,
    pods: &[Pod],
    ready_pods: &[&Pod],
    set_key: &str,
) -> Result<ReconcileAction, String> {
    let namespace = set.namespace().unwrap_or_default();
    let operation = set
        .status
        .as_ref()
        .and_then(|status| status.operation.clone())
        .ok_or_else(|| "failover phase has no durable operation".to_string())?;
    if operation.kind != DurableOperationKind::Failover {
        return Err("FailingOver phase has a non-failover operation".to_string());
    }
    let current_pods = checked_pods_by_id(pods)?;
    let now = unix_seconds();
    let failover = operation
        .failover
        .as_ref()
        .ok_or_else(|| "failover operation has no checkpoint".to_string())?;

    let mut configured_members = BTreeMap::new();
    if let Some(previous) = &failover.previous_configuration {
        for member in previous.members.iter().filter(|member| !member.dropped) {
            configured_members.insert(member.id, member);
        }
    }
    for member in failover
        .current_configuration
        .members
        .iter()
        .filter(|member| !member.dropped)
    {
        configured_members.insert(member.id, member);
    }
    for member in configured_members.into_values() {
        let Some((_, instance_id, _)) = current_pods.iter().find(|(id, _, _)| *id == member.id)
        else {
            continue;
        };
        if instance_id.as_str() != member.instance_id {
            let next = if !failover.target_confirmed {
                adopt_replacement_before_confirmation(
                    &operation,
                    member.id,
                    instance_id.as_str(),
                    now,
                )?
            } else if failover.promotion_committed && member.id != operation.target_primary_id {
                if operation.target_snapshot.members.len().saturating_sub(1)
                    < operation.minimum_committed_replicas.unwrap_or(1) as usize
                {
                    fail_closed(
                        &operation,
                        "post-promotion replacement would reduce target below minimum replicas",
                    )
                } else {
                    let mut next = operation.clone();
                    next.target_snapshot
                        .members
                        .retain(|target| target.id != member.id);
                    next.target_snapshot.write_quorum =
                        next.target_snapshot.members.len() as u32 / 2 + 1;
                    let checkpoint = next.failover.as_mut().unwrap();
                    checkpoint
                        .current_configuration
                        .members
                        .retain(|target| target.id != member.id);
                    checkpoint.current_configuration.write_quorum =
                        checkpoint.current_configuration.members.len() as u32 / 2 + 1;
                    checkpoint
                        .observations
                        .retain(|observation| observation.id != member.id);
                    checkpoint
                        .final_attestations
                        .retain(|observation| observation.id != member.id);
                    checkpoint.next_secondary_index = 0;
                    checkpoint.next_configuration_index = 0;
                    checkpoint.next_label_index = 0;
                    checkpoint.next_attestation_index = 0;
                    next.pending_action = None;
                    next.phase = DurableOperationPhase::FailoverCatchUpConfiguration;
                    next
                }
            } else {
                fail_closed(
                    &operation,
                    &format!(
                        "confirmed failover replica {} incarnation changed",
                        member.id
                    ),
                )
            };
            return persist_failover_operation(set, api, &next, Duration::from_secs(1)).await;
        }
    }

    let mut handles: BTreeMap<ReplicaId, Box<dyn ReplicaHandle>> = BTreeMap::new();
    for (replica_id, _, pod) in &current_pods {
        let relevant = failover
            .current_configuration
            .members
            .iter()
            .any(|member| member.id == *replica_id)
            || failover
                .previous_configuration
                .as_ref()
                .is_some_and(|configuration| {
                    configuration
                        .members
                        .iter()
                        .any(|member| member.id == *replica_id)
                })
            || operation
                .target_snapshot
                .members
                .iter()
                .any(|member| member.id == *replica_id);
        if !relevant {
            continue;
        }
        if let Ok(handle) = api.create_replica_handle(*replica_id, pod, &set.spec).await {
            handles.insert(*replica_id, handle);
        }
    }

    let probe_id = failover_probe_id(&operation);
    let mut observations = OperationObservations::new();
    if let Some(probe_id) = probe_id
        && let Some(handle) = handles.get(&probe_id)
        && let Some((_, _, pod)) = current_pods.iter().find(|(id, _, _)| *id == probe_id)
    {
        match handle.get_status().await {
            Ok(status) => {
                observations.insert(
                    probe_id,
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
                    "failover probe {probe_id} has unsupported or malformed control status: {error}"
                ));
            }
            Err(_) => {}
        }
    }

    if matches!(
        operation.phase,
        DurableOperationPhase::FailoverCollect
            | DurableOperationPhase::FailoverWaitForBestCandidate
            | DurableOperationPhase::FailoverWaitForReadQuorum
    ) && let Some(probe_id) = probe_id
        && let Some(observation) = observations.get(&probe_id)
    {
        let was_waiting = matches!(
            operation.phase,
            DurableOperationPhase::FailoverWaitForBestCandidate
                | DurableOperationPhase::FailoverWaitForReadQuorum
        );
        let mut next =
            match record_observation(&operation, probe_id, &observation.status, false, now) {
                Ok(next) => next,
                Err(error) => fail_closed(&operation, &error),
            };
        if was_waiting && let Some(failover) = next.failover.as_mut() {
            let wait_count = failover_wait_probe_ids(&operation).len();
            if wait_count > 0 {
                let prior = operation.failover.as_ref().unwrap().next_unavailable_index;
                failover.next_unavailable_index = rotate_wait_probe_index(prior, wait_count);
            }
        }
        return persist_failover_operation(set, api, &next, Duration::from_secs(1)).await;
    }
    if operation.phase == DurableOperationPhase::FailoverCollect
        && let Some(probe_id) = probe_id
        && !observations.contains_key(&probe_id)
    {
        let mut next = operation.clone();
        let unavailable = &mut next.failover.as_mut().unwrap().unavailable_replicas;
        if !unavailable.contains(&probe_id) {
            unavailable.push(probe_id);
            unavailable.sort_unstable();
        }
        return persist_failover_operation(set, api, &next, Duration::from_secs(1)).await;
    }
    if matches!(
        operation.phase,
        DurableOperationPhase::FailoverWaitForBestCandidate
            | DurableOperationPhase::FailoverWaitForReadQuorum
    ) && let Some(probe_id) = probe_id
        && !observations.contains_key(&probe_id)
    {
        let mut next = operation.clone();
        let wait_count = failover_wait_probe_ids(&operation).len();
        let failover = next.failover.as_mut().unwrap();
        if wait_count > 0 {
            failover.next_unavailable_index =
                rotate_wait_probe_index(failover.next_unavailable_index, wait_count);
        }
        return persist_failover_operation(set, api, &next, Duration::from_secs(1)).await;
    }

    if operation.phase == DurableOperationPhase::FailoverRefreshCandidate
        && let Some(observation) = observations.get(&operation.target_primary_id)
    {
        if observation.status.epoch
            != Epoch::new(
                operation.target_snapshot.epoch.data_loss_number,
                operation.target_snapshot.epoch.configuration_number,
            )
        {
            let failed = fail_closed(&operation, "refreshed data-loss candidate has wrong epoch");
            return persist_failover_operation(set, api, &failed, Duration::from_secs(10)).await;
        }
        let refreshed = &observation.status;
        let refreshed_valid = refreshed.healthy
            && matches!(
                refreshed.role,
                kuberic_core::types::Role::Primary | kuberic_core::types::Role::ActiveSecondary
            )
            && refreshed.catch_up_capability.is_some_and(|first| {
                first <= refreshed.committed_lsn
                    && refreshed.committed_lsn <= refreshed.current_progress
            })
            && refreshed.deactivation_info.is_some_and(|deactivation| {
                deactivation.epoch
                    == Epoch::new(
                        operation.target_snapshot.epoch.data_loss_number,
                        operation.target_snapshot.epoch.configuration_number,
                    )
            });
        if !refreshed_valid {
            let failed = fail_closed(
                &operation,
                "state-changing data-loss callback produced invalid candidate evidence",
            );
            return persist_failover_operation(set, api, &failed, Duration::from_secs(10)).await;
        }
        let mut next = match record_observation(
            &operation,
            operation.target_primary_id,
            &observation.status,
            false,
            now,
        ) {
            Ok(next) => next,
            Err(error) => fail_closed(&operation, &error),
        };
        if next.phase == DurableOperationPhase::Poisoned {
            return persist_failover_operation(set, api, &next, Duration::from_secs(10)).await;
        }
        let deactivation = refreshed.deactivation_info.unwrap();
        let member = next
            .target_snapshot
            .members
            .iter_mut()
            .find(|member| member.id == operation.target_primary_id)
            .unwrap();
        member.election_metadata = Some(StableReplicaElectionMetadataStatus {
            current_lsn: refreshed.current_progress,
            committed_lsn: refreshed.committed_lsn,
            first_retained_lsn: refreshed.catch_up_capability.unwrap(),
            deactivation_epoch: crate::crd::EpochStatus {
                data_loss_number: deactivation.epoch.data_loss_number,
                configuration_number: deactivation.epoch.configuration_number,
            },
            deactivation_catch_up_lsn: deactivation.catch_up_lsn,
        });
        next.phase = DurableOperationPhase::FailoverPromoteCandidate;
        return persist_failover_operation(set, api, &next, Duration::from_secs(1)).await;
    }

    if operation.phase == DurableOperationPhase::FailoverAttest
        && let Some(probe_id) = probe_id
        && let Some(observation) = observations.get(&probe_id)
    {
        let next = match record_observation(&operation, probe_id, &observation.status, true, now) {
            Ok(next) => next,
            Err(error) => fail_closed(&operation, &error),
        };
        return persist_failover_operation(set, api, &next, Duration::from_secs(1)).await;
    }

    if operation.pending_action.as_ref().is_some_and(|pending| {
        pending.kind == crate::crd::DurableActionKind::FailoverRecordStartingConfiguration
            && !handles.contains_key(&pending.target_id)
    }) {
        let mut next = operation.clone();
        next.pending_action = None;
        next.failover.as_mut().unwrap().next_configuration_index += 1;
        return persist_failover_operation(set, api, &next, Duration::from_secs(1)).await;
    }

    if operation.phase == DurableOperationPhase::FailoverLabelMembers
        && operation
            .pending_action
            .as_ref()
            .is_some_and(|pending| !pending.dispatch_authorized)
    {
        let (target_id, desired) = failover_pending_label(&operation)
            .ok_or_else(|| "failover label phase has invalid pending action".to_string())?;
        let current_label = current_pods
            .iter()
            .find(|(id, _, _)| *id == target_id)
            .and_then(|(_, _, pod)| pod.metadata.labels.as_ref())
            .and_then(|labels| labels.get("kuberic.io/role"))
            .map(String::as_str);
        let mut next = operation.clone();
        if current_label == Some(desired) || !current_pods.iter().any(|(id, _, _)| *id == target_id)
        {
            next.pending_action = None;
            next.failover.as_mut().unwrap().next_label_index += 1;
        } else {
            next.pending_action.as_mut().unwrap().dispatch_authorized = true;
        }
        return persist_failover_operation(set, api, &next, Duration::from_secs(1)).await;
    }

    if operation
        .pending_action
        .as_ref()
        .is_some_and(|pending| pending.dispatch_authorized)
    {
        if let Some((target_id, role)) = failover_pending_label(&operation) {
            let Some((_, _, pod)) = current_pods.iter().find(|(id, _, _)| *id == target_id) else {
                return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
            };
            if pod
                .metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get("kuberic.io/role"))
                .is_some_and(|current| current == role)
            {
                let mut next = operation.clone();
                next.pending_action = None;
                next.failover.as_mut().unwrap().next_label_index += 1;
                return persist_failover_operation(set, api, &next, Duration::from_secs(1)).await;
            }
            let mut labels = BTreeMap::new();
            labels.insert("kuberic.io/role".to_string(), role.to_string());
            let _ = api
                .patch_pod_labels(&namespace, &pod.name_any(), labels)
                .await;
            return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
        }
        let mut probe_operation = operation.clone();
        probe_operation
            .pending_action
            .as_mut()
            .unwrap()
            .dispatch_authorized = false;
        let observed_decision = match decide_failover(&probe_operation, &observations, now) {
            Ok(decision) => decision,
            Err(error) => Decision::Persist(fail_closed(&operation, &error)),
        };
        let still_incomplete = matches!(
            &observed_decision,
            Decision::Persist(next)
                if next.pending_action.as_ref().is_some_and(|pending| pending.dispatch_authorized)
        );
        if still_incomplete {
            let pending = operation.pending_action.as_ref().unwrap();
            let Some(observed) = observations.get(&pending.target_id) else {
                return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
            };
            let Some(handle) = handles.get(&pending.target_id) else {
                return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
            };
            let action = failover_action_for(&operation, pending, &handles)?;
            match plan_dispatch_evidence(
                pending,
                &observed.status,
                &handle.instance_id(),
                &action,
                true,
            ) {
                DispatchEvidencePlan::Persist(planned) => {
                    let mut next = operation.clone();
                    next.pending_action = Some(*planned);
                    return persist_failover_operation(set, api, &next, Duration::from_secs(1))
                        .await;
                }
                DispatchEvidencePlan::WaitForExactIncarnation
                | DispatchEvidencePlan::WaitForSupportedProtocol => {
                    return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
                }
                DispatchEvidencePlan::Ready => {}
            }
            let pending = operation.pending_action.as_ref().unwrap();
            if let Err(error) = execute_planned_control_action(handle.as_ref(), pending, None).await
            {
                let next = operation_after_dispatch_error(&operation, &error);
                return persist_failover_operation(set, api, &next, Duration::from_secs(1)).await;
            }
            return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
        }
        return apply_failover_decision(
            set,
            api,
            state,
            pods,
            ready_pods,
            set_key,
            &current_pods,
            observed_decision,
            now,
        )
        .await;
    }

    let decision = match decide_failover(&operation, &observations, now) {
        Ok(decision) => decision,
        Err(error) => Decision::Persist(fail_closed(&operation, &error)),
    };
    apply_failover_decision(
        set,
        api,
        state,
        pods,
        ready_pods,
        set_key,
        &current_pods,
        decision,
        now,
    )
    .await
}

fn failover_probe_id(operation: &crate::crd::DurableOperationStatus) -> Option<i64> {
    if let Some(pending) = &operation.pending_action {
        return Some(pending.target_id);
    }
    let failover = operation.failover.as_ref()?;
    match operation.phase {
        DurableOperationPhase::FailoverCollect => next_unobserved_failover_member(failover),
        DurableOperationPhase::FailoverWaitForBestCandidate
        | DurableOperationPhase::FailoverWaitForReadQuorum => {
            let probes = failover_wait_probe_ids(operation);
            (!probes.is_empty()).then(|| {
                let index = failover.next_unavailable_index as usize % probes.len();
                probes[index]
            })
        }
        DurableOperationPhase::FailoverRefreshCandidate => Some(operation.target_primary_id),
        DurableOperationPhase::FailoverAttest => operation
            .target_snapshot
            .members
            .iter()
            .find(|member| {
                !failover
                    .final_attestations
                    .iter()
                    .any(|observation| observation.id == member.id)
            })
            .map(|member| member.id),
        _ => None,
    }
}

fn failover_wait_probe_ids(operation: &crate::crd::DurableOperationStatus) -> Vec<i64> {
    let Some(failover) = operation.failover.as_ref() else {
        return Vec::new();
    };
    let expected_epoch = operation
        .previous_snapshot
        .as_ref()
        .map(|snapshot| snapshot.epoch.clone())
        .unwrap_or_else(|| operation.target_snapshot.epoch.clone());
    let mut probes = failover.unavailable_replicas.clone();
    for observation in &failover.observations {
        let eligible_role = matches!(observation.role.as_str(), "primary" | "activeSecondary");
        let transient = eligible_role
            && (observation.epoch != expected_epoch
                || !observation.healthy
                || observation.first_retained_lsn.is_none());
        if transient && !probes.contains(&observation.id) {
            probes.push(observation.id);
        }
    }
    probes.sort_unstable();
    probes
}

fn rotate_wait_probe_index(previous: u32, probe_count: usize) -> u32 {
    debug_assert!(probe_count > 0);
    previous.wrapping_add(1) % probe_count as u32
}

fn next_unobserved_failover_member(failover: &crate::crd::DurableFailoverStatus) -> Option<i64> {
    let mut seen = HashSet::new();
    failover
        .previous_configuration
        .iter()
        .flat_map(|configuration| configuration.members.iter())
        .chain(failover.current_configuration.members.iter())
        .filter(|member| seen.insert(member.id))
        .filter(|member| !member.dropped)
        .find(|member| {
            !failover
                .observations
                .iter()
                .any(|observation| observation.id == member.id)
                && !failover.unavailable_replicas.contains(&member.id)
        })
        .map(|member| member.id)
}

async fn persist_failover_operation(
    set: &KubericSet,
    api: &dyn ClusterApi,
    operation: &crate::crd::DurableOperationStatus,
    requeue: Duration,
) -> Result<ReconcileAction, String> {
    let mut status = set.status.clone().unwrap_or_default();
    status.operation = Some(operation.clone());
    set_operation_condition(&mut status, operation_condition(operation, unix_seconds()));
    api.patch_set_status(
        &set.namespace().unwrap_or_default(),
        &set.name_any(),
        &status,
        set.metadata.resource_version.as_deref(),
    )
    .await?;
    Ok(ReconcileAction::Requeue(requeue))
}

#[allow(clippy::too_many_arguments)]
async fn apply_failover_decision(
    set: &KubericSet,
    api: &dyn ClusterApi,
    state: &ReconcilerState,
    pods: &[Pod],
    ready_pods: &[&Pod],
    set_key: &str,
    current_pods: &[CurrentPod<'_>],
    decision: Decision,
    now: i64,
) -> Result<ReconcileAction, String> {
    match decision {
        Decision::Persist(operation) => {
            persist_failover_operation(set, api, &operation, Duration::from_secs(1)).await
        }
        Decision::CommitSnapshot {
            operation,
            snapshot,
        } => {
            let mut status = set.status.clone().unwrap_or_default();
            status.epoch = snapshot.epoch.clone();
            status.stable_snapshot = Some(snapshot);
            status.operation = Some(operation.clone());
            status.ready_replicas = ready_pods.len() as i32;
            status.replicas = pods.len() as i32;
            set_operation_condition(&mut status, operation_condition(&operation, now));
            persist_committed_status(api, state, set_key, set, &status).await?;
            Ok(ReconcileAction::Requeue(Duration::from_secs(1)))
        }
        Decision::Complete {
            operation,
            snapshot,
            ..
        } => {
            let recovery_snapshot = snapshot.clone();
            let primary_name = pod_name_for_id(current_pods, snapshot.primary_id)
                .ok_or_else(|| "completed failover primary has no current pod".to_string())?;
            let failover = operation
                .failover
                .as_ref()
                .ok_or_else(|| "completed failover has no attestation".to_string())?;
            let mut members = Vec::new();
            for stable in &snapshot.members {
                let observation = failover
                    .final_attestations
                    .iter()
                    .find(|observation| observation.id == stable.id)
                    .ok_or_else(|| {
                        format!("replica {} lacks final status attestation", stable.id)
                    })?;
                let (_, _, pod) = current_pods
                    .iter()
                    .find(|(id, _, _)| *id == stable.id)
                    .ok_or_else(|| format!("replica {} has no current pod", stable.id))?;
                let pod_ip = pod
                    .status
                    .as_ref()
                    .and_then(|status| status.pod_ip.clone())
                    .unwrap_or_default();
                members.push(MemberStatus {
                    name: pod.name_any(),
                    id: stable.id,
                    instance_id: stable.instance_id.clone(),
                    role: if stable.id == snapshot.primary_id {
                        "primary".to_string()
                    } else {
                        "secondary".to_string()
                    },
                    current_progress: observation.current_lsn,
                    healthy: observation.healthy,
                    control_address: format!("http://{}:{}", pod_ip, set.spec.control_port),
                    data_address: format!("http://{}:{}", pod_ip, set.spec.data_port),
                });
            }
            members.sort_by_key(|member| member.id);
            let status = KubericSetStatus {
                epoch: snapshot.epoch.clone(),
                current_primary: Some(primary_name.clone()),
                target_primary: Some(primary_name),
                phase: Phase::Healthy,
                reconfiguration_phase: ReconfigurationPhase::None,
                ready_replicas: ready_pods.len() as i32,
                replicas: pods.len() as i32,
                members,
                stable_snapshot: Some(snapshot),
                operation: None,
                durable_switchover_pilot: None,
                durable_remove_replica_pilot: None,
                conditions: Vec::new(),
                primary_failing_since: None,
                stable_election_metadata_refresh: None,
            };
            persist_committed_status(api, state, set_key, set, &status).await?;
            let core_snapshot = StablePartitionSnapshot::try_from(&recovery_snapshot)
                .map_err(|error| format!("invalid completed failover snapshot: {error}"))?;
            let mut recovery_handles = Vec::new();
            for member in &recovery_snapshot.members {
                let (_, _, pod) = current_pods
                    .iter()
                    .find(|(id, _, _)| *id == member.id)
                    .ok_or_else(|| {
                        format!(
                            "completed failover replica {} has no current pod",
                            member.id
                        )
                    })?;
                recovery_handles.push(
                    api.create_replica_handle(member.id, pod, &set.spec)
                        .await
                        .map_err(|error| {
                            format!(
                                "completed failover replica {} handle failed: {error}",
                                member.id
                            )
                        })?,
                );
            }
            let driver = PartitionDriver::recover(core_snapshot, recovery_handles)
                .await
                .map_err(|error| format!("completed failover recovery failed: {error}"))?;
            state
                .drivers
                .lock()
                .await
                .insert(set_key.to_string(), driver);
            Ok(ReconcileAction::Requeue(Duration::from_secs(1)))
        }
        Decision::Wait => Ok(ReconcileAction::Requeue(Duration::from_secs(1))),
        Decision::Execute { .. }
        | Decision::PatchPodRole { .. }
        | Decision::PatchPodRoleExactUid { .. }
        | Decision::DeletePod { .. }
        | Decision::RecordCommitEvidence(_)
        | Decision::CompleteDegraded { .. }
        | Decision::RestartCreation { .. } => {
            Err("failover emitted an activity without persisted dispatch authorization".to_string())
        }
    }
}

// COMPLEXITY-BOUNDARY: pilot-reconcile:start
#[cfg(feature = "durable-switchover-pilot")]
async fn reconcile_durable_switchover_pilot(
    set: &KubericSet,
    api: &dyn ClusterApi,
    state: &ReconcilerState,
    pods: &[Pod],
    _ready_pods: &[&Pod],
) -> Result<ReconcileAction, String> {
    let namespace = set.namespace().unwrap_or_default();
    let name = set.name_any();
    let set_uid = set
        .metadata
        .uid
        .as_deref()
        .ok_or_else(|| "active durable switchover pilot has no KubericSet UID".to_string())?;
    let reference = set
        .status
        .as_ref()
        .and_then(|status| status.durable_switchover_pilot.as_ref())
        .ok_or_else(|| "switchover phase has no durable pilot reference".to_string())?;
    let runtime = state
        .durable_switchover_pilot
        .as_ref()
        .ok_or_else(|| "durable switchover pilot runtime is not configured".to_string())?;
    let execution = execution_spec(reference)?;
    let host = runtime.host(&namespace, &name, set_uid, reference).await?;
    let store = { host.lock().await.store().clone() };
    let loaded = store
        .load(execution.execution_id())
        .await
        .map_err(|error| format!("load durable switchover checkpoint: {error}"))?;
    if let Some(terminal) = loaded_pilot_terminal(reference, loaded.as_ref(), &execution)? {
        return publish_pilot_terminal(set, api, state, terminal, unix_seconds()).await;
    }
    let operation = current_pilot_operation(reference, &execution, loaded.as_ref())?;

    let current_pods = checked_pods_by_id(pods)?;
    for member in &operation.previous_snapshot.members {
        let Some((_, instance_id, _)) = current_pods.iter().find(|(id, _, _)| *id == member.id)
        else {
            return Err(format!(
                "durable switchover pilot replica {} has no current pod",
                member.id
            ));
        };
        if instance_id.as_str() != member.instance_id {
            return Err(format!(
                "durable switchover pilot replica {} incarnation changed",
                member.id
            ));
        }
    }

    let mut handles: BTreeMap<ReplicaId, Box<dyn ReplicaHandle>> = BTreeMap::new();
    let mut observations = OperationObservations::new();
    for (replica_id, _, pod) in &current_pods {
        if !operation
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
        handles.insert(*replica_id, handle);
    }

    let now = unix_seconds();
    let initial = initial_operation(reference)?;
    let addressed_instances = handles
        .iter()
        .map(|(replica_id, handle)| (*replica_id, handle.instance_id()))
        .collect();
    let resolver =
        PilotPreparedActivityResolver::new(&initial, &observations, &addressed_instances, now);
    let mut host = host.lock().await;
    let mut outcome = host
        .turn_and_expose_with(&DurableSwitchoverWorkflow, execution.clone(), &resolver)
        .await;
    for _ in 0..crate::durable::pilot::PILOT_MAX_ACTIVITY_RECORDS {
        host.store().correlate_host_outcome(&outcome);
        match outcome {
            HostOutcome::DispatchPermitted { permit, .. } => {
                let activity = permit.activity().clone();
                let accepted_attempt = permit.attempt_id();
                let prepared = decode_pilot_activity_input(permit.activity().input())?;
                let operation = prepared.state.apply_to(&initial)?;
                validate_prepared_activity(&operation, &prepared.kind)?;
                if prepared.kind == PilotActivityKind::PassiveObservation {
                    match evaluate_adapter_step(&operation, &observations, now)? {
                        PilotAdapterDecision::Observe(result) => {
                            let result = enrich_pilot_terminal_result(
                                *result,
                                &observations,
                                host.store().measurements(),
                            );
                            outcome = host
                                .observe_and_turn_with(
                                    &DurableSwitchoverWorkflow,
                                    &execution,
                                    ActivityObservation::new(
                                        activity,
                                        encode_step_result(&result)?,
                                    ),
                                    &resolver,
                                )
                                .await;
                            continue;
                        }
                        PilotAdapterDecision::AwaitEvidence => {
                            record_pilot_wait_condition(
                                set,
                                api,
                                "AwaitingReplicaObservation",
                                "pilot activity is waiting for authoritative observation",
                                now,
                            )
                            .await;
                            return Ok(ReconcileAction::Requeue(pilot_deadline_requeue(
                                &operation, now,
                            )));
                        }
                        PilotAdapterDecision::External(_) => {
                            return Err(
                                "pilot resolver exposed an external effect as a passive observation"
                                    .to_string(),
                            );
                        }
                    }
                }
                let mut guard = PilotPermitGuard::new(permit);
                match bridge_pilot_permitted_step(
                    &mut guard,
                    &operation,
                    &prepared.kind,
                    &activity,
                    accepted_attempt,
                    &observations,
                    &handles,
                    api,
                    &namespace,
                )
                .await?
                {
                    PilotEffectBridgeOutcome::Observe(result) => {
                        let result = enrich_pilot_terminal_result(
                            *result,
                            &observations,
                            host.store().measurements(),
                        );
                        outcome = host
                            .observe_and_turn_with(
                                &DurableSwitchoverWorkflow,
                                &execution,
                                ActivityObservation::new(activity, encode_step_result(&result)?),
                                &resolver,
                            )
                            .await;
                        continue;
                    }
                    PilotEffectBridgeOutcome::ObserveAfterFenceRefresh(result) => {
                        let result = enrich_pilot_terminal_result(
                            *result,
                            &observations,
                            host.store().measurements(),
                        );
                        outcome = host
                            .observe(
                                &execution,
                                ActivityObservation::new(activity, encode_step_result(&result)?),
                            )
                            .await;
                        host.store().correlate_host_outcome(&outcome);
                        if matches!(outcome, HostOutcome::ObservationAccepted { .. }) {
                            record_pilot_wait_condition(
                                set,
                                api,
                                "RefreshingReplicaObservation",
                                "proven non-admission was persisted; retry awaits refreshed replica observations",
                                now,
                            )
                            .await;
                            return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
                        }
                        return pilot_host_outcome_action(set, api, &outcome, now).await;
                    }
                    PilotEffectBridgeOutcome::Exposed => {
                        record_pilot_wait_condition(
                            set,
                            api,
                            "EffectExposed",
                            "correlated effect was exposed and awaits authoritative observation",
                            now,
                        )
                        .await;
                    }
                    PilotEffectBridgeOutcome::AwaitEvidence => {
                        record_pilot_wait_condition(
                            set,
                            api,
                            "AwaitingReplicaObservation",
                            "pilot activity is waiting for an exact replica or authoritative effect evidence",
                            now,
                        )
                        .await;
                    }
                }
                return Ok(ReconcileAction::Requeue(pilot_deadline_requeue(
                    &operation, now,
                )));
            }
            HostOutcome::Quarantined { activity, .. } => {
                let prepared = decode_pilot_activity_input(activity.input())?;
                let operation = prepared.state.apply_to(&initial)?;
                validate_prepared_activity(&operation, &prepared.kind)?;
                let decision = evaluate_adapter_step(&operation, &observations, now)?;
                match resolve_pilot_quarantine(&operation, &prepared.kind, decision, &observations)?
                {
                    PilotEffectBridgeOutcome::Observe(result)
                    | PilotEffectBridgeOutcome::ObserveAfterFenceRefresh(result) => {
                        let result = enrich_pilot_terminal_result(
                            *result,
                            &observations,
                            host.store().measurements(),
                        );
                        outcome = host
                            .observe_and_turn_with(
                                &DurableSwitchoverWorkflow,
                                &execution,
                                ActivityObservation::new(activity, encode_step_result(&result)?),
                                &resolver,
                            )
                            .await;
                        continue;
                    }
                    PilotEffectBridgeOutcome::Exposed | PilotEffectBridgeOutcome::AwaitEvidence => {
                        record_pilot_wait_condition(
                            set,
                            api,
                            "Quarantined",
                            "exposed pilot activity remains quarantined pending authoritative evidence",
                            now,
                        )
                        .await;
                        return Ok(ReconcileAction::Requeue(pilot_deadline_requeue(
                            &operation, now,
                        )));
                    }
                }
            }
            HostOutcome::WorkflowCompleted { outcome, .. } => {
                let completed_activity_count = host
                    .store()
                    .measurements()
                    .completed_activity_count
                    .ok_or_else(|| {
                        "completed durable switchover has no authoritative activity count"
                            .to_string()
                    })?;
                let terminal =
                    validate_loaded_terminal(reference, &outcome, completed_activity_count)?;
                drop(host);
                return publish_pilot_terminal(set, api, state, terminal, now).await;
            }
            outcome @ (HostOutcome::ReloadRequired { .. } | HostOutcome::StoreFailed { .. }) => {
                return pilot_host_outcome_action(set, api, &outcome, now).await;
            }
            HostOutcome::ScheduleAccepted { .. } | HostOutcome::ObservationAccepted { .. } => {
                return Err(
                    "fused durable switchover host returned an unfused persistence outcome"
                        .to_string(),
                );
            }
            HostOutcome::Nondeterminism(error) => {
                return Err(format!("durable switchover workflow changed: {error}"));
            }
            HostOutcome::CheckpointRejected(
                kuberic_durable_execution::CheckpointError::PreparedActivityRejected(_),
            ) => {
                record_pilot_wait_condition(
                    set,
                    api,
                    "AwaitingEffectPreparation",
                    "pilot effect could not be prepared from the current authoritative observations",
                    now,
                )
                .await;
                return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
            }
            HostOutcome::CheckpointRejected(error) => {
                return Err(format!("durable switchover checkpoint rejected: {error}"));
            }
            HostOutcome::ObservationRejected(error) => {
                return Err(format!(
                    "durable switchover observation rejected: {error:?}"
                ));
            }
        }
    }
    Err("durable switchover exhausted in-process fused progression fuel".to_string())
}

#[cfg(feature = "durable-switchover-pilot")]
fn pilot_deadline_requeue(operation: &DurableOperationStatus, now: i64) -> Duration {
    let deadline = operation
        .pending_action
        .as_ref()
        .map(|pending| pending.deadline_unix_seconds)
        .unwrap_or(operation.phase_deadline_unix_seconds);
    let seconds = deadline.saturating_sub(now).clamp(1, 10);
    Duration::from_secs(seconds as u64)
}

#[cfg(feature = "durable-switchover-pilot")]
async fn pilot_host_outcome_action(
    set: &KubericSet,
    api: &dyn ClusterApi,
    outcome: &HostOutcome,
    now: i64,
) -> Result<ReconcileAction, String> {
    let diagnostic = match outcome {
        HostOutcome::ObservationAccepted { .. } | HostOutcome::ScheduleAccepted { .. } => None,
        HostOutcome::ReloadRequired { boundary, reason } => Some((
            "ReloadRequired",
            format!("checkpoint {boundary:?} requires reload after {reason:?}"),
        )),
        HostOutcome::StoreFailed { operation, error } => Some((
            "StorageUnavailable",
            format!("checkpoint {operation:?} failed: {error}"),
        )),
        HostOutcome::ObservationRejected(error) => {
            return Err(format!(
                "durable switchover observation rejected: {error:?}"
            ));
        }
        HostOutcome::CheckpointRejected(error) => {
            return Err(format!("durable switchover checkpoint rejected: {error}"));
        }
        HostOutcome::Nondeterminism(error) => {
            return Err(format!("durable switchover workflow changed: {error}"));
        }
        HostOutcome::DispatchPermitted { .. }
        | HostOutcome::Quarantined { .. }
        | HostOutcome::WorkflowCompleted { .. } => {
            return Err("unexpected durable switchover host outcome routing".to_string());
        }
    };
    if let Some((reason, message)) = diagnostic {
        record_pilot_wait_condition(set, api, reason, &message, now).await;
    }
    Ok(ReconcileAction::Requeue(Duration::from_secs(1)))
}

#[cfg(feature = "durable-switchover-pilot")]
async fn record_pilot_wait_condition(
    set: &KubericSet,
    api: &dyn ClusterApi,
    reason: &str,
    message: &str,
    now: i64,
) {
    let normalized_message: String = message.chars().take(512).collect();
    let desired_status = if matches!(reason, "Completed" | "CompensatedOrSafeFailure") {
        "False"
    } else {
        "True"
    };
    let unchanged = set.status.as_ref().is_some_and(|status| {
        status.conditions.iter().any(|condition| {
            condition.type_ == "DurableSwitchoverPilot"
                && condition.reason == reason
                && condition.status == desired_status
                && condition.message == normalized_message
        })
    });
    if unchanged {
        return;
    }
    let mut status = set.status.clone().unwrap_or_default();
    set_pilot_condition(&mut status, reason, &normalized_message, now);
    let _ = api
        .patch_set_status(
            &set.namespace().unwrap_or_default(),
            &set.name_any(),
            &status,
            set.metadata.resource_version.as_deref(),
        )
        .await;
}

#[cfg(feature = "durable-switchover-pilot")]
fn loaded_pilot_terminal(
    reference: &crate::crd::DurableSwitchoverPilotStatus,
    loaded: Option<&kuberic_durable_execution::StoredCheckpoint>,
    execution: &kuberic_durable_execution::ExecutionSpec,
) -> Result<Option<DurableSwitchoverPilotTerminal>, String> {
    let Some(stored) = loaded else {
        return Ok(None);
    };
    let payload = stored
        .checkpoint()
        .decode_and_validate(execution, crate::durable::pilot::checkpoint_limits())
        .map_err(|error| format!("decode durable switchover checkpoint: {error}"))?;
    match payload.terminal_outcome() {
        Some((outcome, completed_activity_count)) => {
            validate_loaded_terminal(reference, outcome, completed_activity_count).map(Some)
        }
        None => Ok(None),
    }
}

#[cfg(feature = "durable-switchover-pilot")]
fn enrich_pilot_terminal_result(
    result: DurableSwitchoverStepResult,
    observations: &OperationObservations,
    measurements: crate::durable::pilot_store::PilotCheckpointMeasurementsSnapshot,
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
                .map(|(external, passive)| PilotActivityAccounting::new(external, passive))
                .or(accounting),
        },
        other => other,
    }
}

#[cfg(feature = "durable-switchover-pilot")]
async fn publish_pilot_terminal(
    set: &KubericSet,
    api: &dyn ClusterApi,
    state: &ReconcilerState,
    terminal: DurableSwitchoverPilotTerminal,
    now: i64,
) -> Result<ReconcileAction, String> {
    let namespace = set.namespace().unwrap_or_default();
    let name = set.name_any();
    let set_key = format!("{namespace}/{name}");
    let set_uid = set
        .metadata
        .uid
        .as_deref()
        .ok_or_else(|| "terminal durable switchover pilot has no KubericSet UID".to_string())?;
    let reference = set
        .status
        .as_ref()
        .and_then(|status| status.durable_switchover_pilot.as_ref())
        .ok_or_else(|| "terminal durable switchover pilot has no reference".to_string())?;
    let runtime = state
        .durable_switchover_pilot
        .as_ref()
        .ok_or_else(|| "terminal durable switchover pilot runtime is not configured".to_string())?;
    match terminal {
        DurableSwitchoverPilotTerminal::Stopped { message, .. } => {
            record_pilot_wait_condition(set, api, "Quarantined", &message, now).await;
        }
        DurableSwitchoverPilotTerminal::Complete {
            operation,
            snapshot,
            compensated,
            ..
        } => {
            let primary_name = set
                .status
                .as_ref()
                .and_then(|status| {
                    status
                        .members
                        .iter()
                        .find(|member| member.id == snapshot.primary_id)
                })
                .map(|member| member.name.clone())
                .ok_or_else(|| {
                    "terminal pilot snapshot primary is absent from persisted members".to_string()
                })?;
            let persisted_members = set
                .status
                .as_ref()
                .map(|status| status.members.as_slice())
                .unwrap_or_default();
            let mut members = Vec::with_capacity(snapshot.members.len());
            for member in &snapshot.members {
                let persisted = persisted_members
                    .iter()
                    .find(|persisted| persisted.id == member.id)
                    .ok_or_else(|| {
                        format!(
                            "terminal pilot snapshot member {} is absent from persisted status",
                            member.id
                        )
                    })?;
                let mut persisted = persisted.clone();
                persisted.instance_id = member.instance_id.clone();
                persisted.role = if member.id == snapshot.primary_id {
                    "primary".to_string()
                } else {
                    "secondary".to_string()
                };
                members.push(persisted);
            }
            let mut status = set.status.clone().unwrap_or_default();
            status.epoch = snapshot.epoch.clone();
            status.current_primary = Some(primary_name.clone());
            status.target_primary = Some(primary_name);
            status.phase = Phase::Healthy;
            status.reconfiguration_phase = ReconfigurationPhase::None;
            status.ready_replicas = members.iter().filter(|member| member.healthy).count() as i32;
            status.replicas = members.len() as i32;
            status.members = members;
            status.stable_snapshot = Some(snapshot);
            status.operation = None;
            status.primary_failing_since = None;
            status.stable_election_metadata_refresh =
                Some(crate::crd::StableElectionMetadataRefreshStatus {
                    snapshot_epoch: status.epoch.clone(),
                    next_member_index: 0,
                    completed_members: Vec::new(),
                    pending_action: None,
                });
            let (reason, message) = if compensated {
                (
                    "CompensatedOrSafeFailure",
                    operation
                        .last_error
                        .as_deref()
                        .unwrap_or("target promotion failed and the old primary was restored"),
                )
            } else {
                (
                    "Completed",
                    "terminal checkpoint accepted before topology publication",
                )
            };
            set_pilot_condition(&mut status, reason, message, now);
            persist_committed_status(api, state, &set_key, set, &status).await?;
            if let Some(measurements) = runtime
                .measurements(&namespace, &name, set_uid, &reference.execution_id)
                .await
            {
                info!(
                    execution_id = reference.execution_id,
                    checkpoint_load_attempts = measurements.load_attempts,
                    checkpoint_write_attempts = measurements.write_attempts,
                    checkpoint_accepted_writes = measurements.accepted_writes,
                    checkpoint_conflicts = measurements.conflicts,
                    checkpoint_unknown_outcomes = measurements.unknown_outcomes,
                    checkpoint_definite_failures = measurements.definite_failures,
                    latest_authoritative_checkpoint_bytes =
                        ?measurements.latest_authoritative_checkpoint_bytes,
                    maximum_authoritative_checkpoint_bytes =
                        measurements.maximum_authoritative_checkpoint_bytes,
                    maximum_active_checkpoint_bytes =
                        measurements.maximum_active_checkpoint_bytes,
                    minimum_active_checkpoint_bytes =
                        ?measurements.minimum_active_checkpoint_bytes,
                    latest_terminal_checkpoint_bytes =
                        ?measurements.latest_terminal_checkpoint_bytes,
                    minimum_terminal_checkpoint_bytes =
                        ?measurements.minimum_terminal_checkpoint_bytes,
                    maximum_terminal_checkpoint_bytes =
                        measurements.maximum_terminal_checkpoint_bytes,
                    durable_boundary_count = ?measurements.completed_activity_count,
                    "durable switchover pilot process summary"
                );
            }
            state.drivers.lock().await.remove(&set_key);
            runtime
                .forget(&namespace, &name, set_uid, &reference.execution_id)
                .await;
        }
    }
    Ok(ReconcileAction::Requeue(Duration::from_secs(1)))
}

#[cfg(feature = "durable-switchover-pilot")]
fn current_pilot_operation(
    reference: &crate::crd::DurableSwitchoverPilotStatus,
    execution: &kuberic_durable_execution::ExecutionSpec,
    loaded: Option<&kuberic_durable_execution::StoredCheckpoint>,
) -> Result<DurableOperationStatus, String> {
    let initial = initial_operation(reference)?;
    let Some(stored) = loaded else {
        return Ok(initial);
    };
    let payload = stored
        .checkpoint()
        .decode_and_validate(execution, crate::durable::pilot::checkpoint_limits())
        .map_err(|error| format!("decode durable switchover checkpoint: {error}"))?;
    let Some(activities) = payload.active_activities() else {
        return Ok(initial);
    };
    let Some(last) = activities.last() else {
        return Ok(initial);
    };
    let state = match last.state() {
        ActivityState::Scheduled | ActivityState::DispatchExposed { .. } => {
            crate::durable::pilot::decode_activity_input_state(last.input())
        }
        ActivityState::Completed { result } => {
            let result = crate::durable::pilot::decode_activity_step_result(result)?;
            Ok(match result {
                DurableSwitchoverStepResult::Advance { operation }
                | DurableSwitchoverStepResult::ProvenNoAdmission { operation, .. }
                | DurableSwitchoverStepResult::Complete { operation, .. }
                | DurableSwitchoverStepResult::Stopped { operation, .. } => operation,
            })
        }
    }?;
    let operation = state.apply_to(&initial)?;
    validate_pilot_operation(&operation)?;
    Ok(operation)
}

// COMPLEXITY-BOUNDARY: pilot-reconcile:end
// COMPLEXITY-BOUNDARY: remove-replica-reconcile-integration:start
#[cfg(feature = "durable-remove-replica-pilot")]
async fn reconcile_durable_remove_replica_pilot(
    set: &KubericSet,
    api: &dyn ClusterApi,
    state: &ReconcilerState,
    pods: &[Pod],
    _ready_pods: &[&Pod],
) -> Result<ReconcileAction, String> {
    let namespace = set.namespace().unwrap_or_default();
    let name = set.name_any();
    let set_uid =
        set.metadata.uid.as_deref().ok_or_else(|| {
            "active durable remove-replica pilot has no KubericSet UID".to_string()
        })?;
    let reference = set
        .status
        .as_ref()
        .and_then(|status| status.durable_remove_replica_pilot.as_ref())
        .ok_or_else(|| "remove-replica phase has no durable pilot reference".to_string())?;
    let runtime = state
        .durable_remove_replica_pilot
        .as_ref()
        .ok_or_else(|| "durable remove-replica pilot runtime is not configured".to_string())?;
    let execution = remove_pilot::execution_spec(reference)?;
    let host = runtime.host(&namespace, &name, set_uid, reference).await?;
    let store = { host.lock().await.store().clone() };
    let loaded = store
        .load(execution.execution_id())
        .await
        .map_err(|error| format!("load durable remove-replica checkpoint: {error}"))?;
    if let Some(terminal) = loaded_remove_pilot_terminal(reference, loaded.as_ref(), &execution)? {
        return publish_remove_pilot_terminal(set, api, state, terminal, unix_seconds()).await;
    }

    let operation = current_remove_pilot_operation(reference, &execution, loaded.as_ref())?;
    let current_pods = checked_pods_by_id(pods)?;
    let pod_identities: OperationPodIdentities = current_pods
        .iter()
        .map(|(id, instance_id, _)| (*id, instance_id.to_string()))
        .collect();
    let target_pod_role_label = operation.target_replica_id.and_then(|target_id| {
        current_pods
            .iter()
            .find(|(id, instance_id, _)| {
                *id == target_id
                    && operation.target_pod_uid.as_deref() == Some(instance_id.as_str())
            })
            .and_then(|(_, _, pod)| {
                pod.metadata
                    .labels
                    .as_ref()
                    .and_then(|labels| labels.get("kuberic.io/role"))
            })
            .map(String::as_str)
    });

    let mut handles: BTreeMap<ReplicaId, Box<dyn ReplicaHandle>> = BTreeMap::new();
    let mut observations = OperationObservations::new();
    for (replica_id, _, pod) in &current_pods {
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
                    "replica {replica_id} has unsupported or malformed control status during durable remove-replica: {error}"
                ));
            }
            Err(_) => {}
        }
        handles.insert(*replica_id, handle);
    }

    let now = state.removal_clock.unix_seconds();
    let initial = remove_pilot::initial_operation(reference)?;
    let addressed_instances = handles
        .iter()
        .map(|(replica_id, handle)| (*replica_id, handle.instance_id()))
        .collect();
    let resolver = RemoveReplicaPreparedActivityResolver::new(
        &initial,
        &observations,
        &pod_identities,
        target_pod_role_label,
        &addressed_instances,
        now,
    );
    let mut host = host.lock().await;
    let mut outcome = host
        .turn_and_expose_with(&DurableRemoveReplicaWorkflow, execution.clone(), &resolver)
        .await;
    for _ in 0..remove_pilot::REMOVE_REPLICA_PILOT_MAX_ACTIVITY_RECORDS {
        host.store().correlate_host_outcome(&outcome);
        match outcome {
            HostOutcome::DispatchPermitted { permit, .. } => {
                let activity = permit.activity().clone();
                let accepted_attempt = permit.attempt_id();
                let prepared =
                    remove_pilot::decode_remove_activity_input(permit.activity().input())?;
                let operation = prepared.state.apply_to(&initial)?;
                remove_pilot::validate_prepared_activity(&operation, &prepared.kind)?;
                if prepared.kind == RemoveReplicaActivityKind::PassiveObservation {
                    match remove_pilot::evaluate_adapter_step(
                        &operation,
                        &observations,
                        &pod_identities,
                        target_pod_role_label,
                        now,
                    )? {
                        decision @ RemoveReplicaAdapterDecision::Advance(_) => {
                            let result = remove_pilot::completed_step(decision, &prepared.kind)?;
                            outcome = host
                                .observe_and_turn_with(
                                    &DurableRemoveReplicaWorkflow,
                                    &execution,
                                    ActivityObservation::new(
                                        activity,
                                        remove_pilot::encode_step_result(&result)?,
                                    ),
                                    &resolver,
                                )
                                .await;
                            continue;
                        }
                        RemoveReplicaAdapterDecision::AwaitEvidence => {
                            record_remove_pilot_wait_condition(
                                set,
                                api,
                                "AwaitingAuthoritativeObservation",
                                "durable remove activity is waiting for authoritative primary, target, lifecycle, UID, or role evidence",
                                now,
                            )
                            .await;
                            return Ok(ReconcileAction::Requeue(remove_pilot_deadline_requeue(
                                &operation, now,
                            )));
                        }
                        RemoveReplicaAdapterDecision::External(_) => {
                            return Err(
                                "durable remove resolver exposed an external effect as a passive observation"
                                    .to_string(),
                            );
                        }
                    }
                }
                let mut guard = RemoveReplicaPermitGuard::new(permit);
                match crate::durable::effects::bridge_remove_replica_permitted_step(
                &mut guard,
                &operation,
                &prepared.kind,
                &activity,
                accepted_attempt,
                &observations,
                &handles,
                api,
                &namespace,
            )
            .await?
            {
                crate::durable::effects::DurableEffectBridgeOutcome::Observe(result) => {
                    outcome = host
                        .observe_and_turn_with(
                            &DurableRemoveReplicaWorkflow,
                            &execution,
                            ActivityObservation::new(
                                activity,
                                remove_pilot::encode_step_result(&result)?,
                            ),
                            &resolver,
                        )
                        .await;
                    continue;
                }
                crate::durable::effects::DurableEffectBridgeOutcome::ObserveAfterFenceRefresh(
                    result,
                ) => {
                    outcome = host
                        .observe(
                            &execution,
                            ActivityObservation::new(
                                activity,
                                remove_pilot::encode_step_result(&result)?,
                            ),
                        )
                        .await;
                    host.store().correlate_host_outcome(&outcome);
                    if matches!(outcome, HostOutcome::ObservationAccepted { .. }) {
                        record_remove_pilot_wait_condition(
                                set,
                                api,
                                "RefreshingReplicaObservation",
                                "proven non-admission was persisted; retry awaits refreshed replica observations",
                                now,
                            )
                            .await;
                        return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
                    }
                    return remove_pilot_host_outcome_action(set, api, &outcome, now).await;
                }
                crate::durable::effects::DurableEffectBridgeOutcome::Exposed => {
                    record_remove_pilot_wait_condition(
                        set,
                        api,
                        "EffectExposed",
                        "durable remove effect was exposed and awaits authoritative observation",
                        now,
                    )
                    .await;
                }
                crate::durable::effects::DurableEffectBridgeOutcome::AwaitEvidence => {
                    record_remove_pilot_wait_condition(
                            set,
                            api,
                            "AwaitingAuthoritativeObservation",
                            "durable remove activity is waiting for exact authoritative effect evidence",
                            now,
                        )
                        .await;
                }
            }
                return Ok(ReconcileAction::Requeue(remove_pilot_deadline_requeue(
                    &operation, now,
                )));
            }
            HostOutcome::Quarantined { activity, .. } => {
                let prepared = remove_pilot::decode_remove_activity_input(activity.input())?;
                let operation = prepared.state.apply_to(&initial)?;
                remove_pilot::validate_prepared_activity(&operation, &prepared.kind)?;
                let decision = remove_pilot::evaluate_adapter_step(
                    &operation,
                    &observations,
                    &pod_identities,
                    target_pod_role_label,
                    now,
                )?;
                match crate::durable::effects::resolve_remove_replica_quarantine(
                &operation,
                &prepared.kind,
                decision,
                &observations,
            )? {
                crate::durable::effects::DurableEffectBridgeOutcome::Observe(result) => {
                    outcome = host
                        .observe_and_turn_with(
                            &DurableRemoveReplicaWorkflow,
                            &execution,
                            ActivityObservation::new(
                                activity,
                                remove_pilot::encode_step_result(&result)?,
                            ),
                            &resolver,
                        )
                        .await;
                    continue;
                }
                crate::durable::effects::DurableEffectBridgeOutcome::ObserveAfterFenceRefresh(
                    result,
                ) => {
                    outcome = host
                        .observe(
                            &execution,
                            ActivityObservation::new(
                                activity,
                                remove_pilot::encode_step_result(&result)?,
                            ),
                        )
                        .await;
                    host.store().correlate_host_outcome(&outcome);
                    if matches!(outcome, HostOutcome::ObservationAccepted { .. }) {
                        record_remove_pilot_wait_condition(
                            set,
                            api,
                            "RefreshingReplicaObservation",
                            "proven non-admission was persisted; retry awaits refreshed replica observations",
                            now,
                        )
                        .await;
                        return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
                    }
                    return remove_pilot_host_outcome_action(set, api, &outcome, now).await;
                }
                crate::durable::effects::DurableEffectBridgeOutcome::Exposed
                | crate::durable::effects::DurableEffectBridgeOutcome::AwaitEvidence => {
                    record_remove_pilot_wait_condition(
                            set,
                            api,
                            "Quarantined",
                            "exposed durable remove activity remains quarantined pending authoritative evidence",
                            now,
                        )
                        .await;
                    return Ok(ReconcileAction::Requeue(remove_pilot_deadline_requeue(
                        &operation, now,
                    )));
                }
            }
            }
            HostOutcome::WorkflowCompleted { outcome, .. } => {
                let completed_activity_count = host
                    .store()
                    .measurements()
                    .completed_activity_count
                    .ok_or_else(|| {
                        "completed durable remove-replica has no authoritative activity count"
                            .to_string()
                    })?;
                let terminal = remove_pilot::validate_loaded_terminal(
                    reference,
                    &outcome,
                    completed_activity_count,
                )?;
                drop(host);
                return publish_remove_pilot_terminal(set, api, state, terminal, now).await;
            }
            outcome @ (HostOutcome::ReloadRequired { .. } | HostOutcome::StoreFailed { .. }) => {
                return remove_pilot_host_outcome_action(set, api, &outcome, now).await;
            }
            HostOutcome::ScheduleAccepted { .. } | HostOutcome::ObservationAccepted { .. } => {
                return Err(
                    "fused durable remove-replica host returned an unfused persistence outcome"
                        .to_string(),
                );
            }
            HostOutcome::Nondeterminism(error) => {
                return Err(format!("durable remove-replica workflow changed: {error}"));
            }
            HostOutcome::CheckpointRejected(
                kuberic_durable_execution::CheckpointError::PreparedActivityRejected(error),
            ) => {
                record_remove_pilot_wait_condition(
                    set,
                    api,
                    "AwaitingEffectPreparation",
                    &format!(
                        "durable remove effect could not be prepared from current authoritative observations: {error:?}"
                    ),
                    now,
                )
                .await;
                return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
            }
            HostOutcome::CheckpointRejected(error) => {
                return Err(format!(
                    "durable remove-replica checkpoint rejected: {error}"
                ));
            }
            HostOutcome::ObservationRejected(error) => {
                return Err(format!(
                    "durable remove-replica observation rejected: {error:?}"
                ));
            }
        }
    }
    Err("durable remove-replica exhausted bounded fused progression".to_string())
}

#[cfg(feature = "durable-remove-replica-pilot")]
fn remove_pilot_deadline_requeue(operation: &DurableOperationStatus, now: i64) -> Duration {
    let deadline = operation
        .pending_action
        .as_ref()
        .map(|pending| pending.deadline_unix_seconds)
        .unwrap_or(operation.phase_deadline_unix_seconds);
    Duration::from_secs(deadline.saturating_sub(now).clamp(1, 10) as u64)
}

#[cfg(feature = "durable-remove-replica-pilot")]
async fn remove_pilot_host_outcome_action(
    set: &KubericSet,
    api: &dyn ClusterApi,
    outcome: &HostOutcome,
    now: i64,
) -> Result<ReconcileAction, String> {
    let diagnostic = match outcome {
        HostOutcome::ObservationAccepted { .. } | HostOutcome::ScheduleAccepted { .. } => None,
        HostOutcome::ReloadRequired { boundary, reason } => Some((
            "ReloadRequired",
            format!("checkpoint {boundary:?} requires reload after {reason:?}"),
        )),
        HostOutcome::StoreFailed { operation, error } => Some((
            "StorageUnavailable",
            format!("checkpoint {operation:?} failed: {error}"),
        )),
        HostOutcome::ObservationRejected(error) => {
            return Err(format!(
                "durable remove-replica observation rejected: {error:?}"
            ));
        }
        HostOutcome::CheckpointRejected(error) => {
            return Err(format!(
                "durable remove-replica checkpoint rejected: {error}"
            ));
        }
        HostOutcome::Nondeterminism(error) => {
            return Err(format!("durable remove-replica workflow changed: {error}"));
        }
        HostOutcome::DispatchPermitted { .. }
        | HostOutcome::Quarantined { .. }
        | HostOutcome::WorkflowCompleted { .. } => {
            return Err("unexpected durable remove-replica host outcome routing".to_string());
        }
    };
    if let Some((reason, message)) = diagnostic {
        record_remove_pilot_wait_condition(set, api, reason, &message, now).await;
    }
    Ok(ReconcileAction::Requeue(Duration::from_secs(1)))
}

#[cfg(feature = "durable-remove-replica-pilot")]
fn loaded_remove_pilot_terminal(
    reference: &crate::crd::DurableRemoveReplicaPilotStatus,
    loaded: Option<&kuberic_durable_execution::StoredCheckpoint>,
    execution: &kuberic_durable_execution::ExecutionSpec,
) -> Result<Option<DurableRemoveReplicaPilotTerminal>, String> {
    let Some(stored) = loaded else {
        return Ok(None);
    };
    let payload = stored
        .checkpoint()
        .decode_and_validate(execution, remove_pilot::checkpoint_limits())
        .map_err(|error| format!("decode durable remove-replica checkpoint: {error}"))?;
    match payload.terminal_outcome() {
        Some((outcome, completed_activity_count)) => {
            remove_pilot::validate_loaded_terminal(reference, outcome, completed_activity_count)
                .map(Some)
        }
        None => Ok(None),
    }
}

#[cfg(feature = "durable-remove-replica-pilot")]
fn current_remove_pilot_operation(
    reference: &crate::crd::DurableRemoveReplicaPilotStatus,
    execution: &kuberic_durable_execution::ExecutionSpec,
    loaded: Option<&kuberic_durable_execution::StoredCheckpoint>,
) -> Result<DurableOperationStatus, String> {
    let initial = remove_pilot::initial_operation(reference)?;
    let Some(stored) = loaded else {
        return Ok(initial);
    };
    let payload = stored
        .checkpoint()
        .decode_and_validate(execution, remove_pilot::checkpoint_limits())
        .map_err(|error| format!("decode durable remove-replica checkpoint: {error}"))?;
    let Some(activities) = payload.active_activities() else {
        return Ok(initial);
    };
    let Some(last) = activities.last() else {
        return Ok(initial);
    };
    let state = match last.state() {
        ActivityState::Scheduled | ActivityState::DispatchExposed { .. } => {
            remove_pilot::decode_remove_activity_input(last.input())?.state
        }
        ActivityState::Completed { result } => {
            match remove_pilot::decode_activity_step_result(result)? {
                DurableRemoveReplicaStepResult::Advance { operation, .. }
                | DurableRemoveReplicaStepResult::ProvenNoAdmission { operation, .. } => operation,
            }
        }
    };
    state.apply_to(&initial)
}

#[cfg(feature = "durable-remove-replica-pilot")]
async fn publish_remove_pilot_terminal(
    set: &KubericSet,
    api: &dyn ClusterApi,
    state: &ReconcilerState,
    terminal: DurableRemoveReplicaPilotTerminal,
    now: i64,
) -> Result<ReconcileAction, String> {
    let namespace = set.namespace().unwrap_or_default();
    let name = set.name_any();
    let set_key = format!("{namespace}/{name}");
    let set_uid = set
        .metadata
        .uid
        .as_deref()
        .ok_or_else(|| "terminal durable remove-replica has no KubericSet UID".to_string())?;
    let reference = set
        .status
        .as_ref()
        .and_then(|status| status.durable_remove_replica_pilot.as_ref())
        .ok_or_else(|| "terminal durable remove-replica has no reference".to_string())?;
    let runtime = state
        .durable_remove_replica_pilot
        .as_ref()
        .ok_or_else(|| "terminal durable remove-replica runtime is not configured".to_string())?;
    let initial = remove_pilot::initial_operation(reference)?;
    let completed = matches!(
        &terminal,
        DurableRemoveReplicaPilotTerminal::Completed { .. }
    );
    match terminal {
        DurableRemoveReplicaPilotTerminal::Completed {
            authority: _,
            commit_evidence: _,
            cleanup: _,
            accounting: _,
        }
        | DurableRemoveReplicaPilotTerminal::Compensated { .. } => {
            let snapshot = if completed {
                initial.target_snapshot.clone()
            } else {
                initial.previous_snapshot.cloned().ok_or_else(|| {
                    "compensated durable remove terminal has no previous snapshot".to_string()
                })?
            };
            let mut status = set.status.clone().unwrap_or_default();
            let persisted_members = status.members.clone();
            let mut members = Vec::with_capacity(snapshot.members.len());
            for member in &snapshot.members {
                let mut persisted = persisted_members
                    .iter()
                    .find(|persisted| persisted.id == member.id)
                    .cloned()
                    .ok_or_else(|| {
                        format!(
                            "terminal remove snapshot member {} is absent from persisted status",
                            member.id
                        )
                    })?;
                persisted.instance_id = member.instance_id.clone();
                persisted.role = if member.id == snapshot.primary_id {
                    "primary".to_string()
                } else {
                    "secondary".to_string()
                };
                members.push(persisted);
            }
            let primary_name = members
                .iter()
                .find(|member| member.id == snapshot.primary_id)
                .map(|member| member.name.clone())
                .ok_or_else(|| {
                    "terminal remove snapshot primary is absent from persisted status".to_string()
                })?;
            status.epoch = snapshot.epoch.clone();
            status.current_primary = Some(primary_name.clone());
            status.target_primary = Some(primary_name);
            status.phase = Phase::Healthy;
            status.reconfiguration_phase = ReconfigurationPhase::None;
            status.ready_replicas = members.iter().filter(|member| member.healthy).count() as i32;
            status.replicas = members.len() as i32;
            status.members = members;
            status.stable_snapshot = Some(snapshot);
            status.operation = None;
            status.primary_failing_since = None;
            status.stable_election_metadata_refresh =
                Some(crate::crd::StableElectionMetadataRefreshStatus {
                    snapshot_epoch: status.epoch.clone(),
                    next_member_index: 0,
                    completed_members: Vec::new(),
                    pending_action: None,
                });
            set_remove_pilot_condition(
                &mut status,
                if completed {
                    "Completed"
                } else {
                    "CompensatedOrSafeFailure"
                },
                if completed {
                    "terminal checkpoint accepted before remove topology publication"
                } else {
                    "terminal checkpoint accepted before compensated topology publication"
                },
                now,
            );
            persist_committed_status(api, state, &set_key, set, &status).await?;
            if let Some(measurements) = runtime
                .measurements(&namespace, &name, set_uid, &reference.execution_id)
                .await
            {
                info!(
                    execution_id = reference.execution_id,
                    checkpoint_load_attempts = measurements.load_attempts,
                    checkpoint_write_attempts = measurements.write_attempts,
                    checkpoint_accepted_writes = measurements.accepted_writes,
                    checkpoint_conflicts = measurements.conflicts,
                    checkpoint_unknown_outcomes = measurements.unknown_outcomes,
                    checkpoint_definite_failures = measurements.definite_failures,
                    latest_authoritative_checkpoint_bytes =
                        ?measurements.latest_authoritative_checkpoint_bytes,
                    maximum_authoritative_checkpoint_bytes =
                        measurements.maximum_authoritative_checkpoint_bytes,
                    maximum_active_checkpoint_bytes =
                        measurements.maximum_active_checkpoint_bytes,
                    latest_terminal_checkpoint_bytes =
                        ?measurements.latest_terminal_checkpoint_bytes,
                    maximum_terminal_checkpoint_bytes =
                        measurements.maximum_terminal_checkpoint_bytes,
                    external_effect_count = ?measurements.completed_external_effect_count,
                    passive_observation_count =
                        ?measurements.completed_passive_observation_count,
                    durable_boundary_count = ?measurements.completed_activity_count,
                    "durable remove-replica pilot process summary"
                );
            }
            state.drivers.lock().await.remove(&set_key);
            runtime
                .forget(&namespace, &name, set_uid, &reference.execution_id)
                .await;
            Ok(ReconcileAction::Requeue(Duration::from_secs(1)))
        }
        DurableRemoveReplicaPilotTerminal::UnsafeAmbiguity { message, .. } => {
            record_remove_pilot_wait_condition(set, api, "Quarantined", &message, now).await;
            Ok(ReconcileAction::Requeue(Duration::from_secs(10)))
        }
        DurableRemoveReplicaPilotTerminal::Rejected { message, .. } => {
            record_remove_pilot_wait_condition(set, api, "Rejected", &message, now).await;
            Ok(ReconcileAction::Requeue(Duration::from_secs(10)))
        }
    }
}
// COMPLEXITY-BOUNDARY: remove-replica-reconcile-integration:end

async fn reconcile_durable_operation(
    set: &KubericSet,
    api: &dyn ClusterApi,
    state: &ReconcilerState,
    pods: &[Pod],
    ready_pods: &[&Pod],
    set_key: &str,
) -> Result<ReconcileAction, String> {
    let name = set.name_any();
    let namespace = set.namespace().unwrap_or_default();
    let operation = set
        .status
        .as_ref()
        .and_then(|status| status.operation.clone())
        .ok_or_else(|| "durable operation phase has no checkpoint".to_string())?;
    if operation.kind != DurableOperationKind::CreatePartition
        && operation.previous_snapshot.is_none()
    {
        let now = unix_seconds();
        let failed = fail_closed(
            &operation,
            "durable topology operation has no previous stable snapshot",
        );
        let mut status = set.status.clone().unwrap_or_default();
        status.operation = Some(failed.clone());
        set_operation_condition(&mut status, operation_condition(&failed, now));
        api.patch_set_status(
            &namespace,
            &name,
            &status,
            set.metadata.resource_version.as_deref(),
        )
        .await?;
        return Ok(ReconcileAction::Requeue(Duration::from_secs(10)));
    }
    let current_pods = checked_pods_by_id(pods)?;

    let identity_members = durable_identity_members(
        set.status
            .as_ref()
            .ok_or_else(|| "durable operation has no persisted status".to_string())?,
        &operation,
    )?;
    let identity_error = identity_members.iter().find_map(|member| {
        match current_pods.iter().find(|(id, _, _)| *id == member.id) {
            None => Some(format!(
                "durable operation replica {} has no current pod",
                member.id
            )),
            Some((_, instance_id, _)) if instance_id.as_str() != member.instance_id => {
                Some(format!(
                    "durable operation replica {} incarnation changed",
                    member.id
                ))
            }
            Some(_) => None,
        }
    });
    if let Some(error) = identity_error {
        let now = if operation.kind == DurableOperationKind::RemoveReplica {
            state.removal_clock.unix_seconds()
        } else {
            unix_seconds()
        };
        let failed = fail_closed(&operation, &error);
        let mut status = set.status.clone().unwrap_or_default();
        status.operation = Some(failed.clone());
        set_operation_condition(&mut status, operation_condition(&failed, now));
        api.patch_set_status(
            &namespace,
            &name,
            &status,
            set.metadata.resource_version.as_deref(),
        )
        .await?;
        return Ok(ReconcileAction::Requeue(Duration::from_secs(10)));
    }

    let mut handles: BTreeMap<ReplicaId, Box<dyn ReplicaHandle>> = BTreeMap::new();
    let mut observations = OperationObservations::new();
    let pod_identities: OperationPodIdentities = current_pods
        .iter()
        .map(|(id, instance_id, _)| (*id, instance_id.to_string()))
        .collect();
    for (replica_id, instance_id, pod) in &current_pods {
        let is_target_member = operation
            .target_snapshot
            .members
            .iter()
            .any(|member| member.id == *replica_id);
        let is_exact_remove_target = operation.kind == DurableOperationKind::RemoveReplica
            && operation.target_replica_id == Some(*replica_id)
            && operation.target_instance_id.as_deref() == Some(instance_id.as_str());
        if !is_target_member && !is_exact_remove_target {
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
                    "replica {replica_id} has unsupported or malformed control status during durable workflow: {error}"
                ));
            }
            Err(_) => {}
        }
        handles.insert(*replica_id, handle);
    }

    let now = if operation.kind == DurableOperationKind::RemoveReplica {
        state.removal_clock.unix_seconds()
    } else {
        unix_seconds()
    };
    let decision = match operation.kind {
        DurableOperationKind::CreatePartition => {
            decide_create_partition(&operation, &observations, &pod_identities, now)
        }
        DurableOperationKind::Switchover => decide(&operation, &observations, now),
        DurableOperationKind::AddReplica => {
            let target_pod_role_label = operation.target_replica_id.and_then(|target_id| {
                current_pods
                    .iter()
                    .find(|(id, _, _)| *id == target_id)
                    .and_then(|(_, _, pod)| {
                        pod.metadata
                            .labels
                            .as_ref()
                            .and_then(|labels| labels.get("kuberic.io/role"))
                    })
                    .map(String::as_str)
            });
            decide_add_replica(
                &operation,
                &observations,
                &pod_identities,
                target_pod_role_label,
                now,
            )
        }
        DurableOperationKind::RemoveReplica => {
            let target_pod_role_label = operation.target_replica_id.and_then(|target_id| {
                current_pods
                    .iter()
                    .find(|(id, instance_id, _)| {
                        *id == target_id
                            && operation.target_pod_uid.as_deref() == Some(instance_id.as_str())
                    })
                    .and_then(|(_, _, pod)| {
                        pod.metadata
                            .labels
                            .as_ref()
                            .and_then(|labels| labels.get("kuberic.io/role"))
                    })
                    .map(String::as_str)
            });
            decide_remove_replica(
                &operation,
                &observations,
                &pod_identities,
                target_pod_role_label,
                now,
            )
        }
        DurableOperationKind::Failover => decide_failover(&operation, &observations, now),
    };
    let decision = match decision {
        Ok(decision) => decision,
        Err(error) => {
            let mut status = set.status.clone().unwrap_or_default();
            set_operation_condition(
                &mut status,
                StatusCondition {
                    type_: "DurableOperation".to_string(),
                    status: "True".to_string(),
                    reason: "IncompatibleOrInvalid".to_string(),
                    message: error,
                    last_transition_time: now.to_string(),
                },
            );
            api.patch_set_status(
                &namespace,
                &name,
                &status,
                set.metadata.resource_version.as_deref(),
            )
            .await?;
            return Ok(ReconcileAction::Requeue(Duration::from_secs(10)));
        }
    };

    match decision {
        Decision::Persist(next_operation) => {
            let mut status = set.status.clone().unwrap_or_default();
            status.operation = Some(next_operation.clone());
            set_operation_condition(&mut status, operation_condition(&next_operation, now));
            api.patch_set_status(
                &namespace,
                &name,
                &status,
                set.metadata.resource_version.as_deref(),
            )
            .await?;
        }
        Decision::Execute {
            target_id,
            action_id,
            action,
        } => {
            let Some(pending) = operation.pending_action.as_ref() else {
                return Err(
                    "durable decision requested execution without pending intent".to_string(),
                );
            };
            if action_id != pending.action_id {
                return Err(
                    "durable decision action ID does not match persisted pending intent"
                        .to_string(),
                );
            }
            let Some(observed) = observations.get(&target_id) else {
                return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
            };
            let Some(handle) = handles.get(&target_id) else {
                return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
            };
            let coarse_intent = matches!(
                (operation.kind, pending.kind),
                (
                    DurableOperationKind::AddReplica,
                    crate::crd::DurableActionKind::AddReplicaIntent
                ) | (
                    DurableOperationKind::RemoveReplica,
                    crate::crd::DurableActionKind::RemoveReplicaIntent
                )
            );
            match plan_dispatch_evidence(
                pending,
                &observed.status,
                &handle.instance_id(),
                &action,
                !coarse_intent,
            ) {
                DispatchEvidencePlan::Persist(planned) => {
                    let mut next_operation = operation.clone();
                    next_operation.pending_action = Some(*planned);
                    let mut status = set.status.clone().unwrap_or_default();
                    status.operation = Some(next_operation.clone());
                    set_operation_condition(&mut status, operation_condition(&next_operation, now));
                    api.patch_set_status(
                        &namespace,
                        &name,
                        &status,
                        set.metadata.resource_version.as_deref(),
                    )
                    .await?;
                    return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
                }
                DispatchEvidencePlan::WaitForExactIncarnation
                | DispatchEvidencePlan::WaitForSupportedProtocol => {
                    return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
                }
                DispatchEvidencePlan::Ready => {}
            }
            let result = execute_planned_control_action(
                handle.as_ref(),
                pending,
                coarse_intent.then_some(action),
            )
            .await;
            if let Err(error) = result {
                let next_operation = operation_after_dispatch_error(&operation, &error);
                let mut status = set.status.clone().unwrap_or_default();
                status.operation = Some(next_operation.clone());
                set_operation_condition(&mut status, operation_condition(&next_operation, now));
                api.patch_set_status(
                    &namespace,
                    &name,
                    &status,
                    set.metadata.resource_version.as_deref(),
                )
                .await?;
            }
        }
        Decision::PatchPodRole { target_id, role } => {
            let pod_name = observations
                .get(&target_id)
                .map(|observation| observation.pod_name.clone())
                .or_else(|| {
                    (operation.kind == DurableOperationKind::AddReplica)
                        .then(|| {
                            current_pods.iter().find_map(|(id, instance_id, pod)| {
                                (*id == target_id
                                    && operation.target_instance_id.as_deref()
                                        == Some(instance_id.as_str()))
                                .then(|| pod.name_any())
                            })
                        })
                        .flatten()
                });
            let Some(pod_name) = pod_name else {
                return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
            };
            let mut labels = BTreeMap::new();
            labels.insert("kuberic.io/role".to_string(), role);
            if let Err(error) = api.patch_pod_labels(&namespace, &pod_name, labels).await {
                let next_operation = record_activity_error(&operation, &error);
                let mut status = set.status.clone().unwrap_or_default();
                status.operation = Some(next_operation.clone());
                set_operation_condition(&mut status, operation_condition(&next_operation, now));
                api.patch_set_status(
                    &namespace,
                    &name,
                    &status,
                    set.metadata.resource_version.as_deref(),
                )
                .await?;
            }
        }
        Decision::PatchPodRoleExactUid {
            target_id,
            expected_uid,
            role,
        } => {
            Box::pin(patch_pod_role_exact_uid(
                set,
                api,
                &operation,
                &current_pods,
                target_id,
                expected_uid,
                role,
                now,
            ))
            .await?;
        }
        Decision::DeletePod {
            pod_name,
            expected_uid,
        } => {
            if let Err(error) = api.delete_pod(&namespace, &pod_name, &expected_uid).await {
                let next_operation = record_activity_error(&operation, &error);
                let mut status = set.status.clone().unwrap_or_default();
                status.operation = Some(next_operation.clone());
                set_operation_condition(&mut status, operation_condition(&next_operation, now));
                api.patch_set_status(
                    &namespace,
                    &name,
                    &status,
                    set.metadata.resource_version.as_deref(),
                )
                .await?;
            }
        }
        Decision::CommitSnapshot {
            operation,
            snapshot,
        } => {
            let snapshot = snapshot_with_observed_metadata(snapshot, &observations);
            let mut status = set.status.clone().unwrap_or_default();
            status.epoch = snapshot.epoch.clone();
            status.ready_replicas = ready_pods.len() as i32;
            status.replicas = pods.len() as i32;
            status.members = build_member_status(pods, &set.spec);
            status.stable_snapshot = Some(snapshot);
            status.operation = Some(operation.clone());
            set_operation_condition(&mut status, operation_condition(&operation, now));
            if operation.kind == DurableOperationKind::RemoveReplica {
                api.patch_set_status(
                    &namespace,
                    &name,
                    &status,
                    set.metadata.resource_version.as_deref(),
                )
                .await?;
            } else {
                persist_committed_status(api, state, set_key, set, &status).await?;
            }
        }
        Decision::RecordCommitEvidence(operation) => {
            let mut status = set.status.clone().unwrap_or_default();
            status.operation = Some(operation.clone());
            set_operation_condition(&mut status, operation_condition(&operation, now));
            api.patch_set_status(
                &namespace,
                &name,
                &status,
                set.metadata.resource_version.as_deref(),
            )
            .await?;
        }
        Decision::Wait => {}
        Decision::Complete {
            operation,
            snapshot,
            compensated: _,
        } => {
            let snapshot = snapshot_with_observed_metadata(snapshot, &observations);
            let recovery_snapshot = snapshot.clone();
            let primary_name = pod_name_for_id(&current_pods, snapshot.primary_id)
                .ok_or_else(|| "completed snapshot primary has no current pod".to_string())?;
            let mut status = set.status.clone().unwrap_or_default();
            status.epoch = snapshot.epoch.clone();
            status.current_primary = Some(primary_name.clone());
            status.target_primary = Some(primary_name);
            status.phase = Phase::Healthy;
            status.reconfiguration_phase = ReconfigurationPhase::None;
            status.ready_replicas = ready_pods.len() as i32;
            status.replicas = pods.len() as i32;
            status.members = build_member_status_for_snapshot(pods, &set.spec, &snapshot);
            status.stable_snapshot = Some(snapshot);
            status.operation = Some(operation.clone());
            status.primary_failing_since = None;
            status.stable_election_metadata_refresh =
                Some(crate::crd::StableElectionMetadataRefreshStatus {
                    snapshot_epoch: status.epoch.clone(),
                    next_member_index: 0,
                    completed_members: Vec::new(),
                    pending_action: None,
                });
            set_operation_condition(&mut status, operation_condition(&operation, now));
            if operation.kind == DurableOperationKind::RemoveReplica {
                api.patch_set_status(
                    &namespace,
                    &name,
                    &status,
                    set.metadata.resource_version.as_deref(),
                )
                .await?;
            } else {
                persist_committed_status(api, state, set_key, set, &status).await?;
            }
            if operation.kind == DurableOperationKind::CreatePartition {
                let core_snapshot = StablePartitionSnapshot::try_from(&recovery_snapshot)
                    .map_err(|error| format!("invalid completed creation snapshot: {error}"))?;
                let mut recovery_handles = Vec::new();
                for member in &recovery_snapshot.members {
                    let handle = handles.remove(&member.id).ok_or_else(|| {
                        format!(
                            "completed creation member {} has no recovery handle",
                            member.id
                        )
                    })?;
                    recovery_handles.push(handle);
                }
                let driver = PartitionDriver::recover(core_snapshot, recovery_handles)
                    .await
                    .map_err(|error| {
                        format!("completed creation driver recovery failed: {error}")
                    })?;
                state
                    .drivers
                    .lock()
                    .await
                    .insert(set_key.to_string(), driver);
            } else {
                state.drivers.lock().await.remove(set_key);
            }
        }
        Decision::CompleteDegraded {
            operation,
            snapshot,
        } => {
            let snapshot = snapshot_with_observed_metadata(snapshot, &observations);
            let primary_name = pod_name_for_id(&current_pods, snapshot.primary_id);
            let mut status = set.status.clone().unwrap_or_default();
            status.epoch = snapshot.epoch.clone();
            status.current_primary = primary_name.clone();
            status.target_primary = primary_name;
            status.phase = Phase::Healthy;
            status.reconfiguration_phase = ReconfigurationPhase::None;
            status.ready_replicas = ready_pods.len() as i32;
            status.replicas = pods.len() as i32;
            status.members = build_member_status_for_snapshot(pods, &set.spec, &snapshot);
            status.stable_snapshot = Some(snapshot);
            status.operation = Some(operation);
            status.primary_failing_since = None;
            set_operation_condition(
                &mut status,
                StatusCondition {
                    type_: "DurableOperation".to_string(),
                    status: "True".to_string(),
                    reason: "CommittedDegraded".to_string(),
                    message: "replica add current configuration committed; final serving attestation is pending recovery".to_string(),
                    last_transition_time: now.to_string(),
                },
            );
            persist_committed_status(api, state, set_key, set, &status).await?;
            state.drivers.lock().await.remove(set_key);
        }
        Decision::RestartCreation { operation } => {
            let mut status = set.status.clone().unwrap_or_default();
            status.phase = Phase::Creating;
            status.operation = Some(operation.clone());
            status.stable_snapshot = None;
            status.current_primary = None;
            status.target_primary = None;
            set_operation_condition(&mut status, operation_condition(&operation, now));
            persist_committed_status(api, state, set_key, set, &status).await?;
            state.drivers.lock().await.remove(set_key);
        }
    }

    Ok(ReconcileAction::Requeue(Duration::from_secs(1)))
}

#[allow(clippy::too_many_arguments)]
async fn patch_pod_role_exact_uid(
    set: &KubericSet,
    api: &dyn ClusterApi,
    operation: &DurableOperationStatus,
    current_pods: &[(ReplicaId, ReplicaInstanceId, &Pod)],
    target_id: ReplicaId,
    expected_uid: String,
    role: String,
    now: i64,
) -> Result<(), String> {
    let Some((_, instance_id, pod)) = current_pods.iter().find(|(id, _, _)| *id == target_id)
    else {
        return Ok(());
    };
    if instance_id.as_str() != expected_uid {
        return Ok(());
    }

    let namespace = set.namespace().unwrap_or_default();
    let mut labels = BTreeMap::new();
    labels.insert("kuberic.io/role".to_string(), role);
    if let Err(error) = api
        .patch_pod_labels_if_uid(&namespace, &pod.name_any(), &expected_uid, labels)
        .await
    {
        let next_operation = record_activity_error(operation, &error);
        let mut status = set.status.clone().unwrap_or_default();
        status.operation = Some(next_operation.clone());
        set_operation_condition(&mut status, operation_condition(&next_operation, now));
        api.patch_set_status(
            &namespace,
            &set.name_any(),
            &status,
            set.metadata.resource_version.as_deref(),
        )
        .await?;
    }
    Ok(())
}

async fn persist_committed_status(
    api: &dyn ClusterApi,
    state: &ReconcilerState,
    set_key: &str,
    set: &KubericSet,
    status: &KubericSetStatus,
) -> Result<(), String> {
    let namespace = set.namespace().unwrap_or_default();
    let name = set.name_any();
    match api
        .patch_set_status(
            &namespace,
            &name,
            status,
            set.metadata.resource_version.as_deref(),
        )
        .await
    {
        Ok(()) => {
            state.pending_statuses.lock().await.remove(set_key);
            Ok(())
        }
        Err(error) => {
            state.pending_statuses.lock().await.insert(
                set_key.to_string(),
                PendingCommittedStatus {
                    owner_uid: set.metadata.uid.clone(),
                    status: status.clone(),
                },
            );
            Err(error)
        }
    }
}

/// Build the current logical/incarnation identity view from required pod
/// metadata. Kubernetes list position is never an identity source.
fn checked_pods_by_id(pods: &[Pod]) -> Result<Vec<CurrentPod<'_>>, String> {
    let mut result = Vec::with_capacity(pods.len());
    let mut ids = HashSet::new();
    let mut instances = HashSet::new();
    for pod in pods {
        let pod_name = pod.name_any();
        let index = pod
            .metadata
            .labels
            .as_ref()
            .and_then(|labels| labels.get("kuberic.io/pod-index"))
            .ok_or_else(|| format!("pod {pod_name} has no kuberic.io/pod-index label"))?
            .parse::<ReplicaId>()
            .map_err(|error| {
                format!("pod {pod_name} has invalid kuberic.io/pod-index label: {error}")
            })?;
        if index < 0 {
            return Err(format!(
                "pod {pod_name} has negative kuberic.io/pod-index label"
            ));
        }

        let replica_id = index
            .checked_add(1)
            .ok_or_else(|| format!("pod {pod_name} has overflowing kuberic.io/pod-index label"))?;
        if !ids.insert(replica_id) {
            return Err(format!("duplicate pod logical replica ID {replica_id}"));
        }
        let instance_id = pod
            .metadata
            .uid
            .as_ref()
            .filter(|uid| !uid.is_empty())
            .cloned()
            .map(ReplicaInstanceId::new)
            .ok_or_else(|| format!("pod {pod_name} has no UID"))?;
        if !instances.insert(instance_id.clone()) {
            return Err(format!("duplicate pod incarnation {instance_id}"));
        }
        result.push((replica_id, instance_id, pod));
    }
    result.sort_by_key(|(id, _, _)| *id);
    Ok(result)
}

fn validate_creation_ids(current_pods: &[CurrentPod<'_>], desired: usize) -> Result<(), String> {
    if current_pods.len() != desired {
        return Err(format!(
            "partition creation expected {desired} pods but found {}",
            current_pods.len()
        ));
    }
    for (index, (replica_id, _, _)) in current_pods.iter().enumerate() {
        let expected = index as i64 + 1;
        if *replica_id != expected {
            return Err(format!(
                "partition creation expected logical replica ID {expected} but found {replica_id}"
            ));
        }
    }
    Ok(())
}

/// Before every Healthy topology decision, ensure current pod identities still
/// attest the process-local handles. Extra identities are allowed only while
/// the desired replica count is larger than the committed driver membership.
fn validate_pod_handle_identities(
    driver: &PartitionDriver,
    current_pods: &[CurrentPod<'_>],
    desired: usize,
) -> Result<(), String> {
    let driver_ids = driver.replica_ids();
    for replica_id in &driver_ids {
        let (_, current_instance, _) = current_pods
            .iter()
            .find(|(id, _, _)| id == replica_id)
            .ok_or_else(|| format!("current pod for driver replica {replica_id} is missing"))?;
        let handle = driver
            .handle(*replica_id)
            .ok_or_else(|| format!("driver handle {replica_id} is missing"))?;
        let handle_instance = handle.instance_id();
        if *current_instance != handle_instance {
            return Err(format!(
                "current pod incarnation drift for replica {replica_id}: driver {handle_instance}, pod {current_instance}"
            ));
        }
    }

    let extras: Vec<_> = current_pods
        .iter()
        .filter(|(id, _, _)| driver.handle(*id).is_none())
        .map(|(id, _, _)| *id)
        .collect();
    if !extras.is_empty() && driver_ids.len() >= desired {
        return Err(format!(
            "current pod logical identities are not a bijection with driver handles; extra replicas: {extras:?}"
        ));
    }
    Ok(())
}

fn pod_name_for_id(current_pods: &[CurrentPod<'_>], id: ReplicaId) -> Option<String> {
    current_pods
        .iter()
        .find(|(current_id, _, _)| *current_id == id)
        .map(|(_, _, pod)| pod.name_any())
}

fn historical_remove_replicator_address(set: &KubericSet, replica_id: ReplicaId) -> String {
    set.status
        .as_ref()
        .and_then(|status| status.members.iter().find(|member| member.id == replica_id))
        .map(|member| member.data_address.clone())
        .filter(|address| !address.is_empty())
        .unwrap_or_else(|| {
            format!(
                "http://{}-{}:{}",
                set.name_any(),
                replica_id.saturating_sub(1),
                set.spec.data_port
            )
        })
}

fn snapshot_status(driver: &PartitionDriver) -> Result<StablePartitionSnapshotStatus, String> {
    let snapshot = driver
        .stable_snapshot()
        .map_err(|error| format!("cannot persist stable snapshot: {error}"))?;
    StablePartitionSnapshotStatus::try_from(&snapshot)
}

fn is_pod_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|conditions| {
            conditions
                .iter()
                .any(|c| c.type_ == "Ready" && c.status == "True")
        })
        .unwrap_or(false)
}

fn build_member_status(pods: &[Pod], spec: &KubericSetSpec) -> Vec<MemberStatus> {
    pods.iter()
        .map(|pod| {
            let name = pod.name_any();
            let labels = pod.metadata.labels.as_ref();
            let role = labels
                .and_then(|l| l.get("kuberic.io/role"))
                .cloned()
                .unwrap_or_default();
            let id: i64 = labels
                .and_then(|l| l.get("kuberic.io/pod-index"))
                .and_then(|v| v.parse().ok())
                .unwrap_or(0)
                + 1;
            let pod_ip = pod
                .status
                .as_ref()
                .and_then(|s| s.pod_ip.as_ref())
                .cloned()
                .unwrap_or_default();

            MemberStatus {
                name,
                id,
                instance_id: pod.metadata.uid.clone().unwrap_or_default(),
                role,
                current_progress: 0,
                healthy: is_pod_ready(pod),
                control_address: format!("http://{}:{}", pod_ip, spec.control_port),
                data_address: format!("http://{}:{}", pod_ip, spec.data_port),
            }
        })
        .collect()
}

fn build_member_status_for_snapshot(
    pods: &[Pod],
    spec: &KubericSetSpec,
    snapshot: &StablePartitionSnapshotStatus,
) -> Vec<MemberStatus> {
    let mut members = build_member_status(pods, spec);
    for member in &mut members {
        if let Some(stable) = snapshot
            .members
            .iter()
            .find(|stable| stable.id == member.id)
        {
            member.role = match stable.role {
                crate::crd::StableReplicaRoleStatus::Primary => "primary",
                crate::crd::StableReplicaRoleStatus::ActiveSecondary => "secondary",
            }
            .to_string();
        }
    }
    members
}

fn snapshot_with_observed_metadata(
    mut snapshot: StablePartitionSnapshotStatus,
    observations: &OperationObservations,
) -> StablePartitionSnapshotStatus {
    for member in &mut snapshot.members {
        let Some(observation) = observations.get(&member.id) else {
            continue;
        };
        if observation.status.instance_id.as_str() != member.instance_id
            || observation.status.epoch
                != Epoch::new(
                    snapshot.epoch.data_loss_number,
                    snapshot.epoch.configuration_number,
                )
        {
            continue;
        }
        let (Some(first_retained_lsn), Some(deactivation)) = (
            observation.status.catch_up_capability,
            observation.status.deactivation_info,
        ) else {
            continue;
        };
        member.election_metadata = Some(StableReplicaElectionMetadataStatus {
            current_lsn: observation.status.current_progress,
            committed_lsn: observation.status.committed_lsn,
            first_retained_lsn,
            deactivation_epoch: crate::crd::EpochStatus {
                data_loss_number: deactivation.epoch.data_loss_number,
                configuration_number: deactivation.epoch.configuration_number,
            },
            deactivation_catch_up_lsn: deactivation.catch_up_lsn,
        });
    }
    snapshot
}

fn set_operation_condition(status: &mut KubericSetStatus, condition: StatusCondition) {
    status
        .conditions
        .retain(|existing| existing.type_ != condition.type_);
    status.conditions.push(condition);
}

#[cfg(feature = "durable-switchover-pilot")]
fn set_pilot_condition(status: &mut KubericSetStatus, reason: &str, message: &str, now: i64) {
    set_operation_condition(
        status,
        StatusCondition {
            type_: "DurableSwitchoverPilot".to_string(),
            status: if matches!(reason, "Completed" | "CompensatedOrSafeFailure") {
                "False".to_string()
            } else {
                "True".to_string()
            },
            reason: reason.to_string(),
            message: message.chars().take(512).collect(),
            last_transition_time: now.to_string(),
        },
    );
}

// COMPLEXITY-BOUNDARY: remove-replica-status-integration:start
#[cfg(feature = "durable-remove-replica-pilot")]
fn set_remove_pilot_condition(
    status: &mut KubericSetStatus,
    reason: &str,
    message: &str,
    now: i64,
) {
    set_operation_condition(
        status,
        StatusCondition {
            type_: "DurableRemoveReplicaPilot".to_string(),
            status: if matches!(reason, "Completed" | "CompensatedOrSafeFailure") {
                "False".to_string()
            } else {
                "True".to_string()
            },
            reason: reason.to_string(),
            message: message.chars().take(512).collect(),
            last_transition_time: now.to_string(),
        },
    );
}

#[cfg(feature = "durable-remove-replica-pilot")]
async fn record_remove_pilot_wait_condition(
    set: &KubericSet,
    api: &dyn ClusterApi,
    reason: &str,
    message: &str,
    now: i64,
) {
    let normalized_message: String = message.chars().take(512).collect();
    let desired_status = if matches!(reason, "Completed" | "CompensatedOrSafeFailure") {
        "False"
    } else {
        "True"
    };
    let unchanged = set.status.as_ref().is_some_and(|status| {
        status.conditions.iter().any(|condition| {
            condition.type_ == "DurableRemoveReplicaPilot"
                && condition.reason == reason
                && condition.status == desired_status
                && condition.message == normalized_message
        })
    });
    if unchanged {
        return;
    }
    let mut status = set.status.clone().unwrap_or_default();
    set_remove_pilot_condition(&mut status, reason, &normalized_message, now);
    let _ = api
        .patch_set_status(
            &set.namespace().unwrap_or_default(),
            &set.name_any(),
            &status,
            set.metadata.resource_version.as_deref(),
        )
        .await;
}
// COMPLEXITY-BOUNDARY: remove-replica-status-integration:end

// COMPLEXITY-BOUNDARY: shared-durable-terminal-cleanup:start
#[cfg(any(
    feature = "durable-switchover-pilot",
    feature = "durable-remove-replica-pilot"
))]
async fn cleanup_persisted_durable_execution(
    state: &ReconcilerState,
    set: &KubericSet,
    status: &KubericSetStatus,
    set_key: &str,
) {
    let Some(uid) = set.metadata.uid.as_deref() else {
        return;
    };
    let mut cleaned = false;
    #[cfg(feature = "durable-switchover-pilot")]
    if let (Some(reference), Some(runtime)) = (
        status.durable_switchover_pilot.as_ref(),
        state.durable_switchover_pilot.as_ref(),
    ) {
        runtime
            .forget(
                &set.namespace().unwrap_or_default(),
                &set.name_any(),
                uid,
                &reference.execution_id,
            )
            .await;
        cleaned = true;
    }
    #[cfg(feature = "durable-remove-replica-pilot")]
    if let (Some(reference), Some(runtime)) = (
        status.durable_remove_replica_pilot.as_ref(),
        state.durable_remove_replica_pilot.as_ref(),
    ) {
        runtime
            .forget(
                &set.namespace().unwrap_or_default(),
                &set.name_any(),
                uid,
                &reference.execution_id,
            )
            .await;
        cleaned = true;
    }
    if cleaned {
        state.drivers.lock().await.remove(set_key);
    }
}
// COMPLEXITY-BOUNDARY: shared-durable-terminal-cleanup:end

fn unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

async fn create_pods(
    api: &dyn ClusterApi,
    set: &KubericSet,
    namespace: &str,
) -> Result<(), String> {
    for i in 0..set.spec.replicas {
        ensure_pvc(api, set, namespace, i).await?;
        ensure_pod(api, set, namespace, i).await?;
    }
    Ok(())
}

async fn ensure_pvc(
    api: &dyn ClusterApi,
    set: &KubericSet,
    namespace: &str,
    index: i32,
) -> Result<(), String> {
    let name = format!("{}-{}-data", set.name_any(), index);
    if api.get_pvc(namespace, &name).await.is_ok() {
        return Ok(());
    }
    let pvc = build_pvc(set, namespace, index);
    api.create_pvc(namespace, &pvc).await
}

async fn ensure_pod(
    api: &dyn ClusterApi,
    set: &KubericSet,
    namespace: &str,
    index: i32,
) -> Result<(), String> {
    let pod = build_pod(set, namespace, index);
    // create_pod is already idempotent (409 → Ok)
    api.create_pod(namespace, &pod).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use kuberic_core::remove_replica::ManualRemoveReplicaClock;

    #[test]
    fn explicit_switchover_engine_is_always_available() {
        assert!(
            validate_new_switchover_engine(crate::crd::SwitchoverExecutionMode::Explicit).is_ok()
        );
    }

    #[test]
    fn durable_pilot_never_silently_falls_back_to_explicit() {
        #[cfg(feature = "durable-switchover-pilot")]
        assert!(
            validate_new_switchover_engine(crate::crd::SwitchoverExecutionMode::DurablePilot)
                .is_ok()
        );
        #[cfg(not(feature = "durable-switchover-pilot"))]
        {
            let error =
                validate_new_switchover_engine(crate::crd::SwitchoverExecutionMode::DurablePilot)
                    .unwrap_err();
            assert!(error.contains("requires the operator"), "{error}");
        }
    }

    #[test]
    fn explicit_remove_replica_engine_is_always_available() {
        assert!(validate_new_remove_replica_engine(RemoveReplicaExecutionMode::Explicit).is_ok());
    }

    #[test]
    fn durable_remove_replica_never_silently_falls_back_to_explicit() {
        #[cfg(feature = "durable-remove-replica-pilot")]
        assert!(
            validate_new_remove_replica_engine(RemoveReplicaExecutionMode::DurablePilot).is_ok()
        );
        #[cfg(not(feature = "durable-remove-replica-pilot"))]
        {
            let error =
                validate_new_remove_replica_engine(RemoveReplicaExecutionMode::DurablePilot)
                    .unwrap_err();
            assert!(error.contains("requires the operator"), "{error}");
        }
    }

    fn snapshot(primary_id: i64, member_ids: &[i64]) -> StablePartitionSnapshotStatus {
        StablePartitionSnapshotStatus {
            epoch: crate::crd::EpochStatus {
                data_loss_number: 1,
                configuration_number: 2,
            },
            primary_id,
            members: member_ids
                .iter()
                .map(|id| crate::crd::StableReplicaSnapshotStatus {
                    id: *id,
                    instance_id: format!("uid-{id}"),
                    role: if *id == primary_id {
                        StableReplicaRoleStatus::Primary
                    } else {
                        StableReplicaRoleStatus::ActiveSecondary
                    },
                    election_metadata: None,
                })
                .collect(),
            write_quorum: member_ids.len() as u32 / 2 + 1,
        }
    }

    #[test]
    fn committed_snapshot_drives_active_removal_recovery_only_until_publication() {
        let previous = snapshot(1, &[1, 2, 3]);
        let reduced = snapshot(1, &[1, 2]);
        let operation = crate::durable::start_remove_replica(
            "set",
            previous.clone(),
            RemoveReplicaTarget {
                replica_id: 3,
                pod_name: "set-2".to_string(),
                pod_uid: "uid-3".to_string(),
                replicator_address: "http://set-2:9091".to_string(),
                agent_generation: None,
            },
            DurableRemoveMode::Force,
            2,
            10,
        )
        .unwrap();
        let mut operation = operation;
        operation.phase = DurableOperationPhase::RemoveAwaitCleanup;
        operation.committed_snapshot = Some(reduced.clone());
        operation.remove_commit_evidence = Some(crate::crd::RemoveReplicaCommitEvidenceStatus {
            attempt_id: "attempt".to_string(),
            action_id: "action".to_string(),
            primary_agent_generation: "generation".to_string(),
            configuration_signature: "signature".to_string(),
            observed_unix_seconds: 20,
        });
        let mut status = KubericSetStatus {
            phase: Phase::RemovingReplica,
            stable_snapshot: Some(previous.clone()),
            operation: Some(operation),
            ..Default::default()
        };
        status.stable_snapshot.as_mut().unwrap().members[1].election_metadata =
            Some(StableReplicaElectionMetadataStatus {
                current_lsn: 99,
                committed_lsn: 99,
                first_retained_lsn: 1,
                deactivation_epoch: crate::crd::EpochStatus {
                    data_loss_number: 1,
                    configuration_number: 2,
                },
                deactivation_catch_up_lsn: 99,
            });
        assert_eq!(authoritative_topology_snapshot(&status), Some(&reduced));
        assert!(
            durable_identity_members(&status, status.operation.as_ref().unwrap())
                .unwrap()
                .is_empty(),
            "committed removal must not fail closed on retained-member pod churn"
        );

        status.operation.as_mut().unwrap().phase = DurableOperationPhase::RemoveFinalize;
        status.stable_snapshot = Some(reduced.clone());
        status.stable_snapshot.as_mut().unwrap().members[1].election_metadata =
            Some(StableReplicaElectionMetadataStatus {
                current_lsn: 7,
                committed_lsn: 7,
                first_retained_lsn: 1,
                deactivation_epoch: crate::crd::EpochStatus {
                    data_loss_number: 1,
                    configuration_number: 2,
                },
                deactivation_catch_up_lsn: 7,
            });
        assert_eq!(
            authoritative_topology_snapshot(&status),
            status.stable_snapshot.as_ref()
        );
    }

    #[test]
    fn active_other_operation_checkpoint_is_rejected_without_a_mutation_decision() {
        let operation = crate::durable::start_remove_replica(
            "set",
            snapshot(1, &[1, 2, 3]),
            RemoveReplicaTarget {
                replica_id: 3,
                pod_name: "set-2".to_string(),
                pod_uid: "uid-3".to_string(),
                replicator_address: "http://set-2:9091".to_string(),
                agent_generation: None,
            },
            DurableRemoveMode::Force,
            2,
            10,
        )
        .unwrap();
        let status = KubericSetStatus {
            phase: Phase::Healthy,
            operation: Some(operation.clone()),
            ..Default::default()
        };
        let error = validate_active_operation_phase(Some(&status), &status.phase).unwrap_err();
        assert!(error.contains("requires RemovingReplica"));

        let mut terminal = status;
        terminal.operation.as_mut().unwrap().phase = DurableOperationPhase::Poisoned;
        assert!(validate_active_operation_phase(Some(&terminal), &terminal.phase).is_ok());
    }

    #[test]
    fn reconciler_removal_clock_is_injected_independently() {
        let clock = ManualRemoveReplicaClock::new(42);
        let state = ReconcilerState::with_removal_clock(Arc::new(clock.clone()));
        assert_eq!(state.removal_clock.unix_seconds(), 42);
        clock.advance(18);
        assert_eq!(state.removal_clock.unix_seconds(), 60);
    }
}

fn build_pod(set: &KubericSet, namespace: &str, index: i32) -> Pod {
    let name = format!("{}-{}", set.name_any(), index);
    let set_name = set.name_any();
    let replica_id = (index + 1).to_string();

    let mut labels = BTreeMap::new();
    labels.insert("kuberic.io/set".into(), set_name.clone());
    labels.insert("kuberic.io/role".into(), "bootstrap".into());
    labels.insert("kuberic.io/pod-index".into(), index.to_string());

    let owner_ref = serde_json::from_value(serde_json::json!({
        "apiVersion": "kuberic.io/v1",
        "kind": "KubericSet",
        "name": set_name,
        "uid": set.metadata.uid.as_deref().unwrap_or(""),
        "controller": true,
        "blockOwnerDeletion": true
    }))
    .expect("valid owner reference");

    let readiness_probe = Probe {
        tcp_socket: Some(TCPSocketAction {
            port: IntOrString::Int(set.spec.control_port),
            ..Default::default()
        }),
        initial_delay_seconds: Some(5),
        period_seconds: Some(5),
        timeout_seconds: Some(3),
        failure_threshold: Some(2),
        ..Default::default()
    };

    let liveness_probe = Probe {
        tcp_socket: Some(TCPSocketAction {
            port: IntOrString::Int(set.spec.control_port),
            ..Default::default()
        }),
        initial_delay_seconds: Some(10),
        period_seconds: Some(10),
        timeout_seconds: Some(5),
        failure_threshold: Some(3),
        ..Default::default()
    };

    let env = vec![
        k8s_openapi::api::core::v1::EnvVar {
            name: "KUBERIC_REPLICA_ID".into(),
            value: Some(replica_id),
            ..Default::default()
        },
        k8s_openapi::api::core::v1::EnvVar {
            name: "KUBERIC_REPLICA_INSTANCE_ID".into(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    api_version: Some("v1".into()),
                    field_path: "metadata.uid".into(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        k8s_openapi::api::core::v1::EnvVar {
            name: "KUBERIC_CONTROL_BIND".into(),
            value: Some(format!("0.0.0.0:{}", set.spec.control_port)),
            ..Default::default()
        },
        k8s_openapi::api::core::v1::EnvVar {
            name: "KUBERIC_DATA_BIND".into(),
            value: Some(format!("0.0.0.0:{}", set.spec.data_port)),
            ..Default::default()
        },
        k8s_openapi::api::core::v1::EnvVar {
            name: "KUBERIC_CLIENT_BIND".into(),
            value: Some(format!("0.0.0.0:{}", set.spec.port)),
            ..Default::default()
        },
        k8s_openapi::api::core::v1::EnvVar {
            name: "RUST_LOG".into(),
            value: Some("info".into()),
            ..Default::default()
        },
    ];

    Pod {
        metadata: kube::api::ObjectMeta {
            name: Some(name),
            namespace: Some(namespace.into()),
            labels: Some(labels),
            owner_references: Some(vec![owner_ref]),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "app".into(),
                image: Some(set.spec.image.clone()),
                image_pull_policy: Some("IfNotPresent".into()),
                ports: Some(vec![
                    ContainerPort {
                        container_port: set.spec.port,
                        name: Some("app".into()),
                        ..Default::default()
                    },
                    ContainerPort {
                        container_port: set.spec.control_port,
                        name: Some("control".into()),
                        ..Default::default()
                    },
                    ContainerPort {
                        container_port: set.spec.data_port,
                        name: Some("data".into()),
                        ..Default::default()
                    },
                ]),
                env: Some(env),
                readiness_probe: Some(readiness_probe),
                liveness_probe: Some(liveness_probe),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn build_pvc(set: &KubericSet, namespace: &str, index: i32) -> PersistentVolumeClaim {
    let name = format!("{}-{}-data", set.name_any(), index);
    let set_name = set.name_any();

    let mut labels = BTreeMap::new();
    labels.insert("kuberic.io/set".into(), set_name);
    labels.insert("kuberic.io/pod-index".into(), index.to_string());

    let mut requests = BTreeMap::new();
    requests.insert("storage".into(), Quantity(set.spec.storage.clone()));

    PersistentVolumeClaim {
        metadata: kube::api::ObjectMeta {
            name: Some(name),
            namespace: Some(namespace.into()),
            labels: Some(labels),
            ..Default::default()
        },
        spec: Some(PersistentVolumeClaimSpec {
            access_modes: Some(vec!["ReadWriteOnce".into()]),
            resources: Some(VolumeResourceRequirements {
                requests: Some(requests),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn create_services(
    api: &dyn ClusterApi,
    set: &KubericSet,
    namespace: &str,
) -> Result<(), String> {
    let set_name = set.name_any();

    // -rw: routes to primary only
    let mut rw_selector = BTreeMap::new();
    rw_selector.insert("kuberic.io/set".into(), set_name.clone());
    rw_selector.insert("kuberic.io/role".into(), "primary".into());
    api.create_service(
        namespace,
        &build_service(
            &set_name,
            namespace,
            &format!("{}-rw", set_name),
            rw_selector,
            &set.spec,
        ),
    )
    .await?;

    // -ro: routes to secondaries only
    let mut ro_selector = BTreeMap::new();
    ro_selector.insert("kuberic.io/set".into(), set_name.clone());
    ro_selector.insert("kuberic.io/role".into(), "secondary".into());
    api.create_service(
        namespace,
        &build_service(
            &set_name,
            namespace,
            &format!("{}-ro", set_name),
            ro_selector,
            &set.spec,
        ),
    )
    .await?;

    // -r: routes to all pods
    let mut r_selector = BTreeMap::new();
    r_selector.insert("kuberic.io/set".into(), set_name.clone());
    api.create_service(
        namespace,
        &build_service(
            &set_name,
            namespace,
            &format!("{}-r", set_name),
            r_selector,
            &set.spec,
        ),
    )
    .await?;

    Ok(())
}

fn build_service(
    set_name: &str,
    namespace: &str,
    name: &str,
    selector: BTreeMap<String, String>,
    spec: &KubericSetSpec,
) -> Service {
    let mut labels = BTreeMap::new();
    labels.insert("kuberic.io/set".into(), set_name.into());

    Service {
        metadata: kube::api::ObjectMeta {
            name: Some(name.into()),
            namespace: Some(namespace.into()),
            labels: Some(labels),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(selector),
            ports: Some(vec![
                ServicePort {
                    port: spec.port,
                    name: Some("app".into()),
                    ..Default::default()
                },
                ServicePort {
                    port: spec.control_port,
                    name: Some("control".into()),
                    ..Default::default()
                },
                ServicePort {
                    port: spec.data_port,
                    name: Some("data".into()),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod dispatch_planning_tests {
    use super::*;
    #[cfg(feature = "durable-switchover-pilot")]
    use crate::durable::effects::{LabelEffectCommand, ReplicaEffectCommand};
    use kuberic_core::types::{
        AccessStatus, AgentControlVersion, AgentGeneration, CorrelatedActionObservation,
        DurableActionErrorClass, DurableActionObservation, ReplicaAgentStatus,
    };
    #[cfg(feature = "durable-switchover-pilot")]
    use std::sync::{
        Arc as StdArc, Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    };

    fn pending() -> PendingActionStatus {
        PendingActionStatus {
            action_id: "operation:1".to_string(),
            sequence: 1,
            kind: crate::crd::DurableActionKind::RevokeWrite,
            target_id: 1,
            target_instance_id: "pod-uid".to_string(),
            expected_epoch: crate::crd::EpochStatus {
                data_loss_number: 0,
                configuration_number: 2,
            },
            desired_postcondition: crate::crd::DurablePostconditionStatus {
                kind: crate::crd::DurablePostconditionKind::WriteRevoked,
                role: None,
            },
            attempts: 3,
            deadline_unix_seconds: 100,
            last_error: None,
            dispatch_authorized: true,
            dispatch_agent_generation: None,
            dispatch_agent_control_version: None,
            dispatch_observed_runtime_epoch: None,
            dispatch_action_payload: String::new(),
        }
    }

    fn observed(protocol_version: u32) -> ReplicaStatusInfo {
        ReplicaStatusInfo {
            instance_id: ReplicaInstanceId::new("pod-uid"),
            role: kuberic_core::types::Role::Primary,
            epoch: Epoch::new(1, 4),
            current_progress: 0,
            catch_up_capability: Some(0),
            committed_lsn: 0,
            healthy: true,
            write_status: AccessStatus::Granted,
            configuration: None,
            election_configuration: None,
            deactivation_info: None,
            active_replica_connections: Vec::new(),
            build_observation: None,
            agent: ReplicaAgentStatus {
                protocol_version,
                lifecycle_peer_protocol_version:
                    kuberic_core::replica_lifecycle::REPLICA_LIFECYCLE_PEER_PROTOCOL_VERSION,
                generation: AgentGeneration::parse("0123456789abcdef0123456789abcdef").unwrap(),
                control_version: AgentControlVersion::new(7),
                current_action: None,
                retained_terminal_actions: Vec::new(),
                local_faults: Vec::new(),
            },
        }
    }

    fn action() -> DurableReplicaAction {
        DurableReplicaAction::RevokeWriteStatus
    }

    #[cfg(feature = "durable-switchover-pilot")]
    fn pilot_snapshot() -> StablePartitionSnapshotStatus {
        StablePartitionSnapshotStatus {
            epoch: crate::crd::EpochStatus {
                data_loss_number: 1,
                configuration_number: 4,
            },
            primary_id: 1,
            members: vec![
                StableReplicaSnapshotStatus {
                    id: 1,
                    instance_id: "pod-uid".to_string(),
                    role: StableReplicaRoleStatus::Primary,
                    election_metadata: None,
                },
                StableReplicaSnapshotStatus {
                    id: 2,
                    instance_id: "target-uid".to_string(),
                    role: StableReplicaRoleStatus::ActiveSecondary,
                    election_metadata: None,
                },
            ],
            write_quorum: 2,
        }
    }

    #[cfg(feature = "durable-switchover-pilot")]
    fn pilot_operation() -> DurableOperationStatus {
        crate::durable::start_switchover("set-uid", pilot_snapshot(), 2, 100).unwrap()
    }

    #[cfg(feature = "durable-switchover-pilot")]
    fn pilot_observations(status: ReplicaStatusInfo) -> OperationObservations {
        BTreeMap::from([(
            1,
            ReplicaObservation {
                status,
                control_address: "http://primary".to_string(),
                replicator_address: "http://primary-data".to_string(),
                pod_name: "database-0".to_string(),
                pod_role_label: Some("primary".to_string()),
            },
        )])
    }

    #[cfg(feature = "durable-switchover-pilot")]
    fn pilot_pending_with_fence() -> (DurableOperationStatus, ReplicaStatusInfo) {
        let operation = pilot_operation();
        let Decision::Persist(mut operation) =
            decide(&operation, &OperationObservations::new(), 100).unwrap()
        else {
            panic!("expected revoke intent");
        };
        let observed = observed(kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION);
        let pending = operation.pending_action.take().unwrap();
        operation.pending_action = Some(persisted(plan_dispatch_evidence(
            &pending,
            &observed,
            &ReplicaInstanceId::new("pod-uid"),
            &action(),
            true,
        )));
        (operation, observed)
    }

    #[cfg(feature = "durable-switchover-pilot")]
    struct BusyPilotHandle {
        requests: StdArc<StdMutex<Vec<CorrelatedControlActionRequest>>>,
        refresh_required: bool,
    }

    #[cfg(feature = "durable-switchover-pilot")]
    #[async_trait::async_trait]
    impl ReplicaHandle for BusyPilotHandle {
        fn id(&self) -> ReplicaId {
            1
        }

        fn instance_id(&self) -> ReplicaInstanceId {
            ReplicaInstanceId::new("pod-uid")
        }

        fn current_progress(&self) -> i64 {
            0
        }

        fn catch_up_capability(&self) -> i64 {
            0
        }

        fn control_address(&self) -> String {
            "http://primary".to_string()
        }

        fn replicator_address(&self) -> String {
            "http://primary-data".to_string()
        }

        async fn get_status(&self) -> kuberic_core::Result<ReplicaStatusInfo> {
            Ok(observed(
                kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION,
            ))
        }

        async fn execute_correlated_control_action(
            &self,
            request: CorrelatedControlActionRequest,
        ) -> kuberic_core::Result<CorrelatedControlActionAcknowledgement> {
            self.requests.lock().unwrap().push(request);
            if self.refresh_required {
                Err(KubericError::RemoteAgentPreconditionRejected(
                    "generation changed before dispatch".to_string(),
                ))
            } else {
                Err(KubericError::AgentBusy)
            }
        }
    }

    #[cfg(feature = "durable-switchover-pilot")]
    struct OnePreparedPilotActivity {
        input: crate::durable::pilot::DurableSwitchoverActivityInput,
    }

    #[cfg(feature = "durable-switchover-pilot")]
    #[async_trait::async_trait]
    impl kuberic_durable_execution::Workflow for OnePreparedPilotActivity {
        async fn run(
            &self,
            context: &mut kuberic_durable_execution::WorkflowContext<'_>,
            _: kuberic_durable_execution::ExactBytes,
        ) -> kuberic_durable_execution::TerminalOutcome {
            let _ = context
                .call::<crate::durable::pilot::DurableSwitchoverActivity>(self.input.clone())
                .await;
            kuberic_durable_execution::TerminalOutcome::failed(*b"unreachable")
        }
    }

    #[cfg(feature = "durable-switchover-pilot")]
    struct FixedPilotResolver {
        input: crate::durable::pilot::DurableSwitchoverActivityInput,
    }

    #[cfg(feature = "durable-switchover-pilot")]
    impl kuberic_durable_execution::PreparedActivityResolver for FixedPilotResolver {
        fn resolve(
            &self,
            logical: &kuberic_durable_execution::ActivitySpec,
            _: Option<&kuberic_durable_execution::ActivitySpec>,
        ) -> Result<
            kuberic_durable_execution::ActivitySpec,
            kuberic_durable_execution::PreparedActivityError,
        > {
            let input = kuberic_durable_execution::encode_activity_input::<
                crate::durable::pilot::DurableSwitchoverActivity,
            >(&self.input)
            .map_err(|_| kuberic_durable_execution::PreparedActivityError::Encoding)?;
            Ok(kuberic_durable_execution::ActivitySpec::new(
                logical.name().clone(),
                input,
                logical.max_result_bytes(),
            ))
        }
    }

    #[cfg(feature = "durable-switchover-pilot")]
    async fn expose_fixed_pilot_input(
        input: crate::durable::pilot::DurableSwitchoverActivityInput,
        seed: u8,
    ) -> kuberic_durable_execution::DispatchPermit {
        let execution = kuberic_durable_execution::ExecutionSpec::new(
            kuberic_durable_execution::ExecutionId::from_bytes([seed; 16]),
            kuberic_durable_execution::ExactBytes::new(b"prepared-test"),
            1024,
        );
        let mut host = kuberic_durable_execution::DurableHost::new(
            kuberic_durable_execution::InMemoryCheckpointStore::new(),
            kuberic_durable_execution::HostEpoch::from_bytes([seed.wrapping_add(1); 16]),
            kuberic_durable_execution::CheckpointLimits::new(4, 128 * 1024).unwrap(),
        );
        let workflow = OnePreparedPilotActivity {
            input: crate::durable::pilot::DurableSwitchoverActivityInput {
                kind: PilotActivityKind::PassiveObservation,
                ..input.clone()
            },
        };
        let resolver = FixedPilotResolver { input };
        let HostOutcome::DispatchPermitted { permit, .. } = host
            .turn_and_expose_with(&workflow, execution, &resolver)
            .await
        else {
            panic!("prepared test activity was not atomically exposed");
        };
        permit
    }

    #[cfg(feature = "durable-switchover-pilot")]
    struct BridgeTestApi {
        exact_uid_patches: AtomicUsize,
        exact_uid_commands: StdMutex<Vec<(String, String, String)>>,
    }

    #[cfg(feature = "durable-switchover-pilot")]
    #[async_trait::async_trait]
    impl ClusterApi for BridgeTestApi {
        async fn list_pods(&self, _: &str, _: &str) -> Result<Vec<Pod>, String> {
            Err("unused".to_string())
        }
        async fn create_pod(&self, _: &str, _: &Pod) -> Result<(), String> {
            Err("unused".to_string())
        }
        async fn delete_pod(&self, _: &str, _: &str, _: &str) -> Result<(), String> {
            Err("unused".to_string())
        }
        async fn patch_pod_labels(
            &self,
            _: &str,
            _: &str,
            _: BTreeMap<String, String>,
        ) -> Result<(), String> {
            panic!("pilot labels must use the UID-fenced API")
        }
        async fn patch_pod_labels_if_uid(
            &self,
            _: &str,
            pod_name: &str,
            expected_uid: &str,
            labels: BTreeMap<String, String>,
        ) -> Result<(), String> {
            self.exact_uid_patches.fetch_add(1, Ordering::SeqCst);
            self.exact_uid_commands.lock().unwrap().push((
                pod_name.to_string(),
                expected_uid.to_string(),
                labels.get("kuberic.io/role").cloned().unwrap_or_default(),
            ));
            Ok(())
        }
        async fn patch_set_status(
            &self,
            _: &str,
            _: &str,
            _: &KubericSetStatus,
            _: Option<&str>,
        ) -> Result<(), String> {
            Err("unused".to_string())
        }
        async fn create_replica_handle(
            &self,
            _: ReplicaId,
            _: &Pod,
            _: &KubericSetSpec,
        ) -> Result<Box<dyn ReplicaHandle>, String> {
            Err("unused".to_string())
        }
        async fn get_pvc(&self, _: &str, _: &str) -> Result<PersistentVolumeClaim, String> {
            Err("unused".to_string())
        }
        async fn create_pvc(&self, _: &str, _: &PersistentVolumeClaim) -> Result<(), String> {
            Err("unused".to_string())
        }
        async fn list_pvcs(&self, _: &str, _: &str) -> Result<Vec<PersistentVolumeClaim>, String> {
            Err("unused".to_string())
        }
        async fn delete_pvc(&self, _: &str, _: &str) -> Result<(), String> {
            Err("unused".to_string())
        }
        async fn get_service(&self, _: &str, _: &str) -> Result<Service, String> {
            Err("unused".to_string())
        }
        async fn create_service(&self, _: &str, _: &Service) -> Result<(), String> {
            Err("unused".to_string())
        }
        async fn delete_service(&self, _: &str, _: &str) -> Result<(), String> {
            Err("unused".to_string())
        }
    }

    fn configuration_action(progress: i64) -> DurableReplicaAction {
        DurableReplicaAction::UpdateCurrentConfiguration {
            current: kuberic_core::types::ReplicaSetConfig {
                members: vec![kuberic_core::types::ReplicaInfo {
                    id: 2,
                    instance_id: ReplicaInstanceId::new("secondary"),
                    role: kuberic_core::types::Role::ActiveSecondary,
                    status: kuberic_core::types::ReplicaStatus::Up,
                    replicator_address: "http://secondary".to_string(),
                    current_progress: progress,
                    catch_up_capability: progress,
                    must_catch_up: true,
                }],
                write_quorum: 2,
            },
        }
    }

    fn persisted(plan: DispatchEvidencePlan) -> PendingActionStatus {
        match plan {
            DispatchEvidencePlan::Persist(pending) => *pending,
            DispatchEvidencePlan::Ready
            | DispatchEvidencePlan::WaitForExactIncarnation
            | DispatchEvidencePlan::WaitForSupportedProtocol => {
                panic!("expected dispatch evidence persistence")
            }
        }
    }

    #[cfg(feature = "durable-switchover-pilot")]
    #[test]
    fn quarantine_only_proves_no_admission_after_agent_generation_changes() {
        let (operation, old_observed) = pilot_pending_with_fence();

        let same_generation = pilot_observations(old_observed.clone());
        let decision =
            crate::durable::pilot::evaluate_adapter_step(&operation, &same_generation, 100)
                .unwrap();
        assert!(matches!(
            resolve_pilot_quarantine(
                &operation,
                &PilotActivityKind::PreparedReplica {
                    command: ReplicaEffectCommand::from_pending(
                        operation.pending_action.as_ref().unwrap(),
                    )
                    .unwrap(),
                },
                decision,
                &same_generation,
            )
            .unwrap(),
            PilotEffectBridgeOutcome::AwaitEvidence
        ));

        let mut restarted = old_observed;
        restarted.agent.generation =
            AgentGeneration::parse("fedcba9876543210fedcba9876543210").unwrap();
        let restarted = pilot_observations(restarted);
        let decision =
            crate::durable::pilot::evaluate_adapter_step(&operation, &restarted, 100).unwrap();
        let prepared = PilotActivityKind::PreparedReplica {
            command: ReplicaEffectCommand::from_pending(operation.pending_action.as_ref().unwrap())
                .unwrap(),
        };
        match resolve_pilot_quarantine(&operation, &prepared, decision, &restarted).unwrap() {
            PilotEffectBridgeOutcome::Observe(result) => match *result {
                DurableSwitchoverStepResult::ProvenNoAdmission {
                    operation: next,
                    action_id,
                    redelivery,
                } => {
                    assert_eq!(
                        action_id,
                        operation.pending_action.as_ref().unwrap().action_id
                    );
                    assert_eq!(redelivery, 1);
                    assert!(
                        next.pending_action
                            .as_ref()
                            .unwrap()
                            .dispatch_agent_generation
                            .is_none()
                    );
                }
                _ => panic!("expected proven-no-admission result"),
            },
            _ => panic!("new agent generation and exact precondition must prove no admission"),
        }
    }

    #[cfg(feature = "durable-switchover-pilot")]
    #[test]
    fn expired_exposed_label_is_not_mistaken_for_effect_free() {
        let mut operation = pilot_operation();
        operation.phase = DurableOperationPhase::LabelTargetPrimary;
        let Decision::Persist(mut operation) =
            decide(&operation, &OperationObservations::new(), 100).unwrap()
        else {
            panic!("expected label intent");
        };
        operation
            .pending_action
            .as_mut()
            .unwrap()
            .deadline_unix_seconds = 0;
        let decision = crate::durable::pilot::evaluate_adapter_step(
            &operation,
            &OperationObservations::new(),
            1,
        )
        .unwrap();
        assert!(matches!(
            resolve_pilot_quarantine(
                &operation,
                &PilotActivityKind::PreparedLabel {
                    command: LabelEffectCommand::new(
                        operation.pending_action.as_ref().unwrap().target_id,
                        "target".to_string(),
                        operation
                            .pending_action
                            .as_ref()
                            .unwrap()
                            .target_instance_id
                            .clone(),
                        "primary".to_string(),
                    ),
                },
                decision,
                &OperationObservations::new(),
            )
            .unwrap(),
            PilotEffectBridgeOutcome::AwaitEvidence
        ));
        let unknown_patch =
            PilotAdapterDecision::External(Box::new(Decision::PatchPodRoleExactUid {
                target_id: operation.pending_action.as_ref().unwrap().target_id,
                expected_uid: "target-uid".to_string(),
                role: "primary".to_string(),
            }));
        assert!(matches!(
            resolve_pilot_quarantine(
                &operation,
                &PilotActivityKind::PreparedLabel {
                    command: LabelEffectCommand::new(
                        operation.pending_action.as_ref().unwrap().target_id,
                        "target".to_string(),
                        operation
                            .pending_action
                            .as_ref()
                            .unwrap()
                            .target_instance_id
                            .clone(),
                        "primary".to_string(),
                    ),
                },
                unknown_patch,
                &OperationObservations::new(),
            )
            .unwrap(),
            PilotEffectBridgeOutcome::AwaitEvidence
        ));
    }

    #[cfg(feature = "durable-switchover-pilot")]
    #[test]
    fn quarantine_observes_postcondition_but_not_in_progress_timeout_or_mixed_state() {
        let (operation, base_observed) = pilot_pending_with_fence();

        let mut postcondition = base_observed.clone();
        postcondition.write_status = AccessStatus::ReconfigurationPending;
        let postcondition = pilot_observations(postcondition);
        let decision =
            crate::durable::pilot::evaluate_adapter_step(&operation, &postcondition, 100).unwrap();
        assert!(matches!(
            resolve_pilot_quarantine(
                &operation,
                &PilotActivityKind::PreparedReplica {
                    command: ReplicaEffectCommand::from_pending(
                        operation.pending_action.as_ref().unwrap(),
                    )
                    .unwrap(),
                },
                decision,
                &postcondition,
            )
            .unwrap(),
            PilotEffectBridgeOutcome::Observe(_)
        ));

        let mut in_progress = base_observed.clone();
        let pending = operation.pending_action.as_ref().unwrap();
        in_progress.agent.current_action = Some(CorrelatedActionObservation {
            generation: in_progress.agent.generation.clone(),
            control_version: in_progress.agent.control_version,
            action: DurableActionObservation {
                action_id: pending.action_id.clone(),
                signature: action().signature(),
                state: DurableActionState::InProgress,
                error_class: None,
                error: None,
                result: None,
                add_replica_progress: None,
                remove_replica_progress: None,
            },
        });
        let in_progress = pilot_observations(in_progress);
        let decision =
            crate::durable::pilot::evaluate_adapter_step(&operation, &in_progress, 111).unwrap();
        assert!(matches!(
            resolve_pilot_quarantine(
                &operation,
                &PilotActivityKind::PreparedReplica {
                    command: ReplicaEffectCommand::from_pending(
                        operation.pending_action.as_ref().unwrap(),
                    )
                    .unwrap(),
                },
                decision,
                &in_progress,
            )
            .unwrap(),
            PilotEffectBridgeOutcome::AwaitEvidence
        ));

        let mut mixed = base_observed;
        mixed.role = kuberic_core::types::Role::ActiveSecondary;
        let mixed = pilot_observations(mixed);
        let decision =
            crate::durable::pilot::evaluate_adapter_step(&operation, &mixed, 100).unwrap();
        assert!(matches!(
            resolve_pilot_quarantine(
                &operation,
                &PilotActivityKind::PreparedReplica {
                    command: ReplicaEffectCommand::from_pending(
                        operation.pending_action.as_ref().unwrap(),
                    )
                    .unwrap(),
                },
                decision,
                &mixed,
            )
            .unwrap(),
            PilotEffectBridgeOutcome::AwaitEvidence
        ));
    }

    #[cfg(feature = "durable-switchover-pilot")]
    #[tokio::test]
    async fn pilot_label_commands_persist_and_dispatch_both_exact_snapshot_incarnations() {
        let operation = pilot_operation();
        let mut observations = pilot_observations(observed(
            kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION,
        ));
        let mut target = observed(kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION);
        target.instance_id = ReplicaInstanceId::new("target-uid");
        observations.insert(
            2,
            ReplicaObservation {
                status: target,
                control_address: "http://target".to_string(),
                replicator_address: "http://target-data".to_string(),
                pod_name: "database-1".to_string(),
                pod_role_label: Some("active-secondary".to_string()),
            },
        );
        assert_eq!(
            exact_pilot_label_target(&operation, 1, &observations).unwrap(),
            ("database-0".to_string(), "pod-uid".to_string())
        );
        assert_eq!(
            exact_pilot_label_target(&operation, 2, &observations).unwrap(),
            ("database-1".to_string(), "target-uid".to_string())
        );
        let old_command = crate::durable::effects::exact_label_command(
            &operation,
            1,
            "active-secondary",
            &observations,
        )
        .unwrap();
        let target_command =
            crate::durable::effects::exact_label_command(&operation, 2, "primary", &observations)
                .unwrap();
        assert_eq!(old_command.role, "active-secondary");
        assert_eq!(target_command.role, "primary");
        assert_eq!(old_command.expected_uid, "pod-uid");
        assert_eq!(target_command.expected_uid, "target-uid");
        for changed in [
            {
                let mut changed = old_command.clone();
                changed.target_id = 2;
                changed
            },
            {
                let mut changed = old_command.clone();
                changed.pod_name = "replacement-pod".to_string();
                changed
            },
            {
                let mut changed = old_command.clone();
                changed.expected_uid = "replacement-uid".to_string();
                changed
            },
            {
                let mut changed = old_command.clone();
                changed.role = "primary".to_string();
                changed
            },
            {
                let mut changed = old_command.clone();
                changed.identity_signature.push_str("-other");
                changed
            },
        ] {
            assert!(!changed.has_valid_identity_signature());
        }
        let api = BridgeTestApi {
            exact_uid_patches: AtomicUsize::new(0),
            exact_uid_commands: StdMutex::new(Vec::new()),
        };
        let handles = BTreeMap::new();
        for (index, command) in [old_command, target_command].into_iter().enumerate() {
            let prepared = PilotActivityKind::PreparedLabel { command };
            let permit = expose_fixed_pilot_input(
                crate::durable::pilot::DurableSwitchoverActivityInput {
                    version: crate::durable::pilot::PILOT_VERSION,
                    state: crate::durable::pilot::DurableSwitchoverState::from_operation(
                        &operation,
                    ),
                    kind: prepared.clone(),
                },
                u8::try_from(index + 70).unwrap(),
            )
            .await;
            let accepted_activity = permit.activity().clone();
            let accepted_attempt = permit.attempt_id();
            let mut guard = PilotPermitGuard::new(permit);
            assert!(matches!(
                bridge_pilot_permitted_step(
                    &mut guard,
                    &operation,
                    &prepared,
                    &accepted_activity,
                    accepted_attempt,
                    &observations,
                    &handles,
                    &api,
                    "default",
                )
                .await
                .unwrap(),
                PilotEffectBridgeOutcome::Exposed
            ));
        }
        assert_eq!(
            *api.exact_uid_commands.lock().unwrap(),
            vec![
                (
                    "database-0".to_string(),
                    "pod-uid".to_string(),
                    "active-secondary".to_string()
                ),
                (
                    "database-1".to_string(),
                    "target-uid".to_string(),
                    "primary".to_string()
                )
            ]
        );

        let mut drifted = observations;
        drifted.get_mut(&1).unwrap().status.instance_id = ReplicaInstanceId::new("replacement-uid");
        assert!(exact_pilot_label_target(&operation, 1, &drifted).is_err());
        drifted.get_mut(&2).unwrap().status.instance_id =
            ReplicaInstanceId::new("replacement-target-uid");
        assert!(exact_pilot_label_target(&operation, 2, &drifted).is_err());
    }

    #[cfg(feature = "durable-switchover-pilot")]
    #[test]
    fn pilot_dispatch_errors_only_observe_definitive_zero_effect_outcomes() {
        let operation = pilot_operation();
        let Decision::Persist(operation) =
            decide(&operation, &OperationObservations::new(), 100).unwrap()
        else {
            panic!("expected revoke intent");
        };
        let action_id = operation.pending_action.as_ref().unwrap().action_id.clone();

        assert!(matches!(
            pilot_result_after_dispatch_error(
                &operation,
                action_id.clone(),
                &KubericError::RemoteAgentPreconditionRejected("stale fence".to_string()),
            ),
            Some(DurableSwitchoverStepResult::ProvenNoAdmission { .. })
        ));
        assert!(matches!(
            pilot_result_after_dispatch_error(
                &operation,
                action_id.clone(),
                &KubericError::AgentBusy,
            ),
            Some(DurableSwitchoverStepResult::ProvenNoAdmission { .. })
        ));
        assert!(matches!(
            pilot_result_after_dispatch_error(
                &operation,
                action_id.clone(),
                &KubericError::RemoteAgentConflict("signature conflict".to_string()),
            ),
            Some(DurableSwitchoverStepResult::Stopped { .. })
        ));
        assert!(
            pilot_result_after_dispatch_error(
                &operation,
                action_id,
                &KubericError::Internal(Box::new(std::io::Error::other("ambiguous transport"))),
            )
            .is_none()
        );
    }

    #[cfg(feature = "durable-switchover-pilot")]
    #[tokio::test]
    async fn bridge_dispatches_one_persisted_command_from_the_fused_permit() {
        use crate::durable::pilot::{
            DurableSwitchoverWorkflow, PilotCheckpointStore, PilotPreparedActivityResolver,
            decode_pilot_activity_input, execution_spec, initial_operation, new_pilot_reference,
        };
        use crate::durable::pilot_store::MeasuredPilotCheckpointStore;
        use kuberic_durable_execution::{
            DurableHost, HostEpoch, HostOutcome, InMemoryCheckpointStore,
        };

        let mut reference = new_pilot_reference("set-uid", pilot_snapshot(), 2, 100).unwrap();
        let initial = initial_operation(&reference).unwrap();
        let PilotAdapterDecision::Observe(result) = crate::durable::pilot::evaluate_adapter_step(
            &initial,
            &OperationObservations::new(),
            100,
        )
        .unwrap() else {
            panic!("expected persisted revoke intent");
        };
        let DurableSwitchoverStepResult::Advance { operation: pending } = *result else {
            panic!("expected compact revoke intent");
        };
        let pending = pending.apply_to(&initial).unwrap();
        reference.initial_operation_json = serde_json::to_string(&pending).unwrap();
        let execution = execution_spec(&reference).unwrap();
        let mut host = DurableHost::new(
            MeasuredPilotCheckpointStore::new(
                execution.execution_id(),
                PilotCheckpointStore::InMemory(InMemoryCheckpointStore::new()),
            ),
            HostEpoch::from_bytes([21; 16]),
            crate::durable::pilot::checkpoint_limits(),
        );
        let workflow = DurableSwitchoverWorkflow;
        let requests = StdArc::new(StdMutex::new(Vec::new()));
        let handles: BTreeMap<ReplicaId, Box<dyn ReplicaHandle>> = BTreeMap::from([(
            1,
            Box::new(BusyPilotHandle {
                requests: requests.clone(),
                refresh_required: false,
            }) as Box<dyn ReplicaHandle>,
        )]);
        let observations = pilot_observations(observed(
            kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION,
        ));
        let api = BridgeTestApi {
            exact_uid_patches: AtomicUsize::new(0),
            exact_uid_commands: StdMutex::new(Vec::new()),
        };
        let addressed_instances = handles
            .iter()
            .map(|(replica_id, handle)| (*replica_id, handle.instance_id()))
            .collect();
        let resolver =
            PilotPreparedActivityResolver::new(&pending, &observations, &addressed_instances, 100);
        let HostOutcome::DispatchPermitted { permit, .. } = host
            .turn_and_expose_with(&workflow, execution, &resolver)
            .await
        else {
            panic!("expected fused prepared dispatch permit");
        };
        let prepared = decode_pilot_activity_input(permit.activity().input()).unwrap();
        let accepted_activity = permit.activity().clone();
        let accepted_attempt = permit.attempt_id();
        let prepared_operation = prepared.state.apply_to(&pending).unwrap();
        let PilotActivityKind::PreparedReplica { command } = &prepared.kind else {
            panic!("expected exact replica command in permit");
        };
        assert_eq!(
            command.action_id,
            pending.pending_action.as_ref().unwrap().action_id
        );
        assert_eq!(command.target_instance_id, "pod-uid");
        assert!(!command.action_payload.is_empty());
        let mut guard = PilotPermitGuard::new(permit);
        let PilotEffectBridgeOutcome::Observe(result) = bridge_pilot_permitted_step(
            &mut guard,
            &prepared_operation,
            &prepared.kind,
            &accepted_activity,
            accepted_attempt,
            &observations,
            &handles,
            &api,
            "default",
        )
        .await
        .unwrap() else {
            panic!("busy dispatch must return proven-no-admission evidence");
        };
        assert!(matches!(
            *result,
            DurableSwitchoverStepResult::ProvenNoAdmission { .. }
        ));
        {
            let recorded = requests.lock().unwrap();
            assert_eq!(recorded.len(), 1);
            let request = &recorded[0];
            let pending_action = prepared_operation.pending_action.as_ref().unwrap();
            assert_eq!(request.action_id, pending_action.action_id);
            assert_eq!(request.target_instance_id.as_str(), "pod-uid");
            assert_eq!(
                request.expected_agent_generation.to_string(),
                pending_action.dispatch_agent_generation.as_deref().unwrap()
            );
            assert_eq!(
                request.expected_control_version.value(),
                pending_action.dispatch_agent_control_version.unwrap()
            );
        }

        assert!(
            bridge_pilot_permitted_step(
                &mut guard,
                &prepared_operation,
                &prepared.kind,
                &accepted_activity,
                accepted_attempt,
                &observations,
                &handles,
                &api,
                "default",
            )
            .await
            .is_err()
        );
        assert_eq!(requests.lock().unwrap().len(), 1);
        assert_eq!(api.exact_uid_patches.load(Ordering::SeqCst), 0);
    }

    #[cfg(feature = "durable-switchover-pilot")]
    #[tokio::test]
    async fn missing_external_observation_rejects_preparation_before_exposure_then_dispatches_exactly()
     {
        use crate::durable::pilot::{
            DurableSwitchoverWorkflow, PilotCheckpointStore, PilotPreparedActivityResolver,
            decode_pilot_activity_input, execution_spec, initial_operation, new_pilot_reference,
        };
        use crate::durable::pilot_store::MeasuredPilotCheckpointStore;
        use kuberic_durable_execution::{
            CheckpointError, DurableHost, HostEpoch, HostOutcome, InMemoryCheckpointStore,
            PreparedActivityError,
        };

        let mut reference = new_pilot_reference("set-uid", pilot_snapshot(), 2, 100).unwrap();
        let initial = initial_operation(&reference).unwrap();
        let PilotAdapterDecision::Observe(result) = crate::durable::pilot::evaluate_adapter_step(
            &initial,
            &OperationObservations::new(),
            100,
        )
        .unwrap() else {
            panic!("expected persisted revoke intent");
        };
        let DurableSwitchoverStepResult::Advance { operation: pending } = *result else {
            panic!("expected compact revoke intent");
        };
        let pending = pending.apply_to(&initial).unwrap();
        assert!(pending.pending_action.is_some());
        assert!(matches!(
            crate::durable::pilot::evaluate_adapter_step(
                &pending,
                &OperationObservations::new(),
                100,
            )
            .unwrap(),
            PilotAdapterDecision::AwaitEvidence
        ));
        assert!(
            resolve_pilot_quarantine(
                &pending,
                &PilotActivityKind::PassiveObservation,
                PilotAdapterDecision::AwaitEvidence,
                &OperationObservations::new(),
            )
            .is_err(),
            "quarantine must reject a pending external effect classified as passive"
        );
        reference.initial_operation_json = serde_json::to_string(&pending).unwrap();
        let execution = execution_spec(&reference).unwrap();
        let backend = InMemoryCheckpointStore::new();
        let measured = MeasuredPilotCheckpointStore::new(
            execution.execution_id(),
            PilotCheckpointStore::InMemory(backend.clone()),
        );
        let mut host = DurableHost::new(
            measured,
            HostEpoch::from_bytes([21; 16]),
            crate::durable::pilot::checkpoint_limits(),
        );
        let no_observations = OperationObservations::new();
        let no_handles = BTreeMap::new();
        let unavailable_resolver =
            PilotPreparedActivityResolver::new(&pending, &no_observations, &no_handles, 100);

        assert_eq!(
            host.turn_and_expose_with(
                &DurableSwitchoverWorkflow,
                execution.clone(),
                &unavailable_resolver,
            )
            .await,
            HostOutcome::CheckpointRejected(CheckpointError::PreparedActivityRejected(
                PreparedActivityError::Derivation,
            ))
        );
        assert!(
            backend
                .load(execution.execution_id())
                .await
                .unwrap()
                .is_none(),
            "preparation rejection must leave the authoritative predecessor unchanged"
        );
        assert_eq!(host.store().measurements().write_attempts, 0);
        assert_eq!(host.store().measurements().accepted_writes, 0);

        let requests = StdArc::new(StdMutex::new(Vec::new()));
        let handles: BTreeMap<ReplicaId, Box<dyn ReplicaHandle>> = BTreeMap::from([(
            1,
            Box::new(BusyPilotHandle {
                requests: requests.clone(),
                refresh_required: false,
            }) as Box<dyn ReplicaHandle>,
        )]);
        let observations = pilot_observations(observed(
            kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION,
        ));
        let addressed_instances = handles
            .iter()
            .map(|(replica_id, handle)| (*replica_id, handle.instance_id()))
            .collect();
        let ready_resolver =
            PilotPreparedActivityResolver::new(&pending, &observations, &addressed_instances, 101);
        let HostOutcome::DispatchPermitted { permit, .. } = host
            .turn_and_expose_with(&DurableSwitchoverWorkflow, execution, &ready_resolver)
            .await
        else {
            panic!("fresh observation must expose the exact prepared effect");
        };
        let activity = permit.activity().clone();
        let attempt = permit.attempt_id();
        let prepared = decode_pilot_activity_input(activity.input()).unwrap();
        let PilotActivityKind::PreparedReplica { command } = &prepared.kind else {
            panic!("pending external action was not prepared as a replica effect");
        };
        assert_eq!(
            command.action_id,
            pending.pending_action.as_ref().unwrap().action_id
        );
        let prepared_operation = prepared.state.apply_to(&pending).unwrap();
        let api = BridgeTestApi {
            exact_uid_patches: AtomicUsize::new(0),
            exact_uid_commands: StdMutex::new(Vec::new()),
        };
        let mut guard = PilotPermitGuard::new(permit);
        assert!(matches!(
            bridge_pilot_permitted_step(
                &mut guard,
                &prepared_operation,
                &prepared.kind,
                &activity,
                attempt,
                &observations,
                &handles,
                &api,
                "default",
            )
            .await
            .unwrap(),
            PilotEffectBridgeOutcome::Observe(_)
        ));
        let dispatched = requests.lock().unwrap();
        assert_eq!(dispatched.len(), 1);
        assert_eq!(dispatched[0].action_id, command.action_id);
        assert_eq!(dispatched[0].action.signature(), command.action_signature);
        assert_eq!(api.exact_uid_patches.load(Ordering::SeqCst), 0);
    }

    #[cfg(feature = "durable-switchover-pilot")]
    #[tokio::test]
    async fn precondition_rejection_waits_for_fresh_observations_before_redelivery() {
        use crate::durable::pilot::{
            DurableSwitchoverWorkflow, PilotCheckpointStore, PilotPreparedActivityResolver,
            decode_pilot_activity_input, execution_spec, initial_operation, new_pilot_reference,
        };
        use crate::durable::pilot_store::MeasuredPilotCheckpointStore;
        use kuberic_durable_execution::{
            ActivityState, DurableHost, HostEpoch, HostOutcome, InMemoryCheckpointStore,
        };

        let mut reference = new_pilot_reference("set-uid", pilot_snapshot(), 2, 100).unwrap();
        let initial = initial_operation(&reference).unwrap();
        let PilotAdapterDecision::Observe(result) = crate::durable::pilot::evaluate_adapter_step(
            &initial,
            &OperationObservations::new(),
            100,
        )
        .unwrap() else {
            panic!("expected persisted revoke intent");
        };
        let DurableSwitchoverStepResult::Advance { operation: pending } = *result else {
            panic!("expected compact revoke intent");
        };
        let pending = pending.apply_to(&initial).unwrap();
        reference.initial_operation_json = serde_json::to_string(&pending).unwrap();
        let execution = execution_spec(&reference).unwrap();
        let backend = InMemoryCheckpointStore::new();
        let mut host = DurableHost::new(
            MeasuredPilotCheckpointStore::new(
                execution.execution_id(),
                PilotCheckpointStore::InMemory(backend),
            ),
            HostEpoch::from_bytes([22; 16]),
            crate::durable::pilot::checkpoint_limits(),
        );
        let workflow = DurableSwitchoverWorkflow;
        let requests = StdArc::new(StdMutex::new(Vec::new()));
        let handles: BTreeMap<ReplicaId, Box<dyn ReplicaHandle>> = BTreeMap::from([(
            1,
            Box::new(BusyPilotHandle {
                requests: requests.clone(),
                refresh_required: true,
            }) as Box<dyn ReplicaHandle>,
        )]);
        let generation_one = pilot_observations(observed(
            kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION,
        ));
        let addressed_instances = handles
            .iter()
            .map(|(replica_id, handle)| (*replica_id, handle.instance_id()))
            .collect();
        let resolver = PilotPreparedActivityResolver::new(
            &pending,
            &generation_one,
            &addressed_instances,
            100,
        );
        let HostOutcome::DispatchPermitted { permit, .. } = host
            .turn_and_expose_with(&workflow, execution.clone(), &resolver)
            .await
        else {
            panic!("expected generation-one dispatch permit");
        };
        let prepared = decode_pilot_activity_input(permit.activity().input()).unwrap();
        let prepared_operation = prepared.state.apply_to(&pending).unwrap();
        let activity = permit.activity().clone();
        let attempt = permit.attempt_id();
        let mut guard = PilotPermitGuard::new(permit);
        let PilotEffectBridgeOutcome::ObserveAfterFenceRefresh(result) =
            bridge_pilot_permitted_step(
                &mut guard,
                &prepared_operation,
                &prepared.kind,
                &activity,
                attempt,
                &generation_one,
                &handles,
                &BridgeTestApi {
                    exact_uid_patches: AtomicUsize::new(0),
                    exact_uid_commands: StdMutex::new(Vec::new()),
                },
                "default",
            )
            .await
            .unwrap()
        else {
            panic!("precondition rejection must require a fresh observation cycle");
        };
        let DurableSwitchoverStepResult::ProvenNoAdmission {
            operation,
            action_id,
            redelivery,
        } = &*result
        else {
            panic!("precondition rejection must persist proven non-admission");
        };
        let retry_operation = operation.apply_to(&pending).unwrap();
        let retry_pending = retry_operation.pending_action.as_ref().unwrap();
        assert_eq!(retry_pending.action_id, *action_id);
        assert_eq!(*redelivery, 1);
        assert!(retry_pending.dispatch_agent_generation.is_none());
        assert!(retry_pending.dispatch_agent_control_version.is_none());
        assert!(retry_pending.dispatch_observed_runtime_epoch.is_none());

        assert!(matches!(
            host.observe(
                &execution,
                ActivityObservation::new(activity, encode_step_result(&result).unwrap()),
            )
            .await,
            HostOutcome::ObservationAccepted { .. }
        ));
        let stored = host
            .store()
            .load(execution.execution_id())
            .await
            .unwrap()
            .unwrap();
        let payload = stored
            .checkpoint()
            .decode_and_validate(&execution, crate::durable::pilot::checkpoint_limits())
            .unwrap();
        let activities = payload.active_activities().unwrap();
        assert_eq!(activities.len(), 1);
        assert!(matches!(
            activities[0].state(),
            ActivityState::Completed { .. }
        ));
        assert_eq!(
            requests.lock().unwrap().len(),
            1,
            "the stale reconcile must not dispatch the retry"
        );

        let mut generation_two_status =
            observed(kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION);
        generation_two_status.agent.generation =
            AgentGeneration::parse("fedcba9876543210fedcba9876543210").unwrap();
        generation_two_status.agent.control_version = AgentControlVersion::new(8);
        let generation_two = pilot_observations(generation_two_status);
        let refreshed_resolver = PilotPreparedActivityResolver::new(
            &pending,
            &generation_two,
            &addressed_instances,
            101,
        );
        let HostOutcome::DispatchPermitted { permit, .. } = host
            .turn_and_expose_with(&workflow, execution, &refreshed_resolver)
            .await
        else {
            panic!("the next reconcile may expose the one bounded redelivery");
        };
        let retry = decode_pilot_activity_input(permit.activity().input()).unwrap();
        let PilotActivityKind::PreparedReplica { command } = retry.kind else {
            panic!("redelivery must retain an exact prepared replica command");
        };
        assert_eq!(command.action_id, *action_id);
        assert_eq!(
            command.expected_agent_generation,
            "fedcba9876543210fedcba9876543210"
        );
        assert_eq!(command.expected_control_version, 8);
        assert_eq!(requests.lock().unwrap().len(), 1);
    }

    #[cfg(feature = "durable-switchover-pilot")]
    #[tokio::test]
    async fn all_seven_replica_commands_cross_fused_exposure_and_permitted_bridge() {
        use kuberic_core::types::{ReplicaSetConfig, ReplicaSetQuorumMode, Role};

        let config = || ReplicaSetConfig {
            members: Vec::new(),
            write_quorum: 1,
        };
        let actions = [
            (
                crate::crd::DurableActionKind::RevokeWrite,
                DurableReplicaAction::RevokeWriteStatus,
            ),
            (
                crate::crd::DurableActionKind::DemoteOldPrimary,
                DurableReplicaAction::ChangeRole {
                    epoch: Epoch::new(1, 4),
                    role: Role::ActiveSecondary,
                },
            ),
            (
                crate::crd::DurableActionKind::PromoteTarget,
                DurableReplicaAction::ChangeRole {
                    epoch: Epoch::new(1, 4),
                    role: Role::Primary,
                },
            ),
            (
                crate::crd::DurableActionKind::UpdateSecondaryEpoch,
                DurableReplicaAction::UpdateEpoch {
                    epoch: Epoch::new(1, 4),
                },
            ),
            (
                crate::crd::DurableActionKind::UpdateCatchUpConfiguration,
                DurableReplicaAction::UpdateCatchUpConfiguration {
                    current: config(),
                    previous: config(),
                },
            ),
            (
                crate::crd::DurableActionKind::WaitForCatchUpQuorum,
                DurableReplicaAction::WaitForCatchUpQuorum {
                    mode: ReplicaSetQuorumMode::Write,
                },
            ),
            (
                crate::crd::DurableActionKind::UpdateCurrentConfiguration,
                DurableReplicaAction::UpdateCurrentConfiguration { current: config() },
            ),
        ];
        for (index, (kind, action)) in actions.into_iter().enumerate() {
            let mut pending = pending();
            pending.kind = kind;
            pending.expected_epoch = crate::crd::EpochStatus {
                data_loss_number: 1,
                configuration_number: 4,
            };
            pending.desired_postcondition.role = match kind {
                crate::crd::DurableActionKind::DemoteOldPrimary => {
                    Some(StableReplicaRoleStatus::ActiveSecondary)
                }
                crate::crd::DurableActionKind::PromoteTarget => {
                    Some(StableReplicaRoleStatus::Primary)
                }
                _ => None,
            };
            let status = observed(kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION);
            let (planned, command) = crate::durable::effects::prepare_replica_effect_command(
                &pending,
                &status,
                &ReplicaInstanceId::new("pod-uid"),
                &action,
            )
            .unwrap();
            let mut operation = pilot_operation();
            operation.pending_action = Some(planned);
            let prepared = PilotActivityKind::PreparedReplica {
                command: command.clone(),
            };
            let permit = expose_fixed_pilot_input(
                crate::durable::pilot::DurableSwitchoverActivityInput {
                    version: crate::durable::pilot::PILOT_VERSION,
                    state: crate::durable::pilot::DurableSwitchoverState::from_operation(
                        &operation,
                    ),
                    kind: prepared.clone(),
                },
                u8::try_from(index + 40).unwrap(),
            )
            .await;
            let accepted_activity = permit.activity().clone();
            let accepted_attempt = permit.attempt_id();
            let requests = StdArc::new(StdMutex::new(Vec::new()));
            let handles: BTreeMap<ReplicaId, Box<dyn ReplicaHandle>> = BTreeMap::from([(
                1,
                Box::new(BusyPilotHandle {
                    requests: requests.clone(),
                    refresh_required: false,
                }) as Box<dyn ReplicaHandle>,
            )]);
            let observations = pilot_observations(status);
            let api = BridgeTestApi {
                exact_uid_patches: AtomicUsize::new(0),
                exact_uid_commands: StdMutex::new(Vec::new()),
            };
            let mut guard = PilotPermitGuard::new(permit);
            assert!(matches!(
                bridge_pilot_permitted_step(
                    &mut guard,
                    &operation,
                    &prepared,
                    &accepted_activity,
                    accepted_attempt,
                    &observations,
                    &handles,
                    &api,
                    "default",
                )
                .await
                .unwrap(),
                PilotEffectBridgeOutcome::Observe(_)
            ));
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].action.signature(), action.signature());
            assert_eq!(requests[0].action_id, command.action_id);
        }
    }

    #[test]
    fn versioned_dispatch_evidence_is_persisted_without_changing_attempt_budget() {
        let original = pending();
        let addressed = ReplicaInstanceId::new("pod-uid");
        let planned = persisted(plan_dispatch_evidence(
            &original,
            &observed(kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION),
            &addressed,
            &action(),
            true,
        ));
        assert_eq!(
            planned.dispatch_agent_generation.as_deref(),
            Some("0123456789abcdef0123456789abcdef")
        );
        assert_eq!(planned.dispatch_agent_control_version, Some(7));
        assert_eq!(
            planned.dispatch_observed_runtime_epoch,
            Some(crate::crd::EpochStatus {
                data_loss_number: 1,
                configuration_number: 4,
            })
        );
        assert_eq!(planned.attempts, original.attempts);
        assert_eq!(
            planned.deadline_unix_seconds,
            original.deadline_unix_seconds
        );
        assert!(matches!(
            plan_dispatch_evidence(
                &planned,
                &observed(kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION),
                &addressed,
                &action(),
                true,
            ),
            DispatchEvidencePlan::Ready
        ));
    }

    #[test]
    fn dispatch_evidence_freezes_exact_action_payload_across_reobservation() {
        let addressed = ReplicaInstanceId::new("pod-uid");
        let first_action = configuration_action(10);
        let planned = persisted(plan_dispatch_evidence(
            &pending(),
            &observed(kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION),
            &addressed,
            &first_action,
            true,
        ));
        let changed_action = configuration_action(11);

        assert!(matches!(
            plan_dispatch_evidence(
                &planned,
                &observed(kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION),
                &addressed,
                &changed_action,
                true,
            ),
            DispatchEvidencePlan::Ready
        ));
        let frozen = kuberic_core::grpc::convert::decode_direct_correlated_action_payload(
            &planned.dispatch_action_payload,
        )
        .unwrap();
        assert_eq!(frozen.signature(), first_action.signature());
        assert_ne!(frozen.signature(), changed_action.signature());

        let mut replay_observation =
            observed(kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION);
        replay_observation.agent.control_version = AgentControlVersion::new(8);
        replay_observation.agent.current_action = Some(CorrelatedActionObservation {
            generation: replay_observation.agent.generation.clone(),
            control_version: AgentControlVersion::new(8),
            action: DurableActionObservation {
                action_id: planned.action_id.clone(),
                signature: first_action.signature(),
                state: DurableActionState::InProgress,
                error_class: None,
                error: None,
                result: None,
                add_replica_progress: None,
                remove_replica_progress: None,
            },
        });
        let replay_planned = persisted(plan_dispatch_evidence(
            &planned,
            &replay_observation,
            &addressed,
            &changed_action,
            true,
        ));
        assert_eq!(
            replay_planned.dispatch_action_payload,
            planned.dispatch_action_payload
        );
    }

    #[test]
    fn terminal_acknowledgement_preserves_error_classification() {
        let generation = AgentGeneration::parse("0123456789abcdef0123456789abcdef").unwrap();
        let error = correlated_acknowledgement_result(
            CorrelatedControlActionAcknowledgement {
                observation: CorrelatedActionObservation {
                    generation: generation.clone(),
                    control_version: AgentControlVersion::new(1),
                    action: DurableActionObservation {
                        action_id: "quorum".to_string(),
                        signature: "wait-for-catch-up:Write".to_string(),
                        state: DurableActionState::Failed,
                        error_class: Some(DurableActionErrorClass::NoWriteQuorum),
                        error: Some("no write quorum".to_string()),
                        result: None,
                        add_replica_progress: None,
                        remove_replica_progress: None,
                    },
                },
            },
            "quorum",
            "wait-for-catch-up:Write",
            &generation,
            AgentControlVersion::new(0),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            KubericError::RemoteAgentTerminalFailure {
                class: DurableActionErrorClass::NoWriteQuorum,
                ..
            }
        ));

        let mismatched = correlated_acknowledgement_result(
            CorrelatedControlActionAcknowledgement {
                observation: CorrelatedActionObservation {
                    generation: generation.clone(),
                    control_version: AgentControlVersion::new(1),
                    action: DurableActionObservation {
                        action_id: "different".to_string(),
                        signature: "wait-for-catch-up:Write".to_string(),
                        state: DurableActionState::Completed,
                        error_class: None,
                        error: None,
                        result: None,
                        add_replica_progress: None,
                        remove_replica_progress: None,
                    },
                },
            },
            "quorum",
            "wait-for-catch-up:Write",
            &generation,
            AgentControlVersion::new(0),
        );
        assert!(matches!(
            mismatched,
            Err(KubericError::RemoteAgentRequestRejected(_))
        ));

        let replay = correlated_acknowledgement_result(
            CorrelatedControlActionAcknowledgement {
                observation: CorrelatedActionObservation {
                    generation: generation.clone(),
                    control_version: AgentControlVersion::new(1),
                    action: DurableActionObservation {
                        action_id: "quorum".to_string(),
                        signature: "wait-for-catch-up:Write".to_string(),
                        state: DurableActionState::Completed,
                        error_class: None,
                        error: None,
                        result: None,
                        add_replica_progress: None,
                        remove_replica_progress: None,
                    },
                },
            },
            "quorum",
            "wait-for-catch-up:Write",
            &generation,
            AgentControlVersion::new(1),
        );
        assert!(replay.is_ok());
    }

    #[test]
    fn unsupported_protocol_waits_without_fallback() {
        assert!(matches!(
            plan_dispatch_evidence(
                &pending(),
                &observed(kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION + 1),
                &ReplicaInstanceId::new("pod-uid"),
                &action(),
                true,
            ),
            DispatchEvidencePlan::WaitForSupportedProtocol
        ));
    }

    #[test]
    fn supported_protocol_replacement_incarnation_waits_without_dispatch() {
        assert!(matches!(
            plan_dispatch_evidence(
                &pending(),
                &observed(kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION),
                &ReplicaInstanceId::new("replacement-pod"),
                &action(),
                true,
            ),
            DispatchEvidencePlan::WaitForExactIncarnation
        ));
    }

    #[test]
    fn generation_drift_refreshes_fences_without_consuming_attempts() {
        let mut original = pending();
        original.dispatch_agent_generation = Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string());
        original.dispatch_agent_control_version = Some(9);
        original.dispatch_observed_runtime_epoch = Some(crate::crd::EpochStatus {
            data_loss_number: 1,
            configuration_number: 4,
        });
        let planned = persisted(plan_dispatch_evidence(
            &original,
            &observed(kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION),
            &ReplicaInstanceId::new("pod-uid"),
            &action(),
            true,
        ));
        assert_eq!(
            planned.dispatch_agent_generation.as_deref(),
            Some("0123456789abcdef0123456789abcdef")
        );
        assert_eq!(planned.dispatch_agent_control_version, Some(7));
        assert_eq!(planned.attempts, original.attempts);
        assert_eq!(
            planned.deadline_unix_seconds,
            original.deadline_unix_seconds
        );
    }

    #[test]
    fn nonexecuting_agent_rejection_clears_fences_without_consuming_attempts() {
        let snapshot = crate::crd::StablePartitionSnapshotStatus {
            epoch: crate::crd::EpochStatus {
                data_loss_number: 0,
                configuration_number: 1,
            },
            primary_id: 1,
            members: vec![
                crate::crd::StableReplicaSnapshotStatus {
                    id: 1,
                    instance_id: "one".to_string(),
                    role: StableReplicaRoleStatus::Primary,
                    election_metadata: None,
                },
                crate::crd::StableReplicaSnapshotStatus {
                    id: 2,
                    instance_id: "pod-uid".to_string(),
                    role: StableReplicaRoleStatus::ActiveSecondary,
                    election_metadata: None,
                },
            ],
            write_quorum: 2,
        };
        let mut operation = start_switchover("set-uid", snapshot, 2, 0).unwrap();
        operation.pending_action = Some(pending());
        let original_attempts = operation.pending_action.as_ref().unwrap().attempts;
        operation
            .pending_action
            .as_mut()
            .unwrap()
            .dispatch_agent_generation = Some("0123456789abcdef0123456789abcdef".to_string());

        let next = operation_after_dispatch_error(
            &operation,
            &KubericError::RemoteAgentPreconditionRejected(
                "stale agent control version".to_string(),
            ),
        );
        let pending = next.pending_action.unwrap();
        assert_eq!(pending.attempts, original_attempts);
        assert!(pending.dispatch_agent_generation.is_none());
        assert!(pending.last_error.unwrap().contains("stale agent"));
    }

    #[test]
    fn busy_agent_rejection_preserves_fences_and_attempt_budget() {
        let snapshot = crate::crd::StablePartitionSnapshotStatus {
            epoch: crate::crd::EpochStatus {
                data_loss_number: 0,
                configuration_number: 1,
            },
            primary_id: 1,
            members: vec![
                crate::crd::StableReplicaSnapshotStatus {
                    id: 1,
                    instance_id: "one".to_string(),
                    role: StableReplicaRoleStatus::Primary,
                    election_metadata: None,
                },
                crate::crd::StableReplicaSnapshotStatus {
                    id: 2,
                    instance_id: "pod-uid".to_string(),
                    role: StableReplicaRoleStatus::ActiveSecondary,
                    election_metadata: None,
                },
            ],
            write_quorum: 2,
        };
        let mut operation = start_switchover("set-uid", snapshot, 2, 0).unwrap();
        let mut pending = pending();
        pending.dispatch_agent_generation = Some("0123456789abcdef0123456789abcdef".to_string());
        let attempts = pending.attempts;
        operation.pending_action = Some(pending);

        let next = operation_after_dispatch_error(&operation, &KubericError::AgentBusy);
        let pending = next.pending_action.unwrap();
        assert_eq!(pending.attempts, attempts);
        assert!(pending.dispatch_agent_generation.is_some());
        assert!(pending.last_error.unwrap().contains("busy"));
    }
}
