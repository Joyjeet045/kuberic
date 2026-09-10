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
use kuberic_core::types::{
    AgentControlVersion, AgentGeneration, CorrelatedControlActionAcknowledgement,
    CorrelatedControlActionRequest, DurableActionState, DurableReplicaAction,
    ReplicaConfigurationMemberStatus, ReplicaConfigurationMode, ReplicaConfigurationStatus,
    ReplicaElectionConfiguration, ReplicaStatusInfo,
};
use kuberic_core::types::{Epoch, ReplicaId, ReplicaInstanceId, StablePartitionSnapshot};

use crate::cluster_api::ClusterApi;
use crate::crd::{
    DurableAddMode, DurableOperationKind, DurableOperationPhase, DurableOperationStatus,
    DurableRemoveMode, KubericSet, KubericSetSpec, KubericSetStatus, MemberStatus,
    PendingActionStatus, Phase, ReconfigurationPhase, StablePartitionSnapshotStatus,
    StableReplicaElectionMetadataStatus, StableReplicaRoleStatus, StableReplicaSnapshotStatus,
    StatusCondition,
};
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
    pending_statuses: Mutex<HashMap<String, KubericSetStatus>>,
    removal_clock: Arc<dyn RemoveReplicaClock>,
}

impl Default for ReconcilerState {
    fn default() -> Self {
        Self {
            drivers: Mutex::new(HashMap::new()),
            pending_statuses: Mutex::new(HashMap::new()),
            removal_clock: Arc::new(SystemRemoveReplicaClock),
        }
    }
}

impl ReconcilerState {
    pub fn with_removal_clock(clock: Arc<dyn RemoveReplicaClock>) -> Self {
        Self {
            drivers: Mutex::new(HashMap::new()),
            pending_statuses: Mutex::new(HashMap::new()),
            removal_clock: clock,
        }
    }
}

/// Result of a reconciliation — either requeue after a duration, or done.
pub enum ReconcileAction {
    Requeue(Duration),
}

enum DispatchEvidencePlan {
    Ready,
    Persist(Box<PendingActionStatus>),
    WaitForExactIncarnation,
    WaitForSupportedProtocol,
}

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
    let mut planned = pending.clone();
    let exact_incarnation = addressed_instance.as_str() == pending.target_instance_id
        && observed.instance_id.as_str() == pending.target_instance_id;
    if !exact_incarnation {
        return DispatchEvidencePlan::WaitForExactIncarnation;
    }
    let agent = &observed.agent;
    if agent.protocol_version != kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION {
        return DispatchEvidencePlan::WaitForSupportedProtocol;
    }
    let generation = agent.generation.to_string();
    let control_version = agent.control_version.value();
    let runtime_epoch = crate::crd::EpochStatus {
        data_loss_number: observed.epoch.data_loss_number,
        configuration_number: observed.epoch.configuration_number,
    };
    let evidence_matches = pending.dispatch_agent_generation.as_deref() == Some(&generation)
        && pending.dispatch_agent_control_version == Some(control_version)
        && pending.dispatch_observed_runtime_epoch.as_ref() == Some(&runtime_epoch);
    planned.dispatch_agent_generation = Some(generation);
    planned.dispatch_agent_control_version = Some(control_version);
    planned.dispatch_observed_runtime_epoch = Some(runtime_epoch);
    let local_record_exists = correlated_action_observation(observed, &pending.action_id).is_some();
    if persist_action_payload {
        if planned.dispatch_action_payload.is_empty() || (!evidence_matches && !local_record_exists)
        {
            let Ok(payload) =
                kuberic_core::grpc::convert::encode_direct_correlated_action_payload(action)
            else {
                return DispatchEvidencePlan::WaitForSupportedProtocol;
            };
            planned.dispatch_action_payload = payload;
        }
    } else {
        planned.dispatch_action_payload.clear();
    }

    if planned == *pending {
        DispatchEvidencePlan::Ready
    } else {
        DispatchEvidencePlan::Persist(Box::new(planned))
    }
}

async fn execute_planned_control_action(
    handle: &dyn ReplicaHandle,
    pending: &PendingActionStatus,
    authoritative_action: Option<DurableReplicaAction>,
) -> kuberic_core::Result<()> {
    let action = match authoritative_action {
        Some(action) => action,
        None => kuberic_core::grpc::convert::decode_direct_correlated_action_payload(
            &pending.dispatch_action_payload,
        )
        .map_err(|error| KubericError::Internal(error.into()))?,
    };
    let generation = pending
        .dispatch_agent_generation
        .as_deref()
        .ok_or_else(|| {
            KubericError::Internal("correlated dispatch is missing agent generation".into())
        })
        .and_then(|generation| {
            AgentGeneration::parse(generation).map_err(|error| KubericError::Internal(error.into()))
        })?;
    let control_version = pending.dispatch_agent_control_version.ok_or_else(|| {
        KubericError::Internal("correlated dispatch is missing agent control version".into())
    })?;
    let observed_epoch = pending
        .dispatch_observed_runtime_epoch
        .as_ref()
        .ok_or_else(|| {
            KubericError::Internal("correlated dispatch is missing observed runtime epoch".into())
        })?;
    let input_signature = action.signature();
    handle
        .execute_correlated_control_action(CorrelatedControlActionRequest {
            protocol_version: kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION,
            action_id: pending.action_id.clone(),
            input_signature: input_signature.clone(),
            target_replica_id: pending.target_id,
            target_instance_id: ReplicaInstanceId::new(pending.target_instance_id.clone()),
            expected_agent_generation: generation.clone(),
            expected_control_version: AgentControlVersion::new(control_version),
            observed_runtime_epoch: Epoch::new(
                observed_epoch.data_loss_number,
                observed_epoch.configuration_number,
            ),
            action,
        })
        .await
        .and_then(|acknowledgement| {
            correlated_acknowledgement_result(
                acknowledgement,
                &pending.action_id,
                &input_signature,
                &generation,
                AgentControlVersion::new(control_version),
            )
        })
}

fn correlated_acknowledgement_result(
    acknowledgement: CorrelatedControlActionAcknowledgement,
    expected_action_id: &str,
    expected_signature: &str,
    expected_generation: &AgentGeneration,
    expected_control_version: AgentControlVersion,
) -> kuberic_core::Result<()> {
    let observation = &acknowledgement.observation;
    if observation.generation != *expected_generation
        || observation.control_version.value() == 0
        || observation.control_version.value() > expected_control_version.value().saturating_add(1)
        || observation.action.action_id != expected_action_id
        || observation.action.signature != expected_signature
    {
        return Err(KubericError::RemoteAgentRequestRejected(
            "correlated acknowledgement does not match the dispatched action".to_string(),
        ));
    }
    if acknowledgement.observation.action.state != DurableActionState::Failed {
        return Ok(());
    }
    let action = acknowledgement.observation.action;
    let class = action.error_class.ok_or_else(|| {
        KubericError::RemoteAgentRequestRejected(
            "failed correlated acknowledgement has no error class".to_string(),
        )
    })?;
    Err(KubericError::RemoteAgentTerminalFailure {
        class,
        message: action
            .error
            .unwrap_or_else(|| "correlated control action failed".to_string()),
    })
}

fn dispatch_rejection_requires_refresh(error: &KubericError) -> bool {
    matches!(
        error,
        KubericError::RemoteAgentPreconditionRejected(_)
            | KubericError::RemoteAgentContinuityUnavailable(_)
    )
}

fn dispatch_rejection_is_retryable_without_execution(error: &KubericError) -> bool {
    matches!(error, KubericError::AgentBusy)
}

fn clear_dispatch_evidence(pending: &mut PendingActionStatus) {
    pending.dispatch_agent_generation = None;
    pending.dispatch_agent_control_version = None;
    pending.dispatch_observed_runtime_epoch = None;
    pending.dispatch_action_payload.clear();
}

fn operation_after_dispatch_error(
    operation: &DurableOperationStatus,
    error: &KubericError,
) -> DurableOperationStatus {
    if matches!(error, KubericError::RemoteAgentConflict(_)) {
        fail_closed(operation, &error.to_string())
    } else if dispatch_rejection_requires_refresh(error) {
        let mut next = operation.clone();
        if let Some(pending) = next.pending_action.as_mut() {
            clear_dispatch_evidence(pending);
            pending.last_error = Some(error.to_string().chars().take(512).collect());
        }
        next
    } else if dispatch_rejection_is_retryable_without_execution(error) {
        let mut next = operation.clone();
        if let Some(pending) = next.pending_action.as_mut() {
            pending.last_error = Some(error.to_string().chars().take(512).collect());
        }
        next
    } else {
        record_activity_error(operation, &error.to_string())
    }
}

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
        if set.status.as_ref() == Some(&pending) {
            state.pending_statuses.lock().await.remove(&set_key);
            return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
        }
        api.patch_set_status(
            &namespace,
            &name,
            &pending,
            set.metadata.resource_version.as_deref(),
        )
        .await?;
        state.pending_statuses.lock().await.remove(&set_key);
        return Ok(ReconcileAction::Requeue(Duration::from_secs(1)));
    }

    let label_selector = format!("kuberic.io/set={}", name);
    let pods = api.list_pods(&namespace, &label_selector).await?;

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
            persist_committed_status(
                api,
                state,
                &set_key,
                &namespace,
                &name,
                &status,
                set.metadata.resource_version.as_deref(),
            )
            .await?;
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
                let operation = start_remove_replica(
                    set.metadata.uid.as_deref().unwrap_or(&set_key),
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
                let mut status = KubericSetStatus {
                    phase: Phase::RemovingReplica,
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
                    let operation = start_remove_replica(
                        set.metadata.uid.as_deref().unwrap_or(&set_key),
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
                    let mut status = KubericSetStatus {
                        phase: Phase::RemovingReplica,
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
                    let operation = start_remove_replica(
                        set.metadata.uid.as_deref().unwrap_or(&set_key),
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
                    let mut status = KubericSetStatus {
                        phase: Phase::RemovingReplica,
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
                let operation = start_remove_replica(
                    set.metadata.uid.as_deref().unwrap_or(&set_key),
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
                let mut status = KubericSetStatus {
                    phase: Phase::RemovingReplica,
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
                let operation = start_switchover(
                    set.metadata.uid.as_deref().unwrap_or(&set_key),
                    previous_snapshot,
                    target_id,
                    now,
                )?;
                let mut status = KubericSetStatus {
                    phase: Phase::Switchover,
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
                let operation = start_remove_replica(
                    set.metadata.uid.as_deref().unwrap_or(&set_key),
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
                let mut status = KubericSetStatus {
                    phase: Phase::RemovingReplica,
                    operation: Some(operation.clone()),
                    ..set.status.clone().unwrap_or_default()
                };
                status
                    .conditions
                    .retain(|condition| condition.type_ != "ScaleDownTargetUnavailable");
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

        Phase::Switchover | Phase::AddingReplica | Phase::RemovingReplica => {
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
            persist_committed_status(
                api,
                state,
                set_key,
                &set.namespace().unwrap_or_default(),
                &set.name_any(),
                &status,
                set.metadata.resource_version.as_deref(),
            )
            .await?;
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
                conditions: Vec::new(),
                primary_failing_since: None,
                stable_election_metadata_refresh: None,
            };
            persist_committed_status(
                api,
                state,
                set_key,
                &set.namespace().unwrap_or_default(),
                &set.name_any(),
                &status,
                set.metadata.resource_version.as_deref(),
            )
            .await?;
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
                persist_committed_status(
                    api,
                    state,
                    set_key,
                    &namespace,
                    &name,
                    &status,
                    set.metadata.resource_version.as_deref(),
                )
                .await?;
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
                persist_committed_status(
                    api,
                    state,
                    set_key,
                    &namespace,
                    &name,
                    &status,
                    set.metadata.resource_version.as_deref(),
                )
                .await?;
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
            persist_committed_status(
                api,
                state,
                set_key,
                &namespace,
                &name,
                &status,
                set.metadata.resource_version.as_deref(),
            )
            .await?;
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
            persist_committed_status(
                api,
                state,
                set_key,
                &namespace,
                &name,
                &status,
                set.metadata.resource_version.as_deref(),
            )
            .await?;
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
    namespace: &str,
    name: &str,
    status: &KubericSetStatus,
    expected_resource_version: Option<&str>,
) -> Result<(), String> {
    match api
        .patch_set_status(namespace, name, status, expected_resource_version)
        .await
    {
        Ok(()) => {
            state.pending_statuses.lock().await.remove(set_key);
            Ok(())
        }
        Err(error) => {
            state
                .pending_statuses
                .lock()
                .await
                .insert(set_key.to_string(), status.clone());
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
    use kuberic_core::types::{
        AccessStatus, AgentControlVersion, AgentGeneration, CorrelatedActionObservation,
        DurableActionErrorClass, DurableActionObservation, ReplicaAgentStatus,
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
