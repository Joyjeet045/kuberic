use std::collections::HashSet;

use kuberic_core::remove_replica::{
    MAX_REMOVE_REPLICA_PRE_COMMIT_ATTEMPTS, REMOVE_REPLICA_CALL_TIMEOUT_SECONDS,
    REMOVE_REPLICA_COMPENSATION_GRACE_SECONDS, REMOVE_REPLICA_INTENT_PROTOCOL_VERSION,
    REMOVE_REPLICA_OVERALL_TIMEOUT_SECONDS, REMOVE_REPLICA_RETIREMENT_TIMEOUT_SECONDS,
    RemoveReplicaCoordinatorPhase, RemoveReplicaIntent, RemoveReplicaMode,
    RemoveReplicaTerminalResult, TargetRetirementObservation,
};
use kuberic_core::replica_lifecycle::{
    ConfigurationDescriptor, ConfigurationMemberDescriptor, ConfigurationProgressSource,
    REPLICA_LIFECYCLE_PEER_PROTOCOL_VERSION,
};
use kuberic_core::types::{
    AccessStatus, AgentControlVersion, AgentGeneration, DurableActionResult, DurableActionState,
    DurableReplicaAction, Epoch, ReplicaConfigurationMode, ReplicaConfigurationStatus,
    ReplicaInstanceId, ReplicaStatus, Role,
};

use crate::crd::{
    ConfigurationDescriptorStatus, ConfigurationMemberDescriptorStatus,
    ConfigurationProgressSourceStatus, DurableActionKind, DurableOperationKind,
    DurableOperationPhase, DurableOperationStatus, DurablePostconditionKind,
    DurablePostconditionStatus, DurableRemoveMode, PendingActionStatus,
    REMOVE_REPLICA_OPERATION_VERSION, RemoveReplicaCleanupStatus,
    RemoveReplicaCommitEvidenceStatus, RemoveReplicaCoordinatorPhaseStatus,
    RemoveReplicaDispositionStatus, RemoveReplicaIntentStatus, RemoveReplicaTerminalResultStatus,
    StablePartitionSnapshotStatus, StableReplicaRoleStatus, TargetRetirementObservationStatus,
};

use super::{
    Decision, OperationObservations, OperationPodIdentities, bounded_error,
    correlated_action_observation, poison,
};

pub struct RemoveReplicaTarget {
    pub replica_id: i64,
    pub pod_name: String,
    pub pod_uid: String,
    pub replicator_address: String,
    pub agent_generation: Option<AgentGeneration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigurationObservation {
    PreviousCurrent,
    ReducedCatchUp,
    ReducedCurrent,
}

pub fn start_remove_replica(
    set_uid: &str,
    previous: StablePartitionSnapshotStatus,
    target_request: RemoveReplicaTarget,
    mode: DurableRemoveMode,
    min_replicas: usize,
    now: i64,
) -> Result<DurableOperationStatus, String> {
    validate_snapshot(&previous)?;
    if target_request.pod_name.is_empty()
        || target_request.pod_uid.is_empty()
        || target_request.replicator_address.is_empty()
    {
        return Err(
            "remove target pod name, UID, and historical replicator address must be non-empty"
                .to_string(),
        );
    }
    if mode == DurableRemoveMode::ScaleDown && target_request.agent_generation.is_none() {
        return Err("ScaleDown target has no preadmitted agent generation".to_string());
    }
    if target_request.replica_id == previous.primary_id {
        return Err("cannot remove the primary".to_string());
    }
    let target_member = previous
        .members
        .iter()
        .find(|member| member.id == target_request.replica_id)
        .ok_or_else(|| {
            format!(
                "remove target replica {} is not committed",
                target_request.replica_id
            )
        })?;
    if target_member.role != StableReplicaRoleStatus::ActiveSecondary {
        return Err("only a stable secondary can be removed".to_string());
    }
    if target_member.instance_id != target_request.pod_uid {
        return Err("remove target runtime incarnation and pod UID differ".to_string());
    }

    let mut target_snapshot = previous.clone();
    target_snapshot
        .members
        .retain(|member| member.id != target_request.replica_id);
    if target_snapshot.members.len() < min_replicas.max(1) {
        return Err(format!(
            "removal would reduce membership below minReplicas ({})",
            min_replicas.max(1)
        ));
    }
    if target_snapshot.members.len() < previous.write_quorum as usize {
        return Err("retained members cannot satisfy the previous write quorum".to_string());
    }
    target_snapshot.write_quorum = target_snapshot.members.len() as u32 / 2 + 1;
    validate_snapshot(&target_snapshot)?;

    let operation_id = format!(
        "{set_uid}:remove-replica:v{REMOVE_REPLICA_OPERATION_VERSION}:{}-{}:{}@{}",
        previous.epoch.data_loss_number,
        previous.epoch.configuration_number,
        target_request.replica_id,
        target_member.instance_id
    );
    Ok(DurableOperationStatus {
        execution_id: format!("{operation_id}:execution-1"),
        operation_id,
        version: REMOVE_REPLICA_OPERATION_VERSION,
        kind: DurableOperationKind::RemoveReplica,
        phase: DurableOperationPhase::RemoveFreezeIntent,
        previous_snapshot: previous.clone().into(),
        target_snapshot,
        committed_snapshot: None,
        minimum_committed_replicas: Some(min_replicas.max(1) as u32),
        old_primary_id: previous.primary_id,
        target_primary_id: previous.primary_id,
        add_mode: None,
        remove_mode: Some(mode),
        target_replica_id: Some(target_request.replica_id),
        target_instance_id: Some(target_member.instance_id.clone()),
        target_pod_name: Some(target_request.pod_name),
        target_pod_uid: Some(target_request.pod_uid),
        remove_target_replicator_address: Some(target_request.replicator_address),
        remove_target_agent_generation: target_request
            .agent_generation
            .map(|generation| generation.to_string()),
        retired_instance_id: None,
        frozen_lsn: None,
        next_secondary_index: 0,
        phase_deadline_unix_seconds: now + REMOVE_REPLICA_OVERALL_TIMEOUT_SECONDS,
        pending_action: None,
        last_error: None,
        failover: None,
        add_intent: None,
        remove_intent: None,
        remove_commit_evidence: None,
        remove_cleanup: None,
        removal_disposition: None,
    })
}

pub fn decide_remove_replica(
    operation: &DurableOperationStatus,
    observations: &OperationObservations,
    pod_identities: &OperationPodIdentities,
    target_pod_role_label: Option<&str>,
    now: i64,
) -> Result<Decision, String> {
    validate_operation(operation)?;
    if matches!(
        operation.phase,
        DurableOperationPhase::Completed
            | DurableOperationPhase::Failed
            | DurableOperationPhase::Poisoned
    ) {
        return Ok(Decision::Wait);
    }

    if operation.remove_commit_evidence.is_some() {
        return decide_post_commit(
            operation,
            observations,
            pod_identities,
            target_pod_role_label,
            now,
        );
    }

    match operation.phase {
        DurableOperationPhase::RemoveFreezeIntent => freeze_attempt(operation, observations, now),
        DurableOperationPhase::RemoveDispatchIntent => {
            let intent = operation
                .remove_intent
                .as_ref()
                .ok_or_else(|| "remove dispatch phase has no frozen intent".to_string())?;
            let mut next = operation.clone();
            next.phase = DurableOperationPhase::RemoveAwaitCoordination;
            next.pending_action = Some(PendingActionStatus {
                action_id: intent.action_id.clone(),
                sequence: intent.attempt,
                kind: DurableActionKind::RemoveReplicaIntent,
                target_id: operation.old_primary_id,
                target_instance_id: intent.primary_instance_id.clone(),
                expected_epoch: operation.previous_snapshot.epoch.clone(),
                desired_postcondition: DurablePostconditionStatus {
                    kind: DurablePostconditionKind::RemoveReplicaCoordinated,
                    role: None,
                },
                attempts: 0,
                deadline_unix_seconds: intent.overall_deadline_unix_seconds,
                last_error: None,
                dispatch_authorized: false,
                dispatch_agent_generation: None,
                dispatch_agent_control_version: None,
                dispatch_observed_runtime_epoch: None,
                dispatch_action_payload: String::new(),
            });
            Ok(Decision::Persist(next))
        }
        DurableOperationPhase::RemoveAwaitCoordination => {
            decide_await_coordination(operation, observations, pod_identities, now)
        }
        DurableOperationPhase::RemoveRecordCommit => {
            Err("remove commit-record phase has no durable commit evidence".to_string())
        }
        DurableOperationPhase::RemoveCompensateFinalize => {
            let mut failed = operation.clone();
            failed.phase = DurableOperationPhase::Failed;
            failed.pending_action = None;
            Ok(Decision::Complete {
                operation: failed,
                snapshot: operation.previous_snapshot.cloned().unwrap(),
                compensated: true,
            })
        }
        phase => Err(format!(
            "phase {phase:?} is not valid for pre-commit coarse removal"
        )),
    }
}

fn freeze_attempt(
    operation: &DurableOperationStatus,
    observations: &OperationObservations,
    now: i64,
) -> Result<Decision, String> {
    let primary = observations
        .get(&operation.old_primary_id)
        .ok_or_else(|| "stable primary is unavailable while freezing remove intent".to_string())?;
    validate_primary_identity(operation, primary)?;
    let observed_configuration = classify_configuration(operation, &primary.status)?;

    let target_id = target_replica_id(operation)?;
    let target = observations.get(&target_id);
    if operation.remove_mode == Some(DurableRemoveMode::ScaleDown) {
        let target = target.ok_or_else(|| {
            "ScaleDown target is unavailable while freezing remove intent".to_string()
        })?;
        validate_scale_down_target(operation, target)?;
    } else if let Some(target) = target {
        validate_optional_force_target(operation, target)?;
    }

    let previous_configuration =
        descriptor_for_snapshot(operation, &operation.previous_snapshot, observations, false)?;
    let reduced =
        descriptor_for_snapshot(operation, &operation.target_snapshot, observations, true)?;
    let post_commit_redrive = observed_configuration == ConfigurationObservation::ReducedCurrent
        && operation.remove_intent.as_ref().is_some_and(|intent| {
            intent.primary_agent_generation != primary.status.agent.generation.to_string()
        });
    let attempt = if post_commit_redrive {
        operation.remove_intent.as_ref().unwrap().attempt
    } else {
        operation
            .remove_intent
            .as_ref()
            .map_or(1, |intent| intent.attempt.saturating_add(1))
    };
    if attempt > MAX_REMOVE_REPLICA_PRE_COMMIT_ATTEMPTS {
        return Ok(Decision::Persist(failed_precommit(
            operation,
            "the durable three-attempt pre-commit removal limit is exhausted",
        )));
    }
    let attempt_id = if post_commit_redrive {
        format!(
            "{}:postcommit-{}",
            operation.operation_id, primary.status.agent.generation
        )
    } else {
        format!("{}:attempt-{attempt}", operation.operation_id)
    };
    let action_id = format!("{attempt_id}:RemoveReplicaIntent");
    let target_observation = target.filter(|target| {
        target.status.instance_id.as_str() == target_instance_id(operation).unwrap_or_default()
            && target.status.agent.lifecycle_peer_protocol_version
                == REPLICA_LIFECYCLE_PEER_PROTOCOL_VERSION
    });
    let core = RemoveReplicaIntent {
        protocol_version: REMOVE_REPLICA_INTENT_PROTOCOL_VERSION,
        operation_id: operation.operation_id.clone(),
        action_id: action_id.clone(),
        attempt_number: attempt,
        attempt_id: attempt_id.clone(),
        input_signature: String::new(),
        mode: remove_mode(operation)?,
        epoch: snapshot_epoch(&operation.previous_snapshot),
        primary_replica_id: operation.old_primary_id,
        primary_instance_id: primary.status.instance_id.clone(),
        primary_agent_generation: primary.status.agent.generation.clone(),
        primary_agent_control_version: primary.status.agent.control_version,
        primary_control_address: primary.control_address.clone(),
        primary_replicator_address: primary.replicator_address.clone(),
        target_replica_id: target_id,
        target_instance_id: ReplicaInstanceId::new(target_instance_id(operation)?.to_string()),
        expected_target_pod_uid: target_pod_uid(operation)?.to_string(),
        target_pod_name: target_pod_name(operation)?.to_string(),
        expected_target_agent_generation: target_observation
            .map(|target| target.status.agent.generation.clone()),
        target_control_address: target_observation.map(|target| target.control_address.clone()),
        target_replicator_address: target_observation
            .map(|target| target.replicator_address.clone()),
        target_lifecycle_peer_protocol_version: target_observation
            .map(|_| REPLICA_LIFECYCLE_PEER_PROTOCOL_VERSION),
        previous_configuration: previous_configuration.clone(),
        reduced_catch_up_configuration: reduced.clone(),
        reduced_current_configuration: reduced.clone(),
        required_write_quorum: operation.previous_snapshot.write_quorum,
        minimum_committed_replicas: operation.minimum_committed_replicas.unwrap_or(1),
        maximum_pre_commit_attempts: MAX_REMOVE_REPLICA_PRE_COMMIT_ATTEMPTS,
        overall_deadline_unix_seconds: now + REMOVE_REPLICA_OVERALL_TIMEOUT_SECONDS,
        compensation_grace_seconds: REMOVE_REPLICA_COMPENSATION_GRACE_SECONDS,
        compensation_deadline_cap_unix_seconds: now
            + REMOVE_REPLICA_OVERALL_TIMEOUT_SECONDS
            + REMOVE_REPLICA_COMPENSATION_GRACE_SECONDS,
        call_timeout_seconds: REMOVE_REPLICA_CALL_TIMEOUT_SECONDS,
        target_retirement_timeout_seconds: REMOVE_REPLICA_RETIREMENT_TIMEOUT_SECONDS,
    };
    let mut core = core;
    core.input_signature = core.signature();
    core.validate()?;
    let persisted = RemoveReplicaIntentStatus {
        attempt,
        attempt_id,
        action_id,
        input_signature: core.input_signature.clone(),
        primary_instance_id: core.primary_instance_id.to_string(),
        primary_agent_generation: core.primary_agent_generation.to_string(),
        primary_agent_control_version: core.primary_agent_control_version.value(),
        primary_control_address: core.primary_control_address.clone(),
        primary_replicator_address: core.primary_replicator_address.clone(),
        target_agent_generation: core
            .expected_target_agent_generation
            .as_ref()
            .map(ToString::to_string),
        target_control_address: core.target_control_address.clone(),
        target_replicator_address: core.target_replicator_address.clone(),
        target_lifecycle_peer_protocol_version: core.target_lifecycle_peer_protocol_version,
        previous_configuration: descriptor_status(&previous_configuration),
        reduced_catch_up_configuration: descriptor_status(&reduced),
        reduced_current_configuration: descriptor_status(&reduced),
        required_write_quorum: core.required_write_quorum,
        maximum_pre_commit_attempts: core.maximum_pre_commit_attempts,
        overall_deadline_unix_seconds: core.overall_deadline_unix_seconds,
        compensation_grace_seconds: core.compensation_grace_seconds,
        compensation_deadline_cap_unix_seconds: core.compensation_deadline_cap_unix_seconds,
        call_timeout_seconds: core.call_timeout_seconds,
        target_retirement_timeout_seconds: core.target_retirement_timeout_seconds,
        compensation_expiry_unix_seconds: None,
        last_observed_phase: None,
        last_observed_result: None,
        current_install_dispatched: false,
    };
    let mut next = operation.clone();
    next.remove_intent = Some(Box::new(persisted));
    next.pending_action = None;
    next.phase = DurableOperationPhase::RemoveDispatchIntent;
    next.phase_deadline_unix_seconds = core.overall_deadline_unix_seconds;
    next.last_error = None;
    Ok(Decision::Persist(next))
}

fn decide_await_coordination(
    operation: &DurableOperationStatus,
    observations: &OperationObservations,
    pod_identities: &OperationPodIdentities,
    now: i64,
) -> Result<Decision, String> {
    let intent = operation
        .remove_intent
        .as_ref()
        .ok_or_else(|| "remove coordination phase has no frozen intent".to_string())?;
    let core = core_intent(operation)?;
    let primary = observations.get(&operation.old_primary_id);

    let Some(primary) = primary else {
        if intent.current_install_dispatched && now >= intent.overall_deadline_unix_seconds {
            return Ok(Decision::Persist(invalid_removal(
                operation,
                "reduced-current installation was dispatched and primary configuration remained unavailable",
            )));
        }
        if precommit_failure_is_proven(intent, now) {
            return Ok(Decision::Persist(failed_precommit(
                operation,
                "same-generation primary remained unavailable through the proven pre-commit expiry",
            )));
        }
        return Ok(Decision::Wait);
    };
    if primary.status.instance_id.as_str() != intent.primary_instance_id {
        return Ok(Decision::Persist(invalid_removal(
            operation,
            "the primary runtime incarnation changed during removal",
        )));
    }

    if primary.status.agent.generation.to_string() != intent.primary_agent_generation {
        return classify_new_primary_generation(operation, primary, pod_identities, now);
    }
    if let Err(error) = validate_primary_identity(operation, primary) {
        return Ok(Decision::Persist(invalid_removal(operation, &error)));
    }

    let configuration = classify_configuration(operation, &primary.status);
    let observed = correlated_action_observation(&primary.status, &intent.action_id);
    if let Some(observed) = observed {
        if observed.signature != intent.input_signature {
            return Ok(Decision::Persist(invalid_removal(
                operation,
                "the primary retained the removal action ID with another signature",
            )));
        }
        let progress = observed.remove_replica_progress.as_ref();
        if let Some(progress) = progress {
            core.validate_progress(progress)?;
            if let Some(decision) = persist_progress_if_changed(operation, progress, observed)? {
                return Ok(decision);
            }
            if progress.commit_observed {
                if configuration? != ConfigurationObservation::ReducedCurrent {
                    return Ok(Decision::Persist(invalid_removal(
                        operation,
                        "coordinator commit evidence does not match exact reduced Current state",
                    )));
                }
                return record_commit(operation, progress);
            }
        }

        match observed.state {
            DurableActionState::Scheduled | DurableActionState::InProgress => {
                return Ok(Decision::Wait);
            }
            DurableActionState::Completed => {
                let result = match observed.result {
                    Some(DurableActionResult::RemoveReplica(result)) => result,
                    _ => {
                        return Ok(Decision::Persist(invalid_removal(
                            operation,
                            "completed remove coordinator has no typed removal result",
                        )));
                    }
                };
                return classify_terminal(operation, configuration, result, progress, now);
            }
            DurableActionState::Failed => {
                return classify_incomplete(operation, configuration, progress, now);
            }
        }
    }

    if configuration.is_err() {
        return classify_incomplete(operation, configuration, None, now);
    }
    match configuration? {
        ConfigurationObservation::PreviousCurrent | ConfigurationObservation::ReducedCatchUp
            if now < intent.overall_deadline_unix_seconds =>
        {
            Ok(Decision::Execute {
                target_id: operation.old_primary_id,
                action_id: intent.action_id.clone(),
                action: DurableReplicaAction::RemoveReplicaIntent {
                    intent: Box::new(core),
                },
            })
        }
        ConfigurationObservation::PreviousCurrent | ConfigurationObservation::ReducedCatchUp => {
            Ok(Decision::Persist(failed_precommit(
                operation,
                "remove coordinator has no retained evidence at the overall deadline",
            )))
        }
        ConfigurationObservation::ReducedCurrent => record_observed_commit(
            operation,
            &primary.status.agent.generation,
            now,
            exact_connection_absent(operation, &primary.status),
        ),
    }
}

fn classify_new_primary_generation(
    operation: &DurableOperationStatus,
    primary: &super::ReplicaObservation,
    pod_identities: &OperationPodIdentities,
    now: i64,
) -> Result<Decision, String> {
    let intent = operation.remove_intent.as_ref().unwrap();
    let complete_restart = primary.status.role == Role::Unknown
        || !primary.status.healthy
        || primary.status.configuration.is_none();
    if complete_restart {
        let same_pod = pod_identities
            .get(&operation.old_primary_id)
            .map(String::as_str)
            == Some(intent.primary_instance_id.as_str());
        if same_pod {
            return Ok(Decision::Persist(ambiguous_restart(
                operation,
                &primary.status.agent.generation,
                "the new primary-agent generation has no surviving exact runtime configuration or coordinator evidence",
            )));
        }
        return Ok(Decision::Persist(invalid_removal(
            operation,
            "the stable primary incarnation changed while removal was active",
        )));
    }
    validate_primary_identity(operation, primary)?;
    match classify_configuration(operation, &primary.status)? {
        ConfigurationObservation::ReducedCurrent => record_observed_commit(
            operation,
            &primary.status.agent.generation,
            now,
            exact_connection_absent(operation, &primary.status),
        ),
        ConfigurationObservation::PreviousCurrent | ConfigurationObservation::ReducedCatchUp
            if intent.current_install_dispatched =>
        {
            Ok(Decision::Persist(invalid_removal(
                operation,
                "the previous generation dispatched reduced-current installation without cancellation or quiescence proof",
            )))
        }
        ConfigurationObservation::PreviousCurrent | ConfigurationObservation::ReducedCatchUp => {
            if intent.attempt >= MAX_REMOVE_REPLICA_PRE_COMMIT_ATTEMPTS
                && primary.status.configuration.as_ref()
                    != Some(&configuration_status(
                        &operation.target_snapshot,
                        ReplicaConfigurationMode::Current,
                    ))
            {
                Ok(Decision::Persist(failed_precommit(
                    operation,
                    "the durable three-attempt pre-commit removal limit is exhausted",
                )))
            } else {
                let mut next = operation.clone();
                next.phase = DurableOperationPhase::RemoveFreezeIntent;
                next.pending_action = None;
                Ok(Decision::Persist(next))
            }
        }
    }
}

fn persist_progress_if_changed(
    operation: &DurableOperationStatus,
    progress: &kuberic_core::remove_replica::RemoveReplicaProgress,
    observed: &kuberic_core::types::DurableActionObservation,
) -> Result<Option<Decision>, String> {
    let intent = operation.remove_intent.as_ref().unwrap();
    if intent.current_install_dispatched && !progress.current_install_dispatched {
        return Ok(Some(Decision::Persist(invalid_removal(
            operation,
            "remove coordinator regressed durable reduced-current installation dispatch evidence",
        ))));
    }
    if let Some(persisted_expiry) = intent.compensation_expiry_unix_seconds
        && progress.compensation_expiry_unix_seconds != Some(persisted_expiry)
    {
        return Ok(Some(Decision::Persist(invalid_removal(
            operation,
            "remove compensation expiry changed after it was durably observed",
        ))));
    }
    let phase = progress.phase.into();
    let result = match observed.result {
        Some(DurableActionResult::RemoveReplica(result)) => Some(result.into()),
        Some(_) => {
            return Err("remove action carries an unrelated terminal result".to_string());
        }
        None => None,
    };
    if intent.last_observed_phase == Some(phase)
        && intent.last_observed_result == result
        && intent.compensation_expiry_unix_seconds == progress.compensation_expiry_unix_seconds
        && intent.current_install_dispatched == progress.current_install_dispatched
    {
        return Ok(None);
    }
    let mut next = operation.clone();
    let next_intent = next.remove_intent.as_mut().unwrap();
    next_intent.last_observed_phase = Some(phase);
    next_intent.last_observed_result = result;
    next_intent.compensation_expiry_unix_seconds = progress.compensation_expiry_unix_seconds;
    next_intent.current_install_dispatched = progress.current_install_dispatched;
    Ok(Some(Decision::Persist(next)))
}

fn classify_terminal(
    operation: &DurableOperationStatus,
    configuration: Result<ConfigurationObservation, String>,
    result: RemoveReplicaTerminalResult,
    progress: Option<&kuberic_core::remove_replica::RemoveReplicaProgress>,
    now: i64,
) -> Result<Decision, String> {
    match result {
        RemoveReplicaTerminalResult::CommittedClean
        | RemoveReplicaTerminalResult::CommittedDegraded => {
            if configuration? != ConfigurationObservation::ReducedCurrent {
                return Ok(Decision::Persist(invalid_removal(
                    operation,
                    "committed coordinator result lacks exact reduced Current configuration",
                )));
            }
            let progress = progress.ok_or_else(|| {
                "committed coordinator result has no retained progress".to_string()
            })?;
            record_commit(operation, progress)
        }
        RemoveReplicaTerminalResult::Compensated => {
            if operation
                .remove_intent
                .as_ref()
                .is_some_and(|intent| intent.current_install_dispatched)
                || progress.is_some_and(|progress| progress.current_install_dispatched)
            {
                return Ok(Decision::Persist(invalid_removal(
                    operation,
                    "compensated coordinator result followed reduced-current installation dispatch",
                )));
            }
            if configuration? != ConfigurationObservation::PreviousCurrent {
                return Ok(Decision::Persist(invalid_removal(
                    operation,
                    "compensated coordinator result lacks exact previous Current restoration",
                )));
            }
            let mut next = operation.clone();
            next.phase = DurableOperationPhase::RemoveCompensateFinalize;
            next.pending_action = None;
            next.last_error = Some("remove-replica pre-commit work was compensated".to_string());
            Ok(Decision::Persist(next))
        }
        RemoveReplicaTerminalResult::CompensationIncomplete => {
            classify_incomplete(operation, configuration, progress, now)
        }
    }
}

fn classify_incomplete(
    operation: &DurableOperationStatus,
    configuration: Result<ConfigurationObservation, String>,
    progress: Option<&kuberic_core::remove_replica::RemoveReplicaProgress>,
    now: i64,
) -> Result<Decision, String> {
    let intent = operation.remove_intent.as_ref().unwrap();
    match configuration {
        Ok(ConfigurationObservation::ReducedCurrent) => {
            let progress = progress.ok_or_else(|| {
                "reduced Current is observed but exact coordinator commit time is unavailable"
                    .to_string()
            })?;
            if progress.commit_observed {
                record_commit(operation, progress)
            } else {
                Ok(Decision::Persist(invalid_removal(
                    operation,
                    "reduced Current is observed without exact coordinator commit evidence",
                )))
            }
        }
        Ok(ConfigurationObservation::PreviousCurrent) => {
            if intent.current_install_dispatched {
                return Ok(Decision::Persist(invalid_removal(
                    operation,
                    "previous Current restoration followed reduced-current installation dispatch",
                )));
            }
            let mut next = operation.clone();
            next.phase = DurableOperationPhase::RemoveCompensateFinalize;
            next.pending_action = None;
            next.last_error = Some("previous Current configuration is restored".to_string());
            Ok(Decision::Persist(next))
        }
        Ok(ConfigurationObservation::ReducedCatchUp) if !intent.current_install_dispatched => {
            if intent.attempt < MAX_REMOVE_REPLICA_PRE_COMMIT_ATTEMPTS {
                let mut next = operation.clone();
                next.phase = DurableOperationPhase::RemoveFreezeIntent;
                next.pending_action = None;
                next.last_error = Some(
                    "pre-commit removal attempt ended with reduced CatchUp installed".to_string(),
                );
                Ok(Decision::Persist(next))
            } else {
                Ok(Decision::Persist(failed_precommit(
                    operation,
                    "reduced CatchUp remains installed after all three proven pre-commit attempts",
                )))
            }
        }
        Ok(_) if intent.current_install_dispatched => Ok(Decision::Persist(invalid_removal(
            operation,
            "reduced-current installation was dispatched but exact commit state is not proven",
        ))),
        Err(error) if intent.current_install_dispatched => {
            Ok(Decision::Persist(invalid_removal(operation, &error)))
        }
        Err(error) if precommit_failure_is_proven(intent, now) => {
            Ok(Decision::Persist(failed_precommit(operation, &error)))
        }
        Err(_) => Ok(Decision::Wait),
        Ok(ConfigurationObservation::ReducedCatchUp) => unreachable!(),
    }
}

fn record_commit(
    operation: &DurableOperationStatus,
    progress: &kuberic_core::remove_replica::RemoveReplicaProgress,
) -> Result<Decision, String> {
    let observed_unix_seconds = progress
        .commit_observed_unix_seconds
        .ok_or_else(|| "committed remove progress has no observation time".to_string())?;
    let intent = operation
        .remove_intent
        .as_ref()
        .ok_or_else(|| "committed remove progress has no frozen intent".to_string())?;
    let primary_generation = AgentGeneration::parse(intent.primary_agent_generation.clone())?;
    record_commit_evidence(
        operation,
        &primary_generation,
        observed_unix_seconds,
        progress.connection_absent,
        terminal_retirement(progress.target_retirement).map(Into::into),
        Some(progress.phase.into()),
        progress.compensation_expiry_unix_seconds,
        progress.current_install_dispatched,
    )
}

fn record_observed_commit(
    operation: &DurableOperationStatus,
    primary_generation: &AgentGeneration,
    observed_unix_seconds: i64,
    connection_absent: bool,
) -> Result<Decision, String> {
    record_commit_evidence(
        operation,
        primary_generation,
        observed_unix_seconds,
        connection_absent,
        None,
        None,
        operation
            .remove_intent
            .as_ref()
            .and_then(|intent| intent.compensation_expiry_unix_seconds),
        true,
    )
}

#[allow(clippy::too_many_arguments)]
fn record_commit_evidence(
    operation: &DurableOperationStatus,
    primary_generation: &AgentGeneration,
    observed_unix_seconds: i64,
    connection_absent: bool,
    target_retirement: Option<TargetRetirementObservationStatus>,
    observed_phase: Option<RemoveReplicaCoordinatorPhaseStatus>,
    compensation_expiry_unix_seconds: Option<i64>,
    current_install_dispatched: bool,
) -> Result<Decision, String> {
    let intent = operation.remove_intent.as_ref().unwrap();
    let mut next = operation.clone();
    next.phase = DurableOperationPhase::RemoveRecordCommit;
    next.pending_action = None;
    next.committed_snapshot = Some(operation.target_snapshot.clone());
    next.remove_commit_evidence = Some(RemoveReplicaCommitEvidenceStatus {
        attempt_id: intent.attempt_id.clone(),
        action_id: intent.action_id.clone(),
        primary_agent_generation: primary_generation.to_string(),
        configuration_signature: core_intent(operation)?
            .reduced_current_configuration
            .signature(),
        observed_unix_seconds,
    });
    next.remove_cleanup = Some(RemoveReplicaCleanupStatus {
        connection_absent,
        target_retirement,
        target_labels_fenced: false,
        target_pod_deleted: false,
    });
    let next_intent = next.remove_intent.as_mut().unwrap();
    if observed_phase.is_some() {
        next_intent.last_observed_phase = observed_phase;
    }
    next_intent.compensation_expiry_unix_seconds = compensation_expiry_unix_seconds;
    next_intent.current_install_dispatched = current_install_dispatched;
    Ok(Decision::RecordCommitEvidence(next))
}

fn decide_post_commit(
    operation: &DurableOperationStatus,
    observations: &OperationObservations,
    pod_identities: &OperationPodIdentities,
    target_pod_role_label: Option<&str>,
    now: i64,
) -> Result<Decision, String> {
    if operation.committed_snapshot.as_ref() != Some(&operation.target_snapshot) {
        return Ok(Decision::Persist(invalid_removal(
            operation,
            "remove commit evidence does not carry the exact frozen reduced snapshot",
        )));
    }
    match operation.phase {
        DurableOperationPhase::RemoveRecordCommit => {
            let mut next = operation.clone();
            next.phase = DurableOperationPhase::RemoveAwaitCleanup;
            Ok(Decision::Persist(next))
        }
        DurableOperationPhase::RemoveAwaitCleanup => {
            let mut next = operation.clone();
            let cleanup = next.remove_cleanup.get_or_insert_with(Default::default);
            let intent = operation.remove_intent.as_ref().unwrap();
            let committed_generation = operation
                .remove_commit_evidence
                .as_ref()
                .map(|evidence| evidence.primary_agent_generation.as_str())
                .unwrap_or(intent.primary_agent_generation.as_str());
            match observations.get(&operation.old_primary_id) {
                Some(primary)
                    if primary.status.instance_id.as_str() == intent.primary_instance_id
                        && primary.status.agent.generation.to_string() == committed_generation =>
                {
                    if classify_configuration(operation, &primary.status)?
                        != ConfigurationObservation::ReducedCurrent
                    {
                        return Ok(Decision::Persist(invalid_removal(
                            operation,
                            "post-commit primary no longer exposes exact reduced Current",
                        )));
                    }
                    cleanup.connection_absent = !primary
                        .status
                        .active_replica_connections
                        .iter()
                        .any(|connection| {
                            connection.id == target_replica_id(operation).unwrap_or_default()
                                && connection.instance_id.as_str()
                                    == target_instance_id(operation).unwrap_or_default()
                        });
                    if let Some(observed) =
                        correlated_action_observation(&primary.status, &intent.action_id)
                    {
                        if observed.signature != intent.input_signature {
                            return Ok(Decision::Persist(invalid_removal(
                                operation,
                                "post-commit primary action signature changed",
                            )));
                        }
                        if let Some(progress) = observed.remove_replica_progress.as_ref() {
                            cleanup.connection_absent |= progress.connection_absent;
                            merge_retirement_evidence(
                                &mut cleanup.target_retirement,
                                terminal_retirement(progress.target_retirement).map(Into::into),
                            );
                            if let Some(next_intent) = next.remove_intent.as_mut() {
                                next_intent.last_observed_phase = Some(progress.phase.into());
                                next_intent.last_observed_result = match observed.result {
                                    Some(DurableActionResult::RemoveReplica(result)) => {
                                        Some(result.into())
                                    }
                                    _ => next_intent.last_observed_result,
                                };
                            }
                        }
                    }
                }
                Some(_) => {
                    cleanup.connection_absent = true;
                    merge_retirement_evidence(
                        &mut cleanup.target_retirement,
                        Some(TargetRetirementObservationStatus::Unavailable),
                    );
                }
                None if pod_identities
                    .get(&operation.old_primary_id)
                    .map(String::as_str)
                    == Some(intent.primary_instance_id.as_str()) => {}
                None => {
                    cleanup.connection_absent = true;
                    merge_retirement_evidence(
                        &mut cleanup.target_retirement,
                        Some(TargetRetirementObservationStatus::Unavailable),
                    );
                }
            }
            if observations
                .get(&target_replica_id(operation)?)
                .is_some_and(|target| {
                    target.status.instance_id.as_str() == target_instance_id(operation).unwrap()
                        && target.status.role == Role::None
                        && !target.status.healthy
                })
            {
                merge_retirement_evidence(
                    &mut cleanup.target_retirement,
                    Some(TargetRetirementObservationStatus::Completed),
                );
            }
            if !cleanup.connection_absent {
                return if next == *operation {
                    Ok(Decision::Wait)
                } else {
                    Ok(Decision::Persist(next))
                };
            }
            if cleanup.target_retirement.is_none() {
                let expiry = operation
                    .remove_commit_evidence
                    .as_ref()
                    .map(|evidence| {
                        evidence
                            .observed_unix_seconds
                            .saturating_add(REMOVE_REPLICA_RETIREMENT_TIMEOUT_SECONDS)
                            .min(intent.overall_deadline_unix_seconds)
                    })
                    .unwrap_or(now);
                if now < expiry {
                    return if next == *operation {
                        Ok(Decision::Wait)
                    } else {
                        Ok(Decision::Persist(next))
                    };
                }
                merge_retirement_evidence(
                    &mut cleanup.target_retirement,
                    Some(TargetRetirementObservationStatus::Unavailable),
                );
            }
            if !cleanup.target_labels_fenced {
                if pod_identities
                    .get(&target_replica_id(operation)?)
                    .map(String::as_str)
                    != Some(target_pod_uid(operation)?)
                {
                    cleanup.target_labels_fenced = true;
                    return Ok(Decision::Persist(next));
                }
                if target_pod_role_label != Some("retired") {
                    return Ok(Decision::PatchPodRoleExactUid {
                        target_id: target_replica_id(operation)?,
                        expected_uid: target_pod_uid(operation)?.to_string(),
                        role: "retired".to_string(),
                    });
                }
                cleanup.target_labels_fenced = true;
            }
            next.phase = DurableOperationPhase::RemoveDeleteTargetPod;
            Ok(Decision::Persist(next))
        }
        DurableOperationPhase::RemoveDeleteTargetPod => {
            if pod_identities
                .get(&target_replica_id(operation)?)
                .map(String::as_str)
                == Some(target_pod_uid(operation)?)
            {
                return Ok(Decision::DeletePod {
                    pod_name: target_pod_name(operation)?.to_string(),
                    expected_uid: target_pod_uid(operation)?.to_string(),
                });
            }
            let mut next = operation.clone();
            next.remove_cleanup
                .get_or_insert_with(Default::default)
                .target_pod_deleted = true;
            next.phase = DurableOperationPhase::RemovePublishTopology;
            Ok(Decision::Persist(next))
        }
        DurableOperationPhase::RemovePublishTopology => {
            let cleanup = operation
                .remove_cleanup
                .as_ref()
                .ok_or_else(|| "remove publication has no cleanup evidence".to_string())?;
            if !cleanup.connection_absent
                || cleanup.target_retirement.is_none()
                || !cleanup.target_labels_fenced
                || !cleanup.target_pod_deleted
            {
                return Ok(Decision::Persist(invalid_removal(
                    operation,
                    "remove publication lacks exact post-commit cleanup evidence",
                )));
            }
            let mut next = operation.clone();
            next.phase = DurableOperationPhase::RemoveFinalize;
            Ok(Decision::CommitSnapshot {
                operation: next,
                snapshot: operation.target_snapshot.clone(),
            })
        }
        DurableOperationPhase::RemoveFinalize => {
            let mut completed = operation.clone();
            completed.phase = DurableOperationPhase::Completed;
            completed.pending_action = None;
            Ok(Decision::Complete {
                operation: completed,
                snapshot: operation.target_snapshot.clone(),
                compensated: false,
            })
        }
        phase => Err(format!(
            "phase {phase:?} is not valid for committed coarse removal"
        )),
    }
}

pub(crate) fn core_intent(
    operation: &DurableOperationStatus,
) -> Result<RemoveReplicaIntent, String> {
    let intent = operation
        .remove_intent
        .as_ref()
        .ok_or_else(|| "remove operation has no frozen intent".to_string())?;
    let core = RemoveReplicaIntent {
        protocol_version: REMOVE_REPLICA_INTENT_PROTOCOL_VERSION,
        operation_id: operation.operation_id.clone(),
        action_id: intent.action_id.clone(),
        attempt_number: intent.attempt,
        attempt_id: intent.attempt_id.clone(),
        input_signature: intent.input_signature.clone(),
        mode: remove_mode(operation)?,
        epoch: snapshot_epoch(&operation.previous_snapshot),
        primary_replica_id: operation.old_primary_id,
        primary_instance_id: ReplicaInstanceId::new(intent.primary_instance_id.clone()),
        primary_agent_generation: AgentGeneration::parse(intent.primary_agent_generation.clone())?,
        primary_agent_control_version: AgentControlVersion::new(
            intent.primary_agent_control_version,
        ),
        primary_control_address: intent.primary_control_address.clone(),
        primary_replicator_address: intent.primary_replicator_address.clone(),
        target_replica_id: target_replica_id(operation)?,
        target_instance_id: ReplicaInstanceId::new(target_instance_id(operation)?.to_string()),
        expected_target_pod_uid: target_pod_uid(operation)?.to_string(),
        target_pod_name: target_pod_name(operation)?.to_string(),
        expected_target_agent_generation: intent
            .target_agent_generation
            .clone()
            .map(AgentGeneration::parse)
            .transpose()?,
        target_control_address: intent.target_control_address.clone(),
        target_replicator_address: intent.target_replicator_address.clone(),
        target_lifecycle_peer_protocol_version: intent.target_lifecycle_peer_protocol_version,
        previous_configuration: core_descriptor(&intent.previous_configuration)?,
        reduced_catch_up_configuration: core_descriptor(&intent.reduced_catch_up_configuration)?,
        reduced_current_configuration: core_descriptor(&intent.reduced_current_configuration)?,
        required_write_quorum: intent.required_write_quorum,
        minimum_committed_replicas: operation.minimum_committed_replicas.unwrap_or(1),
        maximum_pre_commit_attempts: intent.maximum_pre_commit_attempts,
        overall_deadline_unix_seconds: intent.overall_deadline_unix_seconds,
        compensation_grace_seconds: intent.compensation_grace_seconds,
        compensation_deadline_cap_unix_seconds: intent.compensation_deadline_cap_unix_seconds,
        call_timeout_seconds: intent.call_timeout_seconds,
        target_retirement_timeout_seconds: intent.target_retirement_timeout_seconds,
    };
    core.validate()?;
    Ok(core)
}

fn descriptor_for_snapshot(
    operation: &DurableOperationStatus,
    snapshot: &StablePartitionSnapshotStatus,
    observations: &OperationObservations,
    reduced: bool,
) -> Result<ConfigurationDescriptor, String> {
    let target_id = target_replica_id(operation)?;
    let target_instance = target_instance_id(operation)?;
    let mut members = Vec::new();
    for member in &snapshot.members {
        if member.id == snapshot.primary_id {
            continue;
        }
        let observation = observations
            .get(&member.id)
            .filter(|observation| observation.status.instance_id.as_str() == member.instance_id);
        let is_target = member.id == target_id && member.instance_id == target_instance;
        if observation.is_none() && !is_target {
            return Err(format!(
                "retained replica {} is unavailable while freezing remove intent",
                member.id
            ));
        }
        let replicator_address = observation
            .map(|observation| observation.replicator_address.clone())
            .or_else(|| operation.remove_target_replicator_address.clone())
            .ok_or_else(|| "remove target has no historical replicator address".to_string())?;
        let (current_progress, catch_up_capability) = observation
            .map(|observation| {
                (
                    observation.status.current_progress,
                    observation
                        .status
                        .catch_up_capability
                        .unwrap_or(observation.status.current_progress),
                )
            })
            .or_else(|| {
                member
                    .election_metadata
                    .as_ref()
                    .map(|metadata| (metadata.current_lsn, metadata.first_retained_lsn))
            })
            .unwrap_or((0, 0));
        members.push(ConfigurationMemberDescriptor {
            id: member.id,
            instance_id: ReplicaInstanceId::new(member.instance_id.clone()),
            role: Role::ActiveSecondary,
            status: if observation.is_some() {
                ReplicaStatus::Up
            } else {
                ReplicaStatus::Down
            },
            replicator_address,
            must_catch_up: false,
            progress: ConfigurationProgressSource::Frozen {
                current_progress,
                catch_up_capability,
            },
        });
    }
    members.sort_by_key(|member| member.id);
    let descriptor = ConfigurationDescriptor {
        members,
        write_quorum: snapshot.write_quorum,
    };
    if reduced
        && descriptor
            .members
            .iter()
            .any(|member| member.id == target_id)
    {
        return Err("reduced remove descriptor still contains the target".to_string());
    }
    Ok(descriptor)
}

fn descriptor_status(descriptor: &ConfigurationDescriptor) -> ConfigurationDescriptorStatus {
    ConfigurationDescriptorStatus {
        members: descriptor
            .members
            .iter()
            .map(|member| {
                let ConfigurationProgressSource::Frozen {
                    current_progress,
                    catch_up_capability,
                } = member.progress
                else {
                    unreachable!("remove descriptors never use build progress")
                };
                ConfigurationMemberDescriptorStatus {
                    id: member.id,
                    instance_id: member.instance_id.to_string(),
                    role: format!("{:?}", member.role),
                    status: format!("{:?}", member.status),
                    replicator_address: member.replicator_address.clone(),
                    must_catch_up: member.must_catch_up,
                    progress_source: ConfigurationProgressSourceStatus::Frozen,
                    current_progress,
                    catch_up_capability,
                }
            })
            .collect(),
        write_quorum: descriptor.write_quorum,
    }
}

fn core_descriptor(
    descriptor: &ConfigurationDescriptorStatus,
) -> Result<ConfigurationDescriptor, String> {
    let mut ids = HashSet::new();
    let mut instances = HashSet::new();
    let members = descriptor
        .members
        .iter()
        .map(|member| {
            if member.id <= 0 || !ids.insert(member.id) {
                return Err("remove descriptor has an invalid or duplicate replica ID".to_string());
            }
            if member.instance_id.is_empty() || !instances.insert(member.instance_id.clone()) {
                return Err("remove descriptor has a missing or duplicate incarnation".to_string());
            }
            if member.role != "ActiveSecondary"
                || member.must_catch_up
                || member.progress_source != ConfigurationProgressSourceStatus::Frozen
            {
                return Err("remove descriptor has unsupported member semantics".to_string());
            }
            let status = match member.status.as_str() {
                "Up" => ReplicaStatus::Up,
                "Down" => ReplicaStatus::Down,
                _ => return Err("remove descriptor has an unknown member status".to_string()),
            };
            Ok(ConfigurationMemberDescriptor {
                id: member.id,
                instance_id: ReplicaInstanceId::new(member.instance_id.clone()),
                role: Role::ActiveSecondary,
                status,
                replicator_address: member.replicator_address.clone(),
                must_catch_up: false,
                progress: ConfigurationProgressSource::Frozen {
                    current_progress: member.current_progress,
                    catch_up_capability: member.catch_up_capability,
                },
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(ConfigurationDescriptor {
        members,
        write_quorum: descriptor.write_quorum,
    })
}

fn validate_operation(operation: &DurableOperationStatus) -> Result<(), String> {
    if operation.version != REMOVE_REPLICA_OPERATION_VERSION {
        return Err(format!(
            "unsupported remove-replica operation version {}; expected {}",
            operation.version, REMOVE_REPLICA_OPERATION_VERSION
        ));
    }
    if operation.kind != DurableOperationKind::RemoveReplica {
        return Err("remove decision received another operation kind".to_string());
    }
    validate_snapshot(&operation.previous_snapshot)?;
    validate_snapshot(&operation.target_snapshot)?;
    if operation.previous_snapshot.members.len() != operation.target_snapshot.members.len() + 1
        || operation.previous_snapshot.primary_id != operation.target_snapshot.primary_id
        || operation.previous_snapshot.epoch != operation.target_snapshot.epoch
        || operation.old_primary_id != operation.previous_snapshot.primary_id
        || operation.target_primary_id != operation.previous_snapshot.primary_id
    {
        return Err("remove snapshot transition is invalid".to_string());
    }
    let target_id = target_replica_id(operation)?;
    let target_instance = target_instance_id(operation)?;
    if target_instance != target_pod_uid(operation)? {
        return Err("remove target runtime incarnation and pod UID differ".to_string());
    }
    target_pod_name(operation)?;
    operation
        .remove_target_replicator_address
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "remove operation has no historical target endpoint".to_string())?;
    if target_id == operation.previous_snapshot.primary_id {
        return Err("cannot remove the primary".to_string());
    }
    let previous_target = operation
        .previous_snapshot
        .members
        .iter()
        .find(|member| member.id == target_id)
        .ok_or_else(|| "remove previous snapshot has no target".to_string())?;
    if previous_target.instance_id != target_instance
        || previous_target.role != StableReplicaRoleStatus::ActiveSecondary
        || operation
            .target_snapshot
            .members
            .iter()
            .any(|member| member.id == target_id || member.instance_id == target_instance)
    {
        return Err("remove target identity or role is invalid".to_string());
    }
    for retained in &operation.target_snapshot.members {
        if !operation
            .previous_snapshot
            .members
            .iter()
            .any(|member| member == retained)
        {
            return Err(format!(
                "target snapshot changed retained replica {}",
                retained.id
            ));
        }
    }
    operation
        .remove_mode
        .ok_or_else(|| "remove operation has no mode".to_string())?;
    if operation.add_mode.is_some() || operation.add_intent.is_some() {
        return Err("remove operation contains add state".to_string());
    }
    if operation.remove_commit_evidence.is_some() && operation.remove_intent.is_none() {
        return Err("remove commit evidence has no frozen intent".to_string());
    }
    if operation.remove_mode == Some(DurableRemoveMode::ScaleDown) {
        let generation = operation
            .remove_target_agent_generation
            .as_deref()
            .ok_or_else(|| {
                "ScaleDown operation has no preadmitted target generation".to_string()
            })?;
        AgentGeneration::parse(generation.to_string())?;
    }
    if let Some(intent) = &operation.remove_intent {
        if intent.action_id != format!("{}:RemoveReplicaIntent", intent.attempt_id)
            || intent.attempt == 0
            || intent.attempt > MAX_REMOVE_REPLICA_PRE_COMMIT_ATTEMPTS
        {
            return Err("frozen remove action identity or attempt is invalid".to_string());
        }
        core_intent(operation)?;
    }
    if let Some(pending) = &operation.pending_action {
        let intent = operation
            .remove_intent
            .as_ref()
            .ok_or_else(|| "pending remove action has no frozen intent".to_string())?;
        if pending.kind != DurableActionKind::RemoveReplicaIntent
            || pending.action_id != intent.action_id
            || pending.target_id != operation.old_primary_id
            || pending.target_instance_id != intent.primary_instance_id
            || pending.desired_postcondition.kind
                != DurablePostconditionKind::RemoveReplicaCoordinated
        {
            return Err("pending coarse remove action is not deterministic".to_string());
        }
    }
    if operation.removal_disposition.is_some() && operation.phase != DurableOperationPhase::Poisoned
    {
        return Err("remove disposition is not pinned in Poisoned".to_string());
    }
    Ok(())
}

fn validate_snapshot(snapshot: &StablePartitionSnapshotStatus) -> Result<(), String> {
    if snapshot.members.is_empty() {
        return Err("stable snapshot has no members".to_string());
    }
    let mut ids = HashSet::new();
    let mut instances = HashSet::new();
    let mut primaries = 0;
    for member in &snapshot.members {
        if member.id <= 0
            || !ids.insert(member.id)
            || member.instance_id.is_empty()
            || !instances.insert(member.instance_id.clone())
        {
            return Err("stable snapshot contains invalid or duplicate identity".to_string());
        }
        if member.role == StableReplicaRoleStatus::Primary {
            primaries += 1;
            if member.id != snapshot.primary_id {
                return Err("stable snapshot primary role conflicts with primary ID".to_string());
            }
        }
    }
    if primaries != 1 || snapshot.write_quorum != snapshot.members.len() as u32 / 2 + 1 {
        return Err("stable snapshot primary or quorum is invalid".to_string());
    }
    Ok(())
}

fn validate_primary_identity(
    operation: &DurableOperationStatus,
    primary: &super::ReplicaObservation,
) -> Result<(), String> {
    let member = operation
        .previous_snapshot
        .members
        .iter()
        .find(|member| member.id == operation.old_primary_id)
        .ok_or_else(|| "stable primary member is missing".to_string())?;
    if primary.status.instance_id.as_str() != member.instance_id
        || primary.status.role != Role::Primary
        || primary.status.epoch != snapshot_epoch(&operation.previous_snapshot)
        || primary.status.write_status != AccessStatus::Granted
        || !primary.status.healthy
    {
        return Err(
            "stable primary is not the exact writable runtime at the frozen epoch".to_string(),
        );
    }
    Ok(())
}

fn validate_scale_down_target(
    operation: &DurableOperationStatus,
    target: &super::ReplicaObservation,
) -> Result<(), String> {
    let observed_generation = target.status.agent.generation.to_string();
    if operation.remove_target_agent_generation.as_deref() != Some(observed_generation.as_str()) {
        return Err("ScaleDown target generation changed after preadmission".to_string());
    }
    if target.status.instance_id.as_str() != target_instance_id(operation)?
        || target.status.agent.lifecycle_peer_protocol_version
            != REPLICA_LIFECYCLE_PEER_PROTOCOL_VERSION
        || !target.status.healthy
    {
        return Err("ScaleDown target lacks exact lifecycle-peer-v2 capability".to_string());
    }
    Ok(())
}

fn validate_optional_force_target(
    operation: &DurableOperationStatus,
    target: &super::ReplicaObservation,
) -> Result<(), String> {
    if target.status.instance_id.as_str() != target_instance_id(operation)? {
        return Err("Force target observation belongs to another incarnation".to_string());
    }
    Ok(())
}

fn classify_configuration(
    operation: &DurableOperationStatus,
    status: &kuberic_core::types::ReplicaStatusInfo,
) -> Result<ConfigurationObservation, String> {
    let configuration = status
        .configuration
        .as_ref()
        .ok_or_else(|| "primary runtime exposes no exact configuration".to_string())?;
    let previous = configuration_status(
        &operation.previous_snapshot,
        ReplicaConfigurationMode::Current,
    );
    let reduced_catch_up = configuration_status(
        &operation.target_snapshot,
        ReplicaConfigurationMode::CatchUp,
    );
    let reduced_current = configuration_status(
        &operation.target_snapshot,
        ReplicaConfigurationMode::Current,
    );
    if configuration == &previous {
        Ok(ConfigurationObservation::PreviousCurrent)
    } else if configuration == &reduced_catch_up {
        Ok(ConfigurationObservation::ReducedCatchUp)
    } else if configuration == &reduced_current {
        Ok(ConfigurationObservation::ReducedCurrent)
    } else {
        Err(
            "primary configuration is not previous, reduced CatchUp, or reduced Current"
                .to_string(),
        )
    }
}

fn configuration_status(
    snapshot: &StablePartitionSnapshotStatus,
    mode: ReplicaConfigurationMode,
) -> ReplicaConfigurationStatus {
    let mut members = snapshot
        .members
        .iter()
        .filter(|member| member.id != snapshot.primary_id)
        .map(
            |member| kuberic_core::types::ReplicaConfigurationMemberStatus {
                id: member.id,
                instance_id: ReplicaInstanceId::new(member.instance_id.clone()),
                role: Role::ActiveSecondary,
            },
        )
        .collect::<Vec<_>>();
    members.sort_by_key(|member| member.id);
    ReplicaConfigurationStatus {
        mode,
        members,
        write_quorum: snapshot.write_quorum,
    }
}

fn exact_connection_absent(
    operation: &DurableOperationStatus,
    status: &kuberic_core::types::ReplicaStatusInfo,
) -> bool {
    !status.active_replica_connections.iter().any(|connection| {
        connection.id == operation.target_replica_id.unwrap_or_default()
            && Some(connection.instance_id.as_str()) == operation.target_instance_id.as_deref()
    })
}

fn precommit_failure_is_proven(intent: &RemoveReplicaIntentStatus, now: i64) -> bool {
    !intent.current_install_dispatched
        && intent
            .compensation_expiry_unix_seconds
            .unwrap_or(intent.overall_deadline_unix_seconds)
            <= now
}

fn terminal_retirement(
    observation: TargetRetirementObservation,
) -> Option<TargetRetirementObservation> {
    matches!(
        observation,
        TargetRetirementObservation::Completed
            | TargetRetirementObservation::Unavailable
            | TargetRetirementObservation::Stale
            | TargetRetirementObservation::Failed
    )
    .then_some(observation)
}

fn merge_retirement_evidence(
    current: &mut Option<TargetRetirementObservationStatus>,
    observed: Option<TargetRetirementObservationStatus>,
) {
    if *current == Some(TargetRetirementObservationStatus::Completed) {
        return;
    }
    if let Some(observed) = observed {
        *current = Some(observed);
    }
}

fn failed_precommit(operation: &DurableOperationStatus, reason: &str) -> DurableOperationStatus {
    let mut next = operation.clone();
    let intent = next.remove_intent.as_ref();
    next.removal_disposition = Some(RemoveReplicaDispositionStatus::FailedPreCommitIncomplete {
        attempt: intent.map_or(0, |intent| intent.attempt),
        last_observed_phase: intent.and_then(|intent| intent.last_observed_phase),
        reason: bounded_error(reason),
    });
    poison(&next, reason)
}

fn invalid_removal(operation: &DurableOperationStatus, reason: &str) -> DurableOperationStatus {
    let mut next = operation.clone();
    let intent = next.remove_intent.as_ref();
    next.removal_disposition = Some(RemoveReplicaDispositionStatus::InvalidRemovalState {
        attempt: intent.map_or(0, |intent| intent.attempt),
        last_observed_phase: intent.and_then(|intent| intent.last_observed_phase),
        previous_configuration_signature: intent
            .map(|intent| descriptor_signature(&intent.previous_configuration))
            .unwrap_or_else(|| "unfrozen".to_string()),
        reduced_configuration_signature: intent
            .map(|intent| descriptor_signature(&intent.reduced_current_configuration))
            .unwrap_or_else(|| "unfrozen".to_string()),
        reason: bounded_error(reason),
    });
    poison(&next, reason)
}

fn ambiguous_restart(
    operation: &DurableOperationStatus,
    new_generation: &AgentGeneration,
    reason: &str,
) -> DurableOperationStatus {
    let mut next = operation.clone();
    let intent = next.remove_intent.as_ref().unwrap();
    next.removal_disposition = Some(RemoveReplicaDispositionStatus::AmbiguousPrimaryRestart {
        old_generation: intent.primary_agent_generation.clone(),
        new_generation: new_generation.to_string(),
        last_observed_phase: intent.last_observed_phase,
        previous_configuration_signature: descriptor_signature(&intent.previous_configuration),
        reduced_configuration_signature: descriptor_signature(
            &intent.reduced_current_configuration,
        ),
        missing_evidence_reason: bounded_error(reason),
    });
    poison(&next, reason)
}

fn descriptor_signature(descriptor: &ConfigurationDescriptorStatus) -> String {
    core_descriptor(descriptor)
        .map(|descriptor| descriptor.signature())
        .unwrap_or_else(|_| "invalid".to_string())
}

fn remove_mode(operation: &DurableOperationStatus) -> Result<RemoveReplicaMode, String> {
    match operation.remove_mode {
        Some(DurableRemoveMode::ScaleDown) => Ok(RemoveReplicaMode::ScaleDown),
        Some(DurableRemoveMode::Force) => Ok(RemoveReplicaMode::Force),
        None => Err("remove operation has no mode".to_string()),
    }
}

fn target_replica_id(operation: &DurableOperationStatus) -> Result<i64, String> {
    operation
        .target_replica_id
        .ok_or_else(|| "remove operation has no target replica ID".to_string())
}

fn target_instance_id(operation: &DurableOperationStatus) -> Result<&str, String> {
    operation
        .target_instance_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "remove operation has no target incarnation".to_string())
}

fn target_pod_name(operation: &DurableOperationStatus) -> Result<&str, String> {
    operation
        .target_pod_name
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "remove operation has no target pod name".to_string())
}

fn target_pod_uid(operation: &DurableOperationStatus) -> Result<&str, String> {
    operation
        .target_pod_uid
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "remove operation has no target pod UID".to_string())
}

fn snapshot_epoch(snapshot: &StablePartitionSnapshotStatus) -> Epoch {
    Epoch::new(
        snapshot.epoch.data_loss_number,
        snapshot.epoch.configuration_number,
    )
}

impl From<RemoveReplicaCoordinatorPhase> for RemoveReplicaCoordinatorPhaseStatus {
    fn from(phase: RemoveReplicaCoordinatorPhase) -> Self {
        match phase {
            RemoveReplicaCoordinatorPhase::Validating => Self::Validating,
            RemoveReplicaCoordinatorPhase::InstallingCatchUpConfiguration => {
                Self::InstallingCatchUpConfiguration
            }
            RemoveReplicaCoordinatorPhase::WaitingForCatchUpQuorum => Self::WaitingForCatchUpQuorum,
            RemoveReplicaCoordinatorPhase::InstallingCurrentConfiguration => {
                Self::InstallingCurrentConfiguration
            }
            RemoveReplicaCoordinatorPhase::RemovingConnection => Self::RemovingConnection,
            RemoveReplicaCoordinatorPhase::RetiringTarget => Self::RetiringTarget,
            RemoveReplicaCoordinatorPhase::Attesting => Self::Attesting,
            RemoveReplicaCoordinatorPhase::Compensating => Self::Compensating,
        }
    }
}

impl From<RemoveReplicaTerminalResult> for RemoveReplicaTerminalResultStatus {
    fn from(result: RemoveReplicaTerminalResult) -> Self {
        match result {
            RemoveReplicaTerminalResult::CommittedClean => Self::CommittedClean,
            RemoveReplicaTerminalResult::CommittedDegraded => Self::CommittedDegraded,
            RemoveReplicaTerminalResult::Compensated => Self::Compensated,
            RemoveReplicaTerminalResult::CompensationIncomplete => Self::CompensationIncomplete,
        }
    }
}

impl From<TargetRetirementObservation> for TargetRetirementObservationStatus {
    fn from(observation: TargetRetirementObservation) -> Self {
        match observation {
            TargetRetirementObservation::NotAttempted => Self::NotAttempted,
            TargetRetirementObservation::InProgress => Self::InProgress,
            TargetRetirementObservation::Completed => Self::Completed,
            TargetRetirementObservation::Unavailable => Self::Unavailable,
            TargetRetirementObservation::Stale => Self::Stale,
            TargetRetirementObservation::Failed => Self::Failed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        EpochStatus, OptionalStablePartitionSnapshotStatus, StableReplicaSnapshotStatus,
    };
    use crate::durable::ReplicaObservation;
    use kuberic_core::remove_replica::RemoveReplicaProgress;
    use kuberic_core::types::{
        CorrelatedActionObservation, DurableActionObservation, ReplicaAgentStatus,
        ReplicaConnectionStatus, ReplicaStatusInfo,
    };

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

    fn target(id: i64, instance: &str) -> RemoveReplicaTarget {
        RemoveReplicaTarget {
            replica_id: id,
            pod_name: format!("set-{}", id - 1),
            pod_uid: instance.to_string(),
            replicator_address: format!("http://set-{}:9091", id - 1),
            agent_generation: Some(
                AgentGeneration::parse(format!("{id:032x}"))
                    .expect("valid deterministic generation"),
            ),
        }
    }

    fn observation(
        id: i64,
        instance: &str,
        role: Role,
        configuration: Option<ReplicaConfigurationStatus>,
    ) -> ReplicaObservation {
        let generation =
            AgentGeneration::parse(format!("{id:032x}")).expect("valid deterministic generation");
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
                    generation,
                    control_version: AgentControlVersion::default(),
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
        let previous = configuration_status(
            &operation.previous_snapshot,
            ReplicaConfigurationMode::Current,
        );
        let mut observations = OperationObservations::new();
        observations.insert(1, observation(1, "one", Role::Primary, Some(previous)));
        observations.insert(2, observation(2, "two", Role::ActiveSecondary, None));
        observations.insert(3, observation(3, "three", Role::ActiveSecondary, None));
        observations
    }

    fn freeze_and_dispatch(
        mode: DurableRemoveMode,
    ) -> (DurableOperationStatus, OperationObservations) {
        let operation =
            start_remove_replica("set", snapshot(), target(3, "three"), mode, 2, 10).unwrap();
        let observations = observations(&operation);
        let Decision::Persist(frozen) = decide_remove_replica(
            &operation,
            &observations,
            &OperationPodIdentities::new(),
            None,
            10,
        )
        .unwrap() else {
            panic!("expected frozen remove intent");
        };
        let Decision::Persist(dispatched) = decide_remove_replica(
            &frozen,
            &observations,
            &OperationPodIdentities::new(),
            None,
            10,
        )
        .unwrap() else {
            panic!("expected persisted coarse dispatch");
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
        primary.status.agent.current_action = Some(CorrelatedActionObservation {
            generation: primary.status.agent.generation.clone(),
            control_version: AgentControlVersion::new(1),
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

    fn committed_operation(connection_absent: bool) -> DurableOperationStatus {
        let (operation, _) = freeze_and_dispatch(DurableRemoveMode::Force);
        let generation = AgentGeneration::parse(
            operation
                .remove_intent
                .as_ref()
                .unwrap()
                .primary_agent_generation
                .clone(),
        )
        .unwrap();
        let Decision::RecordCommitEvidence(committed) =
            record_observed_commit(&operation, &generation, 100, connection_absent).unwrap()
        else {
            panic!("expected committed removal checkpoint");
        };
        committed
    }

    #[test]
    fn starts_coarse_remove_v2_without_a_fine_grained_cursor() {
        let operation = start_remove_replica(
            "set",
            snapshot(),
            target(3, "three"),
            DurableRemoveMode::ScaleDown,
            2,
            10,
        )
        .unwrap();
        assert_eq!(operation.version, REMOVE_REPLICA_OPERATION_VERSION);
        assert_eq!(operation.phase, DurableOperationPhase::RemoveFreezeIntent);
        assert!(operation.pending_action.is_none());
        assert!(operation.remove_intent.is_none());
    }

    #[test]
    fn rejects_primary_unknown_duplicate_and_below_minimum_targets_without_operation() {
        for (target, expected) in [
            (target(1, "one"), "cannot remove the primary"),
            (target(4, "four"), "is not committed"),
        ] {
            let error =
                start_remove_replica("set", snapshot(), target, DurableRemoveMode::Force, 2, 10)
                    .unwrap_err();
            assert!(error.contains(expected), "{error}");
        }

        let error = start_remove_replica(
            "set",
            snapshot(),
            target(3, "three"),
            DurableRemoveMode::ScaleDown,
            3,
            10,
        )
        .unwrap_err();
        assert!(error.contains("below minReplicas"));

        let mut duplicate = snapshot();
        duplicate.members[2].id = 2;
        assert!(
            start_remove_replica(
                "set",
                duplicate,
                target(3, "three"),
                DurableRemoveMode::Force,
                2,
                10,
            )
            .unwrap_err()
            .contains("duplicate")
        );
    }

    #[test]
    fn remove_v1_is_rejected_without_compatibility() {
        let mut operation = start_remove_replica(
            "set",
            snapshot(),
            target(3, "three"),
            DurableRemoveMode::Force,
            2,
            10,
        )
        .unwrap();
        operation.version = 1;
        assert!(
            decide_remove_replica(
                &operation,
                &OperationObservations::new(),
                &OperationPodIdentities::new(),
                None,
                10,
            )
            .unwrap_err()
            .contains("unsupported")
        );
    }

    #[test]
    fn commit_evidence_without_a_frozen_intent_is_rejected_before_postcommit_cleanup() {
        let mut operation = start_remove_replica(
            "set",
            snapshot(),
            target(3, "three"),
            DurableRemoveMode::Force,
            2,
            10,
        )
        .unwrap();
        operation.phase = DurableOperationPhase::RemoveAwaitCleanup;
        operation.committed_snapshot = Some(operation.target_snapshot.clone());
        operation.remove_commit_evidence = Some(RemoveReplicaCommitEvidenceStatus {
            attempt_id: "attempt".to_string(),
            action_id: "action".to_string(),
            primary_agent_generation: "11111111111111111111111111111111".to_string(),
            configuration_signature: "configuration".to_string(),
            observed_unix_seconds: 20,
        });

        let error = decide_remove_replica(
            &operation,
            &OperationObservations::new(),
            &OperationPodIdentities::new(),
            None,
            20,
        )
        .unwrap_err();
        assert!(
            error.contains("commit evidence has no frozen intent"),
            "{error}"
        );
    }

    #[test]
    fn scale_down_freeze_rejects_target_generation_drift_after_preadmission() {
        let operation = start_remove_replica(
            "set",
            snapshot(),
            target(3, "three"),
            DurableRemoveMode::ScaleDown,
            2,
            10,
        )
        .unwrap();
        let mut observations = observations(&operation);
        observations.get_mut(&3).unwrap().status.agent.generation =
            AgentGeneration::parse("ffffffffffffffffffffffffffffffff").unwrap();

        let error = decide_remove_replica(
            &operation,
            &observations,
            &OperationPodIdentities::new(),
            None,
            10,
        )
        .unwrap_err();
        assert!(
            error.contains("generation changed after preadmission"),
            "{error}"
        );
        assert!(operation.remove_intent.is_none());
    }

    #[test]
    fn freezes_and_dispatches_one_deterministic_coarse_intent() {
        let (operation, observations) = freeze_and_dispatch(DurableRemoveMode::ScaleDown);
        let intent = operation.remove_intent.as_ref().unwrap();
        assert_eq!(intent.attempt, 1);
        assert_eq!(
            intent.target_lifecycle_peer_protocol_version,
            Some(REPLICA_LIFECYCLE_PEER_PROTOCOL_VERSION)
        );
        assert_eq!(
            operation.pending_action.as_ref().unwrap().kind,
            DurableActionKind::RemoveReplicaIntent
        );
        let Decision::Execute {
            target_id,
            action_id,
            action,
        } = decide_remove_replica(
            &operation,
            &observations,
            &OperationPodIdentities::new(),
            None,
            10,
        )
        .unwrap()
        else {
            panic!("expected one coarse remove dispatch");
        };
        assert_eq!(target_id, 1);
        assert_eq!(action_id, intent.action_id);
        assert!(matches!(
            action,
            DurableReplicaAction::RemoveReplicaIntent { .. }
        ));
    }

    #[test]
    fn exact_commit_evidence_is_recorded_without_publishing_stable_topology() {
        let (operation, mut observations) = freeze_and_dispatch(DurableRemoveMode::ScaleDown);
        let intent = operation.remove_intent.as_ref().unwrap().clone();
        let core = core_intent(&operation).unwrap();
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
        primary.status.configuration = Some(core.reduced_current_status());
        primary.status.active_replica_connections.clear();
        primary.status.agent.generation =
            AgentGeneration::parse(intent.primary_agent_generation.clone()).unwrap();
        primary.status.agent.current_action = Some(CorrelatedActionObservation {
            generation: primary.status.agent.generation.clone(),
            control_version: AgentControlVersion::new(1),
            action: DurableActionObservation {
                action_id: intent.action_id.clone(),
                signature: intent.input_signature.clone(),
                state: DurableActionState::Completed,
                error_class: None,
                error: None,
                result: Some(DurableActionResult::RemoveReplica(
                    RemoveReplicaTerminalResult::CommittedClean,
                )),
                add_replica_progress: None,
                remove_replica_progress: Some(progress),
            },
        });
        let Decision::Persist(observed) = decide_remove_replica(
            &operation,
            &observations,
            &OperationPodIdentities::new(),
            None,
            500,
        )
        .unwrap() else {
            panic!("expected durable observed coordinator progress");
        };
        let Decision::RecordCommitEvidence(committed) = decide_remove_replica(
            &observed,
            &observations,
            &OperationPodIdentities::new(),
            None,
            500,
        )
        .unwrap() else {
            panic!("expected removal-specific commit evidence");
        };
        assert_eq!(
            committed
                .remove_commit_evidence
                .as_ref()
                .unwrap()
                .observed_unix_seconds,
            123,
            "the primary's commit observation timestamp must be persisted verbatim"
        );
        assert_eq!(
            committed.committed_snapshot.as_ref(),
            Some(&committed.target_snapshot)
        );
        assert_ne!(
            committed.previous_snapshot.members,
            committed.target_snapshot.members
        );
        assert_eq!(committed.phase, DurableOperationPhase::RemoveRecordCommit);
    }

    #[test]
    fn compensated_previous_current_finalizes_failed_with_previous_snapshot() {
        let (operation, mut observations) = freeze_and_dispatch(DurableRemoveMode::ScaleDown);
        let intent = operation.remove_intent.as_ref().unwrap().clone();
        let progress = RemoveReplicaProgress {
            phase: RemoveReplicaCoordinatorPhase::Compensating,
            attempt_id: intent.attempt_id,
            commit_observed: false,
            commit_observed_unix_seconds: None,
            connection_absent: false,
            target_retirement: TargetRetirementObservation::NotAttempted,
            retirement_expiry_unix_seconds: None,
            compensation_expiry_unix_seconds: Some(50),
            error: Some("pre-commit quorum was lost".to_string()),
            current_install_dispatched: false,
        };
        set_remove_action_progress(
            &operation,
            &mut observations,
            progress,
            DurableActionState::Completed,
            Some(RemoveReplicaTerminalResult::Compensated),
        );

        let Decision::Persist(observed) = decide_remove_replica(
            &operation,
            &observations,
            &OperationPodIdentities::new(),
            None,
            20,
        )
        .unwrap() else {
            panic!("expected compensated progress to be persisted");
        };
        assert!(observed.committed_snapshot.is_none());

        let Decision::Persist(finalizing) = decide_remove_replica(
            &observed,
            &observations,
            &OperationPodIdentities::new(),
            None,
            20,
        )
        .unwrap() else {
            panic!("expected compensated removal finalization");
        };
        assert_eq!(
            finalizing.phase,
            DurableOperationPhase::RemoveCompensateFinalize
        );
        assert!(finalizing.committed_snapshot.is_none());

        let Decision::Complete {
            operation: failed,
            snapshot,
            compensated,
        } = decide_remove_replica(
            &finalizing,
            &observations,
            &OperationPodIdentities::new(),
            None,
            20,
        )
        .unwrap()
        else {
            panic!("expected compensated removal completion");
        };
        assert!(compensated);
        assert_eq!(failed.phase, DurableOperationPhase::Failed);
        assert!(failed.committed_snapshot.is_none());
        assert_eq!(snapshot, operation.previous_snapshot.cloned().unwrap());
    }

    #[test]
    fn malformed_compensated_terminal_after_dispatch_is_poisoned_defense_in_depth() {
        let (operation, mut observations) = freeze_and_dispatch(DurableRemoveMode::ScaleDown);
        let intent = operation.remove_intent.as_ref().unwrap().clone();
        let progress = RemoveReplicaProgress {
            phase: RemoveReplicaCoordinatorPhase::Compensating,
            attempt_id: intent.attempt_id,
            commit_observed: false,
            commit_observed_unix_seconds: None,
            connection_absent: false,
            target_retirement: TargetRetirementObservation::NotAttempted,
            retirement_expiry_unix_seconds: None,
            compensation_expiry_unix_seconds: Some(50),
            error: Some("ambiguous current installation".to_string()),
            current_install_dispatched: true,
        };
        set_remove_action_progress(
            &operation,
            &mut observations,
            progress,
            DurableActionState::Completed,
            Some(RemoveReplicaTerminalResult::Compensated),
        );

        let Decision::Persist(observed) = decide_remove_replica(
            &operation,
            &observations,
            &OperationPodIdentities::new(),
            None,
            20,
        )
        .unwrap() else {
            panic!("expected current-install evidence to be persisted");
        };
        let Decision::Persist(poisoned) = decide_remove_replica(
            &observed,
            &observations,
            &OperationPodIdentities::new(),
            None,
            20,
        )
        .unwrap() else {
            panic!("expected invalid compensation to fail closed");
        };
        assert_eq!(poisoned.phase, DurableOperationPhase::Poisoned);
        assert!(matches!(
            poisoned.removal_disposition,
            Some(RemoveReplicaDispositionStatus::InvalidRemovalState { .. })
        ));
        assert!(poisoned.committed_snapshot.is_none());
    }

    #[test]
    fn failed_agent_after_lost_current_response_and_previous_current_is_poisoned() {
        let (operation, mut observations) = freeze_and_dispatch(DurableRemoveMode::ScaleDown);
        let intent = operation.remove_intent.as_ref().unwrap().clone();
        let progress = RemoveReplicaProgress {
            phase: RemoveReplicaCoordinatorPhase::InstallingCurrentConfiguration,
            attempt_id: intent.attempt_id,
            commit_observed: false,
            commit_observed_unix_seconds: None,
            connection_absent: false,
            target_retirement: TargetRetirementObservation::NotAttempted,
            retirement_expiry_unix_seconds: None,
            compensation_expiry_unix_seconds: None,
            error: None,
            current_install_dispatched: true,
        };
        set_remove_action_progress(
            &operation,
            &mut observations,
            progress,
            DurableActionState::Failed,
            None,
        );

        let Decision::Persist(observed) = decide_remove_replica(
            &operation,
            &observations,
            &OperationPodIdentities::new(),
            None,
            20,
        )
        .unwrap() else {
            panic!("expected current-install dispatch evidence to be persisted");
        };
        assert!(
            observed
                .remove_intent
                .as_ref()
                .unwrap()
                .current_install_dispatched
        );

        let Decision::Persist(poisoned) = decide_remove_replica(
            &observed,
            &observations,
            &OperationPodIdentities::new(),
            None,
            20,
        )
        .unwrap() else {
            panic!("expected the ambiguous agent failure to poison removal");
        };
        assert_eq!(poisoned.phase, DurableOperationPhase::Poisoned);
        assert!(matches!(
            poisoned.removal_disposition,
            Some(RemoveReplicaDispositionStatus::InvalidRemovalState { .. })
        ));
        assert!(poisoned.committed_snapshot.is_none());
    }

    #[test]
    fn persisted_current_install_dispatch_evidence_cannot_regress() {
        let (mut operation, mut observations) = freeze_and_dispatch(DurableRemoveMode::Force);
        operation
            .remove_intent
            .as_mut()
            .unwrap()
            .current_install_dispatched = true;
        let intent = operation.remove_intent.as_ref().unwrap().clone();
        let progress = RemoveReplicaProgress {
            phase: RemoveReplicaCoordinatorPhase::Compensating,
            attempt_id: intent.attempt_id,
            commit_observed: false,
            commit_observed_unix_seconds: None,
            connection_absent: false,
            target_retirement: TargetRetirementObservation::NotAttempted,
            retirement_expiry_unix_seconds: None,
            compensation_expiry_unix_seconds: Some(50),
            error: Some("malformed regressed progress".to_string()),
            current_install_dispatched: false,
        };
        set_remove_action_progress(
            &operation,
            &mut observations,
            progress,
            DurableActionState::Failed,
            None,
        );

        let Decision::Persist(poisoned) = decide_remove_replica(
            &operation,
            &observations,
            &OperationPodIdentities::new(),
            None,
            20,
        )
        .unwrap() else {
            panic!("expected regressed dispatch evidence to poison removal");
        };
        assert_eq!(poisoned.phase, DurableOperationPhase::Poisoned);
        assert!(matches!(
            poisoned.removal_disposition,
            Some(RemoveReplicaDispositionStatus::InvalidRemovalState { .. })
        ));
    }

    #[test]
    fn exact_reduced_current_recognizes_commit_without_action_history() {
        let (operation, mut observations) = freeze_and_dispatch(DurableRemoveMode::Force);
        let core = core_intent(&operation).unwrap();
        let primary = observations.get_mut(&operation.old_primary_id).unwrap();
        primary.status.configuration = Some(core.reduced_current_status());
        primary.status.active_replica_connections.clear();
        primary.status.agent.current_action = None;
        primary.status.agent.retained_terminal_actions.clear();
        primary.status.agent.generation =
            AgentGeneration::parse("ffffffffffffffffffffffffffffffff").unwrap();

        let Decision::RecordCommitEvidence(committed) = decide_remove_replica(
            &operation,
            &observations,
            &OperationPodIdentities::from([(1, "one".to_string())]),
            None,
            101,
        )
        .unwrap() else {
            panic!("exact reduced Current must be recognized as committed");
        };
        assert_eq!(
            committed
                .remove_commit_evidence
                .as_ref()
                .unwrap()
                .primary_agent_generation,
            "ffffffffffffffffffffffffffffffff"
        );
        assert_eq!(
            committed
                .remove_commit_evidence
                .as_ref()
                .unwrap()
                .observed_unix_seconds,
            101
        );
        assert!(committed.remove_cleanup.as_ref().unwrap().connection_absent);
        let pod_identities = OperationPodIdentities::from([
            (1, "one".to_string()),
            (2, "two".to_string()),
            (3, "three".to_string()),
        ]);
        let Decision::Persist(await_cleanup) = decide_remove_replica(
            &committed,
            &observations,
            &pod_identities,
            Some("secondary"),
            101,
        )
        .unwrap() else {
            panic!("commit evidence must advance to cleanup");
        };
        assert!(matches!(
            decide_remove_replica(
                &await_cleanup,
                &observations,
                &pod_identities,
                Some("secondary"),
                101,
            )
            .unwrap(),
            Decision::Wait
        ));
    }

    #[test]
    fn new_primary_generation_redrives_from_live_previous_and_catch_up_postconditions() {
        for configuration in [
            ReplicaConfigurationMode::Current,
            ReplicaConfigurationMode::CatchUp,
        ] {
            let (operation, mut observations) = freeze_and_dispatch(DurableRemoveMode::Force);
            let core = core_intent(&operation).unwrap();
            let primary = observations.get_mut(&operation.old_primary_id).unwrap();
            primary.status.agent.generation =
                AgentGeneration::parse("ffffffffffffffffffffffffffffffff").unwrap();
            primary.status.agent.current_action = None;
            primary.status.agent.retained_terminal_actions.clear();
            primary.status.configuration = Some(match configuration {
                ReplicaConfigurationMode::Current => core.previous_status(),
                ReplicaConfigurationMode::CatchUp => core.reduced_catch_up_status(),
            });
            let identities = OperationPodIdentities::from([
                (1, "one".to_string()),
                (2, "two".to_string()),
                (3, "three".to_string()),
            ]);

            let Decision::Persist(redrive) =
                decide_remove_replica(&operation, &observations, &identities, None, 20).unwrap()
            else {
                panic!("live new-generation postcondition must redrive");
            };
            assert_eq!(redrive.phase, DurableOperationPhase::RemoveFreezeIntent);
            assert!(redrive.removal_disposition.is_none());
            let Decision::Persist(refrozen) =
                decide_remove_replica(&redrive, &observations, &identities, None, 20).unwrap()
            else {
                panic!("redrive must freeze a new generation-qualified attempt");
            };
            assert_eq!(refrozen.remove_intent.as_ref().unwrap().attempt, 2);
            assert_eq!(
                refrozen
                    .remove_intent
                    .as_ref()
                    .unwrap()
                    .primary_agent_generation,
                "ffffffffffffffffffffffffffffffff"
            );
        }
    }

    #[test]
    fn compensation_expiry_is_persisted_once_and_cannot_change() {
        let (operation, mut observations) = freeze_and_dispatch(DurableRemoveMode::Force);
        let intent = operation.remove_intent.as_ref().unwrap();
        let progress = RemoveReplicaProgress {
            phase: RemoveReplicaCoordinatorPhase::Compensating,
            attempt_id: intent.attempt_id.clone(),
            commit_observed: false,
            commit_observed_unix_seconds: None,
            connection_absent: false,
            target_retirement: TargetRetirementObservation::NotAttempted,
            retirement_expiry_unix_seconds: None,
            compensation_expiry_unix_seconds: Some(30),
            error: Some("compensating".to_string()),
            current_install_dispatched: false,
        };
        set_remove_action_progress(
            &operation,
            &mut observations,
            progress.clone(),
            DurableActionState::InProgress,
            None,
        );
        let Decision::Persist(persisted) = decide_remove_replica(
            &operation,
            &observations,
            &OperationPodIdentities::new(),
            None,
            20,
        )
        .unwrap() else {
            panic!("first compensation expiry must be persisted");
        };
        assert_eq!(
            persisted
                .remove_intent
                .as_ref()
                .unwrap()
                .compensation_expiry_unix_seconds,
            Some(30)
        );
        assert!(matches!(
            decide_remove_replica(
                &persisted,
                &observations,
                &OperationPodIdentities::new(),
                None,
                20,
            )
            .unwrap(),
            Decision::Wait
        ));

        let mut changed = progress;
        changed.compensation_expiry_unix_seconds = Some(31);
        set_remove_action_progress(
            &persisted,
            &mut observations,
            changed,
            DurableActionState::InProgress,
            None,
        );
        let Decision::Persist(poisoned) = decide_remove_replica(
            &persisted,
            &observations,
            &OperationPodIdentities::new(),
            None,
            21,
        )
        .unwrap() else {
            panic!("changed compensation expiry must fail closed");
        };
        assert_eq!(poisoned.phase, DurableOperationPhase::Poisoned);
        assert!(matches!(
            poisoned.removal_disposition,
            Some(RemoveReplicaDispositionStatus::InvalidRemovalState { .. })
        ));
    }

    #[test]
    fn commit_and_publication_resource_version_conflict_replay_has_no_duplicate_or_premature_effect()
     {
        let (operation, mut observations) = freeze_and_dispatch(DurableRemoveMode::Force);
        let core = core_intent(&operation).unwrap();
        let primary = observations.get_mut(&operation.old_primary_id).unwrap();
        primary.status.configuration = Some(core.reduced_current_status());
        primary.status.active_replica_connections.clear();
        primary.status.agent.current_action = None;
        primary.status.agent.retained_terminal_actions.clear();

        let first = decide_remove_replica(
            &operation,
            &observations,
            &OperationPodIdentities::new(),
            None,
            100,
        )
        .unwrap();
        let replay = decide_remove_replica(
            &operation,
            &observations,
            &OperationPodIdentities::new(),
            None,
            100,
        )
        .unwrap();
        let (Decision::RecordCommitEvidence(first), Decision::RecordCommitEvidence(replay)) =
            (first, replay)
        else {
            panic!("a rejected commit-evidence status write must replay only that checkpoint");
        };
        assert_eq!(first, replay);
        assert_eq!(first.phase, DurableOperationPhase::RemoveRecordCommit);
        assert_eq!(
            first.committed_snapshot,
            Some(first.target_snapshot.clone())
        );

        let mut publishing = first;
        publishing.phase = DurableOperationPhase::RemovePublishTopology;
        publishing.remove_cleanup = Some(RemoveReplicaCleanupStatus {
            connection_absent: true,
            target_retirement: Some(TargetRetirementObservationStatus::Completed),
            target_labels_fenced: true,
            target_pod_deleted: true,
        });
        for decision in [
            decide_remove_replica(
                &publishing,
                &OperationObservations::new(),
                &OperationPodIdentities::new(),
                None,
                100,
            )
            .unwrap(),
            decide_remove_replica(
                &publishing,
                &OperationObservations::new(),
                &OperationPodIdentities::new(),
                None,
                100,
            )
            .unwrap(),
        ] {
            let Decision::CommitSnapshot {
                operation,
                snapshot,
            } = decision
            else {
                panic!("a rejected publication status write must replay only publication");
            };
            assert_eq!(operation.phase, DurableOperationPhase::RemoveFinalize);
            assert_eq!(snapshot, publishing.target_snapshot);
        }
    }

    #[test]
    fn already_absent_or_replacement_pod_never_receives_old_incarnation_cleanup() {
        for pod_identities in [
            OperationPodIdentities::new(),
            OperationPodIdentities::from([(3, "replacement-three".to_string())]),
        ] {
            let mut operation = committed_operation(true);
            operation.phase = DurableOperationPhase::RemoveAwaitCleanup;
            operation.remove_cleanup.as_mut().unwrap().target_retirement =
                Some(TargetRetirementObservationStatus::Completed);

            let Decision::Persist(labels_fenced) = decide_remove_replica(
                &operation,
                &OperationObservations::new(),
                &pod_identities,
                Some("secondary"),
                100,
            )
            .unwrap() else {
                panic!("absent exact target must be durably label-fenced without mutation");
            };
            assert!(
                labels_fenced
                    .remove_cleanup
                    .as_ref()
                    .unwrap()
                    .target_labels_fenced
            );

            let Decision::Persist(delete_phase) = decide_remove_replica(
                &labels_fenced,
                &OperationObservations::new(),
                &pod_identities,
                Some("secondary"),
                100,
            )
            .unwrap() else {
                panic!("cleanup must advance without patching a replacement");
            };
            assert_eq!(
                delete_phase.phase,
                DurableOperationPhase::RemoveDeleteTargetPod
            );

            let Decision::Persist(publication) = decide_remove_replica(
                &delete_phase,
                &OperationObservations::new(),
                &pod_identities,
                Some("secondary"),
                100,
            )
            .unwrap() else {
                panic!("cleanup must advance without deleting a replacement");
            };
            assert_eq!(
                publication.phase,
                DurableOperationPhase::RemovePublishTopology
            );
            assert!(
                publication
                    .remove_cleanup
                    .as_ref()
                    .unwrap()
                    .target_pod_deleted
            );
        }
    }

    #[test]
    fn missing_exact_primary_status_never_proves_connection_absence() {
        let mut operation = committed_operation(false);
        operation.phase = DurableOperationPhase::RemoveAwaitCleanup;
        let exact_primary_pod = OperationPodIdentities::from([
            (1, "one".to_string()),
            (2, "two".to_string()),
            (3, "three".to_string()),
        ]);

        assert!(matches!(
            decide_remove_replica(
                &operation,
                &OperationObservations::new(),
                &exact_primary_pod,
                Some("secondary"),
                10_000,
            )
            .unwrap(),
            Decision::Wait
        ));
        assert!(!operation.remove_cleanup.as_ref().unwrap().connection_absent);

        let replaced_primary = OperationPodIdentities::from([(1, "replacement-one".to_string())]);
        let Decision::Persist(replaced) = decide_remove_replica(
            &operation,
            &OperationObservations::new(),
            &replaced_primary,
            None,
            10_000,
        )
        .unwrap() else {
            panic!("primary process replacement must prove old process-local connection absence");
        };
        assert!(replaced.remove_cleanup.as_ref().unwrap().connection_absent);
    }

    #[test]
    fn completed_retirement_evidence_never_regresses() {
        let mut operation = committed_operation(true);
        operation.phase = DurableOperationPhase::RemoveAwaitCleanup;
        operation.remove_cleanup.as_mut().unwrap().target_retirement =
            Some(TargetRetirementObservationStatus::Completed);
        let Decision::Persist(next) = decide_remove_replica(
            &operation,
            &OperationObservations::new(),
            &OperationPodIdentities::new(),
            None,
            1_000,
        )
        .unwrap() else {
            panic!("missing target should advance cleanup");
        };
        assert_eq!(
            next.remove_cleanup.as_ref().unwrap().target_retirement,
            Some(TargetRetirementObservationStatus::Completed)
        );
    }

    #[test]
    fn retirement_expiry_backstop_records_unavailable_evidence() {
        let mut operation = committed_operation(true);
        operation.phase = DurableOperationPhase::RemoveAwaitCleanup;
        let core = core_intent(&operation).unwrap();
        let mut observations = observations(&operation);
        let primary = observations.get_mut(&operation.old_primary_id).unwrap();
        primary.status.configuration = Some(core.reduced_current_status());
        primary.status.active_replica_connections.clear();
        primary.status.agent.current_action = None;
        primary.status.agent.retained_terminal_actions.clear();

        assert!(matches!(
            decide_remove_replica(
                &operation,
                &observations,
                &OperationPodIdentities::new(),
                None,
                159,
            )
            .unwrap(),
            Decision::Wait
        ));

        let Decision::Persist(expired) = decide_remove_replica(
            &operation,
            &observations,
            &OperationPodIdentities::new(),
            None,
            160,
        )
        .unwrap() else {
            panic!("retirement expiry must persist a terminal backstop");
        };
        assert_eq!(
            expired.remove_cleanup.as_ref().unwrap().target_retirement,
            Some(TargetRetirementObservationStatus::Unavailable)
        );
        assert!(
            expired
                .remove_cleanup
                .as_ref()
                .unwrap()
                .target_labels_fenced
        );
    }

    #[test]
    fn restart_and_incomplete_states_use_distinct_typed_dispositions() {
        let (operation, mut observations) = freeze_and_dispatch(DurableRemoveMode::Force);
        let intent = operation.remove_intent.as_ref().unwrap().clone();
        let primary = observations.get_mut(&1).unwrap();
        primary.status.agent.generation =
            AgentGeneration::parse("ffffffffffffffffffffffffffffffff").unwrap();
        primary.status.role = Role::Unknown;
        primary.status.healthy = false;
        primary.status.configuration = None;
        let pod_identities = OperationPodIdentities::from([
            (1, "one".to_string()),
            (2, "two".to_string()),
            (3, "three".to_string()),
        ]);
        let Decision::Persist(ambiguous) =
            decide_remove_replica(&operation, &observations, &pod_identities, None, 20).unwrap()
        else {
            panic!("expected ambiguous primary restart poison");
        };
        assert!(matches!(
            ambiguous.removal_disposition,
            Some(RemoveReplicaDispositionStatus::AmbiguousPrimaryRestart { .. })
        ));

        let mut invalid = operation.clone();
        invalid
            .remove_intent
            .as_mut()
            .unwrap()
            .current_install_dispatched = true;
        let primary = observations.get_mut(&1).unwrap();
        primary.status.agent.generation =
            AgentGeneration::parse(intent.primary_agent_generation).unwrap();
        primary.status.role = Role::Primary;
        primary.status.healthy = true;
        primary.status.configuration = None;
        let Decision::Persist(invalid) =
            decide_remove_replica(&invalid, &observations, &pod_identities, None, 20).unwrap()
        else {
            panic!("expected invalid removal state poison");
        };
        assert!(matches!(
            invalid.removal_disposition,
            Some(RemoveReplicaDispositionStatus::InvalidRemovalState { .. })
        ));
    }

    #[test]
    fn new_generation_previous_current_after_dispatch_is_invalid() {
        let (mut operation, mut observations) = freeze_and_dispatch(DurableRemoveMode::Force);
        operation
            .remove_intent
            .as_mut()
            .unwrap()
            .current_install_dispatched = true;
        let primary = observations.get_mut(&operation.old_primary_id).unwrap();
        primary.status.agent.generation =
            AgentGeneration::parse("ffffffffffffffffffffffffffffffff").unwrap();
        let pod_identities = OperationPodIdentities::from([(1, "one".to_string())]);

        let Decision::Persist(ambiguous) =
            decide_remove_replica(&operation, &observations, &pod_identities, None, 20).unwrap()
        else {
            panic!("expected post-dispatch generation change to fail closed");
        };
        assert_eq!(ambiguous.phase, DurableOperationPhase::Poisoned);
        assert!(matches!(
            ambiguous.removal_disposition,
            Some(RemoveReplicaDispositionStatus::InvalidRemovalState { .. })
        ));
    }

    #[test]
    fn durable_ambiguous_primary_restart_is_a_no_op() {
        let (operation, mut observations) = freeze_and_dispatch(DurableRemoveMode::Force);
        let primary = observations.get_mut(&operation.old_primary_id).unwrap();
        primary.status.agent.generation =
            AgentGeneration::parse("ffffffffffffffffffffffffffffffff").unwrap();
        primary.status.role = Role::Unknown;
        primary.status.healthy = false;
        primary.status.configuration = None;
        let pod_identities = OperationPodIdentities::from([(1, "one".to_string())]);
        let Decision::Persist(ambiguous) =
            decide_remove_replica(&operation, &observations, &pod_identities, None, 20).unwrap()
        else {
            panic!("expected durable ambiguous-restart checkpoint");
        };
        assert!(matches!(
            ambiguous.removal_disposition,
            Some(RemoveReplicaDispositionStatus::AmbiguousPrimaryRestart { .. })
        ));
        assert!(matches!(
            decide_remove_replica(
                &ambiguous,
                &OperationObservations::new(),
                &OperationPodIdentities::new(),
                None,
                21,
            )
            .unwrap(),
            Decision::Wait
        ));
    }

    #[test]
    fn exhausted_known_catch_up_state_is_failed_precommit_incomplete() {
        let (mut operation, mut observations) = freeze_and_dispatch(DurableRemoveMode::Force);
        let mut core = core_intent(&operation).unwrap();
        core.attempt_number = MAX_REMOVE_REPLICA_PRE_COMMIT_ATTEMPTS;
        core.attempt_id = format!("{}:attempt-3", core.operation_id);
        core.action_id = format!("{}:RemoveReplicaIntent", core.attempt_id);
        core.input_signature = core.signature();
        {
            let intent = operation.remove_intent.as_mut().unwrap();
            intent.attempt = core.attempt_number;
            intent.attempt_id = core.attempt_id.clone();
            intent.action_id = core.action_id.clone();
            intent.input_signature = core.input_signature.clone();
        }
        {
            let pending = operation.pending_action.as_mut().unwrap();
            pending.sequence = core.attempt_number;
            pending.action_id = core.action_id.clone();
        }
        let progress = RemoveReplicaProgress {
            phase: RemoveReplicaCoordinatorPhase::Compensating,
            attempt_id: core.attempt_id.clone(),
            commit_observed: false,
            commit_observed_unix_seconds: None,
            connection_absent: false,
            target_retirement: TargetRetirementObservation::NotAttempted,
            retirement_expiry_unix_seconds: None,
            compensation_expiry_unix_seconds: Some(30),
            error: Some("restoration incomplete".to_string()),
            current_install_dispatched: false,
        };
        let primary = observations.get_mut(&1).unwrap();
        primary.status.configuration = Some(core.reduced_catch_up_status());
        primary.status.agent.generation = core.primary_agent_generation.clone();
        primary.status.agent.current_action = Some(CorrelatedActionObservation {
            generation: core.primary_agent_generation.clone(),
            control_version: AgentControlVersion::new(1),
            action: DurableActionObservation {
                action_id: core.action_id.clone(),
                signature: core.input_signature.clone(),
                state: DurableActionState::Completed,
                error_class: None,
                error: None,
                result: Some(DurableActionResult::RemoveReplica(
                    RemoveReplicaTerminalResult::CompensationIncomplete,
                )),
                add_replica_progress: None,
                remove_replica_progress: Some(progress),
            },
        });
        let Decision::Persist(observed) = decide_remove_replica(
            &operation,
            &observations,
            &OperationPodIdentities::new(),
            None,
            30,
        )
        .unwrap() else {
            panic!("expected durable compensation evidence");
        };
        let Decision::Persist(poisoned) = decide_remove_replica(
            &observed,
            &observations,
            &OperationPodIdentities::new(),
            None,
            30,
        )
        .unwrap() else {
            panic!("expected failed-precommit poison");
        };
        assert_eq!(poisoned.phase, DurableOperationPhase::Poisoned);
        assert!(matches!(
            poisoned.removal_disposition,
            Some(RemoveReplicaDispositionStatus::FailedPreCommitIncomplete {
                attempt: MAX_REMOVE_REPLICA_PRE_COMMIT_ATTEMPTS,
                ..
            })
        ));
        assert!(poisoned.committed_snapshot.is_none());
    }

    #[test]
    fn poison_helper_preserves_typed_disposition() {
        let operation = start_remove_replica(
            "set",
            snapshot(),
            target(3, "three"),
            DurableRemoveMode::Force,
            2,
            10,
        )
        .unwrap();
        let poisoned = invalid_removal(&operation, "impossible");
        assert_eq!(poisoned.phase, DurableOperationPhase::Poisoned);
        assert!(matches!(
            poisoned.removal_disposition,
            Some(RemoveReplicaDispositionStatus::InvalidRemovalState { .. })
        ));
    }

    #[test]
    fn optional_snapshot_wrapper_remains_usable() {
        assert!(OptionalStablePartitionSnapshotStatus(Some(snapshot())).is_some());
    }

    #[test]
    fn compensate_finalize_condition_describes_safe_finalization() {
        let mut operation = start_remove_replica(
            "set",
            snapshot(),
            target(3, "three"),
            DurableRemoveMode::Force,
            2,
            10,
        )
        .unwrap();
        operation.phase = DurableOperationPhase::RemoveCompensateFinalize;
        operation.last_error = Some("previous Current configuration is restored".to_string());
        let condition = crate::durable::operation_condition(&operation, 20);
        assert_eq!(condition.status, "True");
        assert_eq!(condition.reason, "FinalizingCompensatedRemoval");
        assert_eq!(
            condition.message,
            "previous Current configuration is restored"
        );
    }
}
