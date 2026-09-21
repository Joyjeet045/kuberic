use std::collections::HashSet;

use kuberic_core::types::{
    DurableActionState, DurableReplicaAction, Epoch, OpenMode, ReplicaConfigurationMode,
    ReplicaConfigurationStatus, ReplicaInfo, ReplicaInstanceId, ReplicaSetConfig,
    ReplicaSetQuorumMode, ReplicaStatus, Role,
};

use crate::crd::{
    DURABLE_OPERATION_VERSION, DurableActionKind, DurableOperationKind, DurableOperationPhase,
    DurableOperationStatus, DurablePostconditionKind, DurablePostconditionStatus, EpochStatus,
    PendingActionStatus, StablePartitionSnapshotStatus, StableReplicaRoleStatus,
    StableReplicaSnapshotStatus,
};

use super::{
    ACTION_DEADLINE_SECONDS, Decision, OperationObservations, OperationPodIdentities,
    bounded_error, poison, record_activity_error,
};

const BUILD_DEADLINE_SECONDS: i64 = 600;

#[derive(Debug, Clone)]
pub struct CreatePartitionTarget {
    pub replica_id: i64,
    pub instance_id: String,
    pub pod_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ActionObservation {
    Precondition,
    InProgress,
    Postcondition,
    Failed(String),
    Unavailable,
    Impossible,
}

pub fn start_create_partition(
    set_uid: &str,
    targets: Vec<CreatePartitionTarget>,
    committed_snapshot: Option<StablePartitionSnapshotStatus>,
    min_replicas: usize,
    now: i64,
) -> Result<DurableOperationStatus, String> {
    start_create_partition_with_primary(
        set_uid,
        targets,
        committed_snapshot,
        min_replicas,
        now,
        None,
    )
}

/// Freeze the initial primary choice; an already committed prefix always wins.
pub fn start_create_partition_with_primary(
    set_uid: &str,
    mut targets: Vec<CreatePartitionTarget>,
    committed_snapshot: Option<StablePartitionSnapshotStatus>,
    min_replicas: usize,
    now: i64,
    preferred_primary_id: Option<i64>,
) -> Result<DurableOperationStatus, String> {
    if targets.is_empty() {
        return Err("partition creation has no replica targets".to_string());
    }
    if min_replicas == 0 || min_replicas > targets.len() {
        return Err(format!(
            "minReplicas must be between 1 and the desired replica count ({})",
            targets.len()
        ));
    }
    targets.sort_by_key(|target| target.replica_id);
    let mut ids = HashSet::new();
    let mut instances = HashSet::new();
    for target in &targets {
        if target.replica_id <= 0
            || target.instance_id.is_empty()
            || target.pod_name.is_empty()
            || !ids.insert(target.replica_id)
            || !instances.insert(target.instance_id.as_str())
        {
            return Err("creation targets have invalid or duplicate identity".to_string());
        }
    }

    if preferred_primary_id.is_some_and(|id| !ids.contains(&id)) {
        return Err("preferred creation primary is absent from the exact targets".to_string());
    }
    let primary_id = committed_snapshot
        .as_ref()
        .map(|snapshot| snapshot.primary_id)
        .or(preferred_primary_id)
        .unwrap_or(targets[0].replica_id);
    // Preserve committed ordering, then append new targets deterministically.
    // The protocol opens/commits member zero before constructing secondaries.
    targets.sort_by_key(|target| {
        let committed_index = committed_snapshot.as_ref().and_then(|snapshot| {
            snapshot
                .members
                .iter()
                .position(|member| member.id == target.replica_id)
        });
        (
            target.replica_id != primary_id,
            committed_index.unwrap_or(usize::MAX),
            target.replica_id,
        )
    });
    let epoch = committed_snapshot
        .as_ref()
        .map(|snapshot| snapshot.epoch.clone())
        .unwrap_or(EpochStatus {
            data_loss_number: 0,
            configuration_number: 1,
        });
    let target_snapshot = StablePartitionSnapshotStatus {
        epoch,
        primary_id,
        members: targets
            .iter()
            .map(|target| StableReplicaSnapshotStatus {
                id: target.replica_id,
                instance_id: target.instance_id.clone(),
                role: if target.replica_id == primary_id {
                    StableReplicaRoleStatus::Primary
                } else {
                    StableReplicaRoleStatus::ActiveSecondary
                },
                election_metadata: committed_snapshot.as_ref().and_then(|snapshot| {
                    snapshot
                        .members
                        .iter()
                        .find(|member| {
                            member.id == target.replica_id
                                && member.instance_id == target.instance_id
                        })
                        .and_then(|member| member.election_metadata.clone())
                }),
            })
            .collect(),
        write_quorum: majority(targets.len()),
    };
    validate_snapshot(&target_snapshot)?;
    if let Some(committed) = &committed_snapshot {
        validate_committed_prefix(committed, &target_snapshot)?;
    }

    let phase = DurableOperationPhase::CreateFenceRouting;
    let current = &targets[0];
    let identities = targets
        .iter()
        .map(|target| format!("{}@{}", target.replica_id, target.instance_id))
        .collect::<Vec<_>>()
        .join(",");
    let operation_id = format!(
        "{set_uid}:create-partition:v{DURABLE_OPERATION_VERSION}:{}:{identities}",
        target_snapshot.epoch.configuration_number
    );

    Ok(DurableOperationStatus {
        execution_id: format!("{operation_id}:execution-1"),
        operation_id,
        version: DURABLE_OPERATION_VERSION,
        kind: DurableOperationKind::CreatePartition,
        phase,
        previous_snapshot: None.into(),
        target_snapshot,
        committed_snapshot,
        minimum_committed_replicas: Some(min_replicas as u32),
        old_primary_id: primary_id,
        target_primary_id: primary_id,
        add_mode: None,
        remove_mode: None,
        target_replica_id: Some(current.replica_id),
        target_instance_id: Some(current.instance_id.clone()),
        target_pod_name: Some(current.pod_name.clone()),
        target_pod_uid: Some(current.instance_id.clone()),
        remove_target_replicator_address: None,
        remove_target_agent_generation: None,
        retired_instance_id: None,
        frozen_lsn: None,
        next_secondary_index: 0,
        phase_deadline_unix_seconds: now + ACTION_DEADLINE_SECONDS,
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

pub fn decide_create_partition(
    operation: &DurableOperationStatus,
    observations: &OperationObservations,
    pod_identities: &OperationPodIdentities,
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
    if let Some(pending) = &operation.pending_action {
        validate_pending_action(operation, pending)?;
    }
    if let Err(error) = validate_committed_observations(operation, observations) {
        return Ok(Decision::Persist(poison(operation, &error)));
    }
    if current_candidate_changed(operation, observations) {
        return Ok(failure_decision(
            operation,
            "creation candidate incarnation changed",
            now,
        ));
    }
    if let Some(pending) = &operation.pending_action {
        return decide_pending(operation, pending, observations, pod_identities, now);
    }

    Ok(match operation.phase {
        DurableOperationPhase::CreateFenceRouting => {
            let index = operation.next_secondary_index as usize;
            operation
                .target_snapshot
                .members
                .get(index)
                .ok_or_else(|| "creation fencing index is out of range".to_string())?;
            persist_pending(
                operation,
                pending_action(
                    operation,
                    500 + index as u32,
                    DurableActionKind::CreateFencePod,
                    now,
                )?,
            )
        }
        DurableOperationPhase::CreateOpenPrimary => persist_pending(
            operation,
            pending_action(operation, 1, DurableActionKind::CreateOpenPrimary, now)?,
        ),
        DurableOperationPhase::CreatePromotePrimary => persist_pending(
            operation,
            pending_action(operation, 2, DurableActionKind::CreatePromotePrimary, now)?,
        ),
        DurableOperationPhase::CreatePrimaryCurrentConfiguration => persist_pending(
            operation,
            pending_action(
                operation,
                3,
                DurableActionKind::CreatePrimaryCurrentConfiguration,
                now,
            )?,
        ),
        DurableOperationPhase::CreateCommitPrimary => {
            let primary_snapshot = snapshot_prefix(&operation.target_snapshot, 0);
            let primary = observations
                .get(&operation.target_snapshot.primary_id)
                .ok_or_else(|| "creation primary is unavailable".to_string())?;
            if primary.status.configuration.as_ref()
                != Some(&configuration_status_for_snapshot(
                    &primary_snapshot,
                    ReplicaConfigurationMode::Current,
                ))
            {
                Decision::Persist(poison(
                    operation,
                    "primary-only current configuration is not committed",
                ))
            } else {
                advance_after_commit(operation, primary_snapshot, observations, now)?
            }
        }
        DurableOperationPhase::CreateOpenSecondary => persist_pending(
            operation,
            pending_action(
                operation,
                secondary_sequence(operation, 0)?,
                DurableActionKind::CreateOpenSecondary,
                now,
            )?,
        ),
        DurableOperationPhase::CreateUpdateSecondaryEpoch => persist_pending(
            operation,
            pending_action(
                operation,
                secondary_sequence(operation, 1)?,
                DurableActionKind::CreateUpdateSecondaryEpoch,
                now,
            )?,
        ),
        DurableOperationPhase::CreateAssignSecondaryIdle => persist_pending(
            operation,
            pending_action(
                operation,
                secondary_sequence(operation, 2)?,
                DurableActionKind::CreateAssignSecondaryIdle,
                now,
            )?,
        ),
        DurableOperationPhase::CreateBuildSecondary => persist_pending(
            operation,
            pending_action(
                operation,
                secondary_sequence(operation, 3)?,
                DurableActionKind::CreateBuildSecondary,
                now,
            )?,
        ),
        DurableOperationPhase::CreateAssignSecondaryActive => persist_pending(
            operation,
            pending_action(
                operation,
                secondary_sequence(operation, 4)?,
                DurableActionKind::CreateAssignSecondaryActive,
                now,
            )?,
        ),
        DurableOperationPhase::CreateCatchUpConfiguration => persist_pending(
            operation,
            pending_action(
                operation,
                secondary_sequence(operation, 5)?,
                DurableActionKind::CreateCatchUpConfiguration,
                now,
            )?,
        ),
        DurableOperationPhase::CreateWaitForCatchUpQuorum => persist_pending(
            operation,
            pending_action(
                operation,
                secondary_sequence(operation, 6)?,
                DurableActionKind::CreateWaitForCatchUpQuorum,
                now,
            )?,
        ),
        DurableOperationPhase::CreateCurrentConfiguration => persist_pending(
            operation,
            pending_action(
                operation,
                secondary_sequence(operation, 7)?,
                DurableActionKind::CreateCurrentConfiguration,
                now,
            )?,
        ),
        DurableOperationPhase::CreateCommitSecondary => {
            let expanded = attempted_snapshot(operation)?;
            let primary = observations
                .get(&operation.target_snapshot.primary_id)
                .ok_or_else(|| "creation primary is unavailable".to_string())?;
            if primary.status.configuration.as_ref()
                != Some(&configuration_status_for_snapshot(
                    &expanded,
                    ReplicaConfigurationMode::Current,
                ))
            {
                Decision::Persist(poison(
                    operation,
                    "expanded current configuration is not committed",
                ))
            } else {
                advance_after_commit(operation, expanded, observations, now)?
            }
        }
        DurableOperationPhase::CreatePublishRouting => {
            let index = operation.next_secondary_index as usize;
            let member = operation
                .target_snapshot
                .members
                .get(index)
                .ok_or_else(|| "creation routing index is out of range".to_string())?;
            let kind = if member.id == operation.target_snapshot.primary_id {
                DurableActionKind::CreatePublishPrimary
            } else {
                DurableActionKind::CreatePublishSecondary
            };
            persist_pending(
                operation,
                pending_action(operation, 1000 + index as u32, kind, now)?,
            )
        }
        DurableOperationPhase::CreateFinalize => {
            validate_final_observations(operation, observations)?;
            let mut completed = operation.clone();
            completed.phase = DurableOperationPhase::Completed;
            completed.pending_action = None;
            Decision::Complete {
                operation: completed,
                snapshot: operation.target_snapshot.clone(),
                compensated: false,
            }
        }
        DurableOperationPhase::CreateCompensateRestoreConfiguration => persist_pending(
            operation,
            pending_action(
                operation,
                2000,
                DurableActionKind::CreateCompensateRestoreConfiguration,
                now,
            )?,
        ),
        DurableOperationPhase::CreateCompensateRemoveCandidate => {
            if operation.committed_snapshot.is_none() {
                Decision::Persist(with_phase(
                    operation,
                    DurableOperationPhase::CreateCompensateDemoteCandidate,
                    now,
                ))
            } else {
                persist_pending(
                    operation,
                    pending_action(
                        operation,
                        2001,
                        DurableActionKind::CreateCompensateRemoveCandidate,
                        now,
                    )?,
                )
            }
        }
        DurableOperationPhase::CreateCompensateDemoteCandidate => persist_pending(
            operation,
            pending_action(
                operation,
                2002,
                DurableActionKind::CreateCompensateDemoteCandidate,
                now,
            )?,
        ),
        DurableOperationPhase::CreateCompensateCloseCandidate => persist_pending(
            operation,
            pending_action(
                operation,
                2003,
                DurableActionKind::CreateCompensateCloseCandidate,
                now,
            )?,
        ),
        DurableOperationPhase::CreateCompensateDeleteCandidate => persist_pending(
            operation,
            pending_action(
                operation,
                2004,
                DurableActionKind::CreateCompensateDeleteCandidate,
                now,
            )?,
        ),
        DurableOperationPhase::CreateCompensateFinalize => {
            let mut failed = operation.clone();
            failed.phase = DurableOperationPhase::Failed;
            failed.pending_action = None;
            Decision::RestartCreation { operation: failed }
        }
        _ => {
            return Err(format!(
                "phase {:?} is not valid for partition creation",
                operation.phase
            ));
        }
    })
}

fn decide_pending(
    operation: &DurableOperationStatus,
    pending: &PendingActionStatus,
    observations: &OperationObservations,
    pod_identities: &OperationPodIdentities,
    now: i64,
) -> Result<Decision, String> {
    if pending.kind == DurableActionKind::CreateFencePod {
        return decide_fence_pending(operation, pending, observations, pod_identities, now);
    }
    match observe_action(operation, pending, observations)? {
        ActionObservation::Postcondition => {
            let mut next = operation.clone();
            next.pending_action = None;
            next.last_error = None;
            next.phase_deadline_unix_seconds = now + ACTION_DEADLINE_SECONDS;
            if matches!(
                pending.kind,
                DurableActionKind::CreatePublishPrimary | DurableActionKind::CreatePublishSecondary
            ) {
                let next_index = operation.next_secondary_index as usize + 1;
                if next_index == operation.target_snapshot.members.len() {
                    next.phase = DurableOperationPhase::CreateFinalize;
                } else {
                    next.next_secondary_index = next_index as u32;
                }
            } else {
                next.phase = next_phase(pending.kind);
            }
            Ok(Decision::Persist(next))
        }
        ActionObservation::InProgress => {
            if now >= pending.deadline_unix_seconds {
                Ok(failure_decision(
                    operation,
                    "durable creation activity remained in progress past its deadline",
                    now,
                ))
            } else {
                Ok(Decision::Wait)
            }
        }
        ActionObservation::Precondition => {
            if now >= pending.deadline_unix_seconds {
                return Ok(failure_decision(
                    operation,
                    &format!(
                        "durable creation action {:?} reached its deadline",
                        pending.kind
                    ),
                    now,
                ));
            }
            if matches!(
                pending.kind,
                DurableActionKind::CreatePublishPrimary | DurableActionKind::CreatePublishSecondary
            ) {
                return Ok(Decision::PatchPodRole {
                    target_id: pending.target_id,
                    role: if pending.kind == DurableActionKind::CreatePublishPrimary {
                        "primary".to_string()
                    } else {
                        "secondary".to_string()
                    },
                });
            }
            if pending.kind == DurableActionKind::CreateCompensateDeleteCandidate {
                return Ok(Decision::DeletePod {
                    pod_name: target_pod_name(operation)?.to_string(),
                    expected_uid: target_instance_id(operation)?.to_string(),
                });
            }
            Ok(Decision::Execute {
                target_id: pending.target_id,
                action_id: pending.action_id.clone(),
                action: action_for(operation, pending, observations)?,
            })
        }
        ActionObservation::Failed(error) => Ok(failure_decision(
            &record_activity_error(operation, &error),
            &error,
            now,
        )),
        ActionObservation::Unavailable => {
            if matches!(
                pending.kind,
                DurableActionKind::CreateCompensateDemoteCandidate
                    | DurableActionKind::CreateCompensateCloseCandidate
                    | DurableActionKind::CreateCompensateDeleteCandidate
            ) {
                let mut next = operation.clone();
                next.pending_action = None;
                next.phase = match pending.kind {
                    DurableActionKind::CreateCompensateDemoteCandidate => {
                        DurableOperationPhase::CreateCompensateCloseCandidate
                    }
                    DurableActionKind::CreateCompensateCloseCandidate => {
                        DurableOperationPhase::CreateCompensateDeleteCandidate
                    }
                    _ => DurableOperationPhase::CreateCompensateFinalize,
                };
                return Ok(Decision::Persist(next));
            }
            if now >= pending.deadline_unix_seconds {
                Ok(failure_decision(
                    operation,
                    "durable creation action target was unavailable at its deadline",
                    now,
                ))
            } else {
                Ok(Decision::Wait)
            }
        }
        ActionObservation::Impossible => Ok(Decision::Persist(poison(
            operation,
            "live state matches neither the creation action precondition nor postcondition",
        ))),
    }
}

fn decide_fence_pending(
    operation: &DurableOperationStatus,
    pending: &PendingActionStatus,
    observations: &OperationObservations,
    pod_identities: &OperationPodIdentities,
    now: i64,
) -> Result<Decision, String> {
    let member = fence_member(operation)?;
    if pending.target_id != member.id || pending.target_instance_id != member.instance_id {
        return Err("pending fence action does not target the current fence member".to_string());
    }

    match observations.get(&member.id) {
        Some(observation) if observation.status.instance_id.as_str() != member.instance_id => Ok(
            restart_or_poison_fence(operation, member.id, "fence member incarnation changed"),
        ),
        Some(observation) if observation.pod_role_label.as_deref() == Some("bootstrap") => {
            let mut next = operation.clone();
            next.pending_action = None;
            next.last_error = None;
            next.phase_deadline_unix_seconds = now + ACTION_DEADLINE_SECONDS;
            let next_index = operation.next_secondary_index as usize + 1;
            if next_index == operation.target_snapshot.members.len() {
                advance_after_fencing(&mut next, observations)?;
            } else {
                next.next_secondary_index = next_index as u32;
            }
            Ok(Decision::Persist(next))
        }
        Some(_) if now >= pending.deadline_unix_seconds => Ok(Decision::Persist(poison(
            operation,
            "fence label did not reach the bootstrap postcondition before its deadline",
        ))),
        Some(_) => Ok(Decision::PatchPodRole {
            target_id: member.id,
            role: "bootstrap".to_string(),
        }),
        None if now < pending.deadline_unix_seconds => Ok(Decision::Wait),
        None => match pod_identities.get(&member.id) {
            Some(instance_id) if instance_id != &member.instance_id => Ok(restart_or_poison_fence(
                operation,
                member.id,
                "fence member incarnation changed while unavailable",
            )),
            Some(_) => Ok(Decision::Persist(poison(
                operation,
                "fence member runtime remained unavailable at its deadline",
            ))),
            None => Ok(restart_or_poison_fence(
                operation,
                member.id,
                "fence member pod disappeared",
            )),
        },
    }
}

fn restart_or_poison_fence(
    operation: &DurableOperationStatus,
    target_id: i64,
    error: &str,
) -> Decision {
    if is_committed_member(operation, target_id) {
        return Decision::Persist(poison(operation, error));
    }
    let mut failed = operation.clone();
    failed.phase = DurableOperationPhase::Failed;
    failed.pending_action = None;
    failed.last_error = Some(bounded_error(error));
    Decision::RestartCreation { operation: failed }
}

fn observe_action(
    operation: &DurableOperationStatus,
    pending: &PendingActionStatus,
    observations: &OperationObservations,
) -> Result<ActionObservation, String> {
    let observation = observations.get(&pending.target_id);
    if pending.kind == DurableActionKind::CreateCompensateDeleteCandidate {
        return Ok(match observation {
            None => ActionObservation::Postcondition,
            Some(observation)
                if observation.status.instance_id.as_str() == pending.target_instance_id =>
            {
                ActionObservation::Precondition
            }
            Some(_) => ActionObservation::Postcondition,
        });
    }
    let Some(observation) = observation else {
        return Ok(ActionObservation::Unavailable);
    };
    if observation.status.instance_id.as_str() != pending.target_instance_id {
        if matches!(
            pending.kind,
            DurableActionKind::CreateCompensateDemoteCandidate
                | DurableActionKind::CreateCompensateCloseCandidate
        ) {
            return Ok(ActionObservation::Postcondition);
        }
        return Ok(ActionObservation::Impossible);
    }

    let epoch = target_epoch(operation);
    let status = &observation.status;
    let direct = match pending.kind {
        DurableActionKind::CreateFencePod => {
            if observation.pod_role_label.as_deref() == Some("bootstrap") {
                ActionObservation::Postcondition
            } else {
                ActionObservation::Precondition
            }
        }
        DurableActionKind::CreateOpenPrimary | DurableActionKind::CreateOpenSecondary => {
            if status.healthy && status.role == Role::Unknown {
                ActionObservation::Postcondition
            } else if !status.healthy && status.role == Role::Unknown {
                ActionObservation::Precondition
            } else {
                ActionObservation::Impossible
            }
        }
        DurableActionKind::CreatePromotePrimary => {
            if status.healthy && status.role == Role::Primary && status.epoch == epoch {
                ActionObservation::Postcondition
            } else if status.healthy
                && status.role == Role::Unknown
                && status.epoch == Epoch::default()
            {
                ActionObservation::Precondition
            } else {
                ActionObservation::Impossible
            }
        }
        DurableActionKind::CreatePrimaryCurrentConfiguration => {
            let primary = snapshot_prefix(&operation.target_snapshot, 0);
            configuration_observation(
                status,
                &configuration_status_for_snapshot(&primary, ReplicaConfigurationMode::Current),
                None,
                epoch,
            )
        }
        DurableActionKind::CreateUpdateSecondaryEpoch => {
            if status.healthy && status.role == Role::Unknown && status.epoch == epoch {
                ActionObservation::Postcondition
            } else if status.healthy
                && status.role == Role::Unknown
                && status.epoch == Epoch::default()
            {
                ActionObservation::Precondition
            } else {
                ActionObservation::Impossible
            }
        }
        DurableActionKind::CreateAssignSecondaryIdle => {
            if status.role == Role::IdleSecondary && status.epoch == epoch {
                ActionObservation::Postcondition
            } else if status.role == Role::Unknown && status.epoch == epoch {
                ActionObservation::Precondition
            } else {
                ActionObservation::Impossible
            }
        }
        DurableActionKind::CreateBuildSecondary
        | DurableActionKind::CreateWaitForCatchUpQuorum
        | DurableActionKind::CreateCompensateRemoveCandidate => ActionObservation::Precondition,
        DurableActionKind::CreateAssignSecondaryActive => {
            if status.role == Role::ActiveSecondary && status.epoch == epoch {
                ActionObservation::Postcondition
            } else if status.role == Role::IdleSecondary && status.epoch == epoch {
                ActionObservation::Precondition
            } else {
                ActionObservation::Impossible
            }
        }
        DurableActionKind::CreateCatchUpConfiguration => {
            let committed = committed_snapshot(operation)?;
            let attempted = attempted_snapshot(operation)?;
            configuration_observation(
                status,
                &configuration_status_for_snapshot(&attempted, ReplicaConfigurationMode::CatchUp),
                Some(&configuration_status_for_snapshot(
                    committed,
                    ReplicaConfigurationMode::Current,
                )),
                epoch,
            )
        }
        DurableActionKind::CreateCurrentConfiguration => {
            let attempted = attempted_snapshot(operation)?;
            configuration_observation(
                status,
                &configuration_status_for_snapshot(&attempted, ReplicaConfigurationMode::Current),
                Some(&configuration_status_for_snapshot(
                    &attempted,
                    ReplicaConfigurationMode::CatchUp,
                )),
                epoch,
            )
        }
        DurableActionKind::CreatePublishPrimary | DurableActionKind::CreatePublishSecondary => {
            let expected = if pending.kind == DurableActionKind::CreatePublishPrimary {
                "primary"
            } else {
                "secondary"
            };
            if observation.pod_role_label.as_deref() == Some(expected) {
                ActionObservation::Postcondition
            } else {
                ActionObservation::Precondition
            }
        }
        DurableActionKind::CreateCompensateRestoreConfiguration => {
            let committed = committed_snapshot(operation)?;
            let attempted = attempted_snapshot(operation)?;
            configuration_observation(
                status,
                &configuration_status_for_snapshot(committed, ReplicaConfigurationMode::Current),
                Some(&configuration_status_for_snapshot(
                    &attempted,
                    ReplicaConfigurationMode::CatchUp,
                )),
                epoch,
            )
        }
        DurableActionKind::CreateCompensateDemoteCandidate => {
            if matches!(status.role, Role::None | Role::Unknown) {
                ActionObservation::Postcondition
            } else if status.epoch == epoch
                && matches!(
                    status.role,
                    Role::Primary | Role::IdleSecondary | Role::ActiveSecondary
                )
            {
                ActionObservation::Precondition
            } else {
                ActionObservation::Impossible
            }
        }
        DurableActionKind::CreateCompensateCloseCandidate => {
            if !status.healthy {
                ActionObservation::Postcondition
            } else if matches!(status.role, Role::None | Role::Unknown) {
                ActionObservation::Precondition
            } else {
                ActionObservation::Impossible
            }
        }
        DurableActionKind::CreateCompensateDeleteCandidate => unreachable!(),
        _ => return Err("non-creation action reached creation observation".to_string()),
    };
    if direct == ActionObservation::Postcondition {
        return Ok(direct);
    }

    if let Ok(expected_action) = action_for(operation, pending, observations) {
        let expected_signature = super::pending_action_signature(pending, &expected_action)?;
        if let Some(active) = super::correlated_action_observation(status, &pending.action_id) {
            if active.signature != expected_signature {
                return Ok(ActionObservation::Impossible);
            }
            return Ok(match active.state {
                DurableActionState::Scheduled | DurableActionState::InProgress => {
                    ActionObservation::InProgress
                }
                DurableActionState::Completed => ActionObservation::Postcondition,
                DurableActionState::Failed => ActionObservation::Failed(
                    active
                        .error
                        .clone()
                        .unwrap_or_else(|| "runtime reported durable activity failure".to_string()),
                ),
            });
        }
    }
    Ok(direct)
}

fn action_for(
    operation: &DurableOperationStatus,
    pending: &PendingActionStatus,
    observations: &OperationObservations,
) -> Result<DurableReplicaAction, String> {
    let epoch = target_epoch(operation);
    Ok(match pending.kind {
        DurableActionKind::CreateOpenPrimary | DurableActionKind::CreateOpenSecondary => {
            DurableReplicaAction::Open {
                mode: OpenMode::New,
            }
        }
        DurableActionKind::CreatePromotePrimary => DurableReplicaAction::ChangeRole {
            epoch,
            role: Role::Primary,
        },
        DurableActionKind::CreatePrimaryCurrentConfiguration => {
            DurableReplicaAction::UpdateCurrentConfiguration {
                current: config_for_snapshot(
                    &snapshot_prefix(&operation.target_snapshot, 0),
                    observations,
                    false,
                    None,
                )?,
            }
        }
        DurableActionKind::CreateUpdateSecondaryEpoch => {
            DurableReplicaAction::UpdateEpoch { epoch }
        }
        DurableActionKind::CreateAssignSecondaryIdle => DurableReplicaAction::ChangeRole {
            epoch,
            role: Role::IdleSecondary,
        },
        DurableActionKind::CreateBuildSecondary => DurableReplicaAction::BuildReplica {
            replica: candidate_info(operation, observations, Role::IdleSecondary)?,
        },
        DurableActionKind::CreateAssignSecondaryActive => DurableReplicaAction::ChangeRole {
            epoch,
            role: Role::ActiveSecondary,
        },
        DurableActionKind::CreateCatchUpConfiguration => {
            let committed = committed_snapshot(operation)?;
            let attempted = attempted_snapshot(operation)?;
            DurableReplicaAction::UpdateCatchUpConfiguration {
                current: config_for_snapshot(
                    &attempted,
                    observations,
                    true,
                    operation.target_replica_id,
                )?,
                previous: config_for_snapshot(committed, observations, false, None)?,
            }
        }
        DurableActionKind::CreateWaitForCatchUpQuorum => {
            DurableReplicaAction::WaitForCatchUpQuorum {
                mode: ReplicaSetQuorumMode::Write,
            }
        }
        DurableActionKind::CreateCurrentConfiguration => {
            DurableReplicaAction::UpdateCurrentConfiguration {
                current: config_for_snapshot(
                    &attempted_snapshot(operation)?,
                    observations,
                    false,
                    None,
                )?,
            }
        }
        DurableActionKind::CreateCompensateRestoreConfiguration => {
            DurableReplicaAction::UpdateCurrentConfiguration {
                current: config_for_snapshot(
                    committed_snapshot(operation)?,
                    observations,
                    false,
                    None,
                )?,
            }
        }
        DurableActionKind::CreateCompensateRemoveCandidate => DurableReplicaAction::RemoveReplica {
            replica_id: target_replica_id(operation)?,
            instance_id: ReplicaInstanceId::new(target_instance_id(operation)?.to_string()),
        },
        DurableActionKind::CreateCompensateDemoteCandidate => DurableReplicaAction::ChangeRole {
            epoch,
            role: Role::None,
        },
        DurableActionKind::CreateCompensateCloseCandidate => DurableReplicaAction::Close,
        DurableActionKind::CreateFencePod
        | DurableActionKind::CreatePublishPrimary
        | DurableActionKind::CreatePublishSecondary
        | DurableActionKind::CreateCompensateDeleteCandidate => {
            return Err("cluster action does not map to a runtime action".to_string());
        }
        _ => return Err("non-creation action reached creation action construction".to_string()),
    })
}

fn pending_action(
    operation: &DurableOperationStatus,
    sequence: u32,
    kind: DurableActionKind,
    now: i64,
) -> Result<PendingActionStatus, String> {
    if is_compensation_action(kind) {
        ensure_compensation_target_uncommitted(operation)?;
    }
    let primary_id = operation.target_snapshot.primary_id;
    let current_id = target_replica_id(operation)?;
    let runtime_target_id = match kind {
        DurableActionKind::CreateBuildSecondary
        | DurableActionKind::CreateCatchUpConfiguration
        | DurableActionKind::CreateWaitForCatchUpQuorum
        | DurableActionKind::CreateCurrentConfiguration
        | DurableActionKind::CreatePrimaryCurrentConfiguration
        | DurableActionKind::CreateCompensateRestoreConfiguration
        | DurableActionKind::CreateCompensateRemoveCandidate => primary_id,
        DurableActionKind::CreateFencePod
        | DurableActionKind::CreatePublishPrimary
        | DurableActionKind::CreatePublishSecondary => {
            operation
                .target_snapshot
                .members
                .get(operation.next_secondary_index as usize)
                .ok_or_else(|| "creation routing index is out of range".to_string())?
                .id
        }
        _ => current_id,
    };
    let runtime_instance_id = operation
        .target_snapshot
        .members
        .iter()
        .find(|member| member.id == runtime_target_id)
        .map(|member| member.instance_id.clone())
        .ok_or_else(|| "creation action target is absent from target snapshot".to_string())?;
    let postcondition = match kind {
        DurableActionKind::CreateFencePod => DurablePostconditionKind::PodRoleLabel,
        DurableActionKind::CreateOpenPrimary | DurableActionKind::CreateOpenSecondary => {
            DurablePostconditionKind::Opened
        }
        DurableActionKind::CreatePromotePrimary
        | DurableActionKind::CreateAssignSecondaryIdle
        | DurableActionKind::CreateAssignSecondaryActive
        | DurableActionKind::CreateCompensateDemoteCandidate => DurablePostconditionKind::Role,
        DurableActionKind::CreatePrimaryCurrentConfiguration
        | DurableActionKind::CreateCurrentConfiguration
        | DurableActionKind::CreateCompensateRestoreConfiguration => {
            DurablePostconditionKind::CurrentConfiguration
        }
        DurableActionKind::CreateUpdateSecondaryEpoch => DurablePostconditionKind::Epoch,
        DurableActionKind::CreateBuildSecondary => DurablePostconditionKind::BuildCompleted,
        DurableActionKind::CreateCatchUpConfiguration => {
            DurablePostconditionKind::CatchUpConfiguration
        }
        DurableActionKind::CreateWaitForCatchUpQuorum => DurablePostconditionKind::CatchUpQuorum,
        DurableActionKind::CreatePublishPrimary | DurableActionKind::CreatePublishSecondary => {
            DurablePostconditionKind::PodRoleLabel
        }
        DurableActionKind::CreateCompensateRemoveCandidate => {
            DurablePostconditionKind::ReplicaRemoved
        }
        DurableActionKind::CreateCompensateCloseCandidate => DurablePostconditionKind::Closed,
        DurableActionKind::CreateCompensateDeleteCandidate => DurablePostconditionKind::PodDeleted,
        _ => return Err("non-creation action cannot be scheduled".to_string()),
    };
    let role = match kind {
        DurableActionKind::CreatePublishPrimary => Some(StableReplicaRoleStatus::Primary),
        DurableActionKind::CreatePublishSecondary => Some(StableReplicaRoleStatus::ActiveSecondary),
        _ => None,
    };
    Ok(PendingActionStatus {
        action_id: format!("{}:{sequence}:{kind:?}", operation.execution_id),
        sequence,
        kind,
        target_id: runtime_target_id,
        target_instance_id: runtime_instance_id,
        expected_epoch: operation.target_snapshot.epoch.clone(),
        desired_postcondition: DurablePostconditionStatus {
            kind: postcondition,
            role,
        },
        attempts: 0,
        deadline_unix_seconds: now
            + if kind == DurableActionKind::CreateBuildSecondary {
                BUILD_DEADLINE_SECONDS
            } else {
                ACTION_DEADLINE_SECONDS
            },
        last_error: None,
        dispatch_authorized: false,
        dispatch_agent_generation: None,
        dispatch_agent_control_version: None,
        dispatch_observed_runtime_epoch: None,
        dispatch_action_payload: String::new(),
    })
}

fn validate_pending_action(
    operation: &DurableOperationStatus,
    pending: &PendingActionStatus,
) -> Result<(), String> {
    if !action_allowed_in_phase(operation.phase, pending.kind) {
        return Err(format!(
            "pending action {:?} is illegal in creation phase {:?}",
            pending.kind, operation.phase
        ));
    }
    let expected = pending_action(operation, pending.sequence, pending.kind, 0)?;
    if pending.action_id != expected.action_id
        || pending.target_id != expected.target_id
        || pending.target_instance_id != expected.target_instance_id
        || pending.expected_epoch != expected.expected_epoch
        || pending.desired_postcondition != expected.desired_postcondition
    {
        return Err("pending creation action is not deterministic".to_string());
    }
    Ok(())
}

fn action_allowed_in_phase(phase: DurableOperationPhase, kind: DurableActionKind) -> bool {
    matches!(
        (phase, kind),
        (
            DurableOperationPhase::CreateFenceRouting,
            DurableActionKind::CreateFencePod
        ) | (
            DurableOperationPhase::CreateOpenPrimary,
            DurableActionKind::CreateOpenPrimary
        ) | (
            DurableOperationPhase::CreatePromotePrimary,
            DurableActionKind::CreatePromotePrimary
        ) | (
            DurableOperationPhase::CreatePrimaryCurrentConfiguration,
            DurableActionKind::CreatePrimaryCurrentConfiguration
        ) | (
            DurableOperationPhase::CreateOpenSecondary,
            DurableActionKind::CreateOpenSecondary
        ) | (
            DurableOperationPhase::CreateUpdateSecondaryEpoch,
            DurableActionKind::CreateUpdateSecondaryEpoch
        ) | (
            DurableOperationPhase::CreateAssignSecondaryIdle,
            DurableActionKind::CreateAssignSecondaryIdle
        ) | (
            DurableOperationPhase::CreateBuildSecondary,
            DurableActionKind::CreateBuildSecondary
        ) | (
            DurableOperationPhase::CreateAssignSecondaryActive,
            DurableActionKind::CreateAssignSecondaryActive
        ) | (
            DurableOperationPhase::CreateCatchUpConfiguration,
            DurableActionKind::CreateCatchUpConfiguration
        ) | (
            DurableOperationPhase::CreateWaitForCatchUpQuorum,
            DurableActionKind::CreateWaitForCatchUpQuorum
        ) | (
            DurableOperationPhase::CreateCurrentConfiguration,
            DurableActionKind::CreateCurrentConfiguration
        ) | (
            DurableOperationPhase::CreatePublishRouting,
            DurableActionKind::CreatePublishPrimary | DurableActionKind::CreatePublishSecondary
        ) | (
            DurableOperationPhase::CreateCompensateRestoreConfiguration,
            DurableActionKind::CreateCompensateRestoreConfiguration
        ) | (
            DurableOperationPhase::CreateCompensateRemoveCandidate,
            DurableActionKind::CreateCompensateRemoveCandidate
        ) | (
            DurableOperationPhase::CreateCompensateDemoteCandidate,
            DurableActionKind::CreateCompensateDemoteCandidate
        ) | (
            DurableOperationPhase::CreateCompensateCloseCandidate,
            DurableActionKind::CreateCompensateCloseCandidate
        ) | (
            DurableOperationPhase::CreateCompensateDeleteCandidate,
            DurableActionKind::CreateCompensateDeleteCandidate
        )
    )
}

fn next_phase(kind: DurableActionKind) -> DurableOperationPhase {
    match kind {
        DurableActionKind::CreateOpenPrimary => DurableOperationPhase::CreatePromotePrimary,
        DurableActionKind::CreatePromotePrimary => {
            DurableOperationPhase::CreatePrimaryCurrentConfiguration
        }
        DurableActionKind::CreatePrimaryCurrentConfiguration => {
            DurableOperationPhase::CreateCommitPrimary
        }
        DurableActionKind::CreateOpenSecondary => DurableOperationPhase::CreateUpdateSecondaryEpoch,
        DurableActionKind::CreateUpdateSecondaryEpoch => {
            DurableOperationPhase::CreateAssignSecondaryIdle
        }
        DurableActionKind::CreateAssignSecondaryIdle => DurableOperationPhase::CreateBuildSecondary,
        DurableActionKind::CreateBuildSecondary => {
            DurableOperationPhase::CreateAssignSecondaryActive
        }
        DurableActionKind::CreateAssignSecondaryActive => {
            DurableOperationPhase::CreateCatchUpConfiguration
        }
        DurableActionKind::CreateCatchUpConfiguration => {
            DurableOperationPhase::CreateWaitForCatchUpQuorum
        }
        DurableActionKind::CreateWaitForCatchUpQuorum => {
            DurableOperationPhase::CreateCurrentConfiguration
        }
        DurableActionKind::CreateCurrentConfiguration => {
            DurableOperationPhase::CreateCommitSecondary
        }
        DurableActionKind::CreateCompensateRestoreConfiguration => {
            DurableOperationPhase::CreateCompensateRemoveCandidate
        }
        DurableActionKind::CreateCompensateRemoveCandidate => {
            DurableOperationPhase::CreateCompensateDemoteCandidate
        }
        DurableActionKind::CreateCompensateDemoteCandidate => {
            DurableOperationPhase::CreateCompensateCloseCandidate
        }
        DurableActionKind::CreateCompensateCloseCandidate => {
            DurableOperationPhase::CreateCompensateDeleteCandidate
        }
        DurableActionKind::CreateCompensateDeleteCandidate => {
            DurableOperationPhase::CreateCompensateFinalize
        }
        _ => unreachable!("validated creation action"),
    }
}

fn failure_decision(operation: &DurableOperationStatus, error: &str, now: i64) -> Decision {
    let mut next = operation.clone();
    next.pending_action = None;
    next.last_error = Some(bounded_error(error));
    next.phase_deadline_unix_seconds = now + ACTION_DEADLINE_SECONDS;

    if matches!(
        operation.phase,
        DurableOperationPhase::CreateFenceRouting
            | DurableOperationPhase::CreateCompensateRestoreConfiguration
            | DurableOperationPhase::CreateCompensateRemoveCandidate
            | DurableOperationPhase::CreateCompensateDemoteCandidate
            | DurableOperationPhase::CreateCompensateCloseCandidate
            | DurableOperationPhase::CreateCompensateDeleteCandidate
            | DurableOperationPhase::CreateCompensateFinalize
    ) {
        next.phase = DurableOperationPhase::Poisoned;
    } else if operation.committed_snapshot.is_none()
        && operation.phase == DurableOperationPhase::CreateOpenPrimary
    {
        next.phase = DurableOperationPhase::CreateCompensateDeleteCandidate;
    } else if matches!(
        operation.phase,
        DurableOperationPhase::CreateCatchUpConfiguration
            | DurableOperationPhase::CreateWaitForCatchUpQuorum
            | DurableOperationPhase::CreateCurrentConfiguration
    ) {
        next.phase = DurableOperationPhase::CreateCompensateRestoreConfiguration;
    } else if matches!(
        operation.phase,
        DurableOperationPhase::CreateCommitPrimary
            | DurableOperationPhase::CreateCommitSecondary
            | DurableOperationPhase::CreatePublishRouting
            | DurableOperationPhase::CreateFinalize
    ) {
        next.phase = DurableOperationPhase::Poisoned;
    } else {
        next.phase = DurableOperationPhase::CreateCompensateRemoveCandidate;
    }
    Decision::Persist(next)
}

fn advance_after_commit(
    operation: &DurableOperationStatus,
    committed: StablePartitionSnapshotStatus,
    observations: &OperationObservations,
    now: i64,
) -> Result<Decision, String> {
    let committed_secondaries = committed.members.len() - 1;
    let secondary_count = operation.target_snapshot.members.len() - 1;
    let mut next = operation.clone();
    next.committed_snapshot = Some(committed);
    next.pending_action = None;
    next.last_error = None;
    next.phase_deadline_unix_seconds = now + ACTION_DEADLINE_SECONDS;
    if committed_secondaries == secondary_count {
        next.phase = DurableOperationPhase::CreatePublishRouting;
        next.next_secondary_index = 0;
        let primary = operation
            .target_snapshot
            .members
            .first()
            .ok_or_else(|| "creation target has no primary".to_string())?;
        set_current_target(&mut next, primary, observations)?;
    } else {
        next.phase = DurableOperationPhase::CreateOpenSecondary;
        next.next_secondary_index = committed_secondaries as u32;
        let target = operation
            .target_snapshot
            .members
            .get(committed_secondaries + 1)
            .ok_or_else(|| "creation target has no next secondary".to_string())?;
        if observations.get(&target.id).is_some_and(|observation| {
            observation.status.instance_id.as_str() != target.instance_id
        }) {
            next.phase = DurableOperationPhase::Failed;
            next.last_error = Some("next creation candidate incarnation changed".to_string());
            return Ok(Decision::RestartCreation { operation: next });
        }

        set_current_target(&mut next, target, observations)?;
    }
    Ok(Decision::Persist(next))
}

fn advance_after_fencing(
    operation: &mut DurableOperationStatus,
    observations: &OperationObservations,
) -> Result<(), String> {
    let committed_secondaries = operation
        .committed_snapshot
        .as_ref()
        .map_or(0, |snapshot| snapshot.members.len().saturating_sub(1));
    if operation.committed_snapshot.is_none() {
        operation.phase = DurableOperationPhase::CreateOpenPrimary;
        operation.next_secondary_index = 0;
        let primary = operation.target_snapshot.members[0].clone();
        set_current_target(operation, &primary, observations)?;
    } else if committed_secondaries == operation.target_snapshot.members.len() - 1 {
        operation.phase = DurableOperationPhase::CreatePublishRouting;
        operation.next_secondary_index = 0;
        let primary = operation.target_snapshot.members[0].clone();
        set_current_target(operation, &primary, observations)?;
    } else {
        operation.phase = DurableOperationPhase::CreateOpenSecondary;
        operation.next_secondary_index = committed_secondaries as u32;
        let target = operation.target_snapshot.members[committed_secondaries + 1].clone();
        set_current_target(operation, &target, observations)?;
    }
    Ok(())
}

fn set_current_target(
    operation: &mut DurableOperationStatus,
    member: &StableReplicaSnapshotStatus,
    observations: &OperationObservations,
) -> Result<(), String> {
    operation.target_replica_id = Some(member.id);
    operation.target_instance_id = Some(member.instance_id.clone());
    operation.target_pod_uid = Some(member.instance_id.clone());
    operation.target_pod_name = Some(
        observations
            .get(&member.id)
            .ok_or_else(|| format!("creation target {} is unavailable", member.id))?
            .pod_name
            .clone(),
    );
    Ok(())
}

fn validate_operation(operation: &DurableOperationStatus) -> Result<(), String> {
    if operation.version != DURABLE_OPERATION_VERSION {
        return Err(format!(
            "unsupported durable operation version {}",
            operation.version
        ));
    }
    if operation.kind != DurableOperationKind::CreatePartition {
        return Err("operation kind is not create-partition".to_string());
    }
    if operation.previous_snapshot.is_some() {
        return Err("partition creation must explicitly have no previous topology".to_string());
    }
    validate_snapshot(&operation.target_snapshot)?;
    if operation.target_snapshot.members[0].id != operation.target_snapshot.primary_id
        || operation.old_primary_id != operation.target_snapshot.primary_id
        || operation.target_primary_id != operation.target_snapshot.primary_id
    {
        return Err("creation primary identity is inconsistent".to_string());
    }
    let ordered_tail_start = operation
        .committed_snapshot
        .as_ref()
        .map_or(1, |snapshot| snapshot.members.len());
    if ordered_tail_start > operation.target_snapshot.members.len() {
        return Err("committed creation prefix exceeds target membership".to_string());
    }
    if !operation.target_snapshot.members[ordered_tail_start..]
        .windows(2)
        .all(|members| members[0].id < members[1].id)
    {
        return Err("uncommitted creation targets are not in deterministic ID order".to_string());
    }
    let minimum = operation
        .minimum_committed_replicas
        .ok_or_else(|| "creation operation has no minReplicas".to_string())?;
    if minimum == 0 || minimum as usize > operation.target_snapshot.members.len() {
        return Err("creation minReplicas is outside the target membership".to_string());
    }
    if let Some(committed) = &operation.committed_snapshot {
        validate_committed_prefix(committed, &operation.target_snapshot)?;
    }
    if is_compensation_phase(operation.phase) {
        ensure_compensation_target_uncommitted(operation)?;
    }
    target_replica_id(operation)?;
    target_instance_id(operation)?;
    target_pod_name(operation)?;
    Ok(())
}

fn fence_member(
    operation: &DurableOperationStatus,
) -> Result<&StableReplicaSnapshotStatus, String> {
    operation
        .target_snapshot
        .members
        .get(operation.next_secondary_index as usize)
        .ok_or_else(|| "creation fencing index is out of range".to_string())
}

fn is_committed_member(operation: &DurableOperationStatus, replica_id: i64) -> bool {
    operation
        .committed_snapshot
        .as_ref()
        .is_some_and(|snapshot| {
            snapshot
                .members
                .iter()
                .any(|member| member.id == replica_id)
        })
}

fn ensure_compensation_target_uncommitted(
    operation: &DurableOperationStatus,
) -> Result<(), String> {
    let target_id = target_replica_id(operation)?;
    if is_committed_member(operation, target_id) {
        return Err(format!(
            "creation compensation cannot target committed replica {target_id}"
        ));
    }
    Ok(())
}

fn is_compensation_phase(phase: DurableOperationPhase) -> bool {
    matches!(
        phase,
        DurableOperationPhase::CreateCompensateRestoreConfiguration
            | DurableOperationPhase::CreateCompensateRemoveCandidate
            | DurableOperationPhase::CreateCompensateDemoteCandidate
            | DurableOperationPhase::CreateCompensateCloseCandidate
            | DurableOperationPhase::CreateCompensateDeleteCandidate
            | DurableOperationPhase::CreateCompensateFinalize
    )
}

fn is_compensation_action(kind: DurableActionKind) -> bool {
    matches!(
        kind,
        DurableActionKind::CreateCompensateRestoreConfiguration
            | DurableActionKind::CreateCompensateRemoveCandidate
            | DurableActionKind::CreateCompensateDemoteCandidate
            | DurableActionKind::CreateCompensateCloseCandidate
            | DurableActionKind::CreateCompensateDeleteCandidate
    )
}

fn validate_committed_prefix(
    committed: &StablePartitionSnapshotStatus,
    target: &StablePartitionSnapshotStatus,
) -> Result<(), String> {
    validate_snapshot(committed)?;
    if committed.epoch != target.epoch
        || committed.primary_id != target.primary_id
        || committed.members.len() > target.members.len()
        || committed.members != target.members[..committed.members.len()]
        || committed.write_quorum != majority(committed.members.len())
    {
        return Err("committed bootstrap snapshot is not an exact target prefix".to_string());
    }
    Ok(())
}

fn validate_committed_observations(
    operation: &DurableOperationStatus,
    observations: &OperationObservations,
) -> Result<(), String> {
    let Some(committed) = &operation.committed_snapshot else {
        return Ok(());
    };
    let epoch = target_epoch(operation);
    for member in &committed.members {
        let observation = observations
            .get(&member.id)
            .ok_or_else(|| format!("committed bootstrap member {} is unavailable", member.id))?;
        if observation.status.instance_id.as_str() != member.instance_id {
            return Err(format!(
                "committed bootstrap member {} incarnation changed",
                member.id
            ));
        }
        let expected_role = if member.id == committed.primary_id {
            Role::Primary
        } else {
            Role::ActiveSecondary
        };
        if observation.status.role != expected_role || observation.status.epoch != epoch {
            return Err(format!(
                "committed bootstrap member {} has unexpected role or epoch",
                member.id
            ));
        }
    }
    let primary = observations
        .get(&committed.primary_id)
        .ok_or_else(|| "committed bootstrap primary is unavailable".to_string())?;
    let committed_current =
        configuration_status_for_snapshot(committed, ReplicaConfigurationMode::Current);
    let attempted = attempted_snapshot(operation).ok();
    let attempted_catch_up = attempted.as_ref().map(|snapshot| {
        configuration_status_for_snapshot(snapshot, ReplicaConfigurationMode::CatchUp)
    });
    let attempted_current = attempted.as_ref().map(|snapshot| {
        configuration_status_for_snapshot(snapshot, ReplicaConfigurationMode::Current)
    });
    let observed = primary.status.configuration.as_ref();
    let valid = match operation.phase {
        DurableOperationPhase::CreateCatchUpConfiguration => {
            observed == Some(&committed_current)
                || attempted_catch_up
                    .as_ref()
                    .is_some_and(|value| observed == Some(value))
        }
        DurableOperationPhase::CreateWaitForCatchUpQuorum => attempted_catch_up
            .as_ref()
            .is_some_and(|value| observed == Some(value)),
        DurableOperationPhase::CreateCurrentConfiguration
        | DurableOperationPhase::CreateCommitSecondary => {
            attempted_catch_up
                .as_ref()
                .is_some_and(|value| observed == Some(value))
                || attempted_current
                    .as_ref()
                    .is_some_and(|value| observed == Some(value))
        }
        DurableOperationPhase::CreateCompensateRestoreConfiguration => {
            observed == Some(&committed_current)
                || attempted_catch_up
                    .as_ref()
                    .is_some_and(|value| observed == Some(value))
        }
        DurableOperationPhase::CreatePublishRouting | DurableOperationPhase::CreateFinalize => {
            observed
                == Some(&configuration_status_for_snapshot(
                    &operation.target_snapshot,
                    ReplicaConfigurationMode::Current,
                ))
        }
        _ => observed == Some(&committed_current),
    };
    if !valid {
        return Err("committed bootstrap primary has unexpected configuration".to_string());
    }
    Ok(())
}

fn validate_final_observations(
    operation: &DurableOperationStatus,
    observations: &OperationObservations,
) -> Result<(), String> {
    if operation.committed_snapshot.as_ref() != Some(&operation.target_snapshot) {
        return Err("final creation topology is not durably committed".to_string());
    }
    let epoch = target_epoch(operation);
    for member in &operation.target_snapshot.members {
        let observation = observations
            .get(&member.id)
            .ok_or_else(|| format!("final creation member {} is unavailable", member.id))?;
        let expected_role = if member.id == operation.target_snapshot.primary_id {
            Role::Primary
        } else {
            Role::ActiveSecondary
        };
        let expected_label = if expected_role == Role::Primary {
            "primary"
        } else {
            "secondary"
        };
        if observation.status.instance_id.as_str() != member.instance_id
            || observation.status.role != expected_role
            || observation.status.epoch != epoch
            || observation.pod_role_label.as_deref() != Some(expected_label)
        {
            return Err(format!(
                "final creation member {} does not match committed role, epoch, incarnation, or label",
                member.id
            ));
        }
    }
    Ok(())
}

fn current_candidate_changed(
    operation: &DurableOperationStatus,
    observations: &OperationObservations,
) -> bool {
    if matches!(
        operation.phase,
        DurableOperationPhase::CreateCompensateRestoreConfiguration
            | DurableOperationPhase::CreateCompensateRemoveCandidate
            | DurableOperationPhase::CreateCompensateDemoteCandidate
            | DurableOperationPhase::CreateCompensateCloseCandidate
            | DurableOperationPhase::CreateCompensateDeleteCandidate
            | DurableOperationPhase::CreateCompensateFinalize
            | DurableOperationPhase::CreateFenceRouting
            | DurableOperationPhase::CreatePublishRouting
            | DurableOperationPhase::CreateFinalize
    ) {
        return false;
    }
    let Ok(target_id) = target_replica_id(operation) else {
        return false;
    };
    observations.get(&target_id).is_some_and(|observation| {
        target_instance_id(operation)
            .is_ok_and(|expected| observation.status.instance_id.as_str() != expected)
    })
}

fn attempted_snapshot(
    operation: &DurableOperationStatus,
) -> Result<StablePartitionSnapshotStatus, String> {
    let committed = committed_snapshot(operation)?;
    let target_id = target_replica_id(operation)?;
    let next = operation
        .target_snapshot
        .members
        .iter()
        .find(|member| member.id == target_id)
        .ok_or_else(|| "current creation candidate is absent from target snapshot".to_string())?;
    if committed
        .members
        .iter()
        .any(|member| member.id == target_id)
    {
        return Ok(committed.clone());
    }
    let mut expanded = committed.clone();
    expanded.members.push(next.clone());
    expanded.write_quorum = majority(expanded.members.len());
    Ok(expanded)
}

fn snapshot_prefix(
    target: &StablePartitionSnapshotStatus,
    secondary_count: usize,
) -> StablePartitionSnapshotStatus {
    let member_count = secondary_count + 1;
    StablePartitionSnapshotStatus {
        epoch: target.epoch.clone(),
        primary_id: target.primary_id,
        members: target.members[..member_count].to_vec(),
        write_quorum: majority(member_count),
    }
}

fn committed_snapshot(
    operation: &DurableOperationStatus,
) -> Result<&StablePartitionSnapshotStatus, String> {
    operation
        .committed_snapshot
        .as_ref()
        .ok_or_else(|| "creation has no committed bootstrap topology".to_string())
}

fn candidate_info(
    operation: &DurableOperationStatus,
    observations: &OperationObservations,
    role: Role,
) -> Result<ReplicaInfo, String> {
    let target_id = target_replica_id(operation)?;
    let observation = observations
        .get(&target_id)
        .ok_or_else(|| format!("creation candidate {target_id} is unavailable"))?;
    if observation.status.instance_id.as_str() != target_instance_id(operation)? {
        return Err("creation candidate incarnation changed".to_string());
    }
    Ok(ReplicaInfo {
        id: target_id,
        instance_id: observation.status.instance_id.clone(),
        role,
        status: ReplicaStatus::Up,
        replicator_address: observation.replicator_address.clone(),
        current_progress: observation.status.current_progress,
        catch_up_capability: observation.status.current_progress,
        must_catch_up: false,
    })
}

fn config_for_snapshot(
    snapshot: &StablePartitionSnapshotStatus,
    observations: &OperationObservations,
    must_catch_up: bool,
    must_catch_up_id: Option<i64>,
) -> Result<ReplicaSetConfig, String> {
    let mut members = Vec::new();
    for member in &snapshot.members {
        if member.id == snapshot.primary_id {
            continue;
        }
        let observation = observations
            .get(&member.id)
            .ok_or_else(|| format!("creation member {} is unavailable", member.id))?;
        if observation.status.instance_id.as_str() != member.instance_id {
            return Err(format!("creation member {} incarnation changed", member.id));
        }
        members.push(ReplicaInfo {
            id: member.id,
            instance_id: ReplicaInstanceId::new(member.instance_id.clone()),
            role: Role::ActiveSecondary,
            status: ReplicaStatus::Up,
            replicator_address: observation.replicator_address.clone(),
            current_progress: observation.status.current_progress,
            catch_up_capability: observation.status.current_progress,
            must_catch_up: must_catch_up && must_catch_up_id == Some(member.id),
        });
    }
    Ok(ReplicaSetConfig {
        members,
        write_quorum: snapshot.write_quorum,
    })
}

fn configuration_status_for_snapshot(
    snapshot: &StablePartitionSnapshotStatus,
    mode: ReplicaConfigurationMode,
) -> ReplicaConfigurationStatus {
    let config = ReplicaSetConfig {
        members: snapshot
            .members
            .iter()
            .filter(|member| member.id != snapshot.primary_id)
            .map(|member| ReplicaInfo {
                id: member.id,
                instance_id: ReplicaInstanceId::new(member.instance_id.clone()),
                role: Role::ActiveSecondary,
                status: ReplicaStatus::Up,
                replicator_address: String::new(),
                current_progress: 0,
                catch_up_capability: 0,
                must_catch_up: false,
            })
            .collect(),
        write_quorum: snapshot.write_quorum,
    };
    ReplicaConfigurationStatus::from_config(mode, &config)
}

fn configuration_observation(
    status: &kuberic_core::types::ReplicaStatusInfo,
    expected: &ReplicaConfigurationStatus,
    precondition: Option<&ReplicaConfigurationStatus>,
    expected_epoch: Epoch,
) -> ActionObservation {
    if status.role != Role::Primary || status.epoch != expected_epoch {
        ActionObservation::Impossible
    } else if status.configuration.as_ref() == Some(expected) {
        ActionObservation::Postcondition
    } else if status.configuration.as_ref() == precondition {
        ActionObservation::Precondition
    } else {
        ActionObservation::Impossible
    }
}

fn validate_snapshot(snapshot: &StablePartitionSnapshotStatus) -> Result<(), String> {
    if snapshot.members.is_empty() {
        return Err("stable snapshot has no members".to_string());
    }
    let mut ids = HashSet::new();
    let mut instances = HashSet::new();
    let mut primary_count = 0;
    for member in &snapshot.members {
        if member.instance_id.is_empty()
            || !ids.insert(member.id)
            || !instances.insert(member.instance_id.as_str())
        {
            return Err("stable snapshot has empty or duplicate identity".to_string());
        }
        if member.role == StableReplicaRoleStatus::Primary {
            primary_count += 1;
            if member.id != snapshot.primary_id {
                return Err("stable snapshot primary role disagrees with primary ID".to_string());
            }
        }
    }
    if primary_count != 1 || snapshot.write_quorum != majority(snapshot.members.len()) {
        return Err("stable snapshot primary or majority quorum is invalid".to_string());
    }
    Ok(())
}

fn secondary_sequence(operation: &DurableOperationStatus, offset: u32) -> Result<u32, String> {
    if operation.committed_snapshot.is_none() {
        return Err("secondary creation requires a committed primary".to_string());
    }
    Ok(10 + operation.next_secondary_index * 10 + offset)
}

fn with_phase(
    operation: &DurableOperationStatus,
    phase: DurableOperationPhase,
    now: i64,
) -> DurableOperationStatus {
    let mut next = operation.clone();
    next.phase = phase;
    next.pending_action = None;
    next.phase_deadline_unix_seconds = now + ACTION_DEADLINE_SECONDS;
    next
}

fn persist_pending(operation: &DurableOperationStatus, pending: PendingActionStatus) -> Decision {
    let mut next = operation.clone();
    next.pending_action = Some(pending);
    Decision::Persist(next)
}

fn target_replica_id(operation: &DurableOperationStatus) -> Result<i64, String> {
    operation
        .target_replica_id
        .ok_or_else(|| "creation operation has no current target ID".to_string())
}

fn target_instance_id(operation: &DurableOperationStatus) -> Result<&str, String> {
    operation
        .target_instance_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "creation operation has no current target incarnation".to_string())
}

fn target_pod_name(operation: &DurableOperationStatus) -> Result<&str, String> {
    operation
        .target_pod_name
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "creation operation has no current target pod".to_string())
}

fn target_epoch(operation: &DurableOperationStatus) -> Epoch {
    Epoch::new(
        operation.target_snapshot.epoch.data_loss_number,
        operation.target_snapshot.epoch.configuration_number,
    )
}

fn majority(member_count: usize) -> u32 {
    member_count as u32 / 2 + 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable::ReplicaObservation;
    use kuberic_core::types::{AccessStatus, ReplicaStatusInfo};

    fn targets() -> Vec<CreatePartitionTarget> {
        vec![
            CreatePartitionTarget {
                replica_id: 1,
                instance_id: "one".to_string(),
                pod_name: "pod-0".to_string(),
            },
            CreatePartitionTarget {
                replica_id: 2,
                instance_id: "two".to_string(),
                pod_name: "pod-1".to_string(),
            },
        ]
    }

    fn observation(
        instance_id: &str,
        role: Role,
        epoch: Epoch,
        configuration: Option<ReplicaConfigurationStatus>,
        label: &str,
        healthy: bool,
    ) -> ReplicaObservation {
        ReplicaObservation {
            status: ReplicaStatusInfo {
                instance_id: ReplicaInstanceId::new(instance_id),
                role,
                epoch,
                current_progress: 0,
                catch_up_capability: Some(0),
                committed_lsn: 0,
                healthy,
                write_status: if role == Role::Primary {
                    AccessStatus::Granted
                } else {
                    AccessStatus::NotPrimary
                },
                configuration,
                election_configuration: None,
                deactivation_info: None,
                active_replica_connections: Vec::new(),
                build_observation: None,
                agent: kuberic_core::types::ReplicaAgentStatus {
                    protocol_version:
                        kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION,
                    lifecycle_peer_protocol_version:
                        kuberic_core::replica_lifecycle::REPLICA_LIFECYCLE_PEER_PROTOCOL_VERSION,
                    generation: kuberic_core::types::AgentGeneration::parse(
                        "0123456789abcdef0123456789abcdef",
                    )
                    .unwrap(),
                    control_version: kuberic_core::types::AgentControlVersion::default(),
                    current_action: None,
                    retained_terminal_actions: Vec::new(),
                    local_faults: Vec::new(),
                },
            },
            control_address: format!("http://{instance_id}-control"),
            replicator_address: format!("http://{instance_id}"),
            pod_name: instance_id.to_string(),
            pod_role_label: Some(label.to_string()),
        }
    }

    #[test]
    fn creation_selects_lowest_id_and_exact_majorities() {
        let operation = start_create_partition(
            "set",
            vec![
                CreatePartitionTarget {
                    replica_id: 3,
                    instance_id: "three".to_string(),
                    pod_name: "pod-2".to_string(),
                },
                CreatePartitionTarget {
                    replica_id: 1,
                    instance_id: "one".to_string(),
                    pod_name: "pod-0".to_string(),
                },
                CreatePartitionTarget {
                    replica_id: 2,
                    instance_id: "two".to_string(),
                    pod_name: "pod-1".to_string(),
                },
            ],
            None,
            2,
            10,
        )
        .unwrap();
        assert!(operation.previous_snapshot.is_none());
        assert!(operation.committed_snapshot.is_none());
        assert_eq!(operation.target_snapshot.primary_id, 1);
        assert_eq!(
            operation
                .target_snapshot
                .members
                .iter()
                .map(|member| member.id)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(operation.target_snapshot.write_quorum, 2);
        assert_eq!(
            snapshot_prefix(&operation.target_snapshot, 0).write_quorum,
            1
        );
        assert_eq!(
            snapshot_prefix(&operation.target_snapshot, 1).write_quorum,
            2
        );
    }

    #[test]
    fn explicit_primary_is_first_and_survives_serde_replay() {
        let operation =
            start_create_partition_with_primary("set", targets(), None, 1, 10, Some(2)).unwrap();
        assert_eq!(operation.target_primary_id, 2);
        assert_eq!(operation.target_replica_id, Some(2));
        assert_eq!(operation.target_pod_name.as_deref(), Some("pod-1"));
        assert_eq!(
            operation
                .target_snapshot
                .members
                .iter()
                .map(|member| member.id)
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
        validate_operation(&operation).unwrap();
        let serialized = serde_json::to_string(&operation).unwrap();
        let mut restored: DurableOperationStatus = serde_json::from_str(&serialized).unwrap();
        validate_operation(&restored).unwrap();
        restored.phase = DurableOperationPhase::CreateOpenPrimary;
        let Decision::Persist(pending) = decide_create_partition(
            &restored,
            &OperationObservations::new(),
            &OperationPodIdentities::new(),
            11,
        )
        .unwrap() else {
            panic!("opening the selected primary must persist its action");
        };
        assert_eq!(pending.pending_action.unwrap().target_id, 2);

        restored.committed_snapshot = Some(snapshot_prefix(&restored.target_snapshot, 0));
        restored.target_replica_id = Some(1);
        restored.target_instance_id = Some("one".to_string());
        restored.target_pod_uid = Some("one".to_string());
        restored.target_pod_name = Some("pod-0".to_string());
        let expanded = attempted_snapshot(&restored).unwrap();
        assert_eq!(expanded, restored.target_snapshot);
        validate_committed_prefix(&expanded, &restored.target_snapshot).unwrap();
    }

    #[test]
    fn committed_primary_and_prefix_override_new_preferences_on_recovery() {
        let mut initial_targets = targets();
        initial_targets.push(CreatePartitionTarget {
            replica_id: 3,
            instance_id: "three".to_string(),
            pod_name: "pod-2".to_string(),
        });
        let initial =
            start_create_partition_with_primary("set", initial_targets, None, 1, 10, Some(3))
                .unwrap();
        let committed = snapshot_prefix(&initial.target_snapshot, 1);
        let recovery_targets = vec![
            targets()[0].clone(),
            CreatePartitionTarget {
                replica_id: 3,
                instance_id: "three".to_string(),
                pod_name: "pod-2".to_string(),
            },
            targets()[1].clone(),
        ];
        for preference in [None, Some(1), Some(2)] {
            let recovered = start_create_partition_with_primary(
                "set",
                recovery_targets.clone(),
                Some(committed.clone()),
                1,
                20,
                preference,
            )
            .unwrap();
            assert_eq!(recovered.target_primary_id, 3);
            assert_eq!(recovered.target_snapshot.members[..2], committed.members);
            assert_eq!(recovered.target_snapshot.members[2].id, 2);
            validate_operation(&recovered).unwrap();
            let restored: DurableOperationStatus =
                serde_json::from_str(&serde_json::to_string(&recovered).unwrap()).unwrap();
            validate_operation(&restored).unwrap();
        }
    }

    #[test]
    fn primary_preference_requires_exact_target_and_committed_incarnation() {
        assert!(
            start_create_partition_with_primary("set", targets(), None, 1, 10, Some(99),)
                .unwrap_err()
                .contains("exact targets")
        );
        let initial =
            start_create_partition_with_primary("set", targets(), None, 1, 10, Some(2)).unwrap();
        let committed = snapshot_prefix(&initial.target_snapshot, 0);
        let mut replacement = targets();
        replacement[1].instance_id = "replacement-two".to_string();
        assert!(
            start_create_partition_with_primary(
                "set",
                replacement,
                Some(committed),
                1,
                20,
                Some(1),
            )
            .unwrap_err()
            .contains("exact target prefix")
        );
    }

    #[test]
    fn recovery_preserves_committed_members_ahead_of_new_lower_id_targets() {
        let make_target = |id| CreatePartitionTarget {
            replica_id: id,
            instance_id: format!("instance-{id}"),
            pod_name: format!("pod-{id}"),
        };
        let initial = start_create_partition_with_primary(
            "set",
            [3, 4, 5].into_iter().map(make_target).collect(),
            None,
            1,
            10,
            Some(4),
        )
        .unwrap();
        let committed = snapshot_prefix(&initial.target_snapshot, 1);
        let recovered = start_create_partition_with_primary(
            "set",
            [2, 3, 4, 5].into_iter().map(make_target).collect(),
            Some(committed),
            1,
            20,
            Some(2),
        )
        .unwrap();
        assert_eq!(
            recovered
                .target_snapshot
                .members
                .iter()
                .map(|member| member.id)
                .collect::<Vec<_>>(),
            vec![4, 3, 2, 5]
        );
        validate_operation(&recovered).unwrap();
        let observations = [2, 3, 4, 5]
            .into_iter()
            .map(|id| {
                (
                    id,
                    observation(
                        &format!("instance-{id}"),
                        if id == 4 {
                            Role::Primary
                        } else {
                            Role::ActiveSecondary
                        },
                        target_epoch(&recovered),
                        None,
                        "",
                        true,
                    ),
                )
            })
            .collect();
        let mut resumed = recovered;
        advance_after_fencing(&mut resumed, &observations).unwrap();
        assert_eq!(resumed.phase, DurableOperationPhase::CreateOpenSecondary);
        assert_eq!(resumed.target_replica_id, Some(2));
        let expanded = attempted_snapshot(&resumed).unwrap();
        assert_eq!(
            expanded
                .members
                .iter()
                .map(|member| member.id)
                .collect::<Vec<_>>(),
            vec![4, 3, 2]
        );
        validate_committed_prefix(&expanded, &resumed.target_snapshot).unwrap();
    }

    #[test]
    fn creation_rejects_invalid_minimum_and_identity() {
        let target = CreatePartitionTarget {
            replica_id: 1,
            instance_id: "one".to_string(),
            pod_name: "pod-0".to_string(),
        };
        assert!(start_create_partition("set", vec![target.clone()], None, 0, 10).is_err());
        assert!(start_create_partition("set", vec![target.clone()], None, 2, 10).is_err());
        assert!(start_create_partition("set", vec![target.clone(), target], None, 1, 10).is_err());
    }

    #[test]
    fn creation_resumes_from_committed_prefix() {
        let targets = vec![
            CreatePartitionTarget {
                replica_id: 1,
                instance_id: "one".to_string(),
                pod_name: "pod-0".to_string(),
            },
            CreatePartitionTarget {
                replica_id: 2,
                instance_id: "two".to_string(),
                pod_name: "pod-1".to_string(),
            },
        ];
        let initial = start_create_partition("set", targets.clone(), None, 1, 10).unwrap();
        let committed = snapshot_prefix(&initial.target_snapshot, 0);
        let resumed = start_create_partition("set", targets, Some(committed), 1, 20).unwrap();
        assert_eq!(resumed.phase, DurableOperationPhase::CreateFenceRouting);
        assert_eq!(resumed.target_replica_id, Some(1));
        assert_eq!(resumed.next_secondary_index, 0);
    }

    #[test]
    fn creation_rejects_illegal_pending_action_and_previous_topology() {
        let mut operation = start_create_partition(
            "set",
            vec![CreatePartitionTarget {
                replica_id: 1,
                instance_id: "one".to_string(),
                pod_name: "pod-0".to_string(),
            }],
            None,
            1,
            10,
        )
        .unwrap();
        operation.pending_action = Some(
            pending_action(&operation, 1, DurableActionKind::CreateBuildSecondary, 10).unwrap(),
        );
        assert!(
            decide_create_partition(
                &operation,
                &OperationObservations::new(),
                &OperationPodIdentities::new(),
                10,
            )
            .unwrap_err()
            .contains("illegal")
        );

        operation.pending_action = None;
        operation.previous_snapshot = operation.target_snapshot.clone().into();
        assert!(
            decide_create_partition(
                &operation,
                &OperationObservations::new(),
                &OperationPodIdentities::new(),
                10,
            )
            .unwrap_err()
            .contains("no previous topology")
        );
    }

    #[test]
    fn unavailable_fence_member_restarts_without_candidate_compensation() {
        let mut operation = start_create_partition("set", targets(), None, 1, 0).unwrap();
        operation.next_secondary_index = 1;
        operation.pending_action =
            Some(pending_action(&operation, 501, DurableActionKind::CreateFencePod, 0).unwrap());

        let Decision::RestartCreation { operation: failed } = decide_create_partition(
            &operation,
            &OperationObservations::new(),
            &OperationPodIdentities::new(),
            10,
        )
        .unwrap() else {
            panic!("missing pre-commit fence member must restart creation");
        };
        assert_eq!(failed.phase, DurableOperationPhase::Failed);
        assert!(failed.committed_snapshot.is_none());
        assert!(failed.pending_action.is_none());
    }

    #[test]
    fn unavailable_uncommitted_fence_member_preserves_primary_commit() {
        let initial = start_create_partition("set", targets(), None, 1, 0).unwrap();
        let committed = snapshot_prefix(&initial.target_snapshot, 0);
        let mut operation =
            start_create_partition("set", targets(), Some(committed.clone()), 1, 0).unwrap();
        operation.next_secondary_index = 1;
        operation.pending_action =
            Some(pending_action(&operation, 501, DurableActionKind::CreateFencePod, 0).unwrap());

        let mut observations = OperationObservations::new();
        observations.insert(
            1,
            observation(
                "one",
                Role::Primary,
                target_epoch(&operation),
                Some(configuration_status_for_snapshot(
                    &committed,
                    ReplicaConfigurationMode::Current,
                )),
                "bootstrap",
                true,
            ),
        );
        let Decision::RestartCreation { operation: failed } = decide_create_partition(
            &operation,
            &observations,
            &OperationPodIdentities::new(),
            10,
        )
        .unwrap() else {
            panic!("missing uncommitted fence member must restart from committed topology");
        };
        assert_eq!(failed.phase, DurableOperationPhase::Failed);
        assert_eq!(failed.committed_snapshot.as_ref(), Some(&committed));
        assert!(failed.pending_action.is_none());
    }

    #[test]
    fn fence_incarnation_change_restarts_with_new_persisted_identity() {
        let original_targets = targets();
        let mut operation =
            start_create_partition("set", original_targets.clone(), None, 1, 0).unwrap();
        operation.next_secondary_index = 1;
        operation.pending_action =
            Some(pending_action(&operation, 501, DurableActionKind::CreateFencePod, 0).unwrap());

        let mut observations = OperationObservations::new();
        observations.insert(
            2,
            observation(
                "two-new",
                Role::Unknown,
                Epoch::default(),
                None,
                "secondary",
                false,
            ),
        );
        let pod_identities = OperationPodIdentities::from([(2, "two-new".to_string())]);
        let Decision::RestartCreation { operation: failed } =
            decide_create_partition(&operation, &observations, &pod_identities, 1).unwrap()
        else {
            panic!("uncommitted fence incarnation change must restart");
        };
        assert_eq!(failed.phase, DurableOperationPhase::Failed);

        let mut replacement_targets = original_targets;
        replacement_targets[1].instance_id = "two-new".to_string();
        let replacement = start_create_partition("set", replacement_targets, None, 1, 2).unwrap();
        assert_ne!(replacement.operation_id, operation.operation_id);
        assert_eq!(
            replacement.target_snapshot.members[1].instance_id,
            "two-new"
        );
        assert_ne!(replacement.phase, DurableOperationPhase::Poisoned);

        let committed = snapshot_prefix(&operation.target_snapshot, 0);
        let mut partial =
            start_create_partition("set", targets(), Some(committed.clone()), 1, 0).unwrap();
        partial.next_secondary_index = 1;
        partial.pending_action =
            Some(pending_action(&partial, 501, DurableActionKind::CreateFencePod, 0).unwrap());
        let mut partial_observations = OperationObservations::new();
        partial_observations.insert(
            1,
            observation(
                "one",
                Role::Primary,
                target_epoch(&partial),
                Some(configuration_status_for_snapshot(
                    &committed,
                    ReplicaConfigurationMode::Current,
                )),
                "bootstrap",
                true,
            ),
        );
        partial_observations.insert(
            2,
            observation(
                "two-new",
                Role::Unknown,
                Epoch::default(),
                None,
                "secondary",
                false,
            ),
        );
        let Decision::RestartCreation {
            operation: partial_failed,
        } = decide_create_partition(&partial, &partial_observations, &pod_identities, 1).unwrap()
        else {
            panic!("uncommitted replacement must preserve partial authority and restart");
        };
        assert_eq!(partial_failed.committed_snapshot.as_ref(), Some(&committed));
        assert_ne!(
            partial_failed.phase,
            DurableOperationPhase::Poisoned,
            "uncommitted fence replacement must not poison committed members"
        );
    }

    #[test]
    fn compensation_and_commit_primary_never_mutate_committed_authority() {
        let initial = start_create_partition("set", targets(), None, 1, 0).unwrap();
        let committed = snapshot_prefix(&initial.target_snapshot, 0);
        let mut invalid = start_create_partition("set", targets(), Some(committed), 1, 0).unwrap();
        invalid.phase = DurableOperationPhase::CreateCompensateDeleteCandidate;
        assert!(
            decide_create_partition(
                &invalid,
                &OperationObservations::new(),
                &OperationPodIdentities::new(),
                1,
            )
            .unwrap_err()
            .contains("cannot target committed replica 1")
        );

        let mut commit_primary = initial;
        commit_primary.phase = DurableOperationPhase::CreateCommitPrimary;
        let mut observations = OperationObservations::new();
        observations.insert(
            1,
            observation(
                "one-new",
                Role::Unknown,
                Epoch::default(),
                None,
                "bootstrap",
                false,
            ),
        );
        let pod_identities = OperationPodIdentities::from([(1, "one-new".to_string())]);
        let Decision::Persist(poisoned) =
            decide_create_partition(&commit_primary, &observations, &pod_identities, 1).unwrap()
        else {
            panic!("primary replacement at the commit boundary must fail closed");
        };
        assert_eq!(poisoned.phase, DurableOperationPhase::Poisoned);
        assert!(poisoned.pending_action.is_none());
    }
}
