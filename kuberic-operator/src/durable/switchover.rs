use std::collections::BTreeMap;

use kuberic_core::types::{
    AccessStatus, DurableActionState, DurableReplicaAction, Epoch, ReplicaConfigurationMode,
    ReplicaConfigurationStatus, ReplicaInfo, ReplicaInstanceId, ReplicaSetConfig,
    ReplicaSetQuorumMode, ReplicaStatus, ReplicaStatusInfo, Role,
};

use crate::crd::{
    DURABLE_OPERATION_VERSION, DurableActionKind, DurableOperationKind, DurableOperationPhase,
    DurableOperationStatus, DurablePostconditionKind, DurablePostconditionStatus, EpochStatus,
    PendingActionStatus, StablePartitionSnapshotStatus, StableReplicaRoleStatus,
};

#[cfg(test)]
use super::MAX_ERROR_LENGTH;
use super::{
    ACTION_DEADLINE_SECONDS, Decision, OperationObservations, ReplicaObservation, poison,
    record_activity_error,
};

#[derive(Debug, Clone, PartialEq, Eq)]
enum ActionObservation {
    Precondition,
    Postcondition,
    InProgress,
    Failed(String),
    Unavailable,
    Impossible,
}

pub fn start_switchover(
    set_uid: &str,
    previous: StablePartitionSnapshotStatus,
    target_primary_id: i64,
    now: i64,
) -> Result<DurableOperationStatus, String> {
    validate_snapshot(&previous)?;
    if previous.primary_id == target_primary_id {
        return Err("switchover target is already primary".to_string());
    }
    if !previous
        .members
        .iter()
        .any(|member| member.id == target_primary_id)
    {
        return Err(format!(
            "switchover target replica {target_primary_id} is not in the stable snapshot"
        ));
    }

    let mut target = previous.clone();
    target.epoch.configuration_number = target
        .epoch
        .configuration_number
        .checked_add(1)
        .ok_or_else(|| "switchover epoch overflow".to_string())?;
    target.primary_id = target_primary_id;
    for member in &mut target.members {
        member.role = if member.id == target_primary_id {
            StableReplicaRoleStatus::Primary
        } else {
            StableReplicaRoleStatus::ActiveSecondary
        };
    }

    let operation_id = format!(
        "{set_uid}:switchover:v{DURABLE_OPERATION_VERSION}:{}-{}:{}",
        previous.epoch.data_loss_number, previous.epoch.configuration_number, target_primary_id
    );
    Ok(DurableOperationStatus {
        execution_id: format!("{operation_id}:execution-1"),
        operation_id,
        version: DURABLE_OPERATION_VERSION,
        kind: DurableOperationKind::Switchover,
        phase: DurableOperationPhase::Revoke,
        old_primary_id: previous.primary_id,
        target_primary_id,
        add_mode: None,
        remove_mode: None,
        target_replica_id: None,
        target_instance_id: None,
        target_pod_name: None,
        target_pod_uid: None,
        remove_target_replicator_address: None,
        remove_target_agent_generation: None,
        retired_instance_id: None,
        previous_snapshot: previous.into(),
        target_snapshot: target,
        committed_snapshot: None,
        minimum_committed_replicas: None,
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

pub fn decide(
    operation: &DurableOperationStatus,
    observations: &OperationObservations,
    now: i64,
) -> Result<Decision, String> {
    validate_operation(operation)?;
    if operation.phase == DurableOperationPhase::Completed
        || operation.phase == DurableOperationPhase::Poisoned
    {
        return Ok(Decision::Wait);
    }
    if operation.phase == DurableOperationPhase::Failed {
        let old = observations
            .get(&operation.old_primary_id)
            .ok_or_else(|| "failed operation old primary is unavailable".to_string())?;
        let previous_epoch = epoch(&operation.previous_snapshot.epoch);
        let target_epoch = epoch(&operation.target_snapshot.epoch);
        let snapshot = if old.status.role == Role::Primary && old.status.epoch == previous_epoch {
            operation.previous_snapshot.cloned().unwrap()
        } else if old.status.role == Role::Primary && old.status.epoch == target_epoch {
            compensation_snapshot(operation)
        } else {
            return Ok(Decision::Persist(poison(
                operation,
                "failed operation cannot attest a stable old-primary topology",
            )));
        };
        return Ok(Decision::Complete {
            operation: operation.clone(),
            snapshot,
            compensated: true,
        });
    }

    if let Some(pending) = &operation.pending_action {
        validate_pending_action(operation, pending)?;
        return decide_pending(operation, pending, observations, now);
    }

    match operation.phase {
        DurableOperationPhase::Revoke => Ok(Decision::Persist(with_pending(
            operation,
            pending_action(operation, 1, DurableActionKind::RevokeWrite, now)?,
        ))),
        DurableOperationPhase::CaptureLsn => {
            let Some(old) = observations.get(&operation.old_primary_id) else {
                return Ok(if now >= operation.phase_deadline_unix_seconds {
                    Decision::Persist(poison(
                        operation,
                        "old primary was unavailable while capturing the frozen LSN",
                    ))
                } else {
                    Decision::Wait
                });
            };
            let expected_member = operation
                .previous_snapshot
                .members
                .iter()
                .find(|member| member.id == operation.old_primary_id)
                .unwrap();
            if old.status.instance_id.as_str() != expected_member.instance_id
                || old.status.epoch != epoch(&operation.previous_snapshot.epoch)
            {
                return Ok(Decision::Persist(poison(
                    operation,
                    "old primary incarnation or epoch changed while capturing the frozen LSN",
                )));
            }
            if old.status.role != Role::Primary
                || old.status.write_status != AccessStatus::ReconfigurationPending
            {
                return Ok(Decision::Persist(poison(
                    operation,
                    "old primary is not write-revoked while capturing the frozen LSN",
                )));
            }
            let mut next = operation.clone();
            next.frozen_lsn = Some(old.status.current_progress);
            next.phase = DurableOperationPhase::PreCatchUp;
            next.phase_deadline_unix_seconds = now + ACTION_DEADLINE_SECONDS;
            Ok(Decision::Persist(next))
        }
        DurableOperationPhase::PreCatchUp => {
            let Some(target) = observations.get(&operation.target_primary_id) else {
                if now >= operation.phase_deadline_unix_seconds {
                    let mut next = operation.clone();
                    next.phase = DurableOperationPhase::RestorePreviousConfiguration;
                    next.phase_deadline_unix_seconds = now + ACTION_DEADLINE_SECONDS;
                    next.last_error =
                        Some("switchover target was unavailable during pre-catch-up".to_string());
                    return Ok(Decision::Persist(next));
                }
                return Ok(Decision::Wait);
            };
            let expected_member = operation
                .previous_snapshot
                .members
                .iter()
                .find(|member| member.id == operation.target_primary_id)
                .unwrap();
            if target.status.instance_id.as_str() != expected_member.instance_id
                || target.status.epoch != epoch(&operation.previous_snapshot.epoch)
            {
                return Ok(Decision::Persist(poison(
                    operation,
                    "switchover target incarnation or epoch changed during pre-catch-up",
                )));
            }
            let frozen = operation
                .frozen_lsn
                .ok_or_else(|| "pre-catch-up phase has no frozen LSN".to_string())?;
            if target.status.role != Role::ActiveSecondary {
                return Ok(Decision::Persist(poison(
                    operation,
                    "switchover target is not an active secondary during pre-catch-up",
                )));
            }
            if target.status.current_progress >= frozen {
                let mut next = operation.clone();
                next.phase = DurableOperationPhase::DemoteOldPrimary;
                next.phase_deadline_unix_seconds = now + ACTION_DEADLINE_SECONDS;
                Ok(Decision::Persist(next))
            } else if now >= operation.phase_deadline_unix_seconds {
                let mut next = operation.clone();
                next.phase = DurableOperationPhase::RestorePreviousConfiguration;
                next.phase_deadline_unix_seconds = now + ACTION_DEADLINE_SECONDS;
                next.last_error =
                    Some("switchover target did not reach the frozen primary LSN".to_string());
                Ok(Decision::Persist(next))
            } else {
                Ok(Decision::Wait)
            }
        }
        DurableOperationPhase::DemoteOldPrimary => Ok(Decision::Persist(with_pending(
            operation,
            pending_action(operation, 2, DurableActionKind::DemoteOldPrimary, now)?,
        ))),
        DurableOperationPhase::PromoteTarget => Ok(Decision::Persist(with_pending(
            operation,
            pending_action(operation, 3, DurableActionKind::PromoteTarget, now)?,
        ))),
        DurableOperationPhase::DistributeEpoch => {
            let secondaries = epoch_distribution_ids(operation);
            let index = operation.next_secondary_index as usize;
            if index >= secondaries.len() {
                let mut next = operation.clone();
                next.phase = DurableOperationPhase::UpdateCatchUpConfiguration;
                next.phase_deadline_unix_seconds = now + ACTION_DEADLINE_SECONDS;
                return Ok(Decision::Persist(next));
            }
            Ok(Decision::Persist(with_pending(
                operation,
                pending_action(
                    operation,
                    100 + operation.next_secondary_index,
                    DurableActionKind::UpdateSecondaryEpoch,
                    now,
                )?,
            )))
        }
        DurableOperationPhase::CompensateFinalize => {
            let snapshot = compensation_snapshot(operation);
            if let Err(error) = validate_snapshot_observation(operation, &snapshot, observations) {
                return Ok(if now >= operation.phase_deadline_unix_seconds {
                    Decision::Persist(poison(operation, &error))
                } else {
                    Decision::Wait
                });
            }
            let mut failed = operation.clone();
            failed.phase = DurableOperationPhase::Failed;
            Ok(Decision::Complete {
                operation: failed,
                snapshot,
                compensated: true,
            })
        }
        DurableOperationPhase::UpdateCatchUpConfiguration => Ok(Decision::Persist(with_pending(
            operation,
            pending_action(
                operation,
                1000,
                DurableActionKind::UpdateCatchUpConfiguration,
                now,
            )?,
        ))),
        DurableOperationPhase::WaitForCatchUpQuorum => Ok(Decision::Persist(with_pending(
            operation,
            pending_action(
                operation,
                1001,
                DurableActionKind::WaitForCatchUpQuorum,
                now,
            )?,
        ))),
        DurableOperationPhase::UpdateCurrentConfiguration => Ok(Decision::Persist(with_pending(
            operation,
            pending_action(
                operation,
                1002,
                DurableActionKind::UpdateCurrentConfiguration,
                now,
            )?,
        ))),
        DurableOperationPhase::LabelTargetPrimary => Ok(Decision::Persist(with_pending(
            operation,
            pending_action(operation, 1003, DurableActionKind::LabelTargetPrimary, now)?,
        ))),
        DurableOperationPhase::LabelOldSecondary => Ok(Decision::Persist(with_pending(
            operation,
            pending_action(operation, 1004, DurableActionKind::LabelOldSecondary, now)?,
        ))),
        DurableOperationPhase::Finalize => {
            if let Err(error) =
                validate_snapshot_observation(operation, &operation.target_snapshot, observations)
            {
                return Ok(if now >= operation.phase_deadline_unix_seconds {
                    Decision::Persist(poison(operation, &error))
                } else {
                    Decision::Wait
                });
            }
            let mut completed = operation.clone();
            completed.phase = DurableOperationPhase::Completed;
            completed.pending_action = None;
            Ok(Decision::Complete {
                operation: completed,
                snapshot: operation.target_snapshot.clone(),
                compensated: false,
            })
        }
        DurableOperationPhase::RestorePreviousConfiguration => Ok(Decision::Persist(with_pending(
            operation,
            pending_action(
                operation,
                1500,
                DurableActionKind::RestorePreviousConfiguration,
                now,
            )?,
        ))),
        DurableOperationPhase::CompensatePromoteOldPrimary => Ok(Decision::Persist(with_pending(
            operation,
            pending_action(
                operation,
                2000,
                DurableActionKind::CompensatePromoteOldPrimary,
                now,
            )?,
        ))),
        DurableOperationPhase::CompensateDistributeEpoch => {
            let secondaries = compensation_epoch_distribution_ids(operation);
            let index = operation.next_secondary_index as usize;
            if index >= secondaries.len() {
                let mut next = operation.clone();
                next.phase = DurableOperationPhase::CompensateCatchUpConfiguration;
                next.phase_deadline_unix_seconds = now + ACTION_DEADLINE_SECONDS;
                return Ok(Decision::Persist(next));
            }
            Ok(Decision::Persist(with_pending(
                operation,
                pending_action(
                    operation,
                    2100 + operation.next_secondary_index,
                    DurableActionKind::CompensateUpdateSecondaryEpoch,
                    now,
                )?,
            )))
        }
        DurableOperationPhase::CompensateCatchUpConfiguration => {
            Ok(Decision::Persist(with_pending(
                operation,
                pending_action(
                    operation,
                    2001,
                    DurableActionKind::CompensateCatchUpConfiguration,
                    now,
                )?,
            )))
        }
        DurableOperationPhase::CompensateCurrentConfiguration => {
            Ok(Decision::Persist(with_pending(
                operation,
                pending_action(
                    operation,
                    2002,
                    DurableActionKind::CompensateCurrentConfiguration,
                    now,
                )?,
            )))
        }
        DurableOperationPhase::CompensateLabelOldPrimary => Ok(Decision::Persist(with_pending(
            operation,
            pending_action(
                operation,
                2003,
                DurableActionKind::CompensateLabelOldPrimary,
                now,
            )?,
        ))),
        DurableOperationPhase::CompensateLabelTargetSecondary => {
            Ok(Decision::Persist(with_pending(
                operation,
                pending_action(
                    operation,
                    2004,
                    DurableActionKind::CompensateLabelTargetSecondary,
                    now,
                )?,
            )))
        }
        DurableOperationPhase::Completed
        | DurableOperationPhase::Failed
        | DurableOperationPhase::Poisoned => Ok(Decision::Wait),
        _ => Err(format!(
            "phase {:?} is not valid for switchover",
            operation.phase
        )),
    }
}

fn decide_pending(
    operation: &DurableOperationStatus,
    pending: &PendingActionStatus,
    observations: &OperationObservations,
    now: i64,
) -> Result<Decision, String> {
    match observe_action(operation, pending, observations)? {
        ActionObservation::Postcondition => {
            let mut next = operation.clone();
            next.pending_action = None;
            next.last_error = None;
            next.phase_deadline_unix_seconds = now + ACTION_DEADLINE_SECONDS;
            match pending.kind {
                DurableActionKind::RevokeWrite => next.phase = DurableOperationPhase::CaptureLsn,
                DurableActionKind::DemoteOldPrimary => {
                    next.phase = DurableOperationPhase::PromoteTarget
                }
                DurableActionKind::PromoteTarget => {
                    next.phase = DurableOperationPhase::DistributeEpoch;
                    next.next_secondary_index = 0;
                }
                DurableActionKind::UpdateSecondaryEpoch => {
                    next.next_secondary_index += 1;
                }
                DurableActionKind::UpdateCatchUpConfiguration => {
                    next.phase = DurableOperationPhase::WaitForCatchUpQuorum
                }
                DurableActionKind::WaitForCatchUpQuorum => {
                    next.phase = DurableOperationPhase::UpdateCurrentConfiguration
                }
                DurableActionKind::UpdateCurrentConfiguration => {
                    next.phase = DurableOperationPhase::LabelTargetPrimary
                }
                DurableActionKind::LabelTargetPrimary => {
                    next.phase = DurableOperationPhase::LabelOldSecondary
                }
                DurableActionKind::LabelOldSecondary => {
                    next.phase = DurableOperationPhase::Finalize
                }
                DurableActionKind::RestorePreviousConfiguration => {
                    next.phase = DurableOperationPhase::Failed
                }
                DurableActionKind::CompensatePromoteOldPrimary => {
                    next.phase = DurableOperationPhase::CompensateDistributeEpoch;
                    next.next_secondary_index = 0;
                }
                DurableActionKind::CompensateUpdateSecondaryEpoch => {
                    next.next_secondary_index += 1;
                }
                DurableActionKind::CompensateCatchUpConfiguration => {
                    next.phase = DurableOperationPhase::CompensateCurrentConfiguration
                }
                DurableActionKind::CompensateCurrentConfiguration => {
                    next.phase = DurableOperationPhase::CompensateLabelOldPrimary;
                }
                DurableActionKind::CompensateLabelOldPrimary => {
                    next.phase = DurableOperationPhase::CompensateLabelTargetSecondary;
                }
                DurableActionKind::CompensateLabelTargetSecondary => {
                    next.phase = DurableOperationPhase::CompensateFinalize;
                }
                _ => return Err("non-switchover action reached switchover decision".to_string()),
            }
            Ok(Decision::Persist(next))
        }
        ActionObservation::Precondition => {
            if now >= pending.deadline_unix_seconds {
                return Ok(Decision::Persist(timeout_transition(
                    operation, pending, now,
                )));
            }
            if let Some(role) = pod_role_action(pending.kind) {
                Ok(Decision::PatchPodRole {
                    target_id: pending.target_id,
                    role: role.to_string(),
                })
            } else {
                let action = action_for(operation, pending, observations)?;
                Ok(Decision::Execute {
                    target_id: pending.target_id,
                    action_id: pending.action_id.clone(),
                    action,
                })
            }
        }
        ActionObservation::InProgress => {
            if now >= pending.deadline_unix_seconds {
                Ok(Decision::Persist(timeout_transition(
                    operation, pending, now,
                )))
            } else {
                Ok(Decision::Wait)
            }
        }
        ActionObservation::Failed(error) => {
            let recorded = record_activity_error(operation, &error);
            let mut next = timeout_transition(&recorded, pending, now);
            next.last_error = Some(error);
            Ok(Decision::Persist(next))
        }
        ActionObservation::Unavailable => {
            if now >= pending.deadline_unix_seconds {
                Ok(Decision::Persist(poison(
                    operation,
                    "durable action target was unavailable at its deadline",
                )))
            } else {
                Ok(Decision::Wait)
            }
        }
        ActionObservation::Impossible => Ok(Decision::Persist(poison(
            operation,
            "live state matches neither the durable action precondition nor postcondition",
        ))),
    }
}

fn observe_action(
    operation: &DurableOperationStatus,
    pending: &PendingActionStatus,
    observations: &OperationObservations,
) -> Result<ActionObservation, String> {
    let Some(observation) = observations.get(&pending.target_id) else {
        return Ok(ActionObservation::Unavailable);
    };
    if observation.status.instance_id.as_str() != pending.target_instance_id {
        return Ok(ActionObservation::Impossible);
    }

    if let Some(action) =
        super::correlated_action_observation(&observation.status, &pending.action_id)
    {
        if pod_role_action(pending.kind).is_some() {
            return Ok(ActionObservation::Impossible);
        }
        let expected_action = action_for(operation, pending, observations)?;
        let expected_signature = super::pending_action_signature(pending, &expected_action)?;
        if action.signature != expected_signature {
            return Ok(ActionObservation::Impossible);
        }
        return Ok(match action.state {
            DurableActionState::Scheduled | DurableActionState::InProgress => {
                ActionObservation::InProgress
            }
            DurableActionState::Completed => ActionObservation::Postcondition,
            DurableActionState::Failed => ActionObservation::Failed(
                action
                    .error
                    .clone()
                    .unwrap_or_else(|| "runtime reported durable activity failure".to_string()),
            ),
        });
    }

    let previous_epoch = epoch(&operation.previous_snapshot.epoch);
    let target_epoch = epoch(&operation.target_snapshot.epoch);
    let status = &observation.status;
    let result = match pending.kind {
        DurableActionKind::RevokeWrite => {
            if status.role == Role::Primary
                && status.epoch == previous_epoch
                && status.write_status == AccessStatus::ReconfigurationPending
            {
                ActionObservation::Postcondition
            } else if status.role == Role::Primary
                && status.epoch == previous_epoch
                && status.write_status == AccessStatus::Granted
            {
                ActionObservation::Precondition
            } else {
                ActionObservation::Impossible
            }
        }
        DurableActionKind::DemoteOldPrimary => {
            if status.role == Role::ActiveSecondary && status.epoch == target_epoch {
                ActionObservation::Postcondition
            } else if status.role == Role::Primary
                && status.epoch == previous_epoch
                && status.write_status == AccessStatus::ReconfigurationPending
            {
                ActionObservation::Precondition
            } else {
                ActionObservation::Impossible
            }
        }
        DurableActionKind::PromoteTarget => {
            if status.role == Role::Primary && status.epoch == target_epoch {
                ActionObservation::Postcondition
            } else if status.role == Role::ActiveSecondary && status.epoch == previous_epoch {
                ActionObservation::Precondition
            } else {
                ActionObservation::Impossible
            }
        }
        DurableActionKind::UpdateSecondaryEpoch => {
            if status.role == Role::ActiveSecondary && status.epoch == target_epoch {
                ActionObservation::Postcondition
            } else if status.role == Role::ActiveSecondary && status.epoch == previous_epoch {
                ActionObservation::Precondition
            } else {
                ActionObservation::Impossible
            }
        }
        DurableActionKind::CompensateUpdateSecondaryEpoch => {
            if status.role == Role::ActiveSecondary && status.epoch == target_epoch {
                ActionObservation::Postcondition
            } else if status.role == Role::ActiveSecondary && status.epoch == previous_epoch {
                ActionObservation::Precondition
            } else {
                ActionObservation::Impossible
            }
        }
        DurableActionKind::UpdateCatchUpConfiguration => {
            let target = target_config(operation, observations)?;
            configuration_observation(
                status,
                &ReplicaConfigurationStatus::from_config(
                    ReplicaConfigurationMode::CatchUp,
                    &target,
                ),
                None,
                true,
                target_epoch,
            )
        }
        DurableActionKind::WaitForCatchUpQuorum => {
            if status.role == Role::Primary && status.epoch == target_epoch {
                ActionObservation::Precondition
            } else {
                ActionObservation::Impossible
            }
        }
        DurableActionKind::UpdateCurrentConfiguration => {
            let target = target_config(operation, observations)?;
            configuration_observation(
                status,
                &ReplicaConfigurationStatus::from_config(
                    ReplicaConfigurationMode::Current,
                    &target,
                ),
                Some(&ReplicaConfigurationStatus::from_config(
                    ReplicaConfigurationMode::CatchUp,
                    &target,
                )),
                false,
                target_epoch,
            )
        }
        DurableActionKind::RestorePreviousConfiguration => {
            let previous = previous_config(operation, observations)?;
            let expected = ReplicaConfigurationStatus::from_config(
                ReplicaConfigurationMode::Current,
                &previous,
            );
            if status.role != Role::Primary || status.epoch != previous_epoch {
                ActionObservation::Impossible
            } else if status.write_status == AccessStatus::Granted
                && status.configuration.as_ref() == Some(&expected)
            {
                ActionObservation::Postcondition
            } else if status.write_status == AccessStatus::ReconfigurationPending
                && status.configuration.as_ref() == Some(&expected)
            {
                ActionObservation::Precondition
            } else {
                ActionObservation::Impossible
            }
        }
        DurableActionKind::LabelTargetPrimary
        | DurableActionKind::LabelOldSecondary
        | DurableActionKind::CompensateLabelOldPrimary
        | DurableActionKind::CompensateLabelTargetSecondary => {
            let expected = pod_role_action(pending.kind).unwrap();
            let expected_role = match pending.kind {
                DurableActionKind::LabelTargetPrimary
                | DurableActionKind::CompensateLabelOldPrimary => Role::Primary,
                DurableActionKind::LabelOldSecondary
                | DurableActionKind::CompensateLabelTargetSecondary => Role::ActiveSecondary,
                _ => unreachable!(),
            };
            if status.epoch != target_epoch || status.role != expected_role {
                ActionObservation::Impossible
            } else if observation.pod_role_label.as_deref() == Some(expected) {
                ActionObservation::Postcondition
            } else {
                ActionObservation::Precondition
            }
        }
        DurableActionKind::CompensatePromoteOldPrimary => {
            if status.role == Role::Primary && status.epoch == target_epoch {
                ActionObservation::Postcondition
            } else if status.role == Role::ActiveSecondary && status.epoch == target_epoch {
                ActionObservation::Precondition
            } else {
                ActionObservation::Impossible
            }
        }
        DurableActionKind::CompensateCatchUpConfiguration => {
            let compensation = compensation_config(operation, observations)?;
            let previous = previous_config(operation, observations)?;
            configuration_observation(
                status,
                &ReplicaConfigurationStatus::from_config(
                    ReplicaConfigurationMode::CatchUp,
                    &compensation,
                ),
                Some(&ReplicaConfigurationStatus::from_config(
                    ReplicaConfigurationMode::Current,
                    &previous,
                )),
                false,
                target_epoch,
            )
        }
        DurableActionKind::CompensateCurrentConfiguration => {
            let compensation = compensation_config(operation, observations)?;
            configuration_observation(
                status,
                &ReplicaConfigurationStatus::from_config(
                    ReplicaConfigurationMode::Current,
                    &compensation,
                ),
                Some(&ReplicaConfigurationStatus::from_config(
                    ReplicaConfigurationMode::CatchUp,
                    &compensation,
                )),
                false,
                target_epoch,
            )
        }
        _ => return Err("non-switchover action reached switchover observation".to_string()),
    };
    Ok(result)
}

fn configuration_observation(
    status: &ReplicaStatusInfo,
    expected: &ReplicaConfigurationStatus,
    precondition: Option<&ReplicaConfigurationStatus>,
    allow_none_precondition: bool,
    expected_epoch: Epoch,
) -> ActionObservation {
    if status.role != Role::Primary || status.epoch != expected_epoch {
        return ActionObservation::Impossible;
    }
    if status.configuration.as_ref() == Some(expected) {
        ActionObservation::Postcondition
    } else if status.configuration.as_ref() == precondition
        || (allow_none_precondition && status.configuration.is_none())
    {
        ActionObservation::Precondition
    } else {
        ActionObservation::Impossible
    }
}

fn action_for(
    operation: &DurableOperationStatus,
    pending: &PendingActionStatus,
    observations: &OperationObservations,
) -> Result<DurableReplicaAction, String> {
    let target_epoch = epoch(&operation.target_snapshot.epoch);
    Ok(match pending.kind {
        DurableActionKind::RevokeWrite => DurableReplicaAction::RevokeWriteStatus,
        DurableActionKind::DemoteOldPrimary => DurableReplicaAction::ChangeRole {
            epoch: target_epoch,
            role: Role::ActiveSecondary,
        },
        DurableActionKind::PromoteTarget | DurableActionKind::CompensatePromoteOldPrimary => {
            DurableReplicaAction::ChangeRole {
                epoch: target_epoch,
                role: Role::Primary,
            }
        }
        DurableActionKind::UpdateSecondaryEpoch => DurableReplicaAction::UpdateEpoch {
            epoch: target_epoch,
        },
        DurableActionKind::CompensateUpdateSecondaryEpoch => DurableReplicaAction::UpdateEpoch {
            epoch: target_epoch,
        },
        DurableActionKind::UpdateCatchUpConfiguration => {
            DurableReplicaAction::UpdateCatchUpConfiguration {
                current: target_config(operation, observations)?,
                previous: previous_config(operation, observations)?,
            }
        }
        DurableActionKind::WaitForCatchUpQuorum => DurableReplicaAction::WaitForCatchUpQuorum {
            mode: ReplicaSetQuorumMode::Write,
        },
        DurableActionKind::UpdateCurrentConfiguration => {
            DurableReplicaAction::UpdateCurrentConfiguration {
                current: target_config(operation, observations)?,
            }
        }
        DurableActionKind::RestorePreviousConfiguration => {
            DurableReplicaAction::UpdateCurrentConfiguration {
                current: previous_config(operation, observations)?,
            }
        }
        DurableActionKind::CompensateCatchUpConfiguration => {
            DurableReplicaAction::UpdateCatchUpConfiguration {
                current: compensation_config(operation, observations)?,
                previous: previous_config(operation, observations)?,
            }
        }
        DurableActionKind::CompensateCurrentConfiguration => {
            DurableReplicaAction::UpdateCurrentConfiguration {
                current: compensation_config(operation, observations)?,
            }
        }
        DurableActionKind::LabelTargetPrimary
        | DurableActionKind::LabelOldSecondary
        | DurableActionKind::CompensateLabelOldPrimary
        | DurableActionKind::CompensateLabelTargetSecondary => {
            return Err("pod label actions are executed by the cluster API".to_string());
        }
        _ => return Err("non-switchover action reached switchover action mapping".to_string()),
    })
}

fn pending_action(
    operation: &DurableOperationStatus,
    sequence: u32,
    kind: DurableActionKind,
    now: i64,
) -> Result<PendingActionStatus, String> {
    let target_id = match kind {
        DurableActionKind::RevokeWrite
        | DurableActionKind::DemoteOldPrimary
        | DurableActionKind::CompensatePromoteOldPrimary
        | DurableActionKind::CompensateCatchUpConfiguration
        | DurableActionKind::CompensateCurrentConfiguration => operation.old_primary_id,
        DurableActionKind::RestorePreviousConfiguration => operation.old_primary_id,
        DurableActionKind::PromoteTarget
        | DurableActionKind::UpdateCatchUpConfiguration
        | DurableActionKind::WaitForCatchUpQuorum
        | DurableActionKind::UpdateCurrentConfiguration => operation.target_primary_id,
        DurableActionKind::LabelTargetPrimary
        | DurableActionKind::CompensateLabelTargetSecondary => operation.target_primary_id,
        DurableActionKind::LabelOldSecondary | DurableActionKind::CompensateLabelOldPrimary => {
            operation.old_primary_id
        }
        DurableActionKind::UpdateSecondaryEpoch => {
            epoch_distribution_ids(operation)[operation.next_secondary_index as usize]
        }
        DurableActionKind::CompensateUpdateSecondaryEpoch => {
            compensation_epoch_distribution_ids(operation)[operation.next_secondary_index as usize]
        }
        _ => return Err("non-switchover action cannot be scheduled".to_string()),
    };
    let member = operation
        .previous_snapshot
        .members
        .iter()
        .find(|member| member.id == target_id)
        .ok_or_else(|| format!("pending action target {target_id} is not a snapshot member"))?;
    let expected_epoch = match kind {
        DurableActionKind::RevokeWrite
        | DurableActionKind::PromoteTarget
        | DurableActionKind::RestorePreviousConfiguration => {
            operation.previous_snapshot.epoch.clone()
        }
        _ => operation.target_snapshot.epoch.clone(),
    };
    let postcondition = match kind {
        DurableActionKind::RevokeWrite => DurablePostconditionStatus {
            kind: DurablePostconditionKind::WriteRevoked,
            role: None,
        },
        DurableActionKind::DemoteOldPrimary
        | DurableActionKind::UpdateSecondaryEpoch
        | DurableActionKind::CompensateUpdateSecondaryEpoch => DurablePostconditionStatus {
            kind: if matches!(kind, DurableActionKind::UpdateSecondaryEpoch) {
                DurablePostconditionKind::Epoch
            } else {
                DurablePostconditionKind::Role
            },
            role: Some(StableReplicaRoleStatus::ActiveSecondary),
        },
        DurableActionKind::PromoteTarget | DurableActionKind::CompensatePromoteOldPrimary => {
            DurablePostconditionStatus {
                kind: DurablePostconditionKind::Role,
                role: Some(StableReplicaRoleStatus::Primary),
            }
        }
        DurableActionKind::UpdateCatchUpConfiguration
        | DurableActionKind::CompensateCatchUpConfiguration => DurablePostconditionStatus {
            kind: DurablePostconditionKind::CatchUpConfiguration,
            role: None,
        },
        DurableActionKind::WaitForCatchUpQuorum => DurablePostconditionStatus {
            kind: DurablePostconditionKind::CatchUpQuorum,
            role: None,
        },
        DurableActionKind::UpdateCurrentConfiguration
        | DurableActionKind::RestorePreviousConfiguration
        | DurableActionKind::CompensateCurrentConfiguration => DurablePostconditionStatus {
            kind: DurablePostconditionKind::CurrentConfiguration,
            role: None,
        },
        DurableActionKind::LabelTargetPrimary
        | DurableActionKind::LabelOldSecondary
        | DurableActionKind::CompensateLabelOldPrimary
        | DurableActionKind::CompensateLabelTargetSecondary => DurablePostconditionStatus {
            kind: DurablePostconditionKind::PodRoleLabel,
            role: None,
        },
        _ => return Err("non-switchover action has no switchover postcondition".to_string()),
    };
    Ok(PendingActionStatus {
        action_id: action_id(operation, sequence, kind),
        sequence,
        kind,
        target_id,
        target_instance_id: member.instance_id.clone(),
        expected_epoch,
        desired_postcondition: postcondition,
        attempts: 0,
        deadline_unix_seconds: now + ACTION_DEADLINE_SECONDS,
        last_error: None,
        dispatch_authorized: false,
        dispatch_agent_generation: None,
        dispatch_agent_control_version: None,
        dispatch_observed_runtime_epoch: None,
        dispatch_action_payload: String::new(),
    })
}

fn action_id(operation: &DurableOperationStatus, sequence: u32, kind: DurableActionKind) -> String {
    format!("{}:{sequence}:{kind:?}", operation.execution_id)
}

fn with_pending(
    operation: &DurableOperationStatus,
    pending: PendingActionStatus,
) -> DurableOperationStatus {
    let mut next = operation.clone();
    next.pending_action = Some(pending);
    next
}

fn timeout_transition(
    operation: &DurableOperationStatus,
    pending: &PendingActionStatus,
    now: i64,
) -> DurableOperationStatus {
    let mut next = operation.clone();
    next.pending_action = None;
    next.phase_deadline_unix_seconds = now + ACTION_DEADLINE_SECONDS;
    next.last_error = Some(format!(
        "durable action {:?} reached its deadline",
        pending.kind
    ));
    next.phase = match pending.kind {
        DurableActionKind::RevokeWrite => DurableOperationPhase::Failed,
        DurableActionKind::DemoteOldPrimary => DurableOperationPhase::RestorePreviousConfiguration,
        DurableActionKind::PromoteTarget => DurableOperationPhase::CompensatePromoteOldPrimary,
        DurableActionKind::UpdateSecondaryEpoch
        | DurableActionKind::CompensateUpdateSecondaryEpoch
        | DurableActionKind::UpdateCatchUpConfiguration
        | DurableActionKind::WaitForCatchUpQuorum
        | DurableActionKind::UpdateCurrentConfiguration => DurableOperationPhase::Poisoned,
        DurableActionKind::RestorePreviousConfiguration => DurableOperationPhase::Poisoned,
        DurableActionKind::LabelTargetPrimary | DurableActionKind::LabelOldSecondary => {
            DurableOperationPhase::Poisoned
        }
        DurableActionKind::CompensatePromoteOldPrimary
        | DurableActionKind::CompensateCatchUpConfiguration
        | DurableActionKind::CompensateCurrentConfiguration => DurableOperationPhase::Poisoned,
        DurableActionKind::CompensateLabelOldPrimary
        | DurableActionKind::CompensateLabelTargetSecondary => DurableOperationPhase::Poisoned,
        _ => DurableOperationPhase::Poisoned,
    };
    next
}

fn exact_observation<'a>(
    operation: &DurableOperationStatus,
    observations: &'a OperationObservations,
    id: i64,
    expected_epoch: Epoch,
) -> Result<&'a ReplicaObservation, String> {
    let observation = observations
        .get(&id)
        .ok_or_else(|| format!("replica {id} is unavailable"))?;
    let member = operation
        .previous_snapshot
        .members
        .iter()
        .find(|member| member.id == id)
        .ok_or_else(|| format!("replica {id} is not in the operation snapshot"))?;
    if observation.status.instance_id.as_str() != member.instance_id {
        return Err(format!("replica {id} incarnation changed"));
    }
    if observation.status.epoch != expected_epoch {
        return Err(format!("replica {id} epoch is stale or unexpected"));
    }
    Ok(observation)
}

fn validate_snapshot_observation(
    operation: &DurableOperationStatus,
    snapshot: &StablePartitionSnapshotStatus,
    observations: &OperationObservations,
) -> Result<(), String> {
    let expected_epoch = epoch(&snapshot.epoch);
    for member in &snapshot.members {
        let observation = exact_observation(operation, observations, member.id, expected_epoch)?;
        let expected_role = role(member.role);
        if observation.status.role != expected_role {
            return Err(format!(
                "replica {} has role {:?}, expected {:?}",
                member.id, observation.status.role, expected_role
            ));
        }
        let expected_label = if member.id == snapshot.primary_id {
            "primary"
        } else {
            "secondary"
        };
        if observation.pod_role_label.as_deref() != Some(expected_label) {
            return Err(format!(
                "replica {} pod role label is not {expected_label}",
                member.id
            ));
        }
    }
    let primary = observations
        .get(&snapshot.primary_id)
        .ok_or_else(|| format!("snapshot primary {} is unavailable", snapshot.primary_id))?;
    let config = config_for_snapshot(snapshot, observations)?;
    let expected_configuration =
        ReplicaConfigurationStatus::from_config(ReplicaConfigurationMode::Current, &config);
    if primary.status.configuration.as_ref() != Some(&expected_configuration) {
        return Err("snapshot primary current configuration is not durable target".to_string());
    }
    Ok(())
}

fn validate_operation(operation: &DurableOperationStatus) -> Result<(), String> {
    if operation.version != DURABLE_OPERATION_VERSION {
        return Err(format!(
            "unsupported durable operation version {}",
            operation.version
        ));
    }
    if operation.kind != DurableOperationKind::Switchover {
        return Err("unsupported durable operation kind".to_string());
    }
    if operation.previous_snapshot.is_none() {
        return Err("switchover operation has no previous stable snapshot".to_string());
    }
    validate_snapshot(&operation.previous_snapshot)?;
    validate_snapshot(&operation.target_snapshot)?;
    if operation.old_primary_id != operation.previous_snapshot.primary_id
        || operation.target_primary_id != operation.target_snapshot.primary_id
    {
        return Err("operation primary identities conflict with snapshots".to_string());
    }
    Ok(())
}

fn validate_pending_action(
    operation: &DurableOperationStatus,
    pending: &PendingActionStatus,
) -> Result<(), String> {
    if !action_allowed_in_phase(operation.phase, pending.kind) {
        return Err(format!(
            "pending action {:?} is illegal in phase {:?}",
            pending.kind, operation.phase
        ));
    }
    if pending.action_id != action_id(operation, pending.sequence, pending.kind) {
        return Err("pending action ID is not deterministic for this execution".to_string());
    }
    let member = operation
        .previous_snapshot
        .members
        .iter()
        .find(|member| member.id == pending.target_id)
        .ok_or_else(|| "pending action target is not in the operation snapshot".to_string())?;
    if member.instance_id != pending.target_instance_id {
        return Err("pending action target incarnation conflicts with snapshot".to_string());
    }
    let expected = pending_action(operation, pending.sequence, pending.kind, 0)?;
    if pending.target_id != expected.target_id
        || pending.expected_epoch != expected.expected_epoch
        || pending.desired_postcondition != expected.desired_postcondition
    {
        return Err("pending action target or postcondition is not deterministic".to_string());
    }
    Ok(())
}

fn action_allowed_in_phase(phase: DurableOperationPhase, kind: DurableActionKind) -> bool {
    matches!(
        (phase, kind),
        (
            DurableOperationPhase::Revoke,
            DurableActionKind::RevokeWrite
        ) | (
            DurableOperationPhase::DemoteOldPrimary,
            DurableActionKind::DemoteOldPrimary
        ) | (
            DurableOperationPhase::PromoteTarget,
            DurableActionKind::PromoteTarget
        ) | (
            DurableOperationPhase::DistributeEpoch,
            DurableActionKind::UpdateSecondaryEpoch
        ) | (
            DurableOperationPhase::UpdateCatchUpConfiguration,
            DurableActionKind::UpdateCatchUpConfiguration
        ) | (
            DurableOperationPhase::WaitForCatchUpQuorum,
            DurableActionKind::WaitForCatchUpQuorum
        ) | (
            DurableOperationPhase::UpdateCurrentConfiguration,
            DurableActionKind::UpdateCurrentConfiguration
        ) | (
            DurableOperationPhase::LabelTargetPrimary,
            DurableActionKind::LabelTargetPrimary
        ) | (
            DurableOperationPhase::LabelOldSecondary,
            DurableActionKind::LabelOldSecondary
        ) | (
            DurableOperationPhase::RestorePreviousConfiguration,
            DurableActionKind::RestorePreviousConfiguration
        ) | (
            DurableOperationPhase::CompensatePromoteOldPrimary,
            DurableActionKind::CompensatePromoteOldPrimary
        ) | (
            DurableOperationPhase::CompensateDistributeEpoch,
            DurableActionKind::CompensateUpdateSecondaryEpoch
        ) | (
            DurableOperationPhase::CompensateCatchUpConfiguration,
            DurableActionKind::CompensateCatchUpConfiguration
        ) | (
            DurableOperationPhase::CompensateCurrentConfiguration,
            DurableActionKind::CompensateCurrentConfiguration
        ) | (
            DurableOperationPhase::CompensateLabelOldPrimary,
            DurableActionKind::CompensateLabelOldPrimary
        ) | (
            DurableOperationPhase::CompensateLabelTargetSecondary,
            DurableActionKind::CompensateLabelTargetSecondary
        )
    )
}

fn validate_snapshot(snapshot: &StablePartitionSnapshotStatus) -> Result<(), String> {
    let mut ids = BTreeMap::new();
    let mut primary_count = 0;
    for member in &snapshot.members {
        if member.instance_id.is_empty() {
            return Err(format!("replica {} has an empty incarnation", member.id));
        }
        if ids.insert(member.id, ()).is_some() {
            return Err(format!("duplicate replica ID {}", member.id));
        }
        if member.role == StableReplicaRoleStatus::Primary {
            primary_count += 1;
        }
    }
    if primary_count != 1
        || !snapshot.members.iter().any(|member| {
            member.id == snapshot.primary_id && member.role == StableReplicaRoleStatus::Primary
        })
    {
        return Err("snapshot must contain exactly one matching primary".to_string());
    }
    let expected_quorum = snapshot.members.len() as u32 / 2 + 1;
    if snapshot.write_quorum != expected_quorum {
        return Err("snapshot write quorum is not a majority".to_string());
    }
    Ok(())
}

fn epoch(value: &EpochStatus) -> Epoch {
    Epoch::new(value.data_loss_number, value.configuration_number)
}

fn role(value: StableReplicaRoleStatus) -> Role {
    match value {
        StableReplicaRoleStatus::Primary => Role::Primary,
        StableReplicaRoleStatus::ActiveSecondary => Role::ActiveSecondary,
    }
}

fn epoch_distribution_ids(operation: &DurableOperationStatus) -> Vec<i64> {
    let mut ids = operation
        .previous_snapshot
        .members
        .iter()
        .filter(|member| {
            member.id != operation.old_primary_id && member.id != operation.target_primary_id
        })
        .map(|member| member.id)
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

fn compensation_epoch_distribution_ids(operation: &DurableOperationStatus) -> Vec<i64> {
    let mut ids = operation
        .previous_snapshot
        .members
        .iter()
        .filter(|member| member.id != operation.old_primary_id)
        .map(|member| member.id)
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

fn previous_config(
    operation: &DurableOperationStatus,
    observations: &OperationObservations,
) -> Result<ReplicaSetConfig, String> {
    config_for_snapshot(&operation.previous_snapshot, observations)
}

fn target_config(
    operation: &DurableOperationStatus,
    observations: &OperationObservations,
) -> Result<ReplicaSetConfig, String> {
    config_for_snapshot(&operation.target_snapshot, observations)
}

fn compensation_config(
    operation: &DurableOperationStatus,
    observations: &OperationObservations,
) -> Result<ReplicaSetConfig, String> {
    config_for_snapshot(&compensation_snapshot(operation), observations)
}

fn config_for_snapshot(
    snapshot: &StablePartitionSnapshotStatus,
    observations: &OperationObservations,
) -> Result<ReplicaSetConfig, String> {
    let mut members = Vec::new();
    for member in &snapshot.members {
        if member.id == snapshot.primary_id {
            continue;
        }
        let observation = observations
            .get(&member.id)
            .ok_or_else(|| format!("replica {} is unavailable", member.id))?;
        if observation.status.instance_id.as_str() != member.instance_id {
            return Err(format!("replica {} incarnation changed", member.id));
        }
        members.push(ReplicaInfo {
            id: member.id,
            instance_id: ReplicaInstanceId::new(member.instance_id.clone()),
            role: Role::ActiveSecondary,
            status: ReplicaStatus::Up,
            replicator_address: observation.replicator_address.clone(),
            current_progress: 0,
            catch_up_capability: 0,
            must_catch_up: false,
        });
    }
    members.sort_by_key(|member| member.id);
    Ok(ReplicaSetConfig {
        members,
        write_quorum: snapshot.write_quorum,
    })
}

fn compensation_snapshot(operation: &DurableOperationStatus) -> StablePartitionSnapshotStatus {
    let mut snapshot = operation.previous_snapshot.cloned().unwrap();
    snapshot.epoch = operation.target_snapshot.epoch.clone();
    snapshot
}

fn pod_role_action(kind: DurableActionKind) -> Option<&'static str> {
    match kind {
        DurableActionKind::LabelTargetPrimary | DurableActionKind::CompensateLabelOldPrimary => {
            Some("primary")
        }
        DurableActionKind::LabelOldSecondary
        | DurableActionKind::CompensateLabelTargetSecondary => Some("secondary"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{StableReplicaRoleStatus, StableReplicaSnapshotStatus};

    fn snapshot() -> StablePartitionSnapshotStatus {
        StablePartitionSnapshotStatus {
            epoch: EpochStatus {
                data_loss_number: 1,
                configuration_number: 4,
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
            ],
            write_quorum: 2,
        }
    }

    #[test]
    fn durable_action_ids_are_deterministic() {
        let operation = start_switchover("set", snapshot(), 2, 100).unwrap();
        let pending = pending_action(&operation, 1, DurableActionKind::RevokeWrite, 100).unwrap();
        assert_eq!(
            pending.action_id,
            format!("{}:1:RevokeWrite", operation.execution_id)
        );
    }

    #[test]
    fn unsupported_version_is_rejected() {
        let mut operation = start_switchover("set", snapshot(), 2, 100).unwrap();
        operation.version += 1;
        assert!(
            decide(&operation, &BTreeMap::new(), 100)
                .unwrap_err()
                .contains("unsupported durable operation version")
        );
    }

    #[test]
    fn illegal_pending_action_is_rejected() {
        let mut operation = start_switchover("set", snapshot(), 2, 100).unwrap();
        operation.pending_action =
            Some(pending_action(&operation, 3, DurableActionKind::PromoteTarget, 100).unwrap());
        assert!(
            decide(&operation, &BTreeMap::new(), 100)
                .unwrap_err()
                .contains("illegal in phase")
        );
    }

    #[test]
    fn stale_epoch_observation_poisoned_without_execute_decision() {
        let mut operation = start_switchover("set", snapshot(), 2, 100).unwrap();
        operation = match decide(&operation, &BTreeMap::new(), 100).unwrap() {
            Decision::Persist(operation) => operation,
            other => panic!("unexpected decision: {other:?}"),
        };
        let mut observations = BTreeMap::new();
        observations.insert(
            1,
            ReplicaObservation {
                status: ReplicaStatusInfo {
                    instance_id: ReplicaInstanceId::new("one"),
                    role: Role::Primary,
                    epoch: Epoch::new(1, 5),
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
                control_address: "http://one-control".to_string(),
                replicator_address: "http://one".to_string(),
                pod_name: "set-0".to_string(),
                pod_role_label: Some("primary".to_string()),
            },
        );
        let decision = decide(&operation, &observations, 100).unwrap();
        match decision {
            Decision::Persist(operation) => {
                assert_eq!(operation.phase, DurableOperationPhase::Poisoned)
            }
            other => panic!("stale epoch produced unsafe decision: {other:?}"),
        }
    }

    #[test]
    fn activity_errors_are_bounded_and_counted() {
        let mut operation = start_switchover("set", snapshot(), 2, 100).unwrap();
        operation.pending_action =
            Some(pending_action(&operation, 1, DurableActionKind::RevokeWrite, 100).unwrap());
        let next = record_activity_error(&operation, &"x".repeat(600));
        assert_eq!(next.pending_action.unwrap().attempts, 1);
        assert_eq!(next.last_error.unwrap().len(), MAX_ERROR_LENGTH);
    }
}
