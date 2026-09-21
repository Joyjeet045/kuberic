use std::collections::BTreeMap;

use kuberic_core::driver::ReplicaHandle;
use kuberic_core::types::{
    DataLossAction, DurableActionResult, DurableActionState, DurableReplicaAction, Epoch,
    ReplicaConfigurationMemberStatus, ReplicaConfigurationMode, ReplicaConfigurationStatus,
    ReplicaElectionConfiguration, ReplicaInfo, ReplicaSetConfig, ReplicaSetQuorumMode,
    ReplicaStatus, Role, StablePartitionSnapshot,
};

use crate::crd::{
    DURABLE_OPERATION_VERSION, DurableActionKind, DurableDataLossResultStatus,
    DurableFailoverStatus, DurableOperationKind, DurableOperationPhase, DurableOperationStatus,
    DurablePostconditionKind, DurablePostconditionStatus, EpochStatus, FailoverConfigurationStatus,
    FailoverMemberStatus, OptionalStablePartitionSnapshotStatus, PendingActionStatus,
    ReplicaElectionObservationStatus, StablePartitionSnapshotStatus,
    StableReplicaElectionMetadataStatus, StableReplicaRoleStatus,
};
use crate::durable::failover_election::{
    ElectionConfiguration, ElectionDecision, ElectionInput, ElectionMember, ElectionObservation,
    FailoverPlacementSnapshot, accepted_observations, evaluate_with_placement,
};
use crate::durable::{
    ACTION_DEADLINE_SECONDS, Decision, OperationObservations, bounded_error, poison,
};

const DATA_LOSS_ACTION_DEADLINE_SECONDS: i64 = 60;

pub fn start_failover(
    set_identity: &str,
    previous_snapshot: StablePartitionSnapshotStatus,
    failed_primary_id: i64,
    minimum_committed_replicas: usize,
    now: i64,
) -> Result<DurableOperationStatus, String> {
    start_failover_with_placement(
        set_identity,
        previous_snapshot,
        failed_primary_id,
        minimum_committed_replicas,
        now,
        None,
    )
}

/// Persist placement evidence before any election or failover action is dispatched.
pub fn start_failover_with_placement(
    set_identity: &str,
    previous_snapshot: StablePartitionSnapshotStatus,
    failed_primary_id: i64,
    minimum_committed_replicas: usize,
    now: i64,
    placement: Option<FailoverPlacementSnapshot>,
) -> Result<DurableOperationStatus, String> {
    validate_snapshot(&previous_snapshot)?;
    if let Some(placement) = &placement {
        placement.validate()?;
    }
    if previous_snapshot.primary_id != failed_primary_id {
        return Err(format!(
            "failed primary {failed_primary_id} differs from stable primary {}",
            previous_snapshot.primary_id
        ));
    }
    let current_configuration = FailoverConfigurationStatus {
        members: previous_snapshot
            .members
            .iter()
            .map(|member| FailoverMemberStatus {
                id: member.id,
                instance_id: member.instance_id.clone(),
                role: member.role,
                dropped: member.id == failed_primary_id,
                last_known: member.election_metadata.clone(),
            })
            .collect(),
        write_quorum: previous_snapshot.write_quorum,
    };
    let operation_id = format!(
        "failover:{set_identity}:{}:{}:{}",
        previous_snapshot.epoch.data_loss_number,
        previous_snapshot.epoch.configuration_number,
        failed_primary_id
    );
    Ok(DurableOperationStatus {
        operation_id: operation_id.clone(),
        execution_id: format!("{operation_id}:execution"),
        version: DURABLE_OPERATION_VERSION,
        kind: DurableOperationKind::Failover,
        phase: DurableOperationPhase::FailoverRecordStartingConfiguration,
        previous_snapshot: OptionalStablePartitionSnapshotStatus(Some(previous_snapshot.clone())),
        target_snapshot: previous_snapshot,
        committed_snapshot: None,
        minimum_committed_replicas: Some(minimum_committed_replicas.max(1) as u32),
        old_primary_id: failed_primary_id,
        target_primary_id: failed_primary_id,
        add_mode: None,
        remove_mode: None,
        target_replica_id: None,
        target_instance_id: None,
        target_pod_name: None,
        target_pod_uid: None,
        remove_target_replicator_address: None,
        remove_target_agent_generation: None,
        retired_instance_id: None,
        frozen_lsn: None,
        next_secondary_index: 0,
        phase_deadline_unix_seconds: now + ACTION_DEADLINE_SECONDS,
        pending_action: None,
        last_error: None,
        failover: Some(DurableFailoverStatus {
            placement,
            previous_configuration: None,
            current_configuration,
            observations: Vec::new(),
            unavailable_replicas: Vec::new(),
            final_attestations: Vec::new(),
            target_confirmed: false,
            data_loss_required: false,
            configuration_epoch_intent: None,
            data_loss_epoch_intent: None,
            data_loss_result: None,
            next_configuration_index: 0,
            next_secondary_index: 0,
            next_label_index: 0,
            next_attestation_index: 0,
            next_unavailable_index: 0,
            assessment: None,
            promotion_committed: false,
        }),
        add_intent: None,
        remove_intent: None,
        remove_commit_evidence: None,
        remove_cleanup: None,
        removal_disposition: None,
    })
}

pub fn decide_failover(
    operation: &DurableOperationStatus,
    observations: &OperationObservations,
    now: i64,
) -> Result<Decision, String> {
    validate_operation(operation)?;
    match operation.phase {
        DurableOperationPhase::FailoverRecordStartingConfiguration => {
            decide_record_configuration(operation, observations, now, true)
        }
        DurableOperationPhase::FailoverCollect => {
            if collection_complete(operation) {
                Ok(Decision::Persist(with_phase(
                    operation,
                    DurableOperationPhase::FailoverAssess,
                    now,
                )))
            } else {
                Ok(Decision::Wait)
            }
        }
        DurableOperationPhase::FailoverAssess => decide_assessment(operation, now),
        DurableOperationPhase::FailoverWaitForBestCandidate
        | DurableOperationPhase::FailoverWaitForReadQuorum => Ok(Decision::Wait),
        DurableOperationPhase::FailoverPersistConfigurationEpoch => {
            let mut next = operation.clone();
            let epoch = epoch(&operation.target_snapshot.epoch);
            let configuration_number = epoch
                .configuration_number
                .checked_add(1)
                .ok_or_else(|| "configuration epoch overflow".to_string())?;
            let intent = EpochStatus {
                data_loss_number: epoch.data_loss_number,
                configuration_number,
            };
            next.failover_mut()?.configuration_epoch_intent = Some(intent.clone());
            next.target_snapshot.epoch = intent;
            next.phase = if next.failover_ref()?.data_loss_required {
                DurableOperationPhase::FailoverPersistDataLossEpoch
            } else {
                DurableOperationPhase::FailoverApplyCandidateEpoch
            };
            next.phase_deadline_unix_seconds = now + ACTION_DEADLINE_SECONDS;
            Ok(Decision::Persist(next))
        }
        DurableOperationPhase::FailoverPersistDataLossEpoch => {
            let mut next = operation.clone();
            let configuration_intent = next
                .failover_ref()?
                .configuration_epoch_intent
                .clone()
                .ok_or_else(|| "data-loss intent has no configuration intent".to_string())?;
            let data_loss_number = configuration_intent
                .data_loss_number
                .checked_add(1)
                .ok_or_else(|| "data-loss epoch overflow".to_string())?;
            let intent = EpochStatus {
                data_loss_number,
                configuration_number: configuration_intent.configuration_number,
            };
            next.failover_mut()?.data_loss_epoch_intent = Some(intent.clone());
            next.target_snapshot.epoch = intent;
            next.phase = DurableOperationPhase::FailoverApplyCandidateEpoch;
            next.phase_deadline_unix_seconds = now + ACTION_DEADLINE_SECONDS;
            Ok(Decision::Persist(next))
        }
        DurableOperationPhase::FailoverApplyCandidateEpoch => decide_pending_action(
            operation,
            observations,
            now,
            DurableActionKind::FailoverUpdateCandidateEpoch,
            operation.target_primary_id,
            DurablePostconditionKind::Epoch,
            None,
            DurableOperationPhase::FailoverApplyCandidateEpoch,
            if operation.failover_ref()?.data_loss_required {
                DurableOperationPhase::FailoverNotifyDataLoss
            } else {
                DurableOperationPhase::FailoverPromoteCandidate
            },
        ),
        DurableOperationPhase::FailoverNotifyDataLoss => {
            decide_data_loss(operation, observations, now)
        }
        DurableOperationPhase::FailoverRefreshCandidate => Ok(Decision::Wait),
        DurableOperationPhase::FailoverPromoteCandidate => decide_pending_action(
            operation,
            observations,
            now,
            DurableActionKind::FailoverPromoteCandidate,
            operation.target_primary_id,
            DurablePostconditionKind::Role,
            Some(StableReplicaRoleStatus::Primary),
            DurableOperationPhase::FailoverPromoteCandidate,
            DurableOperationPhase::FailoverCommitPromotion,
        ),
        DurableOperationPhase::FailoverCommitPromotion => {
            let mut next = with_phase(
                operation,
                DurableOperationPhase::FailoverDistributeEpoch,
                now,
            );
            next.failover_mut()?.promotion_committed = true;
            next.failover_mut()?.next_secondary_index = 0;
            let snapshot = primary_only_snapshot(&operation.target_snapshot)?;
            next.committed_snapshot = Some(snapshot.clone());
            Ok(Decision::CommitSnapshot {
                operation: next,
                snapshot,
            })
        }
        DurableOperationPhase::FailoverDistributeEpoch => {
            decide_distribute_epoch(operation, observations, now)
        }
        DurableOperationPhase::FailoverCatchUpConfiguration => decide_pending_action(
            operation,
            observations,
            now,
            DurableActionKind::FailoverCatchUpConfiguration,
            operation.target_primary_id,
            DurablePostconditionKind::CatchUpConfiguration,
            None,
            DurableOperationPhase::FailoverCatchUpConfiguration,
            DurableOperationPhase::FailoverWaitForCatchUpQuorum,
        ),
        DurableOperationPhase::FailoverWaitForCatchUpQuorum => decide_pending_action(
            operation,
            observations,
            now,
            DurableActionKind::FailoverWaitForCatchUpQuorum,
            operation.target_primary_id,
            DurablePostconditionKind::CatchUpQuorum,
            None,
            DurableOperationPhase::FailoverWaitForCatchUpQuorum,
            DurableOperationPhase::FailoverCurrentConfiguration,
        ),
        DurableOperationPhase::FailoverCurrentConfiguration => decide_pending_action(
            operation,
            observations,
            now,
            DurableActionKind::FailoverCurrentConfiguration,
            operation.target_primary_id,
            DurablePostconditionKind::CurrentConfiguration,
            None,
            DurableOperationPhase::FailoverCurrentConfiguration,
            DurableOperationPhase::FailoverRecordElectionConfiguration,
        ),
        DurableOperationPhase::FailoverRecordElectionConfiguration => {
            decide_record_configuration(operation, observations, now, false)
        }
        DurableOperationPhase::FailoverLabelMembers => {
            decide_label_members(operation, observations, now)
        }
        DurableOperationPhase::FailoverAttest => {
            if final_attestation_complete(operation) {
                let snapshot = snapshot_from_attestations(operation)?;
                let mut completed = operation.clone();
                completed.phase = DurableOperationPhase::Completed;
                completed.pending_action = None;
                completed.target_snapshot = snapshot.clone();
                Ok(Decision::Complete {
                    operation: completed,
                    snapshot,
                    compensated: false,
                })
            } else {
                Ok(Decision::Wait)
            }
        }
        DurableOperationPhase::FailoverFinalize
        | DurableOperationPhase::Completed
        | DurableOperationPhase::Failed
        | DurableOperationPhase::Poisoned => Ok(Decision::Wait),
        phase => Err(format!("unsupported failover phase {phase:?}")),
    }
}

pub fn record_observation(
    operation: &DurableOperationStatus,
    replica_id: i64,
    status: &kuberic_core::types::ReplicaStatusInfo,
    final_attestation: bool,
    now: i64,
) -> Result<DurableOperationStatus, String> {
    let mut next = operation.clone();
    let target_snapshot = next.target_snapshot.clone();
    let failover = next.failover_mut()?;
    let member = failover
        .current_configuration
        .members
        .iter()
        .find(|member| member.id == replica_id)
        .or_else(|| {
            failover
                .previous_configuration
                .as_ref()?
                .members
                .iter()
                .find(|member| member.id == replica_id)
        })
        .ok_or_else(|| format!("replica {replica_id} is not in failover configuration"))?;
    let configuration_matches = if final_attestation {
        target_runtime_configuration_matches(
            status.election_configuration.as_ref(),
            &target_snapshot,
        )
    } else {
        runtime_configuration_matches(status.election_configuration.as_ref(), failover)
    };
    let observation = ReplicaElectionObservationStatus {
        id: replica_id,
        instance_id: status.instance_id.to_string(),
        epoch: EpochStatus {
            data_loss_number: status.epoch.data_loss_number,
            configuration_number: status.epoch.configuration_number,
        },
        role: role_name(status.role).to_string(),
        healthy: status.healthy,
        current_lsn: status.current_progress,
        committed_lsn: status.committed_lsn,
        first_retained_lsn: status.catch_up_capability,
        deactivation_epoch: status.deactivation_info.map(|info| EpochStatus {
            data_loss_number: info.epoch.data_loss_number,
            configuration_number: info.epoch.configuration_number,
        }),
        deactivation_catch_up_lsn: status.deactivation_info.map(|info| info.catch_up_lsn),
        configuration_matches,
    };
    if status.instance_id.as_str() != member.instance_id {
        return Err(format!(
            "replica {replica_id} incarnation changed from {} to {}",
            member.instance_id, status.instance_id
        ));
    }
    let target = if final_attestation {
        &mut failover.final_attestations
    } else {
        &mut failover.observations
    };
    target.retain(|existing| existing.id != replica_id);
    target.push(observation);
    target.sort_by_key(|observation| observation.id);
    failover
        .unavailable_replicas
        .retain(|unavailable| *unavailable != replica_id);
    next.phase = if final_attestation {
        DurableOperationPhase::FailoverAttest
    } else if operation.phase == DurableOperationPhase::FailoverCollect
        && !collection_complete(&next)
    {
        DurableOperationPhase::FailoverCollect
    } else {
        DurableOperationPhase::FailoverAssess
    };
    next.phase_deadline_unix_seconds = now + ACTION_DEADLINE_SECONDS;
    Ok(next)
}

pub fn adopt_replacement_before_confirmation(
    operation: &DurableOperationStatus,
    replica_id: i64,
    instance_id: &str,
    now: i64,
) -> Result<DurableOperationStatus, String> {
    if operation.failover_ref()?.target_confirmed {
        return Err("cannot adopt replacement after primary confirmation".to_string());
    }
    let mut next = operation.clone();
    let failover = next.failover_mut()?;
    for configuration in std::iter::once(&mut failover.current_configuration)
        .chain(failover.previous_configuration.iter_mut())
    {
        if let Some(member) = configuration
            .members
            .iter_mut()
            .find(|member| member.id == replica_id)
        {
            member.instance_id = instance_id.to_string();
            member.last_known = None;
        }
    }
    failover
        .observations
        .retain(|observation| observation.id != replica_id);
    failover
        .unavailable_replicas
        .retain(|unavailable| *unavailable != replica_id);
    failover.assessment = None;
    failover.next_configuration_index = 0;
    failover.next_unavailable_index = 0;
    next.pending_action = None;
    next.phase = DurableOperationPhase::FailoverRecordStartingConfiguration;
    next.phase_deadline_unix_seconds = now + ACTION_DEADLINE_SECONDS;
    Ok(next)
}

pub fn action_for(
    operation: &DurableOperationStatus,
    pending: &PendingActionStatus,
    handles: &BTreeMap<i64, Box<dyn ReplicaHandle>>,
) -> Result<DurableReplicaAction, String> {
    let target_epoch = epoch(&pending.expected_epoch);
    match pending.kind {
        DurableActionKind::FailoverRecordStartingConfiguration => {
            Ok(DurableReplicaAction::RecordElectionConfiguration {
                configuration: election_configuration(operation)?,
            })
        }
        DurableActionKind::FailoverRecordElectionConfiguration => {
            Ok(DurableReplicaAction::RecordElectionConfiguration {
                configuration: target_election_configuration(operation),
            })
        }
        DurableActionKind::FailoverUpdateCandidateEpoch
        | DurableActionKind::FailoverUpdateSecondaryEpoch => {
            Ok(DurableReplicaAction::UpdateEpoch {
                epoch: target_epoch,
            })
        }
        DurableActionKind::FailoverOnDataLoss => Ok(DurableReplicaAction::OnDataLoss {
            epoch: target_epoch,
        }),
        DurableActionKind::FailoverPromoteCandidate => Ok(DurableReplicaAction::ChangeRole {
            epoch: target_epoch,
            role: Role::Primary,
        }),
        DurableActionKind::FailoverCatchUpConfiguration => {
            Ok(DurableReplicaAction::UpdateCatchUpConfiguration {
                current: target_config(operation, handles, false)?,
                previous: committed_config(operation, handles)?,
            })
        }
        DurableActionKind::FailoverWaitForCatchUpQuorum => {
            Ok(DurableReplicaAction::WaitForCatchUpQuorum {
                mode: ReplicaSetQuorumMode::Write,
            })
        }
        DurableActionKind::FailoverCurrentConfiguration => {
            Ok(DurableReplicaAction::UpdateCurrentConfiguration {
                current: target_config(operation, handles, false)?,
            })
        }
        kind => Err(format!("unsupported failover action {kind:?}")),
    }
}

pub fn pending_label(operation: &DurableOperationStatus) -> Option<(i64, &'static str)> {
    let pending = operation.pending_action.as_ref()?;
    if pending.kind != DurableActionKind::FailoverLabelMember {
        return None;
    }
    Some((
        pending.target_id,
        if pending.target_id == operation.target_primary_id {
            "primary"
        } else {
            "secondary"
        },
    ))
}

fn decide_assessment(operation: &DurableOperationStatus, now: i64) -> Result<Decision, String> {
    let input = election_input(operation)?;
    match evaluate_with_placement(&input, operation.failover_ref()?.placement.as_ref())? {
        decision @ (ElectionDecision::Proceed { .. }
        | ElectionDecision::DataLossRequired { .. }) => {
            let data_loss_required = matches!(decision, ElectionDecision::DataLossRequired { .. });
            let (primary_id, members, write_quorum) =
                crate::durable::failover_election::build_target(&input, &decision, false)?;
            if members.len() < operation.minimum_committed_replicas.unwrap_or(1) as usize {
                return Err(format!(
                    "failover target has {} replicas below minimum {}",
                    members.len(),
                    operation.minimum_committed_replicas.unwrap_or(1)
                ));
            }
            let core = StablePartitionSnapshot {
                epoch: epoch(&operation.target_snapshot.epoch),
                primary_id,
                members,
                write_quorum,
            };
            let target = StablePartitionSnapshotStatus::try_from(&core)?;
            let mut next = operation.clone();
            next.target_primary_id = primary_id;
            next.target_snapshot = target;
            next.failover_mut()?.target_confirmed = true;
            next.failover_mut()?.data_loss_required = data_loss_required;
            next.failover_mut()?.assessment = Some(if data_loss_required {
                "read quorum unavailable; data-loss negotiation required".to_string()
            } else {
                "previous/current read quorum satisfied".to_string()
            });
            next.phase = DurableOperationPhase::FailoverPersistConfigurationEpoch;
            next.phase_deadline_unix_seconds = now + ACTION_DEADLINE_SECONDS;
            Ok(Decision::Persist(next))
        }
        ElectionDecision::WaitForBestCandidate {
            possible_candidates,
            ..
        } => {
            let mut next = with_phase(
                operation,
                DurableOperationPhase::FailoverWaitForBestCandidate,
                now,
            );
            next.failover_mut()?.assessment = Some(bounded_error(&format!(
                "waiting for possibly better replicas {possible_candidates:?}"
            )));
            Ok(Decision::Persist(next))
        }
        ElectionDecision::WaitForQuorum { .. } => {
            let mut next = with_phase(
                operation,
                DurableOperationPhase::FailoverWaitForReadQuorum,
                now,
            );
            next.failover_mut()?.assessment =
                Some("waiting for previous/current read quorum".to_string());
            Ok(Decision::Persist(next))
        }
    }
}

fn decide_data_loss(
    operation: &DurableOperationStatus,
    observations: &OperationObservations,
    now: i64,
) -> Result<Decision, String> {
    if let Some(pending) = &operation.pending_action {
        if let Some(observation) = observations.get(&pending.target_id) {
            let action = action_for(operation, pending, &BTreeMap::new())?;
            let signature = super::pending_action_signature(pending, &action)?;
            if let Some(result) = completed_data_loss(&observation.status, pending, &signature)? {
                let mut next = operation.clone();
                next.pending_action = None;
                next.failover_mut()?.data_loss_result = Some(result);
                next.phase = if result == DurableDataLossResultStatus::StateChanged {
                    next.target_snapshot = primary_only_snapshot(&next.target_snapshot)?;
                    if next.target_snapshot.members.len()
                        < next.minimum_committed_replicas.unwrap_or(1) as usize
                    {
                        return Ok(Decision::Persist(poison(
                            &next,
                            "state-changing data loss reduced target below minimum replicas",
                        )));
                    }
                    DurableOperationPhase::FailoverRefreshCandidate
                } else {
                    DurableOperationPhase::FailoverPromoteCandidate
                };
                next.phase_deadline_unix_seconds = now + ACTION_DEADLINE_SECONDS;
                return Ok(Decision::Persist(next));
            }
        }
    }
    decide_pending_action(
        operation,
        observations,
        now,
        DurableActionKind::FailoverOnDataLoss,
        operation.target_primary_id,
        DurablePostconditionKind::DataLossCompleted,
        None,
        DurableOperationPhase::FailoverNotifyDataLoss,
        DurableOperationPhase::FailoverNotifyDataLoss,
    )
}

fn decide_distribute_epoch(
    operation: &DurableOperationStatus,
    observations: &OperationObservations,
    now: i64,
) -> Result<Decision, String> {
    let failover = operation.failover_ref()?;
    let secondaries = operation
        .target_snapshot
        .members
        .iter()
        .filter(|member| member.id != operation.target_primary_id)
        .collect::<Vec<_>>();
    let index = failover.next_secondary_index as usize;
    if index >= secondaries.len() {
        return Ok(Decision::Persist(with_phase(
            operation,
            DurableOperationPhase::FailoverCatchUpConfiguration,
            now,
        )));
    }
    let target_id = secondaries[index].id;
    let decision = decide_pending_action(
        operation,
        observations,
        now,
        DurableActionKind::FailoverUpdateSecondaryEpoch,
        target_id,
        DurablePostconditionKind::Epoch,
        None,
        DurableOperationPhase::FailoverDistributeEpoch,
        DurableOperationPhase::FailoverDistributeEpoch,
    )?;
    match decision {
        Decision::Persist(mut next) if next.pending_action.is_none() => {
            next.failover_mut()?.next_secondary_index += 1;
            Ok(Decision::Persist(next))
        }
        other => Ok(other),
    }
}

fn decide_record_configuration(
    operation: &DurableOperationStatus,
    observations: &OperationObservations,
    now: i64,
    starting: bool,
) -> Result<Decision, String> {
    let failover = operation.failover_ref()?;
    let members = if starting {
        let mut members = BTreeMap::new();
        if let Some(previous) = &failover.previous_configuration {
            for member in previous.members.iter().filter(|member| !member.dropped) {
                members.insert(member.id, member.id);
            }
        }
        for member in failover
            .current_configuration
            .members
            .iter()
            .filter(|member| !member.dropped)
        {
            members.insert(member.id, member.id);
        }
        members.into_values().collect::<Vec<_>>()
    } else {
        operation
            .target_snapshot
            .members
            .iter()
            .map(|member| member.id)
            .collect::<Vec<_>>()
    };
    let index = failover.next_configuration_index as usize;
    if index >= members.len() {
        let mut next = operation.clone();
        next.pending_action = None;
        next.failover_mut()?.next_configuration_index = 0;
        next.phase = if starting {
            DurableOperationPhase::FailoverCollect
        } else {
            DurableOperationPhase::FailoverLabelMembers
        };
        next.phase_deadline_unix_seconds = now + ACTION_DEADLINE_SECONDS;
        return Ok(Decision::Persist(next));
    }
    let target_id = members[index];
    let kind = if starting {
        DurableActionKind::FailoverRecordStartingConfiguration
    } else {
        DurableActionKind::FailoverRecordElectionConfiguration
    };
    let phase = operation.phase;
    let decision = decide_pending_action(
        operation,
        observations,
        now,
        kind,
        target_id,
        DurablePostconditionKind::ElectionConfiguration,
        None,
        phase,
        phase,
    )?;
    match decision {
        Decision::Persist(mut next) if next.pending_action.is_none() => {
            next.failover_mut()?.next_configuration_index += 1;
            Ok(Decision::Persist(next))
        }
        other => Ok(other),
    }
}

#[allow(clippy::too_many_arguments)]
fn decide_pending_action(
    operation: &DurableOperationStatus,
    observations: &OperationObservations,
    now: i64,
    kind: DurableActionKind,
    target_id: i64,
    postcondition: DurablePostconditionKind,
    role: Option<StableReplicaRoleStatus>,
    phase: DurableOperationPhase,
    completed_phase: DurableOperationPhase,
) -> Result<Decision, String> {
    if let Some(pending) = &operation.pending_action {
        if pending.kind != kind || pending.target_id != target_id {
            return Err(format!(
                "phase {phase:?} has incompatible pending action {:?}",
                pending.kind
            ));
        }
        if pending.dispatch_authorized {
            let action = action_for(operation, pending, &BTreeMap::new())
                .or_else(|_| placeholder_action(operation, pending))?;
            return Ok(Decision::Execute {
                target_id,
                action_id: pending.action_id.clone(),
                action,
            });
        }
        let Some(observation) = observations.get(&target_id) else {
            return Ok(Decision::Wait);
        };
        let signature = placeholder_action(operation, pending)
            .ok()
            .map(|action| super::pending_action_signature(pending, &action))
            .transpose()?;
        match action_observed(
            &observation.status,
            pending,
            signature.as_deref(),
            operation,
            postcondition,
            role,
        )? {
            ActionObservation::Completed => {
                let mut next = operation.clone();
                next.pending_action = None;
                next.phase = completed_phase;
                next.phase_deadline_unix_seconds = now + ACTION_DEADLINE_SECONDS;
                Ok(Decision::Persist(next))
            }
            ActionObservation::Failed(error) => Ok(Decision::Persist(poison(operation, &error))),
            ActionObservation::InProgress => {
                if now >= pending.deadline_unix_seconds {
                    Ok(Decision::Persist(poison(
                        operation,
                        "durable failover action remained in progress past its deadline",
                    )))
                } else {
                    Ok(Decision::Wait)
                }
            }
            ActionObservation::Incomplete => {
                if now >= pending.deadline_unix_seconds {
                    return Ok(Decision::Persist(poison(
                        operation,
                        "durable failover action reached its deadline",
                    )));
                }
                let mut next = operation.clone();
                next.pending_action.as_mut().unwrap().dispatch_authorized = true;
                Ok(Decision::Persist(next))
            }
        }
    } else {
        let mut next = operation.clone();
        next.phase = phase;
        next.pending_action = Some(make_pending(
            operation,
            kind,
            target_id,
            postcondition,
            role,
            now,
        ));
        Ok(Decision::Persist(next))
    }
}

fn decide_label_members(
    operation: &DurableOperationStatus,
    observations: &OperationObservations,
    now: i64,
) -> Result<Decision, String> {
    let index = operation.failover_ref()?.next_label_index as usize;
    let mut label_members = operation
        .target_snapshot
        .members
        .iter()
        .map(|member| (member.id, member.role))
        .collect::<Vec<_>>();
    if !label_members
        .iter()
        .any(|(id, _)| *id == operation.old_primary_id)
    {
        label_members.push((
            operation.old_primary_id,
            StableReplicaRoleStatus::ActiveSecondary,
        ));
    }
    if index >= label_members.len() {
        return Ok(Decision::Persist(with_phase(
            operation,
            DurableOperationPhase::FailoverAttest,
            now,
        )));
    }
    let (member_id, member_role) = label_members[index];
    let desired = if member_id == operation.target_primary_id {
        "primary"
    } else {
        "secondary"
    };
    if let Some(pending) = &operation.pending_action {
        if pending.kind != DurableActionKind::FailoverLabelMember || pending.target_id != member_id
        {
            return Err("failover label phase has incompatible pending action".to_string());
        }
        if pending.dispatch_authorized {
            return Ok(Decision::PatchPodRole {
                target_id: member_id,
                role: desired.to_string(),
            });
        }
        let Some(observation) = observations.get(&member_id) else {
            return Ok(Decision::Wait);
        };
        if observation.pod_role_label.as_deref() == Some(desired) {
            let mut next = operation.clone();
            next.pending_action = None;
            next.failover_mut()?.next_label_index += 1;
            return Ok(Decision::Persist(next));
        }
        let mut next = operation.clone();
        next.pending_action.as_mut().unwrap().dispatch_authorized = true;
        Ok(Decision::Persist(next))
    } else {
        let mut next = operation.clone();
        next.pending_action = Some(make_pending(
            operation,
            DurableActionKind::FailoverLabelMember,
            member_id,
            DurablePostconditionKind::PodRoleLabel,
            Some(member_role),
            now,
        ));
        Ok(Decision::Persist(next))
    }
}

fn action_observed(
    status: &kuberic_core::types::ReplicaStatusInfo,
    pending: &PendingActionStatus,
    signature: Option<&str>,
    operation: &DurableOperationStatus,
    postcondition: DurablePostconditionKind,
    role: Option<StableReplicaRoleStatus>,
) -> Result<ActionObservation, String> {
    if let Some(action) = super::correlated_action_observation(status, &pending.action_id) {
        if signature.is_some_and(|signature| action.signature != signature) {
            return Ok(ActionObservation::Failed(
                "durable action signature conflict".to_string(),
            ));
        }
        match action.state {
            DurableActionState::Completed => return Ok(ActionObservation::Completed),
            DurableActionState::Failed => {
                return Ok(ActionObservation::Failed(
                    action
                        .error
                        .clone()
                        .unwrap_or_else(|| "durable action failed".to_string()),
                ));
            }
            DurableActionState::Scheduled | DurableActionState::InProgress => {
                return Ok(ActionObservation::InProgress);
            }
        }
    }
    let expected_epoch = epoch(&pending.expected_epoch);
    let completed = match postcondition {
        DurablePostconditionKind::Epoch => status.epoch == expected_epoch,
        DurablePostconditionKind::Role => {
            status.epoch == expected_epoch
                && role.is_some_and(|role| status.role == stable_role(role))
        }
        DurablePostconditionKind::CatchUpConfiguration => {
            status.configuration.as_ref().is_some_and(|configuration| {
                configuration.mode == ReplicaConfigurationMode::CatchUp
                    && configuration_matches_target(configuration, operation)
            })
        }
        DurablePostconditionKind::CurrentConfiguration => {
            status.configuration.as_ref().is_some_and(|configuration| {
                configuration.mode == ReplicaConfigurationMode::Current
                    && configuration_matches_target(configuration, operation)
            })
        }
        DurablePostconditionKind::ElectionConfiguration => status
            .election_configuration
            .as_ref()
            .is_some_and(|configuration| match pending.kind {
                DurableActionKind::FailoverRecordStartingConfiguration => {
                    runtime_configuration_matches(
                        Some(configuration),
                        operation.failover_ref().unwrap(),
                    )
                }
                DurableActionKind::FailoverRecordElectionConfiguration => {
                    target_runtime_configuration_matches(
                        Some(configuration),
                        &operation.target_snapshot,
                    )
                }
                _ => false,
            }),
        DurablePostconditionKind::CatchUpQuorum | DurablePostconditionKind::DataLossCompleted => {
            false
        }
        _ => false,
    };
    Ok(if completed {
        ActionObservation::Completed
    } else {
        ActionObservation::Incomplete
    })
}

fn completed_data_loss(
    status: &kuberic_core::types::ReplicaStatusInfo,
    pending: &PendingActionStatus,
    signature: &str,
) -> Result<Option<DurableDataLossResultStatus>, String> {
    let Some(completed) = super::correlated_action_observation(status, &pending.action_id) else {
        return Ok(None);
    };
    if matches!(
        completed.state,
        DurableActionState::Scheduled | DurableActionState::InProgress
    ) {
        return Ok(None);
    }
    if completed.signature != signature {
        return Err("data-loss completion signature conflict".to_string());
    }
    if completed.state == DurableActionState::Failed {
        return Err(completed
            .error
            .clone()
            .unwrap_or_else(|| "data-loss callback failed".to_string()));
    }
    match completed.result {
        Some(DurableActionResult::DataLoss(DataLossAction::None)) => {
            Ok(Some(DurableDataLossResultStatus::NoStateChange))
        }
        Some(DurableActionResult::DataLoss(DataLossAction::StateChanged)) => {
            Ok(Some(DurableDataLossResultStatus::StateChanged))
        }
        Some(DurableActionResult::AddReplica(_)) | Some(DurableActionResult::RemoveReplica(_)) => {
            Err("data-loss completion carries a non-data-loss result".to_string())
        }
        None => Err("data-loss completion has no result".to_string()),
    }
}

fn make_pending(
    operation: &DurableOperationStatus,
    kind: DurableActionKind,
    target_id: i64,
    postcondition: DurablePostconditionKind,
    role: Option<StableReplicaRoleStatus>,
    now: i64,
) -> PendingActionStatus {
    let sequence = operation
        .pending_action
        .as_ref()
        .map_or(0, |pending| pending.sequence.saturating_add(1))
        .saturating_add(operation.failover_ref().map_or(0, |failover| {
            failover.next_configuration_index
                + failover.next_secondary_index
                + failover.next_label_index
        }));
    PendingActionStatus {
        action_id: format!(
            "{}:{kind:?}:{}:{target_id}:{sequence}",
            operation.operation_id,
            action_scope(operation)
        ),
        sequence,
        kind,
        target_id,
        target_instance_id: target_instance(operation, target_id).unwrap_or_default(),
        expected_epoch: operation.target_snapshot.epoch.clone(),
        desired_postcondition: DurablePostconditionStatus {
            kind: postcondition,
            role,
        },
        attempts: 0,
        deadline_unix_seconds: now
            + if kind == DurableActionKind::FailoverOnDataLoss {
                DATA_LOSS_ACTION_DEADLINE_SECONDS
            } else {
                ACTION_DEADLINE_SECONDS
            },
        last_error: None,
        dispatch_authorized: false,
        dispatch_agent_generation: None,
        dispatch_agent_control_version: None,
        dispatch_observed_runtime_epoch: None,
        dispatch_action_payload: String::new(),
    }
}

fn action_scope(operation: &DurableOperationStatus) -> String {
    let current = operation
        .failover
        .as_ref()
        .map(|failover| {
            failover
                .current_configuration
                .members
                .iter()
                .map(|member| format!("{}@{}", member.id, member.instance_id))
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default();
    let target = operation
        .target_snapshot
        .members
        .iter()
        .map(|member| format!("{}@{}", member.id, member.instance_id))
        .collect::<Vec<_>>()
        .join(",");
    format!("current[{current}]-target[{target}]")
}

fn placeholder_action(
    operation: &DurableOperationStatus,
    pending: &PendingActionStatus,
) -> Result<DurableReplicaAction, String> {
    match pending.kind {
        DurableActionKind::FailoverRecordStartingConfiguration => {
            Ok(DurableReplicaAction::RecordElectionConfiguration {
                configuration: election_configuration(operation)?,
            })
        }
        DurableActionKind::FailoverRecordElectionConfiguration => {
            Ok(DurableReplicaAction::RecordElectionConfiguration {
                configuration: target_election_configuration(operation),
            })
        }
        DurableActionKind::FailoverUpdateCandidateEpoch
        | DurableActionKind::FailoverUpdateSecondaryEpoch => {
            Ok(DurableReplicaAction::UpdateEpoch {
                epoch: epoch(&pending.expected_epoch),
            })
        }
        DurableActionKind::FailoverOnDataLoss => Ok(DurableReplicaAction::OnDataLoss {
            epoch: epoch(&pending.expected_epoch),
        }),
        DurableActionKind::FailoverPromoteCandidate => Ok(DurableReplicaAction::ChangeRole {
            epoch: epoch(&pending.expected_epoch),
            role: Role::Primary,
        }),
        DurableActionKind::FailoverWaitForCatchUpQuorum => {
            Ok(DurableReplicaAction::WaitForCatchUpQuorum {
                mode: ReplicaSetQuorumMode::Write,
            })
        }
        kind => Err(format!("action {kind:?} requires live replica addresses")),
    }
}

fn election_input(operation: &DurableOperationStatus) -> Result<ElectionInput, String> {
    let failover = operation.failover_ref()?;
    let current = election_configuration_status(&failover.current_configuration);
    let previous = failover
        .previous_configuration
        .as_ref()
        .map(election_configuration_status);
    let expected_epoch = operation
        .previous_snapshot
        .as_ref()
        .map(|snapshot| epoch(&snapshot.epoch))
        .ok_or_else(|| "failover has no previous snapshot".to_string())?;
    let observations = failover
        .observations
        .iter()
        .map(|observation| {
            Ok((
                observation.id,
                ElectionObservation {
                    instance_id: kuberic_core::types::ReplicaInstanceId::new(
                        observation.instance_id.clone(),
                    ),
                    epoch: epoch(&observation.epoch),
                    role: parse_role(&observation.role),
                    healthy: observation.healthy,
                    current_lsn: observation.current_lsn,
                    committed_lsn: observation.committed_lsn,
                    first_retained_lsn: observation.first_retained_lsn,
                    deactivation: observation.deactivation_epoch.as_ref().map(|epoch_status| {
                        kuberic_core::types::ReplicaDeactivationInfo {
                            epoch: epoch(epoch_status),
                            catch_up_lsn: observation.deactivation_catch_up_lsn.unwrap_or_default(),
                        }
                    }),
                    configuration: observation
                        .configuration_matches
                        .then(|| election_configuration(operation))
                        .transpose()?,
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>, String>>()?;
    let mut input = ElectionInput {
        expected_epoch,
        failed_primary_id: operation.old_primary_id,
        required_catch_up_lsn: 0,
        previous,
        current,
        observations,
    };
    let accepted = accepted_observations(&input);
    input.required_catch_up_lsn = accepted
        .values()
        .filter_map(|observation| {
            observation
                .deactivation
                .map(|deactivation| deactivation.catch_up_lsn)
        })
        .max()
        .or_else(|| {
            accepted
                .values()
                .map(|observation| observation.committed_lsn)
                .max()
        })
        .unwrap_or_default();
    Ok(input)
}

fn election_configuration_status(
    configuration: &FailoverConfigurationStatus,
) -> ElectionConfiguration {
    ElectionConfiguration {
        members: configuration
            .members
            .iter()
            .map(|member| ElectionMember {
                id: member.id,
                instance_id: kuberic_core::types::ReplicaInstanceId::new(
                    member.instance_id.clone(),
                ),
                role: stable_role(member.role),
                dropped: member.dropped,
                last_known: member.last_known.as_ref().map(core_metadata),
            })
            .collect(),
        write_quorum: configuration.write_quorum,
    }
}

fn election_configuration(
    operation: &DurableOperationStatus,
) -> Result<ReplicaElectionConfiguration, String> {
    let failover = operation.failover_ref()?;
    Ok(ReplicaElectionConfiguration {
        previous: failover
            .previous_configuration
            .as_ref()
            .map(|configuration| runtime_configuration(configuration, true)),
        current: runtime_configuration(&failover.current_configuration, false),
    })
}

fn target_election_configuration(
    operation: &DurableOperationStatus,
) -> ReplicaElectionConfiguration {
    ReplicaElectionConfiguration {
        previous: None,
        current: ReplicaConfigurationStatus {
            mode: ReplicaConfigurationMode::Current,
            members: operation
                .target_snapshot
                .members
                .iter()
                .map(|member| ReplicaConfigurationMemberStatus {
                    id: member.id,
                    instance_id: kuberic_core::types::ReplicaInstanceId::new(
                        member.instance_id.clone(),
                    ),
                    role: stable_role(member.role),
                })
                .collect(),
            write_quorum: operation.target_snapshot.write_quorum,
        },
    }
}

fn runtime_configuration(
    configuration: &FailoverConfigurationStatus,
    catch_up: bool,
) -> ReplicaConfigurationStatus {
    ReplicaConfigurationStatus {
        mode: if catch_up {
            ReplicaConfigurationMode::CatchUp
        } else {
            ReplicaConfigurationMode::Current
        },
        members: configuration
            .members
            .iter()
            .map(|member| ReplicaConfigurationMemberStatus {
                id: member.id,
                instance_id: kuberic_core::types::ReplicaInstanceId::new(
                    member.instance_id.clone(),
                ),
                role: stable_role(member.role),
            })
            .collect(),
        write_quorum: configuration.write_quorum,
    }
}

fn runtime_configuration_matches(
    observed: Option<&ReplicaElectionConfiguration>,
    failover: &DurableFailoverStatus,
) -> bool {
    let Some(observed) = observed else {
        return false;
    };
    configuration_status_equal(
        &observed.current,
        &runtime_configuration(&failover.current_configuration, false),
    ) && match (&observed.previous, &failover.previous_configuration) {
        (None, None) => true,
        (Some(observed), Some(expected)) => {
            configuration_status_equal(observed, &runtime_configuration(expected, true))
        }
        _ => false,
    }
}

fn target_runtime_configuration_matches(
    observed: Option<&ReplicaElectionConfiguration>,
    target: &StablePartitionSnapshotStatus,
) -> bool {
    let Some(observed) = observed else {
        return false;
    };
    observed.previous.is_none()
        && configuration_status_equal(
            &observed.current,
            &ReplicaConfigurationStatus {
                mode: ReplicaConfigurationMode::Current,
                members: target
                    .members
                    .iter()
                    .map(|member| ReplicaConfigurationMemberStatus {
                        id: member.id,
                        instance_id: kuberic_core::types::ReplicaInstanceId::new(
                            member.instance_id.clone(),
                        ),
                        role: stable_role(member.role),
                    })
                    .collect(),
                write_quorum: target.write_quorum,
            },
        )
}

fn configuration_status_equal(
    left: &ReplicaConfigurationStatus,
    right: &ReplicaConfigurationStatus,
) -> bool {
    let mut left_members = left.members.clone();
    let mut right_members = right.members.clone();
    left_members.sort_by_key(|member| member.id);
    right_members.sort_by_key(|member| member.id);
    left.mode == right.mode
        && left.write_quorum == right.write_quorum
        && left_members == right_members
}

fn target_config(
    operation: &DurableOperationStatus,
    handles: &BTreeMap<i64, Box<dyn ReplicaHandle>>,
    must_catch_up: bool,
) -> Result<ReplicaSetConfig, String> {
    snapshot_config(&operation.target_snapshot, handles, must_catch_up)
}

fn committed_config(
    operation: &DurableOperationStatus,
    handles: &BTreeMap<i64, Box<dyn ReplicaHandle>>,
) -> Result<ReplicaSetConfig, String> {
    let snapshot = operation
        .committed_snapshot
        .as_ref()
        .ok_or_else(|| "failover has no committed promotion snapshot".to_string())?;
    snapshot_config(snapshot, handles, false)
}

fn snapshot_config(
    snapshot: &StablePartitionSnapshotStatus,
    handles: &BTreeMap<i64, Box<dyn ReplicaHandle>>,
    must_catch_up: bool,
) -> Result<ReplicaSetConfig, String> {
    Ok(ReplicaSetConfig {
        members: snapshot
            .members
            .iter()
            .map(|member| {
                let handle = handles
                    .get(&member.id)
                    .ok_or_else(|| format!("replica {} has no current handle", member.id))?;
                let metadata = member.election_metadata.as_ref();
                Ok(ReplicaInfo {
                    id: member.id,
                    instance_id: kuberic_core::types::ReplicaInstanceId::new(
                        member.instance_id.clone(),
                    ),
                    role: stable_role(member.role),
                    status: ReplicaStatus::Up,
                    replicator_address: handle.replicator_address(),
                    current_progress: metadata.map_or(0, |metadata| metadata.current_lsn),
                    catch_up_capability: metadata.map_or(0, |metadata| metadata.first_retained_lsn),
                    must_catch_up: must_catch_up && member.id != snapshot.primary_id,
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
        write_quorum: snapshot.write_quorum,
    })
}

fn configuration_matches_target(
    configuration: &ReplicaConfigurationStatus,
    operation: &DurableOperationStatus,
) -> bool {
    let expected = ReplicaConfigurationStatus {
        mode: configuration.mode,
        members: operation
            .target_snapshot
            .members
            .iter()
            .map(|member| ReplicaConfigurationMemberStatus {
                id: member.id,
                instance_id: kuberic_core::types::ReplicaInstanceId::new(
                    member.instance_id.clone(),
                ),
                role: stable_role(member.role),
            })
            .collect(),
        write_quorum: operation.target_snapshot.write_quorum,
    };
    configuration_status_equal(configuration, &expected)
}

fn collection_complete(operation: &DurableOperationStatus) -> bool {
    let Ok(failover) = operation.failover_ref() else {
        return false;
    };
    let mut members = BTreeMap::new();
    if let Some(previous) = &failover.previous_configuration {
        for member in previous.members.iter().filter(|member| !member.dropped) {
            members.insert(member.id, member);
        }
    }
    for member in failover
        .current_configuration
        .members
        .iter()
        .filter(|member| !member.dropped)
    {
        members.insert(member.id, member);
    }
    members.into_values().all(|member| {
        failover
            .observations
            .iter()
            .any(|observation| observation.id == member.id)
            || failover.unavailable_replicas.contains(&member.id)
    })
}

fn final_attestation_complete(operation: &DurableOperationStatus) -> bool {
    let Ok(failover) = operation.failover_ref() else {
        return false;
    };
    operation.target_snapshot.members.iter().all(|member| {
        failover
            .final_attestations
            .iter()
            .any(|observation| observation.id == member.id)
    })
}

fn snapshot_from_attestations(
    operation: &DurableOperationStatus,
) -> Result<StablePartitionSnapshotStatus, String> {
    let mut snapshot = operation.target_snapshot.clone();
    let failover = operation.failover_ref()?;
    for member in &mut snapshot.members {
        let observation = failover
            .final_attestations
            .iter()
            .find(|observation| observation.id == member.id)
            .ok_or_else(|| format!("replica {} lacks final attestation", member.id))?;
        if observation.instance_id != member.instance_id
            || epoch(&observation.epoch) != epoch(&snapshot.epoch)
            || parse_role(&observation.role) != stable_role(member.role)
            || !observation.healthy
            || !observation.configuration_matches
        {
            return Err(format!(
                "replica {} final attestation differs from target",
                member.id
            ));
        }
        let deactivation_epoch = observation
            .deactivation_epoch
            .as_ref()
            .ok_or_else(|| format!("replica {} lacks deactivation evidence", member.id))?;
        member.election_metadata = Some(StableReplicaElectionMetadataStatus {
            current_lsn: observation.current_lsn,
            committed_lsn: observation.committed_lsn,
            first_retained_lsn: observation
                .first_retained_lsn
                .ok_or_else(|| format!("replica {} lacks retained progress", member.id))?,
            deactivation_epoch: deactivation_epoch.clone(),
            deactivation_catch_up_lsn: observation
                .deactivation_catch_up_lsn
                .ok_or_else(|| format!("replica {} lacks deactivation progress", member.id))?,
        });
    }
    validate_snapshot(&snapshot)?;
    Ok(snapshot)
}

fn primary_only_snapshot(
    snapshot: &StablePartitionSnapshotStatus,
) -> Result<StablePartitionSnapshotStatus, String> {
    let mut primary = snapshot
        .members
        .iter()
        .find(|member| member.id == snapshot.primary_id)
        .cloned()
        .ok_or_else(|| "target primary is absent".to_string())?;
    primary.role = StableReplicaRoleStatus::Primary;
    Ok(StablePartitionSnapshotStatus {
        epoch: snapshot.epoch.clone(),
        primary_id: snapshot.primary_id,
        members: vec![primary],
        write_quorum: 1,
    })
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

fn validate_operation(operation: &DurableOperationStatus) -> Result<(), String> {
    if operation.version != DURABLE_OPERATION_VERSION {
        return Err(format!(
            "unsupported durable operation version {}",
            operation.version
        ));
    }
    if operation.kind != DurableOperationKind::Failover {
        return Err("operation is not failover".to_string());
    }
    operation.failover_ref()?;
    validate_snapshot(
        operation
            .previous_snapshot
            .as_ref()
            .ok_or_else(|| "failover has no previous stable snapshot".to_string())?,
    )?;
    if operation.failover_ref()?.target_confirmed {
        validate_snapshot(&operation.target_snapshot)?;
    }
    Ok(())
}

fn validate_snapshot(snapshot: &StablePartitionSnapshotStatus) -> Result<(), String> {
    StablePartitionSnapshot::try_from(snapshot)
        .map(|_| ())
        .map_err(|error| format!("invalid stable snapshot: {error}"))
}

fn target_instance(operation: &DurableOperationStatus, target_id: i64) -> Option<String> {
    operation
        .failover
        .as_ref()
        .and_then(|failover| {
            failover
                .current_configuration
                .members
                .iter()
                .find(|member| member.id == target_id)
                .map(|member| member.instance_id.clone())
        })
        .or_else(|| {
            operation
                .target_snapshot
                .members
                .iter()
                .find(|member| member.id == target_id)
                .map(|member| member.instance_id.clone())
        })
}

fn epoch(status: &EpochStatus) -> Epoch {
    Epoch::new(status.data_loss_number, status.configuration_number)
}

fn core_metadata(
    metadata: &StableReplicaElectionMetadataStatus,
) -> kuberic_core::types::StableReplicaElectionMetadata {
    kuberic_core::types::StableReplicaElectionMetadata {
        current_lsn: metadata.current_lsn,
        committed_lsn: metadata.committed_lsn,
        first_retained_lsn: metadata.first_retained_lsn,
        deactivation_epoch: epoch(&metadata.deactivation_epoch),
        deactivation_catch_up_lsn: metadata.deactivation_catch_up_lsn,
    }
}

fn stable_role(role: StableReplicaRoleStatus) -> Role {
    match role {
        StableReplicaRoleStatus::Primary => Role::Primary,
        StableReplicaRoleStatus::ActiveSecondary => Role::ActiveSecondary,
    }
}

fn role_name(role: Role) -> &'static str {
    match role {
        Role::Unknown => "unknown",
        Role::Primary => "primary",
        Role::ActiveSecondary => "activeSecondary",
        Role::IdleSecondary => "idleSecondary",
        Role::None => "none",
    }
}

fn parse_role(role: &str) -> Role {
    match role {
        "primary" => Role::Primary,
        "activeSecondary" => Role::ActiveSecondary,
        "idleSecondary" => Role::IdleSecondary,
        "none" => Role::None,
        _ => Role::Unknown,
    }
}

enum ActionObservation {
    Completed,
    Failed(String),
    InProgress,
    Incomplete,
}

trait FailoverOperationExt {
    fn failover_ref(&self) -> Result<&DurableFailoverStatus, String>;
    fn failover_mut(&mut self) -> Result<&mut DurableFailoverStatus, String>;
}

impl FailoverOperationExt for DurableOperationStatus {
    fn failover_ref(&self) -> Result<&DurableFailoverStatus, String> {
        self.failover
            .as_ref()
            .ok_or_else(|| "failover operation has no failover checkpoint".to_string())
    }

    fn failover_mut(&mut self) -> Result<&mut DurableFailoverStatus, String> {
        self.failover
            .as_mut()
            .ok_or_else(|| "failover operation has no failover checkpoint".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kuberic_core::types::{
        AccessStatus, ReplicaConnectionStatus, ReplicaDeactivationInfo, ReplicaInstanceId,
        ReplicaStatusInfo,
    };

    fn snapshot(size: i64) -> StablePartitionSnapshotStatus {
        StablePartitionSnapshotStatus {
            epoch: EpochStatus {
                data_loss_number: 2,
                configuration_number: 7,
            },
            primary_id: 1,
            members: (1..=size)
                .map(|id| crate::crd::StableReplicaSnapshotStatus {
                    id,
                    instance_id: format!("instance-{id}"),
                    role: if id == 1 {
                        StableReplicaRoleStatus::Primary
                    } else {
                        StableReplicaRoleStatus::ActiveSecondary
                    },
                    election_metadata: Some(StableReplicaElectionMetadataStatus {
                        current_lsn: id * 10,
                        committed_lsn: id * 10,
                        first_retained_lsn: 0,
                        deactivation_epoch: EpochStatus {
                            data_loss_number: 2,
                            configuration_number: 7,
                        },
                        deactivation_catch_up_lsn: id * 10,
                    }),
                })
                .collect(),
            write_quorum: size as u32 / 2 + 1,
        }
    }

    fn persisted_observation(
        operation: &DurableOperationStatus,
        id: i64,
        healthy: bool,
    ) -> ReplicaElectionObservationStatus {
        ReplicaElectionObservationStatus {
            id,
            instance_id: format!("instance-{id}"),
            epoch: operation.previous_snapshot.as_ref().unwrap().epoch.clone(),
            role: "activeSecondary".to_string(),
            healthy,
            current_lsn: id * 10,
            committed_lsn: id * 10,
            first_retained_lsn: Some(0),
            deactivation_epoch: Some(operation.previous_snapshot.as_ref().unwrap().epoch.clone()),
            deactivation_catch_up_lsn: Some(id * 10),
            configuration_matches: true,
        }
    }

    fn live_status(
        operation: &DurableOperationStatus,
        id: i64,
        epoch: Epoch,
        role: Role,
    ) -> ReplicaStatusInfo {
        ReplicaStatusInfo {
            instance_id: ReplicaInstanceId::new(format!("instance-{id}")),
            role,
            epoch,
            current_progress: id * 10,
            catch_up_capability: Some(0),
            committed_lsn: id * 10,
            healthy: true,
            write_status: if role == Role::Primary {
                AccessStatus::Granted
            } else {
                AccessStatus::NotPrimary
            },
            configuration: None,
            election_configuration: Some(election_configuration(operation).unwrap()),
            deactivation_info: Some(ReplicaDeactivationInfo {
                epoch,
                catch_up_lsn: id * 10,
            }),
            active_replica_connections: Vec::<ReplicaConnectionStatus>::new(),
            build_observation: None,
            agent: kuberic_core::types::ReplicaAgentStatus {
                protocol_version: kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION,
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
        }
    }

    #[test]
    fn placement_is_persisted_before_dispatch_and_replayed_after_restart() {
        let placement = FailoverPlacementSnapshot {
            topology_key: Some("topology.kubernetes.io/zone".to_string()),
            replicas: [2, 3]
                .into_iter()
                .map(
                    |id| crate::durable::failover_election::FailoverReplicaPlacement {
                        id,
                        instance_id: format!("instance-{id}"),
                        node_name: Some(format!("node-{id}")),
                        topology_domain: Some(format!("zone-{id}")),
                        domain_primary_count: Some(if id == 2 { 5 } else { 0 }),
                        node_primary_count: Some(if id == 2 { 5 } else { 0 }),
                        eligible: true,
                    },
                )
                .collect(),
        };
        let mut operation =
            start_failover_with_placement("set", snapshot(3), 1, 1, 100, Some(placement.clone()))
                .unwrap();
        assert_eq!(
            operation.phase,
            DurableOperationPhase::FailoverRecordStartingConfiguration
        );
        assert!(operation.pending_action.is_none());
        assert_eq!(
            operation.failover_ref().unwrap().placement.as_ref(),
            Some(&placement)
        );
        operation.phase = DurableOperationPhase::FailoverAssess;
        let mut observations = vec![
            persisted_observation(&operation, 2, true),
            persisted_observation(&operation, 3, true),
        ];
        for observation in &mut observations {
            observation.current_lsn = 50;
            observation.committed_lsn = 50;
            observation.deactivation_catch_up_lsn = Some(50);
        }
        operation.failover_mut().unwrap().observations = observations;
        let wire = serde_json::to_string(&operation).unwrap();
        let restored: DurableOperationStatus = serde_json::from_str(&wire).unwrap();
        let Decision::Persist(confirmed) =
            decide_failover(&restored, &OperationObservations::new(), 101).unwrap()
        else {
            panic!("replayed placement must confirm a candidate");
        };
        assert_eq!(confirmed.target_primary_id, 3);
        assert_eq!(
            confirmed.failover_ref().unwrap().placement.as_ref(),
            Some(&placement)
        );
        assert!(confirmed.failover_ref().unwrap().target_confirmed);
        assert!(confirmed.pending_action.is_none());

        let mut legacy_json = serde_json::to_value(&operation).unwrap();
        legacy_json["failover"]
            .as_object_mut()
            .unwrap()
            .remove("placement");
        let legacy: DurableOperationStatus = serde_json::from_value(legacy_json).unwrap();
        assert!(legacy.failover_ref().unwrap().placement.is_none());
        let Decision::Persist(legacy_confirmed) =
            decide_failover(&legacy, &OperationObservations::new(), 101).unwrap()
        else {
            panic!("legacy checkpoint must remain replayable");
        };
        assert_eq!(legacy_confirmed.target_primary_id, 2);
    }

    #[test]
    fn configuration_and_data_loss_epoch_intents_are_separate() {
        let mut operation = start_failover("set", snapshot(5), 1, 1, 100).unwrap();
        operation.phase = DurableOperationPhase::FailoverAssess;
        let mut four = persisted_observation(&operation, 4, true);
        four.role = "none".to_string();
        let mut five = persisted_observation(&operation, 5, true);
        five.role = "none".to_string();
        operation.failover_mut().unwrap().observations = vec![
            persisted_observation(&operation, 2, true),
            persisted_observation(&operation, 3, true),
            four,
            five,
        ];
        let Decision::Persist(confirmed) =
            decide_failover(&operation, &OperationObservations::new(), 101).unwrap()
        else {
            panic!("assessment must persist confirmation");
        };
        assert!(confirmed.failover_ref().unwrap().target_confirmed);
        assert!(confirmed.failover_ref().unwrap().data_loss_required);
        assert_eq!(
            confirmed.phase,
            DurableOperationPhase::FailoverPersistConfigurationEpoch
        );

        let Decision::Persist(configuration_intent) =
            decide_failover(&confirmed, &OperationObservations::new(), 102).unwrap()
        else {
            panic!("configuration epoch intent must persist");
        };
        assert_eq!(
            configuration_intent
                .failover_ref()
                .unwrap()
                .configuration_epoch_intent
                .as_ref()
                .unwrap()
                .configuration_number,
            8
        );
        assert!(
            configuration_intent
                .failover_ref()
                .unwrap()
                .data_loss_epoch_intent
                .is_none()
        );

        let Decision::Persist(data_loss_intent) =
            decide_failover(&configuration_intent, &OperationObservations::new(), 103).unwrap()
        else {
            panic!("data-loss epoch intent must persist separately");
        };
        assert_eq!(data_loss_intent.target_snapshot.epoch.data_loss_number, 3);
        assert_eq!(
            data_loss_intent.target_snapshot.epoch.configuration_number,
            8
        );
        assert_eq!(
            data_loss_intent.phase,
            DurableOperationPhase::FailoverApplyCandidateEpoch
        );
    }

    #[test]
    fn pending_action_requires_intent_then_observation_then_authorization() {
        let mut operation = start_failover("set", snapshot(3), 1, 1, 100).unwrap();
        operation.target_primary_id = 3;
        operation.target_snapshot.primary_id = 3;
        operation.target_snapshot.epoch = EpochStatus {
            data_loss_number: 2,
            configuration_number: 8,
        };
        operation.failover_mut().unwrap().target_confirmed = true;
        operation.phase = DurableOperationPhase::FailoverApplyCandidateEpoch;

        let Decision::Persist(intent) =
            decide_failover(&operation, &OperationObservations::new(), 101).unwrap()
        else {
            panic!("first step must persist action intent");
        };
        assert!(!intent.pending_action.as_ref().unwrap().dispatch_authorized);

        let mut observations = OperationObservations::new();
        observations.insert(
            3,
            crate::durable::ReplicaObservation {
                status: live_status(&intent, 3, Epoch::new(2, 7), Role::ActiveSecondary),
                control_address: "http://three-control".to_string(),
                replicator_address: "http://three".to_string(),
                pod_name: "set-2".to_string(),
                pod_role_label: Some("secondary".to_string()),
            },
        );
        let Decision::Persist(authorized) = decide_failover(&intent, &observations, 102).unwrap()
        else {
            panic!("observation must persist dispatch authorization");
        };
        assert!(
            authorized
                .pending_action
                .as_ref()
                .unwrap()
                .dispatch_authorized
        );

        authorized
            .pending_action
            .as_ref()
            .map(|pending| pending.action_id.clone())
            .unwrap();
        let mut completed_observations = OperationObservations::new();
        completed_observations.insert(
            3,
            crate::durable::ReplicaObservation {
                status: live_status(&authorized, 3, Epoch::new(2, 8), Role::ActiveSecondary),
                control_address: "http://three-control".to_string(),
                replicator_address: "http://three".to_string(),
                pod_name: "set-2".to_string(),
                pod_role_label: Some("secondary".to_string()),
            },
        );
        let mut probe = authorized.clone();
        probe.pending_action.as_mut().unwrap().dispatch_authorized = false;
        let Decision::Persist(next) =
            decide_failover(&probe, &completed_observations, 103).unwrap()
        else {
            panic!("observed epoch must advance phase");
        };
        assert_eq!(next.phase, DurableOperationPhase::FailoverPromoteCandidate);
    }

    #[test]
    fn promotion_commit_is_primary_only_and_rolls_forward() {
        let mut operation = start_failover("set", snapshot(3), 1, 1, 100).unwrap();
        operation.target_primary_id = 3;
        operation.target_snapshot.primary_id = 3;
        operation.target_snapshot.members[0].role = StableReplicaRoleStatus::ActiveSecondary;
        operation.target_snapshot.members[2].role = StableReplicaRoleStatus::Primary;
        operation.failover_mut().unwrap().target_confirmed = true;
        operation.phase = DurableOperationPhase::FailoverCommitPromotion;
        let Decision::CommitSnapshot {
            operation: next,
            snapshot,
        } = decide_failover(&operation, &OperationObservations::new(), 101).unwrap()
        else {
            panic!("promotion must commit");
        };
        assert_eq!(snapshot.members.len(), 1);
        assert_eq!(snapshot.primary_id, 3);
        assert!(next.failover_ref().unwrap().promotion_committed);
        assert_eq!(next.phase, DurableOperationPhase::FailoverDistributeEpoch);
    }

    #[test]
    fn replacement_adoption_is_allowed_only_before_confirmation() {
        let operation = start_failover("set", snapshot(3), 1, 1, 100).unwrap();
        let adopted =
            adopt_replacement_before_confirmation(&operation, 2, "replacement-2", 101).unwrap();
        assert_eq!(
            adopted
                .failover_ref()
                .unwrap()
                .current_configuration
                .members[1]
                .instance_id,
            "replacement-2"
        );
        let mut confirmed = adopted;
        confirmed.failover_mut().unwrap().target_confirmed = true;
        assert!(adopt_replacement_before_confirmation(&confirmed, 2, "again", 102).is_err());
    }

    #[test]
    fn durable_failover_restart_matrix_round_trips_every_persisted_phase() {
        let phases = [
            DurableOperationPhase::FailoverRecordStartingConfiguration,
            DurableOperationPhase::FailoverCollect,
            DurableOperationPhase::FailoverAssess,
            DurableOperationPhase::FailoverWaitForBestCandidate,
            DurableOperationPhase::FailoverWaitForReadQuorum,
            DurableOperationPhase::FailoverPersistConfigurationEpoch,
            DurableOperationPhase::FailoverPersistDataLossEpoch,
            DurableOperationPhase::FailoverApplyCandidateEpoch,
            DurableOperationPhase::FailoverNotifyDataLoss,
            DurableOperationPhase::FailoverRefreshCandidate,
            DurableOperationPhase::FailoverPromoteCandidate,
            DurableOperationPhase::FailoverCommitPromotion,
            DurableOperationPhase::FailoverDistributeEpoch,
            DurableOperationPhase::FailoverCatchUpConfiguration,
            DurableOperationPhase::FailoverWaitForCatchUpQuorum,
            DurableOperationPhase::FailoverCurrentConfiguration,
            DurableOperationPhase::FailoverRecordElectionConfiguration,
            DurableOperationPhase::FailoverLabelMembers,
            DurableOperationPhase::FailoverAttest,
            DurableOperationPhase::FailoverFinalize,
        ];
        assert_eq!(phases.len(), 20);
        for phase in phases {
            let mut operation = start_failover("set", snapshot(3), 1, 1, 100).unwrap();
            operation.phase = phase;
            let encoded = serde_json::to_vec(&operation).unwrap();
            let decoded: DurableOperationStatus = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(decoded, operation, "restart changed phase {phase:?}");
        }
    }

    #[test]
    fn replacement_changes_starting_configuration_action_identity() {
        let operation = start_failover("set", snapshot(3), 1, 1, 100).unwrap();
        let Decision::Persist(first) =
            decide_failover(&operation, &OperationObservations::new(), 101).unwrap()
        else {
            panic!("starting configuration must persist intent");
        };
        let first_id = first.pending_action.unwrap().action_id;

        let adopted =
            adopt_replacement_before_confirmation(&operation, 3, "replacement-3", 102).unwrap();
        let Decision::Persist(restarted) =
            decide_failover(&adopted, &OperationObservations::new(), 103).unwrap()
        else {
            panic!("recollection must persist a new intent");
        };
        assert_ne!(first_id, restarted.pending_action.unwrap().action_id);
    }

    #[test]
    fn failover_target_cannot_drop_below_minimum_replicas() {
        let mut operation = start_failover("set", snapshot(5), 1, 3, 100).unwrap();
        operation.phase = DurableOperationPhase::FailoverAssess;
        let mut two = persisted_observation(&operation, 2, true);
        let mut three = persisted_observation(&operation, 3, true);
        two.current_lsn = 20;
        three.current_lsn = 30;
        let mut four = persisted_observation(&operation, 4, true);
        four.role = "none".to_string();
        let mut five = persisted_observation(&operation, 5, true);
        five.role = "none".to_string();
        operation.failover_mut().unwrap().observations = vec![two, three, four, five];

        let error = decide_failover(&operation, &OperationObservations::new(), 101).unwrap_err();
        assert!(error.contains("below minimum"));
    }
}
