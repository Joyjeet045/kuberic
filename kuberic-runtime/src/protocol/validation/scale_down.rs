use std::collections::BTreeSet;

use crate::protocol::command::EnsureConfiguration;
use crate::protocol::observation::AgentReport;
use crate::protocol::types::*;
use crate::protocol::validation::{ValidationError, validate_configuration, validate_policy};

type Result<T = ()> = std::result::Result<T, ValidationError>;

fn invalid(reason: &'static str) -> ValidationError {
    ValidationError::InvalidSecondaryScaleDown(reason)
}

pub fn validate_secondary_scale_down(intent: &SecondaryScaleDownIntent) -> Result {
    let previous = &intent.previous_configuration;
    let current = &intent.current_configuration;
    validate_policy(&intent.previous_policy)?;
    validate_policy(&intent.current_policy)?;
    validate_configuration(previous, Some(&intent.previous_policy))?;
    validate_configuration(current, Some(&intent.current_policy))?;
    if previous.members.iter().any(|member| {
        member.identity.instance_id.is_empty() || member.identity.agent_generation.is_empty()
    }) {
        return Err(invalid(
            "membership requires exact nonempty incarnation and generation",
        ));
    }
    if intent.resource_uid.is_empty()
        || intent.spec_generation == 0
        || intent.desired_replicas == 0
        || intent.desired_replicas > intent.current_policy.replica_set_size
        || intent.operation_id != intent.expected_operation_id()
    {
        return Err(invalid("invalid frozen request or operation identity"));
    }
    if previous.epoch.data_loss_number < 0
        || previous.epoch.configuration_number < 0
        || current.epoch.data_loss_number != previous.epoch.data_loss_number
    {
        return Err(ValidationError::TransitionDataLossChanged);
    }
    if current.epoch.configuration_number <= previous.epoch.configuration_number {
        return Err(ValidationError::TransitionEpochNotNewer);
    }
    if intent.previous_policy.failover_delay_seconds != intent.current_policy.failover_delay_seconds
        || previous.members.len() != current.members.len() + 1
    {
        return Err(invalid(
            "removal must preserve delay and reduce membership by exactly one",
        ));
    }
    let primary = previous
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .expect("validated primary");
    let target = previous
        .members
        .iter()
        .filter(|member| member.role == ReplicaRole::ActiveSecondary)
        .max_by_key(|member| member.identity.replica_id)
        .ok_or_else(|| invalid("no committed secondary"))?;
    if intent.primary != primary.identity
        || intent.target != target.identity
        || previous.primary_id != current.primary_id
        || previous.members.iter().any(|member| {
            !matches!(
                member.role,
                ReplicaRole::Primary | ReplicaRole::ActiveSecondary
            )
        })
        || previous
            .members
            .iter()
            .filter(|member| member.identity != intent.target)
            .any(|member| !current.members.contains(member))
        || current
            .members
            .iter()
            .any(|member| member.identity == intent.target)
    {
        return Err(invalid(
            "target must be the highest committed secondary; retained members cannot change",
        ));
    }
    for resource in [
        &intent.cleanup.pod,
        &intent.cleanup.pvc,
        &intent.cleanup.endpoint,
    ] {
        if resource.name().is_empty()
            || matches!(resource, CleanupResourceIdentity::Present { uid, .. } if uid.is_empty())
        {
            return Err(invalid(
                "cleanup requires an exact name and UID or positive absence",
            ));
        }
    }
    if matches!(&intent.cleanup.pod, CleanupResourceIdentity::Present { uid, .. } if uid != intent.target.instance_id.as_str())
        || intent.cleanup.endpoint.name()
            != derive_replica_endpoint_name(&intent.resource_uid, &intent.target)
    {
        return Err(invalid("cleanup identity differs from frozen target"));
    }
    Ok(())
}

pub fn validate_secondary_removal_preparation(preparation: &SecondaryRemovalPreparation) -> Result {
    validate_secondary_scale_down(&preparation.intent)?;
    if preparation.operation_id
        != preparation
            .intent
            .command_operation_id(SecondaryRemovalStage::Prepare, &preparation.intent.primary)
        || preparation.process_session_id.is_empty()
        || preparation.report_sequence == 0
        || preparation.boundary_lsn < 0
    {
        return Err(invalid("invalid durable primary preparation"));
    }
    Ok(())
}

fn validate_witnesses(
    preparation: &SecondaryRemovalPreparation,
    witnesses: &[SecondaryRemovalWitness],
    stage: SecondaryRemovalStage,
) -> Result {
    let intent = &preparation.intent;
    let old = stage == SecondaryRemovalStage::Prepare;
    let configuration = if old {
        &intent.previous_configuration
    } else {
        &intent.current_configuration
    };
    let quorum = if old {
        intent.previous_policy.read_quorum
    } else {
        intent.current_policy.write_quorum
    };
    let mut identities = BTreeSet::new();
    for witness in witnesses {
        let expected_previous = (stage == SecondaryRemovalStage::PreviousCurrent)
            .then_some(&intent.previous_configuration.configuration_id);
        if witness.resource_uid != intent.resource_uid
            || witness.process_session_id.is_empty()
            || witness.report_sequence == 0
            || !intent
                .current_configuration
                .members
                .iter()
                .any(|member| member.identity == witness.identity && member.role == witness.role)
            || !identities.insert(witness.identity.clone())
            || witness.epoch != configuration.epoch
            || witness.previous_configuration_id.as_ref() != expected_previous
            || witness.current_configuration_id != configuration.configuration_id
            || witness.verified_replication_lsn < 0
            || (!old && witness.verified_replication_lsn < preparation.boundary_lsn)
            || witness.pending_operation_id.is_some()
            || witness.write_status == AccessStatus::Granted
            || (!old
                && witness.retained_operation_id.as_ref()
                    != Some(&intent.command_operation_id(stage, &witness.identity)))
        {
            return Err(invalid(
                "invalid exact quorum witness authority or verified progress",
            ));
        }
        if old
            && witness.identity == intent.primary
            && ((witness.process_session_id == preparation.process_session_id
                && witness.report_sequence < preparation.report_sequence)
                || witness.verified_replication_lsn < preparation.boundary_lsn
                || witness.retained_operation_id.as_ref() != Some(&preparation.operation_id))
        {
            return Err(invalid(
                "primary witness does not attest durable preparation",
            ));
        }
    }
    if identities.len() < quorum as usize || (!old && !identities.contains(&intent.primary)) {
        return Err(invalid(
            "insufficient exact quorum or missing reduced primary",
        ));
    }
    Ok(())
}

pub fn validate_secondary_removal_evidence(
    evidence: &SecondaryRemovalEvidence,
    require_reduced: bool,
) -> Result {
    validate_secondary_removal_preparation(&evidence.preparation)?;
    validate_witnesses(
        &evidence.preparation,
        &evidence.previous_read_quorum,
        SecondaryRemovalStage::Prepare,
    )?;
    if require_reduced || !evidence.reduced_write_quorum.is_empty() {
        validate_witnesses(
            &evidence.preparation,
            &evidence.reduced_write_quorum,
            SecondaryRemovalStage::PreviousCurrent,
        )?;
        for witness in &evidence.reduced_write_quorum {
            if evidence.previous_read_quorum.iter().any(|previous| {
                previous.identity == witness.identity
                    && previous.process_session_id == witness.process_session_id
                    && previous.report_sequence >= witness.report_sequence
            }) || (witness.identity == evidence.preparation.intent.primary
                && witness.process_session_id == evidence.preparation.process_session_id
                && witness.report_sequence <= evidence.preparation.report_sequence)
            {
                return Err(invalid("reduced-authority evidence is not fresh"));
            }
        }
    }
    Ok(())
}

pub fn validate_replica_retirement(report: &ReplicaRetirementReport) -> Result {
    validate_secondary_scale_down(&report.intent)?;
    if report.operation_id
        != report
            .intent
            .command_operation_id(SecondaryRemovalStage::Retire, &report.intent.target)
        || report.process_session_id.is_empty()
        || report.report_sequence == 0
        || report.epoch != report.intent.current_configuration.epoch
        || report.role != ReplicaRole::None
        || report.read_status == AccessStatus::Granted
        || report.write_status == AccessStatus::Granted
        || !report.application_closed
        || !report.peers_fenced
    {
        return Err(invalid(
            "retirement must attest terminal exact local fencing",
        ));
    }
    Ok(())
}

pub fn validate_secondary_scale_down_cleanup(cleanup: &SecondaryScaleDownCleanup) -> Result {
    validate_secondary_removal_evidence(&cleanup.evidence, true)?;
    validate_witnesses(
        &cleanup.evidence.preparation,
        &cleanup.current_only_write_quorum,
        SecondaryRemovalStage::CurrentOnly,
    )?;
    for witness in &cleanup.current_only_write_quorum {
        if cleanup
            .evidence
            .previous_read_quorum
            .iter()
            .chain(&cleanup.evidence.reduced_write_quorum)
            .any(|earlier| {
                earlier.identity == witness.identity
                    && earlier.process_session_id == witness.process_session_id
                    && earlier.report_sequence >= witness.report_sequence
            })
        {
            return Err(invalid("current-only evidence is not fresh"));
        }
    }
    if let Some(retirement) = &cleanup.retirement {
        validate_replica_retirement(retirement)?;
        if retirement.intent != cleanup.evidence.preparation.intent {
            return Err(invalid(
                "retirement does not bind the committed cleanup obligation",
            ));
        }
    }
    Ok(())
}

pub fn validate_accept_secondary_removal_commit(
    command: &crate::protocol::command::AcceptSecondaryRemovalCommit,
) -> Result {
    validate_secondary_scale_down_cleanup(&command.committed)?;
    let intent = &command.committed.evidence.preparation.intent;
    if !intent
        .current_configuration
        .members
        .iter()
        .any(|m| m.identity == command.target)
        || command.operation_id
            != intent.command_operation_id(SecondaryRemovalStage::AcceptCommit, &command.target)
        || command.committed.retirement.is_some()
        || (command.local_recovery && command.target == intent.primary)
    {
        return Err(invalid(
            "commit publication must bind one retained exact member and immutable acceptance evidence",
        ));
    }
    Ok(())
}

pub fn validate_secondary_removal_configuration(command: &EnsureConfiguration) -> Result {
    let evidence = command
        .secondary_removal_evidence
        .as_ref()
        .ok_or_else(|| invalid("missing admission evidence"))?;
    validate_secondary_removal_evidence(evidence, command.current_only)?;
    let intent = &evidence.preparation.intent;
    let target = ReplicaIdentity {
        replica_id: command.local_replica_id,
        instance_id: command.expected_instance_id.clone(),
        agent_generation: command.expected_agent_generation.clone(),
    };
    let stage = if command.current_only {
        SecondaryRemovalStage::CurrentOnly
    } else {
        SecondaryRemovalStage::PreviousCurrent
    };
    if command.transition_kind != TransitionKind::SecondaryScaleDown
        || command.operation_id != intent.command_operation_id(stage, &target)
        || !intent
            .current_configuration
            .members
            .iter()
            .any(|member| member.identity == target)
        || command.current_configuration != intent.current_configuration
        || command.effective_policy != intent.current_policy
        || command.previous_policy.as_ref() != Some(&intent.previous_policy)
        || command.current_epoch != intent.current_configuration.epoch
        || command.previous_configuration.as_ref()
            != (!command.current_only).then_some(&intent.previous_configuration)
        || command.previous_epoch
            != (!command.current_only).then_some(intent.previous_configuration.epoch)
        || command.primary_write_status != AccessStatus::ReconfigurationPending
        || command.failover_safe_lsn.is_some()
        || command.switchover_handoff.is_some()
        || !command.retire_build_ids.is_empty()
        || !command.retire_switchover_preparation_ids.is_empty()
    {
        return Err(invalid(
            "configuration command differs from frozen write-closed removal authority",
        ));
    }
    Ok(())
}

pub fn validate_secondary_removal_report(report: &AgentReport) -> Result {
    if let Some(committed) = &report.accepted_secondary_removal {
        validate_secondary_scale_down_cleanup(committed)?;
        let intent = &committed.evidence.preparation.intent;
        if intent.resource_uid != report.resource_uid
            || !intent
                .current_configuration
                .members
                .iter()
                .any(|m| m.identity == report.identity)
            || report.epoch < intent.current_configuration.epoch
            || (report.epoch == intent.current_configuration.epoch
                && (report.current_configuration.as_ref() != Some(&intent.current_configuration)
                    || report.previous_configuration.is_some()
                    || report.prepared_secondary_removal.as_ref().is_some_and(|p| {
                        p.intent.previous_configuration != intent.current_configuration
                    })
                    || report.secondary_removal_evidence.as_ref() != Some(&committed.evidence)))
        {
            return Err(invalid(
                "accepted removal receipt differs from installed current-only authority",
            ));
        }
    }
    if let Some(preparation) = &report.prepared_secondary_removal {
        validate_secondary_removal_preparation(preparation)?;
        if preparation.intent.primary != report.identity
            || preparation.intent.resource_uid != report.resource_uid
            || report.process_session_id.is_empty()
            || report.report_sequence == 0
            || (report.process_session_id == preparation.process_session_id
                && report.report_sequence < preparation.report_sequence)
            || report.write_status == AccessStatus::Granted
            || report.prepared_switchover.is_some()
            || (report.secondary_removal_evidence.is_none()
                && (report.current_configuration.as_ref()
                    != Some(&preparation.intent.previous_configuration)
                    || report.previous_configuration.is_some()
                    || report.epoch != preparation.intent.previous_configuration.epoch
                    || report.role != ReplicaRole::Primary))
        {
            return Err(invalid(
                "preparation report differs from write-closed primary",
            ));
        }
    }
    if let Some(evidence) = &report.secondary_removal_evidence {
        validate_secondary_removal_evidence(evidence, report.previous_configuration.is_none())?;
        let intent = &evidence.preparation.intent;
        if report.resource_uid != intent.resource_uid
            || report.process_session_id.is_empty()
            || report.report_sequence == 0
            || report.current_configuration.as_ref() != Some(&intent.current_configuration)
            || report.epoch != intent.current_configuration.epoch
            || report
                .previous_configuration
                .as_ref()
                .is_some_and(|previous| previous != &intent.previous_configuration)
            || !intent
                .current_configuration
                .members
                .iter()
                .any(|member| member.identity == report.identity && member.role == report.role)
            || (report.previous_configuration.is_some()
                && report.write_status == AccessStatus::Granted)
            || report
                .prepared_secondary_removal
                .as_ref()
                .is_some_and(|prepared| {
                    prepared != &evidence.preparation
                        && !(report.previous_configuration.is_none()
                            && prepared.intent.previous_configuration
                                == intent.current_configuration
                            && report
                                .accepted_secondary_removal
                                .as_ref()
                                .is_some_and(|c| c.evidence == *evidence))
                })
        {
            return Err(invalid(
                "reported reduced authority is not bound to admission evidence",
            ));
        }
    }
    if let Some(retirement) = &report.retired_replica {
        validate_replica_retirement(retirement)?;
        if retirement.intent.target != report.identity
            || retirement.intent.resource_uid != report.resource_uid
            || report.process_session_id.is_empty()
            || report.report_sequence == 0
            || (retirement.process_session_id == report.process_session_id
                && retirement.report_sequence > report.report_sequence)
            || retirement.epoch != report.epoch
            || report.role != ReplicaRole::None
            || report.read_status == AccessStatus::Granted
            || report.write_status == AccessStatus::Granted
            || report.previous_configuration.is_some()
            || report.current_configuration.is_some()
            || report.pending_operation_id.is_some()
            || report.retained_operation_id.as_ref() != Some(&retirement.operation_id)
            || report.prepared_secondary_removal.is_some()
            || report.secondary_removal_evidence.is_some()
            || report.prepared_switchover.is_some()
        {
            return Err(invalid("retired report still claims active authority"));
        }
    }
    Ok(())
}
