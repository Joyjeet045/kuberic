use std::collections::BTreeSet;

use crate::command::EnsureConfiguration;
use crate::types::*;
use crate::validation::{ValidationError, validate_configuration, validate_policy};

type Result<T = ()> = std::result::Result<T, ValidationError>;

fn invalid(reason: &'static str) -> ValidationError {
    ValidationError::InvalidScaleUp(reason)
}

pub fn validate_scale_up_provisioning(intent: &ProvisioningIntent) -> Result {
    if intent.pod_uid.is_empty()
        || intent.pvc_uid.is_empty()
        || intent.operation_id.is_empty()
        || intent.replica_id().value() <= 0
    {
        return Err(invalid("invalid candidate identity"));
    }
    match intent.purpose.kind {
        ProvisioningKind::Replacement => {
            let Some(replaces) = intent.purpose.replaces.as_ref() else {
                return Err(invalid("replacement provisioning purpose is malformed"));
            };
            if intent.purpose.scale_up.is_some()
                || replaces.replica_id.value() <= 0
                || replaces.instance_id.is_empty()
                || replaces.agent_generation.is_empty()
            {
                return Err(invalid("replacement provisioning purpose is malformed"));
            }
        }
        ProvisioningKind::ScaleUp
            if intent.purpose.replaces.is_some() || intent.purpose.scale_up.is_none() =>
        {
            return Err(invalid("scale-up provisioning purpose is malformed"));
        }
        _ => {}
    }
    let Some(scale_up) = intent.scale_up() else {
        return Ok(());
    };
    validate_scale_up_provisioning_request(scale_up)?;
    if intent.operation_id != intent.expected_operation_id() {
        return Err(invalid("invalid frozen provisioning request"));
    }
    Ok(())
}

pub fn validate_scale_up_provisioning_request(scale_up: &ScaleUpProvisioning) -> Result {
    validate_policy(&scale_up.previous_policy)?;
    validate_policy(&scale_up.current_policy)?;
    validate_configuration(
        &scale_up.previous_configuration,
        Some(&scale_up.previous_policy),
    )?;
    if scale_up.resource_uid.is_empty()
        || scale_up.spec_generation == 0
        || scale_up.desired_replicas == 0
        || scale_up.previous_policy.replica_set_size == u32::MAX
        || scale_up.current_policy.replica_set_size != scale_up.previous_policy.replica_set_size + 1
        || scale_up.desired_replicas < scale_up.current_policy.replica_set_size
        || scale_up.previous_policy.failover_delay_seconds
            != scale_up.current_policy.failover_delay_seconds
        || scale_up
            .previous_configuration
            .members
            .iter()
            .any(|member| member.identity.replica_id == scale_up.target_replica_id)
        || scale_up.next_configuration_epoch().is_none()
    {
        return Err(invalid("invalid frozen provisioning request"));
    }
    let ids = scale_up
        .previous_configuration
        .members
        .iter()
        .map(|member| member.identity.replica_id.value())
        .collect::<BTreeSet<_>>();
    let expected_target = (1..=i64::from(scale_up.current_policy.replica_set_size))
        .find(|candidate| !ids.contains(candidate));
    if expected_target != Some(scale_up.target_replica_id.value()) {
        return Err(invalid(
            "scale-up target must restore the first missing ordinal",
        ));
    }
    Ok(())
}

pub fn validate_scale_up_allocation(allocation: &ScaleUpAllocation) -> Result {
    if allocation.resource_uid.is_empty()
        || allocation.spec_generation == 0
        || allocation.desired_replicas == 0
        || allocation.previous_configuration_id.is_empty()
        || allocation.accepted_configuration_id.is_empty()
        || allocation.target_replica_id.value() <= 0
        || allocation.operation_id.is_empty()
        || allocation.operation_id != allocation.expected_operation_id()
        || allocation
            .previous_operation_id
            .as_ref()
            .is_some_and(|previous| previous.is_empty() || previous == &allocation.operation_id)
        || allocation.pod_uid.as_ref().is_some_and(PodUid::is_empty)
        || allocation.pvc_uid.as_ref().is_some_and(PvcUid::is_empty)
        || (allocation.pod_uid.is_some() && allocation.pvc_uid.is_none())
    {
        return Err(invalid("invalid durable scale-up allocation"));
    }
    Ok(())
}

pub fn validate_scale_up(intent: &ScaleUpIntent) -> Result {
    validate_policy(&intent.previous_policy)?;
    validate_policy(&intent.current_policy)?;
    validate_configuration(
        &intent.previous_configuration,
        Some(&intent.previous_policy),
    )?;
    validate_configuration(&intent.current_configuration, Some(&intent.current_policy))?;
    if intent.resource_uid.is_empty()
        || intent.spec_generation == 0
        || intent.desired_replicas == 0
        || intent.build_id.is_empty()
        || intent.primary.instance_id.is_empty()
        || intent.primary.agent_generation.is_empty()
        || intent.target.instance_id.is_empty()
        || intent.target.agent_generation.is_empty()
        || intent.snapshot_boundary_lsn < 0
        || intent.catch_up_boundary_lsn < intent.snapshot_boundary_lsn
        || intent.operation_id != intent.expected_operation_id()
    {
        return Err(invalid("invalid frozen request, build, or boundary"));
    }
    let previous = &intent.previous_configuration;
    let current = &intent.current_configuration;
    if intent.previous_policy.replica_set_size == u32::MAX
        || intent.current_policy.replica_set_size != intent.previous_policy.replica_set_size + 1
        || intent.desired_replicas < intent.current_policy.replica_set_size
        || intent.previous_policy.failover_delay_seconds
            != intent.current_policy.failover_delay_seconds
        || previous.members.len() + 1 != current.members.len()
    {
        return Err(invalid(
            "scale-up must preserve delay and increase membership by exactly one",
        ));
    }
    if previous.epoch.data_loss_number < 0
        || previous.epoch.configuration_number < 0
        || current.epoch.data_loss_number != previous.epoch.data_loss_number
    {
        return Err(ValidationError::TransitionDataLossChanged);
    }
    if previous
        .epoch
        .configuration_number
        .checked_add(1)
        .is_none_or(|next| current.epoch.configuration_number != next)
    {
        return Err(ValidationError::TransitionEpochNotNewer);
    }
    let primary = previous
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .expect("validated configuration has a primary");
    let added = current
        .members
        .iter()
        .filter(|member| {
            !previous
                .members
                .iter()
                .any(|old| old.identity.replica_id == member.identity.replica_id)
        })
        .collect::<Vec<_>>();
    let previous_ids = previous
        .members
        .iter()
        .map(|member| member.identity.replica_id.value())
        .collect::<BTreeSet<_>>();
    let expected_target = (1..=i64::from(intent.current_policy.replica_set_size))
        .find(|candidate| !previous_ids.contains(candidate));
    if intent.primary != primary.identity
        || previous.primary_id != current.primary_id
        || added.len() != 1
        || added[0].identity != intent.target
        || added[0].role != ReplicaRole::ActiveSecondary
        || expected_target != Some(intent.target.replica_id.value())
        || previous
            .members
            .iter()
            .any(|member| !current.members.iter().any(|candidate| candidate == member))
    {
        return Err(invalid(
            "target must be the next active secondary and retained authority cannot change",
        ));
    }
    Ok(())
}

fn validate_witnesses(
    intent: &ScaleUpIntent,
    reported_authority: &ConfigurationDescriptor,
    eligible_members: &ConfigurationDescriptor,
    witnesses: &[ScaleUpWitness],
    quorum: u32,
    previous_current: bool,
    required_primary: Option<&ReplicaIdentity>,
) -> Result {
    let mut identities = BTreeSet::new();
    for witness in witnesses {
        let current_member = reported_authority
            .members
            .iter()
            .find(|member| member.identity == witness.identity);
        if witness.resource_uid != intent.resource_uid
            || witness.process_session_id.is_empty()
            || witness.report_sequence == 0
            || !eligible_members
                .members
                .iter()
                .any(|member| member.identity == witness.identity)
            || current_member.is_none_or(|member| member.role != witness.role)
            || !identities.insert(witness.identity.clone())
            || witness.epoch != reported_authority.epoch
            || witness.previous_configuration_id.as_ref()
                != previous_current.then_some(&intent.previous_configuration.configuration_id)
            || witness.current_configuration_id != reported_authority.configuration_id
            || witness.verified_replication_lsn < intent.catch_up_boundary_lsn
            || witness.pending_operation_id.is_some()
            || (witness.write_status == AccessStatus::Granted
                && required_primary != Some(&witness.identity))
            || (!previous_current
                && required_primary == Some(&witness.identity)
                && witness.write_status != AccessStatus::Granted)
            || witness.retained_operation_id.as_ref()
                != Some(&intent.command_operation_id(
                    if previous_current {
                        ScaleUpStage::PreviousCurrent
                    } else {
                        ScaleUpStage::CurrentOnly
                    },
                    &witness.identity,
                    reported_authority,
                ))
        {
            return Err(invalid("invalid exact scale-up quorum witness"));
        }
    }
    if identities.len() < quorum as usize
        || required_primary.is_some_and(|primary| !identities.contains(primary))
    {
        return Err(invalid("insufficient scale-up quorum witnesses"));
    }
    Ok(())
}

fn validate_recovery_witnesses(
    intent: &ScaleUpIntent,
    eligible_members: &ConfigurationDescriptor,
    witnesses: &[ScaleUpWitness],
    quorum: u32,
) -> Result {
    let mut identities = BTreeSet::new();
    for witness in witnesses {
        let current_member = intent
            .current_configuration
            .members
            .iter()
            .find(|member| member.identity == witness.identity);
        let stage = match witness.previous_configuration_id.as_ref() {
            Some(previous) if previous == &intent.previous_configuration.configuration_id => {
                ScaleUpStage::PreviousCurrent
            }
            None => ScaleUpStage::CurrentOnly,
            _ => return Err(invalid("invalid exact scale-up recovery witness")),
        };
        if witness.resource_uid != intent.resource_uid
            || witness.process_session_id.is_empty()
            || witness.report_sequence == 0
            || !eligible_members
                .members
                .iter()
                .any(|member| member.identity == witness.identity)
            || current_member.is_none_or(|member| member.role != witness.role)
            || !identities.insert(witness.identity.clone())
            || witness.epoch != intent.current_configuration.epoch
            || witness.current_configuration_id != intent.current_configuration.configuration_id
            || witness.verified_replication_lsn < intent.catch_up_boundary_lsn
            || witness.pending_operation_id.is_some()
            || (witness.write_status == AccessStatus::Granted && witness.identity != intent.primary)
            || (stage == ScaleUpStage::CurrentOnly
                && witness.identity == intent.primary
                && witness.write_status != AccessStatus::Granted)
            || witness.retained_operation_id.as_ref()
                != Some(&intent.command_operation_id(
                    stage,
                    &witness.identity,
                    &intent.current_configuration,
                ))
        {
            return Err(invalid("invalid exact scale-up recovery witness"));
        }
    }
    if identities.len() < quorum as usize {
        return Err(invalid("insufficient scale-up recovery witnesses"));
    }
    Ok(())
}

fn validate_failover_configuration(
    evidence: &ScaleUpFailoverEvidence,
    current: &ConfigurationDescriptor,
    policy: &EffectivePolicy,
) -> Result {
    validate_policy(policy)?;
    validate_configuration(current, Some(policy))?;
    if policy != &evidence.intent.current_policy
        || current.epoch.data_loss_number
            != evidence.intent.current_configuration.epoch.data_loss_number
        || current.epoch.configuration_number
            <= evidence
                .intent
                .current_configuration
                .epoch
                .configuration_number
    {
        return Err(invalid("failover authority did not advance expanded CC"));
    }
    let expected = evidence
        .intent
        .current_configuration
        .members
        .iter()
        .map(|member| member.identity.clone())
        .collect::<BTreeSet<_>>();
    let actual = current
        .members
        .iter()
        .map(|member| member.identity.clone())
        .collect::<BTreeSet<_>>();
    if actual != expected {
        return Err(invalid("failover must preserve exact expanded membership"));
    }
    Ok(())
}

fn validate_final_witnesses(
    evidence: &ScaleUpFailoverEvidence,
    final_election: &ScaleUpFinalElectionEvidence,
) -> Result {
    let intent = &evidence.intent;
    let provisional = &evidence.provisional_configuration;
    let mut identities = BTreeSet::new();
    for witness in &final_election.witnesses {
        let provisional_member = provisional
            .members
            .iter()
            .find(|member| member.identity.replica_id == witness.replica_id);
        if witness.process_session_id.is_empty()
            || witness.report_sequence == 0
            || provisional_member.is_none()
            || !identities.insert(witness.replica_id)
            || witness.current_progress < intent.catch_up_boundary_lsn
            || witness.committed_lsn < 0
            || witness.deactivated_lsn < 0
            || witness.fence_operation_id
                != intent.command_operation_id(
                    ScaleUpStage::PreviousCurrent,
                    &provisional_member
                        .expect("checked final witness provisional member")
                        .identity,
                    provisional,
                )
        {
            return Err(invalid("invalid exact final scale-up election witness"));
        }
    }
    Ok(())
}

fn validate_final_quorum(
    final_election: &ScaleUpFinalElectionEvidence,
    eligible_members: &ConfigurationDescriptor,
    witnesses: &[ReplicaId],
    quorum: u32,
    required_primary: Option<&ReplicaIdentity>,
) -> Result {
    let mut identities = BTreeSet::new();
    for replica_id in witnesses {
        if final_election.witness(*replica_id).is_none()
            || !eligible_members
                .members
                .iter()
                .any(|member| member.identity.replica_id == *replica_id)
            || !identities.insert(*replica_id)
        {
            return Err(invalid("invalid exact final scale-up election quorum"));
        }
    }
    if identities.len() < quorum as usize
        || required_primary.is_some_and(|primary| !identities.contains(&primary.replica_id))
    {
        return Err(invalid(
            "insufficient exact final scale-up election witnesses",
        ));
    }
    Ok(())
}

fn validate_final_election(
    evidence: &ScaleUpFailoverEvidence,
    final_election: &ScaleUpFinalElectionEvidence,
) -> Result {
    let provisional = &evidence.provisional_configuration;
    let final_configuration = final_election
        .final_configuration(provisional)
        .ok_or_else(|| invalid("final scale-up election selected an unknown primary"))?;
    let safe_lsn = final_election
        .safe_lsn()
        .ok_or_else(|| invalid("final scale-up election omitted the selected witness"))?;
    validate_failover_configuration(
        evidence,
        &final_configuration,
        &evidence.intent.current_policy,
    )?;
    if safe_lsn < evidence.intent.catch_up_boundary_lsn
        || final_election.selected_primary_replica_id == evidence.intent.primary.replica_id
    {
        return Err(invalid(
            "final scale-up election did not advance the provisional fence",
        ));
    }
    let primary = final_configuration
        .members
        .iter()
        .find(|member| {
            member.identity.replica_id == final_configuration.primary_id
                && member.role == ReplicaRole::Primary
        })
        .ok_or_else(|| invalid("final scale-up election has no primary"))?;
    validate_final_witnesses(evidence, final_election)?;
    validate_final_quorum(
        final_election,
        &evidence.intent.previous_configuration,
        &final_election.previous_read_quorum,
        evidence.intent.previous_policy.read_quorum,
        None,
    )?;
    validate_final_quorum(
        final_election,
        provisional,
        &final_election.current_read_quorum,
        evidence.intent.current_policy.read_quorum,
        Some(&primary.identity),
    )?;
    let referenced = final_election
        .previous_read_quorum
        .iter()
        .chain(&final_election.current_read_quorum)
        .copied()
        .collect::<BTreeSet<_>>();
    if referenced.len() != final_election.witnesses.len()
        || final_election
            .witnesses
            .iter()
            .any(|witness| !referenced.contains(&witness.replica_id))
    {
        return Err(invalid(
            "final scale-up election contains unreferenced witnesses",
        ));
    }
    let selected = final_election
        .witness(final_election.selected_primary_replica_id)
        .expect("validated final current quorum contains selected primary");
    let expected = final_election
        .current_read_quorum
        .iter()
        .filter(|replica_id| **replica_id != evidence.intent.primary.replica_id)
        .filter_map(|replica_id| final_election.witness(*replica_id))
        .max_by(|left, right| {
            let left_safe = left.current_progress.min(left.deactivated_lsn);
            let right_safe = right.current_progress.min(right.deactivated_lsn);
            left_safe
                .cmp(&right_safe)
                .then_with(|| left.current_progress.cmp(&right.current_progress))
                .then_with(|| left.committed_lsn.cmp(&right.committed_lsn))
                .then_with(|| right.replica_id.cmp(&left.replica_id))
        })
        .ok_or_else(|| invalid("final scale-up election has no surviving candidate"))?;
    if selected != expected {
        return Err(invalid(
            "selected final primary did not certify the exact safe prefix",
        ));
    }
    Ok(())
}

pub fn validate_scale_up_failover_evidence(evidence: &ScaleUpFailoverEvidence) -> Result {
    validate_scale_up(&evidence.intent)?;
    validate_recovery_witnesses(
        &evidence.intent,
        &evidence.intent.previous_configuration,
        &evidence.previous_read_quorum,
        evidence.intent.previous_policy.read_quorum,
    )?;
    validate_recovery_witnesses(
        &evidence.intent,
        &evidence.intent.current_configuration,
        &evidence.current_read_quorum,
        evidence.intent.current_policy.read_quorum,
    )?;
    validate_failover_configuration(
        evidence,
        &evidence.provisional_configuration,
        &evidence.intent.current_policy,
    )?;
    if evidence
        .intent
        .current_configuration
        .epoch
        .configuration_number
        .checked_add(1)
        .is_none_or(|next| {
            evidence
                .provisional_configuration
                .epoch
                .configuration_number
                != next
        })
        || evidence.provisional_configuration.primary_id == evidence.intent.primary.replica_id
    {
        return Err(invalid(
            "provisional scale-up failover did not advance the expanded authority",
        ));
    }
    if let Some(final_election) = evidence.final_election.as_deref() {
        validate_final_election(evidence, final_election)?;
    }
    Ok(())
}

pub fn validate_scale_up_failover_transition(
    evidence: &ScaleUpFailoverEvidence,
    current: &ConfigurationDescriptor,
    policy: &EffectivePolicy,
) -> Result {
    validate_scale_up_failover_evidence(evidence)?;
    validate_failover_configuration(evidence, current, policy)?;
    if current == &evidence.provisional_configuration {
        return Ok(());
    }
    let Some(final_configuration) = evidence
        .final_election
        .as_deref()
        .and_then(|final_election| {
            final_election.final_configuration(&evidence.provisional_configuration)
        })
    else {
        return Err(invalid(
            "failover authority differs from provisional and final election evidence",
        ));
    };
    if current != &final_configuration {
        return Err(invalid(
            "failover authority differs from provisional and final election evidence",
        ));
    }
    Ok(())
}

pub fn validate_scale_up_cleanup(cleanup: &ScaleUpCleanup) -> Result {
    validate_scale_up_provisioning(&cleanup.provisioning)?;
    let Some(scale_up) = cleanup.provisioning.scale_up() else {
        return Err(invalid("cleanup requires scale-up provisioning"));
    };
    let target = cleanup.provisioning.target_identity(&scale_up.resource_uid);
    if cleanup.target != target
        || !matches!(&cleanup.resources.pod, CleanupResourceIdentity::Present { uid, .. } if uid == cleanup.provisioning.pod_uid.as_str())
        || !matches!(&cleanup.resources.pvc, CleanupResourceIdentity::Present { uid, .. } if uid == cleanup.provisioning.pvc_uid.as_str())
        || cleanup.resources.endpoint.name()
            != derive_replica_endpoint_name(&scale_up.resource_uid, &target)
    {
        return Err(invalid("cleanup differs from exact candidate"));
    }
    for resource in [
        &cleanup.resources.pod,
        &cleanup.resources.pvc,
        &cleanup.resources.endpoint,
    ] {
        if resource.name().is_empty()
            || matches!(resource, CleanupResourceIdentity::Present { uid, .. } if uid.is_empty())
        {
            return Err(invalid(
                "cleanup requires exact identity or positive absence",
            ));
        }
    }
    Ok(())
}

pub fn validate_scale_up_receipt(receipt: &ScaleUpReceipt) -> Result {
    validate_scale_up(&receipt.intent)?;
    let primary = receipt
        .accepted_configuration
        .members
        .iter()
        .find(|member| {
            member.identity.replica_id == receipt.accepted_configuration.primary_id
                && member.role == ReplicaRole::Primary
        })
        .ok_or_else(|| invalid("receipt accepted configuration has no primary"))?;
    if receipt.failover_evidence.is_some() {
        let evidence = receipt
            .expanded_failover_evidence()
            .ok_or_else(|| invalid("failover receipt has invalid provisional authority"))?;
        let safe_lsn = receipt
            .failover_safe_lsn
            .ok_or_else(|| invalid("failover receipt omitted its fenced safe LSN"))?;
        validate_scale_up_failover_transition(
            &evidence,
            &receipt.accepted_configuration,
            &receipt.intent.current_policy,
        )?;
        let final_election = evidence
            .final_election
            .as_deref()
            .ok_or_else(|| invalid("failover receipt omitted final election evidence"))?;
        if final_election
            .final_configuration(&evidence.provisional_configuration)
            .as_ref()
            != Some(&receipt.accepted_configuration)
            || final_election.safe_lsn() != Some(safe_lsn)
        {
            return Err(invalid(
                "failover receipt differs from final election evidence",
            ));
        }
        if !receipt.current_only_write_quorum.iter().any(|witness| {
            witness.identity == primary.identity
                && witness.verified_replication_lsn >= safe_lsn
                && witness.write_status == AccessStatus::Granted
        }) {
            return Err(invalid(
                "failover receipt primary did not certify the fenced safe LSN",
            ));
        }
    } else if receipt.accepted_configuration != receipt.intent.current_configuration {
        return Err(invalid(
            "ordinary receipt accepted configuration differs from scale-up intent",
        ));
    } else if receipt.failover_safe_lsn.is_some() {
        return Err(invalid(
            "ordinary receipt must not carry a failover-safe LSN",
        ));
    }
    validate_witnesses(
        &receipt.intent,
        &receipt.accepted_configuration,
        &receipt.accepted_configuration,
        &receipt.current_only_write_quorum,
        receipt.intent.current_policy.write_quorum,
        false,
        Some(&primary.identity),
    )
}

pub fn validate_scale_up_configuration(command: &EnsureConfiguration) -> Result {
    let evidence = command
        .scale_up_evidence
        .as_ref()
        .ok_or_else(|| invalid("missing scale-up configuration evidence"))?;
    let intent = evidence.intent();
    validate_scale_up(intent)?;
    if command.previous_policy.as_ref() != Some(&intent.previous_policy)
        || command.effective_policy != intent.current_policy
        || command.secondary_removal_evidence.is_some()
        || command.switchover_handoff.is_some()
        || !command.retire_switchover_preparation_ids.is_empty()
    {
        return Err(invalid(
            "configuration command differs from scale-up authority",
        ));
    }
    match &**evidence {
        ScaleUpConfigurationEvidence::Admission { .. } => {
            if command.transition_kind != TransitionKind::ScaleUp
                || command.current_configuration != intent.current_configuration
                || command.current_epoch != intent.current_configuration.epoch
                || command.failover_safe_lsn.is_some()
            {
                return Err(invalid("admission command has the wrong transition kind"));
            }
        }
        ScaleUpConfigurationEvidence::Failover { evidence } => {
            validate_scale_up_failover_evidence(evidence)?;
            let provisional = command.failover_safe_lsn.is_none()
                && !command.current_only
                && command.primary_write_status == AccessStatus::ReconfigurationPending
                && evidence.final_election.is_none()
                && command.current_configuration == evidence.provisional_configuration;
            let finalized = evidence
                .final_election
                .as_deref()
                .is_some_and(|final_election| {
                    command.failover_safe_lsn == final_election.safe_lsn()
                        && final_election
                            .final_configuration(&evidence.provisional_configuration)
                            .as_ref()
                            == Some(&command.current_configuration)
                });
            if command.transition_kind != TransitionKind::Failover
                || command.current_epoch != command.current_configuration.epoch
                || (!provisional && !finalized)
                || (!command.current_only && command.primary_write_status == AccessStatus::Granted)
                || (command.current_only && !finalized)
            {
                return Err(invalid("failover command has the wrong transition kind"));
            }
        }
    }
    if command.current_only {
        if command.previous_configuration.is_some()
            || command.previous_epoch.is_some()
            || !command
                .retire_build_ids
                .iter()
                .any(|build_id| build_id == &intent.build_id)
        {
            return Err(invalid(
                "current-only completion must omit PC and retire the scale-up build",
            ));
        }
    } else if command.previous_configuration.as_ref() != Some(&intent.previous_configuration)
        || command.previous_epoch != Some(intent.previous_configuration.epoch)
        || !command.retire_build_ids.is_empty()
    {
        return Err(invalid("PC/CC command differs from scale-up authority"));
    }
    let target = ReplicaIdentity {
        replica_id: command.local_replica_id,
        instance_id: command.expected_instance_id.clone(),
        agent_generation: command.expected_agent_generation.clone(),
    };
    if !intent
        .current_configuration
        .members
        .iter()
        .chain(intent.previous_configuration.members.iter())
        .any(|member| member.identity == target)
    {
        return Err(invalid("command target is outside scale-up authority"));
    }
    let stage = if command.current_only {
        ScaleUpStage::CurrentOnly
    } else {
        ScaleUpStage::PreviousCurrent
    };
    if command.operation_id
        != intent.command_operation_id(stage, &target, &command.current_configuration)
    {
        return Err(invalid(
            "command operation ID differs from scale-up authority",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(id: i64) -> ReplicaIdentity {
        ReplicaIdentity {
            replica_id: ReplicaId::new(id),
            instance_id: ReplicaInstanceId::new(format!("pod-{id}")),
            agent_generation: AgentGeneration::new(format!("gen-{id}")),
        }
    }

    fn member(id: i64, role: ReplicaRole) -> ConfigurationMember {
        ConfigurationMember {
            identity: identity(id),
            role,
        }
    }

    fn intent(size: u32) -> ScaleUpIntent {
        let previous_policy = EffectivePolicy::fixed(size, 30).unwrap();
        let current_policy = EffectivePolicy::fixed(size + 1, 30).unwrap();
        let previous = ConfigurationDescriptor::new(
            Epoch::new(0, 1),
            ReplicaId::new(1),
            (1..=i64::from(size))
                .map(|id| {
                    member(
                        id,
                        if id == 1 {
                            ReplicaRole::Primary
                        } else {
                            ReplicaRole::ActiveSecondary
                        },
                    )
                })
                .collect(),
            previous_policy.write_quorum,
        );
        let target = identity(i64::from(size + 1));
        let mut current_members = previous.members.clone();
        current_members.push(ConfigurationMember {
            identity: target.clone(),
            role: ReplicaRole::ActiveSecondary,
        });
        let current = ConfigurationDescriptor::new(
            Epoch::new(0, 2),
            ReplicaId::new(1),
            current_members,
            current_policy.write_quorum,
        );
        let mut intent = ScaleUpIntent {
            operation_id: OperationId::default(),
            resource_uid: ResourceUid::new("set"),
            spec_generation: 2,
            desired_replicas: size + 1,
            previous_configuration: previous,
            current_configuration: current,
            previous_policy,
            current_policy,
            primary: identity(1),
            target,
            build_id: OperationId::new("build"),
            snapshot_boundary_lsn: 0,
            catch_up_boundary_lsn: 0,
        };
        intent.operation_id = intent.expected_operation_id();
        intent
    }

    fn provisioning(intent: &ScaleUpIntent) -> ProvisioningIntent {
        let mut provisioning = ProvisioningIntent {
            purpose: ProvisioningPurpose::scale_up(ScaleUpProvisioning {
                resource_uid: intent.resource_uid.clone(),
                spec_generation: intent.spec_generation,
                desired_replicas: intent.desired_replicas,
                previous_configuration: intent.previous_configuration.clone(),
                previous_policy: intent.previous_policy.clone(),
                current_policy: intent.current_policy.clone(),
                target_replica_id: intent.target.replica_id,
            }),
            pod_uid: PodUid::new(intent.target.instance_id.as_str()),
            pvc_uid: PvcUid::new("pvc-2"),
            operation_id: OperationId::default(),
        };
        provisioning.operation_id = provisioning.expected_operation_id();
        provisioning
    }

    fn witness(
        intent: &ScaleUpIntent,
        identity: ReplicaIdentity,
        previous_current: bool,
        sequence: u64,
    ) -> ScaleUpWitness {
        let role = intent
            .current_configuration
            .members
            .iter()
            .find(|member| member.identity == identity)
            .unwrap()
            .role;
        let retained_operation_id = intent.command_operation_id(
            if previous_current {
                ScaleUpStage::PreviousCurrent
            } else {
                ScaleUpStage::CurrentOnly
            },
            &identity,
            &intent.current_configuration,
        );
        let write_status = if !previous_current && identity == intent.primary {
            AccessStatus::Granted
        } else {
            AccessStatus::ReconfigurationPending
        };
        ScaleUpWitness {
            resource_uid: intent.resource_uid.clone(),
            identity,
            role,
            process_session_id: ProcessSessionId::new(format!("session-{sequence}")),
            report_sequence: sequence,
            epoch: intent.current_configuration.epoch,
            previous_configuration_id: previous_current
                .then(|| intent.previous_configuration.configuration_id.clone()),
            current_configuration_id: intent.current_configuration.configuration_id.clone(),
            verified_replication_lsn: intent.catch_up_boundary_lsn,
            write_status,
            pending_operation_id: None,
            retained_operation_id: Some(retained_operation_id),
        }
    }

    fn failover_configuration(
        intent: &ScaleUpIntent,
        primary: &ReplicaIdentity,
        configuration_number: i64,
    ) -> ConfigurationDescriptor {
        ConfigurationDescriptor::new(
            Epoch::new(
                intent.current_configuration.epoch.data_loss_number,
                configuration_number,
            ),
            primary.replica_id,
            intent
                .current_configuration
                .members
                .iter()
                .map(|member| ConfigurationMember {
                    identity: member.identity.clone(),
                    role: if member.identity == *primary {
                        ReplicaRole::Primary
                    } else {
                        ReplicaRole::ActiveSecondary
                    },
                })
                .collect(),
            intent.current_policy.write_quorum,
        )
    }

    fn final_witness(
        intent: &ScaleUpIntent,
        provisional: &ConfigurationDescriptor,
        identity: ReplicaIdentity,
        sequence: u64,
        progress: i64,
    ) -> ScaleUpFinalWitness {
        ScaleUpFinalWitness {
            replica_id: identity.replica_id,
            fence_operation_id: intent.command_operation_id(
                ScaleUpStage::PreviousCurrent,
                &identity,
                provisional,
            ),
            process_session_id: ProcessSessionId::new(format!("final-session-{sequence}")),
            report_sequence: sequence,
            current_progress: progress,
            committed_lsn: progress,
            deactivated_lsn: progress,
        }
    }

    #[test]
    fn validates_zero_boundary_one_member_increase() {
        assert_eq!(validate_scale_up(&intent(1)), Ok(()));
    }

    #[test]
    fn rejects_membership_policy_boundary_and_operation_corruption() {
        for mutation in 0..7 {
            let mut invalid = intent(2);
            match mutation {
                0 => invalid.target.replica_id = ReplicaId::new(4),
                1 => {
                    let mut members = invalid.current_configuration.members.clone();
                    for member in &mut members {
                        member.role = if member.identity.replica_id == ReplicaId::new(2) {
                            ReplicaRole::Primary
                        } else {
                            ReplicaRole::ActiveSecondary
                        };
                    }
                    invalid.current_configuration = ConfigurationDescriptor::new(
                        invalid.current_configuration.epoch,
                        ReplicaId::new(2),
                        members,
                        invalid.current_policy.write_quorum,
                    );
                }
                2 => invalid.current_policy = EffectivePolicy::fixed(4, 30).unwrap(),
                3 => invalid.catch_up_boundary_lsn = -1,
                4 => {
                    invalid.snapshot_boundary_lsn = 2;
                    invalid.catch_up_boundary_lsn = 1;
                }
                5 => {
                    let mut members = invalid.current_configuration.members.clone();
                    members[0].identity.agent_generation = AgentGeneration::new("different");
                    invalid.current_configuration = ConfigurationDescriptor::new(
                        invalid.current_configuration.epoch,
                        invalid.current_configuration.primary_id,
                        members,
                        invalid.current_policy.write_quorum,
                    );
                }
                _ => {
                    invalid.operation_id = OperationId::new("other");
                    assert!(validate_scale_up(&invalid).is_err());
                    continue;
                }
            }
            invalid.operation_id = invalid.expected_operation_id();
            assert!(validate_scale_up(&invalid).is_err(), "mutation {mutation}");
        }
    }

    #[test]
    fn canonical_scale_up_rejects_empty_candidate_incarnation() {
        for mutation in 0..2 {
            let mut invalid = intent(2);
            if mutation == 0 {
                invalid.target.instance_id = ReplicaInstanceId::default();
            } else {
                invalid.target.agent_generation = AgentGeneration::default();
            }
            let target = invalid
                .current_configuration
                .members
                .iter_mut()
                .find(|member| member.identity.replica_id == invalid.target.replica_id)
                .unwrap();
            target.identity = invalid.target.clone();
            invalid.current_configuration = ConfigurationDescriptor::new(
                invalid.current_configuration.epoch,
                invalid.current_configuration.primary_id,
                invalid.current_configuration.members,
                invalid.current_policy.write_quorum,
            );
            invalid.operation_id = invalid.expected_operation_id();
            assert!(validate_scale_up(&invalid).is_err(), "mutation {mutation}");
        }
    }

    #[test]
    fn provisioning_is_tagged_exact_and_overflow_safe() {
        let intent = intent(1);
        let provisioning = provisioning(&intent);
        assert_eq!(validate_scale_up_provisioning(&provisioning), Ok(()));
        let build_id = provisioning
            .scale_up_build_id(&intent.resource_uid)
            .unwrap();
        let mut different_candidate = provisioning.clone();
        different_candidate.pod_uid = PodUid::new("other-pod");
        different_candidate.operation_id = different_candidate.expected_operation_id();
        assert_ne!(
            different_candidate
                .scale_up_build_id(&intent.resource_uid)
                .unwrap(),
            build_id
        );

        let mut malformed = provisioning.clone();
        malformed.purpose.replaces = Some(identity(1));
        assert!(validate_scale_up_provisioning(&malformed).is_err());

        let mut overflow = provisioning;
        let scale_up = overflow.purpose.scale_up.as_mut().unwrap();
        scale_up.previous_configuration = ConfigurationDescriptor::new(
            Epoch::new(0, i64::MAX),
            ReplicaId::new(1),
            vec![member(1, ReplicaRole::Primary)],
            scale_up.previous_policy.write_quorum,
        );
        overflow.operation_id = overflow.expected_operation_id();
        assert!(validate_scale_up_provisioning(&overflow).is_err());
    }

    #[test]
    fn cleanup_is_bound_to_fresh_candidate_resources() {
        let intent = intent(1);
        let provisioning = provisioning(&intent);
        let target = provisioning.target_identity(&intent.resource_uid);
        let cleanup = ScaleUpCleanup {
            provisioning,
            target: target.clone(),
            resources: ReplicaCleanupIdentity {
                pod: CleanupResourceIdentity::Present {
                    name: "db-1".into(),
                    uid: target.instance_id.to_string(),
                },
                pvc: CleanupResourceIdentity::Present {
                    name: "db-1-data".into(),
                    uid: "pvc-2".into(),
                },
                endpoint: CleanupResourceIdentity::Present {
                    name: derive_replica_endpoint_name(&intent.resource_uid, &target),
                    uid: "service-2".into(),
                },
            },
        };
        assert_eq!(validate_scale_up_cleanup(&cleanup), Ok(()));
        let mut malformed_lineage = cleanup.clone();
        malformed_lineage.provisioning.operation_id = OperationId::new("stale-retry-lineage");
        assert!(validate_scale_up_cleanup(&malformed_lineage).is_err());
        let mut replaced = cleanup;
        replaced.target.instance_id = ReplicaInstanceId::new("replacement");
        assert!(validate_scale_up_cleanup(&replaced).is_err());
    }

    #[test]
    fn failover_evidence_uses_independent_read_quorums() {
        let intent = intent(2);
        let previous = intent.previous_configuration.members[0].identity.clone();
        let current_a = intent.current_configuration.members[0].identity.clone();
        let current_b = intent.current_configuration.members[1].identity.clone();
        let provisional = failover_configuration(&intent, &current_b, 3);
        let evidence = ScaleUpFailoverEvidence {
            provisional_configuration: provisional,
            previous_read_quorum: vec![witness(&intent, previous, true, 1)],
            current_read_quorum: vec![
                witness(&intent, current_a, true, 2),
                witness(&intent, current_b, true, 3),
            ],
            final_election: None,
            intent,
        };
        assert_eq!(validate_scale_up_failover_evidence(&evidence), Ok(()));
        let mut insufficient = evidence;
        insufficient.current_read_quorum.pop();
        assert!(validate_scale_up_failover_evidence(&insufficient).is_err());
    }

    #[test]
    fn failover_commands_bind_the_superseding_configuration() {
        let intent = intent(2);
        let previous_witness = witness(
            &intent,
            intent.previous_configuration.members[1].identity.clone(),
            true,
            1,
        );
        let current_witnesses = vec![
            witness(
                &intent,
                intent.current_configuration.members[1].identity.clone(),
                true,
                2,
            ),
            witness(
                &intent,
                intent.current_configuration.members[2].identity.clone(),
                true,
                3,
            ),
        ];
        let mut members = intent.current_configuration.members.clone();
        for member in &mut members {
            member.role = if member.identity.replica_id == ReplicaId::new(2) {
                ReplicaRole::Primary
            } else {
                ReplicaRole::ActiveSecondary
            };
        }
        let failover = ConfigurationDescriptor::new(
            Epoch::new(0, 3),
            ReplicaId::new(2),
            members,
            intent.current_policy.write_quorum,
        );
        let evidence = ScaleUpFailoverEvidence {
            intent: intent.clone(),
            provisional_configuration: failover.clone(),
            previous_read_quorum: vec![previous_witness],
            current_read_quorum: current_witnesses,
            final_election: None,
        };
        let target = failover
            .members
            .iter()
            .find(|member| member.identity.replica_id == ReplicaId::new(2))
            .unwrap()
            .identity
            .clone();
        let admission_id = intent.command_operation_id(
            ScaleUpStage::PreviousCurrent,
            &target,
            &intent.current_configuration,
        );
        let failover_id =
            intent.command_operation_id(ScaleUpStage::PreviousCurrent, &target, &failover);
        assert_ne!(admission_id, failover_id);
        let command = EnsureConfiguration {
            operation_id: failover_id,
            previous_configuration: Some(intent.previous_configuration.clone()),
            current_configuration: failover.clone(),
            previous_epoch: Some(intent.previous_configuration.epoch),
            current_epoch: failover.epoch,
            effective_policy: intent.current_policy.clone(),
            previous_policy: Some(intent.previous_policy.clone()),
            secondary_removal_evidence: None,
            scale_up_evidence: Some(Box::new(ScaleUpConfigurationEvidence::Failover {
                evidence,
            })),
            local_replica_id: target.replica_id,
            expected_instance_id: target.instance_id,
            expected_agent_generation: target.agent_generation,
            transition_kind: TransitionKind::Failover,
            failover_safe_lsn: None,
            primary_write_status: AccessStatus::ReconfigurationPending,
            current_only: false,
            retire_build_ids: Vec::new(),
            switchover_handoff: None,
            retire_switchover_preparation_ids: Vec::new(),
        };
        assert_eq!(validate_scale_up_configuration(&command), Ok(()));
    }

    #[test]
    fn final_failover_evidence_binds_exact_fenced_reports_primary_and_safe_prefix() {
        let intent = intent(2);
        let provisional_primary = intent.previous_configuration.members[1].identity.clone();
        let final_primary = intent.target.clone();
        let provisional = failover_configuration(&intent, &provisional_primary, 3);
        let final_configuration = failover_configuration(&intent, &final_primary, 4);
        let previous = witness(&intent, provisional_primary.clone(), true, 1);
        let current = vec![
            witness(&intent, provisional_primary.clone(), true, 2),
            witness(&intent, final_primary.clone(), true, 3),
        ];
        let provisional_witness =
            final_witness(&intent, &provisional, provisional_primary.clone(), 4, 10);
        let selected_witness = final_witness(&intent, &provisional, final_primary.clone(), 5, 11);
        let evidence = ScaleUpFailoverEvidence {
            intent: intent.clone(),
            provisional_configuration: provisional,
            previous_read_quorum: vec![previous],
            current_read_quorum: current,
            final_election: Some(Box::new(ScaleUpFinalElectionEvidence {
                selected_primary_replica_id: final_configuration.primary_id,
                witnesses: vec![provisional_witness, selected_witness],
                previous_read_quorum: vec![provisional_primary.replica_id],
                current_read_quorum: vec![provisional_primary.replica_id, final_primary.replica_id],
            })),
        };
        assert_eq!(validate_scale_up_failover_evidence(&evidence), Ok(()));
        let receipt_evidence = ScaleUpFailoverReceiptEvidence::from_evidence(&evidence).unwrap();
        assert_eq!(receipt_evidence.expand(&intent), Some(evidence.clone()));
        let mut wrong_receipt_reference = receipt_evidence;
        wrong_receipt_reference.provisional_primary_replica_id = ReplicaId::new(99);
        assert!(wrong_receipt_reference.expand(&intent).is_none());

        let command = EnsureConfiguration {
            operation_id: intent.command_operation_id(
                ScaleUpStage::PreviousCurrent,
                &final_primary,
                &final_configuration,
            ),
            previous_configuration: Some(intent.previous_configuration.clone()),
            current_configuration: final_configuration.clone(),
            previous_epoch: Some(intent.previous_configuration.epoch),
            current_epoch: final_configuration.epoch,
            effective_policy: intent.current_policy.clone(),
            previous_policy: Some(intent.previous_policy.clone()),
            secondary_removal_evidence: None,
            scale_up_evidence: Some(Box::new(ScaleUpConfigurationEvidence::Failover {
                evidence: evidence.clone(),
            })),
            local_replica_id: final_primary.replica_id,
            expected_instance_id: final_primary.instance_id.clone(),
            expected_agent_generation: final_primary.agent_generation.clone(),
            transition_kind: TransitionKind::Failover,
            failover_safe_lsn: Some(11),
            primary_write_status: AccessStatus::ReconfigurationPending,
            current_only: false,
            retire_build_ids: Vec::new(),
            switchover_handoff: None,
            retire_switchover_preparation_ids: Vec::new(),
        };
        assert_eq!(validate_scale_up_configuration(&command), Ok(()));

        let mut missing = command.clone();
        let Some(ScaleUpConfigurationEvidence::Failover {
            evidence: missing_evidence,
        }) = missing.scale_up_evidence.as_deref_mut()
        else {
            unreachable!()
        };
        missing_evidence.final_election = None;
        assert!(validate_scale_up_configuration(&missing).is_err());

        let mut stale_primary = evidence.clone();
        stale_primary
            .final_election
            .as_mut()
            .unwrap()
            .selected_primary_replica_id = intent.primary.replica_id;
        assert!(validate_scale_up_failover_evidence(&stale_primary).is_err());

        let mut duplicate_witness = evidence.clone();
        let duplicate = duplicate_witness.final_election.as_ref().unwrap().witnesses[0].clone();
        duplicate_witness
            .final_election
            .as_mut()
            .unwrap()
            .witnesses
            .push(duplicate);
        assert!(validate_scale_up_failover_evidence(&duplicate_witness).is_err());

        let mut mismatched_lsn = evidence.clone();
        mismatched_lsn.final_election.as_mut().unwrap().witnesses[0].current_progress = 12;
        mismatched_lsn.final_election.as_mut().unwrap().witnesses[0].deactivated_lsn = 12;
        assert!(validate_scale_up_failover_evidence(&mismatched_lsn).is_err());
    }

    #[test]
    fn representative_scale_up_status_variants_fit_frozen_linear_guards_and_reject_quadratic_mutations()
     {
        #[derive(Clone, Copy)]
        struct FrozenGrowthGuard {
            phase: &'static str,
            fixed_bytes: usize,
            per_member_bytes: usize,
        }

        // These are reviewed regression guards for the current wire contract, not
        // advertised Kubernetes object limits or supported replica capacities.
        // The fixed component includes a deliberate encoding margin; the
        // per-member component is frozen above the largest observed adjacent
        // fixture delta, including the witness-count steps in receipt variants.
        const FROZEN_GROWTH_GUARDS: [FrozenGrowthGuard; 12] = [
            FrozenGrowthGuard {
                phase: "stable",
                fixed_bytes: 384,
                per_member_bytes: 128,
            },
            FrozenGrowthGuard {
                phase: "allocation-scaffolding",
                fixed_bytes: 768,
                per_member_bytes: 128,
            },
            FrozenGrowthGuard {
                phase: "allocation-frozen-pvc",
                fixed_bytes: 800,
                per_member_bytes: 128,
            },
            FrozenGrowthGuard {
                phase: "allocation-cancellation",
                fixed_bytes: 768,
                per_member_bytes: 128,
            },
            FrozenGrowthGuard {
                phase: "allocation-retry-lineage",
                fixed_bytes: 896,
                per_member_bytes: 128,
            },
            FrozenGrowthGuard {
                phase: "provisioning",
                fixed_bytes: 896,
                per_member_bytes: 224,
            },
            FrozenGrowthGuard {
                phase: "active-build",
                fixed_bytes: 1_152,
                per_member_bytes: 224,
            },
            FrozenGrowthGuard {
                phase: "pc-cc",
                fixed_bytes: 2_688,
                per_member_bytes: 512,
            },
            FrozenGrowthGuard {
                phase: "carried-failover",
                fixed_bytes: 3_200,
                per_member_bytes: 1_400,
            },
            FrozenGrowthGuard {
                phase: "cleanup",
                fixed_bytes: 1_216,
                per_member_bytes: 224,
            },
            FrozenGrowthGuard {
                phase: "committed-degraded",
                fixed_bytes: 2_176,
                per_member_bytes: 960,
            },
            FrozenGrowthGuard {
                phase: "latest-receipt",
                fixed_bytes: 1_792,
                per_member_bytes: 960,
            },
        ];
        const QUADRATIC_MUTATION_SAMPLE_MEMBERS: u32 = 18;
        const QUADRATIC_BYTES_PER_MEMBER_PAIR: usize = 128;
        const COMPACT_CARRIED_FAILOVER_18_MAX_BYTES: usize = 27_000;

        fn bound(guard: FrozenGrowthGuard, members: u32) -> usize {
            guard.fixed_bytes + usize::try_from(members).unwrap() * guard.per_member_bytes
        }

        let mut samples = std::collections::BTreeMap::<&str, Vec<(u32, usize)>>::new();
        let mut quadratic_mutations = std::collections::BTreeMap::<&str, usize>::new();
        for previous_count in [1_u32, 2, 3, 5, 9, 17] {
            let mut intent = intent(previous_count);
            let provisioning = provisioning(&intent);
            let target = provisioning.target_identity(&intent.resource_uid);
            intent.target = target.clone();
            let mut expanded_members = intent.current_configuration.members.clone();
            expanded_members
                .iter_mut()
                .find(|member| member.identity.replica_id == target.replica_id)
                .unwrap()
                .identity = target.clone();
            intent.current_configuration = ConfigurationDescriptor::new(
                intent.current_configuration.epoch,
                intent.current_configuration.primary_id,
                expanded_members,
                intent.current_policy.write_quorum,
            );
            intent.build_id = provisioning
                .scale_up_build_id(&intent.resource_uid)
                .unwrap();
            intent.operation_id = intent.expected_operation_id();
            let receipt = ScaleUpReceipt {
                accepted_configuration: intent.current_configuration.clone(),
                failover_evidence: None,
                failover_safe_lsn: None,
                current_only_write_quorum: intent
                    .current_configuration
                    .members
                    .iter()
                    .take(intent.current_policy.write_quorum as usize)
                    .enumerate()
                    .map(|(index, member)| {
                        witness(&intent, member.identity.clone(), false, index as u64 + 1)
                    })
                    .collect(),
                intent: intent.clone(),
            };
            validate_scale_up_receipt(&receipt).unwrap();
            let cleanup = ScaleUpCleanup {
                provisioning: provisioning.clone(),
                target: target.clone(),
                resources: ReplicaCleanupIdentity {
                    pod: CleanupResourceIdentity::Present {
                        name: format!("db-{}", target.replica_id),
                        uid: target.instance_id.to_string(),
                    },
                    pvc: CleanupResourceIdentity::Present {
                        name: format!("db-{}-data", target.replica_id),
                        uid: provisioning.pvc_uid.to_string(),
                    },
                    endpoint: CleanupResourceIdentity::Present {
                        name: derive_replica_endpoint_name(&receipt.intent.resource_uid, &target),
                        uid: format!("service-{}", target.replica_id),
                    },
                },
            };
            let stable = AcceptedStatus {
                initialized: true,
                effective_policy: Some(intent.previous_policy.clone()),
                topology: Some(AcceptedTopology {
                    configuration: intent.previous_configuration.clone(),
                }),
                ..Default::default()
            };
            let mut allocation = ScaleUpAllocation {
                resource_uid: intent.resource_uid.clone(),
                spec_generation: intent.spec_generation,
                desired_replicas: intent.desired_replicas,
                previous_configuration_id: intent.previous_configuration.configuration_id.clone(),
                accepted_configuration_id: intent.previous_configuration.configuration_id.clone(),
                target_replica_id: intent.target.replica_id,
                operation_id: OperationId::default(),
                previous_operation_id: None,
                scaffolding_requested: false,
                pod_uid: None,
                pvc_uid: None,
                cancellation_started: false,
            };
            allocation.operation_id = allocation.expected_operation_id();
            let allocation_scaffolding = AcceptedStatus {
                scale_up_allocation: Some(ScaleUpAllocation {
                    scaffolding_requested: true,
                    ..allocation.clone()
                }),
                ..stable.clone()
            };
            let allocation_frozen_pvc = AcceptedStatus {
                scale_up_allocation: Some(ScaleUpAllocation {
                    scaffolding_requested: true,
                    pvc_uid: Some(PvcUid::new(format!(
                        "scale-up-data-{}-uid",
                        intent.target.replica_id
                    ))),
                    ..allocation.clone()
                }),
                ..stable.clone()
            };
            let allocation_cancellation = AcceptedStatus {
                scale_up_allocation: Some(ScaleUpAllocation {
                    scaffolding_requested: true,
                    cancellation_started: true,
                    ..allocation.clone()
                }),
                ..stable.clone()
            };
            let allocation_retry = AcceptedStatus {
                scale_up_allocation: Some(ScaleUpAllocation {
                    operation_id: {
                        let mut retry = ScaleUpAllocation {
                            previous_operation_id: Some(allocation.operation_id.clone()),
                            ..allocation.clone()
                        };
                        retry.operation_id = retry.expected_operation_id();
                        retry.operation_id
                    },
                    previous_operation_id: Some(allocation.operation_id.clone()),
                    ..allocation.clone()
                }),
                ..stable.clone()
            };
            let pc_cc = AcceptedStatus {
                provisioning: Some(provisioning.clone()),
                transition: Some(TransitionIntent {
                    transition_id: intent
                        .transition_id(TransitionKind::ScaleUp, &intent.current_configuration),
                    kind: TransitionKind::ScaleUp,
                    spec_generation: intent.spec_generation,
                    effective_policy: intent.current_policy.clone(),
                    previous_configuration_id: Some(
                        intent.previous_configuration.configuration_id.clone(),
                    ),
                    current_configuration: intent.current_configuration.clone(),
                    election_lsn: None,
                    build_id: Some(intent.build_id.clone()),
                    repair: None,
                    switchover: None,
                    secondary_scale_down: None,
                    secondary_removal_evidence: None,
                    scale_up: Some(Box::new(intent.clone())),
                    scale_up_failover: None,
                }),
                scale_up_admission_started: Some(intent.operation_id.clone()),
                ..stable.clone()
            };
            let committed = AcceptedStatus {
                effective_policy: Some(intent.current_policy.clone()),
                topology: Some(AcceptedTopology {
                    configuration: intent.current_configuration.clone(),
                }),
                last_scale_up: Some(Box::new(receipt.clone())),
                ..stable.clone()
            };
            let active_build = AcceptedStatus {
                provisioning: Some(provisioning.clone()),
                conditions: vec![StatusCondition {
                    type_: "Progressing".into(),
                    status: ConditionStatus::True,
                    reason: "ScaleUpBuildActive".into(),
                    message: format!(
                        "copying {:?} through durable catch-up LSN {}",
                        target, intent.catch_up_boundary_lsn
                    ),
                }],
                ..stable.clone()
            };
            let committed_degraded = AcceptedStatus {
                conditions: vec![
                    StatusCondition {
                        type_: "Ready".into(),
                        status: ConditionStatus::Unknown,
                        reason: "ScaleUpCommittedDegraded".into(),
                        message: format!(
                            "accepted member {:?} has not reported current-only authority",
                            intent
                                .previous_configuration
                                .members
                                .last()
                                .unwrap()
                                .identity
                        ),
                    },
                    StatusCondition {
                        type_: "Progressing".into(),
                        status: ConditionStatus::True,
                        reason: "ScaleUpCommittedDegraded".into(),
                        message: "exact late-member local convergence remains pending".into(),
                    },
                ],
                ..committed.clone()
            };
            let failover_members = intent
                .current_configuration
                .members
                .iter()
                .map(|member| ConfigurationMember {
                    identity: member.identity.clone(),
                    role: if member.identity == intent.target {
                        ReplicaRole::Primary
                    } else {
                        ReplicaRole::ActiveSecondary
                    },
                })
                .collect();
            let failover = ConfigurationDescriptor::new(
                Epoch::new(
                    intent.current_configuration.epoch.data_loss_number,
                    intent.current_configuration.epoch.configuration_number + 1,
                ),
                intent.target.replica_id,
                failover_members,
                intent.current_policy.write_quorum,
            );
            let previous_read_quorum = intent
                .previous_configuration
                .members
                .iter()
                .take(intent.previous_policy.read_quorum as usize)
                .enumerate()
                .map(|(index, member)| {
                    witness(&intent, member.identity.clone(), true, index as u64 + 20)
                })
                .collect();
            let current_read_quorum = std::iter::once(intent.target.clone())
                .chain(
                    intent
                        .current_configuration
                        .members
                        .iter()
                        .filter(|member| member.identity != intent.target)
                        .map(|member| member.identity.clone()),
                )
                .take(intent.current_policy.read_quorum as usize)
                .enumerate()
                .map(|(index, identity)| witness(&intent, identity, true, index as u64 + 40))
                .collect();
            let final_configuration = failover_configuration(
                &intent,
                &intent.target,
                failover.epoch.configuration_number + 1,
            );
            let final_previous_read_quorum: Vec<ScaleUpFinalWitness> = intent
                .previous_configuration
                .members
                .iter()
                .take(intent.previous_policy.read_quorum as usize)
                .map(|member| {
                    final_witness(
                        &intent,
                        &failover,
                        member.identity.clone(),
                        u64::try_from(member.identity.replica_id.value()).unwrap() + 60,
                        if member.identity == intent.target {
                            intent.catch_up_boundary_lsn + 1
                        } else {
                            intent.catch_up_boundary_lsn
                        },
                    )
                })
                .collect();
            let final_current_read_quorum: Vec<ScaleUpFinalWitness> =
                std::iter::once(intent.target.clone())
                    .chain(
                        intent
                            .current_configuration
                            .members
                            .iter()
                            .filter(|member| member.identity != intent.target)
                            .map(|member| member.identity.clone()),
                    )
                    .take(intent.current_policy.read_quorum as usize)
                    .map(|identity| {
                        let progress = if identity == intent.target {
                            intent.catch_up_boundary_lsn + 1
                        } else {
                            intent.catch_up_boundary_lsn
                        };
                        let sequence = u64::try_from(identity.replica_id.value()).unwrap() + 60;
                        final_witness(&intent, &failover, identity, sequence, progress)
                    })
                    .collect();
            let failover_evidence = ScaleUpFailoverEvidence {
                intent: intent.clone(),
                provisional_configuration: failover.clone(),
                previous_read_quorum,
                current_read_quorum,
                final_election: Some(Box::new(ScaleUpFinalElectionEvidence {
                    selected_primary_replica_id: final_configuration.primary_id,
                    witnesses: final_previous_read_quorum
                        .iter()
                        .chain(&final_current_read_quorum)
                        .fold(Vec::new(), |mut witnesses, witness| {
                            if !witnesses.iter().any(|existing: &ScaleUpFinalWitness| {
                                existing.replica_id == witness.replica_id
                            }) {
                                witnesses.push(witness.clone());
                            }
                            witnesses
                        }),
                    previous_read_quorum: final_previous_read_quorum
                        .iter()
                        .map(|witness| witness.replica_id)
                        .collect(),
                    current_read_quorum: final_current_read_quorum
                        .iter()
                        .map(|witness| witness.replica_id)
                        .collect(),
                })),
            };
            validate_scale_up_failover_transition(
                &failover_evidence,
                &final_configuration,
                &intent.current_policy,
            )
            .unwrap();
            let carried_failover = AcceptedStatus {
                provisioning: Some(provisioning.clone()),
                transition: Some(TransitionIntent {
                    transition_id: intent
                        .transition_id(TransitionKind::Failover, &final_configuration),
                    kind: TransitionKind::Failover,
                    spec_generation: intent.spec_generation,
                    effective_policy: intent.current_policy.clone(),
                    previous_configuration_id: Some(
                        intent.previous_configuration.configuration_id.clone(),
                    ),
                    current_configuration: final_configuration,
                    election_lsn: Some(intent.catch_up_boundary_lsn + 1),
                    build_id: Some(intent.build_id.clone()),
                    repair: None,
                    switchover: None,
                    secondary_scale_down: None,
                    secondary_removal_evidence: None,
                    scale_up: None,
                    scale_up_failover: Some(Box::new(failover_evidence)),
                }),
                scale_up_admission_started: Some(intent.operation_id.clone()),
                ..stable.clone()
            };
            let statuses = [
                ("stable", stable.clone()),
                ("allocation-scaffolding", allocation_scaffolding),
                ("allocation-frozen-pvc", allocation_frozen_pvc),
                ("allocation-cancellation", allocation_cancellation),
                ("allocation-retry-lineage", allocation_retry),
                (
                    "provisioning",
                    AcceptedStatus {
                        provisioning: Some(provisioning.clone()),
                        ..stable.clone()
                    },
                ),
                ("active-build", active_build),
                ("pc-cc", pc_cc),
                ("carried-failover", carried_failover),
                (
                    "cleanup",
                    AcceptedStatus {
                        scale_up_cleanup: Some(Box::new(cleanup)),
                        ..stable.clone()
                    },
                ),
                ("committed-degraded", committed_degraded),
                ("latest-receipt", committed),
            ];
            for (phase, status) in statuses {
                crate::validation::validate_status(&status).unwrap();
                let size = serde_json::to_vec(&status).unwrap().len();
                let count = previous_count + 1;
                samples.entry(phase).or_default().push((count, size));
                eprintln!("scale-up-status phase={phase} members={count} bytes={size}");
                if count == QUADRATIC_MUTATION_SAMPLE_MEMBERS {
                    let pair_count = usize::try_from(count).unwrap().pow(2);
                    let mutated = serde_json::to_vec(&serde_json::json!({
                        "status": status,
                        "memberPairEvidence": "x".repeat(
                            QUADRATIC_BYTES_PER_MEMBER_PAIR * pair_count
                        ),
                    }))
                    .unwrap()
                    .len();
                    quadratic_mutations.insert(phase, mutated);
                }
            }
        }

        assert_eq!(samples.len(), FROZEN_GROWTH_GUARDS.len());
        assert_eq!(quadratic_mutations.len(), FROZEN_GROWTH_GUARDS.len());
        assert!(
            samples["carried-failover"].iter().any(|&(members, bytes)| {
                members == QUADRATIC_MUTATION_SAMPLE_MEMBERS
                    && bytes <= COMPACT_CARRIED_FAILOVER_18_MAX_BYTES
            }),
            "18-member carried failover exceeded compact final-election status guard"
        );
        for guard in FROZEN_GROWTH_GUARDS {
            let phase_samples = samples
                .get(guard.phase)
                .unwrap_or_else(|| panic!("missing samples for {}", guard.phase));
            for (count, size) in phase_samples {
                assert!(
                    *size <= bound(guard, *count),
                    "{} status bytes {} exceeded frozen {} + {}*{} guard",
                    guard.phase,
                    size,
                    guard.fixed_bytes,
                    guard.per_member_bytes,
                    count
                );
            }
            for window in phase_samples.windows(2) {
                let added_members = usize::try_from(window[1].0 - window[0].0).unwrap();
                let added_bytes = window[1].1.saturating_sub(window[0].1);
                assert!(
                    added_bytes <= added_members * guard.per_member_bytes,
                    "{} actual field/member addition grew {} bytes across {} members; \
                     frozen per-member margin is {}",
                    guard.phase,
                    added_bytes,
                    added_members,
                    guard.per_member_bytes
                );
            }
            let mutated = quadratic_mutations[guard.phase];
            assert!(
                mutated > bound(guard, QUADRATIC_MUTATION_SAMPLE_MEMBERS),
                "{} +{}*N^2 mutation unexpectedly fit frozen linear guard: {} <= {}",
                guard.phase,
                QUADRATIC_BYTES_PER_MEMBER_PAIR,
                mutated,
                bound(guard, QUADRATIC_MUTATION_SAMPLE_MEMBERS)
            );
        }
    }

    #[test]
    fn receipt_witnesses_are_bound_to_the_exact_build_attempt() {
        let intent = intent(2);
        let mut receipt = ScaleUpReceipt {
            accepted_configuration: intent.current_configuration.clone(),
            failover_evidence: None,
            failover_safe_lsn: None,
            current_only_write_quorum: intent
                .current_configuration
                .members
                .iter()
                .take(intent.current_policy.write_quorum as usize)
                .enumerate()
                .map(|(index, member)| {
                    witness(&intent, member.identity.clone(), false, index as u64 + 1)
                })
                .collect(),
            intent,
        };
        validate_scale_up_receipt(&receipt).unwrap();
        receipt.intent.build_id = OperationId::new("other-build");
        receipt.intent.operation_id = receipt.intent.expected_operation_id();
        assert!(validate_scale_up_receipt(&receipt).is_err());
    }

    #[test]
    fn active_transition_binds_accepted_and_expanded_authority() {
        let mut intent = intent(2);
        let provisioning = provisioning(&intent);
        let target = provisioning.target_identity(&intent.resource_uid);
        intent.target = target.clone();
        let mut members = intent.current_configuration.members.clone();
        members
            .iter_mut()
            .find(|member| member.identity.replica_id == target.replica_id)
            .unwrap()
            .identity = target.clone();
        intent.current_configuration = ConfigurationDescriptor::new(
            intent.current_configuration.epoch,
            intent.current_configuration.primary_id,
            members,
            intent.current_policy.write_quorum,
        );
        intent.build_id = provisioning
            .scale_up_build_id(&intent.resource_uid)
            .unwrap();
        intent.operation_id = intent.expected_operation_id();
        let status = AcceptedStatus {
            initialized: true,
            effective_policy: Some(intent.previous_policy.clone()),
            topology: Some(AcceptedTopology {
                configuration: intent.previous_configuration.clone(),
            }),
            provisioning: Some(provisioning),
            transition: Some(TransitionIntent {
                transition_id: intent
                    .transition_id(TransitionKind::ScaleUp, &intent.current_configuration),
                kind: TransitionKind::ScaleUp,
                spec_generation: intent.spec_generation,
                effective_policy: intent.current_policy.clone(),
                previous_configuration_id: Some(
                    intent.previous_configuration.configuration_id.clone(),
                ),
                current_configuration: intent.current_configuration.clone(),
                election_lsn: None,
                build_id: Some(intent.build_id.clone()),
                repair: None,
                switchover: None,
                secondary_scale_down: None,
                secondary_removal_evidence: None,
                scale_up: Some(Box::new(intent.clone())),
                scale_up_failover: None,
            }),
            ..Default::default()
        };
        assert_eq!(crate::validation::validate_status(&status), Ok(()));

        let mut invalid = status;
        invalid
            .transition
            .as_mut()
            .unwrap()
            .current_configuration
            .primary_id = ReplicaId::new(2);
        assert!(crate::validation::validate_status(&invalid).is_err());
    }

    #[test]
    fn pending_candidate_cleanup_allows_ordinary_primary_failover() {
        let intent = intent(2);
        let provisioning = provisioning(&intent);
        let target = provisioning.target_identity(&intent.resource_uid);
        let cleanup = ScaleUpCleanup {
            provisioning,
            target: target.clone(),
            resources: ReplicaCleanupIdentity {
                pod: CleanupResourceIdentity::Present {
                    name: "db-2".into(),
                    uid: target.instance_id.to_string(),
                },
                pvc: CleanupResourceIdentity::Present {
                    name: "db-2-data".into(),
                    uid: "pvc-2".into(),
                },
                endpoint: CleanupResourceIdentity::Present {
                    name: derive_replica_endpoint_name(&intent.resource_uid, &target),
                    uid: "service-2".into(),
                },
            },
        };
        let mut members = intent.previous_configuration.members.clone();
        for member in &mut members {
            member.role = if member.identity.replica_id == ReplicaId::new(2) {
                ReplicaRole::Primary
            } else {
                ReplicaRole::ActiveSecondary
            };
        }
        let failover = ConfigurationDescriptor::new(
            Epoch::new(0, 2),
            ReplicaId::new(2),
            members,
            intent.previous_policy.write_quorum,
        );
        let status = AcceptedStatus {
            initialized: true,
            effective_policy: Some(intent.previous_policy.clone()),
            topology: Some(AcceptedTopology {
                configuration: intent.previous_configuration.clone(),
            }),
            transition: Some(TransitionIntent {
                transition_id: derive_transition_id(
                    &intent.resource_uid,
                    TransitionKind::Failover,
                    &failover.configuration_id,
                ),
                kind: TransitionKind::Failover,
                spec_generation: intent.spec_generation,
                effective_policy: intent.previous_policy.clone(),
                previous_configuration_id: Some(
                    intent.previous_configuration.configuration_id.clone(),
                ),
                current_configuration: failover,
                election_lsn: Some(0),
                build_id: None,
                repair: None,
                switchover: None,
                secondary_scale_down: None,
                secondary_removal_evidence: None,
                scale_up: None,
                scale_up_failover: None,
            }),
            scale_up_cleanup: Some(Box::new(cleanup)),
            ..Default::default()
        };
        assert_eq!(crate::validation::validate_status(&status), Ok(()));
        let mut accepted = status;
        accepted.topology = Some(AcceptedTopology {
            configuration: accepted
                .transition
                .as_ref()
                .unwrap()
                .current_configuration
                .clone(),
        });
        accepted.transition = None;
        assert_eq!(crate::validation::validate_status(&accepted), Ok(()));
    }

    #[test]
    fn pending_candidate_cleanup_rejects_failover_that_admits_the_candidate() {
        let mut intent = intent(1);
        let provisioning = provisioning(&intent);
        let candidate = provisioning.target_identity(&intent.resource_uid);
        intent.target = candidate.clone();
        let mut expanded_members = intent.current_configuration.members.clone();
        expanded_members
            .iter_mut()
            .find(|member| member.identity.replica_id == candidate.replica_id)
            .unwrap()
            .identity = candidate.clone();
        intent.current_configuration = ConfigurationDescriptor::new(
            intent.current_configuration.epoch,
            intent.current_configuration.primary_id,
            expanded_members,
            intent.current_policy.write_quorum,
        );
        intent.build_id = provisioning
            .scale_up_build_id(&intent.resource_uid)
            .unwrap();
        intent.operation_id = intent.expected_operation_id();
        let cleanup = ScaleUpCleanup {
            provisioning: provisioning.clone(),
            target: candidate.clone(),
            resources: ReplicaCleanupIdentity {
                pod: CleanupResourceIdentity::Present {
                    name: "db-1".into(),
                    uid: candidate.instance_id.to_string(),
                },
                pvc: CleanupResourceIdentity::Present {
                    name: "db-1-data".into(),
                    uid: "pvc-2".into(),
                },
                endpoint: CleanupResourceIdentity::Present {
                    name: derive_replica_endpoint_name(&intent.resource_uid, &candidate),
                    uid: "service-2".into(),
                },
            },
        };
        let mut members = intent.current_configuration.members.clone();
        for member in &mut members {
            member.role = if member.identity == candidate {
                ReplicaRole::Primary
            } else {
                ReplicaRole::ActiveSecondary
            };
        }
        let failover = ConfigurationDescriptor::new(
            Epoch::new(0, 3),
            candidate.replica_id,
            members,
            intent.current_policy.write_quorum,
        );
        let evidence = ScaleUpFailoverEvidence {
            provisional_configuration: failover.clone(),
            previous_read_quorum: vec![witness(
                &intent,
                intent.previous_configuration.members[0].identity.clone(),
                true,
                1,
            )],
            current_read_quorum: vec![witness(&intent, candidate.clone(), true, 2)],
            final_election: None,
            intent: intent.clone(),
        };
        let mut status = AcceptedStatus {
            initialized: true,
            effective_policy: Some(intent.previous_policy.clone()),
            topology: Some(AcceptedTopology {
                configuration: intent.previous_configuration.clone(),
            }),
            provisioning: Some(provisioning.clone()),
            transition: Some(TransitionIntent {
                transition_id: intent.transition_id(TransitionKind::Failover, &failover),
                kind: TransitionKind::Failover,
                spec_generation: intent.spec_generation,
                effective_policy: intent.current_policy.clone(),
                previous_configuration_id: Some(
                    intent.previous_configuration.configuration_id.clone(),
                ),
                current_configuration: failover,
                election_lsn: None,
                build_id: Some(intent.build_id.clone()),
                repair: None,
                switchover: None,
                secondary_scale_down: None,
                secondary_removal_evidence: None,
                scale_up: None,
                scale_up_failover: Some(Box::new(evidence)),
            }),
            ..Default::default()
        };
        assert_eq!(crate::validation::validate_status(&status), Ok(()));
        status.scale_up_cleanup = Some(Box::new(cleanup));
        assert!(crate::validation::validate_status(&status).is_err());
    }

    #[test]
    fn typed_report_evidence_allows_only_the_exact_expansion() {
        let intent = intent(2);
        let primary = intent.primary.clone();
        let report = crate::observation::AgentReport {
            protocol_version: crate::PROTOCOL_VERSION,
            resource_uid: intent.resource_uid.clone(),
            identity: primary.clone(),
            process_session_id: ProcessSessionId::new("session"),
            report_sequence: 1,
            role: ReplicaRole::Primary,
            read_status: AccessStatus::Granted,
            write_status: AccessStatus::Granted,
            healthy: true,
            epoch: intent.current_configuration.epoch,
            previous_configuration: Some(intent.previous_configuration.clone()),
            current_configuration: Some(intent.current_configuration.clone()),
            current_progress: 0,
            verified_replication_lsn: Some(0),
            committed_lsn: 0,
            retained_operation_id: Some(intent.command_operation_id(
                ScaleUpStage::PreviousCurrent,
                &primary,
                &intent.current_configuration,
            )),
            scale_up_intent: Some(Box::new(intent.clone())),
            ..Default::default()
        };
        assert_eq!(crate::validation::validate_report_internal(&report), Ok(()));

        let mut missing = report.clone();
        missing.scale_up_intent = None;
        assert!(crate::validation::validate_report_internal(&missing).is_err());

        let mut other = report;
        other.resource_uid = ResourceUid::new("other");
        assert!(crate::validation::validate_report_internal(&other).is_err());
    }
}
