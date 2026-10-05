//! Exact local admission for durable secondary-removal effects.

use crate::authority::AdmittedAuthority;
use crate::protocol::command::{
    AcceptSecondaryRemovalCommit, EnsureConfiguration, PrepareSecondaryRemoval, RetireReplica,
};
use crate::protocol::types::{AccessStatus, ReplicaRole, SecondaryRemovalStage};
use crate::protocol::validation::{
    validate_secondary_removal_configuration, validate_secondary_scale_down,
    validate_secondary_scale_down_cleanup,
};

use crate::host::HostError;
use crate::host::Result;
use crate::host::state::AgentState;

fn reject(message: &str) -> HostError {
    HostError::CommandRejected(message.into())
}

pub(crate) fn admit_commit(
    command: &AcceptSecondaryRemovalCommit,
    state: &AgentState,
) -> Result<()> {
    crate::protocol::validation::validate_accept_secondary_removal_commit(command)
        .map_err(|e| reject(&e.to_string()))?;
    let intent = &command.committed.evidence.preparation.intent;
    if state.identity.local_identity != command.target
        || state.identity.resource_uid != intent.resource_uid
        || state.retired_authority.is_some()
        || state.reconfiguration.is_some()
        || state.previous_configuration.is_some()
        || state.current_configuration.as_ref() != Some(&intent.current_configuration)
        || state.secondary_removal_evidence.as_ref() != Some(&command.committed.evidence)
        || state.highest_epoch != intent.current_configuration.epoch
        || state
            .accepted_secondary_removal
            .as_ref()
            .is_some_and(|c| c != &command.committed)
        || (command.local_recovery
            && (state.role != ReplicaRole::ActiveSecondary
                || state.write_status == AccessStatus::Granted
                || state.prepared_secondary_removal.is_some()
                || state.prepared_switchover.is_some()))
    {
        return Err(reject(
            "commit certificate differs from completed local reduced authority",
        ));
    }
    Ok(())
}

pub(crate) fn admit_preparation(
    command: &PrepareSecondaryRemoval,
    state: &AgentState,
) -> Result<()> {
    validate_secondary_scale_down(&command.intent).map_err(|e| reject(&e.to_string()))?;
    let intent = &command.intent;
    let local = &state.identity.local_identity;
    if state.retired_authority.is_some()
        || intent.resource_uid != state.identity.resource_uid
        || intent.primary != *local
        || command.local_replica_id != local.replica_id
        || command.expected_instance_id != local.instance_id
        || command.expected_agent_generation != local.agent_generation
        || command.operation_id
            != intent.command_operation_id(SecondaryRemovalStage::Prepare, local)
        || state.highest_epoch > intent.current_configuration.epoch
        || state.prepared_switchover.is_some()
    {
        return Err(reject(
            "removal preparation differs from exact primary authority",
        ));
    }
    if let Some(retained) = state.removal_effects.get(&command.operation_id) {
        return match &retained.effect.action {
            crate::effects::RuntimeEffectAction::PrepareSecondaryRemoval {
                intent: existing,
                ..
            } if existing.as_ref() == intent => Ok(()),
            _ => Err(reject("removal preparation operation was mutated")),
        };
    }
    if state.reconfiguration.is_some()
        || state.previous_configuration.is_some()
        || state.current_configuration.as_ref() != Some(&intent.previous_configuration)
        || state
            .admitted_policy
            .as_ref()
            .unwrap_or(&state.identity.effective_policy)
            != &intent.previous_policy
        || state.role != ReplicaRole::Primary
        || state.secondary_removal_evidence.as_ref().is_some_and(|e| {
            state
                .accepted_secondary_removal
                .as_ref()
                .is_none_or(|c| &c.evidence != e)
        })
        || state.prepared_secondary_removal.as_ref().is_some_and(|p| {
            p.intent != *intent
                && intent.previous_configuration.epoch < p.intent.current_configuration.epoch
        })
    {
        return Err(reject(
            "removal preparation requires installed current-only primary authority",
        ));
    }
    Ok(())
}

pub(crate) fn admit_retirement(command: &RetireReplica, state: &AgentState) -> Result<()> {
    validate_secondary_scale_down_cleanup(&command.committed)
        .map_err(|e| reject(&e.to_string()))?;
    let intent = &command.committed.evidence.preparation.intent;
    let local = &state.identity.local_identity;
    if intent.resource_uid != state.identity.resource_uid
        || intent.target != *local
        || command.local_replica_id != local.replica_id
        || command.expected_instance_id != local.instance_id
        || command.expected_agent_generation != local.agent_generation
        || command.operation_id != intent.command_operation_id(SecondaryRemovalStage::Retire, local)
        || state.highest_epoch > intent.current_configuration.epoch
        || state.reconfiguration.is_some()
        || state.prepared_switchover.is_some()
    {
        return Err(reject("retirement differs from exact excluded incarnation"));
    }
    if let Some(retired) = &state.retired_authority {
        if retired.committed == command.committed
            && retired.report.operation_id == command.operation_id
        {
            return Ok(());
        }
        return Err(reject("retirement operation was mutated"));
    }
    if state.current_configuration.as_ref() != Some(&intent.previous_configuration)
        || state.previous_configuration.is_some()
        || state
            .admitted_policy
            .as_ref()
            .unwrap_or(&state.identity.effective_policy)
            != &intent.previous_policy
        || state.role != ReplicaRole::ActiveSecondary
    {
        return Err(reject(
            "retirement requires the exact previous secondary authority",
        ));
    }
    Ok(())
}

pub(crate) fn admit_configuration(
    command: &EnsureConfiguration,
    state: &AgentState,
    persisted: bool,
) -> Result<AdmittedAuthority> {
    validate_secondary_removal_configuration(command).map_err(|e| reject(&e.to_string()))?;
    let evidence = command
        .secondary_removal_evidence
        .as_ref()
        .expect("validated");
    let intent = &evidence.preparation.intent;
    if state.retired_authority.is_some()
        || intent.resource_uid != state.identity.resource_uid
        || state.prepared_switchover.is_some()
        || command.local_replica_id != state.identity.local_identity.replica_id
        || command.expected_instance_id != state.identity.local_identity.instance_id
        || command.expected_agent_generation != state.identity.local_identity.agent_generation
        || command.current_epoch < state.highest_epoch
        || state
            .reconfiguration
            .as_ref()
            .is_some_and(|r| r.command != *command)
        || state.pending_effect.as_ref().is_some_and(|p| {
            matches!(
                p.effect.action,
                crate::effects::RuntimeEffectAction::PrepareSecondaryRemoval { .. }
                    | crate::effects::RuntimeEffectAction::RetireReplica(_)
            )
        })
    {
        return Err(reject("reduction differs from durable local authority"));
    }
    let already_admitted =
        state.current_configuration.as_ref() == Some(&intent.current_configuration);
    if already_admitted {
        let old = state
            .secondary_removal_evidence
            .as_ref()
            .ok_or_else(|| reject("missing frozen admission evidence"))?;
        if old.preparation != evidence.preparation
            || old.previous_read_quorum != evidence.previous_read_quorum
            || (!old.reduced_write_quorum.is_empty()
                && old.reduced_write_quorum != evidence.reduced_write_quorum)
            || state.admitted_policy.as_ref() != Some(&intent.current_policy)
            || (!command.current_only && state.previous_configuration.is_none())
            || (command.current_only && state.previous_configuration.is_none() && !persisted)
        {
            return Err(reject("reduction replay changed frozen admission evidence"));
        }
    } else if command.current_only
        || state.previous_configuration.is_some()
        || state.current_configuration.as_ref() != Some(&intent.previous_configuration)
        || state
            .admitted_policy
            .as_ref()
            .unwrap_or(&state.identity.effective_policy)
            != &intent.previous_policy
    {
        return Err(reject(
            "reduction does not follow installed previous authority",
        ));
    }
    if state.identity.local_identity == intent.primary
        && state.prepared_secondary_removal.as_ref() != Some(&evidence.preparation)
    {
        return Err(reject(
            "reduction primary lacks the exact durable preparation",
        ));
    }
    if command.primary_write_status == AccessStatus::Granted {
        return Err(reject("reduction coordination cannot grant writes"));
    }
    let authority = AdmittedAuthority {
        local_identity: state.identity.local_identity.clone(),
        transition_kind: (!command.current_only).then_some(command.transition_kind),
        previous_configuration: command.previous_configuration.clone(),
        current_configuration: command.current_configuration.clone(),
        switchover_handoff: None,
        secondary_removal: Some(evidence.clone()),
        scale_up: None,
    };
    authority.validate().map_err(|e| reject(&e.to_string()))?;
    Ok(authority)
}
