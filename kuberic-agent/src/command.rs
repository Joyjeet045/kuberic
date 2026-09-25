use std::collections::BTreeSet;

use kuberic_protocol::command::{
    EnsureConfiguration, EnsureReplicaBuild, InitializeAgentStore, PrepareSwitchover,
};
use kuberic_protocol::types::{AccessStatus, ReplicaRole, TransitionKind};
use kuberic_protocol::validation::validate_transition_relationship;
use kuberic_runtime_internal::authority::AdmittedAuthority;

use crate::provisioning::{
    InitializationAuthority, ObservedStorageIdentity, authorize_initialization,
};
use crate::state::AgentState;
use crate::state::StorageIdentity;
use crate::{AgentError, Result};

pub fn admit_initialization(
    command: &InitializeAgentStore,
    observed: &ObservedStorageIdentity,
    authority: InitializationAuthority<'_>,
) -> Result<StorageIdentity> {
    authorize_initialization(command, observed, authority)
}

pub fn admit_configuration(
    command: &EnsureConfiguration,
    state: &AgentState,
) -> Result<AdmittedAuthority> {
    admit_configuration_with_replay(command, state, false)
}

pub fn admit_persisted_configuration(
    command: &EnsureConfiguration,
    state: &AgentState,
) -> Result<AdmittedAuthority> {
    admit_configuration_with_replay(command, state, true)
}

pub fn admit_switchover_preparation(command: &PrepareSwitchover, state: &AgentState) -> Result<()> {
    let identity = &state.identity.local_identity;
    if command.preparation_generation == 0
        || command.operation_id.is_empty()
        || command.request_id.is_empty()
        || command.operation_id
            != kuberic_protocol::types::derive_switchover_preparation_operation_id(
                &state.identity.resource_uid,
                &command.request_id,
                command.preparation_generation,
                &command.current_configuration.configuration_id,
                &command.source,
                &command.target,
            )
        || command.local_replica_id != identity.replica_id
        || command.expected_instance_id != identity.instance_id
        || command.expected_agent_generation != identity.agent_generation
        || command.source != *identity
    {
        return Err(AgentError::CommandRejected(
            "planned switchover preparation target does not match durable identity".into(),
        ));
    }
    if state
        .preparation_retirement
        .as_ref()
        .is_some_and(|retired| {
            retired.starting_epoch == command.current_configuration.epoch
                && retired.starting_configuration_id
                    == command.current_configuration.configuration_id
                && command.preparation_generation <= retired.generation
        })
    {
        return Err(AgentError::CommandRejected(
            "planned switchover preparation has already been retired".into(),
        ));
    }
    if let Some(prepared) = state.prepared_switchover.as_ref() {
        if prepared.preparation_operation_id == command.operation_id
            && prepared.preparation_generation == command.preparation_generation
            && prepared.request_id == command.request_id
            && prepared.source == command.source
            && prepared.target == command.target
            && prepared.starting_configuration_id == command.current_configuration.configuration_id
            && state.current_configuration.as_ref() == Some(&command.current_configuration)
        {
            return Ok(());
        }
        return Err(AgentError::CommandRejected(
            "another planned switchover preparation is retained".into(),
        ));
    }
    if state.reconfiguration.is_some() {
        return Err(AgentError::CommandRejected(
            "configuration work is already pending".into(),
        ));
    }
    if state.role != ReplicaRole::Primary || state.write_status != AccessStatus::Granted {
        return Err(AgentError::CommandRejected(
            "planned switchover preparation requires the writable primary".into(),
        ));
    }
    let current = state.current_configuration.as_ref().ok_or_else(|| {
        AgentError::CommandRejected(
            "planned switchover preparation requires installed authority".into(),
        )
    })?;
    if current != &command.current_configuration {
        return Err(AgentError::CommandRejected(
            "planned switchover preparation differs from installed authority".into(),
        ));
    }
    let primary = current
        .members
        .iter()
        .find(|member| member.identity.replica_id == current.primary_id)
        .expect("validated durable configuration has one primary");
    if primary.identity != command.source
        || command.target.replica_id == current.primary_id
        || !current
            .members
            .iter()
            .any(|member| member.identity == command.target && member.role != ReplicaRole::Primary)
    {
        return Err(AgentError::CommandRejected(
            "planned switchover preparation source or target differs from authority".into(),
        ));
    }
    Ok(())
}

pub(crate) fn is_access_only_configuration(
    command: &EnsureConfiguration,
    state: &AgentState,
) -> bool {
    !command.current_only
        && command.previous_configuration.is_none()
        && state.previous_configuration.is_none()
        && state.current_configuration.as_ref() == Some(&command.current_configuration)
        && command.current_epoch == state.highest_epoch
        && command.failover_safe_lsn.is_none()
        && command.retire_build_ids.is_empty()
        && ((command.switchover_handoff.is_none()
            && command.retire_switchover_preparation_ids.is_empty())
            || command.is_switchover_restoration())
        && command
            .current_configuration
            .members
            .iter()
            .find(|member| member.identity == state.identity.local_identity)
            .is_some_and(|member| member.role == state.role)
}

fn admit_configuration_with_replay(
    command: &EnsureConfiguration,
    state: &AgentState,
    persisted_exact_replay: bool,
) -> Result<AdmittedAuthority> {
    if command.operation_id.is_empty() {
        return Err(AgentError::CommandRejected(
            "operation ID must not be empty".into(),
        ));
    }
    let identity = &state.identity.local_identity;
    if command.local_replica_id != identity.replica_id
        || command.expected_instance_id != identity.instance_id
        || command.expected_agent_generation != identity.agent_generation
    {
        return Err(AgentError::CommandRejected(
            "command target does not match durable replica identity".into(),
        ));
    }
    if command.current_epoch != command.current_configuration.epoch {
        return Err(AgentError::CommandRejected(
            "current epoch differs from Current Configuration".into(),
        ));
    }
    if command
        .previous_configuration
        .as_ref()
        .map(|configuration| configuration.epoch)
        != command.previous_epoch
    {
        return Err(AgentError::CommandRejected(
            "previous epoch differs from Previous Configuration".into(),
        ));
    }
    if command.current_epoch < state.highest_epoch {
        return Err(AgentError::CommandRejected(
            "command epoch regresses durable authority".into(),
        ));
    }
    let current_only_completion = command.current_epoch == state.highest_epoch
        && state.current_configuration.as_ref() == Some(&command.current_configuration)
        && state.previous_configuration.is_some()
        && command.previous_configuration.is_none();
    let completed_current_only_replay = persisted_exact_replay
        && command.current_only
        && command.current_epoch == state.highest_epoch
        && state.current_configuration.as_ref() == Some(&command.current_configuration)
        && state.previous_configuration.is_none()
        && command.previous_configuration.is_none();
    if command.current_epoch == state.highest_epoch
        && !current_only_completion
        && !completed_current_only_replay
        && state
            .current_configuration
            .as_ref()
            .is_some_and(|current| current != &command.current_configuration)
    {
        return Err(AgentError::CommandRejected(
            "same-epoch command conflicts with durable Current Configuration".into(),
        ));
    }
    if command.current_epoch == state.highest_epoch
        && !current_only_completion
        && !completed_current_only_replay
        && state.previous_configuration != command.previous_configuration
    {
        return Err(AgentError::CommandRejected(
            "same-epoch command conflicts with durable Previous Configuration".into(),
        ));
    }
    if command.effective_policy != state.identity.effective_policy {
        return Err(AgentError::CommandRejected(
            "command policy differs from initialized policy".into(),
        ));
    }
    if command.is_switchover_restoration() {
        let retained = state.prepared_switchover.as_ref().or_else(|| {
            persisted_exact_replay
                .then_some(state.retired_switchover.as_ref())
                .flatten()
        });
        if !is_access_only_configuration(command, state)
            || (!persisted_exact_replay
                && state
                    .preparation_retirement
                    .as_ref()
                    .is_some_and(|retired| {
                        retired.starting_epoch == command.current_epoch
                            && retired.starting_configuration_id
                                == command.current_configuration.configuration_id
                            && command.retire_switchover_preparation_ids[0].generation
                                <= retired.generation
                    }))
            || command
                .switchover_handoff
                .as_ref()
                .is_some_and(|certificate| Some(certificate) != retained)
            || state.prepared_switchover.as_ref().is_some_and(|prepared| {
                command.retire_switchover_preparation_ids[0] != prepared.preparation()
            })
        {
            return Err(AgentError::CommandRejected(
                "restoration requires exact starting authority and the whole retained certificate"
                    .into(),
            ));
        }
        return Ok(AdmittedAuthority {
            local_identity: identity.clone(),
            transition_kind: None,
            previous_configuration: None,
            current_configuration: command.current_configuration.clone(),
            switchover_handoff: None,
        });
    }
    match command.transition_kind {
        TransitionKind::Failover if command.failover_safe_lsn.is_none_or(|lsn| lsn < 0) => {
            return Err(AgentError::CommandRejected(
                "failover command requires a non-negative election-safe LSN".into(),
            ));
        }
        TransitionKind::Bootstrap
        | TransitionKind::Replacement
        | TransitionKind::PlannedSwitchover
            if command.failover_safe_lsn.is_some() =>
        {
            return Err(AgentError::CommandRejected(
                "only failover authority can carry an election-safe LSN".into(),
            ));
        }
        _ => {}
    }
    let transition_primary_grant = matches!(
        command.transition_kind,
        TransitionKind::Replacement | TransitionKind::Failover | TransitionKind::PlannedSwitchover
    ) && !command.current_only
        && state.current_configuration.as_ref() == command.previous_configuration.as_ref()
        && command.current_configuration.primary_id == identity.replica_id
        && command
            .current_configuration
            .members
            .iter()
            .any(|member| member.identity == *identity && member.role == ReplicaRole::Primary);
    let installed_primary_grant = state.current_configuration.as_ref()
        == Some(&command.current_configuration)
        && state.role == ReplicaRole::Primary;
    if command.primary_write_status == AccessStatus::Granted && state.prepared_switchover.is_some()
    {
        return Err(AgentError::CommandRejected(
            "retained switchover preparation forbids write grants".into(),
        ));
    }
    if command.primary_write_status == kuberic_protocol::types::AccessStatus::Granted
        && !transition_primary_grant
        && !installed_primary_grant
    {
        return Err(AgentError::CommandRejected(
            "write grant requires the exact installed primary authority".into(),
        ));
    }
    if command.current_only {
        if command.previous_configuration.is_some()
            || (!current_only_completion && !completed_current_only_replay)
            || command.transition_kind == TransitionKind::Bootstrap
        {
            return Err(AgentError::CommandRejected(
                "current-only completion does not match durable PC/CC authority".into(),
            ));
        }
        if command.transition_kind == TransitionKind::Replacement
            && command.retire_build_ids.is_empty()
        {
            return Err(AgentError::CommandRejected(
                "replacement current-only completion must retire its build".into(),
            ));
        }
    } else {
        if !command.retire_build_ids.is_empty() {
            return Err(AgentError::CommandRejected(
                "build retirement is valid only for current-only completion".into(),
            ));
        }
        if !command.retire_switchover_preparation_ids.is_empty() {
            return Err(AgentError::CommandRejected(
                "switchover preparation retirement is valid only for current-only completion"
                    .into(),
            ));
        }
        validate_transition_relationship(
            command.transition_kind,
            command.previous_configuration.as_ref(),
            &command.current_configuration,
            &command.effective_policy,
        )
        .map_err(|error| AgentError::CommandRejected(error.to_string()))?;
    }
    if command.transition_kind == TransitionKind::PlannedSwitchover {
        if command.primary_write_status == AccessStatus::Granted {
            return Err(AgentError::CommandRejected(
                "planned switchover must remain write-closed until stable access convergence"
                    .into(),
            ));
        }
        let handoff = command.switchover_handoff.as_ref().ok_or_else(|| {
            AgentError::CommandRejected(
                "planned switchover authority requires a handoff certificate".into(),
            )
        })?;
        let starting_configuration = command
            .previous_configuration
            .as_ref()
            .or(state.previous_configuration.as_ref());
        let exact_persisted_command = state
            .reconfiguration
            .as_ref()
            .is_some_and(|record| record.command == *command)
            || state
                .retained_command
                .as_ref()
                .is_some_and(|retained| retained.command == *command);
        let starting_authority_was_durably_admitted =
            completed_current_only_replay && exact_persisted_command;
        let source_certificate_matches = *identity != handoff.source
            || state.prepared_switchover.as_ref() == Some(handoff)
            || (state.retired_switchover.as_ref() == Some(handoff)
                && command.current_configuration.primary_id == handoff.source.replica_id
                && command.current_epoch > handoff.starting_epoch)
            || (completed_current_only_replay
                && exact_persisted_command
                && state.prepared_switchover.is_none());
        let starting_authority_matches = starting_configuration.is_some_and(|configuration| {
            let source_is_primary = configuration.members.iter().any(|member| {
                member.identity == handoff.source && member.role == ReplicaRole::Primary
            });
            let target_is_primary = configuration.members.iter().any(|member| {
                member.identity == handoff.target && member.role == ReplicaRole::Primary
            });
            let exact_members_present = configuration
                .members
                .iter()
                .any(|member| member.identity == handoff.source)
                && configuration
                    .members
                    .iter()
                    .any(|member| member.identity == handoff.target);
            exact_members_present
                && if source_is_primary {
                    configuration.configuration_id == handoff.starting_configuration_id
                        && configuration.epoch == handoff.starting_epoch
                } else {
                    target_is_primary
                        && command.current_configuration.primary_id == handoff.source.replica_id
                        && configuration.epoch.data_loss_number
                            == handoff.starting_epoch.data_loss_number
                        && configuration.epoch.configuration_number
                            > handoff.starting_epoch.configuration_number
                }
        });
        let retirement_ids = command
            .retire_switchover_preparation_ids
            .iter()
            .collect::<BTreeSet<_>>();
        if !source_certificate_matches
            || (!starting_authority_was_durably_admitted && !starting_authority_matches)
            || !command
                .current_configuration
                .members
                .iter()
                .any(|member| member.identity == handoff.source)
            || !command
                .current_configuration
                .members
                .iter()
                .any(|member| member.identity == handoff.target)
            || (command.current_configuration.primary_id != handoff.source.replica_id
                && command.current_configuration.primary_id != handoff.target.replica_id)
            || retirement_ids.len() != command.retire_switchover_preparation_ids.len()
            || command
                .retire_switchover_preparation_ids
                .iter()
                .any(|id| id.operation_id.is_empty() || id.generation == 0)
            || (command.current_only
                && *identity == handoff.source
                && (command.retire_switchover_preparation_ids.len() != 1
                    || command.retire_switchover_preparation_ids[0] != handoff.preparation()))
            || (command.current_only
                && *identity != handoff.source
                && !command.retire_switchover_preparation_ids.is_empty())
        {
            return Err(AgentError::CommandRejected(
                "planned switchover handoff differs from configuration authority".into(),
            ));
        }
    } else if command.switchover_handoff.is_some()
        || !command.retire_switchover_preparation_ids.is_empty()
    {
        return Err(AgentError::CommandRejected(
            "non-switchover command contains switchover evidence".into(),
        ));
    }
    let admitted = AdmittedAuthority {
        local_identity: identity.clone(),
        transition_kind: (!command.current_only && !is_access_only_configuration(command, state))
            .then_some(command.transition_kind),
        previous_configuration: command.previous_configuration.clone(),
        current_configuration: command.current_configuration.clone(),
        switchover_handoff: command.switchover_handoff.clone(),
    };
    admitted
        .validate()
        .map_err(|error| AgentError::CommandRejected(error.to_string()))?;
    if command.transition_kind == TransitionKind::Bootstrap
        && admitted.local_role() == ReplicaRole::None
    {
        return Err(AgentError::CommandRejected(
            "bootstrap authority must assign a runtime role".into(),
        ));
    }
    Ok(admitted)
}

pub fn admit_build(command: &EnsureReplicaBuild, state: &AgentState) -> Result<()> {
    let identity = &state.identity.local_identity;
    if command.operation_id.is_empty()
        || command.local_replica_id != identity.replica_id
        || command.expected_instance_id != identity.instance_id
        || command.expected_agent_generation != identity.agent_generation
    {
        return Err(AgentError::CommandRejected(
            "build command target does not match durable replica identity".into(),
        ));
    }
    if let Some(authority) = &command.authority {
        authority
            .validate()
            .map_err(|error| AgentError::CommandRejected(error.to_string()))?;
        if authority.build_id != command.operation_id
            || authority.target != command.target
            || authority.target != *identity
            || command.source_session_id.is_none()
        {
            return Err(AgentError::CommandRejected(
                "target build command differs from admitted build authority".into(),
            ));
        }
    } else {
        if command.source_session_id.is_some() {
            return Err(AgentError::CommandRejected(
                "source build command cannot carry a peer session".into(),
            ));
        }
        let current = state.current_configuration.as_ref().ok_or_else(|| {
            AgentError::CommandRejected("build source has no Current Configuration".into())
        })?;
        let primary = current
            .members
            .iter()
            .find(|member| member.identity.replica_id == current.primary_id)
            .expect("validated configuration has primary");
        if primary.identity != *identity || command.target.replica_id == identity.replica_id {
            return Err(AgentError::CommandRejected(
                "source build command must target another logical replica from the primary".into(),
            ));
        }
    }
    Ok(())
}
