use kuberic_protocol::command::{EnsureConfiguration, EnsureReplicaBuild, InitializeAgentStore};
use kuberic_protocol::types::{ReplicaRole, TransitionKind};
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
    match command.transition_kind {
        TransitionKind::Failover if command.failover_safe_lsn.is_none_or(|lsn| lsn < 0) => {
            return Err(AgentError::CommandRejected(
                "failover command requires a non-negative election-safe LSN".into(),
            ));
        }
        TransitionKind::Bootstrap | TransitionKind::Replacement
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
        TransitionKind::Replacement | TransitionKind::Failover
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
        validate_transition_relationship(
            command.transition_kind,
            command.previous_configuration.as_ref(),
            &command.current_configuration,
            &command.effective_policy,
        )
        .map_err(|error| AgentError::CommandRejected(error.to_string()))?;
    }
    let admitted = AdmittedAuthority {
        local_identity: identity.clone(),
        transition_kind: (!command.current_only && !is_access_only_configuration(command, state))
            .then_some(command.transition_kind),
        previous_configuration: command.previous_configuration.clone(),
        current_configuration: command.current_configuration.clone(),
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
