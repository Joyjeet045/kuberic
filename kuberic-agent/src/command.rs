use std::collections::BTreeSet;

use kuberic_protocol::command::{
    EnsureConfiguration, EnsureReplicaBuild, InitializeAgentStore, PrepareSwitchover,
};
use kuberic_protocol::types::{AccessStatus, ReplicaRole, TransitionKind};
use kuberic_protocol::validation::{
    validate_scale_up_configuration, validate_transition_relationship,
};
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
    if state.retired_authority.is_some() || state.removal_pending() {
        return Err(AgentError::CommandRejected(
            "secondary removal fences switchover preparation".into(),
        ));
    }
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
    if state.retired_authority.is_some() {
        return Err(AgentError::CommandRejected(
            "retired incarnation cannot admit authority".into(),
        ));
    }
    if command.transition_kind == TransitionKind::SecondaryScaleDown {
        return crate::removal::admit_configuration(command, state, persisted_exact_replay);
    }
    if state.removal_pending() {
        return Err(AgentError::CommandRejected(
            "prepared removal may only roll forward".into(),
        ));
    }
    if command.scale_up_evidence.is_some() || command.transition_kind == TransitionKind::ScaleUp {
        return admit_scale_up_configuration(command, state, persisted_exact_replay);
    }
    if command.secondary_removal_evidence.is_some()
        || command
            .previous_policy
            .as_ref()
            .is_some_and(|policy| policy != &command.effective_policy)
    {
        return Err(AgentError::CommandRejected(
            "secondary scale-down execution is not enabled".into(),
        ));
    }
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
    if &command.effective_policy
        != state
            .admitted_policy
            .as_ref()
            .unwrap_or(&state.identity.effective_policy)
    {
        return Err(AgentError::CommandRejected(
            "command policy differs from admitted policy".into(),
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
            secondary_removal: None,
            scale_up: None,
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
        secondary_removal: is_access_only_configuration(command, state)
            .then(|| state.secondary_removal_evidence.clone())
            .flatten(),
        scale_up: is_access_only_configuration(command, state)
            .then(|| state.scale_up_evidence.clone())
            .flatten(),
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

fn admit_scale_up_configuration(
    command: &EnsureConfiguration,
    state: &AgentState,
    persisted_exact_replay: bool,
) -> Result<AdmittedAuthority> {
    validate_scale_up_configuration(command)
        .map_err(|error| AgentError::CommandRejected(error.to_string()))?;
    let evidence = command
        .scale_up_evidence
        .as_ref()
        .expect("validated scale-up command has evidence");
    let intent = evidence.intent();
    let identity = &state.identity.local_identity;
    if intent.resource_uid != state.identity.resource_uid
        || command.local_replica_id != identity.replica_id
        || command.expected_instance_id != identity.instance_id
        || command.expected_agent_generation != identity.agent_generation
        || command.current_epoch < state.highest_epoch
    {
        return Err(AgentError::CommandRejected(
            "scale-up command target or epoch differs from durable identity".into(),
        ));
    }
    let sequential_supersession = state.scale_up_evidence.as_deref().is_some_and(|existing| {
        existing.intent() != intent
            && state.previous_configuration.is_none()
            && state.current_configuration.as_ref() == Some(&intent.previous_configuration)
            && state.admitted_policy.as_ref() == Some(&intent.previous_policy)
            && state.retired_builds.contains(&existing.intent().build_id)
            && state.completed_scale_up.as_ref().is_some_and(|retained| {
                retained.command.current_only
                    && retained.command.scale_up_evidence.as_deref() == Some(existing)
                    && retained.command.current_configuration == intent.previous_configuration
            })
    });
    if let Some(existing) = state.scale_up_evidence.as_deref()
        && existing.intent() != intent
        && !sequential_supersession
    {
        return Err(AgentError::CommandRejected(
            "scale-up command conflicts with incomplete durable attempt authority".into(),
        ));
    }
    let mut historical_failover_current_only = false;
    let mut original_failover_basis = false;
    let mut exact_failover_progression = false;
    if let kuberic_protocol::types::ScaleUpConfigurationEvidence::Failover { evidence } =
        evidence.as_ref()
    {
        let new_primary = command
            .current_configuration
            .members
            .iter()
            .find(|member| {
                member.identity.replica_id == command.current_configuration.primary_id
                    && member.role == ReplicaRole::Primary
            })
            .expect("validated failover configuration has one primary");
        let has_new_primary_witness = if command.failover_safe_lsn.is_none() {
            evidence
                .current_read_quorum
                .iter()
                .any(|witness| witness.identity == new_primary.identity)
        } else {
            evidence
                .final_election
                .as_deref()
                .is_some_and(|final_election| {
                    final_election
                        .final_configuration(&evidence.provisional_configuration)
                        .as_ref()
                        == Some(&command.current_configuration)
                        && command.failover_safe_lsn == final_election.safe_lsn()
                        && final_election
                            .witness(new_primary.identity.replica_id)
                            .is_some_and(|witness| {
                                final_election
                                    .current_read_quorum
                                    .contains(&new_primary.identity.replica_id)
                                    && Some(witness.current_progress.min(witness.deactivated_lsn))
                                        == final_election.safe_lsn()
                            })
                })
        };
        let original_attempt_installed = matches!(
            state.scale_up_evidence.as_deref(),
            Some(
                kuberic_protocol::types::ScaleUpConfigurationEvidence::Admission {
                    intent: durable_intent
                }
            ) if durable_intent == intent
        ) && state.current_configuration.as_ref()
            == Some(&intent.current_configuration)
            && (state.previous_configuration.as_ref() == Some(&intent.previous_configuration)
                || state.previous_configuration.is_none());
        let original_authority_failover = !command.current_only
            && state.previous_configuration.is_none()
            && state.current_configuration.as_ref() == Some(&intent.previous_configuration)
            && state.highest_epoch == intent.previous_configuration.epoch
            && state.admitted_policy.as_ref() == Some(&intent.previous_policy)
            && intent
                .previous_configuration
                .members
                .iter()
                .any(|member| member.identity == *identity && member.role == state.role);
        original_failover_basis = original_attempt_installed || original_authority_failover;
        let first_failover_admission = !command.current_only && original_attempt_installed;
        historical_failover_current_only = command.current_only
            && original_attempt_installed
            && state.current_configuration.as_ref() != Some(&command.current_configuration);
        let exact_failover_pc_cc =
            !command.current_only && state.previous_configuration == command.previous_configuration;
        let exact_failover_current_only = command.current_only
            && (state.previous_configuration.as_ref() == Some(&intent.previous_configuration)
                || (persisted_exact_replay && state.previous_configuration.is_none()));
        let exact_installed_failover = state.scale_up_evidence.as_deref()
            == Some(
                command
                    .scale_up_evidence
                    .as_deref()
                    .expect("validated evidence"),
            )
            && state.current_configuration.as_ref() == Some(&command.current_configuration)
            && (exact_failover_pc_cc || exact_failover_current_only);
        let fenced_failover_progression = !command.current_only
            && state.scale_up_evidence.as_deref() == command.scale_up_evidence.as_deref()
            && state.highest_epoch < command.current_epoch
            && state
                .current_configuration
                .as_ref()
                .is_some_and(|configuration| {
                    kuberic_protocol::validation::validate_scale_up_failover_transition(
                        evidence,
                        configuration,
                        &intent.current_policy,
                    )
                    .is_ok()
                });
        let exact_provisional_return = !command.current_only
            && command.failover_safe_lsn.is_some()
            && state.previous_configuration.as_ref() == Some(&intent.previous_configuration)
            && state.current_configuration.as_ref() == Some(&evidence.provisional_configuration)
            && state.highest_epoch == evidence.provisional_configuration.epoch
            && matches!(
                state.scale_up_evidence.as_deref(),
                Some(
                    kuberic_protocol::types::ScaleUpConfigurationEvidence::Failover {
                        evidence: installed
                    }
                ) if installed.final_election.is_none()
                    && installed.same_provisional_authority(evidence)
            );
        exact_failover_progression =
            exact_installed_failover || fenced_failover_progression || exact_provisional_return;
        if !has_new_primary_witness
            || (!first_failover_admission
                && !original_authority_failover
                && !historical_failover_current_only
                && !fenced_failover_progression
                && !exact_provisional_return
                && !(exact_installed_failover && (command.current_only || persisted_exact_replay)))
        {
            return Err(AgentError::CommandRejected(
                "carried scale-up failover lacks durable PC/CC authority or a new-primary witness"
                    .into(),
            ));
        }
    }
    if *identity == intent.target {
        let provisioning = state.scale_up_initialization.as_ref().ok_or_else(|| {
            AgentError::CommandRejected(
                "candidate lacks durable scale-up initialization authority".into(),
            )
        })?;
        let initialized = provisioning.scale_up().ok_or_else(|| {
            AgentError::CommandRejected(
                "candidate initialization is not tagged for scale-up".into(),
            )
        })?;
        if initialized.resource_uid != intent.resource_uid
            || initialized.spec_generation != intent.spec_generation
            || initialized.desired_replicas != intent.desired_replicas
            || initialized.previous_configuration != intent.previous_configuration
            || initialized.previous_policy != intent.previous_policy
            || initialized.current_policy != intent.current_policy
            || provisioning.target_identity(&intent.resource_uid) != intent.target
            || provisioning
                .scale_up_build_id(&intent.resource_uid)
                .as_ref()
                != Some(&intent.build_id)
        {
            return Err(AgentError::CommandRejected(
                "candidate authority differs from durable scale-up initialization".into(),
            ));
        }
        let build = state.build_commands.get(&intent.build_id).ok_or_else(|| {
            AgentError::CommandRejected(
                "candidate has not durably admitted the exact scale-up build".into(),
            )
        })?;
        let authority = build.authority.as_ref().ok_or_else(|| {
            AgentError::CommandRejected(
                "candidate build command lacks immutable receiver authority".into(),
            )
        })?;
        let progress = state.build_progress.get(&intent.build_id).ok_or_else(|| {
            AgentError::CommandRejected(
                "candidate lacks durable exact scale-up build progress".into(),
            )
        })?;
        if (state.retired_builds.contains(&intent.build_id)
            && !(command.current_only
                && (persisted_exact_replay || historical_failover_current_only))
            && !original_failover_basis
            && !exact_failover_progression)
            || build.target != intent.target
            || authority.build_id != intent.build_id
            || authority.source != intent.primary
            || authority.target != intent.target
            || authority.current_configuration != intent.previous_configuration
            || authority.replication_boundary_lsn != intent.snapshot_boundary_lsn
            || progress.authority != *authority
            || !progress.completed
            || progress.catch_up_boundary_lsn != Some(intent.catch_up_boundary_lsn)
            || progress.durable_lsn < intent.catch_up_boundary_lsn
        {
            return Err(AgentError::CommandRejected(
                "candidate requires exact completed build progress through the frozen boundary"
                    .into(),
            ));
        }
    }

    let completed_current_only_replay = persisted_exact_replay
        && command.current_only
        && state.current_configuration.as_ref() == Some(&command.current_configuration)
        && state.previous_configuration.is_none();
    if command.current_only {
        if !completed_current_only_replay
            && !historical_failover_current_only
            && (state.current_configuration.as_ref() != Some(&command.current_configuration)
                || state.previous_configuration.as_ref() != Some(&intent.previous_configuration))
        {
            return Err(AgentError::CommandRejected(
                "scale-up current-only completion lacks durable PC/CC authority".into(),
            ));
        }
    } else {
        let prior_member = intent
            .previous_configuration
            .members
            .iter()
            .any(|member| member.identity == *identity);
        let same_attempt_replay = state.current_configuration.as_ref()
            == Some(&command.current_configuration)
            && state.previous_configuration.as_ref() == Some(&intent.previous_configuration);
        let admission_start = prior_member
            && state.current_configuration.as_ref() == Some(&intent.previous_configuration)
            && state.previous_configuration.is_none();
        let candidate_start = *identity == intent.target
            && state.current_configuration.is_none()
            && state.previous_configuration.is_none();
        let carried_failover = matches!(
            evidence.as_ref(),
            kuberic_protocol::types::ScaleUpConfigurationEvidence::Failover { .. }
        ) && state.current_configuration.as_ref().is_some_and(
            |configuration| {
                configuration == &intent.current_configuration
                || configuration == &command.current_configuration
                || matches!(
                    (
                        state.scale_up_evidence.as_deref(),
                        evidence.as_ref(),
                    ),
                    (
                        Some(
                            kuberic_protocol::types::ScaleUpConfigurationEvidence::Failover {
                                evidence: installed
                            }
                        ),
                        kuberic_protocol::types::ScaleUpConfigurationEvidence::Failover {
                            evidence: commanded
                        },
                    ) if configuration == &commanded.provisional_configuration
                        && installed.final_election.is_none()
                        && installed.same_provisional_authority(commanded)
                )
                || (state.scale_up_evidence.as_deref() == Some(evidence.as_ref())
                    && state.highest_epoch < command.current_epoch
                    && kuberic_protocol::validation::validate_scale_up_failover_transition(
                        match evidence.as_ref() {
                            kuberic_protocol::types::ScaleUpConfigurationEvidence::Failover {
                                evidence,
                            } => evidence,
                            _ => unreachable!(),
                        },
                        configuration,
                        &intent.current_policy,
                    )
                    .is_ok())
            },
        );
        if !same_attempt_replay && !admission_start && !candidate_start && !carried_failover {
            return Err(AgentError::CommandRejected(
                "scale-up PC/CC command does not extend durable accepted authority".into(),
            ));
        }
    }

    let admitted = AdmittedAuthority {
        secondary_removal: None,
        scale_up: Some(evidence.clone()),
        local_identity: identity.clone(),
        transition_kind: (!command.current_only).then_some(command.transition_kind),
        previous_configuration: command.previous_configuration.clone(),
        current_configuration: command.current_configuration.clone(),
        switchover_handoff: None,
    };
    admitted
        .validate()
        .map_err(|error| AgentError::CommandRejected(error.to_string()))?;
    if admitted.local_role() == ReplicaRole::None {
        return Err(AgentError::CommandRejected(
            "scale-up authority does not assign the local replica".into(),
        ));
    }
    Ok(admitted)
}

pub fn admit_build(command: &EnsureReplicaBuild, state: &AgentState) -> Result<()> {
    if state.retired_authority.is_some() || state.removal_pending() {
        return Err(AgentError::CommandRejected(
            "secondary removal fences replica builds".into(),
        ));
    }
    let identity = &state.identity.local_identity;
    if command.retire {
        if command.authority.is_some() || command.source_session_id.is_some() {
            return Err(AgentError::CommandRejected(
                "build retirement cannot carry delivery authority".into(),
            ));
        }
        let existing = state
            .build_commands
            .get(&command.operation_id)
            .ok_or_else(|| {
                AgentError::CommandRejected(
                    "build retirement requires exact durable build authority".into(),
                )
            })?;
        if existing.retire
            || existing.local_replica_id != command.local_replica_id
            || existing.expected_instance_id != command.expected_instance_id
            || existing.expected_agent_generation != command.expected_agent_generation
            || existing.target != command.target
            || existing.authority.is_some()
        {
            return Err(AgentError::CommandRejected(
                "build retirement differs from exact durable source authority".into(),
            ));
        }
        return Ok(());
    }
    if state.retired_builds.contains(&command.operation_id)
        || state.abandoned_builds.contains(&command.operation_id)
    {
        return Err(AgentError::CommandRejected(
            "abandoned or retired build authority cannot be reopened".into(),
        ));
    }
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
            || command
                .source_session_id
                .as_ref()
                .is_none_or(|session| session.is_empty())
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
