//! SQLite-backed agent persistence.

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::authority::{
    AdmittedAuthority, AuthorityFence, BuildAuthority, BuildAuthorityStore, BuildProgressStore,
    DurableBuildProgress, DurableLocalWrite, LocalWriteJournal, LocalWritePhase,
    ReplicaAuthorityStore, ReplicationProgress, ReplicationProgressStore, RetiredAuthority,
};
use crate::effects::{RuntimeEffect, RuntimeEffectAction, RuntimeEffectResult};
use crate::error::{ContractError, ContractResult};
use crate::protocol::command::{EnsureConfiguration, EnsureReplicaBuild};
use crate::protocol::types::{
    AccessStatus, ConfigurationId, Epoch, FaultType, LoadMetric, OperationId, ReplicaIdentity,
    ReplicaRole, SecondaryRemovalPreparation, SwitchoverHandoff, TransitionKind,
};
use crate::receipts::TopologyReceipt;
use async_trait::async_trait;
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, params};

use crate::host::Result;
use crate::host::command::is_access_only_configuration;
use crate::host::state::{
    AgentState, CoordinatorStage, DeactivationState, EffectStage, PendingEffect,
    ReconfigurationRecord, RetainedCommandResult, RetainedResult, SCHEMA_VERSION, StorageIdentity,
};
use crate::host::store::{AgentStore, BeginConfiguration, BeginEffect};

fn switchover_receipt_matches(
    receipt: &crate::receipts::SwitchoverReceipt,
    preparation_generation: u64,
    request_id: &crate::protocol::types::SwitchoverRequestId,
    source: &ReplicaIdentity,
    target: &ReplicaIdentity,
    starting_configuration_id: &ConfigurationId,
    starting_epoch: Epoch,
) -> bool {
    receipt.preparation_generation == preparation_generation
        && &receipt.request_id == request_id
        && &receipt.source == source
        && &receipt.target == target
        && &receipt.starting_configuration_id == starting_configuration_id
        && receipt.starting_epoch == starting_epoch
        && receipt.handoff_lsn >= 0
}

fn validate_topology_receipt(effect: &RuntimeEffect, result: &RuntimeEffectResult) -> Result<()> {
    let Some(receipt) = result.topology_receipt.as_deref() else {
        return Ok(());
    };
    let valid = match (&effect.action, receipt) {
        (
            RuntimeEffectAction::AuthorizeFailoverPrefix(boundary),
            TopologyReceipt::CertifiedPrefix(receipt),
        ) => receipt.settled_lsn == *boundary,
        (
            RuntimeEffectAction::ChangeApplicationRole(ReplicaRole::Primary),
            TopologyReceipt::CertifiedPrefix(_),
        ) => true,
        (
            RuntimeEffectAction::PrepareSwitchover {
                preparation_generation,
                request_id,
                source,
                target,
                starting_configuration_id,
                starting_epoch,
            },
            TopologyReceipt::Switchover(receipt),
        ) => switchover_receipt_matches(
            receipt,
            *preparation_generation,
            request_id,
            source,
            target,
            starting_configuration_id,
            *starting_epoch,
        ),
        (
            RuntimeEffectAction::PrepareSecondaryRemoval { intent, .. },
            TopologyReceipt::SecondaryRemoval(receipt),
        ) => receipt
            .preparation
            .as_ref()
            .is_some_and(|preparation| preparation.intent == **intent),
        (
            RuntimeEffectAction::ObserveSecondaryRemovalWitness(witness),
            TopologyReceipt::SecondaryRemoval(receipt),
        ) => receipt.witness.as_ref() == Some(witness),
        (
            RuntimeEffectAction::ObserveSecondaryRemovalProgress { witness, committed },
            TopologyReceipt::SecondaryRemoval(receipt),
        ) => {
            receipt.witness.as_ref() == Some(witness)
                && receipt.accepted.as_ref() == Some(committed)
        }
        (
            RuntimeEffectAction::AcceptSecondaryRemovalCommit(committed),
            TopologyReceipt::SecondaryRemoval(receipt),
        ) => receipt.accepted.as_ref() == Some(committed),
        (
            RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(command),
            TopologyReceipt::SecondaryRemoval(receipt),
        ) => receipt.accepted.as_ref() == Some(&command.committed),
        (RuntimeEffectAction::FenceRetirement(retired), TopologyReceipt::Retirement(receipt)) => {
            receipt.retired == **retired && !receipt.completed
        }
        (
            RuntimeEffectAction::RetireReplica(retired)
            | RuntimeEffectAction::CompleteRetirement(retired),
            TopologyReceipt::Retirement(receipt),
        ) => receipt.retired == **retired && receipt.completed,
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(crate::host::HostError::DurableEffectConflict(
            "topology receipt does not match durable intent".into(),
        ))
    }
}

const DATABASE_FILE: &str = "agent.sqlite3";

pub(crate) struct SqliteStore {
    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    path: PathBuf,
    connection: Mutex<Connection>,
    #[cfg(all(test, feature = "testing"))]
    pub(super) authority_admission_failure: std::sync::atomic::AtomicUsize,
}

impl SqliteStore {
    pub(crate) fn metadata_database_path(pvc_root: &Path) -> PathBuf {
        pvc_root.join(".kuberic").join(DATABASE_FILE)
    }

    pub(crate) fn create_authorized(path: impl AsRef<Path>, state: AgentState) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if state.identity.schema_version != SCHEMA_VERSION {
            return Err(crate::host::HostError::SchemaMismatch {
                expected: SCHEMA_VERSION,
                observed: state.identity.schema_version,
            });
        }
        let parent = path.parent().ok_or_else(|| {
            crate::host::HostError::Corrupt("agent database path has no metadata directory".into())
        })?;
        std::fs::create_dir_all(parent)?;
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let result = (|| {
            let mut connection = open_connection(&path)?;
            configure_durability(&connection)?;
            create_schema(&mut connection, &state)?;
            Ok(Self {
                #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
                path: path.clone(),
                connection: Mutex::new(connection),
                #[cfg(all(test, feature = "testing"))]
                authority_admission_failure: std::sync::atomic::AtomicUsize::new(0),
            })
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&path);
        }
        result
    }

    pub(crate) fn open_existing(
        path: impl AsRef<Path>,
        expected_identity: Option<&StorageIdentity>,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if !path.is_file() {
            return Err(crate::host::HostError::MissingEstablishedStore);
        }
        let connection = open_connection(&path).map_err(|error| {
            crate::host::HostError::Corrupt(format!("cannot open SQLite metadata: {error}"))
        })?;
        configure_durability(&connection)
            .map_err(|error| crate::host::HostError::Corrupt(error.to_string()))?;
        validate_integrity(&connection)
            .map_err(|error| crate::host::HostError::Corrupt(error.to_string()))?;
        let version: u32 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if version != SCHEMA_VERSION {
            return Err(crate::host::HostError::SchemaMismatch {
                expected: SCHEMA_VERSION,
                observed: version,
            });
        }
        let state = load_state_from_connection(&connection)?;
        if state.identity.schema_version != version {
            return Err(crate::host::HostError::SchemaMismatch {
                expected: version,
                observed: state.identity.schema_version,
            });
        }
        if let Some(expected) = expected_identity
            && &state.identity != expected
        {
            return Err(crate::host::HostError::IdentityMismatch(
                "resource, Pod, PVC, replica incarnation, generation, or initialization changed"
                    .into(),
            ));
        }
        Ok(Self {
            #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
            path,
            connection: Mutex::new(connection),
            #[cfg(all(test, feature = "testing"))]
            authority_admission_failure: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    fn with_transaction<T>(&self, action: impl FnOnce(&Transaction<'_>) -> Result<T>) -> Result<T> {
        let mut connection = self.connection.lock().map_err(|_| {
            crate::host::HostError::Corrupt("agent database connection mutex was poisoned".into())
        })?;
        let transaction = connection.transaction()?;
        let result = action(&transaction)?;
        transaction.commit()?;
        Ok(result)
    }

    fn contract_transaction<T>(
        &self,
        action: impl FnOnce(&Transaction<'_>) -> ContractResult<T>,
    ) -> ContractResult<T> {
        let mut connection = self.connection.lock().map_err(|_| {
            ContractError::Persistence("agent database connection mutex was poisoned".into())
        })?;
        let transaction = connection.transaction().map_err(contract_sqlite_error)?;
        let result = action(&transaction)?;
        transaction.commit().map_err(contract_sqlite_error)?;
        Ok(result)
    }
}

#[async_trait]
impl AgentStore for SqliteStore {
    async fn identity(&self) -> Result<StorageIdentity> {
        Ok(self.load_state().await?.identity)
    }

    async fn load_state(&self) -> Result<AgentState> {
        let connection = self.connection.lock().map_err(|_| {
            crate::host::HostError::Corrupt("agent database connection mutex was poisoned".into())
        })?;
        load_state_from_connection(&connection)
    }

    async fn complete_application_initialization(&self) -> Result<()> {
        self.with_transaction(|transaction| {
            let mut state = load_state_from_connection(transaction)?;
            if let Some(binding) = &mut state.application_storage
                && binding.initializing
            {
                binding.initializing = false;
                write_agent_state(transaction, &state)?;
            }
            Ok(())
        })
    }

    async fn begin_effect(&self, effect: &RuntimeEffect) -> Result<BeginEffect> {
        self.with_transaction(|transaction| {
            let mut state = load_state_from_connection(transaction)?;
            if let RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(command) =
                &effect.action
            {
                crate::host::removal::admit_commit(command, &state)?;
                if !command.local_recovery || effect.operation_id != command.operation_id {
                    return Err(crate::host::HostError::CommandRejected(
                        "historical acceptance effect identity differs".into(),
                    ));
                }
            }
            if state.retired_authority.is_some()
                && !matches!(effect.action, RuntimeEffectAction::RetireReplica(_))
            {
                return Err(crate::host::HostError::CommandRejected(
                    "retired incarnation is permanently fenced".into(),
                ));
            }
            if let Some(retained) = state.removal_effects.get(&effect.operation_id) {
                if retained.effect != *effect {
                    return Err(crate::host::HostError::DurableEffectConflict(
                        "removal effect operation was mutated".into(),
                    ));
                }
                return Ok(BeginEffect::Completed(Box::new(retained.result.clone())));
            }
            if let Some(retained) = state.retained_result.as_ref()
                && retained.operation_id == effect.operation_id
            {
                if retained.effect != *effect {
                    return Err(crate::host::HostError::DurableEffectConflict(
                        "operation ID was reused with a different effect".into(),
                    ));
                }
                return Ok(BeginEffect::Completed(Box::new(retained.result.clone())));
            }
            if state.next_effect_sequence == u64::MAX || effect.sequence == u64::MAX {
                return Err(crate::host::HostError::DurableEffectConflict(
                    "effect sequence exhausted".into(),
                ));
            }
            if let Some(pending) = state.pending_effect.as_mut() {
                if pending.effect != *effect {
                    match (&pending.effect.action, &effect.action) {
                        (
                            RuntimeEffectAction::AcceptSecondaryRemovalCommit(committed),
                            RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(command),
                        ) if pending.effect.operation_id == effect.operation_id
                            && pending.effect.sequence == effect.sequence
                            && committed.as_ref() == &command.committed =>
                        {
                            validate_acceptance_conversion(transaction, command)?;
                            // Admission above binds the local identity and full certificate.
                            // Change only the execution mode; even EffectApplied is retained.
                            pending.effect = effect.clone();
                            write_agent_state(transaction, &state)?;
                            return Ok(BeginEffect::Pending(effect.clone()));
                        }
                        _ => {
                            return Err(crate::host::HostError::DurableEffectConflict(
                                "another durable effect is pending".into(),
                            ));
                        }
                    }
                }
                return Ok(BeginEffect::Pending(pending.effect.clone()));
            }
            state.pending_effect = Some(PendingEffect {
                effect: effect.clone(),
                stage: EffectStage::IntentCommitted,
            });
            write_agent_state(transaction, &state)?;
            Ok(BeginEffect::Execute(effect.clone()))
        })
    }

    async fn mark_effect_applied(&self, effect: &RuntimeEffect) -> Result<()> {
        self.with_transaction(|transaction| {
            let mut state = load_state_from_connection(transaction)?;
            let pending = state.pending_effect.as_mut().ok_or_else(|| {
                crate::host::HostError::DurableEffectConflict(
                    "runtime effect has no durable intent".into(),
                )
            })?;
            if pending.effect != *effect {
                return Err(crate::host::HostError::DurableEffectConflict(
                    "runtime effect does not match durable intent".into(),
                ));
            }
            pending.stage = EffectStage::EffectApplied;
            write_agent_state(transaction, &state)
        })
    }

    async fn complete_effect(&self, result: &RuntimeEffectResult) -> Result<()> {
        self.with_transaction(|transaction| {
            let mut state = load_state_from_connection(transaction)?;
            let pending = state.pending_effect.take().ok_or_else(|| {
                crate::host::HostError::DurableEffectConflict(
                    "effect completion has no durable intent".into(),
                )
            })?;
            if pending.effect.operation_id != result.operation_id
                || pending.effect.sequence != result.sequence
            {
                return Err(crate::host::HostError::DurableEffectConflict(
                    "effect completion does not match durable intent".into(),
                ));
            }
            validate_topology_receipt(&pending.effect, result)?;
            if let RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(command) =
                &pending.effect.action
            {
                crate::host::removal::admit_commit(command, &state)?;
            }
            state.role = result.postcondition.role;
            state.read_status = result.postcondition.read_status;
            state.write_status = result.postcondition.write_status;
            state.previous_configuration = result
                .postcondition
                .authority
                .as_ref()
                .and_then(|authority| authority.previous_configuration.clone());
            state.current_configuration = result
                .postcondition
                .authority
                .as_ref()
                .map(|authority| authority.current_configuration.clone());
            if let Some(configuration) = state.current_configuration.as_ref() {
                state.highest_epoch = state.highest_epoch.max(configuration.epoch);
            }
            if let Some(authority) = &result.postcondition.authority {
                state.secondary_removal_evidence = authority.secondary_removal.clone();
                state.scale_up_evidence = authority.scale_up.clone();
                if let Some(evidence) = authority.scale_up.as_deref() {
                    let intent = evidence.intent();
                    state.admitted_policy = Some(intent.current_policy.clone());
                    state.previous_policy = authority
                        .previous_configuration
                        .as_ref()
                        .map(|_| intent.previous_policy.clone());
                } else if let Some(evidence) = &authority.secondary_removal {
                    state.admitted_policy =
                        Some(evidence.preparation.intent.current_policy.clone());
                    state.previous_policy = authority
                        .previous_configuration
                        .as_ref()
                        .map(|_| evidence.preparation.intent.previous_policy.clone());
                } else {
                    let policy = state
                        .admitted_policy
                        .clone()
                        .unwrap_or_else(|| state.identity.effective_policy.clone());
                    state.previous_policy = authority
                        .previous_configuration
                        .as_ref()
                        .map(|_| policy.clone());
                    state.admitted_policy = Some(policy);
                }
                if state.prepared_secondary_removal.as_ref().is_some_and(|p| {
                    authority.current_configuration.epoch > p.intent.current_configuration.epoch
                }) {
                    state.prepared_secondary_removal = None;
                }
            }
            state.accepted_secondary_removal =
                result.postcondition.accepted_secondary_removal.clone();
            if result
                .postcondition
                .accepted_secondary_removal
                .as_ref()
                .is_some_and(|c| {
                    state.prepared_secondary_removal.as_ref() == Some(&c.evidence.preparation)
                })
            {
                state.prepared_secondary_removal = None;
            }
            match &pending.effect.action {
                RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(command) => {
                    crate::protocol::validation::validate_accept_secondary_removal_commit(command)
                        .map_err(|e| {
                            crate::host::HostError::DurableEffectConflict(e.to_string())
                        })?;
                    let intent = &command.committed.evidence.preparation.intent;
                    let receipt = match result.topology_receipt.as_deref() {
                        Some(TopologyReceipt::SecondaryRemoval(receipt)) => Some(receipt),
                        _ => None,
                    };
                    if !command.local_recovery
                        || command.operation_id != pending.effect.operation_id
                        || command.target != state.identity.local_identity
                        || intent.resource_uid != state.identity.resource_uid
                        || result.postcondition.accepted_secondary_removal.as_ref()
                            != Some(&command.committed)
                        || receipt.is_some_and(|receipt| {
                            receipt.accepted.as_ref() != Some(&command.committed)
                        })
                        || result.postcondition.role != ReplicaRole::ActiveSecondary
                        || result.postcondition.write_status == AccessStatus::Granted
                        || result.postcondition.role_transition.is_some()
                        || result.postcondition.authority.as_ref().is_none_or(|a| {
                            a.local_identity != command.target
                                || a.previous_configuration.is_some()
                                || a.current_configuration != intent.current_configuration
                                || a.secondary_removal.as_ref() != Some(&command.committed.evidence)
                        })
                        || result
                            .postcondition
                            .verified_replication_lsn
                            .is_none_or(|lsn| {
                                lsn < command.committed.evidence.preparation.boundary_lsn
                            })
                    {
                        return Err(crate::host::HostError::DurableEffectConflict(
                            "historical acceptance omitted exact local postcondition".into(),
                        ));
                    }
                }
                RuntimeEffectAction::PrepareSecondaryRemoval {
                    intent,
                    process_session_id,
                    report_sequence,
                } => {
                    let receipt = match result.topology_receipt.as_deref() {
                        Some(TopologyReceipt::SecondaryRemoval(receipt)) => Some(receipt),
                        _ => None,
                    };
                    let prepared = result
                        .postcondition
                        .prepared_secondary_removal
                        .as_ref()
                        .ok_or_else(|| {
                            crate::host::HostError::DurableEffectConflict(
                                "preparation omitted durable boundary".into(),
                            )
                        })?;
                    crate::protocol::validation::validate_secondary_removal_preparation(prepared)
                        .map_err(|e| crate::host::HostError::DurableEffectConflict(e.to_string()))?;
                    if &prepared.intent != intent.as_ref()
                        || &prepared.process_session_id != process_session_id
                        || prepared.report_sequence != *report_sequence
                        || prepared.operation_id != pending.effect.operation_id
                        || receipt
                            .is_some_and(|receipt| receipt.preparation.as_ref() != Some(prepared))
                        || result.postcondition.role != ReplicaRole::Primary
                        || result.postcondition.read_status == AccessStatus::Granted
                        || result.postcondition.write_status == AccessStatus::Granted
                        || result.postcondition.current_progress < prepared.boundary_lsn
                        || result.postcondition.authority.as_ref().is_none_or(|a| {
                            a.local_identity != intent.primary
                                || a.current_configuration != intent.previous_configuration
                                || a.previous_configuration.is_some()
                        })
                        || result
                            .postcondition
                            .verified_replication_lsn
                            .is_none_or(|lsn| lsn < prepared.boundary_lsn)
                        || result.postcondition.committed_lsn > prepared.boundary_lsn
                    {
                        return Err(crate::host::HostError::DurableEffectConflict(
                            "preparation returned conflicting authority or progress".into(),
                        ));
                    }
                    state.prepared_secondary_removal = Some(prepared.clone());
                }
                RuntimeEffectAction::RetireReplica(retired) => {
                    retired
                        .validate(&state.identity.local_identity)
                        .map_err(|e| {
                            crate::host::HostError::DurableEffectConflict(e.to_string())
                        })?;
                    let retirement_receipt = match result.topology_receipt.as_deref() {
                        Some(TopologyReceipt::Retirement(receipt)) => Some(receipt),
                        _ => None,
                    };
                    if retirement_receipt
                        .is_some_and(|receipt| !receipt.completed || receipt.retired != **retired)
                        || result.postcondition.retired_authority.as_ref() != Some(retired)
                        || result.postcondition.open
                        || result.postcondition.role != ReplicaRole::None
                        || result.postcondition.read_status != AccessStatus::NotPrimary
                        || result.postcondition.write_status != AccessStatus::NotPrimary
                        || result.postcondition.authority.is_some()
                        || result.postcondition.role_transition.is_some()
                        || !result.postcondition.builds.is_empty()
                    {
                        return Err(crate::host::HostError::DurableEffectConflict(
                            "retirement omitted terminal closure".into(),
                        ));
                    }
                    state.retired_authority = Some(*retired.clone());
                    state.highest_epoch = state.highest_epoch.max(retired.report.epoch);
                    state.previous_policy = None;
                    state.secondary_removal_evidence = None;
                    state.scale_up_evidence = None;
                    state.accepted_secondary_removal = None;
                    state.prepared_secondary_removal = None;
                }
                RuntimeEffectAction::RetireBuild(build_id) => {
                    if result
                        .postcondition
                        .builds
                        .iter()
                        .any(|build| &build.authority.build_id == build_id)
                    {
                        return Err(crate::host::HostError::DurableEffectConflict(
                            "build retirement left the build active".into(),
                        ));
                    }
                    state.retired_builds.insert(build_id.clone());
                }
                _ => {}
            }
            if let RuntimeEffectAction::PrepareSwitchover {
                preparation_generation,
                request_id,
                source,
                target,
                starting_configuration_id,
                starting_epoch,
            } = &pending.effect.action
            {
                let switchover_receipt = match result.topology_receipt.as_deref() {
                    Some(TopologyReceipt::Switchover(receipt)) => Some(receipt),
                    _ => None,
                };
                let authority = result.postcondition.authority.as_ref().ok_or_else(|| {
                    crate::host::HostError::DurableEffectConflict(
                        "planned switchover preparation omitted admitted authority".into(),
                    )
                })?;
                if *preparation_generation == 0
                    || result.postcondition.role != ReplicaRole::Primary
                    || result.postcondition.write_status != AccessStatus::ReconfigurationPending
                    || result.postcondition.current_progress < 0
                    || result.postcondition.committed_lsn > result.postcondition.current_progress
                    || authority.local_identity != *source
                    || authority.current_configuration.configuration_id
                        != *starting_configuration_id
                    || authority.current_configuration.epoch != *starting_epoch
                    || authority.primary_identity() != source
                    || switchover_receipt.is_some_and(|receipt| {
                        !switchover_receipt_matches(
                            receipt,
                            *preparation_generation,
                            request_id,
                            source,
                            target,
                            starting_configuration_id,
                            *starting_epoch,
                        )
                    })
                    || !authority
                        .current_configuration
                        .members
                        .iter()
                        .any(|member| {
                            member.identity == *target && member.role != ReplicaRole::Primary
                        })
                {
                    return Err(crate::host::HostError::DurableEffectConflict(
                        "planned switchover preparation returned an invalid postcondition".into(),
                    ));
                }
                let handoff = SwitchoverHandoff {
                    preparation_generation: *preparation_generation,
                    preparation_operation_id: pending.effect.operation_id.clone(),
                    request_id: request_id.clone(),
                    source: source.clone(),
                    target: target.clone(),
                    starting_configuration_id: starting_configuration_id.clone(),
                    starting_epoch: *starting_epoch,
                    handoff_lsn: switchover_receipt
                        .as_ref()
                        .map_or(result.postcondition.current_progress, |receipt| {
                            receipt.handoff_lsn
                        }),
                };
                if state
                    .prepared_switchover
                    .as_ref()
                    .is_some_and(|existing| existing != &handoff)
                {
                    return Err(crate::host::HostError::DurableEffectConflict(
                        "another planned switchover preparation is retained".into(),
                    ));
                }
                state.prepared_switchover = Some(handoff);
            }
            let retained = RetainedResult {
                operation_id: result.operation_id.clone(),
                effect: pending.effect,
                result: result.clone(),
            };
            if matches!(
                retained.effect.action,
                RuntimeEffectAction::PrepareSecondaryRemoval { .. }
                    | RuntimeEffectAction::RetireReplica(_)
                    | RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(_)
            ) {
                state
                    .removal_effects
                    .insert(retained.operation_id.clone(), retained.clone());
            }
            state.retained_result = Some(retained);
            let next = result.sequence.checked_add(1).ok_or_else(|| {
                crate::host::HostError::DurableEffectConflict("effect sequence exhausted".into())
            })?;
            state.next_effect_sequence = state.next_effect_sequence.max(next);
            write_agent_state(transaction, &state)
        })
    }

    async fn cancel_effect(&self, effect: &RuntimeEffect) -> Result<()> {
        self.with_transaction(|transaction| {
            let mut state = load_state_from_connection(transaction)?;
            let pending = state.pending_effect.take().ok_or_else(|| {
                crate::host::HostError::DurableEffectConflict(
                    "runtime effect has no durable intent".into(),
                )
            })?;
            if pending.effect != *effect {
                return Err(crate::host::HostError::DurableEffectConflict(
                    "cancelled runtime effect does not match durable intent".into(),
                ));
            }
            write_agent_state(transaction, &state)
        })
    }

    async fn begin_configuration(
        &self,
        command: &EnsureConfiguration,
    ) -> Result<BeginConfiguration> {
        self.with_transaction(|transaction| {
            let mut state = load_state_from_connection(transaction)?;
            if let Some(retained) = state.removal_commands.get(&command.operation_id) {
                if retained.command != *command {
                    return Err(crate::host::HostError::DurableEffectConflict(
                        "removal command operation was mutated".into(),
                    ));
                }
                return Ok(BeginConfiguration::Completed(retained.clone()));
            }
            if let Some(retained) = state.retained_command.as_ref()
                && retained.command.operation_id == command.operation_id
            {
                if retained.command != *command {
                    return Err(crate::host::HostError::DurableEffectConflict(
                        "operation ID was reused with different configuration authority".into(),
                    ));
                }
                return Ok(BeginConfiguration::Completed(retained.clone()));
            }
            if let Some(pending) = state.reconfiguration.as_ref() {
                if pending.command != *command {
                    if command.current_epoch <= pending.command.current_epoch {
                        return Err(crate::host::HostError::DurableEffectConflict(
                            "another configuration command is pending".into(),
                        ));
                    }
                    if let Some(effect) = state.pending_effect.as_ref()
                        && !effect
                            .effect
                            .operation_id
                            .as_str()
                            .starts_with(&format!("{}:", pending.command.operation_id.as_str()))
                    {
                        return Err(crate::host::HostError::DurableEffectConflict(
                            "pending runtime effect is not owned by the superseded command".into(),
                        ));
                    }
                    state.pending_effect = None;
                    let record = ReconfigurationRecord {
                        command: command.clone(),
                        stage: CoordinatorStage::AdmitAuthority,
                        observed_lsn: None,
                    };
                    state.reconfiguration = Some(record.clone());
                    write_agent_state(transaction, &state)?;
                    return Ok(BeginConfiguration::Superseded(record));
                }
                return Ok(BeginConfiguration::Pending(pending.clone()));
            }
            let record = ReconfigurationRecord {
                command: command.clone(),
                stage: if is_access_only_configuration(command, &state) {
                    CoordinatorStage::Activate
                } else {
                    CoordinatorStage::AdmitAuthority
                },
                observed_lsn: None,
            };
            state.reconfiguration = Some(record.clone());
            write_agent_state(transaction, &state)?;
            Ok(BeginConfiguration::Execute(record))
        })
    }

    async fn journal_build(&self, command: &EnsureReplicaBuild) -> Result<EnsureReplicaBuild> {
        self.with_transaction(|transaction| {
            let mut state = load_state_from_connection(transaction)?;
            if state.retired_builds.contains(&command.operation_id)
                || state.abandoned_builds.contains(&command.operation_id)
            {
                return Err(crate::host::HostError::CommandRejected(
                    "abandoned or retired build authority cannot be reopened".into(),
                ));
            }
            if let Some(existing) = state.build_commands.get(&command.operation_id) {
                let immutable_matches = existing.operation_id == command.operation_id
                    && existing.local_replica_id == command.local_replica_id
                    && existing.expected_instance_id == command.expected_instance_id
                    && existing.expected_agent_generation == command.expected_agent_generation
                    && existing.target == command.target
                    && existing.authority == command.authority
                    && existing.retire == command.retire;
                if !immutable_matches
                    || (existing.authority.is_none()
                        && existing.source_session_id != command.source_session_id)
                {
                    return Err(crate::host::HostError::DurableEffectConflict(
                        "build operation was reused with different immutable authority".into(),
                    ));
                }
                if existing != command {
                    state
                        .build_commands
                        .insert(command.operation_id.clone(), command.clone());
                    write_agent_state(transaction, &state)?;
                }
                return Ok(command.clone());
            }
            state
                .build_commands
                .insert(command.operation_id.clone(), command.clone());
            write_agent_state(transaction, &state)?;
            Ok(command.clone())
        })
    }

    async fn abandon_build(&self, command: &EnsureReplicaBuild) -> Result<()> {
        self.with_transaction(|transaction| {
            let mut state = load_state_from_connection(transaction)?;
            crate::host::command::admit_build(command, &state)?;
            if state.retired_builds.contains(&command.operation_id) {
                return Ok(());
            }
            if let Some(pending) = state.pending_effect.as_ref() {
                let matching_build = matches!(
                    &pending.effect.action,
                    RuntimeEffectAction::BuildReplica {
                        build_id,
                        target,
                        ..
                    } if build_id == &command.operation_id
                        && target == &command.target
                        && pending.effect.operation_id
                            == OperationId::new(format!(
                                "{}:build-replica",
                                command.operation_id
                            ))
                );
                let matching_retirement = matches!(
                    &pending.effect.action,
                    RuntimeEffectAction::RetireBuild(build_id)
                        if build_id == &command.operation_id
                            && pending.effect.operation_id
                                == OperationId::new(format!(
                                    "{}:retire-abandoned-build",
                                    command.operation_id
                                ))
                );
                if !matching_build && !matching_retirement {
                    return Err(crate::host::HostError::DurableEffectConflict(
                        "build abandonment conflicts with unrelated pending work".into(),
                    ));
                }
            }
            state.abandoned_builds.insert(command.operation_id.clone());
            write_agent_state(transaction, &state)
        })
    }

    async fn advance_configuration(
        &self,
        operation_id: &OperationId,
        expected: CoordinatorStage,
        next: CoordinatorStage,
        observed_lsn: Option<i64>,
    ) -> Result<ReconfigurationRecord> {
        self.with_transaction(|transaction| {
            let mut state = load_state_from_connection(transaction)?;
            let record = state.reconfiguration.as_mut().ok_or_else(|| {
                crate::host::HostError::DurableEffectConflict(
                    "configuration command is not pending".into(),
                )
            })?;
            if &record.command.operation_id != operation_id || record.stage != expected {
                return Err(crate::host::HostError::DurableEffectConflict(
                    "configuration stage does not match durable command".into(),
                ));
            }
            record.stage = next;
            if observed_lsn.is_some() {
                record.observed_lsn = observed_lsn;
            }
            let result = record.clone();
            if expected == CoordinatorStage::Deactivate {
                state.deactivation = Some(DeactivationState {
                    epoch: result.command.current_epoch,
                    deactivated_lsn: result.observed_lsn.unwrap_or(0),
                });
            }
            write_agent_state(transaction, &state)?;
            Ok(result)
        })
    }

    async fn complete_configuration(
        &self,
        operation_id: &OperationId,
    ) -> Result<RetainedCommandResult> {
        self.with_transaction(|transaction| {
            let mut state = load_state_from_connection(transaction)?;
            let record = state.reconfiguration.take().ok_or_else(|| {
                crate::host::HostError::DurableEffectConflict(
                    "configuration command is not pending".into(),
                )
            })?;
            if &record.command.operation_id != operation_id
                || record.stage != CoordinatorStage::Complete
            {
                return Err(crate::host::HostError::DurableEffectConflict(
                    "configuration command has not reached its terminal stage".into(),
                ));
            }
            if !record.command.retire_switchover_preparation_ids.is_empty() {
                let retirement_id = &record.command.retire_switchover_preparation_ids[0];
                if record.command.is_switchover_restoration()
                    && record.command.switchover_handoff.is_none()
                {
                    if state
                        .prepared_switchover
                        .as_ref()
                        .is_some_and(|prepared| &prepared.preparation() != retirement_id)
                    {
                        return Err(crate::host::HostError::DurableEffectConflict(
                            "restoration retires another preparation".into(),
                        ));
                    }
                    if let Some(prepared) = state.prepared_switchover.take() {
                        state.retired_switchover = Some(prepared);
                    }
                } else {
                    let prepared = state
                        .prepared_switchover
                        .as_ref()
                        .or(state.retired_switchover.as_ref())
                        .ok_or_else(|| {
                            crate::host::HostError::DurableEffectConflict(
                                "configuration retires missing switchover preparation".into(),
                            )
                        })?;
                    if record.command.switchover_handoff.as_ref() != Some(prepared) {
                        return Err(crate::host::HostError::DurableEffectConflict(
                            "configuration retires a changed switchover certificate".into(),
                        ));
                    }
                    if record.command.retire_switchover_preparation_ids.len() != 1
                        || record.command.retire_switchover_preparation_ids[0]
                            != prepared.preparation()
                    {
                        return Err(crate::host::HostError::DurableEffectConflict(
                            "configuration retires another switchover preparation".into(),
                        ));
                    }
                    state.retired_switchover = Some(prepared.clone());
                    state.prepared_switchover = None;
                }
                let (starting_epoch, starting_configuration_id) =
                    record.command.switchover_handoff.as_ref().map_or(
                        (
                            record.command.current_epoch,
                            record
                                .command
                                .current_configuration
                                .configuration_id
                                .clone(),
                        ),
                        |handoff| {
                            (
                                handoff.starting_epoch,
                                handoff.starting_configuration_id.clone(),
                            )
                        },
                    );
                if state
                    .preparation_retirement
                    .as_ref()
                    .is_some_and(|retired| {
                        retired.starting_epoch > starting_epoch
                            || (retired.starting_epoch == starting_epoch
                                && (retired.starting_configuration_id != starting_configuration_id
                                    || retired.generation > retirement_id.generation))
                    })
                {
                    return Err(crate::host::HostError::DurableEffectConflict(
                        "preparation retirement regresses durable fencing".into(),
                    ));
                }
                state.preparation_retirement = Some(crate::host::state::PreparationRetirement {
                    starting_epoch,
                    starting_configuration_id,
                    generation: retirement_id.generation,
                });
            }
            let result = RetainedCommandResult {
                command: record.command,
                role: state.role,
                epoch: state.highest_epoch,
            };
            if result.command.transition_kind == TransitionKind::SecondaryScaleDown {
                state
                    .removal_commands
                    .insert(result.command.operation_id.clone(), result.clone());
            }
            if result.command.current_only && result.command.scale_up_evidence.is_some() {
                state.completed_scale_up = Some(Box::new(result.clone()));
            }
            state.retained_command = Some(result.clone());
            write_agent_state(transaction, &state)?;
            Ok(result)
        })
    }

    #[cfg(test)]
    async fn retained_result(&self) -> Result<Option<RetainedResult>> {
        Ok(self.load_state().await?.retained_result)
    }

    #[cfg(test)]
    async fn set_reconfiguration(&self, data: Option<String>) -> Result<()> {
        self.with_transaction(|transaction| {
            let mut state = load_state_from_connection(transaction)?;
            state.reconfiguration_data = data;
            write_agent_state(transaction, &state)
        })
    }

    #[cfg(test)]
    async fn clear_reconfiguration(&self) -> Result<()> {
        self.with_transaction(|transaction| {
            let mut state = load_state_from_connection(transaction)?;
            state.reconfiguration_data = None;
            write_agent_state(transaction, &state)
        })
    }

    #[cfg(test)]
    async fn migrate_schema(&self, expected_version: u32, target_version: u32) -> Result<()> {
        self.with_transaction(|transaction| {
            let observed: u32 =
                transaction.query_row("PRAGMA user_version", [], |row| row.get(0))?;
            if observed != expected_version
                || expected_version != SCHEMA_VERSION
                || target_version != SCHEMA_VERSION
            {
                return Err(crate::host::HostError::SchemaMismatch {
                    expected: target_version,
                    observed,
                });
            }
            transaction.execute(
                "INSERT INTO schema_migrations(version) VALUES(?1)
                 ON CONFLICT(version) DO NOTHING",
                [target_version],
            )?;
            Ok(())
        })
    }

    async fn record_partition_reports(
        &self,
        load_metrics: Vec<LoadMetric>,
        reported_fault: Option<FaultType>,
    ) -> Result<()> {
        self.with_transaction(|transaction| {
            let mut names = std::collections::BTreeSet::new();
            if load_metrics.iter().any(|metric| {
                metric.name.is_empty() || metric.value < 0 || !names.insert(metric.name.clone())
            }) {
                return Err(crate::host::HostError::CommandRejected(
                    "load metrics require unique nonempty names and nonnegative values".into(),
                ));
            }
            let mut state = load_state_from_connection(transaction)?;
            state.load_metrics = load_metrics;
            state.reported_fault = reported_fault;
            write_agent_state(transaction, &state)
        })
    }
}

fn validate_acceptance_conversion(
    transaction: &Transaction<'_>,
    command: &crate::protocol::command::AcceptSecondaryRemovalCommit,
) -> Result<()> {
    let authority: Option<AdmittedAuthority> = load_json_optional(
        transaction,
        "SELECT authority_json FROM replica_authority WHERE singleton = 1",
        [],
    )?;
    let expected = AdmittedAuthority {
        local_identity: command.target.clone(),
        transition_kind: None,
        previous_configuration: None,
        current_configuration: command
            .committed
            .evidence
            .preparation
            .intent
            .current_configuration
            .clone(),
        switchover_handoff: None,
        secondary_removal: Some(command.committed.evidence.clone()),
        scale_up: None,
    };
    let progress: Option<ReplicationProgress> = load_json_optional(
        transaction,
        "SELECT progress_json FROM replication_progress WHERE fence_json = ?1",
        [contract_json(&expected.fence())?],
    )?;
    if authority.as_ref() != Some(&expected)
        || progress.as_ref().is_none_or(|p| {
            p.fence != expected.fence()
                || p.verified_lsn < command.committed.evidence.preparation.boundary_lsn
        })
    {
        return Err(crate::host::HostError::CommandRejected(
            "acceptance conversion requires exact durable authority and verified boundary".into(),
        ));
    }
    Ok(())
}

#[async_trait]
impl ReplicaAuthorityStore for SqliteStore {
    async fn load(&self) -> ContractResult<Option<AdmittedAuthority>> {
        let connection = self.connection.lock().map_err(|_| {
            ContractError::Persistence("agent database connection mutex was poisoned".into())
        })?;
        load_json_optional(
            &connection,
            "SELECT authority_json FROM replica_authority WHERE singleton = 1",
            [],
        )
    }

    async fn admit(&self, authority: &AdmittedAuthority) -> ContractResult<()> {
        authority.validate()?;
        #[cfg(all(test, feature = "testing"))]
        let failure = self
            .authority_admission_failure
            .swap(0, std::sync::atomic::Ordering::SeqCst);
        #[cfg(all(test, feature = "testing"))]
        if failure == 1 {
            return Err(ContractError::Persistence(
                "injected authority failure before write".into(),
            ));
        }
        self.contract_transaction(|transaction| {
            let state = load_state_from_connection(transaction)
                .map_err(|e| ContractError::Persistence(e.to_string()))?;
            let retired: Option<RetiredAuthority> = load_json_optional(
                transaction,
                "SELECT value_json FROM runtime_lifecycle WHERE kind = 'retired'",
                [],
            )?;
            let started: Option<RetiredAuthority> = load_json_optional(
                transaction,
                "SELECT value_json FROM runtime_lifecycle WHERE kind = 'retirement-started'",
                [],
            )?;
            if retired.is_some()
                || started.is_some()
                || state.retired_authority.is_some()
                || authority.local_identity != state.identity.local_identity
            {
                return Err(ContractError::AuthorityMismatch(
                    "retired or mismatched local authority".into(),
                ));
            }
            let existing: Option<AdmittedAuthority> = load_json_optional(
                transaction,
                "SELECT authority_json FROM replica_authority WHERE singleton = 1",
                [],
            )?;
            if let Some(existing) = existing {
                if existing == *authority {
                    return Ok(());
                }
                if authority.current_configuration.epoch < existing.current_configuration.epoch {
                    return Err(ContractError::AuthorityMismatch(
                        "replica authority epoch regressed".into(),
                    ));
                }
                if authority.current_configuration.epoch == existing.current_configuration.epoch
                    && existing != *authority
                    && !authority.is_current_only_completion_of(&existing)
                {
                    return Err(ContractError::AuthorityMismatch(
                        "replica authority changed without a newer epoch".into(),
                    ));
                }
            }
            let json = contract_json(authority)?;
            transaction
                .execute(
                    "INSERT INTO replica_authority(singleton, authority_json) VALUES(1, ?1)
                     ON CONFLICT(singleton) DO UPDATE SET authority_json = excluded.authority_json",
                    [json],
                )
                .map_err(contract_sqlite_error)?;
            Ok(())
        })?;
        #[cfg(all(test, feature = "testing"))]
        if failure == 2 {
            return Err(ContractError::Persistence(
                "injected authority failure after commit".into(),
            ));
        }
        Ok(())
    }

    async fn load_secondary_removal(&self) -> ContractResult<Option<SecondaryRemovalPreparation>> {
        let connection = self
            .connection
            .lock()
            .map_err(|e| ContractError::Persistence(e.to_string()))?;
        load_json_optional(
            &connection,
            "SELECT value_json FROM runtime_lifecycle WHERE kind = 'preparation'",
            [],
        )
    }

    async fn record_secondary_removal(
        &self,
        preparation: &SecondaryRemovalPreparation,
    ) -> ContractResult<()> {
        crate::protocol::validation::validate_secondary_removal_preparation(preparation)
            .map_err(|e| ContractError::AuthorityMismatch(e.to_string()))?;
        self.contract_transaction(|transaction| {
            let state = load_state_from_connection(transaction)
                .map_err(|e| ContractError::Persistence(e.to_string()))?;
            if preparation.intent.primary != state.identity.local_identity
                || preparation.intent.resource_uid != state.identity.resource_uid
                || state.retired_authority.is_some()
            {
                return Err(ContractError::AuthorityMismatch(
                    "preparation belongs to another primary".into(),
                ));
            }
            let existing: Option<SecondaryRemovalPreparation> = load_json_optional(
                transaction,
                "SELECT value_json FROM runtime_lifecycle WHERE kind = 'preparation'",
                [],
            )?;
            if let Some(existing) = existing {
                if existing == *preparation {
                    return Ok(());
                }
                if existing.operation_id == preparation.operation_id
                    || preparation.intent.previous_configuration.epoch
                        < existing.intent.current_configuration.epoch
                {
                    return Err(ContractError::AuthorityMismatch(
                        "preparation regresses durable high-water authority".into(),
                    ));
                }
            }
            write_lifecycle(transaction, "preparation", preparation)
        })
    }

    async fn load_retired_authority(&self) -> ContractResult<Option<RetiredAuthority>> {
        let connection = self
            .connection
            .lock()
            .map_err(|e| ContractError::Persistence(e.to_string()))?;
        load_json_optional(
            &connection,
            "SELECT value_json FROM runtime_lifecycle WHERE kind = 'retired'",
            [],
        )
    }

    async fn load_retirement_started(&self) -> ContractResult<Option<RetiredAuthority>> {
        let connection = self
            .connection
            .lock()
            .map_err(|e| ContractError::Persistence(e.to_string()))?;
        load_json_optional(
            &connection,
            "SELECT value_json FROM runtime_lifecycle WHERE kind = 'retirement-started'",
            [],
        )
    }

    async fn record_retirement_started(&self, authority: &RetiredAuthority) -> ContractResult<()> {
        self.contract_transaction(|transaction| {
            if validate_retirement(transaction, authority)? {
                return Ok(());
            }
            write_lifecycle(transaction, "retirement-started", authority)
        })
    }

    async fn load_secondary_removal_commit(
        &self,
    ) -> ContractResult<Option<crate::protocol::types::SecondaryScaleDownCleanup>> {
        let connection = self
            .connection
            .lock()
            .map_err(|e| ContractError::Persistence(e.to_string()))?;
        load_json_optional(
            &connection,
            "SELECT value_json FROM runtime_lifecycle WHERE kind = 'commit'",
            [],
        )
    }

    async fn record_secondary_removal_commit(
        &self,
        committed: &crate::protocol::types::SecondaryScaleDownCleanup,
    ) -> ContractResult<()> {
        crate::protocol::validation::validate_secondary_scale_down_cleanup(committed)
            .map_err(|e| ContractError::AuthorityMismatch(e.to_string()))?;
        self.contract_transaction(|transaction| {
            let active: Option<AdmittedAuthority> = load_json_optional(
                transaction,
                "SELECT authority_json FROM replica_authority WHERE singleton = 1",
                [],
            )?;
            if active.as_ref().is_none_or(|a| {
                a.previous_configuration.is_some()
                    || a.secondary_removal.as_ref() != Some(&committed.evidence)
            }) {
                return Err(ContractError::AuthorityMismatch(
                    "commit differs from admitted reduction".into(),
                ));
            }
            let existing: Option<crate::protocol::types::SecondaryScaleDownCleanup> =
                load_json_optional(
                    transaction,
                    "SELECT value_json FROM runtime_lifecycle WHERE kind = 'commit'",
                    [],
                )?;
            if existing.as_ref().is_some_and(|old| {
                old != committed
                    && old.evidence.preparation.intent.current_configuration.epoch
                        >= committed
                            .evidence
                            .preparation
                            .intent
                            .current_configuration
                            .epoch
            }) {
                return Err(ContractError::AuthorityMismatch(
                    "commit was mutated or regressed".into(),
                ));
            }
            write_lifecycle(transaction, "commit", committed)
        })
    }

    async fn retire(&self, authority: &RetiredAuthority) -> ContractResult<()> {
        self.contract_transaction(|transaction| {
            if validate_retirement(transaction, authority)? {
                return Ok(());
            }
            write_lifecycle(transaction, "retired", authority)?;
            transaction
                .execute("DELETE FROM replica_authority", [])
                .map_err(contract_sqlite_error)?;
            transaction
                .execute(
                    "DELETE FROM runtime_lifecycle WHERE kind = 'retirement-started'",
                    [],
                )
                .map_err(contract_sqlite_error)?;
            Ok(())
        })
    }
}

fn validate_retirement(
    connection: &Connection,
    authority: &RetiredAuthority,
) -> ContractResult<bool> {
    let state = load_state_from_connection(connection)
        .map_err(|e| ContractError::Persistence(e.to_string()))?;
    authority.validate(&state.identity.local_identity)?;
    if authority.report.intent.resource_uid != state.identity.resource_uid {
        return Err(ContractError::AuthorityMismatch(
            "retirement resource differs".into(),
        ));
    }
    let retired: Option<RetiredAuthority> = load_json_optional(
        connection,
        "SELECT value_json FROM runtime_lifecycle WHERE kind = 'retired'",
        [],
    )?;
    let started: Option<RetiredAuthority> = load_json_optional(
        connection,
        "SELECT value_json FROM runtime_lifecycle WHERE kind = 'retirement-started'",
        [],
    )?;
    if [
        retired.as_ref(),
        started.as_ref(),
        state.retired_authority.as_ref(),
    ]
    .into_iter()
    .flatten()
    .any(|old| old != authority)
    {
        return Err(ContractError::AuthorityMismatch(
            "conflicting durable retirement".into(),
        ));
    }
    if retired.is_some() {
        return Ok(true);
    }
    let active: Option<AdmittedAuthority> = load_json_optional(
        connection,
        "SELECT authority_json FROM replica_authority WHERE singleton = 1",
        [],
    )?;
    if active.as_ref().is_some_and(|a| {
        a.local_identity != state.identity.local_identity
            || a.current_configuration != authority.report.intent.previous_configuration
            || a.previous_configuration.is_some()
    }) {
        return Err(ContractError::AuthorityMismatch(
            "retirement differs from installed authority".into(),
        ));
    }
    Ok(false)
}

#[async_trait]
impl ReplicationProgressStore for SqliteStore {
    async fn load_replication_progress(
        &self,
        fence: &AuthorityFence,
    ) -> ContractResult<Option<ReplicationProgress>> {
        let connection = self.connection.lock().map_err(|_| {
            ContractError::Persistence("agent database connection mutex was poisoned".into())
        })?;
        let fence_json = contract_json(fence)?;
        load_json_optional(
            &connection,
            "SELECT progress_json FROM replication_progress WHERE fence_json = ?1",
            [fence_json],
        )
    }

    async fn load_configuration_progress(
        &self,
        epoch: Epoch,
        current_configuration_id: &ConfigurationId,
    ) -> ContractResult<Option<ReplicationProgress>> {
        let connection = self.connection.lock().map_err(|_| {
            ContractError::Persistence("agent database connection mutex was poisoned".into())
        })?;
        let mut statement = connection
            .prepare(
                "SELECT progress_json FROM replication_progress
                 WHERE data_loss_number = ?1 AND configuration_number = ?2
                   AND current_configuration_id = ?3
                 ORDER BY verified_lsn DESC LIMIT 1",
            )
            .map_err(contract_sqlite_error)?;
        let json: Option<String> = statement
            .query_row(
                params![
                    epoch.data_loss_number,
                    epoch.configuration_number,
                    current_configuration_id.as_str()
                ],
                |row| row.get(0),
            )
            .optional()
            .map_err(contract_sqlite_error)?;
        json.map(|value| contract_from_json(&value)).transpose()
    }

    async fn record_replication_progress(
        &self,
        progress: &ReplicationProgress,
    ) -> ContractResult<()> {
        self.contract_transaction(|transaction| {
            let fence_json = contract_json(&progress.fence)?;
            let existing: Option<ReplicationProgress> = load_json_optional(
                transaction,
                "SELECT progress_json FROM replication_progress WHERE fence_json = ?1",
                [fence_json.clone()],
            )?;
            if existing
                .as_ref()
                .is_some_and(|existing| existing.verified_lsn > progress.verified_lsn)
            {
                return Err(ContractError::AuthorityMismatch(
                    "replication progress regressed".into(),
                ));
            }
            let progress_json = contract_json(progress)?;
            transaction
                .execute(
                    "INSERT INTO replication_progress(
                        fence_json, data_loss_number, configuration_number,
                        current_configuration_id, verified_lsn, progress_json
                     ) VALUES(?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT(fence_json) DO UPDATE SET
                        verified_lsn = excluded.verified_lsn,
                        progress_json = excluded.progress_json",
                    params![
                        fence_json,
                        progress.fence.epoch.data_loss_number,
                        progress.fence.epoch.configuration_number,
                        progress.fence.current_configuration_id.as_str(),
                        progress.verified_lsn,
                        progress_json
                    ],
                )
                .map_err(contract_sqlite_error)?;
            Ok(())
        })
    }
}

#[async_trait]
impl LocalWriteJournal for SqliteStore {
    async fn load_local_write(
        &self,
        operation_id: &OperationId,
    ) -> ContractResult<Option<DurableLocalWrite>> {
        let connection = self.connection.lock().map_err(|_| {
            ContractError::Persistence("agent database connection mutex was poisoned".into())
        })?;
        load_json_optional(
            &connection,
            "SELECT write_json FROM local_writes WHERE operation_id = ?1",
            [operation_id.as_str()],
        )
    }

    async fn load_local_writes(&self) -> ContractResult<Vec<DurableLocalWrite>> {
        let connection = self.connection.lock().map_err(|_| {
            ContractError::Persistence("agent database connection mutex was poisoned".into())
        })?;
        let mut statement = connection
            .prepare(
                "SELECT write_json FROM local_writes WHERE phase != ?1 ORDER BY lsn, operation_id",
            )
            .map_err(contract_sqlite_error)?;
        let rows = statement
            .query_map([phase_tag(LocalWritePhase::Committed)], |row| {
                row.get::<_, String>(0)
            })
            .map_err(contract_sqlite_error)?;
        rows.map(|row| {
            let json = row.map_err(contract_sqlite_error)?;
            contract_from_json(&json)
        })
        .collect()
    }

    async fn record_local_write(&self, write: &DurableLocalWrite) -> ContractResult<()> {
        self.contract_transaction(|transaction| {
            let existing: Option<DurableLocalWrite> = load_json_optional(
                transaction,
                "SELECT write_json FROM local_writes WHERE operation_id = ?1",
                [write.operation_id.as_str()],
            )?;
            if let Some(existing) = existing {
                if existing.lsn != write.lsn
                    || existing.committed_lsn != write.committed_lsn
                    || existing.data != write.data
                {
                    return Err(ContractError::AuthorityMismatch(
                        "local write identity or payload changed".into(),
                    ));
                }
                if phase_tag(write.phase) < phase_tag(existing.phase) {
                    return Err(ContractError::AuthorityMismatch(
                        "local write phase regressed".into(),
                    ));
                }
            }
            let json = contract_json(write)?;
            transaction
                .execute(
                    "INSERT INTO local_writes(operation_id, lsn, phase, write_json)
                     VALUES(?1, ?2, ?3, ?4)
                     ON CONFLICT(operation_id) DO UPDATE SET
                        lsn = excluded.lsn,
                        phase = excluded.phase,
                        write_json = excluded.write_json",
                    params![
                        write.operation_id.as_str(),
                        write.lsn,
                        phase_tag(write.phase),
                        json
                    ],
                )
                .map_err(contract_sqlite_error)?;
            Ok(())
        })
    }

    async fn reset_local_writes_after_data_loss(&self, committed_lsn: i64) -> ContractResult<()> {
        self.contract_transaction(|transaction| {
            transaction
                .execute(
                    "DELETE FROM local_writes WHERE phase != ?1 OR lsn > ?2",
                    params![phase_tag(LocalWritePhase::Committed), committed_lsn],
                )
                .map_err(contract_sqlite_error)?;
            Ok(())
        })
    }
}

#[async_trait]
impl BuildAuthorityStore for SqliteStore {
    async fn select_build(
        &self,
        authority: &BuildAuthority,
    ) -> ContractResult<crate::authority::BuildSelection> {
        use crate::authority::BuildSelection;
        self.contract_transaction(|transaction| {
            let mut state = load_state_from_connection(transaction)
                .map_err(|error| ContractError::Persistence(error.to_string()))?;
            let admitted: Option<BuildAuthority> = load_json_optional(
                transaction,
                "SELECT authority_json FROM build_authority WHERE build_id = ?1",
                [authority.build_id.as_str()],
            )?;
            if admitted.as_ref() != Some(authority)
                || state.retired_builds.contains(&authority.build_id)
                || state.abandoned_builds.contains(&authority.build_id)
            {
                return Err(ContractError::AuthorityMismatch(
                    "custom build authority is not admitted".into(),
                ));
            }
            let key = build_selection_key(&authority.target)?;
            let previous: Option<BuildSelection> = load_lifecycle(transaction, &key)?;
            if let Some(previous) = &previous {
                if previous.authority == *authority {
                    return Ok(previous.clone());
                }
                if previous.authority.current_configuration.epoch
                    > authority.current_configuration.epoch
                {
                    return Err(ContractError::AuthorityMismatch(
                        "custom build epoch regressed".into(),
                    ));
                }
                state
                    .abandoned_builds
                    .insert(previous.authority.build_id.clone());
            }
            let generation = previous
                .map_or(Some(1), |previous| previous.generation.checked_add(1))
                .ok_or_else(|| {
                    ContractError::AuthorityMismatch("build selection generation exhausted".into())
                })?;
            let selection = BuildSelection {
                authority: authority.clone(),
                generation,
            };
            write_lifecycle(transaction, &key, &selection)?;
            write_agent_state(transaction, &state)
                .map_err(|error| ContractError::Persistence(error.to_string()))?;
            Ok(selection)
        })
    }

    async fn load_build_selection(
        &self,
        target: &ReplicaIdentity,
    ) -> ContractResult<Option<crate::authority::BuildSelection>> {
        let connection = self.connection.lock().map_err(|_| {
            ContractError::Persistence("agent database connection mutex was poisoned".into())
        })?;
        let selection: Option<crate::authority::BuildSelection> =
            load_lifecycle(&connection, &build_selection_key(target)?)?;
        let state = load_state_from_connection(&connection)
            .map_err(|error| ContractError::Persistence(error.to_string()))?;
        Ok(selection.filter(|selection| {
            selection.authority.target == *target
                && !state.retired_builds.contains(&selection.authority.build_id)
                && !state
                    .abandoned_builds
                    .contains(&selection.authority.build_id)
        }))
    }

    async fn load_build(&self, build_id: &OperationId) -> ContractResult<Option<BuildAuthority>> {
        let connection = self.connection.lock().map_err(|_| {
            ContractError::Persistence("agent database connection mutex was poisoned".into())
        })?;
        let state = load_state_from_connection(&connection)
            .map_err(|error| ContractError::Persistence(error.to_string()))?;
        if state.retired_builds.contains(build_id) || state.abandoned_builds.contains(build_id) {
            return Ok(None);
        }
        load_json_optional(
            &connection,
            "SELECT authority_json FROM build_authority WHERE build_id = ?1",
            [build_id.as_str()],
        )
    }

    async fn load_builds(&self) -> ContractResult<Vec<BuildAuthority>> {
        let connection = self.connection.lock().map_err(|_| {
            ContractError::Persistence("agent database connection mutex was poisoned".into())
        })?;
        let state = load_state_from_connection(&connection)
            .map_err(|error| ContractError::Persistence(error.to_string()))?;
        let mut statement = connection
            .prepare("SELECT build_id, authority_json FROM build_authority ORDER BY build_id")
            .map_err(contract_sqlite_error)?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(contract_sqlite_error)?;
        let mut authorities = Vec::new();
        for row in rows {
            let (build_id, json) = row.map_err(contract_sqlite_error)?;
            let build_id = OperationId::new(build_id);
            if !state.retired_builds.contains(&build_id)
                && !state.abandoned_builds.contains(&build_id)
            {
                authorities.push(contract_from_json(&json)?);
            }
        }
        Ok(authorities)
    }

    async fn admit_build(&self, authority: &BuildAuthority) -> ContractResult<()> {
        authority
            .validate()
            .map_err(|error| ContractError::AuthorityMismatch(error.to_string()))?;
        self.contract_transaction(|transaction| {
            let state = load_state_from_connection(transaction)
                .map_err(|error| ContractError::Persistence(error.to_string()))?;
            if state.retired_builds.contains(&authority.build_id)
                || state.abandoned_builds.contains(&authority.build_id)
            {
                return Err(ContractError::AuthorityMismatch(
                    "retired build authority cannot be admitted again".into(),
                ));
            }
            let existing: Option<BuildAuthority> = load_json_optional(
                transaction,
                "SELECT authority_json FROM build_authority WHERE build_id = ?1",
                [authority.build_id.as_str()],
            )?;
            if let Some(existing) = existing
                && existing != *authority
            {
                return Err(ContractError::AuthorityMismatch(
                    "build ID was reused with different authority".into(),
                ));
            }
            let json = contract_json(authority)?;
            transaction
                .execute(
                    "INSERT INTO build_authority(build_id, authority_json) VALUES(?1, ?2)
                     ON CONFLICT(build_id) DO NOTHING",
                    params![authority.build_id.as_str(), json],
                )
                .map_err(contract_sqlite_error)?;
            Ok(())
        })
    }
}

#[async_trait]
impl BuildProgressStore for SqliteStore {
    async fn load_build_progress(
        &self,
        build_id: &OperationId,
    ) -> ContractResult<Option<DurableBuildProgress>> {
        let connection = self.connection.lock().map_err(|_| {
            ContractError::Persistence("agent database connection mutex was poisoned".into())
        })?;
        load_json_optional(
            &connection,
            "SELECT progress_json FROM build_progress WHERE build_id = ?1",
            [build_id.as_str()],
        )
    }

    async fn record_build_progress(&self, progress: &DurableBuildProgress) -> ContractResult<()> {
        self.record_build_progress_inner(progress, None)
    }

    async fn record_selected_build_progress(
        &self,
        selection: &crate::authority::BuildSelection,
        progress: &DurableBuildProgress,
    ) -> ContractResult<()> {
        self.record_build_progress_inner(progress, Some(selection))
    }
}

fn build_selection_key(target: &ReplicaIdentity) -> ContractResult<String> {
    Ok(format!("build-selection/{}", target.replica_id))
}

fn load_lifecycle<T: serde::de::DeserializeOwned>(
    connection: &Connection,
    key: &str,
) -> ContractResult<Option<T>> {
    load_json_optional(
        connection,
        "SELECT value_json FROM runtime_lifecycle WHERE kind = ?1",
        [key],
    )
}

impl SqliteStore {
    fn record_build_progress_inner(
        &self,
        progress: &DurableBuildProgress,
        selection: Option<&crate::authority::BuildSelection>,
    ) -> ContractResult<()> {
        self.contract_transaction(|transaction| {
            if let Some(selection) = selection {
                let current: Option<crate::authority::BuildSelection> = load_lifecycle(
                    transaction,
                    &build_selection_key(&progress.authority.target)?,
                )?;
                if current.as_ref() != Some(selection) || selection.authority != progress.authority
                {
                    return Err(ContractError::AuthorityMismatch(
                        "custom build selection was superseded".into(),
                    ));
                }
            }
            let state = load_state_from_connection(transaction)
                .map_err(|error| ContractError::Persistence(error.to_string()))?;
            if state.retired_builds.contains(&progress.authority.build_id)
                || state
                    .abandoned_builds
                    .contains(&progress.authority.build_id)
            {
                return Err(ContractError::AuthorityMismatch(
                    "retired or abandoned build cannot publish progress".into(),
                ));
            }
            let authority: Option<BuildAuthority> = load_json_optional(
                transaction,
                "SELECT authority_json FROM build_authority WHERE build_id = ?1",
                [progress.authority.build_id.as_str()],
            )?;
            if authority.as_ref() != Some(&progress.authority) {
                return Err(ContractError::AuthorityMismatch(
                    "build progress lacks matching durable authorization".into(),
                ));
            }
            let existing: Option<DurableBuildProgress> = load_json_optional(
                transaction,
                "SELECT progress_json FROM build_progress WHERE build_id = ?1",
                [progress.authority.build_id.as_str()],
            )?;
            if existing.as_ref().is_some_and(|existing| {
                progress.last_sequence < existing.last_sequence
                    || progress.durable_lsn < existing.durable_lsn
                    || (existing.completed && !progress.completed)
                    || existing.catch_up_boundary_lsn.is_some()
                        && progress.catch_up_boundary_lsn != existing.catch_up_boundary_lsn
            }) {
                return Err(ContractError::AuthorityMismatch(
                    "build progress regressed".into(),
                ));
            }
            let json = contract_json(progress)?;
            transaction
                .execute(
                    "INSERT INTO build_progress(build_id, progress_json) VALUES(?1, ?2)
                     ON CONFLICT(build_id) DO UPDATE SET progress_json = excluded.progress_json",
                    params![progress.authority.build_id.as_str(), json],
                )
                .map_err(contract_sqlite_error)?;
            let mut state = load_state_from_connection(transaction)
                .map_err(|error| ContractError::Persistence(error.to_string()))?;
            state
                .build_progress
                .insert(progress.authority.build_id.clone(), progress.clone());
            write_agent_state(transaction, &state)
                .map_err(|error| ContractError::Persistence(error.to_string()))?;
            Ok(())
        })
    }
}

fn open_connection(path: &Path) -> Result<Connection> {
    Ok(Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?)
}

fn configure_durability(connection: &Connection) -> Result<()> {
    let journal_mode: String =
        connection.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(crate::host::HostError::Corrupt(format!(
            "SQLite refused WAL journaling and selected {journal_mode}"
        )));
    }
    connection.execute_batch(
        "PRAGMA synchronous = FULL;
         PRAGMA foreign_keys = ON;
         PRAGMA wal_autocheckpoint = 0;
         PRAGMA busy_timeout = 5000;",
    )?;
    let synchronous: i64 = connection.query_row("PRAGMA synchronous", [], |row| row.get(0))?;
    if synchronous != 2 {
        return Err(crate::host::HostError::Corrupt(
            "SQLite synchronous mode is not FULL".into(),
        ));
    }
    Ok(())
}

fn create_schema(connection: &mut Connection, state: &AgentState) -> Result<()> {
    let transaction = connection.transaction()?;
    transaction.execute_batch(
        "CREATE TABLE agent_state (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            state_json TEXT NOT NULL
         );
         CREATE TABLE schema_migrations (
            version INTEGER PRIMARY KEY
         );
         CREATE TABLE replica_authority (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            authority_json TEXT NOT NULL
         );
         CREATE TABLE runtime_lifecycle (
            kind TEXT PRIMARY KEY,
            value_json TEXT NOT NULL
         );
         CREATE TABLE replication_progress (
            fence_json TEXT PRIMARY KEY,
            data_loss_number INTEGER NOT NULL,
            configuration_number INTEGER NOT NULL,
            current_configuration_id TEXT NOT NULL,
            verified_lsn INTEGER NOT NULL,
            progress_json TEXT NOT NULL
         );
         CREATE INDEX replication_progress_configuration
           ON replication_progress(
             data_loss_number, configuration_number, current_configuration_id, verified_lsn
           );
         CREATE TABLE local_writes (
            operation_id TEXT PRIMARY KEY,
            lsn INTEGER NOT NULL,
            phase INTEGER NOT NULL,
            write_json TEXT NOT NULL
         );
         CREATE TABLE build_authority (
            build_id TEXT PRIMARY KEY,
            authority_json TEXT NOT NULL
         );
         CREATE TABLE build_progress (
            build_id TEXT PRIMARY KEY REFERENCES build_authority(build_id),
            progress_json TEXT NOT NULL
         );",
    )?;
    transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    transaction.execute(
        "INSERT INTO schema_migrations(version) VALUES(?1)",
        [SCHEMA_VERSION],
    )?;
    write_agent_state(&transaction, state)?;
    transaction.commit()?;
    Ok(())
}

fn validate_integrity(connection: &Connection) -> Result<()> {
    let result: String = connection.query_row("PRAGMA quick_check(1)", [], |row| row.get(0))?;
    if result != "ok" {
        return Err(crate::host::HostError::Corrupt(result));
    }
    Ok(())
}

fn load_state_from_connection(connection: &Connection) -> Result<AgentState> {
    let json: String = connection.query_row(
        "SELECT state_json FROM agent_state WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?;
    Ok(serde_json::from_str(&json)?)
}

fn write_agent_state(transaction: &Transaction<'_>, state: &AgentState) -> Result<()> {
    let json = serde_json::to_string(state)?;
    transaction.execute(
        "INSERT INTO agent_state(singleton, state_json) VALUES(1, ?1)
         ON CONFLICT(singleton) DO UPDATE SET state_json = excluded.state_json",
        [json],
    )?;
    Ok(())
}

fn load_json_optional<T, P>(
    connection: &Connection,
    sql: &str,
    params: P,
) -> ContractResult<Option<T>>
where
    T: serde::de::DeserializeOwned,
    P: rusqlite::Params,
{
    let json: Option<String> = connection
        .query_row(sql, params, |row| row.get(0))
        .optional()
        .map_err(contract_sqlite_error)?;
    json.map(|value| contract_from_json(&value)).transpose()
}

fn contract_json<T: serde::Serialize>(value: &T) -> ContractResult<String> {
    serde_json::to_string(value).map_err(|error| ContractError::Persistence(error.to_string()))
}

fn write_lifecycle<T: serde::Serialize>(
    transaction: &Transaction<'_>,
    kind: &str,
    value: &T,
) -> ContractResult<()> {
    transaction
        .execute(
            "INSERT INTO runtime_lifecycle(kind, value_json) VALUES(?1, ?2)
         ON CONFLICT(kind) DO UPDATE SET value_json = excluded.value_json",
            params![kind, contract_json(value)?],
        )
        .map_err(contract_sqlite_error)?;
    Ok(())
}

fn contract_from_json<T: serde::de::DeserializeOwned>(value: &str) -> ContractResult<T> {
    serde_json::from_str(value).map_err(|error| ContractError::Persistence(error.to_string()))
}

fn contract_sqlite_error(error: rusqlite::Error) -> ContractError {
    ContractError::Persistence(error.to_string())
}

fn phase_tag(phase: LocalWritePhase) -> i64 {
    match phase {
        LocalWritePhase::Reserved => 0,
        LocalWritePhase::Registered => 1,
        LocalWritePhase::Committed => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::{
        AgentGeneration, EffectivePolicy, InitializationId, PodUid, PvcUid, ReplicaId,
        ReplicaIdentity, ReplicaInstanceId, ResourceUid,
    };

    fn identity() -> StorageIdentity {
        StorageIdentity {
            schema_version: SCHEMA_VERSION,
            resource_uid: ResourceUid::new("scale-up-set"),
            pod_uid: PodUid::new("scale-up-pod"),
            pvc_uid: PvcUid::new("scale-up-pvc"),
            initialization_id: InitializationId::new("scale-up-init"),
            local_identity: ReplicaIdentity {
                replica_id: ReplicaId::new(2),
                instance_id: ReplicaInstanceId::new("scale-up-pod"),
                agent_generation: AgentGeneration::new("scale-up-generation"),
            },
            effective_policy: EffectivePolicy::fixed(2, 30).unwrap(),
        }
    }

    #[test]
    fn application_binding_schema_five_rejects_schema_four_without_migration() {
        let directory = crate::host::tests::tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let expected = identity();
        drop(SqliteStore::create_authorized(&path, AgentState::new(expected.clone())).unwrap());
        let connection = Connection::open(&path).unwrap();
        connection.pragma_update(None, "user_version", 4).unwrap();
        drop(connection);
        assert!(matches!(
            SqliteStore::open_existing(&path, Some(&expected)),
            Err(crate::host::HostError::SchemaMismatch {
                expected: 5,
                observed: 4
            })
        ));
    }

    #[test]
    fn switchover_receipt_rejects_negative_or_changed_handoff() {
        let source = identity().local_identity;
        let target = ReplicaIdentity {
            replica_id: ReplicaId::new(3),
            instance_id: ReplicaInstanceId::new("target"),
            agent_generation: AgentGeneration::new("target-generation"),
        };
        let request_id = crate::protocol::types::SwitchoverRequestId::new("metadata-validation");
        let configuration_id = ConfigurationId::new("starting");
        let epoch = Epoch::new(0, 1);
        let mut receipt = crate::receipts::SwitchoverReceipt {
            token: crate::receipts::NativeOperationToken {
                authority: None,
                engine_session_id: "engine".into(),
                engine_generation: 3,
            },
            request_id: request_id.clone(),
            preparation_generation: 1,
            source: source.clone(),
            target: target.clone(),
            starting_configuration_id: configuration_id.clone(),
            starting_epoch: epoch,
            handoff_lsn: 7,
            committed_lsn: 7,
        };
        assert!(switchover_receipt_matches(
            &receipt,
            1,
            &request_id,
            &source,
            &target,
            &configuration_id,
            epoch,
        ));
        receipt.handoff_lsn = -1;
        assert!(!switchover_receipt_matches(
            &receipt,
            1,
            &request_id,
            &source,
            &target,
            &configuration_id,
            epoch,
        ));
        receipt.handoff_lsn = 7;
        receipt.request_id = crate::protocol::types::SwitchoverRequestId::new("changed");
        assert!(!switchover_receipt_matches(
            &receipt,
            1,
            &request_id,
            &source,
            &target,
            &configuration_id,
            epoch,
        ));
    }
}
