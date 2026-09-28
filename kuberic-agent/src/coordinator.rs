//! Durable replica-local reconfiguration coordinator.

use std::sync::Arc;
use tokio::sync::{Mutex, watch};

use kuberic_protocol::command::{
    EnsureConfiguration, EnsureReplicaBuild, PrepareSecondaryRemoval, PrepareSwitchover,
    RetireReplica,
};
use kuberic_protocol::types::{
    AccessStatus, Epoch, OperationId, ProcessSessionId, ReplicaRetirementReport, ReplicaRole,
    SecondaryRemovalPreparation, SwitchoverHandoff,
};
use kuberic_runtime::application::OpenMode;
use kuberic_runtime_internal::authority::RetiredAuthority;
use kuberic_runtime_internal::effects::{RuntimeEffect, RuntimeEffectAction, RuntimeEffectResult};

use crate::command::{
    admit_build, admit_configuration, admit_persisted_configuration, admit_switchover_preparation,
};
use crate::runtime_adapter::{RuntimeAdapter, RuntimeEffectExecutor};
use crate::state::{CoordinatorStage, ReconfigurationRecord, RetainedCommandResult};
use crate::store::{AgentStore, BeginConfiguration};
use crate::{AgentError, Result};

pub struct Coordinator<S, E> {
    store: Arc<S>,
    runtime: RuntimeAdapter<S, E>,
    command_lock: Mutex<()>,
    supersession_epoch: watch::Sender<Epoch>,
}

impl<S, E> Coordinator<S, E>
where
    S: AgentStore,
    E: RuntimeEffectExecutor,
{
    pub fn new(store: Arc<S>, executor: Arc<E>) -> Self {
        let (supersession_epoch, _) = watch::channel(Epoch::default());
        Self {
            runtime: RuntimeAdapter::new(store.clone(), executor),
            store,
            command_lock: Mutex::new(()),
            supersession_epoch,
        }
    }

    pub async fn open_runtime(
        &self,
        mode: OpenMode,
        process_session: &ProcessSessionId,
    ) -> Result<RuntimeEffectResult> {
        let state = self.store.load_state().await?;
        self.runtime
            .execute(RuntimeEffect {
                operation_id: OperationId::new(format!(
                    "{}:open:{}",
                    state.identity.initialization_id.as_str(),
                    process_session.as_str()
                )),
                sequence: state.next_effect_sequence,
                action: RuntimeEffectAction::Open(mode),
            })
            .await
    }

    pub async fn resume_pending(&self) -> Result<Option<RuntimeEffectResult>> {
        self.runtime.resume_pending().await
    }

    pub async fn resume_configuration(&self) -> Result<Option<RetainedCommandResult>> {
        let command = self
            .store
            .load_state()
            .await?
            .reconfiguration
            .map(|record| record.command);
        match command {
            Some(command) => self.ensure_configuration(command).await.map(Some),
            None => Ok(None),
        }
    }

    pub async fn ensure_secondary_removal_prepared(
        &self,
        command: PrepareSecondaryRemoval,
        process_session_id: ProcessSessionId,
        report_sequence: u64,
    ) -> Result<SecondaryRemovalPreparation> {
        let _command = self.command_lock.lock().await;
        if process_session_id.is_empty() || report_sequence == 0 {
            return Err(AgentError::CommandRejected(
                "preparation requires exact session and sequence".into(),
            ));
        }
        let state = self.store.load_state().await?;
        crate::removal::admit_preparation(&command, &state)?;
        let action = RuntimeEffectAction::PrepareSecondaryRemoval {
            intent: Box::new(command.intent.clone()),
            process_session_id,
            report_sequence,
        };
        let existing = state
            .pending_effect
            .as_ref()
            .map(|p| &p.effect)
            .or_else(|| {
                state
                    .removal_effects
                    .get(&command.operation_id)
                    .map(|r| &r.effect)
            });
        let effect = if let Some(existing) = existing {
            if existing.operation_id != command.operation_id
                || !matches!(&existing.action, RuntimeEffectAction::PrepareSecondaryRemoval { intent, .. } if intent.as_ref() == &command.intent)
            {
                return Err(AgentError::EffectConflict(
                    "preparation differs from persisted intent".into(),
                ));
            }
            existing.clone()
        } else {
            RuntimeEffect {
                operation_id: command.operation_id,
                sequence: state.next_effect_sequence,
                action,
            }
        };
        let result = self.runtime.execute(effect).await?;
        result
            .postcondition
            .prepared_secondary_removal
            .ok_or_else(|| {
                AgentError::EffectConflict("preparation omitted terminal evidence".into())
            })
    }

    pub async fn accept_secondary_removal_commit(
        &self,
        command: kuberic_protocol::command::AcceptSecondaryRemovalCommit,
    ) -> Result<()> {
        let _command = self.command_lock.lock().await;
        let state = self.store.load_state().await?;
        crate::removal::admit_commit(&command, &state)?;
        let action = if command.local_recovery {
            RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(Box::new(command.clone()))
        } else {
            RuntimeEffectAction::AcceptSecondaryRemovalCommit(Box::new(command.committed.clone()))
        };
        let effect = if let Some(pending) = &state.pending_effect {
            if pending.effect.operation_id != command.operation_id {
                return Err(AgentError::EffectConflict(
                    "commit conflicts with pending work".into(),
                ));
            }
            // SQLite checks exact replay or atomically narrows ordinary acceptance
            // to receipt-authorized local recovery without allocating a new effect.
            RuntimeEffect {
                action,
                ..pending.effect.clone()
            }
        } else if state.accepted_secondary_removal.as_ref() == Some(&command.committed) {
            return Ok(());
        } else {
            RuntimeEffect {
                operation_id: command.operation_id,
                sequence: state.next_effect_sequence,
                action,
            }
        };
        self.runtime.execute(effect).await?;
        Ok(())
    }

    pub async fn ensure_replica_retired(
        &self,
        command: RetireReplica,
        process_session_id: ProcessSessionId,
        report_sequence: u64,
    ) -> Result<ReplicaRetirementReport> {
        let _command = self.command_lock.lock().await;
        if process_session_id.is_empty() || report_sequence == 0 {
            return Err(AgentError::CommandRejected(
                "retirement requires exact session and sequence".into(),
            ));
        }
        let state = self.store.load_state().await?;
        crate::removal::admit_retirement(&command, &state)?;
        let intent = command.committed.evidence.preparation.intent.clone();
        let retired = RetiredAuthority {
            report: ReplicaRetirementReport {
                intent: intent.clone(),
                operation_id: command.operation_id.clone(),
                process_session_id,
                report_sequence,
                epoch: intent.current_configuration.epoch,
                role: ReplicaRole::None,
                read_status: AccessStatus::NotPrimary,
                write_status: AccessStatus::NotPrimary,
                application_closed: true,
                peers_fenced: true,
            },
            committed: command.committed.clone(),
        };
        let existing = state
            .pending_effect
            .as_ref()
            .map(|p| &p.effect)
            .or_else(|| {
                state
                    .removal_effects
                    .get(&command.operation_id)
                    .map(|r| &r.effect)
            });
        let effect = if let Some(existing) = existing {
            if existing.operation_id != command.operation_id
                || !matches!(&existing.action, RuntimeEffectAction::RetireReplica(r) if r.committed == command.committed)
            {
                return Err(AgentError::EffectConflict(
                    "retirement differs from persisted intent".into(),
                ));
            }
            existing.clone()
        } else {
            retired
                .validate(&state.identity.local_identity)
                .map_err(|e| AgentError::CommandRejected(e.to_string()))?;
            RuntimeEffect {
                operation_id: command.operation_id,
                sequence: state.next_effect_sequence,
                action: RuntimeEffectAction::RetireReplica(Box::new(retired)),
            }
        };
        let result = self.runtime.execute(effect).await?;
        result
            .postcondition
            .retired_authority
            .map(|r| r.report)
            .ok_or_else(|| {
                AgentError::EffectConflict("retirement omitted terminal evidence".into())
            })
    }
    pub async fn ensure_switchover_prepared(
        &self,
        command: PrepareSwitchover,
    ) -> Result<SwitchoverHandoff> {
        let _command = self.command_lock.lock().await;
        let state = self.store.load_state().await?;
        admit_switchover_preparation(&command, &state)?;
        if let Some(prepared) = state.prepared_switchover {
            return Ok(prepared);
        }
        let action = RuntimeEffectAction::PrepareSwitchover {
            preparation_generation: command.preparation_generation,
            request_id: command.request_id.clone(),
            source: command.source.clone(),
            target: command.target.clone(),
            starting_configuration_id: command.current_configuration.configuration_id.clone(),
            starting_epoch: command.current_configuration.epoch,
        };
        let effect = if let Some(pending) = state.pending_effect {
            if pending.effect.operation_id != command.operation_id
                || pending.effect.action != action
            {
                return Err(AgentError::EffectConflict(
                    "another durable effect is pending".into(),
                ));
            }
            pending.effect
        } else if let Some(retained) = state.retained_result {
            if retained.operation_id == command.operation_id {
                if retained.effect.action != action {
                    return Err(AgentError::EffectConflict(
                        "operation ID was reused with another switchover preparation".into(),
                    ));
                }
                retained.effect
            } else {
                RuntimeEffect {
                    operation_id: command.operation_id,
                    sequence: state.next_effect_sequence,
                    action,
                }
            }
        } else {
            RuntimeEffect {
                operation_id: command.operation_id,
                sequence: state.next_effect_sequence,
                action,
            }
        };
        self.runtime.execute(effect).await?;
        self.store
            .load_state()
            .await?
            .prepared_switchover
            .ok_or_else(|| {
                AgentError::EffectConflict(
                    "switchover preparation completed without durable handoff evidence".into(),
                )
            })
    }

    pub async fn ensure_configuration(
        &self,
        command: EnsureConfiguration,
    ) -> Result<RetainedCommandResult> {
        // Full frozen evidence makes this state machine too large for caller task stacks.
        Box::pin(self.drive_configuration(command)).await
    }

    async fn drive_configuration(
        &self,
        command: EnsureConfiguration,
    ) -> Result<RetainedCommandResult> {
        let observed = self.store.load_state().await?;
        if let Some(retained) = observed.removal_commands.get(&command.operation_id) {
            if retained.command != command
                || command.current_epoch < observed.highest_epoch
                || observed.retired_authority.is_some()
            {
                return Err(AgentError::EffectConflict(
                    "stale or mutated removal command replay".into(),
                ));
            }
            return Ok(retained.clone());
        }
        if let Some(pending) = observed.reconfiguration.as_ref()
            && pending.command != command
        {
            admit_configuration(&command, &observed)?;
            if command.current_epoch <= pending.command.current_epoch {
                return Err(crate::AgentError::EffectConflict(
                    "a same-or-newer configuration command is pending".into(),
                ));
            }
            self.supersession_epoch.send_if_modified(|epoch| {
                if command.current_epoch > *epoch {
                    *epoch = command.current_epoch;
                    true
                } else {
                    false
                }
            });
            self.runtime.cancel_configuration_work().await?;
        }
        let _command = self.command_lock.lock().await;
        if self.is_superseded(&command) {
            return Err(crate::AgentError::Runtime(
                kuberic_runtime::RuntimeError::OperationCancelled,
            ));
        }
        let durable = self.store.load_state().await?;
        let persisted_exact = durable
            .reconfiguration
            .as_ref()
            .is_some_and(|record| record.command.operation_id == command.operation_id)
            || durable
                .retained_command
                .as_ref()
                .is_some_and(|retained| retained.command.operation_id == command.operation_id);
        let authority = if persisted_exact {
            admit_persisted_configuration(&command, &durable)?
        } else {
            admit_configuration(&command, &durable)?
        };
        let preserve_same_primary_scale_up =
            preserves_same_primary_scale_up_access(&durable, &authority, &command);
        match self.store.begin_configuration(&command).await? {
            BeginConfiguration::Completed(result) => return Ok(result),
            BeginConfiguration::Execute(_)
            | BeginConfiguration::Pending(_)
            | BeginConfiguration::Superseded(_) => {}
        }

        loop {
            let state = self.store.load_state().await?;
            let record = if let Some(record) = state.reconfiguration.clone() {
                record
            } else if let Some(retained) = state.retained_command
                && retained.command.operation_id == command.operation_id
            {
                return Ok(retained);
            } else {
                return Err(crate::AgentError::EffectConflict(
                    "configuration ownership changed while the command was executing".into(),
                ));
            };
            if self.is_superseded(&record.command) {
                return Err(crate::AgentError::Runtime(
                    kuberic_runtime::RuntimeError::OperationCancelled,
                ));
            }
            match record.stage {
                CoordinatorStage::AdmitAuthority => {
                    self.execute(
                        &record,
                        "admit-authority",
                        RuntimeEffectAction::AdmitAuthority(Box::new(authority.clone())),
                    )
                    .await?;
                    let next = if preserve_same_primary_scale_up {
                        CoordinatorStage::Activate
                    } else if record.command.transition_kind
                        == kuberic_protocol::types::TransitionKind::Failover
                        && record.command.failover_safe_lsn.is_some()
                    {
                        CoordinatorStage::FailoverPrefix
                    } else {
                        CoordinatorStage::Demote
                    };
                    self.advance(&record, next, None).await?;
                }
                CoordinatorStage::FailoverPrefix => {
                    self.execute(
                        &record,
                        "failover-prefix",
                        RuntimeEffectAction::AuthorizeFailoverPrefix(
                            record
                                .command
                                .failover_safe_lsn
                                .expect("admitted failover command has a safe LSN"),
                        ),
                    )
                    .await?;
                    self.advance(&record, CoordinatorStage::Demote, None)
                        .await?;
                }
                CoordinatorStage::Demote => {
                    self.execute(
                        &record,
                        "demote",
                        RuntimeEffectAction::SetReadStatus(AccessStatus::ReconfigurationPending),
                    )
                    .await?;
                    let next = if record.command.transition_kind
                        == kuberic_protocol::types::TransitionKind::Failover
                    {
                        CoordinatorStage::ReplicatorRole
                    } else {
                        CoordinatorStage::GetLsn
                    };
                    self.advance(&record, next, None).await?;
                }
                CoordinatorStage::GetLsn => {
                    let result = self
                        .execute(
                            &record,
                            "get-lsn",
                            RuntimeEffectAction::RefreshApplicationProgress,
                        )
                        .await?;
                    let next = if record.command.transition_kind
                        == kuberic_protocol::types::TransitionKind::Failover
                    {
                        if authority.local_role() == ReplicaRole::Primary
                            && (record.command.primary_write_status == AccessStatus::Granted
                                || (record.command.failover_safe_lsn.is_some()
                                    && authority.scale_up.is_some()))
                        {
                            CoordinatorStage::Catchup
                        } else {
                            CoordinatorStage::Deactivate
                        }
                    } else if record.command.transition_kind
                        != kuberic_protocol::types::TransitionKind::PlannedSwitchover
                        && state.role == ReplicaRole::Primary
                        && authority.local_role() != ReplicaRole::Primary
                    {
                        CoordinatorStage::Catchup
                    } else {
                        CoordinatorStage::Deactivate
                    };
                    self.advance(&record, next, Some(result.postcondition.current_progress))
                        .await?;
                }
                CoordinatorStage::Catchup => {
                    self.execute(&record, "catchup", RuntimeEffectAction::WaitForCatchup)
                        .await?;
                    let next = if record.command.transition_kind
                        == kuberic_protocol::types::TransitionKind::Failover
                    {
                        CoordinatorStage::Deactivate
                    } else if authority.local_role() == ReplicaRole::Primary {
                        CoordinatorStage::Activate
                    } else {
                        CoordinatorStage::Deactivate
                    };
                    self.advance(&record, next, None).await?;
                }
                CoordinatorStage::Deactivate => {
                    self.execute(
                        &record,
                        "deactivate",
                        RuntimeEffectAction::SetWriteStatus(AccessStatus::ReconfigurationPending),
                    )
                    .await?;
                    let next = if record.command.transition_kind
                        == kuberic_protocol::types::TransitionKind::Failover
                    {
                        CoordinatorStage::Activate
                    } else {
                        CoordinatorStage::ReplicatorRole
                    };
                    self.advance(&record, next, None).await?;
                }
                CoordinatorStage::ReplicatorRole => {
                    self.execute(
                        &record,
                        "replicator-role",
                        RuntimeEffectAction::ChangeReplicatorRole(authority.local_role()),
                    )
                    .await?;
                    let next = if authority.local_role() == ReplicaRole::Primary {
                        CoordinatorStage::Epoch
                    } else {
                        CoordinatorStage::ApplicationRole
                    };
                    self.advance(&record, next, None).await?;
                }
                CoordinatorStage::Epoch => {
                    self.execute(&record, "epoch", RuntimeEffectAction::UpdateEpoch)
                        .await?;
                    self.advance(&record, CoordinatorStage::ApplicationRole, None)
                        .await?;
                }
                CoordinatorStage::ApplicationRole => {
                    self.execute(
                        &record,
                        "application-role",
                        RuntimeEffectAction::ChangeApplicationRole(authority.local_role()),
                    )
                    .await?;
                    let next = if record.command.transition_kind
                        == kuberic_protocol::types::TransitionKind::Failover
                    {
                        CoordinatorStage::GetLsn
                    } else if authority.local_role() == ReplicaRole::Primary
                        && (record.command.primary_write_status == AccessStatus::Granted
                            || record.command.transition_kind
                                == kuberic_protocol::types::TransitionKind::PlannedSwitchover)
                    {
                        CoordinatorStage::Catchup
                    } else {
                        CoordinatorStage::Activate
                    };
                    self.advance(&record, next, None).await?;
                }
                CoordinatorStage::Activate => {
                    let provisional_primary = authority.local_role() == ReplicaRole::Primary
                        && matches!(
                            record.command.transition_kind,
                            kuberic_protocol::types::TransitionKind::Failover
                                | kuberic_protocol::types::TransitionKind::PlannedSwitchover
                                | kuberic_protocol::types::TransitionKind::SecondaryScaleDown
                        )
                        && record.command.primary_write_status != AccessStatus::Granted;
                    let read_status = if provisional_primary {
                        AccessStatus::ReconfigurationPending
                    } else if matches!(
                        authority.local_role(),
                        ReplicaRole::Primary | ReplicaRole::ActiveSecondary
                    ) {
                        AccessStatus::Granted
                    } else {
                        AccessStatus::NotPrimary
                    };
                    let write_status = if authority.local_role() == ReplicaRole::Primary {
                        record.command.primary_write_status
                    } else {
                        AccessStatus::NotPrimary
                    };
                    self.execute(
                        &record,
                        "activate",
                        RuntimeEffectAction::SetAccessStatus {
                            read: read_status,
                            write: write_status,
                        },
                    )
                    .await?;
                    let next = if !record.command.retire_build_ids.is_empty()
                        || !record.command.retire_switchover_preparation_ids.is_empty()
                    {
                        CoordinatorStage::RetireBuild
                    } else {
                        CoordinatorStage::Complete
                    };
                    self.advance(&record, next, None).await?;
                }
                CoordinatorStage::RetireBuild => {
                    for (index, build_id) in record.command.retire_build_ids.iter().enumerate() {
                        self.execute(
                            &record,
                            &format!("retire-build-{index}"),
                            RuntimeEffectAction::RetireBuild(build_id.clone()),
                        )
                        .await?;
                    }
                    self.advance(&record, CoordinatorStage::Complete, None)
                        .await?;
                }
                CoordinatorStage::Complete => {
                    return self
                        .store
                        .complete_configuration(&record.command.operation_id)
                        .await;
                }
            }
        }
    }

    fn is_superseded(&self, command: &EnsureConfiguration) -> bool {
        *self.supersession_epoch.borrow() > command.current_epoch
    }

    pub async fn ensure_build(&self, command: EnsureReplicaBuild) -> Result<()> {
        let state = self.store.load_state().await?;
        admit_build(&command, &state)?;
        if command.retire {
            if state.retired_builds.contains(&command.operation_id) {
                return Ok(());
            }
            self.store.abandon_build(&command).await?;
            self.runtime.cancel_build(&command.operation_id).await?;
            let _command = self.command_lock.lock().await;
            self.runtime
                .settle_abandoned_build(&command.operation_id)
                .await?;
            let state = self.store.load_state().await?;
            if state.retired_builds.contains(&command.operation_id) {
                return Ok(());
            }
            self.execute_standalone(
                &command.operation_id,
                "retire-abandoned-build",
                RuntimeEffectAction::RetireBuild(command.operation_id.clone()),
            )
            .await?;
            return Ok(());
        }
        let _command = self.command_lock.lock().await;
        let state = self.store.load_state().await?;
        admit_build(&command, &state)?;
        let command = self.store.journal_build(&command).await?;
        if let Some(authority) = command.authority.clone() {
            if state.current_configuration.as_ref().is_some_and(|current| {
                current.epoch > authority.current_configuration.epoch
                    && current
                        .members
                        .iter()
                        .any(|member| member.identity == state.identity.local_identity)
            }) {
                return Ok(());
            }
            if state.retained_result.as_ref().is_some_and(|retained| {
                matches!(
                    &retained.effect.action,
                    RuntimeEffectAction::AdmitBuildAuthority(existing)
                        if existing.as_ref() == &authority
                )
            }) {
                return Ok(());
            }
            self.execute_standalone(
                &command.operation_id,
                "build-idle-replicator",
                RuntimeEffectAction::ChangeReplicatorRole(ReplicaRole::IdleSecondary),
            )
            .await?;
            self.execute_standalone(
                &command.operation_id,
                "build-idle-application",
                RuntimeEffectAction::ChangeApplicationRole(ReplicaRole::IdleSecondary),
            )
            .await?;
            self.execute_standalone(
                &command.operation_id,
                "admit-build",
                RuntimeEffectAction::AdmitBuildAuthority(Box::new(authority)),
            )
            .await?;
        } else {
            self.execute_standalone(
                &command.operation_id,
                "build-replica",
                RuntimeEffectAction::BuildReplica {
                    build_id: command.operation_id.clone(),
                    target: command.target,
                    replication_address: String::new(),
                },
            )
            .await?;
        }
        Ok(())
    }

    async fn execute(
        &self,
        record: &ReconfigurationRecord,
        stage: &str,
        action: RuntimeEffectAction,
    ) -> Result<RuntimeEffectResult> {
        let state = self.store.load_state().await?;
        let operation_id = stage_operation_id(&record.command.operation_id, stage);
        if let Some(retained) = state.retained_result.as_ref()
            && retained.operation_id == operation_id
        {
            if retained.effect.action != action {
                return Err(crate::AgentError::EffectConflict(
                    "retained coordinator effect has different stage authority".into(),
                ));
            }
            return self.runtime.execute(retained.effect.clone()).await;
        }
        self.runtime
            .execute(RuntimeEffect {
                operation_id,
                sequence: state.next_effect_sequence,
                action,
            })
            .await
    }

    async fn advance(
        &self,
        record: &ReconfigurationRecord,
        next: CoordinatorStage,
        observed_lsn: Option<i64>,
    ) -> Result<ReconfigurationRecord> {
        self.store
            .advance_configuration(
                &record.command.operation_id,
                record.stage,
                next,
                observed_lsn,
            )
            .await
    }

    async fn execute_standalone(
        &self,
        command: &OperationId,
        stage: &str,
        action: RuntimeEffectAction,
    ) -> Result<RuntimeEffectResult> {
        let state = self.store.load_state().await?;
        let operation_id = stage_operation_id(command, stage);
        if let Some(retained) = state.retained_result.as_ref()
            && retained.operation_id == operation_id
        {
            if retained.effect.action != action {
                return Err(crate::AgentError::EffectConflict(
                    "retained build effect has different stage authority".into(),
                ));
            }
            return self.runtime.execute(retained.effect.clone()).await;
        }
        self.runtime
            .execute(RuntimeEffect {
                operation_id,
                sequence: state.next_effect_sequence,
                action,
            })
            .await
    }
}

fn preserves_same_primary_scale_up_access(
    state: &crate::state::AgentState,
    authority: &kuberic_runtime_internal::authority::AdmittedAuthority,
    command: &EnsureConfiguration,
) -> bool {
    matches!(
        authority.scale_up.as_deref(),
        Some(kuberic_protocol::types::ScaleUpConfigurationEvidence::Admission { .. })
    ) && state.role == ReplicaRole::Primary
        && state.read_status == AccessStatus::Granted
        && state.write_status == AccessStatus::Granted
        && authority.local_role() == ReplicaRole::Primary
        && authority.primary_identity() == &state.identity.local_identity
        && command.primary_write_status == AccessStatus::Granted
}

fn stage_operation_id(command: &OperationId, stage: &str) -> OperationId {
    OperationId::new(format!("{}:{stage}", command.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{AgentState, SCHEMA_VERSION, StorageIdentity};
    use kuberic_protocol::types::{
        AgentGeneration, ConfigurationDescriptor, ConfigurationMember, EffectivePolicy,
        InitializationId, PodUid, PvcUid, ReplicaId, ReplicaIdentity, ReplicaInstanceId,
        ResourceUid, ScaleUpConfigurationEvidence, ScaleUpIntent, TransitionKind,
    };

    fn replica(id: i64) -> ReplicaIdentity {
        ReplicaIdentity {
            replica_id: ReplicaId::new(id),
            instance_id: ReplicaInstanceId::new(format!("pod-{id}")),
            agent_generation: AgentGeneration::new(format!("generation-{id}")),
        }
    }

    #[test]
    fn scale_up_same_primary_path_requires_durable_granted_access() {
        let primary = replica(1);
        let target = replica(2);
        let previous_policy = EffectivePolicy::fixed(1, 30).unwrap();
        let current_policy = EffectivePolicy::fixed(2, 30).unwrap();
        let previous = ConfigurationDescriptor::new(
            Epoch::new(0, 1),
            primary.replica_id,
            vec![ConfigurationMember {
                identity: primary.clone(),
                role: ReplicaRole::Primary,
            }],
            previous_policy.write_quorum,
        );
        let current = ConfigurationDescriptor::new(
            Epoch::new(0, 2),
            primary.replica_id,
            vec![
                ConfigurationMember {
                    identity: primary.clone(),
                    role: ReplicaRole::Primary,
                },
                ConfigurationMember {
                    identity: target.clone(),
                    role: ReplicaRole::ActiveSecondary,
                },
            ],
            current_policy.write_quorum,
        );
        let mut intent = ScaleUpIntent {
            operation_id: OperationId::default(),
            resource_uid: ResourceUid::new("set"),
            spec_generation: 2,
            desired_replicas: 2,
            previous_configuration: previous.clone(),
            current_configuration: current.clone(),
            previous_policy: previous_policy.clone(),
            current_policy: current_policy.clone(),
            primary: primary.clone(),
            target,
            build_id: OperationId::new("build"),
            snapshot_boundary_lsn: 0,
            catch_up_boundary_lsn: 0,
        };
        intent.operation_id = intent.expected_operation_id();
        let evidence = ScaleUpConfigurationEvidence::Admission { intent };
        let mut state = AgentState::new(StorageIdentity {
            schema_version: SCHEMA_VERSION,
            resource_uid: ResourceUid::new("set"),
            pod_uid: PodUid::new("pod-1"),
            pvc_uid: PvcUid::new("pvc-1"),
            initialization_id: InitializationId::new("init-1"),
            local_identity: primary.clone(),
            effective_policy: previous_policy.clone(),
        });
        state.role = ReplicaRole::Primary;
        state.read_status = AccessStatus::Granted;
        state.write_status = AccessStatus::Granted;
        let authority = kuberic_runtime_internal::authority::AdmittedAuthority {
            local_identity: primary.clone(),
            transition_kind: Some(TransitionKind::ScaleUp),
            previous_configuration: Some(previous.clone()),
            current_configuration: current.clone(),
            switchover_handoff: None,
            secondary_removal: None,
            scale_up: Some(Box::new(evidence.clone())),
        };
        let command = EnsureConfiguration {
            operation_id: OperationId::new("command"),
            previous_configuration: Some(previous.clone()),
            current_configuration: current.clone(),
            previous_epoch: Some(previous.epoch),
            current_epoch: current.epoch,
            effective_policy: current_policy,
            previous_policy: Some(previous_policy),
            secondary_removal_evidence: None,
            scale_up_evidence: Some(Box::new(evidence)),
            local_replica_id: primary.replica_id,
            expected_instance_id: primary.instance_id,
            expected_agent_generation: primary.agent_generation,
            transition_kind: TransitionKind::ScaleUp,
            failover_safe_lsn: None,
            primary_write_status: AccessStatus::Granted,
            current_only: false,
            retire_build_ids: Vec::new(),
            switchover_handoff: None,
            retire_switchover_preparation_ids: Vec::new(),
        };
        assert!(preserves_same_primary_scale_up_access(
            &state, &authority, &command
        ));
        state.write_status = AccessStatus::NoWriteQuorum;
        assert!(!preserves_same_primary_scale_up_access(
            &state, &authority, &command
        ));
    }
}
