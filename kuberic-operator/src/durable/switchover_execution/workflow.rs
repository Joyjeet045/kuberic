use async_trait::async_trait;
use kuberic_durable_execution::{
    ActivityInvocationError, ActivityOptions, BoundedEffectError, EffectErrorKind, ExactBytes,
    TerminalOutcome, Workflow, WorkflowContext,
};
use serde::{Deserialize, Serialize};

use crate::crd::StablePartitionSnapshotStatus;

use super::SwitchoverWorkflowInput;
use super::activities::{
    AttestCompensatedTopologyActivity, AttestCompensatedTopologyInput,
    AttestCompensatedTopologyOutput, AttestTargetTopologyActivity, AttestTargetTopologyInput,
    AttestTargetTopologyOutput, CaptureFrozenLsnActivity, CaptureFrozenLsnInput,
    CaptureFrozenLsnOutput, CompensateDistributeReplicaEpochActivity,
    CompensateDistributeReplicaEpochInput, CompensatePromoteOldPrimaryActivity,
    CompensatePromoteOldPrimaryInput, DIRECT_SWITCHOVER_CONTRACT_VERSION, DemoteOldPrimaryActivity,
    DemoteOldPrimaryInput, DistributeReplicaEpochActivity, DistributeReplicaEpochInput,
    InstallCompensationCatchUpConfigurationActivity, InstallCompensationCatchUpConfigurationInput,
    InstallCompensationCurrentConfigurationActivity, InstallCompensationCurrentConfigurationInput,
    InstallTargetCatchUpConfigurationActivity, InstallTargetCatchUpConfigurationInput,
    InstallTargetCurrentConfigurationActivity, InstallTargetCurrentConfigurationInput,
    OrdinarySwitchoverActivity, PromoteTargetActivity, PromoteTargetInput,
    PublishOldPrimarySecondaryLabelActivity, PublishOldPrimarySecondaryLabelInput,
    PublishTargetPrimaryLabelActivity, PublishTargetPrimaryLabelInput,
    RestoreOldPrimaryLabelActivity, RestoreOldPrimaryLabelInput,
    RestorePreviousCurrentConfigurationActivity, RestorePreviousCurrentConfigurationInput,
    RestoreTargetSecondaryLabelActivity, RestoreTargetSecondaryLabelInput, RevokeWritesActivity,
    RevokeWritesInput, SwitchoverActivityContract, SwitchoverActivityInput,
    WaitTargetCaughtUpActivity, WaitTargetCaughtUpInput, WaitTargetWriteQuorumActivity,
    WaitTargetWriteQuorumInput,
};
use super::model::{DirectSwitchoverDefinition, next_deadline};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectSwitchoverTerminalBranch {
    TargetSuccess,
    RevokeSafeFailure,
    PreviousConfigurationRestored,
    PostPromotionCompensated,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum DirectSwitchoverTerminalRecord {
    Complete {
        snapshot: StablePartitionSnapshotStatus,
        compensated: bool,
        branch: DirectSwitchoverTerminalBranch,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[serde(with = "super::activities::bounded_optional_error")]
        reason: Option<String>,
    },
    Stopped {
        #[serde(with = "super::activities::bounded_error")]
        message: String,
    },
}

struct TransitionBudget {
    consumed: usize,
}

impl TransitionBudget {
    const fn new() -> Self {
        Self { consumed: 0 }
    }

    fn consume(&mut self, activity_name: &str) -> Result<(), String> {
        if self.consumed >= super::SWITCHOVER_MAX_TRANSITION_FUEL {
            return Err(format!(
                "direct switchover exhausted transition fuel before {activity_name}"
            ));
        }
        self.consumed += 1;
        Ok(())
    }
}

pub struct DirectSwitchoverWorkflow;

#[async_trait]
impl Workflow for DirectSwitchoverWorkflow {
    async fn run(&self, context: &mut WorkflowContext<'_>, input: ExactBytes) -> TerminalOutcome {
        let input: SwitchoverWorkflowInput = match serde_json::from_slice(input.as_slice()) {
            Ok(input) => input,
            Err(error) => return stopped(format!("decode direct switchover input: {error}")),
        };
        if input.version != DIRECT_SWITCHOVER_CONTRACT_VERSION {
            return stopped(format!(
                "unsupported direct switchover workflow version {}",
                input.version
            ));
        }
        if input.execution_id != super::encode_execution_id(context.execution_id()) {
            return stopped("direct switchover execution identity mismatch".to_string());
        }
        let definition = match DirectSwitchoverDefinition::from_initial(&input.initial_operation) {
            Ok(definition) => definition,
            Err(error) => return stopped(error),
        };
        let mut budget = TransitionBudget::new();
        let mut deadline = definition.initial_deadline_unix_seconds;
        let old_primary = match definition.member(definition.old_primary_id) {
            Ok(member) => member,
            Err(error) => return stopped(error),
        };

        let revoke = call_activity_branch::<RevokeWritesActivity>(
            context,
            &mut budget,
            deadline,
            RevokeWritesInput {
                old_primary_id: definition.old_primary_id,
                old_primary_instance_id: old_primary.instance_id.clone(),
            },
        )
        .await;
        deadline = match revoke {
            Ok(revoke) => match next_deadline(revoke.observed_at_unix_seconds) {
                Ok(deadline) => deadline,
                Err(error) => return stopped(error),
            },
            Err(EffectBranch::DomainFailure {
                observed_at,
                message,
            }) => {
                return attest_compensated(
                    context,
                    &mut budget,
                    &definition,
                    definition.previous_snapshot.clone(),
                    observed_at,
                    message,
                    DirectSwitchoverTerminalBranch::RevokeSafeFailure,
                )
                .await;
            }
            Err(EffectBranch::Stopped(message)) => return stopped(message),
        };

        let captured = match call_activity::<CaptureFrozenLsnActivity>(
            context,
            &mut budget,
            deadline,
            CaptureFrozenLsnInput {
                old_primary_id: definition.old_primary_id,
                old_primary_instance_id: old_primary.instance_id.clone(),
                expected_epoch: definition.previous_snapshot.epoch.clone(),
            },
        )
        .await
        {
            Ok(result) => result,
            Err(error) => return stopped(format!("capture frozen LSN activity failed: {error}")),
        };
        let CaptureFrozenLsnOutput {
            frozen_lsn,
            observed_at_unix_seconds: observed_at,
        } = captured;
        deadline = match next_deadline(observed_at) {
            Ok(deadline) => deadline,
            Err(error) => return stopped(error),
        };

        let target = match definition.member(definition.target_primary_id) {
            Ok(member) => member,
            Err(error) => return stopped(error),
        };
        let caught_up = match call_activity_branch::<WaitTargetCaughtUpActivity>(
            context,
            &mut budget,
            deadline,
            WaitTargetCaughtUpInput {
                target_id: definition.target_primary_id,
                target_instance_id: target.instance_id.clone(),
                expected_epoch: definition.previous_snapshot.epoch.clone(),
                frozen_lsn,
            },
        )
        .await
        {
            Ok(result) => result,
            Err(EffectBranch::DomainFailure {
                observed_at,
                message,
            }) => {
                return restore_previous_configuration(
                    context,
                    &mut budget,
                    &definition,
                    observed_at,
                    message,
                )
                .await;
            }
            Err(EffectBranch::Stopped(message)) => return stopped(message),
        };
        deadline = match next_deadline(caught_up.observed_at_unix_seconds) {
            Ok(deadline) => deadline,
            Err(error) => return stopped(error),
        };

        let demote = call_activity_branch::<DemoteOldPrimaryActivity>(
            context,
            &mut budget,
            deadline,
            DemoteOldPrimaryInput {
                old_primary_id: definition.old_primary_id,
                old_primary_instance_id: old_primary.instance_id.clone(),
            },
        )
        .await;
        deadline = match demote {
            Ok(demote) => match next_deadline(demote.observed_at_unix_seconds) {
                Ok(deadline) => deadline,
                Err(error) => return stopped(error),
            },
            Err(EffectBranch::DomainFailure {
                observed_at,
                message,
            }) => {
                return restore_previous_configuration(
                    context,
                    &mut budget,
                    &definition,
                    observed_at,
                    message,
                )
                .await;
            }
            Err(EffectBranch::Stopped(message)) => return stopped(message),
        };

        let promote = call_activity_branch::<PromoteTargetActivity>(
            context,
            &mut budget,
            deadline,
            PromoteTargetInput {
                target_primary_id: definition.target_primary_id,
                target_primary_instance_id: target.instance_id.clone(),
            },
        )
        .await;
        deadline = match promote {
            Ok(promote) => match next_deadline(promote.observed_at_unix_seconds) {
                Ok(deadline) => deadline,
                Err(error) => return stopped(error),
            },
            Err(EffectBranch::DomainFailure {
                observed_at,
                message,
            }) => {
                return compensate_after_promotion_failure(
                    context,
                    &mut budget,
                    &definition,
                    observed_at,
                    message,
                )
                .await;
            }
            Err(EffectBranch::Stopped(message)) => return stopped(message),
        };

        for (index, replica_id) in definition
            .normal_epoch_distribution_ids()
            .into_iter()
            .enumerate()
        {
            let distribution_index = match u8::try_from(index) {
                Ok(index) => index,
                Err(_) => return stopped("normal epoch distribution index overflow".to_string()),
            };
            let replica = match definition.member(replica_id) {
                Ok(member) => member,
                Err(error) => return stopped(error),
            };
            let result = match call_activity::<DistributeReplicaEpochActivity>(
                context,
                &mut budget,
                deadline,
                DistributeReplicaEpochInput {
                    distribution_index,
                    replica_id,
                    replica_instance_id: replica.instance_id.clone(),
                },
            )
            .await
            {
                Ok(result) => result,
                Err(error) => return stopped(error),
            };
            deadline = match next_deadline(result.observed_at_unix_seconds) {
                Ok(deadline) => deadline,
                Err(message) => return stopped(message),
            };
        }

        macro_rules! run_target_replica_step {
            ($activity:ty, $input:expr) => {{
                let result = match call_activity::<$activity>(
                    context,
                    &mut budget,
                    deadline,
                    $input,
                )
                .await
                {
                    Ok(result) => result,
                    Err(error) => return stopped(error),
                };
                deadline = match next_deadline(result.observed_at_unix_seconds) {
                    Ok(deadline) => deadline,
                    Err(message) => return stopped(message),
                };
            }};
        }

        run_target_replica_step!(
            InstallTargetCatchUpConfigurationActivity,
            InstallTargetCatchUpConfigurationInput {
                target_primary_id: definition.target_primary_id,
                target_primary_instance_id: target.instance_id.clone(),
            }
        );
        run_target_replica_step!(
            WaitTargetWriteQuorumActivity,
            WaitTargetWriteQuorumInput {
                target_primary_id: definition.target_primary_id,
                target_primary_instance_id: target.instance_id.clone(),
            }
        );
        run_target_replica_step!(
            InstallTargetCurrentConfigurationActivity,
            InstallTargetCurrentConfigurationInput {
                target_primary_id: definition.target_primary_id,
                target_primary_instance_id: target.instance_id.clone(),
            }
        );

        let target_label = match call_activity::<PublishTargetPrimaryLabelActivity>(
            context,
            &mut budget,
            deadline,
            PublishTargetPrimaryLabelInput {
                target_primary_id: definition.target_primary_id,
                target_primary_instance_id: target.instance_id.clone(),
            },
        )
        .await
        {
            Ok(result) => result,
            Err(error) => return stopped(error),
        };
        deadline = match next_deadline(target_label.observed_at_unix_seconds) {
            Ok(deadline) => deadline,
            Err(message) => return stopped(message),
        };

        let old_label = match call_activity::<PublishOldPrimarySecondaryLabelActivity>(
            context,
            &mut budget,
            deadline,
            PublishOldPrimarySecondaryLabelInput {
                old_primary_id: definition.old_primary_id,
                old_primary_instance_id: old_primary.instance_id.clone(),
            },
        )
        .await
        {
            Ok(result) => result,
            Err(error) => return stopped(error),
        };
        deadline = match next_deadline(old_label.observed_at_unix_seconds) {
            Ok(deadline) => deadline,
            Err(message) => return stopped(message),
        };

        match call_activity::<AttestTargetTopologyActivity>(
            context,
            &mut budget,
            deadline,
            AttestTargetTopologyInput {
                expected_snapshot: definition.target_snapshot.clone(),
            },
        )
        .await
        {
            Ok(AttestTargetTopologyOutput { snapshot, .. }) => complete(
                snapshot,
                false,
                DirectSwitchoverTerminalBranch::TargetSuccess,
                None,
            ),
            Err(error) => stopped(format!("target topology attestation failed: {error}")),
        }
    }
}

async fn restore_previous_configuration(
    context: &mut WorkflowContext<'_>,
    budget: &mut TransitionBudget,
    definition: &DirectSwitchoverDefinition,
    observed_at_unix_seconds: i64,
    reason: String,
) -> TerminalOutcome {
    let deadline = match next_deadline(observed_at_unix_seconds) {
        Ok(deadline) => deadline,
        Err(error) => return stopped(error),
    };
    let old_primary = match definition.member(definition.old_primary_id) {
        Ok(member) => member,
        Err(error) => return stopped(error),
    };
    let restore = call_activity_branch::<RestorePreviousCurrentConfigurationActivity>(
        context,
        budget,
        deadline,
        RestorePreviousCurrentConfigurationInput {
            old_primary_id: definition.old_primary_id,
            old_primary_instance_id: old_primary.instance_id.clone(),
        },
    )
    .await;
    let observed_at = match restore {
        Ok(restore) => restore.observed_at_unix_seconds,
        Err(EffectBranch::DomainFailure { message, .. }) | Err(EffectBranch::Stopped(message)) => {
            return stopped(message);
        }
    };
    attest_compensated(
        context,
        budget,
        definition,
        definition.previous_snapshot.clone(),
        observed_at,
        reason,
        DirectSwitchoverTerminalBranch::PreviousConfigurationRestored,
    )
    .await
}

async fn compensate_after_promotion_failure(
    context: &mut WorkflowContext<'_>,
    budget: &mut TransitionBudget,
    definition: &DirectSwitchoverDefinition,
    observed_at_unix_seconds: i64,
    reason: String,
) -> TerminalOutcome {
    let snapshot = definition.compensation_snapshot();
    let mut deadline = match next_deadline(observed_at_unix_seconds) {
        Ok(deadline) => deadline,
        Err(error) => return stopped(error),
    };
    let old_primary = match definition.member(definition.old_primary_id) {
        Ok(member) => member,
        Err(error) => return stopped(error),
    };

    let promote_old = match call_activity::<CompensatePromoteOldPrimaryActivity>(
        context,
        budget,
        deadline,
        CompensatePromoteOldPrimaryInput {
            old_primary_id: definition.old_primary_id,
            old_primary_instance_id: old_primary.instance_id.clone(),
        },
    )
    .await
    {
        Ok(result) => result,
        Err(error) => return stopped(error),
    };
    deadline = match next_deadline(promote_old.observed_at_unix_seconds) {
        Ok(deadline) => deadline,
        Err(message) => return stopped(message),
    };

    for (index, replica_id) in definition
        .compensation_epoch_distribution_ids()
        .into_iter()
        .enumerate()
    {
        let distribution_index = match u8::try_from(index) {
            Ok(index) => index,
            Err(_) => return stopped("compensation epoch distribution index overflow".to_string()),
        };
        let replica = match definition.member(replica_id) {
            Ok(member) => member,
            Err(error) => return stopped(error),
        };
        let result = match call_activity::<CompensateDistributeReplicaEpochActivity>(
            context,
            budget,
            deadline,
            CompensateDistributeReplicaEpochInput {
                distribution_index,
                replica_id,
                replica_instance_id: replica.instance_id.clone(),
            },
        )
        .await
        {
            Ok(result) => result,
            Err(error) => return stopped(error),
        };
        deadline = match next_deadline(result.observed_at_unix_seconds) {
            Ok(deadline) => deadline,
            Err(message) => return stopped(message),
        };
    }

    for step in [
        CompensationStep::CatchUpConfiguration,
        CompensationStep::CurrentConfiguration,
    ] {
        let result = match step {
            CompensationStep::CatchUpConfiguration => {
                call_activity::<InstallCompensationCatchUpConfigurationActivity>(
                    context,
                    budget,
                    deadline,
                    InstallCompensationCatchUpConfigurationInput {
                        old_primary_id: definition.old_primary_id,
                        old_primary_instance_id: old_primary.instance_id.clone(),
                    },
                )
                .await
            }
            CompensationStep::CurrentConfiguration => {
                call_activity::<InstallCompensationCurrentConfigurationActivity>(
                    context,
                    budget,
                    deadline,
                    InstallCompensationCurrentConfigurationInput {
                        old_primary_id: definition.old_primary_id,
                        old_primary_instance_id: old_primary.instance_id.clone(),
                    },
                )
                .await
            }
        };
        let result = match result {
            Ok(result) => result,
            Err(error) => return stopped(error),
        };
        deadline = match next_deadline(result.observed_at_unix_seconds) {
            Ok(deadline) => deadline,
            Err(message) => return stopped(message),
        };
    }

    let restore_old_label = match call_activity::<RestoreOldPrimaryLabelActivity>(
        context,
        budget,
        deadline,
        RestoreOldPrimaryLabelInput {
            old_primary_id: definition.old_primary_id,
            old_primary_instance_id: old_primary.instance_id.clone(),
        },
    )
    .await
    {
        Ok(result) => result,
        Err(error) => return stopped(error),
    };
    deadline = match next_deadline(restore_old_label.observed_at_unix_seconds) {
        Ok(deadline) => deadline,
        Err(message) => return stopped(message),
    };

    let restore_target_label = match call_activity::<RestoreTargetSecondaryLabelActivity>(
        context,
        budget,
        deadline,
        RestoreTargetSecondaryLabelInput {
            target_primary_id: definition.target_primary_id,
            target_primary_instance_id: definition
                .member(definition.target_primary_id)
                .map(|member| member.instance_id.clone())
                .unwrap_or_default(),
        },
    )
    .await
    {
        Ok(result) => result,
        Err(error) => return stopped(error),
    };
    let observed_at = restore_target_label.observed_at_unix_seconds;

    attest_compensated(
        context,
        budget,
        definition,
        snapshot,
        observed_at,
        reason,
        DirectSwitchoverTerminalBranch::PostPromotionCompensated,
    )
    .await
}

async fn attest_compensated(
    context: &mut WorkflowContext<'_>,
    budget: &mut TransitionBudget,
    _definition: &DirectSwitchoverDefinition,
    snapshot: StablePartitionSnapshotStatus,
    observed_at_unix_seconds: i64,
    reason: String,
    branch: DirectSwitchoverTerminalBranch,
) -> TerminalOutcome {
    let deadline = match next_deadline(observed_at_unix_seconds) {
        Ok(deadline) => deadline,
        Err(error) => return stopped(error),
    };
    match call_activity::<AttestCompensatedTopologyActivity>(
        context,
        budget,
        deadline,
        AttestCompensatedTopologyInput {
            expected_snapshot: snapshot.clone(),
        },
    )
    .await
    {
        Ok(AttestCompensatedTopologyOutput { snapshot, .. }) => {
            complete(snapshot, true, branch, Some(reason))
        }
        Err(error) => stopped(format!("compensated topology attestation failed: {error}")),
    }
}

async fn call_activity<A: SwitchoverActivityContract>(
    context: &mut WorkflowContext<'_>,
    budget: &mut TransitionBudget,
    action_deadline_unix_seconds: i64,
    input: A::Request,
) -> Result<A::Output, String> {
    budget.consume(A::NAME)?;
    let input = SwitchoverActivityInput::<A>::new(input, action_deadline_unix_seconds);
    let options = ActivityOptions::new(
        3,
        1_000,
        input.action_deadline_unix_seconds().checked_mul(1_000),
        None,
    )
    .map_err(|error| error.to_string())?;
    context
        .schedule_activity_typed::<OrdinarySwitchoverActivity<A>>(A::NAME, &input, options)
        .await
        .map_err(activity_invocation_message)
}

async fn call_activity_branch<A: SwitchoverActivityContract>(
    context: &mut WorkflowContext<'_>,
    budget: &mut TransitionBudget,
    action_deadline_unix_seconds: i64,
    input: A::Request,
) -> Result<A::Output, EffectBranch> {
    budget.consume(A::NAME).map_err(EffectBranch::Stopped)?;
    let input = SwitchoverActivityInput::<A>::new(input, action_deadline_unix_seconds);
    let options = ActivityOptions::new(
        3,
        1_000,
        input.action_deadline_unix_seconds().checked_mul(1_000),
        None,
    )
    .map_err(|error| EffectBranch::Stopped(error.to_string()))?;
    context
        .schedule_activity_typed::<OrdinarySwitchoverActivity<A>>(A::NAME, &input, options)
        .await
        .map_err(activity_invocation_branch)
}

fn activity_invocation_message(error: ActivityInvocationError) -> String {
    match error {
        ActivityInvocationError::Application(payload) => {
            serde_json::from_slice::<BoundedEffectError>(payload.as_slice())
                .map(|error| error.to_string())
                .unwrap_or_else(|_| "activity returned an invalid application failure".to_string())
        }
        other => other.to_string(),
    }
}

fn activity_invocation_branch(error: ActivityInvocationError) -> EffectBranch {
    match error {
        ActivityInvocationError::Application(payload) => {
            match serde_json::from_slice::<BoundedEffectError>(payload.as_slice()) {
                Ok(error) => effect_call_branch(error),
                Err(_) => EffectBranch::Stopped(
                    "activity returned an invalid application failure".to_string(),
                ),
            }
        }
        other => EffectBranch::Stopped(other.to_string()),
    }
}

fn effect_call_branch(error: BoundedEffectError) -> EffectBranch {
    match error.kind() {
        EffectErrorKind::DomainFailure | EffectErrorKind::DeadlineExceeded => {
            EffectBranch::DomainFailure {
                observed_at: error.observed_at_unix_seconds().unwrap_or_default(),
                message: error.message().to_string(),
            }
        }
        _ => EffectBranch::Stopped(error.to_string()),
    }
}

enum EffectBranch {
    DomainFailure { observed_at: i64, message: String },
    Stopped(String),
}

enum CompensationStep {
    CatchUpConfiguration,
    CurrentConfiguration,
}

fn complete(
    snapshot: StablePartitionSnapshotStatus,
    compensated: bool,
    branch: DirectSwitchoverTerminalBranch,
    reason: Option<String>,
) -> TerminalOutcome {
    terminal(DirectSwitchoverTerminalRecord::Complete {
        snapshot,
        compensated,
        branch,
        reason: reason
            .map(|reason| super::bounded_utf8(&reason, super::SWITCHOVER_MAX_ERROR_BYTES)),
    })
}

fn stopped(message: String) -> TerminalOutcome {
    let message = super::bounded_utf8(&message, super::SWITCHOVER_MAX_ERROR_BYTES);
    let payload = serde_json::to_vec(&DirectSwitchoverTerminalRecord::Stopped { message })
        .unwrap_or_else(|_| br#"{"status":"stopped","message":"encode failure"}"#.to_vec());
    TerminalOutcome::failed(ExactBytes::new(payload))
}

fn terminal(record: DirectSwitchoverTerminalRecord) -> TerminalOutcome {
    match serde_json::to_vec(&record) {
        Ok(payload) => TerminalOutcome::succeeded(ExactBytes::new(payload)),
        Err(error) => stopped(format!("encode direct switchover terminal: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kuberic_durable_execution::{
        ActivityFailure, ActivityObservation, CheckpointLimits, DurableActivity, DurableHost,
        EffectMetadata, EffectObservation, ExactBytes, ExecutionId, ExecutionSpec, HostEpoch,
        HostOutcome, InMemoryCheckpointStore, PreparedActivityError, PreparedCommand,
        PreparedEffectResolver,
    };

    use crate::crd::{EpochStatus, StableReplicaRoleStatus, StableReplicaSnapshotStatus};
    use crate::durable::switchover_execution::direct_initial_operation;

    fn snapshot(count: i64) -> StablePartitionSnapshotStatus {
        StablePartitionSnapshotStatus {
            epoch: EpochStatus {
                data_loss_number: 1,
                configuration_number: 7,
            },
            primary_id: 1,
            members: (1..=count)
                .rev()
                .map(|id| StableReplicaSnapshotStatus {
                    id,
                    instance_id: format!("instance-{id}"),
                    role: if id == 1 {
                        StableReplicaRoleStatus::Primary
                    } else {
                        StableReplicaRoleStatus::ActiveSecondary
                    },
                    election_metadata: None,
                })
                .collect(),
            write_quorum: u32::try_from(count / 2 + 1).unwrap(),
        }
    }

    #[derive(Clone, Copy)]
    enum ScriptedFailureKind {
        Failed,
        DeadlineExceeded,
        UnavailableAtDeadline,
    }

    #[derive(Clone, Copy)]
    struct ScriptedFailure {
        name: &'static str,
        kind: ScriptedFailureKind,
        message: &'static str,
    }

    struct ScriptedResolver;

    impl PreparedEffectResolver for ScriptedResolver {
        fn resolve(
            &self,
            _execution_id: ExecutionId,
            _logical: &kuberic_durable_execution::ActivitySpec,
            metadata: EffectMetadata,
            _recorded: Option<&PreparedCommand>,
        ) -> Result<PreparedCommand, PreparedActivityError> {
            PreparedCommand::new(
                ExactBytes::new(b"null".to_vec()),
                metadata.max_command_bytes(),
            )
            .map_err(|_| PreparedActivityError::Encoding)
        }
    }

    async fn run_script(
        replica_count: i64,
        failures: &[ScriptedFailure],
        proven_no_admission: Option<&str>,
    ) -> (
        Vec<String>,
        Vec<Option<i64>>,
        DirectSwitchoverTerminalRecord,
    ) {
        let execution_id = ExecutionId::from_bytes([41; 16]);
        let operation =
            direct_initial_operation("set-uid", snapshot(replica_count), 2, 100).unwrap();
        let input = SwitchoverWorkflowInput {
            version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
            execution_id: super::super::encode_execution_id(execution_id),
            initial_operation: operation,
        };
        let execution = ExecutionSpec::new(
            execution_id,
            ExactBytes::new(serde_json::to_vec(&input).unwrap()),
            4_096,
        );
        let limits = CheckpointLimits::new(64, 1024 * 1024, 16 * 1024).unwrap();
        let mut host = DurableHost::new(
            InMemoryCheckpointStore::new(),
            HostEpoch::from_bytes([42; 16]),
            limits,
        );
        let workflow = DirectSwitchoverWorkflow;
        let mut names = Vec::new();
        let mut targets = Vec::new();
        let mut observed_at = 101i64;
        let mut occurrences = std::collections::BTreeMap::<String, usize>::new();

        loop {
            match host
                .turn_and_expose_effects(&workflow, execution.clone(), &ScriptedResolver)
                .await
            {
                HostOutcome::ScheduleAccepted { .. } => {}
                HostOutcome::DispatchPermitted { permit, .. } => {
                    let name = permit.activity().name().name().to_string();
                    names.push(name.clone());
                    let activity_input = serde_json::from_slice::<serde_json::Value>(
                        permit.activity().input().as_slice(),
                    )
                    .unwrap();
                    assert!(activity_input.get("contractVersion").is_none());
                    assert!(activity_input.get("executionId").is_none());
                    assert!(activity_input.get("deadlineUnixSeconds").is_none());
                    assert!(
                        permit
                            .activity()
                            .options()
                            .action_deadline_unix_millis()
                            .is_some()
                    );
                    let target_id = activity_input
                        .get("targetId")
                        .and_then(serde_json::Value::as_i64);
                    targets.push(target_id);
                    let occurrence = occurrences.entry(name.clone()).or_default();
                    let failure = failures
                        .iter()
                        .copied()
                        .find(|failure| failure.name == name);
                    let result = scripted_result(
                        &name,
                        observed_at,
                        failure,
                        proven_no_admission == Some(name.as_str()) && *occurrence == 0,
                        &activity_input,
                    );
                    *occurrence += 1;
                    observed_at += 1;
                    let strict = matches!(
                        super::super::activities::activity_class(&name),
                        Some(
                            super::super::activities::SwitchoverActivityClass::StrictEffectRequired
                        )
                    );
                    let outcome = match (strict, result) {
                        (true, ScriptedResult::Success(result)) => {
                            host.observe_effect(
                                &execution,
                                EffectObservation::completed(
                                    permit.activity().clone(),
                                    permit.attempt_id(),
                                    result,
                                ),
                            )
                            .await
                        }
                        (true, ScriptedResult::Failure(failure)) => {
                            host.observe_effect(
                                &execution,
                                EffectObservation::completed_failure(
                                    permit.activity().clone(),
                                    permit.attempt_id(),
                                    ActivityFailure::Application(failure),
                                ),
                            )
                            .await
                        }
                        (true, ScriptedResult::ProvenNoAdmission(failure)) => {
                            host.observe_effect(
                                &execution,
                                EffectObservation::proven_no_admission(
                                    permit.activity().clone(),
                                    permit.attempt_id(),
                                    ActivityFailure::Application(failure),
                                ),
                            )
                            .await
                        }
                        (false, ScriptedResult::Success(result)) => {
                            host.observe(
                                &execution,
                                ActivityObservation::new(permit.activity().clone(), result),
                            )
                            .await
                        }
                        (false, ScriptedResult::Failure(failure))
                        | (false, ScriptedResult::ProvenNoAdmission(failure)) => {
                            host.observe_failure(
                                &execution,
                                permit.activity(),
                                ActivityFailure::Application(failure),
                            )
                            .await
                        }
                    };
                    assert!(matches!(outcome, HostOutcome::ObservationAccepted { .. }));
                }
                HostOutcome::WorkflowCompleted { outcome, .. } => {
                    let record = serde_json::from_slice::<DirectSwitchoverTerminalRecord>(
                        outcome.payload().as_slice(),
                    )
                    .unwrap();
                    return (names, targets, record);
                }
                other => panic!("unexpected direct workflow host outcome: {other:?}"),
            }
        }
    }

    enum ScriptedResult {
        Success(ExactBytes),
        Failure(ExactBytes),
        ProvenNoAdmission(ExactBytes),
    }

    fn scripted_result(
        name: &str,
        observed_at: i64,
        failure: Option<ScriptedFailure>,
        proven_no_admission: bool,
        activity_input: &serde_json::Value,
    ) -> ScriptedResult {
        if proven_no_admission {
            let error = BoundedEffectError::observed_at(
                EffectErrorKind::ProvenNoAdmission,
                "dispatch was proven not admitted",
                observed_at,
                512,
            )
            .unwrap();
            return ScriptedResult::ProvenNoAdmission(ExactBytes::new(
                serde_json::to_vec(&error).unwrap(),
            ));
        }
        if let Some(failure) = failure {
            let kind = match failure.kind {
                ScriptedFailureKind::Failed => EffectErrorKind::DomainFailure,
                ScriptedFailureKind::DeadlineExceeded => EffectErrorKind::DeadlineExceeded,
                ScriptedFailureKind::UnavailableAtDeadline => {
                    EffectErrorKind::UnavailableAtDeadline
                }
            };
            let error =
                BoundedEffectError::observed_at(kind, failure.message, observed_at, 512).unwrap();
            return ScriptedResult::Failure(ExactBytes::new(serde_json::to_vec(&error).unwrap()));
        }
        let value = if name == CaptureFrozenLsnActivity::NAME {
            serde_json::json!({
                "frozenLsn": 55,
                "observedAtUnixSeconds": observed_at,
            })
        } else if name == AttestTargetTopologyActivity::NAME
            || name == AttestCompensatedTopologyActivity::NAME
        {
            serde_json::json!({
                "observedAtUnixSeconds": observed_at,
                "snapshot": activity_input.get("expectedSnapshot").unwrap(),
            })
        } else {
            serde_json::json!({
                "observedAtUnixSeconds": observed_at,
            })
        };
        ScriptedResult::Success(ExactBytes::new(serde_json::to_vec(&value).unwrap()))
    }

    #[tokio::test]
    async fn direct_switchover_workflow_spells_out_successful_protocol() {
        for replica_count in [2, 4, 9] {
            let (names, _targets, terminal) = run_script(replica_count, &[], None).await;
            let mut expected = vec![
                <RevokeWritesActivity as SwitchoverActivityContract>::NAME,
                CaptureFrozenLsnActivity::NAME,
                WaitTargetCaughtUpActivity::NAME,
                <DemoteOldPrimaryActivity as SwitchoverActivityContract>::NAME,
                <PromoteTargetActivity as SwitchoverActivityContract>::NAME,
            ];
            expected.extend(std::iter::repeat_n(
                DistributeReplicaEpochActivity::NAME,
                usize::try_from(replica_count - 2).unwrap(),
            ));
            expected.extend([
                InstallTargetCatchUpConfigurationActivity::NAME,
                WaitTargetWriteQuorumActivity::NAME,
                InstallTargetCurrentConfigurationActivity::NAME,
                PublishTargetPrimaryLabelActivity::NAME,
                PublishOldPrimarySecondaryLabelActivity::NAME,
                AttestTargetTopologyActivity::NAME,
            ]);
            assert_eq!(names, expected);
            assert!(matches!(
                terminal,
                DirectSwitchoverTerminalRecord::Complete {
                    compensated: false,
                    branch: DirectSwitchoverTerminalBranch::TargetSuccess,
                    ..
                }
            ));
        }
    }

    #[tokio::test]
    async fn direct_switchover_workflow_spells_out_post_promotion_compensation() {
        for replica_count in [2, 4, 9] {
            let (names, _targets, terminal) = run_script(
                replica_count,
                &[ScriptedFailure {
                    name: <PromoteTargetActivity as SwitchoverActivityContract>::NAME,
                    kind: ScriptedFailureKind::Failed,
                    message: "promotion failed",
                }],
                None,
            )
            .await;
            let mut expected = vec![
                <RevokeWritesActivity as SwitchoverActivityContract>::NAME,
                CaptureFrozenLsnActivity::NAME,
                WaitTargetCaughtUpActivity::NAME,
                <DemoteOldPrimaryActivity as SwitchoverActivityContract>::NAME,
                <PromoteTargetActivity as SwitchoverActivityContract>::NAME,
                <CompensatePromoteOldPrimaryActivity as SwitchoverActivityContract>::NAME,
            ];
            expected.extend(std::iter::repeat_n(
                CompensateDistributeReplicaEpochActivity::NAME,
                usize::try_from(replica_count - 1).unwrap(),
            ));
            expected.extend([
                InstallCompensationCatchUpConfigurationActivity::NAME,
                InstallCompensationCurrentConfigurationActivity::NAME,
                RestoreOldPrimaryLabelActivity::NAME,
                RestoreTargetSecondaryLabelActivity::NAME,
                AttestCompensatedTopologyActivity::NAME,
            ]);
            assert_eq!(names, expected);
            assert!(matches!(
                terminal,
                DirectSwitchoverTerminalRecord::Complete {
                    compensated: true,
                    branch: DirectSwitchoverTerminalBranch::PostPromotionCompensated,
                    ..
                }
            ));
        }
    }

    #[tokio::test]
    async fn direct_switchover_workflow_spells_out_pre_promotion_compensation() {
        for replica_count in [2, 4, 9] {
            let (names, _targets, terminal) = run_script(
                replica_count,
                &[ScriptedFailure {
                    name: WaitTargetCaughtUpActivity::NAME,
                    kind: ScriptedFailureKind::DeadlineExceeded,
                    message: "target catch-up timed out",
                }],
                None,
            )
            .await;
            assert_eq!(
                names,
                vec![
                    <RevokeWritesActivity as SwitchoverActivityContract>::NAME,
                    CaptureFrozenLsnActivity::NAME,
                    WaitTargetCaughtUpActivity::NAME,
                    RestorePreviousCurrentConfigurationActivity::NAME,
                    AttestCompensatedTopologyActivity::NAME,
                ]
            );
            assert!(matches!(
                terminal,
                DirectSwitchoverTerminalRecord::Complete {
                    compensated: true,
                    branch: DirectSwitchoverTerminalBranch::PreviousConfigurationRestored,
                    ..
                }
            ));
        }
    }

    #[tokio::test]
    async fn direct_switchover_records_one_same_operation_redelivery() {
        let (names, _targets, terminal) = run_script(
            3,
            &[],
            Some(<DemoteOldPrimaryActivity as SwitchoverActivityContract>::NAME),
        )
        .await;
        assert_eq!(
            names
                .iter()
                .filter(|name| name.as_str()
                    == <DemoteOldPrimaryActivity as SwitchoverActivityContract>::NAME)
                .count(),
            2
        );
        assert!(matches!(
            terminal,
            DirectSwitchoverTerminalRecord::Complete {
                compensated: false,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn direct_switchover_late_failure_stops_without_compensation() {
        let (names, _targets, terminal) = run_script(
            3,
            &[ScriptedFailure {
                name: InstallTargetCurrentConfigurationActivity::NAME,
                kind: ScriptedFailureKind::Failed,
                message: "current configuration failed",
            }],
            None,
        )
        .await;
        assert!(names.contains(&InstallTargetCurrentConfigurationActivity::NAME.to_string()));
        assert!(!names.contains(&RestorePreviousCurrentConfigurationActivity::NAME.to_string()));
        assert!(!names.contains(
            &<CompensatePromoteOldPrimaryActivity as SwitchoverActivityContract>::NAME.to_string()
        ));
        assert!(matches!(
            terminal,
            DirectSwitchoverTerminalRecord::Stopped { .. }
        ));
    }

    #[tokio::test]
    async fn direct_switchover_expired_actions_preserve_prior_compensation_boundaries() {
        for activity_name in [
            <RevokeWritesActivity as SwitchoverActivityContract>::NAME,
            <DemoteOldPrimaryActivity as SwitchoverActivityContract>::NAME,
            <PromoteTargetActivity as SwitchoverActivityContract>::NAME,
        ] {
            let (names, _targets, terminal) = run_script(
                3,
                &[ScriptedFailure {
                    name: activity_name,
                    kind: ScriptedFailureKind::DeadlineExceeded,
                    message: "action reached its exact deadline",
                }],
                None,
            )
            .await;
            assert!(matches!(
                terminal,
                DirectSwitchoverTerminalRecord::Complete {
                    compensated: true,
                    branch,
                    ..
                } if branch == match activity_name {
                    <RevokeWritesActivity as SwitchoverActivityContract>::NAME =>
                        DirectSwitchoverTerminalBranch::RevokeSafeFailure,
                    <DemoteOldPrimaryActivity as SwitchoverActivityContract>::NAME =>
                        DirectSwitchoverTerminalBranch::PreviousConfigurationRestored,
                    <PromoteTargetActivity as SwitchoverActivityContract>::NAME =>
                        DirectSwitchoverTerminalBranch::PostPromotionCompensated,
                    _ => unreachable!(),
                }
            ));
            assert!(
                names.contains(&AttestCompensatedTopologyActivity::NAME.to_string()),
                "{activity_name}"
            );
            match activity_name {
                <RevokeWritesActivity as SwitchoverActivityContract>::NAME => {
                    assert!(
                        !names.contains(
                            &RestorePreviousCurrentConfigurationActivity::NAME.to_string()
                        )
                    );
                    assert!(
                        !names.contains(&<CompensatePromoteOldPrimaryActivity as SwitchoverActivityContract>::NAME.to_string())
                    );
                }
                <DemoteOldPrimaryActivity as SwitchoverActivityContract>::NAME => {
                    assert!(
                        names.contains(
                            &RestorePreviousCurrentConfigurationActivity::NAME.to_string()
                        )
                    );
                    assert!(
                        !names.contains(&<CompensatePromoteOldPrimaryActivity as SwitchoverActivityContract>::NAME.to_string())
                    );
                }
                <PromoteTargetActivity as SwitchoverActivityContract>::NAME => {
                    assert!(names.contains(&<CompensatePromoteOldPrimaryActivity as SwitchoverActivityContract>::NAME.to_string()));
                }
                _ => unreachable!(),
            }
        }
    }

    #[tokio::test]
    async fn direct_switchover_unavailable_at_action_deadline_stops_without_compensation() {
        for activity_name in [
            <RevokeWritesActivity as SwitchoverActivityContract>::NAME,
            <DemoteOldPrimaryActivity as SwitchoverActivityContract>::NAME,
            <PromoteTargetActivity as SwitchoverActivityContract>::NAME,
        ] {
            let (names, _targets, terminal) = run_script(
                3,
                &[ScriptedFailure {
                    name: activity_name,
                    kind: ScriptedFailureKind::UnavailableAtDeadline,
                    message: "action target unavailable at its exact deadline",
                }],
                None,
            )
            .await;
            assert!(matches!(
                terminal,
                DirectSwitchoverTerminalRecord::Stopped { .. }
            ));
            assert!(
                !names.contains(&RestorePreviousCurrentConfigurationActivity::NAME.to_string())
            );
            assert!(
                !names.contains(
                    &<CompensatePromoteOldPrimaryActivity as SwitchoverActivityContract>::NAME
                        .to_string()
                )
            );
            assert!(!names.contains(&AttestCompensatedTopologyActivity::NAME.to_string()));
        }
    }

    #[tokio::test]
    async fn direct_switchover_late_and_compensation_deadlines_stop() {
        for activity_name in [
            DistributeReplicaEpochActivity::NAME,
            InstallTargetCatchUpConfigurationActivity::NAME,
            PublishTargetPrimaryLabelActivity::NAME,
        ] {
            let (names, _targets, terminal) = run_script(
                3,
                &[ScriptedFailure {
                    name: activity_name,
                    kind: ScriptedFailureKind::DeadlineExceeded,
                    message: "late action reached its exact deadline",
                }],
                None,
            )
            .await;
            assert!(matches!(
                terminal,
                DirectSwitchoverTerminalRecord::Stopped { .. }
            ));
            assert!(
                !names.contains(&RestorePreviousCurrentConfigurationActivity::NAME.to_string())
            );
            assert!(
                !names.contains(
                    &<CompensatePromoteOldPrimaryActivity as SwitchoverActivityContract>::NAME
                        .to_string()
                )
            );
        }

        let (names, _targets, terminal) = run_script(
            3,
            &[
                ScriptedFailure {
                    name: <PromoteTargetActivity as SwitchoverActivityContract>::NAME,
                    kind: ScriptedFailureKind::Failed,
                    message: "promotion failed",
                },
                ScriptedFailure {
                    name: <CompensatePromoteOldPrimaryActivity as SwitchoverActivityContract>::NAME,
                    kind: ScriptedFailureKind::DeadlineExceeded,
                    message: "compensation action reached its exact deadline",
                },
            ],
            None,
        )
        .await;
        assert!(matches!(
            terminal,
            DirectSwitchoverTerminalRecord::Stopped { .. }
        ));
        assert!(names.contains(
            &<CompensatePromoteOldPrimaryActivity as SwitchoverActivityContract>::NAME.to_string()
        ));
        assert!(!names.contains(&CompensateDistributeReplicaEpochActivity::NAME.to_string()));
        assert!(!names.contains(&AttestCompensatedTopologyActivity::NAME.to_string()));
    }

    #[tokio::test]
    async fn direct_switchover_replay_is_identical_one_hundred_times() {
        let baseline = run_script(2, &[], None).await;
        for _ in 0..100 {
            assert_eq!(run_script(2, &[], None).await, baseline);
        }
    }

    #[test]
    fn direct_switchover_terminal_messages_are_utf8_bounded() {
        let TerminalOutcome::Failed(payload) = stopped("é".repeat(1_000)) else {
            panic!("stopped terminal must fail");
        };
        let terminal =
            serde_json::from_slice::<DirectSwitchoverTerminalRecord>(payload.as_slice()).unwrap();
        let DirectSwitchoverTerminalRecord::Stopped { message } = terminal else {
            panic!("expected stopped terminal");
        };
        assert!(message.len() <= super::super::SWITCHOVER_MAX_ERROR_BYTES);
        assert!(message.is_char_boundary(message.len()));

        for exact in ["x".repeat(512), "é".repeat(256)] {
            assert!(
                serde_json::to_vec(&DirectSwitchoverTerminalRecord::Stopped { message: exact })
                    .is_ok()
            );
        }
        for one_over in ["x".repeat(513), "é".repeat(257)] {
            assert!(
                serde_json::to_vec(&DirectSwitchoverTerminalRecord::Stopped {
                    message: one_over.clone()
                })
                .is_err()
            );
            assert!(
                serde_json::from_value::<DirectSwitchoverTerminalRecord>(serde_json::json!({
                    "status": "stopped",
                    "message": one_over,
                }))
                .is_err()
            );
        }
        let snapshot = snapshot(2);
        for exact in ["x".repeat(512), "é".repeat(256)] {
            assert!(
                serde_json::to_vec(&DirectSwitchoverTerminalRecord::Complete {
                    snapshot: snapshot.clone(),
                    compensated: true,
                    branch: DirectSwitchoverTerminalBranch::RevokeSafeFailure,
                    reason: Some(exact),
                })
                .is_ok()
            );
        }
        for one_over in ["x".repeat(513), "é".repeat(257)] {
            assert!(
                serde_json::to_vec(&DirectSwitchoverTerminalRecord::Complete {
                    snapshot: snapshot.clone(),
                    compensated: true,
                    branch: DirectSwitchoverTerminalBranch::RevokeSafeFailure,
                    reason: Some(one_over.clone()),
                })
                .is_err()
            );
            assert!(
                serde_json::from_value::<DirectSwitchoverTerminalRecord>(serde_json::json!({
                    "status": "complete",
                    "snapshot": snapshot.clone(),
                    "compensated": true,
                    "branch": "revoke_safe_failure",
                    "reason": one_over,
                    "accounting": {
                        "externalEffectCount": 1,
                        "passiveObservationCount": 1,
                    },
                }))
                .is_err()
            );
        }
    }

    #[derive(Clone, Debug, Deserialize, Serialize)]
    struct BudgetProbeInput {
        index: usize,
    }

    #[derive(Clone, Debug, Deserialize, Serialize)]
    struct BudgetProbeOutput;

    struct BudgetProbeActivity;

    impl DurableActivity for BudgetProbeActivity {
        type Input = BudgetProbeInput;
        type Output = BudgetProbeOutput;

        const NAME: &'static str = "kuberic.switchover.test-transition-budget";
        const VERSION: u32 = 1;
        const MAX_INPUT_BYTES: u64 = 128;
        const MAX_RESULT_BYTES: u64 = 128;
    }

    struct BudgetProbeWorkflow {
        calls: usize,
    }

    #[async_trait]
    impl Workflow for BudgetProbeWorkflow {
        async fn run(
            &self,
            context: &mut WorkflowContext<'_>,
            _input: ExactBytes,
        ) -> TerminalOutcome {
            let mut budget = TransitionBudget::new();
            for index in 0..self.calls {
                if let Err(error) = budget.consume(BudgetProbeActivity::NAME) {
                    return stopped(error);
                }
                if let Err(error) = context
                    .call::<BudgetProbeActivity>(BudgetProbeInput { index })
                    .await
                {
                    return stopped(error.to_string());
                }
            }
            TerminalOutcome::succeeded(ExactBytes::new(b"{}".to_vec()))
        }
    }

    async fn run_budget_probe(calls: usize) -> (TerminalOutcome, u64) {
        let execution_id = ExecutionId::from_bytes([93; 16]);
        let execution = ExecutionSpec::new(execution_id, ExactBytes::new(b"{}".to_vec()), 4_096);
        let mut host = DurableHost::new(
            InMemoryCheckpointStore::new(),
            HostEpoch::from_bytes([94; 16]),
            CheckpointLimits::new(65, 1024 * 1024, 16 * 1024).unwrap(),
        );
        let workflow = BudgetProbeWorkflow { calls };
        loop {
            match host.turn(&workflow, execution.clone()).await {
                HostOutcome::ScheduleAccepted { .. } => {}
                HostOutcome::DispatchPermitted { permit, .. } => {
                    let outcome = host
                        .observe(
                            &execution,
                            ActivityObservation::new(
                                permit.activity().clone(),
                                kuberic_durable_execution::encode_activity_result::<
                                    BudgetProbeActivity,
                                >(&BudgetProbeOutput)
                                .unwrap(),
                            ),
                        )
                        .await;
                    assert!(matches!(outcome, HostOutcome::ObservationAccepted { .. }));
                }
                HostOutcome::WorkflowCompleted {
                    outcome,
                    completed_activity_count,
                    ..
                } => return (outcome, completed_activity_count),
                other => panic!("unexpected budget probe outcome: {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn direct_switchover_transition_budget_is_enforced_at_execution_boundary() {
        let (exact, exact_count) =
            run_budget_probe(super::super::SWITCHOVER_MAX_TRANSITION_FUEL).await;
        assert!(matches!(exact, TerminalOutcome::Succeeded(_)));
        assert_eq!(
            exact_count,
            super::super::SWITCHOVER_MAX_TRANSITION_FUEL as u64
        );

        let (one_over, one_over_count) =
            run_budget_probe(super::super::SWITCHOVER_MAX_TRANSITION_FUEL + 1).await;
        assert!(matches!(one_over, TerminalOutcome::Failed(_)));
        assert_eq!(
            one_over_count,
            super::super::SWITCHOVER_MAX_TRANSITION_FUEL as u64
        );
    }
}
