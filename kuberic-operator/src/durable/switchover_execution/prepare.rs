use std::collections::BTreeMap;

use kuberic_core::types::{
    AccessStatus, DurableActionState, DurableReplicaAction, Epoch,
    ReplicaConfigurationMemberStatus, ReplicaConfigurationMode, ReplicaConfigurationStatus,
    ReplicaInfo, ReplicaInstanceId, ReplicaSetConfig, ReplicaSetQuorumMode, ReplicaStatus, Role,
};
use kuberic_durable_execution::{
    BoundedEffectError, EffectErrorKind, EffectOutcome, ExactBytes, PreparedActivityError,
    decode_activity_result, encode_activity_result,
};

use crate::crd::{
    DurableActionKind, DurablePostconditionKind, DurablePostconditionStatus, EpochStatus,
    PendingActionStatus, StablePartitionSnapshotStatus, StableReplicaRoleStatus,
};
use crate::durable::{
    OperationObservations, correlated_action_observation,
    effects::{
        DurableEffectPreparationError, LabelEffectCommand, ReplicaEffectCommand,
        command_generation_change_proves_no_admission, prepare_replica_effect_command,
    },
};

use super::{
    activities::{
        AttestCompensatedTopologyActivity, AttestCompensatedTopologyInput,
        AttestCompensatedTopologyOutput, AttestTargetTopologyActivity, AttestTargetTopologyInput,
        AttestTargetTopologyOutput, CaptureFrozenLsnActivity, CaptureFrozenLsnInput,
        CaptureFrozenLsnOutput, CompensateDistributeReplicaEpochActivity,
        CompensateDistributeReplicaEpochInput, CompensatePromoteOldPrimaryActivity,
        CompensatePromoteOldPrimaryInput, DIRECT_SWITCHOVER_CONTRACT_VERSION,
        DemoteOldPrimaryActivity, DemoteOldPrimaryInput, DistributeReplicaEpochActivity,
        DistributeReplicaEpochInput, EffectApplied,
        InstallCompensationCatchUpConfigurationActivity,
        InstallCompensationCatchUpConfigurationInput,
        InstallCompensationCurrentConfigurationActivity,
        InstallCompensationCurrentConfigurationInput, InstallTargetCatchUpConfigurationActivity,
        InstallTargetCatchUpConfigurationInput, InstallTargetCurrentConfigurationActivity,
        InstallTargetCurrentConfigurationInput, PromoteTargetActivity, PromoteTargetInput,
        PublishOldPrimarySecondaryLabelActivity, PublishOldPrimarySecondaryLabelInput,
        PublishTargetPrimaryLabelActivity, PublishTargetPrimaryLabelInput, ReplicaEffectFamily,
        RestoreOldPrimaryLabelActivity, RestoreOldPrimaryLabelInput,
        RestorePreviousCurrentConfigurationActivity, RestorePreviousCurrentConfigurationInput,
        RestoreTargetSecondaryLabelActivity, RestoreTargetSecondaryLabelInput,
        RevokeWritesActivity, RevokeWritesInput, StrictSwitchoverActivityContract,
        SwitchoverActivityContract, WaitTargetCaughtUpActivity, WaitTargetCaughtUpInput,
        WaitTargetCaughtUpOutput, WaitTargetWriteQuorumActivity, WaitTargetWriteQuorumInput,
    },
    model::DirectSwitchoverDefinition,
};

pub enum DirectEvaluation {
    Observe(ExactBytes),
    AwaitEvidence,
    DispatchReplica {
        action: DurableReplicaAction,
        pending: Box<PendingActionStatus>,
    },
    DispatchLabel,
}

struct EncodedSwitchoverOutcome<A>(std::marker::PhantomData<A>);

impl<A: SwitchoverActivityContract> kuberic_durable_execution::DurableActivity
    for EncodedSwitchoverOutcome<A>
{
    type Input = A::Request;
    type Output = EffectOutcome<A::Output>;

    const NAME: &'static str = A::NAME;
    const VERSION: u32 = A::VERSION;
    const MAX_INPUT_BYTES: u64 = A::MAX_REQUEST_BYTES;
    const MAX_RESULT_BYTES: u64 = A::MAX_RESULT_BYTES;
}

pub(crate) enum SwitchoverDispatch<'a> {
    Replica(&'a ReplicaEffectCommand),
    ObservationOnly,
}

pub(crate) trait SwitchoverEffectFamily<E>
where
    E: kuberic_durable_execution::DurableEffect
        + StrictSwitchoverActivityContract
        + SwitchoverActivityContract<
            Request = <E as kuberic_durable_execution::DurableEffect>::Request,
            Output = <E as kuberic_durable_execution::DurableEffect>::Output,
        >,
{
    fn prepare_command(
        request: &<E as SwitchoverActivityContract>::Request,
        definition: &DirectSwitchoverDefinition,
        observations: &OperationObservations,
        addressed_instances: &BTreeMap<i64, ReplicaInstanceId>,
        now: i64,
        deadline_unix_seconds: i64,
    ) -> Result<<E as kuberic_durable_execution::DurableEffect>::Command, PreparedActivityError>;

    fn validate_recorded_command(
        request: &<E as SwitchoverActivityContract>::Request,
        command: &<E as kuberic_durable_execution::DurableEffect>::Command,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<(), PreparedActivityError>;

    fn observe(
        request: &<E as SwitchoverActivityContract>::Request,
        command: &<E as kuberic_durable_execution::DurableEffect>::Command,
        definition: &DirectSwitchoverDefinition,
        observations: &OperationObservations,
        now: i64,
        deadline_unix_seconds: i64,
    ) -> Result<
        Option<EffectOutcome<<E as kuberic_durable_execution::DurableEffect>::Output>>,
        String,
    >;

    fn observe_quarantined(
        request: &<E as SwitchoverActivityContract>::Request,
        command: &<E as kuberic_durable_execution::DurableEffect>::Command,
        definition: &DirectSwitchoverDefinition,
        observations: &OperationObservations,
        now: i64,
        deadline_unix_seconds: i64,
    ) -> Result<
        Option<EffectOutcome<<E as kuberic_durable_execution::DurableEffect>::Output>>,
        String,
    >;

    fn dispatch(
        command: &<E as kuberic_durable_execution::DurableEffect>::Command,
    ) -> SwitchoverDispatch<'_>;
}

pub(crate) struct ReplicaRequest<'a> {
    contract_version: u32,
    execution_id: String,
    sequence: u32,
    target_id: i64,
    target_instance_id: &'a str,
    expected_epoch: EpochStatus,
    desired_snapshot: StablePartitionSnapshotStatus,
    deadline_unix_seconds: i64,
}

pub(crate) trait ReplicaOperation:
    SwitchoverActivityContract<Output = EffectApplied>
{
    fn request<'a>(
        input: &'a Self::Request,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<ReplicaRequest<'a>, String>;

    fn action(
        request: &ReplicaRequest<'_>,
        definition: &DirectSwitchoverDefinition,
        observations: &OperationObservations,
    ) -> Result<DurableReplicaAction, String>;

    fn status_relation(
        request: &ReplicaRequest<'_>,
        definition: &DirectSwitchoverDefinition,
        status: &kuberic_core::types::ReplicaStatusInfo,
    ) -> StatusRelation;

    fn action_kind() -> DurableActionKind;

    fn postcondition() -> DurablePostconditionStatus;

    fn action_has_fixed_semantics(
        action: &DurableReplicaAction,
        desired_snapshot: &StablePartitionSnapshotStatus,
        definition: &DirectSwitchoverDefinition,
    ) -> bool;
}

#[allow(clippy::too_many_arguments)]
fn fixed_replica_request<'a>(
    contract_version: u32,
    execution_id: &str,
    sequence: u32,
    target_id: i64,
    target_instance_id: &'a str,
    expected_epoch: EpochStatus,
    desired_snapshot: StablePartitionSnapshotStatus,
    deadline_unix_seconds: i64,
) -> ReplicaRequest<'a> {
    ReplicaRequest {
        contract_version,
        execution_id: execution_id.to_string(),
        sequence,
        target_id,
        target_instance_id,
        expected_epoch,
        desired_snapshot,
        deadline_unix_seconds,
    }
}

fn validate_target(
    actual_id: i64,
    actual_instance_id: &str,
    expected_id: i64,
    definition: &DirectSwitchoverDefinition,
) -> Result<(), String> {
    let expected = definition.member(expected_id)?;
    if actual_id != expected_id || actual_instance_id != expected.instance_id {
        return Err("direct activity target conflicts with admission".to_string());
    }
    Ok(())
}

impl ReplicaOperation for RevokeWritesActivity {
    fn request<'a>(
        input: &'a RevokeWritesInput,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<ReplicaRequest<'a>, String> {
        validate_target(
            input.old_primary_id,
            &input.old_primary_instance_id,
            definition.old_primary_id,
            definition,
        )?;
        Ok(fixed_replica_request(
            DIRECT_SWITCHOVER_CONTRACT_VERSION,
            &definition.execution_id,
            1,
            input.old_primary_id,
            &input.old_primary_instance_id,
            definition.previous_snapshot.epoch.clone(),
            definition.previous_snapshot.clone(),
            deadline_unix_seconds,
        ))
    }

    fn action(
        _: &ReplicaRequest<'_>,
        _: &DirectSwitchoverDefinition,
        _: &OperationObservations,
    ) -> Result<DurableReplicaAction, String> {
        Ok(DurableReplicaAction::RevokeWriteStatus)
    }

    fn status_relation(
        _: &ReplicaRequest<'_>,
        definition: &DirectSwitchoverDefinition,
        status: &kuberic_core::types::ReplicaStatusInfo,
    ) -> StatusRelation {
        let previous_epoch = epoch(&definition.previous_snapshot.epoch);
        if status.role == Role::Primary
            && status.epoch == previous_epoch
            && status.write_status == AccessStatus::ReconfigurationPending
        {
            StatusRelation::Postcondition
        } else if status.role == Role::Primary
            && status.epoch == previous_epoch
            && status.write_status == AccessStatus::Granted
        {
            StatusRelation::Precondition
        } else {
            StatusRelation::conflicting("revoke-writes pre/postcondition is not exact")
        }
    }

    fn action_kind() -> DurableActionKind {
        DurableActionKind::RevokeWrite
    }

    fn postcondition() -> DurablePostconditionStatus {
        postcondition(DurablePostconditionKind::WriteRevoked, None)
    }

    fn action_has_fixed_semantics(
        action: &DurableReplicaAction,
        _: &StablePartitionSnapshotStatus,
        _: &DirectSwitchoverDefinition,
    ) -> bool {
        matches!(action, DurableReplicaAction::RevokeWriteStatus)
    }
}

impl ReplicaOperation for DemoteOldPrimaryActivity {
    fn request<'a>(
        input: &'a DemoteOldPrimaryInput,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<ReplicaRequest<'a>, String> {
        validate_target(
            input.old_primary_id,
            &input.old_primary_instance_id,
            definition.old_primary_id,
            definition,
        )?;
        Ok(fixed_replica_request(
            DIRECT_SWITCHOVER_CONTRACT_VERSION,
            &definition.execution_id,
            2,
            input.old_primary_id,
            &input.old_primary_instance_id,
            definition.target_snapshot.epoch.clone(),
            definition.target_snapshot.clone(),
            deadline_unix_seconds,
        ))
    }

    fn action(
        _: &ReplicaRequest<'_>,
        definition: &DirectSwitchoverDefinition,
        _: &OperationObservations,
    ) -> Result<DurableReplicaAction, String> {
        Ok(DurableReplicaAction::ChangeRole {
            epoch: epoch(&definition.target_snapshot.epoch),
            role: Role::ActiveSecondary,
        })
    }

    fn status_relation(
        _: &ReplicaRequest<'_>,
        definition: &DirectSwitchoverDefinition,
        status: &kuberic_core::types::ReplicaStatusInfo,
    ) -> StatusRelation {
        if status.role == Role::ActiveSecondary
            && status.epoch == epoch(&definition.target_snapshot.epoch)
        {
            StatusRelation::Postcondition
        } else if status.role == Role::Primary
            && status.epoch == epoch(&definition.previous_snapshot.epoch)
            && status.write_status == AccessStatus::ReconfigurationPending
        {
            StatusRelation::Precondition
        } else {
            StatusRelation::conflicting("demote-old-primary pre/postcondition is not exact")
        }
    }

    fn action_kind() -> DurableActionKind {
        DurableActionKind::DemoteOldPrimary
    }

    fn postcondition() -> DurablePostconditionStatus {
        postcondition(
            DurablePostconditionKind::Role,
            Some(StableReplicaRoleStatus::ActiveSecondary),
        )
    }

    fn action_has_fixed_semantics(
        action: &DurableReplicaAction,
        _: &StablePartitionSnapshotStatus,
        definition: &DirectSwitchoverDefinition,
    ) -> bool {
        matches!(
            action,
            DurableReplicaAction::ChangeRole {
                epoch: value,
                role: Role::ActiveSecondary,
            } if *value == epoch(&definition.target_snapshot.epoch)
        )
    }
}

impl ReplicaOperation for PromoteTargetActivity {
    fn request<'a>(
        input: &'a PromoteTargetInput,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<ReplicaRequest<'a>, String> {
        validate_target(
            input.target_primary_id,
            &input.target_primary_instance_id,
            definition.target_primary_id,
            definition,
        )?;
        Ok(fixed_replica_request(
            DIRECT_SWITCHOVER_CONTRACT_VERSION,
            &definition.execution_id,
            3,
            input.target_primary_id,
            &input.target_primary_instance_id,
            definition.previous_snapshot.epoch.clone(),
            definition.target_snapshot.clone(),
            deadline_unix_seconds,
        ))
    }

    fn action(
        _: &ReplicaRequest<'_>,
        definition: &DirectSwitchoverDefinition,
        _: &OperationObservations,
    ) -> Result<DurableReplicaAction, String> {
        Ok(DurableReplicaAction::ChangeRole {
            epoch: epoch(&definition.target_snapshot.epoch),
            role: Role::Primary,
        })
    }

    fn status_relation(
        _: &ReplicaRequest<'_>,
        definition: &DirectSwitchoverDefinition,
        status: &kuberic_core::types::ReplicaStatusInfo,
    ) -> StatusRelation {
        if status.role == Role::Primary && status.epoch == epoch(&definition.target_snapshot.epoch)
        {
            StatusRelation::Postcondition
        } else if status.role == Role::ActiveSecondary
            && status.epoch == epoch(&definition.previous_snapshot.epoch)
        {
            StatusRelation::Precondition
        } else {
            StatusRelation::conflicting("promote-target pre/postcondition is not exact")
        }
    }

    fn action_kind() -> DurableActionKind {
        DurableActionKind::PromoteTarget
    }

    fn postcondition() -> DurablePostconditionStatus {
        postcondition(
            DurablePostconditionKind::Role,
            Some(StableReplicaRoleStatus::Primary),
        )
    }

    fn action_has_fixed_semantics(
        action: &DurableReplicaAction,
        _: &StablePartitionSnapshotStatus,
        definition: &DirectSwitchoverDefinition,
    ) -> bool {
        matches!(
            action,
            DurableReplicaAction::ChangeRole {
                epoch: value,
                role: Role::Primary,
            } if *value == epoch(&definition.target_snapshot.epoch)
        )
    }
}

impl ReplicaOperation for DistributeReplicaEpochActivity {
    fn request<'a>(
        input: &'a DistributeReplicaEpochInput,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<ReplicaRequest<'a>, String> {
        let index = usize::from(input.distribution_index);
        let target_id = *definition
            .normal_epoch_distribution_ids()
            .get(index)
            .ok_or_else(|| "normal epoch distribution index is outside membership".to_string())?;
        validate_target(
            input.replica_id,
            &input.replica_instance_id,
            target_id,
            definition,
        )?;
        Ok(fixed_replica_request(
            DIRECT_SWITCHOVER_CONTRACT_VERSION,
            &definition.execution_id,
            100 + u32::from(input.distribution_index),
            input.replica_id,
            &input.replica_instance_id,
            definition.target_snapshot.epoch.clone(),
            definition.target_snapshot.clone(),
            deadline_unix_seconds,
        ))
    }

    fn action(
        _: &ReplicaRequest<'_>,
        definition: &DirectSwitchoverDefinition,
        _: &OperationObservations,
    ) -> Result<DurableReplicaAction, String> {
        Ok(DurableReplicaAction::UpdateEpoch {
            epoch: epoch(&definition.target_snapshot.epoch),
        })
    }

    fn status_relation(
        _: &ReplicaRequest<'_>,
        definition: &DirectSwitchoverDefinition,
        status: &kuberic_core::types::ReplicaStatusInfo,
    ) -> StatusRelation {
        epoch_distribution_relation(status, definition)
    }

    fn action_kind() -> DurableActionKind {
        DurableActionKind::UpdateSecondaryEpoch
    }

    fn postcondition() -> DurablePostconditionStatus {
        postcondition(
            DurablePostconditionKind::Epoch,
            Some(StableReplicaRoleStatus::ActiveSecondary),
        )
    }

    fn action_has_fixed_semantics(
        action: &DurableReplicaAction,
        _: &StablePartitionSnapshotStatus,
        definition: &DirectSwitchoverDefinition,
    ) -> bool {
        matches!(
            action,
            DurableReplicaAction::UpdateEpoch { epoch: value }
                if *value == epoch(&definition.target_snapshot.epoch)
        )
    }
}

impl ReplicaOperation for InstallTargetCatchUpConfigurationActivity {
    fn request<'a>(
        input: &'a InstallTargetCatchUpConfigurationInput,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<ReplicaRequest<'a>, String> {
        target_replica_request(
            DIRECT_SWITCHOVER_CONTRACT_VERSION,
            &definition.execution_id,
            1000,
            input.target_primary_id,
            &input.target_primary_instance_id,
            deadline_unix_seconds,
            definition,
        )
    }

    fn action(
        request: &ReplicaRequest<'_>,
        definition: &DirectSwitchoverDefinition,
        observations: &OperationObservations,
    ) -> Result<DurableReplicaAction, String> {
        Ok(DurableReplicaAction::UpdateCatchUpConfiguration {
            current: config_for_snapshot(&request.desired_snapshot, observations)?,
            previous: config_for_snapshot(&definition.previous_snapshot, observations)?,
        })
    }

    fn status_relation(
        request: &ReplicaRequest<'_>,
        definition: &DirectSwitchoverDefinition,
        status: &kuberic_core::types::ReplicaStatusInfo,
    ) -> StatusRelation {
        configuration_relation(
            status,
            &configuration_status(ReplicaConfigurationMode::CatchUp, &request.desired_snapshot),
            None,
            true,
            epoch(&definition.target_snapshot.epoch),
        )
    }

    fn action_kind() -> DurableActionKind {
        DurableActionKind::UpdateCatchUpConfiguration
    }

    fn postcondition() -> DurablePostconditionStatus {
        postcondition(DurablePostconditionKind::CatchUpConfiguration, None)
    }

    fn action_has_fixed_semantics(
        action: &DurableReplicaAction,
        desired_snapshot: &StablePartitionSnapshotStatus,
        definition: &DirectSwitchoverDefinition,
    ) -> bool {
        matches!(
            action,
            DurableReplicaAction::UpdateCatchUpConfiguration { current, previous }
                if config_matches_snapshot(current, desired_snapshot)
                    && config_matches_snapshot(previous, &definition.previous_snapshot)
        )
    }
}

impl ReplicaOperation for WaitTargetWriteQuorumActivity {
    fn request<'a>(
        input: &'a WaitTargetWriteQuorumInput,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<ReplicaRequest<'a>, String> {
        target_replica_request(
            DIRECT_SWITCHOVER_CONTRACT_VERSION,
            &definition.execution_id,
            1001,
            input.target_primary_id,
            &input.target_primary_instance_id,
            deadline_unix_seconds,
            definition,
        )
    }

    fn action(
        _: &ReplicaRequest<'_>,
        _: &DirectSwitchoverDefinition,
        _: &OperationObservations,
    ) -> Result<DurableReplicaAction, String> {
        Ok(DurableReplicaAction::WaitForCatchUpQuorum {
            mode: ReplicaSetQuorumMode::Write,
        })
    }

    fn status_relation(
        _: &ReplicaRequest<'_>,
        definition: &DirectSwitchoverDefinition,
        status: &kuberic_core::types::ReplicaStatusInfo,
    ) -> StatusRelation {
        if status.role == Role::Primary && status.epoch == epoch(&definition.target_snapshot.epoch)
        {
            StatusRelation::Precondition
        } else {
            StatusRelation::conflicting("write-quorum wait target is not exact primary")
        }
    }

    fn action_kind() -> DurableActionKind {
        DurableActionKind::WaitForCatchUpQuorum
    }

    fn postcondition() -> DurablePostconditionStatus {
        postcondition(DurablePostconditionKind::CatchUpQuorum, None)
    }

    fn action_has_fixed_semantics(
        action: &DurableReplicaAction,
        _: &StablePartitionSnapshotStatus,
        _: &DirectSwitchoverDefinition,
    ) -> bool {
        matches!(
            action,
            DurableReplicaAction::WaitForCatchUpQuorum {
                mode: ReplicaSetQuorumMode::Write,
            }
        )
    }
}

impl ReplicaOperation for InstallTargetCurrentConfigurationActivity {
    fn request<'a>(
        input: &'a InstallTargetCurrentConfigurationInput,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<ReplicaRequest<'a>, String> {
        target_replica_request(
            DIRECT_SWITCHOVER_CONTRACT_VERSION,
            &definition.execution_id,
            1002,
            input.target_primary_id,
            &input.target_primary_instance_id,
            deadline_unix_seconds,
            definition,
        )
    }

    fn action(
        request: &ReplicaRequest<'_>,
        _: &DirectSwitchoverDefinition,
        observations: &OperationObservations,
    ) -> Result<DurableReplicaAction, String> {
        Ok(DurableReplicaAction::UpdateCurrentConfiguration {
            current: config_for_snapshot(&request.desired_snapshot, observations)?,
        })
    }

    fn status_relation(
        request: &ReplicaRequest<'_>,
        definition: &DirectSwitchoverDefinition,
        status: &kuberic_core::types::ReplicaStatusInfo,
    ) -> StatusRelation {
        let catch_up =
            configuration_status(ReplicaConfigurationMode::CatchUp, &request.desired_snapshot);
        configuration_relation(
            status,
            &configuration_status(ReplicaConfigurationMode::Current, &request.desired_snapshot),
            Some(&catch_up),
            false,
            epoch(&definition.target_snapshot.epoch),
        )
    }

    fn action_kind() -> DurableActionKind {
        DurableActionKind::UpdateCurrentConfiguration
    }

    fn postcondition() -> DurablePostconditionStatus {
        postcondition(DurablePostconditionKind::CurrentConfiguration, None)
    }

    fn action_has_fixed_semantics(
        action: &DurableReplicaAction,
        desired_snapshot: &StablePartitionSnapshotStatus,
        _: &DirectSwitchoverDefinition,
    ) -> bool {
        matches!(
            action,
            DurableReplicaAction::UpdateCurrentConfiguration { current }
                if config_matches_snapshot(current, desired_snapshot)
        )
    }
}

impl ReplicaOperation for RestorePreviousCurrentConfigurationActivity {
    fn request<'a>(
        input: &'a RestorePreviousCurrentConfigurationInput,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<ReplicaRequest<'a>, String> {
        old_primary_replica_request(
            DIRECT_SWITCHOVER_CONTRACT_VERSION,
            &definition.execution_id,
            1500,
            input.old_primary_id,
            &input.old_primary_instance_id,
            definition.previous_snapshot.epoch.clone(),
            definition.previous_snapshot.clone(),
            deadline_unix_seconds,
            definition,
        )
    }

    fn action(
        request: &ReplicaRequest<'_>,
        _: &DirectSwitchoverDefinition,
        observations: &OperationObservations,
    ) -> Result<DurableReplicaAction, String> {
        Ok(DurableReplicaAction::UpdateCurrentConfiguration {
            current: config_for_snapshot(&request.desired_snapshot, observations)?,
        })
    }

    fn status_relation(
        request: &ReplicaRequest<'_>,
        definition: &DirectSwitchoverDefinition,
        status: &kuberic_core::types::ReplicaStatusInfo,
    ) -> StatusRelation {
        let desired_current =
            configuration_status(ReplicaConfigurationMode::Current, &request.desired_snapshot);
        if status.role != Role::Primary
            || status.epoch != epoch(&definition.previous_snapshot.epoch)
        {
            StatusRelation::conflicting(
                "previous-configuration restore target is not exact primary",
            )
        } else if status.configuration.as_ref() == Some(&desired_current)
            && status.write_status == AccessStatus::Granted
        {
            StatusRelation::Postcondition
        } else if status.configuration.as_ref() == Some(&desired_current)
            && status.write_status == AccessStatus::ReconfigurationPending
        {
            StatusRelation::Precondition
        } else {
            StatusRelation::conflicting(
                "previous-configuration restore pre/postcondition is not exact",
            )
        }
    }

    fn action_kind() -> DurableActionKind {
        DurableActionKind::RestorePreviousConfiguration
    }

    fn postcondition() -> DurablePostconditionStatus {
        postcondition(DurablePostconditionKind::CurrentConfiguration, None)
    }

    fn action_has_fixed_semantics(
        action: &DurableReplicaAction,
        desired_snapshot: &StablePartitionSnapshotStatus,
        _: &DirectSwitchoverDefinition,
    ) -> bool {
        matches!(
            action,
            DurableReplicaAction::UpdateCurrentConfiguration { current }
                if config_matches_snapshot(current, desired_snapshot)
        )
    }
}

impl ReplicaOperation for CompensatePromoteOldPrimaryActivity {
    fn request<'a>(
        input: &'a CompensatePromoteOldPrimaryInput,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<ReplicaRequest<'a>, String> {
        old_primary_replica_request(
            DIRECT_SWITCHOVER_CONTRACT_VERSION,
            &definition.execution_id,
            2000,
            input.old_primary_id,
            &input.old_primary_instance_id,
            definition.target_snapshot.epoch.clone(),
            definition.compensation_snapshot(),
            deadline_unix_seconds,
            definition,
        )
    }

    fn action(
        _: &ReplicaRequest<'_>,
        definition: &DirectSwitchoverDefinition,
        _: &OperationObservations,
    ) -> Result<DurableReplicaAction, String> {
        Ok(DurableReplicaAction::ChangeRole {
            epoch: epoch(&definition.target_snapshot.epoch),
            role: Role::Primary,
        })
    }

    fn status_relation(
        _: &ReplicaRequest<'_>,
        definition: &DirectSwitchoverDefinition,
        status: &kuberic_core::types::ReplicaStatusInfo,
    ) -> StatusRelation {
        if status.role == Role::Primary && status.epoch == epoch(&definition.target_snapshot.epoch)
        {
            StatusRelation::Postcondition
        } else if status.role == Role::ActiveSecondary
            && status.epoch == epoch(&definition.target_snapshot.epoch)
        {
            StatusRelation::Precondition
        } else {
            StatusRelation::conflicting("compensating promotion pre/postcondition is not exact")
        }
    }

    fn action_kind() -> DurableActionKind {
        DurableActionKind::CompensatePromoteOldPrimary
    }

    fn postcondition() -> DurablePostconditionStatus {
        postcondition(
            DurablePostconditionKind::Role,
            Some(StableReplicaRoleStatus::Primary),
        )
    }

    fn action_has_fixed_semantics(
        action: &DurableReplicaAction,
        _: &StablePartitionSnapshotStatus,
        definition: &DirectSwitchoverDefinition,
    ) -> bool {
        matches!(
            action,
            DurableReplicaAction::ChangeRole {
                epoch: value,
                role: Role::Primary,
            } if *value == epoch(&definition.target_snapshot.epoch)
        )
    }
}

impl ReplicaOperation for CompensateDistributeReplicaEpochActivity {
    fn request<'a>(
        input: &'a CompensateDistributeReplicaEpochInput,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<ReplicaRequest<'a>, String> {
        let index = usize::from(input.distribution_index);
        let target_id = *definition
            .compensation_epoch_distribution_ids()
            .get(index)
            .ok_or_else(|| {
                "compensation epoch distribution index is outside membership".to_string()
            })?;
        validate_target(
            input.replica_id,
            &input.replica_instance_id,
            target_id,
            definition,
        )?;
        Ok(fixed_replica_request(
            DIRECT_SWITCHOVER_CONTRACT_VERSION,
            &definition.execution_id,
            2100 + u32::from(input.distribution_index),
            input.replica_id,
            &input.replica_instance_id,
            definition.target_snapshot.epoch.clone(),
            definition.compensation_snapshot(),
            deadline_unix_seconds,
        ))
    }

    fn action(
        _: &ReplicaRequest<'_>,
        definition: &DirectSwitchoverDefinition,
        _: &OperationObservations,
    ) -> Result<DurableReplicaAction, String> {
        Ok(DurableReplicaAction::UpdateEpoch {
            epoch: epoch(&definition.target_snapshot.epoch),
        })
    }

    fn status_relation(
        _: &ReplicaRequest<'_>,
        definition: &DirectSwitchoverDefinition,
        status: &kuberic_core::types::ReplicaStatusInfo,
    ) -> StatusRelation {
        epoch_distribution_relation(status, definition)
    }

    fn action_kind() -> DurableActionKind {
        DurableActionKind::CompensateUpdateSecondaryEpoch
    }

    fn postcondition() -> DurablePostconditionStatus {
        postcondition(
            DurablePostconditionKind::Epoch,
            Some(StableReplicaRoleStatus::ActiveSecondary),
        )
    }

    fn action_has_fixed_semantics(
        action: &DurableReplicaAction,
        _: &StablePartitionSnapshotStatus,
        definition: &DirectSwitchoverDefinition,
    ) -> bool {
        matches!(
            action,
            DurableReplicaAction::UpdateEpoch { epoch: value }
                if *value == epoch(&definition.target_snapshot.epoch)
        )
    }
}

impl ReplicaOperation for InstallCompensationCatchUpConfigurationActivity {
    fn request<'a>(
        input: &'a InstallCompensationCatchUpConfigurationInput,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<ReplicaRequest<'a>, String> {
        old_primary_replica_request(
            DIRECT_SWITCHOVER_CONTRACT_VERSION,
            &definition.execution_id,
            2001,
            input.old_primary_id,
            &input.old_primary_instance_id,
            definition.target_snapshot.epoch.clone(),
            definition.compensation_snapshot(),
            deadline_unix_seconds,
            definition,
        )
    }

    fn action(
        request: &ReplicaRequest<'_>,
        definition: &DirectSwitchoverDefinition,
        observations: &OperationObservations,
    ) -> Result<DurableReplicaAction, String> {
        Ok(DurableReplicaAction::UpdateCatchUpConfiguration {
            current: config_for_snapshot(&request.desired_snapshot, observations)?,
            previous: config_for_snapshot(&definition.previous_snapshot, observations)?,
        })
    }

    fn status_relation(
        request: &ReplicaRequest<'_>,
        definition: &DirectSwitchoverDefinition,
        status: &kuberic_core::types::ReplicaStatusInfo,
    ) -> StatusRelation {
        let previous_current = configuration_status(
            ReplicaConfigurationMode::Current,
            &definition.previous_snapshot,
        );
        configuration_relation(
            status,
            &configuration_status(ReplicaConfigurationMode::CatchUp, &request.desired_snapshot),
            Some(&previous_current),
            false,
            epoch(&definition.target_snapshot.epoch),
        )
    }

    fn action_kind() -> DurableActionKind {
        DurableActionKind::CompensateCatchUpConfiguration
    }

    fn postcondition() -> DurablePostconditionStatus {
        postcondition(DurablePostconditionKind::CatchUpConfiguration, None)
    }

    fn action_has_fixed_semantics(
        action: &DurableReplicaAction,
        desired_snapshot: &StablePartitionSnapshotStatus,
        definition: &DirectSwitchoverDefinition,
    ) -> bool {
        matches!(
            action,
            DurableReplicaAction::UpdateCatchUpConfiguration { current, previous }
                if config_matches_snapshot(current, desired_snapshot)
                    && config_matches_snapshot(previous, &definition.previous_snapshot)
        )
    }
}

impl ReplicaOperation for InstallCompensationCurrentConfigurationActivity {
    fn request<'a>(
        input: &'a InstallCompensationCurrentConfigurationInput,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<ReplicaRequest<'a>, String> {
        old_primary_replica_request(
            DIRECT_SWITCHOVER_CONTRACT_VERSION,
            &definition.execution_id,
            2002,
            input.old_primary_id,
            &input.old_primary_instance_id,
            definition.target_snapshot.epoch.clone(),
            definition.compensation_snapshot(),
            deadline_unix_seconds,
            definition,
        )
    }

    fn action(
        request: &ReplicaRequest<'_>,
        _: &DirectSwitchoverDefinition,
        observations: &OperationObservations,
    ) -> Result<DurableReplicaAction, String> {
        Ok(DurableReplicaAction::UpdateCurrentConfiguration {
            current: config_for_snapshot(&request.desired_snapshot, observations)?,
        })
    }

    fn status_relation(
        request: &ReplicaRequest<'_>,
        definition: &DirectSwitchoverDefinition,
        status: &kuberic_core::types::ReplicaStatusInfo,
    ) -> StatusRelation {
        let catch_up =
            configuration_status(ReplicaConfigurationMode::CatchUp, &request.desired_snapshot);
        configuration_relation(
            status,
            &configuration_status(ReplicaConfigurationMode::Current, &request.desired_snapshot),
            Some(&catch_up),
            false,
            epoch(&definition.target_snapshot.epoch),
        )
    }

    fn action_kind() -> DurableActionKind {
        DurableActionKind::CompensateCurrentConfiguration
    }

    fn postcondition() -> DurablePostconditionStatus {
        postcondition(DurablePostconditionKind::CurrentConfiguration, None)
    }

    fn action_has_fixed_semantics(
        action: &DurableReplicaAction,
        desired_snapshot: &StablePartitionSnapshotStatus,
        _: &DirectSwitchoverDefinition,
    ) -> bool {
        matches!(
            action,
            DurableReplicaAction::UpdateCurrentConfiguration { current }
                if config_matches_snapshot(current, desired_snapshot)
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn target_replica_request<'a>(
    contract_version: u32,
    execution_id: &str,
    sequence: u32,
    target_primary_id: i64,
    target_primary_instance_id: &'a str,
    deadline_unix_seconds: i64,
    definition: &DirectSwitchoverDefinition,
) -> Result<ReplicaRequest<'a>, String> {
    validate_target(
        target_primary_id,
        target_primary_instance_id,
        definition.target_primary_id,
        definition,
    )?;
    Ok(fixed_replica_request(
        contract_version,
        execution_id,
        sequence,
        target_primary_id,
        target_primary_instance_id,
        definition.target_snapshot.epoch.clone(),
        definition.target_snapshot.clone(),
        deadline_unix_seconds,
    ))
}

#[allow(clippy::too_many_arguments)]
fn old_primary_replica_request<'a>(
    contract_version: u32,
    execution_id: &str,
    sequence: u32,
    old_primary_id: i64,
    old_primary_instance_id: &'a str,
    expected_epoch: EpochStatus,
    desired_snapshot: StablePartitionSnapshotStatus,
    deadline_unix_seconds: i64,
    definition: &DirectSwitchoverDefinition,
) -> Result<ReplicaRequest<'a>, String> {
    validate_target(
        old_primary_id,
        old_primary_instance_id,
        definition.old_primary_id,
        definition,
    )?;
    Ok(fixed_replica_request(
        contract_version,
        execution_id,
        sequence,
        old_primary_id,
        old_primary_instance_id,
        expected_epoch,
        desired_snapshot,
        deadline_unix_seconds,
    ))
}

pub(crate) struct LabelRequest<'a> {
    contract_version: u32,
    execution_id: String,
    sequence: u32,
    target_id: i64,
    target_instance_id: &'a str,
    desired_role: &'static str,
    deadline_unix_seconds: i64,
}

pub(crate) trait LabelOperation: SwitchoverActivityContract<Output = EffectApplied> {
    fn request<'a>(
        input: &'a Self::Request,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<LabelRequest<'a>, String>;
}

fn validate_label_request<'a>(
    request: LabelRequest<'a>,
    expected_id: i64,
    definition: &DirectSwitchoverDefinition,
) -> Result<LabelRequest<'a>, String> {
    validate_target(
        request.target_id,
        request.target_instance_id,
        expected_id,
        definition,
    )?;
    Ok(request)
}

impl LabelOperation for PublishTargetPrimaryLabelActivity {
    fn request<'a>(
        input: &'a PublishTargetPrimaryLabelInput,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<LabelRequest<'a>, String> {
        validate_label_request(
            LabelRequest {
                contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
                execution_id: definition.execution_id.clone(),
                sequence: 1003,
                target_id: input.target_primary_id,
                target_instance_id: &input.target_primary_instance_id,
                desired_role: "primary",
                deadline_unix_seconds,
            },
            definition.target_primary_id,
            definition,
        )
    }
}

impl LabelOperation for PublishOldPrimarySecondaryLabelActivity {
    fn request<'a>(
        input: &'a PublishOldPrimarySecondaryLabelInput,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<LabelRequest<'a>, String> {
        validate_label_request(
            LabelRequest {
                contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
                execution_id: definition.execution_id.clone(),
                sequence: 1004,
                target_id: input.old_primary_id,
                target_instance_id: &input.old_primary_instance_id,
                desired_role: "secondary",
                deadline_unix_seconds,
            },
            definition.old_primary_id,
            definition,
        )
    }
}

impl LabelOperation for RestoreOldPrimaryLabelActivity {
    fn request<'a>(
        input: &'a RestoreOldPrimaryLabelInput,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<LabelRequest<'a>, String> {
        validate_label_request(
            LabelRequest {
                contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
                execution_id: definition.execution_id.clone(),
                sequence: 2003,
                target_id: input.old_primary_id,
                target_instance_id: &input.old_primary_instance_id,
                desired_role: "primary",
                deadline_unix_seconds,
            },
            definition.old_primary_id,
            definition,
        )
    }
}

impl LabelOperation for RestoreTargetSecondaryLabelActivity {
    fn request<'a>(
        input: &'a RestoreTargetSecondaryLabelInput,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<LabelRequest<'a>, String> {
        validate_label_request(
            LabelRequest {
                contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
                execution_id: definition.execution_id.clone(),
                sequence: 2004,
                target_id: input.target_primary_id,
                target_instance_id: &input.target_primary_instance_id,
                desired_role: "secondary",
                deadline_unix_seconds,
            },
            definition.target_primary_id,
            definition,
        )
    }
}

fn validate_replica<A: ReplicaOperation>(
    input: &A::Request,
    definition: &DirectSwitchoverDefinition,
    deadline_unix_seconds: i64,
) -> Result<(), String> {
    let request = A::request(input, definition, deadline_unix_seconds)?;
    validate_common(
        request.contract_version,
        &request.execution_id,
        request.deadline_unix_seconds,
        definition,
    )
}

fn validate_replica_command<A: ReplicaOperation>(
    input: &A::Request,
    command: &ReplicaEffectCommand,
    definition: &DirectSwitchoverDefinition,
    deadline_unix_seconds: i64,
) -> Result<(), String> {
    validate_replica::<A>(input, definition, deadline_unix_seconds)?;
    let request = A::request(input, definition, deadline_unix_seconds)?;
    let pending = pending::<A>(&request, Some(command));
    if ReplicaEffectCommand::from_pending(&pending)? != *command
        || command.action_id != action_id(definition, request.sequence)
        || command.target_id != request.target_id
        || command.target_instance_id != request.target_instance_id
        || command.expected_epoch != request.expected_epoch
        || command.desired_postcondition != A::postcondition()
        || command.expected_agent_generation.is_empty()
        || command.expected_control_version == 0
        || command.action_payload.is_empty()
    {
        return Err("prepared direct replica command has invalid exact identity".to_string());
    }
    let action = kuberic_core::grpc::convert::decode_direct_correlated_action_payload(
        &command.action_payload,
    )
    .map_err(|error| format!("decode direct prepared action: {error}"))?;
    if action.signature() != command.action_signature
        || !A::action_has_fixed_semantics(&action, &request.desired_snapshot, definition)
    {
        return Err("prepared direct replica command changed fixed semantics".to_string());
    }
    Ok(())
}

fn evaluate_replica<A: ReplicaOperation>(
    input: &A::Request,
    definition: &DirectSwitchoverDefinition,
    observations: &OperationObservations,
    now: i64,
    deadline_unix_seconds: i64,
) -> Result<DirectEvaluation, String> {
    let request = A::request(input, definition, deadline_unix_seconds)?;
    let Some(observed) = observations.get(&request.target_id) else {
        return deadline_or_wait(request.deadline_unix_seconds, now, || {
            encode_effect_error::<A>(
                EffectErrorKind::UnavailableAtDeadline,
                now,
                format!(
                    "direct switchover replica {} is unavailable at deadline",
                    request.target_id
                ),
            )
        });
    };
    if observed.status.instance_id.as_str() != request.target_instance_id {
        return encode_effect_error::<A>(
            EffectErrorKind::ConflictingEvidence,
            now,
            format!(
                "direct switchover replica {} incarnation changed",
                request.target_id
            ),
        )
        .map(DirectEvaluation::Observe);
    }
    if let Some(recorded) =
        correlated_action_observation(&observed.status, &action_id(definition, request.sequence))
    {
        let action = match A::action(&request, definition, observations) {
            Ok(action) => action,
            Err(error) => {
                return deadline_or_wait(request.deadline_unix_seconds, now, || {
                    encode_effect_error::<A>(EffectErrorKind::DeadlineExceeded, now, error)
                });
            }
        };
        if recorded.signature != action.signature() {
            return encode_effect_error::<A>(
                EffectErrorKind::ConflictingEvidence,
                now,
                "correlated action signature conflicts with direct request".to_string(),
            )
            .map(DirectEvaluation::Observe);
        }
        return match recorded.state {
            DurableActionState::Completed => {
                encode_effect_applied::<A>(now).map(DirectEvaluation::Observe)
            }
            DurableActionState::Failed => encode_effect_error::<A>(
                EffectErrorKind::DomainFailure,
                now,
                recorded
                    .error
                    .clone()
                    .unwrap_or_else(|| "correlated direct switchover action failed".to_string()),
            )
            .map(DirectEvaluation::Observe),
            DurableActionState::Scheduled | DurableActionState::InProgress => {
                deadline_or_wait(request.deadline_unix_seconds, now, || {
                    encode_effect_error::<A>(
                        EffectErrorKind::DeadlineExceeded,
                        now,
                        "correlated direct switchover action reached its deadline".to_string(),
                    )
                })
            }
        };
    }
    match A::status_relation(&request, definition, &observed.status) {
        StatusRelation::Postcondition => {
            encode_effect_applied::<A>(now).map(DirectEvaluation::Observe)
        }
        StatusRelation::Precondition if now >= request.deadline_unix_seconds => {
            encode_effect_error::<A>(
                EffectErrorKind::DeadlineExceeded,
                now,
                format!(
                    "direct switchover action {:?} reached its deadline",
                    A::action_kind()
                ),
            )
            .map(DirectEvaluation::Observe)
        }
        StatusRelation::Precondition => {
            let action = match A::action(&request, definition, observations) {
                Ok(action) => action,
                Err(_) => return Ok(DirectEvaluation::AwaitEvidence),
            };
            Ok(DirectEvaluation::DispatchReplica {
                action,
                pending: Box::new(pending::<A>(&request, None)),
            })
        }
        StatusRelation::Conflicting(message) => {
            encode_effect_error::<A>(EffectErrorKind::ConflictingEvidence, now, message)
                .map(DirectEvaluation::Observe)
        }
    }
}

fn evaluate_quarantined_replica<A: ReplicaOperation>(
    input: &A::Request,
    command: &Option<ReplicaEffectCommand>,
    definition: &DirectSwitchoverDefinition,
    observations: &OperationObservations,
    now: i64,
    deadline_unix_seconds: i64,
) -> Result<DirectEvaluation, String> {
    let request = A::request(input, definition, deadline_unix_seconds)?;
    let Some(command) = command.as_ref() else {
        return match evaluate_replica::<A>(
            input,
            definition,
            observations,
            now,
            deadline_unix_seconds,
        )? {
            observation @ DirectEvaluation::Observe(_) => Ok(observation),
            DirectEvaluation::AwaitEvidence
            | DirectEvaluation::DispatchReplica { .. }
            | DirectEvaluation::DispatchLabel => Ok(DirectEvaluation::AwaitEvidence),
        };
    };
    let Some(observed) = observations.get(&request.target_id) else {
        return Ok(DirectEvaluation::AwaitEvidence);
    };
    if observed.status.instance_id.as_str() != request.target_instance_id {
        return Ok(DirectEvaluation::AwaitEvidence);
    }
    if let Some(recorded) = correlated_action_observation(&observed.status, &command.action_id) {
        if recorded.signature == command.action_signature {
            match recorded.state {
                DurableActionState::Completed => {
                    return encode_effect_applied::<A>(now).map(DirectEvaluation::Observe);
                }
                DurableActionState::Failed => {
                    return encode_effect_error::<A>(
                        EffectErrorKind::DomainFailure,
                        now,
                        recorded.error.clone().unwrap_or_else(|| {
                            "correlated direct switchover action failed".to_string()
                        }),
                    )
                    .map(DirectEvaluation::Observe);
                }
                DurableActionState::Scheduled | DurableActionState::InProgress => {}
            }
        }
    }
    if matches!(
        A::status_relation(&request, definition, &observed.status),
        StatusRelation::Postcondition
    ) {
        return encode_effect_applied::<A>(now).map(DirectEvaluation::Observe);
    }
    if command_generation_change_proves_no_admission(
        &command.expected_agent_generation,
        &command.action_id,
        &observed.status,
    ) {
        return encode_effect_outcome::<A>(EffectOutcome::ProvenNoAdmission)
            .map(DirectEvaluation::Observe);
    }
    Ok(DirectEvaluation::AwaitEvidence)
}

fn pending<A: ReplicaOperation>(
    request: &ReplicaRequest<'_>,
    command: Option<&ReplicaEffectCommand>,
) -> PendingActionStatus {
    let mut pending = PendingActionStatus {
        action_id: request_action_id(request),
        sequence: request.sequence,
        kind: A::action_kind(),
        target_id: request.target_id,
        target_instance_id: request.target_instance_id.to_string(),
        expected_epoch: request.expected_epoch.clone(),
        desired_postcondition: A::postcondition(),
        attempts: 0,
        deadline_unix_seconds: request.deadline_unix_seconds,
        last_error: None,
        dispatch_authorized: command.is_some(),
        dispatch_agent_generation: None,
        dispatch_agent_control_version: None,
        dispatch_observed_runtime_epoch: None,
        dispatch_action_payload: String::new(),
    };
    if let Some(command) = command {
        pending.dispatch_agent_generation = Some(command.expected_agent_generation.clone());
        pending.dispatch_agent_control_version = Some(command.expected_control_version);
        pending.dispatch_observed_runtime_epoch = Some(command.observed_runtime_epoch.clone());
        pending.dispatch_action_payload = command.action_payload.clone();
    }
    pending
}

fn request_action_id(request: &ReplicaRequest<'_>) -> String {
    format!("{}:{}", request.execution_id, request.sequence)
}

fn action_id(definition: &DirectSwitchoverDefinition, sequence: u32) -> String {
    format!("{}:{sequence}", definition.execution_id)
}

fn validate_label<A: LabelOperation>(
    input: &A::Request,
    definition: &DirectSwitchoverDefinition,
    deadline_unix_seconds: i64,
) -> Result<(), String> {
    let request = A::request(input, definition, deadline_unix_seconds)?;
    validate_common(
        request.contract_version,
        &request.execution_id,
        request.deadline_unix_seconds,
        definition,
    )
}

fn validate_label_command<A: LabelOperation>(
    input: &A::Request,
    command: &LabelEffectCommand,
    definition: &DirectSwitchoverDefinition,
    deadline_unix_seconds: i64,
) -> Result<(), String> {
    validate_label::<A>(input, definition, deadline_unix_seconds)?;
    let request = A::request(input, definition, deadline_unix_seconds)?;
    if command.target_id != request.target_id
        || command.expected_uid != request.target_instance_id
        || command.role != request.desired_role
        || command.pod_name.is_empty()
        || !command.has_valid_identity_signature()
    {
        return Err("prepared direct label command has invalid exact identity".to_string());
    }
    Ok(())
}

fn evaluate_label<A: LabelOperation>(
    input: &A::Request,
    definition: &DirectSwitchoverDefinition,
    observations: &OperationObservations,
    now: i64,
    deadline_unix_seconds: i64,
) -> Result<DirectEvaluation, String> {
    let request = A::request(input, definition, deadline_unix_seconds)?;
    let Some(observed) = observations.get(&request.target_id) else {
        return deadline_or_wait(request.deadline_unix_seconds, now, || {
            encode_effect_error::<A>(
                EffectErrorKind::UnavailableAtDeadline,
                now,
                format!(
                    "direct switchover label target {} is unavailable at deadline",
                    request.target_id
                ),
            )
        });
    };
    if observed.status.instance_id.as_str() != request.target_instance_id {
        return encode_effect_error::<A>(
            EffectErrorKind::ConflictingEvidence,
            now,
            "direct switchover label target incarnation changed".to_string(),
        )
        .map(DirectEvaluation::Observe);
    }
    let expected_role = label_role(request.desired_role);
    if observed.status.epoch != epoch(&definition.target_snapshot.epoch)
        || observed.status.role != expected_role
    {
        return encode_effect_error::<A>(
            EffectErrorKind::ConflictingEvidence,
            now,
            "direct switchover label target runtime role is not exact".to_string(),
        )
        .map(DirectEvaluation::Observe);
    }
    if observed.pod_role_label.as_deref() == Some(request.desired_role) {
        encode_effect_applied::<A>(now).map(DirectEvaluation::Observe)
    } else if now >= request.deadline_unix_seconds {
        encode_effect_error::<A>(
            EffectErrorKind::DeadlineExceeded,
            now,
            format!(
                "direct switchover label action {} reached its deadline",
                request.sequence
            ),
        )
        .map(DirectEvaluation::Observe)
    } else {
        Ok(DirectEvaluation::DispatchLabel)
    }
}

fn label_role(role: &str) -> Role {
    if role == "primary" {
        Role::Primary
    } else {
        Role::ActiveSecondary
    }
}

fn postcondition(
    kind: DurablePostconditionKind,
    role: Option<StableReplicaRoleStatus>,
) -> DurablePostconditionStatus {
    DurablePostconditionStatus { kind, role }
}

fn epoch_distribution_relation(
    status: &kuberic_core::types::ReplicaStatusInfo,
    definition: &DirectSwitchoverDefinition,
) -> StatusRelation {
    if status.role == Role::ActiveSecondary
        && status.epoch == epoch(&definition.target_snapshot.epoch)
    {
        StatusRelation::Postcondition
    } else if status.role == Role::ActiveSecondary
        && status.epoch == epoch(&definition.previous_snapshot.epoch)
    {
        StatusRelation::Precondition
    } else {
        StatusRelation::conflicting("replica-epoch distribution pre/postcondition is not exact")
    }
}

pub(crate) enum StatusRelation {
    Precondition,
    Postcondition,
    Conflicting(String),
}

impl StatusRelation {
    fn conflicting(message: &str) -> Self {
        Self::Conflicting(message.to_string())
    }
}

fn validate_common(
    contract_version: u32,
    execution_id: &str,
    deadline_unix_seconds: i64,
    definition: &DirectSwitchoverDefinition,
) -> Result<(), String> {
    if contract_version != DIRECT_SWITCHOVER_CONTRACT_VERSION
        || execution_id != definition.execution_id
        || deadline_unix_seconds <= 0
    {
        return Err("direct activity identity, version, or deadline is invalid".to_string());
    }
    Ok(())
}

fn encode_effect_outcome<A>(outcome: EffectOutcome<A::Output>) -> Result<ExactBytes, String>
where
    A: SwitchoverActivityContract,
{
    encode_activity_result::<EncodedSwitchoverOutcome<A>>(&outcome).map_err(|error| {
        format!(
            "encode {} result: {error}",
            <A as SwitchoverActivityContract>::NAME
        )
    })
}

fn encode_effect_applied<A>(observed_at_unix_seconds: i64) -> Result<ExactBytes, String>
where
    A: SwitchoverActivityContract<Output = EffectApplied>,
{
    encode_effect_outcome::<A>(EffectOutcome::Applied(EffectApplied {
        observed_at_unix_seconds,
    }))
}

fn encode_effect_error<A>(
    kind: EffectErrorKind,
    observed_at_unix_seconds: i64,
    message: String,
) -> Result<ExactBytes, String>
where
    A: SwitchoverActivityContract<Output = EffectApplied>,
{
    let error = effect_error::<A>(kind, observed_at_unix_seconds, message)?;
    let outcome = match kind {
        EffectErrorKind::ProvenNoAdmission => EffectOutcome::ProvenNoAdmission,
        EffectErrorKind::DomainFailure => EffectOutcome::DomainFailure(error),
        EffectErrorKind::DeadlineExceeded => EffectOutcome::DeadlineExceeded(error),
        EffectErrorKind::UnavailableAtDeadline => EffectOutcome::UnavailableAtDeadline(error),
        EffectErrorKind::ConflictingEvidence => EffectOutcome::ConflictingEvidence(error),
    };
    encode_effect_outcome::<A>(outcome)
}

fn effect_error<A: SwitchoverActivityContract>(
    kind: EffectErrorKind,
    observed_at_unix_seconds: i64,
    message: String,
) -> Result<BoundedEffectError, String> {
    BoundedEffectError::observed_at(
        kind,
        super::bounded_utf8(&message, super::SWITCHOVER_MAX_ERROR_BYTES),
        observed_at_unix_seconds,
        <A as SwitchoverActivityContract>::MAX_ERROR_MESSAGE_BYTES,
    )
    .map_err(|error| {
        format!(
            "bound {} activity error: {error}",
            <A as SwitchoverActivityContract>::NAME
        )
    })
}

fn decode_evaluation<A>(
    evaluation: DirectEvaluation,
) -> Result<Option<EffectOutcome<A::Output>>, String>
where
    A: SwitchoverActivityContract,
{
    match evaluation {
        DirectEvaluation::Observe(result) => {
            decode_activity_result::<EncodedSwitchoverOutcome<A>>(&result)
                .map(Some)
                .map_err(|error| format!("decode {} observation: {error}", A::NAME))
        }
        DirectEvaluation::AwaitEvidence
        | DirectEvaluation::DispatchReplica { .. }
        | DirectEvaluation::DispatchLabel => Ok(None),
    }
}

pub(crate) enum OrdinaryActivityStep<O> {
    Complete(EffectOutcome<O>),
    AwaitEvidence { authoritative_pending: bool },
    DispatchReplica(ReplicaEffectCommand),
    DispatchLabel(LabelEffectCommand),
}

pub(crate) fn evaluate_ordinary_replica<A>(
    input: &A::Request,
    activity: &kuberic_durable_execution::LogicalActivityId,
    definition: &DirectSwitchoverDefinition,
    observations: &OperationObservations,
    addressed_instances: &BTreeMap<i64, ReplicaInstanceId>,
    now: i64,
    deadline_unix_seconds: i64,
) -> Result<OrdinaryActivityStep<A::Output>, String>
where
    A: ReplicaOperation,
{
    validate_replica::<A>(input, definition, deadline_unix_seconds)?;
    match evaluate_replica::<A>(input, definition, observations, now, deadline_unix_seconds)? {
        DirectEvaluation::Observe(result) => {
            decode_activity_result::<EncodedSwitchoverOutcome<A>>(&result)
                .map(OrdinaryActivityStep::Complete)
                .map_err(|error| format!("decode {} observation: {error}", A::NAME))
        }
        DirectEvaluation::AwaitEvidence => {
            let request = A::request(input, definition, deadline_unix_seconds)?;
            let operation_sequence = request.sequence.to_string();
            let action_id = format!(
                "{}:{}:{}",
                super::encode_execution_id(activity.execution_id()),
                activity.sequence(),
                operation_sequence,
            );
            let authoritative_pending = observations
                .get(&request.target_id)
                .and_then(|observed| correlated_action_observation(&observed.status, &action_id))
                .is_some_and(|recorded| {
                    matches!(
                        recorded.state,
                        DurableActionState::Scheduled | DurableActionState::InProgress
                    )
                });
            Ok(OrdinaryActivityStep::AwaitEvidence {
                authoritative_pending,
            })
        }
        DirectEvaluation::DispatchReplica { action, pending } => {
            let observed = observations
                .get(&pending.target_id)
                .ok_or_else(|| "replica observation disappeared during invocation".to_string())?;
            let addressed = addressed_instances
                .get(&pending.target_id)
                .ok_or_else(|| "replica address disappeared during invocation".to_string())?;
            let (_, mut command) =
                prepare_replica_effect_command(&pending, &observed.status, addressed, &action)
                    .map_err(|error| format!("prepare ordinary replica action: {error:?}"))?;
            validate_replica_command::<A>(input, &command, definition, deadline_unix_seconds)?;
            let operation_sequence = command
                .action_id
                .rsplit(':')
                .next()
                .unwrap_or("0")
                .to_string();
            command.action_id = format!(
                "{}:{}:{}",
                super::encode_execution_id(activity.execution_id()),
                activity.sequence(),
                operation_sequence,
            );
            Ok(OrdinaryActivityStep::DispatchReplica(command))
        }
        DirectEvaluation::DispatchLabel => {
            Err("replica activity produced a label dispatch".to_string())
        }
    }
}

pub(crate) fn evaluate_ordinary_label<A>(
    input: &A::Request,
    definition: &DirectSwitchoverDefinition,
    observations: &OperationObservations,
    now: i64,
    deadline_unix_seconds: i64,
) -> Result<OrdinaryActivityStep<A::Output>, String>
where
    A: LabelOperation,
{
    validate_label::<A>(input, definition, deadline_unix_seconds)?;
    match evaluate_label::<A>(input, definition, observations, now, deadline_unix_seconds)? {
        DirectEvaluation::Observe(result) => {
            decode_activity_result::<EncodedSwitchoverOutcome<A>>(&result)
                .map(OrdinaryActivityStep::Complete)
                .map_err(|error| format!("decode {} observation: {error}", A::NAME))
        }
        DirectEvaluation::AwaitEvidence => Ok(OrdinaryActivityStep::AwaitEvidence {
            authoritative_pending: false,
        }),
        DirectEvaluation::DispatchLabel => {
            let request = A::request(input, definition, deadline_unix_seconds)?;
            let observed = observations
                .get(&request.target_id)
                .ok_or_else(|| "label observation disappeared during invocation".to_string())?;
            if observed.status.instance_id.as_str() != request.target_instance_id {
                return Err("label target incarnation changed during invocation".to_string());
            }
            let command = LabelEffectCommand::new(
                request.target_id,
                observed.pod_name.clone(),
                request.target_instance_id.to_string(),
                request.desired_role.to_string(),
            );
            validate_label_command::<A>(input, &command, definition, deadline_unix_seconds)?;
            Ok(OrdinaryActivityStep::DispatchLabel(command))
        }
        DirectEvaluation::DispatchReplica { .. } => {
            Err("label activity produced a replica dispatch".to_string())
        }
    }
}

pub(crate) fn evaluate_ordinary_passive<A>(
    input: &A::Request,
    definition: &DirectSwitchoverDefinition,
    observations: &OperationObservations,
    now: i64,
    deadline_unix_seconds: i64,
) -> Result<OrdinaryActivityStep<A::Output>, String>
where
    A: PassiveOperation,
{
    A::validate_request(input, definition, deadline_unix_seconds)?;
    match A::evaluate_request(input, observations, now, deadline_unix_seconds)? {
        DirectEvaluation::Observe(result) => {
            decode_activity_result::<EncodedSwitchoverOutcome<A>>(&result)
                .map(OrdinaryActivityStep::Complete)
                .map_err(|error| format!("decode {} observation: {error}", A::NAME))
        }
        DirectEvaluation::AwaitEvidence => Ok(OrdinaryActivityStep::AwaitEvidence {
            authoritative_pending: false,
        }),
        DirectEvaluation::DispatchReplica { .. } | DirectEvaluation::DispatchLabel => {
            Err("passive activity attempted dispatch".to_string())
        }
    }
}

impl<A> SwitchoverEffectFamily<A> for ReplicaEffectFamily
where
    A: ReplicaOperation
        + kuberic_durable_execution::DurableEffect<
            Request = <A as SwitchoverActivityContract>::Request,
            Command = Option<ReplicaEffectCommand>,
            Output = <A as SwitchoverActivityContract>::Output,
        > + StrictSwitchoverActivityContract<Family = ReplicaEffectFamily>,
{
    fn prepare_command(
        request: &<A as SwitchoverActivityContract>::Request,
        definition: &DirectSwitchoverDefinition,
        observations: &OperationObservations,
        addressed_instances: &BTreeMap<i64, ReplicaInstanceId>,
        now: i64,
        deadline_unix_seconds: i64,
    ) -> Result<<A as kuberic_durable_execution::DurableEffect>::Command, PreparedActivityError>
    {
        validate_replica::<A>(request, definition, deadline_unix_seconds)
            .map_err(|_| PreparedActivityError::Validation)?;
        match evaluate_replica::<A>(
            request,
            definition,
            observations,
            now,
            deadline_unix_seconds,
        )
        .map_err(|_| PreparedActivityError::Validation)?
        {
            DirectEvaluation::Observe(_) => Ok(None),
            DirectEvaluation::AwaitEvidence => Err(PreparedActivityError::Derivation),
            DirectEvaluation::DispatchReplica { action, pending } => {
                let observed = observations
                    .get(&pending.target_id)
                    .ok_or(PreparedActivityError::Derivation)?;
                let addressed = addressed_instances
                    .get(&pending.target_id)
                    .ok_or(PreparedActivityError::Derivation)?;
                let (_, command) =
                    prepare_replica_effect_command(&pending, &observed.status, addressed, &action)
                        .map_err(preparation_error)?;
                validate_replica_command::<A>(request, &command, definition, deadline_unix_seconds)
                    .map_err(|_| PreparedActivityError::Validation)?;
                Ok(Some(command))
            }
            DirectEvaluation::DispatchLabel => Err(PreparedActivityError::Validation),
        }
    }

    fn validate_recorded_command(
        request: &<A as SwitchoverActivityContract>::Request,
        command: &<A as kuberic_durable_execution::DurableEffect>::Command,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<(), PreparedActivityError> {
        validate_replica::<A>(request, definition, deadline_unix_seconds)
            .map_err(|_| PreparedActivityError::Validation)?;
        if let Some(command) = command {
            validate_replica_command::<A>(request, command, definition, deadline_unix_seconds)
                .map_err(|_| PreparedActivityError::Validation)?;
        }
        Ok(())
    }

    fn observe(
        request: &<A as SwitchoverActivityContract>::Request,
        _command: &<A as kuberic_durable_execution::DurableEffect>::Command,
        definition: &DirectSwitchoverDefinition,
        observations: &OperationObservations,
        now: i64,
        deadline_unix_seconds: i64,
    ) -> Result<
        Option<EffectOutcome<<A as kuberic_durable_execution::DurableEffect>::Output>>,
        String,
    > {
        decode_evaluation::<A>(evaluate_replica::<A>(
            request,
            definition,
            observations,
            now,
            deadline_unix_seconds,
        )?)
    }

    fn observe_quarantined(
        request: &<A as SwitchoverActivityContract>::Request,
        command: &<A as kuberic_durable_execution::DurableEffect>::Command,
        definition: &DirectSwitchoverDefinition,
        observations: &OperationObservations,
        now: i64,
        deadline_unix_seconds: i64,
    ) -> Result<
        Option<EffectOutcome<<A as kuberic_durable_execution::DurableEffect>::Output>>,
        String,
    > {
        decode_evaluation::<A>(evaluate_quarantined_replica::<A>(
            request,
            command,
            definition,
            observations,
            now,
            deadline_unix_seconds,
        )?)
    }

    fn dispatch(
        command: &<A as kuberic_durable_execution::DurableEffect>::Command,
    ) -> SwitchoverDispatch<'_> {
        command
            .as_ref()
            .map(SwitchoverDispatch::Replica)
            .unwrap_or(SwitchoverDispatch::ObservationOnly)
    }
}

fn preparation_error(error: DurableEffectPreparationError) -> PreparedActivityError {
    match error {
        DurableEffectPreparationError::WaitForExactIncarnation
        | DurableEffectPreparationError::WaitForSupportedProtocol => {
            PreparedActivityError::Derivation
        }
        DurableEffectPreparationError::InvalidCommand => PreparedActivityError::Validation,
    }
}

fn evaluate_capture(
    input: &CaptureFrozenLsnInput,
    observations: &OperationObservations,
    now: i64,
    deadline_unix_seconds: i64,
) -> Result<DirectEvaluation, String> {
    let outcome = match observations.get(&input.old_primary_id) {
        None if now < deadline_unix_seconds => return Ok(DirectEvaluation::AwaitEvidence),
        None => EffectOutcome::UnavailableAtDeadline(effect_error::<CaptureFrozenLsnActivity>(
            EffectErrorKind::UnavailableAtDeadline,
            now,
            "old primary was unavailable at frozen-LSN deadline".to_string(),
        )?),
        Some(observed)
            if observed.status.instance_id.as_str() != input.old_primary_instance_id
                || observed.status.epoch != epoch(&input.expected_epoch)
                || observed.status.role != Role::Primary
                || observed.status.write_status != AccessStatus::ReconfigurationPending =>
        {
            EffectOutcome::ConflictingEvidence(effect_error::<CaptureFrozenLsnActivity>(
                EffectErrorKind::ConflictingEvidence,
                now,
                "old primary frozen-LSN observation is not exact".to_string(),
            )?)
        }
        Some(observed) => EffectOutcome::Applied(CaptureFrozenLsnOutput {
            frozen_lsn: observed.status.current_progress,
            observed_at_unix_seconds: now,
        }),
    };
    encode_activity_result::<EncodedSwitchoverOutcome<CaptureFrozenLsnActivity>>(&outcome)
        .map(DirectEvaluation::Observe)
        .map_err(|error| format!("encode capture-frozen-lsn result: {error}"))
}

fn evaluate_target_catch_up(
    input: &WaitTargetCaughtUpInput,
    observations: &OperationObservations,
    now: i64,
    deadline_unix_seconds: i64,
) -> Result<DirectEvaluation, String> {
    let outcome = match observations.get(&input.target_id) {
        None if now < deadline_unix_seconds => return Ok(DirectEvaluation::AwaitEvidence),
        None => EffectOutcome::UnavailableAtDeadline(effect_error::<WaitTargetCaughtUpActivity>(
            EffectErrorKind::UnavailableAtDeadline,
            now,
            "target was unavailable at catch-up deadline".to_string(),
        )?),
        Some(observed)
            if observed.status.instance_id.as_str() != input.target_instance_id
                || observed.status.epoch != epoch(&input.expected_epoch)
                || observed.status.role != Role::ActiveSecondary =>
        {
            EffectOutcome::ConflictingEvidence(effect_error::<WaitTargetCaughtUpActivity>(
                EffectErrorKind::ConflictingEvidence,
                now,
                "target catch-up observation is not exact".to_string(),
            )?)
        }
        Some(observed) if observed.status.current_progress >= input.frozen_lsn => {
            EffectOutcome::Applied(WaitTargetCaughtUpOutput {
                observed_at_unix_seconds: now,
            })
        }
        Some(_) if now < deadline_unix_seconds => {
            return Ok(DirectEvaluation::AwaitEvidence);
        }
        Some(_) => EffectOutcome::DeadlineExceeded(effect_error::<WaitTargetCaughtUpActivity>(
            EffectErrorKind::DeadlineExceeded,
            now,
            "target did not reach the frozen LSN before deadline".to_string(),
        )?),
    };
    encode_activity_result::<EncodedSwitchoverOutcome<WaitTargetCaughtUpActivity>>(&outcome)
        .map(DirectEvaluation::Observe)
        .map_err(|error| format!("encode wait-target-caught-up result: {error}"))
}

fn evaluate_attestation<A, FAttested>(
    snapshot: &StablePartitionSnapshotStatus,
    deadline_unix_seconds: i64,
    observations: &OperationObservations,
    now: i64,
    attested: FAttested,
) -> Result<DirectEvaluation, String>
where
    A: SwitchoverActivityContract,
    FAttested: FnOnce(i64, StablePartitionSnapshotStatus) -> A::Output,
{
    let outcome = match attestation_error(snapshot, observations) {
        Ok(()) => EffectOutcome::Applied(attested(
            now,
            crate::reconciler::snapshot_with_observed_metadata(snapshot.clone(), observations),
        )),
        Err(AttestationError::Unavailable(_)) if now < deadline_unix_seconds => {
            return Ok(DirectEvaluation::AwaitEvidence);
        }
        Err(AttestationError::Unavailable(message)) => EffectOutcome::UnavailableAtDeadline(
            effect_error::<A>(EffectErrorKind::UnavailableAtDeadline, now, message)?,
        ),
        Err(AttestationError::Conflicting(message)) => EffectOutcome::ConflictingEvidence(
            effect_error::<A>(EffectErrorKind::ConflictingEvidence, now, message)?,
        ),
    };
    encode_activity_result::<EncodedSwitchoverOutcome<A>>(&outcome)
        .map(DirectEvaluation::Observe)
        .map_err(|error| {
            format!(
                "encode {} result: {error}",
                <A as SwitchoverActivityContract>::NAME
            )
        })
}

pub(crate) trait PassiveOperation: SwitchoverActivityContract {
    fn validate_request(
        request: &Self::Request,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<(), String>;

    fn evaluate_request(
        request: &Self::Request,
        observations: &OperationObservations,
        now: i64,
        deadline_unix_seconds: i64,
    ) -> Result<DirectEvaluation, String>;
}

impl PassiveOperation for CaptureFrozenLsnActivity {
    fn validate_request(
        input: &CaptureFrozenLsnInput,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<(), String> {
        validate_common(
            DIRECT_SWITCHOVER_CONTRACT_VERSION,
            &definition.execution_id,
            deadline_unix_seconds,
            definition,
        )?;
        let old = definition.member(definition.old_primary_id)?;
        if input.old_primary_id != definition.old_primary_id
            || input.old_primary_instance_id != old.instance_id
            || input.expected_epoch != definition.previous_snapshot.epoch
        {
            return Err("capture-frozen-lsn input conflicts with admission".to_string());
        }
        Ok(())
    }

    fn evaluate_request(
        input: &CaptureFrozenLsnInput,
        observations: &OperationObservations,
        now: i64,
        deadline_unix_seconds: i64,
    ) -> Result<DirectEvaluation, String> {
        evaluate_capture(input, observations, now, deadline_unix_seconds)
    }
}

impl PassiveOperation for WaitTargetCaughtUpActivity {
    fn validate_request(
        input: &WaitTargetCaughtUpInput,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<(), String> {
        validate_common(
            DIRECT_SWITCHOVER_CONTRACT_VERSION,
            &definition.execution_id,
            deadline_unix_seconds,
            definition,
        )?;
        let target = definition.member(definition.target_primary_id)?;
        if input.target_id != definition.target_primary_id
            || input.target_instance_id != target.instance_id
            || input.expected_epoch != definition.previous_snapshot.epoch
            || input.frozen_lsn < 0
        {
            return Err("wait-target-caught-up input conflicts with admission".to_string());
        }
        Ok(())
    }

    fn evaluate_request(
        input: &WaitTargetCaughtUpInput,
        observations: &OperationObservations,
        now: i64,
        deadline_unix_seconds: i64,
    ) -> Result<DirectEvaluation, String> {
        evaluate_target_catch_up(input, observations, now, deadline_unix_seconds)
    }
}

impl PassiveOperation for AttestTargetTopologyActivity {
    fn validate_request(
        input: &AttestTargetTopologyInput,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<(), String> {
        validate_common(
            DIRECT_SWITCHOVER_CONTRACT_VERSION,
            &definition.execution_id,
            deadline_unix_seconds,
            definition,
        )?;
        if input.expected_snapshot != definition.target_snapshot {
            return Err("target attestation snapshot conflicts with admission".to_string());
        }
        Ok(())
    }

    fn evaluate_request(
        input: &AttestTargetTopologyInput,
        observations: &OperationObservations,
        now: i64,
        deadline_unix_seconds: i64,
    ) -> Result<DirectEvaluation, String> {
        evaluate_attestation::<Self, _>(
            &input.expected_snapshot,
            deadline_unix_seconds,
            observations,
            now,
            |observed_at_unix_seconds, snapshot| AttestTargetTopologyOutput {
                observed_at_unix_seconds,
                snapshot,
            },
        )
    }
}

impl PassiveOperation for AttestCompensatedTopologyActivity {
    fn validate_request(
        input: &AttestCompensatedTopologyInput,
        definition: &DirectSwitchoverDefinition,
        deadline_unix_seconds: i64,
    ) -> Result<(), String> {
        validate_common(
            DIRECT_SWITCHOVER_CONTRACT_VERSION,
            &definition.execution_id,
            deadline_unix_seconds,
            definition,
        )?;
        let compensation = definition.compensation_snapshot();
        if input.expected_snapshot != definition.previous_snapshot
            && input.expected_snapshot != compensation
        {
            return Err("compensated attestation snapshot conflicts with admission".to_string());
        }
        Ok(())
    }

    fn evaluate_request(
        input: &AttestCompensatedTopologyInput,
        observations: &OperationObservations,
        now: i64,
        deadline_unix_seconds: i64,
    ) -> Result<DirectEvaluation, String> {
        evaluate_attestation::<Self, _>(
            &input.expected_snapshot,
            deadline_unix_seconds,
            observations,
            now,
            |observed_at_unix_seconds, snapshot| AttestCompensatedTopologyOutput {
                observed_at_unix_seconds,
                snapshot,
            },
        )
    }
}

enum AttestationError {
    Unavailable(String),
    Conflicting(String),
}

fn attestation_error(
    snapshot: &StablePartitionSnapshotStatus,
    observations: &OperationObservations,
) -> Result<(), AttestationError> {
    let expected_epoch = epoch(&snapshot.epoch);
    for member in &snapshot.members {
        let observed = observations.get(&member.id).ok_or_else(|| {
            AttestationError::Unavailable(format!(
                "replica {} is unavailable for topology attestation",
                member.id
            ))
        })?;
        let expected_role = role(member.role);
        let expected_label = if member.id == snapshot.primary_id {
            "primary"
        } else {
            "secondary"
        };
        if observed.status.instance_id.as_str() != member.instance_id
            || observed.status.epoch != expected_epoch
            || observed.status.role != expected_role
            || observed.pod_role_label.as_deref() != Some(expected_label)
        {
            return Err(AttestationError::Conflicting(format!(
                "replica {} conflicts with exact topology attestation",
                member.id
            )));
        }
    }
    let primary = observations.get(&snapshot.primary_id).ok_or_else(|| {
        AttestationError::Unavailable("topology primary is unavailable".to_string())
    })?;
    let expected = configuration_status(ReplicaConfigurationMode::Current, snapshot);
    if primary.status.configuration.as_ref() != Some(&expected) {
        return Err(AttestationError::Conflicting(
            "topology primary current configuration is not exact".to_string(),
        ));
    }
    Ok(())
}

fn deadline_or_wait(
    deadline_unix_seconds: i64,
    now: i64,
    result: impl FnOnce() -> Result<ExactBytes, String>,
) -> Result<DirectEvaluation, String> {
    if now < deadline_unix_seconds {
        Ok(DirectEvaluation::AwaitEvidence)
    } else {
        result().map(DirectEvaluation::Observe)
    }
}

fn configuration_relation(
    status: &kuberic_core::types::ReplicaStatusInfo,
    expected: &ReplicaConfigurationStatus,
    precondition: Option<&ReplicaConfigurationStatus>,
    allow_none_precondition: bool,
    expected_epoch: Epoch,
) -> StatusRelation {
    if status.role != Role::Primary || status.epoch != expected_epoch {
        return StatusRelation::conflicting("configuration target is not exact primary");
    }
    if status.configuration.as_ref() == Some(expected) {
        StatusRelation::Postcondition
    } else if status.configuration.as_ref() == precondition
        || (allow_none_precondition && status.configuration.is_none())
    {
        StatusRelation::Precondition
    } else {
        StatusRelation::conflicting("configuration pre/postcondition is not exact")
    }
}

fn config_for_snapshot(
    snapshot: &StablePartitionSnapshotStatus,
    observations: &OperationObservations,
) -> Result<ReplicaSetConfig, String> {
    let mut members = Vec::new();
    for member in &snapshot.members {
        if member.id == snapshot.primary_id {
            continue;
        }
        let observed = observations
            .get(&member.id)
            .ok_or_else(|| format!("replica {} is unavailable for configuration", member.id))?;
        if observed.status.instance_id.as_str() != member.instance_id {
            return Err(format!(
                "replica {} incarnation changed before configuration preparation",
                member.id
            ));
        }
        members.push(ReplicaInfo {
            id: member.id,
            instance_id: ReplicaInstanceId::new(member.instance_id.clone()),
            role: Role::ActiveSecondary,
            status: ReplicaStatus::Up,
            replicator_address: observed.replicator_address.clone(),
            current_progress: 0,
            catch_up_capability: 0,
            must_catch_up: false,
        });
    }
    members.sort_by_key(|member| member.id);
    Ok(ReplicaSetConfig {
        members,
        write_quorum: snapshot.write_quorum,
    })
}

fn configuration_status(
    mode: ReplicaConfigurationMode,
    snapshot: &StablePartitionSnapshotStatus,
) -> ReplicaConfigurationStatus {
    let mut members = snapshot
        .members
        .iter()
        .filter(|member| member.id != snapshot.primary_id)
        .map(|member| ReplicaConfigurationMemberStatus {
            id: member.id,
            instance_id: ReplicaInstanceId::new(member.instance_id.clone()),
            role: Role::ActiveSecondary,
        })
        .collect::<Vec<_>>();
    members.sort_by_key(|member| member.id);
    ReplicaConfigurationStatus {
        mode,
        members,
        write_quorum: snapshot.write_quorum,
    }
}

fn config_matches_snapshot(
    config: &ReplicaSetConfig,
    snapshot: &StablePartitionSnapshotStatus,
) -> bool {
    if config.write_quorum != snapshot.write_quorum
        || config.members.len() != snapshot.members.len().saturating_sub(1)
    {
        return false;
    }
    let mut expected = snapshot
        .members
        .iter()
        .filter(|member| member.id != snapshot.primary_id)
        .map(|member| (member.id, member.instance_id.as_str()))
        .collect::<Vec<_>>();
    expected.sort_unstable_by_key(|(id, _)| *id);
    let mut actual = config
        .members
        .iter()
        .map(|member| (member.id, member.instance_id.as_str(), member.role))
        .collect::<Vec<_>>();
    actual.sort_unstable_by_key(|(id, _, _)| *id);
    actual.len() == expected.len()
        && actual.iter().zip(expected).all(
            |((id, instance, role), (expected_id, expected_instance))| {
                *id == expected_id
                    && *instance == expected_instance
                    && *role == Role::ActiveSecondary
            },
        )
}

fn epoch(value: &EpochStatus) -> Epoch {
    Epoch::new(value.data_loss_number, value.configuration_number)
}

fn role(value: StableReplicaRoleStatus) -> Role {
    match value {
        StableReplicaRoleStatus::Primary => Role::Primary,
        StableReplicaRoleStatus::ActiveSecondary => Role::ActiveSecondary,
    }
}
