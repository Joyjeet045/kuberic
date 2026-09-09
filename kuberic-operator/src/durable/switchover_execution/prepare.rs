use std::collections::BTreeMap;

use kuberic_core::types::{
    AccessStatus, DurableActionState, DurableReplicaAction, Epoch,
    ReplicaConfigurationMemberStatus, ReplicaConfigurationMode, ReplicaConfigurationStatus,
    ReplicaInfo, ReplicaInstanceId, ReplicaSetConfig, ReplicaSetQuorumMode, ReplicaStatus, Role,
};
use kuberic_durable_execution::{
    ActivityName, ActivitySpec, DurableActivity, ExactBytes, PreparedActivityError,
    decode_activity_input, encode_activity_input, encode_activity_result,
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
        DistributeReplicaEpochInput, EffectObservation,
        InstallCompensationCatchUpConfigurationActivity,
        InstallCompensationCatchUpConfigurationInput,
        InstallCompensationCurrentConfigurationActivity,
        InstallCompensationCurrentConfigurationInput, InstallTargetCatchUpConfigurationActivity,
        InstallTargetCatchUpConfigurationInput, InstallTargetCurrentConfigurationActivity,
        InstallTargetCurrentConfigurationInput, LabelDirectActivity, PromoteTargetActivity,
        PromoteTargetInput, PublishOldPrimarySecondaryLabelActivity,
        PublishOldPrimarySecondaryLabelInput, PublishTargetPrimaryLabelActivity,
        PublishTargetPrimaryLabelInput, ReplicaDirectActivity, RestoreOldPrimaryLabelActivity,
        RestoreOldPrimaryLabelInput, RestorePreviousCurrentConfigurationActivity,
        RestorePreviousCurrentConfigurationInput, RestoreTargetSecondaryLabelActivity,
        RestoreTargetSecondaryLabelInput, RevokeWritesActivity, RevokeWritesInput,
        WaitTargetCaughtUpActivity, WaitTargetCaughtUpInput, WaitTargetCaughtUpOutput,
        WaitTargetWriteQuorumActivity, WaitTargetWriteQuorumInput,
    },
    model::DirectSwitchoverDefinition,
};

#[derive(Clone, Debug, PartialEq)]
pub enum DirectActivity {
    RevokeWrites(RevokeWritesInput),
    DemoteOldPrimary(DemoteOldPrimaryInput),
    PromoteTarget(PromoteTargetInput),
    DistributeReplicaEpoch(DistributeReplicaEpochInput),
    InstallTargetCatchUpConfiguration(InstallTargetCatchUpConfigurationInput),
    WaitTargetWriteQuorum(WaitTargetWriteQuorumInput),
    InstallTargetCurrentConfiguration(InstallTargetCurrentConfigurationInput),
    RestorePreviousCurrentConfiguration(RestorePreviousCurrentConfigurationInput),
    CompensatePromoteOldPrimary(CompensatePromoteOldPrimaryInput),
    CompensateDistributeReplicaEpoch(CompensateDistributeReplicaEpochInput),
    InstallCompensationCatchUpConfiguration(InstallCompensationCatchUpConfigurationInput),
    InstallCompensationCurrentConfiguration(InstallCompensationCurrentConfigurationInput),
    PublishTargetPrimaryLabel(PublishTargetPrimaryLabelInput),
    PublishOldPrimarySecondaryLabel(PublishOldPrimarySecondaryLabelInput),
    RestoreOldPrimaryLabel(RestoreOldPrimaryLabelInput),
    RestoreTargetSecondaryLabel(RestoreTargetSecondaryLabelInput),
    CaptureFrozenLsn(CaptureFrozenLsnInput),
    WaitTargetCaughtUp(WaitTargetCaughtUpInput),
    AttestTargetTopology(AttestTargetTopologyInput),
    AttestCompensatedTopology(AttestCompensatedTopologyInput),
}

pub enum DirectEvaluation {
    Observe(ExactBytes),
    AwaitEvidence,
    DispatchReplica {
        action: DurableReplicaAction,
        pending: Box<PendingActionStatus>,
    },
    DispatchLabel,
}

impl DirectActivity {
    pub fn decode(spec: &ActivitySpec) -> Result<Self, String> {
        let name = spec.name().name();
        Ok(match name {
            RevokeWritesActivity::NAME => Self::RevokeWrites(decode::<RevokeWritesActivity>(spec)?),
            DemoteOldPrimaryActivity::NAME => {
                Self::DemoteOldPrimary(decode::<DemoteOldPrimaryActivity>(spec)?)
            }
            PromoteTargetActivity::NAME => {
                Self::PromoteTarget(decode::<PromoteTargetActivity>(spec)?)
            }
            DistributeReplicaEpochActivity::NAME => {
                Self::DistributeReplicaEpoch(decode::<DistributeReplicaEpochActivity>(spec)?)
            }
            InstallTargetCatchUpConfigurationActivity::NAME => {
                Self::InstallTargetCatchUpConfiguration(decode::<
                    InstallTargetCatchUpConfigurationActivity,
                >(spec)?)
            }
            WaitTargetWriteQuorumActivity::NAME => {
                Self::WaitTargetWriteQuorum(decode::<WaitTargetWriteQuorumActivity>(spec)?)
            }
            InstallTargetCurrentConfigurationActivity::NAME => {
                Self::InstallTargetCurrentConfiguration(decode::<
                    InstallTargetCurrentConfigurationActivity,
                >(spec)?)
            }
            RestorePreviousCurrentConfigurationActivity::NAME => {
                Self::RestorePreviousCurrentConfiguration(decode::<
                    RestorePreviousCurrentConfigurationActivity,
                >(spec)?)
            }
            CompensatePromoteOldPrimaryActivity::NAME => Self::CompensatePromoteOldPrimary(
                decode::<CompensatePromoteOldPrimaryActivity>(spec)?,
            ),
            CompensateDistributeReplicaEpochActivity::NAME => {
                Self::CompensateDistributeReplicaEpoch(decode::<
                    CompensateDistributeReplicaEpochActivity,
                >(spec)?)
            }
            InstallCompensationCatchUpConfigurationActivity::NAME => {
                Self::InstallCompensationCatchUpConfiguration(decode::<
                    InstallCompensationCatchUpConfigurationActivity,
                >(spec)?)
            }
            InstallCompensationCurrentConfigurationActivity::NAME => {
                Self::InstallCompensationCurrentConfiguration(decode::<
                    InstallCompensationCurrentConfigurationActivity,
                >(spec)?)
            }
            PublishTargetPrimaryLabelActivity::NAME => {
                Self::PublishTargetPrimaryLabel(decode::<PublishTargetPrimaryLabelActivity>(spec)?)
            }
            PublishOldPrimarySecondaryLabelActivity::NAME => {
                Self::PublishOldPrimarySecondaryLabel(decode::<
                    PublishOldPrimarySecondaryLabelActivity,
                >(spec)?)
            }
            RestoreOldPrimaryLabelActivity::NAME => {
                Self::RestoreOldPrimaryLabel(decode::<RestoreOldPrimaryLabelActivity>(spec)?)
            }
            RestoreTargetSecondaryLabelActivity::NAME => Self::RestoreTargetSecondaryLabel(
                decode::<RestoreTargetSecondaryLabelActivity>(spec)?,
            ),
            CaptureFrozenLsnActivity::NAME => {
                Self::CaptureFrozenLsn(decode::<CaptureFrozenLsnActivity>(spec)?)
            }
            WaitTargetCaughtUpActivity::NAME => {
                Self::WaitTargetCaughtUp(decode::<WaitTargetCaughtUpActivity>(spec)?)
            }
            AttestTargetTopologyActivity::NAME => {
                Self::AttestTargetTopology(decode::<AttestTargetTopologyActivity>(spec)?)
            }
            AttestCompensatedTopologyActivity::NAME => {
                Self::AttestCompensatedTopology(decode::<AttestCompensatedTopologyActivity>(spec)?)
            }
            _ => return Err(format!("unknown direct switchover activity {name}")),
        })
    }

    pub fn spec(&self) -> Result<ActivitySpec, String> {
        match self {
            Self::RevokeWrites(input) => spec::<RevokeWritesActivity>(input),
            Self::DemoteOldPrimary(input) => spec::<DemoteOldPrimaryActivity>(input),
            Self::PromoteTarget(input) => spec::<PromoteTargetActivity>(input),
            Self::DistributeReplicaEpoch(input) => spec::<DistributeReplicaEpochActivity>(input),
            Self::InstallTargetCatchUpConfiguration(input) => {
                spec::<InstallTargetCatchUpConfigurationActivity>(input)
            }
            Self::WaitTargetWriteQuorum(input) => spec::<WaitTargetWriteQuorumActivity>(input),
            Self::InstallTargetCurrentConfiguration(input) => {
                spec::<InstallTargetCurrentConfigurationActivity>(input)
            }
            Self::RestorePreviousCurrentConfiguration(input) => {
                spec::<RestorePreviousCurrentConfigurationActivity>(input)
            }
            Self::CompensatePromoteOldPrimary(input) => {
                spec::<CompensatePromoteOldPrimaryActivity>(input)
            }
            Self::CompensateDistributeReplicaEpoch(input) => {
                spec::<CompensateDistributeReplicaEpochActivity>(input)
            }
            Self::InstallCompensationCatchUpConfiguration(input) => {
                spec::<InstallCompensationCatchUpConfigurationActivity>(input)
            }
            Self::InstallCompensationCurrentConfiguration(input) => {
                spec::<InstallCompensationCurrentConfigurationActivity>(input)
            }
            Self::PublishTargetPrimaryLabel(input) => {
                spec::<PublishTargetPrimaryLabelActivity>(input)
            }
            Self::PublishOldPrimarySecondaryLabel(input) => {
                spec::<PublishOldPrimarySecondaryLabelActivity>(input)
            }
            Self::RestoreOldPrimaryLabel(input) => spec::<RestoreOldPrimaryLabelActivity>(input),
            Self::RestoreTargetSecondaryLabel(input) => {
                spec::<RestoreTargetSecondaryLabelActivity>(input)
            }
            Self::CaptureFrozenLsn(input) => spec::<CaptureFrozenLsnActivity>(input),
            Self::WaitTargetCaughtUp(input) => spec::<WaitTargetCaughtUpActivity>(input),
            Self::AttestTargetTopology(input) => spec::<AttestTargetTopologyActivity>(input),
            Self::AttestCompensatedTopology(input) => {
                spec::<AttestCompensatedTopologyActivity>(input)
            }
        }
    }

    pub fn deadline_unix_seconds(&self) -> i64 {
        match self {
            Self::RevokeWrites(input) => input.deadline_unix_seconds,
            Self::DemoteOldPrimary(input) => input.deadline_unix_seconds,
            Self::PromoteTarget(input) => input.deadline_unix_seconds,
            Self::DistributeReplicaEpoch(input) => input.deadline_unix_seconds,
            Self::InstallTargetCatchUpConfiguration(input) => input.deadline_unix_seconds,
            Self::WaitTargetWriteQuorum(input) => input.deadline_unix_seconds,
            Self::InstallTargetCurrentConfiguration(input) => input.deadline_unix_seconds,
            Self::RestorePreviousCurrentConfiguration(input) => input.deadline_unix_seconds,
            Self::CompensatePromoteOldPrimary(input) => input.deadline_unix_seconds,
            Self::CompensateDistributeReplicaEpoch(input) => input.deadline_unix_seconds,
            Self::InstallCompensationCatchUpConfiguration(input) => input.deadline_unix_seconds,
            Self::InstallCompensationCurrentConfiguration(input) => input.deadline_unix_seconds,
            Self::PublishTargetPrimaryLabel(input) => input.deadline_unix_seconds,
            Self::PublishOldPrimarySecondaryLabel(input) => input.deadline_unix_seconds,
            Self::RestoreOldPrimaryLabel(input) => input.deadline_unix_seconds,
            Self::RestoreTargetSecondaryLabel(input) => input.deadline_unix_seconds,
            Self::CaptureFrozenLsn(input) => input.deadline_unix_seconds,
            Self::WaitTargetCaughtUp(input) => input.deadline_unix_seconds,
            Self::AttestTargetTopology(input) => input.deadline_unix_seconds,
            Self::AttestCompensatedTopology(input) => input.deadline_unix_seconds,
        }
    }

    pub fn logical_predecessor(&self) -> Self {
        let mut predecessor = self.clone();
        predecessor.clear_prepared_command();
        predecessor
    }

    pub fn is_logical(&self) -> bool {
        match self {
            Self::RevokeWrites(_)
            | Self::DemoteOldPrimary(_)
            | Self::PromoteTarget(_)
            | Self::DistributeReplicaEpoch(_)
            | Self::InstallTargetCatchUpConfiguration(_)
            | Self::WaitTargetWriteQuorum(_)
            | Self::InstallTargetCurrentConfiguration(_)
            | Self::RestorePreviousCurrentConfiguration(_)
            | Self::CompensatePromoteOldPrimary(_)
            | Self::CompensateDistributeReplicaEpoch(_)
            | Self::InstallCompensationCatchUpConfiguration(_)
            | Self::InstallCompensationCurrentConfiguration(_) => {
                self.prepared_replica_command().is_none()
            }
            Self::PublishTargetPrimaryLabel(_)
            | Self::PublishOldPrimarySecondaryLabel(_)
            | Self::RestoreOldPrimaryLabel(_)
            | Self::RestoreTargetSecondaryLabel(_) => self.prepared_label_command().is_none(),
            Self::CaptureFrozenLsn(_)
            | Self::WaitTargetCaughtUp(_)
            | Self::AttestTargetTopology(_)
            | Self::AttestCompensatedTopology(_) => true,
        }
    }

    pub fn validate(&self, definition: &DirectSwitchoverDefinition) -> Result<(), String> {
        match self {
            Self::RevokeWrites(input) => {
                validate_replica::<RevokeWritesActivity>(input, definition)
            }
            Self::DemoteOldPrimary(input) => {
                validate_replica::<DemoteOldPrimaryActivity>(input, definition)
            }
            Self::PromoteTarget(input) => {
                validate_replica::<PromoteTargetActivity>(input, definition)
            }
            Self::DistributeReplicaEpoch(input) => {
                validate_replica::<DistributeReplicaEpochActivity>(input, definition)
            }
            Self::InstallTargetCatchUpConfiguration(input) => {
                validate_replica::<InstallTargetCatchUpConfigurationActivity>(input, definition)
            }
            Self::WaitTargetWriteQuorum(input) => {
                validate_replica::<WaitTargetWriteQuorumActivity>(input, definition)
            }
            Self::InstallTargetCurrentConfiguration(input) => {
                validate_replica::<InstallTargetCurrentConfigurationActivity>(input, definition)
            }
            Self::RestorePreviousCurrentConfiguration(input) => {
                validate_replica::<RestorePreviousCurrentConfigurationActivity>(input, definition)
            }
            Self::CompensatePromoteOldPrimary(input) => {
                validate_replica::<CompensatePromoteOldPrimaryActivity>(input, definition)
            }
            Self::CompensateDistributeReplicaEpoch(input) => {
                validate_replica::<CompensateDistributeReplicaEpochActivity>(input, definition)
            }
            Self::InstallCompensationCatchUpConfiguration(input) => validate_replica::<
                InstallCompensationCatchUpConfigurationActivity,
            >(input, definition),
            Self::InstallCompensationCurrentConfiguration(input) => validate_replica::<
                InstallCompensationCurrentConfigurationActivity,
            >(input, definition),
            Self::PublishTargetPrimaryLabel(input) => {
                validate_label::<PublishTargetPrimaryLabelActivity>(input, definition)
            }
            Self::PublishOldPrimarySecondaryLabel(input) => {
                validate_label::<PublishOldPrimarySecondaryLabelActivity>(input, definition)
            }
            Self::RestoreOldPrimaryLabel(input) => {
                validate_label::<RestoreOldPrimaryLabelActivity>(input, definition)
            }
            Self::RestoreTargetSecondaryLabel(input) => {
                validate_label::<RestoreTargetSecondaryLabelActivity>(input, definition)
            }
            Self::CaptureFrozenLsn(input) => {
                validate_common(
                    input.contract_version,
                    &input.execution_id,
                    input.deadline_unix_seconds,
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
            Self::WaitTargetCaughtUp(input) => {
                validate_common(
                    input.contract_version,
                    &input.execution_id,
                    input.deadline_unix_seconds,
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
            Self::AttestTargetTopology(input) => {
                validate_common(
                    input.contract_version,
                    &input.execution_id,
                    input.deadline_unix_seconds,
                    definition,
                )?;
                if input.expected_snapshot != definition.target_snapshot {
                    return Err("target attestation snapshot conflicts with admission".to_string());
                }
                Ok(())
            }
            Self::AttestCompensatedTopology(input) => {
                validate_common(
                    input.contract_version,
                    &input.execution_id,
                    input.deadline_unix_seconds,
                    definition,
                )?;
                let compensation = definition.compensation_snapshot();
                if input.expected_snapshot != definition.previous_snapshot
                    && input.expected_snapshot != compensation
                {
                    return Err(
                        "compensated attestation snapshot conflicts with admission".to_string()
                    );
                }
                Ok(())
            }
        }
    }

    pub fn evaluate(
        &self,
        definition: &DirectSwitchoverDefinition,
        observations: &OperationObservations,
        now: i64,
    ) -> Result<DirectEvaluation, String> {
        self.validate(definition)?;
        match self {
            Self::RevokeWrites(input) => {
                evaluate_replica::<RevokeWritesActivity>(input, definition, observations, now)
            }
            Self::DemoteOldPrimary(input) => {
                evaluate_replica::<DemoteOldPrimaryActivity>(input, definition, observations, now)
            }
            Self::PromoteTarget(input) => {
                evaluate_replica::<PromoteTargetActivity>(input, definition, observations, now)
            }
            Self::DistributeReplicaEpoch(input) => {
                evaluate_replica::<DistributeReplicaEpochActivity>(
                    input,
                    definition,
                    observations,
                    now,
                )
            }
            Self::InstallTargetCatchUpConfiguration(input) => {
                evaluate_replica::<InstallTargetCatchUpConfigurationActivity>(
                    input,
                    definition,
                    observations,
                    now,
                )
            }
            Self::WaitTargetWriteQuorum(input) => {
                evaluate_replica::<WaitTargetWriteQuorumActivity>(
                    input,
                    definition,
                    observations,
                    now,
                )
            }
            Self::InstallTargetCurrentConfiguration(input) => {
                evaluate_replica::<InstallTargetCurrentConfigurationActivity>(
                    input,
                    definition,
                    observations,
                    now,
                )
            }
            Self::RestorePreviousCurrentConfiguration(input) => {
                evaluate_replica::<RestorePreviousCurrentConfigurationActivity>(
                    input,
                    definition,
                    observations,
                    now,
                )
            }
            Self::CompensatePromoteOldPrimary(input) => evaluate_replica::<
                CompensatePromoteOldPrimaryActivity,
            >(
                input, definition, observations, now
            ),
            Self::CompensateDistributeReplicaEpoch(input) => evaluate_replica::<
                CompensateDistributeReplicaEpochActivity,
            >(
                input, definition, observations, now
            ),
            Self::InstallCompensationCatchUpConfiguration(input) => {
                evaluate_replica::<InstallCompensationCatchUpConfigurationActivity>(
                    input,
                    definition,
                    observations,
                    now,
                )
            }
            Self::InstallCompensationCurrentConfiguration(input) => {
                evaluate_replica::<InstallCompensationCurrentConfigurationActivity>(
                    input,
                    definition,
                    observations,
                    now,
                )
            }
            Self::PublishTargetPrimaryLabel(input) => evaluate_label::<
                PublishTargetPrimaryLabelActivity,
            >(
                input, definition, observations, now
            ),
            Self::PublishOldPrimarySecondaryLabel(input) => evaluate_label::<
                PublishOldPrimarySecondaryLabelActivity,
            >(
                input, definition, observations, now
            ),
            Self::RestoreOldPrimaryLabel(input) => {
                evaluate_label::<RestoreOldPrimaryLabelActivity>(
                    input,
                    definition,
                    observations,
                    now,
                )
            }
            Self::RestoreTargetSecondaryLabel(input) => evaluate_label::<
                RestoreTargetSecondaryLabelActivity,
            >(
                input, definition, observations, now
            ),
            Self::CaptureFrozenLsn(input) => evaluate_capture(input, observations, now),
            Self::WaitTargetCaughtUp(input) => evaluate_target_catch_up(input, observations, now),
            Self::AttestTargetTopology(input) => {
                evaluate_attestation::<AttestTargetTopologyActivity, _, _, _>(
                    &input.expected_snapshot,
                    input.deadline_unix_seconds,
                    observations,
                    now,
                    |observed_at, snapshot| AttestTargetTopologyOutput::Attested {
                        observed_at_unix_seconds: observed_at,
                        snapshot,
                        accounting: None,
                    },
                    |observed_at, message| AttestTargetTopologyOutput::DeadlineExceeded {
                        observed_at_unix_seconds: observed_at,
                        message,
                    },
                    |observed_at, message| AttestTargetTopologyOutput::Conflicting {
                        observed_at_unix_seconds: observed_at,
                        message,
                    },
                )
            }
            Self::AttestCompensatedTopology(input) => {
                evaluate_attestation::<AttestCompensatedTopologyActivity, _, _, _>(
                    &input.expected_snapshot,
                    input.deadline_unix_seconds,
                    observations,
                    now,
                    |observed_at, snapshot| AttestCompensatedTopologyOutput::Attested {
                        observed_at_unix_seconds: observed_at,
                        snapshot,
                        accounting: None,
                    },
                    |observed_at, message| AttestCompensatedTopologyOutput::DeadlineExceeded {
                        observed_at_unix_seconds: observed_at,
                        message,
                    },
                    |observed_at, message| AttestCompensatedTopologyOutput::Conflicting {
                        observed_at_unix_seconds: observed_at,
                        message,
                    },
                )
            }
        }
    }

    pub fn evaluate_quarantine(
        &self,
        definition: &DirectSwitchoverDefinition,
        observations: &OperationObservations,
        now: i64,
    ) -> Result<DirectEvaluation, String> {
        self.validate(definition)?;
        match self {
            Self::RevokeWrites(input) => evaluate_quarantined_replica::<RevokeWritesActivity>(
                input,
                definition,
                observations,
                now,
            ),
            Self::DemoteOldPrimary(input) => {
                evaluate_quarantined_replica::<DemoteOldPrimaryActivity>(
                    input,
                    definition,
                    observations,
                    now,
                )
            }
            Self::PromoteTarget(input) => evaluate_quarantined_replica::<PromoteTargetActivity>(
                input,
                definition,
                observations,
                now,
            ),
            Self::DistributeReplicaEpoch(input) => evaluate_quarantined_replica::<
                DistributeReplicaEpochActivity,
            >(
                input, definition, observations, now
            ),
            Self::InstallTargetCatchUpConfiguration(input) => {
                evaluate_quarantined_replica::<InstallTargetCatchUpConfigurationActivity>(
                    input,
                    definition,
                    observations,
                    now,
                )
            }
            Self::WaitTargetWriteQuorum(input) => evaluate_quarantined_replica::<
                WaitTargetWriteQuorumActivity,
            >(
                input, definition, observations, now
            ),
            Self::InstallTargetCurrentConfiguration(input) => {
                evaluate_quarantined_replica::<InstallTargetCurrentConfigurationActivity>(
                    input,
                    definition,
                    observations,
                    now,
                )
            }
            Self::RestorePreviousCurrentConfiguration(input) => {
                evaluate_quarantined_replica::<RestorePreviousCurrentConfigurationActivity>(
                    input,
                    definition,
                    observations,
                    now,
                )
            }
            Self::CompensatePromoteOldPrimary(input) => evaluate_quarantined_replica::<
                CompensatePromoteOldPrimaryActivity,
            >(
                input, definition, observations, now
            ),
            Self::CompensateDistributeReplicaEpoch(input) => evaluate_quarantined_replica::<
                CompensateDistributeReplicaEpochActivity,
            >(
                input, definition, observations, now
            ),
            Self::InstallCompensationCatchUpConfiguration(input) => {
                evaluate_quarantined_replica::<InstallCompensationCatchUpConfigurationActivity>(
                    input,
                    definition,
                    observations,
                    now,
                )
            }
            Self::InstallCompensationCurrentConfiguration(input) => {
                evaluate_quarantined_replica::<InstallCompensationCurrentConfigurationActivity>(
                    input,
                    definition,
                    observations,
                    now,
                )
            }
            Self::PublishTargetPrimaryLabel(input) => evaluate_quarantined_label::<
                PublishTargetPrimaryLabelActivity,
            >(
                input, definition, observations, now
            ),
            Self::PublishOldPrimarySecondaryLabel(input) => evaluate_quarantined_label::<
                PublishOldPrimarySecondaryLabelActivity,
            >(
                input, definition, observations, now
            ),
            Self::RestoreOldPrimaryLabel(input) => evaluate_quarantined_label::<
                RestoreOldPrimaryLabelActivity,
            >(
                input, definition, observations, now
            ),
            Self::RestoreTargetSecondaryLabel(input) => evaluate_quarantined_label::<
                RestoreTargetSecondaryLabelActivity,
            >(
                input, definition, observations, now
            ),
            Self::CaptureFrozenLsn(_)
            | Self::WaitTargetCaughtUp(_)
            | Self::AttestTargetTopology(_)
            | Self::AttestCompensatedTopology(_) => self.evaluate(definition, observations, now),
        }
    }

    pub fn prepare(
        &self,
        definition: &DirectSwitchoverDefinition,
        observations: &OperationObservations,
        addressed_instances: &BTreeMap<i64, ReplicaInstanceId>,
        now: i64,
    ) -> Result<Self, PreparedActivityError> {
        if !self.is_logical() {
            return Err(PreparedActivityError::Validation);
        }
        match self
            .evaluate(definition, observations, now)
            .map_err(|_| PreparedActivityError::Validation)?
        {
            DirectEvaluation::Observe(_) => Ok(self.clone()),
            DirectEvaluation::AwaitEvidence => {
                if self.is_effect() {
                    Err(PreparedActivityError::Derivation)
                } else {
                    Ok(self.clone())
                }
            }
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
                let mut prepared = self.clone();
                prepared
                    .set_prepared_replica_command(command)
                    .map_err(|_| PreparedActivityError::Validation)?;
                prepared
                    .validate(definition)
                    .map_err(|_| PreparedActivityError::Validation)?;
                Ok(prepared)
            }
            DirectEvaluation::DispatchLabel => {
                let request = self
                    .label_request(definition)
                    .map_err(|_| PreparedActivityError::Validation)?;
                let observed = observations
                    .get(&request.target_id)
                    .ok_or(PreparedActivityError::Derivation)?;
                if observed.status.instance_id.as_str() != request.target_instance_id {
                    return Err(PreparedActivityError::Validation);
                }
                let command = LabelEffectCommand::new(
                    request.target_id,
                    observed.pod_name.clone(),
                    request.target_instance_id.to_string(),
                    request.desired_role.to_string(),
                );
                let mut prepared = self.clone();
                prepared
                    .set_prepared_label_command(command)
                    .map_err(|_| PreparedActivityError::Validation)?;
                prepared
                    .validate(definition)
                    .map_err(|_| PreparedActivityError::Validation)?;
                Ok(prepared)
            }
        }
    }

    pub fn encode_effect_observation(
        &self,
        observation: EffectObservation,
    ) -> Result<ExactBytes, String> {
        let observation = observation.bounded();
        match self {
            Self::RevokeWrites(_) => {
                encode_replica_observation::<RevokeWritesActivity>(observation)
            }
            Self::DemoteOldPrimary(_) => {
                encode_replica_observation::<DemoteOldPrimaryActivity>(observation)
            }
            Self::PromoteTarget(_) => {
                encode_replica_observation::<PromoteTargetActivity>(observation)
            }
            Self::DistributeReplicaEpoch(_) => {
                encode_replica_observation::<DistributeReplicaEpochActivity>(observation)
            }
            Self::InstallTargetCatchUpConfiguration(_) => {
                encode_replica_observation::<InstallTargetCatchUpConfigurationActivity>(observation)
            }
            Self::WaitTargetWriteQuorum(_) => {
                encode_replica_observation::<WaitTargetWriteQuorumActivity>(observation)
            }
            Self::InstallTargetCurrentConfiguration(_) => {
                encode_replica_observation::<InstallTargetCurrentConfigurationActivity>(observation)
            }
            Self::RestorePreviousCurrentConfiguration(_) => encode_replica_observation::<
                RestorePreviousCurrentConfigurationActivity,
            >(observation),
            Self::CompensatePromoteOldPrimary(_) => {
                encode_replica_observation::<CompensatePromoteOldPrimaryActivity>(observation)
            }
            Self::CompensateDistributeReplicaEpoch(_) => {
                encode_replica_observation::<CompensateDistributeReplicaEpochActivity>(observation)
            }
            Self::InstallCompensationCatchUpConfiguration(_) => encode_replica_observation::<
                InstallCompensationCatchUpConfigurationActivity,
            >(observation),
            Self::InstallCompensationCurrentConfiguration(_) => encode_replica_observation::<
                InstallCompensationCurrentConfigurationActivity,
            >(observation),
            Self::PublishTargetPrimaryLabel(_) => {
                encode_label_observation::<PublishTargetPrimaryLabelActivity>(observation)
            }
            Self::PublishOldPrimarySecondaryLabel(_) => {
                encode_label_observation::<PublishOldPrimarySecondaryLabelActivity>(observation)
            }
            Self::RestoreOldPrimaryLabel(_) => {
                encode_label_observation::<RestoreOldPrimaryLabelActivity>(observation)
            }
            Self::RestoreTargetSecondaryLabel(_) => {
                encode_label_observation::<RestoreTargetSecondaryLabelActivity>(observation)
            }
            Self::CaptureFrozenLsn(_)
            | Self::WaitTargetCaughtUp(_)
            | Self::AttestTargetTopology(_)
            | Self::AttestCompensatedTopology(_) => {
                Err("passive direct activity cannot encode an effect observation".to_string())
            }
        }
    }

    pub fn prepared_replica_command(&self) -> Option<&ReplicaEffectCommand> {
        match self {
            Self::RevokeWrites(input) => RevokeWritesActivity::prepared_command(input),
            Self::DemoteOldPrimary(input) => DemoteOldPrimaryActivity::prepared_command(input),
            Self::PromoteTarget(input) => PromoteTargetActivity::prepared_command(input),
            Self::DistributeReplicaEpoch(input) => {
                DistributeReplicaEpochActivity::prepared_command(input)
            }
            Self::InstallTargetCatchUpConfiguration(input) => {
                InstallTargetCatchUpConfigurationActivity::prepared_command(input)
            }
            Self::WaitTargetWriteQuorum(input) => {
                WaitTargetWriteQuorumActivity::prepared_command(input)
            }
            Self::InstallTargetCurrentConfiguration(input) => {
                InstallTargetCurrentConfigurationActivity::prepared_command(input)
            }
            Self::RestorePreviousCurrentConfiguration(input) => {
                RestorePreviousCurrentConfigurationActivity::prepared_command(input)
            }
            Self::CompensatePromoteOldPrimary(input) => {
                CompensatePromoteOldPrimaryActivity::prepared_command(input)
            }
            Self::CompensateDistributeReplicaEpoch(input) => {
                CompensateDistributeReplicaEpochActivity::prepared_command(input)
            }
            Self::InstallCompensationCatchUpConfiguration(input) => {
                InstallCompensationCatchUpConfigurationActivity::prepared_command(input)
            }
            Self::InstallCompensationCurrentConfiguration(input) => {
                InstallCompensationCurrentConfigurationActivity::prepared_command(input)
            }
            _ => None,
        }
    }

    pub fn prepared_label_command(&self) -> Option<&LabelEffectCommand> {
        match self {
            Self::PublishTargetPrimaryLabel(input) => {
                PublishTargetPrimaryLabelActivity::prepared_command(input)
            }
            Self::PublishOldPrimarySecondaryLabel(input) => {
                PublishOldPrimarySecondaryLabelActivity::prepared_command(input)
            }
            Self::RestoreOldPrimaryLabel(input) => {
                RestoreOldPrimaryLabelActivity::prepared_command(input)
            }
            Self::RestoreTargetSecondaryLabel(input) => {
                RestoreTargetSecondaryLabelActivity::prepared_command(input)
            }
            _ => None,
        }
    }

    fn is_effect(&self) -> bool {
        !matches!(
            self,
            Self::CaptureFrozenLsn(_)
                | Self::WaitTargetCaughtUp(_)
                | Self::AttestTargetTopology(_)
                | Self::AttestCompensatedTopology(_)
        )
    }

    fn clear_prepared_command(&mut self) {
        match self {
            Self::RevokeWrites(input) => *RevokeWritesActivity::prepared_command_mut(input) = None,
            Self::DemoteOldPrimary(input) => {
                *DemoteOldPrimaryActivity::prepared_command_mut(input) = None
            }
            Self::PromoteTarget(input) => {
                *PromoteTargetActivity::prepared_command_mut(input) = None
            }
            Self::DistributeReplicaEpoch(input) => {
                *DistributeReplicaEpochActivity::prepared_command_mut(input) = None
            }
            Self::InstallTargetCatchUpConfiguration(input) => {
                *InstallTargetCatchUpConfigurationActivity::prepared_command_mut(input) = None
            }
            Self::WaitTargetWriteQuorum(input) => {
                *WaitTargetWriteQuorumActivity::prepared_command_mut(input) = None
            }
            Self::InstallTargetCurrentConfiguration(input) => {
                *InstallTargetCurrentConfigurationActivity::prepared_command_mut(input) = None
            }
            Self::RestorePreviousCurrentConfiguration(input) => {
                *RestorePreviousCurrentConfigurationActivity::prepared_command_mut(input) = None
            }
            Self::CompensatePromoteOldPrimary(input) => {
                *CompensatePromoteOldPrimaryActivity::prepared_command_mut(input) = None
            }
            Self::CompensateDistributeReplicaEpoch(input) => {
                *CompensateDistributeReplicaEpochActivity::prepared_command_mut(input) = None
            }
            Self::InstallCompensationCatchUpConfiguration(input) => {
                *InstallCompensationCatchUpConfigurationActivity::prepared_command_mut(input) = None
            }
            Self::InstallCompensationCurrentConfiguration(input) => {
                *InstallCompensationCurrentConfigurationActivity::prepared_command_mut(input) = None
            }
            Self::PublishTargetPrimaryLabel(input) => {
                *PublishTargetPrimaryLabelActivity::prepared_command_mut(input) = None
            }
            Self::PublishOldPrimarySecondaryLabel(input) => {
                *PublishOldPrimarySecondaryLabelActivity::prepared_command_mut(input) = None
            }
            Self::RestoreOldPrimaryLabel(input) => {
                *RestoreOldPrimaryLabelActivity::prepared_command_mut(input) = None
            }
            Self::RestoreTargetSecondaryLabel(input) => {
                *RestoreTargetSecondaryLabelActivity::prepared_command_mut(input) = None
            }
            Self::CaptureFrozenLsn(_)
            | Self::WaitTargetCaughtUp(_)
            | Self::AttestTargetTopology(_)
            | Self::AttestCompensatedTopology(_) => {}
        }
    }

    fn set_prepared_replica_command(
        &mut self,
        command: ReplicaEffectCommand,
    ) -> Result<(), String> {
        match self {
            Self::RevokeWrites(input) => {
                *RevokeWritesActivity::prepared_command_mut(input) = Some(command)
            }
            Self::DemoteOldPrimary(input) => {
                *DemoteOldPrimaryActivity::prepared_command_mut(input) = Some(command)
            }
            Self::PromoteTarget(input) => {
                *PromoteTargetActivity::prepared_command_mut(input) = Some(command)
            }
            Self::DistributeReplicaEpoch(input) => {
                *DistributeReplicaEpochActivity::prepared_command_mut(input) = Some(command)
            }
            Self::InstallTargetCatchUpConfiguration(input) => {
                *InstallTargetCatchUpConfigurationActivity::prepared_command_mut(input) =
                    Some(command)
            }
            Self::WaitTargetWriteQuorum(input) => {
                *WaitTargetWriteQuorumActivity::prepared_command_mut(input) = Some(command)
            }
            Self::InstallTargetCurrentConfiguration(input) => {
                *InstallTargetCurrentConfigurationActivity::prepared_command_mut(input) =
                    Some(command)
            }
            Self::RestorePreviousCurrentConfiguration(input) => {
                *RestorePreviousCurrentConfigurationActivity::prepared_command_mut(input) =
                    Some(command)
            }
            Self::CompensatePromoteOldPrimary(input) => {
                *CompensatePromoteOldPrimaryActivity::prepared_command_mut(input) = Some(command)
            }
            Self::CompensateDistributeReplicaEpoch(input) => {
                *CompensateDistributeReplicaEpochActivity::prepared_command_mut(input) =
                    Some(command)
            }
            Self::InstallCompensationCatchUpConfiguration(input) => {
                *InstallCompensationCatchUpConfigurationActivity::prepared_command_mut(input) =
                    Some(command)
            }
            Self::InstallCompensationCurrentConfiguration(input) => {
                *InstallCompensationCurrentConfigurationActivity::prepared_command_mut(input) =
                    Some(command)
            }
            _ => return Err("direct activity is not a replica effect".to_string()),
        }
        Ok(())
    }

    fn set_prepared_label_command(&mut self, command: LabelEffectCommand) -> Result<(), String> {
        match self {
            Self::PublishTargetPrimaryLabel(input) => {
                *PublishTargetPrimaryLabelActivity::prepared_command_mut(input) = Some(command)
            }
            Self::PublishOldPrimarySecondaryLabel(input) => {
                *PublishOldPrimarySecondaryLabelActivity::prepared_command_mut(input) =
                    Some(command)
            }
            Self::RestoreOldPrimaryLabel(input) => {
                *RestoreOldPrimaryLabelActivity::prepared_command_mut(input) = Some(command)
            }
            Self::RestoreTargetSecondaryLabel(input) => {
                *RestoreTargetSecondaryLabelActivity::prepared_command_mut(input) = Some(command)
            }
            _ => return Err("direct activity is not a label effect".to_string()),
        }
        Ok(())
    }

    fn label_request<'a>(
        &'a self,
        definition: &DirectSwitchoverDefinition,
    ) -> Result<LabelRequest<'a>, String> {
        match self {
            Self::PublishTargetPrimaryLabel(input) => {
                PublishTargetPrimaryLabelActivity::request(input, definition)
            }
            Self::PublishOldPrimarySecondaryLabel(input) => {
                PublishOldPrimarySecondaryLabelActivity::request(input, definition)
            }
            Self::RestoreOldPrimaryLabel(input) => {
                RestoreOldPrimaryLabelActivity::request(input, definition)
            }
            Self::RestoreTargetSecondaryLabel(input) => {
                RestoreTargetSecondaryLabelActivity::request(input, definition)
            }
            _ => Err("direct activity is not a label effect".to_string()),
        }
    }
}

struct ReplicaRequest<'a> {
    contract_version: u32,
    execution_id: &'a str,
    sequence: u32,
    target_id: i64,
    target_instance_id: &'a str,
    expected_epoch: EpochStatus,
    desired_snapshot: StablePartitionSnapshotStatus,
    deadline_unix_seconds: i64,
    redelivery: u8,
    prepared_command: Option<&'a ReplicaEffectCommand>,
}

trait ReplicaOperation: ReplicaDirectActivity {
    fn request<'a>(
        input: &'a Self::Input,
        definition: &DirectSwitchoverDefinition,
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
    execution_id: &'a str,
    sequence: u32,
    target_id: i64,
    target_instance_id: &'a str,
    expected_epoch: EpochStatus,
    desired_snapshot: StablePartitionSnapshotStatus,
    deadline_unix_seconds: i64,
    redelivery: u8,
    prepared_command: Option<&'a ReplicaEffectCommand>,
) -> ReplicaRequest<'a> {
    ReplicaRequest {
        contract_version,
        execution_id,
        sequence,
        target_id,
        target_instance_id,
        expected_epoch,
        desired_snapshot,
        deadline_unix_seconds,
        redelivery,
        prepared_command,
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
    ) -> Result<ReplicaRequest<'a>, String> {
        validate_target(
            input.old_primary_id,
            &input.old_primary_instance_id,
            definition.old_primary_id,
            definition,
        )?;
        Ok(fixed_replica_request(
            input.contract_version,
            &input.execution_id,
            1,
            input.old_primary_id,
            &input.old_primary_instance_id,
            definition.previous_snapshot.epoch.clone(),
            definition.previous_snapshot.clone(),
            input.deadline_unix_seconds,
            input.redelivery,
            input.prepared_command.as_ref(),
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
    ) -> Result<ReplicaRequest<'a>, String> {
        validate_target(
            input.old_primary_id,
            &input.old_primary_instance_id,
            definition.old_primary_id,
            definition,
        )?;
        Ok(fixed_replica_request(
            input.contract_version,
            &input.execution_id,
            2,
            input.old_primary_id,
            &input.old_primary_instance_id,
            definition.target_snapshot.epoch.clone(),
            definition.target_snapshot.clone(),
            input.deadline_unix_seconds,
            input.redelivery,
            input.prepared_command.as_ref(),
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
    ) -> Result<ReplicaRequest<'a>, String> {
        validate_target(
            input.target_primary_id,
            &input.target_primary_instance_id,
            definition.target_primary_id,
            definition,
        )?;
        Ok(fixed_replica_request(
            input.contract_version,
            &input.execution_id,
            3,
            input.target_primary_id,
            &input.target_primary_instance_id,
            definition.previous_snapshot.epoch.clone(),
            definition.target_snapshot.clone(),
            input.deadline_unix_seconds,
            input.redelivery,
            input.prepared_command.as_ref(),
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
            input.contract_version,
            &input.execution_id,
            100 + u32::from(input.distribution_index),
            input.replica_id,
            &input.replica_instance_id,
            definition.target_snapshot.epoch.clone(),
            definition.target_snapshot.clone(),
            input.deadline_unix_seconds,
            input.redelivery,
            input.prepared_command.as_ref(),
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
    ) -> Result<ReplicaRequest<'a>, String> {
        target_replica_request(
            input.contract_version,
            &input.execution_id,
            1000,
            input.target_primary_id,
            &input.target_primary_instance_id,
            input.deadline_unix_seconds,
            input.redelivery,
            input.prepared_command.as_ref(),
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
    ) -> Result<ReplicaRequest<'a>, String> {
        target_replica_request(
            input.contract_version,
            &input.execution_id,
            1001,
            input.target_primary_id,
            &input.target_primary_instance_id,
            input.deadline_unix_seconds,
            input.redelivery,
            input.prepared_command.as_ref(),
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
    ) -> Result<ReplicaRequest<'a>, String> {
        target_replica_request(
            input.contract_version,
            &input.execution_id,
            1002,
            input.target_primary_id,
            &input.target_primary_instance_id,
            input.deadline_unix_seconds,
            input.redelivery,
            input.prepared_command.as_ref(),
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
    ) -> Result<ReplicaRequest<'a>, String> {
        old_primary_replica_request(
            input.contract_version,
            &input.execution_id,
            1500,
            input.old_primary_id,
            &input.old_primary_instance_id,
            definition.previous_snapshot.epoch.clone(),
            definition.previous_snapshot.clone(),
            input.deadline_unix_seconds,
            input.redelivery,
            input.prepared_command.as_ref(),
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
    ) -> Result<ReplicaRequest<'a>, String> {
        old_primary_replica_request(
            input.contract_version,
            &input.execution_id,
            2000,
            input.old_primary_id,
            &input.old_primary_instance_id,
            definition.target_snapshot.epoch.clone(),
            definition.compensation_snapshot(),
            input.deadline_unix_seconds,
            input.redelivery,
            input.prepared_command.as_ref(),
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
            input.contract_version,
            &input.execution_id,
            2100 + u32::from(input.distribution_index),
            input.replica_id,
            &input.replica_instance_id,
            definition.target_snapshot.epoch.clone(),
            definition.compensation_snapshot(),
            input.deadline_unix_seconds,
            input.redelivery,
            input.prepared_command.as_ref(),
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
    ) -> Result<ReplicaRequest<'a>, String> {
        old_primary_replica_request(
            input.contract_version,
            &input.execution_id,
            2001,
            input.old_primary_id,
            &input.old_primary_instance_id,
            definition.target_snapshot.epoch.clone(),
            definition.compensation_snapshot(),
            input.deadline_unix_seconds,
            input.redelivery,
            input.prepared_command.as_ref(),
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
    ) -> Result<ReplicaRequest<'a>, String> {
        old_primary_replica_request(
            input.contract_version,
            &input.execution_id,
            2002,
            input.old_primary_id,
            &input.old_primary_instance_id,
            definition.target_snapshot.epoch.clone(),
            definition.compensation_snapshot(),
            input.deadline_unix_seconds,
            input.redelivery,
            input.prepared_command.as_ref(),
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
    execution_id: &'a str,
    sequence: u32,
    target_primary_id: i64,
    target_primary_instance_id: &'a str,
    deadline_unix_seconds: i64,
    redelivery: u8,
    prepared_command: Option<&'a ReplicaEffectCommand>,
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
        redelivery,
        prepared_command,
    ))
}

#[allow(clippy::too_many_arguments)]
fn old_primary_replica_request<'a>(
    contract_version: u32,
    execution_id: &'a str,
    sequence: u32,
    old_primary_id: i64,
    old_primary_instance_id: &'a str,
    expected_epoch: EpochStatus,
    desired_snapshot: StablePartitionSnapshotStatus,
    deadline_unix_seconds: i64,
    redelivery: u8,
    prepared_command: Option<&'a ReplicaEffectCommand>,
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
        redelivery,
        prepared_command,
    ))
}

struct LabelRequest<'a> {
    contract_version: u32,
    execution_id: &'a str,
    sequence: u32,
    target_id: i64,
    target_instance_id: &'a str,
    desired_role: &'static str,
    deadline_unix_seconds: i64,
    prepared_command: Option<&'a LabelEffectCommand>,
}

trait LabelOperation: LabelDirectActivity {
    fn request<'a>(
        input: &'a Self::Input,
        definition: &DirectSwitchoverDefinition,
    ) -> Result<LabelRequest<'a>, String>;
}

macro_rules! impl_label_operation {
    (
        $activity:ty,
        $input:ty,
        $id_field:ident,
        $instance_field:ident,
        $expected_id:expr,
        $sequence:expr,
        $role:literal
    ) => {
        impl LabelOperation for $activity {
            fn request<'a>(
                input: &'a $input,
                definition: &DirectSwitchoverDefinition,
            ) -> Result<LabelRequest<'a>, String> {
                let expected_id = $expected_id(definition);
                validate_target(
                    input.$id_field,
                    &input.$instance_field,
                    expected_id,
                    definition,
                )?;
                Ok(LabelRequest {
                    contract_version: input.contract_version,
                    execution_id: &input.execution_id,
                    sequence: $sequence,
                    target_id: input.$id_field,
                    target_instance_id: &input.$instance_field,
                    desired_role: $role,
                    deadline_unix_seconds: input.deadline_unix_seconds,
                    prepared_command: input.prepared_command.as_ref(),
                })
            }
        }
    };
}

impl_label_operation!(
    PublishTargetPrimaryLabelActivity,
    PublishTargetPrimaryLabelInput,
    target_primary_id,
    target_primary_instance_id,
    |definition: &DirectSwitchoverDefinition| definition.target_primary_id,
    1003,
    "primary"
);
impl_label_operation!(
    PublishOldPrimarySecondaryLabelActivity,
    PublishOldPrimarySecondaryLabelInput,
    old_primary_id,
    old_primary_instance_id,
    |definition: &DirectSwitchoverDefinition| definition.old_primary_id,
    1004,
    "secondary"
);
impl_label_operation!(
    RestoreOldPrimaryLabelActivity,
    RestoreOldPrimaryLabelInput,
    old_primary_id,
    old_primary_instance_id,
    |definition: &DirectSwitchoverDefinition| definition.old_primary_id,
    2003,
    "primary"
);
impl_label_operation!(
    RestoreTargetSecondaryLabelActivity,
    RestoreTargetSecondaryLabelInput,
    target_primary_id,
    target_primary_instance_id,
    |definition: &DirectSwitchoverDefinition| definition.target_primary_id,
    2004,
    "secondary"
);

fn validate_replica<A: ReplicaOperation>(
    input: &A::Input,
    definition: &DirectSwitchoverDefinition,
) -> Result<(), String> {
    let request = A::request(input, definition)?;
    validate_common(
        request.contract_version,
        request.execution_id,
        request.deadline_unix_seconds,
        definition,
    )?;
    if request.redelivery > 1 {
        return Err("direct replica request has invalid redelivery".to_string());
    }
    let Some(command) = request.prepared_command else {
        return Ok(());
    };
    let pending = pending::<A>(&request);
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
    input: &A::Input,
    definition: &DirectSwitchoverDefinition,
    observations: &OperationObservations,
    now: i64,
) -> Result<DirectEvaluation, String> {
    let request = A::request(input, definition)?;
    let Some(observed) = observations.get(&request.target_id) else {
        return deadline_or_wait(request.deadline_unix_seconds, now, || {
            encode_replica_observation::<A>(EffectObservation::UnavailableAtDeadline {
                observed_at_unix_seconds: now,
                message: format!(
                    "direct switchover replica {} is unavailable at deadline",
                    request.target_id
                ),
            })
        });
    };
    if observed.status.instance_id.as_str() != request.target_instance_id {
        return encode_replica_observation::<A>(EffectObservation::Conflicting {
            observed_at_unix_seconds: now,
            message: format!(
                "direct switchover replica {} incarnation changed",
                request.target_id
            ),
        })
        .map(DirectEvaluation::Observe);
    }
    if let Some(recorded) =
        correlated_action_observation(&observed.status, &action_id(definition, request.sequence))
    {
        let action = match A::action(&request, definition, observations) {
            Ok(action) => action,
            Err(error) => {
                return deadline_or_wait(request.deadline_unix_seconds, now, || {
                    encode_replica_observation::<A>(EffectObservation::DeadlineExceeded {
                        observed_at_unix_seconds: now,
                        message: error,
                    })
                });
            }
        };
        if recorded.signature != action.signature() {
            return encode_replica_observation::<A>(EffectObservation::Conflicting {
                observed_at_unix_seconds: now,
                message: "correlated action signature conflicts with direct request".to_string(),
            })
            .map(DirectEvaluation::Observe);
        }
        return match recorded.state {
            DurableActionState::Completed => {
                encode_replica_observation::<A>(EffectObservation::Applied {
                    observed_at_unix_seconds: now,
                })
                .map(DirectEvaluation::Observe)
            }
            DurableActionState::Failed => {
                encode_replica_observation::<A>(EffectObservation::Failed {
                    observed_at_unix_seconds: now,
                    message: recorded.error.clone().unwrap_or_else(|| {
                        "correlated direct switchover action failed".to_string()
                    }),
                })
                .map(DirectEvaluation::Observe)
            }
            DurableActionState::Scheduled | DurableActionState::InProgress => {
                deadline_or_wait(request.deadline_unix_seconds, now, || {
                    encode_replica_observation::<A>(EffectObservation::DeadlineExceeded {
                        observed_at_unix_seconds: now,
                        message: "correlated direct switchover action reached its deadline"
                            .to_string(),
                    })
                })
            }
        };
    }
    match A::status_relation(&request, definition, &observed.status) {
        StatusRelation::Postcondition => {
            encode_replica_observation::<A>(EffectObservation::Applied {
                observed_at_unix_seconds: now,
            })
            .map(DirectEvaluation::Observe)
        }
        StatusRelation::Precondition if now >= request.deadline_unix_seconds => {
            encode_replica_observation::<A>(EffectObservation::DeadlineExceeded {
                observed_at_unix_seconds: now,
                message: format!(
                    "direct switchover action {:?} reached its deadline",
                    A::action_kind()
                ),
            })
            .map(DirectEvaluation::Observe)
        }
        StatusRelation::Precondition => {
            let action = match A::action(&request, definition, observations) {
                Ok(action) => action,
                Err(_) => return Ok(DirectEvaluation::AwaitEvidence),
            };
            Ok(DirectEvaluation::DispatchReplica {
                action,
                pending: Box::new(pending::<A>(&request)),
            })
        }
        StatusRelation::Conflicting(message) => {
            encode_replica_observation::<A>(EffectObservation::Conflicting {
                observed_at_unix_seconds: now,
                message,
            })
            .map(DirectEvaluation::Observe)
        }
    }
}

fn evaluate_quarantined_replica<A: ReplicaOperation>(
    input: &A::Input,
    definition: &DirectSwitchoverDefinition,
    observations: &OperationObservations,
    now: i64,
) -> Result<DirectEvaluation, String> {
    let request = A::request(input, definition)?;
    let Some(command) = request.prepared_command else {
        return match evaluate_replica::<A>(input, definition, observations, now)? {
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
                    return encode_replica_observation::<A>(EffectObservation::Applied {
                        observed_at_unix_seconds: now,
                    })
                    .map(DirectEvaluation::Observe);
                }
                DurableActionState::Failed => {
                    return encode_replica_observation::<A>(EffectObservation::Failed {
                        observed_at_unix_seconds: now,
                        message: recorded.error.clone().unwrap_or_else(|| {
                            "correlated direct switchover action failed".to_string()
                        }),
                    })
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
        return encode_replica_observation::<A>(EffectObservation::Applied {
            observed_at_unix_seconds: now,
        })
        .map(DirectEvaluation::Observe);
    }
    if command_generation_change_proves_no_admission(
        &command.expected_agent_generation,
        &command.action_id,
        &observed.status,
    ) {
        return encode_replica_observation::<A>(EffectObservation::ProvenNoAdmission {
            observed_at_unix_seconds: now,
        })
        .map(DirectEvaluation::Observe);
    }
    Ok(DirectEvaluation::AwaitEvidence)
}

fn pending<A: ReplicaOperation>(request: &ReplicaRequest<'_>) -> PendingActionStatus {
    let mut pending = PendingActionStatus {
        action_id: request_action_id(request),
        sequence: request.sequence,
        kind: A::action_kind(),
        target_id: request.target_id,
        target_instance_id: request.target_instance_id.to_string(),
        expected_epoch: request.expected_epoch.clone(),
        desired_postcondition: A::postcondition(),
        attempts: u32::from(request.redelivery),
        deadline_unix_seconds: request.deadline_unix_seconds,
        last_error: None,
        dispatch_authorized: request.prepared_command.is_some(),
        dispatch_agent_generation: None,
        dispatch_agent_control_version: None,
        dispatch_observed_runtime_epoch: None,
        dispatch_action_payload: String::new(),
    };
    if let Some(command) = request.prepared_command {
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
    input: &A::Input,
    definition: &DirectSwitchoverDefinition,
) -> Result<(), String> {
    let request = A::request(input, definition)?;
    validate_common(
        request.contract_version,
        request.execution_id,
        request.deadline_unix_seconds,
        definition,
    )?;
    let Some(command) = request.prepared_command else {
        return Ok(());
    };
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
    input: &A::Input,
    definition: &DirectSwitchoverDefinition,
    observations: &OperationObservations,
    now: i64,
) -> Result<DirectEvaluation, String> {
    let request = A::request(input, definition)?;
    let Some(observed) = observations.get(&request.target_id) else {
        return deadline_or_wait(request.deadline_unix_seconds, now, || {
            encode_label_observation::<A>(EffectObservation::UnavailableAtDeadline {
                observed_at_unix_seconds: now,
                message: format!(
                    "direct switchover label target {} is unavailable at deadline",
                    request.target_id
                ),
            })
        });
    };
    if observed.status.instance_id.as_str() != request.target_instance_id {
        return encode_label_observation::<A>(EffectObservation::Conflicting {
            observed_at_unix_seconds: now,
            message: "direct switchover label target incarnation changed".to_string(),
        })
        .map(DirectEvaluation::Observe);
    }
    let expected_role = label_role(request.desired_role);
    if observed.status.epoch != epoch(&definition.target_snapshot.epoch)
        || observed.status.role != expected_role
    {
        return encode_label_observation::<A>(EffectObservation::Conflicting {
            observed_at_unix_seconds: now,
            message: "direct switchover label target runtime role is not exact".to_string(),
        })
        .map(DirectEvaluation::Observe);
    }
    if observed.pod_role_label.as_deref() == Some(request.desired_role) {
        encode_label_observation::<A>(EffectObservation::Applied {
            observed_at_unix_seconds: now,
        })
        .map(DirectEvaluation::Observe)
    } else if now >= request.deadline_unix_seconds {
        encode_label_observation::<A>(EffectObservation::DeadlineExceeded {
            observed_at_unix_seconds: now,
            message: format!(
                "direct switchover label action {} reached its deadline",
                request.sequence
            ),
        })
        .map(DirectEvaluation::Observe)
    } else {
        Ok(DirectEvaluation::DispatchLabel)
    }
}

fn evaluate_quarantined_label<A: LabelOperation>(
    input: &A::Input,
    definition: &DirectSwitchoverDefinition,
    observations: &OperationObservations,
    now: i64,
) -> Result<DirectEvaluation, String> {
    let request = A::request(input, definition)?;
    if request.prepared_command.is_none() {
        return match evaluate_label::<A>(input, definition, observations, now)? {
            observation @ DirectEvaluation::Observe(_) => Ok(observation),
            DirectEvaluation::AwaitEvidence
            | DirectEvaluation::DispatchReplica { .. }
            | DirectEvaluation::DispatchLabel => Ok(DirectEvaluation::AwaitEvidence),
        };
    }
    let Some(observed) = observations.get(&request.target_id) else {
        return Ok(DirectEvaluation::AwaitEvidence);
    };
    if observed.status.instance_id.as_str() == request.target_instance_id
        && observed.status.epoch == epoch(&definition.target_snapshot.epoch)
        && observed.status.role == label_role(request.desired_role)
        && observed.pod_role_label.as_deref() == Some(request.desired_role)
    {
        return encode_label_observation::<A>(EffectObservation::Applied {
            observed_at_unix_seconds: now,
        })
        .map(DirectEvaluation::Observe);
    }
    Ok(DirectEvaluation::AwaitEvidence)
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

enum StatusRelation {
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

fn decode<A: DurableActivity>(spec: &ActivitySpec) -> Result<A::Input, String> {
    validate_spec_identity::<A>(spec)?;
    decode_activity_input::<A>(spec.input())
        .map_err(|error| format!("decode {} activity input: {error}", A::NAME))
}

fn validate_spec_identity<A: DurableActivity>(spec: &ActivitySpec) -> Result<(), String> {
    if spec.name().name() != A::NAME
        || spec.name().version() != A::VERSION
        || spec.max_result_bytes() != A::MAX_RESULT_BYTES
    {
        return Err(format!(
            "{} activity identity or result bound changed",
            A::NAME
        ));
    }
    Ok(())
}

fn spec<A: DurableActivity>(input: &A::Input) -> Result<ActivitySpec, String> {
    Ok(ActivitySpec::new(
        ActivityName::new(A::NAME, A::VERSION)
            .map_err(|error| format!("construct {} activity identity: {error}", A::NAME))?,
        encode_activity_input::<A>(input)
            .map_err(|error| format!("encode {} activity input: {error}", A::NAME))?,
        A::MAX_RESULT_BYTES,
    ))
}

fn encode_replica_observation<A: ReplicaDirectActivity>(
    observation: EffectObservation,
) -> Result<ExactBytes, String> {
    let output = A::output(observation.bounded())?;
    encode_activity_result::<A>(&output)
        .map_err(|error| format!("encode {} result: {error}", A::NAME))
}

fn encode_label_observation<A: LabelDirectActivity>(
    observation: EffectObservation,
) -> Result<ExactBytes, String> {
    let output = A::output(observation.bounded())?;
    encode_activity_result::<A>(&output)
        .map_err(|error| format!("encode {} result: {error}", A::NAME))
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
) -> Result<DirectEvaluation, String> {
    let output = match observations.get(&input.old_primary_id) {
        None if now < input.deadline_unix_seconds => return Ok(DirectEvaluation::AwaitEvidence),
        None => CaptureFrozenLsnOutput::DeadlineExceeded {
            observed_at_unix_seconds: now,
            message: "old primary was unavailable at frozen-LSN deadline".to_string(),
        },
        Some(observed)
            if observed.status.instance_id.as_str() != input.old_primary_instance_id
                || observed.status.epoch != epoch(&input.expected_epoch)
                || observed.status.role != Role::Primary
                || observed.status.write_status != AccessStatus::ReconfigurationPending =>
        {
            CaptureFrozenLsnOutput::Conflicting {
                observed_at_unix_seconds: now,
                message: "old primary frozen-LSN observation is not exact".to_string(),
            }
        }
        Some(observed) => CaptureFrozenLsnOutput::Captured {
            frozen_lsn: observed.status.current_progress,
            observed_at_unix_seconds: now,
        },
    };
    encode_activity_result::<CaptureFrozenLsnActivity>(&output)
        .map(DirectEvaluation::Observe)
        .map_err(|error| format!("encode capture-frozen-lsn result: {error}"))
}

fn evaluate_target_catch_up(
    input: &WaitTargetCaughtUpInput,
    observations: &OperationObservations,
    now: i64,
) -> Result<DirectEvaluation, String> {
    let output = match observations.get(&input.target_id) {
        None if now < input.deadline_unix_seconds => return Ok(DirectEvaluation::AwaitEvidence),
        None => WaitTargetCaughtUpOutput::DeadlineExceeded {
            observed_at_unix_seconds: now,
            message: "target was unavailable at catch-up deadline".to_string(),
        },
        Some(observed)
            if observed.status.instance_id.as_str() != input.target_instance_id
                || observed.status.epoch != epoch(&input.expected_epoch)
                || observed.status.role != Role::ActiveSecondary =>
        {
            WaitTargetCaughtUpOutput::Conflicting {
                observed_at_unix_seconds: now,
                message: "target catch-up observation is not exact".to_string(),
            }
        }
        Some(observed) if observed.status.current_progress >= input.frozen_lsn => {
            WaitTargetCaughtUpOutput::CaughtUp {
                observed_at_unix_seconds: now,
            }
        }
        Some(_) if now < input.deadline_unix_seconds => {
            return Ok(DirectEvaluation::AwaitEvidence);
        }
        Some(_) => WaitTargetCaughtUpOutput::DeadlineExceeded {
            observed_at_unix_seconds: now,
            message: "target did not reach the frozen LSN before deadline".to_string(),
        },
    };
    encode_activity_result::<WaitTargetCaughtUpActivity>(&output)
        .map(DirectEvaluation::Observe)
        .map_err(|error| format!("encode wait-target-caught-up result: {error}"))
}

#[allow(clippy::too_many_arguments)]
fn evaluate_attestation<A, FAttested, FDeadline, FConflicting>(
    snapshot: &StablePartitionSnapshotStatus,
    deadline_unix_seconds: i64,
    observations: &OperationObservations,
    now: i64,
    attested: FAttested,
    deadline: FDeadline,
    conflicting: FConflicting,
) -> Result<DirectEvaluation, String>
where
    A: DurableActivity,
    FAttested: FnOnce(i64, StablePartitionSnapshotStatus) -> A::Output,
    FDeadline: FnOnce(i64, String) -> A::Output,
    FConflicting: FnOnce(i64, String) -> A::Output,
{
    let result = match attestation_error(snapshot, observations) {
        Ok(()) => attested(
            now,
            crate::reconciler::snapshot_with_observed_metadata(snapshot.clone(), observations),
        ),
        Err(AttestationError::Unavailable(_)) if now < deadline_unix_seconds => {
            return Ok(DirectEvaluation::AwaitEvidence);
        }
        Err(AttestationError::Unavailable(message)) => deadline(now, message),
        Err(AttestationError::Conflicting(message)) => conflicting(now, message),
    };
    encode_activity_result::<A>(&result)
        .map(DirectEvaluation::Observe)
        .map_err(|error| format!("encode {} result: {error}", A::NAME))
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
