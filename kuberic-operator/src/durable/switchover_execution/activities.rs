use kuberic_durable_execution::{CompletionClass, DurableActivity, DurableEffect};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

use crate::crd::{EpochStatus, StablePartitionSnapshotStatus};
use crate::durable::effects::ReplicaEffectCommand;

pub const DIRECT_SWITCHOVER_CONTRACT_VERSION: u32 = 4;
pub const DIRECT_ACTIVITY_VERSION: u32 = 1;
pub const DIRECT_ACTIVITY_ERROR_MAX_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EffectApplied {
    pub observed_at_unix_seconds: i64,
}

pub trait SwitchoverActivityContract {
    type Request: Serialize + serde::de::DeserializeOwned;
    type Output: Serialize + serde::de::DeserializeOwned;
    const NAME: &'static str;
    const VERSION: u32;
    const MAX_REQUEST_BYTES: u64;
    const MAX_RESULT_BYTES: u64;
    const MAX_ERROR_MESSAGE_BYTES: u64;
    const COMPLETION_CLASS: CompletionClass;
}

pub(crate) trait StrictSwitchoverActivityContract: SwitchoverActivityContract {
    type Family;
}

pub(crate) struct ReplicaEffectFamily;

pub type RevokeWritesOutput = EffectApplied;
pub type DemoteOldPrimaryOutput = EffectApplied;
pub type PromoteTargetOutput = EffectApplied;
pub type DistributeReplicaEpochOutput = EffectApplied;
pub type InstallTargetCatchUpConfigurationOutput = EffectApplied;
pub type WaitTargetWriteQuorumOutput = EffectApplied;
pub type InstallTargetCurrentConfigurationOutput = EffectApplied;
pub type RestorePreviousCurrentConfigurationOutput = EffectApplied;
pub type CompensatePromoteOldPrimaryOutput = EffectApplied;
pub type CompensateDistributeReplicaEpochOutput = EffectApplied;
pub type InstallCompensationCatchUpConfigurationOutput = EffectApplied;
pub type InstallCompensationCurrentConfigurationOutput = EffectApplied;
pub type PublishTargetPrimaryLabelOutput = EffectApplied;
pub type PublishOldPrimarySecondaryLabelOutput = EffectApplied;
pub type RestoreOldPrimaryLabelOutput = EffectApplied;
pub type RestoreTargetSecondaryLabelOutput = EffectApplied;

macro_rules! define_replica_activity {
    ($activity:ident, $input:ty, $name:literal, $max_input:literal, $max_result:literal) => {
        pub struct $activity;

        impl SwitchoverActivityContract for $activity {
            type Request = $input;
            type Output = EffectApplied;

            const NAME: &'static str = $name;
            const VERSION: u32 = DIRECT_ACTIVITY_VERSION;
            const MAX_REQUEST_BYTES: u64 = $max_input;
            const MAX_RESULT_BYTES: u64 = $max_result;
            const MAX_ERROR_MESSAGE_BYTES: u64 = DIRECT_ACTIVITY_ERROR_MAX_BYTES as u64;
            const COMPLETION_CLASS: CompletionClass = CompletionClass::ExternalEffect;
        }
    };
}

macro_rules! define_label_activity {
    ($activity:ident, $input:ty, $name:literal, $max_input:literal, $max_result:literal) => {
        pub struct $activity;

        impl SwitchoverActivityContract for $activity {
            type Request = $input;
            type Output = EffectApplied;

            const NAME: &'static str = $name;
            const VERSION: u32 = DIRECT_ACTIVITY_VERSION;
            const MAX_REQUEST_BYTES: u64 = $max_input;
            const MAX_RESULT_BYTES: u64 = $max_result;
            const MAX_ERROR_MESSAGE_BYTES: u64 = DIRECT_ACTIVITY_ERROR_MAX_BYTES as u64;
            const COMPLETION_CLASS: CompletionClass = CompletionClass::ExternalEffect;
        }
    };
}

macro_rules! fixed_replica_input {
    ($input:ident, $target_id:ident, $target_instance_id:ident) => {
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        pub struct $input {
            pub $target_id: i64,
            pub $target_instance_id: String,
        }
    };
}

fixed_replica_input!(RevokeWritesInput, old_primary_id, old_primary_instance_id);
fixed_replica_input!(
    DemoteOldPrimaryInput,
    old_primary_id,
    old_primary_instance_id
);
fixed_replica_input!(
    PromoteTargetInput,
    target_primary_id,
    target_primary_instance_id
);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DistributeReplicaEpochInput {
    pub distribution_index: u8,
    pub replica_id: i64,
    pub replica_instance_id: String,
}

fixed_replica_input!(
    InstallTargetCatchUpConfigurationInput,
    target_primary_id,
    target_primary_instance_id
);
fixed_replica_input!(
    WaitTargetWriteQuorumInput,
    target_primary_id,
    target_primary_instance_id
);
fixed_replica_input!(
    InstallTargetCurrentConfigurationInput,
    target_primary_id,
    target_primary_instance_id
);
fixed_replica_input!(
    RestorePreviousCurrentConfigurationInput,
    old_primary_id,
    old_primary_instance_id
);
fixed_replica_input!(
    CompensatePromoteOldPrimaryInput,
    old_primary_id,
    old_primary_instance_id
);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompensateDistributeReplicaEpochInput {
    pub distribution_index: u8,
    pub replica_id: i64,
    pub replica_instance_id: String,
}

fixed_replica_input!(
    InstallCompensationCatchUpConfigurationInput,
    old_primary_id,
    old_primary_instance_id
);
fixed_replica_input!(
    InstallCompensationCurrentConfigurationInput,
    old_primary_id,
    old_primary_instance_id
);

macro_rules! fixed_label_input {
    ($input:ident, $target_id:ident, $target_instance_id:ident) => {
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        pub struct $input {
            pub $target_id: i64,
            pub $target_instance_id: String,
        }
    };
}

fixed_label_input!(
    PublishTargetPrimaryLabelInput,
    target_primary_id,
    target_primary_instance_id
);
fixed_label_input!(
    PublishOldPrimarySecondaryLabelInput,
    old_primary_id,
    old_primary_instance_id
);
fixed_label_input!(
    RestoreOldPrimaryLabelInput,
    old_primary_id,
    old_primary_instance_id
);
fixed_label_input!(
    RestoreTargetSecondaryLabelInput,
    target_primary_id,
    target_primary_instance_id
);

define_replica_activity!(
    RevokeWritesActivity,
    RevokeWritesInput,
    "kuberic.switchover.revoke-writes",
    6_144,
    1_024
);
define_replica_activity!(
    DemoteOldPrimaryActivity,
    DemoteOldPrimaryInput,
    "kuberic.switchover.demote-old-primary",
    6_144,
    1_024
);
define_replica_activity!(
    PromoteTargetActivity,
    PromoteTargetInput,
    "kuberic.switchover.promote-target",
    6_144,
    1_024
);
define_replica_activity!(
    DistributeReplicaEpochActivity,
    DistributeReplicaEpochInput,
    "kuberic.switchover.distribute-replica-epoch",
    6_144,
    1_024
);
define_replica_activity!(
    InstallTargetCatchUpConfigurationActivity,
    InstallTargetCatchUpConfigurationInput,
    "kuberic.switchover.install-target-catch-up-configuration",
    8_192,
    1_024
);
define_replica_activity!(
    WaitTargetWriteQuorumActivity,
    WaitTargetWriteQuorumInput,
    "kuberic.switchover.wait-target-write-quorum",
    6_144,
    1_024
);
define_replica_activity!(
    InstallTargetCurrentConfigurationActivity,
    InstallTargetCurrentConfigurationInput,
    "kuberic.switchover.install-target-current-configuration",
    8_192,
    1_024
);
define_replica_activity!(
    RestorePreviousCurrentConfigurationActivity,
    RestorePreviousCurrentConfigurationInput,
    "kuberic.switchover.restore-previous-current-configuration",
    8_192,
    1_024
);
define_replica_activity!(
    CompensatePromoteOldPrimaryActivity,
    CompensatePromoteOldPrimaryInput,
    "kuberic.switchover.compensate-promote-old-primary",
    6_144,
    1_024
);
define_replica_activity!(
    CompensateDistributeReplicaEpochActivity,
    CompensateDistributeReplicaEpochInput,
    "kuberic.switchover.compensate-distribute-replica-epoch",
    6_144,
    1_024
);
define_replica_activity!(
    InstallCompensationCatchUpConfigurationActivity,
    InstallCompensationCatchUpConfigurationInput,
    "kuberic.switchover.install-compensation-catch-up-configuration",
    8_192,
    1_024
);
define_replica_activity!(
    InstallCompensationCurrentConfigurationActivity,
    InstallCompensationCurrentConfigurationInput,
    "kuberic.switchover.install-compensation-current-configuration",
    8_192,
    1_024
);

define_label_activity!(
    PublishTargetPrimaryLabelActivity,
    PublishTargetPrimaryLabelInput,
    "kuberic.switchover.publish-target-primary-label",
    4_096,
    768
);

macro_rules! implement_strict_effect {
    ($activity:ty) => {
        impl DurableEffect for $activity {
            type Request = <Self as SwitchoverActivityContract>::Request;
            type Command = Option<ReplicaEffectCommand>;
            type Output = <Self as SwitchoverActivityContract>::Output;

            const NAME: &'static str = <Self as SwitchoverActivityContract>::NAME;
            const VERSION: u32 = <Self as SwitchoverActivityContract>::VERSION;
            const MAX_REQUEST_BYTES: u64 = <Self as SwitchoverActivityContract>::MAX_REQUEST_BYTES;
            const MAX_COMMAND_BYTES: u64 = 8_192;
            const MAX_RESULT_BYTES: u64 = <Self as SwitchoverActivityContract>::MAX_RESULT_BYTES;
            const MAX_ERROR_MESSAGE_BYTES: u64 =
                <Self as SwitchoverActivityContract>::MAX_ERROR_MESSAGE_BYTES;
            const COMPLETION_CLASS: CompletionClass =
                <Self as SwitchoverActivityContract>::COMPLETION_CLASS;
        }

        impl StrictSwitchoverActivityContract for $activity {
            type Family = ReplicaEffectFamily;
        }
    };
}

implement_strict_effect!(RevokeWritesActivity);
implement_strict_effect!(DemoteOldPrimaryActivity);
implement_strict_effect!(PromoteTargetActivity);
implement_strict_effect!(CompensatePromoteOldPrimaryActivity);
define_label_activity!(
    PublishOldPrimarySecondaryLabelActivity,
    PublishOldPrimarySecondaryLabelInput,
    "kuberic.switchover.publish-old-primary-secondary-label",
    4_096,
    768
);
define_label_activity!(
    RestoreOldPrimaryLabelActivity,
    RestoreOldPrimaryLabelInput,
    "kuberic.switchover.restore-old-primary-label",
    4_096,
    768
);
define_label_activity!(
    RestoreTargetSecondaryLabelActivity,
    RestoreTargetSecondaryLabelInput,
    "kuberic.switchover.restore-target-secondary-label",
    4_096,
    768
);

pub(crate) mod bounded_error {
    use super::*;

    pub fn serialize<S>(value: &str, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if value.len() > DIRECT_ACTIVITY_ERROR_MAX_BYTES {
            return Err(serde::ser::Error::custom(format!(
                "direct switchover error exceeds {DIRECT_ACTIVITY_ERROR_MAX_BYTES} UTF-8 bytes"
            )));
        }
        serializer.serialize_str(value)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        if value.len() > DIRECT_ACTIVITY_ERROR_MAX_BYTES {
            return Err(D::Error::custom(format!(
                "direct switchover error exceeds {DIRECT_ACTIVITY_ERROR_MAX_BYTES} UTF-8 bytes"
            )));
        }
        Ok(value)
    }
}

pub(crate) mod bounded_optional_error {
    use super::*;

    pub fn serialize<S>(value: &Option<String>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if value
            .as_ref()
            .is_some_and(|value| value.len() > DIRECT_ACTIVITY_ERROR_MAX_BYTES)
        {
            return Err(serde::ser::Error::custom(format!(
                "direct switchover error exceeds {DIRECT_ACTIVITY_ERROR_MAX_BYTES} UTF-8 bytes"
            )));
        }
        value.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Option::<String>::deserialize(deserializer)?;
        if value
            .as_ref()
            .is_some_and(|value| value.len() > DIRECT_ACTIVITY_ERROR_MAX_BYTES)
        {
            return Err(D::Error::custom(format!(
                "direct switchover error exceeds {DIRECT_ACTIVITY_ERROR_MAX_BYTES} UTF-8 bytes"
            )));
        }
        Ok(value)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CaptureFrozenLsnInput {
    pub old_primary_id: i64,
    pub old_primary_instance_id: String,
    pub expected_epoch: EpochStatus,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CaptureFrozenLsnOutput {
    pub frozen_lsn: i64,
    pub observed_at_unix_seconds: i64,
}

pub struct CaptureFrozenLsnActivity;

impl SwitchoverActivityContract for CaptureFrozenLsnActivity {
    type Request = CaptureFrozenLsnInput;
    type Output = CaptureFrozenLsnOutput;

    const NAME: &'static str = "kuberic.switchover.capture-frozen-lsn";
    const VERSION: u32 = DIRECT_ACTIVITY_VERSION;
    const MAX_REQUEST_BYTES: u64 = 2_048;
    const MAX_RESULT_BYTES: u64 = 1_024;
    const MAX_ERROR_MESSAGE_BYTES: u64 = DIRECT_ACTIVITY_ERROR_MAX_BYTES as u64;
    const COMPLETION_CLASS: CompletionClass = CompletionClass::PassiveObservation;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WaitTargetCaughtUpInput {
    pub target_id: i64,
    pub target_instance_id: String,
    pub expected_epoch: EpochStatus,
    pub frozen_lsn: i64,
}

pub type WaitTargetCaughtUpOutput = EffectApplied;

pub struct WaitTargetCaughtUpActivity;

impl SwitchoverActivityContract for WaitTargetCaughtUpActivity {
    type Request = WaitTargetCaughtUpInput;
    type Output = WaitTargetCaughtUpOutput;

    const NAME: &'static str = "kuberic.switchover.wait-target-caught-up";
    const VERSION: u32 = DIRECT_ACTIVITY_VERSION;
    const MAX_REQUEST_BYTES: u64 = 2_048;
    const MAX_RESULT_BYTES: u64 = 1_024;
    const MAX_ERROR_MESSAGE_BYTES: u64 = DIRECT_ACTIVITY_ERROR_MAX_BYTES as u64;
    const COMPLETION_CLASS: CompletionClass = CompletionClass::PassiveObservation;
}

macro_rules! define_attestation_activity {
    ($activity:ident, $input:ident, $output:ident, $name:literal, $max_input:literal, $max_result:literal) => {
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        pub struct $input {
            pub expected_snapshot: StablePartitionSnapshotStatus,
        }

        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        pub struct $output {
            pub observed_at_unix_seconds: i64,
            pub snapshot: StablePartitionSnapshotStatus,
        }

        pub struct $activity;

        impl SwitchoverActivityContract for $activity {
            type Request = $input;
            type Output = $output;

            const NAME: &'static str = $name;
            const VERSION: u32 = DIRECT_ACTIVITY_VERSION;
            const MAX_REQUEST_BYTES: u64 = $max_input;
            const MAX_RESULT_BYTES: u64 = $max_result;
            const MAX_ERROR_MESSAGE_BYTES: u64 = DIRECT_ACTIVITY_ERROR_MAX_BYTES as u64;
            const COMPLETION_CLASS: CompletionClass = CompletionClass::PassiveObservation;
        }
    };
}

define_attestation_activity!(
    AttestTargetTopologyActivity,
    AttestTargetTopologyInput,
    AttestTargetTopologyOutput,
    "kuberic.switchover.attest-target-topology",
    8_192,
    8_192
);

define_attestation_activity!(
    AttestCompensatedTopologyActivity,
    AttestCompensatedTopologyInput,
    AttestCompensatedTopologyOutput,
    "kuberic.switchover.attest-compensated-topology",
    8_192,
    8_192
);

pub const ALL_DIRECT_ACTIVITY_IDENTITIES: &[(&str, u32)] = &[
    (
        <RevokeWritesActivity as SwitchoverActivityContract>::NAME,
        <RevokeWritesActivity as SwitchoverActivityContract>::VERSION,
    ),
    (
        CaptureFrozenLsnActivity::NAME,
        CaptureFrozenLsnActivity::VERSION,
    ),
    (
        WaitTargetCaughtUpActivity::NAME,
        WaitTargetCaughtUpActivity::VERSION,
    ),
    (
        <DemoteOldPrimaryActivity as SwitchoverActivityContract>::NAME,
        <DemoteOldPrimaryActivity as SwitchoverActivityContract>::VERSION,
    ),
    (
        <PromoteTargetActivity as SwitchoverActivityContract>::NAME,
        <PromoteTargetActivity as SwitchoverActivityContract>::VERSION,
    ),
    (
        DistributeReplicaEpochActivity::NAME,
        DistributeReplicaEpochActivity::VERSION,
    ),
    (
        InstallTargetCatchUpConfigurationActivity::NAME,
        InstallTargetCatchUpConfigurationActivity::VERSION,
    ),
    (
        WaitTargetWriteQuorumActivity::NAME,
        WaitTargetWriteQuorumActivity::VERSION,
    ),
    (
        InstallTargetCurrentConfigurationActivity::NAME,
        InstallTargetCurrentConfigurationActivity::VERSION,
    ),
    (
        PublishTargetPrimaryLabelActivity::NAME,
        PublishTargetPrimaryLabelActivity::VERSION,
    ),
    (
        PublishOldPrimarySecondaryLabelActivity::NAME,
        PublishOldPrimarySecondaryLabelActivity::VERSION,
    ),
    (
        AttestTargetTopologyActivity::NAME,
        AttestTargetTopologyActivity::VERSION,
    ),
    (
        RestorePreviousCurrentConfigurationActivity::NAME,
        RestorePreviousCurrentConfigurationActivity::VERSION,
    ),
    (
        <CompensatePromoteOldPrimaryActivity as SwitchoverActivityContract>::NAME,
        <CompensatePromoteOldPrimaryActivity as SwitchoverActivityContract>::VERSION,
    ),
    (
        CompensateDistributeReplicaEpochActivity::NAME,
        CompensateDistributeReplicaEpochActivity::VERSION,
    ),
    (
        InstallCompensationCatchUpConfigurationActivity::NAME,
        InstallCompensationCatchUpConfigurationActivity::VERSION,
    ),
    (
        InstallCompensationCurrentConfigurationActivity::NAME,
        InstallCompensationCurrentConfigurationActivity::VERSION,
    ),
    (
        RestoreOldPrimaryLabelActivity::NAME,
        RestoreOldPrimaryLabelActivity::VERSION,
    ),
    (
        RestoreTargetSecondaryLabelActivity::NAME,
        RestoreTargetSecondaryLabelActivity::VERSION,
    ),
    (
        AttestCompensatedTopologyActivity::NAME,
        AttestCompensatedTopologyActivity::VERSION,
    ),
];

pub(crate) struct OrdinarySwitchoverActivity<E>(std::marker::PhantomData<E>);

pub(crate) struct SwitchoverActivityInput<E: SwitchoverActivityContract> {
    request: E::Request,
    action_deadline_unix_seconds: i64,
}

impl<E: SwitchoverActivityContract> SwitchoverActivityInput<E> {
    pub fn new(request: E::Request, action_deadline_unix_seconds: i64) -> Self {
        Self {
            request,
            action_deadline_unix_seconds,
        }
    }

    pub const fn action_deadline_unix_seconds(&self) -> i64 {
        self.action_deadline_unix_seconds
    }

    pub fn into_request(self) -> E::Request {
        self.request
    }
}

impl<E: SwitchoverActivityContract> Serialize for SwitchoverActivityInput<E> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.request.serialize(serializer)
    }
}

impl<'de, E: SwitchoverActivityContract> Deserialize<'de> for SwitchoverActivityInput<E> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self {
            request: E::Request::deserialize(deserializer)?,
            action_deadline_unix_seconds: 0,
        })
    }
}

impl<E: SwitchoverActivityContract> DurableActivity for OrdinarySwitchoverActivity<E> {
    type Input = SwitchoverActivityInput<E>;
    type Output = E::Output;

    const NAME: &'static str = E::NAME;
    const VERSION: u32 = E::VERSION;
    const MAX_INPUT_BYTES: u64 = E::MAX_REQUEST_BYTES;
    const MAX_RESULT_BYTES: u64 = E::MAX_RESULT_BYTES;

    fn completion_class() -> Option<CompletionClass> {
        Some(E::COMPLETION_CLASS)
    }

    fn strict_effect_metadata() -> Option<kuberic_durable_execution::EffectMetadata> {
        matches!(
            activity_class(E::NAME),
            Some(SwitchoverActivityClass::StrictEffectRequired)
        )
        .then(|| kuberic_durable_execution::EffectMetadata::new(8_192, E::COMPLETION_CLASS))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SwitchoverActivityClass {
    PassiveReadOnly,
    NaturallyIdempotent,
    IdentityFencedIdempotent,
    StrictEffectRequired,
}

pub const SWITCHOVER_ACTIVITY_CLASSIFICATION: &[(&str, SwitchoverActivityClass)] = &[
    (
        CaptureFrozenLsnActivity::NAME,
        SwitchoverActivityClass::PassiveReadOnly,
    ),
    (
        WaitTargetCaughtUpActivity::NAME,
        SwitchoverActivityClass::PassiveReadOnly,
    ),
    (
        AttestTargetTopologyActivity::NAME,
        SwitchoverActivityClass::PassiveReadOnly,
    ),
    (
        AttestCompensatedTopologyActivity::NAME,
        SwitchoverActivityClass::PassiveReadOnly,
    ),
    (
        PublishTargetPrimaryLabelActivity::NAME,
        SwitchoverActivityClass::NaturallyIdempotent,
    ),
    (
        PublishOldPrimarySecondaryLabelActivity::NAME,
        SwitchoverActivityClass::NaturallyIdempotent,
    ),
    (
        RestoreOldPrimaryLabelActivity::NAME,
        SwitchoverActivityClass::NaturallyIdempotent,
    ),
    (
        RestoreTargetSecondaryLabelActivity::NAME,
        SwitchoverActivityClass::NaturallyIdempotent,
    ),
    (
        DistributeReplicaEpochActivity::NAME,
        SwitchoverActivityClass::IdentityFencedIdempotent,
    ),
    (
        InstallTargetCatchUpConfigurationActivity::NAME,
        SwitchoverActivityClass::IdentityFencedIdempotent,
    ),
    (
        WaitTargetWriteQuorumActivity::NAME,
        SwitchoverActivityClass::IdentityFencedIdempotent,
    ),
    (
        InstallTargetCurrentConfigurationActivity::NAME,
        SwitchoverActivityClass::IdentityFencedIdempotent,
    ),
    (
        RestorePreviousCurrentConfigurationActivity::NAME,
        SwitchoverActivityClass::IdentityFencedIdempotent,
    ),
    (
        CompensateDistributeReplicaEpochActivity::NAME,
        SwitchoverActivityClass::IdentityFencedIdempotent,
    ),
    (
        InstallCompensationCatchUpConfigurationActivity::NAME,
        SwitchoverActivityClass::IdentityFencedIdempotent,
    ),
    (
        InstallCompensationCurrentConfigurationActivity::NAME,
        SwitchoverActivityClass::IdentityFencedIdempotent,
    ),
    (
        <RevokeWritesActivity as SwitchoverActivityContract>::NAME,
        SwitchoverActivityClass::StrictEffectRequired,
    ),
    (
        <DemoteOldPrimaryActivity as SwitchoverActivityContract>::NAME,
        SwitchoverActivityClass::StrictEffectRequired,
    ),
    (
        <PromoteTargetActivity as SwitchoverActivityContract>::NAME,
        SwitchoverActivityClass::StrictEffectRequired,
    ),
    (
        <CompensatePromoteOldPrimaryActivity as SwitchoverActivityContract>::NAME,
        SwitchoverActivityClass::StrictEffectRequired,
    ),
];

pub fn activity_class(name: &str) -> Option<SwitchoverActivityClass> {
    SWITCHOVER_ACTIVITY_CLASSIFICATION
        .iter()
        .find_map(|(registered, class)| (*registered == name).then_some(*class))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{StableReplicaRoleStatus, StableReplicaSnapshotStatus};
    use kuberic_durable_execution::{
        ActivityCallError, BoundedEffectError, EffectContractError, EffectErrorKind, EffectOutcome,
        decode_activity_input, decode_activity_result, encode_activity_input,
    };

    fn snapshot() -> StablePartitionSnapshotStatus {
        StablePartitionSnapshotStatus {
            epoch: EpochStatus {
                data_loss_number: 1,
                configuration_number: 2,
            },
            primary_id: 1,
            members: vec![
                StableReplicaSnapshotStatus {
                    id: 1,
                    instance_id: "instance-1".to_string(),
                    role: StableReplicaRoleStatus::Primary,
                    election_metadata: None,
                },
                StableReplicaSnapshotStatus {
                    id: 2,
                    instance_id: "instance-2".to_string(),
                    role: StableReplicaRoleStatus::ActiveSecondary,
                    election_metadata: None,
                },
            ],
            write_quorum: 2,
        }
    }

    fn revoke_input() -> RevokeWritesInput {
        RevokeWritesInput {
            old_primary_id: 1,
            old_primary_instance_id: "instance-1".to_string(),
        }
    }

    fn target_label_input() -> PublishTargetPrimaryLabelInput {
        PublishTargetPrimaryLabelInput {
            target_primary_id: 2,
            target_primary_instance_id: "instance-2".to_string(),
        }
    }

    fn assert_exact_input_bound<A, F>(make: F)
    where
        A: SwitchoverActivityContract,
        F: Fn(usize) -> A::Request,
    {
        let input = |request| SwitchoverActivityInput::<A>::new(request, 0);
        let base = encode_activity_input::<OrdinarySwitchoverActivity<A>>(&input(make(0))).unwrap();
        let maximum =
            usize::try_from(<A as SwitchoverActivityContract>::MAX_REQUEST_BYTES).unwrap();
        assert!(
            base.as_slice().len() <= maximum,
            "{}",
            <A as SwitchoverActivityContract>::NAME
        );
        let padding = maximum - base.as_slice().len();
        let exact =
            encode_activity_input::<OrdinarySwitchoverActivity<A>>(&input(make(padding))).unwrap();
        assert_eq!(
            exact.as_slice().len(),
            maximum,
            "{}",
            <A as SwitchoverActivityContract>::NAME
        );
        assert!(
            decode_activity_input::<OrdinarySwitchoverActivity<A>>(&exact).is_ok(),
            "{}",
            <A as SwitchoverActivityContract>::NAME
        );
        assert!(
            matches!(
                encode_activity_input::<OrdinarySwitchoverActivity<A>>(&input(make(padding + 1))),
                Err(ActivityCallError::InputTooLarge {
                    actual_bytes,
                    max_bytes,
                }) if actual_bytes == max_bytes + 1
                    && max_bytes == <A as SwitchoverActivityContract>::MAX_REQUEST_BYTES
            ),
            "{}",
            <A as SwitchoverActivityContract>::NAME
        );
    }

    fn assert_replica_result_error_bound<A>()
    where
        A: SwitchoverActivityContract<Output = EffectApplied>,
    {
        for message in ["x".repeat(512), "é".repeat(256)] {
            let error = BoundedEffectError::observed_at(
                EffectErrorKind::UnavailableAtDeadline,
                message,
                1,
                <A as SwitchoverActivityContract>::MAX_ERROR_MESSAGE_BYTES,
            )
            .unwrap();
            assert!(
                serde_json::to_vec(&error).unwrap().len()
                    <= <A as SwitchoverActivityContract>::MAX_RESULT_BYTES as usize
            );
        }
        for message in ["x".repeat(513), "é".repeat(257)] {
            assert!(
                matches!(
                    BoundedEffectError::observed_at(
                        EffectErrorKind::UnavailableAtDeadline,
                        message,
                        1,
                        <A as SwitchoverActivityContract>::MAX_ERROR_MESSAGE_BYTES,
                    ),
                    Err(EffectContractError::ErrorMessageTooLarge { .. })
                ),
                "{}",
                <A as SwitchoverActivityContract>::NAME
            );
        }
        assert!(matches!(
            decode_activity_result::<OrdinarySwitchoverActivity<A>>(
                &kuberic_durable_execution::ExactBytes::new(vec![
                    b'x';
                    usize::try_from(
                        <A as SwitchoverActivityContract>::MAX_RESULT_BYTES
                    )
                    .unwrap()
                        + 1
                ])
            ),
            Err(ActivityCallError::ResultTooLarge { .. })
        ));
    }

    fn assert_label_result_error_bound<A>()
    where
        A: SwitchoverActivityContract<Output = EffectApplied>,
    {
        assert_replica_result_error_bound::<A>();
    }

    macro_rules! assert_replica_activity_bounds {
        ($activity:ty, $input:expr) => {{
            let input = SwitchoverActivityInput::<$activity>::new($input, 0);
            let encoded =
                encode_activity_input::<OrdinarySwitchoverActivity<$activity>>(&input).unwrap();
            assert!(
                encoded.as_slice().len()
                    <= <$activity as SwitchoverActivityContract>::MAX_REQUEST_BYTES as usize
            );
            assert!(
                decode_activity_input::<OrdinarySwitchoverActivity<$activity>>(&encoded).is_ok()
            );
            assert_replica_result_error_bound::<$activity>();
        }};
    }

    macro_rules! assert_label_activity_bounds {
        ($activity:ty, $input:expr) => {{
            let input = SwitchoverActivityInput::<$activity>::new($input, 0);
            let encoded =
                encode_activity_input::<OrdinarySwitchoverActivity<$activity>>(&input).unwrap();
            assert!(
                encoded.as_slice().len()
                    <= <$activity as SwitchoverActivityContract>::MAX_REQUEST_BYTES as usize
            );
            assert!(
                decode_activity_input::<OrdinarySwitchoverActivity<$activity>>(&encoded).is_ok()
            );
            assert_label_result_error_bound::<$activity>();
        }};
    }

    fn assert_bounded_message_result<A: SwitchoverActivityContract>() {
        for message in ["x".repeat(512), "é".repeat(256)] {
            let error = BoundedEffectError::observed_at(
                EffectErrorKind::ConflictingEvidence,
                message,
                1,
                <A as SwitchoverActivityContract>::MAX_ERROR_MESSAGE_BYTES,
            )
            .unwrap();
            assert!(
                serde_json::to_vec(&error).unwrap().len()
                    <= <A as SwitchoverActivityContract>::MAX_RESULT_BYTES as usize
            );
        }
        for message in ["x".repeat(513), "é".repeat(257)] {
            assert!(matches!(
                BoundedEffectError::observed_at(
                    EffectErrorKind::ConflictingEvidence,
                    message,
                    1,
                    <A as SwitchoverActivityContract>::MAX_ERROR_MESSAGE_BYTES,
                ),
                Err(EffectContractError::ErrorMessageTooLarge { .. })
            ));
        }
        assert!(matches!(
            decode_activity_result::<OrdinarySwitchoverActivity<A>>(
                &kuberic_durable_execution::ExactBytes::new(vec![
                    b'x';
                    usize::try_from(
                        <A as SwitchoverActivityContract>::MAX_RESULT_BYTES
                    )
                    .unwrap()
                        + 1
                ])
            ),
            Err(ActivityCallError::ResultTooLarge { .. })
        ));
    }

    #[test]
    fn direct_switchover_activity_identities_are_unique_positive_and_operation_specific() {
        let identities = ALL_DIRECT_ACTIVITY_IDENTITIES
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(identities.len(), ALL_DIRECT_ACTIVITY_IDENTITIES.len());
        assert_eq!(identities.len(), 20);
        for (name, version) in identities {
            assert!(name.starts_with("kuberic.switchover."));
            assert_ne!(name, "kuberic.switchover.native-boundary");
            assert_eq!(version, 1);
        }
    }

    #[test]
    fn every_direct_switchover_activity_declares_independent_nonzero_bounds() {
        macro_rules! assert_bounds {
            ($($activity:ty),+ $(,)?) => {
                $(
                    assert!(<$activity as SwitchoverActivityContract>::MAX_REQUEST_BYTES > 0);
                    assert!(<$activity as SwitchoverActivityContract>::MAX_RESULT_BYTES > 0);
                )+
            };
        }
        assert_bounds!(
            RevokeWritesActivity,
            CaptureFrozenLsnActivity,
            WaitTargetCaughtUpActivity,
            DemoteOldPrimaryActivity,
            PromoteTargetActivity,
            DistributeReplicaEpochActivity,
            InstallTargetCatchUpConfigurationActivity,
            WaitTargetWriteQuorumActivity,
            InstallTargetCurrentConfigurationActivity,
            PublishTargetPrimaryLabelActivity,
            PublishOldPrimarySecondaryLabelActivity,
            AttestTargetTopologyActivity,
            RestorePreviousCurrentConfigurationActivity,
            CompensatePromoteOldPrimaryActivity,
            CompensateDistributeReplicaEpochActivity,
            InstallCompensationCatchUpConfigurationActivity,
            InstallCompensationCurrentConfigurationActivity,
            RestoreOldPrimaryLabelActivity,
            RestoreTargetSecondaryLabelActivity,
            AttestCompensatedTopologyActivity,
        );
        assert_ne!(
            <RevokeWritesActivity as SwitchoverActivityContract>::MAX_REQUEST_BYTES,
            <InstallTargetCurrentConfigurationActivity as SwitchoverActivityContract>::MAX_REQUEST_BYTES
        );
        assert_ne!(
            <PublishTargetPrimaryLabelActivity as SwitchoverActivityContract>::MAX_RESULT_BYTES,
            <RevokeWritesActivity as SwitchoverActivityContract>::MAX_RESULT_BYTES
        );
    }

    #[test]
    fn direct_switchover_contracts_reject_unknown_fields() {
        let mut value = serde_json::to_value(revoke_input()).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("desiredSnapshot".to_string(), serde_json::json!(snapshot()));
        assert!(serde_json::from_value::<RevokeWritesInput>(value).is_err());

        let mut label = serde_json::to_value(target_label_input()).unwrap();
        label
            .as_object_mut()
            .unwrap()
            .insert("redelivery".to_string(), serde_json::json!(0));
        assert!(serde_json::from_value::<PublishTargetPrimaryLabelInput>(label).is_err());

        let malformed = serde_json::json!({
            "result": "proven_no_admission",
            "observed_at_unix_seconds": 1,
        });
        assert!(serde_json::from_value::<PublishTargetPrimaryLabelOutput>(malformed).is_err());
    }

    #[test]
    fn direct_switchover_effect_outputs_preserve_domain_outcomes() {
        let error = |kind, message, observed_at| {
            BoundedEffectError::observed_at(kind, message, observed_at, 512).unwrap()
        };
        let outcomes = [
            EffectOutcome::Applied(EffectApplied {
                observed_at_unix_seconds: 1,
            }),
            EffectOutcome::ProvenNoAdmission,
            EffectOutcome::DomainFailure(error(EffectErrorKind::DomainFailure, "failed", 3)),
            EffectOutcome::DeadlineExceeded(error(
                EffectErrorKind::DeadlineExceeded,
                "deadline exceeded",
                4,
            )),
            EffectOutcome::UnavailableAtDeadline(error(
                EffectErrorKind::UnavailableAtDeadline,
                "unavailable at deadline",
                5,
            )),
            EffectOutcome::ConflictingEvidence(error(
                EffectErrorKind::ConflictingEvidence,
                "conflicting",
                6,
            )),
        ];
        for outcome in outcomes {
            let encoded = serde_json::to_vec(&outcome).unwrap();
            let decoded =
                serde_json::from_slice::<EffectOutcome<RevokeWritesOutput>>(&encoded).unwrap();
            assert_eq!(decoded, outcome);
        }
    }

    #[test]
    fn direct_switchover_activity_error_reload_rejects_one_over_utf8_bound() {
        for message in ["x".repeat(513), "é".repeat(257)] {
            assert!(
                serde_json::from_value::<EffectOutcome<RevokeWritesOutput>>(serde_json::json!({
                    "status": "domain_failure",
                    "value": {
                        "kind": "domain_failure",
                        "message": message,
                        "observed_at_unix_seconds": 1
                    }
                }))
                .is_ok()
            );
        }
    }

    #[test]
    fn direct_switchover_every_activity_accepts_exact_bounds_and_rejects_one_over() {
        assert_replica_activity_bounds!(RevokeWritesActivity, revoke_input());
        assert_replica_activity_bounds!(
            DemoteOldPrimaryActivity,
            DemoteOldPrimaryInput {
                old_primary_id: 1,
                old_primary_instance_id: "instance-1".to_string(),
            }
        );
        assert_replica_activity_bounds!(
            PromoteTargetActivity,
            PromoteTargetInput {
                target_primary_id: 2,
                target_primary_instance_id: "instance-2".to_string(),
            }
        );
        assert_replica_activity_bounds!(
            DistributeReplicaEpochActivity,
            DistributeReplicaEpochInput {
                distribution_index: 0,
                replica_id: 2,
                replica_instance_id: "instance-2".to_string(),
            }
        );
        assert_replica_activity_bounds!(
            InstallTargetCatchUpConfigurationActivity,
            InstallTargetCatchUpConfigurationInput {
                target_primary_id: 2,
                target_primary_instance_id: "instance-2".to_string(),
            }
        );
        assert_replica_activity_bounds!(
            WaitTargetWriteQuorumActivity,
            WaitTargetWriteQuorumInput {
                target_primary_id: 2,
                target_primary_instance_id: "instance-2".to_string(),
            }
        );
        assert_replica_activity_bounds!(
            InstallTargetCurrentConfigurationActivity,
            InstallTargetCurrentConfigurationInput {
                target_primary_id: 2,
                target_primary_instance_id: "instance-2".to_string(),
            }
        );
        assert_replica_activity_bounds!(
            RestorePreviousCurrentConfigurationActivity,
            RestorePreviousCurrentConfigurationInput {
                old_primary_id: 1,
                old_primary_instance_id: "instance-1".to_string(),
            }
        );
        assert_replica_activity_bounds!(
            CompensatePromoteOldPrimaryActivity,
            CompensatePromoteOldPrimaryInput {
                old_primary_id: 1,
                old_primary_instance_id: "instance-1".to_string(),
            }
        );
        assert_replica_activity_bounds!(
            CompensateDistributeReplicaEpochActivity,
            CompensateDistributeReplicaEpochInput {
                distribution_index: 0,
                replica_id: 2,
                replica_instance_id: "instance-2".to_string(),
            }
        );
        assert_replica_activity_bounds!(
            InstallCompensationCatchUpConfigurationActivity,
            InstallCompensationCatchUpConfigurationInput {
                old_primary_id: 1,
                old_primary_instance_id: "instance-1".to_string(),
            }
        );
        assert_replica_activity_bounds!(
            InstallCompensationCurrentConfigurationActivity,
            InstallCompensationCurrentConfigurationInput {
                old_primary_id: 1,
                old_primary_instance_id: "instance-1".to_string(),
            }
        );

        assert_label_activity_bounds!(PublishTargetPrimaryLabelActivity, target_label_input());
        assert_label_activity_bounds!(
            PublishOldPrimarySecondaryLabelActivity,
            PublishOldPrimarySecondaryLabelInput {
                old_primary_id: 1,
                old_primary_instance_id: "instance-1".to_string(),
            }
        );
        assert_label_activity_bounds!(
            RestoreOldPrimaryLabelActivity,
            RestoreOldPrimaryLabelInput {
                old_primary_id: 1,
                old_primary_instance_id: "instance-1".to_string(),
            }
        );
        assert_label_activity_bounds!(
            RestoreTargetSecondaryLabelActivity,
            RestoreTargetSecondaryLabelInput {
                target_primary_id: 2,
                target_primary_instance_id: "instance-2".to_string(),
            }
        );

        assert_exact_input_bound::<CaptureFrozenLsnActivity, _>(|padding| CaptureFrozenLsnInput {
            old_primary_id: 1,
            old_primary_instance_id: "x".repeat(padding),
            expected_epoch: snapshot().epoch,
        });
        assert_bounded_message_result::<CaptureFrozenLsnActivity>();

        assert_exact_input_bound::<WaitTargetCaughtUpActivity, _>(|padding| {
            WaitTargetCaughtUpInput {
                target_id: 2,
                target_instance_id: "x".repeat(padding),
                expected_epoch: snapshot().epoch,
                frozen_lsn: 10,
            }
        });
        assert_bounded_message_result::<WaitTargetCaughtUpActivity>();

        assert_exact_input_bound::<AttestTargetTopologyActivity, _>(|padding| {
            let mut expected_snapshot = snapshot();
            expected_snapshot.members[0].instance_id = "x".repeat(padding);
            AttestTargetTopologyInput { expected_snapshot }
        });
        assert_bounded_message_result::<AttestTargetTopologyActivity>();

        assert_exact_input_bound::<AttestCompensatedTopologyActivity, _>(|padding| {
            let mut expected_snapshot = snapshot();
            expected_snapshot.members[0].instance_id = "x".repeat(padding);
            AttestCompensatedTopologyInput { expected_snapshot }
        });
        assert_bounded_message_result::<AttestCompensatedTopologyActivity>();
    }

    #[test]
    fn every_switchover_activity_has_one_reviewed_implementation_class() {
        let identities = ALL_DIRECT_ACTIVITY_IDENTITIES
            .iter()
            .map(|(name, _)| *name)
            .collect::<std::collections::BTreeSet<_>>();
        let classified = SWITCHOVER_ACTIVITY_CLASSIFICATION
            .iter()
            .map(|(name, _)| *name)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(identities, classified);
        assert_eq!(classified.len(), 20);
        for class in [
            SwitchoverActivityClass::PassiveReadOnly,
            SwitchoverActivityClass::NaturallyIdempotent,
            SwitchoverActivityClass::IdentityFencedIdempotent,
            SwitchoverActivityClass::StrictEffectRequired,
        ] {
            let expected = if class == SwitchoverActivityClass::IdentityFencedIdempotent {
                8
            } else {
                4
            };
            assert_eq!(
                SWITCHOVER_ACTIVITY_CLASSIFICATION
                    .iter()
                    .filter(|(_, actual)| *actual == class)
                    .count(),
                expected
            );
        }
        assert!(identities.iter().all(|name| activity_class(name).is_some()));
    }

    #[test]
    fn ordinary_contract_preserves_the_typed_name_and_bounds() {
        type Contract = OrdinarySwitchoverActivity<PromoteTargetActivity>;
        assert_eq!(
            Contract::NAME,
            <PromoteTargetActivity as SwitchoverActivityContract>::NAME
        );
        assert_eq!(
            Contract::MAX_INPUT_BYTES,
            <PromoteTargetActivity as SwitchoverActivityContract>::MAX_REQUEST_BYTES
        );
        assert_eq!(
            Contract::MAX_RESULT_BYTES,
            <PromoteTargetActivity as SwitchoverActivityContract>::MAX_RESULT_BYTES
        );
    }
}
