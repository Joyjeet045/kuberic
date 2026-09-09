use kuberic_durable_execution::DurableActivity;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

use crate::crd::{EpochStatus, StablePartitionSnapshotStatus};
use crate::durable::effects::{LabelEffectCommand, ReplicaEffectCommand};

pub const DIRECT_SWITCHOVER_CONTRACT_VERSION: u32 = 4;
pub const DIRECT_ACTIVITY_VERSION: u32 = 1;
pub const DIRECT_ACTIVITY_ERROR_MAX_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DirectActivityAccounting {
    pub external_effect_count: u64,
    pub passive_observation_count: u64,
}

impl DirectActivityAccounting {
    pub const fn new(external_effect_count: u64, passive_observation_count: u64) -> Self {
        Self {
            external_effect_count,
            passive_observation_count,
        }
    }

    pub fn total(self) -> Option<u64> {
        self.external_effect_count
            .checked_add(self.passive_observation_count)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EffectObservation {
    Applied {
        observed_at_unix_seconds: i64,
    },
    ProvenNoAdmission {
        observed_at_unix_seconds: i64,
    },
    Failed {
        observed_at_unix_seconds: i64,
        message: String,
    },
    DeadlineExceeded {
        observed_at_unix_seconds: i64,
        message: String,
    },
    UnavailableAtDeadline {
        observed_at_unix_seconds: i64,
        message: String,
    },
    Conflicting {
        observed_at_unix_seconds: i64,
        message: String,
    },
}

impl EffectObservation {
    pub const fn observed_at_unix_seconds(&self) -> i64 {
        match self {
            Self::Applied {
                observed_at_unix_seconds,
            }
            | Self::ProvenNoAdmission {
                observed_at_unix_seconds,
            }
            | Self::Failed {
                observed_at_unix_seconds,
                ..
            }
            | Self::DeadlineExceeded {
                observed_at_unix_seconds,
                ..
            }
            | Self::UnavailableAtDeadline {
                observed_at_unix_seconds,
                ..
            }
            | Self::Conflicting {
                observed_at_unix_seconds,
                ..
            } => *observed_at_unix_seconds,
        }
    }

    pub fn bounded(self) -> Self {
        match self {
            Self::Failed {
                observed_at_unix_seconds,
                message,
            } => Self::Failed {
                observed_at_unix_seconds,
                message: bounded_error(message),
            },
            Self::DeadlineExceeded {
                observed_at_unix_seconds,
                message,
            } => Self::DeadlineExceeded {
                observed_at_unix_seconds,
                message: bounded_error(message),
            },
            Self::UnavailableAtDeadline {
                observed_at_unix_seconds,
                message,
            } => Self::UnavailableAtDeadline {
                observed_at_unix_seconds,
                message: bounded_error(message),
            },
            Self::Conflicting {
                observed_at_unix_seconds,
                message,
            } => Self::Conflicting {
                observed_at_unix_seconds,
                message: bounded_error(message),
            },
            observation @ (Self::Applied { .. } | Self::ProvenNoAdmission { .. }) => observation,
        }
    }
}

pub trait ReplicaDirectActivity: DurableActivity {
    fn set_redelivery(input: &mut Self::Input, redelivery: u8);
    fn prepared_command(input: &Self::Input) -> Option<&ReplicaEffectCommand>;
    fn prepared_command_mut(input: &mut Self::Input) -> &mut Option<ReplicaEffectCommand>;
    fn observation(output: Self::Output) -> EffectObservation;
    fn output(observation: EffectObservation) -> Result<Self::Output, String>;
}

pub trait LabelDirectActivity: DurableActivity {
    fn prepared_command(input: &Self::Input) -> Option<&LabelEffectCommand>;
    fn prepared_command_mut(input: &mut Self::Input) -> &mut Option<LabelEffectCommand>;
    fn observation(output: Self::Output) -> EffectObservation;
    fn output(observation: EffectObservation) -> Result<Self::Output, String>;
}

macro_rules! define_replica_activity {
    ($activity:ident, $input:ty, $output:ident, $name:literal, $max_input:literal, $max_result:literal) => {
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        #[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
        pub enum $output {
            Applied {
                observed_at_unix_seconds: i64,
            },
            ProvenNoAdmission {
                observed_at_unix_seconds: i64,
            },
            Failed {
                observed_at_unix_seconds: i64,
                #[serde(with = "bounded_error")]
                message: String,
            },
            DeadlineExceeded {
                observed_at_unix_seconds: i64,
                #[serde(with = "bounded_error")]
                message: String,
            },
            UnavailableAtDeadline {
                observed_at_unix_seconds: i64,
                #[serde(with = "bounded_error")]
                message: String,
            },
            Conflicting {
                observed_at_unix_seconds: i64,
                #[serde(with = "bounded_error")]
                message: String,
            },
        }

        pub struct $activity;

        impl DurableActivity for $activity {
            type Input = $input;
            type Output = $output;

            const NAME: &'static str = $name;
            const VERSION: u32 = DIRECT_ACTIVITY_VERSION;
            const MAX_INPUT_BYTES: u64 = $max_input;
            const MAX_RESULT_BYTES: u64 = $max_result;
        }

        impl ReplicaDirectActivity for $activity {
            fn set_redelivery(input: &mut Self::Input, redelivery: u8) {
                input.redelivery = redelivery;
            }

            fn prepared_command(input: &Self::Input) -> Option<&ReplicaEffectCommand> {
                input.prepared_command.as_ref()
            }

            fn prepared_command_mut(input: &mut Self::Input) -> &mut Option<ReplicaEffectCommand> {
                &mut input.prepared_command
            }

            fn observation(output: Self::Output) -> EffectObservation {
                match output {
                    $output::Applied {
                        observed_at_unix_seconds,
                    } => EffectObservation::Applied {
                        observed_at_unix_seconds,
                    },
                    $output::ProvenNoAdmission {
                        observed_at_unix_seconds,
                    } => EffectObservation::ProvenNoAdmission {
                        observed_at_unix_seconds,
                    },
                    $output::Failed {
                        observed_at_unix_seconds,
                        message,
                    } => EffectObservation::Failed {
                        observed_at_unix_seconds,
                        message,
                    },
                    $output::DeadlineExceeded {
                        observed_at_unix_seconds,
                        message,
                    } => EffectObservation::DeadlineExceeded {
                        observed_at_unix_seconds,
                        message,
                    },
                    $output::UnavailableAtDeadline {
                        observed_at_unix_seconds,
                        message,
                    } => EffectObservation::UnavailableAtDeadline {
                        observed_at_unix_seconds,
                        message,
                    },
                    $output::Conflicting {
                        observed_at_unix_seconds,
                        message,
                    } => EffectObservation::Conflicting {
                        observed_at_unix_seconds,
                        message,
                    },
                }
            }

            fn output(observation: EffectObservation) -> Result<Self::Output, String> {
                Ok(match observation {
                    EffectObservation::Applied {
                        observed_at_unix_seconds,
                    } => $output::Applied {
                        observed_at_unix_seconds,
                    },
                    EffectObservation::ProvenNoAdmission {
                        observed_at_unix_seconds,
                    } => $output::ProvenNoAdmission {
                        observed_at_unix_seconds,
                    },
                    EffectObservation::Failed {
                        observed_at_unix_seconds,
                        message,
                    } => $output::Failed {
                        observed_at_unix_seconds,
                        message,
                    },
                    EffectObservation::DeadlineExceeded {
                        observed_at_unix_seconds,
                        message,
                    } => $output::DeadlineExceeded {
                        observed_at_unix_seconds,
                        message,
                    },
                    EffectObservation::UnavailableAtDeadline {
                        observed_at_unix_seconds,
                        message,
                    } => $output::UnavailableAtDeadline {
                        observed_at_unix_seconds,
                        message,
                    },
                    EffectObservation::Conflicting {
                        observed_at_unix_seconds,
                        message,
                    } => $output::Conflicting {
                        observed_at_unix_seconds,
                        message,
                    },
                })
            }
        }
    };
}

macro_rules! define_label_activity {
    ($activity:ident, $input:ty, $output:ident, $name:literal, $max_input:literal, $max_result:literal) => {
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        #[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
        pub enum $output {
            Applied {
                observed_at_unix_seconds: i64,
            },
            DeadlineExceeded {
                observed_at_unix_seconds: i64,
                #[serde(with = "bounded_error")]
                message: String,
            },
            UnavailableAtDeadline {
                observed_at_unix_seconds: i64,
                #[serde(with = "bounded_error")]
                message: String,
            },
            Conflicting {
                observed_at_unix_seconds: i64,
                #[serde(with = "bounded_error")]
                message: String,
            },
        }

        pub struct $activity;

        impl DurableActivity for $activity {
            type Input = $input;
            type Output = $output;

            const NAME: &'static str = $name;
            const VERSION: u32 = DIRECT_ACTIVITY_VERSION;
            const MAX_INPUT_BYTES: u64 = $max_input;
            const MAX_RESULT_BYTES: u64 = $max_result;
        }

        impl LabelDirectActivity for $activity {
            fn prepared_command(input: &Self::Input) -> Option<&LabelEffectCommand> {
                input.prepared_command.as_ref()
            }

            fn prepared_command_mut(input: &mut Self::Input) -> &mut Option<LabelEffectCommand> {
                &mut input.prepared_command
            }

            fn observation(output: Self::Output) -> EffectObservation {
                match output {
                    $output::Applied {
                        observed_at_unix_seconds,
                    } => EffectObservation::Applied {
                        observed_at_unix_seconds,
                    },
                    $output::DeadlineExceeded {
                        observed_at_unix_seconds,
                        message,
                    } => EffectObservation::DeadlineExceeded {
                        observed_at_unix_seconds,
                        message,
                    },
                    $output::UnavailableAtDeadline {
                        observed_at_unix_seconds,
                        message,
                    } => EffectObservation::UnavailableAtDeadline {
                        observed_at_unix_seconds,
                        message,
                    },
                    $output::Conflicting {
                        observed_at_unix_seconds,
                        message,
                    } => EffectObservation::Conflicting {
                        observed_at_unix_seconds,
                        message,
                    },
                }
            }

            fn output(observation: EffectObservation) -> Result<Self::Output, String> {
                Ok(match observation {
                    EffectObservation::Applied {
                        observed_at_unix_seconds,
                    } => $output::Applied {
                        observed_at_unix_seconds,
                    },
                    EffectObservation::DeadlineExceeded {
                        observed_at_unix_seconds,
                        message,
                    } => $output::DeadlineExceeded {
                        observed_at_unix_seconds,
                        message,
                    },
                    EffectObservation::UnavailableAtDeadline {
                        observed_at_unix_seconds,
                        message,
                    } => $output::UnavailableAtDeadline {
                        observed_at_unix_seconds,
                        message,
                    },
                    EffectObservation::Conflicting {
                        observed_at_unix_seconds,
                        message,
                    } => $output::Conflicting {
                        observed_at_unix_seconds,
                        message,
                    },
                    EffectObservation::ProvenNoAdmission { .. }
                    | EffectObservation::Failed { .. } => {
                        return Err(
                            "label activities cannot report replica-only outcomes".to_string()
                        );
                    }
                })
            }
        }
    };
}

macro_rules! fixed_replica_input {
    ($input:ident, $target_id:ident, $target_instance_id:ident) => {
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        pub struct $input {
            pub contract_version: u32,
            pub execution_id: String,
            pub $target_id: i64,
            pub $target_instance_id: String,
            pub deadline_unix_seconds: i64,
            pub redelivery: u8,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub prepared_command: Option<ReplicaEffectCommand>,
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
    pub contract_version: u32,
    pub execution_id: String,
    pub distribution_index: u8,
    pub replica_id: i64,
    pub replica_instance_id: String,
    pub deadline_unix_seconds: i64,
    pub redelivery: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prepared_command: Option<ReplicaEffectCommand>,
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
    pub contract_version: u32,
    pub execution_id: String,
    pub distribution_index: u8,
    pub replica_id: i64,
    pub replica_instance_id: String,
    pub deadline_unix_seconds: i64,
    pub redelivery: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prepared_command: Option<ReplicaEffectCommand>,
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
            pub contract_version: u32,
            pub execution_id: String,
            pub $target_id: i64,
            pub $target_instance_id: String,
            pub deadline_unix_seconds: i64,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub prepared_command: Option<LabelEffectCommand>,
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
    RevokeWritesOutput,
    "kuberic.switchover.revoke-writes",
    6_144,
    1_024
);
define_replica_activity!(
    DemoteOldPrimaryActivity,
    DemoteOldPrimaryInput,
    DemoteOldPrimaryOutput,
    "kuberic.switchover.demote-old-primary",
    6_144,
    1_024
);
define_replica_activity!(
    PromoteTargetActivity,
    PromoteTargetInput,
    PromoteTargetOutput,
    "kuberic.switchover.promote-target",
    6_144,
    1_024
);
define_replica_activity!(
    DistributeReplicaEpochActivity,
    DistributeReplicaEpochInput,
    DistributeReplicaEpochOutput,
    "kuberic.switchover.distribute-replica-epoch",
    6_144,
    1_024
);
define_replica_activity!(
    InstallTargetCatchUpConfigurationActivity,
    InstallTargetCatchUpConfigurationInput,
    InstallTargetCatchUpConfigurationOutput,
    "kuberic.switchover.install-target-catch-up-configuration",
    8_192,
    1_024
);
define_replica_activity!(
    WaitTargetWriteQuorumActivity,
    WaitTargetWriteQuorumInput,
    WaitTargetWriteQuorumOutput,
    "kuberic.switchover.wait-target-write-quorum",
    6_144,
    1_024
);
define_replica_activity!(
    InstallTargetCurrentConfigurationActivity,
    InstallTargetCurrentConfigurationInput,
    InstallTargetCurrentConfigurationOutput,
    "kuberic.switchover.install-target-current-configuration",
    8_192,
    1_024
);
define_replica_activity!(
    RestorePreviousCurrentConfigurationActivity,
    RestorePreviousCurrentConfigurationInput,
    RestorePreviousCurrentConfigurationOutput,
    "kuberic.switchover.restore-previous-current-configuration",
    8_192,
    1_024
);
define_replica_activity!(
    CompensatePromoteOldPrimaryActivity,
    CompensatePromoteOldPrimaryInput,
    CompensatePromoteOldPrimaryOutput,
    "kuberic.switchover.compensate-promote-old-primary",
    6_144,
    1_024
);
define_replica_activity!(
    CompensateDistributeReplicaEpochActivity,
    CompensateDistributeReplicaEpochInput,
    CompensateDistributeReplicaEpochOutput,
    "kuberic.switchover.compensate-distribute-replica-epoch",
    6_144,
    1_024
);
define_replica_activity!(
    InstallCompensationCatchUpConfigurationActivity,
    InstallCompensationCatchUpConfigurationInput,
    InstallCompensationCatchUpConfigurationOutput,
    "kuberic.switchover.install-compensation-catch-up-configuration",
    8_192,
    1_024
);
define_replica_activity!(
    InstallCompensationCurrentConfigurationActivity,
    InstallCompensationCurrentConfigurationInput,
    InstallCompensationCurrentConfigurationOutput,
    "kuberic.switchover.install-compensation-current-configuration",
    8_192,
    1_024
);

define_label_activity!(
    PublishTargetPrimaryLabelActivity,
    PublishTargetPrimaryLabelInput,
    PublishTargetPrimaryLabelOutput,
    "kuberic.switchover.publish-target-primary-label",
    4_096,
    768
);
define_label_activity!(
    PublishOldPrimarySecondaryLabelActivity,
    PublishOldPrimarySecondaryLabelInput,
    PublishOldPrimarySecondaryLabelOutput,
    "kuberic.switchover.publish-old-primary-secondary-label",
    4_096,
    768
);
define_label_activity!(
    RestoreOldPrimaryLabelActivity,
    RestoreOldPrimaryLabelInput,
    RestoreOldPrimaryLabelOutput,
    "kuberic.switchover.restore-old-primary-label",
    4_096,
    768
);
define_label_activity!(
    RestoreTargetSecondaryLabelActivity,
    RestoreTargetSecondaryLabelInput,
    RestoreTargetSecondaryLabelOutput,
    "kuberic.switchover.restore-target-secondary-label",
    4_096,
    768
);

fn bounded_error(value: String) -> String {
    super::bounded_utf8(&value, DIRECT_ACTIVITY_ERROR_MAX_BYTES)
}

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
    pub contract_version: u32,
    pub execution_id: String,
    pub old_primary_id: i64,
    pub old_primary_instance_id: String,
    pub expected_epoch: EpochStatus,
    pub deadline_unix_seconds: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum CaptureFrozenLsnOutput {
    Captured {
        frozen_lsn: i64,
        observed_at_unix_seconds: i64,
    },
    DeadlineExceeded {
        observed_at_unix_seconds: i64,
        #[serde(with = "bounded_error")]
        message: String,
    },
    Conflicting {
        observed_at_unix_seconds: i64,
        #[serde(with = "bounded_error")]
        message: String,
    },
}

pub struct CaptureFrozenLsnActivity;

impl DurableActivity for CaptureFrozenLsnActivity {
    type Input = CaptureFrozenLsnInput;
    type Output = CaptureFrozenLsnOutput;

    const NAME: &'static str = "kuberic.switchover.capture-frozen-lsn";
    const VERSION: u32 = DIRECT_ACTIVITY_VERSION;
    const MAX_INPUT_BYTES: u64 = 2_048;
    const MAX_RESULT_BYTES: u64 = 1_024;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WaitTargetCaughtUpInput {
    pub contract_version: u32,
    pub execution_id: String,
    pub target_id: i64,
    pub target_instance_id: String,
    pub expected_epoch: EpochStatus,
    pub frozen_lsn: i64,
    pub deadline_unix_seconds: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum WaitTargetCaughtUpOutput {
    CaughtUp {
        observed_at_unix_seconds: i64,
    },
    DeadlineExceeded {
        observed_at_unix_seconds: i64,
        #[serde(with = "bounded_error")]
        message: String,
    },
    Conflicting {
        observed_at_unix_seconds: i64,
        #[serde(with = "bounded_error")]
        message: String,
    },
}

pub struct WaitTargetCaughtUpActivity;

impl DurableActivity for WaitTargetCaughtUpActivity {
    type Input = WaitTargetCaughtUpInput;
    type Output = WaitTargetCaughtUpOutput;

    const NAME: &'static str = "kuberic.switchover.wait-target-caught-up";
    const VERSION: u32 = DIRECT_ACTIVITY_VERSION;
    const MAX_INPUT_BYTES: u64 = 2_048;
    const MAX_RESULT_BYTES: u64 = 1_024;
}

macro_rules! define_attestation_activity {
    ($activity:ident, $input:ident, $output:ident, $name:literal, $max_input:literal, $max_result:literal) => {
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        pub struct $input {
            pub contract_version: u32,
            pub execution_id: String,
            pub expected_snapshot: StablePartitionSnapshotStatus,
            pub deadline_unix_seconds: i64,
        }

        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        #[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
        pub enum $output {
            Attested {
                observed_at_unix_seconds: i64,
                snapshot: StablePartitionSnapshotStatus,
                #[serde(default, skip_serializing_if = "Option::is_none")]
                accounting: Option<DirectActivityAccounting>,
            },
            DeadlineExceeded {
                observed_at_unix_seconds: i64,
                #[serde(with = "bounded_error")]
                message: String,
            },
            Conflicting {
                observed_at_unix_seconds: i64,
                #[serde(with = "bounded_error")]
                message: String,
            },
        }

        pub struct $activity;

        impl DurableActivity for $activity {
            type Input = $input;
            type Output = $output;

            const NAME: &'static str = $name;
            const VERSION: u32 = DIRECT_ACTIVITY_VERSION;
            const MAX_INPUT_BYTES: u64 = $max_input;
            const MAX_RESULT_BYTES: u64 = $max_result;
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
    (RevokeWritesActivity::NAME, RevokeWritesActivity::VERSION),
    (
        CaptureFrozenLsnActivity::NAME,
        CaptureFrozenLsnActivity::VERSION,
    ),
    (
        WaitTargetCaughtUpActivity::NAME,
        WaitTargetCaughtUpActivity::VERSION,
    ),
    (
        DemoteOldPrimaryActivity::NAME,
        DemoteOldPrimaryActivity::VERSION,
    ),
    (PromoteTargetActivity::NAME, PromoteTargetActivity::VERSION),
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
        CompensatePromoteOldPrimaryActivity::NAME,
        CompensatePromoteOldPrimaryActivity::VERSION,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{StableReplicaRoleStatus, StableReplicaSnapshotStatus};
    use kuberic_durable_execution::{
        ActivityCallError, decode_activity_input, decode_activity_result, encode_activity_input,
        encode_activity_result,
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
            contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
            execution_id: "execution".to_string(),
            old_primary_id: 1,
            old_primary_instance_id: "instance-1".to_string(),
            deadline_unix_seconds: 10,
            redelivery: 0,
            prepared_command: None,
        }
    }

    fn target_label_input() -> PublishTargetPrimaryLabelInput {
        PublishTargetPrimaryLabelInput {
            contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
            execution_id: "execution".to_string(),
            target_primary_id: 2,
            target_primary_instance_id: "instance-2".to_string(),
            deadline_unix_seconds: 10,
            prepared_command: None,
        }
    }

    fn assert_exact_input_bound<A, F>(make: F)
    where
        A: DurableActivity,
        F: Fn(usize) -> A::Input,
    {
        let base = encode_activity_input::<A>(&make(0)).unwrap();
        let maximum = usize::try_from(A::MAX_INPUT_BYTES).unwrap();
        assert!(base.as_slice().len() <= maximum, "{}", A::NAME);
        let padding = maximum - base.as_slice().len();
        let exact = encode_activity_input::<A>(&make(padding)).unwrap();
        assert_eq!(exact.as_slice().len(), maximum, "{}", A::NAME);
        assert!(decode_activity_input::<A>(&exact).is_ok(), "{}", A::NAME);
        assert!(
            matches!(
                encode_activity_input::<A>(&make(padding + 1)),
                Err(ActivityCallError::InputTooLarge {
                    actual_bytes,
                    max_bytes,
                }) if actual_bytes == max_bytes + 1 && max_bytes == A::MAX_INPUT_BYTES
            ),
            "{}",
            A::NAME
        );
    }

    fn assert_replica_result_error_bound<A: ReplicaDirectActivity>() {
        for message in ["x".repeat(512), "é".repeat(256)] {
            let encoded = encode_activity_result::<A>(
                &A::output(EffectObservation::UnavailableAtDeadline {
                    observed_at_unix_seconds: 1,
                    message,
                })
                .unwrap(),
            )
            .unwrap();
            assert!(decode_activity_result::<A>(&encoded).is_ok(), "{}", A::NAME);
        }
        for message in ["x".repeat(513), "é".repeat(257)] {
            assert!(
                matches!(
                    encode_activity_result::<A>(
                        &A::output(EffectObservation::UnavailableAtDeadline {
                            observed_at_unix_seconds: 1,
                            message,
                        })
                        .unwrap(),
                    ),
                    Err(ActivityCallError::ResultEncoding)
                ),
                "{}",
                A::NAME
            );
        }
        assert!(matches!(
            decode_activity_result::<A>(&kuberic_durable_execution::ExactBytes::new(vec![
                b'x';
                usize::try_from(A::MAX_RESULT_BYTES).unwrap()
                    + 1
            ])),
            Err(ActivityCallError::ResultTooLarge { .. })
        ));
    }

    fn assert_label_result_error_bound<A: LabelDirectActivity>() {
        for message in ["x".repeat(512), "é".repeat(256)] {
            let encoded = encode_activity_result::<A>(
                &A::output(EffectObservation::UnavailableAtDeadline {
                    observed_at_unix_seconds: 1,
                    message,
                })
                .unwrap(),
            )
            .unwrap();
            assert!(decode_activity_result::<A>(&encoded).is_ok(), "{}", A::NAME);
        }
        for message in ["x".repeat(513), "é".repeat(257)] {
            assert!(
                matches!(
                    encode_activity_result::<A>(
                        &A::output(EffectObservation::UnavailableAtDeadline {
                            observed_at_unix_seconds: 1,
                            message,
                        })
                        .unwrap(),
                    ),
                    Err(ActivityCallError::ResultEncoding)
                ),
                "{}",
                A::NAME
            );
        }
    }

    macro_rules! assert_replica_activity_bounds {
        ($activity:ty, $input:expr) => {{
            assert_exact_input_bound::<$activity, _>(|padding| {
                let mut input = $input;
                input.execution_id = "x".repeat(padding);
                input
            });
            assert_replica_result_error_bound::<$activity>();
        }};
    }

    macro_rules! assert_label_activity_bounds {
        ($activity:ty, $input:expr) => {{
            assert_exact_input_bound::<$activity, _>(|padding| {
                let mut input = $input;
                input.execution_id = "x".repeat(padding);
                input
            });
            assert_label_result_error_bound::<$activity>();
        }};
    }

    fn assert_bounded_message_result<A, F>(make: F)
    where
        A: DurableActivity,
        F: Fn(String) -> A::Output,
    {
        for message in ["x".repeat(512), "é".repeat(256)] {
            let encoded = encode_activity_result::<A>(&make(message)).unwrap();
            assert!(decode_activity_result::<A>(&encoded).is_ok());
        }
        for message in ["x".repeat(513), "é".repeat(257)] {
            assert!(matches!(
                encode_activity_result::<A>(&make(message)),
                Err(ActivityCallError::ResultEncoding)
            ));
        }
        assert!(matches!(
            decode_activity_result::<A>(&kuberic_durable_execution::ExactBytes::new(vec![
                b'x';
                usize::try_from(A::MAX_RESULT_BYTES).unwrap()
                    + 1
            ])),
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
                    assert!(<$activity>::MAX_INPUT_BYTES > 0);
                    assert!(<$activity>::MAX_RESULT_BYTES > 0);
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
            RevokeWritesActivity::MAX_INPUT_BYTES,
            InstallTargetCurrentConfigurationActivity::MAX_INPUT_BYTES
        );
        assert_ne!(
            PublishTargetPrimaryLabelActivity::MAX_RESULT_BYTES,
            RevokeWritesActivity::MAX_RESULT_BYTES
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
        let outcomes = [
            RevokeWritesOutput::Applied {
                observed_at_unix_seconds: 1,
            },
            RevokeWritesOutput::ProvenNoAdmission {
                observed_at_unix_seconds: 2,
            },
            RevokeWritesOutput::Failed {
                observed_at_unix_seconds: 3,
                message: "failed".to_string(),
            },
            RevokeWritesOutput::DeadlineExceeded {
                observed_at_unix_seconds: 4,
                message: "deadline exceeded".to_string(),
            },
            RevokeWritesOutput::UnavailableAtDeadline {
                observed_at_unix_seconds: 5,
                message: "unavailable at deadline".to_string(),
            },
            RevokeWritesOutput::Conflicting {
                observed_at_unix_seconds: 6,
                message: "conflicting".to_string(),
            },
        ];
        for outcome in outcomes {
            let encoded = serde_json::to_vec(&outcome).unwrap();
            let decoded = serde_json::from_slice::<RevokeWritesOutput>(&encoded).unwrap();
            assert_eq!(decoded, outcome);
        }
    }

    #[test]
    fn direct_switchover_activity_error_reload_rejects_one_over_utf8_bound() {
        for message in ["x".repeat(513), "é".repeat(257)] {
            assert!(
                serde_json::from_value::<RevokeWritesOutput>(serde_json::json!({
                    "result": "failed",
                    "observed_at_unix_seconds": 1,
                    "message": message,
                }))
                .is_err()
            );
        }
    }

    #[test]
    fn direct_switchover_every_activity_accepts_exact_bounds_and_rejects_one_over() {
        assert_replica_activity_bounds!(RevokeWritesActivity, revoke_input());
        assert_replica_activity_bounds!(
            DemoteOldPrimaryActivity,
            DemoteOldPrimaryInput {
                contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
                execution_id: "execution".to_string(),
                old_primary_id: 1,
                old_primary_instance_id: "instance-1".to_string(),
                deadline_unix_seconds: 10,
                redelivery: 0,
                prepared_command: None,
            }
        );
        assert_replica_activity_bounds!(
            PromoteTargetActivity,
            PromoteTargetInput {
                contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
                execution_id: "execution".to_string(),
                target_primary_id: 2,
                target_primary_instance_id: "instance-2".to_string(),
                deadline_unix_seconds: 10,
                redelivery: 0,
                prepared_command: None,
            }
        );
        assert_replica_activity_bounds!(
            DistributeReplicaEpochActivity,
            DistributeReplicaEpochInput {
                contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
                execution_id: "execution".to_string(),
                distribution_index: 0,
                replica_id: 2,
                replica_instance_id: "instance-2".to_string(),
                deadline_unix_seconds: 10,
                redelivery: 0,
                prepared_command: None,
            }
        );
        assert_replica_activity_bounds!(
            InstallTargetCatchUpConfigurationActivity,
            InstallTargetCatchUpConfigurationInput {
                contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
                execution_id: "execution".to_string(),
                target_primary_id: 2,
                target_primary_instance_id: "instance-2".to_string(),
                deadline_unix_seconds: 10,
                redelivery: 0,
                prepared_command: None,
            }
        );
        assert_replica_activity_bounds!(
            WaitTargetWriteQuorumActivity,
            WaitTargetWriteQuorumInput {
                contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
                execution_id: "execution".to_string(),
                target_primary_id: 2,
                target_primary_instance_id: "instance-2".to_string(),
                deadline_unix_seconds: 10,
                redelivery: 0,
                prepared_command: None,
            }
        );
        assert_replica_activity_bounds!(
            InstallTargetCurrentConfigurationActivity,
            InstallTargetCurrentConfigurationInput {
                contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
                execution_id: "execution".to_string(),
                target_primary_id: 2,
                target_primary_instance_id: "instance-2".to_string(),
                deadline_unix_seconds: 10,
                redelivery: 0,
                prepared_command: None,
            }
        );
        assert_replica_activity_bounds!(
            RestorePreviousCurrentConfigurationActivity,
            RestorePreviousCurrentConfigurationInput {
                contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
                execution_id: "execution".to_string(),
                old_primary_id: 1,
                old_primary_instance_id: "instance-1".to_string(),
                deadline_unix_seconds: 10,
                redelivery: 0,
                prepared_command: None,
            }
        );
        assert_replica_activity_bounds!(
            CompensatePromoteOldPrimaryActivity,
            CompensatePromoteOldPrimaryInput {
                contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
                execution_id: "execution".to_string(),
                old_primary_id: 1,
                old_primary_instance_id: "instance-1".to_string(),
                deadline_unix_seconds: 10,
                redelivery: 0,
                prepared_command: None,
            }
        );
        assert_replica_activity_bounds!(
            CompensateDistributeReplicaEpochActivity,
            CompensateDistributeReplicaEpochInput {
                contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
                execution_id: "execution".to_string(),
                distribution_index: 0,
                replica_id: 2,
                replica_instance_id: "instance-2".to_string(),
                deadline_unix_seconds: 10,
                redelivery: 0,
                prepared_command: None,
            }
        );
        assert_replica_activity_bounds!(
            InstallCompensationCatchUpConfigurationActivity,
            InstallCompensationCatchUpConfigurationInput {
                contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
                execution_id: "execution".to_string(),
                old_primary_id: 1,
                old_primary_instance_id: "instance-1".to_string(),
                deadline_unix_seconds: 10,
                redelivery: 0,
                prepared_command: None,
            }
        );
        assert_replica_activity_bounds!(
            InstallCompensationCurrentConfigurationActivity,
            InstallCompensationCurrentConfigurationInput {
                contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
                execution_id: "execution".to_string(),
                old_primary_id: 1,
                old_primary_instance_id: "instance-1".to_string(),
                deadline_unix_seconds: 10,
                redelivery: 0,
                prepared_command: None,
            }
        );

        assert_label_activity_bounds!(PublishTargetPrimaryLabelActivity, target_label_input());
        assert_label_activity_bounds!(
            PublishOldPrimarySecondaryLabelActivity,
            PublishOldPrimarySecondaryLabelInput {
                contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
                execution_id: "execution".to_string(),
                old_primary_id: 1,
                old_primary_instance_id: "instance-1".to_string(),
                deadline_unix_seconds: 10,
                prepared_command: None,
            }
        );
        assert_label_activity_bounds!(
            RestoreOldPrimaryLabelActivity,
            RestoreOldPrimaryLabelInput {
                contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
                execution_id: "execution".to_string(),
                old_primary_id: 1,
                old_primary_instance_id: "instance-1".to_string(),
                deadline_unix_seconds: 10,
                prepared_command: None,
            }
        );
        assert_label_activity_bounds!(
            RestoreTargetSecondaryLabelActivity,
            RestoreTargetSecondaryLabelInput {
                contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
                execution_id: "execution".to_string(),
                target_primary_id: 2,
                target_primary_instance_id: "instance-2".to_string(),
                deadline_unix_seconds: 10,
                prepared_command: None,
            }
        );

        assert_exact_input_bound::<CaptureFrozenLsnActivity, _>(|padding| CaptureFrozenLsnInput {
            contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
            execution_id: "x".repeat(padding),
            old_primary_id: 1,
            old_primary_instance_id: "instance-1".to_string(),
            expected_epoch: snapshot().epoch,
            deadline_unix_seconds: 10,
        });
        assert_bounded_message_result::<CaptureFrozenLsnActivity, _>(|message| {
            CaptureFrozenLsnOutput::Conflicting {
                observed_at_unix_seconds: 1,
                message,
            }
        });

        assert_exact_input_bound::<WaitTargetCaughtUpActivity, _>(|padding| {
            WaitTargetCaughtUpInput {
                contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
                execution_id: "x".repeat(padding),
                target_id: 2,
                target_instance_id: "instance-2".to_string(),
                expected_epoch: snapshot().epoch,
                frozen_lsn: 10,
                deadline_unix_seconds: 10,
            }
        });
        assert_bounded_message_result::<WaitTargetCaughtUpActivity, _>(|message| {
            WaitTargetCaughtUpOutput::Conflicting {
                observed_at_unix_seconds: 1,
                message,
            }
        });

        assert_exact_input_bound::<AttestTargetTopologyActivity, _>(|padding| {
            AttestTargetTopologyInput {
                contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
                execution_id: "x".repeat(padding),
                expected_snapshot: snapshot(),
                deadline_unix_seconds: 10,
            }
        });
        assert_bounded_message_result::<AttestTargetTopologyActivity, _>(|message| {
            AttestTargetTopologyOutput::Conflicting {
                observed_at_unix_seconds: 1,
                message,
            }
        });

        assert_exact_input_bound::<AttestCompensatedTopologyActivity, _>(|padding| {
            AttestCompensatedTopologyInput {
                contract_version: DIRECT_SWITCHOVER_CONTRACT_VERSION,
                execution_id: "x".repeat(padding),
                expected_snapshot: snapshot(),
                deadline_unix_seconds: 10,
            }
        });
        assert_bounded_message_result::<AttestCompensatedTopologyActivity, _>(|message| {
            AttestCompensatedTopologyOutput::Conflicting {
                observed_at_unix_seconds: 1,
                message,
            }
        });
    }
}
