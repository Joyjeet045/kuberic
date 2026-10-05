//! Fail-closed validation for accepted authority and observed replica state.

use std::collections::{BTreeMap, BTreeSet};

mod scale_down;
mod scale_up;

pub use scale_down::*;
pub use scale_up::*;
use thiserror::Error;

use crate::protocol::observation::{AgentObservation, ObservationSnapshot, ReplicaObservationKey};
use crate::protocol::types::{
    AcceptedStatus, AccessStatus, ConfigurationDescriptor, EffectivePolicy, Epoch, ReplicaId,
    ReplicaIdentity, ReplicaRole, TransitionKind, duplicate_replica_ids,
};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ValidationError {
    #[error("invalid scale-up authority: {0}")]
    InvalidScaleUp(&'static str),
    #[error("invalid secondary scale-down authority: {0}")]
    InvalidSecondaryScaleDown(&'static str),
    #[error("desired replica count must be greater than zero")]
    DesiredReplicasZero,
    #[error("initialized status has no accepted topology")]
    InitializedWithoutTopology,
    #[error("initialized status has no frozen effective policy")]
    InitializedWithoutPolicy,
    #[error("never-initialized status contains a frozen effective policy")]
    PolicyBeforeInitialization,
    #[error("active transition effective policy differs from frozen status policy")]
    TransitionPolicyMismatch,
    #[error("never-initialized status contains an accepted topology")]
    TopologyBeforeInitialization,
    #[error("status cannot contain provisioning and a PC/CC transition simultaneously")]
    ProvisioningAndTransition,
    #[error("provisioning intent requires initialized accepted topology")]
    ProvisioningWithoutTopology,
    #[error("configuration ID {actual} does not match canonical ID {expected}")]
    ConfigurationIdMismatch { actual: String, expected: String },
    #[error("configuration must contain at least one member")]
    EmptyConfiguration,
    #[error("configuration epoch values must not be negative")]
    InvalidConfigurationEpoch,
    #[error("configuration has duplicate logical replica IDs: {0:?}")]
    DuplicateReplicaIds(Vec<i64>),
    #[error("configuration contains invalid replica ID {0}; IDs must be positive")]
    InvalidReplicaId(i64),
    #[error("configuration has duplicate exact replica identity {0}")]
    DuplicateReplicaIdentity(String),
    #[error("configuration primary {0} is not a Primary member")]
    MissingPrimary(i64),
    #[error("configuration has {0} Primary members; expected exactly one")]
    InvalidPrimaryCount(usize),
    #[error("configuration member count {actual} does not match frozen size {expected}")]
    ReplicaSetSizeMismatch { actual: u32, expected: u32 },
    #[error("configuration write quorum {actual} does not match fixed quorum {expected}")]
    WriteQuorumMismatch { actual: u32, expected: u32 },
    #[error("effective policy quorum values do not match fixed policy for size {0}")]
    InvalidEffectivePolicy(u32),
    #[error("bootstrap transition must not reference a Previous Configuration")]
    BootstrapHasPreviousConfiguration,
    #[error("bootstrap transition cannot coexist with accepted topology")]
    BootstrapHasTopology,
    #[error("non-bootstrap transition has no accepted Previous Configuration")]
    TransitionWithoutTopology,
    #[error("transition Previous Configuration {actual:?} does not match topology {expected}")]
    PreviousConfigurationMismatch {
        actual: Option<String>,
        expected: String,
    },
    #[error("Current Configuration data-loss epoch differs from Previous Configuration")]
    TransitionDataLossChanged,
    #[error("Current Configuration epoch must be newer than Previous Configuration")]
    TransitionEpochNotNewer,
    #[error("Previous and Current Configuration logical membership differs")]
    TransitionLogicalMembershipChanged,
    #[error("failover must preserve exact membership")]
    FailoverMembershipChanged,
    #[error("failover carrying replacement membership must retain its build authority")]
    FailoverReplacementWithoutBuild,
    #[error("failover repair target must be an exact non-primary Current Configuration member")]
    InvalidFailoverRepairTarget,
    #[error("failover requires a non-negative election-safe LSN")]
    InvalidFailoverElectionLsn,
    #[error("replacement must change exactly one non-primary incarnation")]
    InvalidReplacementMembership,
    #[error("replacement cleanup must identify an exact excluded incarnation")]
    InvalidReplacementCleanup,
    #[error("replacement must preserve the accepted primary")]
    ReplacementPrimaryChanged,
    #[error("build source is not the exact Current Configuration primary")]
    BuildSourceNotPrimary,
    #[error("bootstrap build target must be an exact non-primary genesis member")]
    InvalidBootstrapBuildTarget,
    #[error("provisioning build target must remain outside configuration authority")]
    ProvisioningBuildTargetInAuthority,
    #[error("build replication boundary must not be negative")]
    NegativeBuildBoundary,
    #[error("provisioning target reuses the accepted exact incarnation")]
    ProvisioningReusesAcceptedIncarnation,
    #[error("provisioning does not replace one accepted non-primary incarnation")]
    InvalidProvisioningReplacement,
    #[error("replica observation key {key} does not match reported identity {reported}")]
    ReplicaObservationKeyMismatch { key: String, reported: String },
    #[error("replica observation key does not match Kubernetes Pod identity")]
    KubernetesObservationKeyMismatch,
    #[error("replica {0} reports a different resource UID")]
    ReplicaResourceMismatch(i64),
    #[error("replica {replica_id} report sequence {observed} did not advance past {previous}")]
    StaleReportSequence {
        replica_id: i64,
        observed: u64,
        previous: u64,
    },
    #[error("replica {replica_id} reports stale epoch {observed:?} below accepted {accepted:?}")]
    StaleReplicaEpoch {
        replica_id: i64,
        observed: crate::protocol::types::Epoch,
        accepted: crate::protocol::types::Epoch,
    },
    #[error("replica {replica_id} contradicts accepted configuration at epoch {epoch:?}")]
    ConflictingReplicaConfiguration {
        replica_id: i64,
        epoch: crate::protocol::types::Epoch,
    },
    #[error("replica {replica_id} identity contradicts accepted member identity")]
    ConflictingReplicaIdentity { replica_id: i64 },
    #[error("replica {0} reports missing storage for an established authority incarnation")]
    EstablishedStoreMissing(i64),
    #[error("replica {0} report epoch or role contradicts its installed configuration")]
    InvalidReplicaReportAuthority(i64),
    #[error("replica {replica_id} reports epoch {observed:?} newer than authorized {authorized:?}")]
    UnauthorizedReplicaEpoch {
        replica_id: i64,
        observed: Epoch,
        authorized: Epoch,
    },
    #[error("provisioning replica {0} claims replication or write authority")]
    ProvisioningClaimsAuthority(i64),
    #[error("unrelated replica {0} claims primary or write authority")]
    UnrelatedReplicaClaimsAuthority(i64),
    #[error("bootstrap observed initialized authority outside its frozen Current Configuration")]
    BootstrapHasUnrelatedAuthority,
    #[error("bootstrap replica {0} granted writes before topology acceptance")]
    BootstrapWriteGranted(i64),
    #[error("bootstrap replica {0} claims Primary contrary to frozen role")]
    BootstrapRoleConflict(i64),
    #[error("multiple replicas claim Primary for the same accepted authority: {0:?}")]
    ConflictingPrimaryClaims(Vec<i64>),
    #[error("uninitialized agent identity does not match observed Pod/PVC scaffolding")]
    UninitializedScaffoldingMismatch,
    #[error("uninitialized agent identity does not match authorized provisioning")]
    UninitializedProvisioningMismatch,
    #[error("bootstrap replica {0} reports a Previous Configuration")]
    BootstrapReportHasPreviousConfiguration(i64),
    #[error("replica {0} reports a Previous Configuration that differs from frozen authority")]
    ReportedPreviousConfigurationMismatch(i64),
    #[error("primary failure observation does not match the accepted primary")]
    PrimaryFailureMismatch,
    #[error("quorum-loss observation does not match the accepted configuration")]
    QuorumLossMismatch,
    #[error("non-switchover transition contains planned switchover authority")]
    UnexpectedSwitchoverIntent,
    #[error("planned switchover transition is missing its frozen request authority")]
    MissingSwitchoverIntent,
    #[error("planned switchover source is not the accepted exact primary")]
    InvalidSwitchoverSource,
    #[error("planned switchover target is not an accepted exact non-primary member")]
    InvalidSwitchoverTarget,
    #[error("planned switchover Current Configuration primary contradicts its resolution")]
    InvalidSwitchoverResolution,
    #[error("planned switchover contains unrelated election, build, or repair authority")]
    InvalidSwitchoverEvidence,
    #[error("planned switchover handoff certificate is malformed")]
    InvalidSwitchoverHandoff,
    #[error("planned switchover receipt is malformed")]
    InvalidSwitchoverReceipt,
}

/// Validates accepted status and every observed exact replica incarnation.
pub fn validate_snapshot(snapshot: &ObservationSnapshot) -> Result<(), ValidationError> {
    if snapshot.desired.replicas == 0
        && snapshot.status.secondary_scale_down_cleanup.is_none()
        && !snapshot
            .status
            .transition
            .as_ref()
            .is_some_and(|t| t.kind == TransitionKind::SecondaryScaleDown)
    {
        return Err(ValidationError::DesiredReplicasZero);
    }
    validate_status(&snapshot.status)?;
    if snapshot
        .status
        .last_replacement
        .iter()
        .chain(snapshot.status.pending_replacement_cleanup.iter())
        .any(|cleanup| cleanup.resource_uid != snapshot.resource_uid)
    {
        return Err(ValidationError::InvalidReplacementCleanup);
    }
    for cleanup in snapshot
        .status
        .last_replacement
        .iter()
        .chain(snapshot.status.pending_replacement_cleanup.iter())
    {
        if snapshot
            .status
            .topology
            .iter()
            .flat_map(|t| &t.configuration.members)
            .chain(
                snapshot
                    .status
                    .transition
                    .iter()
                    .flat_map(|t| &t.current_configuration.members),
            )
            .filter(|m| m.identity != cleanup.target)
            .any(|m| {
                snapshot
                    .observation_for_identity(&m.identity)
                    .and_then(|o| o.kubernetes.as_ref())
                    .is_some_and(|k| {
                        matches!(&cleanup.resources.pvc,
                    crate::protocol::types::CleanupResourceIdentity::Present { uid, .. }
                        if k.pvc_uid.as_ref().is_some_and(|pvc| pvc.as_str() == uid))
                    })
            })
        {
            return Err(ValidationError::InvalidReplacementCleanup);
        }
    }
    let removal = snapshot
        .status
        .transition
        .as_ref()
        .and_then(|transition| transition.secondary_scale_down.as_ref())
        .or_else(|| {
            snapshot
                .status
                .secondary_scale_down_cleanup
                .as_ref()
                .map(|cleanup| &cleanup.evidence.preparation.intent)
        });
    if removal.is_some_and(|intent| intent.resource_uid != snapshot.resource_uid) {
        return Err(ValidationError::InvalidSecondaryScaleDown(
            "resource UID mismatch",
        ));
    }
    if let Some(operation_id) = &snapshot.status.scale_up_admission_started {
        let active = snapshot.status.transition.as_ref().and_then(|transition| {
            transition.scale_up.as_deref().or_else(|| {
                transition
                    .scale_up_failover
                    .as_deref()
                    .map(|evidence| &evidence.intent)
            })
        });
        if active.is_none_or(|intent| &intent.operation_id != operation_id) {
            return Err(ValidationError::InvalidScaleUp(
                "scale-up admission-start fence differs from active authority",
            ));
        }
    }
    if snapshot
        .status
        .last_secondary_removal
        .as_ref()
        .is_some_and(|receipt| {
            receipt.evidence.preparation.intent.resource_uid != snapshot.resource_uid
        })
    {
        return Err(ValidationError::InvalidSecondaryScaleDown(
            "completed removal resource UID mismatch",
        ));
    }
    if snapshot
        .status
        .scale_up_allocation
        .as_ref()
        .is_some_and(|allocation| allocation.resource_uid != snapshot.resource_uid)
        || snapshot
            .status
            .scale_up_cleanup
            .as_ref()
            .is_some_and(|cleanup| {
                cleanup
                    .provisioning
                    .scale_up()
                    .is_none_or(|scale_up| scale_up.resource_uid != snapshot.resource_uid)
            })
        || snapshot
            .status
            .last_scale_up
            .as_ref()
            .is_some_and(|receipt| receipt.intent.resource_uid != snapshot.resource_uid)
    {
        return Err(ValidationError::InvalidScaleUp("resource UID mismatch"));
    }
    if snapshot
        .status
        .transition
        .as_ref()
        .and_then(|transition| {
            transition.scale_up.as_deref().or_else(|| {
                transition
                    .scale_up_failover
                    .as_deref()
                    .map(|evidence| &evidence.intent)
            })
        })
        .is_some_and(|intent| intent.resource_uid != snapshot.resource_uid)
    {
        return Err(ValidationError::InvalidScaleUp(
            "active transition resource UID mismatch",
        ));
    }

    if let Some(provisioning) = &snapshot.status.provisioning {
        validate_scale_up_provisioning(provisioning)?;
        let target = provisioning.target_identity(&snapshot.resource_uid);
        if let Some(topology) = &snapshot.status.topology
            && topology.configuration.members.iter().any(|member| {
                member.identity.replica_id == target.replica_id
                    && member.identity.instance_id == target.instance_id
            })
        {
            return Err(ValidationError::ProvisioningReusesAcceptedIncarnation);
        }
        let topology = snapshot
            .status
            .topology
            .as_ref()
            .ok_or(ValidationError::ProvisioningWithoutTopology)?;
        match provisioning.purpose.kind {
            crate::protocol::types::ProvisioningKind::Replacement => {
                let replaces = provisioning
                    .replacement()
                    .ok_or(ValidationError::InvalidProvisioningReplacement)?;
                if replaces.replica_id == topology.configuration.primary_id
                    || !topology
                        .configuration
                        .members
                        .iter()
                        .any(|member| member.identity == *replaces)
                {
                    return Err(ValidationError::InvalidProvisioningReplacement);
                }
            }
            crate::protocol::types::ProvisioningKind::ScaleUp => {
                let scale_up = provisioning
                    .scale_up()
                    .ok_or(ValidationError::InvalidScaleUp(
                        "missing scale-up provisioning payload",
                    ))?;
                let accepted_authority_matches = topology.configuration.epoch
                    >= scale_up.previous_configuration.epoch
                    && exact_identities(&topology.configuration)
                        == exact_identities(&scale_up.previous_configuration);
                if scale_up.resource_uid != snapshot.resource_uid
                    || !accepted_authority_matches
                    || snapshot.status.effective_policy.as_ref() != Some(&scale_up.previous_policy)
                {
                    return Err(ValidationError::InvalidScaleUp(
                        "provisioning differs from accepted authority",
                    ));
                }
            }
        }
    }

    let mut primary_claims: BTreeMap<_, Vec<ReplicaIdentity>> = BTreeMap::new();
    for (key, observation) in &snapshot.replicas {
        if let Some(kubernetes) = &observation.kubernetes
            && (kubernetes.replica_id != key.replica_id
                || kubernetes
                    .pod_uid
                    .as_ref()
                    .is_some_and(|pod_uid| pod_uid.as_str() != key.instance_id.as_str()))
        {
            return Err(ValidationError::KubernetesObservationKeyMismatch);
        }
        match &observation.agent {
            AgentObservation::Uninitialized(report) => {
                if report.replica_id != key.replica_id
                    || report.pod_uid.as_str() != key.instance_id.as_str()
                {
                    return Err(ValidationError::ReplicaObservationKeyMismatch {
                        key: observation_key_string(key),
                        reported: format!("{}@{}", report.replica_id, report.pod_uid),
                    });
                }
                if report.resource_uid != snapshot.resource_uid {
                    return Err(ValidationError::ReplicaResourceMismatch(
                        key.replica_id.value(),
                    ));
                }
                validate_report_sequence(
                    snapshot,
                    key,
                    &report.process_session_id,
                    report.report_sequence,
                )?;
                let matches_scaffolding =
                    observation.kubernetes.as_ref().is_some_and(|kubernetes| {
                        kubernetes.replica_id == key.replica_id
                            && kubernetes.pod_uid.as_ref() == Some(&report.pod_uid)
                            && kubernetes.pvc_uid.as_ref() == Some(&report.pvc_uid)
                    });
                if !matches_scaffolding {
                    return Err(ValidationError::UninitializedScaffoldingMismatch);
                }
                let accepted_instance = snapshot.status.topology.as_ref().is_some_and(|topology| {
                    topology.configuration.members.iter().any(|member| {
                        member.identity.replica_id == key.replica_id
                            && member.identity.instance_id == key.instance_id
                    })
                });
                let established_transition_instance = snapshot
                    .status
                    .transition
                    .as_ref()
                    .is_some_and(|transition| {
                        transition.kind != TransitionKind::Bootstrap
                            && transition
                                .current_configuration
                                .members
                                .iter()
                                .any(|member| {
                                    member.identity.replica_id == key.replica_id
                                        && member.identity.instance_id == key.instance_id
                                })
                    });
                if accepted_instance || established_transition_instance {
                    return Err(ValidationError::EstablishedStoreMissing(
                        key.replica_id.value(),
                    ));
                }
                if let Some(provisioning) = snapshot.status.provisioning.as_ref()
                    && provisioning.replica_id() == key.replica_id
                    && provisioning.instance_id() == key.instance_id
                    && (provisioning.pod_uid != report.pod_uid
                        || provisioning.pvc_uid != report.pvc_uid)
                {
                    return Err(ValidationError::UninitializedProvisioningMismatch);
                }
            }
            AgentObservation::Report(report) => {
                if report.identity.replica_id != key.replica_id
                    || report.identity.instance_id != key.instance_id
                {
                    return Err(ValidationError::ReplicaObservationKeyMismatch {
                        key: observation_key_string(key),
                        reported: format!(
                            "{}@{}",
                            report.identity.replica_id, report.identity.instance_id
                        ),
                    });
                }
                if report.resource_uid != snapshot.resource_uid {
                    return Err(ValidationError::ReplicaResourceMismatch(
                        key.replica_id.value(),
                    ));
                }
                validate_report_sequence(
                    snapshot,
                    key,
                    &report.process_session_id,
                    report.report_sequence,
                )?;
                if let Some(previous) = &report.previous_configuration {
                    validate_configuration(previous, None)?;
                }

                if let Some(current) = &report.current_configuration {
                    validate_configuration(current, None)?;
                }
                if let Some(reported) = report.scale_up_intent.as_deref() {
                    let active = snapshot.status.transition.as_ref().and_then(|transition| {
                        transition.scale_up.as_deref().or_else(|| {
                            transition
                                .scale_up_failover
                                .as_deref()
                                .map(|evidence| &evidence.intent)
                        })
                    });
                    let completed = snapshot
                        .status
                        .last_scale_up
                        .as_deref()
                        .map(|receipt| &receipt.intent);
                    if active != Some(reported) && completed != Some(reported) {
                        return Err(ValidationError::InvalidScaleUp(
                            "reported scale-up attempt differs from persisted authority",
                        ));
                    }
                }
                validate_report_internal(report)?;
                validate_report_authority(snapshot, report)?;
                if report.role == ReplicaRole::Primary
                    && let Some(current) = &report.current_configuration
                {
                    primary_claims
                        .entry((report.epoch, current.configuration_id.clone()))
                        .or_default()
                        .push(report.identity.clone());
                }
            }
            AgentObservation::Absent
            | AgentObservation::Unreachable { .. }
            | AgentObservation::Invalid { .. } => {}
        }
    }

    if let Some((_, claims)) = primary_claims
        .into_iter()
        .find(|(_, claims)| claims.len() > 1)
    {
        return Err(ValidationError::ConflictingPrimaryClaims(
            claims
                .into_iter()
                .map(|identity| identity.replica_id.value())
                .collect(),
        ));
    }

    Ok(())
}

pub fn validate_report_internal(
    report: &crate::protocol::observation::AgentReport,
) -> Result<(), ValidationError> {
    if report
        .pending_configuration
        .as_ref()
        .is_some_and(|command| {
            report.pending_operation_id.as_ref() != Some(&command.operation_id)
                || command.local_replica_id != report.identity.replica_id
                || command.expected_instance_id != report.identity.instance_id
                || command.expected_agent_generation != report.identity.agent_generation
        })
        || (report.pending_operation_id.is_none() && report.pending_configuration.is_some())
    {
        return Err(ValidationError::InvalidReplicaReportAuthority(
            report.identity.replica_id.value(),
        ));
    }
    validate_secondary_removal_report(report)?;
    if report.scale_up_intent.is_some() && report.secondary_removal_evidence.is_some() {
        return Err(ValidationError::InvalidScaleUp(
            "report cannot combine scale-up and removal evidence",
        ));
    }
    let mut build_ids = BTreeSet::new();
    if report.builds.iter().any(|build| {
        build.build_id.is_empty()
            || build.target.replica_id.value() <= 0
            || build.target.instance_id.is_empty()
            || build.target.agent_generation.is_empty()
            || build.replication_boundary_lsn < 0
            || build.durable_lsn < 0
            || build
                .catch_up_boundary_lsn
                .is_some_and(|boundary| boundary < 0)
            || !build_ids.insert(build.build_id.clone())
    }) {
        return Err(ValidationError::InvalidReplicaReportAuthority(
            report.identity.replica_id.value(),
        ));
    }
    if let Some(intent) = report.scale_up_intent.as_deref() {
        validate_scale_up(intent)?;
        let current =
            report
                .current_configuration
                .as_ref()
                .ok_or(ValidationError::InvalidScaleUp(
                    "scale-up report requires current authority",
                ))?;
        let current_matches = current == &intent.current_configuration
            || (exact_identities(current) == exact_identities(&intent.current_configuration)
                && current.epoch.data_loss_number
                    == intent.current_configuration.epoch.data_loss_number
                && current.epoch.configuration_number
                    > intent.current_configuration.epoch.configuration_number);
        let previous_matches = report
            .previous_configuration
            .as_ref()
            .is_none_or(|previous| {
                previous == &intent.previous_configuration
                    || (current != &intent.current_configuration
                        && exact_identities(previous) == exact_identities(current)
                        && previous.epoch.data_loss_number == current.epoch.data_loss_number
                        && previous.epoch.configuration_number < current.epoch.configuration_number)
            });
        if report.resource_uid != intent.resource_uid
            || !current_matches
            || !previous_matches
            || !current
                .members
                .iter()
                .any(|member| member.identity == report.identity)
        {
            return Err(ValidationError::InvalidScaleUp(
                "report differs from exact scale-up authority",
            ));
        }
    }
    if report.previous_configuration.is_some() && report.current_configuration.is_none() {
        return Err(ValidationError::InvalidReplicaReportAuthority(
            report.identity.replica_id.value(),
        ));
    }
    if let Some(current) = &report.current_configuration
        && current.epoch != report.epoch
    {
        return Err(ValidationError::InvalidReplicaReportAuthority(
            report.identity.replica_id.value(),
        ));
    }
    if report.role == ReplicaRole::Primary && report.current_configuration.is_none() {
        return Err(ValidationError::InvalidReplicaReportAuthority(
            report.identity.replica_id.value(),
        ));
    }
    if report.write_status == AccessStatus::Granted && report.role != ReplicaRole::Primary {
        return Err(ValidationError::InvalidReplicaReportAuthority(
            report.identity.replica_id.value(),
        ));
    }
    if report.verified_replication_lsn.is_some_and(|verified| {
        verified < 0 || verified > report.current_progress || report.current_configuration.is_none()
    }) {
        return Err(ValidationError::InvalidReplicaReportAuthority(
            report.identity.replica_id.value(),
        ));
    }
    if let Some(handoff) = &report.prepared_switchover {
        validate_switchover_handoff(handoff)?;
        if handoff.source != report.identity {
            return Err(ValidationError::InvalidSwitchoverHandoff);
        }
    }
    if let (Some(previous), Some(current)) = (
        report.previous_configuration.as_ref(),
        report.current_configuration.as_ref(),
    ) {
        let previous_ids = previous
            .members
            .iter()
            .map(|member| member.identity.replica_id)
            .collect::<BTreeSet<_>>();
        let current_ids = current
            .members
            .iter()
            .map(|member| member.identity.replica_id)
            .collect::<BTreeSet<_>>();
        if previous.epoch.data_loss_number != current.epoch.data_loss_number
            || previous.epoch.configuration_number >= current.epoch.configuration_number
            || (previous_ids != current_ids
                && report.secondary_removal_evidence.is_none()
                && report.scale_up_intent.is_none())
        {
            return Err(ValidationError::InvalidReplicaReportAuthority(
                report.identity.replica_id.value(),
            ));
        }
    }
    Ok(())
}

fn validate_report_sequence(
    snapshot: &ObservationSnapshot,
    key: &ReplicaObservationKey,
    session_id: &crate::protocol::types::ProcessSessionId,
    sequence: u64,
) -> Result<(), ValidationError> {
    if let Some(previous) = snapshot.previous_report_watermarks.get(key)
        && previous.process_session_id == *session_id
        && sequence <= previous.report_sequence
    {
        return Err(ValidationError::StaleReportSequence {
            replica_id: key.replica_id.value(),
            observed: sequence,
            previous: previous.report_sequence,
        });
    }
    Ok(())
}

fn validate_report_authority(
    snapshot: &ObservationSnapshot,
    report: &crate::protocol::observation::AgentReport,
) -> Result<(), ValidationError> {
    let historical_receipt = snapshot.status.last_secondary_removal.as_ref();
    // A completed certificate remains evidence of local history after accepted
    // authority advances, but never authorizes work or access at that authority.
    let stale_completed = historical_receipt.is_some_and(|receipt| {
        let intent = &receipt.evidence.preparation.intent;
        snapshot.status.topology.as_ref().is_some_and(|topology| {
            report.epoch < topology.configuration.epoch
                && topology
                    .configuration
                    .members
                    .iter()
                    .any(|member| member.identity == report.identity)
        }) && report.write_status != AccessStatus::Granted
            && report.previous_configuration.is_none()
            && report.prepared_secondary_removal.is_none()
            && report.current_configuration.as_ref() == Some(&intent.current_configuration)
            && report.secondary_removal_evidence.as_ref() == Some(&receipt.evidence)
            && report
                .accepted_secondary_removal
                .as_ref()
                .is_none_or(|accepted| {
                    accepted.evidence == receipt.evidence
                        && accepted.current_only_write_quorum == receipt.current_only_write_quorum
                })
    });
    let completed = snapshot
        .status
        .last_secondary_removal
        .as_ref()
        .filter(|receipt| {
            snapshot.status.topology.as_ref().is_some_and(|topology| {
                topology.configuration == receipt.evidence.preparation.intent.current_configuration
            })
        })
        .map(|receipt| receipt.committed());
    let committed = snapshot
        .status
        .secondary_scale_down_cleanup
        .as_ref()
        .or(completed.as_ref());
    let removal = snapshot
        .status
        .transition
        .as_ref()
        .and_then(|transition| transition.secondary_scale_down.as_ref())
        .or_else(|| committed.map(|cleanup| &cleanup.evidence.preparation.intent));
    for intent in [
        report
            .prepared_secondary_removal
            .as_ref()
            .map(|prepared| &prepared.intent),
        report
            .secondary_removal_evidence
            .as_ref()
            .map(|evidence| &evidence.preparation.intent),
        report
            .retired_replica
            .as_ref()
            .map(|retirement| &retirement.intent),
    ]
    .into_iter()
    .flatten()
    {
        let historical = report
            .secondary_removal_evidence
            .as_ref()
            .is_some_and(|evidence| {
                &evidence.preparation.intent == intent
                    && report.previous_configuration.is_none()
                    && report
                        .prepared_secondary_removal
                        .as_ref()
                        .is_none_or(|p| removal == Some(&p.intent))
                    && report.current_configuration.as_ref() == Some(&intent.current_configuration)
                    && snapshot.status.topology.as_ref().is_some_and(|topology| {
                        topology.configuration == intent.current_configuration
                            || removal.is_some_and(|active| {
                                active.previous_configuration == intent.current_configuration
                            })
                    })
            });
        let known_history = stale_completed
            && historical_receipt
                .is_some_and(|receipt| &receipt.evidence.preparation.intent == intent);
        if removal != Some(intent) && !historical && !known_history {
            return Err(ValidationError::InvalidSecondaryScaleDown(
                "report is not authorized by the frozen removal",
            ));
        }
    }
    if let Some(intent) = removal
        && report
            .accepted_secondary_removal
            .as_ref()
            .is_some_and(|c| c.evidence.preparation.intent == *intent)
        && committed.is_none_or(|c| c.evidence.preparation.intent != *intent)
    {
        return Err(ValidationError::InvalidSecondaryScaleDown(
            "local acceptance cannot precede committed cluster topology",
        ));
    }
    if let (Some(committed), Some(reported)) =
        (committed, report.accepted_secondary_removal.as_ref())
        && reported.evidence.preparation.intent == committed.evidence.preparation.intent
        && (reported.evidence != committed.evidence
            || reported.current_only_write_quorum != committed.current_only_write_quorum)
    {
        return Err(ValidationError::InvalidSecondaryScaleDown(
            "local acceptance must retain the exact committed quorum evidence",
        ));
    }
    if let Some(intent) = removal
        && report.current_configuration.as_ref() == Some(&intent.current_configuration)
        && (report.secondary_removal_evidence.is_none()
            || (snapshot
                .status
                .transition
                .as_ref()
                .is_some_and(|transition| {
                    transition.secondary_scale_down.as_ref() == Some(intent)
                })
                && report.write_status == AccessStatus::Granted))
    {
        return Err(ValidationError::InvalidSecondaryScaleDown(
            "reduced authority requires evidence and pre-commit write closure",
        ));
    }
    let frozen_removal_evidence = snapshot
        .status
        .transition
        .as_ref()
        .and_then(|transition| transition.secondary_removal_evidence.as_ref())
        .or_else(|| {
            committed
                .filter(|cleanup| removal == Some(&cleanup.evidence.preparation.intent))
                .map(|cleanup| &cleanup.evidence)
        });
    if let Some(evidence) = &report.secondary_removal_evidence
        && removal == Some(&evidence.preparation.intent)
    {
        let Some(frozen) = frozen_removal_evidence else {
            return Err(ValidationError::InvalidSecondaryScaleDown(
                "report does not retain the frozen admission evidence",
            ));
        };
        let reduced_evidence_matches = if report.previous_configuration.is_some() {
            evidence.reduced_write_quorum.is_empty()
                || evidence.reduced_write_quorum == frozen.reduced_write_quorum
        } else {
            evidence.reduced_write_quorum == frozen.reduced_write_quorum
        };
        if evidence.preparation != frozen.preparation
            || evidence.previous_read_quorum != frozen.previous_read_quorum
            || !reduced_evidence_matches
        {
            return Err(ValidationError::InvalidSecondaryScaleDown(
                "report does not retain the frozen admission evidence",
            ));
        }
    }
    if let Some(frozen) = frozen_removal_evidence
        && report
            .prepared_secondary_removal
            .as_ref()
            .is_some_and(|prepared| prepared != &frozen.preparation)
    {
        return Err(ValidationError::InvalidSecondaryScaleDown(
            "preparation differs from frozen admission boundary",
        ));
    }
    let provisioning = snapshot.status.transition.is_none()
        && snapshot.status.provisioning.as_ref().is_some_and(|intent| {
            intent.target_identity(&snapshot.resource_uid) == report.identity
        });
    if provisioning {
        if !matches!(report.role, ReplicaRole::None | ReplicaRole::IdleSecondary)
            || report.write_status == AccessStatus::Granted
            || report.epoch != Epoch::default()
            || report.previous_configuration.is_some()
            || report.current_configuration.is_some()
        {
            return Err(ValidationError::ProvisioningClaimsAuthority(
                report.identity.replica_id.value(),
            ));
        }
        return Ok(());
    }

    let accepted = snapshot
        .status
        .topology
        .as_ref()
        .map(|topology| &topology.configuration);
    let current = snapshot
        .status
        .transition
        .as_ref()
        .map(|transition| &transition.current_configuration);
    let accepted_exact = accepted.is_some_and(|configuration| {
        configuration
            .members
            .iter()
            .any(|member| member.identity == report.identity)
    });
    let current_exact = current.is_some_and(|configuration| {
        configuration
            .members
            .iter()
            .any(|member| member.identity == report.identity)
    });
    let accepted_instance = accepted.is_some_and(|configuration| {
        configuration.members.iter().any(|member| {
            member.identity.replica_id == report.identity.replica_id
                && member.identity.instance_id == report.identity.instance_id
        })
    });
    let current_instance = current.is_some_and(|configuration| {
        configuration.members.iter().any(|member| {
            member.identity.replica_id == report.identity.replica_id
                && member.identity.instance_id == report.identity.instance_id
        })
    });
    if !accepted_exact && !current_exact && (accepted_instance || current_instance) {
        return Err(ValidationError::ConflictingReplicaIdentity {
            replica_id: report.identity.replica_id.value(),
        });
    }

    if let Some(transition) = &snapshot.status.transition
        && transition.kind == TransitionKind::Bootstrap
        && current_exact
    {
        if report.previous_configuration.is_some() {
            return Err(ValidationError::BootstrapReportHasPreviousConfiguration(
                report.identity.replica_id.value(),
            ));
        }
        if report.write_status == AccessStatus::Granted {
            return Err(ValidationError::BootstrapWriteGranted(
                report.identity.replica_id.value(),
            ));
        }
        let expected = transition
            .current_configuration
            .members
            .iter()
            .find(|member| member.identity == report.identity)
            .expect("current exact identity has a member");
        if expected.role != ReplicaRole::Primary && report.role == ReplicaRole::Primary {
            return Err(ValidationError::BootstrapRoleConflict(
                report.identity.replica_id.value(),
            ));
        }
    }
    if let Some(transition) = &snapshot.status.transition
        && transition.kind == TransitionKind::PlannedSwitchover
        && report.epoch > accepted.expect("planned transition has topology").epoch
        && (report.write_status == AccessStatus::Granted
            || transition
                .switchover
                .as_ref()
                .is_none_or(|intent| intent.handoff.is_none()))
    {
        return Err(ValidationError::InvalidSwitchoverEvidence);
    }
    if let Some(intent) = snapshot
        .status
        .transition
        .as_ref()
        .and_then(|transition| transition.switchover.as_ref())
        && (accepted_exact || current_exact)
        && let Some(reported) = report.current_configuration.as_ref()
    {
        if ![accepted, Some(&intent.requested_configuration), current]
            .into_iter()
            .flatten()
            .any(|authorized| authorized == reported)
        {
            return Err(ValidationError::InvalidSwitchoverEvidence);
        }
        if reported == &intent.requested_configuration
            && report
                .previous_configuration
                .as_ref()
                .is_some_and(|previous| Some(previous) != accepted)
        {
            return Err(ValidationError::ReportedPreviousConfigurationMismatch(
                report.identity.replica_id.value(),
            ));
        }
    }
    if let Some(transition) = &snapshot.status.transition
        && transition.kind != TransitionKind::Bootstrap
        && report.epoch == transition.current_configuration.epoch
        && report
            .current_configuration
            .as_ref()
            .is_some_and(|current| {
                current.configuration_id == transition.current_configuration.configuration_id
            })
        && let Some(reported_previous) = report.previous_configuration.as_ref()
    {
        let previous = transition
            .switchover
            .as_ref()
            .filter(|intent| {
                intent.resolution
                    == crate::protocol::types::PlannedSwitchoverResolution::CompensatingOldPrimary
            })
            .map(|intent| &intent.requested_configuration)
            .or(accepted)
            .expect("validated non-bootstrap transition has topology");
        if reported_previous != previous {
            return Err(ValidationError::ReportedPreviousConfigurationMismatch(
                report.identity.replica_id.value(),
            ));
        }
    }

    if !accepted_exact && !current_exact {
        if snapshot
            .status
            .transition
            .as_ref()
            .is_some_and(|transition| transition.kind == TransitionKind::Bootstrap)
        {
            return Err(ValidationError::BootstrapHasUnrelatedAuthority);
        }
        if report.role == ReplicaRole::Primary || report.write_status == AccessStatus::Granted {
            return Err(ValidationError::UnrelatedReplicaClaimsAuthority(
                report.identity.replica_id.value(),
            ));
        }
        return Ok(());
    }

    let highest_authorized = current.or(accepted).expect("known identity has authority");
    if report.epoch > highest_authorized.epoch {
        return Err(ValidationError::UnauthorizedReplicaEpoch {
            replica_id: report.identity.replica_id.value(),
            observed: report.epoch,
            authorized: highest_authorized.epoch,
        });
    }

    if let Some(accepted) = accepted
        && accepted_exact
        && report.epoch < accepted.epoch
    {
        let accepted_member = accepted
            .members
            .iter()
            .find(|member| member.identity == report.identity)
            .expect("accepted exact identity has a member");
        if snapshot.status.transition.is_none() && accepted_member.role != ReplicaRole::Primary {
            return Ok(());
        }
        return Err(ValidationError::StaleReplicaEpoch {
            replica_id: report.identity.replica_id.value(),
            observed: report.epoch,
            accepted: accepted.epoch,
        });
    }

    let requested = snapshot
        .status
        .transition
        .as_ref()
        .and_then(|transition| transition.switchover.as_ref())
        .map(|intent| &intent.requested_configuration);
    for configuration in [accepted, requested, current].into_iter().flatten() {
        if report.epoch == configuration.epoch
            && report
                .current_configuration
                .as_ref()
                .is_some_and(|observed| observed.configuration_id != configuration.configuration_id)
        {
            return Err(ValidationError::ConflictingReplicaConfiguration {
                replica_id: report.identity.replica_id.value(),
                epoch: report.epoch,
            });
        }
    }

    if report.write_status == AccessStatus::Granted {
        let matches_current_authority =
            [current, accepted]
                .into_iter()
                .flatten()
                .any(|configuration| {
                    report.epoch == configuration.epoch
                        && report
                            .current_configuration
                            .as_ref()
                            .is_some_and(|observed| {
                                observed.configuration_id == configuration.configuration_id
                            })
                });
        if !matches_current_authority {
            return Err(ValidationError::ConflictingReplicaConfiguration {
                replica_id: report.identity.replica_id.value(),
                epoch: report.epoch,
            });
        }
    }
    Ok(())
}

fn observation_key_string(key: &ReplicaObservationKey) -> String {
    format!("{}@{}", key.replica_id, key.instance_id)
}

pub fn validate_replacement_cleanup(
    cleanup: &crate::protocol::types::ReplacementCleanup,
) -> Result<(), ValidationError> {
    use crate::protocol::types::{
        CleanupResourceIdentity, PodUid, PvcUid, derive_agent_generation, derive_initialization_id,
        derive_replica_endpoint_name,
    };
    let CleanupResourceIdentity::Present { uid: pod, .. } = &cleanup.resources.pod else {
        return Err(ValidationError::InvalidReplacementCleanup);
    };
    let CleanupResourceIdentity::Present { uid: pvc, .. } = &cleanup.resources.pvc else {
        return Err(ValidationError::InvalidReplacementCleanup);
    };
    if cleanup.target.replica_id.value() <= 0
        || cleanup.resource_uid.is_empty()
        || pod.is_empty()
        || pvc.is_empty()
        || pod != cleanup.target.instance_id.as_str()
        || derive_agent_generation(&derive_initialization_id(
            &cleanup.resource_uid,
            cleanup.target.replica_id,
            &PodUid::new(pod),
            &PvcUid::new(pvc),
        )) != cleanup.target.agent_generation
        || [
            &cleanup.resources.pod,
            &cleanup.resources.pvc,
            &cleanup.resources.endpoint,
        ]
        .iter()
        .any(|r| {
            r.name().is_empty()
                || matches!(r, CleanupResourceIdentity::Present { uid, .. } if uid.is_empty())
        })
        || cleanup.resources.endpoint.name()
            != derive_replica_endpoint_name(&cleanup.resource_uid, &cleanup.target)
    {
        return Err(ValidationError::InvalidReplacementCleanup);
    }
    Ok(())
}

/// Validates durable topology, provisioning, and active transition intent.
pub fn validate_status(status: &AcceptedStatus) -> Result<(), ValidationError> {
    if let Some(allocation) = &status.scale_up_allocation {
        validate_scale_up_allocation(allocation)?;
        let topology = status.topology.as_ref();
        let policy = status.effective_policy.as_ref();
        let expected_target = topology.zip(policy).and_then(|(topology, policy)| {
            policy.replica_set_size.checked_add(1).and_then(|size| {
                (1..=i64::from(size)).map(ReplicaId::new).find(|candidate| {
                    topology
                        .configuration
                        .members
                        .iter()
                        .all(|member| member.identity.replica_id != *candidate)
                })
            })
        });
        if !status.initialized
            || topology.is_none_or(|topology| {
                topology.configuration.configuration_id != allocation.accepted_configuration_id
            })
            || policy.is_none_or(|policy| {
                policy.replica_set_size == u32::MAX
                    || allocation.desired_replicas <= policy.replica_set_size
            })
            || expected_target != Some(allocation.target_replica_id)
            || status.provisioning.is_some()
            || status.transition.as_ref().is_some_and(|transition| {
                transition.kind != TransitionKind::Failover
                    || transition.scale_up.is_some()
                    || transition.scale_up_failover.is_some()
                    || transition.previous_configuration_id.as_ref()
                        != Some(&allocation.accepted_configuration_id)
            })
            || status.scale_up_cleanup.is_some()
            || status.secondary_scale_down_cleanup.is_some()
            || status.pending_replacement_cleanup.is_some()
            || status.last_replacement.is_some()
        {
            return Err(ValidationError::InvalidScaleUp(
                "allocation must bind compatible accepted or failover authority",
            ));
        }
    }
    if let Some(operation_id) = &status.scale_up_admission_started {
        let active = status.transition.as_ref().and_then(|transition| {
            transition.scale_up.as_deref().or_else(|| {
                transition
                    .scale_up_failover
                    .as_deref()
                    .map(|evidence| &evidence.intent)
            })
        });
        if active.is_none_or(|intent| &intent.operation_id != operation_id) {
            return Err(ValidationError::InvalidScaleUp(
                "scale-up admission-start fence differs from active authority",
            ));
        }
    }
    if let Some(cleanup) = &status.pending_replacement_cleanup {
        validate_replacement_cleanup(cleanup)?;
        let topology = status
            .topology
            .as_ref()
            .ok_or(ValidationError::InvalidReplacementCleanup)?;
        let configuration = &topology.configuration;
        if status.last_replacement.is_some()
            || status.secondary_scale_down_cleanup.is_some()
            || status.scale_up_cleanup.is_some()
            || !configuration.members.iter().any(|m| m.identity == cleanup.target)
            || status.provisioning.as_ref().is_some_and(|p| {
                p.replacement() != Some(&cleanup.target)
                    || p.pod_uid.as_str() == cleanup.target.instance_id.as_str()
                    || matches!(&cleanup.resources.pvc, crate::protocol::types::CleanupResourceIdentity::Present { uid, .. } if uid == p.pvc_uid.as_str())
                    || p.operation_id != cleanup.provisioning_operation_id(&p.pod_uid, &p.pvc_uid)
            })
            || status.transition.as_ref().is_some_and(|t| {
                !matches!(t.kind, TransitionKind::Replacement | TransitionKind::Failover)
                    || configuration.members.iter()
                        .filter(|old| !t.current_configuration.members.iter().any(|new| new.identity == old.identity))
                        .any(|old| old.identity != cleanup.target)
                    || (!t.current_configuration.members.iter().any(|m| m.identity == cleanup.target)
                        && t.transition_id != cleanup.transition_id(t.kind, &t.current_configuration.configuration_id))
            })
        {
            return Err(ValidationError::InvalidReplacementCleanup);
        }
    }
    if let Some(cleanup) = &status.last_replacement
        && (validate_replacement_cleanup(cleanup).is_err()
            || status.provisioning.is_some()
            || status.transition.is_some()
            || status.secondary_scale_down_cleanup.is_some()
            || status.scale_up_cleanup.is_some()
            || status.topology.as_ref().is_none_or(|topology| {
                !topology.configuration.members.iter().any(|member| {
                    member.identity.replica_id == cleanup.target.replica_id
                        && member.identity.instance_id != cleanup.target.instance_id
                })
            }))
    {
        return Err(ValidationError::InvalidReplacementCleanup);
    }
    if let Some(cleanup) = status
        .pending_replacement_cleanup
        .as_ref()
        .or(status.last_replacement.as_ref())
        && status
            .last_secondary_removal
            .as_ref()
            .is_some_and(|receipt| {
                let protected = &receipt.evidence.preparation.intent.cleanup;
                [
                    (&cleanup.resources.pod, &protected.pod),
                    (&cleanup.resources.pvc, &protected.pvc),
                    (&cleanup.resources.endpoint, &protected.endpoint),
                ]
                .iter()
                .any(|(a, b)| {
                    a.name() == b.name()
                        || matches!((a, b),
                        (crate::protocol::types::CleanupResourceIdentity::Present { uid: a, .. },
                         crate::protocol::types::CleanupResourceIdentity::Present { uid: b, .. }) if a == b)
                })
            })
    {
        return Err(ValidationError::InvalidReplacementCleanup);
    }
    if let Some(receipt) = &status.last_secondary_removal {
        validate_secondary_scale_down_cleanup(&receipt.committed())?;
        let intent = &receipt.evidence.preparation.intent;
        if status.secondary_scale_down_cleanup.is_some()
            || status.topology.as_ref().is_none_or(|topology| {
                topology.configuration.epoch < intent.current_configuration.epoch
                    || (topology.configuration.epoch == intent.current_configuration.epoch
                        && (topology.configuration != intent.current_configuration
                            || status.effective_policy.as_ref() != Some(&intent.current_policy)))
            })
        {
            return Err(ValidationError::InvalidSecondaryScaleDown(
                "completed removal must bind accepted or superseded authority without cleanup",
            ));
        }
    }
    if let Some(cleanup) = &status.scale_up_cleanup {
        validate_scale_up_cleanup(cleanup)?;
        let scale_up = cleanup
            .provisioning
            .scale_up()
            .ok_or(ValidationError::InvalidScaleUp(
                "cleanup requires scale-up provisioning",
            ))?;
        let accepted_cleanup_authority = status.topology.as_ref().is_some_and(|topology| {
            topology.configuration.epoch >= scale_up.previous_configuration.epoch
                && exact_identities(&topology.configuration)
                    == exact_identities(&scale_up.previous_configuration)
        });
        let transition_allows_failover = status.transition.as_ref().is_none_or(|transition| {
            transition.kind == TransitionKind::Failover
                && transition.scale_up.is_none()
                && transition.scale_up_failover.is_none()
                && !transition
                    .current_configuration
                    .members
                    .iter()
                    .any(|member| {
                        member.identity.replica_id == cleanup.target.replica_id
                            || member.identity == cleanup.target
                    })
        });
        if status.provisioning.is_some()
            || !transition_allows_failover
            || status.secondary_scale_down_cleanup.is_some()
            || status.pending_replacement_cleanup.is_some()
            || status.last_replacement.is_some()
            || !accepted_cleanup_authority
            || status.effective_policy.as_ref() != Some(&scale_up.previous_policy)
        {
            return Err(ValidationError::InvalidScaleUp(
                "cleanup must bind uncommitted accepted authority",
            ));
        }
    }
    if let Some(receipt) = &status.last_scale_up {
        validate_scale_up_receipt(receipt)?;
        if status.topology.as_ref().is_none_or(|topology| {
            topology.configuration.epoch < receipt.accepted_configuration.epoch
                || (topology.configuration.epoch == receipt.accepted_configuration.epoch
                    && (topology.configuration != receipt.accepted_configuration
                        || status.effective_policy.as_ref()
                            != Some(&receipt.intent.current_policy)))
        }) {
            return Err(ValidationError::InvalidScaleUp(
                "completed scale-up must bind accepted or superseded authority",
            ));
        }
    }
    if let Some(cleanup) = &status.secondary_scale_down_cleanup {
        validate_secondary_scale_down_cleanup(cleanup)?;
        let intent = &cleanup.evidence.preparation.intent;
        if status.transition.is_some()
            || status.provisioning.is_some()
            || status.primary_failure.is_some()
            || status.scale_up_cleanup.is_some()
            || status
                .topology
                .as_ref()
                .map(|topology| &topology.configuration)
                != Some(&intent.current_configuration)
            || status.effective_policy.as_ref() != Some(&intent.current_policy)
        {
            return Err(ValidationError::InvalidSecondaryScaleDown(
                "cleanup must exclusively bind accepted reduced authority",
            ));
        }
    }
    if let Some(receipt) = &status.last_switchover {
        validate_switchover_receipt(receipt)?;
    }
    match (status.initialized, status.topology.as_ref()) {
        (true, None) => return Err(ValidationError::InitializedWithoutTopology),
        (false, Some(_)) => return Err(ValidationError::TopologyBeforeInitialization),
        _ => {}
    }
    match (status.initialized, status.effective_policy.as_ref()) {
        (true, None) => return Err(ValidationError::InitializedWithoutPolicy),
        (false, Some(_)) if status.transition.is_none() => {
            return Err(ValidationError::PolicyBeforeInitialization);
        }
        _ => {}
    }
    if let Some(policy) = &status.effective_policy {
        validate_policy(policy)?;
    }
    if status.transition.as_ref().is_some_and(|transition| {
        transition.scale_up.is_some() || transition.scale_up_failover.is_some()
    }) && status.provisioning.is_none()
    {
        return Err(ValidationError::InvalidScaleUp(
            "active scale-up transition requires exact provisioning provenance",
        ));
    }
    if let (Some(provisioning), Some(transition)) =
        (status.provisioning.as_ref(), status.transition.as_ref())
    {
        let intent = transition.scale_up.as_deref().or_else(|| {
            transition
                .scale_up_failover
                .as_deref()
                .map(|evidence| &evidence.intent)
        });
        let retained_scale_up_provenance = intent.is_some_and(|intent| {
            provisioning.scale_up().is_some_and(|frozen| {
                frozen.resource_uid == intent.resource_uid
                    && frozen.spec_generation == intent.spec_generation
                    && frozen.desired_replicas == intent.desired_replicas
                    && frozen.previous_configuration == intent.previous_configuration
                    && frozen.previous_policy == intent.previous_policy
                    && frozen.current_policy == intent.current_policy
                    && provisioning.target_identity(&intent.resource_uid) == intent.target
                    && provisioning
                        .scale_up_build_id(&intent.resource_uid)
                        .as_ref()
                        == Some(&intent.build_id)
            })
        });
        let deferred_cleanup_during_failover = provisioning.scale_up().is_some_and(|frozen| {
            transition.kind == TransitionKind::Failover
                && transition.scale_up.is_none()
                && transition.scale_up_failover.is_none()
                && transition.effective_policy == frozen.previous_policy
                && transition.current_configuration.epoch > frozen.previous_configuration.epoch
                && exact_identities(&transition.current_configuration)
                    == exact_identities(&frozen.previous_configuration)
                && !transition
                    .current_configuration
                    .members
                    .iter()
                    .any(|member| {
                        member.identity == provisioning.target_identity(&frozen.resource_uid)
                    })
        });
        if !retained_scale_up_provenance && !deferred_cleanup_during_failover {
            return Err(ValidationError::ProvisioningAndTransition);
        }
    }
    if status.provisioning.is_some() && (!status.initialized || status.topology.is_none()) {
        return Err(ValidationError::ProvisioningWithoutTopology);
    }
    if let Some(provisioning) = &status.provisioning {
        validate_scale_up_provisioning(provisioning)?;
        if let Some(scale_up) = provisioning.scale_up() {
            let accepted_authority_matches = status.topology.as_ref().is_some_and(|topology| {
                topology.configuration.epoch >= scale_up.previous_configuration.epoch
                    && exact_identities(&topology.configuration)
                        == exact_identities(&scale_up.previous_configuration)
            });
            if !accepted_authority_matches
                || status.effective_policy.as_ref() != Some(&scale_up.previous_policy)
                || status.scale_up_cleanup.is_some()
            {
                return Err(ValidationError::InvalidScaleUp(
                    "provisioning differs from accepted authority",
                ));
            }
        }
    }
    if let Some(topology) = &status.topology {
        validate_configuration(&topology.configuration, status.effective_policy.as_ref())?;
    }
    if let Some(failure) = &status.primary_failure {
        let primary =
            status
                .topology
                .as_ref()
                .and_then(|topology| {
                    topology.configuration.members.iter().find(|member| {
                        member.identity.replica_id == topology.configuration.primary_id
                    })
                })
                .ok_or(ValidationError::PrimaryFailureMismatch)?;
        if failure.primary != primary.identity {
            return Err(ValidationError::PrimaryFailureMismatch);
        }
    }
    if let Some(quorum_loss) = &status.quorum_loss
        && status.topology.as_ref().is_none_or(|topology| {
            topology.configuration.configuration_id != quorum_loss.configuration_id
        })
    {
        return Err(ValidationError::QuorumLossMismatch);
    }
    if let Some(transition) = &status.transition {
        if transition.kind == TransitionKind::ScaleUp {
            let intent = transition
                .scale_up
                .as_ref()
                .ok_or(ValidationError::InvalidScaleUp(
                    "missing frozen scale-up intent",
                ))?;
            validate_scale_up(intent)?;
            if status
                .topology
                .as_ref()
                .map(|topology| &topology.configuration)
                != Some(&intent.previous_configuration)
                || status.effective_policy.as_ref() != Some(&intent.previous_policy)
                || transition.effective_policy != intent.current_policy
                || transition.current_configuration != intent.current_configuration
                || transition.previous_configuration_id.as_ref()
                    != Some(&intent.previous_configuration.configuration_id)
                || transition.spec_generation != intent.spec_generation
                || transition.transition_id
                    != intent.transition_id(TransitionKind::ScaleUp, &intent.current_configuration)
                || transition.build_id.as_ref() != Some(&intent.build_id)
                || transition.scale_up_failover.is_some()
                || transition.switchover.is_some()
                || transition.secondary_scale_down.is_some()
                || transition.secondary_removal_evidence.is_some()
                || transition.repair.is_some()
                || transition.election_lsn.is_some()
                || (status.primary_failure.is_some() && status.scale_up_admission_started.is_some())
            {
                return Err(ValidationError::InvalidScaleUp(
                    "transition differs from immutable intent",
                ));
            }
            return Ok(());
        }
        if transition.kind == TransitionKind::SecondaryScaleDown {
            let intent = transition.secondary_scale_down.as_ref().ok_or(
                ValidationError::InvalidSecondaryScaleDown("missing frozen intent"),
            )?;
            validate_secondary_scale_down(intent)?;
            if status
                .topology
                .as_ref()
                .map(|topology| &topology.configuration)
                != Some(&intent.previous_configuration)
                || status.effective_policy.as_ref() != Some(&intent.previous_policy)
                || transition.effective_policy != intent.current_policy
                || transition.current_configuration != intent.current_configuration
                || transition.previous_configuration_id.as_ref()
                    != Some(&intent.previous_configuration.configuration_id)
                || transition.spec_generation != intent.spec_generation
                || transition.transition_id
                    != crate::protocol::types::derive_transition_id(
                        &intent.resource_uid,
                        transition.kind,
                        &intent.current_configuration.configuration_id,
                    )
                || transition.switchover.is_some()
                || transition.build_id.is_some()
                || transition.repair.is_some()
                || transition.election_lsn.is_some()
                || transition.scale_up.is_some()
                || transition.scale_up_failover.is_some()
                || status.primary_failure.is_some()
            {
                return Err(ValidationError::InvalidSecondaryScaleDown(
                    "transition differs from immutable intent",
                ));
            }
            if let Some(evidence) = &transition.secondary_removal_evidence {
                validate_secondary_removal_evidence(evidence, false)?;
                if evidence.preparation.intent != *intent {
                    return Err(ValidationError::InvalidSecondaryScaleDown(
                        "evidence differs from immutable intent",
                    ));
                }
            }
            return Ok(());
        }
        if transition.kind == TransitionKind::Failover
            && let Some(evidence) = &transition.scale_up_failover
        {
            validate_scale_up_failover_transition(
                evidence,
                &transition.current_configuration,
                &transition.effective_policy,
            )?;
            let intent = &evidence.intent;
            if status
                .topology
                .as_ref()
                .map(|topology| &topology.configuration)
                != Some(&intent.previous_configuration)
                || status.effective_policy.as_ref() != Some(&intent.previous_policy)
                || transition.previous_configuration_id.as_ref()
                    != Some(&intent.previous_configuration.configuration_id)
                || transition.spec_generation != intent.spec_generation
                || transition.transition_id
                    != intent
                        .transition_id(TransitionKind::Failover, &transition.current_configuration)
                || transition.build_id.as_ref() != Some(&intent.build_id)
                || transition.scale_up.is_some()
                || transition.switchover.is_some()
                || transition.secondary_scale_down.is_some()
                || transition.secondary_removal_evidence.is_some()
                || transition.election_lsn.is_some_and(|lsn| lsn < 0)
                || match (transition.election_lsn, evidence.final_election.as_deref()) {
                    (None, None) => {
                        transition.current_configuration != evidence.provisional_configuration
                    }
                    (Some(election_lsn), Some(final_election)) => {
                        final_election
                            .final_configuration(&evidence.provisional_configuration)
                            .as_ref()
                            != Some(&transition.current_configuration)
                            || Some(election_lsn) != final_election.safe_lsn()
                    }
                    _ => true,
                }
            {
                return Err(ValidationError::InvalidScaleUp(
                    "failover transition differs from carried scale-up authority",
                ));
            }
            return Ok(());
        }
        if transition.secondary_scale_down.is_some()
            || transition.secondary_removal_evidence.is_some()
        {
            return Err(ValidationError::InvalidSecondaryScaleDown(
                "unexpected removal authority",
            ));
        }
        if transition.scale_up.is_some() || transition.scale_up_failover.is_some() {
            return Err(ValidationError::InvalidScaleUp(
                "unexpected scale-up authority",
            ));
        }
        if status
            .effective_policy
            .as_ref()
            .is_some_and(|policy| policy != &transition.effective_policy)
            || (transition.kind != TransitionKind::Bootstrap && status.effective_policy.is_none())
        {
            return Err(ValidationError::TransitionPolicyMismatch);
        }
        validate_policy(&transition.effective_policy)?;
        validate_configuration(
            &transition.current_configuration,
            Some(&transition.effective_policy),
        )?;
        match transition.kind {
            TransitionKind::ScaleUp => unreachable!("validated independently above"),
            TransitionKind::SecondaryScaleDown => unreachable!("validated independently above"),
            TransitionKind::Bootstrap => {
                if transition.switchover.is_some() {
                    return Err(ValidationError::UnexpectedSwitchoverIntent);
                }
                if transition.previous_configuration_id.is_some() {
                    return Err(ValidationError::BootstrapHasPreviousConfiguration);
                }
                if transition.build_id.is_some() {
                    return Err(ValidationError::InvalidReplacementMembership);
                }
                if transition.repair.is_some() {
                    return Err(ValidationError::InvalidFailoverRepairTarget);
                }
                if transition.election_lsn.is_some() {
                    return Err(ValidationError::InvalidFailoverElectionLsn);
                }
                if status.topology.is_some() {
                    return Err(ValidationError::BootstrapHasTopology);
                }
            }
            TransitionKind::Replacement | TransitionKind::Failover => {
                if transition.switchover.is_some() {
                    return Err(ValidationError::UnexpectedSwitchoverIntent);
                }
                let topology = status
                    .topology
                    .as_ref()
                    .ok_or(ValidationError::TransitionWithoutTopology)?;
                if transition.previous_configuration_id.as_ref()
                    != Some(&topology.configuration.configuration_id)
                {
                    return Err(ValidationError::PreviousConfigurationMismatch {
                        actual: transition
                            .previous_configuration_id
                            .as_ref()
                            .map(ToString::to_string),
                        expected: topology.configuration.configuration_id.to_string(),
                    });
                }
                validate_configuration(
                    &topology.configuration,
                    Some(&transition.effective_policy),
                )?;
                validate_transition_relationship(
                    transition.kind,
                    Some(&topology.configuration),
                    &transition.current_configuration,
                    &transition.effective_policy,
                )?;
                if transition.kind == TransitionKind::Replacement && transition.build_id.is_none() {
                    return Err(ValidationError::InvalidReplacementMembership);
                }
                if transition.kind == TransitionKind::Replacement && transition.repair.is_some() {
                    return Err(ValidationError::InvalidFailoverRepairTarget);
                }
                if transition.kind == TransitionKind::Replacement
                    && transition.election_lsn.is_some()
                {
                    return Err(ValidationError::InvalidFailoverElectionLsn);
                }
                if transition.kind == TransitionKind::Failover {
                    if transition.election_lsn.is_none_or(|lsn| lsn < 0) {
                        return Err(ValidationError::InvalidFailoverElectionLsn);
                    }
                    let exact_membership_changed = exact_identities(&topology.configuration)
                        != exact_identities(&transition.current_configuration);
                    if exact_membership_changed && transition.build_id.is_none() {
                        return Err(ValidationError::FailoverReplacementWithoutBuild);
                    }
                    if let Some(repair) = &transition.repair {
                        let valid_target =
                            transition
                                .current_configuration
                                .members
                                .iter()
                                .any(|member| {
                                    member.identity == repair.target
                                        && member.role != ReplicaRole::Primary
                                });
                        if !valid_target {
                            return Err(ValidationError::InvalidFailoverRepairTarget);
                        }
                    }
                }
            }
            TransitionKind::PlannedSwitchover => {
                let topology = status
                    .topology
                    .as_ref()
                    .ok_or(ValidationError::TransitionWithoutTopology)?;
                if transition.previous_configuration_id.as_ref()
                    != Some(&topology.configuration.configuration_id)
                {
                    return Err(ValidationError::PreviousConfigurationMismatch {
                        actual: transition
                            .previous_configuration_id
                            .as_ref()
                            .map(ToString::to_string),
                        expected: topology.configuration.configuration_id.to_string(),
                    });
                }
                if transition.election_lsn.is_some()
                    || transition.build_id.is_some()
                    || transition.repair.is_some()
                {
                    return Err(ValidationError::InvalidSwitchoverEvidence);
                }
                let switchover = transition
                    .switchover
                    .as_ref()
                    .ok_or(ValidationError::MissingSwitchoverIntent)?;
                if switchover.request_id.is_empty()
                    || switchover.preparation_generation == 0
                    || switchover.preparation_generation != transition.spec_generation
                {
                    return Err(ValidationError::MissingSwitchoverIntent);
                }
                if status.last_switchover.as_ref().is_some_and(|receipt| {
                    receipt.request_id == switchover.request_id
                        && !(receipt.outcome
                            == crate::protocol::types::PlannedSwitchoverOutcome::Unsafe
                            && switchover.resolution
                                == crate::protocol::types::PlannedSwitchoverResolution::Unsafe)
                }) {
                    return Err(ValidationError::InvalidSwitchoverReceipt);
                }
                let accepted_primary = topology
                    .configuration
                    .members
                    .iter()
                    .find(|member| member.identity.replica_id == topology.configuration.primary_id)
                    .expect("validated topology has one primary");
                if switchover.source != accepted_primary.identity {
                    return Err(ValidationError::InvalidSwitchoverSource);
                }
                if switchover.target.replica_id == topology.configuration.primary_id
                    || !topology
                        .configuration
                        .members
                        .iter()
                        .any(|member| member.identity == switchover.target)
                {
                    return Err(ValidationError::InvalidSwitchoverTarget);
                }
                validate_transition_relationship(
                    transition.kind,
                    Some(&topology.configuration),
                    &switchover.requested_configuration,
                    &transition.effective_policy,
                )?;
                if switchover.requested_configuration.primary_id != switchover.target.replica_id {
                    return Err(ValidationError::InvalidSwitchoverResolution);
                }
                match switchover.resolution {
                    crate::protocol::types::PlannedSwitchoverResolution::RequestedTarget
                    | crate::protocol::types::PlannedSwitchoverResolution::RestoringOldPrimary => {
                        if transition.current_configuration != switchover.requested_configuration {
                            return Err(ValidationError::InvalidSwitchoverResolution);
                        }
                    }
                    crate::protocol::types::PlannedSwitchoverResolution::CompensatingOldPrimary => {
                        validate_transition_relationship(
                            transition.kind,
                            Some(&switchover.requested_configuration),
                            &transition.current_configuration,
                            &transition.effective_policy,
                        )?;
                        if transition.current_configuration.primary_id
                            != switchover.source.replica_id
                            || switchover.handoff.is_none()
                        {
                            return Err(ValidationError::InvalidSwitchoverResolution);
                        }
                    }
                    crate::protocol::types::PlannedSwitchoverResolution::Unsafe => {
                        if transition.current_configuration != switchover.requested_configuration {
                            validate_transition_relationship(
                                transition.kind,
                                Some(&switchover.requested_configuration),
                                &transition.current_configuration,
                                &transition.effective_policy,
                            )?;
                            if transition.current_configuration.primary_id
                                != switchover.source.replica_id
                            {
                                return Err(ValidationError::InvalidSwitchoverResolution);
                            }
                        }
                    }
                }
                if let Some(handoff) = &switchover.handoff {
                    validate_switchover_handoff(handoff)?;
                    if handoff.preparation_generation != switchover.preparation_generation
                        || handoff.request_id != switchover.request_id
                        || handoff.source != switchover.source
                        || handoff.target != switchover.target
                        || handoff.starting_configuration_id
                            != topology.configuration.configuration_id
                        || handoff.starting_epoch != topology.configuration.epoch
                    {
                        return Err(ValidationError::InvalidSwitchoverHandoff);
                    }
                }
            }
        }
    }
    Ok(())
}

/// Validates the relationship between PC, CC, epoch, membership, and policy.
pub fn validate_transition_relationship(
    kind: TransitionKind,
    previous: Option<&ConfigurationDescriptor>,
    current: &ConfigurationDescriptor,
    policy: &EffectivePolicy,
) -> Result<(), ValidationError> {
    if kind == TransitionKind::SecondaryScaleDown {
        return Err(ValidationError::InvalidSecondaryScaleDown(
            "requires explicit dual-policy removal intent",
        ));
    }
    if kind == TransitionKind::ScaleUp {
        return Err(ValidationError::InvalidScaleUp(
            "requires explicit dual-policy scale-up intent",
        ));
    }
    validate_policy(policy)?;
    validate_configuration(current, Some(policy))?;
    if kind == TransitionKind::Bootstrap {
        if previous.is_some() {
            return Err(ValidationError::BootstrapHasPreviousConfiguration);
        }
        return Ok(());
    }

    let previous = previous.ok_or(ValidationError::TransitionWithoutTopology)?;
    validate_configuration(previous, Some(policy))?;
    if current.epoch.data_loss_number != previous.epoch.data_loss_number {
        return Err(ValidationError::TransitionDataLossChanged);
    }
    if current.epoch.configuration_number <= previous.epoch.configuration_number {
        return Err(ValidationError::TransitionEpochNotNewer);
    }

    let previous_ids = previous
        .members
        .iter()
        .map(|member| member.identity.replica_id)
        .collect::<BTreeSet<_>>();
    let current_ids = current
        .members
        .iter()
        .map(|member| member.identity.replica_id)
        .collect::<BTreeSet<_>>();
    if previous_ids != current_ids {
        return Err(ValidationError::TransitionLogicalMembershipChanged);
    }

    match kind {
        TransitionKind::ScaleUp => unreachable!("requires typed authority above"),
        TransitionKind::SecondaryScaleDown => unreachable!("requires typed authority above"),
        TransitionKind::Bootstrap => unreachable!("bootstrap returned above"),
        TransitionKind::Failover => {
            let previous_identities = previous
                .members
                .iter()
                .map(|member| member.identity.clone())
                .collect::<BTreeSet<_>>();
            let current_identities = current
                .members
                .iter()
                .map(|member| member.identity.clone())
                .collect::<BTreeSet<_>>();
            if previous_identities != current_identities
                && !valid_single_non_primary_incarnation_change(previous, current)
            {
                return Err(ValidationError::FailoverMembershipChanged);
            }
        }

        TransitionKind::Replacement => {
            if current.primary_id != previous.primary_id {
                return Err(ValidationError::ReplacementPrimaryChanged);
            }
            let changed = previous
                .members
                .iter()
                .filter(|previous_member| {
                    current
                        .members
                        .iter()
                        .find(|current_member| {
                            current_member.identity.replica_id
                                == previous_member.identity.replica_id
                        })
                        .is_none_or(|current_member| {
                            current_member.identity != previous_member.identity
                        })
                })
                .collect::<Vec<_>>();
            if changed.len() != 1 || changed[0].identity.replica_id == previous.primary_id {
                return Err(ValidationError::InvalidReplacementMembership);
            }
        }
        TransitionKind::PlannedSwitchover => {
            let previous_identities = exact_identities(previous);
            let current_identities = exact_identities(current);
            if previous_identities != current_identities {
                return Err(ValidationError::FailoverMembershipChanged);
            }
        }
    }
    Ok(())
}

fn validate_switchover_handoff(
    handoff: &crate::protocol::types::SwitchoverHandoff,
) -> Result<(), ValidationError> {
    if handoff.preparation_generation == 0
        || handoff.preparation_operation_id.is_empty()
        || handoff.request_id.is_empty()
        || handoff.source == handoff.target
        || !valid_exact_identity(&handoff.source)
        || !valid_exact_identity(&handoff.target)
        || handoff.starting_configuration_id.is_empty()
        || handoff.starting_epoch.data_loss_number < 0
        || handoff.starting_epoch.configuration_number < 0
        || handoff.handoff_lsn < 0
    {
        return Err(ValidationError::InvalidSwitchoverHandoff);
    }
    Ok(())
}

fn validate_switchover_receipt(
    receipt: &crate::protocol::types::PlannedSwitchoverReceipt,
) -> Result<(), ValidationError> {
    if receipt.request_id.is_empty() || receipt.requested_target_replica_id.value() <= 0 {
        return Err(ValidationError::InvalidSwitchoverReceipt);
    }
    if receipt.accepted_target.as_ref().is_some_and(|target| {
        !valid_exact_identity(target) || target.replica_id != receipt.requested_target_replica_id
    }) || receipt
        .resulting_primary
        .as_ref()
        .is_some_and(|identity| !valid_exact_identity(identity))
    {
        return Err(ValidationError::InvalidSwitchoverReceipt);
    }
    match receipt.outcome {
        crate::protocol::types::PlannedSwitchoverOutcome::RequestedTargetCompleted => {
            if receipt.accepted_target.is_none()
                || receipt.resulting_primary.as_ref() != receipt.accepted_target.as_ref()
            {
                return Err(ValidationError::InvalidSwitchoverReceipt);
            }
        }
        crate::protocol::types::PlannedSwitchoverOutcome::OldPrimaryRestored
        | crate::protocol::types::PlannedSwitchoverOutcome::OldPrimaryCompensated => {
            if receipt
                .resulting_primary
                .as_ref()
                .is_none_or(|primary| primary.replica_id == receipt.requested_target_replica_id)
            {
                return Err(ValidationError::InvalidSwitchoverReceipt);
            }
        }
        crate::protocol::types::PlannedSwitchoverOutcome::Rejected => {}
        crate::protocol::types::PlannedSwitchoverOutcome::Unsafe => {
            if receipt.resulting_primary.is_some() {
                return Err(ValidationError::InvalidSwitchoverReceipt);
            }
        }
    }
    Ok(())
}

fn valid_exact_identity(identity: &ReplicaIdentity) -> bool {
    identity.replica_id.value() > 0
        && !identity.instance_id.is_empty()
        && !identity.agent_generation.is_empty()
}

fn exact_identities(configuration: &ConfigurationDescriptor) -> BTreeSet<ReplicaIdentity> {
    configuration
        .members
        .iter()
        .map(|member| member.identity.clone())
        .collect()
}

fn valid_single_non_primary_incarnation_change(
    previous: &ConfigurationDescriptor,
    current: &ConfigurationDescriptor,
) -> bool {
    let changed = previous
        .members
        .iter()
        .filter(|previous_member| {
            current
                .members
                .iter()
                .find(|current_member| {
                    current_member.identity.replica_id == previous_member.identity.replica_id
                })
                .is_none_or(|current_member| current_member.identity != previous_member.identity)
        })
        .collect::<Vec<_>>();
    changed.len() == 1 && changed[0].identity.replica_id != previous.primary_id
}

/// Validates one canonical configuration independently.
pub fn validate_configuration(
    configuration: &ConfigurationDescriptor,
    policy: Option<&EffectivePolicy>,
) -> Result<(), ValidationError> {
    if configuration.epoch.data_loss_number < 0 || configuration.epoch.configuration_number < 0 {
        return Err(ValidationError::InvalidConfigurationEpoch);
    }
    if configuration.members.is_empty() {
        return Err(ValidationError::EmptyConfiguration);
    }
    let expected_id = configuration.expected_id();
    if configuration.configuration_id != expected_id {
        return Err(ValidationError::ConfigurationIdMismatch {
            actual: configuration.configuration_id.to_string(),
            expected: expected_id.to_string(),
        });
    }

    let duplicate_ids = duplicate_replica_ids(&configuration.members);
    if !duplicate_ids.is_empty() {
        return Err(ValidationError::DuplicateReplicaIds(
            duplicate_ids.into_iter().map(ReplicaId::value).collect(),
        ));
    }
    if let Some(invalid) = configuration
        .members
        .iter()
        .map(|member| member.identity.replica_id)
        .find(|replica_id| replica_id.value() <= 0)
    {
        return Err(ValidationError::InvalidReplicaId(invalid.value()));
    }

    let mut identities = BTreeSet::new();
    for member in &configuration.members {
        let key = (
            member.identity.instance_id.clone(),
            member.identity.agent_generation.clone(),
        );
        if !identities.insert(key) {
            return Err(ValidationError::DuplicateReplicaIdentity(format!(
                "{}@{}",
                member.identity.instance_id, member.identity.agent_generation
            )));
        }
    }

    let primaries = configuration
        .members
        .iter()
        .filter(|member| member.role == ReplicaRole::Primary)
        .collect::<Vec<_>>();
    if primaries.len() != 1 {
        return Err(ValidationError::InvalidPrimaryCount(primaries.len()));
    }
    if primaries[0].identity.replica_id != configuration.primary_id {
        return Err(ValidationError::MissingPrimary(
            configuration.primary_id.value(),
        ));
    }

    let expected_policy = policy
        .cloned()
        .or_else(|| EffectivePolicy::fixed(configuration.members.len() as u32, 0))
        .expect("empty configurations returned before deriving policy");
    let actual_size = configuration.members.len() as u32;
    if actual_size != expected_policy.replica_set_size {
        return Err(ValidationError::ReplicaSetSizeMismatch {
            actual: actual_size,
            expected: expected_policy.replica_set_size,
        });
    }
    if configuration.write_quorum != expected_policy.write_quorum {
        return Err(ValidationError::WriteQuorumMismatch {
            actual: configuration.write_quorum,
            expected: expected_policy.write_quorum,
        });
    }
    Ok(())
}

pub(crate) fn validate_policy(policy: &EffectivePolicy) -> Result<(), ValidationError> {
    let expected = EffectivePolicy::fixed(policy.replica_set_size, policy.failover_delay_seconds)
        .ok_or(ValidationError::InvalidEffectivePolicy(
        policy.replica_set_size,
    ))?;
    if policy.write_quorum != expected.write_quorum || policy.read_quorum != expected.read_quorum {
        return Err(ValidationError::InvalidEffectivePolicy(
            policy.replica_set_size,
        ));
    }
    Ok(())
}
