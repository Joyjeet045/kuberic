//! Fail-closed validation for accepted authority and observed replica state.

use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;

use crate::observation::{AgentObservation, ObservationSnapshot, ReplicaObservationKey};
use crate::types::{
    AcceptedStatus, AccessStatus, ConfigurationDescriptor, EffectivePolicy, Epoch, ReplicaId,
    ReplicaIdentity, ReplicaRole, TransitionKind, duplicate_replica_ids,
};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ValidationError {
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
        observed: crate::types::Epoch,
        accepted: crate::types::Epoch,
    },
    #[error("replica {replica_id} contradicts accepted configuration at epoch {epoch:?}")]
    ConflictingReplicaConfiguration {
        replica_id: i64,
        epoch: crate::types::Epoch,
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
}

/// Validates accepted status and every observed exact replica incarnation.
pub fn validate_snapshot(snapshot: &ObservationSnapshot) -> Result<(), ValidationError> {
    if snapshot.desired.replicas == 0 {
        return Err(ValidationError::DesiredReplicasZero);
    }
    validate_status(&snapshot.status)?;

    if let Some(provisioning) = &snapshot.status.provisioning {
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
        if provisioning.replaces.replica_id == topology.configuration.primary_id
            || !topology
                .configuration
                .members
                .iter()
                .any(|member| member.identity == provisioning.replaces)
        {
            return Err(ValidationError::InvalidProvisioningReplacement);
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

fn validate_report_internal(
    report: &crate::observation::AgentReport,
) -> Result<(), ValidationError> {
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
            || previous_ids != current_ids
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
    session_id: &crate::types::ProcessSessionId,
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
    report: &crate::observation::AgentReport,
) -> Result<(), ValidationError> {
    let provisioning =
        snapshot.status.provisioning.as_ref().is_some_and(|intent| {
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
        let accepted = accepted.expect("validated non-bootstrap transition has topology");
        if reported_previous != accepted {
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

    for configuration in [accepted, current].into_iter().flatten() {
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

/// Validates durable topology, provisioning, and active transition intent.
pub fn validate_status(status: &AcceptedStatus) -> Result<(), ValidationError> {
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
    if status.provisioning.is_some() && status.transition.is_some() {
        return Err(ValidationError::ProvisioningAndTransition);
    }
    if status.provisioning.is_some() && (!status.initialized || status.topology.is_none()) {
        return Err(ValidationError::ProvisioningWithoutTopology);
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
            TransitionKind::Bootstrap => {
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
    }
    Ok(())
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

fn validate_policy(policy: &EffectivePolicy) -> Result<(), ValidationError> {
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
