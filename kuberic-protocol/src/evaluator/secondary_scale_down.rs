use super::*;
use crate::command::{
    AcceptSecondaryRemovalCommit, PrepareSecondaryRemoval, RetireReplica, ScaleDownResource,
};
use crate::observation::{
    AgentReport, ExactResourceObservation, SecondaryScaleDownResourceObservation,
};
use crate::types::{
    CleanupResourceIdentity, SecondaryRemovalEvidence, SecondaryRemovalReceipt,
    SecondaryRemovalStage, SecondaryRemovalWitness, SecondaryScaleDownCleanup,
    SecondaryScaleDownIntent,
};
use crate::validation::{
    validate_secondary_removal_evidence, validate_secondary_scale_down,
    validate_secondary_scale_down_cleanup,
};

pub(super) fn active(snapshot: &ObservationSnapshot) -> bool {
    snapshot.status.secondary_scale_down_cleanup.is_some()
        || snapshot
            .status
            .last_secondary_removal
            .as_ref()
            .is_some_and(|receipt| {
                snapshot.status.topology.as_ref().is_some_and(|topology| {
                    topology.configuration
                        == receipt.evidence.preparation.intent.current_configuration
                })
            })
        || snapshot
            .status
            .transition
            .as_ref()
            .is_some_and(|t| t.kind == TransitionKind::SecondaryScaleDown)
}

pub(super) fn wait(
    status: AcceptedStatus,
    reason: &str,
    message: &str,
    config: &EvaluationConfig,
) -> Plan {
    Plan::Wait {
        reason: WaitReason::ActiveTransition,
        status: waiting_status(status, reason, message),
        requeue_after_seconds: config.wait_requeue_seconds,
    }
}

fn persist(status: AcceptedStatus) -> Plan {
    Plan::Apply {
        changes: vec![KubernetesChange::PersistStatus {
            status: Box::new(status),
        }],
    }
}

fn report<'a>(
    snapshot: &'a ObservationSnapshot,
    identity: &ReplicaIdentity,
) -> Option<&'a AgentReport> {
    match &snapshot.observation_for_identity(identity)?.agent {
        AgentObservation::Report(report)
            if report.identity == *identity
                && report.healthy
                && report.reported_fault != Some(crate::types::FaultType::Permanent)
                && !report.process_session_id.is_empty()
                && report.report_sequence > 0 =>
        {
            Some(report)
        }
        _ => None,
    }
}

pub(super) fn resources<'a>(
    snapshot: &'a ObservationSnapshot,
    target: &ReplicaIdentity,
) -> Option<&'a SecondaryScaleDownResourceObservation> {
    let mut observations = snapshot
        .secondary_scale_down_resources
        .iter()
        .filter(|r| r.resource_uid == snapshot.resource_uid && r.target == *target);
    let result = observations.next()?;
    observations.next().is_none().then_some(result)
}

pub(super) fn absent(
    identity: &CleanupResourceIdentity,
    observation: &ExactResourceObservation,
) -> bool {
    match (identity, observation) {
        (_, ExactResourceObservation::NotFound) => true,
        (
            CleanupResourceIdentity::Present { uid, .. },
            ExactResourceObservation::ReplacementPresent {
                uid: replacement,
                resource_version,
            },
        ) => !replacement.is_empty() && replacement != uid && !resource_version.is_empty(),
        // A resource absent at admission grants no authority over a later occupant.
        (
            CleanupResourceIdentity::Absent { .. },
            ExactResourceObservation::ReplacementPresent {
                uid,
                resource_version,
            },
        ) => !uid.is_empty() && !resource_version.is_empty(),
        _ => false,
    }
}

pub(super) fn observed(
    identity: &CleanupResourceIdentity,
    observation: &ExactResourceObservation,
) -> bool {
    absent(identity, observation)
        || matches!((identity, observation),
            (CleanupResourceIdentity::Present { .. }, ExactResourceObservation::FrozenUidPresent { resource_version })
            if !resource_version.is_empty())
}

fn drift_status(snapshot: &ObservationSnapshot, mut status: AcceptedStatus) -> AcceptedStatus {
    let topology = &status.topology.as_ref().expect("initialized").configuration;
    let policy = status.effective_policy.as_ref().expect("initialized");
    let (_, drift) = desired_spec_state(snapshot, topology, policy);
    let accepted_count = topology.members.len() as u32;
    status = status.without_condition("UnsupportedSpec");
    if let Some(mut condition) = drift {
        if condition.reason == "ReplicaCountImmutable" && snapshot.desired.replicas > 0 {
            if snapshot.desired.replicas < accepted_count {
                return status;
            }
            condition.reason = "ScaleUpUnsupported".into();
        }
        status = status.with_condition(condition);
    }
    status
}

pub(super) fn begin(
    snapshot: &ObservationSnapshot,
    status: AcceptedStatus,
    config: &EvaluationConfig,
) -> Option<Plan> {
    let previous = &status.topology.as_ref()?.configuration;
    let policy = status.effective_policy.as_ref()?;
    if snapshot.desired.replicas >= policy.replica_set_size {
        return None;
    }
    if let Some(receipt) = &status.last_secondary_removal
        && receipt.evidence.preparation.intent.current_configuration == *previous
        && previous.members.iter().any(|member| {
            !receipt
                .current_only_write_quorum
                .iter()
                .any(|w| w.identity == member.identity)
                && report(snapshot, &member.identity).is_none_or(|r| {
                    !stable_member_report(r, member, previous)
                        || r.prepared_secondary_removal.is_some()
                        || r.accepted_secondary_removal.as_ref() != Some(&receipt.committed())
                })
        })
    {
        return Some(wait(
            status,
            "ScaleDownRetainedMemberPending",
            "Settle late retained members before superseding their last removal proof",
            config,
        ));
    }
    let status = drift_status(snapshot, status.clone());
    if status
        .conditions
        .iter()
        .any(|c| c.type_ == "UnsupportedSpec")
    {
        return Some(wait(
            status,
            "ScaleDownUnsupportedDrift",
            "Only replica-count reduction is supported; image and delay remain unchanged",
            config,
        ));
    }
    let primary = configuration_primary(previous);
    let target = previous
        .members
        .iter()
        .filter(|m| m.role == ReplicaRole::ActiveSecondary)
        .max_by_key(|m| m.identity.replica_id)?;
    // Snapshot validation fences report identity and session freshness. This
    // availability preflight is not the durable post-preparation quorum proof.
    let retained_read_quorum = previous
        .members
        .iter()
        .filter(|member| member.identity != target.identity)
        .filter(|member| {
            report(snapshot, &member.identity).is_some_and(|r| {
                stable_member_report(r, member, previous)
                    && r.prepared_switchover.is_none()
                    && r.prepared_secondary_removal.is_none()
            })
        })
        .count();
    if retained_read_quorum < policy.read_quorum as usize {
        return Some(wait(
            status,
            "ScaleDownRetainedReadQuorumUnavailable",
            "Retained exact members must provide the accepted read quorum before freezing removal; existing service is unchanged",
            config,
        ));
    }
    let Some(exact) = resources(snapshot, &target.identity).filter(|r| {
        observed(&r.identity.pod, &r.pod)
            && observed(&r.identity.pvc, &r.pvc)
            && observed(&r.identity.endpoint, &r.endpoint)
    }) else {
        return Some(wait(
            status,
            "ScaleDownExactIdentityRequired",
            "Authoritative exact Pod, mounted PVC, and endpoint identities are required",
            config,
        ));
    };
    let conflicting_mapping = snapshot.observation_for_identity(&target.identity)
        .and_then(|r| r.kubernetes.as_ref()).is_some_and(|k| {
            matches!(&exact.identity.pod, CleanupResourceIdentity::Present { name, uid }
                if k.pod_uid.as_ref().is_some_and(|observed| observed.as_str() != uid || k.pod_name != *name))
                || matches!(&exact.identity.pvc, CleanupResourceIdentity::Present { name, uid }
                    if k.pvc_uid.as_ref().is_some_and(|observed| observed.as_str() != uid || k.pvc_name != *name))
        })
        || previous.members.iter().filter(|m| m.identity != target.identity).any(|member| {
            snapshot.observation_for_identity(&member.identity)
                .and_then(|r| r.kubernetes.as_ref())
                .and_then(|k| k.pvc_uid.as_ref())
                .is_some_and(|observed| matches!(&exact.identity.pvc,
                    CleanupResourceIdentity::Present { uid, .. } if observed.as_str() == uid))
        });
    if conflicting_mapping {
        return Some(unsafe_plan(
            status,
            UnsafeReason::ContradictoryReplicaEvidence(
                "Scale-down exact Pod/PVC mapping conflicts with observed mounted storage".into(),
            ),
            config,
        ));
    }
    if previous.members.iter().any(|member| {
        snapshot
            .observation_for_identity(&member.identity)
            .and_then(|r| r.kubernetes.as_ref())
            .and_then(|k| k.image.as_ref())
            .is_none()
            && !(member.identity == target.identity && absent(&exact.identity.pod, &exact.pod))
    }) {
        return Some(wait(
            status,
            "ScaleDownSpecObservationPending",
            "Observe accepted-member images before admitting count-only drift",
            config,
        ));
    }
    if report(snapshot, &primary.identity)
        .is_none_or(|r| !stable_member_report(r, primary, previous))
        || previous.members.iter().any(|member| {
            report(snapshot, &member.identity).is_some_and(|r| {
                !stable_member_report(r, member, previous)
                    || r.prepared_switchover.is_some()
                    || r.prepared_secondary_removal.is_some()
            })
        })
    {
        return Some(wait(
            status,
            "ScaleDownAwaitingStableAuthority",
            "Existing exact authority and local work must finish before admission",
            config,
        ));
    }
    let Some(number) = previous.epoch.configuration_number.checked_add(1) else {
        return Some(unsafe_plan(
            status,
            UnsafeReason::InvalidAcceptedAuthority(
                "Scale-down configuration epoch exhausted".into(),
            ),
            config,
        ));
    };
    let current_policy =
        EffectivePolicy::fixed(policy.replica_set_size - 1, policy.failover_delay_seconds)
            .expect("positive reduced policy");
    let current = ConfigurationDescriptor::new(
        Epoch::new(previous.epoch.data_loss_number, number),
        previous.primary_id,
        previous
            .members
            .iter()
            .filter(|m| m.identity != target.identity)
            .cloned()
            .collect(),
        current_policy.write_quorum,
    );
    let mut intent = SecondaryScaleDownIntent {
        operation_id: OperationId::default(),
        resource_uid: snapshot.resource_uid.clone(),
        spec_generation: snapshot.desired.generation,
        desired_replicas: snapshot.desired.replicas,
        previous_configuration: previous.clone(),
        current_configuration: current.clone(),
        previous_policy: policy.clone(),
        current_policy: current_policy.clone(),
        primary: primary.identity.clone(),
        target: target.identity.clone(),
        cleanup: exact.identity.clone(),
    };
    intent.operation_id = intent.expected_operation_id();
    if let Err(error) = validate_secondary_scale_down(&intent) {
        return Some(unsafe_plan(
            status,
            UnsafeReason::InvalidAcceptedAuthority(error.to_string()),
            config,
        ));
    }
    let mut status = waiting_status(
        status,
        "ScaleDownPreparationPending",
        "Frozen exact highest-ID secondary removal; primary preparation is required",
    );
    status.quorum_loss = None;
    status.transition = Some(TransitionIntent {
        transition_id: derive_transition_id(
            &snapshot.resource_uid,
            TransitionKind::SecondaryScaleDown,
            &current.configuration_id,
        ),
        kind: TransitionKind::SecondaryScaleDown,
        spec_generation: intent.spec_generation,
        effective_policy: current_policy,
        previous_configuration_id: Some(previous.configuration_id.clone()),
        current_configuration: current,
        secondary_scale_down: Some(intent),
        secondary_removal_evidence: None,
        scale_up: None,
        scale_up_failover: None,
        election_lsn: None,
        build_id: None,
        repair: None,
        switchover: None,
    });
    Some(persist(status))
}

fn witness(report: &AgentReport) -> Option<SecondaryRemovalWitness> {
    Some(SecondaryRemovalWitness {
        resource_uid: report.resource_uid.clone(),
        identity: report.identity.clone(),
        role: report.role,
        process_session_id: report.process_session_id.clone(),
        report_sequence: report.report_sequence,
        epoch: report.epoch,
        previous_configuration_id: report
            .previous_configuration
            .as_ref()
            .map(|c| c.configuration_id.clone()),
        current_configuration_id: report
            .current_configuration
            .as_ref()?
            .configuration_id
            .clone(),
        verified_replication_lsn: report.verified_replication_lsn?,
        write_status: report.write_status,
        pending_operation_id: report.pending_operation_id.clone(),
        retained_operation_id: report.retained_operation_id.clone(),
    })
}

fn witnesses(
    snapshot: &ObservationSnapshot,
    evidence: &SecondaryRemovalEvidence,
    stage: SecondaryRemovalStage,
) -> Vec<SecondaryRemovalWitness> {
    let intent = &evidence.preparation.intent;
    intent
        .current_configuration
        .members
        .iter()
        .filter_map(|member| {
            let r = report(snapshot, &member.identity)?;
            let old = stage == SecondaryRemovalStage::Prepare;
            let configuration = if old {
                &intent.previous_configuration
            } else {
                &intent.current_configuration
            };
            if r.current_configuration.as_ref() != Some(configuration)
                || r.previous_configuration.as_ref()
                    != (stage == SecondaryRemovalStage::PreviousCurrent)
                        .then_some(&intent.previous_configuration)
                || r.role != member.role
                || r.write_status == AccessStatus::Granted
                || r.pending_operation_id.is_some()
                || (!old
                    && r.retained_operation_id.as_ref()
                        != Some(&intent.command_operation_id(stage, &member.identity)))
                || (!old
                    && r.verified_replication_lsn
                        .is_none_or(|v| v < evidence.preparation.boundary_lsn))
                || (old
                    && member.identity == intent.primary
                    && (r.prepared_secondary_removal.as_ref() != Some(&evidence.preparation)
                        || r.retained_operation_id.as_ref()
                            != Some(&evidence.preparation.operation_id)))
            {
                return None;
            }
            witness(r)
        })
        .collect()
}

fn configuration_command(
    evidence: &SecondaryRemovalEvidence,
    identity: &ReplicaIdentity,
    current_only: bool,
) -> EnsureConfiguration {
    let intent = &evidence.preparation.intent;
    EnsureConfiguration {
        operation_id: intent.command_operation_id(
            if current_only {
                SecondaryRemovalStage::CurrentOnly
            } else {
                SecondaryRemovalStage::PreviousCurrent
            },
            identity,
        ),
        previous_configuration: (!current_only).then(|| intent.previous_configuration.clone()),
        current_configuration: intent.current_configuration.clone(),
        previous_epoch: (!current_only).then_some(intent.previous_configuration.epoch),
        current_epoch: intent.current_configuration.epoch,
        effective_policy: intent.current_policy.clone(),
        previous_policy: Some(intent.previous_policy.clone()),
        secondary_removal_evidence: Some(evidence.clone()),
        scale_up_evidence: None,
        local_replica_id: identity.replica_id,
        expected_instance_id: identity.instance_id.clone(),
        expected_agent_generation: identity.agent_generation.clone(),
        transition_kind: TransitionKind::SecondaryScaleDown,
        failover_safe_lsn: None,
        primary_write_status: AccessStatus::ReconfigurationPending,
        current_only,
        retire_build_ids: Vec::new(),
        switchover_handoff: None,
        retire_switchover_preparation_ids: Vec::new(),
    }
}

fn dispatch(
    snapshot: &ObservationSnapshot,
    evidence: &SecondaryRemovalEvidence,
    current_only: bool,
) -> Option<Plan> {
    let intent = &evidence.preparation.intent;
    // Admit all available members before replaying installed work, then resume
    // the primary first so secondary catch-up cannot starve its source.
    let mut members = intent
        .current_configuration
        .members
        .iter()
        .collect::<Vec<_>>();
    members.sort_by_key(|m| (m.role == ReplicaRole::Primary, m.identity.replica_id));
    let mut replay = None;
    for member in members {
        let Some(r) = report(snapshot, &member.identity) else {
            continue;
        };
        if r.current_configuration.as_ref() != Some(&intent.current_configuration)
            && (r.current_configuration.as_ref() != Some(&intent.previous_configuration)
                || r.previous_configuration.is_some())
        {
            continue;
        }
        let mut admission = evidence.clone();
        admission.reduced_write_quorum.clear();
        let pc_cc = configuration_command(&admission, &member.identity, false);
        let installed = r.current_configuration.as_ref() == Some(&intent.current_configuration);
        let pending_admission = r.pending_operation_id.as_ref() == Some(&pc_cc.operation_id);
        let command = if !installed || (pending_admission && r.previous_configuration.is_some()) {
            pc_cc
        } else if current_only
            && (r.previous_configuration.is_some()
                || r.pending_operation_id.as_ref()
                    == Some(&intent.command_operation_id(
                        SecondaryRemovalStage::CurrentOnly,
                        &member.identity,
                    )))
        {
            configuration_command(evidence, &member.identity, true)
        } else {
            continue;
        };
        if r.pending_operation_id
            .as_ref()
            .is_some_and(|id| id != &command.operation_id)
        {
            continue;
        }
        let plan = Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(Box::new(command)),
        };
        if installed && r.pending_operation_id.is_some() {
            if replay.is_none() || member.role == ReplicaRole::Primary {
                replay = Some(plan);
            }
        } else {
            return Some(plan);
        }
    }
    replay
}

pub(super) fn transition(
    snapshot: &ObservationSnapshot,
    transition: &TransitionIntent,
    config: &EvaluationConfig,
) -> Plan {
    let intent = transition
        .secondary_scale_down
        .as_ref()
        .expect("validated intent");
    let mut status = drift_status(snapshot, snapshot.status.clone());
    if snapshot.routing.write_target.is_some() || snapshot.routing.unresolved_write_target {
        return Plan::Apply {
            changes: vec![KubernetesChange::RemoveWriteRouting],
        };
    }
    let Some(primary) = report(snapshot, &intent.primary) else {
        return wait(
            status,
            "ScaleDownPrimaryUnavailable",
            "The frozen exact primary must recover; no failover or substitution is permitted",
            config,
        );
    };
    if primary.role != ReplicaRole::Primary
        || primary.current_configuration.as_ref().is_none_or(|c| {
            c != &intent.previous_configuration && c != &intent.current_configuration
        })
    {
        return wait(
            status,
            "ScaleDownPrimaryUnavailable",
            "The frozen exact primary must attest previous or requested authority",
            config,
        );
    }
    let Some(evidence) = transition.secondary_removal_evidence.as_ref() else {
        if primary.prepared_secondary_removal.is_none() {
            if primary.pending_operation_id.as_ref().is_some_and(|id| {
                id != &intent.command_operation_id(SecondaryRemovalStage::Prepare, &intent.primary)
            }) {
                return wait(
                    status,
                    "ScaleDownPreparationPending",
                    "The primary is completing local work",
                    config,
                );
            }
            return Plan::Execute {
                command: ProtocolCommand::PrepareSecondaryRemoval(Box::new(
                    PrepareSecondaryRemoval {
                        operation_id: intent
                            .command_operation_id(SecondaryRemovalStage::Prepare, &intent.primary),
                        local_replica_id: intent.primary.replica_id,
                        expected_instance_id: intent.primary.instance_id.clone(),
                        expected_agent_generation: intent.primary.agent_generation.clone(),
                        intent: intent.clone(),
                    },
                )),
            };
        }
        let mut evidence = SecondaryRemovalEvidence {
            preparation: primary.prepared_secondary_removal.clone().expect("checked"),
            previous_read_quorum: Vec::new(),
            reduced_write_quorum: Vec::new(),
        };
        evidence.previous_read_quorum =
            witnesses(snapshot, &evidence, SecondaryRemovalStage::Prepare);
        if primary.pending_operation_id.is_some()
            || primary.retained_operation_id.as_ref() != Some(&evidence.preparation.operation_id)
            || primary
                .verified_replication_lsn
                .is_none_or(|lsn| lsn < evidence.preparation.boundary_lsn)
            || validate_secondary_removal_evidence(&evidence, false).is_err()
        {
            return wait(
                status,
                "ScaleDownPreviousReadQuorumUnavailable",
                "Fresh retained accepted-epoch read quorum and verified primary preparation are required",
                config,
            );
        }
        status
            .transition
            .as_mut()
            .expect("active")
            .secondary_removal_evidence = Some(evidence);
        return persist(waiting_status(
            status,
            "ScaleDownReducedWriteQuorumUnavailable",
            "Frozen previous read evidence; converging write-closed reduced PC/CC",
        ));
    };
    if primary.current_configuration.as_ref() == Some(&intent.previous_configuration)
        && (primary.prepared_secondary_removal.as_ref() != Some(&evidence.preparation)
            || primary
                .verified_replication_lsn
                .is_none_or(|lsn| lsn < evidence.preparation.boundary_lsn))
    {
        return wait(
            status,
            "ScaleDownPreparationPending",
            "Recover the exact frozen durable preparation; never replace its boundary",
            config,
        );
    }
    let current_only = !evidence.reduced_write_quorum.is_empty();
    if let Some(plan) = dispatch(snapshot, evidence, current_only) {
        return plan;
    }
    if !current_only {
        let mut evidence = evidence.clone();
        evidence.reduced_write_quorum =
            witnesses(snapshot, &evidence, SecondaryRemovalStage::PreviousCurrent);
        if validate_secondary_removal_evidence(&evidence, true).is_err() {
            let admitted_quorum = primary.current_configuration.as_ref()
                == Some(&intent.current_configuration)
                && intent
                    .current_configuration
                    .members
                    .iter()
                    .filter(|member| {
                        report(snapshot, &member.identity).is_some_and(|r| {
                            r.current_configuration.as_ref() == Some(&intent.current_configuration)
                                && r.previous_configuration.as_ref()
                                    == Some(&intent.previous_configuration)
                        })
                    })
                    .count()
                    >= intent.current_policy.write_quorum as usize;
            return wait(
                status,
                if admitted_quorum {
                    "ScaleDownReducedCatchUpPending"
                } else {
                    "ScaleDownReducedWriteQuorumUnavailable"
                },
                "Exact reduced write quorum including the primary must verify the prepared boundary",
                config,
            );
        }
        status
            .transition
            .as_mut()
            .expect("active")
            .secondary_removal_evidence = Some(evidence);
        return persist(waiting_status(
            status,
            "ScaleDownCurrentOnlyQuorumUnavailable",
            "Frozen reduced write evidence; converging fresh current-only write quorum",
        ));
    }
    let cleanup = SecondaryScaleDownCleanup {
        evidence: evidence.clone(),
        current_only_write_quorum: witnesses(
            snapshot,
            evidence,
            SecondaryRemovalStage::CurrentOnly,
        ),
        retirement: None,
    };
    if validate_secondary_scale_down_cleanup(&cleanup).is_err() {
        return wait(
            status,
            "ScaleDownCurrentOnlyQuorumUnavailable",
            "Fresh completed exact current-only write quorum must cover the prepared boundary",
            config,
        );
    }
    status.topology = Some(AcceptedTopology {
        configuration: intent.current_configuration.clone(),
    });
    status.effective_policy = Some(intent.current_policy.clone());
    status.transition = None;
    status.quorum_loss = None;
    status.secondary_scale_down_cleanup = Some(cleanup);
    status.last_secondary_removal = None;
    persist(waiting_status(
        status,
        "ScaleDownRetirementPending",
        "Reduced topology and policy committed atomically; exact target retirement and cleanup remain",
    ))
}

pub(super) fn delete(
    resource: ScaleDownResource,
    identity: &CleanupResourceIdentity,
    observation: &ExactResourceObservation,
) -> Option<KubernetesChange> {
    match (identity, observation) {
        (
            CleanupResourceIdentity::Present { name, uid },
            ExactResourceObservation::FrozenUidPresent { resource_version },
        ) if !resource_version.is_empty() => Some(KubernetesChange::DeleteScaleDownResource {
            resource,
            name: name.clone(),
            uid: uid.clone(),
            resource_version: resource_version.clone(),
        }),
        _ => None,
    }
}

pub(super) fn cleanup(
    snapshot: &ObservationSnapshot,
    cleanup: &SecondaryScaleDownCleanup,
    config: &EvaluationConfig,
) -> Plan {
    let intent = &cleanup.evidence.preparation.intent;
    let mut status = drift_status(snapshot, snapshot.status.clone());
    let Some(primary) = report(snapshot, &intent.primary) else {
        if snapshot.routing.write_target.is_some() || snapshot.routing.unresolved_write_target {
            return Plan::Apply {
                changes: vec![KubernetesChange::RemoveWriteRouting],
            };
        }
        return wait(
            status,
            "ScaleDownPrimaryUnavailable",
            "Accepted reduced authority remains frozen during cleanup; the exact primary must recover",
            config,
        );
    };
    if let Some(plan) = converge_committed(snapshot, cleanup) {
        return plan;
    }
    let quorum = intent
        .current_configuration
        .members
        .iter()
        .filter(|m| {
            report(snapshot, &m.identity).is_some_and(|r| {
                stable_member_report(r, m, &intent.current_configuration)
                    && r.verified_replication_lsn
                        .is_some_and(|v| v >= cleanup.evidence.preparation.boundary_lsn)
            })
        })
        .count()
        >= intent.current_policy.write_quorum as usize;
    let primary_member = configuration_primary(&intent.current_configuration);
    if !quorum || !stable_member_report(primary, primary_member, &intent.current_configuration) {
        if snapshot.routing.write_target.is_some() || snapshot.routing.unresolved_write_target {
            return Plan::Apply {
                changes: vec![KubernetesChange::RemoveWriteRouting],
            };
        }
        if primary.write_status == AccessStatus::Granted {
            return access(intent, primary_member, AccessStatus::NoWriteQuorum);
        }
        return wait(
            status,
            "ScaleDownReducedWriteQuorumUnavailable",
            "Accepted reduced current-only quorum is unavailable",
            config,
        );
    }
    if primary.write_status != AccessStatus::Granted {
        return access(intent, primary_member, AccessStatus::Granted);
    }
    if !snapshot.routing.service_present {
        return Plan::Apply {
            changes: vec![KubernetesChange::EnsureWriteRoutingService],
        };
    }
    if snapshot.routing.unresolved_write_target
        || snapshot
            .routing
            .write_target
            .as_ref()
            .is_some_and(|id| id != &intent.primary)
    {
        return Plan::Apply {
            changes: vec![KubernetesChange::RemoveWriteRouting],
        };
    }
    if snapshot.routing.write_target.is_none() {
        return Plan::Apply {
            changes: vec![KubernetesChange::PublishWriteRouting {
                primary: intent.primary.clone(),
            }],
        };
    }
    let Some(exact) = resources(snapshot, &intent.target).filter(|r| r.identity == intent.cleanup)
    else {
        return wait(
            status,
            "ScaleDownExactPodAbsenceRequired",
            "Re-observe frozen resource names and UIDs; label-list omission is not absence",
            config,
        );
    };
    if !absent(&intent.cleanup.endpoint, &exact.endpoint) {
        let status = waiting_status(
            status,
            "ScaleDownCleanupPending",
            "Post-commit cleanup is limited to frozen UIDs; PVC deletion requires authoritative Pod absence",
        );
        if let Some(change) = delete(
            ScaleDownResource::Endpoint,
            &intent.cleanup.endpoint,
            &exact.endpoint,
        ) {
            if status != snapshot.status {
                return persist(status);
            }
            return Plan::Apply {
                changes: vec![change],
            };
        }
        return wait(
            status,
            "ScaleDownCleanupPending",
            "An authoritative exact endpoint lookup is required",
            config,
        );
    }
    // Control dispatch uses the exact Pod address, not the removed peer Service.
    // A committed tombstone is not a substitute for observing Pod absence.
    if cleanup.retirement.is_none()
        && !absent(&intent.cleanup.pod, &exact.pod)
        && let Some(AgentObservation::Report(target)) = snapshot
            .observation_for_identity(&intent.target)
            .map(|r| &r.agent)
        && !target.process_session_id.is_empty()
        && target.report_sequence > 0
        // Older installed authority cannot admit the exact-PC retirement command.
        // Positive observation permits only the already committed exact Pod fence.
        && !(target.epoch < intent.previous_configuration.epoch
            && target.current_configuration.is_some())
    {
        if let Some(retirement) = &target.retired_replica {
            status
                .secondary_scale_down_cleanup
                .as_mut()
                .expect("cleanup")
                .retirement = Some(retirement.clone());
            return persist(waiting_status(
                status,
                "ScaleDownCleanupPending",
                "Exact target retirement is durable; resource cleanup remains",
            ));
        }
        if target.pending_operation_id.as_ref().is_none_or(|id| {
            id == &intent.command_operation_id(SecondaryRemovalStage::Retire, &intent.target)
        }) {
            return Plan::Execute {
                command: ProtocolCommand::RetireReplica(Box::new(RetireReplica {
                    operation_id: intent
                        .command_operation_id(SecondaryRemovalStage::Retire, &intent.target),
                    local_replica_id: intent.target.replica_id,
                    expected_instance_id: intent.target.instance_id.clone(),
                    expected_agent_generation: intent.target.agent_generation.clone(),
                    committed: cleanup.clone(),
                })),
            };
        }
        return wait(
            status,
            "ScaleDownRetirementPending",
            "Exact target retirement is pending",
            config,
        );
    }
    for (resource, identity, observation, reason) in [
        (
            ScaleDownResource::Pod,
            &intent.cleanup.pod,
            &exact.pod,
            "ScaleDownExactPodFencePending",
        ),
        (
            ScaleDownResource::Pvc,
            &intent.cleanup.pvc,
            &exact.pvc,
            "ScaleDownCleanupPending",
        ),
    ] {
        if absent(identity, observation) {
            continue;
        }
        let status = waiting_status(
            status,
            reason,
            "Post-commit cleanup is limited to frozen UIDs; PVC deletion requires authoritative Pod absence",
        );
        if let Some(change) = delete(resource, identity, observation) {
            if status != snapshot.status {
                return persist(status);
            }
            return Plan::Apply {
                changes: vec![change],
            };
        }
        return wait(
            status,
            if resource == ScaleDownResource::Pod {
                "ScaleDownExactPodAbsenceRequired"
            } else {
                reason
            },
            "An authoritative exact-name lookup is required; no absence inferred from lists or RPC failure",
            config,
        );
    }
    status.secondary_scale_down_cleanup = None;
    status.last_secondary_removal = Some(SecondaryRemovalReceipt {
        evidence: cleanup.evidence.clone(),
        current_only_write_quorum: cleanup.current_only_write_quorum.clone(),
    });
    persist(waiting_status(
        status,
        "ScaleDownCleanupComplete",
        "Exact cleanup completed; re-observe latest desired state before another removal",
    ))
}

pub(super) fn recover_local_acceptance(
    snapshot: &ObservationSnapshot,
    config: &EvaluationConfig,
) -> Option<Plan> {
    let receipt = snapshot.status.last_secondary_removal.as_ref()?;
    let intent = &receipt.evidence.preparation.intent;
    let topology = &snapshot.status.topology.as_ref()?.configuration;
    if topology.epoch <= intent.current_configuration.epoch {
        return None;
    }
    for member in &intent.current_configuration.members {
        if member.role != ReplicaRole::ActiveSecondary
            || !topology
                .members
                .iter()
                .any(|m| m.identity == member.identity)
        {
            continue;
        }
        let Some(r) = report(snapshot, &member.identity) else {
            continue;
        };
        if r.current_configuration.as_ref() != Some(&intent.current_configuration)
            || r.previous_configuration.is_some()
            || r.secondary_removal_evidence.as_ref() != Some(&receipt.evidence)
            || r.accepted_secondary_removal.is_some()
        {
            continue;
        }
        let operation_id =
            intent.command_operation_id(SecondaryRemovalStage::AcceptCommit, &member.identity);
        if r.role != member.role
            || r.epoch != intent.current_configuration.epoch
            || r.write_status == AccessStatus::Granted
            || r.prepared_secondary_removal.is_some()
            || r.pending_operation_id
                .as_ref()
                .is_some_and(|id| id != &operation_id)
            || r.verified_replication_lsn
                .is_none_or(|lsn| lsn < receipt.evidence.preparation.boundary_lsn)
        {
            return Some(wait(
                snapshot.status.clone(),
                "ScaleDownLocalAcceptancePending",
                "Exact historical local authority, completed work and verified boundary are required before correction",
                config,
            ));
        }
        return Some(Plan::Execute {
            command: ProtocolCommand::AcceptSecondaryRemovalCommit(Box::new(
                AcceptSecondaryRemovalCommit {
                    operation_id,
                    target: member.identity.clone(),
                    committed: receipt.committed(),
                    local_recovery: true,
                },
            )),
        });
    }
    None
}

pub(super) fn completed(
    snapshot: &ObservationSnapshot,
    receipt: &SecondaryRemovalReceipt,
    config: &EvaluationConfig,
) -> Option<Plan> {
    if snapshot.status.topology.as_ref()?.configuration
        != receipt.evidence.preparation.intent.current_configuration
    {
        return None;
    }
    if let Some(plan) = converge_committed(snapshot, &receipt.committed()) {
        return Some(plan);
    }
    let intent = &receipt.evidence.preparation.intent;
    if intent.current_configuration.members.iter().any(|member| {
        snapshot
            .observation_for_identity(&member.identity)
            .is_some_and(|observation| {
                matches!(&observation.agent, AgentObservation::Report(r)
                    if !stable_member_report(r, member, &intent.current_configuration))
            })
    }) {
        return Some(wait(
            snapshot.status.clone(),
            "ScaleDownRetainedMemberPending",
            "Retained local work must complete; conflicting pending commands cannot be replaced",
            config,
        ));
    }
    None
}

fn converge_committed(
    snapshot: &ObservationSnapshot,
    cleanup: &SecondaryScaleDownCleanup,
) -> Option<Plan> {
    if let Some(plan) = dispatch(snapshot, &cleanup.evidence, true) {
        return Some(plan);
    }
    let intent = &cleanup.evidence.preparation.intent;
    for member in &intent.current_configuration.members {
        let operation_id =
            intent.command_operation_id(SecondaryRemovalStage::AcceptCommit, &member.identity);
        if let Some(r) = report(snapshot, &member.identity)
            && r.identity == member.identity
            && r.role == member.role
            && r.epoch == intent.current_configuration.epoch
            && r.previous_configuration.is_none()
            && r.current_configuration.as_ref() == Some(&intent.current_configuration)
            && r.pending_operation_id
                .as_ref()
                .is_none_or(|id| id == &operation_id)
            && (r.pending_operation_id.is_some()
                || r.accepted_secondary_removal
                    .as_ref()
                    .is_none_or(|accepted| {
                        accepted.evidence != cleanup.evidence
                            || accepted.current_only_write_quorum
                                != cleanup.current_only_write_quorum
                    }))
        {
            let mut committed = cleanup.clone();
            committed.retirement = None;
            return Some(Plan::Execute {
                command: ProtocolCommand::AcceptSecondaryRemovalCommit(Box::new(
                    AcceptSecondaryRemovalCommit {
                        operation_id,
                        target: member.identity.clone(),
                        committed,
                        local_recovery: false,
                    },
                )),
            });
        }
    }
    None
}

fn access(
    intent: &SecondaryScaleDownIntent,
    primary: &ConfigurationMember,
    access: AccessStatus,
) -> Plan {
    Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(Box::new(ensure_configuration_command(
            &intent.current_configuration,
            primary,
            &intent.current_policy,
            OperationId::new(format!(
                "availability:{}:{}",
                intent.current_configuration.configuration_id,
                if access == AccessStatus::Granted {
                    "grant-write"
                } else {
                    "no-write-quorum"
                }
            )),
            access,
            false,
        ))),
    }
}
