use super::*;
use crate::command::ScaleDownResource;
use crate::observation::{AgentBuildReport, AgentReport, ExactResourceObservation};
use crate::types::{
    CleanupResourceIdentity, PodUid, ProvisioningPurpose, PvcUid, ReplicaId, ScaleUpAllocation,
    ScaleUpCleanup, ScaleUpConfigurationEvidence, ScaleUpFailoverEvidence,
    ScaleUpFailoverReceiptEvidence, ScaleUpFinalElectionEvidence, ScaleUpFinalWitness,
    ScaleUpIntent, ScaleUpProvisioning, ScaleUpReceipt, ScaleUpStage, ScaleUpWitness,
};
use crate::validation::{
    validate_scale_up, validate_scale_up_cleanup, validate_scale_up_failover_evidence,
    validate_scale_up_receipt,
};

fn persist(status: AcceptedStatus) -> Plan {
    Plan::Apply {
        changes: vec![KubernetesChange::PersistStatus {
            status: Box::new(status),
        }],
    }
}

fn counts(snapshot: &ObservationSnapshot, status: &AcceptedStatus) -> (u32, u32) {
    (
        status
            .effective_policy
            .as_ref()
            .map_or(0, |policy| policy.replica_set_size),
        snapshot.desired.replicas,
    )
}

fn progress_status(
    snapshot: &ObservationSnapshot,
    status: AcceptedStatus,
    reason: &str,
    phase: &str,
    target: Option<&ReplicaIdentity>,
    attempt: Option<&OperationId>,
    blocking: &str,
) -> AcceptedStatus {
    let (accepted, desired) = counts(snapshot, &status);
    let target = target.map_or_else(
        || "none".to_string(),
        |identity| format!("{}@{}", identity.replica_id, identity.instance_id),
    );
    let attempt = attempt.map_or("none", OperationId::as_str);
    waiting_status(
        status,
        reason,
        &format!(
            "accepted={accepted} desired={desired} target={target} attempt={attempt} phase={phase} blocking={blocking}"
        ),
    )
}

#[allow(clippy::too_many_arguments)]
fn publish_phase_if_changed(
    snapshot: &ObservationSnapshot,
    status: &AcceptedStatus,
    reason: &str,
    phase: &str,
    target: Option<&ReplicaIdentity>,
    attempt: Option<&OperationId>,
    blocking: &str,
) -> Option<Plan> {
    let next = progress_status(
        snapshot,
        status.clone(),
        reason,
        phase,
        target,
        attempt,
        blocking,
    );
    if &next == status {
        return None;
    }
    Some(persist(next))
}

pub(super) fn stable_condition(
    snapshot: &ObservationSnapshot,
    receipt: &ScaleUpReceipt,
) -> StatusCondition {
    let (accepted, desired) = counts(snapshot, &snapshot.status);
    StatusCondition {
        type_: "Ready".into(),
        status: ConditionStatus::True,
        reason: "ScaleUpStable".into(),
        message: format!(
            "accepted={accepted} desired={desired} target={}@{} attempt={} phase=stable blocking=none",
            receipt.intent.target.replica_id,
            receipt.intent.target.instance_id,
            receipt.intent.operation_id
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn wait(
    snapshot: &ObservationSnapshot,
    status: AcceptedStatus,
    reason: &str,
    phase: &str,
    target: Option<&ReplicaIdentity>,
    attempt: Option<&OperationId>,
    blocking: &str,
    config: &EvaluationConfig,
) -> Plan {
    Plan::Wait {
        reason: WaitReason::ActiveTransition,
        status: progress_status(snapshot, status, reason, phase, target, attempt, blocking),
        requeue_after_seconds: config.wait_requeue_seconds,
    }
}

#[allow(clippy::too_many_arguments)]
fn quorum_wait(
    snapshot: &ObservationSnapshot,
    status: AcceptedStatus,
    reason: &str,
    phase: &str,
    target: &ReplicaIdentity,
    attempt: &OperationId,
    blocking: &str,
    config: &EvaluationConfig,
) -> Plan {
    Plan::Wait {
        reason: WaitReason::QuorumLoss,
        status: progress_status(
            snapshot,
            status,
            reason,
            phase,
            Some(target),
            Some(attempt),
            blocking,
        ),
        requeue_after_seconds: config.wait_requeue_seconds,
    }
}

fn report<'a>(
    snapshot: &'a ObservationSnapshot,
    identity: &ReplicaIdentity,
) -> Option<&'a AgentReport> {
    healthy_report(snapshot, identity)
}

pub(super) fn recoverable_failover_conflict_identity(
    snapshot: &ObservationSnapshot,
    _replica_id: i64,
) -> Option<ReplicaIdentity> {
    let evidence = snapshot
        .status
        .transition
        .as_ref()
        .and_then(|transition| transition.scale_up_failover.as_deref());
    let evidence = evidence?;
    let intent = &evidence.intent;
    intent
        .current_configuration
        .members
        .iter()
        .find(|member| member.identity == intent.primary && member.role == ReplicaRole::Primary)
        .and_then(|member| report(snapshot, &member.identity))
        .and_then(|report| {
            if report.epoch != intent.current_configuration.epoch
                || report.current_configuration.as_ref() != Some(&intent.current_configuration)
                || report.scale_up_intent.as_deref() != Some(intent)
                || report.pending_operation_id.is_some()
                || report.identity != intent.primary
                || report.role != ReplicaRole::Primary
            {
                return None;
            }
            let stage = if report.previous_configuration.as_ref()
                == Some(&intent.previous_configuration)
                && matches!(
                    report.write_status,
                    AccessStatus::Granted | AccessStatus::ReconfigurationPending
                ) {
                ScaleUpStage::PreviousCurrent
            } else if report.previous_configuration.is_none()
                && report.write_status == AccessStatus::Granted
            {
                ScaleUpStage::CurrentOnly
            } else {
                return None;
            };
            (report.retained_operation_id.as_ref()
                == Some(&intent.command_operation_id(
                    stage,
                    &report.identity,
                    &intent.current_configuration,
                )))
            .then(|| report.identity.clone())
        })
}

fn build<'a>(
    report: &'a AgentReport,
    build_id: &OperationId,
    target: &ReplicaIdentity,
) -> Option<&'a AgentBuildReport> {
    report
        .builds
        .iter()
        .find(|build| &build.build_id == build_id && &build.target == target)
}

enum BuildPair<'a> {
    Missing,
    Propagating,
    Exact(&'a AgentBuildReport, &'a AgentBuildReport),
}

fn exact_build_pair<'a>(
    source: &'a AgentReport,
    target: &'a AgentReport,
    build_id: &OperationId,
    target_identity: &ReplicaIdentity,
) -> Result<BuildPair<'a>, &'static str> {
    let source_build = build(source, build_id, target_identity);
    let target_build = build(target, build_id, target_identity);
    let (Some(source_build), Some(target_build)) = (source_build, target_build) else {
        return Ok(if source_build.is_some() || target_build.is_some() {
            BuildPair::Propagating
        } else {
            BuildPair::Missing
        });
    };
    if source_build.replication_boundary_lsn != target_build.replication_boundary_lsn {
        return Err("source and receiver report conflicting immutable snapshot boundaries");
    }
    match (
        source_build.catch_up_boundary_lsn,
        target_build.catch_up_boundary_lsn,
    ) {
        (Some(source), Some(target)) if source != target => {
            return Err("source and receiver report conflicting frozen catch-up boundaries");
        }
        (Some(_), None) | (None, Some(_)) => return Ok(BuildPair::Propagating),
        _ => {}
    }
    if source_build
        .catch_up_boundary_lsn
        .is_some_and(|boundary| boundary < source_build.replication_boundary_lsn)
    {
        return Err("source and receiver report conflicting immutable build boundaries");
    }
    Ok(BuildPair::Exact(source_build, target_build))
}

fn source_build_operation_id(build_id: &OperationId) -> OperationId {
    OperationId::new(format!("{build_id}:build-replica"))
}

fn source_build_command(
    primary: &ConfigurationMember,
    target: &ReplicaIdentity,
    build_id: &OperationId,
) -> Plan {
    Plan::Execute {
        command: ProtocolCommand::EnsureReplicaBuild(Box::new(EnsureReplicaBuild {
            operation_id: build_id.clone(),
            local_replica_id: primary.identity.replica_id,
            expected_instance_id: primary.identity.instance_id.clone(),
            expected_agent_generation: primary.identity.agent_generation.clone(),
            target: target.clone(),
            authority: None,
            source_session_id: None,
            retire: false,
        })),
    }
}

fn cleanup_observation<'a>(
    snapshot: &'a ObservationSnapshot,
    target: &ReplicaIdentity,
) -> Option<&'a crate::observation::SecondaryScaleDownResourceObservation> {
    secondary_scale_down::resources(snapshot, target)
}

fn restore_accepted_service_before_cleanup(
    snapshot: &ObservationSnapshot,
    accepted: &ConfigurationDescriptor,
    policy: &EffectivePolicy,
    target: &ReplicaIdentity,
    attempt: &OperationId,
    phase: &str,
    config: &EvaluationConfig,
) -> Option<Plan> {
    let primary = configuration_primary(accepted);
    let reports = accepted
        .members
        .iter()
        .filter_map(|member| {
            let report = report(snapshot, &member.identity)?;
            stable_member_report(report, member, accepted).then_some(report)
        })
        .collect::<Vec<_>>();
    let primary_report = reports
        .iter()
        .copied()
        .find(|report| report.identity == primary.identity);
    let Some(primary_report) = primary_report else {
        return Some(Plan::Wait {
            reason: WaitReason::AwaitingStableEvidence,
            status: progress_status(
                snapshot,
                snapshot.status.clone(),
                "ScaleUpFailoverPrimaryPending",
                phase,
                Some(target),
                Some(attempt),
                "Accepted failover primary must attest exact authority before cleanup",
            ),
            requeue_after_seconds: config.wait_requeue_seconds,
        });
    };
    if reports.len() < accepted.write_quorum as usize {
        return Some(Plan::Wait {
            reason: WaitReason::QuorumLoss,
            status: progress_status(
                snapshot,
                snapshot.status.clone(),
                "ScaleUpFailoverWriteQuorumPending",
                phase,
                Some(target),
                Some(attempt),
                "Accepted failover write quorum must recover before candidate cleanup",
            ),
            requeue_after_seconds: config.wait_requeue_seconds,
        });
    }
    if primary_report.write_status != AccessStatus::Granted {
        return Some(Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(Box::new(ensure_configuration_command(
                accepted,
                primary,
                policy,
                OperationId::new(format!(
                    "scale-up-failover:{}:grant-write",
                    accepted.configuration_id
                )),
                AccessStatus::Granted,
                false,
            ))),
        });
    }
    if !snapshot.routing.service_present {
        return Some(Plan::Apply {
            changes: vec![KubernetesChange::EnsureWriteRoutingService],
        });
    }
    if snapshot.routing.write_target.as_ref() != Some(&primary.identity)
        || snapshot.routing.unresolved_write_target
    {
        let mut changes = Vec::new();
        if snapshot.routing.write_target.is_some() || snapshot.routing.unresolved_write_target {
            changes.push(KubernetesChange::RemoveWriteRouting);
        } else {
            changes.push(KubernetesChange::PublishWriteRouting {
                primary: primary.identity.clone(),
            });
        }
        return Some(Plan::Apply { changes });
    }
    None
}

fn cleanup_source_build_retirement(
    snapshot: &ObservationSnapshot,
    cleanup: &ScaleUpCleanup,
    accepted: &ConfigurationDescriptor,
    config: &EvaluationConfig,
) -> Option<Plan> {
    let scale_up = cleanup
        .provisioning
        .scale_up()
        .expect("validated cleanup has scale-up provisioning");
    let source = configuration_primary(&scale_up.previous_configuration);
    if !accepted
        .members
        .iter()
        .any(|member| member.identity == source.identity)
    {
        return None;
    }
    let source_report = report(snapshot, &source.identity)?;
    let build_id = cleanup
        .provisioning
        .scale_up_build_id(&snapshot.resource_uid)
        .expect("validated cleanup has deterministic build ID");
    let build_operation_id = source_build_operation_id(&build_id);
    let retirement_operation_id = OperationId::new(format!("{build_id}:retire-abandoned-build"));
    let retirement = || Plan::Execute {
        command: ProtocolCommand::EnsureReplicaBuild(Box::new(EnsureReplicaBuild {
            operation_id: build_id.clone(),
            local_replica_id: source.identity.replica_id,
            expected_instance_id: source.identity.instance_id.clone(),
            expected_agent_generation: source.identity.agent_generation.clone(),
            target: cleanup.target.clone(),
            authority: None,
            source_session_id: None,
            retire: true,
        })),
    };

    if source_report.pending_operation_id.as_ref() == Some(&retirement_operation_id)
        || source_report.pending_operation_id.as_ref() == Some(&build_operation_id)
    {
        return Some(retirement());
    }
    if let Some(pending) = source_report.pending_operation_id.as_ref() {
        return Some(wait(
            snapshot,
            snapshot.status.clone(),
            "ScaleUpSourceBuildRetirementBlocked",
            "cleanup",
            Some(&cleanup.target),
            Some(&cleanup.provisioning.operation_id),
            &format!(
                "source has unrelated pending operation {pending}; exact abandoned build retirement remains fenced"
            ),
            config,
        ));
    }
    if source_report.retained_operation_id.as_ref() == Some(&retirement_operation_id) {
        return None;
    }
    if source_report.retained_operation_id.as_ref() == Some(&build_operation_id)
        || source_report
            .builds
            .iter()
            .any(|build| build.build_id == build_id && build.target == cleanup.target)
    {
        return Some(retirement());
    }
    None
}

fn newer_attempt_build_retirement_precedes_receipt(
    snapshot: &ObservationSnapshot,
    receipt: &ScaleUpReceipt,
) -> bool {
    let (provisioning, target, cleanup_frozen) =
        if let Some(cleanup) = snapshot.status.scale_up_cleanup.as_deref() {
            (&cleanup.provisioning, cleanup.target.clone(), true)
        } else if let Some(provisioning) = snapshot.status.provisioning.as_ref()
            && provisioning.scale_up().is_some()
        {
            (
                provisioning,
                provisioning.target_identity(&snapshot.resource_uid),
                false,
            )
        } else {
            return false;
        };
    if provisioning.operation_id == receipt.intent.operation_id {
        return false;
    }
    let scale_up = provisioning
        .scale_up()
        .expect("selected provisioning is scale-up");
    let source = configuration_primary(&scale_up.previous_configuration);
    let build_id = provisioning
        .scale_up_build_id(&snapshot.resource_uid)
        .expect("validated scale-up provisioning has a build ID");
    let exact_transition = snapshot
        .status
        .transition
        .as_ref()
        .is_none_or(|transition| {
            transition.scale_up.as_deref().is_some_and(|intent| {
                intent.operation_id == provisioning.operation_id
                    && intent.build_id == build_id
                    && intent.primary == source.identity
                    && intent.target == target
            })
        });
    if !exact_transition {
        return false;
    }
    let candidate_failed = snapshot
        .observation_for_identity(&target)
        .is_none_or(|observation| {
            matches!(
                &observation.agent,
                AgentObservation::Absent | AgentObservation::Invalid { .. }
            ) || matches!(
                &observation.agent,
                AgentObservation::Report(report)
                    if report.reported_fault == Some(crate::types::FaultType::Permanent)
                        || !report.healthy
            )
        });
    let accepted_count = snapshot
        .status
        .effective_policy
        .as_ref()
        .map_or(0, |policy| policy.replica_set_size);
    if !cleanup_frozen && snapshot.desired.replicas > accepted_count && !candidate_failed {
        return false;
    }
    let Some(source_report) = report(snapshot, &source.identity) else {
        return false;
    };
    let build_operation_id = source_build_operation_id(&build_id);
    let retirement_operation_id = OperationId::new(format!("{build_id}:retire-abandoned-build"));
    source_report
        .pending_operation_id
        .as_ref()
        .is_some_and(|pending| {
            pending == &build_operation_id || pending == &retirement_operation_id
        })
}

fn newer_active_attempt_owns_receipt_reports(
    snapshot: &ObservationSnapshot,
    receipt: &ScaleUpReceipt,
) -> bool {
    let Some(transition) = snapshot.status.transition.as_ref() else {
        return false;
    };
    let Some(active) = transition.scale_up.as_deref().or_else(|| {
        transition
            .scale_up_failover
            .as_deref()
            .map(|evidence| &evidence.intent)
    }) else {
        return false;
    };
    active.operation_id != receipt.intent.operation_id
        && active.previous_configuration == receipt.accepted_configuration
        && snapshot
            .status
            .provisioning
            .as_ref()
            .is_some_and(|provisioning| {
                provisioning.target_identity(&snapshot.resource_uid) == active.target
                    && provisioning
                        .scale_up_build_id(&snapshot.resource_uid)
                        .as_ref()
                        == Some(&active.build_id)
                    && provisioning.scale_up().is_some_and(|scale_up| {
                        scale_up.resource_uid == active.resource_uid
                            && scale_up.spec_generation == active.spec_generation
                            && scale_up.desired_replicas == active.desired_replicas
                            && scale_up.previous_configuration == active.previous_configuration
                            && scale_up.previous_policy == active.previous_policy
                            && scale_up.current_policy == active.current_policy
                            && scale_up.target_replica_id == active.target.replica_id
                    })
            })
}

fn freeze_cleanup(
    snapshot: &ObservationSnapshot,
    provisioning: &ProvisioningIntent,
    mut status: AcceptedStatus,
    reason: &str,
    primary_failed: bool,
    config: &EvaluationConfig,
) -> Plan {
    let target = provisioning.target_identity(&snapshot.resource_uid);
    let Some(exact) = cleanup_observation(snapshot, &target).filter(|observation| {
        secondary_scale_down::observed(&observation.identity.pod, &observation.pod)
            && secondary_scale_down::observed(&observation.identity.pvc, &observation.pvc)
            && secondary_scale_down::observed(&observation.identity.endpoint, &observation.endpoint)
    }) else {
        return wait(
            snapshot,
            status,
            "ScaleUpExactCleanupEvidencePending",
            "cancellation",
            Some(&target),
            Some(&provisioning.operation_id),
            "authoritative exact Pod, PVC, and endpoint observations are required",
            config,
        );
    };
    let cleanup = ScaleUpCleanup {
        provisioning: provisioning.clone(),
        target: target.clone(),
        resources: exact.identity.clone(),
    };
    if let Err(error) = validate_scale_up_cleanup(&cleanup) {
        return unsafe_plan(
            status,
            UnsafeReason::ContradictoryReplicaEvidence(error.to_string()),
            config,
        );
    }
    status.provisioning = None;
    status.transition = None;
    status.scale_up_cleanup = Some(Box::new(cleanup));
    status.scale_up_admission_started = None;
    status = progress_status(
        snapshot,
        status,
        reason,
        if primary_failed {
            "failover-recovery"
        } else {
            "cleanup"
        },
        Some(&target),
        Some(&provisioning.operation_id),
        "frozen exact uncommitted candidate cleanup",
    );
    let mut changes = Vec::new();
    if primary_failed
        && (snapshot.routing.write_target.is_some() || snapshot.routing.unresolved_write_target)
    {
        changes.push(KubernetesChange::RemoveWriteRouting);
    }
    changes.push(KubernetesChange::PersistStatus {
        status: Box::new(status),
    });
    Plan::Apply { changes }
}

fn cleanup_delete(
    identity: &CleanupResourceIdentity,
    observed: &ExactResourceObservation,
    resource: ScaleDownResource,
) -> Option<KubernetesChange> {
    let CleanupResourceIdentity::Present { name, uid } = identity else {
        return None;
    };
    let ExactResourceObservation::FrozenUidPresent { resource_version } = observed else {
        return None;
    };
    Some(KubernetesChange::DeleteScaleDownResource {
        resource,
        name: name.clone(),
        uid: uid.clone(),
        resource_version: resource_version.clone(),
    })
}

fn contextualize_delegated_wait(
    snapshot: &ObservationSnapshot,
    plan: Plan,
    target: &ReplicaIdentity,
    attempt: &OperationId,
    phase: &str,
) -> Plan {
    let Plan::Wait {
        reason,
        status,
        requeue_after_seconds,
    } = plan
    else {
        return plan;
    };
    let condition = status
        .conditions
        .iter()
        .find(|condition| condition.type_ == "Progressing");
    let diagnostic_reason = condition.map_or_else(
        || "ScaleUpRecoveryPending".to_string(),
        |condition| condition.reason.clone(),
    );
    let blocking = condition.map_or_else(
        || "delegated accepted-authority recovery is pending".to_string(),
        |condition| condition.message.clone(),
    );
    Plan::Wait {
        reason,
        status: progress_status(
            snapshot,
            status,
            &diagnostic_reason,
            phase,
            Some(target),
            Some(attempt),
            &blocking,
        ),
        requeue_after_seconds,
    }
}

pub(super) fn cleanup(
    snapshot: &ObservationSnapshot,
    cleanup: &ScaleUpCleanup,
    config: &EvaluationConfig,
) -> Plan {
    if snapshot.status.transition.is_none()
        && let Some(plan) = maybe_begin_stable_failover(snapshot, snapshot.status.clone(), config)
    {
        return contextualize_delegated_wait(
            snapshot,
            plan,
            &cleanup.target,
            &cleanup.provisioning.operation_id,
            "failover-recovery",
        );
    }
    let accepted = &snapshot
        .status
        .topology
        .as_ref()
        .expect("validated cleanup has accepted topology")
        .configuration;
    if let Some(plan) = cleanup_source_build_retirement(snapshot, cleanup, accepted, config) {
        return plan;
    }
    if let Some(plan) = restore_accepted_service_before_cleanup(
        snapshot,
        accepted,
        snapshot
            .status
            .effective_policy
            .as_ref()
            .expect("validated cleanup has policy"),
        &cleanup.target,
        &cleanup.provisioning.operation_id,
        "cleanup",
        config,
    ) {
        return plan;
    }
    let target = &cleanup.target;
    let Some(exact) = cleanup_observation(snapshot, target) else {
        return wait(
            snapshot,
            snapshot.status.clone(),
            "ScaleUpCleanupObservationPending",
            "cleanup",
            Some(target),
            Some(&cleanup.provisioning.operation_id),
            "fresh exact cleanup observation is unavailable",
            config,
        );
    };
    if !secondary_scale_down::absent(&cleanup.resources.endpoint, &exact.endpoint) {
        if let Some(change) = cleanup_delete(
            &cleanup.resources.endpoint,
            &exact.endpoint,
            ScaleDownResource::Endpoint,
        ) {
            return Plan::Apply {
                changes: vec![change],
            };
        }
        return wait(
            snapshot,
            snapshot.status.clone(),
            "ScaleUpEndpointCleanupPending",
            "cleanup",
            Some(target),
            Some(&cleanup.provisioning.operation_id),
            "endpoint lookup is unresolved",
            config,
        );
    }
    if !secondary_scale_down::absent(&cleanup.resources.pod, &exact.pod) {
        if let Some(change) =
            cleanup_delete(&cleanup.resources.pod, &exact.pod, ScaleDownResource::Pod)
        {
            return Plan::Apply {
                changes: vec![change],
            };
        }
        return wait(
            snapshot,
            snapshot.status.clone(),
            "ScaleUpPodCleanupPending",
            "cleanup",
            Some(target),
            Some(&cleanup.provisioning.operation_id),
            "Pod lookup is unresolved",
            config,
        );
    }
    if !secondary_scale_down::absent(&cleanup.resources.pvc, &exact.pvc) {
        if let Some(change) =
            cleanup_delete(&cleanup.resources.pvc, &exact.pvc, ScaleDownResource::Pvc)
        {
            return Plan::Apply {
                changes: vec![change],
            };
        }
        return wait(
            snapshot,
            snapshot.status.clone(),
            "ScaleUpPvcCleanupPending",
            "cleanup",
            Some(target),
            Some(&cleanup.provisioning.operation_id),
            "PVC lookup is unresolved",
            config,
        );
    }
    let mut status = snapshot.status.clone();
    status.scale_up_cleanup = None;
    let policy = status
        .effective_policy
        .as_ref()
        .expect("validated cleanup has accepted policy");
    if snapshot.desired.replicas > policy.replica_set_size {
        let Some(target_value) = policy.replica_set_size.checked_add(1) else {
            return unsafe_plan(
                snapshot.status.clone(),
                UnsafeReason::InvalidDesiredState(
                    "scale-up retry target ordinal overflowed".into(),
                ),
                config,
            );
        };
        let topology = &status
            .topology
            .as_ref()
            .expect("validated cleanup has accepted topology")
            .configuration;
        let Some(target_id) = (1..=i64::from(target_value))
            .map(ReplicaId::new)
            .find(|candidate| {
                topology
                    .members
                    .iter()
                    .all(|member| member.identity.replica_id != *candidate)
            })
        else {
            return unsafe_plan(
                snapshot.status.clone(),
                UnsafeReason::InvalidAcceptedAuthority(
                    "scale-up retry could not allocate a logical identity outside accepted authority"
                        .into(),
                ),
                config,
            );
        };
        let allocation = allocation_for(
            snapshot,
            topology,
            target_id,
            Some(cleanup.provisioning.operation_id.clone()),
        );
        let retry_target = allocation.observation_target();
        let retry_operation_id = allocation.operation_id.clone();
        status.scale_up_allocation = Some(allocation);
        return persist(progress_status(
            snapshot,
            status,
            "ScaleUpAllocationRetryAccepted",
            "allocation",
            Some(&retry_target),
            Some(&retry_operation_id),
            "atomically began a fresh allocation from the completed provisioning cleanup",
        ));
    }
    persist(progress_status(
        snapshot,
        status,
        "ScaleUpCleanupComplete",
        "cleanup",
        Some(target),
        Some(&cleanup.provisioning.operation_id),
        "exact uncommitted candidate is absent",
    ))
}

fn prior_receipt_settled(snapshot: &ObservationSnapshot, receipt: &ScaleUpReceipt) -> bool {
    let accepted = snapshot
        .status
        .topology
        .as_ref()
        .map(|topology| &topology.configuration);
    receipt.accepted_configuration.members.iter().all(|member| {
        if receipt_member_superseded(accepted, member) {
            return true;
        }
        let retired_by_newer_authority = accepted.is_some_and(|configuration| {
            configuration.epoch > receipt.accepted_configuration.epoch
                && configuration.members.iter().any(|accepted_member| {
                    accepted_member.identity == member.identity
                        && accepted_member.role != ReplicaRole::Primary
                })
        });
        let Some(report) = report(snapshot, &member.identity) else {
            return retired_by_newer_authority;
        };
        if retired_by_newer_authority
            && report.reported_fault == Some(crate::types::FaultType::Permanent)
        {
            return true;
        }
        let accepted_newer = accepted.is_some_and(|configuration| {
            report.previous_configuration.is_none()
                && report.current_configuration.as_ref() == Some(configuration)
                && report.epoch >= configuration.epoch
                && report.pending_operation_id.is_none()
        });
        accepted_newer
            || (report.role == member.role
                && report.previous_configuration.is_none()
                && report.current_configuration.as_ref() == Some(&receipt.accepted_configuration)
                && report
                    .verified_replication_lsn
                    .is_some_and(|lsn| lsn >= receipt.intent.catch_up_boundary_lsn)
                && report.scale_up_intent.as_deref() == Some(&receipt.intent)
                && report.pending_operation_id.is_none())
    })
}

fn receipt_member_superseded(
    accepted: Option<&ConfigurationDescriptor>,
    member: &ConfigurationMember,
) -> bool {
    accepted.is_some_and(|configuration| {
        configuration
            .members
            .iter()
            .all(|accepted_member| accepted_member.identity != member.identity)
    })
}

fn configuration_command(
    evidence: ScaleUpConfigurationEvidence,
    current: &ConfigurationDescriptor,
    member: &ConfigurationMember,
    current_only: bool,
    failover_safe_lsn: Option<i64>,
) -> EnsureConfiguration {
    let intent = evidence.intent();
    let failover = matches!(evidence, ScaleUpConfigurationEvidence::Failover { .. });
    let retire_build_id = intent.build_id.clone();
    let stage = if current_only {
        ScaleUpStage::CurrentOnly
    } else {
        ScaleUpStage::PreviousCurrent
    };
    EnsureConfiguration {
        operation_id: intent.command_operation_id(stage, &member.identity, current),
        previous_configuration: (!current_only).then(|| intent.previous_configuration.clone()),
        current_configuration: current.clone(),
        previous_epoch: (!current_only).then_some(intent.previous_configuration.epoch),
        current_epoch: current.epoch,
        effective_policy: intent.current_policy.clone(),
        previous_policy: Some(intent.previous_policy.clone()),
        secondary_removal_evidence: None,
        scale_up_evidence: Some(Box::new(evidence)),
        local_replica_id: member.identity.replica_id,
        expected_instance_id: member.identity.instance_id.clone(),
        expected_agent_generation: member.identity.agent_generation.clone(),
        transition_kind: if failover {
            TransitionKind::Failover
        } else {
            TransitionKind::ScaleUp
        },
        failover_safe_lsn,
        primary_write_status: if failover && !current_only {
            AccessStatus::ReconfigurationPending
        } else {
            AccessStatus::Granted
        },
        current_only,
        retire_build_ids: if current_only {
            vec![retire_build_id]
        } else {
            Vec::new()
        },
        switchover_handoff: None,
        retire_switchover_preparation_ids: Vec::new(),
    }
}

pub(super) fn recover_local_acceptance(
    snapshot: &ObservationSnapshot,
    config: &EvaluationConfig,
) -> Option<Plan> {
    let receipt = snapshot.status.last_scale_up.as_deref()?;
    if newer_active_attempt_owns_receipt_reports(snapshot, receipt) {
        return None;
    }
    if newer_attempt_build_retirement_precedes_receipt(snapshot, receipt) {
        return None;
    }
    let accepted = &snapshot.status.topology.as_ref()?.configuration;
    if replica_failed(snapshot, &configuration_primary(accepted).identity) {
        return None;
    }
    let primary = configuration_primary(accepted);
    let service_needs_restoration = !snapshot.routing.service_present
        || snapshot.routing.unresolved_write_target
        || snapshot.routing.write_target.as_ref() != Some(&primary.identity)
        || report(snapshot, &primary.identity)
            .is_none_or(|report| report.write_status != AccessStatus::Granted);
    if snapshot.status.transition.is_none()
        && service_needs_restoration
        && let Some(plan) = restore_accepted_service_before_cleanup(
            snapshot,
            accepted,
            snapshot.status.effective_policy.as_ref()?,
            &receipt.intent.target,
            &receipt.intent.operation_id,
            "local-convergence",
            config,
        )
    {
        return Some(plan);
    }
    if prior_receipt_settled(snapshot, receipt) {
        return None;
    }
    let failover_evidence = receipt.expanded_failover_evidence();
    if receipt.failover_evidence.is_some() && failover_evidence.is_none() {
        return Some(unsafe_plan(
            snapshot.status.clone(),
            UnsafeReason::InvalidAcceptedAuthority(
                "scale-up receipt has invalid compact provisional authority".into(),
            ),
            config,
        ));
    }
    let evidence = failover_evidence.as_ref().map_or_else(
        || ScaleUpConfigurationEvidence::Admission {
            intent: receipt.intent.clone(),
        },
        |evidence| ScaleUpConfigurationEvidence::Failover {
            evidence: evidence.clone(),
        },
    );
    let failover_safe_lsn = receipt.failover_safe_lsn;
    for member in &receipt.accepted_configuration.members {
        if receipt_member_superseded(Some(accepted), member) {
            // A newer accepted configuration has retired this exact historical
            // incarnation. Keep the receipt as evidence, but never wait on or
            // correct an identity that accepted authority no longer contains.
            continue;
        }
        if snapshot
            .status
            .pending_replacement_cleanup
            .as_ref()
            .is_some_and(|cleanup| cleanup.target == member.identity)
        {
            // Exact accepted-member replacement provenance is already durable.
            // Replacement must no longer depend on the failed process continuing
            // to report its permanent fault.
            return None;
        }
        let permanently_failed = snapshot
            .observation_for_identity(&member.identity)
            .is_some_and(|observation| {
                matches!(
                    &observation.agent,
                    AgentObservation::Report(report)
                        if report.reported_fault == Some(crate::types::FaultType::Permanent)
                )
            });
        if permanently_failed
            && accepted
                .members
                .iter()
                .any(|accepted_member| accepted_member.identity == member.identity)
        {
            // The receipt remains historical evidence, but accepted-membership
            // repair/replacement must arbitrate the committed member failure.
            return None;
        }
        let pod_authoritatively_absent = cleanup_observation(snapshot, &member.identity)
            .is_some_and(|exact| {
                matches!(
                    &exact.identity.pod,
                    CleanupResourceIdentity::Present { uid, .. }
                        if uid == member.identity.instance_id.as_str()
                ) && secondary_scale_down::absent(&exact.identity.pod, &exact.pod)
            });
        if pod_authoritatively_absent
            && accepted
                .members
                .iter()
                .any(|accepted_member| accepted_member.identity == member.identity)
        {
            // A committed candidate remains protected by its receipt. Exact Pod
            // absence is nevertheless accepted-member replacement evidence, not
            // pre-admission cleanup authority.
            return None;
        }
        let Some(report) = report(snapshot, &member.identity) else {
            return Some(wait(
                snapshot,
                snapshot.status.clone(),
                "ScaleUpCommittedDegraded",
                "local-convergence",
                Some(&receipt.intent.target),
                Some(&receipt.intent.operation_id),
                "an exact accepted member is unavailable",
                config,
            ));
        };
        let advanced_to_newer_transition = report.epoch > receipt.accepted_configuration.epoch
            && snapshot
                .status
                .transition
                .as_ref()
                .is_some_and(|transition| {
                    report.current_configuration.as_ref() == Some(&transition.current_configuration)
                });
        if advanced_to_newer_transition {
            continue;
        }
        if let Some(pending) = report.pending_operation_id.as_ref() {
            let pc_cc = configuration_command(
                evidence.clone(),
                &receipt.accepted_configuration,
                member,
                false,
                failover_safe_lsn,
            );
            let current_only = configuration_command(
                evidence.clone(),
                &receipt.accepted_configuration,
                member,
                true,
                failover_safe_lsn,
            );
            let expected = if pending == &pc_cc.operation_id {
                Some(pc_cc)
            } else if pending == &current_only.operation_id {
                Some(current_only)
            } else {
                None
            };
            let Some(expected) = expected else {
                return Some(wait(
                    snapshot,
                    snapshot.status.clone(),
                    "ScaleUpHistoricalAcceptancePending",
                    "local-convergence",
                    Some(&receipt.intent.target),
                    Some(&receipt.intent.operation_id),
                    "an unrelated durable operation must complete before historical scale-up correction",
                    config,
                ));
            };
            let Some(frozen) = report.pending_configuration.as_deref() else {
                return Some(unsafe_plan(
                    snapshot.status.clone(),
                    UnsafeReason::ContradictoryReplicaEvidence(format!(
                        "accepted member {} omitted its pending historical scale-up correction",
                        member.identity.replica_id
                    )),
                    config,
                ));
            };
            if frozen != &expected {
                return Some(unsafe_plan(
                    snapshot.status.clone(),
                    UnsafeReason::ContradictoryReplicaEvidence(format!(
                        "accepted member {} mutated its pending historical scale-up correction",
                        member.identity.replica_id
                    )),
                    config,
                ));
            }
            return Some(Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(Box::new(frozen.clone())),
            });
        }
        let missed_failover_expansion = receipt.failover_evidence.is_some()
            && report.previous_configuration.is_none()
            && report.current_configuration.as_ref()
                == Some(&receipt.intent.previous_configuration)
            && receipt
                .intent
                .previous_configuration
                .members
                .iter()
                .any(|previous| {
                    previous.identity == report.identity && previous.role == report.role
                });
        if missed_failover_expansion {
            return Some(Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(Box::new(configuration_command(
                    evidence.clone(),
                    &receipt.accepted_configuration,
                    member,
                    false,
                    failover_safe_lsn,
                ))),
            });
        }
        let missed_ordinary_pc_cc = receipt.failover_evidence.is_none()
            && report.previous_configuration.is_none()
            && report.current_configuration.as_ref()
                == Some(&receipt.intent.previous_configuration)
            && receipt
                .intent
                .previous_configuration
                .members
                .iter()
                .any(|previous| {
                    previous.identity == report.identity && previous.role == report.role
                });
        if missed_ordinary_pc_cc {
            return Some(Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(Box::new(configuration_command(
                    ScaleUpConfigurationEvidence::Admission {
                        intent: receipt.intent.clone(),
                    },
                    &receipt.accepted_configuration,
                    member,
                    false,
                    None,
                ))),
            });
        }
        let stale_original_failover_authority = receipt.failover_evidence.is_some()
            && report.scale_up_intent.as_deref() == Some(&receipt.intent)
            && report.current_configuration.as_ref() == Some(&receipt.intent.current_configuration)
            && (report.previous_configuration.as_ref()
                == Some(&receipt.intent.previous_configuration)
                || report.previous_configuration.is_none());
        if stale_original_failover_authority {
            return Some(Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(Box::new(configuration_command(
                    evidence.clone(),
                    &receipt.accepted_configuration,
                    member,
                    false,
                    failover_safe_lsn,
                ))),
            });
        }
        let stale_provisional_failover_authority = failover_evidence
            .as_ref()
            .is_some_and(|failover| exact_provisional_failover_report(report, member, failover));
        if stale_provisional_failover_authority {
            return Some(Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(Box::new(configuration_command(
                    evidence.clone(),
                    &receipt.accepted_configuration,
                    member,
                    false,
                    failover_safe_lsn,
                ))),
            });
        }
        let historical_pc_cc = report.previous_configuration.as_ref()
            == Some(&receipt.intent.previous_configuration)
            && report.current_configuration.as_ref() == Some(&receipt.accepted_configuration)
            && report.scale_up_intent.as_deref() == Some(&receipt.intent);
        if historical_pc_cc {
            return Some(Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(Box::new(configuration_command(
                    evidence.clone(),
                    &receipt.accepted_configuration,
                    member,
                    true,
                    failover_safe_lsn,
                ))),
            });
        }
        let accepted_member = accepted
            .members
            .iter()
            .find(|accepted_member| accepted_member.identity == member.identity);
        if accepted_member.is_some()
            && (report.previous_configuration.is_some()
                || report.current_configuration.as_ref() != Some(accepted))
        {
            let retired_historical_consumer = accepted.epoch > receipt.accepted_configuration.epoch
                && accepted_member
                    .is_some_and(|accepted_member| accepted_member.role != ReplicaRole::Primary);
            if retired_historical_consumer {
                // Newer accepted authority has already retired this member's
                // historical receipt obligation. Let ordinary accepted-authority
                // correction repair a reachable stale retained incarnation.
                return None;
            }
            return Some(wait(
                snapshot,
                snapshot.status.clone(),
                "ScaleUpHistoricalAcceptancePending",
                "local-convergence",
                Some(&receipt.intent.target),
                Some(&receipt.intent.operation_id),
                "historical completion must settle before ordinary accepted-authority correction",
                config,
            ));
        }
    }
    None
}

pub(super) fn begin(
    snapshot: &ObservationSnapshot,
    status: AcceptedStatus,
    config: &EvaluationConfig,
) -> Option<Plan> {
    let topology = &status.topology.as_ref()?.configuration;
    let previous_policy = status.effective_policy.as_ref()?;
    if snapshot.desired.replicas <= previous_policy.replica_set_size {
        return None;
    }
    if status
        .conditions
        .iter()
        .any(|condition| condition.type_ == "UnsupportedSpec")
        || status.primary_failure.is_some()
        || status.quorum_loss.is_some()
        || status.pending_replacement_cleanup.is_some()
        || status.last_replacement.is_some()
        || status.secondary_scale_down_cleanup.is_some()
        || status.scale_up_cleanup.is_some()
    {
        return Some(wait(
            snapshot,
            status,
            "ScaleUpBlockedByHigherPriorityWork",
            "admission",
            None,
            None,
            "repair, cleanup, quorum recovery, or unsupported drift remains active",
            config,
        ));
    }
    if let Some(receipt) = status.last_scale_up.as_deref().cloned()
        && !prior_receipt_settled(snapshot, &receipt)
    {
        return Some(wait(
            snapshot,
            status,
            "ScaleUpPriorReceiptPending",
            "local-convergence",
            Some(&receipt.intent.target),
            Some(&receipt.intent.operation_id),
            "late members still require the previous completion receipt",
            config,
        ));
    }
    if snapshot.desired.failover_delay_seconds != previous_policy.failover_delay_seconds
        || topology.members.iter().any(|member| {
            snapshot
                .observation_for_identity(&member.identity)
                .and_then(|observation| observation.kubernetes.as_ref())
                .and_then(|kubernetes| kubernetes.image.as_deref())
                != Some(snapshot.desired.image.as_str())
        })
    {
        return Some(wait(
            snapshot,
            status,
            "ScaleUpUnsupportedDrift",
            "admission",
            None,
            None,
            "scale-up requires count-only desired-state drift",
            config,
        ));
    }
    if topology.members.iter().any(|member| {
        report(snapshot, &member.identity)
            .is_none_or(|report| !stable_member_report(report, member, topology))
    }) {
        return Some(wait(
            snapshot,
            status,
            "ScaleUpAwaitingStableAuthority",
            "admission",
            None,
            None,
            "every exact accepted member must attest stable current-only authority",
            config,
        ));
    }
    let primary = configuration_primary(topology);
    if report(snapshot, &primary.identity)
        .is_none_or(|report| report.write_status != AccessStatus::Granted)
        || snapshot.routing.write_target.as_ref() != Some(&primary.identity)
    {
        return Some(wait(
            snapshot,
            status,
            "ScaleUpAwaitingWritablePrimary",
            "admission",
            None,
            None,
            "accepted write quorum and exact primary routing must be stable",
            config,
        ));
    }
    let Some(target_value) = previous_policy.replica_set_size.checked_add(1) else {
        return Some(unsafe_plan(
            status,
            UnsafeReason::InvalidDesiredState("scale-up target ordinal overflowed".into()),
            config,
        ));
    };
    let Some(target_id) = (1..=i64::from(target_value))
        .map(ReplicaId::new)
        .find(|candidate| {
            topology
                .members
                .iter()
                .all(|member| member.identity.replica_id != *candidate)
        })
    else {
        return Some(unsafe_plan(
            status,
            UnsafeReason::InvalidAcceptedAuthority(
                "scale-up could not allocate a logical identity outside accepted authority".into(),
            ),
            config,
        ));
    };
    let unrelated_extra = snapshot.replicas.iter().any(|(key, observation)| {
        let accepted = topology.members.iter().any(|member| {
            member.identity.replica_id == key.replica_id
                && member.identity.instance_id == key.instance_id
        });
        !accepted && key.replica_id != target_id && observation.kubernetes.is_some()
    });
    if unrelated_extra {
        return Some(wait(
            snapshot,
            status,
            "ScaleUpCleanupBlocking",
            "admission",
            None,
            None,
            "unresolved exact lifecycle resources block a fresh scale-up attempt",
            config,
        ));
    }
    let allocation = allocation_for(snapshot, topology, target_id, None);
    let target = allocation.observation_target();
    let attempt = allocation.operation_id.clone();
    let mut next = status;
    next.scale_up_allocation = Some(allocation);
    Some(persist(progress_status(
        snapshot,
        next,
        "ScaleUpAllocationAccepted",
        "allocation",
        Some(&target),
        Some(&attempt),
        "persisted recoverable candidate allocation before resource creation",
    )))
}

fn allocation_observation<'a>(
    snapshot: &'a ObservationSnapshot,
    allocation: &ScaleUpAllocation,
) -> Option<&'a crate::observation::SecondaryScaleDownResourceObservation> {
    secondary_scale_down::resources(snapshot, &allocation.observation_target())
}

fn allocation_identity(
    identity: &CleanupResourceIdentity,
    observed: &ExactResourceObservation,
) -> Option<String> {
    match (identity, observed) {
        (
            CleanupResourceIdentity::Absent { .. },
            ExactResourceObservation::ReplacementPresent { uid, .. },
        ) => Some(uid.clone()),
        _ => None,
    }
}

fn allocation_wait(
    snapshot: &ObservationSnapshot,
    allocation: &ScaleUpAllocation,
    reason: &str,
    blocking: &str,
    config: &EvaluationConfig,
) -> Plan {
    wait(
        snapshot,
        snapshot.status.clone(),
        reason,
        "allocation",
        Some(&allocation.observation_target()),
        Some(&allocation.operation_id),
        blocking,
        config,
    )
}

fn retry_or_complete_allocation_cleanup(
    snapshot: &ObservationSnapshot,
    allocation: &ScaleUpAllocation,
    mut status: AcceptedStatus,
) -> Plan {
    let previous_policy = status
        .effective_policy
        .as_ref()
        .expect("validated allocation cleanup has accepted policy");
    if snapshot.desired.replicas > previous_policy.replica_set_size {
        let mut retry = allocation.clone();
        retry.previous_configuration_id = retry.accepted_configuration_id.clone();
        retry.previous_operation_id = Some(allocation.operation_id.clone());
        retry.operation_id = OperationId::default();
        retry.scaffolding_requested = false;
        retry.pod_uid = None;
        retry.pvc_uid = None;
        retry.cancellation_started = false;
        retry.operation_id = retry.expected_operation_id();
        let target = retry.observation_target();
        let attempt = retry.operation_id.clone();
        status.scale_up_allocation = Some(retry);
        return persist(progress_status(
            snapshot,
            status,
            "ScaleUpAllocationRetryAccepted",
            "allocation",
            Some(&target),
            Some(&attempt),
            "began a fresh allocation after exact prior-attempt cleanup",
        ));
    }
    status.scale_up_allocation = None;
    persist(progress_status(
        snapshot,
        status,
        "ScaleUpAllocationCleanupComplete",
        "cleanup",
        Some(&allocation.observation_target()),
        Some(&allocation.operation_id),
        "exact cancelled allocation is absent",
    ))
}

fn allocation_for(
    snapshot: &ObservationSnapshot,
    previous_configuration: &ConfigurationDescriptor,
    target_replica_id: ReplicaId,
    previous_operation_id: Option<OperationId>,
) -> ScaleUpAllocation {
    let mut allocation = ScaleUpAllocation {
        resource_uid: snapshot.resource_uid.clone(),
        spec_generation: snapshot.desired.generation,
        desired_replicas: snapshot.desired.replicas,
        previous_configuration_id: previous_configuration.configuration_id.clone(),
        accepted_configuration_id: previous_configuration.configuration_id.clone(),
        target_replica_id,
        operation_id: OperationId::default(),
        previous_operation_id,
        scaffolding_requested: false,
        pod_uid: None,
        pvc_uid: None,
        cancellation_started: false,
    };
    allocation.operation_id = allocation.expected_operation_id();
    allocation
}

pub(super) fn allocation(
    snapshot: &ObservationSnapshot,
    allocation: &ScaleUpAllocation,
    config: &EvaluationConfig,
) -> Plan {
    let target_id = allocation.target_replica_id;
    let previous = &snapshot
        .status
        .topology
        .as_ref()
        .expect("validated allocation has accepted topology")
        .configuration;
    let previous_policy = snapshot
        .status
        .effective_policy
        .as_ref()
        .expect("validated allocation has accepted policy");
    if !allocation.cancellation_started
        && snapshot.desired.replicas <= previous_policy.replica_set_size
    {
        let mut status = snapshot.status.clone();
        status
            .scale_up_allocation
            .as_mut()
            .expect("active allocation")
            .cancellation_started = true;
        return persist(progress_status(
            snapshot,
            status,
            "ScaleUpAllocationCancellationFrozen",
            "cleanup",
            Some(&allocation.observation_target()),
            Some(&allocation.operation_id),
            "persisted irreversible exact allocation cleanup before observing resources",
        ));
    }
    if let Some(plan) = maybe_begin_stable_failover(snapshot, snapshot.status.clone(), config) {
        return plan;
    }
    if let Some(plan) = restore_accepted_service_before_cleanup(
        snapshot,
        previous,
        previous_policy,
        &allocation.observation_target(),
        &allocation.operation_id,
        "allocation",
        config,
    ) {
        return plan;
    }
    let cancelled = allocation.cancellation_started;
    let Some(exact) = allocation_observation(snapshot, allocation) else {
        return allocation_wait(
            snapshot,
            allocation,
            "ScaleUpAllocationObservationPending",
            "fresh exact allocation lookups are unavailable",
            config,
        );
    };
    if matches!(
        exact.endpoint,
        ExactResourceObservation::LookupFailed { .. }
    ) || matches!(exact.pod, ExactResourceObservation::LookupFailed { .. })
        || matches!(exact.pvc, ExactResourceObservation::LookupFailed { .. })
    {
        return allocation_wait(
            snapshot,
            allocation,
            "ScaleUpAllocationObservationPending",
            "fresh exact allocation lookups are unresolved",
            config,
        );
    }
    if !cancelled
        && allocation.pvc_uid.is_some()
        && allocation.pod_uid.is_none()
        && (secondary_scale_down::absent(&exact.identity.pvc, &exact.pvc)
            || exact.pvc_allocation_operation_id.as_ref() != Some(&allocation.operation_id))
    {
        let mut status = snapshot.status.clone();
        status
            .scale_up_allocation
            .as_mut()
            .expect("active allocation")
            .cancellation_started = true;
        return persist(progress_status(
            snapshot,
            status,
            "ScaleUpAllocationStorageLost",
            "cleanup",
            Some(&allocation.observation_target()),
            Some(&allocation.operation_id),
            "authoritative frozen PVC absence, replacement, or provenance loss abandoned the exact allocation",
        ));
    }
    let scaffolding = snapshot.scaffolding_observation_for(target_id);
    let kubernetes = scaffolding.and_then(|observation| observation.kubernetes.as_ref());

    if allocation.pvc_uid.is_none()
        && let Some(uid) = allocation_identity(&exact.identity.pvc, &exact.pvc)
    {
        if !allocation.scaffolding_requested {
            return allocation_wait(
                snapshot,
                allocation,
                "ScaleUpAllocationNameOccupied",
                "same-name PVC is unrelated to this allocation operation",
                config,
            );
        }
        let exact_provenance_matches =
            exact.pvc_allocation_operation_id.as_ref() == Some(&allocation.operation_id);
        let normalized_uid_matches = kubernetes
            .and_then(|observed| observed.pvc_uid.as_ref())
            .map(PvcUid::as_str)
            == Some(uid.as_str());
        if !exact_provenance_matches || !normalized_uid_matches {
            if !cancelled {
                let mut status = snapshot.status.clone();
                status
                    .scale_up_allocation
                    .as_mut()
                    .expect("active allocation")
                    .cancellation_started = true;
                return persist(progress_status(
                    snapshot,
                    status,
                    "ScaleUpAllocationNameCollision",
                    "cleanup",
                    Some(&allocation.observation_target()),
                    Some(&allocation.operation_id),
                    "same-name PVC lacks exact allocation creation provenance; abandoned the attempt without adopting the occupant",
                ));
            }
            return allocation_wait(
                snapshot,
                allocation,
                "ScaleUpAllocationNameOccupied",
                "same-name PVC is unrelated to this allocation operation",
                config,
            );
        }
        let mut status = snapshot.status.clone();
        status
            .scale_up_allocation
            .as_mut()
            .expect("active allocation")
            .pvc_uid = Some(PvcUid::new(uid));
        return persist(progress_status(
            snapshot,
            status,
            "ScaleUpAllocationPvcFrozen",
            "allocation",
            Some(&allocation.observation_target()),
            Some(&allocation.operation_id),
            "persisted exact PVC UID before Pod creation or cleanup",
        ));
    }

    if allocation.pvc_uid.is_none() {
        if !matches!(exact.pvc, ExactResourceObservation::NotFound) {
            return allocation_wait(
                snapshot,
                allocation,
                "ScaleUpAllocationPvcPending",
                "candidate PVC identity is not yet durable",
                config,
            );
        }
        if cancelled {
            if !matches!(exact.pod, ExactResourceObservation::NotFound)
                || !matches!(exact.endpoint, ExactResourceObservation::NotFound)
            {
                return unsafe_plan(
                    snapshot.status.clone(),
                    UnsafeReason::ContradictoryReplicaEvidence(
                        "scale-up allocation observed Pod or endpoint without PVC provenance"
                            .into(),
                    ),
                    config,
                );
            }
            return retry_or_complete_allocation_cleanup(
                snapshot,
                allocation,
                snapshot.status.clone(),
            );
        }
        if !allocation.scaffolding_requested {
            let mut status = snapshot.status.clone();
            status
                .scale_up_allocation
                .as_mut()
                .expect("active allocation")
                .scaffolding_requested = true;
            return persist(progress_status(
                snapshot,
                status,
                "ScaleUpAllocationScaffoldingAuthorized",
                "allocation",
                Some(&allocation.observation_target()),
                Some(&allocation.operation_id),
                "persisted exact-name absence before requesting candidate scaffolding",
            ));
        }
        return Plan::Apply {
            changes: vec![KubernetesChange::EnsureReplicaScaffolding {
                replica_ids: vec![target_id],
            }],
        };
    }

    if allocation.pod_uid.is_none()
        && let Some(uid) = allocation_identity(&exact.identity.pod, &exact.pod)
    {
        let pod_provenance_matches = allocation.scaffolding_requested
            && exact.pod_allocation_operation_id.as_ref() == Some(&allocation.operation_id)
            && exact.pod_matches_allocation_metadata;
        if !pod_provenance_matches {
            if !cancelled {
                let mut status = snapshot.status.clone();
                status
                    .scale_up_allocation
                    .as_mut()
                    .expect("active allocation")
                    .cancellation_started = true;
                return persist(progress_status(
                    snapshot,
                    status,
                    "ScaleUpAllocationPodNameCollision",
                    "cleanup",
                    Some(&allocation.observation_target()),
                    Some(&allocation.operation_id),
                    "same-name Pod lacks exact allocation creation provenance; abandoned the attempt without adopting the occupant",
                ));
            }
            return allocation_wait(
                snapshot,
                allocation,
                "ScaleUpAllocationPodNameOccupied",
                "same-name Pod is unrelated to this allocation operation",
                config,
            );
        }
        let expected_pvc = allocation.pvc_uid.as_ref().map(PvcUid::as_str);
        if kubernetes
            .and_then(|observed| observed.pod_uid.as_ref())
            .map(PodUid::as_str)
            != Some(uid.as_str())
            || kubernetes
                .and_then(|observed| observed.pvc_uid.as_ref())
                .map(PvcUid::as_str)
                != expected_pvc
        {
            let mut status = snapshot.status.clone();
            let allocation = status
                .scale_up_allocation
                .as_mut()
                .expect("active allocation");
            allocation.pod_uid = Some(PodUid::new(uid));
            allocation.cancellation_started = true;
            let target = allocation.observation_target();
            let operation_id = allocation.operation_id.clone();
            return persist(progress_status(
                snapshot,
                status,
                "ScaleUpAllocationPodBindingLost",
                "cleanup",
                Some(&target),
                Some(&operation_id),
                "candidate Pod did not bind the frozen PVC; persisted exact candidate-local cleanup",
            ));
        }

        if cancelled {
            let mut status = snapshot.status.clone();
            status
                .scale_up_allocation
                .as_mut()
                .expect("active allocation")
                .pod_uid = Some(PodUid::new(uid));
            return persist(progress_status(
                snapshot,
                status,
                "ScaleUpAllocationPodFrozen",
                "cleanup",
                Some(&allocation.observation_target()),
                Some(&allocation.operation_id),
                "persisted exact Pod UID before cancellation cleanup",
            ));
        }
        if kubernetes.and_then(|observed| observed.image.as_deref())
            != Some(snapshot.desired.image.as_str())
        {
            return allocation_wait(
                snapshot,
                allocation,
                "ScaleUpCandidateImagePending",
                "candidate image differs from accepted desired image",
                config,
            );
        }
        let pod_uid = PodUid::new(uid);
        let pvc_uid = allocation.pvc_uid.clone().expect("frozen PVC UID");
        let current_policy = EffectivePolicy::fixed(
            previous_policy
                .replica_set_size
                .checked_add(1)
                .expect("validated allocation policy can grow"),
            previous_policy.failover_delay_seconds,
        )
        .expect("validated allocation has positive policy");
        let mut provisioning = ProvisioningIntent {
            purpose: ProvisioningPurpose::scale_up(ScaleUpProvisioning {
                resource_uid: allocation.resource_uid.clone(),
                spec_generation: allocation.spec_generation,
                desired_replicas: allocation.desired_replicas,
                previous_configuration: previous.clone(),
                previous_policy: previous_policy.clone(),
                current_policy,
                target_replica_id: allocation.target_replica_id,
            }),
            pod_uid,
            pvc_uid,
            operation_id: OperationId::default(),
        };
        provisioning.operation_id = provisioning.expected_operation_id();
        let target = provisioning.target_identity(&snapshot.resource_uid);
        let attempt = provisioning.operation_id.clone();
        let mut status = snapshot.status.clone();
        status.scale_up_allocation = None;
        status.provisioning = Some(provisioning);
        return persist(progress_status(
            snapshot,
            status,
            "ScaleUpProvisioningAccepted",
            "provisioning",
            Some(&target),
            Some(&attempt),
            "promoted exact allocated Pod and PVC into provisioning authority",
        ));
    }

    if allocation.pod_uid.is_none() {
        if cancelled {
            if !matches!(exact.endpoint, ExactResourceObservation::NotFound)
                || !matches!(exact.pod, ExactResourceObservation::NotFound)
            {
                return allocation_wait(
                    snapshot,
                    allocation,
                    "ScaleUpAllocationCleanupPending",
                    "endpoint and Pod absence must precede PVC cleanup",
                    config,
                );
            }
            if let Some(change) =
                cleanup_delete(&exact.identity.pvc, &exact.pvc, ScaleDownResource::Pvc)
            {
                return Plan::Apply {
                    changes: vec![change],
                };
            }
            if secondary_scale_down::absent(&exact.identity.pvc, &exact.pvc) {
                return retry_or_complete_allocation_cleanup(
                    snapshot,
                    allocation,
                    snapshot.status.clone(),
                );
            }
            return allocation_wait(
                snapshot,
                allocation,
                "ScaleUpAllocationPvcCleanupPending",
                "exact PVC cleanup is unresolved",
                config,
            );
        }
        return Plan::Apply {
            changes: vec![KubernetesChange::EnsureReplicaScaffolding {
                replica_ids: vec![target_id],
            }],
        };
    }

    if !cancelled {
        return unsafe_plan(
            snapshot.status.clone(),
            UnsafeReason::InvalidAcceptedAuthority(
                "completed allocation was not promoted to provisioning".into(),
            ),
            config,
        );
    }
    if !matches!(exact.endpoint, ExactResourceObservation::NotFound) {
        return allocation_wait(
            snapshot,
            allocation,
            "ScaleUpAllocationEndpointCleanupPending",
            "endpoint absence must precede Pod cleanup",
            config,
        );
    }
    if !secondary_scale_down::absent(&exact.identity.pod, &exact.pod) {
        if let Some(change) =
            cleanup_delete(&exact.identity.pod, &exact.pod, ScaleDownResource::Pod)
        {
            return Plan::Apply {
                changes: vec![change],
            };
        }
        return allocation_wait(
            snapshot,
            allocation,
            "ScaleUpAllocationPodCleanupPending",
            "exact Pod cleanup is unresolved",
            config,
        );
    }
    if !secondary_scale_down::absent(&exact.identity.pvc, &exact.pvc) {
        if let Some(change) =
            cleanup_delete(&exact.identity.pvc, &exact.pvc, ScaleDownResource::Pvc)
        {
            return Plan::Apply {
                changes: vec![change],
            };
        }
        return allocation_wait(
            snapshot,
            allocation,
            "ScaleUpAllocationPvcCleanupPending",
            "exact PVC cleanup is unresolved",
            config,
        );
    }
    retry_or_complete_allocation_cleanup(snapshot, allocation, snapshot.status.clone())
}

pub(super) fn invalid_candidate_binding(
    snapshot: &ObservationSnapshot,
    key: &crate::observation::ReplicaObservationKey,
    observation: &crate::observation::ReplicaObservation,
) -> bool {
    if snapshot
        .status
        .scale_up_cleanup
        .as_deref()
        .is_some_and(|cleanup| {
            cleanup.target.replica_id == key.replica_id
                && cleanup.target.instance_id == key.instance_id
                && matches!(observation.agent, AgentObservation::Invalid { .. })
        })
    {
        return true;
    }
    if let Some(provisioning) = snapshot
        .status
        .provisioning
        .as_ref()
        .filter(|provisioning| provisioning.scale_up().is_some())
    {
        let target = provisioning.target_identity(&snapshot.resource_uid);
        let accepted_contains_target = snapshot.status.topology.as_ref().is_some_and(|topology| {
            topology
                .configuration
                .members
                .iter()
                .any(|member| member.identity == target)
        });
        let admission_started = snapshot
            .status
            .transition
            .as_ref()
            .is_some_and(|transition| {
                let active = transition.scale_up.as_deref().or_else(|| {
                    transition
                        .scale_up_failover
                        .as_deref()
                        .map(|evidence| &evidence.intent)
                });
                active.is_some_and(|active| {
                    snapshot.status.scale_up_admission_started.as_ref()
                        == Some(&active.operation_id)
                        || transition.scale_up_failover.is_some()
                        || snapshot.replicas.values().any(|candidate| {
                            matches!(
                                &candidate.agent,
                                AgentObservation::Report(report)
                                    if exact_pc_cc_report(
                                        report,
                                        active,
                                        &transition.current_configuration,
                                    )
                            )
                        })
                })
            });
        if !accepted_contains_target
            && !admission_started
            && key.replica_id == target.replica_id
            && key.instance_id == target.instance_id
            && matches!(observation.agent, AgentObservation::Invalid { .. })
        {
            return true;
        }
    }
    let Some(allocation) = snapshot
        .status
        .scale_up_allocation
        .as_ref()
        .filter(|allocation| {
            allocation.scaffolding_requested
                && allocation.pvc_uid.is_some()
                && allocation.target_replica_id == key.replica_id
                && allocation
                    .pod_uid
                    .as_ref()
                    .is_none_or(|pod_uid| pod_uid.as_str() == key.instance_id.as_str())
        })
    else {
        return false;
    };
    let AgentObservation::Invalid {
        uninitialized_report: Some(report),
        ..
    } = &observation.agent
    else {
        return false;
    };
    let Some(frozen_pvc_uid) = allocation.pvc_uid.as_ref() else {
        return false;
    };
    if report.resource_uid != snapshot.resource_uid
        || report.replica_id != allocation.target_replica_id
        || report.pod_uid.as_str() != key.instance_id.as_str()
        || report.pvc_uid != *frozen_pvc_uid
        || report.process_session_id.is_empty()
        || report.report_sequence == 0
    {
        return false;
    }
    let Some(exact) = allocation_observation(snapshot, allocation) else {
        return false;
    };
    let pod_matches = match (&exact.identity.pod, &exact.pod) {
        (
            CleanupResourceIdentity::Absent { .. },
            ExactResourceObservation::ReplacementPresent { uid, .. },
        ) => uid == key.instance_id.as_str(),
        (
            CleanupResourceIdentity::Present { uid, .. },
            ExactResourceObservation::FrozenUidPresent { .. },
        ) => uid == key.instance_id.as_str(),
        _ => false,
    };
    let frozen_pvc_is_authoritatively_absent = match &exact.pvc {
        ExactResourceObservation::NotFound => true,
        ExactResourceObservation::ReplacementPresent {
            uid: live_pvc_uid, ..
        } => live_pvc_uid != report.pvc_uid.as_str(),
        ExactResourceObservation::FrozenUidPresent { .. }
        | ExactResourceObservation::LookupFailed { .. } => false,
    };
    pod_matches
        && frozen_pvc_is_authoritatively_absent
        && exact.pod_allocation_operation_id.as_ref() == Some(&allocation.operation_id)
        && exact.pod_matches_allocation_metadata
}

pub(super) fn provisioning(
    snapshot: &ObservationSnapshot,
    provisioning: &ProvisioningIntent,
    config: &EvaluationConfig,
) -> Plan {
    let scale_up = provisioning
        .scale_up()
        .expect("scale-up provisioning branch");
    let target = provisioning.target_identity(&snapshot.resource_uid);
    let build_id = provisioning
        .scale_up_build_id(&snapshot.resource_uid)
        .expect("scale-up provisioning has build ID");
    let previous = &scale_up.previous_configuration;
    let status = progress_status(
        snapshot,
        snapshot.status.clone(),
        "ScaleUpProvisioningInProgress",
        "provisioning",
        Some(&target),
        Some(&provisioning.operation_id),
        "candidate initialization, endpoint, or build evidence is incomplete",
    );
    let accepted = &snapshot
        .status
        .topology
        .as_ref()
        .expect("validated scale-up provisioning has accepted topology")
        .configuration;
    let accepted_primary = configuration_primary(accepted);
    if replica_failed(snapshot, &accepted_primary.identity) {
        return maybe_begin_stable_failover(snapshot, status, config)
            .map(|plan| {
                contextualize_delegated_wait(
                    snapshot,
                    plan,
                    &target,
                    &provisioning.operation_id,
                    "failover-recovery",
                )
            })
            .unwrap_or_else(|| {
                wait(
                    snapshot,
                    snapshot.status.clone(),
                    "ScaleUpFailoverArbitrationPending",
                    "failover-recovery",
                    Some(&target),
                    Some(&provisioning.operation_id),
                    "accepted primary failure arbitration is pending",
                    config,
                )
            });
    }
    if accepted != previous {
        if let Some(plan) = restore_accepted_service_before_cleanup(
            snapshot,
            accepted,
            &scale_up.previous_policy,
            &target,
            &provisioning.operation_id,
            "cleanup",
            config,
        ) {
            return plan;
        }
        return freeze_cleanup(
            snapshot,
            provisioning,
            status,
            "ScaleUpCleanupPendingAfterFailover",
            false,
            config,
        );
    }
    let primary = accepted_primary;
    if snapshot.desired.replicas <= scale_up.previous_policy.replica_set_size {
        return freeze_cleanup(
            snapshot,
            provisioning,
            status,
            "ScaleUpCancelledBeforeAdmission",
            false,
            config,
        );
    }
    let Some(observation) = snapshot.observation_for_identity(&target) else {
        return freeze_cleanup(
            snapshot,
            provisioning,
            status,
            "ScaleUpCandidateDisappeared",
            false,
            config,
        );
    };
    let candidate_failed = matches!(observation.agent, AgentObservation::Invalid { .. })
        || matches!(
            &observation.agent,
            AgentObservation::Report(report)
                if report.reported_fault == Some(crate::types::FaultType::Permanent)
        );
    if candidate_failed {
        return freeze_cleanup(
            snapshot,
            provisioning,
            status,
            "ScaleUpCandidateFailed",
            false,
            config,
        );
    }
    match &observation.agent {
        AgentObservation::Uninitialized(report) => Plan::Execute {
            command: ProtocolCommand::InitializeAgentStore(Box::new(InitializeAgentStore {
                initialization_id: provisioning.initialization_id(&snapshot.resource_uid),
                resource_uid: snapshot.resource_uid.clone(),
                local_replica_id: target.replica_id,
                expected_instance_id: target.instance_id.clone(),
                expected_pod_uid: report.pod_uid.clone(),
                expected_pvc_uid: report.pvc_uid.clone(),
                assigned_agent_generation: target.agent_generation.clone(),
                effective_policy: scale_up.current_policy.clone(),
                bootstrap_configuration: previous.clone(),
                provisioning: Some(provisioning.clone()),
            })),
        },
        AgentObservation::Absent | AgentObservation::Unreachable { .. } => wait(
            snapshot,
            status,
            "ScaleUpCandidateAgentPending",
            "provisioning",
            Some(&target),
            Some(&provisioning.operation_id),
            "candidate agent is unavailable",
            config,
        ),
        AgentObservation::Invalid { message, .. } => unsafe_plan(
            status,
            UnsafeReason::ContradictoryReplicaEvidence(message.clone()),
            config,
        ),
        AgentObservation::Report(target_report) => {
            if target_report.identity != target
                || target_report.resource_uid != snapshot.resource_uid
                || target_report.retired_replica.is_some()
                || target_report.role == ReplicaRole::ActiveSecondary
                || target_report.current_configuration.is_some()
            {
                return unsafe_plan(
                    status,
                    UnsafeReason::ContradictoryReplicaEvidence(
                        "candidate report differs from fresh scale-up initialization authority"
                            .into(),
                    ),
                    config,
                );
            }
            if observation
                .kubernetes
                .as_ref()
                .is_none_or(|kubernetes| !kubernetes.peer_endpoint_ready)
            {
                return Plan::Apply {
                    changes: vec![KubernetesChange::EnsureReplicaScaffolding {
                        replica_ids: vec![target.replica_id],
                    }],
                };
            }
            let Some(source_report) = report(snapshot, &primary.identity) else {
                return wait(
                    snapshot,
                    status,
                    "ScaleUpSourceUnavailable",
                    "copying",
                    Some(&target),
                    Some(&build_id),
                    "accepted primary is unavailable",
                    config,
                );
            };
            let pair = match exact_build_pair(source_report, target_report, &build_id, &target) {
                Ok(pair) => pair,
                Err(message) => {
                    return unsafe_plan(
                        status,
                        UnsafeReason::ContradictoryReplicaEvidence(message.into()),
                        config,
                    );
                }
            };
            let build_operation_id = source_build_operation_id(&build_id);
            let build_active =
                source_report.pending_operation_id.as_ref() == Some(&build_operation_id);
            let source_busy = source_report.pending_operation_id.is_some() && !build_active;
            let build_claimed_complete =
                source_report.retained_operation_id.as_ref() == Some(&build_operation_id);
            let source_build_evidence = build(source_report, &build_id, &target);
            let target_build_evidence = build(target_report, &build_id, &target);
            let source_build_completed = source_build_evidence.is_some_and(|build| {
                build.completed
                    && build.catch_up_boundary_lsn.is_some_and(|boundary| {
                        build.durable_lsn >= boundary && boundary >= build.replication_boundary_lsn
                    })
            });
            let (source_build, target_build) = match pair {
                BuildPair::Missing => {
                    if build_active {
                        return wait(
                            snapshot,
                            status,
                            "ScaleUpCopyActive",
                            "copying",
                            Some(&target),
                            Some(&build_id),
                            "the exact source build operation is still active",
                            config,
                        );
                    }
                    if source_busy {
                        return wait(
                            snapshot,
                            status,
                            "ScaleUpSourceBusy",
                            "copying",
                            Some(&target),
                            Some(&build_id),
                            "the source has unrelated durable work before copy dispatch",
                            config,
                        );
                    }
                    if build_claimed_complete {
                        return freeze_cleanup(
                            snapshot,
                            provisioning,
                            status,
                            "ScaleUpBuildLivenessAmbiguous",
                            false,
                            config,
                        );
                    }
                    if let Some(plan) = publish_phase_if_changed(
                        snapshot,
                        &snapshot.status,
                        "ScaleUpCopying",
                        "copying",
                        Some(&target),
                        Some(&build_id),
                        "source and receiver build progress has not been observed",
                    ) {
                        return plan;
                    }
                    return source_build_command(primary, &target, &build_id);
                }
                BuildPair::Propagating => {
                    if build_active {
                        return wait(
                            snapshot,
                            status,
                            "ScaleUpBoundaryPropagationPending",
                            "catch-up",
                            Some(&target),
                            Some(&build_id),
                            "the exact source build operation is still propagating durable evidence",
                            config,
                        );
                    }
                    if source_build_evidence.is_some() && !source_busy && !build_claimed_complete {
                        return source_build_command(primary, &target, &build_id);
                    }
                    if source_build_evidence.is_none() && target_build_evidence.is_none() {
                        return source_build_command(primary, &target, &build_id);
                    }
                    if source_busy
                        || (build_claimed_complete && !source_build_completed)
                        || (source_build_evidence.is_none() && target_build_evidence.is_some())
                    {
                        return freeze_cleanup(
                            snapshot,
                            provisioning,
                            status,
                            "ScaleUpBuildLivenessAmbiguous",
                            false,
                            config,
                        );
                    }
                    return wait(
                        snapshot,
                        status,
                        "ScaleUpBoundaryPropagationPending",
                        "catch-up",
                        Some(&target),
                        Some(&build_id),
                        "source and receiver have not both observed the frozen boundary",
                        config,
                    );
                }
                BuildPair::Exact(source_build, target_build) => (source_build, target_build),
            };
            let Some(catch_up_boundary) = source_build.catch_up_boundary_lsn else {
                if build_active {
                    return wait(
                        snapshot,
                        status,
                        "ScaleUpBoundaryPending",
                        "catch-up",
                        Some(&target),
                        Some(&build_id),
                        "post-enumeration catch-up boundary is not frozen",
                        config,
                    );
                }
                if build_claimed_complete && source_build.completed {
                    return wait(
                        snapshot,
                        status,
                        "ScaleUpCatchUpPending",
                        "catch-up",
                        Some(&target),
                        Some(&build_id),
                        "completed source build evidence is awaiting receiver report propagation",
                        config,
                    );
                }
                if !source_busy && !build_claimed_complete {
                    return source_build_command(primary, &target, &build_id);
                }
                return freeze_cleanup(
                    snapshot,
                    provisioning,
                    status,
                    "ScaleUpBuildLivenessAmbiguous",
                    false,
                    config,
                );
            };
            if !source_build.completed
                || !target_build.completed
                || target_build.catch_up_boundary_lsn != Some(catch_up_boundary)
                || source_build.durable_lsn < catch_up_boundary
                || target_build.durable_lsn < catch_up_boundary
            {
                if build_active {
                    if let Some(plan) = publish_phase_if_changed(
                        snapshot,
                        &snapshot.status,
                        "ScaleUpCatchUpPending",
                        "catch-up",
                        Some(&target),
                        Some(&build_id),
                        "receiver durable progress has not reached the frozen catch-up boundary",
                    ) {
                        return plan;
                    }
                    return wait(
                        snapshot,
                        status,
                        "ScaleUpCatchUpPending",
                        "catch-up",
                        Some(&target),
                        Some(&build_id),
                        "receiver durable progress has not reached the frozen catch-up boundary",
                        config,
                    );
                }
                if !source_busy && !build_claimed_complete {
                    return source_build_command(primary, &target, &build_id);
                }
                return freeze_cleanup(
                    snapshot,
                    provisioning,
                    status,
                    "ScaleUpBuildLivenessAmbiguous",
                    false,
                    config,
                );
            }
            if build_active {
                return wait(
                    snapshot,
                    status,
                    "ScaleUpBuildSettlementPending",
                    "catch-up",
                    Some(&target),
                    Some(&build_id),
                    "completed build evidence is waiting for the exact source operation to settle",
                    config,
                );
            }
            let Some(configuration_number) = previous.epoch.configuration_number.checked_add(1)
            else {
                return unsafe_plan(
                    status,
                    UnsafeReason::InvalidAcceptedAuthority(
                        "scale-up configuration epoch exhausted".into(),
                    ),
                    config,
                );
            };
            let mut members = previous.members.clone();
            members.push(ConfigurationMember {
                identity: target.clone(),
                role: ReplicaRole::ActiveSecondary,
            });
            let current = ConfigurationDescriptor::new(
                Epoch::new(previous.epoch.data_loss_number, configuration_number),
                previous.primary_id,
                members,
                scale_up.current_policy.write_quorum,
            );
            let mut intent = ScaleUpIntent {
                operation_id: OperationId::default(),
                resource_uid: snapshot.resource_uid.clone(),
                spec_generation: scale_up.spec_generation,
                desired_replicas: scale_up.desired_replicas,
                previous_configuration: previous.clone(),
                current_configuration: current.clone(),
                previous_policy: scale_up.previous_policy.clone(),
                current_policy: scale_up.current_policy.clone(),
                primary: primary.identity.clone(),
                target: target.clone(),
                build_id: provisioning
                    .scale_up_build_id(&snapshot.resource_uid)
                    .expect("scale-up build ID"),
                snapshot_boundary_lsn: source_build.replication_boundary_lsn,
                catch_up_boundary_lsn: catch_up_boundary,
            };
            intent.operation_id = intent.expected_operation_id();
            if let Err(error) = validate_scale_up(&intent) {
                return unsafe_plan(
                    status,
                    UnsafeReason::InvalidAcceptedAuthority(error.to_string()),
                    config,
                );
            }
            let mut next = snapshot.status.clone();
            next.transition = Some(TransitionIntent {
                transition_id: intent.transition_id(TransitionKind::ScaleUp, &current),
                kind: TransitionKind::ScaleUp,
                spec_generation: intent.spec_generation,
                effective_policy: intent.current_policy.clone(),
                previous_configuration_id: Some(previous.configuration_id.clone()),
                current_configuration: current,
                election_lsn: None,
                build_id: Some(intent.build_id.clone()),
                repair: None,
                switchover: None,
                secondary_scale_down: None,
                secondary_removal_evidence: None,
                scale_up: Some(Box::new(intent.clone())),
                scale_up_failover: None,
            });
            persist(progress_status(
                snapshot,
                next,
                "ScaleUpPreviousCurrentPersisted",
                "pc-cc",
                Some(&target),
                Some(&intent.operation_id),
                "exact candidate build completed through the frozen boundary",
            ))
        }
    }
}

fn witness(report: &AgentReport) -> Option<ScaleUpWitness> {
    Some(ScaleUpWitness {
        resource_uid: report.resource_uid.clone(),
        identity: report.identity.clone(),
        role: report.role,
        process_session_id: report.process_session_id.clone(),
        report_sequence: report.report_sequence,
        epoch: report.epoch,
        previous_configuration_id: report
            .previous_configuration
            .as_ref()
            .map(|configuration| configuration.configuration_id.clone()),
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

fn final_witness(report: &AgentReport) -> Option<ScaleUpFinalWitness> {
    Some(ScaleUpFinalWitness {
        replica_id: report.identity.replica_id,
        process_session_id: report.process_session_id.clone(),
        report_sequence: report.report_sequence,
        current_progress: report.current_progress,
        committed_lsn: report.committed_lsn,
        deactivated_lsn: report.deactivated_lsn?,
        fence_operation_id: report.retained_operation_id.clone()?,
    })
}

fn exact_pc_cc_report(
    report: &AgentReport,
    intent: &ScaleUpIntent,
    current: &ConfigurationDescriptor,
) -> bool {
    report.epoch == current.epoch
        && report.previous_configuration.as_ref() == Some(&intent.previous_configuration)
        && report.current_configuration.as_ref() == Some(current)
        && report.scale_up_intent.as_deref() == Some(intent)
        && report
            .verified_replication_lsn
            .is_some_and(|lsn| lsn >= intent.catch_up_boundary_lsn)
}

fn exact_current_only_report(
    report: &AgentReport,
    intent: &ScaleUpIntent,
    current: &ConfigurationDescriptor,
) -> bool {
    report.epoch == current.epoch
        && report.previous_configuration.is_none()
        && report.current_configuration.as_ref() == Some(current)
        && report.scale_up_intent.as_deref() == Some(intent)
        && report
            .verified_replication_lsn
            .is_some_and(|lsn| lsn >= intent.catch_up_boundary_lsn)
}

fn exact_fenced_pc_cc_report(
    report: &AgentReport,
    intent: &ScaleUpIntent,
    current: &ConfigurationDescriptor,
) -> bool {
    let operation_id =
        intent.command_operation_id(ScaleUpStage::PreviousCurrent, &report.identity, current);
    report.epoch == current.epoch
        && report.previous_configuration.as_ref() == Some(&intent.previous_configuration)
        && report.current_configuration.as_ref() == Some(current)
        && report.scale_up_intent.as_deref() == Some(intent)
        && report.current_progress >= intent.catch_up_boundary_lsn
        && report.pending_operation_id.is_none()
        && report.retained_operation_id.as_ref() == Some(&operation_id)
        && report.deactivation_epoch == Some(current.epoch)
        && report.deactivated_lsn.is_some()
        && report.write_status != AccessStatus::Granted
}

fn witnessed_provisional_session(evidence: &ScaleUpFailoverEvidence, report: &AgentReport) -> bool {
    let initial = evidence
        .previous_read_quorum
        .iter()
        .chain(&evidence.current_read_quorum)
        .any(|witness| {
            witness.identity == report.identity
                && witness.process_session_id == report.process_session_id
        });
    let final_election = evidence
        .final_election
        .as_deref()
        .is_some_and(|final_election| {
            final_election.witnesses.iter().any(|witness| {
                witness.replica_id == report.identity.replica_id
                    && evidence
                        .provisional_configuration
                        .members
                        .iter()
                        .any(|member| {
                            member.identity == report.identity
                                && member.identity.replica_id == witness.replica_id
                        })
                    && witness.process_session_id == report.process_session_id
            })
        });
    initial || final_election
}

fn claims_provisional_failover_authority(
    report: &AgentReport,
    evidence: &ScaleUpFailoverEvidence,
) -> bool {
    report.epoch == evidence.provisional_configuration.epoch
        || report.current_configuration.as_ref() == Some(&evidence.provisional_configuration)
}

fn exact_provisional_failover_report(
    report: &AgentReport,
    member: &ConfigurationMember,
    evidence: &ScaleUpFailoverEvidence,
) -> bool {
    let intent = &evidence.intent;
    let provisional = &evidence.provisional_configuration;
    let operation_id =
        intent.command_operation_id(ScaleUpStage::PreviousCurrent, &report.identity, provisional);
    report.identity == member.identity
        && provisional.members.iter().any(|provisional_member| {
            provisional_member.identity == report.identity && provisional_member.role == report.role
        })
        && report.epoch == provisional.epoch
        && report.previous_configuration.as_ref() == Some(&intent.previous_configuration)
        && report.current_configuration.as_ref() == Some(provisional)
        && report.scale_up_intent.as_deref() == Some(intent)
        && report.current_progress >= intent.catch_up_boundary_lsn
        && report
            .verified_replication_lsn
            .is_some_and(|lsn| lsn >= intent.catch_up_boundary_lsn)
        && report.retained_operation_id.as_ref() == Some(&operation_id)
        && report.deactivation_epoch == Some(provisional.epoch)
        && report.deactivated_lsn.is_some()
        && report.write_status != AccessStatus::Granted
        && witnessed_provisional_session(evidence, report)
}

fn needs_scale_up_pc_cc(
    report: &AgentReport,
    member: &ConfigurationMember,
    evidence: &ScaleUpConfigurationEvidence,
) -> bool {
    let intent = evidence.intent();
    let retained_previous = report.previous_configuration.is_none()
        && report.current_configuration.as_ref() == Some(&intent.previous_configuration)
        && intent
            .previous_configuration
            .members
            .iter()
            .any(|previous| {
                previous.identity == report.identity
                    && previous.identity == member.identity
                    && previous.role == report.role
            });
    let idle_candidate = member.identity == intent.target
        && report.identity == intent.target
        && report.role == ReplicaRole::IdleSecondary
        && report.previous_configuration.is_none()
        && report.current_configuration.is_none();
    let provisional_failover = match evidence {
        ScaleUpConfigurationEvidence::Admission { .. } => false,
        ScaleUpConfigurationEvidence::Failover { evidence } => {
            exact_provisional_failover_report(report, member, evidence)
        }
    };
    retained_previous || idle_candidate || provisional_failover
}

fn quorum_witnesses(
    snapshot: &ObservationSnapshot,
    intent: &ScaleUpIntent,
    current: &ConfigurationDescriptor,
    eligible: &ConfigurationDescriptor,
    stage: ScaleUpStage,
) -> Vec<ScaleUpWitness> {
    eligible
        .members
        .iter()
        .filter_map(|member| {
            let report = report(snapshot, &member.identity)?;
            let exact = if stage == ScaleUpStage::CurrentOnly {
                exact_current_only_report(report, intent, current)
            } else {
                exact_pc_cc_report(report, intent, current)
            };
            let operation_id = intent.command_operation_id(stage, &member.identity, current);
            (exact
                && report.pending_operation_id.is_none()
                && report.retained_operation_id.as_ref() == Some(&operation_id))
            .then(|| witness(report))
            .flatten()
        })
        .collect()
}

fn recovery_witnesses(
    snapshot: &ObservationSnapshot,
    intent: &ScaleUpIntent,
    eligible: &ConfigurationDescriptor,
) -> Vec<ScaleUpWitness> {
    eligible
        .members
        .iter()
        .filter_map(|member| {
            let report = report(snapshot, &member.identity)?;
            let stage = if exact_pc_cc_report(report, intent, &intent.current_configuration) {
                ScaleUpStage::PreviousCurrent
            } else if exact_current_only_report(report, intent, &intent.current_configuration) {
                ScaleUpStage::CurrentOnly
            } else {
                return None;
            };
            let operation_id =
                intent.command_operation_id(stage, &member.identity, &intent.current_configuration);
            (report.pending_operation_id.is_none()
                && report.retained_operation_id.as_ref() == Some(&operation_id))
            .then(|| witness(report))
            .flatten()
        })
        .collect()
}

fn begin_failover(
    snapshot: &ObservationSnapshot,
    transition: &TransitionIntent,
    intent: &ScaleUpIntent,
    config: &EvaluationConfig,
) -> Plan {
    let current = &intent.current_configuration;
    let previous_witnesses = recovery_witnesses(snapshot, intent, &intent.previous_configuration);
    let current_witnesses = recovery_witnesses(snapshot, intent, current);
    let previous_required = intent.previous_policy.read_quorum as usize;
    let current_required = intent.current_policy.read_quorum as usize;
    let previous_missing = previous_witnesses.len() < previous_required;
    let current_missing = current_witnesses.len() < current_required;
    if previous_missing || current_missing {
        let (reason, blocking) = match (previous_missing, current_missing) {
            (true, false) => (
                "ScaleUpPreviousQuorumEvidencePending",
                format!(
                    "missing previous configuration read quorum: observed={} required={}; \
                     current configuration satisfied: observed={} required={current_required}",
                    previous_witnesses.len(),
                    previous_required,
                    current_witnesses.len(),
                ),
            ),
            (false, true) => (
                "ScaleUpCurrentQuorumEvidencePending",
                format!(
                    "previous configuration satisfied: observed={} required={previous_required}; \
                     missing current configuration read quorum: observed={} required={current_required}",
                    previous_witnesses.len(),
                    current_witnesses.len(),
                ),
            ),
            (true, true) => (
                "ScaleUpDualQuorumEvidencePending",
                format!(
                    "missing previous configuration read quorum: observed={} required={previous_required}; \
                     missing current configuration read quorum: observed={} required={current_required}",
                    previous_witnesses.len(),
                    current_witnesses.len(),
                ),
            ),
            (false, false) => unreachable!(),
        };
        return wait(
            snapshot,
            snapshot.status.clone(),
            reason,
            "failover-recovery",
            Some(&intent.target),
            Some(&intent.operation_id),
            &blocking,
            config,
        );
    }
    let candidate = current_witnesses
        .iter()
        .filter(|witness| witness.identity != intent.primary)
        .max_by(|left, right| {
            left.verified_replication_lsn
                .cmp(&right.verified_replication_lsn)
                .then_with(|| right.identity.replica_id.cmp(&left.identity.replica_id))
        })
        .cloned();
    let Some(candidate) = candidate else {
        return wait(
            snapshot,
            snapshot.status.clone(),
            "ScaleUpFailoverCandidatePending",
            "failover-recovery",
            Some(&intent.target),
            Some(&intent.operation_id),
            "no exact surviving current member can become primary",
            config,
        );
    };
    let Some(configuration_number) = current.epoch.configuration_number.checked_add(1) else {
        return unsafe_plan(
            snapshot.status.clone(),
            UnsafeReason::InvalidAcceptedAuthority("scale-up failover epoch exhausted".into()),
            config,
        );
    };
    let failover = configuration_with_primary(
        current,
        &candidate.identity,
        Epoch::new(current.epoch.data_loss_number, configuration_number),
    );
    let evidence = ScaleUpFailoverEvidence {
        intent: intent.clone(),
        provisional_configuration: failover.clone(),
        previous_read_quorum: previous_witnesses,
        current_read_quorum: current_witnesses,
        final_election: None,
    };
    if let Err(error) = validate_scale_up_failover_evidence(&evidence) {
        return unsafe_plan(
            snapshot.status.clone(),
            UnsafeReason::ContradictoryReplicaEvidence(error.to_string()),
            config,
        );
    }
    let mut status = snapshot.status.clone();
    status.transition = Some(TransitionIntent {
        transition_id: intent.transition_id(TransitionKind::Failover, &failover),
        kind: TransitionKind::Failover,
        spec_generation: transition.spec_generation,
        effective_policy: intent.current_policy.clone(),
        previous_configuration_id: Some(intent.previous_configuration.configuration_id.clone()),
        current_configuration: failover,
        election_lsn: None,
        build_id: Some(intent.build_id.clone()),
        repair: None,
        switchover: None,
        secondary_scale_down: None,
        secondary_removal_evidence: None,
        scale_up: None,
        scale_up_failover: Some(Box::new(evidence)),
    });
    status.primary_failure = None;
    let status = progress_status(
        snapshot,
        status,
        "ScaleUpFailoverPersisted",
        "failover-recovery",
        Some(&intent.target),
        Some(&intent.operation_id),
        "preserved original PC and expanded CC and persisted a provisional write-closed election epoch",
    );
    let mut changes = Vec::new();
    if snapshot.routing.write_target.is_some() || snapshot.routing.unresolved_write_target {
        changes.push(KubernetesChange::RemoveWriteRouting);
    }
    changes.push(KubernetesChange::PersistStatus {
        status: Box::new(status),
    });
    Plan::Apply { changes }
}

fn fenced_failover_reports<'a>(
    snapshot: &'a ObservationSnapshot,
    intent: &ScaleUpIntent,
    current: &ConfigurationDescriptor,
    eligible: &ConfigurationDescriptor,
) -> Vec<&'a AgentReport> {
    eligible
        .members
        .iter()
        .filter_map(|member| {
            let report = report(snapshot, &member.identity)?;
            (report.healthy
                && report.reported_fault != Some(crate::types::FaultType::Permanent)
                && exact_fenced_pc_cc_report(report, intent, current))
            .then_some(report)
        })
        .collect()
}

fn finalize_fenced_failover(
    snapshot: &ObservationSnapshot,
    transition: &TransitionIntent,
    intent: &ScaleUpIntent,
    config: &EvaluationConfig,
) -> Plan {
    let provisional = &transition.current_configuration;
    let previous_reports = fenced_failover_reports(
        snapshot,
        intent,
        provisional,
        &intent.previous_configuration,
    );
    let current_reports = fenced_failover_reports(snapshot, intent, provisional, provisional);
    let previous_required = intent.previous_policy.read_quorum as usize;
    let current_required = intent.current_policy.read_quorum as usize;
    let previous_missing = previous_reports.len() < previous_required;
    let current_missing = current_reports.len() < current_required;
    if previous_missing || current_missing {
        let (reason, blocking) = match (previous_missing, current_missing) {
            (true, false) => (
                "ScaleUpFencedPreviousQuorumPending",
                format!(
                    "fresh fenced previous configuration read quorum is insufficient: observed={} required={previous_required}; fresh current configuration quorum is satisfied: observed={} required={current_required}",
                    previous_reports.len(),
                    current_reports.len(),
                ),
            ),
            (false, true) => (
                "ScaleUpFencedCurrentQuorumPending",
                format!(
                    "fresh fenced previous configuration quorum is satisfied: observed={} required={previous_required}; fresh current configuration read quorum is insufficient: observed={} required={current_required}",
                    previous_reports.len(),
                    current_reports.len(),
                ),
            ),
            (true, true) => (
                "ScaleUpFencedDualQuorumPending",
                format!(
                    "fresh fenced previous configuration read quorum is insufficient: observed={} required={previous_required}; fresh current configuration read quorum is insufficient: observed={} required={current_required}",
                    previous_reports.len(),
                    current_reports.len(),
                ),
            ),
            (false, false) => unreachable!(),
        };
        return wait(
            snapshot,
            snapshot.status.clone(),
            reason,
            "failover-election",
            Some(&intent.target),
            Some(&intent.operation_id),
            &blocking,
            config,
        );
    }
    let candidate = current_reports
        .iter()
        .copied()
        .filter(|report| report.identity != intent.primary)
        .max_by(|left, right| {
            let left_safe = left
                .current_progress
                .min(left.deactivated_lsn.unwrap_or_default());
            let right_safe = right
                .current_progress
                .min(right.deactivated_lsn.unwrap_or_default());
            left_safe
                .cmp(&right_safe)
                .then_with(|| left.current_progress.cmp(&right.current_progress))
                .then_with(|| left.committed_lsn.cmp(&right.committed_lsn))
                .then_with(|| right.identity.replica_id.cmp(&left.identity.replica_id))
        });
    let Some(candidate) = candidate else {
        return wait(
            snapshot,
            snapshot.status.clone(),
            "ScaleUpFencedCandidatePending",
            "failover-election",
            Some(&intent.target),
            Some(&intent.operation_id),
            "fresh fenced current-configuration evidence has no surviving primary candidate",
            config,
        );
    };
    let safe_lsn = candidate.current_progress.min(
        candidate
            .deactivated_lsn
            .expect("fenced report has deactivation progress"),
    );
    let Some(configuration_number) = provisional.epoch.configuration_number.checked_add(1) else {
        return unsafe_plan(
            snapshot.status.clone(),
            UnsafeReason::InvalidAcceptedAuthority(
                "scale-up final failover epoch exhausted".into(),
            ),
            config,
        );
    };
    let finalized = configuration_with_primary(
        provisional,
        &candidate.identity,
        Epoch::new(provisional.epoch.data_loss_number, configuration_number),
    );
    let previous_read_quorum = previous_reports
        .iter()
        .map(|report| report.identity.replica_id)
        .collect();
    let current_read_quorum = current_reports
        .iter()
        .map(|report| report.identity.replica_id)
        .collect();
    let mut witnesses = Vec::new();
    for report in previous_reports.iter().chain(&current_reports) {
        if !witnesses
            .iter()
            .any(|witness: &ScaleUpFinalWitness| witness.replica_id == report.identity.replica_id)
            && let Some(witness) = final_witness(report)
        {
            witnesses.push(witness);
        }
    }
    let final_election = ScaleUpFinalElectionEvidence {
        selected_primary_replica_id: candidate.identity.replica_id,
        witnesses,
        previous_read_quorum,
        current_read_quorum,
    };
    let mut status = snapshot.status.clone();
    let mut finalized_transition = transition.clone();
    finalized_transition.transition_id = intent.transition_id(TransitionKind::Failover, &finalized);
    finalized_transition.current_configuration = finalized;
    finalized_transition.election_lsn = Some(safe_lsn);
    finalized_transition
        .scale_up_failover
        .as_mut()
        .expect("provisional carried failover has evidence")
        .final_election = Some(Box::new(final_election));
    if let Err(error) = validate_scale_up_failover_evidence(
        finalized_transition
            .scale_up_failover
            .as_deref()
            .expect("final carried failover has evidence"),
    ) {
        return unsafe_plan(
            snapshot.status.clone(),
            UnsafeReason::ContradictoryReplicaEvidence(error.to_string()),
            config,
        );
    }
    status.transition = Some(finalized_transition);
    persist(progress_status(
        snapshot,
        status,
        "ScaleUpFencedElectionFinalized",
        "failover-election",
        Some(&intent.target),
        Some(&intent.operation_id),
        &format!(
            "fresh epoch-fenced PC and CC evidence selected replica {} with safe prefix LSN {safe_lsn}",
            candidate.identity.replica_id
        ),
    ))
}

pub(super) fn transition(
    snapshot: &ObservationSnapshot,
    transition: &TransitionIntent,
    config: &EvaluationConfig,
) -> Plan {
    let evidence = transition
        .scale_up
        .as_deref()
        .map(|intent| ScaleUpConfigurationEvidence::Admission {
            intent: intent.clone(),
        })
        .or_else(|| {
            transition.scale_up_failover.as_deref().map(|evidence| {
                ScaleUpConfigurationEvidence::Failover {
                    evidence: evidence.clone(),
                }
            })
        })
        .expect("scale-up transition branch");
    let intent = evidence.intent();
    let current = &transition.current_configuration;
    let Some(provisioning) = snapshot.status.provisioning.as_ref() else {
        return unsafe_plan(
            snapshot.status.clone(),
            UnsafeReason::InvalidAcceptedAuthority(
                "active scale-up transition lacks exact provisioning provenance".into(),
            ),
            config,
        );
    };
    let admission_started =
        snapshot.status.scale_up_admission_started.as_ref() == Some(&intent.operation_id);
    let accepted_primary_failed = replica_failed(snapshot, &intent.primary);
    if transition.kind == TransitionKind::ScaleUp
        && !accepted_primary_failed
        && snapshot
            .status
            .primary_failure
            .as_ref()
            .is_some_and(|failure| failure.primary == intent.primary)
    {
        let mut status = snapshot.status.clone();
        status.primary_failure = None;
        return persist(progress_status(
            snapshot,
            status,
            "ScaleUpPrimaryRecovered",
            "failover-recovery",
            Some(&intent.target),
            Some(&intent.operation_id),
            "accepted primary recovered before failover allocation",
        ));
    }
    if transition.kind == TransitionKind::ScaleUp
        && !admission_started
        && !accepted_primary_failed
        && let Some(plan) = restore_accepted_service_before_cleanup(
            snapshot,
            &intent.previous_configuration,
            &intent.previous_policy,
            &intent.target,
            &intent.operation_id,
            "failover-recovery",
            config,
        )
    {
        return plan;
    }
    let mut recovering_primary_failure = false;
    if transition.kind == TransitionKind::ScaleUp && accepted_primary_failed {
        if !admission_started {
            return maybe_begin_stable_failover(snapshot, snapshot.status.clone(), config)
                .map(|plan| {
                    contextualize_delegated_wait(
                        snapshot,
                        plan,
                        &intent.target,
                        &intent.operation_id,
                        "failover-recovery",
                    )
                })
                .unwrap_or_else(|| {
                    wait(
                        snapshot,
                        snapshot.status.clone(),
                        "ScaleUpFailoverArbitrationPending",
                        "failover-recovery",
                        Some(&intent.target),
                        Some(&intent.operation_id),
                        "accepted primary recovery is pending before scale-up admission",
                        config,
                    )
                });
        }
        let previous_witnesses =
            recovery_witnesses(snapshot, intent, &intent.previous_configuration);
        let current_witnesses = recovery_witnesses(snapshot, intent, &intent.current_configuration);
        if previous_witnesses.len() >= intent.previous_policy.read_quorum as usize
            && current_witnesses.len() >= intent.current_policy.read_quorum as usize
        {
            return begin_failover(snapshot, transition, intent, config);
        }
        recovering_primary_failure = true;
    }
    if transition.kind == TransitionKind::ScaleUp
        && !admission_started
        && snapshot.desired.replicas <= intent.previous_policy.replica_set_size
    {
        return freeze_cleanup(
            snapshot,
            provisioning,
            snapshot.status.clone(),
            "ScaleUpCancelledBeforePreviousCurrent",
            false,
            config,
        );
    }
    if transition.kind == TransitionKind::ScaleUp && !admission_started {
        let candidate_failed = snapshot
            .observation_for_identity(&intent.target)
            .is_none_or(|observation| {
                matches!(
                    &observation.agent,
                    AgentObservation::Absent | AgentObservation::Invalid { .. }
                ) || matches!(
                    &observation.agent,
                    AgentObservation::Report(report)
                        if report.reported_fault == Some(crate::types::FaultType::Permanent)
                            || !report.healthy
                )
            });
        if candidate_failed {
            return freeze_cleanup(
                snapshot,
                provisioning,
                snapshot.status.clone(),
                "ScaleUpCandidateFailedBeforeAdmission",
                false,
                config,
            );
        }
    }
    if transition.kind == TransitionKind::ScaleUp && !admission_started {
        let mut status = snapshot.status.clone();
        status.scale_up_admission_started = Some(intent.operation_id.clone());
        return persist(progress_status(
            snapshot,
            status,
            "ScaleUpAdmissionStarted",
            "pc-cc",
            Some(&intent.target),
            Some(&intent.operation_id),
            "persisted monotonic admission fence before the first PC/CC command",
        ));
    }

    let primary = configuration_primary(current);
    let current_only_started = current.members.iter().any(|member| {
        report(snapshot, &member.identity)
            .is_some_and(|report| exact_current_only_report(report, intent, current))
    });
    if !current_only_started {
        for member in current
            .members
            .iter()
            .filter(|member| {
                member.identity != primary.identity
                    && (transition.kind != TransitionKind::Failover
                        || member.identity != intent.primary)
            })
            .chain(std::iter::once(primary))
        {
            let Some(report) = report(snapshot, &member.identity) else {
                if transition.kind == TransitionKind::Failover || recovering_primary_failure {
                    continue;
                }
                if member.identity == intent.target && !admission_started {
                    return freeze_cleanup(
                        snapshot,
                        provisioning,
                        snapshot.status.clone(),
                        "ScaleUpCandidateDisappearedBeforeAdmission",
                        false,
                        config,
                    );
                }
                if member.identity != primary.identity {
                    continue;
                }
                return wait(
                    snapshot,
                    snapshot.status.clone(),
                    "ScaleUpParticipantPending",
                    "pc-cc",
                    Some(&intent.target),
                    Some(&intent.operation_id),
                    "an exact participant is unavailable",
                    config,
                );
            };
            let operation_id = intent.command_operation_id(
                ScaleUpStage::PreviousCurrent,
                &member.identity,
                current,
            );
            let current_only_operation_id =
                intent.command_operation_id(ScaleUpStage::CurrentOnly, &member.identity, current);
            if report.pending_operation_id.as_ref() == Some(&current_only_operation_id) {
                return Plan::Execute {
                    command: ProtocolCommand::EnsureConfiguration(Box::new(configuration_command(
                        evidence.clone(),
                        current,
                        member,
                        true,
                        transition.election_lsn,
                    ))),
                };
            }
            let installed = if transition.kind == TransitionKind::Failover
                && transition.election_lsn.is_none()
            {
                exact_fenced_pc_cc_report(report, intent, current)
            } else {
                exact_pc_cc_report(report, intent, current)
            };
            if !installed
                || report.pending_operation_id.is_some()
                || report.retained_operation_id.as_ref() != Some(&operation_id)
            {
                if report
                    .pending_operation_id
                    .as_ref()
                    .is_some_and(|pending| pending != &operation_id)
                {
                    return wait(
                        snapshot,
                        snapshot.status.clone(),
                        "ScaleUpCommandPending",
                        "pc-cc",
                        Some(&intent.target),
                        Some(&intent.operation_id),
                        "one exact authority command remains pending",
                        config,
                    );
                }
                return Plan::Execute {
                    command: ProtocolCommand::EnsureConfiguration(Box::new(configuration_command(
                        evidence.clone(),
                        current,
                        member,
                        false,
                        transition.election_lsn,
                    ))),
                };
            }
        }
        if recovering_primary_failure {
            return begin_failover(snapshot, transition, intent, config);
        }
        if transition.kind == TransitionKind::Failover && transition.election_lsn.is_none() {
            return finalize_fenced_failover(snapshot, transition, intent, config);
        }
        let previous_witnesses = quorum_witnesses(
            snapshot,
            intent,
            current,
            &intent.previous_configuration,
            ScaleUpStage::PreviousCurrent,
        );
        let current_witnesses = quorum_witnesses(
            snapshot,
            intent,
            current,
            current,
            ScaleUpStage::PreviousCurrent,
        );
        let candidate_ready = current_witnesses.iter().any(|witness| {
            witness.identity == intent.target
                && witness.verified_replication_lsn >= intent.catch_up_boundary_lsn
        });
        let primary_ready = current_witnesses.iter().any(|witness| {
            witness.identity == primary.identity
                && (transition.kind == TransitionKind::Failover
                    || witness.write_status == AccessStatus::Granted)
        });
        let previous_quorum = if transition.kind == TransitionKind::Failover {
            intent.previous_policy.read_quorum
        } else {
            intent.previous_policy.write_quorum
        };
        let current_quorum = if transition.kind == TransitionKind::Failover {
            intent.current_policy.read_quorum
        } else {
            intent.current_policy.write_quorum
        };
        if previous_witnesses.len() < previous_quorum as usize
            || current_witnesses.len() < current_quorum as usize
        {
            return quorum_wait(
                snapshot,
                snapshot.status.clone(),
                "ScaleUpDualQuorumUnavailable",
                "pc-cc",
                &intent.target,
                &intent.operation_id,
                "previous and current configuration quorum evidence is insufficient",
                config,
            );
        }
        if transition.kind == TransitionKind::Failover {
            let safe_lsn = transition
                .election_lsn
                .expect("finalized failover has a safe LSN");
            let selected_primary_ready =
                report(snapshot, &primary.identity).is_some_and(|report| {
                    exact_pc_cc_report(report, intent, current)
                        && report.pending_operation_id.is_none()
                        && report.deactivation_epoch == Some(current.epoch)
                        && report.deactivated_lsn.is_some_and(|lsn| lsn >= safe_lsn)
                        && report
                            .verified_replication_lsn
                            .is_some_and(|lsn| lsn >= safe_lsn)
                        && report.catch_up_complete
                        && report.write_status == AccessStatus::ReconfigurationPending
                });
            if !selected_primary_ready {
                return wait(
                    snapshot,
                    snapshot.status.clone(),
                    "ScaleUpFinalPrimaryCatchupPending",
                    "catch-up",
                    Some(&intent.target),
                    Some(&intent.operation_id),
                    "selected primary must durably reach the exact fenced safe prefix while write-closed",
                    config,
                );
            }
        }
        if !candidate_ready || !primary_ready {
            return wait(
                snapshot,
                snapshot.status.clone(),
                "ScaleUpDualQuorumPending",
                "catch-up",
                Some(&intent.target),
                Some(&intent.operation_id),
                "both configurations, the exact candidate boundary, and writable primary are required",
                config,
            );
        }
    }

    if let Some(plan) = publish_phase_if_changed(
        snapshot,
        &snapshot.status,
        "ScaleUpCurrentOnlyInstalling",
        "current-only",
        Some(&intent.target),
        Some(&intent.operation_id),
        "installing expanded current-only authority one exact member at a time",
    ) {
        return plan;
    }

    let exact_current_only = |member: &ConfigurationMember| {
        let Some(report) = report(snapshot, &member.identity) else {
            return false;
        };
        let operation_id =
            intent.command_operation_id(ScaleUpStage::CurrentOnly, &member.identity, current);
        exact_current_only_report(report, intent, current)
            && report.pending_operation_id.is_none()
            && report.retained_operation_id.as_ref() == Some(&operation_id)
    };
    let retained_original_participant = |member: &ConfigurationMember| {
        if transition.kind != TransitionKind::Failover || member.identity != intent.primary {
            return true;
        }
        let Some(report) = report(snapshot, &member.identity) else {
            return false;
        };
        let ScaleUpConfigurationEvidence::Failover { evidence } = &evidence else {
            return false;
        };
        claims_provisional_failover_authority(report, evidence)
            || (report.scale_up_intent.as_deref() == Some(intent)
                && report.epoch == current.epoch
                && report.current_configuration.as_ref() == Some(current))
    };
    let mut convergence_order = current
        .members
        .iter()
        .filter(|member| {
            member.identity != primary.identity && retained_original_participant(member)
        })
        .collect::<Vec<_>>();
    convergence_order.push(primary);

    for member in &convergence_order {
        let Some(report) = report(snapshot, &member.identity) else {
            continue;
        };
        let pc_cc = configuration_command(
            evidence.clone(),
            current,
            member,
            false,
            transition.election_lsn,
        );
        let current_only = configuration_command(
            evidence.clone(),
            current,
            member,
            true,
            transition.election_lsn,
        );
        let Some(pending) = report.pending_operation_id.as_ref() else {
            continue;
        };
        let expected = if pending == &pc_cc.operation_id {
            Some(&pc_cc)
        } else if pending == &current_only.operation_id {
            Some(&current_only)
        } else {
            None
        };
        let Some(expected) = expected else {
            continue;
        };
        let Some(frozen) = report.pending_configuration.as_deref() else {
            return unsafe_plan(
                snapshot.status.clone(),
                UnsafeReason::ContradictoryReplicaEvidence(format!(
                    "scale-up member {} omitted its pending final configuration",
                    member.identity.replica_id
                )),
                config,
            );
        };
        if frozen != expected {
            return unsafe_plan(
                snapshot.status.clone(),
                UnsafeReason::ContradictoryReplicaEvidence(format!(
                    "scale-up member {} mutated its pending final configuration",
                    member.identity.replica_id
                )),
                config,
            );
        }
        return Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(Box::new(frozen.clone())),
        };
    }

    let non_primary_current_only = current
        .members
        .iter()
        .filter(|member| member.identity != primary.identity && exact_current_only(member))
        .count();
    let prioritize_primary = !exact_current_only(primary)
        && non_primary_current_only
            >= intent.current_policy.write_quorum.saturating_sub(1) as usize;
    if prioritize_primary {
        convergence_order.rotate_right(1);
    }

    for member in convergence_order {
        let Some(report) = report(snapshot, &member.identity) else {
            continue;
        };
        let pc_cc = configuration_command(
            evidence.clone(),
            current,
            member,
            false,
            transition.election_lsn,
        );
        let current_only = configuration_command(
            evidence.clone(),
            current,
            member,
            true,
            transition.election_lsn,
        );
        if let ScaleUpConfigurationEvidence::Failover { evidence: failover } = &evidence
            && claims_provisional_failover_authority(report, failover)
            && !exact_provisional_failover_report(report, member, failover)
        {
            return unsafe_plan(
                snapshot.status.clone(),
                UnsafeReason::ContradictoryReplicaEvidence(format!(
                    "scale-up member {} reports malformed provisional failover authority",
                    member.identity.replica_id
                )),
                config,
            );
        }
        if needs_scale_up_pc_cc(report, member, &evidence) {
            return Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(Box::new(pc_cc)),
            };
        }
        let operation_id =
            intent.command_operation_id(ScaleUpStage::CurrentOnly, &member.identity, current);
        if !exact_current_only_report(report, intent, current)
            || report.pending_operation_id.is_some()
            || report.retained_operation_id.as_ref() != Some(&operation_id)
        {
            if report
                .pending_operation_id
                .as_ref()
                .is_some_and(|pending| pending != &operation_id)
            {
                continue;
            }
            return Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(Box::new(current_only)),
            };
        }
    }

    let current_only_witnesses = quorum_witnesses(
        snapshot,
        intent,
        current,
        current,
        ScaleUpStage::CurrentOnly,
    );
    let primary_ready = current_only_witnesses.iter().any(|witness| {
        witness.identity == primary.identity && witness.write_status == AccessStatus::Granted
    });
    if current_only_witnesses.len() >= intent.current_policy.write_quorum as usize && primary_ready
    {
        let receipt = ScaleUpReceipt {
            intent: intent.clone(),
            accepted_configuration: current.clone(),
            failover_evidence: match &evidence {
                ScaleUpConfigurationEvidence::Admission { .. } => None,
                ScaleUpConfigurationEvidence::Failover { evidence } => {
                    ScaleUpFailoverReceiptEvidence::from_evidence(evidence)
                }
            },
            failover_safe_lsn: transition.election_lsn,
            current_only_write_quorum: current_only_witnesses,
        };
        if let Err(error) = validate_scale_up_receipt(&receipt) {
            return unsafe_plan(
                snapshot.status.clone(),
                UnsafeReason::ContradictoryReplicaEvidence(error.to_string()),
                config,
            );
        }
        let candidate_ready = report(snapshot, &intent.target).is_some_and(|report| {
            exact_current_only_report(report, intent, current)
                && report.role == ReplicaRole::ActiveSecondary
        });
        let mut accepted = clear_evaluator_conditions(snapshot.status.clone());
        accepted.observed_generation = transition.spec_generation;
        accepted.effective_policy = Some(intent.current_policy.clone());
        accepted.topology = Some(AcceptedTopology {
            configuration: current.clone(),
        });
        accepted.provisioning = None;
        accepted.transition = None;
        accepted.scale_up_cleanup = None;
        accepted.last_scale_up = Some(Box::new(receipt));
        accepted.scale_up_admission_started = None;
        accepted.primary_failure = None;
        accepted.quorum_loss = None;
        return persist(progress_status(
            snapshot,
            accepted,
            if candidate_ready {
                "ScaleUpTopologyAccepted"
            } else {
                "ScaleUpCommittedDegraded"
            },
            if candidate_ready {
                "current-only"
            } else {
                "local-convergence"
            },
            Some(&intent.target),
            Some(&intent.operation_id),
            if candidate_ready {
                "expanded topology committed; stable routing and fresh local evidence remain"
            } else {
                "expanded topology committed without exact candidate local acceptance"
            },
        ));
    }
    wait(
        snapshot,
        snapshot.status.clone(),
        "ScaleUpCurrentOnlyQuorumPending",
        "current-only",
        Some(&intent.target),
        Some(&intent.operation_id),
        "expanded current-only write quorum including the primary is required",
        config,
    )
}
