//! Deterministic reconciliation decisions over a normalized observation.

use crate::command::{
    EnsureConfiguration, EnsureReplicaBuild, InitializeAgentStore, KubernetesChange,
    PrepareSwitchover, ProtocolCommand, SafetyChange,
};
use crate::observation::{AgentObservation, ObservationSnapshot};
use crate::plan::{Plan, UnsafeReason, WaitReason};
use crate::types::{
    AcceptedStatus, AcceptedTopology, AccessStatus, ConditionStatus, ConfigurationDescriptor,
    ConfigurationMember, EffectivePolicy, Epoch, OperationId, PlannedSwitchoverIntent,
    PlannedSwitchoverOutcome, PlannedSwitchoverReceipt, PlannedSwitchoverResolution,
    PrimaryFailureObservation, ProvisioningIntent, QuorumLossObservation, ReplicaIdentity,
    ReplicaInstanceId, ReplicaRepairIntent, ReplicaRole, StatusCondition, TransitionIntent,
    TransitionKind, derive_agent_generation, derive_failover_repair_operation_id,
    derive_initialization_id, derive_replacement_operation_id,
    derive_switchover_preparation_operation_id, derive_transition_id,
};
use crate::validation::ValidationError;
use crate::validation::validate_snapshot;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationConfig {
    pub supported_protocol_version: u32,
    pub stable_resync_seconds: u64,
    pub wait_requeue_seconds: u64,
    pub unsafe_requeue_seconds: u64,
}

impl Default for EvaluationConfig {
    fn default() -> Self {
        Self {
            supported_protocol_version: crate::PROTOCOL_VERSION,
            stable_resync_seconds: 30,
            wait_requeue_seconds: 5,
            unsafe_requeue_seconds: 30,
        }
    }
}

/// Validates one snapshot and returns the next safe reconciliation outcome.
pub fn evaluate(snapshot: &ObservationSnapshot, config: &EvaluationConfig) -> Plan {
    let switchover = snapshot
        .status
        .transition
        .as_ref()
        .filter(|transition| transition.kind == TransitionKind::PlannedSwitchover);
    if switchover
        .and_then(|transition| transition.switchover.as_ref())
        .is_some_and(|intent| intent.resolution == PlannedSwitchoverResolution::Unsafe)
    {
        let message = snapshot
            .status
            .conditions
            .iter()
            .find(|condition| condition.type_ == "SwitchoverSafety")
            .map(|condition| condition.message.as_str())
            .unwrap_or("Frozen switchover cannot safely complete");
        return switchover_unsafe(snapshot, message, config);
    }
    if let Err(error) = validate_snapshot(snapshot) {
        if switchover.is_some() {
            if matches!(error, ValidationError::StaleReportSequence { .. }) {
                return switchover_wait(
                    snapshot.status.clone(),
                    "SwitchoverFreshReportRequired",
                    "Re-observe a strictly newer report in the current process session",
                    config,
                );
            }
            return switchover_unsafe(snapshot, &error.to_string(), config);
        }
        let reason = if error == ValidationError::DesiredReplicasZero {
            UnsafeReason::InvalidDesiredState(error.to_string())
        } else {
            UnsafeReason::InvalidAcceptedAuthority(error.to_string())
        };
        return unsafe_plan(snapshot.status.clone(), reason, config);
    }

    if let Some(plan) = incompatible_protocol_plan(snapshot, config) {
        if switchover.is_some() {
            return switchover_unsafe(
                snapshot,
                "An exact participant uses incompatible protocol authority",
                config,
            );
        }
        return plan;
    }
    if let Some(plan) = invalid_agent_plan(snapshot, config) {
        if switchover.is_some() {
            return switchover_unsafe(
                snapshot,
                "An exact participant reports contradictory authority",
                config,
            );
        }
        return plan;
    }
    if !snapshot.observation_failures.is_empty() {
        return Plan::Wait {
            reason: WaitReason::AgentUnavailable,
            status: waiting_status(
                snapshot.status.clone(),
                "ObservationFailed",
                "A required observation failed",
            ),
            requeue_after_seconds: config.wait_requeue_seconds,
        };
    }
    if !snapshot.supporting_resources_ready {
        return Plan::Apply {
            changes: vec![KubernetesChange::EnsureReplicaSupport],
        };
    }

    if let Some(transition) = &snapshot.status.transition {
        return evaluate_transition(snapshot, transition, config);
    }

    if let Some(provisioning) = &snapshot.status.provisioning {
        return evaluate_provisioning(snapshot, provisioning, config);
    }

    if snapshot.status.initialized {
        return evaluate_stable(snapshot, config);
    }

    evaluate_never_initialized(snapshot, config)
}

fn evaluate_stable(snapshot: &ObservationSnapshot, config: &EvaluationConfig) -> Plan {
    let topology = snapshot
        .status
        .topology
        .as_ref()
        .expect("validated initialized status has topology");
    let configuration = &topology.configuration;
    let mut status = clear_evaluator_conditions(snapshot.status.clone());
    let policy = snapshot
        .status
        .effective_policy
        .as_ref()
        .expect("validated initialized status has effective policy");
    let (spec_fully_observed, unsupported) = desired_spec_state(snapshot, configuration, policy);
    if let Some(condition) = unsupported {
        status = status.with_condition(condition);
    } else if spec_fully_observed {
        status.observed_generation = snapshot.desired.generation;
    }
    status = project_switchover_request(snapshot, status);
    if switchover_rejection(&status) != switchover_rejection(&snapshot.status) {
        return Plan::Apply {
            changes: vec![KubernetesChange::PersistStatus {
                status: Box::new(status),
            }],
        };
    }

    if let Some(plan) = maybe_begin_stable_failover(snapshot, status.clone(), config) {
        return plan;
    }

    if configuration.members.iter().any(|member| {
        snapshot
            .observation_for_identity(&member.identity)
            .and_then(|observation| observation.kubernetes.as_ref())
            .is_some_and(|kubernetes| !kubernetes.peer_endpoint_ready)
    }) {
        return Plan::Apply {
            changes: vec![KubernetesChange::EnsureReplicaScaffolding {
                replica_ids: configuration
                    .members
                    .iter()
                    .map(|member| member.identity.replica_id)
                    .collect(),
            }],
        };
    }

    if let Some((member, report)) = configuration.members.iter().find_map(|member| {
        if member.role == ReplicaRole::Primary {
            return None;
        }
        let report = snapshot
            .observation_for_identity(&member.identity)
            .and_then(|observation| match &observation.agent {
                AgentObservation::Report(report) => Some(report.as_ref()),
                _ => None,
            })?;
        (report.epoch < configuration.epoch
            && (report.role == ReplicaRole::Primary
                || report.write_status == AccessStatus::Granted))
            .then_some((member, report))
    }) {
        if let Some(previous) = report.current_configuration.as_ref() {
            return Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(Box::new(
                    failover_configuration_command(
                        previous,
                        configuration,
                        member,
                        policy,
                        OperationId::new(format!(
                            "accepted-correction:{}:{}",
                            configuration.configuration_id, member.identity.replica_id
                        )),
                        Some(report.current_progress),
                        AccessStatus::ReconfigurationPending,
                        false,
                        Vec::new(),
                    ),
                )),
            };
        }
    }

    if let Some((member, report, previous)) = configuration.members.iter().find_map(|member| {
        if member.role == ReplicaRole::Primary {
            return None;
        }
        let report = snapshot
            .observation_for_identity(&member.identity)
            .and_then(|observation| match &observation.agent {
                AgentObservation::Report(report) => Some(report.as_ref()),
                _ => None,
            })?;
        let previous = report.previous_configuration.as_ref()?;
        (report.identity == member.identity
            && report.role == member.role
            && report.epoch == configuration.epoch
            && report.current_configuration.as_ref() == Some(configuration))
        .then_some((member, report, previous))
    }) {
        return Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(Box::new(
                failover_configuration_command(
                    previous,
                    configuration,
                    member,
                    policy,
                    OperationId::new(format!(
                        "accepted-current-only:{}:{}",
                        configuration.configuration_id, member.identity.replica_id
                    )),
                    Some(report.current_progress),
                    AccessStatus::ReconfigurationPending,
                    true,
                    Vec::new(),
                ),
            )),
        };
    }

    let recovering_service = status.last_switchover.as_ref().is_some_and(|receipt| {
        matches!(
            receipt.outcome,
            PlannedSwitchoverOutcome::OldPrimaryRestored
                | PlannedSwitchoverOutcome::OldPrimaryCompensated
        ) && receipt.resulting_primary.as_ref()
            == Some(&configuration_primary(configuration).identity)
    }) && (snapshot.routing.write_target.as_ref()
        != Some(&configuration_primary(configuration).identity)
        || healthy_report(snapshot, &configuration_primary(configuration).identity)
            .is_some_and(|report| report.write_status != AccessStatus::Granted))
        && configuration
            .members
            .iter()
            .filter(|member| {
                healthy_report(snapshot, &member.identity)
                    .is_some_and(|report| stable_member_report(report, member, configuration))
            })
            .count()
            >= configuration.write_quorum as usize;
    if let Some(failed) = configuration.members.iter().find(|member| {
        if recovering_service {
            return false;
        }
        if member.identity.replica_id == configuration.primary_id {
            return false;
        }
        let permanent_fault = snapshot
            .observation_for_identity(&member.identity)
            .is_some_and(|observation| {
                matches!(
                    &observation.agent,
                    AgentObservation::Report(report)
                        if report.reported_fault == Some(crate::types::FaultType::Permanent)
                )
            });
        let authorized_lag = snapshot
            .observation_for_identity(&member.identity)
            .is_some_and(|observation| {
                matches!(
                    &observation.agent,
                    AgentObservation::Report(report)
                        if report.role != ReplicaRole::Primary
                            && report.write_status != AccessStatus::Granted
                            && report.epoch < configuration.epoch
                )
            });
        let accepted_incarnation_missing = snapshot
            .observation_for_identity(&member.identity)
            .and_then(|observation| observation.kubernetes.as_ref())
            .is_none();
        let orphaned_storage = snapshot.replicas.iter().any(|(key, observation)| {
            key.replica_id == member.identity.replica_id
                && observation.kubernetes.as_ref().is_some_and(|kubernetes| {
                    kubernetes.pod_uid.is_none() && kubernetes.pvc_uid.is_some()
                })
        });
        permanent_fault || authorized_lag || (accepted_incarnation_missing && orphaned_storage)
    }) {
        let candidate = snapshot.replicas.iter().find(|(key, observation)| {
            key.replica_id == failed.identity.replica_id
                && key.instance_id != failed.identity.instance_id
                && observation.kubernetes.as_ref().is_some_and(|kubernetes| {
                    kubernetes.has_exact_scaffolding() && kubernetes.peer_endpoint_ready
                })
                && matches!(observation.agent, AgentObservation::Uninitialized(_))
        });
        let Some((_key, observation)) = candidate else {
            return Plan::Apply {
                changes: vec![KubernetesChange::EnsureReplacementScaffolding {
                    replica_id: failed.identity.replica_id,
                    replacing: failed.identity.clone(),
                }],
            };
        };
        let kubernetes = observation
            .kubernetes
            .as_ref()
            .expect("replacement candidate has exact scaffolding");
        let pod_uid = kubernetes
            .pod_uid
            .clone()
            .expect("replacement candidate has Pod UID");
        let pvc_uid = kubernetes
            .pvc_uid
            .clone()
            .expect("replacement candidate has PVC UID");
        let mut replacement_status = waiting_status(
            status,
            "ReplacementProvisioning",
            "Persisting one exact replacement outside authority",
        );
        replacement_status.provisioning = Some(ProvisioningIntent {
            replaces: failed.identity.clone(),
            operation_id: derive_replacement_operation_id(
                &snapshot.resource_uid,
                &failed.identity,
                &pod_uid,
                &pvc_uid,
            ),
            pod_uid,
            pvc_uid,
        });
        return Plan::Apply {
            changes: vec![KubernetesChange::PersistStatus {
                status: Box::new(replacement_status),
            }],
        };
    }

    if let Some(extra) = snapshot.replicas.iter().find_map(|(key, observation)| {
        let accepted = configuration.members.iter().any(|member| {
            member.identity.replica_id == key.replica_id
                && member.identity.instance_id == key.instance_id
        });
        (!accepted)
            .then_some(observation.kubernetes.as_ref())
            .flatten()
    }) && !recovering_service
    {
        return Plan::Apply {
            changes: vec![delete_scaffolding_change(extra)],
        };
    }

    let primary = configuration
        .members
        .iter()
        .find(|member| member.identity.replica_id == configuration.primary_id)
        .expect("validated configuration has primary member");
    let mut attested_members = 0_u32;
    let mut primary_report = None;
    for member in &configuration.members {
        let Some(report) = healthy_report(snapshot, &member.identity) else {
            continue;
        };
        let authority_matches = report.identity == member.identity
            && report.role == member.role
            && report.epoch == configuration.epoch
            && report.previous_configuration.is_none()
            && report
                .current_configuration
                .as_ref()
                .is_some_and(|current| current.configuration_id == configuration.configuration_id);
        if authority_matches {
            attested_members += 1;
        }

        if member.identity == primary.identity {
            primary_report = authority_matches.then_some(report);
        }
    }

    let Some(primary_report) = primary_report else {
        return Plan::Wait {
            reason: WaitReason::AwaitingStableEvidence,
            status: waiting_status(
                status,
                "PrimaryAuthorityUnproven",
                "The accepted primary has not attested its exact authority",
            ),
            requeue_after_seconds: config.wait_requeue_seconds,
        };
    };
    if attested_members < configuration.write_quorum {
        let marker_matches = status.quorum_loss.as_ref().is_some_and(|observation| {
            observation.configuration_id == configuration.configuration_id
        });
        if !marker_matches {
            status.quorum_loss = Some(QuorumLossObservation {
                configuration_id: configuration.configuration_id.clone(),
            });
            return Plan::Apply {
                changes: vec![KubernetesChange::PersistStatus {
                    status: Box::new(waiting_status(
                        status,
                        "NoWriteQuorum",
                        "Current Configuration write quorum is unavailable",
                    )),
                }],
            };
        }
        if primary_report.write_status != AccessStatus::NoWriteQuorum {
            return Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(Box::new(
                    ensure_configuration_command(
                        configuration,
                        primary,
                        policy,
                        OperationId::new(format!(
                            "availability:{}:no-write-quorum",
                            configuration.configuration_id
                        )),
                        AccessStatus::NoWriteQuorum,
                        false,
                    ),
                )),
            };
        }
        return Plan::Wait {
            reason: WaitReason::QuorumLoss,
            status: waiting_status(
                status,
                "NoWriteQuorum",
                "Writes remain closed until Current Configuration quorum returns",
            ),
            requeue_after_seconds: config.wait_requeue_seconds,
        };
    }

    if primary_report.write_status != AccessStatus::Granted {
        return Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(Box::new(ensure_configuration_command(
                configuration,
                primary,
                policy,
                OperationId::new(format!(
                    "availability:{}:grant-write",
                    configuration.configuration_id
                )),
                AccessStatus::Granted,
                false,
            ))),
        };
    }

    if status.quorum_loss.is_some() {
        status.quorum_loss = None;
        return Plan::Apply {
            changes: vec![KubernetesChange::PersistStatus {
                status: Box::new(waiting_status(
                    status,
                    "WriteQuorumRestored",
                    "Current Configuration quorum returned and writes are restored",
                )),
            }],
        };
    }

    if !snapshot.routing.service_present {
        return Plan::Apply {
            changes: vec![KubernetesChange::EnsureWriteRoutingService],
        };
    }
    if snapshot.routing.unresolved_write_target {
        return Plan::Apply {
            changes: vec![
                KubernetesChange::RemoveWriteRouting,
                KubernetesChange::PersistStatus {
                    status: Box::new(waiting_status(
                        status,
                        "RoutingFencePending",
                        "Removing unresolved write routing",
                    )),
                },
            ],
        };
    }

    match &snapshot.routing.write_target {
        Some(target) if target == &primary.identity => {}
        Some(_) => {
            return Plan::Apply {
                changes: vec![
                    KubernetesChange::RemoveWriteRouting,
                    KubernetesChange::PersistStatus {
                        status: Box::new(waiting_status(
                            status,
                            "RoutingFencePending",
                            "Removing routing to a non-authoritative target",
                        )),
                    },
                ],
            };
        }
        None => {
            return Plan::Apply {
                changes: vec![
                    KubernetesChange::PublishWriteRouting {
                        primary: primary.identity.clone(),
                    },
                    KubernetesChange::PersistStatus {
                        status: Box::new(waiting_status(
                            status,
                            "RoutingPublicationPending",
                            "Publishing routing to the attested primary",
                        )),
                    },
                ],
            };
        }
    }
    if let Some(plan) = begin_switchover(snapshot, &status, config) {
        return plan;
    }
    status = status.with_condition(ready_condition());
    Plan::Stable {
        status,
        requeue_after_seconds: config.stable_resync_seconds,
    }
}

fn project_switchover_request(
    snapshot: &ObservationSnapshot,
    status: AcceptedStatus,
) -> AcceptedStatus {
    let status = status.without_condition("SwitchoverRejected");
    let reject = |reason: &str, message: &str| {
        status.clone().with_condition(StatusCondition {
            type_: "SwitchoverRejected".to_string(),
            status: ConditionStatus::True,
            reason: reason.to_string(),
            message: message.to_string(),
        })
    };
    if let Some(intent) = snapshot
        .status
        .transition
        .as_ref()
        .and_then(|transition| transition.switchover.as_ref())
    {
        return if snapshot.desired.switchover.as_ref().is_some_and(|request| {
            request.request_id == intent.request_id
                && request.target_replica_id == intent.target.replica_id
        }) {
            status
        } else {
            reject(
                "ActiveRequestImmutable",
                "Cancellation or retargeting is rejected; the accepted switchover remains frozen",
            )
        };
    }
    let Some(request) = &snapshot.desired.switchover else {
        return status;
    };
    if request.request_id.as_str().trim().is_empty() || request.target_replica_id.value() <= 0 {
        return reject(
            "MalformedRequest",
            "Switchover requires a request ID and positive target ID",
        );
    }
    if let Some(receipt) = &status.last_switchover
        && receipt.request_id == request.request_id
    {
        return if receipt.requested_target_replica_id == request.target_replica_id {
            status
        } else {
            reject(
                "RequestIdReused",
                "A completed request ID cannot name a different target",
            )
        };
    }
    let Some(topology) = &status.topology else {
        return status;
    };
    let current = &topology.configuration;
    if request.target_replica_id == current.primary_id {
        return reject(
            "TargetAlreadyPrimary",
            "The requested target is already the accepted primary",
        );
    }
    let Some(target) = current
        .members
        .iter()
        .find(|member| member.identity.replica_id == request.target_replica_id)
    else {
        return reject(
            "TargetNotMember",
            "The requested target is not an accepted member",
        );
    };
    if target.role != ReplicaRole::ActiveSecondary
        || healthy_report(snapshot, &target.identity).is_none_or(|report| {
            !stable_member_report(report, target, current)
                || report.write_status == AccessStatus::Granted
                || report.prepared_switchover.is_some()
        })
    {
        return reject(
            "TargetNotEligible",
            "The exact accepted target must attest healthy, idle secondary authority",
        );
    }
    status
}

fn switchover_rejection(status: &AcceptedStatus) -> Option<&StatusCondition> {
    status
        .conditions
        .iter()
        .find(|condition| condition.type_ == "SwitchoverRejected")
}

fn stable_member_report(
    report: &crate::observation::AgentReport,
    member: &ConfigurationMember,
    configuration: &ConfigurationDescriptor,
) -> bool {
    report.identity == member.identity
        && report.role == member.role
        && report.epoch == configuration.epoch
        && report.previous_configuration.is_none()
        && report.current_configuration.as_ref() == Some(configuration)
        && report.pending_operation_id.is_none()
}

fn begin_switchover(
    snapshot: &ObservationSnapshot,
    status: &AcceptedStatus,
    config: &EvaluationConfig,
) -> Option<Plan> {
    let request = snapshot.desired.switchover.as_ref()?;
    if status
        .conditions
        .iter()
        .any(|condition| condition.type_ == "SwitchoverRejected")
        || status
            .last_switchover
            .as_ref()
            .is_some_and(|receipt| receipt.request_id == request.request_id)
        || status.primary_failure.is_some()
        || status.quorum_loss.is_some()
    {
        return None;
    }
    let previous = &status.topology.as_ref()?.configuration;
    if !previous.members.iter().all(|member| {
        healthy_report(snapshot, &member.identity).is_some_and(|report| {
            stable_member_report(report, member, previous) && report.prepared_switchover.is_none()
        })
    }) {
        return Some(Plan::Wait {
            reason: WaitReason::AwaitingStableEvidence,
            status: waiting_status(
                status.clone(),
                "SwitchoverAwaitingStableAuthority",
                "Every exact member must finish accepted authority before planned movement",
            ),
            requeue_after_seconds: config.wait_requeue_seconds,
        });
    }
    let Some(configuration_number) = previous.epoch.configuration_number.checked_add(1) else {
        return Some(Plan::Stable {
            status: status.clone().with_condition(StatusCondition {
                type_: "SwitchoverRejected".to_string(),
                status: ConditionStatus::True,
                reason: "ConfigurationEpochExhausted".to_string(),
                message: "A strictly newer configuration epoch cannot be allocated".to_string(),
            }),
            requeue_after_seconds: config.stable_resync_seconds,
        });
    };
    let source = configuration_primary(previous).identity.clone();
    let target = previous
        .members
        .iter()
        .find(|member| member.identity.replica_id == request.target_replica_id)?
        .identity
        .clone();
    let members = previous
        .members
        .iter()
        .map(|member| ConfigurationMember {
            identity: member.identity.clone(),
            role: if member.identity == target {
                ReplicaRole::Primary
            } else if member.identity == source {
                ReplicaRole::ActiveSecondary
            } else {
                member.role
            },
        })
        .collect();
    let current = ConfigurationDescriptor::new(
        Epoch::new(previous.epoch.data_loss_number, configuration_number),
        target.replica_id,
        members,
        previous.write_quorum,
    );
    let mut status = waiting_status(
        status.clone(),
        "SwitchoverAccepted",
        "Frozen the exact source, target, and requested authority before revoking routing",
    );
    status.transition = Some(TransitionIntent {
        transition_id: derive_transition_id(
            &snapshot.resource_uid,
            TransitionKind::PlannedSwitchover,
            &current.configuration_id,
        ),
        kind: TransitionKind::PlannedSwitchover,
        spec_generation: snapshot.desired.generation,
        effective_policy: status.effective_policy.clone().expect("stable policy"),
        previous_configuration_id: Some(previous.configuration_id.clone()),
        current_configuration: current.clone(),
        election_lsn: None,
        build_id: None,
        repair: None,
        switchover: Some(PlannedSwitchoverIntent {
            preparation_generation: snapshot.desired.generation,
            request_id: request.request_id.clone(),
            source,
            target,
            requested_configuration: current,
            resolution: PlannedSwitchoverResolution::RequestedTarget,
            handoff: None,
        }),
    });
    Some(Plan::Apply {
        changes: vec![KubernetesChange::PersistStatus {
            status: Box::new(status),
        }],
    })
}

fn switchover_wait(
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

fn exact_pod_absent(snapshot: &ObservationSnapshot, identity: &ReplicaIdentity) -> bool {
    snapshot.observation_failures.is_empty()
        && !snapshot.replicas.values().any(|observation| {
            observation.kubernetes.as_ref().is_some_and(|pod| {
                pod.pod_uid
                    .as_ref()
                    .is_some_and(|uid| uid.as_str() == identity.instance_id.as_str())
            })
        })
}

fn exact_report<'a>(
    snapshot: &'a ObservationSnapshot,
    identity: &ReplicaIdentity,
) -> Option<&'a crate::observation::AgentReport> {
    match &snapshot.observation_for_identity(identity)?.agent {
        AgentObservation::Report(report) if report.identity == *identity => Some(report),
        _ => None,
    }
}

fn definitively_lost(snapshot: &ObservationSnapshot, identity: &ReplicaIdentity) -> bool {
    exact_pod_absent(snapshot, identity)
        || exact_report(snapshot, identity)
            .is_some_and(|report| report.reported_fault == Some(crate::types::FaultType::Permanent))
}

fn switchover_unsafe(
    snapshot: &ObservationSnapshot,
    message: &str,
    config: &EvaluationConfig,
) -> Plan {
    let mut status = snapshot.status.clone();
    let Some(intent) = status
        .transition
        .as_mut()
        .and_then(|transition| transition.switchover.as_mut())
    else {
        return unsafe_plan(
            status,
            UnsafeReason::InvalidAcceptedAuthority(message.to_string()),
            config,
        );
    };
    // Persist the failure decision before destructive safety effects; disappearance of
    // contradictory evidence must never resume authority work after a controller restart.
    if intent.resolution != PlannedSwitchoverResolution::Unsafe {
        intent.resolution = PlannedSwitchoverResolution::Unsafe;
        status = status.with_condition(StatusCondition {
            type_: "SwitchoverSafety".to_string(),
            status: ConditionStatus::True,
            reason: "ClosureRequired".to_string(),
            message: message.to_string(),
        });
        return Plan::Apply {
            changes: vec![
                KubernetesChange::RemoveWriteRouting,
                KubernetesChange::PersistStatus {
                    status: Box::new(waiting_status(status, "SwitchoverSafetyClosure", message)),
                },
            ],
        };
    }
    if snapshot.routing.write_target.is_some() || snapshot.routing.unresolved_write_target {
        return Plan::Apply {
            changes: vec![KubernetesChange::RemoveWriteRouting],
        };
    }
    if !snapshot.observation_failures.is_empty() {
        return switchover_wait(
            status,
            "SwitchoverSafetyObservationRequired",
            "Complete Pod and routing observations are required to prove every possible writer closed or absent",
            config,
        );
    }
    let starting = snapshot
        .status
        .topology
        .as_ref()
        .map(|topology| &topology.configuration);
    for (key, observation) in &snapshot.replicas {
        let Some(pod) = observation
            .kubernetes
            .as_ref()
            .filter(|pod| pod.pod_uid.is_some())
        else {
            continue;
        };
        let uid = pod.pod_uid.as_ref().unwrap();
        let report = match &observation.agent {
            AgentObservation::Report(report)
                if report.protocol_version == config.supported_protocol_version
                    && report.resource_uid == snapshot.resource_uid
                    && report.identity.instance_id.as_str() == uid.as_str()
                    && report.identity.replica_id == key.replica_id
                    && !report.process_session_id.is_empty()
                    && crate::validation::validate_report_internal(report).is_ok()
                    && report
                        .previous_configuration
                        .as_ref()
                        .is_none_or(|previous| {
                            crate::validation::validate_configuration(previous, None).is_ok()
                        })
                    && snapshot
                        .previous_report_watermarks
                        .get(key)
                        .is_none_or(|watermark| {
                            watermark.process_session_id != report.process_session_id
                                || report.report_sequence > watermark.report_sequence
                        })
                    && report
                        .current_configuration
                        .as_ref()
                        .is_some_and(|current| {
                            crate::validation::validate_configuration(current, None).is_ok()
                                && report.epoch == current.epoch
                                && starting.is_some_and(|accepted| current.epoch >= accepted.epoch)
                                && current
                                    .members
                                    .iter()
                                    .any(|member| member.identity == report.identity)
                        }) =>
            {
                Some(report)
            }
            _ => None,
        };
        if let Some(report) = report {
            if report.write_status != AccessStatus::Granted && report.pending_operation_id.is_none()
            {
                continue;
            }
            if let Some(starting) = starting
                && report.current_configuration.as_ref() == Some(starting)
                && report.previous_configuration.is_none()
                && report.pending_operation_id.is_none()
                && report.role == ReplicaRole::Primary
                && report.identity == configuration_primary(starting).identity
                && let Some(policy) = status.effective_policy.as_ref()
            {
                return Plan::Execute {
                    command: ProtocolCommand::EnsureConfiguration(Box::new(
                        ensure_configuration_command(
                            starting,
                            configuration_primary(starting),
                            policy,
                            OperationId::new(format!(
                                "{}:unsafe-close",
                                status.transition.as_ref().unwrap().transition_id
                            )),
                            AccessStatus::ReconfigurationPending,
                            false,
                        ),
                    )),
                };
            }
        }
        return Plan::Apply {
            changes: vec![KubernetesChange::DeleteExactPod {
                pod_name: pod.pod_name.clone(),
                pod_uid: uid.clone(),
            }],
        };
    }
    let intent = status
        .transition
        .as_ref()
        .unwrap()
        .switchover
        .as_ref()
        .unwrap();
    status.last_switchover = Some(PlannedSwitchoverReceipt {
        request_id: intent.request_id.clone(),
        requested_target_replica_id: intent.target.replica_id,
        accepted_target: Some(intent.target.clone()),
        resulting_primary: None,
        outcome: PlannedSwitchoverOutcome::Unsafe,
    });
    unsafe_plan(
        status,
        UnsafeReason::ContradictoryReplicaEvidence(message.to_string()),
        config,
    )
}

fn recover_switchover(
    snapshot: &ObservationSnapshot,
    transition: &TransitionIntent,
    config: &EvaluationConfig,
) -> Option<Plan> {
    let intent = transition.switchover.as_ref().unwrap();
    let starting = &snapshot.status.topology.as_ref().unwrap().configuration;
    let wait = |reason, message| switchover_wait(snapshot.status.clone(), reason, message, config);
    if definitively_lost(snapshot, &intent.source) {
        return Some(switchover_unsafe(
            snapshot,
            "The exact source and its retained handoff proof are lost",
            config,
        ));
    }
    if intent.resolution == PlannedSwitchoverResolution::CompensatingOldPrimary {
        let surviving = surviving_exact_members(snapshot, starting);
        if surviving < transition.effective_policy.write_quorum as usize {
            return Some(switchover_unsafe(
                snapshot,
                "Exact retained membership can no longer provide a compensation write quorum",
                config,
            ));
        }
        if surviving < transition.effective_policy.read_quorum as usize {
            return Some(switchover_unsafe(
                snapshot,
                "Exact retained membership can no longer provide a compensation read quorum",
                config,
            ));
        }
        if healthy_report(snapshot, &intent.source).is_some_and(|source| {
            intent
                .handoff
                .as_ref()
                .is_some_and(|handoff| source.current_progress < handoff.handoff_lsn)
        }) {
            return Some(switchover_unsafe(
                snapshot,
                "Compensation source lost its certified durable prefix",
                config,
            ));
        }
        return None;
    }
    if intent.resolution != PlannedSwitchoverResolution::RestoringOldPrimary
        && !definitively_lost(snapshot, &intent.target)
    {
        return None;
    }
    let Some(source) = healthy_report(snapshot, &intent.source) else {
        return Some(wait(
            "SwitchoverRecoverySourceUnavailable",
            "Re-observe the exact source and its current process session before selecting recovery authority",
        ));
    };
    let admitted = starting
        .members
        .iter()
        .filter_map(|member| exact_report(snapshot, &member.identity))
        .any(|report| report.epoch >= intent.requested_configuration.epoch);
    if !admitted {
        // Source-first installation means a missing target cannot have admitted
        // requested authority while the exact source still attests idle starting authority.
        let starting_evidence = starting.members.iter().all(|member| {
            exact_pod_absent(snapshot, &member.identity)
                || exact_report(snapshot, &member.identity).is_some_and(|report| {
                    stable_member_report(report, member, starting)
                        && (member.identity == intent.source
                            || report.write_status != AccessStatus::Granted)
                })
        });
        if !starting_evidence {
            return Some(wait(
                "SwitchoverAuthorityAdmissionUnknown",
                "Re-observe idle starting authority on every extant exact member; missing or pending evidence cannot authorize old-epoch restoration",
            ));
        }
        if intent.resolution != PlannedSwitchoverResolution::RestoringOldPrimary {
            let mut status = snapshot.status.clone();
            status
                .transition
                .as_mut()
                .unwrap()
                .switchover
                .as_mut()
                .unwrap()
                .resolution = PlannedSwitchoverResolution::RestoringOldPrimary;
            return Some(Plan::Apply {
                changes: vec![KubernetesChange::PersistStatus {
                    status: Box::new(waiting_status(
                        status,
                        "SwitchoverRestoring",
                        "Target completion is impossible before authority admission; retiring exact preparation at starting authority",
                    )),
                }],
            });
        }
        if let Some(handoff) = source.prepared_switchover.as_ref() {
            let expected_id = derive_switchover_preparation_operation_id(
                &snapshot.resource_uid,
                &intent.request_id,
                intent.preparation_generation,
                &starting.configuration_id,
                &intent.source,
                &intent.target,
            );
            if handoff.preparation_operation_id != expected_id
                || handoff.preparation_generation != intent.preparation_generation
                || handoff.request_id != intent.request_id
                || handoff.source != intent.source
                || handoff.target != intent.target
                || handoff.starting_epoch != starting.epoch
                || handoff.starting_configuration_id != starting.configuration_id
                || intent
                    .handoff
                    .as_ref()
                    .is_some_and(|retained| retained != handoff)
                || source.write_status == AccessStatus::Granted
            {
                return Some(switchover_unsafe(
                    snapshot,
                    "Restoration certificate contradicts frozen preparation",
                    config,
                ));
            }
            let mut command = ensure_configuration_command(
                starting,
                configuration_primary(starting),
                &transition.effective_policy,
                OperationId::new(format!("{}:restore", transition.transition_id)),
                AccessStatus::ReconfigurationPending,
                false,
            );
            command.transition_kind = TransitionKind::PlannedSwitchover;
            command.switchover_handoff = Some(handoff.clone());
            command.retire_switchover_preparation_ids = vec![handoff.preparation()];
            return Some(Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(Box::new(command)),
            });
        }
        let retirement_operation =
            OperationId::new(format!("{}:restore", transition.transition_id));
        if source.retained_operation_id.as_ref() != Some(&retirement_operation) {
            if intent.handoff.is_some() {
                return Some(wait(
                    "SwitchoverRetirementPending",
                    "Re-observe the exact restoration receipt before clearing the frozen operation",
                ));
            }
            // Even a preparation whose dispatch never returned may still arrive.
            // Retire its exact ID before clearing intent, without inventing a handoff boundary.
            let mut command = ensure_configuration_command(
                starting,
                configuration_primary(starting),
                &transition.effective_policy,
                retirement_operation,
                AccessStatus::ReconfigurationPending,
                false,
            );
            command.transition_kind = TransitionKind::PlannedSwitchover;
            command.retire_switchover_preparation_ids =
                vec![crate::types::SwitchoverPreparationId {
                    generation: intent.preparation_generation,
                    operation_id: derive_switchover_preparation_operation_id(
                        &snapshot.resource_uid,
                        &intent.request_id,
                        intent.preparation_generation,
                        &starting.configuration_id,
                        &intent.source,
                        &intent.target,
                    ),
                }];
            return Some(Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(Box::new(command)),
            });
        }
        let mut status = snapshot.status.clone();
        status.transition = None;
        status.observed_generation = transition.spec_generation;
        status.last_switchover = Some(PlannedSwitchoverReceipt {
            request_id: intent.request_id.clone(),
            requested_target_replica_id: intent.target.replica_id,
            accepted_target: Some(intent.target.clone()),
            resulting_primary: Some(intent.source.clone()),
            outcome: PlannedSwitchoverOutcome::OldPrimaryRestored,
        });
        return Some(Plan::Apply {
            changes: vec![KubernetesChange::PersistStatus {
                status: Box::new(waiting_status(
                    status,
                    "SwitchoverRestored",
                    "Starting authority and exact retirement accepted; stable convergence may restore source service",
                )),
            }],
        });
    }
    let Some(handoff) = intent.handoff.as_ref() else {
        return Some(switchover_unsafe(
            snapshot,
            "Newer authority exists without a frozen handoff certificate",
            config,
        ));
    };
    let retired = switchover_member_complete(
        source,
        transition,
        intent
            .requested_configuration
            .members
            .iter()
            .find(|member| member.identity == intent.source)
            .unwrap(),
        starting,
        true,
    );
    if source.prepared_switchover.as_ref() != Some(handoff) && !retired {
        return Some(switchover_unsafe(
            snapshot,
            "The source cannot prove the whole retained handoff certificate",
            config,
        ));
    }
    if source.write_status == AccessStatus::Granted || source.current_progress < handoff.handoff_lsn
    {
        return Some(switchover_unsafe(
            snapshot,
            "The source cannot safely retain the frozen write prefix",
            config,
        ));
    }
    let reports = starting
        .members
        .iter()
        .filter_map(|member| healthy_report(snapshot, &member.identity))
        .filter(|report| {
            report.write_status != AccessStatus::Granted
                && report
                    .current_configuration
                    .as_ref()
                    .is_some_and(|current| {
                        current == starting || current == &intent.requested_configuration
                    })
        })
        .collect::<Vec<_>>();
    let surviving = surviving_exact_members(snapshot, starting);
    if surviving < transition.effective_policy.write_quorum as usize {
        return Some(switchover_unsafe(
            snapshot,
            "Exact retained membership cannot provide a compensation write quorum",
            config,
        ));
    }
    if !configuration_read_quorum(starting, &reports, transition.effective_policy.read_quorum) {
        if surviving < transition.effective_policy.read_quorum as usize {
            return Some(switchover_unsafe(
                snapshot,
                "Exact retained membership cannot provide a compensation read quorum",
                config,
            ));
        }
        return Some(wait(
            "SwitchoverCompensationReadQuorumPending",
            "Re-observe a read quorum of exact write-closed members under starting or requested authority",
        ));
    }
    let Some(number) = intent
        .requested_configuration
        .epoch
        .configuration_number
        .checked_add(1)
    else {
        return Some(switchover_unsafe(
            snapshot,
            "No strictly newer compensation epoch is available",
            config,
        ));
    };
    let current = ConfigurationDescriptor::new(
        Epoch::new(starting.epoch.data_loss_number, number),
        intent.source.replica_id,
        starting.members.clone(),
        starting.write_quorum,
    );
    let mut status = snapshot.status.clone();
    let transition = status.transition.as_mut().unwrap();
    transition.transition_id = derive_transition_id(
        &snapshot.resource_uid,
        TransitionKind::PlannedSwitchover,
        &current.configuration_id,
    );
    transition.current_configuration = current;
    transition.switchover.as_mut().unwrap().resolution =
        PlannedSwitchoverResolution::CompensatingOldPrimary;
    Some(Plan::Apply {
        changes: vec![KubernetesChange::PersistStatus {
            status: Box::new(waiting_status(
                status,
                "SwitchoverCompensationAllocated",
                "Frozen a strictly newer compensation epoch without changing data-loss authority, policy, or exact membership",
            )),
        }],
    })
}

fn evaluate_switchover(
    snapshot: &ObservationSnapshot,
    transition: &TransitionIntent,
    config: &EvaluationConfig,
) -> Plan {
    let mut status = project_switchover_request(snapshot, snapshot.status.clone());
    if switchover_rejection(&status) != switchover_rejection(&snapshot.status) {
        return Plan::Apply {
            changes: vec![KubernetesChange::PersistStatus {
                status: Box::new(status),
            }],
        };
    }
    let (_, unsupported) = desired_spec_state(
        snapshot,
        &transition.current_configuration,
        &transition.effective_policy,
    );
    status = status.without_condition("UnsupportedSpec");
    if let Some(condition) = unsupported {
        status = status.with_condition(condition);
    }
    let wait = |reason: &str, message: &str| Plan::Wait {
        reason: WaitReason::ActiveTransition,
        status: waiting_status(status.clone(), reason, message),
        requeue_after_seconds: config.wait_requeue_seconds,
    };
    if snapshot.routing.write_target.is_some() || snapshot.routing.unresolved_write_target {
        return Plan::Apply {
            changes: vec![
                KubernetesChange::RemoveWriteRouting,
                KubernetesChange::PersistStatus {
                    status: Box::new(waiting_status(
                        status,
                        "SwitchoverRoutingRemoved",
                        "Removing write routing before old-primary preparation",
                    )),
                },
            ],
        };
    }
    let intent = transition
        .switchover
        .as_ref()
        .expect("validated switchover intent");
    if let Some(plan) = recover_switchover(snapshot, transition, config) {
        return plan;
    }
    let starting = &snapshot
        .status
        .topology
        .as_ref()
        .expect("accepted topology")
        .configuration;
    let compensating = intent.resolution == PlannedSwitchoverResolution::CompensatingOldPrimary;
    let previous = if compensating {
        &intent.requested_configuration
    } else {
        starting
    };
    let current = &transition.current_configuration;
    let preparation_id = derive_switchover_preparation_operation_id(
        &snapshot.resource_uid,
        &intent.request_id,
        intent.preparation_generation,
        &starting.configuration_id,
        &intent.source,
        &intent.target,
    );
    let Some(source_report) = healthy_report(snapshot, &intent.source) else {
        return wait(
            "SwitchoverSourceUnavailable",
            "Waiting for the exact source's fresh authority evidence",
        );
    };
    if !compensating && healthy_report(snapshot, &intent.target).is_none() {
        return wait(
            "SwitchoverTargetUnavailable",
            "Waiting for the exact target's current-session evidence; temporary unavailability does not select recovery",
        );
    }
    if !compensating {
        for member in &starting.members {
            if definitively_lost(snapshot, &member.identity) {
                return switchover_unsafe(
                    snapshot,
                    "An exact participant required by the frozen requested topology is definitively lost",
                    config,
                );
            }
            if healthy_report(snapshot, &member.identity).is_none() {
                return wait(
                    "SwitchoverMemberUnavailable",
                    "Re-observe every exact participant before admitting further requested authority",
                );
            }
        }
    }
    let source = configuration_primary(starting);
    let Some(handoff) = &intent.handoff else {
        if !stable_member_report(source_report, source, previous) {
            return wait(
                "SwitchoverPreparationPending",
                "Waiting for idle starting authority on the exact source",
            );
        }
        if let Some(handoff) = &source_report.prepared_switchover {
            if handoff.preparation_operation_id != preparation_id
                || handoff.preparation_generation != intent.preparation_generation
                || handoff.request_id != intent.request_id
                || handoff.source != intent.source
                || handoff.target != intent.target
                || handoff.starting_configuration_id != previous.configuration_id
                || handoff.starting_epoch != previous.epoch
                || handoff.handoff_lsn > source_report.current_progress
                || source_report.write_status == AccessStatus::Granted
            {
                return switchover_unsafe(
                    snapshot,
                    "Prepared switchover differs from the frozen operation",
                    config,
                );
            }
            status
                .transition
                .as_mut()
                .unwrap()
                .switchover
                .as_mut()
                .unwrap()
                .handoff = Some(handoff.clone());
            return Plan::Apply {
                changes: vec![KubernetesChange::PersistStatus {
                    status: Box::new(waiting_status(
                        status,
                        "SwitchoverPrepared",
                        "Persisted the old primary's durable write handoff certificate",
                    )),
                }],
            };
        }
        if source_report.write_status != AccessStatus::Granted {
            return wait(
                "SwitchoverPreparationPending",
                "Waiting for retained write-revocation evidence",
            );
        }
        return Plan::Execute {
            command: ProtocolCommand::PrepareSwitchover(Box::new(PrepareSwitchover {
                preparation_generation: intent.preparation_generation,
                operation_id: preparation_id,
                request_id: intent.request_id.clone(),
                source: intent.source.clone(),
                target: intent.target.clone(),
                current_configuration: previous.clone(),
                local_replica_id: intent.source.replica_id,
                expected_instance_id: intent.source.instance_id.clone(),
                expected_agent_generation: intent.source.agent_generation.clone(),
            })),
        };
    };
    let source_retired = switchover_member_complete(
        source_report,
        transition,
        current
            .members
            .iter()
            .find(|member| member.identity == intent.source)
            .unwrap(),
        previous,
        true,
    );
    let mut requested = transition.clone();
    requested.current_configuration = intent.requested_configuration.clone();
    requested.transition_id = derive_transition_id(
        &snapshot.resource_uid,
        TransitionKind::PlannedSwitchover,
        &intent.requested_configuration.configuration_id,
    );
    let source_requested_retired = compensating
        && switchover_member_complete(
            source_report,
            &requested,
            requested
                .current_configuration
                .members
                .iter()
                .find(|member| member.identity == intent.source)
                .unwrap(),
            starting,
            true,
        );
    let compensation_admitted =
        compensating && source_report.current_configuration.as_ref() == Some(current);
    if compensating && surviving_exact_members(snapshot, current) < current.write_quorum as usize {
        return switchover_unsafe(
            snapshot,
            "Exact compensation membership can no longer provide write quorum",
            config,
        );
    }
    if source_report.write_status == AccessStatus::Granted
        || (!source_retired
            && !source_requested_retired
            && !compensation_admitted
            && source_report.prepared_switchover.as_ref() != Some(handoff))
    {
        return switchover_unsafe(
            snapshot,
            "The source must retain the whole exact preparation and remain write-closed until retirement",
            config,
        );
    }
    let primary_identity = &configuration_primary(current).identity;
    let Some(target_report) = healthy_report(snapshot, primary_identity) else {
        return wait(
            "SwitchoverTargetUnavailable",
            "Waiting for the exact requested target",
        );
    };
    if !compensating
        && target_report.epoch == previous.epoch
        && (!stable_member_report(
            target_report,
            previous
                .members
                .iter()
                .find(|member| member.identity == intent.target)
                .unwrap(),
            previous,
        ) || target_report
            .verified_replication_lsn
            .is_none_or(|lsn| lsn < handoff.handoff_lsn))
    {
        return wait(
            "SwitchoverTargetCatchupPending",
            "The exact target must verify the handoff prefix under starting authority; raw progress is insufficient",
        );
    }
    let target = configuration_primary(current);
    let mut ordered = current
        .members
        .iter()
        .filter(|member| member.identity != target.identity)
        .collect::<Vec<_>>();
    ordered.sort_by_key(|member| (member.identity != intent.source, member.identity.replica_id));
    ordered.push(target);

    if compensating {
        for member in &ordered {
            if member.identity == *primary_identity || exact_pod_absent(snapshot, &member.identity)
            {
                continue;
            }
            if exact_report(snapshot, &member.identity).is_some_and(|report| {
                report.reported_fault == Some(crate::types::FaultType::Permanent)
            }) {
                let Some(pod) = snapshot
                    .observation_for_identity(&member.identity)
                    .and_then(|observation| observation.kubernetes.as_ref())
                    .filter(|pod| {
                        pod.pod_uid
                            .as_ref()
                            .is_some_and(|uid| uid.as_str() == member.identity.instance_id.as_str())
                    })
                else {
                    return wait(
                        "SwitchoverFaultedPodObservationRequired",
                        "Re-observe the exact faulted Pod before safety fencing",
                    );
                };
                return Plan::Apply {
                    changes: vec![KubernetesChange::DeleteExactPod {
                        pod_name: pod.pod_name.clone(),
                        pod_uid: pod.pod_uid.clone().unwrap(),
                    }],
                };
            }
        }
    }

    let mut pc_cc_reports = Vec::new();
    let mut current_only_started = false;
    for member in &ordered {
        if compensating
            && member.identity != *primary_identity
            && exact_pod_absent(snapshot, &member.identity)
        {
            continue;
        }
        let Some(report) = (if compensating && member.identity != *primary_identity {
            exact_report(snapshot, &member.identity)
        } else {
            healthy_report(snapshot, &member.identity)
        }) else {
            return wait(
                "SwitchoverMemberUnavailable",
                "Every exact member must attest requested authority",
            );
        };
        let current_only = report.epoch == current.epoch
            && report.current_configuration.as_ref() == Some(current)
            && report.previous_configuration.is_none();
        if current_only {
            current_only_started = true;
            continue;
        }
        if !switchover_member_complete(report, transition, member, previous, false) {
            return Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(Box::new(
                    switchover_configuration_command(transition, previous, member, false),
                )),
            };
        }
        pc_cc_reports.push(report);
    }
    if !current_only_started
        && (!configuration_report_quorum(previous, &pc_cc_reports)
            || !configuration_report_quorum(current, &pc_cc_reports))
    {
        return wait(
            "SwitchoverJointQuorumPending",
            "Waiting for exact PC/CC report quorums",
        );
    }
    if !target_report.catch_up_complete
        || target_report
            .verified_replication_lsn
            .is_none_or(|lsn| lsn < handoff.handoff_lsn)
        || (!current_only_started
            && (target_report.catch_up_boundary != Some(handoff.handoff_lsn)
                || target_report.current_configuration_quorum_progress < handoff.handoff_lsn))
    {
        return wait(
            "SwitchoverRequestedCatchupPending",
            "The requested primary must prove certified catch-up while write-closed",
        );
    }
    for member in &ordered {
        if compensating
            && member.identity != *primary_identity
            && exact_pod_absent(snapshot, &member.identity)
        {
            continue;
        }
        let report = exact_report(snapshot, &member.identity).expect("all members observed above");
        if !switchover_member_complete(report, transition, member, previous, true) {
            return Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(Box::new(
                    switchover_configuration_command(transition, previous, member, true),
                )),
            };
        }
    }
    status.topology = Some(AcceptedTopology {
        configuration: current.clone(),
    });
    status.observed_generation = transition.spec_generation;
    status.transition = None;
    status.primary_failure = None;
    status.quorum_loss = None;
    status.last_switchover = Some(PlannedSwitchoverReceipt {
        request_id: intent.request_id.clone(),
        requested_target_replica_id: intent.target.replica_id,
        accepted_target: Some(intent.target.clone()),
        resulting_primary: Some(primary_identity.clone()),
        outcome: if compensating {
            PlannedSwitchoverOutcome::OldPrimaryCompensated
        } else {
            PlannedSwitchoverOutcome::RequestedTargetCompleted
        },
    });
    Plan::Apply {
        changes: vec![KubernetesChange::PersistStatus {
            status: Box::new(waiting_status(
                status,
                "SwitchoverCompleted",
                "Accepted requested current-only authority; stable convergence will grant writes and publish routing",
            )),
        }],
    }
}

fn switchover_member_complete(
    report: &crate::observation::AgentReport,
    transition: &TransitionIntent,
    member: &ConfigurationMember,
    previous: &ConfigurationDescriptor,
    current_only: bool,
) -> bool {
    let operation_id = switchover_operation_id(transition, member, current_only);
    report.identity == member.identity
        && report.role == member.role
        && report.epoch == transition.current_configuration.epoch
        && report.current_configuration.as_ref() == Some(&transition.current_configuration)
        && report.previous_configuration.as_ref() == (!current_only).then_some(previous)
        && report.write_status != AccessStatus::Granted
        && report.pending_operation_id.is_none()
        && report.retained_operation_id.as_ref() == Some(&operation_id)
        && (!current_only || report.prepared_switchover.is_none())
}

fn switchover_operation_id(
    transition: &TransitionIntent,
    member: &ConfigurationMember,
    current_only: bool,
) -> OperationId {
    OperationId::new(format!(
        "{}:{}:{}",
        transition.transition_id,
        if current_only {
            "current-only"
        } else {
            "pc-cc"
        },
        member.identity.replica_id
    ))
}

fn switchover_configuration_command(
    transition: &TransitionIntent,
    previous: &ConfigurationDescriptor,
    member: &ConfigurationMember,
    current_only: bool,
) -> EnsureConfiguration {
    let handoff = transition
        .switchover
        .as_ref()
        .unwrap()
        .handoff
        .as_ref()
        .unwrap();
    EnsureConfiguration {
        operation_id: switchover_operation_id(transition, member, current_only),
        previous_configuration: (!current_only).then(|| previous.clone()),
        current_configuration: transition.current_configuration.clone(),
        previous_epoch: (!current_only).then_some(previous.epoch),
        current_epoch: transition.current_configuration.epoch,
        effective_policy: transition.effective_policy.clone(),
        local_replica_id: member.identity.replica_id,
        expected_instance_id: member.identity.instance_id.clone(),
        expected_agent_generation: member.identity.agent_generation.clone(),
        transition_kind: TransitionKind::PlannedSwitchover,
        failover_safe_lsn: None,
        primary_write_status: AccessStatus::ReconfigurationPending,
        current_only,
        retire_build_ids: Vec::new(),
        switchover_handoff: Some(handoff.clone()),
        retire_switchover_preparation_ids: if current_only && member.identity == handoff.source {
            vec![handoff.preparation()]
        } else {
            Vec::new()
        },
    }
}

fn maybe_begin_stable_failover(
    snapshot: &ObservationSnapshot,
    mut status: AcceptedStatus,
    config: &EvaluationConfig,
) -> Option<Plan> {
    let accepted = &snapshot
        .status
        .topology
        .as_ref()
        .expect("initialized status has accepted topology")
        .configuration;
    let primary = configuration_primary(accepted);
    if !replica_failed(snapshot, &primary.identity) {
        if status.primary_failure.is_some() {
            status.primary_failure = None;
            return Some(Plan::Apply {
                changes: vec![KubernetesChange::PersistStatus {
                    status: Box::new(waiting_status(
                        status,
                        "PrimaryRecovered",
                        "The accepted primary recovered before failover authority was allocated",
                    )),
                }],
            });
        }
        return None;
    }

    if status
        .primary_failure
        .as_ref()
        .is_none_or(|failure| failure.primary != primary.identity)
    {
        status.primary_failure = Some(PrimaryFailureObservation {
            primary: primary.identity.clone(),
            started_at_unix_seconds: snapshot.now_unix_seconds,
        });
        status.quorum_loss = None;
        let mut changes = Vec::new();
        if snapshot.routing.write_target.is_some() || snapshot.routing.unresolved_write_target {
            changes.push(KubernetesChange::RemoveWriteRouting);
        }
        changes.push(KubernetesChange::PersistStatus {
            status: Box::new(waiting_status(
                status,
                "PrimaryFailureObserved",
                "Persisted exact primary failure observation and fenced write routing",
            )),
        });
        return Some(Plan::Apply { changes });
    }

    if snapshot.routing.write_target.is_some() || snapshot.routing.unresolved_write_target {
        return Some(Plan::Apply {
            changes: vec![
                KubernetesChange::RemoveWriteRouting,
                KubernetesChange::PersistStatus {
                    status: Box::new(waiting_status(
                        status,
                        "RoutingFencePending",
                        "Removing routing to the failed primary before failover",
                    )),
                },
            ],
        });
    }

    let policy = snapshot
        .status
        .effective_policy
        .as_ref()
        .expect("initialized status has policy");
    let failure = status
        .primary_failure
        .as_ref()
        .expect("matching primary failure observation exists");
    let delay = i64::try_from(policy.failover_delay_seconds).unwrap_or(i64::MAX);
    if snapshot.now_unix_seconds < failure.started_at_unix_seconds.saturating_add(delay) {
        return Some(Plan::Wait {
            reason: WaitReason::FailoverDelay,
            status: waiting_status(
                status,
                "FailoverDelay",
                "Waiting for the frozen primary-failure delay",
            ),
            requeue_after_seconds: config.wait_requeue_seconds,
        });
    }

    let reports = available_union_reports(snapshot, accepted, accepted);
    if !configuration_read_quorum(accepted, &reports, policy.read_quorum) {
        if configuration_cannot_regain_read_quorum(snapshot, accepted, policy.read_quorum) {
            return Some(unsafe_plan(
                status,
                UnsafeReason::ContradictoryReplicaEvidence(
                    "ordinary recovery would require abandoning accepted configuration quorum"
                        .to_string(),
                ),
                config,
            ));
        }
        if status
            .quorum_loss
            .as_ref()
            .is_none_or(|quorum_loss| quorum_loss.configuration_id != accepted.configuration_id)
        {
            status.quorum_loss = Some(QuorumLossObservation {
                configuration_id: accepted.configuration_id.clone(),
            });
            return Some(Plan::Apply {
                changes: vec![KubernetesChange::PersistStatus {
                    status: Box::new(waiting_status(
                        status,
                        "FailoverReadQuorumUnavailable",
                        "Failover remains write-closed until Current Configuration read quorum is observed",
                    )),
                }],
            });
        }
        return Some(Plan::Wait {
            reason: WaitReason::QuorumLoss,
            status: waiting_status(
                status,
                "FailoverReadQuorumUnavailable",
                "Failover remains write-closed until Current Configuration read quorum is observed",
            ),
            requeue_after_seconds: config.wait_requeue_seconds,
        });
    }

    let candidate = select_failover_candidate(accepted, &reports)?;
    let current = configuration_with_primary(
        accepted,
        &candidate.identity,
        Epoch::new(
            accepted.epoch.data_loss_number,
            accepted.epoch.configuration_number + 1,
        ),
    );
    status.transition = Some(TransitionIntent {
        transition_id: derive_transition_id(
            &snapshot.resource_uid,
            TransitionKind::Failover,
            &current.configuration_id,
        ),
        kind: TransitionKind::Failover,
        spec_generation: snapshot.status.observed_generation,
        effective_policy: policy.clone(),
        previous_configuration_id: Some(accepted.configuration_id.clone()),
        current_configuration: current,
        election_lsn: Some(candidate.current_progress),
        build_id: None,
        repair: None,
        switchover: None,
    });
    status.quorum_loss = None;
    Some(Plan::Apply {
        changes: vec![KubernetesChange::PersistStatus {
            status: Box::new(waiting_status(
                status,
                "FailoverIntentPersisted",
                "Persisted a newer write-closed failover epoch with a provisional coordinator",
            )),
        }],
    })
}

fn evaluate_never_initialized(snapshot: &ObservationSnapshot, config: &EvaluationConfig) -> Plan {
    if has_durable_replica_evidence(snapshot) {
        return unsafe_plan(
            snapshot.status.clone(),
            UnsafeReason::DurableEvidenceWithoutAuthority,
            config,
        );
    }

    let Some(policy) = EffectivePolicy::fixed(
        snapshot.desired.replicas,
        snapshot.desired.failover_delay_seconds,
    ) else {
        return unsafe_plan(
            snapshot.status.clone(),
            UnsafeReason::InvalidDesiredState(
                "desired replica count must be greater than zero".to_string(),
            ),
            config,
        );
    };

    if !snapshot.has_complete_scaffolding() {
        return Plan::Apply {
            changes: vec![KubernetesChange::EnsureReplicaScaffolding {
                replica_ids: snapshot.intended_replica_ids(),
            }],
        };
    }
    if !snapshot.routing.service_present {
        return Plan::Apply {
            changes: vec![KubernetesChange::EnsureWriteRoutingService],
        };
    }
    if snapshot
        .intended_replica_ids()
        .into_iter()
        .any(|replica_id| {
            !matches!(
                snapshot
                    .scaffolding_observation_for(replica_id)
                    .map(|observation| &observation.agent),
                Some(AgentObservation::Uninitialized(_))
            )
        })
    {
        return Plan::Wait {
            reason: WaitReason::AwaitingAgentInitialization,
            status: waiting_status(
                snapshot.status.clone(),
                "AwaitingFreshStorageEvidence",
                "Every intended replica must explicitly report uninitialized storage",
            ),
            requeue_after_seconds: config.wait_requeue_seconds,
        };
    }
    if let Some(stale) = snapshot.replicas.values().find_map(|observation| {
        observation.kubernetes.as_ref().filter(|kubernetes| {
            kubernetes
                .image
                .as_deref()
                .is_some_and(|image| image != snapshot.desired.image)
        })
    }) {
        return Plan::Apply {
            changes: vec![delete_scaffolding_change(stale)],
        };
    }

    let primary_id = snapshot
        .intended_replica_ids()
        .into_iter()
        .min()
        .expect("positive replica count has a first member");
    let mut members = Vec::with_capacity(policy.replica_set_size as usize);
    for replica_id in snapshot.intended_replica_ids() {
        let kubernetes = snapshot
            .scaffolding_for(replica_id)
            .expect("complete scaffolding has every intended member");
        let pod_uid = kubernetes
            .pod_uid
            .as_ref()
            .expect("complete scaffolding has Pod UID");
        let pvc_uid = kubernetes
            .pvc_uid
            .as_ref()
            .expect("complete scaffolding has PVC UID");
        let initialization_id =
            derive_initialization_id(&snapshot.resource_uid, replica_id, pod_uid, pvc_uid);
        members.push(ConfigurationMember {
            identity: ReplicaIdentity {
                replica_id,
                instance_id: ReplicaInstanceId::new(pod_uid.as_str()),
                agent_generation: derive_agent_generation(&initialization_id),
            },
            role: if replica_id == primary_id {
                ReplicaRole::Primary
            } else {
                ReplicaRole::ActiveSecondary
            },
        });
    }

    let current_configuration =
        ConfigurationDescriptor::new(Epoch::new(0, 1), primary_id, members, policy.write_quorum);
    let transition = TransitionIntent {
        transition_id: derive_transition_id(
            &snapshot.resource_uid,
            TransitionKind::Bootstrap,
            &current_configuration.configuration_id,
        ),
        kind: TransitionKind::Bootstrap,
        spec_generation: snapshot.desired.generation,
        effective_policy: policy,
        previous_configuration_id: None,
        current_configuration,
        election_lsn: None,
        build_id: None,
        repair: None,
        switchover: None,
    };
    let mut status = snapshot.status.clone();
    status.transition = Some(transition);
    status = status.with_condition(progressing_condition(
        "BootstrapIntentPersisted",
        "Persisted deterministic write-closed genesis authority",
    ));
    Plan::Apply {
        changes: vec![KubernetesChange::PersistStatus {
            status: Box::new(status),
        }],
    }
}

fn evaluate_transition(
    snapshot: &ObservationSnapshot,
    transition: &TransitionIntent,
    config: &EvaluationConfig,
) -> Plan {
    if transition.kind == TransitionKind::PlannedSwitchover {
        return evaluate_switchover(snapshot, transition, config);
    }
    let mut status = transition_status(snapshot.status.clone());
    let (_, unsupported) = desired_spec_state(
        snapshot,
        &transition.current_configuration,
        &transition.effective_policy,
    );
    if let Some(condition) = unsupported {
        status = status.with_condition(condition);
    }
    if status != snapshot.status {
        return Plan::Apply {
            changes: vec![KubernetesChange::PersistStatus {
                status: Box::new(status),
            }],
        };
    }

    if transition.kind == TransitionKind::Replacement {
        if let Some(plan) = maybe_begin_failover(snapshot, Some(transition), status.clone(), config)
        {
            return plan;
        }
        return evaluate_replacement_transition(snapshot, transition, status, config);
    }
    if transition.kind == TransitionKind::Failover {
        return evaluate_failover_transition(snapshot, transition, status, config);
    }

    if let Some(plan) =
        evaluate_bootstrap_supersession(snapshot, transition, status.clone(), config)
    {
        return plan;
    }

    fn maybe_begin_failover(
        snapshot: &ObservationSnapshot,
        active_transition: Option<&TransitionIntent>,
        mut status: AcceptedStatus,
        config: &EvaluationConfig,
    ) -> Option<Plan> {
        let accepted = &snapshot
            .status
            .topology
            .as_ref()
            .expect("initialized status has accepted topology")
            .configuration;
        let basis = active_transition
            .map(|transition| &transition.current_configuration)
            .unwrap_or(accepted);
        let primary = configuration_primary(basis);
        if !replica_failed(snapshot, &primary.identity) {
            if status.primary_failure.is_some() {
                status.primary_failure = None;
                return Some(Plan::Apply {
                    changes: vec![KubernetesChange::PersistStatus {
                        status: Box::new(waiting_status(
                            status,
                            "PrimaryRecovered",
                            "The accepted primary recovered before failover authority was allocated",
                        )),
                    }],
                });
            }
            return None;
        }

        let accepted_primary = configuration_primary(accepted);
        let failure_matches = status
            .primary_failure
            .as_ref()
            .is_some_and(|failure| failure.primary == accepted_primary.identity);
        if !failure_matches {
            status.primary_failure = Some(PrimaryFailureObservation {
                primary: accepted_primary.identity.clone(),
                started_at_unix_seconds: snapshot.now_unix_seconds,
            });
            status.quorum_loss = None;
            let mut changes = Vec::new();
            if snapshot.routing.write_target.is_some() || snapshot.routing.unresolved_write_target {
                changes.push(KubernetesChange::RemoveWriteRouting);
            }
            changes.push(KubernetesChange::PersistStatus {
                status: Box::new(waiting_status(
                    status,
                    "PrimaryFailureObserved",
                    "Persisted exact primary failure observation and fenced write routing",
                )),
            });
            return Some(Plan::Apply { changes });
        }

        if snapshot.routing.write_target.is_some() || snapshot.routing.unresolved_write_target {
            return Some(Plan::Apply {
                changes: vec![
                    KubernetesChange::RemoveWriteRouting,
                    KubernetesChange::PersistStatus {
                        status: Box::new(waiting_status(
                            status,
                            "RoutingFencePending",
                            "Removing routing to the failed primary before failover",
                        )),
                    },
                ],
            });
        }

        let failure = status
            .primary_failure
            .as_ref()
            .expect("matching primary failure observation exists");
        let delay_elapsed = snapshot.now_unix_seconds
            >= failure.started_at_unix_seconds.saturating_add(
                i64::try_from(
                    snapshot
                        .status
                        .effective_policy
                        .as_ref()
                        .expect("initialized status has policy")
                        .failover_delay_seconds,
                )
                .unwrap_or(i64::MAX),
            );
        if !delay_elapsed {
            return Some(Plan::Wait {
                reason: WaitReason::FailoverDelay,
                status: waiting_status(
                    status,
                    "FailoverDelay",
                    "Waiting for the frozen primary-failure delay",
                ),
                requeue_after_seconds: config.wait_requeue_seconds,
            });
        }

        let policy = snapshot
            .status
            .effective_policy
            .as_ref()
            .expect("initialized status has policy");
        let reports = available_union_reports(snapshot, accepted, basis);
        if !configuration_read_quorum(accepted, &reports, policy.read_quorum)
            || !configuration_read_quorum(basis, &reports, policy.read_quorum)
        {
            if configuration_cannot_regain_read_quorum(snapshot, accepted, policy.read_quorum)
                || configuration_cannot_regain_read_quorum(snapshot, basis, policy.read_quorum)
            {
                return Some(unsafe_plan(
                    status,
                    UnsafeReason::ContradictoryReplicaEvidence(
                        "ordinary recovery would require abandoning PC or outstanding CC quorum"
                            .to_string(),
                    ),
                    config,
                ));
            }
            if status
                .quorum_loss
                .as_ref()
                .is_none_or(|quorum_loss| quorum_loss.configuration_id != accepted.configuration_id)
            {
                status.quorum_loss = Some(QuorumLossObservation {
                    configuration_id: accepted.configuration_id.clone(),
                });
                return Some(Plan::Apply {
                    changes: vec![KubernetesChange::PersistStatus {
                        status: Box::new(waiting_status(
                            status,
                            "FailoverReadQuorumUnavailable",
                            "Failover remains write-closed until PC and outstanding CC read quorum are observed",
                        )),
                    }],
                });
            }
            return Some(Plan::Wait {
                reason: WaitReason::QuorumLoss,
                status: waiting_status(
                    status,
                    "FailoverReadQuorumUnavailable",
                    "Failover remains write-closed until PC and outstanding CC read quorum are observed",
                ),
                requeue_after_seconds: config.wait_requeue_seconds,
            });
        }

        let candidate = select_failover_candidate(basis, &reports)?;
        let next_epoch = Epoch::new(
            basis.epoch.data_loss_number,
            basis
                .epoch
                .configuration_number
                .max(accepted.epoch.configuration_number)
                + 1,
        );
        let current = configuration_with_primary(basis, &candidate.identity, next_epoch);
        status.transition = Some(TransitionIntent {
            transition_id: derive_transition_id(
                &snapshot.resource_uid,
                TransitionKind::Failover,
                &current.configuration_id,
            ),
            kind: TransitionKind::Failover,
            spec_generation: active_transition
                .map_or(snapshot.status.observed_generation, |transition| {
                    transition.spec_generation
                }),
            effective_policy: policy.clone(),
            previous_configuration_id: Some(accepted.configuration_id.clone()),
            current_configuration: current,
            election_lsn: Some(candidate.current_progress),
            build_id: active_transition.and_then(|transition| transition.build_id.clone()),
            repair: None,
            switchover: None,
        });
        status.quorum_loss = None;
        Some(Plan::Apply {
            changes: vec![KubernetesChange::PersistStatus {
                status: Box::new(waiting_status(
                    status,
                    "FailoverIntentPersisted",
                    "Persisted a newer write-closed failover epoch with a provisional coordinator",
                )),
            }],
        })
    }

    fn evaluate_failover_transition(
        snapshot: &ObservationSnapshot,
        transition: &TransitionIntent,
        mut status: AcceptedStatus,
        config: &EvaluationConfig,
    ) -> Plan {
        let previous = &snapshot
            .status
            .topology
            .as_ref()
            .expect("validated failover has accepted topology")
            .configuration;
        let current = &transition.current_configuration;
        let primary = configuration_primary(current);

        if snapshot.routing.write_target.is_some() || snapshot.routing.unresolved_write_target {
            return Plan::Apply {
                changes: vec![
                    KubernetesChange::RemoveWriteRouting,
                    KubernetesChange::PersistStatus {
                        status: Box::new(waiting_status(
                            status,
                            "RoutingFencePending",
                            "Write routing remains fenced throughout failover",
                        )),
                    },
                ],
            };
        }

        let current_only_started = current.members.iter().any(|member| {
            healthy_report(snapshot, &member.identity).is_some_and(|report| {
                report.epoch == current.epoch
                    && report.previous_configuration.is_none()
                    && report.current_configuration.as_ref() == Some(current)
            })
        });
        let union = configuration_union(previous, current);
        if !current_only_started {
            for member in union
                .iter()
                .filter(|member| member.identity != primary.identity)
                .chain(std::iter::once(primary))
            {
                let Some(report) = healthy_report(snapshot, &member.identity) else {
                    continue;
                };
                let installed = report.epoch == current.epoch
                    && report.previous_configuration.as_ref() == Some(previous)
                    && report.current_configuration.as_ref() == Some(current);
                if !installed {
                    if report.pending_operation_id.is_some() {
                        continue;
                    }
                    return Plan::Execute {
                        command: ProtocolCommand::EnsureConfiguration(Box::new(
                            failover_configuration_command(
                                previous,
                                current,
                                member,
                                &transition.effective_policy,
                                failover_install_operation_id(transition, member),
                                transition.election_lsn,
                                AccessStatus::ReconfigurationPending,
                                false,
                                Vec::new(),
                            ),
                        )),
                    };
                }
            }
        }

        let pc_cc_reports = union
            .iter()
            .filter_map(|member| {
                let report = healthy_report(snapshot, &member.identity)?;
                (report.epoch == current.epoch
                    && report.previous_configuration.as_ref() == Some(previous)
                    && report.current_configuration.as_ref() == Some(current))
                .then_some(report)
            })
            .collect::<Vec<_>>();
        if !current_only_started
            && (!configuration_read_quorum(
                previous,
                &pc_cc_reports,
                transition.effective_policy.read_quorum,
            ) || !configuration_read_quorum(
                current,
                &pc_cc_reports,
                transition.effective_policy.read_quorum,
            ))
        {
            return Plan::Wait {
                reason: WaitReason::QuorumLoss,
                status: waiting_status(
                    status,
                    "ElectionEpochReadQuorumPending",
                    "Eligible replicas must accept the election epoch before candidate selection",
                ),
                requeue_after_seconds: config.wait_requeue_seconds,
            };
        }

        if !current_only_started {
            let election_reports = pc_cc_reports
                .iter()
                .copied()
                .filter(|report| {
                    current
                        .members
                        .iter()
                        .any(|member| member.identity == report.identity)
                        && report.deactivation_epoch == Some(current.epoch)
                        && status
                            .primary_failure
                            .as_ref()
                            .is_none_or(|failure| failure.primary != report.identity)
                })
                .collect::<Vec<_>>();
            let Some(candidate) = select_failover_candidate(current, &election_reports) else {
                return Plan::Wait {
                    reason: WaitReason::AwaitingStableEvidence,
                    status: waiting_status(
                        status,
                        "ElectionProgressPending",
                        "Waiting for epoch-fenced progress and deactivation evidence",
                    ),
                    requeue_after_seconds: config.wait_requeue_seconds,
                };
            };
            let primary_report = pc_cc_reports
                .iter()
                .copied()
                .find(|report| report.identity == primary.identity);
            if candidate.identity != primary.identity
                && primary_report.is_none_or(|report| report.write_status != AccessStatus::Granted)
            {
                let corrected = configuration_with_primary(
                    current,
                    &candidate.identity,
                    Epoch::new(
                        current.epoch.data_loss_number,
                        current.epoch.configuration_number + 1,
                    ),
                );
                status.transition = Some(TransitionIntent {
                    transition_id: derive_transition_id(
                        &snapshot.resource_uid,
                        TransitionKind::Failover,
                        &corrected.configuration_id,
                    ),
                    kind: TransitionKind::Failover,
                    spec_generation: transition.spec_generation,
                    effective_policy: transition.effective_policy.clone(),
                    previous_configuration_id: transition.previous_configuration_id.clone(),
                    current_configuration: corrected,
                    election_lsn: Some(candidate.current_progress),
                    build_id: transition.build_id.clone(),
                    repair: None,
                    switchover: None,
                });
                return Plan::Apply {
                    changes: vec![KubernetesChange::PersistStatus {
                        status: Box::new(waiting_status(
                            status,
                            "FailoverCandidateCorrected",
                            "Allocated a newer epoch for the authoritative progress winner",
                        )),
                    }],
                };
            }

            let Some(primary_report) = primary_report else {
                return Plan::Wait {
                    reason: WaitReason::AwaitingStableEvidence,
                    status: waiting_status(
                        status,
                        "PrimaryElectionEvidencePending",
                        "The selected primary has not completed its write-closed election command",
                    ),
                    requeue_after_seconds: config.wait_requeue_seconds,
                };
            };
            if let Some(plan) = evaluate_failover_repair(
                snapshot,
                transition,
                previous,
                current,
                primary,
                primary_report,
                &pc_cc_reports,
                status.clone(),
            ) {
                return plan;
            }

            if primary_report.write_status != AccessStatus::Granted
                || !primary_report.catch_up_complete
            {
                return Plan::Execute {
                    command: ProtocolCommand::EnsureConfiguration(Box::new(
                        failover_configuration_command(
                            previous,
                            current,
                            primary,
                            &transition.effective_policy,
                            OperationId::new(format!(
                                "{}:grant-write:{}",
                                transition.transition_id, primary.identity.replica_id
                            )),
                            transition.election_lsn,
                            AccessStatus::Granted,
                            false,
                            Vec::new(),
                        ),
                    )),
                };
            }
            if !configuration_deactivation_quorum(
                previous,
                &pc_cc_reports,
                transition.effective_policy.read_quorum,
                current.epoch,
            ) || !configuration_deactivation_quorum(
                current,
                &pc_cc_reports,
                transition.effective_policy.read_quorum,
                current.epoch,
            ) {
                return Plan::Wait {
                    reason: WaitReason::ActiveTransition,
                    status: waiting_status(
                        status,
                        "DeactivationQuorumPending",
                        "Waiting for PC and CC deactivation evidence before current-only activation",
                    ),
                    requeue_after_seconds: config.wait_requeue_seconds,
                };
            }
        }

        let retire_build_ids = transition
            .build_id
            .iter()
            .cloned()
            .chain(
                current
                    .members
                    .iter()
                    .filter(|member| member.identity != primary.identity)
                    .map(|member| {
                        derive_failover_repair_operation_id(
                            &snapshot.resource_uid,
                            &transition.transition_id,
                            &member.identity,
                        )
                    }),
            )
            .collect::<Vec<_>>();
        for member in &current.members {
            let Some(report) = healthy_report(snapshot, &member.identity) else {
                continue;
            };
            let operation_id = failover_current_only_operation_id(transition, member);
            let installed = report.epoch == current.epoch
                && report.previous_configuration.is_none()
                && report.current_configuration.as_ref() == Some(current)
                && report.pending_operation_id.is_none()
                && report.retained_operation_id.as_ref() == Some(&operation_id);
            if !installed {
                return Plan::Execute {
                    command: ProtocolCommand::EnsureConfiguration(Box::new(
                        failover_configuration_command(
                            previous,
                            current,
                            member,
                            &transition.effective_policy,
                            operation_id,
                            transition.election_lsn,
                            if member.identity == primary.identity {
                                AccessStatus::Granted
                            } else {
                                AccessStatus::ReconfigurationPending
                            },
                            true,
                            retire_build_ids.clone(),
                        ),
                    )),
                };
            }
        }

        let current_only_reports = current
            .members
            .iter()
            .filter_map(|member| {
                let report = healthy_report(snapshot, &member.identity)?;
                (report.epoch == current.epoch
                    && report.previous_configuration.is_none()
                    && report.current_configuration.as_ref() == Some(current))
                .then_some(report)
            })
            .collect::<Vec<_>>();
        let primary_ready = current_only_reports.iter().any(|report| {
            report.identity == primary.identity && report.write_status == AccessStatus::Granted
        });
        if primary_ready && configuration_report_quorum(current, &current_only_reports) {
            let retired = previous.members.iter().find(|previous_member| {
                current.members.iter().all(|current_member| {
                    current_member.identity.replica_id != previous_member.identity.replica_id
                        || current_member.identity != previous_member.identity
                })
            });
            let mut accepted = clear_evaluator_conditions(snapshot.status.clone());
            accepted.observed_generation = transition.spec_generation;
            accepted.topology = Some(AcceptedTopology {
                configuration: current.clone(),
            });
            accepted.transition = None;
            accepted.primary_failure = None;
            accepted.quorum_loss = None;
            accepted = accepted.with_condition(progressing_condition(
                "FailoverTopologyAccepted",
                "Accepted the epoch-fenced failover topology",
            ));
            let mut changes = vec![KubernetesChange::PersistStatus {
                status: Box::new(accepted),
            }];
            if let Some(retired) = retired {
                changes.push(KubernetesChange::DeleteReplicaEndpoint {
                    identity: retired.identity.clone(),
                });
            }
            return Plan::Apply { changes };
        }

        Plan::Wait {
            reason: WaitReason::ActiveTransition,
            status,
            requeue_after_seconds: config.wait_requeue_seconds,
        }
    }

    for member in &transition.current_configuration.members {
        let Some(observation) = snapshot.observation_for_identity(&member.identity) else {
            continue;
        };
        if let Err(message) = bootstrap_initialization_command(snapshot, member, observation) {
            return unsafe_plan(
                status,
                UnsafeReason::ContradictoryReplicaEvidence(message),
                config,
            );
        }
    }

    fn evaluate_bootstrap_supersession(
        snapshot: &ObservationSnapshot,
        transition: &TransitionIntent,
        status: AcceptedStatus,
        config: &EvaluationConfig,
    ) -> Option<Plan> {
        let missing = transition
            .current_configuration
            .members
            .iter()
            .find(|member| {
                let exact_missing = snapshot
                    .observation_for_identity(&member.identity)
                    .and_then(|observation| observation.kubernetes.as_ref())
                    .is_none();
                let orphaned_storage = snapshot.replicas.iter().any(|(key, observation)| {
                    key.replica_id == member.identity.replica_id
                        && observation.kubernetes.as_ref().is_some_and(|kubernetes| {
                            kubernetes.pod_uid.is_none() && kubernetes.pvc_uid.is_some()
                        })
                });
                exact_missing && orphaned_storage
            })?;
        let installed = snapshot.replicas.values().any(|observation| {
            matches!(
                &observation.agent,
                AgentObservation::Report(report)
                    if report.current_configuration.as_ref()
                        == Some(&transition.current_configuration)
            )
        });
        if installed {
            return Some(Plan::Wait {
                reason: WaitReason::ActiveTransition,
                status: waiting_status(
                    status,
                    "BootstrapSupersessionBlocked",
                    "Genesis configuration was installed before an incarnation disappeared",
                ),
                requeue_after_seconds: config.wait_requeue_seconds,
            });
        }
        let candidate = snapshot.replicas.iter().find(|(key, observation)| {
            key.replica_id == missing.identity.replica_id
                && key.instance_id != missing.identity.instance_id
                && observation.kubernetes.as_ref().is_some_and(|kubernetes| {
                    kubernetes.has_exact_scaffolding() && kubernetes.peer_endpoint_ready
                })
                && matches!(observation.agent, AgentObservation::Uninitialized(_))
        });
        let Some((key, observation)) = candidate else {
            return Some(Plan::Apply {
                changes: vec![KubernetesChange::EnsureReplacementScaffolding {
                    replica_id: missing.identity.replica_id,
                    replacing: missing.identity.clone(),
                }],
            });
        };
        let kubernetes = observation
            .kubernetes
            .as_ref()
            .expect("bootstrap supersession candidate has scaffolding");
        let pod_uid = kubernetes
            .pod_uid
            .as_ref()
            .expect("bootstrap supersession candidate has Pod UID");
        let pvc_uid = kubernetes
            .pvc_uid
            .as_ref()
            .expect("bootstrap supersession candidate has PVC UID");
        let initialization_id = derive_initialization_id(
            &snapshot.resource_uid,
            missing.identity.replica_id,
            pod_uid,
            pvc_uid,
        );
        let replacement = ReplicaIdentity {
            replica_id: missing.identity.replica_id,
            instance_id: key.instance_id.clone(),
            agent_generation: derive_agent_generation(&initialization_id),
        };
        let members = transition
            .current_configuration
            .members
            .iter()
            .map(|member| {
                if member.identity == missing.identity {
                    ConfigurationMember {
                        identity: replacement.clone(),
                        role: member.role,
                    }
                } else {
                    member.clone()
                }
            })
            .collect();
        let current = ConfigurationDescriptor::new(
            Epoch::new(
                transition.current_configuration.epoch.data_loss_number,
                transition.current_configuration.epoch.configuration_number + 1,
            ),
            transition.current_configuration.primary_id,
            members,
            transition.current_configuration.write_quorum,
        );
        let mut superseded = snapshot.status.clone();
        superseded.transition = Some(TransitionIntent {
            transition_id: derive_transition_id(
                &snapshot.resource_uid,
                TransitionKind::Bootstrap,
                &current.configuration_id,
            ),
            kind: TransitionKind::Bootstrap,
            spec_generation: transition.spec_generation,
            effective_policy: transition.effective_policy.clone(),
            previous_configuration_id: None,
            current_configuration: current,
            election_lsn: None,
            build_id: None,
            repair: None,
            switchover: None,
        });
        superseded = waiting_status(
            superseded,
            "BootstrapIncarnationSuperseded",
            "Replaced one never-installed genesis incarnation",
        );
        Some(Plan::Apply {
            changes: vec![KubernetesChange::PersistStatus {
                status: Box::new(superseded),
            }],
        })
    }

    let mut installed = 0_u32;
    for member in &transition.current_configuration.members {
        let Some(observation) = snapshot.observation_for_identity(&member.identity) else {
            return Plan::Wait {
                reason: WaitReason::AgentUnavailable,
                status,
                requeue_after_seconds: config.wait_requeue_seconds,
            };
        };
        match bootstrap_initialization_command(snapshot, member, observation)
            .expect("bootstrap observations were prevalidated")
        {
            Some(command) => {
                return Plan::Execute {
                    command: ProtocolCommand::InitializeAgentStore(Box::new(command)),
                };
            }
            None if !matches!(observation.agent, AgentObservation::Report(_)) => {
                return Plan::Wait {
                    reason: WaitReason::AgentUnavailable,
                    status,
                    requeue_after_seconds: config.wait_requeue_seconds,
                };
            }
            None => {
                let AgentObservation::Report(report) = &observation.agent else {
                    continue;
                };
                let matches = report.identity == member.identity
                    && report.healthy
                    && report.role == member.role
                    && report.write_status != AccessStatus::Granted
                    && report.epoch == transition.current_configuration.epoch
                    && report.previous_configuration.is_none()
                    && report.current_configuration.as_ref()
                        == Some(&transition.current_configuration)
                    && report.current_progress == 0
                    && report.committed_lsn == 0
                    && report.pending_operation_id.is_none()
                    && report.retained_operation_id.as_ref()
                        == Some(&bootstrap_install_operation_id(transition, member));
                if matches {
                    installed += 1;
                    continue;
                }
                return Plan::Execute {
                    command: ProtocolCommand::EnsureConfiguration(Box::new(
                        ensure_configuration_command(
                            &transition.current_configuration,
                            member,
                            &transition.effective_policy,
                            bootstrap_install_operation_id(transition, member),
                            AccessStatus::ReconfigurationPending,
                            false,
                        ),
                    )),
                };
            }
        }
    }

    if installed == transition.current_configuration.members.len() as u32 {
        let mut accepted = clear_evaluator_conditions(snapshot.status.clone());
        accepted.initialized = true;
        accepted.observed_generation = transition.spec_generation;
        accepted.effective_policy = Some(transition.effective_policy.clone());
        accepted.topology = Some(AcceptedTopology {
            configuration: transition.current_configuration.clone(),
        });
        accepted.transition = None;
        accepted = accepted.with_condition(progressing_condition(
            "BootstrapTopologyAccepted",
            "Accepted the full write-closed genesis topology",
        ));
        return Plan::Apply {
            changes: vec![KubernetesChange::PersistStatus {
                status: Box::new(accepted),
            }],
        };
    }

    Plan::Wait {
        reason: WaitReason::ActiveTransition,
        status,
        requeue_after_seconds: config.wait_requeue_seconds,
    }
}

fn evaluate_provisioning(
    snapshot: &ObservationSnapshot,
    provisioning: &ProvisioningIntent,
    config: &EvaluationConfig,
) -> Plan {
    let mut status = waiting_status(
        snapshot.status.clone(),
        "ProvisioningInProgress",
        "An exact replacement remains outside authority",
    );
    let target_identity = provisioning.target_identity(&snapshot.resource_uid);
    let topology = &snapshot
        .status
        .topology
        .as_ref()
        .expect("validated provisioning has topology")
        .configuration;
    let source = topology
        .members
        .iter()
        .find(|member| member.identity.replica_id == topology.primary_id)
        .expect("validated topology has primary");
    if replica_failed(snapshot, &source.identity) {
        status.provisioning = None;
        status.primary_failure = Some(PrimaryFailureObservation {
            primary: source.identity.clone(),
            started_at_unix_seconds: snapshot
                .status
                .primary_failure
                .as_ref()
                .filter(|failure| failure.primary == source.identity)
                .map_or(snapshot.now_unix_seconds, |failure| {
                    failure.started_at_unix_seconds
                }),
        });
        let mut changes = vec![KubernetesChange::DeleteReplicaScaffolding {
            pod_name: None,
            pod_uid: Some(provisioning.pod_uid.clone()),
            pvc_name: None,
            pvc_uid: Some(provisioning.pvc_uid.clone()),
        }];
        if snapshot.routing.write_target.is_some() || snapshot.routing.unresolved_write_target {
            changes.push(KubernetesChange::RemoveWriteRouting);
        }
        changes.push(KubernetesChange::PersistStatus {
            status: Box::new(waiting_status(
                status,
                "ProvisioningAbandonedForFailover",
                "Abandoned the unaccepted replacement attempt and fenced the failed primary",
            )),
        });
        return Plan::Apply { changes };
    }
    let Some(observation) = snapshot.observation_for_identity(&target_identity) else {
        status.provisioning = None;
        return Plan::Apply {
            changes: vec![
                KubernetesChange::DeleteReplicaScaffolding {
                    pod_name: None,
                    pod_uid: Some(provisioning.pod_uid.clone()),
                    pvc_name: None,
                    pvc_uid: Some(provisioning.pvc_uid.clone()),
                },
                KubernetesChange::PersistStatus {
                    status: Box::new(status),
                },
            ],
        };
    };
    match &observation.agent {
        AgentObservation::Uninitialized(report) => Plan::Execute {
            command: ProtocolCommand::InitializeAgentStore(Box::new(InitializeAgentStore {
                initialization_id: provisioning.initialization_id(&snapshot.resource_uid),
                resource_uid: snapshot.resource_uid.clone(),
                local_replica_id: provisioning.replica_id(),
                expected_instance_id: provisioning.instance_id(),
                expected_pod_uid: report.pod_uid.clone(),
                expected_pvc_uid: report.pvc_uid.clone(),
                assigned_agent_generation: provisioning
                    .assigned_agent_generation(&snapshot.resource_uid),
                effective_policy: snapshot
                    .status
                    .effective_policy
                    .clone()
                    .expect("validated provisioning has effective policy"),
                bootstrap_configuration: snapshot
                    .status
                    .topology
                    .as_ref()
                    .expect("validated provisioning has topology")
                    .configuration
                    .clone(),
                provisioning: Some(provisioning.clone()),
            })),
        },
        AgentObservation::Report(target_report) => {
            let Some(source_observation) = snapshot.observation_for_identity(&source.identity)
            else {
                return Plan::Wait {
                    reason: WaitReason::ProvisioningInProgress,
                    status,
                    requeue_after_seconds: config.wait_requeue_seconds,
                };
            };
            let AgentObservation::Report(source_report) = &source_observation.agent else {
                return Plan::Wait {
                    reason: WaitReason::ProvisioningInProgress,
                    status,
                    requeue_after_seconds: config.wait_requeue_seconds,
                };
            };
            let source_complete = source_report.builds.iter().any(|build| {
                build.build_id == provisioning.operation_id
                    && build.target == target_identity
                    && build.completed
                    && build.durable_lsn >= source_report.current_progress
            });
            let target_complete = target_report.builds.iter().any(|build| {
                build.build_id == provisioning.operation_id
                    && build.target == target_identity
                    && build.completed
            });
            if !source_complete || !target_complete {
                return Plan::Execute {
                    command: ProtocolCommand::EnsureReplicaBuild(Box::new(EnsureReplicaBuild {
                        operation_id: provisioning.operation_id.clone(),
                        local_replica_id: source.identity.replica_id,
                        expected_instance_id: source.identity.instance_id.clone(),
                        expected_agent_generation: source.identity.agent_generation.clone(),
                        target: target_identity,
                        authority: None,
                        source_session_id: None,
                    })),
                };
            }
            let policy = snapshot
                .status
                .effective_policy
                .clone()
                .expect("validated provisioning has policy");
            let members = topology
                .members
                .iter()
                .map(|member| {
                    if member.identity == provisioning.replaces {
                        ConfigurationMember {
                            identity: target_identity.clone(),
                            role: ReplicaRole::ActiveSecondary,
                        }
                    } else {
                        member.clone()
                    }
                })
                .collect();
            let current = ConfigurationDescriptor::new(
                Epoch::new(
                    topology.epoch.data_loss_number,
                    topology.epoch.configuration_number + 1,
                ),
                topology.primary_id,
                members,
                policy.write_quorum,
            );
            let mut transition_status = snapshot.status.clone();
            transition_status.provisioning = None;
            transition_status.transition = Some(TransitionIntent {
                transition_id: derive_transition_id(
                    &snapshot.resource_uid,
                    TransitionKind::Replacement,
                    &current.configuration_id,
                ),
                kind: TransitionKind::Replacement,
                spec_generation: snapshot.status.observed_generation,
                effective_policy: policy,
                previous_configuration_id: Some(topology.configuration_id.clone()),
                current_configuration: current,
                election_lsn: None,
                build_id: Some(provisioning.operation_id.clone()),
                repair: None,
                switchover: None,
            });
            transition_status = waiting_status(
                transition_status,
                "ReplacementTransitionPersisted",
                "Persisted equal-cardinality PC/CC replacement authority",
            );
            Plan::Apply {
                changes: vec![KubernetesChange::PersistStatus {
                    status: Box::new(transition_status),
                }],
            }
        }
        AgentObservation::Absent | AgentObservation::Unreachable { .. } => Plan::Wait {
            reason: WaitReason::ProvisioningInProgress,
            status,
            requeue_after_seconds: config.wait_requeue_seconds,
        },
        AgentObservation::Invalid { message } => unsafe_plan(
            status,
            UnsafeReason::ContradictoryReplicaEvidence(message.clone()),
            config,
        ),
    }
}

fn evaluate_replacement_transition(
    snapshot: &ObservationSnapshot,
    transition: &TransitionIntent,
    status: AcceptedStatus,
    config: &EvaluationConfig,
) -> Plan {
    let previous = &snapshot
        .status
        .topology
        .as_ref()
        .expect("validated replacement has accepted topology")
        .configuration;
    let current = &transition.current_configuration;
    let primary = current
        .members
        .iter()
        .find(|member| member.identity.replica_id == current.primary_id)
        .expect("validated Current Configuration has primary");
    let union = previous.members.iter().chain(current.members.iter()).fold(
        Vec::<ConfigurationMember>::new(),
        |mut members, member| {
            if !members
                .iter()
                .any(|existing| existing.identity == member.identity)
            {
                members.push(member.clone());
            }
            members
        },
    );

    let pc_cc_reports = union
        .iter()
        .filter_map(|member| {
            let observation = snapshot.observation_for_identity(&member.identity)?;
            let AgentObservation::Report(report) = &observation.agent else {
                return None;
            };
            (report.epoch == current.epoch
                && report.previous_configuration.as_ref() == Some(previous)
                && report.current_configuration.as_ref() == Some(current))
            .then_some(report.as_ref())
        })
        .collect::<Vec<_>>();
    let primary_pc_cc = pc_cc_reports
        .iter()
        .find(|report| report.identity == primary.identity)
        .copied();
    let current_only_started = current.members.iter().any(|member| {
        snapshot
            .observation_for_identity(&member.identity)
            .is_some_and(|observation| {
                matches!(
                    &observation.agent,
                    AgentObservation::Report(report)
                        if report.epoch == current.epoch
                            && report.previous_configuration.is_none()
                            && report.current_configuration.as_ref() == Some(current)
                )
            })
    });
    let current_only_phase = current_only_started
        || primary_pc_cc.is_some_and(|report| report.catch_up_complete)
            && configuration_report_quorum(previous, &pc_cc_reports)
            && configuration_report_quorum(current, &pc_cc_reports);

    if !current_only_phase {
        for member in union
            .iter()
            .filter(|member| member.identity != primary.identity)
            .chain(std::iter::once(primary))
        {
            let Some(observation) = snapshot.observation_for_identity(&member.identity) else {
                continue;
            };
            let AgentObservation::Report(report) = &observation.agent else {
                continue;
            };
            let operation_id = replacement_install_operation_id(transition, member);
            let installed = report.epoch == current.epoch
                && report.previous_configuration.as_ref() == Some(previous)
                && report.current_configuration.as_ref() == Some(current)
                && report.pending_operation_id.is_none()
                && report.retained_operation_id.as_ref() == Some(&operation_id);
            if !installed {
                return Plan::Execute {
                    command: ProtocolCommand::EnsureConfiguration(Box::new(
                        replacement_configuration_command(
                            previous,
                            current,
                            member,
                            &transition.effective_policy,
                            operation_id,
                            member.identity == primary.identity,
                            false,
                            None,
                        ),
                    )),
                };
            }
        }
        return Plan::Wait {
            reason: WaitReason::ActiveTransition,
            status,
            requeue_after_seconds: config.wait_requeue_seconds,
        };
    }

    for member in current
        .members
        .iter()
        .filter(|member| member.identity != primary.identity)
        .chain(std::iter::once(primary))
    {
        let Some(observation) = snapshot.observation_for_identity(&member.identity) else {
            continue;
        };
        let AgentObservation::Report(report) = &observation.agent else {
            continue;
        };
        let has_outstanding_authority = report.epoch == current.epoch
            && report.previous_configuration.as_ref() == Some(previous)
            && report.current_configuration.as_ref() == Some(current);
        let has_current_only_authority = report.epoch == current.epoch
            && report.previous_configuration.is_none()
            && report.current_configuration.as_ref() == Some(current);
        if !has_outstanding_authority && !has_current_only_authority {
            return Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(Box::new(
                    replacement_configuration_command(
                        previous,
                        current,
                        member,
                        &transition.effective_policy,
                        replacement_install_operation_id(transition, member),
                        member.identity == primary.identity,
                        false,
                        None,
                    ),
                )),
            };
        }
        let operation_id = replacement_current_only_operation_id(transition, member);
        let installed = has_current_only_authority
            && report.pending_operation_id.is_none()
            && report.retained_operation_id.as_ref() == Some(&operation_id);
        if !installed {
            return Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(Box::new(
                    replacement_configuration_command(
                        previous,
                        current,
                        member,
                        &transition.effective_policy,
                        operation_id,
                        member.identity == primary.identity,
                        true,
                        transition.build_id.clone(),
                    ),
                )),
            };
        }
    }

    let current_only_reports = current
        .members
        .iter()
        .filter_map(|member| {
            let observation = snapshot.observation_for_identity(&member.identity)?;
            let AgentObservation::Report(report) = &observation.agent else {
                return None;
            };
            (report.epoch == current.epoch
                && report.previous_configuration.is_none()
                && report.current_configuration.as_ref() == Some(current))
            .then_some(report.as_ref())
        })
        .collect::<Vec<_>>();
    let primary_ready = current_only_reports.iter().any(|report| {
        report.identity == primary.identity && report.write_status == AccessStatus::Granted
    });
    if primary_ready && configuration_report_quorum(current, &current_only_reports) {
        let retired = previous
            .members
            .iter()
            .find(|previous_member| {
                current.members.iter().all(|current_member| {
                    current_member.identity.replica_id != previous_member.identity.replica_id
                        || current_member.identity != previous_member.identity
                })
            })
            .expect("validated replacement changes one exact incarnation")
            .identity
            .clone();
        let mut accepted = clear_evaluator_conditions(snapshot.status.clone());
        accepted.observed_generation = transition.spec_generation;
        accepted.topology = Some(AcceptedTopology {
            configuration: current.clone(),
        });
        accepted.transition = None;
        accepted = accepted.with_condition(progressing_condition(
            "ReplacementTopologyAccepted",
            "Accepted the equal-cardinality replacement topology",
        ));
        return Plan::Apply {
            changes: vec![
                KubernetesChange::PersistStatus {
                    status: Box::new(accepted),
                },
                KubernetesChange::DeleteReplicaEndpoint { identity: retired },
            ],
        };
    }

    Plan::Wait {
        reason: WaitReason::ActiveTransition,
        status,
        requeue_after_seconds: config.wait_requeue_seconds,
    }
}

fn configuration_report_quorum(
    configuration: &ConfigurationDescriptor,
    reports: &[&crate::observation::AgentReport],
) -> bool {
    configuration
        .members
        .iter()
        .filter(|member| {
            reports
                .iter()
                .any(|report| report.identity == member.identity)
        })
        .count()
        >= configuration.write_quorum as usize
}

#[allow(clippy::too_many_arguments)]
fn evaluate_failover_repair(
    snapshot: &ObservationSnapshot,
    transition: &TransitionIntent,
    previous: &ConfigurationDescriptor,
    current: &ConfigurationDescriptor,
    primary: &ConfigurationMember,
    primary_report: &crate::observation::AgentReport,
    reports: &[&crate::observation::AgentReport],
    mut status: AcceptedStatus,
) -> Option<Plan> {
    let retained_from = primary_report.catch_up_capability?;
    let repair = transition.repair.clone().or_else(|| {
        current
            .members
            .iter()
            .filter(|member| member.identity != primary.identity)
            .filter_map(|member| {
                reports
                    .iter()
                    .copied()
                    .find(|report| report.identity == member.identity)
                    .filter(|report| report.current_progress.saturating_add(1) < retained_from)
                    .map(|_| ReplicaRepairIntent {
                        operation_id: derive_failover_repair_operation_id(
                            &snapshot.resource_uid,
                            &transition.transition_id,
                            &member.identity,
                        ),
                        target: member.identity.clone(),
                    })
            })
            .next()
    });
    let repair = repair?;
    if transition.repair.is_none() {
        let mut updated = transition.clone();
        updated.repair = Some(repair);
        status.transition = Some(updated);
        return Some(Plan::Apply {
            changes: vec![KubernetesChange::PersistStatus {
                status: Box::new(waiting_status(
                    status,
                    "FailoverFullCopyAuthorized",
                    "Persisted exact full-copy authority for a configured lagging member",
                )),
            }],
        });
    }

    let target_report = reports
        .iter()
        .copied()
        .find(|report| report.identity == repair.target);
    let source_complete = primary_report.builds.iter().any(|build| {
        build.build_id == repair.operation_id
            && build.target == repair.target
            && build.completed
            && build.durable_lsn >= primary_report.current_progress
    });
    let target_complete = target_report.is_some_and(|report| {
        report.builds.iter().any(|build| {
            build.build_id == repair.operation_id
                && build.target == repair.target
                && build.completed
        })
    });
    if !source_complete || !target_complete {
        return Some(Plan::Execute {
            command: ProtocolCommand::EnsureReplicaBuild(Box::new(EnsureReplicaBuild {
                operation_id: repair.operation_id,
                local_replica_id: primary.identity.replica_id,
                expected_instance_id: primary.identity.instance_id.clone(),
                expected_agent_generation: primary.identity.agent_generation.clone(),
                target: repair.target,
                authority: None,
                source_session_id: None,
            })),
        });
    }

    let target = current
        .members
        .iter()
        .find(|member| member.identity == repair.target)
        .expect("validated failover repair target belongs to Current Configuration");
    let restored = target_report.is_some_and(|report| {
        report.role == target.role
            && report.current_progress >= primary_report.current_progress
            && report.epoch == current.epoch
            && report.previous_configuration.as_ref() == Some(previous)
            && report.current_configuration.as_ref() == Some(current)
    });
    if !restored {
        return Some(Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(Box::new(
                failover_configuration_command(
                    previous,
                    current,
                    target,
                    &transition.effective_policy,
                    OperationId::new(format!(
                        "{}:post-copy:{}",
                        transition.transition_id, target.identity.replica_id
                    )),
                    transition.election_lsn,
                    AccessStatus::ReconfigurationPending,
                    false,
                    Vec::new(),
                ),
            )),
        });
    }
    let mut updated = transition.clone();
    updated.repair = None;
    status.transition = Some(updated);
    Some(Plan::Apply {
        changes: vec![KubernetesChange::PersistStatus {
            status: Box::new(waiting_status(
                status,
                "FailoverFullCopyCompleted",
                "Completed one exact repair and will evaluate any remaining lagging members",
            )),
        }],
    })
}

fn replica_failed(snapshot: &ObservationSnapshot, identity: &ReplicaIdentity) -> bool {
    let Some(observation) = snapshot.observation_for_identity(identity) else {
        return true;
    };
    let agent_healthy = matches!(
        &observation.agent,
        AgentObservation::Report(report)
            if report.healthy && report.reported_fault != Some(crate::types::FaultType::Permanent)
    );
    if agent_healthy {
        return observation
            .kubernetes
            .as_ref()
            .is_some_and(|kubernetes| !kubernetes.pod_ready);
    }
    true
}

fn healthy_report<'a>(
    snapshot: &'a ObservationSnapshot,
    identity: &ReplicaIdentity,
) -> Option<&'a crate::observation::AgentReport> {
    let observation = snapshot.observation_for_identity(identity)?;
    if !observation
        .kubernetes
        .as_ref()
        .is_some_and(|kubernetes| kubernetes.pod_ready && kubernetes.peer_endpoint_ready)
    {
        return None;
    }
    let AgentObservation::Report(report) = &observation.agent else {
        return None;
    };
    (report.healthy && report.reported_fault != Some(crate::types::FaultType::Permanent))
        .then_some(report.as_ref())
}

fn configuration_primary(configuration: &ConfigurationDescriptor) -> &ConfigurationMember {
    configuration
        .members
        .iter()
        .find(|member| {
            member.identity.replica_id == configuration.primary_id
                && member.role == ReplicaRole::Primary
        })
        .expect("validated configuration has one primary")
}

fn configuration_union(
    previous: &ConfigurationDescriptor,
    current: &ConfigurationDescriptor,
) -> Vec<ConfigurationMember> {
    previous
        .members
        .iter()
        .chain(&current.members)
        .fold(Vec::new(), |mut members, member| {
            if !members
                .iter()
                .any(|existing: &ConfigurationMember| existing.identity == member.identity)
            {
                members.push(member.clone());
            }
            members
        })
}

fn available_union_reports<'a>(
    snapshot: &'a ObservationSnapshot,
    previous: &ConfigurationDescriptor,
    current: &ConfigurationDescriptor,
) -> Vec<&'a crate::observation::AgentReport> {
    configuration_union(previous, current)
        .iter()
        .filter_map(|member| healthy_report(snapshot, &member.identity))
        .collect()
}

fn configuration_read_quorum(
    configuration: &ConfigurationDescriptor,
    reports: &[&crate::observation::AgentReport],
    read_quorum: u32,
) -> bool {
    configuration
        .members
        .iter()
        .filter(|member| {
            reports
                .iter()
                .any(|report| report.identity == member.identity)
        })
        .count()
        >= read_quorum as usize
}

fn surviving_exact_members(
    snapshot: &ObservationSnapshot,
    configuration: &ConfigurationDescriptor,
) -> usize {
    configuration
        .members
        .iter()
        .filter(|member| !definitively_lost(snapshot, &member.identity))
        .count()
}

fn configuration_cannot_regain_read_quorum(
    snapshot: &ObservationSnapshot,
    configuration: &ConfigurationDescriptor,
    read_quorum: u32,
) -> bool {
    configuration
        .members
        .iter()
        .filter(|member| {
            !snapshot
                .observation_for_identity(&member.identity)
                .is_some_and(|observation| {
                    matches!(
                        &observation.agent,
                        AgentObservation::Report(report)
                            if report.reported_fault
                                == Some(crate::types::FaultType::Permanent)
                    )
                })
        })
        .count()
        < read_quorum as usize
}

fn configuration_deactivation_quorum(
    configuration: &ConfigurationDescriptor,
    reports: &[&crate::observation::AgentReport],
    read_quorum: u32,
    epoch: Epoch,
) -> bool {
    configuration
        .members
        .iter()
        .filter(|member| {
            reports.iter().any(|report| {
                report.identity == member.identity
                    && report.deactivation_epoch == Some(epoch)
                    && report.deactivated_lsn.is_some()
            })
        })
        .count()
        >= read_quorum as usize
}

fn select_failover_candidate<'a>(
    configuration: &ConfigurationDescriptor,
    reports: &[&'a crate::observation::AgentReport],
) -> Option<&'a crate::observation::AgentReport> {
    reports
        .iter()
        .copied()
        .filter(|report| {
            configuration
                .members
                .iter()
                .any(|member| member.identity == report.identity)
                && report.write_status != AccessStatus::Granted
                && (report.role != ReplicaRole::Primary
                    || report.identity.replica_id == configuration.primary_id)
        })
        .max_by(|left, right| {
            left.deactivation_epoch
                .unwrap_or_default()
                .cmp(&right.deactivation_epoch.unwrap_or_default())
                .then_with(|| {
                    left.deactivated_lsn
                        .unwrap_or_default()
                        .cmp(&right.deactivated_lsn.unwrap_or_default())
                })
                .then_with(|| left.current_progress.cmp(&right.current_progress))
                .then_with(|| left.committed_lsn.cmp(&right.committed_lsn))
                .then_with(|| right.identity.replica_id.cmp(&left.identity.replica_id))
        })
}

fn configuration_with_primary(
    configuration: &ConfigurationDescriptor,
    primary: &ReplicaIdentity,
    epoch: Epoch,
) -> ConfigurationDescriptor {
    ConfigurationDescriptor::new(
        epoch,
        primary.replica_id,
        configuration
            .members
            .iter()
            .map(|member| ConfigurationMember {
                identity: member.identity.clone(),
                role: if member.identity == *primary {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        configuration.write_quorum,
    )
}

fn failover_install_operation_id(
    transition: &TransitionIntent,
    member: &ConfigurationMember,
) -> OperationId {
    OperationId::new(format!(
        "{}:election:{}",
        transition.transition_id, member.identity.replica_id
    ))
}

fn failover_current_only_operation_id(
    transition: &TransitionIntent,
    member: &ConfigurationMember,
) -> OperationId {
    OperationId::new(format!(
        "{}:current-only:{}",
        transition.transition_id, member.identity.replica_id
    ))
}

#[allow(clippy::too_many_arguments)]
fn failover_configuration_command(
    previous: &ConfigurationDescriptor,
    current: &ConfigurationDescriptor,
    member: &ConfigurationMember,
    policy: &EffectivePolicy,
    operation_id: OperationId,
    failover_safe_lsn: Option<i64>,
    primary_write_status: AccessStatus,
    current_only: bool,
    retire_build_ids: Vec<OperationId>,
) -> EnsureConfiguration {
    EnsureConfiguration {
        operation_id,
        previous_configuration: (!current_only).then(|| previous.clone()),
        current_configuration: current.clone(),
        previous_epoch: (!current_only).then_some(previous.epoch),
        current_epoch: current.epoch,
        effective_policy: policy.clone(),
        local_replica_id: member.identity.replica_id,
        expected_instance_id: member.identity.instance_id.clone(),
        expected_agent_generation: member.identity.agent_generation.clone(),
        transition_kind: TransitionKind::Failover,
        failover_safe_lsn,
        primary_write_status,
        current_only,
        retire_build_ids,
        switchover_handoff: None,
        retire_switchover_preparation_ids: Vec::new(),
    }
}

fn replacement_install_operation_id(
    transition: &TransitionIntent,
    member: &ConfigurationMember,
) -> OperationId {
    OperationId::new(format!(
        "{}:pc-cc:{}",
        transition.transition_id, member.identity.replica_id
    ))
}

fn replacement_current_only_operation_id(
    transition: &TransitionIntent,
    member: &ConfigurationMember,
) -> OperationId {
    OperationId::new(format!(
        "{}:current-only:{}",
        transition.transition_id, member.identity.replica_id
    ))
}

#[allow(clippy::too_many_arguments)]
fn replacement_configuration_command(
    previous: &ConfigurationDescriptor,
    current: &ConfigurationDescriptor,
    member: &ConfigurationMember,
    policy: &EffectivePolicy,
    operation_id: OperationId,
    grant_write: bool,
    current_only: bool,
    retire_build_id: Option<OperationId>,
) -> EnsureConfiguration {
    EnsureConfiguration {
        operation_id,
        previous_configuration: (!current_only).then(|| previous.clone()),
        current_configuration: current.clone(),
        previous_epoch: (!current_only).then_some(previous.epoch),
        current_epoch: current.epoch,
        effective_policy: policy.clone(),
        local_replica_id: member.identity.replica_id,
        expected_instance_id: member.identity.instance_id.clone(),
        expected_agent_generation: member.identity.agent_generation.clone(),
        transition_kind: TransitionKind::Replacement,
        failover_safe_lsn: None,
        primary_write_status: if grant_write {
            AccessStatus::Granted
        } else {
            AccessStatus::ReconfigurationPending
        },
        current_only,
        retire_build_ids: retire_build_id.into_iter().collect(),
        switchover_handoff: None,
        retire_switchover_preparation_ids: Vec::new(),
    }
}

fn bootstrap_install_operation_id(
    transition: &TransitionIntent,
    member: &ConfigurationMember,
) -> OperationId {
    OperationId::new(format!(
        "{}:install:{}",
        transition.transition_id, member.identity.replica_id
    ))
}

fn bootstrap_initialization_command(
    snapshot: &ObservationSnapshot,
    member: &ConfigurationMember,
    observation: &crate::observation::ReplicaObservation,
) -> Result<Option<InitializeAgentStore>, String> {
    match &observation.agent {
        AgentObservation::Uninitialized(report) => {
            let kubernetes = observation
                .kubernetes
                .as_ref()
                .expect("validated uninitialized report matches scaffolding");
            let pod_uid = kubernetes
                .pod_uid
                .clone()
                .expect("validated uninitialized report has Pod UID");
            let pvc_uid = kubernetes
                .pvc_uid
                .clone()
                .expect("validated uninitialized report has PVC UID");
            let initialization_id = derive_initialization_id(
                &snapshot.resource_uid,
                member.identity.replica_id,
                &pod_uid,
                &pvc_uid,
            );
            if derive_agent_generation(&initialization_id) != member.identity.agent_generation
                || ReplicaInstanceId::new(pod_uid.as_str()) != member.identity.instance_id
            {
                return Err(format!(
                    "bootstrap member {} does not match persisted identity",
                    member.identity.replica_id
                ));
            }
            let transition = snapshot
                .status
                .transition
                .as_ref()
                .expect("bootstrap command requires persisted transition");
            Ok(Some(InitializeAgentStore {
                initialization_id,
                resource_uid: snapshot.resource_uid.clone(),
                local_replica_id: member.identity.replica_id,
                expected_instance_id: member.identity.instance_id.clone(),
                expected_pod_uid: report.pod_uid.clone(),
                expected_pvc_uid: report.pvc_uid.clone(),
                assigned_agent_generation: member.identity.agent_generation.clone(),
                effective_policy: transition.effective_policy.clone(),
                bootstrap_configuration: transition.current_configuration.clone(),
                provisioning: None,
            }))
        }

        AgentObservation::Report(report) => {
            if report.identity != member.identity {
                return Err(format!(
                    "bootstrap member {} reports identity {}@{} instead of {}@{}",
                    member.identity.replica_id,
                    report.identity.instance_id,
                    report.identity.agent_generation,
                    member.identity.instance_id,
                    member.identity.agent_generation,
                ));
            }
            Ok(None)
        }
        AgentObservation::Absent
        | AgentObservation::Unreachable { .. }
        | AgentObservation::Invalid { .. } => Ok(None),
    }
}

fn ensure_configuration_command(
    configuration: &ConfigurationDescriptor,
    member: &ConfigurationMember,
    policy: &EffectivePolicy,
    operation_id: OperationId,
    primary_write_status: AccessStatus,
    current_only: bool,
) -> EnsureConfiguration {
    EnsureConfiguration {
        operation_id,
        previous_configuration: None,
        current_configuration: configuration.clone(),
        previous_epoch: None,
        current_epoch: configuration.epoch,
        effective_policy: policy.clone(),
        local_replica_id: member.identity.replica_id,
        expected_instance_id: member.identity.instance_id.clone(),
        expected_agent_generation: member.identity.agent_generation.clone(),
        transition_kind: TransitionKind::Bootstrap,
        failover_safe_lsn: None,
        primary_write_status,
        current_only,
        retire_build_ids: Vec::new(),
        switchover_handoff: None,
        retire_switchover_preparation_ids: Vec::new(),
    }
}

fn delete_scaffolding_change(
    kubernetes: &crate::observation::KubernetesReplicaObservation,
) -> KubernetesChange {
    KubernetesChange::DeleteReplicaScaffolding {
        pod_name: (!kubernetes.pod_name.is_empty()).then(|| kubernetes.pod_name.clone()),
        pod_uid: kubernetes.pod_uid.clone(),
        pvc_name: (!kubernetes.pvc_name.is_empty()).then(|| kubernetes.pvc_name.clone()),
        pvc_uid: kubernetes.pvc_uid.clone(),
    }
}

fn incompatible_protocol_plan(
    snapshot: &ObservationSnapshot,
    config: &EvaluationConfig,
) -> Option<Plan> {
    snapshot.replicas.iter().find_map(|(key, observation)| {
        let observed = match &observation.agent {
            AgentObservation::Uninitialized(report) => report.protocol_version,
            AgentObservation::Report(report) => report.protocol_version,
            AgentObservation::Absent
            | AgentObservation::Unreachable { .. }
            | AgentObservation::Invalid { .. } => return None,
        };
        (observed != config.supported_protocol_version).then(|| {
            unsafe_plan(
                snapshot.status.clone(),
                UnsafeReason::IncompatibleProtocolVersion {
                    replica_id: key.replica_id.value(),
                    expected: config.supported_protocol_version,
                    observed,
                },
                config,
            )
        })
    })
}

fn invalid_agent_plan(snapshot: &ObservationSnapshot, config: &EvaluationConfig) -> Option<Plan> {
    snapshot.replicas.iter().find_map(|(key, observation)| {
        let AgentObservation::Invalid { message } = &observation.agent else {
            return None;
        };
        Some(unsafe_plan(
            snapshot.status.clone(),
            UnsafeReason::ContradictoryReplicaEvidence(format!(
                "replica {}@{} returned invalid evidence: {message}",
                key.replica_id, key.instance_id
            )),
            config,
        ))
    })
}

fn has_durable_replica_evidence(snapshot: &ObservationSnapshot) -> bool {
    snapshot.durable_storage_evidence
        || snapshot
            .replicas
            .values()
            .any(|observation| matches!(observation.agent, AgentObservation::Report(_)))
}

fn unsafe_plan(status: AcceptedStatus, reason: UnsafeReason, config: &EvaluationConfig) -> Plan {
    let status = clear_runtime_conditions(status)
        .with_condition(StatusCondition {
            type_: "Ready".to_string(),
            status: ConditionStatus::False,
            reason: "Unsafe".to_string(),
            message: "Replica authority is unsafe".to_string(),
        })
        .with_condition(StatusCondition {
            type_: "Unsafe".to_string(),
            status: ConditionStatus::True,
            reason: "UnsafeAuthority".to_string(),
            message: format!("{reason:?}"),
        });
    Plan::Unsafe {
        reason,
        status,
        safety_changes: vec![SafetyChange::RemoveWriteRouting],
        requeue_after_seconds: config.unsafe_requeue_seconds,
    }
}

fn ready_condition() -> StatusCondition {
    StatusCondition {
        type_: "Ready".to_string(),
        status: ConditionStatus::True,
        reason: "Stable".to_string(),
        message: "Accepted topology is stable".to_string(),
    }
}

fn progressing_condition(reason: &str, message: &str) -> StatusCondition {
    StatusCondition {
        type_: "Progressing".to_string(),
        status: ConditionStatus::True,
        reason: reason.to_string(),
        message: message.to_string(),
    }
}

fn unsupported_replica_count_condition(requested: u32, frozen: u32) -> StatusCondition {
    StatusCondition {
        type_: "UnsupportedSpec".to_string(),
        status: ConditionStatus::True,
        reason: "ReplicaCountImmutable".to_string(),
        message: format!(
            "requested replica count {requested} differs from frozen replica-set size {frozen}"
        ),
    }
}

fn desired_spec_state(
    snapshot: &ObservationSnapshot,
    configuration: &ConfigurationDescriptor,
    policy: &EffectivePolicy,
) -> (bool, Option<StatusCondition>) {
    let mut differences = Vec::new();
    if snapshot.desired.replicas != policy.replica_set_size {
        differences.push(format!(
            "requested replica count {} differs from frozen replica-set size {}",
            snapshot.desired.replicas, policy.replica_set_size
        ));
    }
    if snapshot.desired.failover_delay_seconds != policy.failover_delay_seconds {
        differences.push(format!(
            "requested failover delay {} differs from frozen delay {}",
            snapshot.desired.failover_delay_seconds, policy.failover_delay_seconds
        ));
    }

    let mut observed_images = 0_usize;
    for member in &configuration.members {
        let Some(image) = snapshot
            .observation_for_identity(&member.identity)
            .and_then(|observation| observation.kubernetes.as_ref())
            .and_then(|kubernetes| kubernetes.image.as_deref())
        else {
            continue;
        };
        observed_images += 1;
        if image != snapshot.desired.image {
            differences.push(format!(
                "replica {} runs image {image} instead of requested image {}",
                member.identity.replica_id, snapshot.desired.image
            ));
        }
    }

    if differences.is_empty() {
        return (observed_images == configuration.members.len(), None);
    }

    let condition =
        if differences.len() == 1 && snapshot.desired.replicas != policy.replica_set_size {
            unsupported_replica_count_condition(snapshot.desired.replicas, policy.replica_set_size)
        } else {
            StatusCondition {
                type_: "UnsupportedSpec".to_string(),
                status: ConditionStatus::True,
                reason: "SpecDriftUnsupported".to_string(),
                message: differences.join("; "),
            }
        };
    (false, Some(condition))
}

fn waiting_status(status: AcceptedStatus, reason: &str, message: &str) -> AcceptedStatus {
    clear_runtime_conditions(status)
        .with_condition(StatusCondition {
            type_: "Ready".to_string(),
            status: ConditionStatus::Unknown,
            reason: reason.to_string(),
            message: message.to_string(),
        })
        .with_condition(progressing_condition(reason, message))
}

fn transition_status(status: AcceptedStatus) -> AcceptedStatus {
    waiting_status(
        status.without_condition("UnsupportedSpec"),
        "TransitionActive",
        "Persisted transition remains in progress",
    )
}

fn clear_evaluator_conditions(status: AcceptedStatus) -> AcceptedStatus {
    clear_runtime_conditions(status).without_condition("UnsupportedSpec")
}

fn clear_runtime_conditions(status: AcceptedStatus) -> AcceptedStatus {
    status
        .without_condition("Ready")
        .without_condition("Unsafe")
        .without_condition("Progressing")
}
