//! Deterministic reconciliation decisions over a normalized observation.

use crate::command::{
    EnsureConfiguration, EnsureReplicaBuild, InitializeAgentStore, KubernetesChange,
    ProtocolCommand, SafetyChange,
};
use crate::observation::{AgentObservation, ObservationSnapshot};
use crate::plan::{Plan, UnsafeReason, WaitReason};
use crate::types::{
    AcceptedStatus, AcceptedTopology, AccessStatus, ConditionStatus, ConfigurationDescriptor,
    ConfigurationMember, EffectivePolicy, Epoch, OperationId, PrimaryFailureObservation,
    ProvisioningIntent, QuorumLossObservation, ReplicaIdentity, ReplicaInstanceId,
    ReplicaRepairIntent, ReplicaRole, StatusCondition, TransitionIntent, TransitionKind,
    derive_agent_generation, derive_failover_repair_operation_id, derive_initialization_id,
    derive_replacement_operation_id, derive_transition_id,
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
    if let Err(error) = validate_snapshot(snapshot) {
        let reason = if error == ValidationError::DesiredReplicasZero {
            UnsafeReason::InvalidDesiredState(error.to_string())
        } else {
            UnsafeReason::InvalidAcceptedAuthority(error.to_string())
        };
        return unsafe_plan(snapshot.status.clone(), reason, config);
    }

    if let Some(plan) = incompatible_protocol_plan(snapshot, config) {
        return plan;
    }
    if let Some(plan) = invalid_agent_plan(snapshot, config) {
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

    if let Some(failed) = configuration.members.iter().find(|member| {
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
    }) {
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
    status = status.with_condition(ready_condition());
    Plan::Stable {
        status,
        requeue_after_seconds: config.stable_resync_seconds,
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
