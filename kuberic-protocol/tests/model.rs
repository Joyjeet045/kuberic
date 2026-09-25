use std::collections::BTreeMap;

use kuberic_protocol::command::{KubernetesChange, ProtocolCommand};
use kuberic_protocol::evaluator::{EvaluationConfig, evaluate};
use kuberic_protocol::observation::{
    AgentObservation, AgentReport, DesiredState, KubernetesReplicaObservation, ObservationSnapshot,
    ReplicaObservation, ReplicaObservationKey, ReportWatermark, RoutingObservation,
};
use kuberic_protocol::plan::Plan;
use kuberic_protocol::types::{
    AcceptedStatus, AcceptedTopology, AccessStatus, AgentGeneration, ConfigurationDescriptor,
    ConfigurationMember, EffectivePolicy, Epoch, OperationId, PlannedSwitchoverOutcome,
    PlannedSwitchoverRequest, PodUid, ProcessSessionId, ProvisioningIntent, PvcUid, ReplicaId,
    ReplicaIdentity, ReplicaInstanceId, ReplicaRole, ResourceUid, SwitchoverHandoff,
    SwitchoverRequestId, TransitionIntent, TransitionKind, derive_transition_id,
};
use kuberic_protocol::validation::{ValidationError, validate_snapshot, validate_status};

fn identity(replica_id: i64, incarnation: u64) -> ReplicaIdentity {
    ReplicaIdentity {
        replica_id: ReplicaId::new(replica_id),
        instance_id: ReplicaInstanceId::new(format!("pod-{replica_id}-{incarnation}")),
        agent_generation: AgentGeneration::new(format!("generation-{replica_id}-{incarnation}")),
    }
}

fn policy(replica_set_size: u32) -> EffectivePolicy {
    EffectivePolicy::fixed(replica_set_size, 7).unwrap()
}

fn configuration(
    replica_set_size: u32,
    epoch: Epoch,
    primary_id: i64,
    incarnations: &[u64],
) -> ConfigurationDescriptor {
    let effective_policy = policy(replica_set_size);
    ConfigurationDescriptor::new(
        epoch,
        ReplicaId::new(primary_id),
        (1..=i64::from(replica_set_size))
            .map(|replica_id| ConfigurationMember {
                identity: identity(replica_id, incarnations[replica_id as usize - 1]),
                role: if replica_id == primary_id {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        effective_policy.write_quorum,
    )
}

fn stable_status(
    configuration: ConfigurationDescriptor,
    effective_policy: EffectivePolicy,
) -> AcceptedStatus {
    AcceptedStatus {
        initialized: true,
        observed_generation: 1,
        effective_policy: Some(effective_policy),
        topology: Some(AcceptedTopology { configuration }),
        ..AcceptedStatus::default()
    }
}

fn transition_status(
    previous: &ConfigurationDescriptor,
    current: ConfigurationDescriptor,
    effective_policy: EffectivePolicy,
    kind: TransitionKind,
) -> AcceptedStatus {
    let resource_uid = ResourceUid::new("model-resource");
    let build_id = (kind == TransitionKind::Replacement)
        .then(|| OperationId::new(format!("build-{}", current.epoch.configuration_number)));
    AcceptedStatus {
        transition: Some(TransitionIntent {
            transition_id: derive_transition_id(&resource_uid, kind, &current.configuration_id),
            kind,
            spec_generation: 1,
            effective_policy: effective_policy.clone(),
            previous_configuration_id: Some(previous.configuration_id.clone()),
            current_configuration: current,
            election_lsn: (kind == TransitionKind::Failover).then_some(100),
            build_id,
            repair: None,
            switchover: None,
        }),
        ..stable_status(previous.clone(), effective_policy)
    }
}

#[test]
fn generated_transition_traces_preserve_authority_invariants() {
    for replica_set_size in [3_u32, 5] {
        let effective_policy = policy(replica_set_size);
        let mut incarnations = vec![1; replica_set_size as usize];
        let mut accepted = configuration(replica_set_size, Epoch::new(0, 1), 1, &incarnations);
        let mut status = stable_status(accepted.clone(), effective_policy.clone());
        validate_status(&status).unwrap();

        for step in 2..=7 {
            let kind = if step % 2 == 0 {
                TransitionKind::Failover
            } else {
                TransitionKind::Replacement
            };
            let primary_id = if kind == TransitionKind::Failover {
                accepted.primary_id.value() % i64::from(replica_set_size) + 1
            } else {
                accepted.primary_id.value()
            };
            if kind == TransitionKind::Replacement {
                let replace = (1..=i64::from(replica_set_size))
                    .find(|replica_id| *replica_id != primary_id)
                    .unwrap();
                incarnations[replace as usize - 1] += 1;
            }
            let current = configuration(
                replica_set_size,
                Epoch::new(0, step),
                primary_id,
                &incarnations,
            );
            status = transition_status(&accepted, current.clone(), effective_policy.clone(), kind);
            validate_status(&status).unwrap();

            let transition = status.transition.as_ref().unwrap();
            assert_eq!(
                transition.current_configuration.members.len(),
                replica_set_size as usize
            );
            assert_eq!(transition.effective_policy, effective_policy);
            assert!(transition.current_configuration.epoch > accepted.epoch);
            assert_eq!(
                transition.previous_configuration_id.as_ref(),
                Some(&accepted.configuration_id)
            );

            let mut conflicting = status.clone();
            let replaced = accepted
                .members
                .iter()
                .find(|member| member.identity.replica_id != accepted.primary_id)
                .unwrap()
                .identity
                .clone();
            conflicting.provisioning = Some(ProvisioningIntent {
                replaces: replaced,
                pod_uid: PodUid::new("conflicting-pod"),
                pvc_uid: PvcUid::new("conflicting-pvc"),
                operation_id: OperationId::new("conflicting-provisioning"),
            });
            assert_eq!(
                validate_status(&conflicting),
                Err(ValidationError::ProvisioningAndTransition)
            );

            let mut changed_policy = status.clone();
            changed_policy
                .transition
                .as_mut()
                .unwrap()
                .effective_policy
                .failover_delay_seconds += 1;
            assert_eq!(
                validate_status(&changed_policy),
                Err(ValidationError::TransitionPolicyMismatch)
            );

            let mut accepted_too_early = status.clone();
            accepted_too_early.topology = Some(AcceptedTopology {
                configuration: current.clone(),
            });
            assert!(matches!(
                validate_status(&accepted_too_early),
                Err(ValidationError::PreviousConfigurationMismatch { .. })
            ));

            accepted = current;
            status = stable_status(accepted.clone(), effective_policy.clone());
            validate_status(&status).unwrap();
        }
    }
}

#[test]
fn generated_report_sets_never_validate_two_granted_writers() {
    for replica_set_size in [1_u32, 3, 5] {
        let effective_policy = policy(replica_set_size);
        let configuration = configuration(
            replica_set_size,
            Epoch::new(0, 4),
            1,
            &vec![1; replica_set_size as usize],
        );
        let writer_masks = 0_u64..(1_u64 << replica_set_size);
        for writer_mask in writer_masks {
            let mut replicas = BTreeMap::new();
            for (index, member) in configuration.members.iter().enumerate() {
                let claims_write = writer_mask & (1 << index) != 0;
                replicas.insert(
                    ReplicaObservationKey::new(
                        member.identity.replica_id,
                        member.identity.instance_id.clone(),
                    ),
                    ReplicaObservation {
                        kubernetes: None,
                        agent: AgentObservation::Report(Box::new(AgentReport {
                            protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                            resource_uid: ResourceUid::new("model-resource"),
                            identity: member.identity.clone(),
                            process_session_id: ProcessSessionId::new(format!(
                                "session-{}",
                                member.identity.replica_id
                            )),
                            report_sequence: 1,
                            role: if claims_write {
                                ReplicaRole::Primary
                            } else {
                                member.role
                            },
                            read_status: if claims_write {
                                AccessStatus::Granted
                            } else {
                                AccessStatus::NotPrimary
                            },
                            write_status: if claims_write {
                                AccessStatus::Granted
                            } else {
                                AccessStatus::NotPrimary
                            },
                            healthy: true,
                            epoch: configuration.epoch,
                            current_configuration: Some(configuration.clone()),
                            current_progress: 10,
                            verified_replication_lsn: Some(10),
                            committed_lsn: 10,
                            ..AgentReport::default()
                        })),
                    },
                );
            }
            let snapshot = ObservationSnapshot {
                resource_uid: ResourceUid::new("model-resource"),
                resource_version: "1".to_string(),
                desired: DesiredState {
                    generation: 1,
                    replicas: replica_set_size,
                    image: "example:v1".to_string(),
                    failover_delay_seconds: effective_policy.failover_delay_seconds,
                    switchover: None,
                },
                status: stable_status(configuration.clone(), effective_policy.clone()),
                replicas,
                previous_report_watermarks: BTreeMap::new(),
                durable_storage_evidence: true,
                supporting_resources_ready: true,
                routing: RoutingObservation::default(),
                observation_failures: Vec::new(),
                now_unix_seconds: 100,
            };
            if validate_snapshot(&snapshot).is_ok() {
                assert!(
                    writer_mask.count_ones() <= 1,
                    "validated more than one granted writer for mask {writer_mask:b}"
                );
                if writer_mask.count_ones() == 1 {
                    assert_eq!(writer_mask, 1, "only the configured primary may write");
                }
            }
        }
    }
}

#[test]
fn generated_transitions_reject_non_monotonic_epochs() {
    for configuration_number in 1..=5 {
        let effective_policy = policy(3);
        let accepted = configuration(3, Epoch::new(0, configuration_number), 1, &[1, 1, 1]);
        for next in 0..=configuration_number {
            let current = configuration(3, Epoch::new(0, next), 2, &[1, 1, 1]);
            let status = transition_status(
                &accepted,
                current,
                effective_policy.clone(),
                TransitionKind::Failover,
            );
            assert_eq!(
                validate_status(&status),
                Err(ValidationError::TransitionEpochNotNewer)
            );
        }
    }
}

#[derive(Clone)]
struct SwitchoverModel {
    snapshot: ObservationSnapshot,
    physical: BTreeMap<ReplicaObservationKey, AgentReport>,
    starting: ConfigurationDescriptor,
    steps: usize,
}

impl SwitchoverModel {
    fn new(size: u32) -> Self {
        let starting = configuration(size, Epoch::new(4, 17), 1, &vec![1; size as usize]);
        let mut replicas = BTreeMap::new();
        for member in &starting.members {
            let id = member.identity.replica_id;
            replicas.insert(
                ReplicaObservationKey::new(id, member.identity.instance_id.clone()),
                ReplicaObservation {
                    kubernetes: Some(KubernetesReplicaObservation {
                        replica_id: id,
                        pod_name: format!("replica-{id}"),
                        pod_uid: Some(PodUid::new(member.identity.instance_id.as_str())),
                        pvc_name: format!("data-{id}"),
                        pvc_uid: Some(PvcUid::new(format!("pvc-{id}"))),
                        image: Some("model:v2".into()),
                        pod_ready: true,
                        peer_endpoint_ready: true,
                    }),
                    agent: AgentObservation::Report(Box::new(AgentReport {
                        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                        resource_uid: ResourceUid::new("model-resource"),
                        identity: member.identity.clone(),
                        process_session_id: ProcessSessionId::new(format!("initial-{id}")),
                        report_sequence: 1,
                        role: member.role,
                        write_status: if id == ReplicaId::new(1) {
                            AccessStatus::Granted
                        } else {
                            AccessStatus::NotPrimary
                        },
                        healthy: true,
                        epoch: starting.epoch,
                        current_configuration: Some(starting.clone()),
                        current_progress: 10,
                        committed_lsn: 10,
                        verified_replication_lsn: Some(10),
                        ..AgentReport::default()
                    })),
                },
            );
        }
        Self {
            physical: replicas
                .iter()
                .map(|(key, observation)| {
                    let AgentObservation::Report(report) = &observation.agent else {
                        unreachable!()
                    };
                    (key.clone(), *report.clone())
                })
                .collect(),
            snapshot: ObservationSnapshot {
                resource_uid: ResourceUid::new("model-resource"),
                resource_version: "1".into(),
                desired: DesiredState {
                    generation: 2,
                    replicas: size,
                    image: "model:v2".into(),
                    failover_delay_seconds: policy(size).failover_delay_seconds,
                    switchover: Some(PlannedSwitchoverRequest {
                        request_id: SwitchoverRequestId::new("model-move"),
                        target_replica_id: ReplicaId::new(2),
                    }),
                },
                status: stable_status(starting.clone(), policy(size)),
                replicas,
                previous_report_watermarks: BTreeMap::new(),
                durable_storage_evidence: true,
                supporting_resources_ready: true,
                routing: RoutingObservation {
                    service_present: true,
                    write_target: Some(starting.members[0].identity.clone()),
                    ..RoutingObservation::default()
                },
                observation_failures: Vec::new(),
                now_unix_seconds: 100,
            },
            starting,
            steps: 0,
        }
    }

    fn report_mut(&mut self, id: ReplicaId) -> &mut AgentReport {
        let AgentObservation::Report(report) = &mut self
            .snapshot
            .replicas
            .get_mut(&ReplicaObservationKey::new(
                id,
                identity(id.value(), 1).instance_id,
            ))
            .unwrap()
            .agent
        else {
            panic!("report {id}")
        };
        report
    }

    fn invariants(&self) {
        let status = &self.snapshot.status;
        validate_status(status).unwrap();
        assert_eq!(
            status.effective_policy,
            Some(policy(self.starting.members.len() as u32))
        );
        assert!(status.provisioning.is_none());
        assert!(status.primary_failure.is_none());
        let check_configuration = |cc: &ConfigurationDescriptor| {
            assert_eq!(
                cc.epoch.data_loss_number,
                self.starting.epoch.data_loss_number
            );
            assert!(cc.epoch >= self.starting.epoch);
            assert_eq!(cc.write_quorum, self.starting.write_quorum);
            assert_eq!(
                cc.members.iter().map(|m| &m.identity).collect::<Vec<_>>(),
                self.starting
                    .members
                    .iter()
                    .map(|m| &m.identity)
                    .collect::<Vec<_>>()
            );
        };
        check_configuration(&status.topology.as_ref().unwrap().configuration);
        if let Some(transition) = &status.transition {
            assert_eq!(transition.kind, TransitionKind::PlannedSwitchover);
            assert_eq!(
                Some(&transition.effective_policy),
                status.effective_policy.as_ref()
            );
            check_configuration(&transition.current_configuration);
            let intent = transition.switchover.as_ref().unwrap();
            assert_eq!(intent.request_id.as_str(), "model-move");
            assert_eq!(intent.source, self.starting.members[0].identity);
            assert_eq!(intent.target, self.starting.members[1].identity);
            check_configuration(&intent.requested_configuration);
        }
        // Unreachable replicas still exist and may serve retained direct clients.
        let writers: Vec<_> = self
            .physical
            .values()
            .filter(|report| report.write_status == AccessStatus::Granted)
            .collect();
        assert!(writers.len() <= 1);
        for writer in writers {
            let accepted = &status.topology.as_ref().unwrap().configuration;
            assert_eq!(writer.identity.replica_id, accepted.primary_id);
            assert_eq!(writer.current_configuration.as_ref(), Some(accepted));
            assert!(writer.previous_configuration.is_none());
        }
        // Routing is allowed to lag absence, but not authority or write admission.
        if let Some(target) = &self.snapshot.routing.write_target {
            assert_eq!(
                target.replica_id,
                status.topology.as_ref().unwrap().configuration.primary_id
            );
            if let Some(report) = self.physical.get(&ReplicaObservationKey::new(
                target.replica_id,
                target.instance_id.clone(),
            )) {
                assert_eq!(report.write_status, AccessStatus::Granted);
            }
        }
        assert!(
            self.snapshot.replicas.values().all(|r| r
                .kubernetes
                .as_ref()
                .unwrap()
                .pvc_uid
                .is_some())
        );
    }

    fn lose_pod(&mut self, id: ReplicaId) {
        self.physical.retain(|key, _| key.replica_id != id);
        let observation = self
            .snapshot
            .replicas
            .values_mut()
            .find(|r| r.kubernetes.as_ref().unwrap().replica_id == id)
            .unwrap();
        let pod = observation.kubernetes.as_mut().unwrap();
        pod.pod_uid = None;
        pod.pod_name.clear();
        pod.pod_ready = false;
        observation.agent = AgentObservation::Absent;
    }

    fn step(&mut self) -> bool {
        self.invariants();
        let before = self
            .snapshot
            .status
            .topology
            .as_ref()
            .unwrap()
            .configuration
            .epoch;
        let plan = evaluate(&self.snapshot, &EvaluationConfig::default());
        assert_eq!(
            plan,
            evaluate(&self.snapshot, &EvaluationConfig::default()),
            "lost reply must reproduce the same operation"
        );
        let mut done = false;
        match plan {
            Plan::Apply { changes } => {
                for change in changes {
                    match change {
                        KubernetesChange::PersistStatus { status } => {
                            self.snapshot.status = *status
                        }
                        KubernetesChange::RemoveWriteRouting => {
                            self.snapshot.routing.write_target = None
                        }
                        KubernetesChange::PublishWriteRouting { primary } => {
                            assert!(self.snapshot.status.transition.is_none());
                            assert!(self.snapshot.status.last_switchover.is_some());
                            assert_eq!(
                                self.report_mut(primary.replica_id).write_status,
                                AccessStatus::Granted
                            );
                            self.snapshot.routing.write_target = Some(primary);
                        }
                        KubernetesChange::DeleteExactPod { pod_uid, .. } => {
                            let id = self
                                .snapshot
                                .replicas
                                .values()
                                .find(|r| {
                                    r.kubernetes.as_ref().unwrap().pod_uid.as_ref()
                                        == Some(&pod_uid)
                                })
                                .unwrap()
                                .kubernetes
                                .as_ref()
                                .unwrap()
                                .replica_id;
                            self.lose_pod(id);
                        }
                        other => panic!(
                            "membership/storage/destructive recovery is forbidden: {other:?}"
                        ),
                    }
                }
            }
            Plan::Execute {
                command: ProtocolCommand::PrepareSwitchover(command),
            } => {
                assert!(self.snapshot.routing.write_target.is_none());
                let report = self.report_mut(command.local_replica_id);
                report.write_status = AccessStatus::ReconfigurationPending;
                report.prepared_switchover = Some(SwitchoverHandoff {
                    preparation_generation: command.preparation_generation,
                    preparation_operation_id: command.operation_id,
                    request_id: command.request_id,
                    source: command.source,
                    target: command.target,
                    starting_configuration_id: command.current_configuration.configuration_id,
                    starting_epoch: command.current_configuration.epoch,
                    handoff_lsn: 10,
                });
            }
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(command),
            } => {
                if command.primary_write_status == AccessStatus::Granted {
                    assert!(self.snapshot.status.transition.is_none());
                    assert!(self.snapshot.status.last_switchover.is_some());
                }
                assert!(command.failover_safe_lsn.is_none());
                assert!(command.retire_build_ids.is_empty());
                assert_eq!(
                    command.effective_policy,
                    policy(self.starting.members.len() as u32)
                );
                let report = self.report_mut(command.local_replica_id);
                assert!(
                    command.current_epoch >= report.epoch,
                    "replica epoch regression"
                );
                assert_eq!(command.expected_instance_id, report.identity.instance_id);
                assert_eq!(
                    command.expected_agent_generation,
                    report.identity.agent_generation
                );
                report.epoch = command.current_epoch;
                report.current_configuration = Some(command.current_configuration.clone());
                report.previous_configuration = command.previous_configuration.clone();
                report.role = command
                    .current_configuration
                    .members
                    .iter()
                    .find(|m| m.identity == report.identity)
                    .unwrap()
                    .role;
                report.write_status = if report.role == ReplicaRole::Primary {
                    command.primary_write_status
                } else {
                    AccessStatus::NotPrimary
                };
                report.retained_operation_id = Some(command.operation_id);
                report.pending_operation_id = None;
                report.catch_up_boundary = command.previous_configuration.map(|_| 10);
                report.catch_up_complete = true;
                report.current_configuration_quorum_progress = 10;
                if !command.retire_switchover_preparation_ids.is_empty() {
                    report.prepared_switchover = None;
                }
            }
            Plan::Execute { command } => panic!("independent transition: {command:?}"),
            Plan::Stable { status, .. } => {
                self.snapshot.status = status;
                done = true;
            }
            Plan::Wait {
                status,
                requeue_after_seconds,
                ..
            } => {
                assert!(requeue_after_seconds > 0);
                assert!(!status.conditions.is_empty());
                self.snapshot.status = status;
            }
            Plan::Unsafe { status, .. } => {
                assert_eq!(
                    status.last_switchover.as_ref().unwrap().outcome,
                    PlannedSwitchoverOutcome::Unsafe
                );
                self.snapshot.status = status;
                self.snapshot.routing.write_target = None;
                done = true;
            }
        }
        assert!(
            self.snapshot
                .status
                .topology
                .as_ref()
                .unwrap()
                .configuration
                .epoch
                >= before
        );
        // Simulate controller persistence/restart and alternating process-session rollover.
        self.snapshot.status =
            serde_json::from_slice(&serde_json::to_vec(&self.snapshot.status).unwrap()).unwrap();
        self.steps += 1;
        for (key, observation) in &mut self.snapshot.replicas {
            if let AgentObservation::Report(report) = &mut observation.agent {
                self.physical.insert(key.clone(), *report.clone());
                self.snapshot.previous_report_watermarks.insert(
                    key.clone(),
                    ReportWatermark {
                        process_session_id: report.process_session_id.clone(),
                        report_sequence: report.report_sequence,
                    },
                );
                if self.steps % 2 == 0 {
                    report.process_session_id =
                        ProcessSessionId::new(format!("{}-{}", key.replica_id, self.steps));
                    report.report_sequence = 1;
                } else {
                    report.report_sequence += 1;
                }
            }
        }
        self.invariants();
        done
    }

    fn finish(&mut self, expected: PlannedSwitchoverOutcome) {
        for _ in 0..60 {
            // Stop at the receipt: ordinary repair after terminal completion is a separate operation.
            if let Some(receipt) = &self.snapshot.status.last_switchover {
                assert_eq!(receipt.outcome, expected);
                return;
            }
            self.step();
        }
        panic!("no terminal receipt: {:?}", self.snapshot);
    }
}

#[test]
fn generated_switchover_traces_cover_availability_sessions_retries_and_frozen_requests() {
    for size in [3, 4, 5] {
        let mut baseline = SwitchoverModel::new(size);
        baseline.step(); // Freeze the exact request before exploring faults.
        let mut boundaries = 0;
        while baseline.snapshot.status.last_switchover.is_none() {
            for mask in 0..(1 << size) {
                let mut trace = baseline.clone();
                let agents = trace.snapshot.replicas.clone();
                for (index, observation) in trace.snapshot.replicas.values_mut().enumerate() {
                    if mask & (1 << index) == 0 {
                        observation.agent = AgentObservation::Unreachable {
                            message: "partition".into(),
                        };
                    }
                }
                for _ in 0..3 {
                    trace.step();
                }
                assert!(trace.snapshot.status.last_switchover.as_ref().is_none_or(
                    |r| r.outcome == PlannedSwitchoverOutcome::RequestedTargetCompleted
                ));
                // Heal reports without a watch or a new user request.
                for (key, original) in agents {
                    if matches!(
                        trace.snapshot.replicas[&key].agent,
                        AgentObservation::Unreachable { .. }
                    ) {
                        trace.snapshot.replicas.get_mut(&key).unwrap().agent = original.agent;
                    }
                }
                trace.snapshot.previous_report_watermarks.clear();
                trace.finish(PlannedSwitchoverOutcome::RequestedTargetCompleted);
            }
            for mutation in 0..3 {
                let mut trace = baseline.clone();
                trace.snapshot.desired.switchover = match mutation {
                    0 => None,
                    1 => Some(PlannedSwitchoverRequest {
                        request_id: SwitchoverRequestId::new("model-move"),
                        target_replica_id: ReplicaId::new(3),
                    }),
                    _ => Some(PlannedSwitchoverRequest {
                        request_id: SwitchoverRequestId::new("second-request"),
                        target_replica_id: ReplicaId::new(2),
                    }),
                };
                trace.snapshot.desired.replicas = size + 2;
                trace.step();
                assert!(
                    trace
                        .snapshot
                        .status
                        .conditions
                        .iter()
                        .any(|c| c.reason == "ActiveRequestImmutable")
                );
                trace.snapshot.desired = baseline.snapshot.desired.clone();
                trace.finish(PlannedSwitchoverOutcome::RequestedTargetCompleted);
            }
            baseline.step();
            boundaries += 1;
            assert!(boundaries < 40);
        }
        assert!(boundaries >= 2 * size as usize + 3);
        for _ in 0..5 {
            baseline.step();
        }
        assert!(baseline.snapshot.status.transition.is_none());
        assert_eq!(
            baseline
                .snapshot
                .routing
                .write_target
                .as_ref()
                .unwrap()
                .replica_id,
            ReplicaId::new(2)
        );
    }
}

#[test]
fn generated_switchover_loss_traces_restore_compensate_or_close_without_epoch_rollback() {
    for size in [3, 5] {
        let mut baseline = SwitchoverModel::new(size);
        baseline.step();
        while baseline.snapshot.status.last_switchover.is_none() {
            for lost in [1, 2, 3] {
                let mut trace = baseline.clone();
                if lost & 1 != 0 {
                    trace.lose_pod(ReplicaId::new(1));
                }
                if lost & 2 != 0 {
                    trace.lose_pod(ReplicaId::new(2));
                }
                let admitted = baseline.snapshot.replicas.values().any(|r|
                            matches!(&r.agent, AgentObservation::Report(report) if report.epoch > baseline.starting.epoch));
                let expected = if lost & 1 != 0 {
                    PlannedSwitchoverOutcome::Unsafe
                } else if admitted {
                    PlannedSwitchoverOutcome::OldPrimaryCompensated
                } else {
                    PlannedSwitchoverOutcome::OldPrimaryRestored
                };
                trace.finish(expected);
                let epoch = trace
                    .snapshot
                    .status
                    .topology
                    .as_ref()
                    .unwrap()
                    .configuration
                    .epoch;
                if expected == PlannedSwitchoverOutcome::OldPrimaryCompensated {
                    assert!(
                        epoch.configuration_number
                            > baseline.starting.epoch.configuration_number + 1
                    );
                }
                if expected == PlannedSwitchoverOutcome::OldPrimaryRestored {
                    assert_eq!(epoch, baseline.starting.epoch);
                }
                if expected != PlannedSwitchoverOutcome::Unsafe {
                    let key =
                        ReplicaObservationKey::new(ReplicaId::new(2), identity(2, 1).instance_id);
                    let orphan = trace.snapshot.replicas.remove(&key).unwrap();
                    trace.snapshot.replicas.insert(
                        ReplicaObservationKey::new(
                            ReplicaId::new(2),
                            ReplicaInstanceId::new("orphan-pvc-2"),
                        ),
                        orphan,
                    );
                    trace.step(); // Grant only after the durable terminal receipt.
                    trace.step(); // Publish only after observing the exact write grant.
                    assert_eq!(
                        trace
                            .snapshot
                            .routing
                            .write_target
                            .as_ref()
                            .unwrap()
                            .replica_id,
                        ReplicaId::new(1)
                    );
                } else {
                    for _ in 0..3 {
                        assert!(trace.step());
                    }
                }
            }
            baseline.step();
        }
    }
}

fn terminal_switchover_model(outcome: &str) -> SwitchoverModel {
    let mut model = SwitchoverModel::new(3);
    model.step();
    if outcome == "compensated" {
        let starting_epoch = model.starting.epoch;
        while model.report_mut(ReplicaId::new(1)).epoch == starting_epoch {
            model.step();
        }
    }
    if matches!(outcome, "restored" | "compensated" | "unsafe") {
        model.lose_pod(ReplicaId::new(2));
    }
    if outcome == "unsafe" {
        model.lose_pod(ReplicaId::new(1));
    }
    model.finish(match outcome {
        "requested" => PlannedSwitchoverOutcome::RequestedTargetCompleted,
        "restored" => PlannedSwitchoverOutcome::OldPrimaryRestored,
        "compensated" => PlannedSwitchoverOutcome::OldPrimaryCompensated,
        "unsafe" => PlannedSwitchoverOutcome::Unsafe,
        _ => unreachable!(),
    });
    model
}

#[test]
fn terminal_switchover_receipts_survive_process_exit_and_do_not_allocate_again() {
    std::fs::create_dir_all("target").unwrap();
    for outcome in ["requested", "restored", "compensated", "unsafe"] {
        let path = format!(
            "target/switchover-receipt-{}-{outcome}.json",
            std::process::id()
        );
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "terminal_switchover_receipt_writer_process",
            ])
            .env("KUBERIC_MODEL_RECEIPT_PATH", &path)
            .env("KUBERIC_MODEL_RECEIPT_OUTCOME", outcome)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(73),
            "{outcome}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let mut model = terminal_switchover_model(outcome);
        let persisted: AcceptedStatus =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        std::fs::remove_file(path).unwrap();
        assert_eq!(persisted, model.snapshot.status);
        model.snapshot.status = persisted;
        let receipt = model.snapshot.status.last_switchover.clone();
        for _ in 0..3 {
            // Don't enter ordinary post-terminal replacement of a missing target.
            if outcome == "requested" || outcome == "unsafe" {
                model.step();
            } else {
                let plan = evaluate(&model.snapshot, &EvaluationConfig::default());
                assert!(!matches!(
                    plan,
                    Plan::Execute {
                        command: ProtocolCommand::PrepareSwitchover(_)
                    }
                ));
                assert!(model.snapshot.status.transition.is_none());
            }
            assert_eq!(model.snapshot.status.last_switchover, receipt);
        }
    }
}

#[test]
#[ignore = "subprocess helper for durable terminal switchover receipts"]
fn terminal_switchover_receipt_writer_process() {
    let (Ok(path), Ok(outcome)) = (
        std::env::var("KUBERIC_MODEL_RECEIPT_PATH"),
        std::env::var("KUBERIC_MODEL_RECEIPT_OUTCOME"),
    ) else {
        return;
    };
    let model = terminal_switchover_model(&outcome);
    let file = std::fs::File::create(&path).unwrap();
    serde_json::to_writer(&file, &model.snapshot.status).unwrap();
    file.sync_all().unwrap();
    std::fs::File::open("target").unwrap().sync_all().unwrap();
    std::process::exit(73);
}
