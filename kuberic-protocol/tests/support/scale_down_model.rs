use std::collections::BTreeMap;

use kuberic_protocol::command::{KubernetesChange, ProtocolCommand, ScaleDownResource};
use kuberic_protocol::evaluator::{EvaluationConfig, evaluate};
use kuberic_protocol::observation::*;
use kuberic_protocol::plan::Plan;
use kuberic_protocol::types::*;
use kuberic_protocol::validation::{validate_secondary_removal_configuration, validate_snapshot};

#[path = "secondary_scale_down.rs"]
pub mod fixture;

pub fn config() -> EvaluationConfig {
    EvaluationConfig {
        enable_secondary_scale_down: true,
        ..EvaluationConfig::default()
    }
}

#[derive(Clone, Copy, Debug)]
pub enum CommandBoundary {
    Intent,
    Pending,
    Effect,
}

#[derive(Clone)]
pub struct Model {
    pub snapshot: ObservationSnapshot,
    pub commands: Vec<ProtocolCommand>,
    pub removed: Vec<ReplicaIdentity>,
    pub deletes: Vec<(ScaleDownResource, String)>,
    pub inflight: BTreeMap<ReplicaId, ProtocolCommand>,
    pub applied_effects: BTreeMap<ReplicaId, AgentReport>,
}

impl Model {
    pub fn new(ids: &[i64], primary: i64, desired: u32) -> Self {
        let intent = fixture::intent(ids, primary);
        let configuration = intent.previous_configuration.clone();
        let mut snapshot = ObservationSnapshot {
            resource_uid: intent.resource_uid,
            resource_version: "1".into(),
            desired: DesiredState {
                generation: 7,
                replicas: desired,
                image: "example:v1".into(),
                failover_delay_seconds: 30,
                switchover: None,
            },
            status: AcceptedStatus {
                initialized: true,
                observed_generation: 6,
                effective_policy: Some(intent.previous_policy),
                topology: Some(AcceptedTopology {
                    configuration: configuration.clone(),
                }),
                ..AcceptedStatus::default()
            },
            replicas: BTreeMap::new(),
            secondary_scale_down_resources: Vec::new(),
            previous_report_watermarks: BTreeMap::new(),
            durable_storage_evidence: true,
            supporting_resources_ready: true,
            routing: RoutingObservation {
                service_present: true,
                write_target: Some(intent.primary),
                unresolved_write_target: false,
            },
            observation_failures: Vec::new(),
            now_unix_seconds: 100,
        };
        for member in &configuration.members {
            let identity = member.identity.clone();
            let id = identity.replica_id;
            let pod_name = format!("db-{id}");
            let pvc_name = format!("db-storage-{id}");
            let pvc_uid = format!("pvc-{id}");
            snapshot.replicas.insert(
                ReplicaObservationKey::new(id, identity.instance_id.clone()),
                ReplicaObservation {
                    kubernetes: Some(KubernetesReplicaObservation {
                        replica_id: id,
                        pod_name: pod_name.clone(),
                        pod_uid: Some(PodUid::new(identity.instance_id.as_str())),
                        pvc_name: pvc_name.clone(),
                        pvc_uid: Some(PvcUid::new(pvc_uid.clone())),
                        image: Some(snapshot.desired.image.clone()),
                        pod_ready: true,
                        peer_endpoint_ready: true,
                    }),
                    agent: AgentObservation::Report(Box::new(AgentReport {
                        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                        resource_uid: snapshot.resource_uid.clone(),
                        identity: identity.clone(),
                        process_session_id: ProcessSessionId::new(format!("session-{id}")),
                        report_sequence: 1,
                        role: member.role,
                        read_status: AccessStatus::Granted,
                        write_status: if id.value() == primary {
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
            snapshot
                .secondary_scale_down_resources
                .push(SecondaryScaleDownResourceObservation {
                    resource_uid: snapshot.resource_uid.clone(),
                    target: identity.clone(),
                    identity: ReplicaCleanupIdentity {
                        pod: CleanupResourceIdentity::Present {
                            name: pod_name,
                            uid: identity.instance_id.to_string(),
                        },
                        pvc: CleanupResourceIdentity::Present {
                            name: pvc_name,
                            uid: pvc_uid,
                        },
                        endpoint: CleanupResourceIdentity::Present {
                            name: derive_replica_endpoint_name(&snapshot.resource_uid, &identity),
                            uid: format!("service-{id}"),
                        },
                    },
                    pod: ExactResourceObservation::FrozenUidPresent {
                        resource_version: "pod-rv".into(),
                    },
                    pod_allocation_operation_id: None,
                    pod_matches_allocation_metadata: false,
                    pvc: ExactResourceObservation::FrozenUidPresent {
                        resource_version: "pvc-rv".into(),
                    },
                    pvc_allocation_operation_id: None,
                    endpoint: ExactResourceObservation::FrozenUidPresent {
                        resource_version: "service-rv".into(),
                    },
                });
        }
        Self {
            snapshot,
            commands: Vec::new(),
            removed: Vec::new(),
            deletes: Vec::new(),
            inflight: BTreeMap::new(),
            applied_effects: BTreeMap::new(),
        }
    }

    pub fn key(&self, id: i64) -> ReplicaObservationKey {
        self.snapshot
            .replicas
            .keys()
            .find(|k| k.replica_id.value() == id)
            .unwrap()
            .clone()
    }

    pub fn report(&mut self, id: i64) -> &mut AgentReport {
        let key = self.key(id);
        let AgentObservation::Report(r) = &mut self.snapshot.replicas.get_mut(&key).unwrap().agent
        else {
            panic!("no report")
        };
        r
    }

    pub fn unavailable(&mut self, id: i64) {
        let key = self.key(id);
        self.snapshot.replicas.get_mut(&key).unwrap().agent = AgentObservation::Unreachable {
            message: "injected".into(),
        };
    }

    pub fn exact(&mut self, id: i64) -> &mut SecondaryScaleDownResourceObservation {
        self.snapshot
            .secondary_scale_down_resources
            .iter_mut()
            .find(|r| r.target.replica_id.value() == id)
            .unwrap()
    }

    pub fn plan(&self) -> Plan {
        let plan = evaluate(&self.snapshot, &config());
        assert_eq!(
            plan,
            evaluate(&self.snapshot, &config()),
            "evaluation is pure"
        );
        plan
    }

    pub fn controller_restart(&mut self) {
        let durable = serde_json::to_vec(&self.snapshot.status).unwrap();
        self.snapshot.status = serde_json::from_slice(&durable).unwrap();
        self.snapshot.previous_report_watermarks.clear();
    }

    pub fn interrupt(&mut self, plan: Plan, boundary: CommandBoundary) {
        let Plan::Execute { command } = &plan else {
            panic!("expected command")
        };
        let (id, operation_id) = match command {
            ProtocolCommand::EnsureConfiguration(c) => (c.local_replica_id, c.operation_id.clone()),
            ProtocolCommand::RetireReplica(c) => (c.local_replica_id, c.operation_id.clone()),
            _ => panic!("unsupported interruption {command:?}"),
        };
        let command = command.clone();
        let before = self.report(id.value()).clone();
        if matches!(boundary, CommandBoundary::Effect) {
            self.apply(plan);
            let applied = self.report(id.value()).clone();
            self.applied_effects.insert(id, applied);
            // Retirement's terminal projection is not published until the reply/report boundary.
            if matches!(command, ProtocolCommand::RetireReplica(_)) {
                *self.report(id.value()) = before;
            }
        }
        if !matches!(boundary, CommandBoundary::Intent) {
            self.report(id.value()).pending_operation_id = Some(operation_id);
        }
        self.inflight.insert(id, command);
        validate_snapshot(&self.snapshot).unwrap();
    }

    pub fn apply(&mut self, plan: Plan) {
        match plan {
            Plan::Apply { changes } => {
                for change in changes {
                    match change {
                        KubernetesChange::PersistStatus { status } => {
                            if self.snapshot.status.transition.is_some()
                                && let Some(cleanup) = status.secondary_scale_down_cleanup.as_ref()
                            {
                                self.removed
                                    .push(cleanup.evidence.preparation.intent.target.clone());
                                assert_eq!(
                                    status.topology.as_ref().unwrap().configuration,
                                    cleanup.evidence.preparation.intent.current_configuration
                                );
                                assert_eq!(
                                    status.effective_policy.as_ref(),
                                    Some(&cleanup.evidence.preparation.intent.current_policy)
                                );
                            }
                            self.snapshot.status = *status;
                        }
                        KubernetesChange::RemoveWriteRouting => {
                            self.snapshot.routing.write_target = None;
                            self.snapshot.routing.unresolved_write_target = false;
                        }
                        KubernetesChange::PublishWriteRouting { primary } => {
                            assert_eq!(
                                primary.replica_id,
                                self.snapshot
                                    .status
                                    .topology
                                    .as_ref()
                                    .unwrap()
                                    .configuration
                                    .primary_id
                            );
                            assert_eq!(
                                self.report(primary.replica_id.value()).write_status,
                                AccessStatus::Granted
                            );
                            self.snapshot.routing.write_target = Some(primary);
                        }
                        KubernetesChange::EnsureWriteRoutingService => {
                            self.snapshot.routing.service_present = true
                        }
                        KubernetesChange::EnsureReplicaScaffolding { replica_ids } => {
                            for observation in self.snapshot.replicas.values_mut() {
                                if let Some(kubernetes) = &mut observation.kubernetes
                                    && replica_ids.contains(&kubernetes.replica_id)
                                {
                                    kubernetes.peer_endpoint_ready = true;
                                }
                            }
                        }
                        KubernetesChange::EnsureReplacementScaffolding {
                            replica_id,
                            replacing,
                        } => {
                            assert_eq!(replica_id, replacing.replica_id);
                            assert_eq!(
                                self.snapshot
                                    .status
                                    .pending_replacement_cleanup
                                    .as_ref()
                                    .map(|cleanup| &cleanup.target),
                                Some(&replacing)
                            );
                            let pod_uid = PodUid::new(format!("replacement-pod-{replica_id}"));
                            let pvc_uid = PvcUid::new(format!("replacement-pvc-{replica_id}"));
                            let instance_id = ReplicaInstanceId::new(pod_uid.as_str());
                            let key = ReplicaObservationKey::new(replica_id, instance_id.clone());
                            self.snapshot.replicas.entry(key).or_insert_with(|| {
                                ReplicaObservation {
                                    kubernetes: Some(KubernetesReplicaObservation {
                                        replica_id,
                                        pod_name: instance_id.to_string(),
                                        pod_uid: Some(pod_uid.clone()),
                                        pvc_name: format!("replacement-data-{replica_id}"),
                                        pvc_uid: Some(pvc_uid.clone()),
                                        image: Some(self.snapshot.desired.image.clone()),
                                        pod_ready: true,
                                        peer_endpoint_ready: true,
                                    }),
                                    agent: AgentObservation::Uninitialized(
                                        UninitializedAgentObservation {
                                            protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                                            resource_uid: self.snapshot.resource_uid.clone(),
                                            replica_id,
                                            pod_uid,
                                            pvc_uid,
                                            process_session_id: ProcessSessionId::new(format!(
                                                "replacement-session-{replica_id}"
                                            )),
                                            report_sequence: 1,
                                        },
                                    ),
                                }
                            });
                        }
                        KubernetesChange::DeleteScaleDownResource {
                            resource,
                            name,
                            uid,
                            resource_version,
                        } => {
                            assert!(
                                self.snapshot.status.transition.is_none(),
                                "never delete before commit"
                            );
                            let target = self
                                .snapshot
                                .status
                                .secondary_scale_down_cleanup
                                .as_ref()
                                .map(|cleanup| &cleanup.evidence.preparation.intent.target)
                                .or_else(|| {
                                    self.snapshot
                                        .status
                                        .last_replacement
                                        .as_ref()
                                        .map(|cleanup| &cleanup.target)
                                })
                                .expect("committed cleanup target");
                            let id = target.replica_id.value();
                            let exact = self.exact(id);
                            let (identity, observed) = match resource {
                                ScaleDownResource::Endpoint => {
                                    (&exact.identity.endpoint, &mut exact.endpoint)
                                }
                                ScaleDownResource::Pod => (&exact.identity.pod, &mut exact.pod),
                                ScaleDownResource::Pvc => {
                                    assert!(matches!(
                                        exact.pod,
                                        ExactResourceObservation::NotFound
                                            | ExactResourceObservation::ReplacementPresent { .. }
                                    ));
                                    (&exact.identity.pvc, &mut exact.pvc)
                                }
                            };
                            assert_eq!(
                                identity,
                                &CleanupResourceIdentity::Present {
                                    name,
                                    uid: uid.clone()
                                }
                            );
                            assert_eq!(
                                observed,
                                &ExactResourceObservation::FrozenUidPresent { resource_version }
                            );
                            *observed = ExactResourceObservation::NotFound;
                            if resource == ScaleDownResource::Pod {
                                let key = self.key(id);
                                self.snapshot.replicas.remove(&key);
                            }
                            self.deletes.push((resource, uid));
                        }
                        other => panic!("unexpected scale-down effect {other:?}"),
                    }
                }
            }
            Plan::Execute { command } => {
                let id = match &command {
                    ProtocolCommand::EnsureConfiguration(c) => Some(c.local_replica_id),
                    ProtocolCommand::RetireReplica(c) => Some(c.local_replica_id),
                    _ => None,
                };
                if let Some(original) = id.and_then(|id| self.inflight.remove(&id)) {
                    assert_eq!(command, original, "replay must preserve the entire command");
                }
                let effect = id.and_then(|id| self.applied_effects.remove(&id));
                self.commands.push(command.clone());
                match command {
                    ProtocolCommand::InitializeAgentStore(c) => {
                        let key = ReplicaObservationKey::new(
                            c.local_replica_id,
                            c.expected_instance_id.clone(),
                        );
                        let observation = self.snapshot.replicas.get_mut(&key).unwrap();
                        let AgentObservation::Uninitialized(uninitialized) = &observation.agent
                        else {
                            panic!("replacement target must be uninitialized")
                        };
                        assert_eq!(uninitialized.pod_uid, c.expected_pod_uid);
                        assert_eq!(uninitialized.pvc_uid, c.expected_pvc_uid);
                        observation.agent = AgentObservation::Report(Box::new(AgentReport {
                            protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                            resource_uid: c.resource_uid,
                            identity: ReplicaIdentity {
                                replica_id: c.local_replica_id,
                                instance_id: c.expected_instance_id,
                                agent_generation: c.assigned_agent_generation,
                            },
                            process_session_id: ProcessSessionId::new(format!(
                                "replacement-initialized-{}",
                                c.local_replica_id
                            )),
                            report_sequence: 2,
                            role: ReplicaRole::None,
                            read_status: AccessStatus::NotPrimary,
                            write_status: AccessStatus::NotPrimary,
                            healthy: true,
                            ..AgentReport::default()
                        }));
                    }
                    ProtocolCommand::EnsureReplicaBuild(c) => {
                        let source_key = self.key(c.local_replica_id.value());
                        if c.retire {
                            if let AgentObservation::Report(source) =
                                &mut self.snapshot.replicas.get_mut(&source_key).unwrap().agent
                            {
                                source
                                    .builds
                                    .retain(|build| build.build_id != c.operation_id);
                            }
                            return;
                        }
                        let source_progress = match &self.snapshot.replicas[&source_key].agent {
                            AgentObservation::Report(report) => report.current_progress,
                            _ => panic!("replacement source report"),
                        };
                        let build = AgentBuildReport {
                            build_id: c.operation_id,
                            target: c.target.clone(),
                            last_sequence: 1,
                            replication_boundary_lsn: source_progress,
                            durable_lsn: source_progress,
                            completed: true,
                            catch_up_boundary_lsn: None,
                        };
                        let AgentObservation::Report(source) =
                            &mut self.snapshot.replicas.get_mut(&source_key).unwrap().agent
                        else {
                            panic!("replacement source report")
                        };
                        source.builds = vec![build.clone()];
                        source.report_sequence += 1;
                        let target_key = ReplicaObservationKey::new(
                            c.target.replica_id,
                            c.target.instance_id.clone(),
                        );
                        let AgentObservation::Report(target) =
                            &mut self.snapshot.replicas.get_mut(&target_key).unwrap().agent
                        else {
                            panic!("replacement target report")
                        };
                        target.role = ReplicaRole::IdleSecondary;
                        target.current_progress = source_progress;
                        target.committed_lsn = source_progress;
                        target.builds = vec![build];
                        target.report_sequence += 1;
                    }
                    ProtocolCommand::PrepareSecondaryRemoval(c) => {
                        assert!(self.snapshot.routing.write_target.is_none());
                        assert_eq!(
                            self.snapshot
                                .status
                                .transition
                                .as_ref()
                                .unwrap()
                                .secondary_scale_down
                                .as_ref(),
                            Some(&c.intent)
                        );
                        let r = self.report(c.local_replica_id.value());
                        assert_eq!(r.identity, c.intent.primary);
                        r.report_sequence += 1;
                        r.read_status = AccessStatus::ReconfigurationPending;
                        r.write_status = AccessStatus::ReconfigurationPending;
                        r.prepared_secondary_removal = Some(SecondaryRemovalPreparation {
                            intent: c.intent,
                            operation_id: c.operation_id.clone(),
                            process_session_id: r.process_session_id.clone(),
                            report_sequence: r.report_sequence,
                            boundary_lsn: r.verified_replication_lsn.unwrap(),
                        });
                        r.retained_operation_id = Some(c.operation_id);
                    }
                    ProtocolCommand::EnsureConfiguration(c) => {
                        if c.transition_kind == TransitionKind::SecondaryScaleDown {
                            validate_secondary_removal_configuration(&c).unwrap();
                            let intent = &c
                                .secondary_removal_evidence
                                .as_ref()
                                .unwrap()
                                .preparation
                                .intent;
                            assert_ne!(c.local_replica_id, intent.target.replica_id);
                            assert_eq!(
                                c.primary_write_status,
                                AccessStatus::ReconfigurationPending
                            );
                        } else if c.transition_kind != TransitionKind::Replacement {
                            assert!(
                                self.snapshot.status.transition.is_none(),
                                "grant only accepted authority"
                            );
                            assert_eq!(
                                self.snapshot
                                    .status
                                    .topology
                                    .as_ref()
                                    .unwrap()
                                    .configuration,
                                c.current_configuration
                            );
                        }
                        let key = ReplicaObservationKey::new(
                            c.local_replica_id,
                            c.expected_instance_id.clone(),
                        );
                        let AgentObservation::Report(r) =
                            &mut self.snapshot.replicas.get_mut(&key).unwrap().agent
                        else {
                            panic!("configuration target report")
                        };
                        r.report_sequence += 1;
                        r.epoch = c.current_epoch;
                        r.previous_configuration = c.previous_configuration;
                        r.read_status = AccessStatus::Granted;
                        r.role = c
                            .current_configuration
                            .members
                            .iter()
                            .find(|m| m.identity == r.identity)
                            .unwrap()
                            .role;
                        if r.current_configuration.as_ref() != Some(&c.current_configuration)
                            && c.secondary_removal_evidence.is_none()
                        {
                            r.secondary_removal_evidence = None;
                            r.accepted_secondary_removal = None;
                            r.prepared_secondary_removal = None;
                        }
                        r.current_configuration = Some(c.current_configuration);
                        r.pending_operation_id = None;
                        r.pending_configuration = None;
                        r.retained_operation_id = Some(c.operation_id);
                        r.catch_up_complete = true;
                        r.builds
                            .retain(|build| !c.retire_build_ids.contains(&build.build_id));
                        if c.transition_kind == TransitionKind::Replacement {
                            r.scale_up_intent = None;
                        }
                        r.write_status = if r.role == ReplicaRole::Primary {
                            c.primary_write_status
                        } else {
                            AccessStatus::NotPrimary
                        };
                        if let Some(evidence) = c.secondary_removal_evidence {
                            if r.secondary_removal_evidence.as_ref() != Some(&evidence) {
                                r.accepted_secondary_removal = None;
                            }
                            r.secondary_removal_evidence = Some(evidence);
                        }
                    }
                    ProtocolCommand::AcceptSecondaryRemovalCommit(c) => {
                        kuberic_protocol::validation::validate_accept_secondary_removal_commit(&c)
                            .unwrap();
                        let completed = self
                            .snapshot
                            .status
                            .last_secondary_removal
                            .as_ref()
                            .map(|receipt| receipt.committed());
                        let cleanup = self
                            .snapshot
                            .status
                            .secondary_scale_down_cleanup
                            .as_ref()
                            .or(completed.as_ref())
                            .unwrap();
                        assert_eq!(cleanup.evidence, c.committed.evidence);
                        assert_eq!(
                            cleanup.current_only_write_quorum,
                            c.committed.current_only_write_quorum
                        );
                        let r = self.report(c.target.replica_id.value());
                        assert_eq!(r.previous_configuration, None);
                        assert_eq!(
                            r.current_configuration.as_ref(),
                            Some(
                                &c.committed
                                    .evidence
                                    .preparation
                                    .intent
                                    .current_configuration
                            )
                        );
                        r.report_sequence += 1;
                        r.prepared_secondary_removal = None;
                        r.accepted_secondary_removal = Some(c.committed);
                        r.pending_operation_id = None;
                        r.pending_configuration = None;
                    }
                    ProtocolCommand::RetireReplica(c) => {
                        assert!(self.snapshot.status.transition.is_none());
                        let r = self.report(c.local_replica_id.value());
                        let mut retirement =
                            fixture::retirement(&c.committed.evidence.preparation.intent);
                        r.report_sequence += 1;
                        retirement.process_session_id = r.process_session_id.clone();
                        retirement.report_sequence = r.report_sequence;
                        r.epoch = retirement.epoch;
                        r.role = ReplicaRole::None;
                        r.read_status = AccessStatus::NotPrimary;
                        r.write_status = AccessStatus::NotPrimary;
                        r.current_configuration = None;
                        r.verified_replication_lsn = None;
                        r.previous_configuration = None;
                        r.pending_operation_id = None;
                        r.pending_configuration = None;
                        r.retained_operation_id = Some(c.operation_id);
                        r.prepared_secondary_removal = None;
                        r.secondary_removal_evidence = None;
                        r.retired_replica = Some(retirement);
                        r.accepted_secondary_removal = None;
                    }
                    other => panic!("unexpected command {other:?}"),
                }
                if let Some(effect) = effect {
                    let r = self.report(id.unwrap().value());
                    assert_eq!(r.epoch, effect.epoch);
                    assert_eq!(r.role, effect.role);
                    assert_eq!(r.previous_configuration, effect.previous_configuration);
                    assert_eq!(r.current_configuration, effect.current_configuration);
                    assert_eq!(r.write_status, effect.write_status);
                    assert_eq!(
                        r.retired_replica.as_ref().map(|retired| &retired.intent),
                        effect
                            .retired_replica
                            .as_ref()
                            .map(|retired| &retired.intent),
                        "replay publishes the same already-applied retirement"
                    );
                }
            }
            Plan::Wait { status, .. } | Plan::Stable { status, .. } => {
                self.snapshot.status = status
            }
            Plan::Unsafe { reason, .. } => panic!("unsafe model: {reason:?}"),
        }
        validate_snapshot(&self.snapshot).unwrap();
    }

    pub fn step(&mut self) -> Plan {
        let plan = self.plan();
        self.apply(plan.clone());
        plan
    }

    pub fn finish(&mut self) {
        for _ in 0..250 {
            if matches!(self.step(), Plan::Stable { .. }) {
                assert!(self.snapshot.status.secondary_scale_down_cleanup.is_none());
                return;
            }
        }
        panic!("model did not converge: {:?}", self.plan());
    }

    pub fn until(&mut self, predicate: impl Fn(&Self) -> bool) {
        for _ in 0..100 {
            if predicate(self) {
                return;
            }
            self.step();
        }
        panic!("boundary not reached: {:?}", self.plan());
    }
}

pub fn reason(plan: &Plan) -> &str {
    let status = match plan {
        Plan::Wait { status, .. } | Plan::Unsafe { status, .. } | Plan::Stable { status, .. } => {
            status
        }
        Plan::Apply { changes } => changes
            .iter()
            .find_map(|c| match c {
                KubernetesChange::PersistStatus { status } => Some(status.as_ref()),
                _ => None,
            })
            .expect("status"),
        _ => panic!("no status"),
    };
    &status
        .conditions
        .iter()
        .find(|c| c.type_ == "Progressing" || c.type_ == "Unsafe")
        .unwrap()
        .reason
}
