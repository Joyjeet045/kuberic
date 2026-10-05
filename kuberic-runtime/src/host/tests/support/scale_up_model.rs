use std::collections::{BTreeMap, BTreeSet};

use crate::protocol::command::{KubernetesChange, ProtocolCommand, ScaleDownResource};
use crate::protocol::observation::*;
use crate::protocol::types::*;
use crate::protocol::validation::validate_status;
use crate::test_controller::Plan;
use crate::test_controller::{EvaluationConfig, evaluate};

type DurableAcknowledgementEntries = Vec<(i64, Vec<(ReplicaIdentity, ProcessSessionId)>)>;
type RestartImage = (
    BTreeMap<i64, String>,
    BTreeMap<i64, BTreeMap<i64, String>>,
    BTreeMap<i64, String>,
    BTreeMap<String, BTreeMap<i64, String>>,
    DurableAcknowledgementEntries,
);

pub fn config() -> EvaluationConfig {
    EvaluationConfig {
        enable_secondary_scale_down: true,
        allow_scale_up: true,
        ..Default::default()
    }
}

pub struct Model {
    pub snapshot: ObservationSnapshot,
    pub accepted_history: Vec<u32>,
    pending_build: Option<PendingBuild>,
    pub durable_source: BTreeMap<i64, String>,
    pub durable_receivers: BTreeMap<i64, BTreeMap<i64, String>>,
    pub acknowledged_writes: BTreeMap<i64, String>,
    durable_incarnations: BTreeMap<String, BTreeMap<i64, String>>,
    durable_acknowledgements: BTreeMap<i64, BTreeMap<ReplicaIdentity, ProcessSessionId>>,
    accepted_epochs: Vec<Epoch>,
    accepted_policy_high_water: u32,
    frozen_boundaries: BTreeMap<OperationId, i64>,
    committed_incarnations: BTreeSet<String>,
    next_incarnation: u64,
}

struct PendingBuild {
    build: AgentBuildReport,
    source: ReplicaObservationKey,
    target: ReplicaObservationKey,
    catch_up_boundary: i64,
    phase: u8,
}

impl Model {
    pub fn new(accepted: u32, desired: u32) -> Self {
        let resource_uid = ResourceUid::new("scale-up-model");
        let policy = EffectivePolicy::fixed(accepted, 10).unwrap();
        let members = (1..=accepted)
            .map(|id| {
                let replica_id = ReplicaId::new(i64::from(id));
                let pod_uid = PodUid::new(format!("accepted-pod-{id}"));
                let pvc_uid = PvcUid::new(format!("accepted-pvc-{id}"));
                ConfigurationMember {
                    identity: ReplicaIdentity {
                        replica_id,
                        instance_id: ReplicaInstanceId::new(pod_uid.as_str()),
                        agent_generation: derive_agent_generation(&derive_initialization_id(
                            &resource_uid,
                            replica_id,
                            &pod_uid,
                            &pvc_uid,
                        )),
                    },
                    role: if id == 1 {
                        ReplicaRole::Primary
                    } else {
                        ReplicaRole::ActiveSecondary
                    },
                }
            })
            .collect::<Vec<_>>();
        let configuration = ConfigurationDescriptor::new(
            Epoch::new(0, 1),
            ReplicaId::new(1),
            members.clone(),
            policy.write_quorum,
        );
        let accepted_epoch = configuration.epoch;
        let replicas = members
            .iter()
            .map(|member| {
                (
                    ReplicaObservationKey::new(
                        member.identity.replica_id,
                        member.identity.instance_id.clone(),
                    ),
                    ReplicaObservation {
                        kubernetes: Some(KubernetesReplicaObservation {
                            replica_id: member.identity.replica_id,
                            pod_name: member.identity.instance_id.to_string(),
                            pod_uid: Some(PodUid::new(member.identity.instance_id.as_str())),
                            pvc_name: format!("data-{}", member.identity.replica_id),
                            pvc_uid: Some(PvcUid::new(format!(
                                "accepted-pvc-{}",
                                member.identity.replica_id
                            ))),
                            image: Some("example:v1".into()),
                            pod_ready: true,
                            peer_endpoint_ready: true,
                        }),
                        agent: AgentObservation::Report(Box::new(AgentReport {
                            protocol_version: crate::protocol::PROTOCOL_VERSION,
                            resource_uid: resource_uid.clone(),
                            identity: member.identity.clone(),
                            process_session_id: ProcessSessionId::new(format!(
                                "accepted-session-{}",
                                member.identity.replica_id
                            )),
                            report_sequence: 1,
                            role: member.role,
                            read_status: AccessStatus::Granted,
                            write_status: if member.role == ReplicaRole::Primary {
                                AccessStatus::Granted
                            } else {
                                AccessStatus::NotPrimary
                            },
                            healthy: true,
                            epoch: configuration.epoch,
                            previous_configuration: None,
                            current_configuration: Some(configuration.clone()),
                            current_progress: 10,
                            verified_replication_lsn: Some(10),
                            committed_lsn: 10,
                            catch_up_capability: Some(1),
                            current_configuration_quorum_progress: 10,
                            catch_up_boundary: None,
                            catch_up_complete: true,
                            ..Default::default()
                        })),
                    },
                )
            })
            .collect();
        let durable_source = (1..=10)
            .map(|lsn| (lsn, format!("value-{lsn}")))
            .collect::<BTreeMap<_, _>>();
        let initial_acknowledgements = (1..=10)
            .map(|lsn| {
                (
                    lsn,
                    members
                        .iter()
                        .map(|member| {
                            (
                                member.identity.clone(),
                                ProcessSessionId::new(format!(
                                    "accepted-session-{}",
                                    member.identity.replica_id
                                )),
                            )
                        })
                        .collect(),
                )
            })
            .collect();
        Self {
            snapshot: ObservationSnapshot {
                resource_uid,
                resource_version: "1".into(),
                desired: DesiredState {
                    generation: 2,
                    replicas: desired,
                    image: "example:v1".into(),
                    failover_delay_seconds: 10,
                    switchover: None,
                },
                status: AcceptedStatus {
                    initialized: true,
                    observed_generation: 1,
                    effective_policy: Some(policy),
                    topology: Some(AcceptedTopology { configuration }),
                    ..Default::default()
                },
                replicas,
                secondary_scale_down_resources: Vec::new(),
                previous_report_watermarks: BTreeMap::new(),
                durable_storage_evidence: true,
                supporting_resources_ready: true,
                routing: RoutingObservation {
                    service_present: true,
                    unresolved_write_target: false,
                    write_target: Some(members[0].identity.clone()),
                },
                observation_failures: Vec::new(),
                now_unix_seconds: 100,
            },
            accepted_history: vec![accepted],
            pending_build: None,
            durable_source: durable_source.clone(),
            durable_receivers: BTreeMap::new(),
            acknowledged_writes: durable_source.clone(),
            durable_incarnations: members
                .iter()
                .map(|member| {
                    (
                        member.identity.instance_id.to_string(),
                        durable_source.clone(),
                    )
                })
                .collect(),
            durable_acknowledgements: initial_acknowledgements,
            accepted_epochs: vec![accepted_epoch],
            accepted_policy_high_water: accepted,
            frozen_boundaries: BTreeMap::new(),
            committed_incarnations: members
                .iter()
                .map(|member| member.identity.instance_id.to_string())
                .collect(),
            next_incarnation: 1,
        }
    }

    pub fn plan(&self) -> Plan {
        let plan = evaluate(&self.snapshot, &config());
        assert_eq!(plan, evaluate(&self.snapshot, &config()));
        plan
    }

    pub fn from_snapshot(snapshot: ObservationSnapshot, accepted_history: Vec<u32>) -> Self {
        let configuration = &snapshot.status.topology.as_ref().unwrap().configuration;
        let accepted_policy_high_water = configuration.members.len() as u32;
        let durable_source = (1..=10)
            .map(|lsn| (lsn, format!("value-{lsn}")))
            .collect::<BTreeMap<_, _>>();
        let initial_sessions = snapshot
            .replicas
            .values()
            .filter_map(|observation| match &observation.agent {
                AgentObservation::Report(report) => {
                    Some((report.identity.clone(), report.process_session_id.clone()))
                }
                _ => None,
            })
            .collect::<BTreeMap<_, _>>();
        let initial_acknowledgements = (1..=10)
            .map(|lsn| (lsn, initial_sessions.clone()))
            .collect();
        Self {
            accepted_epochs: vec![configuration.epoch],
            committed_incarnations: configuration
                .members
                .iter()
                .map(|member| member.identity.instance_id.to_string())
                .collect(),
            durable_incarnations: configuration
                .members
                .iter()
                .map(|member| {
                    (
                        member.identity.instance_id.to_string(),
                        durable_source.clone(),
                    )
                })
                .collect(),
            durable_acknowledgements: initial_acknowledgements,
            snapshot,
            accepted_history,
            pending_build: None,
            durable_source: durable_source.clone(),
            durable_receivers: BTreeMap::new(),
            acknowledged_writes: durable_source,
            frozen_boundaries: BTreeMap::new(),
            accepted_policy_high_water,
            next_incarnation: 1,
        }
    }

    pub fn fork(&self) -> Self {
        Self {
            snapshot: self.snapshot.clone(),
            accepted_history: self.accepted_history.clone(),
            pending_build: self.pending_build.as_ref().map(|pending| PendingBuild {
                build: pending.build.clone(),
                source: pending.source.clone(),
                target: pending.target.clone(),
                catch_up_boundary: pending.catch_up_boundary,
                phase: pending.phase,
            }),
            durable_source: self.durable_source.clone(),
            durable_receivers: self.durable_receivers.clone(),
            acknowledged_writes: self.acknowledged_writes.clone(),
            durable_incarnations: self.durable_incarnations.clone(),
            durable_acknowledgements: self.durable_acknowledgements.clone(),
            accepted_epochs: self.accepted_epochs.clone(),
            accepted_policy_high_water: self.accepted_policy_high_water,
            frozen_boundaries: self.frozen_boundaries.clone(),
            committed_incarnations: self.committed_incarnations.clone(),
            next_incarnation: self.next_incarnation,
        }
    }

    pub fn accepted_count(&self) -> u32 {
        self.snapshot
            .status
            .effective_policy
            .as_ref()
            .unwrap()
            .replica_set_size
    }

    pub fn step(&mut self) -> bool {
        self.assert_safety_invariants();
        match self.plan() {
            Plan::Stable { status, .. } => {
                self.snapshot.status = status;
                self.assert_safety_invariants();
                true
            }
            Plan::Wait { status, .. } => {
                self.snapshot.status = status;
                self.advance_async_build();
                self.assert_safety_invariants();
                false
            }
            Plan::Unsafe { reason, .. } => panic!(
                "unexpected unsafe plan: {reason:?}; transition={:?}; reports={:?}",
                self.snapshot.status.transition,
                self.snapshot
                    .replicas
                    .values()
                    .filter_map(|observation| match &observation.agent {
                        AgentObservation::Report(report) => Some((
                            report.identity.replica_id,
                            report.role,
                            report.epoch,
                            report
                                .current_configuration
                                .as_ref()
                                .map(|configuration| configuration.epoch),
                            report.current_progress,
                            report.verified_replication_lsn,
                            report.scale_up_intent.clone(),
                        )),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            ),
            Plan::Apply { changes } => {
                for change in changes {
                    self.apply(change);
                }
                self.assert_safety_invariants();
                false
            }
            Plan::Execute { command } => {
                self.execute(command);
                self.assert_safety_invariants();
                false
            }
        }
    }

    pub fn run(&mut self, limit: usize) {
        for _ in 0..limit {
            if self.step() && self.accepted_count() == self.snapshot.desired.replicas {
                return;
            }
        }
        panic!("scale-up model did not converge: {:?}", self.plan());
    }

    pub fn apply(&mut self, change: KubernetesChange) {
        match change {
            KubernetesChange::PersistStatus { status } => {
                let old = self.accepted_count();
                let old_epoch = self
                    .snapshot
                    .status
                    .topology
                    .as_ref()
                    .unwrap()
                    .configuration
                    .epoch;
                let new_epoch = status
                    .topology
                    .as_ref()
                    .expect("persisted accepted topology")
                    .configuration
                    .epoch;
                let new_policy = status
                    .effective_policy
                    .as_ref()
                    .expect("persisted accepted policy")
                    .replica_set_size;
                self.validate_accepted_epoch(new_epoch)
                    .unwrap_or_else(|error| panic!("{error}"));
                self.validate_accepted_policy(new_policy)
                    .unwrap_or_else(|error| panic!("{error}"));
                self.snapshot.status = *status;
                self.refresh_allocation_observation();
                self.bind_candidate_provenance();
                let new = self.accepted_count();
                if new_epoch != old_epoch {
                    self.accepted_epochs.push(new_epoch);
                }
                self.accepted_policy_high_water = new_policy;
                if new != old {
                    assert_eq!(new, old + 1);
                    self.accepted_history.push(new);
                    let accepted = &self
                        .snapshot
                        .status
                        .topology
                        .as_ref()
                        .unwrap()
                        .configuration;
                    assert!(accepted.epoch > old_epoch);
                    self.committed_incarnations.extend(
                        accepted
                            .members
                            .iter()
                            .map(|member| member.identity.instance_id.to_string()),
                    );
                }
            }
            KubernetesChange::EnsureReplicaScaffolding { replica_ids } => {
                assert_eq!(replica_ids.len(), 1);
                self.add_candidate(replica_ids[0]);
                self.refresh_allocation_observation();
            }
            KubernetesChange::EnsureWriteRoutingService => {
                self.snapshot.routing.service_present = true;
            }
            KubernetesChange::PublishWriteRouting { primary } => {
                self.snapshot.routing.write_target = Some(primary);
            }
            KubernetesChange::RemoveWriteRouting => {
                self.snapshot.routing.write_target = None;
            }
            KubernetesChange::DeleteScaleDownResource {
                resource,
                name,
                uid,
                resource_version,
            } => {
                let deletion = KubernetesChange::DeleteScaleDownResource {
                    resource,
                    name,
                    uid,
                    resource_version,
                };
                self.validate_deletion_change(&deletion)
                    .unwrap_or_else(|error| panic!("invalid modeled deletion: {error}"));
                let target = self
                    .snapshot
                    .status
                    .scale_up_cleanup
                    .as_ref()
                    .map(|cleanup| cleanup.target.clone())
                    .or_else(|| {
                        self.snapshot
                            .status
                            .scale_up_allocation
                            .as_ref()
                            .map(ScaleUpAllocation::observation_target)
                    })
                    .unwrap();
                let exact = self
                    .snapshot
                    .secondary_scale_down_resources
                    .iter_mut()
                    .find(|observation| observation.target == target)
                    .unwrap();
                match resource {
                    ScaleDownResource::Endpoint => {
                        exact.endpoint = ExactResourceObservation::NotFound
                    }
                    ScaleDownResource::Pod => exact.pod = ExactResourceObservation::NotFound,
                    ScaleDownResource::Pvc => exact.pvc = ExactResourceObservation::NotFound,
                }
                let target_absent = |observed: &ExactResourceObservation| {
                    matches!(
                        observed,
                        ExactResourceObservation::NotFound
                            | ExactResourceObservation::ReplacementPresent { .. }
                    )
                };
                let fully_absent = target_absent(&exact.endpoint)
                    && target_absent(&exact.pod)
                    && target_absent(&exact.pvc);
                if fully_absent {
                    self.snapshot.replicas.remove(&ReplicaObservationKey::new(
                        target.replica_id,
                        target.instance_id.clone(),
                    ));
                    self.durable_incarnations
                        .remove(target.instance_id.as_str());
                }
            }

            other => panic!("unexpected scale-up model change: {other:?}"),
        }
    }

    pub fn validate_deletion_change(&self, change: &KubernetesChange) -> Result<(), String> {
        let KubernetesChange::DeleteScaleDownResource {
            resource,
            name,
            uid,
            resource_version,
        } = change
        else {
            return Err("not a deletion command".into());
        };
        let (target, provenance) = if let Some(cleanup) = &self.snapshot.status.scale_up_cleanup {
            (
                cleanup.target.clone(),
                cleanup.provisioning.operation_id.clone(),
            )
        } else if let Some(allocation) = &self.snapshot.status.scale_up_allocation {
            (
                allocation.observation_target(),
                allocation.operation_id.clone(),
            )
        } else {
            return Err("deletion has no active cleanup/allocation provenance".into());
        };
        let exact = self
            .snapshot
            .secondary_scale_down_resources
            .iter()
            .find(|observation| {
                observation.resource_uid == self.snapshot.resource_uid
                    && observation.target == target
            })
            .ok_or_else(|| "deletion has no exact target observation".to_string())?;
        let (identity, observed, allocation_operation) = match resource {
            ScaleDownResource::Endpoint => (&exact.identity.endpoint, &exact.endpoint, None),
            ScaleDownResource::Pod => (
                &exact.identity.pod,
                &exact.pod,
                exact.pod_allocation_operation_id.as_ref(),
            ),
            ScaleDownResource::Pvc => (
                &exact.identity.pvc,
                &exact.pvc,
                exact.pvc_allocation_operation_id.as_ref(),
            ),
        };
        let CleanupResourceIdentity::Present {
            name: expected_name,
            uid: expected_uid,
        } = identity
        else {
            return Err("deletion identity is not frozen present".into());
        };
        let ExactResourceObservation::FrozenUidPresent {
            resource_version: expected_resource_version,
        } = observed
        else {
            return Err("deletion target is not the frozen incarnation".into());
        };
        if name != expected_name
            || uid != expected_uid
            || resource_version != expected_resource_version
        {
            return Err(format!(
                "deletion identity/version mismatch for {target:?}: expected \
                 {expected_name}/{expected_uid}@{expected_resource_version}, got \
                 {name}/{uid}@{resource_version}"
            ));
        }
        if matches!(resource, ScaleDownResource::Pod | ScaleDownResource::Pvc)
            && allocation_operation != Some(&provenance)
        {
            return Err(format!(
                "deletion allocation provenance mismatch: expected {provenance}, got \
                 {allocation_operation:?}"
            ));
        }
        Ok(())
    }

    pub fn validate_accepted_epoch(&self, epoch: Epoch) -> Result<(), String> {
        let high_water = self
            .accepted_epochs
            .last()
            .copied()
            .expect("accepted epoch high-water");
        (epoch >= high_water).then_some(()).ok_or_else(|| {
            format!("accepted-state persistence rolled epoch back from {high_water:?} to {epoch:?}")
        })
    }

    pub fn validate_accepted_policy(&self, replica_set_size: u32) -> Result<(), String> {
        (replica_set_size >= self.accepted_policy_high_water)
            .then_some(())
            .ok_or_else(|| {
                format!(
                    "accepted-state persistence rolled policy back from {} to {replica_set_size}",
                    self.accepted_policy_high_water
                )
            })
    }

    pub fn validate_exact_candidate_set(&self) -> Result<(), String> {
        let mut active_targets = BTreeSet::new();
        if let Some(allocation) = &self.snapshot.status.scale_up_allocation {
            active_targets.insert(allocation.observation_target());
        }
        if let Some(provisioning) = &self.snapshot.status.provisioning
            && provisioning.scale_up().is_some()
        {
            active_targets.insert(provisioning.target_identity(&self.snapshot.resource_uid));
        }
        if let Some(transition) = &self.snapshot.status.transition
            && let Some(intent) = transition.scale_up.as_deref().or_else(|| {
                transition
                    .scale_up_failover
                    .as_deref()
                    .map(|evidence| &evidence.intent)
            })
        {
            active_targets.insert(intent.target.clone());
        }
        if let Some(cleanup) = &self.snapshot.status.scale_up_cleanup {
            active_targets.insert(cleanup.target.clone());
        }
        (active_targets.len() <= 1).then_some(()).ok_or_else(|| {
            format!("more than one exact scale-up candidate identity is active: {active_targets:?}")
        })
    }

    fn refresh_allocation_observation(&mut self) {
        self.snapshot
            .secondary_scale_down_resources
            .retain(|exact| {
                !exact
                    .target
                    .instance_id
                    .as_str()
                    .starts_with("scale-up-allocation-")
            });
        let Some(allocation) = self.snapshot.status.scale_up_allocation.as_ref() else {
            return;
        };
        let target = allocation.observation_target();
        let replica_id = allocation.target_replica_id;
        let kubernetes = self
            .snapshot
            .replicas
            .iter()
            .find(|(key, observation)| {
                key.replica_id == replica_id && observation.kubernetes.is_some()
            })
            .and_then(|(_, observation)| observation.kubernetes.as_ref());
        let pod_name = format!("candidate-pod-{replica_id}");
        let pvc_name = format!("candidate-data-{replica_id}");
        let (pod_identity, pod) = match (&allocation.pod_uid, kubernetes) {
            (Some(uid), Some(observed)) if observed.pod_uid.as_ref() == Some(uid) => (
                CleanupResourceIdentity::Present {
                    name: pod_name,
                    uid: uid.to_string(),
                },
                ExactResourceObservation::FrozenUidPresent {
                    resource_version: "1".into(),
                },
            ),
            (None, Some(observed)) if observed.pod_uid.is_some() => (
                CleanupResourceIdentity::Absent { name: pod_name },
                ExactResourceObservation::ReplacementPresent {
                    uid: observed.pod_uid.as_ref().unwrap().to_string(),
                    resource_version: "1".into(),
                },
            ),
            _ => (
                CleanupResourceIdentity::Absent { name: pod_name },
                ExactResourceObservation::NotFound,
            ),
        };
        let (pvc_identity, pvc) = match (&allocation.pvc_uid, kubernetes) {
            (Some(uid), Some(observed)) if observed.pvc_uid.as_ref() == Some(uid) => (
                CleanupResourceIdentity::Present {
                    name: pvc_name,
                    uid: uid.to_string(),
                },
                ExactResourceObservation::FrozenUidPresent {
                    resource_version: "1".into(),
                },
            ),
            (None, Some(observed)) if observed.pvc_uid.is_some() => (
                CleanupResourceIdentity::Absent { name: pvc_name },
                ExactResourceObservation::ReplacementPresent {
                    uid: observed.pvc_uid.as_ref().unwrap().to_string(),
                    resource_version: "1".into(),
                },
            ),
            _ => (
                CleanupResourceIdentity::Absent { name: pvc_name },
                ExactResourceObservation::NotFound,
            ),
        };
        self.snapshot
            .secondary_scale_down_resources
            .push(SecondaryScaleDownResourceObservation {
                resource_uid: self.snapshot.resource_uid.clone(),
                target,
                identity: ReplicaCleanupIdentity {
                    pod: pod_identity,
                    pvc: pvc_identity,
                    endpoint: CleanupResourceIdentity::Absent {
                        name: format!("candidate-allocation-{replica_id}"),
                    },
                },
                pod,
                pod_allocation_operation_id: Some(allocation.operation_id.clone()),
                pod_matches_allocation_metadata: true,
                pvc,
                pvc_allocation_operation_id: Some(allocation.operation_id.clone()),
                endpoint: ExactResourceObservation::NotFound,
            });
    }

    fn bind_candidate_provenance(&mut self) {
        let Some(provisioning) = self.snapshot.status.provisioning.as_ref() else {
            return;
        };
        let target = provisioning.target_identity(&self.snapshot.resource_uid);
        if let Some(exact) = self
            .snapshot
            .secondary_scale_down_resources
            .iter_mut()
            .find(|exact| exact.target == target)
        {
            exact.pod_allocation_operation_id = Some(provisioning.operation_id.clone());
            exact.pvc_allocation_operation_id = Some(provisioning.operation_id.clone());
            exact.pod_matches_allocation_metadata = true;
        }
    }

    fn add_candidate(&mut self, replica_id: ReplicaId) {
        if self.snapshot.replicas.iter().any(|(key, observation)| {
            key.replica_id == replica_id
                && observation
                    .kubernetes
                    .as_ref()
                    .is_some_and(KubernetesReplicaObservation::has_exact_scaffolding)
        }) {
            return;
        }
        let incarnation = self.next_incarnation;
        self.next_incarnation += 1;
        let pod_uid = PodUid::new(if incarnation == 1 {
            format!("candidate-pod-{replica_id}")
        } else {
            format!("candidate-pod-{replica_id}-{incarnation}")
        });
        let pvc_uid = PvcUid::new(if incarnation == 1 {
            format!("candidate-pvc-{replica_id}")
        } else {
            format!("candidate-pvc-{replica_id}-{incarnation}")
        });
        let instance_id = ReplicaInstanceId::new(pod_uid.as_str());
        self.snapshot.replicas.insert(
            ReplicaObservationKey::new(replica_id, instance_id.clone()),
            ReplicaObservation {
                kubernetes: Some(KubernetesReplicaObservation {
                    replica_id,
                    pod_name: instance_id.to_string(),
                    pod_uid: Some(pod_uid.clone()),
                    pvc_name: format!("candidate-data-{replica_id}"),
                    pvc_uid: Some(pvc_uid.clone()),
                    image: Some("example:v1".into()),
                    pod_ready: true,
                    peer_endpoint_ready: false,
                }),
                agent: AgentObservation::Uninitialized(UninitializedAgentObservation {
                    protocol_version: crate::protocol::PROTOCOL_VERSION,
                    resource_uid: self.snapshot.resource_uid.clone(),
                    replica_id,
                    pod_uid: pod_uid.clone(),
                    pvc_uid: pvc_uid.clone(),
                    process_session_id: ProcessSessionId::new(format!(
                        "candidate-session-{replica_id}"
                    )),
                    report_sequence: 1,
                }),
            },
        );
        let target = ReplicaIdentity {
            replica_id,
            instance_id,
            agent_generation: derive_agent_generation(&derive_initialization_id(
                &self.snapshot.resource_uid,
                replica_id,
                &pod_uid,
                &pvc_uid,
            )),
        };
        self.snapshot
            .secondary_scale_down_resources
            .push(SecondaryScaleDownResourceObservation {
                resource_uid: self.snapshot.resource_uid.clone(),
                target: target.clone(),
                identity: ReplicaCleanupIdentity {
                    pod: CleanupResourceIdentity::Present {
                        name: target.instance_id.to_string(),
                        uid: pod_uid.to_string(),
                    },
                    pvc: CleanupResourceIdentity::Present {
                        name: format!("candidate-data-{replica_id}"),
                        uid: pvc_uid.to_string(),
                    },
                    endpoint: CleanupResourceIdentity::Present {
                        name: derive_replica_endpoint_name(&self.snapshot.resource_uid, &target),
                        uid: format!("candidate-endpoint-{replica_id}"),
                    },
                },
                pod: ExactResourceObservation::FrozenUidPresent {
                    resource_version: "1".into(),
                },
                pod_allocation_operation_id: None,
                pod_matches_allocation_metadata: false,
                pvc: ExactResourceObservation::FrozenUidPresent {
                    resource_version: "1".into(),
                },
                pvc_allocation_operation_id: None,
                endpoint: ExactResourceObservation::FrozenUidPresent {
                    resource_version: "1".into(),
                },
            });
    }

    pub fn execute(&mut self, command: ProtocolCommand) {
        match command {
            ProtocolCommand::InitializeAgentStore(command) => {
                let target = ReplicaIdentity {
                    replica_id: command.local_replica_id,
                    instance_id: command.expected_instance_id.clone(),
                    agent_generation: command.assigned_agent_generation.clone(),
                };
                let observation = self
                    .snapshot
                    .replicas
                    .get_mut(&ReplicaObservationKey::new(
                        target.replica_id,
                        target.instance_id.clone(),
                    ))
                    .unwrap();
                observation.kubernetes.as_mut().unwrap().peer_endpoint_ready = true;
                observation.agent = AgentObservation::Report(Box::new(AgentReport {
                    protocol_version: crate::protocol::PROTOCOL_VERSION,
                    resource_uid: self.snapshot.resource_uid.clone(),
                    identity: target.clone(),
                    process_session_id: ProcessSessionId::new(format!(
                        "candidate-initialized-{}",
                        target.instance_id
                    )),
                    report_sequence: 2,
                    role: ReplicaRole::None,
                    read_status: AccessStatus::NotPrimary,
                    write_status: AccessStatus::NotPrimary,
                    healthy: true,
                    epoch: Epoch::default(),
                    current_progress: 0,
                    committed_lsn: 0,
                    ..Default::default()
                }));
                if let Some(provisioning) = self.snapshot.status.provisioning.as_ref() {
                    let exact = self
                        .snapshot
                        .secondary_scale_down_resources
                        .iter_mut()
                        .find(|exact| exact.target == target)
                        .expect("initialized target exact resource observation");
                    exact.pod_allocation_operation_id = Some(provisioning.operation_id.clone());
                    exact.pvc_allocation_operation_id = Some(provisioning.operation_id.clone());
                    exact.pod_matches_allocation_metadata = true;
                }
            }
            ProtocolCommand::EnsureReplicaBuild(command) => {
                if command.retire {
                    for observation in self.snapshot.replicas.values_mut() {
                        if let AgentObservation::Report(report) = &mut observation.agent {
                            report
                                .builds
                                .retain(|build| build.build_id != command.operation_id);
                            let build_operation =
                                OperationId::new(format!("{}:build-replica", command.operation_id));
                            if report.pending_operation_id.as_ref() == Some(&build_operation) {
                                report.pending_operation_id = None;
                            }
                            if report.identity.replica_id == command.local_replica_id {
                                report.retained_operation_id = Some(OperationId::new(format!(
                                    "{}:retire-abandoned-build",
                                    command.operation_id
                                )));
                            }
                        }
                    }
                    self.pending_build = None;
                    return;
                }
                if self.pending_build.is_some() {
                    self.advance_async_build();
                    return;
                }
                let source_key = self
                    .snapshot
                    .replicas
                    .iter()
                    .find_map(|(key, observation)| match &observation.agent {
                        AgentObservation::Report(report)
                            if report.identity.replica_id == command.local_replica_id =>
                        {
                            Some(key.clone())
                        }
                        _ => None,
                    })
                    .unwrap();
                if let Some(expected_session) = command.source_session_id.as_ref() {
                    let AgentObservation::Report(source) =
                        &self.snapshot.replicas[&source_key].agent
                    else {
                        panic!("build source report")
                    };
                    assert_eq!(
                        &source.process_session_id, expected_session,
                        "stale build command source session was fenced"
                    );
                }
                let target_key = ReplicaObservationKey::new(
                    command.target.replica_id,
                    command.target.instance_id.clone(),
                );
                assert!(
                    self.snapshot.replicas.contains_key(&target_key),
                    "stale build command target incarnation was fenced: {:?}",
                    command.target
                );
                let observed_source_build = match &self.snapshot.replicas[&source_key].agent {
                    AgentObservation::Report(report) => report
                        .builds
                        .iter()
                        .find(|build| build.build_id == command.operation_id)
                        .cloned(),
                    _ => unreachable!(),
                };
                let observed_target_build =
                    self.snapshot
                        .replicas
                        .get(&target_key)
                        .and_then(|observation| match &observation.agent {
                            AgentObservation::Report(report) => report
                                .builds
                                .iter()
                                .find(|build| build.build_id == command.operation_id)
                                .cloned(),
                            _ => None,
                        });
                let snapshot_boundary = observed_source_build.as_ref().map_or_else(
                    || match &self.snapshot.replicas[&source_key].agent {
                        AgentObservation::Report(report) => report.current_progress,
                        _ => unreachable!(),
                    },
                    |build| build.replication_boundary_lsn,
                );
                let catch_up_boundary = observed_source_build
                    .as_ref()
                    .and_then(|build| build.catch_up_boundary_lsn)
                    .or_else(|| {
                        observed_target_build
                            .as_ref()
                            .and_then(|build| build.catch_up_boundary_lsn)
                    })
                    .unwrap_or(snapshot_boundary + 2);
                let build = observed_source_build.unwrap_or(AgentBuildReport {
                    build_id: command.operation_id,
                    target: command.target,
                    last_sequence: 0,
                    replication_boundary_lsn: snapshot_boundary,
                    durable_lsn: snapshot_boundary,
                    completed: false,
                    catch_up_boundary_lsn: None,
                });
                let phase = if observed_target_build
                    .as_ref()
                    .is_some_and(|target| target.completed)
                {
                    return;
                } else if build.completed {
                    2
                } else if observed_target_build.is_some() {
                    1
                } else {
                    0
                };
                let AgentObservation::Report(source) =
                    &mut self.snapshot.replicas.get_mut(&source_key).unwrap().agent
                else {
                    panic!("build source report")
                };
                if !source
                    .builds
                    .iter()
                    .any(|observed| observed.build_id == build.build_id)
                {
                    source.builds = vec![build.clone()];
                }
                source.pending_operation_id = Some(OperationId::new(format!(
                    "{}:build-replica",
                    build.build_id
                )));
                source.report_sequence += 1;
                self.pending_build = Some(PendingBuild {
                    build,
                    source: source_key,
                    target: target_key,
                    catch_up_boundary,
                    phase,
                });
            }
            ProtocolCommand::EnsureConfiguration(command) => {
                let identity = ReplicaIdentity {
                    replica_id: command.local_replica_id,
                    instance_id: command.expected_instance_id.clone(),
                    agent_generation: command.expected_agent_generation.clone(),
                };
                let boundary = command.scale_up_evidence.as_deref().map_or_else(
                    || {
                        self.snapshot
                            .replicas
                            .get(&ReplicaObservationKey::new(
                                identity.replica_id,
                                identity.instance_id.clone(),
                            ))
                            .and_then(|observation| match &observation.agent {
                                AgentObservation::Report(report) => Some(report.current_progress),
                                _ => None,
                            })
                            .unwrap_or_default()
                    },
                    |evidence| evidence.intent().catch_up_boundary_lsn,
                );
                let durable_lsn = self
                    .validate_exact_durable_history(&identity, boundary)
                    .unwrap_or_else(|error| {
                        panic!("configuration requires exact complete durable history: {error}")
                    });
                let observation = self
                    .snapshot
                    .replicas
                    .get_mut(&ReplicaObservationKey::new(
                        identity.replica_id,
                        identity.instance_id.clone(),
                    ))
                    .unwrap();
                let AgentObservation::Report(report) = &mut observation.agent else {
                    panic!("configuration target report")
                };
                let member = command
                    .current_configuration
                    .members
                    .iter()
                    .find(|member| member.identity == identity)
                    .unwrap();
                // Configuration only installs authority. It must never synthesize
                // application history. Every progress claim below is derived from
                // the contiguous, byte-exact history already durably applied to
                // this exact incarnation. Logical receiver bookkeeping is not
                // admissible evidence.
                report.role = member.role;
                report.read_status = if command.transition_kind == TransitionKind::Failover
                    && member.role == ReplicaRole::Primary
                    && command.primary_write_status != AccessStatus::Granted
                {
                    AccessStatus::ReconfigurationPending
                } else {
                    AccessStatus::Granted
                };
                report.write_status = if member.role == ReplicaRole::Primary {
                    command.primary_write_status
                } else {
                    AccessStatus::NotPrimary
                };
                report.epoch = command.current_epoch;
                report.previous_configuration = command.previous_configuration.clone();
                report.current_configuration = Some(command.current_configuration.clone());
                report.current_progress = durable_lsn;
                report.verified_replication_lsn = Some(durable_lsn);
                report.committed_lsn = durable_lsn;
                report.current_configuration_quorum_progress = durable_lsn;
                report.catch_up_boundary = Some(boundary);
                report.catch_up_complete = true;
                if command.transition_kind == TransitionKind::Failover {
                    report.deactivation_epoch = Some(command.current_epoch);
                    report.deactivated_lsn = Some(durable_lsn);
                }
                report.pending_operation_id = None;
                report.pending_configuration = None;
                report.retained_operation_id = Some(command.operation_id);
                report.scale_up_intent = command
                    .scale_up_evidence
                    .map(|evidence| Box::new(evidence.intent().clone()));
                if report.scale_up_intent.is_some() {
                    report.prepared_secondary_removal = None;
                    report.secondary_removal_evidence = None;
                    report.retired_replica = None;
                    report.accepted_secondary_removal = None;
                }
                report.report_sequence += 1;
            }
            other => panic!("unexpected scale-up command: {other:?}"),
        }
    }

    pub fn report_mut(&mut self, replica_id: i64) -> &mut AgentReport {
        self.snapshot
            .replicas
            .values_mut()
            .find_map(|observation| match &mut observation.agent {
                AgentObservation::Report(report)
                    if report.identity.replica_id == ReplicaId::new(replica_id) =>
                {
                    Some(report.as_mut())
                }

                _ => None,
            })
            .unwrap()
    }

    pub fn restore_report_with_replication_catch_up(
        &mut self,
        key: &ReplicaObservationKey,
        mut report: Box<AgentReport>,
    ) {
        let history = self
            .durable_incarnations
            .entry(report.identity.instance_id.to_string())
            .or_default();
        for (lsn, value) in &self.durable_source {
            history.insert(*lsn, value.clone());
        }
        let progress = self
            .validate_exact_durable_history(
                &report.identity,
                self.durable_source
                    .keys()
                    .next_back()
                    .copied()
                    .unwrap_or_default(),
            )
            .expect("restored retained member exact catch-up");
        report.current_progress = progress;
        report.committed_lsn = progress;
        report.verified_replication_lsn = Some(progress);
        report.current_configuration_quorum_progress = progress;
        report.report_sequence += 1;
        self.snapshot.replicas.get_mut(key).unwrap().agent = AgentObservation::Report(report);
    }

    pub fn accepted_identities(&self) -> Vec<ReplicaIdentity> {
        self.snapshot
            .status
            .topology
            .as_ref()
            .unwrap()
            .configuration
            .members
            .iter()
            .map(|member| member.identity.clone())
            .collect()
    }

    pub fn remove_durable_value(&mut self, identity: &ReplicaIdentity, lsn: i64) {
        self.durable_incarnations
            .get_mut(identity.instance_id.as_str())
            .expect("modeled durable incarnation")
            .remove(&lsn);
    }

    pub fn replace_durable_value(
        &mut self,
        identity: &ReplicaIdentity,
        lsn: i64,
        value: impl Into<String>,
    ) {
        self.durable_incarnations
            .get_mut(identity.instance_id.as_str())
            .expect("modeled durable incarnation")
            .insert(lsn, value.into());
    }

    pub fn validate_exact_durable_history(
        &self,
        identity: &ReplicaIdentity,
        boundary: i64,
    ) -> Result<i64, String> {
        let history = self
            .durable_incarnations
            .get(identity.instance_id.as_str())
            .ok_or_else(|| format!("exact durable history missing for {identity:?}"))?;
        for lsn in 1..=boundary {
            let expected = self.durable_source.get(&lsn).ok_or_else(|| {
                format!("source durable history has an interior gap at LSN {lsn}")
            })?;
            match history.get(&lsn) {
                None => {
                    return Err(format!(
                        "exact durable history for {identity:?} has an interior gap at LSN {lsn}"
                    ));
                }
                Some(actual) if actual != expected => {
                    return Err(format!(
                        "exact durable history for {identity:?} has payload corruption at LSN \
                         {lsn}: expected={expected:?} actual={actual:?}"
                    ));
                }
                Some(_) => {}
            }
        }

        let mut contiguous: i64 = 0;
        while let Some(next) = contiguous.checked_add(1) {
            let (Some(expected), Some(actual)) =
                (self.durable_source.get(&next), history.get(&next))
            else {
                break;
            };
            if actual != expected {
                break;
            }
            contiguous = next;
        }
        Ok(contiguous)
    }

    pub fn truncate_durable_history(&mut self, through_lsn: i64) {
        self.durable_source.retain(|lsn, _| *lsn <= through_lsn);
        self.acknowledged_writes
            .retain(|lsn, _| *lsn <= through_lsn);
        self.durable_acknowledgements
            .retain(|lsn, _| *lsn <= through_lsn);
        for history in self.durable_incarnations.values_mut() {
            history.retain(|lsn, _| *lsn <= through_lsn);
        }
        for history in self.durable_receivers.values_mut() {
            history.retain(|lsn, _| *lsn <= through_lsn);
        }
        for observation in self.snapshot.replicas.values_mut() {
            if let AgentObservation::Report(report) = &mut observation.agent {
                report.current_progress = report.current_progress.min(through_lsn);
                report.committed_lsn = report.committed_lsn.min(through_lsn);
                report.verified_replication_lsn = report
                    .verified_replication_lsn
                    .map(|lsn| lsn.min(through_lsn));
                report.current_configuration_quorum_progress = report
                    .current_configuration_quorum_progress
                    .min(through_lsn);
            }
        }
    }

    pub fn has_pending_build(&self) -> bool {
        self.pending_build.is_some()
    }

    pub fn restart(&mut self, replica_id: i64) {
        let bytes = serde_json::to_vec(&(
            self.durable_source.clone(),
            self.durable_receivers.clone(),
            self.acknowledged_writes.clone(),
            self.durable_incarnations.clone(),
            self.durable_acknowledgements
                .iter()
                .map(|(lsn, acknowledgements)| {
                    (
                        *lsn,
                        acknowledgements
                            .iter()
                            .map(|(identity, session)| (identity.clone(), session.clone()))
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>(),
        ))
        .unwrap();
        let (
            source,
            receivers,
            acknowledged,
            incarnations,
            durable_acknowledgement_entries,
        ): RestartImage = serde_json::from_slice(&bytes).unwrap();
        self.durable_source = source;
        self.durable_receivers = receivers;
        self.acknowledged_writes = acknowledged;
        self.durable_incarnations = incarnations;
        self.durable_acknowledgements = durable_acknowledgement_entries
            .into_iter()
            .map(
                |(lsn, acknowledgements): (i64, Vec<(ReplicaIdentity, ProcessSessionId)>)| {
                    (lsn, acknowledgements.into_iter().collect())
                },
            )
            .collect();
        // The asynchronous executor is process-local. A restart may retain only
        // durable application bytes and observed reports; a later command
        // reconstructs execution from that evidence.
        self.pending_build = None;
        let report = self.report_mut(replica_id);
        report.pending_operation_id = None;
        report.process_session_id =
            ProcessSessionId::new(format!("{}-restart", report.process_session_id));
        report.report_sequence = 1;
    }

    pub fn apply_wait(&mut self, status: AcceptedStatus) {
        self.snapshot.status = status;
        self.advance_async_build();
    }

    fn advance_async_build(&mut self) {
        if self.pending_build.is_none() {
            let active_build_id = self
                .snapshot
                .status
                .transition
                .as_ref()
                .and_then(|transition| {
                    transition
                        .scale_up
                        .as_deref()
                        .map(|intent| intent.build_id.clone())
                })
                .or_else(|| {
                    self.snapshot
                        .status
                        .provisioning
                        .as_ref()
                        .and_then(|provisioning| {
                            provisioning.scale_up_build_id(&self.snapshot.resource_uid)
                        })
                });
            let observed = self
                .snapshot
                .replicas
                .iter()
                .find_map(|(source_key, observation)| {
                    let active_build_id = active_build_id.as_ref()?;
                    let AgentObservation::Report(source) = &observation.agent else {
                        return None;
                    };
                    let build = source
                        .builds
                        .iter()
                        .find(|build| &build.build_id == active_build_id)?
                        .clone();
                    let target_key = ReplicaObservationKey::new(
                        build.target.replica_id,
                        build.target.instance_id.clone(),
                    );
                    if !self.snapshot.replicas.contains_key(&target_key) {
                        return None;
                    }
                    let target_build =
                        self.snapshot
                            .replicas
                            .get(&target_key)
                            .and_then(|observation| match &observation.agent {
                                AgentObservation::Report(report) => report
                                    .builds
                                    .iter()
                                    .find(|target| target.build_id == build.build_id)
                                    .cloned(),
                                _ => None,
                            });
                    let phase = if target_build.as_ref().is_some_and(|target| target.completed) {
                        return None;
                    } else if build.completed {
                        2
                    } else if target_build.is_some() {
                        1
                    } else {
                        0
                    };
                    Some(PendingBuild {
                        catch_up_boundary: build
                            .catch_up_boundary_lsn
                            .or_else(|| {
                                target_build
                                    .as_ref()
                                    .and_then(|target| target.catch_up_boundary_lsn)
                            })
                            .unwrap_or(build.replication_boundary_lsn + 2),
                        build,
                        source: source_key.clone(),
                        target: target_key,
                        phase,
                    })
                });
            self.pending_build = observed;
        }
        let Some(pending) = self.pending_build.as_mut() else {
            return;
        };
        match pending.phase {
            0 => {
                let AgentObservation::Report(source) = &mut self
                    .snapshot
                    .replicas
                    .get_mut(&pending.source)
                    .unwrap()
                    .agent
                else {
                    panic!("build source report")
                };
                source.current_progress += 1;
                source.committed_lsn = source.current_progress;
                self.durable_source.insert(
                    source.current_progress,
                    format!("value-{}", source.current_progress),
                );
                let value = self.durable_source[&source.current_progress].clone();
                for member in self
                    .snapshot
                    .status
                    .topology
                    .as_ref()
                    .unwrap()
                    .configuration
                    .members
                    .iter()
                {
                    self.durable_incarnations
                        .entry(member.identity.instance_id.to_string())
                        .or_default()
                        .insert(source.current_progress, value.clone());
                }
                source.report_sequence += 1;
                let AgentObservation::Report(target) = &mut self
                    .snapshot
                    .replicas
                    .get_mut(&pending.target)
                    .unwrap()
                    .agent
                else {
                    panic!("build target report")
                };
                target.role = ReplicaRole::IdleSecondary;
                target.builds = vec![pending.build.clone()];
                target.report_sequence += 1;
                pending.phase = 1;
            }
            1 => {
                let AgentObservation::Report(source) = &mut self
                    .snapshot
                    .replicas
                    .get_mut(&pending.source)
                    .unwrap()
                    .agent
                else {
                    panic!("build source report")
                };
                source.current_progress = source.current_progress.max(pending.catch_up_boundary);
                source.committed_lsn = source.committed_lsn.max(pending.catch_up_boundary);
                self.durable_source
                    .entry(pending.catch_up_boundary)
                    .or_insert_with(|| format!("value-{}", pending.catch_up_boundary));
                let value = self.durable_source[&pending.catch_up_boundary].clone();
                for member in self
                    .snapshot
                    .status
                    .topology
                    .as_ref()
                    .unwrap()
                    .configuration
                    .members
                    .iter()
                {
                    self.durable_incarnations
                        .entry(member.identity.instance_id.to_string())
                        .or_default()
                        .insert(pending.catch_up_boundary, value.clone());
                }
                source.builds[0].last_sequence = 2;
                source.builds[0].durable_lsn = pending.catch_up_boundary;
                source.builds[0].completed = true;
                source.builds[0].catch_up_boundary_lsn = Some(pending.catch_up_boundary);
                source.report_sequence += 1;
                pending.phase = 2;
            }
            2 => {
                let AgentObservation::Report(target) = &mut self
                    .snapshot
                    .replicas
                    .get_mut(&pending.target)
                    .unwrap()
                    .agent
                else {
                    panic!("build target report")
                };
                target.current_progress = pending.catch_up_boundary;
                target.committed_lsn = pending.catch_up_boundary;
                target.builds[0].last_sequence = 2;
                target.builds[0].durable_lsn = pending.catch_up_boundary;
                target.builds[0].completed = true;
                target.builds[0].catch_up_boundary_lsn = Some(pending.catch_up_boundary);
                target.report_sequence += 1;
                let copied = self
                    .durable_source
                    .range(..=pending.catch_up_boundary)
                    .map(|(lsn, value)| (*lsn, value.clone()))
                    .collect::<BTreeMap<_, _>>();
                let receiver = self
                    .durable_receivers
                    .entry(pending.target.replica_id.value())
                    .or_default();
                let incarnation = self
                    .durable_incarnations
                    .entry(pending.target.instance_id.to_string())
                    .or_default();
                for (lsn, value) in copied {
                    if let Some(existing) = receiver.get(&lsn) {
                        assert_eq!(existing, &value, "receiver payload changed at LSN {lsn}");
                    }
                    if let Some(existing) = incarnation.get(&lsn) {
                        assert_eq!(existing, &value, "incarnation payload changed at LSN {lsn}");
                    }
                    receiver.insert(lsn, value.clone());
                    incarnation.insert(lsn, value);
                }
                let AgentObservation::Report(source) = &mut self
                    .snapshot
                    .replicas
                    .get_mut(&pending.source)
                    .unwrap()
                    .agent
                else {
                    panic!("build source report")
                };
                let build_operation =
                    OperationId::new(format!("{}:build-replica", pending.build.build_id));
                source.pending_operation_id = None;
                source.retained_operation_id = Some(build_operation);
                source.report_sequence += 1;
                self.pending_build = None;
            }
            _ => unreachable!(),
        }
    }

    pub fn remove_candidate_report(&mut self) {
        let target = self
            .snapshot
            .status
            .transition
            .as_ref()
            .and_then(|transition| transition.scale_up.as_deref())
            .map(|intent| intent.target.clone())
            .or_else(|| {
                self.snapshot
                    .status
                    .last_scale_up
                    .as_deref()
                    .map(|receipt| receipt.intent.target.clone())
            })
            .unwrap();
        self.snapshot
            .replicas
            .get_mut(&ReplicaObservationKey::new(
                target.replica_id,
                target.instance_id,
            ))
            .unwrap()
            .agent = AgentObservation::Absent;
    }

    pub fn acknowledge_write(&mut self, case: u64, step: u64) -> i64 {
        self.try_acknowledge_write(case, step, &BTreeSet::new())
            .unwrap_or_else(|reason| panic!("write was not durably acknowledgeable: {reason}"))
    }

    pub fn acknowledge_old_scale_up_authority_write(
        &mut self,
        case: u64,
        step: u64,
        replica_ids: &[i64],
    ) -> i64 {
        let intent = self
            .snapshot
            .status
            .transition
            .as_ref()
            .and_then(|transition| transition.scale_up_failover.as_deref())
            .map(|evidence| evidence.intent.clone())
            .expect("old-authority write requires a carried scale-up failover");
        let recipients = replica_ids
            .iter()
            .map(|id| {
                intent
                    .current_configuration
                    .members
                    .iter()
                    .find(|member| member.identity.replica_id == ReplicaId::new(*id))
                    .expect("old-authority recipient belongs to expanded CC")
                    .identity
                    .clone()
            })
            .collect::<Vec<_>>();
        assert!(
            recipients
                .iter()
                .any(|identity| identity == &intent.primary),
            "old-authority acknowledgement requires the exact original primary"
        );
        let previous_count = intent
            .previous_configuration
            .members
            .iter()
            .filter(|member| recipients.contains(&member.identity))
            .count();
        let current_count = intent
            .current_configuration
            .members
            .iter()
            .filter(|member| recipients.contains(&member.identity))
            .count();
        assert!(
            previous_count >= intent.previous_policy.write_quorum as usize
                && current_count >= intent.current_policy.write_quorum as usize,
            "old-authority acknowledgement lacks exact PC/CC write quorums"
        );
        let primary_session = self
            .snapshot
            .observation_for_identity(&intent.primary)
            .and_then(|observation| match &observation.agent {
                AgentObservation::Report(report) => Some(report.process_session_id.clone()),
                _ => None,
            })
            .expect("old primary report");
        let lsn = self.durable_source.keys().next_back().copied().unwrap_or(0) + 1;
        let value = format!("old-ack-{case}-{step}-{lsn}");
        let mut acknowledgements = BTreeMap::new();
        for identity in recipients {
            self.durable_incarnations
                .entry(identity.instance_id.to_string())
                .or_default()
                .insert(lsn, value.clone());
            let report = self.report_mut(identity.replica_id.value());
            report.current_progress = report.current_progress.max(lsn);
            report.committed_lsn = report.committed_lsn.max(lsn);
            report.current_configuration_quorum_progress =
                report.current_configuration_quorum_progress.max(lsn);
            report.report_sequence += 1;
            acknowledgements.insert(
                identity.clone(),
                if identity == intent.primary {
                    primary_session.clone()
                } else {
                    report.process_session_id.clone()
                },
            );
        }
        self.durable_source.insert(lsn, value.clone());
        self.acknowledged_writes.insert(lsn, value);
        self.durable_acknowledgements.insert(lsn, acknowledgements);
        lsn
    }

    pub fn can_acknowledge_write(&self) -> bool {
        self.fork()
            .try_acknowledge_write(u64::MAX, u64::MAX, &BTreeSet::new())
            .is_ok()
    }

    pub fn try_acknowledge_write(
        &mut self,
        case: u64,
        step: u64,
        dropped: &BTreeSet<ReplicaIdentity>,
    ) -> Result<i64, String> {
        let (previous, current, previous_policy, current_policy) = self.write_authority();
        let primary = current
            .members
            .iter()
            .find(|member| member.role == ReplicaRole::Primary)
            .expect("current primary")
            .identity
            .clone();
        let primary_session = self
            .exact_installed_report(&primary, previous.as_ref(), &current)
            .filter(|report| report.write_status == AccessStatus::Granted)
            .map(|report| report.process_session_id.clone());
        if primary_session.is_none() {
            return Err("exact installed primary does not grant writes".into());
        }
        let lsn = self.durable_source.keys().next_back().copied().unwrap_or(0) + 1;
        let value = format!("ack-{case}-{step}-{lsn}");
        let applied = current
            .members
            .iter()
            .filter_map(|member| {
                (!dropped.contains(&member.identity))
                    .then(|| {
                        self.snapshot.replicas.values().find_map(|observation| {
                            let AgentObservation::Report(report) = &observation.agent else {
                                return None;
                            };
                            (report.identity == member.identity && report.healthy)
                                .then_some(report.as_ref())
                        })
                    })
                    .flatten()
                    .map(|report| (member.identity.clone(), report.process_session_id.clone()))
            })
            .collect::<Vec<_>>();
        for (identity, _) in &applied {
            self.durable_incarnations
                .entry(identity.instance_id.to_string())
                .or_default()
                .insert(lsn, value.clone());
            if let Some(report) = self.snapshot.replicas.values_mut().find_map(|observation| {
                match &mut observation.agent {
                    AgentObservation::Report(report)
                        if report.identity == *identity
                            && matches!(
                                report.role,
                                ReplicaRole::Primary | ReplicaRole::ActiveSecondary
                            ) =>
                    {
                        Some(report.as_mut())
                    }
                    _ => None,
                }
            }) {
                report.current_progress = report.current_progress.max(lsn);
                report.committed_lsn = report.committed_lsn.max(lsn);
                report.verified_replication_lsn =
                    Some(report.verified_replication_lsn.unwrap_or_default().max(lsn));
                report.current_configuration_quorum_progress =
                    report.current_configuration_quorum_progress.max(lsn);
                report.report_sequence += 1;
            }
        }
        let delivered = applied
            .iter()
            .filter(|(identity, _)| {
                self.exact_installed_report(identity, previous.as_ref(), &current)
                    .is_some()
            })
            .cloned()
            .collect::<Vec<_>>();
        // A completed copy stream may continue carrying ordered operations to
        // the unadmitted IdleSecondary. This is explicit durable delivery, but
        // it contributes no PC/CC acknowledgement credit until that exact
        // incarnation has installed membership authority.
        let build_targets = self
            .snapshot
            .replicas
            .values()
            .filter_map(|observation| match &observation.agent {
                AgentObservation::Report(report)
                    if report.role == ReplicaRole::IdleSecondary && !report.builds.is_empty() =>
                {
                    Some(report.identity.clone())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        for identity in build_targets {
            self.durable_incarnations
                .entry(identity.instance_id.to_string())
                .or_default()
                .insert(lsn, value.clone());
            let report = self
                .snapshot
                .replicas
                .values_mut()
                .find_map(|observation| match &mut observation.agent {
                    AgentObservation::Report(report) if report.identity == identity => {
                        Some(report.as_mut())
                    }
                    _ => None,
                })
                .expect("build target report");
            report.current_progress = report.current_progress.max(lsn);
            report.committed_lsn = report.committed_lsn.max(lsn);
            report.report_sequence += 1;
        }
        let delivered_identities = delivered
            .iter()
            .map(|(identity, _)| identity)
            .collect::<BTreeSet<_>>();
        let current_count = current
            .members
            .iter()
            .filter(|member| delivered_identities.contains(&member.identity))
            .count();
        let previous_count = previous.as_ref().map_or(current_count, |configuration| {
            configuration
                .members
                .iter()
                .filter(|member| delivered_identities.contains(&member.identity))
                .count()
        });
        if previous_count < previous_policy.write_quorum as usize {
            return Err(format!(
                "previous write quorum missing: delivered={previous_count}, required={}",
                previous_policy.write_quorum
            ));
        }
        if current_count < current_policy.write_quorum as usize {
            return Err(format!(
                "current write quorum missing: delivered={current_count}, required={}",
                current_policy.write_quorum
            ));
        }
        let acknowledgements = delivered.into_iter().collect::<BTreeMap<_, _>>();
        assert_eq!(
            acknowledgements.get(&primary),
            primary_session.as_ref(),
            "primary acknowledgement must bind the exact installed session"
        );
        self.durable_source.insert(lsn, value.clone());
        self.acknowledged_writes.insert(lsn, value);
        self.durable_acknowledgements.insert(lsn, acknowledgements);
        Ok(lsn)
    }

    fn write_authority(
        &self,
    ) -> (
        Option<ConfigurationDescriptor>,
        ConfigurationDescriptor,
        EffectivePolicy,
        EffectivePolicy,
    ) {
        if let Some(transition) = &self.snapshot.status.transition
            && let Some(intent) = transition.scale_up.as_deref().or_else(|| {
                transition
                    .scale_up_failover
                    .as_deref()
                    .map(|evidence| &evidence.intent)
            })
        {
            return (
                Some(intent.previous_configuration.clone()),
                transition.current_configuration.clone(),
                intent.previous_policy.clone(),
                intent.current_policy.clone(),
            );
        }
        let configuration = self
            .snapshot
            .status
            .topology
            .as_ref()
            .unwrap()
            .configuration
            .clone();
        let policy = self.snapshot.status.effective_policy.clone().unwrap();
        (None, configuration, policy.clone(), policy)
    }

    fn exact_installed_report(
        &self,
        identity: &ReplicaIdentity,
        previous: Option<&ConfigurationDescriptor>,
        current: &ConfigurationDescriptor,
    ) -> Option<&AgentReport> {
        self.snapshot.replicas.values().find_map(|observation| {
            let AgentObservation::Report(report) = &observation.agent else {
                return None;
            };
            (report.identity == *identity
                && report.current_configuration.as_ref() == Some(current)
                && report.previous_configuration.as_ref() == previous
                && current
                    .members
                    .iter()
                    .any(|member| member.identity == report.identity && member.role == report.role))
            .then_some(report.as_ref())
        })
    }

    pub fn assert_acknowledged_write_oracle(&self) {
        let status = &self.snapshot.status;
        let configuration = &status.topology.as_ref().unwrap().configuration;
        let policy = status.effective_policy.as_ref().unwrap();
        for (lsn, value) in &self.acknowledged_writes {
            let durable_acks = self
                .durable_acknowledgements
                .get(lsn)
                .unwrap_or_else(|| panic!("acknowledged write {lsn} has no durable ack record"));
            assert!(
                durable_acks.iter().all(|(identity, session)| {
                    !session.as_str().is_empty()
                        && self
                            .durable_incarnations
                            .get(identity.instance_id.as_str())
                            .and_then(|history| history.get(lsn))
                            == Some(value)
                }),
                "acknowledgement record for {lsn} is not backed by exact durable application"
            );
            let recoverable = configuration
                .members
                .iter()
                .filter(|member| {
                    self.durable_incarnations
                        .get(member.identity.instance_id.as_str())
                        .and_then(|history| history.get(lsn))
                        == Some(value)
                })
                .count();
            assert!(
                recoverable >= policy.read_quorum as usize,
                "acknowledged write {lsn}={value:?} is present on only {recoverable} \
                 accepted members; read quorum is {}",
                policy.read_quorum
            );
        }
        assert!(
            configuration.members.len() < usize::BITS as usize,
            "model recovery-mask width exceeded"
        );
        for mask in 0_usize..(1_usize << configuration.members.len()) {
            if mask.count_ones() < policy.read_quorum {
                continue;
            }
            for (lsn, value) in &self.acknowledged_writes {
                assert!(
                    configuration
                        .members
                        .iter()
                        .enumerate()
                        .filter(|(index, _)| mask & (1 << index) != 0)
                        .any(|(_, member)| {
                            self.durable_incarnations
                                .get(member.identity.instance_id.as_str())
                                .and_then(|history| history.get(lsn))
                                == Some(value)
                        }),
                    "valid terminal recovery mask {mask:#b} lost acknowledged \
                     write {lsn}={value:?}"
                );
            }
        }
    }

    pub fn assert_safety_invariants(&mut self) {
        self.validate_exact_candidate_set().unwrap();
        validate_status(&self.snapshot.status).unwrap();
        let accepted = &self
            .snapshot
            .status
            .topology
            .as_ref()
            .unwrap()
            .configuration;
        if let Some(previous) = self.accepted_epochs.last() {
            assert!(
                accepted.epoch >= *previous,
                "accepted epoch regressed from {previous:?} to {:?}",
                accepted.epoch
            );
        }
        let policy = self.snapshot.status.effective_policy.as_ref().unwrap();
        assert_eq!(
            accepted.members.len(),
            policy.replica_set_size as usize,
            "accepted topology and policy cardinality diverged"
        );

        let mut active_targets = BTreeSet::new();
        if let Some(allocation) = &self.snapshot.status.scale_up_allocation {
            active_targets.insert(allocation.observation_target());
        }
        if let Some(provisioning) = &self.snapshot.status.provisioning
            && provisioning.scale_up().is_some()
        {
            active_targets.insert(provisioning.target_identity(&self.snapshot.resource_uid));
        }
        if let Some(transition) = &self.snapshot.status.transition
            && let Some(intent) = transition.scale_up.as_deref().or_else(|| {
                transition
                    .scale_up_failover
                    .as_deref()
                    .map(|evidence| &evidence.intent)
            })
        {
            active_targets.insert(intent.target.clone());
        }
        if let Some(cleanup) = &self.snapshot.status.scale_up_cleanup {
            active_targets.insert(cleanup.target.clone());
            assert!(
                !accepted
                    .members
                    .iter()
                    .any(|member| member.identity == cleanup.target),
                "committed member acquired pre-admission cleanup authority"
            );
            assert!(
                !self
                    .committed_incarnations
                    .contains(cleanup.target.instance_id.as_str()),
                "historically committed incarnation acquired candidate cleanup authority"
            );
        }
        assert!(
            active_targets.len() <= 1,
            "more than one exact scale-up candidate identity is active: {active_targets:?}"
        );

        for observation in self.snapshot.replicas.values() {
            let AgentObservation::Report(report) = &observation.agent else {
                continue;
            };
            for build in &report.builds {
                if let Some(boundary) = build.catch_up_boundary_lsn {
                    match self.frozen_boundaries.get(&build.build_id) {
                        Some(existing) => assert_eq!(
                            *existing, boundary,
                            "catch-up boundary mutated for {}",
                            build.build_id
                        ),
                        None => {
                            self.frozen_boundaries
                                .insert(build.build_id.clone(), boundary);
                        }
                    }
                }
            }
            if !accepted
                .members
                .iter()
                .any(|member| member.identity == report.identity)
            {
                assert_ne!(
                    report.write_status,
                    AccessStatus::Granted,
                    "unadmitted candidate received write-quorum authority"
                );
            }
        }
    }
}
