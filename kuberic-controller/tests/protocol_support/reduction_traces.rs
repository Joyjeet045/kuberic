use super::scale_down_model::{CommandBoundary, Model};
use kuberic_controller::plan::Plan;
use kuberic_runtime::protocol::command::{KubernetesChange, ProtocolCommand};
use kuberic_runtime::protocol::observation::*;
use kuberic_runtime::protocol::types::*;
use kuberic_runtime::protocol::validation::validate_secondary_scale_down_cleanup;
use std::collections::BTreeMap;

#[derive(Clone)]
struct Trace {
    model: Model,
    physical: BTreeMap<ReplicaObservationKey, ReplicaObservation>,
    resources: Vec<SecondaryScaleDownResourceObservation>,
    initial: ConfigurationDescriptor,
    seed: u64,
    events: Vec<String>,
}

impl Trace {
    fn new(size: i64, desired: u32, seed: u64) -> Self {
        let mut model = Model::new(&(1..=size).collect::<Vec<_>>(), 1, desired);
        let initial = model
            .snapshot
            .status
            .topology
            .as_ref()
            .unwrap()
            .configuration
            .clone();
        // Admission is healthy; subsequent primary loss must not become failover.
        model.step();
        Self {
            physical: model.snapshot.replicas.clone(),
            resources: model.snapshot.secondary_scale_down_resources.clone(),
            initial,
            model,
            seed,
            events: Vec::new(),
        }
    }

    fn observe(&mut self, available: u64) {
        self.model.snapshot.replicas = self.physical.clone();
        self.model.snapshot.secondary_scale_down_resources = self.resources.clone();
        self.model.snapshot.previous_report_watermarks.clear();
        for (key, observation) in &mut self.model.snapshot.replicas {
            if available & (1 << (key.replica_id.value() - 1)) == 0 {
                observation.agent = AgentObservation::Unreachable {
                    message: "partition".into(),
                };
            }
        }
    }

    fn advance(&mut self, event: u64) -> bool {
        let before = self.model.snapshot.status.clone();
        let plan = self.model.plan();
        self.events.push(format!("{event}: {plan:?}"));
        if let Plan::Apply { changes } = &plan {
            for change in changes {
                if let KubernetesChange::PersistStatus { status } = change
                    && status.secondary_scale_down_cleanup.is_some()
                {
                    validate_secondary_scale_down_cleanup(
                        status.secondary_scale_down_cleanup.as_ref().unwrap(),
                    )
                    .unwrap();
                }
                if matches!(change, KubernetesChange::DeleteScaleDownResource { .. }) {
                    assert!(before.transition.is_none());
                    assert!(
                        before.secondary_scale_down_cleanup.is_some(),
                        "bounded convergence receipt has no deletion authority"
                    );
                }
            }
        }
        // Lost requests/conflicted status and finalizer-held deletes have no effect.
        if event == 0 || matches!(plan, Plan::Unsafe { .. }) {
            assert_eq!(self.model.snapshot.status, before);
            return false;
        }
        self.model.snapshot.replicas = self.physical.clone();
        self.model.snapshot.secondary_scale_down_resources = self.resources.clone();
        self.model.snapshot.previous_report_watermarks.clear();
        if matches!(&plan, Plan::Execute { command }
            if matches!(command, ProtocolCommand::EnsureConfiguration(c)
                if c.transition_kind == TransitionKind::SecondaryScaleDown)
                || matches!(command, ProtocolCommand::RetireReplica(_)))
            && (1..=3).contains(&event)
        {
            self.model.interrupt(
                plan.clone(),
                match event {
                    1 => CommandBoundary::Intent,
                    2 => CommandBoundary::Pending,
                    _ => CommandBoundary::Effect,
                },
            );
        } else {
            self.model.apply(plan.clone());
        }
        self.physical = self.model.snapshot.replicas.clone();
        self.resources = self.model.snapshot.secondary_scale_down_resources.clone();
        self.invariants(&before);
        matches!(plan, Plan::Stable { .. })
    }

    fn invariants(&self, before: &AcceptedStatus) {
        let status = &self.model.snapshot.status;
        let previous = &before.topology.as_ref().unwrap().configuration;
        let current = &status.topology.as_ref().unwrap().configuration;
        assert_eq!(current.primary_id, self.initial.primary_id);
        assert_eq!(
            current.epoch.data_loss_number,
            self.initial.epoch.data_loss_number
        );
        assert!(current.epoch >= previous.epoch);
        assert!(current.members.iter().all(|m| previous.members.contains(m)));
        assert_eq!(
            status.effective_policy.as_ref().unwrap(),
            &EffectivePolicy::fixed(current.members.len() as u32, 30).unwrap()
        );
        assert!(!(status.transition.is_some() && status.secondary_scale_down_cleanup.is_some()));
        let intent = status
            .transition
            .as_ref()
            .and_then(|t| t.secondary_scale_down.as_ref())
            .or_else(|| {
                status
                    .secondary_scale_down_cleanup
                    .as_ref()
                    .map(|c| &c.evidence.preparation.intent)
            });
        if let Some(intent) = intent {
            assert_eq!(intent.primary, self.initial.members[0].identity);
            assert_eq!(
                intent.current_configuration.members,
                intent
                    .previous_configuration
                    .members
                    .iter()
                    .filter(|m| m.identity != intent.target)
                    .cloned()
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                intent.previous_policy,
                EffectivePolicy::fixed(intent.previous_configuration.members.len() as u32, 30)
                    .unwrap()
            );
            assert_eq!(
                intent.current_policy,
                EffectivePolicy::fixed(intent.current_configuration.members.len() as u32, 30)
                    .unwrap()
            );
            if let Some(old) = before
                .transition
                .as_ref()
                .and_then(|t| t.secondary_scale_down.as_ref())
                .or_else(|| {
                    before
                        .secondary_scale_down_cleanup
                        .as_ref()
                        .map(|c| &c.evidence.preparation.intent)
                })
            {
                assert_eq!(
                    intent, old,
                    "immutable target and authority until cleanup completes"
                );
            }
        }
        let mut writable = 0;
        for observation in self.physical.values() {
            if let AgentObservation::Report(r) = &observation.agent {
                if r.write_status == AccessStatus::Granted {
                    writable += 1;
                    assert_eq!(r.identity, self.initial.members[0].identity);
                    assert!(r.previous_configuration.is_none(), "no PC/CC client writes");
                }
                if r.previous_configuration.is_some() && r.secondary_removal_evidence.is_some() {
                    assert_ne!(r.write_status, AccessStatus::Granted);
                }
            }
        }
        assert!(writable <= 1);
        for removed in &self.model.removed {
            assert!(!current.members.iter().any(|m| m.identity == *removed));
        }
    }

    fn heal(&mut self, desired: u32, absent_targets: bool) {
        self.model.snapshot.desired.replicas = desired;
        self.model.snapshot.desired.generation += 1;
        for _ in 0..250 {
            let target = self
                .model
                .snapshot
                .status
                .transition
                .as_ref()
                .and_then(|t| t.secondary_scale_down.as_ref())
                .or_else(|| {
                    self.model
                        .snapshot
                        .status
                        .secondary_scale_down_cleanup
                        .as_ref()
                        .map(|c| &c.evidence.preparation.intent)
                })
                .map(|i| i.target.replica_id.value());
            let available = if absent_targets {
                target.map_or(u64::MAX, |id| u64::MAX ^ (1 << (id - 1)))
            } else {
                u64::MAX
            };
            self.observe(available);
            self.model.controller_restart();
            if self.advance(10) {
                assert_eq!(
                    self.model
                        .snapshot
                        .status
                        .topology
                        .as_ref()
                        .unwrap()
                        .configuration
                        .members
                        .len(),
                    desired as usize
                );
                assert!(
                    self.model
                        .snapshot
                        .status
                        .secondary_scale_down_cleanup
                        .is_none()
                );
                return;
            }
        }
        panic!(
            "fair recovery failed seed={} events={:#?}",
            self.seed, self.events
        );
    }
}

#[test]
fn every_availability_mask_at_each_authority_boundary_heals_without_watches() {
    let mut cases = 0;
    for size in 2..=5 {
        let mut baseline = Trace::new(size, size as u32 - 1, size as u64);
        for boundary in 0..80 {
            if baseline.model.snapshot.status.transition.is_none() {
                break;
            }
            for mask in 0..(1 << size) {
                cases += 1;
                let mut trace = baseline.clone();
                trace.seed = (size as u64) << 32 | boundary << 8 | mask;
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    for _ in 0..8 {
                        trace.observe(mask);
                        trace.advance(10);
                    }
                    if mask & 1 == 0 {
                        assert!(
                            trace.model.removed.is_empty(),
                            "no commit without exact primary"
                        );
                        assert!(trace.model.deletes.is_empty());
                    }
                    trace.heal(size as u32 - 1, mask & (1 << (size - 1)) == 0);
                }));
                assert!(
                    result.is_ok(),
                    "seed={} size={size} boundary={boundary} mask={mask} events={:#?}",
                    trace.seed,
                    trace.events
                );
            }
            baseline.observe(u64::MAX);
            baseline.advance(10);
        }
    }
    eprintln!("exhaustive precommit boundary/availability traces: {cases}");
}

#[test]
fn seeded_delivery_session_conflict_and_lookup_traces_converge_sequentially() {
    for size in 2..=5 {
        for seed in 1..=16 {
            let mut trace = Trace::new(size, 1, seed);
            let mut rng = seed;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                for step in 0..64 {
                    rng = rng
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    let event = (rng >> 32) % 12;
                    let mask = if event == 4 {
                        rng & ((1 << size) - 1)
                    } else {
                        u64::MAX
                    };
                    trace.observe(mask);
                    match event {
                        5 => {
                            for (key, o) in &mut trace.physical {
                                if let AgentObservation::Report(r) = &mut o.agent {
                                    r.process_session_id = ProcessSessionId::new(format!(
                                        "seed-{seed}-{step}-{key:?}"
                                    ));
                                    r.report_sequence = 1;
                                }
                            }
                            trace.observe(mask);
                        }
                        6 => {
                            for (key, o) in &trace.model.snapshot.replicas {
                                if let AgentObservation::Report(r) = &o.agent {
                                    trace.model.snapshot.previous_report_watermarks.insert(
                                        key.clone(),
                                        ReportWatermark {
                                            process_session_id: r.process_session_id.clone(),
                                            report_sequence: r.report_sequence + 1,
                                        },
                                    );
                                }
                            }
                        }
                        7 => {
                            trace.model.snapshot.desired.generation += 1;
                            trace.model.snapshot.desired.replicas =
                                1 + (rng % (size as u64 + 2)) as u32;
                        }
                        8 => {
                            for resource in &mut trace.model.snapshot.secondary_scale_down_resources
                            {
                                resource.pod = ExactResourceObservation::LookupFailed {
                                    message: "exact GET failed".into(),
                                };
                            }
                        }
                        9 => trace.model.controller_restart(),
                        _ => {}
                    }
                    if event == 8 {
                        assert!(!matches!(trace.model.plan(), Plan::Apply { changes }
                        if changes.iter().any(|c| matches!(c, KubernetesChange::DeleteScaleDownResource {
                            resource: kuberic_runtime::protocol::command::ScaleDownResource::Pod | kuberic_runtime::protocol::command::ScaleDownResource::Pvc, ..
                        }))));
                    }
                    trace.advance(event);
                }
                trace.heal(1, true);
                assert_eq!(
                    trace
                        .model
                        .removed
                        .iter()
                        .map(|r| r.replica_id.value())
                        .collect::<Vec<_>>(),
                    (2..=size).rev().collect::<Vec<_>>()
                );
                assert_eq!(trace.model.deletes.len(), 3 * (size as usize - 1));
            }));
            assert!(
                result.is_ok(),
                "seed={seed} size={size} events={:#?}",
                trace.events
            );
        }
    }
}

#[test]
fn five_to_two_status_reload_at_every_step_preserves_three_serial_removals() {
    let mut trace = Trace::new(5, 2, 0x502);
    trace.heal(2, true);
    assert_eq!(
        trace
            .model
            .removed
            .iter()
            .map(|r| r.replica_id.value())
            .collect::<Vec<_>>(),
        [5, 4, 3]
    );
    assert_eq!(trace.model.deletes.len(), 9);
}

#[test]
fn exact_lookup_replacements_failures_and_finalizers_never_substitute_uids() {
    use kuberic_runtime::protocol::command::ScaleDownResource;
    for size in [2, 3, 5] {
        for resource in [
            ScaleDownResource::Endpoint,
            ScaleDownResource::Pod,
            ScaleDownResource::Pvc,
        ] {
            let mut baseline = Trace::new(size, size as u32 - 1, 0xfee);
            for _ in 0..80 {
                baseline.observe(u64::MAX);
                if matches!(baseline.model.plan(), Plan::Apply { changes }
                    if matches!(changes.first(), Some(KubernetesChange::DeleteScaleDownResource { resource: kind, .. }) if *kind == resource))
                {
                    break;
                }
                baseline.advance(10);
            }
            for state in 0..4 {
                let mut trace = baseline.clone();
                let frozen = trace
                    .model
                    .snapshot
                    .status
                    .secondary_scale_down_cleanup
                    .clone()
                    .unwrap();
                let exact = trace
                    .resources
                    .iter_mut()
                    .find(|r| r.target == frozen.evidence.preparation.intent.target)
                    .unwrap();
                let observation = match resource {
                    ScaleDownResource::Endpoint => &mut exact.endpoint,
                    ScaleDownResource::Pod => &mut exact.pod,
                    ScaleDownResource::Pvc => &mut exact.pvc,
                };
                let original = observation.clone();
                *observation = match state {
                    0 => ExactResourceObservation::LookupFailed {
                        message: "403".into(),
                    },
                    1 => ExactResourceObservation::NotFound,
                    2 => ExactResourceObservation::ReplacementPresent {
                        uid: format!("replacement-{size}-{resource:?}"),
                        resource_version: "new-rv".into(),
                    },
                    _ => original.clone(),
                };
                let deleted = trace.model.deletes.clone();
                if state == 0 || state == 3 {
                    for _ in 0..3 {
                        trace.observe(u64::MAX);
                        trace.model.controller_restart();
                        trace.advance(if state == 3 { 0 } else { 10 });
                        assert_eq!(
                            trace.model.deletes, deleted,
                            "failed GET/finalizer size={size} resource={resource:?}"
                        );
                        assert_eq!(
                            trace.model.snapshot.status.secondary_scale_down_cleanup,
                            Some(frozen.clone())
                        );
                    }
                    let exact = trace
                        .resources
                        .iter_mut()
                        .find(|r| r.target == frozen.evidence.preparation.intent.target)
                        .unwrap();
                    *match resource {
                        ScaleDownResource::Endpoint => &mut exact.endpoint,
                        ScaleDownResource::Pod => &mut exact.pod,
                        ScaleDownResource::Pvc => &mut exact.pvc,
                    } = original;
                }
                trace.heal(size as u32 - 1, false);
                assert_eq!(
                    trace.model.deletes.len(),
                    if state == 1 || state == 2 { 2 } else { 3 }
                );
                assert!(
                    trace
                        .model
                        .deletes
                        .iter()
                        .all(|(_, uid)| !uid.starts_with("replacement-"))
                );
            }
        }
    }
}
