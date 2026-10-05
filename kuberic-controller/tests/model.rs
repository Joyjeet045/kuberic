use std::collections::{BTreeMap, BTreeSet};
use std::hash::{DefaultHasher, Hash, Hasher};

use kuberic_controller::evaluator::{EvaluationConfig, evaluate};
use kuberic_controller::plan::Plan;
use kuberic_runtime::protocol::command::{KubernetesChange, ProtocolCommand, ScaleDownResource};
use kuberic_runtime::protocol::observation::{
    AgentObservation, AgentReport, DesiredState, KubernetesReplicaObservation, ObservationSnapshot,
    ReplicaObservation, ReplicaObservationKey, ReportWatermark, RoutingObservation,
};
use kuberic_runtime::protocol::types::{
    AcceptedStatus, AcceptedTopology, AccessStatus, AgentGeneration, ConfigurationDescriptor,
    ConfigurationMember, EffectivePolicy, Epoch, FaultType, OperationId, PlannedSwitchoverOutcome,
    PlannedSwitchoverRequest, PodUid, ProcessSessionId, ProvisioningIntent, ProvisioningPurpose,
    PvcUid, ReplicaId, ReplicaIdentity, ReplicaInstanceId, ReplicaRole, ResourceUid, ScaleUpStage,
    SwitchoverHandoff, SwitchoverRequestId, TransitionIntent, TransitionKind, derive_transition_id,
};
use kuberic_runtime::protocol::validation::{ValidationError, validate_snapshot, validate_status};

#[allow(dead_code)]
#[path = "protocol_support/scale_up_model.rs"]
mod scale_up_model;

#[test]
fn scale_up_model_is_level_triggered_sequential_and_restart_deterministic() {
    let mut model = scale_up_model::Model::new(1, 3);
    for _ in 0..160 {
        let replayed = model.snapshot.clone();
        let first = model.plan();
        assert_eq!(first, evaluate(&replayed, &scale_up_model::config()));
        if model.step() && model.accepted_count() == 3 {
            break;
        }
    }
    assert_eq!(model.accepted_history, vec![1, 2, 3]);
    assert_eq!(model.accepted_count(), 3);
}

#[test]
fn restart_drops_volatile_build_execution_and_fences_queued_old_reports() {
    let mut model = scale_up_model::Model::new(1, 2);
    let build_command = loop {
        let plan = model.plan();
        model.step();
        if model.has_pending_build() {
            let Plan::Execute {
                command: ProtocolCommand::EnsureReplicaBuild(command),
            } = plan
            else {
                continue;
            };
            break command;
        }
    };
    let source_id = build_command.local_replica_id.value();
    let (source_key, queued_report) = model
        .snapshot
        .replicas
        .iter()
        .find_map(|(key, observation)| match &observation.agent {
            AgentObservation::Report(report)
                if report.identity.replica_id == ReplicaId::new(source_id) =>
            {
                Some((key.clone(), report.clone()))
            }
            _ => None,
        })
        .unwrap();
    model.restart(source_id);
    assert!(
        !model.has_pending_build(),
        "restart retained process-local build execution"
    );
    let AgentObservation::Report(restarted_report) = &model.snapshot.replicas[&source_key].agent
    else {
        unreachable!()
    };
    assert_ne!(
        restarted_report.process_session_id,
        queued_report.process_session_id
    );

    let mut delayed = model.snapshot.clone();
    delayed.previous_report_watermarks.insert(
        source_key.clone(),
        ReportWatermark {
            process_session_id: restarted_report.process_session_id.clone(),
            report_sequence: restarted_report.report_sequence,
        },
    );
    let AgentObservation::Report(delayed_report) =
        &mut delayed.replicas.get_mut(&source_key).unwrap().agent
    else {
        unreachable!()
    };
    delayed_report.process_session_id = queued_report.process_session_id.clone();
    delayed_report.report_sequence = queued_report.report_sequence;
    let accepted = delayed.status.topology.clone();
    let plan = evaluate(&delayed, &scale_up_model::config());
    match plan {
        Plan::Wait { status, .. } | Plan::Unsafe { status, .. } => {
            assert_eq!(status.topology, accepted);
        }
        Plan::Apply { changes } => assert!(changes.iter().all(|change| {
            matches!(
                change,
                KubernetesChange::PersistStatus { status } if status.topology == accepted
            )
        })),
        Plan::Execute {
            command: ProtocolCommand::EnsureReplicaBuild(replayed),
        } => {
            assert_eq!(
                replayed, build_command,
                "stale report may only trigger the exact deterministic durable build replay"
            );
            assert_eq!(delayed.status.topology, accepted);
        }
        other => panic!("queued old-session report escaped its fence: {other:?}"),
    }

    model.run(160);
    assert_eq!(model.accepted_count(), 2);
}

#[test]
fn queued_old_build_command_is_fenced_after_candidate_session_changes() {
    let mut model = scale_up_model::Model::new(1, 2);
    let queued = loop {
        match model.plan() {
            Plan::Execute {
                command: ProtocolCommand::EnsureReplicaBuild(command),
            } => break ProtocolCommand::EnsureReplicaBuild(command),
            _ => {
                model.step();
            }
        }
    };
    let ProtocolCommand::EnsureReplicaBuild(old) = &queued else {
        unreachable!()
    };
    let old_target = old.target.clone();
    let old_session = model
        .snapshot
        .replicas
        .get(&ReplicaObservationKey::new(
            old_target.replica_id,
            old_target.instance_id.clone(),
        ))
        .and_then(|observation| match &observation.agent {
            AgentObservation::Report(report) => Some(report.process_session_id.clone()),
            _ => None,
        })
        .unwrap();

    model.snapshot.desired.replicas = 1;
    model.snapshot.desired.generation += 1;
    model.run(160);
    model.snapshot.desired.replicas = 2;
    model.snapshot.desired.generation += 1;
    let fresh = loop {
        model.step();
        if let Some(report) = model.snapshot.replicas.values().find_map(|observation| {
            let AgentObservation::Report(report) = &observation.agent else {
                return None;
            };
            (report.identity.replica_id == old_target.replica_id && report.identity != old_target)
                .then_some(report.clone())
        }) {
            break report;
        }
    };
    assert_ne!(fresh.process_session_id, old_session);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        model.execute(queued);
    }))
    .expect_err("old candidate command survived fresh-incarnation fencing");
    let panic = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("<non-string panic>");
    assert!(
        panic.contains("stale build command target incarnation was fenced"),
        "queued command failed at the wrong assertion: {panic}"
    );
    model.run(160);
    assert_eq!(model.accepted_count(), 2);
}

#[derive(Clone, Copy)]
struct ModelRng(u64);

impl ModelRng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }

    fn index(&mut self, upper: usize) -> usize {
        (self.next() as usize) % upper
    }
}

fn model_env_u64(name: &str, default: u64) -> u64 {
    let Ok(value) = std::env::var(name) else {
        return default;
    };
    let parsed = value
        .strip_prefix("0x")
        .map_or_else(|| value.parse(), |hex| u64::from_str_radix(hex, 16));
    parsed.unwrap_or_else(|error| panic!("invalid {name}={value:?}: {error}"))
}

fn scale_up_model_writable(model: &scale_up_model::Model) -> bool {
    model.can_acknowledge_write()
}

fn assert_stale_report_cannot_advance(model: &scale_up_model::Model) {
    let mut stale = model.snapshot.clone();
    let Some((key, report)) = stale.replicas.iter().find_map(|(key, observation)| {
        let AgentObservation::Report(report) = &observation.agent else {
            return None;
        };
        Some((key.clone(), report.clone()))
    }) else {
        return;
    };
    stale.previous_report_watermarks.insert(
        key,
        ReportWatermark {
            process_session_id: report.process_session_id.clone(),
            report_sequence: report.report_sequence + 1,
        },
    );
    let accepted = stale.status.topology.clone();
    let policy = stale.status.effective_policy.clone();
    match evaluate(&stale, &scale_up_model::config()) {
        Plan::Wait { status, .. } | Plan::Unsafe { status, .. } => {
            assert_eq!(status.topology, accepted);
            assert_eq!(status.effective_policy, policy);
        }
        Plan::Apply { changes } => {
            assert!(changes.iter().all(|change| {
                matches!(
                    change,
                    KubernetesChange::PersistStatus { status }
                        if status.topology == accepted
                            && status.effective_policy == policy
                )
            }));
        }
        Plan::Execute { command } => {
            panic!("stale report authorized command {command:?}");
        }
        Plan::Stable { .. } => panic!("stale report was classified as stable"),
    }
}

#[test]
fn seeded_scale_up_adversarial_histories_preserve_authority_and_writes() {
    const RECORDED_SEED: u64 = 0x5ca1_e006_d15c_a11e;
    const RECORDED_CASES: u64 = 24;
    let configured_seed = model_env_u64("KUBERIC_MODEL_SEED", RECORDED_SEED);
    let cases = model_env_u64("KUBERIC_MODEL_CASES", RECORDED_CASES);
    let explicit_seed = std::env::var_os("KUBERIC_MODEL_SEED").is_some();
    let run_seeds = if explicit_seed {
        vec![configured_seed]
    } else {
        vec![
            configured_seed,
            configured_seed.wrapping_add(0x9e37_79b9_7f4a_7c15),
            configured_seed.wrapping_add(0x9e37_79b9_7f4a_7c15_u64.wrapping_mul(2)),
            72,
        ]
    };
    let runs = run_seeds.len();
    println!(
        "scale-up-model seed={configured_seed:#018x} cases={cases} runs={runs}; \
         reproduce with KUBERIC_MODEL_SEED={configured_seed} KUBERIC_MODEL_CASES={cases}"
    );
    let mut coverage = BTreeMap::<&'static str, u64>::new();
    let mut run_fingerprints = BTreeSet::new();
    let mut run_coverages = Vec::new();

    for (run, seed) in run_seeds.into_iter().enumerate() {
        let mut rng = ModelRng(seed);
        let class_offset = rng.index(12);
        let mut run_coverage = BTreeMap::<&'static str, u64>::new();
        let mut run_trace = Vec::new();
        macro_rules! cover {
            ($label:literal) => {{
                *coverage.entry($label).or_default() += 1;
                *run_coverage.entry($label).or_default() += 1;
            }};
        }
        for case in 0..cases {
            let schedule_class = (usize::try_from(case).unwrap() + class_offset) % 12;
            // Participant-outage schedules require an accepted ActiveSecondary.
            // Constrain only that schedule class to an eligible starting topology
            // instead of crediting coverage for an outage that could not occur.
            let accepted = if schedule_class == 3 {
                2 + u32::try_from(rng.index(2)).unwrap()
            } else {
                1 + u32::try_from(rng.index(3)).unwrap()
            };
            let additions = if schedule_class == 10 || rng.index(2) == 0 {
                2
            } else {
                1
            };
            let desired = accepted + additions;
            let write_period = 3 + u64::try_from(rng.index(5)).unwrap();
            let write_offset = u64::try_from(rng.index(write_period as usize)).unwrap();
            let restart_period = 6 + u64::try_from(rng.index(5)).unwrap();
            let stale_period = 9 + u64::try_from(rng.index(5)).unwrap();
            let mut model = scale_up_model::Model::new(accepted, desired);
            let mut cancellation_requested = false;
            let mut cancellation_completed = false;
            let mut cancelled_incarnation = None;
            let mut replay_trace = Vec::new();
            let mut candidate_failure_injected = false;
            let mut participant_restore = None;
            let mut participant_unavailability_injected = false;
            let mut participant_unavailability_observed = false;
            let mut cleanup_variant_injected = false;
            let mut cleanup_conflict_restore = None;
            let mut delayed_authority = false;
            let mut late_member_restore = None;
            let mut late_member_injected = false;

            for step in 0..480_u64 {
                if let Some((restore_at, key, agent)) = participant_restore.take() {
                    if step >= restore_at {
                        model.snapshot.replicas.get_mut(&key).unwrap().agent = agent;
                    } else {
                        participant_restore = Some((restore_at, key, agent));
                    }
                }
                if let Some((restore_at, index, observation)) = cleanup_conflict_restore.take() {
                    if step >= restore_at {
                        model.snapshot.secondary_scale_down_resources[index] = observation;
                    } else {
                        cleanup_conflict_restore = Some((restore_at, index, observation));
                    }
                }
                if let Some((restore_at, key, report)) = late_member_restore.take() {
                    if step >= restore_at {
                        model.restore_report_with_replication_catch_up(&key, report);
                    } else {
                        late_member_restore = Some((restore_at, key, report));
                    }
                }
                if step % write_period == write_offset && scale_up_model_writable(&model) {
                    let lsn = model.acknowledge_write(case + run as u64 * cases, step);
                    assert!(
                        model.durable_source.contains_key(&lsn),
                        "acknowledged write was not durably recorded"
                    );
                    cover!("write-varied-times");
                }
                if step % restart_period == 0 {
                    let ids = model
                        .snapshot
                        .replicas
                        .values()
                        .filter_map(|observation| match &observation.agent {
                            AgentObservation::Report(report) => {
                                Some(report.identity.replica_id.value())
                            }
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    if !ids.is_empty() {
                        let id = ids[rng.index(ids.len())];
                        let previous_session = model.report_mut(id).process_session_id.clone();
                        model.restart(id);
                        assert!(!model.has_pending_build());
                        assert_ne!(model.report_mut(id).process_session_id, previous_session);
                        cover!("restart-drops-volatile-session");
                    }
                }
                if step % stale_period == 0 {
                    assert_stale_report_cannot_advance(&model);
                    cover!("obsolete-session-report");
                }

                if schedule_class == 1
                    && !candidate_failure_injected
                    && model.snapshot.status.provisioning.is_some()
                    && model.snapshot.status.transition.is_none()
                    && let Some(report) =
                        model
                            .snapshot
                            .replicas
                            .values_mut()
                            .find_map(|observation| match &mut observation.agent {
                                AgentObservation::Report(report)
                                    if !model
                                        .snapshot
                                        .status
                                        .topology
                                        .as_ref()
                                        .unwrap()
                                        .configuration
                                        .members
                                        .iter()
                                        .any(|member| member.identity == report.identity) =>
                                {
                                    Some(report)
                                }
                                _ => None,
                            })
                {
                    report.reported_fault = Some(FaultType::Permanent);
                    report.report_sequence += 1;
                    assert_eq!(report.reported_fault, Some(FaultType::Permanent));
                    candidate_failure_injected = true;
                    cover!("candidate-failure-retry");
                }
                if schedule_class == 3
                    && !participant_unavailability_injected
                    && participant_restore.is_none()
                    && model.snapshot.status.provisioning.is_some()
                    && let Some((key, observation)) =
                        model.snapshot.replicas.iter_mut().find(|(_, observation)| {
                            matches!(
                                &observation.agent,
                                AgentObservation::Report(report)
                                    if report.role == ReplicaRole::ActiveSecondary
                                        && model
                                            .snapshot
                                            .status
                                            .topology
                                            .as_ref()
                                            .unwrap()
                                            .configuration
                                            .members
                                            .iter()
                                            .any(|member| member.identity == report.identity)
                            )
                        })
                {
                    let saved = observation.agent.clone();
                    observation.agent = AgentObservation::Unreachable {
                        message: "scheduled participant outage".into(),
                    };
                    participant_restore = Some((step + 1, key.clone(), saved));
                    participant_unavailability_injected = true;
                    assert!(matches!(
                        observation.agent,
                        AgentObservation::Unreachable { .. }
                    ));
                }

                if matches!(schedule_class, 0 | 4 | 8)
                    && !cancellation_requested
                    && model.snapshot.status.transition.is_none()
                    && model.snapshot.status.provisioning.is_some()
                {
                    cancelled_incarnation = model
                        .snapshot
                        .status
                        .provisioning
                        .as_ref()
                        .map(|provisioning| provisioning.pod_uid.to_string());
                    model.snapshot.desired.replicas = model.accepted_count();
                    model.snapshot.desired.generation += 1;
                    cancellation_requested = true;
                    assert_eq!(model.snapshot.desired.replicas, model.accepted_count());
                    cover!("desired-churn-cancel");
                } else if cancellation_requested
                    && !cancellation_completed
                    && model.snapshot.status.scale_up_cleanup.is_none()
                    && model.snapshot.status.scale_up_allocation.is_none()
                    && model.snapshot.status.provisioning.is_none()
                {
                    model.snapshot.desired.replicas = desired;
                    model.snapshot.desired.generation += 1;
                    cancellation_completed = true;
                    assert_eq!(model.snapshot.desired.replicas, desired);
                    cover!("fresh-incarnation-retry");
                }

                if !cleanup_variant_injected
                    && model.snapshot.status.scale_up_cleanup.is_some()
                    && let Some((index, exact)) = model
                        .snapshot
                        .secondary_scale_down_resources
                        .iter_mut()
                        .enumerate()
                        .find(|(_, exact)| {
                            model
                                .snapshot
                                .status
                                .scale_up_cleanup
                                .as_ref()
                                .is_some_and(|cleanup| cleanup.target == exact.target)
                        })
                {
                    match schedule_class {
                        0 => {
                            exact.endpoint =
                                kuberic_runtime::protocol::observation::ExactResourceObservation::NotFound;
                            assert!(matches!(
                                exact.endpoint,
                                kuberic_runtime::protocol::observation::ExactResourceObservation::NotFound
                            ));
                            cover!("cleanup-404");
                        }
                        4 => {
                            exact.endpoint = kuberic_runtime::protocol::observation::ExactResourceObservation::ReplacementPresent {
                                uid: "different-endpoint-uid".into(),
                                resource_version: "replacement-rv".into(),
                            };
                            assert!(matches!(
                                exact.endpoint,
                                kuberic_runtime::protocol::observation::ExactResourceObservation::ReplacementPresent { .. }
                            ));
                            cover!("cleanup-different-uid");
                        }
                        8 => {
                            let saved = exact.clone();
                            exact.endpoint = kuberic_runtime::protocol::observation::ExactResourceObservation::LookupFailed {
                                message: "scheduled cleanup conflict".into(),
                            };
                            cleanup_conflict_restore = Some((step + 1, index, saved));
                            assert!(matches!(
                                exact.endpoint,
                                kuberic_runtime::protocol::observation::ExactResourceObservation::LookupFailed { .. }
                            ));
                            cover!("cleanup-conflict-retry");
                        }
                        _ => {}
                    }
                    cleanup_variant_injected = true;
                }

                if additions == 2
                    && !late_member_injected
                    && model.accepted_count() == accepted + 1
                    && model.snapshot.status.transition.is_none()
                    && let Some(receipt) = model.snapshot.status.last_scale_up.as_deref()
                {
                    let target = receipt.intent.target.clone();
                    let key =
                        ReplicaObservationKey::new(target.replica_id, target.instance_id.clone());
                    if let AgentObservation::Report(report) =
                        &model.snapshot.replicas.get(&key).unwrap().agent
                    {
                        late_member_restore = Some((step + 2, key.clone(), report.clone()));
                        model.snapshot.replicas.get_mut(&key).unwrap().agent =
                            AgentObservation::Absent;
                        late_member_injected = true;
                        assert_eq!(
                            model.snapshot.status.last_scale_up.as_deref(),
                            Some(receipt)
                        );
                        cover!("late-retained-member-sequential-addition");
                    }
                }

                let replay = model.snapshot.clone();
                let plan = model.plan();
                replay_trace.push(format!(
                    "step={step} accepted={} desired={} plan={plan:?}",
                    model.accepted_count(),
                    model.snapshot.desired.replicas
                ));
                let observed_provisioning = model.snapshot.status.provisioning.is_some();
                let observed_cleanup = model.snapshot.status.scale_up_cleanup.is_some();
                let observed_pc_cc = model
                    .snapshot
                    .status
                    .transition
                    .as_ref()
                    .is_some_and(|transition| transition.scale_up.is_some());
                assert_eq!(
                    plan,
                    evaluate(&replay, &scale_up_model::config()),
                    "seed={seed:#x} case={case} step={step}"
                );
                if participant_unavailability_injected
                    && !participant_unavailability_observed
                    && replay.replicas.values().any(|observation| {
                        matches!(observation.agent, AgentObservation::Unreachable { .. })
                    })
                {
                    assert!(
                        replay
                            .status
                            .topology
                            .as_ref()
                            .unwrap()
                            .configuration
                            .members
                            .iter()
                            .any(|member| member.role == ReplicaRole::ActiveSecondary),
                        "participant outage was not exercised against an eligible accepted topology"
                    );
                    participant_unavailability_observed = true;
                    cover!("participant-unavailability");
                }
                let skip_for_delayed_authority = !delayed_authority
                    && model
                        .snapshot
                        .status
                        .transition
                        .as_ref()
                        .is_some_and(|transition| transition.scale_up.is_some())
                    && model.snapshot.status.scale_up_admission_started.is_some();
                if skip_for_delayed_authority {
                    assert_eq!(plan, model.plan());
                    delayed_authority = true;
                    cover!("delayed-authority-delivery");
                    continue;
                }
                if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| model.step())).is_err()
                {
                    panic!(
                        "model replay failed seed={seed:#x} case={case} step={step}; \
                         full replay trace:\n{}",
                        replay_trace.join("\n")
                    );
                }
                if observed_provisioning {
                    cover!("candidate-provisioning");
                }
                if observed_cleanup {
                    cover!("exact-candidate-cleanup");
                }
                if observed_pc_cc {
                    cover!("pc-cc-authority");
                }
                if model.accepted_count() == desired
                    && model.snapshot.status.transition.is_none()
                    && model.snapshot.status.provisioning.is_none()
                    && model.snapshot.status.scale_up_cleanup.is_none()
                    && matches!(model.plan(), Plan::Stable { .. })
                {
                    break;
                }
                assert!(
                    step < 479,
                    "unclassified model state seed={seed:#x} case={case}: {:?}",
                    model.plan()
                );
            }

            assert_eq!(
                model.accepted_history,
                (accepted..=desired).collect::<Vec<_>>(),
                "seed={seed:#x} case={case}"
            );
            if let Some(cancelled) = cancelled_incarnation {
                let accepted_instances = model
                    .snapshot
                    .status
                    .topology
                    .as_ref()
                    .unwrap()
                    .configuration
                    .members
                    .iter()
                    .map(|member| member.identity.instance_id.as_str())
                    .collect::<Vec<_>>();
                assert!(
                    !accepted_instances.contains(&cancelled.as_str()),
                    "cancelled incarnation {cancelled} was admitted"
                );
            }
            model.assert_acknowledged_write_oracle();
            model.assert_safety_invariants();
            if additions == 2 {
                cover!("sequential-additions-late-receipt");
            }
            run_trace.extend(replay_trace);
        }
        let mut hasher = DefaultHasher::new();
        run_trace.hash(&mut hasher);
        let fingerprint = hasher.finish();
        assert!(
            run_fingerprints.insert(fingerprint) || runs == 1,
            "different seeds produced the same replay trace fingerprint {fingerprint:#x}"
        );
        println!(
            "scale-up-model run-seed={seed:#018x} trace={fingerprint:#018x} \
             event-coverage={run_coverage:?}"
        );
        run_coverages.push(run_coverage);
    }
    if runs > 1 {
        assert!(
            run_coverages.windows(2).any(|pair| pair[0] != pair[1]),
            "different seeds produced identical behavioral coverage counts: {run_coverages:?}"
        );
    }
    for required in [
        "write-varied-times",
        "restart-drops-volatile-session",
        "obsolete-session-report",
        "desired-churn-cancel",
        "fresh-incarnation-retry",
        "candidate-provisioning",
        "exact-candidate-cleanup",
        "pc-cc-authority",
        "sequential-additions-late-receipt",
        "candidate-failure-retry",
        "participant-unavailability",
        "cleanup-404",
        "cleanup-different-uid",
        "cleanup-conflict-retry",
        "delayed-authority-delivery",
        "late-retained-member-sequential-addition",
    ] {
        assert!(
            coverage.get(required).copied().unwrap_or_default() > 0,
            "recorded/default corpus missed event class {required}"
        );
    }
    println!("scale-up-model event-coverage={coverage:?}");
}

fn fail_model_primary(model: &mut scale_up_model::Model) -> (ReplicaIdentity, Box<AgentReport>) {
    let primary = model
        .snapshot
        .status
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .unwrap()
        .identity
        .clone();
    let observation = model
        .snapshot
        .replicas
        .get_mut(&ReplicaObservationKey::new(
            primary.replica_id,
            primary.instance_id.clone(),
        ))
        .unwrap();
    let AgentObservation::Report(report) = &observation.agent else {
        panic!("accepted primary report")
    };
    let report = report.clone();
    observation.agent = AgentObservation::Absent;
    model.snapshot.now_unix_seconds += 20;
    (primary, report)
}

fn drive_exact_scale_up_pc_cc(model: &mut scale_up_model::Model) {
    for step in 0..240 {
        let installed = model
            .snapshot
            .status
            .transition
            .as_ref()
            .and_then(|transition| transition.scale_up.as_deref())
            .is_some_and(|intent| {
                model.snapshot.status.scale_up_admission_started.as_ref()
                    == Some(&intent.operation_id)
                    && intent.current_configuration.members.iter().all(|member| {
                        model.snapshot.replicas.values().any(|observation| {
                            matches!(
                                &observation.agent,
                                AgentObservation::Report(report)
                                    if report.identity == member.identity
                                        && report.previous_configuration.as_ref()
                                            == Some(&intent.previous_configuration)
                                        && report.current_configuration.as_ref()
                                            == Some(&intent.current_configuration)
                                        && report.retained_operation_id.as_ref()
                                            == Some(&intent.command_operation_id(
                                                ScaleUpStage::PreviousCurrent,
                                                &member.identity,
                                                &intent.current_configuration,
                                            ))
                            )
                        })
                    })
            });
        if installed {
            return;
        }
        model.step();
        assert!(step < 239, "exact PC/CC installation was not reached");
    }
}

#[test]
fn acknowledged_write_requires_independent_durable_pc_and_cc_quorums() {
    let mut missing_previous = scale_up_model::Model::new(2, 3);
    drive_exact_scale_up_pc_cc(&mut missing_previous);
    let intent = missing_previous
        .snapshot
        .status
        .transition
        .as_ref()
        .unwrap()
        .scale_up
        .as_deref()
        .unwrap()
        .clone();
    let dropped_previous = intent
        .previous_configuration
        .members
        .iter()
        .find(|member| member.identity != intent.primary)
        .unwrap()
        .identity
        .clone();
    let error = missing_previous
        .try_acknowledge_write(1, 1, &BTreeSet::from([dropped_previous]))
        .unwrap_err();
    assert_eq!(
        error,
        "previous write quorum missing: delivered=1, required=2"
    );

    let mut missing_current = scale_up_model::Model::new(3, 4);
    drive_exact_scale_up_pc_cc(&mut missing_current);
    let intent = missing_current
        .snapshot
        .status
        .transition
        .as_ref()
        .unwrap()
        .scale_up
        .as_deref()
        .unwrap()
        .clone();
    let dropped_retained = intent
        .previous_configuration
        .members
        .iter()
        .find(|member| member.identity != intent.primary)
        .unwrap()
        .identity
        .clone();
    let error = missing_current
        .try_acknowledge_write(
            2,
            1,
            &BTreeSet::from([intent.target.clone(), dropped_retained]),
        )
        .unwrap_err();
    assert_eq!(
        error,
        "current write quorum missing: delivered=2, required=3"
    );
}

#[test]
fn configuration_requires_contiguous_byte_exact_incarnation_history() {
    fn candidate_configuration_cut()
    -> (scale_up_model::Model, ProtocolCommand, ReplicaIdentity, i64) {
        let mut model = scale_up_model::Model::new(2, 3);
        loop {
            match model.plan() {
                Plan::Execute {
                    command: ProtocolCommand::EnsureConfiguration(command),
                } if command
                    .scale_up_evidence
                    .as_deref()
                    .is_some_and(|evidence| {
                        command.local_replica_id == evidence.intent().target.replica_id
                    }) =>
                {
                    let intent = command.scale_up_evidence.as_deref().unwrap().intent();
                    let target = intent.target.clone();
                    let boundary = intent.catch_up_boundary_lsn;
                    return (
                        model,
                        ProtocolCommand::EnsureConfiguration(command),
                        target,
                        boundary,
                    );
                }
                _ => {
                    model.step();
                }
            }
        }
    }

    let (mut valid, command, target, boundary) = candidate_configuration_cut();
    let exact_progress = valid
        .validate_exact_durable_history(&target, boundary)
        .expect("valid copied history");
    assert!(exact_progress >= boundary);
    valid.execute(command);
    assert_eq!(
        valid.report_mut(target.replica_id.value()).current_progress,
        exact_progress
    );

    let (mut interior_gap, _, target, boundary) = candidate_configuration_cut();
    let missing = boundary - 1;
    interior_gap.remove_durable_value(&target, missing);
    assert_eq!(
        interior_gap
            .validate_exact_durable_history(&target, boundary)
            .unwrap_err(),
        format!("exact durable history for {target:?} has an interior gap at LSN {missing}")
    );

    let (mut corruption, _, target, boundary) = candidate_configuration_cut();
    let corrupt_lsn = boundary - 1;
    corruption.replace_durable_value(&target, corrupt_lsn, "corrupt-payload");
    let error = corruption
        .validate_exact_durable_history(&target, boundary)
        .unwrap_err();
    assert!(
        error.contains(&format!("payload corruption at LSN {corrupt_lsn}"))
            && error.contains("actual=\"corrupt-payload\""),
        "wrong payload assertion: {error}"
    );

    let (mut final_gap, command, target, boundary) = candidate_configuration_cut();
    final_gap.remove_durable_value(&target, boundary);
    assert_eq!(
        final_gap
            .validate_exact_durable_history(&target, boundary)
            .unwrap_err(),
        format!("exact durable history for {target:?} has an interior gap at LSN {boundary}")
    );
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        final_gap.execute(command);
    }))
    .expect_err("authority installation accepted a missing final boundary");
    let panic = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("<non-string panic>");
    assert!(
        panic.contains("configuration requires exact complete durable history")
            && panic.contains(&format!("interior gap at LSN {boundary}")),
        "configuration failed at the wrong assertion: {panic}"
    );

    let mut terminal = scale_up_model::Model::new(3, 3);
    let lsn = terminal.acknowledge_write(7, 1);
    let identities = terminal.accepted_identities();
    terminal.remove_durable_value(&identities[0], lsn);
    terminal.remove_durable_value(&identities[1], lsn);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            terminal.assert_acknowledged_write_oracle();
        }))
        .is_err(),
        "terminal read masks ignored missing per-incarnation durable bytes"
    );
}

#[test]
fn insufficient_scale_up_failover_witness_masks_block_precisely() {
    struct Scenario {
        accepted: u32,
        desired: u32,
        missing: &'static str,
        reason: &'static str,
        expected_fragment: &'static str,
    }
    for scenario in [
        Scenario {
            accepted: 3,
            desired: 4,
            missing: "previous",
            reason: "ScaleUpPreviousQuorumEvidencePending",
            expected_fragment: "missing previous configuration read quorum: observed=1 required=2; current configuration satisfied: observed=2 required=2",
        },
        Scenario {
            accepted: 2,
            desired: 3,
            missing: "current",
            reason: "ScaleUpCurrentQuorumEvidencePending",
            expected_fragment: "previous configuration satisfied: observed=1 required=1; missing current configuration read quorum: observed=1 required=2",
        },
    ] {
        let mut model = scale_up_model::Model::new(scenario.accepted, scenario.desired);
        drive_exact_scale_up_pc_cc(&mut model);
        let intent = model
            .snapshot
            .status
            .transition
            .as_ref()
            .unwrap()
            .scale_up
            .as_deref()
            .unwrap()
            .clone();
        let (failed_primary, _) = fail_model_primary(&mut model);
        if scenario.missing == "previous" {
            let unavailable = intent
                .previous_configuration
                .members
                .iter()
                .filter(|member| member.identity != failed_primary)
                .nth(1)
                .unwrap()
                .identity
                .clone();
            model
                .snapshot
                .replicas
                .get_mut(&ReplicaObservationKey::new(
                    unavailable.replica_id,
                    unavailable.instance_id.clone(),
                ))
                .unwrap()
                .agent = AgentObservation::Absent;
        } else {
            model
                .snapshot
                .replicas
                .get_mut(&ReplicaObservationKey::new(
                    intent.target.replica_id,
                    intent.target.instance_id.clone(),
                ))
                .unwrap()
                .agent = AgentObservation::Absent;
        }
        let plan = model.plan();
        let Plan::Wait { reason, status, .. } = plan else {
            panic!(
                "{} witness loss was not safely blocked: {plan:?}",
                scenario.missing
            );
        };
        assert_eq!(
            reason,
            kuberic_controller::plan::WaitReason::ActiveTransition
        );
        let condition = status
            .conditions
            .iter()
            .find(|condition| condition.type_ == "Progressing")
            .unwrap();
        assert_eq!(condition.reason, scenario.reason);
        assert!(
            condition.message.contains("phase=failover-recovery")
                && condition.message.contains(scenario.expected_fragment),
            "{} deficit was misclassified: {}",
            scenario.missing,
            condition.message
        );
    }
}

#[test]
fn insufficient_fresh_fenced_pc_cc_masks_wait_with_classified_diagnostics() {
    struct Scenario {
        accepted: u32,
        desired: u32,
        unavailable: &'static [i64],
        reason: &'static str,
        expected_fragment: &'static str,
    }
    for scenario in [
        Scenario {
            accepted: 3,
            desired: 4,
            unavailable: &[3],
            reason: "ScaleUpFencedPreviousQuorumPending",
            expected_fragment: "fresh fenced previous configuration read quorum is insufficient: observed=1 required=2; fresh current configuration quorum is satisfied: observed=2 required=2",
        },
        Scenario {
            accepted: 2,
            desired: 3,
            unavailable: &[3],
            reason: "ScaleUpFencedCurrentQuorumPending",
            expected_fragment: "fresh fenced previous configuration quorum is satisfied: observed=1 required=1; fresh current configuration read quorum is insufficient: observed=1 required=2",
        },
        Scenario {
            accepted: 4,
            desired: 5,
            unavailable: &[3, 4, 5],
            reason: "ScaleUpFencedDualQuorumPending",
            expected_fragment: "fresh fenced previous configuration read quorum is insufficient: observed=1 required=2; fresh current configuration read quorum is insufficient: observed=1 required=3",
        },
    ] {
        let mut model = scale_up_model::Model::new(scenario.accepted, scenario.desired);
        drive_exact_scale_up_pc_cc(&mut model);
        fail_model_primary(&mut model);
        let Plan::Apply { changes } = model.plan() else {
            panic!("primary failure did not persist provisional failover")
        };
        for change in changes {
            model.apply(change);
        }
        assert!(
            model
                .snapshot
                .status
                .transition
                .as_ref()
                .is_some_and(|transition| transition.election_lsn.is_none())
        );
        for replica_id in scenario.unavailable {
            let key = model
                .snapshot
                .replicas
                .iter()
                .find_map(|(key, observation)| match &observation.agent {
                    AgentObservation::Report(report)
                        if report.identity.replica_id == ReplicaId::new(*replica_id) =>
                    {
                        Some(key.clone())
                    }
                    _ => None,
                })
                .unwrap();
            model.snapshot.replicas.get_mut(&key).unwrap().agent = AgentObservation::Absent;
        }

        let blocked = loop {
            match model.plan() {
                Plan::Execute { command } => model.execute(command),
                Plan::Apply { changes } => {
                    for change in changes {
                        model.apply(change);
                    }
                }
                Plan::Wait { status, .. }
                    if status.conditions.iter().any(|condition| {
                        condition.type_ == "Progressing" && condition.reason == scenario.reason
                    }) =>
                {
                    break status;
                }
                Plan::Wait { status, .. } => model.apply_wait(status),
                other => panic!("fresh fenced mask was misclassified: {other:?}"),
            }
        };
        let condition = blocked
            .conditions
            .iter()
            .find(|condition| condition.type_ == "Progressing")
            .unwrap();
        assert_eq!(condition.reason, scenario.reason);
        assert!(
            condition.message.contains("phase=failover-election")
                && condition.message.contains(scenario.expected_fragment),
            "fresh fenced deficit was misclassified: {}",
            condition.message
        );
    }
}

#[test]
fn model_invariant_mutations_reject_epoch_incarnation_and_cleanup_provenance() {
    let epoch = scale_up_model::Model::new(2, 2);
    let accepted_epoch = epoch
        .snapshot
        .status
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .epoch;
    epoch
        .validate_accepted_epoch(accepted_epoch)
        .expect("valid epoch baseline");
    assert_eq!(
        epoch
            .validate_accepted_epoch(Epoch::new(
                accepted_epoch.data_loss_number,
                accepted_epoch.configuration_number - 1,
            ))
            .unwrap_err(),
        format!(
            "accepted-state persistence rolled epoch back from {accepted_epoch:?} to {:?}",
            Epoch::new(
                accepted_epoch.data_loss_number,
                accepted_epoch.configuration_number - 1
            )
        )
    );
    validate_status(&epoch.snapshot.status).expect("epoch mutation control baseline stays valid");

    let policy = scale_up_model::Model::new(3, 3);
    policy
        .validate_accepted_policy(3)
        .expect("valid policy baseline");
    assert_eq!(
        policy.validate_accepted_policy(2).unwrap_err(),
        "accepted-state persistence rolled policy back from 3 to 2"
    );
    validate_status(&policy.snapshot.status).expect("policy mutation control baseline stays valid");

    let mut cleanup = scale_up_model::Model::new(1, 2);
    let delete = loop {
        if cleanup.snapshot.status.provisioning.is_some()
            && cleanup.snapshot.status.transition.is_none()
        {
            cleanup.snapshot.desired.replicas = 1;
            cleanup.snapshot.desired.generation += 1;
        }
        match cleanup.plan() {
            Plan::Apply { changes } => {
                if let Some(delete) = changes.iter().find(|change| {
                    matches!(
                        change,
                        KubernetesChange::DeleteScaleDownResource {
                            resource: ScaleDownResource::Pod | ScaleDownResource::Pvc,
                            ..
                        }
                    )
                }) {
                    break delete.clone();
                }
                for change in changes {
                    cleanup.apply(change);
                }
            }
            _ => {
                cleanup.step();
            }
        }
    };
    cleanup.validate_deletion_change(&delete).unwrap();
    let KubernetesChange::DeleteScaleDownResource {
        resource,
        uid,
        resource_version,
        ..
    } = &delete
    else {
        unreachable!()
    };
    let target = cleanup
        .snapshot
        .status
        .scale_up_cleanup
        .as_ref()
        .unwrap()
        .target
        .clone();
    let name = match &delete {
        KubernetesChange::DeleteScaleDownResource { name, .. } => name.clone(),
        _ => unreachable!(),
    };
    for mutation in [
        KubernetesChange::DeleteScaleDownResource {
            resource: *resource,
            name: "wrong-name".into(),
            uid: uid.clone(),
            resource_version: resource_version.clone(),
        },
        KubernetesChange::DeleteScaleDownResource {
            resource: *resource,
            name: match &delete {
                KubernetesChange::DeleteScaleDownResource { name, .. } => name.clone(),
                _ => unreachable!(),
            },
            uid: "wrong-incarnation".into(),
            resource_version: resource_version.clone(),
        },
        KubernetesChange::DeleteScaleDownResource {
            resource: *resource,
            name: match &delete {
                KubernetesChange::DeleteScaleDownResource { name, .. } => name.clone(),
                _ => unreachable!(),
            },
            uid: uid.clone(),
            resource_version: "wrong-resource-version".into(),
        },
    ] {
        let KubernetesChange::DeleteScaleDownResource {
            name: actual_name,
            uid: actual_uid,
            resource_version: actual_resource_version,
            ..
        } = &mutation
        else {
            unreachable!()
        };
        assert_eq!(
            cleanup.validate_deletion_change(&mutation).unwrap_err(),
            format!(
                "deletion identity/version mismatch for {target:?}: expected \
                 {name}/{uid}@{resource_version}, got \
                 {actual_name}/{actual_uid}@{actual_resource_version}"
            )
        );
    }
    cleanup
        .snapshot
        .status
        .scale_up_cleanup
        .as_mut()
        .unwrap()
        .provisioning
        .operation_id = OperationId::new("wrong-allocation-provenance");
    let provenance_error = cleanup.validate_deletion_change(&delete).unwrap_err();
    assert!(
        provenance_error.starts_with(
            "deletion allocation provenance mismatch: expected \
             wrong-allocation-provenance"
        ),
        "wrong invariant rejected provenance mutation: {provenance_error}"
    );

    let mut duplicate = scale_up_model::Model::new(1, 2);
    while duplicate.snapshot.status.provisioning.is_none() {
        duplicate.step();
    }
    duplicate
        .validate_exact_candidate_set()
        .expect("valid exact candidate baseline");
    let provisioning = duplicate.snapshot.status.provisioning.clone().unwrap();
    let mut allocation = duplicate
        .snapshot
        .status
        .scale_up_allocation
        .clone()
        .unwrap_or_else(|| {
            let scale_up = provisioning.scale_up().unwrap();
            let mut allocation = kuberic_runtime::protocol::types::ScaleUpAllocation {
                resource_uid: scale_up.resource_uid.clone(),
                spec_generation: scale_up.spec_generation,
                desired_replicas: scale_up.desired_replicas,
                previous_configuration_id: scale_up.previous_configuration.configuration_id.clone(),
                accepted_configuration_id: scale_up.previous_configuration.configuration_id.clone(),
                target_replica_id: scale_up.target_replica_id,
                operation_id: OperationId::default(),
                previous_operation_id: None,
                scaffolding_requested: true,
                pod_uid: Some(PodUid::new("duplicate-incarnation")),
                pvc_uid: Some(PvcUid::new("duplicate-pvc")),
                cancellation_started: false,
            };
            allocation.operation_id = allocation.expected_operation_id();
            allocation
        });
    allocation.pod_uid = Some(PodUid::new("duplicate-incarnation"));
    duplicate.snapshot.status.scale_up_allocation = Some(allocation);
    let duplicate_error = duplicate.validate_exact_candidate_set().unwrap_err();
    assert!(
        duplicate_error.starts_with("more than one exact scale-up candidate identity is active:"),
        "wrong invariant rejected duplicate exact identities: {duplicate_error}"
    );
}

#[test]
fn generated_scale_up_primary_failure_traces_classify_pre_and_post_admission_recovery() {
    for fail_after_pc_cc in [false, true] {
        let mut model = scale_up_model::Model::new(2, 3);
        for step in 0..160 {
            if scale_up_model_writable(&model) && step % 4 == 0 {
                model.acknowledge_write(u64::from(fail_after_pc_cc), step);
            }
            let reached_cut = if fail_after_pc_cc {
                model
                    .snapshot
                    .status
                    .transition
                    .as_ref()
                    .and_then(|transition| transition.scale_up.as_deref())
                    .is_some_and(|intent| {
                        model.snapshot.status.scale_up_admission_started.as_ref()
                            == Some(&intent.operation_id)
                            && intent.current_configuration.members.iter().all(|member| {
                                model.snapshot.replicas.values().any(|observation| {
                                    matches!(
                                        &observation.agent,
                                        AgentObservation::Report(report)
                                            if report.identity == member.identity
                                                && report.previous_configuration.as_ref()
                                                    == Some(&intent.previous_configuration)
                                                && report.current_configuration.as_ref()
                                                    == Some(&intent.current_configuration)
                                                && report.retained_operation_id.as_ref()
                                                    == Some(&intent.command_operation_id(
                                                        ScaleUpStage::PreviousCurrent,
                                                        &member.identity,
                                                        &intent.current_configuration,
                                                    ))
                                    )
                                })
                            })
                    })
            } else {
                model.snapshot.status.provisioning.is_some()
                    && model.snapshot.status.transition.is_none()
            };
            if reached_cut {
                break;
            }
            model.step();
            assert!(step < 159, "scale-up failure cut was not reached");
        }
        let (failed_primary, mut returning_primary) = fail_model_primary(&mut model);
        if !fail_after_pc_cc {
            // Explicit desired churn cancels the unadmitted attempt while
            // ordinary failover preserves the accepted two-member authority.
            // Once exact cleanup completes, the original request is restored
            // and must allocate a fresh incarnation.
            model.snapshot.desired.replicas = model.accepted_count();
            model.snapshot.desired.generation += 1;
        }
        let failed_attempt = model
            .snapshot
            .status
            .transition
            .as_ref()
            .and_then(|transition| transition.scale_up.as_deref())
            .map(|intent| intent.operation_id.clone())
            .or_else(|| {
                model
                    .snapshot
                    .status
                    .provisioning
                    .as_ref()
                    .map(|provisioning| provisioning.operation_id.clone())
            })
            .unwrap();
        let mut observed_cleanup = false;
        let mut observed_failover = false;
        let mut primary_returned = false;
        let mut retry_requested = fail_after_pc_cc;
        let mut converged = false;
        for _step in 0..320 {
            model.snapshot.now_unix_seconds += 1;
            observed_cleanup |= model.snapshot.status.scale_up_cleanup.is_some();
            observed_failover |= model
                .snapshot
                .status
                .topology
                .as_ref()
                .is_some_and(|topology| {
                    topology.configuration.primary_id != failed_primary.replica_id
                })
                || model
                    .snapshot
                    .status
                    .transition
                    .as_ref()
                    .is_some_and(|transition| transition.kind == TransitionKind::Failover);
            if observed_failover && !primary_returned && !fail_after_pc_cc {
                returning_primary.process_session_id =
                    ProcessSessionId::new("failed-primary-restarted");
                returning_primary.report_sequence = 1;
                model
                    .snapshot
                    .replicas
                    .get_mut(&ReplicaObservationKey::new(
                        failed_primary.replica_id,
                        failed_primary.instance_id.clone(),
                    ))
                    .unwrap()
                    .agent = AgentObservation::Report(returning_primary.clone());
                primary_returned = true;
            }
            if !fail_after_pc_cc
                && observed_cleanup
                && !retry_requested
                && model.snapshot.status.transition.is_none()
                && model.snapshot.status.scale_up_cleanup.is_none()
                && model.snapshot.status.provisioning.is_none()
            {
                model.snapshot.desired.replicas = 3;
                model.snapshot.desired.generation += 1;
                retry_requested = true;
            }
            if fail_after_pc_cc
                && !primary_returned
                && model.accepted_count() == 3
                && model.snapshot.status.transition.is_none()
            {
                returning_primary.process_session_id =
                    ProcessSessionId::new("failed-primary-late-return");
                returning_primary.report_sequence = 1;
                model
                    .snapshot
                    .replicas
                    .get_mut(&ReplicaObservationKey::new(
                        failed_primary.replica_id,
                        failed_primary.instance_id.clone(),
                    ))
                    .unwrap()
                    .agent = AgentObservation::Report(returning_primary.clone());
                primary_returned = true;
            }
            model.step();
            if model.accepted_count() == 3
                && model.snapshot.status.transition.is_none()
                && model.snapshot.status.scale_up_cleanup.is_none()
                && matches!(model.plan(), Plan::Stable { .. })
            {
                converged = true;
                break;
            }
        }
        assert!(observed_failover);
        assert!(
            converged,
            "{} recoverable primary-failure trace did not converge: {:?}",
            if fail_after_pc_cc {
                "post-PC/CC"
            } else {
                "pre-PC/CC"
            },
            (
                model.plan(),
                model
                    .snapshot
                    .replicas
                    .values()
                    .filter_map(|observation| match &observation.agent {
                        AgentObservation::Report(report) => Some((
                            report.identity.clone(),
                            report.role,
                            report.epoch,
                            report.previous_configuration.clone(),
                            report.current_configuration.clone(),
                            report.deactivation_epoch,
                            report.deactivated_lsn,
                            report.write_status,
                        )),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            )
        );
        if fail_after_pc_cc {
            let receipt = model.snapshot.status.last_scale_up.as_ref().unwrap();
            assert_eq!(receipt.intent.operation_id, failed_attempt);
            assert!(!observed_cleanup);
        } else {
            assert!(observed_cleanup);
            let receipt = model.snapshot.status.last_scale_up.as_ref().unwrap();
            assert_ne!(receipt.intent.operation_id, failed_attempt);
        }
        model.assert_acknowledged_write_oracle();
        model.assert_safety_invariants();
    }
}

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
            secondary_scale_down: None,
            secondary_removal_evidence: None,
            scale_up: None,
            scale_up_failover: None,
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
                purpose: ProvisioningPurpose::replacement(replaced),
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
                            protocol_version: kuberic_runtime::protocol::PROTOCOL_VERSION,
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
                secondary_scale_down_resources: Vec::new(),
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
                        protocol_version: kuberic_runtime::protocol::PROTOCOL_VERSION,
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
                secondary_scale_down_resources: Vec::new(),
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
#[allow(dead_code)]
#[path = "protocol_support/scale_down_model.rs"]
mod scale_down_model;

#[path = "protocol_support/reduction_traces.rs"]
mod reduction_traces;

#[test]
fn scale_down_model_lost_replies_at_intent_pending_and_effect_boundaries() {
    use kuberic_controller::plan::Plan;
    use kuberic_runtime::protocol::command::ProtocolCommand;
    use kuberic_runtime::protocol::types::TransitionKind;
    use scale_down_model::{CommandBoundary, Model};
    let mut trace = Model::new(&[1, 2, 3], 1, 2);
    let mut interrupted = 0;
    for _ in 0..80 {
        let plan = trace.plan();
        if matches!(&plan, Plan::Execute { command } if matches!(command,
            ProtocolCommand::EnsureConfiguration(c) if c.transition_kind == TransitionKind::SecondaryScaleDown)
            || matches!(command, ProtocolCommand::RetireReplica(_)))
        {
            for boundary in [
                CommandBoundary::Intent,
                CommandBoundary::Pending,
                CommandBoundary::Effect,
            ] {
                let mut replay = trace.clone();
                replay.interrupt(plan.clone(), boundary);
                replay.controller_restart();
                replay.finish();
                assert!(replay.inflight.is_empty(), "{boundary:?}");
                assert!(replay.applied_effects.is_empty(), "{boundary:?}");
                assert_eq!(replay.removed.len(), 1);
                assert_eq!(replay.deletes.len(), 3);
                let deletes = replay.deletes.clone();
                replay.controller_restart();
                replay.finish();
                assert_eq!(replay.deletes, deletes);
            }
            interrupted += 1;
        }
        trace.apply(plan.clone());
        if matches!(plan, Plan::Stable { .. }) {
            break;
        }
    }
    assert_eq!(
        interrupted, 5,
        "two PC/CC, two current-only, and retirement"
    );
}

#[test]
fn scale_down_model_desired_mutations_and_ambiguous_replies_at_every_boundary() {
    use kuberic_runtime::protocol::types::TransitionKind;
    use scale_down_model::Model;
    let mut trace = Model::new(&[1, 2, 3], 1, 2);
    trace.step();
    for boundary in 0..60 {
        let intent = trace
            .snapshot
            .status
            .transition
            .as_ref()
            .and_then(|t| t.secondary_scale_down.clone())
            .or_else(|| {
                trace
                    .snapshot
                    .status
                    .secondary_scale_down_cleanup
                    .as_ref()
                    .map(|c| c.evidence.preparation.intent.clone())
            });
        let Some(intent) = intent else { break };
        for desired in [1, 2, 3, 5] {
            let mut changed = trace.clone();
            changed.snapshot.desired.generation = 100 + boundary;
            changed.snapshot.desired.replicas = desired;
            let original_plan = changed.plan();
            assert_eq!(
                original_plan,
                changed.plan(),
                "lost status/command reply is not progress"
            );
            changed.until(|m| {
                m.snapshot.status.transition.is_none()
                    && m.snapshot.status.secondary_scale_down_cleanup.is_none()
            });
            assert_eq!(changed.removed.first(), Some(&intent.target));
            assert_eq!(changed.removed.len(), 1);
            assert!(
                changed.snapshot.status.observed_generation < changed.snapshot.desired.generation
            );
            changed.finish();
            assert_eq!(
                changed
                    .snapshot
                    .status
                    .topology
                    .as_ref()
                    .unwrap()
                    .configuration
                    .members
                    .len(),
                desired.min(2) as usize
            );
            assert!(changed.commands.iter().all(|c| match c {
                kuberic_runtime::protocol::command::ProtocolCommand::EnsureConfiguration(c) =>
                    c.transition_kind == TransitionKind::SecondaryScaleDown
                        || c.previous_configuration.is_none(),
                _ => true,
            }));
        }
        trace.step();
    }
    trace.finish();
}

#[test]
fn scale_down_model_restarts_and_unsupported_edits_at_every_durable_boundary() {
    use kuberic_runtime::protocol::observation::AgentObservation;
    use kuberic_runtime::protocol::types::ProcessSessionId;
    use scale_down_model::Model;
    let mut trace = Model::new(&[1, 2, 3], 1, 2);
    trace.step();
    for boundary in 0..60 {
        if trace.snapshot.status.transition.is_none()
            && trace.snapshot.status.secondary_scale_down_cleanup.is_none()
        {
            break;
        }
        let mut restarted = trace.clone();
        for (key, observation) in &mut restarted.snapshot.replicas {
            if let AgentObservation::Report(r) = &mut observation.agent {
                r.process_session_id =
                    ProcessSessionId::new(format!("restart-{boundary}-{}", key.replica_id));
                r.report_sequence = 1;
            }
        }
        restarted.finish();
        assert_eq!(restarted.removed.len(), 1);
        let mut changed = trace.clone();
        changed.snapshot.desired.generation = 100 + boundary;
        changed.snapshot.desired.image = "unsupported:v2".into();
        changed.snapshot.desired.failover_delay_seconds = 999;
        changed.until(|m| {
            m.snapshot.status.transition.is_none()
                && m.snapshot.status.secondary_scale_down_cleanup.is_none()
        });
        assert_eq!(changed.removed.len(), 1);
        assert!(
            changed
                .snapshot
                .status
                .conditions
                .iter()
                .any(|c| c.reason == "SpecDriftUnsupported")
        );
        assert!(changed.snapshot.status.observed_generation < changed.snapshot.desired.generation);
        trace.step();
    }
}

#[test]
fn scale_down_model_enumerates_quorum_availability_without_target_credit() {
    use kuberic_controller::plan::Plan;
    use kuberic_runtime::protocol::types::AccessStatus;
    use scale_down_model::{Model, reason};
    for size in 2..=5 {
        let ids = (1..=size).collect::<Vec<_>>();
        for mask in 0..(1 << (size - 1)) {
            let mut model = Model::new(&ids, 1, size as u32 - 1);
            let saved = model.snapshot.replicas.clone();
            let routing = model.snapshot.routing.clone();
            if mask & (1 << (size - 2)) == 0 {
                model.unavailable(size);
            }
            let mut retained = 1;
            for id in 2..size {
                if mask & (1 << (id - 2)) == 0 {
                    model.unavailable(id);
                } else {
                    retained += 1;
                }
            }
            let policy = model.snapshot.status.effective_policy.clone().unwrap();
            let reduced =
                kuberic_runtime::protocol::types::EffectivePolicy::fixed(size as u32 - 1, 30)
                    .unwrap();
            let sufficient = retained >= policy.read_quorum && retained >= reduced.write_quorum;
            for _ in 0..100 {
                if matches!(model.step(), Plan::Wait { .. } | Plan::Stable { .. }) {
                    break;
                }
            }
            assert_eq!(
                !model.removed.is_empty(),
                sufficient,
                "size={size} mask={mask}"
            );
            if !sufficient {
                let waiting = model.plan();
                assert_eq!(reason(&waiting), "ScaleDownRetainedReadQuorumUnavailable");
                for _ in 0..3 {
                    model.controller_restart();
                    assert_eq!(
                        model.step(),
                        waiting,
                        "restart/lost reply keeps the same wait"
                    );
                    assert_eq!(model.snapshot.routing, routing);
                    assert_eq!(model.report(1).write_status, AccessStatus::Granted);
                    assert!(model.report(1).prepared_secondary_removal.is_none());
                    assert!(model.snapshot.status.transition.is_none());
                    assert!(model.snapshot.status.provisioning.is_none());
                    assert!(model.commands.is_empty() && model.deletes.is_empty());
                }
                for (key, observation) in saved {
                    if key.replica_id.value() != size
                        && matches!(
                            model.snapshot.replicas[&key].agent,
                            kuberic_runtime::protocol::observation::AgentObservation::Unreachable { .. }
                        )
                    {
                        model.snapshot.replicas.insert(key, observation);
                    }
                }
                model.finish();
                assert_eq!(model.removed.len(), 1);
            }
            assert_eq!(model.removed[0].replica_id.value(), size);
        }
    }
}
