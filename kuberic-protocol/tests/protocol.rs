use std::collections::BTreeMap;

use scale_down_model::fixture as scale_down_fixture;

#[allow(dead_code)]
#[path = "support/scale_down_model.rs"]
mod scale_down_model;

#[allow(dead_code)]
#[path = "support/scale_up_model.rs"]
mod scale_up_model;

#[test]
fn scale_down_commit_boundary_rejects_generated_primary_evidence_corruption() {
    use scale_down_model::Model;
    for size in 2..=5 {
        let mut baseline = Model::new(&(1..=size).collect::<Vec<_>>(), 1, size as u32 - 1);
        baseline.until(|m| {
            matches!(m.plan(), Plan::Apply { ref changes }
            if matches!(changes.first(), Some(KubernetesChange::PersistStatus { status })
                if status.secondary_scale_down_cleanup.is_some()))
        });
        let authority = baseline.snapshot.status.clone();
        for mutation in 0..8 {
            let mut broken = baseline.clone();
            let primary = broken.report(1);
            match mutation {
                0 => primary.verified_replication_lsn = None,
                1 => primary.verified_replication_lsn = Some(9),
                2 => primary.pending_operation_id = Some(OperationId::new("unrelated-inflight")),
                3 => primary.retained_operation_id = Some(OperationId::new("other-completion")),
                4 => primary.report_sequence = 0,
                5 => primary.write_status = AccessStatus::Granted,
                6 => primary.identity.agent_generation = AgentGeneration::new("substitute"),
                _ => primary.epoch.data_loss_number += 1,
            }
            let plan = broken.plan();
            assert!(
                matches!(plan, Plan::Wait { .. } | Plan::Unsafe { .. }),
                "size={size} mutation={mutation}: {plan:?}"
            );
            assert_eq!(broken.snapshot.status, authority);
            assert!(broken.removed.is_empty() && broken.deletes.is_empty());
        }
        baseline.finish();
        assert_eq!(baseline.removed.len(), 1);
    }
}

#[test]
fn scale_down_exact_lookup_failures_after_commit_preserve_cleanup_authority() {
    use kuberic_protocol::command::ScaleDownResource;
    use kuberic_protocol::observation::ExactResourceObservation;
    use scale_down_model::Model;
    for resource in [
        ScaleDownResource::Endpoint,
        ScaleDownResource::Pod,
        ScaleDownResource::Pvc,
    ] {
        let mut failed = Model::new(&[1, 2, 3], 1, 2);
        failed.until(|m| matches!(m.plan(), Plan::Apply { changes }
            if matches!(changes.first(), Some(KubernetesChange::DeleteScaleDownResource { resource: kind, .. }) if *kind == resource)));
        let before = failed.snapshot.status.clone();
        let deletes = failed.deletes.clone();
        let exact = failed.exact(3);
        *match resource {
            ScaleDownResource::Pod => &mut exact.pod,
            ScaleDownResource::Pvc => &mut exact.pvc,
            ScaleDownResource::Endpoint => &mut exact.endpoint,
        } = ExactResourceObservation::LookupFailed {
            message: "authoritative GET failed".into(),
        };
        for _ in 0..3 {
            failed.controller_restart();
            let plan = failed.plan();
            assert!(
                matches!(plan, Plan::Wait { .. }),
                "resource={resource:?}: {plan:?}"
            );
            failed.apply(plan);
            assert_eq!(failed.snapshot.status.topology, before.topology);
            assert_eq!(
                failed.snapshot.status.secondary_scale_down_cleanup,
                before.secondary_scale_down_cleanup
            );
            assert_eq!(failed.deletes, deletes);
        }
    }
}

#[test]
fn evaluator_scale_down_replays_installed_pending_configurations_after_restart() {
    use scale_down_model::{CommandBoundary, Model};
    for current_only in [false, true] {
        for id in [1, 2] {
            let mut model = Model::new(&[1, 2, 3], 1, 2);
            model.until(|m| {
                matches!(m.plan(), Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(c)
            } if c.transition_kind == TransitionKind::SecondaryScaleDown
                && c.current_only == current_only && c.local_replica_id.value() == id)
            });
            let original = model.plan();
            model.interrupt(original.clone(), CommandBoundary::Effect);
            model.controller_restart();
            // Other admissions may precede replay, notably the primary that supplies catch-up.
            model.until(|m| m.plan() == original);
            assert!(model.removed.is_empty());
            model.step();
            assert!(model.report(id).pending_operation_id.is_none());
            model.finish();
            assert_eq!(model.removed.len(), 1);
            assert_eq!(model.deletes.len(), 3);
        }
    }
}

#[test]
fn evaluator_scale_down_pending_primary_replay_precedes_secondary_catch_up() {
    use scale_down_model::{CommandBoundary, Model};
    let mut model = Model::new(&[1, 2, 3], 1, 2);
    model.until(|m| {
        matches!(m.plan(), Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(c)
    } if c.transition_kind == TransitionKind::SecondaryScaleDown)
    });
    model.interrupt(model.plan(), CommandBoundary::Effect);
    let primary = model.plan();
    assert!(matches!(&primary, Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(c)
    } if c.local_replica_id.value() == 1));
    model.interrupt(primary.clone(), CommandBoundary::Effect);
    model.controller_restart();
    assert_eq!(model.plan(), primary);
    model.finish();
    assert!(model.inflight.is_empty());
    assert!(model.applied_effects.is_empty());
}

#[test]
fn evaluator_scale_down_replays_pending_commit_acceptance_after_restart() {
    use scale_down_model::Model;
    for id in [1, 2] {
        for applied in [false, true] {
            let mut model = Model::new(&[1, 2, 3], 1, 2);
            model.until(|m| {
                matches!(m.plan(), Plan::Execute {
                command: ProtocolCommand::AcceptSecondaryRemovalCommit(c)
            } if c.target.replica_id.value() == id)
            });
            let original = model.plan();
            let Plan::Execute {
                command: ProtocolCommand::AcceptSecondaryRemovalCommit(command),
            } = &original
            else {
                unreachable!()
            };
            model.report(id).pending_operation_id = Some(command.operation_id.clone());
            if applied {
                model.report(id).accepted_secondary_removal = Some(command.committed.clone());
                model.report(id).prepared_secondary_removal = None;
            }
            model.controller_restart();
            assert_eq!(model.plan(), original);
            let mut conflict = model.clone();
            conflict.report(id).pending_operation_id = Some(OperationId::new("unrelated-work"));
            assert_ne!(conflict.plan(), original);
            model.report(id).pending_operation_id = None;
            model.finish();
            assert!(model.inflight.is_empty());
            assert_eq!(model.deletes.len(), 3);
        }
    }
}

#[test]
fn evaluator_scale_down_replays_pending_retirement_but_not_conflicting_work() {
    use scale_down_model::{CommandBoundary, Model, reason};
    let mut model = Model::new(&[1, 2], 1, 1);
    model.until(|m| {
        matches!(
            m.plan(),
            Plan::Execute {
                command: ProtocolCommand::RetireReplica(_)
            }
        )
    });
    let original = model.plan();
    model.interrupt(original.clone(), CommandBoundary::Effect);
    model.controller_restart();
    assert_eq!(model.plan(), original);
    assert!(
        model.deletes.iter().all(
            |(resource, _)| *resource == kuberic_protocol::command::ScaleDownResource::Endpoint
        )
    );
    let mut conflict = model.clone();
    conflict.report(2).pending_operation_id = Some(OperationId::new("different-retirement"));
    assert!(matches!(conflict.plan(), Plan::Wait { .. }));
    assert_eq!(reason(&conflict.plan()), "ScaleDownRetirementPending");
    model.finish();
    assert_eq!(model.deletes.len(), 3);
}

#[test]
fn evaluator_scale_down_conflicting_pending_configuration_is_not_overwritten_or_credited() {
    use scale_down_model::{CommandBoundary, Model};
    for current_only in [false, true] {
        let mut model = Model::new(&[1, 2, 3], 1, 2);
        model.until(|m| {
            matches!(m.plan(), Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(c)
        } if c.transition_kind == TransitionKind::SecondaryScaleDown
            && c.current_only == current_only && c.local_replica_id.value() == 2)
        });
        model.interrupt(model.plan(), CommandBoundary::Effect);
        model.report(2).pending_operation_id = Some(OperationId::new("conflicting-work"));
        model.until(|m| matches!(m.plan(), Plan::Wait { .. }));
        assert!(model.removed.is_empty());
        assert!(model.deletes.is_empty());
        assert_eq!(
            model
                .report(2)
                .pending_operation_id
                .as_ref()
                .unwrap()
                .as_str(),
            "conflicting-work"
        );
    }
}

#[test]
fn evaluator_scale_down_completed_receipt_converges_late_members_without_cleanup_authority() {
    use kuberic_protocol::observation::ExactResourceObservation;
    use scale_down_model::{CommandBoundary, Model, reason};
    for late_stage in 0..4 {
        let mut model = Model::new(&[1, 2, 3, 4], 1, 3);
        if late_stage > 0 {
            model.until(|m| {
                matches!(m.plan(), Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(c)
            } if c.transition_kind == TransitionKind::SecondaryScaleDown
                && c.current_only == (late_stage == 3) && c.local_replica_id.value() == 3)
            });
            if late_stage == 2 {
                model.step();
            } else {
                model.interrupt(model.plan(), CommandBoundary::Effect);
            }
        }
        let late = model.report(3).clone();
        let key = model.key(3);
        model.unavailable(3);
        model.until(|m| m.snapshot.status.last_secondary_removal.is_some());
        assert!(model.snapshot.status.secondary_scale_down_cleanup.is_none());
        assert_eq!(model.deletes.len(), 3);
        let receipt = model
            .snapshot
            .status
            .last_secondary_removal
            .clone()
            .unwrap();
        model.controller_restart();

        let exact = model.exact(4);
        for resource in [&mut exact.pod, &mut exact.pvc, &mut exact.endpoint] {
            *resource = ExactResourceObservation::ReplacementPresent {
                uid: "replacement-must-survive".into(),
                resource_version: "replacement-rv".into(),
            };
        }
        // A single bounded receipt must not be superseded while a non-witness still needs it.
        model.snapshot.desired.replicas = 2;
        assert_eq!(reason(&model.plan()), "ScaleDownRetainedMemberPending");
        model.snapshot.desired.replicas = 3;
        model.snapshot.replicas.get_mut(&key).unwrap().agent =
            AgentObservation::Report(Box::new(late));
        let mut conflict = model.clone();
        conflict.report(3).pending_operation_id = Some(OperationId::new("unrelated-local-work"));
        assert_eq!(reason(&conflict.plan()), "ScaleDownRetainedMemberPending");
        assert!(matches!(conflict.plan(), Plan::Wait { .. }));
        let mut unhealthy = model.clone();
        unhealthy.report(3).healthy = false;
        assert_eq!(reason(&unhealthy.plan()), "ScaleDownRetainedMemberPending");
        assert!(matches!(unhealthy.plan(), Plan::Wait { .. }));
        let mut forged = model.clone();
        forged
            .snapshot
            .status
            .last_secondary_removal
            .as_mut()
            .unwrap()
            .current_only_write_quorum
            .clear();
        assert!(matches!(forged.plan(), Plan::Unsafe { .. }));
        assert!(matches!(
            evaluate(&model.snapshot, &EvaluationConfig::default()),
            Plan::Unsafe { .. }
        ));
        model.finish();
        assert!(model.inflight.is_empty());
        assert!(model.applied_effects.is_empty());
        assert!(model.report(3).previous_configuration.is_none());
        assert!(model.report(3).pending_operation_id.is_none());
        assert_eq!(model.report(3).write_status, AccessStatus::NotPrimary);
        assert_eq!(
            model.report(3).accepted_secondary_removal,
            Some(receipt.committed())
        );
        assert_eq!(
            model.deletes.len(),
            3,
            "completed receipt cannot reissue cleanup"
        );
        assert_eq!(
            model.snapshot.status.last_secondary_removal.as_ref(),
            Some(&receipt)
        );
        model.controller_restart();
        assert!(matches!(model.plan(), Plan::Stable { .. }));

        model.snapshot.desired.replicas = 2;
        model.finish();
        assert_eq!(model.removed.len(), 2);
        assert_eq!(model.deletes.len(), 6);
        let next = model
            .snapshot
            .status
            .last_secondary_removal
            .as_ref()
            .unwrap();
        assert_ne!(
            next.evidence.preparation.intent.operation_id,
            receipt.evidence.preparation.intent.operation_id
        );
        assert_eq!(
            next.evidence.preparation.intent.target.replica_id.value(),
            3
        );
        let mut mixed = model.clone();
        mixed
            .snapshot
            .status
            .last_secondary_removal
            .as_mut()
            .unwrap()
            .evidence = receipt.evidence;
        assert!(
            matches!(mixed.plan(), Plan::Unsafe { .. }),
            "one removal cannot supply another's proof"
        );
    }
}

#[test]
fn evaluator_scale_down_completed_witness_can_be_absent_during_next_removal() {
    use scale_down_model::Model;
    let mut model = Model::new(&[1, 2, 3, 4], 1, 3);
    model.finish();
    let previous = model
        .snapshot
        .status
        .last_secondary_removal
        .clone()
        .unwrap();
    assert!(
        previous
            .current_only_write_quorum
            .iter()
            .any(|w| w.identity.replica_id.value() == 3)
    );
    model.unavailable(3);
    model.snapshot.desired.replicas = 2;
    model.controller_restart();
    model.finish();
    assert_eq!(model.removed.len(), 2);
    assert_eq!(model.deletes.len(), 6);
    let current = model
        .snapshot
        .status
        .last_secondary_removal
        .as_ref()
        .unwrap();
    assert_ne!(previous.evidence, current.evidence);
    assert_eq!(
        current
            .evidence
            .preparation
            .intent
            .target
            .replica_id
            .value(),
        3
    );
}

#[test]
fn evaluator_scale_down_healthy_traces_preserve_exact_authority() {
    use scale_down_model::Model;
    for (ids, primary, desired, targets) in [
        (vec![1, 2, 3], 1, 2, vec![3]),
        (vec![1, 2], 1, 1, vec![2]),
        (vec![1, 2, 3, 4, 5], 1, 2, vec![5, 4, 3]),
        (vec![2, 4, 9], 9, 1, vec![4, 2]),
    ] {
        let mut model = Model::new(&ids, primary, desired);
        let starting = model
            .snapshot
            .status
            .topology
            .clone()
            .unwrap()
            .configuration;
        model.finish();
        assert_eq!(
            model
                .removed
                .iter()
                .map(|i| i.replica_id.value())
                .collect::<Vec<_>>(),
            targets
        );
        let accepted = &model
            .snapshot
            .status
            .topology
            .as_ref()
            .unwrap()
            .configuration;
        assert_eq!(accepted.primary_id.value(), primary);
        assert_eq!(accepted.members.len(), desired as usize);
        assert!(
            accepted
                .members
                .iter()
                .all(|m| starting.members.contains(m))
        );
        assert_eq!(
            accepted.epoch.data_loss_number,
            starting.epoch.data_loss_number
        );
        assert_eq!(
            accepted.epoch.configuration_number,
            starting.epoch.configuration_number + targets.len() as i64
        );
        assert_eq!(
            model.snapshot.status.observed_generation,
            model.snapshot.desired.generation
        );
        assert_eq!(model.deletes.len(), targets.len() * 3);
        for removed in &model.removed {
            let commands = model
                .commands
                .iter()
                .filter_map(|command| match command {
                    ProtocolCommand::EnsureConfiguration(c)
                        if c.secondary_removal_evidence
                            .as_ref()
                            .is_some_and(|e| e.preparation.intent.target == *removed) =>
                    {
                        Some(c.as_ref())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            let mut identities = commands[0]
                .current_configuration
                .members
                .iter()
                .map(|m| m.identity.replica_id)
                .collect::<Vec<_>>();
            identities.sort_by_key(|id| (*id == ReplicaId::new(primary), *id));
            let expected = [false, true]
                .into_iter()
                .flat_map(|current_only| identities.iter().map(move |id| (*id, current_only)))
                .collect::<Vec<_>>();
            assert_eq!(
                commands
                    .iter()
                    .map(|c| (c.local_replica_id, c.current_only))
                    .collect::<Vec<_>>(),
                expected
            );
        }
        let commands = model.commands.len();
        for _ in 0..3 {
            assert!(matches!(model.step(), Plan::Stable { .. }));
        }
        assert_eq!(model.commands.len(), commands);
    }
}

#[test]
fn evaluator_scale_down_default_gate_increase_floor_and_equality() {
    use scale_down_model::{Model, config};
    let mut model = Model::new(&[1, 2, 3], 1, 2);
    assert!(matches!(
        evaluate(&model.snapshot, &EvaluationConfig::default()),
        Plan::Stable { .. }
    ));
    model.step();
    assert!(matches!(
        evaluate(&model.snapshot, &EvaluationConfig::default()),
        Plan::Unsafe { .. }
    ));
    let mut equal = Model::new(&[1, 2, 3], 1, 3);
    assert!(matches!(equal.step(), Plan::Stable { .. }));
    assert!(equal.commands.is_empty());
    equal.snapshot.desired.replicas = 4;
    let Plan::Stable { status, .. } = equal.plan() else {
        panic!("increase must not provision")
    };
    assert!(
        status
            .conditions
            .iter()
            .any(|c| c.reason == "ScaleUpUnsupported")
    );
    equal.snapshot.desired.replicas = 0;
    assert!(matches!(
        evaluate(&equal.snapshot, &config()),
        Plan::Unsafe {
            reason: UnsafeReason::InvalidDesiredState(_),
            ..
        }
    ));
}

#[test]
fn evaluator_scale_down_unavailable_highest_target_does_not_trigger_replacement() {
    use scale_down_model::Model;
    for ids in [vec![1, 2], vec![1, 2, 3], vec![2, 4, 9]] {
        let primary = *ids.first().unwrap();
        let target = *ids.last().unwrap();
        let mut model = Model::new(&ids, primary, ids.len() as u32 - 1);
        model.unavailable(target);
        let key = model.key(target);
        model
            .snapshot
            .replicas
            .get_mut(&key)
            .unwrap()
            .kubernetes
            .as_mut()
            .unwrap()
            .peer_endpoint_ready = false;
        model.finish();
        assert_eq!(model.removed[0].replica_id.value(), target);
        assert!(
            !model
                .commands
                .iter()
                .any(|c| matches!(c, ProtocolCommand::RetireReplica(_)))
        );
        assert!(model.deletes.iter().any(|(r, uid)| *r
            == kuberic_protocol::command::ScaleDownResource::Pod
            && uid == &format!("pod-{target}")));
    }
}

#[test]
fn evaluator_scale_down_retained_read_quorum_preflight_preserves_writable_service() {
    use scale_down_model::{Model, config, reason};
    let mut model = Model::new(&[1, 2, 3], 1, 2);
    let retained = model.snapshot.replicas[&model.key(2)].clone();
    let authority = model.snapshot.status.clone();
    let routing = model.snapshot.routing.clone();
    model.unavailable(2);
    for _ in 0..3 {
        let plan = model.step();
        assert_eq!(reason(&plan), "ScaleDownRetainedReadQuorumUnavailable");
        assert!(matches!(plan, Plan::Wait { requeue_after_seconds, .. }
            if requeue_after_seconds == config().wait_requeue_seconds));
        assert!(model.snapshot.status.transition.is_none());
        assert!(model.snapshot.status.provisioning.is_none());
        assert!(model.snapshot.status.pending_replacement_cleanup.is_none());
        assert_eq!(model.snapshot.status.topology, authority.topology);
        assert_eq!(
            model.snapshot.status.effective_policy,
            authority.effective_policy
        );
        assert_eq!(model.snapshot.routing, routing);
        assert_eq!(model.report(1).write_status, AccessStatus::Granted);
        assert!(model.report(1).prepared_secondary_removal.is_none());
        assert!(model.commands.is_empty() && model.deletes.is_empty());
        assert_eq!(model.plan(), plan, "unchanged wait status must be stable");
    }
    model.snapshot.replicas.insert(model.key(2), retained);
    model.step();
    let intent = model
        .snapshot
        .status
        .transition
        .as_ref()
        .unwrap()
        .secondary_scale_down
        .as_ref()
        .unwrap();
    assert_eq!(intent.target.replica_id, ReplicaId::new(3));
    assert_eq!(
        intent.previous_configuration,
        authority.topology.unwrap().configuration
    );
    assert_eq!(
        model.snapshot.routing, routing,
        "admission only freezes intent"
    );
    assert_eq!(model.report(1).write_status, AccessStatus::Granted);
    model.finish();
    assert_eq!(model.removed[0].replica_id, ReplicaId::new(3));
}

#[test]
fn evaluator_scale_down_preflight_requires_fresh_exact_stable_retained_reports() {
    use kuberic_protocol::types::FaultType;
    use scale_down_model::{Model, reason};
    for mutation in 0..8 {
        let mut model = Model::new(&[1, 2, 3], 1, 2);
        match mutation {
            0 => model.report(2).healthy = false,
            1 => model.report(2).reported_fault = Some(FaultType::Permanent),
            2 => model.report(2).pending_operation_id = Some(OperationId::new("unfinished")),
            3 => model.report(2).identity.agent_generation = AgentGeneration::new("other"),
            4 => model.report(2).resource_uid = ResourceUid::new("other"),
            5 => model.report(2).process_session_id = ProcessSessionId::default(),
            6 => model.report(2).report_sequence = 0,
            _ => {
                let report = model.report(2).clone();
                model.snapshot.previous_report_watermarks.insert(
                    model.key(2),
                    ReportWatermark {
                        process_session_id: report.process_session_id,
                        report_sequence: report.report_sequence,
                    },
                );
            }
        }
        let plan = model.plan();
        assert!(
            matches!(plan, Plan::Wait { .. } | Plan::Unsafe { .. }),
            "{mutation}: {plan:?}"
        );
        if mutation < 3 {
            assert_eq!(reason(&plan), "ScaleDownRetainedReadQuorumUnavailable");
        }
        match plan {
            Plan::Wait { .. } => model.apply(plan),
            Plan::Unsafe { status, .. } => assert!(status.transition.is_none()),
            _ => unreachable!(),
        }
        assert!(model.snapshot.status.transition.is_none());
        assert!(model.snapshot.routing.write_target.is_some());
        assert_eq!(model.report(1).write_status, AccessStatus::Granted);
        assert!(model.commands.is_empty() && model.deletes.is_empty());
    }
}

#[test]
fn evaluator_scale_down_never_credits_target_for_previous_or_reduced_quorum() {
    use scale_down_model::{Model, reason};
    let mut model = Model::new(&[1, 2, 3], 1, 2);
    model.step();
    model.unavailable(2);
    model.until(|m| m.snapshot.replicas.values().any(|o| matches!(&o.agent, AgentObservation::Report(r) if r.prepared_secondary_removal.is_some())));
    assert_eq!(
        reason(&model.plan()),
        "ScaleDownPreviousReadQuorumUnavailable"
    );
    assert!(
        model
            .snapshot
            .status
            .transition
            .as_ref()
            .unwrap()
            .secondary_removal_evidence
            .is_none()
    );
    assert!(model.deletes.is_empty());

    let mut model = Model::new(&[1, 2, 3], 1, 2);
    model.until(|m| {
        m.snapshot
            .status
            .transition
            .as_ref()
            .is_some_and(|t| t.secondary_removal_evidence.is_some())
    });
    model.unavailable(2);
    model.until(|m| matches!(m.plan(), Plan::Wait { .. }));
    assert_eq!(
        reason(&model.plan()),
        "ScaleDownReducedWriteQuorumUnavailable"
    );
    assert!(model.removed.is_empty());
    assert!(model.deletes.is_empty());
}

#[test]
fn evaluator_scale_down_primary_prefix_is_required_even_with_other_read_witnesses() {
    use scale_down_model::{Model, reason};
    let mut model = Model::new(&[1, 2, 3, 4, 5], 1, 4);
    model.until(|m| {
        m.snapshot.replicas.values().any(|o|
        matches!(&o.agent, AgentObservation::Report(r) if r.prepared_secondary_removal.is_some()))
    });
    model.report(1).verified_replication_lsn = None;
    assert_eq!(
        reason(&model.plan()),
        "ScaleDownPreviousReadQuorumUnavailable"
    );
    assert!(
        model
            .snapshot
            .status
            .transition
            .as_ref()
            .unwrap()
            .secondary_removal_evidence
            .is_none()
    );
}

#[test]
fn evaluator_scale_down_requires_verified_completed_fresh_current_only_quorum() {
    use scale_down_model::{Model, reason};
    let mut model = Model::new(&[1, 2, 3], 1, 2);
    model.until(|m| {
        m.snapshot.status.transition.as_ref().is_some_and(|t| {
            t.secondary_removal_evidence
                .as_ref()
                .is_some_and(|e| !e.reduced_write_quorum.is_empty())
        })
    });
    model.until(|m| m.snapshot.replicas.values().filter(|o| matches!(&o.agent,
        AgentObservation::Report(r) if r.secondary_removal_evidence.is_some() && r.previous_configuration.is_none())).count() == 2);
    for mutation in 0..4 {
        let mut broken = model.clone();
        let r = broken.report(2);
        match mutation {
            0 => r.verified_replication_lsn = None,
            1 => r.verified_replication_lsn = Some(9),
            2 => r.pending_operation_id = Some(OperationId::new("unfinished")),
            _ => r.report_sequence -= 1,
        }
        assert_eq!(
            reason(&broken.plan()),
            "ScaleDownCurrentOnlyQuorumUnavailable"
        );
        assert!(broken.removed.is_empty());
    }
    model.step();
    assert_eq!(model.removed.len(), 1);
}

#[test]
fn evaluator_scale_down_primary_loss_at_each_stage_freezes_target() {
    use scale_down_model::{Model, reason};
    let mut model = Model::new(&[1, 2, 3], 1, 2);
    model.step();
    for _ in 0..16 {
        if model.snapshot.status.secondary_scale_down_cleanup.is_some() {
            break;
        }
        let mut lost = model.clone();
        lost.unavailable(1);
        lost.snapshot.routing.write_target = None;
        let intent = lost.snapshot.status.transition.clone();
        assert_eq!(reason(&lost.plan()), "ScaleDownPrimaryUnavailable");
        lost.step();
        assert_eq!(lost.snapshot.status.transition, intent);
        assert!(
            lost.commands
                .iter()
                .all(|c| !matches!(c, ProtocolCommand::RetireReplica(_)))
        );
        model.step();
    }
}

#[test]
fn evaluator_scale_down_exact_resources_cannot_be_inferred_from_lists() {
    use scale_down_model::{Model, reason};
    let mut model = Model::new(&[1, 2, 3], 1, 2);
    model.snapshot.secondary_scale_down_resources.clear();
    assert_eq!(reason(&model.plan()), "ScaleDownExactIdentityRequired");
    assert!(model.snapshot.status.transition.is_none());
    let mut model = Model::new(&[1, 2, 3], 1, 2);
    model.exact(3).pvc = kuberic_protocol::observation::ExactResourceObservation::LookupFailed {
        message: "forbidden".into(),
    };
    assert_eq!(reason(&model.plan()), "ScaleDownExactIdentityRequired");
}

#[test]
fn evaluator_scale_down_pod_fence_is_post_commit_and_pvc_requires_exact_absence() {
    use kuberic_protocol::command::ScaleDownResource;
    use kuberic_protocol::observation::ExactResourceObservation;
    use scale_down_model::{Model, reason};
    let mut model = Model::new(&[1, 2, 3], 1, 2);
    model.unavailable(3);
    model.until(|m| matches!(m.plan(), Plan::Apply { changes } if changes.iter().any(|c|
        matches!(c, KubernetesChange::DeleteScaleDownResource { resource: ScaleDownResource::Pod, .. }))));
    assert_eq!(model.removed.len(), 1);
    assert_eq!(model.deletes.len(), 1);
    assert_eq!(
        reason(&Plan::Wait {
            reason: WaitReason::ActiveTransition,
            status: model.snapshot.status.clone(),
            requeue_after_seconds: 5
        }),
        "ScaleDownExactPodFencePending"
    );
    let fence = model.plan();
    assert_eq!(
        fence,
        model.plan(),
        "lost delete reply retries exact UID and RV"
    );
    let key = model.key(3);
    model.snapshot.replicas.remove(&key);
    model.exact(3).pod = ExactResourceObservation::LookupFailed {
        message: "list omission".into(),
    };
    assert_eq!(reason(&model.plan()), "ScaleDownExactPodAbsenceRequired");
    assert!(
        !model
            .deletes
            .iter()
            .any(|(r, _)| *r == ScaleDownResource::Pvc)
    );
    model.exact(3).pod = ExactResourceObservation::NotFound;
    model.finish();
    assert!(
        model
            .deletes
            .iter()
            .any(|(r, _)| *r == ScaleDownResource::Pvc)
    );
}

#[test]
fn evaluator_scale_down_same_name_replacements_are_never_deleted_even_after_cleanup() {
    use kuberic_protocol::observation::ExactResourceObservation;
    use scale_down_model::Model;
    let mut model = Model::new(&[1, 2, 3], 1, 2);
    model.unavailable(3);
    model.until(|m| m.snapshot.status.secondary_scale_down_cleanup.is_some());
    let key = model.key(3);
    model.snapshot.replicas.remove(&key);
    model.exact(3).pod = ExactResourceObservation::ReplacementPresent {
        uid: "replacement".into(),
        resource_version: "new-rv".into(),
    };
    model.exact(3).pvc = ExactResourceObservation::ReplacementPresent {
        uid: "replacement-pvc".into(),
        resource_version: "new-rv".into(),
    };
    model.exact(3).endpoint = ExactResourceObservation::ReplacementPresent {
        uid: "replacement-endpoint".into(),
        resource_version: "new-rv".into(),
    };
    model.finish();
    assert!(model.deletes.is_empty());
    model.snapshot.replicas.insert(
        ReplicaObservationKey::new(ReplicaId::new(3), ReplicaInstanceId::new("replacement")),
        ReplicaObservation {
            agent: AgentObservation::Absent,
            kubernetes: Some(KubernetesReplicaObservation {
                replica_id: ReplicaId::new(3),
                pod_name: "db-3".into(),
                pod_uid: Some(PodUid::new("replacement")),
                pvc_name: "db-storage-3".into(),
                pvc_uid: Some(PvcUid::new("replacement-pvc")),
                image: Some("example:v1".into()),
                pod_ready: true,
                peer_endpoint_ready: true,
            }),
        },
    );
    assert!(matches!(model.plan(), Plan::Stable { status, .. }
        if status.conditions.iter().any(|c| c.reason == "ExactCleanupAuthorityRequired")));
}

#[test]
fn evaluator_scale_down_drift_switchover_and_primary_safety_precede_admission() {
    use scale_down_model::{Model, reason};
    for field in 0..2 {
        let mut model = Model::new(&[1, 2, 3], 1, 2);
        if field == 0 {
            model.snapshot.desired.image = "example:v2".into();
        } else {
            model.snapshot.desired.failover_delay_seconds += 1;
        }
        assert_eq!(reason(&model.plan()), "ScaleDownUnsupportedDrift");
    }
    let mut model = Model::new(&[1, 2, 3], 1, 2);
    model.snapshot.desired.switchover = Some(PlannedSwitchoverRequest {
        request_id: SwitchoverRequestId::new("move"),
        target_replica_id: ReplicaId::new(2),
    });
    let Plan::Apply { changes } = model.plan() else {
        panic!("switchover should win")
    };
    assert!(changes.iter().any(|c| matches!(c, KubernetesChange::PersistStatus { status }
        if status.transition.as_ref().is_some_and(|t| t.kind == TransitionKind::PlannedSwitchover))));
    let mut model = Model::new(&[1, 2, 3], 1, 2);
    model.unavailable(1);
    let Plan::Apply { changes } = model.plan() else {
        panic!("primary safety should win")
    };
    assert!(
        changes
            .iter()
            .any(|c| matches!(c, KubernetesChange::PersistStatus { status }
        if status.primary_failure.is_some() && status.transition.is_none()))
    );
}

#[test]
fn evaluator_scale_down_stale_sessions_and_contradictory_authority_fail_closed() {
    use scale_down_model::{Model, reason};
    let mut model = Model::new(&[1, 2, 3], 1, 2);
    model.step();
    let key = model.key(1);
    let r = model.report(1).clone();
    model.snapshot.previous_report_watermarks.insert(
        key,
        ReportWatermark {
            process_session_id: r.process_session_id,
            report_sequence: r.report_sequence,
        },
    );
    assert_eq!(reason(&model.plan()), "ScaleDownFreshReportRequired");
    model.report(1).process_session_id = ProcessSessionId::new("restart");
    assert!(!matches!(
        model.plan(),
        Plan::Unsafe { .. } | Plan::Wait { .. }
    ));
    model.snapshot.previous_report_watermarks.clear();
    model.report(2).identity.agent_generation = AgentGeneration::new("replacement");
    assert!(matches!(model.plan(), Plan::Unsafe { .. }));
    assert!(model.deletes.is_empty());
}

#[test]
fn evaluator_scale_down_late_member_replays_pc_cc_before_current_only() {
    use kuberic_protocol::types::SecondaryRemovalStage;
    use scale_down_model::Model;
    let mut model = Model::new(&[1, 2, 3, 4], 1, 3);
    let mut late = model.report(3).clone();
    model.unavailable(3);
    model.until(|m| {
        m.snapshot.status.transition.as_ref().is_some_and(|t| {
            t.secondary_removal_evidence
                .as_ref()
                .is_some_and(|e| !e.reduced_write_quorum.is_empty())
        })
    });
    let intent = model
        .snapshot
        .status
        .transition
        .as_ref()
        .unwrap()
        .secondary_scale_down
        .clone()
        .unwrap();
    late.pending_operation_id =
        Some(intent.command_operation_id(SecondaryRemovalStage::PreviousCurrent, &late.identity));
    let key = model.key(3);
    model.snapshot.replicas.get_mut(&key).unwrap().agent = AgentObservation::Report(Box::new(late));
    model.until(|m| matches!(m.plan(), Plan::Execute { command: ProtocolCommand::EnsureConfiguration(c) } if c.local_replica_id == ReplicaId::new(3)));
    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(command),
    } = model.plan()
    else {
        unreachable!()
    };
    assert!(!command.current_only);
    assert_eq!(
        command.previous_configuration.as_ref(),
        Some(&intent.previous_configuration)
    );
    assert!(
        command
            .secondary_removal_evidence
            .as_ref()
            .unwrap()
            .reduced_write_quorum
            .is_empty()
    );
    model.finish();
}

#[test]
fn evaluator_scale_down_freezes_evidence_instead_of_resampling_advanced_reports() {
    use scale_down_model::Model;
    let mut model = Model::new(&[1, 2, 3], 1, 2);
    model.until(|m| {
        m.snapshot
            .status
            .transition
            .as_ref()
            .is_some_and(|t| t.secondary_removal_evidence.is_some())
    });
    let frozen = model
        .snapshot
        .status
        .transition
        .as_ref()
        .unwrap()
        .secondary_removal_evidence
        .clone()
        .unwrap();
    model.step();
    assert!(model.report(2).previous_configuration.is_some());
    assert_eq!(
        model
            .snapshot
            .status
            .transition
            .as_ref()
            .unwrap()
            .secondary_removal_evidence
            .as_ref()
            .unwrap()
            .previous_read_quorum,
        frozen.previous_read_quorum
    );
    model.finish();
}

#[test]
fn evaluator_scale_down_target_return_uses_retirement_not_a_new_membership() {
    use scale_down_model::Model;
    let mut model = Model::new(&[1, 2, 3], 1, 2);
    let saved = model.report(3).clone();
    model.unavailable(3);
    model.until(|m| m.snapshot.status.secondary_scale_down_cleanup.is_some());
    let key = model.key(3);
    model.snapshot.replicas.get_mut(&key).unwrap().agent =
        AgentObservation::Report(Box::new(saved));
    model.finish();
    assert!(model.commands.iter().any(|c| matches!(c, ProtocolCommand::RetireReplica(r) if r.local_replica_id == ReplicaId::new(3))));
    assert_eq!(model.removed.len(), 1);
}

#[test]
fn completed_removal_history_allows_return_after_failover_or_replacement() {
    use kuberic_protocol::validation::validate_snapshot;
    use scale_down_model::Model;
    for replacement in [false, true] {
        let mut model = Model::new(&[1, 2, 3, 4], 1, 3);
        model.finish();
        let late_id = if replacement { 3 } else { 1 };
        let mut late = model.report(late_id).clone();
        late.write_status = AccessStatus::ReconfigurationPending;
        model.unavailable(late_id);
        let previous = model
            .snapshot
            .status
            .topology
            .as_ref()
            .unwrap()
            .configuration
            .clone();
        let mut members = previous.members.clone();
        if replacement {
            members[1].identity.instance_id = ReplicaInstanceId::new("replacement-pod");
            members[1].identity.agent_generation = AgentGeneration::new("replacement-generation");
        } else {
            members[0].role = ReplicaRole::ActiveSecondary;
            members[1].role = ReplicaRole::Primary;
        }
        let accepted = ConfigurationDescriptor::new(
            Epoch::new(
                previous.epoch.data_loss_number,
                previous.epoch.configuration_number + 1,
            ),
            ReplicaId::new(if replacement { 1 } else { 2 }),
            members,
            previous.write_quorum,
        );
        for member in &accepted.members {
            if member.identity.replica_id.value() == late_id {
                continue;
            }
            let key = model.key(member.identity.replica_id.value());
            let mut observation = model.snapshot.replicas.remove(&key).unwrap();
            let AgentObservation::Report(report) = &mut observation.agent else {
                unreachable!()
            };
            report.identity = member.identity.clone();
            report.epoch = accepted.epoch;
            report.role = member.role;
            report.current_configuration = Some(accepted.clone());
            report.secondary_removal_evidence = None;
            report.accepted_secondary_removal = None;
            report.prepared_secondary_removal = None;
            report.write_status = if member.role == ReplicaRole::Primary {
                AccessStatus::Granted
            } else {
                AccessStatus::NotPrimary
            };
            observation.kubernetes.as_mut().unwrap().pod_uid =
                Some(PodUid::new(member.identity.instance_id.as_str()));
            model.snapshot.replicas.insert(
                ReplicaObservationKey::new(
                    member.identity.replica_id,
                    member.identity.instance_id.clone(),
                ),
                observation,
            );
        }
        model.snapshot.status.topology = Some(AcceptedTopology {
            configuration: accepted.clone(),
        });
        model.snapshot.routing.write_target = Some(
            accepted
                .members
                .iter()
                .find(|m| m.role == ReplicaRole::Primary)
                .unwrap()
                .identity
                .clone(),
        );
        let key = model.key(late_id);
        model.snapshot.replicas.get_mut(&key).unwrap().agent =
            AgentObservation::Report(Box::new(late));
        model.controller_restart();
        validate_snapshot(&model.snapshot).unwrap();
        for mutation in 0..7 {
            let mut broken = model.clone();
            let report = broken.report(late_id);
            match mutation {
                0 => report.write_status = AccessStatus::Granted,
                1 => {
                    report
                        .secondary_removal_evidence
                        .as_mut()
                        .unwrap()
                        .preparation
                        .boundary_lsn -= 1
                }
                2 => {
                    report
                        .secondary_removal_evidence
                        .as_mut()
                        .unwrap()
                        .previous_read_quorum[0]
                        .report_sequence += 1
                }
                3 => report.resource_uid = ResourceUid::new("other-resource"),
                4 => report.identity.agent_generation = AgentGeneration::new("wrong-generation"),
                5 => report.epoch = accepted.epoch,
                _ => {
                    report
                        .accepted_secondary_removal
                        .as_mut()
                        .unwrap()
                        .current_only_write_quorum[0]
                        .report_sequence += 1
                }
            }
            assert!(
                validate_snapshot(&broken.snapshot).is_err(),
                "mutation {mutation}"
            );
        }
        assert!(
            matches!(model.plan(), Plan::Execute { command: ProtocolCommand::EnsureConfiguration(ref c) }
            if c.local_replica_id.value() == late_id && c.current_configuration == accepted && !c.current_only)
        );
        model.finish();
        assert_eq!(
            model.report(late_id).current_configuration.as_ref(),
            Some(&accepted)
        );
        assert!(model.report(late_id).secondary_removal_evidence.is_none());
    }
}

#[test]
fn completed_removal_local_acceptance_precedes_newer_authority_correction() {
    use kuberic_protocol::types::SecondaryRemovalStage;
    use scale_down_model::Model;
    for (replacement, pending) in [false, true]
        .into_iter()
        .flat_map(|replacement| [false, true].map(|pending| (replacement, pending)))
    {
        let mut model = Model::new(&[1, 2, 3, 4, 5, 6], 1, 5);
        model.until(|m| {
            matches!(m.plan(), Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(c)
        } if c.local_replica_id == ReplicaId::new(5) && c.current_only)
        });
        model.step();
        let mut late = model.report(5).clone();
        assert!(late.previous_configuration.is_none());
        assert!(late.accepted_secondary_removal.is_none());
        model.unavailable(5);
        model.until(|m| m.snapshot.status.last_secondary_removal.is_some());
        assert!(model.snapshot.status.secondary_scale_down_cleanup.is_none());
        let receipt = model
            .snapshot
            .status
            .last_secondary_removal
            .clone()
            .unwrap();
        if pending {
            late.pending_operation_id = Some(
                receipt
                    .evidence
                    .preparation
                    .intent
                    .command_operation_id(SecondaryRemovalStage::AcceptCommit, &late.identity),
            );
            late.process_session_id = ProcessSessionId::new("restarted-pending-acceptance");
            late.report_sequence = 1;
        }
        let previous = receipt
            .evidence
            .preparation
            .intent
            .current_configuration
            .clone();
        let mut members = previous.members.clone();
        if replacement {
            members[3].identity.instance_id = ReplicaInstanceId::new("replacement-4");
            members[3].identity.agent_generation = AgentGeneration::new("replacement-generation");
        } else {
            members[0].role = ReplicaRole::ActiveSecondary;
            members[1].role = ReplicaRole::Primary;
        }
        let accepted = ConfigurationDescriptor::new(
            Epoch::new(
                previous.epoch.data_loss_number,
                previous.epoch.configuration_number + 1,
            ),
            ReplicaId::new(if replacement { 1 } else { 2 }),
            members,
            previous.write_quorum,
        );
        for member in &accepted.members {
            if member.identity == late.identity {
                continue;
            }
            let key = model.key(member.identity.replica_id.value());
            let mut observation = model.snapshot.replicas.remove(&key).unwrap();
            let AgentObservation::Report(report) = &mut observation.agent else {
                unreachable!()
            };
            report.identity = member.identity.clone();
            report.epoch = accepted.epoch;
            report.role = member.role;
            report.previous_configuration = None;
            report.current_configuration = Some(accepted.clone());
            report.secondary_removal_evidence = None;
            report.accepted_secondary_removal = None;
            report.prepared_secondary_removal = None;
            report.write_status = if member.role == ReplicaRole::Primary {
                AccessStatus::Granted
            } else {
                AccessStatus::NotPrimary
            };
            observation.kubernetes.as_mut().unwrap().pod_uid =
                Some(PodUid::new(member.identity.instance_id.as_str()));
            model.snapshot.replicas.insert(
                ReplicaObservationKey::new(
                    member.identity.replica_id,
                    member.identity.instance_id.clone(),
                ),
                observation,
            );
        }
        model.snapshot.status.topology = Some(AcceptedTopology {
            configuration: accepted.clone(),
        });
        model.snapshot.routing.write_target = Some(
            accepted
                .members
                .iter()
                .find(|m| m.role == ReplicaRole::Primary)
                .unwrap()
                .identity
                .clone(),
        );
        let key = model.key(5);
        model.snapshot.replicas.get_mut(&key).unwrap().agent =
            AgentObservation::Report(Box::new(late));
        model.controller_restart();
        let status = model.snapshot.status.clone();
        let deletes = model.deletes.clone();
        for mutation in 0..8 {
            let mut broken = model.clone();
            match mutation {
                0 => broken.snapshot.status.last_secondary_removal = None,
                1 => broken
                    .snapshot
                    .status
                    .last_secondary_removal
                    .as_mut()
                    .unwrap()
                    .current_only_write_quorum
                    .clear(),
                2 => {
                    broken
                        .report(5)
                        .secondary_removal_evidence
                        .as_mut()
                        .unwrap()
                        .preparation
                        .boundary_lsn -= 1
                }
                3 => {
                    broken.report(5).identity.agent_generation =
                        AgentGeneration::new("wrong-generation")
                }
                4 => broken.report(5).resource_uid = ResourceUid::new("wrong-resource"),
                5 => broken.report(5).epoch.configuration_number += 2,
                6 => {
                    broken.report(5).pending_operation_id = Some(OperationId::new("unrelated-work"))
                }
                _ => broken.report(5).verified_replication_lsn = Some(0),
            }
            assert!(
                matches!(broken.plan(), Plan::Unsafe { .. } | Plan::Wait { .. }),
                "mutation {mutation}: {:?}",
                broken.plan()
            );
        }
        let Plan::Execute {
            command: ProtocolCommand::AcceptSecondaryRemovalCommit(command),
        } = model.plan()
        else {
            panic!(
                "missing exact historical local acceptance: {:?}",
                model.plan()
            );
        };
        assert_eq!(command.target.replica_id, ReplicaId::new(5));
        assert!(command.local_recovery);
        assert_eq!(command.committed, receipt.committed());
        model.step();
        assert_eq!(model.snapshot.status, status);
        assert_eq!(model.deletes, deletes);
        assert_ne!(model.report(5).write_status, AccessStatus::Granted);
        assert_eq!(model.report(5).current_configuration, Some(previous));
        assert_eq!(
            model.report(5).accepted_secondary_removal,
            Some(receipt.committed())
        );
        for current_only in [false, true] {
            assert!(matches!(model.plan(), Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(c)
            } if c.local_replica_id == ReplicaId::new(5) && c.current_configuration == accepted
                && c.current_only == current_only));
            model.step();
        }
        model.finish();
        assert_eq!(model.report(5).current_configuration, Some(accepted));
        assert_eq!(model.snapshot.status.last_secondary_removal, Some(receipt));
    }
}

#[test]
fn scale_down_and_switchover_converge_endpoints_and_lagging_authority_before_admission() {
    use scale_down_model::Model;
    for switchover in [false, true] {
        let mut model = Model::new(&[1, 2, 3, 4], 1, 3);
        if switchover {
            model.snapshot.desired.switchover = Some(PlannedSwitchoverRequest {
                request_id: SwitchoverRequestId::new("after-catchup"),
                target_replica_id: ReplicaId::new(3),
            });
        } else {
            model.unavailable(4);
        }
        let accepted = model
            .snapshot
            .status
            .topology
            .as_ref()
            .unwrap()
            .configuration
            .clone();
        let old = ConfigurationDescriptor::new(
            Epoch::new(
                accepted.epoch.data_loss_number,
                accepted.epoch.configuration_number - 1,
            ),
            accepted.primary_id,
            accepted.members.clone(),
            accepted.write_quorum,
        );
        model.report(2).epoch = old.epoch;
        model.report(2).current_configuration = Some(old);
        let key = model.key(3);
        model
            .snapshot
            .replicas
            .get_mut(&key)
            .unwrap()
            .kubernetes
            .as_mut()
            .unwrap()
            .peer_endpoint_ready = false;
        assert!(
            matches!(model.step(), Plan::Apply { changes } if matches!(changes[0], KubernetesChange::EnsureReplicaScaffolding { .. }))
        );
        for current_only in [false, true] {
            assert!(
                matches!(model.plan(), Plan::Execute { command: ProtocolCommand::EnsureConfiguration(ref c) }
                if c.local_replica_id == ReplicaId::new(2) && c.current_only == current_only)
            );
            model.step();
            assert!(model.snapshot.status.transition.is_none());
        }
        model.step();
        let frozen = model
            .snapshot
            .status
            .transition
            .clone()
            .expect("admitted after convergence");
        assert_eq!(
            frozen.kind,
            if switchover {
                TransitionKind::PlannedSwitchover
            } else {
                TransitionKind::SecondaryScaleDown
            }
        );
        model.snapshot.desired.replicas = 1;
        model.controller_restart();
        assert_eq!(model.snapshot.status.transition.as_ref(), Some(&frozen));
    }
}

#[test]
fn older_returning_excluded_target_uses_exact_pod_fence_and_allows_next_removal() {
    use scale_down_model::Model;
    let mut model = Model::new(&[1, 2, 3, 4], 1, 2);
    let mut saved = model.report(4).clone();
    let previous = saved.current_configuration.as_ref().unwrap();
    let older = ConfigurationDescriptor::new(
        Epoch::new(
            previous.epoch.data_loss_number,
            previous.epoch.configuration_number - 1,
        ),
        previous.primary_id,
        previous.members.clone(),
        previous.write_quorum,
    );
    saved.epoch = older.epoch;
    saved.current_configuration = Some(older);
    model.unavailable(4);
    model.until(|m| m.snapshot.status.secondary_scale_down_cleanup.is_some());
    let key = model.key(4);
    model.snapshot.replicas.get_mut(&key).unwrap().agent =
        AgentObservation::Report(Box::new(saved));
    model.controller_restart();
    model.finish();
    assert_eq!(
        model
            .removed
            .iter()
            .map(|r| r.replica_id.value())
            .collect::<Vec<_>>(),
        [4, 3]
    );
    assert!(!model.commands.iter().any(|c| matches!(c, ProtocolCommand::RetireReplica(c) if c.local_replica_id == ReplicaId::new(4))));
}

#[test]
fn evaluator_scale_down_reduced_catch_up_and_conflicting_storage_are_explicit() {
    use kuberic_protocol::types::CleanupResourceIdentity;
    use scale_down_model::{Model, reason};
    let mut model = Model::new(&[1, 2, 3], 1, 2);
    model.until(|m| {
        m.snapshot
            .replicas
            .values()
            .filter(|o| {
                matches!(&o.agent,
        AgentObservation::Report(r) if r.secondary_removal_evidence.is_some())
            })
            .count()
            == 2
    });
    model.report(2).verified_replication_lsn = Some(9);
    assert_eq!(reason(&model.plan()), "ScaleDownReducedCatchUpPending");
    model.report(2).verified_replication_lsn = Some(10);
    model.finish();
    let mut model = Model::new(&[1, 2, 3], 1, 2);
    model.exact(3).identity.pvc = CleanupResourceIdentity::Present {
        name: "db-storage-1".into(),
        uid: "pvc-1".into(),
    };
    assert!(matches!(
        model.plan(),
        Plan::Unsafe {
            reason: UnsafeReason::ContradictoryReplicaEvidence(_),
            ..
        }
    ));
    assert!(model.deletes.is_empty());
}

#[test]
fn evaluator_scale_down_authoritative_already_absent_target_needs_no_retirement_reply() {
    use kuberic_protocol::observation::ExactResourceObservation;
    use kuberic_protocol::types::CleanupResourceIdentity;
    use scale_down_model::Model;
    let mut model = Model::new(&[1, 2], 1, 1);
    let key = model.key(2);
    model.snapshot.replicas.remove(&key);
    model.exact(2).identity.pod = CleanupResourceIdentity::Absent {
        name: "db-2".into(),
    };
    model.exact(2).pod = ExactResourceObservation::NotFound;
    model.finish();
    assert!(
        !model
            .commands
            .iter()
            .any(|c| matches!(c, ProtocolCommand::RetireReplica(_)))
    );
    assert_eq!(model.deletes.len(), 2);
    assert_eq!(model.removed[0].replica_id, ReplicaId::new(2));
}

#[test]
fn evaluator_scale_down_missing_frozen_preparation_does_not_reprepare_or_replace_boundary() {
    use scale_down_model::{Model, reason};
    let mut model = Model::new(&[1, 2, 3], 1, 2);
    model.until(|m| {
        m.snapshot
            .status
            .transition
            .as_ref()
            .is_some_and(|t| t.secondary_removal_evidence.is_some())
    });
    let frozen = model.snapshot.status.transition.clone();
    model.report(1).prepared_secondary_removal = None;
    assert_eq!(reason(&model.plan()), "ScaleDownPreparationPending");
    model.step();
    assert_eq!(model.snapshot.status.transition, frozen);
    assert!(model.deletes.is_empty());
}

#[test]
fn evaluator_scale_down_invalid_new_desired_count_cannot_cancel_frozen_authority() {
    use scale_down_model::Model;
    let mut trace = Model::new(&[1, 2], 1, 1);
    trace.step();
    for _ in 0..50 {
        if trace.snapshot.status.transition.is_none()
            && trace.snapshot.status.secondary_scale_down_cleanup.is_none()
        {
            break;
        }
        let mut edited = trace.clone();
        edited.snapshot.desired.replicas = 0;
        edited.snapshot.desired.generation += 1;
        assert!(!matches!(edited.plan(), Plan::Unsafe { .. }));
        trace.step();
    }
    trace.snapshot.desired.replicas = 0;
    assert!(matches!(
        trace.plan(),
        Plan::Unsafe {
            reason: UnsafeReason::InvalidDesiredState(_),
            ..
        }
    ));
}

#[test]
fn evaluator_scale_down_existing_transitions_and_provisioning_serialize() {
    use scale_down_model::{Model, config};
    let mut snapshots = vec![prepared_switchover_snapshot()];
    let mut bootstrap = scaffolded_snapshot();
    let Plan::Apply { changes } = evaluate(&bootstrap, &EvaluationConfig::default()) else {
        panic!("bootstrap")
    };
    for change in changes {
        if let KubernetesChange::PersistStatus { status } = change {
            bootstrap.status = *status;
        }
    }
    assert!(bootstrap.status.transition.is_some());
    snapshots.push(bootstrap);
    for kind in [TransitionKind::Replacement, TransitionKind::Failover] {
        let mut model = Model::new(&[1, 2, 3], 1, 2);
        let previous = model
            .snapshot
            .status
            .topology
            .as_ref()
            .unwrap()
            .configuration
            .clone();
        let mut members = previous.members.clone();
        if kind == TransitionKind::Replacement {
            members[2].identity.instance_id = ReplicaInstanceId::new("replacement");
            members[2].identity.agent_generation = AgentGeneration::new("replacement-generation");
        } else {
            members[0].role = ReplicaRole::ActiveSecondary;
            members[1].role = ReplicaRole::Primary;
        }
        let current = ConfigurationDescriptor::new(
            Epoch::new(2, 11),
            ReplicaId::new(if kind == TransitionKind::Replacement {
                1
            } else {
                2
            }),
            members,
            2,
        );
        model.snapshot.status.transition = Some(TransitionIntent {
            transition_id: derive_transition_id(
                &model.snapshot.resource_uid,
                kind,
                &current.configuration_id,
            ),
            kind,
            spec_generation: 6,
            effective_policy: model.snapshot.status.effective_policy.clone().unwrap(),
            previous_configuration_id: Some(previous.configuration_id),
            current_configuration: current,
            election_lsn: (kind == TransitionKind::Failover).then_some(10),
            build_id: (kind == TransitionKind::Replacement).then(|| OperationId::new("build")),
            repair: None,
            switchover: None,
            secondary_scale_down: None,
            secondary_removal_evidence: None,
            scale_up: None,
            scale_up_failover: None,
        });
        snapshots.push(model.snapshot);
    }
    let mut model = Model::new(&[1, 2, 3], 1, 2);
    let replacing = model
        .snapshot
        .status
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .members[2]
        .identity
        .clone();
    model.snapshot.status.provisioning = Some(ProvisioningIntent {
        purpose: ProvisioningPurpose::replacement(replacing),
        operation_id: OperationId::new("existing-provisioning"),
        pod_uid: PodUid::new("new-pod"),
        pvc_uid: PvcUid::new("new-pvc"),
    });
    snapshots.push(model.snapshot);
    for mut snapshot in snapshots {
        snapshot.desired.replicas = 1;
        kuberic_protocol::validation::validate_snapshot(&snapshot).unwrap();
        for _ in 0..3 {
            if snapshot.status.transition.is_none() && snapshot.status.provisioning.is_none() {
                break;
            }
            let default = evaluate(&snapshot, &EvaluationConfig::default());
            assert_eq!(
                evaluate(&snapshot, &config()),
                default,
                "existing work retains precedence"
            );
            if let Plan::Apply { changes } = default {
                for change in changes {
                    if let KubernetesChange::PersistStatus { status } = change {
                        snapshot.status = *status;
                    }
                }
            }
        }
    }
}

#[test]
fn evaluator_scale_down_commit_publication_is_exact_and_separate_from_grant() {
    use kuberic_protocol::validation::validate_accept_secondary_removal_commit;
    use scale_down_model::Model;
    let mut model = Model::new(&[1, 2], 1, 1);
    model.until(|m| m.snapshot.status.secondary_scale_down_cleanup.is_some());
    let Plan::Execute {
        command: ProtocolCommand::AcceptSecondaryRemovalCommit(command),
    } = model.plan()
    else {
        panic!("publish accepted evidence first")
    };
    validate_accept_secondary_removal_commit(&command).unwrap();
    let mut invalid = command.as_ref().clone();
    invalid.target = invalid.committed.evidence.preparation.intent.target.clone();
    assert!(validate_accept_secondary_removal_commit(&invalid).is_err());
    invalid = command.as_ref().clone();
    invalid.operation_id = OperationId::new("mutated");
    assert!(validate_accept_secondary_removal_commit(&invalid).is_err());
    model.step();
    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(grant),
    } = model.plan()
    else {
        panic!("separate grant")
    };
    assert_eq!(grant.primary_write_status, AccessStatus::Granted);
    assert!(grant.secondary_removal_evidence.is_none());
    assert!(model.snapshot.routing.write_target.is_none());
}

#[test]
fn evaluator_scale_down_local_commit_cannot_precede_status_commit_or_change_proof() {
    use scale_down_model::Model;
    let mut model = Model::new(&[1, 2], 1, 1);
    model.until(|m| m.snapshot.status.secondary_scale_down_cleanup.is_some());
    let committed = model
        .snapshot
        .status
        .secondary_scale_down_cleanup
        .clone()
        .unwrap();
    let mut premature = model.clone();
    premature.snapshot.status.topology = Some(AcceptedTopology {
        configuration: committed
            .evidence
            .preparation
            .intent
            .previous_configuration
            .clone(),
    });
    premature.snapshot.status.effective_policy = Some(
        committed
            .evidence
            .preparation
            .intent
            .previous_policy
            .clone(),
    );
    premature.snapshot.status.transition = Some(scale_down_fixture::transition(
        &committed.evidence.preparation.intent,
    ));
    premature
        .snapshot
        .status
        .transition
        .as_mut()
        .unwrap()
        .secondary_removal_evidence = Some(committed.evidence.clone());
    premature.snapshot.status.secondary_scale_down_cleanup = None;
    premature.report(1).prepared_secondary_removal = None;
    premature.report(1).accepted_secondary_removal = Some(committed);
    assert!(matches!(premature.plan(), Plan::Unsafe { .. }));
    model.step();
    model
        .report(1)
        .accepted_secondary_removal
        .as_mut()
        .unwrap()
        .current_only_write_quorum[0]
        .report_sequence += 1;
    assert!(matches!(model.plan(), Plan::Unsafe { .. }));
}

use kuberic_protocol::command::{
    KubernetesChange, ProtocolCommand, SafetyChange, ScaleDownResource,
};
use kuberic_protocol::evaluator::{EvaluationConfig, evaluate};
use kuberic_protocol::observation::{
    AgentBuildReport, AgentObservation, AgentReport, DesiredState, KubernetesReplicaObservation,
    ObservationFailure, ObservationSnapshot, ReplicaObservation, ReplicaObservationKey,
    ReportWatermark, RoutingObservation, UninitializedAgentObservation,
};
use kuberic_protocol::plan::{Plan, UnsafeReason, WaitReason};
use kuberic_protocol::types::{
    AcceptedStatus, AcceptedTopology, AccessStatus, AgentGeneration, ConfigurationDescriptor,
    ConfigurationMember, EffectivePolicy, Epoch, FaultType, OperationId, PlannedSwitchoverIntent,
    PlannedSwitchoverOutcome, PlannedSwitchoverReceipt, PlannedSwitchoverRequest,
    PlannedSwitchoverResolution, PodUid, PrimaryFailureObservation, ProcessSessionId,
    ProvisioningIntent, ProvisioningPurpose, PvcUid, QuorumLossObservation, ReplicaId,
    ReplicaIdentity, ReplicaInstanceId, ReplicaRepairIntent, ReplicaRole, ResourceUid,
    ScaleUpIntent, SwitchoverHandoff, SwitchoverRequestId, TransitionIntent, TransitionKind,
    derive_agent_generation, derive_initialization_id, derive_switchover_preparation_operation_id,
    derive_transition_id,
};
use kuberic_protocol::validation::{
    ValidationError, validate_configuration, validate_status, validate_transition_relationship,
};

fn identity(id: i64, instance: &str, generation: &str) -> ReplicaIdentity {
    ReplicaIdentity {
        replica_id: ReplicaId::new(id),
        instance_id: ReplicaInstanceId::new(instance),
        agent_generation: AgentGeneration::new(generation),
    }
}

fn observation_key(id: i64, instance: &str) -> ReplicaObservationKey {
    ReplicaObservationKey::new(ReplicaId::new(id), ReplicaInstanceId::new(instance))
}

fn member(id: i64, role: ReplicaRole) -> ConfigurationMember {
    ConfigurationMember {
        identity: identity(id, &format!("pod-{id}"), &format!("generation-{id}")),
        role,
    }
}

fn configuration() -> ConfigurationDescriptor {
    ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        ReplicaId::new(1),
        vec![
            member(1, ReplicaRole::Primary),
            member(2, ReplicaRole::ActiveSecondary),
            member(3, ReplicaRole::ActiveSecondary),
        ],
        2,
    )
}

fn replacement_configuration(replica_id: i64) -> ConfigurationDescriptor {
    let mut configuration = configuration();
    let target = &mut configuration
        .members
        .iter_mut()
        .find(|m| m.identity.replica_id == ReplicaId::new(replica_id))
        .unwrap()
        .identity;
    target.agent_generation = derive_agent_generation(&derive_initialization_id(
        &ResourceUid::new("resource-uid"),
        target.replica_id,
        &PodUid::new(target.instance_id.as_str()),
        &PvcUid::new("old-pvc"),
    ));
    ConfigurationDescriptor::new(
        configuration.epoch,
        configuration.primary_id,
        configuration.members,
        configuration.write_quorum,
    )
}

fn freeze_replacement_fixture(snapshot: &mut ObservationSnapshot, target: &ReplicaIdentity) {
    let exact = replacement_resources(&snapshot.resource_uid, target);
    let cleanup = kuberic_protocol::types::ReplacementCleanup {
        resource_uid: snapshot.resource_uid.clone(),
        target: target.clone(),
        resources: exact.identity,
    };
    if let Some(p) = &mut snapshot.status.provisioning {
        p.operation_id = cleanup.provisioning_operation_id(&p.pod_uid, &p.pvc_uid);
    }
    if let Some(t) = &mut snapshot.status.transition {
        t.transition_id = cleanup.transition_id(t.kind, &t.current_configuration.configuration_id);
    }
    snapshot.status.pending_replacement_cleanup = Some(cleanup);
}

#[test]
fn replacement_provenance_binds_ids_status_and_legacy_json_without_granting_cleanup() {
    use kuberic_protocol::types::CleanupResourceIdentity;
    let accepted = replacement_configuration(2);
    let old = accepted.members[1].identity.clone();
    let mut snapshot = empty_snapshot(3);
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: EffectivePolicy::fixed(3, 10),
        topology: Some(AcceptedTopology {
            configuration: accepted,
        }),
        provisioning: Some(ProvisioningIntent {
            purpose: ProvisioningPurpose::replacement(old.clone()),
            pod_uid: PodUid::new("new-pod"),
            pvc_uid: PvcUid::new("new-pvc"),
            operation_id: OperationId::new("unused"),
        }),
        ..Default::default()
    };
    freeze_replacement_fixture(&mut snapshot, &old);
    assert_eq!(validate_status(&snapshot.status), Ok(()));
    let receipt = snapshot.status.pending_replacement_cleanup.clone().unwrap();
    for mutation in 0..9 {
        let mut changed = snapshot.clone();
        let frozen = changed.status.pending_replacement_cleanup.as_mut().unwrap();
        match mutation {
            0 => frozen.resource_uid = ResourceUid::new("another-set"),
            1 => frozen.target.instance_id = ReplicaInstanceId::new("another-pod"),
            2 => {
                frozen.resources.pvc = CleanupResourceIdentity::Present {
                    name: "other".into(),
                    uid: "another-pvc".into(),
                }
            }
            3 => frozen.resources.pod = CleanupResourceIdentity::Absent { name: "old".into() },
            4 => {
                frozen.resources.pvc = CleanupResourceIdentity::Absent {
                    name: "old-data".into(),
                }
            }
            5 => {
                frozen.resources.endpoint = CleanupResourceIdentity::Present {
                    name: frozen.resources.endpoint.name().into(),
                    uid: "changed-endpoint".into(),
                }
            }
            6 => {
                frozen.resources.endpoint = CleanupResourceIdentity::Absent {
                    name: frozen.resources.endpoint.name().into(),
                }
            }
            7 => {
                changed.status.provisioning.as_mut().unwrap().purpose =
                    ProvisioningPurpose::replacement(identity(3, "other", "other"))
            }
            _ => changed.status.last_replacement = Some(receipt.clone()),
        }
        assert!(
            kuberic_protocol::validation::validate_snapshot(&changed).is_err(),
            "mutation={mutation}"
        );
    }
    let configuration_id = &snapshot
        .status
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .configuration_id;
    let original = receipt.transition_id(TransitionKind::Replacement, configuration_id);
    let mut changed = receipt;
    changed.resources.endpoint = CleanupResourceIdentity::Absent {
        name: changed.resources.endpoint.name().into(),
    };
    assert_ne!(
        changed.transition_id(TransitionKind::Replacement, configuration_id),
        original
    );
    let mut json = serde_json::to_value(&snapshot.status).unwrap();
    json.as_object_mut()
        .unwrap()
        .remove("pendingReplacementCleanup");
    snapshot.status = serde_json::from_value(json).unwrap();
    assert!(snapshot.status.pending_replacement_cleanup.is_none());
    assert!(
        matches!(
            evaluate(&snapshot, &EvaluationConfig::default()),
            Plan::Wait { .. }
        ),
        "legacy in-flight intent without pre-admission provenance must fail closed"
    );
}

fn desired(replicas: u32) -> DesiredState {
    DesiredState {
        generation: 1,
        replicas,
        image: "example:v1".to_string(),
        failover_delay_seconds: 10,
        switchover: None,
    }
}

fn empty_snapshot(replicas: u32) -> ObservationSnapshot {
    ObservationSnapshot {
        secondary_scale_down_resources: Vec::new(),
        resource_uid: ResourceUid::new("resource-uid"),
        resource_version: "1".to_string(),
        desired: desired(replicas),
        status: AcceptedStatus::default(),
        replicas: BTreeMap::new(),
        previous_report_watermarks: BTreeMap::new(),
        durable_storage_evidence: false,
        supporting_resources_ready: true,
        routing: RoutingObservation::default(),
        observation_failures: Vec::new(),
        now_unix_seconds: 100,
    }
}

#[test]
fn persisted_scale_up_authority_waits_without_dispatch_and_binds_resource_uid() {
    let previous_policy = EffectivePolicy::fixed(1, 10).unwrap();
    let current_policy = EffectivePolicy::fixed(2, 10).unwrap();
    let primary = identity(1, "pod-1", "generation-1");
    let target = identity(2, "pod-2", "generation-2");
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        primary.replica_id,
        vec![ConfigurationMember {
            identity: primary.clone(),
            role: ReplicaRole::Primary,
        }],
        previous_policy.write_quorum,
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        primary.replica_id,
        vec![
            ConfigurationMember {
                identity: primary.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: target.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        current_policy.write_quorum,
    );
    let mut intent = ScaleUpIntent {
        operation_id: OperationId::default(),
        resource_uid: ResourceUid::new("resource-uid"),
        spec_generation: 2,
        desired_replicas: 2,
        previous_configuration: previous.clone(),
        current_configuration: current.clone(),
        previous_policy: previous_policy.clone(),
        current_policy: current_policy.clone(),
        primary,
        target,
        build_id: OperationId::new("build"),
        snapshot_boundary_lsn: 0,
        catch_up_boundary_lsn: 0,
    };
    intent.operation_id = intent.expected_operation_id();
    let mut snapshot = empty_snapshot(2);
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(previous_policy),
        topology: Some(AcceptedTopology {
            configuration: previous,
        }),
        transition: Some(TransitionIntent {
            transition_id: intent.transition_id(TransitionKind::ScaleUp, &current),
            kind: TransitionKind::ScaleUp,
            spec_generation: intent.spec_generation,
            effective_policy: current_policy,
            previous_configuration_id: Some(intent.previous_configuration.configuration_id.clone()),
            current_configuration: current,
            election_lsn: None,
            build_id: Some(intent.build_id.clone()),
            repair: None,
            switchover: None,
            secondary_scale_down: None,
            secondary_removal_evidence: None,
            scale_up: Some(Box::new(intent.clone())),
            scale_up_failover: None,
        }),
        ..Default::default()
    };
    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Unsafe {
            reason: UnsafeReason::InvalidAcceptedAuthority(_),
            ..
        }
    ));

    let mut different_attempt = intent.clone();
    different_attempt.build_id = OperationId::new("different-build");
    different_attempt.operation_id = different_attempt.expected_operation_id();
    let retained_operation_id = different_attempt.command_operation_id(
        kuberic_protocol::types::ScaleUpStage::PreviousCurrent,
        &different_attempt.primary,
        &different_attempt.current_configuration,
    );
    snapshot.replicas.insert(
        observation_key(1, "pod-1"),
        ReplicaObservation {
            kubernetes: None,
            agent: AgentObservation::Report(Box::new(AgentReport {
                protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                resource_uid: snapshot.resource_uid.clone(),
                identity: different_attempt.primary.clone(),
                process_session_id: ProcessSessionId::new("session"),
                report_sequence: 1,
                role: ReplicaRole::Primary,
                read_status: AccessStatus::Granted,
                write_status: AccessStatus::Granted,
                healthy: true,
                epoch: different_attempt.current_configuration.epoch,
                previous_configuration: Some(different_attempt.previous_configuration.clone()),
                current_configuration: Some(different_attempt.current_configuration.clone()),
                current_progress: 0,
                verified_replication_lsn: Some(0),
                committed_lsn: 0,
                retained_operation_id: Some(retained_operation_id),
                scale_up_intent: Some(Box::new(different_attempt)),
                ..Default::default()
            })),
        },
    );
    assert!(kuberic_protocol::validation::validate_snapshot(&snapshot).is_err());
    snapshot.replicas.clear();

    snapshot.resource_uid = ResourceUid::new("other");
    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Unsafe { .. }
    ));
}

#[test]
fn secondary_removal_rejects_mixed_scale_up_transition_evidence() {
    use scale_down_model::Model;
    let mut model = Model::new(&[1, 2], 1, 1);
    model.until(|model| {
        model
            .snapshot
            .status
            .transition
            .as_ref()
            .is_some_and(|transition| transition.kind == TransitionKind::SecondaryScaleDown)
    });

    let previous_policy = EffectivePolicy::fixed(1, 10).unwrap();
    let current_policy = EffectivePolicy::fixed(2, 10).unwrap();
    let primary = identity(1, "pod-1", "generation-1");
    let target = identity(2, "pod-2", "generation-2");
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        primary.replica_id,
        vec![ConfigurationMember {
            identity: primary.clone(),
            role: ReplicaRole::Primary,
        }],
        previous_policy.write_quorum,
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        primary.replica_id,
        vec![
            ConfigurationMember {
                identity: primary.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: target.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        current_policy.write_quorum,
    );
    let mut scale_up = ScaleUpIntent {
        operation_id: OperationId::default(),
        resource_uid: model.snapshot.resource_uid.clone(),
        spec_generation: 2,
        desired_replicas: 2,
        previous_configuration: previous,
        current_configuration: current,
        previous_policy,
        current_policy,
        primary,
        target,
        build_id: OperationId::new("build"),
        snapshot_boundary_lsn: 0,
        catch_up_boundary_lsn: 0,
    };
    scale_up.operation_id = scale_up.expected_operation_id();
    model.snapshot.status.transition.as_mut().unwrap().scale_up = Some(Box::new(scale_up));

    assert!(kuberic_protocol::validation::validate_status(&model.snapshot.status).is_err());
    assert!(matches!(
        evaluate(&model.snapshot, &scale_down_model::config()),
        Plan::Unsafe { .. }
    ));
}

fn scaffolded_snapshot() -> ObservationSnapshot {
    let mut snapshot = empty_snapshot(3);
    for id in 1..=3 {
        let replica_id = ReplicaId::new(id);
        snapshot.replicas.insert(
            observation_key(id, &format!("pod-uid-{id}")),
            ReplicaObservation {
                kubernetes: Some(KubernetesReplicaObservation {
                    replica_id,
                    pod_name: format!("example-{id}"),
                    pod_uid: Some(PodUid::new(format!("pod-uid-{id}"))),
                    pvc_name: format!("example-{id}"),
                    pvc_uid: Some(PvcUid::new(format!("pvc-uid-{id}"))),
                    image: Some("example:v1".to_string()),
                    pod_ready: true,
                    peer_endpoint_ready: true,
                }),
                agent: AgentObservation::Uninitialized(UninitializedAgentObservation {
                    protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                    resource_uid: snapshot.resource_uid.clone(),
                    replica_id,
                    pod_uid: PodUid::new(format!("pod-uid-{id}")),
                    pvc_uid: PvcUid::new(format!("pvc-uid-{id}")),
                    process_session_id: ProcessSessionId::new(format!("session-{id}")),
                    report_sequence: 1,
                }),
            },
        );
    }
    snapshot.routing.service_present = true;
    snapshot
}

fn attest_stable_topology(
    snapshot: &mut ObservationSnapshot,
    configuration: &ConfigurationDescriptor,
) {
    for member in &configuration.members {
        snapshot.replicas.insert(
            ReplicaObservationKey::new(
                member.identity.replica_id,
                member.identity.instance_id.clone(),
            ),
            ReplicaObservation {
                kubernetes: Some(KubernetesReplicaObservation {
                    replica_id: member.identity.replica_id,
                    pod_name: format!("pod-{}", member.identity.replica_id),
                    pod_uid: Some(PodUid::new(member.identity.instance_id.as_str())),
                    pvc_name: format!("pvc-{}", member.identity.replica_id),
                    pvc_uid: Some(PvcUid::new(format!("pvc-{}", member.identity.replica_id))),
                    image: Some("example:v1".to_string()),
                    pod_ready: true,
                    peer_endpoint_ready: true,
                }),
                agent: AgentObservation::Report(Box::new(AgentReport {
                    protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                    resource_uid: snapshot.resource_uid.clone(),
                    identity: member.identity.clone(),
                    process_session_id: ProcessSessionId::new(format!(
                        "session-{}",
                        member.identity.replica_id
                    )),
                    report_sequence: 1,
                    role: member.role,
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
                    committed_lsn: 10,
                    catch_up_capability: Some(10),
                    ..AgentReport::default()
                })),
            },
        );
    }
    snapshot.routing.write_target = configuration
        .members
        .iter()
        .find(|member| member.identity.replica_id == configuration.primary_id)
        .map(|member| member.identity.clone());
    snapshot.routing.service_present = true;
    snapshot.routing.unresolved_write_target = false;
}

#[test]
fn configuration_id_is_canonical_across_member_order() {
    let first = configuration();
    let second = ConfigurationDescriptor::new(
        first.epoch,
        first.primary_id,
        vec![
            member(3, ReplicaRole::ActiveSecondary),
            member(1, ReplicaRole::Primary),
            member(2, ReplicaRole::ActiveSecondary),
        ],
        first.write_quorum,
    );

    assert_eq!(first.configuration_id, second.configuration_id);
    assert_eq!(first.members, second.members);
}

#[test]
fn secondary_scale_down_validates_independent_majorities_and_minimum_evidence() {
    use kuberic_protocol::validation::*;
    for (size, old_write, old_read, new_write, new_read) in [
        (2, 2, 1, 1, 1),
        (3, 2, 2, 2, 1),
        (4, 3, 2, 2, 2),
        (5, 3, 3, 3, 2),
    ] {
        let intent = scale_down_fixture::intent(&(1..=size).collect::<Vec<_>>(), 1);
        assert_eq!(
            (
                intent.previous_policy.write_quorum,
                intent.previous_policy.read_quorum
            ),
            (old_write, old_read)
        );
        assert_eq!(
            (
                intent.current_policy.write_quorum,
                intent.current_policy.read_quorum
            ),
            (new_write, new_read)
        );
        validate_secondary_scale_down(&intent).unwrap();
        let mut evidence = scale_down_fixture::evidence(&intent);
        evidence.previous_read_quorum.truncate(old_read as usize);
        evidence.reduced_write_quorum.truncate(new_write as usize);
        validate_secondary_removal_evidence(&evidence, true).unwrap();
        for current_only in [false, true] {
            validate_secondary_removal_configuration(&scale_down_fixture::configuration_command(
                &intent,
                current_only,
            ))
            .unwrap();
        }
        let mut missing_old = evidence.clone();
        missing_old.previous_read_quorum.pop();
        assert!(validate_secondary_removal_evidence(&missing_old, false).is_err());
        evidence.reduced_write_quorum.pop();
        assert!(validate_secondary_removal_evidence(&evidence, true).is_err());
    }
}

#[test]
fn secondary_scale_down_preserves_holes_and_high_id_primary() {
    let intent = scale_down_fixture::intent(&[2, 8, 19, 40], 40);
    kuberic_protocol::validation::validate_secondary_scale_down(&intent).unwrap();
    assert_eq!(intent.target.replica_id, ReplicaId::new(19));
    assert_eq!(intent.current_configuration.primary_id, ReplicaId::new(40));
    assert_eq!(
        intent
            .current_configuration
            .members
            .iter()
            .map(|member| member.identity.replica_id.value())
            .collect::<Vec<_>>(),
        [2, 8, 40]
    );
}

#[test]
fn secondary_scale_down_rejects_arbitrary_authority_changes() {
    use kuberic_protocol::types::SecondaryScaleDownIntent;
    use kuberic_protocol::validation::validate_secondary_scale_down;
    type IntentMutation = (&'static str, fn(&mut SecondaryScaleDownIntent));
    let mutations: &[IntentMutation] = &[
        ("primary target", |i| i.target = i.primary.clone()),
        ("non-highest target", |i| {
            i.target = i.previous_configuration.members[3].identity.clone();
            i.current_configuration.members = i
                .previous_configuration
                .members
                .iter()
                .filter(|m| m.identity != i.target)
                .cloned()
                .collect();
        }),
        ("batch removal", |i| {
            i.current_configuration.members.pop();
            i.current_policy = EffectivePolicy::fixed(3, 30).unwrap();
        }),
        ("retained incarnation", |i| {
            i.current_configuration.members[1].identity.instance_id =
                ReplicaInstanceId::new("replacement")
        }),
        ("retained generation", |i| {
            i.current_configuration.members[1].identity.agent_generation =
                AgentGeneration::new("replacement")
        }),
        ("primary changed", |i| {
            i.current_configuration.primary_id = ReplicaId::new(2);
            i.current_configuration.members[0].role = ReplicaRole::ActiveSecondary;
            i.current_configuration.members[1].role = ReplicaRole::Primary;
        }),
        ("data loss", |i| {
            i.current_configuration.epoch.data_loss_number += 1
        }),
        ("same epoch", |i| {
            i.current_configuration.epoch = i.previous_configuration.epoch
        }),
        ("delay", |i| i.current_policy.failover_delay_seconds += 1),
        ("old read quorum", |i| i.previous_policy.read_quorum -= 1),
        ("old write quorum", |i| i.previous_policy.write_quorum -= 1),
        ("new write quorum", |i| i.current_policy.write_quorum -= 1),
        ("new read quorum", |i| i.current_policy.read_quorum += 1),
        ("zero desired", |i| i.desired_replicas = 0),
        ("no reduction requested", |i| i.desired_replicas = 5),
        ("zero generation", |i| i.spec_generation = 0),
        ("missing resource", |i| {
            i.resource_uid = ResourceUid::default()
        }),
        ("missing identity", |i| {
            i.previous_configuration.members[1]
                .identity
                .agent_generation = AgentGeneration::default()
        }),
        ("idle target", |i| {
            i.previous_configuration.members[4].role = ReplicaRole::IdleSecondary
        }),
    ];
    for (name, mutate) in mutations {
        let mut intent = scale_down_fixture::intent(&[1, 2, 3, 4, 5], 1);
        mutate(&mut intent);
        intent.previous_configuration.configuration_id =
            intent.previous_configuration.expected_id();
        intent.current_configuration.configuration_id = intent.current_configuration.expected_id();
        intent.operation_id = intent.expected_operation_id();
        assert!(validate_secondary_scale_down(&intent).is_err(), "{name}");
    }
    let intent = scale_down_fixture::intent(&[1, 2, 3], 1);
    for kind in [
        TransitionKind::Bootstrap,
        TransitionKind::Replacement,
        TransitionKind::Failover,
        TransitionKind::PlannedSwitchover,
        TransitionKind::SecondaryScaleDown,
    ] {
        assert!(
            validate_transition_relationship(
                kind,
                Some(&intent.previous_configuration),
                &intent.current_configuration,
                &intent.current_policy
            )
            .is_err(),
            "{kind:?} must not bypass typed dual-policy authority"
        );
    }
}

#[test]
fn secondary_scale_down_ids_bind_request_authority_target_and_command_stage() {
    use kuberic_protocol::types::SecondaryRemovalStage::*;
    let intent = scale_down_fixture::intent(&[1, 2, 3], 1);
    assert_eq!(intent.operation_id, intent.expected_operation_id());
    let mut reversed = intent.clone();
    reversed.previous_configuration.members.reverse();
    assert_eq!(
        intent.expected_operation_id(),
        reversed.expected_operation_id()
    );
    for change in 0..7 {
        let mut other = intent.clone();
        match change {
            0 => other.resource_uid = ResourceUid::new("other"),
            1 => other.spec_generation += 1,
            2 => other.desired_replicas += 1,
            3 => {
                other.previous_configuration.configuration_id =
                    kuberic_protocol::types::ConfigurationId::new("other")
            }
            4 => {
                other.current_configuration.configuration_id =
                    kuberic_protocol::types::ConfigurationId::new("other")
            }
            5 => other.target.instance_id = ReplicaInstanceId::new("other"),
            _ => other.target.agent_generation = AgentGeneration::new("other"),
        }
        assert_ne!(
            intent.expected_operation_id(),
            other.expected_operation_id()
        );
    }
    let ids = [Prepare, PreviousCurrent, CurrentOnly, Retire]
        .map(|stage| intent.command_operation_id(stage, &intent.primary));
    assert_eq!(
        ids.into_iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        4
    );
    assert_ne!(
        intent.command_operation_id(CurrentOnly, &intent.primary),
        intent.command_operation_id(CurrentOnly, &intent.target)
    );
}

#[test]
fn secondary_removal_evidence_rejects_stale_unbound_or_excluded_credit() {
    use kuberic_protocol::validation::*;
    let intent = scale_down_fixture::intent(&[1, 2, 3, 4, 5], 1);
    for mutation in 0..13 {
        let mut evidence = scale_down_fixture::evidence(&intent);
        match mutation {
            0 => evidence.previous_read_quorum[0].identity = intent.target.clone(),
            1 => evidence.previous_read_quorum[1] = evidence.previous_read_quorum[0].clone(),
            2 => evidence.previous_read_quorum[0].epoch.configuration_number -= 1,
            3 => evidence.previous_read_quorum[0].process_session_id = ProcessSessionId::default(),
            4 => evidence.previous_read_quorum[0].report_sequence = 0,
            5 => evidence.reduced_write_quorum[0].verified_replication_lsn = 9,
            6 => {
                evidence.reduced_write_quorum.remove(0);
            }
            7 => {
                evidence.reduced_write_quorum[0].pending_operation_id =
                    Some(OperationId::new("pending"))
            }
            8 => evidence.reduced_write_quorum[0].retained_operation_id = None,
            9 => evidence.reduced_write_quorum[0].write_status = AccessStatus::Granted,
            10 => evidence.preparation.operation_id = OperationId::new("wrong"),
            11 => evidence.reduced_write_quorum[0].report_sequence = 2,
            _ => evidence.preparation.boundary_lsn = -1,
        }
        assert!(
            validate_secondary_removal_evidence(&evidence, true).is_err(),
            "mutation {mutation}"
        );
    }
    let mut command = scale_down_fixture::configuration_command(&intent, false);
    command.previous_policy = None;
    assert!(validate_secondary_removal_configuration(&command).is_err());
    command.previous_policy = Some(intent.previous_policy.clone());
    command.primary_write_status = AccessStatus::Granted;
    assert!(validate_secondary_removal_configuration(&command).is_err());
}

#[test]
fn secondary_scale_down_status_separates_accepted_policy_intent_and_cleanup() {
    use kuberic_protocol::validation::*;
    let intent = scale_down_fixture::intent(&[1, 2], 1);
    let mut snapshot = empty_snapshot(1);
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(intent.previous_policy.clone()),
        topology: Some(AcceptedTopology {
            configuration: intent.previous_configuration.clone(),
        }),
        transition: Some(scale_down_fixture::transition(&intent)),
        ..Default::default()
    };
    validate_snapshot(&snapshot).unwrap();
    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Unsafe { .. }
    ));
    snapshot.status.effective_policy = Some(intent.current_policy.clone());
    assert!(validate_status(&snapshot.status).is_err());
    snapshot.status.topology = Some(AcceptedTopology {
        configuration: intent.current_configuration.clone(),
    });
    snapshot.status.secondary_scale_down_cleanup = Some(scale_down_fixture::cleanup(&intent));
    assert!(
        validate_status(&snapshot.status).is_err(),
        "cleanup cannot overlap active transition"
    );
    snapshot.status.transition = None;
    validate_snapshot(&snapshot).unwrap();
    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Unsafe { .. }
    ));
    let encoded = serde_json::to_string(&snapshot.status).unwrap();
    assert_eq!(
        serde_json::from_str::<AcceptedStatus>(&encoded).unwrap(),
        snapshot.status
    );
    snapshot.resource_uid = ResourceUid::new("replacement-resource");
    assert!(validate_snapshot(&snapshot).is_err());
}

#[test]
fn secondary_retirement_and_cleanup_reject_malformed_or_unbound_identity() {
    use kuberic_protocol::types::*;
    use kuberic_protocol::validation::*;
    let intent = scale_down_fixture::intent(&[1, 2, 3], 1);
    let mut cleanup = scale_down_fixture::cleanup(&intent);
    cleanup.retirement = Some(scale_down_fixture::retirement(&intent));
    validate_secondary_scale_down_cleanup(&cleanup).unwrap();
    for mutation in 0..7 {
        let mut invalid = cleanup.clone();
        let retirement = invalid.retirement.as_mut().unwrap();
        match mutation {
            0 => retirement.application_closed = false,
            1 => retirement.peers_fenced = false,
            2 => retirement.role = ReplicaRole::ActiveSecondary,
            3 => retirement.read_status = AccessStatus::Granted,
            4 => retirement.epoch.configuration_number -= 1,
            5 => {
                retirement.intent.cleanup.pvc = CleanupResourceIdentity::Absent {
                    name: "different-pvc".into(),
                }
            }
            _ => invalid.current_only_write_quorum[0].report_sequence = 3,
        }
        assert!(
            validate_secondary_scale_down_cleanup(&invalid).is_err(),
            "mutation {mutation}"
        );
    }
    for resource in 0..3 {
        let mut malformed = intent.clone();
        let field = match resource {
            0 => &mut malformed.cleanup.pod,
            1 => &mut malformed.cleanup.pvc,
            _ => &mut malformed.cleanup.endpoint,
        };
        *field = CleanupResourceIdentity::Present {
            name: field.name().into(),
            uid: String::new(),
        };
        assert!(validate_secondary_scale_down(&malformed).is_err());
    }
    let mut absent = intent;
    absent.cleanup.pod = CleanupResourceIdentity::Absent {
        name: absent.cleanup.pod.name().into(),
    };
    validate_secondary_scale_down(&absent).unwrap();
}

#[test]
fn existing_json_status_and_transition_default_new_authority_to_none() {
    let status: AcceptedStatus = serde_json::from_value(serde_json::json!({
        "initialized": false, "observedGeneration": 0, "conditions": []
    }))
    .unwrap();
    assert!(status.secondary_scale_down_cleanup.is_none());
    assert!(status.last_secondary_removal.is_none());
    assert!(
        serde_json::to_value(&status)
            .unwrap()
            .get("lastSecondaryRemoval")
            .is_none()
    );
    let mut value = serde_json::to_value(scale_down_fixture::transition(
        &scale_down_fixture::intent(&[1, 2], 1),
    ))
    .unwrap();
    value.as_object_mut().unwrap().remove("secondaryScaleDown");
    value
        .as_object_mut()
        .unwrap()
        .remove("secondaryRemovalEvidence");
    let transition: TransitionIntent = serde_json::from_value(value).unwrap();
    assert!(transition.secondary_scale_down.is_none());
    assert!(transition.secondary_removal_evidence.is_none());
    let mut status = status;
    status.transition = Some(transition);
    assert!(
        validate_status(&status).is_err(),
        "missing scale-down authority cannot default into admission"
    );
    let mut command = serde_json::to_value(scale_down_fixture::configuration_command(
        &scale_down_fixture::intent(&[1, 2], 1),
        false,
    ))
    .unwrap();
    command.as_object_mut().unwrap().remove("previousPolicy");
    command
        .as_object_mut()
        .unwrap()
        .remove("secondaryRemovalEvidence");
    let command: kuberic_protocol::command::EnsureConfiguration =
        serde_json::from_value(command).unwrap();
    assert!(command.previous_policy.is_none());
    assert!(command.secondary_removal_evidence.is_none());
}

#[test]
fn secondary_removal_reports_bind_frozen_authority_and_fresh_observation() {
    use kuberic_protocol::validation::*;
    let intent = scale_down_fixture::intent(&[1, 2, 3], 1);
    let mut snapshot = empty_snapshot(2);
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(intent.previous_policy.clone()),
        topology: Some(AcceptedTopology {
            configuration: intent.previous_configuration.clone(),
        }),
        transition: Some(scale_down_fixture::transition(&intent)),
        ..Default::default()
    };
    let report = AgentReport {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: intent.resource_uid.clone(),
        identity: intent.primary.clone(),
        process_session_id: ProcessSessionId::new("session-1"),
        report_sequence: 3,
        role: ReplicaRole::Primary,
        write_status: AccessStatus::ReconfigurationPending,
        epoch: intent.current_configuration.epoch,
        previous_configuration: Some(intent.previous_configuration.clone()),
        current_configuration: Some(intent.current_configuration.clone()),
        current_progress: 10,
        verified_replication_lsn: Some(10),
        secondary_removal_evidence: Some(scale_down_fixture::evidence(&intent)),
        ..Default::default()
    };
    let key = ReplicaObservationKey::new(
        intent.primary.replica_id,
        intent.primary.instance_id.clone(),
    );
    snapshot.replicas.insert(
        key.clone(),
        ReplicaObservation {
            kubernetes: None,
            agent: AgentObservation::Report(Box::new(report.clone())),
        },
    );
    validate_snapshot(&snapshot).unwrap();
    for mutation in 0..5 {
        let mut invalid = snapshot.clone();
        let AgentObservation::Report(report) = &mut invalid.replicas.get_mut(&key).unwrap().agent
        else {
            unreachable!()
        };
        match mutation {
            0 => report.secondary_removal_evidence = None,
            1 => report.write_status = AccessStatus::Granted,
            2 => {
                report
                    .secondary_removal_evidence
                    .as_mut()
                    .unwrap()
                    .preparation
                    .boundary_lsn = 9
            }
            3 => {
                report
                    .secondary_removal_evidence
                    .as_mut()
                    .unwrap()
                    .preparation
                    .intent
                    .cleanup
                    .pvc = kuberic_protocol::types::CleanupResourceIdentity::Absent {
                    name: "unbound".into(),
                }
            }
            _ => {
                invalid.status.transition = None;
            }
        }
        assert!(validate_snapshot(&invalid).is_err(), "mutation {mutation}");
    }
    let mut earlier_pc_cc = snapshot.clone();
    {
        let AgentObservation::Report(report) =
            &mut earlier_pc_cc.replicas.get_mut(&key).unwrap().agent
        else {
            unreachable!()
        };
        report
            .secondary_removal_evidence
            .as_mut()
            .unwrap()
            .reduced_write_quorum
            .clear();
    }
    validate_snapshot(&earlier_pc_cc).unwrap();
    {
        let AgentObservation::Report(report) =
            &mut earlier_pc_cc.replicas.get_mut(&key).unwrap().agent
        else {
            unreachable!()
        };
        report.previous_configuration = None;
    }
    assert!(validate_snapshot(&earlier_pc_cc).is_err());

    snapshot.previous_report_watermarks.insert(
        key,
        ReportWatermark {
            process_session_id: report.process_session_id,
            report_sequence: report.report_sequence,
        },
    );
    assert!(matches!(
        validate_snapshot(&snapshot),
        Err(ValidationError::StaleReportSequence { .. })
    ));

    let mut evidence = scale_down_fixture::evidence(&intent);
    evidence.previous_read_quorum[0].process_session_id =
        ProcessSessionId::new("fresh-restarted-primary");
    evidence.previous_read_quorum[0].report_sequence = 1;
    validate_secondary_removal_evidence(&evidence, true).unwrap();
    evidence.reduced_write_quorum[0].role = ReplicaRole::None;
    assert!(validate_secondary_removal_evidence(&evidence, true).is_err());
}

#[test]
fn secondary_removal_cleanup_keeps_reports_bound_to_frozen_admission_evidence() {
    use kuberic_protocol::validation::*;
    let intent = scale_down_fixture::intent(&[1, 2, 3], 1);
    let cleanup = scale_down_fixture::cleanup(&intent);
    let mut snapshot = empty_snapshot(2);
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(intent.current_policy.clone()),
        topology: Some(AcceptedTopology {
            configuration: intent.current_configuration.clone(),
        }),
        secondary_scale_down_cleanup: Some(cleanup.clone()),
        ..Default::default()
    };
    let report = AgentReport {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: intent.resource_uid.clone(),
        identity: intent.primary.clone(),
        process_session_id: ProcessSessionId::new("session-1"),
        report_sequence: 5,
        role: ReplicaRole::Primary,
        read_status: AccessStatus::Granted,
        write_status: AccessStatus::Granted,
        epoch: intent.current_configuration.epoch,
        current_configuration: Some(intent.current_configuration.clone()),
        current_progress: 10,
        verified_replication_lsn: Some(10),
        committed_lsn: 10,
        secondary_removal_evidence: Some(cleanup.evidence.clone()),
        ..Default::default()
    };
    let key = ReplicaObservationKey::new(
        intent.primary.replica_id,
        intent.primary.instance_id.clone(),
    );
    snapshot.replicas.insert(
        key.clone(),
        ReplicaObservation {
            kubernetes: None,
            agent: AgentObservation::Report(Box::new(report)),
        },
    );
    validate_snapshot(&snapshot).unwrap();

    let AgentObservation::Report(report) = &mut snapshot.replicas.get_mut(&key).unwrap().agent
    else {
        unreachable!()
    };
    report
        .secondary_removal_evidence
        .as_mut()
        .unwrap()
        .preparation
        .boundary_lsn = 0;
    assert!(validate_snapshot(&snapshot).is_err());
}

#[test]
fn secondary_removal_current_only_freshness_covers_rotating_quorum_witnesses() {
    use kuberic_protocol::validation::*;
    let intent = scale_down_fixture::intent(&[1, 2, 3, 4], 1);
    let member = |id| {
        intent
            .current_configuration
            .members
            .iter()
            .find(|member| member.identity.replica_id == ReplicaId::new(id))
            .unwrap()
            .identity
            .clone()
    };
    let mut cleanup = scale_down_fixture::cleanup(&intent);
    cleanup
        .evidence
        .previous_read_quorum
        .retain(|witness| [member(1), member(3)].contains(&witness.identity));
    cleanup
        .evidence
        .reduced_write_quorum
        .retain(|witness| [member(1), member(2)].contains(&witness.identity));
    cleanup
        .current_only_write_quorum
        .retain(|witness| [member(1), member(3)].contains(&witness.identity));
    cleanup
        .evidence
        .previous_read_quorum
        .iter_mut()
        .find(|witness| witness.identity == member(3))
        .unwrap()
        .report_sequence = 5;
    cleanup
        .current_only_write_quorum
        .iter_mut()
        .find(|witness| witness.identity == member(3))
        .unwrap()
        .report_sequence = 4;

    validate_secondary_removal_evidence(&cleanup.evidence, true).unwrap();
    assert!(validate_secondary_scale_down_cleanup(&cleanup).is_err());
}

#[test]
fn configuration_json_derives_primary_and_flattens_member_identity() {
    let configuration = configuration();
    let value = serde_json::to_value(&configuration).unwrap();
    assert!(value.get("primaryId").is_none());
    let member = &value["members"][0];
    assert!(member.get("identity").is_none());
    assert_eq!(member["replicaId"], 1);
    assert!(member.get("instanceId").is_some());
    assert!(member.get("agentGeneration").is_some());
    assert_eq!(
        serde_json::from_value::<ConfigurationDescriptor>(value).unwrap(),
        configuration
    );

    let mut legacy = serde_json::json!({
        "configurationId": configuration.configuration_id,
        "epoch": configuration.epoch,
        "primaryId": configuration.primary_id,
        "members": configuration.members.iter().map(|member| serde_json::json!({
            "identity": member.identity,
            "role": member.role,
        })).collect::<Vec<_>>(),
        "writeQuorum": configuration.write_quorum,
    });
    assert_eq!(
        serde_json::from_value::<ConfigurationDescriptor>(legacy.clone()).unwrap(),
        configuration
    );
    legacy["primaryId"] = serde_json::json!(2);
    assert!(serde_json::from_value::<ConfigurationDescriptor>(legacy).is_err());
}

#[test]
fn bootstrap_recreates_fresh_scaffolding_with_drifted_image() {
    let mut snapshot = scaffolded_snapshot();
    snapshot
        .replicas
        .values_mut()
        .next()
        .unwrap()
        .kubernetes
        .as_mut()
        .unwrap()
        .image = Some("example:old".to_string());

    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Apply { changes }
            if matches!(
                changes.first(),
                Some(KubernetesChange::DeleteReplicaScaffolding { .. })
            )
    ));
}

#[test]
fn duplicate_logical_replica_id_is_rejected() {
    let configuration = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        ReplicaId::new(1),
        vec![
            member(1, ReplicaRole::Primary),
            ConfigurationMember {
                identity: identity(1, "other-pod", "other-generation"),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    );

    assert!(matches!(
        validate_configuration(&configuration, None),
        Err(ValidationError::DuplicateReplicaIds(ids)) if ids == vec![1]
    ));
}

#[test]
fn transition_relationship_rejects_epoch_regression_and_data_loss_change() {
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 5),
        ReplicaId::new(1),
        configuration().members,
        2,
    );
    let regressed = ConfigurationDescriptor::new(
        Epoch::new(0, 4),
        ReplicaId::new(1),
        previous.members.clone(),
        2,
    );
    let data_loss_changed = ConfigurationDescriptor::new(
        Epoch::new(1, 6),
        ReplicaId::new(1),
        previous.members.clone(),
        2,
    );
    let policy = EffectivePolicy::fixed(3, 10).unwrap();

    assert!(matches!(
        validate_transition_relationship(
            TransitionKind::Failover,
            Some(&previous),
            &regressed,
            &policy
        ),
        Err(ValidationError::TransitionEpochNotNewer)
    ));
    assert!(matches!(
        validate_transition_relationship(
            TransitionKind::Failover,
            Some(&previous),
            &data_loss_changed,
            &policy
        ),
        Err(ValidationError::TransitionDataLossChanged)
    ));
}

#[test]
fn replacement_relationship_requires_one_non_primary_incarnation_change() {
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 5),
        ReplicaId::new(1),
        configuration().members,
        2,
    );
    let mut replacement_members = previous.members.clone();
    replacement_members
        .iter_mut()
        .find(|member| member.identity.replica_id == ReplicaId::new(3))
        .unwrap()
        .identity = identity(3, "replacement-pod", "replacement-generation");
    let current =
        ConfigurationDescriptor::new(Epoch::new(0, 6), ReplicaId::new(1), replacement_members, 2);
    let policy = EffectivePolicy::fixed(3, 10).unwrap();

    assert!(
        validate_transition_relationship(
            TransitionKind::Replacement,
            Some(&previous),
            &current,
            &policy
        )
        .is_ok()
    );
}

#[test]
fn planned_switchover_relationship_preserves_exact_membership_and_changes_primary() {
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 5),
        ReplicaId::new(1),
        configuration().members,
        2,
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 6),
        ReplicaId::new(2),
        previous
            .members
            .iter()
            .cloned()
            .map(|mut member| {
                member.role = if member.identity.replica_id == ReplicaId::new(2) {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                };
                member
            })
            .collect(),
        2,
    );
    let policy = EffectivePolicy::fixed(3, 10).unwrap();

    assert!(
        validate_transition_relationship(
            TransitionKind::PlannedSwitchover,
            Some(&previous),
            &current,
            &policy
        )
        .is_ok()
    );

    let mut changed_members = current.members.clone();
    changed_members[2].identity = identity(3, "replacement-pod", "replacement-generation");
    let changed =
        ConfigurationDescriptor::new(Epoch::new(0, 6), ReplicaId::new(2), changed_members, 2);
    assert!(
        validate_transition_relationship(
            TransitionKind::PlannedSwitchover,
            Some(&previous),
            &changed,
            &policy
        )
        .is_err()
    );
}

#[test]
fn planned_switchover_status_binds_request_handoff_and_receipt() {
    let resource_uid = ResourceUid::new("resource-uid");
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 5),
        ReplicaId::new(1),
        configuration().members,
        2,
    );
    let source = previous.members[0].identity.clone();
    let target = previous.members[1].identity.clone();
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 6),
        target.replica_id,
        previous
            .members
            .iter()
            .cloned()
            .map(|mut member| {
                member.role = if member.identity == target {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                };
                member
            })
            .collect(),
        2,
    );
    let request_id = SwitchoverRequestId::new("request-1");
    let preparation_operation_id = derive_switchover_preparation_operation_id(
        &resource_uid,
        &request_id,
        2,
        &previous.configuration_id,
        &source,
        &target,
    );
    let handoff = SwitchoverHandoff {
        preparation_generation: 2,
        preparation_operation_id,
        request_id: request_id.clone(),
        source: source.clone(),
        target: target.clone(),
        starting_configuration_id: previous.configuration_id.clone(),
        starting_epoch: previous.epoch,
        handoff_lsn: 12,
    };
    let status = AcceptedStatus {
        initialized: true,
        observed_generation: 1,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: previous.clone(),
        }),
        transition: Some(TransitionIntent {
            secondary_scale_down: None,
            secondary_removal_evidence: None,
            scale_up: None,
            scale_up_failover: None,
            transition_id: derive_transition_id(
                &resource_uid,
                TransitionKind::PlannedSwitchover,
                &current.configuration_id,
            ),
            kind: TransitionKind::PlannedSwitchover,
            spec_generation: 2,
            effective_policy: EffectivePolicy::fixed(3, 10).unwrap(),
            previous_configuration_id: Some(previous.configuration_id.clone()),
            current_configuration: current.clone(),
            election_lsn: None,
            build_id: None,
            repair: None,
            switchover: Some(PlannedSwitchoverIntent {
                preparation_generation: 2,
                request_id: request_id.clone(),
                source,
                target: target.clone(),
                requested_configuration: current.clone(),
                resolution: PlannedSwitchoverResolution::RequestedTarget,
                handoff: Some(handoff),
            }),
        }),
        last_switchover: Some(PlannedSwitchoverReceipt {
            request_id: SwitchoverRequestId::new("request-0"),
            requested_target_replica_id: target.replica_id,
            accepted_target: Some(target.clone()),
            resulting_primary: Some(target),
            outcome: PlannedSwitchoverOutcome::RequestedTargetCompleted,
        }),
        ..AcceptedStatus::default()
    };

    assert!(validate_status(&status).is_ok());

    let mut same_epoch_compensation = status.clone();
    let transition = same_epoch_compensation.transition.as_mut().unwrap();
    transition.switchover.as_mut().unwrap().resolution =
        PlannedSwitchoverResolution::CompensatingOldPrimary;
    transition.current_configuration = ConfigurationDescriptor::new(
        transition.current_configuration.epoch,
        previous.primary_id,
        previous.members.clone(),
        previous.write_quorum,
    );
    assert!(matches!(
        validate_status(&same_epoch_compensation),
        Err(ValidationError::TransitionEpochNotNewer)
    ));

    let mut malformed_receipt = status;
    malformed_receipt.last_switchover = Some(PlannedSwitchoverReceipt {
        request_id: SwitchoverRequestId::new("request-malformed"),
        requested_target_replica_id: ReplicaId::new(2),
        accepted_target: Some(identity(2, "pod-2", "generation-2")),
        resulting_primary: Some(identity(-1, "", "")),
        outcome: PlannedSwitchoverOutcome::OldPrimaryCompensated,
    });
    assert!(matches!(
        validate_status(&malformed_receipt),
        Err(ValidationError::InvalidSwitchoverReceipt)
    ));
}

#[test]
fn switchover_preparation_id_is_deterministic_and_exact_target_bound() {
    let resource_uid = ResourceUid::new("resource-uid");
    let request_id = SwitchoverRequestId::new("request-1");
    let source = identity(1, "pod-1", "generation-1");
    let target = identity(2, "pod-2", "generation-2");
    let first = derive_switchover_preparation_operation_id(
        &resource_uid,
        &request_id,
        2,
        &configuration().configuration_id,
        &source,
        &target,
    );
    let second = derive_switchover_preparation_operation_id(
        &resource_uid,
        &request_id,
        2,
        &configuration().configuration_id,
        &source,
        &target,
    );
    let changed = derive_switchover_preparation_operation_id(
        &resource_uid,
        &request_id,
        2,
        &configuration().configuration_id,
        &source,
        &identity(2, "pod-2b", "generation-2b"),
    );

    assert_eq!(first, second);
    assert_ne!(first, changed);
    assert_ne!(
        first,
        derive_switchover_preparation_operation_id(
            &resource_uid,
            &request_id,
            3,
            &configuration().configuration_id,
            &source,
            &target
        )
    );
    assert_ne!(
        first,
        derive_switchover_preparation_operation_id(
            &resource_uid,
            &request_id,
            2,
            &kuberic_protocol::types::ConfigurationId::new("other-authority"),
            &source,
            &target
        )
    );
}

#[test]
fn unknown_switchover_target_does_not_mutate_accepted_authority() {
    let configuration = configuration();
    let policy = EffectivePolicy::fixed(3, 10).unwrap();
    let mut snapshot = empty_snapshot(3);
    snapshot.status = AcceptedStatus {
        initialized: true,
        observed_generation: 1,
        effective_policy: Some(policy),
        topology: Some(AcceptedTopology {
            configuration: configuration.clone(),
        }),
        ..AcceptedStatus::default()
    };
    snapshot.desired.switchover = Some(PlannedSwitchoverRequest {
        request_id: SwitchoverRequestId::new("request-1"),
        target_replica_id: ReplicaId::new(99),
    });
    attest_stable_topology(&mut snapshot, &configuration);

    let status = match evaluate(&snapshot, &EvaluationConfig::default()) {
        Plan::Apply { changes } => match changes.into_iter().next() {
            Some(KubernetesChange::PersistStatus { status }) => *status,
            _ => panic!("planned switchover request must only project status"),
        },
        Plan::Stable { status, .. } => status,
        other => panic!("planned switchover request must not become unsafe: {other:?}"),
    };
    assert_eq!(status.topology, snapshot.status.topology);
    assert!(status.conditions.iter().any(|condition| {
        condition.type_ == "SwitchoverRejected" && condition.reason == "TargetNotMember"
    }));
}

fn switchover_snapshot() -> ObservationSnapshot {
    let configuration = configuration();
    let mut snapshot = empty_snapshot(3);
    snapshot.status = AcceptedStatus {
        initialized: true,
        observed_generation: 1,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: configuration.clone(),
        }),
        ..AcceptedStatus::default()
    };
    snapshot.desired.generation = 2;
    snapshot.desired.switchover = Some(PlannedSwitchoverRequest {
        request_id: SwitchoverRequestId::new("move-1"),
        target_replica_id: ReplicaId::new(2),
    });
    attest_stable_topology(&mut snapshot, &configuration);
    for id in 1..=3 {
        switchover_report(&mut snapshot, id).verified_replication_lsn = Some(10);
    }
    snapshot
}

fn switchover_report(snapshot: &mut ObservationSnapshot, id: i64) -> &mut AgentReport {
    let AgentObservation::Report(report) = &mut snapshot
        .replicas
        .get_mut(&observation_key(id, &format!("pod-{id}")))
        .unwrap()
        .agent
    else {
        panic!("exact report")
    };
    report
}

fn apply_switchover_status(snapshot: &mut ObservationSnapshot) {
    let plan = evaluate(snapshot, &EvaluationConfig::default());
    let Plan::Apply { changes } = plan else {
        panic!("expected Apply: {plan:?}")
    };
    for change in changes {
        match change {
            KubernetesChange::PersistStatus { status } => {
                validate_status(&status).unwrap();
                snapshot.status = *status;
            }
            KubernetesChange::RemoveWriteRouting => {
                snapshot.routing.write_target = None;
                snapshot.routing.unresolved_write_target = false;
            }
            other => panic!("unexpected change: {other:?}"),
        }
    }
}

fn prepared_switchover_snapshot() -> ObservationSnapshot {
    let mut snapshot = switchover_snapshot();
    apply_switchover_status(&mut snapshot);
    apply_switchover_status(&mut snapshot);
    let Plan::Execute {
        command: ProtocolCommand::PrepareSwitchover(command),
    } = evaluate(&snapshot, &EvaluationConfig::default())
    else {
        panic!("preparation command")
    };
    let source = switchover_report(&mut snapshot, 1);
    source.write_status = AccessStatus::ReconfigurationPending;
    source.prepared_switchover = Some(SwitchoverHandoff {
        preparation_generation: command.preparation_generation,
        preparation_operation_id: command.operation_id,
        request_id: command.request_id,
        source: command.source,
        target: command.target,
        starting_configuration_id: command.current_configuration.configuration_id,
        starting_epoch: command.current_configuration.epoch,
        handoff_lsn: 10,
    });
    apply_switchover_status(&mut snapshot);
    snapshot
}

fn prepared_switchover_snapshot_with_size(replica_set_size: u32) -> ObservationSnapshot {
    let write_quorum = replica_set_size / 2 + 1;
    let configuration = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        ReplicaId::new(1),
        (1..=i64::from(replica_set_size))
            .map(|id| {
                member(
                    id,
                    if id == 1 {
                        ReplicaRole::Primary
                    } else {
                        ReplicaRole::ActiveSecondary
                    },
                )
            })
            .collect(),
        write_quorum,
    );
    let mut snapshot = empty_snapshot(replica_set_size);
    snapshot.status = AcceptedStatus {
        initialized: true,
        observed_generation: 1,
        effective_policy: Some(EffectivePolicy::fixed(replica_set_size, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: configuration.clone(),
        }),
        ..AcceptedStatus::default()
    };
    snapshot.desired.generation = 2;
    snapshot.desired.switchover = Some(PlannedSwitchoverRequest {
        request_id: SwitchoverRequestId::new(format!("move-{replica_set_size}")),
        target_replica_id: ReplicaId::new(2),
    });
    attest_stable_topology(&mut snapshot, &configuration);
    for id in 1..=i64::from(replica_set_size) {
        switchover_report(&mut snapshot, id).verified_replication_lsn = Some(10);
    }
    apply_switchover_status(&mut snapshot);
    apply_switchover_status(&mut snapshot);
    let Plan::Execute {
        command: ProtocolCommand::PrepareSwitchover(command),
    } = evaluate(&snapshot, &EvaluationConfig::default())
    else {
        panic!("preparation command")
    };
    let source = switchover_report(&mut snapshot, 1);
    source.write_status = AccessStatus::ReconfigurationPending;
    source.prepared_switchover = Some(SwitchoverHandoff {
        preparation_generation: command.preparation_generation,
        preparation_operation_id: command.operation_id,
        request_id: command.request_id,
        source: command.source,
        target: command.target,
        starting_configuration_id: command.current_configuration.configuration_id,
        starting_epoch: command.current_configuration.epoch,
        handoff_lsn: 10,
    });
    apply_switchover_status(&mut snapshot);
    snapshot
}

fn lose_switchover_pod(snapshot: &mut ObservationSnapshot, id: i64) {
    let observation = snapshot
        .replicas
        .get_mut(&observation_key(id, &format!("pod-{id}")))
        .unwrap();
    let pod = observation.kubernetes.as_mut().unwrap();
    pod.pod_uid = None;
    pod.pod_name.clear();
    pod.pod_ready = false;
    observation.agent = AgentObservation::Absent;
}

fn assert_switchover_safety_decision(snapshot: &ObservationSnapshot) {
    let plan = evaluate(snapshot, &EvaluationConfig::default());
    let Plan::Apply { changes } = plan else {
        panic!("expected safety convergence, not terminal Unsafe: {plan:?}")
    };
    assert!(matches!(
        changes.first(),
        Some(KubernetesChange::RemoveWriteRouting)
    ));
    let KubernetesChange::PersistStatus { status } = changes.last().unwrap() else {
        panic!("frozen failure")
    };
    assert_eq!(
        status
            .transition
            .as_ref()
            .unwrap()
            .switchover
            .as_ref()
            .unwrap()
            .resolution,
        PlannedSwitchoverResolution::Unsafe
    );
    assert!(status.last_switchover.is_none());
}

#[test]
fn switchover_target_loss_table_selects_restoration_compensation_or_forward_only() {
    for boundary in [
        "before-prepare",
        "prepared",
        "authority-admitted",
        "source-demoted",
        "target-role",
        "current-only",
        "accepted",
    ] {
        let mut snapshot = if boundary == "before-prepare" {
            let mut snapshot = switchover_snapshot();
            apply_switchover_status(&mut snapshot);
            apply_switchover_status(&mut snapshot);
            snapshot
        } else {
            prepared_switchover_snapshot()
        };
        let requested = snapshot
            .status
            .transition
            .as_ref()
            .unwrap()
            .current_configuration
            .clone();
        let starting = snapshot.status.topology.clone().unwrap().configuration;
        let count = match boundary {
            "authority-admitted" | "source-demoted" => 1,
            "target-role" => 3,
            "current-only" | "accepted" => 6,
            _ => 0,
        };
        for _ in 0..count {
            observe_switchover_command(&mut snapshot);
        }
        if boundary == "authority-admitted" {
            let source = switchover_report(&mut snapshot, 1);
            source.role = ReplicaRole::Primary;
            source.pending_operation_id = source.retained_operation_id.take();
        }
        if boundary == "accepted" {
            apply_switchover_status(&mut snapshot);
        }
        lose_switchover_pod(&mut snapshot, 2);
        if boundary == "accepted" {
            let receipt = snapshot.status.last_switchover.clone();
            apply_switchover_status(&mut snapshot);
            assert_eq!(
                snapshot.status.topology.as_ref().unwrap().configuration,
                requested
            );
            assert_eq!(snapshot.status.last_switchover, receipt);
            assert!(
                snapshot
                    .status
                    .transition
                    .as_ref()
                    .is_none_or(|transition| transition.kind != TransitionKind::PlannedSwitchover)
            );
            continue;
        }
        // Allocation/decision is status-only and replayable before any effect.
        let first = evaluate(&snapshot, &EvaluationConfig::default());
        assert_eq!(
            first,
            evaluate(&snapshot, &EvaluationConfig::default()),
            "{boundary}"
        );
        apply_switchover_status(&mut snapshot);
        let intent = snapshot
            .status
            .transition
            .as_ref()
            .unwrap()
            .switchover
            .as_ref()
            .unwrap();
        assert_eq!(intent.requested_configuration, requested);
        if count == 0 {
            assert_eq!(
                intent.resolution,
                PlannedSwitchoverResolution::RestoringOldPrimary
            );
            let retirement = observe_switchover_command(&mut snapshot);
            assert!(retirement.is_switchover_restoration());
            assert_eq!(retirement.current_configuration, starting);
            assert_eq!(retirement.retire_switchover_preparation_ids.len(), 1);
            assert_eq!(
                retirement.switchover_handoff.is_some(),
                boundary == "prepared"
            );
            apply_switchover_status(&mut snapshot);
            assert_eq!(
                snapshot.status.topology.as_ref().unwrap().configuration,
                starting
            );
            assert_eq!(
                snapshot.status.last_switchover.as_ref().unwrap().outcome,
                PlannedSwitchoverOutcome::OldPrimaryRestored
            );
        } else {
            assert_eq!(
                intent.resolution,
                PlannedSwitchoverResolution::CompensatingOldPrimary
            );
            let compensation = snapshot
                .status
                .transition
                .as_ref()
                .unwrap()
                .current_configuration
                .clone();
            assert!(compensation.epoch.configuration_number > requested.epoch.configuration_number);
            assert_eq!(
                compensation.epoch.data_loss_number,
                starting.epoch.data_loss_number
            );
            assert_eq!(compensation.members, starting.members);
            assert_eq!(compensation.write_quorum, starting.write_quorum);
            for (current_only, id) in [(false, 3), (false, 1), (true, 3), (true, 1)] {
                let command = observe_switchover_command(&mut snapshot);
                assert_eq!(command.local_replica_id, ReplicaId::new(id), "{boundary}");
                assert_eq!(command.current_only, current_only);
                assert_eq!(
                    command.previous_configuration,
                    (!current_only).then(|| requested.clone())
                );
                assert_eq!(
                    command.primary_write_status,
                    AccessStatus::ReconfigurationPending
                );
                assert_eq!(
                    command.retire_switchover_preparation_ids.len(),
                    usize::from(current_only && id == 1)
                );
            }
            let receipt = evaluate(&snapshot, &EvaluationConfig::default());
            assert_eq!(receipt, evaluate(&snapshot, &EvaluationConfig::default()));
            apply_switchover_status(&mut snapshot);
            assert_eq!(
                snapshot.status.topology.as_ref().unwrap().configuration,
                compensation
            );
            assert_eq!(
                snapshot.status.last_switchover.as_ref().unwrap().outcome,
                PlannedSwitchoverOutcome::OldPrimaryCompensated
            );
        }
        assert!(snapshot.status.transition.is_none());
        assert!(snapshot.status.primary_failure.is_none());
        assert!(snapshot.status.provisioning.is_none());
        // Normalize the deleted Pod's retained PVC as an orphan, as the real
        // controller does. Availability must precede ordinary replacement.
        let orphan = snapshot
            .replicas
            .remove(&observation_key(2, "pod-2"))
            .unwrap();
        snapshot
            .replicas
            .insert(observation_key(2, "orphan-pvc-2"), orphan);
        let grant = observe_switchover_command(&mut snapshot);
        assert_eq!(grant.primary_write_status, AccessStatus::Granted);
        assert_eq!(grant.local_replica_id, ReplicaId::new(1));
        assert!(
            matches!(evaluate(&snapshot, &EvaluationConfig::default()), Plan::Apply { changes }
            if matches!(&changes[0], KubernetesChange::PublishWriteRouting { primary } if primary.replica_id == ReplicaId::new(1)))
        );
    }
}

#[test]
fn switchover_compensation_converges_returned_target_and_uninvolved_before_source_grant() {
    let mut snapshot = prepared_switchover_snapshot();
    observe_switchover_command(&mut snapshot);
    switchover_report(&mut snapshot, 2).reported_fault =
        Some(kuberic_protocol::types::FaultType::Permanent);
    apply_switchover_status(&mut snapshot);
    // Recovery is frozen: a healed target cannot switch the decision back.
    switchover_report(&mut snapshot, 2).reported_fault = None;
    let policy = snapshot.status.effective_policy.clone();
    for current_only in [false, true] {
        for id in [2, 3, 1] {
            let command = observe_switchover_command(&mut snapshot);
            assert_eq!(command.local_replica_id, ReplicaId::new(id));
            assert_eq!(command.current_only, current_only);
            assert_eq!(
                command.retire_switchover_preparation_ids.len(),
                usize::from(id == 1 && current_only)
            );
            assert!(snapshot.routing.write_target.is_none());
        }
    }
    apply_switchover_status(&mut snapshot);
    assert_eq!(snapshot.status.effective_policy, policy);
    let grant = observe_switchover_command(&mut snapshot);
    assert_eq!(grant.local_replica_id, ReplicaId::new(1));
    assert_eq!(grant.primary_write_status, AccessStatus::Granted);
    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("routing")
    };
    assert!(
        matches!(&changes[0], KubernetesChange::PublishWriteRouting { primary } if primary.replica_id == ReplicaId::new(1))
    );
}

#[test]
fn switchover_compensation_fences_permanent_fault_before_survivor_commands() {
    let mut snapshot = prepared_switchover_snapshot();
    observe_switchover_command(&mut snapshot);
    switchover_report(&mut snapshot, 2).reported_fault =
        Some(kuberic_protocol::types::FaultType::Permanent);
    apply_switchover_status(&mut snapshot);
    let mut temporary = snapshot.clone();
    temporary
        .replicas
        .get_mut(&observation_key(2, "pod-2"))
        .unwrap()
        .agent = AgentObservation::Unreachable {
        message: "temporary restart".into(),
    };
    assert!(matches!(
        evaluate(&temporary, &EvaluationConfig::default()),
        Plan::Wait { .. }
    ));
    let expected = Plan::Apply {
        changes: vec![KubernetesChange::DeleteExactPod {
            pod_name: snapshot.replicas[&observation_key(2, "pod-2")]
                .kubernetes
                .as_ref()
                .unwrap()
                .pod_name
                .clone(),
            pod_uid: PodUid::new("pod-2"),
        }],
    };
    // An extant failed participant never has to execute another lifecycle command.
    for _ in 0..3 {
        assert_eq!(evaluate(&snapshot, &EvaluationConfig::default()), expected);
    }
    lose_switchover_pod(&mut snapshot, 2);
    for current_only in [false, true] {
        for id in [3, 1] {
            let command = observe_switchover_command(&mut snapshot);
            assert_eq!(command.local_replica_id, ReplicaId::new(id));
            assert_eq!(command.current_only, current_only);
        }
    }
    apply_switchover_status(&mut snapshot);
    let orphan = snapshot
        .replicas
        .remove(&observation_key(2, "pod-2"))
        .unwrap();
    snapshot
        .replicas
        .insert(observation_key(2, "orphan-pvc-2"), orphan);
    let grant = observe_switchover_command(&mut snapshot);
    assert_eq!(grant.local_replica_id, ReplicaId::new(1));
    assert_eq!(grant.primary_write_status, AccessStatus::Granted);
    assert_eq!(
        snapshot.status.last_switchover.as_ref().unwrap().outcome,
        PlannedSwitchoverOutcome::OldPrimaryCompensated
    );
}

#[test]
fn switchover_temporary_evidence_waits_but_definitive_source_loss_and_contradiction_close() {
    for id in [1, 2, 3] {
        for admitted in [false, true] {
            let mut snapshot = prepared_switchover_snapshot();
            if admitted {
                observe_switchover_command(&mut snapshot);
            }
            snapshot
                .replicas
                .get_mut(&observation_key(id, &format!("pod-{id}")))
                .unwrap()
                .agent = AgentObservation::Unreachable {
                message: "process restarting".into(),
            };
            let Plan::Wait {
                status,
                requeue_after_seconds,
                ..
            } = evaluate(&snapshot, &EvaluationConfig::default())
            else {
                panic!("temporary evidence for {id}, admitted={admitted}");
            };
            assert_eq!(requeue_after_seconds, 5);
            assert!(status.transition.is_some());
            assert!(status.primary_failure.is_none() && status.provisioning.is_none());
            assert!(status.last_switchover.is_none());
            assert!(
                status
                    .conditions
                    .iter()
                    .any(|condition| condition.type_ == "Progressing")
            );
        }
    }
    let mut lost = prepared_switchover_snapshot();
    lose_switchover_pod(&mut lost, 1);
    assert_switchover_safety_decision(&lost);
    let mut replaced = prepared_switchover_snapshot();
    switchover_report(&mut replaced, 1)
        .identity
        .agent_generation = AgentGeneration::new("replacement");
    assert_switchover_safety_decision(&replaced);
    let mut contradictory = prepared_switchover_snapshot();
    observe_switchover_command(&mut contradictory);
    switchover_report(&mut contradictory, 1)
        .prepared_switchover
        .as_mut()
        .unwrap()
        .handoff_lsn = 9;
    lose_switchover_pod(&mut contradictory, 2);
    assert_switchover_safety_decision(&contradictory);
}

#[test]
fn switchover_compensation_waits_for_recoverable_read_quorum_and_never_restores_after_admission() {
    let mut snapshot = prepared_switchover_snapshot();
    observe_switchover_command(&mut snapshot);
    lose_switchover_pod(&mut snapshot, 2);
    snapshot
        .replicas
        .get_mut(&observation_key(3, "pod-3"))
        .unwrap()
        .agent = AgentObservation::Unreachable {
        message: "partition".into(),
    };
    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Wait { .. }
    ));
    lose_switchover_pod(&mut snapshot, 3);
    assert_switchover_safety_decision(&snapshot);
}

#[test]
fn switchover_compensation_fails_closed_when_even_membership_cannot_regain_write_quorum() {
    let mut two = prepared_switchover_snapshot_with_size(2);
    observe_switchover_command(&mut two);
    lose_switchover_pod(&mut two, 2);
    assert_switchover_safety_decision(&two);

    let mut four = prepared_switchover_snapshot_with_size(4);
    observe_switchover_command(&mut four);
    lose_switchover_pod(&mut four, 2);
    lose_switchover_pod(&mut four, 4);
    assert_switchover_safety_decision(&four);
}

#[test]
fn switchover_any_requested_authority_admission_precludes_old_epoch_restoration() {
    let mut snapshot = prepared_switchover_snapshot();
    let starting = snapshot
        .status
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .clone();
    let requested = snapshot
        .status
        .transition
        .as_ref()
        .unwrap()
        .current_configuration
        .clone();
    let third = switchover_report(&mut snapshot, 3);
    third.epoch = requested.epoch;
    third.current_configuration = Some(requested.clone());
    third.previous_configuration = Some(starting);
    third.pending_operation_id = Some(OperationId::new("admitted-before-role-change"));
    lose_switchover_pod(&mut snapshot, 2);
    apply_switchover_status(&mut snapshot);
    let transition = snapshot.status.transition.unwrap();
    assert_eq!(
        transition.switchover.unwrap().resolution,
        PlannedSwitchoverResolution::CompensatingOldPrimary
    );
    assert!(transition.current_configuration.epoch > requested.epoch);
}

#[test]
fn switchover_unsafe_waits_for_exact_pod_absence_or_observed_authority_closure() {
    let mut snapshot = prepared_switchover_snapshot();
    // Contradictory source plus an ambiguous possible writer: removing routing
    // and issuing deletion are not evidence of runtime closure.
    switchover_report(&mut snapshot, 1)
        .identity
        .agent_generation = AgentGeneration::new("wrong");
    snapshot
        .replicas
        .get_mut(&observation_key(2, "pod-2"))
        .unwrap()
        .agent = AgentObservation::Unreachable {
        message: "ambiguous writer".into(),
    };
    apply_switchover_status(&mut snapshot);
    for id in [1, 2] {
        let plan = evaluate(&snapshot, &EvaluationConfig::default());
        assert_eq!(plan, evaluate(&snapshot, &EvaluationConfig::default()));
        assert!(matches!(plan, Plan::Apply { ref changes }
            if matches!(&changes[..], [KubernetesChange::DeleteExactPod { pod_uid, .. }] if pod_uid.as_str() == format!("pod-{id}"))));
        assert!(snapshot.status.last_switchover.is_none());
        // PVC-only observation remains after exact Pod absence is observed.
        lose_switchover_pod(&mut snapshot, id);
        assert!(
            snapshot
                .replicas
                .get(&observation_key(id, &format!("pod-{id}")))
                .unwrap()
                .kubernetes
                .as_ref()
                .unwrap()
                .pvc_uid
                .is_some()
        );
    }
    let Plan::Unsafe { status, .. } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("closed terminal")
    };
    validate_status(&status).unwrap();
    assert_eq!(
        status.last_switchover.as_ref().unwrap().outcome,
        PlannedSwitchoverOutcome::Unsafe
    );
    snapshot.status = status;
    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Unsafe { .. }
    ));
    // Missing list evidence cannot prove terminal safety even on a later retry.
    snapshot
        .observation_failures
        .push(kuberic_protocol::observation::ObservationFailure {
            source: "pods".into(),
            message: "list failed".into(),
        });
    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Wait { .. }
    ));
}

#[test]
fn switchover_safety_closes_live_starting_writer_before_terminal_receipt() {
    let mut snapshot = switchover_snapshot();
    apply_switchover_status(&mut snapshot);
    switchover_report(&mut snapshot, 2)
        .identity
        .agent_generation = AgentGeneration::new("wrong");
    apply_switchover_status(&mut snapshot);
    let closure = observe_switchover_command(&mut snapshot);
    assert_eq!(
        closure.current_epoch,
        snapshot
            .status
            .topology
            .as_ref()
            .unwrap()
            .configuration
            .epoch
    );
    assert_eq!(
        closure.primary_write_status,
        AccessStatus::ReconfigurationPending
    );
    lose_switchover_pod(&mut snapshot, 2);
    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Unsafe { .. }
    ));
}

#[test]
fn switchover_accepted_target_replays_lost_status_grant_and_routing_forward_only() {
    let mut snapshot = prepared_switchover_snapshot();
    for _ in 0..6 {
        observe_switchover_command(&mut snapshot);
    }
    let receipt = evaluate(&snapshot, &EvaluationConfig::default());
    assert_eq!(receipt, evaluate(&snapshot, &EvaluationConfig::default()));
    apply_switchover_status(&mut snapshot);
    let accepted = snapshot.status.topology.clone();
    let grant = evaluate(&snapshot, &EvaluationConfig::default());
    assert_eq!(grant, evaluate(&snapshot, &EvaluationConfig::default()));
    observe_switchover_command(&mut snapshot);
    let routing = evaluate(&snapshot, &EvaluationConfig::default());
    assert_eq!(routing, evaluate(&snapshot, &EvaluationConfig::default()));
    assert_eq!(snapshot.status.topology, accepted);
    assert_eq!(
        snapshot.status.last_switchover.as_ref().unwrap().outcome,
        PlannedSwitchoverOutcome::RequestedTargetCompleted
    );
}

fn observe_switchover_command(
    snapshot: &mut ObservationSnapshot,
) -> kuberic_protocol::command::EnsureConfiguration {
    let plan = evaluate(snapshot, &EvaluationConfig::default());
    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(command),
    } = plan
    else {
        panic!("expected configuration command: {plan:?}")
    };
    let report = switchover_report(snapshot, command.local_replica_id.value());
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
        .find(|member| member.identity == report.identity)
        .unwrap()
        .role;
    report.write_status = if report.role == ReplicaRole::Primary {
        command.primary_write_status
    } else {
        AccessStatus::NotPrimary
    };
    report.retained_operation_id = Some(command.operation_id.clone());
    report.pending_operation_id = None;
    report.catch_up_complete = true;
    report.catch_up_boundary = command.previous_configuration.as_ref().map(|_| 10);
    report.current_configuration_quorum_progress = 10;
    if !command.retire_switchover_preparation_ids.is_empty() {
        report.prepared_switchover = None;
    }
    *command
}

#[test]
fn invalid_switchover_requests_only_project_rejection() {
    for (request_id, target, reason) in [
        ("", 2, "MalformedRequest"),
        ("  ", 2, "MalformedRequest"),
        ("move-1", 0, "MalformedRequest"),
        ("move-1", 1, "TargetAlreadyPrimary"),
        ("move-1", 99, "TargetNotMember"),
    ] {
        let mut snapshot = switchover_snapshot();
        snapshot.desired.switchover = Some(PlannedSwitchoverRequest {
            request_id: SwitchoverRequestId::new(request_id),
            target_replica_id: ReplicaId::new(target),
        });
        let before = snapshot.clone();
        apply_switchover_status(&mut snapshot);
        assert_eq!(snapshot.status.topology, before.status.topology);
        assert_eq!(snapshot.routing, before.routing);
        assert!(snapshot.status.transition.is_none());
        assert!(
            snapshot
                .status
                .conditions
                .iter()
                .any(|condition| condition.type_ == "SwitchoverRejected"
                    && condition.reason == reason)
        );
    }
    for changed in ["stale", "unavailable", "non-member", "busy"] {
        let mut snapshot = switchover_snapshot();
        match changed {
            "stale" => {
                let old = ConfigurationDescriptor::new(
                    Epoch::new(0, 0),
                    ReplicaId::new(1),
                    configuration().members,
                    2,
                );
                let report = switchover_report(&mut snapshot, 2);
                report.epoch = old.epoch;
                report.current_configuration = Some(old);
            }
            "unavailable" => switchover_report(&mut snapshot, 2).healthy = false,
            "non-member" => {
                let observation = snapshot
                    .replicas
                    .remove(&observation_key(2, "pod-2"))
                    .unwrap();
                let mut extra = observation;
                let AgentObservation::Report(report) = &mut extra.agent else {
                    unreachable!()
                };
                report.identity.instance_id = ReplicaInstanceId::new("other-pod-2");
                extra.kubernetes.as_mut().unwrap().pod_uid = Some(PodUid::new("other-pod-2"));
                snapshot
                    .replicas
                    .insert(observation_key(2, "other-pod-2"), extra);
            }
            "busy" => {
                switchover_report(&mut snapshot, 2).pending_operation_id =
                    Some(OperationId::new("other-work"))
            }
            _ => unreachable!(),
        }
        let topology = snapshot.status.topology.clone();
        let routing = snapshot.routing.clone();
        if changed == "stale" {
            assert!(matches!(evaluate(&snapshot, &EvaluationConfig::default()),
                Plan::Execute { command: ProtocolCommand::EnsureConfiguration(c) }
                    if c.local_replica_id == ReplicaId::new(2)));
            assert!(snapshot.status.transition.is_none());
            continue;
        }
        apply_switchover_status(&mut snapshot);
        assert_eq!(snapshot.status.topology, topology, "{changed}");
        assert_eq!(snapshot.routing, routing, "{changed}");
        assert!(snapshot.status.transition.is_none(), "{changed}");
        assert!(
            snapshot
                .status
                .conditions
                .iter()
                .any(|condition| condition.reason == "TargetNotEligible"),
            "{changed}"
        );
    }
}

#[test]
fn switchover_freezes_deterministic_intent_before_removing_routing() {
    let mut snapshot = switchover_snapshot();
    let first = evaluate(&snapshot, &EvaluationConfig::default());
    assert_eq!(first, evaluate(&snapshot, &EvaluationConfig::default()));
    let previous = snapshot.status.topology.clone().unwrap().configuration;
    let routing = snapshot.routing.clone();
    apply_switchover_status(&mut snapshot);
    assert_eq!(snapshot.routing, routing);
    let transition = snapshot.status.transition.clone().unwrap();
    let intent = transition.switchover.as_ref().unwrap();
    assert_eq!(intent.source, previous.members[0].identity);
    assert_eq!(intent.target, previous.members[1].identity);
    assert_eq!(
        intent.requested_configuration,
        transition.current_configuration
    );
    assert_eq!(
        intent.resolution,
        PlannedSwitchoverResolution::RequestedTarget
    );
    assert_eq!(transition.current_configuration.epoch, Epoch::new(0, 2));
    assert_eq!(
        transition.effective_policy,
        snapshot.status.effective_policy.clone().unwrap()
    );
    assert_eq!(
        transition.current_configuration.members.len(),
        previous.members.len()
    );
    assert!(intent.handoff.is_none());

    apply_switchover_status(&mut snapshot);
    assert!(snapshot.routing.write_target.is_none());
    assert_eq!(snapshot.status.transition.as_ref(), Some(&transition));
    let plan = evaluate(&snapshot, &EvaluationConfig::default());
    let Plan::Execute {
        command: ProtocolCommand::PrepareSwitchover(command),
    } = plan
    else {
        panic!("preparation after routing removal")
    };
    assert_eq!(command.source, intent.source);
    assert_eq!(command.target, intent.target);
    assert_eq!(command.current_configuration, previous);
    assert_eq!(
        command.operation_id,
        derive_switchover_preparation_operation_id(
            &snapshot.resource_uid,
            &intent.request_id,
            intent.preparation_generation,
            &snapshot
                .status
                .topology
                .as_ref()
                .unwrap()
                .configuration
                .configuration_id,
            &intent.source,
            &intent.target
        )
    );
}

#[test]
fn switchover_requires_quiescent_accepted_authority() {
    let mut snapshot = switchover_snapshot();
    switchover_report(&mut snapshot, 3).pending_operation_id = Some(OperationId::new("pending"));
    let Plan::Wait { status, .. } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("wait for stable authority")
    };
    assert!(status.transition.is_none());
    assert_eq!(status.topology, snapshot.status.topology);
}

#[test]
fn switchover_validates_preparation_and_requires_verified_target_progress() {
    let mut snapshot = prepared_switchover_snapshot();
    let handoff = snapshot
        .status
        .transition
        .as_ref()
        .unwrap()
        .switchover
        .as_ref()
        .unwrap()
        .handoff
        .clone()
        .unwrap();
    assert_eq!(
        handoff,
        switchover_report(&mut snapshot, 1)
            .prepared_switchover
            .clone()
            .unwrap()
    );
    for verified in [None, Some(9)] {
        switchover_report(&mut snapshot, 2).verified_replication_lsn = verified;
        let Plan::Wait { status, .. } = evaluate(&snapshot, &EvaluationConfig::default()) else {
            panic!("raw progress cannot satisfy catch-up")
        };
        assert!(
            status
                .conditions
                .iter()
                .any(|condition| condition.reason == "SwitchoverTargetCatchupPending")
        );
    }
    switchover_report(&mut snapshot, 2).verified_replication_lsn = Some(10);
    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Execute { .. }
    ));

    snapshot
        .status
        .transition
        .as_mut()
        .unwrap()
        .switchover
        .as_mut()
        .unwrap()
        .handoff = None;
    switchover_report(&mut snapshot, 1)
        .prepared_switchover
        .as_mut()
        .unwrap()
        .request_id = SwitchoverRequestId::new("wrong-request");
    assert_switchover_safety_decision(&snapshot);
}

#[test]
fn switchover_converges_every_member_closed_before_receipt_grant_and_exact_routing() {
    let mut snapshot = prepared_switchover_snapshot();
    let previous = snapshot.status.topology.clone();
    for (current_only, ids) in [(false, [1, 3, 2]), (true, [1, 3, 2])] {
        for id in ids {
            let command = observe_switchover_command(&mut snapshot);
            assert_eq!(command.local_replica_id, ReplicaId::new(id));
            assert_eq!(command.current_only, current_only);
            assert_eq!(
                command.primary_write_status,
                AccessStatus::ReconfigurationPending
            );
            assert!(command.switchover_handoff.is_some());
            assert_eq!(
                command.retire_switchover_preparation_ids.len(),
                usize::from(current_only && id == 1)
            );
            assert_eq!(snapshot.status.topology, previous);
            assert!(snapshot.routing.write_target.is_none());
            assert!(snapshot.replicas.values().all(
                |observation| matches!(&observation.agent, AgentObservation::Report(report)
                    if report.write_status != AccessStatus::Granted)
            ));
        }
    }
    let transition = snapshot.status.transition.clone().unwrap();
    let mut missing_retained = snapshot.clone();
    switchover_report(&mut missing_retained, 3).retained_operation_id = None;
    assert!(
        matches!(evaluate(&missing_retained, &EvaluationConfig::default()),
        Plan::Execute { command: ProtocolCommand::EnsureConfiguration(command) }
        if command.current_only && command.local_replica_id == ReplicaId::new(3))
    );
    apply_switchover_status(&mut snapshot);
    assert!(snapshot.status.transition.is_none());
    assert_eq!(
        snapshot.status.topology.as_ref().unwrap().configuration,
        transition.current_configuration
    );
    assert_eq!(
        snapshot.status.last_switchover.as_ref().unwrap().outcome,
        PlannedSwitchoverOutcome::RequestedTargetCompleted
    );
    assert!(snapshot.status.primary_failure.is_none());
    assert!(snapshot.status.quorum_loss.is_none());
    assert!(snapshot.routing.write_target.is_none());
    let grant = observe_switchover_command(&mut snapshot);
    assert_eq!(grant.local_replica_id, ReplicaId::new(2));
    assert_eq!(grant.primary_write_status, AccessStatus::Granted);
    assert!(grant.switchover_handoff.is_none());
    assert!(snapshot.routing.write_target.is_none());
    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("publish only after observing target grant")
    };
    let target = switchover_report(&mut snapshot, 2).identity.clone();
    assert!(
        matches!(&changes[0], KubernetesChange::PublishWriteRouting { primary } if *primary == target)
    );
    snapshot.routing.write_target = Some(target);
    for _ in 0..3 {
        let Plan::Stable { status, .. } = evaluate(&snapshot, &EvaluationConfig::default()) else {
            panic!("unchanged completed request must be stable")
        };
        assert!(status.transition.is_none());
        snapshot.status = status;
    }
    snapshot
        .desired
        .switchover
        .as_mut()
        .unwrap()
        .target_replica_id = ReplicaId::new(3);
    apply_switchover_status(&mut snapshot);
    assert!(
        snapshot
            .status
            .conditions
            .iter()
            .any(|condition| condition.reason == "RequestIdReused")
    );
}

#[test]
fn switchover_waits_for_all_reports_and_target_joint_catchup() {
    let mut snapshot = prepared_switchover_snapshot();
    observe_switchover_command(&mut snapshot);
    let mut absent = snapshot.clone();
    absent
        .replicas
        .get_mut(&observation_key(3, "pod-3"))
        .unwrap()
        .agent = AgentObservation::Absent;
    assert!(matches!(
        evaluate(&absent, &EvaluationConfig::default()),
        Plan::Wait { .. }
    ));
    observe_switchover_command(&mut snapshot);
    observe_switchover_command(&mut snapshot);
    for fault in [
        "catchup", "boundary", "quorum", "verified", "retained", "pending",
    ] {
        let mut waiting = snapshot.clone();
        let target = switchover_report(&mut waiting, 2);
        match fault {
            "catchup" => target.catch_up_complete = false,
            "boundary" => target.catch_up_boundary = Some(9),
            "quorum" => target.current_configuration_quorum_progress = 9,
            "verified" => target.verified_replication_lsn = None,
            "retained" => target.retained_operation_id = None,
            "pending" => target.pending_operation_id = Some(OperationId::new("pending")),
            _ => unreachable!(),
        }
        let plan = evaluate(&waiting, &EvaluationConfig::default());
        assert!(
            matches!(plan, Plan::Wait { .. })
                || matches!(plan, Plan::Execute { command: ProtocolCommand::EnsureConfiguration(command) }
                if !command.current_only),
            "{fault}"
        );
    }
    let mut early_writer = snapshot;
    switchover_report(&mut early_writer, 2).write_status = AccessStatus::Granted;
    assert_switchover_safety_decision(&early_writer);
}

#[test]
fn active_switchover_rejects_mutation_and_cancellation_without_retargeting() {
    for request in [
        None,
        Some(PlannedSwitchoverRequest {
            request_id: SwitchoverRequestId::new("another-request"),
            target_replica_id: ReplicaId::new(3),
        }),
    ] {
        let mut snapshot = prepared_switchover_snapshot();
        let frozen = snapshot.status.transition.clone();
        snapshot.desired.switchover = request;
        snapshot.desired.replicas = 5;
        apply_switchover_status(&mut snapshot);
        assert_eq!(snapshot.status.transition, frozen);
        assert!(
            snapshot
                .status
                .conditions
                .iter()
                .any(|condition| condition.reason == "ActiveRequestImmutable")
        );
        let command = observe_switchover_command(&mut snapshot);
        assert_eq!(command.current_configuration.primary_id, ReplicaId::new(2));
        assert_eq!(command.effective_policy.replica_set_size, 3);
    }
}

#[test]
fn missing_scaffolding_produces_apply() {
    let plan = evaluate(&empty_snapshot(3), &EvaluationConfig::default());

    assert_eq!(
        plan,
        Plan::Apply {
            changes: vec![KubernetesChange::EnsureReplicaScaffolding {
                replica_ids: vec![ReplicaId::new(1), ReplicaId::new(2), ReplicaId::new(3)],
            }],
        }
    );
}

#[test]
fn missing_replica_support_converges_before_authority_changes() {
    let mut snapshot = scaffolded_snapshot();
    snapshot.supporting_resources_ready = false;
    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Apply { changes }
            if changes == vec![KubernetesChange::EnsureReplicaSupport]
    ));
}

#[test]
fn durable_evidence_without_authority_is_unsafe() {
    let mut snapshot = empty_snapshot(3);
    snapshot.durable_storage_evidence = true;

    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Unsafe {
            reason: UnsafeReason::DurableEvidenceWithoutAuthority,
            ..
        }
    ));
}

#[test]
fn zero_desired_replicas_is_invalid_desired_state() {
    assert!(matches!(
        evaluate(&empty_snapshot(0), &EvaluationConfig::default()),
        Plan::Unsafe {
            reason: UnsafeReason::InvalidDesiredState(_),
            ..
        }
    ));
}

#[test]
fn observation_failure_waits_instead_of_inventing_defaults() {
    let mut snapshot = empty_snapshot(3);
    snapshot.observation_failures.push(ObservationFailure {
        source: "agent-list".to_string(),
        message: "timeout".to_string(),
    });

    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Wait {
            reason: WaitReason::AgentUnavailable,
            ..
        }
    ));
}

#[test]
fn normalized_invalid_agent_evidence_is_unsafe() {
    let mut snapshot = scaffolded_snapshot();
    snapshot
        .replicas
        .get_mut(&observation_key(1, "pod-uid-1"))
        .unwrap()
        .agent = AgentObservation::Invalid {
        message: "report sequence regressed".to_string(),
        uninitialized_report: None,
    };

    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Unsafe {
            reason: UnsafeReason::ContradictoryReplicaEvidence(_),
            ..
        }
    ));
}

#[test]
fn stale_report_sequence_is_unsafe_within_the_same_session() {
    let mut snapshot = scaffolded_snapshot();
    snapshot.previous_report_watermarks.insert(
        observation_key(1, "pod-uid-1"),
        ReportWatermark {
            process_session_id: ProcessSessionId::new("session-1"),
            report_sequence: 4,
        },
    );
    snapshot
        .replicas
        .get_mut(&observation_key(1, "pod-uid-1"))
        .unwrap()
        .agent = AgentObservation::Uninitialized(UninitializedAgentObservation {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: snapshot.resource_uid.clone(),
        replica_id: ReplicaId::new(1),
        pod_uid: PodUid::new("pod-uid-1"),
        pvc_uid: PvcUid::new("pvc-uid-1"),
        process_session_id: ProcessSessionId::new("session-1"),
        report_sequence: 4,
    });

    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Unsafe {
            reason: UnsafeReason::InvalidAcceptedAuthority(_),
            ..
        }
    ));
}

#[test]
fn bootstrap_intent_is_deterministic() {
    let snapshot = scaffolded_snapshot();
    let first = evaluate(&snapshot, &EvaluationConfig::default());
    let second = evaluate(&snapshot, &EvaluationConfig::default());

    assert_eq!(first, second);
    let Plan::Apply { changes } = first else {
        panic!("expected bootstrap intent persistence");
    };
    let KubernetesChange::PersistStatus { status } = &changes[0] else {
        panic!("expected status persistence");
    };
    let transition = status.transition.as_ref().expect("bootstrap transition");
    assert_eq!(transition.kind, TransitionKind::Bootstrap);
    assert_eq!(transition.current_configuration.members.len(), 3);
    assert_eq!(transition.effective_policy.write_quorum, 2);
}

#[test]
fn bootstrap_installs_full_genesis_accepts_topology_then_grants_writes() {
    let mut snapshot = scaffolded_snapshot();
    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("expected bootstrap persistence");
    };
    let KubernetesChange::PersistStatus { status } = changes[0].clone() else {
        panic!("expected persisted bootstrap status");
    };
    snapshot.status = *status;
    let transition = snapshot.status.transition.clone().unwrap();

    for member in &transition.current_configuration.members {
        let observation = snapshot
            .replicas
            .get_mut(&ReplicaObservationKey::new(
                member.identity.replica_id,
                member.identity.instance_id.clone(),
            ))
            .unwrap();
        observation.agent = AgentObservation::Report(Box::new(AgentReport {
            protocol_version: kuberic_protocol::PROTOCOL_VERSION,
            resource_uid: snapshot.resource_uid.clone(),
            identity: member.identity.clone(),
            process_session_id: ProcessSessionId::new(format!(
                "initialized-{}",
                member.identity.replica_id
            )),
            report_sequence: 1,
            role: ReplicaRole::None,
            read_status: AccessStatus::NotPrimary,
            write_status: AccessStatus::NotPrimary,
            healthy: true,
            ..AgentReport::default()
        }));
    }

    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("expected transition condition persistence");
    };
    let KubernetesChange::PersistStatus { status } = changes[0].clone() else {
        panic!("expected transition status");
    };
    snapshot.status = *status;

    for member in &transition.current_configuration.members {
        let Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(command),
        } = evaluate(&snapshot, &EvaluationConfig::default())
        else {
            panic!("expected write-closed genesis installation");
        };
        assert_eq!(
            command.primary_write_status,
            AccessStatus::ReconfigurationPending
        );
        assert_eq!(command.local_replica_id, member.identity.replica_id);
        let observation = snapshot
            .observation_for_identity(&member.identity)
            .unwrap()
            .clone();
        let AgentObservation::Report(mut report) = observation.agent else {
            panic!("initialized report");
        };
        report.role = member.role;
        report.read_status = AccessStatus::Granted;
        report.write_status = if member.role == ReplicaRole::Primary {
            AccessStatus::ReconfigurationPending
        } else {
            AccessStatus::NotPrimary
        };
        report.epoch = transition.current_configuration.epoch;
        report.current_configuration = Some(transition.current_configuration.clone());
        report.retained_operation_id = Some(OperationId::new(format!(
            "{}:install:{}",
            transition.transition_id, member.identity.replica_id
        )));
        snapshot
            .replicas
            .get_mut(&ReplicaObservationKey::new(
                member.identity.replica_id,
                member.identity.instance_id.clone(),
            ))
            .unwrap()
            .agent = AgentObservation::Report(report);
    }

    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("expected atomic topology acceptance");
    };
    let KubernetesChange::PersistStatus { status } = changes[0].clone() else {
        panic!("expected accepted topology status");
    };
    assert!(status.initialized);
    assert!(status.transition.is_none());
    assert_eq!(
        status
            .topology
            .as_ref()
            .unwrap()
            .configuration
            .members
            .len(),
        3
    );
    snapshot.status = *status;

    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(command),
    } = evaluate(&snapshot, &EvaluationConfig::default())
    else {
        panic!("expected separate write grant");
    };
    assert_eq!(command.primary_write_status, AccessStatus::Granted);
    assert_eq!(command.local_replica_id, ReplicaId::new(1));
}

#[test]
fn persisted_bootstrap_initializes_exact_first_store() {
    let mut snapshot = scaffolded_snapshot();
    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("expected bootstrap persistence");
    };
    let KubernetesChange::PersistStatus { status } = changes[0].clone() else {
        panic!("expected persisted status");
    };
    snapshot.status = *status;

    let first_identity = snapshot
        .status
        .transition
        .as_ref()
        .unwrap()
        .current_configuration
        .members[0]
        .identity
        .clone();
    let observation = snapshot
        .replicas
        .get_mut(&observation_key(1, "pod-uid-1"))
        .unwrap();
    observation.agent = AgentObservation::Uninitialized(UninitializedAgentObservation {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: snapshot.resource_uid.clone(),
        replica_id: ReplicaId::new(1),
        pod_uid: PodUid::new("pod-uid-1"),
        pvc_uid: PvcUid::new("pvc-uid-1"),
        process_session_id: ProcessSessionId::new("session-1"),
        report_sequence: 1,
    });

    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("expected transition condition persistence");
    };
    let KubernetesChange::PersistStatus { status } = changes[0].clone() else {
        panic!("expected persisted transition status");
    };
    snapshot.status = *status;

    let Plan::Execute {
        command: ProtocolCommand::InitializeAgentStore(command),
    } = evaluate(&snapshot, &EvaluationConfig::default())
    else {
        panic!("expected store initialization");
    };
    assert_eq!(
        command.assigned_agent_generation,
        first_identity.agent_generation
    );
    assert_eq!(command.expected_instance_id, first_identity.instance_id);
    assert_eq!(
        command.assigned_agent_generation,
        derive_agent_generation(&derive_initialization_id(
            &snapshot.resource_uid,
            ReplicaId::new(1),
            &PodUid::new("pod-uid-1"),
            &PvcUid::new("pvc-uid-1"),
        ))
    );
}

#[test]
fn bootstrap_validates_all_members_before_initializing_any_store() {
    let mut snapshot = scaffolded_snapshot();
    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("expected bootstrap persistence");
    };
    let KubernetesChange::PersistStatus { status } = changes[0].clone() else {
        panic!("expected persisted status");
    };
    snapshot.status = *status;

    snapshot
        .replicas
        .get_mut(&observation_key(1, "pod-uid-1"))
        .unwrap()
        .agent = AgentObservation::Uninitialized(UninitializedAgentObservation {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: snapshot.resource_uid.clone(),
        replica_id: ReplicaId::new(1),
        pod_uid: PodUid::new("pod-uid-1"),
        pvc_uid: PvcUid::new("pvc-uid-1"),
        process_session_id: ProcessSessionId::new("session-1"),
        report_sequence: 1,
    });
    snapshot
        .replicas
        .get_mut(&observation_key(2, "pod-uid-2"))
        .unwrap()
        .agent = AgentObservation::Report(Box::new(AgentReport {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: snapshot.resource_uid.clone(),
        identity: identity(2, "pod-uid-2", "conflicting-generation"),
        process_session_id: ProcessSessionId::new("session-2"),
        report_sequence: 1,
        role: ReplicaRole::None,
        write_status: AccessStatus::NotPrimary,
        healthy: true,
        epoch: Epoch::default(),
        previous_configuration: None,
        current_configuration: None,
        current_progress: 0,
        committed_lsn: 0,
        catch_up_capability: None,
        ..AgentReport::default()
    }));

    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Unsafe {
            reason: UnsafeReason::InvalidAcceptedAuthority(_),
            ..
        }
    ));
}

#[test]
fn provisioning_observation_can_coexist_with_accepted_incarnation() {
    let accepted = replacement_configuration(3);
    let mut snapshot = empty_snapshot(3);
    let old_identity = accepted
        .members
        .iter()
        .find(|member| member.identity.replica_id == ReplicaId::new(3))
        .unwrap()
        .identity
        .clone();
    let initialization_id = kuberic_protocol::types::derive_initialization_id(
        &snapshot.resource_uid,
        ReplicaId::new(3),
        &PodUid::new("replacement-pod"),
        &PvcUid::new("replacement-pvc"),
    );
    let replacement_identity = ReplicaIdentity {
        replica_id: ReplicaId::new(3),
        instance_id: ReplicaInstanceId::new("replacement-pod"),
        agent_generation: derive_agent_generation(&initialization_id),
    };
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: accepted.clone(),
        }),
        provisioning: Some(ProvisioningIntent {
            purpose: ProvisioningPurpose::replacement(old_identity.clone()),
            pod_uid: PodUid::new("replacement-pod"),
            pvc_uid: PvcUid::new("replacement-pvc"),
            operation_id: OperationId::new("replacement-operation"),
        }),
        ..AcceptedStatus::default()
    };
    freeze_replacement_fixture(&mut snapshot, &old_identity);
    snapshot.replicas.insert(
        ReplicaObservationKey::new(old_identity.replica_id, old_identity.instance_id.clone()),
        ReplicaObservation {
            kubernetes: None,
            agent: AgentObservation::Report(Box::new(AgentReport {
                protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                resource_uid: snapshot.resource_uid.clone(),
                identity: old_identity,
                process_session_id: ProcessSessionId::new("old-session"),
                report_sequence: 1,
                role: ReplicaRole::ActiveSecondary,
                write_status: AccessStatus::NotPrimary,
                healthy: true,
                epoch: accepted.epoch,
                previous_configuration: None,
                current_configuration: Some(accepted.clone()),
                current_progress: 10,
                committed_lsn: 10,
                catch_up_capability: Some(10),
                ..AgentReport::default()
            })),
        },
    );
    snapshot.replicas.insert(
        ReplicaObservationKey::new(
            replacement_identity.replica_id,
            replacement_identity.instance_id.clone(),
        ),
        ReplicaObservation {
            kubernetes: None,
            agent: AgentObservation::Report(Box::new(AgentReport {
                protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                resource_uid: snapshot.resource_uid.clone(),
                identity: replacement_identity,
                process_session_id: ProcessSessionId::new("replacement-session"),
                report_sequence: 1,
                role: ReplicaRole::IdleSecondary,
                write_status: AccessStatus::NotPrimary,
                healthy: true,
                epoch: Epoch::default(),
                previous_configuration: None,
                current_configuration: None,
                current_progress: 0,
                committed_lsn: 0,
                catch_up_capability: None,
                ..AgentReport::default()
            })),
        },
    );
    let primary = accepted
        .members
        .iter()
        .find(|member| member.identity.replica_id == accepted.primary_id)
        .unwrap();
    snapshot.replicas.insert(
        ReplicaObservationKey::new(
            primary.identity.replica_id,
            primary.identity.instance_id.clone(),
        ),
        ReplicaObservation {
            kubernetes: None,
            agent: AgentObservation::Report(Box::new(AgentReport {
                protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                resource_uid: snapshot.resource_uid.clone(),
                identity: primary.identity.clone(),
                process_session_id: ProcessSessionId::new("primary-session"),
                report_sequence: 1,
                role: ReplicaRole::Primary,
                write_status: AccessStatus::Granted,
                healthy: true,
                epoch: accepted.epoch,
                previous_configuration: None,
                current_configuration: Some(accepted.clone()),
                current_progress: 10,
                committed_lsn: 10,
                catch_up_capability: Some(1),
                ..AgentReport::default()
            })),
        },
    );

    assert_eq!(snapshot.replicas.len(), 3);
    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Execute {
            command: ProtocolCommand::EnsureReplicaBuild(_),
        }
    ));
}

#[test]
fn unsupported_protocol_version_is_unsafe() {
    let mut snapshot = scaffolded_snapshot();
    let observation = snapshot
        .replicas
        .get_mut(&observation_key(1, "pod-uid-1"))
        .unwrap();
    observation.agent = AgentObservation::Uninitialized(UninitializedAgentObservation {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION + 1,
        resource_uid: snapshot.resource_uid.clone(),
        replica_id: ReplicaId::new(1),
        pod_uid: PodUid::new("pod-uid-1"),
        pvc_uid: PvcUid::new("pvc-uid-1"),
        process_session_id: ProcessSessionId::new("session-1"),
        report_sequence: 1,
    });

    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Unsafe {
            reason: UnsafeReason::IncompatibleProtocolVersion { .. },
            ..
        }
    ));
}

#[test]
fn active_transition_keeps_frozen_policy_after_spec_change() {
    let current_configuration = configuration();
    let policy = EffectivePolicy::fixed(3, 10).unwrap();
    let transition = TransitionIntent {
        secondary_scale_down: None,
        secondary_removal_evidence: None,
        scale_up: None,
        scale_up_failover: None,
        transition_id: derive_transition_id(
            &ResourceUid::new("resource-uid"),
            TransitionKind::Bootstrap,
            &current_configuration.configuration_id,
        ),
        kind: TransitionKind::Bootstrap,
        spec_generation: 1,
        effective_policy: policy.clone(),
        previous_configuration_id: None,
        current_configuration,
        election_lsn: None,
        build_id: None,
        repair: None,
        switchover: None,
    };
    let mut snapshot = empty_snapshot(5);
    snapshot.status.transition = Some(transition);

    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("expected unsupported-spec condition");
    };
    let KubernetesChange::PersistStatus { status } = changes[0].clone() else {
        panic!("expected status update");
    };
    assert_eq!(status.transition.as_ref().unwrap().effective_policy, policy);

    snapshot.status = *status;
    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Wait {
            reason: WaitReason::AgentUnavailable,
            ..
        }
    ));
}

#[test]
fn stable_topology_does_not_apply_unsupported_scale_request() {
    let configuration = configuration();
    let mut snapshot = empty_snapshot(5);
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: configuration.clone(),
        }),
        ..AcceptedStatus::default()
    };
    attest_stable_topology(&mut snapshot, &configuration);

    let Plan::Stable { status, .. } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("expected stable unsupported projection");
    };
    assert_eq!(
        status
            .conditions
            .iter()
            .find(|condition| condition.type_ == "UnsupportedSpec")
            .unwrap()
            .reason,
        "ReplicaCountImmutable"
    );
}

#[test]
fn stable_topology_does_not_observe_unapplied_image_or_policy_drift() {
    let configuration = configuration();
    let mut snapshot = empty_snapshot(3);
    snapshot.desired.generation = 2;
    snapshot.desired.image = "example:v2".to_string();
    snapshot.desired.failover_delay_seconds = 20;
    snapshot.status = AcceptedStatus {
        initialized: true,
        observed_generation: 1,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: configuration.clone(),
        }),
        ..AcceptedStatus::default()
    };
    attest_stable_topology(&mut snapshot, &configuration);

    let Plan::Stable { status, .. } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("expected unsupported spec projection");
    };
    assert_eq!(status.observed_generation, 1);
    let unsupported = status
        .conditions
        .iter()
        .find(|condition| condition.type_ == "UnsupportedSpec")
        .expect("unsupported spec condition");
    assert_eq!(unsupported.reason, "SpecDriftUnsupported");
    assert!(unsupported.message.contains("failover delay"));
    assert!(unsupported.message.contains("example:v2"));
}

#[test]
fn stable_topology_persists_primary_failure_without_attested_replica_evidence() {
    let configuration = configuration();
    let mut snapshot = empty_snapshot(3);
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology { configuration }),
        ..AcceptedStatus::default()
    };

    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("missing primary evidence must persist a failure observation");
    };
    let KubernetesChange::PersistStatus { status } = changes.last().unwrap() else {
        panic!("expected status persistence");
    };
    assert!(status.primary_failure.is_some());
}

#[test]
fn stable_topology_reconciles_routing_and_clears_resolved_conditions() {
    let configuration = configuration();
    let mut snapshot = empty_snapshot(3);
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: configuration.clone(),
        }),
        conditions: vec![
            kuberic_protocol::types::StatusCondition {
                type_: "Unsafe".to_string(),
                status: kuberic_protocol::types::ConditionStatus::True,
                reason: "OldConflict".to_string(),
                message: "resolved".to_string(),
            },
            kuberic_protocol::types::StatusCondition {
                type_: "UnsupportedSpec".to_string(),
                status: kuberic_protocol::types::ConditionStatus::True,
                reason: "ReplicaCountImmutable".to_string(),
                message: "resolved".to_string(),
            },
        ],
        ..AcceptedStatus::default()
    };
    attest_stable_topology(&mut snapshot, &configuration);

    let Plan::Stable { status, .. } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("expected stable attested topology");
    };
    assert!(status.conditions.iter().any(|condition| {
        condition.type_ == "Ready"
            && condition.status == kuberic_protocol::types::ConditionStatus::True
    }));
    assert!(
        status
            .conditions
            .iter()
            .all(|condition| condition.type_ != "Unsafe" && condition.type_ != "UnsupportedSpec")
    );

    snapshot.routing.write_target = None;
    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Apply { changes }
            if matches!(
                changes.first(),
                Some(KubernetesChange::PublishWriteRouting { .. })
            )
    ));
}

#[test]
fn primary_failure_fences_routing_waits_for_delay_and_allocates_newer_epoch() {
    let configuration = configuration();
    let mut snapshot = empty_snapshot(3);
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: configuration.clone(),
        }),
        ..AcceptedStatus::default()
    };
    attest_stable_topology(&mut snapshot, &configuration);
    let primary = configuration
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .unwrap();
    snapshot
        .replicas
        .get_mut(&ReplicaObservationKey::new(
            primary.identity.replica_id,
            primary.identity.instance_id.clone(),
        ))
        .unwrap()
        .agent = AgentObservation::Unreachable {
        message: "primary unavailable".to_string(),
    };

    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("primary failure must fence routing and persist timing");
    };
    assert!(matches!(
        changes.first(),
        Some(KubernetesChange::RemoveWriteRouting)
    ));
    let KubernetesChange::PersistStatus { status } = changes.last().unwrap() else {
        panic!("expected failure status");
    };
    snapshot.status = (**status).clone();
    snapshot.routing.write_target = None;
    snapshot.now_unix_seconds = 109;
    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Wait {
            reason: WaitReason::FailoverDelay,
            ..
        }
    ));

    snapshot.now_unix_seconds = 110;
    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("elapsed delay with read quorum must persist failover authority");
    };
    let KubernetesChange::PersistStatus { status } = changes.last().unwrap() else {
        panic!("expected failover transition");
    };
    let transition = status.transition.as_ref().unwrap();
    assert_eq!(transition.kind, TransitionKind::Failover);
    assert_eq!(transition.current_configuration.epoch, Epoch::new(0, 2));
    assert_eq!(
        transition.current_configuration.primary_id,
        ReplicaId::new(2)
    );
    assert_eq!(
        transition.current_configuration.epoch.data_loss_number,
        configuration.epoch.data_loss_number
    );
}

#[test]
fn failover_corrects_provisional_candidate_with_a_newer_epoch() {
    let previous = configuration();
    let provisional = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        ReplicaId::new(2),
        previous
            .members
            .iter()
            .map(|member| ConfigurationMember {
                identity: member.identity.clone(),
                role: if member.identity.replica_id == ReplicaId::new(2) {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        2,
    );
    let transition_id = derive_transition_id(
        &ResourceUid::new("resource-uid"),
        TransitionKind::Failover,
        &provisional.configuration_id,
    );
    let mut snapshot = empty_snapshot(3);
    snapshot.routing.service_present = true;
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: previous.clone(),
        }),
        transition: Some(TransitionIntent {
            secondary_scale_down: None,
            secondary_removal_evidence: None,
            scale_up: None,
            scale_up_failover: None,
            transition_id,
            kind: TransitionKind::Failover,
            spec_generation: 1,
            effective_policy: EffectivePolicy::fixed(3, 10).unwrap(),
            previous_configuration_id: Some(previous.configuration_id.clone()),
            current_configuration: provisional.clone(),
            election_lsn: Some(10),
            build_id: None,
            repair: None,
            switchover: None,
        }),
        primary_failure: Some(kuberic_protocol::types::PrimaryFailureObservation {
            primary: previous.members[0].identity.clone(),
            started_at_unix_seconds: 90,
        }),
        ..AcceptedStatus::default()
    };
    for (replica_id, progress) in [(1, 30), (2, 10), (3, 20)] {
        let member = provisional
            .members
            .iter()
            .find(|member| member.identity.replica_id == ReplicaId::new(replica_id))
            .unwrap();
        snapshot.replicas.insert(
            ReplicaObservationKey::new(
                member.identity.replica_id,
                member.identity.instance_id.clone(),
            ),
            ReplicaObservation {
                kubernetes: Some(KubernetesReplicaObservation {
                    replica_id: member.identity.replica_id,
                    pod_name: format!("pod-{replica_id}"),
                    pod_uid: Some(PodUid::new(member.identity.instance_id.as_str())),
                    pvc_name: format!("pvc-{replica_id}"),
                    pvc_uid: Some(PvcUid::new(format!("pvc-{replica_id}"))),
                    image: Some("example:v1".to_string()),
                    pod_ready: true,
                    peer_endpoint_ready: true,
                }),
                agent: AgentObservation::Report(Box::new(AgentReport {
                    protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                    resource_uid: snapshot.resource_uid.clone(),
                    identity: member.identity.clone(),
                    process_session_id: ProcessSessionId::new(format!("session-{replica_id}")),
                    report_sequence: 1,
                    role: member.role,
                    read_status: AccessStatus::Granted,
                    write_status: AccessStatus::ReconfigurationPending,
                    healthy: true,
                    epoch: provisional.epoch,
                    previous_configuration: Some(previous.clone()),
                    current_configuration: Some(provisional.clone()),
                    current_progress: progress,
                    committed_lsn: progress,
                    catch_up_capability: Some(1),
                    deactivated_lsn: Some(progress),
                    deactivation_epoch: Some(provisional.epoch),
                    ..AgentReport::default()
                })),
            },
        );
    }

    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("transition condition must be persisted first");
    };
    let KubernetesChange::PersistStatus { status } = changes.last().unwrap() else {
        panic!("expected transition status");
    };
    snapshot.status = (**status).clone();
    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("higher progress candidate must correct provisional authority");
    };
    let KubernetesChange::PersistStatus { status } = changes.last().unwrap() else {
        panic!("expected corrected transition");
    };
    let corrected = status.transition.as_ref().unwrap();
    assert_eq!(
        corrected.current_configuration.primary_id,
        ReplicaId::new(3)
    );
    assert_eq!(corrected.current_configuration.epoch, Epoch::new(0, 3));
}

#[test]
fn quorum_loss_closes_and_restores_primary_write_access_without_epoch_change() {
    let configuration = configuration();
    let mut snapshot = empty_snapshot(3);
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: configuration.clone(),
        }),
        ..AcceptedStatus::default()
    };
    attest_stable_topology(&mut snapshot, &configuration);
    for replica_id in [2, 3] {
        let member = configuration
            .members
            .iter()
            .find(|member| member.identity.replica_id == ReplicaId::new(replica_id))
            .unwrap();
        snapshot
            .replicas
            .get_mut(&ReplicaObservationKey::new(
                member.identity.replica_id,
                member.identity.instance_id.clone(),
            ))
            .unwrap()
            .agent = AgentObservation::Unreachable {
            message: "secondary unavailable".to_string(),
        };
    }

    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("quorum loss must be durably timed");
    };
    let KubernetesChange::PersistStatus { status } = changes.last().unwrap() else {
        panic!("expected quorum-loss status");
    };
    snapshot.status = (**status).clone();
    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(command),
    } = evaluate(&snapshot, &EvaluationConfig::default())
    else {
        panic!("primary must be switched to NoWriteQuorum");
    };
    assert_eq!(command.primary_write_status, AccessStatus::NoWriteQuorum);
    assert_eq!(command.current_epoch, configuration.epoch);

    let primary = configuration_primary_for_test(&configuration);
    let primary_observation = snapshot
        .replicas
        .get_mut(&ReplicaObservationKey::new(
            primary.identity.replica_id,
            primary.identity.instance_id.clone(),
        ))
        .unwrap();
    let AgentObservation::Report(report) = &mut primary_observation.agent else {
        panic!("primary report");
    };
    report.write_status = AccessStatus::NoWriteQuorum;
    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Wait {
            reason: WaitReason::QuorumLoss,
            ..
        }
    ));

    let secondary = configuration
        .members
        .iter()
        .find(|member| member.identity.replica_id == ReplicaId::new(2))
        .unwrap();
    let mut restored = empty_snapshot(3);
    restored.status = snapshot.status.clone();
    attest_stable_topology(&mut restored, &configuration);
    let primary_observation = restored
        .replicas
        .get_mut(&ReplicaObservationKey::new(
            primary.identity.replica_id,
            primary.identity.instance_id.clone(),
        ))
        .unwrap();
    let AgentObservation::Report(report) = &mut primary_observation.agent else {
        panic!("primary report");
    };
    report.write_status = AccessStatus::NoWriteQuorum;
    let _ = secondary;
    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(command),
    } = evaluate(&restored, &EvaluationConfig::default())
    else {
        panic!("returning quorum must restore writes");
    };
    assert_eq!(command.primary_write_status, AccessStatus::Granted);
    assert_eq!(command.current_epoch, configuration.epoch);
}

#[test]
fn failover_refuses_permanent_non_intersecting_recovery() {
    let configuration = configuration();
    let mut snapshot = empty_snapshot(3);
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: configuration.clone(),
        }),
        ..AcceptedStatus::default()
    };
    attest_stable_topology(&mut snapshot, &configuration);
    for replica_id in [1, 2] {
        let member = configuration
            .members
            .iter()
            .find(|member| member.identity.replica_id == ReplicaId::new(replica_id))
            .unwrap();
        let observation = snapshot
            .replicas
            .get_mut(&ReplicaObservationKey::new(
                member.identity.replica_id,
                member.identity.instance_id.clone(),
            ))
            .unwrap();
        let AgentObservation::Report(report) = &mut observation.agent else {
            panic!("initialized report");
        };
        report.reported_fault = Some(kuberic_protocol::types::FaultType::Permanent);
        report.healthy = false;
    }
    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("failure timing must be persisted before failover");
    };
    let KubernetesChange::PersistStatus { status } = changes.last().unwrap() else {
        panic!("expected failure status");
    };
    snapshot.status = (**status).clone();
    snapshot.routing.write_target = None;
    snapshot.now_unix_seconds += 10;
    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Unsafe {
            reason: UnsafeReason::ContradictoryReplicaEvidence(message),
            ..
        } if message.contains("abandoning accepted configuration quorum")
    ));
}

#[test]
fn failover_authorizes_full_copy_when_primary_history_cannot_repair_a_member() {
    let previous = configuration();
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        ReplicaId::new(2),
        previous
            .members
            .iter()
            .map(|member| ConfigurationMember {
                identity: member.identity.clone(),
                role: if member.identity.replica_id == ReplicaId::new(2) {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        2,
    );
    let transition_id = derive_transition_id(
        &ResourceUid::new("resource-uid"),
        TransitionKind::Failover,
        &current.configuration_id,
    );
    let mut snapshot = empty_snapshot(3);
    snapshot.routing.service_present = true;
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: previous.clone(),
        }),
        transition: Some(TransitionIntent {
            secondary_scale_down: None,
            secondary_removal_evidence: None,
            scale_up: None,
            scale_up_failover: None,
            transition_id,
            kind: TransitionKind::Failover,
            spec_generation: 1,
            effective_policy: EffectivePolicy::fixed(3, 10).unwrap(),
            previous_configuration_id: Some(previous.configuration_id.clone()),
            current_configuration: current.clone(),
            election_lsn: Some(20),
            build_id: None,
            repair: None,
            switchover: None,
        }),
        primary_failure: Some(kuberic_protocol::types::PrimaryFailureObservation {
            primary: previous.members[0].identity.clone(),
            started_at_unix_seconds: 90,
        }),
        ..AcceptedStatus::default()
    };
    for (replica_id, progress, retained_from) in [(2, 20, Some(15)), (3, 5, Some(1))] {
        let member = current
            .members
            .iter()
            .find(|member| member.identity.replica_id == ReplicaId::new(replica_id))
            .unwrap();
        snapshot.replicas.insert(
            ReplicaObservationKey::new(
                member.identity.replica_id,
                member.identity.instance_id.clone(),
            ),
            ReplicaObservation {
                kubernetes: Some(KubernetesReplicaObservation {
                    replica_id: member.identity.replica_id,
                    pod_name: format!("pod-{replica_id}"),
                    pod_uid: Some(PodUid::new(member.identity.instance_id.as_str())),
                    pvc_name: format!("pvc-{replica_id}"),
                    pvc_uid: Some(PvcUid::new(format!("pvc-{replica_id}"))),
                    image: Some("example:v1".to_string()),
                    pod_ready: true,
                    peer_endpoint_ready: true,
                }),
                agent: AgentObservation::Report(Box::new(AgentReport {
                    protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                    resource_uid: snapshot.resource_uid.clone(),
                    identity: member.identity.clone(),
                    process_session_id: ProcessSessionId::new(format!("session-{replica_id}")),
                    report_sequence: 1,
                    role: member.role,
                    read_status: AccessStatus::Granted,
                    write_status: AccessStatus::ReconfigurationPending,
                    healthy: true,
                    epoch: current.epoch,
                    previous_configuration: Some(previous.clone()),
                    current_configuration: Some(current.clone()),
                    current_progress: progress,
                    committed_lsn: progress,
                    catch_up_capability: retained_from,
                    deactivated_lsn: Some(progress),
                    deactivation_epoch: Some(current.epoch),
                    ..AgentReport::default()
                })),
            },
        );
    }
    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("transition condition must be persisted first");
    };
    let KubernetesChange::PersistStatus { status } = changes.last().unwrap() else {
        panic!("expected transition status");
    };
    snapshot.status = (**status).clone();
    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("history gap must persist full-copy authority");
    };
    let KubernetesChange::PersistStatus { status } = changes.last().unwrap() else {
        panic!("expected repair status");
    };
    let repair = status
        .transition
        .as_ref()
        .and_then(|transition| transition.repair.as_ref())
        .expect("repair intent");
    assert_eq!(repair.target.replica_id, ReplicaId::new(3));
}

#[test]
fn failover_serializes_multiple_required_full_copy_repairs() {
    let policy = EffectivePolicy::fixed(5, 10).unwrap();
    let identities = (1..=5)
        .map(|replica_id| {
            identity(
                replica_id,
                &format!("pod-{replica_id}"),
                &format!("generation-{replica_id}"),
            )
        })
        .collect::<Vec<_>>();
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        ReplicaId::new(1),
        identities
            .iter()
            .map(|identity| ConfigurationMember {
                identity: identity.clone(),
                role: if identity.replica_id == ReplicaId::new(1) {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        policy.write_quorum,
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        ReplicaId::new(2),
        identities
            .iter()
            .map(|identity| ConfigurationMember {
                identity: identity.clone(),
                role: if identity.replica_id == ReplicaId::new(2) {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        policy.write_quorum,
    );
    let transition_id = derive_transition_id(
        &ResourceUid::new("resource-uid"),
        TransitionKind::Failover,
        &current.configuration_id,
    );
    let first_target = current
        .members
        .iter()
        .find(|member| member.identity.replica_id == ReplicaId::new(3))
        .unwrap()
        .identity
        .clone();
    let first_repair = ReplicaRepairIntent {
        operation_id: kuberic_protocol::types::derive_failover_repair_operation_id(
            &ResourceUid::new("resource-uid"),
            &transition_id,
            &first_target,
        ),
        target: first_target.clone(),
    };
    let mut snapshot = empty_snapshot(5);
    snapshot.routing.service_present = true;
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(policy.clone()),
        topology: Some(AcceptedTopology {
            configuration: previous.clone(),
        }),
        transition: Some(TransitionIntent {
            secondary_scale_down: None,
            secondary_removal_evidence: None,
            scale_up: None,
            scale_up_failover: None,
            transition_id: transition_id.clone(),
            kind: TransitionKind::Failover,
            spec_generation: 1,
            effective_policy: policy,
            previous_configuration_id: Some(previous.configuration_id.clone()),
            current_configuration: current.clone(),
            election_lsn: Some(20),
            build_id: None,
            repair: Some(first_repair.clone()),
            switchover: None,
        }),
        primary_failure: Some(kuberic_protocol::types::PrimaryFailureObservation {
            primary: previous.members[0].identity.clone(),
            started_at_unix_seconds: 90,
        }),
        ..AcceptedStatus::default()
    };
    for (replica_id, progress, retained_from) in
        [(2, 20, Some(15)), (3, 20, Some(1)), (4, 5, Some(1))]
    {
        let member = current
            .members
            .iter()
            .find(|member| member.identity.replica_id == ReplicaId::new(replica_id))
            .unwrap();
        let completed_build = AgentBuildReport {
            build_id: first_repair.operation_id.clone(),
            target: first_target.clone(),
            last_sequence: 1,
            replication_boundary_lsn: 0,
            durable_lsn: 20,
            completed: true,
            catch_up_boundary_lsn: None,
        };
        snapshot.replicas.insert(
            ReplicaObservationKey::new(
                member.identity.replica_id,
                member.identity.instance_id.clone(),
            ),
            ReplicaObservation {
                kubernetes: Some(KubernetesReplicaObservation {
                    replica_id: member.identity.replica_id,
                    pod_name: format!("pod-{replica_id}"),
                    pod_uid: Some(PodUid::new(member.identity.instance_id.as_str())),
                    pvc_name: format!("pvc-{replica_id}"),
                    pvc_uid: Some(PvcUid::new(format!("pvc-{replica_id}"))),
                    image: Some("example:v1".to_string()),
                    pod_ready: true,
                    peer_endpoint_ready: true,
                }),
                agent: AgentObservation::Report(Box::new(AgentReport {
                    protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                    resource_uid: snapshot.resource_uid.clone(),
                    identity: member.identity.clone(),
                    process_session_id: ProcessSessionId::new(format!("session-{replica_id}")),
                    report_sequence: 1,
                    role: member.role,
                    read_status: AccessStatus::Granted,
                    write_status: AccessStatus::ReconfigurationPending,
                    healthy: true,
                    epoch: current.epoch,
                    previous_configuration: Some(previous.clone()),
                    current_configuration: Some(current.clone()),
                    current_progress: progress,
                    committed_lsn: progress,
                    catch_up_capability: retained_from,
                    deactivated_lsn: Some(progress),
                    deactivation_epoch: Some(current.epoch),
                    builds: (replica_id == 2 || replica_id == 3)
                        .then_some(completed_build)
                        .into_iter()
                        .collect(),
                    ..AgentReport::default()
                })),
            },
        );
    }

    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("transition condition must be persisted");
    };
    let KubernetesChange::PersistStatus { status } = changes.last().unwrap() else {
        panic!("transition status");
    };
    snapshot.status = (**status).clone();

    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("completed repair must advance the serialized repair slot");
    };
    let KubernetesChange::PersistStatus { status } = changes.last().unwrap() else {
        panic!("completed repair status");
    };
    assert!(status.transition.as_ref().unwrap().repair.is_none());
    snapshot.status = (**status).clone();

    let mut next_target = None;
    for _ in 0..3 {
        let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
            panic!("second lagging member must receive a repair intent");
        };
        let KubernetesChange::PersistStatus { status } = changes.last().unwrap() else {
            panic!("second repair status");
        };
        next_target = status
            .transition
            .as_ref()
            .and_then(|transition| transition.repair.as_ref())
            .map(|repair| repair.target.replica_id);
        snapshot.status = (**status).clone();
        if next_target.is_some() {
            break;
        }
    }
    assert_eq!(next_target, Some(ReplicaId::new(4)));
}

#[test]
fn failover_current_only_keeps_secondary_write_access_non_primary() {
    let previous = configuration();
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        ReplicaId::new(2),
        previous
            .members
            .iter()
            .map(|member| ConfigurationMember {
                identity: member.identity.clone(),
                role: if member.identity.replica_id == ReplicaId::new(2) {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        2,
    );
    let transition_id = derive_transition_id(
        &ResourceUid::new("resource-uid"),
        TransitionKind::Failover,
        &current.configuration_id,
    );
    let mut snapshot = empty_snapshot(3);
    snapshot.routing.service_present = true;
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: previous.clone(),
        }),
        transition: Some(TransitionIntent {
            secondary_scale_down: None,
            secondary_removal_evidence: None,
            scale_up: None,
            scale_up_failover: None,
            transition_id: transition_id.clone(),
            kind: TransitionKind::Failover,
            spec_generation: 1,
            effective_policy: EffectivePolicy::fixed(3, 10).unwrap(),
            previous_configuration_id: Some(previous.configuration_id.clone()),
            current_configuration: current.clone(),
            election_lsn: Some(10),
            build_id: None,
            repair: None,
            switchover: None,
        }),
        primary_failure: Some(kuberic_protocol::types::PrimaryFailureObservation {
            primary: previous.members[0].identity.clone(),
            started_at_unix_seconds: 90,
        }),
        ..AcceptedStatus::default()
    };
    for replica_id in [2, 3] {
        let member = current
            .members
            .iter()
            .find(|member| member.identity.replica_id == ReplicaId::new(replica_id))
            .unwrap();
        let primary = replica_id == 2;
        snapshot.replicas.insert(
            ReplicaObservationKey::new(
                member.identity.replica_id,
                member.identity.instance_id.clone(),
            ),
            ReplicaObservation {
                kubernetes: Some(KubernetesReplicaObservation {
                    replica_id: member.identity.replica_id,
                    pod_name: format!("pod-{replica_id}"),
                    pod_uid: Some(PodUid::new(member.identity.instance_id.as_str())),
                    pvc_name: format!("pvc-{replica_id}"),
                    pvc_uid: Some(PvcUid::new(format!("pvc-{replica_id}"))),
                    image: Some("example:v1".to_string()),
                    pod_ready: true,
                    peer_endpoint_ready: true,
                }),
                agent: AgentObservation::Report(Box::new(AgentReport {
                    protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                    resource_uid: snapshot.resource_uid.clone(),
                    identity: member.identity.clone(),
                    process_session_id: ProcessSessionId::new(format!("session-{replica_id}")),
                    report_sequence: 1,
                    role: member.role,
                    read_status: AccessStatus::Granted,
                    write_status: if primary {
                        AccessStatus::Granted
                    } else {
                        AccessStatus::NotPrimary
                    },
                    healthy: true,
                    epoch: current.epoch,
                    previous_configuration: (!primary).then(|| previous.clone()),
                    current_configuration: Some(current.clone()),
                    current_progress: 10,
                    committed_lsn: 10,
                    catch_up_capability: Some(1),
                    deactivated_lsn: Some(10),
                    deactivation_epoch: Some(current.epoch),
                    retained_operation_id: primary.then(|| {
                        OperationId::new(format!(
                            "{}:current-only:{}",
                            transition_id, member.identity.replica_id
                        ))
                    }),
                    ..AgentReport::default()
                })),
            },
        );
    }
    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("transition condition must be persisted first");
    };
    let KubernetesChange::PersistStatus { status } = changes.last().unwrap() else {
        panic!("expected transition status");
    };
    snapshot.status = (**status).clone();
    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(command),
    } = evaluate(&snapshot, &EvaluationConfig::default())
    else {
        panic!("secondary must receive current-only completion");
    };
    assert_eq!(command.local_replica_id, ReplicaId::new(3));
    assert!(command.current_only);
    assert_eq!(
        command.primary_write_status,
        AccessStatus::ReconfigurationPending
    );
}

#[test]
fn failover_preserves_outstanding_replacement_membership_and_build_authority() {
    let previous = replacement_configuration(3);
    let replacement_identity = identity(3, "replacement-pod", "replacement-generation");
    let replacement = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        previous.primary_id,
        previous
            .members
            .iter()
            .map(|member| ConfigurationMember {
                identity: if member.identity.replica_id == ReplicaId::new(3) {
                    replacement_identity.clone()
                } else {
                    member.identity.clone()
                },
                role: member.role,
            })
            .collect(),
        previous.write_quorum,
    );
    let replacement_build = OperationId::new("replacement-build");
    let mut snapshot = empty_snapshot(3);
    snapshot.now_unix_seconds = 100;
    snapshot.routing.service_present = true;
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: previous.clone(),
        }),
        transition: Some(TransitionIntent {
            secondary_scale_down: None,
            secondary_removal_evidence: None,
            scale_up: None,
            scale_up_failover: None,
            transition_id: derive_transition_id(
                &snapshot.resource_uid,
                TransitionKind::Replacement,
                &replacement.configuration_id,
            ),
            kind: TransitionKind::Replacement,
            spec_generation: 1,
            effective_policy: EffectivePolicy::fixed(3, 10).unwrap(),
            previous_configuration_id: Some(previous.configuration_id.clone()),
            current_configuration: replacement.clone(),
            election_lsn: None,
            build_id: Some(replacement_build.clone()),
            repair: None,
            switchover: None,
        }),
        primary_failure: Some(kuberic_protocol::types::PrimaryFailureObservation {
            primary: configuration_primary_for_test(&previous).identity.clone(),
            started_at_unix_seconds: 0,
        }),
        ..AcceptedStatus::default()
    };
    freeze_replacement_fixture(&mut snapshot, &previous.members[2].identity);
    let receipt = snapshot.status.pending_replacement_cleanup.clone();
    for (member, progress) in replacement
        .members
        .iter()
        .filter(|member| member.identity.replica_id != previous.primary_id)
        .zip([20, 10])
    {
        snapshot.replicas.insert(
            ReplicaObservationKey::new(
                member.identity.replica_id,
                member.identity.instance_id.clone(),
            ),
            ReplicaObservation {
                kubernetes: Some(KubernetesReplicaObservation {
                    replica_id: member.identity.replica_id,
                    pod_name: format!("pod-{}", member.identity.replica_id),
                    pod_uid: Some(PodUid::new(member.identity.instance_id.as_str())),
                    pvc_name: format!("pvc-{}", member.identity.replica_id),
                    pvc_uid: Some(PvcUid::new(format!("pvc-{}", member.identity.replica_id))),
                    image: Some("example:v1".to_string()),
                    pod_ready: true,
                    peer_endpoint_ready: true,
                }),
                agent: AgentObservation::Report(Box::new(AgentReport {
                    protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                    resource_uid: snapshot.resource_uid.clone(),
                    identity: member.identity.clone(),
                    process_session_id: ProcessSessionId::new(format!(
                        "session-{}",
                        member.identity.replica_id
                    )),
                    report_sequence: 1,
                    role: member.role,
                    read_status: AccessStatus::Granted,
                    write_status: AccessStatus::NotPrimary,
                    healthy: true,
                    epoch: replacement.epoch,
                    previous_configuration: Some(previous.clone()),
                    current_configuration: Some(replacement.clone()),
                    current_progress: progress,
                    committed_lsn: progress,
                    catch_up_capability: Some(1),
                    ..AgentReport::default()
                })),
            },
        );
    }
    let old_target = previous
        .members
        .iter()
        .find(|member| member.identity.replica_id == ReplicaId::new(3))
        .unwrap();
    snapshot.replicas.insert(
        ReplicaObservationKey::new(
            old_target.identity.replica_id,
            old_target.identity.instance_id.clone(),
        ),
        ReplicaObservation {
            kubernetes: Some(KubernetesReplicaObservation {
                replica_id: old_target.identity.replica_id,
                pod_name: "old-pod-3".to_string(),
                pod_uid: Some(PodUid::new(old_target.identity.instance_id.as_str())),
                pvc_name: "old-pvc-3".to_string(),
                pvc_uid: Some(PvcUid::new("old-pvc-3")),
                image: Some("example:v1".to_string()),
                pod_ready: true,
                peer_endpoint_ready: true,
            }),
            agent: AgentObservation::Report(Box::new(AgentReport {
                protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                resource_uid: snapshot.resource_uid.clone(),
                identity: old_target.identity.clone(),
                process_session_id: ProcessSessionId::new("old-target-session"),
                report_sequence: 1,
                role: ReplicaRole::ActiveSecondary,
                read_status: AccessStatus::Granted,
                write_status: AccessStatus::NotPrimary,
                healthy: true,
                epoch: previous.epoch,
                current_configuration: Some(previous.clone()),
                current_progress: 10,
                committed_lsn: 10,
                catch_up_capability: Some(1),
                ..AgentReport::default()
            })),
        },
    );
    let failed_primary = configuration_primary_for_test(&previous);
    snapshot.replicas.insert(
        ReplicaObservationKey::new(
            failed_primary.identity.replica_id,
            failed_primary.identity.instance_id.clone(),
        ),
        ReplicaObservation {
            kubernetes: Some(KubernetesReplicaObservation {
                replica_id: failed_primary.identity.replica_id,
                pod_name: "pod-1".to_string(),
                pod_uid: Some(PodUid::new(failed_primary.identity.instance_id.as_str())),
                pvc_name: "pvc-1".to_string(),
                pvc_uid: Some(PvcUid::new("pvc-1")),
                image: Some("example:v1".to_string()),
                pod_ready: false,
                peer_endpoint_ready: false,
            }),
            agent: AgentObservation::Unreachable {
                message: "primary unavailable".to_string(),
            },
        },
    );

    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("transition condition must be persisted first");
    };
    let KubernetesChange::PersistStatus { status } = changes.last().unwrap() else {
        panic!("expected transition status");
    };
    snapshot.status = (**status).clone();
    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("replacement failover must persist carried authority");
    };
    let KubernetesChange::PersistStatus { status } = changes.last().unwrap() else {
        panic!("expected failover transition");
    };
    let failover = status.transition.as_ref().unwrap();
    assert_eq!(status.pending_replacement_cleanup, receipt);
    assert!(status.last_replacement.is_none());
    assert_eq!(validate_status(status), Ok(()));
    let mut altered = (**status).clone();
    altered
        .pending_replacement_cleanup
        .as_mut()
        .unwrap()
        .resources
        .endpoint = kuberic_protocol::types::CleanupResourceIdentity::Absent {
        name: receipt.as_ref().unwrap().resources.endpoint.name().into(),
    };
    assert!(
        validate_status(&altered).is_err(),
        "failover IDs bind the original endpoint UID"
    );
    assert_eq!(failover.kind, TransitionKind::Failover);
    assert_eq!(failover.build_id.as_ref(), Some(&replacement_build));
    assert_eq!(
        failover
            .current_configuration
            .members
            .iter()
            .map(|member| member.identity.clone())
            .collect::<std::collections::BTreeSet<_>>(),
        replacement
            .members
            .iter()
            .map(|member| member.identity.clone())
            .collect()
    );
    assert_eq!(failover.current_configuration.epoch, Epoch::new(0, 3));
}

#[test]
fn returned_stale_former_primary_is_corrected_under_the_accepted_failover_epoch() {
    let old = configuration();
    let accepted = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        ReplicaId::new(2),
        old.members
            .iter()
            .map(|member| ConfigurationMember {
                identity: member.identity.clone(),
                role: if member.identity.replica_id == ReplicaId::new(2) {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        old.write_quorum,
    );
    let mut snapshot = empty_snapshot(3);
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: accepted.clone(),
        }),
        ..AcceptedStatus::default()
    };
    attest_stable_topology(&mut snapshot, &accepted);
    let former_primary = configuration_primary_for_test(&old);
    let observation = snapshot
        .replicas
        .get_mut(&ReplicaObservationKey::new(
            former_primary.identity.replica_id,
            former_primary.identity.instance_id.clone(),
        ))
        .unwrap();
    let AgentObservation::Report(report) = &mut observation.agent else {
        panic!("former primary report");
    };
    report.role = ReplicaRole::Primary;
    report.write_status = AccessStatus::Granted;
    report.epoch = old.epoch;
    report.current_configuration = Some(old.clone());

    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(command),
    } = evaluate(&snapshot, &EvaluationConfig::default())
    else {
        panic!("stale former primary must receive exact accepted authority");
    };
    assert_eq!(command.local_replica_id, former_primary.identity.replica_id);
    assert_eq!(command.transition_kind, TransitionKind::Failover);
    assert_eq!(command.previous_configuration.as_ref(), Some(&old));
    assert_eq!(command.current_configuration, accepted);
    assert_eq!(
        command.primary_write_status,
        AccessStatus::ReconfigurationPending
    );
    assert!(!command.current_only);

    let observation = snapshot
        .replicas
        .get_mut(&ReplicaObservationKey::new(
            former_primary.identity.replica_id,
            former_primary.identity.instance_id.clone(),
        ))
        .unwrap();
    let AgentObservation::Report(report) = &mut observation.agent else {
        panic!("former primary report");
    };
    report.role = ReplicaRole::ActiveSecondary;
    report.write_status = AccessStatus::NotPrimary;
    report.epoch = accepted.epoch;
    report.previous_configuration = Some(old.clone());
    report.current_configuration = Some(accepted.clone());

    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(command),
    } = evaluate(&snapshot, &EvaluationConfig::default())
    else {
        panic!("corrected former primary must remove Previous Configuration");
    };
    assert!(command.current_only);
    assert!(command.previous_configuration.is_none());
    assert_eq!(command.current_configuration, accepted);
}

fn configuration_primary_for_test(configuration: &ConfigurationDescriptor) -> &ConfigurationMember {
    configuration
        .members
        .iter()
        .find(|member| member.identity.replica_id == configuration.primary_id)
        .unwrap()
}

#[test]
fn stale_replica_epoch_is_unsafe() {
    let configuration = configuration();
    let mut snapshot = empty_snapshot(3);
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: configuration.clone(),
        }),
        ..AcceptedStatus::default()
    };
    snapshot.replicas.insert(
        observation_key(1, "pod-1"),
        ReplicaObservation {
            kubernetes: None,
            agent: AgentObservation::Report(Box::new(AgentReport {
                protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                resource_uid: snapshot.resource_uid.clone(),
                identity: configuration.members[0].identity.clone(),
                process_session_id: ProcessSessionId::new("session"),
                report_sequence: 1,
                role: ReplicaRole::Primary,
                write_status: AccessStatus::Granted,
                healthy: true,
                epoch: Epoch::new(0, 0),
                previous_configuration: None,
                current_configuration: Some(configuration),
                current_progress: 1,
                committed_lsn: 1,
                catch_up_capability: Some(1),
                ..AgentReport::default()
            })),
        },
    );

    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Unsafe {
            reason: UnsafeReason::InvalidAcceptedAuthority(_),
            ..
        }
    ));
}

#[test]
fn conflicting_primary_claims_are_unsafe() {
    let configuration = configuration();
    let mut snapshot = empty_snapshot(3);
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: configuration.clone(),
        }),
        ..AcceptedStatus::default()
    };
    for replica_id in [ReplicaId::new(1), ReplicaId::new(2)] {
        let expected_identity = configuration
            .members
            .iter()
            .find(|member| member.identity.replica_id == replica_id)
            .unwrap()
            .identity
            .clone();
        snapshot.replicas.insert(
            ReplicaObservationKey::new(replica_id, expected_identity.instance_id.clone()),
            ReplicaObservation {
                kubernetes: None,
                agent: AgentObservation::Report(Box::new(AgentReport {
                    protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                    resource_uid: snapshot.resource_uid.clone(),
                    identity: expected_identity,
                    process_session_id: ProcessSessionId::new(format!("session-{replica_id}")),
                    report_sequence: 1,
                    role: ReplicaRole::Primary,
                    write_status: AccessStatus::Granted,
                    healthy: true,
                    epoch: configuration.epoch,
                    previous_configuration: None,
                    current_configuration: Some(configuration.clone()),
                    current_progress: 1,
                    committed_lsn: 1,
                    catch_up_capability: Some(1),
                    ..AgentReport::default()
                })),
            },
        );
    }

    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Unsafe {
            reason: UnsafeReason::InvalidAcceptedAuthority(_),
            ..
        }
    ));
}

#[test]
fn accepted_replica_cannot_report_another_incarnation() {
    let configuration = configuration();
    let mut snapshot = empty_snapshot(3);
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: configuration.clone(),
        }),
        ..AcceptedStatus::default()
    };
    snapshot.replicas.insert(
        observation_key(1, "replacement-pod"),
        ReplicaObservation {
            kubernetes: None,
            agent: AgentObservation::Report(Box::new(AgentReport {
                protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                resource_uid: snapshot.resource_uid.clone(),
                identity: identity(1, "replacement-pod", "replacement-generation"),
                process_session_id: ProcessSessionId::new("session"),
                report_sequence: 1,
                role: ReplicaRole::Primary,
                write_status: AccessStatus::Granted,
                healthy: true,
                epoch: configuration.epoch,
                previous_configuration: None,
                current_configuration: Some(configuration),
                current_progress: 1,
                committed_lsn: 1,
                catch_up_capability: Some(1),
                ..AgentReport::default()
            })),
        },
    );

    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Unsafe {
            reason: UnsafeReason::InvalidAcceptedAuthority(_),
            ..
        }
    ));
}

#[test]
fn wrong_generation_cannot_prove_ready_or_publish_routing() {
    let configuration = configuration();
    for routing_present in [false, true] {
        let mut snapshot = empty_snapshot(3);
        snapshot.status = AcceptedStatus {
            initialized: true,
            effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
            topology: Some(AcceptedTopology {
                configuration: configuration.clone(),
            }),
            ..AcceptedStatus::default()
        };
        attest_stable_topology(&mut snapshot, &configuration);
        if !routing_present {
            snapshot.routing.write_target = None;
        }
        for replica_id in [ReplicaId::new(2), ReplicaId::new(3)] {
            let member = configuration
                .members
                .iter()
                .find(|member| member.identity.replica_id == replica_id)
                .unwrap();
            let observation = snapshot
                .replicas
                .get_mut(&ReplicaObservationKey::new(
                    replica_id,
                    member.identity.instance_id.clone(),
                ))
                .unwrap();
            let AgentObservation::Report(report) = &mut observation.agent else {
                unreachable!();
            };
            report.identity.agent_generation =
                AgentGeneration::new(format!("wrong-generation-{}", replica_id.value()));
        }

        assert!(matches!(
            evaluate(&snapshot, &EvaluationConfig::default()),
            Plan::Unsafe {
                reason: UnsafeReason::InvalidAcceptedAuthority(_),
                safety_changes,
                ..
            } if safety_changes == vec![SafetyChange::RemoveWriteRouting]
        ));
    }
}

#[test]
fn accepted_incarnation_missing_its_store_is_unsafe() {
    let configuration = configuration();
    let mut snapshot = empty_snapshot(3);
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: configuration.clone(),
        }),
        ..AcceptedStatus::default()
    };
    let primary = &configuration.members[0].identity;
    snapshot.replicas.insert(
        ReplicaObservationKey::new(primary.replica_id, primary.instance_id.clone()),
        ReplicaObservation {
            kubernetes: Some(KubernetesReplicaObservation {
                replica_id: primary.replica_id,
                pod_name: "primary".to_string(),
                pod_uid: Some(PodUid::new(primary.instance_id.as_str())),
                pvc_name: "primary".to_string(),
                pvc_uid: Some(PvcUid::new("established-pvc")),
                image: Some("example:v1".to_string()),
                pod_ready: true,
                peer_endpoint_ready: true,
            }),
            agent: AgentObservation::Uninitialized(UninitializedAgentObservation {
                protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                resource_uid: snapshot.resource_uid.clone(),
                replica_id: primary.replica_id,
                pod_uid: PodUid::new(primary.instance_id.as_str()),
                pvc_uid: PvcUid::new("established-pvc"),
                process_session_id: ProcessSessionId::new("lost-store-session"),
                report_sequence: 1,
            }),
        },
    );
    snapshot.routing.write_target = Some(primary.clone());

    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Unsafe {
            reason: UnsafeReason::InvalidAcceptedAuthority(_),
            safety_changes,
            ..
        } if safety_changes == vec![SafetyChange::RemoveWriteRouting]
    ));
}

#[test]
fn bootstrap_prevalidates_later_uninitialized_fences() {
    let mut snapshot = scaffolded_snapshot();
    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("expected bootstrap persistence");
    };
    let KubernetesChange::PersistStatus { status } = changes[0].clone() else {
        panic!("expected persisted status");
    };
    snapshot.status = *status;

    for id in [1, 2] {
        let observation = snapshot
            .replicas
            .get_mut(&observation_key(id, &format!("pod-uid-{id}")))
            .unwrap();
        let pvc_uid = if id == 2 {
            PvcUid::new("different-pvc")
        } else {
            PvcUid::new("pvc-uid-1")
        };
        observation.kubernetes.as_mut().unwrap().pvc_uid = Some(pvc_uid.clone());
        observation.agent = AgentObservation::Uninitialized(UninitializedAgentObservation {
            protocol_version: kuberic_protocol::PROTOCOL_VERSION,
            resource_uid: snapshot.resource_uid.clone(),
            replica_id: ReplicaId::new(id),
            pod_uid: PodUid::new(format!("pod-uid-{id}")),
            pvc_uid,
            process_session_id: ProcessSessionId::new(format!("session-{id}")),
            report_sequence: 1,
        });
    }

    fn verify_replacement_provisions_exact_target_builds_then_freezes_pc_cc() {
        let accepted = replacement_configuration(2);
        let replacing = accepted
            .members
            .iter()
            .find(|member| member.identity.replica_id != accepted.primary_id)
            .unwrap()
            .identity
            .clone();
        let mut snapshot = empty_snapshot(3);
        snapshot.status = AcceptedStatus {
            initialized: true,
            effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
            topology: Some(AcceptedTopology {
                configuration: accepted.clone(),
            }),
            ..AcceptedStatus::default()
        };
        attest_stable_topology(&mut snapshot, &accepted);
        snapshot.replicas.remove(&ReplicaObservationKey::new(
            replacing.replica_id,
            replacing.instance_id.clone(),
        ));
        snapshot.replicas.insert(
            ReplicaObservationKey::new(
                replacing.replica_id,
                ReplicaInstanceId::new("orphan-old-pvc"),
            ),
            ReplicaObservation {
                kubernetes: Some(KubernetesReplicaObservation {
                    replica_id: replacing.replica_id,
                    pod_name: String::new(),
                    pod_uid: None,
                    pvc_name: "old-data".to_string(),
                    pvc_uid: Some(PvcUid::new("old-pvc")),
                    image: Some("example:v1".to_string()),
                    pod_ready: false,
                    peer_endpoint_ready: false,
                }),
                agent: AgentObservation::Absent,
            },
        );
        snapshot
            .secondary_scale_down_resources
            .push(replacement_resources(&snapshot.resource_uid, &replacing));
        let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
            panic!("freeze cleanup before scaffolding");
        };
        let KubernetesChange::PersistStatus { status } = changes.into_iter().next().unwrap() else {
            panic!("durable admission");
        };
        assert!(status.pending_replacement_cleanup.is_some());
        snapshot.status = *status;
        assert!(matches!(
            evaluate(&snapshot, &EvaluationConfig::default()),
            Plan::Apply { changes }
                if matches!(
                    changes.as_slice(),
                    [KubernetesChange::EnsureReplacementScaffolding { replica_id, replacing: old }]
                        if *replica_id == replacing.replica_id && old == &replacing
                )
        ));

        let pod_uid = PodUid::new("replacement-pod");
        let pvc_uid = PvcUid::new("replacement-pvc");
        let initialization_id = kuberic_protocol::types::derive_initialization_id(
            &snapshot.resource_uid,
            replacing.replica_id,
            &pod_uid,
            &pvc_uid,
        );
        let target = ReplicaIdentity {
            replica_id: replacing.replica_id,
            instance_id: ReplicaInstanceId::new(pod_uid.as_str()),
            agent_generation: derive_agent_generation(&initialization_id),
        };
        snapshot.replicas.insert(
            ReplicaObservationKey::new(target.replica_id, target.instance_id.clone()),
            ReplicaObservation {
                kubernetes: Some(KubernetesReplicaObservation {
                    replica_id: target.replica_id,
                    pod_name: "replacement".to_string(),
                    pod_uid: Some(pod_uid.clone()),
                    pvc_name: "replacement-data".to_string(),
                    pvc_uid: Some(pvc_uid.clone()),
                    image: Some("example:v1".to_string()),
                    pod_ready: true,
                    peer_endpoint_ready: true,
                }),
                agent: AgentObservation::Uninitialized(UninitializedAgentObservation {
                    protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                    resource_uid: snapshot.resource_uid.clone(),
                    replica_id: target.replica_id,
                    pod_uid,
                    pvc_uid,
                    process_session_id: ProcessSessionId::new("replacement-session"),
                    report_sequence: 1,
                }),
            },
        );
        let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
            panic!("replacement target must be persisted outside authority");
        };
        let provisioning_status = changes
            .into_iter()
            .find_map(|change| match change {
                KubernetesChange::PersistStatus { status } => Some(*status),
                _ => None,
            })
            .expect("replacement provisioning status");
        let provisioning = provisioning_status.provisioning.clone().unwrap();
        assert_eq!(provisioning.replacement(), Some(&replacing));
        assert_eq!(provisioning.instance_id(), target.instance_id);

        snapshot.status = provisioning_status;
        let primary = accepted
            .members
            .iter()
            .find(|member| member.identity.replica_id == accepted.primary_id)
            .unwrap()
            .identity
            .clone();
        let build = AgentBuildReport {
            build_id: provisioning.operation_id.clone(),
            target: target.clone(),
            last_sequence: 1,
            replication_boundary_lsn: 0,
            durable_lsn: 5,
            completed: true,
            catch_up_boundary_lsn: None,
        };
        snapshot.replicas.insert(
            ReplicaObservationKey::new(primary.replica_id, primary.instance_id.clone()),
            ReplicaObservation {
                kubernetes: None,
                agent: AgentObservation::Report(Box::new(AgentReport {
                    protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                    resource_uid: snapshot.resource_uid.clone(),
                    identity: primary,
                    process_session_id: ProcessSessionId::new("primary-session"),
                    report_sequence: 1,
                    role: ReplicaRole::Primary,
                    read_status: AccessStatus::Granted,
                    write_status: AccessStatus::Granted,
                    healthy: true,
                    epoch: accepted.epoch,
                    current_configuration: Some(accepted.clone()),
                    current_progress: 5,
                    committed_lsn: 5,
                    builds: vec![build.clone()],
                    ..AgentReport::default()
                })),
            },
        );
        snapshot.replicas.insert(
            ReplicaObservationKey::new(target.replica_id, target.instance_id.clone()),
            ReplicaObservation {
                kubernetes: None,
                agent: AgentObservation::Report(Box::new(AgentReport {
                    protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                    resource_uid: snapshot.resource_uid.clone(),
                    identity: target.clone(),
                    process_session_id: ProcessSessionId::new("target-session"),
                    report_sequence: 2,
                    role: ReplicaRole::IdleSecondary,
                    write_status: AccessStatus::NotPrimary,
                    healthy: true,
                    builds: vec![build],
                    ..AgentReport::default()
                })),
            },
        );
        let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
            panic!("completed build must freeze replacement PC/CC");
        };
        let transition = changes.into_iter().find_map(|change| match change {
            KubernetesChange::PersistStatus { status } => status.transition,
            _ => None,
        });
        let transition = transition.expect("replacement transition");
        assert_eq!(transition.kind, TransitionKind::Replacement);
        assert_eq!(transition.current_configuration.members.len(), 3);
        assert!(
            transition
                .current_configuration
                .members
                .iter()
                .any(|member| member.identity == target)
        );
    }
    verify_replacement_provisions_exact_target_builds_then_freezes_pc_cc();

    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("expected transition condition persistence");
    };
    let KubernetesChange::PersistStatus { status } = changes[0].clone() else {
        panic!("expected persisted transition status");
    };
    snapshot.status = *status;

    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Unsafe {
            reason: UnsafeReason::ContradictoryReplicaEvidence(_),
            ..
        }
    ));
}

#[test]
fn transition_report_previous_configuration_must_match_frozen_topology() {
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 5),
        ReplicaId::new(1),
        configuration().members,
        2,
    );
    let mut current_members = previous.members.clone();
    current_members[2].identity = identity(3, "replacement-pod", "replacement-generation");
    let current =
        ConfigurationDescriptor::new(Epoch::new(0, 6), ReplicaId::new(1), current_members, 2);
    let mut unauthorized_previous_members = previous.members.clone();
    unauthorized_previous_members[1].identity =
        identity(2, "unauthorized-pod", "unauthorized-generation");
    let unauthorized_previous = ConfigurationDescriptor::new(
        previous.epoch,
        previous.primary_id,
        unauthorized_previous_members,
        previous.write_quorum,
    );
    let policy = EffectivePolicy::fixed(3, 10).unwrap();
    let mut snapshot = empty_snapshot(3);
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: previous.clone(),
        }),
        transition: Some(TransitionIntent {
            secondary_scale_down: None,
            secondary_removal_evidence: None,
            scale_up: None,
            scale_up_failover: None,
            transition_id: derive_transition_id(
                &snapshot.resource_uid,
                TransitionKind::Replacement,
                &current.configuration_id,
            ),
            kind: TransitionKind::Replacement,
            spec_generation: 1,
            effective_policy: policy,
            previous_configuration_id: Some(previous.configuration_id.clone()),
            current_configuration: current.clone(),
            election_lsn: None,
            build_id: Some(OperationId::new("replacement-build")),
            repair: None,
            switchover: None,
        }),
        ..AcceptedStatus::default()
    };
    let replacement = current.members[2].clone();
    snapshot.replicas.insert(
        ReplicaObservationKey::new(
            replacement.identity.replica_id,
            replacement.identity.instance_id.clone(),
        ),
        ReplicaObservation {
            kubernetes: None,
            agent: AgentObservation::Report(Box::new(AgentReport {
                protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                resource_uid: snapshot.resource_uid.clone(),
                identity: replacement.identity,
                process_session_id: ProcessSessionId::new("replacement-session"),
                report_sequence: 1,
                role: replacement.role,
                write_status: AccessStatus::NotPrimary,
                healthy: true,
                epoch: current.epoch,
                previous_configuration: Some(unauthorized_previous),
                current_configuration: Some(current),
                current_progress: 1,
                committed_lsn: 1,
                catch_up_capability: Some(1),
                ..AgentReport::default()
            })),
        },
    );

    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Unsafe {
            reason: UnsafeReason::InvalidAcceptedAuthority(_),
            ..
        }
    ));
}

#[test]
fn bootstrap_report_cannot_claim_previous_configuration() {
    let mut snapshot = scaffolded_snapshot();
    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("expected bootstrap persistence");
    };
    let KubernetesChange::PersistStatus { status } = changes[0].clone() else {
        panic!("expected persisted status");
    };
    snapshot.status = *status;
    let transition = snapshot.status.transition.as_ref().unwrap();
    let member = transition.current_configuration.members[0].clone();
    snapshot
        .replicas
        .get_mut(&observation_key(1, "pod-uid-1"))
        .unwrap()
        .agent = AgentObservation::Report(Box::new(AgentReport {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: snapshot.resource_uid.clone(),
        identity: member.identity,
        process_session_id: ProcessSessionId::new("bootstrap-session"),
        report_sequence: 1,
        role: member.role,
        write_status: AccessStatus::ReconfigurationPending,
        healthy: true,
        epoch: transition.current_configuration.epoch,
        previous_configuration: Some(configuration()),
        current_configuration: Some(transition.current_configuration.clone()),
        current_progress: 0,
        committed_lsn: 0,
        catch_up_capability: Some(0),
        ..AgentReport::default()
    }));

    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Unsafe {
            reason: UnsafeReason::InvalidAcceptedAuthority(_),
            ..
        }
    ));
}

#[test]
fn replacement_cleanup_does_not_replace_a_healthy_accepted_incarnation() {
    use kuberic_protocol::observation::ExactResourceObservation;
    use kuberic_protocol::types::ReplacementCleanup;
    let accepted = configuration();
    let member = accepted
        .members
        .iter()
        .find(|member| member.identity.replica_id != accepted.primary_id)
        .unwrap()
        .clone();
    let mut snapshot = empty_snapshot(3);
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: accepted.clone(),
        }),
        ..AcceptedStatus::default()
    };
    attest_stable_topology(&mut snapshot, &accepted);
    snapshot.replicas.insert(
        ReplicaObservationKey::new(
            member.identity.replica_id,
            member.identity.instance_id.clone(),
        ),
        ReplicaObservation {
            kubernetes: Some(KubernetesReplicaObservation {
                replica_id: member.identity.replica_id,
                pod_name: "replacement".to_string(),
                pod_uid: Some(PodUid::new(member.identity.instance_id.as_str())),
                pvc_name: "replacement-data".to_string(),
                pvc_uid: Some(PvcUid::new("replacement-pvc")),
                image: Some("example:v1".to_string()),
                pod_ready: true,
                peer_endpoint_ready: true,
            }),
            agent: AgentObservation::Report(Box::new(AgentReport {
                protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                resource_uid: snapshot.resource_uid.clone(),
                identity: member.identity.clone(),
                process_session_id: ProcessSessionId::new("replacement-session"),
                report_sequence: 1,
                role: member.role,
                write_status: AccessStatus::NotPrimary,
                healthy: true,
                epoch: accepted.epoch,
                current_configuration: Some(accepted),
                ..AgentReport::default()
            })),
        },
    );
    snapshot.replicas.insert(
        ReplicaObservationKey::new(
            member.identity.replica_id,
            ReplicaInstanceId::new("orphan-old-pvc"),
        ),
        ReplicaObservation {
            kubernetes: Some(KubernetesReplicaObservation {
                replica_id: member.identity.replica_id,
                pod_name: String::new(),
                pod_uid: None,
                pvc_name: "old-data".to_string(),
                pvc_uid: Some(PvcUid::new("old-pvc")),
                image: Some("example:v1".to_string()),
                pod_ready: false,
                peer_endpoint_ready: false,
            }),
            agent: AgentObservation::Absent,
        },
    );

    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Apply { changes }
            if matches!(
                changes.as_slice(),
                [KubernetesChange::DeleteReplicaScaffolding { pvc_name: Some(name), .. }]
                    if name == "old-data"
            )
    ));
    let enabled = EvaluationConfig {
        enable_secondary_scale_down: true,
        ..Default::default()
    };
    let deletes = |plan: Plan| {
        matches!(plan, Plan::Apply { changes }
        if changes.iter().any(|change| matches!(change, KubernetesChange::DeleteScaleDownResource { .. })))
    };
    assert!(!deletes(evaluate(&snapshot, &enabled)));
    let retired_pod = PodUid::new("old-pod");
    let retired = ReplicaIdentity {
        replica_id: member.identity.replica_id,
        instance_id: ReplicaInstanceId::new(retired_pod.as_str()),
        agent_generation: derive_agent_generation(&derive_initialization_id(
            &snapshot.resource_uid,
            member.identity.replica_id,
            &retired_pod,
            &PvcUid::new("old-pvc"),
        )),
    };
    let mut exact = replacement_resources(&snapshot.resource_uid, &retired);
    exact.pvc = ExactResourceObservation::FrozenUidPresent {
        resource_version: "1".into(),
    };
    snapshot.status.last_replacement = Some(ReplacementCleanup {
        resource_uid: snapshot.resource_uid.clone(),
        target: retired,
        resources: exact.identity.clone(),
    });
    snapshot.secondary_scale_down_resources.push(exact);
    let mut wrong_storage = snapshot.status.clone();
    wrong_storage
        .last_replacement
        .as_mut()
        .unwrap()
        .resources
        .pvc = kuberic_protocol::types::CleanupResourceIdentity::Present {
        name: "old-data".into(),
        uid: "another-incarnations-storage".into(),
    };
    assert!(validate_status(&wrong_storage).is_err());
    assert!(deletes(evaluate(&snapshot, &enabled)));
    let mut failed_again = snapshot.clone();
    let AgentObservation::Report(report) = &mut failed_again
        .replicas
        .get_mut(&ReplicaObservationKey::new(
            member.identity.replica_id,
            member.identity.instance_id.clone(),
        ))
        .unwrap()
        .agent
    else {
        unreachable!()
    };
    report.reported_fault = Some(kuberic_protocol::types::FaultType::Permanent);
    assert!(
        deletes(evaluate(&failed_again, &enabled)),
        "a second replacement must not overtake the first cleanup obligation"
    );
    snapshot.secondary_scale_down_resources[0].pvc = ExactResourceObservation::ReplacementPresent {
        uid: "same-name-different-uid".into(),
        resource_version: "2".into(),
    };
    assert!(!deletes(evaluate(&snapshot, &enabled)));
    snapshot.status.last_replacement.as_mut().unwrap().target = member.identity;
    assert!(validate_status(&snapshot.status).is_err());
}

fn replacement_resources(
    resource_uid: &ResourceUid,
    target: &ReplicaIdentity,
) -> kuberic_protocol::observation::SecondaryScaleDownResourceObservation {
    use kuberic_protocol::observation::{
        ExactResourceObservation, SecondaryScaleDownResourceObservation,
    };
    use kuberic_protocol::types::{
        CleanupResourceIdentity, ReplicaCleanupIdentity, derive_replica_endpoint_name,
    };
    SecondaryScaleDownResourceObservation {
        resource_uid: resource_uid.clone(),
        target: target.clone(),
        identity: ReplicaCleanupIdentity {
            pod: CleanupResourceIdentity::Present {
                name: "old".into(),
                uid: target.instance_id.to_string(),
            },
            pvc: CleanupResourceIdentity::Present {
                name: "old-data".into(),
                uid: "old-pvc".into(),
            },
            endpoint: CleanupResourceIdentity::Present {
                name: derive_replica_endpoint_name(resource_uid, target),
                uid: "old-endpoint".into(),
            },
        },
        pod: ExactResourceObservation::NotFound,
        pod_allocation_operation_id: None,
        pod_matches_allocation_metadata: false,
        pvc: ExactResourceObservation::NotFound,
        pvc_allocation_operation_id: None,
        endpoint: ExactResourceObservation::NotFound,
    }
}

#[test]
fn replacement_target_loss_before_cc_clears_provisioning() {
    let accepted = replacement_configuration(2);
    let replacing = accepted
        .members
        .iter()
        .find(|member| member.identity.replica_id != accepted.primary_id)
        .unwrap()
        .identity
        .clone();
    let pod_uid = PodUid::new("lost-target");
    let pvc_uid = PvcUid::new("lost-target-pvc");
    let mut snapshot = empty_snapshot(3);
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: accepted,
        }),
        provisioning: Some(ProvisioningIntent {
            purpose: ProvisioningPurpose::replacement(replacing.clone()),
            pod_uid,
            pvc_uid,
            operation_id: OperationId::new("lost-build"),
        }),
        ..AcceptedStatus::default()
    };
    freeze_replacement_fixture(&mut snapshot, &replacing);
    let primary = snapshot
        .status
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .members
        .iter()
        .find(|member| {
            member.identity.replica_id
                == snapshot
                    .status
                    .topology
                    .as_ref()
                    .unwrap()
                    .configuration
                    .primary_id
        })
        .unwrap()
        .clone();
    let configuration = snapshot
        .status
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .clone();
    snapshot.replicas.insert(
        ReplicaObservationKey::new(
            primary.identity.replica_id,
            primary.identity.instance_id.clone(),
        ),
        ReplicaObservation {
            kubernetes: None,
            agent: AgentObservation::Report(Box::new(AgentReport {
                protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                resource_uid: snapshot.resource_uid.clone(),
                identity: primary.identity,
                process_session_id: ProcessSessionId::new("source-session"),
                report_sequence: 1,
                role: ReplicaRole::Primary,
                write_status: AccessStatus::Granted,
                healthy: true,
                epoch: configuration.epoch,
                previous_configuration: None,
                current_configuration: Some(configuration),
                current_progress: 10,
                committed_lsn: 10,
                catch_up_capability: Some(1),
                ..AgentReport::default()
            })),
        },
    );

    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Apply { changes }
            if matches!(
                changes.as_slice(),
                [
                    KubernetesChange::DeleteReplicaScaffolding {
                        pod_uid: Some(_),
                        pvc_uid: Some(_),
                        ..
                    },
                    KubernetesChange::PersistStatus { status },
                ]
                    if status.provisioning.is_none() && status.transition.is_none()
            )
    ));
}

#[test]
fn primary_failure_abandons_pre_cc_provisioning_and_fences_routing() {
    let accepted = replacement_configuration(2);
    let replacing = accepted
        .members
        .iter()
        .find(|member| member.identity.replica_id != accepted.primary_id)
        .unwrap()
        .identity
        .clone();
    let primary = accepted
        .members
        .iter()
        .find(|member| member.identity.replica_id == accepted.primary_id)
        .unwrap()
        .identity
        .clone();
    let mut snapshot = empty_snapshot(3);
    snapshot.routing.write_target = Some(primary.clone());
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: accepted,
        }),
        provisioning: Some(ProvisioningIntent {
            purpose: ProvisioningPurpose::replacement(replacing.clone()),
            pod_uid: PodUid::new("abandoned-target"),
            pvc_uid: PvcUid::new("abandoned-target-pvc"),
            operation_id: OperationId::new("abandoned-build"),
        }),
        ..AcceptedStatus::default()
    };

    freeze_replacement_fixture(&mut snapshot, &replacing);
    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Apply { changes }
            if matches!(
                changes.as_slice(),
                [
                    KubernetesChange::DeleteReplicaScaffolding { .. },
                    KubernetesChange::RemoveWriteRouting,
                    KubernetesChange::PersistStatus { status },
                ] if status.provisioning.is_none()
                    && status.primary_failure.as_ref().is_some_and(|failure| failure.primary == primary)
            )
    ));
}

#[test]
fn replacement_accepts_current_only_quorum_with_missing_target() {
    let configuration = configuration();
    let mut members = configuration.members.clone();
    let replacing = members
        .iter_mut()
        .find(|m| m.identity.replica_id != configuration.primary_id)
        .unwrap();
    replacing.identity.agent_generation = derive_agent_generation(&derive_initialization_id(
        &ResourceUid::new("resource-uid"),
        replacing.identity.replica_id,
        &PodUid::new(replacing.identity.instance_id.as_str()),
        &PvcUid::new("old-pvc"),
    ));
    let previous = ConfigurationDescriptor::new(
        configuration.epoch,
        configuration.primary_id,
        members,
        configuration.write_quorum,
    );
    let replacing = previous
        .members
        .iter()
        .find(|member| member.identity.replica_id != previous.primary_id)
        .unwrap()
        .clone();
    let target = ConfigurationMember {
        identity: identity(
            replacing.identity.replica_id.value(),
            "missing-target",
            "missing-target-generation",
        ),
        role: ReplicaRole::ActiveSecondary,
    };
    let current = ConfigurationDescriptor::new(
        Epoch::new(
            previous.epoch.data_loss_number,
            previous.epoch.configuration_number + 1,
        ),
        previous.primary_id,
        previous
            .members
            .iter()
            .map(|member| {
                if member.identity == replacing.identity {
                    target.clone()
                } else {
                    member.clone()
                }
            })
            .collect(),
        previous.write_quorum,
    );
    let cleanup = kuberic_protocol::types::ReplacementCleanup {
        resource_uid: ResourceUid::new("resource-uid"),
        target: replacing.identity.clone(),
        resources: replacement_resources(&ResourceUid::new("resource-uid"), &replacing.identity)
            .identity,
    };
    let transition_id =
        cleanup.transition_id(TransitionKind::Replacement, &current.configuration_id);
    let mut snapshot = empty_snapshot(3);
    snapshot.status = AcceptedStatus {
        initialized: true,
        effective_policy: Some(EffectivePolicy::fixed(3, 10).unwrap()),
        topology: Some(AcceptedTopology {
            configuration: previous.clone(),
        }),
        transition: Some(TransitionIntent {
            secondary_scale_down: None,
            secondary_removal_evidence: None,
            scale_up: None,
            scale_up_failover: None,
            transition_id: transition_id.clone(),
            kind: TransitionKind::Replacement,
            spec_generation: 1,
            effective_policy: EffectivePolicy::fixed(3, 10).unwrap(),
            previous_configuration_id: Some(previous.configuration_id.clone()),
            current_configuration: current.clone(),
            election_lsn: None,
            build_id: Some(OperationId::new("replacement-build")),
            repair: None,
            switchover: None,
        }),
        ..AcceptedStatus::default()
    };
    snapshot.status.pending_replacement_cleanup = Some(cleanup);
    let mut resources = replacement_resources(&snapshot.resource_uid, &replacing.identity);
    resources.pvc = kuberic_protocol::observation::ExactResourceObservation::FrozenUidPresent {
        resource_version: "held-by-finalizer".into(),
    };
    snapshot.secondary_scale_down_resources.push(resources);
    for member in current
        .members
        .iter()
        .filter(|member| member.identity != target.identity)
    {
        snapshot.replicas.insert(
            ReplicaObservationKey::new(
                member.identity.replica_id,
                member.identity.instance_id.clone(),
            ),
            ReplicaObservation {
                kubernetes: None,
                agent: AgentObservation::Report(Box::new(AgentReport {
                    protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                    resource_uid: snapshot.resource_uid.clone(),
                    identity: member.identity.clone(),
                    process_session_id: ProcessSessionId::new(format!(
                        "session-{}",
                        member.identity.replica_id
                    )),
                    report_sequence: 1,
                    role: member.role,
                    read_status: AccessStatus::Granted,
                    write_status: if member.identity.replica_id == current.primary_id {
                        AccessStatus::Granted
                    } else {
                        AccessStatus::NotPrimary
                    },
                    healthy: true,
                    epoch: current.epoch,
                    current_configuration: Some(current.clone()),
                    retained_operation_id: Some(OperationId::new(format!(
                        "{}:current-only:{}",
                        transition_id, member.identity.replica_id
                    ))),
                    ..AgentReport::default()
                })),
            },
        );
    }
    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("transition condition must be persisted");
    };
    let KubernetesChange::PersistStatus { status } = changes[0].clone() else {
        panic!("transition status");
    };
    snapshot.status = *status;

    snapshot.replicas.insert(
        ReplicaObservationKey::new(
            target.identity.replica_id,
            target.identity.instance_id.clone(),
        ),
        ReplicaObservation {
            kubernetes: None,
            agent: AgentObservation::Report(Box::new(AgentReport {
                protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                resource_uid: snapshot.resource_uid.clone(),
                identity: target.identity.clone(),
                process_session_id: ProcessSessionId::new("returned-target"),
                report_sequence: 1,
                role: ReplicaRole::IdleSecondary,
                healthy: true,
                ..AgentReport::default()
            })),
        },
    );
    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(command)
        } if command.local_replica_id == target.identity.replica_id
            && !command.current_only
            && command.previous_configuration.as_ref() == Some(&previous)
            && command.current_configuration == current
    ));
    snapshot.replicas.remove(&ReplicaObservationKey::new(
        target.identity.replica_id,
        target.identity.instance_id.clone(),
    ));

    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("missing target quorum must accept Current Configuration");
    };
    let accepted = changes.into_iter().find_map(|change| match change {
        KubernetesChange::PersistStatus { status } if status.transition.is_none() => Some(*status),
        _ => None,
    });
    let accepted = accepted.expect("accepted replacement topology");
    snapshot.status = accepted;
    let first_receipt = snapshot.status.last_replacement.clone().unwrap();
    for _ in 0..3 {
        snapshot.status =
            serde_json::from_value(serde_json::to_value(&snapshot.status).unwrap()).unwrap();
        assert!(matches!(evaluate(&snapshot, &EvaluationConfig::default()),
            Plan::Apply { changes } if matches!(changes.as_slice(),
                [KubernetesChange::DeleteScaleDownResource { resource: kuberic_protocol::command::ScaleDownResource::Pvc, uid, .. }]
                if uid == "old-pvc")));
        assert_eq!(
            snapshot.status.last_replacement,
            Some(first_receipt.clone())
        );
        assert!(snapshot.status.provisioning.is_none());
    }
    snapshot.secondary_scale_down_resources[0].pvc =
        kuberic_protocol::observation::ExactResourceObservation::NotFound;
    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("exact absence must clear the cleanup gate");
    };
    let KubernetesChange::PersistStatus { status } = changes[0].clone() else {
        panic!("cleanup completion");
    };
    assert!(status.last_replacement.is_none());
    snapshot.status = *status;
    snapshot.replicas.insert(
        ReplicaObservationKey::new(
            target.identity.replica_id,
            target.identity.instance_id.clone(),
        ),
        ReplicaObservation {
            kubernetes: Some(KubernetesReplicaObservation {
                replica_id: target.identity.replica_id,
                pod_name: "returned-target".to_string(),
                pod_uid: Some(PodUid::new(target.identity.instance_id.as_str())),
                pvc_name: "returned-target-data".to_string(),
                pvc_uid: Some(PvcUid::new("returned-target-pvc")),
                image: Some("example:v1".to_string()),
                pod_ready: true,
                peer_endpoint_ready: true,
            }),
            agent: AgentObservation::Report(Box::new(AgentReport {
                protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                resource_uid: snapshot.resource_uid.clone(),
                identity: target.identity.clone(),
                process_session_id: ProcessSessionId::new("returned-target"),
                report_sequence: 2,
                role: ReplicaRole::IdleSecondary,
                healthy: true,
                ..AgentReport::default()
            })),
        },
    );
    assert!(
        matches!(
            evaluate(&snapshot, &EvaluationConfig::default()),
            Plan::Wait { .. }
        ),
        "the second replacement requires its own authoritative cleanup identity"
    );
}

#[test]
fn bootstrap_replacement_supersedes_only_a_never_installed_incarnation() {
    let mut snapshot = scaffolded_snapshot();
    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("bootstrap transition");
    };
    let KubernetesChange::PersistStatus { status } = changes[0].clone() else {
        panic!("bootstrap status");
    };
    snapshot.status = *status;
    let old = snapshot
        .status
        .transition
        .as_ref()
        .unwrap()
        .current_configuration
        .members[1]
        .identity
        .clone();
    snapshot.replicas.remove(&ReplicaObservationKey::new(
        old.replica_id,
        old.instance_id.clone(),
    ));
    snapshot.replicas.insert(
        ReplicaObservationKey::new(old.replica_id, ReplicaInstanceId::new("orphan-genesis")),
        ReplicaObservation {
            kubernetes: Some(KubernetesReplicaObservation {
                replica_id: old.replica_id,
                pod_name: String::new(),
                pod_uid: None,
                pvc_name: "old-genesis-data".to_string(),
                pvc_uid: Some(PvcUid::new("old-genesis-pvc")),
                image: Some("example:v1".to_string()),
                pod_ready: false,
                peer_endpoint_ready: false,
            }),
            agent: AgentObservation::Absent,
        },
    );
    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("transition condition persistence");
    };
    let KubernetesChange::PersistStatus { status } = changes[0].clone() else {
        panic!("transition status");
    };
    snapshot.status = *status;
    assert!(matches!(
        evaluate(&snapshot, &EvaluationConfig::default()),
        Plan::Apply { changes }
            if matches!(
                changes.as_slice(),
                [KubernetesChange::EnsureReplacementScaffolding { replacing, .. }]
                    if replacing == &old
            )
    ));

    let pod_uid = PodUid::new("new-genesis-pod");
    let pvc_uid = PvcUid::new("new-genesis-pvc");
    snapshot.replicas.insert(
        ReplicaObservationKey::new(old.replica_id, ReplicaInstanceId::new(pod_uid.as_str())),
        ReplicaObservation {
            kubernetes: Some(KubernetesReplicaObservation {
                replica_id: old.replica_id,
                pod_name: "new-genesis".to_string(),
                pod_uid: Some(pod_uid.clone()),
                pvc_name: "new-genesis-data".to_string(),
                pvc_uid: Some(pvc_uid.clone()),
                image: Some("example:v1".to_string()),
                pod_ready: true,
                peer_endpoint_ready: true,
            }),
            agent: AgentObservation::Uninitialized(UninitializedAgentObservation {
                protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                resource_uid: snapshot.resource_uid.clone(),
                replica_id: old.replica_id,
                pod_uid,
                pvc_uid,
                process_session_id: ProcessSessionId::new("new-genesis-session"),
                report_sequence: 1,
            }),
        },
    );
    let Plan::Apply { changes } = evaluate(&snapshot, &EvaluationConfig::default()) else {
        panic!("superseded bootstrap transition");
    };
    let transition = changes.into_iter().find_map(|change| match change {
        KubernetesChange::PersistStatus { status } => status.transition,
        _ => None,
    });
    let transition = transition.expect("superseded transition");
    assert_ne!(transition.current_configuration.members[1].identity, old);
    assert_eq!(transition.current_configuration.epoch, Epoch::new(0, 2));
}

#[test]
fn scale_up_converges_sequentially_for_supported_cardinalities() {
    use scale_up_model::Model;
    for (accepted, desired) in [(1, 2), (2, 3), (3, 4)] {
        let mut model = Model::new(accepted, desired);
        model.run(80);
        assert_eq!(model.accepted_count(), desired);
        assert_eq!(model.accepted_history, vec![accepted, desired]);
        assert!(model.snapshot.status.transition.is_none());
        assert!(model.snapshot.status.provisioning.is_none());
        assert!(model.snapshot.status.last_scale_up.is_some());
    }

    let mut sequential = Model::new(1, 3);
    sequential.run(160);
    assert_eq!(sequential.accepted_history, vec![1, 2, 3]);
}

#[test]
fn scale_up_cancellation_and_failed_build_freeze_exact_cleanup() {
    use scale_up_model::Model;
    let mut cancelled = Model::new(1, 2);
    while cancelled.snapshot.status.provisioning.is_none() {
        cancelled.step();
    }
    cancelled.snapshot.desired.replicas = 1;
    let Plan::Apply { changes } = cancelled.plan() else {
        panic!("cancellation must persist exact cleanup");
    };
    let status = changes.into_iter().find_map(|change| match change {
        KubernetesChange::PersistStatus { status } => Some(*status),
        _ => None,
    });
    let status = status.expect("cleanup status");
    assert!(status.provisioning.is_none());
    assert!(status.scale_up_cleanup.is_some());
    assert!(status.conditions.iter().any(|condition| {
        condition.reason == "ScaleUpCancelledBeforeAdmission"
            && condition.message.contains("phase=cleanup")
            && condition.message.contains("accepted=1")
            && condition.message.contains("desired=1")
    }));
    assert_eq!(
        status
            .topology
            .as_ref()
            .unwrap()
            .configuration
            .members
            .len(),
        1
    );

    let mut failed = Model::new(2, 3);
    while failed.snapshot.status.provisioning.is_none() {
        failed.step();
    }
    let target = failed
        .snapshot
        .status
        .provisioning
        .as_ref()
        .unwrap()
        .target_identity(&failed.snapshot.resource_uid);
    let observation = failed
        .snapshot
        .observation_for_identity(&target)
        .cloned()
        .unwrap();
    let mut report = AgentReport {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: failed.snapshot.resource_uid.clone(),
        identity: target.clone(),
        process_session_id: ProcessSessionId::new("failed-candidate"),
        report_sequence: 2,
        role: ReplicaRole::IdleSecondary,
        healthy: false,
        reported_fault: Some(FaultType::Permanent),
        ..Default::default()
    };
    report.write_status = AccessStatus::NotPrimary;
    failed
        .snapshot
        .replicas
        .get_mut(&ReplicaObservationKey::new(
            target.replica_id,
            target.instance_id.clone(),
        ))
        .unwrap()
        .agent = AgentObservation::Report(Box::new(report));
    assert!(observation.kubernetes.is_some());
    assert!(matches!(
        failed.plan(),
        Plan::Apply { changes }
            if changes.iter().any(|change| matches!(
                change,
                KubernetesChange::PersistStatus { status }
                    if status.scale_up_cleanup.is_some()
            ))
    ));
}

#[test]
fn scale_up_cleanup_retires_exact_source_build_before_service_restoration() {
    use scale_up_model::Model;

    let mut model = Model::new(1, 2);
    let build = loop {
        match model.plan() {
            Plan::Execute {
                command: ProtocolCommand::EnsureReplicaBuild(command),
            } if !command.retire => {
                model.execute(ProtocolCommand::EnsureReplicaBuild(command.clone()));
                break command;
            }
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            Plan::Execute { command } => model.execute(command),
            Plan::Wait { status, .. } => model.apply_wait(status),
            other => panic!("source build setup: {other:?}"),
        }
    };
    let build_operation_id = OperationId::new(format!("{}:build-replica", build.operation_id));
    let retirement_operation_id =
        OperationId::new(format!("{}:retire-abandoned-build", build.operation_id));
    model.snapshot.desired.replicas = 1;
    let Plan::Apply { changes } = model.plan() else {
        panic!("cancellation must freeze exact cleanup")
    };
    for change in changes {
        model.apply(change);
    }
    assert!(model.snapshot.status.scale_up_cleanup.is_some());

    let assert_retirement = |plan: Plan| {
        let Plan::Execute {
            command: ProtocolCommand::EnsureReplicaBuild(retirement),
        } = plan
        else {
            panic!("cleanup must retire the exact source build before restoration")
        };
        assert!(retirement.retire);
        assert_eq!(retirement.operation_id, build.operation_id);
        assert_eq!(retirement.local_replica_id, build.local_replica_id);
        assert_eq!(retirement.expected_instance_id, build.expected_instance_id);
        assert_eq!(
            retirement.expected_agent_generation,
            build.expected_agent_generation
        );
        assert_eq!(retirement.target, build.target);
        assert!(retirement.authority.is_none());
        assert!(retirement.source_session_id.is_none());
    };

    let mut visible = model.fork();
    visible
        .report_mut(build.local_replica_id.value())
        .write_status = AccessStatus::ReconfigurationPending;
    assert_retirement(visible.plan());

    let mut hidden_pending = model.fork();
    hidden_pending
        .report_mut(build.local_replica_id.value())
        .builds
        .clear();
    assert_eq!(
        hidden_pending
            .report_mut(build.local_replica_id.value())
            .pending_operation_id,
        Some(build_operation_id.clone())
    );
    assert_retirement(hidden_pending.plan());

    let mut hidden_consumed = hidden_pending.fork();
    let source = hidden_consumed.report_mut(build.local_replica_id.value());
    source.pending_operation_id = None;
    source.retained_operation_id = Some(build_operation_id.clone());
    assert_retirement(hidden_consumed.plan());

    let mut pending_retirement = hidden_pending.fork();
    pending_retirement
        .report_mut(build.local_replica_id.value())
        .pending_operation_id = Some(retirement_operation_id.clone());
    assert_retirement(pending_retirement.plan());

    let mut unrelated = hidden_pending.fork();
    unrelated
        .report_mut(build.local_replica_id.value())
        .pending_operation_id = Some(OperationId::new("unrelated-pending"));
    assert!(matches!(
        unrelated.plan(),
        Plan::Wait { status, .. }
            if status.conditions.iter().any(|condition|
                matches!(
                    condition.reason.as_str(),
                    "ScaleUpSourceBuildRetirementBlocked"
                        | "ScaleUpHistoricalAcceptancePending"
                ))
    ));

    let mut retired = hidden_pending;
    let source = retired.report_mut(build.local_replica_id.value());
    source.pending_operation_id = None;
    source.retained_operation_id = Some(retirement_operation_id);
    assert!(matches!(
        retired.plan(),
        Plan::Apply { changes }
            if matches!(
                changes.as_slice(),
                [KubernetesChange::DeleteScaleDownResource {
                    resource: ScaleDownResource::Endpoint,
                    ..
                }]
            )
    ));
}

#[test]
fn scale_up_primary_loss_before_and_after_pc_cc_follow_distinct_paths() {
    use scale_up_model::Model;
    let mut before = Model::new(2, 3);
    while before.snapshot.status.provisioning.is_none() {
        before.step();
    }
    let primary = before
        .snapshot
        .status
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .primary_id;
    before.report_mut(primary.value()).reported_fault = Some(FaultType::Permanent);
    before.report_mut(primary.value()).write_status = AccessStatus::ReconfigurationPending;
    assert!(matches!(
        before.plan(),
        Plan::Apply { changes }
            if changes.iter().any(|change| matches!(
                change,
                KubernetesChange::PersistStatus { status }
                    if status.scale_up_cleanup.is_none()
                        && status.provisioning.is_some()
                        && status.primary_failure.is_some()
            ))
    ));

    let mut after = Model::new(2, 3);
    loop {
        let plan = after.plan();
        let pc_cc_primary_installed = after
            .snapshot
            .status
            .transition
            .as_ref()
            .and_then(|transition| transition.scale_up.as_deref())
            .is_some_and(|intent| {
                after
                    .snapshot
                    .observation_for_identity(&intent.primary)
                    .and_then(|observation| match &observation.agent {
                        AgentObservation::Report(report) => Some(report),
                        _ => None,
                    })
                    .is_some_and(|report| report.previous_configuration.is_some())
            });
        if pc_cc_primary_installed {
            break;
        }
        match plan {
            Plan::Apply { changes } => {
                for change in changes {
                    after.apply(change);
                }
            }
            Plan::Execute { command } => after.execute(command),
            Plan::Wait { status, .. } => after.apply_wait(status),
            other => panic!("unexpected pre-PC/CC plan: {other:?}"),
        }
    }
    after.report_mut(1).reported_fault = Some(FaultType::Permanent);
    after.report_mut(1).write_status = AccessStatus::ReconfigurationPending;
    let Plan::Apply { changes } = after.plan() else {
        panic!("post-PC/CC primary loss must persist carried failover");
    };
    let status = changes.into_iter().find_map(|change| match change {
        KubernetesChange::PersistStatus { status } => Some(*status),
        _ => None,
    });
    let status = status.expect("failover status");
    assert!(status.scale_up_cleanup.is_none());
    assert!(
        status
            .transition
            .as_ref()
            .is_some_and(|transition| transition.scale_up_failover.is_some())
    );
    assert!(status.conditions.iter().any(|condition| {
        condition.reason == "ScaleUpFailoverPersisted"
            && condition.message.contains("phase=failover-recovery")
    }));
    after.snapshot.status = status;
    for _ in 0..40 {
        match after.plan() {
            Plan::Apply { changes } => {
                for change in changes {
                    after.apply(change);
                }
            }
            Plan::Execute { command } => after.execute(command),
            Plan::Wait { status, .. } => after.apply_wait(status),
            Plan::Stable { status, .. } => after.snapshot.status = status,
            Plan::Unsafe { reason, .. } => panic!("carried failover became unsafe: {reason:?}"),
        }
        if after.accepted_count() == 3 && after.snapshot.status.transition.is_none() {
            break;
        }
    }
    assert_eq!(after.accepted_count(), 3, "{:?}", after.plan());
    assert!(after.snapshot.status.transition.is_none());
}

#[test]
fn carried_failover_fences_before_selecting_the_acknowledged_tail() {
    use scale_up_model::Model;

    let mut model = Model::new(4, 5);
    model.truncate_durable_history(8);
    loop {
        let pc_cc_complete = model
            .snapshot
            .status
            .transition
            .as_ref()
            .and_then(|transition| transition.scale_up.as_deref())
            .is_some_and(|intent| {
                intent.current_configuration.members.iter().all(|member| {
                    model
                        .snapshot
                        .observation_for_identity(&member.identity)
                        .and_then(|observation| match &observation.agent {
                            AgentObservation::Report(report) => Some(report.as_ref()),
                            _ => None,
                        })
                        .is_some_and(|report| {
                            report.previous_configuration.as_ref()
                                == Some(&intent.previous_configuration)
                                && report.current_configuration.as_ref()
                                    == Some(&intent.current_configuration)
                                && report.pending_operation_id.is_none()
                        })
                })
            });
        if pc_cc_complete {
            break;
        }
        model.step();
    }
    assert_eq!(model.report_mut(2).current_progress, 10);
    model.report_mut(1).reported_fault = Some(FaultType::Permanent);
    model.report_mut(1).write_status = AccessStatus::ReconfigurationPending;
    let Plan::Apply { changes } = model.plan() else {
        panic!("primary failure must persist provisional carried failover")
    };
    for change in changes {
        model.apply(change);
    }
    let provisional = model.snapshot.status.transition.as_ref().unwrap().clone();
    assert_eq!(
        provisional.current_configuration.primary_id,
        ReplicaId::new(2)
    );
    assert_eq!(provisional.election_lsn, None);
    assert!(model.snapshot.routing.write_target.is_none());

    let acknowledged = model.acknowledge_old_scale_up_authority_write(41, 99, &[1, 3, 4]);
    assert_eq!(acknowledged, 11);
    assert_eq!(model.report_mut(2).current_progress, 10);
    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(first_fence),
    } = model.plan()
    else {
        panic!("delayed old-epoch acknowledgement bypassed provisional fencing")
    };
    assert_eq!(first_fence.transition_kind, TransitionKind::Failover);
    assert_eq!(first_fence.failover_safe_lsn, None);
    assert_eq!(
        first_fence.primary_write_status,
        AccessStatus::ReconfigurationPending
    );
    model.execute(ProtocolCommand::EnsureConfiguration(first_fence));

    let finalized = loop {
        match model.plan() {
            Plan::Apply { changes } => {
                let finalized = changes.iter().find_map(|change| match change {
                    KubernetesChange::PersistStatus { status } => status
                        .transition
                        .as_ref()
                        .filter(|transition| transition.election_lsn.is_some())
                        .cloned(),
                    _ => None,
                });
                for change in changes {
                    model.apply(change);
                }
                if let Some(finalized) = finalized {
                    break finalized;
                }
            }
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(command),
            } => {
                assert_eq!(command.failover_safe_lsn, None);
                assert_eq!(
                    command.primary_write_status,
                    AccessStatus::ReconfigurationPending
                );
                let local_replica_id = command.local_replica_id;
                model.execute(ProtocolCommand::EnsureConfiguration(command));
                if local_replica_id == ReplicaId::new(2) {
                    model.report_mut(2).verified_replication_lsn = Some(0);
                }
            }
            Plan::Wait { status, .. } => model.apply_wait(status),
            other => panic!("provisional fencing stalled: {other:?}"),
        }
    };
    assert_eq!(
        finalized.current_configuration.primary_id,
        ReplicaId::new(3)
    );
    assert_eq!(finalized.election_lsn, Some(11));
    assert!(finalized.current_configuration.epoch > provisional.current_configuration.epoch);

    for _ in 0..160 {
        match model.plan() {
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(command),
            } => {
                if command.primary_write_status == AccessStatus::Granted {
                    assert!(command.current_only);
                    assert_eq!(command.failover_safe_lsn, Some(11));
                }
                model.execute(ProtocolCommand::EnsureConfiguration(command));
            }
            Plan::Execute { command } => model.execute(command),
            Plan::Wait { status, .. } => model.apply_wait(status),
            Plan::Stable { status, .. } => model.snapshot.status = status,
            Plan::Unsafe { reason, .. } => panic!("fenced failover became unsafe: {reason:?}"),
        }
        let accepted_primary = model
            .snapshot
            .status
            .topology
            .as_ref()
            .and_then(|topology| {
                topology
                    .configuration
                    .members
                    .iter()
                    .find(|member| member.role == ReplicaRole::Primary)
            })
            .map(|member| &member.identity);
        if model.accepted_count() == 5
            && model.snapshot.status.transition.is_none()
            && model.snapshot.routing.write_target.as_ref() == accepted_primary
            && matches!(model.plan(), Plan::Stable { .. })
        {
            break;
        }
    }
    let receipt = model.snapshot.status.last_scale_up.as_ref().unwrap();
    assert_eq!(receipt.failover_safe_lsn, Some(11));
    assert_eq!(receipt.accepted_configuration.primary_id, ReplicaId::new(3));
    let primary = receipt
        .accepted_configuration
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .unwrap();
    assert!(
        model
            .validate_exact_durable_history(&primary.identity, acknowledged)
            .unwrap()
            >= acknowledged
    );
    assert_eq!(
        model.snapshot.routing.write_target.as_ref(),
        Some(&primary.identity)
    );
    model.assert_acknowledged_write_oracle();
}

#[test]
fn carried_failover_keeps_the_provisional_nominee_when_fenced_progress_confirms_it() {
    use scale_up_model::Model;

    let mut model = Model::new(2, 3);
    loop {
        let pc_cc_complete = model
            .snapshot
            .status
            .transition
            .as_ref()
            .and_then(|transition| transition.scale_up.as_deref())
            .is_some_and(|intent| {
                intent.current_configuration.members.iter().all(|member| {
                    model
                        .snapshot
                        .observation_for_identity(&member.identity)
                        .and_then(|observation| match &observation.agent {
                            AgentObservation::Report(report) => Some(report.as_ref()),
                            _ => None,
                        })
                        .is_some_and(|report| {
                            report.previous_configuration.as_ref()
                                == Some(&intent.previous_configuration)
                                && report.current_configuration.as_ref()
                                    == Some(&intent.current_configuration)
                        })
                })
            });
        if pc_cc_complete {
            break;
        }
        model.step();
    }
    model.report_mut(1).reported_fault = Some(FaultType::Permanent);
    model.report_mut(1).write_status = AccessStatus::ReconfigurationPending;
    let Plan::Apply { changes } = model.plan() else {
        panic!("primary failure must persist provisional failover")
    };
    for change in changes {
        model.apply(change);
    }
    let provisional_primary = model
        .snapshot
        .status
        .transition
        .as_ref()
        .unwrap()
        .current_configuration
        .primary_id;
    assert_eq!(provisional_primary, ReplicaId::new(2));
    let acknowledged = model.acknowledge_old_scale_up_authority_write(42, 1, &[1, 2]);

    for _ in 0..80 {
        let plan = model.plan();
        let finalized = matches!(
            &plan,
            Plan::Apply { changes }
                if changes.iter().any(|change| matches!(
                    change,
                    KubernetesChange::PersistStatus { status }
                        if status.transition.as_ref().is_some_and(|transition|
                            transition.election_lsn == Some(acknowledged)
                                && transition.current_configuration.primary_id
                                    == provisional_primary)
                ))
        );
        match plan {
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            Plan::Execute { command } => model.execute(command),
            Plan::Wait { status, .. } => model.apply_wait(status),
            other => panic!("provisional nominee confirmation stalled: {other:?}"),
        }
        if finalized {
            return;
        }
    }
    panic!("fresh fenced progress did not confirm the provisional nominee");
}

#[test]
fn scale_up_carried_failover_corrects_returning_original_pc_cc_and_current_only_primary() {
    use scale_up_model::Model;

    fn carried_failover() -> (Model, ScaleUpIntent) {
        let mut model = Model::new(2, 3);
        loop {
            let primary_pc_cc = model
                .snapshot
                .status
                .transition
                .as_ref()
                .and_then(|transition| transition.scale_up.as_deref())
                .is_some_and(|intent| {
                    model
                        .snapshot
                        .observation_for_identity(&intent.primary)
                        .and_then(|observation| match &observation.agent {
                            AgentObservation::Report(report) => Some(report.as_ref()),
                            _ => None,
                        })
                        .is_some_and(|report| {
                            report.previous_configuration.as_ref()
                                == Some(&intent.previous_configuration)
                                && report.current_configuration.as_ref()
                                    == Some(&intent.current_configuration)
                        })
                });
            if primary_pc_cc {
                break;
            }
            model.step();
        }
        let intent = model
            .snapshot
            .status
            .transition
            .as_ref()
            .and_then(|transition| transition.scale_up.as_deref())
            .unwrap()
            .clone();
        let report = model.report_mut(intent.primary.replica_id.value());
        report.reported_fault = Some(FaultType::Permanent);
        report.write_status = AccessStatus::ReconfigurationPending;
        let Plan::Apply { changes } = model.plan() else {
            panic!("primary failure must freeze carried failover")
        };
        let status = changes.into_iter().find_map(|change| match change {
            KubernetesChange::PersistStatus { status }
                if status
                    .transition
                    .as_ref()
                    .is_some_and(|transition| transition.scale_up_failover.is_some()) =>
            {
                Some(*status)
            }
            _ => None,
        });
        model.snapshot.status = status.expect("carried failover status");
        (model, intent)
    }

    for current_only in [false, true] {
        let (mut model, intent) = carried_failover();
        let report = model.report_mut(intent.primary.replica_id.value());
        report.healthy = true;
        report.reported_fault = None;
        report.role = ReplicaRole::Primary;
        report.read_status = AccessStatus::Granted;
        report.write_status = if current_only {
            AccessStatus::Granted
        } else {
            // Same-primary scale-up deliberately preserves Granted writes while
            // PC/CC is installed; failover must still recognize this exact
            // retained command as the authorized returning original primary.
            AccessStatus::Granted
        };
        report.epoch = intent.current_configuration.epoch;
        report.previous_configuration =
            (!current_only).then(|| intent.previous_configuration.clone());
        report.current_configuration = Some(intent.current_configuration.clone());
        report.scale_up_intent = Some(Box::new(intent.clone()));
        report.pending_operation_id = None;
        report.retained_operation_id = Some(intent.command_operation_id(
            if current_only {
                kuberic_protocol::types::ScaleUpStage::CurrentOnly
            } else {
                kuberic_protocol::types::ScaleUpStage::PreviousCurrent
            },
            &intent.primary,
            &intent.current_configuration,
        ));
        report.report_sequence += 1;

        if current_only {
            let mut unrelated = model.fork();
            unrelated
                .report_mut(intent.primary.replica_id.value())
                .retained_operation_id = Some(OperationId::new("unrelated-current-only"));
            assert!(matches!(
                unrelated.plan(),
                Plan::Unsafe {
                    safety_changes,
                    ..
                } if safety_changes == vec![SafetyChange::RemoveWriteRouting]
            ));
            let mut stale = model.fork();
            stale
                .report_mut(intent.primary.replica_id.value())
                .scale_up_intent = None;
            assert!(matches!(
                stale.plan(),
                Plan::Unsafe {
                    safety_changes,
                    ..
                } if safety_changes == vec![SafetyChange::RemoveWriteRouting]
            ));
        }

        let independent = model
            .snapshot
            .status
            .transition
            .as_ref()
            .unwrap()
            .current_configuration
            .members
            .iter()
            .find(|member| member.identity != intent.primary)
            .unwrap()
            .identity
            .clone();
        let independent_key =
            ReplicaObservationKey::new(independent.replica_id, independent.instance_id.clone());

        let mut invalid_peer = model.fork();
        invalid_peer
            .snapshot
            .replicas
            .get_mut(&independent_key)
            .unwrap()
            .agent = AgentObservation::Invalid {
            message: "independent invalid peer".into(),
            uninitialized_report: None,
        };
        assert!(matches!(
            invalid_peer.plan(),
            Plan::Unsafe {
                safety_changes,
                ..
            } if safety_changes == vec![SafetyChange::RemoveWriteRouting]
        ));

        let mut wrong_generation = model.fork();
        wrong_generation
            .report_mut(independent.replica_id.value())
            .identity
            .agent_generation = AgentGeneration::new("independent-wrong-generation");
        assert!(matches!(
            wrong_generation.plan(),
            Plan::Unsafe {
                safety_changes,
                ..
            } if safety_changes == vec![SafetyChange::RemoveWriteRouting]
        ));

        let mut incompatible_protocol = model.fork();
        incompatible_protocol
            .report_mut(independent.replica_id.value())
            .protocol_version = kuberic_protocol::PROTOCOL_VERSION + 1;
        assert!(matches!(
            incompatible_protocol.plan(),
            Plan::Unsafe {
                reason: UnsafeReason::IncompatibleProtocolVersion {
                    replica_id,
                    ..
                },
                safety_changes,
                ..
            } if replica_id == independent.replica_id.value()
                && safety_changes == vec![SafetyChange::RemoveWriteRouting]
        ));

        let mut observation_failure = model.fork();
        observation_failure
            .snapshot
            .observation_failures
            .push(ObservationFailure {
                source: "independent-agent".into(),
                message: "timeout".into(),
            });
        assert!(matches!(
            observation_failure.plan(),
            Plan::Wait {
                reason: WaitReason::AgentUnavailable,
                ..
            }
        ));

        let Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(command),
        } = model.plan()
        else {
            panic!(
                "surviving carried-failover participant must advance without the returning original {} authority: {:?}",
                if current_only {
                    "current-only"
                } else {
                    "PC/CC"
                },
                model.plan()
            )
        };
        assert_ne!(command.local_replica_id, intent.primary.replica_id);
        assert_eq!(command.transition_kind, TransitionKind::Failover);
        assert!(!command.current_only);
        assert!(
            command.current_epoch > intent.current_configuration.epoch,
            "carried failover correction must move authority monotonically forward"
        );
        model.execute(ProtocolCommand::EnsureConfiguration(command));
        model.run(120);
        assert_eq!(model.accepted_count(), 3);
        assert!(model.snapshot.status.transition.is_none());
        assert!(model.snapshot.status.last_scale_up.is_some());
        let accepted = model
            .snapshot
            .status
            .topology
            .as_ref()
            .unwrap()
            .configuration
            .clone();
        let returned = model.report_mut(intent.primary.replica_id.value());
        assert_eq!(returned.previous_configuration, None);
        assert_eq!(returned.current_configuration.as_ref(), Some(&accepted));
        assert_eq!(
            returned.role,
            accepted
                .members
                .iter()
                .find(|member| member.identity == intent.primary)
                .unwrap()
                .role
        );
    }
}

#[test]
fn sequential_prior_receipt_cannot_reopen_during_newer_carried_failover() {
    use scale_up_model::Model;

    let mut model = Model::new(1, 3);
    loop {
        let second_pc_cc = model.accepted_count() == 2
            && model
                .snapshot
                .status
                .transition
                .as_ref()
                .and_then(|transition| transition.scale_up.as_deref())
                .is_some_and(|intent| {
                    intent.current_configuration.members.iter().all(|member| {
                        model
                            .snapshot
                            .observation_for_identity(&member.identity)
                            .and_then(|observation| match &observation.agent {
                                AgentObservation::Report(report) => Some(report.as_ref()),
                                _ => None,
                            })
                            .is_some_and(|report| {
                                report.previous_configuration.as_ref()
                                    == Some(&intent.previous_configuration)
                                    && report.current_configuration.as_ref()
                                        == Some(&intent.current_configuration)
                                    && report.pending_operation_id.is_none()
                            })
                    })
                });
        if second_pc_cc {
            break;
        }
        model.step();
    }
    let prior_receipt = model.snapshot.status.last_scale_up.clone().unwrap();
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
    assert_ne!(prior_receipt.intent.operation_id, intent.operation_id);
    let old_primary_key = ReplicaObservationKey::new(
        intent.primary.replica_id,
        intent.primary.instance_id.clone(),
    );
    let returning = model
        .snapshot
        .replicas
        .get(&old_primary_key)
        .unwrap()
        .clone();
    model
        .report_mut(intent.primary.replica_id.value())
        .reported_fault = Some(FaultType::Permanent);
    model
        .report_mut(intent.primary.replica_id.value())
        .write_status = AccessStatus::ReconfigurationPending;
    let Plan::Apply { changes } = model.plan() else {
        panic!("newer carried failover was not persisted")
    };
    for change in changes {
        model.apply(change);
    }
    model.snapshot.replicas.insert(old_primary_key, returning);

    for _ in 0..160 {
        let plan = model.plan();
        assert!(
            !matches!(&plan, Plan::Wait { status, .. }
            if status.conditions.iter().any(|condition|
                matches!(
                    condition.reason.as_str(),
                    "ScaleUpPriorReceiptPending"
                        | "ScaleUpHistoricalAcceptancePending"
                        | "ScaleUpCommittedDegraded"
                ))),
            "prior receipt reopened during exact newer failover: {plan:?}"
        );
        match plan {
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            Plan::Execute { command } => model.execute(command),
            Plan::Wait { status, .. } => model.apply_wait(status),
            Plan::Stable { status, .. } => model.snapshot.status = status,
            Plan::Unsafe { reason, .. } => panic!("newer carried failover unsafe: {reason:?}"),
        }
        if model.accepted_count() == 3 && model.snapshot.status.transition.is_none() {
            break;
        }
    }
    assert_eq!(model.accepted_count(), 3);
    assert!(model.snapshot.status.transition.is_none());
    assert_ne!(
        model
            .snapshot
            .status
            .last_scale_up
            .as_ref()
            .unwrap()
            .intent
            .operation_id,
        prior_receipt.intent.operation_id
    );
}

#[test]
fn scale_up_retry_corrects_late_stale_failover_member_before_pc_cc() {
    use scale_up_model::Model;

    let mut seed = Model::new(3, 4);
    let stale_configuration = seed
        .snapshot
        .status
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .clone();
    let members = stale_configuration
        .members
        .iter()
        .map(|member| ConfigurationMember {
            identity: member.identity.clone(),
            role: if member.identity.replica_id == ReplicaId::new(2) {
                ReplicaRole::Primary
            } else {
                ReplicaRole::ActiveSecondary
            },
        })
        .collect::<Vec<_>>();
    let accepted = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        ReplicaId::new(2),
        members,
        stale_configuration.write_quorum,
    );
    seed.snapshot.status.topology = Some(AcceptedTopology {
        configuration: accepted.clone(),
    });
    for observation in seed.snapshot.replicas.values_mut() {
        let AgentObservation::Report(report) = &mut observation.agent else {
            continue;
        };
        let member = accepted
            .members
            .iter()
            .find(|member| member.identity == report.identity)
            .unwrap();
        report.role = member.role;
        report.epoch = accepted.epoch;
        report.previous_configuration = None;
        report.current_configuration = Some(accepted.clone());
        report.write_status = if member.role == ReplicaRole::Primary {
            AccessStatus::Granted
        } else {
            AccessStatus::NotPrimary
        };
    }
    seed.snapshot.routing.write_target = Some(
        accepted
            .members
            .iter()
            .find(|member| member.role == ReplicaRole::Primary)
            .unwrap()
            .identity
            .clone(),
    );
    let mut model = Model::from_snapshot(seed.snapshot, vec![3]);
    for _ in 0..100 {
        if model
            .snapshot
            .status
            .transition
            .as_ref()
            .is_some_and(|transition| transition.scale_up.is_some())
        {
            break;
        }
        model.step();
    }
    assert!(
        model
            .snapshot
            .status
            .transition
            .as_ref()
            .is_some_and(|transition| transition.scale_up.is_some())
    );

    let stale = model.report_mut(1);
    stale.role = ReplicaRole::Primary;
    stale.epoch = stale_configuration.epoch;
    stale.previous_configuration = None;
    stale.current_configuration = Some(stale_configuration.clone());
    stale.write_status = AccessStatus::Granted;
    stale.pending_operation_id = None;
    stale.pending_configuration = None;
    assert_eq!(stale.identity, accepted.members[0].identity);
    assert_eq!(stale.resource_uid.as_str(), "scale-up-model");
    assert_ne!(stale.reported_fault, Some(FaultType::Permanent));
    let correction_operation = OperationId::new(format!(
        "scale-up-accepted-correction:{}:{}",
        accepted.configuration_id,
        ReplicaId::new(1)
    ));

    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(command),
    } = model.plan()
    else {
        panic!(
            "late stale retained member must receive accepted failover correction: {:?}",
            model.plan()
        )
    };
    assert_eq!(command.local_replica_id, ReplicaId::new(1));
    assert_eq!(command.current_configuration, accepted);
    assert_eq!(command.transition_kind, TransitionKind::Failover);
    assert!(!command.current_only);
    assert_eq!(command.operation_id, correction_operation);
    assert_eq!(command.failover_safe_lsn, Some(0));
    let correction = command.clone();
    model.report_mut(1).current_progress += 1;
    assert_eq!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(correction.clone())
        },
        "progress changes must not mutate accepted-member correction bytes"
    );
    {
        let pending = model.report_mut(1);
        pending.pending_operation_id = Some(correction.operation_id.clone());
        pending.pending_configuration = Some(correction.clone());
    }
    assert_eq!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(correction.clone())
        },
        "matching durable pending PC/CC correction must replay byte-for-byte"
    );
    {
        let installed = model.report_mut(1);
        installed.role = ReplicaRole::ActiveSecondary;
        installed.epoch = accepted.epoch;
        installed.previous_configuration = Some(stale_configuration);
        installed.current_configuration = Some(accepted.clone());
        installed.write_status = AccessStatus::NotPrimary;
    }
    assert_eq!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(correction)
        },
        "pending PC/CC correction must remain replayable after authority installation"
    );
    {
        let completed = model.report_mut(1);
        completed.pending_operation_id = None;
        completed.pending_configuration = None;
    }
    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(command),
    } = model.plan()
    else {
        panic!("accepted correction PC/CC must collapse to current-only before scale-up")
    };
    assert_eq!(command.local_replica_id, ReplicaId::new(1));
    assert_eq!(command.current_configuration, accepted);
    assert_eq!(command.transition_kind, TransitionKind::Failover);
    assert!(command.current_only);
    assert_eq!(command.failover_safe_lsn, Some(0));
    let current_only_operation = command.operation_id.clone();
    model.report_mut(1).current_progress += 1;
    assert_eq!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(command.clone())
        },
        "progress changes must not mutate current-only correction bytes"
    );
    {
        let pending = model.report_mut(1);
        pending.pending_operation_id = Some(current_only_operation.clone());
        pending.pending_configuration = Some(command.clone());
        pending.previous_configuration = None;
    }
    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(replayed),
    } = model.plan()
    else {
        panic!("matching frozen accepted current-only correction must replay")
    };
    assert_eq!(replayed, command);
    assert_eq!(replayed.operation_id, current_only_operation);
    {
        let unrelated = model.report_mut(1);
        unrelated.pending_operation_id = Some(OperationId::new("unrelated-accepted-correction"));
        unrelated.pending_configuration = None;
    }
    let unrelated_plan = model.plan();
    assert!(
        matches!(
            &unrelated_plan,
            Plan::Unsafe {
                safety_changes,
                ..
            } if safety_changes == &vec![SafetyChange::RemoveWriteRouting]
        ),
        "unrelated accepted correction was not fenced: {unrelated_plan:?}"
    );
    {
        let pending = model.report_mut(1);
        pending.pending_operation_id = Some(current_only_operation);
        pending.pending_configuration = Some(command.clone());
    }
    assert_eq!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(command)
        },
        "restoring exact pending evidence must restore exact replay"
    );
}

#[test]
fn scale_up_two_late_stale_members_correct_sequentially_with_pending_replay() {
    use scale_up_model::Model;
    let mut seed = Model::new(5, 6);
    let previous = seed
        .snapshot
        .status
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .clone();
    let accepted = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        ReplicaId::new(2),
        previous
            .members
            .iter()
            .map(|member| ConfigurationMember {
                identity: member.identity.clone(),
                role: if member.identity.replica_id == ReplicaId::new(2) {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        previous.write_quorum,
    );
    seed.snapshot.status.topology = Some(AcceptedTopology {
        configuration: accepted.clone(),
    });
    for observation in seed.snapshot.replicas.values_mut() {
        let AgentObservation::Report(report) = &mut observation.agent else {
            continue;
        };
        let member = accepted
            .members
            .iter()
            .find(|member| member.identity == report.identity)
            .unwrap();
        report.role = member.role;
        report.epoch = accepted.epoch;
        report.previous_configuration = None;
        report.current_configuration = Some(accepted.clone());
        report.write_status = if member.role == ReplicaRole::Primary {
            AccessStatus::Granted
        } else {
            AccessStatus::NotPrimary
        };
    }
    seed.snapshot.routing.write_target = Some(
        accepted
            .members
            .iter()
            .find(|member| member.role == ReplicaRole::Primary)
            .unwrap()
            .identity
            .clone(),
    );
    let mut model = Model::from_snapshot(seed.snapshot, vec![5]);
    for _ in 0..120 {
        if model
            .snapshot
            .status
            .transition
            .as_ref()
            .is_some_and(|transition| transition.scale_up.is_some())
        {
            break;
        }
        model.step();
    }
    assert!(model.snapshot.status.transition.is_some());

    for replica_id in [1, 3] {
        let report = model.report_mut(replica_id);
        let previous_member = previous
            .members
            .iter()
            .find(|member| member.identity.replica_id == ReplicaId::new(replica_id))
            .unwrap();
        report.role = previous_member.role;
        report.epoch = previous.epoch;
        report.previous_configuration = None;
        report.current_configuration = Some(previous.clone());
        report.write_status = if previous_member.role == ReplicaRole::Primary {
            AccessStatus::Granted
        } else {
            AccessStatus::NotPrimary
        };
        report.pending_operation_id = None;
        report.pending_configuration = None;
        report.scale_up_intent = None;
    }

    let mut wrong_identity = model.fork();
    wrong_identity.report_mut(3).identity.agent_generation =
        AgentGeneration::new("wrong-generation");
    assert!(matches!(
        wrong_identity.plan(),
        Plan::Unsafe {
            safety_changes,
            ..
        } if safety_changes == vec![SafetyChange::RemoveWriteRouting]
    ));
    let mut wrong_session = model.fork();
    let wrong_session_report = wrong_session.report_mut(3).clone();
    wrong_session.snapshot.previous_report_watermarks.insert(
        ReplicaObservationKey::new(
            wrong_session_report.identity.replica_id,
            wrong_session_report.identity.instance_id.clone(),
        ),
        ReportWatermark {
            process_session_id: wrong_session_report.process_session_id,
            report_sequence: wrong_session_report.report_sequence,
        },
    );
    assert!(matches!(
        wrong_session.plan(),
        Plan::Unsafe {
            safety_changes,
            ..
        } if safety_changes == vec![SafetyChange::RemoveWriteRouting]
    ));
    let mut wrong_configuration = model.fork();
    let mut unrelated_configuration = previous.clone();
    unrelated_configuration.epoch.configuration_number += 10;
    wrong_configuration.report_mut(3).current_configuration = Some(unrelated_configuration);
    assert!(matches!(
        wrong_configuration.plan(),
        Plan::Unsafe {
            safety_changes,
            ..
        } if safety_changes == vec![SafetyChange::RemoveWriteRouting]
    ));
    let mut malformed_peer = model.fork();
    let peer = accepted
        .members
        .iter()
        .find(|member| member.identity.replica_id == ReplicaId::new(4))
        .unwrap()
        .identity
        .clone();
    malformed_peer
        .snapshot
        .replicas
        .get_mut(&ReplicaObservationKey::new(
            peer.replica_id,
            peer.instance_id.clone(),
        ))
        .unwrap()
        .agent = AgentObservation::Invalid {
        message: "independent malformed peer".into(),
        uninitialized_report: None,
    };
    assert!(matches!(
        malformed_peer.plan(),
        Plan::Unsafe {
            safety_changes,
            ..
        } if safety_changes == vec![SafetyChange::RemoveWriteRouting]
    ));

    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(first),
    } = model.plan()
    else {
        panic!("first deterministic stale correction was not dispatched")
    };
    assert_eq!(first.local_replica_id, ReplicaId::new(1));
    {
        let pending = model.report_mut(1);
        pending.pending_operation_id = Some(first.operation_id.clone());
        pending.pending_configuration = Some(first.clone());
    }
    assert_eq!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(first.clone())
        },
        "first pending correction must replay while another valid stale member remains"
    );
    model.execute(ProtocolCommand::EnsureConfiguration(first));
    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(first_current_only),
    } = model.plan()
    else {
        panic!("first stale member must collapse to current-only")
    };
    assert_eq!(first_current_only.local_replica_id, ReplicaId::new(1));
    assert!(first_current_only.current_only);
    model.execute(ProtocolCommand::EnsureConfiguration(first_current_only));

    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(second),
    } = model.plan()
    else {
        panic!("second stale member must be corrected after the first")
    };
    assert_eq!(second.local_replica_id, ReplicaId::new(3));
    assert!(!second.current_only);
    model.execute(ProtocolCommand::EnsureConfiguration(second));
    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(second_current_only),
    } = model.plan()
    else {
        panic!("second stale member must collapse to current-only")
    };
    assert_eq!(second_current_only.local_replica_id, ReplicaId::new(3));
    assert!(second_current_only.current_only);
    model.execute(ProtocolCommand::EnsureConfiguration(second_current_only));
    assert_eq!(
        model
            .snapshot
            .routing
            .write_target
            .as_ref()
            .map(|identity| identity.replica_id),
        Some(accepted.primary_id)
    );
    model.run(200);
    assert_eq!(model.accepted_count(), 6);
    assert!(matches!(model.plan(), Plan::Stable { .. }));
}

#[test]
fn scale_up_stale_correction_preserves_validation_and_primary_failure_priority() {
    use scale_up_model::Model;

    let mut seed = Model::new(5, 6);
    let stale_configuration = seed
        .snapshot
        .status
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .clone();
    let accepted = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        ReplicaId::new(2),
        stale_configuration
            .members
            .iter()
            .map(|member| ConfigurationMember {
                identity: member.identity.clone(),
                role: if member.identity.replica_id == ReplicaId::new(2) {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        stale_configuration.write_quorum,
    );
    seed.snapshot.status.topology = Some(AcceptedTopology {
        configuration: accepted.clone(),
    });
    for observation in seed.snapshot.replicas.values_mut() {
        let AgentObservation::Report(report) = &mut observation.agent else {
            continue;
        };
        let member = accepted
            .members
            .iter()
            .find(|member| member.identity == report.identity)
            .unwrap();
        report.role = member.role;
        report.epoch = accepted.epoch;
        report.previous_configuration = None;
        report.current_configuration = Some(accepted.clone());
        report.write_status = if member.role == ReplicaRole::Primary {
            AccessStatus::Granted
        } else {
            AccessStatus::NotPrimary
        };
        report.pending_operation_id = None;
        report.pending_configuration = None;
    }
    seed.snapshot.routing.write_target = accepted
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .map(|member| member.identity.clone());
    let mut model = Model::from_snapshot(seed.snapshot, vec![3]);
    for _ in 0..100 {
        if model
            .snapshot
            .status
            .transition
            .as_ref()
            .is_some_and(|transition| transition.scale_up.is_some())
        {
            break;
        }
        model.step();
    }
    assert!(
        model
            .snapshot
            .status
            .transition
            .as_ref()
            .is_some_and(|transition| transition.scale_up.is_some())
    );
    {
        let stale = model.report_mut(1);
        stale.role = ReplicaRole::Primary;
        stale.epoch = stale_configuration.epoch;
        stale.previous_configuration = None;
        stale.current_configuration = Some(stale_configuration);
        stale.write_status = AccessStatus::Granted;
        stale.pending_operation_id = None;
        stale.pending_configuration = None;
    }

    let exact = model.plan();
    assert!(matches!(
        exact,
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(command)
        } if command.local_replica_id == ReplicaId::new(1)
            && !command.current_only
            && command.failover_safe_lsn == Some(0)
    ));

    let mut malformed = model.fork();
    malformed.report_mut(3).role = ReplicaRole::Primary;
    malformed.report_mut(3).write_status = AccessStatus::Granted;
    assert!(matches!(
        malformed.plan(),
        Plan::Unsafe {
            safety_changes,
            ..
        } if safety_changes == vec![SafetyChange::RemoveWriteRouting]
    ));

    let mut unauthorized = model.fork();
    unauthorized.report_mut(3).identity.agent_generation =
        AgentGeneration::new("unauthorized-stale-session-generation");
    assert!(matches!(
        unauthorized.plan(),
        Plan::Unsafe {
            safety_changes,
            ..
        } if safety_changes == vec![SafetyChange::RemoveWriteRouting]
    ));

    let mut primary_failed = model.fork();
    {
        let primary = primary_failed.report_mut(2);
        primary.healthy = false;
        primary.reported_fault = Some(FaultType::Permanent);
        primary.write_status = AccessStatus::ReconfigurationPending;
        primary.report_sequence += 1;
    }
    let unavailable_identity = accepted
        .members
        .iter()
        .find(|member| member.identity.replica_id == ReplicaId::new(5))
        .unwrap()
        .identity
        .clone();
    primary_failed
        .snapshot
        .replicas
        .get_mut(&ReplicaObservationKey::new(
            unavailable_identity.replica_id,
            unavailable_identity.instance_id,
        ))
        .unwrap()
        .agent = AgentObservation::Unreachable {
        message: "bounded 3/5 quorum control".into(),
    };
    let priority = primary_failed.plan();
    assert!(
        matches!(
            &priority,
            Plan::Apply { changes }
                if changes.iter().any(|change| matches!(change, KubernetesChange::RemoveWriteRouting))
                    && changes.iter().any(|change| matches!(
                        change,
                        KubernetesChange::PersistStatus { status }
                            if status.primary_failure.as_ref().is_some_and(|failure|
                                failure.primary.replica_id == ReplicaId::new(2))
                    ))
        ),
        "accepted primary failure did not arbitrate first: {priority:?}"
    );

    primary_failed.report_mut(1).healthy = false;
    primary_failed.report_mut(1).reported_fault = Some(FaultType::Permanent);
    let permanent_stale = primary_failed.plan();
    assert!(
        !matches!(
            permanent_stale,
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(command)
            } if command.local_replica_id == ReplicaId::new(1)
        ),
        "permanently failed stale secondary preempted primary failover"
    );
}

#[test]
fn scale_up_commits_degraded_without_candidate_and_blocks_the_next_addition() {
    use scale_up_model::Model;
    let mut model = Model::new(2, 4);
    let mut candidate_current_only = None;
    for _ in 0..100 {
        match model.plan() {
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(command),
            } if command.current_only
                && command.local_replica_id
                    == model
                        .snapshot
                        .status
                        .transition
                        .as_ref()
                        .and_then(|transition| transition.scale_up.as_deref())
                        .unwrap()
                        .target
                        .replica_id =>
            {
                candidate_current_only =
                    Some(ProtocolCommand::EnsureConfiguration(command.clone()));
                model.remove_candidate_report();
            }
            Plan::Execute { command } => model.execute(command),
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            Plan::Wait { status, .. } => model.apply_wait(status),
            Plan::Stable { status, .. } => model.snapshot.status = status,
            Plan::Unsafe { reason, .. } => panic!("unexpected unsafe plan: {reason:?}"),
        }
        if model.accepted_count() == 3 {
            break;
        }
    }
    assert_eq!(model.accepted_count(), 3);
    assert!(model.snapshot.status.scale_up_cleanup.is_none());
    assert!(model.snapshot.status.last_scale_up.is_some());
    assert!(matches!(model.plan(), Plan::Wait { status, .. }
    if status.conditions.iter().any(|condition| {
        condition.reason == "ScaleUpCommittedDegraded"
            && condition.message.contains("phase=local-convergence")
            && condition.message.contains("accepted=3")
            && condition.message.contains("desired=4")
    })));

    let receipt = model.snapshot.status.last_scale_up.clone().unwrap();
    let target = receipt.intent.target.clone();
    let key = ReplicaObservationKey::new(target.replica_id, target.instance_id.clone());
    model.snapshot.replicas.get_mut(&key).unwrap().agent =
        AgentObservation::Report(Box::new(AgentReport {
            protocol_version: kuberic_protocol::PROTOCOL_VERSION,
            resource_uid: model.snapshot.resource_uid.clone(),
            identity: target.clone(),
            process_session_id: ProcessSessionId::new("candidate-returned"),
            report_sequence: 10,
            role: ReplicaRole::ActiveSecondary,
            read_status: AccessStatus::Granted,
            write_status: AccessStatus::NotPrimary,
            healthy: true,
            epoch: receipt.intent.current_configuration.epoch,
            previous_configuration: Some(receipt.intent.previous_configuration.clone()),
            current_configuration: Some(receipt.intent.current_configuration.clone()),
            current_progress: 10,
            verified_replication_lsn: Some(10),
            committed_lsn: 10,
            scale_up_intent: Some(Box::new(receipt.intent.clone())),
            ..Default::default()
        }));
    model.execute(candidate_current_only.expect("candidate correction command"));
    assert!(!matches!(model.plan(), Plan::Wait { status, .. }
        if status.conditions.iter().any(|condition| condition.reason == "ScaleUpCommittedDegraded")));
}

#[test]
fn scale_up_restores_next_ordinal_with_fresh_deterministic_identity() {
    use scale_up_model::Model;
    let mut first = Model::new(2, 3);
    let mut replay = Model::new(2, 3);
    while first.snapshot.status.provisioning.is_none() {
        first.step();
        replay.step();
    }
    let left = first.snapshot.status.provisioning.as_ref().unwrap();
    let right = replay.snapshot.status.provisioning.as_ref().unwrap();
    assert_eq!(left, right);
    assert_eq!(left.replica_id(), ReplicaId::new(3));
    assert_eq!(
        left.target_identity(&first.snapshot.resource_uid)
            .instance_id,
        ReplicaInstanceId::new("candidate-pod-3")
    );
    assert_ne!(
        left.target_identity(&first.snapshot.resource_uid)
            .agent_generation,
        AgentGeneration::new("accepted-generation-3")
    );
}

#[test]
fn scale_up_desired_reduction_after_pc_cc_finishes_frozen_authority() {
    use scale_up_model::Model;
    let mut model = Model::new(2, 3);
    loop {
        let started = model
            .snapshot
            .status
            .transition
            .as_ref()
            .and_then(|transition| transition.scale_up.as_deref())
            .is_some_and(|intent| {
                model.snapshot.replicas.values().any(|observation| {
                    matches!(&observation.agent,
                        AgentObservation::Report(report)
                            if report.previous_configuration.as_ref()
                                == Some(&intent.previous_configuration)
                                && report.current_configuration.as_ref()
                                    == Some(&intent.current_configuration))
                })
            });
        if started {
            break;
        }
        model.step();
    }
    model.snapshot.desired.replicas = 2;
    for _ in 0..40 {
        model.step();
        if model.accepted_count() == 3 {
            break;
        }
    }
    assert_eq!(model.accepted_count(), 3);
    assert!(model.snapshot.status.scale_up_cleanup.is_none());
}

#[test]
fn scale_up_rejects_mutated_source_receiver_boundaries_and_reports_diagnostics() {
    use scale_up_model::Model;
    let mut model = Model::new(1, 2);
    while model.snapshot.status.provisioning.is_none() {
        model.step();
    }
    let condition = model
        .snapshot
        .status
        .conditions
        .iter()
        .find(|condition| condition.reason == "ScaleUpProvisioningAccepted")
        .unwrap();
    for field in ["accepted=1", "desired=2", "phase=provisioning", "attempt="] {
        assert!(
            condition.message.contains(field),
            "{field}: {}",
            condition.message
        );
    }
    loop {
        match model.plan() {
            Plan::Execute {
                command: ProtocolCommand::EnsureReplicaBuild(command),
            } => {
                model.execute(ProtocolCommand::EnsureReplicaBuild(command));
                break;
            }
            Plan::Execute { command } => model.execute(command),
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            other => panic!("unexpected build preparation plan: {other:?}"),
        }
    }
    let Plan::Wait { status, .. } = model.plan() else {
        panic!("build delivery must remain asynchronous")
    };
    model.apply_wait(status);
    let target = model
        .snapshot
        .status
        .provisioning
        .as_ref()
        .unwrap()
        .target_identity(&model.snapshot.resource_uid);
    let AgentObservation::Report(target_report) = &mut model
        .snapshot
        .observation_for_identity(&target)
        .cloned()
        .unwrap()
        .agent
    else {
        unreachable!()
    };
    let mut broken = (**target_report).clone();
    broken.builds[0].replication_boundary_lsn += 1;
    model
        .snapshot
        .replicas
        .get_mut(&ReplicaObservationKey::new(
            target.replica_id,
            target.instance_id,
        ))
        .unwrap()
        .agent = AgentObservation::Report(Box::new(broken));
    assert!(matches!(
        model.plan(),
        Plan::Unsafe {
            reason: UnsafeReason::ContradictoryReplicaEvidence(_),
            ..
        }
    ));
}

#[test]
fn scale_up_late_retained_member_blocks_receipt_supersession() {
    use scale_up_model::Model;
    let mut model = Model::new(1, 2);
    model.run(80);
    model.snapshot.desired.replicas = 3;
    model.snapshot.desired.generation += 1;
    let receipt = model.snapshot.status.last_scale_up.clone().unwrap();
    let retained = receipt.intent.primary.clone();
    let report = model.report_mut(retained.replica_id.value());
    report.previous_configuration = Some(receipt.intent.previous_configuration.clone());
    report.current_configuration = Some(receipt.intent.current_configuration.clone());
    report.epoch = receipt.intent.current_configuration.epoch;
    report.retained_operation_id = Some(receipt.intent.command_operation_id(
        kuberic_protocol::types::ScaleUpStage::PreviousCurrent,
        &retained,
        &receipt.intent.current_configuration,
    ));
    report.pending_operation_id = None;
    report.pending_configuration = None;
    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(current_only),
    } = model.plan()
    else {
        panic!("late retained member must receive current-only receipt correction")
    };
    assert!(current_only.current_only);
    assert_eq!(current_only.local_replica_id, retained.replica_id);
    assert!(
        current_only
            .scale_up_evidence
            .as_ref()
            .is_some_and(|evidence| evidence.intent() == &receipt.intent)
    );
    {
        let pending = model.report_mut(retained.replica_id.value());
        pending.pending_operation_id = Some(current_only.operation_id.clone());
        pending.pending_configuration = Some(current_only.clone());
    }
    assert_eq!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(current_only.clone())
        },
        "exact pending current-only receipt correction must replay before installation"
    );
    {
        let installed = model.report_mut(retained.replica_id.value());
        installed.previous_configuration = None;
        installed.current_configuration = Some(receipt.accepted_configuration.clone());
        installed.epoch = receipt.accepted_configuration.epoch;
        installed.scale_up_intent = Some(Box::new(receipt.intent.clone()));
    }
    assert_eq!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(current_only.clone())
        },
        "installed authority cannot settle a still-pending receipt correction"
    );
    let mut mutated = model.fork();
    let mutated_report = mutated.report_mut(retained.replica_id.value());
    let mut changed = current_only.clone();
    changed.primary_write_status = AccessStatus::ReconfigurationPending;
    mutated_report.pending_configuration = Some(changed);
    assert!(matches!(
        mutated.plan(),
        Plan::Unsafe {
            safety_changes,
            ..
        } if safety_changes == vec![SafetyChange::RemoveWriteRouting]
    ));
    let mut unrelated = model.fork();
    let unrelated_report = unrelated.report_mut(retained.replica_id.value());
    unrelated_report.pending_operation_id = Some(OperationId::new("unrelated-receipt-command"));
    unrelated_report.pending_configuration = None;
    assert!(matches!(
        unrelated.plan(),
        Plan::Wait { status, .. }
            if status.conditions.iter().any(|condition|
                condition.reason == "ScaleUpHistoricalAcceptancePending")
    ));
    assert!(!matches!(model.plan(), Plan::Stable { .. }));
    {
        let completed = model.report_mut(retained.replica_id.value());
        completed.pending_operation_id = None;
        completed.pending_configuration = None;
        completed.retained_operation_id = Some(current_only.operation_id.clone());
    }
    assert!(model.snapshot.status.provisioning.is_none());
}

#[test]
fn scale_up_receipt_replays_pending_pc_cc_before_advancing_to_current_only() {
    use scale_up_model::Model;
    let mut model = Model::new(2, 3);
    model.run(120);
    let receipt = model.snapshot.status.last_scale_up.clone().unwrap();
    let retained = receipt.intent.primary.clone();
    let previous_member = receipt
        .intent
        .previous_configuration
        .members
        .iter()
        .find(|member| member.identity == retained)
        .unwrap()
        .clone();
    let report = model.report_mut(retained.replica_id.value());
    report.role = previous_member.role;
    report.epoch = receipt.intent.previous_configuration.epoch;
    report.previous_configuration = None;
    report.current_configuration = Some(receipt.intent.previous_configuration.clone());
    report.scale_up_intent = None;
    report.pending_operation_id = None;
    report.pending_configuration = None;
    report.retained_operation_id = None;

    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(pc_cc),
    } = model.plan()
    else {
        panic!(
            "late retained member must replay receipt PC/CC first: {:?}",
            model.plan()
        )
    };
    assert!(!pc_cc.current_only);
    {
        let pending = model.report_mut(retained.replica_id.value());
        pending.pending_operation_id = Some(pc_cc.operation_id.clone());
        pending.pending_configuration = Some(pc_cc.clone());
    }
    assert_eq!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(pc_cc.clone())
        },
        "exact pending receipt PC/CC must replay before authority installation"
    );
    {
        let installed = model.report_mut(retained.replica_id.value());
        installed.role = receipt
            .accepted_configuration
            .members
            .iter()
            .find(|member| member.identity == retained)
            .unwrap()
            .role;
        installed.epoch = receipt.accepted_configuration.epoch;
        installed.previous_configuration = Some(receipt.intent.previous_configuration.clone());
        installed.current_configuration = Some(receipt.accepted_configuration.clone());
        installed.scale_up_intent = Some(Box::new(receipt.intent.clone()));
    }
    assert_eq!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(pc_cc.clone())
        },
        "installed PC/CC cannot advance while the exact local command remains pending"
    );
    {
        let completed = model.report_mut(retained.replica_id.value());
        completed.pending_operation_id = None;
        completed.pending_configuration = None;
        completed.retained_operation_id = Some(pc_cc.operation_id);
    }
    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(current_only),
    } = model.plan()
    else {
        panic!("completed receipt PC/CC must advance to current-only")
    };
    assert!(current_only.current_only);
    assert_eq!(current_only.local_replica_id, retained.replica_id);
}

#[test]
fn previous_receipt_yields_to_exact_newer_build_retirement_then_resumes_correction() {
    use scale_up_model::Model;

    for permanent_candidate_failure in [false, true] {
        let mut model = Model::new(1, 2);
        model.run(80);
        let receipt = model.snapshot.status.last_scale_up.clone().unwrap();
        model.snapshot.desired.replicas = 3;
        model.snapshot.desired.generation += 1;

        let build = loop {
            match model.plan() {
                Plan::Execute {
                    command: ProtocolCommand::EnsureReplicaBuild(command),
                } if !command.retire => {
                    let command = *command;
                    model.execute(ProtocolCommand::EnsureReplicaBuild(Box::new(
                        command.clone(),
                    )));
                    break command;
                }
                Plan::Apply { changes } => {
                    for change in changes {
                        model.apply(change);
                    }
                }
                Plan::Execute { command } => model.execute(command),
                Plan::Wait { status, .. } => model.apply_wait(status),
                other => panic!("newer build setup stalled: {other:?}"),
            }
        };
        assert!(model.has_pending_build());

        let historical = receipt.intent.target.clone();
        let historical_report = model.report_mut(historical.replica_id.value());
        historical_report.previous_configuration =
            Some(receipt.intent.previous_configuration.clone());
        historical_report.current_configuration = Some(receipt.accepted_configuration.clone());
        historical_report.epoch = receipt.accepted_configuration.epoch;
        historical_report.scale_up_intent = Some(Box::new(receipt.intent.clone()));
        historical_report.retained_operation_id = Some(receipt.intent.command_operation_id(
            kuberic_protocol::types::ScaleUpStage::PreviousCurrent,
            &historical,
            &receipt.accepted_configuration,
        ));
        historical_report.pending_operation_id = None;
        historical_report.pending_configuration = None;

        if permanent_candidate_failure {
            let candidate = build.target.clone();
            let candidate_report = model.report_mut(candidate.replica_id.value());
            candidate_report.healthy = false;
            candidate_report.reported_fault = Some(FaultType::Permanent);
        } else {
            model.snapshot.desired.replicas = 2;
            model.snapshot.desired.generation += 1;
        }

        let Plan::Apply { changes } = model.plan() else {
            panic!("newer exact attempt cancellation was preempted by the prior receipt")
        };
        assert!(changes.iter().any(|change| matches!(
            change,
            KubernetesChange::PersistStatus { status }
                if status.scale_up_cleanup.as_ref().is_some_and(|cleanup|
                    cleanup.provisioning.operation_id
                        != receipt.intent.operation_id
                        && cleanup.target == build.target)
                    && status.last_scale_up.as_ref() == Some(&receipt)
        )));
        for change in changes {
            model.apply(change);
        }
        let abandoned_attempt = model
            .snapshot
            .status
            .scale_up_cleanup
            .as_ref()
            .unwrap()
            .provisioning
            .operation_id
            .clone();
        model = model.fork();

        let source = model
            .snapshot
            .status
            .scale_up_cleanup
            .as_ref()
            .unwrap()
            .provisioning
            .scale_up()
            .unwrap()
            .previous_configuration
            .members
            .iter()
            .find(|member| member.role == ReplicaRole::Primary)
            .unwrap()
            .identity
            .clone();
        let mut unrelated = model.fork();
        unrelated
            .report_mut(source.replica_id.value())
            .pending_operation_id = Some(OperationId::new("unrelated-newer-attempt-work"));
        assert!(matches!(
            unrelated.plan(),
            Plan::Wait { status, .. }
                if status.conditions.iter().any(|condition|
                    matches!(
                        condition.reason.as_str(),
                        "ScaleUpSourceBuildRetirementBlocked"
                            | "ScaleUpHistoricalAcceptancePending"
                    ))
        ));

        let Plan::Execute {
            command: ProtocolCommand::EnsureReplicaBuild(retirement),
        } = model.plan()
        else {
            panic!("exact newer build retirement must precede cleanup")
        };
        assert!(retirement.retire);
        assert_eq!(retirement.operation_id, build.operation_id);
        assert_eq!(retirement.target, build.target);
        model.execute(ProtocolCommand::EnsureReplicaBuild(retirement));
        model = model.fork();

        let Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(correction),
        } = model.plan()
        else {
            panic!("genuine prior-receipt correction was not preserved after exact retirement")
        };
        assert!(correction.current_only);
        assert_eq!(correction.local_replica_id, historical.replica_id);
        assert!(
            correction
                .scale_up_evidence
                .as_ref()
                .is_some_and(|evidence| evidence.intent() == &receipt.intent)
        );
        model.execute(ProtocolCommand::EnsureConfiguration(correction));
        model = model.fork();

        let mut deletes = Vec::new();
        for _ in 0..12 {
            match model.plan() {
                Plan::Apply { changes } => {
                    for change in changes {
                        if let KubernetesChange::DeleteScaleDownResource { resource, .. } = &change
                        {
                            deletes.push(*resource);
                        }
                        model.apply(change);
                    }
                    model = model.fork();
                }
                Plan::Wait { status, .. } => model.apply_wait(status),
                other => panic!("newer exact cleanup stalled: {other:?}"),
            }
            if model.snapshot.status.scale_up_cleanup.is_none() {
                break;
            }
        }
        assert_eq!(
            deletes,
            vec![
                ScaleDownResource::Endpoint,
                ScaleDownResource::Pod,
                ScaleDownResource::Pvc,
            ]
        );
        assert_eq!(model.snapshot.status.last_scale_up.as_ref(), Some(&receipt));

        if permanent_candidate_failure {
            for _ in 0..20 {
                let fresh = model
                    .snapshot
                    .status
                    .scale_up_allocation
                    .as_ref()
                    .map(|allocation| &allocation.operation_id)
                    .or_else(|| {
                        model
                            .snapshot
                            .status
                            .provisioning
                            .as_ref()
                            .map(|provisioning| &provisioning.operation_id)
                    });
                if fresh.is_some_and(|operation_id| operation_id != &abandoned_attempt) {
                    break;
                }
                model.step();
            }
            assert!(
                model.snapshot.status.scale_up_allocation.is_some()
                    || model.snapshot.status.provisioning.is_some()
            );
            let fresh = model
                .snapshot
                .status
                .scale_up_allocation
                .as_ref()
                .map(|allocation| &allocation.operation_id)
                .or_else(|| {
                    model
                        .snapshot
                        .status
                        .provisioning
                        .as_ref()
                        .map(|provisioning| &provisioning.operation_id)
                })
                .unwrap();
            assert_ne!(fresh, &abandoned_attempt);
        } else {
            assert!(matches!(model.plan(), Plan::Stable { .. }));
        }
    }
}

#[test]
fn scale_up_failover_receipt_replays_immutable_election_lsn_after_later_progress() {
    use scale_up_model::Model;

    let mut model = Model::new(4, 5);
    let intent = loop {
        match model.plan() {
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(command),
            } if command.current_only => {
                break command
                    .scale_up_evidence
                    .as_deref()
                    .expect("scale-up evidence")
                    .intent()
                    .clone();
            }
            Plan::Execute { command } => model.execute(command),
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            Plan::Wait { status, .. } => model.apply_wait(status),
            other => panic!("4->5 failover setup stalled: {other:?}"),
        }
    };
    for replica_id in 2..=5 {
        let report = model.report_mut(replica_id);
        report.current_progress = 12;
        report.verified_replication_lsn = Some(12);
    }
    let failed_primary = intent.primary.replica_id.value();
    model.report_mut(failed_primary).reported_fault = Some(FaultType::Permanent);
    model.report_mut(failed_primary).write_status = AccessStatus::ReconfigurationPending;
    let failure_plan = model.plan();
    let Plan::Apply { changes } = failure_plan else {
        panic!("post-admission primary failure must persist carried failover: {failure_plan:?}")
    };
    let status = changes.into_iter().find_map(|change| match change {
        KubernetesChange::PersistStatus { status }
            if status
                .transition
                .as_ref()
                .is_some_and(|transition| transition.scale_up_failover.is_some()) =>
        {
            Some(*status)
        }
        _ => None,
    });
    model.snapshot.status = status.expect("carried failover status");
    for _ in 0..240 {
        if model.snapshot.status.transition.is_none()
            && model.snapshot.status.last_scale_up.is_some()
        {
            break;
        }
        match model.plan() {
            Plan::Execute { command } => model.execute(command),
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            Plan::Wait { status, .. } | Plan::Stable { status, .. } => {
                model.snapshot.status = status;
            }
            Plan::Unsafe { reason, .. } => panic!("carried failover became unsafe: {reason:?}"),
        }
    }
    assert_eq!(model.accepted_count(), 5);
    assert!(model.snapshot.status.transition.is_none());

    let mut receipt = model
        .snapshot
        .status
        .last_scale_up
        .clone()
        .expect("failover receipt");
    let accepted_primary_id = receipt.accepted_configuration.primary_id;
    let election = receipt
        .failover_evidence
        .as_ref()
        .and_then(|evidence| {
            evidence
                .current_read_quorum
                .iter()
                .find(|witness| witness.identity.replica_id == accepted_primary_id)
        })
        .expect("frozen elected-primary witness");
    assert_eq!(election.verified_replication_lsn, 12);
    receipt
        .current_only_write_quorum
        .iter_mut()
        .find(|witness| witness.identity.replica_id == accepted_primary_id)
        .expect("completion primary witness")
        .verified_replication_lsn = 13;
    model.snapshot.status.last_scale_up = Some(receipt.clone());

    let returning = intent.primary.clone();
    let previous_member = intent
        .previous_configuration
        .members
        .iter()
        .find(|member| member.identity == returning)
        .unwrap()
        .clone();
    {
        let report = model.report_mut(returning.replica_id.value());
        report.healthy = true;
        report.reported_fault = None;
        report.role = previous_member.role;
        report.epoch = intent.previous_configuration.epoch;
        report.previous_configuration = None;
        report.current_configuration = Some(intent.previous_configuration.clone());
        report.scale_up_intent = None;
        report.pending_operation_id = None;
        report.pending_configuration = None;
        report.retained_operation_id = None;
        report.report_sequence += 1;
    }
    let accepted_primary = receipt
        .accepted_configuration
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .unwrap()
        .identity
        .clone();
    {
        let primary = model.report_mut(accepted_primary.replica_id.value());
        primary.reported_fault = None;
        primary.healthy = true;
        primary.write_status = AccessStatus::Granted;
    }
    model.snapshot.routing.service_present = true;
    model.snapshot.routing.unresolved_write_target = false;
    model.snapshot.routing.write_target = Some(accepted_primary);
    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(pc_cc),
    } = model.plan()
    else {
        panic!(
            "returning carried-failover member must receive PC/CC correction: {:?}",
            model.plan()
        )
    };
    assert!(!pc_cc.current_only);
    assert_eq!(pc_cc.failover_safe_lsn, Some(12));
    {
        let pending = model.report_mut(returning.replica_id.value());
        pending.pending_operation_id = Some(pc_cc.operation_id.clone());
        pending.pending_configuration = Some(pc_cc.clone());
    }
    assert_eq!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(pc_cc.clone())
        }
    );
    {
        let installed = model.report_mut(returning.replica_id.value());
        let accepted_member = receipt
            .accepted_configuration
            .members
            .iter()
            .find(|member| member.identity == returning)
            .unwrap();
        installed.role = accepted_member.role;
        installed.epoch = receipt.accepted_configuration.epoch;
        installed.previous_configuration = Some(intent.previous_configuration.clone());
        installed.current_configuration = Some(receipt.accepted_configuration.clone());
        installed.scale_up_intent = Some(Box::new(intent.clone()));
        installed.current_progress = 13;
        installed.verified_replication_lsn = Some(13);
    }
    assert_eq!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(pc_cc.clone())
        },
        "later installed progress must not mutate a frozen pending PC/CC payload"
    );
    {
        let completed = model.report_mut(returning.replica_id.value());
        completed.pending_operation_id = None;
        completed.pending_configuration = None;
        completed.retained_operation_id = Some(pc_cc.operation_id);
    }
    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(current_only),
    } = model.plan()
    else {
        panic!("completed PC/CC must advance to current-only correction")
    };
    assert!(current_only.current_only);
    assert_eq!(current_only.failover_safe_lsn, Some(12));
    {
        let pending = model.report_mut(returning.replica_id.value());
        pending.previous_configuration = None;
        pending.pending_operation_id = Some(current_only.operation_id.clone());
        pending.pending_configuration = Some(current_only.clone());
    }
    assert_eq!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(current_only.clone())
        },
        "later progress must preserve exact current-only bytes and ID"
    );
    let mut mutated = model.fork();
    mutated
        .report_mut(returning.replica_id.value())
        .pending_configuration
        .as_mut()
        .unwrap()
        .failover_safe_lsn = Some(13);
    assert!(matches!(
        mutated.plan(),
        Plan::Unsafe {
            safety_changes,
            ..
        } if safety_changes == vec![SafetyChange::RemoveWriteRouting]
    ));
    assert!(!matches!(model.plan(), Plan::Stable { .. }));
    {
        let completed = model.report_mut(returning.replica_id.value());
        completed.pending_operation_id = None;
        completed.pending_configuration = None;
        completed.retained_operation_id = Some(current_only.operation_id);
    }
    assert!(matches!(model.plan(), Plan::Stable { .. }));
}

#[test]
fn scale_up_failover_accepts_mixed_pc_cc_and_current_only_recovery_witnesses() {
    use scale_up_model::Model;
    for accepted in [2, 3] {
        let mut model = Model::new(accepted, accepted + 1);
        let mut boundaries = 0;
        for _ in 0..120 {
            match model.plan() {
                Plan::Execute {
                    command: ProtocolCommand::EnsureConfiguration(command),
                } if command.current_only => {
                    model.execute(ProtocolCommand::EnsureConfiguration(command));
                    boundaries += 1;
                    let mut failed = model.fork();
                    let primary_id = failed
                        .snapshot
                        .status
                        .transition
                        .as_ref()
                        .unwrap()
                        .scale_up
                        .as_deref()
                        .unwrap()
                        .primary
                        .replica_id;
                    failed.report_mut(primary_id.value()).reported_fault =
                        Some(FaultType::Permanent);
                    failed.report_mut(primary_id.value()).write_status =
                        AccessStatus::ReconfigurationPending;
                    let Plan::Apply { changes } = failed.plan() else {
                        panic!(
                            "accepted={accepted} boundary={boundaries} stalled: {:?}",
                            failed.plan()
                        );
                    };
                    let status = changes.into_iter().find_map(|change| match change {
                        KubernetesChange::PersistStatus { status } => Some(*status),
                        _ => None,
                    });
                    let transition = status
                        .as_ref()
                        .and_then(|status| status.transition.as_ref())
                        .expect("carried failover transition");
                    assert!(transition.scale_up_failover.is_some());
                }
                Plan::Execute { command } => model.execute(command),
                Plan::Apply { changes } => {
                    for change in changes {
                        model.apply(change);
                    }
                }
                Plan::Wait { status, .. } => model.apply_wait(status),
                Plan::Stable { status, .. } => {
                    model.snapshot.status = status;
                    break;
                }
                Plan::Unsafe { reason, .. } => panic!("unexpected unsafe plan: {reason:?}"),
            }
            if model.accepted_count() == accepted + 1 {
                break;
            }
        }
        assert_eq!(boundaries, accepted + 1);
    }
}

#[test]
fn scale_up_primary_failover_precedes_unsettled_receipt_recovery() {
    use scale_up_model::Model;
    let mut model = Model::new(3, 4);
    for _ in 0..120 {
        match model.plan() {
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(command),
            } if command.current_only && command.local_replica_id == ReplicaId::new(4) => {
                model.remove_candidate_report();
            }
            Plan::Execute { command } => model.execute(command),
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            Plan::Wait { status, .. } => model.apply_wait(status),
            Plan::Stable { status, .. } => model.snapshot.status = status,
            Plan::Unsafe { reason, .. } => panic!("unexpected unsafe plan: {reason:?}"),
        }
        if model.accepted_count() == 4 {
            break;
        }
    }
    assert_eq!(model.accepted_count(), 4);
    assert!(model.snapshot.status.last_scale_up.is_some());
    model.report_mut(1).reported_fault = Some(FaultType::Permanent);
    model.report_mut(1).write_status = AccessStatus::ReconfigurationPending;
    let Plan::Apply { changes } = model.plan() else {
        panic!("primary failure must outrank receipt recovery");
    };
    for change in changes {
        model.apply(change);
    }
    assert!(model.snapshot.status.last_scale_up.is_some());
    model.snapshot.now_unix_seconds += 10;
    let Plan::Apply { changes } = model.plan() else {
        panic!("sufficient surviving read quorum must allocate failover");
    };
    for change in changes {
        model.apply(change);
    }
    assert!(model.snapshot.status.last_scale_up.is_some());
    assert!(
        model
            .snapshot
            .status
            .transition
            .as_ref()
            .is_some_and(|transition| transition.kind == TransitionKind::Failover)
    );
    let mut failover_command = None;
    for _ in 0..4 {
        match model.plan() {
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(command),
            } if command.transition_kind == TransitionKind::Failover => {
                failover_command = Some(command);
                break;
            }
            other => panic!("receipt recovery preempted failover: {other:?}"),
        }
    }
    assert!(failover_command.is_some());
    assert!(model.snapshot.status.last_scale_up.is_some());
}

#[test]
fn scale_up_reports_explicit_quorum_loss_during_pc_cc() {
    use scale_up_model::Model;
    let mut model = Model::new(3, 4);
    loop {
        let pc_cc_complete = model
            .snapshot
            .status
            .transition
            .as_ref()
            .and_then(|transition| transition.scale_up.as_deref())
            .is_some_and(|intent| {
                intent.current_configuration.members.iter().all(|member| {
                    model
                        .snapshot
                        .observation_for_identity(&member.identity)
                        .and_then(|observation| match &observation.agent {
                            AgentObservation::Report(report) => Some(report),
                            _ => None,
                        })
                        .is_some_and(|report| {
                            report.previous_configuration.as_ref()
                                == Some(&intent.previous_configuration)
                                && report.current_configuration.as_ref()
                                    == Some(&intent.current_configuration)
                        })
                })
            });
        if pc_cc_complete {
            break;
        }
        model.step();
    }
    for replica_id in [2, 3] {
        let report = model.report_mut(replica_id);
        report.healthy = false;
        report.reported_fault = Some(FaultType::Transient);
    }
    assert!(matches!(
        model.plan(),
        Plan::Wait {
            reason: WaitReason::QuorumLoss,
            status,
            ..
        } if status.conditions.iter().any(|condition| {
            condition.reason == "ScaleUpDualQuorumUnavailable"
                && condition.message.contains("phase=pc-cc")
                && condition.message.contains("accepted=3")
                && condition.message.contains("desired=4")
        })
    ));
}

#[test]
fn scale_up_diagnostics_cover_every_externally_visible_phase() {
    use scale_up_model::Model;

    fn assert_phase(status: &AcceptedStatus, reason: &str, phase: &str) {
        let condition = status
            .conditions
            .iter()
            .find(|condition| condition.reason == reason)
            .unwrap_or_else(|| panic!("missing {reason}: {:?}", status.conditions));
        for field in ["accepted=", "desired=", "target=", "attempt=", "blocking="] {
            assert!(condition.message.contains(field), "{reason}: {field}");
        }
        assert!(
            condition.message.contains(&format!("phase={phase}")),
            "{reason}: {}",
            condition.message
        );
    }

    let mut model = Model::new(2, 3);
    while model.snapshot.status.provisioning.is_none() {
        model.step();
    }
    assert_phase(
        &model.snapshot.status,
        "ScaleUpProvisioningAccepted",
        "provisioning",
    );

    let ProtocolCommand::InitializeAgentStore(command) = (match model.plan() {
        Plan::Execute { command } => command,
        other => panic!("initialization phase: {other:?}"),
    }) else {
        panic!("initialization command")
    };
    model.execute(ProtocolCommand::InitializeAgentStore(command));
    let primary = model
        .snapshot
        .status
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .members[0]
        .identity
        .clone();
    model.snapshot.observation_for_identity(&primary).unwrap();
    model
        .snapshot
        .replicas
        .get_mut(&ReplicaObservationKey::new(
            primary.replica_id,
            primary.instance_id.clone(),
        ))
        .unwrap()
        .kubernetes
        .as_mut()
        .unwrap()
        .peer_endpoint_ready = false;
    let Plan::Wait { status, .. } = model.plan() else {
        panic!("copying diagnostic")
    };
    assert_phase(&status, "ScaleUpSourceUnavailable", "copying");
    model
        .snapshot
        .replicas
        .get_mut(&ReplicaObservationKey::new(
            primary.replica_id,
            primary.instance_id,
        ))
        .unwrap()
        .kubernetes
        .as_mut()
        .unwrap()
        .peer_endpoint_ready = true;

    let Plan::Apply { changes } = model.plan() else {
        panic!("copying phase publication")
    };
    let copying = changes.iter().find_map(|change| match change {
        KubernetesChange::PersistStatus { status } => Some(status.as_ref()),
        _ => None,
    });
    assert_phase(copying.unwrap(), "ScaleUpCopying", "copying");
    for change in changes {
        model.apply(change);
    }
    let ProtocolCommand::EnsureReplicaBuild(command) = (match model.plan() {
        Plan::Execute { command } => command,
        other => panic!("build phase: {other:?}"),
    }) else {
        panic!("build command")
    };
    model.execute(ProtocolCommand::EnsureReplicaBuild(command));
    let Plan::Wait { status, .. } = model.plan() else {
        panic!("boundary propagation diagnostic")
    };
    assert_phase(&status, "ScaleUpBoundaryPropagationPending", "catch-up");
    model.apply_wait(status);
    let Plan::Wait { status, .. } = model.plan() else {
        panic!("catch-up boundary diagnostic")
    };
    assert_phase(&status, "ScaleUpBoundaryPending", "catch-up");
    model.apply_wait(status);
    let Plan::Wait { status, .. } = model.plan() else {
        panic!("receiver boundary propagation diagnostic")
    };
    assert_phase(&status, "ScaleUpBoundaryPropagationPending", "catch-up");
    model.apply_wait(status);

    let Plan::Apply { changes } = model.plan() else {
        panic!("PC/CC persistence")
    };
    for change in changes {
        model.apply(change);
    }
    let Plan::Apply { changes } = model.plan() else {
        panic!("admission-start fence persistence")
    };
    let admission = changes.iter().find_map(|change| match change {
        KubernetesChange::PersistStatus { status } => Some(status.as_ref()),
        _ => None,
    });
    assert_phase(admission.unwrap(), "ScaleUpAdmissionStarted", "pc-cc");
    for change in changes {
        model.apply(change);
    }
    let ProtocolCommand::EnsureConfiguration(command) = (match model.plan() {
        Plan::Execute { command } => command,
        other => panic!("PC/CC command: {other:?}"),
    }) else {
        panic!("configuration command")
    };
    let target = ReplicaIdentity {
        replica_id: command.local_replica_id,
        instance_id: command.expected_instance_id.clone(),
        agent_generation: command.expected_agent_generation.clone(),
    };
    let AgentObservation::Report(report) = &mut model
        .snapshot
        .replicas
        .get_mut(&ReplicaObservationKey::new(
            target.replica_id,
            target.instance_id.clone(),
        ))
        .unwrap()
        .agent
    else {
        panic!("PC/CC target report")
    };
    let pending_operation = command.operation_id.clone();
    report.pending_operation_id = Some(pending_operation.clone());
    report.pending_configuration = Some(command.clone());
    report.report_sequence += 1;
    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(replayed),
    } = model.plan()
    else {
        panic!("PC/CC matching pending command must be replayed")
    };
    assert_eq!(replayed.operation_id, pending_operation);

    let mut current_only = Model::new(2, 3);
    loop {
        match current_only.plan() {
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(command),
            } if command.current_only => {
                current_only.execute(ProtocolCommand::EnsureConfiguration(command));
                break;
            }
            Plan::Execute { command } => current_only.execute(command),
            Plan::Apply { changes } => {
                for change in changes {
                    current_only.apply(change);
                }
            }
            Plan::Wait { status, .. } => current_only.apply_wait(status),
            other => panic!("current-only setup: {other:?}"),
        }
    }
    let completed_id = current_only
        .snapshot
        .replicas
        .values()
        .filter_map(|observation| match &observation.agent {
            AgentObservation::Report(report) if report.previous_configuration.is_none() => {
                Some(report.identity.replica_id)
            }
            _ => None,
        })
        .max()
        .unwrap();
    for observation in current_only.snapshot.replicas.values_mut() {
        if let AgentObservation::Report(report) = &mut observation.agent
            && report.identity.replica_id != completed_id
        {
            report.pending_operation_id = Some(OperationId::new("held-current-only"));
        }
    }
    let Plan::Wait { status, .. } = current_only.plan() else {
        panic!("current-only diagnostic: {:?}", current_only.plan())
    };
    assert_phase(&status, "ScaleUpCurrentOnlyQuorumPending", "current-only");

    let mut stable = Model::new(1, 2);
    stable.run(80);
    let Plan::Stable { status, .. } = stable.plan() else {
        panic!("stable phase")
    };
    assert_phase(&status, "ScaleUpStable", "stable");
}

#[test]
fn scale_up_replays_exact_pending_current_only_before_and_after_authority_install() {
    use scale_up_model::Model;

    let mut model = Model::new(2, 3);
    let current_only = loop {
        match model.plan() {
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(command),
            } if command.current_only => break command,
            Plan::Execute { command } => model.execute(command),
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            Plan::Wait { status, .. } => model.apply_wait(status),
            other => panic!("current-only setup: {other:?}"),
        }
    };
    let target = current_only.local_replica_id.value();
    model.report_mut(target).pending_operation_id = Some(current_only.operation_id.clone());
    model.report_mut(target).pending_configuration = Some(current_only.clone());
    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(replayed_before_install),
    } = model.plan()
    else {
        panic!(
            "exact pending current-only command before authority installation must be replayed: {:?}",
            model.plan()
        )
    };
    assert_eq!(replayed_before_install, current_only);

    let mut unrelated = model.fork();
    unrelated.report_mut(target).pending_operation_id =
        Some(OperationId::new("unrelated-current-only"));
    unrelated.report_mut(target).pending_configuration = None;
    assert!(matches!(
        unrelated.plan(),
        Plan::Wait { status, .. }
            if status.conditions.iter().any(|condition|
                condition.reason == "ScaleUpCommandPending")
    ));

    model.execute(ProtocolCommand::EnsureConfiguration(current_only.clone()));
    model.report_mut(target).pending_operation_id = Some(current_only.operation_id.clone());
    model.report_mut(target).pending_configuration = Some(current_only.clone());
    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(replayed_after_install),
    } = model.plan()
    else {
        panic!(
            "exact pending current-only command after authority installation must be replayed: {:?}",
            model.plan()
        )
    };
    assert_eq!(replayed_after_install, current_only);
    model.execute(ProtocolCommand::EnsureConfiguration(current_only));
    model.run(100);
    assert_eq!(model.accepted_count(), 3);
    assert!(model.snapshot.status.last_scale_up.is_some());
}

#[test]
fn scale_up_fresh_admission_yields_to_write_and_routing_restoration() {
    use scale_up_model::Model;
    let mut model = Model::new(2, 3);
    let configuration = model
        .snapshot
        .status
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .clone();
    model.snapshot.status.quorum_loss = Some(QuorumLossObservation {
        configuration_id: configuration.configuration_id.clone(),
    });
    model.report_mut(1).write_status = AccessStatus::NoWriteQuorum;
    assert!(matches!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(command)
        } if command.primary_write_status == AccessStatus::Granted
            && command.scale_up_evidence.is_none()
    ));
    model.report_mut(1).write_status = AccessStatus::Granted;
    let Plan::Apply { changes } = model.plan() else {
        panic!("quorum-loss marker must clear before scale-up")
    };
    for change in changes {
        model.apply(change);
    }
    model.snapshot.routing.write_target = None;
    assert!(matches!(
        model.plan(),
        Plan::Apply { changes }
            if matches!(
                changes.as_slice(),
                [
                    KubernetesChange::PublishWriteRouting { .. },
                    KubernetesChange::PersistStatus { .. }
                ]
            )
    ));
    assert!(model.snapshot.status.provisioning.is_none());
}

#[test]
fn scale_up_waits_for_one_sided_boundary_propagation_without_unsafe() {
    use scale_up_model::Model;
    let mut model = Model::new(1, 2);
    loop {
        match model.plan() {
            Plan::Execute {
                command: ProtocolCommand::EnsureReplicaBuild(command),
            } => {
                model.execute(ProtocolCommand::EnsureReplicaBuild(command));
                break;
            }
            Plan::Execute { command } => model.execute(command),
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            other => panic!("build setup: {other:?}"),
        }
    }
    assert!(matches!(
        model.plan(),
        Plan::Wait { status, .. }
            if status.conditions.iter().any(|condition| {
                condition.reason == "ScaleUpBoundaryPropagationPending"
                    && condition.message.contains("phase=catch-up")
            })
    ));
}

#[test]
fn scale_up_transition_without_provisioning_provenance_is_rejected_without_panic() {
    use scale_up_model::Model;
    let mut model = Model::new(1, 2);
    while model.snapshot.status.transition.is_none() {
        model.step();
    }

    fn scale_up_frozen_cleanup_failover_delay_keeps_full_context() {
        use scale_up_model::Model;
        let mut model = Model::new(3, 4);
        while model.snapshot.status.provisioning.is_none() {
            model.step();
        }
        model.snapshot.desired.replicas = 3;
        let Plan::Apply { changes } = model.plan() else {
            panic!("freeze cleanup")
        };
        for change in changes {
            model.apply(change);
        }
        model.snapshot.secondary_scale_down_resources.clear();
        for replica_id in [1, 3] {
            model.report_mut(replica_id).healthy = false;
            model.report_mut(replica_id).reported_fault = Some(FaultType::Permanent);
            model.report_mut(replica_id).write_status = AccessStatus::ReconfigurationPending;
        }
        let Plan::Apply { changes } = model.plan() else {
            panic!("persist primary failure")
        };
        for change in changes {
            model.apply(change);
        }
        let Plan::Wait { status, .. } = model.plan() else {
            panic!("failover delay")
        };
        let condition = status
            .conditions
            .iter()
            .find(|condition| condition.reason == "FailoverDelay")
            .unwrap();
        for field in [
            "accepted=3",
            "desired=3",
            "target=4@",
            "attempt=scale-up-provisioning-",
            "phase=failover-recovery",
            "blocking=",
        ] {
            assert!(
                condition.message.contains(field),
                "{field}: {}",
                condition.message
            );
        }
    }
    scale_up_frozen_cleanup_failover_delay_keeps_full_context();
    model.snapshot.status.provisioning = None;
    assert!(kuberic_protocol::validation::validate_snapshot(&model.snapshot).is_err());
    assert!(matches!(
        evaluate(&model.snapshot, &scale_up_model::config()),
        Plan::Unsafe {
            reason: UnsafeReason::InvalidAcceptedAuthority(_),
            ..
        }
    ));
}

#[test]
fn scale_up_admission_fence_prevents_cleanup_after_candidate_pc_cc_disappears() {
    use scale_up_model::Model;
    let mut model = Model::new(1, 2);
    loop {
        match model.plan() {
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(command),
            } if !command.current_only && command.local_replica_id == ReplicaId::new(2) => {
                model.execute(ProtocolCommand::EnsureConfiguration(command));
                break;
            }
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            Plan::Execute { command } => model.execute(command),
            Plan::Wait { status, .. } => model.apply_wait(status),
            other => panic!("admission-fence setup: {other:?}"),
        }
    }
    let intent = model
        .snapshot
        .status
        .transition
        .as_ref()
        .and_then(|transition| transition.scale_up.as_deref())
        .unwrap()
        .clone();
    assert_eq!(
        model.snapshot.status.scale_up_admission_started.as_ref(),
        Some(&intent.operation_id)
    );
    model.remove_candidate_report();
    model.snapshot.desired.replicas = 1;
    assert!(matches!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(command)
        } if command.local_replica_id == intent.primary.replica_id
            && !command.current_only
    ));
    assert!(model.snapshot.status.scale_up_cleanup.is_none());
    assert!(model.snapshot.status.transition.is_some());
}

#[test]
fn scale_up_diagnostics_republish_exact_desired_count_after_churn() {
    use scale_up_model::Model;
    let mut copying = Model::new(1, 2);
    loop {
        match copying.plan() {
            Plan::Apply { changes }
                if changes.iter().any(|change| {
                    matches!(
                        change,
                        KubernetesChange::PersistStatus { status }
                            if status.conditions.iter().any(|condition|
                                condition.reason == "ScaleUpCopying")
                    )
                }) =>
            {
                for change in changes {
                    copying.apply(change);
                }
                break;
            }
            Plan::Apply { changes } => {
                for change in changes {
                    copying.apply(change);
                }
            }
            Plan::Execute { command } => copying.execute(command),
            Plan::Wait { status, .. } => copying.apply_wait(status),
            other => panic!("copy diagnostic setup: {other:?}"),
        }
    }
    copying.snapshot.desired.replicas = 3;
    copying.snapshot.desired.generation += 1;
    let Plan::Apply { changes } = copying.plan() else {
        panic!("copy diagnostics must republish desired churn")
    };
    assert!(changes.iter().any(|change| matches!(
        change,
        KubernetesChange::PersistStatus { status }
            if status.conditions.iter().any(|condition| {
                condition.reason == "ScaleUpCopying"
                    && condition.message.contains("desired=3")
            })
    )));

    let mut current_only = Model::new(2, 3);
    loop {
        match current_only.plan() {
            Plan::Apply { changes }
                if changes.iter().any(|change| {
                    matches!(
                        change,
                        KubernetesChange::PersistStatus { status }
                            if status.conditions.iter().any(|condition|
                                condition.reason == "ScaleUpCurrentOnlyInstalling")
                    )
                }) =>
            {
                for change in changes {
                    current_only.apply(change);
                }
                break;
            }
            Plan::Apply { changes } => {
                for change in changes {
                    current_only.apply(change);
                }
            }
            Plan::Execute { command } => current_only.execute(command),
            Plan::Wait { status, .. } => current_only.apply_wait(status),
            other => panic!("current-only diagnostic setup: {other:?}"),
        }
    }
    current_only.snapshot.desired.replicas = 4;
    current_only.snapshot.desired.generation += 1;
    let Plan::Apply { changes } = current_only.plan() else {
        panic!("current-only diagnostics must republish desired churn")
    };
    assert!(changes.iter().any(|change| matches!(
        change,
        KubernetesChange::PersistStatus { status }
            if status.conditions.iter().any(|condition| {
                condition.reason == "ScaleUpCurrentOnlyInstalling"
                    && condition.message.contains("desired=4")
            })
    )));
}

#[test]
fn scale_up_receipt_repairs_member_that_missed_pc_cc_before_next_addition() {
    use scale_up_model::Model;
    let mut model = Model::new(3, 5);
    while model.snapshot.status.scale_up_admission_started.is_none() {
        model.step();
    }

    fn scale_up_admission_fence_recovers_primary_loss_at_every_pc_cc_dispatch_cut() {
        use scale_up_model::Model;
        let mut model = Model::new(2, 3);
        while model.snapshot.status.scale_up_admission_started.is_none() {
            model.step();
        }

        fn scale_up_transient_pre_admission_primary_failure_clears_before_fencing() {
            use scale_up_model::Model;
            let mut model = Model::new(2, 3);
            while model.snapshot.status.transition.is_none() {
                model.step();
            }
            assert!(model.snapshot.status.scale_up_admission_started.is_none());
            model.report_mut(1).reported_fault = Some(FaultType::Permanent);
            model.report_mut(1).write_status = AccessStatus::ReconfigurationPending;
            let Plan::Apply { changes } = model.plan() else {
                panic!("primary failure observation")
            };
            for change in changes {
                if let KubernetesChange::PersistStatus { status } = &change {
                    kuberic_protocol::validation::validate_status(status).unwrap();
                }
                model.apply(change);
            }
            assert!(model.snapshot.status.primary_failure.is_some());

            model.report_mut(1).reported_fault = None;
            model.report_mut(1).write_status = AccessStatus::Granted;
            let Plan::Apply { changes } = model.plan() else {
                panic!("primary recovery persistence")
            };
            for change in changes {
                if let KubernetesChange::PersistStatus { status } = &change {
                    kuberic_protocol::validation::validate_status(status).unwrap();
                }
                model.apply(change);
            }
            assert!(model.snapshot.status.primary_failure.is_none());

            for _ in 0..4 {
                let Plan::Apply { changes } = model.plan() else {
                    panic!("service restoration/fence status")
                };
                let mut fenced = false;
                for change in changes {
                    if let KubernetesChange::PersistStatus { status } = &change {
                        kuberic_protocol::validation::validate_status(status).unwrap();
                        fenced |= status.scale_up_admission_started.is_some();
                    }
                    model.apply(change);
                }
                if fenced {
                    break;
                }
            }
            assert!(model.snapshot.status.primary_failure.is_none());
            assert!(model.snapshot.status.scale_up_admission_started.is_some());
        }

        fn scale_up_frozen_cleanup_restores_service_before_waiting_for_observations() {
            use scale_up_model::Model;
            let mut model = Model::new(3, 4);
            while model.snapshot.status.provisioning.is_none() {
                model.step();
            }
            model.snapshot.desired.replicas = 3;
            let Plan::Apply { changes } = model.plan() else {
                panic!("freeze cancellation cleanup")
            };
            for change in changes {
                model.apply(change);
            }
            assert!(model.snapshot.status.scale_up_cleanup.is_some());
            let exact = model.snapshot.secondary_scale_down_resources.clone();
            model.snapshot.secondary_scale_down_resources.clear();
            for replica_id in [2, 3] {
                model.report_mut(replica_id).healthy = false;
                model.report_mut(replica_id).reported_fault = Some(FaultType::Transient);
            }
            let Plan::Wait { status, .. } = model.plan() else {
                panic!("cleanup quorum diagnostics")
            };
            let condition = status
                .conditions
                .iter()
                .find(|condition| condition.reason == "ScaleUpFailoverWriteQuorumPending")
                .unwrap();
            for field in [
                "accepted=3",
                "desired=3",
                "target=4@",
                "attempt=",
                "phase=cleanup",
                "blocking=",
            ] {
                assert!(
                    condition.message.contains(field),
                    "{field}: {}",
                    condition.message
                );
            }
            for replica_id in [2, 3] {
                model.report_mut(replica_id).healthy = true;
                model.report_mut(replica_id).reported_fault = None;
            }
            model.report_mut(1).reported_fault = Some(FaultType::Permanent);
            model.report_mut(1).write_status = AccessStatus::ReconfigurationPending;

            for _ in 0..100 {
                match model.plan() {
                    Plan::Apply { changes } => {
                        for change in changes {
                            model.apply(change);
                        }
                    }
                    Plan::Execute { command } => model.execute(command),
                    Plan::Wait {
                        reason: WaitReason::FailoverDelay,
                        status,
                        ..
                    } => {
                        model.apply_wait(status);
                        model.snapshot.now_unix_seconds += 10;
                    }
                    Plan::Wait { status, .. } => model.apply_wait(status),
                    Plan::Stable { status, .. } => model.snapshot.status = status,
                    Plan::Unsafe { reason, .. } => {
                        panic!("frozen cleanup failover unsafe: {reason:?}")
                    }
                }
                let accepted = &model
                    .snapshot
                    .status
                    .topology
                    .as_ref()
                    .unwrap()
                    .configuration;
                if model.snapshot.status.transition.is_none()
                    && accepted.primary_id != ReplicaId::new(1)
                    && model
                        .snapshot
                        .routing
                        .write_target
                        .as_ref()
                        .map(|identity| identity.replica_id)
                        == Some(accepted.primary_id)
                {
                    break;
                }
            }
            let accepted = &model
                .snapshot
                .status
                .topology
                .as_ref()
                .unwrap()
                .configuration;
            assert_ne!(accepted.primary_id, ReplicaId::new(1));
            assert_eq!(
                model
                    .snapshot
                    .routing
                    .write_target
                    .as_ref()
                    .map(|identity| identity.replica_id),
                Some(accepted.primary_id)
            );
            assert!(matches!(
                model.plan(),
                Plan::Wait { status, .. }
                    if status.conditions.iter().any(|condition|
                        condition.reason == "ScaleUpCleanupObservationPending")
            ));
            model.snapshot.secondary_scale_down_resources = exact;
            for _ in 0..8 {
                match model.plan() {
                    Plan::Apply { changes } => {
                        for change in changes {
                            model.apply(change);
                        }
                    }
                    Plan::Wait { status, .. } => model.apply_wait(status),
                    other => panic!("frozen cleanup convergence: {other:?}"),
                }
                if model.snapshot.status.scale_up_cleanup.is_none() {
                    break;
                }
            }
            assert!(model.snapshot.status.scale_up_cleanup.is_none());
        }
        scale_up_transient_pre_admission_primary_failure_clears_before_fencing();
        scale_up_frozen_cleanup_restores_service_before_waiting_for_observations();
        let mut cuts = vec![model.fork()];
        while cuts.len() < 3 {
            match model.plan() {
                Plan::Execute {
                    command: ProtocolCommand::EnsureConfiguration(command),
                } if !command.current_only && command.local_replica_id != ReplicaId::new(1) => {
                    model.execute(ProtocolCommand::EnsureConfiguration(command));
                    cuts.push(model.fork());
                }
                Plan::Apply { changes } => {
                    for change in changes {
                        model.apply(change);
                    }
                }
                Plan::Execute { command } => model.execute(command),
                Plan::Wait { status, .. } => model.apply_wait(status),
                other => panic!("PC/CC cut setup: {other:?}"),
            }
        }

        for (index, mut cut) in cuts.into_iter().enumerate() {
            cut.report_mut(1).reported_fault = Some(FaultType::Permanent);
            cut.report_mut(1).write_status = AccessStatus::ReconfigurationPending;
            for _ in 0..30 {
                match cut.plan() {
                    Plan::Apply { changes } => {
                        for change in changes {
                            cut.apply(change);
                        }
                    }
                    Plan::Execute { command } => cut.execute(command),
                    Plan::Wait { status, .. } => cut.apply_wait(status),
                    Plan::Stable { status, .. } => cut.snapshot.status = status,
                    Plan::Unsafe { reason, .. } => {
                        panic!("cut {index} became unsafe: {reason:?}")
                    }
                }
                if cut
                    .snapshot
                    .status
                    .transition
                    .as_ref()
                    .is_some_and(|transition| transition.scale_up_failover.is_some())
                {
                    break;
                }
            }
            assert!(
                cut.snapshot
                    .status
                    .transition
                    .as_ref()
                    .is_some_and(|transition| transition.scale_up_failover.is_some())
            );
            assert!(cut.snapshot.status.scale_up_cleanup.is_none());
        }
    }
    scale_up_admission_fence_recovers_primary_loss_at_every_pc_cc_dispatch_cut();
    let key = model
        .snapshot
        .replicas
        .iter()
        .find_map(|(key, observation)| match &observation.agent {
            AgentObservation::Report(report) if report.identity.replica_id == ReplicaId::new(2) => {
                Some(key.clone())
            }
            _ => None,
        })
        .unwrap();
    let saved = model.snapshot.replicas.get(&key).unwrap().clone();
    model.snapshot.replicas.get_mut(&key).unwrap().agent = AgentObservation::Absent;
    for _ in 0..100 {
        model.step();
        if model.accepted_count() == 4 && model.snapshot.status.transition.is_none() {
            break;
        }
    }
    assert_eq!(model.accepted_count(), 4);
    assert!(model.snapshot.status.last_scale_up.is_some());
    model.snapshot.replicas.insert(key, saved);

    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(pc_cc),
    } = model.plan()
    else {
        panic!("returning retained member must receive receipt-backed PC/CC")
    };
    assert!(!pc_cc.current_only);
    assert_eq!(pc_cc.local_replica_id, ReplicaId::new(2));
    model.execute(ProtocolCommand::EnsureConfiguration(pc_cc));

    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(current_only),
    } = model.plan()
    else {
        panic!("returning retained member must receive receipt-backed current-only")
    };
    assert!(current_only.current_only);
    assert_eq!(current_only.local_replica_id, ReplicaId::new(2));
    model.execute(ProtocolCommand::EnsureConfiguration(current_only));

    for _ in 0..20 {
        match model.plan() {
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            Plan::Execute { command } => model.execute(command),
            Plan::Wait { status, .. } => model.apply_wait(status),
            Plan::Stable { status, .. } => model.snapshot.status = status,
            Plan::Unsafe { reason, .. } => panic!("receipt repair unsafe: {reason:?}"),
        }
        if model.snapshot.status.provisioning.is_some()
            || model.snapshot.status.transition.is_some()
        {
            break;
        }
    }
    assert!(
        model.snapshot.status.provisioning.is_some() || model.snapshot.status.transition.is_some(),
        "next sequential addition did not begin after receipt repair"
    );
}

#[test]
fn scale_up_primary_failover_completes_before_deferred_exact_cleanup() {
    use scale_up_model::Model;
    let mut model = Model::new(3, 4);
    while model.snapshot.status.provisioning.is_none() {
        model.step();
    }
    model.snapshot.desired.replicas = 3;
    let exact = model.snapshot.secondary_scale_down_resources.clone();
    model.snapshot.secondary_scale_down_resources.clear();
    model.report_mut(1).reported_fault = Some(FaultType::Permanent);
    model.report_mut(1).write_status = AccessStatus::ReconfigurationPending;

    for _ in 0..100 {
        match model.plan() {
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            Plan::Execute { command } => model.execute(command),
            Plan::Wait {
                reason: WaitReason::FailoverDelay,
                status,
                ..
            } => {
                model.snapshot.status = status;
                model.snapshot.now_unix_seconds += 10;
            }
            Plan::Wait { status, .. } => model.apply_wait(status),
            Plan::Stable { status, .. } => model.snapshot.status = status,
            Plan::Unsafe { reason, .. } => panic!("deferred cleanup trace unsafe: {reason:?}"),
        }
        let accepted = &model
            .snapshot
            .status
            .topology
            .as_ref()
            .unwrap()
            .configuration;
        if model.snapshot.status.transition.is_none() && accepted.primary_id != ReplicaId::new(1) {
            break;
        }
    }
    let accepted = model
        .snapshot
        .status
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .clone();
    assert_ne!(accepted.primary_id, ReplicaId::new(1), "{:?}", model.plan());
    assert!(model.snapshot.status.provisioning.is_some());
    assert!(model.snapshot.status.scale_up_cleanup.is_none());

    for _ in 0..4 {
        match model.plan() {
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            Plan::Wait { status, .. } => {
                model.snapshot.status = status;
                break;
            }
            Plan::Execute { command } => model.execute(command),
            other => panic!("service restoration before cleanup: {other:?}"),
        }
    }
    assert_eq!(
        model
            .snapshot
            .routing
            .write_target
            .as_ref()
            .map(|identity| identity.replica_id),
        Some(accepted.primary_id)
    );
    assert!(matches!(
        model.plan(),
        Plan::Wait { status, .. }
            if status.conditions.iter().any(|condition|
                condition.reason == "ScaleUpExactCleanupEvidencePending")
    ));

    model.snapshot.secondary_scale_down_resources = exact;
    let Plan::Apply { changes } = model.plan() else {
        panic!("exact cleanup must freeze after failover restoration")
    };
    for change in changes {
        model.apply(change);
    }
    assert!(model.snapshot.status.scale_up_cleanup.is_some());
    assert!(model.snapshot.status.provisioning.is_none());
    for _ in 0..8 {
        match model.plan() {
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            Plan::Wait { status, .. } => model.apply_wait(status),
            other => panic!("cleanup convergence: {other:?}"),
        }
        if model.snapshot.status.scale_up_cleanup.is_none() {
            break;
        }
    }
    assert!(model.snapshot.status.scale_up_cleanup.is_none());
}

#[test]
fn scale_up_late_carried_failover_member_reaches_stable_convergence() {
    use scale_up_model::Model;
    let mut model = Model::new(2, 3);
    loop {
        let pc_cc_primary_installed = model
            .snapshot
            .status
            .transition
            .as_ref()
            .and_then(|transition| transition.scale_up.as_deref())
            .is_some_and(|intent| {
                model
                    .snapshot
                    .observation_for_identity(&intent.primary)
                    .and_then(|observation| match &observation.agent {
                        AgentObservation::Report(report) => Some(report),
                        _ => None,
                    })
                    .is_some_and(|report| report.previous_configuration.is_some())
            });
        if pc_cc_primary_installed {
            break;
        }
        model.step();
    }
    model.report_mut(1).reported_fault = Some(FaultType::Permanent);
    model.report_mut(1).write_status = AccessStatus::ReconfigurationPending;
    for _ in 0..80 {
        match model.plan() {
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            Plan::Execute { command } => model.execute(command),
            Plan::Wait { status, .. } => model.apply_wait(status),
            Plan::Stable { status, .. } => model.snapshot.status = status,
            Plan::Unsafe { reason, .. } => panic!("carried failover unsafe: {reason:?}"),
        }
        if model.accepted_count() == 3 && model.snapshot.status.transition.is_none() {
            break;
        }
    }
    let receipt = model.snapshot.status.last_scale_up.clone().unwrap();
    let old_primary = receipt.intent.primary.clone();
    let report = model.report_mut(old_primary.replica_id.value());
    report.healthy = true;
    report.reported_fault = None;
    report.role = ReplicaRole::Primary;
    report.read_status = AccessStatus::ReconfigurationPending;
    report.write_status = AccessStatus::ReconfigurationPending;
    report.epoch = receipt.intent.current_configuration.epoch;
    report.previous_configuration = Some(receipt.intent.previous_configuration.clone());
    report.current_configuration = Some(receipt.intent.current_configuration.clone());
    report.current_progress = receipt.intent.catch_up_boundary_lsn;
    report.verified_replication_lsn = Some(receipt.intent.catch_up_boundary_lsn);
    report.committed_lsn = receipt.intent.catch_up_boundary_lsn;
    report.pending_operation_id = None;
    report.retained_operation_id = Some(receipt.intent.command_operation_id(
        kuberic_protocol::types::ScaleUpStage::PreviousCurrent,
        &old_primary,
        &receipt.intent.current_configuration,
    ));
    report.scale_up_intent = Some(Box::new(receipt.intent.clone()));
    report.report_sequence += 1;

    let mut correction = None;
    for _ in 0..4 {
        match model.plan() {
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(command),
            } => {
                correction = Some(command);
                break;
            }
            other => panic!(
                "late carried member must receive exact failover PC/CC correction: {other:?}"
            ),
        }
    }
    let command = correction.expect("failover PC/CC correction");
    assert!(!command.current_only);
    assert_eq!(command.transition_kind, TransitionKind::Failover);
    assert!(command.failover_safe_lsn.is_some());
    {
        let pending = model.report_mut(old_primary.replica_id.value());
        pending.pending_operation_id = Some(command.operation_id.clone());
        pending.pending_configuration = Some(command.clone());
    }
    assert_eq!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(command.clone())
        },
        "carried failover PC/CC must replay byte-identically before installation"
    );
    {
        let installed = model.report_mut(old_primary.replica_id.value());
        installed.epoch = command.current_epoch;
        installed.previous_configuration = command.previous_configuration.clone();
        installed.current_configuration = Some(command.current_configuration.clone());
        installed.role = command
            .current_configuration
            .members
            .iter()
            .find(|member| member.identity == old_primary)
            .unwrap()
            .role;
    }
    assert_eq!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(command.clone())
        },
        "installed carried failover PC/CC remains pending until local completion"
    );
    let mut mutated = model.fork();
    let mutated_report = mutated.report_mut(old_primary.replica_id.value());
    let mut mutated_command = command.clone();
    mutated_command.failover_safe_lsn = mutated_command.failover_safe_lsn.map(|lsn| lsn + 1);
    mutated_report.pending_configuration = Some(mutated_command);
    assert!(matches!(
        mutated.plan(),
        Plan::Unsafe {
            safety_changes,
            ..
        } if safety_changes == vec![SafetyChange::RemoveWriteRouting]
    ));
    {
        let completed = model.report_mut(old_primary.replica_id.value());
        completed.pending_operation_id = None;
        completed.pending_configuration = None;
        completed.retained_operation_id = Some(command.operation_id.clone());
    }

    let Plan::Execute {
        command: ProtocolCommand::EnsureConfiguration(current_only),
    } = model.plan()
    else {
        panic!("late carried member must receive exact current-only correction")
    };
    assert!(current_only.current_only);
    assert_eq!(current_only.transition_kind, TransitionKind::Failover);
    assert_eq!(current_only.failover_safe_lsn, command.failover_safe_lsn);
    {
        let pending = model.report_mut(old_primary.replica_id.value());
        pending.pending_operation_id = Some(current_only.operation_id.clone());
        pending.pending_configuration = Some(current_only.clone());
    }
    assert_eq!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(current_only.clone())
        }
    );
    {
        let installed = model.report_mut(old_primary.replica_id.value());
        installed.previous_configuration = None;
        installed.current_configuration = Some(current_only.current_configuration.clone());
        installed.epoch = current_only.current_epoch;
    }
    assert_eq!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(current_only.clone())
        },
        "installed carried current-only authority cannot settle a pending receipt"
    );
    assert!(!matches!(model.plan(), Plan::Stable { .. }));
    {
        let completed = model.report_mut(old_primary.replica_id.value());
        completed.pending_operation_id = None;
        completed.pending_configuration = None;
        completed.retained_operation_id = Some(current_only.operation_id);
    }

    for _ in 0..20 {
        match model.plan() {
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            Plan::Execute { command } => model.execute(command),
            Plan::Wait { status, .. } => model.apply_wait(status),
            Plan::Stable { status, .. } => {
                model.snapshot.status = status;
                break;
            }
            Plan::Unsafe { reason, .. } => panic!("late convergence unsafe: {reason:?}"),
        }
    }
    assert!(matches!(
        model.plan(),
        Plan::Stable { status, .. }
            if status.conditions.iter().any(|condition|
                condition.reason == "ScaleUpStable")
    ));
}

#[test]
fn scale_up_real_three_to_two_to_three_restores_fresh_ordinal() {
    let mut reduced = scale_down_model::Model::new(&[1, 2, 3], 1, 2);
    reduced.finish();
    let removed = reduced.removed.first().cloned().expect("removed ordinal");
    assert_eq!(removed.replica_id, ReplicaId::new(3));
    let mut restored = scale_up_model::Model::from_snapshot(reduced.snapshot.clone(), vec![2]);
    restored.snapshot.desired.replicas = 3;
    restored.snapshot.desired.generation += 1;
    restored.run(120);
    let receipt = restored.snapshot.status.last_scale_up.as_ref().unwrap();
    assert_eq!(receipt.intent.target.replica_id, ReplicaId::new(3));
    assert_ne!(receipt.intent.target.instance_id, removed.instance_id);
    assert_ne!(
        receipt.intent.target.agent_generation,
        removed.agent_generation
    );
}

#[test]
fn scale_up_copy_model_closes_concurrent_writes_across_source_and_candidate_restarts() {
    use scale_up_model::Model;
    let mut model = Model::new(1, 2);
    let build_command = loop {
        match model.plan() {
            Plan::Execute {
                command: ProtocolCommand::EnsureReplicaBuild(command),
            } => break command,
            Plan::Execute { command } => model.execute(command),
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            other => panic!("copy setup: {other:?}"),
        }
    };
    let target = build_command.target.clone();
    model.restart(build_command.local_replica_id.value());
    model.restart(target.replica_id.value());
    assert!(matches!(
        model.plan(),
        Plan::Execute {
            command: ProtocolCommand::EnsureReplicaBuild(replayed)
        } if replayed == build_command
    ));
    model.execute(ProtocolCommand::EnsureReplicaBuild(build_command.clone()));
    let source = model
        .report_mut(build_command.local_replica_id.value())
        .clone();
    let source_build = source
        .builds
        .iter()
        .find(|build| build.build_id == build_command.operation_id)
        .unwrap();
    assert_eq!(source_build.replication_boundary_lsn, 10);
    assert_eq!(source_build.catch_up_boundary_lsn, None);
    assert!(!source_build.completed);
    assert!(
        model
            .report_mut(target.replica_id.value())
            .builds
            .is_empty()
    );

    model.restart(build_command.local_replica_id.value());
    model.restart(target.replica_id.value());
    let Plan::Execute {
        command: ProtocolCommand::EnsureReplicaBuild(replayed),
    } = model.plan()
    else {
        panic!("source-only durable build evidence must reissue the exact command")
    };
    assert_eq!(*replayed, *build_command);
    model.execute(ProtocolCommand::EnsureReplicaBuild(replayed));
    let Plan::Wait { status, .. } = model.plan() else {
        panic!("an active exact build must not be dispatched twice")
    };
    model.apply_wait(status);
    let candidate = model.report_mut(target.replica_id.value()).clone();
    let target_build = candidate
        .builds
        .iter()
        .find(|build| build.build_id == build_command.operation_id)
        .unwrap();
    assert_eq!(target_build.catch_up_boundary_lsn, None);
    assert!(!target_build.completed);

    model.restart(target.replica_id.value());
    let Plan::Wait { status, .. } = model.plan() else {
        panic!("enumeration must remain incomplete")
    };
    model.apply_wait(status);
    let source = model
        .report_mut(build_command.local_replica_id.value())
        .clone();
    assert_eq!(source.current_progress, 12);
    assert_eq!(source.builds[0].catch_up_boundary_lsn, Some(12));
    assert!(source.builds[0].completed);
    let candidate = model.report_mut(target.replica_id.value()).clone();
    assert_eq!(candidate.builds[0].catch_up_boundary_lsn, None);

    model.restart(build_command.local_replica_id.value());
    let frozen_snapshot_boundary = source.builds[0].replication_boundary_lsn;
    let frozen_catch_up_boundary = source.builds[0].catch_up_boundary_lsn;
    let Plan::Execute {
        command: ProtocolCommand::EnsureReplicaBuild(replayed),
    } = model.plan()
    else {
        panic!("cancelled catch-up delivery must reissue the exact command")
    };
    assert_eq!(*replayed, *build_command);
    model.execute(ProtocolCommand::EnsureReplicaBuild(replayed));
    let Plan::Wait { status, .. } = model.plan() else {
        panic!("resumed exact build must remain single-dispatch")
    };
    model.apply_wait(status);
    let source = model
        .report_mut(build_command.local_replica_id.value())
        .clone();
    assert_eq!(
        source.builds[0].replication_boundary_lsn,
        frozen_snapshot_boundary
    );
    assert_eq!(
        source.builds[0].catch_up_boundary_lsn,
        frozen_catch_up_boundary
    );
    let candidate = model.report_mut(target.replica_id.value()).clone();
    assert_eq!(candidate.current_progress, 12);
    assert_eq!(candidate.builds[0].durable_lsn, 12);
    assert_eq!(candidate.builds[0].catch_up_boundary_lsn, Some(12));
    assert!(candidate.builds[0].completed);
    let copied = model
        .durable_receivers
        .get(&target.replica_id.value())
        .unwrap();
    for lsn in 1..=12 {
        assert_eq!(copied.get(&lsn), model.durable_source.get(&lsn));
    }

    let build_operation = OperationId::new(format!("{}:build-replica", build_command.operation_id));
    model
        .report_mut(build_command.local_replica_id.value())
        .pending_operation_id = Some(build_operation);
    assert!(matches!(
        model.plan(),
        Plan::Wait { status, .. }
            if status.conditions.iter().any(|condition|
                condition.reason == "ScaleUpBuildSettlementPending")
    ));
    model
        .report_mut(build_command.local_replica_id.value())
        .pending_operation_id = None;

    let before = model.plan();
    assert_eq!(before, model.plan());
    let Plan::Apply { changes } = before else {
        panic!("durable completed build must freeze PC/CC after restart")
    };
    for change in changes {
        model.apply(change);
    }
    model.run(80);
    assert_eq!(model.accepted_count(), 2);
}

#[test]
fn scale_up_partial_copy_with_ambiguous_source_operation_freezes_exact_cleanup() {
    use scale_up_model::Model;
    let mut model = Model::new(2, 3);
    let build_command = loop {
        match model.plan() {
            Plan::Execute {
                command: ProtocolCommand::EnsureReplicaBuild(command),
            } => break command,
            Plan::Execute { command } => model.execute(command),
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            other => panic!("copy setup: {other:?}"),
        }
    };
    let target = build_command.target.clone();
    model.execute(ProtocolCommand::EnsureReplicaBuild(build_command.clone()));
    let Plan::Wait { status, .. } = model.plan() else {
        panic!("active source build should wait")
    };
    model.apply_wait(status);
    assert!(
        !model
            .report_mut(target.replica_id.value())
            .builds
            .is_empty(),
        "receiver must retain partial durable evidence"
    );
    model
        .report_mut(build_command.local_replica_id.value())
        .pending_operation_id = Some(OperationId::new("unrelated-source-operation"));

    let Plan::Apply { changes } = model.plan() else {
        panic!("ambiguous partial copy must freeze exact cleanup")
    };
    let cleanup = changes.into_iter().find_map(|change| match change {
        KubernetesChange::PersistStatus { status } => status.scale_up_cleanup.map(|c| *c),
        _ => None,
    });
    let cleanup = cleanup.expect("exact authorized cleanup");
    assert_eq!(cleanup.target, target);
    assert_eq!(cleanup.provisioning.operation_id, {
        let provisioning = model.snapshot.status.provisioning.as_ref().unwrap();
        provisioning.operation_id.clone()
    });
}

#[test]
fn scale_up_permanent_candidate_failure_before_fence_freezes_cleanup() {
    use scale_up_model::Model;
    let mut model = Model::new(3, 4);
    while model.snapshot.status.transition.is_none() {
        model.step();
    }
    assert!(model.snapshot.status.scale_up_admission_started.is_none());
    model.report_mut(4).healthy = false;
    model.report_mut(4).reported_fault = Some(FaultType::Permanent);
    let failed_target = model.report_mut(4).identity.clone();
    let Plan::Apply { changes } = model.plan() else {
        panic!("permanent pre-admission candidate failure must freeze cleanup")
    };
    let status = changes.into_iter().find_map(|change| match change {
        KubernetesChange::PersistStatus { status } => Some(*status),
        _ => None,
    });
    let status = status.expect("cleanup status");
    assert!(status.scale_up_cleanup.is_some());
    assert!(status.scale_up_admission_started.is_none());
    assert!(status.transition.is_none());
    assert_eq!(
        status
            .topology
            .as_ref()
            .unwrap()
            .configuration
            .members
            .len(),
        3
    );
    model.snapshot.status = status;
    model.run(120);
    let replacement = model.snapshot.status.last_scale_up.as_ref().unwrap();
    assert_eq!(
        replacement.intent.target.replica_id,
        failed_target.replica_id
    );
    assert_ne!(
        replacement.intent.target.instance_id,
        failed_target.instance_id
    );
}

#[test]
fn scale_up_exact_invalid_unadmitted_candidate_cleans_and_retries_fresh() {
    use scale_up_model::Model;
    let mut model = Model::new(2, 3);
    let target = loop {
        if let Some(provisioning) = model.snapshot.status.provisioning.as_ref() {
            let target = provisioning.target_identity(&model.snapshot.resource_uid);
            if model
                .snapshot
                .observation_for_identity(&target)
                .is_some_and(|observation| {
                    matches!(
                        &observation.agent,
                        AgentObservation::Report(report)
                            if report.role == ReplicaRole::None && report.builds.is_empty()
                    )
                })
            {
                break target;
            }
        }
        model.step();
    };
    let target_key = ReplicaObservationKey::new(target.replica_id, target.instance_id.clone());
    model.snapshot.replicas.get_mut(&target_key).unwrap().agent = AgentObservation::Invalid {
        message: "candidate storage is corrupt".into(),
        uninitialized_report: None,
    };

    let mut accepted_invalid = model.fork();
    let accepted = accepted_invalid
        .snapshot
        .status
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .members[0]
        .identity
        .clone();
    accepted_invalid
        .snapshot
        .replicas
        .get_mut(&ReplicaObservationKey::new(
            accepted.replica_id,
            accepted.instance_id.clone(),
        ))
        .unwrap()
        .agent = AgentObservation::Invalid {
        message: "accepted storage is corrupt".into(),
        uninitialized_report: None,
    };
    assert!(matches!(
        accepted_invalid.plan(),
        Plan::Unsafe {
            safety_changes,
            ..
        } if safety_changes == vec![SafetyChange::RemoveWriteRouting]
    ));

    let mut unrelated_invalid = model.fork();
    unrelated_invalid.snapshot.replicas.insert(
        ReplicaObservationKey::new(target.replica_id, ReplicaInstanceId::new("unrelated-pod")),
        ReplicaObservation {
            kubernetes: None,
            agent: AgentObservation::Invalid {
                message: "unrelated invalid process".into(),
                uninitialized_report: None,
            },
        },
    );
    assert!(matches!(
        unrelated_invalid.plan(),
        Plan::Unsafe {
            safety_changes,
            ..
        } if safety_changes == vec![SafetyChange::RemoveWriteRouting]
    ));

    let Plan::Apply { changes } = model.plan() else {
        panic!("exact invalid unadmitted candidate must freeze candidate-local cleanup")
    };
    let status = changes.iter().find_map(|change| match change {
        KubernetesChange::PersistStatus { status } => Some(status.as_ref()),
        _ => None,
    });
    assert!(status.is_some_and(|status| status.scale_up_cleanup.is_some()));
    assert_eq!(
        model
            .snapshot
            .routing
            .write_target
            .as_ref()
            .map(|identity| identity.replica_id),
        Some(ReplicaId::new(1))
    );
    for change in changes {
        model.apply(change);
    }
    model.run(200);
    assert_eq!(model.accepted_count(), 3);
    let fresh = model
        .snapshot
        .status
        .last_scale_up
        .as_ref()
        .unwrap()
        .intent
        .target
        .clone();
    assert_eq!(fresh.replica_id, target.replica_id);
    assert_ne!(fresh.instance_id, target.instance_id);
    assert!(matches!(model.plan(), Plan::Stable { .. }));
}

#[test]
fn scale_up_invalid_candidate_exemption_ends_at_admission_boundaries() {
    use scale_up_model::Model;

    let mut model = Model::new(2, 3);
    let (pre_fence, fenced, target) = loop {
        match model.plan() {
            Plan::Apply { changes } => {
                let admission = changes.iter().find_map(|change| match change {
                    KubernetesChange::PersistStatus { status }
                        if status.scale_up_admission_started.is_some() =>
                    {
                        Some(status.as_ref().clone())
                    }
                    _ => None,
                });
                if let Some(status) = admission {
                    let pre_fence = model.fork();
                    let target = status
                        .transition
                        .as_ref()
                        .and_then(|transition| transition.scale_up.as_deref())
                        .expect("active scale-up intent")
                        .target
                        .clone();
                    model.snapshot.status = status;
                    break (pre_fence, model.fork(), target);
                }
                for change in changes {
                    model.apply(change);
                }
            }
            Plan::Execute { command } => model.execute(command),
            Plan::Wait { status, .. } => model.apply_wait(status),
            other => panic!("admission-fence setup stalled: {other:?}"),
        }
    };

    let invalidate = |model: &mut Model| {
        let key = ReplicaObservationKey::new(target.replica_id, target.instance_id.clone());
        model.snapshot.replicas.get_mut(&key).unwrap().agent = AgentObservation::Invalid {
            message: "candidate storage is corrupt".into(),
            uninitialized_report: None,
        };
    };

    let mut cancelled = pre_fence;
    cancelled.snapshot.desired.replicas = 2;
    invalidate(&mut cancelled);
    assert!(matches!(
        cancelled.plan(),
        Plan::Apply { changes }
            if changes.iter().any(|change| matches!(
                change,
                KubernetesChange::PersistStatus { status }
                    if status.scale_up_cleanup.is_some()
                        && status.scale_up_admission_started.is_none()
            ))
    ));

    let mut fenced_invalid = fenced.fork();
    invalidate(&mut fenced_invalid);
    let fenced_plan = fenced_invalid.plan();
    assert!(
        matches!(
            &fenced_plan,
            Plan::Unsafe {
                safety_changes,
                ..
            } if safety_changes == &vec![SafetyChange::RemoveWriteRouting]
        ),
        "fenced invalid candidate was not globally unsafe: {fenced_plan:?}"
    );

    let mut installed = fenced;
    let pc_cc = loop {
        match installed.plan() {
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(command),
            } if !command.current_only => break command,
            Plan::Execute { command } => installed.execute(command),
            Plan::Apply { changes } => {
                for change in changes {
                    installed.apply(change);
                }
            }
            Plan::Wait { status, .. } => installed.apply_wait(status),
            other => panic!("PC/CC boundary setup stalled: {other:?}"),
        }
    };
    installed.execute(ProtocolCommand::EnsureConfiguration(pc_cc));
    installed.snapshot.status.scale_up_admission_started = None;
    invalidate(&mut installed);
    assert!(matches!(
        installed.plan(),
        Plan::Unsafe {
            safety_changes,
            ..
        } if safety_changes == vec![SafetyChange::RemoveWriteRouting]
    ));

    let mut accepted = Model::new(2, 3);
    accepted.run(160);
    let accepted_target = accepted
        .snapshot
        .status
        .last_scale_up
        .as_ref()
        .unwrap()
        .intent
        .target
        .clone();
    accepted
        .snapshot
        .replicas
        .get_mut(&ReplicaObservationKey::new(
            accepted_target.replica_id,
            accepted_target.instance_id.clone(),
        ))
        .unwrap()
        .agent = AgentObservation::Invalid {
        message: "accepted candidate storage is corrupt".into(),
        uninitialized_report: None,
    };
    assert!(matches!(
        accepted.plan(),
        Plan::Unsafe {
            safety_changes,
            ..
        } if safety_changes == vec![SafetyChange::RemoveWriteRouting]
    ));
}

fn assert_scale_up_receipt_allows_full_replacement(replica_id: i64) {
    use kuberic_protocol::command::ScaleDownResource;
    use kuberic_protocol::observation::{
        ExactResourceObservation, SecondaryScaleDownResourceObservation,
    };
    use kuberic_protocol::types::{
        CleanupResourceIdentity, ReplicaCleanupIdentity, derive_replica_endpoint_name,
    };
    use scale_up_model::Model as ScaleUpModel;

    let mut scale_up = ScaleUpModel::new(2, 3);
    scale_up.run(120);
    assert_eq!(scale_up.accepted_history, vec![2, 3]);
    assert!(matches!(scale_up.plan(), Plan::Stable { .. }));
    assert!(scale_up.snapshot.status.provisioning.is_none());
    assert!(scale_up.snapshot.status.transition.is_none());
    assert!(scale_up.snapshot.status.scale_up_cleanup.is_none());
    let receipt = scale_up
        .snapshot
        .status
        .last_scale_up
        .clone()
        .expect("stable 2->3 receipt");
    let old = scale_up
        .snapshot
        .status
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .members
        .iter()
        .find(|member| member.identity.replica_id == ReplicaId::new(replica_id))
        .expect("accepted member")
        .identity
        .clone();

    if !scale_up
        .snapshot
        .secondary_scale_down_resources
        .iter()
        .any(|resources| resources.target == old)
    {
        let kubernetes = scale_up
            .snapshot
            .observation_for_identity(&old)
            .and_then(|observation| observation.kubernetes.as_ref())
            .expect("accepted member Kubernetes provenance")
            .clone();
        scale_up.snapshot.secondary_scale_down_resources.push(
            SecondaryScaleDownResourceObservation {
                resource_uid: scale_up.snapshot.resource_uid.clone(),
                target: old.clone(),
                identity: ReplicaCleanupIdentity {
                    pod: CleanupResourceIdentity::Present {
                        name: kubernetes.pod_name,
                        uid: kubernetes.pod_uid.unwrap().to_string(),
                    },
                    pvc: CleanupResourceIdentity::Present {
                        name: kubernetes.pvc_name,
                        uid: kubernetes.pvc_uid.unwrap().to_string(),
                    },
                    endpoint: CleanupResourceIdentity::Present {
                        name: derive_replica_endpoint_name(&scale_up.snapshot.resource_uid, &old),
                        uid: format!("accepted-endpoint-{}", old.replica_id),
                    },
                },
                pod: ExactResourceObservation::NotFound,
                pod_allocation_operation_id: None,
                pod_matches_allocation_metadata: false,
                pvc: ExactResourceObservation::NotFound,
                pvc_allocation_operation_id: None,
                endpoint: ExactResourceObservation::NotFound,
            },
        );
    }
    let exact = scale_up
        .snapshot
        .secondary_scale_down_resources
        .iter_mut()
        .find(|resources| resources.target == old)
        .expect("exact old-member resource provenance");
    exact.endpoint = ExactResourceObservation::FrozenUidPresent {
        resource_version: "old-endpoint-rv".into(),
    };
    exact.pod = ExactResourceObservation::FrozenUidPresent {
        resource_version: "old-pod-rv".into(),
    };
    exact.pvc = ExactResourceObservation::FrozenUidPresent {
        resource_version: "old-pvc-rv".into(),
    };
    let exact = exact.clone();
    let exact_uid = |identity: &CleanupResourceIdentity| match identity {
        CleanupResourceIdentity::Present { uid, .. } => uid.clone(),
        CleanupResourceIdentity::Absent { .. } => {
            panic!("replacement cleanup must retain exact UID provenance")
        }
    };
    let expected_deletes = vec![
        (
            ScaleDownResource::Endpoint,
            exact_uid(&exact.identity.endpoint),
        ),
        (ScaleDownResource::Pod, exact_uid(&exact.identity.pod)),
        (ScaleDownResource::Pvc, exact_uid(&exact.identity.pvc)),
    ];

    let failed_report = scale_up.report_mut(replica_id);
    failed_report.reported_fault = Some(FaultType::Permanent);
    failed_report.healthy = false;
    failed_report.report_sequence += 1;
    let mut replacement = scale_down_model::Model {
        snapshot: scale_up.snapshot,
        commands: Vec::new(),
        removed: Vec::new(),
        deletes: Vec::new(),
        inflight: BTreeMap::new(),
        applied_effects: BTreeMap::new(),
    };

    for _ in 0..30 {
        assert!(
            replacement.snapshot.status.scale_up_cleanup.is_none(),
            "an admitted member must never enter pre-admission scale-up cleanup"
        );
        if replacement
            .snapshot
            .status
            .pending_replacement_cleanup
            .is_some()
        {
            break;
        }
        let plan = evaluate(&replacement.snapshot, &scale_up_model::config());
        assert_eq!(
            plan,
            evaluate(&replacement.snapshot, &scale_up_model::config())
        );
        replacement.apply(plan);
    }
    let frozen = replacement
        .snapshot
        .status
        .pending_replacement_cleanup
        .clone()
        .unwrap_or_else(|| {
            panic!(
                "evaluator-produced replacement cleanup; final plan: {:?}",
                evaluate(&replacement.snapshot, &scale_up_model::config())
            )
        });
    assert_eq!(frozen.target, old);
    assert_eq!(frozen.resources, exact.identity);
    assert_eq!(
        replacement.snapshot.status.last_scale_up,
        Some(receipt.clone())
    );

    replacement.controller_restart();
    assert_eq!(
        replacement.snapshot.status.pending_replacement_cleanup,
        Some(frozen)
    );
    assert_eq!(
        replacement.snapshot.status.last_scale_up,
        Some(receipt.clone())
    );

    let failed_key = replacement
        .snapshot
        .replicas
        .iter()
        .find_map(|(key, observation)| match &observation.agent {
            AgentObservation::Report(report) if report.identity == old => Some(key.clone()),
            _ => None,
        })
        .expect("failed member report");
    replacement
        .snapshot
        .replicas
        .get_mut(&failed_key)
        .unwrap()
        .agent = AgentObservation::Absent;

    let mut saw_scaffolding = false;
    let mut saw_initialization = false;
    let mut saw_build = false;
    let mut saw_pc_cc = false;
    let mut saw_current_only = false;
    let mut saw_build_retirement = false;
    let mut saw_stable = false;
    let mut replacement_build_id = None;
    for _ in 0..250 {
        let plan = evaluate(&replacement.snapshot, &scale_up_model::config());
        assert_eq!(
            plan,
            evaluate(&replacement.snapshot, &scale_up_model::config())
        );
        assert!(
            !matches!(&plan, Plan::Wait { status, .. }
                if status.conditions.iter().any(|condition|
                    condition.reason == "ScaleUpCommittedDegraded")),
            "persisted replacement cleanup must prevent a ScaleUpCommittedDegraded deadlock"
        );
        match &plan {
            Plan::Apply { changes } => {
                saw_scaffolding |= changes.iter().any(|change| {
                    matches!(change, KubernetesChange::EnsureReplacementScaffolding {
                        replica_id: target,
                        replacing,
                    } if *target == old.replica_id && replacing == &old)
                });
            }
            Plan::Execute { command } => match command {
                ProtocolCommand::InitializeAgentStore(command) => {
                    saw_initialization |= command.local_replica_id == old.replica_id;
                }
                ProtocolCommand::EnsureReplicaBuild(command) => {
                    saw_build |= command.target.replica_id == old.replica_id;
                    replacement_build_id = Some(command.operation_id.clone());
                }
                ProtocolCommand::EnsureConfiguration(command)
                    if command.transition_kind == TransitionKind::Replacement =>
                {
                    saw_pc_cc |= !command.current_only;
                    saw_current_only |= command.current_only;
                    saw_build_retirement |=
                        command.current_only && !command.retire_build_ids.is_empty();
                }
                _ => {}
            },
            Plan::Stable { .. } => saw_stable = true,
            Plan::Wait { .. } | Plan::Unsafe { .. } => {}
        }
        let stable = matches!(plan, Plan::Stable { .. });
        replacement.apply(plan);
        assert!(
            replacement.snapshot.status.scale_up_cleanup.is_none(),
            "an admitted member must never enter pre-admission scale-up cleanup"
        );
        if stable {
            break;
        }
    }

    assert!(saw_scaffolding, "replacement scaffolding decision");
    assert!(saw_initialization, "replacement initialization decision");
    assert!(saw_build, "replacement build decision");
    assert!(saw_pc_cc, "replacement PC/CC configuration decisions");
    assert!(saw_current_only, "replacement current-only decisions");
    assert!(
        saw_build_retirement,
        "completed replacement build retirement"
    );
    assert!(saw_stable, "replacement must converge to Stable");
    let replacement_build_id = replacement_build_id.expect("replacement build operation");
    assert_eq!(replacement.deletes, expected_deletes);
    assert!(
        replacement
            .snapshot
            .status
            .pending_replacement_cleanup
            .is_none()
    );
    assert!(replacement.snapshot.status.last_replacement.is_none());
    assert_eq!(
        replacement.snapshot.status.last_scale_up,
        Some(receipt.clone())
    );

    let accepted = &replacement
        .snapshot
        .status
        .topology
        .as_ref()
        .unwrap()
        .configuration;
    let fresh = accepted
        .members
        .iter()
        .find(|member| member.identity.replica_id == old.replica_id)
        .expect("fresh accepted incarnation");
    assert_ne!(fresh.identity, old);
    assert!(
        accepted.members.iter().all(|member| member.identity != old),
        "failed incarnation must be retired from accepted membership"
    );
    assert!(
        !replacement
            .snapshot
            .replicas
            .contains_key(&ReplicaObservationKey::new(
                old.replica_id,
                old.instance_id.clone()
            )),
        "exact old Pod cleanup must remove the failed process observation"
    );
    for member in &accepted.members {
        let observation = replacement
            .snapshot
            .observation_for_identity(&member.identity)
            .expect("accepted member observation");
        let AgentObservation::Report(report) = &observation.agent else {
            panic!("accepted member report")
        };
        assert!(report.healthy);
        assert_eq!(report.role, member.role);
        assert_eq!(report.previous_configuration, None);
        assert_eq!(report.current_configuration.as_ref(), Some(accepted));
        assert_eq!(report.read_status, AccessStatus::Granted);
        assert!(
            report
                .builds
                .iter()
                .all(|build| build.build_id != replacement_build_id),
            "replacement build must be retired from every accepted member"
        );
        if member.identity.replica_id == accepted.primary_id {
            assert_eq!(report.write_status, AccessStatus::Granted);
        } else {
            assert_eq!(report.write_status, AccessStatus::NotPrimary);
        }
    }
    assert_eq!(
        replacement.snapshot.routing.write_target.as_ref(),
        accepted
            .members
            .iter()
            .find(|member| member.identity.replica_id == accepted.primary_id)
            .map(|member| &member.identity)
    );
    assert!(matches!(
        evaluate(&replacement.snapshot, &scale_up_model::config()),
        Plan::Stable { .. }
    ));
}

#[test]
fn scale_up_newly_admitted_member_permanently_fails_after_stable_two_to_three_and_replacement_converges()
 {
    assert_scale_up_receipt_allows_full_replacement(3);
}

#[test]
fn scale_up_retained_secondary_permanently_fails_after_stable_two_to_three_and_replacement_converges()
 {
    assert_scale_up_receipt_allows_full_replacement(2);
}

#[test]
fn scale_up_historical_receipt_does_not_block_ready_after_replacement_and_failover() {
    use scale_up_model::Model;

    let mut model = Model::new(2, 3);
    model.run(120);
    let receipt = model.snapshot.status.last_scale_up.clone().unwrap();
    let accepted = model
        .snapshot
        .status
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .clone();
    let former_primary = accepted
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .unwrap()
        .identity
        .clone();
    let new_primary = accepted
        .members
        .iter()
        .find(|member| member.identity != former_primary)
        .unwrap()
        .identity
        .clone();
    let failover = ConfigurationDescriptor::new(
        Epoch::new(
            accepted.epoch.data_loss_number,
            accepted.epoch.configuration_number + 1,
        ),
        new_primary.replica_id,
        accepted
            .members
            .iter()
            .map(|member| ConfigurationMember {
                identity: member.identity.clone(),
                role: if member.identity == new_primary {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        accepted.write_quorum,
    );
    let mut failed_former_primary = model.fork();
    failed_former_primary.snapshot.status.topology = Some(AcceptedTopology {
        configuration: failover.clone(),
    });
    failed_former_primary.snapshot.routing.write_target = Some(new_primary.clone());
    for member in &failover.members {
        let key = ReplicaObservationKey::new(
            member.identity.replica_id,
            member.identity.instance_id.clone(),
        );
        if member.identity == former_primary {
            failed_former_primary
                .snapshot
                .replicas
                .get_mut(&key)
                .unwrap()
                .agent = AgentObservation::Absent;
            continue;
        }
        let report = failed_former_primary.report_mut(member.identity.replica_id.value());
        report.role = member.role;
        report.epoch = failover.epoch;
        report.previous_configuration = None;
        report.current_configuration = Some(failover.clone());
        report.read_status = AccessStatus::Granted;
        report.write_status = if member.role == ReplicaRole::Primary {
            AccessStatus::Granted
        } else {
            AccessStatus::NotPrimary
        };
        report.pending_operation_id = None;
        report.pending_configuration = None;
    }
    let failover_plan = failed_former_primary.plan();
    assert!(
        !matches!(&failover_plan, Plan::Wait { status, .. }
            if status.conditions.iter().any(|condition|
                condition.reason == "ScaleUpCommittedDegraded")),
        "failed former primary blocked replacement after accepted failover: {failover_plan:?}"
    );
    let mut returning_former_primary = model.fork();
    returning_former_primary.snapshot.status.topology = Some(AcceptedTopology {
        configuration: failover.clone(),
    });
    returning_former_primary.snapshot.routing.write_target = Some(new_primary.clone());
    for member in &failover.members {
        let report = returning_former_primary.report_mut(member.identity.replica_id.value());
        if member.identity == former_primary {
            report.role = ReplicaRole::ActiveSecondary;
            report.write_status = AccessStatus::NotPrimary;
            report.healthy = true;
            report.reported_fault = None;
            report.pending_operation_id = None;
            report.pending_configuration = None;
            continue;
        }
        report.role = member.role;
        report.epoch = failover.epoch;
        report.previous_configuration = None;
        report.current_configuration = Some(failover.clone());
        report.read_status = AccessStatus::Granted;
        report.write_status = if member.role == ReplicaRole::Primary {
            AccessStatus::Granted
        } else {
            AccessStatus::NotPrimary
        };
    }
    let returning_plan = returning_former_primary.plan();
    assert!(
        matches!(
            returning_plan,
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(command)
            } if command.local_replica_id == former_primary.replica_id
                && command.current_configuration == failover
        ),
        "reachable stale former primary was not delegated to accepted-authority correction"
    );

    let replacement_identity = ReplicaIdentity {
        replica_id: former_primary.replica_id,
        instance_id: ReplicaInstanceId::new("post-failover-replacement-pod"),
        agent_generation: AgentGeneration::new("post-failover-replacement-generation"),
    };
    let settled = ConfigurationDescriptor::new(
        Epoch::new(
            failover.epoch.data_loss_number,
            failover.epoch.configuration_number + 1,
        ),
        new_primary.replica_id,
        failover
            .members
            .iter()
            .map(|member| ConfigurationMember {
                identity: if member.identity == former_primary {
                    replacement_identity.clone()
                } else {
                    member.identity.clone()
                },
                role: if member.identity == new_primary {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        failover.write_quorum,
    );
    let old_key = ReplicaObservationKey::new(
        former_primary.replica_id,
        former_primary.instance_id.clone(),
    );
    let old_observation = model.snapshot.replicas.get_mut(&old_key).unwrap();
    old_observation.kubernetes = None;
    let AgentObservation::Report(old_report) = &mut old_observation.agent else {
        panic!("former primary report")
    };
    old_report.role = ReplicaRole::ActiveSecondary;
    old_report.write_status = AccessStatus::NotPrimary;
    old_report.healthy = false;
    old_report.reported_fault = Some(FaultType::Permanent);
    old_report.epoch = failover.epoch;
    old_report.previous_configuration = None;
    old_report.current_configuration = Some(failover);
    old_report.pending_operation_id = None;
    old_report.pending_configuration = None;

    for member in &settled.members {
        if member.identity == replacement_identity {
            model.snapshot.replicas.insert(
                ReplicaObservationKey::new(
                    replacement_identity.replica_id,
                    replacement_identity.instance_id.clone(),
                ),
                ReplicaObservation {
                    kubernetes: Some(KubernetesReplicaObservation {
                        replica_id: replacement_identity.replica_id,
                        pod_name: format!("kvstore2-{}", replacement_identity.replica_id),
                        pod_uid: Some(PodUid::new(replacement_identity.instance_id.as_str())),
                        pvc_name: format!("kvstore2-{}-data", replacement_identity.replica_id),
                        pvc_uid: Some(PvcUid::new("post-failover-replacement-pvc")),
                        image: Some(model.snapshot.desired.image.clone()),
                        pod_ready: true,
                        peer_endpoint_ready: true,
                    }),
                    agent: AgentObservation::Report(Box::new(AgentReport {
                        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                        resource_uid: model.snapshot.resource_uid.clone(),
                        identity: replacement_identity.clone(),
                        process_session_id: ProcessSessionId::new(
                            "post-failover-replacement-session",
                        ),
                        report_sequence: 1,
                        role: member.role,
                        read_status: AccessStatus::Granted,
                        write_status: AccessStatus::NotPrimary,
                        healthy: true,
                        epoch: settled.epoch,
                        current_configuration: Some(settled.clone()),
                        current_progress: 12,
                        verified_replication_lsn: Some(12),
                        committed_lsn: 12,
                        ..Default::default()
                    })),
                },
            );
            continue;
        }
        let report = model.report_mut(member.identity.replica_id.value());
        report.role = member.role;
        report.epoch = settled.epoch;
        report.previous_configuration = None;
        report.current_configuration = Some(settled.clone());
        report.scale_up_intent = None;
        report.pending_operation_id = None;
        report.pending_configuration = None;
        report.retained_operation_id = Some(OperationId::new(format!(
            "settled-authority-{}",
            member.identity.replica_id
        )));
        report.read_status = AccessStatus::Granted;
        report.write_status = if member.role == ReplicaRole::Primary {
            AccessStatus::Granted
        } else {
            AccessStatus::NotPrimary
        };
    }
    model.snapshot.status.topology = Some(AcceptedTopology {
        configuration: settled.clone(),
    });
    model.snapshot.status.transition = None;
    model.snapshot.status.primary_failure = None;
    model.snapshot.status.pending_replacement_cleanup = None;
    model.snapshot.status.last_replacement = None;
    model.snapshot.status.last_scale_up = Some(receipt.clone());
    model.snapshot.routing.write_target = Some(new_primary.clone());

    let mut ordinary_failover = model.fork();
    ordinary_failover
        .snapshot
        .replicas
        .get_mut(&old_key)
        .unwrap()
        .agent = AgentObservation::Absent;
    let failed_primary = settled
        .members
        .iter()
        .find(|member| member.identity == new_primary)
        .unwrap()
        .identity
        .clone();
    let elected_primary = settled
        .members
        .iter()
        .find(|member| member.identity != failed_primary)
        .unwrap()
        .identity
        .clone();
    let failover_configuration = ConfigurationDescriptor::new(
        Epoch::new(
            settled.epoch.data_loss_number,
            settled.epoch.configuration_number + 1,
        ),
        elected_primary.replica_id,
        settled
            .members
            .iter()
            .map(|member| ConfigurationMember {
                identity: member.identity.clone(),
                role: if member.identity == elected_primary {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        settled.write_quorum,
    );
    let transition_id = derive_transition_id(
        &ordinary_failover.snapshot.resource_uid,
        TransitionKind::Failover,
        &failover_configuration.configuration_id,
    );
    ordinary_failover.snapshot.status.primary_failure = Some(PrimaryFailureObservation {
        primary: failed_primary.clone(),
        started_at_unix_seconds: ordinary_failover.snapshot.now_unix_seconds - 10,
    });
    ordinary_failover.snapshot.status.transition = Some(TransitionIntent {
        transition_id,
        kind: TransitionKind::Failover,
        spec_generation: ordinary_failover.snapshot.desired.generation,
        effective_policy: ordinary_failover
            .snapshot
            .status
            .effective_policy
            .clone()
            .unwrap(),
        previous_configuration_id: Some(settled.configuration_id.clone()),
        current_configuration: failover_configuration.clone(),
        election_lsn: Some(12),
        build_id: None,
        repair: None,
        switchover: None,
        secondary_scale_down: None,
        secondary_removal_evidence: None,
        scale_up: None,
        scale_up_failover: None,
    });
    for member in &settled.members {
        let report = ordinary_failover.report_mut(member.identity.replica_id.value());
        report.current_progress = 10;
        report.verified_replication_lsn = Some(10);
        report.committed_lsn = 10;
        report.current_configuration_quorum_progress = 10;
    }
    let failed_report = ordinary_failover.report_mut(failed_primary.replica_id.value());
    failed_report.healthy = false;
    failed_report.reported_fault = Some(FaultType::Permanent);
    failed_report.write_status = AccessStatus::ReconfigurationPending;
    failed_report.report_sequence += 1;

    fn apply_composed_plan(model: &mut scale_up_model::Model, plan: Plan) {
        match plan {
            Plan::Apply { changes } => {
                for change in changes {
                    model.apply(change);
                }
            }
            Plan::Execute { command } => model.execute(command),
            Plan::Wait { status, .. } => model.apply_wait(status),
            Plan::Stable { status, .. } => model.snapshot.status = status,
            Plan::Unsafe { reason, .. } => panic!("composed failover became unsafe: {reason:?}"),
        }
    }

    let mut composed = scale_up_model::Model::from_snapshot(ordinary_failover.snapshot, vec![2, 3]);
    let mut installed_pc_cc = false;
    for _ in 0..20 {
        let plan = evaluate(&composed.snapshot, &scale_up_model::config());
        assert!(
            !matches!(&plan, Plan::Wait { status, .. }
                if status.conditions.iter().any(|condition|
                    condition.reason == "ScaleUpCommittedDegraded")),
            "superseded receipt identity blocked ordinary failover before PC/CC: {plan:?}"
        );
        installed_pc_cc |= matches!(
            &plan,
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(command),
            } if command.transition_kind == TransitionKind::Failover && !command.current_only
        );
        apply_composed_plan(&mut composed, plan);
        if installed_pc_cc {
            break;
        }
    }
    assert!(installed_pc_cc, "ordinary failover PC/CC was not installed");
    let after_pc_cc = evaluate(&composed.snapshot, &scale_up_model::config());
    assert!(
        !matches!(&after_pc_cc, Plan::Wait { status, .. }
            if status.conditions.iter().any(|condition|
                condition.reason == "ScaleUpCommittedDegraded")),
        "superseded historical incarnation blocked failover after retained-member PC/CC: \
         {after_pc_cc:?}"
    );
    apply_composed_plan(&mut composed, after_pc_cc);
    let mut converged = false;
    for _ in 0..80 {
        let plan = evaluate(&composed.snapshot, &scale_up_model::config());
        assert!(
            !matches!(&plan, Plan::Wait { status, .. }
                if status.conditions.iter().any(|condition|
                    condition.reason == "ScaleUpCommittedDegraded")),
            "historical receipt blocked composed failover convergence: {plan:?}"
        );
        apply_composed_plan(&mut composed, plan);
        converged = composed.snapshot.status.transition.is_none()
            && composed.snapshot.status.last_scale_up.as_ref() == Some(&receipt)
            && composed
                .snapshot
                .status
                .topology
                .as_ref()
                .is_some_and(|topology| topology.configuration == failover_configuration);
        if converged {
            break;
        }
    }
    assert!(
        converged,
        "scale-up -> replacement -> ordinary failover transition did not converge with receipt \
         retained: {:?}",
        evaluate(&composed.snapshot, &scale_up_model::config())
    );

    let plan = model.plan();
    assert!(
        matches!(&plan, Plan::Stable { status, .. }
            if status.last_scale_up.as_ref() == Some(&receipt)),
        "superseded historical consumer blocked Ready or erased receipt: {plan:?}"
    );
}

#[test]
fn scale_up_provisioning_and_prefence_failover_delays_keep_context() {
    use scale_up_model::Model;
    for transition_persisted in [false, true] {
        let mut model = Model::new(3, 4);
        if transition_persisted {
            while model.snapshot.status.transition.is_none() {
                model.step();
            }
            assert!(model.snapshot.status.scale_up_admission_started.is_none());
        } else {
            while model.snapshot.status.provisioning.is_none() {
                model.step();
            }
        }
        for replica_id in [1, 3] {
            model.report_mut(replica_id).healthy = false;
            model.report_mut(replica_id).reported_fault = Some(FaultType::Permanent);
            model.report_mut(replica_id).write_status = AccessStatus::ReconfigurationPending;
        }
        let Plan::Apply { changes } = model.plan() else {
            panic!("persist primary failure")
        };
        for change in changes {
            model.apply(change);
        }
        let Plan::Wait { status, .. } = model.plan() else {
            panic!("contextual failover delay")
        };
        let condition = status
            .conditions
            .iter()
            .find(|condition| condition.reason == "FailoverDelay")
            .unwrap();
        for field in [
            "accepted=3",
            "desired=4",
            "target=4@",
            "attempt=",
            "phase=failover-recovery",
            "blocking=",
        ] {
            assert!(
                condition.message.contains(field),
                "transition={transition_persisted} {field}: {}",
                condition.message
            );
        }
    }
}
