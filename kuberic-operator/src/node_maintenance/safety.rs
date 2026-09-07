use std::collections::BTreeSet;

use k8s_openapi::jiff::Timestamp;

use super::api::{
    AffectedKubericSetStatus, MaintenanceBlockedReason, MaintenancePhase,
    NodeMaintenanceRequestStatus,
};
use super::discovery::finish;

pub fn reconcile_preparation(
    mut status: NodeMaintenanceRequestStatus,
    placements: &[SetPlacement],
    now: Timestamp,
) -> NodeMaintenanceRequestStatus {
    let mut readiness = Vec::with_capacity(status.affected_sets.len());
    for (set, placement) in status.affected_sets.iter_mut().zip(placements) {
        let evaluation = evaluate_set(set, placement);
        set.primary_moved = evaluation.primary_moved;
        set.quorum_without_node = evaluation.quorum_without_node;
        set.no_eligible_target = evaluation.readiness == SetReadiness::NoEligibleTarget;
        readiness.push(evaluation.readiness);
    }

    let outcome = evaluate_preparation(&status.affected_sets, &readiness);
    if !status.phase.can_transition_to(outcome.phase)
        && status.phase.can_transition_to(MaintenancePhase::Preparing)
    {
        status.phase = MaintenancePhase::Preparing;
    }
    let mut status = finish(
        status,
        outcome.phase,
        outcome.reason,
        Some(outcome.message),
        now,
    );
    if status.phase == MaintenancePhase::Prepared && status.prepared_at.is_none() {
        status.prepared_at = Some(now.to_string());
    }
    status
}

#[derive(Debug, PartialEq, Clone, Default)]
pub struct SetPlacement {
    pub topology: Option<SetTopology>,
    pub promotable_pod_uids: BTreeSet<String>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SetTopology {
    pub primary_pod_uid: Option<String>,
    pub member_pod_uids: Vec<String>,
    pub write_quorum: u32,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SetReadiness {
    Ready,
    TopologyUnknown,
    AwaitingPrimaryMove,
    NoEligibleTarget,
    QuorumAtRisk,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SetEvaluation {
    pub readiness: SetReadiness,
    pub primary_moved: bool,
    pub quorum_without_node: bool,
}

pub fn evaluate_set(
    affected: &AffectedKubericSetStatus,
    placement: &SetPlacement,
) -> SetEvaluation {
    let Some(topology) = placement.topology.as_ref() else {
        return SetEvaluation {
            readiness: SetReadiness::TopologyUnknown,
            primary_moved: false,
            quorum_without_node: false,
        };
    };

    let on_node: BTreeSet<&str> = affected
        .replicas
        .iter()
        .map(|replica| replica.pod_uid.as_str())
        .collect();

    let surviving = topology
        .member_pod_uids
        .iter()
        .filter(|uid| !on_node.contains(uid.as_str()))
        .count();

    let promotable = topology
        .member_pod_uids
        .iter()
        .filter(|uid| !on_node.contains(uid.as_str()))
        .filter(|uid| placement.promotable_pod_uids.contains(uid.as_str()))
        .count();

    let quorum_without_node = surviving >= topology.write_quorum as usize;
    let primary_moved = topology
        .primary_pod_uid
        .as_deref()
        .is_some_and(|primary| !on_node.contains(primary));

    let readiness = if !quorum_without_node {
        SetReadiness::QuorumAtRisk
    } else if primary_moved {
        SetReadiness::Ready
    } else if promotable == 0 {
        SetReadiness::NoEligibleTarget
    } else {
        SetReadiness::AwaitingPrimaryMove
    };

    SetEvaluation {
        readiness,
        primary_moved,
        quorum_without_node,
    }
}

pub struct PreparationOutcome {
    pub phase: MaintenancePhase,
    pub reason: Option<MaintenanceBlockedReason>,
    pub message: String,
}

pub fn evaluate_preparation(
    sets: &[AffectedKubericSetStatus],
    readiness: &[SetReadiness],
) -> PreparationOutcome {
    if sets.is_empty() {
        return PreparationOutcome {
            phase: MaintenancePhase::Prepared,
            reason: None,
            message: "no kuberic replicas on the node".to_string(),
        };
    }

    let mut blocking: Option<SetReadiness> = None;
    let mut blocked_set = String::new();
    for (set, readiness) in sets.iter().zip(readiness) {
        if *readiness == SetReadiness::Ready {
            continue;
        }
        if blocking.is_none_or(|current| severity(*readiness) > severity(current)) {
            blocking = Some(*readiness);
            blocked_set = format!("{}/{}", set.namespace, set.name);
        }
    }

    match blocking {
        None | Some(SetReadiness::Ready) => PreparationOutcome {
            phase: MaintenancePhase::Prepared,
            reason: None,
            message: format!(
                "{} set(s) retain write quorum with no primary on the node",
                sets.len()
            ),
        },
        Some(SetReadiness::QuorumAtRisk) => PreparationOutcome {
            phase: MaintenancePhase::Blocked,
            reason: Some(MaintenanceBlockedReason::BlockedByQuorum),
            message: format!("{blocked_set} would lose write quorum without the node"),
        },
        Some(SetReadiness::NoEligibleTarget) => PreparationOutcome {
            phase: MaintenancePhase::Blocked,
            reason: Some(MaintenanceBlockedReason::NoEligibleTarget),
            message: format!("{blocked_set} has no replica outside the node to promote"),
        },
        Some(SetReadiness::TopologyUnknown) => PreparationOutcome {
            phase: MaintenancePhase::Preparing,
            reason: None,
            message: format!("{blocked_set} has not published a committed topology yet"),
        },
        Some(SetReadiness::AwaitingPrimaryMove) => PreparationOutcome {
            phase: MaintenancePhase::Preparing,
            reason: None,
            message: format!("{blocked_set} is waiting for its primary to move off the node"),
        },
    }
}

fn severity(readiness: SetReadiness) -> u8 {
    match readiness {
        SetReadiness::Ready => 0,
        SetReadiness::TopologyUnknown => 1,
        SetReadiness::AwaitingPrimaryMove => 2,
        SetReadiness::NoEligibleTarget => 3,
        SetReadiness::QuorumAtRisk => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_maintenance::api::AffectedReplicaStatus;

    fn at(text: &str) -> Timestamp {
        text.parse().expect("timestamp")
    }

    fn replica(pod: &str, primary: bool) -> AffectedReplicaStatus {
        AffectedReplicaStatus {
            pod_name: pod.to_string(),
            pod_uid: format!("uid-{pod}"),
            is_primary: primary,
        }
    }

    fn affected(replicas: Vec<AffectedReplicaStatus>) -> AffectedKubericSetStatus {
        AffectedKubericSetStatus {
            namespace: "default".to_string(),
            name: "kv".to_string(),
            hosts_primary: replicas.iter().any(|replica| replica.is_primary),
            replicas,
            primary_moved: false,
            no_eligible_target: false,
            quorum_without_node: false,
        }
    }

    fn placement(primary: Option<&str>, members: &[&str], write_quorum: u32) -> SetPlacement {
        SetPlacement {
            topology: Some(SetTopology {
                primary_pod_uid: primary.map(|pod| format!("uid-{pod}")),
                member_pod_uids: members.iter().map(|pod| format!("uid-{pod}")).collect(),
                write_quorum,
            }),
            promotable_pod_uids: members.iter().map(|pod| format!("uid-{pod}")).collect(),
        }
    }

    fn stranded(primary: Option<&str>, members: &[&str], write_quorum: u32) -> SetPlacement {
        SetPlacement {
            promotable_pod_uids: BTreeSet::new(),
            ..placement(primary, members, write_quorum)
        }
    }

    #[test]
    fn a_secondary_only_node_is_ready_when_quorum_survives() {
        let set = affected(vec![replica("kv-2", false)]);
        let placement = placement(Some("kv-0"), &["kv-0", "kv-1", "kv-2"], 2);
        let evaluation = evaluate_set(&set, &placement);

        assert_eq!(evaluation.readiness, SetReadiness::Ready);
        assert!(evaluation.primary_moved);
        assert!(evaluation.quorum_without_node);
    }

    #[test]
    fn losing_the_node_below_write_quorum_is_blocking() {
        let set = affected(vec![replica("kv-1", false), replica("kv-2", false)]);
        let placement = placement(Some("kv-0"), &["kv-0", "kv-1", "kv-2"], 2);
        let evaluation = evaluate_set(&set, &placement);

        assert_eq!(evaluation.readiness, SetReadiness::QuorumAtRisk);
        assert!(!evaluation.quorum_without_node);
    }

    #[test]
    fn a_primary_on_the_node_waits_while_a_target_exists() {
        let set = affected(vec![replica("kv-0", true)]);
        let placement = placement(Some("kv-0"), &["kv-0", "kv-1", "kv-2"], 2);
        let evaluation = evaluate_set(&set, &placement);

        assert_eq!(evaluation.readiness, SetReadiness::AwaitingPrimaryMove);
        assert!(!evaluation.primary_moved);
        assert!(evaluation.quorum_without_node);
    }

    #[test]
    fn a_single_replica_set_has_no_target_to_promote() {
        let set = affected(vec![replica("kv-0", true)]);
        let placement = placement(Some("kv-0"), &["kv-0"], 1);
        let evaluation = evaluate_set(&set, &placement);

        assert_eq!(evaluation.readiness, SetReadiness::QuorumAtRisk);
    }

    #[test]
    fn quorum_is_assessed_before_primary_placement() {
        let set = affected(vec![replica("kv-0", true), replica("kv-1", false)]);
        let placement = placement(Some("kv-0"), &["kv-0", "kv-1", "kv-2"], 2);
        let evaluation = evaluate_set(&set, &placement);

        assert_eq!(evaluation.readiness, SetReadiness::QuorumAtRisk);
    }

    #[test]
    fn an_unpublished_topology_is_not_treated_as_safe() {
        let set = affected(vec![replica("kv-0", true)]);
        let evaluation = evaluate_set(&set, &SetPlacement::default());

        assert_eq!(evaluation.readiness, SetReadiness::TopologyUnknown);
        assert!(!evaluation.primary_moved);
        assert!(!evaluation.quorum_without_node);
    }

    #[test]
    fn a_replica_absent_from_the_committed_topology_does_not_reduce_quorum() {
        let set = affected(vec![replica("kv-9", false)]);
        let placement = placement(Some("kv-0"), &["kv-0", "kv-1", "kv-2"], 2);
        let evaluation = evaluate_set(&set, &placement);

        assert_eq!(evaluation.readiness, SetReadiness::Ready);
    }

    #[test]
    fn a_primary_with_no_schedulable_replica_elsewhere_has_no_eligible_target() {
        let set = affected(vec![replica("kv-0", true)]);
        let placement = stranded(Some("kv-0"), &["kv-0", "kv-1", "kv-2"], 2);
        let evaluation = evaluate_set(&set, &placement);

        assert_eq!(evaluation.readiness, SetReadiness::NoEligibleTarget);
        assert!(evaluation.quorum_without_node);
        assert!(!evaluation.primary_moved);
    }

    #[test]
    fn a_single_promotable_replica_is_enough_to_keep_waiting() {
        let set = affected(vec![replica("kv-0", true)]);
        let placement = SetPlacement {
            promotable_pod_uids: ["uid-kv-2".to_string()].into_iter().collect(),
            ..placement(Some("kv-0"), &["kv-0", "kv-1", "kv-2"], 2)
        };
        let evaluation = evaluate_set(&set, &placement);

        assert_eq!(evaluation.readiness, SetReadiness::AwaitingPrimaryMove);
    }

    #[test]
    fn a_promotable_replica_on_the_maintenance_node_does_not_count() {
        let set = affected(vec![replica("kv-0", true), replica("kv-1", false)]);
        let placement = SetPlacement {
            promotable_pod_uids: ["uid-kv-1".to_string()].into_iter().collect(),
            ..placement(Some("kv-0"), &["kv-0", "kv-1", "kv-2", "kv-3"], 2)
        };
        let evaluation = evaluate_set(&set, &placement);

        assert_eq!(evaluation.readiness, SetReadiness::NoEligibleTarget);
    }

    fn named(name: &str) -> AffectedKubericSetStatus {
        AffectedKubericSetStatus {
            name: name.to_string(),
            ..affected(vec![replica("kv-2", false)])
        }
    }

    #[test]
    fn a_node_without_replicas_is_prepared_immediately() {
        let outcome = evaluate_preparation(&[], &[]);

        assert_eq!(outcome.phase, MaintenancePhase::Prepared);
        assert_eq!(outcome.reason, None);
    }

    #[test]
    fn every_set_must_be_ready_before_the_request_is_prepared() {
        let sets = [named("kv"), named("sql")];
        let outcome = evaluate_preparation(&sets, &[SetReadiness::Ready, SetReadiness::Ready]);

        assert_eq!(outcome.phase, MaintenancePhase::Prepared);
    }

    #[test]
    fn one_unsafe_set_blocks_the_whole_request() {
        let sets = [named("kv"), named("sql")];
        let outcome =
            evaluate_preparation(&sets, &[SetReadiness::Ready, SetReadiness::QuorumAtRisk]);

        assert_eq!(outcome.phase, MaintenancePhase::Blocked);
        assert_eq!(
            outcome.reason,
            Some(MaintenanceBlockedReason::BlockedByQuorum)
        );
        assert!(outcome.message.contains("default/sql"));
    }

    #[test]
    fn quorum_is_reported_ahead_of_a_missing_promotion_target() {
        let sets = [named("kv"), named("sql")];
        let outcome = evaluate_preparation(
            &sets,
            &[SetReadiness::NoEligibleTarget, SetReadiness::QuorumAtRisk],
        );

        assert_eq!(
            outcome.reason,
            Some(MaintenanceBlockedReason::BlockedByQuorum)
        );
        assert!(outcome.message.contains("default/sql"));
    }

    #[test]
    fn a_set_still_moving_its_primary_keeps_the_request_preparing() {
        let sets = [named("kv")];
        let outcome = evaluate_preparation(&sets, &[SetReadiness::AwaitingPrimaryMove]);

        assert_eq!(outcome.phase, MaintenancePhase::Preparing);
        assert_eq!(outcome.reason, None);
    }

    #[test]
    fn a_set_with_no_promotion_target_is_blocked() {
        let sets = [named("kv")];
        let outcome = evaluate_preparation(&sets, &[SetReadiness::NoEligibleTarget]);

        assert_eq!(outcome.phase, MaintenancePhase::Blocked);
        assert_eq!(
            outcome.reason,
            Some(MaintenanceBlockedReason::NoEligibleTarget)
        );
    }

    #[test]
    fn an_unpublished_topology_keeps_the_request_preparing() {
        let sets = [named("kv")];
        let outcome = evaluate_preparation(&sets, &[SetReadiness::TopologyUnknown]);

        assert_eq!(outcome.phase, MaintenancePhase::Preparing);
        assert_eq!(outcome.reason, None);
    }

    fn requested(sets: Vec<AffectedKubericSetStatus>) -> NodeMaintenanceRequestStatus {
        NodeMaintenanceRequestStatus {
            affected_sets: sets,
            ..Default::default()
        }
    }

    #[test]
    fn a_prepared_request_does_not_move_with_the_clock() {
        let placements = [placement(Some("kv-0"), &["kv-0", "kv-1", "kv-2"], 2)];
        let first = reconcile_preparation(
            requested(vec![affected(vec![replica("kv-2", false)])]),
            &placements,
            at("2026-09-06T20:00:00Z"),
        );
        assert_eq!(first.phase, MaintenancePhase::Prepared);
        assert!(first.prepared_at.is_some());

        let later = reconcile_preparation(first.clone(), &placements, at("2026-09-06T23:45:00Z"));

        assert_eq!(
            first, later,
            "a prepared request must not produce a new status on every reconcile"
        );
    }

    #[test]
    fn a_blocked_request_does_not_move_with_the_clock() {
        let placements = [placement(Some("kv-0"), &["kv-0", "kv-1", "kv-2"], 2)];
        let sets = vec![affected(vec![
            replica("kv-1", false),
            replica("kv-2", false),
        ])];
        let first = reconcile_preparation(requested(sets), &placements, at("2026-09-06T20:00:00Z"));
        assert_eq!(first.phase, MaintenancePhase::Blocked);

        let later = reconcile_preparation(first.clone(), &placements, at("2026-09-06T23:45:00Z"));
        assert_eq!(first, later);
    }

    #[test]
    fn readiness_lost_again_retracts_the_prepared_condition() {
        let safe = [placement(Some("kv-0"), &["kv-0", "kv-1", "kv-2"], 2)];
        let prepared = reconcile_preparation(
            requested(vec![affected(vec![replica("kv-2", false)])]),
            &safe,
            at("2026-09-06T20:00:00Z"),
        );
        assert_eq!(prepared.phase, MaintenancePhase::Prepared);

        let regressed = [placement(Some("kv-2"), &["kv-0", "kv-1", "kv-2"], 2)];
        let later = reconcile_preparation(prepared, &regressed, at("2026-09-06T21:00:00Z"));

        assert_ne!(later.phase, MaintenancePhase::Prepared);
        assert!(later.prepared_at.is_none());
        let condition = later.conditions.first().expect("condition");
        assert_eq!(condition.status, "False");
        assert_eq!(condition.last_transition_time, "2026-09-06T21:00:00Z");
    }

    #[test]
    fn a_blocked_request_can_still_reach_prepared_once_it_is_safe() {
        let unsafe_placement = [placement(Some("kv-0"), &["kv-0", "kv-1", "kv-2"], 2)];
        let blocked = reconcile_preparation(
            requested(vec![affected(vec![
                replica("kv-1", false),
                replica("kv-2", false),
            ])]),
            &unsafe_placement,
            at("2026-09-06T20:00:00Z"),
        );
        assert_eq!(blocked.phase, MaintenancePhase::Blocked);

        let recovered = NodeMaintenanceRequestStatus {
            affected_sets: vec![affected(vec![replica("kv-2", false)])],
            ..blocked
        };
        let later = reconcile_preparation(recovered, &unsafe_placement, at("2026-09-06T21:00:00Z"));

        assert_eq!(later.phase, MaintenancePhase::Prepared);
        assert_eq!(later.blocked_reason, None);
    }
}
