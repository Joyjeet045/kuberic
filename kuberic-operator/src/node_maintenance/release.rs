use k8s_openapi::jiff::Timestamp;

use super::api::{
    MaintenanceBlockedReason, MaintenanceDesiredState, MaintenancePhase,
    NodeMaintenanceRequestSpec, NodeMaintenanceRequestStatus, NodeRecovery,
};
use super::discovery::{NodeRef, finish};

pub(super) fn reconcile_release(
    spec: &NodeMaintenanceRequestSpec,
    mut status: NodeMaintenanceRequestStatus,
    node: Option<&NodeRef>,
    now: Timestamp,
) -> NodeMaintenanceRequestStatus {
    let Some(node) = node else {
        if spec.release_node_uid.is_none()
            && (spec.desired_state == MaintenanceDesiredState::Cancel
                || spec.node_recovery == NodeRecovery::MayDisappear)
        {
            status.released_at = Some(now.to_string());
            return finish(
                status,
                MaintenancePhase::Released,
                None,
                Some(format!(
                    "{:?} acknowledged; the target Node no longer exists",
                    spec.desired_state
                )),
                now,
            );
        }
        return finish(
            status,
            MaintenancePhase::Releasing,
            Some(MaintenanceBlockedReason::NodeNotFound),
            Some(format!(
                "waiting for Node {} to return before release",
                spec.node_name
            )),
            now,
        );
    };
    let expected_uid = spec
        .release_node_uid
        .as_deref()
        .or(status.node_uid.as_deref());
    if expected_uid.is_some_and(|expected| expected != node.uid) {
        return finish(
            status,
            MaintenancePhase::Releasing,
            Some(MaintenanceBlockedReason::NodeIncarnationChanged),
            Some(format!(
                "Node {} now has UID {}; confirm this incarnation with spec.releaseNodeUid after recovery",
                spec.node_name, node.uid
            )),
            now,
        );
    }
    if !node.ready {
        return finish(
            status,
            MaintenancePhase::Releasing,
            Some(MaintenanceBlockedReason::NodeNotReady),
            Some(format!(
                "waiting for Node {} to be Ready before release",
                spec.node_name
            )),
            now,
        );
    }
    status.released_node_uid = Some(node.uid.clone());
    status.released_at = Some(now.to_string());
    finish(
        status,
        MaintenancePhase::Released,
        None,
        Some(format!(
            "{:?} acknowledged for Node UID {}; this request no longer excludes primary placement",
            spec.desired_state, node.uid
        )),
        now,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_maintenance::api::ReplicaRecovery;
    use serde_json::json;

    fn now() -> Timestamp {
        "2026-09-18T10:00:00Z".parse().unwrap()
    }

    fn spec(
        node_recovery: NodeRecovery,
        desired: MaintenanceDesiredState,
    ) -> NodeMaintenanceRequestSpec {
        serde_json::from_value(json!({
            "nodeName": "worker-04", "nodeRecovery": node_recovery, "desiredState": desired
        }))
        .unwrap()
    }

    fn releasing() -> NodeMaintenanceRequestStatus {
        NodeMaintenanceRequestStatus {
            phase: MaintenancePhase::Releasing,
            node_uid: Some("original-uid".to_string()),
            ..Default::default()
        }
    }

    fn node(uid: &str, ready: bool) -> NodeRef {
        NodeRef {
            name: "worker-04".to_string(),
            uid: uid.to_string(),
            ready,
        }
    }

    #[test]
    fn complete_and_cancel_release_a_ready_node_without_reusing_prepared_evidence() {
        for desired in [
            MaintenanceDesiredState::Complete,
            MaintenanceDesiredState::Cancel,
        ] {
            let outcome = reconcile_release(
                &spec(NodeRecovery::Return, desired),
                releasing(),
                Some(&node("original-uid", true)),
                now(),
            );
            assert_eq!(outcome.phase, MaintenancePhase::Released);
            assert!(!outcome.phase.excludes_primary_placement());
            assert_eq!(outcome.released_node_uid.as_deref(), Some("original-uid"));
            assert!(outcome.released_at.is_some());
            assert!(outcome.prepared_at.is_none());
            assert_eq!(outcome.conditions[0].status, "False");
        }
    }

    #[test]
    fn an_unready_node_remains_excluded() {
        let outcome = reconcile_release(
            &spec(NodeRecovery::Return, MaintenanceDesiredState::Complete),
            releasing(),
            Some(&node("original-uid", false)),
            now(),
        );
        assert_eq!(outcome.phase, MaintenancePhase::Releasing);
        assert_eq!(
            outcome.blocked_reason,
            Some(MaintenanceBlockedReason::NodeNotReady)
        );
        assert!(outcome.phase.excludes_primary_placement());
        assert!(outcome.released_at.is_none());
    }

    #[test]
    fn changed_incarnations_need_explicit_confirmation_for_every_recovery_policy() {
        for node_recovery in [NodeRecovery::Return, NodeRecovery::MayDisappear] {
            for replica_recovery in [ReplicaRecovery::Preserve, ReplicaRecovery::Rebuild] {
                let spec = NodeMaintenanceRequestSpec {
                    replica_recovery,
                    ..spec(node_recovery, MaintenanceDesiredState::Complete)
                };
                let outcome = reconcile_release(
                    &spec,
                    releasing(),
                    Some(&node("replacement-uid", true)),
                    now(),
                );
                assert_eq!(outcome.phase, MaintenancePhase::Releasing);
                assert_eq!(
                    outcome.blocked_reason,
                    Some(MaintenanceBlockedReason::NodeIncarnationChanged)
                );
                assert_eq!(outcome.node_uid.as_deref(), Some("original-uid"));
                assert!(outcome.released_at.is_none());
            }
        }
    }

    #[test]
    fn confirmed_replacement_retains_the_original_identity_for_audit() {
        let mut spec = spec(
            NodeRecovery::MayDisappear,
            MaintenanceDesiredState::Complete,
        );
        spec.release_node_uid = Some("replacement-uid".to_string());
        let outcome = reconcile_release(
            &spec,
            releasing(),
            Some(&node("replacement-uid", true)),
            now(),
        );
        assert_eq!(outcome.phase, MaintenancePhase::Released);
        assert_eq!(outcome.node_uid.as_deref(), Some("original-uid"));
        assert_eq!(
            outcome.released_node_uid.as_deref(),
            Some("replacement-uid")
        );
        let changed_again =
            reconcile_release(&spec, releasing(), Some(&node("another-uid", true)), now());
        assert_eq!(changed_again.phase, MaintenancePhase::Releasing);
    }

    #[test]
    fn missing_nodes_only_release_cancellation_or_retirement_without_a_requested_replacement() {
        for node_recovery in [NodeRecovery::Return, NodeRecovery::MayDisappear] {
            for replica_recovery in [ReplicaRecovery::Preserve, ReplicaRecovery::Rebuild] {
                for desired in [
                    MaintenanceDesiredState::Complete,
                    MaintenanceDesiredState::Cancel,
                ] {
                    let mut spec = NodeMaintenanceRequestSpec {
                        replica_recovery,
                        ..spec(node_recovery, desired)
                    };
                    let outcome = reconcile_release(&spec, releasing(), None, now());
                    assert_eq!(
                        outcome.phase == MaintenancePhase::Released,
                        desired == MaintenanceDesiredState::Cancel
                            || node_recovery == NodeRecovery::MayDisappear
                    );
                    spec.release_node_uid = Some("replacement-uid".to_string());
                    let waiting = reconcile_release(&spec, releasing(), None, now());
                    assert_eq!(
                        waiting.blocked_reason,
                        Some(MaintenanceBlockedReason::NodeNotFound)
                    );
                    assert!(waiting.phase.excludes_primary_placement());
                }
            }
        }
    }

    #[test]
    fn waiting_is_idempotent_across_clock_ticks() {
        let spec = spec(NodeRecovery::Return, MaintenanceDesiredState::Complete);
        let first = reconcile_release(
            &spec,
            releasing(),
            Some(&node("original-uid", false)),
            now(),
        );
        let next = reconcile_release(
            &spec,
            first.clone(),
            Some(&node("original-uid", false)),
            "2026-09-18T10:01:00Z".parse().unwrap(),
        );
        assert_eq!(first, next);
    }
}
