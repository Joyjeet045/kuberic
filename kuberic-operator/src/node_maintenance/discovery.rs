use std::collections::BTreeMap;

use k8s_openapi::jiff::Timestamp;

use crate::crd::StatusCondition;

use super::api::{
    AffectedKubericSetStatus, AffectedReplicaStatus, MaintenanceBlockedReason, MaintenancePhase,
    NodeMaintenanceRequestSpec, NodeMaintenanceRequestStatus, PREPARED_CONDITION_TYPE,
};

#[derive(Debug, PartialEq, Clone)]
pub struct NodeRef {
    pub name: String,
    pub uid: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct MaintenancePod {
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub node_name: Option<String>,
    pub set_name: String,
    pub is_primary: bool,
}

#[derive(Debug, Clone)]
pub struct DiscoveryInput<'a> {
    pub spec: &'a NodeMaintenanceRequestSpec,
    pub generation: Option<i64>,
    pub previous: &'a NodeMaintenanceRequestStatus,
    pub node: Option<&'a NodeRef>,
    pub pods: &'a [MaintenancePod],
    pub now: Timestamp,
}

pub fn reconcile_discovery(input: DiscoveryInput<'_>) -> NodeMaintenanceRequestStatus {
    let mut status = input.previous.clone();
    status.observed_generation = input.generation;
    status.observed_desired_state = Some(input.spec.desired_state);

    let Some(node) = input.node else {
        return finish(
            status,
            MaintenancePhase::Blocked,
            Some(MaintenanceBlockedReason::NodeNotFound),
            Some(format!("node {} not found", input.spec.node_name)),
            input.now,
        );
    };

    if let Some(known) = input.previous.node_uid.as_deref()
        && known != node.uid
    {
        return finish(
            status,
            MaintenancePhase::Blocked,
            Some(MaintenanceBlockedReason::NodeIncarnationChanged),
            Some(format!(
                "node {} was replaced: expected uid {known}, observed {}",
                input.spec.node_name, node.uid
            )),
            input.now,
        );
    }

    let node_uid = Some(node.uid.clone());
    let affected_sets = discover_affected_sets(&input.spec.node_name, input.pods);
    let rediscovered = status.node_uid != node_uid || status.affected_sets != affected_sets;
    status.node_uid = node_uid;
    status.affected_sets = affected_sets;
    if rediscovered || status.discovery_completed_at.is_none() {
        status.discovery_completed_at = Some(input.now.to_string());
    }

    let message = if status.affected_sets.is_empty() {
        format!("no kuberic replicas on node {}", input.spec.node_name)
    } else {
        format!(
            "discovered {} affected set(s), {} replica(s), {} hosting a primary",
            status.affected_sets.len(),
            status
                .affected_sets
                .iter()
                .map(|set| set.replicas.len())
                .sum::<usize>(),
            status
                .affected_sets
                .iter()
                .filter(|set| set.hosts_primary)
                .count()
        )
    };

    finish(
        status,
        MaintenancePhase::Preparing,
        None,
        Some(message),
        input.now,
    )
}

fn discover_affected_sets(
    node_name: &str,
    pods: &[MaintenancePod],
) -> Vec<AffectedKubericSetStatus> {
    let mut grouped: BTreeMap<(String, String), AffectedKubericSetStatus> = BTreeMap::new();

    for pod in pods {
        if pod.node_name.as_deref() != Some(node_name) {
            continue;
        }
        let key = (pod.namespace.clone(), pod.set_name.clone());
        let entry = grouped
            .entry(key)
            .or_insert_with(|| AffectedKubericSetStatus {
                namespace: pod.namespace.clone(),
                name: pod.set_name.clone(),
                replicas: Vec::new(),
                hosts_primary: false,
                primary_moved: false,
                quorum_without_node: false,
            });
        entry.replicas.push(AffectedReplicaStatus {
            pod_name: pod.name.clone(),
            pod_uid: pod.uid.clone(),
            is_primary: pod.is_primary,
        });
        entry.hosts_primary |= pod.is_primary;
    }

    let mut sets: Vec<AffectedKubericSetStatus> = grouped.into_values().collect();
    for set in &mut sets {
        set.replicas.sort_by(|a, b| a.pod_name.cmp(&b.pod_name));
    }
    sets
}

pub(super) fn finish(
    mut status: NodeMaintenanceRequestStatus,
    phase: MaintenancePhase,
    reason: Option<MaintenanceBlockedReason>,
    message: Option<String>,
    now: Timestamp,
) -> NodeMaintenanceRequestStatus {
    if status.phase.can_transition_to(phase) {
        status.phase = phase;
        status.blocked_reason = reason;
        status.message = message;
    }
    if status.phase != MaintenancePhase::Prepared {
        status.prepared_at = None;
    }
    set_prepared_condition(&mut status, &now.to_string());
    status
}

fn set_prepared_condition(status: &mut NodeMaintenanceRequestStatus, now: &str) {
    let prepared = status.phase.is_safe_to_drain();
    let reason = match (prepared, status.blocked_reason) {
        (true, _) => "PrimariesMovedAndQuorumVerified".to_string(),
        (false, Some(reason)) => format!("{reason:?}"),
        (false, None) => format!("{:?}", status.phase),
    };
    let condition_status = if prepared { "True" } else { "False" }.to_string();
    let existing = status
        .conditions
        .iter()
        .find(|existing| existing.type_ == PREPARED_CONDITION_TYPE);
    let last_transition_time = match existing {
        Some(existing) if existing.status == condition_status => {
            existing.last_transition_time.clone()
        }
        _ => now.to_string(),
    };
    let condition = StatusCondition {
        type_: PREPARED_CONDITION_TYPE.to_string(),
        status: condition_status,
        reason,
        message: status.message.clone().unwrap_or_default(),
        last_transition_time,
    };

    let unchanged = status.conditions.iter().any(|existing| {
        existing.type_ == condition.type_
            && existing.status == condition.status
            && existing.reason == condition.reason
            && existing.message == condition.message
    });
    if unchanged {
        return;
    }
    status
        .conditions
        .retain(|existing| existing.type_ != condition.type_);
    status.conditions.push(condition);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_maintenance::api::{MaintenanceDesiredState, MaintenanceOperation};

    const NOW: &str = "2026-09-06T20:00:00Z";

    fn at(text: &str) -> Timestamp {
        text.parse().expect("timestamp")
    }

    fn spec(node: &str) -> NodeMaintenanceRequestSpec {
        NodeMaintenanceRequestSpec {
            node_name: node.to_string(),
            operation: MaintenanceOperation::Reboot,
            desired_state: MaintenanceDesiredState::Prepare,
            provider: Some("Manual".to_string()),
            provider_event_id: Some("event-123".to_string()),
            not_before: None,
            deadline: Some("2026-09-06T21:00:00Z".to_string()),
        }
    }

    fn node(uid: &str) -> NodeRef {
        NodeRef {
            name: "worker-04".to_string(),
            uid: uid.to_string(),
        }
    }

    fn pod(name: &str, set: &str, on_node: Option<&str>, primary: bool) -> MaintenancePod {
        MaintenancePod {
            namespace: "default".to_string(),
            name: name.to_string(),
            uid: format!("uid-{name}"),
            node_name: on_node.map(str::to_string),
            set_name: set.to_string(),
            is_primary: primary,
        }
    }

    fn run(
        spec: &NodeMaintenanceRequestSpec,
        previous: &NodeMaintenanceRequestStatus,
        node: Option<&NodeRef>,
        pods: &[MaintenancePod],
    ) -> NodeMaintenanceRequestStatus {
        reconcile_discovery(DiscoveryInput {
            spec,
            generation: Some(1),
            previous,
            node,
            pods,
            now: at(NOW),
        })
    }

    #[test]
    fn discovers_and_groups_replicas_on_the_target_node() {
        let pods = [
            pod("kv-0", "kv", Some("worker-04"), false),
            pod("kv-1", "kv", Some("worker-04"), true),
            pod("kv-2", "kv", Some("worker-09"), false),
            pod("sql-0", "sql", Some("worker-04"), false),
        ];
        let status = run(
            &spec("worker-04"),
            &NodeMaintenanceRequestStatus::default(),
            Some(&node("uid-a")),
            &pods,
        );

        assert_eq!(status.phase, MaintenancePhase::Preparing);
        assert_eq!(status.node_uid.as_deref(), Some("uid-a"));
        assert_eq!(status.affected_sets.len(), 2);

        let kv = &status.affected_sets[0];
        assert_eq!(kv.name, "kv");
        assert_eq!(kv.replicas.len(), 2);
        assert!(kv.hosts_primary);

        let sql = &status.affected_sets[1];
        assert_eq!(sql.name, "sql");
        assert_eq!(sql.replicas.len(), 1);
        assert!(!sql.hosts_primary);
    }

    #[test]
    fn pods_on_other_nodes_are_ignored() {
        let pods = [pod("kv-2", "kv", Some("worker-09"), true)];
        let status = run(
            &spec("worker-04"),
            &NodeMaintenanceRequestStatus::default(),
            Some(&node("uid-a")),
            &pods,
        );
        assert!(status.affected_sets.is_empty());
        assert_eq!(status.phase, MaintenancePhase::Preparing);
    }

    #[test]
    fn unscheduled_pods_are_ignored() {
        let pods = [pod("kv-0", "kv", None, false)];
        let status = run(
            &spec("worker-04"),
            &NodeMaintenanceRequestStatus::default(),
            Some(&node("uid-a")),
            &pods,
        );
        assert!(status.affected_sets.is_empty());
    }

    #[test]
    fn discovery_never_reports_prepared() {
        let pods = [pod("kv-1", "kv", Some("worker-04"), true)];
        let status = run(
            &spec("worker-04"),
            &NodeMaintenanceRequestStatus::default(),
            Some(&node("uid-a")),
            &pods,
        );
        assert_ne!(status.phase, MaintenancePhase::Prepared);
        assert!(!status.phase.is_safe_to_drain());
        let condition = status.conditions.first().expect("condition");
        assert_eq!(condition.type_, PREPARED_CONDITION_TYPE);
        assert_eq!(condition.status, "False");
    }

    #[test]
    fn empty_affected_sets_still_do_not_report_prepared() {
        let status = run(
            &spec("worker-04"),
            &NodeMaintenanceRequestStatus::default(),
            Some(&node("uid-a")),
            &[],
        );
        assert_eq!(status.phase, MaintenancePhase::Preparing);
        assert!(status.affected_sets.is_empty());
        assert!(status.message.unwrap().contains("no kuberic replicas"));
    }

    #[test]
    fn missing_node_blocks_with_structured_reason() {
        let status = run(
            &spec("worker-04"),
            &NodeMaintenanceRequestStatus::default(),
            None,
            &[],
        );
        assert_eq!(status.phase, MaintenancePhase::Blocked);
        assert_eq!(
            status.blocked_reason,
            Some(MaintenanceBlockedReason::NodeNotFound)
        );
    }

    #[test]
    fn replaced_node_incarnation_blocks() {
        let previous = NodeMaintenanceRequestStatus {
            phase: MaintenancePhase::Preparing,
            node_uid: Some("uid-a".to_string()),
            ..Default::default()
        };

        let status = run(&spec("worker-04"), &previous, Some(&node("uid-b")), &[]);
        assert_eq!(status.phase, MaintenancePhase::Blocked);
        assert_eq!(
            status.blocked_reason,
            Some(MaintenanceBlockedReason::NodeIncarnationChanged)
        );
        assert_eq!(status.node_uid.as_deref(), Some("uid-a"));
    }

    #[test]
    fn reconciliation_is_idempotent() {
        let pods = [
            pod("kv-1", "kv", Some("worker-04"), true),
            pod("kv-0", "kv", Some("worker-04"), false),
        ];
        let spec = spec("worker-04");
        let first = run(
            &spec,
            &NodeMaintenanceRequestStatus::default(),
            Some(&node("uid-a")),
            &pods,
        );
        let second = run(&spec, &first, Some(&node("uid-a")), &pods);
        assert_eq!(first, second);
    }

    #[test]
    fn unchanged_discovery_does_not_move_with_the_clock() {
        let pods = [pod("kv-1", "kv", Some("worker-04"), true)];
        let spec = spec("worker-04");
        let first = reconcile_discovery(DiscoveryInput {
            spec: &spec,
            generation: Some(1),
            previous: &NodeMaintenanceRequestStatus::default(),
            node: Some(&node("uid-a")),
            pods: &pods,
            now: at(NOW),
        });
        let later = reconcile_discovery(DiscoveryInput {
            spec: &spec,
            generation: Some(1),
            previous: &first,
            node: Some(&node("uid-a")),
            pods: &pods,
            now: at("2026-09-06T21:30:00Z"),
        });
        assert_eq!(
            first, later,
            "an unchanged request must not produce a new status on every reconcile"
        );
    }

    #[test]
    fn rediscovery_refreshes_the_completion_timestamp() {
        let spec = spec("worker-04");
        let first = reconcile_discovery(DiscoveryInput {
            spec: &spec,
            generation: Some(1),
            previous: &NodeMaintenanceRequestStatus::default(),
            node: Some(&node("uid-a")),
            pods: &[pod("kv-0", "kv", Some("worker-04"), false)],
            now: at(NOW),
        });
        let later = reconcile_discovery(DiscoveryInput {
            spec: &spec,
            generation: Some(1),
            previous: &first,
            node: Some(&node("uid-a")),
            pods: &[
                pod("kv-0", "kv", Some("worker-04"), false),
                pod("kv-1", "kv", Some("worker-04"), true),
            ],
            now: at("2026-09-06T21:30:00Z"),
        });
        assert_ne!(first.affected_sets, later.affected_sets);
        assert_eq!(
            later.discovery_completed_at.as_deref(),
            Some("2026-09-06T21:30:00Z")
        );
    }

    #[test]
    fn condition_transition_time_survives_a_message_only_change() {
        let spec = spec("worker-04");
        let first = reconcile_discovery(DiscoveryInput {
            spec: &spec,
            generation: Some(1),
            previous: &NodeMaintenanceRequestStatus::default(),
            node: Some(&node("uid-a")),
            pods: &[pod("kv-0", "kv", Some("worker-04"), false)],
            now: at(NOW),
        });
        let first_condition = first.conditions.first().expect("condition").clone();
        assert_eq!(first_condition.status, "False");

        let later = reconcile_discovery(DiscoveryInput {
            spec: &spec,
            generation: Some(1),
            previous: &first,
            node: Some(&node("uid-a")),
            pods: &[
                pod("kv-0", "kv", Some("worker-04"), false),
                pod("kv-1", "kv", Some("worker-04"), true),
            ],
            now: at("2026-09-06T22:00:00Z"),
        });
        let later_condition = later.conditions.first().expect("condition");

        assert_ne!(first.message, later.message);
        assert_eq!(later_condition.status, "False");
        assert_eq!(
            later_condition.last_transition_time, first_condition.last_transition_time,
            "lastTransitionTime must change only when the condition status changes"
        );
    }

    #[test]
    fn restart_resumes_from_persisted_status() {
        let pods = [pod("kv-1", "kv", Some("worker-04"), true)];
        let spec = spec("worker-04");
        let persisted = run(
            &spec,
            &NodeMaintenanceRequestStatus::default(),
            Some(&node("uid-a")),
            &pods,
        );

        let resumed = run(&spec, &persisted, Some(&node("uid-a")), &pods);
        assert_eq!(resumed.node_uid, persisted.node_uid);
        assert_eq!(resumed.affected_sets, persisted.affected_sets);
        assert_eq!(resumed.phase, MaintenancePhase::Preparing);
    }

    #[test]
    fn replica_order_is_stable_regardless_of_pod_listing_order() {
        let spec = spec("worker-04");
        let forward = [
            pod("kv-0", "kv", Some("worker-04"), false),
            pod("kv-1", "kv", Some("worker-04"), true),
        ];
        let reversed = [
            pod("kv-1", "kv", Some("worker-04"), true),
            pod("kv-0", "kv", Some("worker-04"), false),
        ];
        let a = run(
            &spec,
            &NodeMaintenanceRequestStatus::default(),
            Some(&node("uid-a")),
            &forward,
        );
        let b = run(
            &spec,
            &NodeMaintenanceRequestStatus::default(),
            Some(&node("uid-a")),
            &reversed,
        );
        assert_eq!(a.affected_sets, b.affected_sets);
    }

    #[test]
    fn a_refused_transition_keeps_the_previous_explanation() {
        let previous = NodeMaintenanceRequestStatus {
            phase: MaintenancePhase::Releasing,
            blocked_reason: Some(MaintenanceBlockedReason::NodeNotFound),
            message: Some("node worker-04 not found".to_string()),
            ..Default::default()
        };
        let status = finish(
            previous,
            MaintenancePhase::Preparing,
            None,
            Some("discovered 1 affected set(s)".to_string()),
            at(NOW),
        );

        assert_eq!(status.phase, MaintenancePhase::Releasing);
        assert_eq!(
            status.blocked_reason,
            Some(MaintenanceBlockedReason::NodeNotFound)
        );
        assert_eq!(status.message.as_deref(), Some("node worker-04 not found"));
    }
}
