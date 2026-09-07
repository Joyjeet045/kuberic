use std::collections::BTreeSet;

#[derive(Debug, PartialEq, Clone)]
pub struct PlacementCandidate {
    pub replica_id: i64,
    pub pod_name: String,
    pub node_name: Option<String>,
}

pub fn switchover_target_for_maintenance(
    candidates: &[PlacementCandidate],
    current_primary: Option<&str>,
    maintenance_nodes: &BTreeSet<String>,
) -> Option<String> {
    if maintenance_nodes.is_empty() {
        return None;
    }

    let primary = candidates
        .iter()
        .find(|candidate| Some(candidate.pod_name.as_str()) == current_primary)?;
    if !is_under_maintenance(primary, maintenance_nodes) {
        return None;
    }

    candidates
        .iter()
        .filter(|candidate| candidate.pod_name != primary.pod_name)
        .filter(|candidate| candidate.node_name.is_some())
        .filter(|candidate| !is_under_maintenance(candidate, maintenance_nodes))
        .min_by_key(|candidate| candidate.replica_id)
        .map(|candidate| candidate.pod_name.clone())
}

fn is_under_maintenance(
    candidate: &PlacementCandidate,
    maintenance_nodes: &BTreeSet<String>,
) -> bool {
    candidate
        .node_name
        .as_deref()
        .is_some_and(|node| maintenance_nodes.contains(node))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(id: i64, node: Option<&str>) -> PlacementCandidate {
        PlacementCandidate {
            replica_id: id,
            pod_name: format!("kv-{}", id - 1),
            node_name: node.map(str::to_string),
        }
    }

    fn nodes(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn no_maintenance_leaves_placement_untouched() {
        let candidates = [
            candidate(1, Some("worker-04")),
            candidate(2, Some("worker-05")),
        ];
        assert_eq!(
            switchover_target_for_maintenance(&candidates, Some("kv-0"), &nodes(&[])),
            None
        );
    }

    #[test]
    fn a_primary_off_the_maintenance_node_is_not_moved() {
        let candidates = [
            candidate(1, Some("worker-05")),
            candidate(2, Some("worker-04")),
        ];
        assert_eq!(
            switchover_target_for_maintenance(&candidates, Some("kv-0"), &nodes(&["worker-04"])),
            None
        );
    }

    #[test]
    fn a_primary_on_the_maintenance_node_moves_to_the_lowest_eligible_replica() {
        let candidates = [
            candidate(1, Some("worker-04")),
            candidate(3, Some("worker-06")),
            candidate(2, Some("worker-05")),
        ];
        assert_eq!(
            switchover_target_for_maintenance(&candidates, Some("kv-0"), &nodes(&["worker-04"])),
            Some("kv-1".to_string())
        );
    }

    #[test]
    fn a_replica_on_another_maintenance_node_is_not_a_target() {
        let candidates = [
            candidate(1, Some("worker-04")),
            candidate(2, Some("worker-05")),
            candidate(3, Some("worker-06")),
        ];
        assert_eq!(
            switchover_target_for_maintenance(
                &candidates,
                Some("kv-0"),
                &nodes(&["worker-04", "worker-05"])
            ),
            Some("kv-2".to_string())
        );
    }

    #[test]
    fn an_unscheduled_replica_is_not_a_target() {
        let candidates = [candidate(1, Some("worker-04")), candidate(2, None)];
        assert_eq!(
            switchover_target_for_maintenance(&candidates, Some("kv-0"), &nodes(&["worker-04"])),
            None
        );
    }

    #[test]
    fn every_replica_on_maintenance_nodes_yields_no_target() {
        let candidates = [
            candidate(1, Some("worker-04")),
            candidate(2, Some("worker-04")),
        ];
        assert_eq!(
            switchover_target_for_maintenance(&candidates, Some("kv-0"), &nodes(&["worker-04"])),
            None
        );
    }

    #[test]
    fn an_unknown_primary_yields_no_target() {
        let candidates = [candidate(1, Some("worker-04"))];
        assert_eq!(
            switchover_target_for_maintenance(&candidates, Some("kv-9"), &nodes(&["worker-04"])),
            None
        );
        assert_eq!(
            switchover_target_for_maintenance(&candidates, None, &nodes(&["worker-04"])),
            None
        );
    }
}
