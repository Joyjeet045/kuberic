//! Replica scheduling constraints. Primary placement is a separate concern.

use std::collections::{BTreeMap, BTreeSet};

use k8s_openapi::{
    api::core::v1::{
        Affinity, Node, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm, Pod,
        PodAffinityTerm, PodSpec, Toleration, TopologySpreadConstraint, WeightedPodAffinityTerm,
    },
    apimachinery::pkg::apis::meta::v1::LabelSelector,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const HOSTNAME_TOPOLOGY_KEY: &str = "kubernetes.io/hostname";
pub const SET_LABEL: &str = "kuberic.io/set";

/// Native Kubernetes scheduling rules plus anti-affinity between this set's replicas.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchedulingPolicy {
    /// Preferred is best effort; Required permits at most one replica per labeled domain.
    #[serde(default)]
    pub mode: ReplicaAntiAffinityMode,
    /// Node label identifying the failure domain. Required mode excludes unlabeled nodes.
    #[serde(default = "default_topology_key")]
    #[schemars(length(min = 1, max = 317))]
    #[schemars(regex(
        pattern = r"^([a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?(\.[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*/)?[A-Za-z0-9]([-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?$"
    ))]
    pub topology_key: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub node_selector: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity: Option<Affinity>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tolerations: Vec<Toleration>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub topology_spread_constraints: Vec<TopologySpreadConstraint>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ReplicaAntiAffinityMode {
    #[default]
    Preferred,
    Required,
}

impl Default for SchedulingPolicy {
    fn default() -> Self {
        Self {
            mode: ReplicaAntiAffinityMode::Preferred,
            topology_key: default_topology_key(),
            node_selector: BTreeMap::new(),
            affinity: None,
            tolerations: Vec::new(),
            topology_spread_constraints: Vec::new(),
        }
    }
}

fn default_topology_key() -> String {
    HOSTNAME_TOPOLOGY_KEY.to_owned()
}

fn valid_label_key(key: &str) -> bool {
    let (prefix, name) = key
        .split_once('/')
        .map_or((None, key), |(prefix, name)| (Some(prefix), name));
    let valid_name = !name.is_empty()
        && name.len() <= 63
        && name.starts_with(|c: char| c.is_ascii_alphanumeric())
        && name.ends_with(|c: char| c.is_ascii_alphanumeric())
        && name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c));
    valid_name
        && prefix.is_none_or(|prefix| {
            !prefix.is_empty()
                && prefix.len() <= 253
                && prefix.split('.').all(|label| {
                    !label.is_empty()
                        && label.len() <= 63
                        && label.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
                        && label.ends_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
                        && label
                            .bytes()
                            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
                })
        })
}

fn valid_label_value(value: &str) -> bool {
    value.is_empty()
        || (value.len() <= 63
            && value.starts_with(|c: char| c.is_ascii_alphanumeric())
            && value.ends_with(|c: char| c.is_ascii_alphanumeric())
            && value
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c)))
}

impl SchedulingPolicy {
    /// Checks common native-field errors before Pod admission. Kubernetes remains the
    /// authority for version-specific affinity, toleration and spread validation.
    pub fn validate(&self) -> Result<(), String> {
        if !valid_label_key(&self.topology_key) {
            return Err("scheduling.topologyKey must be a valid Kubernetes label key".into());
        }
        if self
            .node_selector
            .iter()
            .any(|(key, value)| !valid_label_key(key) || !valid_label_value(value))
        {
            return Err("scheduling.nodeSelector contains an invalid label key or value".into());
        }
        for constraint in &self.topology_spread_constraints {
            if !valid_label_key(&constraint.topology_key)
                || constraint.max_skew < 1
                || !matches!(
                    constraint.when_unsatisfiable.as_str(),
                    "DoNotSchedule" | "ScheduleAnyway"
                )
                || constraint.min_domains.is_some_and(|domains| {
                    domains < 1 || constraint.when_unsatisfiable != "DoNotSchedule"
                })
            {
                return Err("invalid scheduling.topologySpreadConstraints entry".into());
            }
        }
        for toleration in &self.tolerations {
            let key = toleration.key.as_deref().unwrap_or_default();
            let operator = toleration.operator.as_deref().unwrap_or("Equal");
            let effect = toleration.effect.as_deref().unwrap_or_default();
            if (!key.is_empty() && !valid_label_key(key))
                || (!matches!(operator, "Equal" | "Exists"))
                || (key.is_empty() && operator != "Exists")
                || (operator == "Exists"
                    && toleration.value.as_deref().is_some_and(|v| !v.is_empty()))
                || (operator == "Equal"
                    && !valid_label_value(toleration.value.as_deref().unwrap_or_default()))
                || !matches!(effect, "" | "NoSchedule" | "PreferNoSchedule" | "NoExecute")
                || (toleration.toleration_seconds.is_some() && effect != "NoExecute")
            {
                return Err("invalid scheduling.tolerations entry".into());
            }
        }
        Ok(())
    }
}

/// Produces just the scheduling fields, for use as the tail of a PodSpec literal.
/// User rules are cloned verbatim; the generated anti-affinity rule is appended.
pub fn scheduling_pod_spec(
    set_name: &str,
    namespace: &str,
    policy: Option<&SchedulingPolicy>,
) -> PodSpec {
    let default_policy = SchedulingPolicy::default();
    let policy = policy.unwrap_or(&default_policy);
    let mut affinity = policy.affinity.clone().unwrap_or_default();
    let term = PodAffinityTerm {
        label_selector: Some(LabelSelector {
            match_labels: Some(BTreeMap::from([(SET_LABEL.into(), set_name.into())])),
            ..Default::default()
        }),
        namespaces: Some(vec![namespace.into()]),
        topology_key: policy.topology_key.clone(),
        ..Default::default()
    };
    let anti_affinity = affinity.pod_anti_affinity.get_or_insert_default();
    match policy.mode {
        ReplicaAntiAffinityMode::Preferred => {
            anti_affinity
                .preferred_during_scheduling_ignored_during_execution
                .get_or_insert_default()
                .push(WeightedPodAffinityTerm {
                    weight: 100,
                    pod_affinity_term: term,
                });
        }
        ReplicaAntiAffinityMode::Required => {
            anti_affinity
                .required_during_scheduling_ignored_during_execution
                .get_or_insert_default()
                .push(term);
            require_topology_label(&mut affinity, &policy.topology_key);
        }
    }
    PodSpec {
        affinity: Some(affinity),
        node_selector: (!policy.node_selector.is_empty()).then(|| policy.node_selector.clone()),
        tolerations: (!policy.tolerations.is_empty()).then(|| policy.tolerations.clone()),
        topology_spread_constraints: (!policy.topology_spread_constraints.is_empty())
            .then(|| policy.topology_spread_constraints.clone()),
        ..Default::default()
    }
}

fn require_topology_label(affinity: &mut Affinity, topology_key: &str) {
    let requirement = NodeSelectorRequirement {
        key: topology_key.into(),
        operator: "Exists".into(),
        values: None,
    };
    let selector = &mut affinity
        .node_affinity
        .get_or_insert_default()
        .required_during_scheduling_ignored_during_execution;
    match selector {
        None => {
            *selector = Some(NodeSelector {
                node_selector_terms: vec![NodeSelectorTerm {
                    match_expressions: Some(vec![requirement]),
                    ..Default::default()
                }],
            });
        }
        Some(selector) => {
            // Terms are ORed, expressions are ANDed. Empty terms match no nodes;
            // adding Exists to one would incorrectly broaden the user's rule.
            for term in &mut selector.node_selector_terms {
                if term
                    .match_expressions
                    .as_ref()
                    .is_some_and(|v| !v.is_empty())
                    || term.match_fields.as_ref().is_some_and(|v| !v.is_empty())
                {
                    term.match_expressions
                        .get_or_insert_default()
                        .push(requirement.clone());
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulingDiagnosis {
    pub reason: String,
    pub message: String,
}

/// Audits current bindings, including label drift ignored by Kubernetes after scheduling.
/// Only live, bound Pods from this set and namespace participate. Incomplete node
/// inventory is reported separately; it is never interpreted as a policy violation.
pub fn diagnose_required_topology(
    set_name: &str,
    namespace: &str,
    policy: Option<&SchedulingPolicy>,
    pods: &[Pod],
    nodes: &[Node],
) -> Option<SchedulingDiagnosis> {
    let policy = policy.filter(|policy| policy.mode == ReplicaAntiAffinityMode::Required)?;
    let nodes_by_name: BTreeMap<_, _> = nodes
        .iter()
        .filter_map(|node| node.metadata.name.as_deref().map(|name| (name, node)))
        .collect();
    let mut domains: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    let mut missing_labels = BTreeSet::new();
    let mut unknown_nodes = BTreeSet::new();
    for pod in pods {
        if pod.metadata.namespace.as_deref() != Some(namespace)
            || pod
                .metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(SET_LABEL))
                .map(String::as_str)
                != Some(set_name)
            || pod.status.as_ref().is_some_and(|status| {
                matches!(status.phase.as_deref(), Some("Succeeded" | "Failed"))
            })
        {
            continue;
        }
        let Some(node_name) = pod
            .spec
            .as_ref()
            .and_then(|spec| spec.node_name.as_deref())
            .filter(|name| !name.is_empty())
        else {
            continue;
        };
        let pod_name = pod.metadata.name.as_deref().unwrap_or("<unnamed>");
        let Some(node) = nodes_by_name.get(node_name) else {
            unknown_nodes.insert(node_name);
            continue;
        };
        match node
            .metadata
            .labels
            .as_ref()
            .and_then(|labels| labels.get(&policy.topology_key))
        {
            Some(domain) => {
                domains.entry(domain.as_str()).or_default().insert(pod_name);
            }
            None => {
                missing_labels.insert(format!("Pod {pod_name} on node {node_name}"));
            }
        }
    }
    let mut violations: Vec<String> = domains
        .into_iter()
        .filter(|(_, pods)| pods.len() > 1)
        .map(|(domain, pods)| {
            format!(
                "domain {domain:?} contains Pods {}",
                pods.into_iter().collect::<Vec<_>>().join(", ")
            )
        })
        .collect();
    if !missing_labels.is_empty() {
        violations.push(format!(
            "missing topology label on {}",
            missing_labels.into_iter().collect::<Vec<_>>().join(", ")
        ));
    }
    if !violations.is_empty() {
        return Some(SchedulingDiagnosis {
            reason: "RequiredTopologyViolation".into(),
            message: format!(
                "Required replica placement for {namespace}/{set_name} on {} is violated: {}. \
                 Kubernetes does not evict existing Pods when topology labels change",
                policy.topology_key,
                violations.join("; ")
            ),
        });
    }
    (!unknown_nodes.is_empty()).then(|| SchedulingDiagnosis {
        reason: "RequiredTopologyUnverified".into(),
        message: format!(
            "Cannot verify Required replica placement for {namespace}/{set_name} on {}: \
             bound nodes are absent from the node inventory: {}",
            policy.topology_key,
            unknown_nodes.into_iter().collect::<Vec<_>>().join(", ")
        ),
    })
}

/// Returns the scheduler's actual reason without guessing whether capacity,
/// topology, user constraints, or taints caused the failure.
pub fn diagnose_pending_pod(pod: &Pod) -> Option<SchedulingDiagnosis> {
    let status = pod.status.as_ref()?;
    if matches!(
        status.phase.as_deref(),
        Some("Running" | "Succeeded" | "Failed")
    ) {
        return None;
    }
    if pod.spec.as_ref().is_some_and(|spec| {
        spec.node_name
            .as_deref()
            .is_some_and(|name| !name.is_empty())
    }) {
        return None;
    }
    let scheduled = status
        .conditions
        .as_ref()
        .and_then(|conditions| conditions.iter().find(|c| c.type_ == "PodScheduled"));
    if scheduled.is_some_and(|condition| condition.status == "True") {
        return None;
    }
    let name = pod.metadata.name.as_deref().unwrap_or("<unnamed>");
    if let Some(condition) = scheduled
        && condition.status == "False"
    {
        return Some(SchedulingDiagnosis {
            reason: condition
                .reason
                .clone()
                .filter(|reason| !reason.is_empty())
                .unwrap_or_else(|| "Unschedulable".into()),
            message: format!(
                "Pod {name}: {}",
                condition
                    .message
                    .as_deref()
                    .filter(|message| !message.is_empty())
                    .unwrap_or("waiting for an eligible node; inspect topology labels, capacity, taints and scheduling constraints")
            ),
        });
    }
    (status.phase.as_deref() == Some("Pending")).then(|| SchedulingDiagnosis {
        reason: "SchedulingPending".into(),
        message: format!("Pod {name} is Pending and has not yet been assigned to a node"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{NodeAffinity, PodCondition, PodStatus};
    use serde_json::json;

    fn bound_pod(name: &str, set: &str, namespace: &str, node: &str) -> Pod {
        serde_json::from_value(json!({
            "metadata": {"name": name, "namespace": namespace, "labels": {SET_LABEL: set}},
            "spec": {"containers": [], "nodeName": node},
            "status": {"phase": "Running"}
        }))
        .unwrap()
    }

    fn topology_node(name: &str, zone: &str) -> Node {
        serde_json::from_value(json!({
            "metadata": {"name": name, "labels": {"topology.kubernetes.io/zone": zone}}
        }))
        .unwrap()
    }

    #[test]
    fn required_topology_diagnosis_detects_label_drift_collisions_within_only_its_set() {
        let policy = SchedulingPolicy {
            mode: ReplicaAntiAffinityMode::Required,
            topology_key: "topology.kubernetes.io/zone".into(),
            ..Default::default()
        };
        let mut nodes = vec![
            topology_node("worker-a", "zone-a"),
            topology_node("worker-b", "zone-b"),
        ];
        let pods = vec![
            bound_pod("orders-0", "orders", "production", "worker-a"),
            bound_pod("orders-1", "orders", "production", "worker-b"),
            bound_pod("other-0", "other", "production", "worker-a"),
            bound_pod("orders-0", "orders", "staging", "worker-a"),
        ];
        assert!(
            diagnose_required_topology("orders", "production", Some(&policy), &pods, &nodes)
                .is_none()
        );
        nodes[1] = topology_node("worker-b", "zone-a");
        let diagnosis =
            diagnose_required_topology("orders", "production", Some(&policy), &pods, &nodes)
                .unwrap();
        assert_eq!(diagnosis.reason, "RequiredTopologyViolation");
        assert!(diagnosis.message.contains("domain \"zone-a\""));
        assert!(diagnosis.message.contains("orders-0, orders-1"));
        assert!(diagnosis.message.contains("production/orders"));
        assert!(!diagnosis.message.contains("other-0"));
        assert!(diagnose_required_topology("orders", "production", None, &pods, &nodes).is_none());
        let preferred = SchedulingPolicy {
            mode: ReplicaAntiAffinityMode::Preferred,
            ..policy
        };
        assert!(
            diagnose_required_topology("orders", "production", Some(&preferred), &pods, &nodes)
                .is_none()
        );
    }

    #[test]
    fn required_topology_diagnosis_distinguishes_missing_labels_from_unknown_nodes() {
        let policy = SchedulingPolicy {
            mode: ReplicaAntiAffinityMode::Required,
            topology_key: "topology.kubernetes.io/zone".into(),
            ..Default::default()
        };
        let pods = vec![bound_pod("orders-0", "orders", "production", "worker-a")];
        let mut node = topology_node("worker-a", "zone-a");
        node.metadata.labels = None;
        let diagnosis =
            diagnose_required_topology("orders", "production", Some(&policy), &pods, &[node])
                .unwrap();
        assert_eq!(diagnosis.reason, "RequiredTopologyViolation");
        assert!(diagnosis.message.contains("missing topology label"));
        assert!(diagnosis.message.contains("Pod orders-0 on node worker-a"));
        let diagnosis =
            diagnose_required_topology("orders", "production", Some(&policy), &pods, &[]).unwrap();
        assert_eq!(diagnosis.reason, "RequiredTopologyUnverified");
        assert!(
            diagnosis
                .message
                .contains("absent from the node inventory: worker-a")
        );
    }

    #[test]
    fn required_topology_diagnosis_ignores_terminal_and_unbound_pods_and_duplicate_observations() {
        let policy = SchedulingPolicy {
            mode: ReplicaAntiAffinityMode::Required,
            topology_key: "topology.kubernetes.io/zone".into(),
            ..Default::default()
        };
        let live = bound_pod("orders-0", "orders", "production", "worker-a");
        let mut completed = bound_pod("orders-1", "orders", "production", "worker-a");
        completed.status.as_mut().unwrap().phase = Some("Succeeded".into());
        let mut failed = bound_pod("orders-2", "orders", "production", "worker-a");
        failed.status.as_mut().unwrap().phase = Some("Failed".into());
        let mut pending = bound_pod("orders-3", "orders", "production", "worker-a");
        pending.spec.as_mut().unwrap().node_name = None;
        pending.status.as_mut().unwrap().phase = Some("Pending".into());
        let pods = vec![live.clone(), live, completed, failed, pending];
        assert!(
            diagnose_required_topology(
                "orders",
                "production",
                Some(&policy),
                &pods,
                &[topology_node("worker-a", "zone-a")]
            )
            .is_none()
        );
    }

    #[test]
    fn default_is_soft_hostname_anti_affinity_scoped_to_one_set_and_namespace() {
        let spec = scheduling_pod_spec("orders", "production", None);
        let affinity = spec.affinity.unwrap();
        assert!(affinity.node_affinity.is_none());
        let anti = affinity.pod_anti_affinity.unwrap();
        assert!(
            anti.required_during_scheduling_ignored_during_execution
                .is_none()
        );
        let preferred = anti
            .preferred_during_scheduling_ignored_during_execution
            .unwrap();
        assert_eq!(preferred.len(), 1);
        assert_eq!(preferred[0].weight, 100);
        let term = &preferred[0].pod_affinity_term;
        assert_eq!(term.topology_key, HOSTNAME_TOPOLOGY_KEY);
        assert_eq!(term.namespaces, Some(vec!["production".into()]));
        assert!(term.namespace_selector.is_none());
        assert_eq!(
            term.label_selector.as_ref().unwrap().match_labels,
            Some(BTreeMap::from([(SET_LABEL.into(), "orders".into())]))
        );
        assert!(
            term.label_selector
                .as_ref()
                .unwrap()
                .match_expressions
                .is_none()
        );
    }

    #[test]
    fn preferred_preserves_all_native_scheduling_rules() {
        let policy: SchedulingPolicy = serde_json::from_value(json!({
            "nodeSelector": {"disk": "ssd"},
            "affinity": {
                "nodeAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": {
                        "nodeSelectorTerms": [{"matchExpressions": [
                            {"key": "disk", "operator": "In", "values": ["ssd"]}
                        ]}]
                    },
                    "preferredDuringSchedulingIgnoredDuringExecution": [{
                        "weight": 20, "preference": {"matchExpressions": [
                            {"key": "pool", "operator": "In", "values": ["fast"]}
                        ]}
                    }]
                },
                "podAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [{
                    "topologyKey": "kubernetes.io/hostname",
                    "labelSelector": {"matchLabels": {"service": "cache"}}
                }]},
                "podAntiAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": [{
                        "topologyKey": "kubernetes.io/hostname",
                        "labelSelector": {"matchLabels": {"service": "heavy"}}
                    }],
                    "preferredDuringSchedulingIgnoredDuringExecution": [{
                        "weight": 30, "podAffinityTerm": {
                            "topologyKey": "topology.kubernetes.io/zone",
                            "labelSelector": {"matchLabels": {"team": "blue"}}
                        }
                    }]
                }
            },
            "tolerations": [{"key": "dedicated", "operator": "Equal", "value": "db", "effect": "NoSchedule"}],
            "topologySpreadConstraints": [{
                "maxSkew": 1, "topologyKey": "topology.kubernetes.io/zone",
                "whenUnsatisfiable": "ScheduleAnyway",
                "labelSelector": {"matchLabels": {"team": "blue"}}
            }]
        })).unwrap();
        let original = policy.clone();
        assert!(policy.validate().is_ok());
        let spec = scheduling_pod_spec("orders", "production", Some(&policy));
        assert_eq!(policy, original);
        assert_eq!(spec.node_selector, Some(policy.node_selector.clone()));
        assert_eq!(spec.tolerations, Some(policy.tolerations.clone()));
        assert_eq!(
            spec.topology_spread_constraints,
            Some(policy.topology_spread_constraints.clone())
        );
        let actual = spec.affinity.unwrap();
        let expected = policy.affinity.unwrap();
        assert_eq!(actual.node_affinity, expected.node_affinity);
        assert_eq!(actual.pod_affinity, expected.pod_affinity);
        let actual = actual.pod_anti_affinity.unwrap();
        let expected = expected.pod_anti_affinity.unwrap();
        assert_eq!(
            actual.required_during_scheduling_ignored_during_execution,
            expected.required_during_scheduling_ignored_during_execution
        );
        let rules = actual
            .preferred_during_scheduling_ignored_during_execution
            .unwrap();
        assert_eq!(
            &rules[..1],
            expected
                .preferred_during_scheduling_ignored_during_execution
                .as_ref()
                .unwrap()
        );
        assert_eq!(rules.len(), 2);
    }

    #[test]
    fn required_zone_constraint_excludes_missing_labels_in_every_user_or_branch() {
        let policy: SchedulingPolicy = serde_json::from_value(json!({
            "mode": "Required",
            "topologyKey": "topology.kubernetes.io/zone",
            "affinity": {
                "nodeAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
                        {"matchExpressions": [{"key": "disk", "operator": "In", "values": ["ssd"]}]},
                        {"matchFields": [{"key": "metadata.name", "operator": "In", "values": ["worker-1"]}]},
                        {}
                    ]}
                },
                "podAntiAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": [{
                        "topologyKey": "rack",
                        "labelSelector": {"matchLabels": {"noisy": "true"}}
                    }],
                    "preferredDuringSchedulingIgnoredDuringExecution": [{
                        "weight": 10,
                        "podAffinityTerm": {"topologyKey": "rack"}
                    }]
                }
            }
        })).unwrap();
        let spec = scheduling_pod_spec("orders", "production", Some(&policy));
        let affinity = spec.affinity.unwrap();
        let anti = affinity.pod_anti_affinity.unwrap();
        assert_eq!(
            anti.preferred_during_scheduling_ignored_during_execution,
            policy
                .affinity
                .as_ref()
                .unwrap()
                .pod_anti_affinity
                .as_ref()
                .unwrap()
                .preferred_during_scheduling_ignored_during_execution
        );
        let terms = anti
            .required_during_scheduling_ignored_during_execution
            .unwrap();
        assert_eq!(terms.len(), 2);
        assert_eq!(terms[0].topology_key, "rack");
        assert_eq!(terms[1].topology_key, "topology.kubernetes.io/zone");
        assert_eq!(
            terms[1]
                .label_selector
                .as_ref()
                .unwrap()
                .match_labels
                .as_ref()
                .unwrap()[SET_LABEL],
            "orders"
        );
        let node_terms = affinity
            .node_affinity
            .unwrap()
            .required_during_scheduling_ignored_during_execution
            .unwrap()
            .node_selector_terms;
        for term in &node_terms[..2] {
            let exists = term.match_expressions.as_ref().unwrap().last().unwrap();
            assert_eq!(exists.key, "topology.kubernetes.io/zone");
            assert_eq!(exists.operator, "Exists");
        }
        assert_eq!(
            node_terms[0].match_expressions.as_ref().unwrap()[0].key,
            "disk"
        );
        assert_eq!(
            node_terms[1].match_fields.as_ref().unwrap()[0].key,
            "metadata.name"
        );
        assert_eq!(node_terms[2], NodeSelectorTerm::default());
    }

    #[test]
    fn required_default_never_bypasses_missing_hostname_labels_or_empty_user_selectors() {
        let mut policy = SchedulingPolicy {
            mode: ReplicaAntiAffinityMode::Required,
            ..Default::default()
        };
        let spec = scheduling_pod_spec("orders", "production", Some(&policy));
        let required = spec
            .affinity
            .unwrap()
            .node_affinity
            .unwrap()
            .required_during_scheduling_ignored_during_execution
            .unwrap();
        assert_eq!(required.node_selector_terms.len(), 1);
        assert_eq!(
            required.node_selector_terms[0]
                .match_expressions
                .as_ref()
                .unwrap()[0]
                .key,
            HOSTNAME_TOPOLOGY_KEY
        );
        policy.affinity = Some(Affinity {
            node_affinity: Some(NodeAffinity {
                required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                    node_selector_terms: vec![],
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
        let spec = scheduling_pod_spec("orders", "production", Some(&policy));
        assert!(
            spec.affinity
                .unwrap()
                .node_affinity
                .unwrap()
                .required_during_scheduling_ignored_during_execution
                .unwrap()
                .node_selector_terms
                .is_empty()
        );
    }

    #[test]
    fn diagnosis_preserves_scheduler_evidence_and_ignores_scheduled_pending_pods() {
        let mut pod = Pod {
            metadata: kube::api::ObjectMeta {
                name: Some("orders-2".into()),
                ..Default::default()
            },
            spec: Some(PodSpec::default()),
            status: Some(PodStatus {
                phase: Some("Pending".into()),
                conditions: Some(vec![PodCondition {
                    type_: "PodScheduled".into(),
                    status: "False".into(),
                    reason: Some("Unschedulable".into()),
                    message: Some(
                        "0/2 nodes are available: 2 node(s) didn't match pod anti-affinity rules."
                            .into(),
                    ),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
        };
        let diagnosis = diagnose_pending_pod(&pod).unwrap();
        assert_eq!(diagnosis.reason, "Unschedulable");
        assert!(diagnosis.message.contains("orders-2"));
        assert!(
            diagnosis
                .message
                .contains("didn't match pod anti-affinity rules")
        );
        pod.status.as_mut().unwrap().conditions = None;
        assert_eq!(
            diagnose_pending_pod(&pod).unwrap().reason,
            "SchedulingPending"
        );
        pod.spec.as_mut().unwrap().node_name = Some("worker-1".into());
        assert!(diagnose_pending_pod(&pod).is_none());
        assert!(diagnose_pending_pod(&Pod::default()).is_none());
    }

    #[test]
    fn validates_policy_fields_and_exports_schema_constraints() {
        assert_eq!(
            serde_json::from_value::<SchedulingPolicy>(json!({})).unwrap(),
            SchedulingPolicy::default()
        );
        assert!(serde_json::from_value::<SchedulingPolicy>(json!({"mode": "Disabled"})).is_err());
        for key in [
            "",
            "bad key",
            "/zone",
            "example.com/",
            "EXAMPLE.com/zone",
            "a/b/c",
        ] {
            let policy = SchedulingPolicy {
                topology_key: key.into(),
                ..Default::default()
            };
            assert!(policy.validate().is_err(), "{key}");
        }
        for key in [
            HOSTNAME_TOPOLOGY_KEY,
            "topology.kubernetes.io/zone",
            "rack",
            "example.com/Zone_1",
        ] {
            assert!(
                SchedulingPolicy {
                    topology_key: key.into(),
                    ..Default::default()
                }
                .validate()
                .is_ok(),
                "{key}"
            );
        }
        let schema = serde_json::to_value(schemars::schema_for!(SchedulingPolicy)).unwrap();
        let topology = &schema["properties"]["topologyKey"];
        assert_eq!(topology["minLength"], 1);
        assert_eq!(topology["maxLength"], 317);
        assert!(topology["pattern"].as_str().unwrap().contains("{0,61}"));
        let invalid_spread: SchedulingPolicy = serde_json::from_value(json!({
            "topologySpreadConstraints": [{"maxSkew": 0, "topologyKey": "zone", "whenUnsatisfiable": "DoNotSchedule"}]
        })).unwrap();
        assert!(invalid_spread.validate().is_err());
        let invalid_toleration: SchedulingPolicy = serde_json::from_value(json!({
            "tolerations": [{"operator": "Exists", "value": "db"}]
        }))
        .unwrap();
        assert!(invalid_toleration.validate().is_err());
    }
}
