use std::collections::{BTreeMap, BTreeSet};

use k8s_openapi::api::core::v1::{Node, Pod};
use kube::ResourceExt;
use kuberic_core::types::{
    AccessStatus, ReplicaConfigurationMode, ReplicaConfigurationStatus, ReplicaStatusInfo, Role,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::crd::{
    DurableOperationKind, DurableOperationPhase, KubericSet, Phase, ReconfigurationPhase,
    StablePartitionSnapshotStatus, StableReplicaRoleStatus,
};

pub const HOSTNAME_TOPOLOGY_KEY: &str = "kubernetes.io/hostname";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PrimaryBalancingPolicy {
    #[serde(default)]
    pub mode: BalancingMode,
    #[serde(default = "default_topology_key")]
    #[schemars(length(min = 1, max = 253))]
    pub topology_key: String,
    #[serde(default = "default_cooldown")]
    #[schemars(range(min = 30, max = 86400))]
    pub cooldown_seconds: u32,
    #[serde(default = "default_stabilization")]
    #[schemars(range(min = 1, max = 3600))]
    pub stabilization_seconds: u32,
    #[serde(default = "default_improvement")]
    #[schemars(range(min = 1))]
    pub minimum_improvement: u32,
}

impl Default for PrimaryBalancingPolicy {
    fn default() -> Self {
        Self {
            mode: BalancingMode::default(),
            topology_key: default_topology_key(),
            cooldown_seconds: default_cooldown(),
            stabilization_seconds: default_stabilization(),
            minimum_improvement: default_improvement(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum BalancingMode {
    #[default]
    TieBreakOnly,
    Automatic,
}

fn default_topology_key() -> String {
    HOSTNAME_TOPOLOGY_KEY.to_string()
}
fn default_cooldown() -> u32 {
    300
}
fn default_stabilization() -> u32 {
    60
}
fn default_improvement() -> u32 {
    1
}

impl PrimaryBalancingPolicy {
    pub fn validate(&self) -> Result<(), String> {
        if self.topology_key.trim().is_empty()
            || self.topology_key.len() > 253
            || !(30..=86400).contains(&self.cooldown_seconds)
            || !(1..=3600).contains(&self.stabilization_seconds)
            || self.minimum_improvement == 0
        {
            return Err("invalid primaryBalancing policy bounds".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlacementStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_domain: Option<String>,
    #[serde(default)]
    pub replica_domains: BTreeMap<String, u32>,
    #[serde(default)]
    pub missing_topology_replicas: u32,
    #[serde(default)]
    pub unschedulable_replicas: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduling_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduling_message: Option<String>,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_pod: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_instance_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_domain: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub improvement: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_since: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_epoch: Option<crate::crd::EpochStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_primary_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_rebalance_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct PlacementInventory {
    pub nodes: Vec<Node>,
    pub sets: Vec<KubericSet>,
    pub pods: Vec<Pod>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaPlacement {
    pub id: i64,
    pub instance_id: String,
    pub pod_name: String,
    pub node: String,
    pub domain: String,
    pub domain_primaries: u32,
    pub node_primaries: u32,
    pub eligible: bool,
}

pub fn node_eligible(node: &Node, maintenance: &BTreeSet<String>) -> bool {
    node.metadata.deletion_timestamp.is_none()
        && !maintenance.contains(&node.name_any())
        && !node
            .spec
            .as_ref()
            .is_some_and(|spec| spec.unschedulable == Some(true))
        && node.status.as_ref().is_some_and(|status| {
            status
                .conditions
                .iter()
                .flatten()
                .any(|condition| condition.type_ == "Ready" && condition.status == "True")
        })
}

fn pod_node(pod: &Pod) -> Option<&str> {
    pod.spec.as_ref()?.node_name.as_deref()
}

fn domain<'a>(node: &'a Node, key: &str) -> Option<&'a str> {
    node.metadata
        .labels
        .as_ref()?
        .get(key)
        .filter(|value| !value.is_empty())
        .map(String::as_str)
}

fn active_operation(set: &KubericSet) -> bool {
    set.status
        .as_ref()
        .and_then(|status| status.operation.as_ref())
        .is_some_and(|operation| {
            !matches!(
                operation.phase,
                DurableOperationPhase::Completed
                    | DurableOperationPhase::Failed
                    | DurableOperationPhase::Poisoned
            )
        })
}

pub fn replica_placements(
    set: &KubericSet,
    pods: &[Pod],
    inventory: &PlacementInventory,
    maintenance: &BTreeSet<String>,
    topology_key: &str,
) -> Result<Vec<ReplicaPlacement>, String> {
    let nodes: BTreeMap<_, _> = inventory
        .nodes
        .iter()
        .map(|node| (node.name_any(), node))
        .collect();
    let mut node_counts = BTreeMap::<String, u32>::new();
    let mut domain_counts = BTreeMap::<String, u32>::new();
    for other in &inventory.sets {
        let Some(status) = &other.status else {
            continue;
        };
        let primary = if active_operation(other) {
            status.operation.as_ref().and_then(|operation| {
                // A durable target is a reservation, so concurrent plans do not all choose it.
                operation
                    .target_snapshot
                    .members
                    .iter()
                    .find(|member| member.id == operation.target_primary_id)
                    .map(|member| member.instance_id.as_str())
            })
        } else {
            status.stable_snapshot.as_ref().and_then(|snapshot| {
                snapshot
                    .members
                    .iter()
                    .find(|member| member.id == snapshot.primary_id)
                    .map(|member| member.instance_id.as_str())
            })
        };
        let Some(primary) = primary else { continue };
        let Some(pod) = inventory.pods.iter().find(|pod| {
            pod.metadata.uid.as_deref() == Some(primary)
                && pod.namespace() == other.namespace()
                && pod
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|labels| labels.get("kuberic.io/set"))
                    == Some(&other.name_any())
        }) else {
            return Err(format!(
                "primary placement is unavailable for {}/{}",
                other.namespace().unwrap_or_default(),
                other.name_any()
            ));
        };
        let Some(node_name) = pod_node(pod) else {
            return Err("primary Pod is not scheduled".to_string());
        };
        let Some(node) = nodes.get(node_name) else {
            return Err(format!("primary Node {node_name} is absent from inventory"));
        };
        let Some(value) = domain(node, topology_key) else {
            return Err(format!("primary Node {node_name} lacks {topology_key}"));
        };
        *node_counts.entry(node_name.to_string()).or_default() += 1;
        *domain_counts.entry(value.to_string()).or_default() += 1;
    }
    let mut placements = Vec::new();
    for pod in pods {
        let id = pod
            .metadata
            .labels
            .as_ref()
            .and_then(|labels| labels.get("kuberic.io/pod-index"))
            .and_then(|index| index.parse::<i64>().ok())
            .and_then(|index| index.checked_add(1))
            .filter(|id| *id > 0)
            .ok_or_else(|| format!("Pod {} lacks a valid replica ID", pod.name_any()))?;
        let instance_id = pod
            .metadata
            .uid
            .clone()
            .filter(|uid| !uid.is_empty())
            .ok_or_else(|| format!("Pod {} lacks an incarnation UID", pod.name_any()))?;
        let node_name =
            pod_node(pod).ok_or_else(|| format!("Pod {} is unscheduled", pod.name_any()))?;
        let node = nodes
            .get(node_name)
            .ok_or_else(|| format!("Node {node_name} is unavailable"))?;
        let value = domain(node, topology_key)
            .ok_or_else(|| format!("Node {node_name} lacks {topology_key}"))?;
        placements.push(ReplicaPlacement {
            id,
            instance_id,
            pod_name: pod.name_any(),
            node: node_name.to_string(),
            domain: value.to_string(),
            domain_primaries: *domain_counts.get(value).unwrap_or(&0),
            node_primaries: *node_counts.get(node_name).unwrap_or(&0),
            eligible: node_eligible(node, maintenance) && pod.metadata.deletion_timestamp.is_none(),
        });
    }
    if placements.is_empty() && set.spec.replicas > 0 {
        return Err("no scheduled replicas are available".to_string());
    }
    placements.sort_by_key(|candidate| candidate.id);
    Ok(placements)
}

pub fn initial_primary(placements: &[ReplicaPlacement]) -> Option<i64> {
    placements
        .iter()
        .filter(|candidate| candidate.eligible)
        .min_by_key(|candidate| {
            (
                candidate.domain_primaries,
                candidate.node_primaries,
                candidate.id,
            )
        })
        .map(|candidate| candidate.id)
}

pub fn distribution(
    set: &KubericSet,
    pods: &[Pod],
    nodes: &[Node],
    topology_key: &str,
) -> PlacementStatus {
    let mut result = set
        .status
        .as_ref()
        .and_then(|status| status.placement.clone())
        .unwrap_or_default();
    result.primary_node = None;
    result.primary_domain = None;
    result.replica_domains.clear();
    result.missing_topology_replicas = 0;
    result.unschedulable_replicas = 0;
    let diagnosis = crate::scheduling::diagnose_required_topology(
        &set.name_any(),
        &set.namespace().unwrap_or_default(),
        set.spec.scheduling.as_ref(),
        pods,
        nodes,
    )
    .or_else(|| {
        pods.iter()
            .filter_map(|pod| {
                crate::scheduling::diagnose_pending_pod(pod)
                    .map(|diagnosis| (pod.name_any(), diagnosis))
            })
            .min_by(|(left_name, left), (right_name, right)| {
                (left.reason == "SchedulingPending", left_name)
                    .cmp(&(right.reason == "SchedulingPending", right_name))
            })
            .map(|(_, diagnosis)| diagnosis)
    });
    result.scheduling_reason = diagnosis.as_ref().map(|diagnosis| diagnosis.reason.clone());
    result.scheduling_message = diagnosis.map(|diagnosis| diagnosis.message);
    let replica_topology_key = set
        .spec
        .scheduling
        .as_ref()
        .map(|policy| policy.topology_key.as_str())
        .unwrap_or(HOSTNAME_TOPOLOGY_KEY);
    let primary_instance = set
        .status
        .as_ref()
        .and_then(|status| status.stable_snapshot.as_ref())
        .and_then(|snapshot| {
            snapshot
                .members
                .iter()
                .find(|member| member.id == snapshot.primary_id)
        })
        .map(|member| member.instance_id.as_str());
    for pod in pods {
        if pod.status.as_ref().is_some_and(|status| {
            status.conditions.iter().flatten().any(|condition| {
                condition.type_ == "PodScheduled"
                    && condition.status == "False"
                    && condition.reason.as_deref() == Some("Unschedulable")
            })
        }) {
            result.unschedulable_replicas += 1;
        }
        let node_name = pod_node(pod);
        let node = node_name.and_then(|name| {
            nodes
                .iter()
                .find(|node| node.metadata.name.as_deref() == Some(name))
        });
        let value = node.and_then(|node| domain(node, replica_topology_key));
        if let Some(value) = value {
            *result.replica_domains.entry(value.to_string()).or_default() += 1;
        } else {
            result.missing_topology_replicas += 1;
        }
        if primary_instance.is_some() && primary_instance == pod.metadata.uid.as_deref() {
            result.primary_node = node_name.map(str::to_string);
            result.primary_domain = node
                .and_then(|node| domain(node, topology_key))
                .map(str::to_string);
        }
    }
    result
}

pub fn suppress(status: &mut PlacementStatus, reason: &str, message: impl Into<String>) {
    status.reason = reason.to_string();
    status.message = message.into();
    status.target_pod = None;
    status.target_instance_id = None;
    status.target_node = None;
    status.target_domain = None;
    status.improvement = None;
    status.candidate_since = None;
    status.candidate_epoch = None;
    status.candidate_primary_id = None;
}

fn configuration_matches(
    configuration: &ReplicaConfigurationStatus,
    snapshot: &StablePartitionSnapshotStatus,
    include_primary: bool,
) -> bool {
    let members: Vec<_> = snapshot
        .members
        .iter()
        .filter(|member| include_primary || member.id != snapshot.primary_id)
        .collect();
    configuration.mode == ReplicaConfigurationMode::Current
        && configuration.write_quorum == snapshot.write_quorum
        && configuration.members.len() == members.len()
        && members.iter().all(|member| {
            configuration.members.iter().any(|observed| {
                observed.id == member.id
                    && observed.instance_id.as_str() == member.instance_id
                    && observed.role
                        == if member.role == StableReplicaRoleStatus::Primary {
                            Role::Primary
                        } else {
                            Role::ActiveSecondary
                        }
            })
        })
}

fn attested(
    snapshot: &StablePartitionSnapshotStatus,
    id: i64,
    observation: &ReplicaStatusInfo,
) -> bool {
    snapshot.members.iter().any(|member| {
        member.id == id
            && member.instance_id == observation.instance_id.as_str()
            && observation.healthy
            && observation.epoch.data_loss_number == snapshot.epoch.data_loss_number
            && observation.epoch.configuration_number == snapshot.epoch.configuration_number
            && observation.role
                == if member.role == StableReplicaRoleStatus::Primary {
                    Role::Primary
                } else {
                    Role::ActiveSecondary
                }
            && observation.current_progress >= observation.committed_lsn
            && observation.committed_lsn >= 0
            && observation.deactivation_info.as_ref().is_none_or(|info| {
                (
                    info.epoch.data_loss_number,
                    info.epoch.configuration_number,
                ) <= (
                    snapshot.epoch.data_loss_number,
                    snapshot.epoch.configuration_number,
                ) && info.catch_up_lsn <= observation.current_progress
            })
            // Live replication routing excludes the primary itself; election metadata includes it.
            && (id != snapshot.primary_id || observation
                .configuration
                .as_ref()
                .is_some_and(|config| configuration_matches(config, snapshot, false)))
            && observation
                .election_configuration
                .as_ref()
                .is_some_and(|config| {
                    config.previous.is_none() && configuration_matches(&config.current, snapshot, true)
                })
    })
}

pub fn rebalance_target(
    set: &KubericSet,
    placements: &[ReplicaPlacement],
    observations: &BTreeMap<i64, ReplicaStatusInfo>,
    policy: &PrimaryBalancingPolicy,
    status: &mut PlacementStatus,
    now: i64,
) -> Option<i64> {
    let Some(current) = set.status.as_ref() else {
        suppress(
            status,
            "ConflictingOperation",
            "waiting for a committed partition",
        );
        return None;
    };
    let Some(snapshot) = current.stable_snapshot.as_ref() else {
        suppress(
            status,
            "ConflictingOperation",
            "stable topology is unavailable",
        );
        return None;
    };
    if current.phase != Phase::Healthy
        || current.reconfiguration_phase != ReconfigurationPhase::None
        || active_operation(set)
        || snapshot.members.len() != set.spec.replicas as usize
        || placements.len() != snapshot.members.len()
        || current.primary_failing_since.is_some()
        || current
            .conditions
            .iter()
            .any(|condition| condition.reason == "CommittedDegraded")
    {
        suppress(
            status,
            "ConflictingOperation",
            "topology recovery, scaling, or another operation takes precedence",
        );
        return None;
    }
    if policy.mode != BalancingMode::Automatic {
        suppress(
            status,
            "TieBreakOnly",
            "primary density is used only for safety-equivalent initial and failover choices",
        );
        return None;
    }
    if status
        .last_rebalance_at
        .is_some_and(|last| now.saturating_sub(last) < i64::from(policy.cooldown_seconds))
    {
        suppress(
            status,
            "Cooldown",
            "minimum interval since the previous rebalance has not elapsed",
        );
        return None;
    }
    let Some(primary) = observations.get(&snapshot.primary_id).filter(|primary| {
        primary.write_status == AccessStatus::Granted
            && attested(snapshot, snapshot.primary_id, primary)
    }) else {
        suppress(
            status,
            "UnsafeCandidate",
            "primary health, write quorum, or configuration cannot be attested",
        );
        return None;
    };
    // Balancing is optional: require the full configured replica set, not merely a quorum.
    if snapshot.members.iter().any(|member| {
        !placements.iter().any(|placement| {
            placement.id == member.id && placement.instance_id == member.instance_id
        }) || !observations
            .get(&member.id)
            .is_some_and(|observation| attested(snapshot, member.id, observation))
    }) {
        suppress(
            status,
            "UnsafeCandidate",
            "all committed replica incarnations must be healthy and configuration-compatible",
        );
        return None;
    }
    let Some(source) = placements
        .iter()
        .find(|candidate| candidate.id == snapshot.primary_id)
    else {
        suppress(
            status,
            "MissingTopology",
            "current primary location is unavailable",
        );
        return None;
    };
    // Secondary commit notifications can lag even when the complete primary log is replicated.
    let candidate = placements
        .iter()
        .filter(|candidate| {
            candidate.id != snapshot.primary_id
                && candidate.eligible
                && candidate.domain != source.domain
                && observations.get(&candidate.id).is_some_and(|observed| {
                    observed.instance_id.as_str() == candidate.instance_id
                        && observed.current_progress >= primary.current_progress
                        && observed
                            .catch_up_capability
                            .is_some_and(|first| first >= 0 && first <= primary.committed_lsn)
                })
        })
        .min_by_key(|candidate| {
            let observed = &observations[&candidate.id];
            (
                std::cmp::Reverse(observed.current_progress),
                std::cmp::Reverse(observed.committed_lsn),
                candidate.domain_primaries,
                candidate.node_primaries,
                candidate.id,
            )
        });
    let Some(candidate) = candidate else {
        suppress(
            status,
            "UnsafeCandidate",
            "no caught-up eligible replica in another topology domain",
        );
        return None;
    };
    let improvement =
        i64::from(source.domain_primaries) - i64::from(candidate.domain_primaries) - 1;
    if improvement < i64::from(policy.minimum_improvement) {
        suppress(
            status,
            "InsufficientImprovement",
            "moving the primary would not meet the post-move improvement threshold",
        );
        return None;
    }
    let same_candidate = status.target_pod.as_deref() == Some(&candidate.pod_name)
        && status.target_instance_id.as_deref() == Some(&candidate.instance_id)
        && status.target_node.as_deref() == Some(&candidate.node)
        && status.target_domain.as_deref() == Some(&candidate.domain)
        && status.candidate_epoch.as_ref() == Some(&snapshot.epoch)
        && status.candidate_primary_id == Some(snapshot.primary_id)
        && status.reason == "Stabilizing";
    status.target_pod = Some(candidate.pod_name.clone());
    status.target_instance_id = Some(candidate.instance_id.clone());
    status.target_node = Some(candidate.node.clone());
    status.target_domain = Some(candidate.domain.clone());
    status.improvement = Some(improvement);
    let since = if same_candidate {
        status.candidate_since.unwrap_or(now)
    } else {
        now
    };
    status.candidate_since = Some(since);
    status.candidate_epoch = Some(snapshot.epoch.clone());
    status.candidate_primary_id = Some(snapshot.primary_id);
    if now.saturating_sub(since) < i64::from(policy.stabilization_seconds) {
        status.reason = "Stabilizing".to_string();
        status.message =
            "waiting for the same safe improvement to persist through the hysteresis window"
                .to_string();
        return None;
    }
    status.reason = "Scheduled".to_string();
    status.message =
        "safe density improvement admitted through the durable switchover workflow".to_string();
    Some(candidate.id)
}

pub fn record_completion(status: &mut crate::crd::KubericSetStatus, now: i64, compensated: bool) {
    let Some(placement) = status.placement.as_mut() else {
        return;
    };
    let Some(operation) = status.operation.as_ref() else {
        return;
    };
    if operation.kind == DurableOperationKind::Switchover
        && placement.operation_id.as_deref() == Some(&operation.operation_id)
        && matches!(
            operation.phase,
            DurableOperationPhase::Completed | DurableOperationPhase::Failed
        )
    {
        placement.reason = if compensated {
            "Compensated"
        } else if operation.phase == DurableOperationPhase::Completed {
            if placement.target_pod == status.current_primary {
                "Completed"
            } else {
                "Failed"
            }
        } else {
            "Failed"
        }
        .to_string();
        placement.message = format!("durable rebalance operation {:?}", operation.phase);
        placement.last_rebalance_at = Some(now);
        placement.candidate_since = None;
        placement.candidate_epoch = None;
        placement.candidate_primary_id = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{EpochStatus, KubericSetStatus, StableReplicaSnapshotStatus};
    use kuberic_core::types::{
        AgentControlVersion, AgentGeneration, Epoch, ReplicaAgentStatus,
        ReplicaConfigurationMemberStatus, ReplicaElectionConfiguration, ReplicaInstanceId,
    };
    use serde_json::json;

    fn fixture() -> (
        KubericSet,
        Vec<ReplicaPlacement>,
        BTreeMap<i64, ReplicaStatusInfo>,
        PrimaryBalancingPolicy,
    ) {
        let mut set: KubericSet = serde_json::from_value(json!({
            "metadata": {"name": "kv", "namespace": "default", "uid": "set-uid"},
            "spec": {"image": "kv:test", "replicas": 3}
        }))
        .unwrap();
        let snapshot = StablePartitionSnapshotStatus {
            epoch: EpochStatus {
                data_loss_number: 0,
                configuration_number: 1,
            },
            primary_id: 1,
            members: (1..=3)
                .map(|id| StableReplicaSnapshotStatus {
                    id,
                    instance_id: format!("uid-{id}"),
                    role: if id == 1 {
                        StableReplicaRoleStatus::Primary
                    } else {
                        StableReplicaRoleStatus::ActiveSecondary
                    },
                    election_metadata: None,
                })
                .collect(),
            write_quorum: 2,
        };
        let configuration = ReplicaConfigurationStatus {
            mode: ReplicaConfigurationMode::Current,
            write_quorum: 2,
            members: (1..=3)
                .map(|id| ReplicaConfigurationMemberStatus {
                    id,
                    instance_id: ReplicaInstanceId::new(format!("uid-{id}")),
                    role: if id == 1 {
                        Role::Primary
                    } else {
                        Role::ActiveSecondary
                    },
                })
                .collect(),
        };
        let observations = (1..=3)
            .map(|id| {
                (
                    id,
                    ReplicaStatusInfo {
                        instance_id: ReplicaInstanceId::new(format!("uid-{id}")),
                        role: if id == 1 {
                            Role::Primary
                        } else {
                            Role::ActiveSecondary
                        },
                        epoch: Epoch::new(0, 1),
                        current_progress: 100,
                        committed_lsn: 100,
                        catch_up_capability: Some(0),
                        healthy: true,
                        write_status: if id == 1 {
                            AccessStatus::Granted
                        } else {
                            AccessStatus::NotPrimary
                        },
                        configuration: (id == 1).then(|| {
                            let mut routing = configuration.clone();
                            routing.members.retain(|member| member.id != 1);
                            routing
                        }),
                        election_configuration: Some(ReplicaElectionConfiguration {
                            previous: None,
                            current: configuration.clone(),
                        }),
                        deactivation_info: None,
                        active_replica_connections: Vec::new(),
                        build_observation: None,
                        agent: ReplicaAgentStatus {
                            protocol_version: 1,
                            lifecycle_peer_protocol_version: 1,
                            generation: AgentGeneration::parse("0123456789abcdef0123456789abcdef")
                                .unwrap(),
                            control_version: AgentControlVersion::new(0),
                            current_action: None,
                            retained_terminal_actions: Vec::new(),
                            local_faults: Vec::new(),
                        },
                    },
                )
            })
            .collect();
        let placements = (1..=3)
            .map(|id| ReplicaPlacement {
                id,
                instance_id: format!("uid-{id}"),
                pod_name: format!("kv-{}", id - 1),
                node: format!("node-{id}"),
                domain: format!("domain-{id}"),
                domain_primaries: if id == 1 { 4 } else { 0 },
                node_primaries: if id == 1 { 4 } else { 0 },
                eligible: true,
            })
            .collect();
        set.status = Some(KubericSetStatus {
            phase: Phase::Healthy,
            current_primary: Some("kv-0".to_string()),
            stable_snapshot: Some(snapshot),
            ..Default::default()
        });
        (
            set,
            placements,
            observations,
            PrimaryBalancingPolicy {
                mode: BalancingMode::Automatic,
                ..Default::default()
            },
        )
    }

    #[test]
    fn hysteresis_survives_restart_and_cooldown_prevents_duplicate_rebalance() {
        let (set, placements, observations, policy) = fixture();
        let mut status = PlacementStatus::default();
        assert_eq!(
            rebalance_target(&set, &placements, &observations, &policy, &mut status, 100),
            None
        );
        assert_eq!(status.reason, "Stabilizing");
        let mut restored = serde_json::from_value(serde_json::to_value(&status).unwrap()).unwrap();
        assert_eq!(
            rebalance_target(
                &set,
                &placements,
                &observations,
                &policy,
                &mut restored,
                159
            ),
            None
        );
        assert_eq!(restored.candidate_since, Some(100));
        assert_eq!(
            rebalance_target(
                &set,
                &placements,
                &observations,
                &policy,
                &mut restored,
                160
            ),
            Some(2)
        );
        restored.last_rebalance_at = Some(160);
        assert_eq!(
            rebalance_target(
                &set,
                &placements,
                &observations,
                &policy,
                &mut restored,
                161
            ),
            None
        );
        assert_eq!(restored.reason, "Cooldown");
    }

    #[test]
    fn target_incarnation_change_and_lost_improvement_reset_hysteresis() {
        let (set, mut placements, mut observations, policy) = fixture();
        let mut status = PlacementStatus::default();
        rebalance_target(&set, &placements, &observations, &policy, &mut status, 100);
        placements[1].eligible = false;
        rebalance_target(&set, &placements, &observations, &policy, &mut status, 150);
        assert_eq!(status.target_pod.as_deref(), Some("kv-2"));
        assert_eq!(status.candidate_since, Some(150));
        placements[0].domain_primaries = 1;
        rebalance_target(&set, &placements, &observations, &policy, &mut status, 151);
        assert_eq!(status.reason, "InsufficientImprovement");
        assert_eq!(status.candidate_since, None);
        placements[0].domain_primaries = 4;
        observations.get_mut(&3).unwrap().instance_id = ReplicaInstanceId::new("replacement");
        rebalance_target(&set, &placements, &observations, &policy, &mut status, 220);
        assert_eq!(status.reason, "UnsafeCandidate");
    }

    #[test]
    fn topology_commit_restarts_hysteresis_even_for_the_same_target() {
        let (mut set, placements, mut observations, policy) = fixture();
        let mut status = PlacementStatus::default();
        assert_eq!(
            rebalance_target(&set, &placements, &observations, &policy, &mut status, 100),
            None
        );
        set.status
            .as_mut()
            .unwrap()
            .stable_snapshot
            .as_mut()
            .unwrap()
            .epoch
            .configuration_number += 1;
        for observation in observations.values_mut() {
            observation.epoch.configuration_number += 1;
        }
        assert_eq!(
            rebalance_target(&set, &placements, &observations, &policy, &mut status, 200),
            None
        );
        assert_eq!(status.reason, "Stabilizing");
        assert_eq!(status.candidate_since, Some(200));
    }

    #[test]
    fn durable_compensation_records_its_outcome_and_cooldown() {
        let (mut set, _, _, _) = fixture();
        let status = set.status.as_mut().unwrap();
        let mut operation = crate::durable::start_switchover(
            "set-uid",
            status.stable_snapshot.clone().unwrap(),
            2,
            100,
        )
        .unwrap();
        operation.phase = DurableOperationPhase::Failed;
        status.placement = Some(PlacementStatus {
            operation_id: Some(operation.operation_id.clone()),
            target_pod: Some("kv-1".to_string()),
            candidate_since: Some(100),
            ..Default::default()
        });
        status.operation = Some(operation);
        record_completion(status, 150, true);
        let placement = status.placement.as_ref().unwrap();
        assert_eq!(placement.reason, "Compensated");
        assert_eq!(placement.last_rebalance_at, Some(150));
        assert_eq!(placement.candidate_since, None);
        record_completion(status, 151, false);
        assert_eq!(status.placement.as_ref().unwrap().reason, "Failed");
    }

    #[test]
    fn replicated_prefix_is_safe_while_secondary_commit_notification_lags() {
        let (set, placements, mut observations, policy) = fixture();
        observations.get_mut(&2).unwrap().committed_lsn = 99;
        observations.get_mut(&3).unwrap().committed_lsn = 99;
        let mut status = PlacementStatus::default();
        assert_eq!(
            rebalance_target(&set, &placements, &observations, &policy, &mut status, 100),
            None
        );
        assert_eq!(status.reason, "Stabilizing");
        assert_eq!(
            rebalance_target(&set, &placements, &observations, &policy, &mut status, 200),
            Some(2)
        );
    }

    #[test]
    fn stale_or_unhealthy_replica_never_wins_by_density() {
        let (set, placements, observations, policy) = fixture();
        for defect in 0..11 {
            let mut observations = observations.clone();
            let target = observations.get_mut(&2).unwrap();
            match defect {
                0 => target.healthy = false,
                1 => target.epoch.configuration_number += 1,
                2 => target.role = Role::IdleSecondary,
                3 => {
                    target
                        .election_configuration
                        .as_mut()
                        .unwrap()
                        .current
                        .write_quorum = 1
                }
                4 => {
                    target.election_configuration.as_mut().unwrap().previous = Some(
                        target
                            .election_configuration
                            .as_ref()
                            .unwrap()
                            .current
                            .clone(),
                    )
                }
                5 => target.instance_id = ReplicaInstanceId::new("stale"),
                6 => target.current_progress = 99,
                7 => target.catch_up_capability = None,
                8 => target.catch_up_capability = Some(-1),
                9 => {
                    target.deactivation_info = Some(kuberic_core::types::ReplicaDeactivationInfo {
                        epoch: Epoch::new(100, 100),
                        catch_up_lsn: 100,
                    })
                }
                _ => {
                    target.deactivation_info = Some(kuberic_core::types::ReplicaDeactivationInfo {
                        epoch: target.epoch,
                        catch_up_lsn: 101,
                    })
                }
            }
            let mut placements = placements.clone();
            placements[2].eligible = false;
            let mut status = PlacementStatus::default();
            assert_eq!(
                rebalance_target(&set, &placements, &observations, &policy, &mut status, 100),
                None
            );
            assert_eq!(status.reason, "UnsafeCandidate", "defect {defect}");
            assert!(status.target_pod.is_none());
        }
    }

    #[test]
    fn primary_write_quorum_and_replication_progress_precede_load() {
        let (set, mut placements, mut observations, policy) = fixture();
        observations.get_mut(&2).unwrap().current_progress = 101;
        placements[1].domain_primaries = 1;
        let mut status = PlacementStatus::default();
        rebalance_target(&set, &placements, &observations, &policy, &mut status, 100);
        assert_eq!(status.target_pod.as_deref(), Some("kv-1"));
        observations.get_mut(&1).unwrap().write_status = AccessStatus::NoWriteQuorum;
        assert_eq!(
            rebalance_target(&set, &placements, &observations, &policy, &mut status, 200),
            None
        );
        assert_eq!(status.reason, "UnsafeCandidate");
    }

    #[test]
    fn scaling_and_other_topology_operations_take_precedence() {
        let (set, placements, observations, policy) = fixture();
        for phase in [
            Phase::Creating,
            Phase::FailingOver,
            Phase::AddingReplica,
            Phase::RemovingReplica,
            Phase::Switchover,
        ] {
            let mut set = set.clone();
            set.status.as_mut().unwrap().phase = phase;
            let mut status = PlacementStatus::default();
            assert_eq!(
                rebalance_target(&set, &placements, &observations, &policy, &mut status, 100),
                None
            );
            assert_eq!(status.reason, "ConflictingOperation");
        }
        let mut set = set;
        set.spec.replicas += 1;
        let mut status = PlacementStatus::default();
        rebalance_target(&set, &placements, &observations, &policy, &mut status, 100);
        assert_eq!(status.reason, "ConflictingOperation");
    }

    #[test]
    fn one_primary_difference_is_not_an_improvement_and_disabled_is_non_disruptive() {
        let (set, mut placements, observations, mut policy) = fixture();
        placements[0].domain_primaries = 1;
        let mut status = PlacementStatus::default();
        rebalance_target(&set, &placements, &observations, &policy, &mut status, 100);
        assert_eq!(status.reason, "InsufficientImprovement");
        policy.mode = BalancingMode::TieBreakOnly;
        rebalance_target(&set, &placements, &observations, &policy, &mut status, 200);
        assert_eq!(status.reason, "TieBreakOnly");
        assert!(status.target_pod.is_none());
    }

    fn inventory(set: &KubericSet) -> PlacementInventory {
        let nodes = (1..=3).map(|id| serde_json::from_value(json!({
                "metadata": {"name": format!("node-{id}"), "labels": {HOSTNAME_TOPOLOGY_KEY: format!("node-{id}")}},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]}
            })).unwrap()).collect();
        let pods = (1..=3).map(|id| serde_json::from_value(json!({
                "metadata": {"name": format!("kv-{}", id-1), "namespace": "default", "uid": format!("uid-{id}"),
                    "labels": {"kuberic.io/set": "kv", "kuberic.io/pod-index": (id-1).to_string()}},
                "spec": {"nodeName": format!("node-{id}"), "containers": []}
            })).unwrap()).collect();
        PlacementInventory {
            nodes,
            sets: vec![set.clone()],
            pods,
        }
    }

    #[test]
    fn inventory_counts_primary_incarnations_and_reserves_inflight_targets() {
        let (set, _, _, _) = fixture();
        let mut inventory = inventory(&set);
        let initial = replica_placements(
            &set,
            &inventory.pods,
            &inventory,
            &BTreeSet::new(),
            HOSTNAME_TOPOLOGY_KEY,
        )
        .unwrap();
        assert_eq!(initial[0].domain_primaries, 1);
        assert_eq!(initial_primary(&initial), Some(2));
        let status = inventory.sets[0].status.as_mut().unwrap();
        status.operation = Some(
            crate::durable::start_switchover(
                "set-uid",
                status.stable_snapshot.clone().unwrap(),
                2,
                100,
            )
            .unwrap(),
        );
        let reserved = replica_placements(
            &set,
            &inventory.pods,
            &inventory,
            &BTreeSet::new(),
            HOSTNAME_TOPOLOGY_KEY,
        )
        .unwrap();
        assert_eq!(reserved[0].domain_primaries, 0);
        assert_eq!(reserved[1].domain_primaries, 1);
    }

    #[test]
    fn missing_topology_is_unavailable_not_zero_load() {
        let (set, _, _, _) = fixture();
        let mut inventory = inventory(&set);
        inventory.nodes[0].metadata.labels = None;
        assert!(
            replica_placements(
                &set,
                &inventory.pods,
                &inventory,
                &BTreeSet::new(),
                HOSTNAME_TOPOLOGY_KEY
            )
            .unwrap_err()
            .contains("lacks")
        );
    }

    #[test]
    fn maintenance_unschedulable_and_unready_nodes_are_excluded() {
        let (set, _, _, _) = fixture();
        let mut inventory = inventory(&set);
        let mut maintenance = BTreeSet::new();
        maintenance.insert("node-1".to_string());
        inventory.nodes[1].spec =
            Some(serde_json::from_value(json!({"unschedulable": true})).unwrap());
        inventory.nodes[2].status = None;
        let candidates = replica_placements(
            &set,
            &inventory.pods,
            &inventory,
            &maintenance,
            HOSTNAME_TOPOLOGY_KEY,
        )
        .unwrap();
        assert!(candidates.iter().all(|candidate| !candidate.eligible));
        assert_eq!(initial_primary(&candidates), None);
    }

    #[test]
    fn required_topology_drift_and_incomplete_inventory_are_diagnosed() {
        let (mut set, _, _, _) = fixture();
        set.spec.scheduling = Some(crate::scheduling::SchedulingPolicy {
            mode: crate::scheduling::ReplicaAntiAffinityMode::Required,
            ..Default::default()
        });
        let mut inventory = inventory(&set);
        inventory.nodes[1]
            .metadata
            .labels
            .as_mut()
            .unwrap()
            .insert(HOSTNAME_TOPOLOGY_KEY.to_string(), "node-1".to_string());
        let collision = distribution(
            &set,
            &inventory.pods,
            &inventory.nodes,
            HOSTNAME_TOPOLOGY_KEY,
        );
        assert_eq!(
            collision.scheduling_reason.as_deref(),
            Some("RequiredTopologyViolation")
        );
        inventory.nodes.remove(1);
        let unknown = distribution(
            &set,
            &inventory.pods,
            &inventory.nodes,
            HOSTNAME_TOPOLOGY_KEY,
        );
        assert_eq!(
            unknown.scheduling_reason.as_deref(),
            Some("RequiredTopologyUnverified")
        );
    }

    #[test]
    fn replica_and_primary_domains_use_their_independent_policy_keys() {
        let (set, _, _, _) = fixture();
        let mut inventory = inventory(&set);
        for node in &mut inventory.nodes {
            node.metadata.labels.as_mut().unwrap().insert(
                "topology.kubernetes.io/zone".to_string(),
                "zone-a".to_string(),
            );
        }
        let status = distribution(
            &set,
            &inventory.pods,
            &inventory.nodes,
            "topology.kubernetes.io/zone",
        );
        assert_eq!(status.replica_domains.len(), 3);
        assert_eq!(status.primary_domain.as_deref(), Some("zone-a"));
        inventory.pods[0].metadata.uid = Some("replacement".to_string());
        let replaced = distribution(
            &set,
            &inventory.pods,
            &inventory.nodes,
            "topology.kubernetes.io/zone",
        );
        assert!(replaced.primary_node.is_none());
        assert!(replaced.primary_domain.is_none());
    }

    #[test]
    fn required_unschedulability_and_missing_domains_are_counted() {
        let (set, _, _, _) = fixture();
        let mut inventory = inventory(&set);
        inventory.pods[2].spec.as_mut().unwrap().node_name = None;
        inventory.pods[2].status = Some(serde_json::from_value(json!({
                "phase": "Pending", "conditions": [{"type": "PodScheduled", "status": "False", "reason": "Unschedulable"}]
            })).unwrap());
        let status = distribution(
            &set,
            &inventory.pods,
            &inventory.nodes,
            HOSTNAME_TOPOLOGY_KEY,
        );
        assert_eq!(status.unschedulable_replicas, 1);
        assert_eq!(status.missing_topology_replicas, 1);
        assert_eq!(status.primary_node.as_deref(), Some("node-1"));
        assert_eq!(status.replica_domains.len(), 2);
    }
}
