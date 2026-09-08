use async_trait::async_trait;
use k8s_openapi::api::core::v1::{Node, Pod};
use k8s_openapi::jiff::Timestamp;
use std::collections::BTreeSet;

use crate::crd::{KubericSet, Phase, ReconfigurationPhase};

use super::api::{
    NodeMaintenanceRequest, NodeMaintenanceRequestSpec, NodeMaintenanceRequestStatus,
};
use super::attestation::{CommittedMember, CommittedTopology, Epoch, LiveMember, LiveObservation};
use super::discovery::{Discovery, DiscoveryInput, MaintenancePod, NodeRef, reconcile_discovery};
use super::preflight::{Preflight, preflight};
use super::safety::{SetPlacement, reconcile_preparation};

pub const SET_LABEL: &str = "kuberic.io/set";
pub const ROLE_LABEL: &str = "kuberic.io/role";

#[derive(Debug, PartialEq, Clone, Default)]
pub struct SetEvidence {
    pub committed: Option<CommittedTopology>,
    pub live: Option<LiveObservation>,
}

fn epoch_of(epoch: &crate::crd::EpochStatus) -> Epoch {
    Epoch {
        data_loss_number: epoch.data_loss_number,
        configuration_number: epoch.configuration_number,
    }
}

#[async_trait]
pub trait MaintenanceApi: Send + Sync {
    async fn get_node(&self, name: &str) -> Result<Option<NodeRef>, String>;

    async fn list_maintenance_pods(&self) -> Result<Vec<MaintenancePod>, String>;

    async fn get_set_evidence(&self, namespace: &str, name: &str) -> Result<SetEvidence, String>;

    async fn patch_request_status(
        &self,
        name: &str,
        status: &NodeMaintenanceRequestStatus,
    ) -> Result<(), String>;
}

pub struct ReconcileOutcome {
    pub status: NodeMaintenanceRequestStatus,
    pub persisted: bool,
}

pub struct RequestContext<'a> {
    pub name: &'a str,
    pub spec: &'a NodeMaintenanceRequestSpec,
    pub generation: Option<i64>,
    pub previous: &'a NodeMaintenanceRequestStatus,
    pub now: Timestamp,
}

pub async fn reconcile_request<A>(
    api: &A,
    ctx: RequestContext<'_>,
) -> Result<ReconcileOutcome, String>
where
    A: MaintenanceApi + ?Sized,
{
    let status = match preflight(ctx.spec, ctx.generation, ctx.previous, ctx.now) {
        Preflight::Settled(status) => status,
        Preflight::Discover => {
            let node = api.get_node(&ctx.spec.node_name).await?;
            let pods = if node.is_some() {
                api.list_maintenance_pods().await?
            } else {
                Vec::new()
            };

            let discovered = reconcile_discovery(DiscoveryInput {
                spec: ctx.spec,
                generation: ctx.generation,
                previous: ctx.previous,
                node: node.as_ref(),
                pods: &pods,
                now: ctx.now,
            });

            match discovered {
                Discovery::Blocked(status) => status,
                Discovery::Discovered(status) => {
                    let mut placements = Vec::with_capacity(status.affected_sets.len());
                    for set in &status.affected_sets {
                        let evidence = api.get_set_evidence(&set.namespace, &set.name).await?;
                        placements.push(SetPlacement {
                            committed: evidence.committed,
                            live: evidence.live,
                            promotable_pod_uids: promotable_pod_uids(
                                &pods,
                                &set.namespace,
                                &set.name,
                                &ctx.spec.node_name,
                            ),
                        });
                    }
                    reconcile_preparation(status, &placements, ctx.now)
                }
            }
        }
    };

    if &status == ctx.previous {
        return Ok(ReconcileOutcome {
            status,
            persisted: false,
        });
    }

    api.patch_request_status(ctx.name, &status).await?;
    Ok(ReconcileOutcome {
        status,
        persisted: true,
    })
}

fn promotable_pod_uids(
    pods: &[MaintenancePod],
    namespace: &str,
    set_name: &str,
    node_name: &str,
) -> BTreeSet<String> {
    pods.iter()
        .filter(|pod| pod.namespace == namespace && pod.set_name == set_name)
        .filter(|pod| {
            pod.node_name
                .as_deref()
                .is_some_and(|node| node != node_name)
        })
        .map(|pod| pod.uid.clone())
        .collect()
}

pub struct KubeMaintenanceApi {
    pub client: kube::Client,
}

impl KubeMaintenanceApi {
    fn pod_to_maintenance_pod(pod: &Pod) -> Option<MaintenancePod> {
        let meta = &pod.metadata;
        let labels = meta.labels.as_ref()?;
        let set_name = labels.get(SET_LABEL)?.clone();
        Some(MaintenancePod {
            namespace: meta.namespace.clone()?,
            name: meta.name.clone()?,
            uid: meta.uid.clone()?,
            node_name: pod.spec.as_ref().and_then(|spec| spec.node_name.clone()),
            set_name,
            is_primary: labels.get(ROLE_LABEL).map(String::as_str) == Some("primary"),
        })
    }
}

#[async_trait]
impl MaintenanceApi for KubeMaintenanceApi {
    async fn get_node(&self, name: &str) -> Result<Option<NodeRef>, String> {
        let api: kube::Api<Node> = kube::Api::all(self.client.clone());
        match api.get(name).await {
            Ok(node) => {
                let uid = node
                    .metadata
                    .uid
                    .ok_or_else(|| format!("node {name} has no uid"))?;
                Ok(Some(NodeRef {
                    name: name.to_string(),
                    uid,
                }))
            }
            Err(kube::Error::Api(ae)) if ae.code == 404 => Ok(None),
            Err(e) => Err(e.to_string()),
        }
    }

    async fn list_maintenance_pods(&self) -> Result<Vec<MaintenancePod>, String> {
        let api: kube::Api<Pod> = kube::Api::all(self.client.clone());
        let params = kube::api::ListParams::default().labels(SET_LABEL);
        let list = api.list(&params).await.map_err(|e| e.to_string())?;
        Ok(list
            .items
            .iter()
            .filter_map(Self::pod_to_maintenance_pod)
            .collect())
    }

    async fn get_set_evidence(&self, namespace: &str, name: &str) -> Result<SetEvidence, String> {
        let api: kube::Api<KubericSet> = kube::Api::namespaced(self.client.clone(), namespace);
        let Some(set) = api.get_opt(name).await.map_err(|e| e.to_string())? else {
            return Ok(SetEvidence::default());
        };
        let Some(status) = set.status else {
            return Ok(SetEvidence::default());
        };
        let committed = status
            .stable_snapshot
            .as_ref()
            .map(|snapshot| CommittedTopology {
                epoch: epoch_of(&snapshot.epoch),
                write_quorum: snapshot.write_quorum,
                members: snapshot
                    .members
                    .iter()
                    .map(|member| CommittedMember {
                        id: member.id,
                        pod_uid: member.instance_id.clone(),
                        is_primary: member.id == snapshot.primary_id,
                    })
                    .collect(),
            });
        let primary_pod_uid = status.current_primary.as_deref().and_then(|primary| {
            status
                .members
                .iter()
                .find(|member| member.name == primary)
                .map(|member| member.instance_id.clone())
        });
        let live = LiveObservation {
            epoch: epoch_of(&status.epoch),
            settled: status.phase == Phase::Healthy
                && status.reconfiguration_phase == ReconfigurationPhase::None,
            primary_pod_uid,
            members: status
                .members
                .iter()
                .map(|member| LiveMember {
                    id: member.id,
                    pod_uid: member.instance_id.clone(),
                    is_primary: member.role == "primary",
                    healthy: member.healthy,
                })
                .collect(),
        };
        Ok(SetEvidence {
            committed,
            live: Some(live),
        })
    }

    async fn patch_request_status(
        &self,
        name: &str,
        status: &NodeMaintenanceRequestStatus,
    ) -> Result<(), String> {
        let api: kube::Api<NodeMaintenanceRequest> = kube::Api::all(self.client.clone());
        let mut current = api.get(name).await.map_err(|e| e.to_string())?;
        current.status = Some(status.clone());
        api.replace_status(name, &kube::api::PostParams::default(), &current)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_maintenance::api::{
        MaintenanceBlockedReason, MaintenanceDesiredState, MaintenanceOperation, MaintenancePhase,
        PREPARED_CONDITION_TYPE,
    };
    use std::sync::Mutex;

    const NOW: &str = "2026-09-06T20:00:00Z";

    #[derive(Default)]
    struct MockApi {
        node: Option<NodeRef>,
        pods: Vec<MaintenancePod>,
        evidence: SetEvidence,
        patches: Mutex<Vec<NodeMaintenanceRequestStatus>>,
        node_calls: Mutex<usize>,
        list_calls: Mutex<usize>,
        topology_calls: Mutex<usize>,
        fail_patch: bool,
        fail_get_node: bool,
    }

    #[async_trait]
    impl MaintenanceApi for MockApi {
        async fn get_node(&self, _name: &str) -> Result<Option<NodeRef>, String> {
            *self.node_calls.lock().unwrap() += 1;
            if self.fail_get_node {
                return Err("node read rejected".to_string());
            }
            Ok(self.node.clone())
        }

        async fn list_maintenance_pods(&self) -> Result<Vec<MaintenancePod>, String> {
            *self.list_calls.lock().unwrap() += 1;
            Ok(self.pods.clone())
        }

        async fn get_set_evidence(
            &self,
            _namespace: &str,
            _name: &str,
        ) -> Result<SetEvidence, String> {
            *self.topology_calls.lock().unwrap() += 1;
            Ok(self.evidence.clone())
        }

        async fn patch_request_status(
            &self,
            _name: &str,
            status: &NodeMaintenanceRequestStatus,
        ) -> Result<(), String> {
            if self.fail_patch {
                return Err("status patch rejected".to_string());
            }
            self.patches.lock().unwrap().push(status.clone());
            Ok(())
        }
    }

    fn spec() -> NodeMaintenanceRequestSpec {
        NodeMaintenanceRequestSpec {
            node_name: "worker-04".to_string(),
            operation: MaintenanceOperation::Reboot,
            desired_state: MaintenanceDesiredState::Prepare,
            provider: Some("Manual".to_string()),
            provider_event_id: Some("event-123".to_string()),
            not_before: None,
            deadline: None,
        }
    }

    fn node() -> NodeRef {
        NodeRef {
            name: "worker-04".to_string(),
            uid: "node-uid-a".to_string(),
        }
    }

    fn pod(name: &str, node_name: Option<&str>, primary: bool) -> MaintenancePod {
        MaintenancePod {
            namespace: "default".to_string(),
            name: name.to_string(),
            uid: format!("uid-{name}"),
            node_name: node_name.map(str::to_string),
            set_name: "kv".to_string(),
            is_primary: primary,
        }
    }

    async fn run(
        api: &MockApi,
        previous: &NodeMaintenanceRequestStatus,
    ) -> Result<ReconcileOutcome, String> {
        run_spec(api, &spec(), previous).await
    }

    async fn run_spec(
        api: &MockApi,
        spec: &NodeMaintenanceRequestSpec,
        previous: &NodeMaintenanceRequestStatus,
    ) -> Result<ReconcileOutcome, String> {
        reconcile_request(
            api,
            RequestContext {
                name: "req-1",
                spec,
                generation: Some(1),
                previous,
                now: NOW.parse().expect("timestamp"),
            },
        )
        .await
    }

    fn evidence(primary: &str, members: &[&str], write_quorum: u32) -> SetEvidence {
        let uid = |pod: &str| format!("uid-{pod}");
        let epoch = Epoch {
            data_loss_number: 0,
            configuration_number: 1,
        };
        SetEvidence {
            committed: Some(CommittedTopology {
                epoch,
                write_quorum,
                members: members
                    .iter()
                    .enumerate()
                    .map(|(index, pod)| CommittedMember {
                        id: index as i64 + 1,
                        pod_uid: uid(pod),
                        is_primary: *pod == primary,
                    })
                    .collect(),
            }),
            live: Some(LiveObservation {
                epoch,
                settled: true,
                primary_pod_uid: Some(uid(primary)),
                members: members
                    .iter()
                    .enumerate()
                    .map(|(index, pod)| LiveMember {
                        id: index as i64 + 1,
                        pod_uid: uid(pod),
                        is_primary: *pod == primary,
                        healthy: true,
                    })
                    .collect(),
            }),
        }
    }

    fn assert_no_discovery(api: &MockApi) {
        assert_eq!(*api.node_calls.lock().unwrap(), 0);
        assert_eq!(*api.list_calls.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn an_expired_request_does_not_depend_on_reading_the_node() {
        let api = MockApi {
            fail_get_node: true,
            pods: vec![pod("kv-0", Some("worker-04"), true)],
            ..Default::default()
        };
        let spec = NodeMaintenanceRequestSpec {
            deadline: Some("2026-09-06T19:00:00Z".to_string()),
            ..spec()
        };
        let outcome = run_spec(&api, &spec, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Expired);
        assert_eq!(
            outcome.status.blocked_reason,
            Some(MaintenanceBlockedReason::DeadlineExceeded)
        );
        assert_no_discovery(&api);
    }

    #[tokio::test]
    async fn a_terminal_request_does_not_perform_discovery() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-0", Some("worker-04"), true)],
            ..Default::default()
        };
        let previous = NodeMaintenanceRequestStatus {
            phase: MaintenancePhase::Released,
            observed_generation: Some(1),
            observed_desired_state: Some(MaintenanceDesiredState::Prepare),
            ..Default::default()
        };
        let outcome = run(&api, &previous).await.unwrap();

        assert_eq!(outcome.status, previous);
        assert!(!outcome.persisted);
        assert_no_discovery(&api);
    }

    #[tokio::test]
    async fn a_release_request_does_not_perform_discovery() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-0", Some("worker-04"), true)],
            ..Default::default()
        };
        let spec = NodeMaintenanceRequestSpec {
            desired_state: MaintenanceDesiredState::Complete,
            ..spec()
        };
        let previous = NodeMaintenanceRequestStatus {
            phase: MaintenancePhase::Prepared,
            observed_generation: Some(1),
            ..Default::default()
        };
        let outcome = run_spec(&api, &spec, &previous).await.unwrap();

        assert_eq!(
            outcome.status.observed_desired_state,
            Some(MaintenanceDesiredState::Complete)
        );
        assert_no_discovery(&api);

        let repeat = run_spec(&api, &spec, &outcome.status).await.unwrap();
        assert!(!repeat.persisted);
        assert_eq!(api.patches.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_secondary_only_node_is_prepared_when_quorum_survives() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-2", Some("worker-04"), false)],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Prepared);
        assert!(outcome.status.prepared_at.is_some());
        assert!(outcome.status.affected_sets[0].primary_moved);
        assert!(outcome.status.affected_sets[0].quorum_without_node);

        let condition = outcome
            .status
            .conditions
            .iter()
            .find(|condition| condition.type_ == PREPARED_CONDITION_TYPE)
            .expect("prepared condition");
        assert_eq!(condition.status, "True");
        assert_eq!(condition.reason, "PrimariesMovedAndQuorumVerified");
    }

    #[tokio::test]
    async fn an_unhealthy_survivor_is_never_reported_as_prepared() {
        let mut evidence = evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2);
        evidence
            .live
            .as_mut()
            .expect("live")
            .members
            .iter_mut()
            .find(|member| member.pod_uid == "uid-kv-1")
            .expect("member")
            .healthy = false;
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-2", Some("worker-04"), false)],
            evidence,
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_ne!(outcome.status.phase, MaintenancePhase::Prepared);
        assert_eq!(
            outcome.status.blocked_reason,
            Some(MaintenanceBlockedReason::BlockedByQuorum)
        );
        assert!(!outcome.status.affected_sets[0].quorum_without_node);
    }

    #[tokio::test]
    async fn a_reconfiguring_set_is_never_reported_as_prepared() {
        let mut evidence = evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2);
        evidence.live.as_mut().expect("live").settled = false;
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-2", Some("worker-04"), false)],
            evidence,
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Preparing);
        assert!(outcome.status.prepared_at.is_none());
        assert!(!outcome.status.affected_sets[0].quorum_without_node);
    }

    #[tokio::test]
    async fn a_replaced_incarnation_is_never_reported_as_prepared() {
        let mut evidence = evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2);
        evidence
            .live
            .as_mut()
            .expect("live")
            .members
            .iter_mut()
            .find(|member| member.pod_uid == "uid-kv-1")
            .expect("member")
            .pod_uid = "uid-kv-1-restarted".to_string();
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-2", Some("worker-04"), false)],
            evidence,
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Preparing);
        assert!(outcome.status.prepared_at.is_none());
    }

    #[tokio::test]
    async fn losing_write_quorum_blocks_preparation() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![
                pod("kv-1", Some("worker-04"), false),
                pod("kv-2", Some("worker-04"), false),
            ],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Blocked);
        assert_eq!(
            outcome.status.blocked_reason,
            Some(MaintenanceBlockedReason::BlockedByQuorum)
        );
        assert!(outcome.status.prepared_at.is_none());
    }

    #[tokio::test]
    async fn a_primary_on_the_node_waits_while_a_replica_elsewhere_can_take_over() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![
                pod("kv-0", Some("worker-04"), true),
                pod("kv-1", Some("worker-05"), false),
                pod("kv-2", Some("worker-06"), false),
            ],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Preparing);
        assert!(!outcome.status.affected_sets[0].primary_moved);
        assert!(!outcome.status.affected_sets[0].no_eligible_target);
        assert!(outcome.status.affected_sets[0].quorum_without_node);
    }

    #[tokio::test]
    async fn a_primary_with_no_scheduled_replica_elsewhere_is_blocked() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![
                pod("kv-0", Some("worker-04"), true),
                pod("kv-1", None, false),
                pod("kv-2", None, false),
            ],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Blocked);
        assert_eq!(
            outcome.status.blocked_reason,
            Some(MaintenanceBlockedReason::NoEligibleTarget)
        );
        assert!(outcome.status.affected_sets[0].no_eligible_target);
        assert!(outcome.status.prepared_at.is_none());
    }

    #[tokio::test]
    async fn a_request_is_never_prepared_while_a_primary_remains_on_the_node() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-0", Some("worker-04"), true)],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        let condition = outcome
            .status
            .conditions
            .iter()
            .find(|condition| condition.type_ == PREPARED_CONDITION_TYPE)
            .expect("prepared condition");
        assert_eq!(condition.status, "False");
    }

    #[tokio::test]
    async fn an_unpublished_topology_never_reports_readiness() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-2", Some("worker-04"), false)],
            evidence: SetEvidence::default(),
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Preparing);
        assert!(!outcome.status.affected_sets[0].quorum_without_node);
    }

    #[tokio::test]
    async fn a_duplicate_delivery_does_not_rewrite_a_prepared_request() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-2", Some("worker-04"), false)],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let first = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();
        assert!(first.persisted);

        let second = run(&api, &first.status).await.unwrap();
        assert!(!second.persisted);
        assert_eq!(second.status, first.status);
        assert_eq!(api.patches.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn preparation_resumes_from_persisted_status_after_a_restart() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-2", Some("worker-04"), false)],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let persisted = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap()
            .status;

        let restarted = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-2", Some("worker-04"), false)],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let resumed = run(&restarted, &persisted).await.unwrap();

        assert_eq!(resumed.status.phase, MaintenancePhase::Prepared);
        assert_eq!(resumed.status.prepared_at, persisted.prepared_at);
        assert!(!resumed.persisted);
    }

    #[tokio::test]
    async fn a_primary_that_moves_away_advances_the_request_to_prepared() {
        let blocked = MockApi {
            node: Some(node()),
            pods: vec![
                pod("kv-0", Some("worker-04"), true),
                pod("kv-1", Some("worker-05"), false),
                pod("kv-2", Some("worker-06"), false),
            ],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let preparing = run(&blocked, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap()
            .status;
        assert_eq!(preparing.phase, MaintenancePhase::Preparing);

        let moved = MockApi {
            node: Some(node()),
            pods: vec![
                pod("kv-0", Some("worker-04"), false),
                pod("kv-1", Some("worker-05"), true),
                pod("kv-2", Some("worker-06"), false),
            ],
            evidence: evidence("kv-1", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let outcome = run(&moved, &preparing).await.unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Prepared);
        assert!(outcome.status.affected_sets[0].primary_moved);
        assert!(outcome.status.prepared_at.is_some());
    }

    #[tokio::test]
    async fn a_node_without_replicas_is_prepared_without_a_topology_read() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-0", Some("worker-09"), true)],
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Prepared);
        assert!(outcome.status.affected_sets.is_empty());
        assert_eq!(*api.topology_calls.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn a_request_before_its_window_does_not_list_pods() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-0", Some("worker-04"), true)],
            ..Default::default()
        };
        let spec = NodeMaintenanceRequestSpec {
            not_before: Some("2026-09-06T21:00:00Z".to_string()),
            ..spec()
        };
        let outcome = run_spec(&api, &spec, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Requested);
        assert_no_discovery(&api);
    }

    #[tokio::test]
    async fn an_invalid_deadline_blocks_without_discovery() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-0", Some("worker-04"), true)],
            ..Default::default()
        };
        let spec = NodeMaintenanceRequestSpec {
            deadline: Some("tomorrow".to_string()),
            ..spec()
        };
        let outcome = run_spec(&api, &spec, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Blocked);
        assert_eq!(
            outcome.status.blocked_reason,
            Some(MaintenanceBlockedReason::InvalidDeadline)
        );
        assert_no_discovery(&api);
    }

    #[tokio::test]
    async fn discovery_is_persisted_through_the_status_subresource() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-0", Some("worker-04"), true)],
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert!(outcome.persisted);
        assert_eq!(outcome.status.phase, MaintenancePhase::Preparing);
        assert_eq!(outcome.status.node_uid.as_deref(), Some("node-uid-a"));

        let patches = api.patches.lock().unwrap();
        assert_eq!(patches.len(), 1);
        assert_eq!(patches[0].affected_sets.len(), 1);
        assert!(patches[0].affected_sets[0].hosts_primary);
    }

    #[tokio::test]
    async fn unchanged_status_is_not_rewritten() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-0", Some("worker-04"), false)],
            ..Default::default()
        };
        let first = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();
        assert!(first.persisted);

        let second = run(&api, &first.status).await.unwrap();
        assert!(!second.persisted);
        assert_eq!(second.status, first.status);
        assert_eq!(api.patches.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn missing_node_blocks_without_listing_pods() {
        let api = MockApi {
            node: None,
            pods: vec![pod("kv-0", Some("worker-04"), true)],
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Blocked);
        assert_eq!(
            outcome.status.blocked_reason,
            Some(MaintenanceBlockedReason::NodeNotFound)
        );
        assert_eq!(*api.list_calls.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn status_patch_failure_is_reported() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-0", Some("worker-04"), false)],
            fail_patch: true,
            ..Default::default()
        };
        let result = run(&api, &NodeMaintenanceRequestStatus::default()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn pod_conversion_requires_the_set_label() {
        let mut pod = Pod::default();
        pod.metadata.name = Some("kv-0".to_string());
        pod.metadata.namespace = Some("default".to_string());
        pod.metadata.uid = Some("uid-kv-0".to_string());
        assert!(KubeMaintenanceApi::pod_to_maintenance_pod(&pod).is_none());

        pod.metadata.labels = Some(
            [(SET_LABEL.to_string(), "kv".to_string())]
                .into_iter()
                .collect(),
        );
        let converted = KubeMaintenanceApi::pod_to_maintenance_pod(&pod).expect("converted");
        assert_eq!(converted.set_name, "kv");
        assert!(!converted.is_primary);
        assert_eq!(converted.node_name, None);
    }

    #[tokio::test]
    async fn pod_conversion_reads_node_and_primary_role() {
        let mut pod = Pod::default();
        pod.metadata.name = Some("kv-1".to_string());
        pod.metadata.namespace = Some("default".to_string());
        pod.metadata.uid = Some("uid-kv-1".to_string());
        pod.metadata.labels = Some(
            [
                (SET_LABEL.to_string(), "kv".to_string()),
                (ROLE_LABEL.to_string(), "primary".to_string()),
            ]
            .into_iter()
            .collect(),
        );
        pod.spec = Some(k8s_openapi::api::core::v1::PodSpec {
            node_name: Some("worker-04".to_string()),
            ..Default::default()
        });

        let converted = KubeMaintenanceApi::pod_to_maintenance_pod(&pod).expect("converted");
        assert_eq!(converted.node_name.as_deref(), Some("worker-04"));
        assert!(converted.is_primary);
    }
}
