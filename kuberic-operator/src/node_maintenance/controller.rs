use async_trait::async_trait;
use k8s_openapi::api::core::v1::{Node, Pod};
use k8s_openapi::jiff::Timestamp;

use super::api::{
    NodeMaintenanceRequest, NodeMaintenanceRequestSpec, NodeMaintenanceRequestStatus,
};
use super::discovery::{DiscoveryInput, MaintenancePod, NodeRef, reconcile_discovery};
use super::preflight::{Preflight, preflight};

pub const SET_LABEL: &str = "kuberic.io/set";
pub const ROLE_LABEL: &str = "kuberic.io/role";

#[async_trait]
pub trait MaintenanceApi: Send + Sync {
    async fn get_node(&self, name: &str) -> Result<Option<NodeRef>, String>;

    async fn list_maintenance_pods(&self) -> Result<Vec<MaintenancePod>, String>;

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

            reconcile_discovery(DiscoveryInput {
                spec: ctx.spec,
                generation: ctx.generation,
                previous: ctx.previous,
                node: node.as_ref(),
                pods: &pods,
                now: ctx.now,
            })
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
    };
    use std::sync::Mutex;

    const NOW: &str = "2026-09-06T20:00:00Z";

    #[derive(Default)]
    struct MockApi {
        node: Option<NodeRef>,
        pods: Vec<MaintenancePod>,
        patches: Mutex<Vec<NodeMaintenanceRequestStatus>>,
        node_calls: Mutex<usize>,
        list_calls: Mutex<usize>,
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
