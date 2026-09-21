//! Real scheduler coverage using the same scheduling builder as operator Pods.
//! All writes are restricted to a generated, UID-fenced namespace on the owned KinD fixture.

use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, Result, ensure};
use k8s_openapi::api::core::v1::{Container, Namespace, Node, Pod, PodSpec, Toleration};
use kube::{
    Api, Client, ResourceExt,
    api::{DeleteParams, ListParams, ObjectMeta, PostParams, Preconditions},
};
use kuberic_operator::scheduling::{
    HOSTNAME_TOPOLOGY_KEY, ReplicaAntiAffinityMode, SET_LABEL, SchedulingPolicy,
    diagnose_pending_pod, scheduling_pod_spec,
};
use serde_json::json;
use tokio::time::Instant;

const FIXTURE_LABEL: &str = "kuberic.io/test-fixture";
const FIXTURE_NAME: &str = "topology-placement";

fn verify_ownership_receipt(
    receipt: &str,
    cluster: &str,
    context: &str,
    kubeconfig: &str,
) -> Result<()> {
    ensure!(
        !cluster.is_empty() && cluster != "kind",
        "default KinD cluster is forbidden"
    );
    ensure!(
        context == format!("kind-{cluster}"),
        "unexpected fixture context"
    );
    for expected in [
        format!("cluster={cluster}"),
        format!("context={context}"),
        format!("kubeconfig={kubeconfig}"),
    ] {
        ensure!(
            receipt.lines().any(|line| line == expected),
            "missing fixture ownership: {expected}"
        );
    }
    Ok(())
}

struct Fixture {
    client: Client,
    namespace: String,
    uid: String,
}

impl Fixture {
    async fn create() -> Result<Self> {
        let kubeconfig = std::env::var("KUBECONFIG")
            .context("run against the isolated fixture created by just create-kind-cluster")?;
        let cluster = std::env::var("KIND_CLUSTER_NAME")?;
        let context = std::env::var("KUBE_CONTEXT")?;
        let receipt = std::fs::read_to_string(format!("{kubeconfig}.kuberic-owner"))
            .context("missing ownership receipt from just create-kind-cluster")?;
        verify_ownership_receipt(&receipt, &cluster, &context, &kubeconfig)?;
        let client = crate::test_utils::isolated_kube_client().await;
        let namespaces: Api<Namespace> = Api::all(client.clone());
        let namespace = namespaces
            .create(
                &PostParams::default(),
                &Namespace {
                    metadata: ObjectMeta {
                        generate_name: Some("kuberic-topology-".into()),
                        labels: Some(BTreeMap::from([(
                            FIXTURE_LABEL.into(),
                            FIXTURE_NAME.into(),
                        )])),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await?;
        Ok(Self {
            client,
            namespace: namespace.name_any(),
            uid: namespace
                .metadata
                .uid
                .context("created namespace has no UID")?,
        })
    }

    async fn cleanup(&self) -> Result<()> {
        let namespaces: Api<Namespace> = Api::all(self.client.clone());
        let Some(namespace) = namespaces.get_opt(&self.namespace).await? else {
            return Ok(());
        };
        ensure!(
            namespace.uid().as_deref() == Some(&self.uid)
                && namespace.labels().get(FIXTURE_LABEL).map(String::as_str) == Some(FIXTURE_NAME),
            "fixture ownership changed; refusing namespace deletion"
        );
        namespaces
            .delete(
                &self.namespace,
                &DeleteParams {
                    preconditions: Some(Preconditions {
                        uid: Some(self.uid.clone()),
                        resource_version: None,
                    }),
                    ..Default::default()
                },
            )
            .await?;
        Ok(())
    }

    async fn create_pod(&self, set: &str, index: u32, policy: &SchedulingPolicy) -> Result<String> {
        let name = format!("{set}-{index}");
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        pods.create(
            &PostParams::default(),
            &Pod {
                metadata: ObjectMeta {
                    name: Some(name.clone()),
                    labels: Some(BTreeMap::from([(SET_LABEL.into(), set.into())])),
                    ..Default::default()
                },
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "pause".into(),
                        image: Some("registry.k8s.io/pause:3.10".into()),
                        ..Default::default()
                    }],
                    automount_service_account_token: Some(false),
                    termination_grace_period_seconds: Some(0),
                    ..scheduling_pod_spec(set, &self.namespace, Some(policy))
                }),
                ..Default::default()
            },
        )
        .await?;
        Ok(name)
    }

    async fn wait_pod(&self, name: &str, ready: impl Fn(&Pod) -> bool) -> Result<Pod> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let pod = pods.get(name).await?;
            if ready(&pod) {
                return Ok(pod);
            }
            ensure!(
                Instant::now() < deadline,
                "scheduler deadline for {name}: {pod:?}"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn scheduled_node(&self, name: &str) -> Result<String> {
        let pod = self
            .wait_pod(name, |pod| {
                pod.spec
                    .as_ref()
                    .and_then(|s| s.node_name.as_ref())
                    .is_some()
            })
            .await?;
        pod.spec
            .and_then(|s| s.node_name)
            .context("scheduled Pod has no node")
    }
}

fn placement_policy(hostnames: &[String]) -> SchedulingPolicy {
    let mut policy: SchedulingPolicy = serde_json::from_value(json!({
        "affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
            "nodeSelectorTerms": [{"matchExpressions": [{
                "key": HOSTNAME_TOPOLOGY_KEY, "operator": "In", "values": hostnames
            }]}]
        }}}
    }))
    .unwrap();
    policy.tolerations.push(Toleration {
        key: Some("node-role.kubernetes.io/control-plane".into()),
        operator: Some("Exists".into()),
        effect: Some("NoSchedule".into()),
        ..Default::default()
    });
    policy
}

async fn scheduler_scenario(fixture: &Fixture) -> Result<()> {
    let nodes: Api<Node> = Api::all(fixture.client.clone());
    let hostnames: Vec<String> = nodes
        .list(&ListParams::default())
        .await?
        .items
        .iter()
        .filter(|node| {
            node.status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .is_some_and(|conditions| {
                    conditions
                        .iter()
                        .any(|c| c.type_ == "Ready" && c.status == "True")
                })
                && node.spec.as_ref().is_none_or(|spec| {
                    spec.unschedulable != Some(true)
                        && spec.taints.as_ref().is_none_or(|taints| {
                            taints.iter().all(|taint| {
                                !matches!(taint.effect.as_str(), "NoSchedule" | "NoExecute")
                                    || (taint.key == "node-role.kubernetes.io/control-plane"
                                        && taint.effect == "NoSchedule")
                            })
                        })
                })
        })
        .filter_map(|node| node.labels().get(HOSTNAME_TOPOLOGY_KEY).cloned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .take(2)
        .collect();
    ensure!(
        !hostnames.is_empty(),
        "fixture needs a ready schedulable hostname-labeled node"
    );

    // Defaults are soft: a one-domain fixture must still admit both replicas.
    let single_domain = placement_policy(&hostnames[..1]);
    let first = fixture.create_pod("soft-single", 0, &single_domain).await?;
    let first_node = fixture.scheduled_node(&first).await?;
    let second = fixture.create_pod("soft-single", 1, &single_domain).await?;
    ensure!(
        fixture.scheduled_node(&second).await? == first_node,
        "one-domain soft fallback failed"
    );

    let mut policy = placement_policy(&hostnames);
    let first = fixture.create_pod("soft-spread", 0, &policy).await?;
    let first_node = fixture.scheduled_node(&first).await?;
    let second = fixture.create_pod("soft-spread", 1, &policy).await?;
    let second_node = fixture.scheduled_node(&second).await?;
    if hostnames.len() == 2 {
        ensure!(
            first_node != second_node,
            "default scheduler did not honor preferred replica spreading"
        );
    }

    policy.mode = ReplicaAntiAffinityMode::Required;
    let mut assigned = std::collections::BTreeSet::new();
    for index in 0..hostnames.len() {
        let pod = fixture
            .create_pod("hard-spread", index as u32, &policy)
            .await?;
        ensure!(
            assigned.insert(fixture.scheduled_node(&pod).await?),
            "required replicas shared a domain"
        );
    }
    let excess = fixture
        .create_pod("hard-spread", hostnames.len() as u32, &policy)
        .await?;
    let blocked = fixture
        .wait_pod(&excess, |pod| {
            diagnose_pending_pod(pod).is_some_and(|diagnosis| diagnosis.reason == "Unschedulable")
        })
        .await?;
    let diagnosis = diagnose_pending_pod(&blocked).context("missing scheduler diagnosis")?;
    ensure!(
        diagnosis.message.contains("anti-affinity"),
        "unexpected scheduling failure: {diagnosis:?}"
    );
    ensure!(blocked.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Pending"));
    ensure!(
        blocked
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_ref())
            .is_none()
    );

    // A different set is not constrained by the full hard-spread set's replicas.
    let unrelated = fixture.create_pod("other-set", 0, &policy).await?;
    fixture.scheduled_node(&unrelated).await?;
    Ok(())
}

#[tokio::test]
async fn test_topology_placement_k8s_scheduler() -> Result<()> {
    let fixture = Fixture::create().await?;
    let result = tokio::time::timeout(Duration::from_secs(300), scheduler_scenario(&fixture))
        .await
        .context("scheduler scenario deadline exceeded")
        .and_then(|result| result);
    let cleanup = tokio::time::timeout(Duration::from_secs(30), fixture.cleanup())
        .await
        .context("fixture cleanup deadline exceeded")
        .and_then(|result| result);
    if let Err(error) = &cleanup {
        eprintln!(
            "cleanup for owned namespace {} failed: {error:#}",
            fixture.namespace
        );
    }
    result?;
    cleanup
}

#[test]
fn topology_fixture_requires_exact_ownership_coordinates() {
    let receipt =
        "cluster=kuberic-test\ncontext=kind-kuberic-test\nkubeconfig=.fixture/kubeconfig\n";
    assert!(
        verify_ownership_receipt(
            receipt,
            "kuberic-test",
            "kind-kuberic-test",
            ".fixture/kubeconfig"
        )
        .is_ok()
    );
    assert!(
        verify_ownership_receipt(receipt, "other", "kind-other", ".fixture/kubeconfig").is_err()
    );
    assert!(
        verify_ownership_receipt(receipt, "kuberic-test", "kind-kuberic-test", "foreign").is_err()
    );
    assert!(verify_ownership_receipt(receipt, "kind", "kind-kind", ".fixture/kubeconfig").is_err());
}
