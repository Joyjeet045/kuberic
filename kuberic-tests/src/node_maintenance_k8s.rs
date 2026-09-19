use std::time::{Duration, Instant};

use k8s_openapi::{
    api::{
        core::v1::{Node, PersistentVolumeClaim, Pod, Service},
        events::v1::Event,
        policy::v1::PodDisruptionBudget,
    },
    jiff::Timestamp,
};
use kube::{
    Api, ResourceExt,
    api::{DeleteParams, EvictParams, ListParams, Patch, PatchParams, PostParams, Preconditions},
};
use kuberic_operator::{
    cluster_api::{ClusterApi, KubeClusterApi},
    crd::KubericSet,
    node_maintenance::{
        KubeMaintenanceApi, MaintenanceApi, MaintenanceBlockedReason, MaintenanceDesiredState,
        MaintenancePhase, NodeMaintenanceRequest, RequestContext, api::MAINTENANCE_FINALIZER,
    },
};
use serde_json::{Value, json};

use crate::gateway_k8s::{
    connect_gateway, resources, retry_gateway, round_trip, route_ready, wait_resource, wait_set,
};

async fn wait_request(
    api: &Api<NodeMaintenanceRequest>,
    name: &str,
    phase: MaintenancePhase,
    reason: Option<MaintenanceBlockedReason>,
) -> NodeMaintenanceRequest {
    let deadline = Instant::now() + Duration::from_secs(150);
    loop {
        let request = api.get(name).await.unwrap();
        if request.status.as_ref().is_some_and(|status| {
            status.phase == phase
                && status.blocked_reason == reason
                && status.observed_generation == request.metadata.generation
        }) {
            return request;
        }
        assert!(
            Instant::now() < deadline,
            "maintenance request did not converge: {request:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn patch_request(
    api: &Api<NodeMaintenanceRequest>,
    request: &NodeMaintenanceRequest,
    spec: Value,
) -> NodeMaintenanceRequest {
    api.patch(
        &request.name_any(),
        &PatchParams::default(),
        &Patch::Merge(json!({
            "metadata": {"uid": request.metadata.uid}, "spec": spec
        })),
    )
    .await
    .unwrap()
}

async fn set_ready(api: &Api<Node>, node: &Node, ready: bool) {
    api.patch_status(
        &node.name_any(),
        &PatchParams::default(),
        &Patch::Merge(json!({
            "metadata": {"uid": node.metadata.uid},
            "status": {"conditions": [{
                "type": "Ready", "status": if ready { "True" } else { "False" },
                "reason": "MaintenanceTest", "message": "isolated synthetic Node",
                "lastHeartbeatTime": Timestamp::now().to_string(),
                "lastTransitionTime": Timestamp::now().to_string()
            }]}
        })),
    )
    .await
    .unwrap();
}

async fn create_node(api: &Api<Node>, name: &str) -> Node {
    api.create(
        &PostParams::default(),
        &serde_json::from_value(json!({
            "metadata": {"name": name, "labels": {"test.kuberic.io/maintenance": name}},
            "spec": {"unschedulable": true}
        }))
        .unwrap(),
    )
    .await
    .unwrap()
}

async fn create_request(
    api: &Api<NodeMaintenanceRequest>,
    name: &str,
    node: &str,
) -> NodeMaintenanceRequest {
    api.create(&PostParams::default(), &serde_json::from_value(json!({
        "metadata": {"name": name},
        "spec": {"nodeName": node, "operation": "Replace", "provider": "Manual", "providerEventId": name}
    })).unwrap()).await.unwrap()
}

fn fenced_delete(uid: Option<String>) -> DeleteParams {
    DeleteParams {
        preconditions: Some(Preconditions {
            uid,
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn wait_finalizer_removed(
    api: &Api<NodeMaintenanceRequest>,
    name: &str,
) -> NodeMaintenanceRequest {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let request = api.get(name).await.unwrap();
        if request
            .metadata
            .finalizers
            .as_ref()
            .is_none_or(|values| !values.iter().any(|value| value == MAINTENANCE_FINALIZER))
        {
            return request;
        }
        assert!(
            Instant::now() < deadline,
            "maintenance finalizer was not removed after release"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_deleted(api: &Api<NodeMaintenanceRequest>, name: &str) {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if api.get_opt(name).await.unwrap().is_none() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "released maintenance request was not deleted"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test]
#[test_log::test]
async fn test_node_maintenance_release_replacement_and_deletion() {
    tokio::time::timeout(Duration::from_secs(600), lifecycle_scenario())
        .await
        .expect("maintenance lifecycle scenario timed out");
}

async fn lifecycle_scenario() {
    crate::test_utils::ensure_kuberic_operator_deployed().await;
    let client = crate::test_utils::isolated_kube_client().await;
    let nodes: Api<Node> = Api::all(client.clone());
    let requests: Api<NodeMaintenanceRequest> = Api::all(client.clone());
    let cluster = KubeClusterApi {
        client: client.clone(),
    };
    let maintenance = KubeMaintenanceApi {
        client: client.clone(),
    };
    let name = format!("maintenance-test-{:08x}", rand::random::<u32>());
    let first_name = format!("{name}-first");
    let second_name = format!("{name}-second");
    let deletion_name = format!("{name}-delete");
    let node = create_node(&nodes, &name).await;
    set_ready(&nodes, &node, true).await;
    let created_first = create_request(&requests, &first_name, &name).await;
    let created_second = create_request(&requests, &second_name, &name).await;
    let first = wait_request(&requests, &first_name, MaintenancePhase::Prepared, None).await;
    let second = wait_request(&requests, &second_name, MaintenancePhase::Prepared, None).await;
    for request in [&first, &second] {
        let status = request.status.as_ref().unwrap();
        assert_eq!(status.node_uid, node.metadata.uid);
        assert!(
            request
                .metadata
                .finalizers
                .as_ref()
                .unwrap()
                .iter()
                .any(|value| value == MAINTENANCE_FINALIZER)
        );
        assert_eq!(status.conditions[0].status, "True");
    }
    assert!(
        cluster
            .list_maintenance_nodes()
            .await
            .unwrap()
            .contains(&name)
    );

    for invalid in [
        json!({"nodeName": "another-node"}),
        json!({"operation": "Reboot"}),
        json!({"providerEventId": "another-event"}),
    ] {
        let error = requests
            .patch(
                &first_name,
                &PatchParams::default(),
                &Patch::Merge(json!({"spec": invalid})),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, kube::Error::Api(ref error) if error.code == 422),
            "{error}"
        );
    }
    set_ready(&nodes, &node, true).await;
    patch_request(&requests, &first, json!({"desiredState": "Complete"})).await;
    let previous = first.status.as_ref().unwrap();
    assert!(
        maintenance
            .patch_request_status(
                &RequestContext {
                    name: &first_name,
                    uid: first.metadata.uid.as_deref().unwrap(),
                    resource_version: first.metadata.resource_version.as_deref().unwrap(),
                    deleting: false,
                    spec: &first.spec,
                    generation: first.metadata.generation,
                    previous,
                    now: Timestamp::now(),
                },
                previous
            )
            .await
            .is_err()
    );
    let released = wait_request(&requests, &first_name, MaintenancePhase::Released, None).await;
    assert_eq!(
        released.status.as_ref().unwrap().observed_desired_state,
        Some(MaintenanceDesiredState::Complete)
    );
    wait_finalizer_removed(&requests, &first_name).await;
    assert!(
        cluster
            .list_maintenance_nodes()
            .await
            .unwrap()
            .contains(&name),
        "the second request must keep the node excluded"
    );
    let error = requests
        .patch(
            &first_name,
            &PatchParams::default(),
            &Patch::Merge(json!({"spec": {"desiredState": "Prepare"}})),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, kube::Error::Api(ref error) if error.code == 422),
        "{error}"
    );

    set_ready(&nodes, &node, false).await;
    patch_request(&requests, &second, json!({"desiredState": "Cancel"})).await;
    let releasing = wait_request(
        &requests,
        &second_name,
        MaintenancePhase::Releasing,
        Some(MaintenanceBlockedReason::NodeNotReady),
    )
    .await;
    assert_eq!(
        releasing.status.as_ref().unwrap().conditions[0].status,
        "False"
    );
    assert!(
        cluster
            .list_maintenance_nodes()
            .await
            .unwrap()
            .contains(&name)
    );

    patch_request(
        &requests,
        &releasing,
        json!({"releaseNodeUid": node.metadata.uid}),
    )
    .await;
    nodes
        .delete(&name, &fenced_delete(node.metadata.uid.clone()))
        .await
        .unwrap();
    let replacement = create_node(&nodes, &name).await;
    assert_ne!(replacement.metadata.uid, node.metadata.uid);
    set_ready(&nodes, &replacement, true).await;
    let changed = wait_request(
        &requests,
        &second_name,
        MaintenancePhase::Releasing,
        Some(MaintenanceBlockedReason::NodeIncarnationChanged),
    )
    .await;
    assert_eq!(changed.status.as_ref().unwrap().node_uid, node.metadata.uid);
    set_ready(&nodes, &replacement, true).await;
    patch_request(
        &requests,
        &changed,
        json!({"releaseNodeUid": replacement.metadata.uid}),
    )
    .await;
    let released = wait_request(&requests, &second_name, MaintenancePhase::Released, None).await;
    assert_eq!(
        released.status.as_ref().unwrap().node_uid,
        node.metadata.uid
    );
    assert_eq!(
        released.status.as_ref().unwrap().released_node_uid,
        replacement.metadata.uid
    );
    wait_finalizer_removed(&requests, &second_name).await;
    assert!(
        !cluster
            .list_maintenance_nodes()
            .await
            .unwrap()
            .contains(&name)
    );

    let created_delete = create_request(&requests, &deletion_name, &name).await;
    wait_request(&requests, &deletion_name, MaintenancePhase::Prepared, None).await;
    set_ready(&nodes, &replacement, false).await;
    requests
        .delete(&deletion_name, &fenced_delete(created_delete.metadata.uid))
        .await
        .unwrap();
    let deleting = wait_request(
        &requests,
        &deletion_name,
        MaintenancePhase::Releasing,
        Some(MaintenanceBlockedReason::NodeNotReady),
    )
    .await;
    assert!(deleting.metadata.deletion_timestamp.is_some());
    assert_eq!(
        deleting.status.as_ref().unwrap().observed_desired_state,
        Some(MaintenanceDesiredState::Cancel)
    );
    assert!(
        cluster
            .list_maintenance_nodes()
            .await
            .unwrap()
            .contains(&name)
    );
    set_ready(&nodes, &replacement, true).await;
    wait_deleted(&requests, &deletion_name).await;
    assert!(
        !cluster
            .list_maintenance_nodes()
            .await
            .unwrap()
            .contains(&name)
    );

    let events: Api<Event> = Api::namespaced(client, "default");
    let events = events.list(&ListParams::default()).await.unwrap();
    assert!(
        events.items.iter().any(|event| {
            event
                .regarding
                .as_ref()
                .is_some_and(|reference| reference.uid == created_first.metadata.uid)
                && event.reason.as_deref() == Some("Completed")
        }),
        "the controller must publish a completion Event"
    );
    assert!(
        events.items.iter().any(|event| {
            event
                .regarding
                .as_ref()
                .is_some_and(|reference| reference.uid == created_second.metadata.uid)
                && event.reason.as_deref() == Some("NodeIncarnationChanged")
                && event.type_.as_deref() == Some("Warning")
        }),
        "the controller must publish a replacement warning Event"
    );

    for request in [created_first, created_second] {
        requests
            .delete(&request.name_any(), &fenced_delete(request.metadata.uid))
            .await
            .unwrap();
    }
    nodes
        .delete(&name, &fenced_delete(replacement.metadata.uid))
        .await
        .unwrap();
}

async fn verify_workload_write(application: &str, key: &str, acknowledged: &mut Vec<String>) {
    retry_gateway(
        async || {
            let mut client = connect_gateway(
                "http://127.0.0.1:30090",
                &format!("{application}.kuberic.test"),
            )
            .await?;
            round_trip(&mut client, key, key).await?;
            for previous in acknowledged.iter() {
                let response = client
                    .get(kvstore::proto::GetRequest {
                        key: previous.clone(),
                    })
                    .await?
                    .into_inner();
                anyhow::ensure!(
                    response.found && response.value == *previous,
                    "lost acknowledged maintenance write {previous}: {response:?}"
                );
            }
            Ok(())
        },
        tokio::time::Instant::now() + Duration::from_secs(90),
        Duration::from_millis(250),
    )
    .await
    .unwrap();
    acknowledged.push(key.to_string());
}

fn workload_pod(application: &str, index: usize, node: &str) -> Pod {
    serde_json::from_value(json!({
        "metadata": {
            "name": format!("{application}-{index}"),
            "labels": {"kuberic.io/set": application, "kuberic.io/pod-index": index.to_string(), "kuberic.io/role": "bootstrap"}
        },
        "spec": {
            "nodeName": node,
            "terminationGracePeriodSeconds": 30,
            "containers": [{
                "name": "app", "image": "localhost/kvstore:latest", "imagePullPolicy": "IfNotPresent",
                "ports": [{"name": "app", "containerPort": 8080}, {"name": "control", "containerPort": 9090}, {"name": "data", "containerPort": 9091}],
                "env": [
                    {"name": "KUBERIC_REPLICA_ID", "value": (index + 1).to_string()},
                    {"name": "KUBERIC_REPLICA_INSTANCE_ID", "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid"}}},
                    {"name": "KUBERIC_CONTROL_BIND", "value": "0.0.0.0:9090"},
                    {"name": "KUBERIC_DATA_BIND", "value": "0.0.0.0:9091"},
                    {"name": "KUBERIC_CLIENT_BIND", "value": "0.0.0.0:8080"},
                    {"name": "RUST_LOG", "value": "info"}
                ],
                "readinessProbe": {"tcpSocket": {"port": 9090}, "periodSeconds": 1}
            }]
        }
    })).unwrap()
}

async fn reboot_request(
    api: &Api<NodeMaintenanceRequest>,
    name: &str,
    node: &str,
) -> NodeMaintenanceRequest {
    api.create(&PostParams::default(), &serde_json::from_value(json!({
        "metadata": {"name": name},
        "spec": {"nodeName": node, "operation": "Reboot", "provider": "Manual", "providerEventId": name}
    })).unwrap()).await.unwrap()
}

#[tokio::test]
#[test_log::test]
#[ignore = "restarts the operator; CI runs this explicitly after the workspace suite on the owned three-node KinD cluster"]
async fn test_real_replicas_survive_node_maintenance() {
    tokio::time::timeout(Duration::from_secs(900), real_replica_scenario())
        .await
        .expect("real maintenance scenario timed out");
}

async fn real_replica_scenario() {
    crate::test_utils::ensure_kuberic_operator_deployed().await;
    let client = crate::test_utils::isolated_kube_client().await;
    let nodes: Api<Node> = Api::all(client.clone());
    let mut eligible_nodes = nodes.list(&ListParams::default()).await.unwrap().items;
    eligible_nodes.retain(|node| {
        node.status.as_ref().is_some_and(|status| {
            status
                .conditions
                .iter()
                .flatten()
                .any(|condition| condition.type_ == "Ready" && condition.status == "True")
        })
    });
    eligible_nodes.sort_by_key(ResourceExt::name_any);
    assert!(
        eligible_nodes.len() >= 3,
        "real maintenance regression requires the canonical three-node KinD cluster"
    );
    let application = format!("maintenance-workload-{:08x}", rand::random::<u32>());
    let pods: Api<Pod> = Api::namespaced(client.clone(), "xedio");
    let sets: Api<KubericSet> = Api::namespaced(client.clone(), "xedio");
    let requests: Api<NodeMaintenanceRequest> = Api::all(client.clone());
    let budgets: Api<PodDisruptionBudget> = Api::namespaced(client.clone(), "xedio");
    let selector = format!("kuberic.io/set={application}");
    for (index, node) in eligible_nodes.iter().take(3).enumerate() {
        pods.create(
            &PostParams::default(),
            &workload_pod(&application, index, &node.name_any()),
        )
        .await
        .unwrap();
    }
    let set = sets.create(&PostParams::default(), &serde_json::from_value(json!({
        "metadata": {"name": application},
        "spec": {"replicas": 3, "minReplicas": 2, "image": "localhost/kvstore:latest",
            "port": 8080, "controlPort": 9090, "dataPort": 9091, "storage": "256Mi", "failoverDelay": 5}
    })).unwrap()).await.unwrap();
    let owner = json!({"apiVersion": "kuberic.io/v1", "kind": "KubericSet", "name": application, "uid": set.metadata.uid, "controller": true});
    for pod in pods
        .list(&ListParams::default().labels(&selector))
        .await
        .unwrap()
        .items
    {
        pods.patch(
            &pod.name_any(),
            &PatchParams::default(),
            &Patch::Merge(json!({
                "metadata": {"uid": pod.metadata.uid, "ownerReferences": [owner]}
            })),
        )
        .await
        .unwrap();
    }
    let routes = resources(
        client.clone(),
        "gateway.networking.k8s.io",
        "GRPCRoute",
        "grpcroutes",
    );
    let route = routes.create(&PostParams::default(), &serde_json::from_value(json!({
        "apiVersion": "gateway.networking.k8s.io/v1", "kind": "GRPCRoute",
        "metadata": {"name": application, "namespace": "xedio", "ownerReferences": [owner]},
        "spec": {"parentRefs": [{"name": "kuberic", "sectionName": "grpc"}],
            "hostnames": [format!("{application}.kuberic.test")],
            "rules": [{"backendRefs": [{"name": format!("{application}-rw"), "port": 8080}]}]}
    })).unwrap()).await.unwrap();
    wait_resource(&routes, &application, "maintenance route", route_ready)
        .await
        .unwrap();
    let initial = wait_set(client.clone(), &application, None).await.unwrap();
    let original_primary = initial.data["status"]["currentPrimary"]
        .as_str()
        .unwrap()
        .to_string();
    let original_pod = pods.get(&original_primary).await.unwrap();
    let target_node = original_pod
        .spec
        .as_ref()
        .unwrap()
        .node_name
        .as_ref()
        .unwrap()
        .clone();
    let node = nodes.get(&target_node).await.unwrap();
    let mut acknowledged = Vec::new();
    verify_workload_write(&application, "before-maintenance", &mut acknowledged).await;
    let request_name = format!("{application}-primary");
    let request = reboot_request(&requests, &request_name, &target_node).await;
    wait_request(&requests, &request_name, MaintenancePhase::Preparing, None).await;
    let operator_pods = pods
        .list(&ListParams::default().labels("app=kuberic-operator"))
        .await
        .unwrap()
        .items;
    assert_eq!(operator_pods.len(), 1);
    let operator = &operator_pods[0];
    pods.delete(
        &operator.name_any(),
        &fenced_delete(operator.metadata.uid.clone()),
    )
    .await
    .unwrap();
    let prepared = wait_request(&requests, &request_name, MaintenancePhase::Prepared, None).await;
    assert_eq!(prepared.metadata.uid, request.metadata.uid);
    assert_eq!(
        prepared.status.as_ref().unwrap().node_uid,
        node.metadata.uid
    );
    assert_eq!(prepared.status.as_ref().unwrap().affected_sets.len(), 1);
    let moved = wait_set(client.clone(), &application, None).await.unwrap();
    let primary = moved.data["status"]["currentPrimary"]
        .as_str()
        .unwrap()
        .to_string();
    assert_ne!(primary, original_primary);
    assert_ne!(
        pods.get(&primary)
            .await
            .unwrap()
            .spec
            .unwrap()
            .node_name
            .as_deref(),
        Some(target_node.as_str())
    );
    let same = patch_request(&requests, &prepared, json!({"desiredState": "Prepare"})).await;
    assert_eq!(same.metadata.generation, prepared.metadata.generation);
    verify_workload_write(&application, "after-primary-move", &mut acknowledged).await;

    let secondary_name = format!("{application}-secondary");
    reboot_request(&requests, &secondary_name, &target_node).await;
    let secondary =
        wait_request(&requests, &secondary_name, MaintenancePhase::Prepared, None).await;
    assert_eq!(
        wait_set(client.clone(), &application, None)
            .await
            .unwrap()
            .data["status"]["currentPrimary"],
        primary
    );
    verify_workload_write(&application, "secondary-prepared", &mut acknowledged).await;
    patch_request(&requests, &secondary, json!({"desiredState": "Cancel"})).await;
    wait_request(&requests, &secondary_name, MaintenancePhase::Released, None).await;
    wait_finalizer_removed(&requests, &secondary_name).await;
    assert!(
        KubeClusterApi {
            client: client.clone()
        }
        .list_maintenance_nodes()
        .await
        .unwrap()
        .contains(&target_node)
    );
    verify_workload_write(&application, "after-cancellation", &mut acknowledged).await;

    let budget = budgets.create(&PostParams::default(), &serde_json::from_value(json!({
        "metadata": {"name": application, "ownerReferences": [owner]},
        "spec": {"minAvailable": 2, "selector": {"matchLabels": {"kuberic.io/set": application}}}
    })).unwrap()).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if budgets
            .get(&application)
            .await
            .unwrap()
            .status
            .is_some_and(|status| status.disruptions_allowed > 0)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "PDB never permitted a single-replica eviction"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    nodes
        .patch(
            &target_node,
            &PatchParams::default(),
            &Patch::Merge(json!({
                "metadata": {"uid": node.metadata.uid}, "spec": {"unschedulable": true}
            })),
        )
        .await
        .unwrap();
    pods.evict(
        &original_primary,
        &EvictParams {
            delete_options: Some(fenced_delete(original_pod.metadata.uid.clone())),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(240);
    loop {
        let current = pods.get_opt(&original_primary).await.unwrap();
        if current.as_ref().is_some_and(|pod| {
            pod.metadata.uid != original_pod.metadata.uid
                && pod.status.as_ref().is_some_and(|status| {
                    status
                        .conditions
                        .iter()
                        .flatten()
                        .any(|condition| condition.type_ == "Ready" && condition.status == "True")
                })
        }) {
            break;
        }
        assert!(Instant::now() < deadline, "evicted replica was not rebuilt");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    wait_set(client.clone(), &application, None).await.unwrap();
    verify_workload_write(&application, "after-eviction", &mut acknowledged).await;
    let before_release = requests.get(&request_name).await.unwrap();
    patch_request(
        &requests,
        &before_release,
        json!({"desiredState": "Complete"}),
    )
    .await;
    let released = wait_request(&requests, &request_name, MaintenancePhase::Released, None).await;
    assert_eq!(
        released
            .status
            .as_ref()
            .unwrap()
            .prepared_sets
            .as_ref()
            .unwrap()[0]
            .replicas[0]
            .pod_uid,
        original_pod.metadata.uid.clone().unwrap()
    );
    wait_finalizer_removed(&requests, &request_name).await;
    assert!(
        !KubeClusterApi {
            client: client.clone()
        }
        .list_maintenance_nodes()
        .await
        .unwrap()
        .contains(&target_node)
    );
    nodes.patch(&target_node, &PatchParams::default(), &Patch::Merge(json!({
        "metadata": {"uid": node.metadata.uid}, "spec": {"unschedulable": node.spec.as_ref().and_then(|spec| spec.unschedulable).unwrap_or(false)}
    }))).await.unwrap();

    verify_workload_write(&application, "after-completion", &mut acknowledged).await;

    for name in [&request_name, &secondary_name] {
        let current = requests.get(name).await.unwrap();
        requests
            .delete(name, &fenced_delete(current.metadata.uid))
            .await
            .unwrap();
    }
    routes
        .delete(&application, &fenced_delete(route.metadata.uid))
        .await
        .unwrap();
    budgets
        .delete(&application, &fenced_delete(budget.metadata.uid))
        .await
        .unwrap();
    sets.delete(&application, &fenced_delete(set.metadata.uid))
        .await
        .unwrap();
    let services: Api<Service> = Api::namespaced(client.clone(), "xedio");
    for service in services
        .list(&ListParams::default().labels(&selector))
        .await
        .unwrap()
        .items
    {
        services
            .delete(&service.name_any(), &fenced_delete(service.metadata.uid))
            .await
            .unwrap();
    }
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client, "xedio");
    for pvc in pvcs
        .list(&ListParams::default().labels(&selector))
        .await
        .unwrap()
        .items
    {
        pvcs.delete(&pvc.name_any(), &fenced_delete(pvc.metadata.uid))
            .await
            .unwrap();
    }
}
