use std::time::{Duration, Instant};

use k8s_openapi::{
    api::{core::v1::Node, events::v1::Event},
    jiff::Timestamp,
};
use kube::{
    Api, ResourceExt,
    api::{DeleteParams, ListParams, Patch, PatchParams, PostParams, Preconditions},
};
use kuberic_operator::{
    cluster_api::{ClusterApi, KubeClusterApi},
    node_maintenance::{
        KubeMaintenanceApi, MaintenanceApi, MaintenanceBlockedReason, MaintenanceDesiredState,
        MaintenancePhase, NodeMaintenanceRequest, RequestContext, api::MAINTENANCE_FINALIZER,
    },
};
use serde_json::{Value, json};

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
