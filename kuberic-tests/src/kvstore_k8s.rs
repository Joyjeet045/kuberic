/// Test kvstore deployed in KinD: write and read via gRPC client over NodePort.
#[tokio::test]
#[test_log::test]
#[serial_test::serial]
async fn test_kvstore_k8s_write_read() {
    crate::test_utils::ensure_kvstore_deployed().await;

    // Apply NodePort service overlay for dev access (port 30090)
    let repo_root = crate::test_utils::get_repo_root();
    let nodeport_path = repo_root
        .join("examples")
        .join("kvstore")
        .join("deploy")
        .join("nodeport-svc.yaml");
    crate::test_utils::kubectl_apply(&nodeport_path).await;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Connect via NodePort
    let mut client =
        kvstore::proto::kv_store_client::KvStoreClient::connect("http://127.0.0.1:30090")
            .await
            .expect("failed to connect via NodePort 30090");

    // Put a key
    let put_resp = client
        .put(kvstore::proto::PutRequest {
            key: "test-k8s-key".into(),
            value: "test-k8s-value".into(),
        })
        .await
        .expect("Put failed");
    tracing::info!(lsn = put_resp.into_inner().lsn, "Put succeeded");

    // Get the key back
    let get_resp = client
        .get(kvstore::proto::GetRequest {
            key: "test-k8s-key".into(),
        })
        .await
        .expect("Get failed");
    let inner = get_resp.into_inner();
    assert!(inner.found, "key should be found");
    assert_eq!(inner.value, "test-k8s-value", "value mismatch");
    tracing::info!("Put/Get round-trip succeeded via NodePort 30090");
}

/// Test kvstore KubericSet status shows Healthy with 3 replicas.
#[tokio::test]
#[test_log::test]
#[serial_test::serial]
async fn test_kvstore_k8s_status_healthy() {
    crate::test_utils::ensure_kvstore_deployed().await;

    let client = kube::Client::try_default().await.unwrap();
    let api: kube::Api<kube::api::DynamicObject> = kube::Api::namespaced_with(
        client,
        "xedio",
        &kube::discovery::ApiResource {
            group: "kuberic.io".into(),
            version: "v1".into(),
            kind: "KubericSet".into(),
            api_version: "kuberic.io/v1".into(),
            plural: "kubericsets".into(),
        },
    );

    let obj = api.get("kvstore").await.expect("failed to get KubericSet");
    let status = obj.data.get("status").expect("no status");

    let phase = status.get("phase").and_then(|v| v.as_str()).unwrap_or("");
    assert_eq!(phase, "Healthy", "expected Healthy phase, got {}", phase);

    let ready = status
        .get("readyReplicas")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    assert_eq!(ready, 3, "expected 3 ready replicas, got {}", ready);

    let primary = status.get("currentPrimary").and_then(|v| v.as_str());
    assert!(primary.is_some(), "expected a current primary");
    tracing::info!(primary = ?primary, "KubericSet is Healthy");
}

#[tokio::test]
#[test_log::test]
#[serial_test::serial]
async fn test_kvstore_k8s_framework_native_remove_replica() {
    crate::test_utils::ensure_kvstore_deployed().await;
    let client = kube::Client::try_default().await.unwrap();
    let sets: kube::Api<kube::api::DynamicObject> = kube::Api::namespaced_with(
        client.clone(),
        "xedio",
        &kube::discovery::ApiResource {
            group: "kuberic.io".into(),
            version: "v1".into(),
            kind: "KubericSet".into(),
            api_version: "kuberic.io/v1".into(),
            plural: "kubericsets".into(),
        },
    );
    let before = sets.get("kvstore").await.expect("failed to get KubericSet");
    let removed_selector = ["remove", "Replica", "Execution", "Mode"].concat();
    assert!(
        before
            .data
            .get("spec")
            .and_then(|spec| spec.get(&removed_selector))
            .is_none(),
        "framework-native removal must not require a mode selector"
    );
    let owner_uid = before.metadata.uid.clone().expect("KubericSet UID");
    let pods: kube::Api<k8s_openapi::api::core::v1::Pod> =
        kube::Api::namespaced(client.clone(), "xedio");
    let before_pods = pods
        .list(&kube::api::ListParams::default().labels("kuberic.io/set=kvstore"))
        .await
        .expect("failed to list pre-remove pods")
        .items
        .into_iter()
        .map(|pod| {
            (
                pod.metadata.name.expect("pod name"),
                pod.metadata.uid.expect("pod UID"),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(before_pods.len(), 3);

    crate::test_utils::patch_kubericset_replicas("xedio", "kvstore", 2)
        .await
        .expect("failed to request ScaleDown without a mode selector");
    let obj = crate::test_utils::wait_kubericset_native_remove_terminal("xedio", "kvstore", 2, 180)
        .await
        .expect("framework-native remove did not complete");
    let status = obj.data.get("status").expect("no terminal status");
    assert!(
        status.get("removeReplicaExecution").is_some(),
        "native terminal reference must remain published"
    );
    let checkpoint_name = status
        .get("removeReplicaExecution")
        .and_then(|execution| execution.get("checkpointName"))
        .and_then(|name| name.as_str())
        .expect("native checkpoint name");
    let checkpoints: kube::Api<k8s_openapi::api::core::v1::ConfigMap> =
        kube::Api::namespaced(client, "xedio");
    let checkpoint = checkpoints
        .get(checkpoint_name)
        .await
        .expect("terminal checkpoint must precede topology publication");
    let owners = checkpoint
        .metadata
        .owner_references
        .as_deref()
        .expect("checkpoint owner reference");
    assert_eq!(owners.len(), 1);
    assert_eq!(owners[0].uid, owner_uid);
    assert_eq!(owners[0].controller, Some(false));
    assert_eq!(owners[0].block_owner_deletion, Some(false));
    let envelope: kuberic_durable_execution::CheckpointEnvelope = serde_json::from_str(
        checkpoint
            .data
            .as_ref()
            .and_then(|data| data.get("checkpoint.json"))
            .expect("checkpoint payload"),
    )
    .expect("valid checkpoint envelope");
    let payload: kuberic_durable_execution::CheckpointPayload =
        serde_json::from_slice(envelope.payload().as_slice()).expect("valid checkpoint payload");
    let terminal_checkpoint_bytes = envelope.encoded_len().unwrap();
    assert!(
        matches!(
            payload.state(),
            kuberic_durable_execution::CheckpointState::Terminal { .. }
        ),
        "published topology requires a terminal durable checkpoint"
    );
    assert_eq!(
        status
            .get("stableSnapshot")
            .and_then(|snapshot| snapshot.get("members"))
            .and_then(|members| members.as_array())
            .map(Vec::len),
        Some(2),
        "two-member topology must be published after terminal durability"
    );
    let after_pods = pods
        .list(&kube::api::ListParams::default().labels("kuberic.io/set=kvstore"))
        .await
        .expect("failed to list post-remove pods")
        .items
        .into_iter()
        .map(|pod| {
            (
                pod.metadata.name.expect("pod name"),
                pod.metadata.uid.expect("pod UID"),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(after_pods.len(), 2);
    assert_eq!(
        before_pods
            .iter()
            .filter(|(name, uid)| after_pods.get(*name) == Some(*uid))
            .count(),
        2,
        "surviving pods must retain their admitted UIDs"
    );
    let removed_pods = before_pods
        .keys()
        .filter(|name| !after_pods.contains_key(*name))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(removed_pods.len(), 1);
    println!(
        "KUBERIC_LIVE_NATIVE_REMOVE owner_uid={} checkpoint={} terminal_checkpoint_bytes={} removed_pod={} surviving_pods={}",
        owner_uid,
        checkpoint_name,
        terminal_checkpoint_bytes,
        removed_pods[0],
        after_pods.len(),
    );

    crate::test_utils::patch_kubericset_replicas("xedio", "kvstore", 3)
        .await
        .expect("failed to restore the live smoke-test fixture");
    crate::test_utils::wait_pods_ready("xedio", "kuberic.io/set=kvstore", 3, 120)
        .await
        .expect("restored kvstore pods failed to become ready");
    crate::test_utils::wait_kubericset_healthy("xedio", "kvstore", 3, 120)
        .await
        .expect("restored KubericSet failed to become Healthy");
}
