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
    let endpoint = crate::test_utils::isolated_kvstore_endpoint();
    let mut client = kvstore::proto::kv_store_client::KvStoreClient::connect(endpoint)
        .await
        .expect("failed to connect through the dedicated KinD port mapping");

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

    let client = crate::test_utils::isolated_kube_client().await;
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
