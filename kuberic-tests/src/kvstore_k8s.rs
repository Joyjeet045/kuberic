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

#[tokio::test]
#[test_log::test]
#[serial_test::serial]
async fn test_kvstore_k8s_direct_switchover_checkpoint_owner_gc() {
    crate::test_utils::ensure_kvstore_deployed().await;
    let client = crate::test_utils::isolated_kube_client().await;
    let resource = kube::discovery::ApiResource {
        group: "kuberic.io".into(),
        version: "v1".into(),
        kind: "KubericSet".into(),
        api_version: "kuberic.io/v1".into(),
        plural: "kubericsets".into(),
    };
    let sets: kube::Api<kube::api::DynamicObject> =
        kube::Api::namespaced_with(client.clone(), "xedio", &resource);
    let source = sets.get("kvstore").await.expect("failed to get source set");
    let name = format!("kvstore-switchover-gc-{}", std::process::id());
    let mut candidate = kube::api::DynamicObject::new(&name, &resource);
    candidate.data = serde_json::json!({
        "spec": source.data.get("spec").cloned().expect("source spec"),
    });
    sets.create(&kube::api::PostParams::default(), &candidate)
        .await
        .expect("failed to create switchover GC fixture");

    crate::test_utils::wait_pods_ready("xedio", &format!("kuberic.io/set={name}"), 3, 180)
        .await
        .expect("switchover GC fixture pods failed to become ready");
    crate::test_utils::wait_kubericset_healthy("xedio", &name, 3, 180)
        .await
        .expect("switchover GC fixture failed to become Healthy");

    let healthy = sets.get(&name).await.expect("failed to reload fixture");
    let status = healthy.data.get("status").expect("fixture status");
    let current_primary = status
        .get("currentPrimary")
        .and_then(|value| value.as_str())
        .expect("current primary");
    let target = status
        .get("members")
        .and_then(|value| value.as_array())
        .and_then(|members| {
            members
                .iter()
                .filter_map(|member| member.get("name").and_then(|value| value.as_str()))
                .find(|name| *name != current_primary)
        })
        .expect("secondary switchover target")
        .to_string();
    sets.patch_status(
        &name,
        &kube::api::PatchParams::default(),
        &kube::api::Patch::Merge(serde_json::json!({
            "status": {
                "targetPrimary": target.clone(),
            }
        })),
    )
    .await
    .expect("failed to request live direct switchover");

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(240);
    let terminal = loop {
        let object = sets.get(&name).await.expect("failed to poll switchover");
        let status = object.data.get("status");
        let complete = status
            .and_then(|status| status.get("phase"))
            .and_then(|value| value.as_str())
            == Some("Healthy")
            && status
                .and_then(|status| status.get("currentPrimary"))
                .and_then(|value| value.as_str())
                == Some(target.as_str())
            && status
                .and_then(|status| status.get("switchoverExecution"))
                .is_some_and(|value| !value.is_null());
        if complete {
            break object;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "live direct switchover did not complete"
        );
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    };

    let owner_uid = terminal.metadata.uid.clone().expect("fixture UID");
    let checkpoint_name = terminal
        .data
        .get("status")
        .and_then(|status| status.get("switchoverExecution"))
        .and_then(|execution| execution.get("checkpointName"))
        .and_then(|value| value.as_str())
        .expect("direct switchover checkpoint name")
        .to_string();
    let checkpoints: kube::Api<k8s_openapi::api::core::v1::ConfigMap> =
        kube::Api::namespaced(client, "xedio");
    let checkpoint = checkpoints
        .get(&checkpoint_name)
        .await
        .expect("direct switchover terminal checkpoint");
    let owners = checkpoint
        .metadata
        .owner_references
        .as_deref()
        .expect("direct switchover checkpoint owner");
    assert_eq!(owners.len(), 1);
    assert_eq!(owners[0].uid, owner_uid);
    assert_eq!(owners[0].controller, Some(false));
    assert_eq!(owners[0].block_owner_deletion, Some(false));
    let envelope: kuberic_durable_execution::CheckpointEnvelope = serde_json::from_str(
        checkpoint
            .data
            .as_ref()
            .and_then(|data| data.get("checkpoint.json"))
            .expect("direct switchover checkpoint payload"),
    )
    .expect("valid direct switchover checkpoint envelope");
    let payload: kuberic_durable_execution::CheckpointPayload =
        serde_json::from_slice(envelope.payload().as_slice())
            .expect("valid direct switchover checkpoint payload");
    assert!(matches!(
        payload.state(),
        kuberic_durable_execution::CheckpointState::Terminal { .. }
    ));

    sets.delete(&name, &kube::api::DeleteParams::default())
        .await
        .expect("failed to delete switchover GC fixture");
    let gc_deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    loop {
        match checkpoints.get(&checkpoint_name).await {
            Err(kube::Error::Api(error)) if error.code == 404 => break,
            Ok(_) => {}
            Err(error) => panic!("failed to poll checkpoint garbage collection: {error}"),
        }
        assert!(
            std::time::Instant::now() <= gc_deadline,
            "direct switchover checkpoint was not garbage collected"
        );
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    println!(
        "KUBERIC_LIVE_DIRECT_SWITCHOVER owner_uid={owner_uid} checkpoint={checkpoint_name} target={target} owner_gc=complete"
    );
}

#[tokio::test]
#[test_log::test]
#[serial_test::serial]
async fn test_kvstore_k8s_framework_native_remove_replica() {
    crate::test_utils::ensure_kvstore_deployed().await;
    let client = crate::test_utils::isolated_kube_client().await;
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
