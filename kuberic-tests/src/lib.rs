#[cfg(test)]
mod kvstore_k8s;

#[cfg(test)]
mod lease_election;

#[cfg(test)]
pub mod test_utils {

    const NS_XEDIO: &str = "xedio";

    /// Get the root directory of the repository
    pub fn get_repo_root() -> std::path::PathBuf {
        let dir = std::env::current_dir().expect("Failed to get current dir");
        dir.parent().unwrap().to_path_buf()
    }

    fn isolated_kube_coordinates() -> (String, String) {
        let kubeconfig =
            std::env::var("KUBECONFIG").expect("KinD tests require an isolated KUBECONFIG");
        let context =
            std::env::var("KUBE_CONTEXT").expect("KinD tests require an explicit KUBE_CONTEXT");
        let cluster_name = std::env::var("KIND_CLUSTER_NAME")
            .expect("KinD tests require an explicit KIND_CLUSTER_NAME");
        assert_ne!(cluster_name, "kind", "default KinD cluster is forbidden");
        assert_eq!(
            context,
            format!("kind-{cluster_name}"),
            "KUBE_CONTEXT must match the dedicated KinD cluster"
        );
        if let Ok(home) = std::env::var("HOME") {
            assert_ne!(
                std::path::Path::new(&kubeconfig),
                std::path::Path::new(&home).join(".kube/config"),
                "default user kubeconfig is forbidden"
            );
        }
        (kubeconfig, context)
    }

    pub async fn isolated_kube_client() -> kube::Client {
        let (kubeconfig_path, context) = isolated_kube_coordinates();
        let kubeconfig = kube::config::Kubeconfig::read_from(kubeconfig_path)
            .expect("Failed to read isolated kubeconfig");
        let config = kube::Config::from_custom_kubeconfig(
            kubeconfig,
            &kube::config::KubeConfigOptions {
                context: Some(context),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to load isolated Kubernetes context");
        kube::Client::try_from(config).expect("Failed to create isolated Kubernetes client")
    }

    pub fn isolated_kvstore_endpoint() -> String {
        if let Ok(endpoint) = std::env::var("KUBERIC_KVSTORE_ENDPOINT") {
            return endpoint;
        }

        let cluster_name = std::env::var("KIND_CLUSTER_NAME")
            .expect("KinD tests require an explicit KIND_CLUSTER_NAME");
        let container_name = format!("{cluster_name}-control-plane");
        let output = std::process::Command::new("docker")
            .args(["port", &container_name, "30090/tcp"])
            .output()
            .expect("Failed to resolve dedicated KinD NodePort mapping");
        assert!(
            output.status.success(),
            "Failed to resolve dedicated KinD NodePort mapping: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let mapping = String::from_utf8(output.stdout).expect("Docker port output is not UTF-8");
        let port = mapping
            .trim()
            .rsplit_once(':')
            .map(|(_, port)| port)
            .filter(|port| !port.is_empty())
            .expect("Dedicated KinD NodePort mapping has no host port");
        format!("http://127.0.0.1:{port}")
    }

    pub async fn kubectl_apply(path: &std::path::Path) {
        run_kubectl_cmd(&["apply", "-f", path.to_str().unwrap()])
            .await
            .expect("Failed to apply kubectl manifest");
    }

    pub async fn run_kubectl_cmd(args: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
        let (kubeconfig, context) = isolated_kube_coordinates();
        tracing::info!(
            "Running kubectl command against isolated context {}: kubectl {:?}",
            context,
            args.join(" ")
        );
        let output = tokio::process::Command::new("kubectl")
            .args(["--kubeconfig", &kubeconfig, "--context", &context])
            .args(args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .await
            .expect("Failed to execute kubectl command");

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            tracing::error!(
                "kubectl command failed. stdout: {}, stderr: {}",
                stdout,
                stderr
            );
            return Err(format!("kubectl command failed: {:?}", args).into());
        } else {
            let stdout = String::from_utf8_lossy(&output.stdout);
            tracing::info!("kubectl command succeeded. stdout: {}", stdout);
        }

        Ok(())
    }

    pub async fn wait_deployment_replica_ready(
        namespace: &str,
        deployment_name: &str,
        expected_replicas: i32,
        timeout_seconds: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let jsonpath = format!("{{.status.readyReplicas}}={}", expected_replicas);
        let timeout_arg = format!("{}s", timeout_seconds);
        let resource = format!("deployment/{}", deployment_name);
        run_kubectl_cmd(&[
            "wait",
            &resource,
            "--namespace",
            namespace,
            format!("--for=jsonpath={}", jsonpath).as_str(),
            "--timeout",
            &timeout_arg,
        ])
        .await
    }

    // -- Kuberic operator helpers --

    pub async fn ensure_kuberic_operator_deployed() {
        static INIT: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
        INIT.get_or_init(async || {
            ensure_kuberic_operator_deployed_internal().await;
        })
        .await;
    }

    async fn ensure_kuberic_operator_deployed_internal() {
        let client = isolated_kube_client().await;
        let deployments: kube::Api<k8s_openapi::api::apps::v1::Deployment> =
            kube::Api::namespaced(client.clone(), NS_XEDIO);
        let name = "kuberic-operator";
        match deployments.get(name).await {
            Ok(_) => {
                tracing::info!("kuberic-operator already deployed");
            }
            Err(kube::Error::Api(ae)) if ae.code == 404 => {
                tracing::info!("kuberic-operator not found, deploying...");
                let repo_root = get_repo_root();
                let path = repo_root
                    .join("kuberic-operator")
                    .join("deploy")
                    .join("deployment.yaml");
                kubectl_apply(&path).await;
                wait_deployment_replica_ready(NS_XEDIO, name, 1, 120)
                    .await
                    .expect("kuberic-operator failed to become ready");
                tracing::info!("kuberic-operator deployed");
            }
            Err(e) => panic!("Failed to get deployment: {}", e),
        }
    }

    pub async fn ensure_kvstore_deployed() {
        static INIT: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
        INIT.get_or_init(async || {
            ensure_kuberic_operator_deployed().await;
            ensure_kvstore_deployed_internal().await;
        })
        .await;
    }

    async fn ensure_kvstore_deployed_internal() {
        tracing::info!("Ensuring kvstore KubericSet is deployed...");
        let repo_root = get_repo_root();
        let path = repo_root
            .join("examples")
            .join("kvstore")
            .join("deploy")
            .join("kubericset.yaml");
        kubectl_apply(&path).await;

        wait_pods_ready(NS_XEDIO, "kuberic.io/set=kvstore", 3, 120)
            .await
            .expect("kvstore pods failed to become ready");

        // Wait for operator to reconcile status to Healthy
        wait_kubericset_healthy(NS_XEDIO, "kvstore", 3, 60)
            .await
            .expect("kvstore KubericSet failed to reach Healthy phase");
        tracing::info!("kvstore deployed and healthy");
    }

    pub async fn wait_kubericset_healthy(
        namespace: &str,
        name: &str,
        expected_replicas: i64,
        timeout_seconds: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client = isolated_kube_client().await;
        let api: kube::Api<kube::api::DynamicObject> = kube::Api::namespaced_with(
            client,
            namespace,
            &kube::discovery::ApiResource {
                group: "kuberic.io".into(),
                version: "v1".into(),
                kind: "KubericSet".into(),
                api_version: "kuberic.io/v1".into(),
                plural: "kubericsets".into(),
            },
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_seconds);

        loop {
            let obj = api.get(name).await?;
            let phase = obj
                .data
                .get("status")
                .and_then(|s| s.get("phase"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let ready_replicas = obj
                .data
                .get("status")
                .and_then(|s| s.get("readyReplicas"))
                .and_then(|v| v.as_i64())
                .unwrap_or_default();
            let replicas = obj
                .data
                .get("status")
                .and_then(|s| s.get("replicas"))
                .and_then(|v| v.as_i64())
                .unwrap_or_default();

            if phase == "Healthy"
                && ready_replicas == expected_replicas
                && replicas == expected_replicas
            {
                return Ok(());
            }

            if std::time::Instant::now() > deadline {
                return Err(format!(
                    "timeout: KubericSet {name} phase is {phase}, readyReplicas is \
                     {ready_replicas}, replicas is {replicas}; expected Healthy with \
                     {expected_replicas} replicas"
                )
                .into());
            }
            tracing::debug!(
                name,
                phase,
                ready_replicas,
                replicas,
                expected_replicas,
                "waiting for converged Healthy status"
            );
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
    }

    pub async fn wait_pods_ready(
        namespace: &str,
        label_selector: &str,
        expected: usize,
        timeout_seconds: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client = isolated_kube_client().await;
        let pods: kube::Api<k8s_openapi::api::core::v1::Pod> =
            kube::Api::namespaced(client, namespace);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_seconds);

        loop {
            let params = kube::api::ListParams::default().labels(label_selector);
            let list = pods.list(&params).await?;
            let ready_count = list
                .items
                .iter()
                .filter(|p| {
                    p.status
                        .as_ref()
                        .and_then(|s| s.conditions.as_ref())
                        .map(|c| {
                            c.iter()
                                .any(|cond| cond.type_ == "Ready" && cond.status == "True")
                        })
                        .unwrap_or(false)
                })
                .count();

            if ready_count >= expected {
                return Ok(());
            }

            if std::time::Instant::now() > deadline {
                return Err(format!(
                    "timeout: {}/{} pods ready for selector {}",
                    ready_count, expected, label_selector
                )
                .into());
            }

            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
    }
}
