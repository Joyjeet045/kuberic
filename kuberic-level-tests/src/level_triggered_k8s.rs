use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result, bail};

struct NetworkPartition {
    rules: Vec<(String, &'static str, String)>,
}

struct ReplicaSuspension {
    node: String,
    pid: i64,
}

impl Drop for ReplicaSuspension {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["exec", &self.node, "kill", "-CONT", &self.pid.to_string()])
            .status();
    }
}

impl Drop for NetworkPartition {
    fn drop(&mut self) {
        for (node, direction, pod_ip) in self.rules.iter().rev() {
            let _ = Command::new("docker")
                .args([
                    "exec", node, "iptables", "-D", "FORWARD", direction, pod_ip, "-j", "DROP",
                ])
                .status();
        }
    }
}

fn cluster_args() -> Result<(String, String, String)> {
    let kubeconfig = std::env::var("KUBECONFIG").context("KUBECONFIG is required")?;
    let context = std::env::var("KUBE_CONTEXT").context("KUBE_CONTEXT is required")?;
    let cluster = std::env::var("KIND_CLUSTER_NAME").context("KIND_CLUSTER_NAME is required")?;
    if kubeconfig.is_empty()
        || context.is_empty()
        || cluster.is_empty()
        || kubeconfig == format!("{}/.kube/config", std::env::var("HOME").unwrap_or_default())
    {
        bail!("level-triggered KinD tests require an isolated explicit cluster");
    }
    Ok((kubeconfig, context, cluster))
}

fn kubectl(kubeconfig: &str, context: &str, args: &[&str]) -> Result<String> {
    let output = Command::new("kubectl")
        .args(["--kubeconfig", kubeconfig, "--context", context])
        .args(args)
        .output()
        .context("running kubectl")?;
    if !output.status.success() {
        bail!(
            "kubectl failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8(output.stdout)?)
}

fn set_status(kubeconfig: &str, context: &str) -> Result<serde_json::Value> {
    let status = kubectl(
        kubeconfig,
        context,
        &[
            "-n",
            "default",
            "get",
            "kubericset",
            "kvstore2",
            "-o",
            "json",
        ],
    )?;
    Ok(serde_json::from_str(&status)?)
}

fn status_ready(value: &serde_json::Value) -> bool {
    value["status"]["conditions"]
        .as_array()
        .is_some_and(|conditions| {
            conditions
                .iter()
                .any(|condition| condition["type"] == "Ready" && condition["status"] == "true")
        })
}

fn topology_primary_id(value: &serde_json::Value) -> Option<i64> {
    value["status"]["topology"]["members"]
        .as_array()?
        .iter()
        .find(|member| member["role"] == "primary")?["replicaId"]
        .as_i64()
}

fn pod_for_replica(kubeconfig: &str, context: &str, replica_id: i64) -> Result<String> {
    Ok(kubectl(
        kubeconfig,
        context,
        &[
            "-n",
            "default",
            "get",
            "pod",
            "-l",
            &format!("operator.kuberic.io/replica-id={replica_id}"),
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ],
    )?
    .trim()
    .to_string())
}

fn stop_replica_process(kubeconfig: &str, context: &str, replica_id: i64) -> Result<()> {
    let pod = pod_for_replica(kubeconfig, context, replica_id)?;
    let node = kubectl(
        kubeconfig,
        context,
        &[
            "-n",
            "default",
            "get",
            "pod",
            &pod,
            "-o",
            "jsonpath={.spec.nodeName}",
        ],
    )?;
    let container = Command::new("docker")
        .args([
            "exec",
            node.trim(),
            "crictl",
            "ps",
            "--label",
            &format!("io.kubernetes.pod.name={pod}"),
            "-q",
        ])
        .output()
        .context("resolving exact replica container")?;
    if !container.status.success() {
        bail!(
            "cannot resolve replica {replica_id} container: {}",
            String::from_utf8_lossy(&container.stderr)
        );
    }
    let container_id = String::from_utf8(container.stdout)?;
    let Some(container_id) = container_id.lines().find(|line| !line.trim().is_empty()) else {
        return Ok(());
    };
    let stopped = Command::new("docker")
        .args(["exec", node.trim(), "crictl", "stop", container_id.trim()])
        .output()
        .context("stopping exact replica container")?;
    if !stopped.status.success() {
        bail!(
            "cannot stop replica {replica_id} container: {}",
            String::from_utf8_lossy(&stopped.stderr)
        );
    }
    Ok(())
}

fn suspend_replica_process(
    kubeconfig: &str,
    context: &str,
    replica_id: i64,
) -> Result<ReplicaSuspension> {
    let pod = pod_for_replica(kubeconfig, context, replica_id)?;
    let node = kubectl(
        kubeconfig,
        context,
        &[
            "-n",
            "default",
            "get",
            "pod",
            &pod,
            "-o",
            "jsonpath={.spec.nodeName}",
        ],
    )?
    .trim()
    .to_string();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let (container_id, pid) = loop {
        let container = Command::new("docker")
            .args([
                "exec",
                &node,
                "crictl",
                "ps",
                "--label",
                &format!("io.kubernetes.pod.name={pod}"),
                "-q",
            ])
            .output()
            .context("resolving exact replica container")?;
        let container_id = String::from_utf8(container.stdout)?
            .lines()
            .find(|line| !line.trim().is_empty())
            .map(str::to_string);
        if let Some(container_id) = container_id {
            let inspection = Command::new("docker")
                .args(["exec", &node, "crictl", "inspect", &container_id])
                .output()
                .context("inspecting exact replica container")?;
            if inspection.status.success() {
                let inspection: serde_json::Value = serde_json::from_slice(&inspection.stdout)?;
                if let Some(pid) = inspection["info"]["pid"].as_i64() {
                    break (container_id, pid);
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            bail!("replica {replica_id} did not expose a running container process");
        }
        std::thread::sleep(Duration::from_millis(250));
    };
    let suspended = Command::new("docker")
        .args(["exec", &node, "kill", "-STOP", &pid.to_string()])
        .output()
        .context("suspending exact replica process")?;
    if !suspended.status.success() {
        bail!(
            "cannot suspend replica {replica_id} container {container_id}: {}",
            String::from_utf8_lossy(&suspended.stderr)
        );
    }
    Ok(ReplicaSuspension { node, pid })
}

fn partition_replica(
    kubeconfig: &str,
    context: &str,
    cluster: &str,
    replica_id: i64,
) -> Result<NetworkPartition> {
    let pod = pod_for_replica(kubeconfig, context, replica_id)?;
    let pod_ip = kubectl(
        kubeconfig,
        context,
        &[
            "-n",
            "default",
            "get",
            "pod",
            &pod,
            "-o",
            "jsonpath={.status.podIP}",
        ],
    )?
    .trim()
    .to_string();
    if pod_ip.is_empty() {
        bail!("replica {replica_id} has no Pod IP");
    }
    let nodes = Command::new("kind")
        .args(["get", "nodes", "--name", cluster])
        .output()
        .context("listing KinD nodes")?;
    if !nodes.status.success() {
        bail!(
            "cannot list KinD nodes: {}",
            String::from_utf8_lossy(&nodes.stderr)
        );
    }
    let mut partition = NetworkPartition { rules: Vec::new() };
    for node in String::from_utf8(nodes.stdout)?.lines() {
        for direction in ["-s", "-d"] {
            let output = Command::new("docker")
                .args([
                    "exec", node, "iptables", "-I", "FORWARD", direction, &pod_ip, "-j", "DROP",
                ])
                .output()
                .context("installing replica network partition")?;
            if !output.status.success() {
                bail!(
                    "cannot partition replica {replica_id} on {node}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            partition
                .rules
                .push((node.to_string(), direction, pod_ip.clone()));
        }
    }
    Ok(partition)
}

fn put_from_replica(
    kubeconfig: &str,
    context: &str,
    replica_id: i64,
    key: &str,
    value: &str,
) -> Result<u16> {
    let pod = pod_for_replica(kubeconfig, context, replica_id)?;
    let response = kubectl(
        kubeconfig,
        context,
        &[
            "-n",
            "default",
            "exec",
            &pod,
            "--",
            "curl",
            "--silent",
            "--show-error",
            "--max-time",
            "10",
            "-X",
            "PUT",
            "--data-binary",
            value,
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            &format!("http://127.0.0.1:8080/kv/{key}"),
        ],
    )?;
    Ok(response.trim().parse()?)
}

fn replica_diagnostics(
    kubeconfig: &str,
    context: &str,
    replica_id: i64,
) -> Result<serde_json::Value> {
    let pod = pod_for_replica(kubeconfig, context, replica_id)?;
    let response = kubectl(
        kubeconfig,
        context,
        &[
            "-n",
            "default",
            "exec",
            &pod,
            "--",
            "curl",
            "--fail",
            "--silent",
            "--max-time",
            "5",
            "http://127.0.0.1:8080/status",
        ],
    )?;
    Ok(serde_json::from_str(&response)?)
}

#[test]
#[ignore = "requires an explicitly owned isolated KinD cluster"]
fn bootstrap_reaches_three_member_topology_and_quorum_write() -> Result<()> {
    let (kubeconfig, context, _) = cluster_args()?;
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        let status = kubectl(
            &kubeconfig,
            &context,
            &[
                "-n",
                "default",
                "get",
                "kubericset",
                "kvstore2",
                "-o",
                "json",
            ],
        )?;
        let value: serde_json::Value = serde_json::from_str(&status)?;
        let authority = &value["status"];
        let members = authority["topology"]["members"]
            .as_array()
            .map_or(0, Vec::len);
        let ready = authority["conditions"]
            .as_array()
            .is_some_and(|conditions| {
                conditions
                    .iter()
                    .any(|condition| condition["type"] == "Ready" && condition["status"] == "true")
            });
        if authority["initialized"] == true && members == 3 && ready {
            break;
        }
        if std::time::Instant::now() >= deadline {
            bail!("bootstrap did not accept a three-member topology");
        }
        std::thread::sleep(Duration::from_secs(2));
    }

    kubectl(
        &kubeconfig,
        &context,
        &[
            "-n",
            "default",
            "delete",
            "service/kvstore2-peer",
            "secret/kvstore2-agent-credentials",
            "--ignore-not-found=true",
        ],
    )?;
    let support_deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        let peer = kubectl(
            &kubeconfig,
            &context,
            &["-n", "default", "get", "service", "kvstore2-peer"],
        );
        let credentials = kubectl(
            &kubeconfig,
            &context,
            &[
                "-n",
                "default",
                "get",
                "secret",
                "kvstore2-agent-credentials",
            ],
        );
        if peer.is_ok() && credentials.is_ok() {
            break;
        }
        if std::time::Instant::now() >= support_deadline {
            bail!("controller did not reconverge peer Service and credentials");
        }
        std::thread::sleep(Duration::from_secs(1));
    }

    kubectl(
        &kubeconfig,
        &context,
        &[
            "-n",
            "kuberic-system",
            "rollout",
            "restart",
            "deployment/kuberic-controller",
        ],
    )?;
    kubectl(
        &kubeconfig,
        &context,
        &[
            "-n",
            "kuberic-system",
            "rollout",
            "status",
            "deployment/kuberic-controller",
            "--timeout=180s",
        ],
    )?;

    let secondary = kubectl(
        &kubeconfig,
        &context,
        &[
            "-n",
            "default",
            "get",
            "pod",
            "-l",
            "operator.kuberic.io/replica-id=2",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ],
    )?;
    let before: serde_json::Value = serde_json::from_str(&kubectl(
        &kubeconfig,
        &context,
        &[
            "-n",
            "default",
            "exec",
            secondary.trim(),
            "--",
            "curl",
            "--fail",
            "--silent",
            "http://127.0.0.1:8080/status",
        ],
    )?)?;
    let node = kubectl(
        &kubeconfig,
        &context,
        &[
            "-n",
            "default",
            "get",
            "pod",
            secondary.trim(),
            "-o",
            "jsonpath={.spec.nodeName}",
        ],
    )?;
    let container = Command::new("docker")
        .args([
            "exec",
            node.trim(),
            "crictl",
            "ps",
            "--label",
            &format!("io.kubernetes.pod.name={}", secondary.trim()),
            "-q",
        ])
        .output()
        .context("resolving exact replica container")?;
    if !container.status.success() {
        bail!(
            "cannot resolve replica container: {}",
            String::from_utf8_lossy(&container.stderr)
        );
    }
    let container_id = String::from_utf8(container.stdout)?;
    let container_id = container_id.trim();
    if container_id.is_empty() || container_id.lines().count() != 1 {
        bail!("expected one exact replica container, observed {container_id:?}");
    }
    let stopped = Command::new("docker")
        .args(["exec", node.trim(), "crictl", "stop", container_id])
        .output()
        .context("stopping exact replica container")?;
    if !stopped.status.success() {
        bail!(
            "cannot stop replica container: {}",
            String::from_utf8_lossy(&stopped.stderr)
        );
    }
    let restart_deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        let status = kubectl(
            &kubeconfig,
            &context,
            &[
                "-n",
                "default",
                "exec",
                secondary.trim(),
                "--",
                "curl",
                "--fail",
                "--silent",
                "http://127.0.0.1:8080/status",
            ],
        );
        if let Ok(status) = status {
            let after: serde_json::Value = serde_json::from_str(&status)?;
            if after["processSession"] != before["processSession"]
                && after["currentConfiguration"] == before["currentConfiguration"]
            {
                break;
            }
        }
        if std::time::Instant::now() >= restart_deadline {
            bail!("secondary process did not reconstruct its accepted authority");
        }
        std::thread::sleep(Duration::from_secs(2));
    }

    let pod = kubectl(
        &kubeconfig,
        &context,
        &[
            "-n",
            "default",
            "get",
            "pod",
            "-l",
            "operator.kuberic.io/replica-id=1",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ],
    )?;
    let output = Command::new("kubectl")
        .args([
            "--kubeconfig",
            &kubeconfig,
            "--context",
            &context,
            "-n",
            "default",
            "exec",
            pod.trim(),
            "--",
            "sh",
            "-c",
            "curl --fail --silent --show-error -X PUT --data-binary phase6 http://127.0.0.1:8080/kv/bootstrap",
        ])
        .output()?;
    if !output.status.success() {
        bail!(
            "quorum write failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let value = kubectl(
        &kubeconfig,
        &context,
        &[
            "-n",
            "default",
            "exec",
            pod.trim(),
            "--",
            "curl",
            "--fail",
            "--silent",
            "--show-error",
            "http://127.0.0.1:8080/kv/bootstrap",
        ],
    )?;
    if value.trim() != "phase6" {
        bail!("unexpected read after quorum write: {value}");
    }
    Ok(())
}

#[test]
#[ignore = "requires an explicitly owned isolated KinD cluster"]
fn replacement_preserves_quorum_write_and_retires_old_incarnation() -> Result<()> {
    let (kubeconfig, context, _) = cluster_args()?;
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    let old_instance = loop {
        let status = kubectl(
            &kubeconfig,
            &context,
            &[
                "-n",
                "default",
                "get",
                "kubericset",
                "kvstore2",
                "-o",
                "json",
            ],
        )?;
        let value: serde_json::Value = serde_json::from_str(&status)?;
        let ready = value["status"]["conditions"]
            .as_array()
            .is_some_and(|conditions| {
                conditions
                    .iter()
                    .any(|condition| condition["type"] == "Ready" && condition["status"] == "true")
            });
        let member = value["status"]["topology"]["members"]
            .as_array()
            .and_then(|members| {
                members
                    .iter()
                    .find(|member| member["replicaId"].as_i64() == Some(2))
            });
        if ready && let Some(instance) = member.and_then(|member| member["instanceId"].as_str()) {
            break instance.to_string();
        }
        if std::time::Instant::now() >= deadline {
            bail!("bootstrap did not become ready before replacement");
        }
        std::thread::sleep(Duration::from_secs(2));
    };

    let primary = kubectl(
        &kubeconfig,
        &context,
        &[
            "-n",
            "default",
            "get",
            "pod",
            "-l",
            "operator.kuberic.io/replica-id=1",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ],
    )?;
    let before = Command::new("kubectl")
        .args([
            "--kubeconfig",
            &kubeconfig,
            "--context",
            &context,
            "-n",
            "default",
            "exec",
            primary.trim(),
            "--",
            "sh",
            "-c",
            "curl --fail --silent --show-error -X PUT --data-binary before-replacement http://127.0.0.1:8080/kv/replacement",
        ])
        .output()?;
    if !before.status.success() {
        bail!(
            "write before replacement failed: {}",
            String::from_utf8_lossy(&before.stderr)
        );
    }

    let secondary = kubectl(
        &kubeconfig,
        &context,
        &[
            "-n",
            "default",
            "get",
            "pod",
            "-l",
            "operator.kuberic.io/replica-id=2",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ],
    )?;
    kubectl(
        &kubeconfig,
        &context,
        &[
            "-n",
            "default",
            "delete",
            "pod",
            secondary.trim(),
            "--wait=true",
            "--timeout=120s",
        ],
    )?;

    let replacement_deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        let status = kubectl(
            &kubeconfig,
            &context,
            &[
                "-n",
                "default",
                "get",
                "kubericset",
                "kvstore2",
                "-o",
                "json",
            ],
        )?;
        let value: serde_json::Value = serde_json::from_str(&status)?;
        let ready = value["status"]["conditions"]
            .as_array()
            .is_some_and(|conditions| {
                conditions
                    .iter()
                    .any(|condition| condition["type"] == "Ready" && condition["status"] == "true")
            });
        let replacement = value["status"]["topology"]["members"]
            .as_array()
            .and_then(|members| {
                members
                    .iter()
                    .find(|member| member["replicaId"].as_i64() == Some(2))
            })
            .and_then(|member| member["instanceId"].as_str());
        if ready && replacement.is_some_and(|instance| instance != old_instance) {
            break;
        }
        if std::time::Instant::now() >= replacement_deadline {
            bail!("same-cardinality replacement did not converge");
        }
        std::thread::sleep(Duration::from_secs(2));
    }

    let cleanup_deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        let old_pvc = kubectl(
            &kubeconfig,
            &context,
            &["-n", "default", "get", "pvc", "kvstore2-2-data"],
        );
        if old_pvc.is_err() {
            break;
        }
        if std::time::Instant::now() >= cleanup_deadline {
            bail!("old replacement PVC was not retired");
        }
        std::thread::sleep(Duration::from_secs(2));
    }

    let primary = kubectl(
        &kubeconfig,
        &context,
        &[
            "-n",
            "default",
            "get",
            "pod",
            "-l",
            "operator.kuberic.io/replica-id=1",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ],
    )?;
    let after = Command::new("kubectl")
        .args([
            "--kubeconfig",
            &kubeconfig,
            "--context",
            &context,
            "-n",
            "default",
            "exec",
            primary.trim(),
            "--",
            "sh",
            "-c",
            "curl --fail --silent --show-error -X PUT --data-binary after-replacement http://127.0.0.1:8080/kv/replacement",
        ])
        .output()?;
    if !after.status.success() {
        bail!(
            "write after replacement failed: {}",
            String::from_utf8_lossy(&after.stderr)
        );
    }

    let replacement = kubectl(
        &kubeconfig,
        &context,
        &[
            "-n",
            "default",
            "get",
            "pod",
            "-l",
            "operator.kuberic.io/replica-id=2",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ],
    )?;
    let before_restart: serde_json::Value = serde_json::from_str(&kubectl(
        &kubeconfig,
        &context,
        &[
            "-n",
            "default",
            "exec",
            replacement.trim(),
            "--",
            "curl",
            "--fail",
            "--silent",
            "http://127.0.0.1:8080/status",
        ],
    )?)?;
    let node = kubectl(
        &kubeconfig,
        &context,
        &[
            "-n",
            "default",
            "get",
            "pod",
            replacement.trim(),
            "-o",
            "jsonpath={.spec.nodeName}",
        ],
    )?;
    let container = Command::new("docker")
        .args([
            "exec",
            node.trim(),
            "crictl",
            "ps",
            "--label",
            &format!("io.kubernetes.pod.name={}", replacement.trim()),
            "-q",
        ])
        .output()
        .context("resolving replacement container")?;
    let container_id = String::from_utf8(container.stdout)?;
    let container_id = container_id.trim();
    if !container.status.success() || container_id.is_empty() || container_id.lines().count() != 1 {
        bail!("cannot resolve one exact replacement container");
    }
    let stopped = Command::new("docker")
        .args(["exec", node.trim(), "crictl", "stop", container_id])
        .output()
        .context("stopping replacement container")?;
    if !stopped.status.success() {
        bail!(
            "cannot stop replacement container: {}",
            String::from_utf8_lossy(&stopped.stderr)
        );
    }
    let restart_deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        let status = kubectl(
            &kubeconfig,
            &context,
            &[
                "-n",
                "default",
                "exec",
                replacement.trim(),
                "--",
                "curl",
                "--fail",
                "--silent",
                "http://127.0.0.1:8080/status",
            ],
        );
        if let Ok(status) = status {
            let restarted: serde_json::Value = serde_json::from_str(&status)?;
            if restarted["processSession"] != before_restart["processSession"]
                && restarted["currentConfiguration"] == before_restart["currentConfiguration"]
            {
                break;
            }
        }
        if std::time::Instant::now() >= restart_deadline {
            bail!("replacement process did not reconstruct accepted authority");
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    let after_restart = Command::new("kubectl")
        .args([
            "--kubeconfig",
            &kubeconfig,
            "--context",
            &context,
            "-n",
            "default",
            "exec",
            primary.trim(),
            "--",
            "sh",
            "-c",
            "curl --silent --show-error -X PUT --data-binary after-restart -w '\\n%{http_code}' http://127.0.0.1:8080/kv/replacement-restart",
        ])
        .output()?;
    let response = String::from_utf8(after_restart.stdout)?;
    let (body, status) = response.rsplit_once('\n').unwrap_or((&response, ""));
    if !after_restart.status.success() || status.trim() != "200" {
        bail!(
            "write after replacement restart failed ({status}): {body}; {}",
            String::from_utf8_lossy(&after_restart.stderr),
        );
    }
    Ok(())
}

#[test]
#[ignore = "requires an explicitly owned isolated KinD cluster"]
fn failover_fences_old_primary_and_preserves_committed_data() -> Result<()> {
    let (kubeconfig, context, _) = cluster_args()?;
    let ready_deadline = std::time::Instant::now() + Duration::from_secs(120);
    let (old_primary, old_epoch) = loop {
        let value = set_status(&kubeconfig, &context)?;
        if status_ready(&value) {
            let primary =
                topology_primary_id(&value).context("ready topology omitted a Primary member")?;
            let epoch = value["status"]["topology"]["epoch"]["configurationNumber"]
                .as_i64()
                .context("ready topology omitted configuration epoch")?;
            break (primary, epoch);
        }
        if std::time::Instant::now() >= ready_deadline {
            bail!("bootstrap did not become ready before failover");
        }
        std::thread::sleep(Duration::from_secs(2));
    };
    if put_from_replica(
        &kubeconfig,
        &context,
        old_primary,
        "failover",
        "before-failover",
    )? != 200
    {
        bail!("pre-failover quorum write failed");
    }

    let failover_deadline = std::time::Instant::now() + Duration::from_secs(120);
    let new_primary = loop {
        stop_replica_process(&kubeconfig, &context, old_primary)?;
        let value = set_status(&kubeconfig, &context)?;
        let primary = topology_primary_id(&value);
        let epoch = value["status"]["topology"]["epoch"]["configurationNumber"].as_i64();
        if status_ready(&value)
            && primary.is_some_and(|primary| primary != old_primary)
            && epoch.is_some_and(|epoch| epoch > old_epoch)
        {
            break primary.unwrap();
        }
        if std::time::Instant::now() >= failover_deadline {
            bail!("ordinary failover did not select and publish a newer primary");
        }
        std::thread::sleep(Duration::from_millis(500));
    };

    let pod = pod_for_replica(&kubeconfig, &context, new_primary)?;
    let value = kubectl(
        &kubeconfig,
        &context,
        &[
            "-n",
            "default",
            "exec",
            &pod,
            "--",
            "curl",
            "--fail",
            "--silent",
            "http://127.0.0.1:8080/kv/failover",
        ],
    )?;
    if value.trim() != "before-failover" {
        bail!("committed value was not preserved across failover: {value}");
    }
    if put_from_replica(
        &kubeconfig,
        &context,
        new_primary,
        "failover",
        "after-failover",
    )? != 200
    {
        bail!("post-failover quorum write failed");
    }
    Ok(())
}

#[test]
#[ignore = "requires an explicitly owned isolated KinD cluster"]
fn quorum_loss_closes_writes_and_recovers_without_data_loss_epoch_change() -> Result<()> {
    let (kubeconfig, context, _) = cluster_args()?;
    let ready_deadline = std::time::Instant::now() + Duration::from_secs(120);
    let (primary, data_loss_number) = loop {
        let value = set_status(&kubeconfig, &context)?;
        if status_ready(&value) {
            break (
                topology_primary_id(&value).context("ready topology omitted a Primary member")?,
                value["status"]["topology"]["epoch"]["dataLossNumber"]
                    .as_i64()
                    .context("ready topology omitted data-loss epoch")?,
            );
        }
        if std::time::Instant::now() >= ready_deadline {
            bail!("bootstrap did not become ready before quorum-loss test");
        }
        std::thread::sleep(Duration::from_secs(2));
    };
    let secondaries = [1_i64, 2, 3]
        .into_iter()
        .filter(|replica_id| *replica_id != primary)
        .collect::<Vec<_>>();
    let suspensions = secondaries
        .iter()
        .map(|replica_id| suspend_replica_process(&kubeconfig, &context, *replica_id))
        .collect::<Result<Vec<_>>>()?;
    let loss_deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let value = set_status(&kubeconfig, &context)?;
        let no_quorum = value["status"]["conditions"]
            .as_array()
            .is_some_and(|conditions| {
                conditions.iter().any(|condition| {
                    condition["reason"] == "NoWriteQuorum" && condition["status"] != "false"
                })
            });
        let locally_closed = replica_diagnostics(&kubeconfig, &context, primary)
            .is_ok_and(|diagnostics| diagnostics["writeStatus"] == "NoWriteQuorum");
        if no_quorum && locally_closed {
            break;
        }
        if std::time::Instant::now() >= loss_deadline {
            bail!("quorum loss did not close primary write access");
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    if put_from_replica(&kubeconfig, &context, primary, "quorum-loss", "must-fail")? != 503 {
        bail!("write unexpectedly succeeded without Current Configuration quorum");
    }

    drop(suspensions);
    let recovery_deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        let value = set_status(&kubeconfig, &context)?;
        if status_ready(&value)
            && value["status"]["topology"]["epoch"]["dataLossNumber"].as_i64()
                == Some(data_loss_number)
        {
            break;
        }
        if std::time::Instant::now() >= recovery_deadline {
            bail!("returning quorum did not restore writes without data loss");
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    if put_from_replica(&kubeconfig, &context, primary, "quorum-loss", "recovered")? != 200 {
        bail!("write did not recover after quorum returned");
    }
    Ok(())
}

#[test]
#[ignore = "requires an explicitly owned isolated KinD cluster"]
fn adversarial_restart_partition_and_healing_preserve_single_writer() -> Result<()> {
    let (kubeconfig, context, cluster) = cluster_args()?;
    let ready_deadline = std::time::Instant::now() + Duration::from_secs(120);
    let initial_primary = loop {
        let value = set_status(&kubeconfig, &context)?;
        if status_ready(&value) {
            break topology_primary_id(&value)
                .context("ready topology omitted a Primary member")?;
        }
        if std::time::Instant::now() >= ready_deadline {
            bail!("bootstrap did not become ready before adversarial matrix");
        }
        std::thread::sleep(Duration::from_secs(2));
    };
    if put_from_replica(
        &kubeconfig,
        &context,
        initial_primary,
        "phase9",
        "before-adversity",
    )? != 200
    {
        bail!("initial adversarial write failed");
    }

    kubectl(
        &kubeconfig,
        &context,
        &[
            "-n",
            "kuberic-system",
            "rollout",
            "restart",
            "deployment/kuberic-controller",
        ],
    )?;
    kubectl(
        &kubeconfig,
        &context,
        &[
            "-n",
            "kuberic-system",
            "rollout",
            "status",
            "deployment/kuberic-controller",
            "--timeout=180s",
        ],
    )?;

    let partitioned = [1_i64, 2, 3]
        .into_iter()
        .find(|replica_id| *replica_id != initial_primary)
        .unwrap();
    {
        let _partition = partition_replica(&kubeconfig, &context, &cluster, partitioned)?;
        std::thread::sleep(Duration::from_secs(3));
        if put_from_replica(
            &kubeconfig,
            &context,
            initial_primary,
            "phase9",
            "during-secondary-partition",
        )? != 200
        {
            bail!("available quorum could not write during one-replica partition");
        }
    }

    let healed_deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if status_ready(&set_status(&kubeconfig, &context)?) {
            break;
        }
        if std::time::Instant::now() >= healed_deadline {
            bail!("healed network partition did not reconverge");
        }
        std::thread::sleep(Duration::from_secs(2));
    }

    stop_replica_process(&kubeconfig, &context, initial_primary)?;
    let restart_deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        let value = set_status(&kubeconfig, &context)?;
        if status_ready(&value)
            && let Some(primary) = topology_primary_id(&value)
            && put_from_replica(
                &kubeconfig,
                &context,
                primary,
                "phase9-restart",
                "reconstructed",
            )
            .is_ok_and(|status| status == 200)
        {
            break;
        }
        if std::time::Instant::now() >= restart_deadline {
            bail!("replica process restart did not reconstruct usable authority");
        }
        std::thread::sleep(Duration::from_secs(2));
    }

    let former_primary =
        topology_primary_id(&set_status(&kubeconfig, &context)?).context("missing primary")?;
    let failover_deadline = std::time::Instant::now() + Duration::from_secs(120);
    let new_primary = loop {
        stop_replica_process(&kubeconfig, &context, former_primary)?;
        let value = set_status(&kubeconfig, &context)?;
        if status_ready(&value)
            && topology_primary_id(&value).is_some_and(|primary| primary != former_primary)
        {
            break topology_primary_id(&value).unwrap();
        }
        if std::time::Instant::now() >= failover_deadline {
            bail!("adversarial failover did not converge");
        }
        std::thread::sleep(Duration::from_millis(500));
    };

    let stale_deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        match put_from_replica(
            &kubeconfig,
            &context,
            former_primary,
            "phase9-stale",
            "must-not-commit",
        ) {
            Ok(503) => break,
            Ok(200) => bail!("returned former primary accepted a direct write"),
            Ok(_) | Err(_) if std::time::Instant::now() < stale_deadline => {
                std::thread::sleep(Duration::from_secs(2));
            }
            Ok(status) => bail!("former primary returned unexpected HTTP status {status}"),
            Err(error) => return Err(error.context("former primary did not return for fencing")),
        }
    }

    if put_from_replica(
        &kubeconfig,
        &context,
        new_primary,
        "phase9",
        "after-healing",
    )? != 200
    {
        bail!("new primary could not write after adversarial healing");
    }
    let pod = pod_for_replica(&kubeconfig, &context, new_primary)?;
    let value = kubectl(
        &kubeconfig,
        &context,
        &[
            "-n",
            "default",
            "exec",
            &pod,
            "--",
            "curl",
            "--fail",
            "--silent",
            "http://127.0.0.1:8080/kv/phase9",
        ],
    )?;
    if value.trim() != "after-healing" {
        bail!("acknowledged state was not preserved after adversarial healing: {value}");
    }
    Ok(())
}
