use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};

struct NetworkPartition {
    rules: Vec<(String, Vec<String>)>,
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
        for (node, rule) in self.rules.iter().rev() {
            let _ = Command::new("docker")
                .args(["exec", node, "iptables", "-D", "FORWARD"])
                .args(rule)
                .status();
        }
    }
}

fn cluster_args() -> Result<(String, String, String)> {
    let kubeconfig = std::env::var("KUBECONFIG").context("KUBECONFIG is required")?;
    let context = std::env::var("KUBE_CONTEXT").context("KUBE_CONTEXT is required")?;
    let cluster = std::env::var("KIND_CLUSTER_NAME").context("KIND_CLUSTER_NAME is required")?;
    let receipt = std::fs::read_to_string(format!("{kubeconfig}.kuberic-owner"))
        .context("reading explicit cluster ownership receipt")?;
    validate_cluster_contract(&kubeconfig, &context, &cluster, &receipt)?;
    ensure!(
        kubectl(&kubeconfig, &context, &["config", "current-context"])?.trim() == context,
        "owned kubeconfig current context does not match KUBE_CONTEXT"
    );
    Ok((kubeconfig, context, cluster))
}

fn validate_cluster_contract(
    kubeconfig: &str,
    context: &str,
    cluster: &str,
    receipt: &str,
) -> Result<()> {
    if kubeconfig.is_empty()
        || context.is_empty()
        || cluster.is_empty()
        || cluster == "kind"
        || cluster.len() > 40
        || !cluster
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        || context != format!("kind-{cluster}")
        || kubeconfig.contains([':', '\n', '\r'])
        || kubeconfig == format!("{}/.kube/config", std::env::var("HOME").unwrap_or_default())
    {
        bail!("level-triggered KinD tests require an isolated explicit cluster");
    }
    ensure!(
        receipt == format!("cluster={cluster}\ncontext={context}\nkubeconfig={kubeconfig}\n"),
        "cluster ownership receipt does not match the explicit cluster/context/kubeconfig"
    );
    Ok(())
}

fn kubectl(kubeconfig: &str, context: &str, args: &[&str]) -> Result<String> {
    let output = Command::new("kubectl")
        .args([
            "--kubeconfig",
            kubeconfig,
            "--context",
            context,
            "--request-timeout=15s",
        ])
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
    let pods: Value = serde_json::from_str(&kubectl(
        kubeconfig,
        context,
        &[
            "-n",
            "default",
            "get",
            "pod",
            "-l",
            &format!(
                "operator.kuberic.io/set-name=kvstore2,operator.kuberic.io/replica-id={replica_id}"
            ),
            "-o",
            "json",
        ],
    )?)?;
    let items = pods["items"].as_array().context("replica Pod list")?;
    ensure!(
        items.len() == 1,
        "expected one exact kvstore2 replica {replica_id} Pod"
    );
    items[0]["metadata"]["name"]
        .as_str()
        .map(str::to_string)
        .context("replica Pod name")
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
    let containers = String::from_utf8(container.stdout)?;
    let Some(container_id) = containers.lines().find(|line| !line.trim().is_empty()) else {
        return Ok(());
    };
    ensure!(
        containers
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count()
            == 1,
        "expected one exact replica container, observed {containers:?}"
    );
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

fn kill_replica_process(kubeconfig: &str, context: &str, replica_id: i64) -> Result<()> {
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
        .context("resolving exact replica container for forced restart")?;
    ensure!(
        container.status.success(),
        "cannot resolve replica {replica_id} container: {}",
        String::from_utf8_lossy(&container.stderr)
    );
    let containers = String::from_utf8(container.stdout)?;
    let container_id = containers
        .lines()
        .find(|line| !line.trim().is_empty())
        .context("running replica container")?;
    ensure!(
        containers
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count()
            == 1,
        "expected one exact replica container, observed {containers:?}"
    );
    let inspection = Command::new("docker")
        .args(["exec", &node, "crictl", "inspect", container_id])
        .output()
        .context("inspecting exact replica container for forced restart")?;
    ensure!(
        inspection.status.success(),
        "cannot inspect replica {replica_id} container: {}",
        String::from_utf8_lossy(&inspection.stderr)
    );
    let inspection: Value = serde_json::from_slice(&inspection.stdout)?;
    let pid = inspection["info"]["pid"]
        .as_i64()
        .context("exact replica host PID")?;
    let killed = Command::new("docker")
        .args(["exec", &node, "kill", "-KILL", &pid.to_string()])
        .output()
        .context("force-restarting exact replica process")?;
    ensure!(
        killed.status.success(),
        "cannot force-restart replica {replica_id}: {}",
        String::from_utf8_lossy(&killed.stderr)
    );
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
    partition_replica_traffic(kubeconfig, context, cluster, replica_id, false)
}

fn partition_replica_traffic(
    kubeconfig: &str,
    context: &str,
    cluster: &str,
    replica_id: i64,
    replication_only: bool,
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
            let mut rule = vec![direction.to_string(), pod_ip.clone()];
            if replication_only {
                rule.extend(["-p", "tcp", "--dport", "50052"].map(str::to_string));
            }
            rule.extend(["-j", "DROP"].map(str::to_string));
            let output = Command::new("docker")
                .args(["exec", node, "iptables", "-I", "FORWARD"])
                .args(&rule)
                .output()
                .context("installing replica network partition")?;
            if !output.status.success() {
                bail!(
                    "cannot partition replica {replica_id} on {node}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            partition.rules.push((node.to_string(), rule));
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

struct OwnedChild(Child);

impl Drop for OwnedChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct KubeWatch {
    _process: OwnedChild,
    events: Receiver<Value>,
    initial: Value,
}

impl KubeWatch {
    fn start(cluster: &SwitchoverCluster, resource: &str, name: &str) -> Result<Self> {
        let mut process = OwnedChild(
            Command::new("kubectl")
                .args([
                    "--kubeconfig",
                    &cluster.kubeconfig,
                    "--context",
                    &cluster.context,
                ])
                .args([
                    "-n",
                    "default",
                    "get",
                    resource,
                    name,
                    "--watch",
                    "--output-watch-events",
                    "-o",
                    "json",
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()?,
        );
        let stdout = process.0.stdout.take().context("watch stdout")?;
        let (send, events) = channel();
        std::thread::spawn(move || {
            for event in serde_json::Deserializer::from_reader(stdout).into_iter::<Value>() {
                let Ok(event) = event else { break };
                if send.send(event).is_err() {
                    break;
                }
            }
        });
        let initial = events
            .recv_timeout(Duration::from_secs(10))
            .context("watch did not establish its initial resource version")?["object"]
            .clone();
        ensure!(!initial.is_null(), "watch omitted initial object");
        Ok(Self {
            _process: process,
            events,
            initial,
        })
    }
}

fn retry_retained_io<T>(
    deadline: Instant,
    operation: &str,
    mut attempt: impl FnMut(Duration) -> io::Result<T>,
) -> io::Result<T> {
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("deadline exceeded waiting for retained-client {operation}"),
            ));
        }
        match attempt(remaining) {
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) =>
            {
                std::thread::sleep(
                    Duration::from_millis(10)
                        .min(deadline.saturating_duration_since(Instant::now())),
                );
            }
            result => return result,
        }
    }
}

// Retry below BufReader/write_all so partial HTTP messages are never replayed.
struct RetainedIo<T> {
    inner: T,
    deadline: Instant,
}

impl<T: Read> Read for RetainedIo<T> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        retry_retained_io(self.deadline, "read", |_| self.inner.read(buffer))
    }
}

impl<T: Write> Write for RetainedIo<T> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        retry_retained_io(self.deadline, "write", |_| self.inner.write(buffer))
    }

    fn flush(&mut self) -> io::Result<()> {
        retry_retained_io(self.deadline, "flush", |_| self.inner.flush())
    }
}

// Keep the same TCP connection and exact Pod endpoint across authority movement.
// A Service client alone cannot exercise retained former-primary connections.
struct DirectClient {
    _forward: OwnedChild,
    stream: BufReader<RetainedIo<TcpStream>>,
}

impl DirectClient {
    fn connect(cluster: &SwitchoverCluster, pod: &str, deadline: Instant) -> Result<Self> {
        Self::connect_port(cluster, pod, 8080, deadline)
    }

    fn connect_port(
        cluster: &SwitchoverCluster,
        pod: &str,
        remote_port: u16,
        deadline: Instant,
    ) -> Result<Self> {
        let port_mapping = format!(":{remote_port}");
        let mut forward = OwnedChild(
            Command::new("kubectl")
                .args([
                    "--kubeconfig",
                    &cluster.kubeconfig,
                    "--context",
                    &cluster.context,
                ])
                .args(["-n", "default", "port-forward", "--address=127.0.0.1", pod])
                .arg(port_mapping)
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()?,
        );
        let stdout = forward.0.stdout.take().context("port-forward stdout")?;
        let (send, receive) = channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Some(address) = line
                    .strip_prefix("Forwarding from ")
                    .and_then(|line| line.split_once(" -> ").map(|(address, _)| address))
                {
                    let _ = send.send(address.to_string());
                }
            }
        });
        let address = receive
            .recv_timeout(
                Duration::from_secs(10).min(deadline.saturating_duration_since(Instant::now())),
            )?
            .parse()?;
        let stream = retry_retained_io(deadline, "connect", |remaining| {
            TcpStream::connect_timeout(&address, remaining)
        })?;
        stream.set_nonblocking(true)?;
        Ok(Self {
            _forward: forward,
            stream: BufReader::new(RetainedIo {
                inner: stream,
                deadline,
            }),
        })
    }

    fn request(&mut self, method: &str, path: &str, body: &str) -> Result<(u16, String)> {
        request_http(&mut self.stream, method, path, body)
    }

    fn report(&mut self) -> Result<Value> {
        let (code, body) = self.request("GET", "/status", "")?;
        ensure!(
            code == 200,
            "replica diagnostics returned HTTP {code}: {body}"
        );
        Ok(serde_json::from_str(&body)?)
    }
}

fn request_http<T: Read + Write>(
    stream: &mut BufReader<RetainedIo<T>>,
    method: &str,
    path: &str,
    body: &str,
) -> Result<(u16, String)> {
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.get_mut().write_all(request.as_bytes())?;
    stream.get_mut().flush()?;
    read_http_response(stream).with_context(|| format!("{method} {path} response"))
}

fn read_http_line(reader: &mut impl BufRead, budget: &mut usize) -> Result<String> {
    let mut line = Vec::new();
    reader
        .take((*budget + 1) as u64)
        .read_until(b'\n', &mut line)?;
    ensure!(line.len() <= *budget, "unexpectedly large HTTP metadata");
    *budget -= line.len();
    ensure!(line.ends_with(b"\r\n"), "truncated or malformed HTTP line");
    line.truncate(line.len() - 2);
    Ok(String::from_utf8(line)?)
}

fn http_header(line: &str) -> Result<(&str, &str)> {
    let (name, value) = line.split_once(':').context("malformed HTTP header")?;
    ensure!(
        !name.is_empty()
            && name
                .bytes()
                .all(|byte| { byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte) })
            && value
                .bytes()
                .all(|byte| byte == b'\t' || !byte.is_ascii_control()),
        "malformed HTTP header"
    );
    Ok((name, value.trim_matches([' ', '\t'])))
}

fn read_http_response(reader: &mut impl BufRead) -> Result<(u16, String)> {
    const MAX_BODY: usize = 1024 * 1024;
    let mut metadata_budget = 64 * 1024;
    if reader.fill_buf()?.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotConnected,
            "retained connection closed before HTTP response",
        )
        .into());
    }
    let (code, length, chunked) = loop {
        let line = read_http_line(reader, &mut metadata_budget)?;
        let mut status = line.splitn(3, ' ');
        ensure!(
            matches!(status.next(), Some("HTTP/1.1" | "HTTP/1.0")),
            "invalid HTTP version"
        );
        let code = status.next().context("HTTP status")?;
        ensure!(
            code.len() == 3 && code.bytes().all(|byte| byte.is_ascii_digit()),
            "invalid HTTP status"
        );
        let code: u16 = code.parse()?;
        ensure!((100..600).contains(&code), "invalid HTTP status");
        ensure!(
            status.next().is_some_and(|reason| reason
                .bytes()
                .all(|byte| byte == b'\t' || !byte.is_ascii_control())),
            "malformed HTTP reason phrase"
        );
        let mut length = None;
        let mut chunked = false;
        loop {
            let line = read_http_line(reader, &mut metadata_budget)?;
            if line.is_empty() {
                break;
            }
            let (name, value) = http_header(&line)?;
            if name.eq_ignore_ascii_case("content-length") {
                ensure!(
                    length.is_none()
                        && !value.is_empty()
                        && value.bytes().all(|byte| byte.is_ascii_digit()),
                    "invalid or duplicate Content-Length"
                );
                length = Some(value.parse::<usize>()?);
            } else if name.eq_ignore_ascii_case("transfer-encoding") {
                ensure!(
                    !chunked && value.eq_ignore_ascii_case("chunked"),
                    "unsupported or duplicate Transfer-Encoding"
                );
                chunked = true;
            }
        }
        ensure!(!(chunked && length.is_some()), "conflicting HTTP framing");
        ensure!(code != 101, "unexpected HTTP protocol upgrade");
        if code >= 200 {
            break (code, length, chunked);
        }
        ensure!(
            length.is_none() && !chunked,
            "framed informational response"
        );
    };
    if matches!(code, 204 | 304) {
        ensure!(
            code != 204 || (length.is_none() && !chunked),
            "framed HTTP 204 response"
        );
        return Ok((code, String::new()));
    }
    if !chunked {
        let length = length.context("expected Content-Length or chunked response")?;
        ensure!(length <= MAX_BODY, "unexpectedly large HTTP response");
        let mut body = vec![0; length];
        reader.read_exact(&mut body)?;
        return Ok((code, String::from_utf8(body)?));
    }
    let mut body = Vec::new();
    loop {
        let line = read_http_line(reader, &mut metadata_budget)?;
        ensure!(
            line.bytes()
                .all(|byte| byte == b'\t' || !byte.is_ascii_control()),
            "malformed HTTP chunk metadata"
        );
        let size = line.split(';').next().unwrap();
        ensure!(
            !size.is_empty() && size.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "invalid HTTP chunk size"
        );
        let size = usize::from_str_radix(size, 16)?;
        if size == 0 {
            loop {
                let trailer = read_http_line(reader, &mut metadata_budget)?;
                if trailer.is_empty() {
                    break;
                }
                let (name, _) = http_header(&trailer)?;
                ensure!(
                    !name.eq_ignore_ascii_case("content-length")
                        && !name.eq_ignore_ascii_case("transfer-encoding"),
                    "HTTP trailer changes response framing"
                );
            }
            break;
        }
        ensure!(
            size <= MAX_BODY - body.len(),
            "unexpectedly large HTTP response"
        );
        let start = body.len();
        body.resize(start + size, 0);
        reader.read_exact(&mut body[start..])?;
        let mut end = [0; 2];
        reader.read_exact(&mut end)?;
        ensure!(end == *b"\r\n", "malformed HTTP chunk terminator");
    }
    Ok((code, String::from_utf8(body)?))
}

fn check_deleted_target_response(response: Result<(u16, String)>) -> Result<()> {
    match response {
        Ok((503, _)) => Ok(()),
        Err(error)
            if error.downcast_ref::<io::Error>().is_some_and(|error| {
                matches!(
                    error.kind(),
                    io::ErrorKind::NotConnected
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::BrokenPipe
                )
            }) =>
        {
            Ok(())
        }

        Err(error) => Err(error.context("deleted target did not explicitly reject or disconnect")),
        Ok((code, body)) => bail!("unexpected deleted-target response: HTTP {code} {body}"),
    }
}

fn check_old_session_response(response: Result<(u16, String)>) -> Result<()> {
    check_deleted_target_response(response)
        .context("old target process connection did not explicitly reject or disconnect")
}

fn retains_acknowledged_prefix(
    report: &Value,
    member: &Value,
    configuration: &Value,
    committed: i64,
) -> bool {
    identity(report) == identity(member)
        && report["currentProgress"]
            .as_i64()
            .is_some_and(|lsn| lsn >= committed)
        && report["currentConfiguration"] == *configuration
        && report["previousConfiguration"].is_null()
        && report["pendingOperation"].is_null()
}

#[derive(Clone)]
struct SwitchoverCluster {
    kubeconfig: String,
    context: String,
    cluster: String,
}

impl SwitchoverCluster {
    fn command(&self) -> Command {
        let mut command = Command::new("timeout");
        command.args(["--kill-after=2s", "20s", "kubectl"]).args([
            "--kubeconfig",
            &self.kubeconfig,
            "--context",
            &self.context,
            "--request-timeout=10s",
        ]);
        command
    }

    fn kubectl(&self, args: &[&str]) -> Result<String> {
        let output = self.command().args(args).output()?;
        ensure!(
            output.status.success(),
            "kubectl {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(String::from_utf8(output.stdout)?)
    }

    fn status(&self) -> Result<Value> {
        Ok(serde_json::from_str(&self.kubectl(&[
            "-n",
            "default",
            "get",
            "kubericset",
            "kvstore2",
            "-o",
            "json",
        ])?)?)
    }

    fn ready(&self, deadline: Instant) -> Result<Value> {
        loop {
            let status = self.status()?;
            if status_ready(&status)
                && status["status"]["transition"].is_null()
                && status["status"]["provisioning"].is_null()
                && status["status"]["topology"]["members"]
                    .as_array()
                    .is_some_and(|m| m.len() == 3)
            {
                let mut converged = true;
                for member in status["status"]["topology"]["members"].as_array().unwrap() {
                    let report = self.report(member);
                    converged &= report.is_ok_and(|report| {
                        report["instanceId"] == member["instanceId"]
                            && report["agentGeneration"] == member["agentGeneration"]
                            && report["currentConfiguration"]
                                == status["status"]["topology"]["configurationId"]
                            && report["previousConfiguration"].is_null()
                            && report["pendingOperation"].is_null()
                    });
                }
                if converged {
                    return Ok(status);
                }
            }
            poll(deadline, "stable three-member kvstore2")?;
        }
    }

    fn pod(&self, identity: &Value) -> Result<String> {
        let pods: Value = serde_json::from_str(&self.kubectl(&[
            "-n",
            "default",
            "get",
            "pods",
            "-l",
            "operator.kuberic.io/set-name=kvstore2",
            "-o",
            "json",
        ])?)?;
        pods["items"]
            .as_array()
            .context("Pod list")?
            .iter()
            .find(|pod| pod["metadata"]["uid"] == identity["instanceId"])
            .and_then(|pod| pod["metadata"]["name"].as_str())
            .map(str::to_string)
            .context("exact accepted Pod incarnation is absent")
    }

    fn report(&self, identity: &Value) -> Result<Value> {
        let pod = self.pod(identity)?;
        Ok(serde_json::from_str(&self.kubectl(&[
            "-n",
            "default",
            "exec",
            &pod,
            "--",
            "curl",
            "--fail",
            "--silent",
            "--max-time",
            "3",
            "http://127.0.0.1:8080/status",
        ])?)?)
    }

    fn delete_exact_pod(&self, identity: &Value) -> Result<()> {
        let pod = self.pod(identity)?;
        eprintln!(
            "deleting exact {pod}: identity={identity}, agent/runtime={}",
            self.report(identity)?
        );
        eprintln!(
            "{}",
            self.kubectl(&[
                "-n",
                "default",
                "logs",
                &pod,
                "--all-containers",
                "--tail=100"
            ])?
        );
        let mut child = OwnedChild(
            self.command()
                .args([
                    "delete",
                    "--raw",
                    &format!("/api/v1/namespaces/default/pods/{pod}"),
                    "-f",
                    "-",
                ])
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()?,
        );
        child.0.stdin.take().context("delete stdin")?.write_all(
            json!({"apiVersion":"v1","kind":"DeleteOptions",
                "preconditions":{"uid":identity["instanceId"]},"gracePeriodSeconds":0})
            .to_string()
            .as_bytes(),
        )?;
        ensure!(
            child.0.wait()?.success(),
            "UID-preconditioned deletion failed for {pod}"
        );
        Ok(())
    }

    fn partition_replication(&self, replica: i64) -> Result<NetworkPartition> {
        partition_replica_traffic(
            &self.kubeconfig,
            &self.context,
            &self.cluster,
            replica,
            true,
        )
    }
}

fn poll(deadline: Instant, waiting_for: &str) -> Result<()> {
    ensure!(
        Instant::now() < deadline,
        "deadline exceeded waiting for {waiting_for}"
    );
    std::thread::sleep(Duration::from_millis(100));
    Ok(())
}

fn identity(member: &Value) -> Value {
    json!({"replicaId":member["replicaId"],"instanceId":member["instanceId"],"agentGeneration":member["agentGeneration"]})
}

fn routing_instance(service: &Value) -> Option<&str> {
    service["spec"]["selector"]["operator.kuberic.io/instance"].as_str()
}

fn validate_routed_service(service: &Value, primary: &Value) -> Result<()> {
    ensure!(
        service["metadata"]["name"] == "kvstore2-write",
        "unexpected routed Service: {service}"
    );
    ensure!(
        routing_instance(service) == primary["instanceId"].as_str(),
        "routing lost exact primary: {service}"
    );
    let ports = service["spec"]["ports"]
        .as_array()
        .context("routed Service ports")?;
    ensure!(
        ports.len() == 1
            && ports[0]["port"].as_i64() == Some(80)
            && ports[0]["targetPort"].as_i64() == Some(8080),
        "routed Service did not expose exact application targetPort: {ports:?}"
    );
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum RoutedWriteOutcome {
    Acknowledged,
    Retry,
}

fn transient_routed_exec_error(stderr: &str) -> bool {
    let stderr = stderr.trim();
    let upgrade = stderr.starts_with("error: unable to upgrade connection:")
        || stderr.starts_with("error: Internal error occurred: unable to upgrade connection:");
    let exec =
        stderr.starts_with("error: Internal error occurred: error executing command in container:");
    let backend = stderr.starts_with("Error from server: error dialing backend:")
        || stderr.starts_with("error: error upgrading connection:");
    (upgrade && stderr.contains("container not found ("))
        || ((upgrade || exec || stderr.starts_with("Error from server (BadRequest): container "))
            && (stderr.contains("container is not running")
                || stderr.ends_with(" is not running")
                || stderr.contains("container is in CONTAINER_EXITED state")))
        || ((upgrade || backend)
            && [
                "connect: connection refused",
                "connection reset by peer",
                "i/o timeout",
                "context deadline exceeded",
                "EOF",
            ]
            .iter()
            .any(|suffix| stderr.ends_with(suffix)))
        || stderr == "error: lost connection to pod"
}

fn classify_routed_write(
    exit_code: Option<i32>,
    stdout: &str,
    stderr: &str,
) -> Result<RoutedWriteOutcome> {
    match stdout {
        "200" if exit_code == Some(0) => return Ok(RoutedWriteOutcome::Acknowledged),
        "503" if exit_code == Some(0) => return Ok(RoutedWriteOutcome::Retry),
        // A failed transfer can still report its HTTP status. Never hide a 500
        // (or malformed output) behind an otherwise retryable transport error.
        "" | "000" | "200" | "503" if exit_code != Some(0) => {}
        _ => bail!("unexpected routed HTTP status/output {stdout:?}"),
    }
    let curl_transport = exit_code.is_some_and(|code| {
        matches!(code, 7 | 28 | 52 | 56)
            && stderr
                .lines()
                .any(|line| line.starts_with(&format!("curl: ({code}) ")))
    });
    ensure!(
        curl_transport || (exit_code == Some(1) && transient_routed_exec_error(stderr)),
        "unrecognized routed command failure: exit={exit_code:?}, stdout={stdout:?}, stderr={stderr:?}"
    );
    Ok(RoutedWriteOutcome::Retry)
}

fn wait_routed_write(deadline: Instant, mut attempt: impl FnMut() -> Result<Output>) -> Result<()> {
    let mut last = "no attempt made".to_string();
    loop {
        ensure!(
            Instant::now() < deadline,
            "deadline exceeded waiting for routed Service write; last {last}"
        );
        let output = attempt().with_context(|| format!("routed Service write; last {last}"))?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        last = format!(
            "exit={}, stdout={stdout:?}, stderr={stderr:?}",
            output.status
        );
        ensure!(
            Instant::now() < deadline,
            "deadline exceeded waiting for routed Service write; last {last}"
        );
        match classify_routed_write(output.status.code(), &stdout, &stderr)
            .with_context(|| format!("routed Service write; last {last}"))?
        {
            RoutedWriteOutcome::Acknowledged => return Ok(()),
            RoutedWriteOutcome::Retry => std::thread::sleep(
                Duration::from_millis(100).min(deadline.saturating_duration_since(Instant::now())),
            ),
        }
    }
}

fn routed_kubectl(cluster: &SwitchoverCluster, deadline: Instant, args: &[&str]) -> Result<Output> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    ensure!(
        !remaining.is_zero(),
        "routed Service deadline expired before kubectl {args:?}"
    );
    // Bound exec and API discovery by the scenario deadline, not a fresh timeout.
    let attempt_timeout = remaining.min(Duration::from_secs(15));
    Command::new("timeout")
        .args([
            "--kill-after=2s",
            &format!("{}s", attempt_timeout.as_secs_f64()),
            "kubectl",
        ])
        .args([
            "--kubeconfig",
            &cluster.kubeconfig,
            "--context",
            &cluster.context,
            "--request-timeout=10s",
        ])
        .args(args)
        .output()
        .with_context(|| format!("routed kubectl {args:?}"))
}

fn transient_routed_lookup_failure(output: &Output) -> bool {
    if output.status.success() {
        return false;
    }
    if output.status.code() == Some(124) {
        return true;
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    [
        "Unable to connect to the server",
        "The connection to the server",
        "context deadline exceeded",
        "i/o timeout",
        "TLS handshake timeout",
        "connection refused",
        "connection reset by peer",
        "EOF",
        "ServiceUnavailable",
        "Too Many Requests",
    ]
    .iter()
    .any(|transient| stderr.contains(transient))
}

fn require_routed_lookup(output: Output, resource: &str) -> Result<Option<Output>> {
    if output.status.success() {
        return Ok(Some(output));
    }
    if transient_routed_lookup_failure(&output) {
        return Ok(None);
    }
    bail!(
        "nonretryable routed Service {resource} lookup failed: exit={:?}, stdout={:?}, stderr={:?}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn routed_service_client_pod(
    cluster: &SwitchoverCluster,
    primary: &Value,
    deadline: Instant,
) -> Result<Option<String>> {
    let service = routed_kubectl(
        cluster,
        deadline,
        &[
            "-n",
            "default",
            "get",
            "service",
            "kvstore2-write",
            "-o",
            "json",
        ],
    )?;
    let Some(service) = require_routed_lookup(service, "Service")? else {
        return Ok(None);
    };
    let service: Value = serde_json::from_slice(&service.stdout)?;
    if routing_instance(&service) == Some("disabled") {
        return Ok(None);
    }
    validate_routed_service(&service, primary)?;
    let endpoints = routed_kubectl(
        cluster,
        deadline,
        &[
            "-n",
            "default",
            "get",
            "endpoints",
            "kvstore2-write",
            "-o",
            "json",
        ],
    )?;
    let Some(endpoints) = require_routed_lookup(endpoints, "Endpoints")? else {
        return Ok(None);
    };
    let endpoints: Value = serde_json::from_slice(&endpoints.stdout)?;
    let ready = endpoints["subsets"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|subset| {
            subset["addresses"]
                .as_array()
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
        })
        .filter(|address| address["targetRef"]["uid"] == primary["instanceId"])
        .count();
    let not_ready = endpoints["subsets"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|subset| {
            subset["notReadyAddresses"]
                .as_array()
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
        })
        .filter(|address| address["targetRef"]["uid"] == primary["instanceId"])
        .count();
    if ready != 1 || not_ready != 0 {
        return Ok(None);
    }
    let pods = routed_kubectl(
        cluster,
        deadline,
        &[
            "-n",
            "default",
            "get",
            "pods",
            "-l",
            "operator.kuberic.io/set-name=kvstore2",
            "-o",
            "json",
        ],
    )?;
    let Some(pods) = require_routed_lookup(pods, "Pod")? else {
        return Ok(None);
    };
    let pods: Value = serde_json::from_slice(&pods.stdout)?;
    Ok(pods["items"]
        .as_array()
        .context("routed primary Pod list")?
        .iter()
        .find(|pod| pod["metadata"]["uid"] == primary["instanceId"])
        .and_then(|pod| pod["metadata"]["name"].as_str())
        .map(str::to_string))
}

fn assert_routed_service_write(
    cluster: &SwitchoverCluster,
    primary: &Value,
    key: &str,
    value: &str,
    deadline: Instant,
) -> Result<()> {
    wait_routed_write(deadline, || {
        let Some(pod) = routed_service_client_pod(cluster, primary, deadline)? else {
            return Ok(routed_test_output(
                7,
                "000",
                "curl: (7) routed Service endpoint unavailable",
            ));
        };
        routed_kubectl(
            cluster,
            deadline,
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
                "5",
                "-o",
                "/dev/null",
                "-w",
                "%{http_code}",
                "-X",
                "PUT",
                "--data-binary",
                value,
                &format!("http://kvstore2-write/kv/{key}"),
            ],
        )
    })
}

fn assert_routed_service_read(
    cluster: &SwitchoverCluster,
    primary: &Value,
    key: &str,
    expected: &str,
    deadline: Instant,
) -> Result<()> {
    let mut last = "no attempt made".to_string();
    loop {
        ensure!(
            Instant::now() < deadline,
            "deadline exceeded waiting for routed Service read; last {last}"
        );
        let Some(pod) = routed_service_client_pod(cluster, primary, deadline)? else {
            last = "Service shape or exact endpoint not ready".into();
            std::thread::sleep(Duration::from_millis(100));
            continue;
        };
        let service = routed_kubectl(
            cluster,
            deadline,
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
                "5",
                "-w",
                "\n%{http_code}",
                &format!("http://kvstore2-write/kv/{key}"),
            ],
        )?;
        let stdout = String::from_utf8_lossy(&service.stdout);
        let stderr = String::from_utf8_lossy(&service.stderr);
        last = format!(
            "exit={}, stdout={stdout:?}, stderr={stderr:?}",
            service.status
        );
        if service.status.success() {
            let (body, status) = stdout
                .rsplit_once('\n')
                .with_context(|| format!("routed Service read omitted HTTP status; {last}"))?;
            match status {
                "200" => {
                    ensure!(
                        body == expected,
                        "routed Service returned wrong value for {key}: expected {expected:?}, got {body:?}"
                    );
                    return Ok(());
                }
                "503" => {}
                _ => bail!("unexpected routed Service read status {status:?}; {last}"),
            }
        } else {
            let status = stdout.rsplit_once('\n').map_or("", |(_, status)| status);
            classify_routed_write(service.status.code(), status, &stderr)
                .with_context(|| format!("routed Service read; last {last}"))?;
        }
        std::thread::sleep(
            Duration::from_millis(100).min(deadline.saturating_duration_since(Instant::now())),
        );
    }
}

fn check_receipt(
    start: &Value,
    completed: &Value,
    request: &str,
    target: &Value,
    outcome: &str,
) -> Result<()> {
    let receipt = &completed["status"]["lastSwitchover"];
    ensure!(
        receipt["requestId"] == request && receipt["outcome"] == outcome,
        "unexpected receipt: {receipt}"
    );
    ensure!(
        receipt["acceptedTarget"] == identity(target),
        "receipt changed exact requested target"
    );
    ensure!(
        receipt["requestedTargetReplicaId"] == target["replicaId"],
        "receipt changed logical target"
    );
    let before = &start["status"]["topology"];
    let after = &completed["status"]["topology"];
    ensure!(
        before["members"].as_array().map(Vec::len) == after["members"].as_array().map(Vec::len)
            && before["writeQuorum"] == after["writeQuorum"],
        "switchover changed membership cardinality or quorum policy"
    );
    ensure!(
        before["epoch"]["dataLossNumber"] == after["epoch"]["dataLossNumber"],
        "switchover changed data-loss authority"
    );
    let source = before["members"]
        .as_array()
        .context("starting members")?
        .iter()
        .find(|member| member["role"] == "primary")
        .context("starting primary")?;
    let expected = if outcome == "requestedTargetCompleted" {
        target
    } else {
        source
    };
    ensure!(
        receipt["resultingPrimary"] == identity(expected)
            && topology_primary_id(completed) == expected["replicaId"].as_i64(),
        "wrong resulting primary"
    );
    let old_epoch = before["epoch"]["configurationNumber"]
        .as_i64()
        .context("starting epoch")?;
    let new_epoch = after["epoch"]["configurationNumber"]
        .as_i64()
        .context("completion epoch")?;
    ensure!(
        match outcome {
            "requestedTargetCompleted" => new_epoch == old_epoch + 1,
            "oldPrimaryRestored" => new_epoch == old_epoch && before == after,
            "oldPrimaryCompensated" => new_epoch > old_epoch + 1,
            _ => false,
        },
        "unexpected {outcome} configuration epoch {old_epoch} -> {new_epoch}"
    );
    for member in before["members"].as_array().unwrap() {
        ensure!(
            after["members"]
                .as_array()
                .context("completed members")?
                .iter()
                .any(|other| identity(other) == identity(member)),
            "handoff changed exact membership"
        );
    }
    ensure!(
        completed["status"]["transition"].is_null(),
        "terminal receipt left an active transition"
    );
    Ok(())
}

struct SwitchoverCase<'a> {
    cluster: &'a SwitchoverCluster,
    start: Value,
    source: Value,
    target: Value,
    witness: Value,
    source_client: DirectClient,
    target_client: DirectClient,
    witness_client: DirectClient,
    routing: KubeWatch,
    status: KubeWatch,
    request: String,
    acknowledged: Vec<(String, String)>,
    receipt: Option<Value>,
    routing_removed: bool,
    source_closed: bool,
    started: Instant,
    deadline: Instant,
}

impl<'a> SwitchoverCase<'a> {
    fn new(cluster: &'a SwitchoverCluster, name: &str) -> Result<Self> {
        let start = cluster.ready(Instant::now() + Duration::from_secs(120))?;
        let started = Instant::now();
        let deadline = started + Duration::from_secs(270);
        let members = start["status"]["topology"]["members"]
            .as_array()
            .context("topology")?;
        let source = members
            .iter()
            .find(|m| m["role"] == "primary")
            .context("primary")?
            .clone();
        let mut secondaries = members.iter().filter(|m| m["role"] == "activeSecondary");
        let target = secondaries.next().context("target secondary")?.clone();
        let witness = secondaries.next().context("witness secondary")?.clone();
        let source_client = DirectClient::connect(cluster, &cluster.pod(&source)?, deadline)?;
        let target_client = DirectClient::connect(cluster, &cluster.pod(&target)?, deadline)?;
        let witness_client = DirectClient::connect(cluster, &cluster.pod(&witness)?, deadline)?;
        let routing = KubeWatch::start(cluster, "service", "kvstore2-write")?;
        ensure!(
            routing_instance(&routing.initial) == source["instanceId"].as_str(),
            "starting routing is not the exact primary"
        );
        let status = KubeWatch::start(cluster, "kubericset", "kvstore2")?;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let mut case = Self {
            cluster,
            start,
            source,
            target,
            witness,
            source_client,
            target_client,
            witness_client,
            routing,
            status,
            request: format!("live-{name}-{nonce}"),
            acknowledged: Vec::new(),
            receipt: None,
            routing_removed: false,
            source_closed: false,
            started,
            deadline,
        };
        for index in 0..3 {
            case.acknowledge(&format!("seed-{index}"))?;
        }
        case.wait_replicated()?;
        ensure!(
            case.target_client
                .request("PUT", "/kv/not-primary", "rejected")?
                .0
                == 503,
            "target was writable before request"
        );
        Ok(case)
    }

    fn acknowledge(&mut self, suffix: &str) -> Result<i64> {
        let key = format!("{}-{suffix}", self.request);
        let value = format!("acknowledged-{suffix}");
        loop {
            let (code, body) = self
                .source_client
                .request("PUT", &format!("/kv/{key}"), &value)?;
            match code {
                200 => {
                    self.acknowledged.push((key, value));
                    return Ok(body.parse()?);
                }
                503 if body.contains("ReconfigurationPending") => {
                    poll(self.deadline, "source write access before switchover")?;
                }
                _ => bail!("source write failed: HTTP {code} {body}"),
            }
        }
    }

    fn wait_replicated(&mut self) -> Result<()> {
        let committed = self.source_client.report()?["committedLsn"]
            .as_i64()
            .context("committed LSN")?;
        loop {
            let target = self.target_client.report()?;
            let witness = self.witness_client.report()?;
            // Followers learn committed LSNs on later replication items. Their
            // durable applied prefix, not that lagging watermark, covers the writes.
            if [(&target, &self.target), (&witness, &self.witness)]
                .iter()
                .all(|(report, member)| {
                    retains_acknowledged_prefix(
                        report,
                        member,
                        &self.start["status"]["topology"]["configurationId"],
                        committed,
                    )
                })
            {
                return Ok(());
            }
            poll(
                self.deadline,
                "all exact members to retain acknowledged prefix",
            )?;
        }
    }

    fn submit(&self) -> Result<()> {
        self.cluster.kubectl(&[
            "-n", "default", "patch", "kubericset", "kvstore2", "--type=merge", "-p",
            &json!({"spec":{"switchover":{"requestId":self.request,"targetReplicaId":self.target["replicaId"]}}}).to_string(),
        ])?;
        Ok(())
    }

    fn observe(&mut self) -> Result<Value> {
        for event in self.routing.events.try_iter() {
            ensure!(event["type"] != "ERROR", "routing watch failed: {event}");
            let service = &event["object"];
            let routed =
                routing_instance(service).context("routing event omitted exact selector")?;
            self.routing_removed |= routed == "disabled";
            ensure!(
                routed == "disabled"
                    || Some(routed) == self.source["instanceId"].as_str()
                    || Some(routed) == self.target["instanceId"].as_str(),
                "unexpected routed incarnation {routed}"
            );
        }
        for event in self.status.events.try_iter() {
            ensure!(event["type"] != "ERROR", "status watch failed: {event}");
            let status = &event["object"];
            let intent = &status["status"]["transition"]["switchover"];
            if intent["requestId"] == self.request {
                ensure!(
                    intent["source"] == identity(&self.source)
                        && intent["target"] == identity(&self.target),
                    "accepted switchover changed frozen identities: {intent}"
                );
            }
            if self.receipt.is_none()
                && status["status"]["lastSwitchover"]["requestId"] == self.request
            {
                self.receipt = Some(status.clone());
            }
        }
        self.cluster.status()
    }

    fn assert_closed(&mut self) -> Result<()> {
        ensure!(
            self.source_client
                .request("PUT", "/kv/stale-source", "must-not-commit")?
                .0
                == 503,
            "revoked source accepted a retained-client write"
        );
        ensure!(
            self.target_client
                .request("PUT", "/kv/premature-target", "must-not-commit")?
                .0
                == 503,
            "unaccepted target accepted a retained-client write"
        );
        self.source_closed = true;
        Ok(())
    }

    fn wait_prepared(&mut self) -> Result<Value> {
        loop {
            let status = self.observe()?;
            let intent = &status["status"]["transition"]["switchover"];
            ensure!(
                self.receipt.is_none(),
                "handoff completed before the pre-admission fault boundary"
            );
            if intent["requestId"] == self.request
                && intent["handoff"].is_object()
                && self.routing_removed
            {
                for client in [
                    &mut self.source_client,
                    &mut self.target_client,
                    &mut self.witness_client,
                ] {
                    ensure!(
                        client.report()?["currentConfiguration"]
                            == self.start["status"]["topology"]["configurationId"],
                        "authority already admitted at pre-admission boundary"
                    );
                }
                ensure!(
                    self.target_client.report()?["currentProgress"]
                        .as_i64()
                        .context("target progress")?
                        < intent["handoff"]["handoffLsn"]
                            .as_i64()
                            .context("handoff LSN")?,
                    "target was not held behind handoff"
                );
                self.assert_closed()?;
                eprintln!("{}: pre-admission frozen handoff={intent}", self.request);
                return Ok(intent.clone());
            }
            poll(self.deadline, "durable handoff before authority admission")?;
        }
    }

    fn wait_admitted(&mut self) -> Result<()> {
        loop {
            let status = self.observe()?;
            ensure!(
                self.receipt.is_none(),
                "target completed before post-admission fault"
            );
            let requested = &status["status"]["transition"]["switchover"]["requestedConfiguration"];
            let source = self.source_client.report()?;
            if !requested.is_null()
                && source["currentConfiguration"] == requested["configurationId"]
                && source["role"] == "ActiveSecondary"
            {
                ensure!(
                    self.routing_removed,
                    "requested authority preceded routing removal"
                );
                self.assert_closed()?;
                eprintln!(
                    "{}: post-admission source={source}; intent={}",
                    self.request, status["status"]["transition"]
                );
                return Ok(());
            }
            poll(
                self.deadline,
                "old-primary demotion under admitted requested authority",
            )?;
        }
    }

    fn finish(&mut self, outcome: &str, target_absent: bool) -> Result<()> {
        loop {
            let status = self.observe()?;
            if !target_absent {
                self.probe_handoff_clients()?;
            }
            if let Some(completed) = &self.receipt {
                check_receipt(&self.start, completed, &self.request, &self.target, outcome)?;
                let resulting = if outcome == "requestedTargetCompleted" {
                    &self.target
                } else {
                    &self.source
                };
                let service: Value = serde_json::from_str(&self.cluster.kubectl(&[
                    "-n",
                    "default",
                    "get",
                    "service",
                    "kvstore2-write",
                    "-o",
                    "json",
                ])?)?;
                let replaced = !target_absent
                    || status["status"]["topology"]["members"]
                        .as_array()
                        .is_some_and(|members| {
                            members.iter().any(|member| {
                                member["replicaId"] == self.target["replicaId"]
                                    && member["instanceId"] != self.target["instanceId"]
                            })
                        });
                if status_ready(&status)
                    && replaced
                    && status["status"]["transition"].is_null()
                    && status["status"]["provisioning"].is_null()
                    && routing_instance(&service) == resulting["instanceId"].as_str()
                {
                    break;
                }
            }
            poll(self.deadline, "terminal receipt, write grant and routing")?;
        }
        ensure!(self.routing_removed, "never observed routing removal");
        if !target_absent {
            ensure!(
                self.source_closed,
                "never observed a direct-client write interruption"
            );
        }
        let status = self.cluster.ready(self.deadline)?;
        let config = &status["status"]["topology"]["configurationId"];
        let selected = if outcome == "requestedTargetCompleted" {
            &self.target
        } else {
            &self.source
        };
        let mut writer =
            DirectClient::connect(self.cluster, &self.cluster.pod(selected)?, self.deadline)?;
        // Every survivor must finish current-only, including after replacement of a lost target.
        for member in status["status"]["topology"]["members"]
            .as_array()
            .context("members")?
        {
            let mut client =
                DirectClient::connect(self.cluster, &self.cluster.pod(member)?, self.deadline)?;
            let report = client.report()?;
            ensure!(
                report["instanceId"] == member["instanceId"]
                    && report["agentGeneration"] == member["agentGeneration"]
                    && report["currentConfiguration"] == *config
                    && report["previousConfiguration"].is_null()
                    && report["pendingOperation"].is_null(),
                "member did not finish exact current-only authority: {report}"
            );
            let primary = member["replicaId"] == selected["replicaId"];
            ensure!(
                (report["writeStatus"] == "Granted") == primary,
                "not exactly one write grant: {report}"
            );
            ensure!(
                report["role"]
                    == if primary {
                        "Primary"
                    } else {
                        "ActiveSecondary"
                    },
                "incorrect application role: {report}"
            );
        }
        for (key, expected) in &self.acknowledged {
            let (code, value) = writer.request("GET", &format!("/kv/{key}"), "")?;
            ensure!(
                code == 200 && &value == expected,
                "lost acknowledged write {key}: {code} {value}"
            );
        }
        if outcome == "requestedTargetCompleted" {
            for _ in 0..3 {
                ensure!(
                    self.source_client
                        .request("PUT", "/kv/stale-former-primary", "forbidden")?
                        .0
                        == 503,
                    "retained former-primary connection committed a write"
                );
            }
        } else if target_absent {
            check_deleted_target_response(self.target_client.request(
                "PUT",
                "/kv/deleted-target",
                "forbidden",
            ))?;
        }
        ensure!(
            self.witness_client
                .request("PUT", "/kv/stale-witness", "forbidden")?
                .0
                == 503,
            "retained witness client committed a write"
        );
        let pod = self.cluster.pod(&self.witness)?;
        let key = format!("{}-service", self.request);
        self.cluster.kubectl(&[
            "-n",
            "default",
            "exec",
            &pod,
            "--",
            "curl",
            "--fail",
            "--silent",
            "--show-error",
            "--max-time",
            "5",
            "-X",
            "PUT",
            "--data-binary",
            "through-write-service",
            &format!("http://kvstore2-write/kv/{key}"),
        ])?;
        ensure!(
            writer.request("GET", &format!("/kv/{key}"), "")?
                == (200, "through-write-service".into()),
            "new Service write was not readable"
        );
        self.acknowledged
            .push((key, "through-write-service".into()));
        ensure!(
            Instant::now() < self.deadline,
            "after-ready scenario exceeded its 270-second budget"
        );
        eprintln!(
            "{}: outcome={outcome}, preserved={} writes, after-ready elapsed={:?}",
            self.request,
            self.acknowledged.len(),
            self.started.elapsed()
        );
        Ok(())
    }

    fn probe_handoff_clients(&mut self) -> Result<()> {
        let source = self.source_client.report()?;
        self.source_closed |= source["writeStatus"] != "Granted";
        let key = format!("{}-in-flight-{}", self.request, self.acknowledged.len());
        let (code, _) =
            self.source_client
                .request("PUT", &format!("/kv/{key}"), "during-handoff")?;
        if code == 200 {
            ensure!(
                !self.source_closed,
                "retained source write succeeded after observed revocation"
            );
            self.acknowledged.push((key, "during-handoff".into()));
        } else {
            ensure!(code == 503, "unexpected source write status {code}");
            self.source_closed = true;
        }
        let key = format!("{}-target-{}", self.request, self.acknowledged.len());
        let (code, _) =
            self.target_client
                .request("PUT", &format!("/kv/{key}"), "target-client")?;
        if code == 200 {
            let accepted = self.cluster.status()?;
            ensure!(
                accepted["status"]["lastSwitchover"]["requestId"] == self.request
                    && accepted["status"]["lastSwitchover"]["outcome"]
                        == "requestedTargetCompleted"
                    && topology_primary_id(&accepted) == self.target["replicaId"].as_i64(),
                "target committed before its authority was accepted"
            );
            self.acknowledged.push((key, "target-client".into()));
        } else {
            ensure!(code == 503, "unexpected target write status {code}");
        }
        Ok(())
    }
}

fn run_switchover(test: impl FnOnce(&SwitchoverCluster) -> Result<()>) -> Result<()> {
    let (kubeconfig, context, cluster) = cluster_args()?;
    let cluster = SwitchoverCluster {
        kubeconfig,
        context,
        cluster,
    };
    let started = Instant::now();
    let result = test(&cluster);
    eprintln!("switchover scenario total elapsed={:?}", started.elapsed());
    if let Err(error) = &result {
        eprintln!(
            "switchover failed: {error:#}; collecting request, receipt, identity and runtime evidence"
        );
        let _ = Command::new("timeout")
            .args([
                "--kill-after=5s",
                "180s",
                "just",
                "level-triggered-diagnostics",
            ])
            .status();
    }
    result
}

#[derive(Clone, Debug)]
struct ScaleResource {
    kind: &'static str,
    object: Value,
}

impl ScaleResource {
    fn name(&self) -> &str {
        self.object["metadata"]["name"].as_str().unwrap()
    }

    fn uid(&self) -> &str {
        self.object["metadata"]["uid"].as_str().unwrap()
    }

    fn get(&self, cluster: &SwitchoverCluster) -> Result<Option<Value>> {
        let output = cluster.kubectl(&[
            "-n",
            "default",
            "get",
            self.kind,
            self.name(),
            "--ignore-not-found",
            "-o",
            "json",
        ])?;
        if output.trim().is_empty() {
            Ok(None)
        } else {
            Ok(Some(serde_json::from_str(&output)?))
        }
    }
}

#[derive(Clone, Debug)]
struct CleanupDeletionObservation {
    kind: &'static str,
    name: String,
    uid: String,
    resource_version: u64,
    observed_at: std::time::SystemTime,
}

fn validate_cleanup_observations(
    expected: &[ScaleResource],
    observed: &[CleanupDeletionObservation],
    complete: bool,
) -> Result<()> {
    let order = ["service", "pod", "pvc"];
    ensure!(
        expected.len() == order.len()
            && expected
                .iter()
                .zip(order)
                .all(|(resource, kind)| resource.kind == kind),
        "cleanup expectation is not endpoint -> Pod -> PVC: {expected:?}"
    );
    ensure!(
        observed.len() <= expected.len(),
        "cleanup observed duplicate or extra deletion: {observed:?}"
    );
    if complete {
        ensure!(
            observed.len() == expected.len(),
            "cleanup omitted exact deletion observations: {observed:?}"
        );
    }
    for (index, observation) in observed.iter().enumerate() {
        let frozen = &expected[index];
        ensure!(
            observation.kind == frozen.kind
                && observation.name == frozen.name()
                && observation.uid == frozen.uid(),
            "cleanup deletion did not match frozen exact resource at position {index}: \
             expected kind={} name={} uid={}, observed={observation:?}",
            frozen.kind,
            frozen.name(),
            frozen.uid()
        );
        if let Some(previous) = index.checked_sub(1).and_then(|i| observed.get(i)) {
            ensure!(
                previous.resource_version < observation.resource_version,
                "cleanup deletion resourceVersions were not strictly ordered: {observed:?}"
            );
            ensure!(
                previous.observed_at <= observation.observed_at,
                "cleanup observation timestamps regressed: {observed:?}"
            );
        }
    }
    Ok(())
}

fn record_cleanup_deletions(
    cluster: &SwitchoverCluster,
    watches: &[(&'static str, &Receiver<Value>)],
    expected: &[ScaleResource],
    observed: &mut Vec<CleanupDeletionObservation>,
) -> Result<()> {
    let mut batch = Vec::new();
    for (kind, events) in watches {
        for event in events.try_iter() {
            ensure!(event["type"] != "ERROR", "cleanup watch failed: {event}");
            if event["type"] != "DELETED" {
                continue;
            }
            let object = &event["object"];
            let Some(frozen) = expected.iter().find(|resource| {
                resource.kind == *kind
                    && object["metadata"]["name"].as_str() == Some(resource.name())
                    && object["metadata"]["uid"].as_str() == Some(resource.uid())
            }) else {
                continue;
            };
            if observed
                .iter()
                .any(|prior| prior.kind == *kind && prior.uid == frozen.uid())
            {
                continue;
            }
            batch.push(CleanupDeletionObservation {
                kind,
                name: frozen.name().to_string(),
                uid: frozen.uid().to_string(),
                resource_version: resource_version(object)?,
                observed_at: std::time::SystemTime::now(),
            });
        }
    }
    batch.sort_by_key(|observation| observation.resource_version);
    for mut observation in batch {
        observation.observed_at = std::time::SystemTime::now();
        let frozen = expected
            .iter()
            .find(|resource| {
                resource.kind == observation.kind
                    && resource.name() == observation.name
                    && resource.uid() == observation.uid
            })
            .context("frozen cleanup resource")?;
        ensure!(
            frozen
                .get(cluster)?
                .is_none_or(|live| live["metadata"]["uid"].as_str() != Some(frozen.uid())),
            "exact {} {} UID {} remained after its deletion observation",
            frozen.kind,
            frozen.name(),
            frozen.uid()
        );
        if observation.kind == "pod" {
            let endpoint = &expected[0];
            ensure!(
                endpoint
                    .get(cluster)?
                    .is_none_or(|live| live["metadata"]["uid"].as_str() != Some(endpoint.uid())),
                "candidate Pod deletion was observed before exact endpoint absence"
            );
        } else if observation.kind == "pvc" {
            let pod = &expected[1];
            ensure!(
                pod.get(cluster)?
                    .is_none_or(|live| live["metadata"]["uid"].as_str() != Some(pod.uid())),
                "candidate PVC deletion was observed before exact Pod absence"
            );
        }
        observed.push(observation);
        validate_cleanup_observations(expected, observed, false)?;
    }
    Ok(())
}

fn scale_resources(cluster: &SwitchoverCluster, member: &Value) -> Result<Vec<ScaleResource>> {
    let pod: Value = serde_json::from_str(&cluster.kubectl(&[
        "-n",
        "default",
        "get",
        "pod",
        &cluster.pod(member)?,
        "-o",
        "json",
    ])?)?;
    let claims = pod["spec"]["volumes"]
        .as_array()
        .context("volumes")?
        .iter()
        .filter_map(|v| v["persistentVolumeClaim"]["claimName"].as_str())
        .collect::<Vec<_>>();
    ensure!(claims.len() == 1, "ambiguous mounted storage");
    let pvc = serde_json::from_str(
        &cluster.kubectl(&["-n", "default", "get", "pvc", claims[0], "-o", "json"])?,
    )?;
    let services: Value = serde_json::from_str(
        &cluster.kubectl(&["-n", "default", "get", "services", "-o", "json"])?,
    )?;
    let endpoints = services["items"]
        .as_array()
        .context("services")?
        .iter()
        .filter(|s| {
            routing_instance(s) == member["instanceId"].as_str()
                && s["metadata"]["name"] != "kvstore2-write"
        })
        .collect::<Vec<_>>();
    ensure!(endpoints.len() == 1, "ambiguous exact peer endpoint");
    Ok(vec![
        ScaleResource {
            kind: "pod",
            object: pod,
        },
        ScaleResource {
            kind: "pvc",
            object: pvc,
        },
        ScaleResource {
            kind: "service",
            object: endpoints[0].clone(),
        },
    ])
}

fn scale_ready(cluster: &SwitchoverCluster, count: usize, deadline: Instant) -> Result<Value> {
    loop {
        let status = cluster.status()?;
        if status_ready(&status)
            && status["status"]["transition"].is_null()
            && status["status"]["secondaryScaleDownCleanup"].is_null()
            && status["status"]["provisioning"].is_null()
            && status["status"]["topology"]["members"]
                .as_array()
                .is_some_and(|m| m.len() == count)
        {
            let mut ready = true;
            for member in status["status"]["topology"]["members"].as_array().unwrap() {
                ready &= cluster.report(member).is_ok_and(|report| {
                    identity(&report) == identity(member)
                        && report["currentConfiguration"]
                            == status["status"]["topology"]["configurationId"]
                        && report["previousConfiguration"].is_null()
                        && report["pendingOperation"].is_null()
                        && (report["writeStatus"] == "Granted") == (member["role"] == "primary")
                });
            }
            if ready {
                return Ok(status);
            }
        }
        poll(deadline, &format!("stable {count}-member topology"))?;
    }
}

fn reset_scale_set(cluster: &SwitchoverCluster, count: usize) -> Result<()> {
    reset_scale_set_with_delay(cluster, count, 30)
}

fn reset_scale_set_with_delay(
    cluster: &SwitchoverCluster,
    count: usize,
    failover_delay_seconds: u64,
) -> Result<()> {
    cluster.kubectl(&[
        "-n",
        "default",
        "delete",
        "kubericset",
        "kvstore2",
        "--ignore-not-found",
        "--cascade=foreground",
        "--wait=false",
    ])?;
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let objects: Value = serde_json::from_str(&cluster.kubectl(&[
            "-n",
            "default",
            "get",
            "pod,pvc,service",
            "-l",
            "operator.kuberic.io/set-name=kvstore2",
            "-o",
            "json",
        ])?)?;
        if objects["items"]
            .as_array()
            .context("owned resources")?
            .is_empty()
        {
            break;
        }
        poll(deadline, "old set garbage collection")?;
    }
    let object = json!({
        "apiVersion":"operator.kuberic.io/v1alpha1", "kind":"KubericSet",
        "metadata":{
            "name":"kvstore2",
            "namespace":"default",
            "annotations":{"testing.kuberic.io/live-copy-gate":"enabled"}
        },
        "spec":{"replicas":count,"image":"localhost/kvstore2:level-triggered-v1",
            "failoverDelaySeconds":failover_delay_seconds}
    });
    let mut child = OwnedChild(
        cluster
            .command()
            .args(["create", "-f", "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()?,
    );
    child
        .0
        .stdin
        .take()
        .context("create stdin")?
        .write_all(object.to_string().as_bytes())?;
    ensure!(
        child.0.wait()?.success(),
        "create fresh level-triggered set failed"
    );
    scale_ready(cluster, count, Instant::now() + Duration::from_secs(180))?;
    Ok(())
}

fn highest_secondary(topology: &Value) -> Result<&Value> {
    topology["members"]
        .as_array()
        .context("topology members")?
        .iter()
        .filter(|m| m["role"] == "activeSecondary")
        .max_by_key(|m| m["replicaId"].as_i64())
        .context("highest secondary")
}

fn check_reduction(before: &Value, after: &Value) -> Result<i64> {
    let target = highest_secondary(before)?;
    let expected = before["members"]
        .as_array()
        .context("previous members")?
        .iter()
        .filter(|m| identity(m) != identity(target))
        .cloned()
        .collect::<Vec<_>>();
    ensure!(
        after["members"] == json!(expected),
        "not exactly the highest-ID secondary removal: {after}"
    );
    ensure!(
        before["epoch"]["dataLossNumber"] == after["epoch"]["dataLossNumber"],
        "data-loss epoch changed"
    );
    ensure!(
        after["epoch"]["configurationNumber"].as_i64()
            == before["epoch"]["configurationNumber"]
                .as_i64()
                .map(|n| n + 1),
        "not one sequential epoch"
    );
    ensure!(
        after["writeQuorum"].as_u64() == Some((expected.len() / 2 + 1) as u64),
        "wrong reduced quorum"
    );
    target["replicaId"].as_i64().context("target ID")
}

struct ScaleCase<'a> {
    cluster: &'a SwitchoverCluster,
    start: Value,
    accepted: Value,
    resources: Vec<(i64, ScaleResource, KubeWatch)>,
    status: KubeWatch,
    commits: std::collections::BTreeMap<i64, u64>,
    deletions: Vec<(i64, u64)>,
    replacements: std::collections::BTreeMap<String, String>,
    primary: Value,
    writer: DirectClient,
    target_client: DirectClient,
    acknowledged: Vec<(String, String)>,
    started: Instant,
    deadline: Instant,
}

impl<'a> ScaleCase<'a> {
    fn new(cluster: &'a SwitchoverCluster, count: usize) -> Result<Self> {
        let start = scale_ready(cluster, count, Instant::now() + Duration::from_secs(180))?;
        let started = Instant::now();
        let deadline = started + Duration::from_secs(600);
        let topology = &start["status"]["topology"];
        let primary = topology["members"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["role"] == "primary")
            .context("primary")?
            .clone();
        let target = highest_secondary(topology)?;
        let writer = DirectClient::connect(cluster, &cluster.pod(&primary)?, deadline)?;
        let target_client = DirectClient::connect(cluster, &cluster.pod(target)?, deadline)?;
        let mut resources = Vec::new();
        for member in topology["members"].as_array().unwrap() {
            for resource in scale_resources(cluster, member)? {
                let watch = KubeWatch::start(cluster, resource.kind, resource.name())?;
                resources.push((member["replicaId"].as_i64().unwrap(), resource, watch));
            }
        }
        let status = KubeWatch::start(cluster, "kubericset", "kvstore2")?;
        let mut case = Self {
            cluster,
            accepted: topology.clone(),
            start,
            resources,
            status,
            commits: Default::default(),
            deletions: Vec::new(),
            replacements: Default::default(),
            primary,
            writer,
            target_client,
            acknowledged: Vec::new(),
            started,
            deadline,
        };
        for _ in 0..3 {
            case.write("seed")?;
        }
        let committed = case.writer.report()?["committedLsn"]
            .as_i64()
            .context("committed LSN")?;
        loop {
            let mut retained = true;
            for member in case.accepted["members"].as_array().unwrap() {
                retained &= retains_acknowledged_prefix(
                    &cluster.report(member)?,
                    member,
                    &case.accepted["configurationId"],
                    committed,
                );
            }
            if retained {
                break;
            }
            poll(deadline, "acknowledged seed prefix on every exact member")?;
        }
        ensure!(
            case.target_client
                .request("PUT", "/kv/secondary-bypass", "forbidden")?
                .0
                == 503,
            "secondary direct client bypassed access"
        );
        Ok(case)
    }

    fn write(&mut self, label: &str) -> Result<()> {
        let key = format!(
            "scale-{}-{label}-{}",
            self.start["metadata"]["uid"].as_str().unwrap(),
            self.acknowledged.len()
        );
        let value = format!("durable-{key}");
        let (code, body) = self.writer.request("PUT", &format!("/kv/{key}"), &value)?;
        ensure!(code == 200, "write failed {code}: {body}");
        self.acknowledged.push((key, value));
        Ok(())
    }

    fn submit(&self, count: usize) -> Result<()> {
        self.cluster.kubectl(&[
            "-n",
            "default",
            "patch",
            "kubericset",
            "kvstore2",
            "--type=merge",
            "-p",
            &json!({"spec":{"replicas":count}}).to_string(),
        ])?;
        Ok(())
    }

    fn observe(&mut self) -> Result<Value> {
        for event in self.status.events.try_iter() {
            ensure!(
                event["type"] != "ERROR",
                "scale status watch failed: {event}"
            );
            let object = &event["object"];
            ensure!(
                !object["status"]["conditions"]
                    .as_array()
                    .is_some_and(|conditions| conditions
                        .iter()
                        .any(|c| c["type"] == "Unsafe" && c["status"] == "true")),
                "scale-down produced unsafe authority: {}",
                object["status"]["conditions"]
            );
            let topology = &object["status"]["topology"];
            if topology != &self.accepted {
                let removed = check_reduction(&self.accepted, topology)?;
                ensure!(
                    object["status"]["transition"].is_null(),
                    "commit still has active authority transition"
                );
                let cleanup = &object["status"]["secondaryScaleDownCleanup"];
                let intent = &cleanup["evidence"]["preparation"]["intent"];
                ensure!(
                    intent["previousConfiguration"] == self.accepted
                        && intent["currentConfiguration"] == *topology
                        && intent["target"]["replicaId"] == removed,
                    "commit lost its immutable evidence"
                );
                ensure!(
                    !cleanup["currentOnlyWriteQuorum"]
                        .as_array()
                        .context("commit quorum")?
                        .is_empty(),
                    "commit omitted current-only evidence"
                );
                let rv = object["metadata"]["resourceVersion"]
                    .as_str()
                    .context("commit version")?
                    .parse()?;
                ensure!(
                    self.commits.insert(removed, rv).is_none(),
                    "duplicate removal"
                );
                self.accepted = topology.clone();
                eprintln!(
                    "scale commit: target={removed}, epoch={}, elapsed={:?}",
                    topology["epoch"],
                    self.started.elapsed()
                );
            }
            let intent = &object["status"]["transition"]["secondaryScaleDown"];
            if intent.is_object() {
                ensure!(
                    intent["target"] == identity(highest_secondary(&self.accepted)?),
                    "frozen target changed or skipped highest ID"
                );
                ensure!(
                    object["status"]["secondaryScaleDownCleanup"].is_null(),
                    "overlapping cleanup/admission"
                );
                ensure!(!status_ready(object), "in-progress scale-down claims Ready");
                for (id, resource, _) in &self.resources {
                    if Some(*id) == intent["target"]["replicaId"].as_i64() {
                        let field = match resource.kind {
                            "pod" => "pod",
                            "pvc" => "pvc",
                            _ => "endpoint",
                        };
                        let frozen = &intent["cleanup"][field]["present"];
                        ensure!(
                            frozen["name"] == resource.name() && frozen["uid"] == resource.uid(),
                            "cleanup did not freeze actual {field} identity: {frozen}"
                        );
                    }
                }
            }
        }
        // This isolated KinD uses one etcd revision space. Compare mutation
        // revisions, not watch delivery order across independent resource streams.
        for (id, resource, watch) in &self.resources {
            for event in watch.events.try_iter() {
                ensure!(event["type"] != "ERROR", "resource watch failed: {event}");
                if event["type"] == "DELETED"
                    || event["object"]["metadata"]["deletionTimestamp"].is_string()
                {
                    ensure!(
                        event["object"]["metadata"]["uid"] == resource.uid(),
                        "watch changed resource identity"
                    );
                    // Drain status on the next observation if its watch delivery lags.
                    let status = self.cluster.status()?;
                    ensure!(
                        !status["status"]["topology"]["members"]
                            .as_array()
                            .context("accepted members")?
                            .iter()
                            .any(|m| m["replicaId"] == *id),
                        "resource deletion preceded membership commit"
                    );
                    let revision: u64 = event["object"]["metadata"]["resourceVersion"]
                        .as_str()
                        .context("delete revision")?
                        .parse()?;
                    self.deletions.push((*id, revision));
                    if let Some(commit) = self.commits.get(id) {
                        ensure!(
                            revision > *commit,
                            "deletion revision predates membership commit"
                        );
                    }
                }
            }
        }
        self.cluster.status()
    }

    fn finish(&mut self, count: usize) -> Result<()> {
        loop {
            let status = self.observe()?;
            if status_ready(&status)
                && status["status"]["secondaryScaleDownCleanup"].is_null()
                && status["status"]["transition"].is_null()
                && self.accepted["members"].as_array().unwrap().len() == count
            {
                break;
            }
            let key = format!(
                "during-{}-{}",
                self.start["metadata"]["uid"].as_str().unwrap(),
                self.acknowledged.len()
            );
            let response = self
                .writer
                .request("PUT", &format!("/kv/{key}"), "during-removal")?;
            match response.0 {
                200 => self.acknowledged.push((key, "during-removal".into())),
                503 => {}
                code => bail!("unexpected in-flight write result: {code} {}", response.1),
            }
            poll(self.deadline, "accepted reduction and exact cleanup")?;
        }
        let status = scale_ready(self.cluster, count, self.deadline)?;
        self.observe()?;
        ensure!(
            self.commits.len()
                == self.start["status"]["topology"]["members"]
                    .as_array()
                    .unwrap()
                    .len()
                    - count,
            "missed an intermediate accepted topology"
        );
        for (id, revision) in &self.deletions {
            ensure!(
                self.commits.get(id).is_some_and(|commit| revision > commit),
                "resource mutation preceded its exact topology commit"
            );
        }
        for (id, resource, _) in &self.resources {
            let observed = resource.get(self.cluster)?;
            if self.commits.contains_key(id) {
                if let Some(uid) = self.replacements.get(resource.name()) {
                    ensure!(
                        observed.is_some_and(|o| o["metadata"]["uid"] == *uid
                            && o["metadata"]["deletionTimestamp"].is_null()),
                        "same-name replacement was deleted"
                    );
                } else {
                    ensure!(
                        observed.is_none(),
                        "frozen {} {} remains after cleanup",
                        resource.kind,
                        resource.name()
                    );
                }
            } else {
                ensure!(
                    observed
                        .as_ref()
                        .is_some_and(|o| o["metadata"]["uid"] == resource.uid()
                            && o["metadata"]["deletionTimestamp"].is_null()),
                    "retained resource changed: {}",
                    resource.name()
                );
            }
        }
        ensure!(
            topology_primary_id(&status) == self.primary["replicaId"].as_i64(),
            "primary changed"
        );
        let routed_key = format!(
            "routed-{}-{}",
            self.start["metadata"]["uid"].as_str().unwrap(),
            self.acknowledged.len()
        );
        assert_routed_service_write(
            self.cluster,
            &self.primary,
            &routed_key,
            "routed-after-removal",
            self.deadline,
        )?;
        self.acknowledged
            .push((routed_key, "routed-after-removal".into()));
        self.write("after")?;
        for (key, value) in &self.acknowledged {
            ensure!(
                self.writer.request("GET", &format!("/kv/{key}"), "")? == (200, value.clone()),
                "lost acknowledged value {key}"
            );
        }
        check_deleted_target_response(self.target_client.request(
            "PUT",
            "/kv/retired-bypass",
            "forbidden",
        ))?;
        eprintln!(
            "scale {}->{count}: {} acknowledged values, elapsed={:?}",
            self.start["status"]["topology"]["members"]
                .as_array()
                .unwrap()
                .len(),
            self.acknowledged.len(),
            self.started.elapsed()
        );
        Ok(())
    }
}

#[test]
#[ignore = "requires an explicitly owned isolated KinD cluster"]
fn scale_down() -> Result<()> {
    run_switchover(|cluster| {
        reset_scale_set(cluster, 3)?;
        let mut case = ScaleCase::new(cluster, 3)?;
        case.submit(2)?;
        case.finish(2)?;
        let acknowledged = case.acknowledged.clone();
        drop(case);
        let mut case = ScaleCase::new(cluster, 2)?;
        case.acknowledged.extend(acknowledged);
        case.submit(1)?;
        case.finish(1)?;
        let old_session = case.writer.report()?["processSession"].clone();
        stop_replica_process(
            &cluster.kubeconfig,
            &cluster.context,
            case.primary["replicaId"].as_i64().unwrap(),
        )?;
        check_old_session_response(
            case.writer
                .request("PUT", "/kv/stale-singleton", "forbidden"),
        )?;
        scale_ready(cluster, 1, case.deadline)?;
        case.writer = DirectClient::connect(cluster, &cluster.pod(&case.primary)?, case.deadline)?;
        ensure!(
            case.writer.report()?["processSession"] != old_session,
            "singleton session did not change"
        );
        case.finish(1)?;
        // Verify a fresh post-restart write is durable through another process restart.
        case.write("singleton-reopened")?;
        stop_replica_process(
            &cluster.kubeconfig,
            &cluster.context,
            case.primary["replicaId"].as_i64().unwrap(),
        )?;
        scale_ready(cluster, 1, case.deadline)?;
        case.writer = DirectClient::connect(cluster, &cluster.pod(&case.primary)?, case.deadline)?;
        case.finish(1)?;
        drop(case);
        reset_scale_set(cluster, 5)?;
        let mut case = ScaleCase::new(cluster, 5)?;
        case.submit(2)?;
        case.finish(2)
    })
}

struct ControllerPause<'a>(&'a SwitchoverCluster);

impl<'a> ControllerPause<'a> {
    fn new(cluster: &'a SwitchoverCluster) -> Result<Self> {
        cluster.kubectl(&[
            "-n",
            "kuberic-system",
            "scale",
            "deployment/kuberic-controller",
            "--replicas=0",
        ])?;
        let pause = Self(cluster);
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let pods: Value = serde_json::from_str(&cluster.kubectl(&[
                "-n",
                "kuberic-system",
                "get",
                "pods",
                "-l",
                "app.kubernetes.io/name=kuberic-controller",
                "-o",
                "json",
            ])?)?;
            if pods["items"]
                .as_array()
                .context("controller Pods")?
                .is_empty()
            {
                return Ok(pause);
            }
            poll(deadline, "controller process termination")?;
        }
    }
}

impl Drop for ControllerPause<'_> {
    fn drop(&mut self) {
        let _ = self.0.kubectl(&[
            "-n",
            "kuberic-system",
            "scale",
            "deployment/kuberic-controller",
            "--replicas=1",
        ]);
    }
}

struct ControlForward {
    _process: OwnedChild,
    endpoint: String,
}

impl ControlForward {
    fn new(cluster: &SwitchoverCluster, pod: &str) -> Result<Self> {
        let mut process = OwnedChild(
            Command::new("kubectl")
                .args([
                    "--kubeconfig",
                    &cluster.kubeconfig,
                    "--context",
                    &cluster.context,
                    "-n",
                    "default",
                    "port-forward",
                    "--address=127.0.0.1",
                    pod,
                    ":50051",
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()?,
        );
        let stdout = process.0.stdout.take().context("control forward stdout")?;
        let (send, receive) = channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Some(address) = line
                    .strip_prefix("Forwarding from ")
                    .and_then(|l| l.split_once(" -> ").map(|(a, _)| a))
                {
                    let _ = send.send(address.to_string());
                }
            }
        });
        let endpoint = format!("http://{}", receive.recv_timeout(Duration::from_secs(10))?);
        Ok(Self {
            _process: process,
            endpoint,
        })
    }
}

// Only transport addressing is adapted to the host. Observations, normalization,
// authority evaluation, session dispatch, status CAS and UID deletes are production
// controller code against the live API/agents. A fresh controller is reconstructed
// after EVERY effect; successful replies are deliberately discarded, not forged.
struct LiveAgents {
    endpoints: std::collections::BTreeMap<String, ControlForward>,
    last_command: std::sync::Mutex<Option<(String, kuberic_wire::proto::ExecuteCommandRequest)>>,
}

#[async_trait::async_trait]
impl kuberic_controller::cluster_api::AgentApi for LiveAgents {
    async fn get_status(
        &self,
        endpoint: &str,
        token: &str,
        request: kuberic_wire::proto::GetAgentStatusRequest,
    ) -> std::result::Result<
        kuberic_wire::proto::AgentStatusReport,
        kuberic_controller::cluster_api::AgentRpcError,
    > {
        use kuberic_controller::cluster_api::{AgentRpcError, GrpcAgentApi};
        let forward = self
            .endpoints
            .get(endpoint)
            .ok_or_else(|| AgentRpcError::Unavailable("exact Pod absent".into()))?;
        GrpcAgentApi::new(Duration::from_secs(2))
            .get_status(&forward.endpoint, token, request)
            .await
    }

    async fn execute(
        &self,
        endpoint: &str,
        token: &str,
        request: kuberic_wire::proto::ExecuteCommandRequest,
    ) -> std::result::Result<
        kuberic_wire::proto::ExecuteCommandResponse,
        kuberic_controller::cluster_api::AgentRpcError,
    > {
        use kuberic_controller::cluster_api::{AgentRpcError, GrpcAgentApi};
        let forward = self
            .endpoints
            .get(endpoint)
            .ok_or_else(|| AgentRpcError::Unavailable("exact Pod absent".into()))?;
        GrpcAgentApi::new(Duration::from_secs(5))
            .execute(&forward.endpoint, token, request.clone())
            .await?;
        *self.last_command.lock().unwrap() = Some((endpoint.to_string(), request));
        Err(AgentRpcError::Unavailable(
            "live fault: discarded successful Execute reply".into(),
        ))
    }
}

struct LiveStep {
    plan: kuberic_protocol::plan::Plan,
    command: Option<(String, kuberic_wire::proto::ExecuteCommandRequest)>,
}

struct LiveStepper {
    runtime: tokio::runtime::Runtime,
    agents: std::sync::Arc<LiveAgents>,
}

impl LiveStepper {
    fn new(cluster: &SwitchoverCluster) -> Result<Self> {
        let pods: Value = serde_json::from_str(
            &cluster.kubectl(&["-n", "default", "get", "pods", "-o", "json"])?,
        )?;
        let mut endpoints = std::collections::BTreeMap::new();
        for pod in pods["items"].as_array().context("Pod list")? {
            if let (Some(ip), Some(name)) = (
                pod["status"]["podIP"].as_str(),
                pod["metadata"]["name"].as_str(),
            ) && name.starts_with("kvstore2-")
                && pod["metadata"]["deletionTimestamp"].is_null()
            {
                endpoints.insert(
                    format!("http://{ip}:50051"),
                    ControlForward::new(cluster, name)?,
                );
            }
        }
        Ok(Self {
            runtime: tokio::runtime::Runtime::new()?,
            agents: std::sync::Arc::new(LiveAgents {
                endpoints,
                last_command: Default::default(),
            }),
        })
    }

    fn step(&self, cluster: &SwitchoverCluster) -> Result<LiveStep> {
        use kuberic_controller::cluster_api::{ClusterApi, KubeClusterApi};
        self.runtime.block_on(async {
            let options = kube::config::KubeConfigOptions {
                context: Some(cluster.context.clone()),
                ..Default::default()
            };
            let config = kube::Config::from_custom_kubeconfig(
                kube::config::Kubeconfig::read_from(&cluster.kubeconfig)?,
                &options,
            )
            .await?;
            let api = KubeClusterApi::new(
                kube::Client::try_from(config)?,
                self.agents.clone(),
                std::env::var("KUBERIC_AGENT_BEARER_TOKEN").context("live agent token")?,
            )?;
            let raw = api.observe("default", "kvstore2").await?;
            let snapshot =
                kuberic_controller::normalize::normalize(raw.clone(), Default::default())?;
            let plan = kuberic_protocol::evaluator::evaluate(
                &snapshot,
                &kuberic_controller::production_evaluation_config(30, 5, 30),
            );
            let result =
                kuberic_controller::executor::execute_plan(&api, &raw, &snapshot, plan.clone())
                    .await;
            if let Err(error) = result {
                ensure!(
                    matches!(
                        error,
                        kuberic_controller::ControllerError::AgentUnavailable(_)
                            | kuberic_controller::ControllerError::ObservationStale
                    ),
                    "live controller effect: {error}"
                );
            }
            // No completion state is carried into the next controller observation.
            Ok(LiveStep {
                plan,
                command: self.agents.last_command.lock().unwrap().take(),
            })
        })
    }

    fn observe_snapshot(
        &self,
        cluster: &SwitchoverCluster,
    ) -> Result<kuberic_protocol::observation::ObservationSnapshot> {
        use kuberic_controller::cluster_api::{ClusterApi, KubeClusterApi};
        self.runtime.block_on(async {
            let options = kube::config::KubeConfigOptions {
                context: Some(cluster.context.clone()),
                ..Default::default()
            };
            let config = kube::Config::from_custom_kubeconfig(
                kube::config::Kubeconfig::read_from(&cluster.kubeconfig)?,
                &options,
            )
            .await?;
            let api = KubeClusterApi::new(
                kube::Client::try_from(config)?,
                self.agents.clone(),
                std::env::var("KUBERIC_AGENT_BEARER_TOKEN").context("live agent token")?,
            )?;
            let raw = api.observe("default", "kvstore2").await?;
            Ok(kuberic_controller::normalize::normalize(
                raw,
                Default::default(),
            )?)
        })
    }

    fn reject_stale(
        &self,
        endpoint: &str,
        request: kuberic_wire::proto::ExecuteCommandRequest,
    ) -> Result<()> {
        self.runtime.block_on(async {
            let forward = self
                .agents
                .endpoints
                .get(endpoint)
                .context("stale-command exact endpoint")?;
            let mut client =
                kuberic_wire::proto::agent_control_client::AgentControlClient::connect(
                    forward.endpoint.clone(),
                )
                .await?;
            let mut request = tonic::Request::new(request);
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", std::env::var("KUBERIC_AGENT_BEARER_TOKEN")?).parse()?,
            );
            let result =
                tokio::time::timeout(Duration::from_secs(5), client.execute(request)).await?;
            let status = result.expect_err("obsolete process-session command was accepted");
            ensure!(
                status.code() == tonic::Code::FailedPrecondition
                    && status.message().contains("stale agent process session"),
                "not a specific stale-session rejection: {status}"
            );
            Ok(())
        })
    }
}

fn removal_stage(plan: &kuberic_protocol::plan::Plan) -> Option<String> {
    use kuberic_protocol::{
        command::{KubernetesChange, ProtocolCommand},
        plan::Plan,
    };
    match plan {
        Plan::Execute { command } => match command {
            ProtocolCommand::PrepareSecondaryRemoval(_) => Some("prepare".into()),
            ProtocolCommand::EnsureConfiguration(c) if c.secondary_removal_evidence.is_some() => {
                Some(format!(
                    "{}-{}",
                    if c.current_only {
                        "current-only"
                    } else {
                        "pc-cc"
                    },
                    c.local_replica_id
                ))
            }
            ProtocolCommand::AcceptSecondaryRemovalCommit(c) => {
                Some(format!("accept-commit-{}", c.target.replica_id))
            }
            ProtocolCommand::RetireReplica(_) => Some("retire".into()),
            _ => None,
        },
        Plan::Apply { changes } => changes.iter().find_map(|change| match change {
            KubernetesChange::PersistStatus { status }
                if status.secondary_scale_down_cleanup.is_some() =>
            {
                Some("commit-or-cleanup-status".into())
            }
            KubernetesChange::DeleteScaleDownResource { resource, .. } => {
                Some(format!("delete-{resource:?}"))
            }
            _ => None,
        }),
        _ => None,
    }
}

fn restart_scale_process(case: &mut ScaleCase<'_>, member: &Value) -> Result<()> {
    let old = case.cluster.report(member)?["processSession"].clone();
    stop_replica_process(
        &case.cluster.kubeconfig,
        &case.cluster.context,
        member["replicaId"].as_i64().unwrap(),
    )?;
    loop {
        if case
            .cluster
            .report(member)
            .is_ok_and(|r| identity(&r) == identity(member) && r["processSession"] != old)
        {
            break;
        }
        poll(
            case.deadline,
            "new session with unchanged Pod/PVC provenance",
        )?;
    }
    if identity(member) == identity(&case.primary) {
        check_old_session_response(case.writer.request(
            "PUT",
            "/kv/stale-primary-session",
            "forbidden",
        ))?;
        case.writer =
            DirectClient::connect(case.cluster, &case.cluster.pod(member)?, case.deadline)?;
    }
    Ok(())
}

#[test]
#[ignore = "requires an explicitly owned isolated KinD cluster"]
fn scale_down_adversarial() -> Result<()> {
    run_switchover(|cluster| {
        use kuberic_protocol::{command::ProtocolCommand, plan::Plan};
        scale_down_historical_member_return(cluster)?;
        // Reachable-target retirement and every command/status/cleanup lost-reply
        // boundary, with a fresh controller state after each production effect.
        reset_scale_set(cluster, 3)?;
        let mut case = ScaleCase::new(cluster, 3)?;
        let target = highest_secondary(&case.accepted)?.clone();
        let retained = case.accepted["members"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["role"] == "activeSecondary" && identity(m) != identity(&target))
            .unwrap()
            .clone();
        let pause = ControllerPause::new(cluster)?;
        let partition = cluster.partition_replication(target["replicaId"].as_i64().unwrap())?;
        restart_scale_process(&mut case, &target)?;
        check_old_session_response(case.target_client.request(
            "PUT",
            "/kv/old-target-session",
            "forbidden",
        ))?;
        case.target_client = DirectClient::connect(cluster, &cluster.pod(&target)?, case.deadline)?;
        case.submit(2)?;
        let mut stepper = LiveStepper::new(cluster)?;
        let mut stages = std::collections::BTreeSet::new();
        let mut prepared = false;
        let mut admitted = false;
        loop {
            let step = stepper.step(cluster)?;
            if let Some(stage) = removal_stage(&step.plan) {
                eprintln!("scale live controller restart/lost-reply boundary: {stage}");
                stages.insert(stage);
            }
            if matches!(
                &step.plan,
                Plan::Execute {
                    command: ProtocolCommand::RetireReplica(_)
                }
            ) && step.command.is_some()
            {
                ensure!(
                    cluster.report(&target)?["role"] == "None",
                    "retired target retained an application role"
                );
                ensure!(
                    case.target_client
                        .request("PUT", "/kv/retired-write", "forbidden")?
                        .0
                        == 503,
                    "retired target accepted a direct write before Pod deletion"
                );
                ensure!(
                    case.target_client
                        .request("GET", &format!("/kv/{}", case.acknowledged[0].0), "")?
                        .0
                        == 503,
                    "retired target served a retained direct read"
                );
            }
            let status = case.observe()?;
            if !prepared
                && matches!(
                    &step.plan,
                    Plan::Execute {
                        command: ProtocolCommand::PrepareSecondaryRemoval(_)
                    }
                )
                && let Some((endpoint, request)) = step.command
            {
                let primary = case.primary.clone();
                restart_scale_process(&mut case, &primary)?;
                stepper = LiveStepper::new(cluster)?;
                stepper.reject_stale(&endpoint, request)?;
                let suspension = suspend_replica_process(
                    &cluster.kubeconfig,
                    &cluster.context,
                    retained["replicaId"].as_i64().unwrap(),
                )?;
                for _ in 0..2 {
                    stepper.step(cluster)?;
                    let waiting = case.observe()?;
                    ensure!(
                        waiting["status"]["topology"] == case.start["status"]["topology"]
                            && !status_ready(&waiting),
                        "missing retained quorum committed"
                    );
                    ensure!(
                        waiting["status"]["conditions"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|c| c["reason"] == "ScaleDownPreviousReadQuorumUnavailable"),
                        "missing retained quorum not diagnosed: {}",
                        waiting["status"]["conditions"]
                    );
                    ensure!(
                        case.writer
                            .request("PUT", "/kv/closed-preparation", "forbidden")?
                            .0
                            == 503,
                        "retained primary client bypassed preparation closure"
                    );
                    ensure!(
                        case.target_client
                            .request("PUT", "/kv/closed-target", "forbidden")?
                            .0
                            == 503,
                        "retained target bypassed access"
                    );
                    for (_, resource, _) in &case.resources {
                        ensure!(
                            resource
                                .get(cluster)?
                                .is_some_and(|r| r["metadata"]["uid"] == resource.uid()),
                            "precommit resource deleted"
                        );
                    }
                }
                drop(suspension);
                prepared = true;
            }
            if !admitted
                && matches!(&step.plan, Plan::Execute { command: ProtocolCommand::EnsureConfiguration(c) }
                if c.secondary_removal_evidence.is_some() && !c.current_only)
            {
                restart_scale_process(&mut case, &target)?;
                check_old_session_response(case.target_client.request(
                    "PUT",
                    "/kv/target-restarted",
                    "forbidden",
                ))?;
                case.target_client =
                    DirectClient::connect(cluster, &cluster.pod(&target)?, case.deadline)?;
                stepper = LiveStepper::new(cluster)?;
                admitted = true;
            }
            if status_ready(&status)
                && status["status"]["secondaryScaleDownCleanup"].is_null()
                && case.accepted["members"].as_array().unwrap().len() == 2
            {
                break;
            }
            poll(case.deadline, "fault-stepped reachable-target removal")?;
        }
        ensure!(prepared && admitted, "missed process restart boundaries");
        for stage in [
            "prepare",
            "pc-cc-1",
            "pc-cc-2",
            "current-only-1",
            "current-only-2",
            "accept-commit-1",
            "accept-commit-2",
            "retire",
            "delete-Endpoint",
            "delete-Pod",
            "delete-Pvc",
        ] {
            ensure!(
                stages.contains(stage),
                "never exercised live boundary {stage}: {stages:?}"
            );
        }
        drop(partition);
        drop(pause);
        case.finish(2)?;
        drop(case);

        // A permanently unreachable highest-ID target cannot block the next
        // sequential removal; no timeout is interpreted as retirement/absence.
        reset_scale_set(cluster, 3)?;
        let mut case = ScaleCase::new(cluster, 3)?;
        let target = highest_secondary(&case.accepted)?.clone();
        let pause = ControllerPause::new(cluster)?;
        let suspension = suspend_replica_process(
            &cluster.kubeconfig,
            &cluster.context,
            target["replicaId"].as_i64().unwrap(),
        )?;
        let partition = partition_replica(
            &cluster.kubeconfig,
            &cluster.context,
            &cluster.cluster,
            target["replicaId"].as_i64().unwrap(),
        )?;
        case.submit(1)?;
        let stepper = LiveStepper::new(cluster)?;
        let mut label_loss = false;
        let mut fenced = false;
        let mut singleton_target = None;
        loop {
            let step = stepper.step(cluster)?;
            let status = case.observe()?;
            if singleton_target.is_none()
                && case.commits.len() == 1
                && status["status"]["transition"]["secondaryScaleDown"]["target"]["replicaId"] == 2
            {
                singleton_target = Some(suspend_replica_process(
                    &cluster.kubeconfig,
                    &cluster.context,
                    2,
                )?);
            }
            if !label_loss && status["status"]["transition"]["secondaryScaleDown"].is_object() {
                for (id, resource, _) in &case.resources {
                    if Some(*id) == target["replicaId"].as_i64() {
                        cluster.kubectl(&[
                            "-n",
                            "default",
                            "label",
                            resource.kind,
                            resource.name(),
                            "operator.kuberic.io/set-name-",
                            "operator.kuberic.io/set-uid-",
                        ])?;
                    }
                }
                label_loss = true;
            }
            if removal_stage(&step.plan).as_deref() == Some("delete-Pod") && !fenced {
                ensure!(
                    case.accepted["members"].as_array().unwrap().len() == 2,
                    "unreachable target fenced before reduced commit"
                );
                ensure!(
                    status["status"]["secondaryScaleDownCleanup"]["retirement"].is_null(),
                    "unreachable process produced fabricated retirement"
                );
                fenced = true;
            }
            if case.commits.len() == 2
                && status_ready(&status)
                && status["status"]["secondaryScaleDownCleanup"].is_null()
            {
                break;
            }
            poll(
                case.deadline,
                "unreachable-target exact fence and sequential removal",
            )?;
        }
        ensure!(
            fenced && label_loss && singleton_target.is_some(),
            "missing exact fence/ownership-label loss coverage"
        );
        drop(partition);
        drop(suspension);
        drop(singleton_target);
        drop(pause);
        case.finish(1)?;
        drop(case);

        // Freeze first, then lose the Pod while its exact endpoint/PVC remain.
        reset_scale_set(cluster, 2)?;
        let mut case = ScaleCase::new(cluster, 2)?;
        let target = highest_secondary(&case.accepted)?.clone();
        let pause = ControllerPause::new(cluster)?;
        case.submit(1)?;
        let stepper = LiveStepper::new(cluster)?;
        stepper.step(cluster)?;
        let frozen = case.observe()?;
        ensure!(
            frozen["status"]["transition"]["secondaryScaleDown"].is_object(),
            "intent not frozen"
        );
        cluster.delete_exact_pod(&target)?;
        // The deliberate precommit deletion is the fault, not controller cleanup.
        case.resources
            .retain(|(id, r, _)| !(Some(*id) == target["replicaId"].as_i64() && r.kind == "pod"));
        for (id, resource, _) in &case.resources {
            if Some(*id) == target["replicaId"].as_i64() {
                ensure!(
                    resource
                        .get(cluster)?
                        .is_some_and(|r| r["metadata"]["uid"] == resource.uid()),
                    "absent Pod lost frozen endpoint/storage"
                );
            }
        }
        let mut replaced_endpoint = false;
        loop {
            let step = stepper.step(cluster)?;
            if !replaced_endpoint && removal_stage(&step.plan).as_deref() == Some("delete-Endpoint")
            {
                let (_, endpoint, _) = case
                    .resources
                    .iter()
                    .find(|(id, r, _)| {
                        Some(*id) == target["replicaId"].as_i64() && r.kind == "service"
                    })
                    .context("frozen endpoint")?;
                let replacement = json!({
                    "apiVersion":"v1","kind":"Service",
                    "metadata":{"name":endpoint.name(),"namespace":"default",
                        "ownerReferences":endpoint.object["metadata"]["ownerReferences"]},
                    "spec":{"ports":[{"name":"unrelated","port":12345}],"selector":{"unrelated":"replacement"}}
                });
                let mut child = OwnedChild(
                    cluster
                        .command()
                        .args(["create", "-f", "-"])
                        .stdin(Stdio::piped())
                        .stdout(Stdio::null())
                        .spawn()?,
                );
                child
                    .0
                    .stdin
                    .take()
                    .context("replacement stdin")?
                    .write_all(replacement.to_string().as_bytes())?;
                ensure!(
                    child.0.wait()?.success(),
                    "same-name endpoint recreation failed"
                );
                let object = endpoint.get(cluster)?.context("replacement endpoint")?;
                let uid = object["metadata"]["uid"]
                    .as_str()
                    .context("replacement UID")?;
                ensure!(uid != endpoint.uid(), "replacement reused frozen UID");
                case.replacements
                    .insert(endpoint.name().to_string(), uid.to_string());
                replaced_endpoint = true;
            }
            let status = case.observe()?;
            if status_ready(&status)
                && status["status"]["secondaryScaleDownCleanup"].is_null()
                && case.commits.len() == 1
            {
                break;
            }
            poll(case.deadline, "already-absent Pod reduction")?;
        }
        ensure!(
            replaced_endpoint,
            "endpoint UID drift boundary not exercised"
        );
        drop(pause);
        case.finish(1)
    })
}

fn scale_down_historical_member_return(cluster: &SwitchoverCluster) -> Result<()> {
    use kuberic_protocol::{command::ProtocolCommand, plan::Plan};
    reset_scale_set(cluster, 6)?;
    let mut case = ScaleCase::new(cluster, 6)?;
    let late = case.accepted["members"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["replicaId"] == 5)
        .context("retained member 5")?
        .clone();
    let primary = case.primary.clone();
    let pause = ControllerPause::new(cluster)?;
    case.submit(5)?;
    let mut stepper = LiveStepper::new(cluster)?;
    let mut suspension = None;
    let mut old_command = None;
    let receipt = loop {
        let step = stepper.step(cluster)?;
        ensure!(
            !matches!(step.plan, Plan::Unsafe { .. }),
            "unsafe removal: {:?}",
            step.plan
        );
        if matches!(&step.plan, Plan::Execute {
            command: ProtocolCommand::EnsureConfiguration(c)
        } if c.local_replica_id.value() == 5 && c.current_only)
            && step.command.is_some()
        {
            ensure!(suspension.is_none(), "duplicate disappearance boundary");
            old_command = step.command;
            suspension = Some(suspend_replica_process(
                &cluster.kubeconfig,
                &cluster.context,
                5,
            )?);
            eprintln!(
                "historical recovery: suspended 5 after current-only, before local acceptance"
            );
        }
        let status = case.observe()?;
        if status["status"]["lastSecondaryRemoval"].is_object()
            && status["status"]["secondaryScaleDownCleanup"].is_null()
        {
            ensure!(suspension.is_some(), "missed retained acceptance gap");
            break status["status"]["lastSecondaryRemoval"].clone();
        }
        poll(
            case.deadline,
            "6->5 cleanup with locally unaccepted retained member",
        )?;
    };
    for (id, resource, _) in &case.resources {
        if *id == 6 {
            ensure!(
                resource.get(cluster)?.is_none(),
                "target resource remains after cleanup"
            );
        }
    }
    let acknowledged = case.acknowledged.clone();
    drop(case);
    let deadline = Instant::now() + Duration::from_secs(240);
    let peers = (2..=4)
        .map(|id| suspend_replica_process(&cluster.kubeconfig, &cluster.context, id))
        .collect::<Result<Vec<_>>>()?;
    loop {
        let step = stepper.step(cluster)?;
        ensure!(
            !matches!(step.plan, Plan::Unsafe { .. }),
            "unsafe quorum fence: {:?}",
            step.plan
        );
        if cluster.report(&primary)?["writeStatus"] == "NoWriteQuorum" {
            break;
        }
        poll(deadline, "close old primary before failover suspension")?;
    }
    let primary_suspension = suspend_replica_process(
        &cluster.kubeconfig,
        &cluster.context,
        primary["replicaId"].as_i64().unwrap(),
    )?;
    drop(peers);
    let accepted = loop {
        let step = stepper.step(cluster)?;
        ensure!(
            !matches!(step.plan, Plan::Unsafe { .. }),
            "unsafe failover: {:?}",
            step.plan
        );
        let status = cluster.status()?;
        ensure!(
            status["status"]["lastSecondaryRemoval"] == receipt,
            "failover changed removal receipt"
        );
        if topology_primary_id(&status) != primary["replicaId"].as_i64()
            && status["status"]["topology"]["epoch"]["configurationNumber"].as_i64()
                > receipt["evidence"]["preparation"]["intent"]["currentConfiguration"]["epoch"]["configurationNumber"].as_i64()
            && status["status"]["transition"].is_null()
        {
            break status["status"]["topology"].clone();
        }
        poll(deadline, "failover while retained 5 is absent")?;
    };
    ensure!(
        accepted["members"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| identity(m) == identity(&late)),
        "failover replaced the returning member"
    );
    eprintln!(
        "historical recovery: cleanup complete, newer failover accepted: {}",
        accepted["epoch"]
    );
    drop(suspension);
    drop(primary_suspension);
    let old_session = cluster.report(&late)?["processSession"].clone();
    stop_replica_process(&cluster.kubeconfig, &cluster.context, 5)?;
    loop {
        if cluster
            .report(&late)
            .is_ok_and(|r| r["processSession"] != old_session)
        {
            break;
        }
        poll(deadline, "fresh returning retained process session")?;
    }
    stepper = LiveStepper::new(cluster)?;
    let (endpoint, request) = old_command.context("old current-only command")?;
    stepper.reject_stale(&endpoint, request)?;
    let mut accepted_locally = false;
    let mut corrected = false;
    loop {
        let before = cluster.status()?;
        let step = stepper.step(cluster)?;
        ensure!(
            !matches!(step.plan, Plan::Unsafe { .. }),
            "unsafe return: {:?}",
            step.plan
        );
        match &step.plan {
            Plan::Execute {
                command: ProtocolCommand::AcceptSecondaryRemovalCommit(c),
            } if c.target.replica_id.value() == 5 => {
                ensure!(c.local_recovery, "not historical local-only acceptance");
                ensure!(
                    serde_json::to_value(&c.committed.evidence)? == receipt["evidence"],
                    "historical certificate mutated"
                );
                if step.command.is_some() {
                    accepted_locally = true;
                    ensure!(
                        cluster.status()?["status"] == before["status"],
                        "local replay rewrote cluster status"
                    );
                    let report = cluster.report(&late)?;
                    ensure!(
                        report["writeStatus"] != "Granted"
                            && report["currentConfiguration"]
                                == receipt["evidence"]["preparation"]["intent"]["currentConfiguration"]
                                    ["configurationId"],
                        "historical replay granted access or new authority"
                    );
                    eprintln!(
                        "historical recovery: exact local acceptance on 5, status and authority unchanged"
                    );
                }
            }
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(c),
            } if c.local_replica_id.value() == 5 => {
                ensure!(
                    accepted_locally,
                    "newer correction preceded observed local acceptance"
                );
                corrected |= c.current_only && step.command.is_some();
            }
            _ => {}
        }
        if corrected && matches!(step.plan, Plan::Stable { .. }) {
            break;
        }
        poll(
            deadline,
            "historical acceptance then ordinary failover correction",
        )?;
    }
    ensure!(
        cluster.status()?["status"]["lastSecondaryRemoval"] == receipt,
        "recovery rewrote receipt"
    );
    let stable = scale_ready(cluster, 5, deadline)?;
    let writer = stable["status"]["topology"]["members"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["role"] == "primary")
        .context("new primary")?;
    let mut client = DirectClient::connect(cluster, &cluster.pod(writer)?, deadline)?;
    for (key, value) in acknowledged {
        let (code, body) = client.request("GET", &format!("/kv/{key}"), "")?;
        ensure!(
            code == 200 && body == value,
            "failover lost acknowledged prefix"
        );
    }
    ensure!(
        client
            .request("PUT", "/kv/historical-recovery", "after-return")?
            .0
            == 200,
        "post-recovery quorum write failed"
    );
    eprintln!(
        "historical recovery: 6->5, cleanup, failover, restart, exact acceptance, correction, stable and durable read/write PASS"
    );
    drop(pause);
    Ok(())
}

#[test]
fn scale_down_reduction_parser_rejects_skipped_epochs_primary_or_identity_changes() -> Result<()> {
    let before = json!({"epoch":{"dataLossNumber":4,"configurationNumber":9},"writeQuorum":2,
        "members":[
            {"replicaId":7,"instanceId":"p","agentGeneration":"gp","role":"primary"},
            {"replicaId":2,"instanceId":"s2","agentGeneration":"g2","role":"activeSecondary"},
            {"replicaId":9,"instanceId":"s9","agentGeneration":"g9","role":"activeSecondary"}]});
    let mut after = before.clone();
    after["members"].as_array_mut().unwrap().pop();
    after["epoch"]["configurationNumber"] = json!(10);
    assert_eq!(check_reduction(&before, &after)?, 9);
    for pointer in [
        "/epoch/dataLossNumber",
        "/epoch/configurationNumber",
        "/members/0/replicaId",
        "/members/0/instanceId",
        "/members/1/agentGeneration",
        "/writeQuorum",
    ] {
        let mut invalid = after.clone();
        *invalid.pointer_mut(pointer).unwrap() = json!(99);
        assert!(check_reduction(&before, &invalid).is_err(), "{pointer}");
    }
    Ok(())
}

#[test]
fn scale_down_selectors_are_exact_ignored_and_in_all() {
    let recipes = include_str!("../../justfile");
    let nextest = include_str!("../../.config/nextest.toml");
    for (selector, test) in [
        ("scale-down", "scale_down"),
        ("scale-down-adversarial", "scale_down_adversarial"),
    ] {
        assert!(recipes.contains(&format!(
            "{selector}) test_name=\"level_triggered_k8s::{test}\""
        )));
        assert!(
            recipes
                .lines()
                .any(|line| line.contains("expanded+=(") && line.contains(selector))
        );
    }
    assert!(recipes.contains("cargo nextest run -p kuberic-level-tests"));
    assert!(recipes.contains("--profile kind --run-ignored only"));
    assert!(recipes.contains("group(=kind-live) and test(=${test_name})"));
    assert!(nextest.contains("[profile.kind]"));
    assert!(nextest.contains("test-group = 'kind-live'"));
}

struct CollectionWatch {
    _process: OwnedChild,
    events: Receiver<Value>,
}

impl CollectionWatch {
    fn start(cluster: &SwitchoverCluster, resource: &str) -> Result<Self> {
        let mut process = OwnedChild(
            Command::new("kubectl")
                .args([
                    "--kubeconfig",
                    &cluster.kubeconfig,
                    "--context",
                    &cluster.context,
                    "-n",
                    "default",
                    "get",
                    resource,
                    "-l",
                    "operator.kuberic.io/set-name=kvstore2",
                    "--watch",
                    "--output-watch-events",
                    "-o",
                    "json",
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()?,
        );
        let stdout = process.0.stdout.take().context("collection watch stdout")?;
        let (send, events) = channel();
        std::thread::spawn(move || {
            for event in serde_json::Deserializer::from_reader(stdout).into_iter::<Value>() {
                let Ok(event) = event else { break };
                if send.send(event).is_err() {
                    break;
                }
            }
        });
        Ok(Self {
            _process: process,
            events,
        })
    }
}

#[derive(Clone, Debug)]
struct ResourceCreation {
    name: String,
    uid: String,
    resource_version: u64,
}

#[derive(Clone, Debug)]
struct ScaleUpCommit {
    count: usize,
    target: i64,
    resource_version: u64,
}

#[derive(Clone, Debug)]
struct AcknowledgedValue {
    key: String,
    value: String,
    lsn: i64,
}

#[derive(Clone, Debug)]
struct PhaseWriteEvidence {
    target: i64,
    phase: &'static str,
    boundary_lsn: i64,
    write_lsn: i64,
    candidate: Value,
    candidate_process_session: String,
    build_id: String,
    reached_candidate: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ActiveScaleUpCopyCheckpoint {
    operation_id: String,
    build_id: String,
    source: Value,
    source_process_session: String,
    candidate: Value,
    candidate_process_session: String,
    snapshot_boundary_lsn: i64,
}

fn active_scale_up_copy_checkpoint(
    status: &Value,
    source: &Value,
    candidate: &Value,
    expected_source_session: &str,
    expected_candidate_session: &str,
) -> Result<ActiveScaleUpCopyCheckpoint> {
    use kuberic_protocol::types::{ProvisioningIntent, ReplicaRole, ResourceUid};

    let provisioning: ProvisioningIntent =
        serde_json::from_value(status["status"]["provisioning"].clone())
            .context("active scale-up provisioning")?;
    let scale_up = provisioning
        .scale_up()
        .context("active provisioning is not scale-up")?;
    let resource_uid = ResourceUid::new(
        status["metadata"]["uid"]
            .as_str()
            .context("active scale-up resource UID")?,
    );
    ensure!(
        provisioning.operation_id == provisioning.expected_operation_id(),
        "active scale-up provisioning operation is not deterministic"
    );
    let build_id = provisioning
        .scale_up_build_id(&resource_uid)
        .context("active scale-up build ID")?;
    let target = provisioning.target_identity(&resource_uid);
    let primary = scale_up
        .previous_configuration
        .members
        .iter()
        .find(|member| {
            member.identity.replica_id == scale_up.previous_configuration.primary_id
                && member.role == ReplicaRole::Primary
        })
        .context("active scale-up primary")?
        .identity
        .clone();
    let target_json = serde_json::to_value(&target)?;
    let primary_json = serde_json::to_value(&primary)?;
    ensure!(
        status["status"]["transition"].is_null()
            && status["status"]["scaleUpAdmissionStarted"].is_null(),
        "active-copy checkpoint was observed after transition/admission began"
    );
    ensure!(
        status["status"]["topology"]["configurationId"]
            == scale_up.previous_configuration.configuration_id.as_str(),
        "active-copy checkpoint no longer has the exact previous accepted configuration"
    );
    ensure!(
        status["status"]["topology"]["members"]
            .as_array()
            .is_some_and(|members| members.iter().all(|member| identity(member) != target_json)),
        "active-copy candidate already has accepted membership credit"
    );
    ensure!(
        identity(source) == primary_json
            && source["role"] == "Primary"
            && source["previousConfiguration"].is_null()
            && source["currentConfiguration"]
                == scale_up.previous_configuration.configuration_id.as_str(),
        "active-copy source differs from exact pre-admission authority: {source}"
    );
    ensure!(
        identity(candidate) == target_json
            && candidate["role"] == "IdleSecondary"
            && candidate["previousConfiguration"].is_null()
            && candidate["currentConfiguration"].is_null()
            && candidate["scaleUpOperation"].is_null()
            && matches!(
                candidate["readStatus"].as_str(),
                Some("NotPrimary" | "ReconfigurationPending")
            )
            && matches!(
                candidate["writeStatus"].as_str(),
                Some("NotPrimary" | "ReconfigurationPending")
            ),
        "active-copy candidate is not exact, idle, and unadmitted: {candidate}"
    );
    let source_session = source["processSession"]
        .as_str()
        .filter(|session| !session.is_empty())
        .context("active-copy source process session")?;
    let candidate_session = candidate["processSession"]
        .as_str()
        .filter(|session| !session.is_empty())
        .context("active-copy candidate process session")?;
    ensure!(
        source_session == expected_source_session
            && candidate_session == expected_candidate_session,
        "active-copy diagnostics differ from exact observed process sessions: \
         source={source_session}/{expected_source_session} \
         candidate={candidate_session}/{expected_candidate_session}"
    );
    let exact_build = |report: &Value| {
        report["builds"]
            .as_array()
            .and_then(|builds| {
                builds.iter().find(|build| {
                    build["buildId"].as_str() == Some(build_id.as_str())
                        && build["targetInstance"].as_str() == Some(target.instance_id.as_str())
                })
            })
            .cloned()
    };
    let source_build = exact_build(source);
    let candidate_build = exact_build(candidate);
    ensure!(
        source_build.is_some() || candidate_build.is_some(),
        "source and candidate both lack the exact active scale-up build"
    );
    if source_build.is_none() {
        ensure!(
            source["pendingOperation"]
                .as_str()
                .is_some_and(|operation| operation == format!("{build_id}:build-replica")),
            "source lacks both exact build report and exact pending build effect: {source}"
        );
    }
    let boundary = source_build
        .as_ref()
        .or(candidate_build.as_ref())
        .and_then(|build| build["replicationBoundaryLsn"].as_i64())
        .context("active snapshot boundary")?;
    ensure!(boundary >= 0, "active snapshot boundary is negative");
    if let (Some(source_build), Some(candidate_build)) = (&source_build, &candidate_build) {
        ensure!(
            source_build["replicationBoundaryLsn"] == candidate_build["replicationBoundaryLsn"],
            "source/candidate active builds disagree on snapshot boundary: \
             source={source_build} candidate={candidate_build}"
        );
    }
    ensure!(
        source_build
            .iter()
            .chain(candidate_build.iter())
            .any(|build| build["completed"] == false),
        "exact build is already complete rather than actively held"
    );
    ensure!(
        source_build
            .iter()
            .chain(candidate_build.iter())
            .all(|build| build["catchUpBoundaryLsn"].is_null()),
        "active-copy checkpoint was observed after copy enumeration completed"
    );
    Ok(ActiveScaleUpCopyCheckpoint {
        operation_id: provisioning.operation_id.to_string(),
        build_id: build_id.to_string(),
        source: primary_json,
        source_process_session: source_session.to_string(),
        candidate: target_json,
        candidate_process_session: candidate_session.to_string(),
        snapshot_boundary_lsn: boundary,
    })
}

fn active_copy_observed_sessions(
    cluster: &SwitchoverCluster,
    status: &Value,
) -> Result<(String, String)> {
    use kuberic_protocol::observation::AgentObservation;
    use kuberic_protocol::types::{ProvisioningIntent, ReplicaRole, ResourceUid};

    let provisioning: ProvisioningIntent =
        serde_json::from_value(status["status"]["provisioning"].clone())?;
    let scale_up = provisioning.scale_up().context("scale-up provisioning")?;
    let resource_uid = ResourceUid::new(
        status["metadata"]["uid"]
            .as_str()
            .context("scale-up resource UID")?,
    );
    let target = provisioning.target_identity(&resource_uid);
    let primary = scale_up
        .previous_configuration
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .context("scale-up primary")?
        .identity
        .clone();
    let snapshot = LiveStepper::new(cluster)?.observe_snapshot(cluster)?;
    let session = |identity| {
        snapshot
            .replicas
            .values()
            .find_map(|observation| match &observation.agent {
                AgentObservation::Report(report) if report.identity == identity => {
                    Some(report.process_session_id.to_string())
                }
                _ => None,
            })
            .with_context(|| format!("exact observed process session for {identity:?}"))
    };
    Ok((session(primary)?, session(target)?))
}

fn active_copy_reconstruction_checkpoint(
    cluster: &SwitchoverCluster,
    status: &Value,
) -> Result<ActiveScaleUpCopyCheckpoint> {
    use kuberic_protocol::observation::AgentObservation;
    use kuberic_protocol::types::{OperationId, ProvisioningIntent, ReplicaRole, ResourceUid};

    let provisioning: ProvisioningIntent =
        serde_json::from_value(status["status"]["provisioning"].clone())?;
    let scale_up = provisioning.scale_up().context("scale-up provisioning")?;
    let resource_uid = ResourceUid::new(
        status["metadata"]["uid"]
            .as_str()
            .context("scale-up resource UID")?,
    );
    let target = provisioning.target_identity(&resource_uid);
    let build_id = provisioning
        .scale_up_build_id(&resource_uid)
        .context("scale-up build ID")?;
    let primary = scale_up
        .previous_configuration
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .context("scale-up primary")?
        .identity
        .clone();
    ensure!(
        status["status"]["transition"].is_null()
            && status["status"]["scaleUpAdmissionStarted"].is_null()
            && status["status"]["topology"]["configurationId"]
                == scale_up.previous_configuration.configuration_id.as_str(),
        "reconstructed active copy crossed transition/admission authority"
    );
    let snapshot = LiveStepper::new(cluster)?.observe_snapshot(cluster)?;
    let report = |identity| {
        snapshot
            .replicas
            .values()
            .find_map(|observation| match &observation.agent {
                AgentObservation::Report(report) if report.identity == identity => {
                    Some(report.as_ref())
                }
                _ => None,
            })
            .with_context(|| format!("exact reconstructed report for {identity:?}"))
    };
    let source = report(primary.clone())?;
    let candidate = report(target.clone())?;
    ensure!(
        source.role == ReplicaRole::Primary
            && source.previous_configuration.is_none()
            && source.current_configuration.as_ref() == Some(&scale_up.previous_configuration)
            && candidate.role == ReplicaRole::IdleSecondary
            && candidate.previous_configuration.is_none()
            && candidate.current_configuration.is_none()
            && candidate.scale_up_intent.is_none(),
        "reconstructed active copy installed membership authority prematurely"
    );
    let source_build = source
        .builds
        .iter()
        .find(|build| build.build_id == build_id && build.target == target);
    let candidate_build = candidate
        .builds
        .iter()
        .find(|build| build.build_id == build_id && build.target == target);
    ensure!(
        source_build.is_some() || candidate_build.is_some(),
        "reconstructed source/candidate reports omit the exact build"
    );
    if source_build.is_none() {
        ensure!(
            source.pending_operation_id.as_ref()
                == Some(&OperationId::new(format!("{build_id}:build-replica"))),
            "reconstructed source lacks exact pending build authority"
        );
    }
    let boundary = source_build
        .or(candidate_build)
        .map(|build| build.replication_boundary_lsn)
        .context("reconstructed snapshot boundary")?;
    ensure!(
        source_build
            .into_iter()
            .chain(candidate_build)
            .all(|build| {
                build.replication_boundary_lsn == boundary
                    && !build.completed
                    && build.catch_up_boundary_lsn.is_none()
            }),
        "reconstructed exact build is conflicting or no longer active"
    );
    Ok(ActiveScaleUpCopyCheckpoint {
        operation_id: provisioning.operation_id.to_string(),
        build_id: build_id.to_string(),
        source: serde_json::to_value(primary)?,
        source_process_session: source.process_session_id.to_string(),
        candidate: serde_json::to_value(target)?,
        candidate_process_session: candidate.process_session_id.to_string(),
        snapshot_boundary_lsn: boundary,
    })
}

struct LiveCopyGate {
    cleanup: GateCleanupOwner<LiveCopyGateCleanup>,
}

trait CopyGateCleanup {
    fn cleanup(&mut self) -> Result<()>;
}

struct GateCleanupOwner<C: CopyGateCleanup> {
    cleanup: C,
    armed: bool,
}

impl<C: CopyGateCleanup> GateCleanupOwner<C> {
    fn release(&mut self) -> Result<()> {
        if self.armed {
            self.cleanup.cleanup()?;
            self.armed = false;
        }
        Ok(())
    }
}

impl<C: CopyGateCleanup> Drop for GateCleanupOwner<C> {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

fn arm_with_cleanup<C: CopyGateCleanup>(
    cleanup: C,
    arm: impl FnOnce() -> Result<()>,
) -> Result<GateCleanupOwner<C>> {
    let owner = GateCleanupOwner {
        cleanup,
        armed: true,
    };
    arm()?;
    Ok(owner)
}

#[cfg(test)]
struct FileCopyGateCleanup {
    path: PathBuf,
}

#[cfg(test)]
impl CopyGateCleanup for FileCopyGateCleanup {
    fn cleanup(&mut self) -> Result<()> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

#[test]
fn lost_copy_gate_hold_response_still_runs_preowned_cleanup() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("copy-gate");
    let result = arm_with_cleanup(
        FileCopyGateCleanup { path: path.clone() },
        || -> Result<()> {
            std::fs::write(&path, b"armed")?;
            anyhow::bail!("simulated lost hold response")
        },
    );
    assert!(result.is_err());
    assert!(!path.exists());
}

struct LiveCopyGateCleanup {
    cluster: SwitchoverCluster,
    source: Value,
    deadline: Instant,
}

impl LiveCopyGateCleanup {
    fn pod(&self) -> Result<String> {
        self.cluster.pod(&self.source)
    }

    fn sentinel_absent(&self) -> Result<()> {
        let pod = self.pod()?;
        self.cluster
            .kubectl(&[
                "-n",
                "default",
                "exec",
                &pod,
                "--",
                "test",
                "!",
                "-e",
                "/var/lib/kuberic/.live-test-copy-gate",
            ])
            .context("live-test copy gate sentinel remained present")?;
        Ok(())
    }
}

impl CopyGateCleanup for LiveCopyGateCleanup {
    fn cleanup(&mut self) -> Result<()> {
        let pod = self.pod()?;
        let release = (|| {
            let mut client = DirectClient::connect_port(&self.cluster, &pod, 18080, self.deadline)?;
            let (code, body) = client.request("PUT", "/live-test/copy-gate/release", "")?;
            ensure!(
                code == 200 && body == "released",
                "copy gate release failed: HTTP {code} {body}"
            );
            Ok(())
        })();
        if let Err(http_error) = release {
            self.cluster
                .kubectl(&[
                    "-n",
                    "default",
                    "exec",
                    &pod,
                    "--",
                    "rm",
                    "-f",
                    "/var/lib/kuberic/.live-test-copy-gate",
                ])
                .with_context(|| format!("copy gate HTTP release also failed: {http_error:#}"))?;
        }
        self.sentinel_absent()
    }
}

impl LiveCopyGate {
    fn hold(cluster: &SwitchoverCluster, source: &Value, deadline: Instant) -> Result<Self> {
        let pod = cluster.pod(source)?;
        let cleanup = LiveCopyGateCleanup {
            cluster: cluster.clone(),
            source: source.clone(),
            deadline,
        };
        let owner = arm_with_cleanup(cleanup, || {
            let mut client = DirectClient::connect_port(cluster, &pod, 18080, deadline)?;
            let (code, body) = client.request("PUT", "/live-test/copy-gate/hold", "")?;
            ensure!(
                code == 200 && body == "held",
                "copy gate hold failed: HTTP {code} {body}"
            );
            Ok(())
        })?;
        Ok(Self { cleanup: owner })
    }

    fn release(&mut self) -> Result<()> {
        self.cleanup.release()
    }
}

struct HeldActiveCopy {
    gate: LiveCopyGate,
    step: Option<std::thread::JoinHandle<Result<LiveStep>>>,
}

impl HeldActiveCopy {
    fn release_and_finish(mut self) -> Result<()> {
        self.gate.release()?;
        if let Some(step) = self.step.take() {
            step.join()
                .map_err(|_| anyhow::anyhow!("active copy controller step panicked"))??;
        }
        Ok(())
    }
}

impl Drop for HeldActiveCopy {
    fn drop(&mut self) {
        let _ = self.gate.release();
        if let Some(step) = self.step.take() {
            let _ = step.join();
        }
    }
}

fn begin_exact_active_copy(
    cluster: &SwitchoverCluster,
    source: &Value,
    target: i64,
    deadline: Instant,
) -> Result<(ActiveScaleUpCopyCheckpoint, HeldActiveCopy)> {
    let gate = LiveCopyGate::hold(cluster, source, deadline)?;
    let mut step = None;
    let mut build_dispatched = false;
    loop {
        let status = cluster.status()?;
        assert_scale_up_condition_context(&status)?;
        if let (Ok(source_report), Ok(candidate_report)) = (
            replica_diagnostics(
                &cluster.kubeconfig,
                &cluster.context,
                source["replicaId"].as_i64().unwrap(),
            ),
            replica_diagnostics(&cluster.kubeconfig, &cluster.context, target),
        ) && let Ok((source_session, candidate_session)) =
            active_copy_observed_sessions(cluster, &status)
            && let Ok(checkpoint) = active_scale_up_copy_checkpoint(
                &status,
                &source_report,
                &candidate_report,
                &source_session,
                &candidate_session,
            )
        {
            eprintln!(
                "held exact active scale-up copy: operation={}, build={}, source_session={}, \
                 candidate_session={}, snapshot_boundary={}",
                checkpoint.operation_id,
                checkpoint.build_id,
                checkpoint.source_process_session,
                checkpoint.candidate_process_session,
                checkpoint.snapshot_boundary_lsn
            );
            return Ok((checkpoint, HeldActiveCopy { gate, step }));
        }
        if step
            .as_ref()
            .is_some_and(std::thread::JoinHandle::is_finished)
        {
            let finished = step
                .take()
                .expect("finished active-copy step remains present")
                .join()
                .map_err(|_| anyhow::anyhow!("active copy controller step panicked"))?;
            finished?;
        }
        if !build_dispatched
            && scale_up_progress_condition(&status)
                .and_then(|condition| condition["reason"].as_str())
                == Some("ScaleUpCopying")
            && status["status"]["transition"].is_null()
        {
            let owned = cluster.clone();
            build_dispatched = true;
            step = Some(std::thread::spawn(move || {
                manual_live_step(&owned, deadline)
            }));
        } else if !build_dispatched {
            manual_live_step(cluster, deadline)?;
        }
        poll(deadline, "exact active scale-up copy checkpoint")?;
    }
}

fn candidate_delivery_matches(report: &Value, evidence: &PhaseWriteEvidence) -> bool {
    if identity(report) != evidence.candidate
        || report["processSession"].as_str() != Some(&evidence.candidate_process_session)
    {
        return false;
    }
    let Some(build) = report["builds"].as_array().and_then(|builds| {
        builds.iter().find(|build| {
            build["buildId"].as_str() == Some(&evidence.build_id)
                && build["targetInstance"] == evidence.candidate["instanceId"]
        })
    }) else {
        return false;
    };
    report["currentProgress"]
        .as_i64()
        .is_some_and(|progress| progress >= evidence.write_lsn)
        || build["durableLsn"]
            .as_i64()
            .is_some_and(|durable| durable >= evidence.write_lsn)
}

#[derive(Clone, Debug, Default)]
struct StatusObservationWatermark {
    resource_version: u64,
    generation: u64,
    initialized: bool,
}

impl StatusObservationWatermark {
    fn accept(&mut self, object: &Value) -> Result<bool> {
        let next_resource_version = resource_version(object)?;
        let next_generation = object["metadata"]["generation"]
            .as_u64()
            .context("metadata generation")?;
        if self.initialized && next_resource_version <= self.resource_version {
            return Ok(false);
        }
        ensure!(
            !self.initialized || next_generation >= self.generation,
            "newer resourceVersion {next_resource_version} regressed generation \
             {} -> {next_generation}",
            self.generation
        );
        self.resource_version = next_resource_version;
        self.generation = next_generation;
        self.initialized = true;
        Ok(true)
    }
}

fn resource_version(object: &Value) -> Result<u64> {
    object["metadata"]["resourceVersion"]
        .as_str()
        .context("resourceVersion")?
        .parse()
        .context("numeric resourceVersion")
}

fn replica_label(object: &Value) -> Option<i64> {
    object["metadata"]["labels"]["operator.kuberic.io/replica-id"]
        .as_str()?
        .parse()
        .ok()
}

fn scale_up_progress_condition(object: &Value) -> Option<&Value> {
    object["status"]["conditions"]
        .as_array()?
        .iter()
        .find(|condition| {
            condition["reason"]
                .as_str()
                .is_some_and(|reason| reason.starts_with("ScaleUp"))
        })
}

fn assert_scale_up_condition_context(object: &Value) -> Result<()> {
    let Some(condition) = scale_up_progress_condition(object) else {
        return Ok(());
    };
    let reason = condition["reason"].as_str().context("scale-up reason")?;
    let message = condition["message"]
        .as_str()
        .context("scale-up condition message")?;
    for field in [
        "accepted=",
        "desired=",
        "target=",
        "attempt=",
        "phase=",
        "blocking=",
    ] {
        ensure!(
            message.contains(field),
            "{reason} omitted {field} diagnostic context: {message}"
        );
    }
    Ok(())
}

fn check_expansion(before: &Value, after: &Value) -> Result<i64> {
    let old = before["members"]
        .as_array()
        .context("old topology members")?;
    let new = after["members"]
        .as_array()
        .context("new topology members")?;
    ensure!(
        new.len() == old.len() + 1,
        "scale-up did not add exactly one member: {} -> {}",
        old.len(),
        new.len()
    );
    for member in old {
        ensure!(
            new.iter().any(|candidate| candidate == member),
            "scale-up changed retained exact member: {member}"
        );
    }
    let added = new
        .iter()
        .find(|member| old.iter().all(|retained| retained != *member))
        .context("new exact member")?;
    ensure!(
        added["role"] == "activeSecondary",
        "new member was not an ActiveSecondary: {added}"
    );
    ensure!(
        before["primaryId"] == after["primaryId"],
        "healthy scale-up changed primary"
    );
    ensure!(
        before["epoch"]["dataLossNumber"] == after["epoch"]["dataLossNumber"],
        "healthy scale-up changed data-loss epoch"
    );
    ensure!(
        after["epoch"]["configurationNumber"].as_i64()
            == before["epoch"]["configurationNumber"]
                .as_i64()
                .map(|number| number + 1),
        "scale-up did not advance exactly one configuration epoch"
    );
    ensure!(
        after["writeQuorum"].as_u64() == Some((new.len() / 2 + 1) as u64),
        "expanded topology has the wrong majority write quorum"
    );
    let expected = (1..=new.len() as i64)
        .find(|candidate| {
            old.iter()
                .all(|member| member["replicaId"].as_i64() != Some(*candidate))
        })
        .context("next cardinality-derived logical ID")?;
    ensure!(
        added["replicaId"].as_i64() == Some(expected),
        "scale-up added the wrong logical ID: expected {expected}, got {added}"
    );
    Ok(expected)
}

fn exact_member(status: &Value, replica_id: i64) -> Result<&Value> {
    status["status"]["topology"]["members"]
        .as_array()
        .context("accepted topology members")?
        .iter()
        .find(|member| member["replicaId"].as_i64() == Some(replica_id))
        .with_context(|| format!("accepted member {replica_id}"))
}

fn exact_replica_resources(
    cluster: &SwitchoverCluster,
    replica_id: i64,
) -> Result<Vec<ScaleResource>> {
    let pod_name = pod_for_replica(&cluster.kubeconfig, &cluster.context, replica_id)?;
    let pod: Value = serde_json::from_str(
        &cluster.kubectl(&["-n", "default", "get", "pod", &pod_name, "-o", "json"])?,
    )?;
    let pvc_name = pod["spec"]["volumes"]
        .as_array()
        .context("candidate volumes")?
        .iter()
        .find_map(|volume| volume["persistentVolumeClaim"]["claimName"].as_str())
        .context("candidate PVC name")?;
    let pvc: Value = serde_json::from_str(
        &cluster.kubectl(&["-n", "default", "get", "pvc", pvc_name, "-o", "json"])?,
    )?;
    let services: Value = serde_json::from_str(
        &cluster.kubectl(&["-n", "default", "get", "services", "-o", "json"])?,
    )?;
    let endpoint = services["items"]
        .as_array()
        .context("Service list")?
        .iter()
        .find(|service| {
            service["metadata"]["name"] != "kvstore2-write"
                && routing_instance(service) == pod["metadata"]["uid"].as_str()
        })
        .context("exact candidate peer Service")?
        .clone();
    Ok(vec![
        ScaleResource {
            kind: "service",
            object: endpoint,
        },
        ScaleResource {
            kind: "pod",
            object: pod,
        },
        ScaleResource {
            kind: "pvc",
            object: pvc,
        },
    ])
}

struct ScaleUpCase<'a> {
    cluster: &'a SwitchoverCluster,
    start: Value,
    accepted: Value,
    primary: Value,
    writer: DirectClient,
    status: KubeWatch,
    pods: CollectionWatch,
    pvcs: CollectionWatch,
    pod_creations: std::collections::BTreeMap<i64, ResourceCreation>,
    pvc_creations: std::collections::BTreeMap<i64, ResourceCreation>,
    commits: Vec<ScaleUpCommit>,
    phase_reasons: std::collections::BTreeSet<String>,
    phase_writes: std::collections::BTreeMap<(i64, &'static str), PhaseWriteEvidence>,
    acknowledged: Vec<AcknowledgedValue>,
    status_watermark: StatusObservationWatermark,
    started: Instant,
    deadline: Instant,
}

impl<'a> ScaleUpCase<'a> {
    fn new(cluster: &'a SwitchoverCluster, count: usize) -> Result<Self> {
        let start = scale_ready(cluster, count, Instant::now() + Duration::from_secs(180))?;
        let primary = start["status"]["topology"]["members"]
            .as_array()
            .context("starting members")?
            .iter()
            .find(|member| member["role"] == "primary")
            .context("starting primary")?
            .clone();
        let started = Instant::now();
        let deadline = started + Duration::from_secs(900);
        let writer = DirectClient::connect(cluster, &cluster.pod(&primary)?, deadline)?;
        let status = KubeWatch::start(cluster, "kubericset", "kvstore2")?;
        let pods = CollectionWatch::start(cluster, "pods")?;
        let pvcs = CollectionWatch::start(cluster, "persistentvolumeclaims")?;
        let mut case = Self {
            cluster,
            accepted: start["status"]["topology"].clone(),
            start,
            primary,
            writer,
            status,
            pods,
            pvcs,
            pod_creations: Default::default(),
            pvc_creations: Default::default(),
            commits: Vec::new(),
            phase_reasons: Default::default(),
            phase_writes: Default::default(),
            acknowledged: Vec::new(),
            status_watermark: Default::default(),
            started,
            deadline,
        };
        let initial = case.status.initial.clone();
        case.process_status(&initial)?;
        for suffix in ["seed-a", "seed-b", "seed-c"] {
            case.write(suffix)?;
        }
        Ok(case)
    }

    fn submit(&self, count: usize) -> Result<()> {
        self.cluster.kubectl(&[
            "-n",
            "default",
            "patch",
            "kubericset",
            "kvstore2",
            "--type=merge",
            "-p",
            &json!({"spec":{"replicas":count}}).to_string(),
        ])?;
        Ok(())
    }

    fn write(&mut self, phase: &str) -> Result<i64> {
        let key = format!(
            "scale-up-{}-{phase}-{}",
            self.start["metadata"]["uid"].as_str().context("set UID")?,
            self.acknowledged.len()
        );
        let value = format!("acknowledged-{key}");
        let before = self.writer.report()?["committedLsn"]
            .as_i64()
            .context("committed LSN before routed write")?;
        assert_routed_service_write(self.cluster, &self.primary, &key, &value, self.deadline)?;
        let lsn = loop {
            let committed = self.writer.report()?["committedLsn"]
                .as_i64()
                .context("committed LSN after routed write")?;
            if committed > before {
                break committed;
            }
            poll(self.deadline, "routed write committed LSN")?;
        };
        self.acknowledged
            .push(AcknowledgedValue { key, value, lsn });
        Ok(lsn)
    }

    fn record_resource_events(
        events: &Receiver<Value>,
        destination: &mut std::collections::BTreeMap<i64, ResourceCreation>,
    ) -> Result<()> {
        for event in events.try_iter() {
            ensure!(event["type"] != "ERROR", "resource watch failed: {event}");
            let object = &event["object"];
            let Some(replica_id) = replica_label(object) else {
                continue;
            };
            if event["type"] == "ADDED" {
                destination.entry(replica_id).or_insert(ResourceCreation {
                    name: object["metadata"]["name"]
                        .as_str()
                        .context("resource name")?
                        .to_string(),
                    uid: object["metadata"]["uid"]
                        .as_str()
                        .context("resource UID")?
                        .to_string(),
                    resource_version: resource_version(object)?,
                });
            }
        }
        Ok(())
    }

    fn process_status(&mut self, object: &Value) -> Result<()> {
        if !self.status_watermark.accept(object)? {
            return Ok(());
        }
        ensure!(
            !object["status"]["conditions"]
                .as_array()
                .is_some_and(|conditions| conditions.iter().any(|condition| {
                    condition["type"] == "Unsafe" && condition["status"] == "true"
                })),
            "scale-up entered Unsafe: {}",
            object["status"]["conditions"]
        );
        assert_scale_up_condition_context(object)?;
        if let Some(condition) = scale_up_progress_condition(object)
            && let Some(reason) = condition["reason"].as_str()
        {
            self.phase_reasons.insert(reason.to_string());
        }
        let topology = &object["status"]["topology"];
        if topology.is_object() && topology != &self.accepted {
            let target = check_expansion(&self.accepted, topology)?;
            let count = topology["members"]
                .as_array()
                .context("expanded members")?
                .len();
            self.commits.push(ScaleUpCommit {
                count,
                target,
                resource_version: resource_version(object)?,
            });
            self.accepted = topology.clone();
            eprintln!(
                "scale-up accepted target={target}, count={count}, epoch={}, elapsed={:?}",
                topology["epoch"],
                self.started.elapsed()
            );
        }
        Ok(())
    }

    fn observe(&mut self) -> Result<Value> {
        Self::record_resource_events(&self.pods.events, &mut self.pod_creations)?;
        Self::record_resource_events(&self.pvcs.events, &mut self.pvc_creations)?;
        let events = self.status.events.try_iter().collect::<Vec<_>>();
        for event in events {
            ensure!(event["type"] != "ERROR", "status watch failed: {event}");
            self.process_status(&event["object"])?;
            self.maybe_write_in_live_phase(&event["object"])?;
        }
        let current = self.cluster.status()?;
        self.process_status(&current)?;
        self.maybe_write_in_live_phase(&current)?;
        Ok(current)
    }

    fn record_candidate_delivery(&mut self) -> Result<()> {
        let pending = self
            .phase_writes
            .iter()
            .filter_map(|(key, evidence)| {
                (!evidence.reached_candidate).then_some((*key, evidence.clone()))
            })
            .collect::<Vec<_>>();
        for (key, evidence) in pending {
            let report = replica_diagnostics(
                &self.cluster.kubeconfig,
                &self.cluster.context,
                evidence.target,
            )?;
            let reached = candidate_delivery_matches(&report, &evidence);
            if reached {
                self.phase_writes
                    .get_mut(&key)
                    .expect("pending phase write remains present")
                    .reached_candidate = true;
            }
        }
        Ok(())
    }

    fn write_during_exact_active_copy(&mut self, target: i64) -> Result<()> {
        let (checkpoint, held) =
            begin_exact_active_copy(self.cluster, &self.primary, target, self.deadline)?;
        ensure!(
            checkpoint.candidate["replicaId"].as_i64() == Some(target),
            "held copy targeted the wrong candidate: {checkpoint:?}"
        );
        let write_lsn = self.write(&format!("copy-target-{target}"))?;
        ensure!(
            write_lsn > checkpoint.snapshot_boundary_lsn,
            "copy-phase acknowledged write LSN {write_lsn} did not exceed \
             snapshot boundary {}",
            checkpoint.snapshot_boundary_lsn
        );
        self.phase_writes.insert(
            (target, "copy"),
            PhaseWriteEvidence {
                target,
                phase: "copy",
                boundary_lsn: checkpoint.snapshot_boundary_lsn,
                write_lsn,
                candidate: checkpoint.candidate,
                candidate_process_session: checkpoint.candidate_process_session,
                build_id: checkpoint.build_id,
                reached_candidate: false,
            },
        );
        held.release_and_finish()?;
        Ok(())
    }

    fn maybe_write_in_live_phase(&mut self, status: &Value) -> Result<()> {
        let target = self.accepted["members"]
            .as_array()
            .context("accepted members")?
            .len() as i64
            + 1;
        self.record_candidate_delivery()?;
        if !self.phase_writes.contains_key(&(target, "admission")) {
            let intent = &status["status"]["transition"]["scaleUp"];
            if intent.is_object()
                && status["status"]["scaleUpAdmissionStarted"] == intent["operationId"]
                && status["status"]["topology"]["configurationId"]
                    == intent["previousConfiguration"]["configurationId"]
                && let Ok(intent) =
                    serde_json::from_value::<kuberic_protocol::types::ScaleUpIntent>(intent.clone())
                && let Ok(current) =
                    serde_json::from_value::<kuberic_protocol::types::ConfigurationDescriptor>(
                        status["status"]["transition"]["currentConfiguration"].clone(),
                    )
                && let Ok(snapshot) = LiveStepper::new(self.cluster)
                    .and_then(|stepper| stepper.observe_snapshot(self.cluster))
            {
                use kuberic_protocol::observation::AgentObservation;
                let reports =
                    snapshot
                        .replicas
                        .values()
                        .filter_map(|observation| match &observation.agent {
                            AgentObservation::Report(report) => Some(report.as_ref()),
                            _ => None,
                        });
                let source = reports
                    .clone()
                    .find(|report| report.identity == intent.primary);
                let candidate = reports
                    .clone()
                    .find(|report| report.identity == intent.target);
                let exact_pc_cc = |report: &kuberic_protocol::observation::AgentReport| {
                    report.previous_configuration.as_ref() == Some(&intent.previous_configuration)
                        && report.current_configuration.as_ref() == Some(&current)
                        && report.scale_up_intent.as_deref() == Some(&intent)
                        && report.pending_operation_id.is_none()
                };
                if source.is_some_and(|report| {
                    exact_pc_cc(report)
                        && report.identity == intent.primary
                        && report.role == kuberic_protocol::types::ReplicaRole::Primary
                }) && candidate.is_some_and(|report| {
                    exact_pc_cc(report)
                        && report.identity == intent.target
                        && report.role == kuberic_protocol::types::ReplicaRole::ActiveSecondary
                }) {
                    let write_lsn = self.write(&format!("admission-target-{target}"))?;
                    let boundary_lsn = intent.catch_up_boundary_lsn;
                    ensure!(
                        write_lsn > boundary_lsn,
                        "admission write LSN {write_lsn} did not exceed catch-up boundary {boundary_lsn}"
                    );
                    self.phase_writes.insert(
                        (target, "admission"),
                        PhaseWriteEvidence {
                            target,
                            phase: "admission",
                            boundary_lsn,
                            write_lsn,
                            candidate: serde_json::to_value(&intent.target)?,
                            candidate_process_session: candidate
                                .expect("exact PC/CC candidate")
                                .process_session_id
                                .to_string(),
                            build_id: intent.build_id.to_string(),
                            reached_candidate: false,
                        },
                    );
                }
            }
        }
        Ok(())
    }

    fn stable_at(&self, status: &Value, count: usize) -> bool {
        status_ready(status)
            && status["status"]["transition"].is_null()
            && status["status"]["provisioning"].is_null()
            && status["status"]["scaleUpAllocation"].is_null()
            && status["status"]["scaleUpCleanup"].is_null()
            && status["status"]["topology"]["members"]
                .as_array()
                .is_some_and(|members| members.len() == count)
    }

    fn wait_for_count_manually(&mut self, count: usize) -> Result<Value> {
        loop {
            let status = self.observe()?;
            if self.stable_at(&status, count) {
                self.process_status(&status)?;
                return Ok(status);
            }
            let target = self.accepted["members"]
                .as_array()
                .context("accepted members")?
                .len() as i64
                + 1;
            if target <= count as i64 && !self.phase_writes.contains_key(&(target, "copy")) {
                self.write_during_exact_active_copy(target)?;
            } else {
                manual_live_step(self.cluster, self.deadline)?;
            }
            poll(
                self.deadline,
                &format!("manually stepped stable scale-up to {count}"),
            )?;
        }
    }

    fn assert_resource_order(&self, replica_id: i64) -> Result<()> {
        let pvc = self
            .pvc_creations
            .get(&replica_id)
            .with_context(|| format!("PVC creation for replica {replica_id}"))?;
        let pod = self
            .pod_creations
            .get(&replica_id)
            .with_context(|| format!("Pod creation for replica {replica_id}"))?;
        ensure!(
            pvc.resource_version < pod.resource_version,
            "candidate Pod was not observed after its PVC: pvc={pvc:?}, pod={pod:?}"
        );
        ensure!(
            pvc.name == format!("kvstore2-{replica_id}-data")
                && pod.name == format!("kvstore2-{replica_id}"),
            "candidate did not use canonical ordinal names: pvc={pvc:?}, pod={pod:?}"
        );
        ensure!(
            pvc.uid != pod.uid,
            "Pod and PVC unexpectedly shared an identity"
        );
        Ok(())
    }

    fn verify_final(&mut self, status: &Value, count: usize) -> Result<()> {
        let topology = &status["status"]["topology"];
        ensure!(
            status["status"]["effectivePolicy"]["replicaSetSize"].as_u64() == Some(count as u64)
                && status["status"]["effectivePolicy"]["writeQuorum"].as_u64()
                    == Some((count / 2 + 1) as u64),
            "accepted policy did not expand to majority policy: {}",
            status["status"]["effectivePolicy"]
        );
        ensure!(
            topology_primary_id(status) == self.primary["replicaId"].as_i64(),
            "healthy scale-up changed the exact primary"
        );
        let service: Value = serde_json::from_str(&self.cluster.kubectl(&[
            "-n",
            "default",
            "get",
            "service",
            "kvstore2-write",
            "-o",
            "json",
        ])?)?;
        validate_routed_service(&service, &self.primary)?;
        let configuration = &topology["configurationId"];
        let committed = self.writer.report()?["committedLsn"]
            .as_i64()
            .context("committed LSN")?;
        for member in topology["members"].as_array().context("final members")? {
            let report = self.cluster.report(member)?;
            ensure!(
                identity(&report) == identity(member)
                    && report["currentConfiguration"] == *configuration
                    && report["previousConfiguration"].is_null()
                    && report["pendingOperation"].is_null()
                    && report["currentProgress"]
                        .as_i64()
                        .is_some_and(|progress| progress >= committed),
                "member lacked exact stable acknowledged prefix: {report}"
            );
            ensure!(
                report["role"]
                    == if member["role"] == "primary" {
                        "Primary"
                    } else {
                        "ActiveSecondary"
                    },
                "application role did not match accepted topology: {report}"
            );
            let mut exact =
                DirectClient::connect(self.cluster, &self.cluster.pod(member)?, self.deadline)?;
            for acknowledged in &self.acknowledged {
                let (code, value) =
                    exact.request("GET", &format!("/kv/{}", acknowledged.key), "")?;
                ensure!(
                    code == 200 && value == acknowledged.value,
                    "exact member {} lost acknowledged value {} at LSN {}: HTTP {code} {value}",
                    member["replicaId"],
                    acknowledged.key,
                    acknowledged.lsn
                );
            }
        }
        for acknowledged in &self.acknowledged {
            assert_routed_service_read(
                self.cluster,
                &self.primary,
                &acknowledged.key,
                &acknowledged.value,
                self.deadline,
            )?;
        }
        for evidence in self.phase_writes.values() {
            ensure!(
                evidence.write_lsn > evidence.boundary_lsn && evidence.reached_candidate,
                "{} write for target {} lacked exact post-boundary candidate delivery: {evidence:?}",
                evidence.phase,
                evidence.target
            );
        }
        ensure!(
            status["status"]["lastScaleUp"].is_object(),
            "stable scale-up omitted its bounded completion receipt"
        );
        assert_scale_up_condition_context(status)?;
        eprintln!(
            "scale-up stable count={count}, commits={:?}, writes={}, phases={:?}, elapsed={:?}",
            self.commits,
            self.acknowledged.len(),
            self.phase_reasons,
            self.started.elapsed()
        );
        Ok(())
    }
}

#[test]
#[ignore = "requires an explicitly owned isolated KinD cluster"]
fn scale_up() -> Result<()> {
    run_switchover(|cluster| {
        reset_scale_set_with_delay(cluster, 1, 3)?;
        let mut case = ScaleUpCase::new(cluster, 1)?;
        let pause = ControllerPause::new(cluster)?;
        case.submit(2)?;
        let two = case.wait_for_count_manually(2)?;
        case.assert_resource_order(2)?;
        case.verify_final(&two, 2)?;
        case.submit(3)?;
        let three = case.wait_for_count_manually(3)?;
        case.assert_resource_order(3)?;
        case.verify_final(&three, 3)?;
        ensure!(
            case.commits
                .iter()
                .map(|commit| (commit.count, commit.target))
                .collect::<Vec<_>>()
                == vec![(2, 2), (3, 3)],
            "healthy scale-up skipped or reordered additions: {:?}",
            case.commits
        );
        for target in [2, 3] {
            ensure!(
                case.phase_writes.contains_key(&(target, "copy"))
                    && case.phase_writes.contains_key(&(target, "admission")),
                "target {target} lacked acknowledged writes during copy and admission: {:?}",
                case.phase_writes
            );
        }
        drop(pause);
        wait_controller_ready(cluster, case.deadline)?;
        Ok(())
    })
}

#[test]
#[ignore = "requires an explicitly owned isolated KinD cluster"]
fn scale_up_multi() -> Result<()> {
    run_switchover(|cluster| {
        reset_scale_set_with_delay(cluster, 1, 3)?;
        let mut case = ScaleUpCase::new(cluster, 1)?;
        let pause = ControllerPause::new(cluster)?;
        case.submit(3)?;
        let stable = case.wait_for_count_manually(3)?;
        case.assert_resource_order(2)?;
        case.assert_resource_order(3)?;
        case.verify_final(&stable, 3)?;
        ensure!(
            case.commits
                .iter()
                .map(|commit| (commit.count, commit.target))
                .collect::<Vec<_>>()
                == vec![(2, 2), (3, 3)],
            "1->3 did not commit one exact candidate at a time: {:?}",
            case.commits
        );
        let first_commit = case.commits[0].resource_version;
        let second_pvc = case.pvc_creations.get(&3).context("replica 3 PVC")?;
        let second_pod = case.pod_creations.get(&3).context("replica 3 Pod")?;
        ensure!(
            second_pvc.resource_version > first_commit
                && second_pod.resource_version > first_commit,
            "replica 3 resources appeared before replica 2 membership committed: \
             first_commit={first_commit}, pvc={second_pvc:?}, pod={second_pod:?}"
        );
        drop(pause);
        wait_controller_ready(cluster, case.deadline)?;
        Ok(())
    })
}

#[test]
fn scale_up_selectors_are_exact_ignored_and_wired_to_ci() {
    let recipes = include_str!("../../justfile");
    let nextest = include_str!("../../.config/nextest.toml");
    let workflow = include_str!("../../.github/workflows/CI.yml");
    for (selector, test) in [
        ("scale-up", "scale_up"),
        ("scale-up-multi", "scale_up_multi"),
        ("scale-up-adversarial", "scale_up_adversarial"),
    ] {
        assert!(recipes.contains(&format!(
            "{selector}) test_name=\"level_triggered_k8s::{test}\""
        )));
        assert!(
            recipes
                .lines()
                .any(|line| line.contains("expanded+=(") && line.contains(selector))
        );
    }
    assert!(recipes.contains("expanded+=(scale-up scale-up-multi scale-up-adversarial)"));
    assert!(recipes.contains("cargo nextest run -p kuberic-level-tests"));
    assert!(recipes.contains("--profile kind --run-ignored only"));
    assert!(recipes.contains("group(=kind-live) and test(=${test_name})"));
    assert!(nextest.contains("[profile.kind]"));
    assert!(nextest.contains("test-group = 'kind-live'"));
    assert!(workflow.contains("just level-triggered-kind-test scale-up"));
    assert!(workflow.contains("just level-triggered-kind-test all"));
    assert!(workflow.contains("  bootstrap-kind:"));
    assert!(workflow.contains("  full-kind:"));
    assert!(
        workflow
            .contains("extract_dir=\"$(mktemp -d \"$RUNNER_TEMP/nextest-extracted.XXXXXXXXXX\")\"")
    );
    assert!(!workflow.contains("--extract-to target/nextest/extracted"));
    let bootstrap_job = workflow
        .split("  bootstrap-kind:")
        .nth(1)
        .unwrap()
        .split("  full-kind:")
        .next()
        .unwrap();
    for required in [
        "KIND_CLUSTER_NAME: kuberic-level-${{ github.run_id }}-${{ github.run_attempt }}",
        "KUBECONFIG: ${{ github.workspace }}/target/kind/level-${{ github.run_id }}-${{ github.run_attempt }}.kubeconfig",
        "KUBE_CONTEXT: kind-kuberic-level-${{ github.run_id }}-${{ github.run_attempt }}",
        "KUBERIC_AGENT_BEARER_TOKEN: level-${{ github.run_id }}-${{ github.run_attempt }}",
    ] {
        assert!(bootstrap_job.contains(required), "{required}");
    }
    let postgres_job = workflow
        .split("  postgres-tests:")
        .nth(1)
        .unwrap()
        .split("  dex-live:")
        .next()
        .unwrap();
    assert!(
        postgres_job
            .find("Prepare repository-local scratch")
            .unwrap()
            < postgres_job.find("Install pinned cargo-nextest").unwrap()
    );
}

#[test]
fn scale_up_status_watermark_ignores_delayed_watch_but_rejects_newer_rollback() -> Result<()> {
    fn topology(count: i64) -> Value {
        json!({
            "configurationId":format!("configuration-{count}"),
            "epoch":{"dataLossNumber":1,"configurationNumber":count},
            "primaryId":1,
            "writeQuorum":count / 2 + 1,
            "members":(1..=count).map(|replica_id| json!({
                "replicaId":replica_id,
                "instanceId":format!("pod-{replica_id}"),
                "agentGeneration":format!("generation-{replica_id}"),
                "role":if replica_id == 1 {"primary"} else {"activeSecondary"}
            })).collect::<Vec<_>>()
        })
    }
    fn observation(resource_version: u64, generation: u64, topology: Value) -> Value {
        json!({
            "metadata":{
                "resourceVersion":resource_version.to_string(),
                "generation":generation
            },
            "status":{"topology":topology}
        })
    }

    let one = topology(1);
    let two = topology(2);
    let mut watermark = StatusObservationWatermark::default();
    let initial = observation(10, 1, one.clone());
    assert!(watermark.accept(&initial)?);
    let newer_get = observation(20, 2, two.clone());
    assert!(watermark.accept(&newer_get)?);
    let accepted = two.clone();

    let delayed_watch = observation(15, 1, one.clone());
    assert!(!watermark.accept(&delayed_watch)?);
    assert_eq!(accepted, two, "stale watch rolled accepted topology back");

    let truly_newer_rollback = observation(21, 2, one);
    assert!(watermark.accept(&truly_newer_rollback)?);
    assert!(
        check_expansion(&accepted, &truly_newer_rollback["status"]["topology"]).is_err(),
        "truly newer topology rollback was accepted"
    );
    assert_ne!(truly_newer_rollback["status"]["topology"], two);

    let regressed_generation = observation(22, 1, topology(3));
    assert!(watermark.accept(&regressed_generation).is_err());
    Ok(())
}

#[test]
fn scale_up_cleanup_order_rejects_every_wrong_permutation_and_identity() -> Result<()> {
    fn resource(kind: &'static str, name: &str, uid: &str) -> ScaleResource {
        ScaleResource {
            kind,
            object: json!({"metadata":{"name":name,"uid":uid}}),
        }
    }
    fn deletion(resource: &ScaleResource, resource_version: u64) -> CleanupDeletionObservation {
        CleanupDeletionObservation {
            kind: resource.kind,
            name: resource.name().to_string(),
            uid: resource.uid().to_string(),
            resource_version,
            observed_at: std::time::UNIX_EPOCH + Duration::from_secs(resource_version),
        }
    }
    let expected = vec![
        resource("service", "candidate-peer", "service-uid"),
        resource("pod", "candidate", "pod-uid"),
        resource("pvc", "candidate-data", "pvc-uid"),
    ];
    let correct = vec![
        deletion(&expected[0], 10),
        deletion(&expected[1], 11),
        deletion(&expected[2], 12),
    ];
    validate_cleanup_observations(&expected, &correct, true)?;
    for order in [[0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]] {
        let observed = order
            .iter()
            .enumerate()
            .map(|(index, source)| deletion(&expected[*source], 20 + index as u64))
            .collect::<Vec<_>>();
        assert!(
            validate_cleanup_observations(&expected, &observed, true).is_err(),
            "wrong cleanup order passed: {order:?}"
        );
    }
    for (field, value) in [("name", "other-name"), ("uid", "other-uid")] {
        let mut invalid = correct.clone();
        match field {
            "name" => invalid[1].name = value.into(),
            "uid" => invalid[1].uid = value.into(),
            _ => unreachable!(),
        }
        assert!(
            validate_cleanup_observations(&expected, &invalid, true).is_err(),
            "wrong cleanup {field} passed"
        );
    }
    let mut regressed_rv = correct.clone();
    regressed_rv[2].resource_version = regressed_rv[1].resource_version;
    assert!(validate_cleanup_observations(&expected, &regressed_rv, true).is_err());
    Ok(())
}

#[test]
fn scale_up_candidate_delivery_requires_exact_incarnation_session_and_build() {
    let evidence = PhaseWriteEvidence {
        target: 4,
        phase: "copy",
        boundary_lsn: 10,
        write_lsn: 12,
        candidate: json!({
            "replicaId":4,
            "instanceId":"candidate-pod-uid",
            "agentGeneration":"candidate-generation"
        }),
        candidate_process_session: "candidate-session".into(),
        build_id: "scale-up-build".into(),
        reached_candidate: false,
    };
    let report = json!({
        "replicaId":4,
        "instanceId":"candidate-pod-uid",
        "agentGeneration":"candidate-generation",
        "processSession":"candidate-session",
        "currentProgress":12,
        "builds":[{
            "buildId":"scale-up-build",
            "targetInstance":"candidate-pod-uid",
            "durableLsn":12
        }]
    });
    assert!(candidate_delivery_matches(&report, &evidence));
    for (pointer, replacement) in [
        ("/instanceId", json!("wrong-pod-uid")),
        ("/agentGeneration", json!("wrong-generation")),
        ("/processSession", json!("wrong-session")),
        ("/builds/0/buildId", json!("wrong-build")),
        ("/builds/0/targetInstance", json!("other-target")),
    ] {
        let mut wrong = report.clone();
        *wrong.pointer_mut(pointer).unwrap() = replacement;
        assert!(
            !candidate_delivery_matches(&wrong, &evidence),
            "wrong candidate delivery evidence passed at {pointer}: {wrong}"
        );
    }
}

#[test]
fn scale_up_active_copy_oracle_requires_exact_unadmitted_inflight_build() {
    use kuberic_protocol::types::{
        AgentGeneration, ConfigurationDescriptor, ConfigurationMember, EffectivePolicy, Epoch,
        OperationId, PodUid, ProvisioningIntent, ProvisioningPurpose, PvcUid, ReplicaId,
        ReplicaIdentity, ReplicaInstanceId, ReplicaRole, ResourceUid, ScaleUpProvisioning,
    };

    let resource_uid = ResourceUid::new("set-uid");
    let primary = ReplicaIdentity {
        replica_id: ReplicaId::new(1),
        instance_id: ReplicaInstanceId::new("primary-pod"),
        agent_generation: AgentGeneration::new("primary-generation"),
    };
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        primary.replica_id,
        vec![ConfigurationMember {
            identity: primary.clone(),
            role: ReplicaRole::Primary,
        }],
        1,
    );
    let previous_policy = EffectivePolicy::fixed(1, 3).unwrap();
    let current_policy = EffectivePolicy::fixed(2, 3).unwrap();
    let mut provisioning = ProvisioningIntent {
        purpose: ProvisioningPurpose::scale_up(ScaleUpProvisioning {
            resource_uid: resource_uid.clone(),
            spec_generation: 2,
            desired_replicas: 2,
            previous_configuration: previous.clone(),
            previous_policy,
            current_policy,
            target_replica_id: ReplicaId::new(2),
        }),
        pod_uid: PodUid::new("candidate-pod"),
        pvc_uid: PvcUid::new("candidate-pvc"),
        operation_id: OperationId::default(),
    };
    provisioning.operation_id = provisioning.expected_operation_id();
    let target = provisioning.target_identity(&resource_uid);
    let build_id = provisioning.scale_up_build_id(&resource_uid).unwrap();
    let status = json!({
        "metadata":{"uid":resource_uid},
        "status":{
            "topology":serde_json::to_value(&previous).unwrap(),
            "provisioning":serde_json::to_value(&provisioning).unwrap(),
            "transition":null,
            "scaleUpAdmissionStarted":null
        }
    });
    let build = json!({
        "buildId":build_id,
        "targetInstance":target.instance_id,
        "replicationBoundaryLsn":7,
        "durableLsn":7,
        "completed":false,
        "catchUpBoundaryLsn":null
    });
    let source = json!({
        "replicaId":primary.replica_id,
        "instanceId":primary.instance_id,
        "agentGeneration":primary.agent_generation,
        "processSession":"source-session",
        "role":"Primary",
        "previousConfiguration":null,
        "currentConfiguration":previous.configuration_id,
        "scaleUpOperation":null,
        "readStatus":"Granted",
        "writeStatus":"Granted",
        "builds":[build.clone()]
    });
    let candidate = json!({
        "replicaId":target.replica_id,
        "instanceId":target.instance_id,
        "agentGeneration":target.agent_generation,
        "processSession":"candidate-session",
        "role":"IdleSecondary",
        "previousConfiguration":null,
        "currentConfiguration":null,
        "scaleUpOperation":null,
        "readStatus":"NotPrimary",
        "writeStatus":"NotPrimary",
        "builds":[build]
    });
    assert!(
        active_scale_up_copy_checkpoint(
            &status,
            &source,
            &candidate,
            "source-session",
            "candidate-session"
        )
        .is_ok()
    );

    let mut invalid = Vec::new();
    let mut empty_builds = source.clone();
    empty_builds["builds"] = json!([]);
    let mut empty_candidate_builds = candidate.clone();
    empty_candidate_builds["builds"] = json!([]);
    invalid.push((
        "empty builds",
        status.clone(),
        empty_builds,
        empty_candidate_builds,
    ));
    let mut wrong_build = source.clone();
    wrong_build["builds"][0]["buildId"] = json!("wrong-build");
    let mut wrong_candidate_build = candidate.clone();
    wrong_candidate_build["builds"][0]["buildId"] = json!("wrong-build");
    invalid.push((
        "wrong build",
        status.clone(),
        wrong_build,
        wrong_candidate_build,
    ));
    let mut wrong_identity = candidate.clone();
    wrong_identity["instanceId"] = json!("wrong-candidate");
    invalid.push((
        "wrong identity",
        status.clone(),
        source.clone(),
        wrong_identity,
    ));
    let mut wrong_session = candidate.clone();
    wrong_session["processSession"] = json!("wrong-session");
    invalid.push((
        "wrong session",
        status.clone(),
        source.clone(),
        wrong_session,
    ));
    let mut pc_cc = status.clone();
    pc_cc["status"]["transition"] = json!({"kind":"scaleUp"});
    let mut admitted_candidate = candidate.clone();
    admitted_candidate["role"] = json!("ActiveSecondary");
    admitted_candidate["previousConfiguration"] = json!(previous.configuration_id.as_str());
    admitted_candidate["currentConfiguration"] = json!("expanded-configuration");
    invalid.push(("PC/CC installed", pc_cc, source.clone(), admitted_candidate));
    let mut post_admission = status.clone();
    post_admission["status"]["scaleUpAdmissionStarted"] = json!(provisioning.operation_id.as_str());
    invalid.push((
        "post admission",
        post_admission,
        source.clone(),
        candidate.clone(),
    ));
    let mut completed_source = source.clone();
    completed_source["builds"][0]["completed"] = json!(true);
    let mut completed_candidate = candidate.clone();
    completed_candidate["builds"][0]["completed"] = json!(true);
    invalid.push(("post copy", status, completed_source, completed_candidate));

    for (case, status, source, candidate) in invalid {
        assert!(
            active_scale_up_copy_checkpoint(
                &status,
                &source,
                &candidate,
                "source-session",
                "candidate-session"
            )
            .is_err(),
            "{case} evidence passed the active-copy oracle"
        );
    }
}

#[test]
#[cfg(unix)]
fn scale_up_retry_classifiers_propagate_unknown_failures() {
    use kuberic_controller::ControllerError;

    for transient in [
        anyhow::Error::new(ControllerError::ObservationStale),
        anyhow::Error::new(ControllerError::TransientObservation(
            "HTTP 429 Too Many Requests".into(),
        )),
        anyhow::Error::new(ControllerError::TransientObservation(
            "HTTP 503 Service Unavailable".into(),
        )),
        anyhow::Error::new(ControllerError::AgentUnavailable("restart".into())),
        anyhow::Error::new(io::Error::new(io::ErrorKind::ConnectionReset, "reset")),
    ] {
        assert!(transient_live_step_error(&transient), "{transient:#}");
    }
    for permanent in [
        anyhow::Error::new(ControllerError::InvalidAgentEvidence("bad protocol".into())),
        anyhow::Error::new(ControllerError::Effect("rejected".into())),
        anyhow::Error::new(ControllerError::Observation(
            "ApiError: Forbidden (ErrorResponse { code: 403 })".into(),
        )),
        anyhow::Error::new(ControllerError::Observation("KubericSet has no UID".into())),
        anyhow::Error::new(ControllerError::Observation(
            "invalid Kubernetes object".into(),
        )),
        anyhow::anyhow!("unknown live-step failure"),
    ] {
        assert!(!transient_live_step_error(&permanent), "{permanent:#}");
    }
    for (code, transient) in [
        (403, false),
        (404, false),
        (422, false),
        (429, true),
        (500, true),
        (503, true),
    ] {
        let error = anyhow::Error::new(kube::Error::Api(Box::new(kube::core::Status {
            code,
            message: format!("HTTP {code}"),
            ..Default::default()
        })));
        assert_eq!(
            transient_live_step_error(&error),
            transient,
            "HTTP {code} observation classification"
        );
    }

    assert!(
        require_routed_lookup(routed_test_output(0, "{}", ""), "Service")
            .unwrap()
            .is_some()
    );
    assert!(
        require_routed_lookup(
            routed_test_output(1, "", "Unable to connect to the server"),
            "Service"
        )
        .unwrap()
        .is_none()
    );
    assert!(
        require_routed_lookup(
            routed_test_output(1, "", "Error from server (Forbidden): denied"),
            "Service"
        )
        .is_err()
    );
}

fn scale_up_stage(plan: &kuberic_protocol::plan::Plan) -> Option<String> {
    use kuberic_protocol::{
        command::{KubernetesChange, ProtocolCommand},
        plan::Plan,
    };
    match plan {
        Plan::Execute { command } => match command {
            ProtocolCommand::InitializeAgentStore(command)
                if command
                    .provisioning
                    .as_ref()
                    .is_some_and(|provisioning| provisioning.scale_up().is_some()) =>
            {
                Some("initialize".into())
            }
            ProtocolCommand::EnsureReplicaBuild(_) => Some("build".into()),
            ProtocolCommand::EnsureConfiguration(command)
                if command.scale_up_evidence.is_some() =>
            {
                Some(format!(
                    "{}-{}",
                    if command.current_only {
                        "current-only"
                    } else {
                        "pc-cc"
                    },
                    command.local_replica_id
                ))
            }
            _ => None,
        },
        Plan::Apply { changes } => changes.iter().find_map(|change| match change {
            KubernetesChange::EnsureReplicaScaffolding { replica_ids } => Some(format!(
                "scaffolding-{}",
                replica_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            )),
            KubernetesChange::DeleteScaleDownResource { resource, .. } => {
                Some(format!("delete-{resource:?}"))
            }
            KubernetesChange::PersistStatus { status } => status
                .conditions
                .iter()
                .find(|condition| {
                    condition
                        .reason
                        .strip_prefix("ScaleUp")
                        .is_some_and(|suffix| !suffix.is_empty())
                })
                .map(|condition| format!("status-{}", condition.reason)),
            _ => None,
        }),
        _ => None,
    }
}

fn transient_live_step_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        if let Some(controller) = cause.downcast_ref::<kuberic_controller::ControllerError>() {
            return matches!(
                controller,
                kuberic_controller::ControllerError::TransientObservation(_)
                    | kuberic_controller::ControllerError::ObservationStale
                    | kuberic_controller::ControllerError::AgentUnavailable(_)
            );
        }
        if let Some(kube) = cause.downcast_ref::<kube::Error>() {
            return match kube {
                kube::Error::Api(status) => {
                    matches!(status.code, 409 | 410 | 429 | 500 | 502 | 503 | 504)
                }
                kube::Error::HyperError(_)
                | kube::Error::Service(_)
                | kube::Error::ReadEvents(_) => true,
                _ => false,
            };
        }
        cause.downcast_ref::<std::io::Error>().is_some_and(|io| {
            matches!(
                io.kind(),
                io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::ConnectionRefused
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::Interrupted
                    | io::ErrorKind::TimedOut
                    | io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::WouldBlock
            )
        })
    })
}

fn manual_live_step(cluster: &SwitchoverCluster, deadline: Instant) -> Result<LiveStep> {
    loop {
        let error = match LiveStepper::new(cluster).and_then(|stepper| stepper.step(cluster)) {
            Ok(step) => return Ok(step),
            Err(error) => error,
        };
        if !transient_live_step_error(&error) {
            return Err(error.context("manual live controller step failed permanently"));
        }
        ensure!(
            Instant::now() < deadline,
            "manual live controller could not take a step; last error: {:#}",
            error
        );
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn wait_controller_ready(cluster: &SwitchoverCluster, deadline: Instant) -> Result<()> {
    loop {
        let deployment: Value = serde_json::from_str(&cluster.kubectl(&[
            "-n",
            "kuberic-system",
            "get",
            "deployment",
            "kuberic-controller",
            "-o",
            "json",
        ])?)?;
        if deployment["status"]["observedGeneration"] == deployment["metadata"]["generation"]
            && deployment["status"]["updatedReplicas"] == 1
            && deployment["status"]["availableReplicas"] == 1
            && deployment["status"]["replicas"] == 1
        {
            return Ok(());
        }
        poll(deadline, "controller deployment availability")?;
    }
}

fn patch_replicas(cluster: &SwitchoverCluster, count: usize) -> Result<()> {
    cluster.kubectl(&[
        "-n",
        "default",
        "patch",
        "kubericset",
        "kvstore2",
        "--type=merge",
        "-p",
        &json!({"spec":{"replicas":count}}).to_string(),
    ])?;
    Ok(())
}

fn wait_process_session_change(
    cluster: &SwitchoverCluster,
    replica_id: i64,
    previous: &Value,
    deadline: Instant,
) -> Result<Value> {
    loop {
        if let Ok(report) = replica_diagnostics(&cluster.kubeconfig, &cluster.context, replica_id)
            && report["processSession"] != *previous
        {
            return Ok(report);
        }
        poll(
            deadline,
            &format!("replica {replica_id} process-session restart"),
        )?;
    }
}

fn write_routed(
    cluster: &SwitchoverCluster,
    primary: &Value,
    acknowledged: &mut Vec<(String, String)>,
    label: &str,
    deadline: Instant,
) -> Result<()> {
    let key = format!("scale-up-adversarial-{label}-{}", acknowledged.len());
    let value = format!("acknowledged-{key}");
    assert_routed_service_write(cluster, primary, &key, &value, deadline)?;
    acknowledged.push((key, value));
    Ok(())
}

fn write_routed_after_boundary(
    cluster: &SwitchoverCluster,
    primary: &Value,
    primary_client: &mut DirectClient,
    acknowledged: &mut Vec<(String, String)>,
    label: &str,
    boundary_lsn: i64,
    deadline: Instant,
) -> Result<(String, String, i64)> {
    let key = format!("scale-up-adversarial-{label}-{}", acknowledged.len());
    let value = format!("acknowledged-{key}");
    let before = primary_client.report()?["committedLsn"]
        .as_i64()
        .context("committed LSN before routed active-copy write")?;
    assert_routed_service_write(cluster, primary, &key, &value, deadline)?;
    let write_lsn = loop {
        let committed = primary_client.report()?["committedLsn"]
            .as_i64()
            .context("committed LSN after routed active-copy write")?;
        if committed > before {
            break committed;
        }
        poll(deadline, "routed active-copy write committed LSN")?;
    };
    ensure!(
        write_lsn > boundary_lsn,
        "active-copy acknowledged write LSN {write_lsn} did not exceed snapshot boundary {boundary_lsn}"
    );
    acknowledged.push((key.clone(), value.clone()));
    Ok((key, value, write_lsn))
}

fn verify_acknowledged(
    cluster: &SwitchoverCluster,
    status: &Value,
    acknowledged: &[(String, String)],
    deadline: Instant,
) -> Result<()> {
    let primary = status["status"]["topology"]["members"]
        .as_array()
        .context("accepted members")?
        .iter()
        .find(|member| member["role"] == "primary")
        .context("accepted primary")?;
    let service: Value = serde_json::from_str(&cluster.kubectl(&[
        "-n",
        "default",
        "get",
        "service",
        "kvstore2-write",
        "-o",
        "json",
    ])?)?;
    validate_routed_service(&service, primary)?;
    for (key, expected) in acknowledged {
        assert_routed_service_read(cluster, primary, key, expected, deadline)?;
    }
    let configuration = &status["status"]["topology"]["configurationId"];
    for member in status["status"]["topology"]["members"]
        .as_array()
        .context("accepted members")?
    {
        let report = cluster.report(member)?;
        ensure!(
            identity(&report) == identity(member)
                && report["currentConfiguration"] == *configuration
                && report["previousConfiguration"].is_null()
                && report["pendingOperation"].is_null()
                && report["role"]
                    == if member["role"] == "primary" {
                        "Primary"
                    } else {
                        "ActiveSecondary"
                    },
            "member lacked exact stable authority/role: {report}"
        );
        let mut client = DirectClient::connect(cluster, &cluster.pod(member)?, deadline)?;
        for (key, expected) in acknowledged {
            let (code, value) = client.request("GET", &format!("/kv/{key}"), "")?;
            ensure!(
                code == 200 && value == *expected,
                "exact member {} lost acknowledged value {key}: HTTP {code} {value}",
                member["replicaId"]
            );
        }
    }
    Ok(())
}

fn create_same_name_endpoint(
    cluster: &SwitchoverCluster,
    original: &ScaleResource,
) -> Result<Value> {
    let replacement = json!({
        "apiVersion":"v1",
        "kind":"Service",
        "metadata":{
            "name":original.name(),
            "namespace":"default",
            "ownerReferences":original.object["metadata"]["ownerReferences"],
            "labels":{"kuberic-test":"same-name-resource-fence"}
        },
        "spec":{
            "ports":[{"name":"unrelated","port":12345,"targetPort":12345}],
            "selector":{"kuberic-test":"same-name-resource-fence"}
        }
    });
    let mut child = OwnedChild(
        cluster
            .command()
            .args(["create", "-f", "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()?,
    );
    child
        .0
        .stdin
        .take()
        .context("replacement Service stdin")?
        .write_all(replacement.to_string().as_bytes())?;
    ensure!(
        child.0.wait()?.success(),
        "same-name endpoint creation failed"
    );
    Ok(serde_json::from_str(&cluster.kubectl(&[
        "-n",
        "default",
        "get",
        "service",
        original.name(),
        "-o",
        "json",
    ])?)?)
}

fn scale_up_cancellation_failure_and_retry(
    cluster: &SwitchoverCluster,
) -> Result<Vec<(String, String)>> {
    reset_scale_set_with_delay(cluster, 1, 3)?;
    let start = scale_ready(cluster, 1, Instant::now() + Duration::from_secs(180))?;
    let primary = exact_member(
        &start,
        topology_primary_id(&start).context("singleton primary")?,
    )?
    .clone();
    let deadline = Instant::now() + Duration::from_secs(900);
    let mut acknowledged = Vec::new();
    write_routed(
        cluster,
        &primary,
        &mut acknowledged,
        "before-cancellation",
        deadline,
    )?;

    let pause = ControllerPause::new(cluster)?;
    patch_replicas(cluster, 2)?;
    let mut stages = Vec::new();
    loop {
        let step = manual_live_step(cluster, deadline)?;
        if let Some(stage) = scale_up_stage(&step.plan) {
            if stages.last() != Some(&stage) {
                stages.push(stage);
            }
        }
        let status = cluster.status()?;
        assert_scale_up_condition_context(&status)?;
        if status["status"]["transition"]["scaleUp"].is_object()
            && status["status"]["scaleUpAdmissionStarted"].is_null()
        {
            break;
        }
        poll(deadline, "pre-PC scale-up cancellation boundary")?;
    }
    let old = exact_replica_resources(cluster, 2)?;
    let old_uids = old
        .iter()
        .map(|resource| (resource.kind, resource.uid().to_string()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let endpoint = old
        .iter()
        .find(|resource| resource.kind == "service")
        .context("candidate endpoint")?
        .clone();
    let service_cleanup_watch = CollectionWatch::start(cluster, "services")?;
    let pod_cleanup_watch = CollectionWatch::start(cluster, "pods")?;
    let pvc_cleanup_watch = CollectionWatch::start(cluster, "persistentvolumeclaims")?;
    let mut cleanup_observations = Vec::new();
    patch_replicas(cluster, 1)?;
    let mut replacement = None;
    loop {
        let step = manual_live_step(cluster, deadline)?;
        if let Some(stage) = scale_up_stage(&step.plan) {
            if stages.last() != Some(&stage) {
                stages.push(stage);
            }
        }
        record_cleanup_deletions(
            cluster,
            &[
                ("service", &service_cleanup_watch.events),
                ("pod", &pod_cleanup_watch.events),
                ("pvc", &pvc_cleanup_watch.events),
            ],
            &old,
            &mut cleanup_observations,
        )?;
        if cleanup_observations
            .iter()
            .any(|observation| observation.kind == "service")
            && replacement.is_none()
        {
            let object = create_same_name_endpoint(cluster, &endpoint)?;
            ensure!(
                object["metadata"]["uid"].as_str() != Some(endpoint.uid()),
                "same-name endpoint reused the frozen UID"
            );
            replacement = Some(object);
        }
        let status = cluster.status()?;
        assert_scale_up_condition_context(&status)?;
        if status["status"]["scaleUpAllocation"].is_null()
            && status["status"]["scaleUpCleanup"].is_null()
            && status["status"]["provisioning"].is_null()
            && status["status"]["transition"].is_null()
            && status["status"]["topology"]["members"]
                .as_array()
                .is_some_and(|members| members.len() == 1)
        {
            break;
        }
        poll(deadline, "exact pre-PC cancellation cleanup")?;
    }
    record_cleanup_deletions(
        cluster,
        &[
            ("service", &service_cleanup_watch.events),
            ("pod", &pod_cleanup_watch.events),
            ("pvc", &pvc_cleanup_watch.events),
        ],
        &old,
        &mut cleanup_observations,
    )?;
    validate_cleanup_observations(&old, &cleanup_observations, true)?;
    let replacement = replacement.context("same-name endpoint race was not exercised")?;
    let live: Value = serde_json::from_str(&cluster.kubectl(&[
        "-n",
        "default",
        "get",
        "service",
        endpoint.name(),
        "-o",
        "json",
    ])?)?;
    ensure!(
        live["metadata"]["uid"] == replacement["metadata"]["uid"],
        "cleanup deleted a different-UID same-name endpoint"
    );
    cluster.kubectl(&[
        "-n",
        "default",
        "delete",
        "service",
        endpoint.name(),
        "--wait=true",
        "--timeout=60s",
    ])?;
    drop(pause);
    wait_controller_ready(cluster, deadline)?;

    patch_replicas(cluster, 2)?;
    let stable = scale_ready(cluster, 2, deadline)?;
    let fresh = exact_replica_resources(cluster, 2)?;
    for resource in &fresh {
        if let Some(old_uid) = old_uids.get(resource.kind) {
            ensure!(
                resource.uid() != old_uid,
                "retry reused frozen {} UID {}",
                resource.kind,
                old_uid
            );
        }
    }
    verify_acknowledged(cluster, &stable, &acknowledged, deadline)?;

    // Delete an initialized, unadmitted candidate and require exact cleanup plus
    // a fresh automatic retry rather than partial-PVC adoption.
    let pause = ControllerPause::new(cluster)?;
    patch_replicas(cluster, 3)?;
    let failed = loop {
        manual_live_step(cluster, deadline)?;
        let status = cluster.status()?;
        assert_scale_up_condition_context(&status)?;
        if status["status"]["provisioning"]["purpose"]["scaleUp"].is_object()
            && status["status"]["transition"].is_null()
            && let Ok(resources) = exact_replica_resources(cluster, 3)
        {
            break resources;
        }
        poll(
            deadline,
            "unadmitted candidate before injected disappearance",
        )?;
    };
    let failed_uids = failed
        .iter()
        .map(|resource| (resource.kind, resource.uid().to_string()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let failed_member = json!({
        "replicaId":3,
        "instanceId":failed.iter().find(|resource| resource.kind == "pod").unwrap().uid(),
        "agentGeneration":"unadmitted"
    });
    cluster.delete_exact_pod(&failed_member)?;
    drop(pause);
    wait_controller_ready(cluster, deadline)?;
    let stable = scale_ready(cluster, 3, deadline)?;
    let retry = exact_replica_resources(cluster, 3)?;
    for resource in &retry {
        if let Some(old_uid) = failed_uids.get(resource.kind) {
            ensure!(
                resource.uid() != old_uid,
                "failed-candidate retry reused {} UID {}",
                resource.kind,
                old_uid
            );
        }
    }
    verify_acknowledged(cluster, &stable, &acknowledged, deadline)?;
    eprintln!(
        "scale-up cancellation/failure cleanup and fresh retry PASS: \
         stages={stages:?}, exact_cleanup={cleanup_observations:?}"
    );
    Ok(acknowledged)
}

fn scale_up_pre_admission_failover(cluster: &SwitchoverCluster) -> Result<Vec<(String, String)>> {
    reset_scale_set_with_delay(cluster, 3, 3)?;
    let start = scale_ready(cluster, 3, Instant::now() + Duration::from_secs(180))?;
    let old_primary_id = topology_primary_id(&start).context("starting primary")?;
    let old_primary = exact_member(&start, old_primary_id)?.clone();
    let deadline = Instant::now() + Duration::from_secs(900);
    let mut acknowledged = Vec::new();
    write_routed(
        cluster,
        &old_primary,
        &mut acknowledged,
        "before-pre-admission-failover",
        deadline,
    )?;
    let pause = ControllerPause::new(cluster)?;
    patch_replicas(cluster, 4)?;
    let (checkpoint, held_copy) = begin_exact_active_copy(cluster, &old_primary, 4, deadline)?;
    let old_candidate = exact_replica_resources(cluster, 4)?;
    let old_provisioning_id = checkpoint.operation_id.clone();
    let candidate_before = replica_diagnostics(&cluster.kubeconfig, &cluster.context, 4)?;
    let source_before = replica_diagnostics(&cluster.kubeconfig, &cluster.context, old_primary_id)?;
    ensure!(
        source_before["processSession"] == checkpoint.source_process_session
            && candidate_before["processSession"] == checkpoint.candidate_process_session,
        "active-copy checkpoint sessions changed before restart injection: \
         checkpoint={checkpoint:?} source={source_before} candidate={candidate_before}"
    );
    let mut old_source = DirectClient::connect(cluster, &cluster.pod(&old_primary)?, deadline)?;
    let (copy_key, _copy_value, copy_write_lsn) = write_routed_after_boundary(
        cluster,
        &old_primary,
        &mut old_source,
        &mut acknowledged,
        "during-held-copy",
        checkpoint.snapshot_boundary_lsn,
        deadline,
    )?;
    kill_replica_process(&cluster.kubeconfig, &cluster.context, 4)?;
    let candidate_after =
        wait_process_session_change(cluster, 4, &candidate_before["processSession"], deadline)?;
    ensure!(
        identity(&candidate_after) == identity(&candidate_before),
        "candidate process restart changed its frozen identity: before={candidate_before} after={candidate_after}"
    );
    let after_candidate_restart = loop {
        let status = cluster.status()?;
        assert_scale_up_condition_context(&status)?;
        let source = replica_diagnostics(&cluster.kubeconfig, &cluster.context, old_primary_id)?;
        if let Ok((source_session, candidate_session)) =
            active_copy_observed_sessions(cluster, &status)
            && let Ok(active) = active_scale_up_copy_checkpoint(
                &status,
                &source,
                &candidate_after,
                &source_session,
                &candidate_session,
            )
        {
            break active;
        }
        poll(
            deadline,
            "candidate restart under exact active-copy authority",
        )?;
    };
    ensure!(
        after_candidate_restart.operation_id == checkpoint.operation_id
            && after_candidate_restart.build_id == checkpoint.build_id
            && after_candidate_restart.candidate_process_session
                != checkpoint.candidate_process_session,
        "candidate restart did not reconstruct the same exact active build: \
         before={checkpoint:?} after={after_candidate_restart:?}"
    );
    kill_replica_process(&cluster.kubeconfig, &cluster.context, old_primary_id)?;
    drop(old_source);
    let after_source_restart = loop {
        let status = cluster.status()?;
        assert_scale_up_condition_context(&status)?;
        if let Ok(active) = active_copy_reconstruction_checkpoint(cluster, &status)
            && active.source_process_session != checkpoint.source_process_session
        {
            break active;
        }
        poll(deadline, "source restart under exact active-copy authority")?;
    };
    ensure!(
        after_source_restart.operation_id == checkpoint.operation_id
            && after_source_restart.build_id == checkpoint.build_id
            && after_source_restart.source_process_session != checkpoint.source_process_session
            && after_source_restart.candidate_process_session
                == after_candidate_restart.candidate_process_session,
        "source restart did not reconstruct the same exact active build: \
         before={checkpoint:?} after={after_source_restart:?}"
    );
    let copy_evidence = PhaseWriteEvidence {
        target: 4,
        phase: "copy",
        boundary_lsn: checkpoint.snapshot_boundary_lsn,
        write_lsn: copy_write_lsn,
        candidate: checkpoint.candidate.clone(),
        candidate_process_session: after_source_restart.candidate_process_session.clone(),
        build_id: checkpoint.build_id.clone(),
        reached_candidate: false,
    };
    held_copy.release_and_finish()?;
    loop {
        let status = cluster.status()?;
        assert_scale_up_condition_context(&status)?;
        ensure!(
            status["status"]["scaleUpAdmissionStarted"].is_null(),
            "copy delivery crossed the admission fence before exact candidate durability: {status}"
        );
        let candidate = replica_diagnostics(&cluster.kubeconfig, &cluster.context, 4)?;
        if candidate_delivery_matches(&candidate, &copy_evidence) {
            ensure!(
                candidate["currentProgress"]
                    .as_i64()
                    .is_some_and(|progress| progress >= copy_write_lsn),
                "candidate build report claimed delivery without durable application progress: {candidate}"
            );
            break;
        }
        manual_live_step(cluster, deadline)?;
        poll(deadline, "held-copy write durable on restarted candidate")?;
    }
    loop {
        let status = cluster.status()?;
        assert_scale_up_condition_context(&status)?;
        if status["status"]["transition"]["scaleUp"].is_object()
            && status["status"]["scaleUpAdmissionStarted"].is_null()
        {
            break;
        }
        manual_live_step(cluster, deadline)?;
        poll(deadline, "restarted copy through pre-admission boundary")?;
    }
    write_routed(
        cluster,
        &old_primary,
        &mut acknowledged,
        "after-copy-restarts",
        deadline,
    )?;
    let restarted_source_session = after_source_restart.source_process_session.clone();
    let restarted_candidate_session = after_source_restart.candidate_process_session.clone();
    eprintln!(
        "restarted exact active copy resumed: operation={}, build={}, source_session={} -> {}, \
         candidate_session={} -> {}, write={copy_key}@{copy_write_lsn}",
        checkpoint.operation_id,
        checkpoint.build_id,
        checkpoint.source_process_session,
        restarted_source_session,
        checkpoint.candidate_process_session,
        restarted_candidate_session
    );
    let old_uids = old_candidate
        .iter()
        .map(|resource| (resource.kind, resource.uid().to_string()))
        .collect::<std::collections::BTreeMap<_, _>>();
    cluster.delete_exact_pod(&old_primary)?;
    let mut failed_process_changed = false;
    let new_primary = loop {
        manual_live_step(cluster, deadline)?;
        let status = cluster.status()?;
        assert_scale_up_condition_context(&status)?;
        failed_process_changed |=
            replica_diagnostics(&cluster.kubeconfig, &cluster.context, old_primary_id).is_ok_and(
                |report| {
                    report["instanceId"] != old_primary["instanceId"]
                        || report["processSession"] != restarted_source_session
                },
            );
        ensure!(
            status["status"]["scaleUpAdmissionStarted"].is_null(),
            "pre-admission primary failure crossed the PC/CC admission fence: {status}"
        );
        if topology_primary_id(&status).is_some_and(|primary| primary != old_primary_id)
            && status["status"]["topology"]["members"]
                .as_array()
                .is_some_and(|members| members.len() == 3)
        {
            break topology_primary_id(&status).unwrap();
        }
        poll(deadline, "ordinary failover before scale-up admission")?;
    };
    ensure!(
        failed_process_changed || new_primary != old_primary_id,
        "accepted-primary fault produced neither a new process session nor a failover primary"
    );
    loop {
        let status = cluster.status()?;
        let primary = exact_member(&status, new_primary)?.clone();
        let writable = cluster.report(&primary).is_ok_and(|report| {
            identity(&report) == identity(&primary)
                && report["writeStatus"] == "Granted"
                && report["pendingOperation"].is_null()
        });
        let routed = cluster
            .kubectl(&[
                "-n",
                "default",
                "get",
                "service",
                "kvstore2-write",
                "-o",
                "json",
            ])
            .ok()
            .and_then(|service| serde_json::from_str::<Value>(&service).ok())
            .is_some_and(|service| routing_instance(&service) == primary["instanceId"].as_str());
        if writable && routed {
            write_routed(
                cluster,
                &primary,
                &mut acknowledged,
                "after-pre-admission-failover",
                deadline,
            )?;
            break;
        }
        manual_live_step(cluster, deadline)?;
        poll(
            deadline,
            "writable failover primary while desired scale-up retry remains active",
        )?;
    }
    drop(pause);
    wait_controller_ready(cluster, deadline)?;
    let (fresh_status, fresh, fresh_provisioning_id) = loop {
        let status = cluster.status()?;
        assert_scale_up_condition_context(&status)?;
        ensure!(
            status["spec"]["replicas"].as_u64() == Some(4),
            "pre-admission failover lowered desired replicas: {status}"
        );
        let old_exact_absent = old_candidate.iter().all(|resource| {
            resource.get(cluster).is_ok_and(|live| {
                live.is_none_or(|live| live["metadata"]["uid"].as_str() != Some(resource.uid()))
            })
        });
        if status["status"]["provisioning"]["purpose"]["scaleUp"].is_object()
            && status["status"]["topology"]["members"]
                .as_array()
                .is_some_and(|members| members.len() == 3)
            && topology_primary_id(&status) == Some(new_primary)
            && old_exact_absent
            && let Ok(resources) = exact_replica_resources(cluster, 4)
        {
            let operation = status["status"]["provisioning"]["operationId"]
                .as_str()
                .context("fresh provisioning operation")?
                .to_string();
            ensure!(
                operation != old_provisioning_id,
                "pre-admission failover reused old provisioning operation {operation}"
            );
            break (status, resources, operation);
        }
        poll(
            deadline,
            "exact old-candidate cleanup and fresh post-failover scale-up retry",
        )?;
    };
    for resource in &fresh {
        if let Some(old_uid) = old_uids.get(resource.kind) {
            ensure!(
                resource.uid() != old_uid,
                "pre-admission failover retry reused {} UID {}",
                resource.kind,
                old_uid
            );
        }
    }
    ensure!(
        fresh_status["status"]["scaleUpCleanup"].is_null(),
        "fresh retry overlapped unresolved old cleanup: {fresh_status}"
    );
    let retained_identities = fresh_status["status"]["topology"]["members"]
        .as_array()
        .context("post-failover retained members")?
        .iter()
        .map(identity)
        .collect::<Vec<_>>();
    let stable = scale_ready(cluster, 4, deadline)?;
    ensure!(
        topology_primary_id(&stable) == Some(new_primary),
        "fresh 3->4 admission changed the accepted failover primary"
    );
    ensure!(
        stable["status"]["effectivePolicy"]["replicaSetSize"].as_u64() == Some(4)
            && stable["status"]["effectivePolicy"]["writeQuorum"].as_u64() == Some(3),
        "fresh 3->4 admission installed the wrong policy: {}",
        stable["status"]["effectivePolicy"]
    );
    let members = stable["status"]["topology"]["members"]
        .as_array()
        .context("stable expanded members")?;
    ensure!(
        retained_identities
            .iter()
            .filter(|retained| retained["replicaId"].as_i64() != Some(old_primary_id))
            .all(|retained| members.iter().any(|member| identity(member) == *retained)),
        "fresh retry changed a surviving retained exact identity: {members:?}"
    );
    let replacement = exact_member(&stable, old_primary_id)?;
    ensure!(
        identity(replacement) != identity(&old_primary),
        "deleted accepted-primary incarnation returned without a fresh identity: {replacement}"
    );
    let fresh_pod = fresh
        .iter()
        .find(|resource| resource.kind == "pod")
        .context("fresh candidate Pod")?;
    let admitted = exact_member(&stable, 4)?;
    ensure!(
        admitted["instanceId"].as_str() == Some(fresh_pod.uid())
            && admitted["role"] == "activeSecondary",
        "fresh retry did not admit its exact candidate as ActiveSecondary: {admitted}"
    );
    verify_acknowledged(cluster, &stable, &acknowledged, deadline)?;
    eprintln!(
        "scale-up pre-admission process restarts/failover/retry PASS: desired=4, primary \
         {old_primary_id}->{new_primary}, old_operation={old_provisioning_id}, \
         fresh_operation={fresh_provisioning_id}, source_session={restarted_source_session}, \
         candidate_session={restarted_candidate_session}"
    );
    Ok(acknowledged)
}

fn scale_up_post_admission_recovery_and_replacement(
    cluster: &SwitchoverCluster,
) -> Result<Vec<(String, String)>> {
    reset_scale_set_with_delay(cluster, 2, 3)?;
    let start = scale_ready(cluster, 2, Instant::now() + Duration::from_secs(180))?;
    let old_primary_id = topology_primary_id(&start).context("starting primary")?;
    let old_primary = exact_member(&start, old_primary_id)?.clone();
    let retained_secondary = start["status"]["topology"]["members"]
        .as_array()
        .context("starting members")?
        .iter()
        .find(|member| member["replicaId"].as_i64() != Some(old_primary_id))
        .context("retained secondary")?
        .clone();
    let deadline = Instant::now() + Duration::from_secs(900);
    let mut acknowledged = Vec::new();
    write_routed(
        cluster,
        &old_primary,
        &mut acknowledged,
        "before-post-pc-failover",
        deadline,
    )?;
    let pause = ControllerPause::new(cluster)?;
    patch_replicas(cluster, 3)?;
    let (intent, expanded) = loop {
        manual_live_step(cluster, deadline)?;
        let status = cluster.status()?;
        assert_scale_up_condition_context(&status)?;
        let intent_value = &status["status"]["transition"]["scaleUp"];
        if intent_value.is_object()
            && status["status"]["scaleUpAdmissionStarted"].is_string()
            && let Ok(intent) = serde_json::from_value::<kuberic_protocol::types::ScaleUpIntent>(
                intent_value.clone(),
            )
            && let Ok(expanded) =
                serde_json::from_value::<kuberic_protocol::types::ConfigurationDescriptor>(
                    status["status"]["transition"]["currentConfiguration"].clone(),
                )
            && let Ok(snapshot) =
                LiveStepper::new(cluster).and_then(|stepper| stepper.observe_snapshot(cluster))
        {
            use kuberic_protocol::observation::AgentObservation;
            let reports = snapshot
                .replicas
                .values()
                .filter_map(|observation| match &observation.agent {
                    AgentObservation::Report(report) => Some(report.as_ref()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let exact_pc_cc = |expected: &Value| {
                reports.iter().copied().any(|report| {
                    report.identity.replica_id.value() == expected["replicaId"].as_i64().unwrap()
                        && report.identity.instance_id.as_str()
                            == expected["instanceId"].as_str().unwrap()
                        && report.previous_configuration.as_ref()
                            == Some(&intent.previous_configuration)
                        && report.current_configuration.as_ref() == Some(&expanded)
                        && report.scale_up_intent.as_deref() == Some(&intent)
                        && report.pending_operation_id.is_none()
                })
            };
            let candidate = serde_json::to_value(&intent.target)?;
            if exact_pc_cc(&old_primary)
                && exact_pc_cc(&retained_secondary)
                && exact_pc_cc(&candidate)
            {
                ensure!(
                    intent.previous_policy.replica_set_size == 2
                        && intent.current_policy.replica_set_size == 3
                        && intent.current_policy.write_quorum == 2,
                    "installed PC/CC reports carried the wrong scale-up policy: {intent:?}"
                );
                break (intent, expanded);
            }
        }
        poll(
            deadline,
            "exact expanded PC/CC authority/policy on source, retained member, and candidate",
        )?;
    };
    write_routed(
        cluster,
        &old_primary,
        &mut acknowledged,
        "during-pc-cc-admission",
        deadline,
    )?;
    let candidate_before = replica_diagnostics(
        &cluster.kubeconfig,
        &cluster.context,
        intent.target.replica_id.value(),
    )?;
    kill_replica_process(
        &cluster.kubeconfig,
        &cluster.context,
        intent.target.replica_id.value(),
    )?;
    let candidate_after = wait_process_session_change(
        cluster,
        intent.target.replica_id.value(),
        &candidate_before["processSession"],
        deadline,
    )?;
    ensure!(
        identity(&candidate_after) == identity(&candidate_before)
            && candidate_after["previousConfiguration"]
                == candidate_before["previousConfiguration"]
            && candidate_after["currentConfiguration"] == candidate_before["currentConfiguration"],
        "candidate process restart lost exact admitted PC/CC authority: before={candidate_before} after={candidate_after}"
    );
    write_routed(
        cluster,
        &old_primary,
        &mut acknowledged,
        "after-candidate-pc-cc-restart",
        deadline,
    )?;
    let suspension =
        suspend_replica_process(&cluster.kubeconfig, &cluster.context, old_primary_id)?;
    let (carried, new_primary_id) = loop {
        manual_live_step(cluster, deadline)?;
        let status = cluster.status()?;
        assert_scale_up_condition_context(&status)?;
        let evidence = &status["status"]["transition"]["scaleUpFailover"];
        if evidence.is_object() {
            ensure!(
                status["status"]["transition"]["scaleUp"].is_null()
                    && evidence["intent"]["operationId"].as_str()
                        == Some(intent.operation_id.as_str())
                    && evidence["intent"]["currentConfiguration"]["configurationId"].as_str()
                        == Some(expanded.configuration_id.as_str()),
                "failover did not carry and supersede exact outstanding scale-up authority: \
                 {}",
                status["status"]["transition"]
            );
            let new_primary = status["status"]["transition"]["currentConfiguration"]["members"]
                .as_array()
                .context("scale-up failover members")?
                .iter()
                .find(|member| member["role"] == "primary")
                .and_then(|member| member["replicaId"].as_i64())
                .context("scale-up failover primary")?;
            ensure!(
                new_primary != old_primary_id,
                "post-PC/CC failover retained the failed accepted primary"
            );
            break (status, new_primary);
        }
        poll(
            deadline,
            "carried/superseded post-PC/CC primary failover authority",
        )?;
    };
    ensure!(
        carried["status"]["topology"]["members"]
            .as_array()
            .is_some_and(|members| members.len() == 2),
        "post-PC/CC fault mutated accepted topology before failover commit"
    );
    drop(suspension);
    drop(pause);
    wait_controller_ready(cluster, deadline)?;
    let stable = scale_ready(cluster, 3, deadline)?;
    ensure!(
        topology_primary_id(&stable) == Some(new_primary_id),
        "post-PC/CC failover did not retain its exact elected primary"
    );
    let accepted_status =
        serde_json::from_value::<kuberic_protocol::types::AcceptedStatus>(stable["status"].clone())
            .context("deserialize stable carried-failover status")?;
    kuberic_protocol::validation::validate_status(&accepted_status)
        .context("validate stable carried-failover status")?;
    let receipt = accepted_status
        .last_scale_up
        .as_deref()
        .context("stable status omitted carried-failover receipt")?;
    let stable_topology = &accepted_status
        .topology
        .as_ref()
        .context("stable status omitted expanded topology")?
        .configuration;
    ensure!(
        receipt.intent.operation_id == intent.operation_id
            && &receipt.accepted_configuration == stable_topology
            && receipt
                .failover_evidence
                .as_ref()
                .is_some_and(|evidence| evidence
                    .final_election
                    .selected_primary_replica_id
                    .value()
                    == new_primary_id),
        "stable expanded membership omitted exact carried failover receipt: {}",
        stable["status"]["lastScaleUp"]
    );
    let expected_identities = start["status"]["topology"]["members"]
        .as_array()
        .context("starting identities")?
        .iter()
        .map(identity)
        .chain(std::iter::once(serde_json::to_value(&intent.target)?))
        .collect::<Vec<_>>();
    let stable_members = stable["status"]["topology"]["members"]
        .as_array()
        .context("stable expanded members")?;
    ensure!(
        stable_members.len() == 3
            && expected_identities.iter().all(|expected| {
                stable_members
                    .iter()
                    .any(|member| identity(member) == *expected)
            })
            && stable_members.iter().all(|member| {
                member["role"]
                    == if member["replicaId"].as_i64() == Some(new_primary_id) {
                        "primary"
                    } else {
                        "activeSecondary"
                    }
            }),
        "post-PC/CC recovery changed exact identities or roles: {stable_members:?}"
    );
    verify_acknowledged(cluster, &stable, &acknowledged, deadline)?;
    eprintln!(
        "scale-up post-PC/CC accepted-primary failover PASS: carried operation={}, \
         primary {old_primary_id}->{new_primary_id}, candidate_session {}->{}, \
         exact expanded membership stable",
        intent.operation_id, candidate_before["processSession"], candidate_after["processSession"]
    );

    // A separate post-completion cut retains the original member-replacement
    // coverage without conflating it with the post-PC/CC authority recovery.
    let added_before = exact_member(&stable, 3)?.clone();
    let old_resources = exact_replica_resources(cluster, 3)?;
    cluster.delete_exact_pod(&added_before)?;
    let replaced = loop {
        let status = scale_ready(cluster, 3, deadline)?;
        let member = exact_member(&status, 3)?;
        if member["instanceId"] != added_before["instanceId"] {
            break status;
        }
        poll(
            deadline,
            "post-completion replacement of the scaled-up member",
        )?;
    };
    let fresh_resources = exact_replica_resources(cluster, 3)?;
    for resource in &fresh_resources {
        if let Some(old) = old_resources.iter().find(|old| old.kind == resource.kind) {
            ensure!(
                resource.uid() != old.uid(),
                "post-completion replacement reused {} UID {}",
                resource.kind,
                old.uid()
            );
        }
    }
    verify_acknowledged(cluster, &replaced, &acknowledged, deadline)?;
    eprintln!(
        "scale-up separate post-completion member replacement PASS: primary={new_primary_id}"
    );

    let replacement_primary =
        topology_primary_id(&replaced).context("replacement topology primary")?;
    let failover_started = Instant::now();
    let failed_primary =
        suspend_replica_process(&cluster.kubeconfig, &cluster.context, replacement_primary)?;
    let failed_over = loop {
        let status = cluster.status()?;
        if status_ready(&status)
            && topology_primary_id(&status).is_some_and(|primary| primary != replacement_primary)
        {
            break status;
        }
        poll(
            deadline,
            "Ready after scale-up, accepted replacement, and ordinary primary failover",
        )?;
    };
    let failover_primary =
        topology_primary_id(&failed_over).context("post-replacement failover primary")?;
    let routed: Value = serde_json::from_str(&cluster.kubectl(&[
        "-n",
        "default",
        "get",
        "service",
        "kvstore2-write",
        "-o",
        "json",
    ])?)?;
    validate_routed_service(&routed, exact_member(&failed_over, failover_primary)?)?;
    drop(failed_primary);
    loop {
        if replica_diagnostics(&cluster.kubeconfig, &cluster.context, replacement_primary).is_ok() {
            break;
        }
        poll(
            deadline,
            "former primary process resumed after readiness proof",
        )?;
    }
    let recovered = scale_ready(cluster, 3, deadline)?;
    let recovered_primary =
        topology_primary_id(&recovered).context("recovered post-failover primary")?;
    let recovered_member = exact_member(&recovered, recovered_primary)?;
    let routed: Value = serde_json::from_str(&cluster.kubectl(&[
        "-n",
        "default",
        "get",
        "service",
        "kvstore2-write",
        "-o",
        "json",
    ])?)?;
    validate_routed_service(&routed, recovered_member)?;
    write_routed(
        cluster,
        recovered_member,
        &mut acknowledged,
        "after-scale-up-replacement-failover",
        deadline,
    )?;
    ensure!(
        recovered["status"]["lastScaleUp"]["intent"]["operationId"].as_str()
            == Some(intent.operation_id.as_str()),
        "historical scale-up receipt was erased after replacement/failover: {}",
        recovered["status"]
    );
    verify_acknowledged(cluster, &recovered, &acknowledged, deadline)?;
    eprintln!(
        "scale-up -> accepted replacement -> ordinary failover readiness PASS: primary \
         {replacement_primary}->{failover_primary}, recovered_primary={recovered_primary}, \
         ready_ms={}",
        failover_started.elapsed().as_millis()
    );
    Ok(acknowledged)
}

fn scale_down_then_restore_non_contiguous(cluster: &SwitchoverCluster) -> Result<()> {
    reset_scale_set_with_delay(cluster, 3, 3)?;
    let deadline = Instant::now() + Duration::from_secs(900);
    let stable = scale_ready(cluster, 3, deadline)?;
    let primary = stable["status"]["topology"]["members"]
        .as_array()
        .context("restoration starting members")?
        .iter()
        .find(|member| member["role"] == "primary")
        .context("restoration starting primary")?
        .clone();
    let mut acknowledged = Vec::new();
    write_routed(
        cluster,
        &primary,
        &mut acknowledged,
        "before-non-contiguous-restoration",
        deadline,
    )?;
    let target = exact_member(&stable, 3)?.clone();
    if topology_primary_id(&stable) != Some(3) {
        let request = format!(
            "scale-up-restoration-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        );
        cluster.kubectl(&[
            "-n",
            "default",
            "patch",
            "kubericset",
            "kvstore2",
            "--type=merge",
            "-p",
            &json!({"spec":{"switchover":{"requestId":request,"targetReplicaId":3}}}).to_string(),
        ])?;
        loop {
            let status = cluster.status()?;
            if status_ready(&status)
                && topology_primary_id(&status) == Some(3)
                && status["status"]["lastSwitchover"]["requestId"] == request
            {
                break;
            }
            poll(
                deadline,
                "planned primary placement for non-contiguous restoration",
            )?;
        }
        cluster.kubectl(&[
            "-n",
            "default",
            "patch",
            "kubericset",
            "kvstore2",
            "--type=merge",
            "-p",
            r#"{"spec":{"switchover":null}}"#,
        ])?;
        scale_ready(cluster, 3, deadline)?;
    }
    let old_two = exact_replica_resources(cluster, 2)?;
    patch_replicas(cluster, 2)?;
    let reduced = scale_ready(cluster, 2, deadline)?;
    let ids = reduced["status"]["topology"]["members"]
        .as_array()
        .context("reduced members")?
        .iter()
        .map(|member| member["replicaId"].as_i64().unwrap())
        .collect::<Vec<_>>();
    ensure!(
        ids == vec![1, 3],
        "scale-down did not create the planned non-contiguous accepted topology: {ids:?}"
    );
    ensure!(
        topology_primary_id(&reduced) == Some(3),
        "scale-down changed the placed primary"
    );
    patch_replicas(cluster, 3)?;
    let restored = scale_ready(cluster, 3, deadline)?;
    let restored_two = exact_member(&restored, 2)?;
    ensure!(
        restored_two["role"] == "activeSecondary",
        "restored gap was not admitted as an ActiveSecondary"
    );
    let fresh_two = exact_replica_resources(cluster, 2)?;
    for resource in &fresh_two {
        let old = old_two
            .iter()
            .find(|old| old.kind == resource.kind)
            .context("old ordinal-2 resource")?;
        ensure!(
            resource.uid() != old.uid(),
            "scale-down->scale-up restoration reused {} UID {}",
            resource.kind,
            old.uid()
        );
    }
    ensure!(
        target["replicaId"] == 3,
        "restoration setup lost the exact primary target"
    );
    verify_acknowledged(cluster, &restored, &acknowledged, deadline)?;
    eprintln!("scale-down->scale-up non-contiguous restoration PASS: [1,3] -> [1,2,3]");
    Ok(())
}

#[test]
#[ignore = "requires an explicitly owned isolated KinD cluster"]
fn scale_up_adversarial() -> Result<()> {
    run_switchover(|cluster| {
        scale_up_cancellation_failure_and_retry(cluster)?;
        scale_up_pre_admission_failover(cluster)?;
        scale_up_post_admission_recovery_and_replacement(cluster)?;
        scale_down_then_restore_non_contiguous(cluster)
    })
}

#[test]
#[ignore = "requires an explicitly owned isolated KinD cluster"]
fn planned_switchover() -> Result<()> {
    run_switchover(|cluster| {
        let mut case = SwitchoverCase::new(cluster, "healthy")?;
        case.submit()?;
        case.finish("requestedTargetCompleted", false)
    })
}

#[test]
#[ignore = "requires an explicitly owned isolated KinD cluster"]
fn planned_switchover_adversarial() -> Result<()> {
    run_switchover(|cluster| {
        let mut acknowledged;
        {
            let mut case = SwitchoverCase::new(cluster, "restarts")?;
            let partition =
                cluster.partition_replication(case.target["replicaId"].as_i64().unwrap())?;
            case.acknowledge("target-behind")?;
            case.submit()?;
            let frozen = case.wait_prepared()?;
            let session = case.target_client.report()?["processSession"].clone();
            cluster.kubectl(&[
                "-n",
                "kuberic-system",
                "rollout",
                "restart",
                "deployment/kuberic-controller",
            ])?;
            loop {
                let deployment: Value = serde_json::from_str(&cluster.kubectl(&[
                    "-n",
                    "kuberic-system",
                    "get",
                    "deployment",
                    "kuberic-controller",
                    "-o",
                    "json",
                ])?)?;
                if deployment["status"]["observedGeneration"]
                    == deployment["metadata"]["generation"]
                    && deployment["status"]["updatedReplicas"] == 1
                    && deployment["status"]["availableReplicas"] == 1
                    && deployment["status"]["replicas"] == 1
                {
                    break;
                }
                case.assert_closed()?;
                poll(case.deadline, "controller restart during frozen handoff")?;
            }
            stop_replica_process(
                &cluster.kubeconfig,
                &cluster.context,
                case.target["replicaId"].as_i64().unwrap(),
            )?;
            check_old_session_response(case.target_client.request(
                "PUT",
                "/kv/old-session",
                "forbidden",
            ))?;
            loop {
                if let Ok(report) = replica_diagnostics(
                    &cluster.kubeconfig,
                    &cluster.context,
                    case.target["replicaId"].as_i64().unwrap(),
                ) && report["processSession"] != session
                    && report["instanceId"] == case.target["instanceId"]
                    && report["agentGeneration"] == case.target["agentGeneration"]
                    && report["currentConfiguration"]
                        == case.start["status"]["topology"]["configurationId"]
                    && report["writeStatus"] != "Granted"
                {
                    break;
                }
                poll(
                    case.deadline,
                    "new target process session under unchanged durable identity",
                )?;
            }
            case.target_client =
                DirectClient::connect(cluster, &cluster.pod(&case.target)?, case.deadline)?;
            ensure!(
                case.wait_prepared()? == frozen,
                "restart changed frozen handoff evidence"
            );
            drop(partition);
            case.finish("requestedTargetCompleted", false)?;
            acknowledged = case.acknowledged;
        }
        {
            let mut case = SwitchoverCase::new(cluster, "before-admission")?;
            case.acknowledged.extend(acknowledged);
            let partition =
                cluster.partition_replication(case.target["replicaId"].as_i64().unwrap())?;
            case.acknowledge("target-behind")?;
            case.submit()?;
            case.wait_prepared()?;
            cluster.delete_exact_pod(&case.target)?;
            drop(partition);
            case.finish("oldPrimaryRestored", true)?;
            acknowledged = case.acknowledged;
        }
        {
            let mut case = SwitchoverCase::new(cluster, "after-admission")?;
            case.acknowledged.extend(acknowledged);
            // The target already has the certified prefix, but cannot obtain its new
            // replication quorum. Control/diagnostic traffic remains available.
            let partition =
                cluster.partition_replication(case.target["replicaId"].as_i64().unwrap())?;
            case.submit()?;
            case.wait_admitted()?;
            cluster.delete_exact_pod(&case.target)?;
            drop(partition);
            // Two exact survivors retain the certificate: Unsafe is not an acceptable
            // substitute for compensation when this deliberate fault leaves evidence.
            case.finish("oldPrimaryCompensated", true)?;
        }
        Ok(())
    })
}

#[test]
fn switchover_owned_cluster_contract_rejects_ambient_or_mismatched_contexts() {
    let receipt =
        "cluster=kuberic-test\ncontext=kind-kuberic-test\nkubeconfig=target/test.kubeconfig\n";
    assert!(
        validate_cluster_contract(
            "target/test.kubeconfig",
            "kind-kuberic-test",
            "kuberic-test",
            receipt
        )
        .is_ok()
    );
    for (config, context, cluster, receipt) in [
        (
            "target/test.kubeconfig",
            "kind-other",
            "kuberic-test",
            receipt,
        ),
        (
            "target/other.kubeconfig",
            "kind-kuberic-test",
            "kuberic-test",
            receipt,
        ),
        (
            "target/test.kubeconfig",
            "kind-kuberic-test",
            "kuberic-test",
            "",
        ),
        ("target/test.kubeconfig", "kind-kind", "kind", receipt),
        ("a:b", "kind-kuberic-test", "kuberic-test", receipt),
        ("", "", "", ""),
    ] {
        assert!(validate_cluster_contract(config, context, cluster, receipt).is_err());
    }
}

#[test]
fn scale_routed_write_requires_exact_http_success() {
    assert_eq!(
        classify_routed_write(Some(0), "200", "").unwrap(),
        RoutedWriteOutcome::Acknowledged
    );
    assert_eq!(
        classify_routed_write(Some(0), "503", "").unwrap(),
        RoutedWriteOutcome::Retry
    );
    for stdout in [
        "500", "201", "404", "", "000", "20", "200\n", "200200", "garbage", "\u{fffd}",
    ] {
        assert!(
            classify_routed_write(Some(0), stdout, "").is_err(),
            "{stdout:?}"
        );
        assert!(
            classify_routed_write(Some(56), stdout, "curl: (56) Connection reset by peer").is_err()
                || matches!(stdout, "" | "000"),
            "transport failure concealed {stdout:?}"
        );
    }
    assert!(classify_routed_write(Some(1), "200", "unknown failure").is_err());
    assert!(classify_routed_write(Some(1), "503", "unknown failure").is_err());
}

#[test]
fn scale_routed_service_shape_rejects_broken_selector_or_target_port() -> Result<()> {
    let primary = json!({"instanceId":"pod-uid-primary"});
    let service = json!({
        "metadata":{"name":"kvstore2-write"},
        "spec":{
            "selector":{"operator.kuberic.io/instance":"pod-uid-primary"},
            "ports":[{"name":"application","port":80,"targetPort":8080}]
        }
    });
    validate_routed_service(&service, &primary)?;
    for pointer in [
        "/spec/selector/operator.kuberic.io~1instance",
        "/spec/ports/0/targetPort",
        "/spec/ports/0/port",
    ] {
        let mut broken = service.clone();
        *broken
            .pointer_mut(pointer)
            .context("Service test pointer")? = if pointer.ends_with("instance") {
            json!("wrong-pod")
        } else {
            json!(9999)
        };
        assert!(
            validate_routed_service(&broken, &primary).is_err(),
            "broken routed Service field passed: {pointer}"
        );
    }
    Ok(())
}

#[test]
fn scale_routed_write_retries_only_known_transport_and_exec_errors() {
    for (code, message) in [
        (
            7,
            "Failed to connect to kvstore2-write port 80: Connection refused",
        ),
        (28, "Operation timed out"),
        (52, "Empty reply from server"),
        (56, "Recv failure: Connection reset by peer"),
    ] {
        let stderr =
            format!("curl: ({code}) {message}\ncommand terminated with exit code {code}\n");
        for stdout in ["000", "", "200", "503"] {
            assert_eq!(
                classify_routed_write(Some(code), stdout, &stderr).unwrap(),
                RoutedWriteOutcome::Retry
            );
        }
    }
    for stderr in [
        "error: unable to upgrade connection: container not found (\"kvstore2\")",
        "error: Internal error occurred: unable to upgrade connection: container not found (\"kvstore2\")",
        "Error from server (BadRequest): container kvstore2 is not running",
        "error: Internal error occurred: error executing command in container: failed to exec in container: container is not running",
        "error: Internal error occurred: error executing command in container: container is in CONTAINER_EXITED state",
        "Error from server: error dialing backend: dial tcp 10.0.0.1:10250: connect: connection refused",
        "Error from server: error dialing backend: read tcp 10.0.0.1:10250: connection reset by peer",
        "Error from server: error dialing backend: i/o timeout",
        "error: error upgrading connection: context deadline exceeded",
        "error: unable to upgrade connection: EOF",
        "error: lost connection to pod",
    ] {
        assert_eq!(
            classify_routed_write(Some(1), "", stderr).unwrap(),
            RoutedWriteOutcome::Retry,
            "{stderr}"
        );
        assert!(classify_routed_write(Some(1), "500", stderr).is_err());
        assert!(classify_routed_write(Some(1), "malformed", stderr).is_err());
    }
    for (code, stderr) in [
        (1, "unknown exec failure"),
        (1, "Error from server: error dialing backend: Unauthorized"),
        (1, "error: unable to upgrade connection: Forbidden"),
        (
            1,
            "error: Internal error occurred: error executing command in container: curl not found",
        ),
        (
            1,
            "Error from server (NotFound): pods \"wrong-pod\" not found",
        ),
        (7, "unknown stderr"),
        (28, "unknown timeout"),
        (6, "curl: (6) Could not resolve host: kvstore2-write"),
        (22, "curl: (22) The requested URL returned error: 500"),
        (137, "command terminated with exit code 137"),
    ] {
        assert!(
            classify_routed_write(Some(code), "", stderr).is_err(),
            "{stderr}"
        );
    }
    assert!(classify_routed_write(None, "", "").is_err());
}

#[cfg(unix)]
fn routed_test_output(code: i32, stdout: &str, stderr: &str) -> Output {
    use std::os::unix::process::ExitStatusExt;
    Output {
        status: std::process::ExitStatus::from_raw(code << 8),
        stdout: stdout.as_bytes().to_vec(),
        stderr: stderr.as_bytes().to_vec(),
    }
}

#[test]
#[cfg(unix)]
fn scale_routed_write_poll_stops_on_success_or_unknown_failure() -> Result<()> {
    let mut attempts = 0;
    wait_routed_write(Instant::now() + Duration::from_secs(5), || {
        attempts += 1;
        Ok(match attempts {
            1 => routed_test_output(7, "000", "curl: (7) Connection refused"),
            2 => routed_test_output(0, "503", ""),
            3 => routed_test_output(0, "200", ""),
            _ => panic!("must stop after acknowledgement"),
        })
    })?;
    assert_eq!(attempts, 3);
    for output in [
        routed_test_output(0, "500", ""),
        routed_test_output(0, "malformed", ""),
        routed_test_output(1, "", "unknown stderr"),
    ] {
        let mut attempts = 0;
        let error = wait_routed_write(Instant::now() + Duration::from_secs(5), || {
            attempts += 1;
            Ok(output.clone())
        })
        .unwrap_err();
        assert_eq!(attempts, 1);
        let error = format!("{error:#}");
        assert!(
            error.contains("stdout=") && error.contains("stderr="),
            "{error}"
        );
    }
    Ok(())
}

#[test]
#[cfg(unix)]
fn scale_routed_write_preserves_hard_deadline_and_last_response() {
    let deadline = Instant::now();
    assert!(wait_routed_write(deadline, || panic!("must not attempt past deadline")).is_err());

    let deadline = Instant::now() + Duration::from_millis(35);
    let mut attempts = 0;
    let error = wait_routed_write(deadline, || {
        attempts += 1;
        Ok(routed_test_output(
            28,
            "000",
            "curl: (28) Operation timed out",
        ))
    })
    .unwrap_err()
    .to_string();
    assert_eq!(attempts, 1);
    assert!(Instant::now() >= deadline);
    assert!(error.contains("deadline exceeded"), "{error}");
    assert!(
        error.contains("000") && error.contains("Operation timed out"),
        "{error}"
    );

    let deadline = Instant::now() + Duration::from_millis(35);
    let error = wait_routed_write(deadline, || {
        std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
        Ok(routed_test_output(0, "200", ""))
    })
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("deadline exceeded") && error.contains("200"),
        "{error}"
    );
}

#[test]
fn switchover_retained_io_retries_transient_connect_errors() -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut attempts = 0;
    let mut previous_remaining = Duration::from_secs(1);
    let connected = retry_retained_io(deadline, "connect", |remaining| {
        assert!(remaining <= previous_remaining);
        previous_remaining = remaining;
        attempts += 1;
        match attempts {
            1 => Err(io::ErrorKind::WouldBlock.into()),
            2 => Err(io::ErrorKind::Interrupted.into()),
            _ => Ok("connected"),
        }
    })?;
    assert_eq!(connected, "connected");
    assert_eq!(attempts, 3);
    Ok(())
}

#[test]
fn switchover_retained_io_preserves_errors_and_deadline() {
    for kind in [
        io::ErrorKind::ConnectionRefused,
        io::ErrorKind::ConnectionReset,
        io::ErrorKind::BrokenPipe,
        io::ErrorKind::TimedOut,
        io::ErrorKind::UnexpectedEof,
        io::ErrorKind::InvalidData,
    ] {
        let mut attempts = 0;
        let result: io::Result<()> =
            retry_retained_io(Instant::now() + Duration::from_secs(1), "connect", |_| {
                attempts += 1;
                Err(io::Error::new(kind, "original I/O failure"))
            });
        let error = result.unwrap_err();
        assert_eq!(error.kind(), kind);
        assert_eq!(error.to_string(), "original I/O failure");
        assert_eq!(attempts, 1);
    }

    let deadline = Instant::now() + Duration::from_millis(35);
    let result: io::Result<()> =
        retry_retained_io(deadline, "read", |_| Err(io::ErrorKind::WouldBlock.into()));
    let error = result.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(
        error.to_string(),
        "deadline exceeded waiting for retained-client read"
    );
    assert!(Instant::now() >= deadline);
    let result: io::Result<()> = retry_retained_io(deadline, "write", |_| {
        panic!("expired deadline must not attempt more I/O")
    });
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
}

#[test]
fn switchover_retained_io_retries_partial_messages_without_replay() -> Result<()> {
    struct IntermittentIo {
        response: io::Cursor<Vec<u8>>,
        written: Vec<u8>,
        block_read: bool,
        block_write: bool,
        block_flush: bool,
    }

    impl Read for IntermittentIo {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            self.block_read = !self.block_read;
            if self.block_read {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            let length = buffer.len().min(7);
            self.response.read(&mut buffer[..length])
        }
    }

    impl Write for IntermittentIo {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.block_write = !self.block_write;
            if self.block_write {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            let length = buffer.len().min(7);
            self.written.extend_from_slice(&buffer[..length]);
            Ok(length)
        }

        fn flush(&mut self) -> io::Result<()> {
            if std::mem::take(&mut self.block_flush) {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            Ok(())
        }
    }

    let mut stream = BufReader::new(RetainedIo {
        inner: IntermittentIo {
            response: io::Cursor::new(
                b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\noneHTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1;part=first\r\nt\r\n2\r\nwo\r\n0\r\nX-End: yes\r\n\r\nHTTP/1.1 503 Unavailable\r\ncontent-length: 6\r\n\r\nclosed"
                    .to_vec(),
            ),
            written: Vec::new(),
            block_read: false,
            block_write: false,
            block_flush: true,
        },
        deadline: Instant::now() + Duration::from_secs(5),
    });
    assert_eq!(
        request_http(&mut stream, "PUT", "/kv/key", "value")?,
        (200, "one".into())
    );
    assert_eq!(
        stream.get_ref().inner.written,
        b"PUT /kv/key HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\nContent-Length: 5\r\n\r\nvalue"
    );
    assert!(!stream.get_ref().inner.block_flush);
    assert_eq!(read_http_response(&mut stream)?, (200, "two".into()));
    assert_eq!(read_http_response(&mut stream)?, (503, "closed".into()));
    assert!(read_http_response(&mut stream).is_err());

    for response in [
        &b"HTTP/1.1 invalid\r\nContent-Length: 0\r\n\r\n"[..],
        &b"HTTP/1.1 200 OK\r\nContent-Length: invalid\r\n\r\n"[..],
        &b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nab"[..],
        &b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n\xff"[..],
    ] {
        let mut reader = BufReader::new(RetainedIo {
            inner: response,
            deadline: Instant::now() + Duration::from_secs(1),
        });
        assert!(read_http_response(&mut reader).is_err());
    }
    Ok(())
}

#[test]
fn switchover_retained_http_parser_preserves_response_boundaries() -> Result<()> {
    let mut responses = &b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\noneHTTP/1.1 503 Unavailable\r\ncontent-length: 6\r\n\r\nclosedHTTP/1.1 204 No Content\r\n\r\nHTTP/1.1 304 Not Modified\r\nContent-Length: 42\r\n\r\n"[..];
    assert_eq!(read_http_response(&mut responses)?, (200, "one".into()));
    assert_eq!(read_http_response(&mut responses)?, (503, "closed".into()));
    assert_eq!(read_http_response(&mut responses)?, (204, String::new()));
    assert_eq!(read_http_response(&mut responses)?, (304, String::new()));
    assert!(read_http_response(&mut responses).is_err());
    assert!(
        read_http_response(&mut &b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\nshort"[..]).is_err()
    );
    assert!(
        read_http_response(&mut &b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"[..])
            .is_err()
    );
    Ok(())
}

#[test]
fn switchover_retained_http_reuses_live_keep_alive_socket() -> Result<()> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let client = TcpStream::connect_timeout(&listener.local_addr()?, Duration::from_secs(1))?;
    let (mut peer, _) = listener.accept()?;
    peer.set_read_timeout(Some(Duration::from_secs(3)))?;
    peer.set_write_timeout(Some(Duration::from_secs(3)))?;
    let (release, finished) = channel();
    let server = std::thread::spawn(move || -> Result<()> {
        for (index, response) in [
            &b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 1\r\n\r\n1"[..],
            &b"HTTP/1.1 200 OK\r\nConnection: keep-alive\r\ntRaNsFeR-EnCoDiNg: Chunked\r\n\r\n1\r\n2\r\n0\r\nX-End: yes\r\n\r\n"[..],
            &b"HTTP/1.1 503 Service Unavailable\r\nConnection: keep-alive\r\nContent-Length: 6\r\n\r\nclosed"[..],
        ]
        .iter()
        .enumerate()
        {
            let expected = format!(
                "PUT /kv/retained-{index} HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\nContent-Length: 6\r\n\r\nvαlue"
            );
            let mut request = vec![0; expected.len()];
            peer.read_exact(&mut request)?;
            ensure!(request == expected.as_bytes(), "incorrect request framing");
            peer.write_all(response)?;
        }
        // Do not close to delimit the final response: the client must finish first.
        finished.recv_timeout(Duration::from_secs(3))?;
        Ok(())
    });
    client.set_nonblocking(true)?;
    let mut stream = BufReader::new(RetainedIo {
        inner: client,
        deadline: Instant::now() + Duration::from_secs(2),
    });
    let result = (|| -> Result<()> {
        for (index, expected) in [(200, "1"), (200, "2"), (503, "closed")].iter().enumerate() {
            assert_eq!(
                request_http(
                    &mut stream,
                    "PUT",
                    &format!("/kv/retained-{index}"),
                    "vαlue"
                )?,
                (expected.0, expected.1.to_string())
            );
        }
        Ok(())
    })();
    let _ = release.send(());
    server.join().expect("HTTP peer panicked")?;
    result
}

#[test]
fn switchover_retained_http_rejects_malformed_or_unbounded_responses() {
    for response in [
        "HTTP/1.1 200 OK\nContent-Length: 0\r\n\r\n",
        "not-http 200 OK\r\nContent-Length: 0\r\n\r\n",
        "HTTP/1.1 200\r\nContent-Length: 0\r\n\r\n",
        "HTTP/1.1 999 Invalid\r\nContent-Length: 0\r\n\r\n",
        "HTTP/1.1 200 OK\r\nContent-Length 0\r\n\r\n",
        "HTTP/1.1 200 OK\r\n Content-Length: 0\r\n\r\n",
        "HTTP/1.1 200 OK\r\nContent-Length: +1\r\n\r\nx",
        "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Length: 1\r\n\r\nx",
        "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nunframed",
        "HTTP/1.1 200 OK\r\nContent-Length: 1048577\r\n\r\n",
        "HTTP/1.1 200 OK\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n",
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, chunked\r\n\r\n",
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n-1\r\n",
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0;bad=\0\r\n\r\n",
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n100001\r\n",
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nx",
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx!!",
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n",
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nbad trailer\r\n\r\n",
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nContent-Length: 0\r\n\r\n",
        "HTTP/1.1 101 Switching Protocols\r\n\r\n",
    ] {
        assert!(
            read_http_response(&mut response.as_bytes()).is_err(),
            "accepted {response:?}"
        );
    }
    let oversized = format!("HTTP/1.1 200 OK\r\nX-Long: {}", "a".repeat(64 * 1024));
    assert!(read_http_response(&mut oversized.as_bytes()).is_err());
}

#[test]
fn switchover_retained_http_partial_response_timeout_is_not_rejection() {
    struct Stalled<'a>(&'a [u8]);
    impl Read for Stalled<'_> {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if self.0.is_empty() {
                Err(io::ErrorKind::WouldBlock.into())
            } else {
                self.0.read(buffer)
            }
        }
    }
    for response in [
        "HTTP/1.1 503 Unavailable\r\nContent-Len",
        "HTTP/1.1 503 Unavailable\r\nContent-Length: 6\r\n\r\nclo",
        "HTTP/1.1 503 Unavailable\r\nTransfer-Encoding: chunked\r\n\r\n6\r\nclo",
        "HTTP/1.1 503 Unavailable\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n",
    ] {
        let deadline = Instant::now() + Duration::from_millis(35);
        let mut reader = BufReader::new(RetainedIo {
            inner: Stalled(response.as_bytes()),
            deadline,
        });
        let error = read_http_response(&mut reader).unwrap_err();
        assert_eq!(
            error.downcast_ref::<io::Error>().unwrap().kind(),
            io::ErrorKind::TimedOut
        );
        assert!(Instant::now() >= deadline);
        assert!(check_deleted_target_response(Err(error)).is_err());
    }
}

#[test]
fn switchover_deleted_target_requires_rejection_or_disconnect() {
    assert!(check_deleted_target_response(Ok((503, "closed".into()))).is_ok());
    assert!(check_deleted_target_response(read_http_response(&mut &b""[..])).is_ok());
    for kind in [
        io::ErrorKind::BrokenPipe,
        io::ErrorKind::ConnectionReset,
        io::ErrorKind::ConnectionAborted,
    ] {
        assert!(
            check_deleted_target_response(Err(io::Error::from(kind)).context("request")).is_ok()
        );
    }

    for code in [200, 201, 400, 404, 500] {
        assert!(check_deleted_target_response(Ok((code, String::new()))).is_err());
    }
    assert!(
        check_deleted_target_response(read_http_response(
            &mut &b"HTTP/1.1 503 Unavailable\r\nContent-Length: 6\r\n\r\nclo"[..]
        ))
        .is_err()
    );
}

#[test]
fn switchover_old_session_probe_rejects_unrelated_failures() {
    assert!(check_old_session_response(Ok((503, "closed".into()))).is_ok());
    assert!(
        check_old_session_response(Err(io::Error::from(io::ErrorKind::ConnectionReset).into()))
            .is_ok()
    );
    assert!(check_old_session_response(Ok((500, "internal".into()))).is_err());
    assert!(
        check_old_session_response(read_http_response(
            &mut &b"HTTP/1.1 503 Unavailable\r\nContent-Length: 6\r\n\r\nclo"[..]
        ))
        .is_err()
    );
}

#[test]
fn switchover_prefix_barrier_allows_lagging_secondary_commit_watermark() {
    let member = json!({"replicaId":2,"instanceId":"target","agentGeneration":"generation"});
    let config = json!("current");
    let mut report = member.clone();
    report["currentConfiguration"] = config.clone();
    report["currentProgress"] = json!(3);
    report["committedLsn"] = json!(2);
    assert!(retains_acknowledged_prefix(&report, &member, &config, 3));
    for (field, value) in [
        ("currentProgress", json!(2)),
        ("currentProgress", Value::Null),
        ("replicaId", json!(3)),
        ("instanceId", json!("replacement")),
        ("agentGeneration", json!("new-generation")),
        ("currentConfiguration", json!("stale")),
        ("previousConfiguration", json!("previous")),
        ("pendingOperation", json!("applying")),
    ] {
        let mut stale = report.clone();
        stale[field] = value;
        assert!(
            !retains_acknowledged_prefix(&stale, &member, &config, 3),
            "accepted stale {field}"
        );
    }
}

#[test]
fn switchover_receipt_requires_exact_target_and_correct_recovery_epoch() -> Result<()> {
    let source = json!({"replicaId":1,"instanceId":"source","agentGeneration":"source-generation","role":"primary"});
    let target = json!({"replicaId":2,"instanceId":"target","agentGeneration":"target-generation","role":"activeSecondary"});
    let start = json!({"status":{"topology":{"epoch":{"dataLossNumber":0,"configurationNumber":5},"members":[source,target]}}});
    for (outcome, number) in [
        ("requestedTargetCompleted", 6),
        ("oldPrimaryRestored", 5),
        ("oldPrimaryCompensated", 7),
    ] {
        let mut completed = start.clone();
        completed["status"]["topology"]["epoch"]["configurationNumber"] = json!(number);
        if outcome == "requestedTargetCompleted" {
            completed["status"]["topology"]["members"][0]["role"] = json!("activeSecondary");
            completed["status"]["topology"]["members"][1]["role"] = json!("primary");
        }
        completed["status"]["lastSwitchover"] = json!({
            "requestId":"test","outcome":outcome,"acceptedTarget":identity(&target),
            "requestedTargetReplicaId":2,
            "resultingPrimary":identity(if outcome == "requestedTargetCompleted" { &target } else { &source })
        });
        check_receipt(&start, &completed, "test", &target, outcome)?;
        let mut stale = completed.clone();
        stale["status"]["lastSwitchover"]["acceptedTarget"]["instanceId"] =
            json!("new-incarnation");
        assert!(check_receipt(&start, &stale, "test", &target, outcome).is_err());
        let mut wrong_epoch = completed;
        wrong_epoch["status"]["topology"]["epoch"]["configurationNumber"] = json!(4);
        assert!(check_receipt(&start, &wrong_epoch, "test", &target, outcome).is_err());
    }
    Ok(())
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
            );
            if let Ok(primary) = primary {
                let status = kubectl(
                    &kubeconfig,
                    &context,
                    &[
                        "-n",
                        "default",
                        "exec",
                        primary.trim(),
                        "--",
                        "curl",
                        "--fail",
                        "--silent",
                        "http://127.0.0.1:8080/status",
                    ],
                );
                if let Ok(status) = status {
                    let status: serde_json::Value = serde_json::from_str(&status)?;
                    if status["writeStatus"] == "Granted" {
                        break;
                    }
                }
            }
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
                && after["readStatus"] == "Granted"
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
    let access_deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        let status = kubectl(
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
                "http://127.0.0.1:8080/status",
            ],
        );
        if let Ok(status) = status {
            let status: serde_json::Value = serde_json::from_str(&status)?;
            if status["writeStatus"] == "Granted" {
                break;
            }
        }
        if std::time::Instant::now() >= access_deadline {
            bail!("primary did not restore write access after bootstrap restart");
        }
        std::thread::sleep(Duration::from_secs(2));
    }
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
    let write_ready_deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if replica_diagnostics(&kubeconfig, &context, old_primary).is_ok_and(|diagnostics| {
            diagnostics["readStatus"] == "Granted" && diagnostics["writeStatus"] == "Granted"
        }) {
            break;
        }
        if std::time::Instant::now() >= write_ready_deadline {
            bail!("pre-failover primary did not expose granted read/write readiness");
        }
        std::thread::sleep(Duration::from_millis(500));
    }
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
