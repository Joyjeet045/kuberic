use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
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
        let mut forward = OwnedChild(
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
                    "port-forward",
                    "--address=127.0.0.1",
                    pod,
                    ":8080",
                ])
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
        let (code, body) = self
            .source_client
            .request("PUT", &format!("/kv/{key}"), &value)?;
        ensure!(code == 200, "source write failed: HTTP {code} {body}");
        self.acknowledged.push((key, value));
        Ok(body.parse()?)
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
