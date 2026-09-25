mod common;
#[path = "common/tds.rs"]
mod tds_peer;

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::time::timeout;

use common::Fixture;

const UNKNOWN_TOKEN: &[u8] = &[
    4, 1, 0, 21, 0, 0, 1, 0, 1, 0, 11, 0, 1, 8, 0, 12, 0, 1, 255, 2, 0,
];
const INVALID_THREAD_LENGTH: &[u8] = &[
    4, 1, 0, 21, 0, 0, 1, 0, 1, 0, 11, 0, 1, 3, 0, 12, 0, 1, 255, 3, 0,
];
const TLS_REQUIRED: &[u8] = &[4, 1, 0, 15, 0, 0, 1, 0, 1, 0, 6, 0, 1, 255, 3];
const INVALID_ENCRYPTION: &[u8] = &[4, 1, 0, 15, 0, 0, 1, 0, 1, 0, 6, 0, 1, 255, 254];
const PANIC_MESSAGE: &str = "TLS/TDS login: TDS driver panicked; connection discarded";
const PROTOCOL_MESSAGE: &str = "TLS/TDS login: invalid TDS response or unsupported column encoding";

#[derive(Debug, Clone, Copy)]
enum Fault {
    UnknownToken,
    InvalidThreadLength,
    MissingRoots,
    EmptyRoots,
    InvalidRoots,
    #[cfg(unix)]
    UnreadableRoots,
}

impl Fault {
    fn response(self) -> &'static [u8] {
        match self {
            Self::UnknownToken => UNKNOWN_TOKEN,
            Self::InvalidThreadLength => INVALID_THREAD_LENGTH,
            _ => TLS_REQUIRED,
        }
    }

    fn configure_roots(self, command: &mut Command, path: &Path) {
        match self {
            Self::UnknownToken | Self::InvalidThreadLength => return,
            Self::MissingRoots => {}
            Self::EmptyRoots => std::fs::write(path, []).unwrap(),
            Self::InvalidRoots => std::fs::write(
                path,
                b"-----BEGIN CERTIFICATE-----\n!invalid-base64!\n-----END CERTIFICATE-----\n",
            )
            .unwrap(),
            #[cfg(unix)]
            Self::UnreadableRoots => {
                use std::os::unix::fs::PermissionsExt;
                std::fs::write(path, []).unwrap();
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o000)).unwrap();
            }
        }
        command.env("SSL_CERT_FILE", path);
    }
}

fn assert_failure(bytes: &[u8], message: &str) -> u64 {
    let report: Value = serde_json::from_slice(bytes).unwrap();
    assert_eq!(report["schema_version"], 1);
    assert_eq!(report["fresh"], false);
    assert_eq!(report["observation"]["status"], "failed");
    assert_eq!(report["observation"]["kind"], "malformed");
    assert_eq!(report["observation"]["message"], message);
    assert_eq!(report["source"]["replica"]["incarnation"], "pod-uid-0");
    let observed_at = report["observation"]["observed_at_unix_millis"]
        .as_u64()
        .unwrap();
    assert!(observed_at > 0);
    let output = std::str::from_utf8(bytes).unwrap();
    assert!(!output.contains("unit-test-only"));
    assert!(!output.contains("root-path-must-not-be-echoed"));
    observed_at
}

async fn rejected_prelogin(listener: &TcpListener, response: &[u8]) {
    let mut stream = tds_peer::respond_to_prelogin(listener, response).await;
    let mut remaining = Vec::new();
    stream.read_to_end(&mut remaining).await.unwrap();
    assert!(
        remaining.is_empty(),
        "no login or TLS fallback after failure"
    );
}

async fn run_probe(fault: Fault, watch: bool) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let mut fixture = Fixture::new();
    fixture.document["host"] = json!("127.0.0.1");
    fixture.document["port"] = json!(listener.local_addr().unwrap().port());
    fixture.document["poll_interval_ms"] = json!(50);
    assert_eq!(
        fixture.config().connection().endpoint().port(),
        listener.local_addr().unwrap().port()
    );
    let roots = tempfile::tempdir().unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_sqlserver-observer"));
    command
        .arg("--config")
        .arg(fixture.config_path())
        .env_remove("SSL_CERT_FILE")
        .env("RUST_BACKTRACE", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    fault.configure_roots(
        &mut command,
        &roots.path().join("root-path-must-not-be-echoed.pem"),
    );
    if watch {
        command.arg("--watch");
    }
    let (first_report_observed, proceed) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        rejected_prelogin(&listener, fault.response()).await;
        if watch {
            // The watch channel is deliberately lossy; do not race its reader.
            proceed.await.unwrap();
            rejected_prelogin(&listener, INVALID_ENCRYPTION).await;
        }
    });
    let mut child = command.spawn().unwrap();
    let mut lines = watch.then(|| BufReader::new(child.stdout.take().unwrap()).lines());
    if let Some(lines) = &mut lines {
        let mut first_report_observed = Some(first_report_observed);
        let mut previous_timestamp = 0;
        for (index, message) in [PANIC_MESSAGE, PROTOCOL_MESSAGE].into_iter().enumerate() {
            let line = timeout(Duration::from_secs(10), lines.next_line())
                .await
                .unwrap()
                .unwrap()
                .expect("watch must publish a failure and reconnect instead of terminating");
            let observed_at = assert_failure(line.as_bytes(), message);
            assert!(observed_at > previous_timestamp);
            previous_timestamp = observed_at;
            if index == 0 {
                first_report_observed.take().unwrap().send(()).unwrap();
            }
        }
        assert!(
            Command::new("kill")
                .args(["-TERM", &child.id().unwrap().to_string()])
                .status()
                .await
                .unwrap()
                .success()
        );
    }
    let output = timeout(Duration::from_secs(10), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.status.code(), Some(1), "{fault:?}: {output:?}");
    assert!(
        output.stderr.is_empty(),
        "{fault:?}: panic payloads/backtraces must not bypass sanitized reports: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    if !watch {
        assert_failure(&output.stdout, PANIC_MESSAGE);
    }
    timeout(Duration::from_secs(10), peer)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn prelogin_panics_produce_sanitized_one_shot_failure_reports() {
    for fault in [Fault::UnknownToken, Fault::InvalidThreadLength] {
        run_probe(fault, false).await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn watch_reconnects_after_prelogin_panics() {
    for fault in [Fault::UnknownToken, Fault::InvalidThreadLength] {
        run_probe(fault, true).await;
    }
}

#[tokio::test]
async fn native_root_panics_produce_sanitized_one_shot_failure_reports() {
    for fault in [
        Fault::MissingRoots,
        Fault::EmptyRoots,
        Fault::InvalidRoots,
        #[cfg(unix)]
        Fault::UnreadableRoots,
    ] {
        run_probe(fault, false).await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn watch_reconnects_after_native_root_panics() {
    for fault in [
        Fault::MissingRoots,
        Fault::EmptyRoots,
        Fault::InvalidRoots,
        Fault::UnreadableRoots,
    ] {
        run_probe(fault, true).await;
    }
}
