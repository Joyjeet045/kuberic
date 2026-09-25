mod common;

use std::process::Command;

use serde_json::json;

use common::Fixture;

fn binary() -> Command {
    Command::new(env!("CARGO_BIN_EXE_sqlserver-observer"))
}

#[test]
fn help_only_advertises_observation() {
    let output = binary().arg("--help").output().unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(help.contains("--config"));
    assert!(help.contains("--watch"));
    assert!(help.contains("never modifies AG state"));
    for option in [
        "--password",
        "--promote",
        "--failover",
        "--trust-cert",
        "--mutation",
    ] {
        assert!(!help.contains(option));
    }
}

#[test]
fn mutation_and_inline_secrets_are_rejected_without_echoing_credentials() {
    let mut fixture = Fixture::new();
    fixture.document["password"] = json!("sensitive-inline-value");
    let output = binary()
        .args(["--config", fixture.config_path().to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("configuration"));
    assert!(!stderr.contains("sensitive-inline-value"));
}

#[test]
fn a_failed_attempt_has_machine_readable_output_and_nonzero_exit() {
    let fixture = Fixture::new();
    std::fs::remove_file(fixture.document["observer_username_file"].as_str().unwrap()).unwrap();
    let output = binary()
        .args(["--config", fixture.config_path().to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["schema_version"], 1);
    assert_eq!(report["observation"]["status"], "failed");
    assert_eq!(report["observation"]["kind"], "unreachable");
    assert_eq!(report["fresh"], false);
    assert!(
        report["observation"]["observed_at_unix_millis"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(report["source"]["replica"]["incarnation"], "pod-uid-0");
    assert!(
        !String::from_utf8(output.stdout)
            .unwrap()
            .contains("unit-test-only")
    );
}

#[test]
fn configuration_requires_distinct_secret_files_even_for_the_cli() {
    let mut fixture = Fixture::new();
    assert_eq!(
        fixture.config().target().availability_group.as_str(),
        "test-ag"
    );
    fixture.document["observer_password_file"] = fixture.document["observer_username_file"].clone();
    let output = binary()
        .args(["--config", fixture.config_path().to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn watch_keeps_reporting_failures_and_sigterm_shuts_it_down() {
    use std::process::Stdio;
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, BufReader};

    let mut fixture = Fixture::new();
    fixture.document["poll_interval_ms"] = json!(10);
    std::fs::remove_file(fixture.document["observer_username_file"].as_str().unwrap()).unwrap();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_sqlserver-observer"))
        .arg("--config")
        .arg(fixture.config_path())
        .arg("--watch")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    for _ in 0..2 {
        let line = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let report: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(report["observation"]["status"], "failed");
        assert_eq!(report["fresh"], false);
    }
    let pid = child.id().unwrap().to_string();
    assert!(
        Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap()
            .success()
    );
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        status.code(),
        Some(1),
        "failed samples make watch exit nonzero"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn undrained_stdout_does_not_prevent_shutdown_in_either_mode() {
    use std::io::{ErrorKind, Write};
    use std::os::fd::OwnedFd;
    use std::os::unix::net::UnixStream;
    use std::process::Stdio;
    use std::time::Duration;

    for (watch, expected_code, signal) in [
        (true, 1, "-TERM"),
        (false, 130, "-TERM"),
        (true, 1, "-INT"),
        (false, 130, "-INT"),
    ] {
        let (_undrained, mut writer) = UnixStream::pair().unwrap();
        writer.set_nonblocking(true).unwrap();
        loop {
            match writer.write(&[0; 8192]) {
                Ok(0) => panic!("stdout fixture unexpectedly closed"),
                Ok(_) => {}
                Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                Err(error) => panic!("cannot fill stdout fixture: {error}"),
            }
        }
        writer.set_nonblocking(false).unwrap();
        let stdout: OwnedFd = writer.into();
        let fixture = Fixture::new();
        std::fs::remove_file(fixture.document["observer_username_file"].as_str().unwrap()).unwrap();
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_sqlserver-observer"));
        command.arg("--config").arg(fixture.config_path());
        if watch {
            command.arg("--watch");
        }
        let mut child = command
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        let pid = child.id().unwrap().to_string();
        assert!(
            Command::new("kill")
                .args([signal, &pid])
                .status()
                .unwrap()
                .success()
        );
        let status = match tokio::time::timeout(Duration::from_secs(3), child.wait()).await {
            Ok(result) => result.unwrap(),
            Err(_) => {
                child.kill().await.unwrap();
                panic!("undrained stdout prevented graceful shutdown (watch={watch})");
            }
        };
        assert_eq!(status.code(), Some(expected_code));
    }
}

#[cfg(unix)]
#[tokio::test]
async fn broken_stdout_reports_a_write_error_and_exits_nonzero() {
    use std::os::fd::OwnedFd;
    use std::os::unix::net::UnixStream;
    use std::process::Stdio;
    use std::time::Duration;

    let (reader, writer) = UnixStream::pair().unwrap();
    drop(reader);
    let stdout: OwnedFd = writer.into();
    let fixture = Fixture::new();
    std::fs::remove_file(fixture.document["observer_username_file"].as_str().unwrap()).unwrap();
    let child = tokio::process::Command::new(env!("CARGO_BIN_EXE_sqlserver-observer"))
        .arg("--config")
        .arg(fixture.config_path())
        .arg("--watch")
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let output = tokio::time::timeout(Duration::from_secs(5), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("cannot write observation to stdout")
    );
}
