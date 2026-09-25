mod common;
#[path = "common/tds.rs"]
mod tds_peer;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use sqlserver_replicated::executor::{SqlExecutor, SqlSession};
use sqlserver_replicated::instance::SqlServerInstanceManager;
use sqlserver_replicated::monitor::{ObservationReport, SqlServerMonitor};
use sqlserver_replicated::observation::InstanceSnapshot;
use sqlserver_replicated::runtime_config::ObserverConfig;
use sqlserver_replicated::runtime_error::RuntimeError;
use sqlserver_replicated::tds::TdsExecutor;
use sqlserver_replicated::{DecimalProgress, Observation, ObservationFailureKind};
use tokio::io::AsyncReadExt;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use common::Fixture;

#[test]
fn documented_example_is_a_valid_observe_only_configuration() {
    let config = ObserverConfig::from_json(include_bytes!("../observer.example.json")).unwrap();
    assert_eq!(config.target().availability_group.as_str(), "kuberic-ag");
    assert_eq!(config.connection().endpoint().port(), 1433);
}

#[tokio::test]
async fn config_reads_mount_references_and_safe_defaults() {
    let fixture = Fixture::new();
    let config = ObserverConfig::read(&fixture.config_path()).await.unwrap();
    assert_eq!(config.connection().endpoint().port(), 1433);
    assert_eq!(config.target().replica.incarnation(), "pod-uid-0");
    assert_eq!(config.sample_timeout(), Duration::from_secs(30));
    assert_eq!(config.poll_interval(), Duration::from_secs(1));
    assert_eq!(config.max_age(), Duration::from_secs(60));
}

#[test]
fn invalid_configuration_fails_closed_without_echoing_values() {
    let fixture = Fixture::new();
    let marker = "sensitive-input-must-not-be-echoed";
    for (field, value) in [
        ("mode", json!("enabled")),
        ("trust_server_certificate", json!(true)),
        ("password", json!(marker)),
        ("mutation_credentials", json!(marker)),
        ("port", json!(0)),
        ("host", json!("host;password=secret")),
        ("incarnation", json!("")),
        ("observer_password_file", json!("relative/password")),
        ("ca_certificate_file", json!("/mounted/root.txt")),
        ("poll_interval_ms", json!(0)),
        ("max_age_ms", json!(300001)),
        ("connect_timeout_ms", json!(30001)),
        ("query_timeout_ms", json!(30001)),
    ] {
        let mut document = fixture.document.clone();
        document[field] = value;
        let error = ObserverConfig::from_json(&serde_json::to_vec(&document).unwrap()).unwrap_err();
        assert_eq!(error.kind, ObservationFailureKind::Malformed, "{field}");
        assert!(!error.to_string().contains(marker));
        assert!(!format!("{error:?}").contains(marker));
    }
}

#[test]
fn username_and_password_cannot_share_a_secret_key() {
    let mut fixture = Fixture::new();
    fixture.document["observer_password_file"] = fixture.document["observer_username_file"].clone();
    assert!(ObserverConfig::from_json(&serde_json::to_vec(&fixture.document).unwrap()).is_err());
}

#[test]
fn all_progress_and_failure_json_shapes_preserve_the_contract() {
    let progress = DecimalProgress::parse("9999999999999999999999999").unwrap();
    assert_eq!(
        serde_json::to_value(progress).unwrap(),
        json!("9999999999999999999999999")
    );
    let failure: Observation<InstanceSnapshot> = Observation::Failed(
        RuntimeError::new(
            ObservationFailureKind::PermissionDenied,
            "instance",
            "permission missing",
        )
        .into_failure(123),
    );
    let value = serde_json::to_value(failure).unwrap();
    assert_eq!(value["status"], "failed");
    assert_eq!(value["kind"], "permission_denied");
    assert_eq!(value["observed_at_unix_millis"], 123);
}

#[test]
fn freshness_is_inclusive_and_must_be_recomputed_by_consumers() {
    let fixture = Fixture::new();
    let report = ObservationReport::new(
        &fixture.config(),
        Observation::Absent {
            observed_at_unix_millis: 100_000,
        },
        160_000,
    );
    assert!(report.fresh);
    assert!(report.is_fresh_at(160_000));
    assert!(!report.is_fresh_at(160_001));
    assert!(!report.is_fresh_at(99_999));
    let value = serde_json::to_value(&report).unwrap();
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["max_age_millis"], 60_000);
    assert_eq!(value["source"]["availability_group"], "test-ag");
    assert_eq!(value["source"]["replica"]["incarnation"], "pod-uid-0");
    assert!(!value.to_string().contains("password"));
}

struct FailingExecutor(Arc<AtomicUsize>);

#[async_trait]
impl SqlExecutor for FailingExecutor {
    async fn connect(&self) -> Result<Box<dyn SqlSession>, RuntimeError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(RuntimeError::new(
            ObservationFailureKind::PermissionDenied,
            "test",
            "permission missing",
        ))
    }
}

struct PendingExecutor;

#[async_trait]
impl SqlExecutor for PendingExecutor {
    async fn connect(&self) -> Result<Box<dyn SqlSession>, RuntimeError> {
        std::future::pending().await
    }
}

#[tokio::test(start_paused = true)]
async fn whole_sample_deadline_produces_a_timestamped_failure() {
    let config = Fixture::new().config();
    let manager = SqlServerInstanceManager::new(PendingExecutor, config);
    let start = tokio::time::Instant::now();
    let observation = manager.observe().await.unwrap();
    assert_eq!(start.elapsed(), Duration::from_secs(30));
    assert!(!observation.is_fresh_at(observation.observed_at_unix_millis(), 60_000));
    match observation {
        Observation::Failed(failure) => {
            assert!(failure.observed_at_unix_millis > 0);
            assert_eq!(failure.kind, ObservationFailureKind::TimedOut);
        }
        other => panic!("unexpected observation: {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn monitor_publishes_failures_reconnects_and_obeys_poll_interval() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let manager =
        SqlServerInstanceManager::new(FailingExecutor(attempts.clone()), Fixture::new().config());
    let monitor = SqlServerMonitor::new(manager);
    let (sender, mut receiver) = watch::channel(None);
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let task = tokio::spawn(async move { monitor.run(sender, task_cancel).await });

    receiver.changed().await.unwrap();
    let report = receiver.borrow_and_update().clone().unwrap();
    assert!(!report.fresh);
    assert!(matches!(report.observation, Observation::Failed(_)));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_millis(999)).await;
    tokio::task::yield_now().await;
    assert!(!receiver.has_changed().unwrap());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_millis(1)).await;
    receiver.changed().await.unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    cancel.cancel();
    let summary = task.await.unwrap().unwrap();
    assert!(summary.had_failed_or_stale_sample);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn cancellation_drops_an_inflight_observation_without_publishing_success() {
    let monitor = SqlServerMonitor::new(SqlServerInstanceManager::new(
        PendingExecutor,
        Fixture::new().config(),
    ));
    let (sender, receiver) = watch::channel(None);
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let task = tokio::spawn(async move { monitor.run(sender, task_cancel).await });
    tokio::task::yield_now().await;
    cancel.cancel();
    let summary = task.await.unwrap().unwrap();
    assert!(!summary.had_failed_or_stale_sample);
    assert!(receiver.borrow().is_none());
}

#[tokio::test]
async fn a_missing_monitor_subscriber_is_an_explicit_error() {
    let monitor = SqlServerMonitor::new(SqlServerInstanceManager::new(
        FailingExecutor(Arc::new(AtomicUsize::new(0))),
        Fixture::new().config(),
    ));
    let (sender, receiver) = watch::channel(None);
    drop(receiver);
    let error = monitor
        .run(sender, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.stage, "monitor");
    assert_eq!(error.kind, ObservationFailureKind::Unreachable);
}

#[tokio::test]
async fn secret_failures_are_explicit_and_do_not_reach_tds() {
    for contents in ["", "not-a-valid-secret\n", "\0invalid", "two\rlines"] {
        let fixture = Fixture::new();
        std::fs::write(
            fixture.document["observer_password_file"].as_str().unwrap(),
            contents,
        )
        .unwrap();
        let executor = TdsExecutor::new(fixture.config().connection().clone());
        let error = executor
            .connect()
            .await
            .err()
            .expect("invalid Secret must fail");
        assert_eq!(error.kind, ObservationFailureKind::Malformed);
        assert_eq!(error.stage, "observer password");
        assert!(!error.to_string().contains("not-a-valid-secret"));
    }
}

#[tokio::test]
async fn each_attempt_rereads_secrets_and_enforces_file_bounds() {
    let fixture = Fixture::new();
    let password_path = fixture.document["observer_password_file"].as_str().unwrap();
    std::fs::write(password_path, "x".repeat(4097)).unwrap();
    let executor = TdsExecutor::new(fixture.config().connection().clone());
    let first = executor.connect().await.err().unwrap();
    assert_eq!(first.kind, ObservationFailureKind::Malformed);
    assert_eq!(first.stage, "observer password");
    std::fs::write(password_path, "new-unit-test-credential").unwrap();
    std::fs::remove_file(fixture.document["observer_username_file"].as_str().unwrap()).unwrap();
    let second = executor.connect().await.err().unwrap();
    assert_eq!(second.stage, "observer username");
    assert_eq!(second.kind, ObservationFailureKind::Unreachable);
}

#[tokio::test]
async fn an_unresponsive_tds_endpoint_is_bounded_and_does_not_receive_login() {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let mut fixture = Fixture::new();
    fixture.document["host"] = json!("127.0.0.1");
    fixture.document["port"] = json!(listener.local_addr().unwrap().port());
    fixture.document["connect_timeout_ms"] = json!(500);
    let executor = TdsExecutor::new(fixture.config().connection().clone());
    let peer = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).await.unwrap();
        bytes
    };
    let (result, bytes) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(executor.connect(), peer)
    })
    .await
    .unwrap();
    let error = result.err().expect("endpoint never completed prelogin");
    assert_eq!(error.kind, ObservationFailureKind::TimedOut);
    assert_eq!(bytes[0], 0x12, "only a TDS PRELOGIN packet was sent");
    let first_packet_length = usize::from(u16::from_be_bytes([bytes[2], bytes[3]]));
    assert_eq!(
        bytes.len(),
        first_packet_length,
        "no login packet before TLS"
    );
}

#[tokio::test]
async fn a_server_cannot_downgrade_the_observer_to_plaintext_login() {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let mut fixture = Fixture::new();
    fixture.document["host"] = json!("127.0.0.1");
    fixture.document["port"] = json!(listener.local_addr().unwrap().port());
    let executor = TdsExecutor::new(fixture.config().connection().clone());
    let peer = async {
        // A PRELOGIN response advertising ENCRYPT_NOT_SUP.
        let mut stream = tds_peer::respond_to_prelogin(
            &listener,
            &[4, 1, 0, 15, 0, 0, 1, 0, 1, 0, 6, 0, 1, 255, 2],
        )
        .await;
        let mut header = [0; 8];
        stream.read_exact(&mut header).await.unwrap();
        assert_eq!(
            header[0], 0x12,
            "TLS negotiation, never a LOGIN7 (0x10) packet"
        );
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(executor.connect(), peer)
    })
    .await
    .unwrap();
    assert!(result.is_err(), "no plaintext authentication fallback");
}
