#[path = "../tests/common/mod.rs"]
mod common;

use std::future::pending;
use std::process::ExitCode;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use sqlserver_replicated::executor::{QueryRow, SqlExecutor, SqlSession};
use sqlserver_replicated::instance::SqlServerInstanceManager;
use sqlserver_replicated::query::ReadQuery;
use sqlserver_replicated::runtime_config::ObserverConfig;
use sqlserver_replicated::runtime_error::RuntimeError;
use sqlserver_replicated::{AvailabilityGroupName, Observation, ObservationFailureKind};
use tokio::sync::{mpsc, oneshot};

use super::run_watch;
use common::Fixture;

#[derive(Clone, Copy)]
enum Sample {
    Fresh,
    Failed,
}

struct ControlledExecutor {
    requests: mpsc::Sender<oneshot::Sender<Sample>>,
}

#[async_trait]
impl SqlExecutor for ControlledExecutor {
    async fn connect(&self) -> Result<Box<dyn SqlSession>, RuntimeError> {
        let (request, response) = oneshot::channel();
        self.requests.send(request).await.expect("test controller");
        match response.await.expect("test supplies a sample or cancels") {
            Sample::Fresh => Ok(Box::new(AbsentSession)),
            Sample::Failed => Err(RuntimeError::new(
                ObservationFailureKind::PermissionDenied,
                "test",
                "permission missing",
            )),
        }
    }
}

struct AbsentSession;

#[async_trait]
impl SqlSession for AbsentSession {
    async fn query(
        &mut self,
        query: ReadQuery,
        availability_group: &AvailabilityGroupName,
    ) -> Result<Vec<QueryRow>, RuntimeError> {
        assert_eq!(availability_group.as_str(), "test-ag");
        let row = match query {
            ReadQuery::Permissions => json!({
                "product_major_version": "16",
                "view_server_state": "1",
                "view_server_performance_state": "1",
                "view_any_definition": "1",
                "view_any_database": "1"
            }),
            ReadQuery::Anchor => json!({
                "server_name": "sql-0",
                "property_server_name": "sql-0",
                "product_version": "16.0.4225.2",
                "product_major_version": "16",
                "edition": "Developer Edition (64-bit)",
                "engine_edition": "3",
                "is_hadr_enabled": "1",
                "host_platform": "Linux",
                "host_distribution": "Ubuntu",
                "architecture": "x86_64",
                "sqlserver_start_time": "2026-08-01T10:00:00",
                "group_id": null,
                "group_name": null,
                "cluster_type": null,
                "cluster_type_desc": null,
                "sequence_number": null,
                "required_synchronized_secondaries_to_commit": null,
                "basic_features": null,
                "is_distributed": null,
                "local_replica_id": null,
                "local_replica_server_name": null,
                "local_state_group_id": null,
                "local_state_replica_id": null,
                "local_role_desc": null
            }),
            other => panic!("unexpected absent-AG query: {other:?}"),
        };
        Ok(vec![serde_json::from_value(row).unwrap()])
    }
}

async fn blocked_output_exit(samples: &[Sample]) -> ExitCode {
    let mut fixture = Fixture::new();
    fixture.document["poll_interval_ms"] = json!(1);
    let config = ObserverConfig::read(&fixture.config_path()).await.unwrap();
    assert_eq!(config.target().replica, fixture.config().target().replica);
    let (request_sender, mut requests) = mpsc::channel(1);
    let manager = SqlServerInstanceManager::new(
        ControlledExecutor {
            requests: request_sender,
        },
        config,
    );
    let (started_sender, started_receiver) = oneshot::channel();
    let mut started_sender = Some(started_sender);
    let (stop_sender, stop_receiver) = oneshot::channel();
    let mut writes = 0;
    let watch = run_watch(
        manager,
        async |report| {
            writes += 1;
            assert_eq!(writes, 1, "the first write must remain blocked");
            assert!(report.fresh);
            let Observation::Present { value, .. } = &report.observation else {
                panic!("expected a successful instance observation");
            };
            assert!(matches!(
                value.availability_group,
                Observation::Absent { .. }
            ));
            started_sender.take().unwrap().send(()).unwrap();
            pending().await
        },
        async {
            stop_receiver.await.unwrap();
            Ok(())
        },
    );
    let controller = async {
        requests
            .recv()
            .await
            .unwrap()
            .send(Sample::Fresh)
            .unwrap_or_else(|_| panic!("first observation cancelled"));
        started_receiver.await.unwrap();
        for sample in samples {
            requests
                .recv()
                .await
                .unwrap()
                .send(*sample)
                .unwrap_or_else(|_| panic!("scripted observation cancelled"));
        }
        // The next connect starts only after the preceding report was published.
        let mut in_flight = requests.recv().await.unwrap();
        let stopped_at = tokio::time::Instant::now();
        stop_sender.send(()).unwrap();
        in_flight.closed().await;
        assert_eq!(stopped_at.elapsed(), Duration::ZERO);
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(watch, controller)
    })
    .await
    .expect("watch must stop without draining output or timing out a sample");
    assert_eq!(writes, 1);
    result.unwrap()
}

#[tokio::test(start_paused = true)]
async fn overwritten_failure_makes_watch_exit_nonzero() {
    assert_eq!(
        blocked_output_exit(&[Sample::Failed, Sample::Fresh]).await,
        ExitCode::FAILURE
    );
}

#[tokio::test(start_paused = true)]
async fn pending_failure_makes_watch_exit_nonzero_on_shutdown() {
    assert_eq!(
        blocked_output_exit(&[Sample::Failed]).await,
        ExitCode::FAILURE
    );
}

#[tokio::test(start_paused = true)]
async fn fresh_absent_ag_does_not_make_watch_exit_nonzero() {
    assert_eq!(
        blocked_output_exit(&[Sample::Fresh]).await,
        ExitCode::SUCCESS
    );
}

#[tokio::test(start_paused = true)]
async fn shutdown_before_first_publication_does_not_make_watch_exit_nonzero() {
    let (request_sender, mut requests) = mpsc::channel(1);
    let fixture = Fixture::new();
    let manager = SqlServerInstanceManager::new(
        ControlledExecutor {
            requests: request_sender,
        },
        fixture.config(),
    );
    let (stop_sender, stop_receiver) = oneshot::channel();
    let watch = run_watch(
        manager,
        async |_| panic!("cancelled attempt must not publish"),
        async {
            stop_receiver.await.unwrap();
            Ok(())
        },
    );
    let controller = async {
        let mut in_flight = requests.recv().await.unwrap();
        let stopped_at = tokio::time::Instant::now();
        stop_sender.send(()).unwrap();
        in_flight.closed().await;
        assert_eq!(stopped_at.elapsed(), Duration::ZERO);
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(watch, controller)
    })
    .await
    .expect("watch must cancel an unpublished attempt promptly");
    assert_eq!(result.unwrap(), ExitCode::SUCCESS);
}
