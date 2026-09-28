mod persistence;
mod service;
mod state;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, put};
use clap::Parser;
use kuberic_agent::process::{
    ApplicationStorageState, ReplicaDiagnostics, ReplicaHandle, ReplicaHost, ReplicaProcessConfig,
};
use kuberic_agent::transport::KubernetesDnsResolver;
use kuberic_protocol::types::{PodUid, PvcUid, ReplicaId, ResourceUid};
use tokio::sync::watch;

use crate::persistence::KvPersistence;
use crate::service::KvService;
use crate::state::CopyGate;

#[derive(Debug, Parser)]
struct Config {
    #[arg(long, env = "KUBERIC_RESOURCE_UID")]
    resource_uid: String,
    #[arg(long, env = "KUBERIC_REPLICA_ID")]
    replica_id: i64,
    #[arg(long, env = "KUBERIC_POD_UID")]
    pod_uid: String,
    #[arg(long, env = "KUBERIC_PVC_UID")]
    pvc_uid: String,
    #[arg(long, env = "KUBERIC_POD_IP")]
    pod_ip: String,
    #[arg(long, env = "KUBERIC_SET_NAME")]
    set_name: String,
    #[arg(long, env = "KUBERIC_NAMESPACE")]
    namespace: String,
    #[arg(long, env = "KUBERIC_AGENT_BEARER_TOKEN")]
    bearer_token: String,
    #[arg(long, env = "KUBERIC_DATA_ROOT", default_value = "/var/lib/kuberic")]
    data_root: PathBuf,
    #[arg(long, env = "KUBERIC_CONTROL_ADDRESS", default_value = "0.0.0.0:50051")]
    control_address: SocketAddr,
    #[arg(
        long,
        env = "KUBERIC_REPLICATION_ADDRESS",
        default_value = "0.0.0.0:50052"
    )]
    replication_address: SocketAddr,
    #[arg(
        long,
        env = "KUBERIC_APPLICATION_ADDRESS",
        default_value = "0.0.0.0:8080"
    )]
    application_address: SocketAddr,
    #[arg(long, env = "KUBERIC_LIVE_TEST_COPY_GATE_ADDRESS")]
    live_test_copy_gate_address: Option<SocketAddr>,
}

#[derive(Clone)]
struct HttpState {
    application: Arc<KvService>,
    replica: ReplicaHandle,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let config = Config::parse();
    if config.replica_id <= 0 {
        anyhow::bail!("KUBERIC_REPLICA_ID must be positive");
    }
    let application_path = config.data_root.join("application");
    let application_storage = if KvPersistence::is_fresh_empty(&application_path)? {
        ApplicationStorageState::FreshEmpty
    } else {
        ApplicationStorageState::Established
    };
    let persistence = Arc::new(KvPersistence::open(application_path)?);
    let copy_gate = match config.live_test_copy_gate_address {
        Some(_) => CopyGate::enabled(config.data_root.join(".live-test-copy-gate"))?,
        None => CopyGate::disabled(),
    };
    let application = Arc::new(KvService::new(
        persistence,
        format!("http://{}:50052", config.pod_ip),
        copy_gate.clone(),
    ));
    let mut replica = ReplicaHost::new(
        ReplicaProcessConfig {
            resource_uid: ResourceUid::new(&config.resource_uid),
            replica_id: ReplicaId::new(config.replica_id),
            pod_uid: PodUid::new(&config.pod_uid),
            pvc_uid: PvcUid::new(&config.pvc_uid),
            data_root: config.data_root.clone(),
            control_address: config.control_address,
            replication_address: config.replication_address,
            bearer_token: config.bearer_token.clone(),
            rpc_deadline: Duration::from_secs(5),
            transport_window_capacity: 256,
        },
        application.clone(),
        application_storage,
        Arc::new(KubernetesDnsResolver::new(
            ResourceUid::new(&config.resource_uid),
            config.namespace.clone(),
        )),
    )
    .start()
    .await?;
    let shutdown_rx = replica.shutdown_signal();
    let http_state = HttpState {
        application,
        replica: replica.handle(),
    };
    let router = application_router(http_state);
    let listener = tokio::net::TcpListener::bind(config.application_address).await?;
    let mut http_task = tokio::spawn(
        axum::serve(listener, router)
            .with_graceful_shutdown(wait_shutdown(shutdown_rx.clone()))
            .into_future(),
    );
    let live_test_address = config.live_test_copy_gate_address;
    let live_test_router = copy_gate_router(live_test_address.map(|_| copy_gate));
    let mut live_test_task = tokio::spawn(async move {
        if let Some(address) = live_test_address {
            let listener = tokio::net::TcpListener::bind(address).await?;
            axum::serve(listener, live_test_router)
                .with_graceful_shutdown(wait_shutdown(shutdown_rx))
                .await
        } else {
            futures::future::pending::<std::io::Result<()>>().await
        }
    });

    tokio::select! {
        result = replica.wait() => result?,
        result = &mut http_task => result??,
        result = &mut live_test_task => result??,
        result = tokio::signal::ctrl_c() => result?,
    }

    replica.shutdown();
    Ok(())
}

fn application_router(state: HttpState) -> Router {
    Router::new()
        .route("/kv/{key}", put(put_value).get(get_value))
        .route("/status", get(get_status))
        .with_state(state)
}

fn copy_gate_router(copy_gate: Option<CopyGate>) -> Router {
    match copy_gate {
        Some(copy_gate) => Router::new()
            .route("/live-test/copy-gate/{action}", put(set_copy_gate))
            .with_state(copy_gate),
        None => Router::new(),
    }
}

async fn set_copy_gate(
    State(copy_gate): State<CopyGate>,
    Path(action): Path<String>,
) -> std::result::Result<&'static str, (StatusCode, String)> {
    match action.as_str() {
        "hold" => copy_gate.hold().map(|()| "held"),
        "release" => copy_gate.release().map(|()| "released"),
        _ => return Err((StatusCode::BAD_REQUEST, "unknown copy-gate action".into())),
    }
    .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))
}

async fn put_value(
    State(state): State<HttpState>,
    Path(key): Path<String>,
    body: Bytes,
) -> std::result::Result<String, (StatusCode, String)> {
    let value = String::from_utf8(body.to_vec())
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    state
        .application
        .replicate_put(key, value)
        .await
        .map(|lsn| lsn.to_string())
        .map_err(runtime_http_error)
}

async fn get_value(
    State(state): State<HttpState>,
    Path(key): Path<String>,
) -> std::result::Result<String, StatusCode> {
    state
        .application
        .get(&key)
        .await
        .map_err(|error| runtime_http_error(error).0)?
        .ok_or(StatusCode::NOT_FOUND)
}

async fn get_status(
    State(state): State<HttpState>,
) -> std::result::Result<axum::Json<ReplicaDiagnostics>, (StatusCode, String)> {
    state
        .replica
        .diagnostics()
        .await
        .map(axum::Json)
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))
}

fn runtime_http_error(error: kuberic_runtime::RuntimeError) -> (StatusCode, String) {
    let status = match error {
        kuberic_runtime::RuntimeError::NotPrimary
        | kuberic_runtime::RuntimeError::NotOpen
        | kuberic_runtime::RuntimeError::WriteClosed(_)
        | kuberic_runtime::RuntimeError::ReadClosed(_) => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, error.to_string())
}

async fn wait_shutdown(mut shutdown: watch::Receiver<bool>) {
    let _ = shutdown.wait_for(|stopped| *stopped).await;
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    use crate::state::CopyGate;

    #[test]
    fn diagnostics_distinguish_terminal_retirement_from_closed_access() {
        for retired in [false, true] {
            let diagnostics = super::ReplicaDiagnostics {
                replica_id: 3,
                instance_id: "pod-3".into(),
                agent_generation: "generation-3".into(),
                process_session: "restarted".into(),
                role: "None".into(),
                epoch: "0.2".into(),
                previous_configuration: None,
                current_configuration: None,
                current_progress: 5,
                verified_replication_lsn: None,
                committed_lsn: 5,
                read_status: "NotPrimary".into(),
                write_status: "NotPrimary".into(),
                catch_up_boundary_lsn: None,
                catch_up_complete: false,
                scale_up_operation: None,
                retired,
                pending_operation: None,
                blocking: None,
                builds: Vec::new(),
            };
            let json = serde_json::to_value(diagnostics).unwrap();
            assert_eq!(json["retired"], retired);
            assert_eq!(json["role"], "None");
            assert_eq!(json["writeStatus"], "NotPrimary");
            assert!(json.get("retiredAuthority").is_none());
            assert!(json.get("acceptedSecondaryRemoval").is_none());
        }
    }

    #[test]
    fn retired_application_requests_are_explicitly_unavailable() {
        let (status, message) = super::runtime_http_error(kuberic_runtime::RuntimeError::NotOpen);
        assert_eq!(status, axum::http::StatusCode::SERVICE_UNAVAILABLE);
        assert!(!message.is_empty());
    }

    #[test]
    fn application_main_uses_the_agent_host_instead_of_protocol_internals() {
        let source = include_str!("main.rs");
        let production = source.split("#[cfg(test)]").next().unwrap();
        for internal in [
            "AgentService",
            "GrpcOutboundDispatcher",
            "InitializationService",
            "PodRuntime",
            "ReliableTransport",
            "SqliteStore",
            "run_outbound",
            "run_peer_discovery",
        ] {
            assert!(
                !production.contains(internal),
                "kvstore2 main must not assemble {internal}"
            );
        }
        assert!(production.contains("ReplicaHost::new"));
    }

    #[tokio::test]
    async fn default_router_does_not_register_live_test_copy_gate() {
        let response = super::copy_gate_router(None)
            .oneshot(
                Request::put("/live-test/copy-gate/hold")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn explicitly_enabled_live_test_router_arms_gate() {
        let directory = tempfile::tempdir().unwrap();
        let gate = CopyGate::enabled(directory.path().join("copy-gate")).unwrap();
        let response = super::copy_gate_router(Some(gate.clone()))
            .oneshot(
                Request::put("/live-test/copy-gate/hold")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert!(gate.is_held());
        gate.release().unwrap();
    }

    #[test]
    fn default_manifest_neither_enables_nor_routes_live_test_gate() {
        let sample = include_str!("../deploy/sample.yaml");
        let service = include_str!("../deploy/service.yaml");
        assert!(!sample.contains("testing.kuberic.io/live-copy-gate"));
        assert!(!sample.contains("KUBERIC_LIVE_TEST_COPY_GATE_ADDRESS"));
        assert!(service.contains("targetPort: 8080"));
        assert!(!service.contains("18080"));
    }
}
