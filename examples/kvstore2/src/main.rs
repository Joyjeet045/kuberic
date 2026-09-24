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
    let application = Arc::new(KvService::new(
        persistence,
        format!("http://{}:50052", config.pod_ip),
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
    let router = Router::new()
        .route("/kv/{key}", put(put_value).get(get_value))
        .route("/status", get(get_status))
        .with_state(http_state);
    let listener = tokio::net::TcpListener::bind(config.application_address).await?;
    let mut http_task = tokio::spawn(
        axum::serve(listener, router)
            .with_graceful_shutdown(wait_shutdown(shutdown_rx.clone()))
            .into_future(),
    );

    tokio::select! {
        result = replica.wait() => result?,
        result = &mut http_task => result??,
        result = tokio::signal::ctrl_c() => result?,
    }

    replica.shutdown();
    Ok(())
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
}
