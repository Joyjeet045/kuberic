use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use kuberic_agent::process::{ReplicaHost, ReplicaProcessConfig, RunningReplica};
use kuberic_agent::transport::ReplicaEndpointResolver;
use kuberic_protocol::types::{PodUid, PvcUid, ReplicaId, ReplicaIdentity, ResourceUid};
use kuberic_runtime::StatefulServiceReplica;
use postgres_replicated::{PgService, PgServiceConfig, data_service::PgDataServiceImpl};

type ProcessResult = Result<(), Box<dyn std::error::Error>>;
type CoordinationTask = tokio::task::JoinHandle<Result<(), tonic::transport::Error>>;
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Parser)]
#[command(about = "V2 SF custom PostgreSQL replicator (bootstrap, build and recovery)")]
struct Config {
    #[arg(long, env = "KUBERIC_PEER_ROUTES")]
    peer_routes: Option<PathBuf>,
    #[arg(long, env = "KUBERIC_RESOURCE_UID")]
    resource_uid: String,
    #[arg(long, env = "KUBERIC_REPLICA_ID")]
    replica_id: i64,
    #[arg(long, env = "KUBERIC_POD_UID")]
    pod_uid: String,
    #[arg(long, env = "KUBERIC_PVC_UID")]
    pvc_uid: String,
    #[arg(long, env = "KUBERIC_AGENT_BEARER_TOKEN")]
    bearer_token: String,
    #[arg(long, env = "KUBERIC_DATA_ROOT")]
    data_root: PathBuf,
    #[arg(long, env = "KUBERIC_APPLICATION_ROOT")]
    application_root: Option<PathBuf>,
    #[arg(long, env = "PGDATA")]
    pg_data: Option<PathBuf>,
    #[arg(long, env = "KUBERIC_PG_BIN")]
    pg_bin: PathBuf,
    #[arg(long, env = "PGPORT", default_value_t = 5432)]
    pg_port: u16,
    #[arg(
        long,
        env = "KUBERIC_CONTROL_ADDRESS",
        default_value = "127.0.0.1:50051"
    )]
    control_address: SocketAddr,
    #[arg(
        long,
        env = "KUBERIC_REPLICATION_ADDRESS",
        default_value = "127.0.0.1:50052"
    )]
    replication_address: SocketAddr,
    #[arg(
        long,
        env = "KUBERIC_APPLICATION_ADDRESS",
        default_value = "127.0.0.1:50053"
    )]
    application_address: SocketAddr,
    #[arg(long, env = "KUBERIC_APPLICATION_ENDPOINT")]
    application_endpoint: Option<String>,
    #[arg(
        long,
        env = "KUBERIC_CONTROL_ENDPOINT",
        default_value = "http://127.0.0.1:50051"
    )]
    control_endpoint: String,
    #[arg(
        long,
        env = "KUBERIC_REPLICATION_ENDPOINT",
        default_value = "http://127.0.0.1:50052"
    )]
    replication_endpoint: String,
}

struct PgEndpointResolver {
    identity: ReplicaIdentity,
    control: String,
    replication: String,
    routes: std::collections::BTreeMap<ReplicaIdentity, PeerRoute>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PeerRoute {
    identity: ReplicaIdentity,
    control: String,
    replication: String,
}

impl ReplicaEndpointResolver for PgEndpointResolver {
    fn control_endpoint(&self, identity: &ReplicaIdentity) -> String {
        if identity == &self.identity {
            self.control.clone()
        } else {
            self.routes
                .get(identity)
                .map(|route| route.control.clone())
                .unwrap_or_default()
        }
    }
    fn replication_endpoint(&self, identity: &ReplicaIdentity) -> String {
        if identity == &self.identity {
            self.replication.clone()
        } else {
            self.routes
                .get(identity)
                .map(|route| route.replication.clone())
                .unwrap_or_default()
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if postgres_replicated::process_supervisor::run_if_requested()? {
        return Ok(());
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(16 * 1024 * 1024)
        .build()?
        .block_on(run())
}

async fn run() -> ProcessResult {
    tracing_subscriber::fmt::init();
    let mut config = Config::parse();
    if config.replica_id <= 0 || config.pg_port == 0 {
        return Err("replica ID and PostgreSQL port must be positive".into());
    }
    let cwd = std::env::current_dir()?;
    if config.data_root.is_relative() {
        config.data_root = cwd.join(config.data_root);
    }
    let application_endpoint = config
        .application_endpoint
        .unwrap_or_else(|| format!("http://{}", config.application_address));
    if !application_endpoint.starts_with("http://")
        || application_endpoint.len() > 512
        || application_endpoint.chars().any(char::is_control)
    {
        return Err("invalid advertised application endpoint".into());
    }
    let service_config = PgServiceConfig {
        resource_uid: ResourceUid::new(&config.resource_uid),
        application_root: config
            .application_root
            .map(|path| cwd.join(path))
            .unwrap_or_else(|| config.data_root.join("application")),
        pg_data: config
            .pg_data
            .map(|path| cwd.join(path))
            .unwrap_or_else(|| config.data_root.join("pgdata")),
        pg_bin: config.pg_bin,
        pg_port: config.pg_port,
        replication_address: application_endpoint,
    };
    let storage = PgService::storage_state(&service_config)?;
    let storage_paths = PgService::storage_paths(&service_config);
    let coordination_token = config.bearer_token.clone();
    let mut routes = std::collections::BTreeMap::new();
    if let Some(path) = config.peer_routes {
        let bytes = std::fs::read(path)?;
        if bytes.len() > 65536 {
            return Err("peer route file exceeds 64 KiB".into());
        }
        let peers: Vec<PeerRoute> = serde_json::from_slice(&bytes)?;
        if peers.len() > 32 {
            return Err("too many peer routes".into());
        }
        for peer in peers {
            if peer.identity.replica_id.value() <= 0
                || peer.identity.instance_id.is_empty()
                || peer.identity.agent_generation.is_empty()
                || [&peer.control, &peer.replication]
                    .iter()
                    .any(|endpoint| !endpoint.starts_with("http://") || endpoint.len() > 512)
                || routes.insert(peer.identity.clone(), peer).is_some()
            {
                return Err("invalid or duplicate exact peer route".into());
            }
        }
    }
    let application = Arc::new(
        PgService::deferred(service_config).with_coordination_token(coordination_token.clone()),
    );
    let local_identity = ReplicaIdentity {
        replica_id: ReplicaId::new(config.replica_id),
        instance_id: kuberic_protocol::types::ReplicaInstanceId::new(config.pod_uid.clone()),
        agent_generation: kuberic_protocol::types::derive_agent_generation(
            &kuberic_protocol::types::derive_initialization_id(
                &ResourceUid::new(&config.resource_uid),
                ReplicaId::new(config.replica_id),
                &PodUid::new(&config.pod_uid),
                &PvcUid::new(&config.pvc_uid),
            ),
        ),
    };
    let resolver = Arc::new(PgEndpointResolver {
        identity: local_identity,
        control: config.control_endpoint,
        replication: config.replication_endpoint,
        routes,
    });
    let host = ReplicaHost::new(
        ReplicaProcessConfig {
            resource_uid: ResourceUid::new(config.resource_uid),
            replica_id: ReplicaId::new(config.replica_id),
            pod_uid: PodUid::new(config.pod_uid),
            pvc_uid: PvcUid::new(config.pvc_uid),
            data_root: config.data_root,
            control_address: config.control_address,
            replication_address: config.replication_address,
            bearer_token: config.bearer_token,
            rpc_deadline: Duration::from_secs(5),
            transport_window_capacity: 256,
        },
        application.clone(),
        storage,
        resolver,
    )
    .with_application_storage_paths(storage_paths);
    let signal = shutdown_signal();
    tokio::pin!(signal);
    let (startup_shutdown, receiver) = tokio::sync::watch::channel(false);
    let mut startup = tokio::spawn(host.start_with_shutdown(receiver));
    let (started, trigger) = tokio::select! {
        result = &mut startup => (result, None),
        result = &mut signal => {
            startup_shutdown.send_replace(true);
            (startup.await, Some(result.map_err(Box::<dyn std::error::Error>::from)))
        }
    };
    let mut replica = match started {
        Ok(Ok(Some(replica))) => {
            if let Some(trigger) = trigger {
                return finish_shutdown(replica, &application, None, None, trigger).await;
            }
            replica
        }
        result => {
            let result = match result {
                Ok(Ok(None)) => Ok(()),
                Ok(Err(error)) => Err(error.into()),
                Err(error) => Err(error.into()),
                Ok(Ok(Some(_))) => unreachable!(),
            };
            return finish_application(
                &application,
                with_cleanup(result, trigger.unwrap_or(Ok(())), "shutdown trigger"),
            )
            .await;
        }
    };
    let listener = match tokio::net::TcpListener::bind(config.application_address).await {
        Ok(listener) => listener,
        Err(error) => {
            return finish_shutdown(replica, &application, None, None, Err(error.into())).await;
        }
    };
    let mut shutdown = replica.shutdown_signal();
    let coordination_service = PgDataServiceImpl::new(application.clone(), coordination_token);
    let mut coordination = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(coordination_service.into_server())
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move {
                    let _ = shutdown.wait_for(|stopped| *stopped).await;
                },
            )
            .await
    });
    let (completion, result, coordination_finished) = tokio::select! {
        result = replica.wait() => (Some(result), Ok(()), false),
        result = &mut coordination => (None, flatten_coordination(result), true),
        result = &mut signal => (None, result.map_err(Box::<dyn std::error::Error>::from), false),
    };
    finish_shutdown(
        replica,
        &application,
        completion,
        (!coordination_finished).then_some(coordination),
        result,
    )
    .await
}

async fn finish_shutdown(
    mut replica: RunningReplica,
    application: &PgService,
    completion: Option<kuberic_agent::Result<()>>,
    coordination: Option<CoordinationTask>,
    trigger: ProcessResult,
) -> ProcessResult {
    replica.shutdown();
    // Replica completion is the durable fault acknowledgement, even when another
    // task initiated shutdown. Keep it primary without skipping companion cleanup.
    let completion = match completion {
        Some(result) => result,
        None => replica.wait().await,
    };
    let mut result = with_cleanup(completion.map_err(Into::into), trigger, "shutdown trigger");
    if let Some(coordination) = coordination {
        // Runtime shutdown has revoked process authority. Cancel retained RPCs,
        // join their cleanup, and do not wait on a peer to close an HTTP/2 stream.
        coordination.abort();
        let cleanup = match coordination.await {
            Err(error) if error.is_cancelled() => Ok(()),
            result => flatten_coordination(result),
        };
        result = with_cleanup(result, cleanup, "coordination shutdown");
    }
    finish_application(application, result).await
}

async fn finish_application(application: &PgService, result: ProcessResult) -> ProcessResult {
    let mut cleanup = match tokio::time::timeout(CLEANUP_TIMEOUT, application.close()).await {
        Ok(result) => result.map_err(Into::into),
        Err(_) => Err("application shutdown timed out".into()),
    };
    if cleanup.is_err() {
        cleanup = with_cleanup(
            cleanup,
            application.abort_and_wait().map_err(Into::into),
            "owned PostgreSQL termination",
        );
    }
    with_cleanup(result, cleanup, "application shutdown")
}

fn flatten_coordination(
    result: Result<Result<(), tonic::transport::Error>, tokio::task::JoinError>,
) -> ProcessResult {
    result
        .map_err(Box::<dyn std::error::Error>::from)?
        .map_err(Into::into)
}

fn with_cleanup(primary: ProcessResult, cleanup: ProcessResult, context: &str) -> ProcessResult {
    match (primary, cleanup) {
        (Err(primary), Err(cleanup)) => Err(format!("{primary}; {context}: {cleanup}").into()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

async fn shutdown_signal() -> std::io::Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result?,
        _ = terminate.recv() => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shutdown_preserves_acknowledgement_failure_with_cleanup_context() {
        let result = with_cleanup(
            Err("partition fault persistence timed out".into()),
            Err("coordination failed".into()),
            "shutdown trigger",
        );
        let result = with_cleanup(
            result,
            Err("PostgreSQL stop failed".into()),
            "application shutdown",
        );
        assert_eq!(
            result.unwrap_err().to_string(),
            "partition fault persistence timed out; shutdown trigger: coordination failed; \
             application shutdown: PostgreSQL stop failed"
        );
    }

    #[test]
    fn shutdown_never_converts_acknowledgement_or_cleanup_failure_to_success() {
        for acknowledgement in [
            "partition fault persistence timed out",
            "agent shutdown acknowledgement timed out",
            "fault acknowledgement consumer stopped",
            "database is locked",
        ] {
            let result = with_cleanup(Err(acknowledgement.into()), Ok(()), "shutdown trigger");
            assert_eq!(result.unwrap_err().to_string(), acknowledgement);
        }
        let result = with_cleanup(
            Ok(()),
            Err("application close failed".into()),
            "application shutdown",
        );
        assert_eq!(result.unwrap_err().to_string(), "application close failed");
        assert!(with_cleanup(Ok(()), Ok(()), "application shutdown").is_ok());
    }
}
