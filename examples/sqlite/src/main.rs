use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use kuberic_runtime::host::KubernetesDnsResolver;
use kuberic_runtime::host::{ApplicationStorageState, ReplicaHost, ReplicaProcessConfig};
use kuberic_runtime::protocol::types::{PodUid, PvcUid, ReplicaId, ResourceUid};
use sqlite_replicated::{SqlitePersistence, proto, server::SqliteServer, service::SqliteService};

#[derive(Parser)]
#[command(name = "sqlite-replicated", about = "V2 replicated SQLite")]
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
    pod_ip: IpAddr,
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let config = Config::parse();
    if config.replica_id <= 0 {
        return Err("KUBERIC_REPLICA_ID must be positive".into());
    }
    let application_root = config.data_root.join("application");
    let storage = if SqlitePersistence::is_fresh_empty(&application_root)? {
        ApplicationStorageState::FreshEmpty
    } else {
        ApplicationStorageState::Established
    };
    let application = Arc::new(SqliteService::deferred(
        application_root,
        format!(
            "http://{}",
            SocketAddr::new(config.pod_ip, config.replication_address.port())
        ),
    )?);
    let mut replica = ReplicaHost::new(
        ReplicaProcessConfig {
            resource_uid: ResourceUid::new(&config.resource_uid),
            replica_id: ReplicaId::new(config.replica_id),
            pod_uid: PodUid::new(&config.pod_uid),
            pvc_uid: PvcUid::new(&config.pvc_uid),
            data_root: config.data_root,
            control_address: config.control_address,
            replication_address: config.replication_address,
            bearer_token: config.bearer_token,
            rpc_deadline: Duration::from_secs(5),
            transport_window_capacity: 256,
        },
        application.clone(),
        storage,
        Arc::new(KubernetesDnsResolver::new(
            ResourceUid::new(&config.resource_uid),
            config.namespace,
        )),
    )
    .start()
    .await?;
    let listener = tokio::net::TcpListener::bind(config.application_address).await?;
    let mut shutdown = replica.shutdown_signal();
    let mut client = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(proto::sqlite_store_server::SqliteStoreServer::new(
                SqliteServer::new(application),
            ))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move {
                    while !*shutdown.borrow() {
                        if shutdown.changed().await.is_err() {
                            break;
                        }
                    }
                },
            )
            .await
    });
    tokio::select! {
        result = replica.wait() => result?,
        result = &mut client => result??,
        result = shutdown_signal() => result?,
    }
    replica.shutdown();
    Ok(())
}

async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {}
        }
        Ok(())
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn startup_uses_replica_host_and_has_no_demo_surface() {
        assert!(
            !Config::command()
                .get_arguments()
                .any(|argument| argument.get_id() == "demo")
        );
        let source = include_str!("main.rs")
            .split("#[cfg(test)]")
            .next()
            .unwrap();
        assert!(source.contains("ReplicaHost::new("));
        assert!(source.contains("SqlitePersistence::is_fresh_empty"));
        assert!(source.contains("SqliteService::deferred("));
        assert!(!source.contains("SqlitePersistence::open("));
    }
    use clap::CommandFactory;
}
