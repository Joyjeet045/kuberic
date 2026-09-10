use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use kuberic_core::pod::PodRuntime;
use kuberic_core::types::{CancellationToken, ReplicaInstanceId};
use tokio::sync::Mutex;
use tracing::info;

use sqlite_replicated::state::{SharedState, SqliteState};

#[derive(Parser)]
#[command(
    name = "sqlite-replicated",
    about = "Replicated SQLite database example"
)]
struct Args {
    /// Replica ID for this instance.
    #[arg(long, env = "KUBERIC_REPLICA_ID", default_value = "1")]
    replica_id: i64,

    /// Incarnation ID for this concrete runtime process.
    #[arg(long, env = "KUBERIC_REPLICA_INSTANCE_ID")]
    replica_instance_id: Option<String>,

    /// Bind address for the gRPC control server (operator → pod).
    #[arg(long, env = "KUBERIC_CONTROL_BIND", default_value = "127.0.0.1:0")]
    control_bind: String,

    /// Bind address for the gRPC data server (primary → secondary replication).
    #[arg(long, env = "KUBERIC_DATA_BIND", default_value = "127.0.0.1:0")]
    data_bind: String,

    /// Bind address for the client-facing SQL gRPC server.
    #[arg(long, env = "KUBERIC_CLIENT_BIND", default_value = "127.0.0.1:0")]
    client_bind: String,

    /// Data directory for persistent state.
    #[arg(long, default_value = "/var/lib/sqlite-replicated/data")]
    data_dir: PathBuf,

    /// Run in demo mode: simulates operator + client for quick testing.
    #[arg(long)]
    demo: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    info!("=== SQLite: Replicated SQLite Database ===");

    let mut runtime = PodRuntime::builder(args.replica_id);
    if let Some(instance_id) = args.replica_instance_id {
        runtime = runtime.instance_id(ReplicaInstanceId::new(instance_id));
    }
    let bundle = runtime
        .reply_timeout(Duration::from_secs(10))
        .control_bind(args.control_bind)
        .data_bind(args.data_bind)
        .build()
        .await?;

    let control_address = bundle.control_address.clone();
    let shutdown = bundle.runtime.shutdown_token();
    let state: SharedState = Arc::new(Mutex::new(SqliteState::open(args.data_dir).await?));

    info!(
        control = %control_address,
        client_bind = %args.client_bind,
        "ready (data address will be logged after Open)"
    );

    let runtime_handle = tokio::spawn(bundle.runtime.serve());
    let service_handle = tokio::spawn(sqlite_replicated::service::run_service(
        bundle.lifecycle_rx,
        state.clone(),
        args.client_bind.clone(),
    ));

    if args.demo {
        info!("Demo mode: simulating operator + client");
        sqlite_replicated::demo::simulate_operator(control_address.clone(), args.replica_id).await;
        sqlite_replicated::demo::run_demo_client(args.client_bind).await;
        sqlite_replicated::demo::demo_close(control_address, args.replica_id).await;
    } else {
        info!("Waiting for operator commands on {}", control_address);
        info!("Press Ctrl+C to shut down");
        wait_for_signal(shutdown).await;
    }

    let _ = service_handle.await;
    let _ = runtime_handle.await;

    info!("=== Shutdown ===");
    Ok(())
}

#[cfg(unix)]
async fn wait_for_signal(shutdown: CancellationToken) {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler");
    tokio::select! {
        _ = sigterm.recv() => info!("received SIGTERM"),
        _ = sigint.recv() => info!("received SIGINT"),
    }
    info!("initiating graceful shutdown");
    shutdown.cancel();
}

#[cfg(not(unix))]
async fn wait_for_signal(shutdown: CancellationToken) {
    let _ = tokio::signal::ctrl_c().await;
    info!("received Ctrl-C");
    info!("initiating graceful shutdown");
    shutdown.cancel();
}
