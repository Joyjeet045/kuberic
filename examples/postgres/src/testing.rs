use kuberic_core::types::ReplicaInstanceId;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

/// Find the pg_bin directory from the system.
pub fn find_pg_bin() -> PathBuf {
    let candidates = [
        "/usr/lib/postgresql/16/bin",
        "/usr/lib/postgresql/17/bin",
        "/usr/lib/postgresql/15/bin",
        "/usr/pgsql-16/bin",
        "/usr/local/pgsql/bin",
    ];

    for path in &candidates {
        let p = PathBuf::from(path);
        if p.join("initdb").exists() {
            return p;
        }
    }

    // Fallback: try PATH
    if let Some(bindir) = std::process::Command::new("pg_config")
        .arg("--bindir")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
    {
        return PathBuf::from(bindir);
    }

    panic!("PostgreSQL binaries not found. Install postgresql or set --pg-bin.");
}

/// Create a temporary data directory for testing.
pub fn temp_data_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("kuberic-pg-test-{name}-{}", std::process::id()));
    // Clean up any leftover from previous run
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp data dir");
    dir
}

/// Clean up a test data directory.
pub fn cleanup_data_dir(dir: &std::path::Path) {
    let _ = std::fs::remove_dir_all(dir);
}

/// Allocate a free port by binding to port 0 and reading the OS-assigned port.
/// Same approach as kvstore's data plane port allocation.
pub async fn allocate_port() -> u16 {
    static ALLOCATED_PORTS: OnceLock<Mutex<HashSet<u16>>> = OnceLock::new();

    loop {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind to port 0");
        let port = listener.local_addr().unwrap().port();
        let is_new = ALLOCATED_PORTS
            .get_or_init(|| Mutex::new(HashSet::new()))
            .lock()
            .unwrap()
            .insert(port);
        drop(listener);

        if is_new {
            return port;
        }
    }
}

/// A running PG pod: PodRuntime + PG service event loop + PG instance.
/// Equivalent to KvPod but for PostgreSQL.
pub struct PgPod {
    pub instance_id: ReplicaInstanceId,
    pub control_address: String,
    pub data_address: String,
    pub instance: Arc<crate::instance::PgInstanceManager>,
    pub data_dir: PathBuf,
    _runtime_handle: tokio::task::JoinHandle<()>,
    _service_handle: tokio::task::JoinHandle<()>,
}

impl PgPod {
    /// Start a PG pod with PodRuntime and PG service.
    /// initdb + start PG before creating the runtime.
    pub async fn start(id: i64) -> Self {
        use kuberic_core::pod::PodRuntime;
        use std::time::Duration;
        use tokio::sync::mpsc;

        let pg_bin = find_pg_bin();
        static INSTANCE_COUNTER: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(1);
        let generation = INSTANCE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let instance_id = ReplicaInstanceId::new(format!("postgres-pod-{id}-{generation}"));
        let port = allocate_port().await;
        let data_dir = temp_data_dir(&format!("pod-{id}-{port}"));
        let (fault_tx, _fault_rx) = mpsc::channel(1);

        let instance = Arc::new(crate::instance::PgInstanceManager::new(
            data_dir.clone(),
            pg_bin,
            port,
        ));

        // initdb + start PG before Open (PG must be running for monitoring)
        instance.init_db().await.expect("initdb");
        instance.start(fault_tx).await.expect("start PG");

        // Pre-bind data plane port (PgDataService will bind here in Open)
        let data_port = allocate_port().await;
        let data_bind = format!("127.0.0.1:{data_port}");
        let data_address = format!("http://{data_bind}");
        let control_port = allocate_port().await;
        let control_bind = format!("127.0.0.1:{control_port}");

        let bundle = PodRuntime::builder(id)
            .instance_id(instance_id.clone())
            .reply_timeout(Duration::from_secs(30))
            .control_bind(control_bind)
            .data_bind(data_bind)
            .build()
            .await
            .unwrap();

        let control_address = bundle.control_address.clone();

        let runtime_handle = tokio::spawn(bundle.runtime.serve());
        let inst = instance.clone();
        let service_handle = tokio::spawn(crate::service::run_service(bundle.lifecycle_rx, inst));

        tokio::time::sleep(Duration::from_millis(50)).await;

        Self {
            instance_id,
            control_address,
            data_address,
            instance,
            data_dir,
            _runtime_handle: runtime_handle,
            _service_handle: service_handle,
        }
    }

    /// Create a GrpcReplicaHandle for the operator.
    pub async fn replica_handle(&self, id: i64) -> kuberic_core::grpc::handle::GrpcReplicaHandle {
        kuberic_core::grpc::handle::GrpcReplicaHandle::connect(
            id,
            self.instance_id.clone(),
            self.control_address.clone(),
            self.data_address.clone(),
        )
        .await
        .unwrap()
    }

    /// Connect a tokio-postgres client to this pod's PG instance.
    pub async fn connect_pg(&self) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
        self.instance.connect().await.expect("connect to PG")
    }
}
