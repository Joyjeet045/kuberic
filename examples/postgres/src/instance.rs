use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, mpsc};

use crate::config::PgConfig;

use kuberic_core::types::{CancellationToken, FaultType};

/// Manages a PostgreSQL instance as a child process.
///
/// Wraps pg_ctl, initdb, pg_basebackup, pg_rewind, and the postgres
/// server process. Each instance gets its own data_dir which doubles
/// as the Unix socket directory for isolation in tests.
pub struct PgInstanceManager {
    data_dir: PathBuf,
    pg_bin: PathBuf,
    port: u16,
    child: Arc<Mutex<Option<Child>>>,
    config: PgConfig,
    /// Cancelled by stop() to signal health monitor to exit quietly.
    shutdown: Mutex<CancellationToken>,
}

impl PgInstanceManager {
    pub fn new(data_dir: PathBuf, pg_bin: PathBuf, port: u16) -> Self {
        let config = PgConfig::new(port, &data_dir);
        Self {
            data_dir,
            pg_bin,
            port,
            child: Arc::new(Mutex::new(None)),
            config,
            shutdown: Mutex::new(CancellationToken::new()),
        }
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Host address PG listens on for TCP connections.
    /// For local tests: 127.0.0.1. For K8s: pod IP (configurable).
    pub fn listen_host(&self) -> &str {
        "127.0.0.1"
    }

    /// Socket directory — same as data_dir for test isolation.
    pub fn socket_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Connection string for local UDS access.
    /// Uses the current OS user (initdb creates a superuser matching the OS user).
    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} dbname=postgres",
            self.data_dir.display(),
            self.port,
        )
    }

    /// Initialize a new PG cluster with data checksums.
    pub async fn init_db(&self) -> Result<(), PgError> {
        let output = Command::new(self.pg_bin.join("initdb"))
            .args([
                "--data-checksums",
                "-D",
                &self.data_dir.to_string_lossy(),
                "--auth=trust",
                "--no-instructions",
            ])
            .env("LC_ALL", "C")
            .output()
            .await
            .map_err(|e| PgError::Process(format!("initdb spawn: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(PgError::Process(format!("initdb failed: {stderr}")));
        }

        // Write required config
        self.config.write_initial(&self.data_dir).await?;

        tracing::info!(
            data_dir = %self.data_dir.display(),
            port = self.port,
            "initdb complete"
        );
        Ok(())
    }

    /// Access the PgConfig for post-clone patching.
    pub fn config(&self) -> &PgConfig {
        &self.config
    }

    /// Start PostgreSQL. Pipes stdout/stderr through tracing.
    /// Spawns a child process monitor that reports fault on unexpected exit.
    pub async fn start(&self, fault_tx: mpsc::Sender<FaultType>) -> Result<(), PgError> {
        {
            let guard = self.child.lock().await;
            if guard.is_some() {
                return Ok(()); // already running
            }
        }

        let mut child = Command::new(self.pg_bin.join("postgres"))
            .args(["-D", &self.data_dir.to_string_lossy()])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| PgError::Process(format!("postgres spawn: {e}")))?;

        // Forward stdout + stderr through tracing in one task
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let stderr = BufReader::new(child.stderr.take().unwrap());
        tokio::spawn(async move {
            let mut stdout_lines = stdout.lines();
            let mut stderr_lines = stderr.lines();
            loop {
                tokio::select! {
                    result = stderr_lines.next_line() => {
                        match result {
                            Ok(Some(line)) => tracing::info!(target: "postgres", "{}", line),
                            _ => break,
                        }
                    }
                    result = stdout_lines.next_line() => {
                        match result {
                            Ok(Some(line)) => tracing::debug!(target: "postgres", "{}", line),
                            _ => break,
                        }
                    }
                }
            }
        });

        *self.child.lock().await = Some(child);

        // Fresh shutdown token for this run
        let shutdown = CancellationToken::new();
        *self.shutdown.lock().await = shutdown.clone();

        // Wait for PG to be ready
        self.wait_ready().await?;

        // Spawn process exit monitor — detects PG crash immediately via
        // try_wait() and clears the child field (prevents stale state in
        // start()). Also reports FaultType::Permanent unless shutdown was
        // intentional (KP-9 fix).
        {
            let child_mutex = self.child.clone();
            let exit_shutdown = shutdown.clone();
            let exit_ft = fault_tx.clone();
            let exit_port = self.port;
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = exit_shutdown.cancelled() => break,
                        _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {}
                    }
                    let mut guard = child_mutex.lock().await;
                    if let Some(child) = guard.as_mut() {
                        match child.try_wait() {
                            Ok(Some(status)) => {
                                // PG exited — reap immediately
                                guard.take();
                                drop(guard);
                                if !exit_shutdown.is_cancelled() {
                                    tracing::error!(
                                        port = exit_port,
                                        ?status,
                                        "PostgreSQL exited unexpectedly"
                                    );
                                    let _ = exit_ft.send(FaultType::Permanent).await;
                                }
                                break;
                            }
                            Ok(None) => {} // still running
                            Err(e) => {
                                tracing::warn!("try_wait error: {}", e);
                            }
                        }
                    } else {
                        // child was taken by stop() — exit quietly
                        break;
                    }
                }
            });
        }

        // Spawn pg_isready health monitor — catches PG hung but process alive
        // (complements the exit monitor above). Exits quietly on shutdown.
        let port = self.port;
        let ft = fault_tx;
        let data_dir = self.data_dir.clone();
        let pg_bin = self.pg_bin.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut consecutive_failures: u32 = 0;
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
                }
                let status = Command::new(pg_bin.join("pg_isready"))
                    .args([
                        "-h",
                        &data_dir.to_string_lossy(),
                        "-p",
                        &port.to_string(),
                        "-d",
                        "postgres",
                    ])
                    .output()
                    .await;
                match status {
                    Ok(output) if output.status.success() => {
                        consecutive_failures = 0;
                    }
                    _ => {
                        consecutive_failures += 1;
                        if consecutive_failures >= 3 {
                            if !shutdown.is_cancelled() {
                                tracing::error!(
                                    port,
                                    "PostgreSQL unresponsive (3 consecutive pg_isready failures)"
                                );
                                let _ = ft.send(FaultType::Permanent).await;
                            }
                            break;
                        }
                    }
                }
            }
        });

        tracing::info!(port = self.port, "PostgreSQL started");
        Ok(())
    }

    /// Wait for PG to accept connections.
    async fn wait_ready(&self) -> Result<(), PgError> {
        for i in 0..60 {
            let output = Command::new(self.pg_bin.join("pg_isready"))
                .args([
                    "-h",
                    &self.data_dir.to_string_lossy(),
                    "-p",
                    &self.port.to_string(),
                    "-d",
                    "postgres",
                ])
                .output()
                .await
                .map_err(|e| PgError::Process(format!("pg_isready: {e}")))?;

            if output.status.success() {
                tracing::debug!(attempt = i, "pg_isready: accepting connections");
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        Err(PgError::Process("pg_isready timeout (15s)".into()))
    }

    /// Stop PostgreSQL (fast mode).
    pub async fn stop(&self) -> Result<(), PgError> {
        // Signal health monitor to exit quietly before stopping PG
        self.shutdown.lock().await.cancel();

        let output = Command::new(self.pg_bin.join("pg_ctl"))
            .args([
                "stop",
                "-D",
                &self.data_dir.to_string_lossy(),
                "-m",
                "fast",
                "-w",
            ])
            .output()
            .await
            .map_err(|e| PgError::Process(format!("pg_ctl stop: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Not running is OK
            if !stderr.contains("not running") {
                return Err(PgError::Process(format!("pg_ctl stop failed: {stderr}")));
            }
        }

        // Reap child process if we have one
        if let Some(mut child) = self.child.lock().await.take() {
            let _ = child.wait().await;
        }

        tracing::info!(port = self.port, "PostgreSQL stopped");
        Ok(())
    }

    /// Promote standby to primary.
    pub async fn promote(&self) -> Result<(), PgError> {
        let output = Command::new(self.pg_bin.join("pg_ctl"))
            .args([
                "promote",
                "-D",
                &self.data_dir.to_string_lossy(),
                "-w",
                "-t",
                "60",
            ])
            .output()
            .await
            .map_err(|e| PgError::Process(format!("pg_ctl promote: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(PgError::Process(format!("pg_ctl promote failed: {stderr}")));
        }

        tracing::info!(port = self.port, "PostgreSQL promoted to primary");
        Ok(())
    }

    /// Run pg_basebackup from a source to initialize this replica.
    pub async fn base_backup(&self, source_host: &str, source_port: u16) -> Result<(), PgError> {
        let output = Command::new(self.pg_bin.join("pg_basebackup"))
            .args([
                "-D",
                &self.data_dir.to_string_lossy(),
                "-h",
                source_host,
                "-p",
                &source_port.to_string(),
                "-U",
                &whoami::username().unwrap_or_else(|_| "postgres".to_string()),
                "-Fp",
                "-Xs",
                "-R", // creates standby.signal + primary_conninfo
            ])
            .output()
            .await
            .map_err(|e| PgError::Process(format!("pg_basebackup: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(PgError::Process(format!("pg_basebackup failed: {stderr}")));
        }

        tracing::info!(
            port = self.port,
            source = format!("{source_host}:{source_port}"),
            "pg_basebackup complete"
        );
        Ok(())
    }

    /// Run pg_rewind to rejoin as standby after demotion.
    pub async fn rewind(&self, source_host: &str, source_port: u16) -> Result<(), PgError> {
        let source_conn = format!("host={source_host} port={source_port} dbname=postgres");

        let output = Command::new(self.pg_bin.join("pg_rewind"))
            .args([
                "--target-pgdata",
                &self.data_dir.to_string_lossy(),
                "--source-server",
                &source_conn,
            ])
            .output()
            .await
            .map_err(|e| PgError::Process(format!("pg_rewind: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(PgError::Process(format!("pg_rewind failed: {stderr}")));
        }

        tracing::info!(port = self.port, "pg_rewind complete");
        Ok(())
    }

    /// Connect to the local PG instance via UDS.
    pub async fn connect(
        &self,
    ) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>), PgError> {
        let (client, connection) =
            tokio_postgres::connect(&self.connection_string(), tokio_postgres::NoTls)
                .await
                .map_err(|e| {
                    PgError::Connection(format!("connect to {}: {e}", self.connection_string()))
                })?;

        let handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                // Expected during shutdown — PG kills connections on pg_ctl stop
                tracing::debug!("PG connection closed: {}", e);
            }
        });

        Ok((client, handle))
    }
}

impl Drop for PgInstanceManager {
    fn drop(&mut self) {
        // Cancel health monitor + process exit monitor
        self.shutdown.get_mut().cancel();
        // Best-effort stop: check if child exists via try_lock
        if let Ok(guard) = self.child.try_lock() {
            if guard.is_some() {
                let _ = std::process::Command::new(self.pg_bin.join("pg_ctl"))
                    .args([
                        "stop",
                        "-D",
                        &self.data_dir.to_string_lossy(),
                        "-m",
                        "immediate",
                    ])
                    .output();
            }
        } else {
            // Lock held by exit monitor — still try to stop PG
            let _ = std::process::Command::new(self.pg_bin.join("pg_ctl"))
                .args([
                    "stop",
                    "-D",
                    &self.data_dir.to_string_lossy(),
                    "-m",
                    "immediate",
                ])
                .output();
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PgError {
    #[error("process error: {0}")]
    Process(String),
    #[error("connection error: {0}")]
    Connection(String),
    #[error("query error: {0}")]
    Query(String),
}
