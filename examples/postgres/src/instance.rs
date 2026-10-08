use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as ProcessMutex, Weak};

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{Mutex, mpsc};

use crate::config::PgConfig;
use crate::generation::GenerationClock;
use crate::owned_command::{CapturedCommands, OwnedCommands};
use crate::owned_process::OwnedProcess;
use kuberic_runtime::replicator::StatefulServicePartition;

use tokio_util::sync::CancellationToken;

fn increment_generation(generation: &AtomicU64) -> Option<u64> {
    let mut current = generation.load(Ordering::Acquire);
    loop {
        let next = current.checked_add(1)?;
        match generation.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Some(next),
            Err(observed) => current = observed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgProcessFault {
    Transient,
    Permanent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgProcessState {
    Stopped,
    Initialized,
    Starting,
    Running,
    Stopping,
    Faulted,
}

#[derive(Clone)]
struct StartParameters {
    fault_tx: mpsc::Sender<kuberic_runtime::protocol::types::FaultType>,
    cancellation: Option<(Weak<PgInstanceManager>, CancellationToken)>,
}

pub(crate) struct GenerationFault {
    generation: u64,
    fault: kuberic_runtime::protocol::types::FaultType,
}

#[derive(Clone)]
pub(crate) struct GenerationLease(Arc<ProcessGeneration>);

impl GenerationLease {
    pub(crate) fn id(&self) -> u64 {
        self.0.id
    }
}

struct ProcessGeneration {
    id: u64,
    child: ProcessMutex<Option<OwnedProcess>>,
    helpers: Arc<OwnedCommands>,
    cleanup_lock: ProcessMutex<()>,
    cleanup_result: ProcessMutex<Option<Result<(), PgError>>>,
    state: ProcessMutex<PgProcessState>,
    shutdown: CancellationToken,
    monitors: ProcessMutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl ProcessGeneration {
    fn new(id: u64) -> Self {
        Self {
            id,
            child: ProcessMutex::new(None),
            helpers: Arc::new(OwnedCommands::default()),
            cleanup_lock: ProcessMutex::new(()),
            cleanup_result: ProcessMutex::new(None),
            state: ProcessMutex::new(PgProcessState::Stopped),
            shutdown: CancellationToken::new(),
            monitors: ProcessMutex::new(Vec::new()),
        }
    }

    fn running(&self) -> bool {
        self.child
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|child| !child.exited().unwrap_or(false))
    }

    fn retire(&self, commands: &CapturedCommands, data_dir: &Path) -> Result<(), PgError> {
        let _cleanup = self.cleanup_lock.lock().unwrap();
        tracing::debug!(
            generation = self.id,
            "retiring captured PostgreSQL generation"
        );
        let retained = self.cleanup_result.lock().unwrap().clone();
        if matches!(retained, Some(Ok(()))) {
            return Ok(());
        }
        self.shutdown.cancel();
        for task in self.monitors.lock().unwrap().drain(..) {
            task.abort();
        }
        let helpers = commands.terminate();
        let postgres = self
            .child
            .lock()
            .unwrap()
            .as_mut()
            .map_or(Ok(()), |child| child.stop(data_dir));
        if postgres.is_ok() {
            self.child.lock().unwrap().take();
        }
        let result = match helpers {
            Ok(()) => postgres,
            Err(error) => Err(error.with_cleanup(postgres)),
        };
        let result = match retained {
            Some(Err(error)) => Err(error),
            _ => result,
        };
        *self.state.lock().unwrap() = if result.is_ok() {
            PgProcessState::Stopped
        } else {
            PgProcessState::Faulted
        };
        *self.cleanup_result.lock().unwrap() = Some(result.clone());
        result
    }
}

struct CleanupTicket {
    phase: AtomicU8,
    generation: Arc<ProcessGeneration>,
    commands: CapturedCommands,
    previous: PgProcessState,
    launches_closed: Arc<AtomicBool>,
}

struct CleanupWaiter(Arc<CleanupTicket>);

struct Lifecycle<'a> {
    instance: &'a PgInstanceManager,
    _guard: tokio::sync::MutexGuard<'a, ()>,
}

impl Drop for Lifecycle<'_> {
    fn drop(&mut self) {
        if let Err(error) = publish_stopped_generation(
            &self.instance.current,
            &self.instance.next_generation,
            &self.instance.launches_closed,
            None,
        ) {
            tracing::error!(%error, "process generation publication failed closed");
        }
    }
}

fn publish_stopped_generation(
    current: &ProcessMutex<Arc<ProcessGeneration>>,
    next: &GenerationClock,
    closed: &AtomicBool,
    expected: Option<&Arc<ProcessGeneration>>,
) -> Result<(), PgError> {
    let mut current = current.lock().unwrap();
    if expected.is_some_and(|expected| !Arc::ptr_eq(&current, expected)) {
        return Ok(());
    }
    let retired = matches!(*current.cleanup_result.lock().unwrap(), Some(Ok(())))
        && *current.state.lock().unwrap() == PgProcessState::Stopped;
    if retired && !closed.load(Ordering::Acquire) && !next.is_retired() {
        match next.allocate() {
            Ok(id) => *current = Arc::new(ProcessGeneration::new(id)),
            Err(error) => {
                closed.store(true, Ordering::Release);
                *current.state.lock().unwrap() = PgProcessState::Faulted;
                *current.cleanup_result.lock().unwrap() = Some(Err(error.clone()));
                return Err(error.with_generation(current.id));
            }
        }
    }
    Ok(())
}

impl Drop for CleanupWaiter {
    fn drop(&mut self) {
        if self
            .0
            .phase
            .compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.0.commands.cancel_unstarted();
            if !self.0.launches_closed.load(Ordering::Acquire)
                && self.0.generation.cleanup_result.lock().unwrap().is_none()
            {
                *self.0.generation.state.lock().unwrap() = self.0.previous;
            }
        }
    }
}

/// Manages a PostgreSQL instance as a child process.
///
/// Wraps pg_ctl, initdb, pg_basebackup, pg_rewind, and the postgres
/// server process. Each instance keeps its Unix sockets in PGDATA/pg_stat_tmp,
/// a transient directory excluded by PostgreSQL backup and rewind.
pub struct PgInstanceManager {
    data_dir: PathBuf,
    pg_bin: PathBuf,
    port: u16,
    current: Arc<ProcessMutex<Arc<ProcessGeneration>>>,
    next_generation: Arc<GenerationClock>,
    launches_closed: Arc<AtomicBool>,
    abort_generation: AtomicU64,
    config: PgConfig,
    lifecycle_lock: Arc<Mutex<()>>,
    fault_sink: ProcessMutex<Option<mpsc::Sender<GenerationFault>>>,
    start_parameters: Mutex<Option<StartParameters>>,
    pub(crate) access_lock: Mutex<()>,
    pub(crate) access_state: AtomicU8,
    access_generation: AtomicU64,
    #[cfg(feature = "testing")]
    cleanup_hook: ProcessMutex<Option<CleanupHook>>,
    #[cfg(feature = "testing")]
    error_hook: ProcessMutex<Option<Arc<ErrorGate>>>,
    #[cfg(feature = "testing")]
    fault_hook: ProcessMutex<Option<Arc<ErrorGate>>>,
    #[cfg(test)]
    pub(crate) clear_pgdata_hook: ProcessMutex<Option<(String, PathBuf)>>,
}

#[cfg(feature = "testing")]
pub struct CleanupGate {
    pub entered: Arc<tokio::sync::Notify>,
    pub finished: Arc<tokio::sync::Notify>,
    release: std::sync::mpsc::Sender<()>,
}

#[cfg(feature = "testing")]
pub struct ErrorGate {
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}

#[cfg(feature = "testing")]
impl CleanupGate {
    pub fn release(&self) {
        let _ = self.release.send(());
    }
}

#[cfg(feature = "testing")]
struct CleanupHook {
    entered: Arc<tokio::sync::Notify>,
    finished: Arc<tokio::sync::Notify>,
    release: std::sync::mpsc::Receiver<()>,
}

impl PgInstanceManager {
    #[cfg(feature = "testing")]
    pub fn pause_error_handling(&self) -> Arc<ErrorGate> {
        let gate = Arc::new(ErrorGate {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        *self.error_hook.lock().unwrap() = Some(gate.clone());
        gate
    }

    #[cfg(feature = "testing")]
    pub fn pause_fault_delivery(&self) -> Arc<ErrorGate> {
        let gate = Arc::new(ErrorGate {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        *self.fault_hook.lock().unwrap() = Some(gate.clone());
        gate
    }

    #[cfg(feature = "testing")]
    pub fn pause_cleanup(&self) -> CleanupGate {
        let entered = Arc::new(tokio::sync::Notify::new());
        let finished = Arc::new(tokio::sync::Notify::new());
        let (release, receiver) = std::sync::mpsc::channel();
        *self.cleanup_hook.lock().unwrap() = Some(CleanupHook {
            entered: entered.clone(),
            finished: finished.clone(),
            release: receiver,
        });
        CleanupGate {
            entered,
            finished,
            release,
        }
    }

    pub fn new(data_dir: PathBuf, pg_bin: PathBuf, port: u16) -> Self {
        let config = PgConfig::new(port, &data_dir);
        Self {
            data_dir,
            pg_bin,
            port,
            current: Arc::new(ProcessMutex::new(Arc::new(ProcessGeneration::new(0)))),
            next_generation: Arc::new(GenerationClock::default()),
            launches_closed: Arc::new(AtomicBool::new(false)),
            abort_generation: AtomicU64::new(0),
            config,
            lifecycle_lock: Arc::new(Mutex::new(())),
            fault_sink: ProcessMutex::new(None),
            start_parameters: Mutex::new(None),
            access_lock: Mutex::new(()),
            access_state: AtomicU8::new(crate::access::CLOSED),
            access_generation: AtomicU64::new(0),
            #[cfg(feature = "testing")]
            cleanup_hook: ProcessMutex::new(None),
            #[cfg(feature = "testing")]
            error_hook: ProcessMutex::new(None),
            #[cfg(feature = "testing")]
            fault_hook: ProcessMutex::new(None),
            #[cfg(test)]
            clear_pgdata_hook: ProcessMutex::new(None),
        }
    }

    fn generation(&self) -> Arc<ProcessGeneration> {
        self.current.lock().unwrap().clone()
    }

    async fn lifecycle(&self) -> Lifecycle<'_> {
        Lifecycle {
            instance: self,
            _guard: self.lifecycle_lock.lock().await,
        }
    }

    pub(crate) fn bind_fault_sink(&self, sink: mpsc::Sender<GenerationFault>) {
        *self.fault_sink.lock().unwrap() = Some(sink);
    }

    pub(crate) async fn deliver_fault(
        &self,
        notice: GenerationFault,
        partition: &StatefulServicePartition,
    ) {
        #[cfg(feature = "testing")]
        {
            let gate = self.fault_hook.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
        }
        let _lifecycle = self.lifecycle().await;
        let generation = self.generation();
        if generation.id == notice.generation
            && !matches!(
                *generation.state.lock().unwrap(),
                PgProcessState::Stopping | PgProcessState::Stopped
            )
        {
            let _ = partition.report_fault(notice.fault).await;
        }
    }

    pub(crate) fn generation_id(&self) -> u64 {
        self.generation().id
    }

    pub(crate) fn access_generation(&self) -> u64 {
        self.access_generation.load(Ordering::Acquire)
    }

    pub(crate) fn advance_access_generation(&self) -> Result<u64, PgError> {
        increment_generation(&self.access_generation)
            .ok_or_else(|| PgError::GenerationExhausted("access generation".into()))
    }

    pub(crate) async fn bind_generation_store(
        &self,
        store: &Arc<crate::durable::PgDurableStore>,
    ) -> Result<(), PgError> {
        let _lifecycle = self.lifecycle().await;
        self.next_generation.bind(store).await?;
        let id = self.next_generation.allocate()?;
        *self.current.lock().unwrap() = Arc::new(ProcessGeneration::new(id));
        Ok(())
    }

    #[cfg(feature = "testing")]
    pub fn advance_generation_for_test(&self, value: u64) -> Result<(), PgError> {
        self.next_generation.advance_for_test(value)
    }

    #[cfg(feature = "testing")]
    pub fn process_generation_for_test(&self) -> u64 {
        self.generation_id()
    }

    pub(crate) fn generation_lease(&self) -> GenerationLease {
        GenerationLease(self.generation())
    }

    pub(crate) fn generation_is_current(&self, lease: &GenerationLease) -> bool {
        Arc::ptr_eq(&self.generation(), &lease.0)
            && lease.0.cleanup_result.lock().unwrap().is_none()
            && *lease.0.state.lock().unwrap() != PgProcessState::Stopping
    }

    pub(crate) async fn generation_step<T>(
        &self,
        lease: &GenerationLease,
        operation: impl Future<Output = Result<T, PgError>>,
    ) -> Result<T, PgError> {
        let _lifecycle = self.lifecycle().await;
        if !self.generation_is_current(lease) {
            return Err(
                PgError::Process("operation generation retired before mutation".into())
                    .with_generation(lease.id()),
            );
        }
        self.complete_generation(&lease.0, operation.await)
    }

    pub(crate) async fn generation_operation<T>(
        &self,
        operation: impl Future<Output = Result<T, PgError>>,
    ) -> Result<T, PgError> {
        let generation = self.generation();
        self.complete_generation(&generation, operation.await)
    }

    fn complete_generation<T>(
        &self,
        generation: &Arc<ProcessGeneration>,
        result: Result<T, PgError>,
    ) -> Result<T, PgError> {
        let result = result.and_then(|value| {
            if !Arc::ptr_eq(&self.generation(), generation)
                || generation.cleanup_result.lock().unwrap().is_some()
            {
                Err(PgError::Process(
                    "operation completed after its generation retired".into(),
                ))
            } else {
                Ok(value)
            }
        });
        result.map_err(|error| error.with_generation(generation.id))
    }

    async fn command_output(
        &self,
        generation: &Arc<ProcessGeneration>,
        command: &mut Command,
        deadline: Option<(std::time::Duration, &'static str)>,
    ) -> Result<std::process::Output, PgError> {
        let result = match deadline {
            Some((duration, name)) => {
                generation
                    .helpers
                    .output_with_timeout(command, duration, name)
                    .await
            }
            None => generation.helpers.output(command).await,
        };
        self.complete_generation(generation, result)
    }

    pub(crate) async fn handle_error<F, Fut>(
        &self,
        error: PgError,
        access_generation: Option<u64>,
        report: F,
    ) -> kuberic_runtime::RuntimeError
    where
        F: Fn(kuberic_runtime::protocol::types::FaultType, PgError) -> Fut,
        Fut: Future<Output = kuberic_runtime::RuntimeError>,
    {
        #[cfg(feature = "testing")]
        {
            let gate = self.error_hook.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
        }
        if access_generation.is_some_and(|generation| generation != self.access_generation()) {
            return kuberic_runtime::RuntimeError::OperationCancelled;
        }
        let _lifecycle = self.lifecycle().await;
        let generation = self.generation();
        let state = *generation.state.lock().unwrap();
        let retired = state == PgProcessState::Stopping
            || (state == PgProcessState::Stopped && generation.shutdown.is_cancelled());
        if error.origin().is_some_and(|origin| origin != generation.id)
            || (error.origin().is_some() && retired)
        {
            tracing::debug!(%error, current_generation = generation.id, "ignoring retired operation fault");
            return kuberic_runtime::RuntimeError::OperationCancelled;
        }
        let transient = error.is_timeout();
        let cleanup = if transient {
            self.retire_generation(generation.clone(), false).await
        } else {
            let Some(result) = self.abort_generation(&generation) else {
                tracing::debug!(%error, "ignoring replaced operation fault");
                return kuberic_runtime::RuntimeError::OperationCancelled;
            };
            result
        };
        if !Arc::ptr_eq(&self.generation(), &generation) {
            tracing::debug!(%error, "ignoring replaced operation fault");
            return kuberic_runtime::RuntimeError::OperationCancelled;
        }
        let error = error.with_cleanup(cleanup);
        let recovered = transient && error.is_timeout();
        let result = report(error.fault_type(), error).await;
        if recovered {
            if let Err(error) = publish_stopped_generation(
                &self.current,
                &self.next_generation,
                &self.launches_closed,
                Some(&generation),
            ) {
                return report(error.fault_type(), error).await;
            }
            tracing::warn!(%result, generation = generation.id, "failed operation retired; successor cleanup is not authorized");
            return kuberic_runtime::RuntimeError::OperationCancelled;
        }
        result
    }

    fn replace_retired(&self, expected_abort: u64) -> Result<Arc<ProcessGeneration>, PgError> {
        let mut current = self.current.lock().unwrap();
        if expected_abort == u64::MAX {
            return Err(PgError::GenerationExhausted("process abort epoch".into()));
        }
        if self.abort_generation.load(Ordering::Acquire) != expected_abort {
            return Err(PgError::Process(
                "owned PostgreSQL startup was aborted".into(),
            ));
        }
        let id = self.next_generation.allocate().map_err(|error| {
            self.launches_closed.store(true, Ordering::Release);
            *current.state.lock().unwrap() = PgProcessState::Faulted;
            *current.cleanup_result.lock().unwrap() = Some(Err(error.clone()));
            error.with_generation(current.id)
        })?;
        let generation = Arc::new(ProcessGeneration::new(id));
        *current = generation.clone();
        self.launches_closed.store(false, Ordering::Release);
        Ok(generation)
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

    /// Transient socket directory within the owned data directory.
    pub fn socket_dir(&self) -> &Path {
        Path::new(&self.config.socket_dir)
    }

    /// Connection string for local UDS access.
    /// Uses the current OS user (initdb creates a superuser matching the OS user).
    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} dbname=postgres",
            self.socket_dir().display(),
            self.port,
        )
    }

    pub fn application_connection_string(&self) -> String {
        format!(
            "host={} port={} dbname={} user={}",
            self.socket_dir().display(),
            self.port,
            crate::access::APPLICATION_DATABASE,
            crate::access::APPLICATION_ROLE,
        )
    }

    /// Initialize a new PG cluster with data checksums.
    pub async fn init_db(&self) -> Result<(), PgError> {
        let _lifecycle = self.lifecycle().await;
        let generation = self.generation();
        let output = self
            .command_output(
                &generation,
                Command::new(self.pg_bin.join("initdb"))
                    .args([
                        "--data-checksums",
                        "-D",
                        &self.data_dir.to_string_lossy(),
                        "--auth=trust",
                        "--no-instructions",
                    ])
                    .env("LC_ALL", "C"),
                None,
            )
            .await?;

        if !output.status.success() {
            return Err(
                PgError::command_failed("initdb failed", &output).with_generation(generation.id)
            );
        }

        // Write required config
        self.config
            .write_initial(&self.data_dir)
            .await
            .map_err(|error| error.with_generation(generation.id))?;
        *generation.state.lock().unwrap() = PgProcessState::Initialized;

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

    pub async fn start_native(
        &self,
        fault_tx: mpsc::Sender<kuberic_runtime::protocol::types::FaultType>,
    ) -> Result<(), PgError> {
        self.start_with_parameters(StartParameters {
            fault_tx,
            cancellation: None,
        })
        .await
    }

    fn fault_reporter(
        &self,
        fault_tx: mpsc::Sender<kuberic_runtime::protocol::types::FaultType>,
        generation: &Arc<ProcessGeneration>,
    ) -> Arc<dyn Fn(PgProcessFault) + Send + Sync> {
        let current = Arc::downgrade(&self.current);
        let sink = self.fault_sink.lock().unwrap().clone();
        let generation = Arc::downgrade(generation);
        Arc::new(move |fault| {
            let fault = match fault {
                PgProcessFault::Transient => kuberic_runtime::protocol::types::FaultType::Transient,
                PgProcessFault::Permanent => kuberic_runtime::protocol::types::FaultType::Permanent,
            };
            if let (Some(current), Some(generation)) = (current.upgrade(), generation.upgrade())
                && Arc::ptr_eq(&current.lock().unwrap(), &generation)
            {
                if let Some(sink) = &sink {
                    let notice = GenerationFault {
                        generation: generation.id,
                        fault,
                    };
                    match sink.try_send(notice) {
                        Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
                        Err(mpsc::error::TrySendError::Full(notice)) => {
                            let sink = sink.clone();
                            tokio::spawn(async move {
                                let _ = sink.send(notice).await;
                            });
                        }
                    }
                    return;
                }
                match fault_tx.try_send(fault) {
                    Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
                    Err(mpsc::error::TrySendError::Full(fault)) => {
                        let fault_tx = fault_tx.clone();
                        tokio::spawn(async move {
                            if let Ok(permit) = fault_tx.reserve().await
                                && Arc::ptr_eq(&current.lock().unwrap(), &generation)
                            {
                                permit.send(fault);
                            }
                        });
                    }
                }
            }
        })
    }

    pub async fn start_native_with_cancellation(
        self: &Arc<Self>,
        fault_tx: mpsc::Sender<kuberic_runtime::protocol::types::FaultType>,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<(), PgError> {
        self.start_with_parameters(StartParameters {
            fault_tx,
            cancellation: Some((Arc::downgrade(self), cancellation)),
        })
        .await
    }

    pub(crate) async fn restart_access_closed(&self) -> Result<(), PgError> {
        let parameters = self
            .start_parameters
            .lock()
            .await
            .clone()
            .ok_or_else(|| PgError::Process("no owned PostgreSQL run to fence".into()))?;
        self.stop().await?;
        self.start_with_parameters(parameters).await
    }

    pub async fn control_identity(&self) -> Result<(String, u32), PgError> {
        let generation = self.generation();
        let result = async {
            let output = self
                .command_output(
                    &generation,
                    Command::new(self.pg_bin.join("pg_controldata"))
                        .arg(&self.data_dir)
                        .env("LC_ALL", "C"),
                    Some((std::time::Duration::from_secs(2), "pg_controldata")),
                )
                .await?;
            if !output.status.success() {
                return Err(PgError::command_failed(
                    "cannot inspect PostgreSQL control data",
                    &output,
                ));
            }
            let text = String::from_utf8_lossy(&output.stdout);
            let field = |name: &str| {
                text.lines()
                    .find_map(|line| line.strip_prefix(name))
                    .map(str::trim)
                    .ok_or_else(|| PgError::Process(format!("missing control field {name}")))
            };
            let system = field("Database system identifier:")?.to_owned();
            let timeline: u32 = field("Latest checkpoint's TimeLineID:")?
                .parse()
                .map_err(|_| PgError::Process("invalid control timeline".into()))?;
            let recovery_timeline: u32 = field("Min recovery ending loc's timeline:")?
                .parse()
                .map_err(|_| PgError::Process("invalid recovery control timeline".into()))?;
            let timeline = timeline.max(recovery_timeline);
            Ok((system, timeline))
        }
        .await;
        self.complete_generation(&generation, result)
    }

    pub(crate) async fn retained_wal_boundary(&self) -> Result<i64, PgError> {
        let generation = self.generation();
        let result = async {
            let output = self
                .command_output(
                    &generation,
                    Command::new(self.pg_bin.join("pg_controldata"))
                        .arg(&self.data_dir)
                        .env("LC_ALL", "C"),
                    Some((std::time::Duration::from_secs(2), "pg_controldata")),
                )
                .await?;
            if !output.status.success() {
                return Err(PgError::command_failed(
                    "cannot inspect WAL segment size",
                    &output,
                ));
            }
            let text = String::from_utf8_lossy(&output.stdout);
            let size: u64 = text
                .lines()
                .find_map(|line| line.strip_prefix("Bytes per WAL segment:"))
                .and_then(|value| value.trim().parse().ok())
                .filter(|size: &u64| {
                    size.is_power_of_two() && *size >= 1024 * 1024 && *size <= 1024 * 1024 * 1024
                })
                .ok_or_else(|| PgError::Process("invalid WAL segment size".into()))?;
            let mut entries = tokio::fs::read_dir(self.data_dir.join("pg_wal"))
                .await
                .map_err(|error| PgError::Process(format!("inspect retained WAL: {error}")))?;
            let mut oldest = None;
            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|error| PgError::Process(format!("inspect WAL entry: {error}")))?
            {
                let name = entry.file_name();
                let Some(name) = name.to_str() else { continue };
                let name = name.strip_suffix(".partial").unwrap_or(name);
                if name.len() != 24 || !name.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    continue;
                }
                let log = u64::from_str_radix(&name[8..16], 16)
                    .map_err(|error| PgError::Process(error.to_string()))?;
                let segment = u64::from_str_radix(&name[16..24], 16)
                    .map_err(|error| PgError::Process(error.to_string()))?;
                if segment >= (1_u64 << 32) / size {
                    return Err(PgError::Process("invalid retained WAL segment".into()));
                }
                let boundary = log
                    .checked_mul(1_u64 << 32)
                    .and_then(|log| {
                        segment
                            .checked_mul(size)
                            .and_then(|segment| log.checked_add(segment))
                    })
                    .and_then(|position| i64::try_from(position).ok())
                    .ok_or_else(|| PgError::Process("retained WAL position overflow".into()))?;
                oldest = Some(oldest.map_or(boundary, |old: i64| old.min(boundary)));
            }
            oldest.ok_or_else(|| PgError::Process("no retained PostgreSQL WAL".into()))
        }
        .await;
        self.complete_generation(&generation, result)
    }

    async fn start_with_parameters(&self, parameters: StartParameters) -> Result<(), PgError> {
        let _lifecycle = self.lifecycle().await;
        let expected_abort = self.abort_generation.load(Ordering::Acquire);
        if parameters
            .cancellation
            .as_ref()
            .is_some_and(|(_, token)| token.is_cancelled())
        {
            return Err(PgError::Process("PostgreSQL run was cancelled".into()));
        }
        let existing = self.generation();
        if let Some(Err(error)) = existing.cleanup_result.lock().unwrap().clone() {
            return Err(error.with_generation(existing.id));
        }
        if existing.running() && *existing.state.lock().unwrap() == PgProcessState::Running {
            return Ok(());
        }
        *self.start_parameters.lock().await = Some(parameters.clone());
        self.finish_owned().await?;
        let generation = self.replace_retired(expected_abort)?;
        let fault_reporter = self.fault_reporter(parameters.fault_tx.clone(), &generation);
        *generation.state.lock().unwrap() = PgProcessState::Starting;
        let prepared = async {
            OwnedProcess::check_support()?;
            self.control_identity().await?;
            self.config.write_initial(&self.data_dir).await?;
            self.access_state
                .store(crate::access::CLOSED, Ordering::Release);
            Ok::<_, PgError>(())
        }
        .await;
        if let Err(error) = prepared {
            fault_reporter(match error.fault_type() {
                kuberic_runtime::protocol::types::FaultType::Transient => PgProcessFault::Transient,
                kuberic_runtime::protocol::types::FaultType::Permanent => PgProcessFault::Permanent,
            });
            *generation.state.lock().unwrap() = PgProcessState::Faulted;
            return Err(error.with_generation(generation.id));
        }

        let (stdout, stderr) = match self.spawn_owned(&generation) {
            Ok(streams) => streams,
            Err(error) => {
                fault_reporter(PgProcessFault::Permanent);
                *generation.state.lock().unwrap() = PgProcessState::Faulted;
                return Err(error.with_generation(generation.id));
            }
        };

        // Forward stdout + stderr through tracing in one task
        let stdout = BufReader::new(stdout);
        let stderr = BufReader::new(stderr);
        let log_task = tokio::spawn(async move {
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
        generation.monitors.lock().unwrap().push(log_task);

        // Fresh shutdown token for this run
        let shutdown = generation.shutdown.clone();

        // Wait for PG to be ready
        if let Err(error) = self.wait_ready(&generation).await {
            fault_reporter(PgProcessFault::Permanent);
            let cleanup = self.retire_owned(false).await;
            *generation.state.lock().unwrap() = PgProcessState::Faulted;
            return Err(error.with_cleanup(cleanup).with_generation(generation.id));
        }

        // Observe without reaping: shutdown retains ownership of any descendants.
        {
            let observed = generation.clone();
            let exit_shutdown = shutdown.clone();
            let exit_fault_reporter = fault_reporter.clone();
            let exit_port = self.port;
            let exit_task = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = exit_shutdown.cancelled() => break,
                        _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {}
                    }
                    let exited = match observed.child.try_lock() {
                        Ok(guard) => guard.as_ref().map(OwnedProcess::exited),
                        Err(_) => continue,
                    };
                    if let Some(exited) = exited {
                        match exited {
                            Ok(true) => {
                                if !exit_shutdown.is_cancelled() {
                                    tracing::error!(
                                        port = exit_port,
                                        "PostgreSQL exited unexpectedly"
                                    );
                                    exit_fault_reporter(PgProcessFault::Permanent);
                                    *observed.state.lock().unwrap() = PgProcessState::Faulted;
                                }
                                break;
                            }
                            Ok(false) => {}
                            Err(e) => {
                                tracing::warn!("process observation error: {}", e);
                            }
                        }
                    } else {
                        break;
                    }
                }
            });
            generation.monitors.lock().unwrap().push(exit_task);
        }

        // Spawn pg_isready health monitor — catches PG hung but process alive
        // (complements the exit monitor above). Exits quietly on shutdown.
        let port = self.port;
        let health_fault_reporter = fault_reporter;
        let data_dir = self.socket_dir().to_path_buf();
        let pg_bin = self.pg_bin.clone();
        let shutdown = shutdown.clone();
        let health_generation = generation.clone();
        let helpers = generation.helpers.clone();
        let health_task = tokio::spawn(async move {
            let mut consecutive_failures: u32 = 0;
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
                }
                if *health_generation.state.lock().unwrap() != PgProcessState::Running {
                    continue;
                }
                let status = helpers
                    .output(Command::new(pg_bin.join("pg_isready")).args([
                        "-h",
                        &data_dir.to_string_lossy(),
                        "-p",
                        &port.to_string(),
                        "-d",
                        "postgres",
                    ]))
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
                                health_fault_reporter(PgProcessFault::Permanent);
                                *health_generation.state.lock().unwrap() = PgProcessState::Faulted;
                            }
                            break;
                        }
                    }
                }
            }
        });
        generation.monitors.lock().unwrap().push(health_task);

        *generation.state.lock().unwrap() = PgProcessState::Running;
        if let Some((instance, cancellation)) = parameters.cancellation {
            let observed = generation.clone();
            let task = tokio::spawn(async move {
                cancellation.cancelled().await;
                if let Some(instance) = instance.upgrade() {
                    let _lifecycle = instance.lifecycle().await;
                    if Arc::ptr_eq(&instance.generation(), &observed) {
                        let result = instance.abort_owned();
                        *observed.state.lock().unwrap() = if result.is_ok() {
                            PgProcessState::Stopped
                        } else {
                            PgProcessState::Faulted
                        };
                    }
                }
            });
            generation.monitors.lock().unwrap().push(task);
        }
        tracing::info!(port = self.port, "PostgreSQL started");
        Ok(())
    }

    fn spawn_owned(
        &self,
        generation: &Arc<ProcessGeneration>,
    ) -> Result<(tokio::process::ChildStdout, tokio::process::ChildStderr), PgError> {
        // Registration and spawn are indivisible with respect to synchronous abort.
        let _cleanup = generation.cleanup_lock.lock().unwrap();
        if self.launches_closed.load(Ordering::Acquire)
            || generation.cleanup_result.lock().unwrap().is_some()
        {
            return Err(PgError::Process(
                "owned PostgreSQL generation is closed".into(),
            ));
        }
        let mut owned = generation.child.lock().unwrap();
        let mut process = OwnedProcess::spawn(
            Command::new(self.pg_bin.join("postgres"))
                .args(["-D", &self.data_dir.to_string_lossy()]),
        )?;
        let streams = process.take_output();
        *owned = Some(process);
        owned.as_mut().unwrap().start()?;
        Ok(streams)
    }

    /// Wait for PG to accept connections.
    async fn wait_ready(&self, generation: &Arc<ProcessGeneration>) -> Result<(), PgError> {
        for i in 0..60 {
            let output = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                generation
                    .helpers
                    .output(Command::new(self.pg_bin.join("pg_isready")).args([
                        "-h",
                        &self.socket_dir().to_string_lossy(),
                        "-p",
                        &self.port.to_string(),
                        "-d",
                        "postgres",
                    ])),
            )
            .await
            .map_err(|_| PgError::Process("pg_isready command timeout".into()))??;

            if output.status.success() {
                // Readiness must belong to our launch, not another server using
                // this socket. Keep the backend alive while validating its parent.
                let timeout = std::time::Duration::from_secs(2);
                let (client, connection) =
                    tokio::time::timeout(timeout, self.connect())
                        .await
                        .map_err(|_| PgError::Process("postmaster connection timeout".into()))??;
                let backend =
                    tokio::time::timeout(timeout, client.query_one("SELECT pg_backend_pid()", &[]))
                        .await
                        .map_err(|_| PgError::Process("postmaster identity timeout".into()))
                        .and_then(|result| {
                            result.map_err(|error| {
                                PgError::Process(format!("identify postmaster: {error}"))
                            })
                        });
                let bound = backend.and_then(|row| {
                    let backend: i32 = row.get(0);
                    generation
                        .child
                        .lock()
                        .unwrap()
                        .as_mut()
                        .ok_or_else(|| PgError::Process("PostgreSQL ownership lost".into()))?
                        .bind_postmaster(backend as u32)
                });
                drop(client);
                connection.abort();
                bound?;
                tracing::debug!(attempt = i, "pg_isready: accepting connections");
                return Ok(());
            }
            if !self.is_running().await {
                return Err(PgError::Process(
                    "PostgreSQL exited before becoming ready".into(),
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        Err(PgError::Process("pg_isready timeout (15s)".into()))
    }

    /// Stop PostgreSQL (fast mode).
    pub async fn stop(&self) -> Result<(), PgError> {
        let _lifecycle = self.lifecycle().await;
        let result = self.finish_owned().await;
        if result.is_ok() {
            tracing::info!(port = self.port, "PostgreSQL stopped");
        }
        result
    }

    pub async fn is_running(&self) -> bool {
        self.generation().running()
    }

    pub async fn process_state(&self) -> PgProcessState {
        *self.generation().state.lock().unwrap()
    }

    /// Promote standby to primary.
    pub async fn promote(&self) -> Result<(), PgError> {
        let generation = self.generation();
        let output = self
            .command_output(
                &generation,
                Command::new(self.pg_bin.join("pg_ctl")).args([
                    "promote",
                    "-D",
                    &self.data_dir.to_string_lossy(),
                    "-w",
                    "-t",
                    "60",
                ]),
                Some((std::time::Duration::from_secs(60), "pg_ctl promote")),
            )
            .await?;

        if !output.status.success() {
            return Err(PgError::command_failed("pg_ctl promote failed", &output)
                .with_generation(generation.id));
        }

        tracing::info!(port = self.port, "PostgreSQL promoted to primary");
        Ok(())
    }

    /// Run pg_basebackup from a source to initialize this replica.
    pub async fn base_backup(&self, source_host: &str, source_port: u16) -> Result<(), PgError> {
        let generation = self.generation();
        let output = self
            .command_output(
                &generation,
                Command::new(self.pg_bin.join("pg_basebackup")).args([
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
                    "-c",
                    "fast",
                    "-R", // creates standby.signal + primary_conninfo
                ]),
                None,
            )
            .await?;

        if !output.status.success() {
            return Err(PgError::command_failed("pg_basebackup failed", &output)
                .with_generation(generation.id));
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

        let generation = self.generation();
        let output = self
            .command_output(
                &generation,
                Command::new(self.pg_bin.join("pg_rewind")).args([
                    "--target-pgdata",
                    &self.data_dir.to_string_lossy(),
                    "--source-server",
                    &source_conn,
                ]),
                None,
            )
            .await?;

        if !output.status.success() {
            return Err(
                PgError::command_failed("pg_rewind failed", &output).with_generation(generation.id)
            );
        }

        tracing::info!(port = self.port, "pg_rewind complete");
        Ok(())
    }

    pub(crate) async fn prepare_native_build(&self) -> Result<(), PgError> {
        let _lifecycle = self.lifecycle().await;
        let expected_abort = self.abort_generation.load(Ordering::Acquire);
        self.finish_owned().await?;
        self.replace_retired(expected_abort)?;
        Ok(())
    }

    pub(crate) async fn clear_pgdata(&self) -> Result<(), PgError> {
        let _lifecycle = self.lifecycle().await;
        let generation = self.generation();
        if generation.running() {
            return Err(
                PgError::Process("cannot clear running PostgreSQL data".into())
                    .with_generation(generation.id),
            );
        }
        let executable = crate::process_supervisor::executable().map_err(|error| {
            PgError::Process(format!("locate PGDATA deletion helper: {error}"))
                .with_generation(generation.id)
        })?;
        let mut command = Command::new(executable);
        command
            .arg(crate::process_supervisor::CLEAR_PGDATA_ARGUMENT)
            .arg(&self.data_dir);
        #[cfg(test)]
        if let Some((entry, marker)) = self.clear_pgdata_hook.lock().unwrap().take() {
            command.arg(entry).arg(marker);
        }
        let output = self.command_output(&generation, &mut command, None).await?;
        if !output.status.success() {
            return Err(PgError::command_failed("clear PGDATA failed", &output)
                .with_generation(generation.id));
        }
        Ok(())
    }

    pub(crate) async fn rewind_for_build(
        &self,
        source_host: &str,
        source_port: u16,
    ) -> Result<bool, PgError> {
        let generation = self.generation();
        let output = self
            .command_output(
                &generation,
                Command::new(self.pg_bin.join("pg_rewind"))
                    .arg("--target-pgdata")
                    .arg(&self.data_dir)
                    .arg("--source-server")
                    .arg(format!(
                        "host={source_host} port={source_port} dbname=postgres user=kuberic_rewind"
                    ))
                    .arg("--no-ensure-shutdown"),
                None,
            )
            .await?;
        if !output.status.success() {
            tracing::warn!(error = %PgError::command_failed("pg_rewind requires fresh backup", &output), "native rewind fallback");
            return Ok(false);
        }
        Ok(true)
    }

    /// Connect to the local PG instance via UDS.
    pub async fn connect(
        &self,
    ) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>), PgError> {
        let generation = self.generation();
        let (client, connection) =
            tokio_postgres::connect(&self.connection_string(), tokio_postgres::NoTls)
                .await
                .map_err(|e| {
                    PgError::Connection(format!("connect to {}: {e}", self.connection_string()))
                        .with_generation(generation.id)
                })?;

        let handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                // Expected during shutdown — PG kills connections on pg_ctl stop
                tracing::debug!("PG connection closed: {}", e);
            }
        });

        self.complete_generation(&generation, Ok((client, handle)))
    }

    pub async fn connect_application(
        &self,
    ) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>), PgError> {
        let generation = self.generation();
        let connection_string = self.application_connection_string();
        let (client, connection) =
            tokio_postgres::connect(&connection_string, tokio_postgres::NoTls)
                .await
                .map_err(|error| {
                    PgError::Connection(format!("connect to {connection_string}: {error}"))
                        .with_generation(generation.id)
                })?;
        let handle = tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::debug!("PG application connection closed: {}", error);
            }
        });
        self.complete_generation(&generation, Ok((client, handle)))
    }

    async fn finish_owned(&self) -> Result<(), PgError> {
        self.retire_owned(true).await
    }

    async fn retire_owned(&self, publish_stopped: bool) -> Result<(), PgError> {
        let generation = self.generation();
        self.retire_generation(generation, publish_stopped).await
    }

    async fn retire_generation(
        &self,
        generation: Arc<ProcessGeneration>,
        publish_stopped: bool,
    ) -> Result<(), PgError> {
        let previous = *generation.state.lock().unwrap();
        *generation.state.lock().unwrap() = PgProcessState::Stopping;
        let ticket = Arc::new(CleanupTicket {
            phase: AtomicU8::new(0),
            commands: generation.helpers.capture(),
            generation: generation.clone(),
            previous,
            launches_closed: self.launches_closed.clone(),
        });
        let waiter = CleanupWaiter(ticket.clone());
        let data_dir = self.data_dir.clone();
        let current = self.current.clone();
        let next = self.next_generation.clone();
        let closed = self.launches_closed.clone();
        let lifecycle = self.lifecycle_lock.clone();
        let retired = generation.clone();
        #[cfg(feature = "testing")]
        let hook = self.cleanup_hook.lock().unwrap().take();
        let result = tokio::task::spawn_blocking(move || {
            if ticket
                .phase
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Ok(());
            }
            #[cfg(feature = "testing")]
            if let Some(hook) = &hook {
                hook.entered.notify_one();
                let _ = hook.release.recv();
            }
            let mut result = retired
                .retire(&ticket.commands, &data_dir)
                .map_err(|error| error.with_generation(retired.id));
            if result.is_ok()
                && publish_stopped
                && let Ok(_lifecycle) = lifecycle.try_lock()
            {
                result = publish_stopped_generation(&current, &next, &closed, Some(&retired));
            }
            #[cfg(feature = "testing")]
            if let Some(hook) = hook {
                hook.finished.notify_one();
            }
            result
        })
        .await
        .map_err(|error| {
            PgError::Process(format!("join PostgreSQL cleanup: {error}"))
                .with_generation(generation.id)
        })?;
        // A live caller already owns lifecycle_lock. A detached completion may
        // publish only after acquiring it, so fault acknowledgement and process
        // replacement cannot interleave across the generation check.
        if result.is_ok() && publish_stopped {
            publish_stopped_generation(
                &self.current,
                &self.next_generation,
                &self.launches_closed,
                Some(&generation),
            )?;
        }
        drop(waiter);
        result
    }

    pub(crate) fn abort_owned(&self) -> Result<(), PgError> {
        let (generation, advanced) = {
            let current = self.current.lock().unwrap();
            let advanced = increment_generation(&self.abort_generation);
            self.launches_closed.store(true, Ordering::Release);
            (current.clone(), advanced)
        };
        let commands = generation.helpers.capture();
        let cleanup = generation.retire(&commands, &self.data_dir);
        match advanced {
            Some(_) => cleanup,
            None => {
                Err(PgError::GenerationExhausted("process abort epoch".into())
                    .with_cleanup(cleanup))
            }
        }
    }

    fn abort_generation(&self, generation: &Arc<ProcessGeneration>) -> Option<Result<(), PgError>> {
        let advanced = {
            let current = self.current.lock().unwrap();
            if !Arc::ptr_eq(&current, generation) {
                return None;
            }
            let advanced = increment_generation(&self.abort_generation);
            self.launches_closed.store(true, Ordering::Release);
            advanced
        };
        let commands = generation.helpers.capture();
        let cleanup = generation.retire(&commands, &self.data_dir);
        Some(match advanced {
            Some(_) => cleanup,
            None => {
                Err(PgError::GenerationExhausted("process abort epoch".into())
                    .with_cleanup(cleanup))
            }
        })
    }
}

impl Drop for PgInstanceManager {
    fn drop(&mut self) {
        if let Err(error) = self.abort_owned() {
            tracing::error!(%error, "PostgreSQL drop cleanup failed");
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum PgError {
    #[error("generation exhausted: {0}")]
    GenerationExhausted(String),
    #[error("{source} [originating process generation {generation}]")]
    Generation {
        generation: u64,
        source: Box<PgError>,
    },
    #[error("operation timeout: {0}")]
    Timeout(String),
    #[error("process error: {0}")]
    Process(String),
    #[error("connection error: {0}")]
    Connection(String),
    #[error("query error: {0}")]
    Query(String),
    #[error("configuration error: {0}")]
    Configuration(String),
}

impl PgError {
    pub(crate) fn with_generation(self, generation: u64) -> Self {
        if matches!(self, Self::Generation { .. }) {
            self
        } else {
            Self::Generation {
                generation,
                source: Box::new(self),
            }
        }
    }

    fn origin(&self) -> Option<u64> {
        match self {
            Self::Generation { generation, .. } => Some(*generation),
            _ => None,
        }
    }

    pub fn is_timeout(&self) -> bool {
        match self {
            Self::Generation { source, .. } => source.is_timeout(),
            Self::Timeout(_) => true,
            _ => false,
        }
    }
    fn command_failed(context: &str, output: &std::process::Output) -> Self {
        Self::Process(format!(
            "{context} ({}); stdout: {}; stderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        ))
    }

    pub(crate) fn with_cleanup(self, cleanup: Result<(), PgError>) -> Self {
        if let Self::Generation { generation, source } = self {
            return source.with_cleanup(cleanup).with_generation(generation);
        }
        match cleanup {
            Ok(()) => self,
            Err(cleanup) => Self::Process(format!("{self}; owned process cleanup: {cleanup}")),
        }
    }

    pub fn fault_type(&self) -> kuberic_runtime::protocol::types::FaultType {
        match self {
            Self::Generation { source, .. } => source.fault_type(),
            Self::Connection(_) | Self::Query(_) | Self::Timeout(_) => {
                kuberic_runtime::protocol::types::FaultType::Transient
            }
            Self::Process(_) | Self::Configuration(_) | Self::GenerationExhausted(_) => {
                kuberic_runtime::protocol::types::FaultType::Permanent
            }
        }
    }
}
