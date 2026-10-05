//! Reusable replica-process hosting for stateful applications.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::StatefulServiceReplica;
use crate::protocol::types::{
    FaultType, PodUid, PvcUid, ReplicaId, ReplicaInstanceId, ResourceUid,
};
use serde::Serialize;
use tokio::sync::{Mutex, watch};

use crate::host::Result;
use crate::host::hosting::PodRuntime;
use crate::host::provisioning::{ObservedStorageIdentity, validate_established_identity};
use crate::host::service::{AgentService, InitializationService, SHUTDOWN_TIMEOUT};
use crate::host::sqlite_store::SqliteStore;
use crate::host::store::AgentStore;
use crate::host::transport::{
    GrpcOutboundDispatcher, ReliableTransport, ReplicaEndpointResolver, run_outbound,
    run_peer_discovery,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplicationStorageState {
    FreshEmpty,
    Established,
}

#[derive(Debug, Clone)]
pub struct ReplicaProcessConfig {
    pub resource_uid: ResourceUid,
    pub replica_id: ReplicaId,
    pub pod_uid: PodUid,
    pub pvc_uid: PvcUid,
    pub data_root: PathBuf,
    pub control_address: SocketAddr,
    pub replication_address: SocketAddr,
    pub bearer_token: String,
    pub rpc_deadline: Duration,
    pub transport_window_capacity: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaDiagnostics {
    pub replica_id: i64,
    pub instance_id: String,
    pub agent_generation: String,
    pub process_session: String,
    pub role: String,
    pub epoch: String,
    pub previous_configuration: Option<String>,
    pub current_configuration: Option<String>,
    pub current_progress: i64,
    pub verified_replication_lsn: Option<i64>,
    pub committed_lsn: i64,
    pub read_status: String,
    pub write_status: String,
    pub catch_up_boundary_lsn: Option<i64>,
    pub catch_up_complete: bool,
    pub scale_up_operation: Option<String>,
    pub retired: bool,
    pub pending_operation: Option<String>,
    pub blocking: Option<String>,
    pub builds: Vec<ReplicaBuildDiagnostics>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaBuildDiagnostics {
    pub build_id: String,
    pub target_instance: String,
    pub replication_boundary_lsn: i64,
    pub durable_lsn: i64,
    pub completed: bool,
    pub catch_up_boundary_lsn: Option<i64>,
}

#[derive(Clone)]
pub struct ReplicaHandle {
    runtime: Arc<PodRuntime>,
    store: Arc<SqliteStore>,
    process_session: Arc<str>,
}

impl ReplicaHandle {
    pub async fn diagnostics(&self) -> Result<ReplicaDiagnostics> {
        let state = self.store.load_state().await?;
        let snapshot = self.runtime.snapshot().await;
        Ok(ReplicaDiagnostics {
            replica_id: state.identity.local_identity.replica_id.value(),
            instance_id: state.identity.local_identity.instance_id.to_string(),
            agent_generation: state.identity.local_identity.agent_generation.to_string(),
            process_session: self.process_session.to_string(),
            role: format!("{:?}", snapshot.role),
            epoch: format!(
                "{}.{}",
                state.highest_epoch.data_loss_number, state.highest_epoch.configuration_number
            ),
            previous_configuration: state
                .previous_configuration
                .map(|configuration| configuration.configuration_id.to_string()),
            current_configuration: state
                .current_configuration
                .map(|configuration| configuration.configuration_id.to_string()),
            current_progress: snapshot.current_progress,
            verified_replication_lsn: snapshot.verified_replication_lsn,
            committed_lsn: snapshot.committed_lsn,
            read_status: format!("{:?}", snapshot.read_status),
            write_status: format!("{:?}", snapshot.write_status),
            catch_up_boundary_lsn: snapshot.catch_up_boundary,
            catch_up_complete: snapshot.catch_up_complete,
            scale_up_operation: state
                .scale_up_evidence
                .as_deref()
                .map(|evidence| evidence.intent().operation_id.to_string()),
            retired: state.retired_authority.is_some() || snapshot.retired_authority.is_some(),
            pending_operation: state
                .reconfiguration
                .as_ref()
                .map(|record| record.command.operation_id.to_string())
                .or_else(|| {
                    state
                        .pending_effect
                        .as_ref()
                        .map(|pending| pending.effect.operation_id.to_string())
                }),
            blocking: state
                .reconfiguration
                .as_ref()
                .map(|record| {
                    format!(
                        "configuration:{:?}:{}",
                        record.stage, record.command.operation_id
                    )
                })
                .or_else(|| {
                    state.pending_effect.as_ref().map(|pending| {
                        format!("effect:{:?}:{}", pending.stage, pending.effect.operation_id)
                    })
                }),
            builds: snapshot
                .builds
                .into_iter()
                .map(|build| ReplicaBuildDiagnostics {
                    build_id: build.authority.build_id.to_string(),
                    target_instance: build.authority.target.instance_id.to_string(),
                    replication_boundary_lsn: build.authority.replication_boundary_lsn,
                    durable_lsn: build.durable_lsn,
                    completed: build.completed,
                    catch_up_boundary_lsn: build.catch_up_boundary_lsn,
                })
                .collect(),
        })
    }
}

pub struct RunningReplica {
    handle: ReplicaHandle,
    shutdown: watch::Sender<bool>,
    completion: tokio::task::JoinHandle<Result<()>>,
}

impl RunningReplica {
    pub fn handle(&self) -> ReplicaHandle {
        self.handle.clone()
    }

    pub fn shutdown_signal(&self) -> watch::Receiver<bool> {
        self.shutdown.subscribe()
    }

    pub fn shutdown(&self) {
        self.shutdown.send_replace(true);
    }

    pub async fn wait(&mut self) -> Result<()> {
        (&mut self.completion)
            .await
            .map_err(|error| crate::host::HostError::CommandRejected(error.to_string()))?
    }
}

impl Drop for RunningReplica {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
    }
}

pub struct ReplicaHost<A, R> {
    config: ReplicaProcessConfig,
    application: Arc<A>,
    application_storage: ApplicationStorageState,
    application_storage_paths: Option<BTreeMap<String, PathBuf>>,
    resolver: Arc<R>,
}

impl<A, R> ReplicaHost<A, R>
where
    A: StatefulServiceReplica + 'static,
    R: ReplicaEndpointResolver + 'static,
{
    pub fn new(
        config: ReplicaProcessConfig,
        application: Arc<A>,
        application_storage: ApplicationStorageState,
        resolver: Arc<R>,
    ) -> Self {
        Self {
            config,
            application,
            application_storage,
            application_storage_paths: None,
            resolver,
        }
    }

    /// Bind application paths at authorized initialization. Only an unfinished
    /// first open receives `OpenMode::New`; missing bindings on existing stores reject.
    pub fn with_application_storage_paths(mut self, paths: BTreeMap<String, PathBuf>) -> Self {
        self.application_storage_paths = Some(paths);
        self
    }

    pub async fn start(self) -> Result<RunningReplica> {
        let (_shutdown, receiver) = watch::channel(false);
        self.start_with_shutdown(receiver)
            .await?
            .ok_or(crate::host::HostError::Runtime(
                crate::RuntimeError::OperationCancelled,
            ))
    }

    /// Cooperatively cancel startup and await its acknowledgement and task cleanup.
    /// `None` means cancellation completed before readiness. A readiness race may
    /// return a replica instead; the caller must shut it down and await `wait`.
    /// Keep this future alive until completion, including after requesting shutdown.
    pub async fn start_with_shutdown(
        self,
        mut startup_shutdown: watch::Receiver<bool>,
    ) -> Result<Option<RunningReplica>> {
        if *startup_shutdown.borrow() {
            return Ok(None);
        }
        if self.config.replica_id.value() <= 0 {
            return Err(crate::host::HostError::CommandRejected(
                "replica ID must be positive".into(),
            ));
        }
        if self.config.transport_window_capacity == 0 {
            return Err(crate::host::HostError::Backpressure(
                "transport window capacity must be positive".into(),
            ));
        }
        let observed = ObservedStorageIdentity {
            resource_uid: self.config.resource_uid.clone(),
            pod_uid: self.config.pod_uid.clone(),
            pvc_uid: self.config.pvc_uid.clone(),
            instance_id: ReplicaInstanceId::new(self.config.pod_uid.as_str()),
        };
        let paths = self
            .application_storage_paths
            .map(|paths| {
                paths
                    .into_iter()
                    .map(|(name, path)| resolve_storage_path(&path).map(|path| (name, path)))
                    .collect::<std::io::Result<BTreeMap<_, _>>>()
            })
            .transpose()?;
        let database_path = SqliteStore::metadata_database_path(&self.config.data_root);
        if !database_path.is_file() {
            serve_initialization(
                &self.config,
                observed.clone(),
                database_path.clone(),
                self.application_storage == ApplicationStorageState::FreshEmpty,
                paths.clone(),
                startup_shutdown.clone(),
            )
            .await?;
        }
        if *startup_shutdown.borrow() || startup_shutdown.has_changed().is_err() {
            return Ok(None);
        }

        let store = Arc::new(SqliteStore::open_existing(&database_path, None)?);
        let identity = store.identity().await?;
        validate_established_identity(&identity, &observed, self.config.replica_id)?;
        let state = store.load_state().await?;
        if state
            .application_storage
            .as_ref()
            .map(|binding| &binding.paths)
            != paths.as_ref()
        {
            store
                .record_partition_reports(state.load_metrics, Some(FaultType::Permanent))
                .await?;
            return Err(crate::host::HostError::InitializationNotAuthorized(
                "application storage paths differ from the authorized agent binding".into(),
            ));
        }
        let runtime = Arc::new(PodRuntime::new(
            identity.local_identity.clone(),
            self.application,
            store.clone(),
        ));
        let agent = AgentService::new(
            store.clone(),
            runtime.clone(),
            runtime.clone(),
            self.config.bearer_token.clone(),
        )?;
        let process_session: Arc<str> = Arc::from(agent.sessions().local_session().as_str());
        let sessions = agent.sessions().clone();
        let transport = Arc::new(Mutex::new(ReliableTransport::new(
            agent.sessions().local_session().clone(),
            self.config.transport_window_capacity,
        )?));
        let dispatcher = Arc::new(GrpcOutboundDispatcher::new(
            runtime.clone(),
            transport.clone(),
            self.resolver,
            self.config.resource_uid.to_string(),
            self.config.bearer_token,
            self.config.rpc_deadline,
        )?);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (ready, mut ready_rx) = watch::channel(false);
        let mut agent_task = tokio::spawn(agent.serve(
            self.config.control_address,
            self.config.replication_address,
            ready,
            shutdown_rx.clone(),
        ));
        let mut outbound_task = tokio::spawn(run_outbound(
            runtime.clone(),
            transport.clone(),
            dispatcher.clone(),
            shutdown_rx.clone(),
        ));
        let mut peer_task = tokio::spawn(run_peer_discovery(
            identity.local_identity,
            runtime.clone(),
            store.clone(),
            transport,
            dispatcher,
            sessions,
            shutdown_rx,
        ));
        let mut agent_finished = false;
        let mut outbound_finished = false;
        let mut peer_finished = false;
        let startup_result = tokio::select! {
            biased;
            result = &mut agent_task => {
                agent_finished = true;
                match result {
                    Ok(Err(error)) => Err(error),
                    Err(error) => Err(crate::host::HostError::CommandRejected(error.to_string())),
                    Ok(Ok(())) => Err(crate::host::HostError::CommandRejected(
                        "agent service stopped before becoming ready".into(),
                    )),
                }
            }
            result = &mut outbound_task => {
                outbound_finished = true;
                match result {
                    Ok(Err(error)) => Err(error),
                    Err(error) => Err(crate::host::HostError::CommandRejected(error.to_string())),
                    Ok(Ok(())) => Err(crate::host::HostError::CommandRejected(
                        "outbound progress stopped before agent readiness".into(),
                    )),
                }
            }
            result = &mut peer_task => {
                peer_finished = true;
                match result {
                    Ok(Err(error)) => Err(error),
                    Err(error) => Err(crate::host::HostError::CommandRejected(error.to_string())),
                    Ok(Ok(())) => Err(crate::host::HostError::CommandRejected(
                        "peer discovery stopped before agent readiness".into(),
                    )),
                }
            }
            result = async { ready_rx.wait_for(|ready| *ready).await.map(|_| ()) } => {
                match result {
                    Ok(_) => Ok(true),
                    Err(_) => {
                        agent_finished = true;
                        match (&mut agent_task).await {
                            Ok(Err(error)) => Err(error),
                            Err(error) => Err(crate::host::HostError::CommandRejected(error.to_string())),
                            Ok(Ok(())) => Err(crate::host::HostError::CommandRejected(
                                "agent readiness channel closed".into(),
                            )),
                        }
                    }
                }
            }
            _ = async { let _ = startup_shutdown.wait_for(|stopped| *stopped).await; } => Ok(false),
        };
        if !matches!(startup_result, Ok(true)) {
            shutdown.send_replace(true);
            let outbound = abort_progress(&mut outbound_task, outbound_finished).await;
            let peer = abort_progress(&mut peer_task, peer_finished).await;
            let cleanup = if agent_finished {
                Ok(())
            } else {
                finish_agent(&mut agent_task).await
            };
            runtime.abort();
            let cleanup =
                match cleanup {
                    Err(crate::host::HostError::Runtime(
                        crate::RuntimeError::OperationCancelled,
                    )) if matches!(startup_result, Ok(false)) => Ok(()),
                    result => result,
                };
            let result = with_shutdown_error(startup_result.map(|_| ()), cleanup, "agent shutdown");
            let result = with_shutdown_error(result, outbound, "outbound shutdown");
            with_shutdown_error(result, peer, "peer discovery shutdown")?;
            return Ok(None);
        }
        let supervisor_shutdown = shutdown.clone();
        let supervisor_runtime = runtime.clone();
        let completion = tokio::spawn(async move {
            let (first, finished) = tokio::select! {
                result = &mut agent_task => (result, 0),
                result = &mut outbound_task => (result, 1),
                result = &mut peer_task => (result, 2),
            };
            supervisor_shutdown.send_replace(true);
            let outbound = abort_progress(&mut outbound_task, finished == 1).await;
            let peer = abort_progress(&mut peer_task, finished == 2).await;
            // Joining the agent is the persistence acknowledgement. Aborting its
            // wrapper task can otherwise discard an accepted fault during shutdown.
            let cleanup = if finished == 0 {
                Ok(())
            } else {
                finish_agent(&mut agent_task).await
            };
            supervisor_runtime.abort();
            let first = first
                .map_err(|error| crate::host::HostError::CommandRejected(error.to_string()))
                .and_then(|result| result);
            let result = with_shutdown_error(cleanup, first, "replica task");
            let result = with_shutdown_error(result, outbound, "outbound shutdown");
            with_shutdown_error(result, peer, "peer discovery shutdown")
        });

        Ok(Some(RunningReplica {
            handle: ReplicaHandle {
                runtime,
                store,
                process_session,
            },
            shutdown,
            completion,
        }))
    }
}

async fn abort_progress(
    task: &mut tokio::task::JoinHandle<Result<()>>,
    finished: bool,
) -> Result<()> {
    if finished {
        return Ok(());
    }
    task.abort();
    match task.await {
        Ok(result) => result,
        Err(error) if error.is_cancelled() => Ok(()),
        Err(error) => Err(crate::host::HostError::CommandRejected(error.to_string())),
    }
}

fn with_shutdown_error(primary: Result<()>, cleanup: Result<()>, context: &str) -> Result<()> {
    match (primary, cleanup) {
        (Err(primary), Err(cleanup)) => Err(crate::host::HostError::CommandRejected(format!(
            "{primary}; {context}: {cleanup}"
        ))),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

async fn finish_agent(task: &mut tokio::task::JoinHandle<Result<()>>) -> Result<()> {
    match tokio::time::timeout(SHUTDOWN_TIMEOUT + Duration::from_secs(1), &mut *task).await {
        Ok(result) => {
            result.map_err(|error| crate::host::HostError::CommandRejected(error.to_string()))?
        }
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err(crate::host::HostError::CommandRejected(
                "agent shutdown acknowledgement timed out".into(),
            ))
        }
    }
}

async fn serve_initialization(
    config: &ReplicaProcessConfig,
    observed: ObservedStorageIdentity,
    database_path: PathBuf,
    fresh_application_state: bool,
    application_storage_paths: Option<BTreeMap<String, PathBuf>>,
    mut startup_shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let (initialized, mut initialized_rx) = watch::channel(false);
    let service = InitializationService::new(
        observed,
        config.replica_id,
        database_path,
        config.bearer_token.clone(),
        initialized,
        fresh_application_state,
    )?
    .with_application_storage_paths(application_storage_paths);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let (ready, _) = watch::channel(false);
    let stop = tokio::spawn(async move {
        tokio::select! {
            _ = async { let _ = initialized_rx.wait_for(|initialized| *initialized).await; } => {}
            _ = async { let _ = startup_shutdown.wait_for(|stopped| *stopped).await; } => {}
        }
        shutdown.send_replace(true);
    });
    let result = service
        .serve(config.control_address, ready, shutdown_rx)
        .await;
    stop.abort();
    let _ = stop.await;
    result
}

// Resolve existing ancestors without creating even empty application directories.
fn resolve_storage_path(path: &Path) -> std::io::Result<PathBuf> {
    match std::fs::canonicalize(path) {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let absolute = std::path::absolute(path)?;
            let parent = absolute.parent().ok_or(error)?;
            let name = absolute.file_name().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid application path")
            })?;
            Ok(resolve_storage_path(parent)?.join(name))
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;

    #[tokio::test]
    async fn cancelling_progress_joins_and_retains_completed_failures() {
        let mut pending = tokio::spawn(std::future::pending::<Result<()>>());
        abort_progress(&mut pending, false).await.unwrap();
        assert!(pending.is_finished());
        let mut failed = tokio::spawn(async {
            Err(crate::host::HostError::CommandRejected(
                "outbound failed".into(),
            ))
        });
        while !failed.is_finished() {
            tokio::task::yield_now().await;
        }
        let cleanup = abort_progress(&mut failed, false).await;
        let result = with_shutdown_error(
            Err(crate::host::HostError::CommandRejected(
                "startup failed".into(),
            )),
            cleanup,
            "outbound shutdown",
        );
        let error = result.unwrap_err().to_string();
        assert!(error.find("startup failed").unwrap() < error.find("outbound failed").unwrap());
    }

    #[tokio::test]
    async fn agent_shutdown_waits_for_acknowledgement_and_propagates_persistence_failure() {
        let (release, wait) = tokio::sync::oneshot::channel();
        let mut agent = tokio::spawn(async move {
            wait.await.unwrap();
            Err(crate::host::HostError::CommandRejected(
                "injected persistence failure".into(),
            ))
        });
        let completion = finish_agent(&mut agent);
        tokio::pin!(completion);
        tokio::select! {
            biased;
            result = &mut completion => panic!("returned before acknowledgement: {result:?}"),
            _ = tokio::task::yield_now() => {}
        }
        release.send(()).unwrap();
        assert!(
            completion
                .await
                .unwrap_err()
                .to_string()
                .contains("injected persistence failure")
        );
    }

    #[tokio::test]
    async fn agent_shutdown_rejects_an_already_stopped_consumer_without_deadlock() {
        let mut agent = tokio::spawn(std::future::pending::<Result<()>>());
        agent.abort();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), finish_agent(&mut agent))
                .await
                .unwrap()
                .is_err()
        );
    }
}
