//! Reusable replica-process hosting for stateful applications.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use kuberic_protocol::types::{PodUid, PvcUid, ReplicaId, ReplicaInstanceId, ResourceUid};
use kuberic_runtime::StatefulServiceReplica;
use serde::Serialize;
use tokio::sync::{Mutex, watch};

use crate::hosting::PodRuntime;
use crate::provisioning::{ObservedStorageIdentity, validate_established_identity};
use crate::service::{AgentService, InitializationService};
use crate::sqlite_store::SqliteStore;
use crate::store::AgentStore;
use crate::transport::{
    GrpcOutboundDispatcher, ReliableTransport, ReplicaEndpointResolver, run_outbound,
    run_peer_discovery,
};
use crate::{AgentError, Result};

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
    pub committed_lsn: i64,
    pub write_status: String,
    pub pending_operation: Option<String>,
    pub builds: Vec<ReplicaBuildDiagnostics>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaBuildDiagnostics {
    pub build_id: String,
    pub target_instance: String,
    pub durable_lsn: i64,
    pub completed: bool,
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
            committed_lsn: snapshot.committed_lsn,
            write_status: format!("{:?}", snapshot.write_status),
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
            builds: snapshot
                .builds
                .into_iter()
                .map(|build| ReplicaBuildDiagnostics {
                    build_id: build.authority.build_id.to_string(),
                    target_instance: build.authority.target.instance_id.to_string(),
                    durable_lsn: build.durable_lsn,
                    completed: build.completed,
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
            .map_err(|error| AgentError::CommandRejected(error.to_string()))?
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
            resolver,
        }
    }

    pub async fn start(self) -> Result<RunningReplica> {
        if self.config.replica_id.value() <= 0 {
            return Err(AgentError::CommandRejected(
                "replica ID must be positive".into(),
            ));
        }
        if self.config.transport_window_capacity == 0 {
            return Err(AgentError::Backpressure(
                "transport window capacity must be positive".into(),
            ));
        }
        let observed = ObservedStorageIdentity {
            resource_uid: self.config.resource_uid.clone(),
            pod_uid: self.config.pod_uid.clone(),
            pvc_uid: self.config.pvc_uid.clone(),
            instance_id: ReplicaInstanceId::new(self.config.pod_uid.as_str()),
        };
        let database_path = SqliteStore::metadata_database_path(&self.config.data_root);
        if !database_path.is_file() {
            serve_initialization(
                &self.config,
                observed.clone(),
                database_path.clone(),
                self.application_storage == ApplicationStorageState::FreshEmpty,
            )
            .await?;
        }

        let store = Arc::new(SqliteStore::open_existing(&database_path, None)?);
        let identity = store.identity().await?;
        validate_established_identity(&identity, &observed, self.config.replica_id)?;
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
        tokio::select! {
            result = &mut agent_task => {
                result
                    .map_err(|error| AgentError::CommandRejected(error.to_string()))??;
                return Err(AgentError::CommandRejected(
                    "agent service stopped before becoming ready".into(),
                ));
            }
            result = &mut outbound_task => {
                result
                    .map_err(|error| AgentError::CommandRejected(error.to_string()))??;
                return Err(AgentError::CommandRejected(
                    "outbound progress stopped before agent readiness".into(),
                ));
            }
            result = &mut peer_task => {
                result
                    .map_err(|error| AgentError::CommandRejected(error.to_string()))??;
                return Err(AgentError::CommandRejected(
                    "peer discovery stopped before agent readiness".into(),
                ));
            }
            result = ready_rx.wait_for(|ready| *ready) => {
                result.map_err(|_| {
                    AgentError::CommandRejected("agent readiness channel closed".into())
                })?;
            }
        }
        let supervisor_shutdown = shutdown.clone();
        let completion = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            tasks.spawn(async move {
                agent_task
                    .await
                    .map_err(|error| AgentError::CommandRejected(error.to_string()))?
            });
            tasks.spawn(async move {
                outbound_task
                    .await
                    .map_err(|error| AgentError::CommandRejected(error.to_string()))?
            });
            tasks.spawn(async move {
                peer_task
                    .await
                    .map_err(|error| AgentError::CommandRejected(error.to_string()))?
            });
            let first = tasks
                .join_next()
                .await
                .ok_or_else(|| AgentError::CommandRejected("replica host has no tasks".into()))?
                .map_err(|error| AgentError::CommandRejected(error.to_string()))?;
            supervisor_shutdown.send_replace(true);
            tasks.abort_all();
            first
        });

        Ok(RunningReplica {
            handle: ReplicaHandle {
                runtime,
                store,
                process_session,
            },
            shutdown,
            completion,
        })
    }
}

async fn serve_initialization(
    config: &ReplicaProcessConfig,
    observed: ObservedStorageIdentity,
    database_path: PathBuf,
    fresh_application_state: bool,
) -> Result<()> {
    let (initialized, mut initialized_rx) = watch::channel(false);
    let service = InitializationService::new(
        observed,
        config.replica_id,
        database_path,
        config.bearer_token.clone(),
        initialized,
        fresh_application_state,
    )?;
    let (shutdown, shutdown_rx) = watch::channel(false);
    let (ready, _) = watch::channel(false);
    let stop = tokio::spawn(async move {
        let _ = initialized_rx.wait_for(|initialized| *initialized).await;
        shutdown.send_replace(true);
    });
    service
        .serve(config.control_address, ready, shutdown_rx)
        .await?;
    stop.await
        .map_err(|error| AgentError::CommandRejected(error.to_string()))?;
    Ok(())
}
