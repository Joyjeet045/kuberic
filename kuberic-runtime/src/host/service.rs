//! Tonic control, peer, and replication services.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::application::OpenMode;
use crate::control::{normalize_execute_request, proto};
use crate::protocol::command::{EnsureReplicaBuild, ProtocolCommand};
use crate::protocol::types::{
    ProcessSessionId, ReplicaId, ReplicaIdentity, TransitionIntent, TransitionKind,
    derive_transition_id,
};
use futures::{Stream, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::{OwnedRwLockReadGuard, RwLock, watch};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response, Status};

use crate::RuntimeError;
use crate::host::Result;
use crate::host::coordinator::Coordinator;
use crate::host::hosting::{PodRuntime, RuntimeDataPlane};
use crate::host::provisioning::{InitializationAuthority, ObservedStorageIdentity};
use crate::host::report::AgentReporter;
use crate::host::runtime_adapter::RuntimeEffectExecutor;
use crate::host::session::ProcessSession;
use crate::host::sqlite_store::SqliteStore;
use crate::host::state::{AgentState, ApplicationStorageBinding, CoordinatorStage};
use crate::host::store::AgentStore;

const AUTHORIZATION_HEADER: &str = "authorization";
pub(crate) const SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

pub(crate) struct InitializationService {
    observed: ObservedStorageIdentity,
    replica_id: ReplicaId,
    database_path: PathBuf,
    bearer_token: Arc<str>,
    session: Arc<ProcessSession>,
    initialized: watch::Sender<bool>,
    fresh_application_state: bool,
    application_storage_paths: Option<BTreeMap<String, PathBuf>>,
}

impl InitializationService {
    pub(crate) fn new(
        observed: ObservedStorageIdentity,
        replica_id: ReplicaId,
        database_path: PathBuf,
        bearer_token: impl Into<Arc<str>>,
        initialized: watch::Sender<bool>,
        fresh_application_state: bool,
    ) -> Result<Self> {
        let bearer_token = bearer_token.into();
        if bearer_token.is_empty() {
            return Err(crate::host::HostError::CommandRejected(
                "agent bearer token must not be empty".into(),
            ));
        }
        Ok(Self {
            observed,
            replica_id,
            database_path,
            bearer_token,
            session: Arc::new(ProcessSession::new()),
            initialized,
            fresh_application_state,
            application_storage_paths: None,
        })
    }

    pub(crate) fn with_application_storage_paths(
        mut self,
        paths: Option<BTreeMap<String, PathBuf>>,
    ) -> Self {
        self.application_storage_paths = paths;
        self
    }

    pub(crate) async fn serve(
        self,
        control_address: SocketAddr,
        ready: watch::Sender<bool>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let listener = TcpListener::bind(control_address).await?;
        let control = self.clone();
        let peer = self;
        ready.send_replace(true);
        let result = tonic::transport::Server::builder()
            .add_service(proto::agent_control_server::AgentControlServer::new(
                control,
            ))
            .add_service(proto::replica_peer_server::ReplicaPeerServer::new(peer))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                wait_for_shutdown(&mut shutdown).await;
            })
            .await
            .map_err(|error| crate::host::HostError::CommandRejected(error.to_string()));
        ready.send_replace(false);
        result
    }

    fn authorize<T>(&self, request: &Request<T>) -> std::result::Result<(), Status> {
        authorize_request(request, &self.bearer_token)
    }

    fn report(&self) -> proto::AgentStatusReport {
        if !self.fresh_application_state {
            return proto::AgentStatusReport {
                protocol_version: crate::protocol::PROTOCOL_VERSION,
                resource_uid: self.observed.resource_uid.to_string(),
                process_session_id: self.session.id().to_string(),
                report_sequence: self.session.next_report_sequence(),
                storage_state: proto::AgentStorageState::Unsafe as i32,
                pod_uid: self.observed.pod_uid.to_string(),
                pvc_uid: self.observed.pvc_uid.to_string(),
                storage_error: "application state exists without matching Kuberic agent metadata"
                    .to_string(),
                healthy: false,
                replica_id: self.replica_id.value(),
                ..Default::default()
            };
        }
        proto::AgentStatusReport {
            protocol_version: crate::protocol::PROTOCOL_VERSION,
            resource_uid: self.observed.resource_uid.to_string(),
            process_session_id: self.session.id().to_string(),
            report_sequence: self.session.next_report_sequence(),
            storage_state: proto::AgentStorageState::Uninitialized as i32,
            pod_uid: self.observed.pod_uid.to_string(),
            pvc_uid: self.observed.pvc_uid.to_string(),
            healthy: true,
            replica_id: self.replica_id.value(),
            ..Default::default()
        }
    }

    fn validate_status_target(
        &self,
        request: &proto::GetAgentStatusRequest,
    ) -> std::result::Result<(), Status> {
        if request.protocol_version != crate::protocol::PROTOCOL_VERSION {
            return Err(Status::failed_precondition("unsupported protocol version"));
        }
        if request.resource_uid != self.observed.resource_uid.as_str()
            || request.expected_instance_id != self.observed.instance_id.as_str()
            || request.replica_id != self.replica_id.value()
        {
            return Err(Status::failed_precondition(
                "status request targets another replica incarnation",
            ));
        }
        Ok(())
    }

    fn initialize(
        &self,
        request: proto::ExecuteCommandRequest,
    ) -> std::result::Result<proto::ExecuteCommandResponse, Status> {
        if !self.fresh_application_state {
            return Err(Status::failed_precondition(
                "application state is not fresh and empty",
            ));
        }
        let envelope = normalize_execute_request(request)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if envelope.expected_process_session_id != *self.session.id() {
            return Err(Status::failed_precondition(
                "command targets a stale agent process session",
            ));
        }
        let ProtocolCommand::InitializeAgentStore(command) = envelope.command else {
            return Err(Status::failed_precondition(
                "fresh storage accepts only InitializeAgentStore",
            ));
        };
        if envelope.resource_uid != self.observed.resource_uid
            || envelope.target.replica_id != command.local_replica_id
            || envelope.target.instance_id != command.expected_instance_id
            || envelope.target.agent_generation != command.assigned_agent_generation
        {
            return Err(Status::failed_precondition(
                "initialization targets another replica incarnation",
            ));
        }
        let transition = TransitionIntent {
            secondary_scale_down: None,
            secondary_removal_evidence: None,
            scale_up: None,
            scale_up_failover: None,
            transition_id: derive_transition_id(
                &command.resource_uid,
                TransitionKind::Bootstrap,
                &command.bootstrap_configuration.configuration_id,
            ),
            kind: TransitionKind::Bootstrap,
            spec_generation: 0,
            effective_policy: command.effective_policy.clone(),
            previous_configuration_id: None,
            current_configuration: command.bootstrap_configuration.clone(),
            election_lsn: None,
            build_id: None,
            repair: None,
            switchover: None,
        };
        let authority = command.provisioning.as_ref().map_or(
            InitializationAuthority::Bootstrap(&transition),
            |provisioning| {
                if provisioning.scale_up().is_some() {
                    InitializationAuthority::ScaleUp(provisioning)
                } else {
                    InitializationAuthority::Replacement(provisioning)
                }
            },
        );
        let identity =
            crate::host::command::admit_initialization(&command, &self.observed, authority)
                .map_err(status_from_agent)?;
        let mut state = AgentState::new(identity.clone());
        state.application_storage =
            self.application_storage_paths
                .clone()
                .map(|paths| ApplicationStorageBinding {
                    paths,
                    initializing: true,
                });
        state.scale_up_initialization = command
            .provisioning
            .clone()
            .filter(|provisioning| provisioning.scale_up().is_some());
        match SqliteStore::create_authorized(&self.database_path, state) {
            Ok(store) => drop(store),
            Err(crate::host::HostError::Io(error))
                if error.kind() == std::io::ErrorKind::AlreadyExists =>
            {
                SqliteStore::open_existing(&self.database_path, Some(&identity))
                    .map_err(status_from_agent)?;
            }
            Err(error) => return Err(status_from_agent(error)),
        }
        self.initialized.send_replace(true);
        Ok(proto::ExecuteCommandResponse {
            protocol_version: crate::protocol::PROTOCOL_VERSION,
            observation: Some(self.report()),
        })
    }
}

impl Clone for InitializationService {
    fn clone(&self) -> Self {
        Self {
            observed: self.observed.clone(),
            replica_id: self.replica_id,
            database_path: self.database_path.clone(),
            bearer_token: self.bearer_token.clone(),
            session: self.session.clone(),
            initialized: self.initialized.clone(),
            fresh_application_state: self.fresh_application_state,
            application_storage_paths: self.application_storage_paths.clone(),
        }
    }
}

#[tonic::async_trait]
impl proto::agent_control_server::AgentControl for InitializationService {
    async fn get_status(
        &self,
        request: Request<proto::GetAgentStatusRequest>,
    ) -> std::result::Result<Response<proto::AgentStatusReport>, Status> {
        self.authorize(&request)?;
        self.validate_status_target(request.get_ref())?;
        Ok(Response::new(self.report()))
    }

    async fn execute(
        &self,
        request: Request<proto::ExecuteCommandRequest>,
    ) -> std::result::Result<Response<proto::ExecuteCommandResponse>, Status> {
        self.authorize(&request)?;
        Ok(Response::new(self.initialize(request.into_inner())?))
    }
}

#[tonic::async_trait]
impl proto::replica_peer_server::ReplicaPeer for InitializationService {
    async fn get_status(
        &self,
        request: Request<proto::GetAgentStatusRequest>,
    ) -> std::result::Result<Response<proto::AgentStatusReport>, Status> {
        self.authorize(&request)?;
        self.validate_status_target(request.get_ref())?;
        Ok(Response::new(self.report()))
    }

    async fn execute(
        &self,
        request: Request<proto::ExecuteCommandRequest>,
    ) -> std::result::Result<Response<proto::ExecuteCommandResponse>, Status> {
        self.authorize(&request)?;
        Ok(Response::new(self.initialize(request.into_inner())?))
    }
}

fn authorize_request<T>(
    request: &Request<T>,
    bearer_token: &str,
) -> std::result::Result<(), Status> {
    let expected = format!("Bearer {bearer_token}");
    let observed = request
        .metadata()
        .get(AUTHORIZATION_HEADER)
        .and_then(|value| value.to_str().ok());
    if observed != Some(expected.as_str()) {
        return Err(Status::unauthenticated("invalid agent credentials"));
    }
    Ok(())
}

pub(crate) struct SessionRegistry {
    local_session: ProcessSessionId,
    peers: Arc<RwLock<BTreeMap<ReplicaIdentity, ProcessSessionId>>>,
    excluded: RwLock<std::collections::BTreeSet<ReplicaIdentity>>,
}

pub(crate) struct SessionLease {
    _peers: OwnedRwLockReadGuard<BTreeMap<ReplicaIdentity, ProcessSessionId>>,
}

impl SessionRegistry {
    pub(crate) fn new(local_session: ProcessSessionId) -> Self {
        Self {
            local_session,
            peers: Arc::new(RwLock::new(BTreeMap::new())),
            excluded: RwLock::new(std::collections::BTreeSet::new()),
        }
    }

    pub(crate) fn local_session(&self) -> &ProcessSessionId {
        &self.local_session
    }

    pub(crate) async fn register_peer(&self, identity: ReplicaIdentity, session: ProcessSessionId) {
        let excluded = self.excluded.read().await;
        if excluded.contains(&identity) {
            return;
        }
        self.peers.write().await.insert(identity, session);
    }

    pub(crate) async fn retire_peer(&self, identity: &ReplicaIdentity) {
        let mut excluded = self.excluded.write().await;
        excluded.insert(identity.clone());
        self.peers.write().await.remove(identity);
    }

    pub(crate) async fn retain_members(
        &self,
        authority: Option<&crate::authority::AdmittedAuthority>,
    ) {
        self.peers
            .write()
            .await
            .retain(|identity, _| authority.is_some_and(|a| a.contains_member(identity)));
    }

    pub(crate) async fn validate_peer(
        &self,
        sender: &ReplicaIdentity,
        sender_session: &str,
        receiver_session: &str,
    ) -> std::result::Result<SessionLease, Status> {
        if receiver_session != self.local_session.as_str() {
            return Err(Status::failed_precondition(
                "replication targets a retired receiver session",
            ));
        }
        let peers = self.peers.clone().read_owned().await;
        let expected = peers
            .get(sender)
            .ok_or_else(|| Status::failed_precondition("sender session has not been admitted"))?;
        if expected.as_str() != sender_session {
            return Err(Status::failed_precondition(
                "replication originates from a retired sender session",
            ));
        }
        Ok(SessionLease { _peers: peers })
    }
}

pub(crate) struct AgentService<S, E> {
    store: Arc<S>,
    runtime: Arc<PodRuntime>,
    data_plane: RuntimeDataPlane,
    coordinator: Arc<Coordinator<S, E>>,
    reporter: Arc<AgentReporter<S>>,
    sessions: Arc<SessionRegistry>,
    bearer_token: Arc<str>,
    ready_state: Arc<AtomicBool>,
}

impl<S, E> Clone for AgentService<S, E> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            runtime: self.runtime.clone(),
            data_plane: self.data_plane.clone(),
            coordinator: self.coordinator.clone(),
            reporter: self.reporter.clone(),
            sessions: self.sessions.clone(),
            bearer_token: self.bearer_token.clone(),
            ready_state: self.ready_state.clone(),
        }
    }
}

impl<S, E> AgentService<S, E>
where
    S: AgentStore + 'static,
    E: RuntimeEffectExecutor + 'static,
{
    pub(crate) fn new(
        store: Arc<S>,
        runtime: Arc<PodRuntime>,
        executor: Arc<E>,
        bearer_token: impl Into<Arc<str>>,
    ) -> Result<Self> {
        let bearer_token = bearer_token.into();
        if bearer_token.is_empty() {
            return Err(crate::host::HostError::CommandRejected(
                "agent bearer token must not be empty".into(),
            ));
        }
        let reporter = Arc::new(AgentReporter::new(store.clone()));
        let sessions = Arc::new(SessionRegistry::new(reporter.session().id().clone()));
        Ok(Self {
            coordinator: Arc::new(Coordinator::new(store.clone(), executor)),
            store,
            data_plane: runtime.data_plane(),
            runtime,
            reporter,
            sessions,
            bearer_token,
            ready_state: Arc::new(AtomicBool::new(false)),
        })
    }

    pub(crate) fn sessions(&self) -> &Arc<SessionRegistry> {
        &self.sessions
    }

    pub(crate) async fn serve(
        self,
        control_address: SocketAddr,
        replication_address: SocketAddr,
        ready: watch::Sender<bool>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let control_listener = TcpListener::bind(control_address).await?;
        let replication_listener = TcpListener::bind(replication_address).await?;

        let control_service = self.clone();
        let peer_service = self.clone();
        let replication_service = self.clone();
        let mut control_shutdown = shutdown.clone();
        let mut replication_shutdown = shutdown.clone();
        let mut control = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(proto::agent_control_server::AgentControlServer::new(
                    control_service,
                ))
                .add_service(proto::replica_peer_server::ReplicaPeerServer::new(
                    peer_service,
                ))
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(control_listener),
                    async move {
                        wait_for_shutdown(&mut control_shutdown).await;
                    },
                )
                .await
        });
        let mut replication = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(proto::replication_data_server::ReplicationDataServer::new(
                    replication_service,
                ))
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(replication_listener),
                    async move {
                        wait_for_shutdown(&mut replication_shutdown).await;
                    },
                )
                .await
        });

        let startup = self.reconstruct_runtime();
        tokio::pin!(startup);
        let startup_result = tokio::select! {
            biased;
            result = &mut startup => result,
            _ = wait_for_shutdown(&mut shutdown) => {
                Err(crate::host::HostError::Runtime(
                    crate::RuntimeError::OperationCancelled,
                ))
            }
        };
        if let Err(error) = startup_result {
            control.abort();
            replication.abort();
            let _ = tokio::join!(&mut control, &mut replication);
            let persisted = self.persist_partition_fault().await;
            self.runtime.abort();
            if let Err(persisted) = persisted {
                return Err(crate::host::HostError::CommandRejected(format!(
                    "{persisted}; startup: {error}"
                )));
            }
            return Err(error);
        }

        self.ready_state.store(true, Ordering::Release);
        ready.send_replace(true);
        let recovery_coordinator = self.coordinator.clone();
        let recovery_task = tokio::spawn(async move {
            if let Err(error) = recovery_coordinator.resume_configuration().await {
                tracing::warn!(%error, "background configuration recovery stopped");
            }
        });
        let result = tokio::select! {
            result = &mut control => {
                replication.abort();
                let _ = (&mut replication).await;
                flatten_server_result(result)
            }
            result = &mut replication => {
                control.abort();
                let _ = (&mut control).await;
                flatten_server_result(result)
            }
            _ = wait_for_shutdown(&mut shutdown) => {
                self.ready_state.store(false, Ordering::Release);
                ready.send_replace(false);
                // A retained HTTP/2 stream must not delay the durable shutdown
                // acknowledgement. Interrupted command intents are replayable.
                control.abort();
                replication.abort();
                let _ = tokio::join!(&mut control, &mut replication);
                Ok(())
            }
        };
        self.ready_state.store(false, Ordering::Release);
        ready.send_replace(false);
        recovery_task.abort();
        let _ = recovery_task.await;
        let persisted = self.persist_partition_fault().await;
        self.runtime.abort();
        persisted?;
        result
    }

    async fn persist_partition_fault(&self) -> Result<()> {
        tokio::time::timeout(SHUTDOWN_TIMEOUT, async {
            let partition = self.runtime.partition_report().await;
            if partition.reported_fault.is_some() {
                self.store
                    .record_partition_reports(partition.load_metrics, partition.reported_fault)
                    .await?;
            }
            Ok(())
        })
        .await
        .map_err(|_| {
            crate::host::HostError::CommandRejected("partition fault persistence timed out".into())
        })?
    }

    pub(crate) async fn reconstruct_runtime(&self) -> Result<()> {
        let state = self.store.load_state().await?;
        self.runtime.bind_replica_session(
            state.identity.resource_uid.clone(),
            self.sessions.local_session().clone(),
        )?;
        let transition = startup_transition(&state);
        let removal_pending = state.pending_effect.as_ref().is_some_and(|p| {
            matches!(
                p.effect.action,
                crate::effects::RuntimeEffectAction::PrepareSecondaryRemoval { .. }
                    | crate::effects::RuntimeEffectAction::RetireReplica(_)
            )
        }) || state.reconfiguration.as_ref().is_some_and(|r| {
            r.command.transition_kind == crate::protocol::types::TransitionKind::SecondaryScaleDown
        });
        let planned_switchover_pending = state.reconfiguration.as_ref().is_some_and(|record| {
            record.command.transition_kind
                == crate::protocol::types::TransitionKind::PlannedSwitchover
        });
        self.runtime
            .reconstruct(
                if state
                    .application_storage
                    .as_ref()
                    .is_some_and(|b| b.initializing)
                {
                    OpenMode::New
                } else {
                    OpenMode::Existing
                },
                state.role,
                if removal_pending {
                    crate::protocol::types::AccessStatus::ReconfigurationPending
                } else {
                    state.read_status
                },
                startup_write_status(
                    state.write_status,
                    state
                        .pending_effect
                        .as_ref()
                        .map(|pending| &pending.effect.action),
                    removal_pending || planned_switchover_pending,
                ),
                transition,
            )
            .await?;
        // Consume creation permission before readiness permits any authority/access commands.
        if state
            .application_storage
            .as_ref()
            .is_some_and(|b| b.initializing)
        {
            self.store.complete_application_initialization().await?;
        }
        if let Some(committed) = state.accepted_secondary_removal
            && self
                .runtime
                .snapshot()
                .await
                .authority
                .as_ref()
                .is_some_and(|a| {
                    a.previous_configuration.is_none()
                        && a.secondary_removal.as_ref() == Some(&committed.evidence)
                })
        {
            let operation = committed.evidence.preparation.intent.command_operation_id(
                crate::protocol::types::SecondaryRemovalStage::AcceptCommit,
                &state.identity.local_identity,
            );
            let historical = state.removal_effects.get(&operation).and_then(|retained| {
                match &retained.effect.action {
                    crate::effects::RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(
                        command,
                    ) if command.committed == committed => Some(*command.clone()),
                    _ => None,
                }
            });
            self.runtime
                .restore_accepted_removal(committed, historical)
                .await?;
        }
        if let Some(pending) = state.pending_effect.as_ref()
            && matches!(
                pending.effect.action,
                crate::effects::RuntimeEffectAction::Open(_)
            )
        {
            self.store.mark_effect_applied(&pending.effect).await?;
            self.store
                .complete_effect(&crate::effects::RuntimeEffectResult {
                    operation_id: pending.effect.operation_id.clone(),
                    sequence: pending.effect.sequence,
                    topology_receipt: None,
                    postcondition: self.runtime.snapshot().await.into(),
                })
                .await?;
        }
        let pending_acceptance = state.pending_effect.as_ref().is_some_and(|pending| {
            matches!(
                pending.effect.action,
                crate::effects::RuntimeEffectAction::AcceptSecondaryRemovalCommit(_)
            )
        });
        let pending_catchup = state.pending_effect.as_ref().is_some_and(|pending| {
            matches!(
                pending.effect.action,
                crate::effects::RuntimeEffectAction::WaitForCatchup
            )
        });
        let pending_abandoned_build = state.pending_effect.as_ref().is_some_and(|pending| {
            matches!(
                &pending.effect.action,
                crate::effects::RuntimeEffectAction::BuildReplica {
                    build_id,
                    ..
                } if state.abandoned_builds.contains(build_id)
            )
        });
        if !pending_catchup
            && !pending_abandoned_build
            && let Err(error) = self.coordinator.resume_pending().await
            && !(pending_acceptance
                && matches!(
                    error,
                    crate::host::HostError::Runtime(crate::RuntimeError::ReconfigurationPending)
                ))
        {
            return Err(error);
        }
        let build_recovery = self.store.load_state().await?;
        if let Some(retained) = build_recovery.retained_result.as_ref()
            && let crate::effects::RuntimeEffectAction::AdmitBuildAuthority(authority) =
                &retained.effect.action
            && !build_recovery.retired_builds.contains(&authority.build_id)
        {
            self.runtime.apply_effect(retained.effect.clone()).await?;
        }
        for command in build_recovery.build_commands.values().cloned() {
            if !build_recovery
                .retired_builds
                .contains(&command.operation_id)
            {
                if build_recovery
                    .abandoned_builds
                    .contains(&command.operation_id)
                {
                    self.coordinator
                        .ensure_build(EnsureReplicaBuild {
                            retire: true,
                            ..command
                        })
                        .await?;
                    continue;
                }
                if command.authority.is_none() {
                    continue;
                }
                self.coordinator.ensure_build(command).await?;
            }
        }
        Ok(())
    }

    fn require_ready(&self) -> std::result::Result<(), Status> {
        if self.ready_state.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(Status::unavailable("agent runtime is not ready"))
        }
    }

    fn authorize<T>(&self, request: &Request<T>) -> std::result::Result<(), Status> {
        let expected = format!("Bearer {}", self.bearer_token);
        let observed = request
            .metadata()
            .get(AUTHORIZATION_HEADER)
            .and_then(|value| value.to_str().ok());
        if observed != Some(expected.as_str()) {
            return Err(Status::unauthenticated("invalid agent credentials"));
        }
        Ok(())
    }

    async fn get_status_inner(
        &self,
        request: proto::GetAgentStatusRequest,
    ) -> std::result::Result<proto::AgentStatusReport, Status> {
        if request.protocol_version != crate::protocol::PROTOCOL_VERSION {
            return Err(Status::failed_precondition("unsupported protocol version"));
        }
        let state = self.store.load_state().await.map_err(status_from_agent)?;
        if request.resource_uid != state.identity.resource_uid.as_str()
            || request.replica_id != state.identity.local_identity.replica_id.value()
            || request.expected_instance_id != state.identity.local_identity.instance_id.as_str()
        {
            return Err(Status::failed_precondition(
                "status request targets another replica incarnation",
            ));
        }
        let report_runtime = self.runtime.report_runtime();
        self.reporter
            .report(&report_runtime)
            .await
            .map_err(|error| match error {
                crate::host::HostError::DurableEffectConflict(message) => {
                    Status::unavailable(message)
                }
                other => status_from_agent(other),
            })
    }

    async fn execute_inner(
        &self,
        request: proto::ExecuteCommandRequest,
    ) -> std::result::Result<proto::ExecuteCommandResponse, Status> {
        let command = normalize_execute_request(request)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if command.expected_process_session_id != *self.reporter.session().id() {
            return Err(Status::failed_precondition(
                "command targets a stale agent process session",
            ));
        }
        let state = self.store.load_state().await.map_err(status_from_agent)?;
        if command.resource_uid != state.identity.resource_uid
            || command.target != state.identity.local_identity
        {
            return Err(Status::failed_precondition(
                "command targets another durable replica identity",
            ));
        }
        match command.command {
            ProtocolCommand::AcceptSecondaryRemovalCommit(command) => {
                Box::pin(self.coordinator.accept_secondary_removal_commit(*command))
                    .await
                    .map_err(status_from_agent)?;
            }
            ProtocolCommand::PrepareSecondaryRemoval(command) => {
                self.coordinator
                    .ensure_secondary_removal_prepared(
                        *command,
                        self.reporter.session().id().clone(),
                        self.reporter.session().next_report_sequence(),
                    )
                    .await
                    .map_err(status_from_agent)?;
            }
            ProtocolCommand::RetireReplica(command) => {
                self.coordinator
                    .ensure_replica_retired(
                        *command,
                        self.reporter.session().id().clone(),
                        self.reporter.session().next_report_sequence(),
                    )
                    .await
                    .map_err(status_from_agent)?;
            }
            ProtocolCommand::InitializeAgentStore(initialization) => {
                if initialization.initialization_id != state.identity.initialization_id
                    || initialization.resource_uid != state.identity.resource_uid
                    || initialization.local_replica_id != state.identity.local_identity.replica_id
                    || initialization.expected_instance_id
                        != state.identity.local_identity.instance_id
                    || initialization.expected_pod_uid != state.identity.pod_uid
                    || initialization.expected_pvc_uid != state.identity.pvc_uid
                    || initialization.assigned_agent_generation
                        != state.identity.local_identity.agent_generation
                    || initialization.effective_policy != state.identity.effective_policy
                    || initialization
                        .provisioning
                        .as_ref()
                        .filter(|provisioning| provisioning.scale_up().is_some())
                        != state.scale_up_initialization.as_ref()
                {
                    return Err(Status::already_exists(
                        "agent store is initialized with different authority",
                    ));
                }
            }
            ProtocolCommand::EnsureConfiguration(command) => {
                let coordinator = self.coordinator.clone();
                tokio::spawn(async move { coordinator.ensure_configuration(*command).await })
                    .await
                    .map_err(|error| Status::internal(error.to_string()))?
                    .map_err(status_from_agent)?;
            }
            ProtocolCommand::PrepareSwitchover(command) => {
                self.coordinator
                    .ensure_switchover_prepared(*command)
                    .await
                    .map_err(status_from_agent)?;
            }
            ProtocolCommand::EnsureReplicaBuild(command) => {
                if let (Some(authority), Some(source_session_id)) =
                    (&command.authority, &command.source_session_id)
                {
                    self.sessions
                        .register_peer(authority.source.clone(), source_session_id.clone())
                        .await;
                    self.runtime
                        .register_peer_session(authority.source.clone(), source_session_id.clone())
                        .await
                        .map_err(status_from_runtime)?;
                }
                let coordinator = self.coordinator.clone();
                tokio::spawn(async move { coordinator.ensure_build(*command).await })
                    .await
                    .map_err(|error| Status::internal(error.to_string()))?
                    .map_err(status_from_agent)?;
            }
        }
        let report_runtime = self.runtime.report_runtime();
        let observation = self
            .reporter
            .report(&report_runtime)
            .await
            .map_err(status_from_agent)?;
        crate::control::validate_agent_status_report(&observation)
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(proto::ExecuteCommandResponse {
            protocol_version: crate::protocol::PROTOCOL_VERSION,
            observation: Some(observation),
        })
    }
}

fn startup_write_status(
    persisted: crate::protocol::types::AccessStatus,
    pending: Option<&crate::effects::RuntimeEffectAction>,
    lifecycle_pending: bool,
) -> crate::protocol::types::AccessStatus {
    if lifecycle_pending
        || pending.is_some_and(|action| {
            matches!(
                action,
                crate::effects::RuntimeEffectAction::PrepareSwitchover { .. }
                    | crate::effects::RuntimeEffectAction::PrepareSecondaryRemoval { .. }
                    | crate::effects::RuntimeEffectAction::RetireReplica(_)
            )
        })
    {
        crate::protocol::types::AccessStatus::ReconfigurationPending
    } else {
        persisted
    }
}

fn startup_transition(
    state: &AgentState,
) -> Option<(crate::protocol::types::ReplicaRole, bool, bool)> {
    if let Some(pending) = &state.pending_effect {
        match pending.effect.action {
            crate::effects::RuntimeEffectAction::ChangeRole(role)
            | crate::effects::RuntimeEffectAction::ChangeReplicatorRole(role) => {
                return Some((role, false, false));
            }
            _ => {}
        }
    }
    let record = state.reconfiguration.as_ref()?;
    let target_role = record
        .command
        .current_configuration
        .members
        .iter()
        .find(|member| member.identity == state.identity.local_identity)
        .map(|member| member.role)?;
    // A planned demotion may already have replaced primary authority. Restore
    // only the fenced target replicator; the certified handoff needs no further
    // old-primary catch-up and the journal must finish the application transition.
    if record.command.transition_kind == crate::protocol::types::TransitionKind::PlannedSwitchover
        && state.role == crate::protocol::types::ReplicaRole::Primary
        && target_role != crate::protocol::types::ReplicaRole::Primary
    {
        return Some((target_role, false, false));
    }
    if let Some(retained) = state.retained_result.as_ref() {
        match &retained.effect.action {
            crate::effects::RuntimeEffectAction::ChangeReplicatorRole(role)
                if *role == target_role =>
            {
                return Some((target_role, false, false));
            }
            crate::effects::RuntimeEffectAction::UpdateEpoch => {
                return Some((target_role, true, false));
            }
            crate::effects::RuntimeEffectAction::ChangeApplicationRole(role)
                if *role == target_role =>
            {
                return Some((target_role, true, true));
            }
            _ => {}
        }
    }
    match record.stage {
        CoordinatorStage::Epoch => Some((target_role, false, false)),
        CoordinatorStage::ApplicationRole => Some((target_role, true, false)),
        _ => None,
    }
}

fn flatten_server_result(
    result: std::result::Result<
        std::result::Result<(), tonic::transport::Error>,
        tokio::task::JoinError,
    >,
) -> Result<()> {
    result
        .map_err(|error| crate::host::HostError::CommandRejected(error.to_string()))?
        .map_err(|error| crate::host::HostError::CommandRejected(error.to_string()))
}

async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

#[tonic::async_trait]
impl<S, E> proto::agent_control_server::AgentControl for AgentService<S, E>
where
    S: AgentStore + 'static,
    E: RuntimeEffectExecutor + 'static,
{
    async fn get_status(
        &self,
        request: Request<proto::GetAgentStatusRequest>,
    ) -> std::result::Result<Response<proto::AgentStatusReport>, Status> {
        self.authorize(&request)?;
        Ok(Response::new(
            self.get_status_inner(request.into_inner()).await?,
        ))
    }

    async fn execute(
        &self,
        request: Request<proto::ExecuteCommandRequest>,
    ) -> std::result::Result<Response<proto::ExecuteCommandResponse>, Status> {
        self.authorize(&request)?;
        self.require_ready()?;
        Ok(Response::new(
            self.execute_inner(request.into_inner()).await?,
        ))
    }
}

#[tonic::async_trait]
impl<S, E> proto::replica_peer_server::ReplicaPeer for AgentService<S, E>
where
    S: AgentStore + 'static,
    E: RuntimeEffectExecutor + 'static,
{
    async fn get_status(
        &self,
        request: Request<proto::GetAgentStatusRequest>,
    ) -> std::result::Result<Response<proto::AgentStatusReport>, Status> {
        self.authorize(&request)?;
        Ok(Response::new(
            self.get_status_inner(request.into_inner()).await?,
        ))
    }

    async fn execute(
        &self,
        request: Request<proto::ExecuteCommandRequest>,
    ) -> std::result::Result<Response<proto::ExecuteCommandResponse>, Status> {
        self.authorize(&request)?;
        self.require_ready()?;
        Ok(Response::new(
            self.execute_inner(request.into_inner()).await?,
        ))
    }
}

type ReplicationResponseStream =
    Pin<Box<dyn Stream<Item = std::result::Result<proto::ReplicationAck, Status>> + Send>>;
type CopyResponseStream =
    Pin<Box<dyn Stream<Item = std::result::Result<proto::CopyAck, Status>> + Send>>;

#[tonic::async_trait]
impl<S, E> proto::replication_data_server::ReplicationData for AgentService<S, E>
where
    S: AgentStore + 'static,
    E: RuntimeEffectExecutor + 'static,
{
    type ReplicateStream = ReplicationResponseStream;
    type BuildStream = CopyResponseStream;

    async fn replicate(
        &self,
        request: Request<tonic::Streaming<proto::ReplicationItem>>,
    ) -> std::result::Result<Response<Self::ReplicateStream>, Status> {
        self.authorize(&request)?;
        self.require_ready()?;
        let mut incoming = request.into_inner();
        let data_plane = self.data_plane.clone();
        let sessions = self.sessions.clone();
        let (sender, receiver) = tokio::sync::mpsc::channel(32);
        tokio::spawn(async move {
            while let Some(item) = incoming.next().await {
                let result = async {
                    let item = item?;
                    let sender_identity: ReplicaIdentity = item
                        .sender
                        .clone()
                        .ok_or_else(|| Status::invalid_argument("missing sender"))?
                        .try_into()
                        .map_err(|error: crate::control::WireError| {
                            Status::invalid_argument(error.to_string())
                        })?;
                    let _lease = sessions
                        .validate_peer(
                            &sender_identity,
                            &item.sender_session_id,
                            &item.receiver_session_id,
                        )
                        .await?;
                    let sender_session_id = item.sender_session_id.clone();
                    let receiver_session_id = item.receiver_session_id.clone();
                    let mut acknowledgement = data_plane
                        .receive_replication(item)
                        .await
                        .map_err(status_from_runtime)?
                        .applied()
                        .await
                        .map_err(status_from_runtime)?;
                    acknowledgement.sender_session_id = sender_session_id;
                    acknowledgement.receiver_session_id = receiver_session_id;
                    Ok(acknowledgement)
                }
                .await;
                if sender.send(result).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    async fn build(
        &self,
        request: Request<tonic::Streaming<proto::CopyItem>>,
    ) -> std::result::Result<Response<Self::BuildStream>, Status> {
        self.authorize(&request)?;
        self.require_ready()?;
        let mut incoming = request.into_inner();
        let data_plane = self.data_plane.clone();
        let sessions = self.sessions.clone();
        let (sender, receiver) = tokio::sync::mpsc::channel(32);
        tokio::spawn(async move {
            while let Some(item) = incoming.next().await {
                let result = async {
                    let item = item?;
                    let sender_identity: ReplicaIdentity = item
                        .sender
                        .clone()
                        .ok_or_else(|| Status::invalid_argument("missing sender"))?
                        .try_into()
                        .map_err(|error: crate::control::WireError| {
                            Status::invalid_argument(error.to_string())
                        })?;
                    let _lease = sessions
                        .validate_peer(
                            &sender_identity,
                            &item.sender_session_id,
                            &item.receiver_session_id,
                        )
                        .await?;
                    let sender_session_id = item.sender_session_id.clone();
                    let receiver_session_id = item.receiver_session_id.clone();
                    let mut acknowledgement = data_plane
                        .receive_copy_item(item)
                        .await
                        .map_err(status_from_runtime)?;
                    acknowledgement.sender_session_id = sender_session_id;
                    acknowledgement.receiver_session_id = receiver_session_id;
                    Ok(acknowledgement)
                }
                .await;
                if sender.send(result).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

fn status_from_agent(error: crate::host::HostError) -> Status {
    match error {
        crate::host::HostError::DurableEffectConflict(_)
        | crate::host::HostError::CommandRejected(_) => {
            Status::failed_precondition(error.to_string())
        }
        crate::host::HostError::SessionRejected(_) => {
            Status::failed_precondition(error.to_string())
        }
        crate::host::HostError::Backpressure(_) => Status::resource_exhausted(error.to_string()),
        crate::host::HostError::Runtime(error) => status_from_runtime(error),
        _ => Status::internal(error.to_string()),
    }
}

fn status_from_runtime(error: crate::RuntimeError) -> Status {
    match error {
        RuntimeError::OperationCancelled => Status::cancelled(error.to_string()),
        RuntimeError::QueueFull => Status::resource_exhausted(error.to_string()),
        RuntimeError::ReplicaRemoved(_) | RuntimeError::ReconfigurationPending => {
            Status::unavailable(error.to_string())
        }
        RuntimeError::AuthorityMismatch(_)
        | RuntimeError::AuthorityNotAdmitted
        | RuntimeError::InvalidReplication(_)
        | RuntimeError::NotPrimary
        | RuntimeError::WriteClosed(_) => Status::failed_precondition(error.to_string()),
        RuntimeError::Closed | RuntimeError::NotOpen => Status::unavailable(error.to_string()),
        _ => Status::internal(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::{
        AccessStatus, AgentGeneration, ConfigurationDescriptor, ConfigurationId,
        ConfigurationMember, EffectivePolicy, Epoch, OperationId, PodUid, ProcessSessionId, PvcUid,
        ReplicaIdentity, ReplicaInstanceId, ReplicaRole, ResourceUid, ScaleUpConfigurationEvidence,
        ScaleUpIntent, SwitchoverRequestId, TransitionKind,
    };

    #[test]
    fn uninitialized_report_is_unsafe_when_application_state_survives() {
        let directory = crate::host::tests::tempdir().unwrap();
        let (initialized, _) = watch::channel(false);
        let service = InitializationService::new(
            ObservedStorageIdentity {
                resource_uid: ResourceUid::new("resource"),
                pod_uid: PodUid::new("pod"),
                pvc_uid: PvcUid::new("pvc"),
                instance_id: ReplicaInstanceId::new("pod"),
            },
            ReplicaId::new(1),
            SqliteStore::metadata_database_path(directory.path()),
            "token",
            initialized,
            false,
        )
        .unwrap();

        let report = service.report();
        assert_eq!(
            report.storage_state,
            proto::AgentStorageState::Unsafe as i32
        );
        assert!(report.storage_error.contains("application state exists"));
    }

    #[test]
    fn pending_switchover_preparation_reconstructs_write_closed() {
        let action = crate::effects::RuntimeEffectAction::PrepareSwitchover {
            preparation_generation: 1,
            request_id: SwitchoverRequestId::new("request-1"),
            source: ReplicaIdentity {
                replica_id: ReplicaId::new(1),
                instance_id: ReplicaInstanceId::new("pod-1"),
                agent_generation: AgentGeneration::new("generation-1"),
            },
            target: ReplicaIdentity {
                replica_id: ReplicaId::new(2),
                instance_id: ReplicaInstanceId::new("pod-2"),
                agent_generation: AgentGeneration::new("generation-2"),
            },
            starting_configuration_id: ConfigurationId::new("configuration-1"),
            starting_epoch: crate::protocol::types::Epoch::new(0, 1),
        };
        assert_eq!(
            startup_write_status(
                crate::protocol::types::AccessStatus::Granted,
                Some(&action),
                false,
            ),
            crate::protocol::types::AccessStatus::ReconfigurationPending
        );
    }

    #[test]
    fn scale_up_pending_admission_preserves_the_persisted_write_grant() {
        let primary = ReplicaIdentity {
            replica_id: ReplicaId::new(1),
            instance_id: ReplicaInstanceId::new("pod-1"),
            agent_generation: AgentGeneration::new("generation-1"),
        };
        let target = ReplicaIdentity {
            replica_id: ReplicaId::new(2),
            instance_id: ReplicaInstanceId::new("pod-2"),
            agent_generation: AgentGeneration::new("generation-2"),
        };
        let previous_policy = EffectivePolicy::fixed(1, 30).unwrap();
        let current_policy = EffectivePolicy::fixed(2, 30).unwrap();
        let previous = ConfigurationDescriptor::new(
            Epoch::new(0, 1),
            primary.replica_id,
            vec![ConfigurationMember {
                identity: primary.clone(),
                role: ReplicaRole::Primary,
            }],
            previous_policy.write_quorum,
        );
        let current = ConfigurationDescriptor::new(
            Epoch::new(0, 2),
            primary.replica_id,
            vec![
                ConfigurationMember {
                    identity: primary.clone(),
                    role: ReplicaRole::Primary,
                },
                ConfigurationMember {
                    identity: target.clone(),
                    role: ReplicaRole::ActiveSecondary,
                },
            ],
            current_policy.write_quorum,
        );
        let mut intent = ScaleUpIntent {
            operation_id: OperationId::default(),
            resource_uid: ResourceUid::new("set"),
            spec_generation: 2,
            desired_replicas: 2,
            previous_configuration: previous.clone(),
            current_configuration: current.clone(),
            previous_policy,
            current_policy,
            primary: primary.clone(),
            target,
            build_id: OperationId::new("build"),
            snapshot_boundary_lsn: 0,
            catch_up_boundary_lsn: 0,
        };
        intent.operation_id = intent.expected_operation_id();
        let pending = crate::effects::RuntimeEffectAction::AdmitAuthority(Box::new(
            crate::authority::AdmittedAuthority {
                local_identity: primary,
                transition_kind: Some(TransitionKind::ScaleUp),
                previous_configuration: Some(previous),
                current_configuration: current,
                switchover_handoff: None,
                secondary_removal: None,
                scale_up: Some(Box::new(ScaleUpConfigurationEvidence::Admission { intent })),
            },
        ));
        assert_eq!(
            startup_write_status(AccessStatus::Granted, Some(&pending), false),
            AccessStatus::Granted
        );
    }

    #[tokio::test]
    async fn scale_up_carried_failover_rejects_old_primary_and_receiver_sessions() {
        let old_primary = ReplicaIdentity {
            replica_id: ReplicaId::new(1),
            instance_id: ReplicaInstanceId::new("old-primary"),
            agent_generation: AgentGeneration::new("old-primary-generation"),
        };
        let new_primary = ReplicaIdentity {
            replica_id: ReplicaId::new(2),
            instance_id: ReplicaInstanceId::new("new-primary"),
            agent_generation: AgentGeneration::new("new-primary-generation"),
        };
        let registry = SessionRegistry::new(ProcessSessionId::new("receiver-current"));
        registry
            .register_peer(
                new_primary.clone(),
                ProcessSessionId::new("new-primary-current"),
            )
            .await;
        assert!(
            registry
                .validate_peer(
                    &old_primary,
                    "old-primary-retired",
                    registry.local_session().as_str()
                )
                .await
                .is_err()
        );
        assert!(
            registry
                .validate_peer(&new_primary, "new-primary-current", "receiver-retired")
                .await
                .is_err()
        );
        assert!(
            registry
                .validate_peer(
                    &new_primary,
                    "new-primary-current",
                    registry.local_session().as_str()
                )
                .await
                .is_ok()
        );
    }
}
