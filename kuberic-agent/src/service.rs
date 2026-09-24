//! Tonic control, peer, and replication services.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::{Stream, StreamExt};
use kuberic_protocol::command::ProtocolCommand;
use kuberic_protocol::types::{
    ProcessSessionId, ReplicaId, ReplicaIdentity, TransitionIntent, TransitionKind,
    derive_transition_id,
};
use kuberic_runtime::application::OpenMode;
use kuberic_wire::{normalize_execute_request, proto};
use tokio::net::TcpListener;
use tokio::sync::{OwnedRwLockReadGuard, RwLock, watch};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response, Status};

use crate::coordinator::Coordinator;
use crate::hosting::{PodRuntime, RuntimeDataPlane};
use crate::provisioning::{InitializationAuthority, ObservedStorageIdentity};
use crate::report::AgentReporter;
use crate::runtime_adapter::RuntimeEffectExecutor;
use crate::session::ProcessSession;
use crate::sqlite_store::SqliteStore;
use crate::state::{AgentState, CoordinatorStage};
use crate::store::AgentStore;
use crate::{AgentError, Result};

const AUTHORIZATION_HEADER: &str = "authorization";

pub struct InitializationService {
    observed: ObservedStorageIdentity,
    replica_id: ReplicaId,
    database_path: PathBuf,
    bearer_token: Arc<str>,
    session: Arc<ProcessSession>,
    initialized: watch::Sender<bool>,
    fresh_application_state: bool,
}

impl InitializationService {
    pub fn new(
        observed: ObservedStorageIdentity,
        replica_id: ReplicaId,
        database_path: PathBuf,
        bearer_token: impl Into<Arc<str>>,
        initialized: watch::Sender<bool>,
        fresh_application_state: bool,
    ) -> Result<Self> {
        let bearer_token = bearer_token.into();
        if bearer_token.is_empty() {
            return Err(AgentError::CommandRejected(
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
        })
    }

    pub async fn serve(
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
            .map_err(|error| AgentError::CommandRejected(error.to_string()));
        ready.send_replace(false);
        result
    }

    fn authorize<T>(&self, request: &Request<T>) -> std::result::Result<(), Status> {
        authorize_request(request, &self.bearer_token)
    }

    fn report(&self) -> proto::AgentStatusReport {
        if !self.fresh_application_state {
            return proto::AgentStatusReport {
                protocol_version: kuberic_protocol::PROTOCOL_VERSION,
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
            protocol_version: kuberic_protocol::PROTOCOL_VERSION,
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
        if request.protocol_version != kuberic_protocol::PROTOCOL_VERSION {
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
        };
        let authority = command.provisioning.as_ref().map_or(
            InitializationAuthority::Bootstrap(&transition),
            InitializationAuthority::Replacement,
        );
        let identity = crate::command::admit_initialization(&command, &self.observed, authority)
            .map_err(status_from_agent)?;
        match SqliteStore::create_authorized(&self.database_path, AgentState::new(identity.clone()))
        {
            Ok(store) => drop(store),
            Err(AgentError::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                SqliteStore::open_existing(&self.database_path, Some(&identity))
                    .map_err(status_from_agent)?;
            }
            Err(error) => return Err(status_from_agent(error)),
        }
        self.initialized.send_replace(true);
        Ok(proto::ExecuteCommandResponse {
            protocol_version: kuberic_protocol::PROTOCOL_VERSION,
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

pub struct SessionRegistry {
    local_session: ProcessSessionId,
    peers: Arc<RwLock<BTreeMap<ReplicaIdentity, ProcessSessionId>>>,
}

pub struct SessionLease {
    _peers: OwnedRwLockReadGuard<BTreeMap<ReplicaIdentity, ProcessSessionId>>,
}

impl SessionRegistry {
    pub fn new(local_session: ProcessSessionId) -> Self {
        Self {
            local_session,
            peers: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn local_session(&self) -> &ProcessSessionId {
        &self.local_session
    }

    pub async fn register_peer(&self, identity: ReplicaIdentity, session: ProcessSessionId) {
        self.peers.write().await.insert(identity, session);
    }

    pub async fn validate_peer(
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

pub struct AgentService<S, E> {
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
    pub fn new(
        store: Arc<S>,
        runtime: Arc<PodRuntime>,
        executor: Arc<E>,
        bearer_token: impl Into<Arc<str>>,
    ) -> Result<Self> {
        let bearer_token = bearer_token.into();
        if bearer_token.is_empty() {
            return Err(AgentError::CommandRejected(
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

    pub fn sessions(&self) -> &Arc<SessionRegistry> {
        &self.sessions
    }

    pub async fn serve(
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
            result = &mut startup => result,
            _ = wait_for_shutdown(&mut shutdown) => {
                Err(AgentError::Runtime(kuberic_runtime::RuntimeError::OperationCancelled))
            }
        };
        if let Err(error) = startup_result {
            self.runtime.abort();
            control.abort();
            replication.abort();
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
                flatten_server_result(result)
            }
            result = &mut replication => {
                control.abort();
                flatten_server_result(result)
            }
            _ = wait_for_shutdown(&mut shutdown) => {
                let results = tokio::join!(&mut control, &mut replication);
                flatten_server_result(results.0).and_then(|_| flatten_server_result(results.1))
            }
        };
        self.ready_state.store(false, Ordering::Release);
        ready.send_replace(false);
        recovery_task.abort();
        self.runtime.abort();
        result
    }

    async fn reconstruct_runtime(&self) -> Result<()> {
        let state = self.store.load_state().await?;
        let transition = startup_transition(&state);
        self.runtime
            .reconstruct(
                OpenMode::Existing,
                state.role,
                state.read_status,
                state.write_status,
                transition,
            )
            .await?;
        if let Some(pending) = state.pending_effect.as_ref()
            && matches!(
                pending.effect.action,
                kuberic_runtime_internal::effects::RuntimeEffectAction::Open(_)
            )
        {
            self.store.mark_effect_applied(&pending.effect).await?;
            self.store
                .complete_effect(&kuberic_runtime_internal::effects::RuntimeEffectResult {
                    operation_id: pending.effect.operation_id.clone(),
                    sequence: pending.effect.sequence,
                    postcondition: self.runtime.snapshot().await.into(),
                })
                .await?;
        }
        if let Some(retained) = state.retained_result.as_ref()
            && matches!(
                retained.effect.action,
                kuberic_runtime_internal::effects::RuntimeEffectAction::AdmitBuildAuthority(_)
            )
        {
            self.runtime.apply_effect(retained.effect.clone()).await?;
        }
        let pending_catchup = state.pending_effect.as_ref().is_some_and(|pending| {
            matches!(
                pending.effect.action,
                kuberic_runtime_internal::effects::RuntimeEffectAction::WaitForCatchup
            )
        });
        if !pending_catchup {
            self.coordinator.resume_pending().await?;
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
        if request.protocol_version != kuberic_protocol::PROTOCOL_VERSION {
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
        self.reporter
            .report(&self.runtime)
            .await
            .map_err(|error| match error {
                AgentError::EffectConflict(message) => Status::unavailable(message),
                other => status_from_agent(other),
            })
    }

    async fn execute_inner(
        &self,
        request: proto::ExecuteCommandRequest,
    ) -> std::result::Result<proto::ExecuteCommandResponse, Status> {
        let command = normalize_execute_request(request)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let state = self.store.load_state().await.map_err(status_from_agent)?;
        if command.resource_uid != state.identity.resource_uid
            || command.target != state.identity.local_identity
        {
            return Err(Status::failed_precondition(
                "command targets another durable replica identity",
            ));
        }
        match command.command {
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
                {
                    return Err(Status::already_exists(
                        "agent store is initialized with different authority",
                    ));
                }
            }
            ProtocolCommand::EnsureConfiguration(command) => {
                self.coordinator
                    .ensure_configuration(*command)
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
                }
                self.coordinator
                    .ensure_build(*command)
                    .await
                    .map_err(status_from_agent)?;
            }
        }
        Ok(proto::ExecuteCommandResponse {
            protocol_version: kuberic_protocol::PROTOCOL_VERSION,
            observation: Some(
                self.reporter
                    .report(&self.runtime)
                    .await
                    .map_err(status_from_agent)?,
            ),
        })
    }
}

fn startup_transition(
    state: &AgentState,
) -> Option<(kuberic_protocol::types::ReplicaRole, bool, bool)> {
    let record = state.reconfiguration.as_ref()?;
    let target_role = record
        .command
        .current_configuration
        .members
        .iter()
        .find(|member| member.identity == state.identity.local_identity)
        .map(|member| member.role)?;
    if let Some(retained) = state.retained_result.as_ref() {
        match &retained.effect.action {
            kuberic_runtime_internal::effects::RuntimeEffectAction::ChangeReplicatorRole(role)
                if *role == target_role =>
            {
                return Some((target_role, false, false));
            }
            kuberic_runtime_internal::effects::RuntimeEffectAction::UpdateEpoch => {
                return Some((target_role, true, false));
            }
            kuberic_runtime_internal::effects::RuntimeEffectAction::ChangeApplicationRole(role)
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
        .map_err(|error| AgentError::CommandRejected(error.to_string()))?
        .map_err(|error| AgentError::CommandRejected(error.to_string()))
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
                        .map_err(|error: kuberic_wire::WireError| {
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
                        .map_err(|error: kuberic_wire::WireError| {
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

fn status_from_agent(error: AgentError) -> Status {
    match error {
        AgentError::EffectConflict(_) | AgentError::CommandRejected(_) => {
            Status::failed_precondition(error.to_string())
        }
        AgentError::SessionRejected(_) => Status::failed_precondition(error.to_string()),
        AgentError::Backpressure(_) => Status::resource_exhausted(error.to_string()),
        AgentError::Runtime(error) => status_from_runtime(error),
        _ => Status::internal(error.to_string()),
    }
}

fn status_from_runtime(error: kuberic_runtime::RuntimeError) -> Status {
    use kuberic_runtime::RuntimeError;
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
    use kuberic_protocol::types::{PodUid, PvcUid, ReplicaInstanceId, ResourceUid};

    #[test]
    fn uninitialized_report_is_unsafe_when_application_state_survives() {
        let directory = tempfile::tempdir().unwrap();
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
}
