use bytes::Bytes;
use futures::{StreamExt, stream};
use kuberic_protocol::command::EnsureReplicaBuild;
use kuberic_protocol::observation::{AgentObservation, AgentReport};
use kuberic_protocol::types::{
    ProcessSessionId, ReplicaId, ReplicaIdentity, ResourceUid, derive_replica_endpoint_name,
};
use kuberic_runtime::replicator::copy::{BuildConfiguration, PrepareCopyRequest};
use kuberic_runtime::replicator::sender::SenderOutbound;
pub use kuberic_runtime::replicator::sender::{
    ReliableSender as ReliableTransport, ReliableWindow, ResumeWindow, RetainedMessage,
    RoleTransportState,
};
use kuberic_runtime::{Result as RuntimeResult, RuntimeError};
use kuberic_runtime_internal::transport::{
    CopyAck, CopyItem, OutboundOperation, ReplicaEndpoint, ReplicationAck, ReplicationItem,
};
use kuberic_wire::{
    normalize_copy_ack, normalize_copy_item, normalize_replication_ack, normalize_replication_item,
    proto,
};
use std::collections::{BTreeMap, BTreeSet};

use crate::hosting::PodRuntime;
use crate::service::SessionRegistry;
use crate::store::AgentStore;
use crate::{AgentError, Result};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{Mutex, watch};
use tokio_stream::iter;
use tonic::Request;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueuedOutbound {
    Replication {
        receiver: ReplicaIdentity,
        sequence: u64,
        item: proto::ReplicationItem,
    },
    Copy {
        receiver: ReplicaIdentity,
        sequence: u64,
        item: proto::CopyItem,
    },
    Build(ReplicaEndpoint),
    Remove(ReplicaId),
}

#[async_trait]
pub trait OutboundDispatcher: Send + Sync {
    async fn dispatch(&self, outbound: QueuedOutbound) -> Result<()>;

    async fn refresh_peer(&self, _receiver: &ReplicaIdentity) -> Result<()> {
        Ok(())
    }
}

pub trait ReplicaEndpointResolver: Send + Sync {
    fn control_endpoint(&self, identity: &ReplicaIdentity) -> String;

    fn replication_endpoint(&self, identity: &ReplicaIdentity) -> String;
}

#[derive(Debug, Clone)]
pub struct KubernetesDnsResolver {
    resource_uid: ResourceUid,
    namespace: Arc<str>,
}

impl KubernetesDnsResolver {
    pub fn new(resource_uid: ResourceUid, namespace: impl Into<Arc<str>>) -> Self {
        Self {
            resource_uid,
            namespace: namespace.into(),
        }
    }

    fn host(&self, identity: &ReplicaIdentity) -> String {
        format!(
            "{}.{}.svc",
            derive_replica_endpoint_name(&self.resource_uid, identity),
            self.namespace
        )
    }
}

impl ReplicaEndpointResolver for KubernetesDnsResolver {
    fn control_endpoint(&self, identity: &ReplicaIdentity) -> String {
        format!("http://{}:50051", self.host(identity))
    }

    fn replication_endpoint(&self, identity: &ReplicaIdentity) -> String {
        format!("http://{}:50052", self.host(identity))
    }
}

pub struct GrpcOutboundDispatcher<R> {
    runtime: Arc<PodRuntime>,
    transport: Arc<Mutex<ReliableTransport>>,
    resolver: Arc<R>,
    resource_uid: Arc<str>,
    bearer_token: Arc<str>,
    deadline: std::time::Duration,
    build_locks: Mutex<BTreeMap<kuberic_protocol::types::OperationId, Arc<Mutex<()>>>>,
    completed_builds: Mutex<BTreeSet<kuberic_protocol::types::OperationId>>,
}

impl<R> GrpcOutboundDispatcher<R>
where
    R: ReplicaEndpointResolver,
{
    pub fn new(
        runtime: Arc<PodRuntime>,
        transport: Arc<Mutex<ReliableTransport>>,
        resolver: Arc<R>,
        resource_uid: impl Into<Arc<str>>,
        bearer_token: impl Into<Arc<str>>,
        deadline: std::time::Duration,
    ) -> Result<Self> {
        let bearer_token = bearer_token.into();
        if bearer_token.is_empty() {
            return Err(AgentError::CommandRejected(
                "agent bearer token must not be empty".into(),
            ));
        }
        Ok(Self {
            runtime,
            transport,
            resolver,
            resource_uid: resource_uid.into(),
            bearer_token,
            deadline,
            build_locks: Mutex::new(BTreeMap::new()),
            completed_builds: Mutex::new(BTreeSet::new()),
        })
    }

    pub async fn peer_session(&self, receiver: &ReplicaIdentity) -> Result<ProcessSessionId> {
        Ok(self.peer_report(receiver).await?.process_session_id)
    }

    pub async fn peer_report(&self, receiver: &ReplicaIdentity) -> Result<AgentReport> {
        let endpoint = self.resolver.control_endpoint(receiver);
        let mut client = tokio::time::timeout(
            self.deadline,
            proto::agent_control_client::AgentControlClient::connect(endpoint),
        )
        .await
        .map_err(|_| AgentError::SessionRejected("peer status connection timed out".into()))?
        .map_err(|error| AgentError::SessionRejected(error.to_string()))?;
        let mut request = Request::new(proto::GetAgentStatusRequest {
            protocol_version: kuberic_protocol::PROTOCOL_VERSION,
            resource_uid: self.resource_uid.to_string(),
            replica_id: receiver.replica_id.value(),
            expected_instance_id: receiver.instance_id.to_string(),
        });
        add_bearer_token(&mut request, &self.bearer_token)?;
        let report = tokio::time::timeout(self.deadline, client.get_status(request))
            .await
            .map_err(|_| AgentError::SessionRejected("peer status request timed out".into()))?
            .map_err(|error| AgentError::SessionRejected(error.to_string()))?
            .into_inner();
        let AgentObservation::Report(report) = kuberic_wire::normalize_agent_status_report(report)
            .map_err(|error| AgentError::SessionRejected(error.to_string()))?
        else {
            return Err(AgentError::SessionRejected(
                "peer is not initialized".into(),
            ));
        };
        if report.identity != *receiver {
            return Err(AgentError::SessionRejected(
                "peer status returned another exact identity".into(),
            ));
        }
        Ok(*report)
    }

    async fn dispatch_build(&self, endpoint: ReplicaEndpoint) -> Result<()> {
        let build_lock = {
            let mut locks = self.build_locks.lock().await;
            locks
                .entry(endpoint.build_id.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _build = build_lock.lock().await;
        let build_id = endpoint.build_id.clone();
        let result = self.dispatch_build_locked(endpoint).await;
        if result.is_err() {
            let _ = self.runtime.cancel_outbound_build(&build_id).await;
        }
        result
    }

    async fn dispatch_build_locked(&self, endpoint: ReplicaEndpoint) -> Result<()> {
        if self
            .completed_builds
            .lock()
            .await
            .contains(&endpoint.build_id)
        {
            return Ok(());
        }
        let snapshot = self.runtime.snapshot().await;
        if snapshot.builds.iter().any(|build| {
            build.authority.build_id == endpoint.build_id
                && build.authority.target == endpoint.identity
                && build.completed
                && build.durable_lsn >= snapshot.current_progress
        }) {
            return Ok(());
        }
        let source_session_id = self.transport.lock().await.local_session().clone();
        let authority = self
            .runtime
            .authorize_build(
                endpoint.build_id.clone(),
                endpoint.identity.clone(),
                BuildConfiguration::Current,
            )
            .await?;
        let target_session = self.peer_session(&endpoint.identity).await?;
        self.transport
            .lock()
            .await
            .admit_peer(endpoint.identity.clone(), target_session)?;
        let target_command = EnsureReplicaBuild {
            operation_id: endpoint.build_id.clone(),
            local_replica_id: endpoint.identity.replica_id,
            expected_instance_id: endpoint.identity.instance_id.clone(),
            expected_agent_generation: endpoint.identity.agent_generation.clone(),
            target: endpoint.identity.clone(),
            authority: Some(authority),
            source_session_id: Some(source_session_id),
        };
        let control_endpoint = self.resolver.control_endpoint(&endpoint.identity);
        let mut control = tokio::time::timeout(
            self.deadline,
            proto::agent_control_client::AgentControlClient::connect(control_endpoint),
        )
        .await
        .map_err(|_| AgentError::SessionRejected("build target connection timed out".into()))?
        .map_err(|error| AgentError::SessionRejected(error.to_string()))?;
        let mut request = Request::new(proto::ExecuteCommandRequest {
            protocol_version: kuberic_protocol::PROTOCOL_VERSION,
            resource_uid: self.resource_uid.to_string(),
            target: Some(endpoint.identity.clone().into()),
            command: Some(proto::execute_command_request::Command::EnsureReplicaBuild(
                ensure_build_to_proto(target_command),
            )),
        });
        add_bearer_token(&mut request, &self.bearer_token)?;
        let response = tokio::time::timeout(self.deadline, control.execute(request))
            .await
            .map_err(|_| AgentError::SessionRejected("build target admission timed out".into()))?
            .map_err(|error| AgentError::SessionRejected(error.to_string()))?
            .into_inner();
        let report = response.observation.ok_or_else(|| {
            AgentError::SessionRejected("build target admission omitted its report".into())
        })?;
        kuberic_wire::validate_agent_status_report(&report)
            .map_err(|error| AgentError::SessionRejected(error.to_string()))?;

        let build_id = endpoint.build_id.clone();
        let mut prepared = self
            .runtime
            .data_plane()
            .prepare_copy(PrepareCopyRequest {
                build_id: build_id.clone(),
                target: endpoint.identity.clone(),
                configuration: BuildConfiguration::Current,
                copy_context: Box::pin(stream::empty()),
            })
            .await?;
        while let Some(item) = prepared.items.next().await {
            self.dispatch_copy(endpoint.identity.clone(), item?, false)
                .await?;
        }
        self.completed_builds.lock().await.insert(build_id);
        Ok(())
    }

    async fn dispatch_copy(
        &self,
        receiver: ReplicaIdentity,
        mut item: proto::CopyItem,
        retire_window: bool,
    ) -> Result<()> {
        item.sender_session_id = self.transport.lock().await.local_session().to_string();
        item.receiver_session_id = self.peer_session(&receiver).await?.to_string();
        let endpoint = self.resolver.replication_endpoint(&receiver);
        let mut client = tokio::time::timeout(
            self.deadline,
            proto::replication_data_client::ReplicationDataClient::connect(endpoint),
        )
        .await
        .map_err(|_| AgentError::SessionRejected("copy connection timed out".into()))?
        .map_err(|error| AgentError::SessionRejected(error.to_string()))?;
        let mut request = Request::new(iter([item]));
        add_bearer_token(&mut request, &self.bearer_token)?;
        let mut acknowledgements = tokio::time::timeout(self.deadline, client.build(request))
            .await
            .map_err(|_| AgentError::SessionRejected("copy request timed out".into()))?
            .map_err(|error| AgentError::SessionRejected(error.to_string()))?
            .into_inner();
        let acknowledgement = tokio::time::timeout(self.deadline, acknowledgements.message())
            .await
            .map_err(|_| AgentError::SessionRejected("copy acknowledgement timed out".into()))?
            .map_err(|error| AgentError::SessionRejected(error.to_string()))?
            .ok_or_else(|| {
                AgentError::SessionRejected("copy peer returned no acknowledgement".into())
            })?;
        let sequence = acknowledgement.sequence;
        self.runtime
            .data_plane()
            .accept_copy_acknowledgement(acknowledgement)
            .await?;
        if retire_window {
            self.transport
                .lock()
                .await
                .acknowledge_copy(&receiver, sequence)?;
        }
        Ok(())
    }
}

#[async_trait]
impl<R> OutboundDispatcher for GrpcOutboundDispatcher<R>
where
    R: ReplicaEndpointResolver + 'static,
{
    async fn refresh_peer(&self, receiver: &ReplicaIdentity) -> Result<()> {
        let session = self.peer_session(receiver).await?;
        self.transport
            .lock()
            .await
            .admit_peer(receiver.clone(), session)?;
        Ok(())
    }

    async fn dispatch(&self, outbound: QueuedOutbound) -> Result<()> {
        match outbound {
            QueuedOutbound::Replication {
                receiver,
                sequence: _,
                mut item,
            } => {
                item.receiver_session_id = self.peer_session(&receiver).await?.to_string();
                let endpoint = self.resolver.replication_endpoint(&receiver);
                let mut client = tokio::time::timeout(
                    self.deadline,
                    proto::replication_data_client::ReplicationDataClient::connect(endpoint),
                )
                .await
                .map_err(|_| {
                    AgentError::SessionRejected("replication connection timed out".into())
                })?
                .map_err(|error| AgentError::SessionRejected(error.to_string()))?;
                let mut request = Request::new(iter([item]));
                add_bearer_token(&mut request, &self.bearer_token)?;
                let mut acknowledgements =
                    tokio::time::timeout(self.deadline, client.replicate(request))
                        .await
                        .map_err(|_| {
                            AgentError::SessionRejected("replication request timed out".into())
                        })?
                        .map_err(|error| AgentError::SessionRejected(error.to_string()))?
                        .into_inner();
                let acknowledgement =
                    tokio::time::timeout(self.deadline, acknowledgements.message())
                        .await
                        .map_err(|_| {
                            AgentError::SessionRejected(
                                "replication acknowledgement timed out".into(),
                            )
                        })?
                        .map_err(|error| AgentError::SessionRejected(error.to_string()))?
                        .ok_or_else(|| {
                            AgentError::SessionRejected(
                                "replication peer returned no acknowledgement".into(),
                            )
                        })?;
                let applied_lsn = acknowledgement.applied_lsn;
                self.runtime
                    .data_plane()
                    .accept_acknowledgement(acknowledgement)
                    .await?;
                self.transport
                    .lock()
                    .await
                    .acknowledge_replication(&receiver, applied_lsn)
                    .map_err(AgentError::from)
            }
            QueuedOutbound::Copy {
                receiver,
                sequence: _,
                item,
            } => self.dispatch_copy(receiver, item, true).await,
            QueuedOutbound::Build(endpoint) => self.dispatch_build(endpoint).await,
            QueuedOutbound::Remove(replica_id) => {
                let receiver = self.transport.lock().await.peer_for_replica(replica_id);
                if let Some(receiver) = receiver {
                    self.transport.lock().await.retire_peer(&receiver);
                }
                Ok(())
            }
        }
    }
}

fn add_bearer_token<T>(request: &mut Request<T>, token: &str) -> Result<()> {
    let value = format!("Bearer {token}")
        .parse()
        .map_err(|error| AgentError::CommandRejected(format!("invalid bearer token: {error}")))?;
    request.metadata_mut().insert("authorization", value);
    Ok(())
}

fn ensure_build_to_proto(command: EnsureReplicaBuild) -> proto::EnsureReplicaBuildCommand {
    proto::EnsureReplicaBuildCommand {
        operation_id: command.operation_id.to_string(),
        local_replica_id: command.local_replica_id.value(),
        expected_instance_id: command.expected_instance_id.to_string(),
        expected_agent_generation: command.expected_agent_generation.to_string(),
        target: Some(command.target.into()),
        authority: command.authority.map(Into::into),
        source_session_id: command
            .source_session_id
            .map_or_else(String::new, |session| session.to_string()),
    }
}

pub async fn run_outbound<D: OutboundDispatcher + 'static>(
    runtime: Arc<PodRuntime>,
    transport: Arc<Mutex<ReliableTransport>>,
    dispatcher: Arc<D>,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let data_plane = runtime.data_plane();
    let mut workers = BTreeMap::new();
    let mut worker_tasks = tokio::task::JoinSet::new();
    loop {
        let mut receive_shutdown = shutdown.clone();
        tokio::select! {
            _ = wait_for_shutdown_signal(&mut receive_shutdown) => {
                worker_tasks.abort_all();
                return Ok(());
            },
            completed = worker_tasks.join_next(), if !worker_tasks.is_empty() => {
                if let Some(Err(error)) = completed {
                    tracing::warn!(%error, "outbound peer worker failed");
                }
            }
            outbound = data_plane.next_domain_outbound() => {
                let Some(outbound) = outbound else {
                    let mut retry_shutdown = shutdown.clone();
                    tokio::select! {
                        _ = wait_for_shutdown_signal(&mut retry_shutdown) => {
                            worker_tasks.abort_all();
                            return Ok(());
                        }
                        _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
                    }
                    continue;
                };
                let receiver = domain_outbound_receiver(&outbound).cloned();
                let queued = {
                    let mut sender = transport.lock().await;
                    sender.queue(outbound)
                };
                let queued = match queued {
                    Ok(queued) => sender_outbound_to_queued(queued),
                    Err(RuntimeError::QueueFull | RuntimeError::ReconfigurationPending) => {
                        tracing::warn!(
                            ?receiver,
                            "outbound peer backlog unavailable; deferring to peer repair"
                        );
                        continue;
                    }
                    Err(error) => return Err(AgentError::Runtime(error)),
                };
                let key = queued_outbound_receiver(&queued).cloned();
                let sender = workers.entry(key).or_insert_with(|| {
                    spawn_delivery_worker(
                        &mut worker_tasks,
                        Some(runtime.clone()),
                        transport.clone(),
                        dispatcher.clone(),
                        shutdown.clone(),
                    )
                });
                if sender.send(queued.clone()).is_err() {
                    let replacement = spawn_delivery_worker(
                        &mut worker_tasks,
                        Some(runtime.clone()),
                        transport.clone(),
                        dispatcher.clone(),
                        shutdown.clone(),
                    );
                    replacement.send(queued).map_err(|_| {
                        AgentError::Backpressure("outbound peer worker stopped".into())
                    })?;
                    *sender = replacement;
                }
            }
        }
    }
}

fn spawn_delivery_worker<D: OutboundDispatcher + 'static>(
    tasks: &mut tokio::task::JoinSet<()>,
    runtime: Option<Arc<PodRuntime>>,
    transport: Arc<Mutex<ReliableTransport>>,
    dispatcher: Arc<D>,
    shutdown: watch::Receiver<bool>,
) -> tokio::sync::mpsc::UnboundedSender<QueuedOutbound> {
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    tasks.spawn(async move {
        while let Some(outbound) = receiver.recv().await {
            if let Err(error) = dispatch_queued_with_retry(
                runtime.as_deref(),
                transport.clone(),
                dispatcher.clone(),
                outbound,
                shutdown.clone(),
            )
            .await
            {
                tracing::warn!(%error, "outbound delivery failed");
                if matches!(error, AgentError::Runtime(RuntimeError::OperationCancelled)) {
                    break;
                }
            }
        }
    });
    sender
}

async fn dispatch_queued_with_retry<D: OutboundDispatcher>(
    runtime: Option<&PodRuntime>,
    transport: Arc<Mutex<ReliableTransport>>,
    dispatcher: Arc<D>,
    queued: QueuedOutbound,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    loop {
        if let Some(runtime) = runtime
            && !queued_matches_runtime_authority(runtime, &queued).await
        {
            if let Some(receiver) = queued_outbound_receiver(&queued) {
                transport.lock().await.retire_peer(receiver);
            }
            return Err(AgentError::Runtime(RuntimeError::OperationCancelled));
        }
        let mut dispatch_shutdown = shutdown.clone();
        let result = tokio::select! {
            biased;
            _ = wait_for_shutdown_signal(&mut dispatch_shutdown) => return Ok(()),
            result = dispatcher.dispatch(queued.clone()) => result,
        };
        match result {
            Ok(()) => return Ok(()),
            Err(error @ (AgentError::SessionRejected(_) | AgentError::Backpressure(_))) => {
                if matches!(queued, QueuedOutbound::Build(_)) {
                    return Err(error);
                }
                tracing::warn!(%error, "outbound peer unavailable; retaining message for retry");
                if let Some(receiver) = queued_outbound_receiver(&queued) {
                    let _ = dispatcher.refresh_peer(receiver).await;
                }
                let retry_delay = transport.lock().await.retry_delay();
                let mut retry_shutdown = shutdown.clone();
                tokio::select! {
                    _ = wait_for_shutdown_signal(&mut retry_shutdown) => return Ok(()),
                    _ = tokio::time::sleep(retry_delay) => {}
                }
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
async fn deliver_outbound<D: OutboundDispatcher>(
    transport: Arc<Mutex<ReliableTransport>>,
    dispatcher: Arc<D>,
    outbound: OutboundOperation,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let queued = loop {
        let (queued, retry_delay) = {
            let mut sender = transport.lock().await;
            (sender.queue(outbound.clone()), sender.retry_delay())
        };
        match queued {
            Ok(queued) => break sender_outbound_to_queued(queued),
            Err(RuntimeError::ReconfigurationPending) => {
                if let Some(receiver) = domain_outbound_receiver(&outbound) {
                    let _ = dispatcher.refresh_peer(receiver).await;
                }
                let mut retry_shutdown = shutdown.clone();
                tokio::select! {
                    _ = wait_for_shutdown_signal(&mut retry_shutdown) => return Ok(()),
                    _ = tokio::time::sleep(retry_delay) => {}
                }
            }
            Err(RuntimeError::QueueFull) => {
                let mut retry_shutdown = shutdown.clone();
                tokio::select! {
                    _ = wait_for_shutdown_signal(&mut retry_shutdown) => return Ok(()),
                    _ = tokio::time::sleep(retry_delay) => {}
                }
            }
            Err(error) => return Err(AgentError::Runtime(error)),
        }
    };
    dispatch_queued_with_retry(None, transport, dispatcher, queued, shutdown).await
}

async fn queued_matches_runtime_authority(runtime: &PodRuntime, queued: &QueuedOutbound) -> bool {
    let snapshot = runtime.snapshot().await;
    let Some(authority) = snapshot.authority else {
        return !matches!(
            queued,
            QueuedOutbound::Replication { .. } | QueuedOutbound::Copy { .. }
        );
    };
    match queued {
        QueuedOutbound::Replication { item, .. } => normalize_replication_item(item.clone())
            .is_ok_and(|item| {
                item.epoch == authority.current_configuration.epoch
                    && item.current_configuration_id
                        == authority.current_configuration.configuration_id
                    && item.previous_configuration_id
                        == authority
                            .previous_configuration
                            .as_ref()
                            .map(|previous| previous.configuration_id.clone())
            }),
        QueuedOutbound::Copy { item, .. } => normalize_copy_item(item.clone()).is_ok_and(|item| {
            item.epoch == authority.current_configuration.epoch
                && item.current_configuration_id == authority.current_configuration.configuration_id
        }),
        QueuedOutbound::Build(_) | QueuedOutbound::Remove(_) => true,
    }
}

pub async fn run_peer_discovery<S, R>(
    local: ReplicaIdentity,
    runtime: Arc<PodRuntime>,
    store: Arc<S>,
    transport: Arc<Mutex<ReliableTransport>>,
    dispatcher: Arc<GrpcOutboundDispatcher<R>>,
    sessions: Arc<SessionRegistry>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()>
where
    S: AgentStore + 'static,
    R: ReplicaEndpointResolver + 'static,
{
    loop {
        if *shutdown.borrow_and_update() {
            return Ok(());
        }
        let state = store.load_state().await?;
        if let Some(configuration) = state.current_configuration {
            for member in configuration
                .members
                .iter()
                .filter(|member| member.identity != local)
            {
                if let Ok(report) = dispatcher.peer_report(&member.identity).await {
                    let session = report.process_session_id.clone();
                    sessions
                        .register_peer(member.identity.clone(), session.clone())
                        .await;
                    transport
                        .lock()
                        .await
                        .admit_peer(member.identity.clone(), session)?;
                    if report.epoch == configuration.epoch
                        && report.current_configuration.as_ref() == Some(&configuration)
                        && report.previous_configuration == state.previous_configuration
                    {
                        if let Some(verified_lsn) = report.verified_replication_lsn
                            && configuration.members.iter().any(|member| {
                                member.identity == local
                                    && member.role == kuberic_protocol::types::ReplicaRole::Primary
                            })
                        {
                            let _ = runtime
                                .data_plane()
                                .accept_acknowledgement(replication_ack_to_proto(ReplicationAck {
                                    sender: local.clone(),
                                    receiver: report.identity.clone(),
                                    epoch: report.epoch,
                                    previous_configuration_id: report
                                        .previous_configuration
                                        .as_ref()
                                        .map(|previous| previous.configuration_id.clone()),
                                    current_configuration_id: configuration
                                        .configuration_id
                                        .clone(),
                                    received_lsn: verified_lsn,
                                    applied_lsn: verified_lsn,
                                    committed_lsn: report.committed_lsn.min(verified_lsn),
                                }))
                                .await;
                        }
                        let _ = runtime
                            .repair_peer(report.identity, report.current_progress)
                            .await;
                    }
                }
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => {}
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow_and_update() {
                    return Ok(());
                }
            }
        }
    }
}

async fn wait_for_shutdown_signal(shutdown: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

fn sender_outbound_to_queued(outbound: SenderOutbound) -> QueuedOutbound {
    match outbound {
        SenderOutbound::Replication {
            receiver,
            sender_session,
            receiver_session,
            message,
        } => QueuedOutbound::Replication {
            receiver,
            sequence: message.sequence,
            item: replication_to_session_proto(
                message.payload,
                sender_session.as_str(),
                receiver_session.as_str(),
            ),
        },
        SenderOutbound::Copy {
            receiver,
            sender_session,
            receiver_session,
            message,
        } => QueuedOutbound::Copy {
            receiver,
            sequence: message.sequence,
            item: copy_to_session_proto(
                message.payload,
                sender_session.as_str(),
                receiver_session.as_str(),
            ),
        },
        SenderOutbound::Build(endpoint) => QueuedOutbound::Build(endpoint),
        SenderOutbound::Remove(replica_id) => QueuedOutbound::Remove(replica_id),
    }
}

fn domain_outbound_receiver(outbound: &OutboundOperation) -> Option<&ReplicaIdentity> {
    match outbound {
        OutboundOperation::Replication(item) => Some(&item.receiver),
        OutboundOperation::Copy(item) => Some(&item.receiver),
        OutboundOperation::Build(endpoint) => Some(&endpoint.identity),
        OutboundOperation::Remove(_) => None,
    }
}

fn queued_outbound_receiver(outbound: &QueuedOutbound) -> Option<&ReplicaIdentity> {
    match outbound {
        QueuedOutbound::Replication { receiver, .. } | QueuedOutbound::Copy { receiver, .. } => {
            Some(receiver)
        }
        QueuedOutbound::Build(endpoint) => Some(&endpoint.identity),
        QueuedOutbound::Remove(_) => None,
    }
}

pub fn replication_from_proto(item: proto::ReplicationItem) -> RuntimeResult<ReplicationItem> {
    let item = normalize_replication_item(item)
        .map_err(|error| RuntimeError::InvalidReplication(error.to_string()))?;
    Ok(ReplicationItem {
        sender: item.sender,
        receiver: item.receiver,
        epoch: item.epoch,
        previous_configuration_id: item.previous_configuration_id,
        current_configuration_id: item.current_configuration_id,
        lsn: item.lsn,
        committed_lsn: item.committed_lsn,
        data: Bytes::from(item.data),
    })
}

pub fn replication_to_proto(item: ReplicationItem) -> proto::ReplicationItem {
    proto::ReplicationItem {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        sender: Some(item.sender.into()),
        epoch: Some(item.epoch.into()),
        previous_configuration_id: item
            .previous_configuration_id
            .map_or_else(String::new, |configuration| configuration.to_string()),
        current_configuration_id: item.current_configuration_id.to_string(),
        lsn: item.lsn,
        committed_lsn: item.committed_lsn,
        data: item.data.to_vec(),
        receiver: Some(item.receiver.into()),
        sender_session_id: String::new(),
        receiver_session_id: String::new(),
    }
}

pub fn replication_to_session_proto(
    item: ReplicationItem,
    sender_session: &str,
    receiver_session: &str,
) -> proto::ReplicationItem {
    let mut item = replication_to_proto(item);
    item.sender_session_id = sender_session.to_string();
    item.receiver_session_id = receiver_session.to_string();
    item
}

pub fn replication_ack_from_proto(
    acknowledgement: proto::ReplicationAck,
) -> RuntimeResult<ReplicationAck> {
    let acknowledgement = normalize_replication_ack(acknowledgement)
        .map_err(|error| RuntimeError::InvalidReplication(error.to_string()))?;
    Ok(ReplicationAck {
        sender: acknowledgement.sender,
        receiver: acknowledgement.receiver,
        epoch: acknowledgement.epoch,
        previous_configuration_id: acknowledgement.previous_configuration_id,
        current_configuration_id: acknowledgement.current_configuration_id,
        received_lsn: acknowledgement.received_lsn,
        applied_lsn: acknowledgement.applied_lsn,
        committed_lsn: acknowledgement.committed_lsn,
    })
}

pub fn replication_ack_to_proto(acknowledgement: ReplicationAck) -> proto::ReplicationAck {
    proto::ReplicationAck {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        sender: Some(acknowledgement.sender.into()),
        receiver: Some(acknowledgement.receiver.into()),
        epoch: Some(acknowledgement.epoch.into()),
        previous_configuration_id: acknowledgement
            .previous_configuration_id
            .map_or_else(String::new, |configuration| configuration.to_string()),
        current_configuration_id: acknowledgement.current_configuration_id.to_string(),
        received_lsn: acknowledgement.received_lsn,
        applied_lsn: acknowledgement.applied_lsn,
        committed_lsn: acknowledgement.committed_lsn,
        sender_session_id: String::new(),
        receiver_session_id: String::new(),
    }
}

pub fn copy_from_proto(item: proto::CopyItem) -> RuntimeResult<CopyItem> {
    let item = normalize_copy_item(item)
        .map_err(|error| RuntimeError::InvalidReplication(error.to_string()))?;
    Ok(CopyItem {
        build_id: item.build_id,
        sender: item.sender,
        receiver: item.receiver,
        epoch: item.epoch,
        current_configuration_id: item.current_configuration_id,
        sequence: item.sequence,
        lsn: item.lsn,
        committed_lsn: item.committed_lsn,
        replication_boundary_lsn: item.replication_boundary_lsn,
        final_item: item.final_item,
        snapshot_chunk: item.snapshot_chunk,
        data: Bytes::from(item.data),
    })
}

pub fn copy_to_proto(item: CopyItem) -> proto::CopyItem {
    proto::CopyItem {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        build_id: item.build_id.to_string(),
        sender: Some(item.sender.into()),
        receiver: Some(item.receiver.into()),
        epoch: Some(item.epoch.into()),
        current_configuration_id: item.current_configuration_id.to_string(),
        sequence: item.sequence,
        lsn: item.lsn,
        committed_lsn: item.committed_lsn,
        replication_boundary_lsn: item.replication_boundary_lsn,
        final_item: item.final_item,
        data: item.data.to_vec(),
        snapshot_chunk: item.snapshot_chunk,
        sender_session_id: String::new(),
        receiver_session_id: String::new(),
    }
}

pub fn copy_to_session_proto(
    item: CopyItem,
    sender_session: &str,
    receiver_session: &str,
) -> proto::CopyItem {
    let mut item = copy_to_proto(item);
    item.sender_session_id = sender_session.to_string();
    item.receiver_session_id = receiver_session.to_string();
    item
}

pub fn copy_ack_from_proto(acknowledgement: proto::CopyAck) -> RuntimeResult<CopyAck> {
    let acknowledgement = normalize_copy_ack(acknowledgement)
        .map_err(|error| RuntimeError::InvalidReplication(error.to_string()))?;
    Ok(CopyAck {
        build_id: acknowledgement.build_id,
        sender: acknowledgement.sender,
        receiver: acknowledgement.receiver,
        epoch: acknowledgement.epoch,
        current_configuration_id: acknowledgement.current_configuration_id,
        sequence: acknowledgement.sequence,
        durable_lsn: acknowledgement.durable_lsn,
        replication_boundary_lsn: acknowledgement.replication_boundary_lsn,
        final_item: acknowledgement.final_item,
        snapshot_chunk: acknowledgement.snapshot_chunk,
    })
}

pub fn copy_ack_to_proto(acknowledgement: CopyAck) -> proto::CopyAck {
    proto::CopyAck {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        build_id: acknowledgement.build_id.to_string(),
        sender: Some(acknowledgement.sender.into()),
        receiver: Some(acknowledgement.receiver.into()),
        epoch: Some(acknowledgement.epoch.into()),
        current_configuration_id: acknowledgement.current_configuration_id.to_string(),
        sequence: acknowledgement.sequence,
        durable_lsn: acknowledgement.durable_lsn,
        replication_boundary_lsn: acknowledgement.replication_boundary_lsn,
        final_item: acknowledgement.final_item,
        snapshot_chunk: acknowledgement.snapshot_chunk,
        sender_session_id: String::new(),
        receiver_session_id: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kuberic_protocol::types::{AgentGeneration, ConfigurationId, Epoch, ReplicaInstanceId};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FlakyDispatcher {
        failed_peer_attempts: AtomicUsize,
        failures_before_success: usize,
        delivered: tokio::sync::mpsc::UnboundedSender<ReplicaId>,
    }

    #[async_trait]
    impl OutboundDispatcher for FlakyDispatcher {
        async fn dispatch(&self, outbound: QueuedOutbound) -> Result<()> {
            let receiver = match outbound {
                QueuedOutbound::Replication { receiver, .. }
                | QueuedOutbound::Copy { receiver, .. } => receiver,
                QueuedOutbound::Build(endpoint) => endpoint.identity,
                QueuedOutbound::Remove(replica_id) => {
                    self.delivered.send(replica_id).unwrap();
                    return Ok(());
                }
            };
            if receiver.replica_id == ReplicaId::new(2)
                && self.failed_peer_attempts.fetch_add(1, Ordering::SeqCst)
                    < self.failures_before_success
            {
                return Err(AgentError::SessionRejected(
                    "injected unavailable peer".into(),
                ));
            }
            self.delivered.send(receiver.replica_id).unwrap();
            Ok(())
        }
    }

    fn identity(replica_id: i64) -> ReplicaIdentity {
        ReplicaIdentity {
            replica_id: ReplicaId::new(replica_id),
            instance_id: ReplicaInstanceId::new(format!("pod-{replica_id}")),
            agent_generation: AgentGeneration::new(format!("generation-{replica_id}")),
        }
    }

    fn outbound(receiver: ReplicaIdentity) -> OutboundOperation {
        OutboundOperation::Replication(ReplicationItem {
            sender: identity(1),
            receiver,
            epoch: Epoch::new(0, 1),
            previous_configuration_id: None,
            current_configuration_id: ConfigurationId::new("configuration"),
            lsn: 1,
            committed_lsn: 0,
            data: Bytes::from_static(b"value"),
        })
    }

    #[tokio::test]
    async fn unavailable_peer_does_not_block_another_peer_delivery() {
        let transport = Arc::new(Mutex::new(
            ReliableTransport::new(ProcessSessionId::new("primary-session"), 8).unwrap(),
        ));
        for replica_id in [2, 3] {
            transport
                .lock()
                .await
                .admit_peer(
                    identity(replica_id),
                    ProcessSessionId::new(format!("session-{replica_id}")),
                )
                .unwrap();
        }
        let (delivered, mut delivered_rx) = tokio::sync::mpsc::unbounded_channel();
        let dispatcher = Arc::new(FlakyDispatcher {
            failed_peer_attempts: AtomicUsize::new(0),
            failures_before_success: 2,
            delivered,
        });
        let (_shutdown, shutdown) = watch::channel(false);
        let unavailable = tokio::spawn(deliver_outbound(
            transport.clone(),
            dispatcher.clone(),
            outbound(identity(2)),
            shutdown.clone(),
        ));
        let available = tokio::spawn(deliver_outbound(
            transport,
            dispatcher,
            outbound(identity(3)),
            shutdown,
        ));

        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_millis(50), delivered_rx.recv())
                .await
                .unwrap(),
            Some(ReplicaId::new(3))
        );
        available.await.unwrap().unwrap();
        unavailable.await.unwrap().unwrap();
        assert_eq!(delivered_rx.recv().await, Some(ReplicaId::new(2)));
    }

    #[tokio::test]
    async fn sustained_unavailable_peer_does_not_exhaust_healthy_delivery() {
        let transport = Arc::new(Mutex::new(
            ReliableTransport::new(ProcessSessionId::new("primary-session"), 128).unwrap(),
        ));
        for replica_id in [2, 3] {
            transport
                .lock()
                .await
                .admit_peer(
                    identity(replica_id),
                    ProcessSessionId::new(format!("session-{replica_id}")),
                )
                .unwrap();
        }
        let (delivered, mut delivered_rx) = tokio::sync::mpsc::unbounded_channel();
        let dispatcher = Arc::new(FlakyDispatcher {
            failed_peer_attempts: AtomicUsize::new(0),
            failures_before_success: usize::MAX,
            delivered,
        });
        let (stop, shutdown) = watch::channel(false);
        let mut tasks = tokio::task::JoinSet::new();
        let blocked = spawn_delivery_worker(
            &mut tasks,
            None,
            transport.clone(),
            dispatcher.clone(),
            shutdown.clone(),
        );
        let healthy =
            spawn_delivery_worker(&mut tasks, None, transport.clone(), dispatcher, shutdown);

        for _ in 0..65 {
            let queued = transport
                .lock()
                .await
                .queue(outbound(identity(2)))
                .map(sender_outbound_to_queued)
                .unwrap();
            blocked.send(queued).unwrap();
        }
        let queued = transport
            .lock()
            .await
            .queue(outbound(identity(3)))
            .map(sender_outbound_to_queued)
            .unwrap();
        healthy.send(queued).unwrap();

        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_millis(50), delivered_rx.recv())
                .await
                .unwrap(),
            Some(ReplicaId::new(3))
        );
        stop.send_replace(true);
        tasks.abort_all();
    }
}
