//! Service Fabric-aligned process hosting boundary.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};

use async_trait::async_trait;
use futures::{Stream, StreamExt, future::BoxFuture};
use kuberic_protocol::types::{
    AccessStatus, Epoch, FaultType, LoadMetric, OperationId, PartitionId, PartitionInformation,
    ReplicaIdentity, ReplicaRole,
};
use kuberic_runtime::application::{
    ClientWrite, OpenContext, OpenMode, StateProvider, StatefulServiceReplica, WriteReceipt,
};
use kuberic_runtime::internal::{
    PendingReplication as RuntimePendingReplication, PendingWrite as RuntimePendingWrite,
};
use kuberic_runtime::replicator::copy::{
    BuildConfiguration, PrepareCopyRequest, PreparedCopy as RuntimePreparedCopy,
};
use kuberic_runtime::replicator::{
    DefaultReplicatorDependencies, ManagedReplicator, PartitionAccessView, PrimaryReplicator,
    Replicator, ReplicatorCreationReservation, ReplicatorFactoryContext, ReplicatorInterfaces,
    ReplicatorRegistration, StatefulServicePartition,
};
use kuberic_runtime::{Result, RuntimeError};
use kuberic_runtime_internal::RuntimeHostToken;
use kuberic_runtime_internal::authority::{
    AuthorityStore, BuildAuthority, BuildAuthorityKind, BuildAuthorityStore, BuildProgressStore,
    LocalWriteJournal, ReplicaAuthorityStore, ReplicationProgressStore,
};
use kuberic_runtime_internal::effects::{
    RoleTransition, RuntimeEffect, RuntimeEffectAction, RuntimeEffectResult, RuntimePostcondition,
    RuntimeSnapshot,
};
use kuberic_runtime_internal::transport::{OutboundOperation, ReplicaEndpoint};
use kuberic_wire::proto;
use tokio::sync::{Mutex, RwLock};

use crate::transport::{
    copy_ack_from_proto, copy_ack_to_proto, copy_from_proto, copy_to_proto,
    replication_ack_from_proto, replication_ack_to_proto, replication_from_proto,
    replication_to_proto,
};

const REPLICATOR_CREATION_AVAILABLE: u8 = 0;
const REPLICATOR_CREATION_RESERVED: u8 = 1;
const REPLICATOR_CREATION_REGISTERED: u8 = 2;
const REPLICATOR_RESERVATION_ID: u64 = 1;

#[path = "custom.rs"]
mod custom;

#[async_trait]
pub trait RuntimeControlPlane: Send {
    async fn next_effect(&mut self) -> Result<Option<RuntimeEffect>>;

    async fn publish(&mut self, result: RuntimeEffectResult) -> Result<()>;
}

pub struct PendingWrite {
    pub lsn: i64,
    pub replication_items: Vec<proto::ReplicationItem>,
    pub build_items: Vec<proto::CopyItem>,
    inner: RuntimePendingWrite,
}

impl PendingWrite {
    pub async fn committed(self) -> Result<WriteReceipt> {
        self.inner.committed().await
    }
}

pub struct PendingReplication {
    pub received: proto::ReplicationAck,
    inner: RuntimePendingReplication,
}

impl PendingReplication {
    pub async fn applied(self) -> Result<proto::ReplicationAck> {
        self.inner.applied().await.map(replication_ack_to_proto)
    }
}

pub struct PreparedCopy {
    pub authority: kuberic_runtime_internal::authority::BuildAuthority,
    pub items: Pin<Box<dyn Stream<Item = Result<proto::CopyItem>> + Send>>,
}

#[derive(Debug)]
pub enum OutboundReplication {
    Replication(proto::ReplicationItem),
    Copy(proto::CopyItem),
    Build(ReplicaEndpoint),
    Remove(kuberic_protocol::types::ReplicaId),
    Evict(ReplicaIdentity),
}

#[derive(Debug, Clone)]
struct AppliedEffect {
    effect: RuntimeEffect,
    result: RuntimeEffectResult,
}

#[derive(Debug)]
struct HostState {
    effects: BTreeMap<u64, AppliedEffect>,
    fallback_snapshot: RuntimeSnapshot,
    partition_information: PartitionInformation,
    load_metrics: BTreeMap<String, i64>,
    reported_fault: Option<FaultType>,
    role_transition_epoch: Option<Epoch>,
}

struct RegisteredReplicator {
    control: Arc<dyn Replicator>,
    primary: Option<Arc<dyn PrimaryReplicator>>,
    provider: Option<Arc<dyn StateProvider>>,
    managed: Option<Arc<dyn ManagedReplicator>>,
    custom: Option<Arc<custom::CustomReplicatorHost>>,
}

enum HostedLifecycle {
    DefaultEngine(Arc<dyn ManagedReplicator>),
    ServicePrimary(Arc<custom::CustomReplicatorHost>),
}

impl HostedLifecycle {
    // Keep the default engine's boxed-future boundary; custom control futures
    // must not inflate every default-engine effect's stack frame.
    fn complete_open(&self, address: String) -> BoxFuture<'_, Result<()>> {
        match self {
            Self::DefaultEngine(host) => host.complete_open(address),
            Self::ServicePrimary(host) => Box::pin(host.complete_open(address)),
        }
    }

    fn fence_writes(&self) -> BoxFuture<'_, Result<()>> {
        match self {
            Self::DefaultEngine(host) => host.fence_writes(),
            Self::ServicePrimary(host) => Box::pin(host.fence_writes()),
        }
    }

    fn restore_authority(&self) -> BoxFuture<'_, Result<()>> {
        match self {
            Self::DefaultEngine(host) => host.restore_authority(),
            Self::ServicePrimary(host) => Box::pin(host.restore_authority()),
        }
    }

    fn settle_primary_prefix(&self) -> BoxFuture<'_, Result<()>> {
        match self {
            Self::DefaultEngine(host) => host.settle_primary_prefix(),
            Self::ServicePrimary(host) => Box::pin(host.settle_primary_prefix()),
        }
    }

    fn cancel_configuration_work(&self) -> BoxFuture<'_, Result<()>> {
        match self {
            Self::DefaultEngine(host) => host.cancel_configuration_work(),
            Self::ServicePrimary(host) => Box::pin(host.cancel_configuration_work()),
        }
    }

    fn execute_action(&self, action: RuntimeEffectAction) -> BoxFuture<'_, Result<()>> {
        match self {
            Self::DefaultEngine(host) => host.execute_action(action),
            Self::ServicePrimary(host) => Box::pin(host.execute_action(action)),
        }
    }

    fn snapshot(&self) -> BoxFuture<'_, RuntimeSnapshot> {
        match self {
            Self::DefaultEngine(host) => host.snapshot(),
            Self::ServicePrimary(host) => Box::pin(host.snapshot()),
        }
    }

    fn cancel_outbound_build<'a>(&'a self, id: &'a OperationId) -> BoxFuture<'a, Result<()>> {
        match self {
            Self::DefaultEngine(host) => host.cancel_outbound_build(id),
            Self::ServicePrimary(host) => Box::pin(host.cancel_outbound_build(id)),
        }
    }

    fn next_outbound(&self) -> BoxFuture<'_, Option<OutboundOperation>> {
        match self {
            Self::DefaultEngine(host) => host.next_outbound(),
            Self::ServicePrimary(host) => Box::pin(host.next_outbound()),
        }
    }
}

impl RegisteredReplicator {
    fn managed(&self) -> Option<Arc<dyn ManagedReplicator>> {
        self.managed.clone()
    }
    fn lifecycle(&self) -> Option<HostedLifecycle> {
        self.managed
            .clone()
            .map(HostedLifecycle::DefaultEngine)
            .or_else(|| self.custom.clone().map(HostedLifecycle::ServicePrimary))
    }
    fn primary(&self) -> Option<Arc<dyn PrimaryReplicator>> {
        self.primary.clone()
    }
    fn provider(&self) -> Result<&Arc<dyn StateProvider>> {
        self.provider.as_ref().ok_or_else(|| {
            RuntimeError::Application(
                "custom replicator has no operation/copy state provider".into(),
            )
        })
    }
    async fn open(&self) -> Result<Option<String>> {
        let address = self.control.open().await?;
        if let Some(hosted) = self.lifecycle() {
            hosted.complete_open(address.clone()).await?;
        }
        Ok(Some(address))
    }
    async fn change_role(&self, epoch: Epoch, role: ReplicaRole) -> Result<()> {
        self.control.change_role(epoch, role).await
    }
    async fn update_epoch(&self, epoch: Epoch) -> Result<()> {
        self.control.update_epoch(epoch).await
    }
    async fn close(&self) -> Result<()> {
        self.control.close().await
    }
    async fn current_progress(&self) -> Result<i64> {
        self.control.current_progress().await
    }
    async fn catch_up_capability(&self) -> Result<i64> {
        self.control.catch_up_capability().await
    }
    fn abort(&self) {
        self.control.abort();
    }
}

pub struct PodRuntime {
    host: Arc<RuntimeHost>,
}

#[derive(Clone)]
pub struct RuntimeDataPlane {
    host: Arc<RuntimeHost>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionReportSnapshot {
    pub information: PartitionInformation,
    pub read_status: AccessStatus,
    pub write_status: AccessStatus,
    pub load_metrics: Vec<LoadMetric>,
    pub reported_fault: Option<FaultType>,
}

impl Drop for PodRuntime {
    fn drop(&mut self) {
        self.host.abort();
    }
}

impl PodRuntime {
    pub fn new<A, S>(
        identity: ReplicaIdentity,
        application: Arc<A>,
        authority_store: Arc<S>,
    ) -> Self
    where
        A: StatefulServiceReplica + 'static,
        S: AuthorityStore + 'static,
    {
        let partition_id =
            PartitionId::new(format!("partition-{}", identity.agent_generation.as_str()));
        Self::new_for_partition(
            PartitionInformation { partition_id },
            identity,
            application,
            authority_store,
        )
    }

    pub fn new_for_partition<A, S>(
        partition_information: PartitionInformation,
        identity: ReplicaIdentity,
        application: Arc<A>,
        authority_store: Arc<S>,
    ) -> Self
    where
        A: StatefulServiceReplica + 'static,
        S: AuthorityStore + 'static,
    {
        let application: Arc<dyn StatefulServiceReplica> = application;
        let replica_authority_store: Arc<dyn ReplicaAuthorityStore> = authority_store.clone();
        let replication_progress_store: Arc<dyn ReplicationProgressStore> = authority_store.clone();
        let local_write_journal: Arc<dyn LocalWriteJournal> = authority_store.clone();
        let build_authority_store: Arc<dyn BuildAuthorityStore> = authority_store.clone();
        let build_progress_store: Arc<dyn BuildProgressStore> = authority_store;
        let fallback_snapshot = empty_snapshot(identity.clone());
        Self {
            host: Arc::new_cyclic(|weak_self| RuntimeHost {
                identity,
                application: application.clone(),
                default_dependencies: DefaultReplicatorDependencies {
                    replica_authority_store,
                    replication_progress_store,
                    local_write_journal,
                    build_authority_store,
                    build_progress_store,
                },
                state: RwLock::new(HostState {
                    effects: BTreeMap::new(),
                    fallback_snapshot,
                    partition_information,
                    load_metrics: BTreeMap::new(),
                    reported_fault: None,
                    role_transition_epoch: None,
                }),
                effect_lock: Mutex::new(()),
                registered: OnceLock::new(),
                pending_managed: StdMutex::new(None),
                weak_self: weak_self.clone(),
                aborted: AtomicBool::new(false),
                closed: AtomicBool::new(false),
                replicator_creation: AtomicU8::new(REPLICATOR_CREATION_AVAILABLE),
                replica_session: OnceLock::new(),
            }),
        }
    }

    pub async fn serve<C: RuntimeControlPlane>(&self, control_plane: &mut C) -> Result<()> {
        while let Some(effect) = control_plane.next_effect().await? {
            let result = self.apply_effect(effect).await?;
            control_plane.publish(result).await?;
        }
        Ok(())
    }

    pub async fn restore_authority(&self) -> Result<()> {
        self.host.lifecycle()?.restore_authority().await
    }

    pub fn bind_replica_session(
        &self,
        resource: kuberic_protocol::types::ResourceUid,
        session: kuberic_protocol::types::ProcessSessionId,
    ) -> Result<()> {
        let binding = (resource, session);
        if self.host.replica_session.get() == Some(&binding) {
            return Ok(());
        }
        // The operation engine already owns its stream-session lifetime. Existing
        // hosts may attach a fresh control service during durable reconstruction.
        if self
            .host
            .registered
            .get()
            .is_some_and(|r| r.managed.is_some())
        {
            return Ok(());
        }
        if self.host.registered.get().is_some() || binding.0.is_empty() || binding.1.is_empty() {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        let mut state = self
            .host
            .state
            .try_write()
            .map_err(|_| RuntimeError::ReconfigurationPending)?;
        state.partition_information.partition_id = PartitionId::new(binding.0.as_str());
        self.host
            .replica_session
            .set(binding)
            .map_err(|_| RuntimeError::AuthorityNotAdmitted)
    }

    pub async fn authorize_build(
        &self,
        build_id: kuberic_protocol::types::OperationId,
        target: ReplicaIdentity,
        configuration: BuildConfiguration,
    ) -> Result<BuildAuthority> {
        let snapshot = self.snapshot().await;
        let (kind, current_configuration) = match configuration {
            BuildConfiguration::Current => {
                let authority = snapshot
                    .authority
                    .ok_or(RuntimeError::AuthorityNotAdmitted)?;
                let kind = if authority
                    .current_configuration
                    .members
                    .iter()
                    .any(|member| member.identity == target)
                    || authority.transition_kind
                        == Some(kuberic_protocol::types::TransitionKind::Failover)
                {
                    BuildAuthorityKind::Failover
                } else {
                    BuildAuthorityKind::Provisioning
                };
                (kind, authority.current_configuration)
            }
            BuildConfiguration::Bootstrap(configuration) => {
                (BuildAuthorityKind::Bootstrap, configuration)
            }
        };
        if let Some(existing) = self
            .host
            .default_dependencies
            .build_authority_store
            .load_build(&build_id)
            .await?
        {
            if existing.kind != kind
                || existing.source != self.host.identity
                || existing.target != target
                || existing.current_configuration != current_configuration
            {
                return Err(RuntimeError::AuthorityMismatch(
                    "build ID is already bound to different exact authority".into(),
                ));
            }
            if let Some(custom) = self.host.registered.get().and_then(|r| r.custom.as_ref()) {
                custom.select_build(&existing).await?;
            }
            return Ok(existing);
        }
        let authority = BuildAuthority {
            build_id,
            kind,
            source: self.host.identity.clone(),
            target,
            current_configuration,
            replication_boundary_lsn: snapshot.committed_lsn,
        };
        authority.validate()?;
        self.host
            .default_dependencies
            .build_authority_store
            .admit_build(&authority)
            .await?;
        if let Some(custom) = self.host.registered.get().and_then(|r| r.custom.as_ref()) {
            custom.select_build(&authority).await?;
        }
        Ok(authority)
    }

    pub async fn reconstruct(
        &self,
        mode: OpenMode,
        role: ReplicaRole,
        read_status: AccessStatus,
        write_status: AccessStatus,
        transition: Option<(ReplicaRole, bool, bool)>,
    ) -> Result<()> {
        let store = &self.host.default_dependencies.replica_authority_store;
        let retired = match store.load_retired_authority().await? {
            Some(retired) => Some(retired),
            None => {
                if let Some(started) = store.load_retirement_started().await? {
                    started.validate(&self.host.identity)?;
                    // This fresh host has never opened. Process termination closed
                    // the prior host, so the exact durable intent can now finish.
                    if self.host.registered.get().is_some() {
                        return Err(RuntimeError::ReconfigurationPending);
                    }
                    store.retire(&started).await?;
                    Some(started)
                } else {
                    None
                }
            }
        };
        if let Some(retired) = retired {
            retired.validate(&self.host.identity)?;
            let mut state = self.host.state.write().await;
            state.fallback_snapshot = empty_snapshot(self.host.identity.clone());
            state.fallback_snapshot.read_status = AccessStatus::NotPrimary;
            state.fallback_snapshot.write_status = AccessStatus::NotPrimary;
            state.fallback_snapshot.retired_authority = Some(retired);
            self.host.closed.store(true, Ordering::Release);
            return Ok(());
        }
        let has_transition = transition.is_some();
        if !self.host.snapshot().await.open {
            self.host.open(mode).await?;
        }
        if let Ok(managed) = self.host.lifecycle() {
            self.restore_authority().await?;
            self.host
                .sync_access_projection(managed.snapshot().await)
                .await;
        }
        if let Some((target_role, epoch_completed, application_completed)) = transition {
            self.host.change_replicator_role(target_role).await?;
            if target_role == ReplicaRole::Primary && epoch_completed {
                self.host.update_epoch().await?;
            }
            if application_completed {
                self.host.change_application_role(target_role).await?;
            }
        } else if role != ReplicaRole::None {
            self.host.change_replicator_role(role).await?;
            if role == ReplicaRole::Primary {
                self.host.update_epoch().await?;
            }
            self.host.change_application_role(role).await?;
        }
        let (read_status, write_status) = if has_transition {
            (
                AccessStatus::ReconfigurationPending,
                AccessStatus::ReconfigurationPending,
            )
        } else {
            (read_status, write_status)
        };
        if let Ok(managed) = self.host.lifecycle() {
            if write_status == AccessStatus::Granted
                && let Some(committed) = self
                    .host
                    .default_dependencies
                    .replica_authority_store
                    .load_secondary_removal_commit()
                    .await?
                && managed
                    .snapshot()
                    .await
                    .authority
                    .as_ref()
                    .is_some_and(|a| {
                        a.previous_configuration.is_none()
                            && a.secondary_removal.as_ref() == Some(&committed.evidence)
                    })
            {
                managed
                    .execute_action(RuntimeEffectAction::AcceptSecondaryRemovalCommit(Box::new(
                        committed,
                    )))
                    .await?;
            }
            match &managed {
                HostedLifecycle::ServicePrimary(custom) => {
                    match custom.restore_access(read_status, write_status).await {
                        Err(RuntimeError::ReconfigurationPending) => {
                            tracing::info!("custom replica access restoration deferred");
                        }
                        result => result?,
                    }
                }
                HostedLifecycle::DefaultEngine(_) => {
                    managed
                        .execute_action(RuntimeEffectAction::SetAccessStatus {
                            read: read_status,
                            write: write_status,
                        })
                        .await?;
                }
            }
            self.host
                .sync_access_projection(managed.snapshot().await)
                .await;
        } else {
            let mut state = self.host.state.write().await;
            state.fallback_snapshot.read_status = read_status;
            state.fallback_snapshot.write_status = write_status;
        }
        Ok(())
    }

    pub(crate) async fn restore_accepted_removal(
        &self,
        committed: kuberic_protocol::types::SecondaryScaleDownCleanup,
        historical: Option<kuberic_protocol::command::AcceptSecondaryRemovalCommit>,
    ) -> Result<()> {
        self.host
            .lifecycle()?
            .execute_action(match historical {
                Some(command) => {
                    RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(Box::new(command))
                }
                None => RuntimeEffectAction::AcceptSecondaryRemovalCommit(Box::new(committed)),
            })
            .await
    }

    pub fn abort(&self) {
        self.host.abort();
    }

    pub async fn apply_effect(&self, effect: RuntimeEffect) -> Result<RuntimeEffectResult> {
        Box::pin(self.host.apply_effect(effect)).await
    }

    pub(crate) async fn consume_cancelled_build_effect(
        &self,
        effect: RuntimeEffect,
    ) -> Result<RuntimeEffectResult> {
        self.host.consume_cancelled_build_effect(effect).await
    }

    pub async fn cancel_configuration_work(&self) -> Result<()> {
        if let Ok(managed) = self.host.lifecycle() {
            managed.cancel_configuration_work().await
        } else {
            Ok(())
        }
    }

    pub fn data_plane(&self) -> RuntimeDataPlane {
        RuntimeDataPlane {
            host: self.host.clone(),
        }
    }

    pub async fn snapshot(&self) -> RuntimeSnapshot {
        self.host.snapshot().await
    }

    pub async fn primary_replicator(&self) -> Result<Arc<dyn PrimaryReplicator>> {
        self.host
            .registered
            .get()
            .and_then(|registered| registered.primary())
            .ok_or(RuntimeError::NotOpen)
    }

    pub async fn catch_up_capability(&self) -> Result<i64> {
        self.host
            .registered
            .get()
            .ok_or(RuntimeError::NotOpen)?
            .catch_up_capability()
            .await
    }

    pub async fn cancel_outbound_build(&self, build_id: &OperationId) -> Result<()> {
        self.host.lifecycle()?.cancel_outbound_build(build_id).await
    }

    pub async fn repair_peer(&self, identity: ReplicaIdentity, progress: i64) -> Result<()> {
        self.host.managed()?.repair_peer(identity, progress).await
    }

    pub(crate) async fn register_peer_session(
        &self,
        identity: ReplicaIdentity,
        session: kuberic_protocol::types::ProcessSessionId,
    ) -> Result<()> {
        self.host
            .lifecycle()?
            .execute_action(RuntimeEffectAction::RegisterPeerSession { identity, session })
            .await
    }

    pub(crate) async fn register_custom_peer_session(
        &self,
        identity: ReplicaIdentity,
        session: kuberic_protocol::types::ProcessSessionId,
    ) -> Result<()> {
        if let Some(custom) = self.host.registered.get().and_then(|r| r.custom.as_ref()) {
            custom
                .execute_action(RuntimeEffectAction::RegisterPeerSession { identity, session })
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn describe_custom_peer(
        &self,
        replica: kuberic_runtime::replicator::ReplicaInformation,
    ) -> Result<()> {
        if let Some(custom) = self.host.registered.get().and_then(|r| r.custom.as_ref()) {
            custom.describe_peer(replica).await?;
        }
        Ok(())
    }

    pub(crate) async fn refresh_custom_progress(&self) -> Result<()> {
        if let Some(custom) = self.host.registered.get().and_then(|r| r.custom.as_ref()) {
            if !custom.snapshot().await.open {
                return Ok(());
            }
            custom
                .execute_action(RuntimeEffectAction::RefreshApplicationProgress)
                .await?;
        }
        Ok(())
    }

    pub async fn execute_custom_build(
        &self,
        replica: kuberic_runtime::replicator::ReplicaInformation,
    ) -> Result<bool> {
        if let Some(custom) = self.host.registered.get().and_then(|r| r.custom.as_ref()) {
            custom.execute_build(replica).await?;
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) async fn next_outbound(&self) -> Option<OutboundOperation> {
        self.host.lifecycle().ok()?.next_outbound().await
    }

    pub(crate) async fn observe_secondary_removal_witness(
        &self,
        witness: kuberic_protocol::types::SecondaryRemovalWitness,
        committed: Option<kuberic_protocol::types::SecondaryScaleDownCleanup>,
    ) -> Result<()> {
        let action = match committed {
            Some(committed) => RuntimeEffectAction::ObserveSecondaryRemovalProgress {
                witness: Box::new(witness),
                committed: Box::new(committed),
            },
            None => RuntimeEffectAction::ObserveSecondaryRemovalWitness(Box::new(witness)),
        };
        self.host.lifecycle()?.execute_action(action).await
    }

    pub async fn partition_report(&self) -> PartitionReportSnapshot {
        let state = self.host.state.read().await;
        PartitionReportSnapshot {
            information: state.partition_information.clone(),
            read_status: state.fallback_snapshot.read_status,
            write_status: state.fallback_snapshot.write_status,
            load_metrics: state
                .load_metrics
                .iter()
                .map(|(name, value)| LoadMetric {
                    name: name.clone(),
                    value: *value,
                })
                .collect(),
            reported_fault: state.reported_fault,
        }
    }
}

impl RuntimeDataPlane {
    pub async fn begin_write(&self, write: ClientWrite) -> Result<PendingWrite> {
        let pending = self.host.streams()?.begin_write(write).await?;
        Ok(PendingWrite {
            lsn: pending.lsn,
            replication_items: pending
                .replication_items
                .iter()
                .cloned()
                .map(replication_to_proto)
                .collect(),
            build_items: pending
                .build_items
                .iter()
                .cloned()
                .map(copy_to_proto)
                .collect(),
            inner: pending,
        })
    }

    pub async fn accept_acknowledgement(
        &self,
        acknowledgement: proto::ReplicationAck,
    ) -> Result<()> {
        let session = acknowledgement.receiver_session_id.clone();
        let acknowledgement = replication_ack_from_proto(acknowledgement)?;
        if !session.is_empty() {
            return self
                .host
                .streams()?
                .execute_action(RuntimeEffectAction::ObserveReplicationAck {
                    acknowledgement: Box::new(acknowledgement),
                    session: kuberic_protocol::types::ProcessSessionId::new(session),
                })
                .await;
        }
        self.host
            .streams()?
            .accept_acknowledgement(acknowledgement)
            .await
    }

    pub async fn prepare_copy(&self, request: PrepareCopyRequest) -> Result<PreparedCopy> {
        let RuntimePreparedCopy { authority, items } =
            self.host.streams()?.prepare_copy(request).await?;
        Ok(PreparedCopy {
            authority,
            items: Box::pin(items.map(|item| item.map(copy_to_proto))),
        })
    }

    pub async fn accept_copy_acknowledgement(&self, ack: proto::CopyAck) -> Result<()> {
        self.host
            .streams()?
            .accept_copy_acknowledgement(copy_ack_from_proto(ack)?)
            .await
    }

    pub async fn receive_copy_item(&self, item: proto::CopyItem) -> Result<proto::CopyAck> {
        self.host
            .streams()?
            .receive_copy_item(copy_from_proto(item)?)
            .await
            .map(copy_ack_to_proto)
    }

    pub async fn receive_replication(
        &self,
        item: proto::ReplicationItem,
    ) -> Result<PendingReplication> {
        let pending = self
            .host
            .streams()?
            .receive_replication(replication_from_proto(item)?)
            .await?;
        Ok(PendingReplication {
            received: replication_ack_to_proto(pending.received.clone()),
            inner: pending,
        })
    }

    pub async fn next_outbound(&self) -> Option<OutboundReplication> {
        self.host
            .lifecycle()
            .ok()?
            .next_outbound()
            .await
            .map(|outbound| match outbound {
                OutboundOperation::Replication(item) => {
                    OutboundReplication::Replication(replication_to_proto(item))
                }
                OutboundOperation::Copy(item) => OutboundReplication::Copy(copy_to_proto(item)),
                OutboundOperation::Build(replica) => OutboundReplication::Build(replica),
                OutboundOperation::Remove(replica_id) => OutboundReplication::Remove(replica_id),
                OutboundOperation::Evict(identity) => OutboundReplication::Evict(identity),
            })
    }
}

struct RuntimeHost {
    replica_session: OnceLock<(
        kuberic_protocol::types::ResourceUid,
        kuberic_protocol::types::ProcessSessionId,
    )>,
    identity: ReplicaIdentity,
    application: Arc<dyn StatefulServiceReplica>,
    default_dependencies: DefaultReplicatorDependencies,
    state: RwLock<HostState>,
    effect_lock: Mutex<()>,
    registered: OnceLock<RegisteredReplicator>,
    pending_managed: StdMutex<Option<(u64, Arc<dyn ManagedReplicator>)>>,
    weak_self: Weak<Self>,
    aborted: AtomicBool,
    closed: AtomicBool,
    replicator_creation: AtomicU8,
}

struct HostAccessView {
    host: Weak<RuntimeHost>,
    partition_information: PartitionInformation,
}

#[async_trait]
impl PartitionAccessView for HostAccessView {
    fn partition_information(&self) -> PartitionInformation {
        self.partition_information.clone()
    }

    async fn read_status(&self) -> Result<AccessStatus> {
        let host = self.host.upgrade().ok_or(RuntimeError::Closed)?;
        Ok(host.state.read().await.fallback_snapshot.read_status)
    }

    async fn write_status(&self) -> Result<AccessStatus> {
        let host = self.host.upgrade().ok_or(RuntimeError::Closed)?;
        Ok(host.state.read().await.fallback_snapshot.write_status)
    }

    async fn report_load(&self, metrics: Vec<LoadMetric>) -> Result<()> {
        let host = self.host.upgrade().ok_or(RuntimeError::Closed)?;
        let _effect = host.effect_lock.lock().await;
        if host.closed.load(Ordering::Acquire) || host.aborted.load(Ordering::Acquire) {
            return Err(RuntimeError::Closed);
        }
        let mut names = std::collections::BTreeSet::new();
        if metrics.iter().any(|metric| {
            metric.name.is_empty() || metric.value < 0 || !names.insert(metric.name.clone())
        }) {
            return Err(RuntimeError::Application(
                "load metrics require unique nonempty names and nonnegative values".into(),
            ));
        }
        host.state.write().await.load_metrics = metrics
            .into_iter()
            .map(|metric| (metric.name, metric.value))
            .collect();
        Ok(())
    }

    async fn report_fault(&self, fault: FaultType) -> Result<()> {
        let host = self.host.upgrade().ok_or(RuntimeError::Closed)?;
        // Applications may report a fault from Open/change_role while the host
        // already owns effect_lock. Reports change diagnostics, not authority;
        // the state lock serializes them without re-entering a lifecycle effect.
        let mut state = host.state.write().await;
        if host.closed.load(Ordering::Acquire) || host.aborted.load(Ordering::Acquire) {
            return Err(RuntimeError::Closed);
        }
        if state.reported_fault != Some(FaultType::Permanent) {
            state.reported_fault = Some(fault);
        }
        Ok(())
    }
}

#[async_trait]
impl ReplicatorRegistration for RuntimeHost {
    fn reserve_replicator_creation(&self) -> Result<ReplicatorCreationReservation> {
        self.replicator_creation
            .compare_exchange(
                REPLICATOR_CREATION_AVAILABLE,
                REPLICATOR_CREATION_RESERVED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| {
                RuntimeError::Application(
                    "CreateReplicator may be called only once per Open".into(),
                )
            })?;
        Ok(ReplicatorCreationReservation(REPLICATOR_RESERVATION_ID))
    }

    fn cancel_replicator_creation(&self, reservation: ReplicatorCreationReservation) {
        if reservation.0 == REPLICATOR_RESERVATION_ID {
            if let Ok(mut pending) = self.pending_managed.lock()
                && pending.as_ref().is_some_and(|(id, _)| *id == reservation.0)
            {
                if let Some((_, managed)) = pending.take() {
                    managed.abort();
                }
            }
            let _ = self.replicator_creation.compare_exchange(
                REPLICATOR_CREATION_RESERVED,
                REPLICATOR_CREATION_AVAILABLE,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }

    async fn register_managed(
        &self,
        managed: Arc<dyn ManagedReplicator>,
        reservation: ReplicatorCreationReservation,
    ) -> Result<()> {
        if reservation.0 != REPLICATOR_RESERVATION_ID
            || self.replicator_creation.load(Ordering::Acquire) != REPLICATOR_CREATION_RESERVED
        {
            return Err(RuntimeError::Application(
                "CreateReplicator reservation is not active".into(),
            ));
        }
        let mut pending = self.pending_managed.lock().map_err(|_| {
            RuntimeError::Application("managed replicator registration was poisoned".into())
        })?;
        if pending.is_some() {
            return Err(RuntimeError::Application(
                "managed replicator may be registered only once".into(),
            ));
        }
        *pending = Some((reservation.0, managed));
        Ok(())
    }

    async fn register_interfaces(
        &self,
        interfaces: &ReplicatorInterfaces,
        provider: Option<Arc<dyn StateProvider>>,
        reservation: ReplicatorCreationReservation,
    ) -> Result<()> {
        if reservation.0 != REPLICATOR_RESERVATION_ID
            || self.replicator_creation.load(Ordering::Acquire) != REPLICATOR_CREATION_RESERVED
        {
            return Err(RuntimeError::Application(
                "CreateReplicator reservation is not active".into(),
            ));
        }
        let managed_replicator = self
            .pending_managed
            .lock()
            .map_err(|_| {
                RuntimeError::Application("managed replicator registration was poisoned".into())
            })?
            .take()
            .and_then(|(id, managed)| (id == reservation.0).then_some(managed));
        if let Some(managed) = managed_replicator.as_ref() {
            managed
                .attach_interfaces(interfaces.replicator(), interfaces.primary_replicator())
                .await?;
        }
        let custom = if managed_replicator.is_none() {
            interfaces.primary_replicator().map(|primary| {
                Arc::new(custom::CustomReplicatorHost::new(
                    self.weak_self.clone(),
                    interfaces.replicator(),
                    primary,
                ))
            })
        } else {
            None
        };
        self.registered
            .set(RegisteredReplicator {
                control: interfaces.replicator(),
                primary: interfaces.primary_replicator(),
                provider,
                managed: managed_replicator,
                custom,
            })
            .map_err(|_| {
                RuntimeError::Application(
                    "CreateReplicator may be called only once per Open".into(),
                )
            })?;
        self.replicator_creation
            .compare_exchange(
                REPLICATOR_CREATION_RESERVED,
                REPLICATOR_CREATION_REGISTERED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| {
                RuntimeError::Application(
                    "CreateReplicator reservation was lost before registration".into(),
                )
            })?;
        Ok(())
    }
}

struct OpenAttempt<'a> {
    host: &'a RuntimeHost,
    complete: bool,
}

impl Drop for OpenAttempt<'_> {
    fn drop(&mut self) {
        if !self.complete {
            self.host.abort();
        }
    }
}

impl RuntimeHost {
    fn streams(&self) -> Result<Arc<dyn ManagedReplicator>> {
        self.managed()
    }
    fn lifecycle(&self) -> Result<HostedLifecycle> {
        self.registered
            .get()
            .ok_or(RuntimeError::NotOpen)?
            .lifecycle()
            .ok_or_else(|| {
                RuntimeError::Application(
                    "replicator does not expose primary lifecycle capabilities".into(),
                )
            })
    }
    fn managed(&self) -> Result<Arc<dyn ManagedReplicator>> {
        let registered = self.registered.get().ok_or(RuntimeError::NotOpen)?;
        registered.managed().ok_or_else(|| {
            RuntimeError::Application(
                "the selected replicator does not expose default-engine managed data-plane capabilities"
                    .into(),
            )
        })
    }

    fn abort(&self) {
        if self.closed.load(Ordering::Acquire) || self.aborted.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(registered) = self.registered.get() {
            registered.abort();
        } else if let Ok(mut pending) = self.pending_managed.lock()
            && let Some((_, managed)) = pending.take()
        {
            managed.abort();
        }
        self.application.abort();
    }

    async fn apply_effect(&self, effect: RuntimeEffect) -> Result<RuntimeEffectResult> {
        let _guard = self.effect_lock.lock().await;
        if !matches!(
            effect.action,
            RuntimeEffectAction::RetireReplica(_) | RuntimeEffectAction::Abort
        ) && (self
            .default_dependencies
            .replica_authority_store
            .load_retired_authority()
            .await?
            .is_some()
            || self
                .default_dependencies
                .replica_authority_store
                .load_retirement_started()
                .await?
                .is_some())
        {
            return Err(RuntimeError::Closed);
        }
        {
            let state = self.state.read().await;
            if let Some(previous) = state.effects.get(&effect.sequence) {
                if effect == previous.effect {
                    return Ok(previous.result.clone());
                }
                return Err(RuntimeError::EffectConflict {
                    sequence: effect.sequence,
                });
            }
            let expected = state
                .effects
                .last_key_value()
                .map_or(Some(effect.sequence), |(sequence, _)| {
                    sequence.checked_add(1)
                })
                .ok_or_else(|| {
                    RuntimeError::InvalidReplication("effect sequence exhausted".into())
                })?;
            if effect.sequence != expected {
                return Err(RuntimeError::EffectOutOfOrder {
                    expected,
                    observed: effect.sequence,
                });
            }
            if state.fallback_snapshot.role_transition.is_some()
                && matches!(
                    effect.action,
                    RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted)
                        | RuntimeEffectAction::SetReadStatus(AccessStatus::Granted)
                        | RuntimeEffectAction::SetAccessStatus {
                            read: AccessStatus::Granted,
                            ..
                        }
                        | RuntimeEffectAction::SetAccessStatus {
                            write: AccessStatus::Granted,
                            ..
                        }
                )
            {
                return Err(RuntimeError::ReconfigurationPending);
            }
        }
        if !matches!(
            effect.action,
            RuntimeEffectAction::Abort | RuntimeEffectAction::RetireReplica(_)
        ) && (self.aborted.load(Ordering::Acquire) || self.closed.load(Ordering::Acquire))
        {
            return Err(RuntimeError::Closed);
        }
        match effect.action.clone() {
            RuntimeEffectAction::RetireReplica(retired) => {
                retired.validate(&self.identity)?;
                if let Some(durable) = self
                    .default_dependencies
                    .replica_authority_store
                    .load_retired_authority()
                    .await?
                {
                    if durable != *retired {
                        return Err(RuntimeError::AuthorityMismatch(
                            "conflicting terminal retirement".into(),
                        ));
                    }
                    if let Ok(managed) = self.lifecycle() {
                        managed
                            .execute_action(RuntimeEffectAction::CompleteRetirement(retired))
                            .await?;
                    } else {
                        self.state.write().await.fallback_snapshot.retired_authority =
                            Some(durable);
                        self.closed.store(true, Ordering::Release);
                    }
                } else {
                    let managed = self.lifecycle()?;
                    if !self.closed.load(Ordering::Acquire) {
                        managed
                            .execute_action(RuntimeEffectAction::FenceRetirement(retired.clone()))
                            .await?;
                        self.sync_access_projection(managed.snapshot().await).await;
                        self.change_replicator_role_at_epoch(
                            ReplicaRole::None,
                            Some(retired.report.epoch),
                        )
                        .await?;
                        self.change_application_role(ReplicaRole::None).await?;
                        self.close().await?;
                    }
                    managed
                        .execute_action(RuntimeEffectAction::CompleteRetirement(retired))
                        .await?;
                }
            }
            RuntimeEffectAction::Open(mode) => self.open(mode).await?,
            RuntimeEffectAction::ChangeRole(role) => self.change_role(role).await?,
            RuntimeEffectAction::ChangeReplicatorRole(role) => {
                self.change_replicator_role(role).await?
            }
            RuntimeEffectAction::UpdateEpoch => self.update_epoch().await?,
            RuntimeEffectAction::ChangeApplicationRole(role) => {
                self.change_application_role(role).await?
            }
            RuntimeEffectAction::BuildReplica {
                build_id,
                target,
                replication_address,
            } => {
                if let Some(custom) = self.registered.get().and_then(|r| r.custom.as_ref()) {
                    custom
                        .enqueue_build(ReplicaEndpoint {
                            build_id,
                            identity: target,
                            replication_address,
                        })
                        .await?;
                } else {
                    self.registered
                        .get()
                        .and_then(|registered| registered.primary())
                        .ok_or(RuntimeError::NotPrimary)?
                        .build_replica(kuberic_runtime::replicator::ReplicaInformation::new(
                            build_id,
                            target,
                            replication_address,
                        ))
                        .await?;
                }
            }
            RuntimeEffectAction::Close => self.close().await?,
            RuntimeEffectAction::Abort => self.abort_action().await,
            action => {
                if let Ok(managed) = self.lifecycle() {
                    let result = managed.execute_action(action).await;
                    self.sync_access_projection(managed.snapshot().await).await;
                    result?;
                } else {
                    self.execute_custom_action(action).await?;
                }
            }
        }
        let result = RuntimeEffectResult {
            operation_id: effect.operation_id.clone(),
            sequence: effect.sequence,
            postcondition: snapshot_postcondition(self.snapshot().await),
        };
        self.state.write().await.effects.insert(
            result.sequence,
            AppliedEffect {
                effect,
                result: result.clone(),
            },
        );
        Ok(result)
    }

    async fn consume_cancelled_build_effect(
        &self,
        effect: RuntimeEffect,
    ) -> Result<RuntimeEffectResult> {
        let build_id = match &effect.action {
            RuntimeEffectAction::BuildReplica { build_id, .. } => build_id,
            _ => {
                return Err(RuntimeError::Application(
                    "only a build effect can be consumed as cancelled".into(),
                ));
            }
        };
        let _guard = self.effect_lock.lock().await;
        {
            let state = self.state.read().await;
            if let Some(previous) = state.effects.get(&effect.sequence) {
                if effect == previous.effect {
                    return Ok(previous.result.clone());
                }
                return Err(RuntimeError::EffectConflict {
                    sequence: effect.sequence,
                });
            }
            let expected = state
                .effects
                .last_key_value()
                .map_or(Some(effect.sequence), |(sequence, _)| {
                    sequence.checked_add(1)
                })
                .ok_or_else(|| {
                    RuntimeError::InvalidReplication("effect sequence exhausted".into())
                })?;
            if effect.sequence != expected {
                return Err(RuntimeError::EffectOutOfOrder {
                    expected,
                    observed: effect.sequence,
                });
            }
        }
        self.lifecycle()?.cancel_outbound_build(build_id).await?;
        let snapshot = self.snapshot().await;
        if snapshot
            .builds
            .iter()
            .any(|build| &build.authority.build_id == build_id)
        {
            return Err(RuntimeError::ReconfigurationPending);
        }
        let result = RuntimeEffectResult {
            operation_id: effect.operation_id.clone(),
            sequence: effect.sequence,
            postcondition: snapshot_postcondition(snapshot),
        };
        self.state.write().await.effects.insert(
            result.sequence,
            AppliedEffect {
                effect,
                result: result.clone(),
            },
        );
        Ok(result)
    }

    async fn open(&self, mode: OpenMode) -> Result<()> {
        if self
            .default_dependencies
            .replica_authority_store
            .load_retired_authority()
            .await?
            .is_some()
            || self
                .default_dependencies
                .replica_authority_store
                .load_retirement_started()
                .await?
                .is_some()
        {
            return Err(RuntimeError::Closed);
        }
        if self.registered.get().is_some() {
            return Err(RuntimeError::Application("replica already opened".into()));
        }
        let mut attempt = OpenAttempt {
            host: self,
            complete: false,
        };
        let registration: Arc<dyn ReplicatorRegistration> =
            self.weak_self.upgrade().ok_or(RuntimeError::Closed)?;
        let context = ReplicatorFactoryContext::new(
            RuntimeHostToken::new(),
            self.identity.clone(),
            Arc::new(HostAccessView {
                host: self.weak_self.clone(),
                partition_information: self.state.read().await.partition_information.clone(),
            }),
            registration.clone(),
            self.default_dependencies.clone(),
        );
        let registration = self
            .application
            .clone()
            .open(OpenContext {
                identity: self.identity.clone(),
                mode,
                partition: StatefulServicePartition::new(
                    RuntimeHostToken::new(),
                    registration,
                    context,
                ),
            })
            .await?;
        let registered = self.registered.get().ok_or(RuntimeError::NotOpen)?;
        if !Arc::ptr_eq(&registration, &registered.control) {
            return Err(RuntimeError::Application(
                "Open returned a different replica than its registration".into(),
            ));
        }
        let address = registered.open().await?;
        if let Some(hosted) = registered.lifecycle() {
            self.sync_access_projection(hosted.snapshot().await).await;
        } else {
            let progress = registered.current_progress().await?;
            let committed = registered.provider()?.last_committed_lsn().await?;
            let mut state = self.state.write().await;
            state.fallback_snapshot.open = true;
            state.fallback_snapshot.current_progress = progress;
            state.fallback_snapshot.committed_lsn = committed;
        }
        {
            let mut state = self.state.write().await;
            state.fallback_snapshot.open = true;
            state.fallback_snapshot.replication_address = address;
        }
        attempt.complete = true;
        Ok(())
    }

    async fn change_role(&self, role: ReplicaRole) -> Result<()> {
        self.change_replicator_role(role).await?;
        if role == ReplicaRole::Primary {
            self.update_epoch().await?;
        }
        self.change_application_role(role).await
    }

    async fn change_replicator_role(&self, role: ReplicaRole) -> Result<()> {
        self.change_replicator_role_at_epoch(role, None).await
    }

    async fn change_replicator_role_at_epoch(
        &self,
        role: ReplicaRole,
        retirement_epoch: Option<Epoch>,
    ) -> Result<()> {
        let registered = self.registered.get().ok_or(RuntimeError::NotOpen)?;
        let snapshot = self.snapshot().await;
        if !snapshot.open {
            return Err(RuntimeError::NotOpen);
        }
        let epoch = retirement_epoch.unwrap_or_else(|| {
            snapshot
                .authority
                .as_ref()
                .map_or_else(Epoch::default, |authority| {
                    authority.current_configuration.epoch
                })
        });
        let transition = {
            let mut state = self.state.write().await;
            state.fallback_snapshot.write_status = AccessStatus::ReconfigurationPending;
            if state.role_transition_epoch.is_some_and(|old| epoch > old) {
                state.fallback_snapshot.role_transition = None;
            }
            if let Some(transition) = state.fallback_snapshot.role_transition.clone() {
                if transition.target_role != role {
                    return Err(RuntimeError::ReconfigurationPending);
                }
                transition
            } else {
                let transition = RoleTransition {
                    completed_role: snapshot.role,
                    target_role: role,
                    replicator_completed: false,
                    epoch_completed: role != ReplicaRole::Primary,
                    application_completed: false,
                };
                state.fallback_snapshot.role_transition = Some(transition.clone());
                state.role_transition_epoch = Some(epoch);
                transition
            }
        };
        if let Ok(managed) = self.lifecycle() {
            managed.fence_writes().await?;
        }
        self.state.write().await.fallback_snapshot.read_status =
            AccessStatus::ReconfigurationPending;
        if !transition.replicator_completed {
            registered.change_role(epoch, role).await?;
            let mut state = self.state.write().await;
            state
                .fallback_snapshot
                .role_transition
                .as_mut()
                .ok_or(RuntimeError::ReconfigurationPending)?
                .replicator_completed = true;
        }
        Ok(())
    }

    async fn update_epoch(&self) -> Result<()> {
        let registered = self.registered.get().ok_or(RuntimeError::NotOpen)?;
        let transition = self
            .state
            .read()
            .await
            .fallback_snapshot
            .role_transition
            .clone()
            .ok_or(RuntimeError::ReconfigurationPending)?;
        if transition.target_role != ReplicaRole::Primary || !transition.replicator_completed {
            return Err(RuntimeError::ReconfigurationPending);
        }
        if !transition.epoch_completed {
            let epoch = self
                .snapshot()
                .await
                .authority
                .as_ref()
                .map_or_else(Epoch::default, |authority| {
                    authority.current_configuration.epoch
                });
            registered.update_epoch(epoch).await?;
            let mut state = self.state.write().await;
            state
                .fallback_snapshot
                .role_transition
                .as_mut()
                .ok_or(RuntimeError::ReconfigurationPending)?
                .epoch_completed = true;
        }
        Ok(())
    }

    async fn change_application_role(&self, role: ReplicaRole) -> Result<()> {
        let transition = self
            .state
            .read()
            .await
            .fallback_snapshot
            .role_transition
            .clone()
            .ok_or(RuntimeError::ReconfigurationPending)?;
        if transition.target_role != role
            || !transition.replicator_completed
            || (role == ReplicaRole::Primary && !transition.epoch_completed)
        {
            return Err(RuntimeError::ReconfigurationPending);
        }
        if !transition.application_completed {
            if role == ReplicaRole::Primary
                && let Some(managed) = self.registered.get().and_then(|r| r.lifecycle())
            {
                managed.settle_primary_prefix().await?;
            }
            let _ = self.application.change_role(role).await?;
        }
        let mut state = self.state.write().await;
        state
            .fallback_snapshot
            .role_transition
            .as_mut()
            .ok_or(RuntimeError::ReconfigurationPending)?
            .application_completed = true;
        state.fallback_snapshot.role = role;
        state.fallback_snapshot.role_transition = None;
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        let registered = self.registered.get().ok_or(RuntimeError::NotOpen)?;
        {
            let mut state = self.state.write().await;
            state.fallback_snapshot.open = false;
            state.fallback_snapshot.read_status = AccessStatus::ReconfigurationPending;
            state.fallback_snapshot.write_status = AccessStatus::ReconfigurationPending;
        }
        if let Ok(managed) = self.lifecycle() {
            managed.fence_writes().await?;
        }
        if let Err(error) = registered.close().await {
            self.abort();
            return Err(error);
        }
        if let Err(error) = self.application.close().await {
            self.application.abort();
            self.closed.store(true, Ordering::Release);
            let mut state = self.state.write().await;
            state.fallback_snapshot.role = ReplicaRole::None;
            state.fallback_snapshot.role_transition = None;
            state.fallback_snapshot.read_status = AccessStatus::NotPrimary;
            state.fallback_snapshot.write_status = AccessStatus::NotPrimary;
            return Err(error);
        }
        self.closed.store(true, Ordering::Release);
        let mut state = self.state.write().await;
        state.fallback_snapshot.role = ReplicaRole::None;
        state.fallback_snapshot.role_transition = None;
        state.fallback_snapshot.read_status = AccessStatus::NotPrimary;
        state.fallback_snapshot.write_status = AccessStatus::NotPrimary;
        Ok(())
    }

    async fn abort_action(&self) {
        {
            let mut state = self.state.write().await;
            state.fallback_snapshot.open = false;
            state.fallback_snapshot.role = ReplicaRole::None;
            state.fallback_snapshot.role_transition = None;
            state.fallback_snapshot.read_status = AccessStatus::NotPrimary;
            state.fallback_snapshot.write_status = AccessStatus::NotPrimary;
        }
        self.abort();
    }

    async fn sync_access_projection(&self, managed_snapshot: RuntimeSnapshot) {
        let mut state = self.state.write().await;
        state.fallback_snapshot.read_status = managed_snapshot.read_status;
        state.fallback_snapshot.write_status = managed_snapshot.write_status;
        state.fallback_snapshot.authority = managed_snapshot.authority;
    }

    async fn execute_custom_action(&self, action: RuntimeEffectAction) -> Result<()> {
        let registered = self.registered.get().ok_or(RuntimeError::NotOpen)?;
        match action {
            RuntimeEffectAction::SetAccessStatus { read, write } => {
                let mut state = self.state.write().await;
                state.fallback_snapshot.read_status = read;
                state.fallback_snapshot.write_status = write;
            }
            RuntimeEffectAction::SetReadStatus(status) => {
                self.state.write().await.fallback_snapshot.read_status = status;
            }
            RuntimeEffectAction::SetWriteStatus(status) => {
                self.state.write().await.fallback_snapshot.write_status = status;
            }
            RuntimeEffectAction::RefreshApplicationProgress => {
                let current = registered.current_progress().await?;
                let committed = registered.provider()?.last_committed_lsn().await?;
                let mut state = self.state.write().await;
                state.fallback_snapshot.current_progress = current;
                state.fallback_snapshot.committed_lsn = committed;
            }
            RuntimeEffectAction::AdmitAuthority(_)
            | RuntimeEffectAction::AuthorizeFailoverPrefix(_)
            | RuntimeEffectAction::AdmitBuildAuthority(_)
            | RuntimeEffectAction::PrepareSwitchover { .. }
            | RuntimeEffectAction::WaitForCatchup
            | RuntimeEffectAction::BuildReplica { .. }
            | RuntimeEffectAction::RetireBuild(_) => {
                return Err(RuntimeError::Application(
                    "the selected replicator does not expose primary configuration/build capabilities"
                        .into(),
                ));
            }
            RuntimeEffectAction::PrepareSecondaryRemoval { .. }
            | RuntimeEffectAction::RegisterPeerSession { .. }
            | RuntimeEffectAction::ObserveSecondaryRemovalWitness(_)
            | RuntimeEffectAction::ObserveSecondaryRemovalProgress { .. }
            | RuntimeEffectAction::ObserveReplicationAck { .. }
            | RuntimeEffectAction::AcceptSecondaryRemovalCommit(_)
            | RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(_)
            | RuntimeEffectAction::RetireReplica(_)
            | RuntimeEffectAction::FenceRetirement(_)
            | RuntimeEffectAction::CompleteRetirement(_) => {
                return Err(RuntimeError::Application(
                    "secondary removal requires a managed replicator".into(),
                ));
            }
            RuntimeEffectAction::Open(_)
            | RuntimeEffectAction::ChangeRole(_)
            | RuntimeEffectAction::ChangeReplicatorRole(_)
            | RuntimeEffectAction::UpdateEpoch
            | RuntimeEffectAction::ChangeApplicationRole(_)
            | RuntimeEffectAction::Close
            | RuntimeEffectAction::Abort => {
                return Err(RuntimeError::Application(
                    "application lifecycle actions belong to the hosting runtime".into(),
                ));
            }
        }
        Ok(())
    }

    async fn snapshot(&self) -> RuntimeSnapshot {
        if let Ok(managed) = self.lifecycle() {
            let mut snapshot = managed.snapshot().await;
            let host = self.state.read().await.fallback_snapshot.clone();
            snapshot.open = host.open;
            snapshot.replication_address = host.replication_address;
            snapshot.role = host.role;
            snapshot.role_transition = host.role_transition;
            snapshot.read_status = host.read_status;
            snapshot.write_status = host.write_status;
            snapshot.authority = host.authority.or(snapshot.authority);
            if self.aborted.load(Ordering::Acquire) {
                snapshot.open = false;
            }
            snapshot
        } else {
            let mut snapshot = self.state.read().await.fallback_snapshot.clone();
            if self.aborted.load(Ordering::Acquire) {
                snapshot.open = false;
            }
            snapshot
        }
    }
}

pub(crate) fn empty_snapshot(identity: ReplicaIdentity) -> RuntimeSnapshot {
    RuntimeSnapshot {
        identity,
        open: false,
        replication_address: None,
        role: ReplicaRole::None,
        role_transition: None,
        read_status: AccessStatus::NotPrimary,
        write_status: AccessStatus::NotPrimary,
        authority: None,
        prepared_secondary_removal: None,
        retired_authority: None,
        accepted_secondary_removal: None,
        current_progress: 0,
        verified_replication_lsn: None,
        live_builds_only: false,
        committed_lsn: 0,
        current_configuration_quorum_progress: 0,
        catch_up_boundary: None,
        catch_up_complete: false,
        builds: Vec::new(),
    }
}

fn snapshot_postcondition(snapshot: RuntimeSnapshot) -> RuntimePostcondition {
    RuntimePostcondition {
        open: snapshot.open,
        role: snapshot.role,
        role_transition: snapshot.role_transition,
        read_status: snapshot.read_status,
        write_status: snapshot.write_status,
        authority: snapshot.authority,
        prepared_secondary_removal: snapshot.prepared_secondary_removal,
        retired_authority: snapshot.retired_authority,
        accepted_secondary_removal: snapshot.accepted_secondary_removal,
        current_progress: snapshot.current_progress,
        verified_replication_lsn: snapshot.verified_replication_lsn,
        committed_lsn: snapshot.committed_lsn,
        current_configuration_quorum_progress: snapshot.current_configuration_quorum_progress,
        catch_up_boundary: snapshot.catch_up_boundary,
        catch_up_complete: snapshot.catch_up_complete,
        builds: snapshot.builds,
    }
}
