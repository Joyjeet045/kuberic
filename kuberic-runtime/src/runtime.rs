use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use bytes::Bytes;
use futures::StreamExt;
use kuberic_protocol::types::{
    AccessStatus, ConfigurationDescriptor, Epoch, OperationId, ReplicaId, ReplicaIdentity,
    ReplicaRole,
};
use kuberic_runtime_internal::transport::{
    CopyAck, CopyItem, OutboundOperation, ReplicaEndpoint, ReplicationAck, ReplicationItem,
};
use tokio::sync::{Mutex, Notify, RwLock, mpsc, oneshot, watch};

use crate::application::{
    ClientWrite, DurableApplicationAck, DurableApplicationProgress, Lsn, Operation,
    OperationDataStream, StateProvider, WriteReceipt,
};
use crate::authority::{
    AdmittedAuthority, BuildAuthority, BuildAuthorityKind, BuildAuthorityStore, BuildProgressStore,
    DurableBuildProgress, DurableLocalWrite, LocalWriteJournal, LocalWritePhase,
    ReplicaAuthorityStore, ReplicationProgress, ReplicationProgressStore,
};
use crate::effects::{BuildPostcondition, RuntimeEffectAction, RuntimeSnapshot};
use crate::engine::DurableState;
use crate::replicator::copy::{
    BuildConfiguration, BuildProgress, CopyItemStream, PrepareCopyRequest, PreparedCopy,
};
use crate::replicator::log::{PreparedWrite, ReplicationLog};
use crate::replicator::stream::{OperationCompletion, OperationMetadata, ServiceStreams};
use crate::replicator::{
    ManagedReplicator, PrimaryReplicator, ReplicaInformation, ReplicaSetQuorumMode, Replicator,
};
use crate::{Result, RuntimeError};

#[doc(hidden)]
pub use crate::replicator::quorum::QuorumTracker;

#[derive(Debug)]
struct RuntimeState {
    open: bool,
    replication_address: Option<String>,
    role: ReplicaRole,
    read_status: AccessStatus,
    write_status: AccessStatus,
    authority: Option<AdmittedAuthority>,
    replication_progress: Option<ReplicationProgress>,
    current_progress: i64,
    committed_lsn: i64,
    builds: BTreeMap<OperationId, BuildProgress>,
    inbound_build_generations: BTreeMap<OperationId, u64>,
    outbound_builds: BTreeMap<OperationId, OutboundBuild>,
    removed_replicas: BTreeSet<ReplicaId>,
    local_writes: BTreeMap<OperationId, DurableLocalWrite>,
    peer_repair_targets: BTreeMap<ReplicaIdentity, i64>,
}

#[derive(Debug, Clone)]
struct OutboundBuild {
    progress: BuildProgress,
    final_sequence: Option<u64>,
    next_sequence: u64,
    emitted: BTreeMap<u64, EmittedBuildItem>,
    pending_operations: BTreeMap<i64, Operation>,
    catching_up: bool,
    stream_tx: mpsc::Sender<Result<CopyItem>>,
    generation: u64,
}

struct BuildPreparationGuard {
    engine: Weak<DefaultReplicatorInner>,
    build_id: OperationId,
    generation: u64,
    armed: bool,
}

impl BuildPreparationGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for BuildPreparationGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Some(engine) = self.engine.upgrade() else {
            return;
        };
        let build_id = self.build_id.clone();
        let generation = self.generation;
        if let Ok(mut state) = engine.state.try_write() {
            if state
                .outbound_builds
                .get(&build_id)
                .is_some_and(|build| build.generation == generation)
            {
                state.outbound_builds.remove(&build_id);
            }
            return;
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let mut state = engine.state.write().await;
                if state
                    .outbound_builds
                    .get(&build_id)
                    .is_some_and(|build| build.generation == generation)
                {
                    state.outbound_builds.remove(&build_id);
                }
            });
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct EmittedBuildItem {
    lsn: i64,
    final_item: bool,
    snapshot_chunk: bool,
}

pub struct PendingWrite {
    pub lsn: i64,
    pub replication_items: Vec<ReplicationItem>,
    pub build_items: Vec<CopyItem>,
    completion: oneshot::Receiver<Result<i64>>,
    aborted: watch::Receiver<bool>,
}

pub struct PendingReplication {
    pub received: ReplicationAck,
    applied: Pin<Box<dyn Future<Output = Result<ReplicationAck>> + Send>>,
}

impl PendingReplication {
    pub async fn applied(self) -> Result<ReplicationAck> {
        self.applied.await
    }
}

impl PendingWrite {
    pub async fn committed(self) -> Result<WriteReceipt> {
        let mut aborted = self.aborted;
        if *aborted.borrow() {
            return Err(RuntimeError::Closed);
        }
        let committed_lsn = tokio::select! {
            biased;
            _ = aborted.changed() => return Err(RuntimeError::Closed),
            result = self.completion => result.map_err(|_| RuntimeError::WriteCompletionClosed)??,
        };
        Ok(WriteReceipt {
            lsn: self.lsn,
            committed_lsn,
        })
    }
}

const MAX_BUILD_PENDING_OPERATIONS: usize = 64;

pub(crate) struct DefaultReplicatorInner {
    pub(crate) identity: ReplicaIdentity,
    storage: RwLock<Option<Arc<dyn DurableState>>>,
    replica_authority_store: Arc<dyn ReplicaAuthorityStore>,
    replication_progress_store: Arc<dyn ReplicationProgressStore>,
    local_write_journal: Arc<dyn LocalWriteJournal>,
    build_authority_store: Arc<dyn BuildAuthorityStore>,
    build_progress_store: Arc<dyn BuildProgressStore>,
    state: RwLock<RuntimeState>,
    effect_lock: Mutex<()>,
    copy_prepare_lock: Mutex<()>,
    delivery_lock: Arc<Mutex<()>>,
    write_lock: Mutex<()>,
    write_generation: AtomicU64,
    fence_generation: AtomicU64,
    replicator: Mutex<ReplicationLog>,
    control: RwLock<Option<Arc<dyn Replicator>>>,
    primary: RwLock<Option<Arc<dyn PrimaryReplicator>>>,
    provider: RwLock<Option<Arc<dyn StateProvider>>>,
    streams: RwLock<Option<Arc<ServiceStreams>>>,
    weak_self: Weak<Self>,
    aborted: AtomicBool,
    closed: AtomicBool,
    abort_signal: watch::Sender<bool>,
    changed: Notify,
    outbound_tx: mpsc::Sender<OutboundOperation>,
    outbound_rx: Mutex<mpsc::Receiver<OutboundOperation>>,
    session_id: String,
}

impl DefaultReplicatorInner {
    pub(crate) fn new(
        identity: ReplicaIdentity,
        replica_authority_store: Arc<dyn ReplicaAuthorityStore>,
        replication_progress_store: Arc<dyn ReplicationProgressStore>,
        local_write_journal: Arc<dyn LocalWriteJournal>,
        build_authority_store: Arc<dyn BuildAuthorityStore>,
        build_progress_store: Arc<dyn BuildProgressStore>,
    ) -> Arc<Self> {
        let (outbound_tx, outbound_rx) = mpsc::channel(64);
        let (abort_signal, _) = watch::channel(false);
        Arc::new_cyclic(|weak_self| Self {
            replicator: Mutex::new(ReplicationLog::new(identity.clone())),
            identity,
            storage: RwLock::new(None),
            replica_authority_store,
            replication_progress_store,
            local_write_journal,
            build_authority_store,
            build_progress_store,
            state: RwLock::new(RuntimeState {
                open: false,
                replication_address: None,
                role: ReplicaRole::None,
                read_status: AccessStatus::NotPrimary,
                write_status: AccessStatus::NotPrimary,
                authority: None,
                replication_progress: None,
                current_progress: 0,
                committed_lsn: 0,
                builds: BTreeMap::new(),
                inbound_build_generations: BTreeMap::new(),
                outbound_builds: BTreeMap::new(),
                removed_replicas: BTreeSet::new(),
                local_writes: BTreeMap::new(),
                peer_repair_targets: BTreeMap::new(),
            }),
            effect_lock: Mutex::new(()),
            copy_prepare_lock: Mutex::new(()),
            delivery_lock: Arc::new(Mutex::new(())),
            write_lock: Mutex::new(()),
            write_generation: AtomicU64::new(0),
            fence_generation: AtomicU64::new(0),
            control: RwLock::new(None),
            primary: RwLock::new(None),
            provider: RwLock::new(None),
            streams: RwLock::new(None),
            weak_self: weak_self.clone(),
            aborted: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            abort_signal,
            changed: Notify::new(),
            outbound_tx,
            outbound_rx: Mutex::new(outbound_rx),
            session_id: uuid::Uuid::new_v4().to_string(),
        })
    }

    fn check_aborted(&self) -> Result<()> {
        if self.aborted.load(Ordering::Acquire) || self.closed.load(Ordering::Acquire) {
            Err(RuntimeError::Closed)
        } else {
            Ok(())
        }
    }

    async fn recover_pending_local_writes(&self) -> Result<()> {
        let writes = self
            .state
            .read()
            .await
            .local_writes
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for durable in writes {
            let write = ClientWrite {
                operation_id: durable.operation_id.clone(),
                data: durable.data.clone(),
            };
            let mut pending = self.begin_write(write.clone()).await?;
            loop {
                self.publish_replication(&pending).await?;
                match pending.committed().await {
                    Ok(_) => break,
                    Err(RuntimeError::WriteCompletionClosed) => {
                        pending = self.begin_write(write.clone()).await?;
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }

    async fn repair_peer_from_history(
        &self,
        identity: ReplicaIdentity,
        peer_progress: i64,
    ) -> Result<()> {
        self.require_primary().await?;
        let (authority, current_progress) = {
            let mut state = self.state.write().await;
            let authority = state
                .authority
                .clone()
                .ok_or(RuntimeError::AuthorityNotAdmitted)?;
            if !authority
                .current_configuration
                .members
                .iter()
                .any(|member| member.identity == identity)
            {
                return Err(RuntimeError::AuthorityMismatch(
                    "peer repair target is outside Current Configuration".into(),
                ));
            }
            if peer_progress >= state.current_progress {
                state.peer_repair_targets.remove(&identity);
                return Ok(());
            }
            if state
                .peer_repair_targets
                .get(&identity)
                .is_some_and(|target| *target >= state.current_progress)
            {
                return Ok(());
            }
            let current_progress = state.current_progress;
            state
                .peer_repair_targets
                .insert(identity.clone(), current_progress);
            (authority, current_progress)
        };

        let result = async {
            let mut operations = self
                .storage()
                .await?
                .get_replication_operations(peer_progress + 1, current_progress)
                .await?;
            let mut expected = peer_progress + 1;
            while let Some(operation) = operations.next().await {
                let operation = operation?;
                if operation.lsn != expected {
                    return Err(RuntimeError::InvalidReplication(
                        "retained history cannot repair the peer contiguously".into(),
                    ));
                }
                expected += 1;
                self.send_outbound(OutboundOperation::Replication(ReplicationItem {
                    sender: self.identity.clone(),
                    receiver: identity.clone(),
                    epoch: authority.current_configuration.epoch,
                    previous_configuration_id: authority
                        .previous_configuration
                        .as_ref()
                        .map(|configuration| configuration.configuration_id.clone()),
                    current_configuration_id: authority
                        .current_configuration
                        .configuration_id
                        .clone(),
                    lsn: operation.lsn,
                    committed_lsn: operation.committed_lsn,
                    data: operation.data,
                }))
                .await?;
            }
            if expected != current_progress + 1 {
                return Err(RuntimeError::InvalidReplication(
                    "retained history is unavailable; full copy is required".into(),
                ));
            }
            Ok(())
        }
        .await;
        if result.is_err() {
            self.state
                .write()
                .await
                .peer_repair_targets
                .remove(&identity);
        }
        result
    }

    fn check_delivery_generation(&self, generation: u64) -> Result<()> {
        if self.fence_generation.load(Ordering::Acquire) != generation {
            Err(RuntimeError::AuthorityMismatch(
                "delivery was fenced before acknowledgement".into(),
            ))
        } else {
            self.check_aborted()
        }
    }

    pub(crate) async fn attach_interfaces(
        &self,
        control_interface: Arc<dyn Replicator>,
        primary_interface: Option<Arc<dyn PrimaryReplicator>>,
    ) -> Result<()> {
        let mut control = self.control.write().await;
        if control.is_some() {
            return Err(RuntimeError::Application(
                "default replicator interfaces are already attached".into(),
            ));
        }
        *control = Some(control_interface);
        *self.primary.write().await = primary_interface;
        Ok(())
    }

    pub(crate) async fn install_provider(
        &self,
        provider: Arc<dyn StateProvider>,
        storage: Arc<dyn DurableState>,
        streams: Arc<ServiceStreams>,
    ) -> Result<()> {
        let mut installed = self.provider.write().await;
        if installed.is_some() {
            return Err(RuntimeError::Application(
                "replication engine is already bound".into(),
            ));
        }
        *installed = Some(provider);
        *self.storage.write().await = Some(storage);
        *self.streams.write().await = Some(streams);
        Ok(())
    }

    async fn control(&self) -> Result<Arc<dyn Replicator>> {
        self.control
            .read()
            .await
            .clone()
            .ok_or(RuntimeError::NotOpen)
    }

    async fn provider(&self) -> Result<Arc<dyn StateProvider>> {
        self.provider
            .read()
            .await
            .clone()
            .ok_or(RuntimeError::NotOpen)
    }

    async fn storage(&self) -> Result<Arc<dyn DurableState>> {
        self.storage.read().await.clone().ok_or_else(|| {
            RuntimeError::Application(
                "the selected factory does not use the default durability engine".into(),
            )
        })
    }

    async fn service_streams(&self) -> Result<Arc<ServiceStreams>> {
        self.streams
            .read()
            .await
            .clone()
            .ok_or(RuntimeError::NotOpen)
    }

    pub(crate) async fn control_open(&self, committed_lsn: i64) -> Result<()> {
        self.check_aborted()?;
        let progress = self.storage().await?.durable_progress().await?;
        if committed_lsn < 0
            || committed_lsn > progress.applied_lsn
            || progress.committed_lsn < 0
            || progress.committed_lsn > progress.applied_lsn
        {
            return Err(RuntimeError::Application(
                "state-provider progress is not durable".into(),
            ));
        }
        self.replicator.lock().await.open()
    }

    pub(crate) async fn control_change_role(&self, epoch: Epoch, role: ReplicaRole) -> Result<()> {
        self.check_aborted()?;
        self.fence_generation.fetch_add(1, Ordering::AcqRel);
        let _delivery = self.delivery_lock.lock().await;
        self.state.write().await.write_status = AccessStatus::ReconfigurationPending;
        self.replicator.lock().await.fence_client_writes();
        self.replicator.lock().await.change_role(epoch, role)?;
        self.state.write().await.role = role;
        self.changed.notify_waiters();
        Ok(())
    }

    pub(crate) async fn control_update_epoch(
        &self,
        epoch: Epoch,
        provider: &dyn StateProvider,
    ) -> Result<()> {
        self.check_aborted()?;
        self.fence_generation.fetch_add(1, Ordering::AcqRel);
        let _delivery = self.delivery_lock.lock().await;
        let state = self.state.read().await;
        if !state.open {
            return Err(RuntimeError::NotOpen);
        }
        let previous_lsn = state.current_progress;
        drop(state);
        self.state.write().await.write_status = AccessStatus::ReconfigurationPending;
        self.replicator.lock().await.update_epoch(epoch)?;
        self.changed.notify_waiters();
        provider.update_epoch(epoch, previous_lsn).await
    }

    pub(crate) async fn control_close(&self) -> Result<()> {
        self.fence_generation.fetch_add(1, Ordering::AcqRel);
        if let Some(streams) = self.streams.read().await.as_ref() {
            streams.shutdown();
        }
        let _delivery = self.delivery_lock.lock().await;
        {
            let mut state = self.state.write().await;
            state.open = false;
            state.role = ReplicaRole::None;
            state.read_status = AccessStatus::NotPrimary;
            state.write_status = AccessStatus::NotPrimary;
            state.outbound_builds.clear();
        }
        self.replicator.lock().await.close()?;
        self.closed.store(true, Ordering::Release);
        self.changed.notify_waiters();
        Ok(())
    }

    pub(crate) fn control_abort(&self) {
        self.fence_generation.fetch_add(1, Ordering::AcqRel);
        self.aborted.store(true, Ordering::Release);
        self.abort_signal.send_replace(true);
        if let Ok(mut log) = self.replicator.try_lock() {
            log.abort();
        }
        if let Ok(streams) = self.streams.try_read()
            && let Some(streams) = streams.as_ref()
        {
            streams.shutdown();
        }
        if let Ok(mut state) = self.state.try_write() {
            state.outbound_builds.clear();
            state.read_status = AccessStatus::NotPrimary;
            state.write_status = AccessStatus::NotPrimary;
        }
        self.changed.notify_waiters();
    }

    pub(crate) async fn control_progress(&self) -> Result<i64> {
        self.check_aborted()?;
        Ok(self
            .state
            .read()
            .await
            .current_progress
            .max(self.replicator.lock().await.current_progress()))
    }

    pub(crate) async fn control_catch_up_capability(&self) -> Result<i64> {
        self.check_aborted()?;
        let current_progress = self.state.read().await.current_progress;
        let log = self.replicator.lock().await;
        Ok(log.catch_up_capability(current_progress))
    }

    async fn reset_after_data_loss_locked(
        &self,
        current_progress: i64,
        committed_lsn: i64,
    ) -> Result<()> {
        self.fence_generation.fetch_add(1, Ordering::AcqRel);
        let _delivery = self.delivery_lock.lock().await;
        self.local_write_journal
            .reset_local_writes_after_data_loss(committed_lsn)
            .await?;
        self.state.write().await.write_status = AccessStatus::ReconfigurationPending;
        self.replicator.lock().await.fence_client_writes();
        self.replicator
            .lock()
            .await
            .reset_progress_after_data_loss(current_progress, committed_lsn);
        let mut state = self.state.write().await;
        state.current_progress = current_progress;
        state.committed_lsn = committed_lsn;
        state.local_writes.clear();
        self.changed.notify_waiters();
        Ok(())
    }

    pub(crate) async fn control_on_data_loss(
        &self,
        provider: &dyn StateProvider,
    ) -> Result<Option<i64>> {
        let _write = self.write_lock.lock().await;
        self.fence_for_data_loss().await?;
        if !provider.on_data_loss().await? {
            return Ok(None);
        }
        let progress = provider.last_committed_lsn().await?;
        self.reset_after_data_loss_locked(progress, progress)
            .await?;
        Ok(Some(progress))
    }

    pub(crate) async fn fence_for_data_loss(&self) -> Result<()> {
        self.require_primary().await?;
        self.write_generation.fetch_add(1, Ordering::AcqRel);
        self.state.write().await.write_status = AccessStatus::ReconfigurationPending;
        self.replicator.lock().await.fence_client_writes();
        self.changed.notify_waiters();
        Ok(())
    }

    pub(crate) async fn require_primary(&self) -> Result<()> {
        self.check_aborted()?;
        let state = self.state.read().await;
        if !state.open {
            return Err(RuntimeError::NotOpen);
        }
        if state.role != ReplicaRole::Primary {
            return Err(RuntimeError::NotPrimary);
        }
        Ok(())
    }

    pub(crate) async fn require_write_access(&self) -> Result<()> {
        self.require_primary().await?;
        let state = self.state.read().await;
        if state.write_status != AccessStatus::Granted {
            return Err(RuntimeError::WriteClosed(state.write_status));
        }
        Ok(())
    }

    pub(crate) async fn configure_replicas(
        &self,
        current: ConfigurationDescriptor,
        previous: Option<ConfigurationDescriptor>,
    ) -> Result<()> {
        self.check_aborted()?;
        let authority = self
            .replica_authority_store
            .load()
            .await?
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        if authority.local_identity != self.identity
            || authority.current_configuration != current
            || authority.previous_configuration != previous
        {
            return Err(RuntimeError::AuthorityMismatch(
                "configuration is not durably admitted".into(),
            ));
        }
        let progress = self.state.read().await.current_progress;
        self.replicator
            .lock()
            .await
            .admit_authority(authority, progress)?;
        self.changed.notify_waiters();
        Ok(())
    }

    pub(crate) async fn wait_for_quorum(&self, mode: ReplicaSetQuorumMode) -> Result<()> {
        self.require_primary().await?;
        let configuration_generation = self.fence_generation.load(Ordering::Acquire);
        let fence = self
            .state
            .read()
            .await
            .authority
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?
            .fence();
        let boundary = self.replicator.lock().await.current_progress();
        loop {
            let changed = self.changed.notified();
            self.require_primary().await?;
            if self.fence_generation.load(Ordering::Acquire) != configuration_generation {
                return Err(RuntimeError::OperationCancelled);
            }
            if self
                .state
                .read()
                .await
                .authority
                .as_ref()
                .map(|a| a.fence())
                != Some(fence.clone())
            {
                return Err(RuntimeError::AuthorityMismatch(
                    "authority changed during catch-up wait".into(),
                ));
            }
            let log = self.replicator.lock().await;
            if log.epoch() != fence.epoch {
                return Err(RuntimeError::AuthorityMismatch(
                    "epoch changed during catch-up wait".into(),
                ));
            }
            let complete = match mode {
                ReplicaSetQuorumMode::WriteQuorum => log.catch_up_complete(),
                ReplicaSetQuorumMode::All => log.all_caught_up(boundary),
            };
            drop(log);
            if complete {
                return Ok(());
            }
            changed.await;
        }
    }

    pub(crate) async fn wait_for_build(&self, replica: ReplicaInformation) -> Result<()> {
        self.require_primary().await?;
        let build_generation = self.fence_generation.load(Ordering::Acquire);
        if replica.identity == self.identity {
            return Err(RuntimeError::InvalidReplication(
                "cannot build the local replica".into(),
            ));
        }
        let fence = self
            .state
            .read()
            .await
            .authority
            .as_ref()
            .map(|a| a.fence());
        self.state
            .write()
            .await
            .removed_replicas
            .remove(&replica.identity.replica_id);
        self.send_outbound(OutboundOperation::Build(ReplicaEndpoint {
            build_id: replica.build_id,
            identity: replica.identity.clone(),
            replication_address: replica.replication_address.clone(),
        }))
        .await?;
        loop {
            let changed = self.changed.notified();
            self.require_primary().await?;
            if self.fence_generation.load(Ordering::Acquire) != build_generation {
                return Err(RuntimeError::AuthorityMismatch(
                    "epoch changed during replica build".into(),
                ));
            }
            let state = self.state.read().await;
            if state
                .removed_replicas
                .contains(&replica.identity.replica_id)
            {
                return Err(RuntimeError::ReplicaRemoved(
                    replica.identity.replica_id.value(),
                ));
            }
            if state.authority.as_ref().map(|a| a.fence()) != fence {
                return Err(RuntimeError::AuthorityMismatch(
                    "authority changed during replica build".into(),
                ));
            }
            if state.outbound_builds.values().any(|build| {
                build.progress.authority.target == replica.identity
                    && build.generation == build_generation
                    && build.progress.completed
                    && build.progress.durable_lsn >= state.current_progress
                    && state.authority.as_ref().is_none_or(|authority| {
                        build.progress.authority.current_configuration
                            == authority.current_configuration
                    })
            }) {
                return Ok(());
            }
            drop(state);
            changed.await;
        }
    }

    pub(crate) async fn remove_replica(&self, replica_id: ReplicaId) -> Result<()> {
        let _guard = self.effect_lock.lock().await;
        self.require_primary().await?;
        let mut state = self.state.write().await;
        if state.authority.as_ref().is_some_and(|a| {
            a.current_configuration
                .members
                .iter()
                .chain(a.previous_configuration.iter().flat_map(|c| &c.members))
                .any(|m| m.identity.replica_id == replica_id)
        }) {
            return Err(RuntimeError::AuthorityMismatch(
                "cannot remove a configured replica".into(),
            ));
        }
        state
            .outbound_builds
            .retain(|_, build| build.progress.authority.target.replica_id != replica_id);
        state.removed_replicas.insert(replica_id);
        drop(state);
        self.send_outbound(OutboundOperation::Remove(replica_id))
            .await?;
        self.changed.notify_waiters();
        Ok(())
    }

    pub(crate) async fn recover_replicate_write(&self, data: Bytes, id: u64) -> ClientWrite {
        let operation_id = self
            .state
            .read()
            .await
            .local_writes
            .values()
            .find(|write| write.operation_id.as_str().starts_with("sf:") && write.data == data)
            .map(|write| write.operation_id.clone())
            .unwrap_or_else(|| OperationId::new(format!("sf:{}:{id}", self.session_id)));
        ClientWrite { operation_id, data }
    }

    pub(crate) async fn publish_replication(&self, pending: &PendingWrite) -> Result<()> {
        for item in &pending.replication_items {
            self.send_outbound(OutboundOperation::Replication(item.clone()))
                .await?;
        }
        for item in &pending.build_items {
            self.send_outbound(OutboundOperation::Copy(item.clone()))
                .await?;
        }
        Ok(())
    }

    async fn send_outbound(&self, outbound: OutboundOperation) -> Result<()> {
        let mut aborted = self.abort_signal.subscribe();
        if *aborted.borrow() {
            return Err(RuntimeError::Closed);
        }
        tokio::select! {
            biased;
            _ = aborted.changed() => Err(RuntimeError::Closed),
            result = self.outbound_tx.send(outbound) => result.map_err(|_| RuntimeError::Closed),
        }
    }

    pub async fn restore_authority(&self) -> Result<()> {
        let _guard = self.effect_lock.lock().await;
        let Some(authority) = self.replica_authority_store.load().await? else {
            return Ok(());
        };
        authority.validate()?;
        if authority.local_identity != self.identity {
            return Err(RuntimeError::AuthorityMismatch(
                "persisted authority belongs to another runtime identity".to_string(),
            ));
        }
        if authority.local_role() != ReplicaRole::Primary {
            {
                let mut state = self.state.write().await;
                state.write_status = AccessStatus::ReconfigurationPending;
            }
            self.replicator.lock().await.fence_client_writes();
        }
        let current_progress = self.state.read().await.current_progress;
        let secondary = matches!(
            authority.local_role(),
            ReplicaRole::ActiveSecondary | ReplicaRole::IdleSecondary
        );
        if secondary {
            self.control()
                .await?
                .update_epoch(authority.current_configuration.epoch)
                .await?;
        }
        self.configure_admitted_authority(&authority, current_progress)
            .await?;
        let replication_progress = self
            .load_replication_progress_with_handoff(&authority)
            .await?;
        let mut state = self.state.write().await;
        state.authority = Some(authority);
        state.replication_progress = Some(replication_progress);
        Ok(())
    }

    pub async fn begin_write(&self, write: ClientWrite) -> Result<PendingWrite> {
        let _guard = self.effect_lock.lock().await;
        let _write = self.write_lock.lock().await;
        let write_generation = self.write_generation.load(Ordering::Acquire);
        self.check_aborted()?;
        let state = self.state.read().await;
        if !state.open {
            return Err(RuntimeError::NotOpen);
        }
        if state.role != ReplicaRole::Primary {
            return Err(RuntimeError::NotPrimary);
        }
        if state.write_status != AccessStatus::Granted {
            return Err(RuntimeError::WriteClosed(state.write_status));
        }
        if state.outbound_builds.values().any(|build| {
            build.catching_up && build.pending_operations.len() >= MAX_BUILD_PENDING_OPERATIONS
        }) {
            return Err(RuntimeError::QueueFull);
        }
        let authority = state
            .authority
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        if authority.primary_identity() != &self.identity {
            return Err(RuntimeError::AuthorityMismatch(
                "local runtime is not the admitted primary".to_string(),
            ));
        }
        if let Some(unresolved) = state
            .local_writes
            .values()
            .find(|pending| pending.operation_id != write.operation_id)
        {
            return Err(RuntimeError::LocalWritePending(
                unresolved.operation_id.to_string(),
            ));
        }
        drop(state);

        let durable_write = if let Some(existing) = self
            .local_write_journal
            .load_local_write(&write.operation_id)
            .await?
        {
            if existing.data != write.data {
                return Err(RuntimeError::Application(
                    "client operation ID was reused with different data".to_string(),
                ));
            }
            existing
        } else {
            let mut replicator = self.replicator.lock().await;
            let committed_lsn = replicator.committed_lsn();
            let lsn = replicator.reserve_write(&write)?;
            drop(replicator);
            let reserved = DurableLocalWrite {
                operation_id: write.operation_id.clone(),
                lsn,
                committed_lsn,
                data: write.data.clone(),
                phase: LocalWritePhase::Reserved,
            };
            self.local_write_journal
                .record_local_write(&reserved)
                .await?;
            self.state
                .write()
                .await
                .local_writes
                .insert(write.operation_id.clone(), reserved.clone());
            reserved
        };
        let lsn = durable_write.lsn;
        let committed_lsn = durable_write.committed_lsn;
        let operation = Operation {
            lsn,
            committed_lsn,
            data: write.data.clone(),
        };
        if durable_write.phase == LocalWritePhase::Committed {
            self.replicator
                .lock()
                .await
                .restore_committed_write(&operation)?;
            self.state
                .write()
                .await
                .local_writes
                .remove(&write.operation_id);
            let (sender, completion) = oneshot::channel();
            let _ = sender.send(Ok(lsn));
            return Ok(PendingWrite {
                lsn,
                replication_items: Vec::new(),
                build_items: Vec::new(),
                completion,
                aborted: self.abort_signal.subscribe(),
            });
        }
        let progress = self.storage().await?.durable_progress().await?;
        let durable_ack = if progress.applied_lsn >= lsn {
            if !self.storage().await?.verify_applied(&operation).await? {
                return Err(RuntimeError::Application(
                    "reserved write conflicts with durable application state".to_string(),
                ));
            }
            progress
        } else {
            if progress.applied_lsn + 1 != lsn {
                return Err(RuntimeError::Application(format!(
                    "reserved LSN {lsn} is not contiguous with durable progress {}",
                    progress.applied_lsn
                )));
            }
            self.storage().await?.apply(operation.clone()).await?
        };
        if self.write_generation.load(Ordering::Acquire) != write_generation {
            return Err(RuntimeError::DataLossFenced);
        }
        validate_durable_ack(lsn, committed_lsn, durable_ack)?;
        let build_operation = operation.clone();
        let prior_phase = durable_write.phase;
        let registered = DurableLocalWrite {
            phase: LocalWritePhase::Registered,
            ..durable_write
        };
        self.local_write_journal
            .record_local_write(&registered)
            .await?;
        self.state
            .write()
            .await
            .local_writes
            .insert(write.operation_id.clone(), registered);
        {
            let mut replicator = self.replicator.lock().await;
            if prior_phase == LocalWritePhase::Reserved {
                replicator.restore_write_reservation(&write, lsn)?;
            }
        }
        let PreparedWrite {
            lsn,
            items,
            completion,
        } = self
            .replicator
            .lock()
            .await
            .ensure_local_write_registered(&operation)?;
        self.finalize_ready_commit_locked().await?;
        if self.write_generation.load(Ordering::Acquire) != write_generation {
            return Err(RuntimeError::DataLossFenced);
        }
        let mut state = self.state.write().await;
        state.current_progress = durable_ack.applied_lsn;
        let mut live_items = Vec::new();
        for build in state
            .outbound_builds
            .values_mut()
            .filter(|build| build_operation.lsn > build.progress.authority.replication_boundary_lsn)
        {
            if build.final_sequence.is_none() || build.catching_up {
                build
                    .pending_operations
                    .insert(build_operation.lsn, build_operation.clone());
                continue;
            }
            let sequence = build.next_sequence;
            build.next_sequence += 1;
            let item = copy_operation_item(&build.progress.authority, sequence, &build_operation);
            build.emitted.insert(
                sequence,
                EmittedBuildItem {
                    lsn: build_operation.lsn,
                    final_item: false,
                    snapshot_chunk: false,
                },
            );
            live_items.push((build.stream_tx.clone(), item));
        }
        drop(state);
        for (sender, item) in live_items {
            sender
                .send(Ok(item))
                .await
                .map_err(|_| RuntimeError::Closed)?;
        }
        Ok(PendingWrite {
            lsn,
            replication_items: items,
            build_items: Vec::new(),
            completion,
            aborted: self.abort_signal.subscribe(),
        })
    }

    pub async fn accept_acknowledgement(&self, acknowledgement: ReplicationAck) -> Result<()> {
        let _guard = self.effect_lock.lock().await;
        self.check_aborted()?;
        let durable_authority = self.replica_authority_store.load().await?;
        if durable_authority != self.state.read().await.authority {
            return Err(RuntimeError::AuthorityMismatch(
                "acknowledgement authority differs from durable admission".into(),
            ));
        }
        self.replicator.lock().await.acknowledge(&acknowledgement)?;
        self.finalize_ready_commit().await?;
        self.changed.notify_waiters();
        Ok(())
    }

    pub async fn prepare_copy(&self, request: PrepareCopyRequest) -> Result<PreparedCopy> {
        let _prepare = self.copy_prepare_lock.lock().await;
        let PrepareCopyRequest {
            build_id,
            target,
            configuration,
            copy_context,
        } = request;
        let guard = self.effect_lock.lock().await;
        let prepare_generation = self.fence_generation.load(Ordering::Acquire);
        let state = self.state.read().await;
        if !state.open {
            return Err(RuntimeError::NotOpen);
        }
        if state.role != ReplicaRole::Primary {
            return Err(RuntimeError::NotPrimary);
        }
        let (kind, configuration) = match configuration {
            BuildConfiguration::Current => {
                let authority = state
                    .authority
                    .clone()
                    .ok_or(RuntimeError::AuthorityNotAdmitted)?;
                if authority.primary_identity() != &self.identity {
                    return Err(RuntimeError::NotPrimary);
                }
                let kind = if authority.transition_kind
                    == Some(kuberic_protocol::types::TransitionKind::Failover)
                {
                    BuildAuthorityKind::Failover
                } else {
                    BuildAuthorityKind::Provisioning
                };
                (kind, authority.current_configuration)
            }
            BuildConfiguration::Bootstrap(configuration) => {
                if state.write_status == AccessStatus::Granted {
                    return Err(RuntimeError::WriteClosed(AccessStatus::Granted));
                }
                (BuildAuthorityKind::Bootstrap, configuration)
            }
        };
        let local_committed_lsn = state.committed_lsn;
        drop(state);

        let replicator = self.replicator.lock().await;
        let replicator_progress = replicator.current_progress();
        let mut committed_lsn = local_committed_lsn.max(replicator.committed_lsn());
        let retained = replicator.retained_operations_from(1);
        drop(replicator);
        let application_progress = self.storage().await?.durable_progress().await?;
        let current_highest = replicator_progress.max(application_progress.applied_lsn);
        let existing = self
            .build_authority_store
            .load_build(&build_id)
            .await?
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        let boundary = existing.replication_boundary_lsn;
        if current_highest < boundary {
            return Err(RuntimeError::Application(
                "application progress regressed below the copy boundary".to_string(),
            ));
        }
        let candidate = BuildAuthority {
            build_id: build_id.clone(),
            kind,
            source: self.identity.clone(),
            target,
            current_configuration: configuration,
            replication_boundary_lsn: boundary,
        };
        candidate.validate()?;
        if existing != candidate {
            return Err(RuntimeError::AuthorityMismatch(
                "build ID is bound to different immutable authority".to_string(),
            ));
        }
        let build_authority = existing;
        let build_progress = self
            .build_progress_store
            .load_build_progress(&build_authority.build_id)
            .await?
            .unwrap_or(DurableBuildProgress {
                authority: build_authority.clone(),
                last_sequence: 0,
                durable_lsn: 0,
                completed: false,
            });
        if build_progress.authority != build_authority {
            return Err(RuntimeError::AuthorityMismatch(
                "build progress belongs to different authority".to_string(),
            ));
        }

        if self
            .state
            .read()
            .await
            .outbound_builds
            .contains_key(&build_authority.build_id)
        {
            return Err(RuntimeError::ReconfigurationPending);
        }
        let (stream_tx, stream_rx) = mpsc::channel(64);
        let (copy_cancel_tx, copy_cancel_rx) = watch::channel(false);
        self.state.write().await.outbound_builds.insert(
            build_authority.build_id.clone(),
            OutboundBuild {
                progress: build_progress,
                final_sequence: None,
                next_sequence: 1,
                emitted: BTreeMap::new(),
                pending_operations: BTreeMap::new(),
                catching_up: true,
                stream_tx: stream_tx.clone(),
                generation: prepare_generation,
            },
        );
        let mut preparation = BuildPreparationGuard {
            engine: self.weak_self.clone(),
            build_id: build_authority.build_id.clone(),
            generation: prepare_generation,
            armed: true,
        };
        drop(guard);
        let operations_result = async {
            let mut operations = BTreeMap::new();
            if current_highest > boundary {
                let mut stream = self
                    .storage()
                    .await?
                    .get_replication_operations(boundary + 1, current_highest)
                    .await?;
                while let Some(operation) = stream.next().await {
                    let operation = operation?;
                    if operation.lsn > boundary && operation.lsn <= current_highest {
                        insert_copy_operation(&mut operations, operation)?;
                    }
                }
            }
            for operation in retained
                .into_iter()
                .filter(|operation| operation.lsn > boundary && operation.lsn <= current_highest)
            {
                insert_copy_operation(&mut operations, operation)?;
            }
            Ok::<_, RuntimeError>(operations)
        }
        .await;
        let operations = match operations_result {
            Ok(operations) => operations,
            Err(error) => {
                self.state
                    .write()
                    .await
                    .outbound_builds
                    .remove(&build_authority.build_id);
                preparation.disarm();
                return Err(error);
            }
        };
        let guard = self.effect_lock.lock().await;
        if let Err(error) = self.check_delivery_generation(prepare_generation) {
            self.state
                .write()
                .await
                .outbound_builds
                .remove(&build_authority.build_id);
            preparation.disarm();
            return Err(error);
        }
        let replicator = self.replicator.lock().await;
        let replicator_epoch = replicator.epoch();
        committed_lsn = committed_lsn.max(replicator.committed_lsn());
        drop(replicator);
        if replicator_epoch != Epoch::default()
            && replicator_epoch != build_authority.current_configuration.epoch
        {
            self.state
                .write()
                .await
                .outbound_builds
                .remove(&build_authority.build_id);
            preparation.disarm();
            return Err(RuntimeError::AuthorityMismatch(
                "build authority was fenced during preparation".into(),
            ));
        }
        if current_highest > boundary
            && ((boundary + 1)..=current_highest).any(|lsn| !operations.contains_key(&lsn))
        {
            self.state
                .write()
                .await
                .outbound_builds
                .remove(&build_authority.build_id);
            preparation.disarm();
            return Err(RuntimeError::InvalidReplication(
                "retained operations do not close the post-snapshot gap".to_string(),
            ));
        }
        drop(guard);
        let copy_stream = match self
            .provider()
            .await?
            .get_copy_state(boundary, copy_context)
            .await
        {
            Ok(stream) => stream,
            Err(error) => {
                self.state
                    .write()
                    .await
                    .outbound_builds
                    .remove(&build_authority.build_id);
                preparation.disarm();
                return Err(error);
            }
        };
        preparation.disarm();
        let engine = self.weak_self.upgrade().ok_or(RuntimeError::Closed)?;
        let producer_authority = build_authority.clone();
        tokio::spawn(async move {
            engine
                .produce_copy_stream(
                    producer_authority,
                    committed_lsn,
                    copy_stream,
                    operations,
                    stream_tx,
                    prepare_generation,
                    copy_cancel_rx,
                )
                .await;
        });
        let items = Box::pin(CopyItemStream::new(stream_rx, copy_cancel_tx));
        Ok(PreparedCopy {
            authority: build_authority,
            items,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn produce_copy_stream(
        &self,
        authority: BuildAuthority,
        committed_lsn: i64,
        mut copy_stream: OperationDataStream,
        initial_operations: BTreeMap<i64, Operation>,
        sender: mpsc::Sender<Result<CopyItem>>,
        generation: u64,
        cancellation: watch::Receiver<bool>,
    ) {
        let result = self
            .produce_copy_stream_inner(
                &authority,
                committed_lsn,
                &mut copy_stream,
                initial_operations,
                &sender,
                generation,
                cancellation,
            )
            .await;
        if let Err(error) = result {
            let mut state = self.state.write().await;
            if state
                .outbound_builds
                .get(&authority.build_id)
                .is_some_and(|build| build.generation == generation)
            {
                state.outbound_builds.remove(&authority.build_id);
            }
            drop(state);
            self.changed.notify_waiters();
            let _ = sender.send(Err(error)).await;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn produce_copy_stream_inner(
        &self,
        authority: &BuildAuthority,
        committed_lsn: i64,
        copy_stream: &mut OperationDataStream,
        initial_operations: BTreeMap<i64, Operation>,
        sender: &mpsc::Sender<Result<CopyItem>>,
        generation: u64,
        mut cancellation: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut sequence = 1;
        loop {
            let chunk = tokio::select! {
                biased;
                _ = cancellation.changed() => return Err(RuntimeError::OperationCancelled),
                chunk = copy_stream.next() => chunk,
            };
            let Some(chunk) = chunk else {
                break;
            };
            self.check_delivery_generation(generation)?;
            let item = copy_snapshot_item(authority, sequence, chunk?);
            self.record_emitted_copy_item(authority, &item, generation)
                .await?;
            send_copy_item(sender, item, &mut cancellation).await?;
            sequence += 1;
        }

        self.check_delivery_generation(generation)?;
        let final_item = copy_final_item(authority, sequence, committed_lsn);
        {
            let mut state = self.state.write().await;
            let build = state
                .outbound_builds
                .get_mut(&authority.build_id)
                .ok_or(RuntimeError::OperationCancelled)?;
            if build.generation != generation {
                return Err(RuntimeError::AuthorityMismatch(
                    "copy stream belongs to a fenced generation".into(),
                ));
            }
            build.final_sequence = Some(sequence);
            build.next_sequence = sequence + 1;
            build.emitted.insert(
                sequence,
                EmittedBuildItem {
                    lsn: final_item.lsn,
                    final_item: true,
                    snapshot_chunk: false,
                },
            );
        }
        send_copy_item(sender, final_item, &mut cancellation).await?;

        for operation in initial_operations.into_values() {
            self.emit_copy_operation(authority, operation, sender, generation, &mut cancellation)
                .await?;
        }
        loop {
            let pending = {
                let mut state = self.state.write().await;
                let build = state
                    .outbound_builds
                    .get_mut(&authority.build_id)
                    .ok_or(RuntimeError::OperationCancelled)?;
                if build.generation != generation {
                    return Err(RuntimeError::AuthorityMismatch(
                        "copy stream belongs to a fenced generation".into(),
                    ));
                }
                if build.pending_operations.is_empty() {
                    build.catching_up = false;
                    BTreeMap::new()
                } else {
                    std::mem::take(&mut build.pending_operations)
                }
            };
            if pending.is_empty() {
                break;
            }
            for operation in pending.into_values() {
                self.emit_copy_operation(
                    authority,
                    operation,
                    sender,
                    generation,
                    &mut cancellation,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn record_emitted_copy_item(
        &self,
        authority: &BuildAuthority,
        item: &CopyItem,
        generation: u64,
    ) -> Result<()> {
        let mut state = self.state.write().await;
        let build = state
            .outbound_builds
            .get_mut(&authority.build_id)
            .ok_or(RuntimeError::OperationCancelled)?;
        if build.generation != generation {
            return Err(RuntimeError::AuthorityMismatch(
                "copy stream belongs to a fenced generation".into(),
            ));
        }
        build.next_sequence = build.next_sequence.max(item.sequence + 1);
        build.emitted.insert(
            item.sequence,
            EmittedBuildItem {
                lsn: item.lsn,
                final_item: item.final_item,
                snapshot_chunk: item.snapshot_chunk,
            },
        );
        Ok(())
    }

    async fn emit_copy_operation(
        &self,
        authority: &BuildAuthority,
        operation: Operation,
        sender: &mpsc::Sender<Result<CopyItem>>,
        generation: u64,
        cancellation: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        self.check_delivery_generation(generation)?;
        let item = {
            let mut state = self.state.write().await;
            let build = state
                .outbound_builds
                .get_mut(&authority.build_id)
                .ok_or(RuntimeError::OperationCancelled)?;
            if build.generation != generation {
                return Err(RuntimeError::AuthorityMismatch(
                    "copy stream belongs to a fenced generation".into(),
                ));
            }
            let sequence = build.next_sequence;
            build.next_sequence += 1;
            let item = copy_operation_item(authority, sequence, &operation);
            build.emitted.insert(
                sequence,
                EmittedBuildItem {
                    lsn: operation.lsn,
                    final_item: false,
                    snapshot_chunk: false,
                },
            );
            item
        };
        send_copy_item(sender, item, cancellation).await
    }

    pub async fn accept_copy_acknowledgement(&self, acknowledgement: CopyAck) -> Result<()> {
        let _delivery = self.delivery_lock.lock().await;
        let delivery_generation = self.fence_generation.load(Ordering::Acquire);
        let state = self.state.read().await;
        let build = state
            .outbound_builds
            .get(&acknowledgement.build_id)
            .cloned()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        drop(state);
        let authority = &build.progress.authority;
        if build.generation != delivery_generation {
            return Err(RuntimeError::AuthorityMismatch(
                "build acknowledgement belongs to a fenced generation".into(),
            ));
        }
        let replicator_epoch = self.replicator.lock().await.epoch();
        if replicator_epoch != Epoch::default()
            && replicator_epoch != authority.current_configuration.epoch
        {
            return Err(RuntimeError::AuthorityMismatch(
                "build acknowledgement belongs to a fenced epoch".into(),
            ));
        }
        if acknowledgement.sender != authority.source
            || acknowledgement.receiver != authority.target
            || acknowledgement.epoch != authority.current_configuration.epoch
            || acknowledgement.current_configuration_id
                != authority.current_configuration.configuration_id
            || acknowledgement.replication_boundary_lsn != authority.replication_boundary_lsn
        {
            return Err(RuntimeError::AuthorityMismatch(
                "copy acknowledgement does not match the active build".to_string(),
            ));
        }
        let emitted = build
            .emitted
            .get(&acknowledgement.sequence)
            .ok_or_else(|| {
                RuntimeError::InvalidReplication(
                    "copy acknowledgement does not match an emitted item".to_string(),
                )
            })?;
        if acknowledgement.final_item != emitted.final_item
            || acknowledgement.snapshot_chunk != emitted.snapshot_chunk
            || acknowledgement.final_item
                != (build.final_sequence == Some(acknowledgement.sequence))
        {
            return Err(RuntimeError::InvalidReplication(
                "copy acknowledgement does not match an emitted item".to_string(),
            ));
        }
        let max_emitted_lsn = build
            .emitted
            .values()
            .map(|item| item.lsn)
            .max()
            .unwrap_or(0);
        if (!acknowledgement.final_item && acknowledgement.durable_lsn < emitted.lsn)
            || acknowledgement.durable_lsn > max_emitted_lsn
        {
            return Err(RuntimeError::InvalidReplication(
                "copy acknowledgement exceeds emitted durable progress".to_string(),
            ));
        }
        let mut progress = build.progress.clone();
        progress.last_sequence = progress.last_sequence.max(acknowledgement.sequence);
        progress.durable_lsn = progress.durable_lsn.max(acknowledgement.durable_lsn);
        if acknowledgement.final_item {
            progress.completed = true;
        }
        self.build_progress_store
            .record_build_progress(&progress)
            .await?;
        self.check_delivery_generation(delivery_generation)?;
        let mut state = self.state.write().await;
        let current = state
            .outbound_builds
            .get_mut(&acknowledgement.build_id)
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        if current.progress.authority != progress.authority {
            return Err(RuntimeError::AuthorityMismatch(
                "build authority changed before progress persistence".into(),
            ));
        }
        current.progress = progress;
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    pub async fn receive_copy_item(&self, envelope: CopyItem) -> Result<CopyAck> {
        let _delivery = self.delivery_lock.lock().await;
        let delivery_generation = self.fence_generation.load(Ordering::Acquire);
        self.check_aborted()?;
        let state = self.state.read().await;
        if !state.open {
            return Err(RuntimeError::NotOpen);
        }
        if state.role != ReplicaRole::IdleSecondary {
            return Err(RuntimeError::AuthorityMismatch(
                "copy target must be an Idle Secondary".to_string(),
            ));
        }
        let progress = state
            .builds
            .get(&envelope.build_id)
            .cloned()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        let admitted_generation = state
            .inbound_build_generations
            .get(&envelope.build_id)
            .copied()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        drop(state);
        if admitted_generation != delivery_generation {
            return Err(RuntimeError::AuthorityMismatch(
                "copy item belongs to a fenced build generation".into(),
            ));
        }
        let authority = self
            .build_authority_store
            .load_build(&envelope.build_id)
            .await?
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        if authority != progress.authority {
            return Err(RuntimeError::AuthorityMismatch(
                "durable build authority differs from runtime authority".to_string(),
            ));
        }
        authority.validate()?;
        kuberic_runtime_internal::authority::validate_build_envelope(&authority, &envelope)?;
        let replicator_epoch = self.replicator.lock().await.epoch();
        if replicator_epoch != Epoch::default() && replicator_epoch != envelope.epoch {
            return Err(RuntimeError::AuthorityMismatch(
                "copy item belongs to a fenced epoch".into(),
            ));
        }
        if envelope.sequence > progress.last_sequence
            && ((!progress.completed && !envelope.snapshot_chunk && !envelope.final_item)
                || (progress.completed && (envelope.snapshot_chunk || envelope.final_item)))
        {
            return Err(RuntimeError::InvalidReplication(
                "copy item kind does not match the current build phase".to_string(),
            ));
        }

        let durable_lsn = if envelope.sequence <= progress.last_sequence {
            if envelope.snapshot_chunk {
                if !self
                    .storage()
                    .await?
                    .verify_copy_chunk(
                        &envelope.build_id,
                        envelope.sequence,
                        &crate::application::CopyChunk {
                            data: envelope.data.clone(),
                        },
                    )
                    .await?
                {
                    return Err(RuntimeError::InvalidReplication(
                        "duplicate snapshot chunk has conflicting durable contents".to_string(),
                    ));
                }
                0
            } else if envelope.final_item {
                if !progress.completed {
                    return Err(RuntimeError::InvalidReplication(
                        "duplicate final copy marker preceded completion".to_string(),
                    ));
                }
                envelope.replication_boundary_lsn
            } else {
                let operation = Operation {
                    lsn: envelope.lsn,
                    committed_lsn: envelope.committed_lsn,
                    data: envelope.data.clone(),
                };
                if !self.storage().await?.verify_applied(&operation).await? {
                    return Err(RuntimeError::InvalidReplication(
                        "duplicate copy item has conflicting durable contents".to_string(),
                    ));
                }
                progress.durable_lsn
            }
        } else {
            if envelope.sequence != progress.last_sequence + 1 {
                return Err(RuntimeError::InvalidReplication(format!(
                    "copy sequence gap: expected {}, observed {}",
                    progress.last_sequence + 1,
                    envelope.sequence
                )));
            }
            if envelope.snapshot_chunk {
                self.service_streams()
                    .await?
                    .copy(
                        OperationMetadata::Copy {
                            build_id: envelope.build_id.clone(),
                            sequence: envelope.sequence,
                        },
                        envelope.data.clone(),
                    )
                    .await?;
                let updated = DurableBuildProgress {
                    authority: progress.authority.clone(),
                    last_sequence: envelope.sequence,
                    durable_lsn: progress.durable_lsn,
                    completed: false,
                };
                self.check_delivery_generation(delivery_generation)?;
                self.build_progress_store
                    .record_build_progress(&updated)
                    .await?;
                self.check_delivery_generation(delivery_generation)?;
                self.state
                    .write()
                    .await
                    .builds
                    .insert(envelope.build_id.clone(), updated);
                progress.durable_lsn
            } else if envelope.final_item {
                let durable = self
                    .service_streams()
                    .await?
                    .copy(
                        OperationMetadata::CopyComplete {
                            build_id: envelope.build_id.clone(),
                            up_to_lsn: envelope.replication_boundary_lsn,
                            committed_lsn: envelope.committed_lsn,
                        },
                        Bytes::new(),
                    )
                    .await?;
                if durable.applied_lsn < envelope.replication_boundary_lsn
                    || durable.committed_lsn < envelope.committed_lsn
                    || durable.committed_lsn > durable.applied_lsn
                {
                    return Err(RuntimeError::Application(
                        "application lost the durable copy boundary".to_string(),
                    ));
                }
                let updated = DurableBuildProgress {
                    authority: progress.authority.clone(),
                    last_sequence: envelope.sequence,
                    durable_lsn: envelope.replication_boundary_lsn,
                    completed: true,
                };
                self.check_delivery_generation(delivery_generation)?;
                self.build_progress_store
                    .record_build_progress(&updated)
                    .await?;
                self.check_delivery_generation(delivery_generation)?;
                let mut state = self.state.write().await;
                state.builds.insert(envelope.build_id.clone(), updated);
                state.current_progress = state.current_progress.max(durable.applied_lsn);
                state.committed_lsn = state.committed_lsn.max(durable.committed_lsn);
                envelope.replication_boundary_lsn
            } else {
                if !progress.completed {
                    return Err(RuntimeError::InvalidReplication(
                        "live build replication arrived before snapshot completion".to_string(),
                    ));
                }
                if envelope.lsn != progress.durable_lsn + 1 {
                    return Err(RuntimeError::InvalidReplication(format!(
                        "copy LSN gap: expected {}, observed {}",
                        progress.durable_lsn + 1,
                        envelope.lsn
                    )));
                }
                let operation = Operation {
                    lsn: envelope.lsn,
                    committed_lsn: envelope.committed_lsn,
                    data: envelope.data.clone(),
                };
                let application_progress = self.storage().await?.durable_progress().await?;
                let durable = if application_progress.applied_lsn >= envelope.lsn {
                    if !self.storage().await?.verify_applied(&operation).await? {
                        return Err(RuntimeError::InvalidReplication(
                            "copy item conflicts with durable application state".to_string(),
                        ));
                    }
                    application_progress
                } else {
                    if application_progress.applied_lsn + 1 != envelope.lsn {
                        return Err(RuntimeError::InvalidReplication(format!(
                            "copy application gap: expected LSN {}, observed {}",
                            application_progress.applied_lsn + 1,
                            envelope.lsn
                        )));
                    }
                    let durable = self.service_streams().await?.replication(operation).await?;
                    validate_durable_ack(envelope.lsn, envelope.committed_lsn, durable)?;
                    durable
                };
                let updated = DurableBuildProgress {
                    authority: progress.authority.clone(),
                    last_sequence: envelope.sequence,
                    durable_lsn: envelope.lsn,
                    completed: progress.completed,
                };
                self.check_delivery_generation(delivery_generation)?;
                self.build_progress_store
                    .record_build_progress(&updated)
                    .await?;
                self.check_delivery_generation(delivery_generation)?;
                let mut state = self.state.write().await;
                state.builds.insert(envelope.build_id.clone(), updated);
                state.current_progress = state.current_progress.max(durable.applied_lsn);
                state.committed_lsn = state.committed_lsn.max(durable.committed_lsn);
                envelope.lsn
            }
        };
        self.check_delivery_generation(delivery_generation)?;
        Ok(CopyAck {
            build_id: envelope.build_id,
            sender: envelope.sender,
            receiver: envelope.receiver,
            epoch: envelope.epoch,
            current_configuration_id: envelope.current_configuration_id,
            sequence: envelope.sequence,
            durable_lsn,
            replication_boundary_lsn: envelope.replication_boundary_lsn,
            final_item: envelope.final_item,
            snapshot_chunk: envelope.snapshot_chunk,
        })
    }

    pub async fn receive_replication(
        self: Arc<Self>,
        envelope: ReplicationItem,
    ) -> Result<PendingReplication> {
        let delivery = self.delivery_lock.clone().lock_owned().await;
        let delivery_generation = self.fence_generation.load(Ordering::Acquire);
        self.check_aborted()?;
        let state = self.state.read().await;
        if !state.open {
            return Err(RuntimeError::NotOpen);
        }
        let in_memory_authority = state
            .authority
            .clone()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        let mut replication_progress = state
            .replication_progress
            .clone()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        let role = state.role;
        drop(state);
        let authority = self
            .replica_authority_store
            .load()
            .await?
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        authority.validate()?;
        if authority != in_memory_authority {
            return Err(RuntimeError::AuthorityMismatch(
                "durable authority differs from runtime authority".to_string(),
            ));
        }
        if authority.local_identity != self.identity || envelope.receiver != self.identity {
            return Err(RuntimeError::AuthorityMismatch(
                "replication target differs from the local durable identity".to_string(),
            ));
        }
        if role != authority.local_role()
            || !matches!(
                role,
                ReplicaRole::ActiveSecondary | ReplicaRole::IdleSecondary
            )
        {
            return Err(RuntimeError::AuthorityMismatch(
                "runtime role is not admitted to receive replication".to_string(),
            ));
        }
        authority.validate_envelope(&envelope)?;
        if self.replicator.lock().await.epoch() != envelope.epoch {
            return Err(RuntimeError::AuthorityMismatch(
                "replication epoch is fenced by the control interface".into(),
            ));
        }
        if replication_progress.fence != authority.fence() {
            return Err(RuntimeError::AuthorityMismatch(
                "replication progress belongs to another authority".to_string(),
            ));
        }
        if envelope.lsn > replication_progress.verified_lsn + 1 {
            return Err(RuntimeError::InvalidReplication(format!(
                "authority verification gap: expected at most LSN {}, observed {}",
                replication_progress.verified_lsn + 1,
                envelope.lsn
            )));
        }
        let operation = Operation {
            lsn: envelope.lsn,
            committed_lsn: envelope.committed_lsn,
            data: envelope.data.clone(),
        };
        let application_progress = self.storage().await?.durable_progress().await?;
        let completion = if envelope.lsn <= application_progress.applied_lsn {
            None
        } else {
            if envelope.lsn != application_progress.applied_lsn + 1 {
                return Err(RuntimeError::InvalidReplication(format!(
                    "application gap: expected LSN {}, observed {}",
                    application_progress.applied_lsn + 1,
                    envelope.lsn
                )));
            }
            Some(
                self.service_streams()
                    .await?
                    .enqueue_replication(operation.clone())
                    .await?,
            )
        };
        let received_applied_lsn = replication_progress
            .verified_lsn
            .min(application_progress.applied_lsn);
        let received = ReplicationAck {
            sender: envelope.sender.clone(),
            receiver: self.identity.clone(),
            epoch: envelope.epoch,
            previous_configuration_id: envelope.previous_configuration_id.clone(),
            current_configuration_id: envelope.current_configuration_id.clone(),
            received_lsn: envelope.lsn.max(received_applied_lsn),
            applied_lsn: received_applied_lsn,
            committed_lsn: application_progress.committed_lsn.min(received_applied_lsn),
        };
        let engine = self.clone();
        let applied = Box::pin(async move {
            let _delivery = delivery;
            engine
                .complete_replication_delivery(
                    delivery_generation,
                    authority,
                    envelope,
                    operation,
                    application_progress,
                    completion,
                    &mut replication_progress,
                )
                .await
        });
        Ok(PendingReplication { received, applied })
    }

    #[allow(clippy::too_many_arguments)]
    async fn complete_replication_delivery(
        &self,
        delivery_generation: u64,
        authority: AdmittedAuthority,
        envelope: ReplicationItem,
        operation: Operation,
        application_progress: DurableApplicationProgress,
        completion: Option<OperationCompletion>,
        replication_progress: &mut ReplicationProgress,
    ) -> Result<ReplicationAck> {
        let durable_ack = if let Some(completion) = completion {
            let durable_ack = completion.completed().await?;
            validate_durable_ack(envelope.lsn, envelope.committed_lsn, durable_ack)?;
            durable_ack
        } else {
            if !self.storage().await?.verify_applied(&operation).await? {
                return Err(RuntimeError::InvalidReplication(
                    "authority operation conflicts with durable application state".to_string(),
                ));
            }
            if envelope.committed_lsn > application_progress.committed_lsn {
                self.storage().await?.commit(envelope.committed_lsn).await?
            } else {
                application_progress
            }
        };
        self.check_delivery_generation(delivery_generation)?;
        let current_state = self.state.read().await;
        if !current_state.open || current_state.authority.as_ref() != Some(&authority) {
            return Err(RuntimeError::AuthorityMismatch(
                "replication authority changed before service acknowledgement".into(),
            ));
        }
        drop(current_state);
        if self.replicator.lock().await.epoch() != envelope.epoch {
            return Err(RuntimeError::AuthorityMismatch(
                "replication epoch changed before service acknowledgement".into(),
            ));
        }
        if self.replica_authority_store.load().await?.as_ref() != Some(&authority) {
            return Err(RuntimeError::AuthorityMismatch(
                "durable authority changed before service acknowledgement".into(),
            ));
        }
        self.check_delivery_generation(delivery_generation)?;
        if envelope.lsn == replication_progress.verified_lsn + 1 {
            replication_progress.verified_lsn = envelope.lsn;
            self.replication_progress_store
                .record_replication_progress(replication_progress)
                .await?;
            self.check_delivery_generation(delivery_generation)?;
        }
        let mut state = self.state.write().await;
        self.check_delivery_generation(delivery_generation)?;
        state.replication_progress = Some(replication_progress.clone());
        state.current_progress = durable_ack.applied_lsn;
        state.committed_lsn = state.committed_lsn.max(durable_ack.committed_lsn);
        drop(state);
        let acknowledged_committed_lsn = durable_ack
            .committed_lsn
            .min(replication_progress.verified_lsn);
        self.check_delivery_generation(delivery_generation)?;
        Ok(ReplicationAck {
            sender: envelope.sender,
            receiver: self.identity.clone(),
            epoch: envelope.epoch,
            previous_configuration_id: envelope.previous_configuration_id,
            current_configuration_id: envelope.current_configuration_id,
            received_lsn: envelope.lsn.max(replication_progress.verified_lsn),
            applied_lsn: replication_progress.verified_lsn,
            committed_lsn: acknowledged_committed_lsn,
        })
    }

    pub async fn snapshot(&self) -> RuntimeSnapshot {
        let _guard = self.effect_lock.lock().await;
        let state = self.state.read().await;
        let snapshot = (
            state.open,
            state.role,
            state.read_status,
            state.write_status,
            state.authority.clone(),
            state.current_progress,
            state
                .replication_progress
                .as_ref()
                .map(|progress| progress.verified_lsn),
            state.committed_lsn,
            build_postconditions(&state),
        );
        drop(state);
        let replicator = self.replicator.lock().await;
        RuntimeSnapshot {
            identity: self.identity.clone(),
            open: snapshot.0 && !self.aborted.load(Ordering::Acquire),
            replication_address: self.state.read().await.replication_address.clone(),
            role: snapshot.1,
            role_transition: None,
            read_status: snapshot.2,
            write_status: snapshot.3,
            authority: snapshot.4,
            current_progress: snapshot.5,
            verified_replication_lsn: snapshot.6,
            committed_lsn: snapshot.7.max(replicator.committed_lsn()),
            current_configuration_quorum_progress: replicator
                .current_configuration_quorum_progress(),
            catch_up_boundary: replicator.catch_up_boundary(),
            catch_up_complete: replicator.catch_up_complete(),
            builds: snapshot.8,
        }
    }

    async fn complete_open(&self, replication_address: String) -> Result<()> {
        let progress = if let Some(storage) = self.storage.read().await.clone() {
            storage.durable_progress().await?
        } else {
            DurableApplicationProgress {
                applied_lsn: self.control().await?.current_progress().await?,
                committed_lsn: 0,
            }
        };
        let local_writes = self.local_write_journal.load_local_writes().await?;
        let mut state = self.state.write().await;
        state.open = true;
        state.replication_address = Some(replication_address);
        state.current_progress = progress.applied_lsn;
        state.committed_lsn = progress.committed_lsn;
        state.local_writes = local_writes
            .into_iter()
            .map(|write| (write.operation_id.clone(), write))
            .collect();
        Ok(())
    }

    async fn execute_action(&self, action: RuntimeEffectAction) -> Result<()> {
        match action {
            RuntimeEffectAction::Open(_)
            | RuntimeEffectAction::ChangeRole(_)
            | RuntimeEffectAction::ChangeReplicatorRole(_)
            | RuntimeEffectAction::UpdateEpoch
            | RuntimeEffectAction::ChangeApplicationRole(_)
            | RuntimeEffectAction::BuildReplica { .. }
            | RuntimeEffectAction::Close
            | RuntimeEffectAction::Abort => {
                return Err(RuntimeError::Application(
                    "application lifecycle actions belong to the hosting runtime".into(),
                ));
            }
            RuntimeEffectAction::AdmitAuthority(authority) => {
                let authority = *authority;
                authority.validate()?;
                if authority.local_identity != self.identity {
                    return Err(RuntimeError::AuthorityMismatch(
                        "authority target differs from runtime identity".to_string(),
                    ));
                }
                if self
                    .state
                    .read()
                    .await
                    .authority
                    .as_ref()
                    .is_some_and(|existing| {
                        authority.current_configuration.epoch < existing.current_configuration.epoch
                    })
                {
                    return Err(RuntimeError::AuthorityMismatch(
                        "authority epoch cannot regress".into(),
                    ));
                }
                let prior_access = {
                    let state = self.state.read().await;
                    (state.read_status, state.write_status)
                };
                let authority_changed =
                    self.state.read().await.authority.as_ref() != Some(&authority);
                if authority_changed {
                    if self
                        .state
                        .read()
                        .await
                        .authority
                        .as_ref()
                        .is_some_and(|existing| {
                            existing.current_configuration.epoch
                                == authority.current_configuration.epoch
                                && existing != &authority
                                && !authority.is_current_only_completion_of(existing)
                        })
                    {
                        return Err(RuntimeError::AuthorityMismatch(
                            "authority changed without a newer epoch".into(),
                        ));
                    }
                    {
                        let mut state = self.state.write().await;
                        state.read_status = AccessStatus::ReconfigurationPending;
                        state.write_status = AccessStatus::ReconfigurationPending;
                    }
                    self.fence_generation.fetch_add(1, Ordering::AcqRel);
                    self.replicator.lock().await.fence_client_writes();
                    self.changed.notify_waiters();
                }
                if authority.local_role() != ReplicaRole::Primary {
                    {
                        let mut state = self.state.write().await;
                        state.write_status = AccessStatus::ReconfigurationPending;
                    }
                    self.replicator.lock().await.fence_client_writes();
                }
                self.replica_authority_store.admit(&authority).await?;
                let state = self.state.read().await;
                let current_progress = state.current_progress;
                let previous_epoch = state
                    .authority
                    .as_ref()
                    .map(|accepted| accepted.current_configuration.epoch);
                drop(state);
                let secondary = matches!(
                    authority.local_role(),
                    ReplicaRole::ActiveSecondary | ReplicaRole::IdleSecondary
                );
                if previous_epoch != Some(authority.current_configuration.epoch) && secondary {
                    self.control()
                        .await?
                        .update_epoch(authority.current_configuration.epoch)
                        .await?;
                }
                self.configure_admitted_authority(&authority, current_progress)
                    .await?;
                let replication_progress = self
                    .load_replication_progress_with_handoff(&authority)
                    .await?;
                let mut state = self.state.write().await;
                if !authority_changed {
                    state.read_status = prior_access.0;
                    state.write_status = prior_access.1;
                }
                state.authority = Some(authority);
                state.replication_progress = Some(replication_progress);
            }
            RuntimeEffectAction::AuthorizeFailoverPrefix(safe_lsn) => {
                if safe_lsn < 0 {
                    return Err(RuntimeError::AuthorityMismatch(
                        "failover-safe LSN must not be negative".into(),
                    ));
                }
                let (authority, mut progress) = {
                    let state = self.state.read().await;
                    (
                        state
                            .authority
                            .clone()
                            .ok_or(RuntimeError::AuthorityNotAdmitted)?,
                        state
                            .replication_progress
                            .clone()
                            .ok_or(RuntimeError::AuthorityNotAdmitted)?,
                    )
                };
                let durable = self.storage().await?.durable_progress().await?;
                progress.verified_lsn =
                    progress.verified_lsn.max(durable.applied_lsn.min(safe_lsn));
                if progress.fence != authority.fence() {
                    return Err(RuntimeError::AuthorityMismatch(
                        "failover-safe prefix belongs to another authority fence".into(),
                    ));
                }
                self.replication_progress_store
                    .record_replication_progress(&progress)
                    .await?;
                self.state.write().await.replication_progress = Some(progress);
            }
            RuntimeEffectAction::AdmitBuildAuthority(authority) => {
                let authority = *authority;
                authority.validate()?;
                if authority.target != self.identity && authority.source != self.identity {
                    return Err(RuntimeError::AuthorityMismatch(
                        "build authority does not address this runtime".to_string(),
                    ));
                }
                if let Some(existing) = self
                    .build_authority_store
                    .load_build(&authority.build_id)
                    .await?
                {
                    if existing != authority {
                        return Err(RuntimeError::AuthorityMismatch(
                            "build ID is already bound to different authority".to_string(),
                        ));
                    }
                } else {
                    self.build_authority_store.admit_build(&authority).await?;
                }
                let progress = self
                    .build_progress_store
                    .load_build_progress(&authority.build_id)
                    .await?
                    .unwrap_or(DurableBuildProgress {
                        authority: authority.clone(),
                        last_sequence: 0,
                        durable_lsn: 0,
                        completed: false,
                    });
                if progress.authority != authority {
                    return Err(RuntimeError::AuthorityMismatch(
                        "build progress belongs to different authority".to_string(),
                    ));
                }
                if authority.target == self.identity {
                    if authority.kind == BuildAuthorityKind::Provisioning {
                        self.control()
                            .await?
                            .update_epoch(authority.current_configuration.epoch)
                            .await?;
                    }
                    let mut state = self.state.write().await;
                    state.builds.insert(authority.build_id.clone(), progress);
                    state.inbound_build_generations.insert(
                        authority.build_id.clone(),
                        self.fence_generation.load(Ordering::Acquire),
                    );
                }
            }
            RuntimeEffectAction::SetReadStatus(read_status) => {
                let state = self.state.read().await;
                if read_status == AccessStatus::Granted
                    && (!state.open
                        || !matches!(
                            state.role,
                            ReplicaRole::Primary | ReplicaRole::ActiveSecondary
                        ))
                {
                    return Err(RuntimeError::AuthorityMismatch(
                        "read access requires an open Primary or Active Secondary".into(),
                    ));
                }
                drop(state);
                self.state.write().await.read_status = read_status;
            }
            RuntimeEffectAction::SetAccessStatus { read, write } => {
                let state = self.state.read().await;
                if read == AccessStatus::Granted
                    && (!state.open
                        || !matches!(
                            state.role,
                            ReplicaRole::Primary | ReplicaRole::ActiveSecondary
                        ))
                {
                    return Err(RuntimeError::AuthorityMismatch(
                        "read access requires an open Primary or Active Secondary".into(),
                    ));
                }
                let primary_read =
                    read == AccessStatus::Granted && state.role == ReplicaRole::Primary;
                if write == AccessStatus::Granted {
                    if !state.open || state.role != ReplicaRole::Primary {
                        return Err(RuntimeError::NotPrimary);
                    }
                    let authority = state
                        .authority
                        .as_ref()
                        .ok_or(RuntimeError::AuthorityNotAdmitted)?;
                    if authority.primary_identity() != &self.identity {
                        return Err(RuntimeError::ReconfigurationPending);
                    }
                }
                drop(state);
                let replicator = self.replicator.lock().await;
                if primary_read && !replicator.catch_up_complete() {
                    return Err(RuntimeError::ReconfigurationPending);
                }
                drop(replicator);
                if write != AccessStatus::Granted {
                    self.replicator.lock().await.fence_client_writes();
                }
                let mut state = self.state.write().await;
                state.read_status = read;
                state.write_status = write;
            }
            RuntimeEffectAction::WaitForCatchup => {
                self.wait_for_quorum(ReplicaSetQuorumMode::WriteQuorum)
                    .await?;
            }
            RuntimeEffectAction::SetWriteStatus(write_status) => {
                let state = self.state.read().await;
                if write_status == AccessStatus::Granted {
                    if !state.open {
                        return Err(RuntimeError::NotOpen);
                    }
                    if state.role != ReplicaRole::Primary {
                        return Err(RuntimeError::NotPrimary);
                    }
                    let authority = state
                        .authority
                        .as_ref()
                        .ok_or(RuntimeError::AuthorityNotAdmitted)?;
                    if authority.primary_identity() != &self.identity {
                        return Err(RuntimeError::AuthorityMismatch(
                            "write grant target is not the admitted primary".to_string(),
                        ));
                    }
                }
                drop(state);
                if write_status != AccessStatus::Granted {
                    self.replicator.lock().await.fence_client_writes();
                }
                self.state.write().await.write_status = write_status;
            }
            RuntimeEffectAction::RefreshApplicationProgress => {
                let progress = if let Some(storage) = self.storage.read().await.clone() {
                    storage.durable_progress().await?
                } else {
                    DurableApplicationProgress {
                        applied_lsn: self.control().await?.current_progress().await?,
                        committed_lsn: self.provider().await?.last_committed_lsn().await?,
                    }
                };
                {
                    let mut state = self.state.write().await;
                    state.current_progress = state.current_progress.max(progress.applied_lsn);
                    state.committed_lsn = state.committed_lsn.max(progress.committed_lsn);
                }
                if self.state.read().await.authority.is_some() {
                    self.replicator
                        .lock()
                        .await
                        .record_local_progress(progress.applied_lsn)?;
                    self.finalize_ready_commit().await?;
                }
            }
            RuntimeEffectAction::RetireBuild(build_id) => {
                let mut state = self.state.write().await;
                state.builds.remove(&build_id);
                state.outbound_builds.remove(&build_id);
            }
        }
        Ok(())
    }

    async fn configure_admitted_authority(
        &self,
        authority: &AdmittedAuthority,
        progress: i64,
    ) -> Result<()> {
        self.replicator
            .lock()
            .await
            .admit_authority(authority.clone(), progress)?;
        if authority.local_role() == ReplicaRole::Primary {
            let completed_builds = self
                .state
                .read()
                .await
                .outbound_builds
                .values()
                .filter(|build| {
                    build.progress.completed
                        && authority
                            .current_configuration
                            .members
                            .iter()
                            .any(|member| {
                                member.identity == build.progress.authority.target
                                    && build_handoff_matches(&build.progress.authority, authority)
                            })
                })
                .map(|build| {
                    (
                        build.progress.authority.target.clone(),
                        build.progress.durable_lsn,
                    )
                })
                .collect::<Vec<_>>();
            let mut replicator = self.replicator.lock().await;
            for (identity, progress) in completed_builds {
                replicator.record_build_handoff_progress(identity, progress)?;
            }
        }
        if authority.local_role() == ReplicaRole::Primary {
            let primary = self.primary.read().await.clone().ok_or_else(|| {
                RuntimeError::Application("primary role requires IFabricPrimaryReplicator".into())
            })?;
            if let Some(previous) = authority.previous_configuration.clone() {
                primary
                    .update_catch_up_replica_set_configuration(
                        authority.current_configuration.clone(),
                        previous,
                    )
                    .await?;
            } else {
                primary
                    .update_current_replica_set_configuration(
                        authority.current_configuration.clone(),
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn finalize_ready_commit(&self) -> Result<()> {
        let _write = self.write_lock.lock().await;
        self.finalize_ready_commit_locked().await
    }

    async fn finalize_ready_commit_locked(&self) -> Result<()> {
        let Some(ready_lsn) = self.replicator.lock().await.ready_commit_lsn() else {
            return Ok(());
        };
        let progress = self.storage().await?.commit(ready_lsn).await?;
        if progress.committed_lsn < ready_lsn || progress.applied_lsn < progress.committed_lsn {
            return Err(RuntimeError::Application(
                "application did not durably record quorum-ready progress".to_string(),
            ));
        }
        let committed_writes = self
            .state
            .read()
            .await
            .local_writes
            .values()
            .filter(|write| write.phase == LocalWritePhase::Registered && write.lsn <= ready_lsn)
            .cloned()
            .map(|write| DurableLocalWrite {
                phase: LocalWritePhase::Committed,
                ..write
            })
            .collect::<Vec<_>>();
        for write in &committed_writes {
            self.local_write_journal.record_local_write(write).await?;
        }
        self.replicator.lock().await.finalize_commit(ready_lsn)?;
        let mut state = self.state.write().await;
        for write in committed_writes {
            state.local_writes.remove(&write.operation_id);
        }
        state.committed_lsn = state.committed_lsn.max(progress.committed_lsn);
        Ok(())
    }

    async fn load_replication_progress_with_handoff(
        &self,
        authority: &AdmittedAuthority,
    ) -> Result<ReplicationProgress> {
        let mut progress = self
            .replication_progress_store
            .load_replication_progress(&authority.fence())
            .await?
            .unwrap_or(ReplicationProgress {
                fence: authority.fence(),
                verified_lsn: 0,
            });
        if authority.previous_configuration.is_none()
            && let Some(configuration_progress) = self
                .replication_progress_store
                .load_configuration_progress(
                    authority.current_configuration.epoch,
                    &authority.current_configuration.configuration_id,
                )
                .await?
        {
            progress.verified_lsn = progress
                .verified_lsn
                .max(configuration_progress.verified_lsn);
        }
        if let Some(previous) = authority.previous_configuration.as_ref()
            && previous.primary_id == authority.current_configuration.primary_id
            && previous
                .members
                .iter()
                .find(|member| member.identity.replica_id == previous.primary_id)
                .zip(
                    authority
                        .current_configuration
                        .members
                        .iter()
                        .find(|member| {
                            member.identity.replica_id == authority.current_configuration.primary_id
                        }),
                )
                .is_some_and(|(previous, current)| previous.identity == current.identity)
            && previous
                .members
                .iter()
                .any(|member| member.identity == self.identity)
            && authority
                .current_configuration
                .members
                .iter()
                .any(|member| member.identity == self.identity)
        {
            let previous_fence = crate::authority::AuthorityFence {
                epoch: previous.epoch,
                previous_configuration_id: None,
                current_configuration_id: previous.configuration_id.clone(),
            };
            if let Some(previous_progress) = self
                .replication_progress_store
                .load_replication_progress(&previous_fence)
                .await?
            {
                progress.verified_lsn = progress.verified_lsn.max(previous_progress.verified_lsn);
            }
        }
        let handoff_lsn = self
            .state
            .read()
            .await
            .builds
            .values()
            .filter(|build| build.completed)
            .filter(|build| build.authority.target == self.identity)
            .filter(|build| build_handoff_matches(&build.authority, authority))
            .map(|build| build.durable_lsn)
            .max();
        if let Some(handoff_lsn) = handoff_lsn
            && handoff_lsn > progress.verified_lsn
        {
            progress.verified_lsn = handoff_lsn;
        }
        self.replication_progress_store
            .record_replication_progress(&progress)
            .await?;
        Ok(progress)
    }
}

#[async_trait::async_trait]
impl ManagedReplicator for DefaultReplicatorInner {
    async fn attach_interfaces(
        &self,
        control: Arc<dyn Replicator>,
        primary: Option<Arc<dyn PrimaryReplicator>>,
    ) -> Result<()> {
        self.attach_interfaces(control, primary).await
    }

    async fn complete_open(&self, replication_address: String) -> Result<()> {
        self.complete_open(replication_address).await
    }

    async fn fence_writes(&self) -> Result<()> {
        self.check_aborted()?;
        self.state.write().await.write_status = AccessStatus::ReconfigurationPending;
        self.replicator.lock().await.fence_client_writes();
        self.changed.notify_waiters();
        Ok(())
    }

    async fn cancel_configuration_work(&self) -> Result<()> {
        self.check_aborted()?;
        self.fence_generation.fetch_add(1, Ordering::AcqRel);
        self.replicator.lock().await.fence_client_writes();
        self.changed.notify_waiters();
        Ok(())
    }

    async fn restore_authority(&self) -> Result<()> {
        self.restore_authority().await
    }

    async fn recover_pending_writes(&self) -> Result<()> {
        self.recover_pending_local_writes().await
    }

    async fn repair_peer(&self, identity: ReplicaIdentity, progress: Lsn) -> Result<()> {
        self.repair_peer_from_history(identity, progress).await
    }

    async fn execute_action(&self, action: RuntimeEffectAction) -> Result<()> {
        if matches!(
            action,
            RuntimeEffectAction::Open(_)
                | RuntimeEffectAction::ChangeRole(_)
                | RuntimeEffectAction::ChangeReplicatorRole(_)
                | RuntimeEffectAction::UpdateEpoch
                | RuntimeEffectAction::ChangeApplicationRole(_)
                | RuntimeEffectAction::BuildReplica { .. }
                | RuntimeEffectAction::Close
                | RuntimeEffectAction::Abort
        ) {
            return Err(RuntimeError::Application(
                "application lifecycle actions belong to the hosting runtime".into(),
            ));
        }
        if matches!(action, RuntimeEffectAction::WaitForCatchup) {
            self.check_aborted()?;
            self.execute_action(action).await?;
            self.changed.notify_waiters();
            return Ok(());
        }
        let _guard = self.effect_lock.lock().await;
        self.check_aborted()?;
        self.execute_action(action).await?;
        self.changed.notify_waiters();
        Ok(())
    }

    async fn snapshot(&self) -> RuntimeSnapshot {
        self.snapshot().await
    }

    async fn begin_write(&self, write: ClientWrite) -> Result<PendingWrite> {
        self.begin_write(write).await
    }

    async fn accept_acknowledgement(&self, acknowledgement: ReplicationAck) -> Result<()> {
        self.accept_acknowledgement(acknowledgement).await
    }

    async fn cancel_outbound_build(&self, build_id: &OperationId) -> Result<()> {
        self.state.write().await.outbound_builds.remove(build_id);
        self.changed.notify_waiters();
        Ok(())
    }

    async fn prepare_copy(&self, request: PrepareCopyRequest) -> Result<PreparedCopy> {
        self.prepare_copy(request).await
    }

    async fn accept_copy_acknowledgement(&self, acknowledgement: CopyAck) -> Result<()> {
        self.accept_copy_acknowledgement(acknowledgement).await
    }

    async fn receive_copy_item(&self, item: CopyItem) -> Result<CopyAck> {
        self.receive_copy_item(item).await
    }

    async fn receive_replication(&self, item: ReplicationItem) -> Result<PendingReplication> {
        self.weak_self
            .upgrade()
            .ok_or(RuntimeError::Closed)?
            .receive_replication(item)
            .await
    }

    async fn next_outbound(&self) -> Option<OutboundOperation> {
        self.outbound_rx.lock().await.recv().await
    }

    fn abort(&self) {
        self.control_abort();
    }
}

fn validate_durable_ack(
    lsn: i64,
    required_committed_lsn: i64,
    acknowledgement: DurableApplicationAck,
) -> Result<()> {
    if acknowledgement.applied_lsn != lsn
        || acknowledgement.committed_lsn < required_committed_lsn
        || acknowledgement.committed_lsn > acknowledgement.applied_lsn
    {
        return Err(RuntimeError::Application(
            "durable acknowledgement does not prove application acceptance".to_string(),
        ));
    }
    Ok(())
}

fn insert_copy_operation(
    operations: &mut BTreeMap<i64, Operation>,
    operation: Operation,
) -> Result<()> {
    if operation.lsn <= 0 {
        return Err(RuntimeError::InvalidReplication(
            "copy operation LSN must be positive".to_string(),
        ));
    }

    if let Some(existing) = operations.get_mut(&operation.lsn) {
        if existing.data != operation.data {
            return Err(RuntimeError::InvalidReplication(
                "copy and retained streams disagree at the same LSN".to_string(),
            ));
        }
        existing.committed_lsn = existing.committed_lsn.max(operation.committed_lsn);
        return Ok(());
    }
    operations.insert(operation.lsn, operation);
    Ok(())
}

async fn send_copy_item(
    sender: &mpsc::Sender<Result<CopyItem>>,
    item: CopyItem,
    cancellation: &mut watch::Receiver<bool>,
) -> Result<()> {
    tokio::select! {
        biased;
        _ = cancellation.changed() => Err(RuntimeError::OperationCancelled),
        result = sender.send(Ok(item)) => result.map_err(|_| RuntimeError::OperationCancelled),
    }
}

fn copy_snapshot_item(authority: &BuildAuthority, sequence: u64, data: Bytes) -> CopyItem {
    CopyItem {
        build_id: authority.build_id.clone(),
        sender: authority.source.clone(),
        receiver: authority.target.clone(),
        epoch: authority.current_configuration.epoch,
        current_configuration_id: authority.current_configuration.configuration_id.clone(),
        sequence,
        lsn: 0,
        committed_lsn: 0,
        replication_boundary_lsn: authority.replication_boundary_lsn,
        final_item: false,
        data,
        snapshot_chunk: true,
    }
}

fn copy_final_item(authority: &BuildAuthority, sequence: u64, committed_lsn: i64) -> CopyItem {
    CopyItem {
        build_id: authority.build_id.clone(),
        sender: authority.source.clone(),
        receiver: authority.target.clone(),
        epoch: authority.current_configuration.epoch,
        current_configuration_id: authority.current_configuration.configuration_id.clone(),
        sequence,
        lsn: authority.replication_boundary_lsn,
        committed_lsn: committed_lsn.min(authority.replication_boundary_lsn),
        replication_boundary_lsn: authority.replication_boundary_lsn,
        final_item: true,
        data: Bytes::new(),
        snapshot_chunk: false,
    }
}

fn copy_operation_item(
    authority: &BuildAuthority,
    sequence: u64,
    operation: &Operation,
) -> CopyItem {
    CopyItem {
        build_id: authority.build_id.clone(),
        sender: authority.source.clone(),
        receiver: authority.target.clone(),
        epoch: authority.current_configuration.epoch,
        current_configuration_id: authority.current_configuration.configuration_id.clone(),
        sequence,
        lsn: operation.lsn,
        committed_lsn: operation.committed_lsn.min(operation.lsn),
        replication_boundary_lsn: authority.replication_boundary_lsn,
        final_item: false,
        data: operation.data.clone(),
        snapshot_chunk: false,
    }
}

fn build_handoff_matches(build: &BuildAuthority, authority: &AdmittedAuthority) -> bool {
    if !authority
        .current_configuration
        .members
        .iter()
        .any(|member| member.identity == build.target)
        || authority.primary_identity() != &build.source
    {
        return false;
    }
    match build.kind {
        BuildAuthorityKind::Bootstrap => {
            authority.transition_kind == Some(kuberic_protocol::types::TransitionKind::Bootstrap)
                && authority.previous_configuration.is_none()
                && authority.current_configuration.configuration_id
                    == build.current_configuration.configuration_id
        }
        BuildAuthorityKind::Provisioning => {
            authority
                .previous_configuration
                .as_ref()
                .is_some_and(|previous| {
                    previous.configuration_id == build.current_configuration.configuration_id
                })
        }
        BuildAuthorityKind::Failover => {
            authority.transition_kind == Some(kuberic_protocol::types::TransitionKind::Failover)
                && authority.previous_configuration.is_some()
                && authority.current_configuration.configuration_id
                    == build.current_configuration.configuration_id
        }
    }
}

fn build_postcondition(value: BuildProgress) -> BuildPostcondition {
    BuildPostcondition {
        authority: value.authority,
        last_sequence: value.last_sequence,
        durable_lsn: value.durable_lsn,
        completed: value.completed,
    }
}

fn build_postconditions(state: &RuntimeState) -> Vec<BuildPostcondition> {
    state
        .builds
        .values()
        .cloned()
        .chain(
            state
                .outbound_builds
                .values()
                .map(|build| build.progress.clone()),
        )
        .map(build_postcondition)
        .collect()
}
