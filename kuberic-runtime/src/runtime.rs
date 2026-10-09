use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use crate::authority::RetiredAuthority;
use crate::protocol::types::{
    AccessStatus, ConfigurationDescriptor, Epoch, OperationId, ProcessSessionId, ReplicaId,
    ReplicaIdentity, ReplicaRole, SecondaryRemovalPreparation, SecondaryRemovalStage,
    SecondaryRemovalWitness, SecondaryScaleDownCleanup,
};
use crate::protocol::validation::{
    validate_secondary_removal_preparation, validate_secondary_scale_down_cleanup,
};
use crate::receipts::{NativeProgressStatus, NativeTopologyStatus};
use crate::transport::{CopyAck, CopyItem, OutboundOperation, ReplicationAck, ReplicationItem};
use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::{Mutex, Notify, RwLock, mpsc, oneshot, watch};

use crate::application::{
    ClientWrite, DurableApplicationAck, DurableApplicationProgress, Lsn, Operation,
    OperationDataStream, StateProvider, WriteReceipt,
};
use crate::authority::{
    BuildAuthority, BuildAuthorityKind, BuildAuthorityStore, BuildProgressStore,
    DurableBuildProgress, DurableLocalWrite, LocalWriteJournal, LocalWritePhase,
    ReplicationProgress, ReplicationProgressStore,
};
use crate::effects::BuildPostcondition;
use crate::engine::DurableState;
use crate::replicator::configuration::ManagedReplicaConfiguration;
use crate::replicator::copy::{
    BuildConfiguration, BuildProgress, CopyItemStream, PrepareCopyRequest, PreparedCopy,
};
use crate::replicator::log::{PreparedWrite, ReplicationLog};
use crate::replicator::stream::{OperationCompletion, OperationMetadata, ServiceStreams};
use crate::replicator::{
    ManagedAccessPreparation, ManagedConfigurationPreparation, ManagedFenceGuard,
    ManagedOperationFence, ManagedReplicaStore, ManagedReplicatorDataPlane,
    ManagedReplicatorLifecycle, PrimaryReplicator, ReplicaInformation, ReplicaSetQuorumMode,
    Replicator,
};
use crate::{Result, RuntimeError};

#[derive(Debug)]
struct RuntimeState {
    open: bool,
    replication_address: Option<String>,
    role: ReplicaRole,
    read_status: AccessStatus,
    write_status: AccessStatus,
    authority: Option<ManagedReplicaConfiguration>,
    configuration_generation: u64,
    pending_configuration: Option<PendingManagedConfiguration>,
    prepared_secondary_removal: Option<SecondaryRemovalPreparation>,
    removal_in_progress: Option<crate::protocol::types::SecondaryScaleDownIntent>,
    retired_authority: Option<RetiredAuthority>,
    retiring_authority: Option<RetiredAuthority>,
    accepted_secondary_removal: Option<SecondaryScaleDownCleanup>,
    replication_progress: Option<ReplicationProgress>,
    current_progress: i64,
    committed_lsn: i64,
    builds: BTreeMap<OperationId, BuildProgress>,
    inbound_build_generations: BTreeMap<OperationId, u64>,
    outbound_builds: BTreeMap<OperationId, OutboundBuild>,
    cancelled_outbound_builds: BTreeSet<OperationId>,
    removed_replicas: BTreeSet<ReplicaId>,
    local_writes: BTreeMap<OperationId, DurableLocalWrite>,
    peer_repair_targets: BTreeMap<ReplicaIdentity, i64>,
}

#[derive(Debug, Clone)]
struct PendingManagedConfiguration {
    configuration: ManagedReplicaConfiguration,
    engine_generation: u64,
    host_generation: u64,
    active: Arc<AtomicBool>,
    replication_progress: ReplicationProgress,
    current_progress: i64,
    previous_epoch: Option<Epoch>,
    prior_access: (AccessStatus, AccessStatus),
    authority_changed: bool,
    preserve_access: bool,
    completed_builds: Vec<(ReplicaIdentity, i64)>,
}

struct SecondaryRemovalState {
    fence: ManagedOperationFence,
    preparation: Option<SecondaryRemovalPreparation>,
    witness: Option<SecondaryRemovalWitness>,
    accepted: Option<SecondaryScaleDownCleanup>,
    verified_lsn: Option<Lsn>,
    committed_lsn: Lsn,
}

#[derive(Debug, Clone)]
struct OutboundBuild {
    progress: BuildProgress,
    final_sequence: Option<u64>,
    next_sequence: u64,
    emitted: BTreeMap<u64, EmittedBuildItem>,
    pending_operations: BTreeMap<i64, Operation>,
    catching_up: bool,
    stream_tx: Option<mpsc::Sender<Result<CopyItem>>>,
    generation: u64,
}

struct BuildPreparationGuard {
    decision: Option<tokio::sync::oneshot::Sender<bool>>,
    completion: tokio::task::JoinHandle<()>,
}

impl BuildPreparationGuard {
    fn new(engine: Weak<DefaultReplicatorInner>, build_id: OperationId, generation: u64) -> Self {
        let (decision, completion) = tokio::sync::oneshot::channel();
        let completion = tokio::spawn(async move {
            if completion.await != Ok(true)
                && let Some(engine) = engine.upgrade()
            {
                let mut state = engine.state.write().await;
                if state
                    .outbound_builds
                    .get(&build_id)
                    .is_some_and(|build| build.generation == generation)
                {
                    state.outbound_builds.remove(&build_id);
                }
            }
        });
        Self {
            decision: Some(decision),
            completion,
        }
    }

    async fn finish(mut self, success: bool) {
        if let Some(decision) = self.decision.take() {
            let _ = decision.send(success);
        }
        let _ = self.completion.await;
    }
}

#[derive(Debug, Clone, Copy)]
struct EmittedBuildItem {
    lsn: i64,
    final_item: bool,
    snapshot_chunk: bool,
}

pub(crate) struct PendingWrite {
    pub(crate) lsn: i64,
    pub(crate) replication_items: Vec<ReplicationItem>,
    pub(crate) build_items: Vec<CopyItem>,
    completion: oneshot::Receiver<Result<i64>>,
    aborted: watch::Receiver<bool>,
}

pub(crate) struct PendingReplication {
    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    pub(crate) received: ReplicationAck,
    applied: Pin<Box<dyn Future<Output = Result<ReplicationAck>> + Send>>,
}

impl PendingReplication {
    pub(crate) async fn applied(self) -> Result<ReplicationAck> {
        self.applied.await
    }
}

impl PendingWrite {
    pub(crate) async fn committed(self) -> Result<WriteReceipt> {
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

tokio::task_local! {
    static RECOVERING_CONFIGURATION: bool;
}

fn recovering_configuration() -> bool {
    RECOVERING_CONFIGURATION
        .try_with(|recovering| *recovering)
        .unwrap_or(false)
}

pub(crate) struct DefaultReplicatorInner {
    pub(crate) identity: ReplicaIdentity,
    storage: RwLock<Option<Arc<dyn DurableState>>>,
    replica_authority_store: Arc<dyn ManagedReplicaStore>,
    replication_progress_store: Arc<dyn ReplicationProgressStore>,
    local_write_journal: Arc<dyn LocalWriteJournal>,
    build_authority_store: Arc<dyn BuildAuthorityStore>,
    build_progress_store: Arc<dyn BuildProgressStore>,
    state: RwLock<RuntimeState>,
    // Lock order: effect_lock -> delivery_lock whenever an operation needs both.
    // Control paths that do not own effect_lock must acquire delivery_lock before
    // advancing fence_generation, so boundary persistence has one linearization
    // point with authority, role, epoch, access, receive, and cancellation work.
    effect_lock: Mutex<()>,
    copy_prepare_lock: Mutex<()>,
    delivery_lock: Arc<Mutex<()>>,
    write_lock: Mutex<()>,
    write_generation: AtomicU64,
    fence_generation: Arc<AtomicU64>,
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
        replica_authority_store: Arc<dyn ManagedReplicaStore>,
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
                configuration_generation: 0,
                pending_configuration: None,
                prepared_secondary_removal: None,
                removal_in_progress: None,
                retired_authority: None,
                retiring_authority: None,
                accepted_secondary_removal: None,
                replication_progress: None,
                current_progress: 0,
                committed_lsn: 0,
                builds: BTreeMap::new(),
                inbound_build_generations: BTreeMap::new(),
                outbound_builds: BTreeMap::new(),
                cancelled_outbound_builds: BTreeSet::new(),
                removed_replicas: BTreeSet::new(),
                local_writes: BTreeMap::new(),
                peer_repair_targets: BTreeMap::new(),
            }),
            effect_lock: Mutex::new(()),
            copy_prepare_lock: Mutex::new(()),
            delivery_lock: Arc::new(Mutex::new(())),
            write_lock: Mutex::new(()),
            write_generation: AtomicU64::new(0),
            fence_generation: Arc::new(AtomicU64::new(0)),
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

    async fn check_no_pending_configuration(&self) -> Result<()> {
        if self.state.read().await.pending_configuration.is_some() {
            Err(RuntimeError::ReconfigurationPending)
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
            let mut pending = self.resume_local_write(write.clone()).await?;
            loop {
                self.publish_replication(&pending).await?;
                match pending.committed().await {
                    Ok(_) => break,
                    Err(RuntimeError::WriteCompletionClosed) => {
                        pending = self.resume_local_write(write.clone()).await?;
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
        let _delivery = self.delivery_lock.lock().await;
        self.fence_generation.fetch_add(1, Ordering::AcqRel);
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
        let _delivery = self.delivery_lock.lock().await;
        self.fence_generation.fetch_add(1, Ordering::AcqRel);
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
        if let Some(streams) = self.streams.read().await.as_ref() {
            streams.shutdown();
        }
        let _delivery = self.delivery_lock.lock().await;
        self.fence_generation.fetch_add(1, Ordering::AcqRel);
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
        let _delivery = self.delivery_lock.lock().await;
        self.fence_generation.fetch_add(1, Ordering::AcqRel);
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
            .load_configuration()
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
        let mut replicator = self.replicator.lock().await;
        if authority.current_configuration.epoch < replicator.epoch() {
            return Err(RuntimeError::AuthorityMismatch(
                "replicator epoch cannot regress".into(),
            ));
        }
        replicator.admit_prepared_authority(authority, progress, false);
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
        let build_id = replica.build_id.clone();
        {
            let mut state = self.state.write().await;
            state.removed_replicas.remove(&replica.identity.replica_id);
            state.cancelled_outbound_builds.remove(&build_id);
        }
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
            if state.cancelled_outbound_builds.contains(&build_id) {
                return Err(RuntimeError::OperationCancelled);
            }
            if state.authority.as_ref().map(|a| a.fence()) != fence {
                return Err(RuntimeError::AuthorityMismatch(
                    "authority changed during replica build".into(),
                ));
            }
            if state.outbound_builds.values().any(|build| {
                build.progress.authority.build_id == build_id
                    && build.progress.authority.target == replica.identity
                    && build.generation == build_generation
                    && build.progress.completed
                    && build
                        .progress
                        .catch_up_boundary_lsn
                        .is_some_and(|boundary| build.progress.durable_lsn >= boundary)
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

    pub(crate) async fn restore_authority(
        &self,
        authority: Option<ManagedReplicaConfiguration>,
        host_generation: u64,
    ) -> Result<()> {
        if self
            .replica_authority_store
            .load_retired_authority()
            .await?
            .is_some()
            || self
                .replica_authority_store
                .load_retirement_started()
                .await?
                .is_some()
        {
            return Err(RuntimeError::Closed);
        }
        let preparation = self
            .replica_authority_store
            .load_secondary_removal()
            .await?;
        self.state.write().await.removal_in_progress =
            preparation.as_ref().map(|p| p.intent.clone());
        self.state.write().await.prepared_secondary_removal = preparation;
        let Some(authority) = authority else {
            return Ok(());
        };
        {
            let mut state = self.state.write().await;
            if state.prepared_secondary_removal.as_ref().is_some_and(|p| {
                authority.current_configuration.epoch > p.intent.current_configuration.epoch
            }) {
                state.prepared_secondary_removal = None;
                state.removal_in_progress = None;
            }
        }
        let accepted = self
            .replica_authority_store
            .load_secondary_removal_commit()
            .await?
            .filter(|c| {
                authority.previous_configuration.is_none()
                    && authority.secondary_removal.as_ref() == Some(&c.evidence)
            });
        self.state.write().await.accepted_secondary_removal = accepted.clone();
        RECOVERING_CONFIGURATION
            .scope(true, async {
                let prepared = self
                    .prepare_replica_configuration(authority, host_generation)
                    .await?;
                self.commit_replica_configuration(prepared).await
            })
            .await?;
        if let Some(committed) = &accepted {
            validate_secondary_scale_down_cleanup(committed)
                .map_err(|e| RuntimeError::AuthorityMismatch(e.to_string()))?;
            let mut replicator = self.replicator.lock().await;
            for witness in &committed.current_only_write_quorum {
                if witness.identity != self.identity {
                    replicator.restore_committed_secondary_removal(witness)?;
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn begin_write(&self, write: ClientWrite) -> Result<PendingWrite> {
        let _guard = self.effect_lock.lock().await;
        self.check_no_pending_configuration().await?;
        self.begin_write_locked(write, false).await
    }

    async fn resume_local_write(&self, write: ClientWrite) -> Result<PendingWrite> {
        let _guard = self.effect_lock.lock().await;
        self.begin_write_locked(write, true).await
    }

    async fn begin_write_locked(
        &self,
        write: ClientWrite,
        recovering: bool,
    ) -> Result<PendingWrite> {
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
        if state.write_status != AccessStatus::Granted && !recovering {
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
            if recovering {
                return Err(RuntimeError::LocalWritePending(
                    write.operation_id.to_string(),
                ));
            }
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
            let progress = self.storage().await?.durable_progress().await?;
            validate_durable_ack(progress.applied_lsn, lsn, progress)?;
            self.verify_local_write_progress(lsn).await?;
            self.replicator
                .lock()
                .await
                .restore_committed_write(&operation)?;
            let mut state = self.state.write().await;
            state.local_writes.remove(&write.operation_id);
            state.current_progress = state.current_progress.max(progress.applied_lsn);
            state.committed_lsn = state.committed_lsn.max(progress.committed_lsn);
            drop(state);
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
        validate_durable_ack(lsn.max(progress.applied_lsn), committed_lsn, durable_ack)?;
        self.verify_local_write_progress(lsn).await?;
        if durable_ack.committed_lsn >= lsn {
            self.local_write_journal
                .record_local_write(&DurableLocalWrite {
                    phase: LocalWritePhase::Committed,
                    ..durable_write
                })
                .await?;
            self.replicator
                .lock()
                .await
                .restore_committed_write(&operation)?;
            let mut state = self.state.write().await;
            state.local_writes.remove(&write.operation_id);
            state.current_progress = state.current_progress.max(durable_ack.applied_lsn);
            state.committed_lsn = state.committed_lsn.max(durable_ack.committed_lsn);
            drop(state);
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
            // Recovery may register a write already sent from durable retained
            // history while it was still only application-applied.
            if build.emitted.values().any(|item| {
                !item.snapshot_chunk && !item.final_item && item.lsn == build_operation.lsn
            }) {
                continue;
            }
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
            if let Some(sender) = build.stream_tx.clone() {
                live_items.push((sender, item));
            }
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

    pub(crate) async fn accept_acknowledgement(
        &self,
        acknowledgement: ReplicationAck,
    ) -> Result<()> {
        let _guard = self.effect_lock.lock().await;
        self.check_aborted()?;
        let durable_authority = self.replica_authority_store.load_configuration().await?;
        if durable_authority != self.state.read().await.authority {
            return Err(RuntimeError::AuthorityMismatch(
                "acknowledgement authority differs from durable admission".into(),
            ));
        }
        self.check_no_pending_configuration().await?;
        self.replicator.lock().await.acknowledge(&acknowledgement)?;
        self.finalize_ready_commit().await?;
        self.changed.notify_waiters();
        Ok(())
    }

    pub(crate) async fn prepare_copy(&self, request: PrepareCopyRequest) -> Result<PreparedCopy> {
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
                let kind = authority.build_kind;
                (kind, authority.current_configuration)
            }
            #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
            BuildConfiguration::Bootstrap(configuration) => {
                if state.write_status == AccessStatus::Granted {
                    return Err(RuntimeError::WriteClosed(AccessStatus::Granted));
                }
                (BuildAuthorityKind::Bootstrap, configuration)
            }
        };
        drop(state);

        let replicator = self.replicator.lock().await;
        let replicator_progress = replicator.current_progress();
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
        if current_highest < boundary || application_progress.committed_lsn < boundary {
            return Err(RuntimeError::Application(
                "copy boundary is not durably committed by the application".to_string(),
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
                catch_up_boundary_lsn: None,
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
                stream_tx: Some(stream_tx.clone()),
                generation: prepare_generation,
            },
        );
        let preparation = BuildPreparationGuard::new(
            self.weak_self.clone(),
            build_authority.build_id.clone(),
            prepare_generation,
        );
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
                preparation.finish(false).await;
                return Err(error);
            }
        };
        let guard = self.effect_lock.lock().await;
        if let Err(error) = self.check_delivery_generation(prepare_generation) {
            preparation.finish(false).await;
            return Err(error);
        }
        let replicator = self.replicator.lock().await;
        let replicator_epoch = replicator.epoch();
        drop(replicator);
        if replicator_epoch != Epoch::default()
            && replicator_epoch != build_authority.current_configuration.epoch
        {
            preparation.finish(false).await;
            return Err(RuntimeError::AuthorityMismatch(
                "build authority was fenced during preparation".into(),
            ));
        }
        if current_highest > boundary
            && ((boundary + 1)..=current_highest).any(|lsn| !operations.contains_key(&lsn))
        {
            preparation.finish(false).await;
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
                preparation.finish(false).await;
                return Err(error);
            }
        };
        preparation.finish(true).await;
        let engine = self.weak_self.upgrade().ok_or(RuntimeError::Closed)?;
        let producer_authority = build_authority.clone();
        tokio::spawn(async move {
            engine
                .produce_copy_stream(
                    producer_authority,
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
            #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
            authority: build_authority,
            items,
        })
    }

    async fn produce_copy_stream(
        &self,
        authority: BuildAuthority,
        mut copy_stream: OperationDataStream,
        initial_operations: BTreeMap<i64, Operation>,
        sender: mpsc::Sender<Result<CopyItem>>,
        generation: u64,
        cancellation: watch::Receiver<bool>,
    ) {
        let result = self
            .produce_copy_stream_inner(
                &authority,
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

    async fn produce_copy_stream_inner(
        &self,
        authority: &BuildAuthority,
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

        let catch_up_boundary_lsn = self
            .freeze_build_catch_up_boundary(
                authority,
                generation,
                &initial_operations,
                &cancellation,
            )
            .await?;
        let final_item = copy_final_item(authority, sequence, catch_up_boundary_lsn);
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

    async fn freeze_build_catch_up_boundary(
        &self,
        authority: &BuildAuthority,
        generation: u64,
        initial_operations: &BTreeMap<i64, Operation>,
        cancellation: &watch::Receiver<bool>,
    ) -> Result<i64> {
        self.check_copy_producer(generation, cancellation)?;
        let _effect = self.effect_lock.lock().await;
        self.check_copy_producer(generation, cancellation)?;
        let _delivery = self.delivery_lock.lock().await;
        self.check_copy_producer(generation, cancellation)?;
        let state = self.state.read().await;
        let build = state
            .outbound_builds
            .get(&authority.build_id)
            .ok_or(RuntimeError::OperationCancelled)?;
        if build.generation != generation || build.progress.authority != *authority {
            return Err(RuntimeError::AuthorityMismatch(
                "catch-up boundary belongs to a different build generation".into(),
            ));
        }
        let boundary = build.progress.catch_up_boundary_lsn.unwrap_or_else(|| {
            state
                .current_progress
                .max(authority.replication_boundary_lsn)
                // Application acceptance can precede the runtime's progress update.
                .max(initial_operations.keys().next_back().copied().unwrap_or(0))
        });
        let mut available = initial_operations
            .keys()
            .chain(build.pending_operations.keys())
            .copied()
            .filter(|lsn| *lsn > authority.replication_boundary_lsn && *lsn <= boundary)
            .collect::<BTreeSet<_>>();
        drop(state);
        let mut expected = authority.replication_boundary_lsn.checked_add(1);
        while let Some(lsn) = expected.filter(|lsn| *lsn <= boundary) {
            if !available.remove(&lsn) {
                return Err(RuntimeError::InvalidReplication(
                    "post-enumeration operations do not close the catch-up boundary".into(),
                ));
            }
            expected = lsn.checked_add(1);
        }

        let mut state = self.state.write().await;
        let build = state
            .outbound_builds
            .get_mut(&authority.build_id)
            .ok_or(RuntimeError::OperationCancelled)?;
        if build.generation != generation || build.progress.authority != *authority {
            return Err(RuntimeError::AuthorityMismatch(
                "catch-up boundary belongs to a different build generation".into(),
            ));
        }
        if build
            .progress
            .catch_up_boundary_lsn
            .is_some_and(|existing| existing != boundary)
        {
            return Err(RuntimeError::AuthorityMismatch(
                "catch-up boundary changed for an immutable build".into(),
            ));
        }
        let mut progress = build.progress.clone();
        progress.catch_up_boundary_lsn = Some(boundary);
        drop(state);
        self.build_progress_store
            .record_build_progress(&progress)
            .await?;
        // Cancellation can arrive while the immutable durable write is in
        // flight. Re-check before publishing it into the active generation; an
        // exact retry may reuse a linearized write, while a fenced authority
        // ignores it.
        self.check_copy_producer(generation, cancellation)?;
        let mut state = self.state.write().await;
        let build = state
            .outbound_builds
            .get_mut(&authority.build_id)
            .ok_or(RuntimeError::OperationCancelled)?;
        if build.generation != generation || build.progress.authority != *authority {
            return Err(RuntimeError::AuthorityMismatch(
                "build changed before catch-up boundary persistence".into(),
            ));
        }
        build.progress = progress;
        Ok(boundary)
    }

    fn check_copy_producer(
        &self,
        generation: u64,
        cancellation: &watch::Receiver<bool>,
    ) -> Result<()> {
        if *cancellation.borrow() {
            return Err(RuntimeError::OperationCancelled);
        }
        self.check_delivery_generation(generation)
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
        let item =
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
                if build.emitted.values().any(|item| {
                    !item.snapshot_chunk && !item.final_item && item.lsn == operation.lsn
                }) {
                    return Ok(());
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

    pub(crate) async fn accept_copy_acknowledgement(&self, acknowledgement: CopyAck) -> Result<()> {
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
        if acknowledgement.final_item != acknowledgement.catch_up_boundary_lsn.is_some()
            || acknowledgement.catch_up_boundary_lsn
                != acknowledgement
                    .final_item
                    .then_some(build.progress.catch_up_boundary_lsn)
                    .flatten()
        {
            return Err(RuntimeError::AuthorityMismatch(
                "copy acknowledgement has different catch-up authority".into(),
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

    pub(crate) async fn receive_copy_item(&self, envelope: CopyItem) -> Result<CopyAck> {
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
        crate::authority::validate_build_envelope(&authority, &envelope)?;
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
        if envelope.sequence > progress.last_sequence + 1 {
            return Err(RuntimeError::InvalidReplication("copy sequence gap".into()));
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
                if progress.catch_up_boundary_lsn != envelope.catch_up_boundary_lsn {
                    return Err(RuntimeError::AuthorityMismatch(
                        "duplicate final marker changed the catch-up boundary".into(),
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
                    catch_up_boundary_lsn: progress.catch_up_boundary_lsn,
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
                let catch_up_boundary_lsn = envelope.catch_up_boundary_lsn.ok_or_else(|| {
                    RuntimeError::AuthorityMismatch(
                        "final copy marker omitted the catch-up boundary".into(),
                    )
                })?;
                if progress
                    .catch_up_boundary_lsn
                    .is_some_and(|existing| existing != catch_up_boundary_lsn)
                {
                    return Err(RuntimeError::AuthorityMismatch(
                        "final copy marker changed the catch-up boundary".into(),
                    ));
                }
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
                    catch_up_boundary_lsn: envelope.catch_up_boundary_lsn,
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
                if progress.catch_up_boundary_lsn.is_none() {
                    return Err(RuntimeError::AuthorityMismatch(
                        "live build replication lacks a frozen catch-up boundary".into(),
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
                    catch_up_boundary_lsn: progress.catch_up_boundary_lsn,
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
            catch_up_boundary_lsn: envelope.catch_up_boundary_lsn,
            final_item: envelope.final_item,
            snapshot_chunk: envelope.snapshot_chunk,
        })
    }

    pub(crate) async fn receive_replication(
        self: Arc<Self>,
        envelope: ReplicationItem,
    ) -> Result<PendingReplication> {
        let delivery = self.delivery_lock.clone().lock_owned().await;
        let delivery_generation = self.fence_generation.load(Ordering::Acquire);
        self.check_aborted()?;
        self.check_no_pending_configuration().await?;
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
        let durable_authority = self
            .replica_authority_store
            .load_configuration()
            .await?
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        durable_authority.validate()?;
        let authority = durable_authority;
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
        #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
        let received = {
            let received_applied_lsn = replication_progress
                .verified_lsn
                .min(application_progress.applied_lsn);
            ReplicationAck {
                sender: envelope.sender.clone(),
                receiver: self.identity.clone(),
                epoch: envelope.epoch,
                previous_configuration_id: envelope.previous_configuration_id.clone(),
                current_configuration_id: envelope.current_configuration_id.clone(),
                received_lsn: envelope.lsn.max(received_applied_lsn),
                applied_lsn: received_applied_lsn,
                committed_lsn: application_progress.committed_lsn.min(received_applied_lsn),
            }
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
        Ok(PendingReplication {
            #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
            received,
            applied,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn complete_replication_delivery(
        &self,
        delivery_generation: u64,
        authority: ManagedReplicaConfiguration,
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
        if self
            .replica_authority_store
            .load_configuration()
            .await?
            .as_ref()
            != Some(&authority)
        {
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

    async fn authorize_failover_prefix_instruction(&self, safe_lsn: Lsn) -> Result<()> {
        if self.state.read().await.removal_in_progress.is_some()
            || self
                .state
                .read()
                .await
                .authority
                .as_ref()
                .is_some_and(|a| a.secondary_removal.is_some())
        {
            return Err(RuntimeError::AuthorityMismatch(
                "failover prefix cannot certify a secondary removal".into(),
            ));
        }
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
        progress.verified_lsn = progress.verified_lsn.max(durable.applied_lsn.min(safe_lsn));
        if progress.fence != authority.fence() {
            return Err(RuntimeError::AuthorityMismatch(
                "failover-safe prefix belongs to another authority fence".into(),
            ));
        }
        self.replication_progress_store
            .record_replication_progress(&progress)
            .await?;
        self.state.write().await.replication_progress = Some(progress);
        Ok(())
    }

    async fn admit_build_authority_instruction(&self, authority: BuildAuthority) -> Result<()> {
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
                catch_up_boundary_lsn: None,
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
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn prepare_switchover_instruction(
        &self,
        preparation_generation: u64,
        request_id: crate::protocol::types::SwitchoverRequestId,
        source: ReplicaIdentity,
        target: ReplicaIdentity,
        starting_configuration_id: crate::protocol::types::ConfigurationId,
        starting_epoch: Epoch,
    ) -> Result<()> {
        let state = self.state.read().await;
        if state
            .prepared_secondary_removal
            .as_ref()
            .is_some_and(|preparation| {
                state
                    .accepted_secondary_removal
                    .as_ref()
                    .is_none_or(|committed| committed.evidence.preparation != *preparation)
            })
        {
            return Err(RuntimeError::ReconfigurationPending);
        }
        drop(state);
        if preparation_generation == 0 || request_id.is_empty() {
            return Err(RuntimeError::AuthorityMismatch(
                "planned switchover requires a request ID and positive generation".to_string(),
            ));
        }
        let state = self.state.read().await;
        if !state.open {
            return Err(RuntimeError::NotOpen);
        }
        if state.role != ReplicaRole::Primary
            || !matches!(
                state.write_status,
                AccessStatus::Granted | AccessStatus::ReconfigurationPending
            )
        {
            return Err(RuntimeError::NotPrimary);
        }
        let authority = state
            .authority
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        let current = &authority.current_configuration;
        let primary = current
            .members
            .iter()
            .find(|member| member.identity.replica_id == current.primary_id)
            .expect("validated authority has one primary");
        if current.configuration_id != starting_configuration_id
            || current.epoch != starting_epoch
            || source != self.identity
            || primary.identity != source
            || target.replica_id == current.primary_id
            || !current
                .members
                .iter()
                .any(|member| member.identity == target && member.role != ReplicaRole::Primary)
        {
            return Err(RuntimeError::AuthorityMismatch(
                "planned switchover preparation differs from admitted authority".to_string(),
            ));
        }
        drop(state);
        self.replicator.lock().await.fence_client_writes();
        self.state.write().await.write_status = AccessStatus::ReconfigurationPending;
        // A reserved write must be part of the certified prefix even if its
        // original application call failed before preparation.
        let writes = self
            .state
            .read()
            .await
            .local_writes
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for durable in writes {
            let pending = self
                .begin_write_locked(
                    ClientWrite {
                        operation_id: durable.operation_id,
                        data: durable.data,
                    },
                    true,
                )
                .await?;
            self.publish_replication(&pending).await?;
        }
        self.replicator.lock().await.fence_client_writes();
        Ok(())
    }

    async fn refresh_application_progress_instruction(&self) -> Result<()> {
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
        Ok(())
    }

    async fn retire_build_instruction(&self, build_id: OperationId) -> Result<()> {
        let mut state = self.state.write().await;
        state.builds.remove(&build_id);
        state.outbound_builds.remove(&build_id);
        state.cancelled_outbound_builds.remove(&build_id);
        Ok(())
    }

    async fn prepare_secondary_removal_instruction(
        &self,
        intent: crate::protocol::types::SecondaryScaleDownIntent,
        process_session_id: ProcessSessionId,
        report_sequence: u64,
    ) -> Result<()> {
        let mut preparation = SecondaryRemovalPreparation {
            operation_id: intent
                .command_operation_id(SecondaryRemovalStage::Prepare, &intent.primary),
            intent,
            process_session_id,
            report_sequence,
            boundary_lsn: 0,
        };
        validate_secondary_removal_preparation(&preparation)
            .map_err(|e| RuntimeError::AuthorityMismatch(e.to_string()))?;
        if preparation.intent.primary != self.identity {
            return Err(RuntimeError::AuthorityMismatch(
                "preparation belongs to another primary".into(),
            ));
        }
        {
            let state = self.state.read().await;
            let authority = state
                .authority
                .as_ref()
                .ok_or(RuntimeError::AuthorityNotAdmitted)?;
            if !state.open
                || state.role != ReplicaRole::Primary
                || (authority.current_configuration != preparation.intent.previous_configuration
                    && authority
                        .secondary_removal
                        .as_ref()
                        .is_none_or(|e| e.preparation.intent != preparation.intent))
            {
                return Err(RuntimeError::AuthorityMismatch(
                    "preparation no longer matches installed primary authority".into(),
                ));
            }
        }
        if let Some(durable) = self
            .replica_authority_store
            .load_secondary_removal()
            .await?
        {
            preparation.boundary_lsn = durable.boundary_lsn;
            if preparation != durable {
                let state = self.state.read().await;
                let advancing = preparation.intent.previous_configuration.epoch
                    >= durable.intent.current_configuration.epoch
                    && preparation.intent != durable.intent
                    && state.authority.as_ref().is_some_and(|a| {
                        a.previous_configuration.is_none()
                            && a.current_configuration == preparation.intent.previous_configuration
                    })
                    && (state
                        .accepted_secondary_removal
                        .as_ref()
                        .is_some_and(|c| c.evidence.preparation == durable)
                        || preparation.intent.previous_configuration.epoch
                            > durable.intent.current_configuration.epoch
                        || state.removal_in_progress.as_ref() == Some(&preparation.intent));
                if !advancing {
                    return Err(RuntimeError::AuthorityMismatch(
                        "conflicting persisted preparation".into(),
                    ));
                }
                drop(state);
                let mut state = self.state.write().await;
                state.prepared_secondary_removal = None;
                state.removal_in_progress = None;
                preparation.boundary_lsn = 0;
            } else {
                self.state.write().await.prepared_secondary_removal = Some(durable);
            }
        }
        let state = self.state.read().await;
        if let Some(existing) = &state.prepared_secondary_removal {
            preparation.boundary_lsn = existing.boundary_lsn;
            if existing == &preparation {
                return Ok(());
            }
            return Err(RuntimeError::AuthorityMismatch(
                "conflicting removal preparation replay".into(),
            ));
        }
        if state
            .removal_in_progress
            .as_ref()
            .is_some_and(|intent| intent != &preparation.intent)
        {
            return Err(RuntimeError::AuthorityMismatch(
                "conflicting in-flight preparation".into(),
            ));
        }
        let authority = state
            .authority
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        if !state.open
            || state.role != ReplicaRole::Primary
            || preparation.intent.primary != self.identity
            || authority.previous_configuration.is_some()
            || authority.current_configuration != preparation.intent.previous_configuration
        {
            return Err(RuntimeError::AuthorityMismatch(
                "preparation requires the unchanged current-only primary".into(),
            ));
        }
        drop(state);
        self.state.write().await.removal_in_progress = Some(preparation.intent.clone());
        self.state.write().await.read_status = AccessStatus::ReconfigurationPending;
        self.state.write().await.write_status = AccessStatus::ReconfigurationPending;
        self.replicator.lock().await.fence_client_writes();
        // Both ACK completion and write registration hold effect_lock. Resolve
        // journaled identities without waiting for the old write quorum.
        let writes = self.local_write_journal.load_local_writes().await?;
        for write in writes {
            self.begin_write_locked(
                ClientWrite {
                    operation_id: write.operation_id,
                    data: write.data,
                },
                true,
            )
            .await?;
        }
        self.replicator.lock().await.fence_client_writes();
        let durable = self.storage().await?.durable_progress().await?;
        let verified = self
            .state
            .read()
            .await
            .replication_progress
            .as_ref()
            .map_or(0, |p| p.verified_lsn);
        if verified < durable.applied_lsn || durable.committed_lsn > durable.applied_lsn {
            return Err(RuntimeError::AuthorityMismatch(
                "primary prefix is not authority-verified".into(),
            ));
        }
        preparation.boundary_lsn = durable.applied_lsn;
        self.replica_authority_store
            .record_secondary_removal(&preparation)
            .await?;
        self.state.write().await.prepared_secondary_removal = Some(preparation);
        Ok(())
    }

    async fn register_peer_session_instruction(
        &self,
        identity: ReplicaIdentity,
        session: ProcessSessionId,
    ) -> Result<()> {
        self.replicator
            .lock()
            .await
            .register_peer_session(identity, session)?;
        Ok(())
    }

    async fn observe_secondary_removal_witness_instruction(
        &self,
        witness: SecondaryRemovalWitness,
    ) -> Result<()> {
        self.replicator
            .lock()
            .await
            .observe_secondary_removal(&witness)?;
        Ok(())
    }

    async fn observe_secondary_removal_progress_instruction(
        &self,
        witness: SecondaryRemovalWitness,
        committed: SecondaryScaleDownCleanup,
    ) -> Result<()> {
        self.replicator
            .lock()
            .await
            .observe_secondary_removal_progress(&witness, &committed)?;
        Ok(())
    }

    async fn observe_replication_ack_instruction(
        &self,
        acknowledgement: ReplicationAck,
        session: ProcessSessionId,
    ) -> Result<()> {
        if self.replica_authority_store.load_configuration().await?
            != self.state.read().await.authority
        {
            return Err(RuntimeError::AuthorityMismatch(
                "ACK differs from durable authority".into(),
            ));
        }
        self.replicator
            .lock()
            .await
            .acknowledge_in_session(&acknowledgement, &session)?;
        self.finalize_ready_commit().await?;
        Ok(())
    }

    async fn accept_historical_secondary_removal_commit_instruction(
        &self,
        command: crate::protocol::command::AcceptSecondaryRemovalCommit,
    ) -> Result<()> {
        crate::protocol::validation::validate_accept_secondary_removal_commit(&command)
            .map_err(|e| RuntimeError::AuthorityMismatch(e.to_string()))?;
        let committed = &command.committed;
        let state = self.state.read().await;
        let authority = state
            .authority
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        if !command.local_recovery
            || command.target != self.identity
            || state.role != ReplicaRole::ActiveSecondary
            || state.write_status == AccessStatus::Granted
            || state.prepared_secondary_removal.is_some()
            || authority.previous_configuration.is_some()
            || authority.current_configuration
                != committed.evidence.preparation.intent.current_configuration
            || authority.secondary_removal.as_ref() != Some(&committed.evidence)
            || state.current_progress < committed.evidence.preparation.boundary_lsn
            || state.replication_progress.as_ref().is_none_or(|p| {
                p.fence != authority.fence()
                    || p.verified_lsn < committed.evidence.preparation.boundary_lsn
            })
            || state
                .accepted_secondary_removal
                .as_ref()
                .is_some_and(|c| c != committed)
            || self
                .replica_authority_store
                .load_configuration()
                .await?
                .as_ref()
                != Some(authority)
        {
            return Err(RuntimeError::AuthorityMismatch(
                "historical acceptance requires exact verified write-closed secondary authority"
                    .into(),
            ));
        }
        drop(state);
        // Durability belongs to the agent's exact pending/completed effect.
        // Do not persist a live runtime commit or resurrect peer-session credit.
        self.state.write().await.accepted_secondary_removal = Some(committed.clone());
        Ok(())
    }

    async fn accept_secondary_removal_commit_instruction(
        &self,
        committed: SecondaryScaleDownCleanup,
    ) -> Result<()> {
        validate_secondary_scale_down_cleanup(&committed)
            .map_err(|e| RuntimeError::AuthorityMismatch(e.to_string()))?;
        let state = self.state.read().await;
        let authority = state
            .authority
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        if authority.previous_configuration.is_some()
            || authority.secondary_removal.as_ref() != Some(&committed.evidence)
            || authority.current_configuration
                != committed.evidence.preparation.intent.current_configuration
        {
            return Err(RuntimeError::AuthorityMismatch(
                "commit does not match installed reduction".into(),
            ));
        }
        if let Some(existing) = &state.accepted_secondary_removal {
            if existing == &committed {
                return Ok(());
            }
            return Err(RuntimeError::AuthorityMismatch(
                "conflicting reduction commit replay".into(),
            ));
        }
        drop(state);
        self.replicator
            .lock()
            .await
            .validate_secondary_removal_commit(&committed)?;
        for witness in &committed.current_only_write_quorum {
            if witness.identity != self.identity {
                self.replicator
                    .lock()
                    .await
                    .observe_committed_secondary_removal(witness)?;
            }
        }
        if !self.replicator.lock().await.catch_up_complete() {
            return Err(RuntimeError::ReconfigurationPending);
        }
        self.replica_authority_store
            .record_secondary_removal_commit(&committed)
            .await?;
        self.state.write().await.accepted_secondary_removal = Some(committed);
        Ok(())
    }

    async fn fence_retirement_instruction(&self, retired: RetiredAuthority) -> Result<()> {
        retired.validate(&self.identity)?;
        let mut state = self.state.write().await;
        if state
            .retiring_authority
            .as_ref()
            .is_some_and(|old| old != &retired)
        {
            return Err(RuntimeError::AuthorityMismatch(
                "conflicting retirement replay".into(),
            ));
        }
        if let Some(authority) = &state.authority
            && (authority.current_configuration != retired.report.intent.previous_configuration
                || authority.previous_configuration.is_some())
        {
            return Err(RuntimeError::AuthorityMismatch(
                "retirement differs from installed target authority".into(),
            ));
        }
        self.replica_authority_store
            .record_retirement_started(&retired)
            .await?;
        state.read_status = AccessStatus::NotPrimary;
        state.write_status = AccessStatus::NotPrimary;
        state.authority = None;
        state.replication_progress = None;
        state.outbound_builds.clear();
        state.builds.clear();
        state.peer_repair_targets.clear();
        state.retiring_authority = Some(retired);
        drop(state);
        if let Some(streams) = self.streams.read().await.as_ref() {
            streams.shutdown();
        }
        self.fence_generation.fetch_add(1, Ordering::AcqRel);
        self.replicator.lock().await.fence_client_writes();
        Ok(())
    }

    async fn complete_retirement_instruction(&self, retired: RetiredAuthority) -> Result<()> {
        retired.validate(&self.identity)?;
        let state = self.state.read().await;
        if state.open
            || state.role != ReplicaRole::None
            || state.retiring_authority.as_ref() != Some(&retired)
        {
            return Err(RuntimeError::ReconfigurationPending);
        }
        drop(state);
        if let Some(existing) = self
            .replica_authority_store
            .load_retired_authority()
            .await?
        {
            if existing != retired {
                return Err(RuntimeError::AuthorityMismatch(
                    "conflicting durable retirement".into(),
                ));
            }
        } else {
            self.replica_authority_store.retire(&retired).await?;
        }
        self.state.write().await.retired_authority = Some(retired);
        Ok(())
    }

    async fn load_completed_build_handoffs(
        &self,
        authority: &ManagedReplicaConfiguration,
    ) -> Result<Vec<(ReplicaIdentity, i64)>> {
        if authority.local_role() != ReplicaRole::Primary {
            return Ok(Vec::new());
        }
        let mut completed_builds = self
            .state
            .read()
            .await
            .outbound_builds
            .values()
            .filter_map(|build| {
                completed_build_handoff_lsn(&build.progress, authority)
                    .map(|lsn| (build.progress.authority.target.clone(), lsn))
            })
            .collect::<Vec<_>>();
        if let Some(evidence) = authority.scale_up.as_deref()
            && let Some(progress) = self
                .build_progress_store
                .load_build_progress(&evidence.intent().build_id)
                .await?
            && let Some(lsn) = completed_build_handoff_lsn(&progress, authority)
            && !completed_builds
                .iter()
                .any(|(identity, _)| identity == &progress.authority.target)
        {
            completed_builds.push((progress.authority.target.clone(), lsn));
        }
        Ok(completed_builds)
    }

    async fn configure_admitted_authority(
        &self,
        authority: &ManagedReplicaConfiguration,
        progress: i64,
        preserve_write_access: bool,
        completed_builds: Vec<(ReplicaIdentity, i64)>,
    ) -> Result<()> {
        self.replicator.lock().await.admit_prepared_authority(
            authority.clone(),
            progress,
            preserve_write_access,
        );
        if authority.local_role() == ReplicaRole::Primary {
            let mut replicator = self.replicator.lock().await;
            for (identity, progress) in completed_builds {
                replicator.record_prepared_build_handoff_progress(identity, progress);
            }
        }
        if authority.local_role() == ReplicaRole::Primary {
            let primary = self.primary.read().await.clone().ok_or_else(|| {
                RuntimeError::Application("primary role requires IFabricPrimaryReplicator".into())
            })?;
            if let Some(previous) = authority.previous_configuration.clone() {
                primary
                    .update_catch_up_replica_set_configuration(
                        authority.current_configuration.clone().into(),
                        previous.into(),
                    )
                    .await?;
            } else {
                primary
                    .update_current_replica_set_configuration(
                        authority.current_configuration.clone().into(),
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn validate_removal_write_grant(&self) -> Result<()> {
        let state = self.state.read().await;
        if (state.removal_in_progress.is_some()
            || state.prepared_secondary_removal.is_some()
            || state
                .authority
                .as_ref()
                .is_some_and(|a| a.secondary_removal.is_some()))
            && (state
                .authority
                .as_ref()
                .is_none_or(|a| a.previous_configuration.is_some())
                || state
                    .accepted_secondary_removal
                    .as_ref()
                    .is_none_or(|committed| {
                        state
                            .removal_in_progress
                            .as_ref()
                            .is_some_and(|intent| intent != &committed.evidence.preparation.intent)
                            || state.prepared_secondary_removal.as_ref().is_some_and(
                                |preparation| preparation != &committed.evidence.preparation,
                            )
                    })
                || !self.replicator.lock().await.catch_up_complete())
        {
            return Err(RuntimeError::ReconfigurationPending);
        }
        Ok(())
    }

    async fn verify_local_write_progress(&self, lsn: i64) -> Result<()> {
        let mut progress = self
            .state
            .read()
            .await
            .replication_progress
            .clone()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        if lsn == progress.verified_lsn + 1 {
            progress.verified_lsn = lsn;
            self.replication_progress_store
                .record_replication_progress(&progress)
                .await?;
            self.replicator
                .lock()
                .await
                .record_verified_local_progress(lsn);
            self.state.write().await.replication_progress = Some(progress);
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
        authority: &ManagedReplicaConfiguration,
    ) -> Result<ReplicationProgress> {
        let mut progress = self
            .replication_progress_store
            .load_replication_progress(&authority.fence())
            .await?
            .unwrap_or(ReplicationProgress {
                fence: authority.fence(),
                verified_lsn: 0,
            });
        if let Some(evidence) = authority.scale_up.as_deref()
            && evidence.intent().target == self.identity
            && let Some(build) = self
                .build_progress_store
                .load_build_progress(&evidence.intent().build_id)
                .await?
            && build.completed
            && build.authority.source == evidence.intent().primary
            && build.authority.target == evidence.intent().target
            && build.authority.current_configuration == evidence.intent().previous_configuration
            && build.authority.replication_boundary_lsn == evidence.intent().snapshot_boundary_lsn
            && build.catch_up_boundary_lsn == Some(evidence.intent().catch_up_boundary_lsn)
            && build.durable_lsn >= evidence.intent().catch_up_boundary_lsn
        {
            progress.verified_lsn = progress
                .verified_lsn
                .max(evidence.intent().catch_up_boundary_lsn);
        }
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
        if let Some(handoff) = &authority.switchover_handoff {
            let starting_fence = crate::authority::AuthorityFence {
                epoch: handoff.starting_epoch,
                previous_configuration_id: None,
                current_configuration_id: handoff.starting_configuration_id.clone(),
            };
            let stored_progress = self
                .replication_progress_store
                .load_replication_progress(&starting_fence)
                .await?
                .map_or(0, |progress| progress.verified_lsn);
            let configuration_progress = self
                .replication_progress_store
                .load_configuration_progress(
                    handoff.starting_epoch,
                    &handoff.starting_configuration_id,
                )
                .await?
                .map_or(0, |progress| progress.verified_lsn);
            let starting_progress = if self.identity == handoff.source {
                self.state.read().await.current_progress
            } else {
                stored_progress.max(configuration_progress)
            };
            if self.identity == *authority.primary_identity()
                && starting_progress < handoff.handoff_lsn
            {
                return Err(RuntimeError::AuthorityMismatch(
                    "new switchover primary lacks the certified handoff prefix".to_string(),
                ));
            }
            progress.verified_lsn = progress
                .verified_lsn
                .max(starting_progress.min(handoff.handoff_lsn));
        }
        let handoff_lsn = self
            .state
            .read()
            .await
            .builds
            .values()
            .filter(|build| build.authority.target == self.identity)
            .filter_map(|build| completed_build_handoff_lsn(build, authority))
            .max();
        if let Some(handoff_lsn) = handoff_lsn
            && handoff_lsn > progress.verified_lsn
        {
            progress.verified_lsn = handoff_lsn;
        }
        if let Some(evidence) = &authority.secondary_removal
            && self.identity == evidence.preparation.intent.primary
            && (progress.verified_lsn < evidence.preparation.boundary_lsn
                || self.state.read().await.current_progress < evidence.preparation.boundary_lsn)
        {
            return Err(RuntimeError::AuthorityMismatch(
                "reduced primary lacks its verified durable preparation boundary".into(),
            ));
        }
        self.replication_progress_store
            .record_replication_progress(&progress)
            .await?;
        Ok(progress)
    }
}

impl DefaultReplicatorInner {
    async fn validate_managed_configuration_candidate(
        &self,
        authority: &ManagedReplicaConfiguration,
    ) -> Result<()> {
        authority.validate()?;
        if authority.local_identity != self.identity {
            return Err(RuntimeError::AuthorityMismatch(
                "configuration target differs from runtime identity".into(),
            ));
        }
        if self
            .replica_authority_store
            .load_retired_authority()
            .await?
            .is_some()
            || self
                .replica_authority_store
                .load_retirement_started()
                .await?
                .is_some()
        {
            return Err(RuntimeError::Closed);
        }
        let state = self.state.read().await;
        if matches!(
            authority.scale_up.as_deref(),
            Some(crate::protocol::types::ScaleUpConfigurationEvidence::Failover { .. })
        ) && state.authority.is_none()
            && !recovering_configuration()
        {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        if state.authority.as_ref().is_some_and(|existing| {
            authority.current_configuration.epoch < existing.current_configuration.epoch
                || (authority.current_configuration.epoch == existing.current_configuration.epoch
                    && existing != authority
                    && !authority.is_current_only_completion_of(existing))
        }) {
            return Err(RuntimeError::AuthorityMismatch(
                "configuration changed without a newer epoch".into(),
            ));
        }
        if !recovering_configuration()
            && let Some(evidence) = &authority.secondary_removal
        {
            let intent = &evidence.preparation.intent;
            let existing = state
                .authority
                .as_ref()
                .ok_or(RuntimeError::AuthorityNotAdmitted)?;
            if existing.secondary_removal.as_ref().map(|e| &e.preparation)
                != Some(&evidence.preparation)
                && (existing.previous_configuration.is_some()
                    || existing.current_configuration != intent.previous_configuration)
            {
                return Err(RuntimeError::AuthorityMismatch(
                    "reduction does not extend installed current-only configuration".into(),
                ));
            }
            if self.identity == intent.primary
                && state.accepted_secondary_removal.as_ref().is_none_or(|c| {
                    c.evidence != *evidence
                        || authority.previous_configuration.is_some()
                        || authority.current_configuration != intent.current_configuration
                })
                && (state.prepared_secondary_removal.as_ref() != Some(&evidence.preparation)
                    || state.current_progress < evidence.preparation.boundary_lsn
                    || state
                        .replication_progress
                        .as_ref()
                        .is_none_or(|p| p.verified_lsn < evidence.preparation.boundary_lsn))
            {
                return Err(RuntimeError::AuthorityMismatch(
                    "primary lacks exact durable preparation".into(),
                ));
            }
        }
        if !recovering_configuration()
            && let Some(preparation) = &state.prepared_secondary_removal
            && authority.secondary_removal.as_ref().map(|e| &e.preparation) != Some(preparation)
            && state
                .accepted_secondary_removal
                .as_ref()
                .is_none_or(|committed| committed.evidence.preparation != *preparation)
        {
            return Err(RuntimeError::AuthorityMismatch(
                "prepared removal may only roll forward".into(),
            ));
        }
        Ok(())
    }

    async fn install_managed_configuration(
        &self,
        pending: PendingManagedConfiguration,
    ) -> Result<()> {
        let PendingManagedConfiguration {
            configuration: authority,
            replication_progress,
            current_progress,
            previous_epoch,
            prior_access,
            authority_changed,
            preserve_access: preserve_scale_up_access,
            completed_builds,
            ..
        } = pending;
        if authority_changed {
            if !preserve_scale_up_access {
                let mut state = self.state.write().await;
                state.read_status = AccessStatus::ReconfigurationPending;
                state.write_status = AccessStatus::ReconfigurationPending;
            }
            {
                let mut state = self.state.write().await;
                if authority.secondary_removal.is_none()
                    && state.prepared_secondary_removal.as_ref().is_some_and(|p| {
                        authority.current_configuration.epoch > p.intent.current_configuration.epoch
                    })
                {
                    state.prepared_secondary_removal = None;
                    state.removal_in_progress = None;
                }
            }
            self.changed.notify_waiters();
            self.state.write().await.accepted_secondary_removal = None;
        }
        if authority.local_role() != ReplicaRole::Primary {
            self.state.write().await.write_status = AccessStatus::ReconfigurationPending;
            self.replicator.lock().await.fence_client_writes();
        }
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
        self.configure_admitted_authority(
            &authority,
            current_progress,
            preserve_scale_up_access,
            completed_builds,
        )
        .await?;
        self.replicator
            .lock()
            .await
            .record_verified_local_progress(replication_progress.verified_lsn);
        if authority.previous_configuration.is_none()
            && let Some(evidence) = &authority.secondary_removal
        {
            let target = evidence.preparation.intent.target.clone();
            let mut state = self.state.write().await;
            state
                .outbound_builds
                .retain(|_, b| b.progress.authority.target != target);
            state.peer_repair_targets.remove(&target);
        }
        let mut state = self.state.write().await;
        if !authority_changed || preserve_scale_up_access {
            state.read_status = prior_access.0;
            state.write_status = prior_access.1;
        }
        state.authority = Some(authority);
        state.replication_progress = Some(replication_progress);
        Ok(())
    }

    async fn execute_managed_operation<T>(
        &self,
        allow_closed: bool,
        allow_retiring: bool,
        operation: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let _guard = self.effect_lock.lock().await;
        self.check_no_pending_configuration().await?;
        if !allow_closed {
            self.check_aborted()?;
        }
        if !allow_retiring && self.state.read().await.retiring_authority.is_some() {
            return Err(RuntimeError::Closed);
        }
        let result = operation.await?;
        self.changed.notify_waiters();
        Ok(result)
    }

    async fn certified_prefix_outcome(
        &self,
        settled_lsn: i64,
    ) -> Result<crate::replicator::ManagedCertifiedPrefixOutcome> {
        let _effect = self.effect_lock.lock().await;
        let _delivery = self.delivery_lock.lock().await;
        self.check_aborted()?;
        let state = self.state.read().await;
        let verified_lsn = state
            .replication_progress
            .as_ref()
            .map_or(state.current_progress, |progress| progress.verified_lsn);
        let committed_lsn = state.committed_lsn;
        let authority = state.authority.clone();
        drop(state);
        Ok(crate::replicator::ManagedCertifiedPrefixOutcome {
            fence: ManagedOperationFence {
                configuration: authority,
                engine_session_id: self.session_id.clone(),
                engine_generation: self.fence_generation.load(Ordering::Acquire),
            },
            verified_lsn,
            settled_lsn,
            committed_lsn,
        })
    }

    async fn secondary_removal_state(
        &self,
        witness: Option<SecondaryRemovalWitness>,
    ) -> Result<SecondaryRemovalState> {
        let _effect = self.effect_lock.lock().await;
        let _delivery = self.delivery_lock.lock().await;
        self.check_aborted()?;
        let state = self.state.read().await;
        Ok(SecondaryRemovalState {
            fence: ManagedOperationFence {
                configuration: state.authority.clone(),
                engine_session_id: self.session_id.clone(),
                engine_generation: self.fence_generation.load(Ordering::Acquire),
            },
            preparation: state.prepared_secondary_removal.clone(),
            witness,
            accepted: state.accepted_secondary_removal.clone(),
            verified_lsn: state
                .replication_progress
                .as_ref()
                .map(|progress| progress.verified_lsn),
            committed_lsn: state.committed_lsn,
        })
    }

    async fn prepare_switchover_with_token(
        &self,
        preparation_generation: u64,
        request_id: crate::protocol::types::SwitchoverRequestId,
        source: ReplicaIdentity,
        target: ReplicaIdentity,
        starting_configuration_id: crate::protocol::types::ConfigurationId,
        starting_epoch: Epoch,
    ) -> Result<ManagedOperationFence> {
        let _effect = self.effect_lock.lock().await;
        self.check_aborted()?;
        if self.state.read().await.retiring_authority.is_some() {
            return Err(RuntimeError::Closed);
        }
        let state = self.state.read().await;
        let token = ManagedOperationFence {
            configuration: state.authority.clone(),
            engine_session_id: self.session_id.clone(),
            engine_generation: self.fence_generation.load(Ordering::Acquire),
        };
        drop(state);
        self.prepare_switchover_instruction(
            preparation_generation,
            request_id,
            source,
            target,
            starting_configuration_id,
            starting_epoch,
        )
        .await?;
        self.changed.notify_waiters();
        Ok(token)
    }
}

impl DefaultReplicatorInner {
    async fn progress_status_unlocked(&self) -> NativeProgressStatus {
        let state = self.state.read().await;
        let current_progress = state.current_progress;
        let verified_replication_lsn = state
            .replication_progress
            .as_ref()
            .map(|progress| progress.verified_lsn);
        let committed_lsn = state.committed_lsn;
        drop(state);
        let replicator = self.replicator.lock().await;
        NativeProgressStatus {
            current_progress,
            verified_replication_lsn,
            committed_lsn: committed_lsn.max(replicator.committed_lsn()),
            current_configuration_quorum_progress: replicator
                .current_configuration_quorum_progress(),
            catch_up_boundary: replicator.catch_up_boundary(),
            catch_up_complete: replicator.catch_up_complete(),
        }
    }
}

#[async_trait::async_trait]
impl ManagedReplicatorLifecycle for DefaultReplicatorInner {
    async fn complete_open(&self, replication_address: String) -> Result<()> {
        self.complete_open(replication_address).await
    }

    async fn attach_interfaces(
        &self,
        control: Arc<dyn Replicator>,
        primary: Option<Arc<dyn PrimaryReplicator>>,
    ) -> Result<()> {
        self.attach_interfaces(control, primary).await
    }

    async fn fence_writes(&self) -> Result<()> {
        self.check_aborted()?;
        self.state.write().await.write_status = AccessStatus::ReconfigurationPending;
        self.replicator.lock().await.fence_client_writes();
        self.changed.notify_waiters();
        Ok(())
    }

    async fn settle_primary_prefix(
        &self,
    ) -> Result<crate::replicator::ManagedCertifiedPrefixOutcome> {
        let _effect = self.effect_lock.lock().await;
        let _delivery = self.delivery_lock.lock().await;
        self.check_aborted()?;
        let state = self.state.read().await;
        if !state.open
            || state.role != ReplicaRole::Primary
            || state.write_status == AccessStatus::Granted
        {
            return Err(RuntimeError::ReconfigurationPending);
        }
        let authority = state.authority.clone();
        let progress = state.replication_progress.clone();
        // Local reservations still need exact quorum reconciliation. Verification
        // proves their identity, not that this process completed their quorum wait.
        let unresolved_local = state
            .local_writes
            .values()
            .filter(|write| write.phase != LocalWritePhase::Committed)
            .map(|write| write.lsn)
            .min();
        drop(state);
        let storage = self.storage().await?;
        let durable = storage.durable_progress().await?;
        let verified_lsn = match (authority.as_ref(), progress.as_ref()) {
            (Some(authority), Some(progress)) => {
                if authority.primary_identity() != &self.identity
                    || progress.fence != authority.fence()
                    || self
                        .replica_authority_store
                        .load_configuration()
                        .await?
                        .as_ref()
                        != Some(authority)
                    || self
                        .replication_progress_store
                        .load_replication_progress(&authority.fence())
                        .await?
                        .as_ref()
                        != Some(progress)
                {
                    return Err(RuntimeError::AuthorityMismatch(
                        "primary activation lacks durable authority-fenced progress".into(),
                    ));
                }
                progress.verified_lsn
            }
            // A fresh bootstrap may open before admission, but certifies no data.
            (None, None) if durable.applied_lsn == 0 && durable.committed_lsn == 0 => 0,
            _ => return Err(RuntimeError::AuthorityNotAdmitted),
        };
        if verified_lsn < 0 || verified_lsn > durable.applied_lsn {
            return Err(RuntimeError::AuthorityMismatch(
                "primary prefix exceeds durable application data".into(),
            ));
        }
        // Never regress previously durable commitment. Only new advancement is
        // authorized here; an inbound-only certified prefix needs no local journal.
        let settled_lsn = unresolved_local
            .map_or(verified_lsn, |lsn| verified_lsn.min(lsn - 1))
            .max(durable.committed_lsn);
        let committed = storage.commit(settled_lsn).await?;
        if committed.committed_lsn != settled_lsn || committed.applied_lsn != durable.applied_lsn {
            return Err(RuntimeError::Application(
                "failed to settle certified primary prefix".into(),
            ));
        }
        self.replicator
            .lock()
            .await
            .restore_committed_prefix(settled_lsn);
        self.state.write().await.committed_lsn = committed.committed_lsn;
        Ok(crate::replicator::ManagedCertifiedPrefixOutcome {
            fence: ManagedOperationFence {
                configuration: authority,
                engine_session_id: self.session_id.clone(),
                engine_generation: self.fence_generation.load(Ordering::Acquire),
            },
            verified_lsn: verified_lsn.max(settled_lsn),
            settled_lsn,
            committed_lsn: committed.committed_lsn,
        })
    }

    async fn cancel_configuration_work(&self) -> Result<()> {
        self.check_aborted()?;
        let _effect = self.effect_lock.lock().await;
        let _delivery = self.delivery_lock.lock().await;
        self.fence_generation.fetch_add(1, Ordering::AcqRel);
        if let Some(pending) = self.state.write().await.pending_configuration.take() {
            pending.active.store(false, Ordering::Release);
        }
        self.replicator.lock().await.fence_client_writes();
        self.changed.notify_waiters();
        Ok(())
    }

    async fn prepare_access(
        &self,
        read: AccessStatus,
        write: AccessStatus,
    ) -> Result<ManagedAccessPreparation> {
        self.check_aborted()?;
        self.check_no_pending_configuration().await?;
        let generation = self.fence_generation.load(Ordering::Acquire);
        if write == AccessStatus::Granted {
            self.validate_removal_write_grant().await?;
        }
        if write == AccessStatus::Granted
            && self.state.read().await.write_status != AccessStatus::Granted
        {
            self.recover_pending_local_writes().await?;
        }
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
        if write == AccessStatus::Granted {
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
                    "write grant target is not the admitted primary".into(),
                ));
            }
        }
        let primary_read = read == AccessStatus::Granted && state.role == ReplicaRole::Primary;
        drop(state);
        if primary_read && !self.replicator.lock().await.catch_up_complete() {
            return Err(RuntimeError::ReconfigurationPending);
        }
        if generation != self.fence_generation.load(Ordering::Acquire) {
            return Err(RuntimeError::OperationCancelled);
        }
        let state = self.state.read().await;
        Ok(ManagedAccessPreparation {
            fence: ManagedOperationFence {
                configuration: state.authority.clone(),
                engine_session_id: self.session_id.clone(),
                engine_generation: generation,
            },
            read,
            write,
            current_progress: state.current_progress,
            committed_lsn: state.committed_lsn,
        })
    }

    async fn publish_access(&self, preparation: ManagedAccessPreparation) -> Result<()> {
        let _effect = self.effect_lock.lock().await;
        let _delivery = self.delivery_lock.lock().await;
        self.check_aborted()?;
        if preparation.fence.engine_session_id != self.session_id
            || preparation.fence.engine_generation != self.fence_generation.load(Ordering::Acquire)
        {
            return Err(RuntimeError::OperationCancelled);
        }
        let mut state = self.state.write().await;
        if preparation.fence.configuration != state.authority
            || ((preparation.read == AccessStatus::Granted
                || preparation.write == AccessStatus::Granted)
                && (preparation.current_progress != state.current_progress
                    || preparation.committed_lsn != state.committed_lsn))
        {
            return Err(RuntimeError::OperationCancelled);
        }
        if preparation.write != AccessStatus::Granted {
            self.replicator.lock().await.fence_client_writes();
        }
        state.read_status = preparation.read;
        state.write_status = preparation.write;
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    async fn lock_native_fence(
        &self,
        expected: &ManagedOperationFence,
    ) -> Result<ManagedFenceGuard> {
        let _effect = self.effect_lock.lock().await;
        let delivery = self.delivery_lock.clone().lock_owned().await;
        self.check_aborted()?;
        self.check_no_pending_configuration().await?;
        if expected.engine_session_id != self.session_id
            || expected.engine_generation != self.fence_generation.load(Ordering::Acquire)
            || {
                let state = self.state.read().await;
                expected.configuration != state.authority
            }
        {
            return Err(RuntimeError::OperationCancelled);
        }
        let progress = self.progress_status_unlocked().await;
        Ok(ManagedFenceGuard::new(delivery, progress))
    }

    async fn native_fence(&self) -> Result<ManagedOperationFence> {
        let _effect = self.effect_lock.lock().await;
        let _delivery = self.delivery_lock.lock().await;
        self.check_aborted()?;
        self.check_no_pending_configuration().await?;
        let state = self.state.read().await;
        Ok(ManagedOperationFence {
            configuration: state.authority.clone(),
            engine_session_id: self.session_id.clone(),
            engine_generation: self.fence_generation.load(Ordering::Acquire),
        })
    }

    async fn current_engine_fence(&self) -> ManagedOperationFence {
        let _effect = self.effect_lock.lock().await;
        let _delivery = self.delivery_lock.lock().await;
        let state = self.state.read().await;
        ManagedOperationFence {
            configuration: state.authority.clone(),
            engine_session_id: self.session_id.clone(),
            engine_generation: self.fence_generation.load(Ordering::Acquire),
        }
    }

    async fn progress_status(&self) -> NativeProgressStatus {
        let _effect = self.effect_lock.lock().await;
        self.progress_status_unlocked().await
    }

    async fn topology_status(&self) -> NativeTopologyStatus {
        let state = self.state.read().await;
        NativeTopologyStatus {
            prepared_secondary_removal: state.prepared_secondary_removal.clone(),
            accepted_secondary_removal: state.accepted_secondary_removal.clone(),
            retired_authority: state.retired_authority.clone(),
        }
    }

    async fn prepare_replica_configuration(
        &self,
        configuration: ManagedReplicaConfiguration,
        host_generation: u64,
    ) -> Result<ManagedConfigurationPreparation> {
        let _effect = self.effect_lock.lock().await;
        let _delivery = self.delivery_lock.lock().await;
        self.check_aborted()?;
        if self.state.read().await.retiring_authority.is_some() {
            return Err(RuntimeError::Closed);
        }
        self.validate_managed_configuration_candidate(&configuration)
            .await?;
        if self
            .state
            .read()
            .await
            .pending_configuration
            .as_ref()
            .is_some_and(|pending| pending.active.load(Ordering::Acquire))
        {
            return Err(RuntimeError::ReconfigurationPending);
        }
        let (existing, current_progress, previous_epoch, prior_access) = {
            let state = self.state.read().await;
            (
                state.authority.clone(),
                state.current_progress,
                state
                    .authority
                    .as_ref()
                    .map(|authority| authority.current_configuration.epoch),
                (state.read_status, state.write_status),
            )
        };
        let preserve_access = existing.as_ref().is_some_and(|current| {
            current == &configuration
                || preserves_same_primary_scale_up_access(current, &configuration)
        });
        let authority_changed = existing.as_ref() != Some(&configuration);
        let changed = authority_changed
            || self.state.read().await.configuration_generation != host_generation;
        if configuration.current_configuration.epoch < self.replicator.lock().await.epoch() {
            return Err(RuntimeError::AuthorityMismatch(
                "replicator epoch cannot regress".into(),
            ));
        }
        let replication_progress = self
            .load_replication_progress_with_handoff(&configuration)
            .await?;
        let completed_builds = self.load_completed_build_handoffs(&configuration).await?;
        if changed {
            self.fence_generation.fetch_add(1, Ordering::AcqRel);
            if !preserve_access {
                let mut state = self.state.write().await;
                state.read_status = AccessStatus::ReconfigurationPending;
                state.write_status = AccessStatus::ReconfigurationPending;
                drop(state);
                self.replicator.lock().await.fence_client_writes();
            }
        }
        let active = Arc::new(AtomicBool::new(true));
        let preparation = ManagedConfigurationPreparation {
            fence: ManagedOperationFence {
                configuration: Some(configuration.clone()),
                engine_session_id: self.session_id.clone(),
                engine_generation: self.fence_generation.load(Ordering::Acquire),
            },
            configuration: configuration.clone(),
            host_generation,
            active: active.clone(),
            native_generation: self.fence_generation.clone(),
        };
        self.state.write().await.pending_configuration = Some(PendingManagedConfiguration {
            configuration,
            engine_generation: preparation.fence.engine_generation,
            host_generation,
            active,
            replication_progress,
            current_progress,
            previous_epoch,
            prior_access,
            authority_changed,
            preserve_access,
            completed_builds,
        });
        self.changed.notify_waiters();
        Ok(preparation)
    }

    async fn commit_replica_configuration(
        &self,
        preparation: ManagedConfigurationPreparation,
    ) -> Result<()> {
        let recover_scale_up_writes = matches!(
            preparation.configuration.scale_up.as_deref(),
            Some(crate::protocol::types::ScaleUpConfigurationEvidence::Admission { .. })
        );
        let _effect = self.effect_lock.lock().await;
        self.check_aborted()?;
        let pending = self.state.read().await.pending_configuration.clone();
        let pending_matches = pending.as_ref().is_some_and(|pending| {
            pending.active.load(Ordering::Acquire)
                && pending.configuration == preparation.configuration
                && pending.engine_generation == preparation.fence.engine_generation
                && pending.host_generation == preparation.host_generation
        });
        if !preparation.active.load(Ordering::Acquire)
            || preparation.fence.engine_session_id != self.session_id
            || preparation.fence.engine_generation != self.fence_generation.load(Ordering::Acquire)
            || preparation.fence.configuration.as_ref() != Some(&preparation.configuration)
            || !pending_matches
        {
            return Err(RuntimeError::OperationCancelled);
        }
        let pending = pending.expect("matching pending configuration exists");
        self.install_managed_configuration(pending).await?;
        {
            let mut state = self.state.write().await;
            state.configuration_generation = preparation.host_generation;
            state.pending_configuration = None;
        }
        preparation.active.store(false, Ordering::Release);
        drop(_effect);
        if recover_scale_up_writes
            && !recovering_configuration()
            && !self.state.read().await.local_writes.is_empty()
        {
            self.recover_pending_local_writes().await?;
        }
        self.changed.notify_waiters();
        Ok(())
    }

    async fn synchronize_replica_configuration(&self, host_generation: u64) -> Result<()> {
        let _effect = self.effect_lock.lock().await;
        let _delivery = self.delivery_lock.lock().await;
        self.check_aborted()?;
        let mut state = self.state.write().await;
        if state.authority.is_none() || state.pending_configuration.is_some() {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        if state.configuration_generation != host_generation {
            self.fence_generation.fetch_add(1, Ordering::AcqRel);
            state.configuration_generation = host_generation;
            self.changed.notify_waiters();
        }
        Ok(())
    }

    async fn authorize_failover_prefix(
        &self,
        boundary: Lsn,
    ) -> Result<crate::replicator::ManagedCertifiedPrefixOutcome> {
        self.execute_managed_operation(
            false,
            false,
            self.authorize_failover_prefix_instruction(boundary),
        )
        .await?;
        self.certified_prefix_outcome(boundary).await
    }

    async fn prepare_switchover(
        &self,
        preparation_generation: u64,
        request_id: crate::protocol::types::SwitchoverRequestId,
        source: ReplicaIdentity,
        target: ReplicaIdentity,
        starting_configuration_id: crate::protocol::types::ConfigurationId,
        starting_epoch: Epoch,
    ) -> Result<crate::replicator::ManagedSwitchoverOutcome> {
        let fence = self
            .prepare_switchover_with_token(
                preparation_generation,
                request_id.clone(),
                source.clone(),
                target.clone(),
                starting_configuration_id.clone(),
                starting_epoch,
            )
            .await?;
        let state = self.state.read().await;
        let handoff_lsn = state
            .replication_progress
            .as_ref()
            .map_or(state.current_progress, |progress| progress.verified_lsn)
            .min(state.current_progress)
            .max(state.committed_lsn);
        Ok(crate::replicator::ManagedSwitchoverOutcome {
            fence,
            preparation_generation,
            request_id,
            source,
            target,
            starting_configuration_id,
            starting_epoch,
            handoff_lsn,
            committed_lsn: state.committed_lsn,
        })
    }

    async fn prepare_secondary_removal(
        &self,
        intent: crate::protocol::types::SecondaryScaleDownIntent,
        process_session_id: ProcessSessionId,
        report_sequence: u64,
    ) -> Result<crate::replicator::ManagedRemovalPreparationOutcome> {
        self.execute_managed_operation(
            false,
            false,
            self.prepare_secondary_removal_instruction(intent, process_session_id, report_sequence),
        )
        .await?;
        let state = self.secondary_removal_state(None).await?;
        Ok(crate::replicator::ManagedRemovalPreparationOutcome {
            fence: state.fence,
            preparation: state.preparation.ok_or(RuntimeError::OperationCancelled)?,
            verified_lsn: state.verified_lsn,
            committed_lsn: state.committed_lsn,
        })
    }

    async fn observe_secondary_removal_witness(
        &self,
        witness: SecondaryRemovalWitness,
    ) -> Result<crate::replicator::ManagedRemovalWitnessOutcome> {
        self.execute_managed_operation(
            false,
            false,
            self.observe_secondary_removal_witness_instruction(witness.clone()),
        )
        .await?;
        let state = self.secondary_removal_state(Some(witness)).await?;
        Ok(crate::replicator::ManagedRemovalWitnessOutcome {
            fence: state.fence,
            witness: state.witness.ok_or(RuntimeError::OperationCancelled)?,
            verified_lsn: state.verified_lsn,
            committed_lsn: state.committed_lsn,
        })
    }

    async fn observe_secondary_removal_progress(
        &self,
        witness: SecondaryRemovalWitness,
        committed: SecondaryScaleDownCleanup,
    ) -> Result<crate::replicator::ManagedRemovalProgressOutcome> {
        self.execute_managed_operation(
            false,
            false,
            self.observe_secondary_removal_progress_instruction(witness.clone(), committed.clone()),
        )
        .await?;
        let state = self.secondary_removal_state(Some(witness)).await?;
        Ok(crate::replicator::ManagedRemovalProgressOutcome {
            fence: state.fence,
            witness: state.witness.ok_or(RuntimeError::OperationCancelled)?,
            accepted: state.accepted.unwrap_or(committed),
            verified_lsn: state.verified_lsn,
            committed_lsn: state.committed_lsn,
        })
    }

    async fn accept_secondary_removal_commit(
        &self,
        committed: SecondaryScaleDownCleanup,
    ) -> Result<crate::replicator::ManagedRemovalAcceptanceOutcome> {
        self.execute_managed_operation(
            false,
            false,
            self.accept_secondary_removal_commit_instruction(committed.clone()),
        )
        .await?;
        let state = self.secondary_removal_state(None).await?;
        Ok(crate::replicator::ManagedRemovalAcceptanceOutcome {
            fence: state.fence,
            accepted: state.accepted.unwrap_or(committed),
            verified_lsn: state.verified_lsn,
            committed_lsn: state.committed_lsn,
        })
    }

    async fn accept_historical_secondary_removal_commit(
        &self,
        command: crate::protocol::command::AcceptSecondaryRemovalCommit,
    ) -> Result<crate::replicator::ManagedHistoricalRemovalAcceptanceOutcome> {
        let committed = command.committed.clone();
        self.execute_managed_operation(
            false,
            false,
            self.accept_historical_secondary_removal_commit_instruction(command),
        )
        .await?;
        let state = self.secondary_removal_state(None).await?;
        Ok(
            crate::replicator::ManagedHistoricalRemovalAcceptanceOutcome {
                fence: state.fence,
                accepted: state.accepted.unwrap_or(committed),
                verified_lsn: state.verified_lsn,
                committed_lsn: state.committed_lsn,
            },
        )
    }

    async fn fence_retirement(
        &self,
        retired: RetiredAuthority,
    ) -> Result<crate::replicator::ManagedRetirementFenceOutcome> {
        self.execute_managed_operation(
            false,
            true,
            self.fence_retirement_instruction(retired.clone()),
        )
        .await?;
        Ok(crate::replicator::ManagedRetirementFenceOutcome {
            fence: self.current_engine_fence().await,
            retired,
        })
    }

    async fn complete_retirement(
        &self,
        retired: RetiredAuthority,
    ) -> Result<crate::replicator::ManagedRetirementCompletionOutcome> {
        self.execute_managed_operation(
            true,
            true,
            self.complete_retirement_instruction(retired.clone()),
        )
        .await?;
        Ok(crate::replicator::ManagedRetirementCompletionOutcome {
            fence: self.current_engine_fence().await,
            retired,
        })
    }

    async fn register_peer_session_proof(
        &self,
        identity: ReplicaIdentity,
        session: ProcessSessionId,
    ) -> Result<()> {
        self.execute_managed_operation(
            false,
            false,
            self.register_peer_session_instruction(identity, session),
        )
        .await
    }

    async fn admit_build_authority_proof(&self, authority: BuildAuthority) -> Result<()> {
        self.execute_managed_operation(
            false,
            false,
            self.admit_build_authority_instruction(authority),
        )
        .await
    }

    async fn retire_build_proof(&self, build_id: OperationId) -> Result<()> {
        self.execute_managed_operation(false, false, self.retire_build_instruction(build_id))
            .await
    }

    async fn refresh_progress_proof(&self) -> Result<()> {
        self.execute_managed_operation(
            false,
            false,
            self.refresh_application_progress_instruction(),
        )
        .await
    }

    async fn restore_engine_proof(
        &self,
        configuration: Option<ManagedReplicaConfiguration>,
        host_generation: u64,
    ) -> Result<()> {
        self.restore_authority(configuration, host_generation).await
    }

    async fn observe_engine(&self) -> crate::replicator::ManagedReplicaObservation {
        let progress = self.progress_status().await;
        let catch_up_capability = {
            let current_progress = self.state.read().await.current_progress;
            self.replicator
                .lock()
                .await
                .catch_up_capability(current_progress)
        };
        let state = self.state.read().await;
        crate::replicator::ManagedReplicaObservation {
            fence: ManagedOperationFence {
                configuration: state.authority.clone(),
                engine_session_id: self.session_id.clone(),
                engine_generation: self.fence_generation.load(Ordering::Acquire),
            },
            host_generation: state.configuration_generation,
            diagnostic_revision: self.fence_generation.load(Ordering::Acquire),
            progress,
            catch_up_capability,
            builds: build_postconditions(&state),
            prepared_secondary_removal: state.prepared_secondary_removal.clone(),
            accepted_secondary_removal: state.accepted_secondary_removal.clone(),
            retired_authority: state.retired_authority.clone(),
        }
    }

    async fn cancel_outbound_build(&self, build_id: &OperationId) -> Result<()> {
        let _effect = self.effect_lock.lock().await;
        let _delivery = self.delivery_lock.lock().await;
        let mut state = self.state.write().await;
        state.outbound_builds.remove(build_id);
        state.cancelled_outbound_builds.insert(build_id.clone());
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    async fn detach_outbound_build_stream(&self, build_id: &OperationId) -> Result<()> {
        let _effect = self.effect_lock.lock().await;
        let mut state = self.state.write().await;
        let build = state
            .outbound_builds
            .get_mut(build_id)
            .ok_or(RuntimeError::OperationCancelled)?;
        if !build.progress.completed {
            return Err(RuntimeError::ReconfigurationPending);
        }
        build.stream_tx = None;
        Ok(())
    }

    fn abort(&self) {
        self.control_abort();
    }
}

#[async_trait::async_trait]
impl ManagedReplicatorDataPlane for DefaultReplicatorInner {
    async fn next_outbound_item(&self) -> Option<OutboundOperation> {
        let mut receiver = self.outbound_rx.lock().await;
        while let Some(item) = receiver.recv().await {
            if matches!(
                item,
                OutboundOperation::Replication(_) | OutboundOperation::Copy(_)
            ) {
                return Some(item);
            }
        }
        None
    }

    async fn repair_peer(&self, identity: ReplicaIdentity, progress: Lsn) -> Result<()> {
        self.repair_peer_from_history(identity, progress).await
    }

    #[cfg(all(test, kuberic_workspace_tests))]
    async fn begin_write(&self, write: ClientWrite) -> Result<PendingWrite> {
        self.begin_write(write).await
    }

    async fn observe_acknowledgement(
        &self,
        acknowledgement: ReplicationAck,
        session: ProcessSessionId,
    ) -> Result<()> {
        self.execute_managed_operation(
            false,
            false,
            self.observe_replication_ack_instruction(acknowledgement, session),
        )
        .await
    }

    async fn accept_acknowledgement(&self, acknowledgement: ReplicationAck) -> Result<()> {
        self.accept_acknowledgement(acknowledgement).await
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
        catch_up_boundary_lsn: None,
        final_item: false,
        data,
        snapshot_chunk: true,
    }
}

fn copy_final_item(
    authority: &BuildAuthority,
    sequence: u64,
    catch_up_boundary_lsn: i64,
) -> CopyItem {
    CopyItem {
        build_id: authority.build_id.clone(),
        sender: authority.source.clone(),
        receiver: authority.target.clone(),
        epoch: authority.current_configuration.epoch,
        current_configuration_id: authority.current_configuration.configuration_id.clone(),
        sequence,
        lsn: authority.replication_boundary_lsn,
        committed_lsn: authority.replication_boundary_lsn,
        replication_boundary_lsn: authority.replication_boundary_lsn,
        catch_up_boundary_lsn: Some(catch_up_boundary_lsn),
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
        catch_up_boundary_lsn: None,
        final_item: false,
        data: operation.data.clone(),
        snapshot_chunk: false,
    }
}

fn build_handoff_matches(build: &BuildAuthority, authority: &ManagedReplicaConfiguration) -> bool {
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
            authority.previous_configuration.is_none()
                && authority.current_configuration.configuration_id
                    == build.current_configuration.configuration_id
        }
        BuildAuthorityKind::Provisioning => {
            authority.build_kind == BuildAuthorityKind::Provisioning
                && (authority
                    .previous_configuration
                    .as_ref()
                    .is_some_and(|previous| {
                        previous.configuration_id == build.current_configuration.configuration_id
                    })
                    || (authority.previous_configuration.is_none()
                        && authority.scale_up.as_deref().is_some_and(|evidence| {
                            evidence.intent().previous_configuration == build.current_configuration
                                && evidence.intent().current_configuration
                                    == authority.current_configuration
                        })))
                && authority.scale_up.as_deref().is_none_or(|evidence| {
                    let intent = evidence.intent();
                    intent.build_id == build.build_id
                        && intent.primary == build.source
                        && intent.target == build.target
                        && intent.snapshot_boundary_lsn == build.replication_boundary_lsn
                        && intent.previous_configuration == build.current_configuration
                })
        }

        BuildAuthorityKind::Failover => {
            authority.build_kind == BuildAuthorityKind::Failover
                && authority.previous_configuration.is_some()
                && authority.current_configuration.configuration_id
                    == build.current_configuration.configuration_id
        }
    }
}

fn completed_build_handoff_lsn(
    progress: &BuildProgress,
    authority: &ManagedReplicaConfiguration,
) -> Option<i64> {
    if !progress.completed || !build_handoff_matches(&progress.authority, authority) {
        return None;
    }
    let required = authority.scale_up.as_deref().map_or(
        progress
            .catch_up_boundary_lsn
            .unwrap_or(progress.authority.replication_boundary_lsn),
        |evidence| evidence.intent().catch_up_boundary_lsn,
    );
    let boundary_matches = if authority.scale_up.is_some() {
        progress.catch_up_boundary_lsn == Some(required)
    } else {
        progress
            .catch_up_boundary_lsn
            .is_none_or(|boundary| boundary == required)
    };
    (boundary_matches && progress.durable_lsn >= required).then_some(progress.durable_lsn)
}

fn preserves_same_primary_scale_up_access(
    existing: &ManagedReplicaConfiguration,
    next: &ManagedReplicaConfiguration,
) -> bool {
    matches!(
        next.scale_up.as_deref(),
        Some(crate::protocol::types::ScaleUpConfigurationEvidence::Admission { .. })
    ) && existing.primary_identity() == next.primary_identity()
        && next.local_identity == *next.primary_identity()
        && existing.local_identity == next.local_identity
}

fn build_postcondition(value: BuildProgress) -> BuildPostcondition {
    BuildPostcondition {
        authority: value.authority,
        last_sequence: value.last_sequence,
        durable_lsn: value.durable_lsn,
        completed: value.completed,
        catch_up_boundary_lsn: value.catch_up_boundary_lsn,
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
