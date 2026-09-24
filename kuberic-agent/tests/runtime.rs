use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream};
use kuberic_agent::hosting::{OutboundReplication, PodRuntime, PreparedCopy, RuntimeControlPlane};
use kuberic_protocol::types::{
    AccessStatus, AgentGeneration, ConfigurationDescriptor, ConfigurationMember, Epoch, FaultType,
    LoadMetric, OperationId, ReplicaId, ReplicaIdentity, ReplicaInstanceId, ReplicaRole,
    TransitionKind,
};
use kuberic_runtime::application::{
    ClientWrite, CopyChunk, DurableApplicationAck, DurableApplicationProgress, OpenContext,
    OpenMode, Operation, OperationDataStream, RoleChange, StateProvider, StatefulServiceReplica,
};
use kuberic_runtime::engine::{DurableState, RetainedOperationStream};
use kuberic_runtime::replicator::copy::{BuildConfiguration, PrepareCopyRequest};
use kuberic_runtime::replicator::stream::{OperationMetadata, OperationStream};
use kuberic_runtime::replicator::{
    DefaultReplicatorFactory, PrimaryReplicator, ReplicaInformation, ReplicaSetQuorumMode,
    Replicator, ReplicatorFactory, ReplicatorFactoryContext, ReplicatorInterfaces,
    ReplicatorSettings, StateReplicator, StatefulServicePartition,
};
use kuberic_runtime::{Result, RuntimeError};
use kuberic_runtime_internal::authority::{
    AdmittedAuthority, AuthorityFence, BuildAuthority, BuildAuthorityKind, BuildAuthorityStore,
    BuildProgressStore, DurableBuildProgress, DurableLocalWrite, LocalWriteJournal,
    ReplicaAuthorityStore, ReplicationProgress, ReplicationProgressStore,
};
use kuberic_runtime_internal::effects::{RuntimeEffect, RuntimeEffectAction, RuntimeEffectResult};
use kuberic_runtime_internal::{ContractError, Result as ContractResult};
use kuberic_wire::proto;
use tokio::sync::Notify;
use tokio::time::{Duration, timeout};

#[derive(Default)]
struct MemoryAuthorityStore {
    authority: Mutex<Option<AdmittedAuthority>>,
    replication_progress: Mutex<BTreeMap<AuthorityFence, ReplicationProgress>>,
    local_writes: Mutex<BTreeMap<OperationId, DurableLocalWrite>>,
    builds: Mutex<BTreeMap<OperationId, BuildAuthority>>,
    build_progress: Mutex<BTreeMap<OperationId, DurableBuildProgress>>,
    admit_count: AtomicUsize,
    fail_after_admit: AtomicBool,
    fail_build_progress_once: AtomicBool,
    fail_registered_write_once: AtomicBool,
    pause_registered_write: AtomicBool,
    registered_write_notify: Notify,
    resume_registered_write_notify: Notify,
    pause_committed_write: AtomicBool,
    committed_write_notify: Notify,
    resume_committed_write_notify: Notify,
}

#[async_trait]
impl ReplicaAuthorityStore for MemoryAuthorityStore {
    async fn load(&self) -> ContractResult<Option<AdmittedAuthority>> {
        Ok(self.authority.lock().unwrap().clone())
    }

    async fn admit(&self, authority: &AdmittedAuthority) -> ContractResult<()> {
        self.admit_count.fetch_add(1, Ordering::SeqCst);
        *self.authority.lock().unwrap() = Some(authority.clone());
        if self.fail_after_admit.swap(false, Ordering::SeqCst) {
            return Err(ContractError::Persistence(
                "injected ambiguous authority admission".into(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl ReplicationProgressStore for MemoryAuthorityStore {
    async fn load_replication_progress(
        &self,
        fence: &AuthorityFence,
    ) -> ContractResult<Option<ReplicationProgress>> {
        Ok(self
            .replication_progress
            .lock()
            .unwrap()
            .get(fence)
            .cloned())
    }

    async fn load_configuration_progress(
        &self,
        epoch: Epoch,
        current_configuration_id: &kuberic_protocol::types::ConfigurationId,
    ) -> ContractResult<Option<ReplicationProgress>> {
        Ok(self
            .replication_progress
            .lock()
            .unwrap()
            .values()
            .filter(|progress| {
                progress.fence.epoch == epoch
                    && &progress.fence.current_configuration_id == current_configuration_id
            })
            .max_by_key(|progress| progress.verified_lsn)
            .cloned())
    }

    async fn record_replication_progress(
        &self,
        progress: &ReplicationProgress,
    ) -> ContractResult<()> {
        self.replication_progress
            .lock()
            .unwrap()
            .insert(progress.fence.clone(), progress.clone());
        Ok(())
    }
}

#[async_trait]
impl LocalWriteJournal for MemoryAuthorityStore {
    async fn load_local_write(
        &self,
        operation_id: &OperationId,
    ) -> ContractResult<Option<DurableLocalWrite>> {
        Ok(self.local_writes.lock().unwrap().get(operation_id).cloned())
    }

    async fn load_local_writes(&self) -> ContractResult<Vec<DurableLocalWrite>> {
        Ok(self
            .local_writes
            .lock()
            .unwrap()
            .values()
            .filter(|write| {
                write.phase != kuberic_runtime_internal::authority::LocalWritePhase::Committed
            })
            .cloned()
            .collect())
    }

    async fn record_local_write(&self, write: &DurableLocalWrite) -> ContractResult<()> {
        if write.phase == kuberic_runtime_internal::authority::LocalWritePhase::Registered
            && self
                .fail_registered_write_once
                .swap(false, Ordering::SeqCst)
        {
            return Err(ContractError::Persistence(
                "injected registered-write persistence failure".to_string(),
            ));
        }
        self.local_writes
            .lock()
            .unwrap()
            .insert(write.operation_id.clone(), write.clone());
        if write.phase == kuberic_runtime_internal::authority::LocalWritePhase::Registered
            && self.pause_registered_write.load(Ordering::SeqCst)
        {
            self.registered_write_notify.notify_one();
            self.resume_registered_write_notify.notified().await;
        }
        if write.phase == kuberic_runtime_internal::authority::LocalWritePhase::Committed
            && self.pause_committed_write.load(Ordering::SeqCst)
        {
            self.committed_write_notify.notify_one();
            self.resume_committed_write_notify.notified().await;
        }
        Ok(())
    }

    async fn reset_local_writes_after_data_loss(&self, committed_lsn: i64) -> ContractResult<()> {
        self.local_writes.lock().unwrap().retain(|_, write| {
            write.phase == kuberic_runtime_internal::authority::LocalWritePhase::Committed
                && write.lsn <= committed_lsn
        });
        Ok(())
    }
}

#[async_trait]
impl BuildAuthorityStore for MemoryAuthorityStore {
    async fn load_build(&self, build_id: &OperationId) -> ContractResult<Option<BuildAuthority>> {
        Ok(self.builds.lock().unwrap().get(build_id).cloned())
    }

    async fn admit_build(&self, authority: &BuildAuthority) -> ContractResult<()> {
        let mut builds = self.builds.lock().unwrap();
        if let Some(existing) = builds.get(&authority.build_id)
            && existing != authority
        {
            return Err(ContractError::AuthorityMismatch(
                "test store rejected conflicting build authority".to_string(),
            ));
        }
        builds.insert(authority.build_id.clone(), authority.clone());
        Ok(())
    }
}

#[async_trait]
impl BuildProgressStore for MemoryAuthorityStore {
    async fn load_build_progress(
        &self,
        build_id: &OperationId,
    ) -> ContractResult<Option<DurableBuildProgress>> {
        Ok(self.build_progress.lock().unwrap().get(build_id).cloned())
    }

    async fn record_build_progress(&self, progress: &DurableBuildProgress) -> ContractResult<()> {
        if self.fail_build_progress_once.swap(false, Ordering::SeqCst) {
            return Err(ContractError::Persistence(
                "injected build progress failure".to_string(),
            ));
        }
        self.build_progress
            .lock()
            .unwrap()
            .insert(progress.authority.build_id.clone(), progress.clone());
        Ok(())
    }
}

#[derive(Default)]
struct TestApplication {
    partition: Mutex<Option<StatefulServicePartition>>,
    factory: Mutex<Option<Arc<dyn ReplicatorFactory>>>,
    state_replicator: Mutex<Option<Arc<dyn StateReplicator>>>,
    returned_control: Mutex<Option<Arc<dyn Replicator>>>,
    streams_taken: AtomicUsize,
    manual_streams: AtomicBool,
    held_streams: Mutex<Vec<OperationStream>>,
    settings: Mutex<Option<ReplicatorSettings>>,
    pause_open: AtomicBool,
    fail_open: AtomicBool,
    open_notify: Notify,
    resume_open_notify: Notify,
    applied: Mutex<BTreeMap<i64, Operation>>,
    progress: Mutex<DurableApplicationProgress>,
    copy_chunks: Mutex<BTreeMap<(String, u64), Bytes>>,
    copy_context_items: Mutex<Vec<Bytes>>,
    copy_apply_count: AtomicUsize,
    pause_copy_enumeration: Arc<AtomicBool>,
    copy_enumeration_notify: Arc<Notify>,
    resume_copy_enumeration_notify: Arc<Notify>,
    pause_retained_enumeration: Arc<AtomicBool>,
    retained_enumeration_notify: Arc<Notify>,
    resume_retained_enumeration_notify: Arc<Notify>,
    fail_apply: AtomicBool,
    fail_after_apply: AtomicBool,
    pause_after_apply: AtomicBool,
    applied_notify: Notify,
    resume_notify: Notify,
    fail_commit: AtomicBool,
    pause_commit: AtomicBool,
    commit_notify: Notify,
    resume_commit_notify: Notify,
    events: Arc<Mutex<Vec<String>>>,
    pause_close: AtomicBool,
    fail_close: AtomicBool,
    fail_change_role: AtomicBool,
    close_notify: Notify,
    resume_close_notify: Notify,
    fail_update_epoch: AtomicBool,
    pause_update_epoch: AtomicBool,
    update_epoch_notify: Notify,
    resume_update_epoch_notify: Notify,
}

impl TestApplication {
    fn seed_progress(&self, applied_lsn: i64) {
        let mut progress = self.progress.lock().unwrap();
        let mut operations = self.applied.lock().unwrap();
        for lsn in (progress.applied_lsn + 1)..=applied_lsn {
            operations.insert(
                lsn,
                Operation {
                    lsn,
                    committed_lsn: lsn,
                    data: Bytes::from(format!("seed-{lsn}")),
                },
            );
        }
        progress.applied_lsn = applied_lsn;
        progress.committed_lsn = progress.committed_lsn.max(applied_lsn);
    }

    fn seed_operation(&self, lsn: i64, data: Bytes) {
        let mut progress = self.progress.lock().unwrap();
        let mut operations = self.applied.lock().unwrap();
        assert_eq!(lsn, progress.applied_lsn + 1);
        operations.insert(
            lsn,
            Operation {
                lsn,
                committed_lsn: lsn,
                data,
            },
        );
        progress.applied_lsn = lsn;
        progress.committed_lsn = lsn;
    }
}

#[derive(Default)]
struct TestControlPlane {
    effects: VecDeque<RuntimeEffect>,
    published: Vec<RuntimeEffectResult>,
}

#[derive(Clone)]
struct CountingFactory {
    storage: std::sync::Weak<TestApplication>,
    opened: Arc<AtomicUsize>,
    role_changes: Arc<AtomicUsize>,
    epoch_updates: Arc<AtomicUsize>,
    events: Arc<Mutex<Vec<String>>>,
    fail_change_role: Arc<AtomicBool>,
    fail_close: Arc<AtomicBool>,
}

struct CountingReplicator {
    inner: Arc<dyn Replicator>,
    primary: Arc<dyn PrimaryReplicator>,
    counts: CountingFactory,
}

struct PausingFactory {
    storage: std::sync::Weak<TestApplication>,
    captured: Arc<Mutex<Option<Arc<dyn Replicator>>>>,
    created: Arc<Notify>,
    resume: Arc<Notify>,
}

#[async_trait]
impl ReplicatorFactory for PausingFactory {
    async fn create_replicator(
        &self,
        context: ReplicatorFactoryContext,
        provider: Arc<dyn StateProvider>,
        settings: ReplicatorSettings,
    ) -> Result<ReplicatorInterfaces> {
        let interfaces =
            DefaultReplicatorFactory::new(self.storage.upgrade().ok_or(RuntimeError::Closed)?)
                .create_replicator(context, provider, settings)
                .await?;
        *self.captured.lock().unwrap() = Some(interfaces.replicator());
        self.created.notify_one();
        self.resume.notified().await;
        Ok(interfaces)
    }
}

#[async_trait]
impl ReplicatorFactory for CountingFactory {
    async fn create_replicator(
        &self,
        context: ReplicatorFactoryContext,
        provider: Arc<dyn StateProvider>,
        settings: ReplicatorSettings,
    ) -> Result<ReplicatorInterfaces> {
        let interfaces =
            DefaultReplicatorFactory::new(self.storage.upgrade().ok_or(RuntimeError::Closed)?)
                .create_replicator(context, provider, settings)
                .await?;
        let state_replicator = interfaces.state_replicator();
        let replicator = Arc::new(CountingReplicator {
            inner: interfaces.replicator(),
            primary: interfaces
                .primary_replicator()
                .ok_or(RuntimeError::NotPrimary)?,
            counts: self.clone(),
        });
        Ok(ReplicatorInterfaces::primary(replicator, state_replicator))
    }
}

fn runtime_with_factory(
    identity: ReplicaIdentity,
    application: Arc<TestApplication>,
    store: Arc<MemoryAuthorityStore>,
    factory: CountingFactory,
) -> Result<PodRuntime> {
    *application.factory.lock().unwrap() = Some(Arc::new(factory));
    Ok(PodRuntime::new(identity, application, store))
}

#[async_trait]
impl Replicator for CountingReplicator {
    async fn open(&self) -> Result<String> {
        self.counts
            .events
            .lock()
            .unwrap()
            .push("replicator.open".to_string());
        self.counts.opened.fetch_add(1, Ordering::SeqCst);
        self.inner.open().await
    }

    async fn change_role(&self, epoch: Epoch, role: ReplicaRole) -> Result<()> {
        self.counts
            .events
            .lock()
            .unwrap()
            .push("replicator.change_role".to_string());
        self.counts.role_changes.fetch_add(1, Ordering::SeqCst);
        if self.counts.fail_change_role.swap(false, Ordering::SeqCst) {
            return Err(RuntimeError::Application(
                "injected replicator role failure".to_string(),
            ));
        }
        self.inner.change_role(epoch, role).await
    }

    async fn update_epoch(&self, epoch: Epoch) -> Result<()> {
        self.counts
            .events
            .lock()
            .unwrap()
            .push("replicator.update_epoch".to_string());
        self.counts.epoch_updates.fetch_add(1, Ordering::SeqCst);
        self.inner.update_epoch(epoch).await
    }

    async fn current_progress(&self) -> Result<i64> {
        self.inner.current_progress().await
    }

    async fn catch_up_capability(&self) -> Result<i64> {
        self.inner.catch_up_capability().await
    }

    async fn close(&self) -> Result<()> {
        self.counts
            .events
            .lock()
            .unwrap()
            .push("replicator.close".to_string());
        if self.counts.fail_close.swap(false, Ordering::SeqCst) {
            return Err(RuntimeError::Application(
                "injected replicator close failure".to_string(),
            ));
        }
        self.inner.close().await
    }

    fn abort(&self) {
        self.counts
            .events
            .lock()
            .unwrap()
            .push("replicator.abort".to_string());
        self.inner.abort();
    }
}

#[async_trait]
impl PrimaryReplicator for CountingReplicator {
    async fn on_data_loss(&self) -> Result<bool> {
        self.primary.on_data_loss().await
    }

    async fn update_catch_up_replica_set_configuration(
        &self,
        current: ConfigurationDescriptor,
        previous: ConfigurationDescriptor,
    ) -> Result<()> {
        self.primary
            .update_catch_up_replica_set_configuration(current, previous)
            .await
    }

    async fn wait_for_catch_up_quorum(&self, mode: ReplicaSetQuorumMode) -> Result<()> {
        self.primary.wait_for_catch_up_quorum(mode).await
    }

    async fn update_current_replica_set_configuration(
        &self,
        current: ConfigurationDescriptor,
    ) -> Result<()> {
        self.primary
            .update_current_replica_set_configuration(current)
            .await
    }

    async fn build_replica(&self, replica: ReplicaInformation) -> Result<()> {
        self.primary.build_replica(replica).await
    }

    async fn remove_replica(&self, replica_id: ReplicaId) -> Result<()> {
        self.primary.remove_replica(replica_id).await
    }
}

#[async_trait]
impl RuntimeControlPlane for TestControlPlane {
    async fn next_effect(&mut self) -> Result<Option<RuntimeEffect>> {
        Ok(self.effects.pop_front())
    }

    async fn publish(&mut self, result: RuntimeEffectResult) -> Result<()> {
        self.published.push(result);
        Ok(())
    }
}

#[async_trait]
impl StatefulServiceReplica for TestApplication {
    async fn open(self: Arc<Self>, context: OpenContext) -> Result<Arc<dyn Replicator>> {
        self.events.lock().unwrap().push("service.open".to_string());
        *self.partition.lock().unwrap() = Some(context.partition.clone());
        assert_eq!(
            context.partition.get_write_status().await?,
            AccessStatus::NotPrimary
        );
        let factory = self.factory.lock().unwrap().clone();
        let factory =
            factory.unwrap_or_else(|| Arc::new(DefaultReplicatorFactory::new(self.clone())));
        let partition = context.partition.with_factory(factory);
        let settings = self.settings.lock().unwrap().clone();
        let interfaces = partition.create_replicator(self.clone(), settings).await?;
        let state_replicator = interfaces.state_replicator();
        let replication = state_replicator.get_replication_stream().await?;
        let copy = state_replicator.get_copy_stream().await?;
        self.streams_taken.fetch_add(2, Ordering::SeqCst);
        for stream in [replication, copy] {
            if self.manual_streams.load(Ordering::SeqCst) {
                self.held_streams.lock().unwrap().push(stream);
            } else {
                tokio::spawn(consume_stream(Arc::downgrade(&self), stream));
            }
        }
        *self.state_replicator.lock().unwrap() = Some(state_replicator);
        *self.returned_control.lock().unwrap() = Some(interfaces.replicator());
        if self.pause_open.load(Ordering::SeqCst) {
            self.open_notify.notify_one();
            self.resume_open_notify.notified().await;
        }
        if self.fail_open.load(Ordering::SeqCst) {
            return Err(RuntimeError::Application("injected Open failure".into()));
        }
        Ok(interfaces.replicator())
    }

    async fn change_role(&self, _role: ReplicaRole) -> Result<RoleChange> {
        self.events
            .lock()
            .unwrap()
            .push("service.change_role".to_string());
        if self.fail_change_role.swap(false, Ordering::SeqCst) {
            return Err(RuntimeError::Application(
                "injected role callback failure".to_string(),
            ));
        }
        Ok(RoleChange {
            service_address: None,
        })
    }

    async fn close(&self) -> Result<()> {
        self.events
            .lock()
            .unwrap()
            .push("service.close".to_string());
        if self.pause_close.load(Ordering::SeqCst) {
            self.close_notify.notify_one();
            self.resume_close_notify.notified().await;
        }
        if self.fail_close.swap(false, Ordering::SeqCst) {
            return Err(RuntimeError::Application(
                "injected close failure".to_string(),
            ));
        }
        Ok(())
    }

    fn abort(&self) {
        self.events
            .lock()
            .unwrap()
            .push("service.abort".to_string());
    }
}

#[async_trait]
impl StateProvider for TestApplication {
    async fn update_epoch(&self, _epoch: Epoch, _previous_epoch_last_lsn: i64) -> Result<()> {
        self.events
            .lock()
            .unwrap()
            .push("provider.update_epoch".to_string());
        if self.pause_update_epoch.load(Ordering::SeqCst) {
            self.update_epoch_notify.notify_one();
            self.resume_update_epoch_notify.notified().await;
        }
        if self.fail_update_epoch.swap(false, Ordering::SeqCst) {
            return Err(RuntimeError::Application(
                "injected epoch update failure".to_string(),
            ));
        }
        Ok(())
    }

    async fn last_committed_lsn(&self) -> Result<i64> {
        Ok(self.progress.lock().unwrap().committed_lsn)
    }

    async fn get_copy_context(&self) -> Result<OperationDataStream> {
        Ok(Box::pin(stream::empty()))
    }

    async fn get_copy_state(
        &self,
        up_to_lsn: i64,
        mut copy_context: OperationDataStream,
    ) -> Result<OperationDataStream> {
        let mut context_items = Vec::new();
        while let Some(item) = copy_context.next().await {
            context_items.push(item?);
        }
        self.copy_context_items
            .lock()
            .unwrap()
            .extend(context_items);
        let mut snapshot = Vec::new();
        for operation in self
            .applied
            .lock()
            .unwrap()
            .range(..=up_to_lsn)
            .map(|(_, operation)| operation)
        {
            snapshot.extend_from_slice(&operation.lsn.to_be_bytes());
            snapshot.extend_from_slice(&operation.committed_lsn.to_be_bytes());
            snapshot.extend_from_slice(&(operation.data.len() as u64).to_be_bytes());
            snapshot.extend_from_slice(&operation.data);
        }
        let chunks = if snapshot.is_empty() {
            Vec::new()
        } else {
            let split = snapshot.len().div_ceil(2);
            vec![
                Ok(Bytes::copy_from_slice(&snapshot[..split])),
                Ok(Bytes::copy_from_slice(&snapshot[split..])),
            ]
        };
        let pause = self.pause_copy_enumeration.clone();
        let paused = self.copy_enumeration_notify.clone();
        let resume = self.resume_copy_enumeration_notify.clone();
        Ok(Box::pin(stream::unfold(
            (chunks.into_iter(), true),
            move |(mut chunks, first)| {
                let pause = pause.clone();
                let paused = paused.clone();
                let resume = resume.clone();
                async move {
                    if first && pause.load(Ordering::SeqCst) {
                        paused.notify_one();
                        resume.notified().await;
                    }
                    std::iter::Iterator::next(&mut chunks).map(|chunk| (chunk, (chunks, false)))
                }
            },
        )))
    }

    async fn on_data_loss(&self) -> Result<bool> {
        self.events
            .lock()
            .unwrap()
            .push("provider.on_data_loss".into());
        Ok(false)
    }
}

#[async_trait]
impl DurableState for TestApplication {
    async fn get_replication_operations(
        &self,
        from_lsn: i64,
        to_lsn: i64,
    ) -> Result<RetainedOperationStream> {
        if from_lsn > to_lsn {
            return Ok(Box::pin(stream::empty()));
        }
        let operations = self
            .applied
            .lock()
            .unwrap()
            .range(from_lsn..=to_lsn)
            .map(|(_, operation)| Ok(operation.clone()))
            .collect::<Vec<_>>();
        let pause = self.pause_retained_enumeration.clone();
        let paused = self.retained_enumeration_notify.clone();
        let resume = self.resume_retained_enumeration_notify.clone();
        Ok(Box::pin(stream::unfold(
            (operations.into_iter(), true),
            move |(mut operations, first)| {
                let pause = pause.clone();
                let paused = paused.clone();
                let resume = resume.clone();
                async move {
                    if first && pause.load(Ordering::SeqCst) {
                        paused.notify_one();
                        resume.notified().await;
                    }
                    std::iter::Iterator::next(&mut operations)
                        .map(|operation| (operation, (operations, false)))
                }
            },
        )))
    }

    async fn apply_copy_chunk(
        &self,
        build_id: &OperationId,
        sequence: u64,
        chunk: CopyChunk,
    ) -> Result<()> {
        self.copy_apply_count.fetch_add(1, Ordering::SeqCst);
        let key = (build_id.to_string(), sequence);
        let mut chunks = self.copy_chunks.lock().unwrap();
        if let Some(existing) = chunks.get(&key)
            && existing != &chunk.data
        {
            return Err(RuntimeError::Application(
                "copy chunk retry changed contents".to_string(),
            ));
        }
        chunks.insert(key, chunk.data);
        Ok(())
    }

    async fn verify_copy_chunk(
        &self,
        build_id: &OperationId,
        sequence: u64,
        chunk: &CopyChunk,
    ) -> Result<bool> {
        Ok(self
            .copy_chunks
            .lock()
            .unwrap()
            .get(&(build_id.to_string(), sequence))
            .is_some_and(|stored| stored == &chunk.data))
    }

    async fn finish_copy(
        &self,
        build_id: &OperationId,
        up_to_lsn: i64,
        committed_lsn: i64,
    ) -> Result<DurableApplicationProgress> {
        let prefix = build_id.to_string();
        let chunks = self.copy_chunks.lock().unwrap();
        let mut copied = chunks
            .iter()
            .filter(|((id, _), _)| id == &prefix)
            .map(|((_, sequence), data)| (*sequence, data.clone()))
            .collect::<Vec<_>>();
        copied.sort_by_key(|(sequence, _)| *sequence);
        drop(chunks);
        let mut snapshot = Vec::new();
        for (_, chunk) in copied {
            snapshot.extend_from_slice(&chunk);
        }
        let mut operations = self.applied.lock().unwrap();
        let mut offset = 0;
        while offset < snapshot.len() {
            if snapshot.len() - offset < 24 {
                return Err(RuntimeError::Application(
                    "copy chunk is malformed".to_string(),
                ));
            }
            let lsn = i64::from_be_bytes(snapshot[offset..offset + 8].try_into().unwrap());
            let operation_committed_lsn =
                i64::from_be_bytes(snapshot[offset + 8..offset + 16].try_into().unwrap());
            let data_len =
                u64::from_be_bytes(snapshot[offset + 16..offset + 24].try_into().unwrap()) as usize;
            offset += 24;
            if snapshot.len() - offset < data_len {
                return Err(RuntimeError::Application(
                    "copy chunk data is truncated".to_string(),
                ));
            }
            operations.insert(
                lsn,
                Operation {
                    lsn,
                    committed_lsn: operation_committed_lsn,
                    data: Bytes::copy_from_slice(&snapshot[offset..offset + data_len]),
                },
            );
            offset += data_len;
        }
        drop(operations);
        let mut progress = self.progress.lock().unwrap();
        progress.applied_lsn = progress.applied_lsn.max(up_to_lsn);
        progress.committed_lsn = progress.committed_lsn.max(committed_lsn);
        Ok(*progress)
    }

    async fn apply(&self, operation: Operation) -> Result<DurableApplicationAck> {
        if self.fail_apply.load(Ordering::SeqCst) {
            return Err(RuntimeError::Application(
                "injected durable apply failure".to_string(),
            ));
        }
        let result = {
            let mut progress = self.progress.lock().unwrap();
            if operation.lsn != progress.applied_lsn + 1 {
                return Err(RuntimeError::Application(
                    "test application observed a replication gap".to_string(),
                ));
            }
            progress.applied_lsn = operation.lsn;
            progress.committed_lsn = progress.committed_lsn.max(operation.committed_lsn);
            self.applied
                .lock()
                .unwrap()
                .insert(operation.lsn, operation);
            *progress
        };
        if self.pause_after_apply.load(Ordering::SeqCst) {
            self.applied_notify.notify_one();
            self.resume_notify.notified().await;
        }
        if self.fail_after_apply.swap(false, Ordering::SeqCst) {
            return Err(RuntimeError::Application(
                "injected ambiguous apply failure".to_string(),
            ));
        }
        Ok(result)
    }

    async fn durable_progress(&self) -> Result<DurableApplicationProgress> {
        Ok(*self.progress.lock().unwrap())
    }

    async fn verify_applied(&self, operation: &Operation) -> Result<bool> {
        Ok(self
            .applied
            .lock()
            .unwrap()
            .get(&operation.lsn)
            .is_some_and(|applied| applied.data == operation.data))
    }

    async fn commit(&self, committed_lsn: i64) -> Result<DurableApplicationProgress> {
        let result = {
            let mut progress = self.progress.lock().unwrap();
            if committed_lsn > progress.applied_lsn {
                return Err(RuntimeError::Application(
                    "commit exceeds applied progress".to_string(),
                ));
            }
            progress.committed_lsn = progress.committed_lsn.max(committed_lsn);
            *progress
        };
        if self.pause_commit.load(Ordering::SeqCst) {
            self.commit_notify.notify_one();
            self.resume_commit_notify.notified().await;
        }
        if self.fail_commit.swap(false, Ordering::SeqCst) {
            return Err(RuntimeError::Application(
                "injected commit failure".to_string(),
            ));
        }
        Ok(result)
    }
}

async fn consume_stream(
    application: std::sync::Weak<TestApplication>,
    mut stream: OperationStream,
) {
    while let Some(operation) = stream.get_operation().await.unwrap() {
        let Some(application) = application.upgrade() else {
            break;
        };
        let result = match &operation.metadata {
            OperationMetadata::Replication { lsn, committed_lsn } => {
                application
                    .apply(Operation {
                        lsn: *lsn,
                        committed_lsn: *committed_lsn,
                        data: operation.data.clone(),
                    })
                    .await
            }
            OperationMetadata::Copy { build_id, sequence } => {
                match application
                    .apply_copy_chunk(
                        build_id,
                        *sequence,
                        CopyChunk {
                            data: operation.data.clone(),
                        },
                    )
                    .await
                {
                    Ok(()) => application.durable_progress().await,
                    Err(error) => Err(error),
                }
            }
            OperationMetadata::CopyComplete {
                build_id,
                up_to_lsn,
                committed_lsn,
            } => {
                application
                    .finish_copy(build_id, *up_to_lsn, *committed_lsn)
                    .await
            }
        };
        match result {
            Ok(progress) => {
                let _ = operation.acknowledge(progress);
            }
            Err(error) => {
                let _ = operation.reject(error);
            }
        }
    }
}

fn empty_copy_context() -> OperationDataStream {
    Box::pin(stream::empty())
}

async fn next_copy_item(prepared: &mut PreparedCopy) -> proto::CopyItem {
    timeout(Duration::from_secs(1), prepared.items.next())
        .await
        .unwrap()
        .expect("copy stream ended")
        .unwrap()
}

async fn copy_through_final(prepared: &mut PreparedCopy) -> Vec<proto::CopyItem> {
    let mut items = Vec::new();
    loop {
        let item = next_copy_item(prepared).await;
        let final_item = item.final_item;
        items.push(item);
        if final_item {
            return items;
        }
    }
}

async fn prepare_copy_authorized(
    runtime: &PodRuntime,
    request: PrepareCopyRequest,
) -> Result<PreparedCopy> {
    runtime
        .authorize_build(
            request.build_id.clone(),
            request.target.clone(),
            request.configuration.clone(),
        )
        .await?;
    runtime.data_plane().prepare_copy(request).await
}

async fn open_primary(
    application: Arc<TestApplication>,
    members: Vec<ReplicaIdentity>,
) -> Arc<PodRuntime> {
    let local = members[0].clone();
    let runtime = Arc::new(PodRuntime::new(
        local.clone(),
        application,
        Arc::new(MemoryAuthorityStore::default()),
    ));
    for (index, action) in [
        RuntimeEffectAction::Open(OpenMode::Existing),
        RuntimeEffectAction::AdmitAuthority(Box::new(authority(local, members))),
        RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
    ]
    .into_iter()
    .enumerate()
    {
        runtime
            .apply_effect(effect(index as u64 + 1, action))
            .await
            .unwrap();
    }
    runtime
}

#[tokio::test]
async fn existing_build_id_rejects_a_different_exact_target() {
    let runtime = open_primary(
        Arc::new(TestApplication::default()),
        vec![
            identity(1, "primary"),
            identity(2, "secondary-2"),
            identity(3, "secondary-3"),
        ],
    )
    .await;
    let build_id = OperationId::new("target-bound-build");
    let first = identity(4, "replacement-4");
    runtime
        .authorize_build(build_id.clone(), first.clone(), BuildConfiguration::Current)
        .await
        .unwrap();

    assert!(matches!(
        runtime
            .authorize_build(
                build_id,
                identity(5, "replacement-5"),
                BuildConfiguration::Current,
            )
            .await,
        Err(RuntimeError::AuthorityMismatch(_))
    ));
}

#[test]
fn public_trait_method_sets_match_sf_v1_com_divisions() {
    fn methods(source: &str, name: &str) -> Vec<String> {
        let definition = source
            .split_once(&format!("pub trait {name}:"))
            .unwrap()
            .1
            .split_once("\n}")
            .unwrap()
            .0;
        let mut names = definition
            .split("fn ")
            .skip(1)
            .map(|tail| tail.split('(').next().unwrap().trim().to_owned())
            .collect::<Vec<_>>();
        names.sort();
        names
    }
    let replication = include_str!("../../kuberic-runtime/src/replicator/mod.rs");
    let application = include_str!("../../kuberic-runtime/src/application.rs");
    let library = include_str!("../../kuberic-runtime/src/lib.rs");
    assert!(
        replication.contains("#[doc(hidden)]\npub trait ManagedReplicator"),
        "the cross-crate managed bridge must remain hidden from generated user documentation"
    );
    assert!(
        !replication.contains("record_durable_peer_progress"),
        "raw peer status must not cross the managed bridge into quorum credit"
    );
    assert!(
        !include_str!("../src/transport.rs").contains(".record_durable_peer_progress("),
        "peer discovery may use reported progress for repair, never commit quorum credit"
    );
    assert!(!replication.contains("fn managed_replicator("));
    assert!(!replication.contains("ReplicatorInterfaces::new"));
    for internal_module in ["authority", "effects", "runtime"] {
        assert!(
            !library.contains(&format!("pub mod {internal_module};")),
            "{internal_module} must not be a public runtime module"
        );
    }
    for (source, name, expected) in [
        (
            replication,
            "Replicator",
            vec![
                "open",
                "change_role",
                "update_epoch",
                "close",
                "abort",
                "current_progress",
                "catch_up_capability",
            ],
        ),
        (
            replication,
            "PrimaryReplicator",
            vec![
                "on_data_loss",
                "update_catch_up_replica_set_configuration",
                "wait_for_catch_up_quorum",
                "update_current_replica_set_configuration",
                "build_replica",
                "remove_replica",
            ],
        ),
        (
            replication,
            "StateReplicator",
            vec![
                "replicate",
                "get_replication_stream",
                "get_copy_stream",
                "update_replicator_settings",
            ],
        ),
        (
            application,
            "StateProvider",
            vec![
                "update_epoch",
                "last_committed_lsn",
                "get_copy_context",
                "get_copy_state",
                "on_data_loss",
            ],
        ),
        (
            application,
            "StatefulServiceReplica",
            vec!["open", "change_role", "close", "abort"],
        ),
    ] {
        let mut expected = expected;
        expected.sort();
        assert_eq!(methods(source, name), expected, "{name}");
    }
}

#[tokio::test]
async fn streams_require_explicit_acknowledgement_and_drop_is_not_success() {
    let (sender, mut stream) = OperationStream::channel(1);
    let delivery = {
        let sender = sender.clone();
        tokio::spawn(async move {
            sender
                .send(
                    OperationMetadata::Replication {
                        lsn: 1,
                        committed_lsn: 0,
                    },
                    Bytes::from_static(b"one"),
                )
                .await
        })
    };
    let operation = stream.get_operation().await.unwrap().unwrap();
    assert!(!delivery.is_finished());
    drop(operation);
    assert!(matches!(
        delivery.await.unwrap(),
        Err(RuntimeError::WriteCompletionClosed)
    ));

    let delivery = {
        let sender = sender.clone();
        tokio::spawn(async move {
            sender
                .send(
                    OperationMetadata::Replication {
                        lsn: 1,
                        committed_lsn: 0,
                    },
                    Bytes::from_static(b"one"),
                )
                .await
        })
    };
    let operation = stream.get_operation().await.unwrap().unwrap();
    assert!(!delivery.is_finished());
    operation
        .acknowledge(DurableApplicationProgress {
            applied_lsn: 1,
            committed_lsn: 0,
        })
        .unwrap();
    assert_eq!(delivery.await.unwrap().unwrap().applied_lsn, 1);
    sender.close();
    assert!(stream.get_operation().await.unwrap().is_none());
}

#[tokio::test]
async fn service_owned_state_handle_and_stream_drive_exact_durable_quorum() {
    let primary = identity(1, "primary");
    let secondary = identity(2, "secondary");
    let members = vec![primary.clone(), secondary.clone()];
    let primary_app = Arc::new(TestApplication::default());
    *primary_app.settings.lock().unwrap() = Some(ReplicatorSettings {
        replication_address: "replication://primary".into(),
    });
    let source = open_primary(primary_app.clone(), members.clone()).await;
    assert_eq!(
        source.snapshot().await.replication_address.as_deref(),
        Some("replication://primary")
    );
    let target_app = Arc::new(TestApplication::default());
    target_app.manual_streams.store(true, Ordering::SeqCst);
    let target = Arc::new(PodRuntime::new(
        secondary.clone(),
        target_app.clone(),
        Arc::new(MemoryAuthorityStore::default()),
    ));
    for (index, action) in [
        RuntimeEffectAction::Open(OpenMode::New),
        RuntimeEffectAction::AdmitAuthority(Box::new(authority(secondary, members))),
        RuntimeEffectAction::ChangeRole(ReplicaRole::ActiveSecondary),
    ]
    .into_iter()
    .enumerate()
    {
        target
            .apply_effect(effect(index as u64 + 1, action))
            .await
            .unwrap();
    }
    let state = primary_app
        .state_replicator
        .lock()
        .unwrap()
        .clone()
        .unwrap();
    state
        .update_replicator_settings(ReplicatorSettings {
            replication_address: "replication://updated".into(),
        })
        .await
        .unwrap();
    assert!(state.get_replication_stream().await.is_err());
    assert!(state.get_copy_stream().await.is_err());
    let write = tokio::spawn(async move { state.replicate(Bytes::from_static(b"one")).await });
    let OutboundReplication::Replication(item) = source.data_plane().next_outbound().await.unwrap()
    else {
        panic!("expected replication")
    };
    let receive = {
        let target = target.clone();
        tokio::spawn(async move { target.data_plane().receive_replication(item).await })
    };
    let mut stream = target_app.held_streams.lock().unwrap().remove(0);
    let operation = stream.get_operation().await.unwrap().unwrap();
    let receive = receive.await.unwrap().unwrap();
    assert_eq!(receive.received.received_lsn, 1);
    assert_eq!(receive.received.applied_lsn, 0);
    source
        .data_plane()
        .accept_acknowledgement(receive.received.clone())
        .await
        .unwrap();
    assert!(!write.is_finished());
    let OperationMetadata::Replication { lsn, committed_lsn } = operation.metadata else {
        panic!("expected replication metadata")
    };
    let progress = target_app
        .apply(Operation {
            lsn,
            committed_lsn,
            data: operation.data.clone(),
        })
        .await
        .unwrap();
    assert!(!write.is_finished());
    operation.acknowledge(progress).unwrap();
    source
        .data_plane()
        .accept_acknowledgement(receive.applied().await.unwrap())
        .await
        .unwrap();
    assert_eq!(write.await.unwrap().unwrap(), 1);
    assert_eq!(target.snapshot().await.verified_replication_lsn, Some(1));
}

#[tokio::test]
async fn state_replicator_retries_failed_and_cancelled_operations_without_losing_identity() {
    let app = Arc::new(TestApplication::default());
    let runtime = open_primary(app.clone(), vec![identity(1, "primary")]).await;
    let state = app.state_replicator.lock().unwrap().clone().unwrap();
    app.fail_after_apply.store(true, Ordering::SeqCst);
    assert!(state.replicate(Bytes::from_static(b"one")).await.is_err());
    assert_eq!(
        state.replicate(Bytes::from_static(b"one")).await.unwrap(),
        1
    );
    app.pause_after_apply.store(true, Ordering::SeqCst);
    let write = {
        let state = state.clone();
        tokio::spawn(async move { state.replicate(Bytes::from_static(b"two")).await })
    };
    app.applied_notify.notified().await;
    write.abort();
    assert!(write.await.unwrap_err().is_cancelled());
    app.pause_after_apply.store(false, Ordering::SeqCst);
    assert_eq!(
        state.replicate(Bytes::from_static(b"two")).await.unwrap(),
        2
    );
    assert_eq!(runtime.snapshot().await.committed_lsn, 2);
}

#[tokio::test]
async fn restarted_state_replicator_does_not_poison_retry_with_a_different_write() {
    let local = identity(1, "primary");
    let store = Arc::new(MemoryAuthorityStore::default());
    store.local_writes.lock().unwrap().insert(
        OperationId::new("sf:old-session:1"),
        DurableLocalWrite {
            operation_id: OperationId::new("sf:old-session:1"),
            lsn: 1,
            committed_lsn: 0,
            data: Bytes::from_static(b"pending-a"),
            phase: kuberic_runtime_internal::authority::LocalWritePhase::Registered,
        },
    );
    let application = Arc::new(TestApplication::default());
    let runtime = Arc::new(PodRuntime::new(
        local.clone(),
        application.clone(),
        store.clone(),
    ));
    for (index, action) in [
        RuntimeEffectAction::Open(OpenMode::Existing),
        RuntimeEffectAction::AdmitAuthority(Box::new(authority(local.clone(), vec![local]))),
        RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
    ]
    .into_iter()
    .enumerate()
    {
        runtime
            .apply_effect(effect(index as u64 + 1, action))
            .await
            .unwrap();
    }
    let state = application
        .state_replicator
        .lock()
        .unwrap()
        .clone()
        .unwrap();

    assert!(matches!(
        state.replicate(Bytes::from_static(b"different-b")).await,
        Err(RuntimeError::LocalWritePending(_))
    ));
    assert_eq!(
        state
            .replicate(Bytes::from_static(b"pending-a"))
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn reconstruction_republishes_the_original_registered_write_without_a_client() {
    let local = identity(1, "primary");
    let store = Arc::new(MemoryAuthorityStore::default());
    let admitted = authority(local.clone(), vec![local.clone()]);
    *store.authority.lock().unwrap() = Some(admitted);
    store.local_writes.lock().unwrap().insert(
        OperationId::new("sf:old-session:3"),
        DurableLocalWrite {
            operation_id: OperationId::new("sf:old-session:3"),
            lsn: 3,
            committed_lsn: 2,
            data: Bytes::from_static(b"pending-three"),
            phase: kuberic_runtime_internal::authority::LocalWritePhase::Registered,
        },
    );
    let application = Arc::new(TestApplication::default());
    application.applied.lock().unwrap().insert(
        3,
        Operation {
            lsn: 3,
            committed_lsn: 2,
            data: Bytes::from_static(b"pending-three"),
        },
    );
    *application.progress.lock().unwrap() = DurableApplicationProgress {
        applied_lsn: 3,
        committed_lsn: 2,
    };
    let runtime = PodRuntime::new(local, application, store.clone());
    runtime
        .reconstruct(
            OpenMode::Existing,
            ReplicaRole::Primary,
            AccessStatus::Granted,
            AccessStatus::Granted,
            None,
        )
        .await
        .unwrap();

    assert_eq!(runtime.snapshot().await.committed_lsn, 3);
    assert_eq!(
        store
            .local_writes
            .lock()
            .unwrap()
            .get(&OperationId::new("sf:old-session:3"))
            .unwrap()
            .phase,
        kuberic_runtime_internal::authority::LocalWritePhase::Committed
    );
}

#[tokio::test]
async fn cold_primary_replays_retained_history_to_a_lagging_current_peer() {
    let primary = identity(1, "primary");
    let secondary = identity(2, "secondary");
    let application = Arc::new(TestApplication::default());
    application.seed_operation(1, Bytes::from_static(b"one"));
    let runtime = open_primary(application, vec![primary.clone(), secondary.clone()]).await;

    runtime.repair_peer(secondary.clone(), 0).await.unwrap();
    let OutboundReplication::Replication(item) =
        runtime.data_plane().next_outbound().await.unwrap()
    else {
        panic!("expected retained replication repair");
    };
    assert_eq!(
        item.receiver.as_ref().map(|identity| identity.replica_id),
        Some(secondary.replica_id.value())
    );
    assert_eq!(item.lsn, 1);
    assert_eq!(item.data, b"one");
}

#[tokio::test]
async fn peer_repair_status_does_not_grant_commit_quorum() {
    let primary = identity(1, "primary");
    let returned = identity(2, "returned");
    let acknowledged = identity(3, "acknowledged");
    let fourth = identity(4, "fourth");
    let fifth = identity(5, "fifth");
    let members = vec![
        primary.clone(),
        returned.clone(),
        acknowledged.clone(),
        fourth.clone(),
        fifth,
    ];
    let admitted = authority(primary, members.clone());
    let application = Arc::new(TestApplication::default());
    application.seed_progress(100);
    let runtime = open_primary(application, members).await;

    let pending = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("post-failover-101"),
            data: Bytes::from_static(b"new-101"),
        })
        .await
        .unwrap();
    assert_eq!(pending.lsn, 101);

    runtime.repair_peer(returned, 120).await.unwrap();
    runtime
        .data_plane()
        .accept_acknowledgement(acknowledgement(&admitted, acknowledged, 101))
        .await
        .unwrap();

    let committed = tokio::spawn(pending.committed());
    tokio::task::yield_now().await;
    assert!(
        !committed.is_finished(),
        "raw repair progress must not provide the third quorum vote"
    );

    runtime
        .data_plane()
        .accept_acknowledgement(acknowledgement(&admitted, fourth, 101))
        .await
        .unwrap();
    assert_eq!(committed.await.unwrap().unwrap().committed_lsn, 101);
}

#[tokio::test]
async fn primary_control_build_waits_for_service_copy_ack_and_removal_is_fenced() {
    let primary = identity(1, "primary");
    let replacement = identity(2, "replacement");
    let source = open_primary(Arc::new(TestApplication::default()), vec![primary.clone()]).await;
    let control = source.primary_replicator().await.unwrap();
    let build = {
        let control = control.clone();
        let target = replacement.clone();
        tokio::spawn(async move {
            control
                .build_replica(kuberic_runtime::replicator::ReplicaInformation {
                    build_id: OperationId::new("sf-build"),
                    identity: target,
                    replication_address: "target".into(),
                })
                .await
        })
    };
    assert!(matches!(
        source.data_plane().next_outbound().await,
        Some(OutboundReplication::Build(_))
    ));
    let mut prepared = prepare_copy_authorized(
        &source,
        PrepareCopyRequest {
            build_id: OperationId::new("sf-build"),
            target: replacement.clone(),
            configuration: BuildConfiguration::Current,
            copy_context: empty_copy_context(),
        },
    )
    .await
    .unwrap();
    let target_app = Arc::new(TestApplication::default());
    target_app.manual_streams.store(true, Ordering::SeqCst);
    let target = Arc::new(PodRuntime::new(
        replacement.clone(),
        target_app.clone(),
        Arc::new(MemoryAuthorityStore::default()),
    ));
    for (index, action) in [
        RuntimeEffectAction::Open(OpenMode::New),
        RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary),
        RuntimeEffectAction::AdmitBuildAuthority(Box::new(prepared.authority.clone())),
    ]
    .into_iter()
    .enumerate()
    {
        target
            .apply_effect(effect(index as u64 + 1, action))
            .await
            .unwrap();
    }
    let mut copy = target_app.held_streams.lock().unwrap().remove(1);
    let delivery = {
        let target = target.clone();
        let item = next_copy_item(&mut prepared).await;
        tokio::spawn(async move { target.data_plane().receive_copy_item(item).await })
    };
    let operation = copy.get_operation().await.unwrap().unwrap();
    assert!(matches!(
        operation.metadata,
        OperationMetadata::CopyComplete { .. }
    ));
    assert!(!delivery.is_finished());
    assert!(!build.is_finished());
    operation
        .acknowledge(DurableApplicationProgress::default())
        .unwrap();
    source
        .data_plane()
        .accept_copy_acknowledgement(delivery.await.unwrap().unwrap())
        .await
        .unwrap();
    timeout(Duration::from_secs(1), build)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        control.remove_replica(primary.replica_id).await,
        Err(RuntimeError::AuthorityMismatch(_))
    ));
    control
        .remove_replica(replacement.replica_id)
        .await
        .unwrap();
    assert!(source.snapshot().await.builds.is_empty());
    assert!(matches!(
        source.data_plane().next_outbound().await,
        Some(OutboundReplication::Remove(_))
    ));
}

#[tokio::test]
async fn direct_control_epoch_fences_old_replication_and_abort_ends_pending_quorum_waits() {
    let app = Arc::new(TestApplication::default());
    let local = identity(1, "primary");
    let secondary = identity(2, "secondary");
    let runtime = open_primary(app.clone(), vec![local.clone(), secondary.clone()]).await;
    let pending = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("pending"),
            data: Bytes::from_static(b"one"),
        })
        .await
        .unwrap();
    let control = app.returned_control.lock().unwrap().clone().unwrap();
    control.update_epoch(Epoch::new(0, 2)).await.unwrap();
    assert!(matches!(
        pending.committed().await,
        Err(RuntimeError::WriteClosed(_))
    ));
    assert!(matches!(
        runtime
            .data_plane()
            .accept_acknowledgement(acknowledgement(
                &authority(local, vec![identity(1, "primary"), secondary.clone()]),
                secondary,
                1
            ))
            .await,
        Err(RuntimeError::AuthorityMismatch(_))
    ));
    assert!(control.update_epoch(Epoch::new(0, 1)).await.is_err());
    control.abort();
    let state = app.state_replicator.lock().unwrap().clone().unwrap();
    assert!(matches!(
        state.replicate(Bytes::from_static(b"two")).await,
        Err(RuntimeError::Closed)
    ));
}

#[tokio::test]
async fn failed_or_cancelled_service_open_aborts_created_interfaces() {
    for cancelled in [false, true] {
        let app = Arc::new(TestApplication::default());
        app.pause_open.store(cancelled, Ordering::SeqCst);
        app.fail_open.store(!cancelled, Ordering::SeqCst);
        let runtime = Arc::new(PodRuntime::new(
            identity(1, "primary"),
            app.clone(),
            Arc::new(MemoryAuthorityStore::default()),
        ));
        let open = {
            let runtime = runtime.clone();
            tokio::spawn(async move {
                runtime
                    .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
                    .await
            })
        };
        if cancelled {
            app.open_notify.notified().await;
            open.abort();
            assert!(open.await.unwrap_err().is_cancelled());
        } else {
            assert!(matches!(
                open.await.unwrap(),
                Err(RuntimeError::Application(_))
            ));
        }
        assert!(!runtime.snapshot().await.open);
        let state = app.state_replicator.lock().unwrap().clone().unwrap();
        assert!(matches!(
            state.replicate(Bytes::from_static(b"no")).await,
            Err(RuntimeError::Closed)
        ));
        assert!(
            app.events
                .lock()
                .unwrap()
                .iter()
                .any(|event| event == "service.abort")
        );
    }
}

#[tokio::test]
async fn cancelled_factory_creation_aborts_pending_managed_replicator() {
    let application = Arc::new(TestApplication::default());
    let captured = Arc::new(Mutex::new(None));
    let created = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    *application.factory.lock().unwrap() = Some(Arc::new(PausingFactory {
        storage: Arc::downgrade(&application),
        captured: captured.clone(),
        created: created.clone(),
        resume,
    }));
    let runtime = Arc::new(PodRuntime::new(
        identity(1, "cancelled-factory"),
        application,
        Arc::new(MemoryAuthorityStore::default()),
    ));
    let open_runtime = runtime.clone();
    let open = tokio::spawn(async move {
        open_runtime
            .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
            .await
    });
    created.notified().await;
    open.abort();
    assert!(matches!(open.await, Err(error) if error.is_cancelled()));
    let control = captured.lock().unwrap().clone().unwrap();
    assert!(matches!(control.open().await, Err(RuntimeError::Closed)));
}

#[tokio::test]
async fn quorum_modes_data_loss_and_configuration_methods_use_the_default_engine() {
    use kuberic_runtime::replicator::ReplicaSetQuorumMode;
    let members = vec![
        identity(1, "primary"),
        identity(2, "second"),
        identity(3, "third"),
    ];
    let app = Arc::new(TestApplication::default());
    app.seed_progress(1);
    let runtime = open_primary(app.clone(), members.clone()).await;
    let control = runtime.primary_replicator().await.unwrap();
    assert_eq!(control.current_progress().await.unwrap(), 1);
    assert_eq!(control.catch_up_capability().await.unwrap(), 1);
    let admitted = authority(members[0].clone(), members.clone());
    let mut invalid = admitted.current_configuration.clone();
    invalid.epoch = Epoch::new(0, 3);
    assert!(matches!(
        control
            .update_current_replica_set_configuration(invalid)
            .await,
        Err(RuntimeError::AuthorityMismatch(_))
    ));
    assert!(!control.on_data_loss().await.unwrap());
    assert!(
        app.events
            .lock()
            .unwrap()
            .iter()
            .any(|event| event == "provider.on_data_loss")
    );
    let quorum = {
        let control = control.clone();
        tokio::spawn(async move {
            control
                .wait_for_catch_up_quorum(ReplicaSetQuorumMode::WriteQuorum)
                .await
        })
    };
    let all = {
        let control = control.clone();
        tokio::spawn(async move {
            control
                .wait_for_catch_up_quorum(ReplicaSetQuorumMode::All)
                .await
        })
    };
    runtime
        .data_plane()
        .accept_acknowledgement(acknowledgement(&admitted, members[1].clone(), 1))
        .await
        .unwrap();
    timeout(Duration::from_secs(1), quorum)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!all.is_finished());
    control.abort();
    assert!(matches!(
        timeout(Duration::from_secs(1), all).await.unwrap().unwrap(),
        Err(RuntimeError::Closed)
    ));
}

#[tokio::test]
async fn lifecycle_orders_role_changes_and_close_like_service_fabric() {
    let app = Arc::new(TestApplication::default());
    let events = app.events.clone();
    *app.factory.lock().unwrap() = Some(Arc::new(CountingFactory {
        storage: Arc::downgrade(&app),
        opened: Arc::new(AtomicUsize::new(0)),
        role_changes: Arc::new(AtomicUsize::new(0)),
        epoch_updates: Arc::new(AtomicUsize::new(0)),
        events: events.clone(),
        fail_change_role: Arc::new(AtomicBool::new(false)),
        fail_close: Arc::new(AtomicBool::new(false)),
    }));
    let runtime = open_primary(app.clone(), vec![identity(1, "primary")]).await;
    events.lock().unwrap().clear();
    runtime
        .apply_effect(effect(
            5,
            RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            6,
            RuntimeEffectAction::ChangeRole(ReplicaRole::ActiveSecondary),
        ))
        .await
        .unwrap();
    let close = effect(7, RuntimeEffectAction::Close);
    let result = runtime.apply_effect(close.clone()).await.unwrap();
    assert_eq!(runtime.apply_effect(close).await.unwrap(), result);
    runtime
        .apply_effect(effect(8, RuntimeEffectAction::Abort))
        .await
        .unwrap();
    drop(runtime);
    assert_eq!(
        events.lock().unwrap().as_slice(),
        [
            "replicator.change_role",
            "service.change_role",
            "replicator.change_role",
            "service.change_role",
            "replicator.close",
            "service.close",
        ]
    );
}

#[tokio::test]
async fn primary_promotion_retries_epoch_stage_before_application_role() {
    let local = identity(1, "promotion-primary");
    let application = Arc::new(TestApplication::default());
    let events = application.events.clone();
    let role_changes = Arc::new(AtomicUsize::new(0));
    let epoch_updates = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with_factory(
        local.clone(),
        application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
        CountingFactory {
            storage: Arc::downgrade(&application),
            opened: Arc::new(AtomicUsize::new(0)),
            role_changes: role_changes.clone(),
            epoch_updates: epoch_updates.clone(),
            events: events.clone(),
            fail_change_role: Arc::new(AtomicBool::new(false)),
            fail_close: Arc::new(AtomicBool::new(false)),
        },
    )
    .unwrap();
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority(local.clone(), vec![local]))),
        ))
        .await
        .unwrap();
    events.lock().unwrap().clear();
    application.fail_update_epoch.store(true, Ordering::SeqCst);
    let promote = effect(3, RuntimeEffectAction::ChangeRole(ReplicaRole::Primary));
    assert!(matches!(
        runtime.apply_effect(promote.clone()).await,
        Err(RuntimeError::Application(_))
    ));
    assert_eq!(
        runtime.snapshot().await.role_transition,
        Some(kuberic_runtime_internal::effects::RoleTransition {
            completed_role: ReplicaRole::None,
            target_role: ReplicaRole::Primary,
            replicator_completed: true,
            epoch_completed: false,
            application_completed: false,
        })
    );
    runtime.apply_effect(promote).await.unwrap();

    assert_eq!(role_changes.load(Ordering::SeqCst), 1);
    assert_eq!(epoch_updates.load(Ordering::SeqCst), 2);
    assert_eq!(
        events.lock().unwrap().as_slice(),
        [
            "replicator.change_role",
            "replicator.update_epoch",
            "provider.update_epoch",
            "replicator.update_epoch",
            "provider.update_epoch",
            "service.change_role",
        ]
    );
}

#[tokio::test]
async fn newer_primary_authority_stays_write_closed_until_epoch_stage_completes() {
    let local = identity(1, "same-primary-new-epoch");
    let application = Arc::new(TestApplication::default());
    let runtime = open_primary(application.clone(), vec![local.clone()]).await;
    let newer = AdmittedAuthority {
        local_identity: local.clone(),
        transition_kind: None,
        previous_configuration: None,
        current_configuration: ConfigurationDescriptor::new(
            Epoch::new(0, 2),
            local.replica_id,
            vec![ConfigurationMember {
                identity: local,
                role: ReplicaRole::Primary,
            }],
            1,
        ),
    };
    runtime
        .apply_effect(effect(
            5,
            RuntimeEffectAction::AdmitAuthority(Box::new(newer)),
        ))
        .await
        .unwrap();
    assert_eq!(
        runtime.snapshot().await.write_status,
        AccessStatus::ReconfigurationPending
    );
    assert!(matches!(
        runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("blocked-before-epoch"),
                data: Bytes::from_static(b"blocked"),
            })
            .await,
        Err(RuntimeError::WriteClosed(
            AccessStatus::ReconfigurationPending
        ))
    ));
    runtime
        .apply_effect(effect(
            6,
            RuntimeEffectAction::ChangeReplicatorRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    application.pause_update_epoch.store(true, Ordering::SeqCst);
    let epoch_runtime = runtime.clone();
    let update = tokio::spawn(async move {
        epoch_runtime
            .apply_effect(effect(7, RuntimeEffectAction::UpdateEpoch))
            .await
    });
    application.update_epoch_notify.notified().await;
    assert!(matches!(
        runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("blocked-during-epoch"),
                data: Bytes::from_static(b"blocked"),
            })
            .await,
        Err(RuntimeError::WriteClosed(
            AccessStatus::ReconfigurationPending
        ))
    ));
    application
        .pause_update_epoch
        .store(false, Ordering::SeqCst);
    application.resume_update_epoch_notify.notify_waiters();
    update.await.unwrap().unwrap();
    runtime
        .apply_effect(effect(
            8,
            RuntimeEffectAction::ChangeApplicationRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            9,
            RuntimeEffectAction::SetAccessStatus {
                read: AccessStatus::Granted,
                write: AccessStatus::Granted,
            },
        ))
        .await
        .unwrap();
    runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("allowed-after-epoch"),
            data: Bytes::from_static(b"allowed"),
        })
        .await
        .unwrap()
        .committed()
        .await
        .unwrap();
}

#[tokio::test]
async fn explicit_abort_stops_replicator_before_application() {
    let application = Arc::new(TestApplication::default());
    let events = application.events.clone();
    let runtime = runtime_with_factory(
        identity(1, "explicit-abort"),
        application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
        CountingFactory {
            storage: Arc::downgrade(&application),
            opened: Arc::new(AtomicUsize::new(0)),
            role_changes: Arc::new(AtomicUsize::new(0)),
            epoch_updates: Arc::new(AtomicUsize::new(0)),
            events: events.clone(),
            fail_change_role: Arc::new(AtomicBool::new(false)),
            fail_close: Arc::new(AtomicBool::new(false)),
        },
    )
    .unwrap();
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    events.lock().unwrap().clear();
    runtime
        .apply_effect(effect(2, RuntimeEffectAction::Abort))
        .await
        .unwrap();
    assert_eq!(
        events.lock().unwrap().as_slice(),
        ["replicator.abort", "service.abort"]
    );
}

#[tokio::test]
async fn ambiguous_primary_authority_admission_fences_pending_writes_and_old_acks() {
    let local = identity(1, "primary");
    let secondary = identity(2, "secondary");
    let admitted = authority(local.clone(), vec![local.clone(), secondary.clone()]);
    let store = Arc::new(MemoryAuthorityStore::default());
    let runtime = PodRuntime::new(local, Arc::new(TestApplication::default()), store.clone());
    for (index, action) in [
        RuntimeEffectAction::Open(OpenMode::Existing),
        RuntimeEffectAction::AdmitAuthority(Box::new(admitted.clone())),
        RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
    ]
    .into_iter()
    .enumerate()
    {
        runtime
            .apply_effect(effect(index as u64 + 1, action))
            .await
            .unwrap();
    }
    let pending = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("old-epoch"),
            data: Bytes::from_static(b"one"),
        })
        .await
        .unwrap();
    let next = AdmittedAuthority {
        current_configuration: ConfigurationDescriptor::new(
            Epoch::new(0, 2),
            admitted.current_configuration.primary_id,
            admitted.current_configuration.members.clone(),
            admitted.current_configuration.write_quorum,
        ),
        ..admitted.clone()
    };
    store.fail_after_admit.store(true, Ordering::SeqCst);
    assert!(matches!(
        runtime
            .apply_effect(effect(
                5,
                RuntimeEffectAction::AdmitAuthority(Box::new(next))
            ))
            .await,
        Err(RuntimeError::Application(_))
    ));
    assert!(matches!(
        pending.committed().await,
        Err(RuntimeError::WriteClosed(_))
    ));
    assert!(matches!(
        runtime
            .data_plane()
            .accept_acknowledgement(acknowledgement(&admitted, secondary, 1))
            .await,
        Err(RuntimeError::AuthorityMismatch(_))
    ));
    let snapshot = runtime.snapshot().await;
    assert_eq!(snapshot.committed_lsn, 0);
    assert_eq!(snapshot.write_status, AccessStatus::ReconfigurationPending);
}

#[tokio::test]
async fn removing_a_replica_terminates_its_pending_build_wait() {
    let runtime = open_primary(
        Arc::new(TestApplication::default()),
        vec![identity(1, "primary")],
    )
    .await;
    let control = runtime.primary_replicator().await.unwrap();
    let target = identity(2, "target");
    let wait = {
        let control = control.clone();
        let target = target.clone();
        tokio::spawn(async move {
            control
                .build_replica(kuberic_runtime::replicator::ReplicaInformation {
                    build_id: OperationId::new("removed-build"),
                    identity: target,
                    replication_address: "target".into(),
                })
                .await
        })
    };
    assert!(matches!(
        runtime.data_plane().next_outbound().await,
        Some(OutboundReplication::Build(_))
    ));
    control.remove_replica(target.replica_id).await.unwrap();
    assert!(matches!(
        timeout(Duration::from_secs(1), wait)
            .await
            .unwrap()
            .unwrap(),
        Err(RuntimeError::ReplicaRemoved(2))
    ));
}

#[tokio::test]
async fn bounded_outbound_build_queue_is_cancelled_by_abort() {
    let runtime = open_primary(
        Arc::new(TestApplication::default()),
        vec![identity(1, "primary")],
    )
    .await;
    let control = runtime.primary_replicator().await.unwrap();
    let mut builds = Vec::new();
    for id in 2..=66 {
        let control = control.clone();
        builds.push(tokio::spawn(async move {
            control
                .build_replica(kuberic_runtime::replicator::ReplicaInformation {
                    build_id: OperationId::new(format!("bounded-build-{id}")),
                    identity: identity(id, &format!("target-{id}")),
                    replication_address: format!("target-{id}"),
                })
                .await
        }));
    }
    tokio::task::yield_now().await;
    assert!(builds.iter().any(|build| !build.is_finished()));
    control.abort();
    for build in builds {
        assert!(matches!(
            timeout(Duration::from_secs(1), build)
                .await
                .unwrap()
                .unwrap(),
            Err(RuntimeError::Closed)
        ));
    }
}

#[tokio::test]
async fn dropping_runtime_always_aborts_application_and_selected_control() {
    let application = Arc::new(TestApplication::default());
    let events = application.events.clone();
    let runtime = runtime_with_factory(
        identity(1, "drop-abort"),
        application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
        CountingFactory {
            storage: Arc::downgrade(&application),
            opened: Arc::new(AtomicUsize::new(0)),
            role_changes: Arc::new(AtomicUsize::new(0)),
            epoch_updates: Arc::new(AtomicUsize::new(0)),
            events: events.clone(),
            fail_change_role: Arc::new(AtomicBool::new(false)),
            fail_close: Arc::new(AtomicBool::new(false)),
        },
    )
    .unwrap();
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    drop(runtime);
    assert_eq!(
        events.lock().unwrap().as_slice(),
        [
            "service.open",
            "replicator.open",
            "replicator.abort",
            "service.abort",
        ]
    );
}

#[tokio::test]
async fn dropping_runtime_aborts_retained_state_handles_streams_and_pending_writes() {
    let app = Arc::new(TestApplication::default());
    app.manual_streams.store(true, Ordering::SeqCst);
    let runtime = open_primary(
        app.clone(),
        vec![identity(1, "primary"), identity(2, "secondary")],
    )
    .await;
    let state = app.state_replicator.lock().unwrap().clone().unwrap();
    let write = {
        let state = state.clone();
        tokio::spawn(async move { state.replicate(Bytes::from_static(b"pending")).await })
    };
    assert!(matches!(
        runtime.data_plane().next_outbound().await,
        Some(OutboundReplication::Replication(_))
    ));
    drop(runtime);
    assert!(matches!(
        timeout(Duration::from_secs(1), write)
            .await
            .unwrap()
            .unwrap(),
        Err(RuntimeError::Closed)
    ));
    assert!(matches!(
        state.replicate(Bytes::from_static(b"closed")).await,
        Err(RuntimeError::Closed)
    ));
    let streams = std::mem::take(&mut *app.held_streams.lock().unwrap());
    for mut stream in streams {
        assert!(stream.get_operation().await.unwrap().is_none());
    }
}

fn identity(id: i64, instance: &str) -> ReplicaIdentity {
    ReplicaIdentity {
        replica_id: ReplicaId::new(id),
        instance_id: ReplicaInstanceId::new(instance),
        agent_generation: AgentGeneration::new(format!("generation-{instance}")),
    }
}

struct ExternalReplicator {
    replication: Mutex<Option<OperationStream>>,
    copy: Mutex<Option<OperationStream>>,
    senders: Vec<kuberic_runtime::replicator::stream::OperationSender>,
    settings: Mutex<ReplicatorSettings>,
    closed: AtomicBool,
}

#[async_trait]
impl Replicator for ExternalReplicator {
    async fn open(&self) -> Result<String> {
        Ok("external://replica".into())
    }
    async fn change_role(&self, _epoch: Epoch, _role: ReplicaRole) -> Result<()> {
        Ok(())
    }
    async fn update_epoch(&self, _epoch: Epoch) -> Result<()> {
        Ok(())
    }
    async fn close(&self) -> Result<()> {
        self.abort();
        Ok(())
    }
    fn abort(&self) {
        self.closed.store(true, Ordering::SeqCst);
        for sender in &self.senders {
            sender.close();
        }
    }
    async fn current_progress(&self) -> Result<i64> {
        Ok(7)
    }
    async fn catch_up_capability(&self) -> Result<i64> {
        Ok(3)
    }
}

#[async_trait]
impl StateReplicator for ExternalReplicator {
    async fn replicate(&self, _data: Bytes) -> Result<i64> {
        Err(RuntimeError::NotPrimary)
    }
    async fn get_replication_stream(&self) -> Result<OperationStream> {
        self.replication
            .lock()
            .unwrap()
            .take()
            .ok_or(RuntimeError::Closed)
    }
    async fn get_copy_stream(&self) -> Result<OperationStream> {
        self.copy.lock().unwrap().take().ok_or(RuntimeError::Closed)
    }
    async fn update_replicator_settings(&self, settings: ReplicatorSettings) -> Result<()> {
        *self.settings.lock().unwrap() = settings;
        Ok(())
    }
}

struct ExternalFactory;

#[async_trait]
impl ReplicatorFactory for ExternalFactory {
    async fn create_replicator(
        &self,
        _context: ReplicatorFactoryContext,
        _provider: Arc<dyn StateProvider>,
        settings: ReplicatorSettings,
    ) -> Result<ReplicatorInterfaces> {
        let (replication_tx, replication) = OperationStream::channel(1);
        let (copy_tx, copy) = OperationStream::channel(1);
        let replicator = Arc::new(ExternalReplicator {
            replication: Mutex::new(Some(replication)),
            copy: Mutex::new(Some(copy)),
            senders: vec![replication_tx, copy_tx],
            settings: Mutex::new(settings),
            closed: AtomicBool::new(false),
        });
        Ok(ReplicatorInterfaces::secondary(
            replicator.clone(),
            replicator,
        ))
    }
}

struct CountingExternalFactory {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl ReplicatorFactory for CountingExternalFactory {
    async fn create_replicator(
        &self,
        context: ReplicatorFactoryContext,
        provider: Arc<dyn StateProvider>,
        settings: ReplicatorSettings,
    ) -> Result<ReplicatorInterfaces> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        ExternalFactory
            .create_replicator(context, provider, settings)
            .await
    }
}

#[derive(Default)]
struct ExternalService {
    state: Mutex<Option<Arc<dyn StateReplicator>>>,
    streams: Mutex<Vec<OperationStream>>,
}

struct DoubleCreateService {
    factory_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl StatefulServiceReplica for DoubleCreateService {
    async fn open(self: Arc<Self>, context: OpenContext) -> Result<Arc<dyn Replicator>> {
        let partition = context
            .partition
            .with_factory(Arc::new(CountingExternalFactory {
                calls: self.factory_calls.clone(),
            }));
        let first = partition
            .create_replicator(Arc::new(TestApplication::default()), None)
            .await?;
        assert!(matches!(
            partition
                .create_replicator(Arc::new(TestApplication::default()), None)
                .await,
            Err(RuntimeError::Application(_))
        ));
        Ok(first.replicator())
    }

    async fn change_role(&self, _role: ReplicaRole) -> Result<RoleChange> {
        Ok(RoleChange {
            service_address: None,
        })
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    fn abort(&self) {}
}

#[async_trait]
impl StatefulServiceReplica for ExternalService {
    async fn open(self: Arc<Self>, context: OpenContext) -> Result<Arc<dyn Replicator>> {
        let interfaces = context
            .partition
            .with_factory(Arc::new(ExternalFactory))
            .create_replicator(Arc::new(TestApplication::default()), None)
            .await?;
        let state_replicator = interfaces.state_replicator();
        let streams = vec![
            state_replicator.get_replication_stream().await?,
            state_replicator.get_copy_stream().await?,
        ];
        *self.streams.lock().unwrap() = streams;
        *self.state.lock().unwrap() = Some(state_replicator);
        Ok(interfaces.replicator())
    }
    async fn change_role(&self, _role: ReplicaRole) -> Result<RoleChange> {
        Ok(RoleChange {
            service_address: None,
        })
    }
    async fn close(&self) -> Result<()> {
        Ok(())
    }
    fn abort(&self) {}
}

#[tokio::test]
async fn custom_factory_does_not_require_the_default_engine_or_service_storage_traits() {
    let service = Arc::new(ExternalService::default());
    let runtime = PodRuntime::new(
        identity(1, "external"),
        service.clone(),
        Arc::new(MemoryAuthorityStore::default()),
    );
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::New)))
        .await
        .unwrap();
    let snapshot = runtime.snapshot().await;
    assert_eq!(
        snapshot.replication_address.as_deref(),
        Some("external://replica")
    );
    assert_eq!(snapshot.current_progress, 7);
    assert!(service.state.lock().unwrap().is_some());
    assert_eq!(service.streams.lock().unwrap().len(), 2);
    runtime
        .apply_effect(effect(2, RuntimeEffectAction::RefreshApplicationProgress))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(3, RuntimeEffectAction::Close))
        .await
        .unwrap();
    let streams = std::mem::take(&mut *service.streams.lock().unwrap());
    for mut stream in streams {
        assert!(stream.get_operation().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn create_replicator_reserves_one_shot_ownership_before_factory_invocation() {
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let runtime = PodRuntime::new(
        identity(1, "double-create"),
        Arc::new(DoubleCreateService {
            factory_calls: factory_calls.clone(),
        }),
        Arc::new(MemoryAuthorityStore::default()),
    );
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::New)))
        .await
        .unwrap();
    assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
}

fn authority(local: ReplicaIdentity, members: Vec<ReplicaIdentity>) -> AdmittedAuthority {
    let write_quorum = members.len() as u32 / 2 + 1;
    let configuration = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        members[0].replica_id,
        members
            .into_iter()
            .enumerate()
            .map(|(index, identity)| ConfigurationMember {
                identity,
                role: if index == 0 {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        write_quorum,
    );
    AdmittedAuthority {
        local_identity: local,
        transition_kind: None,
        previous_configuration: None,
        current_configuration: configuration,
    }
}

fn effect(sequence: u64, action: RuntimeEffectAction) -> RuntimeEffect {
    RuntimeEffect {
        operation_id: OperationId::new(format!("effect-{sequence}")),
        sequence,
        action,
    }
}

fn acknowledgement(
    authority: &AdmittedAuthority,
    receiver: ReplicaIdentity,
    lsn: i64,
) -> proto::ReplicationAck {
    proto::ReplicationAck {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        sender: Some(authority.primary_identity().clone().into()),
        receiver: Some(receiver.into()),
        epoch: Some(authority.current_configuration.epoch.into()),
        previous_configuration_id: authority
            .previous_configuration
            .as_ref()
            .map_or_else(String::new, |configuration| {
                configuration.configuration_id.to_string()
            }),
        current_configuration_id: authority.current_configuration.configuration_id.to_string(),
        received_lsn: lsn,
        applied_lsn: lsn,
        committed_lsn: 0,
        ..Default::default()
    }
}

#[tokio::test]
async fn direct_writes_require_primary_role_and_explicit_write_grant() {
    let local = identity(1, "single");
    let application = Arc::new(TestApplication::default());
    let store = Arc::new(MemoryAuthorityStore::default());
    let runtime = PodRuntime::new(local.clone(), application.clone(), store);

    assert!(matches!(
        runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("closed"),
                data: Bytes::from_static(b"closed")
            })
            .await,
        Err(RuntimeError::NotOpen)
    ));
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority(
                local.clone(),
                vec![local.clone()],
            ))),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    let primary_snapshot = runtime.snapshot().await;
    assert_eq!(primary_snapshot.role, ReplicaRole::Primary);
    assert_eq!(
        primary_snapshot.write_status,
        AccessStatus::ReconfigurationPending
    );

    assert!(matches!(
        runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("still-closed"),
                data: Bytes::from_static(b"still-closed")
            })
            .await,
        Err(RuntimeError::WriteClosed(
            AccessStatus::ReconfigurationPending
        ))
    ));

    runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let pending = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("committed"),
            data: Bytes::from_static(b"committed"),
        })
        .await
        .unwrap();
    assert!(pending.replication_items.is_empty());
    assert_eq!(
        application.durable_progress().await.unwrap().committed_lsn,
        1
    );
    assert_eq!(pending.committed().await.unwrap().committed_lsn, 1);

    runtime
        .apply_effect(effect(
            5,
            RuntimeEffectAction::SetAccessStatus {
                read: AccessStatus::Granted,
                write: AccessStatus::NoWriteQuorum,
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("no-quorum"),
                data: Bytes::from_static(b"no-quorum"),
            })
            .await,
        Err(RuntimeError::WriteClosed(AccessStatus::NoWriteQuorum))
    ));
}

#[tokio::test]
async fn partition_contract_reports_independent_access_load_and_fault() {
    let application = Arc::new(TestApplication::default());
    let runtime = PodRuntime::new(
        identity(1, "partition-contract"),
        application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
    );
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    let partition = application.partition.lock().unwrap().clone().unwrap();
    assert!(
        !partition
            .get_partition_information()
            .partition_id
            .is_empty()
    );
    assert_eq!(
        partition.get_read_status().await.unwrap(),
        AccessStatus::NotPrimary
    );
    assert_eq!(
        partition.get_write_status().await.unwrap(),
        AccessStatus::NotPrimary
    );
    partition
        .report_load(vec![LoadMetric {
            name: "queue-depth".into(),
            value: 3,
        }])
        .await
        .unwrap();
    partition.report_fault(FaultType::Transient).await.unwrap();
    let report = runtime.partition_report().await;
    assert_eq!(report.load_metrics[0].name, "queue-depth");
    assert_eq!(report.reported_fault, Some(FaultType::Transient));
    assert!(
        partition
            .report_load(vec![
                LoadMetric {
                    name: "duplicate".into(),
                    value: 1,
                },
                LoadMetric {
                    name: "duplicate".into(),
                    value: 2,
                },
            ])
            .await
            .is_err()
    );
    runtime
        .apply_effect(effect(2, RuntimeEffectAction::Close))
        .await
        .unwrap();
    assert!(partition.report_load(Vec::new()).await.is_err());
    assert!(partition.report_fault(FaultType::Permanent).await.is_err());
}

#[tokio::test]
async fn local_write_retries_the_same_lsn_after_definite_or_ambiguous_failure() {
    let local = identity(1, "single");
    let application = Arc::new(TestApplication::default());
    let store = Arc::new(MemoryAuthorityStore::default());
    let runtime = Arc::new(PodRuntime::new(
        local.clone(),
        application.clone(),
        store.clone(),
    ));
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority(local.clone(), vec![local]))),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();

    let first = ClientWrite {
        operation_id: OperationId::new("first"),
        data: Bytes::from_static(b"first"),
    };
    application.fail_apply.store(true, Ordering::SeqCst);
    assert!(matches!(
        runtime.data_plane().begin_write(first.clone()).await,
        Err(RuntimeError::Application(_))
    ));
    application.fail_apply.store(false, Ordering::SeqCst);
    let retry = runtime.data_plane().begin_write(first).await.unwrap();
    assert_eq!(retry.lsn, 1);
    retry.committed().await.unwrap();

    let second = ClientWrite {
        operation_id: OperationId::new("second"),
        data: Bytes::from_static(b"second"),
    };
    application.fail_after_apply.store(true, Ordering::SeqCst);
    assert!(matches!(
        runtime.data_plane().begin_write(second.clone()).await,
        Err(RuntimeError::Application(_))
    ));
    assert_eq!(application.durable_progress().await.unwrap().applied_lsn, 2);
    let retry = runtime.data_plane().begin_write(second).await.unwrap();
    assert_eq!(retry.lsn, 2);
    retry.committed().await.unwrap();

    let third = ClientWrite {
        operation_id: OperationId::new("third"),
        data: Bytes::from_static(b"third"),
    };
    application.pause_after_apply.store(true, Ordering::SeqCst);
    let runtime_task = runtime.clone();
    let third_task =
        tokio::spawn(async move { runtime_task.data_plane().begin_write(third.clone()).await });
    application.applied_notify.notified().await;
    third_task.abort();
    assert!(matches!(third_task.await, Err(error) if error.is_cancelled()));
    application.pause_after_apply.store(false, Ordering::SeqCst);
    application.resume_notify.notify_waiters();
    let retry = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("third"),
            data: Bytes::from_static(b"third"),
        })
        .await
        .unwrap();
    assert_eq!(retry.lsn, 3);
    retry.committed().await.unwrap();

    let fourth = ClientWrite {
        operation_id: OperationId::new("fourth"),
        data: Bytes::from_static(b"fourth"),
    };
    application.fail_commit.store(true, Ordering::SeqCst);
    assert!(matches!(
        runtime.data_plane().begin_write(fourth.clone()).await,
        Err(RuntimeError::Application(_))
    ));
    let retry = runtime.data_plane().begin_write(fourth).await.unwrap();
    assert_eq!(retry.lsn, 4);
    retry.committed().await.unwrap();

    let fifth = ClientWrite {
        operation_id: OperationId::new("fifth"),
        data: Bytes::from_static(b"fifth"),
    };
    application.pause_commit.store(true, Ordering::SeqCst);
    let runtime_task = runtime.clone();
    let fifth_task =
        tokio::spawn(async move { runtime_task.data_plane().begin_write(fifth).await });
    application.commit_notify.notified().await;
    fifth_task.abort();
    assert!(matches!(fifth_task.await, Err(error) if error.is_cancelled()));
    application.pause_commit.store(false, Ordering::SeqCst);
    application.resume_commit_notify.notify_waiters();
    let retry = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("fifth"),
            data: Bytes::from_static(b"fifth"),
        })
        .await
        .unwrap();
    assert_eq!(retry.lsn, 5);
    retry.committed().await.unwrap();

    let sixth = ClientWrite {
        operation_id: OperationId::new("sixth"),
        data: Bytes::from_static(b"sixth"),
    };
    store
        .fail_registered_write_once
        .store(true, Ordering::SeqCst);
    assert!(matches!(
        runtime.data_plane().begin_write(sixth.clone()).await,
        Err(RuntimeError::Application(_))
    ));
    assert!(matches!(
        runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new("blocked-by-sixth"),
                data: Bytes::from_static(b"blocked"),
            })
            .await,
        Err(RuntimeError::LocalWritePending(_))
    ));
    let retry = runtime.data_plane().begin_write(sixth).await.unwrap();
    assert_eq!(retry.lsn, 6);
    retry.committed().await.unwrap();

    let seventh = ClientWrite {
        operation_id: OperationId::new("seventh"),
        data: Bytes::from_static(b"seventh"),
    };
    store.pause_registered_write.store(true, Ordering::SeqCst);
    let runtime_task = runtime.clone();
    let seventh_task =
        tokio::spawn(async move { runtime_task.data_plane().begin_write(seventh).await });
    store.registered_write_notify.notified().await;
    seventh_task.abort();
    assert!(matches!(seventh_task.await, Err(error) if error.is_cancelled()));
    store.pause_registered_write.store(false, Ordering::SeqCst);
    store.resume_registered_write_notify.notify_waiters();
    let retry = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("seventh"),
            data: Bytes::from_static(b"seventh"),
        })
        .await
        .unwrap();
    assert_eq!(retry.lsn, 7);
    retry.committed().await.unwrap();

    let eighth = ClientWrite {
        operation_id: OperationId::new("eighth"),
        data: Bytes::from_static(b"eighth"),
    };
    store.pause_committed_write.store(true, Ordering::SeqCst);
    let runtime_task = runtime.clone();
    let eighth_task =
        tokio::spawn(async move { runtime_task.data_plane().begin_write(eighth).await });
    store.committed_write_notify.notified().await;
    eighth_task.abort();
    assert!(matches!(eighth_task.await, Err(error) if error.is_cancelled()));
    store.pause_committed_write.store(false, Ordering::SeqCst);
    store.resume_committed_write_notify.notify_waiters();
    let retry = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("eighth"),
            data: Bytes::from_static(b"eighth"),
        })
        .await
        .unwrap();
    assert_eq!(retry.lsn, 8);
    retry.committed().await.unwrap();
    let ninth = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("ninth"),
            data: Bytes::from_static(b"ninth"),
        })
        .await
        .unwrap();
    assert_eq!(ninth.lsn, 9);
    ninth.committed().await.unwrap();
}

#[tokio::test]
async fn receiver_ack_requires_durable_authority_and_application_acceptance() {
    let primary = identity(1, "primary");
    let local = identity(2, "secondary");
    let admitted = authority(
        local.clone(),
        vec![primary.clone(), local.clone(), identity(3, "third")],
    );
    let application = Arc::new(TestApplication::default());
    let store = Arc::new(MemoryAuthorityStore::default());
    let runtime = PodRuntime::new(local.clone(), application.clone(), store.clone());
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    let item = proto::ReplicationItem {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        sender: Some(primary.into()),
        epoch: Some(admitted.current_configuration.epoch.into()),
        previous_configuration_id: String::new(),
        current_configuration_id: admitted.current_configuration.configuration_id.to_string(),
        lsn: 1,
        committed_lsn: 0,
        data: b"value".to_vec(),
        receiver: Some(local.clone().into()),
        ..Default::default()
    };

    assert!(matches!(
        runtime.data_plane().receive_replication(item.clone()).await,
        Err(RuntimeError::AuthorityNotAdmitted)
    ));
    assert!(application.applied.lock().unwrap().is_empty());

    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(admitted)),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::ActiveSecondary),
        ))
        .await
        .unwrap();
    assert_eq!(store.admit_count.load(Ordering::SeqCst), 1);

    let mut gap = item.clone();
    gap.lsn = 2;
    gap.data = b"gap".to_vec();
    assert!(matches!(
        runtime.data_plane().receive_replication(gap).await,
        Err(RuntimeError::InvalidReplication(_))
    ));
    assert!(application.applied.lock().unwrap().is_empty());

    application.fail_apply.store(true, Ordering::SeqCst);
    assert!(matches!(
        runtime
            .data_plane()
            .receive_replication(item.clone())
            .await
            .unwrap()
            .applied()
            .await,
        Err(RuntimeError::Application(_))
    ));
    application.fail_apply.store(false, Ordering::SeqCst);

    let delivery = runtime
        .data_plane()
        .receive_replication(item.clone())
        .await
        .unwrap();
    assert_eq!(delivery.received.received_lsn, 1);
    assert_eq!(delivery.received.applied_lsn, 0);
    let acknowledgement = delivery.applied().await.unwrap();
    assert_eq!(acknowledgement.applied_lsn, 1);
    assert_eq!(application.applied.lock().unwrap().len(), 1);

    let retry = runtime
        .data_plane()
        .receive_replication(item.clone())
        .await
        .unwrap()
        .applied()
        .await
        .unwrap();
    assert_eq!(retry.applied_lsn, 1);
    assert_eq!(application.applied.lock().unwrap().len(), 1);

    let mut conflicting = item;
    conflicting.data = b"different".to_vec();
    assert!(matches!(
        runtime
            .data_plane()
            .receive_replication(conflicting)
            .await
            .unwrap()
            .applied()
            .await,
        Err(RuntimeError::InvalidReplication(_))
    ));

    let mut committed = retry_item(&retry, local.clone());
    committed.lsn = 2;
    committed.committed_lsn = 1;
    committed.data = b"next".to_vec();
    runtime
        .data_plane()
        .receive_replication(committed)
        .await
        .unwrap()
        .applied()
        .await
        .unwrap();
    let snapshot = runtime.snapshot().await;
    assert_eq!(snapshot.current_progress, 2);
    assert_eq!(snapshot.committed_lsn, 1);
    assert_eq!(
        application.durable_progress().await.unwrap().committed_lsn,
        1
    );
    let older = retry_item(&retry, local);
    let acknowledgement = runtime
        .data_plane()
        .receive_replication(older)
        .await
        .unwrap()
        .applied()
        .await
        .unwrap();
    assert_eq!(acknowledgement.received_lsn, 2);
    assert_eq!(acknowledgement.applied_lsn, 2);
    kuberic_wire::validate_replication_ack(&acknowledgement).unwrap();
    assert_eq!(application.applied.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn new_authority_ack_does_not_credit_an_unverified_old_suffix() {
    let primary = identity(1, "primary");
    let members = vec![
        primary.clone(),
        identity(2, "second"),
        identity(3, "third"),
        identity(4, "fourth"),
        identity(5, "fifth"),
    ];
    let primary_authority = authority(primary.clone(), members.clone());
    let primary_application = Arc::new(TestApplication::default());
    primary_application.seed_operation(1, Bytes::from_static(b"one"));
    let primary_runtime = PodRuntime::new(
        primary,
        primary_application,
        Arc::new(MemoryAuthorityStore::default()),
    );
    primary_runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    primary_runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(primary_authority.clone())),
        ))
        .await
        .unwrap();
    primary_runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    primary_runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let pending = primary_runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("new-two"),
            data: Bytes::from_static(b"new-two"),
        })
        .await
        .unwrap();
    let pending_items = pending.replication_items.clone();

    for secondary in members.iter().skip(1).take(2) {
        let application = Arc::new(TestApplication::default());
        application.seed_operation(1, Bytes::from_static(b"one"));
        application.seed_operation(2, Bytes::from_static(b"old-two"));
        let local_authority = AdmittedAuthority {
            local_identity: secondary.clone(),
            ..primary_authority.clone()
        };
        let runtime = PodRuntime::new(
            secondary.clone(),
            application,
            Arc::new(MemoryAuthorityStore::default()),
        );
        runtime
            .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
            .await
            .unwrap();
        runtime
            .apply_effect(effect(
                2,
                RuntimeEffectAction::AdmitAuthority(Box::new(local_authority)),
            ))
            .await
            .unwrap();
        runtime
            .apply_effect(effect(
                3,
                RuntimeEffectAction::ChangeRole(ReplicaRole::ActiveSecondary),
            ))
            .await
            .unwrap();
        let mut replay = pending_items
            .iter()
            .find(|item| {
                item.receiver
                    .as_ref()
                    .is_some_and(|receiver| receiver.replica_id == secondary.replica_id.value())
            })
            .unwrap()
            .clone();
        replay.lsn = 1;
        replay.committed_lsn = 1;
        replay.data = b"one".to_vec();
        let ack = runtime
            .data_plane()
            .receive_replication(replay)
            .await
            .unwrap()
            .applied()
            .await
            .unwrap();
        assert_eq!(ack.applied_lsn, 1);
        primary_runtime
            .data_plane()
            .accept_acknowledgement(ack)
            .await
            .unwrap();

        let conflicting = pending_items
            .iter()
            .find(|item| {
                item.receiver
                    .as_ref()
                    .is_some_and(|receiver| receiver.replica_id == secondary.replica_id.value())
            })
            .unwrap()
            .clone();
        assert!(matches!(
            runtime
                .data_plane()
                .receive_replication(conflicting)
                .await
                .unwrap()
                .applied()
                .await,
            Err(RuntimeError::InvalidReplication(_))
        ));
    }

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), pending.committed())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn failover_does_not_inherit_previous_primary_verification_credit() {
    let old_primary = identity(1, "old-primary");
    let local = identity(2, "local");
    let new_primary = identity(3, "new-primary");
    let fourth = identity(4, "fourth");
    let fifth = identity(5, "fifth");
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        old_primary.replica_id,
        vec![
            ConfigurationMember {
                identity: old_primary.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: local.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: new_primary.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: fourth.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: fifth.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        3,
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        new_primary.replica_id,
        vec![
            ConfigurationMember {
                identity: old_primary,
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: local.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: new_primary,
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: fourth,
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: fifth,
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        3,
    );
    let store = Arc::new(MemoryAuthorityStore::default());
    store
        .record_replication_progress(&ReplicationProgress {
            fence: AuthorityFence {
                epoch: previous.epoch,
                previous_configuration_id: None,
                current_configuration_id: previous.configuration_id.clone(),
            },
            verified_lsn: 2,
        })
        .await
        .unwrap();
    let application = Arc::new(TestApplication::default());
    application.seed_operation(1, Bytes::from_static(b"one"));
    application.seed_operation(2, Bytes::from_static(b"old-two"));
    let runtime = PodRuntime::new(local.clone(), application, store);
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(AdmittedAuthority {
                local_identity: local,
                transition_kind: Some(TransitionKind::Failover),
                previous_configuration: Some(previous),
                current_configuration: current,
            })),
        ))
        .await
        .unwrap();
    assert_eq!(runtime.snapshot().await.verified_replication_lsn, Some(0));
    runtime
        .apply_effect(effect(3, RuntimeEffectAction::AuthorizeFailoverPrefix(1)))
        .await
        .unwrap();
    assert_eq!(runtime.snapshot().await.verified_replication_lsn, Some(1));
}

fn retry_item(
    acknowledgement: &proto::ReplicationAck,
    receiver: ReplicaIdentity,
) -> proto::ReplicationItem {
    proto::ReplicationItem {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        sender: acknowledgement.sender.clone(),
        receiver: Some(receiver.into()),
        epoch: acknowledgement.epoch,
        previous_configuration_id: acknowledgement.previous_configuration_id.clone(),
        current_configuration_id: acknowledgement.current_configuration_id.clone(),
        lsn: acknowledgement.received_lsn,
        committed_lsn: acknowledgement.committed_lsn,
        data: b"value".to_vec(),
        ..Default::default()
    }
}

#[tokio::test]
async fn effects_are_ordered_and_idempotent() {
    let local = identity(1, "single");
    let runtime = PodRuntime::new(
        local,
        Arc::new(TestApplication::default()),
        Arc::new(MemoryAuthorityStore::default()),
    );
    let open = effect(1, RuntimeEffectAction::Open(OpenMode::Existing));
    let first = runtime.apply_effect(open.clone()).await.unwrap();
    let duplicate = runtime.apply_effect(open).await.unwrap();
    assert_eq!(duplicate, first);
    assert!(matches!(
        runtime
            .apply_effect(effect(1, RuntimeEffectAction::Close))
            .await,
        Err(RuntimeError::EffectConflict { sequence: 1 })
    ));

    let close = effect(2, RuntimeEffectAction::Close);
    runtime.apply_effect(close).await.unwrap();
    assert_eq!(
        runtime
            .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing),))
            .await
            .unwrap(),
        first
    );
    assert!(matches!(
        runtime
            .apply_effect(effect(4, RuntimeEffectAction::Open(OpenMode::Existing),))
            .await,
        Err(RuntimeError::EffectOutOfOrder {
            expected: 3,
            observed: 4
        })
    ));
}

#[tokio::test]
async fn caller_supplies_the_control_plane() {
    let local = identity(1, "single");
    let runtime = PodRuntime::new(
        local,
        Arc::new(TestApplication::default()),
        Arc::new(MemoryAuthorityStore::default()),
    );
    let mut control_plane = TestControlPlane {
        effects: VecDeque::from([effect(1, RuntimeEffectAction::Open(OpenMode::Existing))]),
        published: Vec::new(),
    };

    runtime.serve(&mut control_plane).await.unwrap();
    assert_eq!(control_plane.published.len(), 1);
    assert!(control_plane.published[0].postcondition.open);
}

#[tokio::test]
async fn runtime_accepts_custom_primary_replicator_and_exposes_state_replicator_trait() {
    let local = identity(1, "custom");
    let opened = Arc::new(AtomicUsize::new(0));
    let role_changes = Arc::new(AtomicUsize::new(0));
    let epoch_updates = Arc::new(AtomicUsize::new(0));
    let application = Arc::new(TestApplication::default());
    let events = application.events.clone();
    let runtime = runtime_with_factory(
        local.clone(),
        application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
        CountingFactory {
            storage: Arc::downgrade(&application),
            opened: opened.clone(),
            role_changes: role_changes.clone(),
            epoch_updates,
            events: events.clone(),
            fail_change_role: Arc::new(AtomicBool::new(false)),
            fail_close: Arc::new(AtomicBool::new(false)),
        },
    )
    .unwrap();
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::New)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority(local.clone(), vec![local]))),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();

    let state_replicator = application
        .state_replicator
        .lock()
        .unwrap()
        .clone()
        .unwrap();
    assert_eq!(
        state_replicator
            .replicate(Bytes::from_static(b"custom"))
            .await
            .unwrap(),
        1
    );
    assert_eq!(application.streams_taken.load(Ordering::SeqCst), 2);
    assert_eq!(opened.load(Ordering::SeqCst), 1);
    assert_eq!(role_changes.load(Ordering::SeqCst), 1);
    assert_eq!(
        events.lock().unwrap().as_slice(),
        [
            "service.open",
            "replicator.open",
            "replicator.change_role",
            "replicator.update_epoch",
            "provider.update_epoch",
            "service.change_role",
        ]
    );
}

#[tokio::test]
async fn secondary_authority_dispatches_replicator_and_provider_epoch_before_role() {
    let primary = identity(1, "primary");
    let local = identity(2, "secondary");
    let application = Arc::new(TestApplication::default());
    let events = application.events.clone();
    let epoch_updates = Arc::new(AtomicUsize::new(0));
    let runtime = runtime_with_factory(
        local.clone(),
        application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
        CountingFactory {
            storage: Arc::downgrade(&application),
            opened: Arc::new(AtomicUsize::new(0)),
            role_changes: Arc::new(AtomicUsize::new(0)),
            epoch_updates: epoch_updates.clone(),
            events: events.clone(),
            fail_change_role: Arc::new(AtomicBool::new(false)),
            fail_close: Arc::new(AtomicBool::new(false)),
        },
    )
    .unwrap();
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::New)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority(
                local,
                vec![primary, identity(2, "secondary"), identity(3, "third")],
            ))),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::ActiveSecondary),
        ))
        .await
        .unwrap();

    assert_eq!(epoch_updates.load(Ordering::SeqCst), 1);
    assert_eq!(
        events.lock().unwrap().as_slice(),
        [
            "service.open",
            "replicator.open",
            "replicator.update_epoch",
            "provider.update_epoch",
            "replicator.change_role",
            "service.change_role",
        ]
    );
}

#[tokio::test]
async fn failed_or_cancelled_secondary_epoch_admission_stays_write_fenced() {
    let old_primary = identity(1, "old-primary");
    let new_primary = identity(2, "new-primary");
    let third = identity(3, "third");
    let previous = authority(
        old_primary.clone(),
        vec![old_primary.clone(), new_primary.clone(), third.clone()],
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        new_primary.replica_id,
        vec![
            ConfigurationMember {
                identity: old_primary.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: new_primary.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: third,
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    );
    let demoted_authority = AdmittedAuthority {
        local_identity: old_primary.clone(),
        transition_kind: Some(TransitionKind::Failover),
        previous_configuration: Some(previous.current_configuration.clone()),
        current_configuration: current,
    };
    let application = Arc::new(TestApplication::default());
    let runtime = Arc::new(PodRuntime::new(
        old_primary,
        application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
    ));
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(previous)),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let pending = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("old-authority-pending"),
            data: Bytes::from_static(b"pending"),
        })
        .await
        .unwrap();
    let delayed_item = pending
        .replication_items
        .iter()
        .find(|item| {
            item.receiver
                .as_ref()
                .is_some_and(|receiver| receiver.instance_id == new_primary.instance_id.as_str())
        })
        .unwrap()
        .clone();

    application.pause_update_epoch.store(true, Ordering::SeqCst);
    let runtime_task = runtime.clone();
    let authority_for_task = demoted_authority.clone();
    let admission = tokio::spawn(async move {
        runtime_task
            .apply_effect(effect(
                5,
                RuntimeEffectAction::AdmitAuthority(Box::new(authority_for_task)),
            ))
            .await
    });
    application.update_epoch_notify.notified().await;
    assert!(matches!(
        pending.committed().await,
        Err(RuntimeError::WriteClosed(
            AccessStatus::ReconfigurationPending
        ))
    ));
    admission.abort();
    assert!(matches!(admission.await, Err(error) if error.is_cancelled()));
    application
        .pause_update_epoch
        .store(false, Ordering::SeqCst);
    application.resume_update_epoch_notify.notify_waiters();

    application.fail_update_epoch.store(true, Ordering::SeqCst);
    assert!(matches!(
        runtime
            .apply_effect(effect(
                5,
                RuntimeEffectAction::AdmitAuthority(Box::new(demoted_authority)),
            ))
            .await,
        Err(RuntimeError::Application(_))
    ));
    assert!(matches!(
        runtime
            .data_plane()
            .accept_acknowledgement(proto::ReplicationAck {
                protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                sender: delayed_item.sender,
                receiver: delayed_item.receiver,
                epoch: delayed_item.epoch,
                previous_configuration_id: delayed_item.previous_configuration_id,
                current_configuration_id: delayed_item.current_configuration_id,
                received_lsn: delayed_item.lsn,
                applied_lsn: delayed_item.lsn,
                committed_lsn: 0,
                ..Default::default()
            })
            .await,
        Err(RuntimeError::AuthorityMismatch(_))
    ));
    assert_eq!(
        runtime.snapshot().await.write_status,
        AccessStatus::ReconfigurationPending
    );
}

#[tokio::test]
async fn close_fences_pending_client_writes_before_ack_processing() {
    let local = identity(1, "primary");
    let secondary = identity(2, "secondary");
    let third = identity(3, "third");
    let application = Arc::new(TestApplication::default());
    let runtime = Arc::new(PodRuntime::new(
        local.clone(),
        application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
    ));
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority(
                local,
                vec![identity(1, "primary"), secondary, third],
            ))),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let pending = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("pending"),
            data: Bytes::from_static(b"pending"),
        })
        .await
        .unwrap();
    let item = pending.replication_items[0].clone();

    application.pause_close.store(true, Ordering::SeqCst);
    let runtime_task = runtime.clone();
    let close_task = tokio::spawn(async move {
        runtime_task
            .apply_effect(effect(5, RuntimeEffectAction::Close))
            .await
    });
    application.close_notify.notified().await;
    assert!(matches!(
        pending.committed().await,
        Err(RuntimeError::WriteClosed(
            AccessStatus::ReconfigurationPending
        ))
    ));
    application.pause_close.store(false, Ordering::SeqCst);
    application.resume_close_notify.notify_waiters();
    close_task.await.unwrap().unwrap();
    assert!(matches!(
        runtime
            .data_plane()
            .accept_acknowledgement(proto::ReplicationAck {
                protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                sender: item.sender,
                receiver: item.receiver,
                epoch: item.epoch,
                previous_configuration_id: item.previous_configuration_id,
                current_configuration_id: item.current_configuration_id,
                received_lsn: item.lsn,
                applied_lsn: item.lsn,
                committed_lsn: 0,
                ..Default::default()
            })
            .await,
        Err(RuntimeError::Closed)
    ));
}

#[tokio::test]
async fn failed_demotion_callback_preserves_completed_role_and_transition_stage() {
    let local = identity(1, "primary");
    let application = Arc::new(TestApplication::default());
    let runtime = PodRuntime::new(
        local.clone(),
        application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
    );
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority(
                local,
                vec![
                    identity(1, "primary"),
                    identity(2, "secondary"),
                    identity(3, "third"),
                ],
            ))),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let pending = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("demotion-pending"),
            data: Bytes::from_static(b"pending"),
        })
        .await
        .unwrap();
    application.fail_change_role.store(true, Ordering::SeqCst);
    assert!(matches!(
        runtime
            .apply_effect(effect(
                5,
                RuntimeEffectAction::ChangeRole(ReplicaRole::ActiveSecondary),
            ))
            .await,
        Err(RuntimeError::Application(_))
    ));
    assert!(matches!(
        pending.committed().await,
        Err(RuntimeError::WriteClosed(
            AccessStatus::ReconfigurationPending
        ))
    ));
    let snapshot = runtime.snapshot().await;
    assert_eq!(snapshot.role, ReplicaRole::Primary);
    assert_eq!(
        snapshot.role_transition,
        Some(kuberic_runtime_internal::effects::RoleTransition {
            completed_role: ReplicaRole::Primary,
            target_role: ReplicaRole::ActiveSecondary,
            replicator_completed: true,
            epoch_completed: true,
            application_completed: false,
        })
    );
    assert_eq!(snapshot.write_status, AccessStatus::ReconfigurationPending);
}

#[tokio::test]
async fn injected_replicator_failures_cannot_leave_pending_writes_live() {
    for fail_close in [false, true] {
        let local = identity(
            1,
            if fail_close {
                "close-primary"
            } else {
                "role-primary"
            },
        );
        let fail_change_role = Arc::new(AtomicBool::new(false));
        let fail_close_flag = Arc::new(AtomicBool::new(false));
        let application = Arc::new(TestApplication::default());
        let runtime = runtime_with_factory(
            local.clone(),
            application.clone(),
            Arc::new(MemoryAuthorityStore::default()),
            CountingFactory {
                storage: Arc::downgrade(&application),
                opened: Arc::new(AtomicUsize::new(0)),
                role_changes: Arc::new(AtomicUsize::new(0)),
                epoch_updates: Arc::new(AtomicUsize::new(0)),
                events: Arc::new(Mutex::new(Vec::new())),
                fail_change_role: fail_change_role.clone(),
                fail_close: fail_close_flag.clone(),
            },
        )
        .unwrap();
        runtime
            .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
            .await
            .unwrap();
        runtime
            .apply_effect(effect(
                2,
                RuntimeEffectAction::AdmitAuthority(Box::new(authority(
                    local.clone(),
                    vec![local, identity(2, "secondary"), identity(3, "third")],
                ))),
            ))
            .await
            .unwrap();
        runtime
            .apply_effect(effect(
                3,
                RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
            ))
            .await
            .unwrap();
        runtime
            .apply_effect(effect(
                4,
                RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
            ))
            .await
            .unwrap();
        let pending = runtime
            .data_plane()
            .begin_write(ClientWrite {
                operation_id: OperationId::new(if fail_close {
                    "close-failure"
                } else {
                    "role-failure"
                }),
                data: Bytes::from_static(b"pending"),
            })
            .await
            .unwrap();
        let result = if fail_close {
            fail_close_flag.store(true, Ordering::SeqCst);
            runtime
                .apply_effect(effect(5, RuntimeEffectAction::Close))
                .await
        } else {
            fail_change_role.store(true, Ordering::SeqCst);
            runtime
                .apply_effect(effect(
                    5,
                    RuntimeEffectAction::ChangeRole(ReplicaRole::ActiveSecondary),
                ))
                .await
        };
        assert!(matches!(result, Err(RuntimeError::Application(_))));
        if fail_close {
            assert!(matches!(
                pending.committed().await,
                Err(RuntimeError::Closed)
            ));
        } else {
            assert!(matches!(
                pending.committed().await,
                Err(RuntimeError::WriteClosed(
                    AccessStatus::ReconfigurationPending
                ))
            ));
        }
    }
}

#[tokio::test]
async fn exact_target_copy_closes_the_replication_gap_before_completion() {
    let source = identity(1, "source");
    let target = identity(1, "replacement");
    let source_runtime = PodRuntime::new(
        source.clone(),
        Arc::new(TestApplication::default()),
        Arc::new(MemoryAuthorityStore::default()),
    );
    source_runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    source_runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority(
                source.clone(),
                vec![source.clone()],
            ))),
        ))
        .await
        .unwrap();
    source_runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    source_runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    source_runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("one"),
            data: Bytes::from_static(b"one"),
        })
        .await
        .unwrap()
        .committed()
        .await
        .unwrap();
    let mut prepared = prepare_copy_authorized(
        &source_runtime,
        PrepareCopyRequest {
            build_id: OperationId::new("build"),
            target: target.clone(),
            configuration: BuildConfiguration::Current,
            copy_context: empty_copy_context(),
        },
    )
    .await
    .unwrap();
    assert_eq!(prepared.authority.replication_boundary_lsn, 1);
    let items = copy_through_final(&mut prepared).await;
    assert_eq!(items.len(), 3);
    let live_write = source_runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("two"),
            data: Bytes::from_static(b"two"),
        })
        .await
        .unwrap();
    assert!(live_write.build_items.is_empty());
    let live_build_item = next_copy_item(&mut prepared).await;
    live_write.committed().await.unwrap();

    let target_application = Arc::new(TestApplication::default());
    let target_runtime = PodRuntime::new(
        target,
        target_application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
    );
    target_runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    target_runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary),
        ))
        .await
        .unwrap();
    target_runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::AdmitBuildAuthority(Box::new(prepared.authority.clone())),
        ))
        .await
        .unwrap();

    assert!(matches!(
        target_runtime
            .data_plane()
            .receive_copy_item(items[2].clone())
            .await,
        Err(RuntimeError::InvalidReplication(_))
    ));
    let first_ack = target_runtime
        .data_plane()
        .receive_copy_item(items[0].clone())
        .await
        .unwrap();
    source_runtime
        .data_plane()
        .accept_copy_acknowledgement(first_ack.clone())
        .await
        .unwrap();
    assert!(!first_ack.final_item);
    assert_eq!(
        target_application.copy_apply_count.load(Ordering::SeqCst),
        1
    );
    target_runtime
        .data_plane()
        .receive_copy_item(items[0].clone())
        .await
        .unwrap();
    assert_eq!(
        target_application.copy_apply_count.load(Ordering::SeqCst),
        1
    );
    assert_eq!(target_application.applied.lock().unwrap().len(), 0);
    target_runtime
        .data_plane()
        .receive_copy_item(items[1].clone())
        .await
        .unwrap();
    let final_ack = target_runtime
        .data_plane()
        .receive_copy_item(items[2].clone())
        .await
        .unwrap();
    assert_eq!(target_application.applied.lock().unwrap().len(), 1);
    assert!(final_ack.final_item);
    assert_eq!(final_ack.durable_lsn, 1);
    let live_ack = target_runtime
        .data_plane()
        .receive_copy_item(live_build_item)
        .await
        .unwrap();
    let mut forged_ack = live_ack.clone();
    forged_ack.durable_lsn = 100;
    assert!(matches!(
        source_runtime
            .data_plane()
            .accept_copy_acknowledgement(forged_ack)
            .await,
        Err(RuntimeError::InvalidReplication(_))
    ));
    source_runtime
        .data_plane()
        .accept_copy_acknowledgement(live_ack.clone())
        .await
        .unwrap();
    assert!(!source_runtime.snapshot().await.builds[0].completed);
    source_runtime
        .data_plane()
        .accept_copy_acknowledgement(final_ack)
        .await
        .unwrap();
    let retried_final = target_runtime
        .data_plane()
        .receive_copy_item(items[2].clone())
        .await
        .unwrap();
    assert_eq!(retried_final.durable_lsn, 1);
    source_runtime
        .data_plane()
        .accept_copy_acknowledgement(retried_final)
        .await
        .unwrap();
    let retried_chunk = target_runtime
        .data_plane()
        .receive_copy_item(items[0].clone())
        .await
        .unwrap();
    assert!(retried_chunk.snapshot_chunk);
    assert_eq!(retried_chunk.durable_lsn, 0);
    source_runtime
        .data_plane()
        .accept_copy_acknowledgement(retried_chunk)
        .await
        .unwrap();
    let mut conflicting_chunk = items[0].clone();
    conflicting_chunk.data.push(0xff);
    assert!(matches!(
        target_runtime
            .data_plane()
            .receive_copy_item(conflicting_chunk)
            .await,
        Err(RuntimeError::InvalidReplication(_))
    ));
    assert_eq!(
        target_application.copy_apply_count.load(Ordering::SeqCst),
        2
    );
    let snapshot = target_runtime.snapshot().await;
    assert_eq!(snapshot.current_progress, 2);
    assert!(snapshot.builds[0].completed);
    let source_snapshot = source_runtime.snapshot().await;
    assert!(source_snapshot.builds[0].completed);
    assert_eq!(source_snapshot.builds[0].durable_lsn, 2);
}

#[tokio::test]
async fn copy_context_and_snapshot_stream_without_blocking_primary_writes() {
    let application = Arc::new(TestApplication::default());
    application.seed_operation(1, Bytes::from_static(b"seed"));
    application
        .pause_copy_enumeration
        .store(true, Ordering::SeqCst);
    let runtime = open_primary(application.clone(), vec![identity(1, "source")]).await;
    let mut prepared = prepare_copy_authorized(
        &runtime,
        PrepareCopyRequest {
            build_id: OperationId::new("streaming-copy"),
            target: identity(1, "replacement"),
            configuration: BuildConfiguration::Current,
            copy_context: Box::pin(stream::iter([
                Ok(Bytes::from_static(b"context-1")),
                Ok(Bytes::from_static(b"context-2")),
            ])),
        },
    )
    .await
    .unwrap();
    application.copy_enumeration_notify.notified().await;
    let pending = timeout(
        Duration::from_secs(1),
        runtime.data_plane().begin_write(ClientWrite {
            operation_id: OperationId::new("write-during-copy"),
            data: Bytes::from_static(b"live"),
        }),
    )
    .await
    .unwrap()
    .unwrap();
    pending.committed().await.unwrap();
    assert_eq!(
        application.copy_context_items.lock().unwrap().as_slice(),
        [
            Bytes::from_static(b"context-1"),
            Bytes::from_static(b"context-2")
        ]
    );
    application
        .pause_copy_enumeration
        .store(false, Ordering::SeqCst);
    application.resume_copy_enumeration_notify.notify_waiters();
    let snapshot = copy_through_final(&mut prepared).await;
    assert_eq!(
        snapshot.iter().filter(|item| item.snapshot_chunk).count(),
        2
    );
    let live = next_copy_item(&mut prepared).await;
    assert_eq!(live.lsn, 2);
    assert!(!live.snapshot_chunk);
    assert!(!live.final_item);
}

#[tokio::test]
async fn source_copy_requires_agent_admitted_build_authority() {
    let runtime = open_primary(
        Arc::new(TestApplication::default()),
        vec![identity(1, "unauthorized-copy-source")],
    )
    .await;
    assert!(matches!(
        runtime
            .data_plane()
            .prepare_copy(PrepareCopyRequest {
                build_id: OperationId::new("unauthorized-copy"),
                target: identity(1, "unauthorized-copy-target"),
                configuration: BuildConfiguration::Current,
                copy_context: empty_copy_context(),
            })
            .await,
        Err(RuntimeError::AuthorityNotAdmitted)
    ));
}

#[tokio::test]
async fn retained_gap_enumeration_does_not_block_primary_writes() {
    let local = identity(1, "retained-gap-source");
    let target = identity(1, "retained-gap-target");
    let application = Arc::new(TestApplication::default());
    application.seed_operation(1, Bytes::from_static(b"seed"));
    let store = Arc::new(MemoryAuthorityStore::default());
    let admitted = authority(local.clone(), vec![local.clone()]);
    let runtime = Arc::new(PodRuntime::new(
        local.clone(),
        application.clone(),
        store.clone(),
    ));
    for (index, action) in [
        RuntimeEffectAction::Open(OpenMode::Existing),
        RuntimeEffectAction::AdmitAuthority(Box::new(admitted.clone())),
        RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
    ]
    .into_iter()
    .enumerate()
    {
        runtime
            .apply_effect(effect(index as u64 + 1, action))
            .await
            .unwrap();
    }
    let build_id = OperationId::new("retained-gap-copy");
    store
        .admit_build(&BuildAuthority {
            build_id: build_id.clone(),
            kind: BuildAuthorityKind::Provisioning,
            source: local,
            target: target.clone(),
            current_configuration: admitted.current_configuration,
            replication_boundary_lsn: 0,
        })
        .await
        .unwrap();
    application
        .pause_retained_enumeration
        .store(true, Ordering::SeqCst);
    let cancelled_runtime = runtime.clone();
    let cancelled_build_id = build_id.clone();
    let cancelled_target = target.clone();
    let cancelled = tokio::spawn(async move {
        prepare_copy_authorized(
            &cancelled_runtime,
            PrepareCopyRequest {
                build_id: cancelled_build_id,
                target: cancelled_target,
                configuration: BuildConfiguration::Current,
                copy_context: empty_copy_context(),
            },
        )
        .await
    });
    application.retained_enumeration_notify.notified().await;
    cancelled.abort();
    assert!(matches!(cancelled.await, Err(error) if error.is_cancelled()));
    tokio::task::yield_now().await;
    assert!(runtime.snapshot().await.builds.is_empty());

    let runtime_for_copy = runtime.clone();
    let prepare = tokio::spawn(async move {
        prepare_copy_authorized(
            &runtime_for_copy,
            PrepareCopyRequest {
                build_id,
                target,
                configuration: BuildConfiguration::Current,
                copy_context: empty_copy_context(),
            },
        )
        .await
    });
    application.retained_enumeration_notify.notified().await;
    let pending = timeout(
        Duration::from_secs(1),
        runtime.data_plane().begin_write(ClientWrite {
            operation_id: OperationId::new("write-during-gap-scan"),
            data: Bytes::from_static(b"live"),
        }),
    )
    .await
    .unwrap()
    .unwrap();
    pending.committed().await.unwrap();
    application
        .pause_retained_enumeration
        .store(false, Ordering::SeqCst);
    application
        .resume_retained_enumeration_notify
        .notify_waiters();

    let mut prepared = prepare.await.unwrap().unwrap();
    let _snapshot = copy_through_final(&mut prepared).await;
    assert_eq!(next_copy_item(&mut prepared).await.lsn, 1);
    assert_eq!(next_copy_item(&mut prepared).await.lsn, 2);
}

#[tokio::test]
async fn dropping_returned_copy_stream_cancels_paused_provider_and_releases_build() {
    let application = Arc::new(TestApplication::default());
    application.seed_operation(1, Bytes::from_static(b"seed"));
    application
        .pause_copy_enumeration
        .store(true, Ordering::SeqCst);
    let runtime = open_primary(application.clone(), vec![identity(1, "copy-cancel")]).await;
    let request = || PrepareCopyRequest {
        build_id: OperationId::new("cancel-returned-copy"),
        target: identity(1, "copy-cancel-target"),
        configuration: BuildConfiguration::Current,
        copy_context: empty_copy_context(),
    };
    let prepared = prepare_copy_authorized(&runtime, request()).await.unwrap();
    application.copy_enumeration_notify.notified().await;
    drop(prepared);
    timeout(Duration::from_secs(1), async {
        loop {
            if runtime.snapshot().await.builds.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    application
        .pause_copy_enumeration
        .store(false, Ordering::SeqCst);
    application.resume_copy_enumeration_notify.notify_waiters();
    let _retry = prepare_copy_authorized(&runtime, request()).await.unwrap();
}

#[tokio::test]
async fn completed_copy_hands_off_to_new_authority_and_normal_quorum_replication() {
    let source = identity(1, "source");
    let secondary = identity(2, "secondary");
    let old = identity(3, "old");
    let replacement = identity(3, "replacement");
    let previous_authority =
        authority(source.clone(), vec![source.clone(), secondary.clone(), old]);
    let source_application = Arc::new(TestApplication::default());
    source_application.seed_operation(1, Bytes::from_static(b"one"));
    let source_runtime = PodRuntime::new(
        source.clone(),
        source_application,
        Arc::new(MemoryAuthorityStore::default()),
    );
    source_runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    source_runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(previous_authority.clone())),
        ))
        .await
        .unwrap();
    source_runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    let mut prepared = prepare_copy_authorized(
        &source_runtime,
        PrepareCopyRequest {
            build_id: OperationId::new("handoff-build"),
            target: replacement.clone(),
            configuration: BuildConfiguration::Current,
            copy_context: empty_copy_context(),
        },
    )
    .await
    .unwrap();
    let items = copy_through_final(&mut prepared).await;

    let target_application = Arc::new(TestApplication::default());
    let target_store = Arc::new(MemoryAuthorityStore::default());
    let target_runtime = PodRuntime::new(replacement.clone(), target_application, target_store);
    target_runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::New)))
        .await
        .unwrap();
    target_runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary),
        ))
        .await
        .unwrap();
    target_runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::AdmitBuildAuthority(Box::new(prepared.authority.clone())),
        ))
        .await
        .unwrap();
    for item in items {
        target_runtime
            .data_plane()
            .receive_copy_item(item)
            .await
            .unwrap();
    }

    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        source.replica_id,
        vec![
            ConfigurationMember {
                identity: source.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: secondary.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: replacement.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    );
    let new_source_authority = AdmittedAuthority {
        local_identity: source.clone(),
        transition_kind: Some(TransitionKind::Replacement),
        previous_configuration: Some(previous_authority.current_configuration.clone()),
        current_configuration: current.clone(),
    };
    let new_target_authority = AdmittedAuthority {
        local_identity: replacement.clone(),
        ..new_source_authority.clone()
    };
    target_runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::AdmitAuthority(Box::new(new_target_authority)),
        ))
        .await
        .unwrap();
    target_runtime
        .apply_effect(effect(
            5,
            RuntimeEffectAction::ChangeRole(ReplicaRole::ActiveSecondary),
        ))
        .await
        .unwrap();
    assert_eq!(
        target_runtime.snapshot().await.verified_replication_lsn,
        Some(1)
    );

    let secondary_application = Arc::new(TestApplication::default());
    secondary_application.seed_operation(1, Bytes::from_static(b"one"));
    let secondary_store = Arc::new(MemoryAuthorityStore::default());
    secondary_store
        .record_replication_progress(&ReplicationProgress {
            fence: previous_authority.fence(),
            verified_lsn: 1,
        })
        .await
        .unwrap();
    let secondary_runtime =
        PodRuntime::new(secondary.clone(), secondary_application, secondary_store);
    secondary_runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    secondary_runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(AdmittedAuthority {
                local_identity: secondary.clone(),
                ..new_source_authority.clone()
            })),
        ))
        .await
        .unwrap();
    secondary_runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::ActiveSecondary),
        ))
        .await
        .unwrap();

    source_runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::AdmitAuthority(Box::new(new_source_authority.clone())),
        ))
        .await
        .unwrap();
    source_runtime
        .apply_effect(effect(
            5,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let pending = source_runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("two"),
            data: Bytes::from_static(b"two"),
        })
        .await
        .unwrap();
    for item in pending.replication_items.clone() {
        let receiver = item.receiver.as_ref().unwrap();
        let ack = if receiver.instance_id == replacement.instance_id.as_str() {
            target_runtime
                .data_plane()
                .receive_replication(item)
                .await
                .unwrap()
                .applied()
                .await
                .unwrap()
        } else if receiver.instance_id == secondary.instance_id.as_str() {
            secondary_runtime
                .data_plane()
                .receive_replication(item)
                .await
                .unwrap()
                .applied()
                .await
                .unwrap()
        } else {
            continue;
        };
        source_runtime
            .data_plane()
            .accept_acknowledgement(ack)
            .await
            .unwrap();
    }
    assert_eq!(pending.committed().await.unwrap().committed_lsn, 2);

    target_runtime
        .apply_effect(effect(
            6,
            RuntimeEffectAction::AdmitAuthority(Box::new(AdmittedAuthority {
                local_identity: replacement.clone(),
                transition_kind: None,
                previous_configuration: None,
                current_configuration: current.clone(),
            })),
        ))
        .await
        .unwrap();
    assert_eq!(
        target_runtime.snapshot().await.verified_replication_lsn,
        Some(2)
    );
    source_runtime
        .apply_effect(effect(
            6,
            RuntimeEffectAction::AdmitAuthority(Box::new(AdmittedAuthority {
                local_identity: source,
                transition_kind: None,
                previous_configuration: None,
                current_configuration: current,
            })),
        ))
        .await
        .unwrap();
    source_runtime
        .apply_effect(effect(
            7,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let pending = source_runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("three"),
            data: Bytes::from_static(b"three"),
        })
        .await
        .unwrap();
    let item = pending
        .replication_items
        .iter()
        .find(|item| {
            item.receiver
                .as_ref()
                .is_some_and(|receiver| receiver.instance_id == replacement.instance_id.as_str())
        })
        .unwrap()
        .clone();
    let ack = target_runtime
        .data_plane()
        .receive_replication(item)
        .await
        .unwrap()
        .applied()
        .await
        .unwrap();
    source_runtime
        .data_plane()
        .accept_acknowledgement(ack)
        .await
        .unwrap();
    assert_eq!(pending.committed().await.unwrap().committed_lsn, 3);
}

#[tokio::test]
async fn bootstrap_primary_builds_full_genesis_members_before_configuration_admission() {
    let primary = identity(1, "primary");
    let secondary = identity(2, "secondary");
    let genesis = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        primary.replica_id,
        vec![
            ConfigurationMember {
                identity: primary.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: secondary.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: identity(3, "third"),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    );
    let runtime = PodRuntime::new(
        primary,
        Arc::new(TestApplication::default()),
        Arc::new(MemoryAuthorityStore::default()),
    );
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::New)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();

    let mut prepared = prepare_copy_authorized(
        &runtime,
        PrepareCopyRequest {
            build_id: OperationId::new("bootstrap-secondary"),
            target: secondary,
            configuration: BuildConfiguration::Bootstrap(genesis),
            copy_context: empty_copy_context(),
        },
    )
    .await
    .unwrap();
    assert_eq!(prepared.authority.kind, BuildAuthorityKind::Bootstrap);
    assert_eq!(prepared.authority.replication_boundary_lsn, 0);
    let items = copy_through_final(&mut prepared).await;
    assert_eq!(items.len(), 1);
    assert!(items[0].final_item);
}

#[tokio::test]
async fn build_identity_is_immutable_and_recoverable_on_source_restart() {
    let source = identity(1, "source");
    let target = identity(1, "replacement");
    let admitted = authority(source.clone(), vec![source.clone()]);
    let application = Arc::new(TestApplication::default());
    let store = Arc::new(MemoryAuthorityStore::default());
    let runtime = PodRuntime::new(source.clone(), application.clone(), store.clone());
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(admitted.clone())),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let request = |target| PrepareCopyRequest {
        build_id: OperationId::new("immutable-build"),
        target,
        configuration: BuildConfiguration::Current,
        copy_context: empty_copy_context(),
    };
    let mut first = prepare_copy_authorized(&runtime, request(target.clone()))
        .await
        .unwrap();
    assert_eq!(first.authority.replication_boundary_lsn, 0);
    let _ = copy_through_final(&mut first).await;
    runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("after-boundary"),
            data: Bytes::from_static(b"after-boundary"),
        })
        .await
        .unwrap()
        .committed()
        .await
        .unwrap();
    let live = next_copy_item(&mut first).await;
    assert_eq!(live.lsn, 1);
    assert!(!live.final_item);
    assert!(matches!(
        prepare_copy_authorized(&runtime, request(target.clone())).await,
        Err(RuntimeError::ReconfigurationPending)
    ));

    assert!(matches!(
        prepare_copy_authorized(&runtime, request(identity(2, "different-target"))).await,
        Err(RuntimeError::AuthorityMismatch(_))
    ));

    let restarted = PodRuntime::new(source, application, store);
    restarted
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    restarted
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(admitted)),
        ))
        .await
        .unwrap();
    restarted
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    let mut resumed = prepare_copy_authorized(&restarted, request(target))
        .await
        .unwrap();
    assert_eq!(resumed.authority.replication_boundary_lsn, 0);
    let _ = copy_through_final(&mut resumed).await;
    let resumed_operation = next_copy_item(&mut resumed).await;
    assert_eq!(resumed_operation.lsn, 1);
    assert!(!resumed_operation.final_item);
}

#[tokio::test]
async fn retrying_accepted_write_does_not_overwrite_build_final_sequence() {
    let source = identity(1, "source");
    let target = identity(1, "replacement");
    let application = Arc::new(TestApplication::default());
    let runtime = PodRuntime::new(
        source.clone(),
        application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
    );
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority(source.clone(), vec![source]))),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let write = ClientWrite {
        operation_id: OperationId::new("ambiguous-before-build"),
        data: Bytes::from_static(b"value"),
    };
    application.fail_after_apply.store(true, Ordering::SeqCst);
    assert!(matches!(
        runtime.data_plane().begin_write(write.clone()).await,
        Err(RuntimeError::Application(_))
    ));
    let mut prepared = prepare_copy_authorized(
        &runtime,
        PrepareCopyRequest {
            build_id: OperationId::new("final-sequence"),
            target,
            configuration: BuildConfiguration::Current,
            copy_context: empty_copy_context(),
        },
    )
    .await
    .unwrap();
    let final_item = copy_through_final(&mut prepared)
        .await
        .into_iter()
        .find(|item| item.final_item)
        .unwrap();
    let retry = runtime.data_plane().begin_write(write).await.unwrap();
    assert!(retry.build_items.is_empty());
    retry.committed().await.unwrap();
    runtime
        .data_plane()
        .accept_copy_acknowledgement(proto::CopyAck {
            protocol_version: kuberic_protocol::PROTOCOL_VERSION,
            build_id: final_item.build_id,
            sender: final_item.sender,
            receiver: final_item.receiver,
            epoch: final_item.epoch,
            current_configuration_id: final_item.current_configuration_id,
            sequence: final_item.sequence,
            durable_lsn: final_item.replication_boundary_lsn,
            replication_boundary_lsn: final_item.replication_boundary_lsn,
            final_item: true,
            snapshot_chunk: false,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(runtime.snapshot().await.builds[0].completed);
}

#[tokio::test]
async fn copy_receiver_recovers_sequence_and_final_completion_after_restart() {
    let source = identity(1, "source");
    let target = identity(1, "replacement");
    let source_runtime = PodRuntime::new(
        source.clone(),
        Arc::new(TestApplication::default()),
        Arc::new(MemoryAuthorityStore::default()),
    );
    source_runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    source_runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority(source.clone(), vec![source]))),
        ))
        .await
        .unwrap();
    source_runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    source_runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    source_runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("copy-value"),
            data: Bytes::from_static(b"copy-value"),
        })
        .await
        .unwrap()
        .committed()
        .await
        .unwrap();
    let mut prepared = prepare_copy_authorized(
        &source_runtime,
        PrepareCopyRequest {
            build_id: OperationId::new("restartable-build"),
            target: target.clone(),
            configuration: BuildConfiguration::Current,
            copy_context: empty_copy_context(),
        },
    )
    .await
    .unwrap();
    let items = copy_through_final(&mut prepared).await;

    let application = Arc::new(TestApplication::default());
    let store = Arc::new(MemoryAuthorityStore::default());
    let first_runtime = PodRuntime::new(target.clone(), application.clone(), store.clone());
    first_runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::New)))
        .await
        .unwrap();
    first_runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary),
        ))
        .await
        .unwrap();
    first_runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::AdmitBuildAuthority(Box::new(prepared.authority.clone())),
        ))
        .await
        .unwrap();
    first_runtime
        .data_plane()
        .receive_copy_item(items[0].clone())
        .await
        .unwrap();

    let restarted = PodRuntime::new(target.clone(), application.clone(), store.clone());
    restarted
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    restarted
        .apply_effect(effect(
            2,
            RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary),
        ))
        .await
        .unwrap();
    restarted
        .apply_effect(effect(
            3,
            RuntimeEffectAction::AdmitBuildAuthority(Box::new(prepared.authority.clone())),
        ))
        .await
        .unwrap();
    restarted
        .data_plane()
        .receive_copy_item(items[0].clone())
        .await
        .unwrap();
    restarted
        .data_plane()
        .receive_copy_item(items[1].clone())
        .await
        .unwrap();
    store.fail_build_progress_once.store(true, Ordering::SeqCst);
    assert!(matches!(
        restarted
            .data_plane()
            .receive_copy_item(items[2].clone())
            .await,
        Err(RuntimeError::Application(_))
    ));

    let after_final_crash = PodRuntime::new(target, application, store);
    after_final_crash
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    after_final_crash
        .apply_effect(effect(
            2,
            RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary),
        ))
        .await
        .unwrap();
    after_final_crash
        .apply_effect(effect(
            3,
            RuntimeEffectAction::AdmitBuildAuthority(Box::new(prepared.authority)),
        ))
        .await
        .unwrap();
    let final_ack = after_final_crash
        .data_plane()
        .receive_copy_item(items[2].clone())
        .await
        .unwrap();
    assert!(final_ack.final_item);
    assert!(after_final_crash.snapshot().await.builds[0].completed);
}

#[tokio::test]
async fn runtime_exposes_cc_boundary_catch_up_evidence() {
    let primary = identity(1, "primary");
    let secondary = identity(2, "secondary");
    let old = identity(3, "old");
    let replacement = identity(3, "replacement");
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 5),
        ReplicaId::new(1),
        vec![
            ConfigurationMember {
                identity: primary.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: secondary.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: old,
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 6),
        ReplicaId::new(1),
        vec![
            ConfigurationMember {
                identity: primary.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: secondary.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: replacement,
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    );
    let admitted = AdmittedAuthority {
        local_identity: primary.clone(),
        transition_kind: Some(TransitionKind::Replacement),
        previous_configuration: Some(previous),
        current_configuration: current,
    };
    let application = Arc::new(TestApplication::default());
    application.seed_progress(10);
    let runtime = PodRuntime::new(
        primary,
        application,
        Arc::new(MemoryAuthorityStore::default()),
    );
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(admitted.clone())),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            3,
            RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
        ))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            4,
            RuntimeEffectAction::SetWriteStatus(AccessStatus::Granted),
        ))
        .await
        .unwrap();
    let waiting = runtime.snapshot().await;
    assert_eq!(waiting.catch_up_boundary, Some(10));
    assert!(!waiting.catch_up_complete);
    assert!(matches!(
        runtime
            .apply_effect(effect(
                5,
                RuntimeEffectAction::SetAccessStatus {
                    read: AccessStatus::Granted,
                    write: AccessStatus::ReconfigurationPending,
                },
            ))
            .await,
        Err(RuntimeError::ReconfigurationPending)
    ));
    let pending_after_configuration = runtime
        .data_plane()
        .begin_write(ClientWrite {
            operation_id: OperationId::new("after-catch-up-boundary"),
            data: Bytes::from_static(b"newer-write"),
        })
        .await
        .unwrap();

    let runtime_for_wait = Arc::new(runtime);
    let wait_runtime = runtime_for_wait.clone();
    let managed_wait = tokio::spawn(async move {
        wait_runtime
            .apply_effect(effect(5, RuntimeEffectAction::WaitForCatchup))
            .await
    });
    tokio::task::yield_now().await;
    runtime_for_wait
        .data_plane()
        .accept_acknowledgement(acknowledgement(&admitted, secondary, 10))
        .await
        .unwrap();
    timeout(Duration::from_secs(1), managed_wait)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let complete = runtime_for_wait.snapshot().await;
    assert_eq!(complete.current_configuration_quorum_progress, 10);
    assert!(complete.catch_up_complete);
    runtime_for_wait
        .apply_effect(effect(
            6,
            RuntimeEffectAction::SetAccessStatus {
                read: AccessStatus::Granted,
                write: AccessStatus::ReconfigurationPending,
            },
        ))
        .await
        .unwrap();
    timeout(
        Duration::from_secs(1),
        runtime_for_wait
            .primary_replicator()
            .await
            .unwrap()
            .wait_for_catch_up_quorum(
                kuberic_runtime::replicator::ReplicaSetQuorumMode::WriteQuorum,
            ),
    )
    .await
    .unwrap()
    .unwrap();
    drop(pending_after_configuration);
}

#[tokio::test]
async fn runtime_exposes_derived_must_catch_up_evidence() {
    let old_primary = identity(1, "old-primary");
    let new_primary = identity(2, "new-primary");
    let third = identity(3, "third");
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 5),
        ReplicaId::new(1),
        vec![
            ConfigurationMember {
                identity: old_primary.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: new_primary.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: third.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 6),
        ReplicaId::new(2),
        vec![
            ConfigurationMember {
                identity: old_primary.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: new_primary.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: third.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    );
    let admitted = AdmittedAuthority {
        local_identity: new_primary.clone(),
        transition_kind: Some(TransitionKind::Failover),
        previous_configuration: Some(previous),
        current_configuration: current,
    };
    let application = Arc::new(TestApplication::default());
    application.seed_progress(5);
    let runtime = PodRuntime::new(
        new_primary,
        application.clone(),
        Arc::new(MemoryAuthorityStore::default()),
    );
    runtime
        .apply_effect(effect(1, RuntimeEffectAction::Open(OpenMode::Existing)))
        .await
        .unwrap();
    runtime
        .apply_effect(effect(
            2,
            RuntimeEffectAction::AdmitAuthority(Box::new(admitted.clone())),
        ))
        .await
        .unwrap();
    runtime
        .data_plane()
        .accept_acknowledgement(acknowledgement(&admitted, old_primary, 10))
        .await
        .unwrap();
    runtime
        .data_plane()
        .accept_acknowledgement(acknowledgement(&admitted, third, 10))
        .await
        .unwrap();
    let waiting = runtime.snapshot().await;
    assert_eq!(waiting.current_configuration_quorum_progress, 10);
    assert!(!waiting.catch_up_complete);

    application.seed_progress(10);
    runtime
        .apply_effect(effect(3, RuntimeEffectAction::RefreshApplicationProgress))
        .await
        .unwrap();
    assert!(runtime.snapshot().await.catch_up_complete);
}
