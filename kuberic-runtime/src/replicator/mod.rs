#[doc(hidden)]
pub mod copy;
mod queue;
#[doc(hidden)]
pub mod quorum;
#[doc(hidden)]
pub mod sender;
pub mod stream;

pub(crate) mod log;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use kuberic_protocol::types::{
    AccessStatus, ConfigurationDescriptor, Epoch, FaultType, LoadMetric, OperationId,
    PartitionInformation, ProcessSessionId, ReplicaId, ReplicaIdentity, ReplicaRole,
    SecondaryRemovalPreparation, SecondaryRemovalWitness, SecondaryScaleDownCleanup,
    SecondaryScaleDownIntent,
};
use kuberic_runtime_internal::RuntimeHostToken;
use tokio::sync::{Mutex, RwLock};

use crate::application::{ClientWrite, Lsn, OperationData, StateProvider};
use crate::authority::{
    AdmittedAuthority, BuildAuthority, BuildAuthorityStore, BuildProgressStore, LocalWriteJournal,
    ReplicaAuthorityStore, ReplicationProgressStore, RetiredAuthority,
};
use crate::effects::RuntimeSnapshot;
use crate::engine::DurableState;
use crate::internal::{DefaultReplicatorInner, PendingReplication, PendingWrite};
use crate::replicator::copy::{PrepareCopyRequest, PreparedCopy};
use crate::{Result, RuntimeError};
use kuberic_runtime_internal::transport::{
    CopyAck, CopyItem, OutboundOperation, ReplicationAck, ReplicationItem,
};
use stream::{OperationStream, ServiceStreams};

/// IFabricReplicator, with COM Begin/End pairs collapsed to async calls.
#[async_trait]
pub trait Replicator: Send + Sync {
    async fn open(&self) -> Result<String>;
    async fn change_role(&self, epoch: Epoch, role: ReplicaRole) -> Result<()>;
    async fn update_epoch(&self, epoch: Epoch) -> Result<()>;
    async fn close(&self) -> Result<()>;
    fn abort(&self);
    async fn current_progress(&self) -> Result<Lsn>;
    async fn catch_up_capability(&self) -> Result<Lsn>;
}

/// IFabricPrimaryReplicator; engine bookkeeping is deliberately not part of this API.
#[async_trait]
pub trait PrimaryReplicator: Replicator {
    async fn on_data_loss(&self) -> Result<bool>;
    async fn update_catch_up_replica_set_configuration(
        &self,
        current: ReplicaSetConfiguration,
        previous: ReplicaSetConfiguration,
    ) -> Result<()>;
    async fn wait_for_catch_up_quorum(&self, mode: ReplicaSetQuorumMode) -> Result<()>;
    async fn update_current_replica_set_configuration(
        &self,
        current: ReplicaSetConfiguration,
    ) -> Result<()>;
    async fn build_replica(&self, replica: ReplicaInformation) -> Result<()>;
    async fn remove_replica(&self, replica_id: ReplicaId) -> Result<()>;
}

#[async_trait]
pub trait StateReplicator: Send + Sync {
    /// Completes only after durable local acceptance and the admitted PC/CC write quorums.
    async fn replicate(&self, data: OperationData) -> Result<Lsn>;
    async fn get_replication_stream(&self) -> Result<OperationStream>;
    async fn get_copy_stream(&self) -> Result<OperationStream>;
    async fn update_replicator_settings(&self, settings: ReplicatorSettings) -> Result<()>;
}

#[async_trait]
#[doc(hidden)]
pub trait ManagedReplicatorLifecycle: Send + Sync {
    async fn fence_writes(&self) -> Result<()>;
    async fn settle_primary_prefix(&self) -> Result<()>;
    async fn cancel_configuration_work(&self) -> Result<()>;
    async fn prepare_access(&self, read: AccessStatus, write: AccessStatus) -> Result<u64>;
    async fn publish_access(
        &self,
        read: AccessStatus,
        write: AccessStatus,
        generation: u64,
    ) -> Result<()>;
    async fn admit_authority_proof(&self, authority: AdmittedAuthority) -> Result<()>;
    async fn authorize_failover_prefix_proof(&self, boundary: Lsn) -> Result<()>;
    async fn wait_for_catch_up_proof(&self) -> Result<()>;
    async fn prepare_switchover_proof(
        &self,
        preparation_generation: u64,
        request_id: kuberic_protocol::types::SwitchoverRequestId,
        source: ReplicaIdentity,
        target: ReplicaIdentity,
        starting_configuration_id: kuberic_protocol::types::ConfigurationId,
        starting_epoch: Epoch,
    ) -> Result<()>;
    async fn prepare_secondary_removal_proof(
        &self,
        intent: SecondaryScaleDownIntent,
        process_session_id: ProcessSessionId,
        report_sequence: u64,
    ) -> Result<SecondaryRemovalPreparation>;
    async fn observe_secondary_removal_proof(&self, witness: SecondaryRemovalWitness)
    -> Result<()>;
    async fn observe_secondary_removal_progress_proof(
        &self,
        witness: SecondaryRemovalWitness,
        committed: SecondaryScaleDownCleanup,
    ) -> Result<()>;
    async fn accept_secondary_removal_proof(
        &self,
        committed: SecondaryScaleDownCleanup,
    ) -> Result<()>;
    async fn accept_historical_secondary_removal_proof(
        &self,
        command: kuberic_protocol::command::AcceptSecondaryRemovalCommit,
    ) -> Result<()>;
    async fn fence_retirement_proof(&self, retired: RetiredAuthority) -> Result<()>;
    async fn complete_retirement_proof(&self, retired: RetiredAuthority) -> Result<()>;
    async fn register_peer_session_proof(
        &self,
        identity: ReplicaIdentity,
        session: ProcessSessionId,
    ) -> Result<()>;
    async fn admit_build_authority_proof(&self, authority: BuildAuthority) -> Result<()>;
    async fn retire_build_proof(&self, build_id: OperationId) -> Result<()>;
    async fn build_replica_proof(&self, replica: ReplicaInformation) -> Result<()>;
    async fn remove_replica_proof(&self, replica_id: ReplicaId) -> Result<()>;
    async fn refresh_progress_proof(&self) -> Result<()>;
    async fn restore_engine_proof(&self) -> Result<()>;
    async fn snapshot(&self) -> RuntimeSnapshot;
    async fn cancel_outbound_build(&self, build_id: &OperationId) -> Result<()>;
    async fn complete_open(&self, replication_address: String) -> Result<()>;
    async fn attach_interfaces(
        &self,
        control: Arc<dyn Replicator>,
        primary: Option<Arc<dyn PrimaryReplicator>>,
    ) -> Result<()>;
    fn abort(&self);
}

#[async_trait]
#[doc(hidden)]
pub trait ManagedReplicatorDataPlane: Send + Sync {
    async fn next_outbound_item(&self) -> Option<OutboundOperation>;
    async fn recover_pending_writes(&self) -> Result<()>;
    async fn repair_peer(&self, identity: ReplicaIdentity, progress: Lsn) -> Result<()>;
    async fn begin_write(&self, write: ClientWrite) -> Result<PendingWrite>;
    async fn observe_acknowledgement(
        &self,
        acknowledgement: ReplicationAck,
        session: ProcessSessionId,
    ) -> Result<()>;
    async fn accept_acknowledgement(&self, acknowledgement: ReplicationAck) -> Result<()>;
    async fn prepare_copy(&self, request: PrepareCopyRequest) -> Result<PreparedCopy>;
    async fn accept_copy_acknowledgement(&self, acknowledgement: CopyAck) -> Result<()>;
    async fn receive_copy_item(&self, item: CopyItem) -> Result<CopyAck>;
    async fn receive_replication(&self, item: ReplicationItem) -> Result<PendingReplication>;
    fn abort(&self);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaSetQuorumMode {
    WriteQuorum,
    All,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaInformation {
    pub build_id: OperationId,
    pub identity: ReplicaIdentity,
    pub replication_address: String,
    pub process_session_id: ProcessSessionId,
    pub role: ReplicaRole,
    pub current_progress: Lsn,
    pub catch_up_capability: Lsn,
}

impl ReplicaInformation {
    pub fn new(
        build_id: OperationId,
        identity: ReplicaIdentity,
        replication_address: String,
    ) -> Self {
        Self {
            build_id,
            identity,
            replication_address,
            process_session_id: ProcessSessionId::default(),
            role: ReplicaRole::IdleSecondary,
            current_progress: 0,
            catch_up_capability: 0,
        }
    }
}

/// SF ReplicaSetConfiguration: voting policy plus exact replica descriptions.
/// Idle replicas may be described without belonging to the voting configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaSetConfiguration {
    pub configuration: ConfigurationDescriptor,
    pub replicas: Vec<ReplicaInformation>,
}

impl From<ConfigurationDescriptor> for ReplicaSetConfiguration {
    fn from(configuration: ConfigurationDescriptor) -> Self {
        Self {
            configuration,
            replicas: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplicatorSettings {
    pub replication_address: String,
}

/// Rust's explicit counterpart of obtaining coherent interfaces from CreateReplicator.
pub struct ReplicatorInterfaces {
    replicator: Arc<dyn Replicator>,
    state_replicator: Option<Arc<dyn StateReplicator>>,
    primary_replicator: Option<Arc<dyn PrimaryReplicator>>,
}

impl ReplicatorInterfaces {
    pub fn secondary(
        replicator: Arc<dyn Replicator>,
        state_replicator: Option<Arc<dyn StateReplicator>>,
    ) -> Self {
        Self {
            replicator,
            state_replicator,
            primary_replicator: None,
        }
    }

    pub fn primary<T>(
        primary_replicator: Arc<T>,
        state_replicator: Option<Arc<dyn StateReplicator>>,
    ) -> Self
    where
        T: PrimaryReplicator + 'static,
    {
        let replicator: Arc<dyn Replicator> = primary_replicator.clone();
        let primary_replicator: Arc<dyn PrimaryReplicator> = primary_replicator;
        Self {
            replicator,
            state_replicator,
            primary_replicator: Some(primary_replicator),
        }
    }

    pub fn replicator(&self) -> Arc<dyn Replicator> {
        self.replicator.clone()
    }

    pub fn state_replicator(&self) -> Option<Arc<dyn StateReplicator>> {
        self.state_replicator.clone()
    }

    pub fn primary_replicator(&self) -> Option<Arc<dyn PrimaryReplicator>> {
        self.primary_replicator.clone()
    }
}

#[derive(Clone)]
pub struct ReplicatorFactoryContext {
    identity: ReplicaIdentity,
    access: Arc<dyn PartitionAccessView>,
    registration: Arc<dyn ReplicatorRegistration>,
    reservation: Option<ReplicatorCreationReservation>,
    pub(crate) default_dependencies: Option<DefaultReplicatorDependencies>,
}

impl ReplicatorFactoryContext {
    #[doc(hidden)]
    pub fn new(
        _token: RuntimeHostToken,
        identity: ReplicaIdentity,
        access: Arc<dyn PartitionAccessView>,
        registration: Arc<dyn ReplicatorRegistration>,
        default_dependencies: DefaultReplicatorDependencies,
    ) -> Self {
        Self {
            identity,
            access,
            registration,
            reservation: None,
            default_dependencies: Some(default_dependencies),
        }
    }

    pub fn identity(&self) -> Result<ReplicaIdentity> {
        Ok(self.identity.clone())
    }

    pub async fn write_status(&self) -> Result<AccessStatus> {
        self.access.write_status().await
    }

    pub async fn read_status(&self) -> Result<AccessStatus> {
        self.access.read_status().await
    }

    pub fn partition_information(&self) -> PartitionInformation {
        self.access.partition_information()
    }

    pub async fn report_load(&self, metrics: Vec<LoadMetric>) -> Result<()> {
        self.access.report_load(metrics).await
    }

    pub async fn report_fault(&self, fault: FaultType) -> Result<()> {
        self.access.report_fault(fault).await
    }

    fn for_creation(&self, reservation: ReplicatorCreationReservation) -> Self {
        let mut context = self.clone();
        context.reservation = Some(reservation);
        context
    }

    async fn register_managed<T>(&self, managed: Arc<T>) -> Result<()>
    where
        T: ManagedReplicatorLifecycle + ManagedReplicatorDataPlane + 'static,
    {
        let reservation = self.reservation.ok_or_else(|| {
            RuntimeError::Application("managed replicator registration is not reserved".into())
        })?;
        let lifecycle: Arc<dyn ManagedReplicatorLifecycle> = managed.clone();
        self.registration
            .register_managed_lifecycle(lifecycle, reservation)
            .await?;
        let data_plane: Arc<dyn ManagedReplicatorDataPlane> = managed;
        self.registration
            .register_managed_data_plane(data_plane, reservation)
            .await
    }
}

#[async_trait]
#[doc(hidden)]
pub trait PartitionAccessView: Send + Sync {
    fn partition_information(&self) -> PartitionInformation;
    async fn read_status(&self) -> Result<AccessStatus>;

    async fn write_status(&self) -> Result<AccessStatus>;

    async fn report_load(&self, metrics: Vec<LoadMetric>) -> Result<()>;

    async fn report_fault(&self, fault: FaultType) -> Result<()>;
}

#[doc(hidden)]
#[derive(Clone)]
pub struct DefaultReplicatorDependencies {
    pub replica_authority_store: Arc<dyn ReplicaAuthorityStore>,
    pub replication_progress_store: Arc<dyn ReplicationProgressStore>,
    pub local_write_journal: Arc<dyn LocalWriteJournal>,
    pub build_authority_store: Arc<dyn BuildAuthorityStore>,
    pub build_progress_store: Arc<dyn BuildProgressStore>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct ReplicatorCreationReservation(pub u64);

#[async_trait]
#[doc(hidden)]
pub trait ReplicatorRegistration: Send + Sync {
    fn reserve_replicator_creation(&self) -> Result<ReplicatorCreationReservation>;

    fn cancel_replicator_creation(&self, reservation: ReplicatorCreationReservation);

    async fn register_managed_lifecycle(
        &self,
        lifecycle: Arc<dyn ManagedReplicatorLifecycle>,
        reservation: ReplicatorCreationReservation,
    ) -> Result<()>;

    async fn register_managed_data_plane(
        &self,
        data_plane: Arc<dyn ManagedReplicatorDataPlane>,
        reservation: ReplicatorCreationReservation,
    ) -> Result<()>;

    async fn register_interfaces(
        &self,
        interfaces: &ReplicatorInterfaces,
        provider: Option<Arc<dyn StateProvider>>,
        reservation: ReplicatorCreationReservation,
    ) -> Result<()>;
}

#[async_trait]
pub trait ReplicatorFactory: Send + Sync {
    async fn create_replicator(
        &self,
        context: ReplicatorFactoryContext,
        state_provider: Option<Arc<dyn StateProvider>>,
        settings: ReplicatorSettings,
    ) -> Result<ReplicatorInterfaces>;
}

#[derive(Clone)]
pub struct StatefulServicePartition {
    context: ReplicatorFactoryContext,
    registration: Arc<dyn ReplicatorRegistration>,
    factory: Option<Arc<dyn ReplicatorFactory>>,
}

impl StatefulServicePartition {
    #[doc(hidden)]
    pub fn new(
        _token: RuntimeHostToken,
        registration: Arc<dyn ReplicatorRegistration>,
        context: ReplicatorFactoryContext,
    ) -> Self {
        Self {
            context,
            registration,
            factory: None,
        }
    }

    /// Select an implementation during service Open, not in the PodRuntime constructor.
    pub fn with_factory(&self, factory: Arc<dyn ReplicatorFactory>) -> Self {
        Self {
            context: self.context.clone(),
            registration: self.registration.clone(),
            factory: Some(factory),
        }
    }

    pub async fn get_write_status(&self) -> Result<AccessStatus> {
        self.context.write_status().await
    }

    pub async fn get_read_status(&self) -> Result<AccessStatus> {
        self.context.read_status().await
    }

    pub fn get_partition_information(&self) -> PartitionInformation {
        self.context.partition_information()
    }

    pub async fn report_load(&self, metrics: Vec<LoadMetric>) -> Result<()> {
        self.context.report_load(metrics).await
    }

    pub async fn report_fault(&self, fault: FaultType) -> Result<()> {
        self.context.report_fault(fault).await
    }

    pub async fn create_replicator(
        &self,
        state_provider: Option<Arc<dyn StateProvider>>,
        settings: Option<ReplicatorSettings>,
    ) -> Result<ReplicatorInterfaces> {
        let factory = self.factory.as_ref().ok_or_else(|| {
            RuntimeError::Application("select a replicator factory during Open".into())
        })?;
        let reservation = self.registration.reserve_replicator_creation()?;
        let context = self.context.for_creation(reservation);
        let interfaces = match factory
            .create_replicator(
                context,
                state_provider.clone(),
                settings.unwrap_or_default(),
            )
            .await
        {
            Ok(interfaces) => interfaces,
            Err(error) => {
                self.registration.cancel_replicator_creation(reservation);
                return Err(error);
            }
        };
        if let Err(error) = self
            .registration
            .register_interfaces(&interfaces, state_provider, reservation)
            .await
        {
            interfaces.replicator().abort();
            self.registration.cancel_replicator_creation(reservation);
            return Err(error);
        }
        Ok(interfaces)
    }
}

pub struct DefaultReplicatorFactory {
    storage: Arc<dyn DurableState>,
}

impl DefaultReplicatorFactory {
    pub fn new(storage: Arc<dyn DurableState>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl ReplicatorFactory for DefaultReplicatorFactory {
    async fn create_replicator(
        &self,
        context: ReplicatorFactoryContext,
        state_provider: Option<Arc<dyn StateProvider>>,
        settings: ReplicatorSettings,
    ) -> Result<ReplicatorInterfaces> {
        let state_provider = state_provider.ok_or_else(|| {
            RuntimeError::Application("the default replicator requires a state provider".into())
        })?;
        let streams = Arc::new(ServiceStreams::new());
        let dependencies = context.default_dependencies.clone().ok_or_else(|| {
            RuntimeError::Application(
                "default replicator dependencies are unavailable for this partition".into(),
            )
        })?;
        let engine = DefaultReplicatorInner::new(
            context.identity.clone(),
            dependencies.replica_authority_store,
            dependencies.replication_progress_store,
            dependencies.local_write_journal,
            dependencies.build_authority_store,
            dependencies.build_progress_store,
        );
        engine
            .install_provider(
                state_provider.clone(),
                self.storage.clone(),
                streams.clone(),
            )
            .await?;
        let pending = Arc::new(Mutex::new(None));
        let next_operation = Arc::new(AtomicU64::new(0));
        let replicator = Arc::new(DefaultReplicator {
            engine: engine.clone(),
            provider: state_provider,
            settings: Arc::new(RwLock::new(settings)),
            pending: pending.clone(),
            next_operation: next_operation.clone(),
        });
        let state_replicator = Arc::new(DefaultStateReplicator {
            engine: engine.clone(),
            streams,
            settings: replicator.settings.clone(),
            next_operation,
            pending,
        });
        context.register_managed(engine).await?;
        Ok(ReplicatorInterfaces::primary(
            replicator,
            Some(state_replicator),
        ))
    }
}

pub struct DefaultReplicator {
    engine: Arc<DefaultReplicatorInner>,
    provider: Arc<dyn StateProvider>,
    settings: Arc<RwLock<ReplicatorSettings>>,
    pending: Arc<Mutex<Option<ClientWrite>>>,
    next_operation: Arc<AtomicU64>,
}

#[async_trait]
impl Replicator for DefaultReplicator {
    async fn open(&self) -> Result<String> {
        let committed_lsn = self.provider.last_committed_lsn().await?;
        self.engine.control_open(committed_lsn).await?;
        Ok(self.settings.read().await.replication_address.clone())
    }

    async fn change_role(&self, epoch: Epoch, role: ReplicaRole) -> Result<()> {
        self.engine.control_change_role(epoch, role).await
    }

    async fn update_epoch(&self, epoch: Epoch) -> Result<()> {
        self.engine
            .control_update_epoch(epoch, self.provider.as_ref())
            .await
    }

    async fn close(&self) -> Result<()> {
        self.engine.control_close().await
    }

    fn abort(&self) {
        self.engine.control_abort();
    }

    async fn current_progress(&self) -> Result<Lsn> {
        self.engine.control_progress().await
    }

    async fn catch_up_capability(&self) -> Result<Lsn> {
        self.engine.control_catch_up_capability().await
    }
}

#[async_trait]
impl PrimaryReplicator for DefaultReplicator {
    async fn on_data_loss(&self) -> Result<bool> {
        if let Some(progress) = self
            .engine
            .control_on_data_loss(self.provider.as_ref())
            .await?
        {
            *self.pending.lock().await = None;
            self.next_operation
                .store(progress.max(0) as u64, Ordering::Release);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn update_catch_up_replica_set_configuration(
        &self,
        current: ReplicaSetConfiguration,
        previous: ReplicaSetConfiguration,
    ) -> Result<()> {
        self.engine
            .configure_replicas(current.configuration, Some(previous.configuration))
            .await
    }

    async fn wait_for_catch_up_quorum(&self, mode: ReplicaSetQuorumMode) -> Result<()> {
        self.engine.wait_for_quorum(mode).await
    }

    async fn update_current_replica_set_configuration(
        &self,
        current: ReplicaSetConfiguration,
    ) -> Result<()> {
        self.engine
            .configure_replicas(current.configuration, None)
            .await
    }

    async fn build_replica(&self, replica: ReplicaInformation) -> Result<()> {
        self.engine.wait_for_build(replica, true).await
    }

    async fn remove_replica(&self, replica_id: ReplicaId) -> Result<()> {
        self.engine.remove_replica(replica_id, true).await
    }
}

struct DefaultStateReplicator {
    engine: Arc<DefaultReplicatorInner>,
    streams: Arc<ServiceStreams>,
    settings: Arc<RwLock<ReplicatorSettings>>,
    next_operation: Arc<AtomicU64>,
    pending: Arc<Mutex<Option<ClientWrite>>>,
}

#[async_trait]
impl StateReplicator for DefaultStateReplicator {
    async fn replicate(&self, data: OperationData) -> Result<Lsn> {
        let engine = &self.engine;
        engine.require_write_access().await?;
        let mut reservation = self.pending.lock().await;
        engine.require_write_access().await?;
        let created_reservation = if let Some(write) = reservation.as_ref() {
            if write.data != data {
                return Err(RuntimeError::LocalWritePending(
                    write.operation_id.to_string(),
                ));
            }
            false
        } else {
            let id = self.next_operation.fetch_add(1, Ordering::Relaxed);
            *reservation = Some(engine.recover_replicate_write(data, id).await);
            true
        };
        let write = reservation.as_ref().expect("write reserved").clone();
        let mut pending = match engine.begin_write(write.clone()).await {
            Ok(pending) => pending,
            Err(
                error @ (RuntimeError::DataLossFenced
                | RuntimeError::WriteClosed(_)
                | RuntimeError::AuthorityMismatch(_)
                | RuntimeError::NotPrimary),
            ) => {
                *reservation = None;
                return Err(error);
            }
            Err(error @ RuntimeError::LocalWritePending(_)) if created_reservation => {
                *reservation = None;
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        let lsn = loop {
            if let Err(error) = engine.publish_replication(&pending).await {
                if matches!(
                    error,
                    RuntimeError::DataLossFenced
                        | RuntimeError::WriteClosed(_)
                        | RuntimeError::AuthorityMismatch(_)
                        | RuntimeError::NotPrimary
                ) {
                    *reservation = None;
                }
                return Err(error);
            }
            match pending.committed().await {
                Ok(receipt) => break receipt.lsn,
                Err(RuntimeError::WriteCompletionClosed) => {
                    pending = engine.begin_write(write.clone()).await?;
                }
                Err(error) => {
                    if matches!(
                        error,
                        RuntimeError::DataLossFenced
                            | RuntimeError::WriteClosed(_)
                            | RuntimeError::AuthorityMismatch(_)
                            | RuntimeError::NotPrimary
                    ) {
                        *reservation = None;
                    }
                    return Err(error);
                }
            }
        };
        *reservation = None;
        Ok(lsn)
    }

    async fn get_replication_stream(&self) -> Result<OperationStream> {
        self.streams.take_replication().await
    }

    async fn get_copy_stream(&self) -> Result<OperationStream> {
        self.streams.take_copy().await
    }

    async fn update_replicator_settings(&self, settings: ReplicatorSettings) -> Result<()> {
        *self.settings.write().await = settings;
        Ok(())
    }
}
