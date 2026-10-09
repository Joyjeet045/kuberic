pub(crate) mod configuration;
pub(crate) mod copy;
mod queue;
pub(crate) mod quorum;
pub(crate) mod sender;
pub mod stream;

pub(crate) mod log;

#[cfg(test)]
mod capability_tests;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};

use crate::capabilities::{ReplicatorCreationIdentity, RuntimeHostToken};
use crate::protocol::types::{
    AccessStatus, ConfigurationDescriptor, ConfigurationId, Epoch, FaultType, LoadMetric,
    OperationId, PartitionInformation, ProcessSessionId, ReplicaId, ReplicaIdentity, ReplicaRole,
    SecondaryRemovalWitness, SecondaryScaleDownCleanup, SecondaryScaleDownIntent,
    SwitchoverRequestId,
};
use crate::receipts::{NativeProgressStatus, NativeTopologyStatus};
use async_trait::async_trait;
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};

use crate::application::{ClientWrite, Lsn, OperationData, StateProvider};
use crate::authority::{
    BuildAuthority, BuildAuthorityStore, BuildProgressStore, LocalWriteJournal,
    ReplicaAuthorityStore, ReplicationProgressStore, RetiredAuthority,
};
use crate::effects::BuildPostcondition;
use crate::engine::DurableState;
use crate::replicator::configuration::ManagedReplicaConfiguration;
use crate::replicator::copy::{PrepareCopyRequest, PreparedCopy};
#[cfg(all(test, kuberic_workspace_tests))]
use crate::runtime::PendingWrite;
use crate::runtime::{DefaultReplicatorInner, PendingReplication};
use crate::transport::{CopyAck, CopyItem, OutboundOperation, ReplicationAck, ReplicationItem};
use crate::{Result, RuntimeError};
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
pub(crate) trait ManagedReplicatorLifecycle: Send + Sync {
    async fn fence_writes(&self) -> Result<()>;
    async fn settle_primary_prefix(&self) -> Result<ManagedCertifiedPrefixOutcome>;
    async fn cancel_configuration_work(&self) -> Result<()>;
    async fn prepare_access(
        &self,
        read: AccessStatus,
        write: AccessStatus,
    ) -> Result<ManagedAccessPreparation>;
    async fn publish_access(&self, preparation: ManagedAccessPreparation) -> Result<()>;
    async fn lock_native_fence(
        &self,
        expected: &ManagedOperationFence,
    ) -> Result<ManagedFenceGuard>;
    async fn native_fence(&self) -> Result<ManagedOperationFence>;
    async fn current_engine_fence(&self) -> ManagedOperationFence;
    async fn progress_status(&self) -> NativeProgressStatus;
    async fn topology_status(&self) -> NativeTopologyStatus;
    async fn prepare_replica_configuration(
        &self,
        configuration: ManagedReplicaConfiguration,
        host_generation: u64,
    ) -> Result<ManagedConfigurationPreparation>;
    async fn commit_replica_configuration(
        &self,
        preparation: ManagedConfigurationPreparation,
    ) -> Result<()>;
    async fn synchronize_replica_configuration(&self, host_generation: u64) -> Result<()>;
    async fn authorize_failover_prefix(
        &self,
        boundary: Lsn,
    ) -> Result<ManagedCertifiedPrefixOutcome>;
    async fn prepare_switchover(
        &self,
        preparation_generation: u64,
        request_id: SwitchoverRequestId,
        source: ReplicaIdentity,
        target: ReplicaIdentity,
        starting_configuration_id: ConfigurationId,
        starting_epoch: Epoch,
    ) -> Result<ManagedSwitchoverOutcome>;
    async fn prepare_secondary_removal(
        &self,
        intent: SecondaryScaleDownIntent,
        process_session_id: ProcessSessionId,
        report_sequence: u64,
    ) -> Result<ManagedRemovalPreparationOutcome>;
    async fn observe_secondary_removal_witness(
        &self,
        witness: SecondaryRemovalWitness,
    ) -> Result<ManagedRemovalWitnessOutcome>;
    async fn observe_secondary_removal_progress(
        &self,
        witness: SecondaryRemovalWitness,
        committed: SecondaryScaleDownCleanup,
    ) -> Result<ManagedRemovalProgressOutcome>;
    async fn accept_secondary_removal_commit(
        &self,
        committed: SecondaryScaleDownCleanup,
    ) -> Result<ManagedRemovalAcceptanceOutcome>;
    async fn accept_historical_secondary_removal_commit(
        &self,
        command: crate::protocol::command::AcceptSecondaryRemovalCommit,
    ) -> Result<ManagedHistoricalRemovalAcceptanceOutcome>;
    async fn fence_retirement(
        &self,
        retired: RetiredAuthority,
    ) -> Result<ManagedRetirementFenceOutcome>;
    async fn complete_retirement(
        &self,
        retired: RetiredAuthority,
    ) -> Result<ManagedRetirementCompletionOutcome>;
    async fn register_peer_session_proof(
        &self,
        identity: ReplicaIdentity,
        session: ProcessSessionId,
    ) -> Result<()>;
    async fn admit_build_authority_proof(&self, authority: BuildAuthority) -> Result<()>;
    async fn retire_build_proof(&self, build_id: OperationId) -> Result<()>;
    async fn refresh_progress_proof(&self) -> Result<()>;
    async fn restore_engine_proof(
        &self,
        configuration: Option<ManagedReplicaConfiguration>,
        host_generation: u64,
    ) -> Result<()>;
    async fn observe_engine(&self) -> ManagedReplicaObservation;
    async fn cancel_outbound_build(&self, build_id: &OperationId) -> Result<()>;
    async fn detach_outbound_build_stream(&self, build_id: &OperationId) -> Result<()>;
    async fn complete_open(&self, replication_address: String) -> Result<()>;
    async fn attach_interfaces(
        &self,
        control: Arc<dyn Replicator>,
        primary: Option<Arc<dyn PrimaryReplicator>>,
    ) -> Result<()>;
    fn abort(&self);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedOperationFence {
    pub(crate) configuration: Option<ManagedReplicaConfiguration>,
    pub(crate) engine_session_id: String,
    pub(crate) engine_generation: u64,
}

#[derive(Debug)]
pub(crate) struct ManagedConfigurationPreparation {
    pub(crate) fence: ManagedOperationFence,
    pub(crate) configuration: ManagedReplicaConfiguration,
    pub(crate) host_generation: u64,
    pub(crate) active: Arc<AtomicBool>,
    pub(crate) native_generation: Arc<AtomicU64>,
}

impl Drop for ManagedConfigurationPreparation {
    fn drop(&mut self) {
        if self.active.swap(false, Ordering::AcqRel) {
            self.native_generation.fetch_add(1, Ordering::AcqRel);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedAccessPreparation {
    pub(crate) fence: ManagedOperationFence,
    pub(crate) read: AccessStatus,
    pub(crate) write: AccessStatus,
    pub(crate) current_progress: Lsn,
    pub(crate) committed_lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedCertifiedPrefixOutcome {
    pub(crate) fence: ManagedOperationFence,
    pub(crate) verified_lsn: Lsn,
    pub(crate) settled_lsn: Lsn,
    pub(crate) committed_lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedSwitchoverOutcome {
    pub(crate) fence: ManagedOperationFence,
    pub(crate) preparation_generation: u64,
    pub(crate) request_id: SwitchoverRequestId,
    pub(crate) source: ReplicaIdentity,
    pub(crate) target: ReplicaIdentity,
    pub(crate) starting_configuration_id: ConfigurationId,
    pub(crate) starting_epoch: Epoch,
    pub(crate) handoff_lsn: Lsn,
    pub(crate) committed_lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedRemovalPreparationOutcome {
    pub(crate) fence: ManagedOperationFence,
    pub(crate) preparation: crate::protocol::types::SecondaryRemovalPreparation,
    pub(crate) verified_lsn: Option<Lsn>,
    pub(crate) committed_lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedRemovalWitnessOutcome {
    pub(crate) fence: ManagedOperationFence,
    pub(crate) witness: SecondaryRemovalWitness,
    pub(crate) verified_lsn: Option<Lsn>,
    pub(crate) committed_lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedRemovalProgressOutcome {
    pub(crate) fence: ManagedOperationFence,
    pub(crate) witness: SecondaryRemovalWitness,
    pub(crate) accepted: SecondaryScaleDownCleanup,
    pub(crate) verified_lsn: Option<Lsn>,
    pub(crate) committed_lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedRemovalAcceptanceOutcome {
    pub(crate) fence: ManagedOperationFence,
    pub(crate) accepted: SecondaryScaleDownCleanup,
    pub(crate) verified_lsn: Option<Lsn>,
    pub(crate) committed_lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedHistoricalRemovalAcceptanceOutcome {
    pub(crate) fence: ManagedOperationFence,
    pub(crate) accepted: SecondaryScaleDownCleanup,
    pub(crate) verified_lsn: Option<Lsn>,
    pub(crate) committed_lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedRetirementFenceOutcome {
    pub(crate) fence: ManagedOperationFence,
    pub(crate) retired: RetiredAuthority,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedRetirementCompletionOutcome {
    pub(crate) fence: ManagedOperationFence,
    pub(crate) retired: RetiredAuthority,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedReplicaObservation {
    pub(crate) fence: ManagedOperationFence,
    pub(crate) host_generation: u64,
    #[allow(dead_code)]
    pub(crate) diagnostic_revision: u64,
    pub(crate) progress: NativeProgressStatus,
    pub(crate) catch_up_capability: Lsn,
    pub(crate) builds: Vec<BuildPostcondition>,
    pub(crate) prepared_secondary_removal:
        Option<crate::protocol::types::SecondaryRemovalPreparation>,
    pub(crate) accepted_secondary_removal: Option<SecondaryScaleDownCleanup>,
    pub(crate) retired_authority: Option<RetiredAuthority>,
}

#[async_trait]
pub(crate) trait ManagedReplicaStore: Send + Sync {
    async fn load_configuration(&self) -> Result<Option<ManagedReplicaConfiguration>>;
    async fn load_secondary_removal(
        &self,
    ) -> Result<Option<crate::protocol::types::SecondaryRemovalPreparation>>;
    async fn record_secondary_removal(
        &self,
        preparation: &crate::protocol::types::SecondaryRemovalPreparation,
    ) -> Result<()>;
    async fn load_secondary_removal_commit(&self) -> Result<Option<SecondaryScaleDownCleanup>>;
    async fn record_secondary_removal_commit(
        &self,
        committed: &SecondaryScaleDownCleanup,
    ) -> Result<()>;
    async fn load_retired_authority(&self) -> Result<Option<RetiredAuthority>>;
    async fn load_retirement_started(&self) -> Result<Option<RetiredAuthority>>;
    async fn record_retirement_started(&self, authority: &RetiredAuthority) -> Result<()>;
    async fn retire(&self, authority: &RetiredAuthority) -> Result<()>;
}

#[doc(hidden)]
pub(crate) struct ManagedFenceGuard {
    _delivery: OwnedMutexGuard<()>,
    progress: NativeProgressStatus,
}

impl ManagedFenceGuard {
    pub(crate) fn new(delivery: OwnedMutexGuard<()>, progress: NativeProgressStatus) -> Self {
        Self {
            _delivery: delivery,
            progress,
        }
    }

    pub(crate) fn progress(&self) -> &NativeProgressStatus {
        &self.progress
    }
}

#[async_trait]
#[doc(hidden)]
pub(crate) trait ManagedReplicatorDataPlane: Send + Sync {
    async fn next_outbound_item(&self) -> Option<OutboundOperation>;
    async fn repair_peer(&self, identity: ReplicaIdentity, progress: Lsn) -> Result<()>;
    #[cfg(all(test, kuberic_workspace_tests))]
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
    creation: Arc<ReplicatorCreation>,
}

#[derive(Clone)]
struct ManagedReplicatorCapabilities {
    lifecycle: Arc<dyn ManagedReplicatorLifecycle>,
    data_plane: Arc<dyn ManagedReplicatorDataPlane>,
}

struct ReplicatorCreation {
    identity: OnceLock<ReplicatorCreationIdentity>,
    guarded_control: StdMutex<Arc<dyn Replicator>>,
    managed: Option<ManagedReplicatorCapabilities>,
    armed: AtomicBool,
}

impl ReplicatorCreation {
    fn new(
        guarded_control: Arc<dyn Replicator>,
        identity: Option<ReplicatorCreationIdentity>,
        managed: Option<ManagedReplicatorCapabilities>,
    ) -> Self {
        let creation_identity = OnceLock::new();
        if let Some(identity) = identity {
            creation_identity
                .set(identity)
                .expect("new creation identity is unset");
        }
        Self {
            identity: creation_identity,
            guarded_control: StdMutex::new(guarded_control),
            managed,
            armed: AtomicBool::new(true),
        }
    }

    fn bind_identity(&self, expected: ReplicatorCreationIdentity) -> Result<()> {
        if let Some(actual) = self.identity.get() {
            if *actual != expected {
                return Err(RuntimeError::Application(
                    "replicator capability creation identity does not match reservation".into(),
                ));
            }
            return Ok(());
        }
        self.identity.set(expected).map_err(|actual| {
            RuntimeError::Application(format!(
                "replicator capability creation identity was concurrently bound to {actual:?}"
            ))
        })
    }

    fn replace_guarded_control(&self, control: Arc<dyn Replicator>) {
        let mut guarded = self
            .guarded_control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guarded = control;
    }
}

impl Drop for ReplicatorCreation {
    fn drop(&mut self) {
        if !self.armed.load(Ordering::Acquire) {
            return;
        }
        let control = self
            .guarded_control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        control.abort();
        if let Some(managed) = self.managed.as_ref() {
            managed.lifecycle.abort();
            managed.data_plane.abort();
        }
    }
}

#[doc(hidden)]
pub(crate) struct ReplicatorAttachment {
    creation: Arc<ReplicatorCreation>,
    replicator: Arc<dyn Replicator>,
    primary_replicator: Option<Arc<dyn PrimaryReplicator>>,
}

#[doc(hidden)]
impl ReplicatorAttachment {
    pub(crate) fn identity(&self, _token: RuntimeHostToken) -> ReplicatorCreationIdentity {
        *self
            .creation
            .identity
            .get()
            .expect("replicator attachment identity is bound")
    }

    pub(crate) fn managed_lifecycle(
        &self,
        _token: RuntimeHostToken,
    ) -> Option<Arc<dyn ManagedReplicatorLifecycle>> {
        self.creation
            .managed
            .as_ref()
            .map(|managed| managed.lifecycle.clone())
    }

    pub(crate) fn managed_data_plane(
        &self,
        _token: RuntimeHostToken,
    ) -> Option<Arc<dyn ManagedReplicatorDataPlane>> {
        self.creation
            .managed
            .as_ref()
            .map(|managed| managed.data_plane.clone())
    }

    pub(crate) fn replicator(&self, _token: RuntimeHostToken) -> Arc<dyn Replicator> {
        self.replicator.clone()
    }

    pub(crate) fn primary_replicator(
        &self,
        _token: RuntimeHostToken,
    ) -> Option<Arc<dyn PrimaryReplicator>> {
        self.primary_replicator.clone()
    }

    fn disarm(&self) {
        self.creation.armed.store(false, Ordering::Release);
    }

    #[cfg(all(test, feature = "testing"))]
    pub(crate) fn testing_disarm(&self, _token: RuntimeHostToken) {
        self.disarm();
    }
}

impl ReplicatorInterfaces {
    pub fn secondary(
        replicator: Arc<dyn Replicator>,
        state_replicator: Option<Arc<dyn StateReplicator>>,
    ) -> Self {
        let creation = Arc::new(ReplicatorCreation::new(replicator.clone(), None, None));
        Self {
            replicator,
            state_replicator,
            primary_replicator: None,
            creation,
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
        let creation = Arc::new(ReplicatorCreation::new(replicator.clone(), None, None));
        Self {
            replicator,
            state_replicator,
            primary_replicator: Some(primary_replicator),
            creation,
        }
    }

    fn managed_primary<T, M>(
        identity: ReplicatorCreationIdentity,
        primary_replicator: Arc<T>,
        state_replicator: Option<Arc<dyn StateReplicator>>,
        managed: Arc<M>,
    ) -> Self
    where
        T: PrimaryReplicator + 'static,
        M: ManagedReplicatorLifecycle + ManagedReplicatorDataPlane + 'static,
    {
        let replicator: Arc<dyn Replicator> = primary_replicator.clone();
        let primary_replicator: Arc<dyn PrimaryReplicator> = primary_replicator;
        let lifecycle: Arc<dyn ManagedReplicatorLifecycle> = managed.clone();
        let data_plane: Arc<dyn ManagedReplicatorDataPlane> = managed;
        let creation = Arc::new(ReplicatorCreation::new(
            replicator.clone(),
            Some(identity),
            Some(ManagedReplicatorCapabilities {
                lifecycle,
                data_plane,
            }),
        ));
        Self {
            replicator,
            state_replicator,
            primary_replicator: Some(primary_replicator),
            creation,
        }
    }

    #[cfg(all(test, feature = "testing"))]
    #[doc(hidden)]
    pub(crate) fn testing_managed_primary<T, M>(
        _token: RuntimeHostToken,
        identity: ReplicatorCreationIdentity,
        primary_replicator: Arc<T>,
        state_replicator: Option<Arc<dyn StateReplicator>>,
        managed: Arc<M>,
    ) -> Self
    where
        T: PrimaryReplicator + 'static,
        M: ManagedReplicatorLifecycle + ManagedReplicatorDataPlane + 'static,
    {
        Self::managed_primary(identity, primary_replicator, state_replicator, managed)
    }

    /// Replaces the public primary interfaces while preserving this bundle's
    /// opaque creation identity and optional host-only capabilities.
    pub fn wrap_primary<T>(
        self,
        primary_replicator: Arc<T>,
        state_replicator: Option<Arc<dyn StateReplicator>>,
    ) -> Self
    where
        T: PrimaryReplicator + 'static,
    {
        let replicator: Arc<dyn Replicator> = primary_replicator.clone();
        let primary_replicator: Arc<dyn PrimaryReplicator> = primary_replicator;
        self.creation.replace_guarded_control(replicator.clone());
        Self {
            replicator,
            state_replicator,
            primary_replicator: Some(primary_replicator),
            creation: self.creation.clone(),
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

    fn prepare_attachment(
        &self,
        reservation: ReplicatorCreationReservation,
    ) -> Result<ReplicatorAttachment> {
        self.creation
            .bind_identity(reservation.identity(RuntimeHostToken::new()))?;
        Ok(ReplicatorAttachment {
            creation: self.creation.clone(),
            replicator: self.replicator(),
            primary_replicator: self.primary_replicator(),
        })
    }

    #[cfg(all(test, feature = "testing"))]
    #[doc(hidden)]
    pub(crate) fn testing_prepare_attachment(
        &self,
        _token: RuntimeHostToken,
        reservation: ReplicatorCreationReservation,
    ) -> Result<ReplicatorAttachment> {
        self.prepare_attachment(reservation)
    }
}

#[derive(Clone)]
pub struct ReplicatorFactoryContext {
    identity: ReplicaIdentity,
    access: Arc<dyn PartitionAccessView>,
    reservation: Option<ReplicatorCreationReservation>,
    pub(crate) default_dependencies: Option<DefaultReplicatorDependencies>,
}

impl ReplicatorFactoryContext {
    #[doc(hidden)]
    pub(crate) fn new(
        _token: RuntimeHostToken,
        identity: ReplicaIdentity,
        access: Arc<dyn PartitionAccessView>,
        default_dependencies: DefaultReplicatorDependencies,
    ) -> Self {
        Self {
            identity,
            access,
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

    fn creation_identity(&self) -> Result<ReplicatorCreationIdentity> {
        self.reservation
            .map(|reservation| reservation.identity(RuntimeHostToken::new()))
            .ok_or_else(|| RuntimeError::Application("replicator creation is not reserved".into()))
    }
}

#[async_trait]
#[doc(hidden)]
pub(crate) trait PartitionAccessView: Send + Sync {
    fn partition_information(&self) -> PartitionInformation;
    async fn read_status(&self) -> Result<AccessStatus>;

    async fn write_status(&self) -> Result<AccessStatus>;

    async fn report_load(&self, metrics: Vec<LoadMetric>) -> Result<()>;

    async fn report_fault(&self, fault: FaultType) -> Result<()>;
}

#[doc(hidden)]
#[derive(Clone)]
pub(crate) struct DefaultReplicatorDependencies {
    pub(crate) replica_authority_store: Arc<dyn ReplicaAuthorityStore>,
    pub(crate) managed_store: Arc<dyn ManagedReplicaStore>,
    pub(crate) replication_progress_store: Arc<dyn ReplicationProgressStore>,
    pub(crate) local_write_journal: Arc<dyn LocalWriteJournal>,
    pub(crate) build_authority_store: Arc<dyn BuildAuthorityStore>,
    pub(crate) build_progress_store: Arc<dyn BuildProgressStore>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub(crate) struct ReplicatorCreationReservation(ReplicatorCreationIdentity);

#[doc(hidden)]
impl ReplicatorCreationReservation {
    pub(crate) fn new(_token: RuntimeHostToken) -> Self {
        Self(ReplicatorCreationIdentity::new(RuntimeHostToken::new()))
    }

    pub(crate) fn identity(&self, _token: RuntimeHostToken) -> ReplicatorCreationIdentity {
        self.0
    }
}

#[async_trait]
#[doc(hidden)]
pub(crate) trait ReplicatorRegistration: Send + Sync {
    fn reserve_replicator_creation(&self) -> Result<ReplicatorCreationReservation>;

    fn cancel_replicator_creation(&self, reservation: ReplicatorCreationReservation);

    async fn register_interfaces(
        &self,
        attachment: &ReplicatorAttachment,
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
    pub(crate) fn new(
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
        let attachment = match interfaces.prepare_attachment(reservation) {
            Ok(attachment) => attachment,
            Err(error) => {
                self.registration.cancel_replicator_creation(reservation);
                return Err(error);
            }
        };
        if let Err(error) = self
            .registration
            .register_interfaces(&attachment, state_provider, reservation)
            .await
        {
            self.registration.cancel_replicator_creation(reservation);
            return Err(error);
        }
        attachment.disarm();
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
            dependencies.managed_store,
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
        Ok(ReplicatorInterfaces::managed_primary(
            context.creation_identity()?,
            replicator,
            Some(state_replicator),
            engine,
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
        self.engine.wait_for_build(replica).await
    }

    async fn remove_replica(&self, replica_id: ReplicaId) -> Result<()> {
        self.engine.remove_replica(replica_id).await
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
