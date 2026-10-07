//! Service Fabric-aligned process hosting boundary.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};

#[cfg(all(test, kuberic_workspace_tests))]
use crate::application::{ClientWrite, WriteReceipt};
use crate::application::{OpenContext, OpenMode, StateProvider, StatefulServiceReplica};
use crate::authority::{
    AdmittedAuthority, AuthorityStore, BuildAuthority, BuildAuthorityKind, BuildAuthorityStore,
    BuildProgressStore, LocalWriteJournal, LocalWritePhase, ReplicaAuthorityStore,
    ReplicationProgressStore,
};
use crate::capabilities::{ReplicatorCreationIdentity, RuntimeHostToken};
use crate::control::proto;
use crate::effects::{
    RoleTransition, RuntimeEffect, RuntimeEffectAction, RuntimeEffectResult, RuntimePostcondition,
    RuntimeSnapshot,
};
use crate::protocol::types::{
    AccessStatus, Epoch, FaultType, LoadMetric, OperationId, PartitionId, PartitionInformation,
    ReplicaIdentity, ReplicaRole,
};
use crate::replicator::copy::{
    BuildConfiguration, PrepareCopyRequest, PreparedCopy as RuntimePreparedCopy,
};
use crate::replicator::{
    DefaultReplicatorDependencies, ManagedReplicatorDataPlane, PartitionAccessView,
    PrimaryReplicator, Replicator, ReplicatorAttachment, ReplicatorCreationReservation,
    ReplicatorFactoryContext, ReplicatorRegistration, StatefulServicePartition,
};
use crate::runtime::PendingReplication as RuntimePendingReplication;
#[cfg(all(test, kuberic_workspace_tests))]
use crate::runtime::PendingWrite as RuntimePendingWrite;
use crate::transport::{OutboundOperation, ReplicaEndpoint};
use crate::{Result, RuntimeError};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use tokio::sync::{Mutex, RwLock, oneshot};

tokio::task_local! {
    static ACCESS_PROOF_VIEW: (AccessStatus, AccessStatus);
    static ACCESS_PUBLICATION_DEADLINE: tokio::time::Instant;
}

fn access_publication_deadline() -> Option<tokio::time::Instant> {
    ACCESS_PUBLICATION_DEADLINE
        .try_with(|deadline| *deadline)
        .ok()
}

async fn with_access_publication_deadline<F: Future>(
    deadline: tokio::time::Instant,
    future: F,
) -> F::Output {
    ACCESS_PUBLICATION_DEADLINE.scope(deadline, future).await
}

pub(super) async fn with_access_proof_view<F>(
    read: AccessStatus,
    write: AccessStatus,
    future: F,
) -> F::Output
where
    F: std::future::Future,
{
    ACCESS_PROOF_VIEW.scope((read, write), future).await
}

use crate::host::runtime_adapter::RuntimeEffectExecution;
#[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
use crate::host::transport::replication_to_proto;
use crate::host::transport::{
    copy_ack_from_proto, copy_ack_to_proto, copy_from_proto, copy_to_proto,
    replication_ack_from_proto, replication_ack_to_proto, replication_from_proto,
};

#[path = "custom.rs"]
mod custom;
#[path = "lifecycle.rs"]
mod lifecycle;
pub(crate) use custom::AcceptedAccessEffect;

#[async_trait]
#[cfg(all(test, kuberic_workspace_tests))]
pub(crate) trait RuntimeControlPlane: Send {
    async fn next_effect(&mut self) -> Result<Option<RuntimeEffect>>;

    async fn publish(&mut self, result: RuntimeEffectResult) -> Result<()>;
}

#[cfg(all(test, kuberic_workspace_tests))]
pub(crate) struct PendingWrite {
    pub(crate) lsn: i64,
    pub(crate) replication_items: Vec<proto::ReplicationItem>,
    pub(crate) build_items: Vec<proto::CopyItem>,
    inner: RuntimePendingWrite,
}

#[cfg(all(test, kuberic_workspace_tests))]
impl PendingWrite {
    pub(crate) async fn committed(self) -> Result<WriteReceipt> {
        self.inner.committed().await
    }
}

pub(crate) struct PendingReplication {
    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    pub(crate) received: proto::ReplicationAck,
    inner: RuntimePendingReplication,
}

impl PendingReplication {
    pub(crate) async fn applied(self) -> Result<proto::ReplicationAck> {
        self.inner.applied().await.map(replication_ack_to_proto)
    }
}

pub(crate) struct PreparedCopy {
    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    pub(crate) authority: crate::authority::BuildAuthority,
    pub(crate) items: Pin<Box<dyn Stream<Item = Result<proto::CopyItem>> + Send>>,
}

#[derive(Debug)]
#[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
pub(crate) enum OutboundReplication {
    Replication(proto::ReplicationItem),
    Copy(proto::CopyItem),
    Build(ReplicaEndpoint),
    Remove(crate::protocol::types::ReplicaId),
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
    role_transition_authority: Option<AdmittedAuthority>,
}

struct RegisteredReplicator {
    control: Arc<dyn Replicator>,
    primary: Option<Arc<dyn PrimaryReplicator>>,
    provider: Option<Arc<dyn StateProvider>>,
    process_lifecycle: Option<lifecycle::ProcessRuntime>,
    authority_lifecycle: Option<lifecycle::AuthorityRuntime>,
    peer_lifecycle: Option<lifecycle::PeerRuntime>,
    access_closure: Option<lifecycle::AccessClosure>,
    access_lifecycle: Option<lifecycle::AccessRuntime>,
    report_lifecycle: Option<lifecycle::ReportLifecycle>,
    lifecycle_evidence: Option<lifecycle::EvidenceRuntime>,
    effect_evidence: Option<lifecycle::EffectEvidenceRuntime>,
    build_lifecycle: Option<lifecycle::BuildLifecycleRuntime>,
    build_cancellation: Option<lifecycle::BuildCancellationRuntime>,
    outbound_lifecycle: Option<lifecycle::OutboundLifecycleRuntime>,
    removal_witness: Option<lifecycle::RemovalWitnessRuntime>,
    topology_lifecycle: Option<lifecycle::TopologyRuntime>,
    recovery_lifecycle: Option<lifecycle::RecoveryRuntime>,
    managed_data_plane: Option<Arc<dyn ManagedReplicatorDataPlane>>,
}

enum ReplicatorCreationState {
    Available,
    Reserved(ReplicatorCreationIdentity),
    Registered,
}

#[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
struct HostedPrimaryReplicator {
    inner: Arc<dyn PrimaryReplicator>,
    process_lifecycle: lifecycle::ProcessRuntime,
    build_lifecycle: lifecycle::BuildLifecycleRuntime,
}

#[async_trait]
#[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
impl Replicator for HostedPrimaryReplicator {
    async fn open(&self) -> Result<String> {
        self.inner.open().await
    }

    async fn change_role(&self, epoch: Epoch, role: ReplicaRole) -> Result<()> {
        self.process_lifecycle.invalidate_public_access().await?;
        self.inner.change_role(epoch, role).await
    }

    async fn update_epoch(&self, epoch: Epoch) -> Result<()> {
        self.process_lifecycle.invalidate_public_access().await?;
        self.inner.update_epoch(epoch).await
    }

    async fn close(&self) -> Result<()> {
        self.process_lifecycle.invalidate_public_access().await?;
        self.inner.close().await
    }

    fn abort(&self) {
        self.process_lifecycle.notify_abort();
        self.inner.abort();
    }

    async fn current_progress(&self) -> Result<i64> {
        self.inner.current_progress().await
    }

    async fn catch_up_capability(&self) -> Result<i64> {
        self.inner.catch_up_capability().await
    }
}

#[async_trait]
#[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
impl PrimaryReplicator for HostedPrimaryReplicator {
    async fn on_data_loss(&self) -> Result<bool> {
        self.inner.on_data_loss().await
    }

    async fn update_catch_up_replica_set_configuration(
        &self,
        current: crate::replicator::ReplicaSetConfiguration,
        previous: crate::replicator::ReplicaSetConfiguration,
    ) -> Result<()> {
        self.inner
            .update_catch_up_replica_set_configuration(current, previous)
            .await
    }

    async fn wait_for_catch_up_quorum(
        &self,
        mode: crate::replicator::ReplicaSetQuorumMode,
    ) -> Result<()> {
        self.inner.wait_for_catch_up_quorum(mode).await
    }

    async fn update_current_replica_set_configuration(
        &self,
        current: crate::replicator::ReplicaSetConfiguration,
    ) -> Result<()> {
        self.inner
            .update_current_replica_set_configuration(current)
            .await
    }

    async fn build_replica(&self, replica: crate::replicator::ReplicaInformation) -> Result<()> {
        self.build_lifecycle.build_replica(replica).await
    }

    async fn remove_replica(&self, replica_id: crate::protocol::types::ReplicaId) -> Result<()> {
        self.build_lifecycle.remove_replica(replica_id).await
    }
}

impl RegisteredReplicator {
    fn managed_data_plane(&self) -> Option<Arc<dyn ManagedReplicatorDataPlane>> {
        self.managed_data_plane.clone()
    }
    fn process_lifecycle(&self) -> Option<lifecycle::ProcessRuntime> {
        self.process_lifecycle.clone()
    }
    fn authority_lifecycle(&self) -> Option<lifecycle::AuthorityRuntime> {
        self.authority_lifecycle.clone()
    }
    fn peer_lifecycle(&self) -> Option<lifecycle::PeerRuntime> {
        self.peer_lifecycle.clone()
    }
    fn access_closure(&self) -> Option<lifecycle::AccessClosure> {
        self.access_closure.clone()
    }
    fn access_lifecycle(&self) -> Option<lifecycle::AccessRuntime> {
        self.access_lifecycle.clone()
    }
    fn report_lifecycle(&self) -> Option<lifecycle::ReportLifecycle> {
        self.report_lifecycle.clone()
    }
    fn lifecycle_evidence(&self) -> Option<lifecycle::EvidenceRuntime> {
        self.lifecycle_evidence.clone()
    }
    fn effect_evidence(&self) -> Option<lifecycle::EffectEvidenceRuntime> {
        self.effect_evidence.clone()
    }
    fn build_lifecycle(&self) -> Option<lifecycle::BuildLifecycleRuntime> {
        self.build_lifecycle.clone()
    }
    fn build_cancellation(&self) -> Option<lifecycle::BuildCancellationRuntime> {
        self.build_cancellation.clone()
    }
    fn outbound_lifecycle(&self) -> Option<lifecycle::OutboundLifecycleRuntime> {
        self.outbound_lifecycle.clone()
    }
    fn removal_witness(&self) -> Option<lifecycle::RemovalWitnessRuntime> {
        self.removal_witness.clone()
    }
    fn topology_lifecycle(&self) -> Option<lifecycle::TopologyRuntime> {
        self.topology_lifecycle.clone()
    }
    fn recovery_lifecycle(&self) -> Option<lifecycle::RecoveryRuntime> {
        self.recovery_lifecycle.clone()
    }
    fn primary(&self) -> Option<Arc<dyn PrimaryReplicator>> {
        self.primary.clone()
    }
    async fn open(&self) -> Result<Option<String>> {
        let address = self.control.open().await?;
        if let Some(hosted) = self.process_lifecycle() {
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

pub(crate) struct PodRuntime {
    host: Arc<RuntimeHost>,
}

enum BuildCancellationDecision {
    Commit,
    Cancel { public_cleanup: bool },
}

#[derive(Clone)]
pub(crate) struct RuntimeDataPlane {
    host: Arc<RuntimeHost>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartitionReportSnapshot {
    pub(crate) information: PartitionInformation,
    pub(crate) read_status: AccessStatus,
    pub(crate) write_status: AccessStatus,
    pub(crate) load_metrics: Vec<LoadMetric>,
    pub(crate) reported_fault: Option<FaultType>,
}

#[async_trait]
trait ReportHost: Send + Sync {
    async fn observe_progress(&self) -> Result<()>;
    async fn snapshot(&self) -> RuntimeSnapshot;
    async fn reconcile_access(&self, read: AccessStatus, write: AccessStatus) -> Result<()>;
    async fn partition_report(&self) -> PartitionReportSnapshot;
    async fn catch_up_capability(&self) -> Result<i64>;
}

#[derive(Clone)]
pub(crate) struct ReportRuntime {
    inner: Arc<dyn ReportHost>,
}

impl ReportRuntime {
    pub(crate) async fn observe_progress(&self) -> Result<()> {
        self.inner.observe_progress().await
    }

    pub(crate) async fn snapshot(&self) -> RuntimeSnapshot {
        self.inner.snapshot().await
    }

    pub(crate) async fn reconcile_durable_access(
        &self,
        read: AccessStatus,
        write: AccessStatus,
    ) -> Result<()> {
        self.inner.reconcile_access(read, write).await
    }

    pub(crate) async fn partition_report(&self) -> PartitionReportSnapshot {
        self.inner.partition_report().await
    }

    pub(crate) async fn catch_up_capability(&self) -> Result<i64> {
        self.inner.catch_up_capability().await
    }
}

#[async_trait]
trait BuildHost: Send + Sync {
    fn is_managed(&self) -> bool;
    async fn describe_peer(&self, replica: crate::replicator::ReplicaInformation) -> Result<()>;
    async fn snapshot(&self) -> RuntimeSnapshot;
    async fn register_peer_session(
        &self,
        identity: ReplicaIdentity,
        session: crate::protocol::types::ProcessSessionId,
    ) -> Result<()>;
    async fn authorize_build(
        &self,
        build_id: OperationId,
        target: ReplicaIdentity,
        configuration: BuildConfiguration,
    ) -> Result<BuildAuthority>;
    async fn execute_build(
        &self,
        replica: crate::replicator::ReplicaInformation,
    ) -> Result<Option<custom::BuildAdmission>>;
    async fn accept_build(&self, receipt: Option<custom::BuildAdmission>) -> Result<()>;
    async fn prepare_copy(&self, request: PrepareCopyRequest) -> Result<PreparedCopy>;
    async fn accept_copy_acknowledgement(&self, acknowledgement: proto::CopyAck) -> Result<()>;
    async fn accept_acknowledgement(&self, acknowledgement: proto::ReplicationAck) -> Result<()>;
}

#[async_trait]
trait BuildAttemptHost: Send + Sync {
    async fn generation(&self, build_id: &OperationId) -> Result<u64>;
    async fn cancel_attempt(
        &self,
        build_id: &OperationId,
        generation: u64,
        public_cleanup: bool,
    ) -> Result<()>;
}

#[derive(Clone)]
pub(crate) struct BuildRuntime {
    inner: Arc<dyn BuildHost>,
    cancellation: BuildAttemptRuntime,
}

#[derive(Clone)]
pub(crate) struct BuildAttemptRuntime {
    inner: Arc<dyn BuildAttemptHost>,
}

impl BuildAttemptRuntime {
    pub(crate) async fn generation(&self, build_id: &OperationId) -> Result<u64> {
        self.inner.generation(build_id).await
    }

    pub(crate) async fn cancel_attempt(
        &self,
        build_id: &OperationId,
        generation: u64,
        public_cleanup: bool,
    ) -> Result<()> {
        self.inner
            .cancel_attempt(build_id, generation, public_cleanup)
            .await
    }
}

impl BuildRuntime {
    pub(crate) async fn describe_peer(
        &self,
        replica: crate::replicator::ReplicaInformation,
    ) -> Result<()> {
        self.inner.describe_peer(replica).await
    }

    pub(crate) async fn generation(&self, build_id: &OperationId) -> Result<u64> {
        self.cancellation.generation(build_id).await
    }

    pub(crate) fn cancellation(&self) -> BuildAttemptRuntime {
        self.cancellation.clone()
    }

    pub(crate) async fn snapshot(&self) -> RuntimeSnapshot {
        self.inner.snapshot().await
    }

    pub(crate) async fn register_peer_session(
        &self,
        identity: ReplicaIdentity,
        session: crate::protocol::types::ProcessSessionId,
    ) -> Result<()> {
        self.inner.register_peer_session(identity, session).await
    }

    pub(crate) async fn authorize_build(
        &self,
        build_id: OperationId,
        target: ReplicaIdentity,
        configuration: BuildConfiguration,
    ) -> Result<BuildAuthority> {
        self.inner
            .authorize_build(build_id, target, configuration)
            .await
    }

    pub(crate) async fn prepare_copy(&self, request: PrepareCopyRequest) -> Result<PreparedCopy> {
        self.inner.prepare_copy(request).await
    }

    pub(crate) async fn accept_copy_acknowledgement(
        &self,
        acknowledgement: proto::CopyAck,
    ) -> Result<()> {
        self.inner
            .accept_copy_acknowledgement(acknowledgement)
            .await
    }

    pub(crate) async fn accept_acknowledgement(
        &self,
        acknowledgement: proto::ReplicationAck,
    ) -> Result<()> {
        self.inner.accept_acknowledgement(acknowledgement).await
    }

    pub(crate) async fn execute_admitted_build<F, Fut, E>(
        &self,
        replica: crate::replicator::ReplicaInformation,
        managed_copy: F,
    ) -> std::result::Result<(), E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = std::result::Result<(), E>>,
        E: From<RuntimeError>,
    {
        let generation = self.generation(&replica.build_id).await.map_err(E::from)?;
        let cancellation = BuildRuntimeCancellation::new(
            self.cancellation(),
            replica.build_id.clone(),
            generation,
        );
        let (result, public_cleanup) = if self.inner.is_managed() {
            (
                async {
                    let execution =
                        async { self.inner.execute_build(replica).await.map_err(E::from) };
                    let (receipt, ()) = tokio::try_join!(execution, managed_copy())?;
                    self.inner.accept_build(receipt).await.map_err(E::from)
                }
                .await,
                true,
            )
        } else {
            match self.inner.execute_build(replica).await {
                Ok(receipt) => (
                    self.inner.accept_build(receipt).await.map_err(E::from),
                    true,
                ),
                Err(error) => (Err(E::from(error)), false),
            }
        };
        match result {
            Ok(()) => {
                cancellation
                    .finish(BuildCancellationDecision::Commit)
                    .await?;
                Ok(())
            }
            Err(error) => {
                cancellation
                    .finish(BuildCancellationDecision::Cancel { public_cleanup })
                    .await?;
                Err(error)
            }
        }
    }
}

struct BuildRuntimeCancellation {
    decision: Option<tokio::sync::oneshot::Sender<BuildCancellationDecision>>,
    completion: tokio::task::JoinHandle<Result<()>>,
}

impl BuildRuntimeCancellation {
    fn new(runtime: BuildAttemptRuntime, build_id: OperationId, generation: u64) -> Self {
        let (decision, completion) = tokio::sync::oneshot::channel();
        let completion = tokio::spawn(async move {
            let public_cleanup = match completion.await {
                Ok(BuildCancellationDecision::Commit) => return Ok(()),
                Ok(BuildCancellationDecision::Cancel { public_cleanup }) => public_cleanup,
                Err(_) => true,
            };
            match runtime
                .cancel_attempt(&build_id, generation, public_cleanup)
                .await
            {
                Err(RuntimeError::Closed) => Ok(()),
                result => result,
            }
        });
        Self {
            decision: Some(decision),
            completion,
        }
    }

    async fn finish<E>(mut self, decision: BuildCancellationDecision) -> std::result::Result<(), E>
    where
        E: From<RuntimeError>,
    {
        if let Some(sender) = self.decision.take() {
            let _ = sender.send(decision);
        }
        self.completion
            .await
            .map_err(|error| RuntimeError::Application(error.to_string()))?
            .map_err(E::from)
    }
}

#[async_trait]
trait PeerDiscoveryHost: Send + Sync {
    async fn snapshot(&self) -> RuntimeSnapshot;
    async fn register_peer_session(
        &self,
        identity: ReplicaIdentity,
        session: crate::protocol::types::ProcessSessionId,
    ) -> Result<()>;
    async fn observe_secondary_removal_witness(
        &self,
        witness: crate::protocol::types::SecondaryRemovalWitness,
        committed: Option<crate::protocol::types::SecondaryScaleDownCleanup>,
    ) -> Result<()>;
    async fn accept_acknowledgement(&self, acknowledgement: proto::ReplicationAck) -> Result<()>;
    async fn repair_peer(&self, identity: ReplicaIdentity, progress: i64) -> Result<()>;
}

#[derive(Clone)]
pub(crate) struct PeerDiscoveryRuntime {
    inner: Arc<dyn PeerDiscoveryHost>,
}

impl PeerDiscoveryRuntime {
    pub(crate) async fn snapshot(&self) -> RuntimeSnapshot {
        self.inner.snapshot().await
    }

    pub(crate) async fn register_peer_session(
        &self,
        identity: ReplicaIdentity,
        session: crate::protocol::types::ProcessSessionId,
    ) -> Result<()> {
        self.inner.register_peer_session(identity, session).await
    }

    pub(crate) async fn observe_secondary_removal_witness(
        &self,
        witness: crate::protocol::types::SecondaryRemovalWitness,
        committed: Option<crate::protocol::types::SecondaryScaleDownCleanup>,
    ) -> Result<()> {
        self.inner
            .observe_secondary_removal_witness(witness, committed)
            .await
    }

    pub(crate) async fn accept_acknowledgement(
        &self,
        acknowledgement: proto::ReplicationAck,
    ) -> Result<()> {
        self.inner.accept_acknowledgement(acknowledgement).await
    }

    pub(crate) async fn repair_peer(&self, identity: ReplicaIdentity, progress: i64) -> Result<()> {
        self.inner.repair_peer(identity, progress).await
    }
}

#[async_trait]
trait OutboundHost: Send + Sync {
    async fn next_outbound(&self) -> Option<OutboundOperation>;
    async fn snapshot(&self) -> RuntimeSnapshot;
}

#[derive(Clone)]
pub(crate) struct OutboundRuntime {
    inner: Arc<dyn OutboundHost>,
}

impl OutboundRuntime {
    pub(crate) async fn next_outbound(&self) -> Option<OutboundOperation> {
        self.inner.next_outbound().await
    }

    pub(crate) async fn snapshot(&self) -> RuntimeSnapshot {
        self.inner.snapshot().await
    }
}

#[cfg(all(test, feature = "testing"))]
#[derive(Clone)]
pub(crate) struct AccessEffectAcceptanceGate {
    pub(crate) entered: Arc<tokio::sync::Notify>,
    pub(crate) release: Arc<tokio::sync::Notify>,
}

impl Drop for PodRuntime {
    fn drop(&mut self) {
        self.host.abort();
    }
}

impl PodRuntime {
    pub(crate) fn new<A, S>(
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

    pub(crate) fn new_for_partition<A, S>(
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
                    role_transition_authority: None,
                }),
                effect_lock: Mutex::new(()),
                registered: OnceLock::new(),
                replicator_creation: StdMutex::new(ReplicatorCreationState::Available),
                weak_self: weak_self.clone(),
                aborted: AtomicBool::new(false),
                closed: AtomicBool::new(false),
                custom_authority_attempt: StdMutex::new(Weak::new()),
                custom_authority_invalidated: AtomicBool::new(false),
                custom_access_requires_authorization: AtomicBool::new(false),
                pending_authority_recovery: StdMutex::new(None),
                custom_restoration: Mutex::new(()),
                replica_session: OnceLock::new(),
                #[cfg(all(test, feature = "testing"))]
                access_effect_acceptance_gate: StdMutex::new(None),
                #[cfg(all(test, feature = "testing"))]
                authority_publication_gate: StdMutex::new(None),
                #[cfg(all(test, feature = "testing"))]
                custom_restoration_gate: StdMutex::new(None),
                #[cfg(all(test, feature = "testing"))]
                peer_discovery_ready_gate: StdMutex::new(None),
            }),
        }
    }

    #[cfg(all(test, feature = "testing"))]
    pub(crate) fn testing_pause_access_effect_acceptance(&self) -> AccessEffectAcceptanceGate {
        let gate = AccessEffectAcceptanceGate {
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
        };
        *self.host.access_effect_acceptance_gate.lock().unwrap() = Some(gate.clone());
        gate
    }

    #[cfg(all(test, feature = "testing"))]
    pub(crate) fn testing_pause_authority_publication(&self) -> AccessEffectAcceptanceGate {
        let gate = AccessEffectAcceptanceGate {
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
        };
        *self.host.authority_publication_gate.lock().unwrap() = Some(gate.clone());
        gate
    }

    #[cfg(all(test, feature = "testing"))]
    pub(crate) fn testing_pause_custom_restoration(&self) -> AccessEffectAcceptanceGate {
        let gate = AccessEffectAcceptanceGate {
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
        };
        *self.host.custom_restoration_gate.lock().unwrap() = Some(gate.clone());
        gate
    }

    #[cfg(all(test, feature = "testing"))]
    pub(crate) fn testing_pause_peer_discovery_ready(&self) -> AccessEffectAcceptanceGate {
        let gate = AccessEffectAcceptanceGate {
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
        };
        *self.host.peer_discovery_ready_gate.lock().unwrap() = Some(gate.clone());
        gate
    }

    #[cfg(all(test, feature = "testing"))]
    pub(crate) fn testing_resume_access_effect_acceptance(&self) {
        if let Some(gate) = self
            .host
            .access_effect_acceptance_gate
            .lock()
            .unwrap()
            .take()
        {
            gate.release.notify_waiters();
        }
    }

    #[cfg(all(test, feature = "testing"))]
    pub(crate) async fn testing_has_applied_effect(&self, sequence: u64) -> bool {
        self.host.state.read().await.effects.contains_key(&sequence)
    }

    #[cfg(all(test, feature = "testing"))]
    pub(crate) async fn testing_cancel_configuration_work(&self) -> Result<()> {
        self.host
            .authority_lifecycle()?
            .cancel_configuration_work()
            .await
    }

    #[cfg(all(test, kuberic_workspace_tests))]
    pub(crate) async fn serve<C: RuntimeControlPlane>(&self, control_plane: &mut C) -> Result<()> {
        while let Some(effect) = control_plane.next_effect().await? {
            let result = self.apply_effect(effect).await?;
            control_plane.publish(result).await?;
        }
        Ok(())
    }

    #[cfg(all(test, feature = "testing"))]
    pub(crate) async fn restore_authority(&self) -> Result<()> {
        self.host.recovery_lifecycle()?.restore_authority().await
    }

    pub(super) fn stage_authority_recovery(&self, effect: Option<RuntimeEffect>) {
        *self.host.pending_authority_recovery.lock().unwrap() =
            effect.filter(|effect| matches!(effect.action, RuntimeEffectAction::AdmitAuthority(_)));
    }

    pub(super) fn finish_authority_recovery(&self) {
        self.host.pending_authority_recovery.lock().unwrap().take();
    }

    pub(crate) fn bind_replica_session(
        &self,
        resource: crate::protocol::types::ResourceUid,
        session: crate::protocol::types::ProcessSessionId,
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
            .and_then(RegisteredReplicator::process_lifecycle)
            .is_some_and(|lifecycle| lifecycle.owns_stream_session())
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

    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    pub(crate) async fn authorize_build(
        &self,
        build_id: crate::protocol::types::OperationId,
        target: ReplicaIdentity,
        configuration: BuildConfiguration,
    ) -> Result<BuildAuthority> {
        self.host
            .authorize_build(build_id, target, configuration)
            .await
    }

    pub(crate) async fn reconstruct(
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
        if let Ok(recovery) = self.host.recovery_lifecycle() {
            recovery.restore_authority().await?;
            self.host
                .sync_access_projection(recovery.snapshot().await)
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
        let (read_status, write_status) = if has_transition || self.host.custom_recovery_pending() {
            (
                AccessStatus::ReconfigurationPending,
                AccessStatus::ReconfigurationPending,
            )
        } else {
            (read_status, write_status)
        };
        if let Ok(recovery) = self.host.recovery_lifecycle() {
            if write_status == AccessStatus::Granted
                && let Some(committed) = self
                    .host
                    .default_dependencies
                    .replica_authority_store
                    .load_secondary_removal_commit()
                    .await?
                && recovery
                    .snapshot()
                    .await
                    .authority
                    .as_ref()
                    .is_some_and(|a| {
                        a.previous_configuration.is_none()
                            && a.secondary_removal.as_ref() == Some(&committed.evidence)
                    })
            {
                recovery.accept_secondary_removal(committed).await?;
            }
            match recovery.restore_access(read_status, write_status).await {
                Err(RuntimeError::ReconfigurationPending) => {
                    tracing::info!("replica access restoration deferred");
                }
                result => result?,
            }
            self.host
                .sync_access_projection(recovery.snapshot().await)
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
        committed: crate::protocol::types::SecondaryScaleDownCleanup,
        historical: Option<crate::protocol::command::AcceptSecondaryRemovalCommit>,
    ) -> Result<()> {
        let recovery = self.host.recovery_lifecycle()?;
        match historical {
            Some(command) => recovery.accept_historical_secondary_removal(command).await,
            None => recovery.accept_secondary_removal(committed).await,
        }
    }

    pub(crate) fn abort(&self) {
        self.host.abort();
    }

    pub(crate) async fn apply_effect(&self, effect: RuntimeEffect) -> Result<RuntimeEffectResult> {
        Box::pin(self.host.prepare_effect(effect))
            .await?
            .accept()
            .await
            .map_err(|error| match error {
                crate::host::HostError::Runtime(error) => error,
                error => RuntimeError::Application(error.to_string()),
            })
    }

    pub(crate) async fn prepare_effect(
        &self,
        effect: RuntimeEffect,
    ) -> Result<RuntimeEffectExecution> {
        Box::pin(self.host.prepare_effect(effect)).await
    }

    pub(crate) async fn consume_cancelled_build_effect(
        &self,
        effect: RuntimeEffect,
    ) -> Result<RuntimeEffectResult> {
        self.host.consume_cancelled_build_effect(effect).await
    }

    pub(crate) async fn cancel_configuration_work(&self) -> Result<()> {
        self.host
            .authority_lifecycle()?
            .cancel_configuration_work()
            .await
    }

    #[cfg(all(test, feature = "testing"))]
    pub(crate) async fn testing_set_access(
        &self,
        read: AccessStatus,
        write: AccessStatus,
    ) -> Result<()> {
        self.host.access_closure()?.set_access(read, write).await
    }

    #[cfg(all(test, feature = "testing"))]
    pub(crate) async fn testing_wait_for_catch_up(&self) -> Result<()> {
        self.host.topology_lifecycle()?.wait_for_catch_up().await
    }

    #[cfg(all(test, feature = "testing"))]
    pub(crate) async fn testing_admit_authority(&self, authority: AdmittedAuthority) -> Result<()> {
        self.host
            .authority_lifecycle()?
            .admit_authority(authority)
            .await
    }

    #[cfg(all(test, feature = "testing"))]
    pub(crate) async fn testing_close(&self) -> Result<()> {
        self.host.close().await
    }

    pub(crate) fn data_plane(&self) -> RuntimeDataPlane {
        RuntimeDataPlane {
            host: self.host.clone(),
        }
    }

    #[cfg(all(test, feature = "testing"))]
    pub(crate) fn testing_replicator_registration(&self) -> Arc<dyn ReplicatorRegistration> {
        self.host.clone()
    }

    #[cfg(all(test, feature = "testing"))]
    pub(crate) fn testing_lifecycle_registration(&self) -> (Option<bool>, bool) {
        self.host
            .registered
            .get()
            .map_or((None, false), |registered| {
                (
                    registered
                        .process_lifecycle()
                        .map(|lifecycle| lifecycle.owns_stream_session()),
                    registered.managed_data_plane.is_some(),
                )
            })
    }

    #[cfg(all(test, feature = "testing"))]
    pub(crate) fn testing_provider_capability(&self) -> Result<()> {
        self.host
            .registered
            .get()
            .and_then(|registered| registered.provider.as_ref())
            .map(|_| ())
            .ok_or_else(|| {
                RuntimeError::Application(
                    "the selected replicator does not expose default-engine provider access".into(),
                )
            })
    }

    #[cfg(all(test, feature = "testing"))]
    pub(crate) fn testing_outbound_data_plane_capability(&self) -> Result<()> {
        self.host.managed_data_plane().map(|_| ())
    }

    pub(crate) async fn snapshot(&self) -> RuntimeSnapshot {
        self.host.snapshot().await
    }

    pub(crate) fn report_runtime(&self) -> ReportRuntime {
        ReportRuntime {
            inner: self.host.clone(),
        }
    }

    pub(crate) fn build_runtime(&self) -> BuildRuntime {
        BuildRuntime {
            inner: self.host.clone(),
            cancellation: BuildAttemptRuntime {
                inner: self.host.clone(),
            },
        }
    }

    #[cfg(all(test, feature = "testing"))]
    pub(crate) fn build_attempt_runtime(&self) -> BuildAttemptRuntime {
        BuildAttemptRuntime {
            inner: self.host.clone(),
        }
    }

    pub(crate) fn peer_discovery_runtime(&self) -> PeerDiscoveryRuntime {
        PeerDiscoveryRuntime {
            inner: self.host.clone(),
        }
    }

    pub(crate) fn outbound_runtime(&self) -> OutboundRuntime {
        OutboundRuntime {
            inner: self.host.clone(),
        }
    }

    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    pub(crate) async fn primary_replicator(&self) -> Result<Arc<dyn PrimaryReplicator>> {
        let registered = self.host.registered.get().ok_or(RuntimeError::NotOpen)?;
        let primary = registered.primary().ok_or(RuntimeError::NotPrimary)?;
        match registered.build_lifecycle() {
            Some(build_lifecycle) => Ok(Arc::new(HostedPrimaryReplicator {
                inner: primary,
                process_lifecycle: registered
                    .process_lifecycle()
                    .ok_or(RuntimeError::NotPrimary)?,
                build_lifecycle,
            })),
            None => Ok(primary),
        }
    }

    pub(crate) async fn cancel_outbound_build(&self, build_id: &OperationId) -> Result<()> {
        self.host.build_lifecycle()?.cancel(build_id).await
    }

    pub(crate) async fn wait_for_build_completion(
        &self,
        build_id: &OperationId,
        target: &ReplicaIdentity,
    ) -> Result<()> {
        self.host
            .build_lifecycle()?
            .wait_for_completion(build_id, target)
            .await
    }

    pub(crate) async fn observe_build_completion(
        &self,
        effect: RuntimeEffect,
    ) -> Result<RuntimeEffectResult> {
        self.host.observe_build_completion(effect).await
    }

    pub(crate) async fn discard_cancelled_build_effect(
        &self,
        effect: &RuntimeEffect,
    ) -> Result<()> {
        self.host.discard_cancelled_build_effect(effect).await
    }

    pub(crate) async fn reissue_outbound_build(
        &self,
        build_id: OperationId,
        target: ReplicaIdentity,
        replication_address: String,
    ) -> Result<()> {
        let build = self.host.build_lifecycle()?;
        let snapshot = self.host.lifecycle_evidence()?.snapshot().await;
        if snapshot.builds.iter().any(|build| {
            build.authority.build_id == build_id
                && build.authority.target == target
                && build.completed
                && build.durable_lsn >= snapshot.current_progress
        }) {
            return Ok(());
        }
        build
            .enqueue(ReplicaEndpoint {
                build_id,
                identity: target,
                replication_address,
            })
            .await?;
        Ok(())
    }

    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    pub(crate) async fn repair_peer(&self, identity: ReplicaIdentity, progress: i64) -> Result<()> {
        self.host
            .managed_data_plane()?
            .repair_peer(identity, progress)
            .await
    }

    pub(crate) async fn register_peer_session(
        &self,
        identity: ReplicaIdentity,
        session: crate::protocol::types::ProcessSessionId,
    ) -> Result<()> {
        self.host
            .peer_lifecycle()?
            .register_peer_session(identity, session)
            .await
    }

    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    pub(crate) async fn describe_peer(
        &self,
        replica: crate::replicator::ReplicaInformation,
    ) -> Result<()> {
        self.host.peer_lifecycle()?.describe_peer(replica).await
    }

    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    pub(crate) async fn execute_admitted_build<F, Fut, E>(
        &self,
        replica: crate::replicator::ReplicaInformation,
        managed_copy: F,
    ) -> std::result::Result<(), E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = std::result::Result<(), E>>,
        E: From<RuntimeError>,
    {
        self.build_runtime()
            .execute_admitted_build(replica, managed_copy)
            .await
    }

    pub(crate) async fn partition_report(&self) -> PartitionReportSnapshot {
        self.host.partition_report_snapshot().await
    }
}

impl RuntimeDataPlane {
    #[cfg(all(test, kuberic_workspace_tests))]
    pub(crate) async fn begin_write(&self, write: ClientWrite) -> Result<PendingWrite> {
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

    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    pub(crate) async fn accept_acknowledgement(
        &self,
        acknowledgement: proto::ReplicationAck,
    ) -> Result<()> {
        self.host
            .accept_replication_acknowledgement(acknowledgement)
            .await
    }

    pub(crate) async fn prepare_copy(&self, request: PrepareCopyRequest) -> Result<PreparedCopy> {
        let RuntimePreparedCopy {
            #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
            authority,
            items,
        } = self.host.streams()?.prepare_copy(request).await?;
        Ok(PreparedCopy {
            #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
            authority,
            items: Box::pin(items.map(|item| item.map(copy_to_proto))),
        })
    }

    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    pub(crate) async fn accept_copy_acknowledgement(&self, ack: proto::CopyAck) -> Result<()> {
        self.host
            .streams()?
            .accept_copy_acknowledgement(copy_ack_from_proto(ack)?)
            .await
    }

    pub(crate) async fn receive_copy_item(&self, item: proto::CopyItem) -> Result<proto::CopyAck> {
        self.host
            .streams()?
            .receive_copy_item(copy_from_proto(item)?)
            .await
            .map(copy_ack_to_proto)
    }

    pub(crate) async fn receive_replication(
        &self,
        item: proto::ReplicationItem,
    ) -> Result<PendingReplication> {
        let pending = self
            .host
            .streams()?
            .receive_replication(replication_from_proto(item)?)
            .await?;
        Ok(PendingReplication {
            #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
            received: replication_ack_to_proto(pending.received.clone()),
            inner: pending,
        })
    }

    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    pub(crate) async fn next_outbound(&self) -> Option<OutboundReplication> {
        let registered = self.host.registered.get()?;
        let outbound = match (
            registered.outbound_lifecycle(),
            registered.managed_data_plane(),
        ) {
            (Some(outbound), Some(data_plane)) => {
                tokio::select! {
                    item = outbound.next() => item,
                    item = data_plane.next_outbound_item() => item,
                }
            }
            (Some(outbound), None) => outbound.next().await,
            (None, Some(data_plane)) => data_plane.next_outbound_item().await,
            (None, None) => None,
        }?;
        Some(match outbound {
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
        crate::protocol::types::ResourceUid,
        crate::protocol::types::ProcessSessionId,
    )>,
    identity: ReplicaIdentity,
    application: Arc<dyn StatefulServiceReplica>,
    default_dependencies: DefaultReplicatorDependencies,
    state: RwLock<HostState>,
    effect_lock: Mutex<()>,
    registered: OnceLock<RegisteredReplicator>,
    replicator_creation: StdMutex<ReplicatorCreationState>,
    weak_self: Weak<Self>,
    aborted: AtomicBool,
    closed: AtomicBool,
    custom_authority_attempt: StdMutex<Weak<AtomicBool>>,
    custom_authority_invalidated: AtomicBool,
    custom_access_requires_authorization: AtomicBool,
    pending_authority_recovery: StdMutex<Option<RuntimeEffect>>,
    custom_restoration: Mutex<()>,
    #[cfg(all(test, feature = "testing"))]
    access_effect_acceptance_gate: StdMutex<Option<AccessEffectAcceptanceGate>>,
    #[cfg(all(test, feature = "testing"))]
    authority_publication_gate: StdMutex<Option<AccessEffectAcceptanceGate>>,
    #[cfg(all(test, feature = "testing"))]
    custom_restoration_gate: StdMutex<Option<AccessEffectAcceptanceGate>>,
    #[cfg(all(test, feature = "testing"))]
    peer_discovery_ready_gate: StdMutex<Option<AccessEffectAcceptanceGate>>,
}

impl OpenAttempt<'_> {
    fn authority(host: &RuntimeHost) -> OpenAttempt<'_> {
        let mut slot = host.custom_authority_attempt.lock().unwrap();
        let (entered, owned) = match slot.upgrade() {
            Some(entered) => (entered, false),
            None => {
                let entered = Arc::new(AtomicBool::new(false));
                host.custom_authority_invalidated
                    .store(false, Ordering::Release);
                *slot = Arc::downgrade(&entered);
                (entered, true)
            }
        };
        drop(slot);
        OpenAttempt {
            host,
            entered: Some(entered),
            owned,
            complete: false,
        }
    }

    fn arm(&self) {
        self.entered
            .as_ref()
            .unwrap()
            .store(true, Ordering::Release);
    }

    fn complete(mut self) {
        if self.owned {
            self.entered
                .as_ref()
                .unwrap()
                .store(false, Ordering::Release);
        }
        self.complete = true;
    }

    fn validate(&self) -> Result<()> {
        if self.host.aborted.load(Ordering::Acquire)
            || self.host.closed.load(Ordering::Acquire)
            || self
                .host
                .custom_authority_invalidated
                .load(Ordering::Acquire)
        {
            return Err(RuntimeError::OperationCancelled);
        }
        Ok(())
    }
}

impl RuntimeHost {
    fn custom_recovery_pending(&self) -> bool {
        !BuildHost::is_managed(self) && self.pending_authority_recovery.lock().unwrap().is_some()
    }

    fn custom_configuration_blocked(&self) -> bool {
        self.custom_recovery_pending()
            || self
                .custom_authority_attempt
                .lock()
                .unwrap()
                .upgrade()
                .is_some_and(|entered| entered.load(Ordering::Acquire))
    }

    #[cfg(all(test, feature = "testing"))]
    async fn pause_custom_restoration(&self) {
        let gate = self.custom_restoration_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
    }
}

#[async_trait]
impl ReportHost for RuntimeHost {
    async fn observe_progress(&self) -> Result<()> {
        if !BuildHost::is_managed(self) {
            let host = self.weak_self.upgrade().ok_or(RuntimeError::Closed)?;
            return tokio::spawn(async move { host.observe_report_progress().await })
                .await
                .map_err(|error| RuntimeError::Application(error.to_string()))?;
        }
        self.observe_report_progress().await
    }

    async fn snapshot(&self) -> RuntimeSnapshot {
        let lifecycle = self
            .registered
            .get()
            .and_then(RegisteredReplicator::report_lifecycle);
        let snapshot = match lifecycle {
            Some(lifecycle) => Some(lifecycle.snapshot().await),
            None => None,
        };
        self.compose_snapshot(snapshot).await
    }

    async fn reconcile_access(&self, read: AccessStatus, write: AccessStatus) -> Result<()> {
        if !BuildHost::is_managed(self) {
            let host = self.weak_self.upgrade().ok_or(RuntimeError::Closed)?;
            return tokio::spawn(async move { host.reconcile_report_access(read, write).await })
                .await
                .map_err(|error| RuntimeError::Application(error.to_string()))?;
        }
        self.reconcile_report_access(read, write).await
    }

    async fn partition_report(&self) -> PartitionReportSnapshot {
        self.partition_report_snapshot().await
    }

    async fn catch_up_capability(&self) -> Result<i64> {
        self.registered
            .get()
            .ok_or(RuntimeError::NotOpen)?
            .catch_up_capability()
            .await
    }
}

impl RuntimeHost {
    async fn describe_peer_with_owned_access_recovery(
        &self,
        replica: crate::replicator::ReplicaInformation,
        discovery_ready: oneshot::Sender<()>,
    ) -> Result<()> {
        let effect = self.effect_lock.try_lock().ok();
        let owned_access = if effect.is_some() {
            let state = self.state.read().await;
            let snapshot = &state.fallback_snapshot;
            state
                .effects
                .values()
                .rev()
                .find(|applied| {
                    matches!(
                        applied.effect.action,
                        RuntimeEffectAction::SetAccessStatus { .. }
                            | RuntimeEffectAction::SetReadStatus(_)
                            | RuntimeEffectAction::SetWriteStatus(_)
                    )
                })
                .map(|applied| applied.result.postcondition.clone())
                .filter(|owned| {
                    snapshot.open
                        && snapshot.role_transition.is_none()
                        && state.reported_fault.is_none()
                        && owned.authority.is_some()
                        && owned.authority == snapshot.authority
                        && owned.role == snapshot.role
                        && owned.read_status == snapshot.read_status
                        && owned.write_status == snapshot.write_status
                        && (owned.read_status == AccessStatus::Granted
                            || owned.write_status == AccessStatus::Granted)
                })
        } else {
            None
        };
        self.peer_lifecycle()?.describe_peer(replica).await?;
        let Some(owned) = owned_access else {
            return Ok(());
        };
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        #[cfg(all(test, feature = "testing"))]
        {
            let gate = self.peer_discovery_ready_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
        }
        let mut discovery_ready = Some(discovery_ready);
        for attempt in 0..3 {
            let current = self.snapshot().await;
            if !current.open
                || current.role_transition.is_some()
                || current.authority != owned.authority
                || current.role != owned.role
                || (current.read_status == owned.read_status
                    && current.write_status == owned.write_status)
            {
                return Ok(());
            }
            let store = &self.default_dependencies.replica_authority_store;
            if store.load().await? != owned.authority
                || store.load_retired_authority().await?.is_some()
                || store.load_retirement_started().await?.is_some()
            {
                return Ok(());
            }
            if owned.write_status == AccessStatus::Granted
                && self
                    .default_dependencies
                    .local_write_journal
                    .load_local_writes()
                    .await?
                    .iter()
                    .any(|write| write.phase != LocalWritePhase::Committed)
                && let Some(ready) = discovery_ready.take()
            {
                let _ = ready.send(());
            }
            let access = self
                .registered
                .get()
                .and_then(RegisteredReplicator::access_lifecycle)
                .ok_or(RuntimeError::NotOpen)?;
            let result = async {
                let ready = with_access_publication_deadline(
                    deadline,
                    access.begin_effect(owned.read_status, owned.write_status),
                )
                .await?;
                let (_, accepted) = ready.accept().await?;
                accepted.commit().await
            }
            .await;
            match result {
                Err(RuntimeError::OperationCancelled) if attempt < 2 => {
                    tokio::task::yield_now().await
                }
                result => {
                    if discovery_ready.is_none()
                        && let Err(error) = &result
                    {
                        tracing::warn!(%error, "owned peer access recovery remains closed");
                    }
                    return result;
                }
            }
        }
        unreachable!()
    }

    async fn observe_report_progress(&self) -> Result<()> {
        let _restoration = if !BuildHost::is_managed(self) {
            Some(self.custom_restoration.lock().await)
        } else {
            None
        };
        #[cfg(all(test, feature = "testing"))]
        self.pause_custom_restoration().await;
        if let Some(lifecycle) = self
            .registered
            .get()
            .and_then(RegisteredReplicator::report_lifecycle)
        {
            lifecycle.observe_progress().await?;
        } else {
            let result = async {
                let registered = self.registered.get().ok_or(RuntimeError::NotOpen)?;
                let current = registered.current_progress().await?;
                let committed = match registered.provider.as_ref() {
                    Some(provider) => Some(provider.last_committed_lsn().await?),
                    None => None,
                };
                let mut state = self.state.write().await;
                state.fallback_snapshot.current_progress = current;
                if let Some(committed) = committed {
                    state.fallback_snapshot.committed_lsn = committed;
                }
                Ok(())
            }
            .await;
            if matches!(result, Err(RuntimeError::NotOpen | RuntimeError::Closed)) {
                self.state.write().await.fallback_snapshot.open = false;
                return Ok(());
            }
            result?;
        }
        Ok(())
    }

    async fn reconcile_report_access(&self, read: AccessStatus, write: AccessStatus) -> Result<()> {
        let _restoration = if !BuildHost::is_managed(self) {
            Some(self.custom_restoration.lock().await)
        } else {
            None
        };
        if self
            .custom_access_requires_authorization
            .load(Ordering::Acquire)
            && (read == AccessStatus::Granted || write == AccessStatus::Granted)
        {
            return Err(RuntimeError::ReconfigurationPending);
        }
        #[cfg(all(test, feature = "testing"))]
        self.pause_custom_restoration().await;
        if let Some(lifecycle) = self
            .registered
            .get()
            .and_then(RegisteredReplicator::report_lifecycle)
        {
            lifecycle.reconcile_access(read, write).await?;
        } else {
            let mut state = self.state.write().await;
            state.fallback_snapshot.read_status = read;
            state.fallback_snapshot.write_status = write;
        }

        Ok(())
    }
}

#[async_trait]
impl BuildHost for RuntimeHost {
    fn is_managed(&self) -> bool {
        self.registered
            .get()
            .and_then(RegisteredReplicator::build_lifecycle)
            .is_some_and(|build| build.is_managed())
    }

    async fn describe_peer(&self, replica: crate::replicator::ReplicaInformation) -> Result<()> {
        if BuildHost::is_managed(self) {
            let host = self.weak_self.upgrade().ok_or(RuntimeError::Closed)?;
            let (ready, discovered) = oneshot::channel();
            let mut recovery = tokio::spawn(async move {
                host.describe_peer_with_owned_access_recovery(replica, ready)
                    .await
            });
            return tokio::select! {
                result = &mut recovery => result.map_err(|error| RuntimeError::Application(error.to_string()))?,
                result = discovered => match result {
                    Ok(()) => Ok(()),
                    Err(_) => recovery.await.map_err(|error| RuntimeError::Application(error.to_string()))?,
                },
            };
        }
        self.peer_lifecycle()?.describe_peer(replica).await
    }

    async fn snapshot(&self) -> RuntimeSnapshot {
        RuntimeHost::snapshot(self).await
    }

    async fn register_peer_session(
        &self,
        identity: ReplicaIdentity,
        session: crate::protocol::types::ProcessSessionId,
    ) -> Result<()> {
        self.peer_lifecycle()?
            .register_peer_session(identity, session)
            .await
    }

    async fn authorize_build(
        &self,
        build_id: OperationId,
        target: ReplicaIdentity,
        configuration: BuildConfiguration,
    ) -> Result<BuildAuthority> {
        RuntimeHost::authorize_build(self, build_id, target, configuration).await
    }

    async fn execute_build(
        &self,
        replica: crate::replicator::ReplicaInformation,
    ) -> Result<Option<custom::BuildAdmission>> {
        self.build_lifecycle()?.execute(replica).await
    }

    async fn accept_build(&self, receipt: Option<custom::BuildAdmission>) -> Result<()> {
        self.build_lifecycle()?.accept(receipt).await
    }

    async fn prepare_copy(&self, request: PrepareCopyRequest) -> Result<PreparedCopy> {
        RuntimeDataPlane {
            host: self.weak_self.upgrade().ok_or(RuntimeError::Closed)?,
        }
        .prepare_copy(request)
        .await
    }

    async fn accept_copy_acknowledgement(&self, acknowledgement: proto::CopyAck) -> Result<()> {
        self.streams()?
            .accept_copy_acknowledgement(copy_ack_from_proto(acknowledgement)?)
            .await
    }

    async fn accept_acknowledgement(&self, acknowledgement: proto::ReplicationAck) -> Result<()> {
        self.accept_replication_acknowledgement(acknowledgement)
            .await
    }
}

#[async_trait]
impl BuildAttemptHost for RuntimeHost {
    async fn generation(&self, build_id: &OperationId) -> Result<u64> {
        Ok(self.build_cancellation()?.generation(build_id).await)
    }

    async fn cancel_attempt(
        &self,
        build_id: &OperationId,
        generation: u64,
        public_cleanup: bool,
    ) -> Result<()> {
        self.build_cancellation()?
            .cancel_attempt(build_id, generation, public_cleanup)
            .await
    }
}

#[async_trait]
impl PeerDiscoveryHost for RuntimeHost {
    async fn snapshot(&self) -> RuntimeSnapshot {
        RuntimeHost::snapshot(self).await
    }

    async fn register_peer_session(
        &self,
        identity: ReplicaIdentity,
        session: crate::protocol::types::ProcessSessionId,
    ) -> Result<()> {
        self.peer_lifecycle()?
            .register_peer_session(identity, session)
            .await
    }

    async fn observe_secondary_removal_witness(
        &self,
        witness: crate::protocol::types::SecondaryRemovalWitness,
        committed: Option<crate::protocol::types::SecondaryScaleDownCleanup>,
    ) -> Result<()> {
        self.registered
            .get()
            .ok_or(RuntimeError::NotOpen)?
            .removal_witness()
            .ok_or_else(|| {
                RuntimeError::Application(
                    "replicator does not expose removal witness capabilities".into(),
                )
            })?
            .observe(witness, committed)
            .await
    }

    async fn accept_acknowledgement(&self, acknowledgement: proto::ReplicationAck) -> Result<()> {
        self.accept_replication_acknowledgement(acknowledgement)
            .await
    }

    async fn repair_peer(&self, identity: ReplicaIdentity, progress: i64) -> Result<()> {
        self.managed_data_plane()?
            .repair_peer(identity, progress)
            .await
    }
}

#[async_trait]
impl OutboundHost for RuntimeHost {
    async fn next_outbound(&self) -> Option<OutboundOperation> {
        let registered = self.registered.get()?;
        match (
            registered.outbound_lifecycle(),
            registered.managed_data_plane(),
        ) {
            (Some(outbound), Some(data_plane)) => {
                tokio::select! {
                    item = outbound.next() => item,
                    item = data_plane.next_outbound_item() => item,
                }
            }
            (Some(outbound), None) => outbound.next().await,
            (None, Some(data_plane)) => data_plane.next_outbound_item().await,
            (None, None) => None,
        }
    }

    async fn snapshot(&self) -> RuntimeSnapshot {
        RuntimeHost::snapshot(self).await
    }
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
        if let Ok(status) = ACCESS_PROOF_VIEW.try_with(|status| status.0) {
            return Ok(status);
        }
        let host = self.host.upgrade().ok_or(RuntimeError::Closed)?;
        Ok(host.state.read().await.fallback_snapshot.read_status)
    }

    async fn write_status(&self) -> Result<AccessStatus> {
        if let Ok(status) = ACCESS_PROOF_VIEW.try_with(|status| status.1) {
            return Ok(status);
        }
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
        let reservation = ReplicatorCreationReservation::new(RuntimeHostToken::new());
        let mut creation = self.replicator_creation.lock().map_err(|_| {
            RuntimeError::Application("replicator creation state was poisoned".into())
        })?;
        match &*creation {
            ReplicatorCreationState::Available => {
                *creation = ReplicatorCreationState::Reserved(
                    reservation.identity(RuntimeHostToken::new()),
                );
                Ok(reservation)
            }
            ReplicatorCreationState::Reserved(_) | ReplicatorCreationState::Registered => {
                Err(RuntimeError::Application(
                    "CreateReplicator may be called only once per Open".into(),
                ))
            }
        }
    }

    fn cancel_replicator_creation(&self, reservation: ReplicatorCreationReservation) {
        if let Ok(mut creation) = self.replicator_creation.lock()
            && matches!(
                &*creation,
                ReplicatorCreationState::Reserved(identity)
                    if *identity == reservation.identity(RuntimeHostToken::new())
            )
        {
            *creation = ReplicatorCreationState::Available;
        }
    }

    async fn register_interfaces(
        &self,
        attachment: &ReplicatorAttachment,
        provider: Option<Arc<dyn StateProvider>>,
        reservation: ReplicatorCreationReservation,
    ) -> Result<()> {
        let reservation_identity = reservation.identity(RuntimeHostToken::new());
        {
            let creation = self.replicator_creation.lock().map_err(|_| {
                RuntimeError::Application("replicator creation state was poisoned".into())
            })?;
            if !matches!(
                &*creation,
                ReplicatorCreationState::Reserved(identity)
                    if *identity == reservation_identity
                        && attachment.identity(RuntimeHostToken::new()) == *identity
            ) {
                return Err(RuntimeError::Application(
                    "CreateReplicator reservation is not active".into(),
                ));
            }
        }
        let managed_lifecycle = attachment.managed_lifecycle(RuntimeHostToken::new());
        let managed_data_plane = attachment.managed_data_plane(RuntimeHostToken::new());
        let control = attachment.replicator(RuntimeHostToken::new());
        let primary = attachment.primary_replicator(RuntimeHostToken::new());
        if let Some(lifecycle) = managed_lifecycle.as_ref() {
            lifecycle
                .attach_interfaces(control.clone(), primary.clone())
                .await?;
        }
        let lifecycle_registration = match (managed_lifecycle.as_ref(), primary.clone()) {
            (Some(lifecycle), Some(primary)) => {
                Some(custom::ReplicatorLifecycleRegistration::managed(
                    self.weak_self.clone(),
                    control.clone(),
                    primary,
                    lifecycle.clone(),
                ))
            }
            (Some(_), None) => {
                return Err(RuntimeError::Application(
                    "managed lifecycle requires a primary replicator".into(),
                ));
            }
            (None, Some(primary)) => Some(custom::ReplicatorLifecycleRegistration::service(
                self.weak_self.clone(),
                control.clone(),
                primary,
            )),
            (None, None) => None,
        };
        let (
            process_lifecycle,
            authority_lifecycle,
            peer_lifecycle,
            access_closure,
            access_lifecycle,
            report_lifecycle,
            lifecycle_evidence,
            effect_evidence,
            build_lifecycle,
            build_cancellation,
            outbound_lifecycle,
            removal_witness,
            topology_lifecycle,
            recovery_lifecycle,
        ) = match lifecycle_registration {
            Some(registration) => (
                Some(registration.process),
                Some(registration.authority),
                Some(registration.peer),
                Some(registration.access_closure),
                Some(registration.access),
                Some(registration.report),
                Some(registration.evidence),
                Some(registration.effect_evidence),
                Some(registration.build),
                Some(registration.build_cancellation),
                Some(registration.outbound),
                Some(registration.removal_witness),
                Some(registration.topology),
                Some(registration.recovery),
            ),
            None => (
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
            ),
        };
        let mut creation = self.replicator_creation.lock().map_err(|_| {
            RuntimeError::Application("replicator creation state was poisoned".into())
        })?;
        if !matches!(
            &*creation,
            ReplicatorCreationState::Reserved(identity)
                if *identity == reservation_identity
                    && attachment.identity(RuntimeHostToken::new()) == *identity
        ) {
            return Err(RuntimeError::Application(
                "CreateReplicator reservation was lost before registration".into(),
            ));
        }
        self.registered
            .set(RegisteredReplicator {
                control,
                primary,
                provider,
                process_lifecycle,
                authority_lifecycle,
                peer_lifecycle,
                access_closure,
                access_lifecycle,
                report_lifecycle,
                lifecycle_evidence,
                effect_evidence,
                build_lifecycle,
                build_cancellation,
                outbound_lifecycle,
                removal_witness,
                topology_lifecycle,
                recovery_lifecycle,
                managed_data_plane,
            })
            .map_err(|_| {
                RuntimeError::Application(
                    "CreateReplicator may be called only once per Open".into(),
                )
            })?;
        *creation = ReplicatorCreationState::Registered;
        Ok(())
    }
}

struct OpenAttempt<'a> {
    host: &'a RuntimeHost,
    complete: bool,
    entered: Option<Arc<AtomicBool>>,
    owned: bool,
}

impl Drop for OpenAttempt<'_> {
    fn drop(&mut self) {
        if !self.complete
            && self.owned
            && self
                .entered
                .as_ref()
                .is_none_or(|entered| entered.load(Ordering::Acquire))
        {
            self.host.abort();
        }
    }
}

impl RuntimeHost {
    fn streams(&self) -> Result<Arc<dyn ManagedReplicatorDataPlane>> {
        self.managed_data_plane()
    }
    async fn accept_replication_acknowledgement(
        &self,
        acknowledgement: proto::ReplicationAck,
    ) -> Result<()> {
        let session = acknowledgement.receiver_session_id.clone();
        let acknowledgement = replication_ack_from_proto(acknowledgement)?;
        if !session.is_empty() {
            return self
                .streams()?
                .observe_acknowledgement(
                    acknowledgement,
                    crate::protocol::types::ProcessSessionId::new(session),
                )
                .await;
        }
        self.streams()?
            .accept_acknowledgement(acknowledgement)
            .await
    }
    fn process_lifecycle(&self) -> Result<lifecycle::ProcessRuntime> {
        self.registered
            .get()
            .ok_or(RuntimeError::NotOpen)?
            .process_lifecycle()
            .ok_or_else(|| {
                RuntimeError::Application(
                    "replicator does not expose process lifecycle capabilities".into(),
                )
            })
    }
    fn authority_lifecycle(&self) -> Result<lifecycle::AuthorityRuntime> {
        self.registered
            .get()
            .ok_or(RuntimeError::NotOpen)?
            .authority_lifecycle()
            .ok_or_else(|| {
                RuntimeError::Application(
                    "replicator does not expose authority lifecycle capabilities".into(),
                )
            })
    }
    fn peer_lifecycle(&self) -> Result<lifecycle::PeerRuntime> {
        self.registered
            .get()
            .ok_or(RuntimeError::NotOpen)?
            .peer_lifecycle()
            .ok_or_else(|| {
                RuntimeError::Application(
                    "replicator does not expose peer lifecycle capabilities".into(),
                )
            })
    }
    fn access_closure(&self) -> Result<lifecycle::AccessClosure> {
        self.registered
            .get()
            .ok_or(RuntimeError::NotOpen)?
            .access_closure()
            .ok_or_else(|| {
                RuntimeError::Application(
                    "replicator does not expose access lifecycle capabilities".into(),
                )
            })
    }
    fn lifecycle_evidence(&self) -> Result<lifecycle::EvidenceRuntime> {
        self.registered
            .get()
            .ok_or(RuntimeError::NotOpen)?
            .lifecycle_evidence()
            .ok_or_else(|| {
                RuntimeError::Application(
                    "replicator does not expose lifecycle evidence capabilities".into(),
                )
            })
    }
    fn build_lifecycle(&self) -> Result<lifecycle::BuildLifecycleRuntime> {
        self.registered
            .get()
            .ok_or(RuntimeError::NotOpen)?
            .build_lifecycle()
            .ok_or_else(|| {
                RuntimeError::Application(
                    "replicator does not expose build lifecycle capabilities".into(),
                )
            })
    }
    fn build_cancellation(&self) -> Result<lifecycle::BuildCancellationRuntime> {
        self.registered
            .get()
            .ok_or(RuntimeError::NotOpen)?
            .build_cancellation()
            .ok_or_else(|| {
                RuntimeError::Application(
                    "replicator does not expose build cancellation capabilities".into(),
                )
            })
    }
    fn topology_lifecycle(&self) -> Result<lifecycle::TopologyRuntime> {
        self.registered
            .get()
            .ok_or(RuntimeError::NotOpen)?
            .topology_lifecycle()
            .ok_or_else(|| {
                RuntimeError::Application(
                    "replicator does not expose topology lifecycle capabilities".into(),
                )
            })
    }
    fn recovery_lifecycle(&self) -> Result<lifecycle::RecoveryRuntime> {
        self.registered
            .get()
            .ok_or(RuntimeError::NotOpen)?
            .recovery_lifecycle()
            .ok_or_else(|| {
                RuntimeError::Application(
                    "replicator does not expose recovery lifecycle capabilities".into(),
                )
            })
    }
    fn managed_data_plane(&self) -> Result<Arc<dyn ManagedReplicatorDataPlane>> {
        let registered = self.registered.get().ok_or(RuntimeError::NotOpen)?;
        registered.managed_data_plane().ok_or_else(|| {
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
            if let Some(lifecycle) = registered.process_lifecycle() {
                lifecycle.notify_abort();
            }
            registered.abort();
        }
        self.application.abort();
    }

    async fn authorize_build(
        &self,
        build_id: OperationId,
        target: ReplicaIdentity,
        configuration: BuildConfiguration,
    ) -> Result<BuildAuthority> {
        let build = self.build_lifecycle()?;
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
                        == Some(crate::protocol::types::TransitionKind::Failover)
                {
                    BuildAuthorityKind::Failover
                } else {
                    BuildAuthorityKind::Provisioning
                };
                (kind, authority.current_configuration)
            }
            #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
            BuildConfiguration::Bootstrap(configuration) => {
                (BuildAuthorityKind::Bootstrap, configuration)
            }
        };
        if let Some(existing) = self
            .default_dependencies
            .build_authority_store
            .load_build(&build_id)
            .await?
        {
            if existing.kind != kind
                || existing.source != self.identity
                || existing.target != target
                || existing.current_configuration != current_configuration
            {
                return Err(RuntimeError::AuthorityMismatch(
                    "build ID is already bound to different exact authority".into(),
                ));
            }
            build.select(&existing).await?;
            return Ok(existing);
        }
        let authority = BuildAuthority {
            build_id,
            kind,
            source: self.identity.clone(),
            target,
            current_configuration,
            replication_boundary_lsn: snapshot.committed_lsn,
        };
        authority.validate()?;
        self.default_dependencies
            .build_authority_store
            .admit_build(&authority)
            .await?;
        build.select(&authority).await?;
        Ok(authority)
    }

    async fn prepare_effect(&self, effect: RuntimeEffect) -> Result<RuntimeEffectExecution> {
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
                    return Ok(RuntimeEffectExecution::completed(previous.result.clone()));
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
        let authority_attempt = if matches!(effect.action, RuntimeEffectAction::AdmitAuthority(_))
            && !BuildHost::is_managed(self)
        {
            Some(OpenAttempt::authority(self))
        } else {
            None
        };
        let mut access_commit = None;
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
                    if let Ok(topology) = self.topology_lifecycle() {
                        topology.complete_retirement(*retired).await?;
                    } else {
                        self.state.write().await.fallback_snapshot.retired_authority =
                            Some(durable);
                        self.closed.store(true, Ordering::Release);
                    }
                } else {
                    let topology = self.topology_lifecycle()?;
                    if !self.closed.load(Ordering::Acquire) {
                        topology.fence_retirement(*retired.clone()).await?;
                        self.sync_access_projection(self.lifecycle_evidence()?.snapshot().await)
                            .await;
                        self.change_replicator_role_at_epoch(
                            ReplicaRole::None,
                            Some(retired.report.epoch),
                        )
                        .await?;
                        self.change_application_role(ReplicaRole::None).await?;
                        self.close().await?;
                    }
                    topology.complete_retirement(*retired).await?;
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
                self.build_lifecycle()?
                    .enqueue(ReplicaEndpoint {
                        build_id,
                        identity: target,
                        replication_address,
                    })
                    .await?;
            }
            RuntimeEffectAction::AdmitAuthority(authority) => {
                self.authority_lifecycle()?
                    .admit_authority(*authority)
                    .await?;
                self.sync_access_projection(self.lifecycle_evidence()?.snapshot().await)
                    .await;
            }
            RuntimeEffectAction::AdmitBuildAuthority(authority) => {
                self.build_lifecycle()?.admit_authority(*authority).await?;
            }
            RuntimeEffectAction::RegisterPeerSession { identity, session } => {
                self.peer_lifecycle()?
                    .register_peer_session(identity, session)
                    .await?;
            }
            RuntimeEffectAction::RetireBuild(build_id) => {
                self.build_lifecycle()?.retire(build_id).await?;
            }
            RuntimeEffectAction::SetAccessStatus { read, write } => {
                if let Some(access) = self
                    .registered
                    .get()
                    .and_then(RegisteredReplicator::access_lifecycle)
                {
                    access_commit = Some(access.begin_effect(read, write).await?);
                } else {
                    self.execute_secondary_action(RuntimeEffectAction::SetAccessStatus {
                        read,
                        write,
                    })
                    .await?;
                }
            }
            RuntimeEffectAction::SetReadStatus(read) => {
                if let Some(access) = self
                    .registered
                    .get()
                    .and_then(RegisteredReplicator::access_lifecycle)
                {
                    let write = self.state.read().await.fallback_snapshot.write_status;
                    access_commit = Some(access.begin_effect(read, write).await?);
                } else {
                    self.execute_secondary_action(RuntimeEffectAction::SetReadStatus(read))
                        .await?;
                }
            }
            RuntimeEffectAction::SetWriteStatus(write) => {
                if let Some(access) = self
                    .registered
                    .get()
                    .and_then(RegisteredReplicator::access_lifecycle)
                {
                    let read = self.state.read().await.fallback_snapshot.read_status;
                    access_commit = Some(access.begin_effect(read, write).await?);
                } else {
                    self.execute_secondary_action(RuntimeEffectAction::SetWriteStatus(write))
                        .await?;
                }
            }
            RuntimeEffectAction::WaitForCatchup => {
                self.topology_lifecycle()?.wait_for_catch_up().await?;
            }
            RuntimeEffectAction::AuthorizeFailoverPrefix(boundary) => {
                self.topology_lifecycle()?
                    .authorize_failover_prefix(boundary)
                    .await?;
                self.require_primary_application_refresh().await;
            }
            RuntimeEffectAction::PrepareSwitchover {
                preparation_generation,
                request_id,
                source,
                target,
                starting_configuration_id,
                starting_epoch,
            } => {
                self.topology_lifecycle()?
                    .prepare_switchover(
                        preparation_generation,
                        request_id,
                        source,
                        target,
                        starting_configuration_id,
                        starting_epoch,
                    )
                    .await?;
            }
            RuntimeEffectAction::RefreshApplicationProgress => {
                if let Some(evidence) = self
                    .registered
                    .get()
                    .and_then(RegisteredReplicator::effect_evidence)
                {
                    evidence.refresh_progress().await?;
                } else {
                    self.execute_secondary_action(RuntimeEffectAction::RefreshApplicationProgress)
                        .await?;
                }
            }
            RuntimeEffectAction::PrepareSecondaryRemoval {
                intent,
                process_session_id,
                report_sequence,
            } => {
                self.topology_lifecycle()?
                    .prepare_secondary_removal(*intent, process_session_id, report_sequence)
                    .await?;
            }
            RuntimeEffectAction::ObserveSecondaryRemovalWitness(witness) => {
                self.topology_lifecycle()?
                    .observe_secondary_removal(*witness)
                    .await?;
            }
            RuntimeEffectAction::ObserveSecondaryRemovalProgress { witness, committed } => {
                self.topology_lifecycle()?
                    .observe_secondary_removal_progress(*witness, *committed)
                    .await?;
            }
            RuntimeEffectAction::ObserveReplicationAck {
                acknowledgement,
                session,
            } => {
                self.managed_data_plane()?
                    .observe_acknowledgement(*acknowledgement, session)
                    .await?;
            }
            RuntimeEffectAction::AcceptSecondaryRemovalCommit(committed) => {
                self.topology_lifecycle()?
                    .accept_secondary_removal(*committed)
                    .await?;
            }
            RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(command) => {
                self.topology_lifecycle()?
                    .accept_historical_secondary_removal(*command)
                    .await?;
            }
            RuntimeEffectAction::FenceRetirement(retired) => {
                self.topology_lifecycle()?
                    .fence_retirement(*retired)
                    .await?;
            }
            RuntimeEffectAction::CompleteRetirement(retired) => {
                self.topology_lifecycle()?
                    .complete_retirement(*retired)
                    .await?;
            }
            RuntimeEffectAction::Close => self.close().await?,
            RuntimeEffectAction::Abort => self.abort_action().await,
        }
        #[cfg(all(test, feature = "testing"))]
        if access_commit.is_some() || authority_attempt.is_some() {
            let gate = self.access_effect_acceptance_gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.entered.notify_waiters();
                gate.release.notified().await;
            }
        }
        let (access_progress, access_commit) = match access_commit {
            Some(transaction) => {
                let (progress, transaction) = transaction.accept().await?;
                (progress, Some(transaction))
            }
            None => (None, None),
        };
        let topology_receipt = match self
            .registered
            .get()
            .and_then(RegisteredReplicator::topology_lifecycle)
        {
            Some(topology) => topology.receipt(&effect.action).await.map(Box::new),
            None => None,
        };
        let postcondition = match self
            .registered
            .get()
            .and_then(RegisteredReplicator::effect_evidence)
        {
            Some(evidence) => evidence.postcondition(access_progress.as_ref()).await,
            None => snapshot_postcondition(self.snapshot().await),
        };
        let result = RuntimeEffectResult {
            operation_id: effect.operation_id.clone(),
            sequence: effect.sequence,
            topology_receipt,
            postcondition,
        };
        let applied = AppliedEffect {
            effect,
            result: result.clone(),
        };
        if let Some(commit) = access_commit {
            let commit = commit.into_effect(self.weak_self.clone(), applied);
            return Ok(RuntimeEffectExecution::prepared(result, commit));
        }
        let mut state = self.state.write().await;
        if let Some(attempt) = &authority_attempt {
            attempt.validate()?;
        }
        state.effects.insert(result.sequence, applied);
        if let Some(attempt) = authority_attempt {
            attempt.complete();
        }
        Ok(RuntimeEffectExecution::completed(result))
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
                    drop(state);
                } else {
                    return Err(RuntimeError::EffectConflict {
                        sequence: effect.sequence,
                    });
                }
            } else {
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
        }
        self.build_lifecycle()?.cancel(build_id).await?;
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
            topology_receipt: None,
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

    async fn discard_cancelled_build_effect(&self, effect: &RuntimeEffect) -> Result<()> {
        if !matches!(effect.action, RuntimeEffectAction::BuildReplica { .. }) {
            return Err(RuntimeError::Application(
                "only a build effect can be discarded after cancellation".into(),
            ));
        }
        let _guard = self.effect_lock.lock().await;
        let mut state = self.state.write().await;
        match state.effects.get(&effect.sequence) {
            Some(previous) if previous.effect == *effect => {
                state.effects.remove(&effect.sequence);
                Ok(())
            }
            Some(_) => Err(RuntimeError::EffectConflict {
                sequence: effect.sequence,
            }),
            None => Ok(()),
        }
    }

    async fn observe_build_completion(&self, effect: RuntimeEffect) -> Result<RuntimeEffectResult> {
        let (build_id, target) = match &effect.action {
            RuntimeEffectAction::BuildReplica {
                build_id, target, ..
            } => (build_id, target),
            _ => {
                return Err(RuntimeError::Application(
                    "only a build effect can observe build completion".into(),
                ));
            }
        };
        let _guard = self.effect_lock.lock().await;
        let previous = self
            .state
            .read()
            .await
            .effects
            .get(&effect.sequence)
            .cloned()
            .ok_or(RuntimeError::EffectOutOfOrder {
                expected: effect.sequence,
                observed: effect.sequence,
            })?;
        if previous.effect != effect {
            return Err(RuntimeError::EffectConflict {
                sequence: effect.sequence,
            });
        }
        let confirmation = self.build_lifecycle()?.confirm(build_id, target).await?;
        let result = RuntimeEffectResult {
            operation_id: effect.operation_id.clone(),
            sequence: effect.sequence,
            topology_receipt: None,
            postcondition: confirmation.postcondition.clone(),
        };
        self.state.write().await.effects.insert(
            result.sequence,
            AppliedEffect {
                effect,
                result: result.clone(),
            },
        );
        drop(confirmation);
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
            entered: None,
            owned: true,
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
        if let Some(evidence) = registered.lifecycle_evidence() {
            self.sync_access_projection(evidence.snapshot().await).await;
        } else {
            let progress = registered.current_progress().await?;
            let committed = match registered.provider.as_ref() {
                Some(provider) => provider.last_committed_lsn().await?,
                None => 0,
            };
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

    async fn require_primary_application_refresh(&self) {
        let snapshot = self.snapshot().await;
        if snapshot.role != ReplicaRole::Primary
            || snapshot
                .authority
                .as_ref()
                .is_none_or(|authority| authority.local_role() != ReplicaRole::Primary)
        {
            return;
        }
        let epoch = snapshot
            .authority
            .as_ref()
            .map_or_else(Epoch::default, |authority| {
                authority.current_configuration.epoch
            });
        let authority = snapshot.authority.clone();
        let mut state = self.state.write().await;
        state.fallback_snapshot.role_transition = Some(RoleTransition {
            completed_role: ReplicaRole::Primary,
            target_role: ReplicaRole::Primary,
            replicator_completed: true,
            epoch_completed: true,
            application_completed: false,
        });
        state.role_transition_epoch = Some(epoch);
        state.role_transition_authority = authority;
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
        if role == ReplicaRole::Primary && registered.primary().is_none() {
            return Err(RuntimeError::NotPrimary);
        }
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
        let authority = snapshot.authority.clone();
        let transition = {
            let mut state = self.state.write().await;
            if snapshot.role != role {
                state.fallback_snapshot.write_status = AccessStatus::ReconfigurationPending;
            }
            if state.role_transition_epoch.is_some_and(|old| epoch > old)
                || state.role_transition_authority != authority
            {
                state.fallback_snapshot.role_transition = None;
            }
            if let Some(transition) = state.fallback_snapshot.role_transition.clone() {
                if transition.target_role != role {
                    return Err(RuntimeError::ReconfigurationPending);
                }
                transition
            } else {
                let same_role = snapshot.role == role;
                let role_stage_completed = same_role
                    && state
                        .role_transition_epoch
                        .is_some_and(|completed| completed >= epoch)
                    && state.role_transition_authority == authority;
                let epoch_completed = role != ReplicaRole::Primary || role_stage_completed;
                let transition = RoleTransition {
                    completed_role: snapshot.role,
                    target_role: role,
                    replicator_completed: same_role,
                    epoch_completed,
                    application_completed: role_stage_completed,
                };
                state.fallback_snapshot.role_transition = Some(transition.clone());
                state.role_transition_epoch = Some(epoch);
                state.role_transition_authority = authority;
                transition
            }
        };
        if !transition.replicator_completed {
            if let Ok(managed) = self.process_lifecycle() {
                managed.fence_writes().await?;
            }
            self.state.write().await.fallback_snapshot.read_status =
                AccessStatus::ReconfigurationPending;
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
                && let Some(managed) = self
                    .registered
                    .get()
                    .and_then(RegisteredReplicator::process_lifecycle)
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
        if let Ok(authority) = self.authority_lifecycle() {
            authority.cancel_configuration_work().await?;
            self.access_closure()?
                .set_access(
                    AccessStatus::ReconfigurationPending,
                    AccessStatus::ReconfigurationPending,
                )
                .await?;
        }
        if let Err(error) = registered.close().await {
            if let Ok(managed) = self.process_lifecycle() {
                managed.complete_abort().await;
            }
            self.abort();
            return Err(error);
        }
        if let Err(error) = self.application.close().await {
            if let Ok(managed) = self.process_lifecycle() {
                managed.complete_abort().await;
            }
            self.application.abort();
            self.closed.store(true, Ordering::Release);
            let mut state = self.state.write().await;
            state.fallback_snapshot.role = ReplicaRole::None;
            state.fallback_snapshot.role_transition = None;
            state.fallback_snapshot.read_status = AccessStatus::NotPrimary;
            state.fallback_snapshot.write_status = AccessStatus::NotPrimary;
            return Err(error);
        }
        if let Ok(managed) = self.process_lifecycle()
            && let Err(error) = managed.complete_close().await
        {
            self.abort();
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
        if let Ok(managed) = self.process_lifecycle() {
            managed.complete_abort().await;
        }
        self.abort();
    }

    async fn sync_access_projection(&self, managed_snapshot: RuntimeSnapshot) {
        let mut state = self.state.write().await;
        state.fallback_snapshot.read_status = managed_snapshot.read_status;
        state.fallback_snapshot.write_status = managed_snapshot.write_status;
        state.fallback_snapshot.authority = managed_snapshot.authority;
    }

    async fn execute_secondary_action(&self, action: RuntimeEffectAction) -> Result<()> {
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
                let committed = match registered.provider.as_ref() {
                    Some(provider) => Some(provider.last_committed_lsn().await?),
                    None => None,
                };
                let mut state = self.state.write().await;
                state.fallback_snapshot.current_progress = current;
                if let Some(committed) = committed {
                    state.fallback_snapshot.committed_lsn = committed;
                }
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

    async fn compose_snapshot(&self, managed: Option<RuntimeSnapshot>) -> RuntimeSnapshot {
        if let Some(mut snapshot) = managed {
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

    async fn partition_report_snapshot(&self) -> PartitionReportSnapshot {
        let state = self.state.read().await;
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

    async fn snapshot(&self) -> RuntimeSnapshot {
        let managed = match self.lifecycle_evidence() {
            Ok(evidence) => Some(evidence.snapshot().await),
            Err(_) => None,
        };
        self.compose_snapshot(managed).await
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
