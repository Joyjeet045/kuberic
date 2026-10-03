//! Private SF lifecycle/effect hosting, independent of operation/copy capability.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Weak};

use async_trait::async_trait;
use kuberic_protocol::types::{
    AccessStatus, ConfigurationDescriptor, OperationId, ProcessSessionId, ReplicaIdentity,
};
use kuberic_runtime::replicator::{
    ManagedReplicatorLifecycle, PrimaryReplicator, ReplicaInformation, ReplicaSetConfiguration,
    ReplicaSetQuorumMode, Replicator,
};
use kuberic_runtime::{Result, RuntimeError};
use kuberic_runtime_internal::authority::{
    AdmittedAuthority, BuildAuthority, BuildSelection, DurableBuildProgress,
};
use kuberic_runtime_internal::effects::{BuildPostcondition, RuntimeEffectAction, RuntimeSnapshot};
use kuberic_runtime_internal::transport::{OutboundOperation, ReplicaEndpoint};
use tokio::sync::{Mutex, Notify, RwLock, mpsc};

use super::{RuntimeHost, empty_snapshot};
#[path = "custom_removal.rs"]
mod removal;

#[derive(Clone, PartialEq, Eq)]
struct BuildReceipt {
    selection: BuildSelection,
    source_session: ProcessSessionId,
    target_session: ProcessSessionId,
    attempt_generation: u64,
    configuration_generation: u64,
}

impl BuildReceipt {
    fn matches_durable_selection(&self, other: &Self) -> bool {
        self.selection == other.selection
            && self.source_session == other.source_session
            && self.target_session == other.target_session
            && self.attempt_generation == other.attempt_generation
    }
}

struct AccessProjection {
    read: AccessStatus,
    write: AccessStatus,
    authority: Option<AdmittedAuthority>,
    configuration: Option<ReplicaSetConfiguration>,
    sessions: BTreeMap<ReplicaIdentity, ProcessSessionId>,
    role: kuberic_protocol::types::ReplicaRole,
    configuration_generation: u64,
    access_generation: u64,
    faulted_grant: bool,
}

type DeferredConfiguration = tokio::task::JoinHandle<Result<(u64, ReplicaSetConfiguration)>>;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum BuildExecution {
    ApplicationCompleted,
    BuiltInCopyRequired,
}

#[async_trait]
trait ReplicatorLifecycleBackend: Send + Sync {
    fn owns_stream_session(&self) -> bool;

    async fn complete_open(&self, address: String) -> Result<()>;
    async fn complete_close(&self) -> Result<()>;
    async fn complete_abort(&self);
    fn notify_abort(&self);
    async fn fence_writes(&self) -> Result<()>;
    async fn settle_primary_prefix(&self) -> Result<()>;
    async fn cancel_configuration_work(&self) -> Result<()>;
    async fn restore_authority(&self) -> Result<()>;
    async fn restore_access(&self, read: AccessStatus, write: AccessStatus) -> Result<()>;
    async fn admit_authority(&self, authority: AdmittedAuthority) -> Result<()>;
    async fn admit_build_authority(&self, authority: BuildAuthority) -> Result<()>;
    async fn register_peer_session(
        &self,
        identity: ReplicaIdentity,
        session: ProcessSessionId,
    ) -> Result<()>;
    async fn retire_build(&self, build_id: OperationId) -> Result<()>;
    async fn set_access(&self, read: AccessStatus, write: AccessStatus) -> Result<()>;
    async fn wait_for_catch_up(&self) -> Result<()>;
    async fn authorize_failover_prefix(&self, boundary: i64) -> Result<()>;
    async fn prepare_switchover(
        &self,
        preparation_generation: u64,
        request_id: kuberic_protocol::types::SwitchoverRequestId,
        source: ReplicaIdentity,
        target: ReplicaIdentity,
        starting_configuration_id: kuberic_protocol::types::ConfigurationId,
        starting_epoch: kuberic_protocol::types::Epoch,
    ) -> Result<()>;
    async fn refresh_progress(&self) -> Result<()>;
    async fn observe_progress(&self) -> Result<()>;
    async fn prepare_secondary_removal(
        &self,
        intent: kuberic_protocol::types::SecondaryScaleDownIntent,
        process_session_id: ProcessSessionId,
        report_sequence: u64,
    ) -> Result<()>;
    async fn observe_secondary_removal(
        &self,
        witness: kuberic_protocol::types::SecondaryRemovalWitness,
    ) -> Result<()>;
    async fn observe_secondary_removal_progress(
        &self,
        witness: kuberic_protocol::types::SecondaryRemovalWitness,
        committed: kuberic_protocol::types::SecondaryScaleDownCleanup,
    ) -> Result<()>;
    async fn accept_secondary_removal(
        &self,
        committed: kuberic_protocol::types::SecondaryScaleDownCleanup,
    ) -> Result<()>;
    async fn accept_historical_secondary_removal(
        &self,
        command: kuberic_protocol::command::AcceptSecondaryRemovalCommit,
    ) -> Result<()>;
    async fn fence_retirement(
        &self,
        retired: kuberic_runtime_internal::authority::RetiredAuthority,
    ) -> Result<()>;
    async fn complete_retirement(
        &self,
        retired: kuberic_runtime_internal::authority::RetiredAuthority,
    ) -> Result<()>;
    async fn snapshot(&self) -> RuntimeSnapshot;
    async fn cancel_outbound_build(&self, id: &OperationId) -> Result<()>;
    async fn next_outbound(&self) -> Option<OutboundOperation>;
    async fn wait_for_build_completion(
        &self,
        build_id: &OperationId,
        target: &ReplicaIdentity,
    ) -> Result<()>;
    async fn build_replica(&self, replica: ReplicaInformation) -> Result<()>;
    async fn remove_replica(&self, replica_id: kuberic_protocol::types::ReplicaId) -> Result<()>;
    async fn select_build(&self, authority: &BuildAuthority) -> Result<()>;
    async fn describe_peer(&self, replica: ReplicaInformation) -> Result<()>;
    async fn execute_build(&self, replica: ReplicaInformation) -> Result<BuildExecution>;
    async fn enqueue_build(&self, endpoint: ReplicaEndpoint) -> Result<()>;
}

struct ManagedLifecycleBackend {
    legacy: Arc<dyn ManagedReplicatorLifecycle>,
    common: CustomReplicatorHost,
}

#[async_trait]
impl ReplicatorLifecycleBackend for ManagedLifecycleBackend {
    fn owns_stream_session(&self) -> bool {
        true
    }

    async fn complete_open(&self, address: String) -> Result<()> {
        self.common.complete_open_common(address.clone()).await;
        self.legacy.complete_open(address).await?;
        self.sync_engine_proof().await
    }

    async fn complete_close(&self) -> Result<()> {
        self.common.terminate().await
    }

    async fn complete_abort(&self) {
        self.common.notify_abort();
        self.common.terminate().await.ok();
    }

    fn notify_abort(&self) {
        self.common.notify_abort();
    }

    async fn fence_writes(&self) -> Result<()> {
        self.common.fence_managed_access().await?;
        self.legacy.fence_writes().await?;
        self.sync_engine_proof().await
    }

    async fn settle_primary_prefix(&self) -> Result<()> {
        self.legacy.settle_primary_prefix().await?;
        self.common.settle_primary_prefix().await?;
        self.sync_engine_proof().await
    }

    async fn cancel_configuration_work(&self) -> Result<()> {
        self.common.cancel_configuration_work().await?;
        self.legacy.cancel_configuration_work().await?;
        self.sync_engine_proof().await
    }

    async fn restore_authority(&self) -> Result<()> {
        self.legacy.restore_engine_proof().await?;
        self.common.restore_authority().await?;
        self.sync_engine_proof().await
    }

    async fn restore_access(&self, read: AccessStatus, write: AccessStatus) -> Result<()> {
        let generation = self.legacy.prepare_access(read, write).await?;
        let projection = match self.common.prepare_access_projection(read, write).await {
            Err(RuntimeError::ReconfigurationPending) => {
                *self.common.restored_access.write().await = Some((read, write));
                return Err(RuntimeError::ReconfigurationPending);
            }
            result => result?,
        };
        if let Err(error) = self.legacy.publish_access(read, write, generation).await {
            self.common.fence_managed_access().await?;
            return Err(error);
        }
        if let Err(error) = self.common.publish_access_projection(projection).await {
            self.legacy.fence_writes().await?;
            self.common.fence_managed_access().await?;
            self.sync_engine_proof().await?;
            return Err(error);
        }
        self.sync_engine_proof().await
    }

    async fn admit_authority(&self, authority: AdmittedAuthority) -> Result<()> {
        self.common.prepare_authority_admission(&authority).await?;
        if let Err(error) = self.legacy.admit_authority_proof(authority.clone()).await {
            self.sync_engine_proof().await?;
            return Err(error);
        }
        self.common.install_managed_authority(&authority).await?;
        self.sync_engine_proof().await
    }

    async fn admit_build_authority(&self, authority: BuildAuthority) -> Result<()> {
        self.legacy
            .admit_build_authority_proof(authority.clone())
            .await?;
        self.common.install_managed_build(&authority).await?;
        self.sync_engine_proof().await
    }

    async fn register_peer_session(
        &self,
        identity: ReplicaIdentity,
        session: ProcessSessionId,
    ) -> Result<()> {
        let registered_engine_peer =
            self.legacy
                .snapshot()
                .await
                .authority
                .as_ref()
                .is_some_and(|authority| {
                    authority
                        .current_configuration
                        .members
                        .iter()
                        .chain(
                            authority
                                .previous_configuration
                                .iter()
                                .flat_map(|configuration| &configuration.members),
                        )
                        .any(|member| member.identity == identity)
                });
        if registered_engine_peer {
            self.legacy
                .register_peer_session_proof(identity.clone(), session.clone())
                .await?;
        }
        self.common
            .execute_common_build_action(RuntimeEffectAction::RegisterPeerSession {
                identity,
                session,
            })
            .await
    }

    async fn retire_build(&self, build_id: OperationId) -> Result<()> {
        self.legacy.retire_build_proof(build_id.clone()).await?;
        self.common.retire_managed_build(build_id).await?;
        self.sync_engine_proof().await
    }

    async fn set_access(&self, read: AccessStatus, write: AccessStatus) -> Result<()> {
        self.execute_access(read, write).await
    }

    async fn wait_for_catch_up(&self) -> Result<()> {
        let result = self.legacy.wait_for_catch_up_proof().await;
        self.sync_engine_proof().await?;
        result
    }

    async fn authorize_failover_prefix(&self, boundary: i64) -> Result<()> {
        let result = self.legacy.authorize_failover_prefix_proof(boundary).await;
        self.sync_engine_proof().await?;
        result
    }

    async fn prepare_switchover(
        &self,
        preparation_generation: u64,
        request_id: kuberic_protocol::types::SwitchoverRequestId,
        source: ReplicaIdentity,
        target: ReplicaIdentity,
        starting_configuration_id: kuberic_protocol::types::ConfigurationId,
        starting_epoch: kuberic_protocol::types::Epoch,
    ) -> Result<()> {
        self.common.fence_managed_access().await?;
        let result = self
            .legacy
            .prepare_switchover_proof(
                preparation_generation,
                request_id,
                source,
                target,
                starting_configuration_id,
                starting_epoch,
            )
            .await;
        self.sync_engine_proof().await?;
        result
    }

    async fn refresh_progress(&self) -> Result<()> {
        self.legacy.refresh_progress_proof().await?;
        self.common
            .apply_common_action(RuntimeEffectAction::RefreshApplicationProgress)
            .await?;
        self.sync_engine_proof().await
    }

    async fn observe_progress(&self) -> Result<()> {
        self.common.retry_restored_access().await
    }

    async fn prepare_secondary_removal(
        &self,
        intent: kuberic_protocol::types::SecondaryScaleDownIntent,
        process_session_id: ProcessSessionId,
        report_sequence: u64,
    ) -> Result<()> {
        self.execute_removal_action(RuntimeEffectAction::PrepareSecondaryRemoval {
            intent: Box::new(intent),
            process_session_id,
            report_sequence,
        })
        .await
    }

    async fn observe_secondary_removal(
        &self,
        witness: kuberic_protocol::types::SecondaryRemovalWitness,
    ) -> Result<()> {
        self.execute_removal_action(RuntimeEffectAction::ObserveSecondaryRemovalWitness(
            Box::new(witness),
        ))
        .await
    }

    async fn observe_secondary_removal_progress(
        &self,
        witness: kuberic_protocol::types::SecondaryRemovalWitness,
        committed: kuberic_protocol::types::SecondaryScaleDownCleanup,
    ) -> Result<()> {
        self.execute_removal_action(RuntimeEffectAction::ObserveSecondaryRemovalProgress {
            witness: Box::new(witness),
            committed: Box::new(committed),
        })
        .await
    }

    async fn accept_secondary_removal(
        &self,
        committed: kuberic_protocol::types::SecondaryScaleDownCleanup,
    ) -> Result<()> {
        self.execute_removal_action(RuntimeEffectAction::AcceptSecondaryRemovalCommit(Box::new(
            committed,
        )))
        .await
    }

    async fn accept_historical_secondary_removal(
        &self,
        command: kuberic_protocol::command::AcceptSecondaryRemovalCommit,
    ) -> Result<()> {
        self.execute_removal_action(RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(
            Box::new(command),
        ))
        .await
    }

    async fn fence_retirement(
        &self,
        retired: kuberic_runtime_internal::authority::RetiredAuthority,
    ) -> Result<()> {
        self.execute_removal_action(RuntimeEffectAction::FenceRetirement(Box::new(retired)))
            .await
    }

    async fn complete_retirement(
        &self,
        retired: kuberic_runtime_internal::authority::RetiredAuthority,
    ) -> Result<()> {
        self.execute_removal_action(RuntimeEffectAction::CompleteRetirement(Box::new(retired)))
            .await
    }

    async fn snapshot(&self) -> RuntimeSnapshot {
        let mut snapshot = self.common.snapshot().await;
        let engine = self.legacy.snapshot().await;
        snapshot.current_progress = engine.current_progress;
        snapshot.committed_lsn = engine.committed_lsn;
        snapshot.verified_replication_lsn = engine.verified_replication_lsn;
        snapshot.current_configuration_quorum_progress =
            engine.current_configuration_quorum_progress;
        snapshot.catch_up_boundary = engine.catch_up_boundary;
        snapshot.catch_up_complete = engine.catch_up_complete;
        merge_builds(&mut snapshot.builds, engine.builds);
        snapshot.live_builds_only = false;
        snapshot.prepared_secondary_removal = engine.prepared_secondary_removal;
        snapshot.accepted_secondary_removal = engine.accepted_secondary_removal;
        snapshot.retired_authority = engine.retired_authority;
        snapshot
    }

    async fn cancel_outbound_build(&self, id: &OperationId) -> Result<()> {
        self.common.cancel_common_build(id).await?;
        self.legacy.cancel_outbound_build(id).await
    }

    async fn next_outbound(&self) -> Option<OutboundOperation> {
        self.common.next_outbound().await
    }

    async fn wait_for_build_completion(
        &self,
        build_id: &OperationId,
        target: &ReplicaIdentity,
    ) -> Result<()> {
        let generation = self.common.build_generation(build_id).await;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(75);
        loop {
            let changed = self.common.changed.notified();
            let snapshot = self.snapshot().await;
            if snapshot.builds.iter().any(|build| {
                &build.authority.build_id == build_id
                    && &build.authority.target == target
                    && build.completed
            }) {
                return Ok(());
            }

            self.common
                .ensure_build_generation(build_id, generation)
                .await?;
            if tokio::time::Instant::now() >= deadline {
                return Err(RuntimeError::OperationCancelled);
            }
            tokio::select! {
                _ = changed => {}
                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
            }
        }
    }

    async fn build_replica(&self, replica: ReplicaInformation) -> Result<()> {
        let build_id = replica.build_id.clone();
        let endpoint = ReplicaEndpoint {
            build_id: build_id.clone(),
            identity: replica.identity.clone(),
            replication_address: replica.replication_address.clone(),
        };
        self.common.enqueue_build_wait(endpoint).await?;
        if let Err(error) = self.legacy.build_replica_proof(replica).await {
            self.common.cancel_common_build(&build_id).await?;
            return Err(error);
        }
        self.sync_engine_proof().await
    }

    async fn remove_replica(&self, replica_id: kuberic_protocol::types::ReplicaId) -> Result<()> {
        let authority_before = self.common.state.read().await.authority.clone();
        let sessions_before = self.common.sessions.read().await.clone();
        let configuration_generation = self.common.configuration_generation.load(Ordering::Acquire);
        self.legacy.remove_replica_proof(replica_id).await?;
        self.common.active_host()?;
        if authority_before != self.common.state.read().await.authority
            || sessions_before != *self.common.sessions.read().await
            || configuration_generation
                != self.common.configuration_generation.load(Ordering::Acquire)
        {
            return Err(RuntimeError::OperationCancelled);
        }
        self.common.remove_managed_replica(replica_id).await
    }

    async fn select_build(&self, authority: &BuildAuthority) -> Result<()> {
        let _gate = self.common.gate.lock().await;
        self.common.select_build_inner(authority).await
    }

    async fn describe_peer(&self, replica: ReplicaInformation) -> Result<()> {
        self.common.describe_peer(replica).await
    }

    async fn execute_build(&self, replica: ReplicaInformation) -> Result<BuildExecution> {
        let mut replica = replica;
        self.common.prepare_build_description(&mut replica).await?;
        Ok(BuildExecution::BuiltInCopyRequired)
    }

    async fn enqueue_build(&self, endpoint: ReplicaEndpoint) -> Result<()> {
        self.common.enqueue_build(endpoint).await
    }
}

impl ManagedLifecycleBackend {
    async fn execute_removal_action(&self, action: RuntimeEffectAction) -> Result<()> {
        let result = match action {
            RuntimeEffectAction::PrepareSecondaryRemoval {
                intent,
                process_session_id,
                report_sequence,
            } => {
                self.common.fence_managed_access().await?;
                self.legacy
                    .prepare_secondary_removal_proof(*intent, process_session_id, report_sequence)
                    .await
                    .map(|_| ())
            }
            RuntimeEffectAction::ObserveSecondaryRemovalWitness(witness) => {
                self.legacy.observe_secondary_removal_proof(*witness).await
            }
            RuntimeEffectAction::ObserveSecondaryRemovalProgress { witness, committed } => {
                self.legacy
                    .observe_secondary_removal_progress_proof(*witness, *committed)
                    .await
            }
            RuntimeEffectAction::AcceptSecondaryRemovalCommit(committed) => {
                self.legacy.accept_secondary_removal_proof(*committed).await
            }
            RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(command) => {
                self.legacy
                    .accept_historical_secondary_removal_proof(*command)
                    .await
            }
            RuntimeEffectAction::FenceRetirement(retired) => {
                let result = self.legacy.fence_retirement_proof((*retired).clone()).await;
                if result.is_ok() {
                    self.common.fence_retirement_state(&retired).await?;
                }
                result
            }
            RuntimeEffectAction::CompleteRetirement(retired) => {
                let result = self
                    .legacy
                    .complete_retirement_proof((*retired).clone())
                    .await;
                if result.is_ok() {
                    self.common.complete_retirement_state(&retired).await?;
                }
                result
            }
            _ => {
                return Err(RuntimeError::Application(
                    "managed removal proof requires a removal action".into(),
                ));
            }
        };
        self.common
            .install_engine_removal_proof(self.legacy.snapshot().await)
            .await?;
        result
    }

    async fn execute_access(&self, read: AccessStatus, write: AccessStatus) -> Result<()> {
        let generation = self.legacy.prepare_access(read, write).await?;
        let projection = self.common.prepare_access_projection(read, write).await?;
        if let Err(error) = self.legacy.publish_access(read, write, generation).await {
            self.common.fence_managed_access().await?;
            return Err(error);
        }
        if let Err(error) = self.common.publish_access_projection(projection).await {
            self.legacy.fence_writes().await?;
            self.common.fence_managed_access().await?;
            self.sync_engine_proof().await?;
            return Err(error);
        }
        self.sync_engine_proof().await
    }

    async fn sync_engine_proof(&self) -> Result<()> {
        self.common
            .restore_engine_proof(self.legacy.snapshot().await)
            .await
    }
}

pub(super) struct ReplicatorLifecycleHost {
    backend: Arc<dyn ReplicatorLifecycleBackend>,
}

impl ReplicatorLifecycleHost {
    pub(super) fn managed(
        host: Weak<RuntimeHost>,
        control: Arc<dyn Replicator>,
        primary: Arc<dyn PrimaryReplicator>,
        lifecycle: Arc<dyn ManagedReplicatorLifecycle>,
    ) -> Self {
        Self {
            backend: Arc::new(ManagedLifecycleBackend {
                legacy: lifecycle,
                common: CustomReplicatorHost::new(host, control, primary, false),
            }),
        }
    }

    pub(super) fn service(
        host: Weak<RuntimeHost>,
        control: Arc<dyn Replicator>,
        primary: Arc<dyn PrimaryReplicator>,
    ) -> Self {
        Self {
            backend: Arc::new(CustomReplicatorHost::new(host, control, primary, true)),
        }
    }

    pub(super) fn is_managed(&self) -> bool {
        self.backend.owns_stream_session()
    }

    pub(super) async fn complete_open(&self, address: String) -> Result<()> {
        self.backend.complete_open(address).await
    }

    pub(super) async fn complete_close(&self) -> Result<()> {
        self.backend.complete_close().await
    }

    pub(super) async fn complete_abort(&self) {
        self.backend.complete_abort().await;
    }

    pub(super) fn notify_abort(&self) {
        self.backend.notify_abort();
    }

    pub(super) async fn fence_writes(&self) -> Result<()> {
        self.backend.fence_writes().await
    }

    pub(super) async fn settle_primary_prefix(&self) -> Result<()> {
        self.backend.settle_primary_prefix().await
    }

    pub(super) async fn cancel_configuration_work(&self) -> Result<()> {
        self.backend.cancel_configuration_work().await
    }

    pub(super) async fn restore_authority(&self) -> Result<()> {
        self.backend.restore_authority().await
    }

    pub(super) async fn restore_access(
        &self,
        read: AccessStatus,
        write: AccessStatus,
    ) -> Result<()> {
        self.backend.restore_access(read, write).await
    }

    pub(super) async fn admit_authority(&self, authority: AdmittedAuthority) -> Result<()> {
        self.backend.admit_authority(authority).await
    }

    pub(super) async fn admit_build_authority(&self, authority: BuildAuthority) -> Result<()> {
        self.backend.admit_build_authority(authority).await
    }

    pub(super) async fn register_peer_session(
        &self,
        identity: ReplicaIdentity,
        session: ProcessSessionId,
    ) -> Result<()> {
        self.backend.register_peer_session(identity, session).await
    }

    pub(super) async fn retire_build(&self, build_id: OperationId) -> Result<()> {
        self.backend.retire_build(build_id).await
    }

    pub(super) async fn set_access(&self, read: AccessStatus, write: AccessStatus) -> Result<()> {
        self.backend.set_access(read, write).await
    }

    pub(super) async fn wait_for_catch_up(&self) -> Result<()> {
        self.backend.wait_for_catch_up().await
    }

    pub(super) async fn authorize_failover_prefix(&self, boundary: i64) -> Result<()> {
        self.backend.authorize_failover_prefix(boundary).await
    }

    pub(super) async fn prepare_switchover(
        &self,
        preparation_generation: u64,
        request_id: kuberic_protocol::types::SwitchoverRequestId,
        source: ReplicaIdentity,
        target: ReplicaIdentity,
        starting_configuration_id: kuberic_protocol::types::ConfigurationId,
        starting_epoch: kuberic_protocol::types::Epoch,
    ) -> Result<()> {
        self.backend
            .prepare_switchover(
                preparation_generation,
                request_id,
                source,
                target,
                starting_configuration_id,
                starting_epoch,
            )
            .await
    }

    pub(super) async fn refresh_progress(&self) -> Result<()> {
        self.backend.refresh_progress().await
    }

    pub(super) async fn observe_progress(&self) -> Result<()> {
        self.backend.observe_progress().await
    }

    pub(super) async fn prepare_secondary_removal(
        &self,
        intent: kuberic_protocol::types::SecondaryScaleDownIntent,
        process_session_id: ProcessSessionId,
        report_sequence: u64,
    ) -> Result<()> {
        self.backend
            .prepare_secondary_removal(intent, process_session_id, report_sequence)
            .await
    }

    pub(super) async fn observe_secondary_removal(
        &self,
        witness: kuberic_protocol::types::SecondaryRemovalWitness,
    ) -> Result<()> {
        self.backend.observe_secondary_removal(witness).await
    }

    pub(super) async fn observe_secondary_removal_progress(
        &self,
        witness: kuberic_protocol::types::SecondaryRemovalWitness,
        committed: kuberic_protocol::types::SecondaryScaleDownCleanup,
    ) -> Result<()> {
        self.backend
            .observe_secondary_removal_progress(witness, committed)
            .await
    }

    pub(super) async fn accept_secondary_removal(
        &self,
        committed: kuberic_protocol::types::SecondaryScaleDownCleanup,
    ) -> Result<()> {
        self.backend.accept_secondary_removal(committed).await
    }

    pub(super) async fn accept_historical_secondary_removal(
        &self,
        command: kuberic_protocol::command::AcceptSecondaryRemovalCommit,
    ) -> Result<()> {
        self.backend
            .accept_historical_secondary_removal(command)
            .await
    }

    pub(super) async fn fence_retirement(
        &self,
        retired: kuberic_runtime_internal::authority::RetiredAuthority,
    ) -> Result<()> {
        self.backend.fence_retirement(retired).await
    }

    pub(super) async fn complete_retirement(
        &self,
        retired: kuberic_runtime_internal::authority::RetiredAuthority,
    ) -> Result<()> {
        self.backend.complete_retirement(retired).await
    }

    pub(super) async fn snapshot(&self) -> RuntimeSnapshot {
        self.backend.snapshot().await
    }

    pub(super) async fn cancel_outbound_build(&self, id: &OperationId) -> Result<()> {
        self.backend.cancel_outbound_build(id).await
    }

    pub(super) async fn next_outbound(&self) -> Option<OutboundOperation> {
        self.backend.next_outbound().await
    }

    pub(super) async fn wait_for_build_completion(
        &self,
        build_id: &OperationId,
        target: &ReplicaIdentity,
    ) -> Result<()> {
        self.backend
            .wait_for_build_completion(build_id, target)
            .await
    }

    pub(super) async fn build_replica(&self, replica: ReplicaInformation) -> Result<()> {
        self.backend.build_replica(replica).await
    }

    pub(super) async fn remove_replica(
        &self,
        replica_id: kuberic_protocol::types::ReplicaId,
    ) -> Result<()> {
        self.backend.remove_replica(replica_id).await
    }

    pub(super) async fn select_build(&self, authority: &BuildAuthority) -> Result<()> {
        self.backend.select_build(authority).await
    }

    pub(super) async fn describe_peer(&self, replica: ReplicaInformation) -> Result<()> {
        self.backend.describe_peer(replica).await
    }

    pub(super) async fn execute_build(
        &self,
        replica: ReplicaInformation,
    ) -> Result<BuildExecution> {
        self.backend.execute_build(replica).await
    }

    pub(super) async fn enqueue_build(&self, endpoint: ReplicaEndpoint) -> Result<()> {
        self.backend.enqueue_build(endpoint).await
    }
}

pub(super) struct CustomReplicatorHost {
    host: Weak<RuntimeHost>,
    control: Arc<dyn Replicator>,
    primary: Arc<dyn PrimaryReplicator>,
    native_receipts: bool,
    abort_notified: std::sync::atomic::AtomicBool,
    gate: Mutex<()>,
    state: RwLock<RuntimeSnapshot>,
    sessions: RwLock<BTreeMap<ReplicaIdentity, ProcessSessionId>>,
    addresses: RwLock<BTreeMap<ReplicaIdentity, (ProcessSessionId, String)>>,
    retired_sessions: RwLock<BTreeSet<(ReplicaIdentity, ProcessSessionId)>>,
    retired_builds: RwLock<BTreeSet<OperationId>>,
    build_generations: RwLock<BTreeMap<OperationId, u64>>,
    receipts: RwLock<BTreeMap<OperationId, BuildReceipt>>,
    configuration: Arc<RwLock<Option<ReplicaSetConfiguration>>>,
    deferred_configuration: Mutex<Option<DeferredConfiguration>>,
    deferred_configuration_abort: StdMutex<Option<tokio::task::AbortHandle>>,
    configuration_generation: Arc<AtomicU64>,
    configuration_commit: Arc<Mutex<()>>,
    access_generation: Arc<AtomicU64>,
    access_commit: Mutex<()>,
    restored_access: RwLock<Option<(AccessStatus, AccessStatus)>>,
    removal_witnesses:
        RwLock<BTreeMap<ReplicaIdentity, kuberic_protocol::types::SecondaryRemovalWitness>>,
    outbound: mpsc::Sender<OutboundOperation>,
    receiver: Mutex<mpsc::Receiver<OutboundOperation>>,
    changed: Notify,
}

impl CustomReplicatorHost {
    pub(super) fn new(
        host: Weak<RuntimeHost>,
        control: Arc<dyn Replicator>,
        primary: Arc<dyn PrimaryReplicator>,
        native_receipts: bool,
    ) -> Self {
        let identity = host
            .upgrade()
            .expect("registering host exists")
            .identity
            .clone();
        let (outbound, receiver) = mpsc::channel(16);
        let mut snapshot = empty_snapshot(identity);
        snapshot.live_builds_only = true;
        Self {
            host,
            control,
            primary,
            native_receipts,
            abort_notified: std::sync::atomic::AtomicBool::new(false),
            gate: Mutex::new(()),
            state: RwLock::new(snapshot),
            sessions: RwLock::default(),
            addresses: RwLock::default(),
            retired_sessions: RwLock::default(),
            retired_builds: RwLock::default(),
            build_generations: RwLock::default(),
            receipts: RwLock::default(),
            configuration: Arc::new(RwLock::default()),
            deferred_configuration: Mutex::new(None),
            deferred_configuration_abort: StdMutex::new(None),
            configuration_generation: Arc::new(AtomicU64::new(0)),
            configuration_commit: Arc::new(Mutex::new(())),
            access_generation: Arc::new(AtomicU64::new(0)),
            access_commit: Mutex::new(()),
            restored_access: RwLock::default(),
            removal_witnesses: RwLock::default(),
            outbound,
            receiver: Mutex::new(receiver),
            changed: Notify::new(),
        }
    }

    fn host(&self) -> Result<Arc<RuntimeHost>> {
        let host = self.host.upgrade().ok_or(RuntimeError::Closed)?;
        if self
            .abort_notified
            .load(std::sync::atomic::Ordering::Acquire)
            || host.aborted.load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(RuntimeError::Closed);
        }
        Ok(host)
    }

    fn active_host(&self) -> Result<Arc<RuntimeHost>> {
        let host = self.host()?;
        if host.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(RuntimeError::Closed);
        }
        Ok(host)
    }

    pub(super) async fn describe_peer(&self, replica: ReplicaInformation) -> Result<()> {
        self.apply_common_action(RuntimeEffectAction::RegisterPeerSession {
            identity: replica.identity.clone(),
            session: replica.process_session_id.clone(),
        })
        .await?;
        {
            let _gate = self.gate.lock().await;
            if self.sessions.read().await.get(&replica.identity)
                != Some(&replica.process_session_id)
            {
                return Err(RuntimeError::AuthorityNotAdmitted);
            }
            if replica.replication_address.is_empty() || replica.replication_address.len() > 512 {
                return Err(RuntimeError::InvalidReplication(
                    "invalid replica address".into(),
                ));
            }
            let value = (replica.process_session_id, replica.replication_address);
            if self.addresses.read().await.get(&replica.identity) == Some(&value) {
                return Ok(());
            }
            self.addresses.write().await.insert(replica.identity, value);
        }
        self.configure().await
    }

    pub(super) async fn select_build(&self, authority: &BuildAuthority) -> Result<()> {
        {
            let _gate = self.gate.lock().await;
            self.select_build_inner(authority).await?;
        }
        self.configure().await
    }

    async fn select_build_inner(&self, authority: &BuildAuthority) -> Result<()> {
        self.host()?
            .default_dependencies
            .build_authority_store
            .select_build(authority)
            .await?;
        self.state.write().await.builds.retain(|build| {
            build.authority.target.replica_id != authority.target.replica_id
                || build.authority == *authority
        });
        self.receipts.write().await.retain(|_, receipt| {
            receipt.selection.authority.target.replica_id != authority.target.replica_id
                || receipt.selection.authority == *authority
        });
        self.changed.notify_waiters();
        Ok(())
    }

    async fn receipt(&self, authority: &BuildAuthority) -> Result<BuildReceipt> {
        let host = self.host()?;
        let selection = host
            .default_dependencies
            .build_authority_store
            .load_build_selection(&authority.target)
            .await?
            .filter(|selection| selection.authority == *authority)
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        let sessions = self.sessions.read().await;
        let local_session = &host
            .replica_session
            .get()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?
            .1;
        let session = |identity: &ReplicaIdentity| {
            if identity == &host.identity {
                Some(local_session.clone())
            } else {
                sessions.get(identity).cloned()
            }
            .ok_or(RuntimeError::AuthorityNotAdmitted)
        };
        Ok(BuildReceipt {
            selection,
            source_session: session(&authority.source)?,
            target_session: session(&authority.target)?,
            attempt_generation: self
                .build_generations
                .read()
                .await
                .get(&authority.build_id)
                .copied()
                .unwrap_or_default(),
            configuration_generation: self.configuration_generation.load(Ordering::Acquire),
        })
    }

    async fn descriptions(&self) -> Result<Option<ReplicaSetConfiguration>> {
        let host = self.host()?;
        let builds = host
            .default_dependencies
            .build_authority_store
            .load_builds()
            .await?;
        let authority = self.state.read().await.authority.clone();
        let previous = authority
            .as_ref()
            .and_then(|a| a.previous_configuration.clone());
        let admitted = authority.map(|a| a.current_configuration);
        let incoming = builds
            .iter()
            .filter(|b| b.target == host.identity)
            .max_by_key(|b| b.current_configuration.epoch)
            .map(|b| b.current_configuration.clone());
        let configuration = match (admitted, incoming) {
            (Some(admitted), Some(incoming)) if incoming.epoch > admitted.epoch => Some(incoming),
            (Some(admitted), _) => Some(admitted),
            (None, incoming) => incoming,
        };
        let previous_configuration = self
            .published_configuration()
            .await
            .as_ref()
            .map(|c| c.configuration.clone());
        let configuration = configuration.or(previous_configuration);
        let Some(configuration) = configuration else {
            return Ok(None);
        };
        let mut sessions = self.sessions.read().await.clone();
        let local_session = match host.replica_session.get() {
            Some((_, session)) => session.clone(),
            None if !self.native_receipts => ProcessSessionId::default(),
            None => return Err(RuntimeError::AuthorityNotAdmitted),
        };
        sessions.insert(host.identity.clone(), local_session.clone());
        let address = self
            .state
            .read()
            .await
            .replication_address
            .clone()
            .unwrap_or_default();
        let addresses = self.addresses.read().await.clone();
        let mut members = configuration.members.clone();
        if let Some(previous) = previous {
            for member in previous.members {
                if !members
                    .iter()
                    .any(|current| current.identity == member.identity)
                {
                    members.push(member);
                }
            }
        }
        let mut replicas = members
            .iter()
            .map(|member| {
                let mut replica = ReplicaInformation::new(
                    OperationId::default(),
                    member.identity.clone(),
                    String::new(),
                );
                replica.role = member.role;
                replica.process_session_id =
                    sessions.get(&member.identity).cloned().unwrap_or_default();
                if member.identity == host.identity {
                    replica.replication_address = address.clone();
                } else if let Some((session, address)) = addresses.get(&member.identity)
                    && session == &replica.process_session_id
                {
                    replica.replication_address = address.clone();
                }
                replica
            })
            .collect::<Vec<_>>();
        for build in builds {
            if self.retired_builds.read().await.contains(&build.build_id)
                || build.current_configuration != configuration
            {
                continue;
            }
            if host
                .default_dependencies
                .build_authority_store
                .load_build_selection(&build.target)
                .await?
                .is_none_or(|selection| selection.authority != build)
            {
                continue;
            }
            if !replicas.iter().any(|r| r.identity == build.source) {
                continue;
            }
            let mut target =
                ReplicaInformation::new(build.build_id, build.target.clone(), String::new());
            target.process_session_id = sessions.get(&build.target).cloned().unwrap_or_default();
            target.current_progress = build.replication_boundary_lsn;
            target.catch_up_capability = build.replication_boundary_lsn;
            if build.target == host.identity {
                target.replication_address = address.clone();
            }
            replicas.push(target);
        }
        if !replicas.iter().any(|r| r.identity == host.identity) {
            let mut local =
                ReplicaInformation::new(OperationId::default(), host.identity.clone(), address);
            local.process_session_id = local_session;
            replicas.push(local);
        }
        Ok(Some(ReplicaSetConfiguration {
            configuration,
            replicas,
        }))
    }

    async fn published_configuration(&self) -> Option<ReplicaSetConfiguration> {
        let _commit = self.configuration_commit.lock().await;
        self.configuration.read().await.clone()
    }

    async fn configuration_update(
        &self,
    ) -> Result<Option<(ReplicaSetConfiguration, Option<ConfigurationDescriptor>)>> {
        let Some(current) = self.descriptions().await? else {
            return Ok(None);
        };
        let previous = self
            .state
            .read()
            .await
            .authority
            .as_ref()
            .and_then(|a| a.previous_configuration.clone());
        Ok(Some((current, previous)))
    }

    async fn apply_configuration_update(
        primary: Arc<dyn PrimaryReplicator>,
        current: ReplicaSetConfiguration,
        previous: Option<ConfigurationDescriptor>,
    ) -> Result<ReplicaSetConfiguration> {
        if let Some(previous) = previous {
            primary
                .update_catch_up_replica_set_configuration(
                    current.clone(),
                    ReplicaSetConfiguration {
                        replicas: current
                            .replicas
                            .iter()
                            .filter_map(|replica| {
                                let member = previous
                                    .members
                                    .iter()
                                    .find(|m| m.identity == replica.identity)?;
                                let mut replica = replica.clone();
                                replica.role = member.role;
                                Some(replica)
                            })
                            .collect(),
                        configuration: previous,
                    },
                )
                .await?;
        } else {
            primary
                .update_current_replica_set_configuration(current.clone())
                .await?;
        }
        Ok(current)
    }

    fn advance_configuration_generation(&self) -> Result<u64> {
        self.configuration_generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |generation| {
                generation.checked_add(1)
            })
            .map(|generation| generation + 1)
            .map_err(|_| RuntimeError::OperationCancelled)
    }

    fn advance_access_generation(&self) -> Result<u64> {
        self.access_generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |generation| {
                generation.checked_add(1)
            })
            .map(|generation| generation + 1)
            .map_err(|_| RuntimeError::OperationCancelled)
    }

    async fn invalidate_access_projections(&self) -> Result<()> {
        let _commit = self.access_commit.lock().await;
        self.advance_access_generation()?;
        Ok(())
    }

    fn ensure_configuration_generation(&self, generation: u64) -> Result<()> {
        self.active_host()?;
        if self.configuration_generation.load(Ordering::Acquire) != generation {
            return Err(RuntimeError::OperationCancelled);
        }
        Ok(())
    }

    async fn finish_deferred_configuration(&self) -> Result<()> {
        let Some(mut handle) = self.deferred_configuration.lock().await.take() else {
            return Ok(());
        };
        let (generation, _) =
            match tokio::time::timeout(std::time::Duration::from_secs(75), &mut handle).await {
                Ok(result) => result.map_err(|error| {
                    if error.is_cancelled() {
                        RuntimeError::OperationCancelled
                    } else {
                        RuntimeError::Application(error.to_string())
                    }
                })??,
                Err(_) => {
                    handle.abort();
                    return Err(RuntimeError::OperationCancelled);
                }
            };
        self.deferred_configuration_abort.lock().unwrap().take();
        self.ensure_configuration_generation(generation)?;
        Ok(())
    }

    async fn defer_configuration(&self) -> Result<()> {
        {
            let _commit = self.configuration_commit.lock().await;
            self.deferred_configuration_abort.lock().unwrap().take();
            if let Some(handle) = self.deferred_configuration.lock().await.take() {
                handle.abort();
                let _ = handle.await;
            }
        }
        let authority_before = self.state.read().await.authority.clone();
        let sessions_before = self.sessions.read().await.clone();
        let Some((current, previous)) = self.configuration_update().await? else {
            return Ok(());
        };
        let _commit = self.configuration_commit.lock().await;
        self.active_host()?;
        if authority_before != self.state.read().await.authority
            || sessions_before != *self.sessions.read().await
        {
            return Err(RuntimeError::OperationCancelled);
        }
        let generation = self.advance_configuration_generation()?;
        let configuration_generation = self.configuration_generation.clone();
        let configuration = self.configuration.clone();
        let configuration_commit = self.configuration_commit.clone();
        let host = self.host.clone();
        let primary = self.primary.clone();
        let handle = tokio::spawn(async move {
            let current = tokio::time::timeout(
                std::time::Duration::from_secs(75),
                Self::apply_configuration_update(primary, current, previous),
            )
            .await
            .map_err(|_| RuntimeError::OperationCancelled)??;
            if configuration_generation.load(Ordering::Acquire) != generation {
                return Err(RuntimeError::OperationCancelled);
            }
            let _commit = configuration_commit.lock().await;
            let active = host.upgrade().is_some_and(|host| {
                !host.aborted.load(std::sync::atomic::Ordering::Acquire)
                    && !host.closed.load(std::sync::atomic::Ordering::Acquire)
            });
            if !active || configuration_generation.load(Ordering::Acquire) != generation {
                return Err(RuntimeError::OperationCancelled);
            }
            *configuration.write().await = Some(current.clone());
            let still_active = host.upgrade().is_some_and(|host| {
                !host.aborted.load(std::sync::atomic::Ordering::Acquire)
                    && !host.closed.load(std::sync::atomic::Ordering::Acquire)
            });
            if !still_active || configuration_generation.load(Ordering::Acquire) != generation {
                *configuration.write().await = None;
                return Err(RuntimeError::OperationCancelled);
            }
            Ok((generation, current))
        });
        *self.deferred_configuration_abort.lock().unwrap() = Some(handle.abort_handle());
        *self.deferred_configuration.lock().await = Some(handle);
        Ok(())
    }

    async fn configure(&self) -> Result<()> {
        self.finish_deferred_configuration().await?;
        let Some((current, previous)) = self.configuration_update().await? else {
            return Ok(());
        };
        let generation = {
            let _commit = self.configuration_commit.lock().await;
            if self.configuration.read().await.as_ref() == Some(&current) {
                self.configuration_generation.load(Ordering::Acquire)
            } else {
                self.advance_configuration_generation()?
            }
        };
        let current =
            Self::apply_configuration_update(self.primary.clone(), current, previous).await?;
        let _commit = self.configuration_commit.lock().await;
        self.ensure_configuration_generation(generation)?;
        *self.configuration.write().await = Some(current);
        if let Err(error) = self.ensure_configuration_generation(generation) {
            *self.configuration.write().await = None;
            return Err(error);
        }
        Ok(())
    }

    async fn prepare_access_projection(
        &self,
        read: AccessStatus,
        write: AccessStatus,
    ) -> Result<AccessProjection> {
        *self.restored_access.write().await = None;
        let host = self.host()?;
        let faulted_grant = (read == AccessStatus::Granted || write == AccessStatus::Granted)
            && host.state.read().await.reported_fault.is_some();
        let (read, write) = if faulted_grant {
            (
                AccessStatus::ReconfigurationPending,
                AccessStatus::ReconfigurationPending,
            )
        } else {
            (read, write)
        };
        let authority_before = self.state.read().await.authority.clone();
        let configuration_before = self.published_configuration().await;
        let sessions_before = self.sessions.read().await.clone();
        let role_before = host.state.read().await.fallback_snapshot.role;
        let configuration_generation = self.configuration_generation.load(Ordering::Acquire);
        let access_generation = self.access_generation.load(Ordering::Acquire);
        if write == AccessStatus::Granted {
            if role_before != kuberic_protocol::types::ReplicaRole::Primary {
                return Err(RuntimeError::NotPrimary);
            }
            let authority = authority_before
                .as_ref()
                .ok_or(RuntimeError::AuthorityNotAdmitted)?;
            if authority.primary_identity() != &host.identity {
                return Err(RuntimeError::NotPrimary);
            }
            let state = self.state.read().await;
            if authority
                .secondary_removal
                .as_ref()
                .is_some_and(|evidence| {
                    authority.previous_configuration.is_some()
                        || state
                            .accepted_secondary_removal
                            .as_ref()
                            .is_none_or(|committed| &committed.evidence != evidence)
                })
            {
                return Err(RuntimeError::ReconfigurationPending);
            }
        }
        let progress =
            super::with_access_proof_view(read, write, self.control.current_progress()).await;
        if let Err(error) = progress {
            let mut state = self.state.write().await;
            state.read_status = AccessStatus::ReconfigurationPending;
            state.write_status = AccessStatus::ReconfigurationPending;
            drop(state);
            let mut state = host.state.write().await;
            state.fallback_snapshot.read_status = AccessStatus::ReconfigurationPending;
            state.fallback_snapshot.write_status = AccessStatus::ReconfigurationPending;
            if matches!(error, RuntimeError::ReconfigurationPending) {
                return Err(error);
            }
            if matches!(error, RuntimeError::OperationCancelled) {
                return Err(error);
            }
            self.control.abort();
            return Err(error);
        }
        self.active_host()?;
        let projection = AccessProjection {
            read,
            write,
            authority: authority_before,
            configuration: configuration_before,
            sessions: sessions_before,
            role: role_before,
            configuration_generation,
            access_generation,
            faulted_grant,
        };
        self.validate_access_projection(&projection).await?;
        Ok(projection)
    }

    async fn validate_access_projection(&self, projection: &AccessProjection) -> Result<()> {
        let host = self.active_host()?;
        if projection.authority != self.state.read().await.authority
            || projection.configuration != self.published_configuration().await
            || projection.sessions != *self.sessions.read().await
            || projection.role != host.state.read().await.fallback_snapshot.role
            || projection.configuration_generation
                != self.configuration_generation.load(Ordering::Acquire)
            || projection.access_generation != self.access_generation.load(Ordering::Acquire)
        {
            return Err(RuntimeError::OperationCancelled);
        }
        Ok(())
    }

    async fn publish_access_projection(&self, projection: AccessProjection) -> Result<()> {
        let _commit = self.access_commit.lock().await;
        self.validate_access_projection(&projection).await?;
        {
            let mut state = self.state.write().await;
            state.read_status = projection.read;
            state.write_status = projection.write;
        }
        let host = match self.active_host() {
            Ok(host) => host,
            Err(error) => {
                let mut state = self.state.write().await;
                state.read_status = AccessStatus::ReconfigurationPending;
                state.write_status = AccessStatus::ReconfigurationPending;
                return Err(error);
            }
        };
        {
            let mut state = host.state.write().await;
            state.fallback_snapshot.read_status = projection.read;
            state.fallback_snapshot.write_status = projection.write;
        }
        if let Err(error) = self.validate_access_projection(&projection).await {
            let mut state = self.state.write().await;
            state.read_status = AccessStatus::ReconfigurationPending;
            state.write_status = AccessStatus::ReconfigurationPending;
            drop(state);
            let mut state = host.state.write().await;
            state.fallback_snapshot.read_status = AccessStatus::ReconfigurationPending;
            state.fallback_snapshot.write_status = AccessStatus::ReconfigurationPending;
            return Err(error);
        }
        if projection.faulted_grant {
            Err(RuntimeError::ReconfigurationPending)
        } else {
            Ok(())
        }
    }

    async fn set_access(&self, read: AccessStatus, write: AccessStatus) -> Result<()> {
        let projection = self.prepare_access_projection(read, write).await?;
        self.publish_access_projection(projection).await
    }

    async fn record_completion(&self, receipt: BuildReceipt) -> Result<()> {
        let authority = receipt.selection.authority.clone();
        let host = self.host()?;
        if self.receipt(&authority).await? != receipt {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        if self
            .retired_builds
            .read()
            .await
            .contains(&authority.build_id)
            || host
                .default_dependencies
                .build_authority_store
                .load_build(&authority.build_id)
                .await?
                .as_ref()
                != Some(&authority)
        {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        if authority.source == host.identity
            && self
                .state
                .read()
                .await
                .authority
                .as_ref()
                .is_none_or(|a| a.current_configuration != authority.current_configuration)
        {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        let existing = host
            .default_dependencies
            .build_progress_store
            .load_build_progress(&authority.build_id)
            .await?;
        let progress = DurableBuildProgress {
            last_sequence: existing
                .as_ref()
                .map_or(Some(1), |p| p.last_sequence.checked_add(1))
                .ok_or_else(|| {
                    RuntimeError::InvalidReplication("build sequence exhausted".into())
                })?,
            durable_lsn: authority.replication_boundary_lsn,
            catch_up_boundary_lsn: Some(authority.replication_boundary_lsn),
            completed: true,
            authority,
        };
        host.default_dependencies
            .build_progress_store
            .record_selected_build_progress(&receipt.selection, &progress)
            .await?;
        self.receipts
            .write()
            .await
            .insert(progress.authority.build_id.clone(), receipt);
        let mut state = self.state.write().await;
        state
            .builds
            .retain(|b| b.authority.build_id != progress.authority.build_id);
        state.builds.push(BuildPostcondition {
            authority: progress.authority,
            last_sequence: progress.last_sequence,
            durable_lsn: progress.durable_lsn,
            completed: true,
            catch_up_boundary_lsn: progress.catch_up_boundary_lsn,
        });
        Ok(())
    }

    async fn record_build_start(&self, receipt: &BuildReceipt) -> Result<()> {
        let authority = receipt.selection.authority.clone();
        let host = self.host()?;
        if self.receipt(&authority).await? != *receipt {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        let existing = host
            .default_dependencies
            .build_progress_store
            .load_build_progress(&authority.build_id)
            .await?;
        let progress = match existing {
            Some(progress) if progress.authority == authority => progress,
            Some(_) => {
                return Err(RuntimeError::AuthorityMismatch(
                    "build progress belongs to different authority".into(),
                ));
            }
            None => {
                let progress = DurableBuildProgress {
                    authority,
                    last_sequence: 0,
                    durable_lsn: receipt.selection.authority.replication_boundary_lsn,
                    completed: false,
                    catch_up_boundary_lsn: None,
                };
                host.default_dependencies
                    .build_progress_store
                    .record_selected_build_progress(&receipt.selection, &progress)
                    .await?;
                progress
            }
        };
        let mut state = self.state.write().await;
        state
            .builds
            .retain(|build| build.authority.build_id != progress.authority.build_id);
        state.builds.push(BuildPostcondition {
            authority: progress.authority,
            last_sequence: progress.last_sequence,
            durable_lsn: progress.durable_lsn,
            completed: progress.completed,
            catch_up_boundary_lsn: progress.catch_up_boundary_lsn,
        });
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    async fn refresh(&self) -> Result<()> {
        let host = self.host()?;
        if self.native_receipts
            && host
                .default_dependencies
                .build_authority_store
                .load_build_selection(&host.identity)
                .await?
                .is_some()
        {
            self.configure().await?;
        }
        let restored = *self.restored_access.read().await;
        if let Some((read, write)) = restored {
            self.try_restore_access(read, write).await?;
        }
        let incoming = match host
            .default_dependencies
            .build_authority_store
            .load_build_selection(&host.identity)
            .await?
        {
            Some(selection) => self.receipt(&selection.authority).await.ok(),
            None => None,
        };
        let authority_before = self.state.read().await.authority.clone();
        let sessions_before = self.sessions.read().await.clone();
        let configuration_generation = self.configuration_generation.load(Ordering::Acquire);
        let progress = match self.control.current_progress().await {
            Ok(progress) => progress,
            Err(RuntimeError::ReconfigurationPending) => {
                let mut state = self.state.write().await;
                state.read_status = AccessStatus::ReconfigurationPending;
                state.write_status = AccessStatus::ReconfigurationPending;
                drop(state);
                let mut state = host.state.write().await;
                state.fallback_snapshot.read_status = AccessStatus::ReconfigurationPending;
                state.fallback_snapshot.write_status = AccessStatus::ReconfigurationPending;
                return Err(RuntimeError::ReconfigurationPending);
            }
            Err(error) => return Err(error),
        };
        self.active_host()?;
        if authority_before != self.state.read().await.authority
            || sessions_before != *self.sessions.read().await
            || configuration_generation != self.configuration_generation.load(Ordering::Acquire)
        {
            return Err(RuntimeError::OperationCancelled);
        }
        if progress < 0 {
            return Err(RuntimeError::InvalidReplication(
                "negative custom replicator progress".into(),
            ));
        }
        let described = self.published_configuration().await;
        if let Some(receipt) = incoming {
            let build = &receipt.selection.authority;
            let exact_description = described.as_ref().is_some_and(|c| {
                c.configuration == build.current_configuration
                    && c.replicas
                        .iter()
                        .filter(|r| r.identity == host.identity && !r.build_id.is_empty())
                        .count()
                        == 1
                    && c.replicas.iter().any(|r| {
                        r.build_id == build.build_id
                            && r.identity == build.target
                            && r.process_session_id == receipt.target_session
                            && r.current_progress == build.replication_boundary_lsn
                    })
            });
            if exact_description
                && !self.retired_builds.read().await.contains(&build.build_id)
                && progress > 0
                && progress >= build.replication_boundary_lsn
                && self
                    .receipts
                    .read()
                    .await
                    .get(&build.build_id)
                    .is_none_or(|stored| !stored.matches_durable_selection(&receipt))
            {
                self.record_completion(receipt).await?;
            }
        }
        let mut state = self.state.write().await;
        state.current_progress = progress;
        state.committed_lsn = progress;
        state.current_configuration_quorum_progress = progress;
        state.verified_replication_lsn = state.authority.as_ref().map(|_| progress);
        Ok(())
    }

    pub(super) async fn enqueue_build(&self, endpoint: ReplicaEndpoint) -> Result<()> {
        let mut generations = self.build_generations.write().await;
        let generation = generations.entry(endpoint.build_id.clone()).or_default();
        *generation = generation
            .checked_add(1)
            .ok_or(RuntimeError::OperationCancelled)?;
        drop(generations);
        self.enqueue_outbound(OutboundOperation::Build(endpoint))
    }

    async fn enqueue_build_wait(&self, endpoint: ReplicaEndpoint) -> Result<()> {
        let mut generations = self.build_generations.write().await;
        let generation = generations.entry(endpoint.build_id.clone()).or_default();
        *generation = generation
            .checked_add(1)
            .ok_or(RuntimeError::OperationCancelled)?;
        drop(generations);
        let mut operation = OutboundOperation::Build(endpoint);
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(75);
        loop {
            match self.outbound.try_send(operation) {
                Ok(()) => return Ok(()),
                Err(mpsc::error::TrySendError::Full(returned)) => {
                    operation = returned;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    return Err(RuntimeError::Closed);
                }
            }
            self.host()?;
            if tokio::time::Instant::now() >= deadline {
                return Err(RuntimeError::QueueFull);
            }
            let changed = self.changed.notified();
            tokio::select! {
                _ = changed => {}
                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
            }
        }
    }

    fn enqueue_outbound(&self, operation: OutboundOperation) -> Result<()> {
        self.outbound
            .try_send(operation)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => RuntimeError::QueueFull,
                mpsc::error::TrySendError::Closed(_) => RuntimeError::Closed,
            })
    }

    async fn prepare_build_description(
        &self,
        replica: &mut ReplicaInformation,
    ) -> Result<BuildAuthority> {
        let host = self.host()?;
        let _gate = self.gate.lock().await;
        let authority = host
            .default_dependencies
            .build_authority_store
            .load_build(&replica.build_id)
            .await?
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        if authority.source != host.identity || authority.target != replica.identity {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        if self
            .retired_builds
            .read()
            .await
            .contains(&authority.build_id)
            || self
                .state
                .read()
                .await
                .authority
                .as_ref()
                .is_none_or(|a| a.current_configuration != authority.current_configuration)
        {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        let sessions = self.sessions.read().await.clone();
        replica.process_session_id = sessions
            .get(&replica.identity)
            .cloned()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        replica.current_progress = authority.replication_boundary_lsn;
        replica.catch_up_capability = authority.replication_boundary_lsn;
        self.configure().await?;
        Ok(authority)
    }

    async fn prepare_build(&self, replica: &mut ReplicaInformation) -> Result<BuildReceipt> {
        let authority = self.prepare_build_description(replica).await?;
        self.receipt(&authority).await
    }

    pub(super) async fn execute_build(&self, mut replica: ReplicaInformation) -> Result<()> {
        let receipt = self.prepare_build(&mut replica).await?;
        self.record_build_start(&receipt).await?;
        self.primary.build_replica(replica).await?;
        self.active_host()?;
        let _gate = self.gate.lock().await;
        self.record_completion(receipt).await?;
        self.refresh().await
    }

    async fn complete_open_common(&self, address: String) {
        let mut state = self.state.write().await;
        state.open = true;
        state.replication_address = Some(address);
    }

    async fn terminate(&self) -> Result<()> {
        *self.restored_access.write().await = None;
        self.removal_witnesses.write().await.clear();
        self.invalidate_build_attempts().await?;
        self.receiver.lock().await.close();
        let mut state = self.state.write().await;
        state.open = false;
        state.role = kuberic_protocol::types::ReplicaRole::None;
        state.role_transition = None;
        state.read_status = AccessStatus::NotPrimary;
        state.write_status = AccessStatus::NotPrimary;
        state.builds.clear();
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    async fn mark_not_open(&self) {
        self.state.write().await.open = false;
        if let Some(host) = self.host.upgrade() {
            host.state.write().await.fallback_snapshot.open = false;
        }
    }

    async fn invalidate_configuration_attempts(&self) -> Result<()> {
        self.invalidate_access_projections().await?;
        let _commit = self.configuration_commit.lock().await;
        self.advance_configuration_generation()?;
        self.deferred_configuration_abort.lock().unwrap().take();
        if let Some(handle) = self.deferred_configuration.lock().await.take() {
            handle.abort();
            let _ = handle.await;
        }
        Ok(())
    }

    async fn invalidate_build_attempts(&self) -> Result<()> {
        self.invalidate_configuration_attempts().await?;
        for generation in self.build_generations.write().await.values_mut() {
            *generation = generation
                .checked_add(1)
                .ok_or(RuntimeError::OperationCancelled)?;
        }
        self.changed.notify_waiters();
        Ok(())
    }

    async fn prepare_authority_admission(&self, authority: &AdmittedAuthority) -> Result<()> {
        let previous = self.state.read().await.authority.clone();
        if previous.as_ref() != Some(authority) {
            self.invalidate_configuration_attempts().await?;
        }
        let preserve_access = previous.as_ref().is_some_and(|existing| {
            existing == authority || preserves_same_primary_scale_up_access(existing, authority)
        });
        if preserve_access {
            return Ok(());
        }
        if self.native_receipts {
            return self.fence_writes().await;
        }
        self.fence_managed_access().await
    }

    async fn fence_managed_access(&self) -> Result<()> {
        *self.restored_access.write().await = None;
        self.removal_witnesses.write().await.clear();
        self.invalidate_build_attempts().await?;
        self.set_access(
            AccessStatus::ReconfigurationPending,
            AccessStatus::ReconfigurationPending,
        )
        .await
    }

    async fn restore_engine_proof(&self, snapshot: RuntimeSnapshot) -> Result<()> {
        let mut state = self.state.write().await;
        state.committed_lsn = snapshot.committed_lsn;
        state.current_progress = snapshot.current_progress;
        state.verified_replication_lsn = snapshot.verified_replication_lsn;
        state.current_configuration_quorum_progress =
            snapshot.current_configuration_quorum_progress;
        state.catch_up_boundary = snapshot.catch_up_boundary;
        state.catch_up_complete = snapshot.catch_up_complete;
        merge_builds(&mut state.builds, snapshot.builds);
        state.prepared_secondary_removal = snapshot.prepared_secondary_removal;
        state.accepted_secondary_removal = snapshot.accepted_secondary_removal;
        state.retired_authority = snapshot.retired_authority;
        Ok(())
    }

    async fn restore_builds(&self) -> Result<()> {
        let host = self.host()?;
        let authorities = host
            .default_dependencies
            .build_authority_store
            .load_builds()
            .await?;
        let mut builds = Vec::with_capacity(authorities.len());
        for authority in authorities
            .into_iter()
            .filter(|authority| authority.target == host.identity)
        {
            let progress = host
                .default_dependencies
                .build_progress_store
                .load_build_progress(&authority.build_id)
                .await?;
            let build = match progress {
                Some(progress) if progress.authority == authority => BuildPostcondition {
                    authority,
                    last_sequence: progress.last_sequence,
                    durable_lsn: progress.durable_lsn,
                    completed: progress.completed,
                    catch_up_boundary_lsn: progress.catch_up_boundary_lsn,
                },
                Some(_) => {
                    return Err(RuntimeError::AuthorityMismatch(
                        "build progress belongs to different authority".into(),
                    ));
                }
                None => BuildPostcondition {
                    authority,
                    last_sequence: 0,
                    durable_lsn: 0,
                    completed: false,
                    catch_up_boundary_lsn: None,
                },
            };
            builds.push(build);
        }
        self.state.write().await.builds = builds;
        Ok(())
    }

    async fn install_managed_authority(&self, authority: &AdmittedAuthority) -> Result<()> {
        let _gate = self.gate.lock().await;
        authority.validate()?;
        let previous = self.state.read().await.authority.clone();
        self.state.write().await.authority = Some(authority.clone());
        if let Some(previous) = previous {
            let retained = authority
                .current_configuration
                .members
                .iter()
                .chain(
                    authority
                        .previous_configuration
                        .iter()
                        .flat_map(|configuration| &configuration.members),
                )
                .map(|member| member.identity.clone())
                .collect::<BTreeSet<_>>();
            for identity in previous
                .current_configuration
                .members
                .iter()
                .chain(
                    previous
                        .previous_configuration
                        .iter()
                        .flat_map(|configuration| &configuration.members),
                )
                .map(|member| member.identity.clone())
                .filter(|identity| !retained.contains(identity))
            {
                self.enqueue_outbound(OutboundOperation::Evict(identity))?;
            }
        }
        self.configure().await
    }

    async fn retire_managed_build(&self, build_id: OperationId) -> Result<()> {
        let host = self.host()?;
        let build = host
            .default_dependencies
            .build_authority_store
            .load_build(&build_id)
            .await?;
        self.execute_common_build_action(RuntimeEffectAction::RetireBuild(build_id))
            .await?;
        if let Some(build) = build {
            self.enqueue_outbound(OutboundOperation::Remove(build.target.replica_id))?;
        }
        Ok(())
    }

    async fn remove_managed_replica(
        &self,
        replica_id: kuberic_protocol::types::ReplicaId,
    ) -> Result<()> {
        let retired = self
            .state
            .read()
            .await
            .builds
            .iter()
            .filter(|build| build.authority.target.replica_id == replica_id)
            .map(|build| build.authority.build_id.clone())
            .collect::<Vec<_>>();
        self.state
            .write()
            .await
            .builds
            .retain(|build| build.authority.target.replica_id != replica_id);
        self.receipts
            .write()
            .await
            .retain(|_, receipt| receipt.selection.authority.target.replica_id != replica_id);
        self.retired_builds.write().await.extend(retired);
        self.changed.notify_waiters();
        self.enqueue_outbound(OutboundOperation::Remove(replica_id))
    }

    async fn install_managed_build(&self, authority: &BuildAuthority) -> Result<()> {
        let _gate = self.gate.lock().await;
        authority.validate()?;
        let host = self.host()?;
        if self
            .retired_builds
            .read()
            .await
            .contains(&authority.build_id)
        {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        self.select_build_inner(authority).await?;
        self.restore_builds().await?;
        let completed = self.state.read().await.builds.iter().any(|build| {
            build.authority == *authority
                && build.completed
                && build.durable_lsn >= authority.replication_boundary_lsn
        });
        if authority.target == host.identity && !completed {
            let read = self.state.read().await.read_status;
            self.set_access(read, AccessStatus::ReconfigurationPending)
                .await?;
        }
        if self.state.read().await.authority.is_some() {
            self.configure().await?;
        }
        Ok(())
    }

    async fn admit_common_build(&self, authority: BuildAuthority) -> Result<()> {
        let (configure, defer) = {
            let _gate = self.gate.lock().await;
            authority.validate()?;
            let host = self.host()?;
            if self
                .retired_builds
                .read()
                .await
                .contains(&authority.build_id)
            {
                return Err(RuntimeError::AuthorityNotAdmitted);
            }
            let superseding = host
                .default_dependencies
                .build_authority_store
                .load_build_selection(&authority.target)
                .await?
                .is_some_and(|selection| selection.authority != authority);
            host.default_dependencies
                .build_authority_store
                .admit_build(&authority)
                .await?;
            self.select_build_inner(&authority).await?;
            let completed = self.state.read().await.builds.iter().any(|build| {
                build.authority == authority
                    && build.completed
                    && build.durable_lsn >= authority.replication_boundary_lsn
            });
            if authority.target == host.identity && !completed {
                let read = self.state.read().await.read_status;
                if self.native_receipts && superseding {
                    let mut state = self.state.write().await;
                    state.read_status = read;
                    state.write_status = AccessStatus::ReconfigurationPending;
                    drop(state);
                    let mut state = host.state.write().await;
                    state.fallback_snapshot.read_status = read;
                    state.fallback_snapshot.write_status = AccessStatus::ReconfigurationPending;
                } else {
                    self.set_access(read, AccessStatus::ReconfigurationPending)
                        .await?;
                }
            }
            let configure = self.native_receipts || self.state.read().await.authority.is_some();
            (
                configure && !(self.native_receipts && superseding),
                configure && self.native_receipts && superseding,
            )
        };
        if defer {
            self.defer_configuration().await?;
        } else if configure {
            self.configure().await?;
        }
        Ok(())
    }

    async fn install_engine_removal_proof(&self, snapshot: RuntimeSnapshot) -> Result<()> {
        {
            let mut state = self.state.write().await;
            state.prepared_secondary_removal = snapshot.prepared_secondary_removal;
            state.accepted_secondary_removal = snapshot.accepted_secondary_removal;
            state.retired_authority = snapshot.retired_authority;
            state.current_progress = snapshot.current_progress;
            state.verified_replication_lsn = snapshot.verified_replication_lsn;
            state.committed_lsn = snapshot.committed_lsn;
            state.current_configuration_quorum_progress =
                snapshot.current_configuration_quorum_progress;
        }
        Ok(())
    }

    async fn fence_retirement_state(
        &self,
        retired: &kuberic_runtime_internal::authority::RetiredAuthority,
    ) -> Result<()> {
        let host = self.host()?;
        retired.validate(&host.identity)?;
        self.invalidate_configuration_attempts().await?;
        *self.restored_access.write().await = None;
        self.removal_witnesses.write().await.clear();
        {
            let mut state = self.state.write().await;
            state.authority = None;
            state.verified_replication_lsn = None;
            state.read_status = AccessStatus::NotPrimary;
            state.write_status = AccessStatus::NotPrimary;
            state.builds.clear();
        }
        let mut state = host.state.write().await;
        state.fallback_snapshot.authority = None;
        state.fallback_snapshot.read_status = AccessStatus::NotPrimary;
        state.fallback_snapshot.write_status = AccessStatus::NotPrimary;
        Ok(())
    }

    async fn complete_retirement_state(
        &self,
        retired: &kuberic_runtime_internal::authority::RetiredAuthority,
    ) -> Result<()> {
        let host = self.host()?;
        retired.validate(&host.identity)?;
        let closed = host.state.read().await.fallback_snapshot.clone();
        if closed.open || closed.role != kuberic_protocol::types::ReplicaRole::None {
            return Err(RuntimeError::ReconfigurationPending);
        }
        self.terminate().await?;
        let mut state = self.state.write().await;
        state.retired_authority = Some(retired.clone());
        state.authority = None;
        drop(state);
        host.state.write().await.fallback_snapshot.authority = None;
        Ok(())
    }

    async fn execute_common_build_action(&self, action: RuntimeEffectAction) -> Result<()> {
        let _gate = self.gate.lock().await;
        let host = self.host()?;
        match action {
            RuntimeEffectAction::AdmitBuildAuthority(authority) => {
                authority.validate()?;
                if self
                    .retired_builds
                    .read()
                    .await
                    .contains(&authority.build_id)
                {
                    return Err(RuntimeError::AuthorityNotAdmitted);
                }
                host.default_dependencies
                    .build_authority_store
                    .admit_build(&authority)
                    .await?;
                self.select_build_inner(&authority).await?;
                Ok(())
            }
            RuntimeEffectAction::RegisterPeerSession { identity, session } => {
                if session.is_empty()
                    || self
                        .retired_sessions
                        .read()
                        .await
                        .contains(&(identity.clone(), session.clone()))
                {
                    return Err(RuntimeError::AuthorityNotAdmitted);
                }
                let old = self
                    .sessions
                    .write()
                    .await
                    .insert(identity.clone(), session.clone());
                if let Some(old) = old.filter(|old| old != &session) {
                    self.retired_sessions
                        .write()
                        .await
                        .insert((identity.clone(), old));
                    self.state.write().await.builds.retain(|b| {
                        b.authority.source != identity && b.authority.target != identity
                    });
                    self.invalidate_build_attempts().await?;
                }
                if self.native_receipts {
                    self.configure().await?;
                }
                Ok(())
            }
            RuntimeEffectAction::RetireBuild(id) => {
                self.retired_builds.write().await.insert(id.clone());
                self.state
                    .write()
                    .await
                    .builds
                    .retain(|b| b.authority.build_id != id);
                self.changed.notify_waiters();
                Ok(())
            }
            _ => Err(RuntimeError::Application(
                "action is not common build/session work".into(),
            )),
        }
    }

    async fn cancel_common_build(&self, id: &OperationId) -> Result<()> {
        let mut generations = self.build_generations.write().await;
        let generation = generations.entry(id.clone()).or_default();
        *generation = generation
            .checked_add(1)
            .ok_or(RuntimeError::OperationCancelled)?;
        drop(generations);
        self.state
            .write()
            .await
            .builds
            .retain(|b| &b.authority.build_id != id);
        self.changed.notify_waiters();
        Ok(())
    }

    async fn build_generation(&self, id: &OperationId) -> u64 {
        self.build_generations
            .read()
            .await
            .get(id)
            .copied()
            .unwrap_or_default()
    }

    async fn ensure_build_generation(&self, id: &OperationId, generation: u64) -> Result<()> {
        self.active_host()?;
        if self.build_generation(id).await != generation
            || self.retired_builds.read().await.contains(id)
        {
            return Err(RuntimeError::OperationCancelled);
        }
        Ok(())
    }
}

impl CustomReplicatorHost {
    pub(super) async fn restore_access(
        &self,
        read: AccessStatus,
        write: AccessStatus,
    ) -> Result<()> {
        let _gate = self.gate.lock().await;
        self.try_restore_access(read, write).await
    }

    async fn try_restore_access(&self, read: AccessStatus, write: AccessStatus) -> Result<()> {
        let result = self.set_access(read, write).await;
        if matches!(result, Err(RuntimeError::ReconfigurationPending)) {
            // Retain the durable intent, not a successful effect receipt. Progress
            // reconciliation retries it until discovery/application readiness converge.
            *self.restored_access.write().await = Some((read, write));
        }
        result
    }

    async fn retry_restored_access(&self) -> Result<()> {
        let restored = *self.restored_access.read().await;
        if let Some((read, write)) = restored {
            self.try_restore_access(read, write).await?;
        }
        Ok(())
    }

    pub(super) async fn complete_open(&self, address: String) -> Result<()> {
        self.complete_open_common(address).await;
        Ok(())
    }
    pub(super) async fn fence_writes(&self) -> Result<()> {
        *self.restored_access.write().await = None;
        self.removal_witnesses.write().await.clear();
        let configuration = self.published_configuration().await;
        if let Some(configuration) = configuration {
            self.invalidate_build_attempts().await?;
            self.control
                .update_epoch(configuration.configuration.epoch)
                .await?;
        }
        self.set_access(
            AccessStatus::ReconfigurationPending,
            AccessStatus::ReconfigurationPending,
        )
        .await
    }
    pub(super) async fn settle_primary_prefix(&self) -> Result<()> {
        self.refresh().await
    }
    pub(super) async fn cancel_configuration_work(&self) -> Result<()> {
        let _gate = self.gate.lock().await;
        *self.restored_access.write().await = None;
        self.removal_witnesses.write().await.clear();
        self.invalidate_build_attempts().await
    }
    pub(super) async fn restore_authority(&self) -> Result<()> {
        let _gate = self.gate.lock().await;
        self.state.write().await.authority = self
            .host()?
            .default_dependencies
            .replica_authority_store
            .load()
            .await?;
        self.state.write().await.prepared_secondary_removal = self
            .host()?
            .default_dependencies
            .replica_authority_store
            .load_secondary_removal()
            .await?;
        self.restore_builds().await?;
        if self.native_receipts || self.state.read().await.authority.is_some() {
            self.configure().await?;
        }
        self.refresh().await
    }

    async fn wait_for_common_catchup(&self) -> Result<()> {
        let (authority_before, sessions_before, configuration_generation) = {
            let _gate = self.gate.lock().await;
            (
                self.state.read().await.authority.clone(),
                self.sessions.read().await.clone(),
                self.configuration_generation.load(Ordering::Acquire),
            )
        };
        self.primary
            .wait_for_catch_up_quorum(ReplicaSetQuorumMode::WriteQuorum)
            .await?;
        let boundary = self.control.current_progress().await?;
        let _gate = self.gate.lock().await;
        self.active_host()?;
        if authority_before != self.state.read().await.authority
            || sessions_before != *self.sessions.read().await
            || configuration_generation != self.configuration_generation.load(Ordering::Acquire)
        {
            return Err(RuntimeError::OperationCancelled);
        }
        let mut state = self.state.write().await;
        state.catch_up_boundary = Some(boundary);
        state.catch_up_complete = true;
        Ok(())
    }

    async fn authorize_common_failover_prefix(&self, boundary: i64) -> Result<()> {
        let (authority_before, sessions_before, configuration_generation) = {
            let _gate = self.gate.lock().await;
            (
                self.state.read().await.authority.clone(),
                self.sessions.read().await.clone(),
                self.configuration_generation.load(Ordering::Acquire),
            )
        };
        let progress = self.control.current_progress().await?;
        let _gate = self.gate.lock().await;
        self.active_host()?;
        if authority_before != self.state.read().await.authority
            || sessions_before != *self.sessions.read().await
            || configuration_generation != self.configuration_generation.load(Ordering::Acquire)
        {
            return Err(RuntimeError::OperationCancelled);
        }
        if progress < boundary {
            return Err(RuntimeError::ReconfigurationPending);
        }
        self.state.write().await.verified_replication_lsn = Some(boundary);
        Ok(())
    }

    async fn register_common_peer(
        &self,
        identity: ReplicaIdentity,
        session: ProcessSessionId,
    ) -> Result<()> {
        let replaced = {
            let _gate = self.gate.lock().await;
            if session.is_empty()
                || self
                    .retired_sessions
                    .read()
                    .await
                    .contains(&(identity.clone(), session.clone()))
            {
                return Err(RuntimeError::AuthorityNotAdmitted);
            }
            let old = self
                .sessions
                .write()
                .await
                .insert(identity.clone(), session.clone());
            if let Some(old) = old.filter(|old| old != &session) {
                self.retired_sessions
                    .write()
                    .await
                    .insert((identity.clone(), old));
                self.state.write().await.builds.retain(|build| {
                    build.authority.source != identity && build.authority.target != identity
                });
                true
            } else {
                false
            }
        };
        if replaced {
            self.invalidate_build_attempts().await?;
            self.fence_writes().await?;
        }
        self.configure().await
    }

    async fn apply_common_action(&self, action: RuntimeEffectAction) -> Result<()> {
        let action = match action {
            RuntimeEffectAction::AdmitBuildAuthority(authority) => {
                return self.admit_common_build(*authority).await;
            }
            RuntimeEffectAction::SetAccessStatus { read, write } => {
                return self.set_access(read, write).await;
            }
            RuntimeEffectAction::SetReadStatus(read) => {
                let write = self.state.read().await.write_status;
                return self.set_access(read, write).await;
            }
            RuntimeEffectAction::SetWriteStatus(write) => {
                let read = self.state.read().await.read_status;
                return self.set_access(read, write).await;
            }
            RuntimeEffectAction::WaitForCatchup => {
                return self.wait_for_common_catchup().await;
            }
            RuntimeEffectAction::AuthorizeFailoverPrefix(boundary) => {
                return self.authorize_common_failover_prefix(boundary).await;
            }
            RuntimeEffectAction::RegisterPeerSession { identity, session } => {
                return self.register_common_peer(identity, session).await;
            }
            action => action,
        };
        let _gate = self.gate.lock().await;
        let host = self.host()?;
        match action {
            RuntimeEffectAction::AdmitAuthority(authority) => {
                authority.validate()?;
                if self.native_receipts
                    && let Some(evidence) = &authority.scale_up
                {
                    let intent = evidence.intent();
                    if intent.target == host.identity
                        && self
                            .state
                            .read()
                            .await
                            .authority
                            .as_ref()
                            .is_none_or(|old| {
                                old.current_configuration != authority.current_configuration
                            })
                    {
                        let build = host
                            .default_dependencies
                            .build_authority_store
                            .load_build(&intent.build_id)
                            .await?
                            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
                        let current_receipt = self.receipt(&build).await?;
                        let retained_receipt =
                            self.receipts.read().await.get(&intent.build_id).cloned();
                        if build.source != intent.primary
                            || build.target != intent.target
                            || build.current_configuration != intent.previous_configuration
                            || build.replication_boundary_lsn != intent.snapshot_boundary_lsn
                            || retained_receipt.as_ref().is_none_or(|retained| {
                                !retained.matches_durable_selection(&current_receipt)
                            })
                            || !self.state.read().await.builds.iter().any(|progress| {
                                progress.authority == build
                                    && progress.completed
                                    && progress.catch_up_boundary_lsn
                                        == Some(intent.catch_up_boundary_lsn)
                                    && progress.durable_lsn >= intent.catch_up_boundary_lsn
                            })
                        {
                            return Err(RuntimeError::AuthorityNotAdmitted);
                        }
                    }
                }
                self.prepare_authority_admission(&authority).await?;
                host.default_dependencies
                    .replica_authority_store
                    .admit(&authority)
                    .await?;
                self.state.write().await.authority = Some(*authority);
                self.configure().await?;
            }
            RuntimeEffectAction::AdmitBuildAuthority(_) => unreachable!(),
            RuntimeEffectAction::RegisterPeerSession { .. } => unreachable!(),
            RuntimeEffectAction::RetireBuild(id) => {
                self.retired_builds.write().await.insert(id.clone());
                if let Some(build) = host
                    .default_dependencies
                    .build_authority_store
                    .load_build(&id)
                    .await?
                {
                    self.primary.remove_replica(build.target.replica_id).await?;
                }
                self.state
                    .write()
                    .await
                    .builds
                    .retain(|b| b.authority.build_id != id);
                self.configure().await?;
                self.refresh().await?;
            }
            RuntimeEffectAction::SetAccessStatus { .. }
            | RuntimeEffectAction::SetReadStatus(_)
            | RuntimeEffectAction::SetWriteStatus(_)
            | RuntimeEffectAction::WaitForCatchup
            | RuntimeEffectAction::AuthorizeFailoverPrefix(_) => unreachable!(),
            RuntimeEffectAction::PrepareSwitchover {
                source,
                target,
                starting_configuration_id,
                starting_epoch,
                ..
            } => {
                let authority = self
                    .state
                    .read()
                    .await
                    .authority
                    .clone()
                    .ok_or(RuntimeError::AuthorityNotAdmitted)?;
                if source != host.identity
                    || authority.current_configuration.configuration_id != starting_configuration_id
                    || authority.current_configuration.epoch != starting_epoch
                    || !authority
                        .current_configuration
                        .members
                        .iter()
                        .any(|m| m.identity == target)
                {
                    return Err(RuntimeError::AuthorityNotAdmitted);
                }
                self.primary
                    .wait_for_catch_up_quorum(ReplicaSetQuorumMode::All)
                    .await?;
                self.set_access(
                    AccessStatus::ReconfigurationPending,
                    AccessStatus::ReconfigurationPending,
                )
                .await?;
                self.primary
                    .wait_for_catch_up_quorum(ReplicaSetQuorumMode::All)
                    .await?;
                self.refresh().await?;
            }
            RuntimeEffectAction::RefreshApplicationProgress => self.refresh().await?,
            RuntimeEffectAction::PrepareSecondaryRemoval {
                intent,
                process_session_id,
                report_sequence,
            } => {
                self.prepare_removal(*intent, process_session_id, report_sequence)
                    .await?;
            }
            RuntimeEffectAction::ObserveSecondaryRemovalWitness(witness) => {
                self.observe_removal(*witness).await?
            }
            RuntimeEffectAction::ObserveSecondaryRemovalProgress { witness, committed } => {
                self.observe_removal_progress(*witness, *committed).await?;
            }
            RuntimeEffectAction::AcceptSecondaryRemovalCommit(committed) => {
                self.accept_removal(*committed, false).await?
            }
            RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(command) => {
                kuberic_protocol::validation::validate_accept_secondary_removal_commit(&command)?;
                if !command.local_recovery || command.target != host.identity {
                    return Err(RuntimeError::AuthorityNotAdmitted);
                }
                self.accept_removal(command.committed, true).await?;
            }
            RuntimeEffectAction::FenceRetirement(retired) => {
                retired.validate(&host.identity)?;
                self.invalidate_configuration_attempts().await?;
                host.default_dependencies
                    .replica_authority_store
                    .record_retirement_started(&retired)
                    .await?;
                self.set_access(AccessStatus::NotPrimary, AccessStatus::NotPrimary)
                    .await?;
            }
            RuntimeEffectAction::CompleteRetirement(retired) => {
                retired.validate(&host.identity)?;
                let closed = host.state.read().await.fallback_snapshot.clone();
                if closed.open || closed.role != kuberic_protocol::types::ReplicaRole::None {
                    return Err(RuntimeError::ReconfigurationPending);
                }
                host.default_dependencies
                    .replica_authority_store
                    .retire(&retired)
                    .await?;
                let mut state = self.state.write().await;
                state.retired_authority = Some(*retired);
                state.open = false;
                state.authority = None;
                state.builds.clear();
                state.read_status = AccessStatus::NotPrimary;
                state.write_status = AccessStatus::NotPrimary;
                drop(state);
                host.state.write().await.fallback_snapshot.authority = None;
            }
            _ => return unavailable(),
        }
        Ok(())
    }
    pub(super) async fn snapshot(&self) -> RuntimeSnapshot {
        let mut snapshot = self.state.read().await.clone();
        if !self.native_receipts {
            return snapshot;
        }
        let receipts = self.receipts.read().await.clone();
        let mut current = Vec::new();
        for build in snapshot.builds {
            if let Some(receipt) = receipts.get(&build.authority.build_id)
                && let Ok(current_receipt) = self.receipt(&build.authority).await
                && receipt.matches_durable_selection(&current_receipt)
                && !self
                    .retired_builds
                    .read()
                    .await
                    .contains(&build.authority.build_id)
            {
                current.push(build);
            }
        }
        snapshot.builds = current;
        snapshot
    }
    pub(super) async fn cancel_outbound_build(&self, id: &OperationId) -> Result<()> {
        let _gate = self.gate.lock().await;
        let mut generations = self.build_generations.write().await;
        let generation = generations.entry(id.clone()).or_default();
        *generation = generation
            .checked_add(1)
            .ok_or(RuntimeError::OperationCancelled)?;
        drop(generations);
        self.state
            .write()
            .await
            .builds
            .retain(|b| &b.authority.build_id != id);
        self.changed.notify_waiters();
        if let Some(build) = self
            .host()?
            .default_dependencies
            .build_authority_store
            .load_build(id)
            .await?
        {
            self.primary.remove_replica(build.target.replica_id).await?;
        }
        self.configure().await
    }
    pub(super) async fn next_outbound(&self) -> Option<OutboundOperation> {
        loop {
            if self
                .abort_notified
                .load(std::sync::atomic::Ordering::Acquire)
                || self.host.upgrade().is_none_or(|host| {
                    host.aborted.load(std::sync::atomic::Ordering::Acquire)
                        || host.closed.load(std::sync::atomic::Ordering::Acquire)
                })
            {
                return None;
            }
            let changed = self.changed.notified();
            let mut receiver = self.receiver.lock().await;
            tokio::select! {
                item = receiver.recv() => return item,
                _ = changed => {}
                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
            }
        }
    }

    fn notify_abort(&self) {
        self.abort_notified
            .store(true, std::sync::atomic::Ordering::Release);
        let _ = self.configuration_generation.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |generation| generation.checked_add(1),
        );
        let _ = self.access_generation.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |generation| generation.checked_add(1),
        );
        if let Some(handle) = self.deferred_configuration_abort.lock().unwrap().take() {
            handle.abort();
        }
        self.changed.notify_waiters();
    }
}

#[async_trait]
impl ReplicatorLifecycleBackend for CustomReplicatorHost {
    fn owns_stream_session(&self) -> bool {
        false
    }

    async fn complete_open(&self, address: String) -> Result<()> {
        CustomReplicatorHost::complete_open(self, address).await
    }

    async fn complete_close(&self) -> Result<()> {
        self.terminate().await
    }

    async fn complete_abort(&self) {
        self.notify_abort();
        self.terminate().await.ok();
    }

    fn notify_abort(&self) {
        CustomReplicatorHost::notify_abort(self);
    }

    async fn fence_writes(&self) -> Result<()> {
        CustomReplicatorHost::fence_writes(self).await
    }

    async fn settle_primary_prefix(&self) -> Result<()> {
        CustomReplicatorHost::settle_primary_prefix(self).await
    }

    async fn cancel_configuration_work(&self) -> Result<()> {
        CustomReplicatorHost::cancel_configuration_work(self).await
    }

    async fn restore_authority(&self) -> Result<()> {
        CustomReplicatorHost::restore_authority(self).await
    }

    async fn restore_access(&self, read: AccessStatus, write: AccessStatus) -> Result<()> {
        CustomReplicatorHost::restore_access(self, read, write).await
    }

    async fn admit_authority(&self, authority: AdmittedAuthority) -> Result<()> {
        CustomReplicatorHost::apply_common_action(
            self,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority)),
        )
        .await
    }

    async fn admit_build_authority(&self, authority: BuildAuthority) -> Result<()> {
        CustomReplicatorHost::apply_common_action(
            self,
            RuntimeEffectAction::AdmitBuildAuthority(Box::new(authority)),
        )
        .await
    }

    async fn register_peer_session(
        &self,
        identity: ReplicaIdentity,
        session: ProcessSessionId,
    ) -> Result<()> {
        CustomReplicatorHost::apply_common_action(
            self,
            RuntimeEffectAction::RegisterPeerSession { identity, session },
        )
        .await
    }

    async fn retire_build(&self, build_id: OperationId) -> Result<()> {
        CustomReplicatorHost::apply_common_action(self, RuntimeEffectAction::RetireBuild(build_id))
            .await
    }

    async fn set_access(&self, read: AccessStatus, write: AccessStatus) -> Result<()> {
        CustomReplicatorHost::apply_common_action(
            self,
            RuntimeEffectAction::SetAccessStatus { read, write },
        )
        .await
    }

    async fn wait_for_catch_up(&self) -> Result<()> {
        CustomReplicatorHost::apply_common_action(self, RuntimeEffectAction::WaitForCatchup).await
    }

    async fn authorize_failover_prefix(&self, boundary: i64) -> Result<()> {
        CustomReplicatorHost::apply_common_action(
            self,
            RuntimeEffectAction::AuthorizeFailoverPrefix(boundary),
        )
        .await
    }

    async fn prepare_switchover(
        &self,
        preparation_generation: u64,
        request_id: kuberic_protocol::types::SwitchoverRequestId,
        source: ReplicaIdentity,
        target: ReplicaIdentity,
        starting_configuration_id: kuberic_protocol::types::ConfigurationId,
        starting_epoch: kuberic_protocol::types::Epoch,
    ) -> Result<()> {
        CustomReplicatorHost::apply_common_action(
            self,
            RuntimeEffectAction::PrepareSwitchover {
                preparation_generation,
                request_id,
                source,
                target,
                starting_configuration_id,
                starting_epoch,
            },
        )
        .await
    }

    async fn refresh_progress(&self) -> Result<()> {
        CustomReplicatorHost::apply_common_action(
            self,
            RuntimeEffectAction::RefreshApplicationProgress,
        )
        .await
    }

    async fn observe_progress(&self) -> Result<()> {
        if !CustomReplicatorHost::snapshot(self).await.open {
            return Ok(());
        }
        let result = CustomReplicatorHost::apply_common_action(
            self,
            RuntimeEffectAction::RefreshApplicationProgress,
        )
        .await;
        if matches!(result, Err(RuntimeError::NotOpen | RuntimeError::Closed)) {
            self.fence_managed_access().await?;
            self.mark_not_open().await;
            return Ok(());
        }
        result
    }

    async fn prepare_secondary_removal(
        &self,
        intent: kuberic_protocol::types::SecondaryScaleDownIntent,
        process_session_id: ProcessSessionId,
        report_sequence: u64,
    ) -> Result<()> {
        CustomReplicatorHost::apply_common_action(
            self,
            RuntimeEffectAction::PrepareSecondaryRemoval {
                intent: Box::new(intent),
                process_session_id,
                report_sequence,
            },
        )
        .await
    }

    async fn observe_secondary_removal(
        &self,
        witness: kuberic_protocol::types::SecondaryRemovalWitness,
    ) -> Result<()> {
        CustomReplicatorHost::apply_common_action(
            self,
            RuntimeEffectAction::ObserveSecondaryRemovalWitness(Box::new(witness)),
        )
        .await
    }

    async fn observe_secondary_removal_progress(
        &self,
        witness: kuberic_protocol::types::SecondaryRemovalWitness,
        committed: kuberic_protocol::types::SecondaryScaleDownCleanup,
    ) -> Result<()> {
        CustomReplicatorHost::apply_common_action(
            self,
            RuntimeEffectAction::ObserveSecondaryRemovalProgress {
                witness: Box::new(witness),
                committed: Box::new(committed),
            },
        )
        .await
    }

    async fn accept_secondary_removal(
        &self,
        committed: kuberic_protocol::types::SecondaryScaleDownCleanup,
    ) -> Result<()> {
        CustomReplicatorHost::apply_common_action(
            self,
            RuntimeEffectAction::AcceptSecondaryRemovalCommit(Box::new(committed)),
        )
        .await
    }

    async fn accept_historical_secondary_removal(
        &self,
        command: kuberic_protocol::command::AcceptSecondaryRemovalCommit,
    ) -> Result<()> {
        CustomReplicatorHost::apply_common_action(
            self,
            RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(Box::new(command)),
        )
        .await
    }

    async fn fence_retirement(
        &self,
        retired: kuberic_runtime_internal::authority::RetiredAuthority,
    ) -> Result<()> {
        CustomReplicatorHost::apply_common_action(
            self,
            RuntimeEffectAction::FenceRetirement(Box::new(retired)),
        )
        .await
    }

    async fn complete_retirement(
        &self,
        retired: kuberic_runtime_internal::authority::RetiredAuthority,
    ) -> Result<()> {
        CustomReplicatorHost::apply_common_action(
            self,
            RuntimeEffectAction::CompleteRetirement(Box::new(retired)),
        )
        .await
    }

    async fn snapshot(&self) -> RuntimeSnapshot {
        CustomReplicatorHost::snapshot(self).await
    }

    async fn cancel_outbound_build(&self, id: &OperationId) -> Result<()> {
        CustomReplicatorHost::cancel_outbound_build(self, id).await
    }

    async fn next_outbound(&self) -> Option<OutboundOperation> {
        CustomReplicatorHost::next_outbound(self).await
    }

    async fn wait_for_build_completion(
        &self,
        build_id: &OperationId,
        target: &ReplicaIdentity,
    ) -> Result<()> {
        let generation = self.build_generation(build_id).await;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(75);
        loop {
            let changed = self.changed.notified();
            let snapshot = CustomReplicatorHost::snapshot(self).await;
            if snapshot.builds.iter().any(|build| {
                &build.authority.build_id == build_id
                    && &build.authority.target == target
                    && build.completed
            }) {
                return Ok(());
            }

            self.ensure_build_generation(build_id, generation).await?;
            if tokio::time::Instant::now() >= deadline {
                return Err(RuntimeError::OperationCancelled);
            }
            tokio::select! {
                _ = changed => {}
                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
            }
        }
    }

    async fn build_replica(&self, replica: ReplicaInformation) -> Result<()> {
        CustomReplicatorHost::execute_build(self, replica).await
    }

    async fn remove_replica(&self, replica_id: kuberic_protocol::types::ReplicaId) -> Result<()> {
        let authority_before = self.state.read().await.authority.clone();
        let sessions_before = self.sessions.read().await.clone();
        let configuration_generation = self.configuration_generation.load(Ordering::Acquire);
        self.primary.remove_replica(replica_id).await?;
        self.active_host()?;
        if authority_before != self.state.read().await.authority
            || sessions_before != *self.sessions.read().await
            || configuration_generation != self.configuration_generation.load(Ordering::Acquire)
        {
            return Err(RuntimeError::OperationCancelled);
        }
        self.remove_managed_replica(replica_id).await
    }

    async fn select_build(&self, authority: &BuildAuthority) -> Result<()> {
        CustomReplicatorHost::select_build(self, authority).await
    }

    async fn describe_peer(&self, replica: ReplicaInformation) -> Result<()> {
        CustomReplicatorHost::describe_peer(self, replica).await
    }

    async fn execute_build(&self, replica: ReplicaInformation) -> Result<BuildExecution> {
        CustomReplicatorHost::execute_build(self, replica).await?;
        Ok(BuildExecution::ApplicationCompleted)
    }

    async fn enqueue_build(&self, endpoint: ReplicaEndpoint) -> Result<()> {
        CustomReplicatorHost::enqueue_build(self, endpoint).await
    }
}

fn preserves_same_primary_scale_up_access(
    existing: &AdmittedAuthority,
    next: &AdmittedAuthority,
) -> bool {
    matches!(
        next.scale_up.as_deref(),
        Some(kuberic_protocol::types::ScaleUpConfigurationEvidence::Admission { .. })
    ) && existing.primary_identity() == next.primary_identity()
        && next.local_identity == *next.primary_identity()
        && existing.local_identity == next.local_identity
}

fn merge_builds(current: &mut Vec<BuildPostcondition>, incoming: Vec<BuildPostcondition>) {
    for build in incoming {
        if let Some(existing) = current
            .iter_mut()
            .find(|existing| existing.authority.build_id == build.authority.build_id)
        {
            *existing = build;
        } else {
            current.push(build);
        }
    }
}

fn unavailable<T>() -> Result<T> {
    Err(RuntimeError::Application(
        "the selected replicator does not support this managed operation".into(),
    ))
}
