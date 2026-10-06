//! Private SF lifecycle/effect hosting, independent of operation/copy capability.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};

use crate::authority::{AdmittedAuthority, BuildAuthority, BuildSelection, DurableBuildProgress};
use crate::effects::{
    BuildPostcondition, RuntimeEffectAction, RuntimePostcondition, RuntimeSnapshot,
};
use crate::protocol::types::{
    AccessStatus, ConfigurationDescriptor, OperationId, ProcessSessionId, ReplicaIdentity,
};
use crate::receipts::{
    AccessPreparation, CertifiedPrefixReceipt, NativeOperationToken, NativeProgressStatus,
    NativeTopologyStatus, RetirementReceipt, SecondaryRemovalReceipt, SwitchoverReceipt,
    TopologyReceipt,
};
use crate::replicator::{
    ManagedFenceGuard, ManagedReplicatorLifecycle, PrimaryReplicator, ReplicaInformation,
    ReplicaSetConfiguration, ReplicaSetQuorumMode, Replicator,
};
use crate::transport::{OutboundOperation, ReplicaEndpoint};
use crate::{Result, RuntimeError};
use async_trait::async_trait;
use tokio::sync::{Mutex, Notify, RwLock, mpsc, oneshot};

use super::lifecycle::{
    AccessLifecycle, AuthorityLifecycle, BuildCancellation, BuildLifecycle, LifecycleObservation,
    LifecycleWiring, OutboundLifecycle, ProcessLifecycle, TopologyLifecycle,
};
use super::{AppliedEffect, RuntimeHost, empty_snapshot};
#[path = "custom_removal.rs"]
mod removal;

#[cfg(feature = "testing")]
const ACCESS_CLOSE_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(100);
#[cfg(not(feature = "testing"))]
const ACCESS_CLOSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Clone, PartialEq, Eq)]
pub(super) struct BuildAdmission {
    selection: BuildSelection,
    source_session: ProcessSessionId,
    target_session: ProcessSessionId,
    attempt_generation: u64,
    configuration_generation: u64,
    native_token: Option<NativeOperationToken>,
}

impl BuildAdmission {
    fn matches_durable_selection(&self, other: &Self) -> bool {
        self.selection == other.selection
            && self.source_session == other.source_session
            && self.target_session == other.target_session
            && self.attempt_generation == other.attempt_generation
    }

    fn matches_host_admission(&self, other: &Self) -> bool {
        self.matches_durable_selection(other)
            && self.configuration_generation == other.configuration_generation
    }
}

#[derive(Clone, PartialEq, Eq)]
struct AcceptedBuild {
    admission: BuildAdmission,
}

pub(super) struct ReadyAccessTransaction {
    pub(super) accept: oneshot::Sender<()>,
    pub(super) accepted: oneshot::Receiver<Option<NativeProgressStatus>>,
    pub(super) decision: oneshot::Sender<AccessDecision>,
    pub(super) completion: tokio::task::JoinHandle<Result<()>>,
}

pub(crate) struct AcceptedAccessTransaction {
    decision: oneshot::Sender<AccessDecision>,
    completion: tokio::task::JoinHandle<Result<()>>,
}

pub(crate) struct AcceptedAccessEffect {
    transaction: AcceptedAccessTransaction,
    host: Weak<RuntimeHost>,
    applied: AppliedEffect,
    #[cfg(test)]
    observer_gate: Option<crate::host::runtime_adapter::RuntimeEffectObserverGate>,
}

pub(super) enum AccessCommit {
    Direct,
    Durable {
        host: Weak<RuntimeHost>,
        applied: Box<AppliedEffect>,
    },
}

pub(super) enum AccessDecision {
    Commit(AccessCommit),
    Reject,
}

struct BuildQueueAdmission {
    generation: u64,
    decision: Option<oneshot::Sender<bool>>,
    completion: tokio::task::JoinHandle<()>,
}

pub(super) struct BuildCompletionConfirmation {
    pub(super) postcondition: RuntimePostcondition,
    _native: Option<ManagedFenceGuard>,
}

impl BuildQueueAdmission {
    fn new(build: Weak<dyn BuildCancellation>, build_id: OperationId, generation: u64) -> Self {
        let (decision, completion) = oneshot::channel();
        let completion = tokio::spawn(async move {
            if completion.await != Ok(true)
                && let Some(build) = build.upgrade()
            {
                let _ = build
                    .cancel_outbound_build_attempt(&build_id, generation, false)
                    .await;
            }
        });
        Self {
            generation,
            decision: Some(decision),
            completion,
        }
    }

    fn generation(&self) -> u64 {
        self.generation
    }

    async fn finish(mut self, admitted: bool) {
        if let Some(decision) = self.decision.take() {
            let _ = decision.send(admitted);
        }
        let _ = self.completion.await;
    }
}

impl AccessCommit {
    async fn record(self) {
        if let Self::Durable { host, applied } = self
            && let Some(host) = host.upgrade()
        {
            host.state
                .write()
                .await
                .effects
                .insert(applied.result.sequence, *applied);
        }
    }
}

impl ReadyAccessTransaction {
    pub(super) async fn accept(
        self,
    ) -> Result<(Option<NativeProgressStatus>, AcceptedAccessTransaction)> {
        let Self {
            accept,
            accepted,
            decision,
            completion,
        } = self;
        let _ = accept.send(());
        let progress = accepted
            .await
            .map_err(|_| RuntimeError::OperationCancelled)?;
        Ok((
            progress,
            AcceptedAccessTransaction {
                decision,
                completion,
            },
        ))
    }
}

impl AcceptedAccessTransaction {
    async fn finish(self, decision: AccessDecision) -> Result<()> {
        let _ = self.decision.send(decision);
        self.completion
            .await
            .map_err(|error| RuntimeError::Application(error.to_string()))?
    }

    pub(super) async fn commit(self) -> Result<()> {
        self.finish(AccessDecision::Commit(AccessCommit::Direct))
            .await
    }

    pub(super) fn into_effect(
        self,
        host: Weak<RuntimeHost>,
        applied: AppliedEffect,
    ) -> AcceptedAccessEffect {
        AcceptedAccessEffect {
            transaction: self,
            host,
            applied,
            #[cfg(test)]
            observer_gate: None,
        }
    }
}

impl AcceptedAccessEffect {
    pub(crate) async fn accept(self) -> Result<()> {
        let Self {
            transaction,
            host,
            applied,
            #[cfg(test)]
            observer_gate,
        } = self;
        let decision = AccessDecision::Commit(AccessCommit::Durable {
            host,
            applied: Box::new(applied),
        });
        let _ = transaction.decision.send(decision);
        #[cfg(test)]
        if let Some(gate) = observer_gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        transaction
            .completion
            .await
            .map_err(|error| RuntimeError::Application(error.to_string()))?
    }

    pub(crate) async fn reject(self) -> Result<()> {
        match self.transaction.finish(AccessDecision::Reject).await {
            Ok(()) | Err(RuntimeError::OperationCancelled) => Ok(()),
            Err(error) => Err(error),
        }
    }

    #[cfg(test)]
    pub(crate) fn testing_pause_after_decision(
        &mut self,
    ) -> crate::host::runtime_adapter::RuntimeEffectObserverGate {
        let gate = crate::host::runtime_adapter::RuntimeEffectObserverGate {
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
        };
        self.observer_gate = Some(gate.clone());
        gate
    }
}

#[derive(Clone)]
struct AccessProjection {
    previous_read: AccessStatus,
    previous_write: AccessStatus,
    read: AccessStatus,
    write: AccessStatus,
    authority: Option<AdmittedAuthority>,
    configuration: Option<ReplicaSetConfiguration>,
    sessions: BTreeMap<ReplicaIdentity, ProcessSessionId>,
    role: crate::protocol::types::ReplicaRole,
    configuration_generation: u64,
    access_generation: u64,
    faulted_grant: bool,
}

fn validate_access_preparation(
    preparation: &AccessPreparation,
    projection: &AccessProjection,
) -> Result<()> {
    if preparation.authority != projection.authority
        || preparation.read != projection.read
        || preparation.write != projection.write
        || preparation.engine_session_id.is_empty()
    {
        return Err(RuntimeError::OperationCancelled);
    }
    Ok(())
}

fn validate_native_token(
    receipt: &NativeOperationToken,
    expected: &NativeOperationToken,
) -> Result<()> {
    if receipt != expected || receipt.engine_session_id.is_empty() {
        return Err(RuntimeError::OperationCancelled);
    }
    Ok(())
}

fn validate_certified_prefix_receipt(
    receipt: &CertifiedPrefixReceipt,
    token: &NativeOperationToken,
) -> Result<()> {
    validate_native_token(&receipt.token, token)?;
    if receipt.settled_lsn < 0 || receipt.settled_lsn > receipt.verified_lsn {
        return Err(RuntimeError::OperationCancelled);
    }
    Ok(())
}

fn validate_secondary_removal_receipt(
    receipt: &SecondaryRemovalReceipt,
    token: &NativeOperationToken,
) -> Result<()> {
    validate_native_token(&receipt.token, token)
}

fn validate_retirement_receipt(
    receipt: &RetirementReceipt,
    retired: &crate::authority::RetiredAuthority,
    completed: bool,
) -> Result<()> {
    if &receipt.retired != retired
        || receipt.completed != completed
        || receipt.engine_session_id.is_empty()
    {
        return Err(RuntimeError::OperationCancelled);
    }
    Ok(())
}

fn apply_progress_status(
    postcondition: &mut RuntimePostcondition,
    progress: &NativeProgressStatus,
) {
    postcondition.current_progress = progress.current_progress;
    postcondition.verified_replication_lsn = progress.verified_replication_lsn;
    postcondition.committed_lsn = progress.committed_lsn;
    postcondition.current_configuration_quorum_progress =
        progress.current_configuration_quorum_progress;
    postcondition.catch_up_boundary = progress.catch_up_boundary;
    postcondition.catch_up_complete = progress.catch_up_complete;
}

fn cleanup_releases_claim(result: &Result<()>) -> bool {
    result.is_ok()
        || matches!(
            result,
            Err(RuntimeError::OperationCancelled
                | RuntimeError::ReconfigurationPending
                | RuntimeError::Closed
                | RuntimeError::ReplicaRemoved(_))
        )
}

type DeferredConfiguration = tokio::task::JoinHandle<Result<(u64, ReplicaSetConfiguration)>>;

struct ManagedLifecycleBackend {
    legacy: Arc<dyn ManagedReplicatorLifecycle>,
    common: Arc<CustomReplicatorHost>,
    accepted_builds: RwLock<BTreeMap<OperationId, AcceptedBuild>>,
    topology_receipt: RwLock<Option<TopologyReceipt>>,
}

#[async_trait]
impl ProcessLifecycle for ManagedLifecycleBackend {
    fn owns_stream_session(&self) -> bool {
        true
    }

    async fn complete_open(&self, address: String) -> Result<()> {
        self.common.complete_open_common(address.clone()).await;
        self.legacy.complete_open(address).await
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
        self.legacy.fence_writes().await
    }

    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    async fn invalidate_public_access(&self) -> Result<()> {
        self.common.fence_managed_access().await
    }

    async fn settle_primary_prefix(&self) -> Result<()> {
        let token = self.legacy.native_fence().await?;
        let receipt = self.legacy.settle_primary_prefix().await?;
        validate_certified_prefix_receipt(&receipt, &token)?;
        if receipt.committed_lsn != receipt.settled_lsn {
            return Err(RuntimeError::OperationCancelled);
        }
        self.common.settle_primary_prefix().await?;
        self.common.accept_certified_prefix(&receipt).await?;
        *self.topology_receipt.write().await =
            Some(TopologyReceipt::CertifiedPrefix(Box::new(receipt)));
        Ok(())
    }
}

#[async_trait]
impl AuthorityLifecycle for ManagedLifecycleBackend {
    async fn cancel_configuration_work(&self) -> Result<()> {
        self.common.cancel_configuration_work().await?;
        self.legacy.cancel_configuration_work().await?;
        self.legacy.fence_writes().await
    }

    async fn restore_authority(&self) -> Result<()> {
        self.legacy.restore_engine_proof().await?;
        self.common.restore_authority().await?;
        self.sync_topology_status().await
    }

    async fn admit_authority(&self, authority: AdmittedAuthority) -> Result<()> {
        self.common.prepare_authority_admission(&authority).await?;
        self.legacy.admit_authority_proof(authority.clone()).await?;
        self.common.install_managed_authority(&authority).await?;
        self.sync_topology_status().await
    }

    async fn register_peer_session(
        &self,
        identity: ReplicaIdentity,
        session: ProcessSessionId,
    ) -> Result<()> {
        {
            let _registration = self.common.session_registration.lock().await;
            let registered_engine_peer = {
                let _gate = self.common.gate.lock().await;
                self.common
                    .validate_peer_session_replacement(&identity, &session)
                    .await?;
                self.common
                    .state
                    .read()
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
                    })
            };
            let _access = self.common.access_commit.lock().await;
            if registered_engine_peer {
                self.legacy
                    .register_peer_session_proof(identity.clone(), session.clone())
                    .await?;
            }
            self.common
                .validate_peer_session_replacement(&identity, &session)
                .await?;
            self.common
                .register_common_peer_locked(identity, session)
                .await?;
        }
        Ok(())
    }

    async fn describe_peer(&self, replica: ReplicaInformation) -> Result<()> {
        self.common.describe_peer(replica).await
    }
}

#[async_trait]
impl AccessLifecycle for ManagedLifecycleBackend {
    async fn defer_restored_access(&self, read: AccessStatus, write: AccessStatus) {
        *self.common.restored_access.write().await = Some((read, write));
    }

    async fn run_access_transaction(
        &self,
        read: AccessStatus,
        write: AccessStatus,
        ready: oneshot::Sender<()>,
        accept: oneshot::Receiver<()>,
        accepted: oneshot::Sender<Option<NativeProgressStatus>>,
        decision: oneshot::Receiver<AccessDecision>,
    ) -> Result<()> {
        self.execute_access_transaction(read, write, ready, accept, accepted, decision)
            .await
    }

    async fn restored_access(&self) -> Option<(AccessStatus, AccessStatus)> {
        *self.common.restored_access.read().await
    }
}

#[async_trait]
impl BuildLifecycle for ManagedLifecycleBackend {
    async fn admit_build_authority(&self, authority: BuildAuthority) -> Result<()> {
        self.legacy
            .admit_build_authority_proof(authority.clone())
            .await?;
        self.common.install_managed_build(&authority).await?;
        Ok(())
    }

    async fn retire_build(&self, build_id: OperationId) -> Result<()> {
        self.accepted_builds.write().await.remove(&build_id);
        self.legacy.retire_build_proof(build_id.clone()).await?;
        self.common.retire_managed_build(build_id).await?;
        Ok(())
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
            self.common
                .ensure_build_generation(build_id, generation)
                .await?;
            let snapshot = self.snapshot().await;
            if snapshot.builds.iter().any(|build| {
                &build.authority.build_id == build_id
                    && &build.authority.target == target
                    && build.completed
            }) {
                self.common
                    .ensure_build_generation(build_id, generation)
                    .await?;
                return Ok(());
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(RuntimeError::OperationCancelled);
            }
            tokio::select! {
                _ = changed => {}
                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
            }
        }
    }

    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    async fn build_replica(&self, replica: ReplicaInformation) -> Result<()> {
        let build_id = replica.build_id.clone();
        let target = replica.identity.clone();
        if self.snapshot().await.builds.iter().any(|build| {
            build.authority.build_id == build_id
                && build.authority.target == target
                && build.completed
        }) {
            return Ok(());
        }
        let endpoint = ReplicaEndpoint {
            build_id: build_id.clone(),
            identity: replica.identity.clone(),
            replication_address: replica.replication_address.clone(),
        };
        self.common.enqueue_build_wait(endpoint).await?;
        self.wait_for_build_completion(&build_id, &target).await
    }

    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    async fn remove_replica(&self, replica_id: crate::protocol::types::ReplicaId) -> Result<()> {
        let native_token = self.legacy.native_fence().await?;
        let authority_before = self.common.state.read().await.authority.clone();
        let sessions_before = self.common.sessions.read().await.clone();
        let configuration_generation = self.common.configuration_generation.load(Ordering::Acquire);
        self.common.primary.remove_replica(replica_id).await?;
        let _native = self.legacy.lock_native_fence(&native_token).await?;
        let _gate = self.common.gate.lock().await;
        self.common.active_host()?;
        if authority_before != self.common.state.read().await.authority
            || sessions_before != *self.common.sessions.read().await
            || configuration_generation
                != self.common.configuration_generation.load(Ordering::Acquire)
            || native_token.authority != authority_before
        {
            return Err(RuntimeError::OperationCancelled);
        }
        self.accepted_builds.write().await.retain(|_, receipt| {
            receipt.admission.selection.authority.target.replica_id != replica_id
        });
        self.common.remove_managed_replica(replica_id).await
    }

    async fn select_build(&self, authority: &BuildAuthority) -> Result<()> {
        let _gate = self.common.gate.lock().await;
        self.common.select_build_inner(authority).await
    }

    async fn execute_build(&self, replica: ReplicaInformation) -> Result<Option<BuildAdmission>> {
        let mut replica = replica;
        let mut receipt = self.common.prepare_build(&mut replica).await?;
        receipt.native_token = Some(self.legacy.native_fence().await?);
        self.common.primary.build_replica(replica).await?;
        Ok(Some(receipt))
    }

    async fn accept_build(&self, receipt: Option<BuildAdmission>) -> Result<()> {
        let receipt = receipt.ok_or_else(|| {
            RuntimeError::Application("managed build acceptance omitted its receipt".into())
        })?;
        let expected_native = receipt
            .native_token
            .as_ref()
            .ok_or(RuntimeError::OperationCancelled)?;
        self.legacy
            .detach_outbound_build_stream(&receipt.selection.authority.build_id)
            .await?;
        let _native = self.legacy.lock_native_fence(expected_native).await?;
        self.common.active_host()?;
        let _gate = self.common.gate.lock().await;
        if !self
            .common
            .receipt(&receipt.selection.authority)
            .await?
            .matches_host_admission(&receipt)
        {
            return Err(RuntimeError::OperationCancelled);
        }
        self.common.accept_managed_build(&receipt).await?;
        self.accepted_builds.write().await.insert(
            receipt.selection.authority.build_id.clone(),
            AcceptedBuild {
                admission: receipt.clone(),
            },
        );
        self.common
            .pending_builds
            .write()
            .await
            .remove(&receipt.selection.authority.build_id);
        self.common.changed.notify_waiters();
        Ok(())
    }

    async fn enqueue_build(&self, endpoint: ReplicaEndpoint) -> Result<()> {
        self.common.enqueue_build(endpoint).await
    }

    async fn confirm_build_completion(
        &self,
        build_id: &OperationId,
        target: &ReplicaIdentity,
    ) -> Result<BuildCompletionConfirmation> {
        let accepted = self
            .accepted_builds
            .read()
            .await
            .get(build_id)
            .cloned()
            .ok_or(RuntimeError::ReconfigurationPending)?;
        let expected_native = accepted
            .admission
            .native_token
            .as_ref()
            .ok_or(RuntimeError::ReconfigurationPending)?;
        let native_guard = self.legacy.lock_native_fence(expected_native).await?;
        if &accepted.admission.selection.authority.target != target
            || !self
                .common
                .receipt(&accepted.admission.selection.authority)
                .await?
                .matches_host_admission(&accepted.admission)
        {
            return Err(RuntimeError::ReconfigurationPending);
        }
        let mut postcondition = self.common.narrow_postcondition().await;
        apply_progress_status(&mut postcondition, native_guard.progress());
        Ok(BuildCompletionConfirmation {
            postcondition,
            _native: Some(native_guard),
        })
    }
}

#[async_trait]
impl BuildCancellation for ManagedLifecycleBackend {
    async fn cancel_outbound_build(&self, id: &OperationId, generation: u64) -> Result<()> {
        let _cleanup = self.common.build_cleanup_lock(id).await;
        if !self
            .common
            .cancel_common_build_attempt(id, generation)
            .await?
        {
            return Ok(());
        }
        self.accepted_builds.write().await.remove(id);
        let result = self.legacy.cancel_outbound_build(id).await;
        if cleanup_releases_claim(&result) {
            self.common
                .complete_common_build_cancellation(id, generation)
                .await;
        }
        result
    }

    async fn cancel_outbound_build_attempt(
        &self,
        id: &OperationId,
        generation: u64,
        _public_cleanup: bool,
    ) -> Result<()> {
        let _cleanup = self.common.build_cleanup_lock(id).await;
        if !self
            .common
            .cancel_common_build_attempt(id, generation)
            .await?
        {
            return Ok(());
        }
        self.accepted_builds.write().await.remove(id);
        let result = self.legacy.cancel_outbound_build(id).await;
        if cleanup_releases_claim(&result) {
            self.common
                .complete_common_build_cancellation(id, generation)
                .await;
        }
        result
    }

    async fn build_generation(&self, id: &OperationId) -> u64 {
        self.common.build_generation(id).await
    }
}

#[async_trait]
impl TopologyLifecycle for ManagedLifecycleBackend {
    async fn wait_for_catch_up(&self) -> Result<()> {
        let native_token = self.legacy.native_fence().await?;
        let (authority_before, sessions_before, configuration_generation) = {
            let _gate = self.common.gate.lock().await;
            (
                self.common.state.read().await.authority.clone(),
                self.common.sessions.read().await.clone(),
                self.common.configuration_generation.load(Ordering::Acquire),
            )
        };
        self.common
            .primary
            .wait_for_catch_up_quorum(ReplicaSetQuorumMode::WriteQuorum)
            .await?;
        let _native = self.legacy.lock_native_fence(&native_token).await?;
        self.common.active_host()?;
        let _gate = self.common.gate.lock().await;
        if authority_before != self.common.state.read().await.authority
            || sessions_before != *self.common.sessions.read().await
            || configuration_generation
                != self.common.configuration_generation.load(Ordering::Acquire)
            || native_token.authority != authority_before
        {
            return Err(RuntimeError::OperationCancelled);
        }
        let progress = self.common.primary.current_progress().await?;
        self.common.accept_catch_up_completion(progress).await?;
        Ok(())
    }

    async fn authorize_failover_prefix(&self, boundary: i64) -> Result<()> {
        let token = self.legacy.native_fence().await?;
        let TopologyReceipt::CertifiedPrefix(receipt) = self
            .legacy
            .apply_topology(RuntimeEffectAction::AuthorizeFailoverPrefix(boundary))
            .await?
        else {
            return Err(RuntimeError::OperationCancelled);
        };
        validate_certified_prefix_receipt(&receipt, &token)?;
        if receipt.settled_lsn != boundary {
            return Err(RuntimeError::OperationCancelled);
        }
        self.common.accept_certified_prefix(&receipt).await?;
        *self.topology_receipt.write().await = Some(TopologyReceipt::CertifiedPrefix(receipt));
        Ok(())
    }

    async fn prepare_switchover(
        &self,
        preparation_generation: u64,
        request_id: crate::protocol::types::SwitchoverRequestId,
        source: ReplicaIdentity,
        target: ReplicaIdentity,
        starting_configuration_id: crate::protocol::types::ConfigurationId,
        starting_epoch: crate::protocol::types::Epoch,
    ) -> Result<()> {
        let expected_authority = self.common.state.read().await.authority.clone();
        let expected_request = request_id.clone();
        let expected_source = source.clone();
        let expected_target = target.clone();
        let expected_configuration = starting_configuration_id.clone();
        self.common.fence_managed_access().await?;
        let TopologyReceipt::Switchover(receipt) = self
            .legacy
            .apply_topology(RuntimeEffectAction::PrepareSwitchover {
                preparation_generation,
                request_id,
                source,
                target,
                starting_configuration_id,
                starting_epoch,
            })
            .await?
        else {
            return Err(RuntimeError::OperationCancelled);
        };
        let current_token = self.legacy.native_fence().await?;
        validate_native_token(&receipt.token, &current_token)?;
        if receipt.token.authority != expected_authority {
            return Err(RuntimeError::OperationCancelled);
        }
        if receipt.preparation_generation != preparation_generation
            || receipt.request_id != expected_request
            || receipt.source != expected_source
            || receipt.target != expected_target
            || receipt.starting_configuration_id != expected_configuration
            || receipt.starting_epoch != starting_epoch
        {
            return Err(RuntimeError::OperationCancelled);
        }
        self.common.accept_switchover_receipt(&receipt).await?;
        *self.topology_receipt.write().await = Some(TopologyReceipt::Switchover(receipt));
        Ok(())
    }

    async fn prepare_secondary_removal(
        &self,
        intent: crate::protocol::types::SecondaryScaleDownIntent,
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
        witness: crate::protocol::types::SecondaryRemovalWitness,
    ) -> Result<()> {
        self.execute_removal_action(RuntimeEffectAction::ObserveSecondaryRemovalWitness(
            Box::new(witness),
        ))
        .await
    }

    async fn observe_secondary_removal_progress(
        &self,
        witness: crate::protocol::types::SecondaryRemovalWitness,
        committed: crate::protocol::types::SecondaryScaleDownCleanup,
    ) -> Result<()> {
        self.execute_removal_action(RuntimeEffectAction::ObserveSecondaryRemovalProgress {
            witness: Box::new(witness),
            committed: Box::new(committed),
        })
        .await
    }

    async fn accept_secondary_removal(
        &self,
        committed: crate::protocol::types::SecondaryScaleDownCleanup,
    ) -> Result<()> {
        self.execute_removal_action(RuntimeEffectAction::AcceptSecondaryRemovalCommit(Box::new(
            committed,
        )))
        .await
    }

    async fn accept_historical_secondary_removal(
        &self,
        command: crate::protocol::command::AcceptSecondaryRemovalCommit,
    ) -> Result<()> {
        self.execute_removal_action(RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(
            Box::new(command),
        ))
        .await
    }

    async fn fence_retirement(&self, retired: crate::authority::RetiredAuthority) -> Result<()> {
        self.execute_removal_action(RuntimeEffectAction::FenceRetirement(Box::new(retired)))
            .await
    }

    async fn complete_retirement(&self, retired: crate::authority::RetiredAuthority) -> Result<()> {
        self.execute_removal_action(RuntimeEffectAction::CompleteRetirement(Box::new(retired)))
            .await
    }

    async fn topology_receipt(&self, action: &RuntimeEffectAction) -> Option<TopologyReceipt> {
        let receipt = self.topology_receipt.read().await.clone()?;
        let matches = matches!(
            (action, &receipt),
            (
                RuntimeEffectAction::AuthorizeFailoverPrefix(_)
                    | RuntimeEffectAction::ChangeApplicationRole(
                        crate::protocol::types::ReplicaRole::Primary
                    ),
                TopologyReceipt::CertifiedPrefix(_)
            ) | (
                RuntimeEffectAction::PrepareSwitchover { .. },
                TopologyReceipt::Switchover(_)
            ) | (
                RuntimeEffectAction::PrepareSecondaryRemoval { .. }
                    | RuntimeEffectAction::ObserveSecondaryRemovalWitness(_)
                    | RuntimeEffectAction::ObserveSecondaryRemovalProgress { .. }
                    | RuntimeEffectAction::AcceptSecondaryRemovalCommit(_)
                    | RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(_),
                TopologyReceipt::SecondaryRemoval(_)
            ) | (
                RuntimeEffectAction::RetireReplica(_)
                    | RuntimeEffectAction::FenceRetirement(_)
                    | RuntimeEffectAction::CompleteRetirement(_),
                TopologyReceipt::Retirement(_)
            )
        );
        matches.then_some(receipt)
    }
}

#[async_trait]
impl LifecycleObservation for ManagedLifecycleBackend {
    async fn refresh_progress(&self) -> Result<()> {
        self.legacy.refresh_progress_proof().await?;
        self.common
            .apply_common_action(RuntimeEffectAction::RefreshApplicationProgress)
            .await
    }

    async fn observe_progress(&self) -> Result<()> {
        Ok(())
    }

    async fn snapshot(&self) -> RuntimeSnapshot {
        let mut snapshot = self.common.snapshot().await;
        let engine = self.engine_snapshot_for_host().await;
        snapshot.current_progress = engine.current_progress;
        snapshot.committed_lsn = engine.committed_lsn;
        snapshot.verified_replication_lsn = engine.verified_replication_lsn;
        snapshot.current_configuration_quorum_progress =
            engine.current_configuration_quorum_progress;
        snapshot.catch_up_boundary = if engine.catch_up_complete {
            snapshot.catch_up_boundary.or(engine.catch_up_boundary)
        } else {
            engine.catch_up_boundary
        };
        snapshot.catch_up_complete = engine.catch_up_complete;
        merge_builds(&mut snapshot.builds, engine.builds);
        snapshot.live_builds_only = false;
        snapshot.prepared_secondary_removal = engine.prepared_secondary_removal;
        snapshot.accepted_secondary_removal = engine.accepted_secondary_removal;
        snapshot.retired_authority = engine.retired_authority;
        snapshot
    }

    async fn postcondition(&self, progress: Option<&NativeProgressStatus>) -> RuntimePostcondition {
        let mut postcondition = self.common.narrow_postcondition().await;
        match progress {
            Some(progress) => apply_progress_status(&mut postcondition, progress),
            None => {
                let progress = self.legacy.progress_status().await;
                apply_progress_status(&mut postcondition, &progress);
            }
        }
        postcondition
    }
}

#[async_trait]
impl OutboundLifecycle for ManagedLifecycleBackend {
    async fn next_outbound(&self) -> Option<OutboundOperation> {
        self.common.next_outbound().await
    }
}

impl ManagedLifecycleBackend {
    async fn sync_topology_status(&self) -> Result<()> {
        self.common
            .install_topology_status(self.legacy.topology_status().await)
            .await
    }

    async fn engine_snapshot_for_host(&self) -> RuntimeSnapshot {
        let mut snapshot = self.legacy.snapshot().await;
        let accepted = self.accepted_builds.read().await.clone();
        for build in &mut snapshot.builds {
            if !build.completed
                || self
                    .common
                    .build_generation(&build.authority.build_id)
                    .await
                    == 0
            {
                continue;
            }
            build.completed = match accepted.get(&build.authority.build_id) {
                Some(receipt) if receipt.admission.selection.authority == build.authority => self
                    .common
                    .receipt(&build.authority)
                    .await
                    .is_ok_and(|current| current.matches_durable_selection(&receipt.admission)),
                _ => false,
            };
        }
        snapshot
    }

    async fn execute_removal_action(&self, action: RuntimeEffectAction) -> Result<()> {
        match action {
            RuntimeEffectAction::PrepareSecondaryRemoval {
                intent,
                process_session_id,
                report_sequence,
            } => {
                let token = self.legacy.native_fence().await?;
                let expected_intent = (*intent).clone();
                let expected_session = process_session_id.clone();
                self.common.fence_managed_access().await?;
                let TopologyReceipt::SecondaryRemoval(receipt) = self
                    .legacy
                    .apply_topology(RuntimeEffectAction::PrepareSecondaryRemoval {
                        intent,
                        process_session_id,
                        report_sequence,
                    })
                    .await?
                else {
                    return Err(RuntimeError::OperationCancelled);
                };
                validate_secondary_removal_receipt(&receipt, &token)?;
                if receipt.preparation.as_ref().is_none_or(|preparation| {
                    preparation.intent != expected_intent
                        || preparation.process_session_id != expected_session
                        || preparation.report_sequence != report_sequence
                }) {
                    return Err(RuntimeError::OperationCancelled);
                }
                self.common
                    .accept_secondary_removal_receipt(&receipt)
                    .await?;
                *self.topology_receipt.write().await =
                    Some(TopologyReceipt::SecondaryRemoval(receipt));
                Ok(())
            }
            RuntimeEffectAction::ObserveSecondaryRemovalWitness(witness) => {
                let token = self.legacy.native_fence().await?;
                let expected = (*witness).clone();
                let TopologyReceipt::SecondaryRemoval(receipt) = self
                    .legacy
                    .apply_topology(RuntimeEffectAction::ObserveSecondaryRemovalWitness(witness))
                    .await?
                else {
                    return Err(RuntimeError::OperationCancelled);
                };
                validate_secondary_removal_receipt(&receipt, &token)?;
                if receipt.witness.as_ref() != Some(&expected) {
                    return Err(RuntimeError::OperationCancelled);
                }
                self.common
                    .accept_secondary_removal_receipt(&receipt)
                    .await?;
                *self.topology_receipt.write().await =
                    Some(TopologyReceipt::SecondaryRemoval(receipt));
                Ok(())
            }
            RuntimeEffectAction::ObserveSecondaryRemovalProgress { witness, committed } => {
                let token = self.legacy.native_fence().await?;
                let expected_witness = (*witness).clone();
                let expected_committed = (*committed).clone();
                let TopologyReceipt::SecondaryRemoval(receipt) = self
                    .legacy
                    .apply_topology(RuntimeEffectAction::ObserveSecondaryRemovalProgress {
                        witness,
                        committed,
                    })
                    .await?
                else {
                    return Err(RuntimeError::OperationCancelled);
                };
                validate_secondary_removal_receipt(&receipt, &token)?;
                if receipt.witness.as_ref() != Some(&expected_witness)
                    || receipt.accepted.as_ref() != Some(&expected_committed)
                {
                    return Err(RuntimeError::OperationCancelled);
                }
                self.common
                    .accept_secondary_removal_receipt(&receipt)
                    .await?;
                *self.topology_receipt.write().await =
                    Some(TopologyReceipt::SecondaryRemoval(receipt));
                Ok(())
            }
            RuntimeEffectAction::AcceptSecondaryRemovalCommit(committed) => {
                let token = self.legacy.native_fence().await?;
                let expected = (*committed).clone();
                let TopologyReceipt::SecondaryRemoval(receipt) = self
                    .legacy
                    .apply_topology(RuntimeEffectAction::AcceptSecondaryRemovalCommit(committed))
                    .await?
                else {
                    return Err(RuntimeError::OperationCancelled);
                };
                validate_secondary_removal_receipt(&receipt, &token)?;
                if receipt.accepted.as_ref() != Some(&expected) {
                    return Err(RuntimeError::OperationCancelled);
                }
                self.common
                    .accept_secondary_removal_receipt(&receipt)
                    .await?;
                *self.topology_receipt.write().await =
                    Some(TopologyReceipt::SecondaryRemoval(receipt));
                Ok(())
            }
            RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(command) => {
                let token = self.legacy.native_fence().await?;
                let TopologyReceipt::SecondaryRemoval(receipt) = self
                    .legacy
                    .apply_topology(RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(
                        command,
                    ))
                    .await?
                else {
                    return Err(RuntimeError::OperationCancelled);
                };
                validate_secondary_removal_receipt(&receipt, &token)?;
                self.common
                    .accept_secondary_removal_receipt(&receipt)
                    .await?;
                *self.topology_receipt.write().await =
                    Some(TopologyReceipt::SecondaryRemoval(receipt));
                Ok(())
            }
            RuntimeEffectAction::FenceRetirement(retired) => {
                let TopologyReceipt::Retirement(receipt) = self
                    .legacy
                    .apply_topology(RuntimeEffectAction::FenceRetirement(retired.clone()))
                    .await?
                else {
                    return Err(RuntimeError::OperationCancelled);
                };
                validate_retirement_receipt(&receipt, &retired, false)?;
                self.common.fence_retirement_state(&retired).await?;
                *self.topology_receipt.write().await = Some(TopologyReceipt::Retirement(receipt));
                Ok(())
            }
            RuntimeEffectAction::CompleteRetirement(retired) => {
                let TopologyReceipt::Retirement(receipt) = self
                    .legacy
                    .apply_topology(RuntimeEffectAction::CompleteRetirement(retired.clone()))
                    .await?
                else {
                    return Err(RuntimeError::OperationCancelled);
                };
                validate_retirement_receipt(&receipt, &retired, true)?;
                self.common.complete_retirement_state(&retired).await?;
                *self.topology_receipt.write().await = Some(TopologyReceipt::Retirement(receipt));
                Ok(())
            }
            _ => Err(RuntimeError::Application(
                "managed removal proof requires a removal action".into(),
            )),
        }
    }

    async fn publish_managed_access(
        &self,
        projection: AccessProjection,
    ) -> Result<(AccessProjection, NativeOperationToken)> {
        let preparation = match self
            .legacy
            .prepare_access(projection.read, projection.write)
            .await
        {
            Ok(preparation) => preparation,
            Err(error) => {
                self.common.release_access_reservation(&projection).await;
                return Err(error);
            }
        };
        if let Err(error) = self.common.complete_access_projection(&projection).await {
            self.common
                .rollback_common_access_projection(&projection)
                .await;
            return Err(error);
        }
        if let Err(error) = validate_access_preparation(&preparation, &projection) {
            self.common
                .rollback_common_access_projection(&projection)
                .await;
            return Err(error);
        }
        let _commit = self.common.access_commit.lock().await;
        if let Err(error) = self.common.validate_access_projection(&projection).await {
            self.common
                .rollback_common_access_projection_locked(&projection)
                .await;
            return Err(error);
        }
        if let Err(error) = self.legacy.publish_access(preparation.clone()).await {
            self.common
                .rollback_common_access_projection_locked(&projection)
                .await;
            return Err(error);
        }
        if let Err(error) = self
            .common
            .publish_access_projection_locked(projection.clone())
            .await
        {
            let _ = self.legacy.fence_writes().await;
            self.common
                .rollback_common_access_projection_locked(&projection)
                .await;
            return Err(error);
        }
        Ok((
            projection,
            NativeOperationToken {
                authority: preparation.authority,
                engine_session_id: preparation.engine_session_id,
                engine_generation: preparation.engine_generation,
            },
        ))
    }

    async fn rollback_managed_access(&self, projection: &AccessProjection) {
        let _commit = self.common.access_commit.lock().await;
        if self
            .common
            .published_access_generation
            .load(Ordering::Acquire)
            > projection.access_generation
        {
            return;
        }
        let _ = self.legacy.fence_writes().await;
        self.common
            .rollback_common_access_projection_locked(projection)
            .await;
    }

    async fn rollback_cancelled_managed_access(&self, projection: &AccessProjection) {
        let _commit = self.common.access_commit.lock().await;
        if self.common.access_generation.load(Ordering::Acquire) != projection.access_generation
            || self
                .common
                .published_access_generation
                .load(Ordering::Acquire)
                > projection.access_generation
        {
            return;
        }
        let _ = self.legacy.fence_writes().await;
        self.common
            .rollback_common_access_projection_locked(projection)
            .await;
    }

    async fn execute_access_transaction(
        &self,
        read: AccessStatus,
        write: AccessStatus,
        mut ready: oneshot::Sender<()>,
        accept: oneshot::Receiver<()>,
        accepted: oneshot::Sender<Option<NativeProgressStatus>>,
        decision: oneshot::Receiver<AccessDecision>,
    ) -> Result<()> {
        let projection = self.common.reserve_access_projection(read, write).await?;
        let publication = {
            let publication = self.publish_managed_access(projection.clone());
            tokio::pin!(publication);
            tokio::select! {
                result = &mut publication => Some(result),
                _ = ready.closed() => None,
            }
        };
        let Some(publication) = publication else {
            self.rollback_cancelled_managed_access(&projection).await;
            return Err(RuntimeError::OperationCancelled);
        };
        let (projection, native_token) = publication?;
        if ready.send(()).is_err() || accept.await.is_err() {
            self.rollback_managed_access(&projection).await;
            return Err(RuntimeError::OperationCancelled);
        }
        let common_guard = self.common.access_commit.clone().lock_owned().await;
        if self.common.access_generation.load(Ordering::Acquire) != projection.access_generation
            || self
                .common
                .published_access_generation
                .load(Ordering::Acquire)
                != projection.access_generation
        {
            drop(common_guard);
            self.rollback_managed_access(&projection).await;
            return Err(RuntimeError::OperationCancelled);
        }
        let native_guard = match self.legacy.lock_native_fence(&native_token).await {
            Ok(guard) => guard,
            Err(error) => {
                drop(common_guard);
                self.rollback_managed_access(&projection).await;
                return Err(error);
            }
        };
        let progress = native_guard.progress().clone();
        if accepted.send(Some(progress)).is_err() {
            drop(native_guard);
            let _ = self.legacy.fence_writes().await;
            self.common
                .rollback_common_access_projection_locked(&projection)
                .await;
            drop(common_guard);
            return Err(RuntimeError::OperationCancelled);
        }
        let decision = decision.await.unwrap_or(AccessDecision::Reject);
        drop(native_guard);
        match decision {
            AccessDecision::Commit(commit)
                if self
                    .common
                    .validate_access_projection(&projection)
                    .await
                    .is_ok() =>
            {
                drop(common_guard);
                commit.record().await;
                Ok(())
            }
            AccessDecision::Commit(_) | AccessDecision::Reject => {
                let _ = self.legacy.fence_writes().await;
                self.common
                    .rollback_common_access_projection_locked(&projection)
                    .await;
                drop(common_guard);
                Err(RuntimeError::OperationCancelled)
            }
        }
    }
}

pub(super) struct ReplicatorLifecycleRegistration {
    pub(super) process: super::lifecycle::ProcessRuntime,
    pub(super) authority: super::lifecycle::AuthorityRuntime,
    pub(super) peer: super::lifecycle::PeerRuntime,
    pub(super) access_closure: super::lifecycle::AccessClosure,
    pub(super) access: super::lifecycle::AccessRuntime,
    pub(super) report: super::lifecycle::ReportLifecycle,
    pub(super) evidence: super::lifecycle::EvidenceRuntime,
    pub(super) effect_evidence: super::lifecycle::EffectEvidenceRuntime,
    pub(super) build: super::lifecycle::BuildLifecycleRuntime,
    pub(super) build_cancellation: super::lifecycle::BuildCancellationRuntime,
    pub(super) outbound: super::lifecycle::OutboundLifecycleRuntime,
    pub(super) removal_witness: super::lifecycle::RemovalWitnessRuntime,
    pub(super) topology: super::lifecycle::TopologyRuntime,
    pub(super) recovery: super::lifecycle::RecoveryRuntime,
}

impl ReplicatorLifecycleRegistration {
    pub(super) fn managed(
        host: Weak<RuntimeHost>,
        control: Arc<dyn Replicator>,
        primary: Arc<dyn PrimaryReplicator>,
        lifecycle: Arc<dyn ManagedReplicatorLifecycle>,
    ) -> Self {
        let common = Arc::new(CustomReplicatorHost::new(host, control, primary, false));
        let backend = Arc::new(ManagedLifecycleBackend {
            legacy: lifecycle,
            common: common.clone(),
            accepted_builds: RwLock::default(),
            topology_receipt: RwLock::default(),
        });
        let build: Arc<dyn BuildCancellation> = backend.clone();
        common.bind_build_cancellation(Arc::downgrade(&build));
        Self::from_wiring(LifecycleWiring::new(backend), true)
    }

    pub(super) fn service(
        host: Weak<RuntimeHost>,
        control: Arc<dyn Replicator>,
        primary: Arc<dyn PrimaryReplicator>,
    ) -> Self {
        let backend = Arc::new(CustomReplicatorHost::new(host, control, primary, true));
        let build: Arc<dyn BuildCancellation> = backend.clone();
        backend.bind_build_cancellation(Arc::downgrade(&build));
        Self::from_wiring(LifecycleWiring::new(backend), false)
    }

    fn from_wiring(wiring: LifecycleWiring, managed: bool) -> Self {
        let process = wiring.process_runtime();
        let authority = wiring.authority_runtime();
        let peer = wiring.peer_runtime();
        let access_closure = wiring.access_closure();
        let access = wiring.access_runtime();
        let report = wiring.report_lifecycle();
        let evidence = wiring.evidence_runtime();
        let effect_evidence = wiring.effect_evidence_runtime();
        let build = wiring.build_runtime(managed);
        let build_cancellation = wiring.build_cancellation();
        let outbound = wiring.outbound_runtime();
        let removal_witness = wiring.removal_witness_runtime();
        let topology = wiring.topology_runtime();
        let recovery = wiring.recovery_runtime();
        Self {
            process,
            authority,
            peer,
            access_closure,
            access,
            report,
            evidence,
            effect_evidence,
            build,
            build_cancellation,
            outbound,
            removal_witness,
            topology,
            recovery,
        }
    }
}

pub(super) struct CustomReplicatorHost {
    host: Weak<RuntimeHost>,
    control: Arc<dyn Replicator>,
    primary: Arc<dyn PrimaryReplicator>,
    native_receipts: bool,
    abort_notified: std::sync::atomic::AtomicBool,
    gate: Mutex<()>,
    state: Arc<RwLock<RuntimeSnapshot>>,
    sessions: RwLock<BTreeMap<ReplicaIdentity, ProcessSessionId>>,
    addresses: RwLock<BTreeMap<ReplicaIdentity, (ProcessSessionId, String)>>,
    retired_sessions: RwLock<BTreeSet<(ReplicaIdentity, ProcessSessionId)>>,
    retired_builds: RwLock<BTreeSet<OperationId>>,
    build_generations: RwLock<BTreeMap<OperationId, u64>>,
    cancelling_builds: RwLock<BTreeMap<OperationId, u64>>,
    build_cleanup_locks: Mutex<BTreeMap<OperationId, Arc<Mutex<()>>>>,
    build_cancellation: OnceLock<Weak<dyn BuildCancellation>>,
    pending_builds: RwLock<BTreeMap<OperationId, ReplicaEndpoint>>,
    receipts: RwLock<BTreeMap<OperationId, BuildAdmission>>,
    configuration: Arc<RwLock<Option<ReplicaSetConfiguration>>>,
    deferred_configuration: Mutex<Option<DeferredConfiguration>>,
    deferred_configuration_abort: StdMutex<Option<tokio::task::AbortHandle>>,
    configuration_generation: Arc<AtomicU64>,
    configuration_commit: Arc<Mutex<()>>,
    access_generation: Arc<AtomicU64>,
    published_access_generation: Arc<AtomicU64>,
    access_commit: Arc<Mutex<()>>,
    session_registration: Mutex<()>,
    restored_access: RwLock<Option<(AccessStatus, AccessStatus)>>,
    removal_witnesses:
        RwLock<BTreeMap<ReplicaIdentity, crate::protocol::types::SecondaryRemovalWitness>>,
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
            state: Arc::new(RwLock::new(snapshot)),
            sessions: RwLock::default(),
            addresses: RwLock::default(),
            retired_sessions: RwLock::default(),
            retired_builds: RwLock::default(),
            build_generations: RwLock::default(),
            cancelling_builds: RwLock::default(),
            build_cleanup_locks: Mutex::default(),
            build_cancellation: OnceLock::new(),
            pending_builds: RwLock::default(),
            receipts: RwLock::default(),
            configuration: Arc::new(RwLock::default()),
            deferred_configuration: Mutex::new(None),
            deferred_configuration_abort: StdMutex::new(None),
            configuration_generation: Arc::new(AtomicU64::new(0)),
            configuration_commit: Arc::new(Mutex::new(())),
            access_generation: Arc::new(AtomicU64::new(0)),
            published_access_generation: Arc::new(AtomicU64::new(0)),
            access_commit: Arc::new(Mutex::new(())),
            session_registration: Mutex::new(()),
            restored_access: RwLock::default(),
            removal_witnesses: RwLock::default(),
            outbound,
            receiver: Mutex::new(receiver),
            changed: Notify::new(),
        }
    }

    fn bind_build_cancellation(&self, build: Weak<dyn BuildCancellation>) {
        self.build_cancellation
            .set(build)
            .expect("build cancellation owner is bound once");
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

    async fn receipt(&self, authority: &BuildAuthority) -> Result<BuildAdmission> {
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
        Ok(BuildAdmission {
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
            native_token: None,
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

    async fn reserve_access_projection(
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
        let (previous_read, previous_write) = {
            let state = self.state.read().await;
            (state.read_status, state.write_status)
        };
        let configuration_before = self.published_configuration().await;
        let sessions_before = self.sessions.read().await.clone();
        let role_before = host.state.read().await.fallback_snapshot.role;
        let configuration_generation = self.configuration_generation.load(Ordering::Acquire);
        if write == AccessStatus::Granted {
            if role_before != crate::protocol::types::ReplicaRole::Primary {
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
        let _commit = self.access_commit.lock().await;
        self.active_host()?;
        if authority_before != self.state.read().await.authority
            || configuration_before != self.published_configuration().await
            || sessions_before != *self.sessions.read().await
            || role_before != host.state.read().await.fallback_snapshot.role
            || configuration_generation != self.configuration_generation.load(Ordering::Acquire)
        {
            return Err(RuntimeError::OperationCancelled);
        }
        // Advancing the generation is the final operation before returning the
        // reservation. The caller installs rollback ownership immediately,
        // without an intervening await.
        let access_generation = self.advance_access_generation()?;
        Ok(AccessProjection {
            previous_read,
            previous_write,
            read,
            write,
            authority: authority_before,
            configuration: configuration_before,
            sessions: sessions_before,
            role: role_before,
            configuration_generation,
            access_generation,
            faulted_grant,
        })
    }

    async fn complete_access_projection(&self, projection: &AccessProjection) -> Result<()> {
        let progress = super::with_access_proof_view(
            projection.read,
            projection.write,
            self.control.current_progress(),
        )
        .await;
        if let Err(error) = progress {
            if !matches!(
                error,
                RuntimeError::ReconfigurationPending | RuntimeError::OperationCancelled
            ) {
                let _commit = self.access_commit.lock().await;
                if self.published_access_generation.load(Ordering::Acquire)
                    <= projection.access_generation
                    && self.access_generation.load(Ordering::Acquire)
                        == projection.access_generation
                {
                    self.control.abort();
                }
            }
            return Err(error);
        }
        self.active_host()?;
        self.validate_access_projection(projection).await?;
        Ok(())
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
        self.publish_access_projection_locked(projection).await
    }

    async fn publish_access_projection_locked(&self, projection: AccessProjection) -> Result<()> {
        self.validate_access_projection(&projection).await?;
        let host = self.active_host()?;
        let configuration = self.published_configuration().await;
        let sessions = self.sessions.read().await.clone();
        let mut state = self.state.write().await;
        let mut host_state = host.state.write().await;
        if projection.authority != state.authority
            || projection.configuration != configuration
            || projection.sessions != sessions
            || projection.role != host_state.fallback_snapshot.role
            || projection.configuration_generation
                != self.configuration_generation.load(Ordering::Acquire)
            || projection.access_generation != self.access_generation.load(Ordering::Acquire)
            || self
                .abort_notified
                .load(std::sync::atomic::Ordering::Acquire)
            || host.aborted.load(std::sync::atomic::Ordering::Acquire)
            || host.closed.load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(RuntimeError::OperationCancelled);
        }
        // Both projection locks are held before either view changes. From this
        // point to commit there is no await, so cancellation cannot leave a
        // partially granted common/external projection.
        state.read_status = projection.read;
        state.write_status = projection.write;
        host_state.fallback_snapshot.read_status = projection.read;
        host_state.fallback_snapshot.write_status = projection.write;
        self.published_access_generation
            .store(projection.access_generation, Ordering::Release);
        if projection.faulted_grant {
            Err(RuntimeError::ReconfigurationPending)
        } else {
            Ok(())
        }
    }

    async fn set_access(&self, read: AccessStatus, write: AccessStatus) -> Result<()> {
        self.publish_common_access(read, write).await.map(|_| ())
    }

    async fn publish_common_access(
        &self,
        read: AccessStatus,
        write: AccessStatus,
    ) -> Result<AccessProjection> {
        let projection = self.reserve_access_projection(read, write).await?;
        self.publish_common_access_projection(projection).await
    }

    async fn publish_common_access_projection(
        &self,
        projection: AccessProjection,
    ) -> Result<AccessProjection> {
        if let Err(error) = self.complete_access_projection(&projection).await {
            self.cleanup_failed_access_projection(
                &projection,
                matches!(error, RuntimeError::OperationCancelled),
            )
            .await;
            return Err(error);
        }
        if let Err(error) = self.publish_access_projection(projection.clone()).await {
            self.cleanup_failed_access_projection(
                &projection,
                matches!(error, RuntimeError::OperationCancelled),
            )
            .await;
            return Err(error);
        }
        Ok(projection)
    }

    async fn execute_common_access_transaction(
        &self,
        read: AccessStatus,
        write: AccessStatus,
        mut ready: oneshot::Sender<()>,
        accept: oneshot::Receiver<()>,
        accepted: oneshot::Sender<Option<NativeProgressStatus>>,
        decision: oneshot::Receiver<AccessDecision>,
    ) -> Result<()> {
        let projection = self.reserve_access_projection(read, write).await?;
        let publication = {
            let publication = self.publish_common_access_projection(projection.clone());
            tokio::pin!(publication);
            tokio::select! {
                result = &mut publication => Some(result),
                _ = ready.closed() => None,
            }
        };
        let Some(publication) = publication else {
            self.rollback_cancelled_common_access(&projection).await;
            return Err(RuntimeError::OperationCancelled);
        };
        let projection = publication?;
        if ready.send(()).is_err() || accept.await.is_err() {
            self.rollback_published_common_access(&projection).await;
            return Err(RuntimeError::OperationCancelled);
        }
        let guard = self.access_commit.clone().lock_owned().await;
        if self.access_generation.load(Ordering::Acquire) != projection.access_generation
            || self.published_access_generation.load(Ordering::Acquire)
                != projection.access_generation
        {
            drop(guard);
            self.rollback_common_access_projection(&projection).await;
            return Err(RuntimeError::OperationCancelled);
        }
        if accepted.send(None).is_err() {
            self.rollback_published_common_access_locked(&projection)
                .await;
            drop(guard);
            return Err(RuntimeError::OperationCancelled);
        }
        let decision = decision.await.unwrap_or(AccessDecision::Reject);
        match decision {
            AccessDecision::Commit(commit)
                if self.validate_access_projection(&projection).await.is_ok() =>
            {
                drop(guard);
                commit.record().await;
                Ok(())
            }
            AccessDecision::Commit(_) | AccessDecision::Reject => {
                self.rollback_published_common_access_locked(&projection)
                    .await;
                drop(guard);
                Err(RuntimeError::OperationCancelled)
            }
        }
    }

    async fn rollback_common_access_projection(&self, projection: &AccessProjection) {
        let _commit = self.access_commit.lock().await;
        let owns_publication = self.published_access_generation.load(Ordering::Acquire)
            <= projection.access_generation;
        self.rollback_common_access_projection_locked(projection)
            .await;
        if owns_publication
            && projection.previous_read != AccessStatus::Granted
            && projection.previous_write != AccessStatus::Granted
        {
            let _ = self.close_custom_native_access(true).await;
        }
    }

    async fn release_access_reservation(&self, projection: &AccessProjection) {
        let _commit = self.access_commit.lock().await;
        if self.access_generation.load(Ordering::Acquire) == projection.access_generation
            && self.published_access_generation.load(Ordering::Acquire)
                < projection.access_generation
        {
            self.access_generation.store(
                projection.access_generation.saturating_add(1),
                Ordering::Release,
            );
        }
    }

    async fn rollback_cancelled_common_access(&self, projection: &AccessProjection) {
        let _commit = self.access_commit.lock().await;
        if self.access_generation.load(Ordering::Acquire) != projection.access_generation {
            return;
        }
        if self.published_access_generation.load(Ordering::Acquire) > projection.access_generation {
            return;
        }
        self.rollback_common_access_projection_locked(projection)
            .await;
        if projection.previous_read != AccessStatus::Granted
            && projection.previous_write != AccessStatus::Granted
        {
            let _ = self.close_custom_native_access(true).await;
        }
    }

    async fn rollback_published_common_access(&self, projection: &AccessProjection) {
        let _commit = self.access_commit.lock().await;
        self.rollback_published_common_access_locked(projection)
            .await;
    }

    async fn cleanup_failed_access_projection(
        &self,
        projection: &AccessProjection,
        preserve_existing: bool,
    ) {
        let _commit = self.access_commit.lock().await;
        let access_generation = self.access_generation.load(Ordering::Acquire);
        let published_generation = self.published_access_generation.load(Ordering::Acquire);
        if access_generation > projection.access_generation
            && published_generation <= projection.access_generation
        {
            return;
        }
        if published_generation > projection.access_generation {
            let state = self.state.read().await;
            if state.read_status == AccessStatus::Granted
                || state.write_status == AccessStatus::Granted
            {
                return;
            }
            drop(state);
            let _ = self.close_custom_native_access(false).await;
            return;
        }
        if access_generation == projection.access_generation
            && published_generation < projection.access_generation
        {
            if preserve_existing {
                self.access_generation.store(
                    projection.access_generation.saturating_add(1),
                    Ordering::Release,
                );
            } else {
                self.rollback_common_access_projection_locked(projection)
                    .await;
            }
        }
    }

    async fn rollback_published_common_access_locked(&self, projection: &AccessProjection) {
        if self.published_access_generation.load(Ordering::Acquire) > projection.access_generation {
            return;
        }
        self.rollback_common_access_projection_locked(projection)
            .await;
        let _ = self.close_custom_native_access(true).await;
    }

    async fn close_custom_native_access(&self, abort_on_failure: bool) -> Result<()> {
        let result = tokio::time::timeout(
            ACCESS_CLOSE_TIMEOUT,
            super::with_access_proof_view(
                AccessStatus::ReconfigurationPending,
                AccessStatus::ReconfigurationPending,
                self.control.current_progress(),
            ),
        )
        .await;
        match result {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(error)) => {
                if abort_on_failure {
                    self.notify_abort();
                    self.control.abort();
                }
                Err(error)
            }
            Err(_) => {
                if abort_on_failure {
                    self.notify_abort();
                    self.control.abort();
                }
                Err(RuntimeError::OperationCancelled)
            }
        }
    }

    async fn rollback_common_access_projection_locked(&self, projection: &AccessProjection) {
        if self.published_access_generation.load(Ordering::Acquire) > projection.access_generation {
            return;
        }
        if self.access_generation.load(Ordering::Acquire) == projection.access_generation {
            self.access_generation.store(
                projection.access_generation.saturating_add(1),
                Ordering::Release,
            );
        }
        let mut state = self.state.write().await;
        state.read_status = AccessStatus::ReconfigurationPending;
        state.write_status = AccessStatus::ReconfigurationPending;
        drop(state);
        if let Some(host) = self.host.upgrade() {
            let mut state = host.state.write().await;
            state.fallback_snapshot.read_status = AccessStatus::ReconfigurationPending;
            state.fallback_snapshot.write_status = AccessStatus::ReconfigurationPending;
        }
    }

    async fn accept_catch_up_completion(&self, progress: i64) -> Result<()> {
        let mut state = self.state.write().await;
        state.current_progress = state.current_progress.max(progress);
        state.catch_up_boundary = Some(progress);
        state.catch_up_complete = true;
        Ok(())
    }

    async fn install_topology_status(&self, status: NativeTopologyStatus) -> Result<()> {
        let mut state = self.state.write().await;
        state.prepared_secondary_removal = status.prepared_secondary_removal;
        state.accepted_secondary_removal = status.accepted_secondary_removal;
        state.retired_authority = status.retired_authority;
        Ok(())
    }

    async fn accept_certified_prefix(&self, receipt: &CertifiedPrefixReceipt) -> Result<()> {
        let mut state = self.state.write().await;
        if state.authority != receipt.token.authority {
            return Err(RuntimeError::OperationCancelled);
        }
        state.current_progress = state.current_progress.max(receipt.verified_lsn);
        state.verified_replication_lsn = Some(
            state
                .verified_replication_lsn
                .map_or(receipt.verified_lsn, |verified| {
                    verified.max(receipt.verified_lsn)
                }),
        );
        state.committed_lsn = state.committed_lsn.max(receipt.committed_lsn);
        Ok(())
    }

    async fn accept_switchover_receipt(&self, receipt: &SwitchoverReceipt) -> Result<()> {
        let mut state = self.state.write().await;
        if state.authority != receipt.token.authority {
            return Err(RuntimeError::OperationCancelled);
        }
        state.current_progress = state.current_progress.max(receipt.handoff_lsn);
        state.committed_lsn = state.committed_lsn.max(receipt.committed_lsn);
        Ok(())
    }

    async fn accept_secondary_removal_receipt(
        &self,
        receipt: &SecondaryRemovalReceipt,
    ) -> Result<()> {
        let mut state = self.state.write().await;
        if state.authority != receipt.token.authority {
            return Err(RuntimeError::OperationCancelled);
        }
        if let Some(preparation) = &receipt.preparation {
            state.prepared_secondary_removal = Some(preparation.clone());
            state.current_progress = state.current_progress.max(preparation.boundary_lsn);
        }
        if let Some(accepted) = &receipt.accepted {
            state.accepted_secondary_removal = Some(accepted.clone());
            state.current_progress = state
                .current_progress
                .max(accepted.evidence.preparation.boundary_lsn);
        }
        if let Some(verified_lsn) = receipt.verified_lsn {
            state.verified_replication_lsn = Some(
                state
                    .verified_replication_lsn
                    .map_or(verified_lsn, |verified| verified.max(verified_lsn)),
            );
            state.current_progress = state.current_progress.max(verified_lsn);
        }
        state.committed_lsn = state.committed_lsn.max(receipt.committed_lsn);
        Ok(())
    }

    async fn accept_managed_build(&self, receipt: &BuildAdmission) -> Result<()> {
        let mut state = self.state.write().await;
        state
            .builds
            .retain(|build| build.authority.build_id != receipt.selection.authority.build_id);
        state.builds.push(BuildPostcondition {
            authority: receipt.selection.authority.clone(),
            last_sequence: 0,
            durable_lsn: receipt.selection.authority.replication_boundary_lsn,
            completed: true,
            catch_up_boundary_lsn: Some(receipt.selection.authority.replication_boundary_lsn),
        });
        Ok(())
    }

    async fn record_completion(&self, receipt: BuildAdmission) -> Result<()> {
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

    async fn record_build_start(&self, receipt: &BuildAdmission) -> Result<()> {
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
        let Some(admission) = self.begin_build_attempt(&endpoint).await? else {
            return Ok(());
        };
        let result = {
            let _gate = self.gate.lock().await;
            self.ensure_build_generation(&endpoint.build_id, admission.generation())
                .await?;
            self.enqueue_outbound(OutboundOperation::Build(endpoint))
        };
        admission.finish(result.is_ok()).await;
        result
    }

    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    async fn enqueue_build_wait(&self, endpoint: ReplicaEndpoint) -> Result<()> {
        let Some(admission) = self.begin_build_attempt(&endpoint).await? else {
            return Ok(());
        };
        let build_id = endpoint.build_id.clone();
        let result = self
            .enqueue_build_wait_inner(endpoint, admission.generation())
            .await;
        let current_generation = self.build_generation(&build_id).await;
        let admission_generation = admission.generation();
        admission
            .finish(result.is_ok() || current_generation != admission_generation)
            .await;
        result
    }

    async fn begin_build_attempt(
        &self,
        endpoint: &ReplicaEndpoint,
    ) -> Result<Option<BuildQueueAdmission>> {
        let _gate = self.gate.lock().await;
        if self
            .cancelling_builds
            .read()
            .await
            .contains_key(&endpoint.build_id)
        {
            return Err(RuntimeError::ReconfigurationPending);
        }
        let mut pending = self.pending_builds.write().await;
        if let Some(existing) = pending.get(&endpoint.build_id) {
            if existing == endpoint {
                return Ok(None);
            }
            return Err(RuntimeError::AuthorityMismatch(
                "build ID is already queued for another exact target".into(),
            ));
        }
        let mut generations = self.build_generations.write().await;
        let generation = generations.entry(endpoint.build_id.clone()).or_default();
        *generation = generation
            .checked_add(1)
            .ok_or(RuntimeError::OperationCancelled)?;
        let attempt_generation = *generation;
        pending.insert(endpoint.build_id.clone(), endpoint.clone());
        let build = self
            .build_cancellation
            .get()
            .cloned()
            .ok_or(RuntimeError::Closed)?;
        Ok(Some(BuildQueueAdmission::new(
            build,
            endpoint.build_id.clone(),
            attempt_generation,
        )))
    }

    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    async fn enqueue_build_wait_inner(
        &self,
        endpoint: ReplicaEndpoint,
        generation: u64,
    ) -> Result<()> {
        let build_id = endpoint.build_id.clone();
        let mut operation = OutboundOperation::Build(endpoint);
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(75);
        loop {
            let send = {
                let _gate = self.gate.lock().await;
                self.ensure_build_generation(&build_id, generation).await?;
                self.outbound.try_send(operation)
            };
            match send {
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

    async fn prepare_build(&self, replica: &mut ReplicaInformation) -> Result<BuildAdmission> {
        let authority = self.prepare_build_description(replica).await?;
        self.receipt(&authority).await
    }

    pub(super) async fn execute_build(&self, mut replica: ReplicaInformation) -> Result<()> {
        let receipt = self.prepare_build(&mut replica).await?;
        self.record_build_start(&receipt).await?;
        self.primary.build_replica(replica).await?;
        self.active_host()?;
        let _gate = self.gate.lock().await;
        self.record_completion(receipt.clone()).await?;
        self.pending_builds
            .write()
            .await
            .remove(&receipt.selection.authority.build_id);
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
        state.role = crate::protocol::types::ReplicaRole::None;
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
        let _access = self.access_commit.lock().await;
        self.invalidate_configuration_attempts_locked().await
    }

    async fn invalidate_configuration_attempts_locked(&self) -> Result<()> {
        self.advance_access_generation()?;
        self.invalidate_configuration_attempts_without_access_locked()
            .await
    }

    async fn invalidate_configuration_attempts_without_access_locked(&self) -> Result<()> {
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
        let _access = self.access_commit.lock().await;
        self.invalidate_build_attempts_locked().await
    }

    async fn invalidate_build_attempts_locked(&self) -> Result<()> {
        self.invalidate_configuration_attempts_locked().await?;
        self.invalidate_build_attempts_without_access_locked().await
    }

    async fn invalidate_build_attempts_without_access_locked(&self) -> Result<()> {
        for generation in self.build_generations.write().await.values_mut() {
            *generation = generation
                .checked_add(1)
                .ok_or(RuntimeError::OperationCancelled)?;
        }
        self.pending_builds.write().await.clear();
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

    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    async fn remove_managed_replica(
        &self,
        replica_id: crate::protocol::types::ReplicaId,
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
        {
            let mut generations = self.build_generations.write().await;
            for build_id in &retired {
                let generation = generations.entry(build_id.clone()).or_default();
                *generation = generation
                    .checked_add(1)
                    .ok_or(RuntimeError::OperationCancelled)?;
            }
        }
        self.state
            .write()
            .await
            .builds
            .retain(|build| build.authority.target.replica_id != replica_id);
        self.receipts
            .write()
            .await
            .retain(|_, receipt| receipt.selection.authority.target.replica_id != replica_id);
        self.pending_builds
            .write()
            .await
            .retain(|_, endpoint| endpoint.identity.replica_id != replica_id);
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

    async fn fence_retirement_state(
        &self,
        retired: &crate::authority::RetiredAuthority,
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
        retired: &crate::authority::RetiredAuthority,
    ) -> Result<()> {
        let host = self.host()?;
        retired.validate(&host.identity)?;
        let closed = host.state.read().await.fallback_snapshot.clone();
        if closed.open || closed.role != crate::protocol::types::ReplicaRole::None {
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
            RuntimeEffectAction::RetireBuild(id) => {
                self.retired_builds.write().await.insert(id.clone());
                self.pending_builds.write().await.remove(&id);
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

    async fn cancel_common_build_attempt(
        &self,
        id: &OperationId,
        expected_generation: u64,
    ) -> Result<bool> {
        let _gate = self.gate.lock().await;
        self.cancel_common_build_attempt_locked(id, expected_generation)
            .await
    }

    async fn cancel_common_build_attempt_locked(
        &self,
        id: &OperationId,
        expected_generation: u64,
    ) -> Result<bool> {
        let mut generations = self.build_generations.write().await;
        let generation = generations.entry(id.clone()).or_default();
        if self.cancelling_builds.read().await.get(id) == Some(&expected_generation) {
            return Ok(*generation == expected_generation.saturating_add(1));
        }
        if *generation != expected_generation {
            return Ok(false);
        }
        *generation = generation
            .checked_add(1)
            .ok_or(RuntimeError::OperationCancelled)?;
        self.cancelling_builds
            .write()
            .await
            .insert(id.clone(), expected_generation);
        drop(generations);
        self.pending_builds.write().await.remove(id);
        self.state
            .write()
            .await
            .builds
            .retain(|build| &build.authority.build_id != id);
        self.changed.notify_waiters();
        Ok(true)
    }

    async fn complete_common_build_cancellation(&self, id: &OperationId, expected_generation: u64) {
        let mut cancelling = self.cancelling_builds.write().await;
        if cancelling.get(id) == Some(&expected_generation) {
            cancelling.remove(id);
        }
    }

    async fn build_generation(&self, id: &OperationId) -> u64 {
        self.build_generations
            .read()
            .await
            .get(id)
            .copied()
            .unwrap_or_default()
    }

    async fn build_cleanup_lock(&self, id: &OperationId) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self.build_cleanup_locks.lock().await;
            locks
                .entry(id.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        lock.lock_owned().await
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
        {
            let _gate = self.gate.lock().await;
            *self.restored_access.write().await = None;
            self.removal_witnesses.write().await.clear();
            self.invalidate_configuration_attempts_without_access_locked()
                .await?;
            self.invalidate_build_attempts_without_access_locked()
                .await?;
        }
        let _access = self.access_commit.lock().await;
        self.advance_access_generation()?;
        Ok(())
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
        {
            let _registration = self.session_registration.lock().await;
            let replaced = {
                let _access = self.access_commit.lock().await;
                self.validate_peer_session_replacement(&identity, &session)
                    .await?;
                self.register_common_peer_locked(identity, session).await?
            };
            if replaced {
                self.fence_writes().await?;
            }
        }
        self.configure().await
    }

    async fn validate_peer_session_replacement(
        &self,
        identity: &ReplicaIdentity,
        session: &ProcessSessionId,
    ) -> Result<bool> {
        if session.is_empty()
            || self
                .retired_sessions
                .read()
                .await
                .contains(&(identity.clone(), session.clone()))
        {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        Ok(self
            .sessions
            .read()
            .await
            .get(identity)
            .is_some_and(|old| old != session))
    }

    async fn register_common_peer_locked(
        &self,
        identity: ReplicaIdentity,
        session: ProcessSessionId,
    ) -> Result<bool> {
        let old = self
            .sessions
            .write()
            .await
            .insert(identity.clone(), session.clone());
        let replaced = if let Some(old) = old.filter(|old| old != &session) {
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
        };
        if replaced {
            self.invalidate_build_attempts_locked().await?;
        }
        Ok(replaced)
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
        let _session_registration = if matches!(&action, RuntimeEffectAction::AdmitAuthority(_)) {
            Some(self.session_registration.lock().await)
        } else {
            None
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
                self.pending_builds.write().await.remove(&id);
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
                crate::protocol::validation::validate_accept_secondary_removal_commit(&command)?;
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
                if closed.open || closed.role != crate::protocol::types::ReplicaRole::None {
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

    async fn narrow_postcondition(&self) -> RuntimePostcondition {
        let mut snapshot = self.snapshot().await;
        if let Some(host) = self.host.upgrade() {
            let fallback = host.state.read().await.fallback_snapshot.clone();
            snapshot.open = fallback.open;
            snapshot.role = fallback.role;
            snapshot.role_transition = fallback.role_transition;
        }
        snapshot.into()
    }

    pub(super) async fn cancel_outbound_build(
        &self,
        id: &OperationId,
        generation: u64,
    ) -> Result<()> {
        let _cleanup = self.build_cleanup_lock(id).await;
        if !self.cancel_common_build_attempt(id, generation).await? {
            return Ok(());
        }
        let result = async {
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
        .await;
        if cleanup_releases_claim(&result) {
            self.complete_common_build_cancellation(id, generation)
                .await;
        }
        result
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
impl ProcessLifecycle for CustomReplicatorHost {
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

    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    async fn invalidate_public_access(&self) -> Result<()> {
        self.fence_managed_access().await
    }

    async fn settle_primary_prefix(&self) -> Result<()> {
        CustomReplicatorHost::settle_primary_prefix(self).await
    }
}

#[async_trait]
impl AuthorityLifecycle for CustomReplicatorHost {
    async fn cancel_configuration_work(&self) -> Result<()> {
        CustomReplicatorHost::cancel_configuration_work(self).await
    }

    async fn restore_authority(&self) -> Result<()> {
        CustomReplicatorHost::restore_authority(self).await
    }

    async fn admit_authority(&self, authority: AdmittedAuthority) -> Result<()> {
        CustomReplicatorHost::apply_common_action(
            self,
            RuntimeEffectAction::AdmitAuthority(Box::new(authority)),
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

    async fn describe_peer(&self, replica: ReplicaInformation) -> Result<()> {
        CustomReplicatorHost::describe_peer(self, replica).await
    }
}

#[async_trait]
impl AccessLifecycle for CustomReplicatorHost {
    async fn defer_restored_access(&self, read: AccessStatus, write: AccessStatus) {
        *self.restored_access.write().await = Some((read, write));
    }

    async fn run_access_transaction(
        &self,
        read: AccessStatus,
        write: AccessStatus,
        ready: oneshot::Sender<()>,
        accept: oneshot::Receiver<()>,
        accepted: oneshot::Sender<Option<NativeProgressStatus>>,
        decision: oneshot::Receiver<AccessDecision>,
    ) -> Result<()> {
        self.execute_common_access_transaction(read, write, ready, accept, accepted, decision)
            .await
    }

    async fn restored_access(&self) -> Option<(AccessStatus, AccessStatus)> {
        *self.restored_access.read().await
    }
}

#[async_trait]
impl BuildLifecycle for CustomReplicatorHost {
    async fn admit_build_authority(&self, authority: BuildAuthority) -> Result<()> {
        CustomReplicatorHost::apply_common_action(
            self,
            RuntimeEffectAction::AdmitBuildAuthority(Box::new(authority)),
        )
        .await
    }

    async fn retire_build(&self, build_id: OperationId) -> Result<()> {
        CustomReplicatorHost::apply_common_action(self, RuntimeEffectAction::RetireBuild(build_id))
            .await
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
            self.ensure_build_generation(build_id, generation).await?;
            let snapshot = CustomReplicatorHost::snapshot(self).await;
            if snapshot.builds.iter().any(|build| {
                &build.authority.build_id == build_id
                    && &build.authority.target == target
                    && build.completed
            }) {
                self.ensure_build_generation(build_id, generation).await?;
                return Ok(());
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(RuntimeError::OperationCancelled);
            }
            tokio::select! {
                _ = changed => {}
                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
            }
        }
    }

    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    async fn build_replica(&self, replica: ReplicaInformation) -> Result<()> {
        let build_id = replica.build_id.clone();
        let target = replica.identity.clone();
        if self.snapshot().await.builds.iter().any(|build| {
            build.authority.build_id == build_id
                && build.authority.target == target
                && build.completed
        }) {
            return Ok(());
        }
        self.enqueue_build_wait(ReplicaEndpoint {
            build_id: build_id.clone(),
            identity: target.clone(),
            replication_address: replica.replication_address,
        })
        .await?;
        self.wait_for_build_completion(&build_id, &target).await
    }

    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    async fn remove_replica(&self, replica_id: crate::protocol::types::ReplicaId) -> Result<()> {
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

    async fn execute_build(&self, replica: ReplicaInformation) -> Result<Option<BuildAdmission>> {
        CustomReplicatorHost::execute_build(self, replica).await?;
        Ok(None)
    }

    async fn accept_build(&self, receipt: Option<BuildAdmission>) -> Result<()> {
        if receipt.is_some() {
            return Err(RuntimeError::Application(
                "independent build returned a managed acceptance receipt".into(),
            ));
        }
        Ok(())
    }

    async fn enqueue_build(&self, endpoint: ReplicaEndpoint) -> Result<()> {
        CustomReplicatorHost::enqueue_build(self, endpoint).await
    }

    async fn confirm_build_completion(
        &self,
        build_id: &OperationId,
        target: &ReplicaIdentity,
    ) -> Result<BuildCompletionConfirmation> {
        let receipt = self
            .receipts
            .read()
            .await
            .get(build_id)
            .cloned()
            .ok_or(RuntimeError::ReconfigurationPending)?;
        if &receipt.selection.authority.target != target
            || !self
                .receipt(&receipt.selection.authority)
                .await?
                .matches_host_admission(&receipt)
            || !self.state.read().await.builds.iter().any(|build| {
                &build.authority.build_id == build_id
                    && &build.authority.target == target
                    && build.completed
            })
        {
            return Err(RuntimeError::ReconfigurationPending);
        }
        Ok(BuildCompletionConfirmation {
            postcondition: self.narrow_postcondition().await,
            _native: None,
        })
    }
}

#[async_trait]
impl BuildCancellation for CustomReplicatorHost {
    async fn cancel_outbound_build(&self, id: &OperationId, generation: u64) -> Result<()> {
        CustomReplicatorHost::cancel_outbound_build(self, id, generation).await
    }

    async fn cancel_outbound_build_attempt(
        &self,
        id: &OperationId,
        generation: u64,
        public_cleanup: bool,
    ) -> Result<()> {
        let _cleanup = self.build_cleanup_lock(id).await;
        if !self.cancel_common_build_attempt(id, generation).await? {
            return Ok(());
        }
        let result = async {
            if public_cleanup {
                let _gate = self.gate.lock().await;
                if let Some(build) = self
                    .host()?
                    .default_dependencies
                    .build_authority_store
                    .load_build(id)
                    .await?
                {
                    self.primary.remove_replica(build.target.replica_id).await?;
                }
            }
            self.configure().await
        }
        .await;
        if cleanup_releases_claim(&result) {
            self.complete_common_build_cancellation(id, generation)
                .await;
        }
        result
    }

    async fn build_generation(&self, id: &OperationId) -> u64 {
        CustomReplicatorHost::build_generation(self, id).await
    }
}

#[async_trait]
impl TopologyLifecycle for CustomReplicatorHost {
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
        request_id: crate::protocol::types::SwitchoverRequestId,
        source: ReplicaIdentity,
        target: ReplicaIdentity,
        starting_configuration_id: crate::protocol::types::ConfigurationId,
        starting_epoch: crate::protocol::types::Epoch,
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

    async fn prepare_secondary_removal(
        &self,
        intent: crate::protocol::types::SecondaryScaleDownIntent,
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
        witness: crate::protocol::types::SecondaryRemovalWitness,
    ) -> Result<()> {
        CustomReplicatorHost::apply_common_action(
            self,
            RuntimeEffectAction::ObserveSecondaryRemovalWitness(Box::new(witness)),
        )
        .await
    }

    async fn observe_secondary_removal_progress(
        &self,
        witness: crate::protocol::types::SecondaryRemovalWitness,
        committed: crate::protocol::types::SecondaryScaleDownCleanup,
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
        committed: crate::protocol::types::SecondaryScaleDownCleanup,
    ) -> Result<()> {
        CustomReplicatorHost::apply_common_action(
            self,
            RuntimeEffectAction::AcceptSecondaryRemovalCommit(Box::new(committed)),
        )
        .await
    }

    async fn accept_historical_secondary_removal(
        &self,
        command: crate::protocol::command::AcceptSecondaryRemovalCommit,
    ) -> Result<()> {
        CustomReplicatorHost::apply_common_action(
            self,
            RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(Box::new(command)),
        )
        .await
    }

    async fn fence_retirement(&self, retired: crate::authority::RetiredAuthority) -> Result<()> {
        CustomReplicatorHost::apply_common_action(
            self,
            RuntimeEffectAction::FenceRetirement(Box::new(retired)),
        )
        .await
    }

    async fn complete_retirement(&self, retired: crate::authority::RetiredAuthority) -> Result<()> {
        CustomReplicatorHost::apply_common_action(
            self,
            RuntimeEffectAction::CompleteRetirement(Box::new(retired)),
        )
        .await
    }

    async fn topology_receipt(&self, _action: &RuntimeEffectAction) -> Option<TopologyReceipt> {
        None
    }
}

#[async_trait]
impl LifecycleObservation for CustomReplicatorHost {
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

    async fn snapshot(&self) -> RuntimeSnapshot {
        CustomReplicatorHost::snapshot(self).await
    }

    async fn postcondition(
        &self,
        _progress: Option<&NativeProgressStatus>,
    ) -> RuntimePostcondition {
        self.narrow_postcondition().await
    }
}

#[async_trait]
impl OutboundLifecycle for CustomReplicatorHost {
    async fn next_outbound(&self) -> Option<OutboundOperation> {
        CustomReplicatorHost::next_outbound(self).await
    }
}

fn preserves_same_primary_scale_up_access(
    existing: &AdmittedAuthority,
    next: &AdmittedAuthority,
) -> bool {
    matches!(
        next.scale_up.as_deref(),
        Some(crate::protocol::types::ScaleUpConfigurationEvidence::Admission { .. })
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
