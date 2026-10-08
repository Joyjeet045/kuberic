//! Independent custom-authority admission and recovery containment.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Weak};

use tokio::sync::{Mutex, MutexGuard};

use super::CustomReplicatorHost;
use crate::authority::AdmittedAuthority;
use crate::effects::{RuntimeEffect, RuntimeEffectAction};
use crate::{Result, RuntimeError};

#[cfg(all(test, feature = "testing"))]
use super::super::AccessEffectAcceptanceGate;
use super::super::RuntimeHost;

/// Host-lifetime owner for independent custom-authority containment.
pub(in super::super) struct CustomAuthorityContainment {
    host: Weak<RuntimeHost>,
    active_attempt: StdMutex<Weak<AtomicBool>>,
    invalidated: AtomicBool,
    access_requires_authorization: AtomicBool,
    pending_recovery: StdMutex<Option<RuntimeEffect>>,
    callback: Arc<Mutex<()>>,
    restoration: Mutex<()>,
    #[cfg(all(test, feature = "testing"))]
    publication_gate: StdMutex<Option<AccessEffectAcceptanceGate>>,
    #[cfg(all(test, feature = "testing"))]
    restoration_gate: StdMutex<Option<AccessEffectAcceptanceGate>>,
}

impl CustomAuthorityContainment {
    pub(in super::super) fn new(host: Weak<RuntimeHost>) -> Self {
        Self {
            host,
            active_attempt: StdMutex::new(Weak::new()),
            invalidated: AtomicBool::new(false),
            access_requires_authorization: AtomicBool::new(false),
            pending_recovery: StdMutex::new(None),
            callback: Arc::new(Mutex::new(())),
            restoration: Mutex::new(()),
            #[cfg(all(test, feature = "testing"))]
            publication_gate: StdMutex::new(None),
            #[cfg(all(test, feature = "testing"))]
            restoration_gate: StdMutex::new(None),
        }
    }

    pub(in super::super) fn attempt(&self) -> CustomAuthorityAttempt<'_> {
        let mut slot = self.active_attempt.lock().unwrap();
        let (entered, owned) = match slot.upgrade() {
            Some(entered) => (entered, false),
            None => {
                let entered = Arc::new(AtomicBool::new(false));
                self.invalidated.store(false, Ordering::Release);
                *slot = Arc::downgrade(&entered);
                (entered, true)
            }
        };
        CustomAuthorityAttempt {
            host: self.host.clone(),
            containment: self,
            entered,
            owned,
            complete: false,
        }
    }

    pub(in super::super) fn invalidate_attempt(&self) {
        if self
            .active_attempt
            .lock()
            .unwrap()
            .upgrade()
            .is_some_and(|entered| entered.load(Ordering::Acquire))
        {
            self.invalidated.store(true, Ordering::Release);
        }
    }

    pub(in super::super) fn attempt_entered(&self) -> bool {
        self.active_attempt
            .lock()
            .unwrap()
            .upgrade()
            .is_some_and(|entered| entered.load(Ordering::Acquire))
    }

    fn require_authorization(&self) {
        self.access_requires_authorization
            .store(true, Ordering::Release);
    }

    pub(in super::super) fn authorization_required(&self) -> bool {
        self.access_requires_authorization.load(Ordering::Acquire)
    }

    pub(in super::super) fn stage_recovery(&self, effect: Option<RuntimeEffect>) {
        *self.pending_recovery.lock().unwrap() =
            effect.filter(|effect| matches!(effect.action, RuntimeEffectAction::AdmitAuthority(_)));
    }

    pub(in super::super) fn finish_recovery(&self) {
        self.pending_recovery.lock().unwrap().take();
    }

    pub(in super::super) fn recovery_pending(&self) -> bool {
        self.pending_recovery.lock().unwrap().is_some()
    }

    pub(in super::super) fn callback_lock(&self) -> Arc<Mutex<()>> {
        self.callback.clone()
    }

    pub(in super::super) async fn restoration(&self) -> MutexGuard<'_, ()> {
        self.restoration.lock().await
    }

    pub(in super::super) async fn restore(&self, common: &CustomReplicatorHost) -> Result<()> {
        let session_registration = common.session_registration.lock().await;
        let gate = common.gate.lock().await;
        common.state.write().await.authority = common
            .host()?
            .default_dependencies
            .replica_authority_store
            .load()
            .await?;
        common.state.write().await.prepared_secondary_removal = common
            .host()?
            .default_dependencies
            .replica_authority_store
            .load_secondary_removal()
            .await?;
        common.restore_builds().await?;
        let pending = common
            .native_receipts
            .then(|| self.pending_recovery.lock().unwrap().clone())
            .flatten();
        if let Some(effect) = pending
            && let RuntimeEffectAction::AdmitAuthority(authority) = effect.action
        {
            drop(gate);
            drop(session_registration);
            return common
                .apply_common_action(RuntimeEffectAction::AdmitAuthority(authority))
                .await;
        }
        if common.native_receipts || common.state.read().await.authority.is_some() {
            common.configure().await?;
        }
        common.refresh().await
    }

    pub(in super::super) async fn admit(
        &self,
        common: &CustomReplicatorHost,
        authority: AdmittedAuthority,
    ) -> Result<()> {
        let _restoration = self.restoration.lock().await;
        let _session_registration = common.session_registration.lock().await;
        let _gate = common.gate.lock().await;
        let host = common.host()?;
        authority.validate()?;
        self.preflight(&host, &authority).await?;
        self.validate_scale_up(common, &authority).await?;
        let current = common
            .descriptions_with_policy(Some(authority.clone()), true)
            .await?
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        let _callback = self.callback.lock().await;
        common.active_host()?;
        let attempt = self.attempt();
        self.close_access(common, &attempt).await?;
        common.close_custom_native_access(false).await?;
        let configuration_generation = common.configuration_generation.load(Ordering::Acquire);
        let configuration = CustomReplicatorHost::apply_configuration_update(
            common.primary.clone(),
            current,
            authority.previous_configuration.clone(),
        )
        .await?;
        common.ensure_configuration_generation(configuration_generation)?;
        host.default_dependencies
            .replica_authority_store
            .admit(&authority)
            .await?;
        common.ensure_configuration_generation(configuration_generation)?;
        self.publish(common, configuration_generation, configuration, authority)
            .await?;
        attempt.complete();
        Ok(())
    }

    async fn preflight(&self, host: &RuntimeHost, authority: &AdmittedAuthority) -> Result<()> {
        let store = &host.default_dependencies.replica_authority_store;
        if authority.local_identity != host.identity
            || store.load_retired_authority().await?.is_some()
            || store.load_retirement_started().await?.is_some()
        {
            return Err(RuntimeError::AuthorityMismatch(
                "retired or mismatched local authority".into(),
            ));
        }
        if let Some(existing) = store.load().await? {
            existing.validate()?;
            if existing.local_identity != host.identity
                || authority.current_configuration.epoch < existing.current_configuration.epoch
                || (authority.current_configuration.epoch == existing.current_configuration.epoch
                    && existing != *authority
                    && !authority.is_current_only_completion_of(&existing))
            {
                return Err(RuntimeError::AuthorityMismatch(
                    "custom proposal conflicts with durable authority".into(),
                ));
            }
        }
        Ok(())
    }

    async fn validate_scale_up(
        &self,
        common: &CustomReplicatorHost,
        authority: &AdmittedAuthority,
    ) -> Result<()> {
        let host = common.host()?;
        if let Some(evidence) = &authority.scale_up {
            let intent = evidence.intent();
            if intent.target == host.identity
                && common
                    .state
                    .read()
                    .await
                    .authority
                    .as_ref()
                    .is_none_or(|old| old.current_configuration != authority.current_configuration)
            {
                let build = host
                    .default_dependencies
                    .build_authority_store
                    .load_build(&intent.build_id)
                    .await?
                    .ok_or(RuntimeError::AuthorityNotAdmitted)?;
                let current_receipt = common.receipt(&build).await?;
                let retained_receipt = common.receipts.read().await.get(&intent.build_id).cloned();
                if build.source != intent.primary
                    || build.target != intent.target
                    || build.current_configuration != intent.previous_configuration
                    || build.replication_boundary_lsn != intent.snapshot_boundary_lsn
                    || retained_receipt.as_ref().is_none_or(|retained| {
                        !retained.matches_durable_selection(&current_receipt)
                    })
                    || !common.state.read().await.builds.iter().any(|progress| {
                        progress.authority == build
                            && progress.completed
                            && progress.catch_up_boundary_lsn == Some(intent.catch_up_boundary_lsn)
                            && progress.durable_lsn >= intent.catch_up_boundary_lsn
                    })
                {
                    return Err(RuntimeError::AuthorityNotAdmitted);
                }
            }
        }
        Ok(())
    }

    async fn close_access(
        &self,
        common: &CustomReplicatorHost,
        attempt: &CustomAuthorityAttempt<'_>,
    ) -> Result<()> {
        let host = common.active_host()?;
        let access = common.access_commit.lock().await;
        let configuration = common.configuration_commit.lock().await;
        let generation = common.advance_access_generation()?;
        common.advance_configuration_generation()?;
        if let Some(abort) = common.deferred_configuration_abort.lock().unwrap().take() {
            abort.abort();
        }
        let deferred = common.deferred_configuration.lock().await.take();
        if let Some(handle) = &deferred {
            handle.handle.abort();
        }
        {
            let mut restored = common.restored_access.write().await;
            self.require_authorization();
            *restored = None;
        }
        common.removal_witnesses.write().await.clear();
        common
            .invalidate_build_attempts_without_access_locked()
            .await?;
        let mut state = common.state.write().await;
        let mut host_state = host.state.write().await;
        common.active_host()?;
        state.read_status = crate::protocol::types::AccessStatus::ReconfigurationPending;
        state.write_status = crate::protocol::types::AccessStatus::ReconfigurationPending;
        host_state.fallback_snapshot.read_status =
            crate::protocol::types::AccessStatus::ReconfigurationPending;
        host_state.fallback_snapshot.write_status =
            crate::protocol::types::AccessStatus::ReconfigurationPending;
        common
            .published_access_generation
            .store(generation, Ordering::Release);
        attempt.arm();
        drop(host_state);
        drop(state);
        drop(configuration);
        drop(access);
        if let Some(handle) = deferred {
            let _ = handle.handle.await;
        }
        Ok(())
    }

    async fn publish(
        &self,
        common: &CustomReplicatorHost,
        generation: u64,
        current: crate::replicator::ReplicaSetConfiguration,
        authority: AdmittedAuthority,
    ) -> Result<()> {
        let _commit = common.configuration_commit.lock().await;
        #[cfg(all(test, feature = "testing"))]
        self.pause_publication().await;
        let mut state = common.state.write().await;
        let mut configuration = common.configuration.write().await;
        common.ensure_configuration_generation(generation)?;
        state.authority = Some(authority);
        *configuration = Some(current);
        Ok(())
    }

    #[cfg(all(test, feature = "testing"))]
    pub(in super::super) fn set_publication_gate(&self, gate: AccessEffectAcceptanceGate) {
        *self.publication_gate.lock().unwrap() = Some(gate);
    }

    #[cfg(all(test, feature = "testing"))]
    pub(in super::super) fn set_restoration_gate(&self, gate: AccessEffectAcceptanceGate) {
        *self.restoration_gate.lock().unwrap() = Some(gate);
    }

    #[cfg(all(test, feature = "testing"))]
    async fn pause_publication(&self) {
        let gate = self.publication_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
    }

    #[cfg(all(test, feature = "testing"))]
    pub(in super::super) async fn pause_restoration(&self) {
        let gate = self.restoration_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
    }
}

/// Short-lived guard that owns one independent custom-authority attempt.
pub(in super::super) struct CustomAuthorityAttempt<'a> {
    host: Weak<RuntimeHost>,
    containment: &'a CustomAuthorityContainment,
    entered: Arc<AtomicBool>,
    owned: bool,
    complete: bool,
}

impl CustomAuthorityAttempt<'_> {
    fn arm(&self) {
        self.entered.store(true, Ordering::Release);
    }

    pub(in super::super) fn validate(&self) -> Result<()> {
        let host = self.host.upgrade().ok_or(RuntimeError::Closed)?;
        if host.aborted.load(Ordering::Acquire)
            || host.closed.load(Ordering::Acquire)
            || self.containment.invalidated.load(Ordering::Acquire)
        {
            return Err(RuntimeError::OperationCancelled);
        }
        Ok(())
    }

    pub(in super::super) fn complete(mut self) {
        if self.owned {
            self.entered.store(false, Ordering::Release);
        }
        self.complete = true;
    }
}

impl Drop for CustomAuthorityAttempt<'_> {
    fn drop(&mut self) {
        if !self.complete
            && self.owned
            && self.entered.load(Ordering::Acquire)
            && let Some(host) = self.host.upgrade()
        {
            host.abort();
        }
    }
}
