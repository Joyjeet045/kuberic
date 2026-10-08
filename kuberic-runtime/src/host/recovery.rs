//! Caller-independent lifecycle recovery and restart-effect inspection.

use std::sync::Arc;

#[cfg(test)]
use crate::effects::{RuntimeEffect, RuntimeEffectResult};
use crate::protocol::types::AccessStatus;

use crate::RuntimeError;
use crate::host::Result;
#[cfg(all(test, feature = "testing"))]
use crate::host::hosting::AccessEffectAcceptanceGate;
use crate::host::hosting::{OwnedRecoveryTask, RecoveryOwnerRuntime};
#[cfg(test)]
use crate::host::observation::RecoveryObservation;
#[cfg(test)]
use crate::host::runtime_adapter::{RuntimeAdapter, RuntimeEffectExecutor};
#[cfg(test)]
use crate::host::state::RetainedResult;
use crate::host::store::AgentStore;
use tokio::sync::{Mutex, oneshot, watch};

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecoveryEligibility(serde_json::Value);

fn recovery_eligibility(state: &crate::host::state::AgentState) -> RecoveryEligibility {
    RecoveryEligibility(serde_json::json!({
        "identity": &state.identity,
        "highest_epoch": state.highest_epoch,
        "previous_configuration": &state.previous_configuration,
        "current_configuration": &state.current_configuration,
        "role": state.role,
        "read_status": state.read_status,
        "write_status": state.write_status,
        "pending_effect": &state.pending_effect,
        "reconfiguration": &state.reconfiguration,
        "scale_up_evidence": &state.scale_up_evidence,
        "prepared_secondary_removal": &state.prepared_secondary_removal,
        "accepted_secondary_removal": &state.accepted_secondary_removal,
        "retired_authority": &state.retired_authority,
        "prepared_switchover": &state.prepared_switchover,
        "retired_switchover": &state.retired_switchover,
        "preparation_retirement": &state.preparation_retirement,
    }))
}

#[cfg(test)]
pub(crate) fn same_recovery_eligibility(
    left: &crate::host::state::AgentState,
    right: &crate::host::state::AgentState,
) -> bool {
    recovery_eligibility(left) == recovery_eligibility(right)
}

pub(crate) struct RecoveryOwner<S> {
    runtime: RecoveryOwnerRuntime,
    store: Arc<S>,
    admission_lock: Arc<Mutex<()>>,
    diagnostics: Option<OwnedRecoveryTask<()>>,
    #[cfg(all(test, feature = "testing"))]
    selection_gate: Option<AccessEffectAcceptanceGate>,
}

#[derive(Clone, Copy)]
pub(crate) struct RecoverySchedule {
    retry: bool,
    custom_tick: bool,
}

impl RecoverySchedule {
    fn wait_for_timer(self) -> bool {
        self.retry || self.custom_tick
    }
}

impl<S: AgentStore> RecoveryOwner<S> {
    #[cfg(test)]
    pub(crate) fn new(runtime: RecoveryOwnerRuntime, store: Arc<S>) -> Self {
        Self {
            runtime,
            store,
            admission_lock: Arc::new(Mutex::new(())),
            diagnostics: None,
            #[cfg(all(test, feature = "testing"))]
            selection_gate: None,
        }
    }

    pub(crate) fn with_admission_lock(
        runtime: RecoveryOwnerRuntime,
        store: Arc<S>,
        admission_lock: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            runtime,
            store,
            admission_lock,
            diagnostics: None,
            #[cfg(all(test, feature = "testing"))]
            selection_gate: None,
        }
    }

    #[cfg(all(test, feature = "testing"))]
    #[allow(dead_code)]
    pub(crate) fn pause_after_selection(&mut self, gate: AccessEffectAcceptanceGate) {
        self.selection_gate = Some(gate);
    }

    async fn maintain_diagnostics(&mut self) {
        if let Some(diagnostics) = self.diagnostics.as_mut() {
            match diagnostics.try_recv() {
                Ok(result) => {
                    if let Err(error) = result {
                        tracing::warn!(%error, "background lifecycle observation retrying");
                    }
                    self.diagnostics = None;
                }
                Err(oneshot::error::TryRecvError::Empty) => return,
                Err(oneshot::error::TryRecvError::Closed) => {
                    self.diagnostics = None;
                }
            }
        }
        if self.diagnostics.is_none() {
            self.diagnostics = Some(self.runtime.start_diagnostics().await);
        }
    }

    fn cancel_diagnostics(&mut self) {
        if let Some(diagnostics) = self.diagnostics.take() {
            diagnostics.abort();
        }
    }

    async fn reconcile_access(&self, read: AccessStatus, write: AccessStatus) -> crate::Result<()> {
        let mut attempt = self.runtime.start_access_reconciliation(read, write).await;
        attempt.wait().await
    }

    pub(crate) async fn advance(&mut self) -> Result<RecoverySchedule> {
        let durable = self.store.load_state().await?;
        if durable.pending_effect.is_some() || durable.reconfiguration.is_some() {
            self.cancel_diagnostics();
            return Ok(RecoverySchedule {
                retry: true,
                custom_tick: false,
            });
        }
        let mut retry = false;
        let durable_eligibility = recovery_eligibility(&durable);
        {
            let admission_lock = self.admission_lock.clone();
            let _admission = admission_lock.lock().await;
            let eligible = self.store.load_state().await?;
            if recovery_eligibility(&eligible) != durable_eligibility
                || eligible.pending_effect.is_some()
                || eligible.reconfiguration.is_some()
            {
                return Ok(RecoverySchedule {
                    retry: false,
                    custom_tick: false,
                });
            }
            let observation = self.runtime.observation().await;
            if observation.host.read_status != eligible.read_status
                || observation.host.write_status != eligible.write_status
            {
                self.cancel_diagnostics();
                let selected = self.store.load_state().await?;
                let selected_eligibility = recovery_eligibility(&selected);
                if selected_eligibility != recovery_eligibility(&eligible)
                    || selected.pending_effect.is_some()
                    || selected.reconfiguration.is_some()
                {
                    return Ok(RecoverySchedule {
                        retry: false,
                        custom_tick: false,
                    });
                }
                #[cfg(all(test, feature = "testing"))]
                if let Some(gate) = self.selection_gate.take() {
                    gate.entered.notify_waiters();
                    gate.release.notified().await;
                }
                let confirmed = self.store.load_state().await?;
                if recovery_eligibility(&confirmed) != selected_eligibility
                    || confirmed.pending_effect.is_some()
                    || confirmed.reconfiguration.is_some()
                {
                    return Ok(RecoverySchedule {
                        retry: false,
                        custom_tick: false,
                    });
                }
                match self
                    .reconcile_access(confirmed.read_status, confirmed.write_status)
                    .await
                {
                    Ok(()) | Err(RuntimeError::NotOpen | RuntimeError::Closed) => {}
                    Err(
                        RuntimeError::ReconfigurationPending | RuntimeError::OperationCancelled,
                    ) => {
                        retry = true;
                    }
                    Err(error) => return Err(error.into()),
                }
                let current = self.store.load_state().await?;
                if recovery_eligibility(&current) != selected_eligibility {
                    let (read, write) =
                        if current.pending_effect.is_some() || current.reconfiguration.is_some() {
                            (
                                AccessStatus::ReconfigurationPending,
                                AccessStatus::ReconfigurationPending,
                            )
                        } else {
                            (current.read_status, current.write_status)
                        };
                    let _ = self.reconcile_access(read, write).await;
                }
            }
        }
        self.maintain_diagnostics().await;
        let observation = self.runtime.observation().await;
        Ok(RecoverySchedule {
            retry,
            custom_tick: observation.host.open && !observation.host.engine_required,
        })
    }

    pub(crate) async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow_and_update() {
                self.cancel_diagnostics();
                return;
            }
            let observed = self.runtime.revision();
            let schedule = tokio::select! {
                result = self.advance() => {
                    match result {
                        Ok(schedule) => schedule,
                        Err(error) => {
                            tracing::warn!(%error, "background lifecycle recovery retrying");
                            RecoverySchedule {
                                retry: true,
                                custom_tick: false,
                            }
                        }
                    }
                }
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow_and_update() {
                        self.cancel_diagnostics();
                        return;
                    }
                    continue;
                }
            };
            if schedule.wait_for_timer() {
                tokio::select! {
                    result = shutdown.changed() => {
                        if result.is_err() || *shutdown.borrow_and_update() {
                            self.cancel_diagnostics();
                            return;
                        }
                    }
                    _ = self.runtime.wait_for_change(observed) => {}
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
                }
            } else {
                tokio::select! {
                    result = shutdown.changed() => {
                        if result.is_err() || *shutdown.borrow_and_update() {
                            self.cancel_diagnostics();
                            return;
                        }
                    }
                    _ = self.runtime.wait_for_change(observed) => {}
                }
            }
        }
    }
}

pub(crate) struct PartitionReportOwner<S> {
    runtime: RecoveryOwnerRuntime,
    store: Arc<S>,
    persisted_revision: Option<u64>,
}

impl<S: AgentStore> PartitionReportOwner<S> {
    pub(crate) fn new(runtime: RecoveryOwnerRuntime, store: Arc<S>) -> Self {
        Self {
            runtime,
            store,
            persisted_revision: None,
        }
    }

    pub(crate) async fn advance(&mut self) -> Result<()> {
        let partition = self.runtime.partition_report().await;
        if self.persisted_revision != Some(partition.revision) {
            self.store
                .record_partition_reports(partition.load_metrics, partition.reported_fault)
                .await?;
            self.persisted_revision = Some(partition.revision);
        }
        Ok(())
    }

    pub(crate) async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow_and_update() {
                return;
            }
            let observed = self.runtime.revision();
            tokio::select! {
                result = self.advance() => {
                    if let Err(error) = result {
                        tracing::warn!(%error, "partition observation persistence retrying");
                    }
                }
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow_and_update() {
                        return;
                    }
                }
            }
            tokio::select! {
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow_and_update() {
                        return;
                    }
                }
                _ = self.runtime.wait_for_change(observed) => {}
                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
            }
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecoveryDecision {
    Idle,
    Reissue(Box<RuntimeEffect>),
    ReturnRetained(Box<RetainedResult>),
}

#[cfg(test)]
pub(crate) async fn inspect_recovery<S: AgentStore>(
    store: &S,
    runtime: &RecoveryObservation,
) -> Result<RecoveryDecision> {
    if runtime.write_status == AccessStatus::Granted {
        return Err(crate::host::HostError::DurableEffectConflict(
            "runtime must start write-closed before recovery".into(),
        ));
    }
    let state = store.load_state().await?;
    if let Some(pending) = state.pending_effect {
        return Ok(RecoveryDecision::Reissue(Box::new(pending.effect)));
    }
    if let Some(retained) = state.retained_result {
        return Ok(RecoveryDecision::ReturnRetained(Box::new(retained)));
    }
    Ok(RecoveryDecision::Idle)
}

#[cfg(test)]
pub(crate) async fn recover_pending<S, E>(
    adapter: &RuntimeAdapter<S, E>,
    runtime: &RecoveryObservation,
) -> Result<Option<RuntimeEffectResult>>
where
    S: AgentStore,
    E: RuntimeEffectExecutor,
{
    match inspect_recovery(adapter.store().as_ref(), runtime).await? {
        RecoveryDecision::Idle => Ok(None),
        RecoveryDecision::ReturnRetained(retained) => Ok(Some(retained.result)),
        RecoveryDecision::Reissue(effect) => adapter.execute(*effect).await.map(Some),
    }
}
