//! Restart recovery for durable agent effects.

use crate::effects::{RuntimeEffect, RuntimeEffectResult, RuntimeSnapshot};
use crate::protocol::types::AccessStatus;

use crate::host::Result;
use crate::host::runtime_adapter::{RuntimeAdapter, RuntimeEffectExecutor};
use crate::host::state::RetainedResult;
use crate::host::store::AgentStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecoveryDecision {
    Idle,
    Reissue(Box<RuntimeEffect>),
    ReturnRetained(Box<RetainedResult>),
}

pub(crate) async fn inspect_recovery<S: AgentStore>(
    store: &S,
    runtime: &RuntimeSnapshot,
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

pub(crate) async fn recover_pending<S, E>(
    adapter: &RuntimeAdapter<S, E>,
    runtime: &RuntimeSnapshot,
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
