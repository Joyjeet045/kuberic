//! Restart recovery for durable agent effects.

use kuberic_protocol::types::AccessStatus;
use kuberic_runtime_internal::effects::{RuntimeEffect, RuntimeEffectResult, RuntimeSnapshot};

use crate::runtime_adapter::{RuntimeAdapter, RuntimeEffectExecutor};
use crate::state::RetainedResult;
use crate::store::AgentStore;
use crate::{AgentError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryDecision {
    Idle,
    Reissue(RuntimeEffect),
    ReturnRetained(Box<RetainedResult>),
}

pub async fn inspect_recovery<S: AgentStore>(
    store: &S,
    runtime: &RuntimeSnapshot,
) -> Result<RecoveryDecision> {
    if runtime.write_status == AccessStatus::Granted {
        return Err(AgentError::EffectConflict(
            "runtime must start write-closed before recovery".into(),
        ));
    }
    let state = store.load_state().await?;
    if let Some(pending) = state.pending_effect {
        return Ok(RecoveryDecision::Reissue(pending.effect));
    }
    if let Some(retained) = state.retained_result {
        return Ok(RecoveryDecision::ReturnRetained(Box::new(retained)));
    }
    Ok(RecoveryDecision::Idle)
}

pub async fn recover_pending<S, E>(
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
        RecoveryDecision::Reissue(effect) => adapter.execute(effect).await.map(Some),
    }
}
