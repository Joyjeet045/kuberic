//! Intent-before-effect runtime integration.

use std::sync::Arc;

use async_trait::async_trait;
use kuberic_runtime_internal::effects::{RuntimeEffect, RuntimeEffectResult};

use crate::hosting::PodRuntime;
use crate::store::{AgentStore, BeginEffect};
use crate::{AgentError, Result};

#[async_trait]
pub trait RuntimeEffectExecutor: Send + Sync {
    async fn apply_runtime_effect(&self, effect: RuntimeEffect) -> Result<RuntimeEffectResult>;

    async fn cancel_configuration_work(&self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl RuntimeEffectExecutor for PodRuntime {
    async fn apply_runtime_effect(&self, effect: RuntimeEffect) -> Result<RuntimeEffectResult> {
        Ok(self.apply_effect(effect).await?)
    }

    async fn cancel_configuration_work(&self) -> Result<()> {
        Ok(PodRuntime::cancel_configuration_work(self).await?)
    }
}

pub struct RuntimeAdapter<S, E> {
    store: Arc<S>,
    executor: Arc<E>,
}

impl<S, E> RuntimeAdapter<S, E>
where
    S: AgentStore,
    E: RuntimeEffectExecutor,
{
    pub fn new(store: Arc<S>, executor: Arc<E>) -> Self {
        Self { store, executor }
    }

    pub fn store(&self) -> &Arc<S> {
        &self.store
    }

    pub async fn execute(&self, effect: RuntimeEffect) -> Result<RuntimeEffectResult> {
        match self.store.begin_effect(&effect).await? {
            BeginEffect::Completed(result) => Ok(*result),
            BeginEffect::Execute(effect) | BeginEffect::Pending(effect) => {
                let result = match self.executor.apply_runtime_effect(effect.clone()).await {
                    Ok(result) => result,
                    Err(
                        error @ AgentError::Runtime(
                            kuberic_runtime::RuntimeError::OperationCancelled
                            | kuberic_runtime::RuntimeError::ReplicaRemoved(_),
                        ),
                    ) => {
                        self.store.cancel_effect(&effect).await?;
                        return Err(error);
                    }
                    Err(error) => return Err(error),
                };
                require_matching_result(&effect, &result)?;
                self.store.mark_effect_applied(&effect).await?;
                self.store.complete_effect(&result).await?;
                Ok(result)
            }
        }
    }

    pub async fn resume_pending(&self) -> Result<Option<RuntimeEffectResult>> {
        let state = self.store.load_state().await?;
        let Some(pending) = state.pending_effect else {
            return Ok(state.retained_result.map(|retained| retained.result));
        };
        self.execute(pending.effect).await.map(Some)
    }

    pub async fn cancel_configuration_work(&self) -> Result<()> {
        self.executor.cancel_configuration_work().await
    }
}

pub fn require_matching_result(effect: &RuntimeEffect, result: &RuntimeEffectResult) -> Result<()> {
    if effect.operation_id != result.operation_id || effect.sequence != result.sequence {
        return Err(AgentError::EffectConflict(
            "runtime returned a result for a different durable effect".into(),
        ));
    }
    Ok(())
}
