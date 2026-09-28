//! Intent-before-effect runtime integration.

use std::sync::Arc;

use async_trait::async_trait;
use kuberic_protocol::types::OperationId;
use kuberic_runtime_internal::effects::{RuntimeEffect, RuntimeEffectResult};

use crate::hosting::PodRuntime;
use crate::store::{AgentStore, BeginEffect};
use crate::{AgentError, Result};

#[async_trait]
pub trait RuntimeEffectExecutor: Send + Sync {
    async fn apply_runtime_effect(&self, effect: RuntimeEffect) -> Result<RuntimeEffectResult>;

    async fn consume_cancelled_build_effect(
        &self,
        _effect: RuntimeEffect,
    ) -> Result<RuntimeEffectResult> {
        Err(AgentError::EffectConflict(
            "runtime cannot consume a cancelled build effect".into(),
        ))
    }

    async fn cancel_configuration_work(&self) -> Result<()> {
        Ok(())
    }

    async fn cancel_build(&self, _build_id: &OperationId) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl RuntimeEffectExecutor for PodRuntime {
    async fn apply_runtime_effect(&self, effect: RuntimeEffect) -> Result<RuntimeEffectResult> {
        Ok(self.apply_effect(effect).await?)
    }

    async fn consume_cancelled_build_effect(
        &self,
        effect: RuntimeEffect,
    ) -> Result<RuntimeEffectResult> {
        Ok(PodRuntime::consume_cancelled_build_effect(self, effect).await?)
    }

    async fn cancel_configuration_work(&self) -> Result<()> {
        Ok(PodRuntime::cancel_configuration_work(self).await?)
    }

    async fn cancel_build(&self, build_id: &OperationId) -> Result<()> {
        Ok(PodRuntime::cancel_outbound_build(self, build_id).await?)
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
                        let state = self.store.load_state().await?;
                        let removal = matches!(effect.action,
                            kuberic_runtime_internal::effects::RuntimeEffectAction::PrepareSecondaryRemoval { .. }
                                | kuberic_runtime_internal::effects::RuntimeEffectAction::RetireReplica(_))
                            || state.reconfiguration.as_ref().is_some_and(|r|
                                r.command.transition_kind == kuberic_protocol::types::TransitionKind::SecondaryScaleDown);
                        let abandoned_build = match &effect.action {
                            kuberic_runtime_internal::effects::RuntimeEffectAction::BuildReplica {
                                build_id,
                                ..
                            } => state.abandoned_builds.contains(build_id),
                            _ => false,
                        };
                        if !removal && !abandoned_build {
                            self.store.cancel_effect(&effect).await?;
                        }
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

    pub async fn settle_abandoned_build(
        &self,
        build_id: &OperationId,
    ) -> Result<Option<RuntimeEffectResult>> {
        let state = self.store.load_state().await?;
        if !state.abandoned_builds.contains(build_id) {
            return Err(AgentError::EffectConflict(
                "build cancellation lacks durable abandonment".into(),
            ));
        }
        let Some(pending) = state.pending_effect else {
            return Ok(None);
        };
        let matching_build = matches!(
            &pending.effect.action,
            kuberic_runtime_internal::effects::RuntimeEffectAction::BuildReplica {
                build_id: pending_build_id,
                ..
            } if pending_build_id == build_id
                && pending.effect.operation_id
                    == OperationId::new(format!("{build_id}:build-replica"))
        );
        let matching_retirement = matches!(
            &pending.effect.action,
            kuberic_runtime_internal::effects::RuntimeEffectAction::RetireBuild(
                pending_build_id
            ) if pending_build_id == build_id
                && pending.effect.operation_id
                    == OperationId::new(format!("{build_id}:retire-abandoned-build"))
        );
        if matching_retirement {
            return Ok(None);
        }
        if !matching_build {
            return Err(AgentError::EffectConflict(
                "build cancellation would consume unrelated pending work".into(),
            ));
        }
        let effect = pending.effect;
        let result = self
            .executor
            .consume_cancelled_build_effect(effect.clone())
            .await?;
        require_matching_result(&effect, &result)?;
        self.store.mark_effect_applied(&effect).await?;
        self.store.complete_effect(&result).await?;
        Ok(Some(result))
    }

    pub async fn cancel_configuration_work(&self) -> Result<()> {
        self.executor.cancel_configuration_work().await
    }

    pub async fn cancel_build(&self, build_id: &OperationId) -> Result<()> {
        self.executor.cancel_build(build_id).await
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
