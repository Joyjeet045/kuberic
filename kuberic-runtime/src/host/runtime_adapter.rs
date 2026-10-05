//! Intent-before-effect runtime integration.

use std::sync::Arc;

use crate::RuntimeError;
use crate::effects::{RuntimeEffect, RuntimeEffectResult};
use crate::protocol::types::OperationId;
use async_trait::async_trait;
use tokio::sync::oneshot;

use crate::host::Result;
use crate::host::hosting::PodRuntime;
use crate::host::store::{AgentStore, BeginEffect};

#[doc(hidden)]
pub(crate) struct RuntimeEffectCommit {
    decision: Option<oneshot::Sender<bool>>,
    completion: tokio::task::JoinHandle<Result<()>>,
}

impl RuntimeEffectCommit {
    pub(crate) fn new(
        decision: oneshot::Sender<bool>,
        completion: tokio::task::JoinHandle<Result<()>>,
    ) -> Self {
        Self {
            decision: Some(decision),
            completion,
        }
    }

    async fn finish(mut self, committed: bool) -> Result<()> {
        if let Some(decision) = self.decision.take() {
            let _ = decision.send(committed);
        }
        self.completion
            .await
            .map_err(|error| RuntimeError::Application(error.to_string()))?
    }
}

#[doc(hidden)]
pub(crate) struct RuntimeEffectExecution {
    result: RuntimeEffectResult,
    commit: Option<RuntimeEffectCommit>,
}

impl RuntimeEffectExecution {
    pub(crate) fn completed(result: RuntimeEffectResult) -> Self {
        Self {
            result,
            commit: None,
        }
    }

    pub(crate) fn prepared(result: RuntimeEffectResult, commit: RuntimeEffectCommit) -> Self {
        Self {
            result,
            commit: Some(commit),
        }
    }

    pub(crate) fn result(&self) -> &RuntimeEffectResult {
        &self.result
    }

    pub(crate) async fn accept(mut self) -> Result<RuntimeEffectResult> {
        if let Some(commit) = self.commit.take() {
            commit.finish(true).await?;
        }
        Ok(self.result)
    }

    pub(crate) async fn reject(mut self) -> Result<()> {
        if let Some(commit) = self.commit.take() {
            commit.finish(false).await?;
        }
        Ok(())
    }
}

impl From<RuntimeEffectResult> for RuntimeEffectExecution {
    fn from(result: RuntimeEffectResult) -> Self {
        Self::completed(result)
    }
}

#[async_trait]
pub(crate) trait RuntimeEffectExecutor: Send + Sync {
    async fn apply_runtime_effect(&self, effect: RuntimeEffect) -> Result<RuntimeEffectResult>;

    async fn prepare_runtime_effect(
        &self,
        effect: RuntimeEffect,
    ) -> Result<RuntimeEffectExecution> {
        self.apply_runtime_effect(effect).await.map(Into::into)
    }

    async fn consume_cancelled_build_effect(
        &self,
        _effect: RuntimeEffect,
    ) -> Result<RuntimeEffectResult> {
        Err(crate::host::HostError::DurableEffectConflict(
            "runtime cannot consume a cancelled build effect".into(),
        ))
    }

    async fn cancel_configuration_work(&self) -> Result<()> {
        Ok(())
    }

    async fn cancel_build(&self, _build_id: &OperationId) -> Result<()> {
        Ok(())
    }

    async fn reissue_build(
        &self,
        _build_id: OperationId,
        _target: crate::protocol::types::ReplicaIdentity,
        _replication_address: String,
    ) -> Result<()> {
        Ok(())
    }

    async fn wait_for_build_completion(
        &self,
        _build_id: &OperationId,
        _target: &crate::protocol::types::ReplicaIdentity,
    ) -> Result<()> {
        Err(crate::host::HostError::DurableEffectConflict(
            "runtime cannot observe build completion".into(),
        ))
    }

    async fn observe_build_completion(
        &self,
        _effect: RuntimeEffect,
    ) -> Result<RuntimeEffectResult> {
        Err(crate::host::HostError::DurableEffectConflict(
            "runtime cannot publish build completion".into(),
        ))
    }

    async fn discard_cancelled_build_effect(&self, _effect: &RuntimeEffect) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl RuntimeEffectExecutor for PodRuntime {
    async fn apply_runtime_effect(&self, effect: RuntimeEffect) -> Result<RuntimeEffectResult> {
        Ok(self.apply_effect(effect).await?)
    }

    async fn prepare_runtime_effect(
        &self,
        effect: RuntimeEffect,
    ) -> Result<RuntimeEffectExecution> {
        Ok(self.prepare_effect(effect).await?)
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

    async fn reissue_build(
        &self,
        build_id: OperationId,
        target: crate::protocol::types::ReplicaIdentity,
        replication_address: String,
    ) -> Result<()> {
        Ok(PodRuntime::reissue_outbound_build(self, build_id, target, replication_address).await?)
    }

    async fn wait_for_build_completion(
        &self,
        build_id: &OperationId,
        target: &crate::protocol::types::ReplicaIdentity,
    ) -> Result<()> {
        Ok(PodRuntime::wait_for_build_completion(self, build_id, target).await?)
    }

    async fn observe_build_completion(&self, effect: RuntimeEffect) -> Result<RuntimeEffectResult> {
        Ok(PodRuntime::observe_build_completion(self, effect).await?)
    }

    async fn discard_cancelled_build_effect(&self, effect: &RuntimeEffect) -> Result<()> {
        Ok(PodRuntime::discard_cancelled_build_effect(self, effect).await?)
    }
}

pub(crate) struct RuntimeAdapter<S, E> {
    store: Arc<S>,
    executor: Arc<E>,
}

impl<S, E> RuntimeAdapter<S, E>
where
    S: AgentStore,
    E: RuntimeEffectExecutor,
{
    pub(crate) fn new(store: Arc<S>, executor: Arc<E>) -> Self {
        Self { store, executor }
    }

    #[cfg(test)]
    pub(crate) fn store(&self) -> &Arc<S> {
        &self.store
    }

    pub(crate) async fn execute(&self, effect: RuntimeEffect) -> Result<RuntimeEffectResult> {
        match self.store.begin_effect(&effect).await? {
            BeginEffect::Completed(result) => {
                if let crate::effects::RuntimeEffectAction::BuildReplica {
                    build_id,
                    target,
                    replication_address,
                } = effect.action
                {
                    self.executor
                        .reissue_build(build_id, target, replication_address)
                        .await?;
                }
                Ok(*result)
            }
            BeginEffect::Execute(effect) | BeginEffect::Pending(effect) => {
                let execution = match self.executor.prepare_runtime_effect(effect.clone()).await {
                    Ok(execution) => execution,
                    Err(
                        error @ crate::host::HostError::Runtime(
                            crate::RuntimeError::OperationCancelled
                            | crate::RuntimeError::ReplicaRemoved(_),
                        ),
                    ) => {
                        let state = self.store.load_state().await?;
                        let removal = matches!(
                            effect.action,
                            crate::effects::RuntimeEffectAction::PrepareSecondaryRemoval { .. }
                                | crate::effects::RuntimeEffectAction::RetireReplica(_)
                        ) || state.reconfiguration.as_ref().is_some_and(|r| {
                            r.command.transition_kind
                                == crate::protocol::types::TransitionKind::SecondaryScaleDown
                        });
                        let abandoned_build = match &effect.action {
                            crate::effects::RuntimeEffectAction::BuildReplica {
                                build_id, ..
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
                let result = execution.result().clone();
                if let Err(error) = require_matching_result(&effect, &result) {
                    execution.reject().await?;
                    return Err(error);
                }
                let pending_build_completion = match &effect.action {
                    crate::effects::RuntimeEffectAction::BuildReplica {
                        build_id, target, ..
                    } => !result.postcondition.builds.iter().any(|build| {
                        &build.authority.build_id == build_id
                            && &build.authority.target == target
                            && build.completed
                    }),
                    _ => false,
                };
                if pending_build_completion {
                    let result = execution.accept().await?;
                    return Box::pin(self.await_build_completion(effect, result)).await;
                }
                if let Err(error) = self.store.mark_effect_applied(&effect).await {
                    execution.reject().await?;
                    return Err(error);
                }
                if let Err(error) = self.store.complete_effect(&result).await {
                    execution.reject().await?;
                    return Err(error);
                }
                execution.accept().await
            }
        }
    }

    async fn await_build_completion(
        &self,
        effect: RuntimeEffect,
        dispatched: RuntimeEffectResult,
    ) -> Result<RuntimeEffectResult> {
        let (build_id, target) = match &effect.action {
            crate::effects::RuntimeEffectAction::BuildReplica {
                build_id, target, ..
            } => (build_id, target),
            _ => {
                return Err(crate::host::HostError::DurableEffectConflict(
                    "build completion wait requires a build effect".into(),
                ));
            }
        };
        match self
            .executor
            .wait_for_build_completion(build_id, target)
            .await
        {
            Ok(()) => {
                let completed = self
                    .executor
                    .observe_build_completion(effect.clone())
                    .await?;
                require_matching_result(&effect, &completed)?;
                self.store.mark_effect_applied(&effect).await?;
                self.store.complete_effect(&completed).await?;
                Ok(completed)
            }
            Err(
                error @ crate::host::HostError::Runtime(
                    crate::RuntimeError::OperationCancelled
                    | crate::RuntimeError::ReplicaRemoved(_),
                ),
            ) => {
                let state = self.store.load_state().await?;
                if state.abandoned_builds.contains(build_id) {
                    return Err(error);
                }
                self.executor
                    .discard_cancelled_build_effect(&effect)
                    .await?;
                self.store.cancel_effect(&effect).await?;
                Ok(dispatched)
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn resume_pending(&self) -> Result<Option<RuntimeEffectResult>> {
        let state = self.store.load_state().await?;
        let Some(pending) = state.pending_effect else {
            return Ok(state.retained_result.map(|retained| retained.result));
        };
        self.execute(pending.effect).await.map(Some)
    }

    pub(crate) async fn settle_abandoned_build(
        &self,
        build_id: &OperationId,
    ) -> Result<Option<RuntimeEffectResult>> {
        let state = self.store.load_state().await?;
        if !state.abandoned_builds.contains(build_id) {
            return Err(crate::host::HostError::DurableEffectConflict(
                "build cancellation lacks durable abandonment".into(),
            ));
        }
        let Some(pending) = state.pending_effect else {
            return Ok(None);
        };
        let matching_build = matches!(
            &pending.effect.action,
            crate::effects::RuntimeEffectAction::BuildReplica {
                build_id: pending_build_id,
                ..
            } if pending_build_id == build_id
                && pending.effect.operation_id
                    == OperationId::new(format!("{build_id}:build-replica"))
        );
        let matching_retirement = matches!(
            &pending.effect.action,
            crate::effects::RuntimeEffectAction::RetireBuild(
                pending_build_id
            ) if pending_build_id == build_id
                && pending.effect.operation_id
                    == OperationId::new(format!("{build_id}:retire-abandoned-build"))
        );
        if matching_retirement {
            return Ok(None);
        }
        if !matching_build {
            return Err(crate::host::HostError::DurableEffectConflict(
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

    pub(crate) async fn cancel_configuration_work(&self) -> Result<()> {
        self.executor.cancel_configuration_work().await
    }

    pub(crate) async fn cancel_build(&self, build_id: &OperationId) -> Result<()> {
        self.executor.cancel_build(build_id).await
    }
}

pub(crate) fn require_matching_result(
    effect: &RuntimeEffect,
    result: &RuntimeEffectResult,
) -> Result<()> {
    if effect.operation_id != result.operation_id || effect.sequence != result.sequence {
        return Err(crate::host::HostError::DurableEffectConflict(
            "runtime returned a result for a different durable effect".into(),
        ));
    }
    Ok(())
}
