use std::task::Poll;

use async_trait::async_trait;
use futures::future::poll_fn;
use serde::{Deserialize, Serialize};

use crate::{
    ActivityRecord, ActivitySequence, ActivitySpec, ActivityState, CompletionClass, DurableEffect,
    EffectActivity, EffectCallError, EffectMetadata, ExactBytes, ExecutionId, LogicalActivityId,
    Nondeterminism, PreparedCommand, PreparedEffectResolver,
    typed::{
        ActivityCallError, DurableActivity, PreparedActivityError, PreparedActivityResolver,
        activity_spec, decode_activity_result,
    },
};

/// Exact terminal result of one workflow execution.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "status",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum TerminalOutcome {
    Succeeded(ExactBytes),
    Failed(ExactBytes),
}

impl TerminalOutcome {
    pub fn succeeded(payload: impl Into<ExactBytes>) -> Self {
        Self::Succeeded(payload.into())
    }

    pub fn failed(payload: impl Into<ExactBytes>) -> Self {
        Self::Failed(payload.into())
    }

    pub const fn payload(&self) -> &ExactBytes {
        match self {
            Self::Succeeded(payload) | Self::Failed(payload) => payload,
        }
    }
}

/// Provisional ordinary-async workflow authoring surface.
#[async_trait]
pub trait Workflow: Sync {
    async fn run(&self, context: &mut WorkflowContext<'_>, input: ExactBytes) -> TerminalOutcome;
}

/// Linear replay context. `activity` is its only workflow-body operation.
pub struct WorkflowContext<'history> {
    execution_id: ExecutionId,
    history: &'history [ActivityRecord],
    resolver: &'history dyn PreparedActivityResolver,
    effect_resolver: Option<&'history dyn PreparedEffectResolver>,
    cursor: usize,
    pub(crate) decision: Option<ContextDecision>,
}

impl<'history> WorkflowContext<'history> {
    pub(crate) fn new(
        execution_id: ExecutionId,
        history: &'history [ActivityRecord],
        resolver: &'history dyn PreparedActivityResolver,
    ) -> Self {
        Self {
            execution_id,
            history,
            resolver,
            effect_resolver: None,
            cursor: 0,
            decision: None,
        }
    }

    pub(crate) fn new_with_effects(
        execution_id: ExecutionId,
        history: &'history [ActivityRecord],
        resolver: &'history dyn PreparedActivityResolver,
        effect_resolver: &'history dyn PreparedEffectResolver,
    ) -> Self {
        Self {
            execution_id,
            history,
            resolver,
            effect_resolver: Some(effect_resolver),
            cursor: 0,
            decision: None,
        }
    }

    pub async fn activity(&mut self, spec: ActivitySpec) -> ExactBytes {
        poll_fn(|_| self.poll_activity(&spec)).await
    }

    /// Invoke a versioned activity with typed, bounded input and output.
    pub async fn call<A: DurableActivity>(
        &mut self,
        input: A::Input,
    ) -> Result<A::Output, ActivityCallError> {
        let spec = activity_spec::<A>(&input)?;
        let result = self.activity(spec).await;
        decode_activity_result::<A>(&result)
    }

    /// Invoke a durable effect and expose only its typed applied value or
    /// bounded typed failure to workflow code.
    pub async fn call_effect<E: DurableEffect>(
        &mut self,
        request: E::Request,
    ) -> Result<E::Output, EffectCallError> {
        let spec = activity_spec::<EffectActivity<E>>(&request)?;
        let result = poll_fn(|_| self.poll_effect(&spec, EffectMetadata::of::<E>())).await;
        decode_activity_result::<EffectActivity<E>>(&result)?
            .into_workflow_result(E::MAX_ERROR_MESSAGE_BYTES)
    }

    pub(crate) const fn cursor(&self) -> usize {
        self.cursor
    }

    /// Return the immutable execution identity being replayed.
    pub const fn execution_id(&self) -> ExecutionId {
        self.execution_id
    }

    fn poll_activity(&mut self, spec: &ActivitySpec) -> Poll<ExactBytes> {
        if self.decision.is_some() {
            return Poll::Pending;
        }

        let sequence = ActivitySequence::new(
            u64::try_from(self.cursor).expect("validated history length fits in u64"),
        );
        let record = self.history.get(self.cursor);
        let prepared = match self
            .resolver
            .resolve(spec, record.map(ActivityRecord::spec))
        {
            Ok(prepared) => prepared,
            Err(error) => {
                self.decision = Some(ContextDecision::PreparationRejected(error));
                return Poll::Pending;
            }
        };
        let requested_id = LogicalActivityId::new(self.execution_id, sequence, prepared.clone());

        let Some(record) = record else {
            self.decision = Some(ContextDecision::Schedule {
                sequence,
                spec: prepared,
                logical_id: requested_id,
            });
            return Poll::Pending;
        };

        if record.spec() != &prepared {
            self.decision = Some(ContextDecision::Nondeterminism(
                Nondeterminism::ActivityMismatch {
                    sequence,
                    recorded: record.spec().clone(),
                    requested: prepared,
                },
            ));
            return Poll::Pending;
        }

        match record.state() {
            ActivityState::Completed { result } => {
                self.cursor += 1;
                Poll::Ready(result.clone())
            }
            state @ (ActivityState::Scheduled | ActivityState::DispatchExposed { .. }) => {
                self.decision = Some(ContextDecision::ExistingPending {
                    logical_id: requested_id,
                    state: state.clone(),
                });
                Poll::Pending
            }
        }
    }

    fn poll_effect(&mut self, spec: &ActivitySpec, metadata: EffectMetadata) -> Poll<ExactBytes> {
        if self.decision.is_some() {
            return Poll::Pending;
        }
        let sequence = ActivitySequence::new(
            u64::try_from(self.cursor).expect("validated history length fits in u64"),
        );
        let record = self.history.get(self.cursor);
        if let Some(record) = record
            && record.spec() != spec
        {
            self.decision = Some(ContextDecision::Nondeterminism(
                Nondeterminism::ActivityMismatch {
                    sequence,
                    recorded: record.spec().clone(),
                    requested: spec.clone(),
                },
            ));
            return Poll::Pending;
        }
        let Some(resolver) = self.effect_resolver else {
            self.decision = Some(ContextDecision::PreparationRejected(
                PreparedActivityError::Validation,
            ));
            return Poll::Pending;
        };
        let prepared = match resolver.resolve(
            self.execution_id,
            spec,
            metadata,
            record.and_then(ActivityRecord::prepared_command),
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                self.decision = Some(ContextDecision::PreparationRejected(error));
                return Poll::Pending;
            }
        };
        if prepared.max_bytes() != metadata.max_command_bytes() {
            self.decision = Some(ContextDecision::PreparationRejected(
                PreparedActivityError::CommandBoundMismatch {
                    actual_bytes: prepared.max_bytes(),
                    max_bytes: metadata.max_command_bytes(),
                },
            ));
            return Poll::Pending;
        }
        let actual_command_bytes =
            u64::try_from(prepared.bytes().as_slice().len()).unwrap_or(u64::MAX);
        if actual_command_bytes > metadata.max_command_bytes() {
            self.decision = Some(ContextDecision::PreparationRejected(
                PreparedActivityError::CommandTooLarge {
                    actual_bytes: actual_command_bytes,
                    max_bytes: metadata.max_command_bytes(),
                },
            ));
            return Poll::Pending;
        }
        let requested_id = LogicalActivityId::new(self.execution_id, sequence, spec.clone());
        let Some(record) = record else {
            self.decision = Some(ContextDecision::ScheduleEffect {
                sequence,
                spec: spec.clone(),
                logical_id: requested_id,
                prepared_command: prepared,
                completion_class: metadata.completion_class(),
            });
            return Poll::Pending;
        };
        if record.prepared_command() != Some(&prepared) {
            self.decision = Some(ContextDecision::Nondeterminism(
                Nondeterminism::PreparedCommandMismatch {
                    sequence,
                    recorded: record.prepared_command().cloned(),
                    requested: prepared,
                },
            ));
            return Poll::Pending;
        }
        if record.completion_class() != Some(metadata.completion_class()) {
            self.decision = Some(ContextDecision::Nondeterminism(
                Nondeterminism::CompletionClassMismatch {
                    sequence,
                    recorded: record.completion_class(),
                    requested: metadata.completion_class(),
                },
            ));
            return Poll::Pending;
        }
        match record.state() {
            ActivityState::Completed { result } => {
                self.cursor += 1;
                Poll::Ready(result.clone())
            }
            state @ (ActivityState::Scheduled | ActivityState::DispatchExposed { .. }) => {
                self.decision = Some(ContextDecision::ExistingPending {
                    logical_id: requested_id,
                    state: state.clone(),
                });
                Poll::Pending
            }
        }
    }
}

pub(crate) enum ContextDecision {
    Schedule {
        sequence: ActivitySequence,
        spec: ActivitySpec,
        logical_id: LogicalActivityId,
    },
    ScheduleEffect {
        sequence: ActivitySequence,
        spec: ActivitySpec,
        logical_id: LogicalActivityId,
        prepared_command: PreparedCommand,
        completion_class: CompletionClass,
    },
    ExistingPending {
        logical_id: LogicalActivityId,
        state: ActivityState,
    },
    Nondeterminism(Nondeterminism),
    PreparationRejected(PreparedActivityError),
}
