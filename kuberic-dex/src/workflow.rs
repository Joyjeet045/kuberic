use std::{
    collections::BTreeMap, future::Future, marker::PhantomData, pin::Pin, sync::Arc, task::Poll,
};

use async_trait::async_trait;
use futures::future::poll_fn;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::{
    ActivityFailure, ActivityOptions, ActivityRecord, ActivitySequence, ActivitySpec,
    ActivityState, CompletionClass, EffectMetadata, ExactBytes, ExecutionId, LogicalActivityId,
    Nondeterminism, PreparedCommand, PreparedEffectResolver,
    typed::{
        ActivityCallError, ActivityInvocationError, DurableActivity, PreparedActivityError,
        PreparedActivityResolver, activity_spec, activity_spec_named, canonical_json,
        decode_activity_result, decode_typed_result, typed_activity_spec,
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

/// Failure while encoding or decoding a typed orchestration boundary.
#[derive(Clone, Debug, Deserialize, Eq, Error, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkflowCodecError {
    #[error("workflow input could not be encoded")]
    InputEncoding,
    #[error("workflow input could not be decoded")]
    InputDecoding,
    #[error("workflow output could not be encoded")]
    OutputEncoding,
    #[error("workflow error could not be encoded")]
    ErrorEncoding,
    #[error("terminal workflow output could not be decoded")]
    OutputDecoding,
    #[error("terminal workflow error could not be decoded")]
    ErrorDecoding,
}

/// Typed ordinary-async orchestration contract.
///
/// Implementations receive decoded input and return an ordinary [`Result`], so
/// activity failures can be propagated with `?`. The replay kernel encodes the
/// success or error value into the terminal checkpoint.
#[async_trait]
#[doc(hidden)]
pub trait Orchestration: Sync {
    type Input: Serialize + DeserializeOwned + Send;
    type Output: Serialize + DeserializeOwned + Send;
    type Error: Serialize + DeserializeOwned + Send;

    async fn run(
        &self,
        context: &mut OrchestrationContext<'_>,
        input: Self::Input,
    ) -> Result<Self::Output, Self::Error>;
}

/// Boxed future returned by a typed orchestration registry handler.
pub type OrchestrationFuture<'a, O, E> = Pin<Box<dyn Future<Output = Result<O, E>> + Send + 'a>>;

/// Immutable registry of named orchestration handlers.
pub struct OrchestrationRegistry {
    handlers: BTreeMap<String, Arc<dyn Workflow>>,
}

impl OrchestrationRegistry {
    pub fn builder() -> OrchestrationRegistryBuilder {
        OrchestrationRegistryBuilder::default()
    }

    pub fn get(&self, name: &str) -> Result<&dyn Workflow, OrchestrationRegistryError> {
        self.handlers
            .get(name)
            .map(Arc::as_ref)
            .ok_or_else(|| OrchestrationRegistryError::Unregistered(name.to_owned()))
    }
}

#[derive(Default)]
pub struct OrchestrationRegistryBuilder {
    handlers: BTreeMap<String, Arc<dyn Workflow>>,
    error: Option<OrchestrationRegistryError>,
}

impl OrchestrationRegistryBuilder {
    pub fn register_typed<In, Out, E, H>(mut self, name: &str, handler: H) -> Self
    where
        In: Serialize + DeserializeOwned + Send + 'static,
        Out: Serialize + DeserializeOwned + Send + 'static,
        E: Serialize + DeserializeOwned + Send + 'static,
        H: for<'a, 'history> Fn(
                &'a mut OrchestrationContext<'history>,
                In,
            ) -> OrchestrationFuture<'a, Out, E>
            + Send
            + Sync
            + 'static,
    {
        if self.error.is_some() {
            return self;
        }
        if name.is_empty() {
            self.error = Some(OrchestrationRegistryError::EmptyName);
            return self;
        }
        if self.handlers.contains_key(name) {
            self.error = Some(OrchestrationRegistryError::DuplicateRegistration(
                name.to_owned(),
            ));
            return self;
        }
        self.handlers.insert(
            name.to_owned(),
            Arc::new(TypedOrchestration::<In, Out, E, H> {
                handler,
                types: PhantomData,
            }),
        );
        self
    }

    pub fn build(self) -> Result<OrchestrationRegistry, OrchestrationRegistryError> {
        if let Some(error) = self.error {
            return Err(error);
        }
        Ok(OrchestrationRegistry {
            handlers: self.handlers,
        })
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum OrchestrationRegistryError {
    #[error("orchestration name must not be empty")]
    EmptyName,
    #[error("orchestration {0:?} is registered more than once")]
    DuplicateRegistration(String),
    #[error("orchestration {0:?} is not registered")]
    Unregistered(String),
}

struct TypedOrchestration<In, Out, E, H> {
    handler: H,
    types: PhantomData<fn(In) -> (Out, E)>,
}

#[async_trait]
impl<In, Out, E, H> Workflow for TypedOrchestration<In, Out, E, H>
where
    In: Serialize + DeserializeOwned + Send + 'static,
    Out: Serialize + DeserializeOwned + Send + 'static,
    E: Serialize + DeserializeOwned + Send + 'static,
    H: for<'a, 'history> Fn(
            &'a mut OrchestrationContext<'history>,
            In,
        ) -> OrchestrationFuture<'a, Out, E>
        + Send
        + Sync
        + 'static,
{
    async fn run(&self, context: &mut WorkflowContext<'_>, input: ExactBytes) -> TerminalOutcome {
        let input = match serde_json::from_slice::<In>(input.as_slice()) {
            Ok(input) => input,
            Err(_) => return codec_failure(WorkflowCodecError::InputDecoding),
        };
        match (self.handler)(context, input).await {
            Ok(output) => match canonical_json(&output) {
                Ok(output) => TerminalOutcome::succeeded(output),
                Err(_) => codec_failure(WorkflowCodecError::OutputEncoding),
            },
            Err(error) => match canonical_json(&WorkflowFailure::Application(error)) {
                Ok(error) => TerminalOutcome::failed(error),
                Err(_) => codec_failure(WorkflowCodecError::ErrorEncoding),
            },
        }
    }
}

/// Encode typed orchestration input using DEX's canonical JSON codec.
pub fn encode_workflow_input<T: Serialize>(input: &T) -> Result<ExactBytes, WorkflowCodecError> {
    canonical_json(input)
        .map(ExactBytes::new)
        .map_err(|_| WorkflowCodecError::InputEncoding)
}

/// Decode a typed orchestration's terminal success or error value.
pub fn decode_workflow_result<O: DeserializeOwned, E: DeserializeOwned>(
    outcome: &TerminalOutcome,
) -> Result<Result<O, E>, WorkflowCodecError> {
    match outcome {
        TerminalOutcome::Succeeded(payload) => serde_json::from_slice(payload.as_slice())
            .map(Ok)
            .map_err(|_| WorkflowCodecError::OutputDecoding),
        TerminalOutcome::Failed(payload) => {
            match serde_json::from_slice::<WorkflowFailure<E>>(payload.as_slice())
                .map_err(|_| WorkflowCodecError::ErrorDecoding)?
            {
                WorkflowFailure::Application(error) => Ok(Err(error)),
                WorkflowFailure::Codec(error) => Err(error),
            }
        }
    }
}

/// Decode the terminal result using an orchestration's associated types.
#[doc(hidden)]
pub fn decode_orchestration_result<O: Orchestration>(
    outcome: &TerminalOutcome,
) -> Result<Result<O::Output, O::Error>, WorkflowCodecError> {
    decode_workflow_result(outcome)
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
enum WorkflowFailure<E> {
    Application(E),
    Codec(WorkflowCodecError),
}

/// Low-level exact-byte workflow contract used by the replay kernel.
///
/// Application code should normally implement [`Orchestration`] instead.
#[async_trait]
pub trait Workflow: Sync {
    async fn run(&self, context: &mut WorkflowContext<'_>, input: ExactBytes) -> TerminalOutcome;
}

/// Duroxide-style name for the workflow replay context.
pub type OrchestrationContext<'history> = WorkflowContext<'history>;

#[async_trait]
impl<T> Workflow for T
where
    T: Orchestration,
{
    async fn run(&self, context: &mut WorkflowContext<'_>, input: ExactBytes) -> TerminalOutcome {
        let input = match serde_json::from_slice::<T::Input>(input.as_slice()) {
            Ok(input) => input,
            Err(_) => return codec_failure(WorkflowCodecError::InputDecoding),
        };
        match Orchestration::run(self, context, input).await {
            Ok(output) => match canonical_json(&output) {
                Ok(output) => TerminalOutcome::succeeded(output),
                Err(_) => codec_failure(WorkflowCodecError::OutputEncoding),
            },
            Err(error) => match canonical_json(&WorkflowFailure::Application(error)) {
                Ok(error) => TerminalOutcome::failed(error),
                Err(_) => codec_failure(WorkflowCodecError::ErrorEncoding),
            },
        }
    }
}

fn codec_failure(error: WorkflowCodecError) -> TerminalOutcome {
    let payload = canonical_json(&WorkflowFailure::<()>::Codec(error))
        .expect("serializing the framework-owned workflow codec error must succeed");
    TerminalOutcome::failed(payload)
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

    #[doc(hidden)]
    pub async fn activity(&mut self, spec: ActivitySpec) -> ExactBytes {
        match poll_fn(|_| self.poll_activity(&spec, None)).await {
            Ok(result) => result,
            Err(failure) => failure
                .payload()
                .cloned()
                .unwrap_or_else(ExactBytes::default),
        }
    }

    /// Legacy typed call using the low-level domain-result failure model.
    #[doc(hidden)]
    pub async fn call<A: DurableActivity>(
        &mut self,
        input: A::Input,
    ) -> Result<A::Output, ActivityCallError> {
        let spec = activity_spec::<A>(&input)?;
        let result = self.activity(spec).await;
        decode_activity_result::<A>(&result)
    }

    /// Schedule a named typed activity with default options.
    pub async fn schedule_activity_typed<In, Out>(
        &mut self,
        name: &str,
        input: &In,
    ) -> Result<Out, ActivityInvocationError>
    where
        In: Serialize,
        Out: DeserializeOwned,
    {
        self.schedule_activity_typed_with_options(name, input, ActivityOptions::default())
            .await
    }

    /// Schedule a named typed activity with replay-matched options.
    pub async fn schedule_activity_typed_with_options<In, Out>(
        &mut self,
        name: &str,
        input: &In,
        options: ActivityOptions,
    ) -> Result<Out, ActivityInvocationError>
    where
        In: Serialize,
        Out: DeserializeOwned,
    {
        let spec = typed_activity_spec(name, input, options)?;
        let result = poll_fn(|_| self.poll_activity(&spec, None))
            .await
            .map_err(Self::activity_invocation_error)?;
        decode_typed_result(&result, crate::MAX_ACTIVITY_RESULT_BYTES)
            .map_err(ActivityInvocationError::Call)
    }

    /// Schedule a typed activity using an internal activity contract.
    #[doc(hidden)]
    pub async fn schedule_activity<A: DurableActivity>(
        &mut self,
        input: &A::Input,
    ) -> Result<A::Output, ActivityInvocationError> {
        self.schedule_activity_with_options::<A>(input, ActivityOptions::default())
            .await
    }

    /// Schedule a typed activity using an internal contract and replay-matched options.
    #[doc(hidden)]
    pub async fn schedule_activity_with_options<A: DurableActivity>(
        &mut self,
        input: &A::Input,
        options: ActivityOptions,
    ) -> Result<A::Output, ActivityInvocationError> {
        self.schedule_activity_contract_with_options::<A>(A::NAME, input, options)
            .await
    }

    /// Schedule an activity using an internal contract.
    #[doc(hidden)]
    pub async fn schedule_activity_contract_with_options<A: DurableActivity>(
        &mut self,
        name: &str,
        input: &A::Input,
        options: ActivityOptions,
    ) -> Result<A::Output, ActivityInvocationError> {
        let spec = activity_spec_named::<A>(name, input, options)?;
        let result = if let Some(metadata) = A::strict_effect_metadata() {
            poll_fn(|_| self.poll_effect(&spec, metadata)).await
        } else {
            poll_fn(|_| self.poll_activity(&spec, A::completion_class())).await
        }
        .map_err(Self::activity_invocation_error)?;
        decode_activity_result::<A>(&result).map_err(ActivityInvocationError::Call)
    }

    fn activity_invocation_error(failure: ActivityFailure) -> ActivityInvocationError {
        match failure {
            ActivityFailure::Application(error) => ActivityInvocationError::Application(error),
            ActivityFailure::TimedOut => ActivityInvocationError::TimedOut,
            ActivityFailure::ActionDeadlineExceeded => {
                ActivityInvocationError::ActionDeadlineExceeded
            }
        }
    }

    pub(crate) const fn cursor(&self) -> usize {
        self.cursor
    }

    /// Return the immutable execution identity being replayed.
    pub const fn execution_id(&self) -> ExecutionId {
        self.execution_id
    }

    fn poll_activity(
        &mut self,
        spec: &ActivitySpec,
        completion_class: Option<CompletionClass>,
    ) -> Poll<Result<ExactBytes, ActivityFailure>> {
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
                completion_class,
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
        if record.completion_class() != completion_class {
            self.decision = Some(ContextDecision::Nondeterminism(
                Nondeterminism::CompletionClassMismatch {
                    sequence,
                    recorded: record.completion_class(),
                    requested: completion_class.unwrap_or(CompletionClass::ExternalEffect),
                },
            ));
            return Poll::Pending;
        }

        match record.state() {
            ActivityState::Completed { result } => {
                self.cursor += 1;
                Poll::Ready(match record.failure() {
                    Some(failure) => Err(failure.clone()),
                    None => Ok(result.clone()),
                })
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

    fn poll_effect(
        &mut self,
        spec: &ActivitySpec,
        metadata: EffectMetadata,
    ) -> Poll<Result<ExactBytes, ActivityFailure>> {
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
                Poll::Ready(match record.failure() {
                    Some(failure) => Err(failure.clone()),
                    None => Ok(result.clone()),
                })
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
        completion_class: Option<CompletionClass>,
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
