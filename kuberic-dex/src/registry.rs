use std::{collections::BTreeMap, future::Future, marker::PhantomData, pin::Pin, sync::Arc};

use thiserror::Error;

use crate::{
    ActivityCallError, ActivityFailure, ActivityName, ActivityObservation, ActivityOptions,
    AttemptId, CheckpointError, CheckpointStore, DurableActivity, DurableHost, ExactBytes,
    HostOutcome, LogicalActivityId, decode_activity_input, encode_activity_result,
};

type HandlerFuture = Pin<Box<dyn Future<Output = Result<ExactBytes, ActivityHandlerError>> + Send>>;
type ErasedHandler = dyn Fn(ActivityContext, ExactBytes) -> HandlerFuture + Send + Sync;
pub type ScopedHandlerFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ExactBytes, ActivityHandlerError>> + Send + 'a>>;
pub type ScopedTypedHandlerFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ActivityHandlerError>> + Send + 'a>>;

trait ScopedErasedHandler<S>: Send + Sync {
    fn invoke<'a>(
        &'a self,
        state: &'a mut S,
        context: ActivityContext,
        input: ExactBytes,
    ) -> ScopedHandlerFuture<'a>;
}

struct TypedScopedHandler<A, H> {
    handler: H,
    activity: PhantomData<fn() -> A>,
}

impl<S, A, H> ScopedErasedHandler<S> for TypedScopedHandler<A, H>
where
    A: DurableActivity + Send + Sync + 'static,
    A::Input: Send + 'static,
    A::Output: Send + 'static,
    H: for<'a> Fn(
            &'a mut S,
            ActivityContext,
            A::Input,
        ) -> Pin<
            Box<dyn Future<Output = Result<A::Output, ActivityHandlerError>> + Send + 'a>,
        > + Send
        + Sync,
{
    fn invoke<'a>(
        &'a self,
        state: &'a mut S,
        context: ActivityContext,
        input: ExactBytes,
    ) -> ScopedHandlerFuture<'a> {
        match decode_activity_input::<A>(&input) {
            Ok(input) => {
                let future = (self.handler)(state, context, input);
                Box::pin(async move {
                    let output = future.await?;
                    encode_activity_result::<A>(&output).map_err(ActivityHandlerError::Codec)
                })
            }
            Err(error) => Box::pin(async move { Err(ActivityHandlerError::Codec(error)) }),
        }
    }
}

/// Embedding-runtime timer used to cancel an in-flight activity invocation.
///
/// The durable kernel owns timeout semantics but does not embed a concrete
/// async runtime. Tokio, async-std, or another host runtime supplies this
/// small adapter.
pub trait ActivityTimeoutRuntime: Send + Sync {
    fn invoke<'a>(
        &'a self,
        timeout_millis: u64,
        invocation: Pin<
            Box<dyn Future<Output = Result<ExactBytes, ActivityHandlerError>> + Send + 'a>,
        >,
    ) -> Pin<Box<dyn Future<Output = Result<ExactBytes, ActivityHandlerError>> + Send + 'a>>;
}

struct RegistryEntry {
    handler: Arc<ErasedHandler>,
    max_input_bytes: u64,
    max_result_bytes: u64,
}

struct ScopedRegistryEntry<S> {
    handler: Arc<dyn ScopedErasedHandler<S>>,
    max_input_bytes: u64,
    max_result_bytes: u64,
}

/// Runtime-owned metadata for one physical invocation of a logical activity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActivityContext {
    logical_id: LogicalActivityId,
    attempt_id: AttemptId,
    attempt_ordinal: u32,
    retry_not_before_unix_millis: Option<i64>,
    dispatch_authorized: bool,
}

impl ActivityContext {
    pub fn new(
        logical_id: LogicalActivityId,
        attempt_id: AttemptId,
        attempt_ordinal: u32,
        retry_not_before_unix_millis: Option<i64>,
    ) -> Self {
        Self {
            logical_id,
            attempt_id,
            attempt_ordinal,
            retry_not_before_unix_millis,
            dispatch_authorized: true,
        }
    }

    pub fn recovery(
        logical_id: LogicalActivityId,
        attempt_id: AttemptId,
        attempt_ordinal: u32,
        retry_not_before_unix_millis: Option<i64>,
    ) -> Self {
        Self {
            logical_id,
            attempt_id,
            attempt_ordinal,
            retry_not_before_unix_millis,
            dispatch_authorized: false,
        }
    }

    pub const fn activity_instance_id(&self) -> &LogicalActivityId {
        &self.logical_id
    }

    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    pub const fn attempt_ordinal(&self) -> u32 {
        self.attempt_ordinal
    }

    pub const fn options(&self) -> &ActivityOptions {
        self.logical_id.options()
    }

    pub const fn action_deadline_unix_millis(&self) -> Option<i64> {
        self.options().action_deadline_unix_millis()
    }

    pub const fn retry_not_before_unix_millis(&self) -> Option<i64> {
        self.retry_not_before_unix_millis
    }

    pub const fn attempt_timeout_millis(&self) -> Option<u64> {
        self.options().attempt_timeout_millis()
    }

    pub const fn dispatch_authorized(&self) -> bool {
        self.dispatch_authorized
    }
}

/// Handler failures are classified before retry policy is applied.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ActivityHandlerError {
    #[error("activity returned a retryable application failure")]
    Retryable(ExactBytes),
    #[error("activity returned a terminal application failure")]
    Terminal(ExactBytes),
    #[error("activity invocation timed out")]
    TimedOut,
    #[error("activity is waiting until {0} unix milliseconds")]
    WaitUntil(i64),
    #[error("activity codec failed: {0}")]
    Codec(ActivityCallError),
}

/// Framework-owned result of invoking one ordinary activity handler.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ActivityInvocationOutcome {
    Completed(ExactBytes),
    Retryable(ExactBytes),
    Failed(ActivityFailure),
    WaitUntil(i64),
    Rejected(ActivityCallError),
}

/// Shared ordinary-handler invocation semantics for embedded runtimes.
#[derive(Clone, Default)]
pub struct ActivityInvocationRuntime {
    timeout_runtime: Option<Arc<dyn ActivityTimeoutRuntime>>,
}

impl ActivityInvocationRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_timeout_runtime(mut self, runtime: impl ActivityTimeoutRuntime + 'static) -> Self {
        self.timeout_runtime = Some(Arc::new(runtime));
        self
    }

    pub async fn invoke<'a>(
        &self,
        context: ActivityContext,
        now_unix_millis: i64,
        invocation: ScopedHandlerFuture<'a>,
    ) -> ActivityInvocationOutcome {
        let action_deadline = context.action_deadline_unix_millis();
        if context.dispatch_authorized()
            && action_deadline.is_some_and(|deadline| now_unix_millis >= deadline)
        {
            return ActivityInvocationOutcome::Failed(ActivityFailure::ActionDeadlineExceeded);
        }
        let action_timeout = context
            .dispatch_authorized()
            .then_some(action_deadline)
            .flatten()
            .and_then(|deadline| deadline.checked_sub(now_unix_millis))
            .and_then(|remaining| u64::try_from(remaining).ok());
        let attempt_timeout = context.attempt_timeout_millis();
        let (effective_timeout, action_deadline_wins) = match (action_timeout, attempt_timeout) {
            (Some(action), Some(attempt)) if action <= attempt => (Some(action), true),
            (Some(_), Some(attempt)) => (Some(attempt), false),
            (Some(action), None) => (Some(action), true),
            (None, Some(attempt)) => (Some(attempt), false),
            (None, None) => (None, false),
        };
        let result = match effective_timeout {
            Some(timeout_millis) => {
                let Some(runtime) = &self.timeout_runtime else {
                    return ActivityInvocationOutcome::Rejected(ActivityCallError::Handler(
                        "activity timeout requires an embedding timeout runtime".to_string(),
                    ));
                };
                match runtime.invoke(timeout_millis, invocation).await {
                    Err(ActivityHandlerError::TimedOut) if action_deadline_wins => {
                        return ActivityInvocationOutcome::Failed(
                            ActivityFailure::ActionDeadlineExceeded,
                        );
                    }
                    result => result,
                }
            }
            None => invocation.await,
        };
        match result {
            Ok(result) => ActivityInvocationOutcome::Completed(result),
            Err(ActivityHandlerError::Retryable(failure))
                if context.attempt_ordinal() < context.options().max_attempts() =>
            {
                ActivityInvocationOutcome::Retryable(failure)
            }
            Err(ActivityHandlerError::Retryable(failure))
            | Err(ActivityHandlerError::Terminal(failure)) => {
                ActivityInvocationOutcome::Failed(ActivityFailure::Application(failure))
            }
            Err(ActivityHandlerError::TimedOut) => {
                ActivityInvocationOutcome::Failed(ActivityFailure::TimedOut)
            }
            Err(ActivityHandlerError::WaitUntil(wake_at_unix_millis)) => {
                let wake_at_unix_millis = ActivityWakeups {
                    action_deadline_unix_millis: context
                        .dispatch_authorized()
                        .then_some(action_deadline)
                        .flatten(),
                    handler_wait_unix_millis: Some(wake_at_unix_millis),
                    ..ActivityWakeups::default()
                }
                .earliest()
                .expect("handler wait is present");
                ActivityInvocationOutcome::WaitUntil(wake_at_unix_millis)
            }
            Err(ActivityHandlerError::Codec(error)) => ActivityInvocationOutcome::Rejected(error),
        }
    }
}

/// Absolute wakeup candidates returned to the reconciler-hosted runner.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ActivityWakeups {
    pub action_deadline_unix_millis: Option<i64>,
    pub retry_not_before_unix_millis: Option<i64>,
    pub handler_wait_unix_millis: Option<i64>,
    pub attempt_timeout_unix_millis: Option<i64>,
}

impl ActivityWakeups {
    pub fn earliest(self) -> Option<i64> {
        [
            self.action_deadline_unix_millis,
            self.retry_not_before_unix_millis,
            self.handler_wait_unix_millis,
            self.attempt_timeout_unix_millis,
        ]
        .into_iter()
        .flatten()
        .min()
    }
}

impl ActivityHandlerError {
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable(_))
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ActivityRegistryError {
    #[error("activity {0} is not registered")]
    NotRegistered(ActivityName),
    #[error("activity {0} is registered more than once")]
    DuplicateRegistration(ActivityName),
    #[error(transparent)]
    Contract(#[from] ActivityCallError),
}

/// Builder for one immutable in-process activity registry.
#[derive(Default)]
pub struct ActivityRegistryBuilder {
    handlers: BTreeMap<ActivityName, RegistryEntry>,
    error: Option<ActivityRegistryError>,
}

/// Builder for handlers that borrow embedding-runtime state for one invocation.
pub struct ScopedActivityRegistryBuilder<S> {
    handlers: BTreeMap<ActivityName, ScopedRegistryEntry<S>>,
    error: Option<ActivityRegistryError>,
}

impl<S> Default for ScopedActivityRegistryBuilder<S> {
    fn default() -> Self {
        Self {
            handlers: BTreeMap::new(),
            error: None,
        }
    }
}

impl<S> ScopedActivityRegistryBuilder<S> {
    pub fn register<A, H>(mut self, name: &str, handler: H) -> Self
    where
        A: DurableActivity + Send + Sync + 'static,
        A::Input: Send + 'static,
        A::Output: Send + 'static,
        H: for<'a> Fn(
                &'a mut S,
                ActivityContext,
                A::Input,
            ) -> Pin<
                Box<dyn Future<Output = Result<A::Output, ActivityHandlerError>> + Send + 'a>,
            > + Send
            + Sync
            + 'static,
    {
        if self.error.is_some() {
            return self;
        }
        if name != A::NAME {
            self.error = Some(ActivityRegistryError::Contract(
                ActivityCallError::NameMismatch {
                    registered: name.to_owned(),
                    contract: A::NAME.to_owned(),
                },
            ));
            return self;
        }
        let activity_name = match ActivityName::new(name, A::VERSION) {
            Ok(name) => name,
            Err(_) => {
                self.error = Some(ActivityRegistryError::Contract(
                    ActivityCallError::EmptyName,
                ));
                return self;
            }
        };
        if self.handlers.contains_key(&activity_name) {
            self.error = Some(ActivityRegistryError::DuplicateRegistration(activity_name));
            return self;
        }
        self.handlers.insert(
            activity_name,
            ScopedRegistryEntry {
                handler: Arc::new(TypedScopedHandler::<A, H> {
                    handler,
                    activity: PhantomData,
                }),
                max_input_bytes: A::MAX_INPUT_BYTES,
                max_result_bytes: A::MAX_RESULT_BYTES,
            },
        );
        self
    }

    pub fn build(self) -> Result<ScopedActivityRegistry<S>, ActivityRegistryError> {
        if let Some(error) = self.error {
            return Err(error);
        }
        Ok(ScopedActivityRegistry::<S> {
            handlers: self.handlers,
        })
    }
}

impl ActivityRegistryBuilder {
    /// Register an ordinary typed async activity handler.
    ///
    /// This mirrors Duroxide's `register_typed` shape while retaining DEX's
    /// versioned activity contract and payload bounds.
    pub fn register_typed<A, H, F>(self, name: &str, handler: H) -> Self
    where
        A: DurableActivity + Send + Sync + 'static,
        A::Input: Send + 'static,
        A::Output: Send + 'static,
        H: Fn(ActivityContext, A::Input) -> F + Send + Sync + 'static,
        F: Future<Output = Result<A::Output, ActivityHandlerError>> + Send + 'static,
    {
        self.register::<A, H, F>(name, handler)
    }

    pub fn register<A, H, F>(self, name: &str, handler: H) -> Self
    where
        A: DurableActivity + Send + Sync + 'static,
        A::Input: Send + 'static,
        A::Output: Send + 'static,
        H: Fn(ActivityContext, A::Input) -> F + Send + Sync + 'static,
        F: Future<Output = Result<A::Output, ActivityHandlerError>> + Send + 'static,
    {
        self.register_inner::<A, H, F>(name, handler)
    }

    fn register_inner<A, H, F>(self, name: &str, handler: H) -> Self
    where
        A: DurableActivity + Send + Sync + 'static,
        A::Input: Send + 'static,
        A::Output: Send + 'static,
        H: Fn(ActivityContext, A::Input) -> F + Send + Sync + 'static,
        F: Future<Output = Result<A::Output, ActivityHandlerError>> + Send + 'static,
    {
        let erased = move |context: ActivityContext, input: ExactBytes| {
            let decoded = decode_activity_input::<A>(&input);
            match decoded {
                Ok(input) => {
                    let future = handler(context, input);
                    Box::pin(async move {
                        let output = future.await?;
                        encode_activity_result::<A>(&output).map_err(ActivityHandlerError::Codec)
                    }) as HandlerFuture
                }
                Err(error) => Box::pin(async move { Err(ActivityHandlerError::Codec(error)) })
                    as HandlerFuture,
            }
        };
        self.register_entry::<A>(name, Arc::new(erased))
    }

    fn register_entry<A>(mut self, name: &str, handler: Arc<ErasedHandler>) -> Self
    where
        A: DurableActivity,
    {
        if self.error.is_some() {
            return self;
        }
        if name != A::NAME {
            self.error = Some(ActivityRegistryError::Contract(
                ActivityCallError::NameMismatch {
                    registered: name.to_owned(),
                    contract: A::NAME.to_owned(),
                },
            ));
            return self;
        }
        let activity_name = match ActivityName::new(name, A::VERSION) {
            Ok(name) => name,
            Err(_) => {
                self.error = Some(ActivityRegistryError::Contract(
                    ActivityCallError::EmptyName,
                ));
                return self;
            }
        };
        if self.handlers.contains_key(&activity_name) {
            self.error = Some(ActivityRegistryError::DuplicateRegistration(activity_name));
            return self;
        }
        self.handlers.insert(
            activity_name,
            RegistryEntry {
                handler,
                max_input_bytes: A::MAX_INPUT_BYTES,
                max_result_bytes: A::MAX_RESULT_BYTES,
            },
        );
        self
    }

    pub fn build(self) -> Result<ActivityRegistry, ActivityRegistryError> {
        if let Some(error) = self.error {
            return Err(error);
        }
        Ok(ActivityRegistry {
            handlers: self.handlers,
        })
    }
}

/// Immutable registry used by the reconciler-hosted runner.
pub struct ActivityRegistry {
    handlers: BTreeMap<ActivityName, RegistryEntry>,
}

/// Immutable registry whose handlers borrow state supplied by the embedding
/// runtime for the duration of one invocation.
pub struct ScopedActivityRegistry<S> {
    handlers: BTreeMap<ActivityName, ScopedRegistryEntry<S>>,
}

impl<S> ScopedActivityRegistry<S> {
    pub fn builder() -> ScopedActivityRegistryBuilder<S> {
        ScopedActivityRegistryBuilder::<S>::default()
    }

    pub fn contains(&self, name: &ActivityName) -> bool {
        self.handlers.contains_key(name)
    }

    pub async fn invoke_classified(
        &self,
        state: &mut S,
        context: ActivityContext,
        input: ExactBytes,
    ) -> Result<ExactBytes, ActivityHandlerError> {
        let name = context.activity_instance_id().name().clone();
        let Some(entry) = self.handlers.get(&name) else {
            return Err(ActivityHandlerError::Codec(
                ActivityCallError::UnregisteredActivity(name.to_string()),
            ));
        };
        validate_contract_bounds(
            entry.max_input_bytes,
            entry.max_result_bytes,
            &context,
            &input,
        )
        .map_err(ActivityHandlerError::Codec)?;
        entry.handler.invoke(state, context, input).await
    }
}

impl ActivityRegistry {
    pub fn builder() -> ActivityRegistryBuilder {
        ActivityRegistryBuilder::default()
    }

    pub fn contains(&self, name: &ActivityName) -> bool {
        self.handlers.contains_key(name)
    }

    pub async fn invoke(
        &self,
        context: ActivityContext,
        input: ExactBytes,
    ) -> Result<ExactBytes, ActivityRegistryError> {
        let name = context.activity_instance_id().name().clone();
        let entry = self
            .handlers
            .get(&name)
            .ok_or(ActivityRegistryError::NotRegistered(name))?;
        validate_registered_contract(entry, &context, &input)
            .map_err(ActivityRegistryError::Contract)?;
        (entry.handler)(context, input)
            .await
            .map_err(|error| match error {
                ActivityHandlerError::Codec(error) => ActivityRegistryError::Contract(error),
                other => {
                    ActivityRegistryError::Contract(ActivityCallError::Handler(other.to_string()))
                }
            })
    }

    pub async fn invoke_classified(
        &self,
        context: ActivityContext,
        input: ExactBytes,
    ) -> Result<ExactBytes, ActivityHandlerError> {
        let name = context.activity_instance_id().name().clone();
        let Some(entry) = self.handlers.get(&name) else {
            return Err(ActivityHandlerError::Codec(
                ActivityCallError::UnregisteredActivity(name.to_string()),
            ));
        };
        validate_registered_contract(entry, &context, &input)
            .map_err(ActivityHandlerError::Codec)?;
        (entry.handler)(context, input).await
    }

    /// Validate an invocation against an immutable typed contract registry.
    pub fn validate_invocation(
        &self,
        context: &ActivityContext,
        input: &ExactBytes,
    ) -> Result<(), ActivityRegistryError> {
        let name = context.activity_instance_id().name().clone();
        let entry = self
            .handlers
            .get(&name)
            .ok_or(ActivityRegistryError::NotRegistered(name))?;
        validate_registered_contract(entry, context, input).map_err(ActivityRegistryError::Contract)
    }
}

fn validate_registered_contract(
    entry: &RegistryEntry,
    context: &ActivityContext,
    input: &ExactBytes,
) -> Result<(), ActivityCallError> {
    validate_contract_bounds(
        entry.max_input_bytes,
        entry.max_result_bytes,
        context,
        input,
    )
}

fn validate_contract_bounds(
    max_input_bytes: u64,
    max_result_bytes: u64,
    context: &ActivityContext,
    input: &ExactBytes,
) -> Result<(), ActivityCallError> {
    let input_len = u64::try_from(input.as_slice().len()).unwrap_or(u64::MAX);
    let recorded_input_bound = context.activity_instance_id().spec().max_input_bytes();
    if recorded_input_bound != max_input_bytes {
        return Err(ActivityCallError::Handler(format!(
            "registered input bound {} differs from recorded {}",
            max_input_bytes, recorded_input_bound
        )));
    }
    if input_len > max_input_bytes {
        return Err(ActivityCallError::InputTooLarge {
            actual_bytes: input_len,
            max_bytes: max_input_bytes,
        });
    }
    let recorded_result_bound = context.activity_instance_id().max_result_bytes();
    if recorded_result_bound != max_result_bytes {
        return Err(ActivityCallError::Handler(format!(
            "registered result bound {} differs from recorded {}",
            max_result_bytes, recorded_result_bound
        )));
    }
    Ok(())
}

/// One bounded ordinary-activity runner step.
pub struct ActivityRunner<S> {
    host: DurableHost<S>,
    registry: ActivityRegistry,
    invocation_runtime: ActivityInvocationRuntime,
}

impl<S: CheckpointStore> ActivityRunner<S> {
    pub fn new(host: DurableHost<S>, registry: ActivityRegistry) -> Self {
        Self {
            host,
            registry,
            invocation_runtime: ActivityInvocationRuntime::new(),
        }
    }

    pub fn with_timeout_runtime(mut self, runtime: impl ActivityTimeoutRuntime + 'static) -> Self {
        self.invocation_runtime = self.invocation_runtime.with_timeout_runtime(runtime);
        self
    }

    pub const fn host(&self) -> &DurableHost<S> {
        &self.host
    }

    pub async fn run_once<W: crate::Workflow>(
        &mut self,
        workflow: &W,
        execution: crate::ExecutionSpec,
        now_unix_millis: i64,
    ) -> HostOutcome {
        let outcome = self
            .host
            .turn_and_expose_at(workflow, execution.clone(), now_unix_millis)
            .await;
        if let HostOutcome::Quarantined {
            activity,
            prepared_command: None,
            ..
        } = &outcome
        {
            let retry = self
                .host
                .schedule_retry(&execution, activity, now_unix_millis)
                .await;
            if matches!(
                retry,
                HostOutcome::CheckpointRejected(
                    CheckpointError::ActivityAttemptLimitExceeded { .. }
                )
            ) {
                return self
                    .host
                    .observe_failure(
                        &execution,
                        activity,
                        ActivityFailure::Application(ExactBytes::new(
                            b"lost_result_retry_exhausted",
                        )),
                    )
                    .await;
            }
            return retry;
        }
        let HostOutcome::DispatchPermitted { permit, .. } = outcome else {
            return outcome;
        };
        let activity = permit.activity().clone();
        let attempt_id = permit.attempt_id();
        let stored = match self.host.store().load(execution.execution_id()).await {
            Ok(Some(stored)) => stored,
            Ok(None) => {
                return HostOutcome::CheckpointRejected(
                    CheckpointError::RetryRequiresExposedActivity {
                        sequence: activity.sequence(),
                    },
                );
            }
            Err(error) => {
                return HostOutcome::StoreFailed {
                    operation: crate::StoreOperation::Load,
                    error,
                };
            }
        };
        let payload = match stored
            .checkpoint()
            .decode_and_validate(&execution, self.host.checkpoint_limits())
        {
            Ok(payload) => payload,
            Err(error) => return HostOutcome::CheckpointRejected(error),
        };
        let attempt = payload
            .active_activities()
            .and_then(|activities| activities.last())
            .map(|record| record.attempt())
            .expect("accepted exposure stores its final activity");
        let context = ActivityContext::new(
            activity.clone(),
            attempt_id,
            attempt.ordinal(),
            attempt.retry_not_before_unix_millis(),
        );
        let input = activity.input().clone();
        let invocation = Box::pin(self.registry.invoke_classified(context.clone(), input));
        match self
            .invocation_runtime
            .invoke(context, now_unix_millis, invocation)
            .await
        {
            ActivityInvocationOutcome::Completed(result) => {
                self.host
                    .observe(&execution, ActivityObservation::new(activity, result))
                    .await
            }
            ActivityInvocationOutcome::Retryable(_) => {
                self.host
                    .schedule_retry(&execution, &activity, now_unix_millis)
                    .await
            }
            ActivityInvocationOutcome::Failed(failure) => {
                self.host
                    .observe_failure(&execution, &activity, failure)
                    .await
            }
            ActivityInvocationOutcome::WaitUntil(wake_at_unix_millis) => {
                self.host
                    .defer_wait(&execution, &activity, wake_at_unix_millis)
                    .await
            }
            ActivityInvocationOutcome::Rejected(_) => HostOutcome::CheckpointRejected(
                CheckpointError::PreparedActivityRejected(crate::PreparedActivityError::Validation),
            ),
        }
    }
}
