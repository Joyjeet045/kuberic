use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use kuberic_durable_execution::{
    ActivityContext, ActivityFailure, ActivityHandlerError, ActivityInvocationOutcome,
    ActivityInvocationRuntime, ActivityName, ActivityOptions, ActivityRegistry,
    ActivityRegistryError, ActivityRunner, ActivitySequence, ActivitySpec, ActivityTimeoutRuntime,
    ActivityWakeups, AttemptId, CasOutcome, CheckpointEnvelope, CheckpointError, CheckpointLimits,
    CheckpointStore, DurableActivity, DurableHost, Evaluation, ExactBytes, ExecutionId,
    ExecutionSpec, HostEpoch, HostOutcome, InMemoryCheckpointStore, InMemoryFault,
    LogicalActivityId, Nondeterminism, ReloadReason, ScopedActivityRegistry, StorageRevision,
    StoreError, StoredCheckpoint, TerminalCheckpointStatus, TerminalOutcome, Workflow,
    WorkflowContext, evaluate,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct EchoInput {
    value: String,
}

struct Echo;

struct TokioTimeoutRuntime;

impl ActivityTimeoutRuntime for TokioTimeoutRuntime {
    fn invoke<'a>(
        &'a self,
        timeout_millis: u64,
        invocation: Pin<
            Box<dyn Future<Output = Result<ExactBytes, ActivityHandlerError>> + Send + 'a>,
        >,
    ) -> Pin<Box<dyn Future<Output = Result<ExactBytes, ActivityHandlerError>> + Send + 'a>> {
        Box::pin(async move {
            tokio::time::timeout(std::time::Duration::from_millis(timeout_millis), invocation)
                .await
                .unwrap_or(Err(ActivityHandlerError::TimedOut))
        })
    }
}

impl DurableActivity for Echo {
    type Input = EchoInput;
    type Output = String;

    const NAME: &'static str = "Echo";
    const VERSION: u32 = 1;
    const MAX_INPUT_BYTES: u64 = 128;
    const MAX_RESULT_BYTES: u64 = 128;
}

fn execution() -> ExecutionSpec {
    ExecutionSpec::new(ExecutionId::from_bytes([7; 16]), ExactBytes::default(), 128)
}

fn limits() -> CheckpointLimits {
    CheckpointLimits::new(8, 64 * 1024, 8 * 1024).unwrap()
}

fn logical(options: ActivityOptions) -> LogicalActivityId {
    LogicalActivityId::new(
        execution().execution_id(),
        ActivitySequence::new(0),
        ActivitySpec::with_bounds_and_options(
            ActivityName::new(Echo::NAME, Echo::VERSION).unwrap(),
            ExactBytes::new(br#"{"value":"hello"}"#),
            Echo::MAX_INPUT_BYTES,
            Echo::MAX_RESULT_BYTES,
            options,
        ),
    )
}

#[derive(Clone, Default)]
struct CountingStore {
    inner: InMemoryCheckpointStore,
    accepted: Arc<AtomicUsize>,
}

#[async_trait]
impl CheckpointStore for CountingStore {
    async fn load(
        &self,
        execution_id: ExecutionId,
    ) -> Result<Option<StoredCheckpoint>, StoreError> {
        self.inner.load(execution_id).await
    }

    async fn compare_and_swap(
        &self,
        execution_id: ExecutionId,
        expected: Option<StorageRevision>,
        checkpoint: CheckpointEnvelope,
    ) -> Result<CasOutcome, StoreError> {
        let outcome = self
            .inner
            .compare_and_swap(execution_id, expected, checkpoint)
            .await?;
        if matches!(outcome, CasOutcome::Accepted(_)) {
            self.accepted.fetch_add(1, Ordering::SeqCst);
        }
        Ok(outcome)
    }
}

#[tokio::test]
async fn registry_invokes_an_ordinary_send_handler_with_runtime_identity() {
    let registry = ActivityRegistry::builder()
        .register::<Echo, _, _>("Echo", |context, input| async move {
            assert_eq!(context.activity_instance_id().sequence().get(), 0);
            assert_eq!(context.attempt_ordinal(), 2);
            assert_eq!(context.action_deadline_unix_millis(), Some(9_000));
            assert_eq!(context.retry_not_before_unix_millis(), Some(2_000));
            assert_eq!(context.attempt_timeout_millis(), Some(500));
            Ok(format!("{}-{}", input.value, context.attempt_ordinal()))
        })
        .build()
        .unwrap();
    let options = ActivityOptions::new(3, 1_000, Some(9_000), Some(500)).unwrap();
    let context = ActivityContext::new(
        logical(options),
        AttemptId::new(HostEpoch::from_bytes([8; 16]), 2).unwrap(),
        2,
        Some(2_000),
    );

    let result = registry
        .invoke_classified(context, ExactBytes::new(br#"{"value":"hello"}"#))
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<String>(result.as_slice()).unwrap(),
        "hello-2"
    );
}

#[tokio::test]
async fn scoped_registry_owns_handlers_that_borrow_embedding_state() {
    struct State {
        suffix: String,
        calls: usize,
    }

    let registry = ScopedActivityRegistry::<State>::builder()
        .register::<Echo, _>("Echo", |state, context, input| {
            Box::pin(async move {
                state.calls += 1;
                Ok(format!(
                    "{}-{}-{}",
                    input.value,
                    state.suffix,
                    context.attempt_ordinal()
                ))
            })
        })
        .build()
        .unwrap();
    let context = ActivityContext::new(
        logical(ActivityOptions::default()),
        AttemptId::new(HostEpoch::from_bytes([28; 16]), 1).unwrap(),
        1,
        None,
    );
    let mut state = State {
        suffix: "scoped".to_string(),
        calls: 0,
    };

    let result = registry
        .invoke_classified(
            &mut state,
            context,
            ExactBytes::new(br#"{"value":"hello"}"#),
        )
        .await
        .unwrap();

    assert_eq!(state.calls, 1);
    assert_eq!(
        serde_json::from_slice::<String>(result.as_slice()).unwrap(),
        "hello-scoped-1"
    );
}

#[tokio::test]
async fn shared_invocation_runtime_classifies_scoped_retry_exhaustion() {
    let registry = ScopedActivityRegistry::<Vec<(u32, Option<i64>)>>::builder()
        .register::<Echo, _>("Echo", |state, context, _| {
            Box::pin(async move {
                state.push((
                    context.attempt_ordinal(),
                    context.retry_not_before_unix_millis(),
                ));
                Err(ActivityHandlerError::Retryable(ExactBytes::new(
                    b"retry exhausted",
                )))
            })
        })
        .build()
        .unwrap();
    let options = ActivityOptions::new(2, 1_000, None, None).unwrap();
    let context = ActivityContext::new(
        logical(options),
        AttemptId::new(HostEpoch::from_bytes([29; 16]), 2).unwrap(),
        2,
        Some(2_000),
    );
    let mut calls = Vec::new();
    let handler = Box::pin(registry.invoke_classified(
        &mut calls,
        context.clone(),
        ExactBytes::new(br#"{"value":"hello"}"#),
    ));

    let outcome = ActivityInvocationRuntime::new()
        .invoke(context, 2_000, handler)
        .await;

    assert_eq!(calls, vec![(2, Some(2_000))]);
    assert_eq!(
        outcome,
        ActivityInvocationOutcome::Failed(ActivityFailure::Application(ExactBytes::new(
            b"retry exhausted"
        )))
    );
}

#[tokio::test]
async fn shared_invocation_runtime_allows_observation_only_recovery_after_deadline() {
    let registry = ScopedActivityRegistry::<usize>::builder()
        .register::<Echo, _>("Echo", |calls, context, input| {
            Box::pin(async move {
                assert!(!context.dispatch_authorized());
                *calls += 1;
                Ok(input.value)
            })
        })
        .build()
        .unwrap();
    let options = ActivityOptions::new(3, 1_000, Some(1_000), None).unwrap();
    let context = ActivityContext::recovery(
        logical(options),
        AttemptId::new(HostEpoch::from_bytes([30; 16]), 2).unwrap(),
        2,
        Some(900),
    );
    let mut calls = 0;
    let handler = Box::pin(registry.invoke_classified(
        &mut calls,
        context.clone(),
        ExactBytes::new(br#"{"value":"observed"}"#),
    ));

    let outcome = ActivityInvocationRuntime::new()
        .invoke(context, 2_000, handler)
        .await;

    assert_eq!(calls, 1);
    assert_eq!(
        outcome,
        ActivityInvocationOutcome::Completed(ExactBytes::new(br#""observed""#))
    );
}

#[tokio::test]
async fn registry_rejects_recorded_contract_bound_mismatches() {
    let registry = ActivityRegistry::builder()
        .register::<Echo, _, _>("Echo", |_context, input| async move { Ok(input.value) })
        .build()
        .unwrap();
    let wrong = LogicalActivityId::new(
        execution().execution_id(),
        ActivitySequence::new(0),
        ActivitySpec::with_bounds_and_options(
            ActivityName::new("Echo", 1).unwrap(),
            ExactBytes::new(br#"{"value":"hello"}"#),
            Echo::MAX_INPUT_BYTES - 1,
            Echo::MAX_RESULT_BYTES,
            ActivityOptions::default(),
        ),
    );
    let context = ActivityContext::new(
        wrong,
        AttemptId::new(HostEpoch::from_bytes([18; 16]), 1).unwrap(),
        1,
        None,
    );
    assert!(matches!(
        registry
            .invoke_classified(context, ExactBytes::new(br#"{"value":"hello"}"#))
            .await,
        Err(ActivityHandlerError::Codec(_))
    ));
}

#[test]
fn registry_rejects_name_mismatch_and_duplicates() {
    let mismatch = ActivityRegistry::builder()
        .register::<Echo, _, _>("Other", |_context, _input| async { Ok(String::new()) })
        .build();
    assert!(matches!(mismatch, Err(ActivityRegistryError::Contract(_))));

    let duplicate = ActivityRegistry::builder()
        .register::<Echo, _, _>("Echo", |_context, _input| async { Ok(String::new()) })
        .register::<Echo, _, _>("Echo", |_context, _input| async { Ok(String::new()) })
        .build();
    assert!(matches!(
        duplicate,
        Err(ActivityRegistryError::DuplicateRegistration(_))
    ));
}

#[test]
fn only_retryable_application_failures_are_retryable() {
    assert!(ActivityHandlerError::Retryable(ExactBytes::default()).is_retryable());
    assert!(!ActivityHandlerError::Terminal(ExactBytes::default()).is_retryable());
    assert!(!ActivityHandlerError::TimedOut.is_retryable());
    assert!(!ActivityHandlerError::WaitUntil(5).is_retryable());
    assert_eq!(
        ActivityWakeups {
            action_deadline_unix_millis: Some(9_000),
            retry_not_before_unix_millis: Some(2_000),
            handler_wait_unix_millis: Some(3_000),
            attempt_timeout_unix_millis: Some(2_500),
        }
        .earliest(),
        Some(2_000)
    );
}

struct TypedWorkflow {
    options: ActivityOptions,
}

#[async_trait]
impl Workflow for TypedWorkflow {
    async fn run(&self, context: &mut WorkflowContext<'_>, _input: ExactBytes) -> TerminalOutcome {
        match context
            .schedule_activity_typed::<Echo>(
                "Echo",
                &EchoInput {
                    value: "hello".to_owned(),
                },
                self.options,
            )
            .await
        {
            Ok(value) => TerminalOutcome::succeeded(value.into_bytes()),
            Err(error) => TerminalOutcome::failed(error.to_string().into_bytes()),
        }
    }
}

#[test]
fn scheduling_options_are_replay_identity() {
    let first = TypedWorkflow {
        options: ActivityOptions::new(3, 1_000, Some(9_000), Some(500)).unwrap(),
    };
    let Evaluation::Scheduled { checkpoint, .. } = evaluate(&first, &execution(), None, limits())
    else {
        panic!("activity was not scheduled");
    };
    let changed = TypedWorkflow {
        options: ActivityOptions::new(2, 1_000, Some(9_000), Some(500)).unwrap(),
    };
    assert!(matches!(
        evaluate(&changed, &execution(), Some(&checkpoint), limits()),
        Evaluation::Nondeterminism(Nondeterminism::ActivityMismatch { .. })
    ));
}

#[tokio::test]
async fn retry_is_persisted_under_one_logical_activity_and_gates_reexposure() {
    let workflow = TypedWorkflow {
        options: ActivityOptions::new(3, 1_000, Some(9_000), None).unwrap(),
    };
    let store = kuberic_durable_execution::InMemoryCheckpointStore::new();
    let mut host = DurableHost::new(store.clone(), HostEpoch::from_bytes([9; 16]), limits());

    let HostOutcome::DispatchPermitted { permit, .. } =
        host.turn_and_expose_at(&workflow, execution(), 1_000).await
    else {
        panic!("first attempt was not exposed");
    };
    let logical_id = permit.activity().clone();

    let HostOutcome::RetryScheduled {
        retry_not_before_unix_millis,
        ..
    } = host.schedule_retry(&execution(), &logical_id, 1_500).await
    else {
        panic!("retry was not scheduled");
    };
    assert_eq!(retry_not_before_unix_millis, 2_500);
    assert!(matches!(
        host.turn_and_expose_at(&workflow, execution(), 2_000).await,
        HostOutcome::Waiting {
            wake_at_unix_millis: 2_500,
            ..
        }
    ));

    let HostOutcome::DispatchPermitted { permit, .. } =
        host.turn_and_expose_at(&workflow, execution(), 2_500).await
    else {
        panic!("second attempt was not exposed");
    };
    assert_eq!(permit.activity(), &logical_id);
    let stored = store
        .load(execution().execution_id())
        .await
        .unwrap()
        .unwrap();
    let payload = stored
        .checkpoint()
        .decode_and_validate(&execution(), limits())
        .unwrap();
    let record = &payload.active_activities().unwrap()[0];
    assert_eq!(record.attempt().ordinal(), 2);
}

#[tokio::test]
async fn integrated_runner_retries_twice_then_replays_success() {
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let registry = ActivityRegistry::builder()
        .register::<Echo, _, _>("Echo", move |_context, input| {
            let calls = handler_calls.clone();
            async move {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                if call < 2 {
                    Err(ActivityHandlerError::Retryable(ExactBytes::new(b"retry")))
                } else {
                    Ok(input.value)
                }
            }
        })
        .build()
        .unwrap();
    let store = kuberic_durable_execution::InMemoryCheckpointStore::new();
    let host = DurableHost::new(store, HostEpoch::from_bytes([10; 16]), limits());
    let mut runner = ActivityRunner::new(host, registry);
    let workflow = TypedWorkflow {
        options: ActivityOptions::new(3, 100, None, None).unwrap(),
    };

    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_000).await,
        HostOutcome::RetryScheduled {
            retry_not_before_unix_millis: 1_100,
            ..
        }
    ));
    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_100).await,
        HostOutcome::RetryScheduled {
            retry_not_before_unix_millis: 1_300,
            ..
        }
    ));
    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_300).await,
        HostOutcome::ObservationAccepted { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_300).await,
        HostOutcome::WorkflowCompleted {
            checkpoint_status: TerminalCheckpointStatus::Accepted,
            ..
        }
    ));
    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_300).await,
        HostOutcome::WorkflowCompleted {
            checkpoint_status: TerminalCheckpointStatus::Reloaded,
            outcome: TerminalOutcome::Succeeded(_),
            ..
        }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn each_attempt_uses_only_exposure_and_result_boundaries() {
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let registry = ActivityRegistry::builder()
        .register::<Echo, _, _>("Echo", move |_context, input| {
            let call = handler_calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if call == 0 {
                    Err(ActivityHandlerError::Retryable(ExactBytes::new(b"retry")))
                } else {
                    Ok(input.value)
                }
            }
        })
        .build()
        .unwrap();
    let store = CountingStore::default();
    let accepted = store.accepted.clone();
    let host = DurableHost::new(store, HostEpoch::from_bytes([19; 16]), limits());
    let mut runner = ActivityRunner::new(host, registry);
    let workflow = TypedWorkflow {
        options: ActivityOptions::new(3, 10, None, None).unwrap(),
    };

    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_000).await,
        HostOutcome::RetryScheduled { .. }
    ));
    assert_eq!(accepted.load(Ordering::SeqCst), 2);
    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_010).await,
        HostOutcome::ObservationAccepted { .. }
    ));
    assert_eq!(accepted.load(Ordering::SeqCst), 4);
    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_010).await,
        HostOutcome::WorkflowCompleted { .. }
    ));
    assert_eq!(accepted.load(Ordering::SeqCst), 5);
}

#[tokio::test]
async fn exhausted_retry_replays_the_final_application_error() {
    let registry = ActivityRegistry::builder()
        .register::<Echo, _, _>("Echo", |_context, _input| async {
            Err(ActivityHandlerError::Retryable(ExactBytes::new(
                b"final-error",
            )))
        })
        .build()
        .unwrap();
    let store = kuberic_durable_execution::InMemoryCheckpointStore::new();
    let host = DurableHost::new(store, HostEpoch::from_bytes([11; 16]), limits());
    let mut runner = ActivityRunner::new(host, registry);
    let workflow = TypedWorkflow {
        options: ActivityOptions::new(3, 1, None, None).unwrap(),
    };

    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_000).await,
        HostOutcome::RetryScheduled { .. }
    ));
    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_001).await,
        HostOutcome::RetryScheduled { .. }
    ));
    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_003).await,
        HostOutcome::ObservationAccepted { .. }
    ));
    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_003).await,
        HostOutcome::WorkflowCompleted {
            checkpoint_status: TerminalCheckpointStatus::Accepted,
            outcome: TerminalOutcome::Failed(_),
            ..
        }
    ));
}

#[tokio::test]
async fn timeout_is_terminal_and_does_not_consume_a_retry() {
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let registry = ActivityRegistry::builder()
        .register::<Echo, _, _>("Echo", move |_context, _input| {
            handler_calls.fetch_add(1, Ordering::SeqCst);
            async {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                Ok("late".to_string())
            }
        })
        .build()
        .unwrap();
    let store = kuberic_durable_execution::InMemoryCheckpointStore::new();
    let host = DurableHost::new(store, HostEpoch::from_bytes([13; 16]), limits());
    let mut runner = ActivityRunner::new(host, registry).with_timeout_runtime(TokioTimeoutRuntime);
    let workflow = TypedWorkflow {
        options: ActivityOptions::new(3, 1, None, Some(1)).unwrap(),
    };

    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_000).await,
        HostOutcome::ObservationAccepted { .. }
    ));
    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_000).await,
        HostOutcome::WorkflowCompleted {
            outcome: TerminalOutcome::Failed(_),
            ..
        }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn elapsed_action_deadline_fails_without_invoking_handler() {
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let registry = ActivityRegistry::builder()
        .register::<Echo, _, _>("Echo", move |_context, input| {
            handler_calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(input.value) }
        })
        .build()
        .unwrap();
    let store = kuberic_durable_execution::InMemoryCheckpointStore::new();
    let host = DurableHost::new(store, HostEpoch::from_bytes([15; 16]), limits());
    let mut runner = ActivityRunner::new(host, registry);
    let workflow = TypedWorkflow {
        options: ActivityOptions::new(3, 1, Some(999), None).unwrap(),
    };
    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_000).await,
        HostOutcome::ObservationAccepted { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_000).await,
        HostOutcome::WorkflowCompleted {
            outcome: TerminalOutcome::Failed(_),
            ..
        }
    ));
}

#[tokio::test]
async fn handler_wait_is_persisted_without_consuming_a_retry() {
    let calls = Arc::new(AtomicUsize::new(0));
    let ordinals = Arc::new(std::sync::Mutex::new(Vec::new()));
    let handler_calls = calls.clone();
    let handler_ordinals = ordinals.clone();
    let registry = ActivityRegistry::builder()
        .register::<Echo, _, _>("Echo", move |context, input| {
            let call = handler_calls.fetch_add(1, Ordering::SeqCst);
            handler_ordinals
                .lock()
                .unwrap()
                .push(context.attempt_ordinal());
            async move {
                if call == 0 {
                    Err(ActivityHandlerError::WaitUntil(1_010))
                } else {
                    Ok(input.value)
                }
            }
        })
        .build()
        .unwrap();
    let store = InMemoryCheckpointStore::new();
    let host = DurableHost::new(store, HostEpoch::from_bytes([21; 16]), limits());
    let mut runner = ActivityRunner::new(host, registry);
    let workflow = TypedWorkflow {
        options: ActivityOptions::default(),
    };

    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_000).await,
        HostOutcome::Waiting {
            wake_at_unix_millis: 1_010,
            ..
        }
    ));
    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_005).await,
        HostOutcome::Waiting {
            wake_at_unix_millis: 1_010,
            ..
        }
    ));
    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_010).await,
        HostOutcome::ObservationAccepted { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(*ordinals.lock().unwrap(), vec![1, 1]);
}

#[tokio::test]
async fn unknown_result_cas_reloads_without_duplicate_after_apply() {
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let registry = ActivityRegistry::builder()
        .register::<Echo, _, _>("Echo", move |_context, input| {
            handler_calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(input.value) }
        })
        .build()
        .unwrap();
    let store = kuberic_durable_execution::InMemoryCheckpointStore::new();
    store.fail_compare_and_swap_after(1, InMemoryFault::OutcomeUnknownAfterApply);
    let host = DurableHost::new(store, HostEpoch::from_bytes([14; 16]), limits());
    let mut runner = ActivityRunner::new(host, registry);
    let workflow = TypedWorkflow {
        options: ActivityOptions::default(),
    };

    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_000).await,
        HostOutcome::ReloadRequired {
            reason: ReloadReason::OutcomeUnknown,
            ..
        }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_000).await,
        HostOutcome::WorkflowCompleted {
            checkpoint_status: TerminalCheckpointStatus::Accepted,
            ..
        }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn unknown_retry_cas_after_apply_reloads_persisted_next_attempt() {
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let registry = ActivityRegistry::builder()
        .register::<Echo, _, _>("Echo", move |_context, input| {
            let call = handler_calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if call == 0 {
                    Err(ActivityHandlerError::Retryable(ExactBytes::new(b"retry")))
                } else {
                    Ok(input.value)
                }
            }
        })
        .build()
        .unwrap();
    let store = kuberic_durable_execution::InMemoryCheckpointStore::new();
    store.fail_compare_and_swap_after(1, InMemoryFault::OutcomeUnknownAfterApply);
    let host = DurableHost::new(store, HostEpoch::from_bytes([17; 16]), limits());
    let mut runner = ActivityRunner::new(host, registry);
    let workflow = TypedWorkflow {
        options: ActivityOptions::new(3, 10, None, None).unwrap(),
    };

    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_000).await,
        HostOutcome::ReloadRequired {
            reason: ReloadReason::OutcomeUnknown,
            ..
        }
    ));
    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_010).await,
        HostOutcome::ObservationAccepted { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn lost_result_is_retried_with_the_same_logical_identity() {
    let observed_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
    let handler_ids = observed_ids.clone();
    let registry = ActivityRegistry::builder()
        .register::<Echo, _, _>("Echo", move |context, input| {
            let ids = handler_ids.clone();
            async move {
                ids.lock()
                    .unwrap()
                    .push(context.activity_instance_id().clone());
                Ok(input.value)
            }
        })
        .build()
        .unwrap();
    let store = kuberic_durable_execution::InMemoryCheckpointStore::new();
    let mut host = DurableHost::new(store, HostEpoch::from_bytes([12; 16]), limits());
    let workflow = TypedWorkflow {
        options: ActivityOptions::new(3, 10, None, None).unwrap(),
    };
    let HostOutcome::DispatchPermitted { permit, .. } =
        host.turn_and_expose_at(&workflow, execution(), 1_000).await
    else {
        panic!("first attempt was not exposed");
    };
    let first_logical_id = permit.activity().clone();
    // Simulate side effect completion followed by process loss before result persistence.
    observed_ids.lock().unwrap().push(first_logical_id.clone());

    let mut runner = ActivityRunner::new(host, registry);
    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_001).await,
        HostOutcome::RetryScheduled { .. }
    ));
    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_011).await,
        HostOutcome::ObservationAccepted { .. }
    ));
    let ids = observed_ids.lock().unwrap();
    assert_eq!(ids.len(), 2);
    assert_eq!(ids[0], ids[1]);
}

#[tokio::test]
async fn lost_result_at_attempt_limit_becomes_a_replayed_application_failure() {
    let registry = ActivityRegistry::builder()
        .register::<Echo, _, _>("Echo", |_context, input| async move { Ok(input.value) })
        .build()
        .unwrap();
    let store = kuberic_durable_execution::InMemoryCheckpointStore::new();
    let mut host = DurableHost::new(store, HostEpoch::from_bytes([16; 16]), limits());
    let workflow = TypedWorkflow {
        options: ActivityOptions::new(1, 10, None, None).unwrap(),
    };
    assert!(matches!(
        host.turn_and_expose_at(&workflow, execution(), 1_000).await,
        HostOutcome::DispatchPermitted { .. }
    ));
    let mut runner = ActivityRunner::new(host, registry);
    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_001).await,
        HostOutcome::ObservationAccepted { .. }
    ));
    assert!(matches!(
        runner.run_once(&workflow, execution(), 1_001).await,
        HostOutcome::WorkflowCompleted {
            outcome: TerminalOutcome::Failed(_),
            ..
        }
    ));
}

#[test]
fn format_three_checkpoint_fails_closed_before_workflow_polling() {
    let old = CheckpointEnvelope::new(3, ExactBytes::default());
    assert!(matches!(
        old.decode_and_validate(&execution(), limits()),
        Err(CheckpointError::UnsupportedFormat {
            actual: 3,
            supported: 4
        })
    ));
}
