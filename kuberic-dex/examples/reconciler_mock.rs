use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use kuberic_dex::{
    ActivityContext, ActivityHandlerError, ActivityInvocationError, ActivityRegistry,
    CheckpointLimits, DurableActivity, DurableHost, ExactBytes, ExecutionId, ExecutionSpec,
    HostEpoch, InMemoryCheckpointStore, Orchestration, OrchestrationContext,
    decode_orchestration_result,
    reconciler_mock::{MockReconcileAction, MockReconciler},
};
use serde::{Deserialize, Serialize};

#[derive(Default)]
struct MockKubernetesApi {
    annotations: BTreeMap<String, String>,
}

#[derive(Deserialize, Serialize)]
struct ReconcileInput {
    resource: String,
}

#[derive(Deserialize, Serialize)]
struct ReconcileResult {
    resource: String,
}

struct MarkReady;

impl DurableActivity for MarkReady {
    type Input = ReconcileInput;
    type Output = ReconcileResult;

    const NAME: &'static str = "MarkReady";
    const VERSION: u32 = 1;
    const MAX_INPUT_BYTES: u64 = 1024;
    const MAX_RESULT_BYTES: u64 = 1024;
}

struct ReconcileResource;

#[async_trait]
impl Orchestration for ReconcileResource {
    type Input = ReconcileInput;
    type Output = ReconcileResult;
    type Error = ActivityInvocationError;

    async fn run(
        &self,
        context: &mut OrchestrationContext<'_>,
        input: ReconcileInput,
    ) -> Result<ReconcileResult, ActivityInvocationError> {
        Ok(context.schedule_activity::<MarkReady>(&input).await?)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api = Arc::new(Mutex::new(MockKubernetesApi::default()));
    let activity_api = api.clone();
    let attempts = Arc::new(AtomicUsize::new(0));
    let activity_attempts = attempts.clone();
    let activities = ActivityRegistry::builder()
        .register_typed::<MarkReady, _, _>("MarkReady", move |_context: ActivityContext, input| {
            let api = activity_api.clone();
            let attempts = activity_attempts.clone();
            async move {
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(ActivityHandlerError::Retryable(ExactBytes::new(
                        b"API temporarily unavailable",
                    )));
                }
                api.lock()
                    .expect("mock API lock")
                    .annotations
                    .insert(input.resource.clone(), "ready".to_owned());
                Ok::<_, ActivityHandlerError>(ReconcileResult {
                    resource: input.resource,
                })
            }
        })
        .build()?;

    let host = DurableHost::new(
        InMemoryCheckpointStore::new(),
        HostEpoch::from_bytes([1; 16]),
        CheckpointLimits::new(16, 64 * 1024, 64 * 1024)?,
    );
    let mut reconciler = MockReconciler::new(host, activities, 1_000);
    let execution = ExecutionSpec::for_orchestration::<ReconcileResource>(
        ExecutionId::from_bytes([2; 16]),
        &ReconcileInput {
            resource: "demo".to_owned(),
        },
        1024,
    )?;

    for turn in 1..=4 {
        match reconciler
            .reconcile(&ReconcileResource, execution.clone())
            .await
        {
            MockReconcileAction::RequeueNow => {
                println!("reconcile {turn}: result persisted; requeue now");
            }
            MockReconcileAction::RequeueAt(time) => {
                println!("reconcile {turn}: retry at {time}ms");
                reconciler.advance_to(time);
            }
            MockReconcileAction::AwaitChange => return Err("waiting for a watch event".into()),
            MockReconcileAction::Complete(outcome) => {
                let result = decode_orchestration_result::<ReconcileResource>(&outcome)??;
                let state = api.lock().expect("mock API lock");
                println!("reconcile {turn}: workflow complete");
                println!(
                    "{}: {}",
                    result.resource, state.annotations[&result.resource]
                );
                return Ok(());
            }
            MockReconcileAction::Failed(outcome) => {
                return Err(format!("reconciliation failed: {outcome:?}").into());
            }
        }
    }

    Err("workflow did not complete".into())
}
