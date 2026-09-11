use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use kuberic_dex::{
    ActivityContext, ActivityHandlerError, ActivityInvocationError, ActivityRegistry,
    CheckpointLimits, DurableHost, ExactBytes, ExecutionId, ExecutionSpec, HostEpoch,
    InMemoryCheckpointStore, OrchestrationContext, OrchestrationRegistry, decode_workflow_result,
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let orchestrations = OrchestrationRegistry::builder()
        .register_typed::<ReconcileInput, ReconcileResult, ActivityInvocationError, _, _>(
            "ReconcileResource",
            |context: OrchestrationContext, input| async move {
                context
                    .schedule_activity_typed::<ReconcileInput, ReconcileResult>("MarkReady", &input)
                    .await
            },
        )
        .build()?;
    let workflow = orchestrations.get("ReconcileResource")?;

    let api = Arc::new(Mutex::new(MockKubernetesApi::default()));
    let activity_api = api.clone();
    let attempts = Arc::new(AtomicUsize::new(0));
    let activity_attempts = attempts.clone();
    let activities = ActivityRegistry::builder()
        .register_typed::<ReconcileInput, ReconcileResult, _, _>(
            "MarkReady",
            move |_context: ActivityContext, input| {
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
            },
        )
        .build()?;

    let host = DurableHost::new(
        InMemoryCheckpointStore::new(),
        HostEpoch::from_bytes([1; 16]),
        CheckpointLimits::new(16, 64 * 1024, 64 * 1024)?,
    );
    let mut reconciler = MockReconciler::new(host, activities, 1_000);
    let execution = ExecutionSpec::typed(
        ExecutionId::from_bytes([2; 16]),
        &ReconcileInput {
            resource: "demo".to_owned(),
        },
        1024,
    )?;

    for turn in 1..=4 {
        match reconciler.reconcile(workflow, execution.clone()).await {
            MockReconcileAction::RequeueNow => {
                println!("reconcile {turn}: result persisted; requeue now");
            }
            MockReconcileAction::RequeueAt(time) => {
                println!("reconcile {turn}: retry at {time}ms");
                reconciler.advance_to(time);
            }
            MockReconcileAction::AwaitChange => return Err("waiting for a watch event".into()),
            MockReconcileAction::Complete(outcome) => {
                let result =
                    decode_workflow_result::<ReconcileResult, ActivityInvocationError>(&outcome)??;
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
