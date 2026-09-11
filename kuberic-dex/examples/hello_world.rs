use kuberic_dex::{
    ActivityContext, ActivityHandlerError, ActivityInvocationError, ActivityRegistry,
    ActivityRunner, CheckpointLimits, DurableHost, ExecutionId, ExecutionSpec, HostEpoch,
    HostOutcome, InMemoryCheckpointStore, OrchestrationContext, OrchestrationRegistry,
    decode_workflow_result,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
struct GreetingInput {
    name: String,
}

#[derive(Deserialize, Serialize)]
struct Greeting {
    message: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let orchestrations = OrchestrationRegistry::builder()
        .register_typed::<GreetingInput, Greeting, ActivityInvocationError, _, _>(
            "HelloWorld",
            |context: OrchestrationContext, input| async move {
                context
                    .schedule_activity_typed::<GreetingInput, Greeting>("Greet", &input)
                    .await
            },
        )
        .build()?;
    let workflow = orchestrations.get("HelloWorld")?;

    let activities = ActivityRegistry::builder()
        .register_typed::<GreetingInput, Greeting, _, _>(
            "Greet",
            |_context: ActivityContext, input| async move {
                Ok::<_, ActivityHandlerError>(Greeting {
                    message: format!("Hello, {}!", input.name),
                })
            },
        )
        .build()?;

    let host = DurableHost::new(
        InMemoryCheckpointStore::new(),
        HostEpoch::from_bytes([1; 16]),
        CheckpointLimits::new(16, 64 * 1024, 64 * 1024)?,
    );
    let mut runner = ActivityRunner::new(host, activities);
    let execution = ExecutionSpec::typed(
        ExecutionId::from_bytes([2; 16]),
        &GreetingInput {
            name: "DEX".to_owned(),
        },
        1024,
    )?;

    for turn in 0..4 {
        if let HostOutcome::WorkflowCompleted { outcome, .. } =
            runner.run_once(workflow, execution.clone(), turn).await
        {
            let greeting = decode_workflow_result::<Greeting, ActivityInvocationError>(&outcome)??;
            println!("{}", greeting.message);
            return Ok(());
        }
    }

    Err("workflow did not complete".into())
}
