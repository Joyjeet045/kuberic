use async_trait::async_trait;
use kuberic_dex::{
    ActivityContext, ActivityHandlerError, ActivityInvocationError, ActivityRegistry,
    ActivityRunner, CheckpointLimits, DurableActivity, DurableHost, ExecutionId, ExecutionSpec,
    HostEpoch, HostOutcome, InMemoryCheckpointStore, Orchestration, OrchestrationContext,
    decode_orchestration_result,
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

struct Greet;

impl DurableActivity for Greet {
    type Input = GreetingInput;
    type Output = Greeting;

    const NAME: &'static str = "Greet";
    const VERSION: u32 = 1;
    const MAX_INPUT_BYTES: u64 = 1024;
    const MAX_RESULT_BYTES: u64 = 1024;
}

struct HelloWorld;

#[async_trait]
impl Orchestration for HelloWorld {
    type Input = GreetingInput;
    type Output = Greeting;
    type Error = ActivityInvocationError;

    async fn run(
        &self,
        context: &mut OrchestrationContext<'_>,
        input: GreetingInput,
    ) -> Result<Greeting, ActivityInvocationError> {
        Ok(context.schedule_activity::<Greet>(&input).await?)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let activities = ActivityRegistry::builder()
        .register_typed::<Greet, _, _>("Greet", |_context: ActivityContext, input| async move {
            Ok::<_, ActivityHandlerError>(Greeting {
                message: format!("Hello, {}!", input.name),
            })
        })
        .build()?;

    let host = DurableHost::new(
        InMemoryCheckpointStore::new(),
        HostEpoch::from_bytes([1; 16]),
        CheckpointLimits::new(16, 64 * 1024, 64 * 1024)?,
    );
    let mut runner = ActivityRunner::new(host, activities);
    let execution = ExecutionSpec::for_orchestration::<HelloWorld>(
        ExecutionId::from_bytes([2; 16]),
        &GreetingInput {
            name: "DEX".to_owned(),
        },
        1024,
    )?;

    for turn in 0..4 {
        if let HostOutcome::WorkflowCompleted { outcome, .. } =
            runner.run_once(&HelloWorld, execution.clone(), turn).await
        {
            let greeting = decode_orchestration_result::<HelloWorld>(&outcome)??;
            println!("{}", greeting.message);
            return Ok(());
        }
    }

    Err("workflow did not complete".into())
}
