use async_trait::async_trait;
use kuberic_durable_execution::{
    CheckpointLimits, DurableActivity, Evaluation, ExactBytes, ExecutionId, ExecutionSpec,
    TerminalOutcome, Workflow, WorkflowContext, evaluate,
};

struct OneActivityWorkflow;
struct OneActivity;

impl DurableActivity for OneActivity {
    type Input = Vec<u8>;
    type Output = Vec<u8>;

    const NAME: &'static str = "one-activity";
    const VERSION: u32 = 1;
    const MAX_INPUT_BYTES: u64 = 1024;
    const MAX_RESULT_BYTES: u64 = 1024;
}

#[async_trait]
impl Workflow for OneActivityWorkflow {
    async fn run(&self, context: &mut WorkflowContext<'_>, input: ExactBytes) -> TerminalOutcome {
        match context.call::<OneActivity>(input.into_vec()).await {
            Ok(result) => TerminalOutcome::succeeded(result),
            Err(error) => TerminalOutcome::failed(error.to_string().into_bytes()),
        }
    }
}

#[test]
fn one_activity_is_an_ordinary_async_method_with_one_framework_call() {
    let outcome = evaluate(
        &OneActivityWorkflow,
        &ExecutionSpec::new(
            ExecutionId::from_bytes([1; 16]),
            ExactBytes::new(b"exact input"),
            1024,
        ),
        None,
        CheckpointLimits::new(16, 100_000, 100_000).unwrap(),
    );

    assert!(matches!(outcome, Evaluation::Scheduled { .. }));
}
