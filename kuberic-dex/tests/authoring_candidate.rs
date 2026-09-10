use async_trait::async_trait;
use kuberic_dex::{
    CheckpointLimits, Evaluation, ExactBytes, ExecutionId, ExecutionSpec, TerminalOutcome,
    Workflow, WorkflowContext, evaluate,
};

struct OneActivityWorkflow;

#[async_trait]
impl Workflow for OneActivityWorkflow {
    async fn run(&self, context: &mut WorkflowContext<'_>, input: ExactBytes) -> TerminalOutcome {
        match context
            .schedule_activity_typed::<Vec<u8>, Vec<u8>>("one-activity", &input.into_vec())
            .await
        {
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
