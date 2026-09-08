mod support;

use async_trait::async_trait;
use futures::executor::block_on;
use kuberic_durable_execution::{
    CheckpointLimits, DurableActivity, DurableHost, ExactBytes, ExecutionId, ExecutionSpec,
    HOST_OUTCOME_VARIANTS, HostEpoch, HostOutcome, InMemoryCheckpointStore, TerminalOutcome,
    Workflow, WorkflowContext,
};
use serde::{Deserialize, Serialize};
use support::scenarios::{ScenarioId, run_conformance_matrix};

struct OrdinaryAsyncWorkflow;

struct OrdinaryAsyncActivity;

#[derive(Deserialize, Serialize)]
struct GreetingInput {
    message: String,
}

impl DurableActivity for OrdinaryAsyncActivity {
    type Input = GreetingInput;
    type Output = Vec<u8>;

    const NAME: &'static str = "ordinary-async";
    const VERSION: u32 = 1;
    const MAX_INPUT_BYTES: u64 = 1024;
    const MAX_RESULT_BYTES: u64 = 1024;
}

#[async_trait]
impl Workflow for OrdinaryAsyncWorkflow {
    async fn run(&self, context: &mut WorkflowContext<'_>, _input: ExactBytes) -> TerminalOutcome {
        // FR012_WORKFLOW_START
        match context
            .call::<OrdinaryAsyncActivity>(GreetingInput {
                message: "hello".to_owned(),
            })
            .await
        {
            Ok(result) => TerminalOutcome::succeeded(result),
            Err(error) => TerminalOutcome::failed(error.to_string().into_bytes()),
        }
        // FR012_WORKFLOW_END
    }
}

#[test]
fn ordinary_async_mechanically_passes_fr_012_and_is_the_sole_surface() {
    let source = include_str!("authoring_surface.rs");
    let body = source
        .split_once("// FR012_WORKFLOW_START")
        .unwrap()
        .1
        .split_once("// FR012_WORKFLOW_END")
        .unwrap()
        .0;
    let framework_operation_count = body.matches(".call::<").count();
    let authored_poll = body.contains(concat!("fn po", "ll("))
        || body.contains(concat!("impl Future", " for"))
        || body.contains(concat!("state_", "machine"));
    let raw_plumbing = [
        "ActivitySpec",
        "ActivityName",
        "ExactBytes",
        "into_vec",
        "serde_json",
        "MAX_RESULT_BYTES",
    ]
    .iter()
    .any(|symbol| body.contains(symbol));

    let store = InMemoryCheckpointStore::new();
    let mut host = DurableHost::new(
        store,
        HostEpoch::from_bytes([1; 16]),
        CheckpointLimits::new(16, 100_000, 100_000).unwrap(),
    );
    let first_turn = block_on(host.turn(
        &OrdinaryAsyncWorkflow,
        ExecutionSpec::new(
            ExecutionId::from_bytes([1; 16]),
            ExactBytes::new(b"input"),
            1024,
        ),
    ));
    let all_scenarios = block_on(run_conformance_matrix());
    for scenario in &all_scenarios {
        println!(
            "{} public-API fixture: {}",
            scenario.id.stable_id(),
            if scenario.passed() { "PASS" } else { "FAIL" }
        );
    }
    let library_exports = include_str!("../src/lib.rs");
    let async_surface_exported =
        library_exports.contains("pub use workflow::{TerminalOutcome, Workflow, WorkflowContext};");
    let fallback_surface_exported = library_exports.contains(concat!("mod po", "ll;"))
        || library_exports.contains("ReplayWorkflow")
        || library_exports.contains("ReplayContext");

    let predicates = [
        (
            "one-activity workflow is an ordinary async method",
            matches!(first_turn, HostOutcome::ScheduleAccepted { .. }),
        ),
        ("no author-written poll or state machine", !authored_poll),
        (
            "no raw identity, byte, serde, or bound plumbing",
            !raw_plumbing,
        ),
        (
            "workflow body uses no more than two framework operations",
            framework_operation_count <= 2,
        ),
        (
            "every FR-013 fixture uses and passes through public APIs",
            all_scenarios.len() == ScenarioId::ALL.len()
                && all_scenarios.iter().all(|scenario| scenario.passed()),
        ),
        (
            "exactly one public authoring surface remains",
            async_surface_exported && !fallback_surface_exported,
        ),
    ];

    println!("selected surface: ordinary async Workflow::run");
    println!("workflow-body framework operations: {framework_operation_count}");
    let outcome_count = HOST_OUTCOME_VARIANTS.len();
    println!("public HostOutcome variants: {outcome_count}");
    assert_eq!(outcome_count, 10);
    for (predicate, passed) in predicates {
        println!("[{}] {predicate}", if passed { "PASS" } else { "FAIL" });
        assert!(passed, "FR-012 predicate failed: {predicate}");
    }
}
