mod support;

use async_trait::async_trait;
use futures::executor::block_on;
use kuberic_dex::{
    CheckpointLimits, DurableActivity, DurableHost, Evaluation, ExecutionId, ExecutionSpec,
    HOST_OUTCOME_VARIANTS, HostEpoch, HostOutcome, InMemoryCheckpointStore, Orchestration,
    OrchestrationContext, WorkflowCodecError, decode_orchestration_result, evaluate,
};
use serde::{Deserialize, Serialize};
use support::scenarios::{ScenarioId, run_conformance_matrix};

struct OrdinaryAsyncOrchestration;

struct ImmediateSuccess;
struct ImmediateFailure;

#[derive(Deserialize, Serialize)]
struct GreetingInput {
    message: String,
}

struct OrdinaryAsyncActivity;

impl DurableActivity for OrdinaryAsyncActivity {
    type Input = GreetingInput;
    type Output = Vec<u8>;

    const NAME: &'static str = "ordinary-async";
    const VERSION: u32 = 1;
    const MAX_INPUT_BYTES: u64 = 1024;
    const MAX_RESULT_BYTES: u64 = 1024;
}

#[async_trait]
impl Orchestration for OrdinaryAsyncOrchestration {
    type Input = GreetingInput;
    type Output = Vec<u8>;
    type Error = kuberic_dex::ActivityInvocationError;

    async fn run(
        &self,
        context: &mut OrchestrationContext<'_>,
        input: GreetingInput,
    ) -> Result<Vec<u8>, kuberic_dex::ActivityInvocationError> {
        // FR012_WORKFLOW_START
        let result = context
            .schedule_activity::<OrdinaryAsyncActivity>(&input)
            .await?;
        Ok(result)
        // FR012_WORKFLOW_END
    }
}

#[async_trait]
impl Orchestration for ImmediateSuccess {
    type Input = GreetingInput;
    type Output = String;
    type Error = String;

    async fn run(
        &self,
        _context: &mut OrchestrationContext<'_>,
        input: GreetingInput,
    ) -> Result<String, String> {
        Ok(input.message)
    }
}

#[async_trait]
impl Orchestration for ImmediateFailure {
    type Input = GreetingInput;
    type Output = String;
    type Error = String;

    async fn run(
        &self,
        _context: &mut OrchestrationContext<'_>,
        input: GreetingInput,
    ) -> Result<String, String> {
        Err(format!("cannot greet {}", input.message))
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
    let framework_operation_count = body.matches(".schedule_activity::<").count();
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
    let first_turn = block_on(
        host.turn(
            &OrdinaryAsyncOrchestration,
            ExecutionSpec::for_orchestration::<OrdinaryAsyncOrchestration>(
                ExecutionId::from_bytes([1; 16]),
                &GreetingInput {
                    message: "hello".to_owned(),
                },
                1024,
            )
            .unwrap(),
        ),
    );
    let all_scenarios = block_on(run_conformance_matrix());
    for scenario in &all_scenarios {
        println!(
            "{} public-API fixture: {}",
            scenario.id.stable_id(),
            if scenario.passed() { "PASS" } else { "FAIL" }
        );
    }

    let library_exports = include_str!("../src/lib.rs");
    let async_surface_exported = library_exports.contains("Orchestration, OrchestrationContext");
    let low_level_surface_hidden =
        library_exports.contains("#[doc(hidden)]\npub use workflow::{Workflow, WorkflowContext};");

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
            "typed orchestration is the documented application surface",
            async_surface_exported && low_level_surface_hidden,
        ),
    ];

    println!("selected surface: typed async Orchestration::run");
    println!("workflow-body framework operations: {framework_operation_count}");
    let outcome_count = HOST_OUTCOME_VARIANTS.len();
    println!("public HostOutcome variants: {outcome_count}");
    assert_eq!(outcome_count, 12);
    for (predicate, passed) in predicates {
        println!("[{}] {predicate}", if passed { "PASS" } else { "FAIL" });
        assert!(passed, "FR-012 predicate failed: {predicate}");
    }
}

#[test]
fn typed_orchestration_boundary_round_trips_results_and_reports_bad_input() {
    let limits = CheckpointLimits::new(16, 100_000, 100_000).unwrap();
    let execution = ExecutionSpec::typed(
        ExecutionId::from_bytes([2; 16]),
        &GreetingInput {
            message: "hello".to_owned(),
        },
        1024,
    )
    .unwrap();
    let Evaluation::Complete { outcome, .. } =
        evaluate(&ImmediateSuccess, &execution, None, limits)
    else {
        panic!("typed orchestration did not complete");
    };
    assert_eq!(
        decode_orchestration_result::<ImmediateSuccess>(&outcome).unwrap(),
        Ok("hello".to_owned())
    );

    let invalid = ExecutionSpec::new(
        ExecutionId::from_bytes([3; 16]),
        b"not-json".to_vec().into(),
        1024,
    );
    let Evaluation::Complete { outcome, .. } = evaluate(&ImmediateSuccess, &invalid, None, limits)
    else {
        panic!("invalid typed input did not become a terminal failure");
    };
    assert_eq!(
        decode_orchestration_result::<ImmediateSuccess>(&outcome),
        Err(WorkflowCodecError::InputDecoding)
    );

    let execution = ExecutionSpec::typed(
        ExecutionId::from_bytes([4; 16]),
        &GreetingInput {
            message: "Ada".to_owned(),
        },
        1024,
    )
    .unwrap();
    let Evaluation::Complete { outcome, .. } =
        evaluate(&ImmediateFailure, &execution, None, limits)
    else {
        panic!("typed orchestration failure did not complete");
    };
    assert_eq!(
        decode_orchestration_result::<ImmediateFailure>(&outcome).unwrap(),
        Err("cannot greet Ada".to_owned())
    );
}
