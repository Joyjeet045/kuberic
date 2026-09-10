mod support;

use futures::executor::block_on;
use kuberic_dex::{
    CheckpointLimits, DurableHost, Evaluation, ExecutionId, ExecutionSpec, HOST_OUTCOME_VARIANTS,
    HostEpoch, HostOutcome, InMemoryCheckpointStore, OrchestrationContext, OrchestrationRegistry,
    OrchestrationRegistryError, WorkflowCodecError, decode_workflow_result, evaluate,
};
use serde::{Deserialize, Serialize};
use support::scenarios::{ScenarioId, run_conformance_matrix};

#[derive(Deserialize, Serialize)]
struct GreetingInput {
    message: String,
}

fn orchestrations() -> OrchestrationRegistry {
    OrchestrationRegistry::builder()
        .register_typed::<GreetingInput, Vec<u8>, kuberic_dex::ActivityInvocationError, _>(
            "OrdinaryAsync",
            |context: &mut OrchestrationContext<'_>, input| {
                Box::pin(async move {
                    // FR012_WORKFLOW_START
                    let result = context
                        .schedule_activity_typed::<GreetingInput, Vec<u8>>("ordinary-async", &input)
                        .await?;
                    Ok(result)
                    // FR012_WORKFLOW_END
                })
            },
        )
        .register_typed::<GreetingInput, String, String, _>(
            "ImmediateSuccess",
            |_context: &mut OrchestrationContext<'_>, input| {
                Box::pin(async move { Ok(input.message) })
            },
        )
        .register_typed::<GreetingInput, String, String, _>(
            "ImmediateFailure",
            |_context: &mut OrchestrationContext<'_>, input| {
                Box::pin(async move { Err(format!("cannot greet {}", input.message)) })
            },
        )
        .build()
        .unwrap()
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
    let framework_operation_count = body.matches(".schedule_activity_typed::<").count();
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
    let orchestrations = orchestrations();
    let workflow = orchestrations.get("OrdinaryAsync").unwrap();
    let mut host = DurableHost::new(
        store,
        HostEpoch::from_bytes([1; 16]),
        CheckpointLimits::new(16, 100_000, 100_000).unwrap(),
    );
    let first_turn = block_on(
        host.turn(
            workflow,
            ExecutionSpec::typed(
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
    let async_surface_exported =
        library_exports.contains("OrchestrationContext, OrchestrationFuture");
    let low_level_surface_hidden = library_exports
        .contains("#[doc(hidden)]\npub use workflow::{Orchestration, Workflow, WorkflowContext");

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

    println!("selected surface: typed async orchestration registry");
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
    let orchestrations = orchestrations();
    let immediate_success = orchestrations.get("ImmediateSuccess").unwrap();
    let immediate_failure = orchestrations.get("ImmediateFailure").unwrap();
    let execution = ExecutionSpec::typed(
        ExecutionId::from_bytes([2; 16]),
        &GreetingInput {
            message: "hello".to_owned(),
        },
        1024,
    )
    .unwrap();
    let Evaluation::Complete { outcome, .. } =
        evaluate(immediate_success, &execution, None, limits)
    else {
        panic!("typed orchestration did not complete");
    };
    assert_eq!(
        decode_workflow_result::<String, String>(&outcome).unwrap(),
        Ok("hello".to_owned())
    );

    let invalid = ExecutionSpec::new(
        ExecutionId::from_bytes([3; 16]),
        b"not-json".to_vec().into(),
        1024,
    );
    let Evaluation::Complete { outcome, .. } = evaluate(immediate_success, &invalid, None, limits)
    else {
        panic!("invalid typed input did not become a terminal failure");
    };
    assert_eq!(
        decode_workflow_result::<String, String>(&outcome),
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
        evaluate(immediate_failure, &execution, None, limits)
    else {
        panic!("typed orchestration failure did not complete");
    };
    assert_eq!(
        decode_workflow_result::<String, String>(&outcome).unwrap(),
        Err("cannot greet Ada".to_owned())
    );
}

#[test]
fn orchestration_registry_rejects_invalid_names_and_duplicates() {
    let empty = OrchestrationRegistry::builder()
        .register_typed::<GreetingInput, String, String, _>(
            "",
            |_context: &mut OrchestrationContext<'_>, input| {
                Box::pin(async move { Ok(input.message) })
            },
        )
        .build();
    assert!(matches!(empty, Err(OrchestrationRegistryError::EmptyName)));

    let duplicate = OrchestrationRegistry::builder()
        .register_typed::<GreetingInput, String, String, _>(
            "Greeting",
            |_context: &mut OrchestrationContext<'_>, input| {
                Box::pin(async move { Ok(input.message) })
            },
        )
        .register_typed::<GreetingInput, String, String, _>(
            "Greeting",
            |_context: &mut OrchestrationContext<'_>, input| {
                Box::pin(async move { Ok(input.message) })
            },
        )
        .build();
    assert!(matches!(
        duplicate,
        Err(OrchestrationRegistryError::DuplicateRegistration(name)) if name == "Greeting"
    ));

    let registry = OrchestrationRegistry::builder().build().unwrap();
    assert!(matches!(
        registry.get("Missing"),
        Err(OrchestrationRegistryError::Unregistered(name)) if name == "Missing"
    ));
}
