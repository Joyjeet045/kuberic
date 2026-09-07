mod support;

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use futures::executor::block_on;
use kuberic_durable_execution::{
    ActivityName, ActivitySpec, CheckpointLimits, DurableHost, ExactBytes, ExecutionId,
    ExecutionSpec, FeasibilityClassification, FeasibilityInputs, HOST_OUTCOME_VARIANTS, HostEpoch,
    HostOutcome, InMemoryCheckpointStore, TerminalOutcome, Workflow, WorkflowContext,
    classify_feasibility,
};
use serde::Deserialize;
use support::scenarios::{ScenarioEvidence, ScenarioId, run_conformance_matrix};

const EXPECTED_FR_013_SCENARIOS: usize = 45;

struct SelectedOrdinaryAsyncSurface;

// FR012_SELECTED_WORKFLOW_START
#[async_trait]
impl Workflow for SelectedOrdinaryAsyncSurface {
    async fn run(&self, context: &mut WorkflowContext<'_>, input: ExactBytes) -> TerminalOutcome {
        TerminalOutcome::succeeded(
            context
                .activity(ActivitySpec::new(
                    ActivityName::new("ordinary-async", 1).unwrap(),
                    input,
                    1024,
                ))
                .await,
        )
    }
}
// FR012_SELECTED_WORKFLOW_END

#[derive(Clone, Copy)]
struct PredicateEvidence {
    name: &'static str,
    passed: bool,
}

fn library_dependency_lines(manifest: &str) -> Vec<&str> {
    let mut library_dependencies = false;
    manifest
        .lines()
        .filter(|line| {
            let trimmed = line.trim();
            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                library_dependencies = trimmed == "[dependencies]"
                    || (trimmed.starts_with("[target.") && trimmed.ends_with(".dependencies]"));
                return false;
            }
            library_dependencies
        })
        .collect()
}

fn library_dependencies_are_runtime_neutral(manifest: &str) -> bool {
    let dependencies = library_dependency_lines(manifest);
    if dependencies.is_empty() {
        return false;
    }
    const ASYNC_RUNTIMES: [&str; 4] = ["tokio", "async-std", "smol", "glommio"];
    !dependencies.into_iter().any(|line| {
        let line = line.split('#').next().unwrap_or_default();
        let Some((name, declaration)) = line.split_once('=') else {
            return false;
        };
        let name = name
            .trim()
            .trim_matches('"')
            .strip_suffix(".workspace")
            .unwrap_or_else(|| name.trim().trim_matches('"'));
        ASYNC_RUNTIMES.contains(&name)
            || ASYNC_RUNTIMES
                .iter()
                .any(|runtime| declaration.contains(&format!("package = \"{runtime}\"")))
    })
}

#[test]
fn runtime_neutrality_inspects_only_root_library_dependencies() {
    let test_only_runtime = r#"
[dependencies]
serde.workspace = true

[dev-dependencies]
tokio.workspace = true

[features]
runtime-test = ["tokio/test-util"]
"#;
    assert!(library_dependencies_are_runtime_neutral(test_only_runtime));

    let library_runtime = r#"
[dependencies]
serde.workspace = true
tokio = { workspace = true, features = ["rt"] }

[dev-dependencies]
pretty_assertions = "1"
"#;
    assert!(!library_dependencies_are_runtime_neutral(library_runtime));

    let target_runtime_is_a_library_dependency = r#"
[dependencies]
serde.workspace = true

[target.'cfg(unix)'.dependencies]
tokio.workspace = true

["dev-dependencies"]
tokio.workspace = true
"#;
    assert!(!library_dependencies_are_runtime_neutral(
        target_runtime_is_a_library_dependency
    ));

    let renamed_library_runtime = r#"
[dependencies]
runtime = { package = "tokio", version = "1" }
"#;
    assert!(!library_dependencies_are_runtime_neutral(
        renamed_library_runtime
    ));

    assert!(!library_dependencies_are_runtime_neutral(
        "[dev-dependencies]\ntokio.workspace = true\n"
    ));
}

#[test]
fn mechanically_assesses_the_selected_surface_and_full_denominator() {
    let scenarios = block_on(run_conformance_matrix());
    let registry_ids: Vec<_> = ScenarioId::ALL.iter().map(|id| id.stable_id()).collect();
    let unique_registry_ids: BTreeSet<_> = registry_ids.iter().copied().collect();

    assert_eq!(
        ScenarioId::ALL.len(),
        EXPECTED_FR_013_SCENARIOS,
        "the sole FR-013 registry must retain the reviewed denominator"
    );
    assert_eq!(
        scenarios.len(),
        EXPECTED_FR_013_SCENARIOS,
        "the runner must evaluate every registered FR-013 scenario"
    );
    assert_eq!(
        unique_registry_ids.len(),
        EXPECTED_FR_013_SCENARIOS,
        "FR-013 stable IDs must be unique"
    );
    for (index, id) in registry_ids.iter().enumerate() {
        assert_eq!(
            *id,
            format!("FR-013-{:02}", index + 1),
            "FR-013 stable IDs must be contiguous"
        );
    }

    let source = include_str!("feasibility.rs");
    let workflow_body = source
        .split_once("// FR012_SELECTED_WORKFLOW_START")
        .unwrap()
        .1
        .split_once("// FR012_SELECTED_WORKFLOW_END")
        .unwrap()
        .0;
    let framework_operation_count = workflow_body.matches(".activity(").count();
    let authored_poll_or_state_machine = workflow_body.contains(concat!("fn po", "ll("))
        || workflow_body.contains(concat!("impl Future", " for"))
        || workflow_body.contains(concat!("state_", "machine"));

    let library_exports = include_str!("../src/lib.rs");
    let ordinary_async_exported =
        library_exports.contains("pub use workflow::{TerminalOutcome, Workflow, WorkflowContext};");
    let fallback_exported = library_exports.contains(concat!("mod po", "ll;"))
        || library_exports.contains("ReplayWorkflow")
        || library_exports.contains("ReplayContext");
    let public_authoring_surface_count =
        usize::from(ordinary_async_exported) + usize::from(fallback_exported);

    let store = InMemoryCheckpointStore::new();
    let mut host = DurableHost::new(
        store,
        HostEpoch::from_bytes([1; 16]),
        CheckpointLimits::new(16, 100_000).unwrap(),
    );
    let first_turn = block_on(host.turn(
        &SelectedOrdinaryAsyncSurface,
        ExecutionSpec::new(
            ExecutionId::from_bytes([1; 16]),
            ExactBytes::new(b"input"),
            1024,
        ),
    ));

    let fr_012 = [
        PredicateEvidence {
            name: "one-activity workflow is an ordinary async method",
            passed: matches!(first_turn, HostOutcome::ScheduleAccepted { .. })
                && workflow_body.matches("async fn run").count() == 1,
        },
        PredicateEvidence {
            name: "no author-written Future, poll, or state machine",
            passed: !authored_poll_or_state_machine,
        },
        PredicateEvidence {
            name: "workflow body uses no more than two framework-specific operations",
            passed: framework_operation_count <= 2,
        },
        PredicateEvidence {
            name: "every registered fixture runs through the integration-test public API",
            passed: scenarios.len() == ScenarioId::ALL.len()
                && scenarios
                    .iter()
                    .all(|scenario| !scenario.assertions.is_empty()),
        },
        PredicateEvidence {
            name: "exactly one public authoring surface remains",
            passed: ordinary_async_exported
                && !fallback_exported
                && public_authoring_surface_count == 1,
        },
    ];

    let scenario_passed = |id| {
        scenarios
            .iter()
            .find(|scenario| scenario.id == id)
            .is_some_and(|scenario| scenario.passed())
    };
    let provider_contract_pass = [
        ScenarioId::LoadAbsenceAndProviderFailures,
        ScenarioId::OpaqueStorageRevisions,
        ScenarioId::OutcomeUnknownApplyStateHidden,
    ]
    .into_iter()
    .all(scenario_passed);
    let bounded_checkpoint_pass = [
        ScenarioId::ChangedResultBound,
        ScenarioId::ActivityCountAndGrowingHistory,
        ScenarioId::EncodedByteReservation,
        ScenarioId::OversizedObservation,
        ScenarioId::Base64ExactBytes,
    ]
    .into_iter()
    .all(scenario_passed);
    let terminal_lifecycle_pass = [
        ScenarioId::ActiveToTerminalCompaction,
        ScenarioId::TerminalReloadWithoutWorkflowPoll,
        ScenarioId::ZeroActivityTerminalization,
        ScenarioId::TerminalOutcomeBounds,
        ScenarioId::TerminalCapacityAdmission,
        ScenarioId::CompletionConflict,
        ScenarioId::CompletionOutcomeUnknownAfterApply,
        ScenarioId::CompletionOutcomeUnknownWithoutApply,
        ScenarioId::CompletionStoreFailures,
        ScenarioId::ExecutionContractValidation,
    ]
    .into_iter()
    .all(scenario_passed);
    let store_source = include_str!("../src/store.rs");
    let host_source = include_str!("../src/host.rs");
    let crate_manifest = include_str!("../Cargo.toml");
    let async_runtime_neutral_pass = store_source.contains("async fn load")
        && store_source.contains("async fn compare_and_swap")
        && host_source.contains("pub async fn turn")
        && host_source.contains("pub async fn observe")
        && library_dependencies_are_runtime_neutral(crate_manifest);
    let readme = include_str!("../README.md");
    let kernel_scope_documented = readme.contains("not an end-user runtime")
        && readme.contains("Deferred usability roadmap")
        && readme.contains("Kubernetes checkpoint provider");
    let revision_contract = [
        PredicateEvidence {
            name: "async provider and host contract is runtime neutral",
            passed: async_runtime_neutral_pass,
        },
        PredicateEvidence {
            name: "provider failure and uncertainty scenarios pass",
            passed: provider_contract_pass,
        },
        PredicateEvidence {
            name: "bounded checkpoint and base64 scenarios pass",
            passed: bounded_checkpoint_pass,
        },
        PredicateEvidence {
            name: "completion-only terminal lifecycle scenarios pass",
            passed: terminal_lifecycle_pass,
        },
        PredicateEvidence {
            name: "kernel scope and deferred roadmap are documented",
            passed: kernel_scope_documented,
        },
    ];

    let assertion_count = scenarios
        .iter()
        .map(|scenario| scenario.assertions.len())
        .sum::<usize>();
    let passed_scenarios = scenarios
        .iter()
        .filter(|scenario| scenario.passed())
        .count();
    let passed_assertions = scenarios
        .iter()
        .flat_map(|scenario| &scenario.assertions)
        .filter(|assertion| assertion.passed)
        .count();
    println!("EVIDENCE selected_surface=\"ordinary async Workflow::run\"");
    println!(
        "COUNT taxonomy=public_authoring_surfaces value={public_authoring_surface_count} bound=1"
    );
    println!(
        "COUNT taxonomy=framework_specific_workflow_body_operations value={framework_operation_count} bound=2"
    );
    println!(
        "COUNT taxonomy=public_host_outcome_variants value={} bound=none",
        HOST_OUTCOME_VARIANTS.len()
    );
    println!(
        "COUNT taxonomy=fr_013_scenarios value={} denominator={EXPECTED_FR_013_SCENARIOS} passed={passed_scenarios}",
        scenarios.len()
    );
    println!("COUNT taxonomy=fr_013_assertions value={assertion_count} passed={passed_assertions}");

    for predicate in fr_012 {
        println!(
            "FR012 status={} predicate={:?}",
            status(predicate.passed),
            predicate.name
        );
    }
    for predicate in revision_contract {
        println!(
            "REVISION status={} predicate={:?}",
            status(predicate.passed),
            predicate.name
        );
    }
    for scenario in &scenarios {
        println!(
            "SCENARIO id={} status={} assertions={} setup={:?}",
            scenario.id.stable_id(),
            status(scenario.passed()),
            scenario.assertions.len(),
            scenario.setup
        );
        for (index, assertion) in scenario.assertions.iter().enumerate() {
            println!(
                "ASSERTION scenario={} index={} class={} status={} text={:?}",
                scenario.id.stable_id(),
                index + 1,
                if assertion.safety_or_determinism {
                    "safety-or-determinism"
                } else {
                    "conformance"
                },
                status(assertion.passed),
                assertion.assertion
            );
        }
    }

    let safety_and_determinism_pass = scenarios
        .iter()
        .flat_map(|scenario| &scenario.assertions)
        .filter(|assertion| assertion.safety_or_determinism)
        .all(|assertion| assertion.passed);
    let all_conformance_pass = passed_scenarios == EXPECTED_FR_013_SCENARIOS
        && passed_assertions == assertion_count
        && scenarios
            .iter()
            .all(|scenario| !scenario.assertions.is_empty());
    let authoring_simplicity_pass = fr_012.iter().all(|predicate| predicate.passed);
    let has_in_scope_limitation = revision_contract.iter().any(|predicate| !predicate.passed);
    let classification = classify_feasibility(FeasibilityInputs {
        safety_and_determinism_pass,
        all_conformance_pass,
        authoring_simplicity_pass,
        has_in_scope_limitation,
    });

    println!(
        "INPUT safety_and_determinism_pass={safety_and_determinism_pass} all_conformance_pass={all_conformance_pass} authoring_simplicity_pass={authoring_simplicity_pass} has_in_scope_limitation={has_in_scope_limitation}"
    );
    println!(
        "CLASSIFICATION value={}",
        classification_name(classification)
    );
}

#[test]
fn fr_014_classifier_matches_all_truth_table_cases() {
    for safety_and_determinism_pass in [false, true] {
        for all_conformance_pass in [false, true] {
            for authoring_simplicity_pass in [false, true] {
                for has_in_scope_limitation in [false, true] {
                    let inputs = FeasibilityInputs {
                        safety_and_determinism_pass,
                        all_conformance_pass,
                        authoring_simplicity_pass,
                        has_in_scope_limitation,
                    };
                    let expected = if !safety_and_determinism_pass {
                        FeasibilityClassification::Infeasible
                    } else if !all_conformance_pass
                        || !authoring_simplicity_pass
                        || has_in_scope_limitation
                    {
                        FeasibilityClassification::ConditionallyFeasible
                    } else {
                        FeasibilityClassification::Feasible
                    };
                    assert_eq!(classify_feasibility(inputs), expected, "{inputs:?}");
                }
            }
        }
    }
}

#[test]
fn non_safety_conformance_failure_is_conditionally_feasible() {
    let mut scenarios = block_on(run_conformance_matrix());
    let size_evidence = scenarios
        .iter_mut()
        .find(|scenario| scenario.id == ScenarioId::Base64ExactBytes)
        .unwrap()
        .assertions
        .iter_mut()
        .find(|assertion| !assertion.safety_or_determinism)
        .unwrap();
    size_evidence.passed = false;

    let safety_and_determinism_pass = scenarios
        .iter()
        .flat_map(|scenario| &scenario.assertions)
        .filter(|assertion| assertion.safety_or_determinism)
        .all(|assertion| assertion.passed);
    let all_conformance_pass = scenarios.iter().all(ScenarioEvidence::passed);
    assert!(safety_and_determinism_pass);
    assert!(!all_conformance_pass);
    assert_eq!(
        classify_feasibility(FeasibilityInputs {
            safety_and_determinism_pass,
            all_conformance_pass,
            authoring_simplicity_pass: true,
            has_in_scope_limitation: false,
        }),
        FeasibilityClassification::ConditionallyFeasible
    );
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RbacDocument {
    api_version: String,
    kind: String,
    metadata: RbacMetadata,
    #[serde(default)]
    rules: Vec<RbacRule>,
    role_ref: Option<RbacRoleRef>,
    #[serde(default)]
    subjects: Vec<RbacSubject>,
}

#[derive(Debug, Deserialize)]
struct RbacMetadata {
    name: String,
    namespace: String,
    #[serde(default)]
    annotations: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RbacRule {
    api_groups: Vec<String>,
    resources: Vec<String>,
    verbs: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RbacRoleRef {
    api_group: String,
    kind: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct RbacSubject {
    kind: String,
    name: String,
    namespace: String,
}

fn parse_rbac_documents(source: &str) -> Vec<RbacDocument> {
    serde_yaml_ng::Deserializer::from_str(source)
        .map(|document| RbacDocument::deserialize(document).expect("valid RBAC YAML document"))
        .collect()
}

fn assert_least_privilege_rbac(
    source: &str,
    expected_name: &str,
    expected_lifecycle: &str,
    expected_verbs: &[&str],
) {
    let documents = parse_rbac_documents(source);
    assert_eq!(documents.len(), 3);
    assert_eq!(
        documents
            .iter()
            .map(|document| document.kind.as_str())
            .collect::<Vec<_>>(),
        ["ServiceAccount", "Role", "RoleBinding"]
    );

    for document in &documents {
        assert_eq!(document.metadata.name, expected_name);
        assert_eq!(document.metadata.namespace, "kuberic-checkpoints");
        assert_eq!(
            document
                .metadata
                .annotations
                .get("kuberic.io/checkpoint-lifecycle")
                .map(String::as_str),
            Some(expected_lifecycle)
        );
        assert!(!document.api_version.contains('*'));
    }

    assert_eq!(documents[0].api_version, "v1");
    assert!(documents[0].rules.is_empty());
    assert!(documents[0].role_ref.is_none());
    assert!(documents[0].subjects.is_empty());

    let role = &documents[1];
    assert_eq!(role.api_version, "rbac.authorization.k8s.io/v1");
    assert_eq!(role.rules.len(), 1);
    assert_eq!(role.rules[0].api_groups, [""]);
    assert_eq!(role.rules[0].resources, ["configmaps"]);
    assert_eq!(
        role.rules[0].verbs,
        expected_verbs
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    );
    assert!(
        role.rules[0]
            .api_groups
            .iter()
            .chain(&role.rules[0].resources)
            .chain(&role.rules[0].verbs)
            .all(|entry| entry != "*")
    );

    let binding = &documents[2];
    assert_eq!(binding.api_version, "rbac.authorization.k8s.io/v1");
    assert!(binding.rules.is_empty());
    let role_ref = binding.role_ref.as_ref().expect("RoleBinding roleRef");
    assert_eq!(role_ref.api_group, "rbac.authorization.k8s.io");
    assert_eq!(role_ref.kind, "Role");
    assert_eq!(role_ref.name, expected_name);
    assert_eq!(binding.subjects.len(), 1);
    assert_eq!(binding.subjects[0].kind, "ServiceAccount");
    assert_eq!(binding.subjects[0].name, expected_name);
    assert_eq!(binding.subjects[0].namespace, "kuberic-checkpoints");
}

#[test]
fn checkpoint_rbac_examples_are_structural_and_lifecycle_specific() {
    assert_least_privilege_rbac(
        include_str!("../deploy/checkpoint-writer-rbac.yaml"),
        "kuberic-checkpoint-writer",
        "retained-writer",
        &["get", "create", "update"],
    );
    assert_least_privilege_rbac(
        include_str!("../deploy/checkpoint-cleanup-rbac.yaml"),
        "kuberic-checkpoint-cleanup",
        "explicit-orphan-cleanup",
        &["list", "delete"],
    );
}

#[test]
fn checkpoint_provider_readiness_contract_is_user_visible() {
    let readme = include_str!("../README.md");
    let roadmap = include_str!("../../docs/features/kuberic/durable-execution-roadmap.md");
    let workflow = include_str!("../../.github/workflows/CI.yml");
    let real_test = include_str!("kubernetes_checkpoint_real.rs");

    for required in [
        "independently retained checkpoints",
        "786,432-byte ConfigMap data budget",
        "1 through 983,040",
        "metadata, managed fields",
        "owner references, admission mutation, and API-server policy",
        "checkpoint-writer-rbac.yaml",
        "checkpoint-cleanup-rbac.yaml",
        "feature do not select this test",
    ] {
        assert!(
            readme.contains(required),
            "provider documentation must retain {required:?}"
        );
    }
    for required in [
        "retention contract",
        "separately authorized",
        "configurable 786,432-byte default",
        "operator workflow pilot",
        "switchover",
        "workflow-ownership change",
    ] {
        assert!(
            roadmap.contains(required),
            "roadmap must retain {required:?}"
        );
    }
    let kind_step = workflow
        .find("uses: helm/kind-action@v1")
        .expect("existing KinD action");
    let checkpoint_step = workflow
        .find("name: Run cargo test")
        .expect("existing workspace test step");
    assert!(kind_step < checkpoint_step);
    assert_eq!(workflow.matches("uses: helm/kind-action@v1").count(), 1);
    assert!(!workflow.contains("name: Run real Kubernetes checkpoint test"));
    assert!(workflow.contains("cargo test --all --all-features"));
    assert!(!workflow.contains("kubernetes_checkpoint_real -- --nocapture"));
    assert!(!real_test.contains("#[ignore"));
}

const fn status(passed: bool) -> &'static str {
    if passed { "pass" } else { "fail" }
}

const fn classification_name(classification: FeasibilityClassification) -> &'static str {
    match classification {
        FeasibilityClassification::Feasible => "feasible",
        FeasibilityClassification::ConditionallyFeasible => "conditionally feasible",
        FeasibilityClassification::Infeasible => "infeasible",
    }
}
