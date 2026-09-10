use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use kuberic_dex::{
    ActivityCallError, ActivityName, ActivityOptions, ActivityRecord, ActivitySequence,
    ActivitySpec, ActivityState, AttemptId, CheckpointEnvelope, CheckpointError, CheckpointLimits,
    CheckpointPayload, CompletionClass, DurableActivity, DurableEffect, EffectActivity,
    EffectAttempt, EffectAttemptState, EffectMetadata, EffectOutcome, Evaluation, ExactBytes,
    ExecutionContract, ExecutionId, ExecutionSpec, HostEpoch, IdentityError, LogicalActivityId,
    Nondeterminism, PreparedActivityError, PreparedActivityResolver, PreparedCommand,
    PreparedEffectResolver, TerminalOutcome, Workflow, WorkflowContext, encode_activity_input,
    encode_activity_result, evaluate as evaluate_with_spec, evaluate_effects, evaluate_prepared,
};
use serde::{Deserialize, Serialize};

const MAX_RESULT_BYTES: u64 = 1024;

struct Cell(AtomicUsize);

impl Cell {
    fn new(value: usize) -> Self {
        Self(AtomicUsize::new(value))
    }

    fn get(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }

    fn set(&self, value: usize) {
        self.0.store(value, Ordering::Relaxed);
    }
}

fn execution(byte: u8) -> ExecutionId {
    ExecutionId::from_bytes([byte; 16])
}

fn name(value: &str, version: u32) -> ActivityName {
    ActivityName::new(value, version).unwrap()
}

fn bytes(value: &[u8]) -> ExactBytes {
    ExactBytes::new(value)
}

fn spec(value: &str, version: u32, input: &[u8]) -> ActivitySpec {
    ActivitySpec::new(name(value, version), bytes(input), MAX_RESULT_BYTES)
}

fn limits() -> CheckpointLimits {
    CheckpointLimits::new(128, 1_000_000, 1_000_000).unwrap()
}

#[derive(Clone)]
struct LinearWorkflow {
    activities: Vec<ActivitySpec>,
}

#[async_trait]
impl Workflow for LinearWorkflow {
    async fn run(&self, context: &mut WorkflowContext<'_>, _input: ExactBytes) -> TerminalOutcome {
        let mut results = Vec::new();
        for spec in &self.activities {
            results.extend(context.activity(spec.clone()).await.as_slice());
        }
        TerminalOutcome::succeeded(ExactBytes::new(results))
    }
}

fn execution_spec(execution_id: ExecutionId, workflow_input: ExactBytes) -> ExecutionSpec {
    ExecutionSpec::new(execution_id, workflow_input, MAX_RESULT_BYTES)
}

fn evaluate<W: Workflow>(
    workflow: &W,
    execution_id: ExecutionId,
    workflow_input: ExactBytes,
    checkpoint: Option<&CheckpointEnvelope>,
    limits: CheckpointLimits,
) -> Evaluation {
    evaluate_with_spec(
        workflow,
        &execution_spec(execution_id, workflow_input),
        checkpoint,
        limits,
    )
}

fn envelope(
    execution_id: ExecutionId,
    workflow_input: ExactBytes,
    activities: Vec<ActivityRecord>,
) -> CheckpointEnvelope {
    CheckpointEnvelope::encode(&CheckpointPayload::active(
        ExecutionContract::new(
            execution_spec(execution_id, workflow_input),
            limits().max_active_encoded_bytes() as u64,
        ),
        activities,
    ))
    .unwrap()
}

#[test]
fn first_turn_schedules_exactly_one_activity() {
    let workflow = LinearWorkflow {
        activities: vec![spec("greeting", 1, b"A")],
    };

    let Evaluation::Scheduled {
        activity,
        checkpoint,
    } = evaluate(&workflow, execution(1), bytes(b"workflow"), None, limits())
    else {
        panic!("first turn did not schedule");
    };

    assert_eq!(activity.execution_id(), execution(1));
    assert_eq!(activity.sequence(), ActivitySequence::new(0));
    assert_eq!(activity.name(), &name("greeting", 1));
    assert_eq!(activity.input(), &bytes(b"A"));
    assert_eq!(activity.max_result_bytes(), MAX_RESULT_BYTES);
    let payload = checkpoint
        .decode_and_validate(&execution_spec(execution(1), bytes(b"workflow")), limits())
        .unwrap();
    assert_eq!(payload.active_activities().unwrap().len(), 1);
    assert_eq!(
        payload.active_activities().unwrap()[0].state(),
        &ActivityState::Scheduled
    );
}

#[test]
fn completed_result_replays_without_rescheduling() {
    let workflow = LinearWorkflow {
        activities: vec![spec("greeting", 1, b"A")],
    };
    let checkpoint = envelope(
        execution(2),
        bytes(b"workflow"),
        vec![ActivityRecord::completed(
            ActivitySequence::new(0),
            spec("greeting", 1, b"A"),
            bytes(b"recorded-result"),
        )],
    );

    let first = evaluate(
        &workflow,
        execution(2),
        bytes(b"workflow"),
        Some(&checkpoint),
        limits(),
    );
    let second = evaluate(
        &workflow,
        execution(2),
        bytes(b"workflow"),
        Some(&checkpoint),
        limits(),
    );

    assert_eq!(first, second);
    let Evaluation::Complete {
        outcome,
        checkpoint: replayed,
        ..
    } = first
    else {
        panic!("completed history did not complete");
    };
    assert_eq!(
        outcome,
        TerminalOutcome::succeeded(bytes(b"recorded-result"))
    );
    assert_eq!(
        replayed
            .decode_and_validate(&execution_spec(execution(2), bytes(b"workflow")), limits(),)
            .unwrap(),
        checkpoint
            .decode_and_validate(&execution_spec(execution(2), bytes(b"workflow")), limits(),)
            .unwrap()
    );
}

#[test]
fn completed_prefix_can_schedule_one_next_activity() {
    let workflow = LinearWorkflow {
        activities: vec![spec("first", 1, b"A"), spec("second", 2, b"B")],
    };
    let checkpoint = envelope(
        execution(3),
        bytes(b"workflow"),
        vec![ActivityRecord::completed(
            ActivitySequence::new(0),
            spec("first", 1, b"A"),
            bytes(b"done"),
        )],
    );

    let Evaluation::Scheduled {
        activity,
        checkpoint,
    } = evaluate(
        &workflow,
        execution(3),
        bytes(b"workflow"),
        Some(&checkpoint),
        limits(),
    )
    else {
        panic!("next activity was not scheduled");
    };

    assert_eq!(activity.sequence(), ActivitySequence::new(1));
    let decoded = checkpoint
        .decode_and_validate(&execution_spec(execution(3), bytes(b"workflow")), limits())
        .unwrap();
    assert_eq!(decoded.active_activities().unwrap().len(), 2);
    assert_eq!(
        decoded.active_activities().unwrap()[1].state(),
        &ActivityState::Scheduled
    );
}

#[test]
fn changed_order_name_or_exact_input_is_nondeterminism() {
    let cases = [
        spec("second", 1, b"A"),
        spec("first", 2, b"A"),
        spec("first", 1, b"a"),
        ActivitySpec::new(name("first", 1), bytes(b"A"), MAX_RESULT_BYTES + 1),
    ];
    let checkpoint = envelope(
        execution(4),
        bytes(b"workflow"),
        vec![ActivityRecord::completed(
            ActivitySequence::new(0),
            spec("first", 1, b"A"),
            bytes(b"done"),
        )],
    );

    for requested in cases {
        let workflow = LinearWorkflow {
            activities: vec![requested],
        };
        assert!(matches!(
            evaluate(
                &workflow,
                execution(4),
                bytes(b"workflow"),
                Some(&checkpoint),
                limits()
            ),
            Evaluation::Nondeterminism(Nondeterminism::ActivityMismatch { .. })
        ));
    }
}

#[test]
fn unused_otherwise_valid_history_is_nondeterminism() {
    let workflow = LinearWorkflow { activities: vec![] };
    let checkpoint = envelope(
        execution(5),
        bytes(b"workflow"),
        vec![ActivityRecord::completed(
            ActivitySequence::new(0),
            spec("old", 1, b"A"),
            bytes(b"done"),
        )],
    );

    assert_eq!(
        evaluate(
            &workflow,
            execution(5),
            bytes(b"workflow"),
            Some(&checkpoint),
            limits()
        ),
        Evaluation::Nondeterminism(Nondeterminism::UnusedHistory {
            consumed: 0,
            remaining: 1,
        })
    );
}

#[test]
fn logical_identity_equality_and_rendering_cover_the_full_semantic_tuple() {
    let base = LogicalActivityId::new(
        execution(6),
        ActivitySequence::new(7),
        spec("write", 3, &[0, 1, 255]),
    );
    let same = LogicalActivityId::new(
        execution(6),
        ActivitySequence::new(7),
        spec("write", 3, &[0, 1, 255]),
    );
    assert_eq!(base, same);
    assert_eq!(base.to_external_id(), same.to_external_id());

    let variants = [
        LogicalActivityId::new(
            execution(7),
            ActivitySequence::new(7),
            spec("write", 3, &[0, 1, 255]),
        ),
        LogicalActivityId::new(
            execution(6),
            ActivitySequence::new(8),
            spec("write", 3, &[0, 1, 255]),
        ),
        LogicalActivityId::new(
            execution(6),
            ActivitySequence::new(7),
            spec("write-other", 3, &[0, 1, 255]),
        ),
        LogicalActivityId::new(
            execution(6),
            ActivitySequence::new(7),
            spec("write", 4, &[0, 1, 255]),
        ),
        LogicalActivityId::new(
            execution(6),
            ActivitySequence::new(7),
            spec("write", 3, &[0, 1, 254]),
        ),
        LogicalActivityId::new(
            execution(6),
            ActivitySequence::new(7),
            ActivitySpec::new(name("write", 3), bytes(&[0, 1, 255]), MAX_RESULT_BYTES + 1),
        ),
    ];
    for variant in variants {
        assert_ne!(base, variant);
        assert_ne!(base.to_external_id(), variant.to_external_id());
    }
    assert!(base.to_external_id().ends_with(":3:0001ff:1024"));
}

#[test]
fn changed_name_and_input_cannot_alias_a_logical_external_id() {
    let original = LogicalActivityId::new(
        execution(8),
        ActivitySequence::new(0),
        spec("effect", 1, b"input"),
    );
    let renamed = LogicalActivityId::new(
        execution(8),
        ActivitySequence::new(0),
        spec("effect", 2, b"input"),
    );
    let changed_input = LogicalActivityId::new(
        execution(8),
        ActivitySequence::new(0),
        spec("effect", 1, b"Input"),
    );

    assert_ne!(original.to_external_id(), renamed.to_external_id());
    assert_ne!(original.to_external_id(), changed_input.to_external_id());
}

#[test]
fn name_and_attempt_identifiers_reject_unversioned_or_reserved_values() {
    assert_eq!(
        ActivityName::new("", 1),
        Err(IdentityError::EmptyActivityName)
    );
    assert_eq!(
        ActivityName::new("effect", 0),
        Err(IdentityError::ZeroActivityVersion)
    );
    assert_eq!(
        AttemptId::new(HostEpoch::from_bytes([1; 16]), 0),
        Err(IdentityError::ZeroAttemptCounter)
    );
}

#[test]
fn rejects_invalid_sequence_and_pending_shapes() {
    let workflow = LinearWorkflow {
        activities: vec![spec("first", 1, b"A")],
    };
    let invalid_sequence = envelope(
        execution(9),
        bytes(b"workflow"),
        vec![ActivityRecord::completed(
            ActivitySequence::new(1),
            spec("first", 1, b"A"),
            bytes(b"done"),
        )],
    );
    assert!(matches!(
        evaluate(
            &workflow,
            execution(9),
            bytes(b"workflow"),
            Some(&invalid_sequence),
            limits()
        ),
        Evaluation::CheckpointRejected(CheckpointError::NonContiguousSequence { .. })
    ));

    let invalid_pending = envelope(
        execution(9),
        bytes(b"workflow"),
        vec![
            ActivityRecord::scheduled(ActivitySequence::new(0), spec("first", 1, b"A")),
            ActivityRecord::completed(
                ActivitySequence::new(1),
                spec("second", 1, b"B"),
                bytes(b"done"),
            ),
        ],
    );
    assert!(matches!(
        evaluate(
            &workflow,
            execution(9),
            bytes(b"workflow"),
            Some(&invalid_pending),
            limits()
        ),
        Evaluation::CheckpointRejected(CheckpointError::PendingActivityNotFinal { .. })
    ));
}

struct PollCountingWorkflow {
    polls: Cell,
}

#[async_trait]
impl Workflow for PollCountingWorkflow {
    async fn run(&self, _context: &mut WorkflowContext<'_>, _input: ExactBytes) -> TerminalOutcome {
        self.polls.set(self.polls.get() + 1);
        TerminalOutcome::succeeded(bytes(b"complete"))
    }
}

#[test]
fn identity_input_and_format_validation_happen_before_workflow_polling() {
    let workflow = PollCountingWorkflow {
        polls: Cell::new(0),
    };
    let valid_payload = CheckpointPayload::active(
        ExecutionContract::new(
            execution_spec(execution(10), bytes(b"expected")),
            limits().max_active_encoded_bytes() as u64,
        ),
        vec![],
    );
    let unsupported = CheckpointEnvelope::new(
        1,
        CheckpointEnvelope::encode(&valid_payload)
            .unwrap()
            .payload()
            .clone(),
    );

    assert!(matches!(
        evaluate(
            &workflow,
            execution(10),
            bytes(b"expected"),
            Some(&unsupported),
            limits()
        ),
        Evaluation::CheckpointRejected(CheckpointError::UnsupportedFormat { .. })
    ));
    assert_eq!(workflow.polls.get(), 0);

    assert!(matches!(
        evaluate(
            &workflow,
            execution(10),
            bytes(b"expected"),
            Some(&unsupported),
            CheckpointLimits::new(1, 1, 1).unwrap()
        ),
        Evaluation::CheckpointRejected(CheckpointError::UnsupportedFormat { .. })
    ));
    assert_eq!(workflow.polls.get(), 0);

    let wrong_execution = envelope(execution(11), bytes(b"expected"), vec![]);
    assert!(matches!(
        evaluate(
            &workflow,
            execution(10),
            bytes(b"expected"),
            Some(&wrong_execution),
            limits()
        ),
        Evaluation::CheckpointRejected(CheckpointError::ExecutionMismatch { .. })
    ));
    assert_eq!(workflow.polls.get(), 0);

    let wrong_input = envelope(execution(10), bytes(b"different"), vec![]);
    assert!(matches!(
        evaluate(
            &workflow,
            execution(10),
            bytes(b"expected"),
            Some(&wrong_input),
            limits()
        ),
        Evaluation::CheckpointRejected(CheckpointError::WorkflowInputMismatch { .. })
    ));
    assert_eq!(workflow.polls.get(), 0);
}

#[test]
fn configured_limits_reject_loaded_checkpoints_before_workflow_polling() {
    let workflow = PollCountingWorkflow {
        polls: Cell::new(0),
    };
    let execution_id = execution(12);
    let input = bytes(b"workflow");
    let oversized_history = envelope(
        execution_id,
        input.clone(),
        vec![
            ActivityRecord::completed(
                ActivitySequence::new(0),
                spec("first", 1, b"A"),
                bytes(b"one"),
            ),
            ActivityRecord::completed(
                ActivitySequence::new(1),
                spec("second", 1, b"B"),
                bytes(b"two"),
            ),
        ],
    );
    assert!(matches!(
        evaluate(
            &workflow,
            execution_id,
            input.clone(),
            Some(&oversized_history),
            CheckpointLimits::new(1, 1_000_000, 1_000_000).unwrap(),
        ),
        Evaluation::CheckpointRejected(CheckpointError::ActivityRecordLimitExceeded {
            actual: 2,
            maximum: 1
        })
    ));
    assert_eq!(workflow.polls.get(), 0);

    let empty = envelope(execution_id, input.clone(), Vec::new());
    let exact_encoded = empty.encoded_len().unwrap();
    assert!(matches!(
        evaluate(
            &workflow,
            execution_id,
            input.clone(),
            Some(&empty),
            CheckpointLimits::new(1, exact_encoded - 1, exact_encoded - 1).unwrap(),
        ),
        Evaluation::CheckpointRejected(CheckpointError::EncodedCheckpointLimitExceeded {
            actual,
            maximum
        }) if actual == exact_encoded && maximum + 1 == exact_encoded
    ));
    assert_eq!(workflow.polls.get(), 0);

    assert!(matches!(
        evaluate(&workflow, execution_id, input, Some(&empty), limits(),),
        Evaluation::Complete { .. }
    ));
    assert_eq!(workflow.polls.get(), 1);
}

#[test]
fn active_and_terminal_encoded_limits_are_enforced_independently() {
    let execution_id = execution(14);
    let execution = execution_spec(execution_id, bytes(b"independent-limits"));
    let terminal_limit = 20_000_u64;
    let mut active_limit = 1_000_000_u64;
    let active = loop {
        let payload = CheckpointPayload::active(
            ExecutionContract::with_encoded_limits(execution.clone(), active_limit, terminal_limit),
            vec![ActivityRecord::completed(
                ActivitySequence::new(0),
                spec("bounded", 1, b"input"),
                bytes(b"result"),
            )],
        );
        let envelope = CheckpointEnvelope::encode(&payload).unwrap();
        let encoded = u64::try_from(envelope.encoded_len().unwrap()).unwrap();
        if encoded == active_limit {
            break envelope;
        }
        active_limit = encoded;
    };
    let active_limit = usize::try_from(active_limit).unwrap();
    let terminal_limit = usize::try_from(terminal_limit).unwrap();
    assert!(
        active
            .decode_and_validate(
                &execution,
                CheckpointLimits::new(1, active_limit, terminal_limit).unwrap(),
            )
            .is_ok()
    );
    assert!(matches!(
        active.decode_and_validate(
            &execution,
            CheckpointLimits::new(1, active_limit - 1, terminal_limit).unwrap(),
        ),
        Err(CheckpointError::EncodedCheckpointLimitExceeded { actual, maximum })
            if actual == active_limit && maximum + 1 == active_limit
    ));

    let mut admitted_terminal = 20_000_u64;
    let (terminal, terminal_limit) = loop {
        let contract =
            ExecutionContract::with_encoded_limits(execution.clone(), 100_000, admitted_terminal);
        let active = CheckpointPayload::active(contract.clone(), Vec::new());
        let required = u64::try_from(active.maximum_terminal_encoded_len().unwrap()).unwrap();
        if required != admitted_terminal {
            admitted_terminal = required;
            continue;
        }
        let external = 10_000_000_000_000_000_000_u64;
        let metadata =
            kuberic_dex::CompletionMetadata::from_counts(u64::MAX, external, u64::MAX - external)
                .unwrap();
        let terminals = [
            CheckpointPayload::terminal(
                contract.clone(),
                TerminalOutcome::succeeded(vec![7; MAX_RESULT_BYTES as usize]),
                u64::MAX,
            ),
            CheckpointPayload::terminal(
                contract.clone(),
                TerminalOutcome::failed(vec![7; MAX_RESULT_BYTES as usize]),
                u64::MAX,
            ),
            CheckpointPayload::terminal_with_metadata(
                contract.clone(),
                TerminalOutcome::succeeded(vec![7; MAX_RESULT_BYTES as usize]),
                metadata,
            ),
            CheckpointPayload::terminal_with_metadata(
                contract,
                TerminalOutcome::failed(vec![7; MAX_RESULT_BYTES as usize]),
                metadata,
            ),
        ];
        let terminal = terminals
            .into_iter()
            .map(|payload| CheckpointEnvelope::encode(&payload).unwrap())
            .max_by_key(|envelope| envelope.encoded_len().unwrap())
            .unwrap();
        break (terminal, usize::try_from(admitted_terminal).unwrap());
    };
    assert_eq!(terminal.encoded_len().unwrap(), terminal_limit);
    assert!(
        terminal
            .decode_and_validate(
                &execution,
                CheckpointLimits::new(1, 100_000, terminal_limit).unwrap(),
            )
            .is_ok()
    );
    assert!(matches!(
        terminal.decode_and_validate(
            &execution,
            CheckpointLimits::new(1, 100_000, terminal_limit - 1).unwrap(),
        ),
        Err(CheckpointError::TerminalEncodedCheckpointLimitExceeded { actual, maximum })
            if actual == terminal_limit && maximum + 1 == terminal_limit
    ));
}

#[test]
fn legacy_single_limit_contract_and_versioned_limits_fail_closed_before_polling() {
    let workflow = PollCountingWorkflow {
        polls: Cell::new(0),
    };
    let execution = execution_spec(execution(15), bytes(b"compatibility"));
    let legacy_limit = 100_000;
    let legacy = CheckpointEnvelope::encode(&CheckpointPayload::active(
        ExecutionContract::new(execution.clone(), legacy_limit),
        Vec::new(),
    ))
    .unwrap();
    assert!(matches!(
        evaluate_with_spec(
            &workflow,
            &execution,
            Some(&legacy),
            CheckpointLimits::new(1, legacy_limit as usize, legacy_limit as usize).unwrap(),
        ),
        Evaluation::Complete { .. }
    ));
    assert_eq!(workflow.polls.get(), 1);

    let versioned = CheckpointEnvelope::encode(&CheckpointPayload::active(
        ExecutionContract::with_encoded_limits(execution.clone(), 262_144, 12_288),
        Vec::new(),
    ))
    .unwrap();
    let versioned_workflow = PollCountingWorkflow {
        polls: Cell::new(0),
    };
    assert!(matches!(
        evaluate_with_spec(
            &versioned_workflow,
            &execution,
            Some(&versioned),
            CheckpointLimits::new(1, 262_144, 12_289).unwrap(),
        ),
        Evaluation::CheckpointRejected(
            CheckpointError::TerminalEncodedCheckpointCapacityMismatch {
                configured: 12_289,
                admitted: 12_288,
            }
        )
    ));
    assert_eq!(versioned_workflow.polls.get(), 0);

    let active_limit_changed_workflow = PollCountingWorkflow {
        polls: Cell::new(0),
    };
    assert!(matches!(
        evaluate_with_spec(
            &active_limit_changed_workflow,
            &execution,
            Some(&versioned),
            CheckpointLimits::new(1, 262_145, 12_288).unwrap(),
        ),
        Evaluation::CheckpointRejected(CheckpointError::ConfiguredCapacityAboveAdmission {
            configured: 262_145,
            admitted: 262_144,
        })
    ));
    assert_eq!(active_limit_changed_workflow.polls.get(), 0);
}

#[test]
fn production_terminal_ceiling_rejects_a_record_still_below_the_active_ceiling() {
    let execution = ExecutionSpec::new(execution(16), bytes(b"production-terminal-limit"), 16_384);
    let terminal = CheckpointEnvelope::encode(&CheckpointPayload::terminal(
        ExecutionContract::with_encoded_limits(execution.clone(), 262_144, 262_144),
        TerminalOutcome::succeeded(vec![7; 9_000]),
        16,
    ))
    .unwrap();
    let encoded = terminal.encoded_len().unwrap();
    assert!(encoded > 12_288);
    assert!(encoded < 262_144);

    let workflow = PollCountingWorkflow {
        polls: Cell::new(0),
    };
    assert!(matches!(
        evaluate_with_spec(
            &workflow,
            &execution,
            Some(&terminal),
            CheckpointLimits::new(16, 262_144, 12_288).unwrap(),
        ),
        Evaluation::CheckpointRejected(
            CheckpointError::TerminalEncodedCheckpointLimitExceeded {
                actual,
                maximum: 12_288,
            }
        ) if actual == encoded
    ));
    assert_eq!(workflow.polls.get(), 0);
}

#[test]
fn loaded_completed_result_must_respect_its_declared_bound() {
    let workflow = PollCountingWorkflow {
        polls: Cell::new(0),
    };
    let execution_id = execution(13);
    let input = bytes(b"workflow");
    let checkpoint = envelope(
        execution_id,
        input.clone(),
        vec![ActivityRecord::completed(
            ActivitySequence::new(0),
            ActivitySpec::new(name("bounded", 1), bytes(b"input"), 2),
            bytes(b"three"),
        )],
    );

    assert!(matches!(
        evaluate(&workflow, execution_id, input, Some(&checkpoint), limits(),),
        Evaluation::CheckpointRejected(CheckpointError::CompletedResultExceedsDeclared {
            actual: 5,
            maximum: 2,
            ..
        })
    ));
    assert_eq!(workflow.polls.get(), 0);
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum TypedOutcome {
    Applied { value: String },
    Rejected { code: u16 },
}

struct TypedEffectV1;

impl DurableActivity for TypedEffectV1 {
    type Input = String;
    type Output = TypedOutcome;

    const NAME: &'static str = "typed.effect";
    const VERSION: u32 = 1;
    const MAX_INPUT_BYTES: u64 = 32;
    const MAX_RESULT_BYTES: u64 = 128;
}

struct TypedEffectV2;

impl DurableActivity for TypedEffectV2 {
    type Input = String;
    type Output = TypedOutcome;

    const NAME: &'static str = "typed.effect";
    const VERSION: u32 = 2;
    const MAX_INPUT_BYTES: u64 = 32;
    const MAX_RESULT_BYTES: u64 = 128;
}

struct TypedWorkflowV1;
struct TypedWorkflowV2;

async fn run_typed<A: DurableActivity<Input = String, Output = TypedOutcome>>(
    context: &mut WorkflowContext<'_>,
) -> TerminalOutcome {
    match context.call::<A>("hello".to_owned()).await {
        Ok(outcome) => TerminalOutcome::succeeded(serde_json::to_vec(&outcome).unwrap()),
        Err(error) => TerminalOutcome::failed(serde_json::to_vec(&error).unwrap()),
    }
}

#[async_trait]
impl Workflow for TypedWorkflowV1 {
    async fn run(&self, context: &mut WorkflowContext<'_>, _input: ExactBytes) -> TerminalOutcome {
        run_typed::<TypedEffectV1>(context).await
    }
}

#[async_trait]
impl Workflow for TypedWorkflowV2 {
    async fn run(&self, context: &mut WorkflowContext<'_>, _input: ExactBytes) -> TerminalOutcome {
        run_typed::<TypedEffectV2>(context).await
    }
}

fn typed_spec<A: DurableActivity<Input = String>>(input: &str) -> ActivitySpec {
    ActivitySpec::with_bounds_and_options(
        ActivityName::new(A::NAME, A::VERSION).unwrap(),
        encode_activity_input::<A>(&input.to_owned()).unwrap(),
        A::MAX_INPUT_BYTES,
        A::MAX_RESULT_BYTES,
        ActivityOptions::default(),
    )
}

#[test]
fn typed_call_schedules_canonical_input_and_immutable_identity() {
    let Evaluation::Scheduled { activity, .. } = evaluate(
        &TypedWorkflowV1,
        execution(14),
        bytes(b"workflow"),
        None,
        limits(),
    ) else {
        panic!("typed call did not schedule");
    };

    assert_eq!(activity.name(), &name("typed.effect", 1));
    assert_eq!(
        activity.input(),
        &encode_activity_input::<TypedEffectV1>(&"hello".to_owned()).unwrap()
    );
    assert_eq!(activity.max_result_bytes(), 128);
}

#[test]
fn typed_domain_failure_is_a_bounded_completed_output() {
    let execution_id = execution(15);
    let workflow_input = bytes(b"workflow");
    let rejection = TypedOutcome::Rejected { code: 409 };
    let checkpoint = envelope(
        execution_id,
        workflow_input.clone(),
        vec![ActivityRecord::completed(
            ActivitySequence::new(0),
            typed_spec::<TypedEffectV1>("hello"),
            encode_activity_result::<TypedEffectV1>(&rejection).unwrap(),
        )],
    );

    let Evaluation::Complete { outcome, .. } = evaluate(
        &TypedWorkflowV1,
        execution_id,
        workflow_input,
        Some(&checkpoint),
        limits(),
    ) else {
        panic!("typed rejection did not replay as a completed output");
    };
    let TerminalOutcome::Succeeded(payload) = outcome else {
        panic!("domain rejection incorrectly entered the kernel failure lifecycle");
    };
    assert_eq!(
        serde_json::from_slice::<TypedOutcome>(payload.as_slice()).unwrap(),
        rejection
    );
}

#[test]
fn typed_identity_change_remains_nondeterminism() {
    let execution_id = execution(16);
    let workflow_input = bytes(b"workflow");
    let checkpoint = envelope(
        execution_id,
        workflow_input.clone(),
        vec![ActivityRecord::completed(
            ActivitySequence::new(0),
            typed_spec::<TypedEffectV1>("hello"),
            encode_activity_result::<TypedEffectV1>(&TypedOutcome::Applied {
                value: "done".to_owned(),
            })
            .unwrap(),
        )],
    );

    assert!(matches!(
        evaluate(
            &TypedWorkflowV2,
            execution_id,
            workflow_input,
            Some(&checkpoint),
            limits()
        ),
        Evaluation::Nondeterminism(Nondeterminism::ActivityMismatch { .. })
    ));
}

struct WorkflowEffect;

impl DurableEffect for WorkflowEffect {
    type Request = String;
    type Command = String;
    type Output = String;

    const NAME: &'static str = "typed.workflow.effect";
    const VERSION: u32 = 1;
    const MAX_REQUEST_BYTES: u64 = 32;
    const MAX_COMMAND_BYTES: u64 = 32;
    const MAX_RESULT_BYTES: u64 = 128;
    const MAX_ERROR_MESSAGE_BYTES: u64 = 64;
    const COMPLETION_CLASS: CompletionClass = CompletionClass::ExternalEffect;
}

struct EffectWorkflow;

struct ExactEffectResolver;

impl PreparedEffectResolver for ExactEffectResolver {
    fn resolve(
        &self,
        _execution_id: ExecutionId,
        _logical: &ActivitySpec,
        metadata: EffectMetadata,
        recorded: Option<&PreparedCommand>,
    ) -> Result<PreparedCommand, PreparedActivityError> {
        if let Some(recorded) = recorded {
            return Ok(recorded.clone());
        }
        PreparedCommand::new(
            ExactBytes::new(br#"{"command":"exact"}"#),
            metadata.max_command_bytes(),
        )
        .map_err(|_| PreparedActivityError::Encoding)
    }
}

async fn effect_body(context: &mut WorkflowContext<'_>) -> Result<String, String> {
    let outcome = context
        .schedule_activity_typed::<EffectActivity<WorkflowEffect>>(
            WorkflowEffect::NAME,
            &"hello".to_owned(),
            ActivityOptions::default(),
        )
        .await
        .map_err(|error| error.to_string())?;
    let value = outcome
        .into_workflow_result(WorkflowEffect::MAX_ERROR_MESSAGE_BYTES)
        .map_err(|error| error.to_string())?;
    Ok(format!("{value}!"))
}

#[async_trait]
impl Workflow for EffectWorkflow {
    async fn run(&self, context: &mut WorkflowContext<'_>, _input: ExactBytes) -> TerminalOutcome {
        match effect_body(context).await {
            Ok(value) => TerminalOutcome::succeeded(value.into_bytes()),
            Err(error) => TerminalOutcome::failed(error.to_string().into_bytes()),
        }
    }
}

#[test]
fn typed_effect_call_supports_standard_error_propagation() {
    let execution_id = execution(26);
    let workflow_input = bytes(b"workflow");
    let checkpoint = envelope(
        execution_id,
        workflow_input.clone(),
        vec![
            ActivityRecord::prepared_effect(
                ActivitySequence::new(0),
                typed_spec::<EffectActivity<WorkflowEffect>>("hello"),
                PreparedCommand::new(ExactBytes::new(br#"{"command":"exact"}"#), 32).unwrap(),
                CompletionClass::ExternalEffect,
                vec![EffectAttempt::new(
                    AttemptId::new(HostEpoch::from_bytes([8; 16]), 1).unwrap(),
                    EffectAttemptState::Observed,
                )],
                ActivityState::Completed {
                    result: encode_activity_result::<EffectActivity<WorkflowEffect>>(
                        &EffectOutcome::Applied("done".to_owned()),
                    )
                    .unwrap(),
                },
            )
            .unwrap(),
        ],
    );

    let Evaluation::Complete { outcome, .. } = evaluate_effects(
        &EffectWorkflow,
        &ExecutionSpec::new(execution_id, workflow_input, 1024),
        Some(&checkpoint),
        limits(),
        &ExactEffectResolver,
    ) else {
        panic!("typed effect did not replay to completion");
    };
    assert_eq!(
        outcome,
        TerminalOutcome::succeeded(ExactBytes::new(b"done!"))
    );
}

#[test]
fn malformed_typed_result_is_a_deterministic_portable_call_error() {
    let execution_id = execution(17);
    let workflow_input = bytes(b"workflow");
    let checkpoint = envelope(
        execution_id,
        workflow_input.clone(),
        vec![ActivityRecord::completed(
            ActivitySequence::new(0),
            typed_spec::<TypedEffectV1>("hello"),
            bytes(b"not-json"),
        )],
    );

    let first = evaluate(
        &TypedWorkflowV1,
        execution_id,
        workflow_input.clone(),
        Some(&checkpoint),
        limits(),
    );
    let second = evaluate(
        &TypedWorkflowV1,
        execution_id,
        workflow_input,
        Some(&checkpoint),
        limits(),
    );
    assert_eq!(first, second);
    let Evaluation::Complete {
        outcome: TerminalOutcome::Failed(payload),
        ..
    } = first
    else {
        panic!("malformed typed result did not fail deterministically");
    };
    assert_eq!(
        serde_json::from_slice::<ActivityCallError>(payload.as_slice()).unwrap(),
        ActivityCallError::ResultDecoding
    );
}

struct TinyInput;

impl DurableActivity for TinyInput {
    type Input = String;
    type Output = String;

    const NAME: &'static str = "typed.tiny";
    const VERSION: u32 = 1;
    const MAX_INPUT_BYTES: u64 = 4;
    const MAX_RESULT_BYTES: u64 = 4;
}

struct OversizedTypedInputWorkflow;

#[async_trait]
impl Workflow for OversizedTypedInputWorkflow {
    async fn run(&self, context: &mut WorkflowContext<'_>, _input: ExactBytes) -> TerminalOutcome {
        match context.call::<TinyInput>("too large".to_owned()).await {
            Ok(_) => TerminalOutcome::succeeded([]),
            Err(error) => TerminalOutcome::failed(serde_json::to_vec(&error).unwrap()),
        }
    }
}

#[test]
fn typed_input_and_result_bounds_fail_before_persistence() {
    let Evaluation::Complete {
        outcome: TerminalOutcome::Failed(payload),
        completed_activity_count,
        ..
    } = evaluate(
        &OversizedTypedInputWorkflow,
        execution(18),
        bytes(b"workflow"),
        None,
        limits(),
    )
    else {
        panic!("oversized typed input did not fail before scheduling");
    };
    assert_eq!(completed_activity_count, 0);
    assert!(matches!(
        serde_json::from_slice::<ActivityCallError>(payload.as_slice()).unwrap(),
        ActivityCallError::InputTooLarge { max_bytes: 4, .. }
    ));

    assert!(matches!(
        encode_activity_result::<TinyInput>(&"large".to_owned()),
        Err(ActivityCallError::ResultTooLarge {
            actual_bytes: 7,
            max_bytes: 4
        })
    ));
}

#[test]
fn typed_call_errors_have_stable_portable_encoding() {
    let error = ActivityCallError::InputTooLarge {
        actual_bytes: 9,
        max_bytes: 4,
    };
    let encoded = serde_json::to_vec(&error).unwrap();
    assert_eq!(
        serde_json::from_slice::<ActivityCallError>(&encoded).unwrap(),
        error
    );
    assert!(encoded.len() < 128);
}

struct MapInputActivity;

impl DurableActivity for MapInputActivity {
    type Input = HashMap<String, u64>;
    type Output = ();

    const NAME: &'static str = "typed.map-input";
    const VERSION: u32 = 1;
    const MAX_INPUT_BYTES: u64 = 128;
    const MAX_RESULT_BYTES: u64 = 4;
}

#[test]
fn typed_input_encoding_canonicalizes_object_key_order() {
    let mut first = HashMap::new();
    first.insert("zeta".to_owned(), 1);
    first.insert("alpha".to_owned(), 2);

    let mut second = HashMap::new();
    second.insert("alpha".to_owned(), 2);
    second.insert("zeta".to_owned(), 1);

    let first = encode_activity_input::<MapInputActivity>(&first).unwrap();
    let second = encode_activity_input::<MapInputActivity>(&second).unwrap();
    assert_eq!(first, second);
    assert_eq!(first.as_slice(), br#"{"alpha":2,"zeta":1}"#);
}

#[derive(Clone)]
struct TypedPreparedResolver {
    command: &'static [u8],
    target: &'static [u8],
    result_bound: u64,
}

impl PreparedActivityResolver for TypedPreparedResolver {
    fn resolve(
        &self,
        logical: &ActivitySpec,
        _recorded: Option<&ActivitySpec>,
    ) -> Result<ActivitySpec, PreparedActivityError> {
        let mut prepared = Vec::new();
        prepared.extend_from_slice(b"command=");
        prepared.extend_from_slice(self.command);
        prepared.extend_from_slice(b";target=");
        prepared.extend_from_slice(self.target);
        prepared.extend_from_slice(b";logical=");
        prepared.extend_from_slice(logical.input().as_slice());
        Ok(ActivitySpec::new(
            logical.name().clone(),
            ExactBytes::new(prepared),
            self.result_bound,
        ))
    }
}

fn typed_prepared_resolver() -> TypedPreparedResolver {
    TypedPreparedResolver {
        command: b"promote",
        target: b"replica-2:generation-7",
        result_bound: TypedEffectV1::MAX_RESULT_BYTES,
    }
}

#[test]
fn typed_prepared_spec_is_canonical_and_exact_replay_succeeds() {
    let resolver = typed_prepared_resolver();
    let execution_id = execution(41);
    let workflow_input = bytes(b"workflow");
    let Evaluation::Scheduled {
        activity,
        checkpoint,
    } = evaluate_prepared(
        &TypedWorkflowV1,
        &execution_spec(execution_id, workflow_input.clone()),
        None,
        limits(),
        &resolver,
    )
    else {
        panic!("prepared typed call did not schedule");
    };
    assert_eq!(
        activity.input().as_slice(),
        br#"command=promote;target=replica-2:generation-7;logical="hello""#
    );

    assert!(matches!(
        evaluate_prepared(
            &TypedWorkflowV1,
            &execution_spec(execution_id, workflow_input),
            Some(&checkpoint),
            limits(),
            &resolver,
        ),
        Evaluation::Pending {
            state: ActivityState::Scheduled,
            ..
        }
    ));
}

#[test]
fn typed_prepared_replay_rejects_each_complete_specification_mismatch() {
    let resolver = typed_prepared_resolver();
    let execution_id = execution(42);
    let workflow_input = bytes(b"workflow");
    let Evaluation::Scheduled { checkpoint, .. } = evaluate_prepared(
        &TypedWorkflowV1,
        &execution_spec(execution_id, workflow_input.clone()),
        None,
        limits(),
        &resolver,
    ) else {
        panic!("prepared typed call did not schedule");
    };

    let changed_resolvers = [
        TypedPreparedResolver {
            command: b"demote",
            ..resolver.clone()
        },
        TypedPreparedResolver {
            target: b"replica-3:generation-7",
            ..resolver.clone()
        },
        TypedPreparedResolver {
            result_bound: 127,
            ..resolver.clone()
        },
    ];
    for changed in changed_resolvers {
        assert!(matches!(
            evaluate_prepared(
                &TypedWorkflowV1,
                &execution_spec(execution_id, workflow_input.clone()),
                Some(&checkpoint),
                limits(),
                &changed,
            ),
            Evaluation::Nondeterminism(Nondeterminism::ActivityMismatch { .. })
        ));
    }

    assert!(matches!(
        evaluate_prepared(
            &TypedWorkflowV2,
            &execution_spec(execution_id, workflow_input.clone()),
            Some(&checkpoint),
            limits(),
            &resolver,
        ),
        Evaluation::Nondeterminism(Nondeterminism::ActivityMismatch { .. })
    ));

    struct ChangedLogicalInput;
    #[async_trait]
    impl Workflow for ChangedLogicalInput {
        async fn run(
            &self,
            context: &mut WorkflowContext<'_>,
            _input: ExactBytes,
        ) -> TerminalOutcome {
            match context.call::<TypedEffectV1>("changed".to_owned()).await {
                Ok(_) => TerminalOutcome::succeeded([]),
                Err(error) => TerminalOutcome::failed(serde_json::to_vec(&error).unwrap()),
            }
        }
    }
    assert!(matches!(
        evaluate_prepared(
            &ChangedLogicalInput,
            &execution_spec(execution_id, workflow_input),
            Some(&checkpoint),
            limits(),
            &resolver,
        ),
        Evaluation::Nondeterminism(Nondeterminism::ActivityMismatch { .. })
    ));
}

#[test]
fn typed_completed_prepared_activity_replays_its_exact_recorded_result_and_specification() {
    let resolver = typed_prepared_resolver();
    let execution_id = execution(44);
    let workflow_input = bytes(b"workflow");
    let logical = encode_activity_input::<TypedEffectV1>(&"hello".to_owned()).unwrap();
    let requested = ActivitySpec::new(
        ActivityName::new(TypedEffectV1::NAME, TypedEffectV1::VERSION).unwrap(),
        logical,
        TypedEffectV1::MAX_RESULT_BYTES,
    );
    let recorded = resolver.resolve(&requested, None).unwrap();
    let recorded_result = TypedOutcome::Applied {
        value: "recorded-result".to_owned(),
    };
    let terminal_result = serde_json::to_vec(&recorded_result).unwrap();
    let checkpoint = envelope(
        execution_id,
        workflow_input.clone(),
        vec![ActivityRecord::completed(
            ActivitySequence::new(0),
            recorded,
            encode_activity_result::<TypedEffectV1>(&recorded_result).unwrap(),
        )],
    );

    assert!(matches!(
        evaluate_prepared(
            &TypedWorkflowV1,
            &execution_spec(execution_id, workflow_input.clone()),
            Some(&checkpoint),
            limits(),
            &resolver,
        ),
        Evaluation::Complete {
            outcome: TerminalOutcome::Succeeded(ref result),
            ..
        } if result.as_slice() == terminal_result
    ));
    assert!(matches!(
        evaluate_prepared(
            &TypedWorkflowV1,
            &execution_spec(execution_id, workflow_input),
            Some(&checkpoint),
            limits(),
            &TypedPreparedResolver {
                command: b"demote",
                ..resolver
            },
        ),
        Evaluation::Nondeterminism(Nondeterminism::ActivityMismatch { .. })
    ));
}

struct RejectPrepared(PreparedActivityError);

impl PreparedActivityResolver for RejectPrepared {
    fn resolve(
        &self,
        _logical: &ActivitySpec,
        _recorded: Option<&ActivitySpec>,
    ) -> Result<ActivitySpec, PreparedActivityError> {
        Err(self.0.clone())
    }
}

#[test]
fn typed_prepared_failure_classes_reject_before_checkpoint_creation() {
    for error in [
        PreparedActivityError::Derivation,
        PreparedActivityError::Validation,
        PreparedActivityError::Encoding,
        PreparedActivityError::InputTooLarge {
            actual_bytes: 65,
            max_bytes: 64,
        },
        PreparedActivityError::ResultBoundTooLarge {
            actual_bytes: 129,
            max_bytes: 128,
        },
    ] {
        assert_eq!(
            evaluate_prepared(
                &TypedWorkflowV1,
                &execution_spec(execution(43), bytes(b"workflow")),
                None,
                limits(),
                &RejectPrepared(error.clone()),
            ),
            Evaluation::PreparationRejected(error)
        );
    }
}
