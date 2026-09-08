use std::task::{Context, Poll};

use futures::task::noop_waker_ref;
use thiserror::Error;

use crate::{
    ActivityRecord, ActivitySequence, ActivitySpec, ActivityState, CheckpointEnvelope,
    CheckpointError, CheckpointLimits, CheckpointPayload, ExecutionContract, ExecutionSpec,
    LogicalActivityId, TerminalOutcome, Workflow, WorkflowContext,
    typed::{IDENTITY_ACTIVITY_RESOLVER, PreparedActivityError, PreparedActivityResolver},
    workflow::ContextDecision,
};

/// Result of deterministically polling one workflow turn.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Evaluation {
    Scheduled {
        activity: LogicalActivityId,
        checkpoint: CheckpointEnvelope,
    },
    Pending {
        activity: LogicalActivityId,
        state: ActivityState,
    },
    Complete {
        outcome: TerminalOutcome,
        completed_activity_count: u64,
        checkpoint: CheckpointEnvelope,
    },
    Terminal {
        outcome: TerminalOutcome,
        completed_activity_count: u64,
    },
    Nondeterminism(Nondeterminism),
    CheckpointRejected(CheckpointError),
    PreparationRejected(PreparedActivityError),
    WorkflowStalled,
}

/// A valid checkpoint that cannot be replayed by the supplied workflow.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum Nondeterminism {
    #[error("activity {sequence} differs from its recorded immutable specification")]
    ActivityMismatch {
        sequence: ActivitySequence,
        recorded: ActivitySpec,
        requested: ActivitySpec,
    },
    #[error(
        "workflow completed after consuming {consumed} activities with {remaining} history records unused"
    )]
    UnusedHistory { consumed: u64, remaining: u64 },
    #[error("workflow suspended without requesting its next durable activity")]
    UnsupportedSuspension,
}

/// Validate a checkpoint, then poll an active workflow exactly once.
pub fn evaluate<W: Workflow>(
    workflow: &W,
    execution: &ExecutionSpec,
    checkpoint: Option<&CheckpointEnvelope>,
    limits: CheckpointLimits,
) -> Evaluation {
    evaluate_prepared(
        workflow,
        execution,
        checkpoint,
        limits,
        &IDENTITY_ACTIVITY_RESOLVER,
    )
}

/// Validate a checkpoint and evaluate one turn through an opt-in prepared
/// activity resolver.
pub fn evaluate_prepared<W: Workflow>(
    workflow: &W,
    execution: &ExecutionSpec,
    checkpoint: Option<&CheckpointEnvelope>,
    limits: CheckpointLimits,
    resolver: &dyn PreparedActivityResolver,
) -> Evaluation {
    let mut payload = match checkpoint {
        Some(envelope) => match envelope.decode_and_validate(execution, limits) {
            Ok(payload) => payload,
            Err(error) => return Evaluation::CheckpointRejected(error),
        },
        None => {
            let admitted_active = match u64::try_from(limits.max_active_encoded_bytes()) {
                Ok(admitted) => admitted,
                Err(_) => {
                    return Evaluation::CheckpointRejected(CheckpointError::EncodedLengthOverflow);
                }
            };
            let admitted_terminal = match u64::try_from(limits.max_terminal_encoded_bytes()) {
                Ok(admitted) => admitted,
                Err(_) => {
                    return Evaluation::CheckpointRejected(CheckpointError::EncodedLengthOverflow);
                }
            };
            let payload = CheckpointPayload::active(
                ExecutionContract::with_encoded_limits(
                    execution.clone(),
                    admitted_active,
                    admitted_terminal,
                ),
                Vec::new(),
            );
            if let Err(error) = payload.validate(execution, limits) {
                return Evaluation::CheckpointRejected(error);
            }
            payload
        }
    };

    if let Some((outcome, completed_activity_count)) = payload.terminal_outcome() {
        return Evaluation::Terminal {
            outcome: outcome.clone(),
            completed_activity_count,
        };
    }
    let history = payload
        .active_activities()
        .expect("terminal state returned before active replay");
    let (poll, cursor, decision) = {
        let mut context = WorkflowContext::new(execution.execution_id(), history, resolver);
        let poll = {
            let mut future = workflow.run(&mut context, execution.workflow_input().clone());
            let mut task_context = Context::from_waker(noop_waker_ref());
            future.as_mut().poll(&mut task_context)
        };
        (poll, context.cursor(), context.decision)
    };

    match decision {
        Some(ContextDecision::Schedule {
            sequence,
            spec,
            logical_id,
        }) => {
            payload
                .active_activities_mut()
                .expect("validated replay state is active")
                .push(ActivityRecord::scheduled(sequence, spec));
            match CheckpointEnvelope::encode_with_limits(&payload, limits) {
                Ok(checkpoint) => Evaluation::Scheduled {
                    activity: logical_id,
                    checkpoint,
                },
                Err(error) => Evaluation::CheckpointRejected(error),
            }
        }
        Some(ContextDecision::ExistingPending { logical_id, state }) => Evaluation::Pending {
            activity: logical_id,
            state,
        },
        Some(ContextDecision::Nondeterminism(error)) => Evaluation::Nondeterminism(error),
        Some(ContextDecision::PreparationRejected(error)) => Evaluation::PreparationRejected(error),
        None => match poll {
            Poll::Ready(outcome) => {
                let history_len = payload
                    .active_activities()
                    .expect("validated replay state is active")
                    .len();
                if cursor != history_len {
                    let remaining = history_len - cursor;
                    return Evaluation::Nondeterminism(Nondeterminism::UnusedHistory {
                        consumed: u64::try_from(cursor)
                            .expect("validated history length fits in u64"),
                        remaining: u64::try_from(remaining)
                            .expect("validated history length fits in u64"),
                    });
                }
                let completed_activity_count = match u64::try_from(cursor) {
                    Ok(count) => count,
                    Err(_) => {
                        return Evaluation::CheckpointRejected(CheckpointError::HistoryTooLong);
                    }
                };
                match CheckpointEnvelope::encode_with_limits(&payload, limits) {
                    Ok(checkpoint) => Evaluation::Complete {
                        outcome,
                        completed_activity_count,
                        checkpoint,
                    },
                    Err(error) => Evaluation::CheckpointRejected(error),
                }
            }
            Poll::Pending => Evaluation::WorkflowStalled,
        },
    }
}
