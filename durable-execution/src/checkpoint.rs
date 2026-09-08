use base64::encoded_len;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    ActivityName, ActivitySequence, ActivitySpec, AttemptId, ExactBytes, ExecutionId,
    ExecutionSpec, LogicalActivityId, PreparedActivityError, TerminalOutcome,
};

pub const CHECKPOINT_FORMAT_VERSION: u32 = 3;

/// Required limits for every loaded or proposed checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointLimits {
    max_activity_records: usize,
    max_active_encoded_bytes: usize,
    max_terminal_encoded_bytes: usize,
}

impl CheckpointLimits {
    pub fn new(
        max_activity_records: usize,
        max_active_encoded_bytes: usize,
        max_terminal_encoded_bytes: usize,
    ) -> Result<Self, CheckpointError> {
        if max_activity_records == 0 {
            return Err(CheckpointError::ZeroActivityRecordLimit);
        }
        if max_active_encoded_bytes == 0 {
            return Err(CheckpointError::ZeroEncodedCheckpointLimit);
        }
        if max_terminal_encoded_bytes == 0 {
            return Err(CheckpointError::ZeroTerminalEncodedCheckpointLimit);
        }
        Ok(Self {
            max_activity_records,
            max_active_encoded_bytes,
            max_terminal_encoded_bytes,
        })
    }

    pub const fn max_activity_records(self) -> usize {
        self.max_activity_records
    }

    pub const fn max_active_encoded_bytes(self) -> usize {
        self.max_active_encoded_bytes
    }

    pub const fn max_terminal_encoded_bytes(self) -> usize {
        self.max_terminal_encoded_bytes
    }
}

/// Version discriminator and opaque JSON payload bytes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointEnvelope {
    format_version: u32,
    payload: ExactBytes,
}

impl CheckpointEnvelope {
    pub fn new(format_version: u32, payload: ExactBytes) -> Self {
        Self {
            format_version,
            payload,
        }
    }

    pub fn encode(payload: &CheckpointPayload) -> Result<Self, CheckpointError> {
        serde_json::to_vec(payload)
            .map(ExactBytes::new)
            .map(|payload| Self::new(CHECKPOINT_FORMAT_VERSION, payload))
            .map_err(invalid_json)
    }

    pub fn encode_with_limits(
        payload: &CheckpointPayload,
        limits: CheckpointLimits,
    ) -> Result<Self, CheckpointError> {
        payload.validate_internal(limits)?;
        let checkpoint = Self::encode(payload)?;
        checkpoint.validate_lifecycle_encoded_size(payload.state(), limits)?;
        Ok(checkpoint)
    }

    pub const fn format_version(&self) -> u32 {
        self.format_version
    }

    pub fn payload(&self) -> &ExactBytes {
        &self.payload
    }

    /// Canonical JSON bytes required to persist this complete envelope.
    pub fn encoded_len(&self) -> Result<usize, CheckpointError> {
        let payload_base64_len = encoded_len(self.payload.as_slice().len(), true)
            .ok_or(CheckpointError::EncodedLengthOverflow)?;
        let fixed_envelope_len =
            serde_json::to_vec(&Self::new(self.format_version, ExactBytes::default()))
                .map_err(invalid_json)?
                .len();
        fixed_envelope_len
            .checked_add(payload_base64_len)
            .ok_or(CheckpointError::EncodedLengthOverflow)
    }

    pub fn decode_and_validate(
        &self,
        expected: &ExecutionSpec,
        limits: CheckpointLimits,
    ) -> Result<CheckpointPayload, CheckpointError> {
        if self.format_version != CHECKPOINT_FORMAT_VERSION {
            return Err(CheckpointError::UnsupportedFormat {
                actual: self.format_version,
                supported: CHECKPOINT_FORMAT_VERSION,
            });
        }
        self.validate_active_encoded_size(limits)?;

        let payload: CheckpointPayload =
            serde_json::from_slice(self.payload.as_slice()).map_err(invalid_json)?;
        self.validate_lifecycle_encoded_size(payload.state(), limits)?;
        payload.validate(expected, limits)?;
        Ok(payload)
    }

    fn validate_active_encoded_size(
        &self,
        limits: CheckpointLimits,
    ) -> Result<(), CheckpointError> {
        let actual = self.encoded_len()?;
        if actual > limits.max_active_encoded_bytes {
            return Err(CheckpointError::EncodedCheckpointLimitExceeded {
                actual,
                maximum: limits.max_active_encoded_bytes,
            });
        }
        Ok(())
    }

    fn validate_lifecycle_encoded_size(
        &self,
        state: &CheckpointState,
        limits: CheckpointLimits,
    ) -> Result<(), CheckpointError> {
        let actual = self.encoded_len()?;
        match state {
            CheckpointState::Active { .. } if actual > limits.max_active_encoded_bytes => {
                Err(CheckpointError::EncodedCheckpointLimitExceeded {
                    actual,
                    maximum: limits.max_active_encoded_bytes,
                })
            }
            CheckpointState::Terminal { .. } if actual > limits.max_terminal_encoded_bytes => {
                Err(CheckpointError::TerminalEncodedCheckpointLimitExceeded {
                    actual,
                    maximum: limits.max_terminal_encoded_bytes,
                })
            }
            _ => Ok(()),
        }
    }
}

/// Immutable execution-level checkpoint authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionContract {
    spec: ExecutionSpec,
    admitted_max_encoded_checkpoint_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    admitted_max_terminal_encoded_checkpoint_bytes: Option<u64>,
}

impl ExecutionContract {
    pub const fn new(spec: ExecutionSpec, admitted_max_encoded_checkpoint_bytes: u64) -> Self {
        Self {
            spec,
            admitted_max_encoded_checkpoint_bytes,
            admitted_max_terminal_encoded_checkpoint_bytes: None,
        }
    }

    pub const fn with_encoded_limits(
        spec: ExecutionSpec,
        admitted_max_encoded_checkpoint_bytes: u64,
        admitted_max_terminal_encoded_checkpoint_bytes: u64,
    ) -> Self {
        Self {
            spec,
            admitted_max_encoded_checkpoint_bytes,
            admitted_max_terminal_encoded_checkpoint_bytes: Some(
                admitted_max_terminal_encoded_checkpoint_bytes,
            ),
        }
    }

    pub const fn spec(&self) -> &ExecutionSpec {
        &self.spec
    }

    pub const fn admitted_max_encoded_checkpoint_bytes(&self) -> u64 {
        self.admitted_max_encoded_checkpoint_bytes
    }

    pub const fn admitted_max_terminal_encoded_checkpoint_bytes(&self) -> u64 {
        match self.admitted_max_terminal_encoded_checkpoint_bytes {
            Some(limit) => limit,
            None => self.admitted_max_encoded_checkpoint_bytes,
        }
    }
}

/// Explicit lifecycle state for one execution.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "lifecycle", rename_all = "snake_case", deny_unknown_fields)]
pub enum CheckpointState {
    Active {
        activities: Vec<ActivityRecord>,
    },
    Terminal {
        outcome: TerminalOutcome,
        completed_activity_count: u64,
    },
}

/// Decoded checkpoint state for one execution.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointPayload {
    execution: ExecutionContract,
    state: CheckpointState,
}

impl CheckpointPayload {
    pub const fn active(execution: ExecutionContract, activities: Vec<ActivityRecord>) -> Self {
        Self {
            execution,
            state: CheckpointState::Active { activities },
        }
    }

    pub const fn terminal(
        execution: ExecutionContract,
        outcome: TerminalOutcome,
        completed_activity_count: u64,
    ) -> Self {
        Self {
            execution,
            state: CheckpointState::Terminal {
                outcome,
                completed_activity_count,
            },
        }
    }

    pub const fn execution(&self) -> &ExecutionContract {
        &self.execution
    }

    pub const fn state(&self) -> &CheckpointState {
        &self.state
    }

    pub fn active_activities(&self) -> Option<&[ActivityRecord]> {
        match &self.state {
            CheckpointState::Active { activities } => Some(activities),
            CheckpointState::Terminal { .. } => None,
        }
    }

    pub fn terminal_outcome(&self) -> Option<(&TerminalOutcome, u64)> {
        match &self.state {
            CheckpointState::Terminal {
                outcome,
                completed_activity_count,
            } => Some((outcome, *completed_activity_count)),
            CheckpointState::Active { .. } => None,
        }
    }

    pub(crate) fn active_activities_mut(&mut self) -> Option<&mut Vec<ActivityRecord>> {
        match &mut self.state {
            CheckpointState::Active { activities } => Some(activities),
            CheckpointState::Terminal { .. } => None,
        }
    }

    pub(crate) fn into_terminal(
        self,
        outcome: TerminalOutcome,
        completed_activity_count: u64,
    ) -> Result<Self, CheckpointError> {
        if !matches!(self.state, CheckpointState::Active { .. }) {
            return Err(CheckpointError::ExpectedActiveCheckpoint);
        }
        validate_terminal_outcome(&outcome, self.execution.spec.max_terminal_payload_bytes())?;
        Ok(Self::terminal(
            self.execution,
            outcome,
            completed_activity_count,
        ))
    }

    pub fn validate(
        &self,
        expected: &ExecutionSpec,
        limits: CheckpointLimits,
    ) -> Result<(), CheckpointError> {
        if self.execution.spec.execution_id() != expected.execution_id() {
            return Err(CheckpointError::ExecutionMismatch {
                expected: expected.execution_id(),
                actual: self.execution.spec.execution_id(),
            });
        }
        if self.execution.spec.workflow_input() != expected.workflow_input() {
            return Err(CheckpointError::WorkflowInputMismatch {
                expected: expected.workflow_input().clone(),
                actual: self.execution.spec.workflow_input().clone(),
            });
        }
        if self.execution.spec.max_terminal_payload_bytes() != expected.max_terminal_payload_bytes()
        {
            return Err(CheckpointError::TerminalPayloadBoundMismatch {
                expected: expected.max_terminal_payload_bytes(),
                actual: self.execution.spec.max_terminal_payload_bytes(),
            });
        }
        self.validate_internal(limits)
    }

    /// Exact encoded envelope size of the larger terminal outcome at the
    /// declared payload maximum, without allocating that payload.
    pub fn maximum_terminal_encoded_len(&self) -> Result<usize, CheckpointError> {
        let declared_len = usize::try_from(self.execution.spec.max_terminal_payload_bytes())
            .map_err(|_| CheckpointError::TerminalPayloadLengthUnrepresentable)?;
        let payload_base64_len =
            encoded_len(declared_len, true).ok_or(CheckpointError::EncodedLengthOverflow)?;

        [
            TerminalOutcome::succeeded(ExactBytes::default()),
            TerminalOutcome::failed(ExactBytes::default()),
        ]
        .into_iter()
        .map(|outcome| {
            let empty_terminal = Self::terminal(self.execution.clone(), outcome, u64::MAX);
            let empty_inner_len = serde_json::to_vec(&empty_terminal)
                .map_err(invalid_json)?
                .len();
            let projected_inner_len = empty_inner_len
                .checked_add(payload_base64_len)
                .ok_or(CheckpointError::EncodedLengthOverflow)?;
            let projected_envelope_payload_len = encoded_len(projected_inner_len, true)
                .ok_or(CheckpointError::EncodedLengthOverflow)?;
            let empty_envelope_len =
                CheckpointEnvelope::new(CHECKPOINT_FORMAT_VERSION, ExactBytes::default())
                    .encoded_len()?;
            empty_envelope_len
                .checked_add(projected_envelope_payload_len)
                .ok_or(CheckpointError::EncodedLengthOverflow)
        })
        .try_fold(0, |largest, projected| {
            projected.map(|value| largest.max(value))
        })
    }

    /// Exact encoded envelope size for completing the final activity at its
    /// declared maximum, without allocating that result.
    pub fn maximum_activity_completed_encoded_len(&self) -> Result<usize, CheckpointError> {
        let activities = self
            .active_activities()
            .ok_or(CheckpointError::ExpectedActiveCheckpoint)?;
        let record = activities
            .last()
            .ok_or(CheckpointError::MissingPendingActivity)?;
        let result_len = usize::try_from(record.spec.max_result_bytes())
            .map_err(|_| CheckpointError::ResultLengthUnrepresentable)?;
        let result_base64_len =
            encoded_len(result_len, true).ok_or(CheckpointError::EncodedLengthOverflow)?;

        let mut empty_completion = self.clone();
        let final_record = empty_completion
            .active_activities_mut()
            .and_then(|activities| activities.last_mut())
            .expect("final active record was checked above");
        final_record.state = ActivityState::Completed {
            result: ExactBytes::default(),
        };
        let empty_inner_len = serde_json::to_vec(&empty_completion)
            .map_err(invalid_json)?
            .len();
        let projected_inner_len = empty_inner_len
            .checked_add(result_base64_len)
            .ok_or(CheckpointError::EncodedLengthOverflow)?;
        let projected_payload_base64_len =
            encoded_len(projected_inner_len, true).ok_or(CheckpointError::EncodedLengthOverflow)?;
        let empty_envelope_len =
            CheckpointEnvelope::new(CHECKPOINT_FORMAT_VERSION, ExactBytes::default())
                .encoded_len()?;
        empty_envelope_len
            .checked_add(projected_payload_base64_len)
            .ok_or(CheckpointError::EncodedLengthOverflow)
    }

    fn validate_internal(&self, limits: CheckpointLimits) -> Result<(), CheckpointError> {
        let configured = u64::try_from(limits.max_active_encoded_bytes)
            .map_err(|_| CheckpointError::EncodedLengthOverflow)?;
        let admitted = self.execution.admitted_max_encoded_checkpoint_bytes;
        let configured_terminal = u64::try_from(limits.max_terminal_encoded_bytes)
            .map_err(|_| CheckpointError::EncodedLengthOverflow)?;
        let admitted_terminal = self
            .execution
            .admitted_max_terminal_encoded_checkpoint_bytes();
        let required = self.maximum_terminal_encoded_len()?;
        let required =
            u64::try_from(required).map_err(|_| CheckpointError::EncodedLengthOverflow)?;
        if required > admitted_terminal {
            return Err(CheckpointError::AdmittedTerminalCapacityInsufficient {
                required,
                admitted: admitted_terminal,
            });
        }
        if configured_terminal != admitted_terminal {
            return Err(CheckpointError::TerminalEncodedCheckpointCapacityMismatch {
                configured: configured_terminal,
                admitted: admitted_terminal,
            });
        }
        if configured < admitted {
            return Err(CheckpointError::ConfiguredCapacityBelowAdmission {
                configured,
                admitted,
            });
        }
        if configured > admitted {
            return Err(CheckpointError::ConfiguredCapacityAboveAdmission {
                configured,
                admitted,
            });
        }

        match &self.state {
            CheckpointState::Active { activities } => {
                validate_activity_count(activities, limits)?;
                validate_active_history(activities)
            }
            CheckpointState::Terminal { outcome, .. } => {
                validate_terminal_outcome(outcome, self.execution.spec.max_terminal_payload_bytes())
            }
        }
    }
}

/// One activity in contiguous zero-based workflow history.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivityRecord {
    sequence: ActivitySequence,
    spec: ActivitySpec,
    state: ActivityState,
}

impl ActivityRecord {
    pub const fn new(sequence: ActivitySequence, spec: ActivitySpec, state: ActivityState) -> Self {
        Self {
            sequence,
            spec,
            state,
        }
    }

    pub const fn scheduled(sequence: ActivitySequence, spec: ActivitySpec) -> Self {
        Self::new(sequence, spec, ActivityState::Scheduled)
    }

    pub const fn completed(
        sequence: ActivitySequence,
        spec: ActivitySpec,
        result: ExactBytes,
    ) -> Self {
        Self::new(sequence, spec, ActivityState::Completed { result })
    }

    pub const fn dispatch_exposed(
        sequence: ActivitySequence,
        spec: ActivitySpec,
        attempt_id: AttemptId,
    ) -> Self {
        Self::new(
            sequence,
            spec,
            ActivityState::DispatchExposed { attempt_id },
        )
    }

    pub const fn sequence(&self) -> ActivitySequence {
        self.sequence
    }

    pub const fn spec(&self) -> &ActivitySpec {
        &self.spec
    }

    pub const fn name(&self) -> &ActivityName {
        self.spec.name()
    }

    pub const fn input(&self) -> &ExactBytes {
        self.spec.input()
    }

    pub const fn max_result_bytes(&self) -> u64 {
        self.spec.max_result_bytes()
    }

    pub const fn state(&self) -> &ActivityState {
        &self.state
    }

    pub fn logical_id(&self, execution_id: ExecutionId) -> LogicalActivityId {
        LogicalActivityId::new(execution_id, self.sequence, self.spec.clone())
    }
}

/// Persisted state of one logical activity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ActivityState {
    Scheduled,
    DispatchExposed { attempt_id: AttemptId },
    Completed { result: ExactBytes },
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum CheckpointError {
    #[error("prepared activity was rejected: {0}")]
    PreparedActivityRejected(PreparedActivityError),
    #[error("checkpoint format {actual} is unsupported; this build supports {supported}")]
    UnsupportedFormat { actual: u32, supported: u32 },
    #[error("checkpoint JSON is invalid: {0}")]
    InvalidJson(String),
    #[error("checkpoint execution identity differs from the requested execution")]
    ExecutionMismatch {
        expected: ExecutionId,
        actual: ExecutionId,
    },
    #[error("checkpoint workflow input differs from the exact requested input")]
    WorkflowInputMismatch {
        expected: ExactBytes,
        actual: ExactBytes,
    },
    #[error("checkpoint terminal payload bound {actual} differs from requested bound {expected}")]
    TerminalPayloadBoundMismatch { expected: u64, actual: u64 },
    #[error("maximum activity records must be greater than zero")]
    ZeroActivityRecordLimit,
    #[error("maximum encoded checkpoint bytes must be greater than zero")]
    ZeroEncodedCheckpointLimit,
    #[error("maximum terminal encoded checkpoint bytes must be greater than zero")]
    ZeroTerminalEncodedCheckpointLimit,
    #[error("checkpoint has {actual} activity records; configured maximum is {maximum}")]
    ActivityRecordLimitExceeded { actual: usize, maximum: usize },
    #[error("encoded checkpoint uses {actual} bytes; configured maximum is {maximum}")]
    EncodedCheckpointLimitExceeded { actual: usize, maximum: usize },
    #[error("terminal encoded checkpoint uses {actual} bytes; configured maximum is {maximum}")]
    TerminalEncodedCheckpointLimitExceeded { actual: usize, maximum: usize },
    #[error(
        "configured encoded checkpoint capacity {configured} is below admitted capacity {admitted}"
    )]
    ConfiguredCapacityBelowAdmission { configured: u64, admitted: u64 },
    #[error(
        "configured encoded checkpoint capacity {configured} is above admitted capacity {admitted}"
    )]
    ConfiguredCapacityAboveAdmission { configured: u64, admitted: u64 },
    #[error(
        "configured terminal encoded checkpoint capacity {configured} differs from admitted capacity {admitted}"
    )]
    TerminalEncodedCheckpointCapacityMismatch { configured: u64, admitted: u64 },
    #[error(
        "admitted terminal checkpoint capacity {admitted} is below required capacity {required}"
    )]
    AdmittedTerminalCapacityInsufficient { required: u64, admitted: u64 },
    #[error("activity result length cannot be represented on this platform")]
    ResultLengthUnrepresentable,
    #[error("terminal payload length cannot be represented on this platform")]
    TerminalPayloadLengthUnrepresentable,
    #[error("encoded checkpoint length overflow")]
    EncodedLengthOverflow,
    #[error("capacity reservation requires a final pending activity")]
    MissingPendingActivity,
    #[error("operation requires an active checkpoint")]
    ExpectedActiveCheckpoint,
    #[error("activity {sequence} result uses {actual} bytes; declared maximum is {maximum}")]
    CompletedResultExceedsDeclared {
        sequence: ActivitySequence,
        actual: u64,
        maximum: u64,
    },
    #[error("terminal payload uses {actual} bytes; declared maximum is {maximum}")]
    TerminalPayloadExceedsDeclared { actual: u64, maximum: u64 },
    #[error("activity history is too long to address with a u64 sequence")]
    HistoryTooLong,
    #[error("expected activity sequence {expected}, found {actual}")]
    NonContiguousSequence {
        expected: ActivitySequence,
        actual: ActivitySequence,
    },
    #[error("pending activity {sequence} must be the final history record")]
    PendingActivityNotFinal { sequence: ActivitySequence },
}

fn validate_activity_count(
    activities: &[ActivityRecord],
    limits: CheckpointLimits,
) -> Result<(), CheckpointError> {
    let actual = activities.len();
    if actual > limits.max_activity_records {
        return Err(CheckpointError::ActivityRecordLimitExceeded {
            actual,
            maximum: limits.max_activity_records,
        });
    }
    Ok(())
}

fn validate_active_history(activities: &[ActivityRecord]) -> Result<(), CheckpointError> {
    for (index, record) in activities.iter().enumerate() {
        let expected = u64::try_from(index).map_err(|_| CheckpointError::HistoryTooLong)?;
        if record.sequence.get() != expected {
            return Err(CheckpointError::NonContiguousSequence {
                expected: ActivitySequence::new(expected),
                actual: record.sequence,
            });
        }
        if let ActivityState::Completed { result } = &record.state {
            let actual = u64::try_from(result.as_slice().len())
                .map_err(|_| CheckpointError::ResultLengthUnrepresentable)?;
            if actual > record.spec.max_result_bytes() {
                return Err(CheckpointError::CompletedResultExceedsDeclared {
                    sequence: record.sequence,
                    actual,
                    maximum: record.spec.max_result_bytes(),
                });
            }
        } else if index + 1 != activities.len() {
            return Err(CheckpointError::PendingActivityNotFinal {
                sequence: record.sequence,
            });
        }
    }
    Ok(())
}

fn validate_terminal_outcome(
    outcome: &TerminalOutcome,
    maximum: u64,
) -> Result<(), CheckpointError> {
    let actual = u64::try_from(outcome.payload().as_slice().len())
        .map_err(|_| CheckpointError::TerminalPayloadLengthUnrepresentable)?;
    if actual > maximum {
        return Err(CheckpointError::TerminalPayloadExceedsDeclared { actual, maximum });
    }
    Ok(())
}

fn invalid_json(error: serde_json::Error) -> CheckpointError {
    CheckpointError::InvalidJson(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(max_terminal_payload_bytes: u64) -> ExecutionSpec {
        ExecutionSpec::new(
            ExecutionId::from_bytes([1; 16]),
            ExactBytes::new(b"input"),
            max_terminal_payload_bytes,
        )
    }

    fn contract(max_terminal_payload_bytes: u64, admitted: u64) -> ExecutionContract {
        ExecutionContract::new(spec(max_terminal_payload_bytes), admitted)
    }

    #[test]
    fn nonallocating_encoded_length_matches_canonical_json() {
        for payload_len in [0, 1, 2, 3, 4, 255, 1024] {
            let checkpoint = CheckpointEnvelope::new(
                CHECKPOINT_FORMAT_VERSION,
                ExactBytes::new(vec![7; payload_len]),
            );
            assert_eq!(
                checkpoint.encoded_len().unwrap(),
                serde_json::to_vec(&checkpoint).unwrap().len()
            );
        }
    }

    #[test]
    fn terminal_projection_covers_both_variants_and_maximum_count() {
        for maximum in [0, 1, 2, 3, 1024] {
            let payload = CheckpointPayload::active(contract(maximum, 1_000_000), Vec::new());
            let projected = payload.maximum_terminal_encoded_len().unwrap();
            for outcome in [
                TerminalOutcome::succeeded(ExactBytes::new(vec![7; maximum as usize])),
                TerminalOutcome::failed(ExactBytes::new(vec![7; maximum as usize])),
            ] {
                let terminal =
                    CheckpointPayload::terminal(payload.execution.clone(), outcome, u64::MAX);
                assert!(
                    CheckpointEnvelope::encode(&terminal)
                        .unwrap()
                        .encoded_len()
                        .unwrap()
                        <= projected
                );
            }
        }
    }

    #[test]
    fn terminal_projection_rejects_unrepresentable_length_without_allocating() {
        let payload = CheckpointPayload::active(contract(u64::MAX, u64::MAX), Vec::new());
        assert!(matches!(
            payload.maximum_terminal_encoded_len(),
            Err(CheckpointError::EncodedLengthOverflow)
        ));
    }

    #[test]
    fn terminal_state_has_no_serialized_history_field() {
        let payload = CheckpointPayload::terminal(
            contract(16, 1_000_000),
            TerminalOutcome::succeeded(ExactBytes::new(b"done")),
            4,
        );
        let json = serde_json::to_string(&payload).unwrap();
        assert!(!json.contains("activities"));
        assert!(!json.contains("history"));
        assert!(!json.contains("digest"));
    }

    #[test]
    fn terminal_state_rejects_ignored_active_history_fields() {
        let payload = CheckpointPayload::terminal(
            contract(16, 1_000_000),
            TerminalOutcome::succeeded(ExactBytes::new(b"done")),
            4,
        );
        for path in ["state", "outcome", "execution"] {
            let mut value = serde_json::to_value(&payload).unwrap();
            match path {
                "state" => value["state"]["activities"] = serde_json::json!([]),
                "outcome" => value["state"]["outcome"]["activities"] = serde_json::json!([]),
                "execution" => {
                    value["execution"]["spec"]["unexpected"] = serde_json::json!(true);
                }
                _ => unreachable!(),
            }
            assert!(serde_json::from_value::<CheckpointPayload>(value).is_err());
        }
    }

    #[test]
    fn terminal_encoded_limit_must_be_nonzero() {
        assert_eq!(
            CheckpointLimits::new(1, 1024, 0),
            Err(CheckpointError::ZeroTerminalEncodedCheckpointLimit)
        );
    }
}
