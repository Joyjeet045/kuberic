use base64::encoded_len;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    ActivityName, ActivitySequence, ActivitySpec, AttemptId, CompletionClass, CompletionMetadata,
    EffectAttempt, EffectAttemptState, EffectObservationDisposition, ExactBytes, ExecutionId,
    ExecutionSpec, LogicalActivityId, PreparedActivityError, PreparedCommand, TerminalOutcome,
    validate_effect_attempts,
};

pub const CHECKPOINT_FORMAT_VERSION: u32 = 4;

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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        completion_metadata: Option<CompletionMetadata>,
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
                completion_metadata: None,
            },
        }
    }

    pub const fn terminal_with_metadata(
        execution: ExecutionContract,
        outcome: TerminalOutcome,
        completion_metadata: CompletionMetadata,
    ) -> Self {
        Self {
            execution,
            state: CheckpointState::Terminal {
                outcome,
                completed_activity_count: completion_metadata.completed_activity_count(),
                completion_metadata: Some(completion_metadata),
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
                ..
            } => Some((outcome, *completed_activity_count)),
            CheckpointState::Active { .. } => None,
        }
    }

    pub const fn terminal_completion_metadata(&self) -> Option<CompletionMetadata> {
        match &self.state {
            CheckpointState::Terminal {
                completion_metadata,
                ..
            } => *completion_metadata,
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
        let CheckpointState::Active { ref activities } = self.state else {
            return Err(CheckpointError::ExpectedActiveCheckpoint);
        };
        validate_terminal_outcome(&outcome, self.execution.spec.max_terminal_payload_bytes())?;
        let completion_metadata = if !activities.is_empty()
            && activities
                .iter()
                .all(|record| record.completion_class.is_some())
        {
            let metadata = CompletionMetadata::from_classes(
                activities
                    .iter()
                    .map(|record| record.completion_class.expect("checked above")),
            )
            .map_err(|error| CheckpointError::EffectContract(error.to_string()))?;
            if metadata.completed_activity_count() != completed_activity_count {
                return Err(CheckpointError::EffectContract(
                    "authenticated completion count differs from replay cursor".to_owned(),
                ));
            }
            Some(metadata)
        } else {
            None
        };
        Ok(match completion_metadata {
            Some(metadata) => Self::terminal_with_metadata(self.execution, outcome, metadata),
            None => Self::terminal(self.execution, outcome, completed_activity_count),
        })
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

        let maximum_metadata = CompletionMetadata::from_counts(u64::MAX, u64::MAX, 0)
            .map_err(|error| CheckpointError::EffectContract(error.to_string()))?;
        let maximum_split = 10_000_000_000_000_000_000_u64;
        let maximum_split_metadata =
            CompletionMetadata::from_counts(u64::MAX, maximum_split, u64::MAX - maximum_split)
                .map_err(|error| CheckpointError::EffectContract(error.to_string()))?;
        [
            (TerminalOutcome::succeeded(ExactBytes::default()), None),
            (TerminalOutcome::failed(ExactBytes::default()), None),
            (
                TerminalOutcome::succeeded(ExactBytes::default()),
                Some(maximum_metadata),
            ),
            (
                TerminalOutcome::failed(ExactBytes::default()),
                Some(maximum_metadata),
            ),
            (
                TerminalOutcome::succeeded(ExactBytes::default()),
                Some(maximum_split_metadata),
            ),
            (
                TerminalOutcome::failed(ExactBytes::default()),
                Some(maximum_split_metadata),
            ),
        ]
        .into_iter()
        .map(|(outcome, metadata)| {
            let empty_terminal = match metadata {
                Some(metadata) => {
                    Self::terminal_with_metadata(self.execution.clone(), outcome, metadata)
                }
                None => Self::terminal(self.execution.clone(), outcome, u64::MAX),
            };
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
            CheckpointState::Terminal {
                outcome,
                completed_activity_count,
                completion_metadata,
            } => {
                validate_terminal_outcome(
                    outcome,
                    self.execution.spec.max_terminal_payload_bytes(),
                )?;
                if let Some(metadata) = completion_metadata {
                    CompletionMetadata::from_counts(
                        metadata.completed_activity_count(),
                        metadata.external_effect_count(),
                        metadata.passive_observation_count(),
                    )
                    .map_err(|error| CheckpointError::EffectContract(error.to_string()))?;
                    if metadata.completed_activity_count() != *completed_activity_count {
                        return Err(CheckpointError::EffectContract(
                            "terminal completion metadata count mismatch".to_owned(),
                        ));
                    }
                }
                Ok(())
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
    #[serde(default = "ActivityAttemptState::first")]
    attempt: ActivityAttemptState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    prepared_command: Option<PreparedCommand>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    completion_class: Option<CompletionClass>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    attempts: Vec<EffectAttempt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    failure: Option<ActivityFailure>,
    state: ActivityState,
}

impl ActivityRecord {
    pub const fn new(sequence: ActivitySequence, spec: ActivitySpec, state: ActivityState) -> Self {
        Self {
            sequence,
            spec,
            attempt: ActivityAttemptState::first(),
            prepared_command: None,
            completion_class: None,
            attempts: Vec::new(),
            failure: None,
            state,
        }
    }

    pub fn prepared_effect(
        sequence: ActivitySequence,
        spec: ActivitySpec,
        prepared_command: PreparedCommand,
        completion_class: CompletionClass,
        attempts: Vec<EffectAttempt>,
        state: ActivityState,
    ) -> Result<Self, CheckpointError> {
        validate_effect_attempts(&attempts)
            .map_err(|error| CheckpointError::EffectContract(error.to_string()))?;
        Ok(Self {
            sequence,
            spec,
            attempt: ActivityAttemptState::first(),
            prepared_command: Some(prepared_command),
            completion_class: Some(completion_class),
            attempts,
            failure: None,
            state,
        })
    }

    pub const fn scheduled(sequence: ActivitySequence, spec: ActivitySpec) -> Self {
        Self::new(sequence, spec, ActivityState::Scheduled)
    }

    pub(crate) fn classified(
        sequence: ActivitySequence,
        spec: ActivitySpec,
        completion_class: CompletionClass,
        state: ActivityState,
    ) -> Self {
        let mut record = Self::new(sequence, spec, state);
        record.completion_class = Some(completion_class);
        record
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

    pub const fn attempt(&self) -> ActivityAttemptState {
        self.attempt
    }

    pub const fn prepared_command(&self) -> Option<&PreparedCommand> {
        self.prepared_command.as_ref()
    }

    pub const fn completion_class(&self) -> Option<CompletionClass> {
        self.completion_class
    }

    pub fn attempts(&self) -> &[EffectAttempt] {
        &self.attempts
    }

    pub const fn failure(&self) -> Option<&ActivityFailure> {
        self.failure.as_ref()
    }

    pub(crate) fn with_state(mut self, state: ActivityState) -> Self {
        self.failure = None;
        self.state = state;
        self
    }

    pub(crate) fn with_failure(mut self, failure: ActivityFailure) -> Self {
        self.failure = Some(failure);
        self.state = ActivityState::Completed {
            result: ExactBytes::default(),
        };
        self
    }

    pub fn schedule_retry(
        mut self,
        retry_not_before_unix_millis: i64,
    ) -> Result<Self, CheckpointError> {
        if !matches!(self.state, ActivityState::DispatchExposed { .. }) {
            return Err(CheckpointError::RetryRequiresExposedActivity {
                sequence: self.sequence,
            });
        }
        let next = self.attempt.ordinal.checked_add(1).ok_or(
            CheckpointError::ActivityAttemptLimitExceeded {
                sequence: self.sequence,
                attempted: u32::MAX,
                maximum: self.spec.options().max_attempts(),
            },
        )?;
        if next > self.spec.options().max_attempts() {
            return Err(CheckpointError::ActivityAttemptLimitExceeded {
                sequence: self.sequence,
                attempted: next,
                maximum: self.spec.options().max_attempts(),
            });
        }
        self.attempt = ActivityAttemptState::new(next, Some(retry_not_before_unix_millis), None);
        self.state = ActivityState::Scheduled;
        Ok(self)
    }

    pub fn defer_wait(mut self, wait_until_unix_millis: i64) -> Result<Self, CheckpointError> {
        if !matches!(self.state, ActivityState::DispatchExposed { .. }) {
            return Err(CheckpointError::RetryRequiresExposedActivity {
                sequence: self.sequence,
            });
        }
        self.attempt.handler_wait_until_unix_millis = Some(wait_until_unix_millis);
        self.state = ActivityState::Scheduled;
        Ok(self)
    }

    pub(crate) fn expose_attempt(mut self, attempt_id: AttemptId) -> Result<Self, CheckpointError> {
        self.attempt.handler_wait_until_unix_millis = None;
        if self.prepared_command.is_some() {
            self.attempts
                .push(EffectAttempt::new(attempt_id, EffectAttemptState::Exposed));
            validate_effect_attempts(&self.attempts)
                .map_err(|error| CheckpointError::EffectContract(error.to_string()))?;
        }
        self.state = ActivityState::DispatchExposed { attempt_id };
        Ok(self)
    }

    pub(crate) fn observe_effect_attempt(
        mut self,
        attempt_id: AttemptId,
        disposition: EffectObservationDisposition,
        result: ExactBytes,
        failure: Option<ActivityFailure>,
    ) -> Result<Self, CheckpointError> {
        let attempt_count = self.attempts.len();
        let Some(attempt) = self.attempts.last_mut() else {
            return Err(CheckpointError::EffectContract(
                "effect observation requires a recorded attempt".to_owned(),
            ));
        };
        if attempt.attempt_id() != attempt_id || attempt.state() != EffectAttemptState::Exposed {
            return Err(CheckpointError::EffectContract(
                "effect observation attempt does not match the exposed attempt".to_owned(),
            ));
        }
        match disposition {
            EffectObservationDisposition::Completed => {
                *attempt = EffectAttempt::new(attempt_id, EffectAttemptState::Observed);
                if let Some(failure) = failure {
                    self = self.with_failure(failure);
                } else {
                    self.state = ActivityState::Completed { result };
                }
            }
            EffectObservationDisposition::ProvenNoAdmission if attempt_count == 1 => {
                *attempt = EffectAttempt::new(attempt_id, EffectAttemptState::ProvenNoAdmission);
                self.state = ActivityState::Scheduled;
            }
            EffectObservationDisposition::ProvenNoAdmission => {
                *attempt = EffectAttempt::new(attempt_id, EffectAttemptState::Observed);
                if let Some(failure) = failure {
                    self = self.with_failure(failure);
                } else {
                    self.state = ActivityState::Completed { result };
                }
            }
        }
        validate_effect_attempts(&self.attempts)
            .map_err(|error| CheckpointError::EffectContract(error.to_string()))?;
        Ok(self)
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

/// Durable non-success result of an ordinary activity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "detail", rename_all = "snake_case")]
pub enum ActivityFailure {
    Application(ExactBytes),
    TimedOut,
    ActionDeadlineExceeded,
}

impl ActivityFailure {
    pub const fn payload(&self) -> Option<&ExactBytes> {
        match self {
            Self::Application(payload) => Some(payload),
            Self::TimedOut | Self::ActionDeadlineExceeded => None,
        }
    }
}

/// Persisted physical-attempt state for one stable logical activity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivityAttemptState {
    ordinal: u32,
    retry_not_before_unix_millis: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    handler_wait_until_unix_millis: Option<i64>,
}

impl ActivityAttemptState {
    pub const fn new(
        ordinal: u32,
        retry_not_before_unix_millis: Option<i64>,
        handler_wait_until_unix_millis: Option<i64>,
    ) -> Self {
        Self {
            ordinal,
            retry_not_before_unix_millis,
            handler_wait_until_unix_millis,
        }
    }

    pub const fn first() -> Self {
        Self::new(1, None, None)
    }

    pub const fn ordinal(self) -> u32 {
        self.ordinal
    }

    pub const fn retry_not_before_unix_millis(self) -> Option<i64> {
        self.retry_not_before_unix_millis
    }

    pub const fn handler_wait_until_unix_millis(self) -> Option<i64> {
        self.handler_wait_until_unix_millis
    }
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
    #[error(
        "activity {sequence} attempted ordinal {attempted}, exceeding the configured maximum {maximum}"
    )]
    ActivityAttemptLimitExceeded {
        sequence: ActivitySequence,
        attempted: u32,
        maximum: u32,
    },
    #[error("activity {sequence} must be exposed before a retry can be scheduled")]
    RetryRequiresExposedActivity { sequence: ActivitySequence },
    #[error("activity {sequence} stores a failure without completed state")]
    FailureRequiresCompletedActivity { sequence: ActivitySequence },
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
    #[error("activity {sequence} input uses {actual} bytes; declared maximum is {maximum}")]
    ActivityInputExceedsDeclared {
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
    #[error("durable effect contract is invalid: {0}")]
    EffectContract(String),
    #[error("activity {sequence} has effect metadata without an exact prepared command")]
    MissingPreparedCommand { sequence: ActivitySequence },
    #[error("activity {sequence} has an exact prepared command without effect classification")]
    MissingCompletionClass { sequence: ActivitySequence },
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
        if record.attempt.ordinal == 0
            || record.attempt.ordinal > record.spec.options().max_attempts()
        {
            return Err(CheckpointError::ActivityAttemptLimitExceeded {
                sequence: record.sequence,
                attempted: record.attempt.ordinal,
                maximum: record.spec.options().max_attempts(),
            });
        }
        if record.attempt.ordinal == 1 && record.attempt.retry_not_before_unix_millis.is_some() {
            return Err(CheckpointError::RetryRequiresExposedActivity {
                sequence: record.sequence,
            });
        }
        let input_actual = u64::try_from(record.spec.input().as_slice().len())
            .map_err(|_| CheckpointError::ResultLengthUnrepresentable)?;
        if input_actual > record.spec.max_input_bytes() {
            return Err(CheckpointError::ActivityInputExceedsDeclared {
                sequence: record.sequence,
                actual: input_actual,
                maximum: record.spec.max_input_bytes(),
            });
        }
        match (&record.prepared_command, record.completion_class) {
            (Some(command), Some(_)) => {
                PreparedCommand::new(command.bytes().clone(), command.max_bytes())
                    .map_err(|error| CheckpointError::EffectContract(error.to_string()))?;
                validate_effect_attempts(&record.attempts)
                    .map_err(|error| CheckpointError::EffectContract(error.to_string()))?;
                validate_effect_record_state(record)?;
            }
            (None, _) if record.attempts.is_empty() => {}
            (None, Some(_)) => {
                return Err(CheckpointError::MissingPreparedCommand {
                    sequence: record.sequence,
                });
            }
            (Some(_), None) => {
                return Err(CheckpointError::MissingCompletionClass {
                    sequence: record.sequence,
                });
            }
            (None, None) => {
                return Err(CheckpointError::MissingPreparedCommand {
                    sequence: record.sequence,
                });
            }
        }
        if record.failure.is_some() && !matches!(record.state, ActivityState::Completed { .. }) {
            return Err(CheckpointError::FailureRequiresCompletedActivity {
                sequence: record.sequence,
            });
        }
        if let Some(failure) = record.failure.as_ref().and_then(ActivityFailure::payload) {
            let actual = u64::try_from(failure.as_slice().len())
                .map_err(|_| CheckpointError::ResultLengthUnrepresentable)?;
            if actual > record.spec.max_result_bytes() {
                return Err(CheckpointError::CompletedResultExceedsDeclared {
                    sequence: record.sequence,
                    actual,
                    maximum: record.spec.max_result_bytes(),
                });
            }
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

fn validate_effect_record_state(record: &ActivityRecord) -> Result<(), CheckpointError> {
    let valid = match (record.attempts.as_slice(), &record.state) {
        ([], ActivityState::Scheduled) => true,
        ([attempt], ActivityState::DispatchExposed { attempt_id }) => {
            attempt.state() == EffectAttemptState::Exposed && attempt.attempt_id() == *attempt_id
        }
        ([attempt], ActivityState::Scheduled) => {
            attempt.state() == EffectAttemptState::ProvenNoAdmission
        }
        ([attempt], ActivityState::Completed { .. }) => {
            attempt.state() == EffectAttemptState::Observed
        }
        ([first, second], ActivityState::DispatchExposed { attempt_id }) => {
            first.state() == EffectAttemptState::ProvenNoAdmission
                && second.state() == EffectAttemptState::Exposed
                && second.attempt_id() == *attempt_id
        }
        ([first, second], ActivityState::Completed { .. }) => {
            first.state() == EffectAttemptState::ProvenNoAdmission
                && second.state() == EffectAttemptState::Observed
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(CheckpointError::EffectContract(
            "effect attempt ledger does not match activity state".to_owned(),
        ))
    }
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
    fn terminal_projection_covers_authenticated_completion_metadata() {
        let payload = CheckpointPayload::active(contract(8, 1_000_000), Vec::new());
        let projected = payload.maximum_terminal_encoded_len().unwrap();
        let external = 10_000_000_000_000_000_000_u64;
        let metadata =
            CompletionMetadata::from_counts(u64::MAX, external, u64::MAX - external).unwrap();
        let terminal = CheckpointPayload::terminal_with_metadata(
            payload.execution.clone(),
            TerminalOutcome::succeeded(ExactBytes::new([7; 8])),
            metadata,
        );
        assert!(
            CheckpointEnvelope::encode(&terminal)
                .unwrap()
                .encoded_len()
                .unwrap()
                <= projected
        );
    }

    #[test]
    fn terminal_rejects_internally_inconsistent_completion_metadata() {
        let terminal = CheckpointPayload::terminal_with_metadata(
            contract(8, 1_000_000),
            TerminalOutcome::succeeded(ExactBytes::new(b"done")),
            CompletionMetadata::from_counts(2, 1, 1).unwrap(),
        );
        let mut value = serde_json::to_value(terminal).unwrap();
        value["state"]["completion_metadata"]["completed_activity_count"] =
            serde_json::json!(3_u64);
        let tampered: CheckpointPayload = serde_json::from_value(value).unwrap();
        assert!(matches!(
            tampered.validate(
                &spec(8),
                CheckpointLimits::new(4, 1_000_000, 1_000_000).unwrap()
            ),
            Err(CheckpointError::EffectContract(_))
        ));
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

    #[test]
    fn prepared_effect_command_and_attempt_ledger_round_trip_exactly() {
        let activity_spec = ActivitySpec::new(
            ActivityName::new("effect", 1).unwrap(),
            ExactBytes::new(br#"{"request":1}"#),
            64,
        );
        let first = AttemptId::new(crate::HostEpoch::from_bytes([2; 16]), 1).unwrap();
        let second = AttemptId::new(crate::HostEpoch::from_bytes([2; 16]), 2).unwrap();
        let record = ActivityRecord::prepared_effect(
            ActivitySequence::new(0),
            activity_spec,
            PreparedCommand::new(ExactBytes::new(br#"{"command":2}"#), 13).unwrap(),
            CompletionClass::ExternalEffect,
            vec![
                EffectAttempt::new(first, crate::EffectAttemptState::ProvenNoAdmission),
                EffectAttempt::new(second, crate::EffectAttemptState::Exposed),
            ],
            ActivityState::DispatchExposed { attempt_id: second },
        )
        .unwrap();
        let payload = CheckpointPayload::active(
            ExecutionContract::with_encoded_limits(spec(16), 4096, 4096),
            vec![record],
        );
        let limits = CheckpointLimits::new(1, 4096, 4096).unwrap();
        let envelope = CheckpointEnvelope::encode_with_limits(&payload, limits).unwrap();
        let decoded = envelope.decode_and_validate(&spec(16), limits).unwrap();
        let decoded = &decoded.active_activities().unwrap()[0];
        assert_eq!(
            decoded.prepared_command().unwrap().bytes().as_slice(),
            br#"{"command":2}"#
        );
        assert_eq!(
            decoded.completion_class(),
            Some(CompletionClass::ExternalEffect)
        );
        assert_eq!(decoded.attempts().len(), 2);
    }
}
