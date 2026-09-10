use std::{fmt, marker::PhantomData};

use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::{
    ActivityCallError, ActivitySpec, AttemptId, DurableActivity, ExactBytes, ExecutionId,
    PreparedActivityError,
};

/// Immutable framework classification authenticated with completed history.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionClass {
    ExternalEffect,
    PassiveObservation,
}

/// Kernel-authenticated summary retained after activity history compaction.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionMetadata {
    completed_activity_count: u64,
    external_effect_count: u64,
    passive_observation_count: u64,
}

impl CompletionMetadata {
    pub fn from_counts(
        completed_activity_count: u64,
        external_effect_count: u64,
        passive_observation_count: u64,
    ) -> Result<Self, EffectContractError> {
        let classified = external_effect_count
            .checked_add(passive_observation_count)
            .ok_or(EffectContractError::CompletionCountOverflow)?;
        if classified != completed_activity_count {
            return Err(EffectContractError::CompletionCountMismatch {
                completed: completed_activity_count,
                classified,
            });
        }
        Ok(Self {
            completed_activity_count,
            external_effect_count,
            passive_observation_count,
        })
    }

    pub fn from_classes(
        classes: impl IntoIterator<Item = CompletionClass>,
    ) -> Result<Self, EffectContractError> {
        let mut completed_activity_count = 0_u64;
        let mut external_effect_count = 0_u64;
        let mut passive_observation_count = 0_u64;
        for class in classes {
            completed_activity_count = completed_activity_count
                .checked_add(1)
                .ok_or(EffectContractError::CompletionCountOverflow)?;
            match class {
                CompletionClass::ExternalEffect => {
                    external_effect_count = external_effect_count
                        .checked_add(1)
                        .ok_or(EffectContractError::CompletionCountOverflow)?;
                }
                CompletionClass::PassiveObservation => {
                    passive_observation_count = passive_observation_count
                        .checked_add(1)
                        .ok_or(EffectContractError::CompletionCountOverflow)?;
                }
            }
        }
        Self::from_counts(
            completed_activity_count,
            external_effect_count,
            passive_observation_count,
        )
    }

    pub const fn completed_activity_count(self) -> u64 {
        self.completed_activity_count
    }

    pub const fn external_effect_count(self) -> u64 {
        self.external_effect_count
    }

    pub const fn passive_observation_count(self) -> u64 {
        self.passive_observation_count
    }
}

/// Auditable state of one dispatch attempt in a bounded effect ledger.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum EffectAttemptState {
    Exposed,
    ProvenNoAdmission,
    Observed,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EffectAttempt {
    attempt_id: AttemptId,
    state: EffectAttemptState,
}

impl EffectAttempt {
    pub const fn new(attempt_id: AttemptId, state: EffectAttemptState) -> Self {
        Self { attempt_id, state }
    }

    pub const fn attempt_id(self) -> AttemptId {
        self.attempt_id
    }

    pub const fn state(self) -> EffectAttemptState {
        self.state
    }
}

/// Validate the framework's at-most-two-attempt redelivery model.
pub fn validate_effect_attempts(attempts: &[EffectAttempt]) -> Result<(), EffectContractError> {
    match attempts {
        [] => Ok(()),
        [_] => Ok(()),
        [first, second]
            if first.state == EffectAttemptState::ProvenNoAdmission
                && first.attempt_id != second.attempt_id =>
        {
            Ok(())
        }
        [first, second] if first.attempt_id == second.attempt_id => {
            Err(EffectContractError::DuplicateAttemptIdentity)
        }
        [..] if attempts.len() > 2 => Err(EffectContractError::TooManyAttempts {
            actual: attempts.len(),
        }),
        _ => Err(EffectContractError::InvalidRedelivery),
    }
}

/// Bounded, portable failure returned to deterministic workflow code.
#[derive(Clone, Debug, Deserialize, Eq, Error, PartialEq, Serialize)]
#[error("{kind}: {message}")]
#[serde(deny_unknown_fields)]
pub struct BoundedEffectError {
    kind: EffectErrorKind,
    message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    observed_at_unix_seconds: Option<i64>,
}

impl BoundedEffectError {
    pub fn new(
        kind: EffectErrorKind,
        message: impl Into<String>,
        max_message_bytes: u64,
    ) -> Result<Self, EffectContractError> {
        let message = message.into();
        let actual_bytes = u64::try_from(message.len()).unwrap_or(u64::MAX);
        if actual_bytes > max_message_bytes {
            return Err(EffectContractError::ErrorMessageTooLarge {
                actual_bytes,
                max_bytes: max_message_bytes,
            });
        }
        Ok(Self {
            kind,
            message,
            observed_at_unix_seconds: None,
        })
    }

    pub fn observed_at(
        kind: EffectErrorKind,
        message: impl Into<String>,
        observed_at_unix_seconds: i64,
        max_message_bytes: u64,
    ) -> Result<Self, EffectContractError> {
        let mut error = Self::new(kind, message, max_message_bytes)?;
        error.observed_at_unix_seconds = Some(observed_at_unix_seconds);
        Ok(error)
    }

    pub const fn kind(&self) -> EffectErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub const fn observed_at_unix_seconds(&self) -> Option<i64> {
        self.observed_at_unix_seconds
    }

    pub fn validate(&self, max_message_bytes: u64) -> Result<(), EffectContractError> {
        let actual_bytes = u64::try_from(self.message.len()).unwrap_or(u64::MAX);
        if actual_bytes > max_message_bytes {
            return Err(EffectContractError::ErrorMessageTooLarge {
                actual_bytes,
                max_bytes: max_message_bytes,
            });
        }
        Ok(())
    }
}

/// Common non-success cases for bounded effects.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectErrorKind {
    ProvenNoAdmission,
    DomainFailure,
    DeadlineExceeded,
    UnavailableAtDeadline,
    ConflictingEvidence,
}

impl fmt::Display for EffectErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ProvenNoAdmission => "proven no admission",
            Self::DomainFailure => "domain failure",
            Self::DeadlineExceeded => "deadline exceeded",
            Self::UnavailableAtDeadline => "unavailable at deadline",
            Self::ConflictingEvidence => "conflicting evidence",
        })
    }
}

/// Host-facing outcome shared by every durable effect.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", content = "value", rename_all = "snake_case")]
pub enum EffectOutcome<T> {
    Applied(T),
    ProvenNoAdmission,
    DomainFailure(BoundedEffectError),
    DeadlineExceeded(BoundedEffectError),
    UnavailableAtDeadline(BoundedEffectError),
    ConflictingEvidence(BoundedEffectError),
}

impl<T> EffectOutcome<T> {
    /// Convert the durable host outcome into the workflow-facing result used by
    /// ordinary `?` propagation.
    pub fn into_workflow_result(self, max_message_bytes: u64) -> Result<T, EffectCallError> {
        match self {
            Self::Applied(output) => Ok(output),
            Self::ProvenNoAdmission => {
                let error = BoundedEffectError::new(
                    EffectErrorKind::ProvenNoAdmission,
                    "dispatch was proven not admitted",
                    max_message_bytes,
                )?;
                Err(error.into())
            }
            Self::DomainFailure(error)
            | Self::DeadlineExceeded(error)
            | Self::UnavailableAtDeadline(error)
            | Self::ConflictingEvidence(error) => {
                error.validate(max_message_bytes)?;
                Err(error.into())
            }
        }
    }
}

/// Stable metadata and independent payload bounds for one durable effect.
///
/// Preparation, dispatch, and observation are deliberately separate traits so
/// replay can validate a recorded command without possessing dispatch
/// authority.
pub trait DurableEffect {
    type Request: Serialize + DeserializeOwned + Send + Sync;
    type Command: Serialize + DeserializeOwned + Send + Sync;
    type Output: Serialize + DeserializeOwned + Send + Sync;

    const NAME: &'static str;
    const VERSION: u32;
    const MAX_REQUEST_BYTES: u64;
    const MAX_COMMAND_BYTES: u64;
    const MAX_RESULT_BYTES: u64;
    const MAX_ERROR_MESSAGE_BYTES: u64;
    const COMPLETION_CLASS: CompletionClass;
}

/// Pure preparation and replay validation for an effect.
pub trait PrepareEffect<E: DurableEffect> {
    type Evidence;
    type Authority;
    type Error;

    fn prepare(
        &self,
        request: &E::Request,
        authority: &Self::Authority,
        evidence: &Self::Evidence,
    ) -> Result<E::Command, Self::Error>;

    fn validate_recorded(
        &self,
        request: &E::Request,
        command: &E::Command,
        authority: &Self::Authority,
    ) -> Result<(), Self::Error>;
}

/// Operation-specific dispatch semantics. The shared host supplies the exact
/// persisted command only after it has accepted the exposure transition.
#[async_trait]
pub trait DispatchEffect<E: DurableEffect>: Send {
    type Error: Send;

    async fn dispatch(
        &mut self,
        request: &E::Request,
        command: &E::Command,
        attempt_id: AttemptId,
    ) -> Result<Option<EffectOutcome<E::Output>>, Self::Error>;
}

/// Ordinary authoritative observation after a persisted exposure.
#[async_trait]
pub trait ObserveEffect<E: DurableEffect>: Send {
    type Error: Send;

    async fn observe(
        &mut self,
        request: &E::Request,
        command: &E::Command,
        attempt_id: AttemptId,
    ) -> Result<Option<EffectOutcome<E::Output>>, Self::Error>;
}

/// Observation-only recovery for an outcome whose dispatch result is unknown.
///
/// This interface intentionally has no dispatch permit or dispatch method.
#[async_trait]
pub trait ObserveQuarantinedEffect<E: DurableEffect>: Send {
    type Error: Send;

    async fn observe_quarantined(
        &mut self,
        context: EffectQuarantineContext<'_, E>,
    ) -> Result<Option<EffectOutcome<E::Output>>, Self::Error>;
}

/// Evidence available to quarantine resolution without dispatch authority.
///
/// ```compile_fail
/// use kuberic_dex::{DurableEffect, EffectQuarantineContext};
///
/// fn cannot_redispatch<E: DurableEffect>(context: EffectQuarantineContext<'_, E>) {
///     let _permit = context.dispatch_permit();
/// }
/// ```
#[derive(Clone, Copy, Debug)]
pub struct EffectQuarantineContext<'a, E: DurableEffect> {
    request: &'a E::Request,
    command: &'a E::Command,
    attempt_id: AttemptId,
}

impl<'a, E: DurableEffect> EffectQuarantineContext<'a, E> {
    pub const fn new(
        request: &'a E::Request,
        command: &'a E::Command,
        attempt_id: AttemptId,
    ) -> Self {
        Self {
            request,
            command,
            attempt_id,
        }
    }

    pub const fn request(&self) -> &'a E::Request {
        self.request
    }

    pub const fn command(&self) -> &'a E::Command {
        self.command
    }

    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }
}

/// Immutable metadata carried from a typed effect definition into history.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EffectMetadata {
    max_command_bytes: u64,
    completion_class: CompletionClass,
}

/// Result of one framework-owned typed lifecycle evaluation.
#[derive(Debug, Eq, PartialEq)]
pub enum EffectHostStep {
    Observed(Box<crate::EffectObservation>),
    Pending,
}

fn validate_effect_activity<E: DurableEffect>(
    activity: &ActivitySpec,
) -> Result<E::Request, PreparedActivityError> {
    if activity.name().name() != E::NAME
        || activity.name().version() != E::VERSION
        || activity.max_input_bytes() != E::MAX_REQUEST_BYTES
        || activity.max_result_bytes() != E::MAX_RESULT_BYTES
    {
        return Err(PreparedActivityError::Validation);
    }
    decode_effect_request::<E>(activity.input()).map_err(|_| PreparedActivityError::Encoding)
}

/// Resolve one explicitly routed strict-effect command.
///
/// Callers own the small operation-specific route table; the durable crate
/// owns codec, bound, replay-validation, and prepared-command invariants.
pub fn resolve_prepared_effect<E, H>(
    handler: &H,
    logical: &ActivitySpec,
    metadata: EffectMetadata,
    recorded: Option<&PreparedCommand>,
) -> Result<PreparedCommand, PreparedActivityError>
where
    E: DurableEffect,
    H: PrepareEffect<E, Evidence = (), Authority = ()>,
{
    if metadata != EffectMetadata::of::<E>() {
        return Err(PreparedActivityError::Validation);
    }
    let request = validate_effect_activity::<E>(logical)?;
    let command = if let Some(recorded) = recorded {
        if recorded.max_bytes() != E::MAX_COMMAND_BYTES {
            return Err(PreparedActivityError::Validation);
        }
        let command =
            decode_effect_command::<E>(recorded).map_err(|_| PreparedActivityError::Validation)?;
        handler
            .validate_recorded(&request, &command, &())
            .map_err(|_| PreparedActivityError::Validation)?;
        command
    } else {
        handler
            .prepare(&request, &(), &())
            .map_err(|_| PreparedActivityError::Derivation)?
    };
    encode_effect_command::<E>(&command).map_err(|error| match error {
        EffectContractError::CommandTooLarge {
            actual_bytes,
            max_bytes,
        } => PreparedActivityError::CommandTooLarge {
            actual_bytes,
            max_bytes,
        },
        _ => PreparedActivityError::Encoding,
    })
}

/// Execute observe-before-dispatch for one explicitly routed strict effect.
pub async fn observe_or_dispatch_effect<E, H>(
    handler: &mut H,
    activity: &crate::LogicalActivityId,
    attempt_id: AttemptId,
    prepared: &PreparedCommand,
) -> Result<EffectHostStep, String>
where
    E: DurableEffect,
    H: Send + DispatchEffect<E> + ObserveEffect<E>,
    <H as DispatchEffect<E>>::Error: std::fmt::Display,
    <H as ObserveEffect<E>>::Error: std::fmt::Display,
{
    let request =
        validate_effect_activity::<E>(activity.spec()).map_err(|error| error.to_string())?;
    if prepared.max_bytes() != E::MAX_COMMAND_BYTES {
        return Err("prepared command bound differs from the effect contract".to_string());
    }
    let command =
        decode_effect_command::<E>(prepared).map_err(|error| format!("decode command: {error}"))?;
    if let Some(outcome) = <H as ObserveEffect<E>>::observe(handler, &request, &command, attempt_id)
        .await
        .map_err(|error| error.to_string())?
    {
        return crate::EffectObservation::from_outcome::<E>(activity.clone(), attempt_id, &outcome)
            .map(Box::new)
            .map(EffectHostStep::Observed)
            .map_err(|error| error.to_string());
    }
    match <H as DispatchEffect<E>>::dispatch(handler, &request, &command, attempt_id)
        .await
        .map_err(|error| error.to_string())?
    {
        Some(outcome) => {
            crate::EffectObservation::from_outcome::<E>(activity.clone(), attempt_id, &outcome)
                .map(Box::new)
                .map(EffectHostStep::Observed)
                .map_err(|error| error.to_string())
        }
        None => Ok(EffectHostStep::Pending),
    }
}

/// Observe one quarantined explicitly routed strict effect without dispatch.
pub async fn observe_quarantined_effect<E, H>(
    handler: &mut H,
    activity: &crate::LogicalActivityId,
    attempt_id: AttemptId,
    prepared: &PreparedCommand,
) -> Result<Option<crate::EffectObservation>, String>
where
    E: DurableEffect,
    H: Send + ObserveQuarantinedEffect<E>,
    <H as ObserveQuarantinedEffect<E>>::Error: std::fmt::Display,
{
    let request =
        validate_effect_activity::<E>(activity.spec()).map_err(|error| error.to_string())?;
    if prepared.max_bytes() != E::MAX_COMMAND_BYTES {
        return Err("prepared command bound differs from the effect contract".to_string());
    }
    let command =
        decode_effect_command::<E>(prepared).map_err(|error| format!("decode command: {error}"))?;
    let context = EffectQuarantineContext::<E>::new(&request, &command, attempt_id);
    <H as ObserveQuarantinedEffect<E>>::observe_quarantined(handler, context)
        .await
        .map_err(|error| error.to_string())?
        .map(|outcome| {
            crate::EffectObservation::from_outcome::<E>(activity.clone(), attempt_id, &outcome)
                .map_err(|error| error.to_string())
        })
        .transpose()
}

/// Decode a persisted typed result into an authenticated strict observation.
pub fn decode_effect_observation<E: DurableEffect>(
    activity: crate::LogicalActivityId,
    attempt_id: AttemptId,
    result: &ExactBytes,
) -> Result<crate::EffectObservation, crate::ActivityCallError> {
    let outcome = crate::decode_activity_result::<EffectActivity<E>>(result)?;
    crate::EffectObservation::from_outcome::<E>(activity, attempt_id, &outcome)
}

impl EffectMetadata {
    pub const fn new(max_command_bytes: u64, completion_class: CompletionClass) -> Self {
        Self {
            max_command_bytes,
            completion_class,
        }
    }

    pub const fn of<E: DurableEffect>() -> Self {
        Self {
            max_command_bytes: E::MAX_COMMAND_BYTES,
            completion_class: E::COMPLETION_CLASS,
        }
    }

    pub const fn max_command_bytes(self) -> u64 {
        self.max_command_bytes
    }

    pub const fn completion_class(self) -> CompletionClass {
        self.completion_class
    }
}

/// Runtime-neutral preparation bridge used by deterministic replay.
///
/// On replay `recorded` is authoritative. Implementations validate it against
/// the logical request and immutable execution authority and return the same
/// exact command; they must not derive a replacement from mutable evidence.
pub trait PreparedEffectResolver: Sync {
    fn resolve(
        &self,
        execution_id: ExecutionId,
        logical: &ActivitySpec,
        metadata: EffectMetadata,
        recorded: Option<&PreparedCommand>,
    ) -> Result<PreparedCommand, PreparedActivityError>;
}

/// How an authoritative typed observation advances an exposed attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectObservationDisposition {
    Completed,
    ProvenNoAdmission,
}

/// Type adapter used by the existing typed codec while effect hosting is
/// introduced incrementally.
pub struct EffectActivity<E>(PhantomData<E>);

impl<E: DurableEffect> DurableActivity for EffectActivity<E> {
    type Input = E::Request;
    type Output = EffectOutcome<E::Output>;

    const NAME: &'static str = E::NAME;
    const VERSION: u32 = E::VERSION;
    const MAX_INPUT_BYTES: u64 = E::MAX_REQUEST_BYTES;
    const MAX_RESULT_BYTES: u64 = E::MAX_RESULT_BYTES;

    fn strict_effect_metadata() -> Option<EffectMetadata> {
        Some(EffectMetadata::of::<E>())
    }
}

/// Persistable exact prepared command, independent of the logical request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedCommand {
    bytes: ExactBytes,
    max_bytes: u64,
}

impl PreparedCommand {
    pub fn new(bytes: ExactBytes, max_bytes: u64) -> Result<Self, EffectContractError> {
        let actual_bytes = u64::try_from(bytes.as_slice().len()).unwrap_or(u64::MAX);
        if actual_bytes > max_bytes {
            return Err(EffectContractError::CommandTooLarge {
                actual_bytes,
                max_bytes,
            });
        }
        Ok(Self { bytes, max_bytes })
    }

    pub const fn bytes(&self) -> &ExactBytes {
        &self.bytes
    }

    pub const fn max_bytes(&self) -> u64 {
        self.max_bytes
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum EffectContractError {
    #[error("effect request could not be encoded")]
    RequestEncoding,
    #[error("effect request could not be decoded")]
    RequestDecoding,
    #[error("effect command could not be encoded")]
    CommandEncoding,
    #[error("effect command could not be decoded")]
    CommandDecoding,
    #[error("effect command is {actual_bytes} bytes, exceeding the {max_bytes}-byte bound")]
    CommandTooLarge { actual_bytes: u64, max_bytes: u64 },
    #[error(
        "recorded command bound {actual_bytes} differs from the declared {max_bytes}-byte bound"
    )]
    CommandBoundMismatch { actual_bytes: u64, max_bytes: u64 },
    #[error("effect error text is {actual_bytes} bytes, exceeding the {max_bytes}-byte bound")]
    ErrorMessageTooLarge { actual_bytes: u64, max_bytes: u64 },
    #[error("completed activity count overflow")]
    CompletionCountOverflow,
    #[error(
        "completed activity count {completed} differs from classified activity count {classified}"
    )]
    CompletionCountMismatch { completed: u64, classified: u64 },
    #[error("a durable effect may record at most two attempts, found {actual}")]
    TooManyAttempts { actual: usize },
    #[error(
        "redelivery requires a first attempt proven not admitted and a distinct attempt identity"
    )]
    InvalidRedelivery,
    #[error("redelivery attempt identity must differ from the first attempt")]
    DuplicateAttemptIdentity,
    #[error(transparent)]
    Activity(#[from] ActivityCallError),
}

/// Deterministic typed failure returned by a strict activity implementation.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum EffectCallError {
    #[error(transparent)]
    Activity(#[from] ActivityCallError),
    #[error(transparent)]
    Contract(#[from] EffectContractError),
    #[error(transparent)]
    Effect(#[from] BoundedEffectError),
}

pub fn encode_effect_request<E: DurableEffect>(
    request: &E::Request,
) -> Result<ExactBytes, EffectContractError> {
    let bytes = canonical_json(request).map_err(|_| EffectContractError::RequestEncoding)?;
    enforce(
        bytes.len(),
        E::MAX_REQUEST_BYTES,
        |actual_bytes, max_bytes| {
            EffectContractError::Activity(ActivityCallError::InputTooLarge {
                actual_bytes,
                max_bytes,
            })
        },
    )?;
    Ok(ExactBytes::new(bytes))
}

pub fn decode_effect_request<E: DurableEffect>(
    request: &ExactBytes,
) -> Result<E::Request, EffectContractError> {
    enforce(
        request.as_slice().len(),
        E::MAX_REQUEST_BYTES,
        |actual_bytes, max_bytes| {
            EffectContractError::Activity(ActivityCallError::InputTooLarge {
                actual_bytes,
                max_bytes,
            })
        },
    )?;
    serde_json::from_slice(request.as_slice()).map_err(|_| EffectContractError::RequestDecoding)
}

pub fn encode_effect_command<E: DurableEffect>(
    command: &E::Command,
) -> Result<PreparedCommand, EffectContractError> {
    let bytes = canonical_json(command).map_err(|_| EffectContractError::CommandEncoding)?;
    PreparedCommand::new(ExactBytes::new(bytes), E::MAX_COMMAND_BYTES)
}

pub fn decode_effect_command<E: DurableEffect>(
    command: &PreparedCommand,
) -> Result<E::Command, EffectContractError> {
    if command.max_bytes != E::MAX_COMMAND_BYTES {
        return Err(EffectContractError::CommandBoundMismatch {
            actual_bytes: command.max_bytes,
            max_bytes: E::MAX_COMMAND_BYTES,
        });
    }
    PreparedCommand::new(command.bytes.clone(), E::MAX_COMMAND_BYTES)?;
    serde_json::from_slice(command.bytes.as_slice())
        .map_err(|_| EffectContractError::CommandDecoding)
}

fn enforce<F>(actual: usize, maximum: u64, error: F) -> Result<(), EffectContractError>
where
    F: FnOnce(u64, u64) -> EffectContractError,
{
    let actual = u64::try_from(actual).unwrap_or(u64::MAX);
    if actual <= maximum {
        Ok(())
    } else {
        Err(error(actual, maximum))
    }
}

fn canonical_json<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    let mut value = serde_json::to_value(value)?;
    canonicalize(&mut value);
    serde_json::to_vec(&value)
}

fn canonicalize(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => values.iter_mut().for_each(canonicalize),
        serde_json::Value::Object(object) => {
            let mut entries = std::mem::take(object).into_iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            for (_, value) in &mut entries {
                canonicalize(value);
            }
            object.extend(entries);
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
    struct Request {
        value: String,
    }

    #[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
    struct Command {
        value: String,
    }

    struct Effect;

    impl DurableEffect for Effect {
        type Request = Request;
        type Command = Command;
        type Output = String;

        const NAME: &'static str = "test.effect";
        const VERSION: u32 = 1;
        const MAX_REQUEST_BYTES: u64 = 14;
        const MAX_COMMAND_BYTES: u64 = 14;
        const MAX_RESULT_BYTES: u64 = 32;
        const MAX_ERROR_MESSAGE_BYTES: u64 = 8;
        const COMPLETION_CLASS: CompletionClass = CompletionClass::ExternalEffect;
    }

    #[test]
    fn request_and_command_bounds_are_independent_and_exact() {
        let exact = Request { value: "xx".into() };
        assert_eq!(
            encode_effect_request::<Effect>(&exact)
                .unwrap()
                .as_slice()
                .len(),
            14
        );
        let over = Request {
            value: "xxx".into(),
        };
        assert!(matches!(
            encode_effect_request::<Effect>(&over),
            Err(EffectContractError::Activity(
                ActivityCallError::InputTooLarge {
                    actual_bytes: 15,
                    max_bytes: 14
                }
            ))
        ));

        let exact = Command { value: "xx".into() };
        let encoded = encode_effect_command::<Effect>(&exact).unwrap();
        assert_eq!(encoded.bytes().as_slice().len(), 14);
        assert_eq!(decode_effect_command::<Effect>(&encoded).unwrap(), exact);
        let over = Command {
            value: "xxx".into(),
        };
        assert!(matches!(
            encode_effect_command::<Effect>(&over),
            Err(EffectContractError::CommandTooLarge {
                actual_bytes: 15,
                max_bytes: 14
            })
        ));

        let exact_result = crate::encode_activity_result::<EffectActivity<Effect>>(
            &EffectOutcome::Applied("x".to_owned()),
        )
        .unwrap();
        assert_eq!(exact_result.as_slice().len(), 32);
        assert!(matches!(
            crate::encode_activity_result::<EffectActivity<Effect>>(&EffectOutcome::Applied(
                "xx".to_owned(),
            )),
            Err(ActivityCallError::ResultTooLarge {
                actual_bytes: 33,
                max_bytes: 32
            })
        ));
    }

    #[test]
    fn bounded_errors_and_workflow_conversion_are_typed() {
        let error = BoundedEffectError::new(EffectErrorKind::DomainFailure, "failure", 7).unwrap();
        assert_eq!(
            EffectOutcome::<()>::DomainFailure(error.clone()).into_workflow_result(32),
            Err(EffectCallError::Effect(error))
        );
        assert!(matches!(
            BoundedEffectError::new(EffectErrorKind::DomainFailure, "too-long!", 8),
            Err(EffectContractError::ErrorMessageTooLarge {
                actual_bytes: 9,
                max_bytes: 8
            })
        ));
    }

    #[test]
    fn completion_metadata_and_redelivery_ledger_are_exact() {
        let metadata = CompletionMetadata::from_classes([
            CompletionClass::ExternalEffect,
            CompletionClass::PassiveObservation,
            CompletionClass::ExternalEffect,
        ])
        .unwrap();
        assert_eq!(metadata.completed_activity_count(), 3);
        assert_eq!(metadata.external_effect_count(), 2);
        assert_eq!(metadata.passive_observation_count(), 1);
        assert!(matches!(
            CompletionMetadata::from_counts(3, 1, 1),
            Err(EffectContractError::CompletionCountMismatch {
                completed: 3,
                classified: 2
            })
        ));

        let first = AttemptId::new(crate::HostEpoch::from_bytes([1; 16]), 1).unwrap();
        let second = AttemptId::new(crate::HostEpoch::from_bytes([1; 16]), 2).unwrap();
        assert!(
            validate_effect_attempts(&[
                EffectAttempt::new(first, EffectAttemptState::ProvenNoAdmission),
                EffectAttempt::new(second, EffectAttemptState::Exposed),
            ])
            .is_ok()
        );
        assert!(matches!(
            validate_effect_attempts(&[
                EffectAttempt::new(first, EffectAttemptState::Exposed),
                EffectAttempt::new(second, EffectAttemptState::Exposed),
            ]),
            Err(EffectContractError::InvalidRedelivery)
        ));
    }
}
