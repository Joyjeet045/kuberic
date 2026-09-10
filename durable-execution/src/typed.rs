use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::{ActivityName, ActivityOptions, ActivitySpec, ExactBytes};

/// Deterministic failure while resolving a logical activity into the exact
/// specification that may be exposed.
#[derive(Clone, Debug, Deserialize, Eq, Error, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PreparedActivityError {
    #[error("prepared activity derivation failed")]
    Derivation,
    #[error("prepared activity validation failed")]
    Validation,
    #[error("prepared activity encoding failed")]
    Encoding,
    #[error(
        "prepared activity input is {actual_bytes} bytes, exceeding the {max_bytes}-byte bound"
    )]
    InputTooLarge { actual_bytes: u64, max_bytes: u64 },
    #[error(
        "prepared activity result bound is {actual_bytes} bytes, exceeding the {max_bytes}-byte bound"
    )]
    ResultBoundTooLarge { actual_bytes: u64, max_bytes: u64 },
    #[error(
        "prepared activity command is {actual_bytes} bytes, exceeding the {max_bytes}-byte bound"
    )]
    CommandTooLarge { actual_bytes: u64, max_bytes: u64 },
    #[error(
        "prepared activity command bound {actual_bytes} differs from the declared {max_bytes}-byte bound"
    )]
    CommandBoundMismatch { actual_bytes: u64, max_bytes: u64 },
}

/// Opt-in resolver for replacing a logical request with its exact bounded
/// dispatch specification.
///
/// `recorded` is the authoritative complete specification during replay.
/// Implementations must validate it against the logical request and return the
/// exact specification they expect; the kernel performs the final byte-for-byte
/// comparison.
pub trait PreparedActivityResolver: Sync {
    fn resolve(
        &self,
        logical: &ActivitySpec,
        recorded: Option<&ActivitySpec>,
    ) -> Result<ActivitySpec, PreparedActivityError>;
}

/// Identity preparation used by all existing workflow callers.
#[derive(Clone, Copy, Debug, Default)]
pub struct IdentityActivityResolver;

impl PreparedActivityResolver for IdentityActivityResolver {
    fn resolve(
        &self,
        logical: &ActivitySpec,
        _recorded: Option<&ActivitySpec>,
    ) -> Result<ActivitySpec, PreparedActivityError> {
        Ok(logical.clone())
    }
}

pub(crate) static IDENTITY_ACTIVITY_RESOLVER: IdentityActivityResolver = IdentityActivityResolver;

/// A versioned, bounded durable activity contract.
///
/// Workflow bodies invoke an activity with [`crate::WorkflowContext::call`].
/// The activity type, rather than each call site, owns its immutable replay
/// identity and encoded payload limits.
///
/// Domain rejection and failure belong in `Output`; they are durable activity
/// results rather than a second kernel failure lifecycle.
/// Implementations must serialize equal values deterministically. The built-in
/// codec canonicalizes JSON object-key order before exact-byte matching.
///
/// ```compile_fail
/// use kuberic_durable_execution::DurableActivity;
///
/// struct NotSerializable;
/// struct InvalidActivity;
///
/// impl DurableActivity for InvalidActivity {
///     type Input = NotSerializable;
///     type Output = ();
///     const NAME: &'static str = "invalid";
///     const VERSION: u32 = 1;
///     const MAX_INPUT_BYTES: u64 = 16;
///     const MAX_RESULT_BYTES: u64 = 16;
/// }
/// ```
///
/// ```compile_fail
/// use kuberic_durable_execution::DurableActivity;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize, Serialize)]
/// struct Input;
/// struct NotSerializable;
/// struct InvalidActivity;
///
/// impl DurableActivity for InvalidActivity {
///     type Input = Input;
///     type Output = NotSerializable;
///     const NAME: &'static str = "invalid-output";
///     const VERSION: u32 = 1;
///     const MAX_INPUT_BYTES: u64 = 16;
///     const MAX_RESULT_BYTES: u64 = 16;
/// }
/// ```
///
/// ```compile_fail
/// use kuberic_durable_execution::{DurableActivity, WorkflowContext};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize, Serialize)]
/// struct ExpectedInput;
/// struct Activity;
///
/// impl DurableActivity for Activity {
///     type Input = ExpectedInput;
///     type Output = ();
///     const NAME: &'static str = "typed-input";
///     const VERSION: u32 = 1;
///     const MAX_INPUT_BYTES: u64 = 16;
///     const MAX_RESULT_BYTES: u64 = 16;
/// }
///
/// async fn invalid_call(context: &mut WorkflowContext<'_>) {
///     context.call::<Activity>("wrong input").await;
/// }
/// ```
pub trait DurableActivity {
    type Input: Serialize + DeserializeOwned;
    type Output: Serialize + DeserializeOwned;

    const NAME: &'static str;
    const VERSION: u32;
    const MAX_INPUT_BYTES: u64;
    const MAX_RESULT_BYTES: u64;

    #[doc(hidden)]
    fn completion_class() -> Option<crate::CompletionClass> {
        None
    }

    #[doc(hidden)]
    fn strict_effect_metadata() -> Option<crate::EffectMetadata> {
        None
    }
}

/// Portable deterministic failure while constructing or decoding a typed call.
#[derive(Clone, Debug, Deserialize, Eq, Error, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActivityCallError {
    #[error("activity name must not be empty")]
    EmptyName,
    #[error("activity version must be greater than zero")]
    ZeroVersion,
    #[error("registered activity name {registered:?} does not match contract name {contract:?}")]
    NameMismatch {
        registered: String,
        contract: String,
    },
    #[error("activity {0} is not registered")]
    UnregisteredActivity(String),
    #[error("activity handler failed: {0}")]
    Handler(String),
    #[error("activity input could not be encoded")]
    InputEncoding,
    #[error("activity input is {actual_bytes} bytes, exceeding the {max_bytes}-byte bound")]
    InputTooLarge { actual_bytes: u64, max_bytes: u64 },
    #[error("activity input could not be decoded")]
    InputDecoding,
    #[error("activity result could not be encoded")]
    ResultEncoding,
    #[error("activity result is {actual_bytes} bytes, exceeding the {max_bytes}-byte bound")]
    ResultTooLarge { actual_bytes: u64, max_bytes: u64 },
    #[error("activity result could not be decoded")]
    ResultDecoding,
}

/// Durable result of invoking an ordinary activity.
#[derive(Clone, Debug, Deserialize, Eq, Error, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActivityInvocationError {
    #[error(transparent)]
    Call(#[from] ActivityCallError),
    #[error("activity application failed")]
    Application(ExactBytes),
    #[error("activity attempt timed out")]
    TimedOut,
    #[error("activity action deadline was exceeded")]
    ActionDeadlineExceeded,
}

/// Encode and bound a typed activity input with the canonical JSON codec.
pub fn encode_activity_input<A: DurableActivity>(
    input: &A::Input,
) -> Result<ExactBytes, ActivityCallError> {
    let encoded = canonical_json(input).map_err(|_| ActivityCallError::InputEncoding)?;
    enforce_bound(encoded.len(), A::MAX_INPUT_BYTES, PayloadKind::Input)?;
    Ok(ExactBytes::new(encoded))
}

/// Decode a typed activity input received by an activity adapter.
pub fn decode_activity_input<A: DurableActivity>(
    input: &ExactBytes,
) -> Result<A::Input, ActivityCallError> {
    enforce_bound(
        input.as_slice().len(),
        A::MAX_INPUT_BYTES,
        PayloadKind::Input,
    )?;
    serde_json::from_slice(input.as_slice()).map_err(|_| ActivityCallError::InputDecoding)
}

/// Encode and bound a typed activity result for durable observation.
pub fn encode_activity_result<A: DurableActivity>(
    result: &A::Output,
) -> Result<ExactBytes, ActivityCallError> {
    let encoded = canonical_json(result).map_err(|_| ActivityCallError::ResultEncoding)?;
    enforce_bound(encoded.len(), A::MAX_RESULT_BYTES, PayloadKind::Result)?;
    Ok(ExactBytes::new(encoded))
}

/// Decode a bounded typed activity result during workflow replay.
pub fn decode_activity_result<A: DurableActivity>(
    result: &ExactBytes,
) -> Result<A::Output, ActivityCallError> {
    enforce_bound(
        result.as_slice().len(),
        A::MAX_RESULT_BYTES,
        PayloadKind::Result,
    )?;
    serde_json::from_slice(result.as_slice()).map_err(|_| ActivityCallError::ResultDecoding)
}

pub(crate) fn activity_spec<A: DurableActivity>(
    input: &A::Input,
) -> Result<ActivitySpec, ActivityCallError> {
    activity_spec_named::<A>(A::NAME, input, ActivityOptions::default())
}

pub(crate) fn activity_spec_named<A: DurableActivity>(
    name: &str,
    input: &A::Input,
    options: ActivityOptions,
) -> Result<ActivitySpec, ActivityCallError> {
    if name != A::NAME {
        return Err(ActivityCallError::NameMismatch {
            registered: name.to_owned(),
            contract: A::NAME.to_owned(),
        });
    }
    let name = ActivityName::new(name, A::VERSION).map_err(|error| match error {
        crate::IdentityError::EmptyActivityName => ActivityCallError::EmptyName,
        crate::IdentityError::ZeroActivityVersion => ActivityCallError::ZeroVersion,
        crate::IdentityError::ZeroActivityAttempts | crate::IdentityError::ZeroAttemptCounter => {
            unreachable!("activity identity construction does not create an attempt")
        }
    })?;
    Ok(ActivitySpec::with_bounds_and_options(
        name,
        encode_activity_input::<A>(input)?,
        A::MAX_INPUT_BYTES,
        A::MAX_RESULT_BYTES,
        options,
    ))
}

#[derive(Clone, Copy)]
enum PayloadKind {
    Input,
    Result,
}

fn enforce_bound(
    actual_bytes: usize,
    max_bytes: u64,
    kind: PayloadKind,
) -> Result<(), ActivityCallError> {
    let actual_bytes = u64::try_from(actual_bytes).unwrap_or(u64::MAX);
    if actual_bytes <= max_bytes {
        return Ok(());
    }
    Err(match kind {
        PayloadKind::Input => ActivityCallError::InputTooLarge {
            actual_bytes,
            max_bytes,
        },
        PayloadKind::Result => ActivityCallError::ResultTooLarge {
            actual_bytes,
            max_bytes,
        },
    })
}

fn canonical_json<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    let mut value = serde_json::to_value(value)?;
    canonicalize_object_keys(&mut value);
    serde_json::to_vec(&value)
}

fn canonicalize_object_keys(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                canonicalize_object_keys(value);
            }
        }
        serde_json::Value::Object(object) => {
            let mut entries = std::mem::take(object).into_iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            for (_, value) in &mut entries {
                canonicalize_object_keys(value);
            }
            object.extend(entries);
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {}
    }
}
