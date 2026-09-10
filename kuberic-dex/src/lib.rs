//! Kuberic DEX durable execution kernel primitives.
//!
//! This crate is isolated from Kuberic production components. Its current
//! ordinary-async authoring API is paired with a runtime-neutral asynchronous
//! provider and host contract. Dispatch permission is available only after
//! separate accepted schedule and exposure checkpoint transitions. The crate
//! owns no executor and does not depend on Tokio.
//!
//! The crate README documents the selected
//! [ordinary-async authoring surface](../README.md#selected-authoring-surface),
//! [replay and checkpoint semantics](../README.md#replay-and-checkpoint-semantics),
//! [turn and dispatch-permission boundary](../README.md#turns-and-dispatch-permission),
//! [quarantine recovery](../README.md#quarantine-and-observation-recovery), and
//! [bounded limitations](../README.md#limitations-and-exclusions).

mod assessment;
mod checkpoint;
mod effect;
mod host;
mod identity;
mod in_memory;
#[cfg(feature = "kubernetes")]
mod kubernetes;
mod registry;
mod replay;
mod store;
mod typed;
mod workflow;

pub use assessment::{FeasibilityClassification, FeasibilityInputs, classify_feasibility};
pub use checkpoint::{
    ActivityAttemptState, ActivityFailure, ActivityRecord, ActivityState,
    CHECKPOINT_FORMAT_VERSION, CheckpointEnvelope, CheckpointError, CheckpointLimits,
    CheckpointPayload, CheckpointState, ExecutionContract,
};
pub use effect::{
    BoundedEffectError, CompletionClass, CompletionMetadata, DurableEffect, EffectActivity,
    EffectAttempt, EffectAttemptState, EffectCallError, EffectContractError, EffectErrorKind,
    EffectHostStep, EffectMetadata, EffectObservationDisposition, EffectOutcome, PreparedCommand,
    PreparedEffectResolver, decode_effect_command, decode_effect_observation,
    decode_effect_request, encode_effect_command, encode_effect_request, validate_effect_attempts,
};
/// Narrow integration surface for optional strict-effect activity handlers.
#[doc(hidden)]
pub mod strict {
    pub use crate::effect::{
        DispatchEffect, EffectQuarantineContext, ObserveEffect, ObserveQuarantinedEffect,
        PrepareEffect, observe_or_dispatch_effect, observe_quarantined_effect,
        resolve_prepared_effect,
    };
}
pub use host::{
    ActivityObservation, DispatchPermit, DurableHost, EffectObservation, HOST_OUTCOME_VARIANTS,
    HostOutcome, ObservationRejection, PersistenceBoundary, ReloadReason, StoreOperation,
    TerminalCheckpointStatus,
};
pub use identity::{
    ActivityName, ActivityOptions, ActivitySequence, ActivitySpec, AttemptId, ExactBytes,
    ExecutionId, ExecutionSpec, HostEpoch, IdentityError, LogicalActivityId,
};
pub use in_memory::{InMemoryCheckpointStore, InMemoryFault};
#[cfg(feature = "kubernetes")]
pub use kubernetes::{
    DEFAULT_CONFIG_MAP_DATA_BUDGET_BYTES, KubernetesCheckpointMetrics,
    KubernetesCheckpointMetricsSnapshot, KubernetesCheckpointOwner, KubernetesCheckpointOwnerScope,
    KubernetesCheckpointStore, KubernetesCheckpointStoreOptions, MAX_CONFIG_MAP_DATA_BUDGET_BYTES,
};
pub use registry::{
    ActivityContext, ActivityHandlerError, ActivityInvocationOutcome, ActivityInvocationRuntime,
    ActivityRegistry, ActivityRegistryBuilder, ActivityRegistryError, ActivityRunner,
    ActivityTimeoutRuntime, ActivityWakeups, ScopedActivityRegistry, ScopedActivityRegistryBuilder,
    ScopedHandlerFuture, ScopedTypedHandlerFuture,
};
pub use replay::{Evaluation, Nondeterminism, evaluate, evaluate_effects, evaluate_prepared};
pub use store::{
    CasOutcome, CheckpointStore, StorageRevision, StoreError, StoreErrorKind, StoredCheckpoint,
};
pub use typed::{
    ActivityCallError, ActivityInvocationError, DurableActivity, IdentityActivityResolver,
    PreparedActivityError, PreparedActivityResolver, decode_activity_input, decode_activity_result,
    encode_activity_input, encode_activity_result,
};
pub use workflow::{TerminalOutcome, Workflow, WorkflowContext};
