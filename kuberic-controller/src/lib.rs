//! Kubernetes control plane and pure level-triggered reconciliation policy.
//!
//! Canonical command, observation, validation, and transport contracts are
//! owned by `kuberic_runtime::protocol` and `kuberic_runtime::control`.
//! They are re-exported here so controller integrations need only this crate.

pub mod cluster_api;
pub mod crd;
mod error;
pub mod evaluator;
mod exact_resources;
pub mod executor;
pub mod normalize;
pub mod observation;
pub mod plan;
pub mod reconciler;

pub use error::{ControllerError, Result};
pub use evaluator::{EvaluationConfig, evaluate};
pub use kuberic_runtime::{control, protocol};
pub use plan::{Plan, UnsafeReason, WaitReason};

pub fn production_evaluation_config(
    stable_resync_seconds: u64,
    wait_requeue_seconds: u64,
    unsafe_requeue_seconds: u64,
) -> EvaluationConfig {
    EvaluationConfig {
        enable_secondary_scale_down: true,
        allow_scale_up: true,
        supported_protocol_version: kuberic_runtime::protocol::PROTOCOL_VERSION,
        stable_resync_seconds,
        wait_requeue_seconds,
        unsafe_requeue_seconds,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn production_enables_only_completed_membership_gates() {
        let config = super::production_evaluation_config(31, 7, 13);
        assert!(config.enable_secondary_scale_down);
        assert!(config.allow_scale_up);
        assert_eq!(
            config.supported_protocol_version,
            kuberic_runtime::protocol::PROTOCOL_VERSION
        );
        assert_eq!(config.stable_resync_seconds, 31);
        assert_eq!(config.wait_requeue_seconds, 7);
        assert_eq!(config.unsafe_requeue_seconds, 13);
    }
}
