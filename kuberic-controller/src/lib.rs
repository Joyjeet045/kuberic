pub mod cluster_api;
pub mod crd;
mod exact_resources;
pub mod executor;
pub mod normalize;
pub mod observation;
pub mod reconciler;

use kuberic_protocol::evaluator::EvaluationConfig;
use thiserror::Error;

pub fn production_evaluation_config(
    stable_resync_seconds: u64,
    wait_requeue_seconds: u64,
    unsafe_requeue_seconds: u64,
) -> EvaluationConfig {
    EvaluationConfig {
        enable_secondary_scale_down: true,
        allow_scale_up: true,
        supported_protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        stable_resync_seconds,
        wait_requeue_seconds,
        unsafe_requeue_seconds,
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ControllerError {
    #[error("observation failed: {0}")]
    Observation(String),
    #[error("transient observation failed: {0}")]
    TransientObservation(String),
    #[error("observed Kubernetes object changed; a fresh observation is required")]
    ObservationStale,
    #[error("agent is unavailable: {0}")]
    AgentUnavailable(String),
    #[error("agent returned invalid evidence: {0}")]
    InvalidAgentEvidence(String),
    #[error("effect failed: {0}")]
    Effect(String),
}

pub type Result<T> = std::result::Result<T, ControllerError>;

#[cfg(test)]
mod tests {
    #[test]
    fn production_enables_only_completed_membership_gates() {
        let config = super::production_evaluation_config(31, 7, 13);
        assert!(config.enable_secondary_scale_down);
        assert!(config.allow_scale_up);
        assert_eq!(
            config.supported_protocol_version,
            kuberic_protocol::PROTOCOL_VERSION
        );
        assert_eq!(config.stable_resync_seconds, 31);
        assert_eq!(config.wait_requeue_seconds, 7);
        assert_eq!(config.unsafe_requeue_seconds, 13);
    }
}
