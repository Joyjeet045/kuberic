pub mod cluster_api;
pub mod crd;
pub mod executor;
pub mod normalize;
pub mod observation;
pub mod reconciler;

use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ControllerError {
    #[error("observation failed: {0}")]
    Observation(String),
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
