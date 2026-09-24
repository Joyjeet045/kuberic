//! Side-effect-free reconciliation outcomes returned by the evaluator.

use serde::{Deserialize, Serialize};

use crate::command::{KubernetesChange, ProtocolCommand, SafetyChange};
use crate::types::AcceptedStatus;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WaitReason {
    ActiveTransition,
    ProvisioningInProgress,
    AgentUnavailable,
    AwaitingStableEvidence,
    AwaitingAgentInitialization,
    UnsupportedSpecDuringTransition,
    FailoverDelay,
    QuorumLoss,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum UnsafeReason {
    InvalidDesiredState(String),
    InvalidAcceptedAuthority(String),
    DurableEvidenceWithoutAuthority,
    IncompatibleProtocolVersion {
        replica_id: i64,
        expected: u32,
        observed: u32,
    },
    ContradictoryReplicaEvidence(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
/// Complete outcome of one pure evaluation cycle.
pub enum Plan {
    Stable {
        status: AcceptedStatus,
        requeue_after_seconds: u64,
    },
    Apply {
        changes: Vec<KubernetesChange>,
    },
    Execute {
        command: ProtocolCommand,
    },
    Wait {
        reason: WaitReason,
        status: AcceptedStatus,
        requeue_after_seconds: u64,
    },
    Unsafe {
        reason: UnsafeReason,
        status: AcceptedStatus,
        safety_changes: Vec<SafetyChange>,
        requeue_after_seconds: u64,
    },
}
