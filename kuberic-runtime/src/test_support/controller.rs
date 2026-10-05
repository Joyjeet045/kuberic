//! Side-effect-free reconciliation outcomes returned by the evaluator.

use serde::{Deserialize, Serialize};

use crate::protocol::command::{KubernetesChange, ProtocolCommand, SafetyChange};
use crate::protocol::types::AcceptedStatus;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum WaitReason {
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
pub(crate) enum UnsafeReason {
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
pub(crate) enum Plan {
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

pub(crate) use kuberic_controller::evaluator::EvaluationConfig;

// Cargo builds unit-test and dependency instances of runtime separately. Bridge
// detached records, never private capabilities, to the actual controller policy.
pub(crate) fn evaluate(
    snapshot: &crate::protocol::observation::ObservationSnapshot,
    config: &EvaluationConfig,
) -> Plan {
    let snapshot = serde_json::to_vec(&serde_json::json!({
        "resourceUid": &snapshot.resource_uid,
        "resourceVersion": &snapshot.resource_version,
        "desired": &snapshot.desired,
        "status": &snapshot.status,
        "replicas": snapshot.replicas.iter().collect::<Vec<_>>(),
        "secondaryScaleDownResources": &snapshot.secondary_scale_down_resources,
        "previousReportWatermarks": snapshot
            .previous_report_watermarks
            .iter()
            .collect::<Vec<_>>(),
        "durableStorageEvidence": snapshot.durable_storage_evidence,
        "supportingResourcesReady": snapshot.supporting_resources_ready,
        "routing": &snapshot.routing,
        "observationFailures": &snapshot.observation_failures,
        "nowUnixSeconds": snapshot.now_unix_seconds,
    }))
    .expect("serialize exact observation");
    let plan = kuberic_controller::evaluator::test_bridge::evaluate_json(&snapshot, config)
        .expect("controller observation contract");
    serde_json::from_slice(&plan).expect("runtime command contract")
}
