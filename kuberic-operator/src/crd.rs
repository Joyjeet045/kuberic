use kube::CustomResource;
use kuberic_core::types::{
    Epoch, ReplicaInstanceId, Role, StablePartitionSnapshot, StableReplicaElectionMetadata,
    StableReplicaSnapshot,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::ops::Deref;

/// KubericSet is the primary CRD for managing a stateful replica set.
#[derive(CustomResource, Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[kube(
    group = "kuberic.io",
    version = "v1",
    kind = "KubericSet",
    plural = "kubericsets",
    shortname = "kls",
    derive = "PartialEq",
    namespaced,
    status = "KubericSetStatus",
    printcolumn = r#"{"name":"Replicas","type":"integer","jsonPath":".spec.replicas"}"#,
    printcolumn = r#"{"name":"Ready","type":"integer","jsonPath":".status.readyReplicas"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Primary","type":"string","jsonPath":".status.currentPrimary"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct KubericSetSpec {
    /// Total number of replicas (1 primary + N-1 secondaries).
    #[serde(default = "default_replicas")]
    pub replicas: i32,

    /// Minimum replica set size. Operator won't reduce below this.
    #[serde(default = "default_min_replicas")]
    pub min_replicas: i32,

    /// Container image for the application pods.
    pub image: String,

    /// Seconds to wait before triggering failover after primary failure.
    #[serde(default)]
    #[schemars(range(min = 0))]
    pub failover_delay: i32,

    /// Max seconds for graceful primary demotion during switchover.
    #[serde(default = "default_switchover_delay")]
    pub switchover_delay: i32,

    /// Port for the application container.
    #[serde(default = "default_port")]
    pub port: i32,

    /// Port for the gRPC control server (operator → pod).
    #[serde(default = "default_control_port")]
    pub control_port: i32,

    /// Port for the gRPC data server (pod ↔ pod replication).
    #[serde(default = "default_data_port")]
    pub data_port: i32,

    /// Storage size per pod PVC (e.g. "256Mi").
    #[serde(default = "default_storage")]
    pub storage: String,

    /// PVC retention policy on CR deletion: Delete (default) or Retain.
    #[serde(default)]
    pub pvc_retention_policy: PvcRetentionPolicy,
}

/// Status of the KubericSet.
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct KubericSetStatus {
    /// Current epoch.
    #[serde(default)]
    pub epoch: EpochStatus,

    /// Pod name of the current primary.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_primary: Option<String>,

    /// Pod name that should be primary. "pending" during failover.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_primary: Option<String>,

    /// Lifecycle phase.
    #[serde(default)]
    pub phase: Phase,

    /// Current reconfiguration phase (if any).
    #[serde(default)]
    pub reconfiguration_phase: ReconfigurationPhase,

    /// Active replica count.
    #[serde(default)]
    pub ready_replicas: i32,

    /// Total replica count (including non-ready).
    #[serde(default)]
    pub replicas: i32,

    /// Per-replica status.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub members: Vec<MemberStatus>,

    /// Last durably committed stable topology. This is the only authoritative
    /// recovery input after operator process state is lost.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stable_snapshot: Option<StablePartitionSnapshotStatus>,

    /// Compact durable checkpoint for the current or most recent operation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<DurableOperationStatus>,

    /// Kubernetes-style conditions describing durable operation state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<StatusCondition>,

    /// When the primary started failing (for failover delay).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_failing_since: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub stable_election_metadata_refresh: Option<StableElectionMetadataRefreshStatus>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StableElectionMetadataRefreshStatus {
    pub snapshot_epoch: EpochStatus,
    #[serde(default)]
    pub next_member_index: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub completed_members: Vec<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_action: Option<PendingActionStatus>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct EpochStatus {
    pub data_loss_number: i64,
    pub configuration_number: i64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MemberStatus {
    /// Pod name.
    pub name: String,
    /// Replica ID.
    pub id: i64,
    /// Kubernetes pod UID for this concrete replica incarnation.
    #[serde(default)]
    pub instance_id: String,
    /// Role: primary, active-secondary, idle-secondary, none.
    pub role: String,
    /// Current progress LSN.
    pub current_progress: i64,
    /// Whether the pod is healthy.
    pub healthy: bool,
    /// gRPC control address.
    pub control_address: String,
    /// gRPC data address.
    pub data_address: String,
}

/// Schema-safe persisted form of the core stable partition snapshot.
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StablePartitionSnapshotStatus {
    pub epoch: EpochStatus,
    pub primary_id: i64,
    pub members: Vec<StableReplicaSnapshotStatus>,
    pub write_quorum: u32,
}

/// Schema-safe persisted identity and stable role of one replica.
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StableReplicaSnapshotStatus {
    pub id: i64,
    pub instance_id: String,
    pub role: StableReplicaRoleStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub election_metadata: Option<StableReplicaElectionMetadataStatus>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StableReplicaElectionMetadataStatus {
    pub current_lsn: i64,
    pub committed_lsn: i64,
    pub first_retained_lsn: i64,
    pub deactivation_epoch: EpochStatus,
    pub deactivation_catch_up_lsn: i64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema, Default)]
#[serde(transparent)]
pub struct OptionalStablePartitionSnapshotStatus(pub Option<StablePartitionSnapshotStatus>);

impl OptionalStablePartitionSnapshotStatus {
    pub fn is_none(&self) -> bool {
        self.0.is_none()
    }

    pub fn is_some(&self) -> bool {
        self.0.is_some()
    }

    pub fn as_ref(&self) -> Option<&StablePartitionSnapshotStatus> {
        self.0.as_ref()
    }

    pub fn cloned(&self) -> Option<StablePartitionSnapshotStatus> {
        self.0.clone()
    }
}

impl From<Option<StablePartitionSnapshotStatus>> for OptionalStablePartitionSnapshotStatus {
    fn from(value: Option<StablePartitionSnapshotStatus>) -> Self {
        Self(value)
    }
}

impl From<StablePartitionSnapshotStatus> for OptionalStablePartitionSnapshotStatus {
    fn from(value: StablePartitionSnapshotStatus) -> Self {
        Self(Some(value))
    }
}

impl Deref for OptionalStablePartitionSnapshotStatus {
    type Target = StablePartitionSnapshotStatus;

    fn deref(&self) -> &Self::Target {
        self.0
            .as_ref()
            .expect("operation protocol requires a previous stable snapshot")
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum StableReplicaRoleStatus {
    Primary,
    ActiveSecondary,
}

pub const DURABLE_OPERATION_VERSION: u32 = 1;
pub const ADD_REPLICA_OPERATION_VERSION: u32 = 3;
pub const REMOVE_REPLICA_OPERATION_VERSION: u32 = 2;

fn is_zero_u32(value: &u32) -> bool {
    *value == 0
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DurableOperationStatus {
    pub operation_id: String,
    pub execution_id: String,
    pub version: u32,
    pub kind: DurableOperationKind,
    pub phase: DurableOperationPhase,
    #[serde(
        default,
        skip_serializing_if = "OptionalStablePartitionSnapshotStatus::is_none"
    )]
    #[schemars(with = "Option<StablePartitionSnapshotStatus>")]
    pub previous_snapshot: OptionalStablePartitionSnapshotStatus,
    pub target_snapshot: StablePartitionSnapshotStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committed_snapshot: Option<StablePartitionSnapshotStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub minimum_committed_replicas: Option<u32>,
    pub old_primary_id: i64,
    pub target_primary_id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub add_mode: Option<DurableAddMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remove_mode: Option<DurableRemoveMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_replica_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_instance_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_pod_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_pod_uid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remove_target_replicator_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remove_target_agent_generation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retired_instance_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frozen_lsn: Option<i64>,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub next_secondary_index: u32,
    pub phase_deadline_unix_seconds: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_action: Option<PendingActionStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failover: Option<DurableFailoverStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub add_intent: Option<Box<AddReplicaIntentStatus>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remove_intent: Option<Box<RemoveReplicaIntentStatus>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remove_commit_evidence: Option<RemoveReplicaCommitEvidenceStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remove_cleanup: Option<RemoveReplicaCleanupStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub removal_disposition: Option<RemoveReplicaDispositionStatus>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AddReplicaIntentStatus {
    pub attempt: u32,
    pub attempt_id: String,
    pub action_id: String,
    pub primary_instance_id: String,
    pub primary_agent_generation: String,
    pub primary_control_address: String,
    pub target_agent_generation: String,
    pub target_control_address: String,
    pub target_replicator_address: String,
    pub target_lifecycle_peer_protocol_version: u32,
    pub previous_configuration: ConfigurationDescriptorStatus,
    pub catch_up_configuration: ConfigurationDescriptorStatus,
    pub current_configuration: ConfigurationDescriptorStatus,
    pub deadline_unix_seconds: i64,
    pub compensation_deadline_unix_seconds: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_observed_phase: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RemoveReplicaIntentStatus {
    pub attempt: u32,
    pub attempt_id: String,
    pub action_id: String,
    pub input_signature: String,
    pub primary_instance_id: String,
    pub primary_agent_generation: String,
    pub primary_agent_control_version: u64,
    pub primary_control_address: String,
    pub primary_replicator_address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_agent_generation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_control_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_replicator_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_lifecycle_peer_protocol_version: Option<u32>,
    pub previous_configuration: ConfigurationDescriptorStatus,
    pub reduced_catch_up_configuration: ConfigurationDescriptorStatus,
    pub reduced_current_configuration: ConfigurationDescriptorStatus,
    pub required_write_quorum: u32,
    pub maximum_pre_commit_attempts: u32,
    pub overall_deadline_unix_seconds: i64,
    pub compensation_grace_seconds: i64,
    pub compensation_deadline_cap_unix_seconds: i64,
    pub call_timeout_seconds: i64,
    pub target_retirement_timeout_seconds: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compensation_expiry_unix_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_observed_phase: Option<RemoveReplicaCoordinatorPhaseStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_observed_result: Option<RemoveReplicaTerminalResultStatus>,
    #[serde(default)]
    pub current_install_dispatched: bool,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RemoveReplicaCommitEvidenceStatus {
    pub attempt_id: String,
    pub action_id: String,
    pub primary_agent_generation: String,
    pub configuration_signature: String,
    pub observed_unix_seconds: i64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct RemoveReplicaCleanupStatus {
    #[serde(default)]
    pub connection_absent: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_retirement: Option<TargetRetirementObservationStatus>,
    #[serde(default)]
    pub target_labels_fenced: bool,
    #[serde(default)]
    pub target_pod_deleted: bool,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum RemoveReplicaCoordinatorPhaseStatus {
    Validating,
    InstallingCatchUpConfiguration,
    WaitingForCatchUpQuorum,
    InstallingCurrentConfiguration,
    RemovingConnection,
    RetiringTarget,
    Attesting,
    Compensating,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum TargetRetirementObservationStatus {
    NotAttempted,
    InProgress,
    Completed,
    Unavailable,
    Stale,
    Failed,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum RemoveReplicaTerminalResultStatus {
    CommittedClean,
    CommittedDegraded,
    Compensated,
    CompensationIncomplete,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum RemoveReplicaDispositionStatus {
    FailedPreCommitIncomplete {
        attempt: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        last_observed_phase: Option<RemoveReplicaCoordinatorPhaseStatus>,
        reason: String,
    },
    InvalidRemovalState {
        attempt: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        last_observed_phase: Option<RemoveReplicaCoordinatorPhaseStatus>,
        previous_configuration_signature: String,
        reduced_configuration_signature: String,
        reason: String,
    },
    AmbiguousPrimaryRestart {
        old_generation: String,
        new_generation: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        last_observed_phase: Option<RemoveReplicaCoordinatorPhaseStatus>,
        previous_configuration_signature: String,
        reduced_configuration_signature: String,
        missing_evidence_reason: String,
    },
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConfigurationDescriptorStatus {
    pub members: Vec<ConfigurationMemberDescriptorStatus>,
    pub write_quorum: u32,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConfigurationMemberDescriptorStatus {
    pub id: i64,
    pub instance_id: String,
    pub role: String,
    pub status: String,
    pub replicator_address: String,
    pub must_catch_up: bool,
    pub progress_source: ConfigurationProgressSourceStatus,
    pub current_progress: i64,
    pub catch_up_capability: i64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ConfigurationProgressSourceStatus {
    Frozen,
    BuildCopyLsn,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DurableFailoverStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_configuration: Option<FailoverConfigurationStatus>,
    pub current_configuration: FailoverConfigurationStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub observations: Vec<ReplicaElectionObservationStatus>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unavailable_replicas: Vec<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub final_attestations: Vec<ReplicaElectionObservationStatus>,
    #[serde(default)]
    pub target_confirmed: bool,
    #[serde(default)]
    pub data_loss_required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configuration_epoch_intent: Option<EpochStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_loss_epoch_intent: Option<EpochStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_loss_result: Option<DurableDataLossResultStatus>,
    #[serde(default)]
    pub next_configuration_index: u32,
    #[serde(default)]
    pub next_secondary_index: u32,
    #[serde(default)]
    pub next_label_index: u32,
    #[serde(default)]
    pub next_attestation_index: u32,
    #[serde(default)]
    pub next_unavailable_index: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assessment: Option<String>,
    #[serde(default)]
    pub promotion_committed: bool,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FailoverConfigurationStatus {
    pub members: Vec<FailoverMemberStatus>,
    pub write_quorum: u32,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FailoverMemberStatus {
    pub id: i64,
    pub instance_id: String,
    pub role: StableReplicaRoleStatus,
    #[serde(default)]
    pub dropped: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_known: Option<StableReplicaElectionMetadataStatus>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaElectionObservationStatus {
    pub id: i64,
    pub instance_id: String,
    pub epoch: EpochStatus,
    pub role: String,
    pub healthy: bool,
    pub current_lsn: i64,
    pub committed_lsn: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_retained_lsn: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deactivation_epoch: Option<EpochStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deactivation_catch_up_lsn: Option<i64>,
    #[serde(default)]
    pub configuration_matches: bool,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum DurableDataLossResultStatus {
    NoStateChange,
    StateChanged,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum DurableOperationKind {
    CreatePartition,
    Switchover,
    AddReplica,
    RemoveReplica,
    Failover,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum DurableAddMode {
    ScaleUp,
    Rebuild,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum DurableRemoveMode {
    ScaleDown,
    Force,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum DurableOperationPhase {
    AddFreezeIntent,
    AddDispatchIntent,
    AddAwaitCoordination,
    AddRecordCommit,
    AddPublishTarget,
    AddCommittedDegradedHandoff,
    AddDeleteCompensatedTarget,
    CreateFenceRouting,
    CreateOpenPrimary,
    CreatePromotePrimary,
    CreatePrimaryCurrentConfiguration,
    CreateCommitPrimary,
    CreateOpenSecondary,
    CreateUpdateSecondaryEpoch,
    CreateAssignSecondaryIdle,
    CreateBuildSecondary,
    CreateAssignSecondaryActive,
    CreateCatchUpConfiguration,
    CreateWaitForCatchUpQuorum,
    CreateCurrentConfiguration,
    CreateCommitSecondary,
    CreatePublishRouting,
    CreateFinalize,
    CreateCompensateRestoreConfiguration,
    CreateCompensateRemoveCandidate,
    CreateCompensateDemoteCandidate,
    CreateCompensateCloseCandidate,
    CreateCompensateDeleteCandidate,
    CreateCompensateFinalize,
    Revoke,
    CaptureLsn,
    PreCatchUp,
    DemoteOldPrimary,
    PromoteTarget,
    DistributeEpoch,
    UpdateCatchUpConfiguration,
    WaitForCatchUpQuorum,
    UpdateCurrentConfiguration,
    LabelTargetPrimary,
    LabelOldSecondary,
    Finalize,
    RestorePreviousConfiguration,
    CompensatePromoteOldPrimary,
    CompensateDistributeEpoch,
    CompensateCatchUpConfiguration,
    CompensateCurrentConfiguration,
    CompensateLabelOldPrimary,
    CompensateLabelTargetSecondary,
    CompensateFinalize,
    AddFinalize,
    AddCompensateFinalize,
    RemoveFreezeIntent,
    RemoveDispatchIntent,
    RemoveAwaitCoordination,
    RemoveRecordCommit,
    RemoveAwaitCleanup,
    RemoveDeleteTargetPod,
    RemovePublishTopology,
    RemoveFinalize,
    RemoveCompensateFinalize,
    FailoverRecordStartingConfiguration,
    FailoverCollect,
    FailoverAssess,
    FailoverWaitForBestCandidate,
    FailoverWaitForReadQuorum,
    FailoverPersistConfigurationEpoch,
    FailoverPersistDataLossEpoch,
    FailoverApplyCandidateEpoch,
    FailoverNotifyDataLoss,
    FailoverRefreshCandidate,
    FailoverPromoteCandidate,
    FailoverCommitPromotion,
    FailoverDistributeEpoch,
    FailoverCatchUpConfiguration,
    FailoverWaitForCatchUpQuorum,
    FailoverCurrentConfiguration,
    FailoverRecordElectionConfiguration,
    FailoverLabelMembers,
    FailoverAttest,
    FailoverFinalize,
    Completed,
    Failed,
    Poisoned,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PendingActionStatus {
    pub action_id: String,
    pub sequence: u32,
    pub kind: DurableActionKind,
    pub target_id: i64,
    pub target_instance_id: String,
    pub expected_epoch: EpochStatus,
    pub desired_postcondition: DurablePostconditionStatus,
    #[serde(default)]
    pub attempts: u32,
    pub deadline_unix_seconds: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default)]
    pub dispatch_authorized: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch_agent_generation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch_agent_control_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch_observed_runtime_epoch: Option<EpochStatus>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub dispatch_action_payload: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum DurableActionKind {
    AddReplicaIntent,
    RemoveReplicaIntent,
    CreateFencePod,
    CreateOpenPrimary,
    CreatePromotePrimary,
    CreatePrimaryCurrentConfiguration,
    CreateOpenSecondary,
    CreateUpdateSecondaryEpoch,
    CreateAssignSecondaryIdle,
    CreateBuildSecondary,
    CreateAssignSecondaryActive,
    CreateCatchUpConfiguration,
    CreateWaitForCatchUpQuorum,
    CreateCurrentConfiguration,
    CreatePublishPrimary,
    CreatePublishSecondary,
    CreateCompensateRestoreConfiguration,
    CreateCompensateRemoveCandidate,
    CreateCompensateDemoteCandidate,
    CreateCompensateCloseCandidate,
    CreateCompensateDeleteCandidate,
    RevokeWrite,
    DemoteOldPrimary,
    PromoteTarget,
    UpdateSecondaryEpoch,
    UpdateCatchUpConfiguration,
    WaitForCatchUpQuorum,
    UpdateCurrentConfiguration,
    LabelTargetPrimary,
    LabelOldSecondary,
    RestorePreviousConfiguration,
    CompensatePromoteOldPrimary,
    CompensateUpdateSecondaryEpoch,
    CompensateCatchUpConfiguration,
    CompensateCurrentConfiguration,
    CompensateLabelOldPrimary,
    CompensateLabelTargetSecondary,
    FailoverRecordStartingConfiguration,
    FailoverUpdateCandidateEpoch,
    FailoverOnDataLoss,
    FailoverPromoteCandidate,
    FailoverUpdateSecondaryEpoch,
    FailoverCatchUpConfiguration,
    FailoverWaitForCatchUpQuorum,
    FailoverCurrentConfiguration,
    FailoverRecordElectionConfiguration,
    FailoverLabelMember,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DurablePostconditionStatus {
    pub kind: DurablePostconditionKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<StableReplicaRoleStatus>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum DurablePostconditionKind {
    AddReplicaCoordinated,
    RemoveReplicaCoordinated,
    WriteRevoked,
    Role,
    Epoch,
    CatchUpConfiguration,
    CatchUpQuorum,
    CurrentConfiguration,
    PodRoleLabel,
    Opened,
    BuildCompleted,
    ReplicaRemoved,
    Closed,
    PodDeleted,
    DataLossCompleted,
    ElectionConfiguration,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StatusCondition {
    #[serde(rename = "type")]
    pub type_: String,
    pub status: String,
    pub reason: String,
    pub message: String,
    pub last_transition_time: String,
}

impl TryFrom<&StablePartitionSnapshotStatus> for StablePartitionSnapshot {
    type Error = String;

    fn try_from(value: &StablePartitionSnapshotStatus) -> Result<Self, Self::Error> {
        let members = value
            .members
            .iter()
            .map(StableReplicaSnapshot::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            epoch: Epoch::new(
                value.epoch.data_loss_number,
                value.epoch.configuration_number,
            ),
            primary_id: value.primary_id,
            members,
            write_quorum: value.write_quorum,
        })
    }
}

impl TryFrom<&StableReplicaSnapshotStatus> for StableReplicaSnapshot {
    type Error = String;

    fn try_from(value: &StableReplicaSnapshotStatus) -> Result<Self, Self::Error> {
        if value.instance_id.is_empty() {
            return Err(format!(
                "stable snapshot replica {} has an empty incarnation",
                value.id
            ));
        }
        Ok(Self {
            id: value.id,
            instance_id: ReplicaInstanceId::new(value.instance_id.clone()),
            role: match value.role {
                StableReplicaRoleStatus::Primary => Role::Primary,
                StableReplicaRoleStatus::ActiveSecondary => Role::ActiveSecondary,
            },
            election_metadata: value.election_metadata.as_ref().map(|metadata| {
                StableReplicaElectionMetadata {
                    current_lsn: metadata.current_lsn,
                    committed_lsn: metadata.committed_lsn,
                    first_retained_lsn: metadata.first_retained_lsn,
                    deactivation_epoch: Epoch::new(
                        metadata.deactivation_epoch.data_loss_number,
                        metadata.deactivation_epoch.configuration_number,
                    ),
                    deactivation_catch_up_lsn: metadata.deactivation_catch_up_lsn,
                }
            }),
        })
    }
}

impl TryFrom<&StablePartitionSnapshot> for StablePartitionSnapshotStatus {
    type Error = String;

    fn try_from(value: &StablePartitionSnapshot) -> Result<Self, Self::Error> {
        Ok(Self {
            epoch: EpochStatus {
                data_loss_number: value.epoch.data_loss_number,
                configuration_number: value.epoch.configuration_number,
            },
            primary_id: value.primary_id,
            members: value
                .members
                .iter()
                .map(StableReplicaSnapshotStatus::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            write_quorum: value.write_quorum,
        })
    }
}

impl TryFrom<&StableReplicaSnapshot> for StableReplicaSnapshotStatus {
    type Error = String;

    fn try_from(value: &StableReplicaSnapshot) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            instance_id: value.instance_id.to_string(),
            role: match value.role {
                Role::Primary => StableReplicaRoleStatus::Primary,
                Role::ActiveSecondary => StableReplicaRoleStatus::ActiveSecondary,
                role => {
                    return Err(format!(
                        "replica {} has unsupported stable role {role:?}",
                        value.id
                    ));
                }
            },
            election_metadata: value.election_metadata.as_ref().map(|metadata| {
                StableReplicaElectionMetadataStatus {
                    current_lsn: metadata.current_lsn,
                    committed_lsn: metadata.committed_lsn,
                    first_retained_lsn: metadata.first_retained_lsn,
                    deactivation_epoch: EpochStatus {
                        data_loss_number: metadata.deactivation_epoch.data_loss_number,
                        configuration_number: metadata.deactivation_epoch.configuration_number,
                    },
                    deactivation_catch_up_lsn: metadata.deactivation_catch_up_lsn,
                }
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::CustomResourceExt;

    #[test]
    fn stable_snapshot_round_trips_without_changing_incarnations() {
        let core = StablePartitionSnapshot {
            epoch: Epoch::new(2, 7),
            primary_id: 4,
            members: vec![
                StableReplicaSnapshot {
                    id: 4,
                    instance_id: ReplicaInstanceId::new("pod-uid/exact"),
                    role: Role::Primary,
                    election_metadata: Some(StableReplicaElectionMetadata {
                        current_lsn: 12,
                        committed_lsn: 11,
                        first_retained_lsn: 3,
                        deactivation_epoch: Epoch::new(2, 6),
                        deactivation_catch_up_lsn: 10,
                    }),
                },
                StableReplicaSnapshot {
                    id: 8,
                    instance_id: ReplicaInstanceId::new("another uid"),
                    role: Role::ActiveSecondary,
                    election_metadata: None,
                },
            ],
            write_quorum: 2,
        };

        let persisted = StablePartitionSnapshotStatus::try_from(&core).unwrap();
        let recovered = StablePartitionSnapshot::try_from(&persisted).unwrap();
        assert_eq!(recovered, core);

        let json = serde_json::to_value(&persisted).unwrap();
        assert_eq!(json["primaryId"], 4);
        assert_eq!(json["members"][0]["instanceId"], "pod-uid/exact");
        assert_eq!(json["members"][0]["role"], "primary");
        assert_eq!(json["members"][0]["electionMetadata"]["currentLsn"], 12);
        assert_eq!(json["members"][1]["role"], "activeSecondary");
        assert!(json["members"][1].get("electionMetadata").is_none());
    }

    #[test]
    fn status_without_stable_snapshot_remains_backward_compatible() {
        let status: KubericSetStatus =
            serde_json::from_value(serde_json::json!({"phase": "Healthy"})).unwrap();
        assert!(status.stable_snapshot.is_none());
        assert!(status.operation.is_none());
        assert!(status.conditions.is_empty());
        assert!(
            serde_json::to_value(status)
                .unwrap()
                .get("stableSnapshot")
                .is_none()
        );
    }

    #[test]
    fn persisted_snapshot_rejects_empty_incarnation() {
        let persisted = StableReplicaSnapshotStatus {
            id: 1,
            instance_id: String::new(),
            role: StableReplicaRoleStatus::Primary,
            election_metadata: None,
        };
        assert_eq!(
            StableReplicaSnapshot::try_from(&persisted).unwrap_err(),
            "stable snapshot replica 1 has an empty incarnation"
        );
    }

    #[test]
    fn core_snapshot_rejects_unsupported_stable_role() {
        let core = StableReplicaSnapshot {
            id: 1,
            instance_id: ReplicaInstanceId::new("one"),
            role: Role::IdleSecondary,
            election_metadata: None,
        };
        assert_eq!(
            StableReplicaSnapshotStatus::try_from(&core).unwrap_err(),
            "replica 1 has unsupported stable role IdleSecondary"
        );
    }

    #[test]
    fn legacy_and_zero_election_metadata_remain_distinct() {
        let legacy: StableReplicaSnapshotStatus = serde_json::from_value(serde_json::json!({
            "id": 1,
            "instanceId": "one",
            "role": "primary"
        }))
        .unwrap();
        assert!(legacy.election_metadata.is_none());

        let core = StableReplicaSnapshot {
            id: 1,
            instance_id: ReplicaInstanceId::new("one"),
            role: Role::Primary,
            election_metadata: Some(StableReplicaElectionMetadata {
                current_lsn: 0,
                committed_lsn: 0,
                first_retained_lsn: 0,
                deactivation_epoch: Epoch::new(0, 0),
                deactivation_catch_up_lsn: 0,
            }),
        };
        let persisted = StableReplicaSnapshotStatus::try_from(&core).unwrap();
        let recovered = StableReplicaSnapshot::try_from(&persisted).unwrap();
        assert_eq!(recovered, core);
        let metadata = serde_json::to_value(persisted).unwrap()["electionMetadata"].clone();
        assert_eq!(metadata["currentLsn"], 0);
        assert_eq!(metadata["committedLsn"], 0);
        assert_eq!(metadata["firstRetainedLsn"], 0);
        assert_eq!(metadata["deactivationCatchUpLsn"], 0);
    }

    #[test]
    fn failover_schema_contains_durable_state_and_nonnegative_delay() {
        let generated = serde_json::to_string(&KubericSet::crd()).unwrap();
        for required in [
            "failoverRecordStartingConfiguration",
            "failoverWaitForBestCandidate",
            "failoverPersistDataLossEpoch",
            "failoverOnDataLoss",
            "dataLossCompleted",
            "dispatchAuthorized",
            "dispatchAgentGeneration",
            "dispatchAgentControlVersion",
            "dispatchObservedRuntimeEpoch",
            "dispatchActionPayload",
            "unavailableReplicas",
            "stableElectionMetadataRefresh",
        ] {
            assert!(
                generated.contains(required),
                "missing generated schema {required}"
            );
        }
        assert!(!generated.contains("dispatchProtocol"));
        let deployment = include_str!("../deploy/deployment.yaml");
        for required in [
            "failoverRecordStartingConfiguration",
            "failoverWaitForBestCandidate",
            "failoverPersistDataLossEpoch",
            "failoverOnDataLoss",
            "dataLossCompleted",
            "dispatchAuthorized",
            "dispatchAgentGeneration",
            "dispatchAgentControlVersion",
            "dispatchObservedRuntimeEpoch",
            "dispatchActionPayload",
            "unavailableReplicas",
            "stableElectionMetadataRefresh",
            "minimum: 0",
        ] {
            assert!(
                deployment.contains(required),
                "missing deployment schema {required}"
            );
        }
        assert!(!deployment.contains("dispatchProtocol"));
    }

    #[test]
    fn pending_action_serializes_only_correlated_dispatch_fences() {
        let mut pending: PendingActionStatus = serde_json::from_value(serde_json::json!({
            "actionId": "operation:1",
            "sequence": 1,
            "kind": "revokeWrite",
            "targetId": 1,
            "targetInstanceId": "pod-uid",
            "expectedEpoch": {
                "dataLossNumber": 0,
                "configurationNumber": 1
            },
            "desiredPostcondition": {
                "kind": "writeRevoked"
            },
            "attempts": 1,
            "deadlineUnixSeconds": 100,
            "dispatchAuthorized": true
        }))
        .unwrap();
        assert!(pending.dispatch_agent_generation.is_none());
        assert!(pending.dispatch_agent_control_version.is_none());
        assert!(pending.dispatch_observed_runtime_epoch.is_none());

        pending.dispatch_agent_generation = Some("0123456789abcdef0123456789abcdef".to_string());
        pending.dispatch_agent_control_version = Some(7);
        pending.dispatch_observed_runtime_epoch = Some(EpochStatus {
            data_loss_number: 0,
            configuration_number: 2,
        });
        pending.dispatch_action_payload = "010203".to_string();
        let serialized = serde_json::to_value(pending).unwrap();
        assert!(serialized.get("dispatchProtocol").is_none());
        assert_eq!(serialized["dispatchAgentControlVersion"], 7);
        assert_eq!(
            serialized["dispatchObservedRuntimeEpoch"]["configurationNumber"],
            2
        );
        assert_eq!(
            serialized["dispatchActionPayload"],
            serde_json::json!("010203")
        );
    }

    #[test]
    fn add_replica_schema_contains_coarse_intent_and_removes_fine_grained_cursor() {
        let generated = serde_json::to_string(&KubericSet::crd()).unwrap();
        let deployment = include_str!("../deploy/deployment.yaml");
        for required in [
            "addIntent",
            "attemptId",
            "actionId",
            "primaryAgentGeneration",
            "primaryControlAddress",
            "targetAgentGeneration",
            "targetControlAddress",
            "targetReplicatorAddress",
            "previousConfiguration",
            "catchUpConfiguration",
            "currentConfiguration",
            "compensationDeadlineUnixSeconds",
            "addFreezeIntent",
            "addDispatchIntent",
            "addAwaitCoordination",
            "addRecordCommit",
            "addPublishTarget",
            "addCommittedDegradedHandoff",
            "addDeleteCompensatedTarget",
            "addReplicaIntent",
            "addReplicaCoordinated",
        ] {
            assert!(generated.contains(required), "missing generated {required}");
            assert!(
                deployment.contains(required),
                "missing deployment {required}"
            );
        }
        for removed in [
            "retireOldReplica",
            "openCandidate",
            "buildCandidate",
            "assignCandidateActive",
            "addCatchUpConfiguration",
            "addWaitForCatchUpQuorum",
            "addCurrentConfiguration",
            "compensateRemoveCandidate",
            "compensateCloseCandidate",
        ] {
            assert!(
                !generated.contains(removed),
                "generated schema retained {removed}"
            );
            assert!(
                !deployment.contains(removed),
                "deployment schema retained {removed}"
            );
        }
    }

    #[test]
    fn remove_replica_schema_is_coarse_v2_and_rejects_deleted_surfaces() {
        let generated_crd = serde_json::to_value(KubericSet::crd()).unwrap();
        let generated = serde_json::to_string(&generated_crd).unwrap();
        let deployment = include_str!("../deploy/deployment.yaml");
        assert_eq!(
            generated_crd.pointer("/spec/names/kind"),
            Some(&serde_json::json!("KubericSet"))
        );
        let columns = generated_crd
            .pointer("/spec/versions/0/additionalPrinterColumns")
            .and_then(serde_json::Value::as_array)
            .expect("generated CRD must retain kubectl printer columns");
        assert_eq!(
            columns
                .iter()
                .map(|column| {
                    (
                        column["name"].as_str().unwrap(),
                        column["type"].as_str().unwrap(),
                        column["jsonPath"].as_str().unwrap(),
                    )
                })
                .collect::<Vec<_>>(),
            [
                ("Replicas", "integer", ".spec.replicas"),
                ("Ready", "integer", ".status.readyReplicas"),
                ("Phase", "string", ".status.phase"),
                ("Primary", "string", ".status.currentPrimary"),
                ("Age", "date", ".metadata.creationTimestamp"),
            ]
        );
        for required in [
            "listKind: KubericSetList",
            "additionalPrinterColumns:",
            "jsonPath: .spec.replicas",
            "jsonPath: .status.readyReplicas",
            "jsonPath: .status.phase",
            "jsonPath: .status.currentPrimary",
            "jsonPath: .metadata.creationTimestamp",
        ] {
            assert!(
                deployment.contains(required),
                "deployment CRD metadata is missing {required}"
            );
        }
        for required in [
            "removeIntent",
            "removeCommitEvidence",
            "removeCleanup",
            "removalDisposition",
            "committedSnapshot",
            "removeTargetAgentGeneration",
            "maximumPreCommitAttempts",
            "currentInstallDispatched",
            "removeFreezeIntent",
            "removeDispatchIntent",
            "removeAwaitCoordination",
            "removeRecordCommit",
            "removeAwaitCleanup",
            "removeDeleteTargetPod",
            "removePublishTopology",
            "removeFinalize",
            "removeCompensateFinalize",
            "removeReplicaIntent",
            "removeReplicaCoordinated",
            "failedPreCommitIncomplete",
            "invalidRemovalState",
            "ambiguousPrimaryRestart",
        ] {
            assert!(generated.contains(required), "missing generated {required}");
            assert!(
                deployment.contains(required),
                "missing deployment {required}"
            );
        }
        for removed in [
            "removeCatchUpConfiguration",
            "removeWaitForCatchUpQuorum",
            "removeCurrentConfiguration",
            "removePrimaryConnection",
            "removeDemoteTarget",
            "removeCloseTarget",
            "removeCompensateConfiguration",
            "replicaAddBuildPeerProtocolVersion",
        ] {
            assert!(
                !generated.contains(removed),
                "generated schema retained {removed}"
            );
            assert!(
                !deployment.contains(removed),
                "deployment schema retained {removed}"
            );
        }
        assert!(generated.contains("\"removeDeleteTargetPod\""));
        assert!(!generated.contains("\"removeDeleteTarget\""));
        assert!(
            deployment
                .lines()
                .any(|line| line.trim() == "- removeDeleteTargetPod")
        );
        assert!(
            !deployment
                .lines()
                .any(|line| line.trim() == "- removeDeleteTarget")
        );
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema, Default)]
pub enum Phase {
    #[default]
    Pending,
    Creating,
    Healthy,
    FailingOver,
    Switchover,
    AddingReplica,
    RemovingReplica,
    Deleting,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema, Default)]
pub enum ReconfigurationPhase {
    #[default]
    None,
    Phase0Demote,
    Phase1GetLSN,
    Phase2Catchup,
    Phase3Deactivate,
    Phase4Activate,
}

fn default_replicas() -> i32 {
    3
}

fn default_min_replicas() -> i32 {
    2
}

fn default_switchover_delay() -> i32 {
    3600
}

fn default_port() -> i32 {
    8080
}

fn default_control_port() -> i32 {
    9090
}

fn default_data_port() -> i32 {
    9091
}

fn default_storage() -> String {
    "256Mi".to_string()
}

/// PVC retention policy when the KubericSet CR is deleted.
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema, Default)]
pub enum PvcRetentionPolicy {
    /// Delete PVCs when the KubericSet is deleted.
    #[default]
    Delete,
    /// Retain PVCs for manual recovery.
    Retain,
}
