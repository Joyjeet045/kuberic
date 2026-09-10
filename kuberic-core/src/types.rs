use bytes::Bytes;
use std::fmt;

use crate::add_replica::{
    AddReplicaIntent, AddReplicaProgress, AddReplicaTerminalResult, RuntimeBuildObservation,
};
use crate::remove_replica::{
    RemoveReplicaIntent, RemoveReplicaProgress, RemoveReplicaTerminalResult,
};

pub type CancellationToken = tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Epoch
// ---------------------------------------------------------------------------

/// Configuration version for a partition. Incremented on every reconfiguration.
/// Provides total ordering (data_loss_number first) for epoch-based fencing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Epoch {
    /// Incremented when write quorum is lost (data loss event).
    pub data_loss_number: i64,
    /// Incremented on every reconfiguration.
    pub configuration_number: i64,
}

impl Epoch {
    pub fn new(data_loss_number: i64, configuration_number: i64) -> Self {
        Self {
            data_loss_number,
            configuration_number,
        }
    }
}

impl PartialOrd for Epoch {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Epoch {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.data_loss_number
            .cmp(&other.data_loss_number)
            .then(self.configuration_number.cmp(&other.configuration_number))
    }
}

// ---------------------------------------------------------------------------
// Replica types
// ---------------------------------------------------------------------------

pub type Lsn = i64;
pub type ReplicaId = i64;

/// Identifies one concrete runtime generation of a logical replica.
///
/// A replica ID remains stable when Kubernetes recreates a pod, while the
/// incarnation changes to fence stale connections and control messages.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct ReplicaInstanceId(String);

impl ReplicaInstanceId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ReplicaInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Role {
    /// Pod just started, never assigned a role.
    Unknown = 0,
    Primary = 1,
    ActiveSecondary = 2,
    IdleSecondary = 3,
    /// Explicitly demoted — triggers Close/data removal.
    None = 4,
}

impl TryFrom<u8> for Role {
    type Error = u8;

    fn try_from(v: u8) -> std::result::Result<Self, u8> {
        match v {
            0 => Ok(Role::Unknown),
            1 => Ok(Role::Primary),
            2 => Ok(Role::ActiveSecondary),
            3 => Ok(Role::IdleSecondary),
            4 => Ok(Role::None),
            other => Err(other),
        }
    }
}

/// Durable description of one member in a committed stable partition.
///
/// Transport addresses and clients are deliberately excluded. They are
/// reconstructed from current Kubernetes pods after an operator restart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StableReplicaSnapshot {
    pub id: ReplicaId,
    pub instance_id: ReplicaInstanceId,
    pub role: Role,
    pub election_metadata: Option<StableReplicaElectionMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StableReplicaElectionMetadata {
    pub current_lsn: Lsn,
    pub committed_lsn: Lsn,
    pub first_retained_lsn: Lsn,
    pub deactivation_epoch: Epoch,
    pub deactivation_catch_up_lsn: Lsn,
}

/// Durable description of a complete committed partition topology.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StablePartitionSnapshot {
    pub epoch: Epoch,
    pub primary_id: ReplicaId,
    pub members: Vec<StableReplicaSnapshot>,
    pub write_quorum: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaStatus {
    Up,
    Down,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    /// New replica — no previous state. Initialize fresh.
    New,
    /// Existing replica — has persisted state from a previous run.
    Existing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataLossAction {
    /// State accepted as-is, no changes.
    None,
    /// State was modified (e.g., restored from backup).
    /// Triggers full rebuild of other replicas.
    StateChanged,
}

// ---------------------------------------------------------------------------
// Replica set configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ReplicaInfo {
    pub id: ReplicaId,
    pub instance_id: ReplicaInstanceId,
    pub role: Role,
    pub status: ReplicaStatus,
    /// Data plane address (replication streams + copy protocol).
    pub replicator_address: String,
    pub current_progress: Lsn,
    pub catch_up_capability: Lsn,
    /// Whether this replica must individually catch up for WaitForCatchUpQuorum(Write).
    /// When set, the QuorumTracker requires this replica to have ACKed all ops
    /// up to highest_lsn, not just that quorum was met. Used during failover
    /// (SF pre-promotion phase) or when adding a critical replica.
    pub must_catch_up: bool,
}

#[derive(Debug, Clone)]
pub struct ReplicaSetConfig {
    pub members: Vec<ReplicaInfo>,
    pub write_quorum: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaSetQuorumMode {
    /// Every replica in the config must individually catch up to highest LSN.
    /// SF legacy fallback for replicators without `must_catch_up` support.
    /// Not used by PartitionDriver — kept for completeness.
    All,
    /// Write-quorum subset must catch up, and every `must_catch_up` replica
    /// must individually catch up. Default mode used by PartitionDriver for
    /// all workflows (create, failover, switchover, restart).
    Write,
}

// ---------------------------------------------------------------------------
// Replica status info (operator-facing health probe result)
// ---------------------------------------------------------------------------

/// Identifies one pod-local control process independently of replica
/// incarnation. A new value is generated for every process start.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AgentGeneration(String);

impl AgentGeneration {
    pub fn generate() -> Self {
        Self(format!("{:032x}", rand::random::<u128>()))
    }

    #[cfg(test)]
    pub(crate) fn from_string(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        if value.len() != 32
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("agent generation must be 32 lowercase hexadecimal characters".to_string());
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AgentGeneration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Monotonic command-admission version scoped to one [`AgentGeneration`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct AgentControlVersion(u64);

impl AgentControlVersion {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u64 {
        self.0
    }

    pub fn advance(&mut self) -> Self {
        self.0 = self.0.saturating_add(1);
        *self
    }
}

#[derive(Debug, Clone)]
pub struct CorrelatedControlActionRequest {
    pub protocol_version: u32,
    pub action_id: String,
    pub input_signature: String,
    pub target_replica_id: ReplicaId,
    pub target_instance_id: ReplicaInstanceId,
    pub expected_agent_generation: AgentGeneration,
    pub expected_control_version: AgentControlVersion,
    pub observed_runtime_epoch: Epoch,
    pub action: DurableReplicaAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorrelatedControlActionAcknowledgement {
    pub observation: CorrelatedActionObservation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorrelatedActionObservation {
    pub generation: AgentGeneration,
    pub control_version: AgentControlVersion,
    pub action: DurableActionObservation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalFaultRecord {
    pub sequence: u64,
    pub fault_type: FaultType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaAgentStatus {
    pub protocol_version: u32,
    pub lifecycle_peer_protocol_version: u32,
    pub generation: AgentGeneration,
    pub control_version: AgentControlVersion,
    pub current_action: Option<CorrelatedActionObservation>,
    pub retained_terminal_actions: Vec<CorrelatedActionObservation>,
    pub local_faults: Vec<LocalFaultRecord>,
}

/// Status returned by `ReplicaHandle::get_status()`. Used by the
/// reconciler to detect restarted pods (epoch mismatch, role=None).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaStatusInfo {
    pub instance_id: ReplicaInstanceId,
    pub role: Role,
    pub epoch: Epoch,
    pub current_progress: Lsn,
    pub catch_up_capability: Option<Lsn>,
    pub committed_lsn: Lsn,
    pub healthy: bool,
    pub write_status: AccessStatus,
    pub configuration: Option<ReplicaConfigurationStatus>,
    pub election_configuration: Option<ReplicaElectionConfiguration>,
    pub deactivation_info: Option<ReplicaDeactivationInfo>,
    pub active_replica_connections: Vec<ReplicaConnectionStatus>,
    pub build_observation: Option<RuntimeBuildObservation>,
    pub agent: ReplicaAgentStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaConnectionStatus {
    pub id: ReplicaId,
    pub instance_id: ReplicaInstanceId,
}

// ---------------------------------------------------------------------------
// Access status (ReadStatus / WriteStatus)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AccessStatus {
    /// Operation is allowed.
    Granted = 0,
    /// Reconfiguration in progress. Retry later.
    ReconfigurationPending = 1,
    /// This replica is not primary. Redirect client.
    NotPrimary = 2,
    /// Primary but no write quorum. Reads may still work.
    NoWriteQuorum = 3,
}

impl AccessStatus {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Granted,
            1 => Self::ReconfigurationPending,
            2 => Self::NotPrimary,
            3 => Self::NoWriteQuorum,
            _ => Self::NotPrimary,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaConfigurationMode {
    CatchUp,
    Current,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaConfigurationMemberStatus {
    pub id: ReplicaId,
    pub instance_id: ReplicaInstanceId,
    pub role: Role,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaConfigurationStatus {
    pub mode: ReplicaConfigurationMode,
    pub members: Vec<ReplicaConfigurationMemberStatus>,
    pub write_quorum: u32,
}

impl ReplicaConfigurationStatus {
    pub fn from_config(mode: ReplicaConfigurationMode, config: &ReplicaSetConfig) -> Self {
        let mut members = config
            .members
            .iter()
            .map(|member| ReplicaConfigurationMemberStatus {
                id: member.id,
                instance_id: member.instance_id.clone(),
                role: member.role,
            })
            .collect::<Vec<_>>();
        members.sort_by_key(|member| member.id);
        Self {
            mode,
            members,
            write_quorum: config.write_quorum,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaElectionConfiguration {
    pub previous: Option<ReplicaConfigurationStatus>,
    pub current: ReplicaConfigurationStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaDeactivationInfo {
    pub epoch: Epoch,
    pub catch_up_lsn: Lsn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableActionResult {
    DataLoss(DataLossAction),
    AddReplica(AddReplicaTerminalResult),
    RemoveReplica(RemoveReplicaTerminalResult),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableActionErrorClass {
    Internal,
    NotPrimary,
    NoWriteQuorum,
    ReconfigurationPending,
    StaleEpoch,
    Cancelled,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableActionState {
    Scheduled,
    InProgress,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableActionObservation {
    pub action_id: String,
    pub signature: String,
    pub state: DurableActionState,
    pub error_class: Option<DurableActionErrorClass>,
    pub error: Option<String>,
    pub result: Option<DurableActionResult>,
    pub add_replica_progress: Option<AddReplicaProgress>,
    pub remove_replica_progress: Option<RemoveReplicaProgress>,
}

#[derive(Debug, Clone)]
pub enum DurableReplicaAction {
    AddReplicaIntent {
        intent: Box<AddReplicaIntent>,
    },
    RemoveReplicaIntent {
        intent: Box<RemoveReplicaIntent>,
    },
    Open {
        mode: OpenMode,
    },
    Close,
    RevokeWriteStatus,
    ChangeRole {
        epoch: Epoch,
        role: Role,
    },
    UpdateEpoch {
        epoch: Epoch,
    },
    UpdateCatchUpConfiguration {
        current: ReplicaSetConfig,
        previous: ReplicaSetConfig,
    },
    WaitForCatchUpQuorum {
        mode: ReplicaSetQuorumMode,
    },
    UpdateCurrentConfiguration {
        current: ReplicaSetConfig,
    },
    BuildReplica {
        replica: ReplicaInfo,
    },
    RemoveReplica {
        replica_id: ReplicaId,
        instance_id: ReplicaInstanceId,
    },
    OnDataLoss {
        epoch: Epoch,
    },
    RecordElectionConfiguration {
        configuration: ReplicaElectionConfiguration,
    },
}

impl DurableReplicaAction {
    pub fn signature(&self) -> String {
        match self {
            Self::AddReplicaIntent { intent } => intent.signature(),
            Self::RemoveReplicaIntent { intent } => intent.signature(),
            Self::Open { mode } => format!("open:{mode:?}"),
            Self::Close => "close".to_string(),
            Self::RevokeWriteStatus => "revoke-write-status".to_string(),
            Self::ChangeRole { epoch, role } => {
                format!(
                    "change-role:{:?}:{}:{}",
                    role, epoch.data_loss_number, epoch.configuration_number
                )
            }
            Self::UpdateEpoch { epoch } => format!(
                "update-epoch:{}:{}",
                epoch.data_loss_number, epoch.configuration_number
            ),
            Self::UpdateCatchUpConfiguration { current, previous } => format!(
                "update-catch-up:{}:{}",
                config_signature(current),
                config_signature(previous)
            ),
            Self::WaitForCatchUpQuorum { mode } => {
                format!("wait-for-catch-up:{mode:?}")
            }
            Self::UpdateCurrentConfiguration { current } => {
                format!("update-current:{}", config_signature(current))
            }
            Self::BuildReplica { replica } => format!(
                "build-replica:{}@{}:{:?}:{:?}:{}:{}:{}:{}",
                replica.id,
                replica.instance_id,
                replica.role,
                replica.status,
                replica.replicator_address,
                replica.current_progress,
                replica.catch_up_capability,
                replica.must_catch_up
            ),
            Self::RemoveReplica {
                replica_id,
                instance_id,
            } => format!("remove-replica:{replica_id}@{instance_id}"),
            Self::OnDataLoss { epoch } => format!(
                "on-data-loss:{}:{}",
                epoch.data_loss_number, epoch.configuration_number
            ),
            Self::RecordElectionConfiguration { configuration } => format!(
                "record-election-configuration:current={}:previous={}",
                configuration_signature(&configuration.current),
                configuration
                    .previous
                    .as_ref()
                    .map(configuration_signature)
                    .unwrap_or_else(|| "none".to_string())
            ),
        }
    }
}

fn configuration_signature(configuration: &ReplicaConfigurationStatus) -> String {
    let mut members = configuration
        .members
        .iter()
        .map(|member| format!("{}@{}:{:?}", member.id, member.instance_id, member.role))
        .collect::<Vec<_>>();
    members.sort();
    format!(
        "{:?}:q{}[{}]",
        configuration.mode,
        configuration.write_quorum,
        members.join(",")
    )
}

fn config_signature(config: &ReplicaSetConfig) -> String {
    let mut members = config
        .members
        .iter()
        .map(|member| {
            format!(
                "{}@{}:{:?}:{:?}:{}:{}:{}:{}",
                member.id,
                member.instance_id,
                member.role,
                member.status,
                member.replicator_address,
                member.current_progress,
                member.catch_up_capability,
                member.must_catch_up
            )
        })
        .collect::<Vec<_>>();
    members.sort();
    format!("q{}[{}]", config.write_quorum, members.join(","))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultType {
    /// Restart replica, preserve persisted state.
    Transient,
    /// Drop replica and rebuild from scratch.
    Permanent,
}

// ---------------------------------------------------------------------------
// Replication data
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ReplData {
    pub data: Bytes,
    pub lsn: Lsn,
}

// ---------------------------------------------------------------------------
// Operation + OperationStream (pull-based secondary delivery)
// ---------------------------------------------------------------------------

/// A single replicated operation delivered to the user via an OperationStream.
/// The user must call `acknowledge()` after applying the operation.
///
/// In persisted mode, `acknowledge()` gates quorum — the primary waits for
/// the secondary to acknowledge before completing `replicate()`.
/// In volatile mode, `acknowledge()` only releases memory.
pub struct Operation {
    pub lsn: Lsn,
    pub data: Bytes,
    ack_tx: Option<tokio::sync::oneshot::Sender<()>>,
    copy_completion: bool,
}

impl Operation {
    pub fn new(lsn: Lsn, data: Bytes, ack_tx: Option<tokio::sync::oneshot::Sender<()>>) -> Self {
        Self {
            lsn,
            data,
            ack_tx,
            copy_completion: false,
        }
    }

    pub(crate) fn copy_completion(lsn: Lsn, ack_tx: tokio::sync::oneshot::Sender<()>) -> Self {
        Self {
            lsn,
            data: Bytes::new(),
            ack_tx: Some(ack_tx),
            copy_completion: true,
        }
    }

    /// Acknowledge this operation. Mandatory for persisted replicators —
    /// blocks subsequent ops until called.
    pub fn acknowledge(mut self) {
        if let Some(tx) = self.ack_tx.take() {
            let _ = tx.send(());
        }
    }
}

impl std::fmt::Debug for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Operation")
            .field("lsn", &self.lsn)
            .field("data_len", &self.data.len())
            .field("has_ack", &self.ack_tx.is_some())
            .finish()
    }
}

/// Stream of operations delivered to the user on secondaries.
/// Wraps an mpsc receiver — user calls `get_operation()` in a loop.
pub struct OperationStream {
    rx: tokio::sync::mpsc::Receiver<Operation>,
    completion: Option<Operation>,
}

impl OperationStream {
    pub fn new(rx: tokio::sync::mpsc::Receiver<Operation>) -> Self {
        Self {
            rx,
            completion: None,
        }
    }

    /// Returns the next operation, or None when the stream ends.
    pub async fn get_operation(&mut self) -> Option<Operation> {
        if self.completion.is_some() {
            return None;
        }
        let operation = self.rx.recv().await?;
        if operation.copy_completion {
            self.completion = Some(operation);
            None
        } else {
            Some(operation)
        }
    }

    /// The authoritative copy boundary, available after the copy stream ends.
    pub fn copy_lsn(&self) -> Option<Lsn> {
        self.completion.as_ref().map(|operation| operation.lsn)
    }

    /// Complete copy only after validation, installation, and required checkpoints.
    /// Dropping the stream without this acknowledgement fails the copy RPC.
    pub fn acknowledge_completion(&mut self) -> crate::Result<()> {
        let completion = self.completion.take().ok_or_else(|| {
            crate::KubericError::Internal("copy stream ended without a completion boundary".into())
        })?;
        completion.acknowledge();
        Ok(())
    }

    /// Create a pair (sender, stream) for wiring.
    pub fn channel(buffer: usize) -> (tokio::sync::mpsc::Sender<Operation>, Self) {
        let (tx, rx) = tokio::sync::mpsc::channel(buffer);
        (tx, Self::new(rx))
    }
}

#[cfg(test)]
mod durable_action_tests {
    use super::*;

    #[test]
    fn durable_action_signatures_fence_build_and_remove_incarnations() {
        let replica = ReplicaInfo {
            id: 7,
            instance_id: ReplicaInstanceId::new("new"),
            role: Role::IdleSecondary,
            status: ReplicaStatus::Up,
            replicator_address: "http://replica".to_string(),
            current_progress: -1,
            catch_up_capability: -1,
            must_catch_up: false,
        };
        let build = DurableReplicaAction::BuildReplica { replica };
        let remove_old = DurableReplicaAction::RemoveReplica {
            replica_id: 7,
            instance_id: ReplicaInstanceId::new("old"),
        };
        let remove_new = DurableReplicaAction::RemoveReplica {
            replica_id: 7,
            instance_id: ReplicaInstanceId::new("new"),
        };

        assert_ne!(build.signature(), remove_old.signature());
        assert_ne!(remove_old.signature(), remove_new.signature());
        assert!(build.signature().contains("7@new"));
    }

    #[test]
    fn configuration_signatures_include_execution_relevant_progress() {
        let member = ReplicaInfo {
            id: 2,
            instance_id: ReplicaInstanceId::new("secondary"),
            role: Role::ActiveSecondary,
            status: ReplicaStatus::Up,
            replicator_address: "http://secondary".to_string(),
            current_progress: 10,
            catch_up_capability: 4,
            must_catch_up: true,
        };
        let first = DurableReplicaAction::UpdateCurrentConfiguration {
            current: ReplicaSetConfig {
                members: vec![member.clone()],
                write_quorum: 2,
            },
        };
        let mut advanced = member;
        advanced.current_progress = 11;
        let second = DurableReplicaAction::UpdateCurrentConfiguration {
            current: ReplicaSetConfig {
                members: vec![advanced],
                write_quorum: 2,
            },
        };

        assert_ne!(first.signature(), second.signature());
    }

    #[test]
    fn data_loss_and_election_configuration_signatures_include_all_inputs() {
        let epoch = Epoch::new(3, 9);
        assert_eq!(
            DurableReplicaAction::OnDataLoss { epoch }.signature(),
            "on-data-loss:3:9"
        );

        let current = ReplicaConfigurationStatus {
            mode: ReplicaConfigurationMode::Current,
            members: vec![ReplicaConfigurationMemberStatus {
                id: 1,
                instance_id: ReplicaInstanceId::new("one"),
                role: Role::Primary,
            }],
            write_quorum: 1,
        };
        let without_previous = DurableReplicaAction::RecordElectionConfiguration {
            configuration: ReplicaElectionConfiguration {
                previous: None,
                current: current.clone(),
            },
        };
        let with_previous = DurableReplicaAction::RecordElectionConfiguration {
            configuration: ReplicaElectionConfiguration {
                previous: Some(current.clone()),
                current,
            },
        };

        assert_ne!(without_previous.signature(), with_previous.signature());
        assert!(with_previous.signature().contains("one"));
    }
}
