use crate::protocol::types::{
    AccessStatus, ConfigurationId, Epoch, OperationId, ReplicaIdentity, ReplicaRole,
    SwitchoverRequestId,
};
use serde::{Deserialize, Serialize};

use crate::authority::RetiredAuthority;
use crate::authority::{AdmittedAuthority, BuildAuthority};
use crate::protocol::types::{
    ProcessSessionId, SecondaryRemovalPreparation, SecondaryRemovalWitness,
    SecondaryScaleDownCleanup, SecondaryScaleDownIntent,
};
use crate::receipts::{
    CertifiedPrefixReceipt, RetirementReceipt, SecondaryRemovalReceipt, SwitchoverReceipt,
};

use crate::application::OpenMode;

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::deserialize(deserializer)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RoleTransition {
    pub(crate) completed_role: ReplicaRole,
    pub(crate) target_role: ReplicaRole,
    pub(crate) replicator_completed: bool,
    pub(crate) epoch_completed: bool,
    pub(crate) application_completed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RuntimeEffect {
    pub(crate) operation_id: OperationId,
    pub(crate) sequence: u64,
    pub(crate) action: RuntimeEffectAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum RuntimeEffectAction {
    Open(OpenMode),
    AdmitAuthority(Box<AdmittedAuthority>),
    PrepareSecondaryRemoval {
        intent: Box<SecondaryScaleDownIntent>,
        process_session_id: ProcessSessionId,
        report_sequence: u64,
    },
    RegisterPeerSession {
        identity: ReplicaIdentity,
        session: ProcessSessionId,
    },
    ObserveSecondaryRemovalWitness(Box<SecondaryRemovalWitness>),
    ObserveSecondaryRemovalProgress {
        witness: Box<SecondaryRemovalWitness>,
        committed: Box<SecondaryScaleDownCleanup>,
    },
    ObserveReplicationAck {
        acknowledgement: Box<crate::transport::ReplicationAck>,
        session: ProcessSessionId,
    },
    AcceptSecondaryRemovalCommit(Box<SecondaryScaleDownCleanup>),
    AcceptHistoricalSecondaryRemovalCommit(
        Box<crate::protocol::command::AcceptSecondaryRemovalCommit>,
    ),
    RetireReplica(Box<RetiredAuthority>),
    FenceRetirement(Box<RetiredAuthority>),
    CompleteRetirement(Box<RetiredAuthority>),
    AuthorizeFailoverPrefix(i64),
    AdmitBuildAuthority(Box<BuildAuthority>),
    ChangeRole(ReplicaRole),
    ChangeReplicatorRole(ReplicaRole),
    UpdateEpoch,
    ChangeApplicationRole(ReplicaRole),
    WaitForCatchup,
    SetAccessStatus {
        read: AccessStatus,
        write: AccessStatus,
    },
    SetReadStatus(AccessStatus),
    SetWriteStatus(AccessStatus),
    PrepareSwitchover {
        preparation_generation: u64,
        request_id: SwitchoverRequestId,
        source: ReplicaIdentity,
        target: ReplicaIdentity,
        starting_configuration_id: ConfigurationId,
        starting_epoch: crate::protocol::types::Epoch,
    },
    RefreshApplicationProgress,
    BuildReplica {
        build_id: OperationId,
        target: ReplicaIdentity,
        replication_address: String,
    },
    RetireBuild(OperationId),
    Close,
    Abort,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BuildPostcondition {
    pub(crate) authority: BuildAuthority,
    pub(crate) last_sequence: u64,
    pub(crate) durable_lsn: i64,
    pub(crate) completed: bool,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub(crate) catch_up_boundary_lsn: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RuntimeSnapshot {
    pub(crate) identity: ReplicaIdentity,
    pub(crate) open: bool,
    pub(crate) replication_address: Option<String>,
    pub(crate) role: ReplicaRole,
    pub(crate) role_transition: Option<RoleTransition>,
    pub(crate) read_status: AccessStatus,
    pub(crate) write_status: AccessStatus,
    pub(crate) authority: Option<AdmittedAuthority>,
    pub(crate) prepared_secondary_removal: Option<SecondaryRemovalPreparation>,
    pub(crate) retired_authority: Option<RetiredAuthority>,
    pub(crate) accepted_secondary_removal: Option<SecondaryScaleDownCleanup>,
    pub(crate) current_progress: i64,
    pub(crate) verified_replication_lsn: Option<i64>,
    #[serde(default)]
    pub(crate) live_builds_only: bool,
    pub(crate) committed_lsn: i64,
    pub(crate) current_configuration_quorum_progress: i64,
    pub(crate) catch_up_boundary: Option<i64>,
    pub(crate) catch_up_complete: bool,
    pub(crate) builds: Vec<BuildPostcondition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RoleCompletion {
    pub(crate) role: ReplicaRole,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub(crate) role_transition: Option<RoleTransition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct EpochCompletion {
    pub(crate) epoch: Epoch,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub(crate) role_transition: Option<RoleTransition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum AccessCompletionKind {
    Combined,
    Read,
    Write,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AccessCompletion {
    pub(crate) kind: AccessCompletionKind,
    pub(crate) read_status: AccessStatus,
    pub(crate) write_status: AccessStatus,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub(crate) authority: Option<AdmittedAuthority>,
    pub(crate) role: ReplicaRole,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CatchUpCompletion {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub(crate) authority: Option<AdmittedAuthority>,
    pub(crate) boundary_lsn: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum BuildEffectState {
    Dispatched,
    Completed(Box<BuildPostcondition>),
    Abandoned,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BuildCompletion {
    pub(crate) build_id: OperationId,
    pub(crate) target: ReplicaIdentity,
    pub(crate) state: BuildEffectState,
}

impl BuildCompletion {
    pub(crate) fn is_dispatched(&self) -> bool {
        matches!(&self.state, BuildEffectState::Dispatched)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AuthorityCompletion {
    pub(crate) authority: AdmittedAuthority,
    pub(crate) read_status: AccessStatus,
    pub(crate) write_status: AccessStatus,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub(crate) accepted_secondary_removal: Option<SecondaryScaleDownCleanup>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SecondaryRemovalPreparationCompletion {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub(crate) prepared_secondary_removal: Option<SecondaryRemovalPreparation>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub(crate) authority: Option<AdmittedAuthority>,
    pub(crate) role: ReplicaRole,
    pub(crate) read_status: AccessStatus,
    pub(crate) write_status: AccessStatus,
    pub(crate) current_progress: i64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub(crate) verified_replication_lsn: Option<i64>,
    pub(crate) committed_lsn: i64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub(crate) receipt: Option<Box<SecondaryRemovalReceipt>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct HistoricalSecondaryRemovalCompletion {
    pub(crate) accepted_secondary_removal: SecondaryScaleDownCleanup,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub(crate) authority: Option<AdmittedAuthority>,
    pub(crate) role: ReplicaRole,
    pub(crate) write_status: AccessStatus,
    pub(crate) role_transition_clear: bool,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub(crate) verified_replication_lsn: Option<i64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub(crate) receipt: Option<Box<SecondaryRemovalReceipt>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SwitchoverCompletion {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub(crate) authority: Option<AdmittedAuthority>,
    pub(crate) role: ReplicaRole,
    pub(crate) write_status: AccessStatus,
    pub(crate) current_progress: i64,
    pub(crate) committed_lsn: i64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub(crate) receipt: Option<Box<SwitchoverReceipt>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RetirementCompletion {
    pub(crate) retired: RetiredAuthority,
    pub(crate) open: bool,
    pub(crate) role: ReplicaRole,
    pub(crate) read_status: AccessStatus,
    pub(crate) write_status: AccessStatus,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub(crate) authority: Option<AdmittedAuthority>,
    pub(crate) role_transition_clear: bool,
    pub(crate) active_builds: bool,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub(crate) receipt: Option<Box<RetirementReceipt>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProcessCompletion {
    pub(crate) open: bool,
    pub(crate) role: ReplicaRole,
    pub(crate) read_status: AccessStatus,
    pub(crate) write_status: AccessStatus,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub(crate) authority: Option<AdmittedAuthority>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum RuntimeEffectOutcome {
    Opened,
    AuthorityAdmitted(AuthorityCompletion),
    SecondaryRemovalPrepared(SecondaryRemovalPreparationCompletion),
    PeerSessionRegistered {
        identity: ReplicaIdentity,
        session: ProcessSessionId,
    },
    SecondaryRemovalWitnessObserved {
        witness: Box<SecondaryRemovalWitness>,
        #[serde(deserialize_with = "deserialize_required_option")]
        receipt: Option<Box<SecondaryRemovalReceipt>>,
    },
    SecondaryRemovalProgressObserved {
        witness: Box<SecondaryRemovalWitness>,
        committed: Box<SecondaryScaleDownCleanup>,
        #[serde(deserialize_with = "deserialize_required_option")]
        receipt: Option<Box<SecondaryRemovalReceipt>>,
    },
    ReplicationAckObserved {
        acknowledgement: Box<crate::transport::ReplicationAck>,
        session: ProcessSessionId,
    },
    SecondaryRemovalAccepted {
        committed: Box<SecondaryScaleDownCleanup>,
        #[serde(deserialize_with = "deserialize_required_option")]
        receipt: Option<Box<SecondaryRemovalReceipt>>,
    },
    HistoricalSecondaryRemovalAccepted(HistoricalSecondaryRemovalCompletion),
    ReplicaRetired(RetirementCompletion),
    RetirementFenced(RetirementCompletion),
    RetirementCompleted(RetirementCompletion),
    FailoverPrefixAuthorized {
        boundary_lsn: i64,
        #[serde(deserialize_with = "deserialize_required_option")]
        receipt: Option<Box<CertifiedPrefixReceipt>>,
    },
    BuildAuthorityAdmitted {
        authority: Box<BuildAuthority>,
    },
    RoleChanged(RoleCompletion),
    ReplicatorRoleChanged(RoleCompletion),
    EpochUpdated(EpochCompletion),
    ApplicationRoleChanged {
        completion: RoleCompletion,
        #[serde(deserialize_with = "deserialize_required_option")]
        receipt: Option<Box<CertifiedPrefixReceipt>>,
    },
    CatchUpCompleted(CatchUpCompletion),
    AccessChanged(AccessCompletion),
    SwitchoverPrepared(SwitchoverCompletion),
    ApplicationProgressRefreshed {
        current_progress: i64,
    },
    BuildReplica(BuildCompletion),
    BuildRetired {
        build_id: OperationId,
        active: bool,
    },
    Closed(ProcessCompletion),
    Aborted(ProcessCompletion),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeEffectFamily {
    Open,
    Authority,
    SecondaryRemovalPreparation,
    PeerSession,
    SecondaryRemovalWitness,
    SecondaryRemovalProgress,
    ReplicationAck,
    SecondaryRemovalAcceptance,
    HistoricalSecondaryRemovalAcceptance,
    ReplicaRetirement,
    RetirementFence,
    RetirementCompletion,
    FailoverPrefix,
    BuildAuthority,
    Role,
    ReplicatorRole,
    Epoch,
    ApplicationRole,
    CatchUp,
    Access,
    Switchover,
    ApplicationProgress,
    Build,
    BuildRetirement,
    Close,
    Abort,
}

impl RuntimeEffectAction {
    fn completion_family(&self) -> RuntimeEffectFamily {
        match self {
            Self::Open(_) => RuntimeEffectFamily::Open,
            Self::AdmitAuthority(_) => RuntimeEffectFamily::Authority,
            Self::PrepareSecondaryRemoval { .. } => {
                RuntimeEffectFamily::SecondaryRemovalPreparation
            }
            Self::RegisterPeerSession { .. } => RuntimeEffectFamily::PeerSession,
            Self::ObserveSecondaryRemovalWitness(_) => RuntimeEffectFamily::SecondaryRemovalWitness,
            Self::ObserveSecondaryRemovalProgress { .. } => {
                RuntimeEffectFamily::SecondaryRemovalProgress
            }
            Self::ObserveReplicationAck { .. } => RuntimeEffectFamily::ReplicationAck,
            Self::AcceptSecondaryRemovalCommit(_) => {
                RuntimeEffectFamily::SecondaryRemovalAcceptance
            }
            Self::AcceptHistoricalSecondaryRemovalCommit(_) => {
                RuntimeEffectFamily::HistoricalSecondaryRemovalAcceptance
            }
            Self::RetireReplica(_) => RuntimeEffectFamily::ReplicaRetirement,
            Self::FenceRetirement(_) => RuntimeEffectFamily::RetirementFence,
            Self::CompleteRetirement(_) => RuntimeEffectFamily::RetirementCompletion,
            Self::AuthorizeFailoverPrefix(_) => RuntimeEffectFamily::FailoverPrefix,
            Self::AdmitBuildAuthority(_) => RuntimeEffectFamily::BuildAuthority,
            Self::ChangeRole(_) => RuntimeEffectFamily::Role,
            Self::ChangeReplicatorRole(_) => RuntimeEffectFamily::ReplicatorRole,
            Self::UpdateEpoch => RuntimeEffectFamily::Epoch,
            Self::ChangeApplicationRole(_) => RuntimeEffectFamily::ApplicationRole,
            Self::WaitForCatchup => RuntimeEffectFamily::CatchUp,
            Self::SetAccessStatus { .. } | Self::SetReadStatus(_) | Self::SetWriteStatus(_) => {
                RuntimeEffectFamily::Access
            }
            Self::PrepareSwitchover { .. } => RuntimeEffectFamily::Switchover,
            Self::RefreshApplicationProgress => RuntimeEffectFamily::ApplicationProgress,
            Self::BuildReplica { .. } => RuntimeEffectFamily::Build,
            Self::RetireBuild(_) => RuntimeEffectFamily::BuildRetirement,
            Self::Close => RuntimeEffectFamily::Close,
            Self::Abort => RuntimeEffectFamily::Abort,
        }
    }
}

impl RuntimeEffectOutcome {
    fn completion_family(&self) -> RuntimeEffectFamily {
        match self {
            Self::Opened => RuntimeEffectFamily::Open,
            Self::AuthorityAdmitted(_) => RuntimeEffectFamily::Authority,
            Self::SecondaryRemovalPrepared(_) => RuntimeEffectFamily::SecondaryRemovalPreparation,
            Self::PeerSessionRegistered { .. } => RuntimeEffectFamily::PeerSession,
            Self::SecondaryRemovalWitnessObserved { .. } => {
                RuntimeEffectFamily::SecondaryRemovalWitness
            }
            Self::SecondaryRemovalProgressObserved { .. } => {
                RuntimeEffectFamily::SecondaryRemovalProgress
            }
            Self::ReplicationAckObserved { .. } => RuntimeEffectFamily::ReplicationAck,
            Self::SecondaryRemovalAccepted { .. } => {
                RuntimeEffectFamily::SecondaryRemovalAcceptance
            }
            Self::HistoricalSecondaryRemovalAccepted(_) => {
                RuntimeEffectFamily::HistoricalSecondaryRemovalAcceptance
            }
            Self::ReplicaRetired(_) => RuntimeEffectFamily::ReplicaRetirement,
            Self::RetirementFenced(_) => RuntimeEffectFamily::RetirementFence,
            Self::RetirementCompleted(_) => RuntimeEffectFamily::RetirementCompletion,
            Self::FailoverPrefixAuthorized { .. } => RuntimeEffectFamily::FailoverPrefix,
            Self::BuildAuthorityAdmitted { .. } => RuntimeEffectFamily::BuildAuthority,
            Self::RoleChanged(_) => RuntimeEffectFamily::Role,
            Self::ReplicatorRoleChanged(_) => RuntimeEffectFamily::ReplicatorRole,
            Self::EpochUpdated(_) => RuntimeEffectFamily::Epoch,
            Self::ApplicationRoleChanged { .. } => RuntimeEffectFamily::ApplicationRole,
            Self::CatchUpCompleted(_) => RuntimeEffectFamily::CatchUp,
            Self::AccessChanged(_) => RuntimeEffectFamily::Access,
            Self::SwitchoverPrepared(_) => RuntimeEffectFamily::Switchover,
            Self::ApplicationProgressRefreshed { .. } => RuntimeEffectFamily::ApplicationProgress,
            Self::BuildReplica(_) => RuntimeEffectFamily::Build,
            Self::BuildRetired { .. } => RuntimeEffectFamily::BuildRetirement,
            Self::Closed(_) => RuntimeEffectFamily::Close,
            Self::Aborted(_) => RuntimeEffectFamily::Abort,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RuntimeEffectResult {
    pub(crate) operation_id: OperationId,
    pub(crate) sequence: u64,
    pub(crate) outcome: RuntimeEffectOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RecordedEffect {
    pub(crate) effect: RuntimeEffect,
    pub(crate) result: RuntimeEffectResult,
}

impl RuntimeEffectResult {
    pub(crate) fn validate_for(&self, effect: &RuntimeEffect) -> Result<(), &'static str> {
        if self.operation_id != effect.operation_id || self.sequence != effect.sequence {
            return Err("runtime returned a result for a different durable effect");
        }
        if effect.action.completion_family() != self.outcome.completion_family() {
            return Err("runtime result belongs to a different effect family");
        }
        let valid = match (&effect.action, &self.outcome) {
            (RuntimeEffectAction::Open(_), RuntimeEffectOutcome::Opened) => true,
            (
                RuntimeEffectAction::AdmitAuthority(expected),
                RuntimeEffectOutcome::AuthorityAdmitted(completion),
            ) => completion.authority == **expected,
            (
                RuntimeEffectAction::PrepareSecondaryRemoval {
                    intent,
                    process_session_id,
                    report_sequence,
                },
                RuntimeEffectOutcome::SecondaryRemovalPrepared(completion),
            ) => completion
                .prepared_secondary_removal
                .as_ref()
                .is_some_and(|prepared| {
                    prepared.intent == **intent
                        && prepared.process_session_id == *process_session_id
                        && prepared.report_sequence == *report_sequence
                        && prepared.operation_id == effect.operation_id
                }),
            (
                RuntimeEffectAction::RegisterPeerSession { identity, session },
                RuntimeEffectOutcome::PeerSessionRegistered {
                    identity: completed_identity,
                    session: completed_session,
                },
            ) => identity == completed_identity && session == completed_session,
            (
                RuntimeEffectAction::ObserveSecondaryRemovalWitness(expected),
                RuntimeEffectOutcome::SecondaryRemovalWitnessObserved { witness, .. },
            ) => expected == witness,
            (
                RuntimeEffectAction::ObserveSecondaryRemovalProgress { witness, committed },
                RuntimeEffectOutcome::SecondaryRemovalProgressObserved {
                    witness: completed_witness,
                    committed: completed_cleanup,
                    ..
                },
            ) => witness == completed_witness && committed == completed_cleanup,
            (
                RuntimeEffectAction::ObserveReplicationAck {
                    acknowledgement,
                    session,
                },
                RuntimeEffectOutcome::ReplicationAckObserved {
                    acknowledgement: completed_acknowledgement,
                    session: completed_session,
                },
            ) => acknowledgement == completed_acknowledgement && session == completed_session,
            (
                RuntimeEffectAction::AcceptSecondaryRemovalCommit(expected),
                RuntimeEffectOutcome::SecondaryRemovalAccepted { committed, .. },
            ) => expected == committed,
            (
                RuntimeEffectAction::AcceptHistoricalSecondaryRemovalCommit(command),
                RuntimeEffectOutcome::HistoricalSecondaryRemovalAccepted(completion),
            ) => completion.accepted_secondary_removal == command.committed,
            (
                RuntimeEffectAction::RetireReplica(expected),
                RuntimeEffectOutcome::ReplicaRetired(completion),
            )
            | (
                RuntimeEffectAction::FenceRetirement(expected),
                RuntimeEffectOutcome::RetirementFenced(completion),
            )
            | (
                RuntimeEffectAction::CompleteRetirement(expected),
                RuntimeEffectOutcome::RetirementCompleted(completion),
            ) => completion.retired == **expected,
            (
                RuntimeEffectAction::AuthorizeFailoverPrefix(expected),
                RuntimeEffectOutcome::FailoverPrefixAuthorized { boundary_lsn, .. },
            ) => expected == boundary_lsn,
            (
                RuntimeEffectAction::AdmitBuildAuthority(expected),
                RuntimeEffectOutcome::BuildAuthorityAdmitted { authority },
            ) => expected == authority,
            (
                RuntimeEffectAction::ChangeRole(expected),
                RuntimeEffectOutcome::RoleChanged(completion),
            ) => completion.role == *expected,
            (
                RuntimeEffectAction::ChangeReplicatorRole(expected),
                RuntimeEffectOutcome::ReplicatorRoleChanged(completion),
            ) => completion
                .role_transition
                .as_ref()
                .is_some_and(|transition| {
                    transition.target_role == *expected && transition.replicator_completed
                }),
            (RuntimeEffectAction::UpdateEpoch, RuntimeEffectOutcome::EpochUpdated(completion)) => {
                completion
                    .role_transition
                    .as_ref()
                    .is_some_and(|transition| transition.epoch_completed)
            }
            (
                RuntimeEffectAction::ChangeApplicationRole(expected),
                RuntimeEffectOutcome::ApplicationRoleChanged { completion, .. },
            ) => completion.role == *expected && completion.role_transition.is_none(),
            (RuntimeEffectAction::WaitForCatchup, RuntimeEffectOutcome::CatchUpCompleted(_)) => {
                true
            }
            (
                RuntimeEffectAction::SetAccessStatus { read, write },
                RuntimeEffectOutcome::AccessChanged(completion),
            ) => {
                completion.kind == AccessCompletionKind::Combined
                    && completion.read_status == *read
                    && completion.write_status == *write
            }
            (
                RuntimeEffectAction::SetReadStatus(expected),
                RuntimeEffectOutcome::AccessChanged(completion),
            ) => {
                completion.kind == AccessCompletionKind::Read && completion.read_status == *expected
            }
            (
                RuntimeEffectAction::SetWriteStatus(expected),
                RuntimeEffectOutcome::AccessChanged(completion),
            ) => {
                completion.kind == AccessCompletionKind::Write
                    && completion.write_status == *expected
            }
            (
                RuntimeEffectAction::PrepareSwitchover {
                    preparation_generation,
                    request_id,
                    source,
                    target,
                    starting_configuration_id,
                    starting_epoch,
                },
                RuntimeEffectOutcome::SwitchoverPrepared(completion),
            ) => completion.receipt.as_ref().is_none_or(|receipt| {
                receipt.preparation_generation == *preparation_generation
                    && receipt.request_id == *request_id
                    && receipt.source == *source
                    && receipt.target == *target
                    && receipt.starting_configuration_id == *starting_configuration_id
                    && receipt.starting_epoch == *starting_epoch
            }),
            (
                RuntimeEffectAction::RefreshApplicationProgress,
                RuntimeEffectOutcome::ApplicationProgressRefreshed { .. },
            ) => true,
            (
                RuntimeEffectAction::BuildReplica {
                    build_id, target, ..
                },
                RuntimeEffectOutcome::BuildReplica(completion),
            ) => {
                completion.build_id == *build_id
                    && completion.target == *target
                    && match &completion.state {
                        BuildEffectState::Dispatched | BuildEffectState::Abandoned => true,
                        BuildEffectState::Completed(build) => {
                            build.completed
                                && build.authority.build_id == *build_id
                                && build.authority.target == *target
                        }
                    }
            }
            (
                RuntimeEffectAction::RetireBuild(expected),
                RuntimeEffectOutcome::BuildRetired { build_id, active },
            ) => expected == build_id && !active,
            (RuntimeEffectAction::Close, RuntimeEffectOutcome::Closed(completion))
            | (RuntimeEffectAction::Abort, RuntimeEffectOutcome::Aborted(completion)) => {
                !completion.open
                    && completion.role == ReplicaRole::None
                    && completion.read_status == AccessStatus::NotPrimary
                    && completion.write_status == AccessStatus::NotPrimary
            }
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err("runtime result does not match the durable effect action")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::BuildAuthorityKind;
    use crate::protocol::types::{
        AgentGeneration, ConfigurationDescriptor, ConfigurationMember, ReplicaId, ReplicaInstanceId,
    };

    fn identity(id: i64) -> ReplicaIdentity {
        ReplicaIdentity {
            replica_id: ReplicaId::new(id),
            instance_id: ReplicaInstanceId::new(format!("instance-{id}")),
            agent_generation: AgentGeneration::new(format!("generation-{id}")),
        }
    }

    fn configuration() -> ConfigurationDescriptor {
        let local = identity(1);
        ConfigurationDescriptor::new(
            Epoch::new(1, 2),
            local.replica_id,
            vec![ConfigurationMember {
                identity: local,
                role: ReplicaRole::Primary,
            }],
            1,
        )
    }

    fn authority() -> AdmittedAuthority {
        AdmittedAuthority {
            local_identity: identity(1),
            transition_kind: None,
            previous_configuration: None,
            current_configuration: configuration(),
            switchover_handoff: None,
            scale_up: None,
            secondary_removal: None,
        }
    }

    fn required_family_cases() -> Vec<(RuntimeEffectAction, RuntimeEffectOutcome)> {
        let transition = RoleTransition {
            completed_role: ReplicaRole::IdleSecondary,
            target_role: ReplicaRole::Primary,
            replicator_completed: true,
            epoch_completed: true,
            application_completed: false,
        };
        let build_authority = BuildAuthority {
            build_id: OperationId::new("build"),
            kind: BuildAuthorityKind::Provisioning,
            source: identity(1),
            target: identity(2),
            current_configuration: configuration(),
            replication_boundary_lsn: 7,
        };
        vec![
            (
                RuntimeEffectAction::ChangeApplicationRole(ReplicaRole::Primary),
                RuntimeEffectOutcome::ApplicationRoleChanged {
                    completion: RoleCompletion {
                        role: ReplicaRole::Primary,
                        role_transition: None,
                    },
                    receipt: None,
                },
            ),
            (
                RuntimeEffectAction::UpdateEpoch,
                RuntimeEffectOutcome::EpochUpdated(EpochCompletion {
                    epoch: Epoch::new(1, 2),
                    role_transition: Some(transition),
                }),
            ),
            (
                RuntimeEffectAction::SetReadStatus(AccessStatus::Granted),
                RuntimeEffectOutcome::AccessChanged(AccessCompletion {
                    kind: AccessCompletionKind::Read,
                    read_status: AccessStatus::Granted,
                    write_status: AccessStatus::NotPrimary,
                    authority: Some(authority()),
                    role: ReplicaRole::Primary,
                }),
            ),
            (
                RuntimeEffectAction::WaitForCatchup,
                RuntimeEffectOutcome::CatchUpCompleted(CatchUpCompletion {
                    authority: Some(authority()),
                    boundary_lsn: 9,
                }),
            ),
            (
                RuntimeEffectAction::BuildReplica {
                    build_id: OperationId::new("build"),
                    target: identity(2),
                    replication_address: "in-process://target".into(),
                },
                RuntimeEffectOutcome::BuildReplica(BuildCompletion {
                    build_id: OperationId::new("build"),
                    target: identity(2),
                    state: BuildEffectState::Completed(Box::new(BuildPostcondition {
                        authority: build_authority,
                        last_sequence: 1,
                        durable_lsn: 9,
                        completed: true,
                        catch_up_boundary_lsn: Some(7),
                    })),
                }),
            ),
        ]
    }

    #[test]
    fn required_completion_families_round_trip_and_reject_substitution() {
        let cases = required_family_cases();
        for (index, (action, outcome)) in cases.iter().enumerate() {
            let effect = RuntimeEffect {
                operation_id: OperationId::new(format!("effect-{index}")),
                sequence: index as u64 + 1,
                action: action.clone(),
            };
            let result = RuntimeEffectResult {
                operation_id: effect.operation_id.clone(),
                sequence: effect.sequence,
                outcome: outcome.clone(),
            };
            result.validate_for(&effect).unwrap();
            let encoded = serde_json::to_vec(&result).unwrap();
            assert_eq!(
                serde_json::from_slice::<RuntimeEffectResult>(&encoded).unwrap(),
                result
            );
            let wrong = RuntimeEffectResult {
                outcome: cases[(index + 1) % cases.len()].1.clone(),
                ..result
            };
            assert!(wrong.validate_for(&effect).is_err());
        }
    }

    #[test]
    fn access_completion_rejects_wrong_suboperation_with_matching_status() {
        let result = RuntimeEffectResult {
            operation_id: OperationId::new("read"),
            sequence: 1,
            outcome: RuntimeEffectOutcome::AccessChanged(AccessCompletion {
                kind: AccessCompletionKind::Write,
                read_status: AccessStatus::Granted,
                write_status: AccessStatus::NotPrimary,
                authority: Some(authority()),
                role: ReplicaRole::Primary,
            }),
        };
        let effect = RuntimeEffect {
            operation_id: result.operation_id.clone(),
            sequence: result.sequence,
            action: RuntimeEffectAction::SetReadStatus(AccessStatus::Granted),
        };
        assert!(result.validate_for(&effect).is_err());
    }

    #[test]
    fn build_completion_rejects_nonterminal_or_mismatched_nested_proof() {
        let (action, outcome) = required_family_cases().pop().unwrap();
        let effect = RuntimeEffect {
            operation_id: OperationId::new("build-effect"),
            sequence: 1,
            action,
        };
        let RuntimeEffectOutcome::BuildReplica(completion) = outcome else {
            unreachable!()
        };
        let build = match &completion.state {
            BuildEffectState::Completed(build) => build.clone(),
            _ => unreachable!(),
        };
        let mut nonterminal = build.clone();
        nonterminal.completed = false;
        let result = RuntimeEffectResult {
            operation_id: effect.operation_id.clone(),
            sequence: effect.sequence,
            outcome: RuntimeEffectOutcome::BuildReplica(BuildCompletion {
                state: BuildEffectState::Completed(nonterminal),
                ..completion.clone()
            }),
        };
        assert!(result.validate_for(&effect).is_err());

        let mut mismatched = build;
        mismatched.authority.target = identity(3);
        let result = RuntimeEffectResult {
            outcome: RuntimeEffectOutcome::BuildReplica(BuildCompletion {
                state: BuildEffectState::Completed(mismatched),
                ..completion
            }),
            ..result
        };
        assert!(result.validate_for(&effect).is_err());
    }

    #[test]
    fn canonical_outcomes_reject_missing_required_fields() {
        let mut transition = serde_json::to_value(RoleTransition {
            completed_role: ReplicaRole::IdleSecondary,
            target_role: ReplicaRole::Primary,
            replicator_completed: true,
            epoch_completed: true,
            application_completed: false,
        })
        .unwrap();
        transition
            .as_object_mut()
            .unwrap()
            .remove("epoch_completed");
        assert!(serde_json::from_value::<RoleTransition>(transition).is_err());

        let mut build = match required_family_cases().pop().unwrap().1 {
            RuntimeEffectOutcome::BuildReplica(BuildCompletion {
                state: BuildEffectState::Completed(build),
                ..
            }) => serde_json::to_value(build).unwrap(),
            _ => unreachable!(),
        };
        build
            .as_object_mut()
            .unwrap()
            .remove("catch_up_boundary_lsn");
        assert!(serde_json::from_value::<BuildPostcondition>(build).is_err());
    }
}
