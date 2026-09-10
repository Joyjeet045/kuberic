use std::collections::HashSet;

use prost::Message;

use crate::add_replica::{
    AddReplicaCoordinatorPhase, AddReplicaIntent, AddReplicaMode, AddReplicaProgress,
    AddReplicaTerminalResult, RuntimeBuildObservation, RuntimeBuildState,
};
use crate::proto;
use crate::remove_replica::{
    RemoveReplicaCoordinatorPhase, RemoveReplicaIntent, RemoveReplicaMode, RemoveReplicaProgress,
    RemoveReplicaTerminalResult, TargetRetirementObservation,
};
use crate::replica_lifecycle::{
    ConfigurationDescriptor, ConfigurationMemberDescriptor, ConfigurationProgressSource,
    MAX_LIFECYCLE_ERROR_BYTES, PeerLifecycleStatus, PeerOperationKind, PeerStage,
    PeerStageObservation, PeerStageRequest, PeerStageState,
};
use crate::types::{
    AccessStatus, AgentControlVersion, AgentGeneration, CorrelatedActionObservation,
    CorrelatedControlActionRequest, DataLossAction, DurableActionErrorClass,
    DurableActionObservation, DurableActionResult, DurableActionState, DurableReplicaAction, Epoch,
    FaultType, LocalFaultRecord, OpenMode, ReplicaConfigurationMemberStatus,
    ReplicaConfigurationMode, ReplicaConfigurationStatus, ReplicaDeactivationInfo,
    ReplicaElectionConfiguration, ReplicaInfo, ReplicaInstanceId, ReplicaSetConfig,
    ReplicaSetQuorumMode, ReplicaStatus, Role,
};

// --- Epoch ---

impl From<Epoch> for proto::EpochProto {
    fn from(e: Epoch) -> Self {
        proto::EpochProto {
            data_loss_number: e.data_loss_number,
            configuration_number: e.configuration_number,
        }
    }
}

impl From<proto::EpochProto> for Epoch {
    fn from(e: proto::EpochProto) -> Self {
        Epoch::new(e.data_loss_number, e.configuration_number)
    }
}

// --- Role ---

impl From<Role> for proto::RoleProto {
    fn from(r: Role) -> Self {
        match r {
            Role::Unknown => proto::RoleProto::RoleUnknown,
            Role::Primary => proto::RoleProto::RolePrimary,
            Role::ActiveSecondary => proto::RoleProto::RoleActiveSecondary,
            Role::IdleSecondary => proto::RoleProto::RoleIdleSecondary,
            Role::None => proto::RoleProto::RoleNone,
        }
    }
}

impl From<proto::RoleProto> for Role {
    fn from(r: proto::RoleProto) -> Self {
        match r {
            proto::RoleProto::RoleUnknown => Role::Unknown,
            proto::RoleProto::RolePrimary => Role::Primary,
            proto::RoleProto::RoleActiveSecondary => Role::ActiveSecondary,
            proto::RoleProto::RoleIdleSecondary => Role::IdleSecondary,
            proto::RoleProto::RoleNone => Role::None,
        }
    }
}

impl From<i32> for Role {
    fn from(v: i32) -> Self {
        proto::RoleProto::try_from(v)
            .unwrap_or(proto::RoleProto::RoleUnknown)
            .into()
    }
}

// --- OpenMode ---

impl From<OpenMode> for proto::OpenModeProto {
    fn from(m: OpenMode) -> Self {
        match m {
            OpenMode::New => proto::OpenModeProto::OpenNew,
            OpenMode::Existing => proto::OpenModeProto::OpenExisting,
        }
    }
}

impl From<proto::OpenModeProto> for OpenMode {
    fn from(m: proto::OpenModeProto) -> Self {
        match m {
            proto::OpenModeProto::OpenNew => OpenMode::New,
            proto::OpenModeProto::OpenExisting => OpenMode::Existing,
        }
    }
}

impl From<i32> for OpenMode {
    fn from(v: i32) -> Self {
        proto::OpenModeProto::try_from(v)
            .unwrap_or(proto::OpenModeProto::OpenNew)
            .into()
    }
}

// --- QuorumMode ---

impl From<ReplicaSetQuorumMode> for proto::QuorumModeProto {
    fn from(m: ReplicaSetQuorumMode) -> Self {
        match m {
            ReplicaSetQuorumMode::All => proto::QuorumModeProto::QuorumAll,
            ReplicaSetQuorumMode::Write => proto::QuorumModeProto::QuorumWrite,
        }
    }
}

impl From<proto::QuorumModeProto> for ReplicaSetQuorumMode {
    fn from(m: proto::QuorumModeProto) -> Self {
        match m {
            proto::QuorumModeProto::QuorumAll => ReplicaSetQuorumMode::All,
            proto::QuorumModeProto::QuorumWrite => ReplicaSetQuorumMode::Write,
        }
    }
}

impl From<i32> for ReplicaSetQuorumMode {
    fn from(v: i32) -> Self {
        proto::QuorumModeProto::try_from(v)
            .unwrap_or(proto::QuorumModeProto::QuorumAll)
            .into()
    }
}

impl From<AccessStatus> for proto::AccessStatusProto {
    fn from(status: AccessStatus) -> Self {
        match status {
            AccessStatus::Granted => Self::AccessGranted,
            AccessStatus::ReconfigurationPending => Self::AccessReconfigurationPending,
            AccessStatus::NotPrimary => Self::AccessNotPrimary,
            AccessStatus::NoWriteQuorum => Self::AccessNoWriteQuorum,
        }
    }
}

pub(crate) fn try_access_status(value: i32) -> Result<AccessStatus, String> {
    match proto::AccessStatusProto::try_from(value)
        .map_err(|_| format!("unknown runtime write status {value}"))?
    {
        proto::AccessStatusProto::AccessGranted => Ok(AccessStatus::Granted),
        proto::AccessStatusProto::AccessReconfigurationPending => {
            Ok(AccessStatus::ReconfigurationPending)
        }
        proto::AccessStatusProto::AccessNotPrimary => Ok(AccessStatus::NotPrimary),
        proto::AccessStatusProto::AccessNoWriteQuorum => Ok(AccessStatus::NoWriteQuorum),
    }
}

impl From<ReplicaConfigurationStatus> for proto::ReplicaConfigurationStatusProto {
    fn from(status: ReplicaConfigurationStatus) -> Self {
        Self {
            mode: match status.mode {
                ReplicaConfigurationMode::CatchUp => {
                    proto::ReplicaConfigurationModeProto::ConfigurationCatchUp as i32
                }
                ReplicaConfigurationMode::Current => {
                    proto::ReplicaConfigurationModeProto::ConfigurationCurrent as i32
                }
            },
            members: status
                .members
                .into_iter()
                .map(|member| proto::ReplicaConfigurationMemberStatusProto {
                    id: member.id,
                    instance_id: member.instance_id.to_string(),
                    role: proto::RoleProto::from(member.role) as i32,
                })
                .collect(),
            write_quorum: status.write_quorum,
        }
    }
}

pub(crate) fn try_runtime_configuration_status(
    status: proto::ReplicaConfigurationStatusProto,
) -> Result<ReplicaConfigurationStatus, String> {
    let mode = match proto::ReplicaConfigurationModeProto::try_from(status.mode)
        .map_err(|_| format!("unknown runtime configuration mode {}", status.mode))?
    {
        proto::ReplicaConfigurationModeProto::ConfigurationCatchUp => {
            ReplicaConfigurationMode::CatchUp
        }
        proto::ReplicaConfigurationModeProto::ConfigurationCurrent => {
            ReplicaConfigurationMode::Current
        }
        proto::ReplicaConfigurationModeProto::ConfigurationNone => {
            return Err("runtime configuration mode is none".to_string());
        }
    };
    let mut ids = HashSet::new();
    let mut instances = HashSet::new();
    let members = status
        .members
        .into_iter()
        .map(|member| {
            if member.id <= 0 || !ids.insert(member.id) {
                return Err(format!(
                    "runtime configuration has invalid or duplicate replica ID {}",
                    member.id
                ));
            }
            if member.instance_id.is_empty() || !instances.insert(member.instance_id.clone()) {
                return Err(format!(
                    "runtime configuration replica {} has missing or duplicate incarnation",
                    member.id
                ));
            }
            let role = proto::RoleProto::try_from(member.role)
                .map_err(|_| format!("unknown role {}", member.role))?;
            if role == proto::RoleProto::RoleUnknown {
                return Err(format!(
                    "runtime configuration replica {} has unknown role",
                    member.id
                ));
            }
            Ok(ReplicaConfigurationMemberStatus {
                id: member.id,
                instance_id: ReplicaInstanceId::new(member.instance_id),
                role: role.into(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    if status.write_quorum == 0 || status.write_quorum as usize > members.len() + 1 {
        return Err(format!(
            "invalid runtime configuration write quorum {} for {} remote members",
            status.write_quorum,
            members.len()
        ));
    }
    Ok(ReplicaConfigurationStatus {
        mode,
        members,
        write_quorum: status.write_quorum,
    })
}

impl From<ReplicaElectionConfiguration> for proto::ReplicaElectionConfigurationProto {
    fn from(configuration: ReplicaElectionConfiguration) -> Self {
        Self {
            current: Some(configuration.current.into()),
            previous: configuration.previous.map(Into::into),
        }
    }
}

impl TryFrom<proto::ReplicaElectionConfigurationProto> for ReplicaElectionConfiguration {
    type Error = String;

    fn try_from(
        configuration: proto::ReplicaElectionConfigurationProto,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            current: try_configuration_status(
                configuration
                    .current
                    .ok_or_else(|| "missing current election configuration".to_string())?,
            )?,
            previous: configuration
                .previous
                .map(try_configuration_status)
                .transpose()?,
        })
    }
}

impl From<ReplicaDeactivationInfo> for proto::ReplicaDeactivationInfoProto {
    fn from(info: ReplicaDeactivationInfo) -> Self {
        Self {
            epoch: Some(info.epoch.into()),
            catch_up_lsn: info.catch_up_lsn,
        }
    }
}

impl TryFrom<proto::ReplicaDeactivationInfoProto> for ReplicaDeactivationInfo {
    type Error = String;

    fn try_from(info: proto::ReplicaDeactivationInfoProto) -> Result<Self, Self::Error> {
        Ok(Self {
            epoch: info
                .epoch
                .ok_or_else(|| "missing deactivation epoch".to_string())?
                .into(),
            catch_up_lsn: info.catch_up_lsn,
        })
    }
}

impl TryFrom<DurableActionResult> for proto::DurableActionResultProto {
    type Error = String;

    fn try_from(result: DurableActionResult) -> Result<Self, Self::Error> {
        Ok(match result {
            DurableActionResult::DataLoss(DataLossAction::None) => {
                Self::DurableActionResultDataLossNoStateChange
            }
            DurableActionResult::DataLoss(DataLossAction::StateChanged) => {
                Self::DurableActionResultDataLossStateChanged
            }
            DurableActionResult::AddReplica(AddReplicaTerminalResult::Committed) => {
                Self::DurableActionResultAddReplicaCommitted
            }
            DurableActionResult::AddReplica(AddReplicaTerminalResult::Compensated) => {
                Self::DurableActionResultAddReplicaCompensated
            }
            DurableActionResult::AddReplica(AddReplicaTerminalResult::CompensationIncomplete) => {
                Self::DurableActionResultAddReplicaCompensationIncomplete
            }
            DurableActionResult::RemoveReplica(RemoveReplicaTerminalResult::CommittedClean) => {
                Self::DurableActionResultRemoveReplicaCommittedClean
            }
            DurableActionResult::RemoveReplica(RemoveReplicaTerminalResult::CommittedDegraded) => {
                Self::DurableActionResultRemoveReplicaCommittedDegraded
            }
            DurableActionResult::RemoveReplica(RemoveReplicaTerminalResult::Compensated) => {
                Self::DurableActionResultRemoveReplicaCompensated
            }
            DurableActionResult::RemoveReplica(
                RemoveReplicaTerminalResult::CompensationIncomplete,
            ) => Self::DurableActionResultRemoveReplicaCompensationIncomplete,
        })
    }
}

impl TryFrom<proto::DurableActionResultProto> for DurableActionResult {
    type Error = ();

    fn try_from(result: proto::DurableActionResultProto) -> Result<Self, Self::Error> {
        match result {
            proto::DurableActionResultProto::DurableActionResultDataLossNoStateChange => {
                Ok(Self::DataLoss(DataLossAction::None))
            }

            proto::DurableActionResultProto::DurableActionResultDataLossStateChanged => {
                Ok(Self::DataLoss(DataLossAction::StateChanged))
            }
            proto::DurableActionResultProto::DurableActionResultAddReplicaCommitted => Ok(
                Self::AddReplica(AddReplicaTerminalResult::Committed),
            ),
            proto::DurableActionResultProto::DurableActionResultAddReplicaCompensated => Ok(
                Self::AddReplica(AddReplicaTerminalResult::Compensated),
            ),
            proto::DurableActionResultProto::DurableActionResultAddReplicaCompensationIncomplete => {
                Ok(Self::AddReplica(
                    AddReplicaTerminalResult::CompensationIncomplete,
                ))
            }
            proto::DurableActionResultProto::DurableActionResultRemoveReplicaCommittedClean => {
                Ok(Self::RemoveReplica(
                    RemoveReplicaTerminalResult::CommittedClean,
                ))
            }
            proto::DurableActionResultProto::DurableActionResultRemoveReplicaCommittedDegraded => {
                Ok(Self::RemoveReplica(
                    RemoveReplicaTerminalResult::CommittedDegraded,
                ))
            }
            proto::DurableActionResultProto::DurableActionResultRemoveReplicaCompensated => {
                Ok(Self::RemoveReplica(
                    RemoveReplicaTerminalResult::Compensated,
                ))
            }
            proto::DurableActionResultProto::DurableActionResultRemoveReplicaCompensationIncomplete => {
                Ok(Self::RemoveReplica(
                    RemoveReplicaTerminalResult::CompensationIncomplete,
                ))
            }
            proto::DurableActionResultProto::DurableActionResultNone => Err(()),
        }
    }
}

impl From<DurableActionErrorClass> for proto::DurableActionErrorClassProto {
    fn from(class: DurableActionErrorClass) -> Self {
        match class {
            DurableActionErrorClass::Internal => Self::DurableActionErrorInternal,
            DurableActionErrorClass::NotPrimary => Self::DurableActionErrorNotPrimary,
            DurableActionErrorClass::NoWriteQuorum => Self::DurableActionErrorNoWriteQuorum,
            DurableActionErrorClass::ReconfigurationPending => {
                Self::DurableActionErrorReconfigurationPending
            }
            DurableActionErrorClass::StaleEpoch => Self::DurableActionErrorStaleEpoch,
            DurableActionErrorClass::Cancelled => Self::DurableActionErrorCancelled,
            DurableActionErrorClass::Closed => Self::DurableActionErrorClosed,
        }
    }
}

impl TryFrom<proto::DurableActionErrorClassProto> for DurableActionErrorClass {
    type Error = ();

    fn try_from(class: proto::DurableActionErrorClassProto) -> Result<Self, Self::Error> {
        match class {
            proto::DurableActionErrorClassProto::DurableActionErrorInternal => Ok(Self::Internal),
            proto::DurableActionErrorClassProto::DurableActionErrorNotPrimary => {
                Ok(Self::NotPrimary)
            }
            proto::DurableActionErrorClassProto::DurableActionErrorNoWriteQuorum => {
                Ok(Self::NoWriteQuorum)
            }
            proto::DurableActionErrorClassProto::DurableActionErrorReconfigurationPending => {
                Ok(Self::ReconfigurationPending)
            }
            proto::DurableActionErrorClassProto::DurableActionErrorStaleEpoch => {
                Ok(Self::StaleEpoch)
            }
            proto::DurableActionErrorClassProto::DurableActionErrorCancelled => Ok(Self::Cancelled),
            proto::DurableActionErrorClassProto::DurableActionErrorClosed => Ok(Self::Closed),
            proto::DurableActionErrorClassProto::DurableActionErrorNone => Err(()),
        }
    }
}

fn try_configuration_status(
    status: proto::ReplicaConfigurationStatusProto,
) -> Result<ReplicaConfigurationStatus, String> {
    let mode = match proto::ReplicaConfigurationModeProto::try_from(status.mode)
        .map_err(|_| format!("unknown configuration mode {}", status.mode))?
    {
        proto::ReplicaConfigurationModeProto::ConfigurationCatchUp => {
            ReplicaConfigurationMode::CatchUp
        }
        proto::ReplicaConfigurationModeProto::ConfigurationCurrent => {
            ReplicaConfigurationMode::Current
        }
        proto::ReplicaConfigurationModeProto::ConfigurationNone => {
            return Err("election configuration mode is none".to_string());
        }
    };
    let members = status
        .members
        .into_iter()
        .map(|member| {
            if member.instance_id.is_empty() {
                return Err(format!(
                    "election configuration member {} has empty incarnation",
                    member.id
                ));
            }
            let role = proto::RoleProto::try_from(member.role)
                .map_err(|_| format!("unknown role {}", member.role))?;
            if role == proto::RoleProto::RoleUnknown {
                return Err(format!(
                    "election configuration member {} has unknown role",
                    member.id
                ));
            }
            Ok(ReplicaConfigurationMemberStatus {
                id: member.id,
                instance_id: ReplicaInstanceId::new(member.instance_id),
                role: role.into(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    if members.is_empty() {
        return Err("election configuration has no members".to_string());
    }
    if status.write_quorum == 0 || status.write_quorum as usize > members.len() {
        return Err(format!(
            "invalid election configuration write quorum {} for {} members",
            status.write_quorum,
            members.len()
        ));
    }
    Ok(ReplicaConfigurationStatus {
        mode,
        members,
        write_quorum: status.write_quorum,
    })
}

// --- ReplicaInfo ---

impl From<ReplicaInfo> for proto::ReplicaInfoProto {
    fn from(r: ReplicaInfo) -> Self {
        proto::ReplicaInfoProto {
            id: r.id,
            instance_id: r.instance_id.to_string(),
            role: proto::RoleProto::from(r.role) as i32,
            status: match r.status {
                ReplicaStatus::Up => proto::ReplicaStatusProto::StatusUp as i32,
                ReplicaStatus::Down => proto::ReplicaStatusProto::StatusDown as i32,
            },
            replicator_address: r.replicator_address,
            current_progress: r.current_progress,
            catch_up_capability: r.catch_up_capability,
            must_catch_up: r.must_catch_up,
        }
    }
}

impl From<proto::ReplicaInfoProto> for ReplicaInfo {
    fn from(r: proto::ReplicaInfoProto) -> Self {
        ReplicaInfo {
            id: r.id,
            instance_id: ReplicaInstanceId::new(r.instance_id),
            role: Role::from(r.role),
            status: if r.status == proto::ReplicaStatusProto::StatusUp as i32 {
                ReplicaStatus::Up
            } else {
                ReplicaStatus::Down
            },
            replicator_address: r.replicator_address,
            current_progress: r.current_progress,
            catch_up_capability: r.catch_up_capability,
            must_catch_up: r.must_catch_up,
        }
    }
}

// --- ReplicaSetConfig ---

impl From<ReplicaSetConfig> for proto::ReplicaSetConfigProto {
    fn from(c: ReplicaSetConfig) -> Self {
        proto::ReplicaSetConfigProto {
            members: c.members.into_iter().map(|r| r.into()).collect(),
            write_quorum: c.write_quorum,
        }
    }
}

impl From<proto::ReplicaSetConfigProto> for ReplicaSetConfig {
    fn from(c: proto::ReplicaSetConfigProto) -> Self {
        ReplicaSetConfig {
            members: c.members.into_iter().map(|r| r.into()).collect(),
            write_quorum: c.write_quorum,
        }
    }
}

fn configuration_descriptor_proto(
    descriptor: ConfigurationDescriptor,
) -> proto::ConfigurationDescriptorProto {
    proto::ConfigurationDescriptorProto {
        members: descriptor
            .members
            .into_iter()
            .map(|member| {
                let (progress_source, current_progress, catch_up_capability) = match member.progress
                {
                    ConfigurationProgressSource::Frozen {
                        current_progress,
                        catch_up_capability,
                    } => (
                        proto::ConfigurationProgressSourceProto::ConfigurationProgressFrozen,
                        current_progress,
                        catch_up_capability,
                    ),
                    ConfigurationProgressSource::BuildCopyLsn => (
                        proto::ConfigurationProgressSourceProto::ConfigurationProgressBuildCopyLsn,
                        0,
                        0,
                    ),
                };
                proto::ConfigurationMemberDescriptorProto {
                    id: member.id,
                    instance_id: member.instance_id.to_string(),
                    role: proto::RoleProto::from(member.role) as i32,
                    status: match member.status {
                        ReplicaStatus::Up => proto::ReplicaStatusProto::StatusUp as i32,
                        ReplicaStatus::Down => proto::ReplicaStatusProto::StatusDown as i32,
                    },
                    replicator_address: member.replicator_address,
                    must_catch_up: member.must_catch_up,
                    progress_source: progress_source as i32,
                    current_progress,
                    catch_up_capability,
                }
            })
            .collect(),
        write_quorum: descriptor.write_quorum,
    }
}

fn try_configuration_descriptor(
    descriptor: proto::ConfigurationDescriptorProto,
) -> Result<ConfigurationDescriptor, String> {
    let mut ids = HashSet::new();
    let mut instances = HashSet::new();
    let members = descriptor
        .members
        .into_iter()
        .map(|member| {
            if member.id <= 0 || !ids.insert(member.id) {
                return Err(format!(
                    "configuration descriptor has invalid or duplicate replica ID {}",
                    member.id
                ));
            }
            if member.instance_id.is_empty() || !instances.insert(member.instance_id.clone()) {
                return Err(format!(
                    "configuration descriptor replica {} has missing or duplicate incarnation",
                    member.id
                ));
            }
            if member.replicator_address.is_empty() {
                return Err(format!(
                    "configuration descriptor replica {} has no replicator address",
                    member.id
                ));
            }
            let role = proto::RoleProto::try_from(member.role)
                .map_err(|_| format!("unknown descriptor role {}", member.role))?;
            if role == proto::RoleProto::RoleUnknown || role == proto::RoleProto::RolePrimary {
                return Err(format!(
                    "configuration descriptor replica {} has invalid remote role",
                    member.id
                ));
            }
            let status = match proto::ReplicaStatusProto::try_from(member.status)
                .map_err(|_| format!("unknown descriptor status {}", member.status))?
            {
                proto::ReplicaStatusProto::StatusUp => ReplicaStatus::Up,
                proto::ReplicaStatusProto::StatusDown => ReplicaStatus::Down,
            };
            let progress =
                match proto::ConfigurationProgressSourceProto::try_from(member.progress_source)
                    .map_err(|_| {
                        format!(
                            "unknown configuration progress source {}",
                            member.progress_source
                        )
                    })? {
                    proto::ConfigurationProgressSourceProto::ConfigurationProgressFrozen => {
                        ConfigurationProgressSource::Frozen {
                            current_progress: member.current_progress,
                            catch_up_capability: member.catch_up_capability,
                        }
                    }
                    proto::ConfigurationProgressSourceProto::ConfigurationProgressBuildCopyLsn => {
                        if member.current_progress != 0 || member.catch_up_capability != 0 {
                            return Err(format!(
                                "build-copy progress member {} carries frozen progress",
                                member.id
                            ));
                        }
                        ConfigurationProgressSource::BuildCopyLsn
                    }
                    proto::ConfigurationProgressSourceProto::ConfigurationProgressUnknown => {
                        return Err("configuration progress source is unknown".to_string());
                    }
                };
            Ok(ConfigurationMemberDescriptor {
                id: member.id,
                instance_id: ReplicaInstanceId::new(member.instance_id),
                role: role.into(),
                status,
                replicator_address: member.replicator_address,
                must_catch_up: member.must_catch_up,
                progress,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    if descriptor.write_quorum == 0 {
        return Err("configuration descriptor write quorum must be positive".to_string());
    }
    Ok(ConfigurationDescriptor {
        members,
        write_quorum: descriptor.write_quorum,
    })
}

impl From<AddReplicaIntent> for proto::AddReplicaIntentProto {
    fn from(intent: AddReplicaIntent) -> Self {
        Self {
            operation_id: intent.operation_id,
            attempt_id: intent.attempt_id,
            mode: match intent.mode {
                AddReplicaMode::ScaleUp => proto::AddReplicaModeProto::AddReplicaModeScaleUp,
                AddReplicaMode::Rebuild => proto::AddReplicaModeProto::AddReplicaModeRebuild,
            } as i32,
            epoch: Some(intent.epoch.into()),
            primary_replica_id: intent.primary_replica_id,
            primary_instance_id: intent.primary_instance_id.to_string(),
            primary_agent_generation: intent.primary_agent_generation.to_string(),
            primary_control_address: intent.primary_control_address,
            target_replica_id: intent.target_replica_id,
            target_instance_id: intent.target_instance_id.to_string(),
            target_agent_generation: intent.target_agent_generation.to_string(),
            target_control_address: intent.target_control_address,
            target_replicator_address: intent.target_replicator_address,
            retired_instance_id: intent.retired_instance_id.map(|value| value.to_string()),
            previous_configuration: Some(configuration_descriptor_proto(
                intent.previous_configuration,
            )),
            catch_up_configuration: Some(configuration_descriptor_proto(
                intent.catch_up_configuration,
            )),
            current_configuration: Some(configuration_descriptor_proto(
                intent.current_configuration,
            )),
            minimum_committed_replicas: intent.minimum_committed_replicas,
            deadline_unix_seconds: intent.deadline_unix_seconds,
            compensation_deadline_unix_seconds: intent.compensation_deadline_unix_seconds,
            target_lifecycle_peer_protocol_version: intent.target_lifecycle_peer_protocol_version,
        }
    }
}

impl TryFrom<proto::AddReplicaIntentProto> for AddReplicaIntent {
    type Error = String;

    fn try_from(intent: proto::AddReplicaIntentProto) -> Result<Self, Self::Error> {
        if intent.operation_id.is_empty() || intent.attempt_id.is_empty() {
            return Err("add-replica intent identity is missing".to_string());
        }
        if intent.primary_replica_id <= 0
            || intent.target_replica_id <= 0
            || intent.primary_replica_id == intent.target_replica_id
        {
            return Err("add-replica primary/target identity is invalid".to_string());
        }
        if intent.primary_instance_id.is_empty()
            || intent.target_instance_id.is_empty()
            || intent.primary_control_address.is_empty()
            || intent.target_control_address.is_empty()
            || intent.target_replicator_address.is_empty()
        {
            return Err("add-replica intent has missing identity or endpoint".to_string());
        }
        let mode = match proto::AddReplicaModeProto::try_from(intent.mode)
            .map_err(|_| format!("unknown add-replica mode {}", intent.mode))?
        {
            proto::AddReplicaModeProto::AddReplicaModeScaleUp => AddReplicaMode::ScaleUp,
            proto::AddReplicaModeProto::AddReplicaModeRebuild => AddReplicaMode::Rebuild,
            proto::AddReplicaModeProto::AddReplicaModeUnknown => {
                return Err("add-replica mode is unknown".to_string());
            }
        };
        let retired_instance_id = intent.retired_instance_id.map(ReplicaInstanceId::new);
        if mode == AddReplicaMode::Rebuild && retired_instance_id.is_none() {
            return Err("rebuild intent has no retired incarnation".to_string());
        }
        if mode == AddReplicaMode::ScaleUp && retired_instance_id.is_some() {
            return Err("scale-up intent unexpectedly has a retired incarnation".to_string());
        }
        if intent.minimum_committed_replicas == 0
            || intent.deadline_unix_seconds <= 0
            || intent.compensation_deadline_unix_seconds < intent.deadline_unix_seconds
        {
            return Err("add-replica intent has invalid limits or deadlines".to_string());
        }
        let intent = Self {
            operation_id: intent.operation_id,
            attempt_id: intent.attempt_id,
            mode,
            epoch: intent
                .epoch
                .ok_or_else(|| "add-replica intent has no epoch".to_string())?
                .into(),
            primary_replica_id: intent.primary_replica_id,
            primary_instance_id: ReplicaInstanceId::new(intent.primary_instance_id),
            primary_agent_generation: AgentGeneration::parse(intent.primary_agent_generation)?,
            primary_control_address: intent.primary_control_address,
            target_replica_id: intent.target_replica_id,
            target_instance_id: ReplicaInstanceId::new(intent.target_instance_id),
            target_agent_generation: AgentGeneration::parse(intent.target_agent_generation)?,
            target_control_address: intent.target_control_address,
            target_replicator_address: intent.target_replicator_address,
            retired_instance_id,
            previous_configuration: try_configuration_descriptor(
                intent
                    .previous_configuration
                    .ok_or_else(|| "missing previous configuration descriptor".to_string())?,
            )?,
            catch_up_configuration: try_configuration_descriptor(
                intent
                    .catch_up_configuration
                    .ok_or_else(|| "missing catch-up configuration descriptor".to_string())?,
            )?,
            current_configuration: try_configuration_descriptor(
                intent
                    .current_configuration
                    .ok_or_else(|| "missing current configuration descriptor".to_string())?,
            )?,
            minimum_committed_replicas: intent.minimum_committed_replicas,
            deadline_unix_seconds: intent.deadline_unix_seconds,
            compensation_deadline_unix_seconds: intent.compensation_deadline_unix_seconds,
            target_lifecycle_peer_protocol_version: intent.target_lifecycle_peer_protocol_version,
        };
        intent.validate()?;
        Ok(intent)
    }
}

impl From<AddReplicaProgress> for proto::AddReplicaProgressProto {
    fn from(progress: AddReplicaProgress) -> Self {
        Self {
                phase: match progress.phase {
                    AddReplicaCoordinatorPhase::Validating => {
                        proto::AddReplicaCoordinatorPhaseProto::AddReplicaPhaseValidating
                    }
                    AddReplicaCoordinatorPhase::RetiringOldConnection => {
                        proto::AddReplicaCoordinatorPhaseProto::AddReplicaPhaseRetiringOldConnection
                    }
                    AddReplicaCoordinatorPhase::PreparingTarget => {
                        proto::AddReplicaCoordinatorPhaseProto::AddReplicaPhasePreparingTarget
                    }
                    AddReplicaCoordinatorPhase::Building => {
                        proto::AddReplicaCoordinatorPhaseProto::AddReplicaPhaseBuilding
                    }
                    AddReplicaCoordinatorPhase::ActivatingTarget => {
                        proto::AddReplicaCoordinatorPhaseProto::AddReplicaPhaseActivatingTarget
                    }
                    AddReplicaCoordinatorPhase::InstallingCatchUpConfiguration => {
                        proto::AddReplicaCoordinatorPhaseProto::AddReplicaPhaseInstallingCatchUpConfiguration
                    }
                    AddReplicaCoordinatorPhase::WaitingForCatchUpQuorum => {
                        proto::AddReplicaCoordinatorPhaseProto::AddReplicaPhaseWaitingForCatchUpQuorum
                    }
                    AddReplicaCoordinatorPhase::InstallingCurrentConfiguration => {
                        proto::AddReplicaCoordinatorPhaseProto::AddReplicaPhaseInstallingCurrentConfiguration
                    }
                    AddReplicaCoordinatorPhase::Attesting => {
                        proto::AddReplicaCoordinatorPhaseProto::AddReplicaPhaseAttesting
                    }
                    AddReplicaCoordinatorPhase::Compensating => {
                        proto::AddReplicaCoordinatorPhaseProto::AddReplicaPhaseCompensating
                    }
                } as i32,
                commit_observed: progress.commit_observed,
                copy_lsn: progress.copy_lsn,
            }
    }
}

impl TryFrom<proto::AddReplicaProgressProto> for AddReplicaProgress {
    type Error = String;

    fn try_from(progress: proto::AddReplicaProgressProto) -> Result<Self, Self::Error> {
        let phase = match proto::AddReplicaCoordinatorPhaseProto::try_from(progress.phase)
                .map_err(|_| format!("unknown add-replica phase {}", progress.phase))?
            {
                proto::AddReplicaCoordinatorPhaseProto::AddReplicaPhaseValidating => {
                    AddReplicaCoordinatorPhase::Validating
                }
                proto::AddReplicaCoordinatorPhaseProto::AddReplicaPhaseRetiringOldConnection => {
                    AddReplicaCoordinatorPhase::RetiringOldConnection
                }
                proto::AddReplicaCoordinatorPhaseProto::AddReplicaPhasePreparingTarget => {
                    AddReplicaCoordinatorPhase::PreparingTarget
                }
                proto::AddReplicaCoordinatorPhaseProto::AddReplicaPhaseBuilding => {
                    AddReplicaCoordinatorPhase::Building
                }
                proto::AddReplicaCoordinatorPhaseProto::AddReplicaPhaseActivatingTarget => {
                    AddReplicaCoordinatorPhase::ActivatingTarget
                }
                proto::AddReplicaCoordinatorPhaseProto::AddReplicaPhaseInstallingCatchUpConfiguration => {
                    AddReplicaCoordinatorPhase::InstallingCatchUpConfiguration
                }
                proto::AddReplicaCoordinatorPhaseProto::AddReplicaPhaseWaitingForCatchUpQuorum => {
                    AddReplicaCoordinatorPhase::WaitingForCatchUpQuorum
                }
                proto::AddReplicaCoordinatorPhaseProto::AddReplicaPhaseInstallingCurrentConfiguration => {
                    AddReplicaCoordinatorPhase::InstallingCurrentConfiguration
                }
                proto::AddReplicaCoordinatorPhaseProto::AddReplicaPhaseAttesting => {
                    AddReplicaCoordinatorPhase::Attesting
                }
                proto::AddReplicaCoordinatorPhaseProto::AddReplicaPhaseCompensating => {
                    AddReplicaCoordinatorPhase::Compensating
                }
                proto::AddReplicaCoordinatorPhaseProto::AddReplicaPhaseUnknown => {
                    return Err("add-replica progress phase is unknown".to_string());
                }
            };
        Ok(Self {
            phase,
            commit_observed: progress.commit_observed,
            copy_lsn: progress.copy_lsn,
        })
    }
}

impl From<RemoveReplicaMode> for proto::RemoveReplicaModeProto {
    fn from(mode: RemoveReplicaMode) -> Self {
        match mode {
            RemoveReplicaMode::ScaleDown => Self::RemoveReplicaModeScaleDown,
            RemoveReplicaMode::Force => Self::RemoveReplicaModeForce,
        }
    }
}

impl TryFrom<proto::RemoveReplicaModeProto> for RemoveReplicaMode {
    type Error = String;

    fn try_from(mode: proto::RemoveReplicaModeProto) -> Result<Self, Self::Error> {
        match mode {
            proto::RemoveReplicaModeProto::RemoveReplicaModeScaleDown => Ok(Self::ScaleDown),
            proto::RemoveReplicaModeProto::RemoveReplicaModeForce => Ok(Self::Force),
            proto::RemoveReplicaModeProto::RemoveReplicaModeUnknown => {
                Err("remove-replica mode is unknown".to_string())
            }
        }
    }
}

impl TryFrom<RemoveReplicaIntent> for proto::RemoveReplicaIntentProto {
    type Error = String;

    fn try_from(intent: RemoveReplicaIntent) -> Result<Self, Self::Error> {
        intent.validate()?;
        Ok(Self {
            protocol_version: intent.protocol_version,
            operation_id: intent.operation_id,
            action_id: intent.action_id,
            attempt_number: intent.attempt_number,
            attempt_id: intent.attempt_id,
            input_signature: intent.input_signature,
            mode: proto::RemoveReplicaModeProto::from(intent.mode) as i32,
            epoch: Some(intent.epoch.into()),
            primary_replica_id: intent.primary_replica_id,
            primary_instance_id: intent.primary_instance_id.to_string(),
            primary_agent_generation: intent.primary_agent_generation.to_string(),
            primary_agent_control_version: Some(intent.primary_agent_control_version.value()),
            primary_control_address: intent.primary_control_address,
            primary_replicator_address: intent.primary_replicator_address,
            target_replica_id: intent.target_replica_id,
            target_instance_id: intent.target_instance_id.to_string(),
            expected_target_pod_uid: intent.expected_target_pod_uid,
            target_pod_name: intent.target_pod_name,
            expected_target_agent_generation: intent
                .expected_target_agent_generation
                .map(|value| value.to_string()),
            target_control_address: intent.target_control_address,
            target_replicator_address: intent.target_replicator_address,
            target_lifecycle_peer_protocol_version: intent.target_lifecycle_peer_protocol_version,
            previous_configuration: Some(configuration_descriptor_proto(
                intent.previous_configuration,
            )),
            reduced_catch_up_configuration: Some(configuration_descriptor_proto(
                intent.reduced_catch_up_configuration,
            )),
            reduced_current_configuration: Some(configuration_descriptor_proto(
                intent.reduced_current_configuration,
            )),
            required_write_quorum: intent.required_write_quorum,
            minimum_committed_replicas: intent.minimum_committed_replicas,
            maximum_pre_commit_attempts: intent.maximum_pre_commit_attempts,
            overall_deadline_unix_seconds: intent.overall_deadline_unix_seconds,
            compensation_grace_seconds: intent.compensation_grace_seconds,
            compensation_deadline_cap_unix_seconds: intent.compensation_deadline_cap_unix_seconds,
            call_timeout_seconds: intent.call_timeout_seconds,
            target_retirement_timeout_seconds: intent.target_retirement_timeout_seconds,
        })
    }
}

impl TryFrom<proto::RemoveReplicaIntentProto> for RemoveReplicaIntent {
    type Error = String;

    fn try_from(intent: proto::RemoveReplicaIntentProto) -> Result<Self, Self::Error> {
        let mode = proto::RemoveReplicaModeProto::try_from(intent.mode)
            .map_err(|_| format!("unknown remove-replica mode {}", intent.mode))?
            .try_into()?;
        let value = Self {
            protocol_version: intent.protocol_version,
            operation_id: intent.operation_id,
            action_id: intent.action_id,
            attempt_number: intent.attempt_number,
            attempt_id: intent.attempt_id,
            input_signature: intent.input_signature,
            mode,
            epoch: intent
                .epoch
                .ok_or_else(|| "remove-replica intent has no epoch".to_string())?
                .into(),
            primary_replica_id: intent.primary_replica_id,
            primary_instance_id: ReplicaInstanceId::new(intent.primary_instance_id),
            primary_agent_generation: AgentGeneration::parse(intent.primary_agent_generation)?,
            primary_agent_control_version: AgentControlVersion::new(
                intent.primary_agent_control_version.ok_or_else(|| {
                    "remove-replica intent has no primary agent control version".to_string()
                })?,
            ),
            primary_control_address: intent.primary_control_address,
            primary_replicator_address: intent.primary_replicator_address,
            target_replica_id: intent.target_replica_id,
            target_instance_id: ReplicaInstanceId::new(intent.target_instance_id),
            expected_target_pod_uid: intent.expected_target_pod_uid,
            target_pod_name: intent.target_pod_name,
            expected_target_agent_generation: intent
                .expected_target_agent_generation
                .map(AgentGeneration::parse)
                .transpose()?,
            target_control_address: intent.target_control_address,
            target_replicator_address: intent.target_replicator_address,
            target_lifecycle_peer_protocol_version: intent.target_lifecycle_peer_protocol_version,
            previous_configuration: try_configuration_descriptor(
                intent
                    .previous_configuration
                    .ok_or_else(|| "missing previous remove configuration".to_string())?,
            )?,
            reduced_catch_up_configuration: try_configuration_descriptor(
                intent
                    .reduced_catch_up_configuration
                    .ok_or_else(|| "missing reduced catch-up remove configuration".to_string())?,
            )?,
            reduced_current_configuration: try_configuration_descriptor(
                intent
                    .reduced_current_configuration
                    .ok_or_else(|| "missing reduced current remove configuration".to_string())?,
            )?,
            required_write_quorum: intent.required_write_quorum,
            minimum_committed_replicas: intent.minimum_committed_replicas,
            maximum_pre_commit_attempts: intent.maximum_pre_commit_attempts,
            overall_deadline_unix_seconds: intent.overall_deadline_unix_seconds,
            compensation_grace_seconds: intent.compensation_grace_seconds,
            compensation_deadline_cap_unix_seconds: intent.compensation_deadline_cap_unix_seconds,
            call_timeout_seconds: intent.call_timeout_seconds,
            target_retirement_timeout_seconds: intent.target_retirement_timeout_seconds,
        };
        value.validate()?;
        Ok(value)
    }
}

impl From<RemoveReplicaCoordinatorPhase> for proto::RemoveReplicaCoordinatorPhaseProto {
    fn from(phase: RemoveReplicaCoordinatorPhase) -> Self {
        match phase {
            RemoveReplicaCoordinatorPhase::Validating => Self::RemoveReplicaPhaseValidating,
            RemoveReplicaCoordinatorPhase::InstallingCatchUpConfiguration => {
                Self::RemoveReplicaPhaseInstallingCatchUpConfiguration
            }
            RemoveReplicaCoordinatorPhase::WaitingForCatchUpQuorum => {
                Self::RemoveReplicaPhaseWaitingForCatchUpQuorum
            }
            RemoveReplicaCoordinatorPhase::InstallingCurrentConfiguration => {
                Self::RemoveReplicaPhaseInstallingCurrentConfiguration
            }
            RemoveReplicaCoordinatorPhase::RemovingConnection => {
                Self::RemoveReplicaPhaseRemovingConnection
            }
            RemoveReplicaCoordinatorPhase::RetiringTarget => Self::RemoveReplicaPhaseRetiringTarget,
            RemoveReplicaCoordinatorPhase::Attesting => Self::RemoveReplicaPhaseAttesting,
            RemoveReplicaCoordinatorPhase::Compensating => Self::RemoveReplicaPhaseCompensating,
        }
    }
}

impl TryFrom<proto::RemoveReplicaCoordinatorPhaseProto> for RemoveReplicaCoordinatorPhase {
    type Error = String;

    fn try_from(phase: proto::RemoveReplicaCoordinatorPhaseProto) -> Result<Self, Self::Error> {
        match phase {
            proto::RemoveReplicaCoordinatorPhaseProto::RemoveReplicaPhaseValidating => {
                Ok(Self::Validating)
            }
            proto::RemoveReplicaCoordinatorPhaseProto::RemoveReplicaPhaseInstallingCatchUpConfiguration => {
                Ok(Self::InstallingCatchUpConfiguration)
            }
            proto::RemoveReplicaCoordinatorPhaseProto::RemoveReplicaPhaseWaitingForCatchUpQuorum => {
                Ok(Self::WaitingForCatchUpQuorum)
            }
            proto::RemoveReplicaCoordinatorPhaseProto::RemoveReplicaPhaseInstallingCurrentConfiguration => {
                Ok(Self::InstallingCurrentConfiguration)
            }
            proto::RemoveReplicaCoordinatorPhaseProto::RemoveReplicaPhaseRemovingConnection => {
                Ok(Self::RemovingConnection)
            }
            proto::RemoveReplicaCoordinatorPhaseProto::RemoveReplicaPhaseRetiringTarget => {
                Ok(Self::RetiringTarget)
            }
            proto::RemoveReplicaCoordinatorPhaseProto::RemoveReplicaPhaseAttesting => {
                Ok(Self::Attesting)
            }
            proto::RemoveReplicaCoordinatorPhaseProto::RemoveReplicaPhaseCompensating => {
                Ok(Self::Compensating)
            }
            proto::RemoveReplicaCoordinatorPhaseProto::RemoveReplicaPhaseUnknown => {
                Err("remove-replica coordinator phase is unknown".to_string())
            }
        }
    }
}

impl From<TargetRetirementObservation> for proto::TargetRetirementObservationProto {
    fn from(observation: TargetRetirementObservation) -> Self {
        match observation {
            TargetRetirementObservation::NotAttempted => {
                Self::TargetRetirementObservationNotAttempted
            }
            TargetRetirementObservation::InProgress => Self::TargetRetirementObservationInProgress,
            TargetRetirementObservation::Completed => Self::TargetRetirementObservationCompleted,
            TargetRetirementObservation::Unavailable => {
                Self::TargetRetirementObservationUnavailable
            }
            TargetRetirementObservation::Stale => Self::TargetRetirementObservationStale,
            TargetRetirementObservation::Failed => Self::TargetRetirementObservationFailed,
        }
    }
}

impl TryFrom<proto::TargetRetirementObservationProto> for TargetRetirementObservation {
    type Error = String;

    fn try_from(observation: proto::TargetRetirementObservationProto) -> Result<Self, Self::Error> {
        match observation {
            proto::TargetRetirementObservationProto::TargetRetirementObservationNotAttempted => {
                Ok(Self::NotAttempted)
            }
            proto::TargetRetirementObservationProto::TargetRetirementObservationInProgress => {
                Ok(Self::InProgress)
            }
            proto::TargetRetirementObservationProto::TargetRetirementObservationCompleted => {
                Ok(Self::Completed)
            }
            proto::TargetRetirementObservationProto::TargetRetirementObservationUnavailable => {
                Ok(Self::Unavailable)
            }
            proto::TargetRetirementObservationProto::TargetRetirementObservationStale => {
                Ok(Self::Stale)
            }
            proto::TargetRetirementObservationProto::TargetRetirementObservationFailed => {
                Ok(Self::Failed)
            }
            proto::TargetRetirementObservationProto::TargetRetirementObservationUnknown => {
                Err("target retirement observation is unknown".to_string())
            }
        }
    }
}

impl TryFrom<RemoveReplicaProgress> for proto::RemoveReplicaProgressProto {
    type Error = String;

    fn try_from(progress: RemoveReplicaProgress) -> Result<Self, Self::Error> {
        progress.validate()?;
        Ok(Self {
            phase: proto::RemoveReplicaCoordinatorPhaseProto::from(progress.phase) as i32,
            attempt_id: progress.attempt_id,
            commit_observed: progress.commit_observed,
            commit_observed_unix_seconds: progress.commit_observed_unix_seconds,
            connection_absent: progress.connection_absent,
            target_retirement: proto::TargetRetirementObservationProto::from(
                progress.target_retirement,
            ) as i32,
            retirement_expiry_unix_seconds: progress.retirement_expiry_unix_seconds,
            compensation_expiry_unix_seconds: progress.compensation_expiry_unix_seconds,
            error: progress.error,
            current_install_dispatched: progress.current_install_dispatched,
        })
    }
}

impl TryFrom<proto::RemoveReplicaProgressProto> for RemoveReplicaProgress {
    type Error = String;

    fn try_from(progress: proto::RemoveReplicaProgressProto) -> Result<Self, Self::Error> {
        let phase = proto::RemoveReplicaCoordinatorPhaseProto::try_from(progress.phase)
            .map_err(|_| format!("unknown remove-replica phase {}", progress.phase))?
            .try_into()?;
        let target_retirement =
            proto::TargetRetirementObservationProto::try_from(progress.target_retirement)
                .map_err(|_| {
                    format!(
                        "unknown target retirement observation {}",
                        progress.target_retirement
                    )
                })?
                .try_into()?;
        let value = Self {
            phase,
            attempt_id: progress.attempt_id,
            commit_observed: progress.commit_observed,
            commit_observed_unix_seconds: progress.commit_observed_unix_seconds,
            connection_absent: progress.connection_absent,
            target_retirement,
            retirement_expiry_unix_seconds: progress.retirement_expiry_unix_seconds,
            compensation_expiry_unix_seconds: progress.compensation_expiry_unix_seconds,
            error: progress.error,
            current_install_dispatched: progress.current_install_dispatched,
        };
        value.validate()?;
        Ok(value)
    }
}

impl From<RemoveReplicaTerminalResult> for proto::RemoveReplicaTerminalResultProto {
    fn from(result: RemoveReplicaTerminalResult) -> Self {
        match result {
            RemoveReplicaTerminalResult::CommittedClean => {
                Self::RemoveReplicaTerminalResultCommittedClean
            }
            RemoveReplicaTerminalResult::CommittedDegraded => {
                Self::RemoveReplicaTerminalResultCommittedDegraded
            }
            RemoveReplicaTerminalResult::Compensated => {
                Self::RemoveReplicaTerminalResultCompensated
            }
            RemoveReplicaTerminalResult::CompensationIncomplete => {
                Self::RemoveReplicaTerminalResultCompensationIncomplete
            }
        }
    }
}

impl TryFrom<proto::RemoveReplicaTerminalResultProto> for RemoveReplicaTerminalResult {
    type Error = String;

    fn try_from(result: proto::RemoveReplicaTerminalResultProto) -> Result<Self, Self::Error> {
        match result {
            proto::RemoveReplicaTerminalResultProto::RemoveReplicaTerminalResultCommittedClean => {
                Ok(Self::CommittedClean)
            }
            proto::RemoveReplicaTerminalResultProto::RemoveReplicaTerminalResultCommittedDegraded => {
                Ok(Self::CommittedDegraded)
            }
            proto::RemoveReplicaTerminalResultProto::RemoveReplicaTerminalResultCompensated => {
                Ok(Self::Compensated)
            }
            proto::RemoveReplicaTerminalResultProto::RemoveReplicaTerminalResultCompensationIncomplete => {
                Ok(Self::CompensationIncomplete)
            }
            proto::RemoveReplicaTerminalResultProto::RemoveReplicaTerminalResultUnknown => {
                Err("remove-replica terminal result is unknown".to_string())
            }
        }
    }
}

impl TryFrom<ReplicaConfigurationStatus> for proto::ReducedCurrentConfigurationProjectionProto {
    type Error = String;

    fn try_from(status: ReplicaConfigurationStatus) -> Result<Self, Self::Error> {
        validate_reduced_current_projection(&status)?;
        Ok(Self {
            mode: match status.mode {
                ReplicaConfigurationMode::CatchUp => {
                    proto::ReplicaConfigurationModeProto::ConfigurationCatchUp as i32
                }
                ReplicaConfigurationMode::Current => {
                    proto::ReplicaConfigurationModeProto::ConfigurationCurrent as i32
                }
            },
            members: status
                .members
                .into_iter()
                .map(|member| proto::ReplicaConfigurationMemberStatusProto {
                    id: member.id,
                    instance_id: member.instance_id.to_string(),
                    role: proto::RoleProto::from(member.role) as i32,
                })
                .collect(),
            write_quorum: status.write_quorum,
        })
    }
}

impl TryFrom<proto::ReducedCurrentConfigurationProjectionProto> for ReplicaConfigurationStatus {
    type Error = String;

    fn try_from(
        projection: proto::ReducedCurrentConfigurationProjectionProto,
    ) -> Result<Self, Self::Error> {
        let mode =
            proto::ReplicaConfigurationModeProto::try_from(projection.mode).map_err(|_| {
                format!(
                    "unknown reduced current projection mode {}",
                    projection.mode
                )
            })?;
        let status = Self {
            mode: match mode {
                proto::ReplicaConfigurationModeProto::ConfigurationCurrent => {
                    ReplicaConfigurationMode::Current
                }
                proto::ReplicaConfigurationModeProto::ConfigurationCatchUp => {
                    ReplicaConfigurationMode::CatchUp
                }
                proto::ReplicaConfigurationModeProto::ConfigurationNone => {
                    return Err("reduced current projection mode is none".to_string());
                }
            },
            members: projection
                .members
                .into_iter()
                .map(|member| {
                    if member.id <= 0 || member.instance_id.is_empty() {
                        return Err(
                            "reduced current projection member identity is invalid".to_string()
                        );
                    }
                    let role = proto::RoleProto::try_from(member.role).map_err(|_| {
                        format!("unknown reduced current projection role {}", member.role)
                    })?;
                    if role == proto::RoleProto::RoleUnknown {
                        return Err("reduced current projection member role is unknown".to_string());
                    }
                    Ok(ReplicaConfigurationMemberStatus {
                        id: member.id,
                        instance_id: ReplicaInstanceId::new(member.instance_id),
                        role: role.into(),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            write_quorum: projection.write_quorum,
        };
        validate_reduced_current_projection(&status)?;
        Ok(status)
    }
}

fn validate_reduced_current_projection(status: &ReplicaConfigurationStatus) -> Result<(), String> {
    if status.mode != ReplicaConfigurationMode::Current {
        return Err("reduced current projection is not Current".to_string());
    }
    if status.write_quorum == 0 || status.write_quorum as usize > status.members.len() + 1 {
        return Err("reduced current projection write quorum is invalid".to_string());
    }
    let mut ids = HashSet::new();
    let mut instances = HashSet::new();
    let mut previous_id = None;
    for member in &status.members {
        if member.id <= 0
            || member.instance_id.as_str().is_empty()
            || member.role == Role::Unknown
            || !ids.insert(member.id)
            || !instances.insert(member.instance_id.clone())
            || previous_id.is_some_and(|id| member.id <= id)
        {
            return Err(
                "reduced current projection members are invalid or not strictly sorted".to_string(),
            );
        }
        previous_id = Some(member.id);
    }
    Ok(())
}

impl From<RuntimeBuildObservation> for proto::RuntimeBuildObservationProto {
    fn from(observation: RuntimeBuildObservation) -> Self {
        Self {
            execution_id: observation.execution_id,
            build_key: observation.build_key,
            target_replica_id: observation.target_replica_id,
            target_instance_id: observation.target_instance_id.to_string(),
            target_agent_generation: observation.target_agent_generation.to_string(),
            state: match observation.state {
                RuntimeBuildState::InProgress => {
                    proto::RuntimeBuildStateProto::RuntimeBuildStateInProgress
                }
                RuntimeBuildState::Completed => {
                    proto::RuntimeBuildStateProto::RuntimeBuildStateCompleted
                }
                RuntimeBuildState::Failed => proto::RuntimeBuildStateProto::RuntimeBuildStateFailed,
                RuntimeBuildState::Cancelled => {
                    proto::RuntimeBuildStateProto::RuntimeBuildStateCancelled
                }
            } as i32,
            copy_lsn: observation.copy_lsn,
            error: observation.error,
        }
    }
}

impl TryFrom<proto::RuntimeBuildObservationProto> for RuntimeBuildObservation {
    type Error = String;

    fn try_from(observation: proto::RuntimeBuildObservationProto) -> Result<Self, Self::Error> {
        if observation.execution_id.is_empty()
            || observation.build_key.is_empty()
            || observation.target_replica_id <= 0
            || observation.target_instance_id.is_empty()
        {
            return Err("runtime build observation identity is invalid".to_string());
        }
        let state = match proto::RuntimeBuildStateProto::try_from(observation.state)
            .map_err(|_| format!("unknown runtime build state {}", observation.state))?
        {
            proto::RuntimeBuildStateProto::RuntimeBuildStateInProgress => {
                RuntimeBuildState::InProgress
            }
            proto::RuntimeBuildStateProto::RuntimeBuildStateCompleted => {
                RuntimeBuildState::Completed
            }
            proto::RuntimeBuildStateProto::RuntimeBuildStateFailed => RuntimeBuildState::Failed,
            proto::RuntimeBuildStateProto::RuntimeBuildStateCancelled => {
                RuntimeBuildState::Cancelled
            }
            proto::RuntimeBuildStateProto::RuntimeBuildStateUnknown => {
                return Err("runtime build state is unknown".to_string());
            }
        };
        if state == RuntimeBuildState::Completed && observation.copy_lsn.is_none() {
            return Err("completed runtime build has no copy LSN".to_string());
        }
        if observation
            .error
            .as_ref()
            .is_some_and(|error| error.len() > MAX_LIFECYCLE_ERROR_BYTES)
        {
            return Err("runtime build error exceeds the protocol bound".to_string());
        }
        Ok(Self {
            execution_id: observation.execution_id,
            build_key: observation.build_key,
            target_replica_id: observation.target_replica_id,
            target_instance_id: ReplicaInstanceId::new(observation.target_instance_id),
            target_agent_generation: AgentGeneration::parse(observation.target_agent_generation)?,
            state,
            copy_lsn: observation.copy_lsn,
            error: observation.error,
        })
    }
}

fn peer_stage_proto(stage: PeerStage) -> proto::PeerStageProto {
    match stage {
        PeerStage::Prepare => proto::PeerStageProto::PeerStagePrepare,
        PeerStage::Activate => proto::PeerStageProto::PeerStageActivate,
        PeerStage::Cleanup => proto::PeerStageProto::PeerStageCleanup,
        PeerStage::Retire => proto::PeerStageProto::PeerStageRetire,
    }
}

fn try_peer_stage(value: i32) -> Result<PeerStage, String> {
    match proto::PeerStageProto::try_from(value)
        .map_err(|_| format!("unknown peer stage {value}"))?
    {
        proto::PeerStageProto::PeerStagePrepare => Ok(PeerStage::Prepare),
        proto::PeerStageProto::PeerStageActivate => Ok(PeerStage::Activate),
        proto::PeerStageProto::PeerStageCleanup => Ok(PeerStage::Cleanup),
        proto::PeerStageProto::PeerStageRetire => Ok(PeerStage::Retire),
        proto::PeerStageProto::PeerStageUnknown => Err("peer stage is unknown".to_string()),
    }
}

impl From<PeerOperationKind> for proto::PeerOperationKindProto {
    fn from(kind: PeerOperationKind) -> Self {
        match kind {
            PeerOperationKind::AddBuild => Self::PeerOperationKindAddBuild,
            PeerOperationKind::Remove => Self::PeerOperationKindRemove,
        }
    }
}

impl TryFrom<proto::PeerOperationKindProto> for PeerOperationKind {
    type Error = String;

    fn try_from(kind: proto::PeerOperationKindProto) -> Result<Self, Self::Error> {
        match kind {
            proto::PeerOperationKindProto::PeerOperationKindAddBuild => Ok(Self::AddBuild),
            proto::PeerOperationKindProto::PeerOperationKindRemove => Ok(Self::Remove),
            proto::PeerOperationKindProto::PeerOperationKindUnknown => {
                Err("peer operation kind is unknown".to_string())
            }
        }
    }
}

impl TryFrom<PeerStageRequest> for proto::ExecuteLifecycleStageRequest {
    type Error = String;

    fn try_from(request: PeerStageRequest) -> Result<Self, Self::Error> {
        request.validate()?;
        Ok(Self {
            protocol_version: request.protocol_version,
            operation_kind: proto::PeerOperationKindProto::from(request.operation_kind) as i32,
            stage_semantic_version: request.stage_semantic_version,
            operation_id: request.operation_id,
            attempt_id: request.attempt_id,
            message_id: request.message_id,
            input_signature: request.input_signature,
            stage: peer_stage_proto(request.stage) as i32,
            sender_replica_id: request.sender_replica_id,
            sender_instance_id: request.sender_instance_id.to_string(),
            sender_agent_generation: request.sender_agent_generation.to_string(),
            sender_control_address: request.sender_control_address,
            parent_action_id: request.parent_action_id,
            parent_action_signature: request.parent_action_signature,
            target_replica_id: request.target_replica_id,
            target_instance_id: request.target_instance_id.to_string(),
            expected_target_agent_generation: request.expected_target_agent_generation.to_string(),
            expected_target_peer_control_version: request.expected_target_peer_control_version,
            epoch: Some(request.epoch.into()),
            configuration_fence: request.configuration_fence,
            build_key: request.build_key,
            copy_lsn: request.copy_lsn,
            removal_mode: request
                .removal_mode
                .map(|mode| proto::RemoveReplicaModeProto::from(mode) as i32),
            commit_observed_unix_seconds: request.commit_observed_unix_seconds,
            retirement_expiry_unix_seconds: request.retirement_expiry_unix_seconds,
            reduced_current_projection: request
                .reduced_current_projection
                .map(proto::ReducedCurrentConfigurationProjectionProto::try_from)
                .transpose()?,
        })
    }
}

impl TryFrom<proto::ExecuteLifecycleStageRequest> for PeerStageRequest {
    type Error = String;

    fn try_from(request: proto::ExecuteLifecycleStageRequest) -> Result<Self, Self::Error> {
        if request.operation_id.is_empty()
            || request.attempt_id.is_empty()
            || request.message_id.is_empty()
            || request.input_signature.is_empty()
            || request.parent_action_id.is_empty()
            || request.parent_action_signature.is_empty()
            || request.sender_control_address.is_empty()
            || request.configuration_fence.is_empty()
        {
            return Err("peer stage request has missing required text".to_string());
        }
        if request.sender_replica_id <= 0
            || request.target_replica_id <= 0
            || request.sender_instance_id.is_empty()
            || request.target_instance_id.is_empty()
        {
            return Err("peer stage request identity is invalid".to_string());
        }
        let stage = try_peer_stage(request.stage)?;
        let operation_kind = proto::PeerOperationKindProto::try_from(request.operation_kind)
            .map_err(|_| format!("unknown peer operation kind {}", request.operation_kind))?
            .try_into()?;
        let removal_mode = request
            .removal_mode
            .map(|mode| {
                proto::RemoveReplicaModeProto::try_from(mode)
                    .map_err(|_| format!("unknown remove-replica mode {mode}"))?
                    .try_into()
            })
            .transpose()?;
        let reduced_current_projection = request
            .reduced_current_projection
            .map(ReplicaConfigurationStatus::try_from)
            .transpose()?;
        let request = Self {
            protocol_version: request.protocol_version,
            operation_kind,
            stage_semantic_version: request.stage_semantic_version,
            operation_id: request.operation_id,
            attempt_id: request.attempt_id,
            message_id: request.message_id,
            input_signature: request.input_signature,
            stage,
            sender_replica_id: request.sender_replica_id,
            sender_instance_id: ReplicaInstanceId::new(request.sender_instance_id),
            sender_agent_generation: AgentGeneration::parse(request.sender_agent_generation)?,
            sender_control_address: request.sender_control_address,
            parent_action_id: request.parent_action_id,
            parent_action_signature: request.parent_action_signature,
            target_replica_id: request.target_replica_id,
            target_instance_id: ReplicaInstanceId::new(request.target_instance_id),
            expected_target_agent_generation: AgentGeneration::parse(
                request.expected_target_agent_generation,
            )?,
            expected_target_peer_control_version: request.expected_target_peer_control_version,
            epoch: request
                .epoch
                .ok_or_else(|| "peer stage request has no epoch".to_string())?
                .into(),
            configuration_fence: request.configuration_fence,
            build_key: request.build_key,
            copy_lsn: request.copy_lsn,
            removal_mode,
            commit_observed_unix_seconds: request.commit_observed_unix_seconds,
            retirement_expiry_unix_seconds: request.retirement_expiry_unix_seconds,
            reduced_current_projection,
        };
        request.validate()?;
        Ok(request)
    }
}

impl From<PeerStageObservation> for proto::PeerStageObservationProto {
    fn from(observation: PeerStageObservation) -> Self {
        Self {
            protocol_version: observation.protocol_version,
            operation_kind: proto::PeerOperationKindProto::from(observation.operation_kind) as i32,
            stage_semantic_version: observation.stage_semantic_version,
            message_id: observation.message_id,
            input_signature: observation.input_signature,
            stage: peer_stage_proto(observation.stage) as i32,
            state: match observation.state {
                PeerStageState::Accepted => proto::PeerStageStateProto::PeerStageStateAccepted,
                PeerStageState::InProgress => proto::PeerStageStateProto::PeerStageStateInProgress,
                PeerStageState::Completed => proto::PeerStageStateProto::PeerStageStateCompleted,
                PeerStageState::Failed => proto::PeerStageStateProto::PeerStageStateFailed,
                PeerStageState::Stale => proto::PeerStageStateProto::PeerStageStateStale,
                PeerStageState::Rejected => proto::PeerStageStateProto::PeerStageStateRejected,
                PeerStageState::Conflict => proto::PeerStageStateProto::PeerStageStateConflict,
            } as i32,
            target_agent_generation: observation.target_agent_generation.to_string(),
            target_peer_control_version: observation.target_peer_control_version,
            error: observation.error,
        }
    }
}

impl TryFrom<proto::PeerStageObservationProto> for PeerStageObservation {
    type Error = String;

    fn try_from(observation: proto::PeerStageObservationProto) -> Result<Self, Self::Error> {
        if observation.message_id.is_empty() || observation.input_signature.is_empty() {
            return Err("peer stage observation identity is missing".to_string());
        }
        let state = match proto::PeerStageStateProto::try_from(observation.state)
            .map_err(|_| format!("unknown peer stage state {}", observation.state))?
        {
            proto::PeerStageStateProto::PeerStageStateAccepted => PeerStageState::Accepted,
            proto::PeerStageStateProto::PeerStageStateInProgress => PeerStageState::InProgress,
            proto::PeerStageStateProto::PeerStageStateCompleted => PeerStageState::Completed,
            proto::PeerStageStateProto::PeerStageStateFailed => PeerStageState::Failed,
            proto::PeerStageStateProto::PeerStageStateStale => PeerStageState::Stale,
            proto::PeerStageStateProto::PeerStageStateRejected => PeerStageState::Rejected,
            proto::PeerStageStateProto::PeerStageStateConflict => PeerStageState::Conflict,
            proto::PeerStageStateProto::PeerStageStateUnknown => {
                return Err("peer stage observation state is unknown".to_string());
            }
        };
        if matches!(
            state,
            PeerStageState::Failed
                | PeerStageState::Stale
                | PeerStageState::Rejected
                | PeerStageState::Conflict
        ) != observation.error.is_some()
        {
            return Err("peer stage observation terminal error is malformed".to_string());
        }
        if observation
            .error
            .as_ref()
            .is_some_and(|error| error.len() > MAX_LIFECYCLE_ERROR_BYTES)
        {
            return Err("peer stage error exceeds the protocol bound".to_string());
        }
        Ok(Self {
            protocol_version: observation.protocol_version,
            operation_kind: proto::PeerOperationKindProto::try_from(observation.operation_kind)
                .map_err(|_| format!("unknown peer operation kind {}", observation.operation_kind))?
                .try_into()?,
            stage_semantic_version: observation.stage_semantic_version,
            message_id: observation.message_id,
            input_signature: observation.input_signature,
            stage: try_peer_stage(observation.stage)?,
            state,
            target_agent_generation: AgentGeneration::parse(observation.target_agent_generation)?,
            target_peer_control_version: observation.target_peer_control_version,
            error: observation.error,
        })
    }
}

impl From<PeerLifecycleStatus> for proto::GetLifecycleStatusResponse {
    fn from(status: PeerLifecycleStatus) -> Self {
        Self {
            protocol_version: status.protocol_version,
            target_replica_id: status.target_replica_id,
            target_instance_id: status.target_instance_id.to_string(),
            agent_generation: status.agent_generation.to_string(),
            peer_control_version: status.peer_control_version,
            role: proto::RoleProto::from(status.role) as i32,
            epoch: Some(status.epoch.into()),
            healthy: status.healthy,
            current_progress: status.current_progress,
            current_action: status.current_action.map(Into::into),
            retained_terminal_actions: status
                .retained_terminal_actions
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}

impl TryFrom<proto::GetLifecycleStatusResponse> for PeerLifecycleStatus {
    type Error = String;

    fn try_from(status: proto::GetLifecycleStatusResponse) -> Result<Self, Self::Error> {
        if status.target_replica_id <= 0 || status.target_instance_id.is_empty() {
            return Err("peer status target identity is invalid".to_string());
        }
        let role = proto::RoleProto::try_from(status.role)
            .map_err(|_| format!("unknown peer runtime role {}", status.role))?;
        let current_action = status
            .current_action
            .map(PeerStageObservation::try_from)
            .transpose()?;
        let retained_terminal_actions = status
            .retained_terminal_actions
            .into_iter()
            .map(PeerStageObservation::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            protocol_version: status.protocol_version,
            target_replica_id: status.target_replica_id,
            target_instance_id: ReplicaInstanceId::new(status.target_instance_id),
            agent_generation: AgentGeneration::parse(status.agent_generation)?,
            peer_control_version: status.peer_control_version,
            role: role.into(),
            epoch: status
                .epoch
                .ok_or_else(|| "peer status has no epoch".to_string())?
                .into(),
            healthy: status.healthy,
            current_progress: status.current_progress,
            current_action,
            retained_terminal_actions,
        })
    }
}

/// Serialize a direct non-coarse correlated action for durable operator
/// write-ahead intent. Agent-owned lifecycle workflows use structured CRD
/// intent directly and must not use this projection.
pub fn encode_direct_correlated_action_payload(
    action: &DurableReplicaAction,
) -> Result<String, String> {
    if matches!(
        action,
        DurableReplicaAction::AddReplicaIntent { .. }
            | DurableReplicaAction::RemoveReplicaIntent { .. }
    ) {
        return Err(
            "agent-owned replica lifecycle intent has no direct payload projection".to_string(),
        );
    }
    let encoded = proto::ExecuteCorrelatedControlActionRequest {
        protocol_version: 0,
        action_id: String::new(),
        input_signature: String::new(),
        target_replica_id: 0,
        target_instance_id: String::new(),
        expected_agent_generation: String::new(),
        expected_agent_control_version: None,
        observed_runtime_epoch: None,
        action: Some(correlated_action_proto(action.clone())?),
    }
    .encode_to_vec();
    Ok(encoded.iter().map(|byte| format!("{byte:02x}")).collect())
}

/// Decode a persisted direct non-add correlated action payload.
pub fn decode_direct_correlated_action_payload(
    encoded: &str,
) -> Result<DurableReplicaAction, String> {
    let encoded = encoded.as_bytes();
    if encoded.is_empty()
        || encoded.len() % 2 != 0
        || !encoded
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Err("persisted correlated action payload is not valid hexadecimal".to_string());
    }
    let bytes = encoded
        .chunks_exact(2)
        .map(|pair| {
            let nibble = |byte: u8| {
                if byte.is_ascii_digit() {
                    byte - b'0'
                } else {
                    byte - b'a' + 10
                }
            };
            (nibble(pair[0]) << 4) | nibble(pair[1])
        })
        .collect::<Vec<_>>();
    let request = proto::ExecuteCorrelatedControlActionRequest::decode(bytes.as_slice())
        .map_err(|error| format!("invalid persisted correlated action payload: {error}"))?;
    let action = strict_correlated_action(
        request
            .action
            .ok_or_else(|| "persisted correlated action payload is missing action".to_string())?,
    )?;
    if matches!(
        action,
        DurableReplicaAction::AddReplicaIntent { .. }
            | DurableReplicaAction::RemoveReplicaIntent { .. }
    ) {
        return Err("persisted replica lifecycle payload projection is unsupported".to_string());
    }
    Ok(action)
}

impl TryFrom<proto::ExecuteCorrelatedControlActionRequest> for CorrelatedControlActionRequest {
    type Error = String;

    fn try_from(
        request: proto::ExecuteCorrelatedControlActionRequest,
    ) -> Result<Self, Self::Error> {
        if request.action_id.is_empty() {
            return Err("missing correlated action ID".to_string());
        }
        if request.input_signature.is_empty() {
            return Err("missing correlated action input signature".to_string());
        }
        if request.target_replica_id <= 0 {
            return Err("target replica ID must be positive".to_string());
        }
        if request.target_instance_id.is_empty() {
            return Err("missing target replica incarnation".to_string());
        }
        let expected_agent_generation = AgentGeneration::parse(request.expected_agent_generation)?;
        let expected_control_version = AgentControlVersion::new(
            request
                .expected_agent_control_version
                .ok_or_else(|| "missing expected agent control version".to_string())?,
        );
        let observed_runtime_epoch = request
            .observed_runtime_epoch
            .ok_or_else(|| "missing observed runtime epoch".to_string())?
            .into();
        let action = strict_correlated_action(
            request
                .action
                .ok_or_else(|| "missing correlated control action".to_string())?,
        )?;
        Ok(Self {
            protocol_version: request.protocol_version,
            action_id: request.action_id,
            input_signature: request.input_signature,
            target_replica_id: request.target_replica_id,
            target_instance_id: ReplicaInstanceId::new(request.target_instance_id),
            expected_agent_generation,
            expected_control_version,
            observed_runtime_epoch,
            action,
        })
    }
}

impl TryFrom<CorrelatedControlActionRequest> for proto::ExecuteCorrelatedControlActionRequest {
    type Error = String;

    fn try_from(request: CorrelatedControlActionRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            protocol_version: request.protocol_version,
            action_id: request.action_id,
            input_signature: request.input_signature,
            target_replica_id: request.target_replica_id,
            target_instance_id: request.target_instance_id.to_string(),
            expected_agent_generation: request.expected_agent_generation.to_string(),
            expected_agent_control_version: Some(request.expected_control_version.value()),
            observed_runtime_epoch: Some(request.observed_runtime_epoch.into()),
            action: Some(correlated_action_proto(request.action)?),
        })
    }
}

impl TryFrom<CorrelatedActionObservation> for proto::CorrelatedActionObservationProto {
    type Error = String;

    fn try_from(observation: CorrelatedActionObservation) -> Result<Self, Self::Error> {
        let result = observation
            .action
            .result
            .map(proto::DurableActionResultProto::try_from)
            .transpose()?
            .unwrap_or(proto::DurableActionResultProto::DurableActionResultNone);
        Ok(Self {
            agent_generation: observation.generation.to_string(),
            agent_control_version: observation.control_version.value(),
            action_id: observation.action.action_id,
            input_signature: observation.action.signature,
            state: match observation.action.state {
                DurableActionState::Scheduled => {
                    proto::DurableActionStateProto::DurableActionScheduled as i32
                }
                DurableActionState::InProgress => {
                    proto::DurableActionStateProto::DurableActionInProgress as i32
                }
                DurableActionState::Completed => {
                    proto::DurableActionStateProto::DurableActionCompleted as i32
                }
                DurableActionState::Failed => {
                    proto::DurableActionStateProto::DurableActionFailed as i32
                }
            },
            error: observation.action.error.unwrap_or_default(),
            result: result as i32,
            error_class: observation
                .action
                .error_class
                .map(proto::DurableActionErrorClassProto::from)
                .unwrap_or(proto::DurableActionErrorClassProto::DurableActionErrorNone)
                as i32,
            add_replica_progress: observation.action.add_replica_progress.map(Into::into),
            remove_replica_progress: observation
                .action
                .remove_replica_progress
                .map(proto::RemoveReplicaProgressProto::try_from)
                .transpose()?,
        })
    }
}

impl TryFrom<proto::CorrelatedActionObservationProto> for CorrelatedActionObservation {
    type Error = String;

    fn try_from(observation: proto::CorrelatedActionObservationProto) -> Result<Self, Self::Error> {
        if observation.action_id.is_empty() {
            return Err("missing correlated action observation ID".to_string());
        }
        if observation.input_signature.is_empty() {
            return Err("missing correlated action observation signature".to_string());
        }
        let state = match proto::DurableActionStateProto::try_from(observation.state)
            .map_err(|_| format!("unknown durable action state {}", observation.state))?
        {
            proto::DurableActionStateProto::DurableActionScheduled => DurableActionState::Scheduled,
            proto::DurableActionStateProto::DurableActionInProgress => {
                DurableActionState::InProgress
            }
            proto::DurableActionStateProto::DurableActionCompleted => DurableActionState::Completed,
            proto::DurableActionStateProto::DurableActionFailed => DurableActionState::Failed,
            proto::DurableActionStateProto::DurableActionNone => {
                return Err("correlated action state is none".to_string());
            }
        };
        let result = proto::DurableActionResultProto::try_from(observation.result)
            .map_err(|_| format!("unknown durable action result {}", observation.result))?;
        let error_class = proto::DurableActionErrorClassProto::try_from(observation.error_class)
            .map_err(|_| {
                format!(
                    "unknown durable action error class {}",
                    observation.error_class
                )
            })?;
        let error_class =
            if error_class == proto::DurableActionErrorClassProto::DurableActionErrorNone {
                None
            } else {
                Some(
                    DurableActionErrorClass::try_from(error_class)
                        .map_err(|_| "invalid durable action error class".to_string())?,
                )
            };
        let error = (!observation.error.is_empty()).then_some(observation.error);
        let add_replica_progress = observation
            .add_replica_progress
            .map(AddReplicaProgress::try_from)
            .transpose()?;
        let remove_replica_progress = observation
            .remove_replica_progress
            .map(RemoveReplicaProgress::try_from)
            .transpose()?;
        let result = if result == proto::DurableActionResultProto::DurableActionResultNone {
            None
        } else {
            Some(
                DurableActionResult::try_from(result)
                    .map_err(|_| "invalid durable action result".to_string())?,
            )
        };
        match state {
            DurableActionState::Scheduled | DurableActionState::InProgress
                if error_class.is_some() || error.is_some() || result.is_some() =>
            {
                return Err("non-terminal correlated action carries terminal data".to_string());
            }
            DurableActionState::Completed if error_class.is_some() || error.is_some() => {
                return Err("completed correlated action carries an error".to_string());
            }
            DurableActionState::Failed
                if error_class.is_none() || error.is_none() || result.is_some() =>
            {
                return Err("failed correlated action has malformed terminal data".to_string());
            }
            _ => {}
        }
        match (result, remove_replica_progress.as_ref()) {
            (Some(DurableActionResult::RemoveReplica(result)), Some(progress)) => {
                progress.validate_terminal(result)?;
            }
            (Some(DurableActionResult::RemoveReplica(_)), None) => {
                return Err("remove-replica terminal result has no progress".to_string());
            }
            (Some(_), Some(_)) => {
                return Err("non-removal result carries remove-replica progress".to_string());
            }
            _ => {}
        }
        Ok(Self {
            generation: AgentGeneration::parse(observation.agent_generation)?,
            control_version: AgentControlVersion::new(observation.agent_control_version),
            action: DurableActionObservation {
                action_id: observation.action_id,
                signature: observation.input_signature,
                state,
                error_class,
                error,
                result,
                add_replica_progress,
                remove_replica_progress,
            },
        })
    }
}

impl From<LocalFaultRecord> for proto::LocalFaultRecordProto {
    fn from(record: LocalFaultRecord) -> Self {
        Self {
            sequence: record.sequence,
            fault_type: match record.fault_type {
                FaultType::Transient => proto::LocalFaultTypeProto::LocalFaultTransient as i32,
                FaultType::Permanent => proto::LocalFaultTypeProto::LocalFaultPermanent as i32,
            },
        }
    }
}

impl TryFrom<proto::LocalFaultRecordProto> for LocalFaultRecord {
    type Error = String;

    fn try_from(record: proto::LocalFaultRecordProto) -> Result<Self, Self::Error> {
        let fault_type = match proto::LocalFaultTypeProto::try_from(record.fault_type)
            .map_err(|_| format!("unknown local fault type {}", record.fault_type))?
        {
            proto::LocalFaultTypeProto::LocalFaultTransient => FaultType::Transient,
            proto::LocalFaultTypeProto::LocalFaultPermanent => FaultType::Permanent,
            proto::LocalFaultTypeProto::LocalFaultUnknown => {
                return Err("local fault type is unknown".to_string());
            }
        };
        Ok(Self {
            sequence: record.sequence,
            fault_type,
        })
    }
}

fn strict_correlated_action(
    action: proto::execute_correlated_control_action_request::Action,
) -> Result<DurableReplicaAction, String> {
    use proto::execute_correlated_control_action_request::Action;
    match action {
        Action::AddReplicaIntent(intent) => Ok(DurableReplicaAction::AddReplicaIntent {
            intent: Box::new((*intent).try_into()?),
        }),
        Action::RemoveReplicaIntent(intent) => Ok(DurableReplicaAction::RemoveReplicaIntent {
            intent: Box::new((*intent).try_into()?),
        }),
        Action::RevokeWriteStatus(_) => Ok(DurableReplicaAction::RevokeWriteStatus),
        Action::ChangeRole(request) => {
            let role = proto::RoleProto::try_from(request.role)
                .map_err(|_| format!("unknown role {}", request.role))?;
            if role == proto::RoleProto::RoleUnknown {
                return Err("change-role target role is unknown".to_string());
            }
            Ok(DurableReplicaAction::ChangeRole {
                epoch: request
                    .epoch
                    .ok_or_else(|| "missing change-role target epoch".to_string())?
                    .into(),
                role: role.into(),
            })
        }
        Action::UpdateEpoch(request) => Ok(DurableReplicaAction::UpdateEpoch {
            epoch: request
                .epoch
                .ok_or_else(|| "missing update-epoch target epoch".to_string())?
                .into(),
        }),
        Action::UpdateCatchUpConfiguration(request) => {
            Ok(DurableReplicaAction::UpdateCatchUpConfiguration {
                current: try_replica_set_config(
                    request
                        .current
                        .ok_or_else(|| "missing current catch-up configuration".to_string())?,
                )?,
                previous: try_replica_set_config(
                    request
                        .previous
                        .ok_or_else(|| "missing previous catch-up configuration".to_string())?,
                )?,
            })
        }
        Action::WaitForCatchUpQuorum(request) => {
            let mode = proto::QuorumModeProto::try_from(request.mode)
                .map_err(|_| format!("unknown quorum mode {}", request.mode))?;
            Ok(DurableReplicaAction::WaitForCatchUpQuorum { mode: mode.into() })
        }
        Action::UpdateCurrentConfiguration(request) => {
            Ok(DurableReplicaAction::UpdateCurrentConfiguration {
                current: try_replica_set_config(
                    request
                        .current
                        .ok_or_else(|| "missing current configuration".to_string())?,
                )?,
            })
        }
        Action::Open(request) => {
            let mode = proto::OpenModeProto::try_from(request.mode)
                .map_err(|_| format!("unknown open mode {}", request.mode))?;
            Ok(DurableReplicaAction::Open { mode: mode.into() })
        }
        Action::Close(_) => Ok(DurableReplicaAction::Close),
        Action::BuildReplica(request) => Ok(DurableReplicaAction::BuildReplica {
            replica: try_replica_info(
                request
                    .replica
                    .ok_or_else(|| "missing build replica".to_string())?,
            )?,
        }),
        Action::RemoveReplica(request) => {
            if request.replica_id <= 0 || request.instance_id.is_empty() {
                return Err("remove-replica target identity is invalid".to_string());
            }
            Ok(DurableReplicaAction::RemoveReplica {
                replica_id: request.replica_id,
                instance_id: ReplicaInstanceId::new(request.instance_id),
            })
        }
        Action::OnDataLoss(request) => Ok(DurableReplicaAction::OnDataLoss {
            epoch: request
                .expected_epoch
                .ok_or_else(|| "missing data-loss epoch".to_string())?
                .into(),
        }),
        Action::RecordElectionConfiguration(request) => {
            Ok(DurableReplicaAction::RecordElectionConfiguration {
                configuration: ReplicaElectionConfiguration::try_from(
                    request
                        .configuration
                        .ok_or_else(|| "missing election configuration".to_string())?,
                )?,
            })
        }
    }
}

fn correlated_action_proto(
    action: DurableReplicaAction,
) -> Result<proto::execute_correlated_control_action_request::Action, String> {
    use proto::execute_correlated_control_action_request::Action;
    Ok(match action {
        DurableReplicaAction::AddReplicaIntent { intent } => {
            Action::AddReplicaIntent(Box::new((*intent).into()))
        }
        DurableReplicaAction::RemoveReplicaIntent { intent } => {
            Action::RemoveReplicaIntent(Box::new((*intent).try_into()?))
        }
        DurableReplicaAction::Open { mode } => Action::Open(proto::OpenRequest {
            mode: proto::OpenModeProto::from(mode) as i32,
        }),
        DurableReplicaAction::Close => Action::Close(proto::CloseRequest {}),
        DurableReplicaAction::RevokeWriteStatus => {
            Action::RevokeWriteStatus(proto::RevokeWriteStatusRequest {})
        }
        DurableReplicaAction::ChangeRole { epoch, role } => {
            Action::ChangeRole(proto::ChangeRoleRequest {
                epoch: Some(epoch.into()),
                role: proto::RoleProto::from(role) as i32,
            })
        }
        DurableReplicaAction::UpdateEpoch { epoch } => {
            Action::UpdateEpoch(proto::UpdateEpochRequest {
                epoch: Some(epoch.into()),
            })
        }
        DurableReplicaAction::UpdateCatchUpConfiguration { current, previous } => {
            Action::UpdateCatchUpConfiguration(proto::UpdateCatchUpConfigRequest {
                current: Some(current.into()),
                previous: Some(previous.into()),
            })
        }
        DurableReplicaAction::WaitForCatchUpQuorum { mode } => {
            Action::WaitForCatchUpQuorum(proto::WaitForCatchUpQuorumRequest {
                mode: proto::QuorumModeProto::from(mode) as i32,
            })
        }
        DurableReplicaAction::UpdateCurrentConfiguration { current } => {
            Action::UpdateCurrentConfiguration(proto::UpdateCurrentConfigRequest {
                current: Some(current.into()),
            })
        }
        DurableReplicaAction::BuildReplica { replica } => {
            Action::BuildReplica(proto::BuildReplicaRequest {
                replica: Some(replica.into()),
            })
        }
        DurableReplicaAction::RemoveReplica {
            replica_id,
            instance_id,
        } => Action::RemoveReplica(proto::RemoveReplicaRequest {
            replica_id,
            instance_id: instance_id.to_string(),
        }),
        DurableReplicaAction::OnDataLoss { epoch } => {
            Action::OnDataLoss(proto::DurableOnDataLossRequest {
                expected_epoch: Some(epoch.into()),
            })
        }
        DurableReplicaAction::RecordElectionConfiguration { configuration } => {
            Action::RecordElectionConfiguration(proto::RecordElectionConfigurationRequest {
                configuration: Some(configuration.into()),
            })
        }
    })
}

fn try_replica_set_config(
    configuration: proto::ReplicaSetConfigProto,
) -> Result<ReplicaSetConfig, String> {
    Ok(ReplicaSetConfig {
        members: configuration
            .members
            .into_iter()
            .map(try_replica_info)
            .collect::<Result<Vec<_>, _>>()?,
        write_quorum: configuration.write_quorum,
    })
}

fn try_replica_info(replica: proto::ReplicaInfoProto) -> Result<ReplicaInfo, String> {
    if replica.id <= 0 || replica.instance_id.is_empty() {
        return Err("replica identity is invalid".to_string());
    }
    let role = proto::RoleProto::try_from(replica.role)
        .map_err(|_| format!("unknown replica role {}", replica.role))?;
    if role == proto::RoleProto::RoleUnknown {
        return Err(format!("replica {} has unknown role", replica.id));
    }
    let status = match proto::ReplicaStatusProto::try_from(replica.status)
        .map_err(|_| format!("unknown replica status {}", replica.status))?
    {
        proto::ReplicaStatusProto::StatusUp => ReplicaStatus::Up,
        proto::ReplicaStatusProto::StatusDown => ReplicaStatus::Down,
    };
    Ok(ReplicaInfo {
        id: replica.id,
        instance_id: ReplicaInstanceId::new(replica.instance_id),
        role: role.into(),
        status,
        replicator_address: replica.replicator_address,
        current_progress: replica.current_progress,
        catch_up_capability: replica.catch_up_capability,
        must_catch_up: replica.must_catch_up,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replica_info_round_trip_preserves_incarnation() {
        let original = ReplicaInfo {
            id: 7,
            instance_id: ReplicaInstanceId::new("pod-uid-7"),
            role: Role::ActiveSecondary,
            status: ReplicaStatus::Up,
            replicator_address: "http://127.0.0.1:7007".into(),
            current_progress: 42,
            catch_up_capability: 40,
            must_catch_up: true,
        };

        let round_trip = ReplicaInfo::from(proto::ReplicaInfoProto::from(original.clone()));

        assert_eq!(round_trip.id, original.id);
        assert_eq!(round_trip.instance_id, original.instance_id);
        assert_eq!(round_trip.role, original.role);
        assert_eq!(round_trip.status, original.status);
        assert_eq!(round_trip.replicator_address, original.replicator_address);
        assert_eq!(round_trip.current_progress, original.current_progress);
        assert_eq!(round_trip.catch_up_capability, original.catch_up_capability);
        assert_eq!(round_trip.must_catch_up, original.must_catch_up);
    }

    #[test]
    fn election_evidence_and_data_loss_result_round_trip() {
        let current = ReplicaConfigurationStatus {
            mode: ReplicaConfigurationMode::Current,
            members: vec![ReplicaConfigurationMemberStatus {
                id: 2,
                instance_id: ReplicaInstanceId::new("two"),
                role: Role::ActiveSecondary,
            }],
            write_quorum: 1,
        };
        let configuration = ReplicaElectionConfiguration {
            previous: Some(current.clone()),
            current,
        };
        let round_trip = ReplicaElectionConfiguration::try_from(
            proto::ReplicaElectionConfigurationProto::from(configuration.clone()),
        )
        .unwrap();
        assert_eq!(round_trip, configuration);

        let deactivation = ReplicaDeactivationInfo {
            epoch: Epoch::new(4, 8),
            catch_up_lsn: 17,
        };
        assert_eq!(
            ReplicaDeactivationInfo::try_from(proto::ReplicaDeactivationInfoProto::from(
                deactivation,
            ))
            .unwrap(),
            deactivation
        );

        for result in [
            DurableActionResult::DataLoss(DataLossAction::None),
            DurableActionResult::DataLoss(DataLossAction::StateChanged),
        ] {
            let encoded = proto::DurableActionResultProto::try_from(result).unwrap();
            assert_eq!(DurableActionResult::try_from(encoded), Ok(result));
        }
    }

    #[test]
    fn malformed_election_evidence_is_rejected() {
        let missing_current = proto::ReplicaElectionConfigurationProto::default();
        assert!(ReplicaElectionConfiguration::try_from(missing_current).is_err());

        let missing_epoch = proto::ReplicaDeactivationInfoProto {
            epoch: None,
            catch_up_lsn: 1,
        };
        assert!(ReplicaDeactivationInfo::try_from(missing_epoch).is_err());

        let malformed = proto::ReplicaElectionConfigurationProto {
            current: Some(proto::ReplicaConfigurationStatusProto {
                mode: 999,
                members: Vec::new(),
                write_quorum: 0,
            }),
            previous: None,
        };
        assert!(ReplicaElectionConfiguration::try_from(malformed).is_err());
    }

    fn valid_correlated_request() -> CorrelatedControlActionRequest {
        let action = DurableReplicaAction::RevokeWriteStatus;
        CorrelatedControlActionRequest {
            protocol_version: 1,
            action_id: "operation:1".to_string(),
            input_signature: action.signature(),
            target_replica_id: 1,
            target_instance_id: ReplicaInstanceId::new("pod-uid"),
            expected_agent_generation: AgentGeneration::parse("0123456789abcdef0123456789abcdef")
                .unwrap(),
            expected_control_version: AgentControlVersion::new(7),
            observed_runtime_epoch: Epoch::new(2, 3),
            action,
        }
    }

    #[test]
    fn correlated_request_and_observation_round_trip_strictly() {
        let request = valid_correlated_request();
        let round_trip = CorrelatedControlActionRequest::try_from(
            proto::ExecuteCorrelatedControlActionRequest::try_from(request.clone()).unwrap(),
        )
        .unwrap();
        assert_eq!(round_trip.protocol_version, request.protocol_version);
        assert_eq!(round_trip.action_id, request.action_id);
        assert_eq!(round_trip.input_signature, request.input_signature);
        assert_eq!(round_trip.target_replica_id, request.target_replica_id);
        assert_eq!(round_trip.target_instance_id, request.target_instance_id);
        assert_eq!(
            round_trip.expected_agent_generation,
            request.expected_agent_generation
        );
        assert_eq!(
            round_trip.expected_control_version,
            request.expected_control_version
        );
        assert_eq!(
            round_trip.observed_runtime_epoch,
            request.observed_runtime_epoch
        );
        assert_eq!(round_trip.action.signature(), request.action.signature());

        let observation = CorrelatedActionObservation {
            generation: request.expected_agent_generation,
            control_version: AgentControlVersion::new(8),
            action: DurableActionObservation {
                action_id: request.action_id,
                signature: request.input_signature,
                state: DurableActionState::Completed,
                error_class: None,
                error: None,
                result: Some(DurableActionResult::DataLoss(DataLossAction::StateChanged)),
                add_replica_progress: None,
                remove_replica_progress: None,
            },
        };
        let round_trip = CorrelatedActionObservation::try_from(
            proto::CorrelatedActionObservationProto::try_from(observation.clone()).unwrap(),
        )
        .unwrap();
        assert_eq!(round_trip, observation);

        let failed = CorrelatedActionObservation {
            generation: AgentGeneration::parse("0123456789abcdef0123456789abcdef").unwrap(),
            control_version: AgentControlVersion::new(9),
            action: DurableActionObservation {
                action_id: "operation:failed".to_string(),
                signature: "wait-for-catch-up:Write".to_string(),
                state: DurableActionState::Failed,
                error_class: Some(DurableActionErrorClass::NoWriteQuorum),
                error: Some("no write quorum".to_string()),
                result: None,
                add_replica_progress: None,
                remove_replica_progress: None,
            },
        };
        let failed_round_trip = CorrelatedActionObservation::try_from(
            proto::CorrelatedActionObservationProto::try_from(failed.clone()).unwrap(),
        )
        .unwrap();
        assert_eq!(failed_round_trip, failed);
    }

    #[test]
    fn persisted_correlated_action_payload_round_trips_exact_progress() {
        assert!(decode_direct_correlated_action_payload("éé").is_err());
        assert!(decode_direct_correlated_action_payload("0g").is_err());

        let action = DurableReplicaAction::UpdateCurrentConfiguration {
            current: ReplicaSetConfig {
                members: vec![ReplicaInfo {
                    id: 2,
                    instance_id: ReplicaInstanceId::new("secondary"),
                    role: Role::ActiveSecondary,
                    status: ReplicaStatus::Up,
                    replicator_address: "http://secondary".to_string(),
                    current_progress: 42,
                    catch_up_capability: 17,
                    must_catch_up: true,
                }],
                write_quorum: 2,
            },
        };
        let signature = action.signature();
        let decoded = decode_direct_correlated_action_payload(
            &encode_direct_correlated_action_payload(&action).unwrap(),
        )
        .unwrap();

        assert_eq!(decoded.signature(), signature);
        let DurableReplicaAction::UpdateCurrentConfiguration { current } = decoded else {
            panic!("wrong decoded action");
        };
        assert_eq!(current.members[0].current_progress, 42);
        assert_eq!(current.members[0].catch_up_capability, 17);
    }

    #[test]
    fn malformed_correlated_safety_fields_are_rejected() {
        let encoded =
            proto::ExecuteCorrelatedControlActionRequest::try_from(valid_correlated_request())
                .unwrap();

        let mut missing_version = encoded.clone();
        missing_version.expected_agent_control_version = None;
        assert!(
            CorrelatedControlActionRequest::try_from(missing_version)
                .unwrap_err()
                .contains("control version")
        );

        let mut missing_epoch = encoded.clone();
        missing_epoch.observed_runtime_epoch = None;
        assert!(
            CorrelatedControlActionRequest::try_from(missing_epoch)
                .unwrap_err()
                .contains("runtime epoch")
        );

        let mut malformed_generation = encoded.clone();
        malformed_generation.expected_agent_generation = "not-a-generation".to_string();
        assert!(
            CorrelatedControlActionRequest::try_from(malformed_generation)
                .unwrap_err()
                .contains("lowercase hexadecimal")
        );

        let mut missing_action = encoded;
        missing_action.action = None;
        assert!(
            CorrelatedControlActionRequest::try_from(missing_action)
                .unwrap_err()
                .contains("missing correlated control action")
        );
    }

    #[test]
    fn add_replica_and_peer_protocol_round_trip_strictly() {
        let generation = AgentGeneration::parse("0123456789abcdef0123456789abcdef").unwrap();
        let target_generation = AgentGeneration::parse("11111111111111111111111111111111").unwrap();
        let descriptor = ConfigurationDescriptor {
            members: vec![ConfigurationMemberDescriptor {
                id: 2,
                instance_id: ReplicaInstanceId::new("target"),
                role: Role::ActiveSecondary,
                status: ReplicaStatus::Up,
                replicator_address: "http://target-data".to_string(),
                must_catch_up: true,
                progress: ConfigurationProgressSource::BuildCopyLsn,
            }],
            write_quorum: 2,
        };
        let intent = AddReplicaIntent {
            operation_id: "operation".to_string(),
            attempt_id: "attempt-1".to_string(),
            mode: AddReplicaMode::ScaleUp,
            epoch: Epoch::new(1, 2),
            primary_replica_id: 1,
            primary_instance_id: ReplicaInstanceId::new("primary"),
            primary_agent_generation: generation.clone(),
            primary_control_address: "http://primary".to_string(),
            target_replica_id: 2,
            target_instance_id: ReplicaInstanceId::new("target"),
            target_agent_generation: target_generation.clone(),
            target_control_address: "http://target".to_string(),
            target_replicator_address: "http://target-data".to_string(),
            retired_instance_id: None,
            previous_configuration: ConfigurationDescriptor {
                members: Vec::new(),
                write_quorum: 1,
            },
            catch_up_configuration: descriptor.clone(),
            current_configuration: ConfigurationDescriptor {
                members: descriptor
                    .members
                    .iter()
                    .cloned()
                    .map(|mut member| {
                        member.must_catch_up = false;
                        member
                    })
                    .collect(),
                write_quorum: 2,
            },
            minimum_committed_replicas: 1,
            deadline_unix_seconds: 100,
            compensation_deadline_unix_seconds: 110,
            target_lifecycle_peer_protocol_version:
                crate::replica_lifecycle::REPLICA_LIFECYCLE_PEER_PROTOCOL_VERSION,
        };
        let action = DurableReplicaAction::AddReplicaIntent {
            intent: Box::new(intent.clone()),
        };
        let request = CorrelatedControlActionRequest {
            protocol_version: crate::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION,
            action_id: "add-action".to_string(),
            input_signature: action.signature(),
            target_replica_id: 1,
            target_instance_id: ReplicaInstanceId::new("primary"),
            expected_agent_generation: generation.clone(),
            expected_control_version: AgentControlVersion::new(0),
            observed_runtime_epoch: Epoch::new(1, 2),
            action: action.clone(),
        };
        let decoded = CorrelatedControlActionRequest::try_from(
            proto::ExecuteCorrelatedControlActionRequest::try_from(request).unwrap(),
        )
        .unwrap();
        assert_eq!(decoded.action.signature(), action.signature());

        let mut peer = PeerStageRequest {
            protocol_version: crate::replica_lifecycle::REPLICA_LIFECYCLE_PEER_PROTOCOL_VERSION,
            operation_kind: PeerOperationKind::AddBuild,
            stage_semantic_version: crate::replica_lifecycle::PEER_STAGE_SEMANTIC_VERSION,
            operation_id: intent.operation_id,
            attempt_id: intent.attempt_id,
            message_id: "prepare".to_string(),
            input_signature: String::new(),
            stage: PeerStage::Prepare,
            sender_replica_id: 1,
            sender_instance_id: ReplicaInstanceId::new("primary"),
            sender_agent_generation: generation,
            sender_control_address: "http://primary".to_string(),
            parent_action_id: "parent".to_string(),
            parent_action_signature: action.signature(),
            target_replica_id: 2,
            target_instance_id: ReplicaInstanceId::new("target"),
            expected_target_agent_generation: target_generation,
            expected_target_peer_control_version: 0,
            epoch: Epoch::new(1, 2),
            configuration_fence: "fence".to_string(),
            build_key: None,
            copy_lsn: None,
            removal_mode: None,
            commit_observed_unix_seconds: None,
            retirement_expiry_unix_seconds: None,
            reduced_current_projection: None,
        };
        peer.input_signature = peer.signature();
        let round_trip = PeerStageRequest::try_from(
            proto::ExecuteLifecycleStageRequest::try_from(peer.clone()).unwrap(),
        )
        .unwrap();
        assert_eq!(round_trip, peer);
    }

    fn remove_member(
        id: i64,
        instance_id: &str,
        status: ReplicaStatus,
    ) -> ConfigurationMemberDescriptor {
        ConfigurationMemberDescriptor {
            id,
            instance_id: ReplicaInstanceId::new(instance_id),
            role: Role::ActiveSecondary,
            status,
            replicator_address: format!("http://replica-{id}:7001"),
            must_catch_up: false,
            progress: ConfigurationProgressSource::Frozen {
                current_progress: id * 10,
                catch_up_capability: id * 10,
            },
        }
    }

    fn remove_intent() -> RemoveReplicaIntent {
        let reduced = ConfigurationDescriptor {
            members: vec![remove_member(3, "retained", ReplicaStatus::Up)],
            write_quorum: 2,
        };
        let mut intent = RemoveReplicaIntent {
            protocol_version: crate::remove_replica::REMOVE_REPLICA_INTENT_PROTOCOL_VERSION,
            operation_id: "remove-operation".to_string(),
            action_id: "remove-action".to_string(),
            attempt_number: 1,
            attempt_id: "attempt-1".to_string(),
            input_signature: String::new(),
            mode: RemoveReplicaMode::ScaleDown,
            epoch: Epoch::new(4, 9),
            primary_replica_id: 1,
            primary_instance_id: ReplicaInstanceId::new("primary"),
            primary_agent_generation: AgentGeneration::parse("0123456789abcdef0123456789abcdef")
                .unwrap(),
            primary_agent_control_version: AgentControlVersion::new(7),
            primary_control_address: "http://primary:7000".to_string(),
            primary_replicator_address: "http://primary:7001".to_string(),
            target_replica_id: 2,
            target_instance_id: ReplicaInstanceId::new("target"),
            expected_target_pod_uid: "target".to_string(),
            target_pod_name: "set-2".to_string(),
            expected_target_agent_generation: Some(
                AgentGeneration::parse("11111111111111111111111111111111").unwrap(),
            ),
            target_control_address: Some("http://target:7000".to_string()),
            target_replicator_address: Some("http://target:7001".to_string()),
            target_lifecycle_peer_protocol_version: Some(
                crate::replica_lifecycle::REPLICA_LIFECYCLE_PEER_PROTOCOL_VERSION,
            ),
            previous_configuration: ConfigurationDescriptor {
                members: vec![
                    remove_member(2, "target", ReplicaStatus::Up),
                    remove_member(3, "retained", ReplicaStatus::Up),
                ],
                write_quorum: 2,
            },
            reduced_catch_up_configuration: reduced.clone(),
            reduced_current_configuration: reduced,
            required_write_quorum: 2,
            minimum_committed_replicas: 2,
            maximum_pre_commit_attempts:
                crate::remove_replica::MAX_REMOVE_REPLICA_PRE_COMMIT_ATTEMPTS,
            overall_deadline_unix_seconds: 1_600,
            compensation_grace_seconds:
                crate::remove_replica::REMOVE_REPLICA_COMPENSATION_GRACE_SECONDS,
            compensation_deadline_cap_unix_seconds: 1_630,
            call_timeout_seconds: crate::remove_replica::REMOVE_REPLICA_CALL_TIMEOUT_SECONDS,
            target_retirement_timeout_seconds:
                crate::remove_replica::REMOVE_REPLICA_RETIREMENT_TIMEOUT_SECONDS,
        };
        intent.input_signature = intent.signature();
        intent
    }

    #[test]
    fn remove_replica_standalone_types_round_trip_strictly() {
        for mode in [RemoveReplicaMode::ScaleDown, RemoveReplicaMode::Force] {
            let mut intent = remove_intent();
            intent.mode = mode;
            if mode == RemoveReplicaMode::Force {
                intent.expected_target_agent_generation = None;
                intent.target_control_address = None;
                intent.target_replicator_address = None;
                intent.target_lifecycle_peer_protocol_version = None;
                intent.previous_configuration.members[0].status = ReplicaStatus::Down;
            }
            intent.input_signature = intent.signature();
            let encoded = proto::RemoveReplicaIntentProto::try_from(intent.clone()).unwrap();
            assert_eq!(RemoveReplicaIntent::try_from(encoded).unwrap(), intent);
        }

        for phase in [
            RemoveReplicaCoordinatorPhase::Validating,
            RemoveReplicaCoordinatorPhase::InstallingCatchUpConfiguration,
            RemoveReplicaCoordinatorPhase::WaitingForCatchUpQuorum,
            RemoveReplicaCoordinatorPhase::InstallingCurrentConfiguration,
            RemoveReplicaCoordinatorPhase::RemovingConnection,
            RemoveReplicaCoordinatorPhase::RetiringTarget,
            RemoveReplicaCoordinatorPhase::Attesting,
            RemoveReplicaCoordinatorPhase::Compensating,
        ] {
            let committed = matches!(
                phase,
                RemoveReplicaCoordinatorPhase::RemovingConnection
                    | RemoveReplicaCoordinatorPhase::RetiringTarget
                    | RemoveReplicaCoordinatorPhase::Attesting
            );
            let progress = RemoveReplicaProgress {
                phase,
                attempt_id: "attempt-1".to_string(),
                commit_observed: committed,
                commit_observed_unix_seconds: committed.then_some(100),
                connection_absent: matches!(
                    phase,
                    RemoveReplicaCoordinatorPhase::RetiringTarget
                        | RemoveReplicaCoordinatorPhase::Attesting
                ),
                target_retirement: match phase {
                    RemoveReplicaCoordinatorPhase::RetiringTarget => {
                        TargetRetirementObservation::InProgress
                    }
                    RemoveReplicaCoordinatorPhase::Attesting => {
                        TargetRetirementObservation::Completed
                    }
                    _ => TargetRetirementObservation::NotAttempted,
                },
                retirement_expiry_unix_seconds: committed.then_some(160),
                compensation_expiry_unix_seconds: (phase
                    == RemoveReplicaCoordinatorPhase::Compensating)
                    .then_some(130),
                error: None,
                current_install_dispatched: matches!(
                    phase,
                    RemoveReplicaCoordinatorPhase::InstallingCurrentConfiguration
                        | RemoveReplicaCoordinatorPhase::RemovingConnection
                        | RemoveReplicaCoordinatorPhase::RetiringTarget
                        | RemoveReplicaCoordinatorPhase::Attesting
                ),
            };
            let encoded = proto::RemoveReplicaProgressProto::try_from(progress.clone()).unwrap();
            assert_eq!(RemoveReplicaProgress::try_from(encoded).unwrap(), progress);
        }

        for retirement in [
            TargetRetirementObservation::NotAttempted,
            TargetRetirementObservation::InProgress,
            TargetRetirementObservation::Completed,
            TargetRetirementObservation::Unavailable,
            TargetRetirementObservation::Stale,
            TargetRetirementObservation::Failed,
        ] {
            let encoded = proto::TargetRetirementObservationProto::from(retirement);
            assert_eq!(
                TargetRetirementObservation::try_from(encoded).unwrap(),
                retirement
            );
        }

        for result in [
            RemoveReplicaTerminalResult::CommittedClean,
            RemoveReplicaTerminalResult::CommittedDegraded,
            RemoveReplicaTerminalResult::Compensated,
            RemoveReplicaTerminalResult::CompensationIncomplete,
        ] {
            let encoded = proto::RemoveReplicaTerminalResultProto::from(result);
            assert_eq!(
                RemoveReplicaTerminalResult::try_from(encoded).unwrap(),
                result
            );
        }
    }

    #[test]
    fn malformed_remove_replica_standalone_wire_values_are_rejected() {
        let encoded = proto::RemoveReplicaIntentProto::try_from(remove_intent()).unwrap();
        let mut cases = Vec::new();

        let mut unsupported_version = encoded.clone();
        unsupported_version.protocol_version = 2;
        cases.push(unsupported_version);

        let mut unknown_mode = encoded.clone();
        unknown_mode.mode = proto::RemoveReplicaModeProto::RemoveReplicaModeUnknown as i32;
        cases.push(unknown_mode);

        let mut missing_epoch = encoded.clone();
        missing_epoch.epoch = None;
        cases.push(missing_epoch);

        let mut invalid_primary_generation = encoded.clone();
        invalid_primary_generation.primary_agent_generation = "invalid".to_string();
        cases.push(invalid_primary_generation);

        let mut invalid_target_generation = encoded.clone();
        invalid_target_generation.expected_target_agent_generation = Some("invalid".to_string());
        cases.push(invalid_target_generation);

        let mut invalid_endpoint = encoded.clone();
        invalid_endpoint.target_control_address = Some("not a URI".to_string());
        cases.push(invalid_endpoint);

        let mut mismatched_uid = encoded.clone();
        mismatched_uid.expected_target_pod_uid = "replacement".to_string();
        cases.push(mismatched_uid);

        let mut missing_descriptor = encoded.clone();
        missing_descriptor.reduced_current_configuration = None;
        cases.push(missing_descriptor);

        let mut invalid_deadline = encoded;
        invalid_deadline.compensation_deadline_cap_unix_seconds += 1;
        cases.push(invalid_deadline);

        for malformed in cases {
            assert!(RemoveReplicaIntent::try_from(malformed).is_err());
        }

        let invalid_progress = proto::RemoveReplicaProgressProto {
            phase: proto::RemoveReplicaCoordinatorPhaseProto::RemoveReplicaPhaseValidating as i32,
            attempt_id: "attempt-1".to_string(),
            commit_observed: false,
            commit_observed_unix_seconds: Some(100),
            connection_absent: false,
            target_retirement:
                proto::TargetRetirementObservationProto::TargetRetirementObservationNotAttempted
                    as i32,
            retirement_expiry_unix_seconds: None,
            compensation_expiry_unix_seconds: None,
            error: None,
            current_install_dispatched: false,
        };
        assert!(RemoveReplicaProgress::try_from(invalid_progress).is_err());

        let unknown_progress = proto::RemoveReplicaProgressProto {
            phase: 999,
            ..Default::default()
        };
        assert!(RemoveReplicaProgress::try_from(unknown_progress).is_err());
        assert!(
            RemoveReplicaTerminalResult::try_from(
                proto::RemoveReplicaTerminalResultProto::RemoveReplicaTerminalResultUnknown
            )
            .is_err()
        );
    }

    #[test]
    fn reduced_current_projection_is_strict_and_sorted() {
        let projection = remove_intent().reduced_current_status();
        let encoded =
            proto::ReducedCurrentConfigurationProjectionProto::try_from(projection.clone())
                .unwrap();
        assert_eq!(
            ReplicaConfigurationStatus::try_from(encoded).unwrap(),
            projection
        );

        let mut unsorted = projection.clone();
        unsorted.members.insert(
            0,
            ReplicaConfigurationMemberStatus {
                id: 4,
                instance_id: ReplicaInstanceId::new("four"),
                role: Role::ActiveSecondary,
            },
        );
        assert!(proto::ReducedCurrentConfigurationProjectionProto::try_from(unsorted).is_err());

        let mut wrong_mode = projection;
        wrong_mode.mode = ReplicaConfigurationMode::CatchUp;
        assert!(proto::ReducedCurrentConfigurationProjectionProto::try_from(wrong_mode).is_err());
    }

    #[test]
    fn lifecycle_peer_v2_retire_wire_rejects_malformed_combinations() {
        let intent = remove_intent();
        let mut request = PeerStageRequest {
            protocol_version: crate::replica_lifecycle::REPLICA_LIFECYCLE_PEER_PROTOCOL_VERSION,
            operation_kind: PeerOperationKind::Remove,
            stage_semantic_version: crate::replica_lifecycle::RETIRE_STAGE_SEMANTIC_VERSION,
            operation_id: intent.operation_id.clone(),
            attempt_id: intent.attempt_id.clone(),
            message_id: "retire".to_string(),
            input_signature: String::new(),
            stage: PeerStage::Retire,
            sender_replica_id: intent.primary_replica_id,
            sender_instance_id: intent.primary_instance_id.clone(),
            sender_agent_generation: intent.primary_agent_generation.clone(),
            sender_control_address: intent.primary_control_address.clone(),
            parent_action_id: intent.action_id.clone(),
            parent_action_signature: intent.input_signature.clone(),
            target_replica_id: intent.target_replica_id,
            target_instance_id: intent.target_instance_id.clone(),
            expected_target_agent_generation: intent
                .expected_target_agent_generation
                .clone()
                .unwrap(),
            expected_target_peer_control_version: 0,
            epoch: intent.epoch,
            configuration_fence: intent.configuration_fence(),
            build_key: None,
            copy_lsn: None,
            removal_mode: Some(intent.mode),
            commit_observed_unix_seconds: Some(100),
            retirement_expiry_unix_seconds: Some(160),
            reduced_current_projection: Some(intent.reduced_current_status()),
        };
        request.input_signature = request.signature();
        let encoded = proto::ExecuteLifecycleStageRequest::try_from(request.clone()).unwrap();
        assert_eq!(PeerStageRequest::try_from(encoded).unwrap(), request);

        let mut missing_projection = request.clone();
        missing_projection.reduced_current_projection = None;
        missing_projection.input_signature = missing_projection.signature();
        assert!(
            proto::ExecuteLifecycleStageRequest::try_from(missing_projection)
                .unwrap_err()
                .contains("projection")
        );

        let mut non_current = request.clone();
        non_current
            .reduced_current_projection
            .as_mut()
            .unwrap()
            .mode = ReplicaConfigurationMode::CatchUp;
        non_current.input_signature = non_current.signature();
        assert!(
            proto::ExecuteLifecycleStageRequest::try_from(non_current)
                .unwrap_err()
                .contains("not Current")
        );

        let mut mismatched_fence = request;
        mismatched_fence.configuration_fence =
            "previous=x;reduced-catch-up=q2[];reduced-current=q2[]".to_string();
        mismatched_fence.parent_action_signature =
            format!("parent:{}:", mismatched_fence.configuration_fence);
        mismatched_fence.input_signature = mismatched_fence.signature();
        assert!(
            proto::ExecuteLifecycleStageRequest::try_from(mismatched_fence)
                .unwrap_err()
                .contains("membership")
        );
    }

    #[test]
    fn lifecycle_peer_v2_preserves_typed_rejected_and_conflict_outcomes() {
        for state in [PeerStageState::Rejected, PeerStageState::Conflict] {
            let observation = PeerStageObservation {
                protocol_version: crate::replica_lifecycle::REPLICA_LIFECYCLE_PEER_PROTOCOL_VERSION,
                operation_kind: PeerOperationKind::Remove,
                stage_semantic_version: crate::replica_lifecycle::RETIRE_STAGE_SEMANTIC_VERSION,
                message_id: "retire".to_string(),
                input_signature: "signature".to_string(),
                stage: PeerStage::Retire,
                state,
                target_agent_generation: AgentGeneration::parse("11111111111111111111111111111111")
                    .unwrap(),
                target_peer_control_version: 7,
                error: Some("typed outcome".to_string()),
            };
            let encoded = proto::PeerStageObservationProto::from(observation.clone());
            assert_eq!(
                PeerStageObservation::try_from(encoded).unwrap(),
                observation
            );
        }
    }

    #[test]
    fn active_v3_wire_round_trips_remove_replica_state() {
        let intent = remove_intent();
        let action = DurableReplicaAction::RemoveReplicaIntent {
            intent: Box::new(intent.clone()),
        };
        let request = CorrelatedControlActionRequest {
            protocol_version: crate::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION,
            action_id: intent.action_id.clone(),
            input_signature: action.signature(),
            target_replica_id: intent.primary_replica_id,
            target_instance_id: intent.primary_instance_id.clone(),
            expected_agent_generation: intent.primary_agent_generation.clone(),
            expected_control_version: intent.primary_agent_control_version,
            observed_runtime_epoch: intent.epoch,
            action: action.clone(),
        };
        let encoded = proto::ExecuteCorrelatedControlActionRequest::try_from(request).unwrap();
        let decoded = CorrelatedControlActionRequest::try_from(encoded).unwrap();
        assert_eq!(decoded.action.signature(), action.signature());
        assert!(
            encode_direct_correlated_action_payload(&action)
                .unwrap_err()
                .contains("no direct")
        );

        assert_eq!(
            proto::DurableActionResultProto::try_from(DurableActionResult::RemoveReplica(
                RemoveReplicaTerminalResult::CommittedClean,
            ))
            .unwrap(),
            proto::DurableActionResultProto::DurableActionResultRemoveReplicaCommittedClean
        );
        let observation = CorrelatedActionObservation {
            generation: intent.primary_agent_generation,
            control_version: AgentControlVersion::new(8),
            action: DurableActionObservation {
                action_id: intent.action_id,
                signature: intent.input_signature,
                state: DurableActionState::InProgress,
                error_class: None,
                error: None,
                result: None,
                add_replica_progress: None,
                remove_replica_progress: Some(RemoveReplicaProgress {
                    phase: RemoveReplicaCoordinatorPhase::Validating,
                    attempt_id: intent.attempt_id,
                    commit_observed: false,
                    commit_observed_unix_seconds: None,
                    connection_absent: false,
                    target_retirement: TargetRetirementObservation::NotAttempted,
                    retirement_expiry_unix_seconds: None,
                    compensation_expiry_unix_seconds: None,
                    error: None,
                    current_install_dispatched: false,
                }),
            },
        };
        let encoded = proto::CorrelatedActionObservationProto::try_from(observation).unwrap();
        assert!(encoded.remove_replica_progress.is_some());
    }
}
