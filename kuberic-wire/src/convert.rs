//! Strict validation and conversion between protobuf and canonical protocol types.

use std::collections::BTreeSet;

use kuberic_protocol::command::{
    EnsureConfiguration, EnsureReplicaBuild, InitializeAgentStore, ProtocolCommand,
};
use kuberic_protocol::observation::{
    AgentBuildReport, AgentObservation, AgentReport, UninitializedAgentObservation,
};
use kuberic_protocol::types::{
    AccessStatus, AgentGeneration, BuildAuthority, BuildAuthorityKind, ConfigurationDescriptor,
    ConfigurationId, ConfigurationMember, EffectivePolicy, Epoch, InitializationId, OperationId,
    PodUid, ProcessSessionId, ProvisioningIntent, PvcUid, ReplicaId, ReplicaIdentity,
    ReplicaInstanceId, ReplicaRole, ResourceUid, TransitionKind, derive_agent_generation,
};
use kuberic_protocol::validation::{validate_configuration, validate_transition_relationship};
use thiserror::Error;

use crate::proto;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WireError {
    #[error("unsupported protocol version {observed}; expected {expected}")]
    UnsupportedProtocolVersion { expected: u32, observed: u32 },
    #[error("missing required field {0}")]
    MissingField(&'static str),
    #[error("invalid enum value {value} for {field}")]
    InvalidEnum { field: &'static str, value: i32 },
    #[error("invalid wire authority: {0}")]
    InvalidAuthority(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationEnvelope {
    pub sender: ReplicaIdentity,
    pub receiver: ReplicaIdentity,
    pub epoch: Epoch,
    pub previous_configuration_id: Option<ConfigurationId>,
    pub current_configuration_id: ConfigurationId,
    pub lsn: i64,
    pub committed_lsn: i64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationAcknowledgement {
    pub sender: ReplicaIdentity,
    pub receiver: ReplicaIdentity,
    pub epoch: Epoch,
    pub previous_configuration_id: Option<ConfigurationId>,
    pub current_configuration_id: ConfigurationId,
    pub received_lsn: i64,
    pub applied_lsn: i64,
    pub committed_lsn: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyEnvelope {
    pub build_id: OperationId,
    pub sender: ReplicaIdentity,
    pub receiver: ReplicaIdentity,
    pub epoch: Epoch,
    pub current_configuration_id: ConfigurationId,
    pub sequence: u64,
    pub lsn: i64,
    pub committed_lsn: i64,
    pub replication_boundary_lsn: i64,
    pub final_item: bool,
    pub snapshot_chunk: bool,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyAcknowledgement {
    pub build_id: OperationId,
    pub sender: ReplicaIdentity,
    pub receiver: ReplicaIdentity,
    pub epoch: Epoch,
    pub current_configuration_id: ConfigurationId,
    pub sequence: u64,
    pub durable_lsn: i64,
    pub replication_boundary_lsn: i64,
    pub final_item: bool,
    pub snapshot_chunk: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteEnvelope {
    pub resource_uid: ResourceUid,
    pub target: ReplicaIdentity,
    pub command: ProtocolCommand,
}

/// Requires an exact protocol-version match; negotiation is intentionally unsupported.
pub fn ensure_supported_version(observed: u32) -> Result<(), WireError> {
    if observed == kuberic_protocol::PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(WireError::UnsupportedProtocolVersion {
            expected: kuberic_protocol::PROTOCOL_VERSION,
            observed,
        })
    }
}

/// Validates an agent report without retaining its canonical representation.
pub fn validate_agent_status_report(report: &proto::AgentStatusReport) -> Result<(), WireError> {
    normalize_agent_status_report(report.clone()).map(|_| ())
}

/// Converts a fully validated wire report into canonical observation evidence.
pub fn normalize_agent_status_report(
    report: proto::AgentStatusReport,
) -> Result<AgentObservation, WireError> {
    ensure_supported_version(report.protocol_version)?;
    if report.resource_uid.is_empty() {
        return Err(WireError::MissingField("agent_status.resource_uid"));
    }
    if report.process_session_id.is_empty() {
        return Err(WireError::MissingField("agent_status.process_session_id"));
    }
    let storage_state = proto::AgentStorageState::try_from(report.storage_state).map_err(|_| {
        WireError::InvalidEnum {
            field: "agent_status.storage_state",
            value: report.storage_state,
        }
    })?;
    match storage_state {
        proto::AgentStorageState::Unknown => Err(WireError::InvalidEnum {
            field: "agent_status.storage_state",
            value: report.storage_state,
        }),
        proto::AgentStorageState::Uninitialized => {
            if report.replica_id <= 0 {
                return Err(WireError::InvalidAuthority(
                    "uninitialized replica ID must be positive".to_string(),
                ));
            }
            if report.pod_uid.is_empty() {
                return Err(WireError::MissingField("agent_status.pod_uid"));
            }
            if report.pvc_uid.is_empty() {
                return Err(WireError::MissingField("agent_status.pvc_uid"));
            }
            if report.identity.is_some() {
                return Err(WireError::InvalidAuthority(
                    "uninitialized agent status must not claim durable identity".to_string(),
                ));
            }
            if report.epoch.is_some()
                || report.previous_configuration.is_some()
                || report.current_configuration.is_some()
                || report.role != proto::ReplicaRole::Unknown as i32
                || report.read_status != proto::AccessStatus::Unknown as i32
                || report.write_status != proto::AccessStatus::Unknown as i32
                || report.current_progress != 0
                || report.verified_replication_lsn.is_some()
                || report.committed_lsn != 0
                || report.catch_up_capability.is_some()
                || report.current_configuration_quorum_progress != 0
                || report.catch_up_boundary.is_some()
                || report.catch_up_complete
                || report.deactivated_lsn.is_some()
                || report.deactivation_epoch.is_some()
                || !report.load_metrics.is_empty()
                || report.reported_fault != proto::FaultType::Unknown as i32
                || !report.pending_operation_id.is_empty()
                || !report.retained_operation_id.is_empty()
                || !report.builds.is_empty()
            {
                return Err(WireError::InvalidAuthority(
                    "uninitialized status contains durable authority".to_string(),
                ));
            }
            Ok(AgentObservation::Uninitialized(
                UninitializedAgentObservation {
                    protocol_version: report.protocol_version,
                    resource_uid: ResourceUid::new(report.resource_uid),
                    replica_id: ReplicaId::new(report.replica_id),
                    pod_uid: PodUid::new(report.pod_uid),
                    pvc_uid: PvcUid::new(report.pvc_uid),
                    process_session_id: ProcessSessionId::new(report.process_session_id),
                    report_sequence: report.report_sequence,
                },
            ))
        }
        proto::AgentStorageState::Initialized => {
            let identity: ReplicaIdentity = report
                .identity
                .clone()
                .ok_or(WireError::MissingField("agent_status.identity"))?
                .try_into()?;
            if report.replica_id != 0 && report.replica_id != identity.replica_id.value() {
                return Err(WireError::InvalidAuthority(
                    "status replica ID differs from durable identity".to_string(),
                ));
            }
            let epoch: Epoch = report
                .epoch
                .ok_or(WireError::MissingField("agent_status.epoch"))?
                .into();
            let role = proto::ReplicaRole::try_from(report.role)
                .map_err(|_| WireError::InvalidEnum {
                    field: "agent_status.role",
                    value: report.role,
                })
                .and_then(role_from_proto)?;
            let write_status = proto::AccessStatus::try_from(report.write_status)
                .map_err(|_| WireError::InvalidEnum {
                    field: "agent_status.write_status",
                    value: report.write_status,
                })
                .and_then(access_status_from_proto)?;
            let read_status = proto::AccessStatus::try_from(report.read_status)
                .map_err(|_| WireError::InvalidEnum {
                    field: "agent_status.read_status",
                    value: report.read_status,
                })
                .and_then(access_status_from_proto)?;
            if report.verified_replication_lsn.is_some_and(|verified| {
                verified < 0
                    || verified > report.current_progress
                    || report.current_configuration.is_none()
            }) {
                return Err(WireError::InvalidAuthority(
                    "verified replication progress is outside reported authority".to_string(),
                ));
            }
            let reported_fault =
                match proto::FaultType::try_from(report.reported_fault).map_err(|_| {
                    WireError::InvalidEnum {
                        field: "agent_status.reported_fault",
                        value: report.reported_fault,
                    }
                })? {
                    proto::FaultType::Unknown => None,
                    proto::FaultType::Transient => {
                        Some(kuberic_protocol::types::FaultType::Transient)
                    }
                    proto::FaultType::Permanent => {
                        Some(kuberic_protocol::types::FaultType::Permanent)
                    }
                };
            let mut load_names = BTreeSet::new();
            let load_metrics = report
                .load_metrics
                .into_iter()
                .map(|metric| {
                    if metric.name.is_empty()
                        || metric.value < 0
                        || !load_names.insert(metric.name.clone())
                    {
                        return Err(WireError::InvalidAuthority(
                            "load metrics require unique nonempty names and nonnegative values"
                                .into(),
                        ));
                    }
                    Ok(kuberic_protocol::types::LoadMetric {
                        name: metric.name,
                        value: metric.value,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut build_ids = BTreeSet::new();
            let builds = report
                .builds
                .into_iter()
                .map(|build| {
                    if build.build_id.is_empty()
                        || build.durable_lsn < 0
                        || !build_ids.insert(build.build_id.clone())
                    {
                        return Err(WireError::InvalidAuthority(
                            "build reports require unique IDs and nonnegative progress".into(),
                        ));
                    }
                    Ok(AgentBuildReport {
                        build_id: OperationId::new(build.build_id),
                        target: build
                            .target
                            .ok_or(WireError::MissingField("build_status.target"))?
                            .try_into()?,
                        last_sequence: build.last_sequence,
                        durable_lsn: build.durable_lsn,
                        completed: build.completed,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            if report.deactivated_lsn.is_some() != report.deactivation_epoch.is_some() {
                return Err(WireError::InvalidAuthority(
                    "deactivation LSN and epoch must be reported together".into(),
                ));
            }
            let previous_configuration = report
                .previous_configuration
                .map(ConfigurationDescriptor::try_from)
                .transpose()?;
            let current_configuration = report
                .current_configuration
                .map(ConfigurationDescriptor::try_from)
                .transpose()?;
            validate_report_configurations(
                epoch,
                previous_configuration.as_ref(),
                current_configuration.as_ref(),
                role,
                read_status,
                write_status,
            )?;
            Ok(AgentObservation::Report(Box::new(AgentReport {
                protocol_version: report.protocol_version,
                resource_uid: ResourceUid::new(report.resource_uid),
                identity,
                process_session_id: ProcessSessionId::new(report.process_session_id),
                report_sequence: report.report_sequence,
                role,
                read_status,
                write_status,
                healthy: report.healthy,
                epoch,
                previous_configuration,
                current_configuration,
                current_progress: report.current_progress,
                verified_replication_lsn: report.verified_replication_lsn,
                committed_lsn: report.committed_lsn,
                catch_up_capability: report.catch_up_capability,
                current_configuration_quorum_progress: report.current_configuration_quorum_progress,
                catch_up_boundary: report.catch_up_boundary,
                catch_up_complete: report.catch_up_complete,
                deactivated_lsn: report.deactivated_lsn,
                deactivation_epoch: report.deactivation_epoch.map(Into::into),
                load_metrics,
                reported_fault,
                pending_operation_id: (!report.pending_operation_id.is_empty())
                    .then(|| OperationId::new(report.pending_operation_id)),
                retained_operation_id: (!report.retained_operation_id.is_empty())
                    .then(|| OperationId::new(report.retained_operation_id)),
                builds,
            })))
        }
        proto::AgentStorageState::Unsafe => {
            if report.storage_error.is_empty() {
                return Err(WireError::MissingField("agent_status.storage_error"));
            }
            if report.identity.is_some()
                || report.epoch.is_some()
                || report.previous_configuration.is_some()
                || report.current_configuration.is_some()
                || report.role != proto::ReplicaRole::Unknown as i32
                || report.read_status != proto::AccessStatus::Unknown as i32
                || report.write_status != proto::AccessStatus::Unknown as i32
                || !report.builds.is_empty()
                || report.deactivation_epoch.is_some()
            {
                return Err(WireError::InvalidAuthority(
                    "unsafe storage report contains untrusted authority".to_string(),
                ));
            }
            Ok(AgentObservation::Invalid {
                message: report.storage_error,
            })
        }
    }
}

/// Validates command fencing, policy, and PC/CC relationships.
pub fn validate_execute_request(request: &proto::ExecuteCommandRequest) -> Result<(), WireError> {
    ensure_supported_version(request.protocol_version)?;
    if request.resource_uid.is_empty() {
        return Err(WireError::MissingField("execute.resource_uid"));
    }

    let command = request
        .command
        .as_ref()
        .ok_or(WireError::MissingField("execute.command"))?;
    match command {
        proto::execute_command_request::Command::InitializeAgentStore(command) => {
            for (field, value) in [
                (
                    "initialize.initialization_id",
                    command.initialization_id.as_str(),
                ),
                ("initialize.resource_uid", command.resource_uid.as_str()),
                (
                    "initialize.expected_instance_id",
                    command.expected_instance_id.as_str(),
                ),
                (
                    "initialize.expected_pod_uid",
                    command.expected_pod_uid.as_str(),
                ),
                (
                    "initialize.expected_pvc_uid",
                    command.expected_pvc_uid.as_str(),
                ),
                (
                    "initialize.assigned_agent_generation",
                    command.assigned_agent_generation.as_str(),
                ),
            ] {
                if value.is_empty() {
                    return Err(WireError::MissingField(field));
                }
            }
            if command.resource_uid != request.resource_uid {
                return Err(WireError::InvalidAuthority(
                    "initialize resource UID differs from request fence".to_string(),
                ));
            }
            if command.local_replica_id <= 0 {
                return Err(WireError::InvalidAuthority(
                    "initialize replica ID must be positive".to_string(),
                ));
            }
            if command.expected_instance_id != command.expected_pod_uid {
                return Err(WireError::InvalidAuthority(
                    "initialize instance ID must equal exact Pod UID".to_string(),
                ));
            }
            if derive_agent_generation(&InitializationId::new(command.initialization_id.as_str()))
                .as_str()
                != command.assigned_agent_generation
            {
                return Err(WireError::InvalidAuthority(
                    "assigned generation does not match initialization identity".to_string(),
                ));
            }
            let policy = command
                .effective_policy
                .as_ref()
                .ok_or(WireError::MissingField("initialize.effective_policy"))?;
            validate_policy(policy)?;
            let bootstrap_configuration =
                command
                    .bootstrap_configuration
                    .clone()
                    .ok_or(WireError::MissingField(
                        "initialize.bootstrap_configuration",
                    ))?;
            let bootstrap_configuration =
                ConfigurationDescriptor::try_from(bootstrap_configuration)?;
            let effective_policy = EffectivePolicy {
                replica_set_size: policy.replica_set_size,
                write_quorum: policy.write_quorum,
                read_quorum: policy.read_quorum,
                failover_delay_seconds: policy.failover_delay_seconds,
            };
            validate_transition_relationship(
                TransitionKind::Bootstrap,
                None,
                &bootstrap_configuration,
                &effective_policy,
            )
            .map_err(|error| WireError::InvalidAuthority(error.to_string()))?;
            let target: ReplicaIdentity = request
                .target
                .clone()
                .ok_or(WireError::MissingField("execute.target"))?
                .try_into()?;
            if let Some(provisioning) = command.provisioning.clone() {
                let provisioning = provisioning_from_proto(provisioning)?;
                let resource_uid = ResourceUid::new(&request.resource_uid);
                if provisioning.target_identity(&resource_uid) != target {
                    return Err(WireError::InvalidAuthority(
                        "initialize target differs from replacement provisioning".to_string(),
                    ));
                }
            } else if !bootstrap_configuration
                .members
                .iter()
                .any(|member| member.identity == target)
            {
                return Err(WireError::InvalidAuthority(
                    "initialize target is not an exact genesis member".to_string(),
                ));
            }
            Ok(())
        }
        proto::execute_command_request::Command::EnsureConfiguration(command) => {
            let target: ReplicaIdentity = request
                .target
                .clone()
                .ok_or(WireError::MissingField("execute.target"))?
                .try_into()?;
            if command.operation_id.is_empty() {
                return Err(WireError::MissingField("ensure_configuration.operation_id"));
            }
            let current = command
                .current_configuration
                .clone()
                .ok_or(WireError::MissingField(
                    "ensure_configuration.current_configuration",
                ))?;
            let current = ConfigurationDescriptor::try_from(current)?;
            let current_epoch: Epoch = command
                .current_epoch
                .ok_or(WireError::MissingField(
                    "ensure_configuration.current_epoch",
                ))?
                .into();
            let policy = command
                .effective_policy
                .as_ref()
                .ok_or(WireError::MissingField(
                    "ensure_configuration.effective_policy",
                ))?;
            validate_policy(policy)?;
            let transition_kind = proto::TransitionKind::try_from(command.transition_kind)
                .map_err(|_| WireError::InvalidEnum {
                    field: "ensure_configuration.transition_kind",
                    value: command.transition_kind,
                })
                .and_then(transition_kind_from_proto)?;
            if command.expected_instance_id.is_empty() {
                return Err(WireError::MissingField(
                    "ensure_configuration.expected_instance_id",
                ));
            }
            if command.expected_agent_generation.is_empty() {
                return Err(WireError::MissingField(
                    "ensure_configuration.expected_agent_generation",
                ));
            }
            if target.replica_id != ReplicaId::new(command.local_replica_id)
                || target.instance_id.as_str() != command.expected_instance_id
                || target.agent_generation.as_str() != command.expected_agent_generation
            {
                return Err(WireError::InvalidAuthority(
                    "ensure target differs from command fence".to_string(),
                ));
            }
            if current.epoch != current_epoch {
                return Err(WireError::InvalidAuthority(
                    "ensure current epoch differs from Current Configuration".to_string(),
                ));
            }
            if current.members.len() as u32 != policy.replica_set_size
                || current.write_quorum != policy.write_quorum
            {
                return Err(WireError::InvalidAuthority(
                    "ensure policy differs from Current Configuration".to_string(),
                ));
            }
            let previous = if let Some(previous) = command.previous_configuration.clone() {
                let previous = ConfigurationDescriptor::try_from(previous)?;
                let previous_epoch: Epoch = command
                    .previous_epoch
                    .ok_or(WireError::MissingField(
                        "ensure_configuration.previous_epoch",
                    ))?
                    .into();
                if previous.epoch != previous_epoch {
                    return Err(WireError::InvalidAuthority(
                        "ensure previous epoch differs from Previous Configuration".to_string(),
                    ));
                }
                Some(previous)
            } else if command.previous_epoch.is_some() {
                return Err(WireError::InvalidAuthority(
                    "ensure previous epoch exists without Previous Configuration".to_string(),
                ));
            } else {
                None
            };
            if !current
                .members
                .iter()
                .any(|member| member.identity == target)
                && !previous.as_ref().is_some_and(|configuration| {
                    configuration
                        .members
                        .iter()
                        .any(|member| member.identity == target)
                })
            {
                return Err(WireError::InvalidAuthority(
                    "ensure target is outside Previous and Current Configuration".to_string(),
                ));
            }
            if command.current_only {
                if previous.is_some() || transition_kind == TransitionKind::Bootstrap {
                    return Err(WireError::InvalidAuthority(
                        "current-only completion must omit PC for a non-bootstrap transition"
                            .to_string(),
                    ));
                }
                if transition_kind == TransitionKind::Replacement
                    && command.retire_build_id.is_empty()
                    && command.retire_build_ids.is_empty()
                {
                    return Err(WireError::InvalidAuthority(
                        "replacement current-only completion must retire its build".to_string(),
                    ));
                }
            } else {
                if !command.retire_build_id.is_empty() || !command.retire_build_ids.is_empty() {
                    return Err(WireError::InvalidAuthority(
                        "build retirement requires current-only completion".to_string(),
                    ));
                }
                validate_transition_relationship(
                    transition_kind,
                    previous.as_ref(),
                    &current,
                    &EffectivePolicy {
                        replica_set_size: policy.replica_set_size,
                        write_quorum: policy.write_quorum,
                        read_quorum: policy.read_quorum,
                        failover_delay_seconds: policy.failover_delay_seconds,
                    },
                )
                .map_err(|error| WireError::InvalidAuthority(error.to_string()))?;
            }
            Ok(())
        }
        proto::execute_command_request::Command::EnsureReplicaBuild(command) => {
            let target: ReplicaIdentity = request
                .target
                .clone()
                .ok_or(WireError::MissingField("execute.target"))?
                .try_into()?;
            if command.operation_id.is_empty()
                || command.expected_instance_id.is_empty()
                || command.expected_agent_generation.is_empty()
            {
                return Err(WireError::MissingField("ensure_build.fence"));
            }
            if target.replica_id != ReplicaId::new(command.local_replica_id)
                || target.instance_id.as_str() != command.expected_instance_id
                || target.agent_generation.as_str() != command.expected_agent_generation
            {
                return Err(WireError::InvalidAuthority(
                    "build command target differs from command fence".to_string(),
                ));
            }
            let build_target: ReplicaIdentity = command
                .target
                .clone()
                .ok_or(WireError::MissingField("ensure_build.target"))?
                .try_into()?;
            if let Some(authority) = command.authority.clone() {
                let authority = build_authority_from_proto(authority)?;
                if authority.build_id.as_str() != command.operation_id
                    || authority.target != build_target
                    || authority.target != target
                    || command.source_session_id.is_empty()
                {
                    return Err(WireError::InvalidAuthority(
                        "target build command differs from durable build authority".to_string(),
                    ));
                }
            } else {
                if build_target == target {
                    return Err(WireError::InvalidAuthority(
                        "source build command must target another exact replica".to_string(),
                    ));
                }
                if !command.source_session_id.is_empty() {
                    return Err(WireError::InvalidAuthority(
                        "source build command cannot carry a peer session".to_string(),
                    ));
                }
            }
            Ok(())
        }
    }
}

pub fn normalize_execute_request(
    request: proto::ExecuteCommandRequest,
) -> Result<ExecuteEnvelope, WireError> {
    validate_execute_request(&request)?;
    let target = request
        .target
        .ok_or(WireError::MissingField("execute.target"))?
        .try_into()?;
    let command = match request
        .command
        .ok_or(WireError::MissingField("execute.command"))?
    {
        proto::execute_command_request::Command::InitializeAgentStore(command) => {
            ProtocolCommand::InitializeAgentStore(Box::new(InitializeAgentStore {
                initialization_id: InitializationId::new(command.initialization_id),
                resource_uid: ResourceUid::new(command.resource_uid),
                local_replica_id: ReplicaId::new(command.local_replica_id),
                expected_instance_id: ReplicaInstanceId::new(command.expected_instance_id),
                expected_pod_uid: PodUid::new(command.expected_pod_uid),
                expected_pvc_uid: PvcUid::new(command.expected_pvc_uid),
                assigned_agent_generation: AgentGeneration::new(command.assigned_agent_generation),
                effective_policy: policy_from_proto(
                    command
                        .effective_policy
                        .ok_or(WireError::MissingField("initialize.effective_policy"))?,
                )?,
                bootstrap_configuration: command
                    .bootstrap_configuration
                    .ok_or(WireError::MissingField(
                        "initialize.bootstrap_configuration",
                    ))?
                    .try_into()?,
                provisioning: command
                    .provisioning
                    .map(provisioning_from_proto)
                    .transpose()?,
            }))
        }
        proto::execute_command_request::Command::EnsureConfiguration(command) => {
            let transition_kind = proto::TransitionKind::try_from(command.transition_kind)
                .map_err(|_| WireError::InvalidEnum {
                    field: "ensure.transition_kind",
                    value: command.transition_kind,
                })
                .and_then(transition_kind_from_proto)?;
            ProtocolCommand::EnsureConfiguration(Box::new(EnsureConfiguration {
                operation_id: OperationId::new(command.operation_id),
                previous_configuration: command
                    .previous_configuration
                    .map(ConfigurationDescriptor::try_from)
                    .transpose()?,
                current_configuration: command
                    .current_configuration
                    .ok_or(WireError::MissingField("ensure.current_configuration"))?
                    .try_into()?,
                previous_epoch: command.previous_epoch.map(Into::into),
                current_epoch: command
                    .current_epoch
                    .ok_or(WireError::MissingField("ensure.current_epoch"))?
                    .into(),
                effective_policy: policy_from_proto(
                    command
                        .effective_policy
                        .ok_or(WireError::MissingField("ensure.effective_policy"))?,
                )?,
                local_replica_id: ReplicaId::new(command.local_replica_id),
                expected_instance_id: ReplicaInstanceId::new(command.expected_instance_id),
                expected_agent_generation: AgentGeneration::new(command.expected_agent_generation),
                transition_kind,
                failover_safe_lsn: command.failover_safe_lsn,
                primary_write_status: if command.primary_write_status
                    == proto::AccessStatus::Unknown as i32
                {
                    if command.grant_write {
                        AccessStatus::Granted
                    } else {
                        AccessStatus::ReconfigurationPending
                    }
                } else {
                    proto::AccessStatus::try_from(command.primary_write_status)
                        .map_err(|_| WireError::InvalidEnum {
                            field: "ensure.primary_write_status",
                            value: command.primary_write_status,
                        })
                        .and_then(access_status_from_proto)?
                },
                current_only: command.current_only,
                retire_build_ids: if command.retire_build_ids.is_empty() {
                    (!command.retire_build_id.is_empty())
                        .then(|| OperationId::new(command.retire_build_id))
                        .into_iter()
                        .collect()
                } else {
                    command
                        .retire_build_ids
                        .into_iter()
                        .map(OperationId::new)
                        .collect()
                },
            }))
        }
        proto::execute_command_request::Command::EnsureReplicaBuild(command) => {
            ProtocolCommand::EnsureReplicaBuild(Box::new(EnsureReplicaBuild {
                operation_id: OperationId::new(command.operation_id),
                local_replica_id: ReplicaId::new(command.local_replica_id),
                expected_instance_id: ReplicaInstanceId::new(command.expected_instance_id),
                expected_agent_generation: AgentGeneration::new(command.expected_agent_generation),
                target: command
                    .target
                    .ok_or(WireError::MissingField("ensure_build.target"))?
                    .try_into()?,
                authority: command
                    .authority
                    .map(build_authority_from_proto)
                    .transpose()?,
                source_session_id: (!command.source_session_id.is_empty())
                    .then(|| ProcessSessionId::new(command.source_session_id)),
            }))
        }
    };
    Ok(ExecuteEnvelope {
        resource_uid: ResourceUid::new(request.resource_uid),
        target,
        command,
    })
}

/// Validates the exact authority carried by one replication item.
pub fn validate_replication_item(item: &proto::ReplicationItem) -> Result<(), WireError> {
    normalize_replication_item(item.clone()).map(|_| ())
}

/// Validates exact sender/receiver authority and monotonic ACK progress.
pub fn validate_replication_ack(ack: &proto::ReplicationAck) -> Result<(), WireError> {
    normalize_replication_ack(ack.clone()).map(|_| ())
}

/// Converts a validated replication item into exact canonical authority.
pub fn normalize_replication_item(
    item: proto::ReplicationItem,
) -> Result<ReplicationEnvelope, WireError> {
    ensure_supported_version(item.protocol_version)?;
    let sender = item
        .sender
        .ok_or(WireError::MissingField("replication_item.sender"))?
        .try_into()?;
    let receiver = item
        .receiver
        .ok_or(WireError::MissingField("replication_item.receiver"))?
        .try_into()?;
    let epoch = item
        .epoch
        .ok_or(WireError::MissingField("replication_item.epoch"))?
        .into();
    if item.current_configuration_id.is_empty() {
        return Err(WireError::MissingField(
            "replication_item.current_configuration_id",
        ));
    }
    if item.lsn <= 0 || item.committed_lsn < 0 || item.committed_lsn > item.lsn {
        return Err(WireError::InvalidAuthority(
            "replication item progress is inconsistent".to_string(),
        ));
    }
    Ok(ReplicationEnvelope {
        sender,
        receiver,
        epoch,
        previous_configuration_id: (!item.previous_configuration_id.is_empty())
            .then(|| ConfigurationId::new(item.previous_configuration_id)),
        current_configuration_id: ConfigurationId::new(item.current_configuration_id),
        lsn: item.lsn,
        committed_lsn: item.committed_lsn,
        data: item.data,
    })
}

/// Converts a validated acknowledgement into exact canonical authority.
pub fn normalize_replication_ack(
    ack: proto::ReplicationAck,
) -> Result<ReplicationAcknowledgement, WireError> {
    ensure_supported_version(ack.protocol_version)?;
    let sender = ack
        .sender
        .ok_or(WireError::MissingField("replication_ack.sender"))?
        .try_into()?;
    let receiver = ack
        .receiver
        .ok_or(WireError::MissingField("replication_ack.receiver"))?
        .try_into()?;
    let epoch = ack
        .epoch
        .ok_or(WireError::MissingField("replication_ack.epoch"))?
        .into();
    if ack.current_configuration_id.is_empty() {
        return Err(WireError::MissingField(
            "replication_ack.current_configuration_id",
        ));
    }
    if ack.received_lsn <= 0
        || ack.applied_lsn < 0
        || ack.received_lsn < ack.applied_lsn
        || ack.committed_lsn < 0
        || ack.committed_lsn > ack.applied_lsn
    {
        return Err(WireError::InvalidAuthority(
            "replication ACK progress is inconsistent".to_string(),
        ));
    }
    Ok(ReplicationAcknowledgement {
        sender,
        receiver,
        epoch,
        previous_configuration_id: (!ack.previous_configuration_id.is_empty())
            .then(|| ConfigurationId::new(ack.previous_configuration_id)),
        current_configuration_id: ConfigurationId::new(ack.current_configuration_id),
        received_lsn: ack.received_lsn,
        applied_lsn: ack.applied_lsn,
        committed_lsn: ack.committed_lsn,
    })
}

pub fn validate_copy_item(item: &proto::CopyItem) -> Result<(), WireError> {
    normalize_copy_item(item.clone()).map(|_| ())
}

pub fn validate_copy_ack(ack: &proto::CopyAck) -> Result<(), WireError> {
    normalize_copy_ack(ack.clone()).map(|_| ())
}

pub fn normalize_copy_item(item: proto::CopyItem) -> Result<CopyEnvelope, WireError> {
    ensure_supported_version(item.protocol_version)?;
    if item.build_id.is_empty() {
        return Err(WireError::MissingField("copy_item.build_id"));
    }
    let sender = item
        .sender
        .ok_or(WireError::MissingField("copy_item.sender"))?
        .try_into()?;
    let receiver = item
        .receiver
        .ok_or(WireError::MissingField("copy_item.receiver"))?
        .try_into()?;
    let epoch = item
        .epoch
        .ok_or(WireError::MissingField("copy_item.epoch"))?
        .into();
    if item.current_configuration_id.is_empty() {
        return Err(WireError::MissingField(
            "copy_item.current_configuration_id",
        ));
    }
    if item.sequence == 0
        || item.replication_boundary_lsn < 0
        || item.committed_lsn < 0
        || if item.final_item {
            item.lsn != item.replication_boundary_lsn
                || item.committed_lsn > item.replication_boundary_lsn
                || item.snapshot_chunk
                || !item.data.is_empty()
        } else if item.snapshot_chunk {
            item.lsn != 0 || item.committed_lsn != 0
        } else {
            item.lsn <= item.replication_boundary_lsn || item.committed_lsn > item.lsn
        }
    {
        return Err(WireError::InvalidAuthority(
            "copy item progress is inconsistent".to_string(),
        ));
    }
    Ok(CopyEnvelope {
        build_id: OperationId::new(item.build_id),
        sender,
        receiver,
        epoch,
        current_configuration_id: ConfigurationId::new(item.current_configuration_id),
        sequence: item.sequence,
        lsn: item.lsn,
        committed_lsn: item.committed_lsn,
        replication_boundary_lsn: item.replication_boundary_lsn,
        final_item: item.final_item,
        snapshot_chunk: item.snapshot_chunk,
        data: item.data,
    })
}

pub fn normalize_copy_ack(ack: proto::CopyAck) -> Result<CopyAcknowledgement, WireError> {
    ensure_supported_version(ack.protocol_version)?;
    if ack.build_id.is_empty() {
        return Err(WireError::MissingField("copy_ack.build_id"));
    }
    let sender = ack
        .sender
        .ok_or(WireError::MissingField("copy_ack.sender"))?
        .try_into()?;
    let receiver = ack
        .receiver
        .ok_or(WireError::MissingField("copy_ack.receiver"))?
        .try_into()?;
    let epoch = ack
        .epoch
        .ok_or(WireError::MissingField("copy_ack.epoch"))?
        .into();
    if ack.current_configuration_id.is_empty() {
        return Err(WireError::MissingField("copy_ack.current_configuration_id"));
    }
    if ack.sequence == 0
        || ack.durable_lsn < 0
        || ack.replication_boundary_lsn < 0
        || (ack.final_item
            && (ack.snapshot_chunk || ack.durable_lsn != ack.replication_boundary_lsn))
        || (ack.snapshot_chunk && (ack.final_item || ack.durable_lsn != 0))
    {
        return Err(WireError::InvalidAuthority(
            "copy acknowledgement progress is inconsistent".to_string(),
        ));
    }
    Ok(CopyAcknowledgement {
        build_id: OperationId::new(ack.build_id),
        sender,
        receiver,
        epoch,
        current_configuration_id: ConfigurationId::new(ack.current_configuration_id),
        sequence: ack.sequence,
        durable_lsn: ack.durable_lsn,
        replication_boundary_lsn: ack.replication_boundary_lsn,
        final_item: ack.final_item,
        snapshot_chunk: ack.snapshot_chunk,
    })
}

impl From<Epoch> for proto::Epoch {
    fn from(value: Epoch) -> Self {
        Self {
            data_loss_number: value.data_loss_number,
            configuration_number: value.configuration_number,
        }
    }
}

impl From<proto::Epoch> for Epoch {
    fn from(value: proto::Epoch) -> Self {
        Self::new(value.data_loss_number, value.configuration_number)
    }
}

impl From<ReplicaIdentity> for proto::ReplicaIdentity {
    fn from(value: ReplicaIdentity) -> Self {
        Self {
            replica_id: value.replica_id.value(),
            instance_id: value.instance_id.to_string(),
            agent_generation: value.agent_generation.to_string(),
        }
    }
}

impl TryFrom<proto::ReplicaIdentity> for ReplicaIdentity {
    type Error = WireError;

    fn try_from(value: proto::ReplicaIdentity) -> Result<Self, Self::Error> {
        if value.replica_id <= 0 {
            return Err(WireError::InvalidAuthority(
                "replica ID must be positive".to_string(),
            ));
        }
        if value.instance_id.is_empty() {
            return Err(WireError::MissingField("replica_identity.instance_id"));
        }
        if value.agent_generation.is_empty() {
            return Err(WireError::MissingField("replica_identity.agent_generation"));
        }
        Ok(Self {
            replica_id: ReplicaId::new(value.replica_id),
            instance_id: ReplicaInstanceId::new(value.instance_id),
            agent_generation: AgentGeneration::new(value.agent_generation),
        })
    }
}

impl From<ConfigurationDescriptor> for proto::Configuration {
    fn from(value: ConfigurationDescriptor) -> Self {
        Self {
            configuration_id: value.configuration_id.to_string(),
            epoch: Some(value.epoch.into()),
            primary_id: value.primary_id.value(),
            members: value
                .members
                .into_iter()
                .map(|member| proto::ConfigurationMember {
                    identity: Some(member.identity.into()),
                    role: role_to_proto(member.role) as i32,
                })
                .collect(),
            write_quorum: value.write_quorum,
        }
    }
}

impl TryFrom<proto::Configuration> for ConfigurationDescriptor {
    type Error = WireError;

    fn try_from(value: proto::Configuration) -> Result<Self, Self::Error> {
        if value.configuration_id.is_empty() {
            return Err(WireError::MissingField("configuration.configuration_id"));
        }

        let epoch = value
            .epoch
            .ok_or(WireError::MissingField("configuration.epoch"))?
            .into();
        let mut members = value
            .members
            .into_iter()
            .map(|member| {
                let identity = member
                    .identity
                    .ok_or(WireError::MissingField("configuration.member.identity"))?
                    .try_into()?;
                let role = proto::ReplicaRole::try_from(member.role).map_err(|_| {
                    WireError::InvalidEnum {
                        field: "configuration.member.role",
                        value: member.role,
                    }
                })?;
                Ok(ConfigurationMember {
                    identity,
                    role: role_from_proto(role)?,
                })
            })
            .collect::<Result<Vec<_>, WireError>>()?;
        members.sort_by_key(|member| member.identity.replica_id);
        let configuration = ConfigurationDescriptor {
            configuration_id: ConfigurationId::new(value.configuration_id),
            epoch,
            primary_id: ReplicaId::new(value.primary_id),
            members,
            write_quorum: value.write_quorum,
        };
        validate_configuration(&configuration, None)
            .map_err(|error| WireError::InvalidAuthority(error.to_string()))?;
        Ok(configuration)
    }
}

impl From<BuildAuthority> for proto::BuildAuthority {
    fn from(authority: BuildAuthority) -> Self {
        Self {
            build_id: authority.build_id.to_string(),
            kind: match authority.kind {
                BuildAuthorityKind::Bootstrap => proto::BuildAuthorityKind::Bootstrap as i32,
                BuildAuthorityKind::Provisioning => proto::BuildAuthorityKind::Provisioning as i32,
                BuildAuthorityKind::Failover => proto::BuildAuthorityKind::Failover as i32,
            },
            source: Some(authority.source.into()),
            target: Some(authority.target.into()),
            current_configuration: Some(authority.current_configuration.into()),
            replication_boundary_lsn: authority.replication_boundary_lsn,
        }
    }
}

impl TryFrom<proto::BuildAuthority> for BuildAuthority {
    type Error = WireError;

    fn try_from(authority: proto::BuildAuthority) -> Result<Self, Self::Error> {
        build_authority_from_proto(authority)
    }
}

fn build_authority_from_proto(
    authority: proto::BuildAuthority,
) -> Result<BuildAuthority, WireError> {
    let kind = match proto::BuildAuthorityKind::try_from(authority.kind).map_err(|_| {
        WireError::InvalidEnum {
            field: "build_authority.kind",
            value: authority.kind,
        }
    })? {
        proto::BuildAuthorityKind::Bootstrap => BuildAuthorityKind::Bootstrap,
        proto::BuildAuthorityKind::Provisioning => BuildAuthorityKind::Provisioning,
        proto::BuildAuthorityKind::Failover => BuildAuthorityKind::Failover,
        proto::BuildAuthorityKind::Unspecified => {
            return Err(WireError::InvalidAuthority(
                "build authority kind is unspecified".to_string(),
            ));
        }
    };
    let authority = BuildAuthority {
        build_id: OperationId::new(authority.build_id),
        kind,
        source: authority
            .source
            .ok_or(WireError::MissingField("build_authority.source"))?
            .try_into()?,
        target: authority
            .target
            .ok_or(WireError::MissingField("build_authority.target"))?
            .try_into()?,
        current_configuration: authority
            .current_configuration
            .ok_or(WireError::MissingField(
                "build_authority.current_configuration",
            ))?
            .try_into()?,
        replication_boundary_lsn: authority.replication_boundary_lsn,
    };
    authority
        .validate()
        .map_err(|error| WireError::InvalidAuthority(error.to_string()))?;
    Ok(authority)
}

fn provisioning_from_proto(
    provisioning: proto::ProvisioningIntent,
) -> Result<ProvisioningIntent, WireError> {
    let intent = ProvisioningIntent {
        replaces: provisioning
            .replaces
            .ok_or(WireError::MissingField("provisioning.replaces"))?
            .try_into()?,
        pod_uid: PodUid::new(provisioning.pod_uid),
        pvc_uid: PvcUid::new(provisioning.pvc_uid),
        operation_id: OperationId::new(provisioning.operation_id),
    };
    if intent.operation_id.is_empty()
        || intent.pod_uid.is_empty()
        || intent.pvc_uid.is_empty()
        || intent.replaces.instance_id.is_empty()
        || intent.replaces.agent_generation.is_empty()
    {
        return Err(WireError::InvalidAuthority(
            "replacement provisioning identifiers must not be empty".to_string(),
        ));
    }
    Ok(intent)
}

fn role_to_proto(role: ReplicaRole) -> proto::ReplicaRole {
    match role {
        ReplicaRole::Primary => proto::ReplicaRole::Primary,
        ReplicaRole::ActiveSecondary => proto::ReplicaRole::ActiveSecondary,
        ReplicaRole::IdleSecondary => proto::ReplicaRole::IdleSecondary,
        ReplicaRole::None => proto::ReplicaRole::None,
    }
}

fn role_from_proto(role: proto::ReplicaRole) -> Result<ReplicaRole, WireError> {
    match role {
        proto::ReplicaRole::Unknown => Err(WireError::InvalidEnum {
            field: "configuration.member.role",
            value: role as i32,
        }),
        proto::ReplicaRole::Primary => Ok(ReplicaRole::Primary),
        proto::ReplicaRole::ActiveSecondary => Ok(ReplicaRole::ActiveSecondary),
        proto::ReplicaRole::IdleSecondary => Ok(ReplicaRole::IdleSecondary),
        proto::ReplicaRole::None => Ok(ReplicaRole::None),
    }
}

fn transition_kind_from_proto(kind: proto::TransitionKind) -> Result<TransitionKind, WireError> {
    match kind {
        proto::TransitionKind::Unknown => Err(WireError::InvalidEnum {
            field: "ensure_configuration.transition_kind",
            value: kind as i32,
        }),
        proto::TransitionKind::Bootstrap => Ok(TransitionKind::Bootstrap),
        proto::TransitionKind::Replacement => Ok(TransitionKind::Replacement),
        proto::TransitionKind::Failover => Ok(TransitionKind::Failover),
    }
}

fn access_status_from_proto(status: proto::AccessStatus) -> Result<AccessStatus, WireError> {
    match status {
        proto::AccessStatus::Unknown => Err(WireError::InvalidEnum {
            field: "agent_status.write_status",
            value: status as i32,
        }),
        proto::AccessStatus::Granted => Ok(AccessStatus::Granted),
        proto::AccessStatus::ReconfigurationPending => Ok(AccessStatus::ReconfigurationPending),
        proto::AccessStatus::NotPrimary => Ok(AccessStatus::NotPrimary),
        proto::AccessStatus::NoWriteQuorum => Ok(AccessStatus::NoWriteQuorum),
    }
}

fn validate_report_configurations(
    epoch: Epoch,
    previous: Option<&ConfigurationDescriptor>,
    current: Option<&ConfigurationDescriptor>,
    role: ReplicaRole,
    read_status: AccessStatus,
    write_status: AccessStatus,
) -> Result<(), WireError> {
    if previous.is_some() && current.is_none() {
        return Err(WireError::InvalidAuthority(
            "report has Previous Configuration without Current Configuration".to_string(),
        ));
    }
    if let Some(current) = current
        && current.epoch != epoch
    {
        return Err(WireError::InvalidAuthority(
            "report epoch differs from Current Configuration".to_string(),
        ));
    }
    if let (Some(previous), Some(current)) = (previous, current) {
        let previous_ids = previous
            .members
            .iter()
            .map(|member| member.identity.replica_id)
            .collect::<BTreeSet<_>>();
        let current_ids = current
            .members
            .iter()
            .map(|member| member.identity.replica_id)
            .collect::<BTreeSet<_>>();
        if previous.epoch.data_loss_number != current.epoch.data_loss_number
            || previous.epoch.configuration_number >= current.epoch.configuration_number
            || previous_ids != current_ids
            || previous.write_quorum != current.write_quorum
        {
            return Err(WireError::InvalidAuthority(
                "report PC/CC relationship is invalid".to_string(),
            ));
        }
    }
    if role == ReplicaRole::Primary && current.is_none() {
        return Err(WireError::InvalidAuthority(
            "Primary report has no Current Configuration".to_string(),
        ));
    }
    if write_status == AccessStatus::Granted && role != ReplicaRole::Primary {
        return Err(WireError::InvalidAuthority(
            "granted WriteStatus requires Primary role".to_string(),
        ));
    }
    if read_status == AccessStatus::Granted
        && !matches!(role, ReplicaRole::Primary | ReplicaRole::ActiveSecondary)
    {
        return Err(WireError::InvalidAuthority(
            "granted ReadStatus requires Primary or Active Secondary role".to_string(),
        ));
    }
    Ok(())
}

fn validate_policy(policy: &proto::EffectivePolicy) -> Result<(), WireError> {
    let expected = EffectivePolicy::fixed(policy.replica_set_size, policy.failover_delay_seconds)
        .ok_or_else(|| {
        WireError::InvalidAuthority("replica-set size must be positive".to_string())
    })?;
    if policy.write_quorum != expected.write_quorum || policy.read_quorum != expected.read_quorum {
        return Err(WireError::InvalidAuthority(
            "effective policy quorum values are not fixed-majority values".to_string(),
        ));
    }
    Ok(())
}

fn policy_from_proto(policy: proto::EffectivePolicy) -> Result<EffectivePolicy, WireError> {
    validate_policy(&policy)?;
    Ok(EffectivePolicy {
        replica_set_size: policy.replica_set_size,
        write_quorum: policy.write_quorum,
        read_quorum: policy.read_quorum,
        failover_delay_seconds: policy.failover_delay_seconds,
    })
}
