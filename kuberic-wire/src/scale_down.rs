use kuberic_protocol::command::{
    AcceptSecondaryRemovalCommit, PrepareSecondaryRemoval, RetireReplica,
};
use kuberic_protocol::types::*;
use kuberic_protocol::validation::*;

use crate::convert::{WireError, access_status_from_proto, policy_from_proto, role_from_proto};
use crate::proto;

fn required<T>(value: Option<T>, name: &'static str) -> Result<T, WireError> {
    value.ok_or(WireError::MissingField(name))
}

fn authority(error: ValidationError) -> WireError {
    WireError::InvalidAuthority(error.to_string())
}

impl From<EffectivePolicy> for proto::EffectivePolicy {
    fn from(value: EffectivePolicy) -> Self {
        Self {
            replica_set_size: value.replica_set_size,
            write_quorum: value.write_quorum,
            read_quorum: value.read_quorum,
            failover_delay_seconds: value.failover_delay_seconds,
        }
    }
}

impl From<CleanupResourceIdentity> for proto::CleanupResourceIdentity {
    fn from(value: CleanupResourceIdentity) -> Self {
        use proto::cleanup_resource_identity::Identity;
        match value {
            CleanupResourceIdentity::Present { name, uid } => Self {
                name,
                identity: Some(Identity::Uid(uid)),
            },
            CleanupResourceIdentity::Absent { name } => Self {
                name,
                identity: Some(Identity::AuthoritativelyAbsent(true)),
            },
        }
    }
}

impl TryFrom<proto::CleanupResourceIdentity> for CleanupResourceIdentity {
    type Error = WireError;
    fn try_from(value: proto::CleanupResourceIdentity) -> Result<Self, Self::Error> {
        use proto::cleanup_resource_identity::Identity;
        if value.name.is_empty() {
            return Err(WireError::MissingField("cleanup.name"));
        }
        match required(value.identity, "cleanup.identity")? {
            Identity::Uid(uid) if !uid.is_empty() => Ok(Self::Present {
                name: value.name,
                uid,
            }),
            Identity::AuthoritativelyAbsent(true) => Ok(Self::Absent { name: value.name }),
            _ => Err(WireError::InvalidAuthority(
                "cleanup requires a UID or positive exact-name absence".into(),
            )),
        }
    }
}

impl From<ReplicaCleanupIdentity> for proto::ReplicaCleanupIdentity {
    fn from(value: ReplicaCleanupIdentity) -> Self {
        Self {
            pod: Some(value.pod.into()),
            pvc: Some(value.pvc.into()),
            endpoint: Some(value.endpoint.into()),
        }
    }
}

impl TryFrom<proto::ReplicaCleanupIdentity> for ReplicaCleanupIdentity {
    type Error = WireError;
    fn try_from(value: proto::ReplicaCleanupIdentity) -> Result<Self, Self::Error> {
        Ok(Self {
            pod: required(value.pod, "cleanup.pod")?.try_into()?,
            pvc: required(value.pvc, "cleanup.pvc")?.try_into()?,
            endpoint: required(value.endpoint, "cleanup.endpoint")?.try_into()?,
        })
    }
}

impl From<SecondaryScaleDownIntent> for proto::SecondaryScaleDownIntent {
    fn from(value: SecondaryScaleDownIntent) -> Self {
        Self {
            operation_id: value.operation_id.to_string(),
            resource_uid: value.resource_uid.to_string(),
            spec_generation: value.spec_generation,
            desired_replicas: value.desired_replicas,
            previous_configuration: Some(value.previous_configuration.into()),
            current_configuration: Some(value.current_configuration.into()),
            previous_policy: Some(value.previous_policy.into()),
            current_policy: Some(value.current_policy.into()),
            primary: Some(value.primary.into()),
            target: Some(value.target.into()),
            cleanup: Some(value.cleanup.into()),
        }
    }
}

impl TryFrom<proto::SecondaryScaleDownIntent> for SecondaryScaleDownIntent {
    type Error = WireError;
    fn try_from(value: proto::SecondaryScaleDownIntent) -> Result<Self, Self::Error> {
        let intent = Self {
            operation_id: OperationId::new(value.operation_id),
            resource_uid: ResourceUid::new(value.resource_uid),
            spec_generation: value.spec_generation,
            desired_replicas: value.desired_replicas,
            previous_configuration: required(
                value.previous_configuration,
                "removal.previous_configuration",
            )?
            .try_into()?,
            current_configuration: required(
                value.current_configuration,
                "removal.current_configuration",
            )?
            .try_into()?,
            previous_policy: policy_from_proto(required(
                value.previous_policy,
                "removal.previous_policy",
            )?)?,
            current_policy: policy_from_proto(required(
                value.current_policy,
                "removal.current_policy",
            )?)?,
            primary: required(value.primary, "removal.primary")?.try_into()?,
            target: required(value.target, "removal.target")?.try_into()?,
            cleanup: required(value.cleanup, "removal.cleanup")?.try_into()?,
        };
        validate_secondary_scale_down(&intent).map_err(authority)?;
        Ok(intent)
    }
}

impl From<SecondaryRemovalPreparation> for proto::SecondaryRemovalPreparation {
    fn from(value: SecondaryRemovalPreparation) -> Self {
        Self {
            intent: Some(value.intent.into()),
            operation_id: value.operation_id.to_string(),
            process_session_id: value.process_session_id.to_string(),
            report_sequence: value.report_sequence,
            boundary_lsn: value.boundary_lsn,
        }
    }
}

impl TryFrom<proto::SecondaryRemovalPreparation> for SecondaryRemovalPreparation {
    type Error = WireError;
    fn try_from(value: proto::SecondaryRemovalPreparation) -> Result<Self, Self::Error> {
        let preparation = Self {
            intent: required(value.intent, "preparation.intent")?.try_into()?,
            operation_id: OperationId::new(value.operation_id),
            process_session_id: ProcessSessionId::new(value.process_session_id),
            report_sequence: value.report_sequence,
            boundary_lsn: value.boundary_lsn,
        };
        validate_secondary_removal_preparation(&preparation).map_err(authority)?;
        Ok(preparation)
    }
}

fn access(value: i32) -> Result<AccessStatus, WireError> {
    access_status_from_proto(proto::AccessStatus::try_from(value).map_err(|_| {
        WireError::InvalidEnum {
            field: "removal.access",
            value,
        }
    })?)
}

fn access_proto(value: AccessStatus) -> i32 {
    (match value {
        AccessStatus::Granted => proto::AccessStatus::Granted,
        AccessStatus::ReconfigurationPending => proto::AccessStatus::ReconfigurationPending,
        AccessStatus::NotPrimary => proto::AccessStatus::NotPrimary,
        AccessStatus::NoWriteQuorum => proto::AccessStatus::NoWriteQuorum,
    }) as i32
}

pub(crate) fn configuration_from_proto(
    value: proto::EnsureConfigurationCommand,
) -> Result<kuberic_protocol::command::EnsureConfiguration, WireError> {
    if value.grant_write
        || !value.retire_build_id.is_empty()
        || !value.retire_build_ids.is_empty()
        || value.switchover_handoff.is_some()
        || !value.retire_switchover_preparation_ids.is_empty()
        || value.scale_up_evidence.is_some()
    {
        return Err(WireError::InvalidAuthority(
            "removal cannot carry write grants or unrelated retirements".into(),
        ));
    }
    let command = kuberic_protocol::command::EnsureConfiguration {
        operation_id: OperationId::new(value.operation_id),
        previous_configuration: value
            .previous_configuration
            .map(TryInto::try_into)
            .transpose()?,
        current_configuration: required(
            value.current_configuration,
            "ensure.current_configuration",
        )?
        .try_into()?,
        previous_epoch: value.previous_epoch.map(Into::into),
        current_epoch: required(value.current_epoch, "ensure.current_epoch")?.into(),
        effective_policy: policy_from_proto(required(
            value.effective_policy,
            "ensure.effective_policy",
        )?)?,
        previous_policy: Some(policy_from_proto(required(
            value.previous_policy,
            "ensure.previous_policy",
        )?)?),
        secondary_removal_evidence: Some(
            required(
                value.secondary_removal_evidence,
                "ensure.secondary_removal_evidence",
            )?
            .try_into()?,
        ),
        scale_up_evidence: None,
        local_replica_id: ReplicaId::new(value.local_replica_id),
        expected_instance_id: ReplicaInstanceId::new(value.expected_instance_id),
        expected_agent_generation: AgentGeneration::new(value.expected_agent_generation),
        transition_kind: TransitionKind::SecondaryScaleDown,
        failover_safe_lsn: value.failover_safe_lsn,
        primary_write_status: access(value.primary_write_status)?,
        current_only: value.current_only,
        retire_build_ids: Vec::new(),
        switchover_handoff: None,
        retire_switchover_preparation_ids: Vec::new(),
    };
    validate_secondary_removal_configuration(&command).map_err(authority)?;
    Ok(command)
}

impl From<SecondaryRemovalWitness> for proto::SecondaryRemovalWitness {
    fn from(value: SecondaryRemovalWitness) -> Self {
        Self {
            resource_uid: value.resource_uid.to_string(),
            identity: Some(value.identity.into()),
            role: crate::convert::role_to_proto(value.role) as i32,
            process_session_id: value.process_session_id.to_string(),
            report_sequence: value.report_sequence,
            epoch: Some(value.epoch.into()),
            previous_configuration_id: value
                .previous_configuration_id
                .map_or_else(String::new, |id| id.to_string()),
            current_configuration_id: value.current_configuration_id.to_string(),
            verified_replication_lsn: value.verified_replication_lsn,
            write_status: access_proto(value.write_status),
            pending_operation_id: value
                .pending_operation_id
                .map_or_else(String::new, |id| id.to_string()),
            retained_operation_id: value
                .retained_operation_id
                .map_or_else(String::new, |id| id.to_string()),
        }
    }
}

impl TryFrom<proto::SecondaryRemovalWitness> for SecondaryRemovalWitness {
    type Error = WireError;
    fn try_from(value: proto::SecondaryRemovalWitness) -> Result<Self, Self::Error> {
        Ok(Self {
            resource_uid: ResourceUid::new(value.resource_uid),
            identity: required(value.identity, "witness.identity")?.try_into()?,
            role: role_from_proto(proto::ReplicaRole::try_from(value.role).map_err(|_| {
                WireError::InvalidEnum {
                    field: "witness.role",
                    value: value.role,
                }
            })?)?,
            process_session_id: ProcessSessionId::new(value.process_session_id),
            report_sequence: value.report_sequence,
            epoch: required(value.epoch, "witness.epoch")?.into(),
            previous_configuration_id: (!value.previous_configuration_id.is_empty())
                .then(|| ConfigurationId::new(value.previous_configuration_id)),
            current_configuration_id: ConfigurationId::new(value.current_configuration_id),
            verified_replication_lsn: value.verified_replication_lsn,
            write_status: access(value.write_status)?,
            pending_operation_id: (!value.pending_operation_id.is_empty())
                .then(|| OperationId::new(value.pending_operation_id)),
            retained_operation_id: (!value.retained_operation_id.is_empty())
                .then(|| OperationId::new(value.retained_operation_id)),
        })
    }
}

impl From<SecondaryRemovalEvidence> for proto::SecondaryRemovalEvidence {
    fn from(value: SecondaryRemovalEvidence) -> Self {
        Self {
            preparation: Some(value.preparation.into()),
            previous_read_quorum: value
                .previous_read_quorum
                .into_iter()
                .map(Into::into)
                .collect(),
            reduced_write_quorum: value
                .reduced_write_quorum
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}

impl TryFrom<proto::SecondaryRemovalEvidence> for SecondaryRemovalEvidence {
    type Error = WireError;
    fn try_from(value: proto::SecondaryRemovalEvidence) -> Result<Self, Self::Error> {
        let evidence = Self {
            preparation: required(value.preparation, "removal_evidence.preparation")?.try_into()?,
            previous_read_quorum: value
                .previous_read_quorum
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            reduced_write_quorum: value
                .reduced_write_quorum
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        };
        validate_secondary_removal_evidence(&evidence, false).map_err(authority)?;
        Ok(evidence)
    }
}

impl From<ReplicaRetirementReport> for proto::ReplicaRetirementReport {
    fn from(value: ReplicaRetirementReport) -> Self {
        Self {
            intent: Some(value.intent.into()),
            operation_id: value.operation_id.to_string(),
            process_session_id: value.process_session_id.to_string(),
            report_sequence: value.report_sequence,
            epoch: Some(value.epoch.into()),
            role: crate::convert::role_to_proto(value.role) as i32,
            read_status: access_proto(value.read_status),
            write_status: access_proto(value.write_status),
            application_closed: value.application_closed,
            peers_fenced: value.peers_fenced,
        }
    }
}

impl TryFrom<proto::ReplicaRetirementReport> for ReplicaRetirementReport {
    type Error = WireError;
    fn try_from(value: proto::ReplicaRetirementReport) -> Result<Self, Self::Error> {
        let report = Self {
            intent: required(value.intent, "retirement.intent")?.try_into()?,
            operation_id: OperationId::new(value.operation_id),
            process_session_id: ProcessSessionId::new(value.process_session_id),
            report_sequence: value.report_sequence,
            epoch: required(value.epoch, "retirement.epoch")?.into(),
            role: role_from_proto(proto::ReplicaRole::try_from(value.role).map_err(|_| {
                WireError::InvalidEnum {
                    field: "retirement.role",
                    value: value.role,
                }
            })?)?,
            read_status: access(value.read_status)?,
            write_status: access(value.write_status)?,
            application_closed: value.application_closed,
            peers_fenced: value.peers_fenced,
        };
        validate_replica_retirement(&report).map_err(authority)?;
        Ok(report)
    }
}

impl From<SecondaryScaleDownCleanup> for proto::SecondaryScaleDownCleanup {
    fn from(value: SecondaryScaleDownCleanup) -> Self {
        Self {
            evidence: Some(value.evidence.into()),
            current_only_write_quorum: value
                .current_only_write_quorum
                .into_iter()
                .map(Into::into)
                .collect(),
            retirement: value.retirement.map(Into::into),
        }
    }
}

impl TryFrom<proto::SecondaryScaleDownCleanup> for SecondaryScaleDownCleanup {
    type Error = WireError;
    fn try_from(value: proto::SecondaryScaleDownCleanup) -> Result<Self, Self::Error> {
        let cleanup = Self {
            evidence: required(value.evidence, "cleanup.evidence")?.try_into()?,
            current_only_write_quorum: value
                .current_only_write_quorum
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            retirement: value.retirement.map(TryInto::try_into).transpose()?,
        };
        validate_secondary_scale_down_cleanup(&cleanup).map_err(authority)?;
        Ok(cleanup)
    }
}

impl From<PrepareSecondaryRemoval> for proto::PrepareSecondaryRemovalCommand {
    fn from(value: PrepareSecondaryRemoval) -> Self {
        Self {
            operation_id: value.operation_id.to_string(),
            local_replica_id: value.local_replica_id.value(),
            expected_instance_id: value.expected_instance_id.to_string(),
            expected_agent_generation: value.expected_agent_generation.to_string(),
            intent: Some(value.intent.into()),
        }
    }
}

impl TryFrom<proto::PrepareSecondaryRemovalCommand> for PrepareSecondaryRemoval {
    type Error = WireError;
    fn try_from(value: proto::PrepareSecondaryRemovalCommand) -> Result<Self, Self::Error> {
        let command = Self {
            operation_id: OperationId::new(value.operation_id),
            local_replica_id: ReplicaId::new(value.local_replica_id),
            expected_instance_id: ReplicaInstanceId::new(value.expected_instance_id),
            expected_agent_generation: AgentGeneration::new(value.expected_agent_generation),
            intent: required(value.intent, "prepare_removal.intent")?.try_into()?,
        };
        let primary = &command.intent.primary;
        if command.operation_id
            != command
                .intent
                .command_operation_id(SecondaryRemovalStage::Prepare, primary)
            || command.local_replica_id != primary.replica_id
            || command.expected_instance_id != primary.instance_id
            || command.expected_agent_generation != primary.agent_generation
        {
            return Err(WireError::InvalidAuthority(
                "preparation command fence differs from exact primary".into(),
            ));
        }
        Ok(command)
    }
}

impl From<RetireReplica> for proto::RetireReplicaCommand {
    fn from(value: RetireReplica) -> Self {
        Self {
            operation_id: value.operation_id.to_string(),
            local_replica_id: value.local_replica_id.value(),
            expected_instance_id: value.expected_instance_id.to_string(),
            expected_agent_generation: value.expected_agent_generation.to_string(),
            committed: Some(value.committed.into()),
        }
    }
}

impl From<AcceptSecondaryRemovalCommit> for proto::AcceptSecondaryRemovalCommitCommand {
    fn from(value: AcceptSecondaryRemovalCommit) -> Self {
        Self {
            operation_id: value.operation_id.to_string(),
            target: Some(value.target.into()),
            committed: Some(value.committed.into()),
            local_recovery: value.local_recovery,
        }
    }
}

impl TryFrom<proto::AcceptSecondaryRemovalCommitCommand> for AcceptSecondaryRemovalCommit {
    type Error = WireError;
    fn try_from(value: proto::AcceptSecondaryRemovalCommitCommand) -> Result<Self, Self::Error> {
        let command = Self {
            operation_id: OperationId::new(value.operation_id),
            target: required(value.target, "accept_removal.target")?.try_into()?,
            committed: required(value.committed, "accept_removal.committed")?.try_into()?,
            local_recovery: value.local_recovery,
        };
        validate_accept_secondary_removal_commit(&command).map_err(authority)?;
        Ok(command)
    }
}

impl TryFrom<proto::RetireReplicaCommand> for RetireReplica {
    type Error = WireError;
    fn try_from(value: proto::RetireReplicaCommand) -> Result<Self, Self::Error> {
        let command = Self {
            operation_id: OperationId::new(value.operation_id),
            local_replica_id: ReplicaId::new(value.local_replica_id),
            expected_instance_id: ReplicaInstanceId::new(value.expected_instance_id),
            expected_agent_generation: AgentGeneration::new(value.expected_agent_generation),
            committed: required(value.committed, "retire.committed")?.try_into()?,
        };
        let intent = &command.committed.evidence.preparation.intent;
        if command.operation_id
            != intent.command_operation_id(SecondaryRemovalStage::Retire, &intent.target)
            || command.local_replica_id != intent.target.replica_id
            || command.expected_instance_id != intent.target.instance_id
            || command.expected_agent_generation != intent.target.agent_generation
        {
            return Err(WireError::InvalidAuthority(
                "retirement command fence differs from excluded target".into(),
            ));
        }
        Ok(command)
    }
}
