use crate::protocol::command::EnsureConfiguration;
use crate::protocol::types::*;
use crate::protocol::validation::{
    ValidationError, validate_scale_up_cleanup, validate_scale_up_configuration,
    validate_scale_up_failover_evidence, validate_scale_up_provisioning_request,
    validate_scale_up_receipt,
};

use crate::control::convert::{
    WireError, access_status_from_proto, policy_from_proto, role_from_proto,
};
use crate::control::proto;

fn required<T>(value: Option<T>, name: &'static str) -> Result<T, WireError> {
    value.ok_or(WireError::MissingField(name))
}

fn authority(error: ValidationError) -> WireError {
    WireError::InvalidAuthority(error.to_string())
}

impl From<ScaleUpProvisioning> for proto::ScaleUpProvisioning {
    fn from(value: ScaleUpProvisioning) -> Self {
        Self {
            resource_uid: value.resource_uid.to_string(),
            spec_generation: value.spec_generation,
            desired_replicas: value.desired_replicas,
            previous_configuration: Some(value.previous_configuration.into()),
            previous_policy: Some(value.previous_policy.into()),
            current_policy: Some(value.current_policy.into()),
            target_replica_id: value.target_replica_id.value(),
        }
    }
}

impl TryFrom<proto::ScaleUpProvisioning> for ScaleUpProvisioning {
    type Error = WireError;

    fn try_from(value: proto::ScaleUpProvisioning) -> Result<Self, Self::Error> {
        let provisioning = Self {
            resource_uid: ResourceUid::new(value.resource_uid),
            spec_generation: value.spec_generation,
            desired_replicas: value.desired_replicas,
            previous_configuration: required(
                value.previous_configuration,
                "scale_up_provisioning.previous_configuration",
            )?
            .try_into()?,
            previous_policy: policy_from_proto(required(
                value.previous_policy,
                "scale_up_provisioning.previous_policy",
            )?)?,
            current_policy: policy_from_proto(required(
                value.current_policy,
                "scale_up_provisioning.current_policy",
            )?)?,
            target_replica_id: ReplicaId::new(value.target_replica_id),
        };
        validate_scale_up_provisioning_request(&provisioning).map_err(authority)?;
        Ok(provisioning)
    }
}

impl From<ScaleUpIntent> for proto::ScaleUpIntent {
    fn from(value: ScaleUpIntent) -> Self {
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
            build_id: value.build_id.to_string(),
            snapshot_boundary_lsn: value.snapshot_boundary_lsn,
            catch_up_boundary_lsn: value.catch_up_boundary_lsn,
        }
    }
}

impl TryFrom<proto::ScaleUpIntent> for ScaleUpIntent {
    type Error = WireError;

    fn try_from(value: proto::ScaleUpIntent) -> Result<Self, Self::Error> {
        let intent = Self {
            operation_id: OperationId::new(value.operation_id),
            resource_uid: ResourceUid::new(value.resource_uid),
            spec_generation: value.spec_generation,
            desired_replicas: value.desired_replicas,
            previous_configuration: required(
                value.previous_configuration,
                "scale_up.previous_configuration",
            )?
            .try_into()?,
            current_configuration: required(
                value.current_configuration,
                "scale_up.current_configuration",
            )?
            .try_into()?,
            previous_policy: policy_from_proto(required(
                value.previous_policy,
                "scale_up.previous_policy",
            )?)?,
            current_policy: policy_from_proto(required(
                value.current_policy,
                "scale_up.current_policy",
            )?)?,
            primary: required(value.primary, "scale_up.primary")?.try_into()?,
            target: required(value.target, "scale_up.target")?.try_into()?,
            build_id: OperationId::new(value.build_id),
            snapshot_boundary_lsn: value.snapshot_boundary_lsn,
            catch_up_boundary_lsn: value.catch_up_boundary_lsn,
        };
        crate::protocol::validation::validate_scale_up(&intent).map_err(authority)?;
        Ok(intent)
    }
}

impl From<ScaleUpWitness> for proto::ScaleUpWitness {
    fn from(value: ScaleUpWitness) -> Self {
        Self {
            resource_uid: value.resource_uid.to_string(),
            identity: Some(value.identity.into()),
            process_session_id: value.process_session_id.to_string(),
            report_sequence: value.report_sequence,
            epoch: Some(value.epoch.into()),
            previous_configuration_id: value
                .previous_configuration_id
                .map_or_else(String::new, |id| id.to_string()),
            current_configuration_id: value.current_configuration_id.to_string(),
            verified_replication_lsn: value.verified_replication_lsn,
            write_status: crate::control::convert::access_status_to_proto(value.write_status)
                as i32,
            pending_operation_id: value
                .pending_operation_id
                .map_or_else(String::new, |id| id.to_string()),
            retained_operation_id: value
                .retained_operation_id
                .map_or_else(String::new, |id| id.to_string()),
            role: crate::control::convert::role_to_proto(value.role) as i32,
        }
    }
}

impl TryFrom<proto::ScaleUpWitness> for ScaleUpWitness {
    type Error = WireError;

    fn try_from(value: proto::ScaleUpWitness) -> Result<Self, Self::Error> {
        let witness = Self {
            resource_uid: ResourceUid::new(value.resource_uid),
            identity: required(value.identity, "scale_up_witness.identity")?.try_into()?,
            role: role_from_proto(proto::ReplicaRole::try_from(value.role).map_err(|_| {
                WireError::InvalidEnum {
                    field: "scale_up_witness.role",
                    value: value.role,
                }
            })?)?,
            process_session_id: ProcessSessionId::new(value.process_session_id),
            report_sequence: value.report_sequence,
            epoch: required(value.epoch, "scale_up_witness.epoch")?.into(),
            previous_configuration_id: (!value.previous_configuration_id.is_empty())
                .then(|| ConfigurationId::new(value.previous_configuration_id)),
            current_configuration_id: ConfigurationId::new(value.current_configuration_id),
            verified_replication_lsn: value.verified_replication_lsn,
            write_status: access_status_from_proto(
                proto::AccessStatus::try_from(value.write_status).map_err(|_| {
                    WireError::InvalidEnum {
                        field: "scale_up_witness.write_status",
                        value: value.write_status,
                    }
                })?,
            )?,
            pending_operation_id: (!value.pending_operation_id.is_empty())
                .then(|| OperationId::new(value.pending_operation_id)),
            retained_operation_id: (!value.retained_operation_id.is_empty())
                .then(|| OperationId::new(value.retained_operation_id)),
        };
        if witness.resource_uid.is_empty()
            || witness.identity.replica_id.value() <= 0
            || witness.identity.instance_id.is_empty()
            || witness.identity.agent_generation.is_empty()
            || witness.process_session_id.is_empty()
            || witness.report_sequence == 0
            || witness.epoch.data_loss_number < 0
            || witness.epoch.configuration_number < 0
            || witness.current_configuration_id.is_empty()
            || witness.verified_replication_lsn < 0
        {
            return Err(WireError::InvalidAuthority(
                "scale-up witness contains invalid identity, epoch, sequence, or progress".into(),
            ));
        }
        Ok(witness)
    }
}

impl From<ScaleUpFinalWitness> for proto::ScaleUpFinalWitness {
    fn from(value: ScaleUpFinalWitness) -> Self {
        Self {
            replica_id: value.replica_id.value(),
            process_session_id: value.process_session_id.to_string(),
            report_sequence: value.report_sequence,
            current_progress: value.current_progress,
            committed_lsn: value.committed_lsn,
            deactivated_lsn: value.deactivated_lsn,
            fence_operation_id: value.fence_operation_id.to_string(),
        }
    }
}

impl TryFrom<proto::ScaleUpFinalWitness> for ScaleUpFinalWitness {
    type Error = WireError;

    fn try_from(value: proto::ScaleUpFinalWitness) -> Result<Self, Self::Error> {
        let witness = Self {
            replica_id: ReplicaId::new(value.replica_id),
            process_session_id: ProcessSessionId::new(value.process_session_id),
            report_sequence: value.report_sequence,
            current_progress: value.current_progress,
            committed_lsn: value.committed_lsn,
            deactivated_lsn: value.deactivated_lsn,
            fence_operation_id: OperationId::new(value.fence_operation_id),
        };
        if witness.replica_id.value() <= 0
            || witness.process_session_id.is_empty()
            || witness.report_sequence == 0
            || witness.current_progress < 0
            || witness.committed_lsn < 0
            || witness.deactivated_lsn < 0
            || witness.fence_operation_id.is_empty()
        {
            return Err(WireError::InvalidAuthority(
                "final scale-up witness contains invalid replica, sequence, or progress".into(),
            ));
        }
        Ok(witness)
    }
}

impl From<ScaleUpFinalElectionEvidence> for proto::ScaleUpFinalElectionEvidence {
    fn from(value: ScaleUpFinalElectionEvidence) -> Self {
        Self {
            selected_primary_replica_id: value.selected_primary_replica_id.value(),
            witnesses: value.witnesses.into_iter().map(Into::into).collect(),
            previous_read_quorum: value
                .previous_read_quorum
                .into_iter()
                .map(ReplicaId::value)
                .collect(),
            current_read_quorum: value
                .current_read_quorum
                .into_iter()
                .map(ReplicaId::value)
                .collect(),
        }
    }
}

impl TryFrom<proto::ScaleUpFinalElectionEvidence> for ScaleUpFinalElectionEvidence {
    type Error = WireError;

    fn try_from(value: proto::ScaleUpFinalElectionEvidence) -> Result<Self, Self::Error> {
        let evidence = Self {
            selected_primary_replica_id: ReplicaId::new(value.selected_primary_replica_id),
            witnesses: value
                .witnesses
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            previous_read_quorum: value
                .previous_read_quorum
                .into_iter()
                .map(ReplicaId::new)
                .collect(),
            current_read_quorum: value
                .current_read_quorum
                .into_iter()
                .map(ReplicaId::new)
                .collect(),
        };
        if evidence.selected_primary_replica_id.value() <= 0
            || evidence
                .previous_read_quorum
                .iter()
                .chain(&evidence.current_read_quorum)
                .any(|replica_id| replica_id.value() <= 0)
        {
            return Err(WireError::InvalidAuthority(
                "final scale-up election contains invalid authority or progress".into(),
            ));
        }
        Ok(evidence)
    }
}

impl From<ScaleUpFailoverEvidence> for proto::ScaleUpFailoverEvidence {
    fn from(value: ScaleUpFailoverEvidence) -> Self {
        Self {
            intent: Some(value.intent.into()),
            provisional_configuration: Some(value.provisional_configuration.into()),
            previous_read_quorum: value
                .previous_read_quorum
                .into_iter()
                .map(Into::into)
                .collect(),
            current_read_quorum: value
                .current_read_quorum
                .into_iter()
                .map(Into::into)
                .collect(),
            final_election: value.final_election.map(|evidence| (*evidence).into()),
        }
    }
}

impl TryFrom<proto::ScaleUpFailoverEvidence> for ScaleUpFailoverEvidence {
    type Error = WireError;

    fn try_from(value: proto::ScaleUpFailoverEvidence) -> Result<Self, Self::Error> {
        let evidence = Self {
            intent: required(value.intent, "scale_up_failover.intent")?.try_into()?,
            provisional_configuration: required(
                value.provisional_configuration,
                "scale_up_failover.provisional_configuration",
            )?
            .try_into()?,
            previous_read_quorum: value
                .previous_read_quorum
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            current_read_quorum: value
                .current_read_quorum
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            final_election: value
                .final_election
                .map(TryInto::try_into)
                .transpose()?
                .map(Box::new),
        };
        validate_scale_up_failover_evidence(&evidence).map_err(authority)?;
        Ok(evidence)
    }
}

impl From<ScaleUpConfigurationEvidence> for proto::ScaleUpConfigurationEvidence {
    fn from(value: ScaleUpConfigurationEvidence) -> Self {
        use proto::scale_up_configuration_evidence::Evidence;
        Self {
            evidence: Some(match value {
                ScaleUpConfigurationEvidence::Admission { intent } => {
                    Evidence::Admission(Box::new(intent.into()))
                }
                ScaleUpConfigurationEvidence::Failover { evidence } => {
                    Evidence::Failover(Box::new(evidence.into()))
                }
            }),
        }
    }
}

impl TryFrom<proto::ScaleUpConfigurationEvidence> for ScaleUpConfigurationEvidence {
    type Error = WireError;

    fn try_from(value: proto::ScaleUpConfigurationEvidence) -> Result<Self, Self::Error> {
        use proto::scale_up_configuration_evidence::Evidence;
        match required(value.evidence, "scale_up_evidence.evidence")? {
            Evidence::Admission(intent) => Ok(Self::Admission {
                intent: (*intent).try_into()?,
            }),
            Evidence::Failover(evidence) => Ok(Self::Failover {
                evidence: (*evidence).try_into()?,
            }),
        }
    }
}

impl From<ScaleUpCleanup> for proto::ScaleUpCleanup {
    fn from(value: ScaleUpCleanup) -> Self {
        Self {
            provisioning: Some(super::convert::provisioning_to_proto(value.provisioning)),
            target: Some(value.target.into()),
            resources: Some(value.resources.into()),
        }
    }
}

impl TryFrom<proto::ScaleUpCleanup> for ScaleUpCleanup {
    type Error = WireError;

    fn try_from(value: proto::ScaleUpCleanup) -> Result<Self, Self::Error> {
        let cleanup = Self {
            provisioning: super::convert::provisioning_from_proto(required(
                value.provisioning,
                "scale_up_cleanup.provisioning",
            )?)?,
            target: required(value.target, "scale_up_cleanup.target")?.try_into()?,
            resources: required(value.resources, "scale_up_cleanup.resources")?.try_into()?,
        };
        validate_scale_up_cleanup(&cleanup).map_err(authority)?;
        Ok(cleanup)
    }
}

impl From<ScaleUpFailoverReceiptEvidence> for proto::ScaleUpFailoverReceiptEvidence {
    fn from(value: ScaleUpFailoverReceiptEvidence) -> Self {
        Self {
            provisional_primary_replica_id: value.provisional_primary_replica_id.value(),
            previous_read_quorum: value
                .previous_read_quorum
                .into_iter()
                .map(Into::into)
                .collect(),
            current_read_quorum: value
                .current_read_quorum
                .into_iter()
                .map(Into::into)
                .collect(),
            final_election: Some((*value.final_election).into()),
        }
    }
}

impl TryFrom<proto::ScaleUpFailoverReceiptEvidence> for ScaleUpFailoverReceiptEvidence {
    type Error = WireError;

    fn try_from(value: proto::ScaleUpFailoverReceiptEvidence) -> Result<Self, Self::Error> {
        let evidence = Self {
            provisional_primary_replica_id: ReplicaId::new(value.provisional_primary_replica_id),
            previous_read_quorum: value
                .previous_read_quorum
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            current_read_quorum: value
                .current_read_quorum
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            final_election: Box::new(
                required(
                    value.final_election,
                    "scale_up_failover_receipt.final_election",
                )?
                .try_into()?,
            ),
        };
        if evidence.provisional_primary_replica_id.value() <= 0 {
            return Err(WireError::InvalidAuthority(
                "scale-up failover receipt contains invalid provisional authority".into(),
            ));
        }
        Ok(evidence)
    }
}

impl From<ScaleUpReceipt> for proto::ScaleUpReceipt {
    fn from(value: ScaleUpReceipt) -> Self {
        Self {
            intent: Some(value.intent.into()),
            accepted_configuration: Some(value.accepted_configuration.into()),
            failover_evidence: value.failover_evidence.map(Into::into),
            failover_safe_lsn: value.failover_safe_lsn,
            current_only_write_quorum: value
                .current_only_write_quorum
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}

impl TryFrom<proto::ScaleUpReceipt> for ScaleUpReceipt {
    type Error = WireError;

    fn try_from(value: proto::ScaleUpReceipt) -> Result<Self, Self::Error> {
        let receipt = Self {
            intent: required(value.intent, "scale_up_receipt.intent")?.try_into()?,
            accepted_configuration: required(
                value.accepted_configuration,
                "scale_up_receipt.accepted_configuration",
            )?
            .try_into()?,
            failover_evidence: value.failover_evidence.map(TryInto::try_into).transpose()?,
            failover_safe_lsn: value.failover_safe_lsn,
            current_only_write_quorum: value
                .current_only_write_quorum
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        };
        validate_scale_up_receipt(&receipt).map_err(authority)?;
        Ok(receipt)
    }
}

pub(crate) fn configuration_from_proto(
    value: proto::EnsureConfigurationCommand,
) -> Result<EnsureConfiguration, WireError> {
    if value.secondary_removal_evidence.is_some()
        || value.switchover_handoff.is_some()
        || !value.retire_switchover_preparation_ids.is_empty()
    {
        return Err(WireError::InvalidAuthority(
            "scale-up cannot carry removal or switchover evidence".into(),
        ));
    }
    let transition_kind =
        match proto::TransitionKind::try_from(value.transition_kind).map_err(|_| {
            WireError::InvalidEnum {
                field: "ensure.transition_kind",
                value: value.transition_kind,
            }
        })? {
            proto::TransitionKind::ScaleUp => TransitionKind::ScaleUp,
            proto::TransitionKind::Failover => TransitionKind::Failover,
            other => {
                return Err(WireError::InvalidEnum {
                    field: "ensure.transition_kind",
                    value: other as i32,
                });
            }
        };
    let primary_write_status = access_status_from_proto(
        proto::AccessStatus::try_from(value.primary_write_status).map_err(|_| {
            WireError::InvalidEnum {
                field: "ensure.primary_write_status",
                value: value.primary_write_status,
            }
        })?,
    )?;
    let command = EnsureConfiguration {
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
        secondary_removal_evidence: None,
        scale_up_evidence: Some(Box::new(
            required(value.scale_up_evidence, "ensure.scale_up_evidence")?.try_into()?,
        )),
        local_replica_id: ReplicaId::new(value.local_replica_id),
        expected_instance_id: ReplicaInstanceId::new(value.expected_instance_id),
        expected_agent_generation: AgentGeneration::new(value.expected_agent_generation),
        transition_kind,
        failover_safe_lsn: value.failover_safe_lsn,
        primary_write_status,
        current_only: value.current_only,
        retire_build_ids: if value.retire_build_ids.is_empty() {
            (!value.retire_build_id.is_empty())
                .then(|| OperationId::new(value.retire_build_id))
                .into_iter()
                .collect()
        } else {
            value
                .retire_build_ids
                .into_iter()
                .map(OperationId::new)
                .collect()
        },
        switchover_handoff: None,
        retire_switchover_preparation_ids: Vec::new(),
    };
    validate_scale_up_configuration(&command).map_err(authority)?;
    Ok(command)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(id: i64) -> ReplicaIdentity {
        ReplicaIdentity {
            replica_id: ReplicaId::new(id),
            instance_id: ReplicaInstanceId::new(format!("pod-{id}")),
            agent_generation: AgentGeneration::new(format!("gen-{id}")),
        }
    }

    fn intent() -> ScaleUpIntent {
        let previous_policy = EffectivePolicy::fixed(1, 30).unwrap();
        let current_policy = EffectivePolicy::fixed(2, 30).unwrap();
        let primary = identity(1);
        let target = identity(2);
        let previous_configuration = ConfigurationDescriptor::new(
            Epoch::new(0, 1),
            primary.replica_id,
            vec![ConfigurationMember {
                identity: primary.clone(),
                role: ReplicaRole::Primary,
            }],
            previous_policy.write_quorum,
        );
        let current_configuration = ConfigurationDescriptor::new(
            Epoch::new(0, 2),
            primary.replica_id,
            vec![
                ConfigurationMember {
                    identity: primary.clone(),
                    role: ReplicaRole::Primary,
                },
                ConfigurationMember {
                    identity: target.clone(),
                    role: ReplicaRole::ActiveSecondary,
                },
            ],
            current_policy.write_quorum,
        );
        let mut intent = ScaleUpIntent {
            operation_id: OperationId::default(),
            resource_uid: ResourceUid::new("set"),
            spec_generation: 2,
            desired_replicas: 2,
            previous_configuration,
            current_configuration,
            previous_policy,
            current_policy,
            primary,
            target,
            build_id: OperationId::new("build"),
            snapshot_boundary_lsn: 0,
            catch_up_boundary_lsn: 0,
        };
        intent.operation_id = intent.expected_operation_id();
        intent
    }

    fn witness(
        intent: &ScaleUpIntent,
        identity: ReplicaIdentity,
        previous_current: bool,
        sequence: u64,
    ) -> ScaleUpWitness {
        let role = intent
            .current_configuration
            .members
            .iter()
            .find(|member| member.identity == identity)
            .unwrap()
            .role;
        let stage = if previous_current {
            ScaleUpStage::PreviousCurrent
        } else {
            ScaleUpStage::CurrentOnly
        };
        ScaleUpWitness {
            resource_uid: intent.resource_uid.clone(),
            retained_operation_id: Some(intent.command_operation_id(
                stage,
                &identity,
                &intent.current_configuration,
            )),
            identity: identity.clone(),
            role,
            process_session_id: ProcessSessionId::new(format!("session-{sequence}")),
            report_sequence: sequence,
            epoch: intent.current_configuration.epoch,
            previous_configuration_id: previous_current
                .then(|| intent.previous_configuration.configuration_id.clone()),
            current_configuration_id: intent.current_configuration.configuration_id.clone(),
            verified_replication_lsn: intent.catch_up_boundary_lsn,
            write_status: if !previous_current && identity == intent.primary {
                AccessStatus::Granted
            } else {
                AccessStatus::ReconfigurationPending
            },
            pending_operation_id: None,
        }
    }

    #[test]
    fn scale_up_intent_round_trips_with_zero_boundaries() {
        let canonical = intent();
        let wire: proto::ScaleUpIntent = canonical.clone().into();
        assert_eq!(ScaleUpIntent::try_from(wire).unwrap(), canonical);
    }

    #[test]
    fn scale_up_intent_rejects_negative_and_reversed_boundaries() {
        let mut wire: proto::ScaleUpIntent = intent().into();
        wire.snapshot_boundary_lsn = -1;
        assert!(ScaleUpIntent::try_from(wire).is_err());

        let mut wire: proto::ScaleUpIntent = intent().into();
        wire.snapshot_boundary_lsn = 2;
        wire.catch_up_boundary_lsn = 1;
        assert!(ScaleUpIntent::try_from(wire).is_err());
    }

    #[test]
    fn standalone_witness_conversion_rejects_invalid_domains() {
        let intent = intent();
        let canonical = witness(&intent, intent.primary.clone(), true, 1);
        for mutation in 0..4 {
            let mut wire: proto::ScaleUpWitness = canonical.clone().into();
            match mutation {
                0 => wire.process_session_id.clear(),
                1 => wire.report_sequence = 0,
                2 => wire.verified_replication_lsn = -1,
                _ => wire.epoch.as_mut().unwrap().configuration_number = -1,
            }
            assert!(
                ScaleUpWitness::try_from(wire).is_err(),
                "mutation {mutation}"
            );
        }
    }

    #[test]
    fn tagged_scale_up_provisioning_round_trips() {
        use proto::provisioning_intent::Purpose;
        let intent = intent();
        let mut canonical = ProvisioningIntent {
            purpose: ProvisioningPurpose::scale_up(ScaleUpProvisioning {
                resource_uid: intent.resource_uid.clone(),
                spec_generation: intent.spec_generation,
                desired_replicas: intent.desired_replicas,
                previous_configuration: intent.previous_configuration.clone(),
                previous_policy: intent.previous_policy.clone(),
                current_policy: intent.current_policy.clone(),
                target_replica_id: intent.target.replica_id,
            }),
            pod_uid: PodUid::new("pod-2"),
            pvc_uid: PvcUid::new("pvc-2"),
            operation_id: OperationId::default(),
        };
        canonical.operation_id = canonical.expected_operation_id();
        let wire = proto::ProvisioningIntent {
            purpose: Some(Purpose::ScaleUp(
                canonical.purpose.scale_up.clone().unwrap().into(),
            )),
            pod_uid: canonical.pod_uid.to_string(),
            pvc_uid: canonical.pvc_uid.to_string(),
            operation_id: canonical.operation_id.to_string(),
        };
        assert_eq!(
            super::super::convert::provisioning_from_proto(wire.clone()).unwrap(),
            canonical
        );
        let mut negative_epoch = wire;
        let proto::provisioning_intent::Purpose::ScaleUp(scale_up) =
            negative_epoch.purpose.as_mut().unwrap()
        else {
            unreachable!()
        };
        scale_up
            .previous_configuration
            .as_mut()
            .unwrap()
            .epoch
            .as_mut()
            .unwrap()
            .configuration_number = -1;
        assert!(super::super::convert::provisioning_from_proto(negative_epoch).is_err());
    }

    #[test]
    fn scale_up_configuration_command_round_trips_typed_evidence() {
        let intent = intent();
        let target = intent.primary.clone();
        let operation_id = intent.command_operation_id(
            ScaleUpStage::PreviousCurrent,
            &target,
            &intent.current_configuration,
        );
        let wire = proto::EnsureConfigurationCommand {
            operation_id: operation_id.to_string(),
            previous_configuration: Some(intent.previous_configuration.clone().into()),
            current_configuration: Some(intent.current_configuration.clone().into()),
            previous_epoch: Some(intent.previous_configuration.epoch.into()),
            current_epoch: Some(intent.current_configuration.epoch.into()),
            effective_policy: Some(intent.current_policy.clone().into()),
            local_replica_id: target.replica_id.value(),
            expected_instance_id: target.instance_id.to_string(),
            expected_agent_generation: target.agent_generation.to_string(),
            transition_kind: proto::TransitionKind::ScaleUp as i32,
            grant_write: false,
            current_only: false,
            retire_build_id: String::new(),
            primary_write_status: proto::AccessStatus::ReconfigurationPending as i32,
            retire_build_ids: Vec::new(),
            failover_safe_lsn: None,
            switchover_handoff: None,
            retire_switchover_preparation_ids: Vec::new(),
            previous_policy: Some(intent.previous_policy.clone().into()),
            secondary_removal_evidence: None,
            scale_up_evidence: Some(
                ScaleUpConfigurationEvidence::Admission {
                    intent: intent.clone(),
                }
                .into(),
            ),
        };
        let canonical = configuration_from_proto(wire).unwrap();
        assert_eq!(canonical.operation_id, operation_id);
        assert_eq!(
            canonical.scale_up_evidence,
            Some(Box::new(ScaleUpConfigurationEvidence::Admission { intent }))
        );
    }

    #[test]
    fn failover_configuration_round_trips_independent_recovery_evidence() {
        let intent = intent();
        let mut members = intent.current_configuration.members.clone();
        for member in &mut members {
            member.role = if member.identity == intent.target {
                ReplicaRole::Primary
            } else {
                ReplicaRole::ActiveSecondary
            };
        }
        let failover = ConfigurationDescriptor::new(
            Epoch::new(0, 3),
            intent.target.replica_id,
            members,
            intent.current_policy.write_quorum,
        );
        let evidence = ScaleUpFailoverEvidence {
            provisional_configuration: failover.clone(),
            previous_read_quorum: vec![witness(&intent, intent.primary.clone(), true, 1)],
            current_read_quorum: vec![witness(&intent, intent.target.clone(), true, 2)],
            final_election: None,
            intent: intent.clone(),
        };
        let wire_evidence: proto::ScaleUpFailoverEvidence = evidence.clone().into();
        assert_eq!(
            ScaleUpFailoverEvidence::try_from(wire_evidence).unwrap(),
            evidence
        );
        let final_configuration = ConfigurationDescriptor::new(
            Epoch::new(0, 4),
            intent.target.replica_id,
            failover.members.clone(),
            intent.current_policy.write_quorum,
        );
        let final_witness = |identity: ReplicaIdentity, sequence: u64| ScaleUpFinalWitness {
            replica_id: identity.replica_id,
            process_session_id: ProcessSessionId::new(format!("final-session-{sequence}")),
            report_sequence: sequence,
            current_progress: intent.catch_up_boundary_lsn,
            committed_lsn: intent.catch_up_boundary_lsn,
            deactivated_lsn: intent.catch_up_boundary_lsn,
            fence_operation_id: intent.command_operation_id(
                ScaleUpStage::PreviousCurrent,
                &identity,
                &failover,
            ),
        };
        let mut finalized_evidence = evidence.clone();
        let previous_witness = final_witness(intent.primary.clone(), 3);
        let current_witness = final_witness(intent.target.clone(), 4);
        finalized_evidence.final_election = Some(Box::new(ScaleUpFinalElectionEvidence {
            selected_primary_replica_id: final_configuration.primary_id,
            witnesses: vec![previous_witness, current_witness],
            previous_read_quorum: vec![intent.primary.replica_id],
            current_read_quorum: vec![intent.target.replica_id],
        }));
        let final_wire: proto::ScaleUpFailoverEvidence = finalized_evidence.clone().into();
        assert_eq!(
            ScaleUpFailoverEvidence::try_from(final_wire).unwrap(),
            finalized_evidence
        );
        let receipt_evidence =
            ScaleUpFailoverReceiptEvidence::from_evidence(&finalized_evidence).unwrap();
        let receipt_wire: proto::ScaleUpFailoverReceiptEvidence = receipt_evidence.clone().into();
        let decoded_receipt = ScaleUpFailoverReceiptEvidence::try_from(receipt_wire).unwrap();
        assert_eq!(decoded_receipt, receipt_evidence);
        assert_eq!(
            decoded_receipt.expand(&intent),
            Some(finalized_evidence.clone())
        );

        let target = intent.target.clone();
        let operation_id =
            intent.command_operation_id(ScaleUpStage::PreviousCurrent, &target, &failover);
        let wire = proto::EnsureConfigurationCommand {
            operation_id: operation_id.to_string(),
            previous_configuration: Some(intent.previous_configuration.clone().into()),
            current_configuration: Some(failover.clone().into()),
            previous_epoch: Some(intent.previous_configuration.epoch.into()),
            current_epoch: Some(failover.epoch.into()),
            effective_policy: Some(intent.current_policy.clone().into()),
            local_replica_id: target.replica_id.value(),
            expected_instance_id: target.instance_id.to_string(),
            expected_agent_generation: target.agent_generation.to_string(),
            transition_kind: proto::TransitionKind::Failover as i32,
            grant_write: false,
            current_only: false,
            retire_build_id: String::new(),
            primary_write_status: proto::AccessStatus::ReconfigurationPending as i32,
            retire_build_ids: Vec::new(),
            failover_safe_lsn: None,
            switchover_handoff: None,
            retire_switchover_preparation_ids: Vec::new(),
            previous_policy: Some(intent.previous_policy.clone().into()),
            secondary_removal_evidence: None,
            scale_up_evidence: Some(ScaleUpConfigurationEvidence::Failover { evidence }.into()),
        };
        let canonical = configuration_from_proto(wire).unwrap();
        assert_eq!(canonical.operation_id, operation_id);
        assert_eq!(canonical.current_configuration, failover);
    }

    #[test]
    fn scale_up_cleanup_and_receipt_round_trip_with_attempt_fencing() {
        let intent = intent();
        let mut provisioning = ProvisioningIntent {
            purpose: ProvisioningPurpose::scale_up(ScaleUpProvisioning {
                resource_uid: intent.resource_uid.clone(),
                spec_generation: intent.spec_generation,
                desired_replicas: intent.desired_replicas,
                previous_configuration: intent.previous_configuration.clone(),
                previous_policy: intent.previous_policy.clone(),
                current_policy: intent.current_policy.clone(),
                target_replica_id: intent.target.replica_id,
            }),
            pod_uid: PodUid::new("pod-2"),
            pvc_uid: PvcUid::new("pvc-2"),
            operation_id: OperationId::default(),
        };
        provisioning.operation_id = provisioning.expected_operation_id();
        let target = provisioning.target_identity(&intent.resource_uid);
        let cleanup = ScaleUpCleanup {
            provisioning,
            target: target.clone(),
            resources: ReplicaCleanupIdentity {
                pod: CleanupResourceIdentity::Present {
                    name: "db-1".into(),
                    uid: target.instance_id.to_string(),
                },
                pvc: CleanupResourceIdentity::Present {
                    name: "db-1-data".into(),
                    uid: "pvc-2".into(),
                },
                endpoint: CleanupResourceIdentity::Present {
                    name: derive_replica_endpoint_name(&intent.resource_uid, &target),
                    uid: "service-2".into(),
                },
            },
        };
        let wire: proto::ScaleUpCleanup = cleanup.clone().into();
        assert_eq!(ScaleUpCleanup::try_from(wire).unwrap(), cleanup);

        let receipt = ScaleUpReceipt {
            accepted_configuration: intent.current_configuration.clone(),
            failover_evidence: None,
            failover_safe_lsn: None,
            current_only_write_quorum: vec![
                witness(&intent, intent.primary.clone(), false, 2),
                witness(&intent, intent.target.clone(), false, 3),
            ],
            intent,
        };
        let wire: proto::ScaleUpReceipt = receipt.clone().into();
        assert_eq!(ScaleUpReceipt::try_from(wire).unwrap(), receipt);
    }

    #[test]
    fn agent_report_requires_typed_evidence_for_expanded_pc_cc() {
        let intent = intent();
        let report = proto::AgentStatusReport {
            protocol_version: crate::protocol::PROTOCOL_VERSION,
            resource_uid: intent.resource_uid.to_string(),
            identity: Some(intent.primary.clone().into()),
            process_session_id: "session".into(),
            report_sequence: 1,
            role: proto::ReplicaRole::Primary as i32,
            write_status: proto::AccessStatus::Granted as i32,
            epoch: Some(intent.current_configuration.epoch.into()),
            previous_configuration: Some(intent.previous_configuration.clone().into()),
            current_configuration: Some(intent.current_configuration.clone().into()),
            current_progress: 0,
            committed_lsn: 0,
            storage_state: proto::AgentStorageState::Initialized as i32,
            pod_uid: intent.primary.instance_id.to_string(),
            pvc_uid: "pvc-1".into(),
            healthy: true,
            replica_id: intent.primary.replica_id.value(),
            read_status: proto::AccessStatus::Granted as i32,
            verified_replication_lsn: Some(0),
            scale_up_intent: Some(intent.clone().into()),
            ..Default::default()
        };
        let result = crate::control::validate_agent_status_report(&report);
        assert!(result.is_ok(), "{result:?}");
        let mut wrong_resource = report.clone();
        wrong_resource.resource_uid = "other".into();
        assert!(crate::control::validate_agent_status_report(&wrong_resource).is_err());

        let mut unrelated = report.clone();
        let mut members = intent.current_configuration.members.clone();
        members.push(ConfigurationMember {
            identity: identity(3),
            role: ReplicaRole::ActiveSecondary,
        });
        unrelated.current_configuration = Some(
            ConfigurationDescriptor::new(
                intent.current_configuration.epoch,
                intent.current_configuration.primary_id,
                members,
                2,
            )
            .into(),
        );
        assert!(crate::control::validate_agent_status_report(&unrelated).is_err());

        let mut missing = report;
        missing.scale_up_intent = None;
        assert!(crate::control::validate_agent_status_report(&missing).is_err());
    }
}
