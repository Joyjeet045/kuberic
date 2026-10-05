use kuberic_runtime::protocol::command::{
    EnsureConfiguration, PrepareSecondaryRemoval, RetireReplica,
};
use kuberic_runtime::protocol::types::*;

pub fn intent(ids: &[i64], primary_id: i64) -> SecondaryScaleDownIntent {
    let previous_policy = EffectivePolicy::fixed(ids.len() as u32, 30).unwrap();
    let current_policy = EffectivePolicy::fixed(ids.len() as u32 - 1, 30).unwrap();
    let previous = ConfigurationDescriptor::new(
        Epoch::new(2, 10),
        ReplicaId::new(primary_id),
        ids.iter()
            .map(|id| ConfigurationMember {
                identity: ReplicaIdentity {
                    replica_id: ReplicaId::new(*id),
                    instance_id: ReplicaInstanceId::new(format!("pod-{id}")),
                    agent_generation: AgentGeneration::new(format!("generation-{id}")),
                },
                role: if *id == primary_id {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        previous_policy.write_quorum,
    );
    let primary = previous
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .unwrap()
        .identity
        .clone();
    let target = previous
        .members
        .iter()
        .filter(|member| member.role != ReplicaRole::Primary)
        .max_by_key(|member| member.identity.replica_id)
        .unwrap()
        .identity
        .clone();
    let current = ConfigurationDescriptor::new(
        Epoch::new(2, 11),
        previous.primary_id,
        previous
            .members
            .iter()
            .filter(|member| member.identity != target)
            .cloned()
            .collect(),
        current_policy.write_quorum,
    );
    let resource_uid = ResourceUid::new("resource-uid");
    let cleanup = ReplicaCleanupIdentity {
        pod: CleanupResourceIdentity::Present {
            name: format!("db-{}", target.replica_id),
            uid: target.instance_id.to_string(),
        },
        pvc: CleanupResourceIdentity::Present {
            name: format!("db-storage-{}", target.replica_id),
            uid: format!("pvc-{}", target.replica_id),
        },
        endpoint: CleanupResourceIdentity::Present {
            name: derive_replica_endpoint_name(&resource_uid, &target),
            uid: format!("service-{}", target.replica_id),
        },
    };
    let mut intent = SecondaryScaleDownIntent {
        operation_id: OperationId::default(),
        resource_uid,
        spec_generation: 7,
        desired_replicas: 1,
        previous_configuration: previous,
        current_configuration: current,
        previous_policy,
        current_policy,
        primary,
        target,
        cleanup,
    };
    intent.operation_id = intent.expected_operation_id();
    intent
}

pub fn preparation(intent: &SecondaryScaleDownIntent) -> SecondaryRemovalPreparation {
    SecondaryRemovalPreparation {
        intent: intent.clone(),
        operation_id: intent.command_operation_id(SecondaryRemovalStage::Prepare, &intent.primary),
        process_session_id: ProcessSessionId::new(format!("session-{}", intent.primary.replica_id)),
        report_sequence: 1,
        boundary_lsn: 10,
    }
}

pub fn witnesses(
    intent: &SecondaryScaleDownIntent,
    stage: SecondaryRemovalStage,
) -> Vec<SecondaryRemovalWitness> {
    let old = stage == SecondaryRemovalStage::Prepare;
    let configuration = if old {
        &intent.previous_configuration
    } else {
        &intent.current_configuration
    };
    intent
        .current_configuration
        .members
        .iter()
        .map(|member| SecondaryRemovalWitness {
            resource_uid: intent.resource_uid.clone(),
            identity: member.identity.clone(),
            role: member.role,
            process_session_id: ProcessSessionId::new(format!(
                "session-{}",
                member.identity.replica_id
            )),
            report_sequence: match stage {
                SecondaryRemovalStage::Prepare => 2,
                SecondaryRemovalStage::PreviousCurrent => 3,
                _ => 4,
            },
            epoch: configuration.epoch,
            previous_configuration_id: (stage == SecondaryRemovalStage::PreviousCurrent)
                .then(|| intent.previous_configuration.configuration_id.clone()),
            current_configuration_id: configuration.configuration_id.clone(),
            verified_replication_lsn: 10,
            write_status: AccessStatus::ReconfigurationPending,
            pending_operation_id: None,
            retained_operation_id: Some(intent.command_operation_id(stage, &member.identity)),
        })
        .collect()
}

pub fn evidence(intent: &SecondaryScaleDownIntent) -> SecondaryRemovalEvidence {
    SecondaryRemovalEvidence {
        preparation: preparation(intent),
        previous_read_quorum: witnesses(intent, SecondaryRemovalStage::Prepare),
        reduced_write_quorum: witnesses(intent, SecondaryRemovalStage::PreviousCurrent),
    }
}

pub fn cleanup(intent: &SecondaryScaleDownIntent) -> SecondaryScaleDownCleanup {
    SecondaryScaleDownCleanup {
        evidence: evidence(intent),
        current_only_write_quorum: witnesses(intent, SecondaryRemovalStage::CurrentOnly),
        retirement: None,
    }
}

pub fn transition(intent: &SecondaryScaleDownIntent) -> TransitionIntent {
    TransitionIntent {
        transition_id: derive_transition_id(
            &intent.resource_uid,
            TransitionKind::SecondaryScaleDown,
            &intent.current_configuration.configuration_id,
        ),
        kind: TransitionKind::SecondaryScaleDown,
        spec_generation: intent.spec_generation,
        effective_policy: intent.current_policy.clone(),
        previous_configuration_id: Some(intent.previous_configuration.configuration_id.clone()),
        current_configuration: intent.current_configuration.clone(),
        secondary_scale_down: Some(intent.clone()),
        secondary_removal_evidence: Some(evidence(intent)),
        scale_up: None,
        scale_up_failover: None,
        election_lsn: None,
        build_id: None,
        repair: None,
        switchover: None,
    }
}

pub fn configuration_command(
    intent: &SecondaryScaleDownIntent,
    current_only: bool,
) -> EnsureConfiguration {
    EnsureConfiguration {
        operation_id: intent.command_operation_id(
            if current_only {
                SecondaryRemovalStage::CurrentOnly
            } else {
                SecondaryRemovalStage::PreviousCurrent
            },
            &intent.primary,
        ),
        previous_configuration: (!current_only).then(|| intent.previous_configuration.clone()),
        current_configuration: intent.current_configuration.clone(),
        previous_epoch: (!current_only).then_some(intent.previous_configuration.epoch),
        current_epoch: intent.current_configuration.epoch,
        effective_policy: intent.current_policy.clone(),
        previous_policy: Some(intent.previous_policy.clone()),
        secondary_removal_evidence: Some(evidence(intent)),
        scale_up_evidence: None,
        local_replica_id: intent.primary.replica_id,
        expected_instance_id: intent.primary.instance_id.clone(),
        expected_agent_generation: intent.primary.agent_generation.clone(),
        transition_kind: TransitionKind::SecondaryScaleDown,
        failover_safe_lsn: None,
        primary_write_status: AccessStatus::ReconfigurationPending,
        current_only,
        retire_build_ids: Vec::new(),
        switchover_handoff: None,
        retire_switchover_preparation_ids: Vec::new(),
    }
}

pub fn prepare_command(intent: &SecondaryScaleDownIntent) -> PrepareSecondaryRemoval {
    PrepareSecondaryRemoval {
        operation_id: intent.command_operation_id(SecondaryRemovalStage::Prepare, &intent.primary),
        local_replica_id: intent.primary.replica_id,
        expected_instance_id: intent.primary.instance_id.clone(),
        expected_agent_generation: intent.primary.agent_generation.clone(),
        intent: intent.clone(),
    }
}

pub fn retire_command(intent: &SecondaryScaleDownIntent) -> RetireReplica {
    RetireReplica {
        operation_id: intent.command_operation_id(SecondaryRemovalStage::Retire, &intent.target),
        local_replica_id: intent.target.replica_id,
        expected_instance_id: intent.target.instance_id.clone(),
        expected_agent_generation: intent.target.agent_generation.clone(),
        committed: cleanup(intent),
    }
}

pub fn retirement(intent: &SecondaryScaleDownIntent) -> ReplicaRetirementReport {
    ReplicaRetirementReport {
        intent: intent.clone(),
        operation_id: intent.command_operation_id(SecondaryRemovalStage::Retire, &intent.target),
        process_session_id: ProcessSessionId::new("retired-session"),
        report_sequence: 5,
        epoch: intent.current_configuration.epoch,
        role: ReplicaRole::None,
        read_status: AccessStatus::NotPrimary,
        write_status: AccessStatus::NotPrimary,
        application_closed: true,
        peers_fenced: true,
    }
}
