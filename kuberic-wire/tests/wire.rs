use kuberic_protocol::types::{
    AgentGeneration, ConfigurationDescriptor, ConfigurationMember, Epoch, InitializationId,
    ReplicaId, ReplicaIdentity, ReplicaInstanceId, ReplicaRole, derive_agent_generation,
};
use kuberic_wire::convert::WireError;
use kuberic_wire::{
    ensure_supported_version, normalize_agent_status_report, normalize_execute_request, proto,
    validate_agent_status_report, validate_copy_ack, validate_copy_item, validate_execute_request,
    validate_replication_ack, validate_replication_item,
};
use prost::Message;

fn configuration() -> ConfigurationDescriptor {
    ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        ReplicaId::new(1),
        vec![
            ConfigurationMember {
                identity: ReplicaIdentity {
                    replica_id: ReplicaId::new(1),
                    instance_id: ReplicaInstanceId::new("pod-1"),
                    agent_generation: AgentGeneration::new("generation-1"),
                },
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: ReplicaIdentity {
                    replica_id: ReplicaId::new(2),
                    instance_id: ReplicaInstanceId::new("pod-2"),
                    agent_generation: AgentGeneration::new("generation-2"),
                },
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: ReplicaIdentity {
                    replica_id: ReplicaId::new(3),
                    instance_id: ReplicaInstanceId::new("pod-3"),
                    agent_generation: AgentGeneration::new("generation-3"),
                },
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    )
}

#[test]
fn protocol_version_requires_exact_match() {
    assert!(ensure_supported_version(kuberic_protocol::PROTOCOL_VERSION).is_ok());
    assert!(matches!(
        ensure_supported_version(kuberic_protocol::PROTOCOL_VERSION + 1),
        Err(WireError::UnsupportedProtocolVersion { .. })
    ));
}

#[test]
fn identity_requires_exact_incarnation_and_generation() {
    let missing_instance = proto::ReplicaIdentity {
        replica_id: 1,
        instance_id: String::new(),
        agent_generation: "generation".to_string(),
    };
    assert!(matches!(
        ReplicaIdentity::try_from(missing_instance),
        Err(WireError::MissingField("replica_identity.instance_id"))
    ));

    let missing_generation = proto::ReplicaIdentity {
        replica_id: 1,
        instance_id: "pod".to_string(),
        agent_generation: String::new(),
    };
    assert!(matches!(
        ReplicaIdentity::try_from(missing_generation),
        Err(WireError::MissingField("replica_identity.agent_generation"))
    ));
}

#[test]
fn configuration_round_trip_preserves_canonical_authority() {
    let original = configuration();
    let wire: proto::Configuration = original.clone().into();
    let decoded = ConfigurationDescriptor::try_from(wire).unwrap();
    assert_eq!(decoded, original);
}

#[test]
fn configuration_id_mismatch_is_rejected() {
    let mut wire: proto::Configuration = configuration().into();
    wire.configuration_id = "cfg-conflict".to_string();

    assert!(matches!(
        ConfigurationDescriptor::try_from(wire),
        Err(WireError::InvalidAuthority(_))
    ));
}

#[test]
fn uninitialized_status_requires_pod_and_pvc_without_durable_identity() {
    let report = proto::AgentStatusReport {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: "resource".to_string(),
        identity: None,
        process_session_id: "session".to_string(),
        report_sequence: 1,
        role: proto::ReplicaRole::Unknown as i32,
        write_status: proto::AccessStatus::Unknown as i32,
        epoch: None,
        previous_configuration: None,
        current_configuration: None,
        current_progress: 0,
        committed_lsn: 0,
        catch_up_capability: None,
        storage_state: proto::AgentStorageState::Uninitialized as i32,
        pod_uid: "pod".to_string(),
        pvc_uid: "pvc".to_string(),
        storage_error: String::new(),
        healthy: false,
        replica_id: 1,
        ..Default::default()
    };
    assert!(validate_agent_status_report(&report).is_ok());

    let mut invalid = report.clone();
    invalid.identity = Some(proto::ReplicaIdentity {
        replica_id: 1,
        instance_id: "pod".to_string(),
        agent_generation: "generation".to_string(),
    });
    assert!(matches!(
        validate_agent_status_report(&invalid),
        Err(WireError::InvalidAuthority(_))
    ));

    let mut contradictory = report;
    contradictory.role = proto::ReplicaRole::Primary as i32;
    contradictory.epoch = Some(proto::Epoch {
        data_loss_number: 0,
        configuration_number: 1,
    });
    assert!(matches!(
        validate_agent_status_report(&contradictory),
        Err(WireError::InvalidAuthority(_))
    ));

    let mut certified = contradictory;
    certified.role = proto::ReplicaRole::Unknown as i32;
    certified.verified_replication_lsn = Some(1);
    assert!(matches!(
        validate_agent_status_report(&certified),
        Err(WireError::InvalidAuthority(_))
    ));
}

#[test]
fn initialize_request_carries_complete_fresh_storage_fence() {
    let assigned_generation = derive_agent_generation(&InitializationId::new("init"));
    let target = ReplicaIdentity {
        replica_id: ReplicaId::new(1),
        instance_id: ReplicaInstanceId::new("pod"),
        agent_generation: assigned_generation.clone(),
    };
    let bootstrap_configuration = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        ReplicaId::new(1),
        vec![
            ConfigurationMember {
                identity: target.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: ReplicaIdentity {
                    replica_id: ReplicaId::new(2),
                    instance_id: ReplicaInstanceId::new("pod-2"),
                    agent_generation: AgentGeneration::new("generation-2"),
                },
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: ReplicaIdentity {
                    replica_id: ReplicaId::new(3),
                    instance_id: ReplicaInstanceId::new("pod-3"),
                    agent_generation: AgentGeneration::new("generation-3"),
                },
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    );
    let request = proto::ExecuteCommandRequest {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: "resource".to_string(),
        target: Some(target.into()),
        expected_process_session_id: "session".to_string(),
        command: Some(
            proto::execute_command_request::Command::InitializeAgentStore(
                proto::InitializeAgentStoreCommand {
                    initialization_id: "init".to_string(),
                    resource_uid: "resource".to_string(),
                    local_replica_id: 1,
                    expected_instance_id: "pod".to_string(),
                    expected_pod_uid: "pod".to_string(),
                    expected_pvc_uid: "pvc".to_string(),
                    assigned_agent_generation: assigned_generation.to_string(),
                    effective_policy: Some(proto::EffectivePolicy {
                        replica_set_size: 3,
                        write_quorum: 2,
                        read_quorum: 2,
                        failover_delay_seconds: 10,
                    }),
                    bootstrap_configuration: Some(bootstrap_configuration.into()),
                    provisioning: None,
                },
            ),
        ),
    };
    assert!(validate_execute_request(&request).is_ok());
    let normalized = normalize_execute_request(request.clone()).unwrap();
    assert_eq!(normalized.expected_process_session_id.as_str(), "session");

    let mut invalid = request;
    let Some(proto::execute_command_request::Command::InitializeAgentStore(command)) =
        invalid.command.as_mut()
    else {
        unreachable!();
    };
    command.assigned_agent_generation.clear();
    assert!(matches!(
        validate_execute_request(&invalid),
        Err(WireError::MissingField(
            "initialize.assigned_agent_generation"
        ))
    ));
}

#[test]
fn execute_request_requires_target_process_session() {
    let mut request = proto::ExecuteCommandRequest {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: "resource".to_string(),
        target: Some(proto::ReplicaIdentity {
            replica_id: 1,
            instance_id: "pod-1".to_string(),
            agent_generation: "generation-1".to_string(),
        }),
        expected_process_session_id: String::new(),
        command: Some(proto::execute_command_request::Command::PrepareSwitchover(
            proto::PrepareSwitchoverCommand::default(),
        )),
    };
    assert!(matches!(
        validate_execute_request(&request),
        Err(WireError::MissingField(
            "execute.expected_process_session_id"
        ))
    ));

    request.expected_process_session_id = "session".to_string();
    assert!(matches!(
        validate_execute_request(&request),
        Err(WireError::MissingField("prepare_switchover.operation_id"))
    ));
}

#[test]
fn planned_switchover_preparation_round_trip_preserves_exact_authority() {
    let current = configuration();
    let source = current.members[0].identity.clone();
    let target = current.members[1].identity.clone();
    let request = proto::ExecuteCommandRequest {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: "resource".to_string(),
        target: Some(source.clone().into()),
        expected_process_session_id: "session-1".to_string(),
        command: Some(proto::execute_command_request::Command::PrepareSwitchover(
            proto::PrepareSwitchoverCommand {
                preparation_generation: 2,
                operation_id: "prepare-1".to_string(),
                request_id: "request-1".to_string(),
                local_replica_id: source.replica_id.value(),
                expected_instance_id: source.instance_id.to_string(),
                expected_agent_generation: source.agent_generation.to_string(),
                source: Some(source.clone().into()),
                target: Some(target.clone().into()),
                current_configuration: Some(current.clone().into()),
            },
        )),
    };

    let mut wrong_source = request.clone();
    let Some(proto::execute_command_request::Command::PrepareSwitchover(command)) =
        wrong_source.command.as_mut()
    else {
        unreachable!()
    };
    command.source.as_mut().unwrap().agent_generation = "other-generation".to_string();
    assert!(matches!(
        validate_execute_request(&wrong_source),
        Err(WireError::InvalidAuthority(_))
    ));

    let encoded = request.encode_to_vec();
    let decoded = proto::ExecuteCommandRequest::decode(encoded.as_slice()).unwrap();
    let envelope = normalize_execute_request(decoded).unwrap();
    assert_eq!(envelope.target, source);
    assert_eq!(envelope.expected_process_session_id.as_str(), "session-1");
    match envelope.command {
        kuberic_protocol::command::ProtocolCommand::PrepareSwitchover(command) => {
            assert_eq!(command.preparation_generation, 2);
            assert_eq!(command.request_id.as_str(), "request-1");
            assert_eq!(command.target, target);
            assert_eq!(command.current_configuration, current);
        }
        other => panic!("unexpected command: {other:?}"),
    }
}

#[test]
fn planned_switchover_configuration_round_trip_preserves_handoff() {
    let previous = configuration();
    let source = previous.members[0].identity.clone();
    let target = previous.members[1].identity.clone();
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        target.replica_id,
        previous
            .members
            .iter()
            .cloned()
            .map(|mut member| {
                member.role = if member.identity == target {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                };
                member
            })
            .collect(),
        2,
    );
    let handoff = proto::SwitchoverHandoff {
        preparation_generation: 2,
        preparation_operation_id: "prepare-1".to_string(),
        request_id: "request-1".to_string(),
        source: Some(source.into()),
        target: Some(target.clone().into()),
        starting_configuration_id: previous.configuration_id.to_string(),
        handoff_lsn: 12,
        starting_epoch: Some(previous.epoch.into()),
    };
    let request = proto::ExecuteCommandRequest {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: "resource".to_string(),
        target: Some(target.clone().into()),
        expected_process_session_id: "session-2".to_string(),
        command: Some(
            proto::execute_command_request::Command::EnsureConfiguration(Box::new(
                proto::EnsureConfigurationCommand {
                    operation_id: "install-1".to_string(),
                    previous_configuration: Some(previous.clone().into()),
                    current_configuration: Some(current.clone().into()),
                    previous_epoch: Some(previous.epoch.into()),
                    current_epoch: Some(current.epoch.into()),
                    effective_policy: Some(proto::EffectivePolicy {
                        replica_set_size: 3,
                        write_quorum: 2,
                        read_quorum: 2,
                        failover_delay_seconds: 10,
                    }),
                    local_replica_id: target.replica_id.value(),
                    expected_instance_id: target.instance_id.to_string(),
                    expected_agent_generation: target.agent_generation.to_string(),
                    transition_kind: proto::TransitionKind::PlannedSwitchover as i32,
                    grant_write: false,
                    current_only: false,
                    retire_build_id: String::new(),
                    primary_write_status: proto::AccessStatus::ReconfigurationPending as i32,
                    retire_build_ids: Vec::new(),
                    failover_safe_lsn: None,
                    switchover_handoff: Some(handoff),
                    retire_switchover_preparation_ids: Vec::new(),
                },
            )),
        ),
    };

    let mut restoration = request.clone();
    let source = previous.members[0].identity.clone();
    restoration.target = Some(source.clone().into());
    let Some(proto::execute_command_request::Command::EnsureConfiguration(command)) =
        restoration.command.as_mut()
    else {
        unreachable!()
    };
    command.operation_id = "restore-source".into();
    command.previous_configuration = None;
    command.previous_epoch = None;
    command.current_configuration = Some(previous.clone().into());
    command.current_epoch = Some(previous.epoch.into());
    command.local_replica_id = source.replica_id.value();
    command.expected_instance_id = source.instance_id.to_string();
    command.expected_agent_generation = source.agent_generation.to_string();
    command.retire_switchover_preparation_ids = vec![proto::SwitchoverPreparationId {
        operation_id: "prepare-1".into(),
        generation: 2,
    }];
    let decoded =
        proto::ExecuteCommandRequest::decode(restoration.encode_to_vec().as_slice()).unwrap();
    let envelope = normalize_execute_request(decoded).unwrap();
    assert!(matches!(envelope.command,
        kuberic_protocol::command::ProtocolCommand::EnsureConfiguration(command)
        if command.is_switchover_restoration()));
    let mut unobserved_preparation = restoration.clone();
    let Some(proto::execute_command_request::Command::EnsureConfiguration(command)) =
        unobserved_preparation.command.as_mut()
    else {
        unreachable!()
    };
    command.switchover_handoff = None;
    let envelope = normalize_execute_request(unobserved_preparation).unwrap();
    assert!(matches!(envelope.command,
        kuberic_protocol::command::ProtocolCommand::EnsureConfiguration(command)
        if command.is_switchover_restoration() && command.switchover_handoff.is_none()));
    for fault in [
        "grant",
        "retirement",
        "authority",
        "generation",
        "retired-generation",
    ] {
        let mut invalid = restoration.clone();
        let Some(proto::execute_command_request::Command::EnsureConfiguration(command)) =
            invalid.command.as_mut()
        else {
            unreachable!()
        };
        match fault {
            "grant" => command.primary_write_status = proto::AccessStatus::Granted as i32,
            "generation" => {
                command
                    .switchover_handoff
                    .as_mut()
                    .unwrap()
                    .preparation_generation = 0
            }
            "retired-generation" => command.retire_switchover_preparation_ids[0].generation = 3,
            "retirement" => {
                command.retire_switchover_preparation_ids = vec![proto::SwitchoverPreparationId {
                    operation_id: "other".into(),
                    generation: 2,
                }]
            }
            "authority" => {
                command
                    .switchover_handoff
                    .as_mut()
                    .unwrap()
                    .starting_configuration_id = "other".into()
            }
            _ => unreachable!(),
        }
        assert!(validate_execute_request(&invalid).is_err(), "{fault}");
    }

    let mut wrong_start = request.clone();
    let Some(proto::execute_command_request::Command::EnsureConfiguration(command)) =
        wrong_start.command.as_mut()
    else {
        unreachable!()
    };
    command
        .switchover_handoff
        .as_mut()
        .unwrap()
        .starting_configuration_id = "unrelated".to_string();
    assert!(matches!(
        validate_execute_request(&wrong_start),
        Err(WireError::InvalidAuthority(_))
    ));

    let mut empty_retirement = request.clone();
    let Some(proto::execute_command_request::Command::EnsureConfiguration(command)) =
        empty_retirement.command.as_mut()
    else {
        unreachable!()
    };
    command.retire_switchover_preparation_ids = vec![proto::SwitchoverPreparationId {
        operation_id: String::new(),
        generation: 2,
    }];
    assert!(matches!(
        validate_execute_request(&empty_retirement),
        Err(WireError::InvalidAuthority(_))
    ));

    let encoded = request.encode_to_vec();
    let decoded = proto::ExecuteCommandRequest::decode(encoded.as_slice()).unwrap();
    let envelope = normalize_execute_request(decoded).unwrap();
    match envelope.command {
        kuberic_protocol::command::ProtocolCommand::EnsureConfiguration(command) => {
            assert_eq!(
                command.transition_kind,
                kuberic_protocol::types::TransitionKind::PlannedSwitchover
            );
            assert_eq!(command.switchover_handoff.as_ref().unwrap().handoff_lsn, 12);
            assert_eq!(command.current_configuration, current);
        }
        other => panic!("unexpected command: {other:?}"),
    }
}

#[test]
fn replication_ack_requires_exact_authority_and_consistent_progress() {
    let sender = proto::ReplicaIdentity {
        replica_id: 1,
        instance_id: "pod-1".to_string(),
        agent_generation: "generation-1".to_string(),
    };
    let receiver = proto::ReplicaIdentity {
        replica_id: 2,
        instance_id: "pod-2".to_string(),
        agent_generation: "generation-2".to_string(),
    };
    let ack = proto::ReplicationAck {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        sender: Some(sender.clone()),
        receiver: Some(receiver),
        epoch: Some(proto::Epoch {
            data_loss_number: 0,
            configuration_number: 1,
        }),
        previous_configuration_id: String::new(),
        current_configuration_id: "cfg".to_string(),
        received_lsn: 10,
        applied_lsn: 9,
        committed_lsn: 8,
        ..Default::default()
    };
    assert!(validate_replication_ack(&ack).is_ok());

    let item = proto::ReplicationItem {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        sender: Some(sender),
        epoch: ack.epoch,
        previous_configuration_id: String::new(),
        current_configuration_id: "cfg".to_string(),
        lsn: 10,
        committed_lsn: 8,
        data: vec![1],
        receiver: ack.receiver.clone(),
        ..Default::default()
    };
    assert!(validate_replication_item(&item).is_ok());

    let mut invalid = ack;
    invalid.applied_lsn = 11;
    assert!(matches!(
        validate_replication_ack(&invalid),
        Err(WireError::InvalidAuthority(_))
    ));

    let mut missing_receiver = item;
    missing_receiver.receiver = None;
    assert!(matches!(
        validate_replication_item(&missing_receiver),
        Err(WireError::MissingField("replication_item.receiver"))
    ));
}

#[test]
fn copy_contract_requires_exact_target_and_final_boundary_ack() {
    let sender = proto::ReplicaIdentity {
        replica_id: 1,
        instance_id: "pod-1".to_string(),
        agent_generation: "generation-1".to_string(),
    };
    let receiver = proto::ReplicaIdentity {
        replica_id: 3,
        instance_id: "replacement-pod".to_string(),
        agent_generation: "replacement-generation".to_string(),
    };
    let item = proto::CopyItem {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        build_id: "build".to_string(),
        sender: Some(sender.clone()),
        receiver: Some(receiver.clone()),
        epoch: Some(proto::Epoch {
            data_loss_number: 0,
            configuration_number: 2,
        }),
        current_configuration_id: "cfg".to_string(),
        sequence: 1,
        lsn: 0,
        committed_lsn: 0,
        replication_boundary_lsn: 2,
        final_item: false,
        data: vec![1],
        snapshot_chunk: true,
        ..Default::default()
    };
    assert!(validate_copy_item(&item).is_ok());

    let final_ack = proto::CopyAck {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        build_id: "build".to_string(),
        sender: Some(sender),
        receiver: Some(receiver),
        epoch: item.epoch,
        current_configuration_id: "cfg".to_string(),
        sequence: 3,
        durable_lsn: 2,
        replication_boundary_lsn: 2,
        final_item: true,
        snapshot_chunk: false,
        ..Default::default()
    };
    assert!(validate_copy_ack(&final_ack).is_ok());

    let mut missing_target = item;
    missing_target.receiver = None;
    assert!(matches!(
        validate_copy_item(&missing_target),
        Err(WireError::MissingField("copy_item.receiver"))
    ));

    let mut early_final = final_ack;
    early_final.durable_lsn = 1;
    assert!(matches!(
        validate_copy_ack(&early_final),
        Err(WireError::InvalidAuthority(_))
    ));
}

#[test]
fn initialized_status_rejects_unknown_enums_and_malformed_configuration() {
    let identity = proto::ReplicaIdentity {
        replica_id: 1,
        instance_id: "pod-1".to_string(),
        agent_generation: "generation-1".to_string(),
    };
    let configuration: proto::Configuration = configuration().into();
    let report = proto::AgentStatusReport {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: "resource".to_string(),
        identity: Some(identity),
        process_session_id: "session".to_string(),
        report_sequence: 1,
        role: 999,
        write_status: proto::AccessStatus::Granted as i32,
        epoch: configuration.epoch,
        previous_configuration: None,
        current_configuration: Some(configuration),
        current_progress: 10,
        committed_lsn: 10,
        catch_up_capability: Some(10),
        storage_state: proto::AgentStorageState::Initialized as i32,
        pod_uid: "pod-1".to_string(),
        pvc_uid: "pvc-1".to_string(),
        storage_error: String::new(),
        healthy: true,
        replica_id: 1,
        read_status: proto::AccessStatus::Granted as i32,
        ..Default::default()
    };
    assert!(matches!(
        validate_agent_status_report(&report),
        Err(WireError::InvalidEnum {
            field: "agent_status.role",
            ..
        })
    ));

    let mut unknown_write_status = report.clone();
    unknown_write_status.role = proto::ReplicaRole::Primary as i32;
    unknown_write_status.write_status = 999;
    assert!(matches!(
        validate_agent_status_report(&unknown_write_status),
        Err(WireError::InvalidEnum {
            field: "agent_status.write_status",
            ..
        })
    ));

    let mut certified = report.clone();
    certified.role = proto::ReplicaRole::Primary as i32;
    certified.verified_replication_lsn = Some(10);
    assert!(validate_agent_status_report(&certified).is_ok());
    certified.verified_replication_lsn = Some(11);
    assert!(matches!(
        validate_agent_status_report(&certified),
        Err(WireError::InvalidAuthority(_))
    ));

    let mut deactivation = report.clone();
    deactivation.role = proto::ReplicaRole::Primary as i32;
    deactivation.write_status = proto::AccessStatus::Granted as i32;
    deactivation.deactivated_lsn = Some(7);
    assert!(matches!(
        validate_agent_status_report(&deactivation),
        Err(WireError::InvalidAuthority(_))
    ));
    deactivation.deactivation_epoch = deactivation.epoch;
    assert!(validate_agent_status_report(&deactivation).is_ok());

    let mut malformed = report;
    malformed.role = proto::ReplicaRole::Primary as i32;
    malformed
        .current_configuration
        .as_mut()
        .unwrap()
        .configuration_id = "conflict".to_string();
    assert!(matches!(
        validate_agent_status_report(&malformed),
        Err(WireError::InvalidAuthority(_))
    ));
}

#[test]
fn prepared_switchover_report_survives_protobuf_round_trip() {
    let configuration = configuration();
    let source = configuration.members[0].identity.clone();
    let target = configuration.members[1].identity.clone();
    let report = proto::AgentStatusReport {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: "resource".to_string(),
        identity: Some(source.clone().into()),
        process_session_id: "session-1".to_string(),
        report_sequence: 1,
        role: proto::ReplicaRole::Primary as i32,
        write_status: proto::AccessStatus::Granted as i32,
        epoch: Some(configuration.epoch.into()),
        current_configuration: Some(configuration.clone().into()),
        current_progress: 12,
        verified_replication_lsn: Some(12),
        committed_lsn: 12,
        catch_up_capability: Some(12),
        storage_state: proto::AgentStorageState::Initialized as i32,
        pod_uid: source.instance_id.to_string(),
        pvc_uid: "pvc-1".to_string(),
        healthy: true,
        replica_id: source.replica_id.value(),
        read_status: proto::AccessStatus::Granted as i32,
        prepared_switchover: Some(proto::SwitchoverHandoff {
            preparation_generation: 2,
            preparation_operation_id: "prepare-1".to_string(),
            request_id: "request-1".to_string(),
            source: Some(source.into()),
            target: Some(target.into()),
            starting_configuration_id: configuration.configuration_id.to_string(),
            handoff_lsn: 12,
            starting_epoch: Some(configuration.epoch.into()),
        }),
        ..Default::default()
    };

    let encoded = report.encode_to_vec();
    let decoded = proto::AgentStatusReport::decode(encoded.as_slice()).unwrap();
    let observation = normalize_agent_status_report(decoded).unwrap();
    let kuberic_protocol::observation::AgentObservation::Report(report) = observation else {
        panic!("expected initialized report");
    };
    assert_eq!(
        report
            .prepared_switchover
            .as_ref()
            .unwrap()
            .preparation_operation_id
            .as_str(),
        "prepare-1"
    );
}

#[test]
fn ensure_request_rejects_previous_configuration_outside_frozen_policy() {
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 5),
        ReplicaId::new(1),
        vec![ConfigurationMember {
            identity: ReplicaIdentity {
                replica_id: ReplicaId::new(1),
                instance_id: ReplicaInstanceId::new("pod-1"),
                agent_generation: AgentGeneration::new("generation-1"),
            },
            role: ReplicaRole::Primary,
        }],
        1,
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 6),
        ReplicaId::new(1),
        configuration().members,
        2,
    );
    let target = current.members[0].identity.clone();
    let request = proto::ExecuteCommandRequest {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: "resource".to_string(),
        target: Some(target.clone().into()),
        expected_process_session_id: "session".to_string(),
        command: Some(
            proto::execute_command_request::Command::EnsureConfiguration(Box::new(
                proto::EnsureConfigurationCommand {
                    operation_id: "operation".to_string(),
                    previous_configuration: Some(previous.clone().into()),
                    current_configuration: Some(current.clone().into()),
                    previous_epoch: Some(previous.epoch.into()),
                    current_epoch: Some(current.epoch.into()),
                    effective_policy: Some(proto::EffectivePolicy {
                        replica_set_size: 3,
                        write_quorum: 2,
                        read_quorum: 2,
                        failover_delay_seconds: 10,
                    }),
                    local_replica_id: target.replica_id.value(),
                    expected_instance_id: target.instance_id.to_string(),
                    expected_agent_generation: target.agent_generation.to_string(),
                    transition_kind: proto::TransitionKind::Failover as i32,
                    grant_write: false,
                    current_only: false,
                    retire_build_id: String::new(),
                    primary_write_status: proto::AccessStatus::ReconfigurationPending as i32,
                    retire_build_ids: Vec::new(),
                    failover_safe_lsn: Some(0),
                    switchover_handoff: None,
                    retire_switchover_preparation_ids: Vec::new(),
                },
            )),
        ),
    };

    assert!(matches!(
        validate_execute_request(&request),
        Err(WireError::InvalidAuthority(_))
    ));
}

#[test]
fn ensure_request_rejects_regressing_pc_cc_relationship() {
    let previous = ConfigurationDescriptor::new(
        Epoch::new(0, 5),
        ReplicaId::new(1),
        configuration().members,
        2,
    );
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 4),
        ReplicaId::new(1),
        previous.members.clone(),
        2,
    );
    let target = current.members[0].identity.clone();
    let request = proto::ExecuteCommandRequest {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: "resource".to_string(),
        target: Some(target.clone().into()),
        expected_process_session_id: "session".to_string(),
        command: Some(
            proto::execute_command_request::Command::EnsureConfiguration(Box::new(
                proto::EnsureConfigurationCommand {
                    operation_id: "operation".to_string(),
                    previous_configuration: Some(previous.clone().into()),
                    current_configuration: Some(current.clone().into()),
                    previous_epoch: Some(previous.epoch.into()),
                    current_epoch: Some(current.epoch.into()),
                    effective_policy: Some(proto::EffectivePolicy {
                        replica_set_size: 3,
                        write_quorum: 2,
                        read_quorum: 2,
                        failover_delay_seconds: 10,
                    }),
                    local_replica_id: target.replica_id.value(),
                    expected_instance_id: target.instance_id.to_string(),
                    expected_agent_generation: target.agent_generation.to_string(),
                    transition_kind: proto::TransitionKind::Failover as i32,
                    grant_write: false,
                    current_only: false,
                    retire_build_id: String::new(),
                    primary_write_status: proto::AccessStatus::ReconfigurationPending as i32,
                    retire_build_ids: Vec::new(),
                    failover_safe_lsn: Some(0),
                    switchover_handoff: None,
                    retire_switchover_preparation_ids: Vec::new(),
                },
            )),
        ),
    };

    assert!(matches!(
        validate_execute_request(&request),
        Err(WireError::InvalidAuthority(_))
    ));
}
