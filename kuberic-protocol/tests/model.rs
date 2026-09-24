use std::collections::BTreeMap;

use kuberic_protocol::observation::{
    AgentObservation, AgentReport, DesiredState, ObservationSnapshot, ReplicaObservation,
    ReplicaObservationKey, RoutingObservation,
};
use kuberic_protocol::types::{
    AcceptedStatus, AcceptedTopology, AccessStatus, AgentGeneration, ConfigurationDescriptor,
    ConfigurationMember, EffectivePolicy, Epoch, OperationId, PodUid, ProcessSessionId,
    ProvisioningIntent, PvcUid, ReplicaId, ReplicaIdentity, ReplicaInstanceId, ReplicaRole,
    ResourceUid, TransitionIntent, TransitionKind, derive_transition_id,
};
use kuberic_protocol::validation::{ValidationError, validate_snapshot, validate_status};

fn identity(replica_id: i64, incarnation: u64) -> ReplicaIdentity {
    ReplicaIdentity {
        replica_id: ReplicaId::new(replica_id),
        instance_id: ReplicaInstanceId::new(format!("pod-{replica_id}-{incarnation}")),
        agent_generation: AgentGeneration::new(format!("generation-{replica_id}-{incarnation}")),
    }
}

fn policy(replica_set_size: u32) -> EffectivePolicy {
    EffectivePolicy::fixed(replica_set_size, 7).unwrap()
}

fn configuration(
    replica_set_size: u32,
    epoch: Epoch,
    primary_id: i64,
    incarnations: &[u64],
) -> ConfigurationDescriptor {
    let effective_policy = policy(replica_set_size);
    ConfigurationDescriptor::new(
        epoch,
        ReplicaId::new(primary_id),
        (1..=i64::from(replica_set_size))
            .map(|replica_id| ConfigurationMember {
                identity: identity(replica_id, incarnations[replica_id as usize - 1]),
                role: if replica_id == primary_id {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        effective_policy.write_quorum,
    )
}

fn stable_status(
    configuration: ConfigurationDescriptor,
    effective_policy: EffectivePolicy,
) -> AcceptedStatus {
    AcceptedStatus {
        initialized: true,
        observed_generation: 1,
        effective_policy: Some(effective_policy),
        topology: Some(AcceptedTopology { configuration }),
        ..AcceptedStatus::default()
    }
}

fn transition_status(
    previous: &ConfigurationDescriptor,
    current: ConfigurationDescriptor,
    effective_policy: EffectivePolicy,
    kind: TransitionKind,
) -> AcceptedStatus {
    let resource_uid = ResourceUid::new("model-resource");
    let build_id = (kind == TransitionKind::Replacement)
        .then(|| OperationId::new(format!("build-{}", current.epoch.configuration_number)));
    AcceptedStatus {
        transition: Some(TransitionIntent {
            transition_id: derive_transition_id(&resource_uid, kind, &current.configuration_id),
            kind,
            spec_generation: 1,
            effective_policy: effective_policy.clone(),
            previous_configuration_id: Some(previous.configuration_id.clone()),
            current_configuration: current,
            election_lsn: (kind == TransitionKind::Failover).then_some(100),
            build_id,
            repair: None,
        }),
        ..stable_status(previous.clone(), effective_policy)
    }
}

#[test]
fn generated_transition_traces_preserve_authority_invariants() {
    for replica_set_size in [3_u32, 5] {
        let effective_policy = policy(replica_set_size);
        let mut incarnations = vec![1; replica_set_size as usize];
        let mut accepted = configuration(replica_set_size, Epoch::new(0, 1), 1, &incarnations);
        let mut status = stable_status(accepted.clone(), effective_policy.clone());
        validate_status(&status).unwrap();

        for step in 2..=7 {
            let kind = if step % 2 == 0 {
                TransitionKind::Failover
            } else {
                TransitionKind::Replacement
            };
            let primary_id = if kind == TransitionKind::Failover {
                accepted.primary_id.value() % i64::from(replica_set_size) + 1
            } else {
                accepted.primary_id.value()
            };
            if kind == TransitionKind::Replacement {
                let replace = (1..=i64::from(replica_set_size))
                    .find(|replica_id| *replica_id != primary_id)
                    .unwrap();
                incarnations[replace as usize - 1] += 1;
            }
            let current = configuration(
                replica_set_size,
                Epoch::new(0, step),
                primary_id,
                &incarnations,
            );
            status = transition_status(&accepted, current.clone(), effective_policy.clone(), kind);
            validate_status(&status).unwrap();

            let transition = status.transition.as_ref().unwrap();
            assert_eq!(
                transition.current_configuration.members.len(),
                replica_set_size as usize
            );
            assert_eq!(transition.effective_policy, effective_policy);
            assert!(transition.current_configuration.epoch > accepted.epoch);
            assert_eq!(
                transition.previous_configuration_id.as_ref(),
                Some(&accepted.configuration_id)
            );

            let mut conflicting = status.clone();
            let replaced = accepted
                .members
                .iter()
                .find(|member| member.identity.replica_id != accepted.primary_id)
                .unwrap()
                .identity
                .clone();
            conflicting.provisioning = Some(ProvisioningIntent {
                replaces: replaced,
                pod_uid: PodUid::new("conflicting-pod"),
                pvc_uid: PvcUid::new("conflicting-pvc"),
                operation_id: OperationId::new("conflicting-provisioning"),
            });
            assert_eq!(
                validate_status(&conflicting),
                Err(ValidationError::ProvisioningAndTransition)
            );

            let mut changed_policy = status.clone();
            changed_policy
                .transition
                .as_mut()
                .unwrap()
                .effective_policy
                .failover_delay_seconds += 1;
            assert_eq!(
                validate_status(&changed_policy),
                Err(ValidationError::TransitionPolicyMismatch)
            );

            let mut accepted_too_early = status.clone();
            accepted_too_early.topology = Some(AcceptedTopology {
                configuration: current.clone(),
            });
            assert!(matches!(
                validate_status(&accepted_too_early),
                Err(ValidationError::PreviousConfigurationMismatch { .. })
            ));

            accepted = current;
            status = stable_status(accepted.clone(), effective_policy.clone());
            validate_status(&status).unwrap();
        }
    }
}

#[test]
fn generated_report_sets_never_validate_two_granted_writers() {
    for replica_set_size in [1_u32, 3, 5] {
        let effective_policy = policy(replica_set_size);
        let configuration = configuration(
            replica_set_size,
            Epoch::new(0, 4),
            1,
            &vec![1; replica_set_size as usize],
        );
        let writer_masks = 0_u64..(1_u64 << replica_set_size);
        for writer_mask in writer_masks {
            let mut replicas = BTreeMap::new();
            for (index, member) in configuration.members.iter().enumerate() {
                let claims_write = writer_mask & (1 << index) != 0;
                replicas.insert(
                    ReplicaObservationKey::new(
                        member.identity.replica_id,
                        member.identity.instance_id.clone(),
                    ),
                    ReplicaObservation {
                        kubernetes: None,
                        agent: AgentObservation::Report(Box::new(AgentReport {
                            protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                            resource_uid: ResourceUid::new("model-resource"),
                            identity: member.identity.clone(),
                            process_session_id: ProcessSessionId::new(format!(
                                "session-{}",
                                member.identity.replica_id
                            )),
                            report_sequence: 1,
                            role: if claims_write {
                                ReplicaRole::Primary
                            } else {
                                member.role
                            },
                            read_status: if claims_write {
                                AccessStatus::Granted
                            } else {
                                AccessStatus::NotPrimary
                            },
                            write_status: if claims_write {
                                AccessStatus::Granted
                            } else {
                                AccessStatus::NotPrimary
                            },
                            healthy: true,
                            epoch: configuration.epoch,
                            current_configuration: Some(configuration.clone()),
                            current_progress: 10,
                            verified_replication_lsn: Some(10),
                            committed_lsn: 10,
                            ..AgentReport::default()
                        })),
                    },
                );
            }
            let snapshot = ObservationSnapshot {
                resource_uid: ResourceUid::new("model-resource"),
                resource_version: "1".to_string(),
                desired: DesiredState {
                    generation: 1,
                    replicas: replica_set_size,
                    image: "example:v1".to_string(),
                    failover_delay_seconds: effective_policy.failover_delay_seconds,
                },
                status: stable_status(configuration.clone(), effective_policy.clone()),
                replicas,
                previous_report_watermarks: BTreeMap::new(),
                durable_storage_evidence: true,
                supporting_resources_ready: true,
                routing: RoutingObservation::default(),
                observation_failures: Vec::new(),
                now_unix_seconds: 100,
            };
            if validate_snapshot(&snapshot).is_ok() {
                assert!(
                    writer_mask.count_ones() <= 1,
                    "validated more than one granted writer for mask {writer_mask:b}"
                );
                if writer_mask.count_ones() == 1 {
                    assert_eq!(writer_mask, 1, "only the configured primary may write");
                }
            }
        }
    }
}

#[test]
fn generated_transitions_reject_non_monotonic_epochs() {
    for configuration_number in 1..=5 {
        let effective_policy = policy(3);
        let accepted = configuration(3, Epoch::new(0, configuration_number), 1, &[1, 1, 1]);
        for next in 0..=configuration_number {
            let current = configuration(3, Epoch::new(0, next), 2, &[1, 1, 1]);
            let status = transition_status(
                &accepted,
                current,
                effective_policy.clone(),
                TransitionKind::Failover,
            );
            assert_eq!(
                validate_status(&status),
                Err(ValidationError::TransitionEpochNotNewer)
            );
        }
    }
}
