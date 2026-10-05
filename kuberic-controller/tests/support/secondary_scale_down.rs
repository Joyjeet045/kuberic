use super::*;
use kuberic_runtime::protocol::command::{KubernetesChange, ScaleDownResource};
use kuberic_runtime::protocol::observation::ExactResourceObservation;
use kuberic_runtime::protocol::types::*;

fn enabled() -> EvaluationConfig {
    EvaluationConfig {
        enable_secondary_scale_down: true,
        ..config()
    }
}

pub(super) fn fixture(count: u32, desired: u32) -> RawObservation {
    let mut raw = raw(desired);
    raw.set.spec.failover_delay_seconds = 10;
    raw.set.metadata.generation = Some(2);
    let policy = EffectivePolicy::fixed(count, 10).unwrap();
    let members = (1..=count)
        .map(|id| {
            let replica_id = ReplicaId::new(i64::from(id));
            let pod_uid = format!("pod-uid-{id}");
            let pvc_uid = format!("pvc-uid-{id}");
            let identity = ReplicaIdentity {
                replica_id,
                instance_id: ReplicaInstanceId::new(&pod_uid),
                agent_generation: derive_agent_generation(&derive_initialization_id(
                    &ResourceUid::new(UID),
                    replica_id,
                    &PodUid::new(&pod_uid),
                    &PvcUid::new(&pvc_uid),
                )),
            };
            let mut labels = labels(replica_id);
            labels.insert(INSTANCE_LABEL.into(), pod_uid.clone());
            raw.pods.push(Pod {
                metadata: kube::core::ObjectMeta {
                    name: Some(format!("db-{id}")),
                    uid: Some(pod_uid.clone()),
                    resource_version: Some("10".into()),
                    labels: Some(labels.clone()),
                    ..Default::default()
                },
                spec: Some(PodSpec {
                    containers: vec![k8s_openapi::api::core::v1::Container {
                        name: "application".into(),
                        image: Some(raw.set.spec.image.clone()),
                        ..Default::default()
                    }],
                    volumes: Some(vec![Volume {
                        name: "data".into(),
                        persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                            claim_name: format!("db-{id}-data"),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }]),
                    ..Default::default()
                }),
                status: Some(PodStatus {
                    pod_ip: Some("127.0.0.1".into()),
                    conditions: Some(vec![PodCondition {
                        type_: "Ready".into(),
                        status: "True".into(),
                        ..Default::default()
                    }]),
                    ..Default::default()
                }),
            });
            raw.pvcs.push(PersistentVolumeClaim {
                metadata: kube::core::ObjectMeta {
                    name: Some(format!("db-{id}-data")),
                    uid: Some(pvc_uid),
                    resource_version: Some("11".into()),
                    labels: Some(labels.clone()),
                    ..Default::default()
                },
                ..Default::default()
            });
            raw.services.push(Service {
                metadata: kube::core::ObjectMeta {
                    name: Some(derive_replica_endpoint_name(
                        &ResourceUid::new(UID),
                        &identity,
                    )),
                    uid: Some(format!("endpoint-{id}")),
                    resource_version: Some("12".into()),
                    labels: Some(labels),
                    ..Default::default()
                },
                spec: Some(ServiceSpec {
                    selector: Some(BTreeMap::from([(INSTANCE_LABEL.into(), pod_uid)])),
                    ports: Some(vec![
                        ServicePort {
                            name: Some("control".into()),
                            port: 50051,
                            ..Default::default()
                        },
                        ServicePort {
                            name: Some("replication".into()),
                            port: 50052,
                            ..Default::default()
                        },
                    ]),
                    ..Default::default()
                }),
                ..Default::default()
            });
            ConfigurationMember {
                identity,
                role: if id == 1 {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            }
        })
        .collect();
    let configuration = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        ReplicaId::new(1),
        members,
        policy.write_quorum,
    );
    for member in &configuration.members {
        let mut report = initialized_report(member.identity.clone(), configuration.clone());
        report.replica_id = member.identity.replica_id.value();
        report.role = if member.role == ReplicaRole::Primary {
            proto::ReplicaRole::Primary as i32
        } else {
            proto::ReplicaRole::ActiveSecondary as i32
        };
        report.write_status = if member.role == ReplicaRole::Primary {
            proto::AccessStatus::Granted as i32
        } else {
            proto::AccessStatus::NotPrimary as i32
        };
        report.verified_replication_lsn = Some(5);
        report.pod_uid = member.identity.instance_id.to_string();
        report.pvc_uid = format!("pvc-uid-{}", member.identity.replica_id);
        report.process_session_id = format!("session-{}", member.identity.replica_id);
        raw.agents.insert(
            ReplicaObservationKey::new(
                member.identity.replica_id,
                member.identity.instance_id.clone(),
            ),
            RawAgentObservation::Report(Box::new(report)),
        );
    }
    raw.set.status = Some(KubericSetStatus {
        authority: AcceptedStatus {
            initialized: true,
            observed_generation: 1,
            effective_policy: Some(policy),
            topology: Some(AcceptedTopology { configuration }),
            ..Default::default()
        },
    });
    raw.services.push(write_service("pod-uid-1"));
    raw
}

pub(super) async fn apply_command(api: &InMemoryClusterApi, command: &ProtocolCommand) {
    let mut raw = api.observation().await;
    let target = match command {
        ProtocolCommand::PrepareSecondaryRemoval(c) => c.intent.primary.clone(),
        ProtocolCommand::EnsureConfiguration(c) => ReplicaIdentity {
            replica_id: c.local_replica_id,
            instance_id: c.expected_instance_id.clone(),
            agent_generation: c.expected_agent_generation.clone(),
        },
        ProtocolCommand::AcceptSecondaryRemovalCommit(c) => c.target.clone(),
        ProtocolCommand::RetireReplica(c) => c.committed.evidence.preparation.intent.target.clone(),
        other => panic!("unexpected removal command {other:?}"),
    };
    let RawAgentObservation::Report(report) = raw
        .agents
        .get_mut(&ReplicaObservationKey::new(
            target.replica_id,
            target.instance_id.clone(),
        ))
        .unwrap()
    else {
        panic!("exact target report")
    };
    match command {
        ProtocolCommand::PrepareSecondaryRemoval(c) => {
            report.read_status = proto::AccessStatus::ReconfigurationPending as i32;
            report.write_status = proto::AccessStatus::ReconfigurationPending as i32;
            report.prepared_secondary_removal = Some(
                SecondaryRemovalPreparation {
                    intent: c.intent.clone(),
                    operation_id: c.operation_id.clone(),
                    process_session_id: ProcessSessionId::new(&report.process_session_id),
                    report_sequence: report.report_sequence,
                    boundary_lsn: report.verified_replication_lsn.unwrap(),
                }
                .into(),
            );
            report.retained_operation_id = c.operation_id.to_string();
        }
        ProtocolCommand::EnsureConfiguration(c) => {
            if report.current_configuration.as_ref()
                != Some(&c.current_configuration.clone().into())
                && c.secondary_removal_evidence.is_none()
            {
                report.secondary_removal_evidence = None;
                report.accepted_secondary_removal = None;
                report.prepared_secondary_removal = None;
            }
            report.epoch = Some(c.current_epoch.into());
            report.previous_configuration = c.previous_configuration.clone().map(Into::into);
            report.current_configuration = Some(c.current_configuration.clone().into());
            if let Some(evidence) = &c.secondary_removal_evidence {
                if report.secondary_removal_evidence.as_ref() != Some(&evidence.clone().into()) {
                    report.accepted_secondary_removal = None;
                }
                report.secondary_removal_evidence = Some(evidence.clone().into());
            }
            report.role = if c.current_configuration.primary_id == c.local_replica_id {
                proto::ReplicaRole::Primary as i32
            } else {
                proto::ReplicaRole::ActiveSecondary as i32
            };
            report.write_status = if c.primary_write_status == AccessStatus::Granted {
                proto::AccessStatus::Granted as i32
            } else if c.local_replica_id == c.current_configuration.primary_id {
                proto::AccessStatus::ReconfigurationPending as i32
            } else {
                proto::AccessStatus::NotPrimary as i32
            };
            report.read_status = report.write_status;
            report.retained_operation_id = c.operation_id.to_string();
            report.catch_up_complete = true;
            report.catch_up_boundary = c
                .secondary_removal_evidence
                .as_ref()
                .map(|e| e.preparation.boundary_lsn);
            report.current_configuration_quorum_progress = report.current_progress;
            if c.transition_kind == TransitionKind::Failover {
                report.deactivation_epoch = Some(c.current_epoch.into());
                report.deactivated_lsn = Some(report.current_progress);
                report.catch_up_boundary = c.failover_safe_lsn;
            }
            report.pending_operation_id.clear();
        }
        ProtocolCommand::AcceptSecondaryRemovalCommit(c) => {
            report.accepted_secondary_removal = Some(c.committed.clone().into());
            report.prepared_secondary_removal = None;
            report.pending_operation_id.clear();
        }
        ProtocolCommand::RetireReplica(c) => {
            let intent = &c.committed.evidence.preparation.intent;
            report.retired_replica = Some(
                ReplicaRetirementReport {
                    intent: intent.clone(),
                    operation_id: c.operation_id.clone(),
                    process_session_id: ProcessSessionId::new(&report.process_session_id),
                    report_sequence: report.report_sequence,
                    epoch: intent.current_configuration.epoch,
                    role: ReplicaRole::None,
                    read_status: AccessStatus::NotPrimary,
                    write_status: AccessStatus::NotPrimary,
                    application_closed: true,
                    peers_fenced: true,
                }
                .into(),
            );
            report.role = proto::ReplicaRole::None as i32;
            report.epoch = Some(intent.current_configuration.epoch.into());
            report.read_status = proto::AccessStatus::NotPrimary as i32;
            report.write_status = proto::AccessStatus::NotPrimary as i32;
            report.previous_configuration = None;
            report.current_configuration = None;
            report.verified_replication_lsn = None;
            report.secondary_removal_evidence = None;
            report.accepted_secondary_removal = None;
            report.prepared_secondary_removal = None;
            report.retained_operation_id = c.operation_id.to_string();
            report.pending_operation_id.clear();
        }
        _ => unreachable!(),
    }
    api.set_observation(raw).await;
}

async fn tick(api: &Arc<InMemoryClusterApi>) -> (ReconcileKind, Vec<EffectRecord>) {
    let mut restored = api.observation().await;
    restored.set = serde_json::from_value(serde_json::to_value(&restored.set).unwrap()).unwrap();
    api.set_observation(restored).await;
    refresh_switchover_reports(api).await;
    let before = api.effects().await.len();
    let observed = api.observation_count().await;
    let action = Reconciler::new(api.clone(), enabled())
        .reconcile("tests", "db")
        .await
        .unwrap();
    assert_eq!(api.observation_count().await, observed + 1);
    let effects = api.effects().await[before..].to_vec();
    assert!(
        effects.len() <= 1,
        "one effect per fresh observation: {effects:?}; {:?}",
        api.observation()
            .await
            .set
            .status
            .unwrap()
            .authority
            .conditions
    );
    for effect in &effects {
        if let EffectRecord::Execute(command) = effect {
            apply_command(api, command).await;
        }
    }
    (action.kind, effects)
}

async fn next_plan(api: &InMemoryClusterApi) -> (RawObservation, Plan) {
    let raw = api.observe("tests", "db").await.unwrap();
    let plan = evaluate(
        &normalize(raw.clone(), BTreeMap::new()).unwrap(),
        &enabled(),
    );
    (raw, plan)
}

async fn finish(api: &Arc<InMemoryClusterApi>) {
    for _ in 0..250 {
        if tick(api).await.0 == ReconcileKind::Stable {
            return;
        }
    }
    panic!("did not converge: {:?}", next_plan(api).await.1);
}

async fn at_delete(
    api: &Arc<InMemoryClusterApi>,
    resource: ScaleDownResource,
) -> (RawObservation, KubernetesChange) {
    for _ in 0..100 {
        let (raw, plan) = next_plan(api).await;
        if let Plan::Apply { changes } = plan
            && let Some(change @ KubernetesChange::DeleteScaleDownResource { resource: kind, .. }) =
                changes.first()
            && *kind == resource
        {
            return (raw, change.clone());
        }
        tick(api).await;
    }
    panic!("no {resource:?} deletion: {:?}", next_plan(api).await.1)
}

fn metadata<'a>(
    raw: &'a mut RawObservation,
    resource: ScaleDownResource,
    name: &str,
) -> &'a mut kube::core::ObjectMeta {
    match resource {
        ScaleDownResource::Pod => {
            &mut raw
                .pods
                .iter_mut()
                .find(|o| o.name_any() == name)
                .unwrap()
                .metadata
        }
        ScaleDownResource::Pvc => {
            &mut raw
                .pvcs
                .iter_mut()
                .find(|o| o.name_any() == name)
                .unwrap()
                .metadata
        }
        ScaleDownResource::Endpoint => {
            &mut raw
                .services
                .iter_mut()
                .find(|o| o.name_any() == name)
                .unwrap()
                .metadata
        }
    }
}

#[tokio::test]
async fn retained_read_quorum_preflight_preserves_service_until_the_same_member_heals() {
    for failure in ["unreachable", "permanent", "missing"] {
        let mut original = fixture(3, 2);
        let key = original
            .agents
            .keys()
            .find(|k| k.replica_id == ReplicaId::new(2))
            .unwrap()
            .clone();
        let retained = original.agents[&key].clone();
        match failure {
            "unreachable" => {
                original.agents.insert(
                    key.clone(),
                    RawAgentObservation::Unavailable {
                        message: "retained member unavailable".into(),
                    },
                );
            }
            "permanent" => {
                let RawAgentObservation::Report(r) = original.agents.get_mut(&key).unwrap() else {
                    unreachable!()
                };
                r.reported_fault = proto::FaultType::Permanent as i32;
            }
            _ => {
                original.agents.remove(&key);
            }
        }
        let before = original.clone();
        let api = Arc::new(InMemoryClusterApi::new(original));
        for attempt in 0..3 {
            let (kind, effects) = tick(&api).await;
            assert_eq!(kind, ReconcileKind::Waiting, "{failure}");
            assert!(
                effects
                    .iter()
                    .all(|e| matches!(e, EffectRecord::ReplaceStatus))
            );
            if attempt > 0 {
                assert!(
                    effects.is_empty(),
                    "unchanged condition must not rewrite status"
                );
            }
            let observed = api.observation().await;
            let status = &observed.set.status.as_ref().unwrap().authority;
            assert!(
                status
                    .conditions
                    .iter()
                    .any(|c| c.reason == "ScaleDownRetainedReadQuorumUnavailable")
            );
            assert!(status.transition.is_none() && status.provisioning.is_none());
            assert!(status.pending_replacement_cleanup.is_none());
            assert_eq!(
                status.topology,
                before.set.status.as_ref().unwrap().authority.topology
            );
            assert_eq!(
                status.effective_policy,
                before
                    .set
                    .status
                    .as_ref()
                    .unwrap()
                    .authority
                    .effective_policy
            );
            assert_eq!(
                observed.services, before.services,
                "write routing stays live"
            );
            let primary = observed
                .agents
                .iter()
                .find(|(key, _)| key.replica_id == ReplicaId::new(1))
                .unwrap()
                .1;
            let RawAgentObservation::Report(primary) = primary else {
                unreachable!()
            };
            assert_eq!(primary.write_status, proto::AccessStatus::Granted as i32);
            assert!(primary.prepared_secondary_removal.is_none());
            assert!(primary.pending_operation_id.is_empty());
            assert_eq!(observed.pods, before.pods);
            assert_eq!(observed.pvcs, before.pvcs);
        }
        let mut healed = api.observation().await;
        healed.agents.insert(key, retained);
        api.set_observation(healed).await;
        tick(&api).await;
        let admitted = api.observation().await;
        assert_eq!(
            admitted
                .set
                .status
                .as_ref()
                .unwrap()
                .authority
                .transition
                .as_ref()
                .unwrap()
                .secondary_scale_down
                .as_ref()
                .unwrap()
                .target
                .replica_id,
            ReplicaId::new(3)
        );
        assert_eq!(admitted.services, before.services);
        finish(&api).await;
        assert_eq!(
            api.observation()
                .await
                .set
                .status
                .unwrap()
                .authority
                .topology
                .unwrap()
                .configuration
                .members
                .len(),
            2
        );
    }
}

#[tokio::test]
async fn healthy_reductions_are_sequential_exact_and_quiescent() {
    for (count, desired) in [(3, 2), (2, 1), (3, 1), (5, 2)] {
        let original = fixture(count, desired);
        let retained_pods = original.pods[..desired as usize].to_vec();
        let retained_pvcs = original.pvcs[..desired as usize].to_vec();
        let api = Arc::new(InMemoryClusterApi::new(original));
        finish(&api).await;
        let completed = api.observation().await;
        let status = completed.set.status.unwrap().authority;
        let topology = status.topology.unwrap().configuration;
        assert_eq!(topology.members.len(), desired as usize);
        assert_eq!(topology.primary_id, ReplicaId::new(1));
        assert_eq!(
            topology.epoch,
            Epoch::new(0, 1 + i64::from(count - desired))
        );
        assert!(status.transition.is_none() && status.secondary_scale_down_cleanup.is_none());
        assert!(status.last_secondary_removal.is_some());
        assert_eq!(completed.pods, retained_pods);
        assert_eq!(completed.pvcs, retained_pvcs);
        let effects = api.effects().await;
        let prepared = effects
            .iter()
            .filter_map(|e| {
                if let EffectRecord::Execute(ProtocolCommand::PrepareSecondaryRemoval(c)) = e {
                    Some(c.intent.target.replica_id.value())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(
            prepared,
            ((desired + 1)..=count)
                .rev()
                .map(i64::from)
                .collect::<Vec<_>>()
        );
        let cleanup = effects
            .iter()
            .filter_map(|e| match e {
                EffectRecord::DeleteScaleDownResource { resource, .. } => {
                    Some(format!("{resource:?}"))
                }
                EffectRecord::Execute(ProtocolCommand::RetireReplica(_)) => Some("retire".into()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            cleanup,
            ["Endpoint", "retire", "Pod", "Pvc"].repeat((count - desired) as usize)
        );
        for _ in 0..3 {
            assert_eq!(tick(&api).await.0, ReconcileKind::Stable);
        }
        assert_eq!(api.effects().await, effects);
    }
}

#[tokio::test]
async fn completed_reduction_then_real_failover_corrects_returning_old_primary() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(4, 3)));
    finish(&api).await;
    let mut raw = api.observation().await;
    let topology = raw
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .topology
        .clone()
        .unwrap();
    let old_primary = topology.configuration.members[0].identity.clone();
    let key = ReplicaObservationKey::new(old_primary.replica_id, old_primary.instance_id.clone());
    let RawAgentObservation::Report(mut returning) = raw.agents.remove(&key).unwrap() else {
        unreachable!()
    };
    returning.write_status = proto::AccessStatus::ReconfigurationPending as i32;
    raw.set.status.as_mut().unwrap().authority.primary_failure = Some(PrimaryFailureObservation {
        primary: old_primary,
        started_at_unix_seconds: 0,
    });
    raw.services
        .iter_mut()
        .find(|s| s.name_any() == "db-write")
        .unwrap()
        .spec
        .as_mut()
        .unwrap()
        .selector = None;
    api.set_observation(raw).await;
    for _ in 0..80 {
        let status = api.observation().await.set.status.unwrap().authority;
        if status.transition.is_none() && status.topology != Some(topology.clone()) {
            break;
        }
        assert_ne!(tick(&api).await.0, ReconcileKind::Unsafe);
    }
    let mut raw = api.observation().await;
    let accepted = raw
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .clone();
    assert_eq!(accepted.primary_id, ReplicaId::new(2));
    assert!(accepted.epoch > topology.configuration.epoch);
    assert!(
        raw.set
            .status
            .as_ref()
            .unwrap()
            .authority
            .transition
            .is_none()
    );
    raw.agents
        .insert(key, RawAgentObservation::Report(returning));
    *raw.services
        .iter_mut()
        .find(|s| s.name_any() == "db-write")
        .unwrap() = write_service("pod-uid-2");
    api.set_observation(raw).await;
    let (_, plan) = next_plan(&api).await;
    assert!(
        matches!(plan, Plan::Execute { command: ProtocolCommand::EnsureConfiguration(ref c) } if c.local_replica_id.value() == 1),
        "{plan:?}"
    );
    finish(&api).await;
    assert_eq!(
        api.observation()
            .await
            .set
            .status
            .unwrap()
            .authority
            .topology
            .unwrap()
            .configuration,
        accepted
    );
}

#[tokio::test]
async fn historical_removal_survives_new_authority_and_status_reload() {
    historical_removal_return(false, false).await;
}

#[tokio::test]
async fn historical_removal_local_acceptance_precedes_correction_after_cleanup() {
    historical_removal_return(true, false).await;
}

#[tokio::test]
async fn historical_removal_resumes_pending_acceptance_before_newer_correction() {
    historical_removal_return(true, true).await;
}

async fn historical_removal_return(unaccepted: bool, pending: bool) {
    for replacement in [false, true] {
        let api = Arc::new(InMemoryClusterApi::new(if unaccepted {
            fixture(6, 5)
        } else {
            fixture(4, 3)
        }));
        let mut returning = None;
        if unaccepted {
            for _ in 0..100 {
                let (_, plan) = next_plan(&api).await;
                let boundary = matches!(plan, Plan::Execute {
                    command: ProtocolCommand::EnsureConfiguration(ref c)
                } if c.local_replica_id.value() == 5 && c.current_only);
                tick(&api).await;
                if boundary {
                    let mut raw = api.observation().await;
                    let key = raw
                        .agents
                        .keys()
                        .find(|k| k.replica_id.value() == 5)
                        .unwrap()
                        .clone();
                    returning = Some((key.clone(), raw.agents.remove(&key).unwrap()));
                    api.set_observation(raw).await;
                    break;
                }
            }
            assert!(returning.is_some());
            for _ in 0..100 {
                if api
                    .observation()
                    .await
                    .set
                    .status
                    .as_ref()
                    .unwrap()
                    .authority
                    .last_secondary_removal
                    .is_some()
                {
                    break;
                }
                tick(&api).await;
            }
        } else {
            finish(&api).await;
        }
        let mut raw = api.observation().await;
        assert!(
            raw.set
                .status
                .as_ref()
                .unwrap()
                .authority
                .secondary_scale_down_cleanup
                .is_none()
        );
        let receipt = raw
            .set
            .status
            .as_ref()
            .unwrap()
            .authority
            .last_secondary_removal
            .clone()
            .unwrap();
        if let Some((key, report)) = returning {
            raw.agents.insert(key, report);
        }
        let previous = raw
            .set
            .status
            .as_ref()
            .unwrap()
            .authority
            .topology
            .as_ref()
            .unwrap()
            .configuration
            .clone();
        let late = if unaccepted {
            5
        } else if replacement {
            3
        } else {
            1
        };
        let mut members = previous.members.clone();
        if replacement {
            let identity = &mut members[1].identity;
            identity.instance_id = ReplicaInstanceId::new("replacement-pod-2");
            identity.agent_generation = derive_agent_generation(&derive_initialization_id(
                &ResourceUid::new(UID),
                identity.replica_id,
                &PodUid::new(identity.instance_id.as_str()),
                &PvcUid::new("pvc-uid-2"),
            ));
            raw.pods[1].metadata.uid = Some(identity.instance_id.to_string());
            raw.pods[1]
                .metadata
                .labels
                .as_mut()
                .unwrap()
                .insert(INSTANCE_LABEL.into(), identity.instance_id.to_string());
            let service = raw
                .services
                .iter_mut()
                .find(|s| s.metadata.uid.as_deref() == Some("endpoint-2"))
                .unwrap();
            service.metadata.name = Some(derive_replica_endpoint_name(
                &ResourceUid::new(UID),
                identity,
            ));
            service.spec.as_mut().unwrap().selector = Some(BTreeMap::from([(
                INSTANCE_LABEL.into(),
                identity.instance_id.to_string(),
            )]));
        } else {
            members[0].role = ReplicaRole::ActiveSecondary;
            members[1].role = ReplicaRole::Primary;
        }
        let accepted = ConfigurationDescriptor::new(
            Epoch::new(
                previous.epoch.data_loss_number,
                previous.epoch.configuration_number + 1,
            ),
            ReplicaId::new(if replacement { 1 } else { 2 }),
            members,
            previous.write_quorum,
        );
        for member in &accepted.members {
            let key = raw
                .agents
                .keys()
                .find(|k| k.replica_id == member.identity.replica_id)
                .unwrap()
                .clone();
            let RawAgentObservation::Report(mut report) = raw.agents.remove(&key).unwrap() else {
                unreachable!()
            };
            if member.identity.replica_id.value() == late {
                report.write_status = proto::AccessStatus::ReconfigurationPending as i32;
                if pending {
                    report.pending_operation_id = receipt
                        .evidence
                        .preparation
                        .intent
                        .command_operation_id(SecondaryRemovalStage::AcceptCommit, &member.identity)
                        .to_string();
                    report.process_session_id = "restarted-pending-acceptance".into();
                    report.report_sequence = 1;
                }
            } else {
                report.identity = Some(member.identity.clone().into());
                report.pod_uid = member.identity.instance_id.to_string();
                report.epoch = Some(accepted.epoch.into());
                report.current_configuration = Some(accepted.clone().into());
                report.previous_configuration = None;
                report.secondary_removal_evidence = None;
                report.accepted_secondary_removal = None;
                report.prepared_secondary_removal = None;
                report.role = if member.role == ReplicaRole::Primary {
                    proto::ReplicaRole::Primary
                } else {
                    proto::ReplicaRole::ActiveSecondary
                } as i32;
                report.write_status = if member.role == ReplicaRole::Primary {
                    proto::AccessStatus::Granted
                } else {
                    proto::AccessStatus::NotPrimary
                } as i32;
            }
            raw.agents.insert(
                ReplicaObservationKey::new(
                    member.identity.replica_id,
                    member.identity.instance_id.clone(),
                ),
                RawAgentObservation::Report(report),
            );
        }
        raw.set.status.as_mut().unwrap().authority.topology = Some(AcceptedTopology {
            configuration: accepted.clone(),
        });
        if !replacement {
            let service = raw
                .services
                .iter_mut()
                .find(|s| s.name_any() == "db-write")
                .unwrap();
            *service = write_service("pod-uid-2");
        }
        api.set_observation(raw).await;
        let api = Arc::new(InMemoryClusterApi::new(api.observation().await));
        if unaccepted {
            let (raw, plan) = next_plan(&api).await;
            let status = raw.set.status.unwrap().authority;
            assert!(
                matches!(plan, Plan::Execute {
                command: ProtocolCommand::AcceptSecondaryRemovalCommit(ref c)
            } if c.target.replica_id.value() == 5 && c.local_recovery && c.committed == receipt.committed()),
                "{plan:?}"
            );
            tick(&api).await;
            assert_eq!(
                api.observation().await.set.status.unwrap().authority,
                status
            );
        }
        let (_, plan) = next_plan(&api).await;
        assert!(
            matches!(plan, Plan::Execute { command: ProtocolCommand::EnsureConfiguration(ref c) } if c.local_replica_id.value() == late),
            "{plan:?}"
        );
        finish(&api).await;
        let result = api.observation().await;
        assert_eq!(
            result
                .set
                .status
                .as_ref()
                .unwrap()
                .authority
                .topology
                .as_ref()
                .unwrap()
                .configuration,
            accepted
        );
        assert_eq!(
            result.set.status.unwrap().authority.last_secondary_removal,
            Some(receipt)
        );
    }
}

#[tokio::test]
async fn transition_request_waits_for_endpoint_and_retained_configuration_catchup() {
    for switchover in [false, true] {
        let mut raw = fixture(4, 3);
        if switchover {
            raw.set.spec.switchover = Some(kuberic_controller::crd::PlannedSwitchoverRequestSpec {
                request_id: "after-convergence".into(),
                target_replica_id: 3,
            });
        } else {
            let key = raw
                .agents
                .keys()
                .find(|k| k.replica_id.value() == 4)
                .unwrap()
                .clone();
            raw.agents.remove(&key);
        }
        let accepted = raw
            .set
            .status
            .as_ref()
            .unwrap()
            .authority
            .topology
            .as_ref()
            .unwrap()
            .configuration
            .clone();
        // A member on accepted CC with PC still installed must complete current-only first.
        let key = raw
            .agents
            .keys()
            .find(|k| k.replica_id.value() == 2)
            .unwrap()
            .clone();
        let RawAgentObservation::Report(report) = raw.agents.get_mut(&key).unwrap() else {
            unreachable!()
        };
        let older = ConfigurationDescriptor::new(
            Epoch::new(0, 0),
            accepted.primary_id,
            accepted.members.clone(),
            accepted.write_quorum,
        );
        report.previous_configuration = Some(older.into());
        let endpoint = raw
            .services
            .iter()
            .find(|s| s.metadata.uid.as_deref() == Some("endpoint-3"))
            .unwrap()
            .clone();
        raw.services
            .retain(|s| s.metadata.uid.as_deref() != Some("endpoint-3"));
        let api = Arc::new(InMemoryClusterApi::new(raw));
        let (_, plan) = next_plan(&api).await;
        assert!(
            matches!(plan, Plan::Apply { ref changes } if matches!(changes[0], KubernetesChange::EnsureReplicaScaffolding { .. })),
            "{plan:?}"
        );
        tick(&api).await;
        // The in-memory API records scaffolding; a later API observation reports readiness.
        let mut raw = api.observation().await;
        raw.services.retain(|s| s.name_any() != endpoint.name_any());
        raw.services.push(endpoint);
        api.set_observation(raw).await;
        let (_, plan) = next_plan(&api).await;
        assert!(
            matches!(plan, Plan::Execute { command: ProtocolCommand::EnsureConfiguration(ref c) } if c.local_replica_id.value() == 2 && c.current_only),
            "{plan:?}"
        );
        tick(&api).await;
        tick(&api).await;
        let status = api.observation().await.set.status.unwrap().authority;
        assert_eq!(
            status.transition.unwrap().kind,
            if switchover {
                TransitionKind::PlannedSwitchover
            } else {
                TransitionKind::SecondaryScaleDown
            }
        );
    }
}

#[tokio::test]
async fn obsolete_returning_target_is_pod_fenced_then_next_removal_completes() {
    let mut raw = fixture(4, 2);
    let key = raw
        .agents
        .keys()
        .find(|k| k.replica_id.value() == 4)
        .unwrap()
        .clone();
    let RawAgentObservation::Report(mut saved) = raw.agents.remove(&key).unwrap() else {
        unreachable!()
    };
    let previous = raw
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .clone();
    let older = ConfigurationDescriptor::new(
        Epoch::new(0, 0),
        previous.primary_id,
        previous.members.clone(),
        previous.write_quorum,
    );
    saved.epoch = Some(older.epoch.into());
    saved.current_configuration = Some(older.into());
    let api = Arc::new(InMemoryClusterApi::new(raw));
    let (mut raw, _) = at_delete(&api, ScaleDownResource::Endpoint).await;
    raw.agents.insert(key, RawAgentObservation::Report(saved));
    api.set_observation(raw).await;
    finish(&api).await;
    assert_eq!(api.observation().await.pods.len(), 2);
    let effects = api.effects().await;
    assert!(!effects.iter().any(|e| matches!(e, EffectRecord::Execute(ProtocolCommand::RetireReplica(c)) if c.local_replica_id.value() == 4)));
    assert!(effects.iter().any(|e| matches!(e, EffectRecord::DeleteScaleDownResource { resource: ScaleDownResource::Pod, uid, .. } if uid == "pod-uid-4")));
}

#[tokio::test]
async fn commit_status_conflict_blocks_all_cleanup_then_heals() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(3, 2)));
    for _ in 0..80 {
        let (_, plan) = next_plan(&api).await;
        if matches!(plan, Plan::Apply { ref changes } if matches!(changes.first(), Some(KubernetesChange::PersistStatus { status }) if status.secondary_scale_down_cleanup.is_some()))
        {
            let before = api.observation().await;
            api.conflict_next_status().await;
            let (kind, effects) = tick(&api).await;
            assert_eq!(kind, ReconcileKind::ObservationStale);
            assert!(effects.is_empty());
            assert_eq!(before.set.status, api.observation().await.set.status);
            assert_eq!(api.observation().await.pods.len(), 3);
            finish(&api).await;
            return;
        }
        tick(&api).await;
    }
    panic!("commit boundary not reached")
}

#[tokio::test]
async fn exact_cleanup_survives_restart_lost_delete_reply_and_finalizers() {
    for resource in [
        ScaleDownResource::Endpoint,
        ScaleDownResource::Pod,
        ScaleDownResource::Pvc,
    ] {
        let baseline = Arc::new(InMemoryClusterApi::new(fixture(3, 2)));
        let (_, change) = at_delete(&baseline, resource).await;
        let KubernetesChange::DeleteScaleDownResource { name, .. } = change else {
            unreachable!()
        };
        let mut stored = baseline.observation().await;
        metadata(&mut stored, resource, &name).finalizers = Some(vec!["test/finalizer".into()]);
        let api = Arc::new(InMemoryClusterApi::new(stored));
        for _ in 0..3 {
            let (_, effects) = tick(&api).await;
            assert!(
                matches!(effects.as_slice(), [EffectRecord::DeleteScaleDownResource { resource: r, .. }] if *r == resource)
            );
            assert!(
                api.observation()
                    .await
                    .set
                    .status
                    .unwrap()
                    .authority
                    .secondary_scale_down_cleanup
                    .is_some()
            );
            assert_eq!(api.observation().await.pvcs.len(), 3);
        }
        let mut stored = api.observation().await;
        metadata(&mut stored, resource, &name).finalizers = None;
        api.set_observation(stored).await;
        api.lose_next_delete_reply().await;
        assert_eq!(tick(&api).await.0, ReconcileKind::ObservationStale);
        let restarted = Arc::new(InMemoryClusterApi::new(api.observation().await));
        finish(&restarted).await;
        assert!(!restarted.effects().await.iter().any(|e| matches!(e, EffectRecord::DeleteScaleDownResource { resource: r, .. } if *r == resource)));
    }
}

#[tokio::test]
async fn published_removal_commit_survives_retained_peer_session_restart() {
    for id in [1, 2] {
        let api = Arc::new(InMemoryClusterApi::new(fixture(3, 2)));
        let frozen = loop {
            let (raw, plan) = next_plan(&api).await;
            if let Plan::Execute {
                command: ProtocolCommand::AcceptSecondaryRemovalCommit(c),
            } = plan
                && c.target.replica_id.value() == id
            {
                assert_eq!(
                    raw.set
                        .status
                        .unwrap()
                        .authority
                        .secondary_scale_down_cleanup,
                    Some(c.committed.clone())
                );
                break c.committed;
            }
            tick(&api).await;
        };
        let mut raw = api.observation().await;
        let key = ReplicaObservationKey::new(
            ReplicaId::new(id),
            ReplicaInstanceId::new(format!("pod-uid-{id}")),
        );
        let RawAgentObservation::Report(report) = raw.agents.get_mut(&key).unwrap() else {
            unreachable!()
        };
        report.process_session_id = format!("restarted-{id}");
        report.report_sequence = 1;
        let restarted = Arc::new(InMemoryClusterApi::new(raw));
        let (_, plan) = next_plan(&restarted).await;
        assert!(matches!(plan, Plan::Execute {
            command: ProtocolCommand::AcceptSecondaryRemovalCommit(c)
        } if c.target.replica_id.value() == id && c.committed == frozen));
        finish(&restarted).await;
        let raw = restarted.observation().await;
        let receipt = raw
            .set
            .status
            .unwrap()
            .authority
            .last_secondary_removal
            .unwrap();
        assert_eq!(receipt.committed(), frozen);
        let RawAgentObservation::Report(primary) = &raw.agents
            [&ReplicaObservationKey::new(ReplicaId::new(1), ReplicaInstanceId::new("pod-uid-1"))]
        else {
            unreachable!()
        };
        assert_eq!(primary.write_status, proto::AccessStatus::Granted as i32);
    }
}

fn replacement_at_commit() -> (RawObservation, ReplicaIdentity, ReplicaIdentity) {
    let mut raw = fixture(3, 3);
    let status = &mut raw.set.status.as_mut().unwrap().authority;
    let previous = status.topology.as_ref().unwrap().configuration.clone();
    let old = previous.members[1].identity.clone();
    let new = ReplicaIdentity {
        replica_id: old.replica_id,
        instance_id: ReplicaInstanceId::new("replacement-pod"),
        agent_generation: derive_agent_generation(&derive_initialization_id(
            &ResourceUid::new(UID),
            old.replica_id,
            &PodUid::new("replacement-pod"),
            &PvcUid::new("replacement-pvc"),
        )),
    };
    let mut members = previous.members.clone();
    members[1].identity = new.clone();
    let current = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        previous.primary_id,
        members,
        previous.write_quorum,
    );
    let cleanup = ReplacementCleanup {
        resource_uid: ResourceUid::new(UID),
        target: old.clone(),
        resources: ReplicaCleanupIdentity {
            pod: CleanupResourceIdentity::Present {
                name: "db-2".into(),
                uid: old.instance_id.to_string(),
            },
            pvc: CleanupResourceIdentity::Present {
                name: "db-2-data".into(),
                uid: "pvc-uid-2".into(),
            },
            endpoint: CleanupResourceIdentity::Present {
                name: derive_replica_endpoint_name(&ResourceUid::new(UID), &old),
                uid: "endpoint-2".into(),
            },
        },
    };
    let transition_id =
        cleanup.transition_id(TransitionKind::Replacement, &current.configuration_id);
    status.pending_replacement_cleanup = Some(cleanup);
    status.transition = Some(TransitionIntent {
        transition_id: transition_id.clone(),
        kind: TransitionKind::Replacement,
        spec_generation: 2,
        effective_policy: status.effective_policy.clone().unwrap(),
        previous_configuration_id: Some(previous.configuration_id),
        current_configuration: current.clone(),
        election_lsn: None,
        build_id: Some(OperationId::new("first-replacement")),
        repair: None,
        switchover: None,
        secondary_scale_down: None,
        secondary_removal_evidence: None,
        scale_up: None,
        scale_up_failover: None,
    });
    let mut pod = raw.pods[1].clone();
    pod.metadata.name = Some("db-replacement".into());
    pod.metadata.uid = Some(new.instance_id.to_string());
    pod.metadata
        .labels
        .as_mut()
        .unwrap()
        .insert(INSTANCE_LABEL.into(), new.instance_id.to_string());
    pod.spec.as_mut().unwrap().volumes.as_mut().unwrap()[0]
        .persistent_volume_claim
        .as_mut()
        .unwrap()
        .claim_name = "db-replacement-data".into();
    raw.pods.push(pod);
    let mut pvc = raw.pvcs[1].clone();
    pvc.metadata.name = Some("db-replacement-data".into());
    pvc.metadata.uid = Some("replacement-pvc".into());
    pvc.metadata
        .labels
        .as_mut()
        .unwrap()
        .insert(INSTANCE_LABEL.into(), new.instance_id.to_string());
    raw.pvcs.push(pvc);
    let mut endpoint = raw.services[1].clone();
    endpoint.metadata.name = Some(derive_replica_endpoint_name(&ResourceUid::new(UID), &new));
    endpoint.metadata.uid = Some("replacement-endpoint".into());
    endpoint.spec.as_mut().unwrap().selector = Some(BTreeMap::from([(
        INSTANCE_LABEL.into(),
        new.instance_id.to_string(),
    )]));
    raw.services.push(endpoint);
    raw.agents.remove(&ReplicaObservationKey::new(
        old.replica_id,
        old.instance_id.clone(),
    ));
    for member in &current.members {
        let mut report = initialized_report(member.identity.clone(), current.clone());
        report.replica_id = member.identity.replica_id.value();
        report.pod_uid = member.identity.instance_id.to_string();
        report.pvc_uid = if member.identity == new {
            "replacement-pvc".into()
        } else {
            format!("pvc-uid-{}", member.identity.replica_id)
        };
        report.process_session_id = format!("replacement-session-{}", member.identity.replica_id);
        report.role = if member.role == ReplicaRole::Primary {
            proto::ReplicaRole::Primary as i32
        } else {
            proto::ReplicaRole::ActiveSecondary as i32
        };
        report.write_status = if member.role == ReplicaRole::Primary {
            proto::AccessStatus::Granted as i32
        } else {
            proto::AccessStatus::NotPrimary as i32
        };
        report.retained_operation_id = format!(
            "{transition_id}:current-only:{}",
            member.identity.replica_id
        );
        raw.agents.insert(
            ReplicaObservationKey::new(
                member.identity.replica_id,
                member.identity.instance_id.clone(),
            ),
            RawAgentObservation::Report(Box::new(report)),
        );
    }
    (raw, old, new)
}

#[tokio::test]
async fn consecutive_replacement_failures_serialize_frozen_cleanup_across_restarts() {
    let (raw, old, new) = replacement_at_commit();
    let mut api = Arc::new(InMemoryClusterApi::new(raw));
    let receipt = loop {
        tick(&api).await;
        if let Some(receipt) = api
            .observation()
            .await
            .set
            .status
            .unwrap()
            .authority
            .last_replacement
        {
            assert_eq!(receipt.target, old);
            break receipt;
        }
    };
    let healthy = api.observation().await;
    let mut failed = healthy.clone();
    let RawAgentObservation::Report(report) = failed
        .agents
        .get_mut(&ReplicaObservationKey::new(
            new.replica_id,
            new.instance_id.clone(),
        ))
        .unwrap()
    else {
        unreachable!()
    };
    report.reported_fault = proto::FaultType::Permanent as i32;
    api.set_observation(failed).await;
    for (resource, identity) in [
        (ScaleDownResource::Endpoint, &receipt.resources.endpoint),
        (ScaleDownResource::Pod, &receipt.resources.pod),
        (ScaleDownResource::Pvc, &receipt.resources.pvc),
    ] {
        let mut raw = api.observation().await;
        metadata(&mut raw, resource, identity.name()).finalizers = Some(vec!["test/hold".into()]);
        api = Arc::new(InMemoryClusterApi::new(raw));
        for _ in 0..3 {
            let (_, effects) = tick(&api).await;
            assert!(
                matches!(effects.as_slice(), [EffectRecord::DeleteScaleDownResource { resource: r, .. }] if *r == resource)
            );
            let status = api.observation().await.set.status.unwrap().authority;
            assert_eq!(status.last_replacement, Some(receipt.clone()));
            assert!(status.provisioning.is_none() && status.transition.is_none());
        }
        let mut raw = api.observation().await;
        metadata(&mut raw, resource, identity.name()).finalizers = None;
        api.set_observation(raw).await;
        api.lose_next_delete_reply().await;
        assert_eq!(tick(&api).await.0, ReconcileKind::ObservationStale);
        api = Arc::new(InMemoryClusterApi::new(api.observation().await));
    }
    tick(&api).await;
    assert!(
        api.observation()
            .await
            .set
            .status
            .unwrap()
            .authority
            .last_replacement
            .is_none()
    );
    let raw = api.observation().await;
    assert!(
        raw.pods
            .iter()
            .any(|p| p.uid().as_deref() == Some(new.instance_id.as_str()))
    );
    assert!(
        raw.pvcs
            .iter()
            .any(|p| p.uid().as_deref() == Some("replacement-pvc"))
    );
    assert!(
        raw.services
            .iter()
            .any(|s| s.uid().as_deref() == Some("replacement-endpoint"))
    );
    assert!(
        !raw.pods
            .iter()
            .any(|p| p.uid().as_deref() == Some(old.instance_id.as_str()))
    );
    assert!(
        !raw.pvcs
            .iter()
            .any(|p| p.uid().as_deref() == Some("pvc-uid-2"))
    );
    assert!(
        !raw.services
            .iter()
            .any(|s| s.uid().as_deref() == Some("endpoint-2"))
    );
    let (_, plan) = next_plan(&api).await;
    assert!(
        matches!(plan, Plan::Apply { changes } if matches!(changes.as_slice(),
        [KubernetesChange::PersistStatus { status }] if status.pending_replacement_cleanup.as_ref().is_some_and(|c| c.target == new)))
    );
    tick(&api).await;
    let (_, plan) = next_plan(&api).await;
    assert!(
        matches!(plan, Plan::Apply { changes } if matches!(changes.as_slice(),
        [KubernetesChange::EnsureReplacementScaffolding { replacing, .. }] if replacing == &new))
    );
}

#[tokio::test]
async fn replacement_no_longer_stalls_when_old_pvc_vanishes_before_acceptance() {
    let (mut raw, _, _) = replacement_at_commit();
    raw.pvcs.retain(|p| p.uid().as_deref() != Some("pvc-uid-2"));
    raw.set.status =
        serde_json::from_value(serde_json::to_value(&raw.set.status).unwrap()).unwrap();
    let restarted = Arc::new(InMemoryClusterApi::new(raw));
    for _ in 0..10 {
        tick(&restarted).await;
        if restarted
            .observation()
            .await
            .set
            .status
            .unwrap()
            .authority
            .transition
            .is_none()
        {
            return;
        }
    }
    panic!("replacement acceptance stalled after the old PVC disappeared");
}

#[tokio::test]
async fn replacement_acceptance_survives_old_resources_disappearing_before_commit() {
    for resource in [
        ScaleDownResource::Endpoint,
        ScaleDownResource::Pod,
        ScaleDownResource::Pvc,
    ] {
        for recreated in [false, true] {
            let (mut raw, old, new) = replacement_at_commit();
            let receipt = raw
                .set
                .status
                .as_ref()
                .unwrap()
                .authority
                .pending_replacement_cleanup
                .clone()
                .unwrap();
            let identity = match resource {
                ScaleDownResource::Endpoint => &receipt.resources.endpoint,
                ScaleDownResource::Pod => &receipt.resources.pod,
                ScaleDownResource::Pvc => &receipt.resources.pvc,
            };
            if recreated {
                let meta = metadata(&mut raw, resource, identity.name());
                meta.uid = Some("unrelated-same-name-occupant".into());
                meta.resource_version = Some("99".into());
                meta.labels = None;
            } else {
                match resource {
                    ScaleDownResource::Endpoint => {
                        raw.services.retain(|s| s.name_any() != identity.name())
                    }
                    ScaleDownResource::Pod => raw.pods.retain(|p| p.name_any() != identity.name()),
                    ScaleDownResource::Pvc => raw.pvcs.retain(|p| p.name_any() != identity.name()),
                }
            }
            // Only persisted JSON survives the controller restart; no old resource list is required.
            raw.set.status =
                serde_json::from_value(serde_json::to_value(&raw.set.status).unwrap()).unwrap();
            let api = Arc::new(InMemoryClusterApi::new(raw));
            for _ in 0..20 {
                tick(&api).await;
                let status = api.observation().await.set.status.unwrap().authority;
                if status.transition.is_none() {
                    assert_eq!(status.last_replacement, Some(receipt.clone()));
                    assert!(status.pending_replacement_cleanup.is_none());
                    break;
                }
            }
            assert!(
                api.observation()
                    .await
                    .set
                    .status
                    .unwrap()
                    .authority
                    .transition
                    .is_none(),
                "pre-fix acceptance stalled after old PVC disappeared"
            );
            let restarted = Arc::new(InMemoryClusterApi::new(api.observation().await));
            finish(&restarted).await;
            let mut raw = restarted.observation().await;
            assert!(
                raw.set
                    .status
                    .as_ref()
                    .unwrap()
                    .authority
                    .last_replacement
                    .is_none()
            );
            assert!(
                raw.pods
                    .iter()
                    .any(|p| p.uid().as_deref() == Some(new.instance_id.as_str()))
            );
            assert!(
                !raw.pods
                    .iter()
                    .any(|p| p.uid().as_deref() == Some(old.instance_id.as_str()))
            );
            if recreated {
                assert_eq!(
                    metadata(&mut raw, resource, identity.name()).uid.as_deref(),
                    Some("unrelated-same-name-occupant")
                );
            }
            assert!(!restarted.effects().await.iter().any(|e| matches!(e,
                EffectRecord::DeleteScaleDownResource { uid, .. } if uid == "unrelated-same-name-occupant")));
        }
    }
}

#[tokio::test]
async fn failover_accepts_carried_replacement_after_all_old_resources_disappear() {
    let (mut raw, old, _) = replacement_at_commit();
    let status = &mut raw.set.status.as_mut().unwrap().authority;
    let cleanup = status.pending_replacement_cleanup.clone().unwrap();
    let transition = status.transition.as_mut().unwrap();
    let mut members = transition.current_configuration.members.clone();
    for member in &mut members {
        member.role = if member.identity.replica_id == ReplicaId::new(3) {
            ReplicaRole::Primary
        } else {
            ReplicaRole::ActiveSecondary
        };
    }
    transition.current_configuration =
        ConfigurationDescriptor::new(Epoch::new(0, 3), ReplicaId::new(3), members, 2);
    transition.kind = TransitionKind::Failover;
    transition.election_lsn = Some(5);
    transition.transition_id = cleanup.transition_id(
        transition.kind,
        &transition.current_configuration.configuration_id,
    );
    for member in &transition.current_configuration.members {
        let RawAgentObservation::Report(report) = raw
            .agents
            .get_mut(&ReplicaObservationKey::new(
                member.identity.replica_id,
                member.identity.instance_id.clone(),
            ))
            .unwrap()
        else {
            unreachable!()
        };
        report.epoch = Some(transition.current_configuration.epoch.into());
        report.current_configuration = Some(transition.current_configuration.clone().into());
        report.role = if member.role == ReplicaRole::Primary {
            proto::ReplicaRole::Primary
        } else {
            proto::ReplicaRole::ActiveSecondary
        } as i32;
        report.write_status = if member.role == ReplicaRole::Primary {
            proto::AccessStatus::Granted
        } else {
            proto::AccessStatus::NotPrimary
        } as i32;
        report.retained_operation_id = format!(
            "{}:current-only:{}",
            transition.transition_id, member.identity.replica_id
        );
    }
    raw.pods
        .retain(|p| p.uid().as_deref() != Some(old.instance_id.as_str()));
    raw.pvcs
        .retain(|p| p.name_any() != cleanup.resources.pvc.name());
    raw.services.retain(|s| {
        s.name_any() != cleanup.resources.endpoint.name() && s.name_any() != "db-write"
    });
    raw.services.push(write_service("disabled"));
    let api = Arc::new(InMemoryClusterApi::new(raw));
    for _ in 0..20 {
        tick(&api).await;
        let status = api.observation().await.set.status.unwrap().authority;
        if status.transition.is_none() {
            assert_eq!(status.last_replacement, Some(cleanup.clone()));
            assert!(status.pending_replacement_cleanup.is_none());
            break;
        }
        assert_eq!(status.pending_replacement_cleanup, Some(cleanup.clone()));
    }
    assert!(
        api.observation()
            .await
            .set
            .status
            .unwrap()
            .authority
            .transition
            .is_none()
    );
    let restarted = Arc::new(InMemoryClusterApi::new(api.observation().await));
    tick(&restarted).await;
    let status = restarted.observation().await.set.status.unwrap().authority;
    assert!(status.last_replacement.is_none());
    assert_eq!(
        status.topology.unwrap().configuration.primary_id,
        ReplicaId::new(3)
    );
    assert!(!restarted.effects().await.iter().any(|e| matches!(
        e,
        EffectRecord::DeleteScaleDownResource { .. } | EffectRecord::DeleteScaffolding { .. }
    )));
}

#[tokio::test]
async fn replacement_admission_freezes_exact_identity_before_any_scaffolding() {
    for failure in [false, true] {
        let mut raw = fixture(3, 3);
        let old = raw
            .set
            .status
            .as_ref()
            .unwrap()
            .authority
            .topology
            .as_ref()
            .unwrap()
            .configuration
            .members[1]
            .identity
            .clone();
        let RawAgentObservation::Report(report) = raw
            .agents
            .get_mut(&ReplicaObservationKey::new(
                old.replica_id,
                old.instance_id.clone(),
            ))
            .unwrap()
        else {
            unreachable!()
        };
        report.reported_fault = proto::FaultType::Permanent as i32;
        let api = Arc::new(InMemoryClusterApi::new(raw));
        if failure {
            api.fail_exact_lookup("PVC/db-2-data".into(), Some("403".into()))
                .await;
            for _ in 0..3 {
                assert_eq!(tick(&api).await.0, ReconcileKind::Waiting);
                assert!(
                    api.observation()
                        .await
                        .set
                        .status
                        .unwrap()
                        .authority
                        .pending_replacement_cleanup
                        .is_none()
                );
            }
            assert!(
                !api.effects()
                    .await
                    .iter()
                    .any(|e| matches!(e, EffectRecord::EnsureReplacement(_)))
            );
            api.fail_exact_lookup("PVC/db-2-data".into(), None).await;
        }
        let (_, effects) = tick(&api).await;
        assert!(
            effects
                .iter()
                .all(|e| matches!(e, EffectRecord::ReplaceStatus))
        );
        let status = api.observation().await.set.status.unwrap().authority;
        assert_eq!(
            status.pending_replacement_cleanup.as_ref().unwrap().target,
            old
        );
        assert!(status.provisioning.is_none() && status.transition.is_none());
        let restarted = Arc::new(InMemoryClusterApi::new(api.observation().await));
        let (_, effects) = tick(&restarted).await;
        assert_eq!(effects, vec![EffectRecord::EnsureReplacement(old)]);
    }
}

#[tokio::test]
async fn replacement_admission_requires_storage_provenance_and_authoritative_endpoint_absence() {
    for missing_pvc in [false, true] {
        let mut raw = fixture(3, 3);
        let old = raw
            .set
            .status
            .as_ref()
            .unwrap()
            .authority
            .topology
            .as_ref()
            .unwrap()
            .configuration
            .members[1]
            .identity
            .clone();
        let endpoint = derive_replica_endpoint_name(&ResourceUid::new(UID), &old);
        raw.services.retain(|s| s.name_any() != endpoint);
        let RawAgentObservation::Report(report) = raw
            .agents
            .get_mut(&ReplicaObservationKey::new(
                old.replica_id,
                old.instance_id.clone(),
            ))
            .unwrap()
        else {
            unreachable!()
        };
        report.reported_fault = proto::FaultType::Permanent as i32;
        if missing_pvc {
            raw.pvcs.retain(|p| p.uid().as_deref() != Some("pvc-uid-2"));
        }
        let api = Arc::new(InMemoryClusterApi::new(raw));
        tick(&api).await;
        let status = api.observation().await.set.status.unwrap().authority;
        if missing_pvc {
            assert!(status.pending_replacement_cleanup.is_none());
            assert!(
                !api.effects()
                    .await
                    .iter()
                    .any(|e| matches!(e, EffectRecord::EnsureReplacement(_)))
            );
        } else {
            let receipt = status.pending_replacement_cleanup.unwrap();
            assert_eq!(
                receipt.resources.endpoint,
                CleanupResourceIdentity::Absent { name: endpoint }
            );
            assert_eq!(receipt.target, old);
        }
    }
}

#[tokio::test]
async fn replacement_cleanup_does_not_adopt_churned_uids_or_list_absence() {
    for resource in [
        ScaleDownResource::Endpoint,
        ScaleDownResource::Pod,
        ScaleDownResource::Pvc,
    ] {
        let (raw, _, new) = replacement_at_commit();
        let api = Arc::new(InMemoryClusterApi::new(raw));
        let (observed, change) = at_delete(&api, resource).await;
        let KubernetesChange::DeleteScaleDownResource {
            name,
            uid,
            resource_version,
            ..
        } = change
        else {
            unreachable!()
        };
        let frozen = observed
            .set
            .status
            .as_ref()
            .unwrap()
            .authority
            .last_replacement
            .clone()
            .unwrap();
        let kind = match resource {
            ScaleDownResource::Endpoint => "Service",
            ScaleDownResource::Pod => "Pod",
            ScaleDownResource::Pvc => "PVC",
        };
        api.fail_exact_lookup(format!("{kind}/{name}"), Some("unavailable".into()))
            .await;
        assert_eq!(tick(&api).await.0, ReconcileKind::Waiting);
        assert_eq!(
            api.observation()
                .await
                .set
                .status
                .unwrap()
                .authority
                .last_replacement,
            Some(frozen.clone())
        );
        api.fail_exact_lookup(format!("{kind}/{name}"), None).await;
        let mut raw = api.observation().await;
        let meta = metadata(&mut raw, resource, &name);
        meta.uid = Some("unrelated-recreated-resource".into());
        meta.resource_version = Some("999".into());
        meta.labels = None;
        api.set_observation(raw).await;
        assert!(matches!(
            api.delete_scale_down_resource(&observed, resource, &name, &uid, &resource_version)
                .await,
            Err(ControllerError::ObservationStale)
        ));
        let restarted = Arc::new(InMemoryClusterApi::new(api.observation().await));
        finish(&restarted).await;
        let mut raw = restarted.observation().await;
        assert_eq!(
            metadata(&mut raw, resource, &name).uid.as_deref(),
            Some("unrelated-recreated-resource")
        );
        assert!(raw.set.status.unwrap().authority.last_replacement.is_none());
        assert!(
            raw.pods
                .iter()
                .any(|p| p.uid().as_deref() == Some(new.instance_id.as_str()))
        );
        assert!(
            raw.pvcs
                .iter()
                .any(|p| p.uid().as_deref() == Some("replacement-pvc"))
        );
        assert!(!restarted.effects().await.iter().any(|e| matches!(
            e,
            EffectRecord::DeleteScaffolding { .. } | EffectRecord::EnsureReplacement(_)
        )));
    }
}

#[tokio::test]
async fn same_name_replacements_and_rv_races_never_acquire_cleanup_authority() {
    for resource in [
        ScaleDownResource::Endpoint,
        ScaleDownResource::Pod,
        ScaleDownResource::Pvc,
    ] {
        for replace_before_observe in [true, false] {
            let api = Arc::new(InMemoryClusterApi::new(fixture(3, 2)));
            let (old, change) = at_delete(&api, resource).await;
            let KubernetesChange::DeleteScaleDownResource {
                name,
                uid,
                resource_version,
                ..
            } = change
            else {
                unreachable!()
            };
            let mut replaced = api.observation().await;
            let meta = metadata(&mut replaced, resource, &name);
            meta.uid = Some("replacement".into());
            meta.resource_version = Some("99".into());
            api.set_observation(replaced).await;
            assert_generic_cleanup_protected(&api, resource, &name).await;
            if !replace_before_observe {
                assert!(matches!(
                    api.delete_scale_down_resource(&old, resource, &name, &uid, &resource_version)
                        .await,
                    Err(ControllerError::ObservationStale)
                ));
            }
            finish(&api).await;
            for _ in 0..3 {
                tick(&api).await;
            }
            let mut result = api.observation().await;
            assert_eq!(
                metadata(&mut result, resource, &name).uid.as_deref(),
                Some("replacement")
            );
            assert_generic_cleanup_protected(&api, resource, &name).await;
            assert!(
                !api.effects()
                    .await
                    .iter()
                    .any(|e| matches!(e, EffectRecord::DeleteScaffolding { .. }))
            );
            assert!(matches!(
                api.delete_scale_down_resource(&old, resource, &name, &uid, &resource_version)
                    .await,
                Err(ControllerError::ObservationStale)
            ));
        }

        let api = Arc::new(InMemoryClusterApi::new(fixture(3, 2)));
        let (old, change) = at_delete(&api, resource).await;
        let KubernetesChange::DeleteScaleDownResource {
            name,
            uid,
            resource_version,
            ..
        } = change
        else {
            unreachable!()
        };
        let mut newer = api.observation().await;
        metadata(&mut newer, resource, &name).resource_version = Some("changed".into());
        api.set_observation(newer).await;
        assert!(matches!(
            api.delete_scale_down_resource(&old, resource, &name, &uid, &resource_version)
                .await,
            Err(ControllerError::ObservationStale)
        ));
        finish(&api).await;
    }
}

#[tokio::test]
async fn generic_cleanup_cannot_adopt_a_protected_replacement_endpoint() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(3, 2)));
    finish(&api).await;
    let mut raw = api.observation().await;
    let receipt = raw
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .last_secondary_removal
        .as_ref()
        .unwrap();
    let endpoint_name = receipt
        .evidence
        .preparation
        .intent
        .cleanup
        .endpoint
        .name()
        .to_string();
    raw.services.push(Service {
        metadata: kube::core::ObjectMeta {
            name: Some(endpoint_name.clone()),
            uid: Some("replacement-service-uid".into()),
            resource_version: Some("99".into()),
            labels: Some(BTreeMap::from([(SET_UID_LABEL.into(), UID.into())])),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(BTreeMap::from([(
                INSTANCE_LABEL.into(),
                "replacement-pod-uid".into(),
            )])),
            ..Default::default()
        }),
        ..Default::default()
    });
    api.set_observation(raw).await;
    let observed = api.observation().await;

    assert!(matches!(
        api.delete_replica_scaffolding(
            &observed,
            Some("replacement-pod"),
            Some(&PodUid::new("replacement-pod-uid")),
            None,
            None,
        )
        .await,
        Err(ControllerError::ObservationStale)
    ));
    assert!(
        api.observation()
            .await
            .services
            .iter()
            .any(|service| service.name_any() == endpoint_name
                && service.uid().as_deref() == Some("replacement-service-uid"))
    );
    assert!(
        !api.effects()
            .await
            .iter()
            .any(|effect| matches!(effect, EffectRecord::DeleteScaffolding { .. }))
    );
}

async fn assert_generic_cleanup_protected(
    api: &InMemoryClusterApi,
    resource: ScaleDownResource,
    name: &str,
) {
    let raw = api.observation().await;
    let status = &raw.set.status.as_ref().unwrap().authority;
    let target = &status
        .secondary_scale_down_cleanup
        .as_ref()
        .map(|c| &c.evidence)
        .or_else(|| status.last_secondary_removal.as_ref().map(|r| &r.evidence))
        .unwrap()
        .preparation
        .intent
        .target;
    let result = match resource {
        ScaleDownResource::Pod => {
            assert!(matches!(
                api.delete_exact_pod(&raw, name, &PodUid::new("replacement"))
                    .await,
                Err(ControllerError::ObservationStale)
            ));
            api.delete_replica_scaffolding(
                &raw,
                Some(name),
                Some(&PodUid::new("replacement")),
                None,
                None,
            )
            .await
        }
        ScaleDownResource::Pvc => {
            api.delete_replica_scaffolding(
                &raw,
                None,
                None,
                Some(name),
                Some(&PvcUid::new("replacement")),
            )
            .await
        }
        ScaleDownResource::Endpoint => api.delete_replica_endpoint(&raw, target).await,
    };
    assert!(matches!(result, Err(ControllerError::ObservationStale)));
}

#[tokio::test]
async fn unavailable_target_label_loss_and_exact_lookup_failures_never_imply_absence() {
    let mut raw = fixture(2, 1);
    raw.agents.insert(
        ReplicaObservationKey::new(ReplicaId::new(2), ReplicaInstanceId::new("pod-uid-2")),
        RawAgentObservation::Unavailable {
            message: "partition".into(),
        },
    );
    let api = Arc::new(InMemoryClusterApi::new(raw));
    at_delete(&api, ScaleDownResource::Endpoint).await;
    let mut raw = api.observation().await;
    raw.pods[1].metadata.labels = None;
    api.set_observation(raw).await;
    let (raw, _) = next_plan(&api).await;
    let snapshot = normalize(raw, BTreeMap::new()).unwrap();
    assert!(matches!(
        snapshot.secondary_scale_down_resources[0].pod,
        ExactResourceObservation::FrozenUidPresent { .. }
    ));
    assert!(
        snapshot
            .observation_for_identity(&snapshot.secondary_scale_down_resources[0].target)
            .is_some()
    );
    api.fail_exact_lookup("Pod/db-2".into(), Some("403 forbidden".into()))
        .await;
    for _ in 0..2 {
        assert_eq!(tick(&api).await.0, ReconcileKind::Waiting);
        assert_eq!(api.observation().await.pods.len(), 2);
        assert_eq!(api.observation().await.pvcs.len(), 2);
    }
    let (raw, _) = next_plan(&api).await;
    let snapshot = normalize(raw, BTreeMap::new()).unwrap();
    assert!(matches!(
        snapshot.secondary_scale_down_resources[0].pod,
        ExactResourceObservation::LookupFailed { .. }
    ));
    assert!(
        snapshot
            .observation_failures
            .iter()
            .any(|f| f.source == "exact-Pod/db-2")
    );
    api.fail_exact_lookup("Pod/db-2".into(), None).await;
    finish(&api).await;
    assert!(
        !api.effects()
            .await
            .iter()
            .any(|e| matches!(e, EffectRecord::Execute(ProtocolCommand::RetireReplica(_))))
    );
    assert!(api.effects().await.iter().any(|e| matches!(e, EffectRecord::DeleteScaleDownResource { resource: ScaleDownResource::Pod, uid, .. } if uid == "pod-uid-2")));
}

#[tokio::test]
async fn authoritative_not_found_requires_no_target_retirement() {
    let mut raw = fixture(2, 1);
    raw.pods.pop();
    let api = Arc::new(InMemoryClusterApi::new(raw));
    let (observed, _) = next_plan(&api).await;
    assert!(matches!(
        normalize(observed, BTreeMap::new())
            .unwrap()
            .secondary_scale_down_resources[0]
            .pod,
        ExactResourceObservation::NotFound
    ));
    finish(&api).await;
    assert!(!api.effects().await.iter().any(|e| matches!(
        e,
        EffectRecord::Execute(ProtocolCommand::RetireReplica(_))
            | EffectRecord::DeleteScaleDownResource {
                resource: ScaleDownResource::Pod,
                ..
            }
    )));
}

#[tokio::test]
async fn ambiguous_commands_reobserve_exact_sessions_before_replay() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(2, 1)));
    let mut seen = 0;
    for _ in 0..120 {
        let (raw, plan) = next_plan(&api).await;
        if let Plan::Execute { command } = plan {
            let before = api.effects().await.len();
            api.unavailable_next_execute().await;
            let action = Reconciler::new(api.clone(), enabled())
                .reconcile("tests", "db")
                .await
                .unwrap();
            assert_eq!(action.kind, ReconcileKind::Waiting);
            assert_eq!(api.effects().await.len(), before + 1);
            assert_eq!(
                next_plan(&api).await.1,
                Plan::Execute {
                    command: command.clone()
                }
            );
            let mut restarted = api.observation().await;
            for report in restarted.agents.values_mut() {
                if let RawAgentObservation::Report(report) = report {
                    report.process_session_id.push_str("-restart");
                }
            }
            api.set_observation(restarted).await;
            assert!(matches!(
                api.execute_command(&raw, &command).await,
                Err(ControllerError::ObservationStale)
            ));
            // The command can have applied despite reply loss; completion is only
            // supplied by the next independently observed process report.
            apply_command(&api, &command).await;
            seen += 1;
        } else if tick(&api).await.0 == ReconcileKind::Stable {
            assert!(seen >= 5);
            return;
        }
    }
    panic!("ambiguous command convergence failed")
}

#[tokio::test]
async fn late_member_after_cleanup_and_writable_primary_restart_allows_next_removal() {
    let mut original = fixture(4, 3);
    let key = ReplicaObservationKey::new(ReplicaId::new(3), ReplicaInstanceId::new("pod-uid-3"));
    let late = original.agents.remove(&key).unwrap();
    let api = Arc::new(InMemoryClusterApi::new(original));
    finish(&api).await;
    let mut raw = api.observation().await;
    let receipt = raw
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .last_secondary_removal
        .clone()
        .unwrap();
    assert!(
        receipt
            .current_only_write_quorum
            .iter()
            .all(|w| w.identity.replica_id.value() != 3)
    );
    let primary_key =
        ReplicaObservationKey::new(ReplicaId::new(1), ReplicaInstanceId::new("pod-uid-1"));
    let RawAgentObservation::Report(primary) = raw.agents.get_mut(&primary_key).unwrap() else {
        unreachable!()
    };
    primary.process_session_id = "restarted-writing-primary".into();
    primary.report_sequence = 1;
    primary.current_progress = 10;
    primary.committed_lsn = 10;
    primary.verified_replication_lsn = Some(10);
    assert_eq!(primary.write_status, proto::AccessStatus::Granted as i32);
    raw.agents.insert(key.clone(), late);
    let restarted = Arc::new(InMemoryClusterApi::new(raw));
    finish(&restarted).await;
    let mut raw = restarted.observation().await;
    assert_eq!(
        raw.set
            .status
            .as_ref()
            .unwrap()
            .authority
            .last_secondary_removal,
        Some(receipt)
    );
    assert!(restarted.effects().await.iter().any(|e| matches!(e,
        EffectRecord::Execute(ProtocolCommand::AcceptSecondaryRemovalCommit(c)) if c.target.replica_id.value() == 3)));
    assert!(
        !restarted
            .effects()
            .await
            .iter()
            .any(|e| matches!(e, EffectRecord::DeleteScaleDownResource { .. }))
    );
    raw.set.spec.replicas = 2;
    raw.set.metadata.generation = Some(3);
    // Replication catches the retained secondary through the primary's fresh prefix.
    for report in raw.agents.values_mut() {
        if let RawAgentObservation::Report(report) = report {
            report.current_progress = 10;
            report.committed_lsn = 10;
            report.verified_replication_lsn = Some(10);
        }
    }
    restarted.set_observation(raw).await;
    finish(&restarted).await;
    assert_eq!(restarted.observation().await.pods.len(), 2);
}

#[tokio::test]
async fn completed_receipt_corrects_late_retained_member_without_deletion_authority() {
    let original = fixture(5, 4);
    let key = ReplicaObservationKey::new(ReplicaId::new(3), ReplicaInstanceId::new("pod-uid-3"));
    let late = original.agents[&key].clone();
    let api = Arc::new(InMemoryClusterApi::new(original));
    for _ in 0..50 {
        tick(&api).await;
        let mut raw = api.observation().await;
        if raw
            .set
            .status
            .as_ref()
            .unwrap()
            .authority
            .transition
            .as_ref()
            .is_some_and(|t| t.secondary_removal_evidence.is_some())
        {
            raw.agents.insert(
                key.clone(),
                RawAgentObservation::Unavailable {
                    message: "retained member partition".into(),
                },
            );
            api.set_observation(raw).await;
            break;
        }
    }
    finish(&api).await;
    let completed = api.observation().await;
    let receipt = completed
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .last_secondary_removal
        .clone()
        .unwrap();
    assert!(
        !receipt
            .current_only_write_quorum
            .iter()
            .any(|w| w.identity.replica_id == ReplicaId::new(3))
    );
    let mut returning = completed.clone();
    returning.agents.insert(key.clone(), late);
    let restarted = Arc::new(InMemoryClusterApi::new(returning));
    finish(&restarted).await;
    let final_raw = restarted.observation().await;
    assert_eq!(final_raw.pods, completed.pods);
    assert_eq!(final_raw.pvcs, completed.pvcs);
    assert_eq!(final_raw.services, completed.services);
    assert_eq!(
        final_raw
            .set
            .status
            .unwrap()
            .authority
            .last_secondary_removal,
        Some(receipt)
    );
    let commands = restarted.effects().await;
    assert!(commands.iter().any(|e| matches!(e, EffectRecord::Execute(ProtocolCommand::EnsureConfiguration(c)) if c.local_replica_id == ReplicaId::new(3) && !c.current_only)));
    assert!(commands.iter().any(|e| matches!(e, EffectRecord::Execute(ProtocolCommand::AcceptSecondaryRemovalCommit(c)) if c.target.replica_id == ReplicaId::new(3))));
    assert!(!commands.iter().any(|e| matches!(
        e,
        EffectRecord::DeleteScaleDownResource { .. } | EffectRecord::DeleteScaffolding { .. }
    )));
}

#[tokio::test]
async fn exact_get_and_list_failures_block_each_cleanup_kind_until_observed() {
    for (resource, kind) in [
        (ScaleDownResource::Pod, "Pod"),
        (ScaleDownResource::Pvc, "PVC"),
        (ScaleDownResource::Endpoint, "Service"),
    ] {
        let baseline = Arc::new(InMemoryClusterApi::new(fixture(3, 2)));
        let (_, change) = at_delete(&baseline, resource).await;
        let KubernetesChange::DeleteScaleDownResource { name, .. } = change else {
            unreachable!()
        };
        let api = Arc::new(InMemoryClusterApi::new(baseline.observation().await));
        api.fail_exact_lookup(format!("{kind}/{name}"), Some("lookup timeout".into()))
            .await;
        for _ in 0..2 {
            assert_eq!(tick(&api).await.0, ReconcileKind::Waiting);
        }
        assert!(
            !api.effects()
                .await
                .iter()
                .any(|e| matches!(e, EffectRecord::DeleteScaleDownResource { .. }))
        );
        api.fail_exact_lookup(format!("{kind}/{name}"), None).await;
        let mut raw = api.observation().await;
        raw.failures.push(RawObservationFailure {
            source: "pods".into(),
            message: "list 403".into(),
        });
        api.set_observation(raw).await;
        assert_eq!(tick(&api).await.0, ReconcileKind::Waiting);
        let mut raw = api.observation().await;
        raw.failures.clear();
        api.set_observation(raw).await;
        finish(&api).await;
    }
}

#[tokio::test]
async fn exact_observation_preserves_unlabelled_target_and_simultaneous_incarnations() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(3, 2)));
    at_delete(&api, ScaleDownResource::Endpoint).await;
    let mut raw = api.observation().await;
    let mut extra = raw.pods[2].clone();
    extra.metadata.name = Some("extra-target".into());
    extra.metadata.uid = Some("extra-uid".into());
    raw.pods[2].metadata.labels = None;
    raw.pvcs[2].metadata.labels = None;
    raw.pods.push(extra);
    api.set_observation(raw).await;
    let snapshot = normalize(api.observe("tests", "db").await.unwrap(), BTreeMap::new()).unwrap();
    assert!(matches!(
        &snapshot
            .observation_for_identity(&snapshot.secondary_scale_down_resources[0].target)
            .unwrap()
            .agent,
        AgentObservation::Report(_)
    ));
    assert_eq!(
        snapshot
            .replicas
            .keys()
            .filter(|k| k.replica_id == ReplicaId::new(3))
            .count(),
        2
    );
    finish(&api).await;
    assert!(
        api.observation()
            .await
            .pods
            .iter()
            .any(|p| p.uid().as_deref() == Some("extra-uid"))
    );
}

#[tokio::test]
async fn missing_storage_mapping_or_exact_metadata_never_freezes_cleanup() {
    for mutation in 0..3 {
        let mut raw = fixture(3, 2);
        match mutation {
            0 => raw.pods[2].spec.as_mut().unwrap().volumes = None,
            1 => raw.pvcs[2].metadata.uid = Some("unproven-storage".into()),
            _ => raw.pods[2].metadata.resource_version = None,
        }
        let api = Arc::new(InMemoryClusterApi::new(raw));
        for _ in 0..3 {
            tick(&api).await;
        }
        assert!(
            api.observation()
                .await
                .set
                .status
                .unwrap()
                .authority
                .transition
                .is_none()
        );
        assert!(!api.effects().await.iter().any(|e| matches!(
            e,
            EffectRecord::Execute(_) | EffectRecord::DeleteScaleDownResource { .. }
        )));
    }
}
#[tokio::test]
async fn every_status_and_command_boundary_survives_conflict_loss_and_controller_restart() {
    for (size, desired) in [(2, 1), (3, 2), (5, 2)] {
        let baseline = Arc::new(InMemoryClusterApi::new(fixture(size, desired)));
        let mut statuses = 0;
        let mut commands = 0;
        for boundary in 0..250 {
            let (raw, plan) = next_plan(&baseline).await;
            if matches!(plan, Plan::Stable { .. }) {
                break;
            }
            let mut persisted = raw.clone();
            persisted.set =
                serde_json::from_slice(&serde_json::to_vec(&persisted.set).unwrap()).unwrap();
            let fork = Arc::new(InMemoryClusterApi::new(persisted));
            match &plan {
                Plan::Apply { changes }
                    if matches!(
                        changes.first(),
                        Some(KubernetesChange::PersistStatus { .. })
                    ) =>
                {
                    fork.conflict_next_status().await;
                    let (kind, effects) = tick(&fork).await;
                    assert_eq!(
                        kind,
                        ReconcileKind::ObservationStale,
                        "size={size} boundary={boundary}"
                    );
                    assert!(effects.is_empty());
                    assert_eq!(fork.observation().await.set.status, raw.set.status);
                    assert_eq!(fork.observation().await.pods, raw.pods);
                    assert_eq!(fork.observation().await.pvcs, raw.pvcs);
                    statuses += 1;
                }
                Plan::Execute { command } => {
                    fork.unavailable_next_execute().await;
                    let result = Reconciler::new(fork.clone(), enabled())
                        .reconcile("tests", "db")
                        .await
                        .unwrap();
                    assert_eq!(result.kind, ReconcileKind::Waiting);
                    assert_eq!(
                        next_plan(&fork).await.1,
                        plan,
                        "undelivered request retains exact command"
                    );
                    // The same ambiguity also permits a completed effect with no reply.
                    apply_command(&fork, command).await;
                    assert_eq!(fork.observation().await.set.status, raw.set.status);
                    commands += 1;
                }
                _ => {}
            }
            let restarted = Arc::new(InMemoryClusterApi::new(fork.observation().await));
            finish(&restarted).await;
            let final_raw = restarted.observation().await;
            let status = final_raw.set.status.unwrap().authority;
            assert_eq!(
                status.topology.unwrap().configuration.members.len(),
                desired as usize
            );
            assert!(status.transition.is_none() && status.secondary_scale_down_cleanup.is_none());
            assert_eq!(final_raw.pods.len(), desired as usize);
            assert_eq!(final_raw.pvcs.len(), desired as usize);
            tick(&baseline).await;
        }
        assert!(
            statuses >= 4,
            "intent/evidence/commit/cleanup status boundaries"
        );
        assert!(
            commands >= 5,
            "prepare/member/current-only/accept/retire boundaries"
        );
    }
}

#[tokio::test]
async fn primary_or_retained_loss_at_every_precommit_boundary_preserves_authority_then_heals() {
    let baseline = Arc::new(InMemoryClusterApi::new(fixture(3, 2)));
    tick(&baseline).await;
    let mut boundaries = 0;
    for _ in 0..80 {
        let (raw, _) = next_plan(&baseline).await;
        let before = raw.set.status.as_ref().unwrap().authority.clone();
        if before.secondary_scale_down_cleanup.is_some() {
            break;
        }
        for id in [1, 2] {
            let mut lost = raw.clone();
            let key = ReplicaObservationKey::new(
                ReplicaId::new(id),
                ReplicaInstanceId::new(format!("pod-uid-{id}")),
            );
            let original = lost
                .agents
                .insert(
                    key.clone(),
                    RawAgentObservation::Unavailable {
                        message: "exact retained process lost".into(),
                    },
                )
                .unwrap();
            let api = Arc::new(InMemoryClusterApi::new(lost));
            for _ in 0..8 {
                tick(&api).await;
                let after = api.observation().await;
                let status = after.set.status.unwrap().authority;
                assert_eq!(status.topology, before.topology);
                assert_eq!(status.effective_policy, before.effective_policy);
                assert_eq!(
                    status.transition.as_ref().unwrap().secondary_scale_down,
                    before.transition.as_ref().unwrap().secondary_scale_down
                );
                assert!(status.secondary_scale_down_cleanup.is_none());
                assert_eq!(after.pods, raw.pods);
                assert_eq!(after.pvcs, raw.pvcs);
                assert!(!api.effects().await.iter().any(|e| matches!(
                    e,
                    EffectRecord::DeleteScaleDownResource { .. }
                        | EffectRecord::DeleteScaffolding { .. }
                )));
            }
            let mut healed = api.observation().await;
            healed.agents.insert(key, original);
            let restarted = Arc::new(InMemoryClusterApi::new(healed));
            finish(&restarted).await;
            assert_eq!(restarted.observation().await.pods.len(), 2);
        }
        boundaries += 1;
        tick(&baseline).await;
    }
    assert!(boundaries >= 8);
}

#[tokio::test]
async fn target_return_after_commit_is_retired_before_sequential_next_removal() {
    let mut original = fixture(3, 1);
    let key = ReplicaObservationKey::new(ReplicaId::new(3), ReplicaInstanceId::new("pod-uid-3"));
    let target = original
        .agents
        .insert(
            key.clone(),
            RawAgentObservation::Unavailable {
                message: "partitioned target".into(),
            },
        )
        .unwrap();
    let api = Arc::new(InMemoryClusterApi::new(original));
    at_delete(&api, ScaleDownResource::Endpoint).await;
    let mut returned = api.observation().await;
    let accepted = returned
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .topology
        .clone();
    assert_eq!(accepted.as_ref().unwrap().configuration.members.len(), 2);
    let RawAgentObservation::Report(mut target) = target else {
        unreachable!()
    };
    target.process_session_id = "returned-new-session".into();
    target.report_sequence = 1;
    returned
        .agents
        .insert(key, RawAgentObservation::Report(target));
    let restarted = Arc::new(InMemoryClusterApi::new(returned));
    finish(&restarted).await;
    let effects = restarted.effects().await;
    let retired = effects.iter().position(|e| matches!(e,
                EffectRecord::Execute(ProtocolCommand::RetireReplica(c)) if c.local_replica_id == ReplicaId::new(3))).unwrap();
    let pvc = effects.iter().position(|e| matches!(e,
                EffectRecord::DeleteScaleDownResource { resource: ScaleDownResource::Pvc, uid, .. } if uid == "pvc-uid-3")).unwrap();
    let next = effects.iter().position(|e| matches!(e,
                EffectRecord::Execute(ProtocolCommand::PrepareSecondaryRemoval(c)) if c.intent.target.replica_id == ReplicaId::new(2))).unwrap();
    assert!(retired < pvc && pvc < next);
    assert_eq!(restarted.observation().await.pods.len(), 1);
    assert!(!effects.iter().any(|e| matches!(e,
                EffectRecord::Execute(ProtocolCommand::EnsureConfiguration(c)) if c.local_replica_id == ReplicaId::new(3))));
}

#[tokio::test]
async fn contradictory_current_only_reports_cannot_publish_reduced_status() {
    let baseline = Arc::new(InMemoryClusterApi::new(fixture(3, 2)));
    for _ in 0..80 {
        let (_, plan) = next_plan(&baseline).await;
        if matches!(plan, Plan::Apply { changes }
                    if matches!(changes.first(), Some(KubernetesChange::PersistStatus { status })
                        if status.secondary_scale_down_cleanup.is_some()))
        {
            break;
        }
        tick(&baseline).await;
    }
    let original = baseline.observation().await;
    let before = original.set.status.as_ref().unwrap().authority.clone();
    let key = ReplicaObservationKey::new(ReplicaId::new(1), ReplicaInstanceId::new("pod-uid-1"));
    for mutation in 0..4 {
        let mut broken = original.clone();
        let RawAgentObservation::Report(report) = broken.agents.get_mut(&key).unwrap() else {
            unreachable!()
        };
        match mutation {
            0 => report.verified_replication_lsn = None,
            1 => report.pending_operation_id = "unrelated-command".into(),
            2 => report.epoch.as_mut().unwrap().configuration_number += 1,
            _ => report.identity.as_mut().unwrap().agent_generation = "different-generation".into(),
        }
        let api = Arc::new(InMemoryClusterApi::new(broken));
        for _ in 0..3 {
            let count = api.observation_count().await;
            Reconciler::new(api.clone(), enabled())
                .reconcile("tests", "db")
                .await
                .unwrap();
            assert_eq!(api.observation_count().await, count + 1);
            assert!(api.effects().await.iter().all(|e| matches!(
                e,
                EffectRecord::RemoveWriteRouting | EffectRecord::ReplaceStatus
            )));
            let observed = api.observation().await;
            let status = observed.set.status.unwrap().authority;
            assert_eq!(status.topology, before.topology, "mutation={mutation}");
            assert_eq!(status.transition, before.transition);
            assert!(status.secondary_scale_down_cleanup.is_none());
            assert_eq!(observed.pods, original.pods);
            assert_eq!(observed.pvcs, original.pvcs);
            assert!(!api.effects().await.iter().any(|e| matches!(
                e,
                EffectRecord::DeleteScaleDownResource { .. }
                    | EffectRecord::DeleteScaffolding { .. }
            )));
        }
        let mut healed = api.observation().await;
        healed
            .agents
            .insert(key.clone(), original.agents[&key].clone());
        let restarted = Arc::new(InMemoryClusterApi::new(healed));
        finish(&restarted).await;
        assert_eq!(restarted.observation().await.pods.len(), 2);
    }
}
