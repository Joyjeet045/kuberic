use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::{
    PersistentVolumeClaim, PersistentVolumeClaimVolumeSource, Pod, PodCondition, PodSpec,
    PodStatus, Secret, Service, ServicePort, ServiceSpec, Volume,
};
use kube::ResourceExt;
use kuberic_controller::ControllerError;
use kuberic_controller::cluster_api::{ClusterApi, EffectRecord, InMemoryClusterApi};
use kuberic_controller::crd::{
    INSTANCE_LABEL, KubericSet, KubericSetSpec, KubericSetStatus, PlannedSwitchoverRequestSpec,
    REPLICA_ID_LABEL, SET_UID_LABEL,
};
use kuberic_controller::normalize::normalize;
use kuberic_controller::observation::{RawAgentObservation, RawObservation, RawObservationFailure};
use kuberic_controller::reconciler::{ReconcileKind, Reconciler};
use kuberic_protocol::command::ProtocolCommand;
use kuberic_protocol::evaluator::{EvaluationConfig, evaluate};
use kuberic_protocol::observation::{AgentObservation, ReplicaObservationKey};
use kuberic_protocol::plan::Plan;
use kuberic_protocol::types::{
    AcceptedStatus, AcceptedTopology, PodUid, PvcUid, ReplicaId, ReplicaIdentity,
    ReplicaInstanceId, ReplicaRole, ResourceUid, derive_agent_generation, derive_initialization_id,
    derive_replica_endpoint_name,
};
use kuberic_wire::proto;

const UID: &str = "set-uid";
const POD_UID: &str = "pod-uid-1";
const PVC_UID: &str = "pvc-uid-1";

fn config() -> EvaluationConfig {
    EvaluationConfig {
        supported_protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        stable_resync_seconds: 11,
        wait_requeue_seconds: 3,
        unsafe_requeue_seconds: 7,
    }
}

fn raw(replicas: u32) -> RawObservation {
    let mut set = KubericSet::new(
        "db",
        KubericSetSpec {
            replicas,
            image: "example/db:latest".to_string(),
            failover_delay_seconds: 9,
            switchover: None,
        },
    );
    set.metadata.namespace = Some("tests".to_string());
    set.metadata.uid = Some(UID.to_string());
    set.metadata.resource_version = Some("1".to_string());
    set.metadata.generation = Some(1);
    RawObservation {
        set,
        pods: Vec::new(),
        pvcs: Vec::new(),
        services: vec![peer_service()],
        secrets: vec![credential_secret()],
        agents: BTreeMap::new(),
        failures: Vec::new(),
        now_unix_seconds: 100,
    }
}

#[test]
fn normalization_projects_planned_switchover_user_intent() {
    let mut observation = raw(3);
    observation.set.spec.switchover = Some(PlannedSwitchoverRequestSpec {
        request_id: "request-1".to_string(),
        target_replica_id: 2,
    });

    let snapshot = normalize(observation, BTreeMap::new()).unwrap();
    let request = snapshot.desired.switchover.unwrap();
    assert_eq!(request.request_id.as_str(), "request-1");
    assert_eq!(request.target_replica_id, ReplicaId::new(2));
}

fn labels(replica_id: ReplicaId) -> BTreeMap<String, String> {
    BTreeMap::from([
        (SET_UID_LABEL.to_string(), UID.to_string()),
        (REPLICA_ID_LABEL.to_string(), replica_id.to_string()),
    ])
}

fn with_scaffolding(mut raw: RawObservation) -> RawObservation {
    let initialization_id = derive_initialization_id(
        &ResourceUid::new(UID),
        ReplicaId::new(1),
        &PodUid::new(POD_UID),
        &PvcUid::new(PVC_UID),
    );
    let identity = ReplicaIdentity {
        replica_id: ReplicaId::new(1),
        instance_id: ReplicaInstanceId::new(POD_UID),
        agent_generation: derive_agent_generation(&initialization_id),
    };
    let mut pod_labels = labels(ReplicaId::new(1));
    pod_labels.insert(INSTANCE_LABEL.to_string(), POD_UID.to_string());
    raw.pods.push(Pod {
        metadata: kube::core::ObjectMeta {
            name: Some("db-1".to_string()),
            namespace: Some("tests".to_string()),
            uid: Some(POD_UID.to_string()),
            resource_version: Some("2".to_string()),
            labels: Some(pod_labels),
            ..Default::default()
        },
        status: Some(PodStatus {
            conditions: Some(vec![PodCondition {
                last_probe_time: None,
                last_transition_time: None,
                message: None,
                reason: None,
                status: "True".to_string(),
                type_: "Ready".to_string(),
            }]),
            pod_ip: Some("127.0.0.1".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    });
    raw.pvcs.push(PersistentVolumeClaim {
        metadata: kube::core::ObjectMeta {
            name: Some("db-1-data".to_string()),
            namespace: Some("tests".to_string()),
            uid: Some(PVC_UID.to_string()),
            resource_version: Some("3".to_string()),
            labels: Some(labels(ReplicaId::new(1))),
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
            labels: Some(BTreeMap::from([(
                SET_UID_LABEL.to_string(),
                UID.to_string(),
            )])),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(BTreeMap::from([(
                INSTANCE_LABEL.to_string(),
                POD_UID.to_string(),
            )])),
            ports: Some(vec![
                ServicePort {
                    name: Some("control".to_string()),
                    port: 50051,
                    ..Default::default()
                },
                ServicePort {
                    name: Some("replication".to_string()),
                    port: 50052,
                    ..Default::default()
                },
            ]),
            ..Default::default()
        }),
        ..Default::default()
    });
    raw
}

fn replica_key(pod_uid: &str) -> ReplicaObservationKey {
    ReplicaObservationKey::new(ReplicaId::new(1), ReplicaInstanceId::new(pod_uid))
}

fn write_service(instance: &str) -> Service {
    Service {
        metadata: kube::core::ObjectMeta {
            name: Some("db-write".to_string()),
            uid: Some("service-uid".to_string()),
            resource_version: Some("4".to_string()),
            labels: Some(BTreeMap::from([(
                SET_UID_LABEL.to_string(),
                UID.to_string(),
            )])),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(BTreeMap::from([(
                INSTANCE_LABEL.to_string(),
                instance.to_string(),
            )])),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn peer_service() -> Service {
    Service {
        metadata: kube::core::ObjectMeta {
            name: Some("db-peer".to_string()),
            labels: Some(BTreeMap::from([(
                SET_UID_LABEL.to_string(),
                UID.to_string(),
            )])),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            publish_not_ready_addresses: Some(true),
            selector: Some(BTreeMap::from([(
                SET_UID_LABEL.to_string(),
                UID.to_string(),
            )])),
            ports: Some(vec![
                ServicePort {
                    name: Some("control".to_string()),
                    port: 50051,
                    ..Default::default()
                },
                ServicePort {
                    name: Some("replication".to_string()),
                    port: 50052,
                    ..Default::default()
                },
            ]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn credential_secret() -> Secret {
    Secret {
        metadata: kube::core::ObjectMeta {
            name: Some("db-agent-credentials".to_string()),
            labels: Some(BTreeMap::from([(
                SET_UID_LABEL.to_string(),
                UID.to_string(),
            )])),
            ..Default::default()
        },
        data: Some(BTreeMap::from([(
            "bearer-token".to_string(),
            k8s_openapi::ByteString(b"token".to_vec()),
        )])),
        ..Default::default()
    }
}

fn fresh_bootstrap_observation() -> RawObservation {
    let mut observation = with_scaffolding(raw(1));
    observation.services.push(write_service("disabled"));
    observation.agents.insert(
        replica_key(POD_UID),
        RawAgentObservation::Report(Box::new(uninitialized_report())),
    );
    observation
}

fn bootstrap_status(raw: &RawObservation) -> AcceptedStatus {
    let mut candidate = raw.clone();
    if !candidate
        .services
        .iter()
        .any(|service| service.name_any() == "db-write")
    {
        candidate.services.push(write_service("disabled"));
    }
    candidate.agents.insert(
        replica_key(POD_UID),
        RawAgentObservation::Report(Box::new(uninitialized_report())),
    );
    let snapshot = normalize(candidate, BTreeMap::new()).unwrap();
    let Plan::Apply { changes } = evaluate(&snapshot, &config()) else {
        panic!("complete scaffolding must persist bootstrap intent");
    };
    changes
        .into_iter()
        .find_map(|change| match change {
            kuberic_protocol::command::KubernetesChange::PersistStatus { status } => Some(*status),
            _ => None,
        })
        .unwrap()
}

fn uninitialized_report() -> proto::AgentStatusReport {
    proto::AgentStatusReport {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: UID.to_string(),
        process_session_id: "session-1".to_string(),
        report_sequence: 1,
        storage_state: proto::AgentStorageState::Uninitialized as i32,
        pod_uid: POD_UID.to_string(),
        pvc_uid: PVC_UID.to_string(),
        replica_id: 1,
        ..Default::default()
    }
}

fn initialized_report(
    identity: kuberic_protocol::types::ReplicaIdentity,
    configuration: kuberic_protocol::types::ConfigurationDescriptor,
) -> proto::AgentStatusReport {
    proto::AgentStatusReport {
        protocol_version: kuberic_protocol::PROTOCOL_VERSION,
        resource_uid: UID.to_string(),
        identity: Some(identity.into()),
        process_session_id: "session-2".to_string(),
        report_sequence: 2,
        role: proto::ReplicaRole::Primary as i32,
        read_status: proto::AccessStatus::Granted as i32,
        write_status: proto::AccessStatus::Granted as i32,
        epoch: Some(configuration.epoch.into()),
        current_configuration: Some(configuration.into()),
        current_progress: 5,
        committed_lsn: 5,
        catch_up_capability: Some(5),
        current_configuration_quorum_progress: 5,
        catch_up_complete: true,
        storage_state: proto::AgentStorageState::Initialized as i32,
        healthy: true,
        replica_id: 1,
        ..Default::default()
    }
}

fn stable_observation() -> RawObservation {
    let scaffold = with_scaffolding(raw(1));
    let transition = bootstrap_status(&scaffold)
        .transition
        .expect("bootstrap transition");
    let configuration = transition.current_configuration;
    let identity = configuration.members[0].identity.clone();
    let mut stable = scaffold;
    stable
        .pods
        .first_mut()
        .unwrap()
        .metadata
        .labels
        .get_or_insert_default()
        .insert(INSTANCE_LABEL.to_string(), POD_UID.to_string());
    stable.set.status = Some(KubericSetStatus {
        authority: AcceptedStatus {
            initialized: true,
            observed_generation: 1,
            effective_policy: Some(kuberic_protocol::types::EffectivePolicy::fixed(1, 10).unwrap()),
            topology: Some(AcceptedTopology {
                configuration: configuration.clone(),
            }),
            ..Default::default()
        },
    });
    stable.agents.insert(
        replica_key(POD_UID),
        RawAgentObservation::Report(Box::new(initialized_report(
            identity.clone(),
            configuration,
        ))),
    );
    stable
        .services
        .push(write_service(identity.instance_id.as_str()));
    stable
}

fn switchover_observation() -> RawObservation {
    let mut observation = stable_observation();
    observation.set.spec.replicas = 3;
    observation.set.spec.failover_delay_seconds = 10;
    observation.set.spec.switchover = Some(PlannedSwitchoverRequestSpec {
        request_id: "move-primary".to_string(),
        target_replica_id: 2,
    });
    observation.set.metadata.generation = Some(2);
    let primary = observation
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .members[0]
        .clone();
    let mut members = vec![primary];
    for id in 2..=3 {
        let pod_uid = format!("pod-uid-{id}");
        let pvc_uid = format!("pvc-uid-{id}");
        let local = ReplicaIdentity {
            replica_id: ReplicaId::new(id),
            instance_id: ReplicaInstanceId::new(&pod_uid),
            agent_generation: derive_agent_generation(&derive_initialization_id(
                &ResourceUid::new(UID),
                ReplicaId::new(id),
                &PodUid::new(&pod_uid),
                &PvcUid::new(&pvc_uid),
            )),
        };
        let mut pod = observation.pods[0].clone();
        pod.metadata.uid = Some(pod_uid.clone());
        pod.metadata.name = Some(format!("db-{id}"));
        let mut pod_labels = labels(local.replica_id);
        pod_labels.insert(INSTANCE_LABEL.to_string(), pod_uid.clone());
        pod.metadata.labels = Some(pod_labels);
        observation.pods.push(pod);
        let mut pvc = observation.pvcs[0].clone();
        pvc.metadata.name = Some(format!("db-{id}-data"));
        pvc.metadata.uid = Some(pvc_uid);
        pvc.metadata.labels = Some(labels(local.replica_id));
        observation.pvcs.push(pvc);
        let mut endpoint = observation
            .services
            .iter()
            .find(|service| service.name_any().starts_with("kr-"))
            .unwrap()
            .clone();
        endpoint.metadata.name = Some(derive_replica_endpoint_name(&ResourceUid::new(UID), &local));
        endpoint.spec.as_mut().unwrap().selector =
            Some(BTreeMap::from([(INSTANCE_LABEL.to_string(), pod_uid)]));
        observation.services.push(endpoint);
        members.push(kuberic_protocol::types::ConfigurationMember {
            identity: local,
            role: ReplicaRole::ActiveSecondary,
        });
    }
    let configuration = kuberic_protocol::types::ConfigurationDescriptor::new(
        kuberic_protocol::types::Epoch::new(0, 1),
        ReplicaId::new(1),
        members,
        2,
    );
    let status = &mut observation.set.status.as_mut().unwrap().authority;
    status.effective_policy = kuberic_protocol::types::EffectivePolicy::fixed(3, 10);
    status.topology = Some(AcceptedTopology {
        configuration: configuration.clone(),
    });
    for pod in &mut observation.pods {
        pod.spec = Some(PodSpec {
            containers: vec![k8s_openapi::api::core::v1::Container {
                name: "replica".to_string(),
                image: Some(observation.set.spec.image.clone()),
                ..Default::default()
            }],
            ..Default::default()
        });
    }
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
        observation.agents.insert(
            ReplicaObservationKey::new(
                member.identity.replica_id,
                member.identity.instance_id.clone(),
            ),
            RawAgentObservation::Report(Box::new(report)),
        );
    }
    observation
}

async fn refresh_switchover_reports(api: &InMemoryClusterApi) {
    let mut observation = api.observation().await;
    for agent in observation.agents.values_mut() {
        if let RawAgentObservation::Report(report) = agent {
            report.report_sequence += 1;
        }
    }
    api.set_observation(observation).await;
}

async fn observe_switchover_result(api: &InMemoryClusterApi, command: &ProtocolCommand) {
    let mut observation = api.observation().await;
    let (id, instance) = match command {
        ProtocolCommand::PrepareSwitchover(command) => {
            (command.local_replica_id, &command.expected_instance_id)
        }
        ProtocolCommand::EnsureConfiguration(command) => {
            (command.local_replica_id, &command.expected_instance_id)
        }
        _ => panic!("unexpected switchover command"),
    };
    let RawAgentObservation::Report(report) = observation
        .agents
        .get_mut(&ReplicaObservationKey::new(id, instance.clone()))
        .unwrap()
    else {
        panic!("exact report")
    };
    match command {
        ProtocolCommand::PrepareSwitchover(command) => {
            report.write_status = proto::AccessStatus::ReconfigurationPending as i32;
            report.prepared_switchover = Some(
                kuberic_protocol::types::SwitchoverHandoff {
                    preparation_generation: command.preparation_generation,
                    preparation_operation_id: command.operation_id.clone(),
                    request_id: command.request_id.clone(),
                    source: command.source.clone(),
                    target: command.target.clone(),
                    starting_configuration_id: command
                        .current_configuration
                        .configuration_id
                        .clone(),
                    starting_epoch: command.current_configuration.epoch,
                    handoff_lsn: 5,
                }
                .into(),
            );
        }
        ProtocolCommand::EnsureConfiguration(command) => {
            report.epoch = Some(command.current_epoch.into());
            report.previous_configuration = command.previous_configuration.clone().map(Into::into);
            report.current_configuration = Some(command.current_configuration.clone().into());
            let primary = command.current_configuration.primary_id == id;
            report.role = if primary {
                proto::ReplicaRole::Primary as i32
            } else {
                proto::ReplicaRole::ActiveSecondary as i32
            };
            report.write_status = if primary
                && command.primary_write_status == kuberic_protocol::types::AccessStatus::Granted
            {
                proto::AccessStatus::Granted as i32
            } else if primary {
                proto::AccessStatus::ReconfigurationPending as i32
            } else {
                proto::AccessStatus::NotPrimary as i32
            };
            report.retained_operation_id = command.operation_id.to_string();
            report.pending_operation_id.clear();
            report.catch_up_boundary = command.previous_configuration.as_ref().map(|_| 5);
            report.catch_up_complete = true;
            report.current_configuration_quorum_progress = 5;
            if !command.retire_switchover_preparation_ids.is_empty() {
                report.prepared_switchover = None;
            }
        }
        _ => unreachable!(),
    }
    api.set_observation(observation).await;
}

#[tokio::test]
async fn switchover_request_mutations_at_every_boundary_heal_without_watch_events() {
    let baseline = Arc::new(InMemoryClusterApi::new(switchover_observation()));
    Reconciler::new(baseline.clone(), config())
        .reconcile("tests", "db")
        .await
        .unwrap();
    let mut boundaries = 0;
    while baseline
        .observation()
        .await
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .transition
        .is_some()
    {
        for mutation in ["cancel", "retarget", "new-request"] {
            let mut raw = baseline.observation().await;
            let frozen = raw
                .set
                .status
                .as_ref()
                .unwrap()
                .authority
                .transition
                .clone()
                .unwrap();
            let original = raw.set.spec.clone();
            raw.set.spec.switchover = match mutation {
                "cancel" => None,
                "retarget" => Some(PlannedSwitchoverRequestSpec {
                    request_id: "move-primary".into(),
                    target_replica_id: 3,
                }),
                _ => Some(PlannedSwitchoverRequestSpec {
                    request_id: "other-request".into(),
                    target_replica_id: 2,
                }),
            };
            raw.set.spec.replicas = 5;
            raw.set.metadata.generation = Some(3);
            let api = Arc::new(InMemoryClusterApi::new(raw));
            let stale = api.observation().await;
            let mut newer = stale.clone();
            newer.set.metadata.resource_version = Some("newer-request".into());
            api.set_observation(newer).await;
            assert!(matches!(
                api.replace_status(&stale, &stale.set.status.as_ref().unwrap().authority)
                    .await,
                Err(ControllerError::ObservationStale)
            ));
            assert!(api.effects().await.is_empty());
            refresh_switchover_reports(&api).await;
            Reconciler::new(api.clone(), config())
                .reconcile("tests", "db")
                .await
                .unwrap();
            let rejected = api.observation().await.set.status.unwrap().authority;
            assert_eq!(rejected.transition, Some(frozen.clone()));
            assert!(
                rejected
                    .conditions
                    .iter()
                    .any(|c| c.reason == "ActiveRequestImmutable")
            );
            let mut repaired = api.observation().await;
            repaired.set.spec = original;
            repaired.set.metadata.generation = Some(4);
            api.set_observation(repaired).await;
            let mut completed = false;
            let mut waited = false;
            let mut command_count = 0;
            for _ in 0..40 {
                refresh_switchover_reports(&api).await;
                let observation = api.observation().await;
                let snapshot = normalize(observation.clone(), BTreeMap::new()).unwrap();
                let plan = evaluate(&snapshot, &config());
                // A fresh reconciler at each tick has neither watch events nor an in-memory phase.
                let reconciler = Reconciler::new(api.clone(), config());
                if let Plan::Execute { command } = &plan {
                    if !waited && snapshot.status.transition.is_some() {
                        let mut partitioned = observation.clone();
                        for agent in partitioned.agents.values_mut() {
                            *agent = RawAgentObservation::Unavailable {
                                message: "lost observation".into(),
                            };
                        }
                        api.set_observation(partitioned).await;
                        let result = reconciler.reconcile("tests", "db").await.unwrap();
                        assert_eq!(result.kind, ReconcileKind::Waiting);
                        assert_eq!(result.requeue_after, Duration::from_secs(3));
                        let waiting_authority = api.observation().await.set.status;
                        let mut healed = observation;
                        healed.set.status = waiting_authority;
                        for (key, agent) in &mut healed.agents {
                            if let RawAgentObservation::Report(report) = agent {
                                report.process_session_id = format!("rollover-{}", key.replica_id);
                                report.report_sequence = 1;
                            }
                        }
                        api.set_observation(healed).await;
                        waited = true;
                        continue;
                    }
                    api.unavailable_next_execute().await;
                    assert_eq!(
                        reconciler.reconcile("tests", "db").await.unwrap().kind,
                        ReconcileKind::Waiting
                    );
                    observe_switchover_result(&api, command).await;
                    command_count += 1;
                } else {
                    let result = reconciler.reconcile("tests", "db").await.unwrap();
                    if matches!(plan, Plan::Stable { .. }) {
                        assert_eq!(result.kind, ReconcileKind::Stable);
                        completed = true;
                        break;
                    }
                    assert_eq!(result.kind, ReconcileKind::Applied, "{plan:?}");
                }
                let now = normalize(api.observation().await, BTreeMap::new()).unwrap();
                if let Some(transition) = &now.status.transition {
                    assert_eq!(transition.transition_id, frozen.transition_id);
                    assert_eq!(
                        transition.current_configuration,
                        frozen.current_configuration
                    );
                }
                if let Some(routed) = &now.routing.write_target {
                    if routed.replica_id == ReplicaId::new(2) {
                        assert!(now.status.transition.is_none());
                        assert!(now.status.last_switchover.is_some());
                    } else {
                        assert_eq!(routed.replica_id, ReplicaId::new(1));
                    }
                    assert!(
                        matches!(&now.observation_for_identity(routed).unwrap().agent,
                        AgentObservation::Report(report) if report.write_status == kuberic_protocol::types::AccessStatus::Granted)
                    );
                }
            }
            assert!(completed && command_count > 0, "{mutation}/{boundaries}");
            let before_retry = api
                .observation()
                .await
                .set
                .status
                .unwrap()
                .authority
                .last_switchover;
            let effects_before = api.effects().await.len();
            for _ in 0..3 {
                refresh_switchover_reports(&api).await;
                assert_eq!(
                    Reconciler::new(api.clone(), config())
                        .reconcile("tests", "db")
                        .await
                        .unwrap()
                        .kind,
                    ReconcileKind::Stable
                );
            }
            assert_eq!(
                api.observation()
                    .await
                    .set
                    .status
                    .unwrap()
                    .authority
                    .last_switchover,
                before_retry
            );
            assert!(
                api.effects().await[effects_before..]
                    .iter()
                    .all(|effect| !matches!(effect, EffectRecord::Execute(_)))
            );
        }
        refresh_switchover_reports(&baseline).await;
        let snapshot = normalize(baseline.observation().await, BTreeMap::new()).unwrap();
        let plan = evaluate(&snapshot, &config());
        Reconciler::new(baseline.clone(), config())
            .reconcile("tests", "db")
            .await
            .unwrap();
        if let Plan::Execute { command } = plan {
            observe_switchover_result(&baseline, &command).await;
        }
        boundaries += 1;
        assert!(boundaries < 20);
    }
    assert!(boundaries >= 10);
}

#[tokio::test]
async fn switchover_recovery_reobserves_lost_effects_allocation_receipts_and_session_rollover() {
    use kuberic_protocol::types::{PlannedSwitchoverOutcome, PlannedSwitchoverResolution};
    for compensate in [false, true] {
        let api = Arc::new(InMemoryClusterApi::new(switchover_observation()));
        let mut injected = false;
        let mut admitted_commands = 0;
        let mut allocation_conflict = false;
        let mut receipt_conflict = false;
        let mut rolled_session = false;
        let mut completed = false;
        for _ in 0..45 {
            refresh_switchover_reports(&api).await;
            let snapshot = normalize(api.observation().await, BTreeMap::new()).unwrap();
            if !injected
                && snapshot
                    .status
                    .transition
                    .as_ref()
                    .and_then(|transition| transition.switchover.as_ref())
                    .is_some_and(|intent| intent.handoff.is_some())
                && (!compensate || admitted_commands == 1)
            {
                let mut observation = api.observation().await;
                let key = ReplicaObservationKey::new(
                    ReplicaId::new(2),
                    ReplicaInstanceId::new("pod-uid-2"),
                );
                let RawAgentObservation::Report(target) = observation.agents.get_mut(&key).unwrap()
                else {
                    unreachable!()
                };
                target.reported_fault = proto::FaultType::Permanent as i32;
                api.set_observation(observation).await;
                injected = true;
                continue;
            }
            let plan = evaluate(&snapshot, &config());
            // Each controller restart reads authority rather than replaying an in-memory phase.
            let reconciler = Reconciler::new(api.clone(), config());
            if let Plan::Apply { changes } = &plan {
                let recovery = changes.iter().find_map(|change| match change {
                    kuberic_protocol::command::KubernetesChange::PersistStatus { status } => {
                        Some(status)
                    }
                    _ => None,
                });
                if let Some(status) = recovery {
                    let recovery_allocated = status
                        .transition
                        .as_ref()
                        .and_then(|transition| transition.switchover.as_ref())
                        .is_some_and(|intent| {
                            matches!(
                                intent.resolution,
                                PlannedSwitchoverResolution::RestoringOldPrimary
                                    | PlannedSwitchoverResolution::CompensatingOldPrimary
                            )
                        });
                    if (recovery_allocated && !allocation_conflict)
                        || (status.last_switchover.is_some() && !receipt_conflict)
                    {
                        if status.last_switchover.is_some() {
                            receipt_conflict = true;
                        } else {
                            allocation_conflict = true;
                        }
                        api.conflict_next_status().await;
                        assert_eq!(
                            reconciler.reconcile("tests", "db").await.unwrap().kind,
                            ReconcileKind::ObservationStale
                        );
                        continue;
                    }
                }
            }
            if allocation_conflict
                && snapshot
                    .status
                    .transition
                    .as_ref()
                    .and_then(|transition| transition.switchover.as_ref())
                    .is_some_and(|intent| {
                        intent.resolution != PlannedSwitchoverResolution::RequestedTarget
                    })
                && !rolled_session
            {
                let mut observation = api.observation().await;
                for (key, agent) in &mut observation.agents {
                    if let RawAgentObservation::Report(report) = agent {
                        report.reported_fault = proto::FaultType::Unknown as i32;
                        report.process_session_id = format!("restarted-{}", key.replica_id);
                        report.report_sequence = 1;
                    }
                }
                api.set_observation(observation).await;
                rolled_session = true;
                continue;
            }
            if let Plan::Execute { command } = &plan {
                if let ProtocolCommand::EnsureConfiguration(command) = command {
                    if command.primary_write_status
                        == kuberic_protocol::types::AccessStatus::Granted
                    {
                        assert!(snapshot.status.transition.is_none());
                        assert!(snapshot.status.last_switchover.is_some());
                    } else if !injected {
                        admitted_commands += 1;
                    }
                }
                api.unavailable_next_execute().await;
                assert_eq!(
                    reconciler.reconcile("tests", "db").await.unwrap().kind,
                    ReconcileKind::Waiting
                );
                observe_switchover_result(&api, command).await;
            } else {
                let result = reconciler.reconcile("tests", "db").await.unwrap();
                if matches!(plan, Plan::Stable { .. }) {
                    assert_eq!(result.kind, ReconcileKind::Stable);
                    completed = true;
                    break;
                }
                assert_eq!(result.kind, ReconcileKind::Applied, "{plan:?}");
            }
        }
        assert!(completed && allocation_conflict && receipt_conflict && rolled_session);
        let snapshot = normalize(api.observation().await, BTreeMap::new()).unwrap();
        let outcome = if compensate {
            PlannedSwitchoverOutcome::OldPrimaryCompensated
        } else {
            PlannedSwitchoverOutcome::OldPrimaryRestored
        };
        assert_eq!(
            snapshot.status.last_switchover.as_ref().unwrap().outcome,
            outcome
        );
        assert_eq!(
            snapshot
                .status
                .topology
                .as_ref()
                .unwrap()
                .configuration
                .epoch
                .configuration_number,
            if compensate { 3 } else { 1 }
        );
        assert_eq!(
            snapshot.routing.write_target.unwrap().replica_id,
            ReplicaId::new(1)
        );
    }
}

#[tokio::test]
async fn exact_safety_deletion_is_uid_and_resource_version_fenced_and_preserves_pvcs() {
    let observation = switchover_observation();
    let api = InMemoryClusterApi::new(observation.clone());
    let name = observation.pods[0].name_any();
    let uid = PodUid::new(POD_UID);
    let pvcs = observation.pvcs.clone();
    let mut replaced = observation.clone();
    replaced.pods[0].metadata.uid = Some("replacement-pod".into());
    api.set_observation(replaced).await;
    assert!(matches!(
        api.delete_exact_pod(&observation, &name, &uid).await,
        Err(ControllerError::ObservationStale)
    ));
    let mut changed = observation.clone();
    changed.pods[0].metadata.resource_version = Some("changed-version".into());
    api.set_observation(changed).await;
    assert!(matches!(
        api.delete_exact_pod(&observation, &name, &uid).await,
        Err(ControllerError::ObservationStale)
    ));
    api.set_observation(observation.clone()).await;
    api.delete_exact_pod(&observation, &name, &uid)
        .await
        .unwrap();
    let after = api.observation().await;
    assert_eq!(after.pods.len(), observation.pods.len() - 1);
    assert_eq!(after.pvcs, pvcs);
    assert_eq!(after.services, observation.services);
    assert!(
        matches!(api.effects().await.as_slice(), [EffectRecord::DeleteExactPod { pod_uid, .. }] if pod_uid == &uid)
    );

    let mut overlap = observation.clone();
    let mut replacement = overlap.pods[0].clone();
    replacement.metadata.name = Some("db-1-new-incarnation".into());
    replacement.metadata.uid = Some("replacement-pod".into());
    overlap.pods.push(replacement.clone());
    api.set_observation(overlap.clone()).await;
    let mut unversioned = overlap.clone();
    unversioned.pods[0].metadata.resource_version = None;
    assert!(matches!(
        api.delete_exact_pod(&unversioned, &name, &uid).await,
        Err(ControllerError::ObservationStale)
    ));
    api.delete_exact_pod(&overlap, &name, &uid).await.unwrap();
    let after = api.observation().await;
    assert!(after.pods.contains(&replacement));
    assert!(
        !after
            .pods
            .iter()
            .any(|pod| pod.uid().as_deref() == Some(POD_UID))
    );
    assert_eq!(after.pvcs, pvcs);
    assert_eq!(after.services, overlap.services);
}

#[tokio::test]
async fn persistent_fault_compensation_deletes_exact_pod_without_waiting_for_its_commands() {
    use kuberic_protocol::command::KubernetesChange;
    use kuberic_protocol::types::PlannedSwitchoverOutcome;
    let api = Arc::new(InMemoryClusterApi::new(switchover_observation()));
    let pvcs = api.observation().await.pvcs;
    let mut faulted = false;
    let mut deleted = false;
    let mut source_admitted = false;
    let mut completed = false;
    for _ in 0..40 {
        refresh_switchover_reports(&api).await;
        let snapshot = normalize(api.observation().await, BTreeMap::new()).unwrap();
        if source_admitted && !faulted {
            let mut observation = api.observation().await;
            let RawAgentObservation::Report(report) = observation
                .agents
                .get_mut(&ReplicaObservationKey::new(
                    ReplicaId::new(2),
                    ReplicaInstanceId::new("pod-uid-2"),
                ))
                .unwrap()
            else {
                panic!("target report")
            };
            report.reported_fault = proto::FaultType::Permanent as i32;
            api.set_observation(observation).await;
            faulted = true;
            continue;
        }
        let plan = evaluate(&snapshot, &config());
        if let Plan::Execute { command } = &plan {
            if let ProtocolCommand::EnsureConfiguration(command) = command {
                if faulted {
                    assert_ne!(command.local_replica_id, ReplicaId::new(2));
                    assert!(deleted, "survivor convergence must follow exact fencing");
                } else {
                    source_admitted = true;
                }
            }
            Reconciler::new(api.clone(), config())
                .reconcile("tests", "db")
                .await
                .unwrap();
            observe_switchover_result(&api, command).await;
        } else {
            let safety_delete = matches!(&plan, Plan::Apply { changes } if changes.iter().any(|change|
                matches!(change, KubernetesChange::DeleteExactPod { pod_uid, .. } if pod_uid.as_str() == "pod-uid-2")));
            Reconciler::new(api.clone(), config())
                .reconcile("tests", "db")
                .await
                .unwrap();
            if safety_delete {
                deleted = true;
                assert_eq!(api.observation().await.pvcs, pvcs);
                assert!(
                    !api.observation()
                        .await
                        .pods
                        .iter()
                        .any(|pod| pod.uid().as_deref() == Some("pod-uid-2"))
                );
            }
        }
        let current = normalize(api.observation().await, BTreeMap::new()).unwrap();
        if current
            .routing
            .write_target
            .as_ref()
            .is_some_and(|primary| primary.replica_id == ReplicaId::new(1))
            && current
                .status
                .last_switchover
                .as_ref()
                .is_some_and(|receipt| {
                    receipt.outcome == PlannedSwitchoverOutcome::OldPrimaryCompensated
                })
        {
            completed = true;
            break;
        }
    }
    assert!(completed && deleted && faulted);
    assert_eq!(api.observation().await.pvcs, pvcs);
}

#[tokio::test]
async fn active_switchover_waits_on_stale_reports_and_revalidates_a_new_process_session() {
    let mut initial = switchover_observation();
    for agent in initial.agents.values_mut() {
        if let RawAgentObservation::Report(report) = agent {
            report.report_sequence = 10;
        }
    }
    let api = Arc::new(InMemoryClusterApi::new(initial));
    let reconciler = Reconciler::new(api.clone(), config());
    reconciler.reconcile("tests", "db").await.unwrap();
    refresh_switchover_reports(&api).await;
    reconciler.reconcile("tests", "db").await.unwrap();
    let frozen = api
        .observation()
        .await
        .set
        .status
        .unwrap()
        .authority
        .transition;
    refresh_switchover_reports(&api).await;
    let mut stale = api.observation().await;
    let RawAgentObservation::Report(source) = stale.agents.get_mut(&replica_key(POD_UID)).unwrap()
    else {
        unreachable!()
    };
    source.report_sequence = 9;
    api.set_observation(stale).await;
    let waiting = reconciler.reconcile("tests", "db").await.unwrap();
    assert_eq!(waiting.kind, ReconcileKind::Waiting);
    assert_eq!(waiting.requeue_after, Duration::from_secs(3));
    assert_eq!(
        api.observation()
            .await
            .set
            .status
            .unwrap()
            .authority
            .transition,
        frozen
    );
    refresh_switchover_reports(&api).await;
    let mut restarted = api.observation().await;
    let RawAgentObservation::Report(source) =
        restarted.agents.get_mut(&replica_key(POD_UID)).unwrap()
    else {
        unreachable!()
    };
    source.process_session_id = "fresh-recovery-session".into();
    source.report_sequence = 1;
    api.set_observation(restarted).await;
    assert_eq!(
        reconciler.reconcile("tests", "db").await.unwrap().kind,
        ReconcileKind::Executed
    );
    assert_eq!(
        api.observation()
            .await
            .set
            .status
            .unwrap()
            .authority
            .transition,
        frozen
    );
}

#[tokio::test]
async fn switchover_unsafe_receipt_requires_observed_closure_after_ambiguous_pod_deletion() {
    let api = Arc::new(InMemoryClusterApi::new(switchover_observation()));
    let reconciler = Reconciler::new(api.clone(), config());
    reconciler.reconcile("tests", "db").await.unwrap(); // accepted intent
    let mut observation = api.observation().await;
    let original_pvcs = observation.pvcs.clone();
    observation.agents.insert(
        replica_key(POD_UID),
        RawAgentObservation::Invalid {
            message: "contradictory exact authority".into(),
        },
    );
    api.set_observation(observation).await;
    refresh_switchover_reports(&api).await;
    assert_eq!(
        reconciler.reconcile("tests", "db").await.unwrap().kind,
        ReconcileKind::Applied
    );
    let before_delete = api.observation().await;
    refresh_switchover_reports(&api).await;
    assert_eq!(
        reconciler.reconcile("tests", "db").await.unwrap().kind,
        ReconcileKind::Applied
    );
    let deleted = api.observation().await;
    assert!(
        !deleted
            .pods
            .iter()
            .any(|pod| pod.uid().as_deref() == Some(POD_UID))
    );
    assert!(
        deleted
            .set
            .status
            .as_ref()
            .unwrap()
            .authority
            .last_switchover
            .is_none()
    );
    // A lost deletion response or stale observation repeats only the same exact deletion.
    api.set_observation(before_delete).await;
    refresh_switchover_reports(&api).await;
    assert_eq!(
        reconciler.reconcile("tests", "db").await.unwrap().kind,
        ReconcileKind::Applied
    );
    refresh_switchover_reports(&api).await;
    api.conflict_next_status().await;
    assert_eq!(
        reconciler.reconcile("tests", "db").await.unwrap().kind,
        ReconcileKind::ObservationStale
    );
    refresh_switchover_reports(&api).await;
    assert_eq!(
        reconciler.reconcile("tests", "db").await.unwrap().kind,
        ReconcileKind::Unsafe
    );
    let terminal = api.observation().await;
    assert_eq!(terminal.pvcs, original_pvcs);
    assert_eq!(
        terminal
            .set
            .status
            .unwrap()
            .authority
            .last_switchover
            .unwrap()
            .outcome,
        kuberic_protocol::types::PlannedSwitchoverOutcome::Unsafe
    );
}

#[tokio::test]
async fn switchover_reobserves_conflicts_ambiguous_replies_sessions_and_completed_request() {
    let api = Arc::new(InMemoryClusterApi::new(switchover_observation()));
    let reconciler = Reconciler::new(api.clone(), config());
    api.conflict_next_status().await;
    assert_eq!(
        reconciler.reconcile("tests", "db").await.unwrap().kind,
        ReconcileKind::ObservationStale
    );
    assert!(api.effects().await.is_empty());
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
    let mut first_preparation = None;
    let mut receipt_conflicted = false;
    let mut completed = false;
    let mut command_count = 0;
    for _ in 0..30 {
        refresh_switchover_reports(&api).await;
        let observation = api.observation().await;
        let snapshot = normalize(observation.clone(), BTreeMap::new()).unwrap();
        let plan = evaluate(&snapshot, &config());
        let command = if let Plan::Execute { command } = &plan {
            Some(command.clone())
        } else {
            None
        };
        if let Some(ProtocolCommand::PrepareSwitchover(_)) = &command
            && first_preparation.is_none()
        {
            assert!(snapshot.routing.write_target.is_none());
            api.unavailable_next_execute().await;
            let action = reconciler.reconcile("tests", "db").await.unwrap();
            assert_eq!(action.kind, ReconcileKind::Waiting);
            first_preparation = command;
            let mut restarted = api.observation().await;
            let RawAgentObservation::Report(source) =
                restarted.agents.get_mut(&replica_key(POD_UID)).unwrap()
            else {
                unreachable!()
            };
            source.process_session_id = "source-restarted".to_string();
            source.report_sequence = 1;
            api.set_observation(restarted).await;
            continue;
        }
        if let Some(ProtocolCommand::PrepareSwitchover(_)) = &command {
            assert_eq!(command, first_preparation);
        }
        if let Plan::Apply { changes } = &plan
            && changes.iter().any(|change| {
                matches!(change,
                kuberic_protocol::command::KubernetesChange::PersistStatus { status }
                    if status.last_switchover.is_some())
            })
            && snapshot.status.transition.is_some()
            && !receipt_conflicted
        {
            api.conflict_next_status().await;
            assert_eq!(
                reconciler.reconcile("tests", "db").await.unwrap().kind,
                ReconcileKind::ObservationStale
            );
            receipt_conflicted = true;
            continue;
        }
        if matches!(command, Some(ProtocolCommand::EnsureConfiguration(_))) {
            api.unavailable_next_execute().await;
        }
        let action = reconciler.reconcile("tests", "db").await.unwrap();
        if let Some(command) = command {
            assert!(matches!(
                action.kind,
                ReconcileKind::Executed | ReconcileKind::Waiting
            ));
            observe_switchover_result(&api, &command).await;
            command_count += 1;
        } else if matches!(plan, Plan::Stable { .. }) {
            assert_eq!(action.kind, ReconcileKind::Stable);
            completed = true;
            break;
        } else {
            assert_eq!(action.kind, ReconcileKind::Applied, "{plan:?}");
        }
        let snapshot = normalize(api.observation().await, BTreeMap::new()).unwrap();
        if let Some(target) = &snapshot.routing.write_target
            && target.replica_id == ReplicaId::new(2)
        {
            assert!(snapshot.status.transition.is_none());
            assert!(snapshot.status.last_switchover.is_some());
            assert!(
                matches!(&snapshot.observation_for_identity(target).unwrap().agent,
                AgentObservation::Report(report) if report.write_status == kuberic_protocol::types::AccessStatus::Granted)
            );
        }
    }
    assert!(completed && receipt_conflicted);
    assert_eq!(command_count, 8); // prepare, six authority commands, stable write grant
    let completed_snapshot = normalize(api.observation().await, BTreeMap::new()).unwrap();
    assert_eq!(
        completed_snapshot
            .routing
            .write_target
            .as_ref()
            .unwrap()
            .replica_id,
        ReplicaId::new(2)
    );
    let effects = api.effects().await;
    let prepare = effects
        .iter()
        .position(|effect| {
            matches!(
                effect,
                EffectRecord::Execute(ProtocolCommand::PrepareSwitchover(_))
            )
        })
        .unwrap();
    let remove = effects
        .iter()
        .position(|effect| matches!(effect, EffectRecord::RemoveWriteRouting))
        .unwrap();
    assert!(remove < prepare);
    let before = effects
        .iter()
        .filter(|effect| matches!(effect, EffectRecord::Execute(_)))
        .count();
    for _ in 0..3 {
        refresh_switchover_reports(&api).await;
        assert_eq!(
            reconciler.reconcile("tests", "db").await.unwrap().kind,
            ReconcileKind::Stable
        );
    }
    assert_eq!(
        api.effects()
            .await
            .iter()
            .filter(|effect| matches!(effect, EffectRecord::Execute(_)))
            .count(),
        before
    );
}

#[test]
fn normalization_distinguishes_missing_and_unreachable_agents() {
    let missing = normalize(raw(1), BTreeMap::new()).unwrap();
    assert!(matches!(
        missing.replicas.values().next().unwrap().agent,
        AgentObservation::Absent
    ));

    let mut unreachable = with_scaffolding(raw(1));
    unreachable.agents.insert(
        replica_key(POD_UID),
        RawAgentObservation::Unavailable {
            message: "reconstructing".to_string(),
        },
    );
    let unreachable = normalize(unreachable, BTreeMap::new()).unwrap();
    assert!(matches!(
        unreachable.replicas.values().next().unwrap().agent,
        AgentObservation::Unreachable { .. }
    ));
}

#[tokio::test]
async fn status_conflict_reobserves_without_dispatching_authority() {
    let api = Arc::new(InMemoryClusterApi::new(fresh_bootstrap_observation()));
    api.conflict_next_status().await;
    let reconciler = Reconciler::new(api.clone(), config());
    let action = reconciler.reconcile("tests", "db").await.unwrap();
    assert_eq!(action.kind, ReconcileKind::ObservationStale);
    assert_eq!(action.requeue_after, Duration::ZERO);
    assert!(api.effects().await.is_empty());
}

#[tokio::test]
async fn overlapping_reconciles_are_serialized_per_resource() {
    let api = Arc::new(InMemoryClusterApi::new(raw(1)));
    api.set_observation_delay(Duration::from_millis(30)).await;
    let reconciler = Arc::new(Reconciler::new(api.clone(), config()));
    let first = {
        let reconciler = reconciler.clone();
        tokio::spawn(async move { reconciler.reconcile("tests", "db").await })
    };
    let second = {
        let reconciler = reconciler.clone();
        tokio::spawn(async move { reconciler.reconcile("tests", "db").await })
    };
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    assert_eq!(api.max_active_observations().await, 1);
}

#[tokio::test]
async fn each_execute_requires_a_fresh_full_observation() {
    let mut observation = fresh_bootstrap_observation();
    observation.set.status = Some(KubericSetStatus {
        authority: bootstrap_status(&observation),
    });
    let api = Arc::new(InMemoryClusterApi::new(observation));
    let reconciler = Reconciler::new(api.clone(), config());

    let status_update = reconciler.reconcile("tests", "db").await.unwrap();
    assert_eq!(status_update.kind, ReconcileKind::Applied);
    let mut refreshed = api.observation().await;
    let RawAgentObservation::Report(report) =
        refreshed.agents.get_mut(&replica_key(POD_UID)).unwrap()
    else {
        panic!("uninitialized report");
    };
    report.report_sequence += 1;
    api.set_observation(refreshed).await;
    let first = reconciler.reconcile("tests", "db").await.unwrap();
    assert_eq!(first.kind, ReconcileKind::Executed);
    assert_eq!(first.requeue_after, Duration::ZERO);
    assert_eq!(api.observation_count().await, 2);
    assert_eq!(
        api.effects()
            .await
            .iter()
            .filter(|effect| matches!(effect, EffectRecord::Execute(_)))
            .count(),
        1
    );

    let mut refreshed = api.observation().await;
    let RawAgentObservation::Report(report) =
        refreshed.agents.get_mut(&replica_key(POD_UID)).unwrap()
    else {
        panic!("uninitialized report");
    };
    report.report_sequence += 1;
    api.set_observation(refreshed).await;
    reconciler.reconcile("tests", "db").await.unwrap();
    assert_eq!(api.observation_count().await, 3);
    assert_eq!(
        api.effects()
            .await
            .iter()
            .filter(|effect| matches!(effect, EffectRecord::Execute(_)))
            .count(),
        2
    );
}

#[tokio::test]
async fn ambiguous_execute_is_reobserved_and_replayed_with_identical_authority() {
    let mut observation = fresh_bootstrap_observation();
    observation.set.status = Some(KubericSetStatus {
        authority: bootstrap_status(&observation),
    });
    let api = Arc::new(InMemoryClusterApi::new(observation));
    let reconciler = Reconciler::new(api.clone(), config());

    assert_eq!(
        reconciler.reconcile("tests", "db").await.unwrap().kind,
        ReconcileKind::Applied
    );
    let mut refreshed = api.observation().await;
    let RawAgentObservation::Report(report) =
        refreshed.agents.get_mut(&replica_key(POD_UID)).unwrap()
    else {
        panic!("uninitialized report");
    };
    report.report_sequence += 1;
    api.set_observation(refreshed).await;
    api.unavailable_next_execute().await;
    let ambiguous = reconciler.reconcile("tests", "db").await.unwrap();
    assert_eq!(ambiguous.kind, ReconcileKind::Waiting);
    assert_eq!(ambiguous.requeue_after, Duration::from_secs(3));

    let mut refreshed = api.observation().await;
    let RawAgentObservation::Report(report) =
        refreshed.agents.get_mut(&replica_key(POD_UID)).unwrap()
    else {
        panic!("uninitialized report");
    };
    report.report_sequence += 1;
    api.set_observation(refreshed).await;
    assert_eq!(
        reconciler.reconcile("tests", "db").await.unwrap().kind,
        ReconcileKind::Executed
    );

    let executions = api
        .effects()
        .await
        .into_iter()
        .filter_map(|effect| match effect {
            EffectRecord::Execute(command) => Some(command),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(executions.len(), 2);
    assert_eq!(executions[0], executions[1]);
}

#[tokio::test]
async fn startup_unavailable_is_a_bounded_wait() {
    let mut observation = with_scaffolding(raw(1));
    observation.set.status = Some(KubericSetStatus {
        authority: bootstrap_status(&observation),
    });
    observation.agents.insert(
        replica_key(POD_UID),
        RawAgentObservation::Unavailable {
            message: "runtime reconstruction in progress".to_string(),
        },
    );
    let api = Arc::new(InMemoryClusterApi::new(observation));
    let reconciler = Reconciler::new(api, config());
    let status_update = reconciler.reconcile("tests", "db").await.unwrap();
    assert_eq!(status_update.kind, ReconcileKind::Applied);
    let action = reconciler.reconcile("tests", "db").await.unwrap();
    assert_eq!(action.kind, ReconcileKind::Waiting);
    assert_eq!(action.requeue_after, Duration::from_secs(3));
}

#[tokio::test]
async fn stable_and_unsafe_states_use_bounded_reobservation() {
    let stable = stable_observation();
    assert_eq!(
        stable
            .set
            .status
            .as_ref()
            .unwrap()
            .authority
            .topology
            .as_ref()
            .unwrap()
            .configuration
            .members[0]
            .role,
        ReplicaRole::Primary
    );
    let stable_api = Arc::new(InMemoryClusterApi::new(stable));
    let stable_action = Reconciler::new(stable_api, config())
        .reconcile("tests", "db")
        .await
        .unwrap();
    assert_eq!(stable_action.kind, ReconcileKind::Stable);
    assert_eq!(stable_action.requeue_after, Duration::from_secs(11));

    let unsafe_api = Arc::new(InMemoryClusterApi::new(raw(0)));
    let unsafe_reconciler = Reconciler::new(unsafe_api.clone(), config());
    let unsafe_action = unsafe_reconciler.reconcile("tests", "db").await.unwrap();
    assert_eq!(unsafe_action.kind, ReconcileKind::Unsafe);
    assert_eq!(unsafe_action.requeue_after, Duration::from_secs(7));

    let recovered = raw(1);
    unsafe_api.set_observation(recovered).await;
    let recovered_action = unsafe_reconciler.reconcile("tests", "db").await.unwrap();
    assert_eq!(recovered_action.kind, ReconcileKind::Applied);
    assert_eq!(recovered_action.requeue_after, Duration::ZERO);
}

#[test]
fn exact_pod_uid_is_preserved_as_the_replica_instance() {
    let snapshot = normalize(with_scaffolding(raw(1)), BTreeMap::new()).unwrap();
    let observation = snapshot.replicas.values().next().unwrap();
    let kubernetes = observation.kubernetes.as_ref().unwrap();
    assert_eq!(kubernetes.pod_uid.as_ref().unwrap().as_str(), POD_UID);
    assert_eq!(kubernetes.pvc_uid.as_ref().unwrap().as_str(), PVC_UID);
    assert_eq!(kubernetes.pod_name, "db-1");
    assert_eq!(kubernetes.pvc_name, "db-1-data");
    assert!(kubernetes.pod_ready);
    assert_eq!(snapshot.resource_uid.as_str(), UID);
    assert_eq!(snapshot.resource_version, "1");
    assert_eq!(snapshot.intended_replica_ids(), vec![ReplicaId::new(1)]);
    assert_eq!(snapshot.replicas.len(), 1);
    let key = snapshot.replicas.keys().next().unwrap();
    assert_eq!(key.instance_id.as_str(), POD_UID);
    assert_eq!(key.replica_id, ReplicaId::new(1));
    assert!(!snapshot.durable_storage_evidence);
    assert!(snapshot.observation_failures.is_empty());
    assert!(snapshot.routing.write_target.is_none());
    assert!(snapshot.has_complete_scaffolding());
    assert_eq!(observation.agent, AgentObservation::Absent);
}

#[tokio::test]
async fn bootstrap_waits_for_explicit_uninitialized_storage_evidence() {
    let mut observation = with_scaffolding(raw(1));
    observation.services.push(write_service("disabled"));
    let api = Arc::new(InMemoryClusterApi::new(observation));
    let action = Reconciler::new(api.clone(), config())
        .reconcile("tests", "db")
        .await
        .unwrap();
    assert_eq!(action.kind, ReconcileKind::Waiting);
    assert_eq!(action.requeue_after, Duration::from_secs(3));
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
}

#[tokio::test]
async fn stale_report_watermark_survives_repeated_rejection() {
    let mut observation = fresh_bootstrap_observation();
    observation.set.status = Some(KubericSetStatus {
        authority: bootstrap_status(&observation),
    });
    let RawAgentObservation::Report(report) =
        observation.agents.get_mut(&replica_key(POD_UID)).unwrap()
    else {
        panic!("uninitialized report");
    };
    report.report_sequence = 5;
    let api = Arc::new(InMemoryClusterApi::new(observation));
    api.conflict_next_status().await;
    let reconciler = Reconciler::new(api.clone(), config());
    assert_eq!(
        reconciler.reconcile("tests", "db").await.unwrap().kind,
        ReconcileKind::ObservationStale
    );

    let mut stale = api.observation().await;
    let RawAgentObservation::Report(report) = stale.agents.get_mut(&replica_key(POD_UID)).unwrap()
    else {
        panic!("uninitialized report");
    };
    report.report_sequence = 4;
    api.set_observation(stale).await;
    assert_eq!(
        reconciler.reconcile("tests", "db").await.unwrap().kind,
        ReconcileKind::Unsafe
    );
    assert_eq!(
        reconciler.reconcile("tests", "db").await.unwrap().kind,
        ReconcileKind::Unsafe
    );
    assert!(
        api.effects()
            .await
            .iter()
            .all(|effect| !matches!(effect, EffectRecord::Execute(_)))
    );
}

#[tokio::test]
async fn new_process_session_accepts_a_fresh_sequence_without_losing_authority() {
    let observation = stable_observation();
    let api = Arc::new(InMemoryClusterApi::new(observation));
    let reconciler = Reconciler::new(api.clone(), config());
    assert_eq!(
        reconciler.reconcile("tests", "db").await.unwrap().kind,
        ReconcileKind::Stable
    );

    let mut restarted = api.observation().await;
    let RawAgentObservation::Report(report) =
        restarted.agents.get_mut(&replica_key(POD_UID)).unwrap()
    else {
        panic!("initialized report");
    };
    report.process_session_id = "restarted-session".to_string();
    report.report_sequence = 1;
    api.set_observation(restarted).await;

    assert_eq!(
        reconciler.reconcile("tests", "db").await.unwrap().kind,
        ReconcileKind::Stable
    );
    assert!(
        api.effects()
            .await
            .iter()
            .all(|effect| !matches!(effect, EffectRecord::RemoveWriteRouting))
    );
}

#[tokio::test]
async fn bounded_resync_heals_changes_without_a_watch_event() {
    let api = Arc::new(InMemoryClusterApi::new(stable_observation()));
    let reconciler = Reconciler::new(api.clone(), config());
    let stable = reconciler.reconcile("tests", "db").await.unwrap();
    assert_eq!(stable.kind, ReconcileKind::Stable);
    assert_eq!(stable.requeue_after, Duration::from_secs(11));

    let mut drifted = api.observation().await;
    drifted
        .services
        .retain(|service| service.name_any() != "db-peer");
    for observation in drifted.agents.values_mut() {
        if let RawAgentObservation::Report(report) = observation {
            report.report_sequence += 1;
        }
    }
    api.set_observation(drifted).await;
    assert_eq!(
        reconciler.reconcile("tests", "db").await.unwrap().kind,
        ReconcileKind::Applied
    );
    assert!(
        api.effects()
            .await
            .contains(&EffectRecord::EnsureReplicaSupport)
    );
}

#[test]
fn normalization_preserves_simultaneous_replica_incarnations() {
    let mut observation = with_scaffolding(raw(1));
    let second_uid = "pod-uid-2";
    let second_pvc_uid = "pvc-uid-2";
    observation.pods.push(Pod {
        metadata: kube::core::ObjectMeta {
            name: Some("db-1-replacement".to_string()),
            uid: Some(second_uid.to_string()),
            labels: Some(labels(ReplicaId::new(1))),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: Vec::new(),
            volumes: Some(vec![Volume {
                name: "data".to_string(),
                persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                    claim_name: "db-1-replacement-data".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    });
    observation.pvcs.push(PersistentVolumeClaim {
        metadata: kube::core::ObjectMeta {
            name: Some("db-1-replacement-data".to_string()),
            uid: Some(second_pvc_uid.to_string()),
            labels: Some(labels(ReplicaId::new(1))),
            ..Default::default()
        },
        ..Default::default()
    });
    observation.agents.insert(
        replica_key(POD_UID),
        RawAgentObservation::Unavailable {
            message: "old".to_string(),
        },
    );
    observation.agents.insert(
        replica_key(second_uid),
        RawAgentObservation::Unavailable {
            message: "replacement".to_string(),
        },
    );

    let snapshot = normalize(observation, BTreeMap::new()).unwrap();
    let keys = snapshot
        .replicas
        .keys()
        .filter(|key| key.replica_id == ReplicaId::new(1))
        .map(|key| key.instance_id.to_string())
        .collect::<Vec<_>>();
    assert_eq!(keys, vec![POD_UID.to_string(), second_uid.to_string()]);
    assert!(snapshot.observation_failures.is_empty());
}

#[tokio::test]
async fn unresolved_routing_is_fenced_before_ready() {
    let mut observation = stable_observation();
    observation
        .pods
        .first_mut()
        .unwrap()
        .metadata
        .labels
        .as_mut()
        .unwrap()
        .remove(INSTANCE_LABEL);
    let api = Arc::new(InMemoryClusterApi::new(observation));
    let action = Reconciler::new(api.clone(), config())
        .reconcile("tests", "db")
        .await
        .unwrap();
    assert_eq!(action.kind, ReconcileKind::Applied);
    assert!(
        api.effects()
            .await
            .contains(&EffectRecord::RemoveWriteRouting)
    );
}

#[tokio::test]
async fn primary_failure_fences_routing_before_persisting_failover_timing() {
    let mut observation = stable_observation();
    observation.pods[0].status.as_mut().unwrap().conditions = Some(vec![PodCondition {
        last_probe_time: None,
        last_transition_time: None,
        message: None,
        reason: None,
        status: "False".to_string(),
        type_: "Ready".to_string(),
    }]);
    observation.agents.insert(
        replica_key(POD_UID),
        RawAgentObservation::Unavailable {
            message: "primary unavailable".to_string(),
        },
    );
    let api = Arc::new(InMemoryClusterApi::new(observation));
    let action = Reconciler::new(api.clone(), config())
        .reconcile("tests", "db")
        .await
        .unwrap();
    assert_eq!(action.kind, ReconcileKind::Applied);
    assert_eq!(
        api.effects().await.as_slice(),
        [
            EffectRecord::RemoveWriteRouting,
            EffectRecord::ReplaceStatus
        ]
    );
    assert!(
        api.observation()
            .await
            .set
            .status
            .unwrap()
            .authority
            .primary_failure
            .is_some()
    );
}

#[tokio::test]
async fn missing_write_service_is_recreated() {
    let mut observation = stable_observation();
    observation
        .services
        .retain(|service| !service.name_any().ends_with("-write"));
    let api = Arc::new(InMemoryClusterApi::new(observation));
    let action = Reconciler::new(api.clone(), config())
        .reconcile("tests", "db")
        .await
        .unwrap();
    assert_eq!(action.kind, ReconcileKind::Applied);
    assert!(
        api.effects()
            .await
            .contains(&EffectRecord::EnsureWriteRoutingService)
    );
    let services = api.observation().await.services;
    assert_eq!(services.len(), 3);
    assert!(
        services
            .iter()
            .any(|service| service.name_any() == "db-write")
    );
}

#[tokio::test]
async fn stable_state_recreates_missing_replica_support() {
    let mut missing_peer = stable_observation();
    missing_peer
        .services
        .retain(|service| service.name_any() != "db-peer");
    let peer_api = Arc::new(InMemoryClusterApi::new(missing_peer));
    let action = Reconciler::new(peer_api.clone(), config())
        .reconcile("tests", "db")
        .await
        .unwrap();
    assert_eq!(action.kind, ReconcileKind::Applied);
    assert!(matches!(
        peer_api.effects().await.as_slice(),
        [EffectRecord::EnsureReplicaSupport]
    ));

    let mut missing_secret = stable_observation();
    missing_secret.secrets.clear();
    let secret_api = Arc::new(InMemoryClusterApi::new(missing_secret));
    let action = Reconciler::new(secret_api.clone(), config())
        .reconcile("tests", "db")
        .await
        .unwrap();
    assert_eq!(action.kind, ReconcileKind::Applied);
    assert!(matches!(
        secret_api.effects().await.as_slice(),
        [EffectRecord::EnsureReplicaSupport]
    ));
}

#[tokio::test]
async fn failed_service_observation_cannot_claim_routing_removal() {
    let mut observation = raw(0);
    observation.failures.push(RawObservationFailure {
        source: "services".to_string(),
        message: "forbidden".to_string(),
    });
    let api = Arc::new(InMemoryClusterApi::new(observation));
    let error = Reconciler::new(api.clone(), config())
        .reconcile("tests", "db")
        .await
        .unwrap_err();
    assert!(matches!(error, ControllerError::Observation(_)));
    assert!(api.effects().await.is_empty());
}
