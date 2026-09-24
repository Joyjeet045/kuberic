use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::{
    PersistentVolumeClaim, PersistentVolumeClaimVolumeSource, Pod, PodCondition, PodSpec,
    PodStatus, Secret, Service, ServicePort, ServiceSpec, Volume,
};
use kube::ResourceExt;
use kuberic_controller::ControllerError;
use kuberic_controller::cluster_api::{EffectRecord, InMemoryClusterApi};
use kuberic_controller::crd::{
    INSTANCE_LABEL, KubericSet, KubericSetSpec, KubericSetStatus, REPLICA_ID_LABEL, SET_UID_LABEL,
};
use kuberic_controller::normalize::normalize;
use kuberic_controller::observation::{RawAgentObservation, RawObservation, RawObservationFailure};
use kuberic_controller::reconciler::{ReconcileKind, Reconciler};
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
