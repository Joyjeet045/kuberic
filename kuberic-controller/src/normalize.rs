use std::collections::{BTreeMap, BTreeSet};

use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod, Service};
use kube::ResourceExt;
use kuberic_protocol::observation::{
    AgentObservation, DesiredState, KubernetesReplicaObservation, ObservationFailure,
    ObservationSnapshot, ReplicaObservation, ReplicaObservationKey, ReportWatermark,
    RoutingObservation,
};
use kuberic_protocol::types::{
    AcceptedStatus, PodUid, PvcUid, ReplicaId, ReplicaIdentity, ReplicaInstanceId, ResourceUid,
    derive_agent_generation, derive_initialization_id, derive_replica_endpoint_name,
};

use crate::crd::{INSTANCE_LABEL, REPLICA_ID_LABEL, SET_UID_LABEL};
use crate::observation::{RawAgentObservation, RawObservation};
use crate::{ControllerError, Result};

pub fn normalize(
    raw: RawObservation,
    previous_report_watermarks: BTreeMap<ReplicaObservationKey, ReportWatermark>,
) -> Result<ObservationSnapshot> {
    let resource_uid = raw
        .set
        .uid()
        .filter(|uid| !uid.is_empty())
        .map(ResourceUid::new)
        .ok_or_else(|| ControllerError::Observation("KubericSet has no UID".to_string()))?;
    let resource_version = raw
        .set
        .resource_version()
        .filter(|version| !version.is_empty())
        .ok_or_else(|| {
            ControllerError::Observation("KubericSet has no resourceVersion".to_string())
        })?;
    let generation = raw.set.metadata.generation.unwrap_or_default();
    if generation < 0 {
        return Err(ControllerError::Observation(
            "KubericSet generation is negative".to_string(),
        ));
    }

    let status = raw
        .set
        .status
        .as_ref()
        .map(|status| status.authority.clone())
        .unwrap_or_default();
    let mut failures = raw
        .failures
        .iter()
        .cloned()
        .map(|failure| ObservationFailure {
            source: failure.source,
            message: failure.message,
        })
        .collect::<Vec<_>>();
    let mut replica_ids = authority_replica_ids(&status);
    replica_ids.extend((1..=raw.set.spec.replicas).map(|value| ReplicaId::new(i64::from(value))));
    collect_labeled_replica_ids(&raw.pods, &mut replica_ids, &mut failures, "Pod");
    collect_labeled_replica_ids(&raw.pvcs, &mut replica_ids, &mut failures, "PVC");

    let mut replicas = BTreeMap::new();
    for replica_id in replica_ids {
        let pods = matching_objects(&raw.pods, &resource_uid, replica_id);
        let pvcs = matching_objects(&raw.pvcs, &resource_uid, replica_id);
        let mut used_pvcs = BTreeSet::new();
        for pod in pods {
            let pod_uid = pod.uid().map(PodUid::new);
            let instance_id = pod_uid
                .as_ref()
                .map(|uid| ReplicaInstanceId::new(uid.as_str()))
                .unwrap_or_else(|| {
                    ReplicaInstanceId::new(format!("missing-pod-uid-{}", pod.name_any()))
                });
            let key = ReplicaObservationKey::new(replica_id, instance_id);
            let pvc = pvc_for_pod(pod, &pvcs);
            if let Some(pvc) = pvc {
                used_pvcs.insert(pvc.name_any());
            }
            let raw_agent = raw
                .agents
                .get(&key)
                .cloned()
                .unwrap_or(RawAgentObservation::Absent);
            let pvc_uid = pvc.and_then(|pvc| pvc.uid()).map(PvcUid::new);
            let peer_endpoint_ready =
                pod_uid
                    .as_ref()
                    .zip(pvc_uid.as_ref())
                    .is_some_and(|(pod_uid, pvc_uid)| {
                        exact_peer_endpoint_ready(&raw, &resource_uid, replica_id, pod_uid, pvc_uid)
                    });
            insert_replica_observation(
                &mut replicas,
                &mut failures,
                key,
                pod_uid,
                pvc_uid,
                Some(pod),
                pvc,
                raw_agent,
                peer_endpoint_ready,
                &resource_uid,
                &previous_report_watermarks,
            );
        }
        for pvc in pvcs
            .into_iter()
            .filter(|pvc| !used_pvcs.contains(&pvc.name_any()))
        {
            let pvc_uid = pvc.uid().map(PvcUid::new);
            let pvc_identity = pvc_uid
                .as_ref()
                .map(|uid| uid.as_str().to_string())
                .unwrap_or_else(|| pvc.name_any());
            let instance_id = ReplicaInstanceId::new(format!("orphan-pvc-{pvc_identity}"));
            insert_replica_observation(
                &mut replicas,
                &mut failures,
                ReplicaObservationKey::new(replica_id, instance_id),
                None,
                pvc_uid,
                None,
                Some(pvc),
                RawAgentObservation::Absent,
                false,
                &resource_uid,
                &previous_report_watermarks,
            );
        }
        if !replicas.keys().any(|key| key.replica_id == replica_id) {
            replicas.insert(
                ReplicaObservationKey::new(
                    replica_id,
                    ReplicaInstanceId::new(format!("missing-{replica_id}")),
                ),
                ReplicaObservation {
                    kubernetes: None,
                    agent: AgentObservation::Absent,
                },
            );
        }
    }

    let routing = normalize_routing(&raw, &replicas, &resource_uid, &mut failures);
    let supporting_resources_ready = supporting_resources_ready(&raw, &resource_uid);
    Ok(ObservationSnapshot {
        resource_uid,
        resource_version,
        desired: DesiredState {
            generation: generation as u64,
            replicas: raw.set.spec.replicas,
            image: raw.set.spec.image,
            failover_delay_seconds: raw.set.spec.failover_delay_seconds,
        },
        status,
        replicas,
        previous_report_watermarks,
        durable_storage_evidence: false,
        supporting_resources_ready,
        routing,
        observation_failures: failures,
        now_unix_seconds: raw.now_unix_seconds,
    })
}

fn supporting_resources_ready(raw: &RawObservation, resource_uid: &ResourceUid) -> bool {
    let set_name = raw.set.name_any();
    let peer_name = format!("{set_name}-peer");
    let credential_name = format!("{set_name}-agent-credentials");
    raw.services.iter().any(|service| {
        service.name_any() == peer_name
            && owned_by_set(service, resource_uid)
            && service.spec.as_ref().is_some_and(|spec| {
                spec.cluster_ip.as_deref() == Some("None")
                    && spec.publish_not_ready_addresses == Some(true)
                    && spec.selector.as_ref().is_some_and(|selector| {
                        selector.get(SET_UID_LABEL).map(String::as_str)
                            == Some(resource_uid.as_str())
                    })
                    && has_service_port(service, "control", 50051)
                    && has_service_port(service, "replication", 50052)
            })
    }) && raw.secrets.iter().any(|secret| {
        secret.name_any() == credential_name
            && owned_by_set(secret, resource_uid)
            && secret.data.as_ref().is_some_and(|data| {
                data.get("bearer-token")
                    .is_some_and(|value| !value.0.is_empty())
            })
    })
}

fn owned_by_set<K: ResourceExt>(resource: &K, resource_uid: &ResourceUid) -> bool {
    resource.labels().get(SET_UID_LABEL).map(String::as_str) == Some(resource_uid.as_str())
}

fn has_service_port(service: &Service, name: &str, port: i32) -> bool {
    service
        .spec
        .as_ref()
        .and_then(|spec| spec.ports.as_ref())
        .is_some_and(|ports| {
            ports
                .iter()
                .any(|candidate| candidate.name.as_deref() == Some(name) && candidate.port == port)
        })
}

pub fn report_watermarks(
    snapshot: &ObservationSnapshot,
) -> BTreeMap<ReplicaObservationKey, ReportWatermark> {
    snapshot
        .replicas
        .iter()
        .filter_map(|(key, observation)| match &observation.agent {
            AgentObservation::Uninitialized(report) => Some((
                key.clone(),
                ReportWatermark {
                    process_session_id: report.process_session_id.clone(),
                    report_sequence: report.report_sequence,
                },
            )),
            AgentObservation::Report(report) => Some((
                key.clone(),
                ReportWatermark {
                    process_session_id: report.process_session_id.clone(),
                    report_sequence: report.report_sequence,
                },
            )),
            AgentObservation::Absent
            | AgentObservation::Unreachable { .. }
            | AgentObservation::Invalid { .. } => None,
        })
        .collect()
}

fn authority_replica_ids(status: &AcceptedStatus) -> BTreeSet<ReplicaId> {
    status
        .topology
        .iter()
        .flat_map(|topology| &topology.configuration.members)
        .chain(
            status
                .transition
                .iter()
                .flat_map(|transition| &transition.current_configuration.members),
        )
        .map(|member| member.identity.replica_id)
        .chain(status.provisioning.iter().map(|intent| intent.replica_id()))
        .collect()
}

fn collect_labeled_replica_ids<K>(
    objects: &[K],
    replica_ids: &mut BTreeSet<ReplicaId>,
    failures: &mut Vec<ObservationFailure>,
    kind: &str,
) where
    K: ResourceExt,
{
    for object in objects {
        let Some(value) = object.labels().get(REPLICA_ID_LABEL) else {
            continue;
        };
        match value.parse::<i64>() {
            Ok(value) if value > 0 => {
                replica_ids.insert(ReplicaId::new(value));
            }
            _ => failures.push(ObservationFailure {
                source: format!("{kind}/{}", object.name_any()),
                message: "replica-id label is not a positive integer".to_string(),
            }),
        }
    }
}

fn matching_objects<'a, K>(
    objects: &'a [K],
    resource_uid: &ResourceUid,
    replica_id: ReplicaId,
) -> Vec<&'a K>
where
    K: ResourceExt,
{
    objects
        .iter()
        .filter(|object| {
            object.labels().get(SET_UID_LABEL).map(String::as_str) == Some(resource_uid.as_str())
                && object
                    .labels()
                    .get(REPLICA_ID_LABEL)
                    .is_some_and(|value| value == &replica_id.to_string())
        })
        .collect()
}

fn normalize_agent(
    raw: RawAgentObservation,
    resource_uid: &ResourceUid,
    replica_id: ReplicaId,
    pod_uid: Option<&PodUid>,
    pvc_uid: Option<&PvcUid>,
    previous: &BTreeMap<ReplicaObservationKey, ReportWatermark>,
) -> AgentObservation {
    let observation = match raw {
        RawAgentObservation::Absent => AgentObservation::Absent,
        RawAgentObservation::Unavailable { message } => AgentObservation::Unreachable { message },
        RawAgentObservation::Invalid { message } => AgentObservation::Invalid { message },
        RawAgentObservation::Report(report) => {
            match kuberic_wire::normalize_agent_status_report(*report) {
                Ok(observation) => observation,
                Err(error) => {
                    return AgentObservation::Invalid {
                        message: error.to_string(),
                    };
                }
            }
        }
    };
    let mismatch = match &observation {
        AgentObservation::Uninitialized(report) => (report.resource_uid != *resource_uid)
            .then_some("uninitialized report resource UID differs from the KubericSet")
            .or_else(|| {
                (report.replica_id != replica_id)
                    .then_some("uninitialized report replica ID differs from its Pod")
            })
            .or_else(|| {
                (pod_uid != Some(&report.pod_uid))
                    .then_some("uninitialized report Pod UID differs from Kubernetes")
            })
            .or_else(|| {
                (pvc_uid != Some(&report.pvc_uid))
                    .then_some("uninitialized report PVC UID differs from Kubernetes")
            }),
        AgentObservation::Report(report) => (report.resource_uid != *resource_uid)
            .then_some("agent report resource UID differs from the KubericSet")
            .or_else(|| {
                (report.identity.replica_id != replica_id)
                    .then_some("agent report replica ID differs from its Pod")
            })
            .or_else(|| {
                pod_uid
                    .is_some_and(|uid| uid.as_str() != report.identity.instance_id.as_str())
                    .then_some("agent identity differs from the current Pod UID")
            }),
        AgentObservation::Absent
        | AgentObservation::Unreachable { .. }
        | AgentObservation::Invalid { .. } => None,
    };
    if let Some(message) = mismatch {
        return AgentObservation::Invalid {
            message: message.to_string(),
        };
    }

    let watermark = match &observation {
        AgentObservation::Uninitialized(report) => Some((
            ReplicaObservationKey::new(
                report.replica_id,
                ReplicaInstanceId::new(report.pod_uid.as_str()),
            ),
            &report.process_session_id,
            report.report_sequence,
        )),
        AgentObservation::Report(report) => Some((
            ReplicaObservationKey::new(
                report.identity.replica_id,
                report.identity.instance_id.clone(),
            ),
            &report.process_session_id,
            report.report_sequence,
        )),
        AgentObservation::Absent
        | AgentObservation::Unreachable { .. }
        | AgentObservation::Invalid { .. } => None,
    };
    if let Some((key, session, sequence)) = watermark
        && previous.get(&key).is_some_and(|previous| {
            previous.process_session_id == *session && sequence < previous.report_sequence
        })
    {
        return AgentObservation::Invalid {
            message: "agent report sequence regressed within one process session".to_string(),
        };
    }
    observation
}

#[allow(clippy::too_many_arguments)]
fn insert_replica_observation(
    replicas: &mut BTreeMap<ReplicaObservationKey, ReplicaObservation>,
    failures: &mut Vec<ObservationFailure>,
    key: ReplicaObservationKey,
    pod_uid: Option<PodUid>,
    pvc_uid: Option<PvcUid>,
    pod: Option<&Pod>,
    pvc: Option<&PersistentVolumeClaim>,
    raw_agent: RawAgentObservation,
    peer_endpoint_ready: bool,
    resource_uid: &ResourceUid,
    previous: &BTreeMap<ReplicaObservationKey, ReportWatermark>,
) {
    let agent = normalize_agent(
        raw_agent,
        resource_uid,
        key.replica_id,
        pod_uid.as_ref(),
        pvc_uid.as_ref(),
        previous,
    );
    let observation = ReplicaObservation {
        kubernetes: (pod.is_some() || pvc.is_some()).then(|| KubernetesReplicaObservation {
            replica_id: key.replica_id,
            pod_name: pod.map(ResourceExt::name_any).unwrap_or_default(),
            pod_uid,
            pvc_name: pvc.map(ResourceExt::name_any).unwrap_or_default(),
            pvc_uid,
            image: pod.and_then(application_image),
            pod_ready: pod.is_some_and(pod_ready),
            peer_endpoint_ready,
        }),
        agent,
    };
    if replicas.insert(key.clone(), observation).is_some() {
        failures.push(ObservationFailure {
            source: format!("replica/{}@{}", key.replica_id, key.instance_id),
            message: "multiple observations claim one exact replica incarnation".to_string(),
        });
    }
}

fn application_image(pod: &Pod) -> Option<String> {
    pod.spec
        .as_ref()?
        .containers
        .iter()
        .find(|container| container.name == "application")?
        .image
        .clone()
}

fn exact_peer_endpoint_ready(
    raw: &RawObservation,
    resource_uid: &ResourceUid,
    replica_id: ReplicaId,
    pod_uid: &PodUid,
    pvc_uid: &PvcUid,
) -> bool {
    let initialization_id = derive_initialization_id(resource_uid, replica_id, pod_uid, pvc_uid);
    let identity = ReplicaIdentity {
        replica_id,
        instance_id: ReplicaInstanceId::new(pod_uid.as_str()),
        agent_generation: derive_agent_generation(&initialization_id),
    };
    let name = derive_replica_endpoint_name(resource_uid, &identity);
    raw.services.iter().any(|service| {
        service.name_any() == name
            && owned_by_set(service, resource_uid)
            && service.spec.as_ref().is_some_and(|spec| {
                spec.selector.as_ref().is_some_and(|selector| {
                    selector.get(INSTANCE_LABEL).map(String::as_str) == Some(pod_uid.as_str())
                }) && has_service_port(service, "control", 50051)
                    && has_service_port(service, "replication", 50052)
            })
    })
}

fn pvc_for_pod<'a>(
    pod: &Pod,
    pvcs: &[&'a PersistentVolumeClaim],
) -> Option<&'a PersistentVolumeClaim> {
    let claimed = pod
        .spec
        .as_ref()
        .and_then(|spec| spec.volumes.as_ref())
        .and_then(|volumes| {
            volumes.iter().find_map(|volume| {
                volume
                    .persistent_volume_claim
                    .as_ref()
                    .map(|claim| claim.claim_name.as_str())
            })
        });
    let fallback = format!("{}-data", pod.name_any());
    let expected = claimed.unwrap_or(&fallback);
    pvcs.iter().copied().find(|pvc| pvc.name_any() == expected)
}

fn pod_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .is_some_and(|conditions| {
            conditions
                .iter()
                .any(|condition| condition.type_ == "Ready" && condition.status == "True")
        })
}

fn normalize_routing(
    raw: &RawObservation,
    replicas: &BTreeMap<ReplicaObservationKey, ReplicaObservation>,
    resource_uid: &ResourceUid,
    failures: &mut Vec<ObservationFailure>,
) -> RoutingObservation {
    let write_services = raw
        .services
        .iter()
        .filter(|service| {
            service.labels().get(SET_UID_LABEL).map(String::as_str) == Some(resource_uid.as_str())
                && service.name_any().ends_with("-write")
        })
        .collect::<Vec<_>>();
    if write_services.len() > 1 {
        failures.push(ObservationFailure {
            source: "routing".to_string(),
            message: "multiple write Services claim the KubericSet".to_string(),
        });
        return RoutingObservation {
            service_present: true,
            unresolved_write_target: true,
            write_target: None,
        };
    }
    let Some(_service) = write_services.first() else {
        return RoutingObservation::default();
    };
    let Some(instance) = write_services
        .first()
        .and_then(|service| service.spec.as_ref())
        .and_then(|spec| spec.selector.as_ref())
        .and_then(|selector| selector.get(INSTANCE_LABEL))
        .filter(|instance| instance.as_str() != "disabled")
    else {
        return RoutingObservation {
            service_present: true,
            unresolved_write_target: false,
            write_target: None,
        };
    };
    let matches = replicas
        .values()
        .filter_map(|observation| match &observation.agent {
            AgentObservation::Report(report)
                if report.identity.instance_id.as_str() == instance.as_str()
                    && observation.kubernetes.as_ref().is_some_and(|kubernetes| {
                        kubernetes.pod_uid.as_ref().is_some_and(|uid| {
                            uid.as_str() == instance
                                && raw.pods.iter().any(|pod| {
                                    pod.uid().as_deref() == Some(uid.as_str())
                                        && pod.labels().get(INSTANCE_LABEL).map(String::as_str)
                                            == Some(instance.as_str())
                                })
                        })
                    }) =>
            {
                Some(report.identity.clone())
            }
            _ => None,
        })
        .collect::<Vec<ReplicaIdentity>>();
    if matches.len() == 1 {
        RoutingObservation {
            service_present: true,
            unresolved_write_target: false,
            write_target: matches.into_iter().next(),
        }
    } else {
        RoutingObservation {
            service_present: true,
            unresolved_write_target: true,
            write_target: None,
        }
    }
}
