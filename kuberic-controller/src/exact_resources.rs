use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod};
use kube::{Resource, ResourceExt};
use kuberic_runtime::protocol::observation::{
    ExactResourceObservation, ReplicaObservationKey, SecondaryScaleDownResourceObservation,
};
use kuberic_runtime::protocol::types::{
    CleanupResourceIdentity, Epoch, OperationId, PodUid, PvcUid, ReplicaCleanupIdentity, ReplicaId,
    ReplicaIdentity, ReplicaRole, ResourceUid, derive_agent_generation, derive_initialization_id,
    derive_replica_endpoint_name,
};

use crate::crd::{INSTANCE_LABEL, REPLICA_ID_LABEL, SCALE_UP_ALLOCATION_ANNOTATION, SET_UID_LABEL};
use crate::observation::{
    ExactLookup, RawAgentObservation, RawObservation, RawObservationFailure, RawScaleDownResources,
};

pub(crate) fn requests(
    raw: &RawObservation,
) -> Vec<(ReplicaIdentity, ReplicaCleanupIdentity, bool)> {
    let Some(status) = raw.set.status.as_ref().map(|s| &s.authority) else {
        return Vec::new();
    };
    let mut requests = Vec::new();
    if let Some(allocation) = status.scale_up_allocation.as_ref() {
        push_request(
            &mut requests,
            allocation.observation_target(),
            scale_up_allocation_identity(raw, allocation),
            true,
        );
    }
    if let Some(intent) = status
        .secondary_scale_down_cleanup
        .as_ref()
        .map(|c| &c.evidence.preparation.intent)
        .or_else(|| status.transition.as_ref()?.secondary_scale_down.as_ref())
    {
        push_request(
            &mut requests,
            intent.target.clone(),
            intent.cleanup.clone(),
            true,
        );
    }
    if let Some(cleanup) = status.scale_up_cleanup.as_deref() {
        push_request(
            &mut requests,
            cleanup.target.clone(),
            cleanup.resources.clone(),
            true,
        );
    }
    if let Some(cleanup) = &status.last_replacement {
        push_request(
            &mut requests,
            cleanup.target.clone(),
            cleanup.resources.clone(),
            true,
        );
    }
    if let Some(provisioning) = status
        .provisioning
        .as_ref()
        .filter(|provisioning| provisioning.scale_up().is_some())
    {
        let target =
            provisioning.target_identity(&ResourceUid::new(raw.set.uid().unwrap_or_default()));
        push_request(
            &mut requests,
            target.clone(),
            scale_up_candidate_identity(raw, &target, Some(&provisioning.pvc_uid)),
            false,
        );
    }
    if let Some(target) = status
        .transition
        .as_ref()
        .and_then(scale_up_transition_target)
    {
        push_request(
            &mut requests,
            target.clone(),
            candidate(raw, target)
                .unwrap_or_else(|| scale_up_candidate_identity(raw, target, None)),
            false,
        );
    }
    if let Some(receipt) = status.last_scale_up.as_deref() {
        let target = &receipt.intent.target;
        push_request(
            &mut requests,
            target.clone(),
            candidate(raw, target)
                .unwrap_or_else(|| scale_up_candidate_identity(raw, target, None)),
            false,
        );
    }
    // Admission already captured the identity. Old-resource lookups are not
    // prerequisites for building or accepting the new topology.
    if status.pending_replacement_cleanup.is_some()
        || status.transition.is_some()
        || status.provisioning.is_some()
    {
        return requests;
    }
    let Some(topology) = status.topology.as_ref() else {
        return requests;
    };
    let mut targets = if raw.set.spec.replicas > 0
        && (raw.set.spec.replicas as usize) < topology.configuration.members.len()
    {
        topology
            .configuration
            .members
            .iter()
            .filter(|m| m.role == ReplicaRole::ActiveSecondary)
            .max_by_key(|m| m.identity.replica_id)
            .into_iter()
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    for member in &topology.configuration.members {
        if member.role == ReplicaRole::ActiveSecondary
            && replacement_candidate(raw, &member.identity, topology.configuration.epoch)
            && !targets.contains(&member)
        {
            targets.push(member);
        }
    }
    for member in targets {
        if let Some(identity) = candidate(raw, &member.identity) {
            push_request(&mut requests, member.identity.clone(), identity, false);
        }
    }
    requests
}

fn push_request(
    requests: &mut Vec<(ReplicaIdentity, ReplicaCleanupIdentity, bool)>,
    target: ReplicaIdentity,
    identity: ReplicaCleanupIdentity,
    frozen: bool,
) {
    if !requests.iter().any(|(existing, _, _)| existing == &target) {
        requests.push((target, identity, frozen));
    }
}

fn scale_up_transition_target(
    transition: &kuberic_runtime::protocol::types::TransitionIntent,
) -> Option<&ReplicaIdentity> {
    transition
        .scale_up
        .as_deref()
        .map(|intent| &intent.target)
        .or_else(|| {
            transition
                .scale_up_failover
                .as_deref()
                .map(|evidence| &evidence.intent.target)
        })
}

fn scale_up_lifecycle_target(raw: &RawObservation, target: &ReplicaIdentity) -> bool {
    let Some(status) = raw.set.status.as_ref().map(|status| &status.authority) else {
        return false;
    };
    status
        .scale_up_allocation
        .as_ref()
        .is_some_and(|allocation| allocation.observation_target() == *target)
        || status
            .scale_up_cleanup
            .as_deref()
            .is_some_and(|cleanup| cleanup.target == *target)
        || status
            .provisioning
            .as_ref()
            .filter(|provisioning| provisioning.scale_up().is_some())
            .is_some_and(|provisioning| {
                provisioning.target_identity(&ResourceUid::new(raw.set.uid().unwrap_or_default()))
                    == *target
            })
        || status
            .transition
            .as_ref()
            .and_then(scale_up_transition_target)
            == Some(target)
        || status
            .last_scale_up
            .as_deref()
            .is_some_and(|receipt| receipt.intent.target == *target)
}

fn scale_up_allocation_identity(
    raw: &RawObservation,
    allocation: &kuberic_runtime::protocol::types::ScaleUpAllocation,
) -> ReplicaCleanupIdentity {
    let pod_name = format!(
        "{}-{}",
        raw.set.name_any(),
        allocation.target_replica_id.value()
    );
    let pvc_name = format!("{pod_name}-data");
    let endpoint_name = format!("{pod_name}-allocation");
    ReplicaCleanupIdentity {
        pod: allocation.pod_uid.as_ref().map_or_else(
            || CleanupResourceIdentity::Absent {
                name: pod_name.clone(),
            },
            |uid| CleanupResourceIdentity::Present {
                name: pod_name.clone(),
                uid: uid.to_string(),
            },
        ),
        pvc: allocation.pvc_uid.as_ref().map_or_else(
            || CleanupResourceIdentity::Absent {
                name: pvc_name.clone(),
            },
            |uid| CleanupResourceIdentity::Present {
                name: pvc_name.clone(),
                uid: uid.to_string(),
            },
        ),
        endpoint: CleanupResourceIdentity::Absent {
            name: endpoint_name,
        },
    }
}

fn scale_up_candidate_identity(
    raw: &RawObservation,
    target: &ReplicaIdentity,
    pvc_uid: Option<&PvcUid>,
) -> ReplicaCleanupIdentity {
    let pod_name = format!("{}-{}", raw.set.name_any(), target.replica_id.value());
    let pvc_name = format!("{pod_name}-data");
    let endpoint_name =
        derive_replica_endpoint_name(&ResourceUid::new(raw.set.uid().unwrap_or_default()), target);
    ReplicaCleanupIdentity {
        pod: CleanupResourceIdentity::Present {
            name: pod_name,
            uid: target.instance_id.to_string(),
        },
        pvc: match pvc_uid {
            Some(uid) => CleanupResourceIdentity::Present {
                name: pvc_name,
                uid: uid.to_string(),
            },
            None => CleanupResourceIdentity::Absent { name: pvc_name },
        },
        endpoint: CleanupResourceIdentity::Absent {
            name: endpoint_name,
        },
    }
}

fn replacement_candidate(raw: &RawObservation, target: &ReplicaIdentity, epoch: Epoch) -> bool {
    use kuberic_runtime::control::proto;
    if !raw
        .pods
        .iter()
        .any(|p| p.uid().as_deref() == Some(target.instance_id.as_str()))
    {
        return true;
    }
    let Some(RawAgentObservation::Report(report)) = raw.agents.get(&ReplicaObservationKey::new(
        target.replica_id,
        target.instance_id.clone(),
    )) else {
        return false;
    };
    report.reported_fault == proto::FaultType::Permanent as i32
        || (report.role != proto::ReplicaRole::Primary as i32
            && report.write_status != proto::AccessStatus::Granted as i32
            && report.epoch.as_ref().is_some_and(|e| {
                (e.data_loss_number, e.configuration_number)
                    < (epoch.data_loss_number, epoch.configuration_number)
            }))
}

fn candidate(raw: &RawObservation, target: &ReplicaIdentity) -> Option<ReplicaCleanupIdentity> {
    let resource_uid = ResourceUid::new(raw.set.uid().unwrap_or_default());
    let pod = raw
        .pods
        .iter()
        .find(|p| p.uid().as_deref() == Some(target.instance_id.as_str()));
    let pvc = raw.pvcs.iter().find(|pvc| {
        let Some(uid) = pvc.uid() else { return false };
        derive_agent_generation(&derive_initialization_id(
            &resource_uid,
            target.replica_id,
            &PodUid::new(target.instance_id.as_str()),
            &PvcUid::new(uid),
        )) == target.agent_generation
    });
    let pvc = pvc?;
    let pvc_name = pvc.name_any();
    let pod_name = if let Some(pod) = pod {
        // The durable generation binds this PVC UID; the actual mount must agree too.
        if mounted_pvc(pod) != Some(pvc_name.as_str()) {
            return None;
        }
        pod.name_any()
    } else {
        // Both scaffold constructors use <pod>-data. The generation proves storage
        // provenance; a subsequent exact Pod GET, not list omission, proves absence.
        let name = pvc_name.strip_suffix("-data")?;
        name.to_string()
    };
    let endpoint_name = derive_replica_endpoint_name(&resource_uid, target);
    let endpoint = raw
        .services
        .iter()
        .find(|s| s.name_any() == endpoint_name)
        .and_then(|s| s.uid())
        .map(|uid| CleanupResourceIdentity::Present {
            name: endpoint_name.clone(),
            uid,
        })
        .unwrap_or(CleanupResourceIdentity::Absent {
            name: endpoint_name,
        });
    Some(ReplicaCleanupIdentity {
        pod: CleanupResourceIdentity::Present {
            name: pod_name,
            uid: target.instance_id.to_string(),
        },
        pvc: CleanupResourceIdentity::Present {
            name: pvc_name,
            uid: pvc.uid().expect("proven UID"),
        },
        endpoint,
    })
}

fn mounted_pvc(pod: &Pod) -> Option<&str> {
    let mut claims = pod.spec.as_ref()?.volumes.as_ref()?.iter().filter_map(|v| {
        v.persistent_volume_claim
            .as_ref()
            .map(|p| p.claim_name.as_str())
    });
    let name = claims.next()?;
    claims.next().is_none().then_some(name)
}

pub(crate) fn pod_matches_scale_up_allocation_metadata(
    set: &crate::crd::KubericSet,
    pod: &Pod,
    replica_id: ReplicaId,
    expected_pvc_uid: Option<&PvcUid>,
) -> bool {
    let Some(set_uid) = set.uid().filter(|uid| !uid.is_empty()) else {
        return false;
    };
    let Some(owner) = set.controller_owner_ref(&()) else {
        return false;
    };
    let pod_name = format!("{}-{}", set.name_any(), replica_id.value());
    let pvc_name = format!("{pod_name}-data");
    let configured_pvc_uid = pod
        .spec
        .as_ref()
        .and_then(|spec| {
            spec.containers
                .iter()
                .find(|container| container.name == "application")
        })
        .and_then(|container| container.env.as_ref())
        .and_then(|env| env.iter().find(|env| env.name == "KUBERIC_PVC_UID"))
        .and_then(|env| env.value.as_deref());
    pod.name_any() == pod_name
        && pod.labels().get(SET_UID_LABEL).map(String::as_str) == Some(set_uid.as_str())
        && pod.labels().get(REPLICA_ID_LABEL).map(String::as_str)
            == Some(replica_id.to_string().as_str())
        && pod
            .owner_references()
            .iter()
            .any(|candidate| candidate == &owner)
        && mounted_pvc(pod) == Some(pvc_name.as_str())
        && expected_pvc_uid.is_some_and(|expected| configured_pvc_uid == Some(expected.as_str()))
}

pub(crate) fn name(identity: &CleanupResourceIdentity) -> &str {
    match identity {
        CleanupResourceIdentity::Present { name, .. }
        | CleanupResourceIdentity::Absent { name } => name,
    }
}

pub(crate) fn protected(
    raw: &RawObservation,
    resource_name: Option<&str>,
    resource_uid: Option<&str>,
) -> bool {
    protected_by_active_lifecycle(raw, resource_name, resource_uid)
        || raw
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.last_scale_up.as_deref())
            .is_some_and(|receipt| {
                scale_up_target_protected(
                    raw,
                    &receipt.intent.target,
                    None,
                    resource_name,
                    resource_uid,
                )
            })
}

pub(crate) fn protected_from_exact_pod_fence(
    raw: &RawObservation,
    resource_name: Option<&str>,
    resource_uid: Option<&str>,
) -> bool {
    protected_by_active_lifecycle(raw, resource_name, resource_uid)
}

fn protected_by_active_lifecycle(
    raw: &RawObservation,
    resource_name: Option<&str>,
    resource_uid: Option<&str>,
) -> bool {
    let Some(status) = raw.set.status.as_ref().map(|s| &s.authority) else {
        return false;
    };
    let protected_cleanup = status.transition.iter().filter_map(|t| t.secondary_scale_down.as_ref())
        .chain(status.secondary_scale_down_cleanup.iter().map(|c| &c.evidence.preparation.intent))
        .chain(status.last_secondary_removal.iter().map(|r| &r.evidence.preparation.intent))
        .flat_map(|intent| [&intent.cleanup.pod, &intent.cleanup.pvc, &intent.cleanup.endpoint])
        .chain(status.last_replacement.iter().flat_map(|c| [&c.resources.pod, &c.resources.pvc, &c.resources.endpoint]))
        .chain(status.pending_replacement_cleanup.iter().flat_map(|c| [&c.resources.pod, &c.resources.pvc, &c.resources.endpoint]))
        .chain(status.scale_up_cleanup.iter().flat_map(|c| [&c.resources.pod, &c.resources.pvc, &c.resources.endpoint]))
        .any(|identity| resource_name == Some(name(identity))
            || matches!(identity, CleanupResourceIdentity::Present { uid, .. } if resource_uid == Some(uid.as_str())));
    protected_cleanup
        || status
            .scale_up_allocation
            .as_ref()
            .is_some_and(|allocation| {
                let identity = scale_up_allocation_identity(raw, allocation);
                [&identity.pod, &identity.pvc, &identity.endpoint]
                    .into_iter()
                    .any(|identity| {
                        resource_name == Some(name(identity))
                            || matches!(identity, CleanupResourceIdentity::Present { uid, .. }
                                if resource_uid == Some(uid.as_str()))
                    })
            })
        || status
            .provisioning
            .as_ref()
            .filter(|provisioning| provisioning.scale_up().is_some())
            .is_some_and(|provisioning| {
                let target = provisioning
                    .target_identity(&ResourceUid::new(raw.set.uid().unwrap_or_default()));
                scale_up_target_protected(
                    raw,
                    &target,
                    Some(&provisioning.pvc_uid),
                    resource_name,
                    resource_uid,
                )
            })
        || status
            .transition
            .as_ref()
            .and_then(scale_up_transition_target)
            .is_some_and(|target| {
                scale_up_target_protected(raw, target, None, resource_name, resource_uid)
            })
}

fn scale_up_target_protected(
    raw: &RawObservation,
    target: &ReplicaIdentity,
    pvc_uid: Option<&PvcUid>,
    resource_name: Option<&str>,
    resource_uid: Option<&str>,
) -> bool {
    let identity = scale_up_candidate_identity(raw, target, pvc_uid);
    [&identity.pod, &identity.pvc, &identity.endpoint]
        .into_iter()
        .any(|identity| {
            resource_name == Some(name(identity))
                || matches!(identity, CleanupResourceIdentity::Present { uid, .. }
                    if resource_uid == Some(uid.as_str()))
        })
}

pub(crate) fn matches<K: ResourceExt>(identity: &CleanupResourceIdentity, object: &K) -> bool {
    matches!(identity, CleanupResourceIdentity::Present { name, uid }
        if object.name_any() == *name && object.uid().as_deref() == Some(uid.as_str()))
}

pub(crate) fn finish(raw: &mut RawObservation, mut exact: RawScaleDownResources, frozen: bool) {
    let scale_up_lifecycle = scale_up_lifecycle_target(raw, &exact.target);
    if !frozen && let ExactLookup::Present(service) = &exact.endpoint {
        let owned = service.labels().get(SET_UID_LABEL) == raw.set.metadata.uid.as_ref()
            && service
                .spec
                .as_ref()
                .and_then(|s| s.selector.as_ref())
                .and_then(|s| s.get(INSTANCE_LABEL))
                .map(String::as_str)
                == Some(exact.target.instance_id.as_str());
        if !owned {
            exact.endpoint =
                ExactLookup::Failed("endpoint is not owned by the exact accepted target".into());
        } else if let Some(uid) = service.uid() {
            exact.identity.endpoint = CleanupResourceIdentity::Present {
                name: service.name_any(),
                uid,
            };
        }
    }
    if !frozen && matches!(exact.endpoint, ExactLookup::NotFound) {
        exact.identity.endpoint = CleanupResourceIdentity::Absent {
            name: name(&exact.identity.endpoint).into(),
        };
    }
    for (kind, identity, result) in [
        (
            "Pod",
            &exact.identity.pod,
            classify(&exact.identity.pod, &exact.pod),
        ),
        (
            "PVC",
            &exact.identity.pvc,
            classify(&exact.identity.pvc, &exact.pvc),
        ),
        (
            "Service",
            &exact.identity.endpoint,
            classify(&exact.identity.endpoint, &exact.endpoint),
        ),
    ] {
        // Scale-up keeps failed exact GETs in the typed lifecycle observation so
        // cleanup and retry remain blocked, but they must not become a global
        // observation failure that prevents accepted-primary failover.
        if !scale_up_lifecycle && let ExactResourceObservation::LookupFailed { message } = result {
            raw.failures.push(RawObservationFailure {
                source: format!("exact-{kind}/{}", name(identity)),
                message,
            });
        }
    }
    merge(&mut raw.pods, name(&exact.identity.pod), &exact.pod);
    merge(&mut raw.pvcs, name(&exact.identity.pvc), &exact.pvc);
    merge(
        &mut raw.services,
        name(&exact.identity.endpoint),
        &exact.endpoint,
    );
    raw.exact_resources.push(exact);
}

fn merge<K: ResourceExt + Clone>(objects: &mut Vec<K>, name: &str, lookup: &ExactLookup<K>) {
    if !matches!(lookup, ExactLookup::Failed(_)) {
        objects.retain(|old| old.name_any() != name);
    }
    if let ExactLookup::Present(object) = lookup {
        objects.push(object.clone());
    }
}

pub(crate) fn classify<K: ResourceExt>(
    identity: &CleanupResourceIdentity,
    lookup: &ExactLookup<K>,
) -> ExactResourceObservation {
    match lookup {
        ExactLookup::NotFound => ExactResourceObservation::NotFound,
        ExactLookup::Failed(message) => ExactResourceObservation::LookupFailed {
            message: message.clone(),
        },
        ExactLookup::Present(object) => {
            let Some((uid, resource_version)) = object
                .uid()
                .filter(|s| !s.is_empty())
                .zip(object.resource_version().filter(|s| !s.is_empty()))
            else {
                return ExactResourceObservation::LookupFailed {
                    message: "exact GET omitted UID or resourceVersion".into(),
                };
            };
            if object.name_any() != name(identity) {
                return ExactResourceObservation::LookupFailed {
                    message: "exact GET returned another name".into(),
                };
            }
            if matches(identity, object) {
                ExactResourceObservation::FrozenUidPresent { resource_version }
            } else {
                ExactResourceObservation::ReplacementPresent {
                    uid,
                    resource_version,
                }
            }
        }
    }
}

pub(crate) fn normalized(
    raw: &RawObservation,
    resource_uid: &ResourceUid,
) -> Vec<SecondaryScaleDownResourceObservation> {
    raw.exact_resources
        .iter()
        .map(|r| SecondaryScaleDownResourceObservation {
            resource_uid: resource_uid.clone(),
            target: r.target.clone(),
            identity: r.identity.clone(),
            pod: classify(&r.identity.pod, &r.pod),
            pod_allocation_operation_id: match &r.pod {
                ExactLookup::Present(pod) => pod
                    .annotations()
                    .get(SCALE_UP_ALLOCATION_ANNOTATION)
                    .filter(|operation_id| !operation_id.is_empty())
                    .map(OperationId::new),
                ExactLookup::NotFound | ExactLookup::Failed(_) => None,
            },
            pod_matches_allocation_metadata: match &r.pod {
                ExactLookup::Present(pod) => pod_matches_scale_up_allocation_metadata(
                    &raw.set,
                    pod,
                    r.target.replica_id,
                    raw.set
                        .status
                        .as_ref()
                        .and_then(|status| status.authority.scale_up_allocation.as_ref())
                        .filter(|allocation| allocation.target_replica_id == r.target.replica_id)
                        .and_then(|allocation| allocation.pvc_uid.as_ref()),
                ),
                ExactLookup::NotFound | ExactLookup::Failed(_) => false,
            },
            pvc: classify(&r.identity.pvc, &r.pvc),
            pvc_allocation_operation_id: match &r.pvc {
                ExactLookup::Present(pvc) => pvc
                    .annotations()
                    .get(SCALE_UP_ALLOCATION_ANNOTATION)
                    .filter(|operation_id| !operation_id.is_empty())
                    .map(OperationId::new),
                ExactLookup::NotFound | ExactLookup::Failed(_) => None,
            },
            endpoint: classify(&r.identity.endpoint, &r.endpoint),
        })
        .collect()
}

pub(crate) fn frozen_pod_target<'a>(
    raw: &'a RawObservation,
    pod: &Pod,
) -> Option<&'a ReplicaIdentity> {
    raw.exact_resources
        .iter()
        .find(|r| matches(&r.identity.pod, pod))
        .map(|r| &r.target)
}

pub(crate) fn frozen_pvc_target<'a>(
    raw: &'a RawObservation,
    pvc: &PersistentVolumeClaim,
) -> Option<&'a ReplicaIdentity> {
    raw.exact_resources
        .iter()
        .find(|r| matches(&r.identity.pvc, pvc))
        .map(|r| &r.target)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_reads_supersede_stale_lists_without_adopting_replacements() {
        let pod = Pod {
            metadata: kube::core::ObjectMeta {
                name: Some("frozen".into()),
                uid: Some("old".into()),
                resource_version: Some("10".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        let identity = CleanupResourceIdentity::Present {
            name: "frozen".into(),
            uid: "old".into(),
        };
        let mut pods = vec![pod.clone()];
        let failed = ExactLookup::Failed("403".into());
        merge(&mut pods, "frozen", &failed);
        assert_eq!(pods.as_slice(), std::slice::from_ref(&pod));
        assert!(matches!(
            classify(&identity, &failed),
            ExactResourceObservation::LookupFailed { .. }
        ));
        merge(&mut pods, "frozen", &ExactLookup::NotFound);
        assert!(pods.is_empty());
        let mut replacement = pod;
        replacement.metadata.uid = Some("new".into());
        let observed = ExactLookup::Present(replacement.clone());
        merge(&mut pods, "frozen", &observed);
        assert_eq!(pods, [replacement]);
        assert_eq!(
            classify(&identity, &observed),
            ExactResourceObservation::ReplacementPresent {
                uid: "new".into(),
                resource_version: "10".into(),
            }
        );
    }
}
