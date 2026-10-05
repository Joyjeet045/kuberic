use super::*;
use kuberic_runtime::protocol::command::ScaleDownResource;
use kuberic_runtime::protocol::types::ReplacementCleanup;

pub(super) fn transition_id(
    snapshot: &ObservationSnapshot,
    kind: TransitionKind,
    current: &ConfigurationDescriptor,
) -> kuberic_runtime::protocol::types::TransitionId {
    match &snapshot.status.pending_replacement_cleanup {
        Some(cleanup) if !current.members.iter().any(|m| m.identity == cleanup.target) => {
            cleanup.transition_id(kind, &current.configuration_id)
        }
        _ => derive_transition_id(&snapshot.resource_uid, kind, &current.configuration_id),
    }
}

pub(super) fn freeze(
    snapshot: &ObservationSnapshot,
    target: &ReplicaIdentity,
) -> Option<ReplacementCleanup> {
    let exact = secondary_scale_down::resources(snapshot, target)?;
    if ![
        (&exact.identity.pod, &exact.pod),
        (&exact.identity.pvc, &exact.pvc),
        (&exact.identity.endpoint, &exact.endpoint),
    ]
    .iter()
    .all(|(identity, observed)| secondary_scale_down::observed(identity, observed))
    {
        return None;
    }
    let mut cleanup = ReplacementCleanup {
        resource_uid: snapshot.resource_uid.clone(),
        target: target.clone(),
        resources: exact.identity.clone(),
    };
    match (&cleanup.resources.endpoint, &exact.endpoint) {
        (_, kuberic_runtime::protocol::observation::ExactResourceObservation::NotFound) => {
            cleanup.resources.endpoint =
                kuberic_runtime::protocol::types::CleanupResourceIdentity::Absent {
                    name: cleanup.resources.endpoint.name().into(),
                };
        }
        (
            kuberic_runtime::protocol::types::CleanupResourceIdentity::Present { .. },
            kuberic_runtime::protocol::observation::ExactResourceObservation::FrozenUidPresent {
                ..
            },
        ) => {}
        _ => return None,
    }
    kuberic_runtime::protocol::validation::validate_replacement_cleanup(&cleanup).ok()?;
    Some(cleanup)
}

pub(super) fn admit(
    snapshot: &ObservationSnapshot,
    target: &ReplicaIdentity,
    config: &EvaluationConfig,
) -> Option<Plan> {
    if snapshot
        .status
        .pending_replacement_cleanup
        .as_ref()
        .is_some_and(|cleanup| cleanup.target != *target)
    {
        return Some(waiting(snapshot, config));
    }
    if snapshot
        .status
        .pending_replacement_cleanup
        .as_ref()
        .is_some_and(|cleanup| cleanup.target == *target)
    {
        return None;
    }
    let Some(cleanup) = freeze(snapshot, target) else {
        return Some(waiting(snapshot, config));
    };
    let mut status = snapshot.status.clone();
    status.pending_replacement_cleanup = Some(cleanup);
    if kuberic_runtime::protocol::validation::validate_status(&status).is_err() {
        return Some(waiting(snapshot, config));
    }
    Some(Plan::Apply {
        changes: vec![KubernetesChange::PersistStatus {
            status: Box::new(waiting_status(
                status,
                "ReplacementCleanupFrozen",
                "Persist exact old-incarnation cleanup before replacement side effects",
            )),
        }],
    })
}

pub(super) fn waiting(snapshot: &ObservationSnapshot, config: &EvaluationConfig) -> Plan {
    let (reason, message) = if snapshot.status.last_replacement.is_none()
        && snapshot.status.pending_replacement_cleanup.is_none()
    {
        (
            "ReplacementCleanupIdentityPending",
            "Authoritative old-incarnation cleanup identity is required before replacement; legacy in-flight work without provenance remains fenced",
        )
    } else {
        (
            "ReplacementCleanupPending",
            "Exact replacement cleanup must finish before another operation can start",
        )
    };
    Plan::Wait {
        reason: WaitReason::ActiveTransition,
        status: waiting_status(snapshot.status.clone(), reason, message),
        requeue_after_seconds: config.wait_requeue_seconds,
    }
}

pub(super) fn evaluate(
    snapshot: &ObservationSnapshot,
    cleanup: &ReplacementCleanup,
    config: &EvaluationConfig,
) -> Plan {
    let Some(exact) = secondary_scale_down::resources(snapshot, &cleanup.target)
        .filter(|r| r.identity == cleanup.resources && r.resource_uid == cleanup.resource_uid)
    else {
        return waiting(snapshot, config);
    };
    for (kind, identity, observation) in [
        (
            ScaleDownResource::Endpoint,
            &cleanup.resources.endpoint,
            &exact.endpoint,
        ),
        (ScaleDownResource::Pod, &cleanup.resources.pod, &exact.pod),
        (ScaleDownResource::Pvc, &cleanup.resources.pvc, &exact.pvc),
    ] {
        if secondary_scale_down::absent(identity, observation) {
            continue;
        }
        return match secondary_scale_down::delete(kind, identity, observation) {
            Some(change) => Plan::Apply {
                changes: vec![change],
            },
            None => waiting(snapshot, config),
        };
    }
    let mut status = snapshot.status.clone();
    status.last_replacement = None;
    Plan::Apply {
        changes: vec![KubernetesChange::PersistStatus {
            status: Box::new(waiting_status(
                status,
                "ReplacementCleanupComplete",
                "Frozen replacement resources are absent; re-observe before admitting new work",
            )),
        }],
    }
}
