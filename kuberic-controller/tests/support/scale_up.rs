use super::*;
use kube::Client;
use kube::Resource;
use kuberic_controller::cluster_api::{GrpcAgentApi, KubeClusterApi};
use kuberic_controller::crd::{INSTANCE_LABEL, SCALE_UP_ALLOCATION_ANNOTATION};
use kuberic_controller::executor::execute_plan;
use kuberic_runtime::protocol::command::{KubernetesChange, SafetyChange, ScaleDownResource};
use kuberic_runtime::protocol::types::{
    AccessStatus, ConfigurationDescriptor, ConfigurationMember, Epoch, OperationId, TransitionKind,
};
use std::collections::BTreeSet;

fn enabled() -> EvaluationConfig {
    EvaluationConfig {
        enable_secondary_scale_down: true,
        allow_scale_up: true,
        ..config()
    }
}

fn fixture(count: u32, desired: u32) -> RawObservation {
    secondary_scale_down::fixture(count, desired)
}

fn accepted_count(raw: &RawObservation) -> u32 {
    raw.set
        .status
        .as_ref()
        .and_then(|status| status.authority.effective_policy.as_ref())
        .map_or(0, |policy| policy.replica_set_size)
}

fn candidate_key(raw: &RawObservation) -> Option<(ReplicaObservationKey, String)> {
    candidate_keys(raw).into_iter().next()
}

fn candidate_keys(raw: &RawObservation) -> Vec<(ReplicaObservationKey, String)> {
    let authority = raw.set.status.as_ref().map(|status| &status.authority);
    let accepted = authority
        .and_then(|authority| authority.topology.as_ref())
        .map(|topology| &topology.configuration.members);
    raw.pods
        .iter()
        .filter_map(|pod| {
            let replica_id = pod
                .labels()
                .get(REPLICA_ID_LABEL)?
                .parse::<i64>()
                .ok()
                .map(ReplicaId::new)?;
            let pod_uid = pod.uid()?;
            if accepted.is_some_and(|members| {
                members.iter().any(|member| {
                    member.identity.replica_id == replica_id
                        && member.identity.instance_id.as_str() == pod_uid
                })
            }) {
                return None;
            }
            let pvc_name = pod
                .spec
                .as_ref()?
                .volumes
                .as_ref()?
                .iter()
                .find_map(|volume| {
                    volume
                        .persistent_volume_claim
                        .as_ref()
                        .map(|claim| claim.claim_name.as_str())
                })?;
            let live_pvc_uid = raw
                .pvcs
                .iter()
                .find(|pvc| pvc.name_any() == pvc_name)?
                .uid()?;
            let configured_pvc_uid = pod
                .spec
                .as_ref()?
                .containers
                .iter()
                .find(|container| container.name == "application")?
                .env
                .as_ref()?
                .iter()
                .find(|env| env.name == "KUBERIC_PVC_UID")?
                .value
                .clone()?;
            let active_candidate = authority.is_some_and(|authority| {
                authority
                    .scale_up_allocation
                    .as_ref()
                    .is_some_and(|allocation| {
                        allocation.target_replica_id == replica_id
                            && pod
                                .annotations()
                                .get(SCALE_UP_ALLOCATION_ANNOTATION)
                                .map(String::as_str)
                                == Some(allocation.operation_id.as_str())
                    })
                    || authority
                        .provisioning
                        .as_ref()
                        .is_some_and(|provisioning| provisioning.pod_uid.as_str() == pod_uid)
                    || authority
                        .transition
                        .as_ref()
                        .and_then(|transition| {
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
                        })
                        .is_some_and(|target| target.instance_id.as_str() == pod_uid)
            });
            if !active_candidate && configured_pvc_uid != live_pvc_uid {
                return None;
            }
            Some((
                ReplicaObservationKey::new(replica_id, ReplicaInstanceId::new(&pod_uid)),
                configured_pvc_uid,
            ))
        })
        .collect()
}

fn set_configured_pvc_uid(pod: &mut k8s_openapi::api::core::v1::Pod, pvc_uid: &str) {
    let env = pod
        .spec
        .as_mut()
        .expect("replica Pod spec")
        .containers
        .iter_mut()
        .find(|container| container.name == "application")
        .expect("application container")
        .env
        .get_or_insert_default();
    if let Some(configured) = env.iter_mut().find(|env| env.name == "KUBERIC_PVC_UID") {
        configured.value = Some(pvc_uid.to_string());
    } else {
        env.push(k8s_openapi::api::core::v1::EnvVar {
            name: "KUBERIC_PVC_UID".into(),
            value: Some(pvc_uid.to_string()),
            ..Default::default()
        });
    }
}

async fn observe_fresh_candidate(api: &InMemoryClusterApi) {
    let mut raw = api.observation().await;
    for (key, pvc_uid) in candidate_keys(&raw) {
        if raw.agents.contains_key(&key) {
            continue;
        }
        raw.agents.insert(
            key.clone(),
            RawAgentObservation::Report(Box::new(proto::AgentStatusReport {
                protocol_version: kuberic_runtime::protocol::PROTOCOL_VERSION,
                resource_uid: UID.to_string(),
                process_session_id: format!("fresh-session-{}", key.replica_id),
                report_sequence: 1,
                storage_state: proto::AgentStorageState::Uninitialized as i32,
                pod_uid: key.instance_id.to_string(),
                pvc_uid,
                replica_id: key.replica_id.value(),
                ..Default::default()
            })),
        );
    }
    api.set_observation(raw).await;
}

async fn apply_command(api: &InMemoryClusterApi, command: &ProtocolCommand) {
    if matches!(
        command,
        ProtocolCommand::PrepareSecondaryRemoval(_)
            | ProtocolCommand::AcceptSecondaryRemovalCommit(_)
            | ProtocolCommand::RetireReplica(_)
    ) || matches!(
        command,
        ProtocolCommand::EnsureConfiguration(command)
            if command.secondary_removal_evidence.is_some()
    ) {
        secondary_scale_down::apply_command(api, command).await;
        return;
    }
    if matches!(command, ProtocolCommand::PrepareSwitchover(_))
        || matches!(
            command,
            ProtocolCommand::EnsureConfiguration(command)
                if command.transition_kind == TransitionKind::PlannedSwitchover
        )
    {
        observe_switchover_result(api, command).await;
        return;
    }
    let mut raw = api.observation().await;
    match command {
        ProtocolCommand::InitializeAgentStore(command) => {
            let target = ReplicaIdentity {
                replica_id: command.local_replica_id,
                instance_id: command.expected_instance_id.clone(),
                agent_generation: command.assigned_agent_generation.clone(),
            };
            let key = ReplicaObservationKey::new(target.replica_id, target.instance_id.clone());
            let previous = match raw.agents.get(&key) {
                Some(RawAgentObservation::Report(report)) => report.report_sequence,
                _ => 0,
            };
            raw.agents.insert(
                key,
                RawAgentObservation::Report(Box::new(proto::AgentStatusReport {
                    protocol_version: kuberic_runtime::protocol::PROTOCOL_VERSION,
                    resource_uid: UID.to_string(),
                    identity: Some(target.into()),
                    process_session_id: format!("initialized-session-{}", command.local_replica_id),
                    report_sequence: previous + 1,
                    role: proto::ReplicaRole::None as i32,
                    read_status: proto::AccessStatus::NotPrimary as i32,
                    write_status: proto::AccessStatus::NotPrimary as i32,
                    epoch: Some(Epoch::default().into()),
                    storage_state: proto::AgentStorageState::Initialized as i32,
                    healthy: true,
                    replica_id: command.local_replica_id.value(),
                    ..Default::default()
                })),
            );
        }
        ProtocolCommand::EnsureReplicaBuild(command) => {
            if command.retire {
                for observation in raw.agents.values_mut() {
                    if let RawAgentObservation::Report(report) = observation {
                        report
                            .builds
                            .retain(|build| build.build_id != command.operation_id.as_str());
                        report.report_sequence += 1;
                    }
                }
                api.set_observation(raw).await;
                return;
            }
            let source_key = ReplicaObservationKey::new(
                command.local_replica_id,
                command.expected_instance_id.clone(),
            );
            let target_key = ReplicaObservationKey::new(
                command.target.replica_id,
                command.target.instance_id.clone(),
            );
            let boundary = match raw.agents.get(&source_key) {
                Some(RawAgentObservation::Report(report)) => report.current_progress,
                _ => panic!("scale-up source report"),
            };
            let catch_up_boundary = boundary + 2;
            let build = proto::BuildStatus {
                build_id: command.operation_id.to_string(),
                target: Some(command.target.clone().into()),
                last_sequence: 2,
                durable_lsn: catch_up_boundary,
                completed: true,
                catch_up_boundary_lsn: Some(catch_up_boundary),
                replication_boundary_lsn: boundary,
            };
            for key in [&source_key, &target_key] {
                let RawAgentObservation::Report(report) =
                    raw.agents.get_mut(key).expect("exact build participant")
                else {
                    panic!("exact build participant report")
                };
                report.current_progress = catch_up_boundary;
                report.committed_lsn = catch_up_boundary;
                report.builds = vec![build.clone()];
                report.report_sequence += 1;
                if key == &target_key {
                    report.role = proto::ReplicaRole::IdleSecondary as i32;
                }
            }
        }
        ProtocolCommand::EnsureConfiguration(command) => {
            let target = ReplicaIdentity {
                replica_id: command.local_replica_id,
                instance_id: command.expected_instance_id.clone(),
                agent_generation: command.expected_agent_generation.clone(),
            };
            let key = ReplicaObservationKey::new(target.replica_id, target.instance_id.clone());
            let RawAgentObservation::Report(report) =
                raw.agents.get_mut(&key).expect("configuration participant")
            else {
                panic!("configuration participant report")
            };
            let member = command
                .current_configuration
                .members
                .iter()
                .find(|member| member.identity == target)
                .unwrap_or_else(|| {
                    panic!(
                        "configuration contains local target: target={target:?} command={command:?}"
                    )
                });
            let boundary = command
                .scale_up_evidence
                .as_deref()
                .map_or(report.current_progress, |evidence| {
                    evidence.intent().catch_up_boundary_lsn
                });
            report.role = match member.role {
                ReplicaRole::Primary => proto::ReplicaRole::Primary as i32,
                ReplicaRole::ActiveSecondary => proto::ReplicaRole::ActiveSecondary as i32,
                ReplicaRole::IdleSecondary => proto::ReplicaRole::IdleSecondary as i32,
                ReplicaRole::None => proto::ReplicaRole::None as i32,
            };
            report.read_status = proto::AccessStatus::Granted as i32;
            report.write_status = if member.role == ReplicaRole::Primary {
                match command.primary_write_status {
                    AccessStatus::Granted => proto::AccessStatus::Granted as i32,
                    AccessStatus::ReconfigurationPending => {
                        proto::AccessStatus::ReconfigurationPending as i32
                    }
                    AccessStatus::NoWriteQuorum => proto::AccessStatus::NoWriteQuorum as i32,
                    AccessStatus::NotPrimary => proto::AccessStatus::NotPrimary as i32,
                }
            } else {
                proto::AccessStatus::NotPrimary as i32
            };
            report.epoch = Some(command.current_epoch.into());
            report.previous_configuration = command.previous_configuration.clone().map(Into::into);
            report.current_configuration = Some(command.current_configuration.clone().into());
            report.current_progress = report.current_progress.max(boundary);
            report.committed_lsn = report.committed_lsn.max(boundary);
            report.verified_replication_lsn = Some(boundary);
            report.current_configuration_quorum_progress = boundary;
            report.catch_up_boundary = Some(boundary);
            report.catch_up_complete = true;
            report.pending_operation_id.clear();
            report.pending_configuration = None;
            report.retained_operation_id = command.operation_id.to_string();
            report.scale_up_intent = command
                .scale_up_evidence
                .as_deref()
                .map(|evidence| evidence.intent().clone().into());
            if command.scale_up_evidence.is_some() {
                report.prepared_secondary_removal = None;
                report.secondary_removal_evidence = None;
                report.accepted_secondary_removal = None;
            }
            if command.transition_kind == TransitionKind::Failover {
                report.deactivation_epoch = Some(command.current_epoch.into());
                report.deactivated_lsn = Some(boundary);
            }
            report.report_sequence += 1;
        }
        other => panic!("unexpected scale-up command {other:?}"),
    }
    api.set_observation(raw).await;
}

async fn tick(api: &Arc<InMemoryClusterApi>) -> (ReconcileKind, Vec<EffectRecord>) {
    observe_fresh_candidate(api).await;
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
        effects.len() <= 1
            || matches!(
                effects.as_slice(),
                [
                    EffectRecord::RemoveWriteRouting,
                    EffectRecord::ReplaceStatus
                ] | [
                    EffectRecord::PublishWriteRouting(_),
                    EffectRecord::ReplaceStatus
                ]
            ),
        "at most one authority command per fresh observation: {effects:?}"
    );
    for effect in &effects {
        if let EffectRecord::Execute(command) = effect {
            apply_command(api, command).await;
        }
    }
    (action.kind, effects)
}

async fn finish(api: &Arc<InMemoryClusterApi>, desired: u32) -> Vec<(String, String)> {
    let mut diagnostics = BTreeMap::new();
    for _ in 0..300 {
        let (kind, _) = tick(api).await;
        let raw = api.observation().await;
        if let Some(status) = &raw.set.status {
            for condition in &status.authority.conditions {
                if condition.reason.starts_with("ScaleUp") {
                    diagnostics.insert(condition.reason.clone(), condition.message.clone());
                }
            }
            if kind == ReconcileKind::Stable
                && accepted_count(&raw) == desired
                && status.authority.provisioning.is_none()
                && status.authority.transition.is_none()
            {
                return diagnostics.into_iter().collect();
            }
        }
    }
    let effects = api.effects().await;
    panic!(
        "scale-up did not converge: status={:?} recent_effects={:?}",
        api.observation().await.set.status,
        &effects[effects.len().saturating_sub(12)..]
    );
}

async fn finish_switchover(api: &Arc<InMemoryClusterApi>, primary_id: i64) {
    for _ in 0..200 {
        tick(api).await;
        let raw = api.observation().await;
        let status = raw.set.status.as_ref().unwrap();
        if status
            .authority
            .topology
            .as_ref()
            .is_some_and(|topology| topology.configuration.primary_id.value() == primary_id)
            && status.authority.transition.is_none()
            && status.authority.last_switchover.is_some()
        {
            return;
        }
    }
    let raw = api.observation().await;
    let status = raw.set.status.clone();
    let snapshot = normalize(raw, BTreeMap::new()).unwrap();
    let invalid = snapshot
        .replicas
        .iter()
        .filter_map(|(key, observation)| match &observation.agent {
            AgentObservation::Invalid { message, .. } => Some((key.clone(), message.clone())),
            _ => None,
        })
        .collect::<Vec<_>>();
    let validation = kuberic_runtime::protocol::validation::validate_snapshot(&snapshot);
    let plan = evaluate(&snapshot, &enabled());
    panic!(
        "switchover did not converge: invalid={invalid:?} validation={validation:?} \
         status={status:?} plan={plan:?}"
    );
}

async fn finish_scale_down(api: &Arc<InMemoryClusterApi>, desired: u32) {
    for _ in 0..300 {
        let (kind, _) = tick(api).await;
        let raw = api.observation().await;
        if kind == ReconcileKind::Stable
            && accepted_count(&raw) == desired
            && raw.set.status.as_ref().is_some_and(|status| {
                status.authority.transition.is_none()
                    && status.authority.secondary_scale_down_cleanup.is_none()
            })
        {
            return;
        }
    }
    let raw = api.observation().await;
    let status = raw.set.status.clone();
    panic!(
        "scale-down did not converge: status={:?} plan={:?}",
        status,
        evaluate(&normalize(raw, BTreeMap::new()).unwrap(), &enabled())
    );
}

async fn drive_until_cleanup(api: &Arc<InMemoryClusterApi>) {
    for _ in 0..80 {
        tick(api).await;
        if api
            .observation()
            .await
            .set
            .status
            .as_ref()
            .is_some_and(|status| status.authority.scale_up_cleanup.is_some())
        {
            return;
        }
        let mut raw = api.observation().await;
        if raw
            .set
            .status
            .as_ref()
            .is_some_and(|status| status.authority.provisioning.is_some())
            && candidate_key(&raw).is_some_and(|(key, _)| {
                raw.services.iter().any(|service| {
                    service
                        .spec
                        .as_ref()
                        .and_then(|spec| spec.selector.as_ref())
                        .and_then(|selector| selector.get(INSTANCE_LABEL))
                        .map(String::as_str)
                        == Some(key.instance_id.as_str())
                })
            })
        {
            raw.set.spec.replicas = accepted_count(&raw);
            raw.set.metadata.generation = Some(raw.set.metadata.generation.unwrap_or_default() + 1);
            api.set_observation(raw).await;
        }
    }
    panic!("scale-up cleanup was not frozen");
}

#[tokio::test]
async fn canonical_candidate_is_created_pvc_before_pod_and_lost_create_replays() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(1, 2)));
    api.lose_next_create_reply().await;
    assert_eq!(tick(&api).await.0, ReconcileKind::Applied);
    assert_eq!(tick(&api).await.0, ReconcileKind::Applied);
    assert_eq!(tick(&api).await.0, ReconcileKind::ObservationStale);
    let after_lost_reply = api.observation().await;
    assert!(
        after_lost_reply
            .pvcs
            .iter()
            .any(|pvc| pvc.name_any() == "db-2-data")
    );
    assert!(
        after_lost_reply
            .pods
            .iter()
            .all(|pod| pod.name_any() != "db-2")
    );

    for _ in 0..4 {
        tick(&api).await;
        if api
            .observation()
            .await
            .pods
            .iter()
            .any(|pod| pod.name_any() == "db-2")
        {
            break;
        }
    }
    let after_pod = api.observation().await;
    let pod = after_pod
        .pods
        .iter()
        .find(|pod| pod.name_any() == "db-2")
        .expect("canonical candidate Pod");
    assert_eq!(
        pod.spec.as_ref().unwrap().volumes.as_ref().unwrap()[0]
            .persistent_volume_claim
            .as_ref()
            .unwrap()
            .claim_name,
        "db-2-data"
    );
    assert_eq!(
        pod.spec.as_ref().unwrap().containers[0].image.as_deref(),
        Some("example/db:latest")
    );
    let allocation = after_pod
        .set
        .status
        .as_ref()
        .and_then(|status| status.authority.scale_up_allocation.as_ref())
        .expect("candidate remains in allocation");
    assert_eq!(
        pod.annotations()
            .get(SCALE_UP_ALLOCATION_ANNOTATION)
            .map(String::as_str),
        Some(allocation.operation_id.as_str())
    );
    assert_eq!(
        candidate_keys(&after_pod)
            .into_iter()
            .find(|(key, _)| key.instance_id.as_str() == pod.uid().unwrap())
            .map(|(_, pvc_uid)| pvc_uid),
        allocation.pvc_uid.as_ref().map(ToString::to_string),
        "the simulated agent report must use the Pod's frozen environment UID"
    );
}

async fn authorized_allocation_without_candidate(
    api: &Arc<InMemoryClusterApi>,
) -> kuberic_runtime::protocol::types::ScaleUpAllocation {
    for _ in 0..20 {
        tick(api).await;
        let raw = api.observation().await;
        if let Some(allocation) = raw
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.scale_up_allocation.as_ref())
            .filter(|allocation| {
                allocation.scaffolding_requested
                    && allocation.pvc_uid.is_none()
                    && raw.pvcs.iter().all(|pvc| pvc.name_any() != "db-2-data")
            })
        {
            return allocation.clone();
        }
    }
    panic!("scaffolding authorization boundary not reached");
}

fn unrelated_candidate_pvc(raw: &RawObservation, uid: &str) -> PersistentVolumeClaim {
    let mut pvc = raw
        .pvcs
        .iter()
        .find(|pvc| pvc.name_any() == "db-1-data")
        .expect("accepted PVC")
        .clone();
    pvc.metadata.name = Some("db-2-data".into());
    pvc.metadata.uid = Some(uid.into());
    pvc.metadata.resource_version = Some(format!("{uid}-rv"));
    pvc.metadata
        .labels
        .get_or_insert_default()
        .insert(REPLICA_ID_LABEL.into(), "2".into());
    pvc.metadata
        .annotations
        .get_or_insert_default()
        .remove(SCALE_UP_ALLOCATION_ANNOTATION);
    pvc
}

#[tokio::test]
async fn authorized_allocation_never_adopts_or_deletes_unprovenanced_same_name_pvc() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(1, 2)));
    let allocation = authorized_allocation_without_candidate(&api).await;
    let unrelated_uid = "unrelated-candidate-pvc";
    let mut raw = api.observation().await;
    raw.pvcs.push(unrelated_candidate_pvc(&raw, unrelated_uid));
    api.set_observation(raw).await;
    let effects_start = api.effects().await.len();
    let mut frozen_unrelated = false;
    let mut pod_created = false;
    let mut unsafe_seen = false;

    for _ in 0..6 {
        let (kind, _) = tick(&api).await;
        unsafe_seen |= kind == ReconcileKind::Unsafe;
        let observed = api.observation().await;
        frozen_unrelated |= observed.set.status.as_ref().is_some_and(|status| {
            status
                .authority
                .scale_up_allocation
                .as_ref()
                .is_some_and(|active| {
                    active.pvc_uid.as_ref().map(PvcUid::as_str) == Some(unrelated_uid)
                })
        });
        pod_created |= observed.pods.iter().any(|pod| pod.name_any() == "db-2");
    }

    let mut reduced = api.observation().await;
    reduced.set.spec.replicas = 1;
    reduced.set.metadata.generation = Some(reduced.set.metadata.generation.unwrap_or_default() + 1);
    api.set_observation(reduced).await;
    for _ in 0..10 {
        let (kind, _) = tick(&api).await;
        unsafe_seen |= kind == ReconcileKind::Unsafe;
        let observed = api.observation().await;
        frozen_unrelated |= observed.set.status.as_ref().is_some_and(|status| {
            status
                .authority
                .scale_up_allocation
                .as_ref()
                .is_some_and(|active| {
                    active.pvc_uid.as_ref().map(PvcUid::as_str) == Some(unrelated_uid)
                })
        });
        pod_created |= observed.pods.iter().any(|pod| pod.name_any() == "db-2");
    }

    let blocked = api.observation().await;
    assert_eq!(allocation.operation_id, allocation.expected_operation_id());
    assert!(!unsafe_seen);
    assert!(!frozen_unrelated);
    assert!(!pod_created);
    assert!(blocked.pvcs.iter().any(|pvc| {
        pvc.name_any() == "db-2-data" && pvc.uid().as_deref() == Some(unrelated_uid)
    }));
    assert!(api.effects().await[effects_start..].iter().all(|effect| {
        !matches!(
            effect,
            EffectRecord::DeleteScaleDownResource {
                resource: ScaleDownResource::Pvc,
                uid,
                ..
            } if uid == unrelated_uid
        ) && !matches!(effect, EffectRecord::RemoveWriteRouting)
    }));
}

#[tokio::test]
async fn lost_reply_candidate_replaced_before_uid_freeze_is_abandoned_without_adoption() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(1, 2)));
    api.lose_next_create_reply().await;
    let (allocation, created_uid) = loop {
        let _ = tick(&api).await;
        let raw = api.observation().await;
        let Some(allocation) = raw
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.scale_up_allocation.as_ref())
            .filter(|allocation| allocation.scaffolding_requested && allocation.pvc_uid.is_none())
            .cloned()
        else {
            continue;
        };
        let Some(pvc) = raw.pvcs.iter().find(|pvc| pvc.name_any() == "db-2-data") else {
            continue;
        };
        let created_uid = pvc.uid().expect("created candidate PVC UID");
        assert_eq!(
            pvc.annotations()
                .get(SCALE_UP_ALLOCATION_ANNOTATION)
                .map(String::as_str),
            Some(allocation.operation_id.as_str()),
        );
        break (allocation, created_uid);
    };

    let replacement_uid = "replacement-before-freeze";
    let mut replaced = api.observation().await;
    let pvc = replaced
        .pvcs
        .iter_mut()
        .find(|pvc| pvc.name_any() == "db-2-data")
        .unwrap();
    pvc.metadata.uid = Some(replacement_uid.into());
    pvc.metadata.resource_version = Some("replacement-before-freeze-rv".into());
    pvc.metadata.annotations.get_or_insert_default().insert(
        SCALE_UP_ALLOCATION_ANNOTATION.into(),
        "different-allocation".into(),
    );
    api.set_observation(replaced).await;
    let effects_start = api.effects().await.len();
    let mut replacement_frozen = false;
    let mut old_attempt_abandoned = false;
    let mut fresh_attempt_while_occupied = false;

    for _ in 0..10 {
        let (kind, _) = tick(&api).await;
        assert_ne!(kind, ReconcileKind::Unsafe);
        let observed = api.observation().await;
        if let Some(active) = observed
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.scale_up_allocation.as_ref())
        {
            replacement_frozen |=
                active.pvc_uid.as_ref().map(PvcUid::as_str) == Some(replacement_uid);
            old_attempt_abandoned |=
                active.operation_id == allocation.operation_id && active.cancellation_started;
            fresh_attempt_while_occupied |= active.operation_id != allocation.operation_id;
        }
    }

    let occupied = api.observation().await;
    assert!(old_attempt_abandoned);
    assert!(!replacement_frozen);
    assert!(!fresh_attempt_while_occupied);
    assert!(occupied.pods.iter().all(|pod| pod.name_any() != "db-2"));
    assert!(occupied.pvcs.iter().any(|pvc| {
        pvc.name_any() == "db-2-data" && pvc.uid().as_deref() == Some(replacement_uid)
    }));
    assert!(api.effects().await[effects_start..].iter().all(|effect| {
        !matches!(
            effect,
            EffectRecord::DeleteScaleDownResource {
                resource: ScaleDownResource::Pvc,
                uid,
                ..
            } if uid == replacement_uid || uid == created_uid.as_str()
        ) && !matches!(effect, EffectRecord::RemoveWriteRouting)
    }));

    let mut available = occupied;
    available
        .pvcs
        .retain(|pvc| pvc.uid().as_deref() != Some(replacement_uid));
    api.set_observation(available).await;
    finish(&api, 2).await;
    let completed = api.observation().await;
    let pvc = completed
        .pvcs
        .iter()
        .find(|pvc| pvc.name_any() == "db-2-data")
        .expect("fresh candidate PVC");
    assert_ne!(pvc.uid().as_deref(), Some(created_uid.as_str()));
    assert_ne!(pvc.uid().as_deref(), Some(replacement_uid));
    let fresh_provenance = pvc
        .annotations()
        .get(SCALE_UP_ALLOCATION_ANNOTATION)
        .expect("fresh allocation provenance");
    assert_ne!(fresh_provenance.as_str(), allocation.operation_id.as_str());
}

#[tokio::test]
async fn matching_provenance_lost_reply_is_frozen_and_replayed_idempotently() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(1, 2)));
    api.lose_next_create_reply().await;
    let (allocation, pvc_uid) = loop {
        let _ = tick(&api).await;
        let raw = api.observation().await;
        let Some(allocation) = raw
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.scale_up_allocation.as_ref())
            .filter(|allocation| allocation.scaffolding_requested && allocation.pvc_uid.is_none())
            .cloned()
        else {
            continue;
        };
        let Some(pvc) = raw.pvcs.iter().find(|pvc| pvc.name_any() == "db-2-data") else {
            continue;
        };
        assert_eq!(
            pvc.annotations()
                .get(SCALE_UP_ALLOCATION_ANNOTATION)
                .map(String::as_str),
            Some(allocation.operation_id.as_str())
        );
        break (allocation, PvcUid::new(pvc.uid().unwrap()));
    };

    let mut frozen = false;
    for _ in 0..8 {
        let (kind, _) = tick(&api).await;
        assert_ne!(kind, ReconcileKind::Unsafe);
        frozen |= api
            .observation()
            .await
            .set
            .status
            .as_ref()
            .is_some_and(|status| {
                status
                    .authority
                    .scale_up_allocation
                    .as_ref()
                    .is_some_and(|active| {
                        active.operation_id == allocation.operation_id
                            && active.pvc_uid.as_ref() == Some(&pvc_uid)
                    })
            });
        if frozen
            && api
                .observation()
                .await
                .pods
                .iter()
                .any(|pod| pod.name_any() == "db-2")
        {
            break;
        }
    }
    assert!(frozen);
    finish(&api, 2).await;
    let completed = api.observation().await;
    assert!(completed.set.status.as_ref().is_some_and(|status| {
        status.authority.scale_up_allocation.is_none() && status.authority.last_scale_up.is_some()
    }));
}

#[tokio::test]
async fn controller_converges_sequential_scale_up_through_exact_agent_commands() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(1, 3)));
    let diagnostics = finish(&api, 3).await;
    let raw = api.observation().await;
    let status = &raw.set.status.as_ref().unwrap().authority;
    assert_eq!(
        status
            .topology
            .as_ref()
            .unwrap()
            .configuration
            .members
            .iter()
            .map(|member| member.identity.replica_id.value())
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(status.last_scale_up.is_some());
    assert!(status.scale_up_cleanup.is_none());
    for id in 2..=3 {
        assert!(
            raw.pvcs
                .iter()
                .any(|pvc| pvc.name_any() == format!("db-{id}-data"))
        );
        assert!(
            raw.pods
                .iter()
                .any(|pod| pod.name_any() == format!("db-{id}"))
        );
    }
    let effects = api.effects().await;
    let initialized = effects
        .iter()
        .filter(|effect| {
            matches!(
                effect,
                EffectRecord::Execute(ProtocolCommand::InitializeAgentStore(_))
            )
        })
        .count();
    let builds = effects
        .iter()
        .filter(|effect| {
            matches!(
                effect,
                EffectRecord::Execute(ProtocolCommand::EnsureReplicaBuild(_))
            )
        })
        .count();
    let configurations = effects
        .iter()
        .filter(|effect| {
            matches!(
                effect,
                EffectRecord::Execute(ProtocolCommand::EnsureConfiguration(command))
                    if command.scale_up_evidence.is_some()
            )
        })
        .count();
    assert_eq!(initialized, 2);
    assert_eq!(builds, 2);
    assert!(configurations >= 6);
    for (reason, message) in &diagnostics {
        for field in [
            "accepted=",
            "desired=",
            "target=",
            "attempt=",
            "phase=",
            "blocking=",
        ] {
            assert!(
                message.contains(field),
                "{reason} omitted {field}: {message}"
            );
        }
    }
    for reason in [
        "ScaleUpProvisioningAccepted",
        "ScaleUpCopying",
        "ScaleUpPreviousCurrentPersisted",
        "ScaleUpStable",
    ] {
        assert!(
            diagnostics.iter().any(|(candidate, _)| candidate == reason),
            "missing controller-visible phase {reason}: {diagnostics:?}"
        );
    }
}

#[tokio::test]
async fn scale_up_reobserves_status_conflict_and_lost_agent_replies() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(1, 2)));
    let mut conflicted = false;
    for _ in 0..20 {
        observe_fresh_candidate(&api).await;
        let raw = api.observe("tests", "db").await.unwrap();
        let plan = evaluate(&normalize(raw, BTreeMap::new()).unwrap(), &enabled());
        if matches!(
            plan,
            Plan::Apply { ref changes }
                if changes.iter().any(|change| {
                    matches!(
                        change,
                        KubernetesChange::PersistStatus { status }
                            if status.provisioning.is_some()
                    )
                })
        ) {
            api.conflict_next_status().await;
            let (kind, effects) = tick(&api).await;
            assert_eq!(kind, ReconcileKind::ObservationStale);
            assert!(
                effects
                    .iter()
                    .all(|effect| !matches!(effect, EffectRecord::Execute(_)))
            );
            conflicted = true;
            break;
        }
        tick(&api).await;
    }
    assert!(conflicted, "scale-up provisioning status was not reached");

    let mut lost_initialize = false;
    let mut lost_build = false;
    let mut lost_configuration = false;
    for _ in 0..200 {
        observe_fresh_candidate(&api).await;
        let raw = api.observe("tests", "db").await.unwrap();
        let plan = evaluate(&normalize(raw, BTreeMap::new()).unwrap(), &enabled());
        let inject = match &plan {
            Plan::Execute {
                command: ProtocolCommand::InitializeAgentStore(_),
            } if !lost_initialize => {
                lost_initialize = true;
                true
            }
            Plan::Execute {
                command: ProtocolCommand::EnsureReplicaBuild(_),
            } if !lost_build => {
                lost_build = true;
                true
            }
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(command),
            } if command.scale_up_evidence.is_some() && !lost_configuration => {
                lost_configuration = true;
                true
            }
            _ => false,
        };
        if inject {
            api.unavailable_next_execute().await;
        }
        let (kind, effects) = tick(&api).await;
        if inject {
            assert_eq!(kind, ReconcileKind::Waiting);
            assert!(
                effects
                    .iter()
                    .any(|effect| matches!(effect, EffectRecord::Execute(_)))
            );
        }
        if kind == ReconcileKind::Stable && accepted_count(&api.observation().await) == 2 {
            break;
        }
    }
    assert!(lost_initialize && lost_build && lost_configuration);
    finish(&api, 2).await;
}

#[tokio::test]
async fn scale_up_replays_pending_current_only_before_and_after_installation() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(2, 3)));
    let current_only = loop {
        observe_fresh_candidate(&api).await;
        let raw = api.observe("tests", "db").await.unwrap();
        match evaluate(&normalize(raw, BTreeMap::new()).unwrap(), &enabled()) {
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(command),
            } if command.current_only => break command,
            _ => {
                tick(&api).await;
            }
        }
    };
    let key = ReplicaObservationKey::new(
        current_only.local_replica_id,
        current_only.expected_instance_id.clone(),
    );
    let mut pending = api.observation().await;
    let RawAgentObservation::Report(report) = pending.agents.get_mut(&key).unwrap() else {
        panic!("current-only target report")
    };
    report.pending_operation_id = current_only.operation_id.to_string();
    report.pending_configuration = Some(kuberic_runtime::control::configuration_command_to_proto(
        (*current_only).clone(),
    ));
    report.report_sequence += 1;
    api.set_observation(pending).await;

    let (kind, effects) = tick(&api).await;
    assert_eq!(kind, ReconcileKind::Executed);
    assert!(effects.iter().any(|effect| {
        matches!(
            effect,
            EffectRecord::Execute(ProtocolCommand::EnsureConfiguration(replayed))
                if replayed == &current_only
        )
    }));

    let mut installed_pending = api.observation().await;
    let RawAgentObservation::Report(report) = installed_pending.agents.get_mut(&key).unwrap()
    else {
        panic!("installed current-only target report")
    };
    assert_eq!(
        report.current_configuration.as_ref(),
        Some(&current_only.current_configuration.clone().into())
    );
    assert!(report.previous_configuration.is_none());
    report.pending_operation_id = current_only.operation_id.to_string();
    report.pending_configuration = Some(kuberic_runtime::control::configuration_command_to_proto(
        (*current_only).clone(),
    ));
    report.report_sequence += 1;
    api.set_observation(installed_pending).await;

    let (kind, effects) = tick(&api).await;
    assert_eq!(kind, ReconcileKind::Executed);
    assert!(effects.iter().any(|effect| {
        matches!(
            effect,
            EffectRecord::Execute(ProtocolCommand::EnsureConfiguration(replayed))
                if replayed == &current_only
        )
    }));
    finish(&api, 3).await;
}

#[tokio::test]
async fn invalid_provisioning_candidate_retires_build_cleans_exact_resources_and_retries() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(1, 2)));
    let (target_key, old_pod_uid) = loop {
        let (_, effects) = tick(&api).await;
        if effects.iter().any(|effect| {
            matches!(
                effect,
                EffectRecord::Execute(ProtocolCommand::EnsureReplicaBuild(command))
                    if !command.retire
            )
        }) {
            let raw = api.observation().await;
            let (key, _) = candidate_key(&raw).expect("active provisioning candidate");
            let pod_uid = key.instance_id.to_string();
            assert!(
                raw.set
                    .status
                    .as_ref()
                    .is_some_and(|status| status.authority.provisioning.is_some())
            );
            break (key, pod_uid);
        }
    };
    let mut invalid = api.observation().await;
    invalid.agents.insert(
        target_key.clone(),
        RawAgentObservation::Invalid {
            message: "candidate durable storage is invalid".into(),
        },
    );
    let mut accepted_invalid = invalid.clone();
    let accepted_key = accepted_invalid
        .agents
        .keys()
        .find(|key| key.replica_id == ReplicaId::new(1))
        .unwrap()
        .clone();
    accepted_invalid.agents.insert(
        accepted_key,
        RawAgentObservation::Invalid {
            message: "accepted durable storage is invalid".into(),
        },
    );
    let accepted_snapshot = normalize(accepted_invalid, BTreeMap::new()).unwrap();
    assert!(matches!(
        evaluate(&accepted_snapshot, &enabled()),
        Plan::Unsafe {
            ref safety_changes,
            ..
        } if safety_changes == &[SafetyChange::RemoveWriteRouting]
    ));

    api.set_observation(invalid).await;
    let effects_start = api.effects().await.len();
    finish(&api, 2).await;
    let completed = api.observation().await;
    let accepted = completed
        .set
        .status
        .as_ref()
        .and_then(|status| status.authority.topology.as_ref())
        .unwrap();
    let fresh = accepted
        .configuration
        .members
        .iter()
        .find(|member| member.identity.replica_id == ReplicaId::new(2))
        .unwrap();
    assert_ne!(fresh.identity.instance_id.as_str(), old_pod_uid);
    let effects = &api.effects().await[effects_start..];
    assert!(
        effects
            .iter()
            .all(|effect| !matches!(effect, EffectRecord::RemoveWriteRouting))
    );
    let retired = effects
        .iter()
        .position(|effect| {
            matches!(
                effect,
                EffectRecord::Execute(ProtocolCommand::EnsureReplicaBuild(command))
                    if command.retire
            )
        })
        .expect("exact source build retirement");
    let endpoint = effects
        .iter()
        .position(|effect| {
            matches!(
                effect,
                EffectRecord::DeleteScaleDownResource {
                    resource: ScaleDownResource::Endpoint,
                    ..
                }
            )
        })
        .expect("candidate endpoint cleanup");
    let pod = effects
        .iter()
        .position(|effect| {
            matches!(
                effect,
                EffectRecord::DeleteScaleDownResource {
                    resource: ScaleDownResource::Pod,
                    uid,
                    ..
                } if uid == &old_pod_uid
            )
        })
        .expect("candidate Pod cleanup");
    let pvc = effects
        .iter()
        .position(|effect| {
            matches!(
                effect,
                EffectRecord::DeleteScaleDownResource {
                    resource: ScaleDownResource::Pvc,
                    ..
                }
            )
        })
        .expect("candidate PVC cleanup");
    assert!(retired < endpoint && endpoint < pod && pod < pvc);
}

#[tokio::test]
async fn invalid_candidate_is_globally_fenced_at_and_after_admission_start() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(2, 3)));
    let fenced = loop {
        tick(&api).await;
        let raw = api.observation().await;
        if raw
            .set
            .status
            .as_ref()
            .is_some_and(|status| status.authority.scale_up_admission_started.is_some())
        {
            break raw;
        }
    };
    let (candidate_key, _) = candidate_key(&fenced).expect("fenced candidate");
    assert!(
        fenced.agents.values().all(|observation| {
            !matches!(
                observation,
                RawAgentObservation::Report(report)
                    if report.previous_configuration.is_some()
            )
        }),
        "admission fence regression must stop before the first PC/CC command"
    );
    let mut invalid_at_fence = fenced.clone();
    invalid_at_fence.agents.insert(
        candidate_key.clone(),
        RawAgentObservation::Invalid {
            message: "candidate became invalid after admission fence".into(),
        },
    );
    assert!(matches!(
        evaluate(
            &normalize(invalid_at_fence, BTreeMap::new()).unwrap(),
            &enabled()
        ),
        Plan::Unsafe {
            ref safety_changes,
            ..
        } if safety_changes == &[SafetyChange::RemoveWriteRouting]
    ));

    let installed_api = Arc::new(InMemoryClusterApi::new(fenced));
    let installed = loop {
        tick(&installed_api).await;
        let raw = installed_api.observation().await;
        if raw.agents.values().any(|observation| {
            matches!(
                observation,
                RawAgentObservation::Report(report)
                    if report.previous_configuration.is_some()
            )
        }) {
            break raw;
        }
    };
    let mut invalid_after_install = installed;
    invalid_after_install
        .set
        .status
        .as_mut()
        .unwrap()
        .authority
        .scale_up_admission_started = None;
    invalid_after_install.agents.insert(
        candidate_key,
        RawAgentObservation::Invalid {
            message: "candidate became invalid after PC/CC install".into(),
        },
    );
    assert!(matches!(
        evaluate(
            &normalize(invalid_after_install, BTreeMap::new()).unwrap(),
            &enabled()
        ),
        Plan::Unsafe {
            ref safety_changes,
            ..
        } if safety_changes == &[SafetyChange::RemoveWriteRouting]
    ));
}

#[tokio::test]
async fn scale_up_carried_failover_repairs_returning_original_current_only_primary() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(2, 3)));
    let (intent, original_key) = loop {
        tick(&api).await;
        let raw = api.observation().await;
        let Some(intent) = raw
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.transition.as_ref())
            .and_then(|transition| transition.scale_up.as_deref())
            .cloned()
        else {
            continue;
        };
        let key = ReplicaObservationKey::new(
            intent.primary.replica_id,
            intent.primary.instance_id.clone(),
        );
        let installed = matches!(
            raw.agents.get(&key),
            Some(RawAgentObservation::Report(report))
                if report.previous_configuration.as_ref().is_some_and(|previous|
                    previous.configuration_id == intent.previous_configuration.configuration_id.as_str())
                    && report.current_configuration.as_ref().is_some_and(|current|
                        current.configuration_id == intent.current_configuration.configuration_id.as_str())
        );
        if installed {
            break (intent, key);
        }
    };

    let mut failed = api.observation().await;
    let RawAgentObservation::Report(report) = failed.agents.get_mut(&original_key).unwrap() else {
        panic!("original primary report")
    };
    report.healthy = false;
    report.reported_fault = proto::FaultType::Permanent as i32;
    report.write_status = proto::AccessStatus::ReconfigurationPending as i32;
    failed.now_unix_seconds += 11;
    api.set_observation(failed).await;
    for _ in 0..80 {
        let mut raw = api.observation().await;
        raw.now_unix_seconds += 11;
        api.set_observation(raw).await;
        tick(&api).await;
        if api
            .observation()
            .await
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.transition.as_ref())
            .is_some_and(|transition| transition.scale_up_failover.is_some())
        {
            break;
        }
    }
    let mut returned = api.observation().await;
    assert!(
        returned
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.transition.as_ref())
            .is_some_and(|transition| transition.scale_up_failover.is_some())
    );
    let RawAgentObservation::Report(report) = returned.agents.get_mut(&original_key).unwrap()
    else {
        panic!("returning original primary report")
    };
    report.healthy = true;
    report.reported_fault = proto::FaultType::Unknown as i32;
    report.role = proto::ReplicaRole::Primary as i32;
    report.read_status = proto::AccessStatus::Granted as i32;
    report.write_status = proto::AccessStatus::Granted as i32;
    report.epoch = Some(intent.current_configuration.epoch.into());
    report.previous_configuration = None;
    report.current_configuration = Some(intent.current_configuration.clone().into());
    report.scale_up_intent = Some(intent.clone().into());
    report.pending_operation_id.clear();
    report.retained_operation_id = intent
        .command_operation_id(
            kuberic_runtime::protocol::types::ScaleUpStage::CurrentOnly,
            &intent.primary,
            &intent.current_configuration,
        )
        .to_string();
    report.report_sequence += 1;
    api.set_observation(returned).await;

    let effects_start = api.effects().await.len();
    let (kind, effects) = tick(&api).await;
    assert_ne!(kind, ReconcileKind::Unsafe);
    assert!(
        effects
            .iter()
            .all(|effect| !matches!(effect, EffectRecord::RemoveWriteRouting))
    );
    assert!(effects.iter().any(|effect| {
        matches!(
            effect,
            EffectRecord::Execute(ProtocolCommand::EnsureConfiguration(command))
                if command.local_replica_id != intent.primary.replica_id
                    && command.transition_kind == TransitionKind::Failover
                    && !command.current_only
                    && command.current_epoch > intent.current_configuration.epoch
        )
    }));
    finish(&api, 3).await;
    assert!(api.effects().await[effects_start..].iter().any(|effect| {
        matches!(
            effect,
            EffectRecord::Execute(ProtocolCommand::EnsureConfiguration(command))
                if command.local_replica_id == intent.primary.replica_id
                    && command.transition_kind == TransitionKind::Failover
                    && command.current_epoch > intent.current_configuration.epoch
        )
    }));
}

#[tokio::test]
async fn scale_up_accepted_member_correction_replays_exact_pending_and_fences_unrelated() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(3, 4)));
    let starting = api
        .observation()
        .await
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
    while api
        .observation()
        .await
        .set
        .status
        .as_ref()
        .is_none_or(|status| status.authority.provisioning.is_none())
    {
        tick(&api).await;
    }
    let original = starting
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .unwrap()
        .identity
        .clone();
    let original_key =
        ReplicaObservationKey::new(original.replica_id, original.instance_id.clone());
    let mut failed = api.observation().await;
    let RawAgentObservation::Report(report) = failed.agents.get_mut(&original_key).unwrap() else {
        panic!("accepted primary report")
    };
    report.healthy = false;
    report.reported_fault = proto::FaultType::Permanent as i32;
    report.write_status = proto::AccessStatus::ReconfigurationPending as i32;
    failed.now_unix_seconds += 11;
    api.set_observation(failed).await;
    let accepted =
        loop {
            let mut raw = api.observation().await;
            raw.now_unix_seconds += 11;
            api.set_observation(raw).await;
            tick(&api).await;
            let raw = api.observation().await;
            let status = raw.set.status.as_ref().unwrap();
            if status.authority.transition.is_none()
                && status.authority.topology.as_ref().is_some_and(|topology| {
                    topology.configuration.primary_id != original.replica_id
                })
            {
                break status
                    .authority
                    .topology
                    .as_ref()
                    .unwrap()
                    .configuration
                    .clone();
            }
        };

    let correction_id = OperationId::new(format!(
        "scale-up-accepted-correction:{}:{}",
        accepted.configuration_id, original.replica_id
    ));
    for _ in 0..200 {
        if api
            .observation()
            .await
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.transition.as_ref())
            .is_some_and(|transition| transition.scale_up.is_some())
        {
            break;
        }
        tick(&api).await;
    }
    let mut stale = api.observation().await;
    assert!(
        stale
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.transition.as_ref())
            .is_some_and(|transition| transition.scale_up.is_some()),
        "fresh scale-up transition did not begin after accepted failover correction"
    );
    let RawAgentObservation::Report(report) = stale.agents.get_mut(&original_key).unwrap() else {
        panic!("late original member report")
    };
    report.healthy = true;
    report.reported_fault = proto::FaultType::Unknown as i32;
    report.role = proto::ReplicaRole::Primary as i32;
    report.read_status = proto::AccessStatus::Granted as i32;
    report.write_status = proto::AccessStatus::Granted as i32;
    report.epoch = Some(starting.epoch.into());
    report.previous_configuration = None;
    report.current_configuration = Some(starting.into());
    report.scale_up_intent = None;
    report.pending_operation_id.clear();
    report.pending_configuration = None;
    report.report_sequence += 1;

    let mut unrelated = stale.clone();
    let RawAgentObservation::Report(report) = unrelated.agents.get_mut(&original_key).unwrap()
    else {
        unreachable!()
    };
    report.pending_operation_id = "unrelated-accepted-correction".into();
    let unrelated_api = Arc::new(InMemoryClusterApi::new(unrelated));
    let mut unrelated_effects = Vec::new();
    for _ in 0..4 {
        let (_, effects) = tick(&unrelated_api).await;
        unrelated_effects.extend(effects);
        if unrelated_effects
            .iter()
            .any(|effect| matches!(effect, EffectRecord::RemoveWriteRouting))
        {
            break;
        }
    }
    assert!(
        unrelated_effects
            .iter()
            .any(|effect| matches!(effect, EffectRecord::RemoveWriteRouting)),
        "unrelated pending correction was not fenced: {unrelated_effects:?}"
    );
    assert!(
        unrelated_effects
            .iter()
            .all(|effect| !matches!(effect, EffectRecord::Execute(_))),
        "unrelated pending correction dispatched authority: {unrelated_effects:?}"
    );

    api.set_observation(stale).await;
    let mut correction = None;
    for _ in 0..8 {
        let (_, effects) = tick(&api).await;
        correction = effects.iter().find_map(|effect| match effect {
            EffectRecord::Execute(ProtocolCommand::EnsureConfiguration(command))
                if command.operation_id == correction_id =>
            {
                Some(command.clone())
            }
            _ => None,
        });
        if correction.is_some() {
            break;
        }
    }
    let correction = correction.expect("exact pending accepted-member correction replay");
    assert_eq!(correction.current_configuration, accepted);
    assert!(!correction.current_only);
    assert_eq!(correction.failover_safe_lsn, Some(0));

    let mut installed_pc_cc = api.observation().await;
    let RawAgentObservation::Report(report) =
        installed_pc_cc.agents.get_mut(&original_key).unwrap()
    else {
        unreachable!()
    };
    report.current_progress += 1;
    report.pending_operation_id = correction.operation_id.to_string();
    report.pending_configuration = Some(kuberic_runtime::control::configuration_command_to_proto(
        (*correction).clone(),
    ));
    report.report_sequence += 1;
    api.set_observation(installed_pc_cc).await;
    let (kind, effects) = tick(&api).await;
    assert_eq!(kind, ReconcileKind::Executed);
    assert!(effects.iter().any(|effect| {
        matches!(
            effect,
            EffectRecord::Execute(ProtocolCommand::EnsureConfiguration(replayed))
                if replayed == &correction
        )
    }));

    let current_only = loop {
        let raw = api.observe("tests", "db").await.unwrap();
        match evaluate(&normalize(raw, BTreeMap::new()).unwrap(), &enabled()) {
            Plan::Execute {
                command: ProtocolCommand::EnsureConfiguration(command),
            } if command.current_only && command.local_replica_id == original.replica_id => {
                break command;
            }
            _ => {
                tick(&api).await;
            }
        }
    };
    let mut pending = api.observation().await;
    let RawAgentObservation::Report(report) = pending.agents.get_mut(&original_key).unwrap() else {
        unreachable!()
    };
    report.pending_operation_id = current_only.operation_id.to_string();
    report.pending_configuration = Some(kuberic_runtime::control::configuration_command_to_proto(
        (*current_only).clone(),
    ));
    report.current_progress += 1;
    report.report_sequence += 1;
    api.set_observation(pending).await;
    let (kind, effects) = tick(&api).await;
    assert_eq!(kind, ReconcileKind::Executed);
    assert!(effects.iter().any(|effect| {
        matches!(
            effect,
            EffectRecord::Execute(ProtocolCommand::EnsureConfiguration(replayed))
                if replayed == &current_only
        )
    }));
    finish(&api, 4).await;
}

#[tokio::test]
async fn scale_up_receipt_allows_replacement_then_failover_to_return_ready() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(2, 3)));
    finish(&api, 3).await;
    let completed = api.observation().await;
    let receipt = completed
        .set
        .status
        .as_ref()
        .and_then(|status| status.authority.last_scale_up.as_ref())
        .expect("completed scale-up receipt")
        .clone();
    let topology = &completed
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .topology
        .as_ref()
        .unwrap()
        .configuration;
    let primary = topology
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .unwrap()
        .identity
        .clone();
    let replaced = topology
        .members
        .iter()
        .find(|member| member.identity != primary)
        .unwrap()
        .identity
        .clone();
    let replaced_key =
        ReplicaObservationKey::new(replaced.replica_id, replaced.instance_id.clone());
    let RawAgentObservation::Report(report) = completed.agents.get(&replaced_key).unwrap() else {
        panic!("accepted secondary report")
    };
    let mut superseded_report = report.clone();
    let pod_name = completed
        .pods
        .iter()
        .find(|pod| pod.uid().as_deref() == Some(replaced.instance_id.as_str()))
        .map(ResourceExt::name_any)
        .expect("accepted secondary Pod");
    api.delete_exact_pod(
        &completed,
        &pod_name,
        &PodUid::new(replaced.instance_id.as_str()),
    )
    .await
    .unwrap();
    finish(&api, 3).await;
    let replacement_identity = api
        .observation()
        .await
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .members
        .iter()
        .find(|member| member.identity.replica_id == replaced.replica_id)
        .unwrap()
        .identity
        .clone();
    assert_ne!(replacement_identity, replaced);

    let mut failover_observed = api.observation().await;
    let authority = &mut failover_observed.set.status.as_mut().unwrap().authority;
    let replacement_configuration = authority.topology.as_ref().unwrap().configuration.clone();
    let current_primary = replacement_configuration
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .unwrap()
        .identity
        .clone();
    let failover_primary = replacement_configuration
        .members
        .iter()
        .find(|member| member.identity != current_primary)
        .unwrap()
        .identity
        .clone();
    let failover_configuration = ConfigurationDescriptor::new(
        Epoch::new(
            replacement_configuration.epoch.data_loss_number,
            replacement_configuration.epoch.configuration_number + 1,
        ),
        failover_primary.replica_id,
        replacement_configuration
            .members
            .iter()
            .map(|member| ConfigurationMember {
                identity: member.identity.clone(),
                role: if member.identity == failover_primary {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        replacement_configuration.write_quorum,
    );
    authority.topology = Some(AcceptedTopology {
        configuration: failover_configuration.clone(),
    });
    authority.primary_failure = None;
    authority.transition = None;
    authority.conditions.clear();
    for member in &failover_configuration.members {
        let key = ReplicaObservationKey::new(
            member.identity.replica_id,
            member.identity.instance_id.clone(),
        );
        let RawAgentObservation::Report(report) = failover_observed.agents.get_mut(&key).unwrap()
        else {
            panic!("accepted post-failover report")
        };
        report.role = match member.role {
            ReplicaRole::Primary => proto::ReplicaRole::Primary as i32,
            ReplicaRole::ActiveSecondary => proto::ReplicaRole::ActiveSecondary as i32,
            ReplicaRole::IdleSecondary => proto::ReplicaRole::IdleSecondary as i32,
            ReplicaRole::None => proto::ReplicaRole::None as i32,
        };
        report.epoch = Some(failover_configuration.epoch.into());
        report.previous_configuration = None;
        report.current_configuration = Some(failover_configuration.clone().into());
        report.scale_up_intent = None;
        report.pending_operation_id.clear();
        report.pending_configuration = None;
        report.read_status = proto::AccessStatus::Granted as i32;
        report.write_status = if member.role == ReplicaRole::Primary {
            proto::AccessStatus::Granted as i32
        } else {
            proto::AccessStatus::NotPrimary as i32
        };
        report.healthy = true;
        report.reported_fault = proto::FaultType::Unknown as i32;
        report.report_sequence += 1;
    }
    let mut failed_former_primary = failover_observed.clone();
    let current_primary_key = ReplicaObservationKey::new(
        current_primary.replica_id,
        current_primary.instance_id.clone(),
    );
    failed_former_primary
        .agents
        .insert(current_primary_key, RawAgentObservation::Absent);
    let failed_former_plan = evaluate(
        &normalize(failed_former_primary, BTreeMap::new()).unwrap(),
        &enabled(),
    );
    assert!(
        !matches!(&failed_former_plan, Plan::Wait { status, .. }
            if status.conditions.iter().any(|condition|
                condition.reason == "ScaleUpCommittedDegraded")),
        "accepted failover former primary blocked replacement: {failed_former_plan:?}"
    );
    superseded_report.role = proto::ReplicaRole::ActiveSecondary as i32;
    superseded_report.write_status = proto::AccessStatus::NotPrimary as i32;
    superseded_report.healthy = false;
    superseded_report.reported_fault = proto::FaultType::Permanent as i32;
    superseded_report.pending_operation_id.clear();
    superseded_report.pending_configuration = None;
    failover_observed.agents.insert(
        ReplicaObservationKey::new(replaced.replica_id, replaced.instance_id.clone()),
        RawAgentObservation::Report(superseded_report),
    );
    if let Some(service) = failover_observed
        .services
        .iter_mut()
        .find(|service| service.name_any().ends_with("-write"))
    {
        service.spec.get_or_insert_default().selector = Some(BTreeMap::from([(
            INSTANCE_LABEL.to_string(),
            failover_primary.instance_id.to_string(),
        )]));
    }
    api.set_observation(failover_observed).await;
    let mut final_status = None;
    for _ in 0..20 {
        let (kind, _) = tick(&api).await;
        let observed = api.observation().await;
        let authority = &observed.set.status.as_ref().unwrap().authority;
        assert!(
            authority
                .conditions
                .iter()
                .all(|condition| { condition.reason != "ScaleUpCommittedDegraded" }),
            "historical receipt blocked ordinary failover/replacement: {authority:?}"
        );
        if kind == ReconcileKind::Stable
            && authority.topology.as_ref().is_some_and(|topology| {
                topology.configuration.primary_id == failover_primary.replica_id
            })
        {
            final_status = Some(observed);
            break;
        }
    }
    let final_status = final_status.expect("ordinary failover/replacement returned to Stable");
    let authority = &final_status.set.status.as_ref().unwrap().authority;
    assert_eq!(authority.last_scale_up.as_ref(), Some(&receipt));
    assert_eq!(
        normalize(final_status.clone(), BTreeMap::new())
            .unwrap()
            .routing
            .write_target
            .as_ref()
            .map(|identity| identity.replica_id),
        authority
            .topology
            .as_ref()
            .map(|topology| topology.configuration.primary_id)
    );
}

#[tokio::test]
async fn scale_up_replacement_then_ordinary_failover_pc_cc_converges_with_receipt() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(2, 3)));
    finish(&api, 3).await;
    let completed = api.observation().await;
    let receipt = completed
        .set
        .status
        .as_ref()
        .and_then(|status| status.authority.last_scale_up.as_ref())
        .expect("completed scale-up receipt")
        .clone();
    let topology = &completed
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .topology
        .as_ref()
        .unwrap()
        .configuration;
    let primary = topology
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .unwrap()
        .identity
        .clone();
    let historical = topology
        .members
        .iter()
        .find(|member| member.identity != primary)
        .unwrap()
        .identity
        .clone();
    let pod_name = completed
        .pods
        .iter()
        .find(|pod| pod.uid().as_deref() == Some(historical.instance_id.as_str()))
        .map(ResourceExt::name_any)
        .expect("historical accepted secondary Pod");
    api.delete_exact_pod(
        &completed,
        &pod_name,
        &PodUid::new(historical.instance_id.as_str()),
    )
    .await
    .unwrap();
    finish(&api, 3).await;
    let replacement = api.observation().await;
    let replacement_configuration = replacement
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
    assert!(
        replacement_configuration
            .members
            .iter()
            .all(|member| member.identity != historical)
    );

    let failed_primary = replacement_configuration
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .unwrap()
        .identity
        .clone();
    let failed_key = ReplicaObservationKey::new(
        failed_primary.replica_id,
        failed_primary.instance_id.clone(),
    );
    let mut failed = replacement;
    let RawAgentObservation::Report(report) = failed.agents.get_mut(&failed_key).unwrap() else {
        panic!("accepted primary report")
    };
    report.healthy = false;
    report.reported_fault = proto::FaultType::Permanent as i32;
    report.write_status = proto::AccessStatus::ReconfigurationPending as i32;
    report.report_sequence += 1;
    failed.now_unix_seconds += 11;
    api.set_observation(failed).await;

    let mut saw_pc_cc = false;
    let mut converged = None;
    for _ in 0..160 {
        let mut advancing = api.observation().await;
        advancing.now_unix_seconds += 1;
        api.set_observation(advancing).await;
        let (_kind, effects) = tick(&api).await;
        saw_pc_cc |= effects.iter().any(|effect| {
            matches!(
                effect,
                EffectRecord::Execute(ProtocolCommand::EnsureConfiguration(command))
                    if command.transition_kind == TransitionKind::Failover
                        && !command.current_only
            )
        });
        let observed = api.observation().await;
        let authority = &observed.set.status.as_ref().unwrap().authority;
        assert_eq!(authority.last_scale_up.as_ref(), Some(&receipt));
        assert!(
            authority
                .conditions
                .iter()
                .all(|condition| condition.reason != "ScaleUpCommittedDegraded"),
            "superseded historical receipt consumer blocked ordinary failover: {authority:?}"
        );
        if saw_pc_cc
            && authority.transition.is_none()
            && authority.topology.as_ref().is_some_and(|topology| {
                topology.configuration.primary_id != failed_primary.replica_id
            })
        {
            converged = Some(observed);
            break;
        }
    }
    assert!(saw_pc_cc, "ordinary failover never installed PC/CC");
    let converged = converged.expect("ordinary failover transition did not converge");
    assert_eq!(
        converged
            .set
            .status
            .as_ref()
            .unwrap()
            .authority
            .last_scale_up
            .as_ref(),
        Some(&receipt)
    );
}

#[tokio::test]
async fn stable_scale_up_then_planned_switchover_converges_with_receipt() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(2, 3)));
    finish(&api, 3).await;
    let completed = api.observation().await;
    let receipt = completed
        .set
        .status
        .as_ref()
        .and_then(|status| status.authority.last_scale_up.as_ref())
        .expect("completed scale-up receipt")
        .clone();
    let topology = &completed
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .topology
        .as_ref()
        .unwrap()
        .configuration;
    let target = topology
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::ActiveSecondary)
        .unwrap()
        .identity
        .replica_id;

    let mut requested = completed;
    requested.set.spec.switchover = Some(PlannedSwitchoverRequestSpec {
        request_id: "post-scale-up-switchover".into(),
        target_replica_id: u32::try_from(target.value()).unwrap(),
    });
    requested.set.metadata.generation =
        Some(requested.set.metadata.generation.unwrap_or_default() + 1);
    api.set_observation(requested).await;
    let mut switchover_complete = false;
    for step in 0..200 {
        let snapshot = normalize(api.observation().await, BTreeMap::new()).unwrap();
        assert!(
            kuberic_runtime::protocol::validation::validate_snapshot(&snapshot).is_ok(),
            "post-scale-up switchover snapshot invalid at step {step}: {:?}",
            kuberic_runtime::protocol::validation::validate_snapshot(&snapshot)
        );
        let invalid = snapshot
            .replicas
            .iter()
            .filter_map(|(key, observation)| match &observation.agent {
                AgentObservation::Invalid { message, .. } => Some((key.clone(), message.clone())),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            invalid.is_empty(),
            "post-scale-up switchover produced invalid agent evidence at step {step}: {invalid:?}"
        );
        let plan = evaluate(&snapshot, &enabled());
        assert!(
            !matches!(plan, Plan::Unsafe { .. }),
            "post-scale-up switchover became unsafe at step {step}: invalid={invalid:?} \
             plan={plan:?}"
        );
        tick(&api).await;
        let observed = api.observation().await;
        let status = observed.set.status.as_ref().unwrap();
        if status
            .authority
            .topology
            .as_ref()
            .is_some_and(|topology| topology.configuration.primary_id == target)
            && status.authority.transition.is_none()
            && status.authority.last_switchover.is_some()
        {
            switchover_complete = true;
            break;
        }
    }
    assert!(
        switchover_complete,
        "post-scale-up switchover did not converge: {:?}",
        api.observation().await.set.status
    );

    let completed = api.observation().await;
    let authority = &completed.set.status.as_ref().unwrap().authority;
    assert_eq!(authority.last_scale_up.as_ref(), Some(&receipt));
    assert!(authority.transition.is_none());
    assert!(authority.last_switchover.is_some());
    assert_eq!(
        authority
            .topology
            .as_ref()
            .unwrap()
            .configuration
            .primary_id,
        target
    );
}

#[tokio::test]
async fn scale_up_stale_process_session_cannot_advance_and_fresh_session_recovers() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(1, 2)));
    let mut stale_key = None;
    for _ in 0..100 {
        tick(&api).await;
        let raw = api.observation().await;
        if let Some((key, _)) = candidate_key(&raw)
            && matches!(raw.agents.get(&key), Some(RawAgentObservation::Report(report))
                if report.storage_state == proto::AgentStorageState::Initialized as i32)
        {
            stale_key = Some(key);
            break;
        }
    }
    let stale_key = stale_key.expect("initialized scale-up candidate");
    let reconciler = Reconciler::new(api.clone(), enabled());

    // First accept the current sequence into the reconciler's durable watermark.
    reconciler.reconcile("tests", "db").await.unwrap();
    let mut stale = api.observation().await;
    let RawAgentObservation::Report(report) = stale.agents.get_mut(&stale_key).unwrap() else {
        panic!("initialized scale-up candidate report")
    };
    let accepted_sequence = report.report_sequence;
    let stale_session = report.process_session_id.clone();
    report.report_sequence = accepted_sequence.saturating_sub(1);
    api.set_observation(stale).await;
    let effects_before = api.effects().await.len();
    assert_eq!(
        reconciler.reconcile("tests", "db").await.unwrap().kind,
        ReconcileKind::Unsafe
    );
    assert!(
        api.effects().await[effects_before..]
            .iter()
            .all(|effect| !matches!(effect, EffectRecord::Execute(_))),
        "a stale same-session report dispatched scale-up authority"
    );

    let mut restarted = api.observation().await;
    for (key, observation) in &mut restarted.agents {
        if let RawAgentObservation::Report(report) = observation {
            report.process_session_id = if key == &stale_key {
                format!("{stale_session}-restart")
            } else {
                format!("fresh-session-{}", key.replica_id)
            };
            report.report_sequence = 1;
        }
    }
    api.set_observation(restarted).await;
    let resumed = reconciler.reconcile("tests", "db").await.unwrap();
    assert_ne!(resumed.kind, ReconcileKind::Unsafe);
    finish(&api, 2).await;
    let completed = api.observation().await;
    assert_eq!(accepted_count(&completed), 2);
    assert!(
        completed
            .set
            .status
            .as_ref()
            .unwrap()
            .authority
            .last_scale_up
            .is_some()
    );
}

#[tokio::test]
async fn cancelled_candidate_cleanup_is_exact_ordered_and_restart_safe() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(1, 2)));
    drive_until_cleanup(&api).await;
    let before_cleanup = api.observation().await;
    let old_pod_uid = before_cleanup
        .pods
        .iter()
        .find(|pod| pod.name_any() == "db-2")
        .and_then(ResourceExt::uid)
        .unwrap();
    let old_pvc_uid = before_cleanup
        .pvcs
        .iter()
        .find(|pvc| pvc.name_any() == "db-2-data")
        .and_then(ResourceExt::uid)
        .unwrap();
    api.lose_next_delete_reply().await;
    let diagnostics = finish(&api, 1).await;
    let raw = api.observation().await;
    assert!(raw.pods.iter().all(|pod| pod.name_any() != "db-2"));
    assert!(raw.pvcs.iter().all(|pvc| pvc.name_any() != "db-2-data"));
    let deleted = api
        .effects()
        .await
        .into_iter()
        .filter_map(|effect| match effect {
            EffectRecord::DeleteScaleDownResource { resource, .. } => Some(resource),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        deleted,
        vec![
            ScaleDownResource::Endpoint,
            ScaleDownResource::Pod,
            ScaleDownResource::Pvc
        ]
    );
    assert!(
        diagnostics
            .iter()
            .any(|(reason, _)| reason == "ScaleUpCleanupComplete")
    );

    let mut retry = api.observation().await;
    retry.set.spec.replicas = 2;
    retry.set.metadata.generation = Some(retry.set.metadata.generation.unwrap_or_default() + 1);
    api.set_observation(retry).await;
    finish(&api, 2).await;
    let retried = api.observation().await;
    assert_ne!(
        retried
            .pods
            .iter()
            .find(|pod| pod.name_any() == "db-2")
            .and_then(ResourceExt::uid)
            .unwrap(),
        old_pod_uid
    );
    assert_ne!(
        retried
            .pvcs
            .iter()
            .find(|pvc| pvc.name_any() == "db-2-data")
            .and_then(ResourceExt::uid)
            .unwrap(),
        old_pvc_uid
    );
}

#[tokio::test]
async fn failed_provisioning_cleanup_seeds_bounded_fresh_allocation_lineage() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(1, 2)));
    let (old_allocation_operation_id, provisioning_operation_id, old_pvc) = loop {
        tick(&api).await;
        let raw = api.observation().await;
        let Some(provisioning) = raw
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.provisioning.as_ref())
            .filter(|provisioning| provisioning.scale_up().is_some())
        else {
            continue;
        };
        let target = provisioning.target_identity(&ResourceUid::new(UID));
        let key = ReplicaObservationKey::new(target.replica_id, target.instance_id);
        let Some(RawAgentObservation::Report(report)) = raw.agents.get(&key) else {
            continue;
        };
        if report.storage_state != proto::AgentStorageState::Initialized as i32 {
            continue;
        }
        let pvc = raw
            .pvcs
            .iter()
            .find(|pvc| pvc.name_any() == "db-2-data")
            .unwrap()
            .clone();
        let allocation_operation_id = OperationId::new(
            pvc.annotations()
                .get(SCALE_UP_ALLOCATION_ANNOTATION)
                .expect("candidate PVC allocation provenance"),
        );
        break (
            allocation_operation_id,
            provisioning.operation_id.clone(),
            pvc,
        );
    };

    let mut failed = api.observation().await;
    let target = failed
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .provisioning
        .as_ref()
        .unwrap()
        .target_identity(&ResourceUid::new(UID));
    let key = ReplicaObservationKey::new(target.replica_id, target.instance_id.clone());
    let RawAgentObservation::Report(report) = failed.agents.get_mut(&key).unwrap() else {
        panic!("provisioning candidate report");
    };
    report.reported_fault = proto::FaultType::Permanent as i32;
    report.healthy = false;
    api.set_observation(failed).await;

    let cleanup_effects_start = api.effects().await.len();
    let fresh_allocation = loop {
        let (kind, effects) = tick(&api).await;
        let raw = api.observation().await;
        assert_ne!(
            kind,
            ReconcileKind::Unsafe,
            "{:?}",
            raw.set
                .status
                .as_ref()
                .map(|status| &status.authority.conditions)
        );
        assert!(
            effects
                .iter()
                .all(|effect| !matches!(effect, EffectRecord::RemoveWriteRouting))
        );
        if let Some(allocation) = raw
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.scale_up_allocation.as_ref())
            .filter(|allocation| {
                allocation.scaffolding_requested
                    && allocation.pvc_uid.is_none()
                    && raw.pvcs.iter().all(|pvc| pvc.name_any() != "db-2-data")
            })
        {
            break allocation.clone();
        }
    };
    assert_ne!(fresh_allocation.operation_id, old_allocation_operation_id);
    assert_eq!(
        fresh_allocation.previous_operation_id.as_ref(),
        Some(&provisioning_operation_id)
    );
    let cleanup_effects = api.effects().await;
    assert!(
        cleanup_effects[cleanup_effects_start..]
            .iter()
            .any(|effect| {
                matches!(
                    effect,
                    EffectRecord::DeleteScaleDownResource {
                        resource: ScaleDownResource::Pvc,
                        uid,
                        ..
                    } if uid == old_pvc.uid().as_deref().unwrap()
                )
            })
    );

    let mut occupied = api.observation().await;
    occupied.pvcs.push(old_pvc.clone());
    api.set_observation(occupied).await;
    let occupant_effects_start = api.effects().await.len();
    for _ in 0..10 {
        let (kind, effects) = tick(&api).await;
        assert_ne!(kind, ReconcileKind::Unsafe);
        assert!(
            effects
                .iter()
                .all(|effect| !matches!(effect, EffectRecord::RemoveWriteRouting))
        );
    }
    let blocked = api.observation().await;
    assert!(
        blocked
            .pvcs
            .iter()
            .any(|pvc| { pvc.name_any() == "db-2-data" && pvc.uid() == old_pvc.uid() })
    );
    assert!(blocked.set.status.as_ref().is_some_and(|status| {
        status
            .authority
            .scale_up_allocation
            .as_ref()
            .is_none_or(|allocation| {
                allocation.pvc_uid.as_ref().map(PvcUid::as_str) != old_pvc.uid().as_deref()
            })
    }));
    assert!(
        api.effects().await[occupant_effects_start..]
            .iter()
            .all(|effect| {
                !matches!(
                    effect,
                    EffectRecord::DeleteScaleDownResource {
                        resource: ScaleDownResource::Pvc,
                        uid,
                        ..
                    } if Some(uid.as_str()) == old_pvc.uid().as_deref()
                ) && !matches!(effect, EffectRecord::RemoveWriteRouting)
            })
    );

    let old_uid = old_pvc.uid().unwrap();
    let mut available = blocked;
    available
        .pvcs
        .retain(|pvc| pvc.uid().as_deref() != Some(old_uid.as_str()));
    api.set_observation(available).await;
    finish(&api, 2).await;
    let completed = api.observation().await;
    let receipt = completed
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .last_scale_up
        .as_deref()
        .unwrap();
    assert_ne!(
        receipt.intent.target.instance_id.as_str(),
        target.instance_id.as_str()
    );
    assert!(
        completed
            .pvcs
            .iter()
            .all(|pvc| { pvc.uid().as_deref() != Some(old_uid.as_str()) })
    );
}

#[tokio::test]
async fn cleanup_preserves_same_name_replacements_and_waits_on_lookup_failure() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(1, 2)));
    drive_until_cleanup(&api).await;
    let cleanup = api
        .observation()
        .await
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .scale_up_cleanup
        .as_deref()
        .unwrap()
        .clone();
    let endpoint_name = match &cleanup.resources.endpoint {
        kuberic_runtime::protocol::types::CleanupResourceIdentity::Present { name, .. }
        | kuberic_runtime::protocol::types::CleanupResourceIdentity::Absent { name } => {
            name.clone()
        }
    };
    api.fail_exact_lookup(
        format!("Service/{endpoint_name}"),
        Some("temporary lookup failure".into()),
    )
    .await;
    let before = api.effects().await.len();
    tick(&api).await;
    assert!(
        api.effects().await[before..]
            .iter()
            .all(|effect| !matches!(effect, EffectRecord::DeleteScaleDownResource { .. }))
    );
    api.fail_exact_lookup(format!("Service/{endpoint_name}"), None)
        .await;

    let mut raw = api.observation().await;
    for pod in raw.pods.iter_mut().filter(|pod| pod.name_any() == "db-2") {
        pod.metadata.uid = Some("replacement-pod".into());
        pod.metadata.resource_version = Some("replacement-rv-pod".into());
    }
    for pvc in raw
        .pvcs
        .iter_mut()
        .filter(|pvc| pvc.name_any() == "db-2-data")
    {
        pvc.metadata.uid = Some("replacement-pvc".into());
        pvc.metadata.resource_version = Some("replacement-rv-pvc".into());
    }
    for service in raw
        .services
        .iter_mut()
        .filter(|service| service.name_any() == endpoint_name)
    {
        service.metadata.uid = Some("replacement-endpoint".into());
        service.metadata.resource_version = Some("replacement-rv-endpoint".into());
    }
    api.set_observation(raw).await;
    finish(&api, 1).await;
    let preserved = api.observation().await;
    assert!(
        preserved
            .pods
            .iter()
            .any(|pod| pod.uid().as_deref() == Some("replacement-pod"))
    );
    assert!(
        preserved
            .pvcs
            .iter()
            .any(|pvc| pvc.uid().as_deref() == Some("replacement-pvc"))
    );
    assert!(
        preserved
            .services
            .iter()
            .any(|service| { service.uid().as_deref() == Some("replacement-endpoint") })
    );
}

#[tokio::test]
async fn terminating_candidate_blocks_pvc_cleanup_until_exact_pod_absence() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(1, 2)));
    drive_until_cleanup(&api).await;
    let mut raw = api.observation().await;
    raw.pods
        .iter_mut()
        .find(|pod| pod.name_any() == "db-2")
        .unwrap()
        .metadata
        .finalizers = Some(vec!["tests/finalizer".into()]);
    api.set_observation(raw).await;

    let mut saw_blocked_pod_delete = false;
    for _ in 0..10 {
        let (_, effects) = tick(&api).await;
        if effects.iter().any(|effect| {
            matches!(
                effect,
                EffectRecord::DeleteScaleDownResource {
                    resource: ScaleDownResource::Pod,
                    ..
                }
            )
        }) {
            saw_blocked_pod_delete = true;
            break;
        }
    }
    assert!(saw_blocked_pod_delete);
    let observed = api.observation().await;
    assert!(observed.pods.iter().any(|pod| pod.name_any() == "db-2"));
    assert!(
        observed
            .pvcs
            .iter()
            .any(|pvc| pvc.name_any() == "db-2-data")
    );
    assert!(
        observed
            .set
            .status
            .as_ref()
            .unwrap()
            .authority
            .scale_up_cleanup
            .is_some()
    );
    let (_, repeated) = tick(&api).await;
    assert!(repeated.iter().any(|effect| {
        matches!(
            effect,
            EffectRecord::DeleteScaleDownResource {
                resource: ScaleDownResource::Pod,
                ..
            }
        )
    }));
    assert!(repeated.iter().all(|effect| {
        !matches!(
            effect,
            EffectRecord::DeleteScaleDownResource {
                resource: ScaleDownResource::Pvc,
                ..
            }
        )
    }));
    let mut raw = api.observation().await;
    raw.pods
        .iter_mut()
        .find(|pod| pod.name_any() == "db-2")
        .unwrap()
        .metadata
        .finalizers = None;
    api.set_observation(raw).await;
    finish(&api, 1).await;
    let completed = api.observation().await;
    assert!(completed.pods.iter().all(|pod| pod.name_any() != "db-2"));
    assert!(
        completed
            .pvcs
            .iter()
            .all(|pvc| pvc.name_any() != "db-2-data")
    );
}

#[tokio::test]
async fn stale_cleanup_resource_version_is_reobserved_without_broad_deletion() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(1, 2)));
    drive_until_cleanup(&api).await;
    let raw = api.observe("tests", "db").await.unwrap();
    let snapshot = normalize(raw.clone(), BTreeMap::new()).unwrap();
    let Plan::Apply { changes } = evaluate(&snapshot, &enabled()) else {
        panic!("expected exact cleanup delete")
    };
    let KubernetesChange::DeleteScaleDownResource {
        resource,
        name,
        uid,
        resource_version,
    } = changes[0].clone()
    else {
        panic!("expected exact cleanup delete")
    };
    let mut raced = api.observation().await;
    let object = match resource {
        ScaleDownResource::Endpoint => raced
            .services
            .iter_mut()
            .find(|object| object.name_any() == name)
            .unwrap()
            .meta_mut(),
        ScaleDownResource::Pod => raced
            .pods
            .iter_mut()
            .find(|object| object.name_any() == name)
            .unwrap()
            .meta_mut(),
        ScaleDownResource::Pvc => raced
            .pvcs
            .iter_mut()
            .find(|object| object.name_any() == name)
            .unwrap()
            .meta_mut(),
    };
    object.resource_version = Some("raced-resource-version".into());
    api.set_observation(raced).await;
    assert_eq!(
        api.delete_scale_down_resource(&raw, resource, &name, &uid, &resource_version)
            .await,
        Err(ControllerError::ObservationStale)
    );
    assert!(api.observation().await.services.iter().any(|service| {
        service.name_any() == name && service.uid().as_deref() == Some(uid.as_str())
    }));
}

#[tokio::test]
async fn pending_candidate_cleanup_allows_primary_failover_but_blocks_retry() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(2, 3)));
    drive_until_cleanup(&api).await;
    let cleanup = api
        .observation()
        .await
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .scale_up_cleanup
        .as_deref()
        .unwrap()
        .clone();
    let endpoint_name = match cleanup.resources.endpoint {
        kuberic_runtime::protocol::types::CleanupResourceIdentity::Present { name, .. }
        | kuberic_runtime::protocol::types::CleanupResourceIdentity::Absent { name } => name,
    };
    api.fail_exact_lookup(
        format!("Service/{endpoint_name}"),
        Some("candidate endpoint lookup unavailable".into()),
    )
    .await;
    let before = api.effects().await.len();
    let mut raw = api.observation().await;
    let primary = raw
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .unwrap()
        .identity
        .clone();
    let RawAgentObservation::Report(report) = raw
        .agents
        .get_mut(&ReplicaObservationKey::new(
            primary.replica_id,
            primary.instance_id,
        ))
        .unwrap()
    else {
        panic!("primary report")
    };
    report.reported_fault = proto::FaultType::Permanent as i32;
    report.healthy = false;
    raw.set.spec.replicas = 3;
    raw.set.metadata.generation = Some(raw.set.metadata.generation.unwrap_or_default() + 1);
    api.set_observation(raw).await;

    let mut saw_failover = false;
    for _ in 0..30 {
        let mut raw = api.observation().await;
        raw.now_unix_seconds += 11;
        api.set_observation(raw).await;
        let (_, effects) = tick(&api).await;
        assert!(
            effects
                .iter()
                .all(|effect| !matches!(effect, EffectRecord::EnsureScaffolding(_))),
            "cleanup obligation must block a fresh scale-up candidate"
        );
        if effects.iter().any(|effect| {
            matches!(
                effect,
                EffectRecord::Execute(ProtocolCommand::EnsureConfiguration(command))
                    if command.transition_kind == TransitionKind::Failover
            )
        }) {
            saw_failover = true;
            break;
        }
    }
    assert!(
        saw_failover,
        "primary failover must outrank candidate cleanup"
    );
    assert!(
        api.effects().await[before..]
            .iter()
            .all(|effect| { !matches!(effect, EffectRecord::DeleteScaleDownResource { .. }) })
    );
}

#[tokio::test]
async fn allocation_lookup_failure_allows_primary_failover_and_retains_exact_authority() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(4, 5)));
    tick(&api).await;
    let before = api.observation().await;
    let allocation = before
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .scale_up_allocation
        .as_ref()
        .expect("allocation is durable before provisioning")
        .clone();
    let old_primary = before
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .unwrap()
        .identity
        .clone();
    api.fail_exact_lookup(
        "Service/db-5-allocation".into(),
        Some("candidate allocation endpoint lookup unavailable".into()),
    )
    .await;

    let mut failed = before;
    let RawAgentObservation::Report(report) = failed
        .agents
        .get_mut(&ReplicaObservationKey::new(
            old_primary.replica_id,
            old_primary.instance_id.clone(),
        ))
        .unwrap()
    else {
        panic!("primary report")
    };
    report.reported_fault = proto::FaultType::Permanent as i32;
    report.healthy = false;
    failed.now_unix_seconds += 11;
    api.set_observation(failed).await;

    for _ in 0..25 {
        let mut raw = api.observation().await;
        raw.now_unix_seconds += 11;
        api.set_observation(raw).await;
        tick(&api).await;
    }

    let effects = api.effects().await;
    assert!(effects.iter().any(|effect| {
        matches!(
            effect,
            EffectRecord::Execute(ProtocolCommand::EnsureConfiguration(command))
                if command.transition_kind == TransitionKind::Failover
        )
    }));
    assert!(
        effects
            .iter()
            .any(|effect| matches!(effect, EffectRecord::RemoveWriteRouting))
    );
    let converged = api.observation().await;
    let authority = &converged.set.status.as_ref().unwrap().authority;
    assert!(authority.transition.is_none(), "{authority:?}");
    assert_ne!(
        authority
            .topology
            .as_ref()
            .unwrap()
            .configuration
            .primary_id,
        old_primary.replica_id
    );
    assert_eq!(
        authority
            .scale_up_allocation
            .as_ref()
            .map(|retained| &retained.operation_id),
        Some(&allocation.operation_id)
    );
    let accepted_primary = authority
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .unwrap();
    assert_eq!(
        converged
            .services
            .iter()
            .find(|service| service.name_any() == "db-write")
            .and_then(|service| service.spec.as_ref())
            .and_then(|spec| spec.selector.as_ref())
            .and_then(|selector| selector.get(INSTANCE_LABEL))
            .map(String::as_str),
        Some(accepted_primary.identity.instance_id.as_str())
    );
}

#[tokio::test]
async fn accepted_repair_and_explicit_switchover_outrank_scale_up() {
    let replacement = Arc::new(InMemoryClusterApi::new(fixture(2, 3)));
    let mut raw = replacement.observation().await;
    let secondary = raw
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::ActiveSecondary)
        .unwrap()
        .identity
        .clone();
    let RawAgentObservation::Report(report) = raw
        .agents
        .get_mut(&ReplicaObservationKey::new(
            secondary.replica_id,
            secondary.instance_id,
        ))
        .unwrap()
    else {
        panic!("secondary report")
    };
    report.reported_fault = proto::FaultType::Permanent as i32;
    raw.now_unix_seconds += 20;
    replacement.set_observation(raw).await;
    for _ in 0..10 {
        tick(&replacement).await;
        if replacement
            .effects()
            .await
            .iter()
            .any(|effect| matches!(effect, EffectRecord::EnsureReplacement(_)))
        {
            break;
        }
    }
    assert!(
        replacement
            .effects()
            .await
            .iter()
            .any(|effect| matches!(effect, EffectRecord::EnsureReplacement(_)))
    );
    assert!(
        replacement
            .observation()
            .await
            .pvcs
            .iter()
            .all(|pvc| pvc.name_any() != "db-3-data")
    );

    let switchover = Arc::new(InMemoryClusterApi::new(fixture(2, 3)));
    let mut raw = switchover.observation().await;
    raw.set.spec.switchover = Some(PlannedSwitchoverRequestSpec {
        request_id: "scale-up-priority".into(),
        target_replica_id: 2,
    });
    raw.set.metadata.generation = Some(raw.set.metadata.generation.unwrap_or_default() + 1);
    switchover.set_observation(raw).await;
    for _ in 0..5 {
        let (_, effects) = tick(&switchover).await;
        assert!(
            effects
                .iter()
                .all(|effect| !matches!(effect, EffectRecord::EnsureScaffolding(_)))
        );
        if effects.iter().any(|effect| {
            matches!(
                effect,
                EffectRecord::Execute(ProtocolCommand::PrepareSwitchover(_))
            )
        }) {
            return;
        }
    }
    panic!("explicit switchover did not outrank scale-up");
}

#[tokio::test]
async fn completion_receipt_does_not_block_exact_pod_safety_fencing() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(2, 3)));
    finish(&api, 3).await;
    let raw = api.observation().await;
    assert!(
        raw.set
            .status
            .as_ref()
            .unwrap()
            .authority
            .last_scale_up
            .is_some()
    );
    let original_pvcs = raw.pvcs.clone();
    let pod = raw
        .pods
        .iter()
        .find(|pod| pod.labels().get(REPLICA_ID_LABEL).map(String::as_str) == Some("3"))
        .unwrap();
    let name = pod.name_any();
    let uid = PodUid::new(pod.uid().unwrap());
    api.delete_exact_pod(&raw, &name, &uid).await.unwrap();

    let after = api.observation().await;
    assert_eq!(after.pvcs, original_pvcs);
    assert!(
        after
            .pods
            .iter()
            .all(|pod| pod.uid().as_deref() != Some(uid.as_str()))
    );
    assert!(api.effects().await.iter().any(|effect| matches!(
        effect,
        EffectRecord::DeleteExactPod { pod_uid, .. } if pod_uid == &uid
    )));
}

#[tokio::test]
async fn switchover_scale_down_scale_up_restores_missing_ordinal_with_fresh_incarnation() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(3, 3)));
    let old_member_two = api
        .observation()
        .await
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .members
        .iter()
        .find(|member| member.identity.replica_id.value() == 2)
        .unwrap()
        .identity
        .clone();

    let mut raw = api.observation().await;
    raw.set.spec.switchover = Some(PlannedSwitchoverRequestSpec {
        request_id: "primary-to-three".into(),
        target_replica_id: 3,
    });
    raw.set.metadata.generation = Some(raw.set.metadata.generation.unwrap_or_default() + 1);
    api.set_observation(raw).await;
    finish_switchover(&api, 3).await;

    let mut raw = api.observation().await;
    raw.set.spec.replicas = 2;
    raw.set.metadata.generation = Some(raw.set.metadata.generation.unwrap_or_default() + 1);
    api.set_observation(raw).await;
    finish_scale_down(&api, 2).await;
    let reduced = api.observation().await;
    assert_eq!(
        reduced
            .set
            .status
            .as_ref()
            .unwrap()
            .authority
            .topology
            .as_ref()
            .unwrap()
            .configuration
            .members
            .iter()
            .map(|member| member.identity.replica_id.value())
            .collect::<Vec<_>>(),
        vec![1, 3]
    );

    let mut raw = reduced;
    raw.set.spec.replicas = 3;
    raw.set.metadata.generation = Some(raw.set.metadata.generation.unwrap_or_default() + 1);
    api.set_observation(raw).await;
    finish(&api, 3).await;

    let completed = api.observation().await;
    let authority = &completed.set.status.as_ref().unwrap().authority;
    let restored = authority
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .members
        .iter()
        .find(|member| member.identity.replica_id.value() == 2)
        .expect("missing logical member is restored");
    assert_ne!(restored.identity, old_member_two);
    let primary = authority
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .unwrap();
    assert_eq!(primary.identity.replica_id.value(), 3);
    assert!(completed.services.iter().any(|service| {
        service.name_any() == "db-write"
            && service
                .spec
                .as_ref()
                .and_then(|spec| spec.selector.as_ref())
                .and_then(|selector| selector.get(INSTANCE_LABEL))
                .map(String::as_str)
                == Some(primary.identity.instance_id.as_str())
    }));
}

#[tokio::test]
async fn committed_candidate_pod_loss_replaces_and_converges_with_receipt_retained() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(2, 3)));
    finish(&api, 3).await;
    let before = api.observation().await;
    let old = before
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .members
        .iter()
        .find(|member| member.identity.replica_id.value() == 3)
        .unwrap()
        .identity
        .clone();
    let old_pvc_uid = before
        .pvcs
        .iter()
        .find(|pvc| pvc.name_any() == "db-3-data")
        .and_then(ResourceExt::uid)
        .unwrap();
    api.delete_exact_pod(&before, "db-3", &PodUid::new(old.instance_id.as_str()))
        .await
        .unwrap();
    assert!(
        api.observation()
            .await
            .pvcs
            .iter()
            .any(|pvc| pvc.uid().as_deref() == Some(old_pvc_uid.as_str()))
    );

    finish(&api, 3).await;
    let completed = api.observation().await;
    let authority = &completed.set.status.as_ref().unwrap().authority;
    let replacement = authority
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .members
        .iter()
        .find(|member| member.identity.replica_id.value() == 3)
        .unwrap();
    assert_ne!(replacement.identity.instance_id, old.instance_id);
    assert!(authority.last_scale_up.is_some());
    assert!(authority.pending_replacement_cleanup.is_none());
    assert!(authority.last_replacement.is_none());
}

#[tokio::test]
async fn completion_receipt_yields_to_different_uid_canonical_pod_replacement() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(2, 3)));
    finish(&api, 3).await;
    let mut raw = api.observation().await;
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
        .members
        .iter()
        .find(|member| member.identity.replica_id.value() == 3)
        .unwrap()
        .identity
        .clone();
    let old_pvc_uid = raw
        .pvcs
        .iter()
        .find(|pvc| pvc.name_any() == "db-3-data")
        .and_then(ResourceExt::uid)
        .unwrap();
    let mut replacement_pvc = raw
        .pvcs
        .iter()
        .find(|pvc| pvc.name_any() == "db-3-data")
        .unwrap()
        .clone();
    replacement_pvc.metadata.name = Some("db-3-successor-data".into());
    replacement_pvc.metadata.uid = Some("different-pvc-uid".into());
    replacement_pvc.metadata.resource_version = Some("different-pvc-rv".into());
    raw.pvcs.push(replacement_pvc);
    let pod = raw
        .pods
        .iter_mut()
        .find(|pod| pod.name_any() == "db-3")
        .unwrap();
    pod.metadata.uid = Some("different-pod-uid".into());
    pod.metadata.resource_version = Some("different-pod-rv".into());
    pod.metadata
        .labels
        .get_or_insert_default()
        .insert(INSTANCE_LABEL.into(), "different-pod-uid".into());
    pod.spec.as_mut().unwrap().volumes.as_mut().unwrap()[0]
        .persistent_volume_claim
        .as_mut()
        .unwrap()
        .claim_name = "db-3-successor-data".into();
    raw.agents.insert(
        ReplicaObservationKey::new(old.replica_id, old.instance_id.clone()),
        RawAgentObservation::Unavailable {
            message: "frozen accepted Pod no longer exists".into(),
        },
    );
    api.set_observation(raw).await;

    finish(&api, 3).await;
    let completed = api.observation().await;
    let authority = &completed.set.status.as_ref().unwrap().authority;
    let member = authority
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .members
        .iter()
        .find(|member| member.identity.replica_id.value() == 3)
        .unwrap();
    assert_ne!(member.identity, old);
    assert_ne!(member.identity.instance_id.as_str(), "different-pod-uid");
    assert!(authority.last_scale_up.is_some());
    assert!(authority.pending_replacement_cleanup.is_none());
    assert!(authority.last_replacement.is_none());
    assert!(completed.pods.iter().any(|pod| {
        pod.name_any() == "db-3" && pod.uid().as_deref() == Some("different-pod-uid")
    }));
    assert!(
        completed
            .pvcs
            .iter()
            .any(|pvc| pvc.uid().as_deref() == Some("different-pvc-uid"))
    );
    let effects = api.effects().await;
    let admission = effects
        .iter()
        .position(|effect| {
            matches!(
                effect,
                EffectRecord::Execute(ProtocolCommand::EnsureConfiguration(command))
                    if command.transition_kind == TransitionKind::Replacement
            )
        })
        .expect("ordinary accepted-member replacement was admitted");
    let old_storage_cleanup = effects
        .iter()
        .position(|effect| {
            matches!(
                effect,
                EffectRecord::DeleteScaleDownResource {
                    resource: ScaleDownResource::Pvc,
                    uid,
                    ..
                } if uid == old_pvc_uid.as_str()
            )
        })
        .expect("orphaned accepted-member storage was eventually cleaned");
    assert!(
        admission < old_storage_cleanup,
        "receipt resources must not grant pre-admission cleanup authority"
    );
}

#[tokio::test]
async fn repeated_primary_loss_outranks_deferred_candidate_cleanup() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(4, 5)));
    for _ in 0..30 {
        tick(&api).await;
        if api
            .observation()
            .await
            .set
            .status
            .as_ref()
            .is_some_and(|status| status.authority.provisioning.is_some())
        {
            break;
        }
    }
    let raw = api.observation().await;
    let provisioning = raw
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .provisioning
        .as_ref()
        .expect("active scale-up provisioning")
        .clone();
    let target = provisioning.target_identity(&ResourceUid::new(UID));
    let endpoint_name = derive_replica_endpoint_name(&ResourceUid::new(UID), &target);
    api.fail_exact_lookup(
        format!("Service/{endpoint_name}"),
        Some("candidate endpoint lookup unavailable".into()),
    )
    .await;

    for expected_failovers in 1..=2 {
        let mut raw = api.observation().await;
        let primary = raw
            .set
            .status
            .as_ref()
            .unwrap()
            .authority
            .topology
            .as_ref()
            .unwrap()
            .configuration
            .members
            .iter()
            .find(|member| member.role == ReplicaRole::Primary)
            .unwrap()
            .identity
            .clone();
        let RawAgentObservation::Report(report) = raw
            .agents
            .get_mut(&ReplicaObservationKey::new(
                primary.replica_id,
                primary.instance_id,
            ))
            .unwrap()
        else {
            panic!("primary report")
        };
        report.reported_fault = proto::FaultType::Permanent as i32;
        report.healthy = false;
        raw.now_unix_seconds += 11;
        api.set_observation(raw).await;

        for _ in 0..120 {
            let mut raw = api.observation().await;
            raw.now_unix_seconds += 11;
            api.set_observation(raw).await;
            tick(&api).await;
            let failovers = api
                .effects()
                .await
                .iter()
                .filter_map(|effect| {
                    if let EffectRecord::Execute(ProtocolCommand::EnsureConfiguration(command)) =
                        effect
                        && command.transition_kind == TransitionKind::Failover
                    {
                        return Some(command.current_configuration.configuration_id.clone());
                    }
                    None
                })
                .collect::<BTreeSet<_>>()
                .len();
            let status = api.observation().await.set.status.unwrap().authority;
            assert!(
                status.provisioning.is_some(),
                "exact candidate cleanup provenance must remain durable"
            );
            if failovers >= expected_failovers
                && status.transition.is_none()
                && status
                    .topology
                    .as_ref()
                    .is_some_and(|topology| topology.configuration.primary_id != primary.replica_id)
            {
                break;
            }
        }
        let failovers = api
            .effects()
            .await
            .iter()
            .filter_map(|effect| {
                if let EffectRecord::Execute(ProtocolCommand::EnsureConfiguration(command)) = effect
                    && command.transition_kind == TransitionKind::Failover
                {
                    return Some(command.current_configuration.configuration_id.clone());
                }
                None
            })
            .collect::<BTreeSet<_>>()
            .len();
        assert!(
            failovers >= expected_failovers,
            "primary loss {expected_failovers} did not produce another failover command: {:?}",
            api.observation().await.set.status
        );
        if expected_failovers == 1 {
            let mut converged = api.observation().await;
            let configuration = converged
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
            for member in &configuration.members {
                let RawAgentObservation::Report(report) = converged
                    .agents
                    .get_mut(&ReplicaObservationKey::new(
                        member.identity.replica_id,
                        member.identity.instance_id.clone(),
                    ))
                    .unwrap()
                else {
                    panic!("accepted failover member report")
                };
                report.epoch = Some(configuration.epoch.into());
                report.previous_configuration = None;
                report.current_configuration = Some(configuration.clone().into());
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
                report.report_sequence += 1;
            }
            api.set_observation(converged).await;
        }
    }
}

#[tokio::test]
async fn cancellation_after_pvc_create_lost_reply_cleans_exact_storage_after_restart() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(1, 2)));
    api.lose_next_create_reply().await;
    for _ in 0..10 {
        tick(&api).await;
        let raw = api.observation().await;
        if raw.pvcs.iter().any(|pvc| pvc.name_any() == "db-2-data")
            && raw.pods.iter().all(|pod| pod.name_any() != "db-2")
        {
            let mut cancelled = raw;
            cancelled.set.spec.replicas = 1;
            cancelled.set.metadata.generation =
                Some(cancelled.set.metadata.generation.unwrap_or_default() + 1);
            let restarted = Arc::new(InMemoryClusterApi::new(cancelled));
            finish(&restarted, 1).await;
            let completed = restarted.observation().await;
            assert!(
                completed
                    .pvcs
                    .iter()
                    .all(|pvc| pvc.name_any() != "db-2-data")
            );
            assert!(completed.pods.iter().any(|pod| pod.name_any() == "db-1"));
            assert!(
                completed
                    .pvcs
                    .iter()
                    .any(|pvc| pvc.name_any() == "db-1-data")
            );
            return;
        }
    }
    panic!("PVC-only allocation boundary not reached");
}

#[tokio::test]
async fn cancellation_after_pod_create_cleans_endpoint_pod_pvc_without_unrelated_deletion() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(1, 2)));
    for _ in 0..20 {
        tick(&api).await;
        let raw = api.observation().await;
        let before_provisioning = raw
            .set
            .status
            .as_ref()
            .is_some_and(|status| status.authority.provisioning.is_none());
        if before_provisioning && raw.pods.iter().any(|pod| pod.name_any() == "db-2") {
            let mut cancelled = raw;
            cancelled.set.spec.replicas = 1;
            cancelled.set.metadata.generation =
                Some(cancelled.set.metadata.generation.unwrap_or_default() + 1);
            api.set_observation(cancelled).await;
            finish(&api, 1).await;
            let completed = api.observation().await;
            assert!(completed.pods.iter().all(|pod| pod.name_any() != "db-2"));
            assert!(
                completed
                    .pvcs
                    .iter()
                    .all(|pvc| pvc.name_any() != "db-2-data")
            );
            assert!(completed.pods.iter().any(|pod| pod.name_any() == "db-1"));
            assert!(
                completed
                    .pvcs
                    .iter()
                    .any(|pvc| pvc.name_any() == "db-1-data")
            );
            return;
        }
    }
    panic!("Pod-created pre-provisioning boundary not reached");
}

async fn active_pvc_only_allocation(
    api: &Arc<InMemoryClusterApi>,
) -> kuberic_runtime::protocol::types::ScaleUpAllocation {
    for _ in 0..20 {
        tick(api).await;
        let raw = api.observation().await;
        if let Some(allocation) = raw
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.scale_up_allocation.as_ref())
            .filter(|allocation| {
                allocation.pvc_uid.is_some()
                    && allocation.pod_uid.is_none()
                    && !allocation.cancellation_started
                    && raw.pvcs.iter().any(|pvc| {
                        pvc.name_any() == "db-2-data"
                            && pvc.uid().as_deref()
                                == allocation.pvc_uid.as_ref().map(PvcUid::as_str)
                    })
                    && raw.pods.iter().all(|pod| pod.name_any() != "db-2")
            })
        {
            return allocation.clone();
        }
    }
    panic!("active PVC-only allocation boundary not reached");
}

fn kube_response<T: serde::Serialize>(object: &T, kind: &str) -> serde_json::Value {
    let mut value = serde_json::to_value(object).unwrap();
    let object = value.as_object_mut().unwrap();
    object.insert("apiVersion".into(), "v1".into());
    object.insert("kind".into(), kind.into());
    value
}

async fn kube_http_sequence(
    responses: Vec<serde_json::Value>,
) -> (Client, tokio::task::JoinHandle<Vec<String>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for response in responses {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            loop {
                let mut chunk = [0; 4096];
                let count = stream.read(&mut chunk).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&chunk[..count]);
                let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") else {
                    continue;
                };
                let headers = String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
                let length = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: "))
                    .map(|value| value.parse::<usize>().unwrap())
                    .unwrap_or(0);
                if bytes.len() >= end + 4 + length {
                    break;
                }
            }
            requests.push(String::from_utf8(bytes).unwrap());
            let body = response.to_string();
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
        requests
    });
    let config = kube::Config::new(format!("http://{address}").parse().unwrap());
    (Client::try_from(config).unwrap(), server)
}

#[tokio::test]
async fn production_pod_effect_revalidates_live_pvc_before_create() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(1, 2)));
    let allocation = active_pvc_only_allocation(&api).await;
    let cached = api.observe("tests", "db").await.unwrap();
    let snapshot = normalize(cached.clone(), BTreeMap::new()).unwrap();
    let plan = evaluate(&snapshot, &enabled());
    assert!(matches!(
        plan,
        Plan::Apply { ref changes }
            if changes == &vec![KubernetesChange::EnsureReplicaScaffolding {
                replica_ids: vec![ReplicaId::new(2)],
            }]
    ));

    let old_pvc_uid = allocation.pvc_uid.as_ref().unwrap().clone();
    let replacement_uid = "replacement-at-pod-effect";
    let mut physical = api.observation().await;
    let replacement = physical
        .pvcs
        .iter_mut()
        .find(|pvc| pvc.name_any() == "db-2-data")
        .unwrap();
    replacement.metadata.uid = Some(replacement_uid.into());
    replacement.metadata.resource_version = Some("replacement-at-effect-rv".into());
    let live_replacement = replacement.clone();
    let secret = cached
        .secrets
        .iter()
        .find(|secret| secret.name_any() == "db-agent-credentials")
        .unwrap()
        .clone();
    api.set_observation(physical).await;

    let (client, requests) = kube_http_sequence(vec![
        kube_response(&secret, "Secret"),
        kube_response(&secret, "Secret"),
        kube_response(&live_replacement, "PersistentVolumeClaim"),
    ])
    .await;
    let production = KubeClusterApi::new(
        client,
        Arc::new(GrpcAgentApi::new(Duration::from_secs(1))),
        "test-token",
    )
    .unwrap();
    let result = execute_plan(&production, &cached, &snapshot, plan).await;
    assert_eq!(result, Err(ControllerError::ObservationStale));
    let requests = requests.await.unwrap();
    assert!(requests.iter().any(|request| {
        request.starts_with("GET /api/v1/namespaces/tests/persistentvolumeclaims/db-2-data ")
    }));
    assert!(requests.iter().all(|request| {
        !request.starts_with("POST /api/v1/namespaces/tests/pods ")
            && !request.contains("\"KUBERIC_PVC_UID\":\"replacement-at-pod-effect\"")
    }));

    let effects_start = api.effects().await.len();
    for _ in 0..8 {
        let (kind, effects) = tick(&api).await;
        assert_ne!(kind, ReconcileKind::Unsafe);
        assert!(
            effects
                .iter()
                .all(|effect| !matches!(effect, EffectRecord::RemoveWriteRouting))
        );
    }
    let occupied = api.observation().await;
    assert!(occupied.pods.iter().all(|pod| pod.name_any() != "db-2"));
    assert!(occupied.pvcs.iter().any(|pvc| {
        pvc.name_any() == "db-2-data" && pvc.uid().as_deref() == Some(replacement_uid)
    }));
    assert!(api.effects().await[effects_start..].iter().all(|effect| {
        !matches!(
            effect,
            EffectRecord::DeleteScaleDownResource {
                resource: ScaleDownResource::Pvc,
                uid,
                ..
            } if uid == replacement_uid
        ) && !matches!(effect, EffectRecord::RemoveWriteRouting)
    }));

    let mut available = occupied;
    available
        .pvcs
        .retain(|pvc| pvc.uid().as_deref() != Some(replacement_uid));
    api.set_observation(available).await;
    finish(&api, 2).await;
    let completed = api.observation().await;
    assert!(completed.set.status.as_ref().is_some_and(|status| {
        status.authority.last_scale_up.is_some() && status.authority.scale_up_allocation.is_none()
    }));
    assert!(completed.pvcs.iter().all(|pvc| {
        pvc.uid().as_deref() != Some(old_pvc_uid.as_str())
            && pvc.uid().as_deref() != Some(replacement_uid)
    }));
}

#[tokio::test]
async fn residual_pvc_replace_after_live_get_is_candidate_local() {
    for case in ["current", "missing", "empty", "malformed", "stale"] {
        let api = Arc::new(InMemoryClusterApi::new(fixture(1, 2)));
        let allocation = active_pvc_only_allocation(&api).await;
        let frozen_pvc_uid = allocation.pvc_uid.as_ref().unwrap().clone();
        let replacement_uid = format!("replacement-after-live-get-{case}");
        let raced_pod_uid = "raced-candidate-pod";
        let mut raced = api.observation().await;
        let pvc = raced
            .pvcs
            .iter_mut()
            .find(|pvc| pvc.name_any() == "db-2-data")
            .unwrap();
        let candidate_owner_references = pvc.metadata.owner_references.clone();
        pvc.metadata.uid = Some(replacement_uid.clone());
        pvc.metadata.resource_version = Some(format!("replacement-after-live-get-{case}-rv"));
        let replacement_pvc_operation = match case {
            "current" => Some(allocation.operation_id.as_str()),
            "missing" => None,
            "empty" => Some(""),
            "malformed" => Some("not an operation id"),
            "stale" => Some("stale-allocation-operation"),
            _ => unreachable!(),
        };
        match replacement_pvc_operation {
            Some(operation) => {
                pvc.metadata
                    .annotations
                    .get_or_insert_default()
                    .insert(SCALE_UP_ALLOCATION_ANNOTATION.into(), operation.into());
            }
            None => {
                pvc.metadata
                    .annotations
                    .get_or_insert_default()
                    .remove(SCALE_UP_ALLOCATION_ANNOTATION);
            }
        }
        let mut pod = raced
            .pods
            .iter()
            .find(|pod| pod.name_any() == "db-1")
            .unwrap()
            .clone();
        pod.metadata.name = Some("db-2".into());
        pod.metadata.uid = Some(raced_pod_uid.into());
        pod.metadata.resource_version = Some("raced-candidate-pod-rv".into());
        pod.metadata.owner_references = candidate_owner_references;
        pod.metadata
            .labels
            .get_or_insert_default()
            .insert(REPLICA_ID_LABEL.into(), "2".into());
        pod.metadata
            .labels
            .get_or_insert_default()
            .insert(INSTANCE_LABEL.into(), raced_pod_uid.into());
        pod.metadata.annotations.get_or_insert_default().insert(
            SCALE_UP_ALLOCATION_ANNOTATION.into(),
            allocation.operation_id.to_string(),
        );
        let spec = pod.spec.as_mut().unwrap();
        spec.volumes
            .as_mut()
            .unwrap()
            .iter_mut()
            .find_map(|volume| volume.persistent_volume_claim.as_mut())
            .unwrap()
            .claim_name = "db-2-data".into();
        set_configured_pvc_uid(&mut pod, frozen_pvc_uid.as_str());
        raced.pods.push(pod);
        let raced_key =
            ReplicaObservationKey::new(ReplicaId::new(2), ReplicaInstanceId::new(raced_pod_uid));
        raced.agents.insert(
            raced_key.clone(),
            RawAgentObservation::Report(Box::new(proto::AgentStatusReport {
                protocol_version: kuberic_runtime::protocol::PROTOCOL_VERSION,
                resource_uid: UID.to_string(),
                process_session_id: "agent-started-before-controller-recovery".into(),
                report_sequence: 1,
                storage_state: proto::AgentStorageState::Uninitialized as i32,
                pod_uid: raced_pod_uid.into(),
                pvc_uid: frozen_pvc_uid.to_string(),
                healthy: true,
                replica_id: 2,
                ..Default::default()
            })),
        );

        let mut wrong_pod_provenance = raced.clone();
        wrong_pod_provenance
            .pods
            .iter_mut()
            .find(|pod| pod.uid().as_deref() == Some(raced_pod_uid))
            .unwrap()
            .metadata
            .annotations
            .get_or_insert_default()
            .insert(
                SCALE_UP_ALLOCATION_ANNOTATION.into(),
                "stale-pod-allocation-operation".into(),
            );
        let mut wrong_owner = raced.clone();
        wrong_owner
            .pods
            .iter_mut()
            .find(|pod| pod.uid().as_deref() == Some(raced_pod_uid))
            .unwrap()
            .metadata
            .owner_references = None;
        let mut wrong_session = raced.clone();
        let RawAgentObservation::Report(report) = wrong_session.agents.get_mut(&raced_key).unwrap()
        else {
            unreachable!();
        };
        report.process_session_id.clear();
        let mut zero_report_sequence = raced.clone();
        let RawAgentObservation::Report(report) =
            zero_report_sequence.agents.get_mut(&raced_key).unwrap()
        else {
            unreachable!();
        };
        report.report_sequence = 0;
        for (control, raw) in [
            ("wrong Pod allocation provenance", wrong_pod_provenance),
            ("wrong Pod owner", wrong_owner),
            ("empty agent process session", wrong_session),
            ("zero agent report sequence", zero_report_sequence),
        ] {
            let control_api = InMemoryClusterApi::new(raw);
            let observed = control_api.observe("tests", "db").await.unwrap();
            let snapshot = normalize(observed, BTreeMap::new()).unwrap();
            assert!(
                matches!(
                    evaluate(&snapshot, &enabled()),
                    Plan::Unsafe {
                        ref safety_changes,
                        ..
                    } if safety_changes == &[SafetyChange::RemoveWriteRouting]
                ),
                "{case}: {control} must not authorize candidate-local cleanup"
            );
        }
        let mut absent_frozen_pvc = raced.clone();
        absent_frozen_pvc
            .pvcs
            .retain(|pvc| pvc.name_any() != "db-2-data");
        let absent_api = InMemoryClusterApi::new(absent_frozen_pvc);
        let absent_snapshot = normalize(
            absent_api.observe("tests", "db").await.unwrap(),
            BTreeMap::new(),
        )
        .unwrap();
        assert!(
            !matches!(evaluate(&absent_snapshot, &enabled()), Plan::Unsafe { .. }),
            "{case}: authoritative frozen PVC absence must retain independently proven candidate-local cleanup"
        );

        api.set_observation(raced).await;
        let recovered_raw = api.observe("tests", "db").await.unwrap();
        let recovered_snapshot = normalize(recovered_raw, BTreeMap::new()).unwrap();
        let recovered_plan = evaluate(&recovered_snapshot, &enabled());
        assert!(
            !matches!(recovered_plan, Plan::Unsafe { .. }),
            "{case}: startup report with the Pod's frozen PVC UID must enter candidate-local arbitration regardless of replacement PVC metadata: {recovered_plan:?}; exact={:?}; replicas={:?}",
            recovered_snapshot.secondary_scale_down_resources,
            recovered_snapshot.replicas,
        );
        let mut accepted_invalid = recovered_snapshot.clone();
        accepted_invalid
            .replicas
            .get_mut(&ReplicaObservationKey::new(
                ReplicaId::new(1),
                ReplicaInstanceId::new("pod-uid-1"),
            ))
            .unwrap()
            .agent = AgentObservation::Invalid {
            message: "accepted member remains globally fenced".into(),
            uninitialized_report: None,
        };
        assert!(
            matches!(
                evaluate(&accepted_invalid, &enabled()),
                Plan::Unsafe {
                    ref safety_changes,
                    ..
                } if safety_changes == &[SafetyChange::RemoveWriteRouting]
            ),
            "{case}"
        );
        let effects_start = api.effects().await.len();

        for _ in 0..12 {
            let (kind, effects) = tick(&api).await;
            assert_ne!(kind, ReconcileKind::Unsafe, "{case}");
            assert!(
                effects
                    .iter()
                    .all(|effect| !matches!(effect, EffectRecord::RemoveWriteRouting)),
                "{case}: {effects:?}"
            );
        }
        let blocked = api.observation().await;
        let waiting_retry = blocked
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.scale_up_allocation.as_ref())
            .expect("fresh allocation waits for the canonical PVC name");
        assert_ne!(
            waiting_retry.operation_id, allocation.operation_id,
            "{case}"
        );
        assert!(waiting_retry.pvc_uid.is_none(), "{case}");
        assert!(waiting_retry.pod_uid.is_none(), "{case}");
        assert!(
            blocked
                .pods
                .iter()
                .all(|pod| pod.uid().as_deref() != Some(raced_pod_uid)),
            "{case}"
        );
        assert!(
            blocked.pvcs.iter().any(|pvc| {
                pvc.name_any() == "db-2-data"
                    && pvc.uid().as_deref() == Some(replacement_uid.as_str())
            }),
            "{case}"
        );
        assert!(
            api.effects().await[effects_start..].iter().all(|effect| {
                !matches!(
                    effect,
                    EffectRecord::DeleteScaleDownResource {
                        resource: ScaleDownResource::Pvc,
                        uid,
                        ..
                    } if uid == &replacement_uid
                ) && !matches!(effect, EffectRecord::RemoveWriteRouting)
            }),
            "{case}"
        );
        assert!(
            api.effects().await[effects_start..]
                .iter()
                .all(|effect| match effect {
                    EffectRecord::DeleteScaleDownResource { resource, uid, .. } =>
                        *resource == ScaleDownResource::Pod && uid == raced_pod_uid,
                    _ => true,
                }),
            "{case}: cleanup may delete only the independently proven candidate Pod"
        );
        assert!(
            api.effects().await[effects_start..].iter().any(|effect| {
                matches!(
                    effect,
                    EffectRecord::DeleteScaleDownResource {
                        resource: ScaleDownResource::Pod,
                        uid,
                        ..
                    } if uid == raced_pod_uid
                )
            }),
            "{case}"
        );
        let expected_primary = blocked
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.topology.as_ref())
            .and_then(|topology| {
                topology
                    .configuration
                    .members
                    .iter()
                    .find(|member| member.role == ReplicaRole::Primary)
            })
            .map(|member| member.identity.clone());
        assert_eq!(
            normalize(blocked.clone(), BTreeMap::new())
                .unwrap()
                .routing
                .write_target,
            expected_primary,
            "{case}"
        );

        let mut available = blocked;
        available
            .pvcs
            .retain(|pvc| pvc.uid().as_deref() != Some(replacement_uid.as_str()));
        api.set_observation(available).await;
        finish(&api, 2).await;
        let completed = api.observation().await;
        let receipt = completed
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.last_scale_up.as_ref())
            .expect("fresh scale-up retry converged");
        assert_ne!(
            receipt.intent.target.instance_id.as_str(),
            raced_pod_uid,
            "{case}: the cleaned candidate must not be admitted"
        );
        assert!(
            completed.pvcs.iter().all(|pvc| {
                pvc.uid().as_deref() != Some(replacement_uid.as_str())
                    && pvc.uid().as_deref() != Some(frozen_pvc_uid.as_str())
            }),
            "{case}"
        );
    }
}

#[tokio::test]
async fn pvc_only_allocation_never_adopts_same_name_pod_without_exact_provenance() {
    for (case, provenance) in [
        ("missing-metadata", None),
        ("malformed-provenance", Some("")),
        ("stale-prior-operation", Some("stale-allocation-operation")),
    ] {
        let api = Arc::new(InMemoryClusterApi::new(fixture(1, 2)));
        let allocation = active_pvc_only_allocation(&api).await;
        let frozen_pvc_uid = allocation.pvc_uid.as_ref().unwrap().clone();
        let occupant_uid = format!("unrelated-{case}");
        let mut occupied = api.observation().await;
        let mut occupant = occupied
            .pods
            .iter()
            .find(|pod| pod.name_any() == "db-1")
            .unwrap()
            .clone();
        occupant.metadata.name = Some("db-2".into());
        occupant.metadata.uid = Some(occupant_uid.clone());
        occupant.metadata.resource_version = Some(format!("{case}-rv"));
        let spec = occupant.spec.as_mut().unwrap();
        spec.volumes
            .as_mut()
            .unwrap()
            .iter_mut()
            .find_map(|volume| volume.persistent_volume_claim.as_mut())
            .unwrap()
            .claim_name = "db-2-data".into();
        set_configured_pvc_uid(&mut occupant, frozen_pvc_uid.as_str());
        if case == "missing-metadata" {
            occupant.metadata.labels = None;
            occupant.metadata.owner_references = None;
            occupant.metadata.annotations = None;
        } else {
            let labels = occupant.metadata.labels.get_or_insert_default();
            labels.insert(REPLICA_ID_LABEL.into(), "2".into());
            labels.insert(INSTANCE_LABEL.into(), occupant_uid.clone());
            occupant
                .metadata
                .annotations
                .get_or_insert_default()
                .insert(
                    SCALE_UP_ALLOCATION_ANNOTATION.into(),
                    provenance.unwrap().into(),
                );
        }
        occupied.agents.insert(
            ReplicaObservationKey::new(ReplicaId::new(2), ReplicaInstanceId::new(&occupant_uid)),
            RawAgentObservation::Absent,
        );
        occupied.pods.push(occupant);
        api.set_observation(occupied).await;
        let effects_start = api.effects().await.len();

        for _ in 0..8 {
            let (kind, effects) = tick(&api).await;
            assert_ne!(kind, ReconcileKind::Unsafe, "{case}");
            assert!(
                effects.iter().all(|effect| {
                    !matches!(
                        effect,
                        EffectRecord::RemoveWriteRouting
                            | EffectRecord::EnsureScaffolding(_)
                            | EffectRecord::DeleteScaleDownResource {
                                resource: ScaleDownResource::Pod,
                                ..
                            }
                    )
                }),
                "{case}: {effects:?}"
            );
        }

        let blocked = api.observation().await;
        let still_frozen = blocked
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.scale_up_allocation.as_ref())
            .expect("collision keeps the exact allocation pending");
        assert_eq!(still_frozen.operation_id, allocation.operation_id, "{case}");
        assert_eq!(still_frozen.pvc_uid, allocation.pvc_uid, "{case}");
        assert!(still_frozen.pod_uid.is_none(), "{case}");
        assert!(still_frozen.cancellation_started, "{case}");
        assert!(blocked.pods.iter().any(|pod| {
            pod.name_any() == "db-2" && pod.uid().as_deref() == Some(occupant_uid.as_str())
        }));
        assert!(blocked.pvcs.iter().any(|pvc| {
            pvc.name_any() == "db-2-data" && pvc.uid().as_deref() == Some(frozen_pvc_uid.as_str())
        }));
        assert!(api.effects().await[effects_start..].iter().all(|effect| {
            !matches!(effect, EffectRecord::RemoveWriteRouting)
                && !matches!(
                    effect,
                    EffectRecord::DeleteScaleDownResource {
                        resource: ScaleDownResource::Pod,
                        uid,
                        ..
                    } if uid == &occupant_uid
                )
                && !matches!(
                    effect,
                    EffectRecord::DeleteScaleDownResource {
                        resource: ScaleDownResource::Pvc,
                        uid,
                        ..
                    } if uid == frozen_pvc_uid.as_str()
                )
        }));
        let expected_primary = blocked
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.topology.as_ref())
            .and_then(|topology| {
                topology
                    .configuration
                    .members
                    .iter()
                    .find(|member| member.role == ReplicaRole::Primary)
            })
            .map(|member| member.identity.clone());
        assert_eq!(
            normalize(blocked.clone(), BTreeMap::new())
                .unwrap()
                .routing
                .write_target,
            expected_primary,
            "{case}"
        );

        let mut available = blocked;
        available
            .pods
            .retain(|pod| pod.uid().as_deref() != Some(occupant_uid.as_str()));
        available.agents.remove(&ReplicaObservationKey::new(
            ReplicaId::new(2),
            ReplicaInstanceId::new(&occupant_uid),
        ));
        api.set_observation(available).await;
        finish(&api, 2).await;
        let completed = api.observation().await;
        let receipt = completed
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.last_scale_up.as_ref())
            .expect("fresh legitimate allocation converges after collision clears");
        assert_ne!(
            receipt.intent.target.instance_id.as_str(),
            occupant_uid,
            "{case}"
        );
        assert!(
            completed
                .pods
                .iter()
                .all(|pod| pod.uid().as_deref() != Some(occupant_uid.as_str())),
            "{case}"
        );
    }
}

#[tokio::test]
async fn active_allocation_frozen_pvc_404_abandons_exact_attempt_without_routing_flap() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(1, 2)));
    let old_allocation = active_pvc_only_allocation(&api).await;
    let old_pvc_uid = old_allocation.pvc_uid.as_ref().unwrap().clone();
    let effects_start = api.effects().await.len();
    let mut raw = api.observation().await;
    raw.pvcs
        .retain(|pvc| pvc.uid().as_deref() != Some(old_pvc_uid.as_str()));
    api.set_observation(raw).await;

    let mut fresh_allocation = None;
    for _ in 0..12 {
        let (kind, effects) = tick(&api).await;
        assert_ne!(kind, ReconcileKind::Unsafe);
        assert!(
            effects
                .iter()
                .all(|effect| !matches!(effect, EffectRecord::RemoveWriteRouting))
        );
        if let Some(allocation) = api
            .observation()
            .await
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.scale_up_allocation.as_ref())
            .filter(|allocation| allocation.operation_id != old_allocation.operation_id)
        {
            fresh_allocation = Some(allocation.operation_id.clone());
        }
    }

    let after_recovery = api.observation().await;
    assert!(after_recovery.set.status.as_ref().is_some_and(|status| {
        status
            .authority
            .scale_up_allocation
            .as_ref()
            .is_none_or(|allocation| allocation.operation_id != old_allocation.operation_id)
    }));
    assert!(
        fresh_allocation.is_some(),
        "the still-desired retry must have a fresh allocation operation"
    );
    assert!(
        api.effects().await[effects_start..]
            .iter()
            .all(|effect| !matches!(effect, EffectRecord::RemoveWriteRouting))
    );

    finish(&api, 2).await;
    let completed = api.observation().await;
    let authority = &completed.set.status.as_ref().unwrap().authority;
    let receipt = authority.last_scale_up.as_ref().unwrap();
    assert_ne!(
        receipt.intent.target.instance_id,
        old_allocation.observation_target().instance_id
    );
    assert!(
        completed
            .pvcs
            .iter()
            .all(|pvc| pvc.uid().as_deref() != Some(old_pvc_uid.as_str()))
    );
    let expected_primary = authority
        .topology
        .as_ref()
        .and_then(|topology| {
            topology
                .configuration
                .members
                .iter()
                .find(|member| member.role == ReplicaRole::Primary)
        })
        .map(|member| member.identity.clone());
    assert_eq!(
        normalize(completed, BTreeMap::new())
            .unwrap()
            .routing
            .write_target,
        expected_primary
    );
}

#[tokio::test]
async fn active_allocation_frozen_pvc_replacement_abandons_without_adoption() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(1, 2)));
    let old_allocation = active_pvc_only_allocation(&api).await;
    let old_pvc_uid = old_allocation.pvc_uid.as_ref().unwrap().clone();
    let replacement_uid = "replacement-pvc-uid";
    let mut raw = api.observation().await;
    let pvc = raw
        .pvcs
        .iter_mut()
        .find(|pvc| pvc.name_any() == "db-2-data")
        .unwrap();
    pvc.metadata.uid = Some(replacement_uid.into());
    pvc.metadata.resource_version = Some("replacement-pvc-rv".into());
    api.set_observation(raw).await;
    let effects_start = api.effects().await.len();

    let mut fresh_allocation = None;
    for _ in 0..12 {
        let (kind, effects) = tick(&api).await;
        assert_ne!(kind, ReconcileKind::Unsafe);
        assert!(effects.iter().all(|effect| {
            !matches!(
                effect,
                EffectRecord::RemoveWriteRouting | EffectRecord::EnsureScaffolding(_)
            )
        }));
        if let Some(allocation) = api
            .observation()
            .await
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.scale_up_allocation.as_ref())
            .filter(|allocation| allocation.operation_id != old_allocation.operation_id)
        {
            fresh_allocation = Some(allocation.operation_id.clone());
        }
    }

    let blocked = api.observation().await;
    let allocation = blocked
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .scale_up_allocation
        .as_ref()
        .expect("fresh allocation remains pending behind the unrelated PVC");
    assert_eq!(Some(&allocation.operation_id), fresh_allocation.as_ref());
    assert_ne!(allocation.operation_id, old_allocation.operation_id);
    assert!(blocked.pvcs.iter().any(|pvc| {
        pvc.name_any() == "db-2-data" && pvc.uid().as_deref() == Some(replacement_uid)
    }));
    assert!(blocked.pods.iter().all(|pod| pod.name_any() != "db-2"));
    assert!(api.effects().await[effects_start..].iter().all(|effect| {
        !matches!(
            effect,
            EffectRecord::DeleteScaleDownResource {
                resource: ScaleDownResource::Pvc,
                uid,
                ..
            } if uid == replacement_uid || uid == old_pvc_uid.as_str()
        )
    }));

    let mut replacement_removed = blocked;
    replacement_removed
        .pvcs
        .retain(|pvc| pvc.uid().as_deref() != Some(replacement_uid));
    api.set_observation(replacement_removed).await;
    finish(&api, 2).await;
    let completed = api.observation().await;
    assert!(completed.set.status.as_ref().is_some_and(|status| {
        status.authority.scale_up_allocation.is_none() && status.authority.last_scale_up.is_some()
    }));
    assert!(completed.pvcs.iter().all(|pvc| {
        pvc.uid().as_deref() != Some(replacement_uid)
            && pvc.uid().as_deref() != Some(old_pvc_uid.as_str())
    }));
}

#[tokio::test]
async fn pvc_boundary_decrease_increase_finishes_cleanup_before_fresh_attempt() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(1, 2)));
    let old_allocation = loop {
        tick(&api).await;
        let raw = api.observation().await;
        if let Some(allocation) = raw
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.scale_up_allocation.as_ref())
            .filter(|allocation| allocation.pvc_uid.is_some() && allocation.pod_uid.is_none())
        {
            break allocation.clone();
        }
    };
    let old_pvc_uid = old_allocation.pvc_uid.as_ref().unwrap().clone();
    let churn_start = api.effects().await.len();
    let mut reduced = api.observation().await;
    reduced.set.spec.replicas = 1;
    reduced.set.metadata.generation = Some(reduced.set.metadata.generation.unwrap_or_default() + 1);
    api.set_observation(reduced).await;
    tick(&api).await;
    let mut renewed = api.observation().await;
    renewed.set.spec.replicas = 2;
    renewed.set.metadata.generation = Some(renewed.set.metadata.generation.unwrap_or_default() + 1);
    api.set_observation(renewed).await;

    finish(&api, 2).await;
    let completed = api.observation().await;
    let authority = &completed.set.status.as_ref().unwrap().authority;
    let receipt = authority.last_scale_up.as_ref().unwrap();
    assert_ne!(receipt.intent.operation_id, old_allocation.operation_id);
    assert!(
        completed
            .pvcs
            .iter()
            .all(|pvc| pvc.uid().as_deref() != Some(old_pvc_uid.as_str()))
    );
    assert!(
        api.effects().await[churn_start..]
            .iter()
            .all(|effect| !matches!(effect, EffectRecord::RemoveWriteRouting))
    );
}

#[tokio::test]
async fn pod_boundary_decrease_increase_cleans_old_exact_attempt_before_fresh_attempt() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(1, 2)));
    let mut allocation_before_cancel = None;
    for _ in 0..20 {
        tick(&api).await;
        let raw = api.observation().await;
        if let Some(allocation) = raw
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.scale_up_allocation.as_ref())
            .filter(|allocation| {
                allocation.pvc_uid.is_some()
                    && allocation.pod_uid.is_none()
                    && raw.pods.iter().any(|pod| pod.name_any() == "db-2")
            })
        {
            allocation_before_cancel = Some(allocation.clone());
            break;
        }
    }
    let allocation_before_cancel =
        allocation_before_cancel.expect("Pod-created allocation boundary not reached");
    let churn_start = api.effects().await.len();
    let mut reduced = api.observation().await;
    reduced.set.spec.replicas = 1;
    reduced.set.metadata.generation = Some(reduced.set.metadata.generation.unwrap_or_default() + 1);
    api.set_observation(reduced).await;
    tick(&api).await;
    tick(&api).await;
    let frozen = api.observation().await;
    let old_allocation = frozen
        .set
        .status
        .as_ref()
        .unwrap()
        .authority
        .scale_up_allocation
        .as_ref()
        .expect("cancelled allocation remains durable")
        .clone();
    assert_eq!(
        old_allocation.operation_id,
        allocation_before_cancel.operation_id
    );
    let old_pod_uid = old_allocation
        .pod_uid
        .as_ref()
        .expect("cancellation froze exact Pod UID")
        .clone();
    let old_pvc_uid = old_allocation.pvc_uid.as_ref().unwrap().clone();
    let mut renewed = api.observation().await;
    renewed.set.spec.replicas = 2;
    renewed.set.metadata.generation = Some(renewed.set.metadata.generation.unwrap_or_default() + 1);
    api.set_observation(renewed).await;

    finish(&api, 2).await;
    let completed = api.observation().await;
    let authority = &completed.set.status.as_ref().unwrap().authority;
    let member = authority
        .topology
        .as_ref()
        .unwrap()
        .configuration
        .members
        .iter()
        .find(|member| member.identity.replica_id.value() == 2)
        .unwrap();
    assert_ne!(member.identity.instance_id.as_str(), old_pod_uid.as_str());
    assert_ne!(
        authority
            .last_scale_up
            .as_ref()
            .unwrap()
            .intent
            .operation_id,
        old_allocation.operation_id
    );
    assert!(
        completed
            .pvcs
            .iter()
            .all(|pvc| pvc.uid().as_deref() != Some(old_pvc_uid.as_str()))
    );
    assert!(
        api.effects().await[churn_start..]
            .iter()
            .all(|effect| !matches!(effect, EffectRecord::RemoveWriteRouting))
    );
}

#[tokio::test]
async fn pvc_only_cancellation_treats_different_uid_replacement_as_frozen_uid_absence() {
    let api = Arc::new(InMemoryClusterApi::new(fixture(1, 2)));
    let allocation = loop {
        tick(&api).await;
        let raw = api.observation().await;
        if let Some(allocation) = raw
            .set
            .status
            .as_ref()
            .and_then(|status| status.authority.scale_up_allocation.as_ref())
            .filter(|allocation| allocation.pvc_uid.is_some() && allocation.pod_uid.is_none())
        {
            break allocation.clone();
        }
    };
    let frozen_pvc_uid = allocation.pvc_uid.as_ref().unwrap().clone();
    let mut raw = api.observation().await;
    let pvc = raw
        .pvcs
        .iter_mut()
        .find(|pvc| pvc.name_any() == "db-2-data")
        .unwrap();
    pvc.metadata.uid = Some("replacement-pvc-uid".into());
    pvc.metadata.resource_version = Some("replacement-pvc-rv".into());
    raw.set.spec.replicas = 1;
    raw.set.metadata.generation = Some(raw.set.metadata.generation.unwrap_or_default() + 1);
    api.set_observation(raw).await;

    finish(&api, 1).await;
    let cancelled = api.observation().await;
    assert!(
        cancelled
            .set
            .status
            .as_ref()
            .unwrap()
            .authority
            .scale_up_allocation
            .is_none()
    );
    assert!(cancelled.pvcs.iter().any(|pvc| {
        pvc.name_any() == "db-2-data" && pvc.uid().as_deref() == Some("replacement-pvc-uid")
    }));
    assert!(api.effects().await.iter().all(|effect| {
        !matches!(
            effect,
            EffectRecord::DeleteScaleDownResource {
                resource: ScaleDownResource::Pvc,
                uid,
                ..
            } if uid == "replacement-pvc-uid"
        )
    }));
    assert!(api.effects().await.iter().all(|effect| {
        !matches!(
            effect,
            EffectRecord::DeleteScaleDownResource {
                resource: ScaleDownResource::Pvc,
                uid,
                ..
            } if uid == frozen_pvc_uid.as_str()
        )
    }));

    let mut replacement_removed = cancelled;
    replacement_removed
        .pvcs
        .retain(|pvc| pvc.uid().as_deref() != Some("replacement-pvc-uid"));
    replacement_removed.set.spec.replicas = 2;
    replacement_removed.set.metadata.generation = Some(
        replacement_removed
            .set
            .metadata
            .generation
            .unwrap_or_default()
            + 1,
    );
    api.set_observation(replacement_removed).await;
    finish(&api, 2).await;
    assert_ne!(
        api.observation()
            .await
            .set
            .status
            .as_ref()
            .unwrap()
            .authority
            .last_scale_up
            .as_ref()
            .unwrap()
            .intent
            .operation_id,
        allocation.operation_id
    );
}
