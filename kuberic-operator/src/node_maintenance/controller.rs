use async_trait::async_trait;
use k8s_openapi::api::core::v1::{Node, Pod};
use k8s_openapi::jiff::Timestamp;
use std::collections::BTreeSet;

use crate::crd::{KubericSet, Phase, ReconfigurationPhase};

use super::api::{
    MAINTENANCE_FINALIZER, MaintenanceBlockedReason, MaintenanceDesiredState, MaintenancePhase,
    NodeMaintenanceRequest, NodeMaintenanceRequestSpec, NodeMaintenanceRequestStatus,
};
use super::attestation::{
    Attestation, CommittedMember, CommittedTopology, Epoch, LiveMember, LiveObservation, attest,
};
use super::discovery::{
    Discovery, DiscoveryInput, MaintenancePod, NodeRef, finish, reconcile_discovery,
};
use super::preflight::{Preflight, preflight};
use super::release::reconcile_release;
use super::safety::{SetPlacement, reconcile_preparation};

pub const SET_LABEL: &str = "kuberic.io/set";
pub const ROLE_LABEL: &str = "kuberic.io/role";

#[derive(Debug, PartialEq, Clone, Default)]
pub struct SetEvidence {
    pub set_exists: bool,
    pub creation_pending: bool,
    pub committed: Option<CommittedTopology>,
    pub live: Option<LiveObservation>,
}

fn epoch_of(epoch: &crate::crd::EpochStatus) -> Epoch {
    Epoch {
        data_loss_number: epoch.data_loss_number,
        configuration_number: epoch.configuration_number,
    }
}

#[async_trait]
pub trait MaintenanceApi: Send + Sync {
    async fn ensure_request_finalizer(&self, ctx: &RequestContext<'_>) -> Result<bool, String>;

    async fn remove_request_finalizer(&self, ctx: &RequestContext<'_>) -> Result<bool, String>;

    async fn get_node(&self, name: &str) -> Result<Option<NodeRef>, String>;

    async fn list_maintenance_pods(&self) -> Result<Vec<MaintenancePod>, String>;

    async fn get_set_evidence(&self, namespace: &str, name: &str) -> Result<SetEvidence, String>;

    async fn patch_request_status(
        &self,
        ctx: &RequestContext<'_>,
        status: &NodeMaintenanceRequestStatus,
    ) -> Result<(), String>;
}

pub struct ReconcileOutcome {
    pub status: NodeMaintenanceRequestStatus,
    pub persisted: bool,
}

#[derive(Clone, Copy)]
pub struct RequestContext<'a> {
    pub name: &'a str,
    pub uid: &'a str,
    pub resource_version: &'a str,
    pub deleting: bool,
    pub spec: &'a NodeMaintenanceRequestSpec,
    pub generation: Option<i64>,
    pub previous: &'a NodeMaintenanceRequestStatus,
    pub now: Timestamp,
}

pub async fn reconcile_request<A>(
    api: &A,
    ctx: RequestContext<'_>,
) -> Result<ReconcileOutcome, String>
where
    A: MaintenanceApi + ?Sized,
{
    if ctx.previous.phase == MaintenancePhase::Released {
        api.remove_request_finalizer(&ctx).await?;
        return Ok(ReconcileOutcome {
            status: ctx.previous.clone(),
            persisted: false,
        });
    }
    if !ctx.deleting && api.ensure_request_finalizer(&ctx).await? {
        return Ok(ReconcileOutcome {
            status: ctx.previous.clone(),
            persisted: false,
        });
    }
    let mut spec = ctx.spec.clone();
    if ctx.deleting && spec.desired_state == MaintenanceDesiredState::Prepare {
        spec.desired_state = MaintenanceDesiredState::Cancel;
    }
    let mut status = match preflight(&spec, ctx.generation, ctx.previous, ctx.now) {
        Preflight::Settled(status) => status,
        Preflight::Release(status) => {
            let node = api.get_node(&spec.node_name).await?;
            let candidate = reconcile_release(&spec, status.clone(), node.as_ref(), ctx.now);
            if candidate.phase == MaintenancePhase::Released && node.is_some() {
                if let Some((reason, message)) = restoration_blocker(api, &spec, &status).await? {
                    finish(
                        status,
                        MaintenancePhase::Releasing,
                        Some(reason),
                        Some(message),
                        ctx.now,
                    )
                } else {
                    candidate
                }
            } else {
                candidate
            }
        }
        Preflight::Discover => {
            let node = api.get_node(&ctx.spec.node_name).await?;
            let pods = if node.is_some() {
                api.list_maintenance_pods().await?
            } else {
                Vec::new()
            };

            let mut previous = ctx.previous.clone();
            previous
                .preparation_started_at
                .get_or_insert_with(|| ctx.now.to_string());
            let discovered = reconcile_discovery(DiscoveryInput {
                spec: ctx.spec,
                generation: ctx.generation,
                previous: &previous,
                node: node.as_ref(),
                pods: &pods,
                now: ctx.now,
            });

            match discovered {
                Discovery::Blocked(status) => status,
                Discovery::Discovered(status) => {
                    let mut placements = Vec::with_capacity(status.affected_sets.len());
                    for set in &status.affected_sets {
                        let evidence = api.get_set_evidence(&set.namespace, &set.name).await?;
                        placements.push(SetPlacement {
                            committed: evidence.committed,
                            live: evidence.live,
                            promotable_pod_uids: promotable_pod_uids(
                                &pods,
                                &set.namespace,
                                &set.name,
                                &ctx.spec.node_name,
                            ),
                        });
                    }
                    reconcile_preparation(status, &placements, ctx.now)
                }
            }
        }
    };

    if status.phase == MaintenancePhase::Prepared && status.prepared_sets.is_none() {
        status.prepared_sets = Some(status.affected_sets.clone());
    }
    if &status == ctx.previous {
        return Ok(ReconcileOutcome {
            status,
            persisted: false,
        });
    }

    api.patch_request_status(&ctx, &status).await?;
    Ok(ReconcileOutcome {
        status,
        persisted: true,
    })
}

async fn restoration_blocker<A: MaintenanceApi + ?Sized>(
    api: &A,
    spec: &NodeMaintenanceRequestSpec,
    status: &NodeMaintenanceRequestStatus,
) -> Result<Option<(MaintenanceBlockedReason, String)>, String> {
    let pods = api.list_maintenance_pods().await?;
    let prepared_sets = status
        .prepared_sets
        .as_deref()
        .unwrap_or(&status.affected_sets);
    let affected: BTreeSet<_> = status
        .affected_sets
        .iter()
        .chain(prepared_sets)
        .map(|set| (set.namespace.clone(), set.name.clone()))
        .chain(
            pods.iter()
                .filter(|pod| pod.node_name.as_deref() == Some(&spec.node_name))
                .map(|pod| (pod.namespace.clone(), pod.set_name.clone())),
        )
        .collect();
    if spec.desired_state == MaintenanceDesiredState::Complete
        && spec.operation.discards_local_state()
    {
        let old_replicas: BTreeSet<_> = prepared_sets
            .iter()
            .flat_map(|set| &set.replicas)
            .map(|replica| &replica.pod_uid)
            .collect();
        if pods.iter().any(|pod| old_replicas.contains(&pod.uid)) {
            return Ok(Some((MaintenanceBlockedReason::ReplicaRecoveryIncomplete,
                "state-losing maintenance requires rebuilding the recorded replica incarnations; the operator does not delete their storage automatically".to_string())));
        }
    }
    for (namespace, name) in affected {
        let evidence = api.get_set_evidence(&namespace, &name).await?;
        let set_pods: Vec<_> = pods
            .iter()
            .filter(|pod| pod.namespace == namespace && pod.set_name == name)
            .collect();
        if !evidence.set_exists && set_pods.is_empty() {
            continue;
        }
        if evidence.creation_pending
            && (spec.desired_state == MaintenanceDesiredState::Cancel
                || !prepared_sets
                    .iter()
                    .any(|set| set.namespace == namespace && set.name == name))
        {
            continue;
        }
        let attestation = attest(
            evidence.committed.as_ref(),
            evidence.live.as_ref(),
            &BTreeSet::new(),
        );
        if attestation != Attestation::Verified {
            return Ok(Some((
                if attestation == Attestation::QuorumLost {
                    MaintenanceBlockedReason::BlockedByQuorum
                } else {
                    MaintenanceBlockedReason::ConflictingOperation
                },
                format!(
                    "waiting for {namespace}/{name} to attest a settled primary and write quorum before release"
                ),
            )));
        }
        let committed = evidence.committed.as_ref().unwrap();
        let live = evidence.live.as_ref().unwrap();
        if set_pods
            .iter()
            .filter(|pod| pod.node_name.as_deref() == Some(&spec.node_name))
            .any(|pod| {
                !committed
                    .members
                    .iter()
                    .any(|member| member.pod_uid == pod.uid)
                    || !live
                        .members
                        .iter()
                        .any(|member| member.pod_uid == pod.uid && member.healthy)
            })
        {
            return Ok(Some((
                MaintenanceBlockedReason::ReplicaRecoveryIncomplete,
                format!(
                    "waiting for replicas on {} in {namespace}/{name} to rejoin the committed topology",
                    spec.node_name
                ),
            )));
        }
    }
    Ok(None)
}

fn promotable_pod_uids(
    pods: &[MaintenancePod],
    namespace: &str,
    set_name: &str,
    node_name: &str,
) -> BTreeSet<String> {
    pods.iter()
        .filter(|pod| pod.namespace == namespace && pod.set_name == set_name)
        .filter(|pod| {
            pod.node_name
                .as_deref()
                .is_some_and(|node| node != node_name)
        })
        .map(|pod| pod.uid.clone())
        .collect()
}

pub struct KubeMaintenanceApi {
    pub client: kube::Client,
}

fn check_request_identity(
    current: &NodeMaintenanceRequest,
    ctx: &RequestContext<'_>,
) -> Result<(), String> {
    if ctx.uid.is_empty()
        || ctx.resource_version.is_empty()
        || current.metadata.name.as_deref() != Some(ctx.name)
        || current.metadata.uid.as_deref() != Some(ctx.uid)
        || current.metadata.resource_version.as_deref() != Some(ctx.resource_version)
        || current.metadata.generation != ctx.generation
        || current.metadata.deletion_timestamp.is_some() != ctx.deleting
        || current.spec != *ctx.spec
    {
        return Err(
            "maintenance request identity, generation, or resource version changed".to_string(),
        );
    }
    Ok(())
}

fn request_finalizer_patch(
    current: &NodeMaintenanceRequest,
    ctx: &RequestContext<'_>,
    adding: bool,
) -> Result<Option<serde_json::Value>, String> {
    check_request_identity(current, ctx)?;
    if adding && ctx.deleting {
        return Err("cannot add a maintenance finalizer to a deleting request".to_string());
    }
    if !adding
        && current
            .status
            .as_ref()
            .is_none_or(|status| status.phase != MaintenancePhase::Released)
    {
        return Err(
            "placement release must be durable before removing the maintenance finalizer"
                .to_string(),
        );
    }
    let previous = current.metadata.finalizers.clone().unwrap_or_default();
    let mut finalizers = previous.clone();
    if adding {
        if !finalizers
            .iter()
            .any(|value| value == MAINTENANCE_FINALIZER)
        {
            finalizers.push(MAINTENANCE_FINALIZER.to_string());
        }
    } else {
        finalizers.retain(|value| value != MAINTENANCE_FINALIZER);
    }
    if finalizers == previous {
        return Ok(None);
    }
    Ok(Some(serde_json::json!({"metadata": {
        "uid": ctx.uid,
        "resourceVersion": ctx.resource_version,
        "finalizers": finalizers
    }})))
}

impl KubeMaintenanceApi {
    async fn update_finalizer(
        &self,
        ctx: &RequestContext<'_>,
        adding: bool,
    ) -> Result<bool, String> {
        let api: kube::Api<NodeMaintenanceRequest> = kube::Api::all(self.client.clone());
        let current = api.get(ctx.name).await.map_err(|error| error.to_string())?;
        let Some(patch) = request_finalizer_patch(&current, ctx, adding)? else {
            return Ok(false);
        };
        api.patch(
            ctx.name,
            &kube::api::PatchParams::default(),
            &kube::api::Patch::Merge(patch),
        )
        .await
        .map_err(|error| error.to_string())?;
        Ok(true)
    }

    fn pod_to_maintenance_pod(pod: &Pod) -> Option<MaintenancePod> {
        let meta = &pod.metadata;
        let labels = meta.labels.as_ref()?;
        let set_name = labels.get(SET_LABEL)?.clone();
        Some(MaintenancePod {
            namespace: meta.namespace.clone()?,
            name: meta.name.clone()?,
            uid: meta.uid.clone()?,
            node_name: pod.spec.as_ref().and_then(|spec| spec.node_name.clone()),
            set_name,
            is_primary: labels.get(ROLE_LABEL).map(String::as_str) == Some("primary"),
        })
    }
}

#[async_trait]
impl MaintenanceApi for KubeMaintenanceApi {
    async fn ensure_request_finalizer(&self, ctx: &RequestContext<'_>) -> Result<bool, String> {
        self.update_finalizer(ctx, true).await
    }

    async fn remove_request_finalizer(&self, ctx: &RequestContext<'_>) -> Result<bool, String> {
        self.update_finalizer(ctx, false).await
    }

    async fn get_node(&self, name: &str) -> Result<Option<NodeRef>, String> {
        let api: kube::Api<Node> = kube::Api::all(self.client.clone());
        match api.get(name).await {
            Ok(node) => {
                let ready = node.metadata.deletion_timestamp.is_none()
                    && node.status.as_ref().is_some_and(|status| {
                        status.conditions.iter().flatten().any(|condition| {
                            condition.type_ == "Ready" && condition.status == "True"
                        })
                    });
                let uid = node
                    .metadata
                    .uid
                    .ok_or_else(|| format!("node {name} has no uid"))?;
                Ok(Some(NodeRef {
                    name: name.to_string(),
                    uid,
                    ready,
                }))
            }
            Err(kube::Error::Api(ae)) if ae.code == 404 => Ok(None),
            Err(e) => Err(e.to_string()),
        }
    }

    async fn list_maintenance_pods(&self) -> Result<Vec<MaintenancePod>, String> {
        let api: kube::Api<Pod> = kube::Api::all(self.client.clone());
        let params = kube::api::ListParams::default().labels(SET_LABEL);
        let list = api.list(&params).await.map_err(|e| e.to_string())?;
        Ok(list
            .items
            .iter()
            .filter_map(Self::pod_to_maintenance_pod)
            .collect())
    }

    async fn get_set_evidence(&self, namespace: &str, name: &str) -> Result<SetEvidence, String> {
        let api: kube::Api<KubericSet> = kube::Api::namespaced(self.client.clone(), namespace);
        let Some(set) = api.get_opt(name).await.map_err(|e| e.to_string())? else {
            return Ok(SetEvidence::default());
        };
        let Some(status) = set.status else {
            return Ok(SetEvidence {
                set_exists: true,
                creation_pending: true,
                ..Default::default()
            });
        };
        let committed = status
            .stable_snapshot
            .as_ref()
            .map(|snapshot| CommittedTopology {
                epoch: epoch_of(&snapshot.epoch),
                write_quorum: snapshot.write_quorum,
                members: snapshot
                    .members
                    .iter()
                    .map(|member| CommittedMember {
                        id: member.id,
                        pod_uid: member.instance_id.clone(),
                        is_primary: member.id == snapshot.primary_id,
                    })
                    .collect(),
            });
        let primary_pod_uid = status.current_primary.as_deref().and_then(|primary| {
            status
                .members
                .iter()
                .find(|member| member.name == primary)
                .map(|member| member.instance_id.clone())
        });
        let live = LiveObservation {
            epoch: epoch_of(&status.epoch),
            settled: status.phase == Phase::Healthy
                && status.reconfiguration_phase == ReconfigurationPhase::None,
            primary_pod_uid,
            members: status
                .members
                .iter()
                .map(|member| LiveMember {
                    id: member.id,
                    pod_uid: member.instance_id.clone(),
                    is_primary: member.role == "primary",
                    healthy: member.healthy,
                })
                .collect(),
        };
        Ok(SetEvidence {
            set_exists: true,
            creation_pending: matches!(status.phase, Phase::Pending | Phase::Creating),
            committed,
            live: Some(live),
        })
    }

    async fn patch_request_status(
        &self,
        ctx: &RequestContext<'_>,
        status: &NodeMaintenanceRequestStatus,
    ) -> Result<(), String> {
        let api: kube::Api<NodeMaintenanceRequest> = kube::Api::all(self.client.clone());
        let mut current = api.get(ctx.name).await.map_err(|e| e.to_string())?;
        check_request_identity(&current, ctx)?;
        current.status = Some(status.clone());
        api.replace_status(ctx.name, &kube::api::PostParams::default(), &current)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_maintenance::api::{
        MaintenanceBlockedReason, MaintenanceDesiredState, MaintenanceOperation, MaintenancePhase,
        PREPARED_CONDITION_TYPE,
    };
    use std::sync::Mutex;

    const NOW: &str = "2026-09-06T20:00:00Z";

    #[tokio::test]
    async fn drained_requests_retract_prepared_when_surviving_quorum_is_lost() {
        let mut api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-2", Some("worker-04"), false)],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let prepared = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap()
            .status;
        assert_eq!(prepared.phase, MaintenancePhase::Prepared);
        api.pods.clear();
        api.evidence.live.as_mut().unwrap().members[1].healthy = false;
        let drained = run(&api, &prepared).await.unwrap().status;
        assert_eq!(drained.phase, MaintenancePhase::Blocked);
        assert_eq!(
            drained.blocked_reason,
            Some(MaintenanceBlockedReason::BlockedByQuorum)
        );
        assert_eq!(drained.conditions[0].status, "False");
        assert!(drained.prepared_at.is_none());
    }

    #[tokio::test]
    async fn unchanged_prepared_discovery_is_stable_across_clock_ticks() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-2", Some("worker-04"), false)],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let prepared = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap()
            .status;
        let mut request = request_with_identity();
        request.status = Some(prepared.clone());
        let mut ctx = request_context(&request);
        ctx.now = "2026-09-06T20:01:00Z".parse().unwrap();
        let next = reconcile_request(&api, ctx).await.unwrap();
        assert_eq!(next.status, prepared);
        assert!(!next.persisted);
    }

    #[derive(Default)]
    struct MockApi {
        node: Option<NodeRef>,
        pods: Vec<MaintenancePod>,
        evidence: SetEvidence,
        patches: Mutex<Vec<NodeMaintenanceRequestStatus>>,
        node_calls: Mutex<usize>,
        list_calls: Mutex<usize>,
        topology_calls: Mutex<usize>,
        fail_patch: bool,
        fail_get_node: bool,
        missing_finalizer: Mutex<bool>,
        finalizer_additions: Mutex<usize>,
        finalizer_removals: Mutex<usize>,
        fail_finalizer: bool,
    }

    #[async_trait]
    impl MaintenanceApi for MockApi {
        async fn ensure_request_finalizer(
            &self,
            _ctx: &RequestContext<'_>,
        ) -> Result<bool, String> {
            if self.fail_finalizer {
                return Err("finalizer write rejected".to_string());
            }
            let mut missing = self.missing_finalizer.lock().unwrap();
            if !*missing {
                return Ok(false);
            }
            *missing = false;
            *self.finalizer_additions.lock().unwrap() += 1;
            Ok(true)
        }

        async fn remove_request_finalizer(
            &self,
            _ctx: &RequestContext<'_>,
        ) -> Result<bool, String> {
            if self.fail_finalizer {
                return Err("finalizer write rejected".to_string());
            }
            let mut missing = self.missing_finalizer.lock().unwrap();
            if *missing {
                return Ok(false);
            }
            *missing = true;
            *self.finalizer_removals.lock().unwrap() += 1;
            Ok(true)
        }

        async fn get_node(&self, _name: &str) -> Result<Option<NodeRef>, String> {
            *self.node_calls.lock().unwrap() += 1;
            if self.fail_get_node {
                return Err("node read rejected".to_string());
            }
            Ok(self.node.clone())
        }

        async fn list_maintenance_pods(&self) -> Result<Vec<MaintenancePod>, String> {
            *self.list_calls.lock().unwrap() += 1;
            Ok(self.pods.clone())
        }

        async fn get_set_evidence(
            &self,
            _namespace: &str,
            _name: &str,
        ) -> Result<SetEvidence, String> {
            *self.topology_calls.lock().unwrap() += 1;
            Ok(self.evidence.clone())
        }

        async fn patch_request_status(
            &self,
            _ctx: &RequestContext<'_>,
            status: &NodeMaintenanceRequestStatus,
        ) -> Result<(), String> {
            if self.fail_patch {
                return Err("status patch rejected".to_string());
            }
            self.patches.lock().unwrap().push(status.clone());
            Ok(())
        }
    }

    fn spec() -> NodeMaintenanceRequestSpec {
        NodeMaintenanceRequestSpec {
            node_name: "worker-04".to_string(),
            operation: MaintenanceOperation::Reboot,
            desired_state: MaintenanceDesiredState::Prepare,
            provider: Some("Manual".to_string()),
            provider_event_id: Some("event-123".to_string()),
            not_before: None,
            deadline: None,
            release_node_uid: None,
        }
    }

    fn node() -> NodeRef {
        NodeRef {
            name: "worker-04".to_string(),
            uid: "node-uid-a".to_string(),
            ready: true,
        }
    }

    fn pod(name: &str, node_name: Option<&str>, primary: bool) -> MaintenancePod {
        MaintenancePod {
            namespace: "default".to_string(),
            name: name.to_string(),
            uid: format!("uid-{name}"),
            node_name: node_name.map(str::to_string),
            set_name: "kv".to_string(),
            is_primary: primary,
        }
    }

    async fn run(
        api: &MockApi,
        previous: &NodeMaintenanceRequestStatus,
    ) -> Result<ReconcileOutcome, String> {
        run_spec(api, &spec(), previous).await
    }

    async fn run_spec(
        api: &MockApi,
        spec: &NodeMaintenanceRequestSpec,
        previous: &NodeMaintenanceRequestStatus,
    ) -> Result<ReconcileOutcome, String> {
        run_with_deletion(api, spec, previous, false).await
    }

    async fn run_with_deletion(
        api: &MockApi,
        spec: &NodeMaintenanceRequestSpec,
        previous: &NodeMaintenanceRequestStatus,
        deleting: bool,
    ) -> Result<ReconcileOutcome, String> {
        reconcile_request(
            api,
            RequestContext {
                name: "req-1",
                uid: "request-uid",
                resource_version: "1",
                deleting,
                spec,
                generation: Some(1),
                previous,
                now: NOW.parse().expect("timestamp"),
            },
        )
        .await
    }

    fn evidence(primary: &str, members: &[&str], write_quorum: u32) -> SetEvidence {
        let uid = |pod: &str| format!("uid-{pod}");
        let epoch = Epoch {
            data_loss_number: 0,
            configuration_number: 1,
        };
        SetEvidence {
            set_exists: true,
            creation_pending: false,
            committed: Some(CommittedTopology {
                epoch,
                write_quorum,
                members: members
                    .iter()
                    .enumerate()
                    .map(|(index, pod)| CommittedMember {
                        id: index as i64 + 1,
                        pod_uid: uid(pod),
                        is_primary: *pod == primary,
                    })
                    .collect(),
            }),
            live: Some(LiveObservation {
                epoch,
                settled: true,
                primary_pod_uid: Some(uid(primary)),
                members: members
                    .iter()
                    .enumerate()
                    .map(|(index, pod)| LiveMember {
                        id: index as i64 + 1,
                        pod_uid: uid(pod),
                        is_primary: *pod == primary,
                        healthy: true,
                    })
                    .collect(),
            }),
        }
    }

    fn assert_no_discovery(api: &MockApi) {
        assert_eq!(*api.node_calls.lock().unwrap(), 0);
        assert_eq!(*api.list_calls.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn finalizer_is_installed_before_any_preparation_or_release() {
        let api = MockApi {
            node: Some(node()),
            missing_finalizer: Mutex::new(true),
            ..Default::default()
        };
        let previous = NodeMaintenanceRequestStatus::default();
        let first = run(&api, &previous).await.unwrap();
        assert_eq!(first.status, previous);
        assert!(!first.persisted);
        assert_no_discovery(&api);
        assert_eq!(*api.finalizer_additions.lock().unwrap(), 1);
        assert!(api.patches.lock().unwrap().is_empty());
        let prepared = run(&api, &previous).await.unwrap();
        assert_eq!(prepared.status.phase, MaintenancePhase::Prepared);
    }

    #[tokio::test]
    async fn deletion_cancels_but_retains_protection_until_release_is_durable() {
        let mut api = MockApi {
            node: Some(NodeRef {
                ready: false,
                ..node()
            }),
            ..Default::default()
        };
        let previous = NodeMaintenanceRequestStatus {
            phase: MaintenancePhase::Prepared,
            node_uid: Some("node-uid-a".to_string()),
            ..Default::default()
        };
        let releasing = run_with_deletion(&api, &spec(), &previous, true)
            .await
            .unwrap()
            .status;
        assert_eq!(releasing.phase, MaintenancePhase::Releasing);
        assert_eq!(
            releasing.observed_desired_state,
            Some(MaintenanceDesiredState::Cancel)
        );
        let blocked = run_with_deletion(&api, &spec(), &releasing, true)
            .await
            .unwrap()
            .status;
        assert!(blocked.phase.excludes_primary_placement());
        assert_eq!(*api.finalizer_removals.lock().unwrap(), 0);
        api.node.as_mut().unwrap().ready = true;
        let released = run_with_deletion(&api, &spec(), &blocked, true)
            .await
            .unwrap()
            .status;
        assert_eq!(released.phase, MaintenancePhase::Released);
        assert_eq!(*api.finalizer_removals.lock().unwrap(), 0);
        run_with_deletion(&api, &spec(), &released, true)
            .await
            .unwrap();
        assert_eq!(*api.finalizer_removals.lock().unwrap(), 1);
        run_with_deletion(&api, &spec(), &released, true)
            .await
            .unwrap();
        assert_eq!(*api.finalizer_removals.lock().unwrap(), 1);
    }

    fn request_with_identity() -> NodeMaintenanceRequest {
        let mut request = NodeMaintenanceRequest::new("req-1", spec());
        request.metadata.uid = Some("request-uid".to_string());
        request.metadata.resource_version = Some("1".to_string());
        request.metadata.generation = Some(1);
        request.status = Some(NodeMaintenanceRequestStatus::default());
        request
    }

    fn request_context(request: &NodeMaintenanceRequest) -> RequestContext<'_> {
        RequestContext {
            name: request.metadata.name.as_deref().unwrap(),
            uid: request.metadata.uid.as_deref().unwrap(),
            resource_version: request.metadata.resource_version.as_deref().unwrap(),
            deleting: request.metadata.deletion_timestamp.is_some(),
            spec: &request.spec,
            generation: request.metadata.generation,
            previous: request.status.as_ref().unwrap(),
            now: NOW.parse().unwrap(),
        }
    }

    #[test]
    fn stale_reconciliation_cannot_write_status_or_finalizers() {
        let observed = request_with_identity();
        let ctx = request_context(&observed);
        assert!(check_request_identity(&observed, &ctx).is_ok());
        for changed in ["uid", "version", "generation", "spec", "deletion"] {
            let mut current = observed.clone();
            match changed {
                "uid" => current.metadata.uid = Some("replacement-request".to_string()),
                "version" => current.metadata.resource_version = Some("2".to_string()),
                "generation" => current.metadata.generation = Some(2),
                "spec" => current.spec.desired_state = MaintenanceDesiredState::Cancel,
                "deletion" => {
                    current.metadata.deletion_timestamp = Some(
                        k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(ctx.now),
                    )
                }
                _ => unreachable!(),
            }
            assert!(check_request_identity(&current, &ctx).is_err(), "{changed}");
            assert!(
                request_finalizer_patch(&current, &ctx, true).is_err(),
                "{changed}"
            );
        }
    }

    #[test]
    fn finalizer_mutations_are_fenced_preserve_other_owners_and_require_durable_release() {
        let mut current = request_with_identity();
        current.metadata.finalizers = Some(vec!["another.example/finalizer".to_string()]);
        let patch = request_finalizer_patch(&current, &request_context(&current), true)
            .unwrap()
            .unwrap();
        assert_eq!(patch["metadata"]["uid"], "request-uid");
        assert_eq!(patch["metadata"]["resourceVersion"], "1");
        assert_eq!(
            patch["metadata"]["finalizers"],
            serde_json::json!(["another.example/finalizer", MAINTENANCE_FINALIZER])
        );
        current
            .metadata
            .finalizers
            .as_mut()
            .unwrap()
            .push(MAINTENANCE_FINALIZER.to_string());
        assert!(
            request_finalizer_patch(&current, &request_context(&current), true)
                .unwrap()
                .is_none()
        );
        assert!(request_finalizer_patch(&current, &request_context(&current), false).is_err());
        current.status.as_mut().unwrap().phase = MaintenancePhase::Released;
        let patch = request_finalizer_patch(&current, &request_context(&current), false)
            .unwrap()
            .unwrap();
        assert_eq!(
            patch["metadata"]["finalizers"],
            serde_json::json!(["another.example/finalizer"])
        );
    }

    #[tokio::test]
    async fn an_expired_request_does_not_depend_on_reading_the_node() {
        let api = MockApi {
            fail_get_node: true,
            pods: vec![pod("kv-0", Some("worker-04"), true)],
            ..Default::default()
        };
        let spec = NodeMaintenanceRequestSpec {
            deadline: Some("2026-09-06T19:00:00Z".to_string()),
            ..spec()
        };
        let outcome = run_spec(&api, &spec, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Expired);
        assert_eq!(
            outcome.status.blocked_reason,
            Some(MaintenanceBlockedReason::DeadlineExceeded)
        );
        assert_no_discovery(&api);
    }

    #[tokio::test]
    async fn a_terminal_request_does_not_perform_discovery() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-0", Some("worker-04"), true)],
            ..Default::default()
        };
        let previous = NodeMaintenanceRequestStatus {
            phase: MaintenancePhase::Released,
            observed_generation: Some(1),
            observed_desired_state: Some(MaintenanceDesiredState::Prepare),
            ..Default::default()
        };
        let outcome = run(&api, &previous).await.unwrap();

        assert_eq!(outcome.status, previous);
        assert!(!outcome.persisted);
        assert_no_discovery(&api);
    }

    #[tokio::test]
    async fn release_is_persisted_before_health_checks_and_duplicate_delivery_is_a_noop() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-0", Some("worker-04"), true)],
            evidence: evidence("kv-0", &["kv-0"], 1),
            ..Default::default()
        };
        let spec = NodeMaintenanceRequestSpec {
            desired_state: MaintenanceDesiredState::Complete,
            ..spec()
        };
        let previous = NodeMaintenanceRequestStatus {
            phase: MaintenancePhase::Prepared,
            observed_generation: Some(1),
            ..Default::default()
        };
        let outcome = run_spec(&api, &spec, &previous).await.unwrap();

        assert_eq!(
            outcome.status.observed_desired_state,
            Some(MaintenanceDesiredState::Complete)
        );
        assert_eq!(outcome.status.phase, MaintenancePhase::Releasing);
        assert_eq!(outcome.status.conditions[0].status, "False");
        assert_no_discovery(&api);

        let released = run_spec(&api, &spec, &outcome.status).await.unwrap();
        assert_eq!(released.status.phase, MaintenancePhase::Released);
        assert_eq!(
            released.status.released_node_uid.as_deref(),
            Some("node-uid-a")
        );
        assert_eq!(*api.node_calls.lock().unwrap(), 1);
        assert_eq!(*api.list_calls.lock().unwrap(), 1);
        let repeat = run_spec(&api, &spec, &released.status).await.unwrap();
        assert!(!repeat.persisted);
        assert_eq!(api.patches.lock().unwrap().len(), 2);
        assert_eq!(*api.node_calls.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn release_resumes_from_durable_status_after_restart() {
        let spec = NodeMaintenanceRequestSpec {
            desired_state: MaintenanceDesiredState::Cancel,
            ..spec()
        };
        let original = MockApi::default();
        let previous = NodeMaintenanceRequestStatus {
            phase: MaintenancePhase::Preparing,
            node_uid: Some("node-uid-a".to_string()),
            ..Default::default()
        };
        let releasing = run_spec(&original, &spec, &previous).await.unwrap().status;
        let persisted = serde_json::from_value(serde_json::to_value(releasing).unwrap()).unwrap();
        let restarted = MockApi {
            node: Some(node()),
            ..Default::default()
        };
        let resumed = run_spec(&restarted, &spec, &persisted).await.unwrap();
        assert_eq!(resumed.status.phase, MaintenancePhase::Released);
        assert_eq!(resumed.status.node_uid, previous.node_uid);
        assert_eq!(resumed.status.released_at.as_deref(), Some(NOW));
    }

    #[tokio::test]
    async fn release_waits_for_topology_and_returning_replica_health() {
        let mut api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-2", Some("worker-04"), false)],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let prepared = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap()
            .status;
        let spec = NodeMaintenanceRequestSpec {
            desired_state: MaintenanceDesiredState::Complete,
            ..spec()
        };
        let releasing = run_spec(&api, &spec, &prepared).await.unwrap().status;
        api.evidence.live.as_mut().unwrap().settled = false;
        let blocked = run_spec(&api, &spec, &releasing).await.unwrap().status;
        assert_eq!(
            blocked.blocked_reason,
            Some(MaintenanceBlockedReason::ConflictingOperation)
        );
        assert!(blocked.phase.excludes_primary_placement());
        api.evidence.live.as_mut().unwrap().settled = true;
        api.evidence.live.as_mut().unwrap().members[2].healthy = false;
        let blocked = run_spec(&api, &spec, &blocked).await.unwrap().status;
        assert_eq!(
            blocked.blocked_reason,
            Some(MaintenanceBlockedReason::ReplicaRecoveryIncomplete)
        );
        api.evidence.live.as_mut().unwrap().members[2].healthy = true;
        let released = run_spec(&api, &spec, &blocked).await.unwrap().status;
        assert_eq!(released.phase, MaintenancePhase::Released);
    }

    #[tokio::test]
    async fn cancellation_can_release_creation_paused_by_the_same_request() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-0", Some("worker-04"), false)],
            evidence: SetEvidence {
                set_exists: true,
                creation_pending: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let preparing = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap()
            .status;
        assert_ne!(preparing.phase, MaintenancePhase::Prepared);
        let canceled_spec = NodeMaintenanceRequestSpec {
            desired_state: MaintenanceDesiredState::Cancel,
            ..spec()
        };
        let releasing = run_spec(&api, &canceled_spec, &preparing)
            .await
            .unwrap()
            .status;
        let released = run_spec(&api, &canceled_spec, &releasing)
            .await
            .unwrap()
            .status;
        assert_eq!(released.phase, MaintenancePhase::Released);
        let complete_spec = NodeMaintenanceRequestSpec {
            desired_state: MaintenanceDesiredState::Complete,
            ..spec()
        };
        let mut previously_prepared = releasing.clone();
        previously_prepared.prepared_sets = Some(preparing.affected_sets);
        let blocked = run_spec(&api, &complete_spec, &previously_prepared)
            .await
            .unwrap()
            .status;
        assert_eq!(blocked.phase, MaintenancePhase::Releasing);
        assert_eq!(
            blocked.blocked_reason,
            Some(MaintenanceBlockedReason::ConflictingOperation)
        );
        previously_prepared.prepared_sets = Some(Vec::new());
        let new_workload = run_spec(&api, &complete_spec, &previously_prepared)
            .await
            .unwrap()
            .status;
        assert_eq!(new_workload.phase, MaintenancePhase::Released);
    }

    #[tokio::test]
    async fn reimage_requires_new_replica_incarnations_but_cancellation_does_not() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-2", Some("worker-04"), false)],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let prepared = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap()
            .status;
        let mut spec = NodeMaintenanceRequestSpec {
            operation: MaintenanceOperation::Reimage,
            desired_state: MaintenanceDesiredState::Complete,
            ..spec()
        };
        let releasing = run_spec(&api, &spec, &prepared).await.unwrap().status;
        let blocked = run_spec(&api, &spec, &releasing).await.unwrap().status;
        assert_eq!(
            blocked.blocked_reason,
            Some(MaintenanceBlockedReason::ReplicaRecoveryIncomplete)
        );
        spec.desired_state = MaintenanceDesiredState::Cancel;
        let canceled = run_spec(&api, &spec, &releasing).await.unwrap().status;
        assert_eq!(canceled.phase, MaintenancePhase::Released);
        let rebuilt = MockApi {
            node: Some(node()),
            pods: vec![pod("rebuilt-2", Some("worker-04"), false)],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "rebuilt-2"], 2),
            ..Default::default()
        };
        spec.desired_state = MaintenanceDesiredState::Complete;
        let released = run_spec(&rebuilt, &spec, &blocked).await.unwrap().status;
        assert_eq!(released.phase, MaintenancePhase::Released);
    }

    #[tokio::test]
    async fn deletion_cannot_downgrade_completion_to_bypass_reimage_recovery() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-2", Some("worker-04"), false)],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let mut spec = NodeMaintenanceRequestSpec {
            operation: MaintenanceOperation::Reimage,
            ..spec()
        };
        let prepared = run_spec(&api, &spec, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap()
            .status;
        spec.desired_state = MaintenanceDesiredState::Complete;
        let releasing = run_spec(&api, &spec, &prepared).await.unwrap().status;
        let deleting = run_with_deletion(&api, &spec, &releasing, true)
            .await
            .unwrap()
            .status;
        assert_eq!(deleting.phase, MaintenancePhase::Releasing);
        assert_eq!(
            deleting.observed_desired_state,
            Some(MaintenanceDesiredState::Complete)
        );
        assert_eq!(
            deleting.blocked_reason,
            Some(MaintenanceBlockedReason::ReplicaRecoveryIncomplete)
        );
        assert_eq!(*api.finalizer_removals.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn drained_and_rebuilt_pods_do_not_replace_the_original_recovery_inventory() {
        let mut api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-2", Some("worker-04"), false)],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let mut spec = NodeMaintenanceRequestSpec {
            operation: MaintenanceOperation::Reimage,
            ..spec()
        };
        let prepared = run_spec(&api, &spec, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap()
            .status;
        let inventory = prepared.prepared_sets.clone();
        api.pods.clear();
        let drained = run_spec(&api, &spec, &prepared).await.unwrap().status;
        assert_eq!(drained.affected_sets.len(), 1);
        assert_eq!(drained.affected_sets[0].replicas[0].pod_uid, "uid-kv-2");
        assert_eq!(drained.prepared_sets, inventory);
        api.evidence.live.as_mut().unwrap().settled = false;
        spec.desired_state = MaintenanceDesiredState::Complete;
        let releasing = run_spec(&api, &spec, &drained).await.unwrap().status;
        let blocked = run_spec(&api, &spec, &releasing).await.unwrap().status;
        assert_eq!(
            blocked.blocked_reason,
            Some(MaintenanceBlockedReason::ConflictingOperation)
        );
        api.pods = vec![pod("rebuilt-2", Some("worker-04"), false)];
        api.evidence = evidence("kv-0", &["kv-0", "kv-1", "rebuilt-2"], 2);
        let released = run_spec(&api, &spec, &blocked).await.unwrap().status;
        assert_eq!(released.phase, MaintenancePhase::Released);
        assert_eq!(released.prepared_sets, inventory);
    }

    #[tokio::test]
    async fn failed_release_status_write_preserves_exclusion_and_does_not_read_node() {
        let api = MockApi {
            fail_patch: true,
            ..Default::default()
        };
        let spec = NodeMaintenanceRequestSpec {
            desired_state: MaintenanceDesiredState::Cancel,
            ..spec()
        };
        let previous = NodeMaintenanceRequestStatus {
            phase: MaintenancePhase::Prepared,
            ..Default::default()
        };
        assert!(run_spec(&api, &spec, &previous).await.is_err());
        assert!(previous.phase.excludes_primary_placement());
        assert!(api.patches.lock().unwrap().is_empty());
        assert_no_discovery(&api);
    }

    #[tokio::test]
    async fn a_secondary_only_node_is_prepared_when_quorum_survives() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-2", Some("worker-04"), false)],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Prepared);
        assert!(outcome.status.prepared_at.is_some());
        assert!(outcome.status.affected_sets[0].primary_moved);
        assert!(outcome.status.affected_sets[0].quorum_without_node);

        let condition = outcome
            .status
            .conditions
            .iter()
            .find(|condition| condition.type_ == PREPARED_CONDITION_TYPE)
            .expect("prepared condition");
        assert_eq!(condition.status, "True");
        assert_eq!(condition.reason, "PrimariesMovedAndQuorumVerified");
    }

    #[tokio::test]
    async fn an_unhealthy_survivor_is_never_reported_as_prepared() {
        let mut evidence = evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2);
        evidence
            .live
            .as_mut()
            .expect("live")
            .members
            .iter_mut()
            .find(|member| member.pod_uid == "uid-kv-1")
            .expect("member")
            .healthy = false;
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-2", Some("worker-04"), false)],
            evidence,
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_ne!(outcome.status.phase, MaintenancePhase::Prepared);
        assert_eq!(
            outcome.status.blocked_reason,
            Some(MaintenanceBlockedReason::BlockedByQuorum)
        );
        assert!(!outcome.status.affected_sets[0].quorum_without_node);
    }

    #[tokio::test]
    async fn a_reconfiguring_set_is_never_reported_as_prepared() {
        let mut evidence = evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2);
        evidence.live.as_mut().expect("live").settled = false;
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-2", Some("worker-04"), false)],
            evidence,
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Preparing);
        assert!(outcome.status.prepared_at.is_none());
        assert!(!outcome.status.affected_sets[0].quorum_without_node);
    }

    #[tokio::test]
    async fn a_replaced_incarnation_is_never_reported_as_prepared() {
        let mut evidence = evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2);
        evidence
            .live
            .as_mut()
            .expect("live")
            .members
            .iter_mut()
            .find(|member| member.pod_uid == "uid-kv-1")
            .expect("member")
            .pod_uid = "uid-kv-1-restarted".to_string();
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-2", Some("worker-04"), false)],
            evidence,
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Preparing);
        assert!(outcome.status.prepared_at.is_none());
    }

    #[tokio::test]
    async fn losing_write_quorum_blocks_preparation() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![
                pod("kv-1", Some("worker-04"), false),
                pod("kv-2", Some("worker-04"), false),
            ],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Blocked);
        assert_eq!(
            outcome.status.blocked_reason,
            Some(MaintenanceBlockedReason::BlockedByQuorum)
        );
        assert!(outcome.status.prepared_at.is_none());
    }

    #[tokio::test]
    async fn a_primary_on_the_node_waits_while_a_replica_elsewhere_can_take_over() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![
                pod("kv-0", Some("worker-04"), true),
                pod("kv-1", Some("worker-05"), false),
                pod("kv-2", Some("worker-06"), false),
            ],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Preparing);
        assert!(!outcome.status.affected_sets[0].primary_moved);
        assert!(!outcome.status.affected_sets[0].no_eligible_target);
        assert!(outcome.status.affected_sets[0].quorum_without_node);
    }

    #[tokio::test]
    async fn a_primary_with_no_scheduled_replica_elsewhere_is_blocked() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![
                pod("kv-0", Some("worker-04"), true),
                pod("kv-1", None, false),
                pod("kv-2", None, false),
            ],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Blocked);
        assert_eq!(
            outcome.status.blocked_reason,
            Some(MaintenanceBlockedReason::NoEligibleTarget)
        );
        assert!(outcome.status.affected_sets[0].no_eligible_target);
        assert!(outcome.status.prepared_at.is_none());
    }

    #[tokio::test]
    async fn a_request_is_never_prepared_while_a_primary_remains_on_the_node() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-0", Some("worker-04"), true)],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        let condition = outcome
            .status
            .conditions
            .iter()
            .find(|condition| condition.type_ == PREPARED_CONDITION_TYPE)
            .expect("prepared condition");
        assert_eq!(condition.status, "False");
    }

    #[tokio::test]
    async fn an_unpublished_topology_never_reports_readiness() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-2", Some("worker-04"), false)],
            evidence: SetEvidence::default(),
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Preparing);
        assert!(!outcome.status.affected_sets[0].quorum_without_node);
    }

    #[tokio::test]
    async fn a_duplicate_delivery_does_not_rewrite_a_prepared_request() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-2", Some("worker-04"), false)],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let first = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();
        assert!(first.persisted);

        let second = run(&api, &first.status).await.unwrap();
        assert!(!second.persisted);
        assert_eq!(second.status, first.status);
        assert_eq!(api.patches.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn preparation_resumes_from_persisted_status_after_a_restart() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-2", Some("worker-04"), false)],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let persisted = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap()
            .status;

        let restarted = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-2", Some("worker-04"), false)],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let resumed = run(&restarted, &persisted).await.unwrap();

        assert_eq!(resumed.status.phase, MaintenancePhase::Prepared);
        assert_eq!(resumed.status.prepared_at, persisted.prepared_at);
        assert!(!resumed.persisted);
    }

    #[tokio::test]
    async fn a_primary_that_moves_away_advances_the_request_to_prepared() {
        let blocked = MockApi {
            node: Some(node()),
            pods: vec![
                pod("kv-0", Some("worker-04"), true),
                pod("kv-1", Some("worker-05"), false),
                pod("kv-2", Some("worker-06"), false),
            ],
            evidence: evidence("kv-0", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let preparing = run(&blocked, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap()
            .status;
        assert_eq!(preparing.phase, MaintenancePhase::Preparing);

        let moved = MockApi {
            node: Some(node()),
            pods: vec![
                pod("kv-0", Some("worker-04"), false),
                pod("kv-1", Some("worker-05"), true),
                pod("kv-2", Some("worker-06"), false),
            ],
            evidence: evidence("kv-1", &["kv-0", "kv-1", "kv-2"], 2),
            ..Default::default()
        };
        let outcome = run(&moved, &preparing).await.unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Prepared);
        assert!(outcome.status.affected_sets[0].primary_moved);
        assert!(outcome.status.prepared_at.is_some());
    }

    #[tokio::test]
    async fn a_node_without_replicas_is_prepared_without_a_topology_read() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-0", Some("worker-09"), true)],
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Prepared);
        assert!(outcome.status.affected_sets.is_empty());
        assert_eq!(*api.topology_calls.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn a_request_before_its_window_does_not_list_pods() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-0", Some("worker-04"), true)],
            ..Default::default()
        };
        let spec = NodeMaintenanceRequestSpec {
            not_before: Some("2026-09-06T21:00:00Z".to_string()),
            ..spec()
        };
        let outcome = run_spec(&api, &spec, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Requested);
        assert_no_discovery(&api);
    }

    #[tokio::test]
    async fn an_invalid_deadline_blocks_without_discovery() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-0", Some("worker-04"), true)],
            ..Default::default()
        };
        let spec = NodeMaintenanceRequestSpec {
            deadline: Some("tomorrow".to_string()),
            ..spec()
        };
        let outcome = run_spec(&api, &spec, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Blocked);
        assert_eq!(
            outcome.status.blocked_reason,
            Some(MaintenanceBlockedReason::InvalidDeadline)
        );
        assert_no_discovery(&api);
    }

    #[tokio::test]
    async fn discovery_is_persisted_through_the_status_subresource() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-0", Some("worker-04"), true)],
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert!(outcome.persisted);
        assert_eq!(outcome.status.phase, MaintenancePhase::Preparing);
        assert_eq!(outcome.status.node_uid.as_deref(), Some("node-uid-a"));

        let patches = api.patches.lock().unwrap();
        assert_eq!(patches.len(), 1);
        assert_eq!(patches[0].affected_sets.len(), 1);
        assert!(patches[0].affected_sets[0].hosts_primary);
    }

    #[tokio::test]
    async fn unchanged_status_is_not_rewritten() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-0", Some("worker-04"), false)],
            ..Default::default()
        };
        let first = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();
        assert!(first.persisted);

        let second = run(&api, &first.status).await.unwrap();
        assert!(!second.persisted);
        assert_eq!(second.status, first.status);
        assert_eq!(api.patches.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn missing_node_blocks_without_listing_pods() {
        let api = MockApi {
            node: None,
            pods: vec![pod("kv-0", Some("worker-04"), true)],
            ..Default::default()
        };
        let outcome = run(&api, &NodeMaintenanceRequestStatus::default())
            .await
            .unwrap();

        assert_eq!(outcome.status.phase, MaintenancePhase::Blocked);
        assert_eq!(
            outcome.status.blocked_reason,
            Some(MaintenanceBlockedReason::NodeNotFound)
        );
        assert_eq!(*api.list_calls.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn status_patch_failure_is_reported() {
        let api = MockApi {
            node: Some(node()),
            pods: vec![pod("kv-0", Some("worker-04"), false)],
            fail_patch: true,
            ..Default::default()
        };
        let result = run(&api, &NodeMaintenanceRequestStatus::default()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn pod_conversion_requires_the_set_label() {
        let mut pod = Pod::default();
        pod.metadata.name = Some("kv-0".to_string());
        pod.metadata.namespace = Some("default".to_string());
        pod.metadata.uid = Some("uid-kv-0".to_string());
        assert!(KubeMaintenanceApi::pod_to_maintenance_pod(&pod).is_none());

        pod.metadata.labels = Some(
            [(SET_LABEL.to_string(), "kv".to_string())]
                .into_iter()
                .collect(),
        );
        let converted = KubeMaintenanceApi::pod_to_maintenance_pod(&pod).expect("converted");
        assert_eq!(converted.set_name, "kv");
        assert!(!converted.is_primary);
        assert_eq!(converted.node_name, None);
    }

    #[tokio::test]
    async fn pod_conversion_reads_node_and_primary_role() {
        let mut pod = Pod::default();
        pod.metadata.name = Some("kv-1".to_string());
        pod.metadata.namespace = Some("default".to_string());
        pod.metadata.uid = Some("uid-kv-1".to_string());
        pod.metadata.labels = Some(
            [
                (SET_LABEL.to_string(), "kv".to_string()),
                (ROLE_LABEL.to_string(), "primary".to_string()),
            ]
            .into_iter()
            .collect(),
        );
        pod.spec = Some(k8s_openapi::api::core::v1::PodSpec {
            node_name: Some("worker-04".to_string()),
            ..Default::default()
        });

        let converted = KubeMaintenanceApi::pod_to_maintenance_pod(&pod).expect("converted");
        assert_eq!(converted.node_name.as_deref(), Some("worker-04"));
        assert!(converted.is_primary);
    }
}
