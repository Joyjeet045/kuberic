use std::collections::BTreeMap;
use std::sync::{Arc as StdArc, Mutex as StdMutex};

use async_trait::async_trait;
use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod, Service};
use kube::api::ObjectMeta;
use kuberic_core::driver::ReplicaHandle;
use kuberic_core::error::KubericError;
use kuberic_core::replica_lifecycle::REPLICA_LIFECYCLE_PEER_PROTOCOL_VERSION;
use kuberic_core::types::{
    AccessStatus, AgentControlVersion, AgentGeneration, CorrelatedControlActionAcknowledgement,
    CorrelatedControlActionRequest, Epoch, ReplicaAgentStatus, ReplicaConfigurationMemberStatus,
    ReplicaConfigurationMode, ReplicaConfigurationStatus, ReplicaConnectionStatus, ReplicaId,
    ReplicaInstanceId, ReplicaStatusInfo, Role,
};
use kuberic_durable_execution::{
    CasOutcome, CheckpointEnvelope, CheckpointPayload, CheckpointStore, ExactBytes,
    ExecutionContract, InMemoryCheckpointStore, TerminalOutcome,
};

use super::*;
use crate::crd::{
    EpochStatus, MemberStatus, PvcRetentionPolicy, RemoveReplicaCleanupStatus,
    RemoveReplicaCommitEvidenceStatus, StableReplicaRoleStatus, StableReplicaSnapshotStatus,
    TargetRetirementObservationStatus,
};
use crate::durable::remove_replica_execution::{
    REMOVE_REPLICA_MAX_ACTIVE_ENCODED_BYTES, REMOVE_REPLICA_MAX_TERMINAL_ENCODED_BYTES,
    RemoveReplicaActivityAccounting, RemoveReplicaExecution, RemoveReplicaTerminal,
    checkpoint_limits, execution_spec, new_execution, reconstruct_initial_operation,
};

#[derive(Default)]
struct RoutingApi {
    pods: StdMutex<Vec<Pod>>,
    statuses: StdMutex<Vec<KubericSetStatus>>,
    fail_status_patch: StdMutex<bool>,
    replica_statuses: StdArc<StdMutex<BTreeMap<ReplicaId, ReplicaStatusInfo>>>,
    status_failures: StdArc<StdMutex<BTreeMap<ReplicaId, StatusFailure>>>,
}

impl RoutingApi {
    fn new(pods: Vec<Pod>) -> Self {
        Self {
            pods: StdMutex::new(pods),
            replica_statuses: StdArc::new(StdMutex::new(replica_statuses())),
            ..Self::default()
        }
    }

    fn fail_next_status_patch(&self) {
        *self.fail_status_patch.lock().unwrap() = true;
    }

    fn last_status(&self) -> Option<KubericSetStatus> {
        self.statuses.lock().unwrap().last().cloned()
    }

    fn fail_status(&self, replica_id: ReplicaId, failure: StatusFailure) {
        self.status_failures
            .lock()
            .unwrap()
            .insert(replica_id, failure);
    }
}

#[derive(Clone, Copy)]
enum StatusFailure {
    Unavailable,
    Malformed,
}

struct RoutingHandle {
    replica_id: ReplicaId,
    statuses: StdArc<StdMutex<BTreeMap<ReplicaId, ReplicaStatusInfo>>>,
    failures: StdArc<StdMutex<BTreeMap<ReplicaId, StatusFailure>>>,
}

#[async_trait]
impl ReplicaHandle for RoutingHandle {
    fn id(&self) -> ReplicaId {
        self.replica_id
    }

    fn instance_id(&self) -> ReplicaInstanceId {
        self.statuses
            .lock()
            .unwrap()
            .get(&self.replica_id)
            .unwrap()
            .instance_id
            .clone()
    }

    fn current_progress(&self) -> i64 {
        10
    }

    fn catch_up_capability(&self) -> i64 {
        10
    }

    fn control_address(&self) -> String {
        format!("http://{}:9090", self.instance_id())
    }

    fn replicator_address(&self) -> String {
        format!("http://{}:9091", self.instance_id())
    }

    async fn get_status(&self) -> kuberic_core::Result<ReplicaStatusInfo> {
        if let Some(failure) = self.failures.lock().unwrap().get(&self.replica_id) {
            return Err(match failure {
                StatusFailure::Unavailable => {
                    KubericError::Internal("injected unavailable status".into())
                }
                StatusFailure::Malformed => KubericError::RemoteAgentRequestRejected(
                    "injected malformed replica-agent status".to_string(),
                ),
            });
        }
        Ok(self
            .statuses
            .lock()
            .unwrap()
            .get(&self.replica_id)
            .unwrap()
            .clone())
    }

    async fn execute_correlated_control_action(
        &self,
        _: CorrelatedControlActionRequest,
    ) -> kuberic_core::Result<CorrelatedControlActionAcknowledgement> {
        panic!("authority-gap tests must not dispatch effects")
    }
}

#[async_trait]
impl ClusterApi for RoutingApi {
    async fn list_pods(&self, _: &str, _: &str) -> Result<Vec<Pod>, String> {
        Ok(self.pods.lock().unwrap().clone())
    }

    async fn create_pod(&self, _: &str, _: &Pod) -> Result<(), String> {
        Err("unexpected pod creation".to_string())
    }

    async fn delete_pod(&self, _: &str, _: &str, _: &str) -> Result<(), String> {
        Err("terminal reload must not delete a pod".to_string())
    }

    async fn patch_pod_labels(
        &self,
        _: &str,
        _: &str,
        _: BTreeMap<String, String>,
    ) -> Result<(), String> {
        Err("terminal reload must not patch pod labels".to_string())
    }

    async fn patch_pod_labels_if_uid(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: BTreeMap<String, String>,
    ) -> Result<(), String> {
        Err("terminal reload must not patch pod labels".to_string())
    }

    async fn patch_set_status(
        &self,
        _: &str,
        _: &str,
        status: &KubericSetStatus,
        _: Option<&str>,
    ) -> Result<(), String> {
        if std::mem::take(&mut *self.fail_status_patch.lock().unwrap()) {
            return Err("resource version conflict".to_string());
        }
        self.statuses.lock().unwrap().push(status.clone());
        Ok(())
    }

    async fn create_replica_handle(
        &self,
        replica_id: ReplicaId,
        _: &Pod,
        _: &KubericSetSpec,
    ) -> Result<Box<dyn ReplicaHandle>, String> {
        Ok(Box::new(RoutingHandle {
            replica_id,
            statuses: self.replica_statuses.clone(),
            failures: self.status_failures.clone(),
        }))
    }

    async fn get_pvc(&self, _: &str, _: &str) -> Result<PersistentVolumeClaim, String> {
        Err("unused".to_string())
    }

    async fn create_pvc(&self, _: &str, _: &PersistentVolumeClaim) -> Result<(), String> {
        Err("unused".to_string())
    }

    async fn list_pvcs(&self, _: &str, _: &str) -> Result<Vec<PersistentVolumeClaim>, String> {
        Err("unused".to_string())
    }

    async fn delete_pvc(&self, _: &str, _: &str) -> Result<(), String> {
        Err("unused".to_string())
    }

    async fn get_service(&self, _: &str, _: &str) -> Result<Service, String> {
        Err("unused".to_string())
    }

    async fn create_service(&self, _: &str, _: &Service) -> Result<(), String> {
        Err("unused".to_string())
    }

    async fn delete_service(&self, _: &str, _: &str) -> Result<(), String> {
        Err("unused".to_string())
    }
}

fn generation(id: i64) -> AgentGeneration {
    AgentGeneration::parse(format!("{id:032x}")).unwrap()
}

fn replica_statuses() -> BTreeMap<ReplicaId, ReplicaStatusInfo> {
    BTreeMap::from([
        (
            1,
            replica_status(
                1,
                "one",
                Role::Primary,
                Some(ReplicaConfigurationStatus {
                    mode: ReplicaConfigurationMode::Current,
                    members: vec![
                        ReplicaConfigurationMemberStatus {
                            id: 2,
                            instance_id: ReplicaInstanceId::new("two"),
                            role: Role::ActiveSecondary,
                        },
                        ReplicaConfigurationMemberStatus {
                            id: 3,
                            instance_id: ReplicaInstanceId::new("three"),
                            role: Role::ActiveSecondary,
                        },
                    ],
                    write_quorum: 2,
                }),
            ),
        ),
        (2, replica_status(2, "two", Role::ActiveSecondary, None)),
        (3, replica_status(3, "three", Role::ActiveSecondary, None)),
    ])
}

fn replica_status(
    id: ReplicaId,
    uid: &str,
    role: Role,
    configuration: Option<ReplicaConfigurationStatus>,
) -> ReplicaStatusInfo {
    ReplicaStatusInfo {
        instance_id: ReplicaInstanceId::new(uid),
        role,
        epoch: Epoch::new(1, 7),
        current_progress: 10,
        catch_up_capability: Some(10),
        committed_lsn: 10,
        healthy: true,
        write_status: if role == Role::Primary {
            AccessStatus::Granted
        } else {
            AccessStatus::NotPrimary
        },
        configuration,
        election_configuration: None,
        deactivation_info: None,
        active_replica_connections: if role == Role::Primary {
            vec![
                ReplicaConnectionStatus {
                    id: 2,
                    instance_id: ReplicaInstanceId::new("two"),
                },
                ReplicaConnectionStatus {
                    id: 3,
                    instance_id: ReplicaInstanceId::new("three"),
                },
            ]
        } else {
            Vec::new()
        },
        build_observation: None,
        agent: ReplicaAgentStatus {
            protocol_version: kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION,
            lifecycle_peer_protocol_version: REPLICA_LIFECYCLE_PEER_PROTOCOL_VERSION,
            generation: generation(id),
            control_version: AgentControlVersion::new(11),
            current_action: None,
            retained_terminal_actions: Vec::new(),
            local_faults: Vec::new(),
        },
    }
}

fn snapshot() -> StablePartitionSnapshotStatus {
    StablePartitionSnapshotStatus {
        epoch: EpochStatus {
            data_loss_number: 1,
            configuration_number: 7,
        },
        primary_id: 1,
        members: vec![
            StableReplicaSnapshotStatus {
                id: 1,
                instance_id: "one".to_string(),
                role: StableReplicaRoleStatus::Primary,
                election_metadata: None,
            },
            StableReplicaSnapshotStatus {
                id: 2,
                instance_id: "two".to_string(),
                role: StableReplicaRoleStatus::ActiveSecondary,
                election_metadata: None,
            },
            StableReplicaSnapshotStatus {
                id: 3,
                instance_id: "three".to_string(),
                role: StableReplicaRoleStatus::ActiveSecondary,
                election_metadata: None,
            },
        ],
        write_quorum: 2,
    }
}

fn reference() -> RemoveReplicaExecution {
    new_execution(
        "set-uid",
        snapshot(),
        RemoveReplicaTarget {
            replica_id: 3,
            pod_name: "set-2".to_string(),
            pod_uid: "three".to_string(),
            replicator_address: "http://three:9091".to_string(),
            agent_generation: Some(generation(3)),
        },
        DurableRemoveMode::ScaleDown,
        2,
        10,
    )
    .unwrap()
}

fn pod(id: i64, uid: &str, role: &str) -> Pod {
    Pod {
        metadata: ObjectMeta {
            name: Some(format!("set-{}", id - 1)),
            namespace: Some("default".to_string()),
            uid: Some(uid.to_string()),
            labels: Some(BTreeMap::from([
                ("kuberic.io/set".to_string(), "set".to_string()),
                ("kuberic.io/pod-index".to_string(), (id - 1).to_string()),
                ("kuberic.io/role".to_string(), role.to_string()),
            ])),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn set(reference: RemoveReplicaExecution) -> KubericSet {
    KubericSet {
        metadata: ObjectMeta {
            name: Some("set".to_string()),
            namespace: Some("default".to_string()),
            uid: Some("set-uid".to_string()),
            resource_version: Some("1".to_string()),
            ..Default::default()
        },
        spec: KubericSetSpec {
            replicas: 2,
            min_replicas: 2,
            image: "test:latest".to_string(),
            failover_delay: 0,
            switchover_delay: 30,
            switchover_execution_mode: Default::default(),
            port: 8080,
            control_port: 9090,
            data_port: 9091,
            storage: "256Mi".to_string(),
            pvc_retention_policy: PvcRetentionPolicy::Delete,
        },
        status: Some(KubericSetStatus {
            phase: Phase::RemovingReplica,
            replicas: 3,
            ready_replicas: 3,
            current_primary: Some("set-0".to_string()),
            target_primary: Some("set-0".to_string()),
            epoch: EpochStatus {
                data_loss_number: 1,
                configuration_number: 7,
            },
            members: vec![
                member(1, "one", "primary"),
                member(2, "two", "secondary"),
                member(3, "three", "secondary"),
            ],
            stable_snapshot: Some(snapshot()),
            remove_replica_execution: Some(reference),
            ..Default::default()
        }),
    }
}

fn member(id: i64, uid: &str, role: &str) -> MemberStatus {
    MemberStatus {
        name: format!("set-{}", id - 1),
        id,
        instance_id: uid.to_string(),
        role: role.to_string(),
        current_progress: 10,
        healthy: true,
        control_address: format!("http://{uid}:9090"),
        data_address: format!("http://{uid}:9091"),
    }
}

async fn store_completed_terminal(
    store: &InMemoryCheckpointStore,
    reference: &RemoveReplicaExecution,
) {
    let initial = reconstruct_initial_operation(reference.input.as_ref().unwrap()).unwrap();
    let terminal = RemoveReplicaTerminal::Completed {
        commit_evidence: RemoveReplicaCommitEvidenceStatus {
            attempt_id: format!("{}:attempt-1", initial.operation_id),
            action_id: format!("{}:attempt-1:RemoveReplicaIntent", initial.operation_id),
            primary_agent_generation: generation(1).to_string(),
            configuration_signature: "q2[2@two:]".to_string(),
            observed_unix_seconds: 10,
        },
        cleanup: RemoveReplicaCleanupStatus {
            connection_absent: true,
            target_retirement: Some(TargetRetirementObservationStatus::Completed),
            target_labels_fenced: true,
            target_pod_deleted: true,
        },
        accounting: RemoveReplicaActivityAccounting::default(),
    };
    let execution = execution_spec(reference).unwrap();
    let outcome =
        TerminalOutcome::succeeded(ExactBytes::new(serde_json::to_vec(&terminal).unwrap()));
    let envelope = CheckpointEnvelope::encode_with_limits(
        &CheckpointPayload::terminal(
            ExecutionContract::with_encoded_limits(
                execution.clone(),
                REMOVE_REPLICA_MAX_ACTIVE_ENCODED_BYTES as u64,
                REMOVE_REPLICA_MAX_TERMINAL_ENCODED_BYTES as u64,
            ),
            outcome,
            0,
        ),
        checkpoint_limits(),
    )
    .unwrap();
    assert!(matches!(
        store
            .compare_and_swap(execution.execution_id(), None, envelope)
            .await
            .unwrap(),
        CasOutcome::Accepted(_)
    ));
}

#[tokio::test]
async fn framework_native_remove_replica_route_publishes_reloaded_terminal() {
    let reference = reference();
    let store = InMemoryCheckpointStore::new();
    store_completed_terminal(&store, &reference).await;
    let state = ReconcilerState::with_remove_replica_store(store);
    let api = RoutingApi::new(vec![pod(1, "one", "primary"), pod(2, "two", "secondary")]);

    reconcile_set(&set(reference.clone()), &api, &state)
        .await
        .unwrap();

    let completed = api.last_status().unwrap();
    assert_eq!(completed.phase, Phase::Healthy);
    assert_eq!(completed.replicas, 2);
    assert_eq!(completed.stable_snapshot.unwrap().members.len(), 2);
    assert_eq!(
        completed.remove_replica_execution.unwrap().execution_id,
        reference.execution_id
    );
    assert!(completed.conditions.iter().any(|condition| {
        condition.type_ == "FrameworkNativeRemoveReplica" && condition.reason == "Completed"
    }));
}

#[tokio::test]
async fn framework_native_remove_replica_route_preserves_terminal_before_status_ordering() {
    let reference = reference();
    let store = InMemoryCheckpointStore::new();
    store_completed_terminal(&store, &reference).await;
    let state = ReconcilerState::with_remove_replica_store(store.clone());
    let api = RoutingApi::new(vec![pod(1, "one", "primary"), pod(2, "two", "secondary")]);
    api.fail_next_status_patch();

    let error = match reconcile_set(&set(reference.clone()), &api, &state).await {
        Ok(_) => panic!("publication conflict must surface after terminal reload"),
        Err(error) => error,
    };
    assert_eq!(error, "resource version conflict");
    assert!(api.last_status().is_none());

    let restarted = ReconcilerState::with_remove_replica_store(store);
    reconcile_set(&set(reference), &api, &restarted)
        .await
        .unwrap();
    assert_eq!(api.last_status().unwrap().phase, Phase::Healthy);
}

#[tokio::test]
async fn framework_native_remove_replica_route_records_incompatible_contract() {
    let mut reference = reference();
    reference.contract_version += 1;
    let state = ReconcilerState::with_remove_replica_store(InMemoryCheckpointStore::new());
    let api = RoutingApi::new(vec![
        pod(1, "one", "primary"),
        pod(2, "two", "secondary"),
        pod(3, "three", "secondary"),
    ]);

    reconcile_set(&set(reference), &api, &state).await.unwrap();

    let status = api.last_status().unwrap();
    assert_eq!(status.phase, Phase::RemovingReplica);
    assert!(status.conditions.iter().any(|condition| {
        condition.type_ == "FrameworkNativeRemoveReplica"
            && condition.reason == "Incompatible"
            && condition.message.contains("incompatible")
    }));
}

#[tokio::test]
async fn framework_native_remove_replica_route_primary_gap_has_no_status_churn() {
    let reference = reference();
    let state = ReconcilerState::with_remove_replica_store(InMemoryCheckpointStore::new());
    let api = RoutingApi::new(vec![
        pod(1, "one", "primary"),
        pod(2, "two", "secondary"),
        pod(3, "three", "secondary"),
    ]);
    api.fail_status(1, StatusFailure::Unavailable);

    reconcile_set(&set(reference.clone()), &api, &state)
        .await
        .unwrap();
    let waiting = api.last_status().unwrap();
    let patch_count = api.statuses.lock().unwrap().len();
    reconcile_set(
        &KubericSet {
            status: Some(waiting.clone()),
            ..set(reference)
        },
        &api,
        &state,
    )
    .await
    .unwrap();

    assert_eq!(api.statuses.lock().unwrap().len(), patch_count);
    assert!(waiting.conditions.iter().any(|condition| {
        condition.type_ == "FrameworkNativeRemoveReplica"
            && condition.reason == "AwaitingFreshAuthority"
    }));
}

#[tokio::test]
async fn framework_native_remove_replica_route_target_gap_has_no_status_churn() {
    let reference = reference();
    let state = ReconcilerState::with_remove_replica_store(InMemoryCheckpointStore::new());
    let api = RoutingApi::new(vec![
        pod(1, "one", "primary"),
        pod(2, "two", "secondary"),
        pod(3, "three", "secondary"),
    ]);
    api.fail_status(3, StatusFailure::Unavailable);

    reconcile_set(&set(reference.clone()), &api, &state)
        .await
        .unwrap();
    let waiting = api.last_status().unwrap();
    let patch_count = api.statuses.lock().unwrap().len();
    reconcile_set(
        &KubericSet {
            status: Some(waiting.clone()),
            ..set(reference)
        },
        &api,
        &state,
    )
    .await
    .unwrap();

    assert_eq!(api.statuses.lock().unwrap().len(), patch_count);
    assert!(waiting.conditions.iter().any(|condition| {
        condition.type_ == "FrameworkNativeRemoveReplica"
            && condition.reason == "AwaitingFreshAuthority"
    }));
}

#[tokio::test]
async fn framework_native_remove_replica_route_isolates_malformed_agent_status() {
    let reference = reference();
    let state = ReconcilerState::with_remove_replica_store(InMemoryCheckpointStore::new());
    let api = RoutingApi::new(vec![
        pod(1, "one", "primary"),
        pod(2, "two", "secondary"),
        pod(3, "three", "secondary"),
    ]);
    api.fail_status(1, StatusFailure::Malformed);

    reconcile_set(&set(reference), &api, &state).await.unwrap();

    assert!(
        api.last_status()
            .unwrap()
            .conditions
            .iter()
            .any(|condition| {
                condition.type_ == "FrameworkNativeRemoveReplica"
                    && condition.reason == "Isolated"
                    && condition
                        .message
                        .contains("unsupported or malformed control status")
            })
    );
}
