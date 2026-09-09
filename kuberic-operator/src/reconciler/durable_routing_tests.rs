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
    ActivityName, ActivityRecord, ActivitySequence, ActivitySpec, CasOutcome, CheckpointEnvelope,
    CheckpointPayload, CheckpointStore, ExactBytes, ExecutionContract, InMemoryCheckpointStore,
    InMemoryFault, StoreErrorKind, TerminalOutcome,
};

use super::*;
use crate::crd::{
    EpochStatus, MemberStatus, PvcRetentionPolicy, RemoveReplicaCleanupStatus,
    RemoveReplicaCommitEvidenceStatus, StableReplicaRoleStatus, StableReplicaSnapshotStatus,
    SwitchoverExecutionStatus, TargetRetirementObservationStatus,
};
use crate::durable::remove_replica_execution::{
    REMOVE_REPLICA_MAX_ACTIVE_ENCODED_BYTES, REMOVE_REPLICA_MAX_TERMINAL_ENCODED_BYTES,
    RemoveReplicaActivityAccounting, RemoveReplicaExecution, RemoveReplicaTerminal,
    checkpoint_limits, execution_spec, new_execution, reconstruct_initial_operation,
};
use crate::durable::switchover_execution::{
    DirectSwitchoverTerminalBranch, DirectSwitchoverTerminalRecord,
    SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES, SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES,
    SwitchoverActivityAccounting, checkpoint_limits as switchover_checkpoint_limits,
    encode_terminal as encode_switchover_terminal, is_switchover_activity_identity,
    native_execution_spec, native_initial_operation, new_switchover_execution,
};

#[derive(Default)]
struct RoutingApi {
    pods: StdMutex<Vec<Pod>>,
    statuses: StdMutex<Vec<KubericSetStatus>>,
    fail_status_patch: StdMutex<bool>,
    replica_statuses: StdArc<StdMutex<BTreeMap<ReplicaId, ReplicaStatusInfo>>>,
    status_failures: StdArc<StdMutex<BTreeMap<ReplicaId, StatusFailure>>>,
    reject_dispatch_as_busy: StdArc<StdMutex<bool>>,
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

    fn reject_dispatch_as_busy(&self) {
        *self.reject_dispatch_as_busy.lock().unwrap() = true;
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
    reject_dispatch_as_busy: StdArc<StdMutex<bool>>,
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
        if *self.reject_dispatch_as_busy.lock().unwrap() {
            return Err(KubericError::AgentBusy);
        }
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
            reject_dispatch_as_busy: self.reject_dispatch_as_busy.clone(),
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

fn switchover_set(reference: SwitchoverExecutionStatus) -> KubericSet {
    KubericSet {
        metadata: ObjectMeta {
            name: Some("set".to_string()),
            namespace: Some("default".to_string()),
            uid: Some("set-uid".to_string()),
            resource_version: Some("1".to_string()),
            ..Default::default()
        },
        spec: KubericSetSpec {
            replicas: 3,
            min_replicas: 2,
            image: "test:latest".to_string(),
            failover_delay: 0,
            switchover_delay: 30,
            port: 8080,
            control_port: 9090,
            data_port: 9091,
            storage: "256Mi".to_string(),
            pvc_retention_policy: PvcRetentionPolicy::Delete,
        },
        status: Some(KubericSetStatus {
            phase: Phase::Switchover,
            replicas: 3,
            ready_replicas: 3,
            current_primary: Some("set-0".to_string()),
            target_primary: Some("set-1".to_string()),
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
            switchover_execution: Some(reference),
            ..Default::default()
        }),
    }
}

async fn store_switchover_terminal(
    store: &InMemoryCheckpointStore,
    reference: &SwitchoverExecutionStatus,
) {
    let initial = native_initial_operation(reference).unwrap();
    let terminal = DirectSwitchoverTerminalRecord::Complete {
        snapshot: initial.target_snapshot,
        compensated: false,
        branch: DirectSwitchoverTerminalBranch::TargetSuccess,
        reason: None,
        accounting: Some(SwitchoverActivityAccounting::new(9, 3)),
    };
    store_switchover_terminal_outcome(
        store,
        reference,
        TerminalOutcome::succeeded(encode_switchover_terminal(&terminal).unwrap()),
        12,
    )
    .await;
}

async fn store_switchover_terminal_outcome(
    store: &InMemoryCheckpointStore,
    reference: &SwitchoverExecutionStatus,
    outcome: TerminalOutcome,
    completed_activity_count: u64,
) {
    let execution = native_execution_spec(reference).unwrap();
    let envelope = CheckpointEnvelope::encode_with_limits(
        &CheckpointPayload::terminal(
            ExecutionContract::with_encoded_limits(
                execution.clone(),
                SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES as u64,
                SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES as u64,
            ),
            outcome,
            completed_activity_count,
        ),
        switchover_checkpoint_limits(),
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
async fn framework_native_switchover_route_publishes_reloaded_terminal() {
    let reference = new_switchover_execution("set-uid", snapshot(), 2, 10).unwrap();
    let store = InMemoryCheckpointStore::new();
    store_switchover_terminal(&store, &reference).await;
    let state = ReconcilerState::with_switchover_store(store);
    let api = RoutingApi::new(vec![
        pod(1, "one", "primary"),
        pod(2, "two", "secondary"),
        pod(3, "three", "secondary"),
    ]);
    let current_pods = api.pods.lock().unwrap().clone();

    reconcile_framework_native_switchover(
        &switchover_set(reference.clone()),
        &api,
        &state,
        &current_pods,
    )
    .await
    .unwrap();

    let published = api.last_status().unwrap();
    assert_eq!(published.phase, Phase::Healthy);
    assert_eq!(published.current_primary.as_deref(), Some("set-1"));
    assert_eq!(published.stable_snapshot.as_ref().unwrap().primary_id, 2);
    assert_eq!(
        published
            .switchover_execution
            .as_ref()
            .unwrap()
            .execution_id,
        reference.execution_id
    );
    assert!(published.conditions.iter().any(|condition| {
        condition.type_ == "FrameworkNativeSwitchover"
            && condition.reason == "Completed"
            && condition.status == "False"
    }));
}

#[tokio::test]
async fn framework_native_switchover_route_records_invalid_reference_condition() {
    let mut reference = new_switchover_execution("set-uid", snapshot(), 2, 10).unwrap();
    reference.contract_version += 1;
    let state = ReconcilerState::with_switchover_store(InMemoryCheckpointStore::new());
    let api = RoutingApi::new(vec![
        pod(1, "one", "primary"),
        pod(2, "two", "secondary"),
        pod(3, "three", "secondary"),
    ]);
    let current_pods = api.pods.lock().unwrap().clone();

    reconcile_framework_native_switchover(&switchover_set(reference), &api, &state, &current_pods)
        .await
        .unwrap();

    assert!(
        api.last_status()
            .unwrap()
            .conditions
            .iter()
            .any(|condition| {
                condition.type_ == "FrameworkNativeSwitchover"
                    && condition.reason == "Blocked"
                    && condition.message.contains("unsupported")
            })
    );
}

#[tokio::test]
async fn framework_native_switchover_route_blocks_invalid_admission_authority_without_dispatch() {
    let valid = new_switchover_execution("set-uid", snapshot(), 2, 10).unwrap();
    let mut invalid_references = Vec::new();
    let mut foreign_authority = valid.clone();
    foreign_authority.input.operation_authority = "other-uid".to_string();
    invalid_references.push(foreign_authority);
    let mut invalid_time = valid;
    invalid_time.input.accepted_unix_seconds = i64::MAX;
    invalid_references.push(invalid_time);

    for reference in invalid_references {
        let state = ReconcilerState::with_switchover_store(InMemoryCheckpointStore::new());
        let api = RoutingApi::new(vec![
            pod(1, "one", "primary"),
            pod(2, "two", "secondary"),
            pod(3, "three", "secondary"),
        ]);
        let current_pods = api.pods.lock().unwrap().clone();

        reconcile_framework_native_switchover(
            &switchover_set(reference),
            &api,
            &state,
            &current_pods,
        )
        .await
        .unwrap();

        assert!(
            api.last_status()
                .unwrap()
                .conditions
                .iter()
                .any(|condition| {
                    condition.type_ == "FrameworkNativeSwitchover" && condition.reason == "Blocked"
                })
        );
    }
}

#[tokio::test]
async fn switchover_phase_without_native_reference_fails_closed_without_dispatch() {
    let mut set = switchover_set(new_switchover_execution("set-uid", snapshot(), 2, 10).unwrap());
    let status = set.status.as_mut().unwrap();
    status.switchover_execution = None;
    status.legacy_status_fields.insert(
        "removedSwitchoverField".to_string(),
        serde_json::json!({"ignored": true}),
    );
    let api = RoutingApi::new(vec![
        pod(1, "one", "primary"),
        pod(2, "two", "secondary"),
        pod(3, "three", "secondary"),
    ]);

    let error = match reconcile_set(&set, &api, &ReconcilerState::default()).await {
        Ok(_) => panic!("missing native switchover reference unexpectedly reconciled"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        "switchover phase has no production execution reference"
    );
    assert!(api.statuses.lock().unwrap().is_empty());
}

#[tokio::test]
async fn framework_native_switchover_publication_retry_cleans_native_host() {
    let reference = new_switchover_execution("set-uid", snapshot(), 2, 10).unwrap();
    let store = InMemoryCheckpointStore::new();
    store_switchover_terminal(&store, &reference).await;
    let state = ReconcilerState::with_switchover_store(store);
    let api = RoutingApi::new(vec![
        pod(1, "one", "primary"),
        pod(2, "two", "secondary"),
        pod(3, "three", "secondary"),
    ]);
    api.fail_next_status_patch();
    let set = switchover_set(reference);
    let current_pods = api.pods.lock().unwrap().clone();

    reconcile_framework_native_switchover(&set, &api, &state, &current_pods)
        .await
        .unwrap();
    assert_eq!(state.framework_native_switchover.host_count().await, 1);

    reconcile_set(&set, &api, &state).await.unwrap();
    assert_eq!(state.framework_native_switchover.host_count().await, 0);
}

async fn assert_native_switchover_condition_for_store(
    reference: SwitchoverExecutionStatus,
    store: InMemoryCheckpointStore,
    expected_reason: &str,
    pods: Vec<Pod>,
) -> KubericSetStatus {
    let state = ReconcilerState::with_switchover_store(store);
    let api = RoutingApi::new(pods);
    let current_pods = api.pods.lock().unwrap().clone();
    reconcile_framework_native_switchover(&switchover_set(reference), &api, &state, &current_pods)
        .await
        .unwrap();
    let status = api.last_status().unwrap();
    assert!(
        status.conditions.iter().any(|condition| {
            condition.type_ == "FrameworkNativeSwitchover" && condition.reason == expected_reason
        }),
        "missing native switchover condition {expected_reason}"
    );
    status
}

#[tokio::test]
async fn framework_native_switchover_route_records_checkpoint_dispositions() {
    let reference = new_switchover_execution("set-uid", snapshot(), 2, 10).unwrap();
    let execution = native_execution_spec(&reference).unwrap();
    let incompatible = InMemoryCheckpointStore::new();
    assert!(matches!(
        incompatible
            .compare_and_swap(
                execution.execution_id(),
                None,
                CheckpointEnvelope::new(99, ExactBytes::new(b"{}".to_vec())),
            )
            .await
            .unwrap(),
        CasOutcome::Accepted(_)
    ));
    assert_native_switchover_condition_for_store(
        reference.clone(),
        incompatible,
        "Incompatible",
        vec![
            pod(1, "one", "primary"),
            pod(2, "two", "secondary"),
            pod(3, "three", "secondary"),
        ],
    )
    .await;

    let rejected = InMemoryCheckpointStore::new();
    assert!(matches!(
        rejected
            .compare_and_swap(
                execution.execution_id(),
                None,
                CheckpointEnvelope::new(
                    kuberic_durable_execution::CHECKPOINT_FORMAT_VERSION,
                    ExactBytes::new(b"not-json".to_vec()),
                ),
            )
            .await
            .unwrap(),
        CasOutcome::Accepted(_)
    ));
    assert_native_switchover_condition_for_store(
        reference,
        rejected,
        "Rejected",
        vec![
            pod(1, "one", "primary"),
            pod(2, "two", "secondary"),
            pod(3, "three", "secondary"),
        ],
    )
    .await;

    assert_native_switchover_condition_for_store(
        new_switchover_execution("set-uid", snapshot(), 2, 10).unwrap(),
        InMemoryCheckpointStore::new(),
        "Isolated",
        vec![pod(1, "one", "primary"), pod(2, "two", "secondary")],
    )
    .await;
}

#[tokio::test]
async fn framework_native_switchover_route_rejects_unreachable_terminal_transcripts() {
    let reference = new_switchover_execution("set-uid", snapshot(), 2, 10).unwrap();
    let initial = native_initial_operation(&reference).unwrap();
    let store = InMemoryCheckpointStore::new();
    let impossible_success = DirectSwitchoverTerminalRecord::Complete {
        snapshot: initial.target_snapshot.clone(),
        compensated: false,
        branch: DirectSwitchoverTerminalBranch::TargetSuccess,
        reason: None,
        accounting: Some(SwitchoverActivityAccounting::new(1, 0)),
    };
    store_switchover_terminal_outcome(
        &store,
        &reference,
        TerminalOutcome::succeeded(encode_switchover_terminal(&impossible_success).unwrap()),
        1,
    )
    .await;
    let rejected_status = assert_native_switchover_condition_for_store(
        reference,
        store,
        "Rejected",
        vec![
            pod(1, "one", "primary"),
            pod(2, "two", "secondary"),
            pod(3, "three", "secondary"),
        ],
    )
    .await;
    assert_eq!(rejected_status.phase, Phase::Switchover);
    assert_eq!(rejected_status.current_primary.as_deref(), Some("set-0"));

    let reference = new_switchover_execution("set-uid", snapshot(), 2, 10).unwrap();
    let initial = native_initial_operation(&reference).unwrap();
    let store = InMemoryCheckpointStore::new();
    let mut compensation_snapshot = initial.previous_snapshot.cloned().unwrap();
    compensation_snapshot.epoch = initial.target_snapshot.epoch.clone();
    let impossible_compensation = DirectSwitchoverTerminalRecord::Complete {
        snapshot: compensation_snapshot,
        compensated: true,
        branch: DirectSwitchoverTerminalBranch::PostPromotionCompensated,
        reason: Some("unreachable compensation transcript".to_string()),
        accounting: Some(SwitchoverActivityAccounting::new(1, 1)),
    };
    store_switchover_terminal_outcome(
        &store,
        &reference,
        TerminalOutcome::succeeded(encode_switchover_terminal(&impossible_compensation).unwrap()),
        2,
    )
    .await;
    let rejected_status = assert_native_switchover_condition_for_store(
        reference,
        store,
        "Rejected",
        vec![
            pod(1, "one", "primary"),
            pod(2, "two", "secondary"),
            pod(3, "three", "secondary"),
        ],
    )
    .await;
    assert_eq!(rejected_status.phase, Phase::Switchover);
    assert_eq!(rejected_status.current_primary.as_deref(), Some("set-0"));
}

#[tokio::test]
async fn framework_native_switchover_route_publishes_reachable_passive_effect_transcripts() {
    for (label, branch, accounting, expected_primary) in [
        (
            "demotion-deadline",
            DirectSwitchoverTerminalBranch::PreviousConfigurationRestored,
            SwitchoverActivityAccounting::new(2, 4),
            "set-0",
        ),
        (
            "promotion-deadline",
            DirectSwitchoverTerminalBranch::PostPromotionCompensated,
            SwitchoverActivityAccounting::new(7, 6),
            "set-0",
        ),
        (
            "exact-replica-postcondition",
            DirectSwitchoverTerminalBranch::TargetSuccess,
            SwitchoverActivityAccounting::new(8, 4),
            "set-1",
        ),
        (
            "already-exact-labels",
            DirectSwitchoverTerminalBranch::TargetSuccess,
            SwitchoverActivityAccounting::new(7, 5),
            "set-1",
        ),
    ] {
        let reference = new_switchover_execution("set-uid", snapshot(), 2, 10).unwrap();
        let initial = native_initial_operation(&reference).unwrap();
        let terminal_snapshot = match branch {
            DirectSwitchoverTerminalBranch::TargetSuccess => initial.target_snapshot.clone(),
            DirectSwitchoverTerminalBranch::PreviousConfigurationRestored => {
                initial.previous_snapshot.clone().0.unwrap()
            }
            DirectSwitchoverTerminalBranch::PostPromotionCompensated => {
                let mut snapshot = initial.previous_snapshot.clone().0.unwrap();
                snapshot.epoch = initial.target_snapshot.epoch.clone();
                snapshot
            }
            DirectSwitchoverTerminalBranch::RevokeSafeFailure => unreachable!(),
        };
        let terminal = DirectSwitchoverTerminalRecord::Complete {
            snapshot: terminal_snapshot,
            compensated: branch != DirectSwitchoverTerminalBranch::TargetSuccess,
            branch,
            reason: (branch != DirectSwitchoverTerminalBranch::TargetSuccess)
                .then(|| label.to_string()),
            accounting: Some(accounting),
        };
        let store = InMemoryCheckpointStore::new();
        store_switchover_terminal_outcome(
            &store,
            &reference,
            TerminalOutcome::succeeded(encode_switchover_terminal(&terminal).unwrap()),
            accounting.total().unwrap(),
        )
        .await;
        let state = ReconcilerState::with_switchover_store(store);
        let api = RoutingApi::new(vec![
            pod(1, "one", "primary"),
            pod(2, "two", "secondary"),
            pod(3, "three", "secondary"),
        ]);
        let current_pods = api.pods.lock().unwrap().clone();

        reconcile_framework_native_switchover(
            &switchover_set(reference.clone()),
            &api,
            &state,
            &current_pods,
        )
        .await
        .unwrap();

        let published = api.last_status().unwrap();
        assert_eq!(published.phase, Phase::Healthy, "{label}");
        assert_eq!(
            published.current_primary.as_deref(),
            Some(expected_primary),
            "{label}"
        );
        let measurements = state
            .framework_native_switchover_measurements(
                "default",
                "set",
                "set-uid",
                &reference.execution_id,
            )
            .await
            .unwrap();
        assert_eq!(
            measurements.completed_activity_count,
            accounting.total(),
            "{label}"
        );
        assert_eq!(
            measurements.completed_external_effect_count,
            Some(accounting.external_effect_count),
            "{label}"
        );
        assert_eq!(
            measurements.completed_passive_observation_count,
            Some(accounting.passive_observation_count),
            "{label}"
        );
    }
}

#[tokio::test]
async fn framework_native_switchover_route_records_reload_and_persistence_failures() {
    let reload = InMemoryCheckpointStore::new();
    reload.fail_next_compare_and_swap(InMemoryFault::OutcomeUnknownWithoutApply);
    assert_native_switchover_condition_for_store(
        new_switchover_execution("set-uid", snapshot(), 2, 10).unwrap(),
        reload,
        "ReloadRequired",
        vec![
            pod(1, "one", "primary"),
            pod(2, "two", "secondary"),
            pod(3, "three", "secondary"),
        ],
    )
    .await;

    let failed = InMemoryCheckpointStore::new();
    failed.fail_next_compare_and_swap(InMemoryFault::FailBeforeRequest(
        StoreErrorKind::Unavailable,
    ));
    assert_native_switchover_condition_for_store(
        new_switchover_execution("set-uid", snapshot(), 2, 10).unwrap(),
        failed,
        "StorageUnavailable",
        vec![
            pod(1, "one", "primary"),
            pod(2, "two", "secondary"),
            pod(3, "three", "secondary"),
        ],
    )
    .await;
}

#[tokio::test]
async fn framework_native_switchover_route_records_adapter_wait_and_fuel_exhaustion() {
    let reference = new_switchover_execution("set-uid", snapshot(), 2, unix_seconds()).unwrap();
    let state = ReconcilerState::with_switchover_store(InMemoryCheckpointStore::new());
    let api = RoutingApi::new(vec![
        pod(1, "one", "primary"),
        pod(2, "two", "secondary"),
        pod(3, "three", "secondary"),
    ]);
    api.fail_status(1, StatusFailure::Unavailable);
    let current_pods = api.pods.lock().unwrap().clone();
    reconcile_framework_native_switchover(&switchover_set(reference), &api, &state, &current_pods)
        .await
        .unwrap();
    let waiting = api.last_status().unwrap();
    assert!(
        waiting.conditions.iter().any(|condition| {
            condition.type_ == "FrameworkNativeSwitchover"
                && condition.reason == "AwaitingEffectPreparation"
        }),
        "{:?}",
        waiting.conditions
    );

    let reference = new_switchover_execution("set-uid", snapshot(), 2, unix_seconds()).unwrap();
    let store = InMemoryCheckpointStore::new();
    let state = ReconcilerState::with_switchover_store(store.clone());
    let api = RoutingApi::new(vec![
        pod(1, "one", "primary"),
        pod(2, "two", "secondary"),
        pod(3, "three", "secondary"),
    ]);
    api.reject_dispatch_as_busy();
    let current_pods = api.pods.lock().unwrap().clone();
    reconcile_framework_native_switchover_with_fuel(
        &switchover_set(reference.clone()),
        &api,
        &state,
        &current_pods,
        1,
    )
    .await
    .unwrap();
    assert!(
        api.last_status()
            .unwrap()
            .conditions
            .iter()
            .any(|condition| {
                condition.type_ == "FrameworkNativeSwitchover"
                    && condition.reason == "FuelExhausted"
            })
    );

    let execution = native_execution_spec(&reference).unwrap();
    let stored = store
        .load(execution.execution_id())
        .await
        .unwrap()
        .expect("production switchover must persist an active checkpoint");
    let payload = stored
        .checkpoint()
        .decode_and_validate(&execution, switchover_checkpoint_limits())
        .unwrap();
    let activities = payload.active_activities().unwrap();
    assert!(!activities.is_empty());
    for activity in activities {
        assert!(is_switchover_activity_identity(
            activity.name().name(),
            activity.name().version()
        ));
        assert_eq!(activity.name().version(), 1);
        assert_ne!(
            activity.name().name(),
            ["kuberic.switchover.", "native", "-boundary"].concat()
        );
    }
}

#[tokio::test]
async fn framework_native_switchover_route_records_nondeterminism() {
    let reference = new_switchover_execution("set-uid", snapshot(), 2, 10).unwrap();
    let execution = native_execution_spec(&reference).unwrap();
    let store = InMemoryCheckpointStore::new();
    let wrong = ActivitySpec::new(
        ActivityName::new("kuberic.switchover.wrong-boundary", 1).unwrap(),
        ExactBytes::new(b"{}".to_vec()),
        1,
    );
    let envelope = CheckpointEnvelope::encode_with_limits(
        &CheckpointPayload::active(
            ExecutionContract::with_encoded_limits(
                execution.clone(),
                SWITCHOVER_MAX_ACTIVE_ENCODED_BYTES as u64,
                SWITCHOVER_MAX_TERMINAL_ENCODED_BYTES as u64,
            ),
            vec![ActivityRecord::scheduled(ActivitySequence::new(0), wrong)],
        ),
        switchover_checkpoint_limits(),
    )
    .unwrap();
    assert!(matches!(
        store
            .compare_and_swap(execution.execution_id(), None, envelope)
            .await
            .unwrap(),
        CasOutcome::Accepted(_)
    ));
    assert_native_switchover_condition_for_store(
        reference,
        store,
        "Nondeterministic",
        vec![
            pod(1, "one", "primary"),
            pod(2, "two", "secondary"),
            pod(3, "three", "secondary"),
        ],
    )
    .await;
}

#[tokio::test]
async fn framework_native_switchover_route_publishes_compensation_and_quarantine() {
    let compensation_reference = new_switchover_execution("set-uid", snapshot(), 2, 10).unwrap();
    let initial = native_initial_operation(&compensation_reference).unwrap();
    let mut compensation_snapshot = initial.previous_snapshot.cloned().unwrap();
    compensation_snapshot.epoch = initial.target_snapshot.epoch.clone();
    let terminal = DirectSwitchoverTerminalRecord::Complete {
        snapshot: compensation_snapshot,
        compensated: true,
        branch: DirectSwitchoverTerminalBranch::PostPromotionCompensated,
        reason: Some("target promotion failed and the old primary was restored".to_string()),
        accounting: Some(SwitchoverActivityAccounting::new(8, 5)),
    };
    let store = InMemoryCheckpointStore::new();
    store_switchover_terminal_outcome(
        &store,
        &compensation_reference,
        TerminalOutcome::succeeded(encode_switchover_terminal(&terminal).unwrap()),
        13,
    )
    .await;
    let state = ReconcilerState::with_switchover_store(store);
    let api = RoutingApi::new(vec![
        pod(1, "one", "primary"),
        pod(2, "two", "secondary"),
        pod(3, "three", "secondary"),
    ]);
    let current_pods = api.pods.lock().unwrap().clone();
    reconcile_framework_native_switchover(
        &switchover_set(compensation_reference),
        &api,
        &state,
        &current_pods,
    )
    .await
    .unwrap();
    assert!(
        api.last_status()
            .unwrap()
            .conditions
            .iter()
            .any(|condition| {
                condition.type_ == "FrameworkNativeSwitchover"
                    && condition.reason == "CompensatedOrSafeFailure"
                    && condition.status == "False"
            })
    );

    let quarantine_reference = new_switchover_execution("set-uid", snapshot(), 2, 10).unwrap();
    let terminal = DirectSwitchoverTerminalRecord::Stopped {
        message: "unknown exposed effect".to_string(),
    };
    let store = InMemoryCheckpointStore::new();
    store_switchover_terminal_outcome(
        &store,
        &quarantine_reference,
        TerminalOutcome::failed(encode_switchover_terminal(&terminal).unwrap()),
        1,
    )
    .await;
    let state = ReconcilerState::with_switchover_store(store);
    let api = RoutingApi::new(vec![
        pod(1, "one", "primary"),
        pod(2, "two", "secondary"),
        pod(3, "three", "secondary"),
    ]);
    let current_pods = api.pods.lock().unwrap().clone();
    reconcile_framework_native_switchover(
        &switchover_set(quarantine_reference),
        &api,
        &state,
        &current_pods,
    )
    .await
    .unwrap();
    assert!(
        api.last_status()
            .unwrap()
            .conditions
            .iter()
            .any(|condition| {
                condition.type_ == "FrameworkNativeSwitchover" && condition.reason == "Quarantined"
            })
    );
}
