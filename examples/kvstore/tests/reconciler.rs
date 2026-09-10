use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod, PodCondition, PodStatus, Service};
use kube::api::ObjectMeta;
use serial_test::serial;
use tokio::sync::{RwLock, mpsc, watch};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use kuberic_core::driver::ReplicaHandle;
use kuberic_core::error::{KubericError, Result as CoreResult};
use kuberic_core::events::LifecycleEvent;
use kuberic_core::grpc::handle::GrpcReplicaHandle;
use kuberic_core::pod::PodRuntime;
use kuberic_core::proto::replica_lifecycle_peer_client::ReplicaLifecyclePeerClient;
use kuberic_core::proto::replica_lifecycle_peer_server::{
    ReplicaLifecyclePeer, ReplicaLifecyclePeerServer,
};
use kuberic_core::proto::{
    ExecuteLifecycleStageRequest, ExecuteLifecycleStageResponse, GetLifecycleStatusRequest,
    GetLifecycleStatusResponse,
};
use kuberic_core::remove_replica::{
    ManualRemoveReplicaClock, RemoveReplicaCoordinatorPhase, RemoveReplicaProgress,
    RemoveReplicaTerminalResult, TargetRetirementObservation,
};
use kuberic_core::types::{
    CorrelatedControlActionAcknowledgement, CorrelatedControlActionRequest, DurableActionResult,
    DurableReplicaAction, Epoch, Lsn, ReplicaConfigurationMode, ReplicaId, ReplicaInstanceId,
    ReplicaStatusInfo, Role,
};

use kuberic_operator::cluster_api::ClusterApi;
use kuberic_operator::crd::{
    DurableActionKind, DurableAddMode, DurableOperationKind, DurableOperationPhase,
    DurableRemoveMode, KubericSet, KubericSetSpec, KubericSetStatus, Phase, PvcRetentionPolicy,
    RemoveReplicaCleanupStatus, RemoveReplicaCommitEvidenceStatus,
    RemoveReplicaCoordinatorPhaseStatus, RemoveReplicaDispositionStatus,
    RemoveReplicaTerminalResultStatus, ReplicaElectionObservationStatus, StableReplicaRoleStatus,
    TargetRetirementObservationStatus,
};
use kuberic_operator::durable::{RemoveReplicaTarget, start_remove_replica};
use kuberic_operator::reconciler::{ReconcilerState, reconcile_set};

use kvstore::proto;
use kvstore::service;
use kvstore::state::{KvState, SharedState};

async fn allocate_unique_address() -> String {
    static ALLOCATED_PORTS: OnceLock<Mutex<HashSet<u16>>> = OnceLock::new();

    loop {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let is_new = ALLOCATED_PORTS
            .get_or_init(|| Mutex::new(HashSet::new()))
            .lock()
            .unwrap()
            .insert(port);
        drop(listener);

        if is_new {
            return format!("127.0.0.1:{port}");
        }
    }
}

struct LivePod {
    control_address: String,
    data_address: String,
    client_address: String,
    data_dir: PathBuf,
    lifecycle_control: LifecycleControl,
    runtime_shutdown: kuberic_core::types::CancellationToken,
    #[allow(dead_code)]
    state: SharedState,
    _runtime_handle: tokio::task::JoinHandle<()>,
    _lifecycle_relay_handle: tokio::task::JoinHandle<()>,
    _service_handle: tokio::task::JoinHandle<()>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum HeldLifecycleEffect {
    ChangeRoleNone,
    Close,
}

#[derive(Clone)]
struct LifecycleControl {
    held: Arc<Mutex<HashSet<HeldLifecycleEffect>>>,
    entered: watch::Sender<Option<HeldLifecycleEffect>>,
    release: Arc<tokio::sync::Notify>,
}

impl LifecycleControl {
    fn new() -> Self {
        let (entered, _) = watch::channel(None);
        Self {
            held: Arc::new(Mutex::new(HashSet::new())),
            entered,
            release: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn hold(&self, effect: HeldLifecycleEffect) {
        self.held.lock().unwrap().insert(effect);
    }

    async fn wait_until_entered(&self, effect: HeldLifecycleEffect) {
        let mut entered = self.entered.subscribe();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if *entered.borrow_and_update() == Some(effect) {
                    return;
                }
                entered.changed().await.unwrap();
            }
        })
        .await
        .expect("held lifecycle effect was not reached");
    }

    fn has_entered(&self, effect: HeldLifecycleEffect) -> bool {
        *self.entered.borrow() == Some(effect)
    }

    async fn before_forward(&self, effect: HeldLifecycleEffect) {
        if !self.held.lock().unwrap().contains(&effect) {
            return;
        }
        self.entered.send_replace(Some(effect));
        while self.held.lock().unwrap().contains(&effect) {
            self.release.notified().await;
        }
    }
}

async fn relay_lifecycle_events(
    mut source: mpsc::Receiver<LifecycleEvent>,
    target: mpsc::Sender<LifecycleEvent>,
    control: LifecycleControl,
) {
    while let Some(event) = source.recv().await {
        let held = match &event {
            LifecycleEvent::ChangeRole {
                new_role: Role::None,
                ..
            } => Some(HeldLifecycleEffect::ChangeRoleNone),
            LifecycleEvent::Close { .. } => Some(HeldLifecycleEffect::Close),
            _ => None,
        };
        if let Some(effect) = held {
            control.before_forward(effect).await;
        }
        if target.send(event).await.is_err() {
            break;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PeerProxyMode {
    Forward,
    Unavailable,
    LoseNextStageReply,
}

#[derive(Clone)]
struct PeerProxyControl {
    mode: Arc<Mutex<PeerProxyMode>>,
}

impl PeerProxyControl {
    fn set(&self, mode: PeerProxyMode) {
        *self.mode.lock().unwrap() = mode;
    }
}

struct PeerProxy {
    target_address: String,
    mode: Arc<Mutex<PeerProxyMode>>,
}

#[tonic::async_trait]
impl ReplicaLifecyclePeer for PeerProxy {
    async fn get_lifecycle_status(
        &self,
        request: Request<GetLifecycleStatusRequest>,
    ) -> Result<Response<GetLifecycleStatusResponse>, Status> {
        if *self.mode.lock().unwrap() == PeerProxyMode::Unavailable {
            return Err(Status::unavailable(
                "injected lifecycle peer unavailability",
            ));
        }
        let mut client = ReplicaLifecyclePeerClient::connect(self.target_address.clone())
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        client.get_lifecycle_status(request.into_inner()).await
    }

    async fn execute_lifecycle_stage(
        &self,
        request: Request<ExecuteLifecycleStageRequest>,
    ) -> Result<Response<ExecuteLifecycleStageResponse>, Status> {
        if *self.mode.lock().unwrap() == PeerProxyMode::Unavailable {
            return Err(Status::unavailable(
                "injected lifecycle peer unavailability",
            ));
        }
        let mut client = ReplicaLifecyclePeerClient::connect(self.target_address.clone())
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let response = client.execute_lifecycle_stage(request.into_inner()).await?;
        let mut mode = self.mode.lock().unwrap();
        if *mode == PeerProxyMode::LoseNextStageReply {
            *mode = PeerProxyMode::Forward;
            return Err(Status::unavailable(
                "injected lost lifecycle stage reply after apply",
            ));
        }
        Ok(response)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlOperation {
    AddReplicaIntent,
    RemoveReplicaIntent,
    Open,
    Close,
    ChangeRole,
    UpdateEpoch,
    UpdateCatchUpConfiguration,
    UpdateCurrentConfiguration,
    WaitForCatchUpQuorum,
    BuildReplica,
    RemoveReplica,
    OnDataLoss,
    RecordElectionConfiguration,
    RevokeWriteStatus,
    GetStatus,
}

#[derive(Debug, Clone, Copy)]
enum InjectedStatusError {
    UnsupportedProtocol,
    MalformedAgentStatus,
    Unavailable,
}

struct ObservedHandle {
    inner: Box<dyn ReplicaHandle>,
    pod_name: String,
    exposed_control_address: Option<String>,
    operations: Arc<Mutex<Vec<ControlOperation>>>,
    fail_before_next_durable_action: Arc<Mutex<Option<ControlOperation>>>,
    fail_after_next_durable_action: Arc<Mutex<Option<ControlOperation>>>,
    fail_next_status: Arc<Mutex<Option<InjectedStatusError>>>,
    status_call_counts: Arc<Mutex<HashMap<String, usize>>>,
    fail_status_on_call: Arc<Mutex<HashMap<String, (usize, InjectedStatusError)>>>,
}

impl ObservedHandle {
    fn record(&self, operation: ControlOperation) {
        self.operations.lock().unwrap().push(operation);
    }

    fn operation_for(action: &DurableReplicaAction) -> ControlOperation {
        match action {
            DurableReplicaAction::AddReplicaIntent { .. } => ControlOperation::AddReplicaIntent,
            DurableReplicaAction::RemoveReplicaIntent { .. } => {
                ControlOperation::RemoveReplicaIntent
            }
            DurableReplicaAction::Open { .. } => ControlOperation::Open,
            DurableReplicaAction::Close => ControlOperation::Close,
            DurableReplicaAction::RevokeWriteStatus => ControlOperation::RevokeWriteStatus,
            DurableReplicaAction::ChangeRole { .. } => ControlOperation::ChangeRole,
            DurableReplicaAction::UpdateEpoch { .. } => ControlOperation::UpdateEpoch,
            DurableReplicaAction::UpdateCatchUpConfiguration { .. } => {
                ControlOperation::UpdateCatchUpConfiguration
            }
            DurableReplicaAction::WaitForCatchUpQuorum { .. } => {
                ControlOperation::WaitForCatchUpQuorum
            }
            DurableReplicaAction::UpdateCurrentConfiguration { .. } => {
                ControlOperation::UpdateCurrentConfiguration
            }
            DurableReplicaAction::BuildReplica { .. } => ControlOperation::BuildReplica,
            DurableReplicaAction::RemoveReplica { .. } => ControlOperation::RemoveReplica,
            DurableReplicaAction::OnDataLoss { .. } => ControlOperation::OnDataLoss,
            DurableReplicaAction::RecordElectionConfiguration { .. } => {
                ControlOperation::RecordElectionConfiguration
            }
        }
    }
}

#[async_trait]
impl ReplicaHandle for ObservedHandle {
    fn id(&self) -> ReplicaId {
        self.inner.id()
    }

    fn instance_id(&self) -> ReplicaInstanceId {
        self.inner.instance_id()
    }

    fn current_progress(&self) -> Lsn {
        self.inner.current_progress()
    }

    fn catch_up_capability(&self) -> Lsn {
        self.inner.catch_up_capability()
    }

    fn control_address(&self) -> String {
        self.exposed_control_address
            .clone()
            .unwrap_or_else(|| self.inner.control_address())
    }

    fn replicator_address(&self) -> String {
        self.inner.replicator_address()
    }

    async fn get_status(&self) -> CoreResult<ReplicaStatusInfo> {
        self.record(ControlOperation::GetStatus);
        let call = {
            let mut counts = self.status_call_counts.lock().unwrap();
            let count = counts.entry(self.pod_name.clone()).or_default();
            *count += 1;
            *count
        };
        let targeted_error = {
            let mut failures = self.fail_status_on_call.lock().unwrap();
            failures
                .get(&self.pod_name)
                .filter(|(fail_call, _)| *fail_call == call)
                .copied()
                .map(|(_, error)| {
                    failures.remove(&self.pod_name);
                    error
                })
        };
        if let Some(error) = targeted_error {
            return Err(injected_status_error(error));
        }
        if let Some(error) = self.fail_next_status.lock().unwrap().take() {
            return Err(injected_status_error(error));
        }
        self.inner.get_status().await
    }

    async fn execute_correlated_control_action(
        &self,
        request: CorrelatedControlActionRequest,
    ) -> CoreResult<CorrelatedControlActionAcknowledgement> {
        let operation = Self::operation_for(&request.action);
        self.record(operation);
        if self
            .fail_before_next_durable_action
            .lock()
            .unwrap()
            .as_ref()
            == Some(&operation)
        {
            self.fail_before_next_durable_action.lock().unwrap().take();
            return Err(kuberic_core::error::KubericError::Internal(
                "injected activity failure".into(),
            ));
        }

        let acknowledgement = self
            .inner
            .execute_correlated_control_action(request)
            .await?;
        if self.fail_after_next_durable_action.lock().unwrap().as_ref() == Some(&operation) {
            self.fail_after_next_durable_action.lock().unwrap().take();
            return Err(kuberic_core::error::KubericError::Internal(
                "injected lost activity reply".into(),
            ));
        }
        Ok(acknowledgement)
    }
}

fn injected_status_error(error: InjectedStatusError) -> KubericError {
    match error {
        InjectedStatusError::UnsupportedProtocol => KubericError::RemoteControlProtocolUnsupported(
            "injected unsupported replica-agent protocol".to_string(),
        ),
        InjectedStatusError::MalformedAgentStatus => KubericError::RemoteAgentRequestRejected(
            "injected malformed replica-agent status".to_string(),
        ),
        InjectedStatusError::Unavailable => {
            KubericError::Internal("injected transient status loss".into())
        }
    }
}

async fn execute_with_fresh_fences(
    handle: &dyn ReplicaHandle,
    action_id: &str,
    action: DurableReplicaAction,
) -> CoreResult<CorrelatedControlActionAcknowledgement> {
    let status = handle.get_status().await?;
    let input_signature = action.signature();
    handle
        .execute_correlated_control_action(CorrelatedControlActionRequest {
            protocol_version: kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION,
            action_id: action_id.to_string(),
            input_signature,
            target_replica_id: handle.id(),
            target_instance_id: status.instance_id,
            expected_agent_generation: status.agent.generation,
            expected_control_version: status.agent.control_version,
            observed_runtime_epoch: status.epoch,
            action,
        })
        .await
}

/// Mock ClusterApi that starts real PodRuntime + KV service for each pod.
struct KvClusterApi {
    pods: Mutex<Vec<Pod>>,
    live_pods: Mutex<HashMap<String, LivePod>>,
    /// Preserved data dirs from crashed pods (simulates PVC survival).
    data_dirs: Mutex<HashMap<String, PathBuf>>,
    statuses: Mutex<Vec<KubericSetStatus>>,
    pvcs: Mutex<HashMap<String, PersistentVolumeClaim>>,
    services: Mutex<HashMap<String, Service>>,
    operations: Arc<Mutex<Vec<ControlOperation>>>,
    fail_next_status_patch: Mutex<bool>,
    fail_after_next_status_patch: Mutex<bool>,
    fail_next_status_conflict: Mutex<bool>,
    fail_before_next_durable_action: Arc<Mutex<Option<ControlOperation>>>,
    fail_after_next_durable_action: Arc<Mutex<Option<ControlOperation>>>,
    fail_next_status: Arc<Mutex<Option<InjectedStatusError>>>,
    status_call_counts: Arc<Mutex<HashMap<String, usize>>>,
    fail_status_on_call: Arc<Mutex<HashMap<String, (usize, InjectedStatusError)>>>,
    exposed_control_addresses: Arc<Mutex<HashMap<String, String>>>,
    peer_proxy_handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    removal_clock: ManualRemoveReplicaClock,
    data_loss_behavior: service::DataLossBehavior,
}

impl KvClusterApi {
    fn new() -> Self {
        Self {
            pods: Mutex::new(Vec::new()),
            live_pods: Mutex::new(HashMap::new()),
            data_dirs: Mutex::new(HashMap::new()),
            statuses: Mutex::new(Vec::new()),
            pvcs: Mutex::new(HashMap::new()),
            services: Mutex::new(HashMap::new()),
            operations: Arc::new(Mutex::new(Vec::new())),
            fail_next_status_patch: Mutex::new(false),
            fail_after_next_status_patch: Mutex::new(false),
            fail_next_status_conflict: Mutex::new(false),
            fail_before_next_durable_action: Arc::new(Mutex::new(None)),
            fail_after_next_durable_action: Arc::new(Mutex::new(None)),
            fail_next_status: Arc::new(Mutex::new(None)),
            status_call_counts: Arc::new(Mutex::new(HashMap::new())),
            fail_status_on_call: Arc::new(Mutex::new(HashMap::new())),
            exposed_control_addresses: Arc::new(Mutex::new(HashMap::new())),
            peer_proxy_handles: Mutex::new(Vec::new()),
            removal_clock: ManualRemoveReplicaClock::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64,
            ),
            data_loss_behavior: service::DataLossBehavior::default(),
        }
    }

    fn new_with_data_loss_behavior(behavior: service::DataLossBehavior) -> Self {
        Self {
            data_loss_behavior: behavior,
            ..Self::new()
        }
    }

    fn mark_all_pods_ready(&self) {
        let mut pods = self.pods.lock().unwrap();
        let live = self.live_pods.lock().unwrap();
        for pod in pods.iter_mut() {
            let name = pod.metadata.name.as_deref().unwrap_or("");
            let ip = live
                .get(name)
                .map(|lp| {
                    lp.control_address
                        .strip_prefix("http://")
                        .unwrap_or("")
                        .split(':')
                        .next()
                        .unwrap_or("127.0.0.1")
                        .to_string()
                })
                .unwrap_or_else(|| "127.0.0.1".to_string());
            pod.status = Some(PodStatus {
                conditions: Some(vec![PodCondition {
                    type_: "Ready".to_string(),
                    status: "True".to_string(),
                    ..Default::default()
                }]),
                pod_ip: Some(ip),
                ..Default::default()
            });
        }
    }

    /// Mark a specific pod as not ready (simulates pod failure for
    /// testing reconciler failure detection paths).
    fn mark_pod_not_ready(&self, pod_name: &str) {
        let mut pods = self.pods.lock().unwrap();
        if let Some(pod) = pods
            .iter_mut()
            .find(|p| p.metadata.name.as_deref() == Some(pod_name))
        {
            pod.status = Some(PodStatus {
                conditions: Some(vec![PodCondition {
                    type_: "Ready".to_string(),
                    status: "False".to_string(),
                    ..Default::default()
                }]),
                ..Default::default()
            });
        }
    }

    fn last_status(&self) -> Option<KubericSetStatus> {
        self.statuses.lock().unwrap().last().cloned()
    }

    fn client_address(&self, pod_name: &str) -> Option<String> {
        self.live_pods
            .lock()
            .unwrap()
            .get(pod_name)
            .map(|lp| lp.client_address.clone())
    }

    fn reset_operations(&self) {
        self.operations.lock().unwrap().clear();
    }

    fn operations(&self) -> Vec<ControlOperation> {
        self.operations.lock().unwrap().clone()
    }

    fn fail_next_status_patch(&self) {
        *self.fail_next_status_patch.lock().unwrap() = true;
    }

    fn fail_next_status_conflict(&self) {
        *self.fail_next_status_conflict.lock().unwrap() = true;
    }

    fn fail_before_next_durable_action(&self, operation: ControlOperation) {
        *self.fail_before_next_durable_action.lock().unwrap() = Some(operation);
    }

    fn fail_after_next_durable_action(&self, operation: ControlOperation) {
        *self.fail_after_next_durable_action.lock().unwrap() = Some(operation);
    }

    fn fail_next_status(&self, error: InjectedStatusError) {
        *self.fail_next_status.lock().unwrap() = Some(error);
    }

    fn fail_status_after_successes(
        &self,
        pod_name: &str,
        successful_calls: usize,
        error: InjectedStatusError,
    ) {
        let current = self
            .status_call_counts
            .lock()
            .unwrap()
            .get(pod_name)
            .copied()
            .unwrap_or_default();
        self.fail_status_on_call.lock().unwrap().insert(
            pod_name.to_string(),
            (current + successful_calls + 1, error),
        );
    }

    fn removal_state(&self) -> ReconcilerState {
        ReconcilerState::with_removal_clock(Arc::new(self.removal_clock.clone()))
    }

    fn lifecycle_control(&self, pod_name: &str) -> LifecycleControl {
        self.live_pods
            .lock()
            .unwrap()
            .get(pod_name)
            .unwrap()
            .lifecycle_control
            .clone()
    }

    async fn install_peer_proxy(&self, pod_name: &str) -> PeerProxyControl {
        let target_address = self
            .live_pods
            .lock()
            .unwrap()
            .get(pod_name)
            .unwrap()
            .control_address
            .clone();
        let listener = tokio::net::TcpListener::bind(allocate_unique_address().await)
            .await
            .unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let mode = Arc::new(Mutex::new(PeerProxyMode::Forward));
        let proxy = PeerProxy {
            target_address,
            mode: mode.clone(),
        };
        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(ReplicaLifecyclePeerServer::new(proxy))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        self.peer_proxy_handles.lock().unwrap().push(handle);
        self.exposed_control_addresses
            .lock()
            .unwrap()
            .insert(pod_name.to_string(), address);
        PeerProxyControl { mode }
    }

    /// Simulate a pod crash. Aborts the PodRuntime and service tasks
    /// (gRPC channels break immediately), marks the K8s Pod NotReady,
    /// and removes the LivePod entry. Preserves data_dir (simulates PVC).
    fn crash_pod(&self, pod_name: &str) {
        if let Some(live) = self.live_pods.lock().unwrap().remove(pod_name) {
            live.runtime_shutdown.cancel();
            live._runtime_handle.abort();
            live._lifecycle_relay_handle.abort();
            live._service_handle.abort();
            self.data_dirs
                .lock()
                .unwrap()
                .insert(pod_name.to_string(), live.data_dir);
        }
        self.mark_pod_not_ready(pod_name);
    }

    /// Simulate replacement of a crashed Pod with a new Pod UID.
    async fn restart_pod(&self, pod_name: &str) {
        static INSTANCE_COUNTER: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(1);
        let generation = INSTANCE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let instance_id = ReplicaInstanceId::new(format!("restarted-{pod_name}-{generation}"));
        self.restart_runtime(pod_name, instance_id, true).await;
    }

    /// Simulate a container/process restart inside the same Kubernetes Pod.
    async fn restart_process_same_pod_uid(&self, pod_name: &str) {
        let instance_id = self
            .pods
            .lock()
            .unwrap()
            .iter()
            .find(|pod| pod.metadata.name.as_deref() == Some(pod_name))
            .and_then(|pod| pod.metadata.uid.clone())
            .map(ReplicaInstanceId::new)
            .expect("existing pod UID");
        self.restart_runtime(pod_name, instance_id, false).await;
    }

    async fn restart_runtime(
        &self,
        pod_name: &str,
        instance_id: ReplicaInstanceId,
        replace_pod_uid: bool,
    ) {
        let replica_id: i64 = pod_name.rsplit('-').next().unwrap().parse::<i64>().unwrap() + 1;

        let client_address = allocate_unique_address().await;
        let data_bind = allocate_unique_address().await;
        let data_address = format!("http://{}", data_bind);
        let control_bind = allocate_unique_address().await;

        let bundle = PodRuntime::builder(replica_id)
            .instance_id(instance_id.clone())
            .reply_timeout(Duration::from_secs(5))
            .control_bind(control_bind)
            .data_bind(data_bind)
            .removal_clock(Arc::new(self.removal_clock.clone()))
            .build()
            .await
            .unwrap();

        let control_address = bundle.control_address.clone();

        // Reuse data_dir from crashed pod (PVC re-attach)
        let data_dir = self
            .data_dirs
            .lock()
            .unwrap()
            .remove(pod_name)
            .unwrap_or_else(|| {
                std::env::temp_dir().join("kv-test").join(format!(
                    "restart-{}-{}",
                    pod_name,
                    std::process::id(),
                ))
            });
        let state: SharedState =
            Arc::new(RwLock::new(KvState::open(data_dir.clone()).await.unwrap()));

        let runtime_shutdown = bundle.runtime.shutdown_token();
        let runtime_handle = tokio::spawn(bundle.runtime.serve());
        let lifecycle_control = LifecycleControl::new();
        let (lifecycle_tx, lifecycle_rx) = mpsc::channel(16);
        let lifecycle_relay_handle = tokio::spawn(relay_lifecycle_events(
            bundle.lifecycle_rx,
            lifecycle_tx,
            lifecycle_control.clone(),
        ));
        let st = state.clone();
        let bind = client_address.clone();
        let service_handle = tokio::spawn(service::run_service_with_options_and_data_loss(
            lifecycle_rx,
            st,
            bind,
            kuberic_core::replicator::WalReplicatorOptions::default(),
            self.data_loss_behavior.clone(),
        ));

        tokio::time::sleep(Duration::from_millis(50)).await;

        self.live_pods.lock().unwrap().insert(
            pod_name.to_string(),
            LivePod {
                control_address,
                data_address,
                client_address,
                data_dir,
                lifecycle_control,
                runtime_shutdown,
                state,
                _runtime_handle: runtime_handle,
                _lifecycle_relay_handle: lifecycle_relay_handle,
                _service_handle: service_handle,
            },
        );

        if replace_pod_uid {
            let mut pods = self.pods.lock().unwrap();
            let pod = pods
                .iter_mut()
                .find(|pod| pod.metadata.name.as_deref() == Some(pod_name))
                .unwrap();
            pod.metadata.uid = Some(instance_id.to_string());
        }
        self.mark_all_pods_ready();
    }
}

#[async_trait]
impl ClusterApi for KvClusterApi {
    async fn list_pods(&self, _ns: &str, _sel: &str) -> Result<Vec<Pod>, String> {
        Ok(self.pods.lock().unwrap().clone())
    }

    async fn create_pod(&self, _ns: &str, pod: &Pod) -> Result<(), String> {
        let pod_name = pod.metadata.name.as_deref().unwrap().to_string();

        // Idempotent: skip if pod already exists
        {
            let pods = self.pods.lock().unwrap();
            if pods
                .iter()
                .any(|p| p.metadata.name.as_deref() == Some(pod_name.as_str()))
            {
                return Ok(());
            }
        }

        let replica_id: i64 = pod
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("kuberic.io/pod-index"))
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0)
            + 1;
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let instance_id = ReplicaInstanceId::new(format!("reconciler-{replica_id}-{n}"));

        let client_address = allocate_unique_address().await;

        // Pre-bind data plane port
        let data_bind = allocate_unique_address().await;
        let data_address = format!("http://{}", data_bind);
        let control_bind = allocate_unique_address().await;

        let bundle = PodRuntime::builder(replica_id)
            .instance_id(instance_id.clone())
            .reply_timeout(Duration::from_secs(5))
            .control_bind(control_bind)
            .data_bind(data_bind)
            .removal_clock(Arc::new(self.removal_clock.clone()))
            .build()
            .await
            .unwrap();

        let control_address = bundle.control_address.clone();
        let data_dir = std::env::temp_dir().join("kv-test").join(format!(
            "reconciler-{}-{}-{}",
            replica_id,
            std::process::id(),
            n
        ));
        let state: SharedState =
            Arc::new(RwLock::new(KvState::open(data_dir.clone()).await.unwrap()));

        let runtime_shutdown = bundle.runtime.shutdown_token();
        let runtime_handle = tokio::spawn(bundle.runtime.serve());
        let lifecycle_control = LifecycleControl::new();
        let (lifecycle_tx, lifecycle_rx) = mpsc::channel(16);
        let lifecycle_relay_handle = tokio::spawn(relay_lifecycle_events(
            bundle.lifecycle_rx,
            lifecycle_tx,
            lifecycle_control.clone(),
        ));
        let st = state.clone();
        let bind = client_address.clone();
        let service_handle = tokio::spawn(service::run_service_with_options_and_data_loss(
            lifecycle_rx,
            st,
            bind,
            kuberic_core::replicator::WalReplicatorOptions::default(),
            self.data_loss_behavior.clone(),
        ));

        tokio::time::sleep(Duration::from_millis(50)).await;

        self.live_pods.lock().unwrap().insert(
            pod_name,
            LivePod {
                control_address,
                data_address,
                client_address,
                data_dir,
                lifecycle_control,
                runtime_shutdown,
                state,
                _runtime_handle: runtime_handle,
                _lifecycle_relay_handle: lifecycle_relay_handle,
                _service_handle: service_handle,
            },
        );

        let mut stored_pod = pod.clone();
        stored_pod.metadata.uid = Some(instance_id.to_string());
        self.pods.lock().unwrap().push(stored_pod);
        Ok(())
    }

    async fn delete_pod(
        &self,
        _ns: &str,
        pod_name: &str,
        expected_uid: &str,
    ) -> Result<(), String> {
        let mut pods = self.pods.lock().unwrap();
        let Some(index) = pods
            .iter()
            .position(|pod| pod.metadata.name.as_deref() == Some(pod_name))
        else {
            return Ok(());
        };
        if pods[index].metadata.uid.as_deref() != Some(expected_uid) {
            return Err("pod UID precondition failed".to_string());
        }
        pods.remove(index);
        if let Some(live) = self.live_pods.lock().unwrap().remove(pod_name) {
            live.runtime_shutdown.cancel();
            live._runtime_handle.abort();
            live._lifecycle_relay_handle.abort();
            live._service_handle.abort();
        }
        Ok(())
    }

    async fn patch_pod_labels(
        &self,
        _ns: &str,
        pod_name: &str,
        labels: BTreeMap<String, String>,
    ) -> Result<(), String> {
        let mut pods = self.pods.lock().unwrap();
        if let Some(pod) = pods
            .iter_mut()
            .find(|p| p.metadata.name.as_deref() == Some(pod_name))
        {
            let pod_labels = pod.metadata.labels.get_or_insert_with(BTreeMap::new);
            pod_labels.extend(labels);
        }
        Ok(())
    }

    async fn patch_pod_labels_if_uid(
        &self,
        _ns: &str,
        pod_name: &str,
        expected_uid: &str,
        labels: BTreeMap<String, String>,
    ) -> Result<(), String> {
        let mut pods = self.pods.lock().unwrap();
        let pod = pods
            .iter_mut()
            .find(|pod| pod.metadata.name.as_deref() == Some(pod_name))
            .ok_or_else(|| "pod not found".to_string())?;
        if pod.metadata.uid.as_deref() != Some(expected_uid) {
            return Err("pod UID precondition failed".to_string());
        }
        let current = pod.metadata.labels.get_or_insert_with(BTreeMap::new);
        current.extend(labels);
        Ok(())
    }

    async fn patch_set_status(
        &self,
        _ns: &str,
        _name: &str,
        status: &KubericSetStatus,
        _expected_resource_version: Option<&str>,
    ) -> Result<(), String> {
        if std::mem::take(&mut *self.fail_next_status_conflict.lock().unwrap()) {
            return Err("resource version conflict".to_string());
        }
        if std::mem::take(&mut *self.fail_next_status_patch.lock().unwrap()) {
            return Err("injected status persistence failure".to_string());
        }
        self.statuses.lock().unwrap().push(status.clone());
        if std::mem::take(&mut *self.fail_after_next_status_patch.lock().unwrap()) {
            return Err("injected status response loss after apply".to_string());
        }
        Ok(())
    }

    async fn create_replica_handle(
        &self,
        replica_id: ReplicaId,
        pod: &Pod,
        _spec: &KubericSetSpec,
    ) -> Result<Box<dyn ReplicaHandle>, String> {
        let pod_name = pod.metadata.name.as_deref().unwrap();
        let (control_addr, data_addr) = {
            let live = self.live_pods.lock().unwrap();
            let lp = live
                .get(pod_name)
                .ok_or_else(|| format!("no live pod for {}", pod_name))?;
            (lp.control_address.clone(), lp.data_address.clone())
        };

        let instance_id = pod
            .metadata
            .uid
            .clone()
            .map(ReplicaInstanceId::new)
            .ok_or_else(|| format!("pod {pod_name} has no UID"))?;
        let handle = GrpcReplicaHandle::connect(replica_id, instance_id, control_addr, data_addr)
            .await
            .map_err(|e| e.to_string())?;

        Ok(Box::new(ObservedHandle {
            inner: Box::new(handle),
            pod_name: pod_name.to_string(),
            exposed_control_address: self
                .exposed_control_addresses
                .lock()
                .unwrap()
                .get(pod_name)
                .cloned(),
            operations: self.operations.clone(),
            fail_before_next_durable_action: self.fail_before_next_durable_action.clone(),
            fail_after_next_durable_action: self.fail_after_next_durable_action.clone(),
            fail_next_status: self.fail_next_status.clone(),
            status_call_counts: self.status_call_counts.clone(),
            fail_status_on_call: self.fail_status_on_call.clone(),
        }))
    }

    async fn get_pvc(&self, _ns: &str, name: &str) -> Result<PersistentVolumeClaim, String> {
        self.pvcs
            .lock()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| format!("pvc {} not found", name))
    }

    async fn create_pvc(&self, _ns: &str, pvc: &PersistentVolumeClaim) -> Result<(), String> {
        let name = pvc.metadata.name.as_deref().unwrap().to_string();
        self.pvcs
            .lock()
            .unwrap()
            .entry(name)
            .or_insert_with(|| pvc.clone());
        Ok(())
    }

    async fn list_pvcs(&self, _ns: &str, _sel: &str) -> Result<Vec<PersistentVolumeClaim>, String> {
        Ok(self.pvcs.lock().unwrap().values().cloned().collect())
    }

    async fn delete_pvc(&self, _ns: &str, name: &str) -> Result<(), String> {
        self.pvcs.lock().unwrap().remove(name);
        Ok(())
    }

    async fn get_service(&self, _ns: &str, name: &str) -> Result<Service, String> {
        self.services
            .lock()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| format!("service {} not found", name))
    }

    async fn create_service(&self, _ns: &str, svc: &Service) -> Result<(), String> {
        let name = svc.metadata.name.as_deref().unwrap().to_string();
        self.services
            .lock()
            .unwrap()
            .entry(name)
            .or_insert_with(|| svc.clone());
        Ok(())
    }

    async fn delete_service(&self, _ns: &str, name: &str) -> Result<(), String> {
        self.services.lock().unwrap().remove(name);
        Ok(())
    }
}

fn make_set(name: &str, replicas: i32, status: Option<KubericSetStatus>) -> KubericSet {
    make_set_with_min(name, replicas, 1, status)
}

fn make_set_with_min(
    name: &str,
    replicas: i32,
    min_replicas: i32,
    status: Option<KubericSetStatus>,
) -> KubericSet {
    KubericSet {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some("default".to_string()),
            uid: Some("test-uid".to_string()),
            ..Default::default()
        },
        spec: KubericSetSpec {
            replicas,
            min_replicas,
            image: "test:latest".to_string(),
            failover_delay: 0,
            switchover_delay: 3600,
            port: 8080,
            control_port: 9090,
            data_port: 9091,
            storage: "256Mi".to_string(),
            pvc_retention_policy: PvcRetentionPolicy::Delete,
        },
        status,
    }
}

async fn connect_kv(
    addr: &str,
) -> proto::kv_store_client::KvStoreClient<tonic::transport::Channel> {
    for attempt in 0..30 {
        match proto::kv_store_client::KvStoreClient::connect(format!("http://{}", addr)).await {
            Ok(c) => return c,
            Err(_) if attempt < 29 => tokio::time::sleep(Duration::from_millis(100)).await,
            Err(e) => panic!("connect failed: {}", e),
        }
    }
    unreachable!()
}

/// Retry a Get call — the client gRPC service may not be registered yet
/// after a role transition (returns Unimplemented until ready).
async fn retry_get(
    client: &mut proto::kv_store_client::KvStoreClient<tonic::transport::Channel>,
    key: &str,
) -> tonic::Response<proto::GetResponse> {
    for attempt in 0..30 {
        match client.get(proto::GetRequest { key: key.into() }).await {
            Ok(resp) => return resp,
            Err(e) if attempt < 29 => {
                tracing::debug!(attempt, error = %e, "Get retry");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("Get({}) failed after retries: {}", key, e),
        }
    }
    unreachable!()
}

async fn create_healthy_set(
    api: &KvClusterApi,
    state: &ReconcilerState,
    name: &str,
    replicas: i32,
) -> KubericSetStatus {
    reconcile_set(&make_set(name, replicas, None), api, state)
        .await
        .unwrap();
    api.mark_all_pods_ready();
    reconcile_set(
        &make_set(
            name,
            replicas,
            Some(KubericSetStatus {
                phase: Phase::Creating,
                ..Default::default()
            }),
        ),
        api,
        state,
    )
    .await
    .unwrap();
    let mut status =
        drive_create_partition(api, state, name, replicas, api.last_status().unwrap()).await;
    for _ in 0..60 {
        if status.stable_election_metadata_refresh.is_none() {
            break;
        }
        reconcile_set(&make_set(name, replicas, Some(status.clone())), api, state)
            .await
            .unwrap();
        status = api.last_status().unwrap();
    }
    assert_eq!(status.phase, Phase::Healthy, "{status:?}");
    assert!(status.stable_election_metadata_refresh.is_none());
    assert_eq!(
        status
            .stable_snapshot
            .as_ref()
            .expect("creation must persist a stable snapshot")
            .members
            .len(),
        replicas as usize
    );
    assert!(
        status
            .stable_snapshot
            .as_ref()
            .unwrap()
            .members
            .iter()
            .all(|member| member.election_metadata.is_some())
    );
    status
}

async fn drive_create_partition(
    api: &KvClusterApi,
    state: &ReconcilerState,
    name: &str,
    replicas: i32,
    status: KubericSetStatus,
) -> KubericSetStatus {
    drive_create_partition_with_min(api, state, name, replicas, 1, status).await
}

async fn drive_create_partition_with_min(
    api: &KvClusterApi,
    state: &ReconcilerState,
    name: &str,
    replicas: i32,
    min_replicas: i32,
    mut status: KubericSetStatus,
) -> KubericSetStatus {
    for _ in 0..180 {
        if status.phase == Phase::Healthy {
            return status;
        }
        reconcile_set(
            &make_set_with_min(name, replicas, min_replicas, Some(status.clone())),
            api,
            state,
        )
        .await
        .unwrap();
        status = api.last_status().unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("durable partition creation did not reach Healthy");
}

async fn advance_create_until_fence_target(
    api: &KvClusterApi,
    name: &str,
    replicas: i32,
    mut status: KubericSetStatus,
    target_id: i64,
) -> KubericSetStatus {
    for _ in 0..40 {
        if status
            .operation
            .as_ref()
            .and_then(|operation| operation.pending_action.as_ref())
            .is_some_and(|pending| {
                pending.kind == DurableActionKind::CreateFencePod && pending.target_id == target_id
            })
        {
            return status;
        }
        reconcile_set(
            &make_set(name, replicas, Some(status.clone())),
            api,
            &ReconcilerState::default(),
        )
        .await
        .unwrap();
        status = api.last_status().unwrap();
    }
    panic!("creation did not reach fence intent for replica {target_id}");
}

async fn drive_switchover(
    api: &KvClusterApi,
    state: &ReconcilerState,
    name: &str,
    replicas: i32,
    mut status: KubericSetStatus,
) -> KubericSetStatus {
    for _ in 0..80 {
        reconcile_set(&make_set(name, replicas, Some(status.clone())), api, state)
            .await
            .unwrap();
        status = api.last_status().unwrap();
        if status.phase == Phase::Healthy && status.stable_election_metadata_refresh.is_none() {
            return status;
        }
    }
    panic!("durable switchover did not reach a terminal healthy status");
}

async fn drive_add_replica(
    api: &KvClusterApi,
    state: &ReconcilerState,
    name: &str,
    replicas: i32,
    mut status: KubericSetStatus,
) -> KubericSetStatus {
    for _ in 0..120 {
        reconcile_set(&make_set(name, replicas, Some(status.clone())), api, state)
            .await
            .unwrap();
        status = api.last_status().unwrap();
        if status.phase == Phase::Healthy
            && status
                .stable_snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.members.len() == replicas as usize)
            && status.stable_election_metadata_refresh.is_none()
        {
            reconcile_set(&make_set(name, replicas, Some(status.clone())), api, state)
                .await
                .unwrap();
            return api.last_status().unwrap();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("durable replica add did not reach a terminal healthy status: {status:?}");
}

async fn advance_until_pending_action(
    api: &KvClusterApi,
    name: &str,
    replicas: i32,
    mut status: KubericSetStatus,
    kind: DurableActionKind,
) -> KubericSetStatus {
    for _ in 0..60 {
        if status
            .operation
            .as_ref()
            .and_then(|operation| operation.pending_action.as_ref())
            .is_some_and(|action| action.kind == kind && action.dispatch_agent_generation.is_some())
        {
            return status;
        }

        reconcile_set(
            &make_set(name, replicas, Some(status.clone())),
            api,
            &ReconcilerState::default(),
        )
        .await
        .unwrap();
        status = api.last_status().unwrap();
    }
    panic!("durable switchover did not reach pending action {kind:?}");
}

async fn advance_add_until_phase(
    api: &KvClusterApi,
    state: &ReconcilerState,
    name: &str,
    replicas: i32,
    mut status: KubericSetStatus,
    phase: DurableOperationPhase,
) -> KubericSetStatus {
    for _ in 0..100 {
        if status
            .operation
            .as_ref()
            .is_some_and(|operation| operation.phase == phase && operation.pending_action.is_none())
        {
            return status;
        }
        reconcile_set(&make_set(name, replicas, Some(status.clone())), api, state)
            .await
            .unwrap();
        status = api.last_status().unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("durable add did not reach phase {phase:?}");
}

async fn drive_operation_to_healthy(
    api: &KvClusterApi,
    name: &str,
    replicas: i32,
    status: KubericSetStatus,
) -> KubericSetStatus {
    drive_operation_to_healthy_with_state(api, &ReconcilerState::default(), name, replicas, status)
        .await
}

async fn drive_operation_to_healthy_with_state(
    api: &KvClusterApi,
    state: &ReconcilerState,
    name: &str,
    replicas: i32,
    mut status: KubericSetStatus,
) -> KubericSetStatus {
    for _ in 0..120 {
        reconcile_set(&make_set(name, replicas, Some(status.clone())), api, state)
            .await
            .unwrap();
        status = api.last_status().unwrap();
        if status.phase == Phase::Healthy {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("durable operation did not return to Healthy: {status:?}");
}

async fn advance_remove_until_operation_phase(
    api: &KvClusterApi,
    state: &ReconcilerState,
    name: &str,
    replicas: i32,
    mut status: KubericSetStatus,
    phase: DurableOperationPhase,
) -> KubericSetStatus {
    for _ in 0..160 {
        if status
            .operation
            .as_ref()
            .is_some_and(|operation| operation.phase == phase)
        {
            return status;
        }
        reconcile_set(&make_set(name, replicas, Some(status.clone())), api, state)
            .await
            .unwrap();
        status = api.last_status().unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("durable removal did not reach operation phase {phase:?}: {status:?}");
}

async fn advance_remove_until_coordinator_phase(
    api: &KvClusterApi,
    state: &ReconcilerState,
    name: &str,
    replicas: i32,
    mut status: KubericSetStatus,
    phase: RemoveReplicaCoordinatorPhaseStatus,
) -> KubericSetStatus {
    for _ in 0..200 {
        if status
            .operation
            .as_ref()
            .and_then(|operation| operation.remove_intent.as_ref())
            .is_some_and(|intent| intent.last_observed_phase == Some(phase))
        {
            return status;
        }
        reconcile_set(&make_set(name, replicas, Some(status.clone())), api, state)
            .await
            .unwrap();
        status = api.last_status().unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("durable removal did not reach coordinator phase {phase:?}: {status:?}");
}

async fn advance_remove_until_dispatch_authorized(
    api: &KvClusterApi,
    state: &ReconcilerState,
    name: &str,
    replicas: i32,
    mut status: KubericSetStatus,
) -> KubericSetStatus {
    for _ in 0..160 {
        if status
            .operation
            .as_ref()
            .and_then(|operation| operation.pending_action.as_ref())
            .is_some_and(|pending| {
                pending.kind == DurableActionKind::RemoveReplicaIntent
                    && pending.dispatch_agent_generation.is_some()
            })
        {
            return status;
        }
        reconcile_set(&make_set(name, replicas, Some(status.clone())), api, state)
            .await
            .unwrap();
        status = api.last_status().unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("durable removal did not persist coarse dispatch authorization: {status:?}");
}

async fn live_remove_progress(
    api: &KvClusterApi,
    name: &str,
    replicas: i32,
    status: &KubericSetStatus,
) -> Option<RemoveReplicaProgress> {
    let operation = status.operation.as_ref()?;
    let intent = operation.remove_intent.as_ref()?;
    let primary_id = operation.old_primary_id;
    let primary = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .find(|pod| {
            pod.metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get("kuberic.io/pod-index"))
                .and_then(|index| index.parse::<i64>().ok())
                .map(|index| index + 1)
                == Some(primary_id)
        })
        .cloned()?;
    let handle = api
        .create_replica_handle(primary_id, &primary, &make_set(name, replicas, None).spec)
        .await
        .ok()?;
    let observed = handle.get_status().await.ok()?;
    observed
        .agent
        .current_action
        .as_ref()
        .into_iter()
        .chain(observed.agent.retained_terminal_actions.iter().rev())
        .find(|observation| observation.action.action_id == intent.action_id)
        .and_then(|observation| observation.action.remove_replica_progress.clone())
}

async fn live_remove_terminal_result(
    api: &KvClusterApi,
    name: &str,
    replicas: i32,
    status: &KubericSetStatus,
) -> Option<RemoveReplicaTerminalResult> {
    let operation = status.operation.as_ref()?;
    let intent = operation.remove_intent.as_ref()?;
    let primary_id = operation.old_primary_id;
    let primary = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .find(|pod| {
            pod.metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get("kuberic.io/pod-index"))
                .and_then(|index| index.parse::<i64>().ok())
                .map(|index| index + 1)
                == Some(primary_id)
        })
        .cloned()?;
    let handle = api
        .create_replica_handle(primary_id, &primary, &make_set(name, replicas, None).spec)
        .await
        .ok()?;
    let observed = handle.get_status().await.ok()?;
    observed
        .agent
        .current_action
        .as_ref()
        .into_iter()
        .chain(observed.agent.retained_terminal_actions.iter().rev())
        .find(|observation| observation.action.action_id == intent.action_id)
        .and_then(|observation| match observation.action.result {
            Some(DurableActionResult::RemoveReplica(result)) => Some(result),
            _ => None,
        })
}

fn control_for_action(kind: DurableActionKind) -> Option<ControlOperation> {
    match kind {
        DurableActionKind::AddReplicaIntent => Some(ControlOperation::AddReplicaIntent),
        DurableActionKind::RemoveReplicaIntent => Some(ControlOperation::RemoveReplicaIntent),
        DurableActionKind::CreateOpenPrimary | DurableActionKind::CreateOpenSecondary => {
            Some(ControlOperation::Open)
        }
        DurableActionKind::CreatePromotePrimary
        | DurableActionKind::CreateAssignSecondaryIdle
        | DurableActionKind::CreateAssignSecondaryActive
        | DurableActionKind::CreateCompensateDemoteCandidate => Some(ControlOperation::ChangeRole),
        DurableActionKind::CreatePrimaryCurrentConfiguration
        | DurableActionKind::CreateCurrentConfiguration
        | DurableActionKind::CreateCompensateRestoreConfiguration => {
            Some(ControlOperation::UpdateCurrentConfiguration)
        }
        DurableActionKind::CreateUpdateSecondaryEpoch => Some(ControlOperation::UpdateEpoch),
        DurableActionKind::CreateBuildSecondary => Some(ControlOperation::BuildReplica),
        DurableActionKind::CreateCatchUpConfiguration => {
            Some(ControlOperation::UpdateCatchUpConfiguration)
        }
        DurableActionKind::CreateWaitForCatchUpQuorum => {
            Some(ControlOperation::WaitForCatchUpQuorum)
        }
        DurableActionKind::CreateCompensateRemoveCandidate => Some(ControlOperation::RemoveReplica),
        DurableActionKind::CreateCompensateCloseCandidate => Some(ControlOperation::Close),
        DurableActionKind::FailoverRecordStartingConfiguration
        | DurableActionKind::FailoverRecordElectionConfiguration => {
            Some(ControlOperation::RecordElectionConfiguration)
        }
        DurableActionKind::FailoverUpdateCandidateEpoch
        | DurableActionKind::FailoverUpdateSecondaryEpoch => Some(ControlOperation::UpdateEpoch),
        DurableActionKind::FailoverOnDataLoss => Some(ControlOperation::OnDataLoss),
        DurableActionKind::FailoverPromoteCandidate => Some(ControlOperation::ChangeRole),
        DurableActionKind::FailoverCatchUpConfiguration => {
            Some(ControlOperation::UpdateCatchUpConfiguration)
        }
        DurableActionKind::FailoverWaitForCatchUpQuorum => {
            Some(ControlOperation::WaitForCatchUpQuorum)
        }
        DurableActionKind::FailoverCurrentConfiguration => {
            Some(ControlOperation::UpdateCurrentConfiguration)
        }
        _ => None,
    }
}

async fn drive_operation_with_lost_replies(
    api: &KvClusterApi,
    name: &str,
    replicas: i32,
    mut status: KubericSetStatus,
    expected: &[DurableActionKind],
) -> (KubericSetStatus, Vec<DurableActionKind>) {
    let mut injected = Vec::new();
    for _ in 0..120 {
        if let Some(pending) = status
            .operation
            .as_ref()
            .and_then(|operation| operation.pending_action.as_ref())
            && pending.attempts == 0
            && expected.contains(&pending.kind)
            && !injected.contains(&pending.kind)
            && let Some(control) = control_for_action(pending.kind)
        {
            api.fail_after_next_durable_action(control);
            injected.push(pending.kind);
        }
        reconcile_set(
            &make_set(name, replicas, Some(status.clone())),
            api,
            &ReconcilerState::default(),
        )
        .await
        .unwrap();
        status = api.last_status().unwrap();
        if status.phase == Phase::Healthy {
            return (status, injected);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("durable operation with lost replies did not return to Healthy");
}

async fn live_replica_statuses(
    api: &KvClusterApi,
    set_name: &str,
    replicas: i32,
) -> Vec<ReplicaStatusInfo> {
    let set = make_set(set_name, replicas, api.last_status());
    let pods = api.pods.lock().unwrap().clone();
    let mut statuses = Vec::new();
    for pod in pods {
        let replica_id = pod
            .metadata
            .labels
            .as_ref()
            .and_then(|labels| labels.get("kuberic.io/pod-index"))
            .unwrap()
            .parse::<i64>()
            .unwrap()
            + 1;
        let handle = api
            .create_replica_handle(replica_id, &pod, &set.spec)
            .await
            .unwrap();
        statuses.push(handle.get_status().await.unwrap());
    }
    statuses
}

fn assert_status_reads_only(operations: &[ControlOperation]) {
    for forbidden in [
        ControlOperation::Open,
        ControlOperation::Close,
        ControlOperation::ChangeRole,
        ControlOperation::UpdateEpoch,
        ControlOperation::UpdateCatchUpConfiguration,
        ControlOperation::UpdateCurrentConfiguration,
        ControlOperation::WaitForCatchUpQuorum,
        ControlOperation::BuildReplica,
        ControlOperation::RemoveReplica,
        ControlOperation::OnDataLoss,
        ControlOperation::RevokeWriteStatus,
    ] {
        assert!(
            !operations.contains(&forbidden),
            "recovery invoked mutating RPC {forbidden:?}: {operations:?}"
        );
    }

    assert!(
        operations
            .iter()
            .all(|operation| *operation == ControlOperation::GetStatus)
    );
}

fn assert_one_coarse_remove_intent_and_no_fine_grained_controls(operations: &[ControlOperation]) {
    assert_eq!(
        operations
            .iter()
            .filter(|operation| **operation == ControlOperation::RemoveReplicaIntent)
            .count(),
        1,
        "one coarse remove intent must be production-dispatched: {operations:?}"
    );
    assert!(
        operations.iter().all(|operation| matches!(
            operation,
            ControlOperation::GetStatus | ControlOperation::RemoveReplicaIntent
        )),
        "operator dispatched a fine-grained removal control operation: {operations:?}"
    );
}

fn assert_no_same_epoch_primary_restoration(operations: &[ControlOperation]) {
    assert!(
        !operations.contains(&ControlOperation::Open),
        "removal recovery issued same-epoch Open restoration: {operations:?}"
    );
    assert!(
        !operations.contains(&ControlOperation::ChangeRole),
        "removal recovery issued same-epoch Primary restoration: {operations:?}"
    );
}

fn reconcile_error(
    result: Result<kuberic_operator::reconciler::ReconcileAction, String>,
) -> String {
    match result {
        Ok(_) => panic!("reconcile unexpectedly succeeded"),
        Err(error) => error,
    }
}

fn assert_stable_snapshot(api: &KvClusterApi, status: &KubericSetStatus, member_count: usize) {
    let snapshot = status
        .stable_snapshot
        .as_ref()
        .expect("stable operation must persist a snapshot");
    assert_eq!(snapshot.epoch, status.epoch);
    assert_eq!(snapshot.members.len(), member_count);
    assert_eq!(snapshot.write_quorum, member_count as u32 / 2 + 1);
    assert_eq!(
        snapshot
            .members
            .iter()
            .filter(|member| member.role == StableReplicaRoleStatus::Primary)
            .count(),
        1
    );
    assert_eq!(
        snapshot
            .members
            .iter()
            .find(|member| member.role == StableReplicaRoleStatus::Primary)
            .unwrap()
            .id,
        snapshot.primary_id
    );

    let pods = api.pods.lock().unwrap();
    let mut member_ids = HashSet::new();
    let mut member_instances = HashSet::new();
    for member in &snapshot.members {
        assert!(member_ids.insert(member.id), "duplicate snapshot member ID");
        assert!(
            member_instances.insert(member.instance_id.as_str()),
            "duplicate snapshot member incarnation"
        );
        let pod = pods
            .iter()
            .find(|pod| {
                pod.metadata
                    .labels
                    .as_ref()
                    .and_then(|labels| labels.get("kuberic.io/pod-index"))
                    .and_then(|index| index.parse::<i64>().ok())
                    .map(|index| index + 1)
                    == Some(member.id)
            })
            .expect("every snapshot member must have a current pod");
        assert_eq!(
            member.instance_id,
            pod.metadata.uid.as_deref().unwrap(),
            "snapshot incarnation must exactly match current pod UID"
        );
        let expected_role = if member.id == snapshot.primary_id {
            StableReplicaRoleStatus::Primary
        } else {
            StableReplicaRoleStatus::ActiveSecondary
        };
        assert_eq!(member.role, expected_role);
    }
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_operator_restart_recovers_read_only_then_switches_and_scales() {
    let api = KvClusterApi::new();
    let initial_state = ReconcilerState::default();
    let status = create_healthy_set(&api, &initial_state, "recover", 3).await;
    let original_primary = status.current_primary.clone().unwrap();

    let mut kv = connect_kv(&api.client_address(&original_primary).unwrap()).await;
    kv.put(proto::PutRequest {
        key: "before-restart".into(),
        value: "durable".into(),
    })
    .await
    .unwrap();

    // Lose only operator process memory. Pods and their real runtimes remain.
    let recovered_state = ReconcilerState::default();
    api.reset_operations();
    reconcile_set(
        &make_set("recover", 3, Some(status.clone())),
        &api,
        &recovered_state,
    )
    .await
    .unwrap();
    assert_status_reads_only(&api.operations());

    // Recovery neither reopens nor changes the existing replicas.
    kv.put(proto::PutRequest {
        key: "after-restart".into(),
        value: "still-open".into(),
    })
    .await
    .unwrap();

    let target = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .map(|pod| pod.metadata.name.clone().unwrap())
        .find(|pod_name| pod_name != &original_primary)
        .unwrap();
    reconcile_set(
        &make_set(
            "recover",
            3,
            Some(KubericSetStatus {
                target_primary: Some(target.clone()),
                ..status
            }),
        ),
        &api,
        &recovered_state,
    )
    .await
    .unwrap();
    let switching = api.last_status().unwrap();
    assert_eq!(switching.phase, Phase::Switchover);
    let switched = drive_switchover(&api, &recovered_state, "recover", 3, switching).await;
    assert_eq!(switched.current_primary.as_deref(), Some(target.as_str()));
    assert_eq!(
        switched.stable_snapshot.as_ref().unwrap().primary_id,
        switched
            .stable_snapshot
            .as_ref()
            .unwrap()
            .members
            .iter()
            .find(|member| member.instance_id
                == api
                    .pods
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|pod| pod.metadata.name.as_deref() == Some(target.as_str()))
                    .unwrap()
                    .metadata
                    .uid
                    .clone()
                    .unwrap())
            .unwrap()
            .id
    );

    // First loop creates the fourth pod; the next commits and persists it.
    let scale_request = make_set("recover", 4, Some(switched.clone()));
    reconcile_set(&scale_request, &api, &recovered_state)
        .await
        .unwrap();
    let uncommitted_target = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .find(|pod| {
            pod.metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get("kuberic.io/pod-index"))
                .map(String::as_str)
                == Some("3")
        })
        .unwrap()
        .metadata
        .name
        .clone()
        .unwrap();
    let error = reconcile_set(
        &make_set(
            "recover",
            4,
            Some(KubericSetStatus {
                target_primary: Some(uncommitted_target),
                ..switched.clone()
            }),
        ),
        &api,
        &recovered_state,
    )
    .await
    .err()
    .expect("switchover to an uncommitted scale-up pod must fail");
    assert!(error.contains("not in the committed driver topology"));
    api.mark_all_pods_ready();
    let scaled = drive_add_replica(
        &api,
        &recovered_state,
        "recover",
        4,
        api.last_status().unwrap(),
    )
    .await;
    assert_eq!(scaled.stable_snapshot.as_ref().unwrap().members.len(), 4);
    assert_eq!(scaled.stable_snapshot.as_ref().unwrap().write_quorum, 3);

    let mut new_primary = connect_kv(&api.client_address(&target).unwrap()).await;
    new_primary
        .put(proto::PutRequest {
            key: "after-scale".into(),
            value: "works".into(),
        })
        .await
        .unwrap();
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_operator_restart_recovers_unhealthy_primary_then_fails_over() {
    let api = KvClusterApi::new();
    let initial_state = ReconcilerState::default();
    let status = create_healthy_set(&api, &initial_state, "recover-failover", 3).await;
    let failed_primary = status.current_primary.clone().unwrap();
    api.mark_pod_not_ready(&failed_primary);

    let recovered_state = ReconcilerState::default();
    api.reset_operations();
    reconcile_set(
        &make_set("recover-failover", 3, Some(status)),
        &api,
        &recovered_state,
    )
    .await
    .unwrap();
    assert_status_reads_only(&api.operations());
    let failing = api.last_status().unwrap();
    assert_eq!(failing.phase, Phase::FailingOver);

    let recovered = drive_operation_to_healthy(&api, "recover-failover", 3, failing).await;
    assert_eq!(recovered.phase, Phase::Healthy);
    assert_ne!(
        recovered.current_primary.as_deref(),
        Some(failed_primary.as_str())
    );
    assert_eq!(recovered.stable_snapshot.as_ref().unwrap().members.len(), 2);

    let mut kv = connect_kv(
        &api.client_address(recovered.current_primary.as_deref().unwrap())
            .unwrap(),
    )
    .await;
    kv.put(proto::PutRequest {
        key: "recovered-failover".into(),
        value: "works".into(),
    })
    .await
    .unwrap();
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_recovery_rejects_legacy_and_persisted_live_mismatch_without_mutation() {
    let api = KvClusterApi::new();
    let initial_state = ReconcilerState::default();
    let status = create_healthy_set(&api, &initial_state, "reject-recovery", 3).await;
    let status_count = api.statuses.lock().unwrap().len();

    let mut legacy = status.clone();
    legacy.stable_snapshot = None;
    api.reset_operations();
    let error = reconcile_set(
        &make_set("reject-recovery", 3, Some(legacy)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .err()
    .expect("legacy recovery should fail");
    assert!(error.contains("stable snapshot is absent"));
    assert!(api.operations().is_empty());
    assert_eq!(api.statuses.lock().unwrap().len(), status_count);

    let mut mismatched = status;
    mismatched.stable_snapshot.as_mut().unwrap().members[0].instance_id =
        "persisted-stale-incarnation".into();
    api.reset_operations();
    reconcile_set(
        &make_set("reject-recovery", 3, Some(mismatched)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert_eq!(api.last_status().unwrap().phase, Phase::FailingOver);
    assert!(api.operations().is_empty());
    assert_eq!(api.statuses.lock().unwrap().len(), status_count + 1);
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_post_recovery_pod_identity_drift_is_rejected_before_rpc() {
    let api = KvClusterApi::new();
    let initial_state = ReconcilerState::default();
    let status = create_healthy_set(&api, &initial_state, "identity-drift", 3).await;
    let recovered_state = ReconcilerState::default();
    reconcile_set(
        &make_set("identity-drift", 3, Some(status.clone())),
        &api,
        &recovered_state,
    )
    .await
    .unwrap();

    {
        let mut pods = api.pods.lock().unwrap();
        pods[0]
            .metadata
            .labels
            .as_mut()
            .unwrap()
            .insert("kuberic.io/pod-index".into(), "9".into());
    }
    api.reset_operations();
    reconcile_set(
        &make_set("identity-drift", 3, Some(status.clone())),
        &api,
        &recovered_state,
    )
    .await
    .unwrap();
    assert_eq!(api.last_status().unwrap().phase, Phase::FailingOver);
    assert!(api.operations().is_empty());
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_creation_uses_pod_labels_when_list_is_unordered() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    reconcile_set(&make_set("unordered", 3, None), &api, &state)
        .await
        .unwrap();
    api.pods.lock().unwrap().reverse();
    api.mark_all_pods_ready();

    reconcile_set(
        &make_set(
            "unordered",
            3,
            Some(KubericSetStatus {
                phase: Phase::Creating,
                ..Default::default()
            }),
        ),
        &api,
        &state,
    )
    .await
    .unwrap();
    let status =
        drive_create_partition(&api, &state, "unordered", 3, api.last_status().unwrap()).await;
    assert_eq!(status.current_primary.as_deref(), Some("unordered-0"));
    assert_eq!(status.stable_snapshot.as_ref().unwrap().primary_id, 1);
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_committed_topology_retries_status_before_another_mutation() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let status = create_healthy_set(&api, &state, "persist-retry", 1).await;

    // Create two scale-up pods, then fail persistence after the first one's
    // current configuration is committed.
    let scale_request = make_set("persist-retry", 3, Some(status.clone()));
    reconcile_set(&scale_request, &api, &state).await.unwrap();
    api.mark_all_pods_ready();
    reconcile_set(&scale_request, &api, &state).await.unwrap();
    let finalizing = advance_add_until_phase(
        &api,
        &state,
        "persist-retry",
        3,
        api.last_status().unwrap(),
        DurableOperationPhase::AddFinalize,
    )
    .await;
    api.fail_next_status_patch();
    let error = reconcile_set(
        &make_set("persist-retry", 3, Some(finalizing.clone())),
        &api,
        &state,
    )
    .await
    .err()
    .expect("injected status persistence failure must be returned");
    assert_eq!(error, "injected status persistence failure");

    // The next reconcile retries only the pending status. It must not add the
    // third replica until the first committed add is durable.
    api.reset_operations();
    reconcile_set(
        &make_set("persist-retry", 3, Some(finalizing)),
        &api,
        &state,
    )
    .await
    .unwrap();
    assert!(api.operations().is_empty());
    let persisted_first_add = api.last_status().unwrap();
    assert_stable_snapshot(&api, &persisted_first_add, 2);

    let completed = drive_add_replica(&api, &state, "persist-retry", 3, persisted_first_add).await;
    assert_stable_snapshot(&api, &completed, 3);
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_creation_status_retry_does_not_reopen_replicas() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    reconcile_set(&make_set("create-persist-retry", 1, None), &api, &state)
        .await
        .unwrap();
    api.mark_all_pods_ready();
    let creating = make_set(
        "create-persist-retry",
        1,
        Some(KubericSetStatus {
            phase: Phase::Creating,
            ..Default::default()
        }),
    );

    api.fail_next_status_patch();
    let error = reconcile_set(&creating, &api, &state)
        .await
        .err()
        .expect("injected creation status failure must be returned");
    assert_eq!(error, "injected status persistence failure");
    assert!(
        api.operations().is_empty(),
        "failed creation-intent persistence must perform no runtime activity"
    );

    // The original process retries only the pending creation-intent status.
    api.reset_operations();
    reconcile_set(&creating, &api, &state).await.unwrap();
    assert!(
        api.operations().is_empty(),
        "status retry must not execute Open or any creation RPC"
    );
    let recovered_status = drive_create_partition(
        &api,
        &state,
        "create-persist-retry",
        1,
        api.last_status().unwrap(),
    )
    .await;
    assert_stable_snapshot(&api, &recovered_status, 1);
}

/// Full reconciler test: Pending → Creating → Healthy → write KV data.
#[test_log::test(tokio::test)]
#[serial]
async fn test_reconciler_creates_partition_and_serves_kv() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();

    // Pending → Creating (creates 3 pods with real PodRuntimes)
    let set = make_set("myapp", 3, None);
    reconcile_set(&set, &api, &state).await.unwrap();
    assert_eq!(api.pods.lock().unwrap().len(), 3);

    // Verify PVCs created alongside pods
    assert_eq!(api.pvcs.lock().unwrap().len(), 3);
    assert!(api.pvcs.lock().unwrap().contains_key("myapp-0-data"));
    assert!(api.pvcs.lock().unwrap().contains_key("myapp-1-data"));
    assert!(api.pvcs.lock().unwrap().contains_key("myapp-2-data"));

    // Verify services created (rw, ro, r)
    assert_eq!(api.services.lock().unwrap().len(), 3);
    assert!(api.services.lock().unwrap().contains_key("myapp-rw"));
    assert!(api.services.lock().unwrap().contains_key("myapp-ro"));
    assert!(api.services.lock().unwrap().contains_key("myapp-r"));

    // Mark pods ready
    api.mark_all_pods_ready();

    // Creating checkpoints the durable operation, then each reconcile advances
    // one transition or activity.
    let set = make_set(
        "myapp",
        3,
        Some(KubericSetStatus {
            phase: Phase::Creating,
            ..Default::default()
        }),
    );
    reconcile_set(&set, &api, &state).await.unwrap();

    let status = drive_create_partition(&api, &state, "myapp", 3, api.last_status().unwrap()).await;
    assert_eq!(status.phase, Phase::Healthy);
    assert!(status.current_primary.is_some());
    assert_stable_snapshot(&api, &status, 3);

    // Write via KV API on primary
    let primary_name = status.current_primary.unwrap();
    let client_addr = api.client_address(&primary_name).unwrap();
    let mut kv = connect_kv(&client_addr).await;

    let resp = kv
        .put(proto::PutRequest {
            key: "hello".to_string(),
            value: "reconciler".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(resp.get_ref().lsn, 1);

    let resp = kv
        .get(proto::GetRequest {
            key: "hello".to_string(),
        })
        .await
        .unwrap();
    assert!(resp.get_ref().found);
    assert_eq!(resp.get_ref().value, "reconciler");
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_create_survives_state_loss_and_every_lost_runtime_reply() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    reconcile_set(
        &make_set_with_min("durable-create", 3, 2, None),
        &api,
        &state,
    )
    .await
    .unwrap();
    api.mark_all_pods_ready();
    assert!(api.pods.lock().unwrap().iter().all(|pod| {
        pod.metadata
            .labels
            .as_ref()
            .and_then(|labels| labels.get("kuberic.io/role"))
            .is_some_and(|role| role == "bootstrap")
    }));

    reconcile_set(
        &make_set_with_min(
            "durable-create",
            3,
            2,
            Some(KubericSetStatus {
                phase: Phase::Creating,
                ..Default::default()
            }),
        ),
        &api,
        &state,
    )
    .await
    .unwrap();
    let mut status = api.last_status().unwrap();
    let mut injected_action_ids = HashSet::new();
    let mut committed_sizes = Vec::new();

    for _ in 0..260 {
        if let Some(operation) = status.operation.as_ref() {
            if let Some(snapshot) = operation.committed_snapshot.as_ref()
                && committed_sizes.last().copied() != Some(snapshot.members.len())
            {
                committed_sizes.push(snapshot.members.len());
            }
            if operation
                .committed_snapshot
                .as_ref()
                .is_none_or(|snapshot| snapshot.members.len() < 2)
            {
                assert!(api.pods.lock().unwrap().iter().all(|pod| {
                    pod.metadata
                        .labels
                        .as_ref()
                        .and_then(|labels| labels.get("kuberic.io/role"))
                        .is_some_and(|role| role == "bootstrap")
                }));
            }
            if let Some(pending) = operation.pending_action.as_ref()
                && let Some(control) = control_for_action(pending.kind)
                && injected_action_ids.insert(pending.action_id.clone())
            {
                api.fail_after_next_durable_action(control);
            }
        }

        reconcile_set(
            &make_set_with_min("durable-create", 3, 2, Some(status.clone())),
            &api,
            &ReconcilerState::default(),
        )
        .await
        .unwrap();
        status = api.last_status().unwrap();
        if status.phase == Phase::Healthy {
            break;
        }
        assert!(
            status.stable_snapshot.is_none(),
            "partial bootstrap authority must remain inside the creation operation"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(status.phase, Phase::Healthy);
    assert_eq!(committed_sizes, vec![1, 2, 3]);
    assert_eq!(injected_action_ids.len(), 19);
    assert_stable_snapshot(&api, &status, 3);
    let operations = api.operations();
    assert_eq!(
        operations
            .iter()
            .filter(|operation| **operation == ControlOperation::Open)
            .count(),
        3
    );
    assert_eq!(
        operations
            .iter()
            .filter(|operation| **operation == ControlOperation::BuildReplica)
            .count(),
        2
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_create_status_conflict_prevents_first_open() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    reconcile_set(&make_set("create-conflict", 1, None), &api, &state)
        .await
        .unwrap();
    api.mark_all_pods_ready();
    api.reset_operations();
    api.fail_next_status_conflict();

    let error = match reconcile_set(
        &make_set(
            "create-conflict",
            1,
            Some(KubericSetStatus {
                phase: Phase::Creating,
                ..Default::default()
            }),
        ),
        &api,
        &state,
    )
    .await
    {
        Ok(_) => panic!("status conflict unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(error.contains("conflict"));
    assert!(
        api.operations().is_empty(),
        "creation activity ran before intent was durably persisted"
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_create_fences_legacy_serving_labels_before_open() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    reconcile_set(&make_set("create-fence-labels", 2, None), &api, &state)
        .await
        .unwrap();
    api.mark_all_pods_ready();
    {
        let mut pods = api.pods.lock().unwrap();
        for pod in pods.iter_mut() {
            let index = pod
                .metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get("kuberic.io/pod-index"))
                .unwrap()
                .parse::<i32>()
                .unwrap();
            pod.metadata.labels.as_mut().unwrap().insert(
                "kuberic.io/role".to_string(),
                if index == 0 { "primary" } else { "secondary" }.to_string(),
            );
        }
    }
    reconcile_set(
        &make_set(
            "create-fence-labels",
            2,
            Some(KubericSetStatus {
                phase: Phase::Creating,
                ..Default::default()
            }),
        ),
        &api,
        &state,
    )
    .await
    .unwrap();
    api.reset_operations();
    let mut status = api.last_status().unwrap();
    for _ in 0..20 {
        if status
            .operation
            .as_ref()
            .and_then(|operation| operation.pending_action.as_ref())
            .is_some_and(|pending| pending.kind == DurableActionKind::CreateOpenPrimary)
        {
            break;
        }
        reconcile_set(
            &make_set("create-fence-labels", 2, Some(status.clone())),
            &api,
            &ReconcilerState::default(),
        )
        .await
        .unwrap();
        status = api.last_status().unwrap();
    }
    assert!(api.pods.lock().unwrap().iter().all(|pod| {
        pod.metadata
            .labels
            .as_ref()
            .and_then(|labels| labels.get("kuberic.io/role"))
            .is_some_and(|role| role == "bootstrap")
    }));
    assert!(
        !api.operations().contains(&ControlOperation::Open),
        "runtime Open ran before legacy serving labels were fenced"
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_create_unavailable_fence_target_fails_closed_before_commit() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    reconcile_set(&make_set("create-fence-missing", 2, None), &api, &state)
        .await
        .unwrap();
    api.mark_all_pods_ready();
    reconcile_set(
        &make_set(
            "create-fence-missing",
            2,
            Some(KubericSetStatus {
                phase: Phase::Creating,
                ..Default::default()
            }),
        ),
        &api,
        &state,
    )
    .await
    .unwrap();
    let mut pending = advance_create_until_fence_target(
        &api,
        "create-fence-missing",
        2,
        api.last_status().unwrap(),
        2,
    )
    .await;
    let target_name = "create-fence-missing-1";
    api.crash_pod(target_name);
    pending
        .operation
        .as_mut()
        .unwrap()
        .pending_action
        .as_mut()
        .unwrap()
        .deadline_unix_seconds = 0;
    api.reset_operations();
    reconcile_set(
        &make_set("create-fence-missing", 2, Some(pending)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert_status_reads_only(&api.operations());
    let poisoned = api.last_status().unwrap();
    assert_eq!(poisoned.phase, Phase::Creating);
    assert_eq!(
        poisoned.operation.as_ref().unwrap().phase,
        DurableOperationPhase::Poisoned
    );
    assert!(
        poisoned
            .operation
            .as_ref()
            .unwrap()
            .committed_snapshot
            .is_none()
    );
    api.reset_operations();
    reconcile_set(
        &make_set("create-fence-missing", 2, Some(poisoned)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert!(
        api.operations().is_empty(),
        "poisoned fence failure must not livelock or dispatch destructive activity"
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_create_unavailable_fence_target_preserves_committed_primary() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    reconcile_set(&make_set("create-fence-partial", 2, None), &api, &state)
        .await
        .unwrap();
    api.mark_all_pods_ready();
    reconcile_set(
        &make_set(
            "create-fence-partial",
            2,
            Some(KubericSetStatus {
                phase: Phase::Creating,
                ..Default::default()
            }),
        ),
        &api,
        &state,
    )
    .await
    .unwrap();
    let mut status = api.last_status().unwrap();
    for _ in 0..100 {
        if status
            .operation
            .as_ref()
            .and_then(|operation| operation.committed_snapshot.as_ref())
            .is_some_and(|snapshot| snapshot.members.len() == 1)
        {
            break;
        }
        reconcile_set(
            &make_set("create-fence-partial", 2, Some(status.clone())),
            &api,
            &ReconcilerState::default(),
        )
        .await
        .unwrap();
        status = api.last_status().unwrap();
    }
    let committed = status
        .operation
        .as_ref()
        .unwrap()
        .committed_snapshot
        .clone()
        .unwrap();
    status.operation.as_mut().unwrap().phase = DurableOperationPhase::Failed;
    status.operation.as_mut().unwrap().pending_action = None;
    reconcile_set(
        &make_set("create-fence-partial", 2, Some(status)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    let pending = advance_create_until_fence_target(
        &api,
        "create-fence-partial",
        2,
        api.last_status().unwrap(),
        2,
    )
    .await;
    api.crash_pod("create-fence-partial-1");
    let mut pending = pending;
    pending
        .operation
        .as_mut()
        .unwrap()
        .pending_action
        .as_mut()
        .unwrap()
        .deadline_unix_seconds = 0;
    api.reset_operations();
    reconcile_set(
        &make_set("create-fence-partial", 2, Some(pending)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert_status_reads_only(&api.operations());
    let poisoned = api.last_status().unwrap();
    assert_eq!(
        poisoned
            .operation
            .as_ref()
            .unwrap()
            .committed_snapshot
            .as_ref(),
        Some(&committed)
    );
    assert_eq!(
        poisoned.operation.as_ref().unwrap().phase,
        DurableOperationPhase::Poisoned
    );
    api.reset_operations();
    reconcile_set(
        &make_set("create-fence-partial", 2, Some(poisoned)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert!(
        api.operations().is_empty(),
        "poisoned partial fence failure must not mutate the committed primary"
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_create_fence_uid_replacement_restarts_with_new_identity() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    reconcile_set(&make_set("create-fence-replace", 2, None), &api, &state)
        .await
        .unwrap();
    api.mark_all_pods_ready();
    reconcile_set(
        &make_set(
            "create-fence-replace",
            2,
            Some(KubericSetStatus {
                phase: Phase::Creating,
                ..Default::default()
            }),
        ),
        &api,
        &state,
    )
    .await
    .unwrap();
    let pending = advance_create_until_fence_target(
        &api,
        "create-fence-replace",
        2,
        api.last_status().unwrap(),
        2,
    )
    .await;
    let original_operation_id = pending.operation.as_ref().unwrap().operation_id.clone();
    let original_uid = pending.operation.as_ref().unwrap().target_snapshot.members[1]
        .instance_id
        .clone();
    api.crash_pod("create-fence-replace-1");
    api.restart_pod("create-fence-replace-1").await;
    let replacement_uid = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .find(|pod| pod.metadata.name.as_deref() == Some("create-fence-replace-1"))
        .unwrap()
        .metadata
        .uid
        .clone()
        .unwrap();
    assert_ne!(replacement_uid, original_uid);
    api.reset_operations();
    reconcile_set(
        &make_set("create-fence-replace", 2, Some(pending)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert_status_reads_only(&api.operations());
    let failed = api.last_status().unwrap();
    assert_eq!(
        failed.operation.as_ref().unwrap().phase,
        DurableOperationPhase::Failed
    );

    reconcile_set(
        &make_set("create-fence-replace", 2, Some(failed)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    let restarted = api.last_status().unwrap();
    let operation = restarted.operation.as_ref().unwrap();
    assert_eq!(operation.phase, DurableOperationPhase::CreateFenceRouting);
    assert_ne!(operation.operation_id, original_operation_id);
    assert_eq!(
        operation.target_snapshot.members[1].instance_id,
        replacement_uid
    );
    assert_ne!(operation.phase, DurableOperationPhase::Poisoned);

    let completed = drive_create_partition(
        &api,
        &ReconcilerState::default(),
        "create-fence-replace",
        2,
        restarted,
    )
    .await;
    assert_eq!(completed.phase, Phase::Healthy);
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_create_one_two_three_replicas_and_routing_gate() {
    for (name, replicas, min_replicas) in [
        ("create-one", 1, 1),
        ("create-two", 2, 2),
        ("create-three", 3, 2),
    ] {
        let api = KvClusterApi::new();
        let state = ReconcilerState::default();
        reconcile_set(
            &make_set_with_min(name, replicas, min_replicas, None),
            &api,
            &state,
        )
        .await
        .unwrap();
        api.mark_all_pods_ready();
        reconcile_set(
            &make_set_with_min(
                name,
                replicas,
                min_replicas,
                Some(KubericSetStatus {
                    phase: Phase::Creating,
                    ..Default::default()
                }),
            ),
            &api,
            &state,
        )
        .await
        .unwrap();
        let completed = drive_create_partition_with_min(
            &api,
            &state,
            name,
            replicas,
            min_replicas,
            api.last_status().unwrap(),
        )
        .await;
        assert_stable_snapshot(&api, &completed, replicas as usize);

        let mut committed_sizes = api
            .statuses
            .lock()
            .unwrap()
            .iter()
            .filter_map(|status| {
                status
                    .operation
                    .as_ref()
                    .filter(|operation| operation.kind == DurableOperationKind::CreatePartition)
                    .and_then(|operation| operation.committed_snapshot.as_ref())
                    .map(|snapshot| snapshot.members.len())
            })
            .collect::<Vec<_>>();
        committed_sizes.sort_unstable();
        committed_sizes.dedup();
        assert_eq!(committed_sizes, (1..=replicas as usize).collect::<Vec<_>>());

        {
            let pods = api.pods.lock().unwrap();
            for pod in pods.iter() {
                let id = pod
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|labels| labels.get("kuberic.io/pod-index"))
                    .unwrap()
                    .parse::<i64>()
                    .unwrap()
                    + 1;
                let role = pod
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|labels| labels.get("kuberic.io/role"))
                    .unwrap();
                assert_eq!(
                    role,
                    if id == completed.stable_snapshot.as_ref().unwrap().primary_id {
                        "primary"
                    } else {
                        "secondary"
                    }
                );
            }
        }

        let live = live_replica_statuses(&api, name, replicas).await;
        let snapshot = completed.stable_snapshot.as_ref().unwrap();
        for member in &snapshot.members {
            let observed = live
                .iter()
                .find(|status| status.instance_id.as_str() == member.instance_id)
                .unwrap();
            assert_eq!(
                observed.role,
                if member.id == snapshot.primary_id {
                    Role::Primary
                } else {
                    Role::ActiveSecondary
                }
            );
            assert_eq!(
                observed.epoch,
                Epoch::new(
                    snapshot.epoch.data_loss_number,
                    snapshot.epoch.configuration_number
                )
            );
        }
        let primary = live
            .iter()
            .find(|status| status.role == Role::Primary)
            .unwrap();
        let configuration = primary.configuration.as_ref().unwrap();
        assert_eq!(configuration.mode, ReplicaConfigurationMode::Current);
        assert_eq!(configuration.write_quorum, snapshot.write_quorum);
        assert_eq!(
            configuration
                .members
                .iter()
                .map(|member| member.id)
                .collect::<Vec<_>>(),
            snapshot
                .members
                .iter()
                .filter(|member| member.id != snapshot.primary_id)
                .map(|member| member.id)
                .collect::<Vec<_>>()
        );
    }
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_create_candidate_replacement_rolls_forward_from_partial_commit() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    reconcile_set(&make_set("create-replace", 3, None), &api, &state)
        .await
        .unwrap();
    api.mark_all_pods_ready();
    reconcile_set(
        &make_set(
            "create-replace",
            3,
            Some(KubericSetStatus {
                phase: Phase::Creating,
                ..Default::default()
            }),
        ),
        &api,
        &state,
    )
    .await
    .unwrap();
    let mut status = api.last_status().unwrap();
    for _ in 0..120 {
        if status
            .operation
            .as_ref()
            .and_then(|operation| operation.pending_action.as_ref())
            .is_some_and(|pending| pending.kind == DurableActionKind::CreateBuildSecondary)
        {
            break;
        }
        reconcile_set(
            &make_set("create-replace", 3, Some(status.clone())),
            &api,
            &ReconcilerState::default(),
        )
        .await
        .unwrap();
        status = api.last_status().unwrap();
    }
    let operation = status.operation.as_ref().unwrap();
    assert_eq!(
        operation.committed_snapshot.as_ref().unwrap().members.len(),
        1
    );
    let candidate_name = operation.target_pod_name.clone().unwrap();
    let old_uid = operation.target_instance_id.clone().unwrap();

    api.fail_after_next_durable_action(ControlOperation::BuildReplica);
    reconcile_set(
        &make_set("create-replace", 3, Some(status)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    api.crash_pod(&candidate_name);
    api.restart_pod(&candidate_name).await;
    api.mark_all_pods_ready();
    let replacement_uid = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .find(|pod| pod.metadata.name.as_deref() == Some(candidate_name.as_str()))
        .unwrap()
        .metadata
        .uid
        .clone()
        .unwrap();
    assert_ne!(old_uid, replacement_uid);

    let mut status = api.last_status().unwrap();
    for _ in 0..260 {
        reconcile_set(
            &make_set("create-replace", 3, Some(status.clone())),
            &api,
            &ReconcilerState::default(),
        )
        .await
        .unwrap();
        api.mark_all_pods_ready();
        status = api.last_status().unwrap();
        if status.phase == Phase::Healthy {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(status.phase, Phase::Healthy);
    assert_stable_snapshot(&api, &status, 3);
    assert!(
        status
            .stable_snapshot
            .as_ref()
            .unwrap()
            .members
            .iter()
            .any(|member| member.instance_id == replacement_uid)
    );
    assert!(
        status
            .stable_snapshot
            .as_ref()
            .unwrap()
            .members
            .iter()
            .all(|member| member.instance_id != old_uid)
    );
    assert_eq!(
        api.operations()
            .iter()
            .filter(|operation| **operation == ControlOperation::Open)
            .count(),
        4,
        "the committed primary was not reopened; only the replacement added one Open"
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_create_compensates_before_commit_and_preserves_partial_topology() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    reconcile_set(&make_set("create-precommit-failure", 1, None), &api, &state)
        .await
        .unwrap();
    api.mark_all_pods_ready();
    reconcile_set(
        &make_set(
            "create-precommit-failure",
            1,
            Some(KubericSetStatus {
                phase: Phase::Creating,
                ..Default::default()
            }),
        ),
        &api,
        &state,
    )
    .await
    .unwrap();
    let pending = advance_until_pending_action(
        &api,
        "create-precommit-failure",
        1,
        api.last_status().unwrap(),
        DurableActionKind::CreatePromotePrimary,
    )
    .await;
    api.fail_before_next_durable_action(ControlOperation::ChangeRole);
    reconcile_set(
        &make_set("create-precommit-failure", 1, Some(pending)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    let mut timed_out = api.last_status().unwrap();
    timed_out
        .operation
        .as_mut()
        .unwrap()
        .pending_action
        .as_mut()
        .unwrap()
        .deadline_unix_seconds = 0;
    reconcile_set(
        &make_set("create-precommit-failure", 1, Some(timed_out)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    let mut status = api.last_status().unwrap();
    for _ in 0..180 {
        reconcile_set(
            &make_set("create-precommit-failure", 1, Some(status.clone())),
            &api,
            &ReconcilerState::default(),
        )
        .await
        .unwrap();
        api.mark_all_pods_ready();
        status = api.last_status().unwrap();
        if status.phase == Phase::Healthy {
            break;
        }
    }
    assert_eq!(status.phase, Phase::Healthy);
    assert!(api.statuses.lock().unwrap().iter().any(|status| {
        status.operation.as_ref().is_some_and(|operation| {
            operation.kind == DurableOperationKind::CreatePartition
                && operation.phase == DurableOperationPhase::Failed
                && operation.committed_snapshot.is_none()
        })
    }));

    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    reconcile_set(
        &make_set("create-postcommit-failure", 3, None),
        &api,
        &state,
    )
    .await
    .unwrap();
    api.mark_all_pods_ready();
    reconcile_set(
        &make_set(
            "create-postcommit-failure",
            3,
            Some(KubericSetStatus {
                phase: Phase::Creating,
                ..Default::default()
            }),
        ),
        &api,
        &state,
    )
    .await
    .unwrap();
    let pending = advance_until_pending_action(
        &api,
        "create-postcommit-failure",
        3,
        api.last_status().unwrap(),
        DurableActionKind::CreateWaitForCatchUpQuorum,
    )
    .await;
    api.fail_before_next_durable_action(ControlOperation::WaitForCatchUpQuorum);
    reconcile_set(
        &make_set("create-postcommit-failure", 3, Some(pending)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    let mut timed_out = api.last_status().unwrap();
    timed_out
        .operation
        .as_mut()
        .unwrap()
        .pending_action
        .as_mut()
        .unwrap()
        .deadline_unix_seconds = 0;
    reconcile_set(
        &make_set("create-postcommit-failure", 3, Some(timed_out)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    let mut status = api.last_status().unwrap();
    for _ in 0..260 {
        reconcile_set(
            &make_set("create-postcommit-failure", 3, Some(status.clone())),
            &api,
            &ReconcilerState::default(),
        )
        .await
        .unwrap();
        api.mark_all_pods_ready();
        status = api.last_status().unwrap();
        if status.phase == Phase::Healthy {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(status.phase, Phase::Healthy);
    assert!(api.statuses.lock().unwrap().iter().any(|status| {
        status.operation.as_ref().is_some_and(|operation| {
            operation.kind == DurableOperationKind::CreatePartition
                && operation.phase == DurableOperationPhase::Failed
                && operation
                    .committed_snapshot
                    .as_ref()
                    .is_some_and(|snapshot| snapshot.members.len() == 1)
        })
    }));
    assert_eq!(
        api.operations()
            .iter()
            .filter(|operation| **operation == ControlOperation::Open)
            .count(),
        4,
        "post-commit retry reopened only the failed secondary candidate"
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_create_invalid_checkpoint_and_committed_replacement_fail_closed() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    reconcile_set(&make_set("create-invalid", 2, None), &api, &state)
        .await
        .unwrap();
    api.mark_all_pods_ready();
    reconcile_set(
        &make_set(
            "create-invalid",
            2,
            Some(KubericSetStatus {
                phase: Phase::Creating,
                ..Default::default()
            }),
        ),
        &api,
        &state,
    )
    .await
    .unwrap();
    let mut invalid = api.last_status().unwrap();
    invalid.operation.as_mut().unwrap().version = u32::MAX;
    api.reset_operations();
    reconcile_set(
        &make_set("create-invalid", 2, Some(invalid)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert_status_reads_only(&api.operations());

    let mut status = api
        .statuses
        .lock()
        .unwrap()
        .iter()
        .find(|status| {
            status.operation.as_ref().is_some_and(|operation| {
                operation.kind == DurableOperationKind::CreatePartition
                    && operation.version != u32::MAX
            })
        })
        .cloned()
        .unwrap();
    for _ in 0..80 {
        if status
            .operation
            .as_ref()
            .and_then(|operation| operation.committed_snapshot.as_ref())
            .is_some_and(|snapshot| snapshot.members.len() == 1)
        {
            break;
        }
        reconcile_set(
            &make_set("create-invalid", 2, Some(status.clone())),
            &api,
            &ReconcilerState::default(),
        )
        .await
        .unwrap();
        status = api.last_status().unwrap();
    }
    let primary_name = status
        .operation
        .as_ref()
        .unwrap()
        .target_snapshot
        .members
        .first()
        .map(|member| format!("create-invalid-{}", member.id - 1))
        .unwrap();
    api.crash_pod(&primary_name);
    api.restart_pod(&primary_name).await;
    api.mark_all_pods_ready();
    api.reset_operations();
    reconcile_set(
        &make_set("create-invalid", 2, Some(status)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert!(api.operations().is_empty());
    assert_eq!(
        api.last_status().unwrap().operation.as_ref().unwrap().phase,
        DurableOperationPhase::Poisoned
    );
    let poisoned = api.last_status().unwrap();
    api.reset_operations();
    reconcile_set(
        &make_set("create-invalid", 2, Some(poisoned)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert!(
        api.operations().is_empty(),
        "poisoned creation must remain fail closed on later reconciles"
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_create_rejects_missing_and_duplicate_ids_and_uids() {
    async fn setup(name: &str) -> (KvClusterApi, ReconcilerState) {
        let api = KvClusterApi::new();
        let state = ReconcilerState::default();
        reconcile_set(&make_set(name, 2, None), &api, &state)
            .await
            .unwrap();
        api.mark_all_pods_ready();
        api.reset_operations();
        (api, state)
    }

    let (api, state) = setup("create-missing-id").await;
    api.pods.lock().unwrap()[0]
        .metadata
        .labels
        .as_mut()
        .unwrap()
        .remove("kuberic.io/pod-index");
    assert!(
        reconcile_error(
            reconcile_set(
                &make_set(
                    "create-missing-id",
                    2,
                    Some(KubericSetStatus {
                        phase: Phase::Creating,
                        ..Default::default()
                    })
                ),
                &api,
                &state
            )
            .await
        )
        .contains("has no kuberic.io/pod-index")
    );
    assert!(api.operations().is_empty());

    let (api, state) = setup("create-duplicate-id").await;
    for pod in api.pods.lock().unwrap().iter_mut() {
        pod.metadata
            .labels
            .as_mut()
            .unwrap()
            .insert("kuberic.io/pod-index".to_string(), "0".to_string());
    }
    assert!(
        reconcile_error(
            reconcile_set(
                &make_set(
                    "create-duplicate-id",
                    2,
                    Some(KubericSetStatus {
                        phase: Phase::Creating,
                        ..Default::default()
                    })
                ),
                &api,
                &state
            )
            .await
        )
        .contains("duplicate pod logical replica ID")
    );
    assert!(api.operations().is_empty());

    let (api, state) = setup("create-missing-uid").await;
    api.pods.lock().unwrap()[0].metadata.uid = None;
    assert!(
        reconcile_error(
            reconcile_set(
                &make_set(
                    "create-missing-uid",
                    2,
                    Some(KubericSetStatus {
                        phase: Phase::Creating,
                        ..Default::default()
                    })
                ),
                &api,
                &state
            )
            .await
        )
        .contains("has no UID")
    );
    assert!(api.operations().is_empty());

    let (api, state) = setup("create-duplicate-uid").await;
    let uid = api.pods.lock().unwrap()[0].metadata.uid.clone();
    api.pods.lock().unwrap()[1].metadata.uid = uid;
    assert!(
        reconcile_error(
            reconcile_set(
                &make_set(
                    "create-duplicate-uid",
                    2,
                    Some(KubericSetStatus {
                        phase: Phase::Creating,
                        ..Default::default()
                    })
                ),
                &api,
                &state
            )
            .await
        )
        .contains("duplicate pod incarnation")
    );
    assert!(api.operations().is_empty());
}

/// Reconciler test: create partition → write data → switchover → write on new primary.
#[test_log::test(tokio::test)]
#[serial]
async fn test_reconciler_switchover() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();

    // Pending → Creating
    let set = make_set("myapp", 3, None);
    reconcile_set(&set, &api, &state).await.unwrap();
    api.mark_all_pods_ready();

    // Creating → Healthy
    let set = make_set(
        "myapp",
        3,
        Some(KubericSetStatus {
            phase: Phase::Creating,
            ..Default::default()
        }),
    );
    reconcile_set(&set, &api, &state).await.unwrap();

    let status = drive_create_partition(&api, &state, "myapp", 3, api.last_status().unwrap()).await;
    assert_eq!(status.phase, Phase::Healthy);
    let original_primary = status.current_primary.clone().unwrap();

    // Write data on original primary
    let client_addr = api.client_address(&original_primary).unwrap();
    let mut kv = connect_kv(&client_addr).await;
    kv.put(proto::PutRequest {
        key: "before-switch".to_string(),
        value: "original".to_string(),
    })
    .await
    .unwrap();

    // Pick a secondary as switchover target
    let target_name = {
        let pods = api.pods.lock().unwrap();
        pods.iter()
            .map(|p| p.metadata.name.clone().unwrap())
            .find(|n| n != &original_primary)
            .unwrap()
    };

    // Healthy → Switchover: set target_primary to a different pod
    let set = make_set(
        "myapp",
        3,
        Some(KubericSetStatus {
            phase: Phase::Healthy,
            current_primary: Some(original_primary.clone()),
            target_primary: Some(target_name.clone()),
            ..status.clone()
        }),
    );
    reconcile_set(&set, &api, &state).await.unwrap();
    let mut status = api.last_status().unwrap();
    for _ in 0..10 {
        if status.phase == Phase::Switchover {
            break;
        }
        reconcile_set(
            &make_set(
                "myapp",
                3,
                Some(KubericSetStatus {
                    current_primary: Some(original_primary.clone()),
                    target_primary: Some(target_name.clone()),
                    ..status.clone()
                }),
            ),
            &api,
            &state,
        )
        .await
        .unwrap();
        status = api.last_status().unwrap();
    }
    assert_eq!(
        status.phase,
        Phase::Switchover,
        "switchover request did not start: {status:?}"
    );

    // Switchover → Healthy: execute the switchover
    let status = drive_switchover(&api, &state, "myapp", 3, status).await;
    assert_eq!(status.phase, Phase::Healthy);
    assert_eq!(
        status.current_primary.as_deref(),
        Some(target_name.as_str())
    );
    assert_stable_snapshot(&api, &status, 3);
    assert_ne!(
        status.current_primary.as_deref(),
        Some(original_primary.as_str())
    );

    // Write on new primary
    let new_client_addr = api.client_address(&target_name).unwrap();
    let mut kv2 = connect_kv(&new_client_addr).await;

    let resp = kv2
        .put(proto::PutRequest {
            key: "after-switch".to_string(),
            value: "new-primary".to_string(),
        })
        .await
        .unwrap();
    assert!(resp.get_ref().lsn > 0);

    // Read back on new primary
    let resp = kv2
        .get(proto::GetRequest {
            key: "after-switch".to_string(),
        })
        .await
        .unwrap();
    assert!(resp.get_ref().found);
    assert_eq!(resp.get_ref().value, "new-primary");

    // Write to old primary should fail (no longer primary)
    let result = kv
        .put(proto::PutRequest {
            key: "stale-write".to_string(),
            value: "should-fail".to_string(),
        })
        .await;
    assert!(result.is_err(), "write to demoted primary should fail");
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_switchover_survives_state_loss_at_every_boundary() {
    let api = KvClusterApi::new();
    let initial_state = ReconcilerState::default();
    let status = create_healthy_set(&api, &initial_state, "durable-boundaries", 3).await;
    let original_primary = status.current_primary.clone().unwrap();
    let target = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .map(|pod| pod.metadata.name.clone().unwrap())
        .find(|name| name != &original_primary)
        .unwrap();

    reconcile_set(
        &make_set(
            "durable-boundaries",
            3,
            Some(KubericSetStatus {
                target_primary: Some(target.clone()),
                ..status
            }),
        ),
        &api,
        &initial_state,
    )
    .await
    .unwrap();
    let mut status = api.last_status().unwrap();
    let operation_id = status.operation.as_ref().unwrap().operation_id.clone();
    api.reset_operations();

    for _ in 0..80 {
        reconcile_set(
            &make_set("durable-boundaries", 3, Some(status.clone())),
            &api,
            &ReconcilerState::default(),
        )
        .await
        .unwrap();
        status = api.last_status().unwrap();
        if status.phase == Phase::Healthy {
            break;
        }
    }

    assert_eq!(status.phase, Phase::Healthy);
    assert_eq!(status.current_primary.as_deref(), Some(target.as_str()));
    assert_eq!(
        status.operation.as_ref().unwrap().phase,
        DurableOperationPhase::Completed
    );
    assert_eq!(
        status.operation.as_ref().unwrap().operation_id,
        operation_id
    );
    assert_stable_snapshot(&api, &status, 3);
    let live = live_replica_statuses(&api, "durable-boundaries", 3).await;
    assert_eq!(
        live.iter()
            .filter(|status| status.role == Role::Primary)
            .count(),
        1
    );
    assert_eq!(
        live.iter()
            .filter(|status| status.write_status == kuberic_core::types::AccessStatus::Granted)
            .count(),
        1
    );

    let operations = api.operations();
    assert_eq!(
        operations
            .iter()
            .filter(|operation| **operation == ControlOperation::RevokeWriteStatus)
            .count(),
        1
    );
    assert_eq!(
        operations
            .iter()
            .filter(|operation| **operation == ControlOperation::ChangeRole)
            .count(),
        2
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_switchover_observes_lost_promotion_reply_without_duplicate() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let status = create_healthy_set(&api, &state, "durable-ambiguous", 3).await;
    let original_primary = status.current_primary.clone().unwrap();
    let target = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .map(|pod| pod.metadata.name.clone().unwrap())
        .find(|name| name != &original_primary)
        .unwrap();
    reconcile_set(
        &make_set(
            "durable-ambiguous",
            3,
            Some(KubericSetStatus {
                target_primary: Some(target.clone()),
                ..status
            }),
        ),
        &api,
        &state,
    )
    .await
    .unwrap();
    let switching = advance_until_pending_action(
        &api,
        "durable-ambiguous",
        3,
        api.last_status().unwrap(),
        DurableActionKind::PromoteTarget,
    )
    .await;

    api.reset_operations();
    api.fail_after_next_durable_action(ControlOperation::ChangeRole);
    reconcile_set(
        &make_set("durable-ambiguous", 3, Some(switching)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    let after_lost_reply = api.last_status().unwrap();
    assert_eq!(
        after_lost_reply
            .operation
            .as_ref()
            .unwrap()
            .pending_action
            .as_ref()
            .unwrap()
            .attempts,
        1
    );

    let completed = drive_switchover(
        &api,
        &ReconcilerState::default(),
        "durable-ambiguous",
        3,
        after_lost_reply,
    )
    .await;
    assert_eq!(completed.current_primary.as_deref(), Some(target.as_str()));
    assert_eq!(
        api.operations()
            .iter()
            .filter(|operation| **operation == ControlOperation::ChangeRole)
            .count(),
        1,
        "the ambiguous target promotion must not be dispatched twice"
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_switchover_observes_every_runtime_lost_reply_window() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let status = create_healthy_set(&api, &state, "durable-all-ambiguous", 3).await;
    let original_primary = status.current_primary.clone().unwrap();
    let target = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .map(|pod| pod.metadata.name.clone().unwrap())
        .find(|name| name != &original_primary)
        .unwrap();
    reconcile_set(
        &make_set(
            "durable-all-ambiguous",
            3,
            Some(KubericSetStatus {
                target_primary: Some(target.clone()),
                ..status
            }),
        ),
        &api,
        &state,
    )
    .await
    .unwrap();
    api.reset_operations();
    let mut status = api.last_status().unwrap();
    let mut injected = Vec::new();

    for _ in 0..100 {
        if let Some(pending) = status
            .operation
            .as_ref()
            .and_then(|operation| operation.pending_action.as_ref())
            && pending.attempts == 0
            && !injected.contains(&pending.kind)
        {
            let control = match pending.kind {
                DurableActionKind::RevokeWrite => Some(ControlOperation::RevokeWriteStatus),
                DurableActionKind::DemoteOldPrimary | DurableActionKind::PromoteTarget => {
                    Some(ControlOperation::ChangeRole)
                }
                DurableActionKind::UpdateSecondaryEpoch => Some(ControlOperation::UpdateEpoch),
                DurableActionKind::UpdateCatchUpConfiguration => {
                    Some(ControlOperation::UpdateCatchUpConfiguration)
                }
                DurableActionKind::WaitForCatchUpQuorum => {
                    Some(ControlOperation::WaitForCatchUpQuorum)
                }
                DurableActionKind::UpdateCurrentConfiguration => {
                    Some(ControlOperation::UpdateCurrentConfiguration)
                }
                _ => None,
            };
            if let Some(control) = control {
                api.fail_after_next_durable_action(control);
                injected.push(pending.kind);
            }
        }

        reconcile_set(
            &make_set("durable-all-ambiguous", 3, Some(status.clone())),
            &api,
            &ReconcilerState::default(),
        )
        .await
        .unwrap();
        status = api.last_status().unwrap();
        if status.phase == Phase::Healthy {
            break;
        }
    }

    assert_eq!(status.phase, Phase::Healthy);
    assert_eq!(status.current_primary.as_deref(), Some(target.as_str()));
    for kind in [
        DurableActionKind::RevokeWrite,
        DurableActionKind::DemoteOldPrimary,
        DurableActionKind::PromoteTarget,
        DurableActionKind::UpdateSecondaryEpoch,
        DurableActionKind::UpdateCatchUpConfiguration,
        DurableActionKind::WaitForCatchUpQuorum,
        DurableActionKind::UpdateCurrentConfiguration,
    ] {
        assert!(
            injected.contains(&kind),
            "missing lost-reply window {kind:?}"
        );
    }
    let operations = api.operations();
    assert_eq!(
        operations
            .iter()
            .filter(|operation| **operation == ControlOperation::RevokeWriteStatus)
            .count(),
        1
    );
    assert_eq!(
        operations
            .iter()
            .filter(|operation| **operation == ControlOperation::ChangeRole)
            .count(),
        2
    );
    assert_eq!(
        operations
            .iter()
            .filter(|operation| **operation == ControlOperation::UpdateEpoch)
            .count(),
        1
    );
    assert_eq!(
        operations
            .iter()
            .filter(|operation| **operation == ControlOperation::UpdateCatchUpConfiguration)
            .count(),
        1
    );
    assert_eq!(
        operations
            .iter()
            .filter(|operation| **operation == ControlOperation::WaitForCatchUpQuorum)
            .count(),
        1
    );
    assert_eq!(
        operations
            .iter()
            .filter(|operation| **operation == ControlOperation::UpdateCurrentConfiguration)
            .count(),
        1
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_switchover_compensates_failed_target_promotion() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let status = create_healthy_set(&api, &state, "durable-compensate", 3).await;
    let original_primary = status.current_primary.clone().unwrap();
    let target = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .map(|pod| pod.metadata.name.clone().unwrap())
        .find(|name| name != &original_primary)
        .unwrap();
    reconcile_set(
        &make_set(
            "durable-compensate",
            3,
            Some(KubericSetStatus {
                target_primary: Some(target),
                ..status
            }),
        ),
        &api,
        &state,
    )
    .await
    .unwrap();
    let switching = advance_until_pending_action(
        &api,
        "durable-compensate",
        3,
        api.last_status().unwrap(),
        DurableActionKind::PromoteTarget,
    )
    .await;

    api.fail_before_next_durable_action(ControlOperation::ChangeRole);
    reconcile_set(
        &make_set("durable-compensate", 3, Some(switching)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    let mut failed = api.last_status().unwrap();
    failed
        .operation
        .as_mut()
        .unwrap()
        .pending_action
        .as_mut()
        .unwrap()
        .deadline_unix_seconds = 0;
    reconcile_set(
        &make_set("durable-compensate", 3, Some(failed)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();

    let completed = drive_switchover(
        &api,
        &ReconcilerState::default(),
        "durable-compensate",
        3,
        api.last_status().unwrap(),
    )
    .await;
    assert_eq!(
        completed.current_primary.as_deref(),
        Some(original_primary.as_str())
    );
    assert_eq!(
        completed.operation.as_ref().unwrap().phase,
        DurableOperationPhase::Failed
    );
    assert_stable_snapshot(&api, &completed, 3);

    api.reset_operations();
    reconcile_set(
        &make_set("durable-compensate", 3, Some(completed)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert_status_reads_only(&api.operations());
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_switchover_restores_writes_when_demotion_never_runs() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let status = create_healthy_set(&api, &state, "durable-safe-abort", 3).await;
    let original_primary = status.current_primary.clone().unwrap();
    let original_epoch = status.epoch.clone();
    let target = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .map(|pod| pod.metadata.name.clone().unwrap())
        .find(|name| name != &original_primary)
        .unwrap();
    reconcile_set(
        &make_set(
            "durable-safe-abort",
            3,
            Some(KubericSetStatus {
                target_primary: Some(target),
                ..status
            }),
        ),
        &api,
        &state,
    )
    .await
    .unwrap();
    let switching = advance_until_pending_action(
        &api,
        "durable-safe-abort",
        3,
        api.last_status().unwrap(),
        DurableActionKind::DemoteOldPrimary,
    )
    .await;

    api.fail_before_next_durable_action(ControlOperation::ChangeRole);
    reconcile_set(
        &make_set("durable-safe-abort", 3, Some(switching)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    let mut failed = api.last_status().unwrap();
    failed
        .operation
        .as_mut()
        .unwrap()
        .pending_action
        .as_mut()
        .unwrap()
        .deadline_unix_seconds = 0;
    reconcile_set(
        &make_set("durable-safe-abort", 3, Some(failed)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    let completed = drive_switchover(
        &api,
        &ReconcilerState::default(),
        "durable-safe-abort",
        3,
        api.last_status().unwrap(),
    )
    .await;
    assert_eq!(
        completed.current_primary.as_deref(),
        Some(original_primary.as_str())
    );
    assert_eq!(completed.epoch, original_epoch);
    assert_eq!(
        completed.operation.as_ref().unwrap().phase,
        DurableOperationPhase::Failed
    );
    let live = live_replica_statuses(&api, "durable-safe-abort", 3).await;
    assert_eq!(
        live.iter()
            .filter(|status| status.write_status == kuberic_core::types::AccessStatus::Granted)
            .count(),
        1
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_switchover_conflict_and_invalid_checkpoint_do_not_mutate() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let status = create_healthy_set(&api, &state, "durable-conflict", 3).await;
    let original_primary = status.current_primary.clone().unwrap();
    let target = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .map(|pod| pod.metadata.name.clone().unwrap())
        .find(|name| name != &original_primary)
        .unwrap();

    api.reset_operations();
    api.fail_next_status_conflict();
    let result = reconcile_set(
        &make_set(
            "durable-conflict",
            3,
            Some(KubericSetStatus {
                target_primary: Some(target.clone()),
                ..status.clone()
            }),
        ),
        &api,
        &state,
    )
    .await;
    assert_eq!(result.err().as_deref(), Some("resource version conflict"));
    assert!(
        api.operations()
            .iter()
            .all(|operation| *operation == ControlOperation::GetStatus)
    );
    assert!(
        state
            .drivers
            .lock()
            .await
            .contains_key("default/durable-conflict")
    );

    reconcile_set(
        &make_set(
            "durable-conflict",
            3,
            Some(KubericSetStatus {
                target_primary: Some(target),
                ..status
            }),
        ),
        &api,
        &state,
    )
    .await
    .unwrap();
    let mut invalid = api.last_status().unwrap();
    invalid.operation.as_mut().unwrap().version += 1;
    api.reset_operations();
    reconcile_set(
        &make_set("durable-conflict", 3, Some(invalid)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert!(
        api.operations()
            .iter()
            .all(|operation| *operation == ControlOperation::GetStatus)
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_switchover_rejects_incarnation_drift_before_mutation() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let status = create_healthy_set(&api, &state, "durable-stale", 3).await;
    let original_primary = status.current_primary.clone().unwrap();
    let target = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .map(|pod| pod.metadata.name.clone().unwrap())
        .find(|name| name != &original_primary)
        .unwrap();
    reconcile_set(
        &make_set(
            "durable-stale",
            3,
            Some(KubericSetStatus {
                target_primary: Some(target.clone()),
                ..status
            }),
        ),
        &api,
        &state,
    )
    .await
    .unwrap();
    let switching = api.last_status().unwrap();
    api.pods
        .lock()
        .unwrap()
        .iter_mut()
        .find(|pod| pod.metadata.name.as_deref() == Some(target.as_str()))
        .unwrap()
        .metadata
        .uid = Some("replacement-incarnation".to_string());

    api.reset_operations();
    reconcile_set(
        &make_set("durable-stale", 3, Some(switching)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert!(api.operations().is_empty());
    let poisoned = api.last_status().unwrap();
    assert_eq!(
        poisoned.operation.as_ref().unwrap().phase,
        DurableOperationPhase::Poisoned
    );
    assert!(
        poisoned
            .operation
            .as_ref()
            .unwrap()
            .last_error
            .as_deref()
            .unwrap()
            .contains("incarnation changed")
    );
}

/// Reconciler test: Creating phase requeues when pods are not yet ready.
#[test_log::test(tokio::test)]
#[serial]
async fn test_reconciler_creating_waits_for_ready() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();

    // Pending → Creating (creates pods but they're not ready yet)
    let set = make_set("myapp", 3, None);
    reconcile_set(&set, &api, &state).await.unwrap();
    assert_eq!(api.pods.lock().unwrap().len(), 3);

    // Don't mark pods ready — reconcile Creating phase
    let set = make_set(
        "myapp",
        3,
        Some(KubericSetStatus {
            phase: Phase::Creating,
            ..Default::default()
        }),
    );
    let result = reconcile_set(&set, &api, &state).await.unwrap();

    // Should requeue (waiting for pods to become ready)
    assert!(
        matches!(
            result,
            kuberic_operator::reconciler::ReconcileAction::Requeue(_)
        ),
        "should requeue when pods not ready"
    );

    // Status should still be Creating (no transition to Healthy)
    let status = api.last_status().unwrap();
    assert_eq!(status.phase, Phase::Creating);
}

/// Reconciler test: Healthy phase detects primary pod NotReady → FailingOver,
/// then FailingOver phase completes failover → back to Healthy with new primary.
#[test_log::test(tokio::test)]
#[serial]
async fn test_reconciler_detects_primary_failure_and_fails_over() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();

    let status = create_healthy_set(&api, &state, "myapp", 3).await;
    assert!(
        status
            .members
            .iter()
            .all(|member| !member.instance_id.is_empty())
    );
    {
        let drivers = state.drivers.lock().await;
        let driver = drivers.get("default/myapp").unwrap();
        for member in &status.members {
            let handle = driver.handle(member.id).unwrap();
            assert_eq!(member.instance_id, handle.instance_id().as_str());
            assert_eq!(
                handle.get_status().await.unwrap().instance_id,
                handle.instance_id()
            );
        }
    }
    let primary_name = status.current_primary.clone().unwrap();

    // Write data on primary before failure
    let client_addr = api.client_address(&primary_name).unwrap();
    let mut kv = connect_kv(&client_addr).await;
    kv.put(proto::PutRequest {
        key: "before-crash".to_string(),
        value: "important".to_string(),
    })
    .await
    .unwrap();

    // Crash primary (abrupt — abort tasks, gRPC breaks)
    api.crash_pod(&primary_name);

    // Reconcile Healthy — should detect failure → FailingOver
    let set = make_set("myapp", 3, Some(status));
    reconcile_set(&set, &api, &state).await.unwrap();

    let status = api.last_status().unwrap();
    assert_eq!(status.phase, Phase::FailingOver);

    let status = drive_operation_to_healthy(&api, "myapp", 3, status).await;
    assert_stable_snapshot(&api, &status, 2);
    assert!(
        status
            .members
            .iter()
            .find(|member| member.name == status.current_primary.as_deref().unwrap())
            .unwrap()
            .current_progress
            > 0
    );
    assert!(
        status
            .stable_snapshot
            .as_ref()
            .unwrap()
            .members
            .iter()
            .all(|member| member.election_metadata.is_some())
    );

    // New primary should be different from the crashed one
    let new_primary = status.current_primary.clone().unwrap();
    assert_ne!(new_primary, primary_name);

    // New primary should serve data that was written before crash
    let new_client_addr = api.client_address(&new_primary).unwrap();
    let mut kv2 = connect_kv(&new_client_addr).await;
    let resp = kv2
        .get(proto::GetRequest {
            key: "before-crash".to_string(),
        })
        .await
        .unwrap();
    assert!(resp.get_ref().found);
    assert_eq!(resp.get_ref().value, "important");

    // New primary accepts new writes
    let resp = kv2
        .put(proto::PutRequest {
            key: "after-failover".to_string(),
            value: "recovered".to_string(),
        })
        .await
        .unwrap();
    assert!(resp.get_ref().lsn > 0);

    // Driver now has 2 replicas (failed primary was removed, not demoted).
    // The old primary pod is still running but orphaned — nobody closed it
    // or replaced it. The partition is "Healthy" but degraded.
    // TODO: Healthy phase should detect spec.replicas (3) > driver count (2)
    //       and create a replacement. See operator-failure-scenarios.md §2, §9.
    assert_eq!(
        status.stable_snapshot.as_ref().unwrap().members.len(),
        2,
        "failed primary removed from the durable snapshot"
    );
    assert_eq!(
        api.pods.lock().unwrap().len(),
        3,
        "old pod still exists (orphaned)"
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_add_survives_state_loss_and_every_lost_runtime_reply() {
    let api = KvClusterApi::new();
    let initial_state = ReconcilerState::default();
    let previous = create_healthy_set(&api, &initial_state, "durable-add", 1).await;
    let previous_snapshot = previous.stable_snapshot.clone().unwrap();

    reconcile_set(
        &make_set("durable-add", 2, Some(previous.clone())),
        &api,
        &initial_state,
    )
    .await
    .unwrap();
    api.mark_all_pods_ready();
    reconcile_set(
        &make_set("durable-add", 2, Some(previous.clone())),
        &api,
        &initial_state,
    )
    .await
    .unwrap();
    let mut status = api.last_status().unwrap();
    assert_eq!(status.phase, Phase::AddingReplica);
    assert_eq!(
        status.operation.as_ref().unwrap().kind,
        DurableOperationKind::AddReplica
    );
    assert_eq!(
        status.operation.as_ref().unwrap().add_mode,
        Some(DurableAddMode::ScaleUp)
    );

    status = advance_until_pending_action(
        &api,
        "durable-add",
        2,
        status,
        DurableActionKind::AddReplicaIntent,
    )
    .await;
    api.reset_operations();
    api.fail_after_next_durable_action(ControlOperation::AddReplicaIntent);
    reconcile_set(
        &make_set("durable-add", 2, Some(status)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    status = drive_add_replica(
        &api,
        &ReconcilerState::default(),
        "durable-add",
        2,
        api.last_status().unwrap(),
    )
    .await;

    assert_eq!(status.phase, Phase::Healthy);
    assert_eq!(
        status.operation.as_ref().unwrap().phase,
        DurableOperationPhase::Completed
    );
    assert_stable_snapshot(&api, &status, 2);
    assert_ne!(status.stable_snapshot.as_ref(), Some(&previous_snapshot));
    let operations = api.operations();
    assert_eq!(
        operations
            .iter()
            .filter(|operation| **operation == ControlOperation::AddReplicaIntent)
            .count(),
        1
    );
    assert_eq!(
        operations
            .iter()
            .filter(|operation| **operation == ControlOperation::Open)
            .count(),
        0,
        "operator must not send target runtime actions"
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_rejoin_retires_old_incarnation_once() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let previous = create_healthy_set(&api, &state, "durable-rejoin", 3).await;
    let primary = previous.current_primary.clone().unwrap();
    let target = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .map(|pod| pod.metadata.name.clone().unwrap())
        .find(|name| name != &primary)
        .unwrap();
    let old_instance = previous
        .stable_snapshot
        .as_ref()
        .unwrap()
        .members
        .iter()
        .find(|member| member.id == 2)
        .map(|member| member.instance_id.clone())
        .unwrap_or_else(|| {
            previous
                .stable_snapshot
                .as_ref()
                .unwrap()
                .members
                .iter()
                .find(|member| member.id != previous.stable_snapshot.as_ref().unwrap().primary_id)
                .unwrap()
                .instance_id
                .clone()
        });

    api.crash_pod(&target);
    api.restart_pod(&target).await;
    let replacement_instance = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .find(|pod| pod.metadata.name.as_deref() == Some(target.as_str()))
        .unwrap()
        .metadata
        .uid
        .clone()
        .unwrap();
    assert_ne!(old_instance, replacement_instance);

    reconcile_set(
        &make_set("durable-rejoin", 3, Some(previous.clone())),
        &api,
        &state,
    )
    .await
    .unwrap();
    let mut status = api.last_status().unwrap();
    assert_eq!(status.phase, Phase::AddingReplica);
    assert_eq!(
        status.operation.as_ref().unwrap().add_mode,
        Some(DurableAddMode::Rebuild)
    );
    assert_eq!(
        status
            .operation
            .as_ref()
            .unwrap()
            .retired_instance_id
            .as_deref(),
        Some(old_instance.as_str())
    );

    status = advance_until_pending_action(
        &api,
        "durable-rejoin",
        3,
        status,
        DurableActionKind::AddReplicaIntent,
    )
    .await;
    let target_replica_id = status
        .operation
        .as_ref()
        .unwrap()
        .target_replica_id
        .unwrap();
    api.reset_operations();
    api.fail_after_next_durable_action(ControlOperation::AddReplicaIntent);
    reconcile_set(
        &make_set("durable-rejoin", 3, Some(status)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    let completed =
        drive_operation_to_healthy(&api, "durable-rejoin", 3, api.last_status().unwrap()).await;
    assert_eq!(
        api.operations()
            .iter()
            .filter(|operation| **operation == ControlOperation::AddReplicaIntent)
            .count(),
        1
    );
    assert!(
        completed
            .stable_snapshot
            .as_ref()
            .unwrap()
            .members
            .iter()
            .any(|member| member.instance_id == replacement_instance)
    );

    let primary_id = completed.stable_snapshot.as_ref().unwrap().primary_id;
    let primary_pod = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .find(|pod| {
            pod.metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get("kuberic.io/pod-index"))
                .and_then(|index| index.parse::<i64>().ok())
                .map(|index| index + 1)
                == Some(primary_id)
        })
        .unwrap()
        .clone();
    let primary_handle = api
        .create_replica_handle(
            primary_id,
            &primary_pod,
            &make_set("durable-rejoin", 3, Some(completed.clone())).spec,
        )
        .await
        .unwrap();
    execute_with_fresh_fences(
        primary_handle.as_ref(),
        "delayed-old-removal",
        DurableReplicaAction::RemoveReplica {
            replica_id: target_replica_id,
            instance_id: ReplicaInstanceId::new(old_instance),
        },
    )
    .await
    .unwrap();
    let target_status = live_replica_statuses(&api, "durable-rejoin", 3)
        .await
        .into_iter()
        .find(|status| status.instance_id.as_str() == replacement_instance)
        .unwrap();
    assert_eq!(target_status.role, Role::ActiveSecondary);
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_rejoin_compensation_recreates_and_retries() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let previous = create_healthy_set(&api, &state, "rejoin-compensate", 3).await;
    let previous_snapshot = previous.stable_snapshot.clone().unwrap();
    let primary = previous.current_primary.clone().unwrap();
    let target = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .map(|pod| pod.metadata.name.clone().unwrap())
        .find(|name| name != &primary)
        .unwrap();

    api.crash_pod(&target);
    api.restart_pod(&target).await;
    reconcile_set(
        &make_set("rejoin-compensate", 3, Some(previous)),
        &api,
        &state,
    )
    .await
    .unwrap();
    let pending = advance_until_pending_action(
        &api,
        "rejoin-compensate",
        3,
        api.last_status().unwrap(),
        DurableActionKind::AddReplicaIntent,
    )
    .await;
    api.crash_pod(&target);
    reconcile_set(
        &make_set("rejoin-compensate", 3, Some(pending)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    let compensated =
        drive_operation_to_healthy(&api, "rejoin-compensate", 3, api.last_status().unwrap()).await;
    assert_eq!(
        compensated.stable_snapshot.as_ref(),
        Some(&previous_snapshot)
    );
    assert_eq!(api.pods.lock().unwrap().len(), 2);

    reconcile_set(
        &make_set("rejoin-compensate", 3, Some(compensated.clone())),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert_eq!(api.pods.lock().unwrap().len(), 3);
    api.mark_all_pods_ready();
    let completed = drive_add_replica(
        &api,
        &ReconcilerState::default(),
        "rejoin-compensate",
        3,
        api.last_status().unwrap(),
    )
    .await;
    assert_eq!(
        completed.operation.as_ref().unwrap().phase,
        DurableOperationPhase::Completed
    );
    assert_stable_snapshot(&api, &completed, 3);
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_add_status_conflict_prevents_open() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let previous = create_healthy_set(&api, &state, "durable-add-conflict", 1).await;
    reconcile_set(
        &make_set("durable-add-conflict", 2, Some(previous.clone())),
        &api,
        &state,
    )
    .await
    .unwrap();
    api.mark_all_pods_ready();
    reconcile_set(
        &make_set("durable-add-conflict", 2, Some(previous)),
        &api,
        &state,
    )
    .await
    .unwrap();
    let operation_started = api.last_status().unwrap();
    assert_eq!(operation_started.phase, Phase::AddingReplica);
    api.reset_operations();
    api.fail_next_status_conflict();
    let error = match reconcile_set(
        &make_set("durable-add-conflict", 2, Some(operation_started)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    {
        Ok(_) => panic!("conflicting intent status must fail"),
        Err(error) => error,
    };
    assert_eq!(error, "resource version conflict");
    assert!(
        !api.operations().contains(&ControlOperation::Open),
        "candidate must not be opened before durable intent persists"
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_add_uses_structured_intent_without_payload_projection() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let previous = create_healthy_set(&api, &state, "add-payload-conflict", 1).await;
    reconcile_set(
        &make_set("add-payload-conflict", 2, Some(previous.clone())),
        &api,
        &state,
    )
    .await
    .unwrap();
    api.mark_all_pods_ready();
    reconcile_set(
        &make_set("add-payload-conflict", 2, Some(previous)),
        &api,
        &state,
    )
    .await
    .unwrap();
    let pending = advance_until_pending_action(
        &api,
        "add-payload-conflict",
        2,
        api.last_status().unwrap(),
        DurableActionKind::AddReplicaIntent,
    )
    .await;
    assert!(
        pending
            .operation
            .as_ref()
            .unwrap()
            .pending_action
            .as_ref()
            .unwrap()
            .dispatch_action_payload
            .is_empty(),
        "coarse add dispatch must derive directly from structured addIntent"
    );
    let json = serde_json::to_value(pending.operation.as_ref().unwrap()).unwrap();
    assert!(
        json["pendingAction"].get("dispatchActionPayload").is_none(),
        "coarse add CRD status must not project an encoded action payload"
    );
    api.reset_operations();
    reconcile_set(
        &make_set("add-payload-conflict", 2, Some(pending)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert!(
        api.operations()
            .contains(&ControlOperation::AddReplicaIntent)
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_add_transient_primary_status_loss_preserves_target() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let previous = create_healthy_set(&api, &state, "add-primary-status-loss", 1).await;
    reconcile_set(
        &make_set("add-primary-status-loss", 2, Some(previous.clone())),
        &api,
        &state,
    )
    .await
    .unwrap();
    api.mark_all_pods_ready();
    reconcile_set(
        &make_set("add-primary-status-loss", 2, Some(previous)),
        &api,
        &state,
    )
    .await
    .unwrap();
    let pending = advance_until_pending_action(
        &api,
        "add-primary-status-loss",
        2,
        api.last_status().unwrap(),
        DurableActionKind::AddReplicaIntent,
    )
    .await;
    api.reset_operations();
    api.fail_next_status(InjectedStatusError::Unavailable);
    reconcile_set(
        &make_set("add-primary-status-loss", 2, Some(pending)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    let status = api.last_status().unwrap();
    assert_eq!(status.phase, Phase::AddingReplica);
    assert_ne!(
        status.operation.as_ref().unwrap().phase,
        DurableOperationPhase::AddDeleteCompensatedTarget
    );
    assert_eq!(api.pods.lock().unwrap().len(), 2);
    assert!(
        !api.operations()
            .contains(&ControlOperation::AddReplicaIntent)
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_add_compensates_before_and_during_configuration() {
    async fn prepare(api: &KvClusterApi, state: &ReconcilerState, name: &str) -> KubericSetStatus {
        let previous = create_healthy_set(api, state, name, 1).await;
        reconcile_set(&make_set(name, 2, Some(previous.clone())), api, state)
            .await
            .unwrap();
        api.mark_all_pods_ready();
        reconcile_set(&make_set(name, 2, Some(previous)), api, state)
            .await
            .unwrap();
        api.last_status().unwrap()
    }

    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let status = prepare(&api, &state, "add-preconfig-compensate").await;
    let pending = advance_until_pending_action(
        &api,
        "add-preconfig-compensate",
        2,
        status,
        DurableActionKind::AddReplicaIntent,
    )
    .await;
    let target_name = pending
        .operation
        .as_ref()
        .unwrap()
        .target_pod_name
        .clone()
        .unwrap();
    api.crash_pod(&target_name);
    reconcile_set(
        &make_set("add-preconfig-compensate", 2, Some(pending)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    let compensated = drive_operation_to_healthy(
        &api,
        "add-preconfig-compensate",
        2,
        api.last_status().unwrap(),
    )
    .await;
    assert_eq!(
        compensated.stable_snapshot.as_ref().unwrap().members.len(),
        1
    );
    assert_eq!(
        compensated.operation.as_ref().unwrap().phase,
        DurableOperationPhase::Failed
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_add_rolls_forward_after_current_configuration_commit() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let previous = create_healthy_set(&api, &state, "add-roll-forward", 1).await;
    reconcile_set(
        &make_set("add-roll-forward", 2, Some(previous.clone())),
        &api,
        &state,
    )
    .await
    .unwrap();
    api.mark_all_pods_ready();
    reconcile_set(
        &make_set("add-roll-forward", 2, Some(previous)),
        &api,
        &state,
    )
    .await
    .unwrap();
    let committed = advance_add_until_phase(
        &api,
        &ReconcilerState::default(),
        "add-roll-forward",
        2,
        api.last_status().unwrap(),
        DurableOperationPhase::AddRecordCommit,
    )
    .await;
    let completed = drive_operation_to_healthy(&api, "add-roll-forward", 2, committed).await;
    assert_eq!(
        completed.operation.as_ref().unwrap().phase,
        DurableOperationPhase::Completed
    );
    assert_stable_snapshot(&api, &completed, 2);
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_committed_degraded_fences_unreachable_target_from_serving() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let previous = create_healthy_set(&api, &state, "add-degraded-fence", 1).await;
    reconcile_set(
        &make_set("add-degraded-fence", 2, Some(previous.clone())),
        &api,
        &state,
    )
    .await
    .unwrap();
    api.mark_all_pods_ready();
    reconcile_set(
        &make_set("add-degraded-fence", 2, Some(previous)),
        &api,
        &state,
    )
    .await
    .unwrap();
    let publishing = advance_add_until_phase(
        &api,
        &state,
        "add-degraded-fence",
        2,
        api.last_status().unwrap(),
        DurableOperationPhase::AddPublishTarget,
    )
    .await;
    reconcile_set(
        &make_set("add-degraded-fence", 2, Some(publishing)),
        &api,
        &state,
    )
    .await
    .unwrap();
    let mut labelled = api.last_status().unwrap();
    let target_name = labelled
        .operation
        .as_ref()
        .unwrap()
        .target_pod_name
        .clone()
        .unwrap();
    assert_eq!(
        api.pods
            .lock()
            .unwrap()
            .iter()
            .find(|pod| pod.metadata.name.as_deref() == Some(target_name.as_str()))
            .and_then(|pod| pod.metadata.labels.as_ref())
            .and_then(|labels| labels.get("kuberic.io/role"))
            .map(String::as_str),
        Some("secondary")
    );
    api.crash_pod(&target_name);
    labelled
        .operation
        .as_mut()
        .unwrap()
        .phase_deadline_unix_seconds = 0;
    reconcile_set(
        &make_set("add-degraded-fence", 2, Some(labelled.clone())),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        api.pods
            .lock()
            .unwrap()
            .iter()
            .find(|pod| pod.metadata.name.as_deref() == Some(target_name.as_str()))
            .and_then(|pod| pod.metadata.labels.as_ref())
            .and_then(|labels| labels.get("kuberic.io/role"))
            .map(String::as_str),
        Some("bootstrap")
    );
    let completed = drive_operation_to_healthy(&api, "add-degraded-fence", 2, labelled).await;
    assert!(
        completed
            .conditions
            .iter()
            .any(|condition| { condition.reason == "CommittedDegraded" })
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_remove_coarse_activation() {
    let api = KvClusterApi::new();
    let state = api.removal_state();
    let previous = create_healthy_set(&api, &state, "durable-remove", 3).await;
    let previous_snapshot = previous.stable_snapshot.clone().unwrap();

    reconcile_set(&make_set("durable-remove", 2, Some(previous)), &api, &state)
        .await
        .unwrap();
    let mut status = api.last_status().unwrap();
    assert_eq!(status.phase, Phase::RemovingReplica);
    assert_eq!(
        status.operation.as_ref().unwrap().kind,
        DurableOperationKind::RemoveReplica
    );
    assert_eq!(
        status.operation.as_ref().unwrap().remove_mode,
        Some(DurableRemoveMode::ScaleDown)
    );

    let expected = [DurableActionKind::RemoveReplicaIntent];
    let mut injected = Vec::new();
    let mut persisted_phases = Vec::new();
    let history_start = api.statuses.lock().unwrap().len();
    api.reset_operations();

    for _ in 0..160 {
        let operation = status.operation.as_ref().unwrap();
        if !persisted_phases.contains(&operation.phase) {
            persisted_phases.push(operation.phase);
        }
        if let Some(pending) = operation.pending_action.as_ref()
            && pending.attempts == 0
            && expected.contains(&pending.kind)
            && !injected.contains(&pending.kind)
            && let Some(control) = control_for_action(pending.kind)
        {
            api.fail_after_next_durable_action(control);
            injected.push(pending.kind);
        }

        reconcile_set(
            &make_set("durable-remove", 2, Some(status.clone())),
            &api,
            &state,
        )
        .await
        .unwrap();
        status = api.last_status().unwrap();
        if status.phase == Phase::Healthy {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(status.phase, Phase::Healthy);
    assert_eq!(
        status.operation.as_ref().unwrap().phase,
        DurableOperationPhase::Completed
    );
    assert_eq!(
        status
            .operation
            .as_ref()
            .unwrap()
            .remove_intent
            .as_ref()
            .unwrap()
            .last_observed_result,
        Some(RemoveReplicaTerminalResultStatus::CommittedClean)
    );
    assert_eq!(injected, expected);
    assert_stable_snapshot(&api, &status, 2);
    assert!(persisted_phases.contains(&DurableOperationPhase::RemoveFreezeIntent));
    assert!(persisted_phases.contains(&DurableOperationPhase::RemoveDispatchIntent));
    assert!(persisted_phases.contains(&DurableOperationPhase::RemoveAwaitCoordination));
    assert!(persisted_phases.contains(&DurableOperationPhase::RemoveRecordCommit));
    assert!(persisted_phases.contains(&DurableOperationPhase::RemoveAwaitCleanup));
    assert!(persisted_phases.contains(&DurableOperationPhase::RemoveDeleteTargetPod));
    assert!(persisted_phases.contains(&DurableOperationPhase::RemovePublishTopology));
    assert!(persisted_phases.contains(&DurableOperationPhase::RemoveFinalize));

    let history = api.statuses.lock().unwrap();
    let removal_history = &history[history_start..];
    let first_reduced = removal_history
        .iter()
        .position(|entry| {
            entry
                .stable_snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.members.len() == 2)
        })
        .expect("current configuration commit must persist the reduced snapshot");
    assert!(
        removal_history[..first_reduced]
            .iter()
            .all(|entry| entry.stable_snapshot.as_ref() == Some(&previous_snapshot))
    );
    assert_eq!(
        removal_history[first_reduced]
            .operation
            .as_ref()
            .unwrap()
            .phase,
        DurableOperationPhase::RemoveFinalize
    );
    assert_one_coarse_remove_intent_and_no_fine_grained_controls(&api.operations());
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_force_remove_unreachable_secondary_with_retained_quorum() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let previous = create_healthy_set(&api, &state, "force-remove", 3).await;
    let primary = previous.current_primary.clone().unwrap();
    let target = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .map(|pod| pod.metadata.name.clone().unwrap())
        .find(|name| name != &primary)
        .unwrap();
    let target_uid = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .find(|pod| pod.metadata.name.as_deref() == Some(target.as_str()))
        .unwrap()
        .metadata
        .uid
        .clone()
        .unwrap();
    api.crash_pod(&target);

    reconcile_set(&make_set("force-remove", 3, Some(previous)), &api, &state)
        .await
        .unwrap();
    let started = api.last_status().unwrap();
    assert_eq!(started.phase, Phase::RemovingReplica);
    assert_eq!(
        started.operation.as_ref().unwrap().remove_mode,
        Some(DurableRemoveMode::Force)
    );
    assert_eq!(
        started
            .operation
            .as_ref()
            .unwrap()
            .target_pod_uid
            .as_deref(),
        Some(target_uid.as_str())
    );

    let completed = drive_operation_to_healthy(&api, "force-remove", 3, started).await;
    assert_stable_snapshot(&api, &completed, 2);
    let operation = completed.operation.as_ref().unwrap();
    assert_eq!(
        operation
            .remove_intent
            .as_ref()
            .unwrap()
            .last_observed_result,
        Some(RemoveReplicaTerminalResultStatus::CommittedDegraded)
    );
    assert_eq!(
        operation.remove_cleanup.as_ref().unwrap().target_retirement,
        Some(TargetRetirementObservationStatus::Unavailable)
    );
    assert!(
        api.pods
            .lock()
            .unwrap()
            .iter()
            .all(|pod| pod.metadata.uid.as_deref() != Some(target_uid.as_str()))
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_remove_rejects_malformed_agent_status_without_mutation() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let status = create_healthy_set(&api, &state, "remove-malformed-agent", 3).await;

    reconcile_set(
        &make_set("remove-malformed-agent", 2, Some(status)),
        &api,
        &state,
    )
    .await
    .unwrap();
    let started = api.last_status().unwrap();
    assert_eq!(started.phase, Phase::RemovingReplica);
    let status_count = api.statuses.lock().unwrap().len();

    api.reset_operations();
    api.fail_next_status(InjectedStatusError::MalformedAgentStatus);
    let error = reconcile_set(
        &make_set("remove-malformed-agent", 2, Some(started.clone())),
        &api,
        &state,
    )
    .await
    .err()
    .expect("malformed control status must fail closed");

    assert!(error.contains("unsupported or malformed control status"));
    assert_eq!(api.statuses.lock().unwrap().len(), status_count);
    assert_eq!(api.last_status().unwrap(), started);
    assert!(
        api.operations()
            .iter()
            .all(|operation| *operation == ControlOperation::GetStatus)
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_remove_intent_conflict_and_uid_fenced_cleanup_are_mutation_free() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let previous = create_healthy_set(&api, &state, "remove-conflict", 3).await;
    api.reset_operations();
    api.fail_next_status_conflict();
    let error = match reconcile_set(
        &make_set("remove-conflict", 2, Some(previous)),
        &api,
        &state,
    )
    .await
    {
        Ok(_) => panic!("conflicting remove intent status must fail"),
        Err(error) => error,
    };
    assert_eq!(error, "resource version conflict");
    assert_status_reads_only(&api.operations());

    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let previous = create_healthy_set(&api, &state, "remove-uid-race", 3).await;
    let primary = previous.current_primary.clone().unwrap();
    let target_name = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .map(|pod| pod.metadata.name.clone().unwrap())
        .find(|name| name != &primary)
        .unwrap();
    let old_uid = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .find(|pod| pod.metadata.name.as_deref() == Some(target_name.as_str()))
        .unwrap()
        .metadata
        .uid
        .clone()
        .unwrap();
    api.crash_pod(&target_name);
    api.restart_pod(&target_name).await;
    api.mark_pod_not_ready(&target_name);
    let replacement_uid = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .find(|pod| pod.metadata.name.as_deref() == Some(target_name.as_str()))
        .unwrap()
        .metadata
        .uid
        .clone()
        .unwrap();
    assert_ne!(old_uid, replacement_uid);

    reconcile_set(
        &make_set("remove-uid-race", 3, Some(previous)),
        &api,
        &state,
    )
    .await
    .unwrap();
    let completed =
        drive_operation_to_healthy(&api, "remove-uid-race", 3, api.last_status().unwrap()).await;
    assert_stable_snapshot(&api, &completed, 2);
    assert_eq!(
        api.pods
            .lock()
            .unwrap()
            .iter()
            .find(|pod| pod.metadata.name.as_deref() == Some(target_name.as_str()))
            .unwrap()
            .metadata
            .uid
            .as_deref(),
        Some(replacement_uid.as_str()),
        "old-incarnation cleanup deleted the replacement pod"
    );
    assert_eq!(
        api.pods
            .lock()
            .unwrap()
            .iter()
            .find(|pod| pod.metadata.name.as_deref() == Some(target_name.as_str()))
            .and_then(|pod| pod.metadata.labels.as_ref())
            .and_then(|labels| labels.get("kuberic.io/role"))
            .map(String::as_str),
        Some("secondary"),
        "old-incarnation cleanup relabelled the replacement pod"
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_scale_down_preadmission_and_minimum_are_mutation_free() {
    let api = KvClusterApi::new();
    let state = api.removal_state();
    let previous = create_healthy_set(&api, &state, "remove-preadmit", 3).await;
    let target_name = "remove-preadmit-2";
    let target_uid = previous
        .stable_snapshot
        .as_ref()
        .unwrap()
        .members
        .iter()
        .find(|member| member.id == 3)
        .unwrap()
        .instance_id
        .clone();
    api.fail_status_after_successes(target_name, 1, InjectedStatusError::Unavailable);
    api.reset_operations();

    reconcile_set(
        &make_set("remove-preadmit", 2, Some(previous.clone())),
        &api,
        &state,
    )
    .await
    .unwrap();
    let blocked = api.last_status().unwrap();
    assert_eq!(blocked.phase, Phase::Healthy);
    assert!(
        blocked.operation.as_ref().is_none_or(|operation| {
            operation.kind != DurableOperationKind::RemoveReplica
                || matches!(
                    operation.phase,
                    DurableOperationPhase::Completed
                        | DurableOperationPhase::Failed
                        | DurableOperationPhase::Poisoned
                )
        }),
        "unreachable ScaleDown target persisted a removal operation"
    );
    let conditions = blocked
        .conditions
        .iter()
        .filter(|condition| condition.type_ == "ScaleDownTargetUnavailable")
        .collect::<Vec<_>>();
    assert_eq!(conditions.len(), 1);
    assert_eq!(conditions[0].reason, "ScaleDownTargetUnavailable");
    assert!(
        !api.operations()
            .contains(&ControlOperation::RemoveReplicaIntent)
    );

    let status_count = api.statuses.lock().unwrap().len();
    api.fail_status_after_successes(target_name, 1, InjectedStatusError::Unavailable);
    api.reset_operations();
    reconcile_set(
        &make_set("remove-preadmit", 2, Some(blocked.clone())),
        &api,
        &state,
    )
    .await
    .unwrap();
    assert_eq!(
        api.statuses.lock().unwrap().len(),
        status_count,
        "persistent preadmission unavailability rewrote unchanged status"
    );
    assert_eq!(api.last_status().unwrap(), blocked);
    assert_status_reads_only(&api.operations());

    api.crash_pod(target_name);
    api.delete_pod("default", target_name, &target_uid)
        .await
        .unwrap();
    reconcile_set(&make_set("remove-preadmit", 2, Some(blocked)), &api, &state)
        .await
        .unwrap();
    let force_authorized = api.last_status().unwrap();
    assert_eq!(force_authorized.phase, Phase::RemovingReplica);
    assert_eq!(
        force_authorized.operation.as_ref().unwrap().remove_mode,
        Some(DurableRemoveMode::Force),
        "Force must be separately authorized by the later missing-member health evaluation"
    );

    let minimum_api = KvClusterApi::new();
    let minimum_state = minimum_api.removal_state();
    let minimum = create_healthy_set(&minimum_api, &minimum_state, "remove-minimum", 3).await;
    minimum_api.reset_operations();
    let error = reconcile_error(
        reconcile_set(
            &make_set_with_min("remove-minimum", 2, 3, Some(minimum.clone())),
            &minimum_api,
            &minimum_state,
        )
        .await,
    );
    assert!(error.contains("below minReplicas"));
    assert_eq!(minimum_api.last_status().unwrap(), minimum);
    assert!(
        !minimum_api
            .operations()
            .contains(&ControlOperation::RemoveReplicaIntent)
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_scale_down_freeze_rejects_same_uid_target_process_generation_drift() {
    let api = KvClusterApi::new();
    let state = api.removal_state();
    let previous = create_healthy_set(&api, &state, "remove-target-generation", 3).await;

    reconcile_set(
        &make_set("remove-target-generation", 2, Some(previous)),
        &api,
        &state,
    )
    .await
    .unwrap();
    let started = api.last_status().unwrap();
    let operation = started.operation.as_ref().unwrap();
    assert_eq!(operation.phase, DurableOperationPhase::RemoveFreezeIntent);
    let preadmitted_generation = operation
        .remove_target_agent_generation
        .clone()
        .expect("ScaleDown must persist the exact preadmitted target generation");
    let target_name = operation.target_pod_name.clone().unwrap();
    let target_uid = operation.target_pod_uid.clone().unwrap();

    api.crash_pod(&target_name);
    api.restart_process_same_pod_uid(&target_name).await;
    let current_target_uid = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .find(|pod| pod.metadata.name.as_deref() == Some(target_name.as_str()))
        .and_then(|pod| pod.metadata.uid.clone());
    assert_eq!(current_target_uid.as_deref(), Some(target_uid.as_str()));

    api.reset_operations();
    reconcile_set(
        &make_set("remove-target-generation", 2, Some(started)),
        &api,
        &state,
    )
    .await
    .unwrap();
    let blocked = api.last_status().unwrap();
    let blocked_operation = blocked.operation.as_ref().unwrap();
    assert_eq!(
        blocked_operation.phase,
        DurableOperationPhase::RemoveFreezeIntent
    );
    assert_eq!(
        blocked_operation.remove_target_agent_generation.as_deref(),
        Some(preadmitted_generation.as_str())
    );
    assert!(blocked_operation.remove_intent.is_none());
    assert!(blocked.conditions.iter().any(|condition| {
        condition.reason == "IncompatibleOrInvalid"
            && condition
                .message
                .contains("generation changed after preadmission")
    }));
    assert!(
        !api.operations()
            .contains(&ControlOperation::RemoveReplicaIntent)
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_scale_down_target_loss_after_dispatch_never_changes_to_force() {
    let api = KvClusterApi::new();
    let state = api.removal_state();
    let previous = create_healthy_set(&api, &state, "remove-target-loss", 3).await;
    reconcile_set(
        &make_set("remove-target-loss", 2, Some(previous)),
        &api,
        &state,
    )
    .await
    .unwrap();
    let frozen = advance_remove_until_operation_phase(
        &api,
        &state,
        "remove-target-loss",
        2,
        api.last_status().unwrap(),
        DurableOperationPhase::RemoveDispatchIntent,
    )
    .await;
    assert_eq!(
        frozen.operation.as_ref().unwrap().remove_mode,
        Some(DurableRemoveMode::ScaleDown)
    );
    api.crash_pod("remove-target-loss-2");
    let retiring = advance_remove_until_coordinator_phase(
        &api,
        &state,
        "remove-target-loss",
        2,
        frozen,
        RemoveReplicaCoordinatorPhaseStatus::RetiringTarget,
    )
    .await;
    let deadline = retiring
        .operation
        .as_ref()
        .unwrap()
        .remove_intent
        .as_ref()
        .unwrap()
        .overall_deadline_unix_seconds;
    api.removal_clock.set(deadline);
    let completed =
        drive_operation_to_healthy_with_state(&api, &state, "remove-target-loss", 2, retiring)
            .await;
    assert_eq!(
        completed.operation.as_ref().unwrap().remove_mode,
        Some(DurableRemoveMode::ScaleDown)
    );
    assert_eq!(
        completed
            .operation
            .as_ref()
            .unwrap()
            .remove_intent
            .as_ref()
            .unwrap()
            .last_observed_result,
        Some(RemoveReplicaTerminalResultStatus::CommittedDegraded)
    );
    assert_stable_snapshot(&api, &completed, 2);
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_precommit_quorum_loss_compensates_without_reduced_publication() {
    let api = KvClusterApi::new();
    let state = api.removal_state();
    let previous = create_healthy_set(&api, &state, "remove-quorum-compensate", 3).await;
    let previous_snapshot = previous.stable_snapshot.clone().unwrap();
    let history_start = api.statuses.lock().unwrap().len();

    reconcile_set(
        &make_set("remove-quorum-compensate", 2, Some(previous)),
        &api,
        &state,
    )
    .await
    .unwrap();
    let authorized = advance_remove_until_dispatch_authorized(
        &api,
        &state,
        "remove-quorum-compensate",
        2,
        api.last_status().unwrap(),
    )
    .await;

    let retained_pod = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .find(|pod| pod.metadata.name.as_deref() == Some("remove-quorum-compensate-1"))
        .unwrap()
        .clone();
    let retained = api
        .create_replica_handle(
            2,
            &retained_pod,
            &make_set("remove-quorum-compensate", 2, None).spec,
        )
        .await
        .unwrap();
    execute_with_fresh_fences(
        retained.as_ref(),
        "remove-quorum-compensate:close-retained",
        DurableReplicaAction::Close,
    )
    .await
    .unwrap();
    let target_pod = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .find(|pod| pod.metadata.name.as_deref() == Some("remove-quorum-compensate-2"))
        .unwrap()
        .clone();
    let target = api
        .create_replica_handle(
            3,
            &target_pod,
            &make_set("remove-quorum-compensate", 2, None).spec,
        )
        .await
        .unwrap();
    execute_with_fresh_fences(
        target.as_ref(),
        "remove-quorum-compensate:close-target",
        DurableReplicaAction::Close,
    )
    .await
    .unwrap();
    let mut client = connect_kv(&api.client_address("remove-quorum-compensate-0").unwrap()).await;
    let write = tokio::spawn(async move {
        client
            .put(proto::PutRequest {
                key: "uncommitted-before-remove".to_string(),
                value: "must-not-reduce".to_string(),
            })
            .await
    });
    tokio::task::yield_now().await;

    let waiting = advance_remove_until_coordinator_phase(
        &api,
        &state,
        "remove-quorum-compensate",
        2,
        authorized,
        RemoveReplicaCoordinatorPhaseStatus::WaitingForCatchUpQuorum,
    )
    .await;
    let deadline = waiting
        .operation
        .as_ref()
        .unwrap()
        .remove_intent
        .as_ref()
        .unwrap()
        .overall_deadline_unix_seconds;
    api.removal_clock.set(deadline);

    let completed =
        drive_operation_to_healthy_with_state(&api, &state, "remove-quorum-compensate", 2, waiting)
            .await;
    let write = tokio::time::timeout(Duration::from_secs(8), write)
        .await
        .expect("quorum-loss write must return or fail within the replication bound")
        .unwrap();
    assert!(
        write.is_err(),
        "write unexpectedly committed after both secondary runtimes closed"
    );
    let operation = completed.operation.as_ref().unwrap();
    assert_eq!(operation.phase, DurableOperationPhase::Failed);
    assert_eq!(
        operation
            .remove_intent
            .as_ref()
            .unwrap()
            .last_observed_result,
        Some(RemoveReplicaTerminalResultStatus::Compensated)
    );
    assert!(operation.remove_commit_evidence.is_none());
    assert!(operation.committed_snapshot.is_none());
    let republished = completed.stable_snapshot.as_ref().unwrap();
    assert_eq!(republished.epoch, previous_snapshot.epoch);
    assert_eq!(republished.primary_id, previous_snapshot.primary_id);
    assert_eq!(republished.write_quorum, previous_snapshot.write_quorum);
    assert_eq!(
        republished
            .members
            .iter()
            .map(|member| (member.id, member.instance_id.as_str(), member.role))
            .collect::<Vec<_>>(),
        previous_snapshot
            .members
            .iter()
            .map(|member| (member.id, member.instance_id.as_str(), member.role))
            .collect::<Vec<_>>(),
        "compensation must republish the exact previous topology"
    );
    assert!(
        api.statuses.lock().unwrap()[history_start..]
            .iter()
            .all(|status| status
                .stable_snapshot
                .as_ref()
                .is_none_or(|snapshot| snapshot.members.len() == 3)),
        "pre-commit quorum loss published a reduced stable snapshot"
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_primary_process_restart_matrix_never_restores_same_epoch_primary() {
    for (suffix, phase, current_install_dispatched) in [
        (
            "validating",
            RemoveReplicaCoordinatorPhaseStatus::Validating,
            false,
        ),
        (
            "install-catchup",
            RemoveReplicaCoordinatorPhaseStatus::InstallingCatchUpConfiguration,
            false,
        ),
        (
            "wait-quorum",
            RemoveReplicaCoordinatorPhaseStatus::WaitingForCatchUpQuorum,
            false,
        ),
        (
            "install-current",
            RemoveReplicaCoordinatorPhaseStatus::InstallingCurrentConfiguration,
            true,
        ),
        (
            "compensating",
            RemoveReplicaCoordinatorPhaseStatus::Compensating,
            false,
        ),
    ] {
        let name = format!("remove-primary-restart-{suffix}");
        let api = KvClusterApi::new();
        let state = api.removal_state();
        let previous = create_healthy_set(&api, &state, &name, 3).await;
        let previous_snapshot = previous.stable_snapshot.clone();
        let primary_name = previous.current_primary.clone().unwrap();
        reconcile_set(&make_set(&name, 2, Some(previous)), &api, &state)
            .await
            .unwrap();
        let mut checkpoint = advance_remove_until_dispatch_authorized(
            &api,
            &state,
            &name,
            2,
            api.last_status().unwrap(),
        )
        .await;
        api.reset_operations();
        reconcile_set(&make_set(&name, 2, Some(checkpoint.clone())), &api, &state)
            .await
            .unwrap();
        assert_eq!(
            api.operations()
                .iter()
                .filter(|operation| **operation == ControlOperation::RemoveReplicaIntent)
                .count(),
            1,
            "the {phase:?} restart fixture did not dispatch one coarse intent"
        );

        let intent = checkpoint
            .operation
            .as_mut()
            .unwrap()
            .remove_intent
            .as_mut()
            .unwrap();
        intent.last_observed_phase = Some(phase);
        intent.last_observed_result = None;
        intent.current_install_dispatched = current_install_dispatched;
        api.crash_pod(&primary_name);
        api.restart_process_same_pod_uid(&primary_name).await;

        api.reset_operations();
        reconcile_set(
            &make_set(&name, 2, Some(checkpoint)),
            &api,
            &api.removal_state(),
        )
        .await
        .unwrap();
        let poisoned = api.last_status().unwrap();
        let operation = poisoned.operation.as_ref().unwrap();
        assert_eq!(operation.phase, DurableOperationPhase::Poisoned);
        assert!(operation.remove_commit_evidence.is_none());
        assert!(operation.committed_snapshot.is_none());
        assert_eq!(poisoned.stable_snapshot, previous_snapshot);
        assert!(matches!(
            operation.removal_disposition,
            Some(RemoveReplicaDispositionStatus::AmbiguousPrimaryRestart {
                last_observed_phase: Some(observed),
                ..
            }) if observed == phase
        ));
        assert_status_reads_only(&api.operations());
        assert_no_same_epoch_primary_restoration(&api.operations());
    }

    for (suffix, phase) in [
        (
            "remove-connection",
            RemoveReplicaCoordinatorPhaseStatus::RemovingConnection,
        ),
        (
            "retire-target",
            RemoveReplicaCoordinatorPhaseStatus::RetiringTarget,
        ),
        ("attesting", RemoveReplicaCoordinatorPhaseStatus::Attesting),
    ] {
        let name = format!("remove-primary-restart-{suffix}");
        let api = KvClusterApi::new();
        let state = api.removal_state();
        let previous = create_healthy_set(&api, &state, &name, 3).await;
        let primary_name = previous.current_primary.clone().unwrap();
        reconcile_set(&make_set(&name, 2, Some(previous)), &api, &state)
            .await
            .unwrap();
        let mut committed = advance_remove_until_operation_phase(
            &api,
            &state,
            &name,
            2,
            api.last_status().unwrap(),
            DurableOperationPhase::RemoveRecordCommit,
        )
        .await;
        committed
            .operation
            .as_mut()
            .unwrap()
            .remove_intent
            .as_mut()
            .unwrap()
            .last_observed_phase = Some(phase);
        api.crash_pod(&primary_name);
        api.restart_process_same_pod_uid(&primary_name).await;

        api.reset_operations();
        let completed =
            drive_operation_to_healthy_with_state(&api, &api.removal_state(), &name, 2, committed)
                .await;
        assert_stable_snapshot(&api, &completed, 2);
        assert_eq!(
            completed.operation.as_ref().unwrap().phase,
            DurableOperationPhase::Completed
        );
        assert_no_same_epoch_primary_restoration(&api.operations());
    }
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_lifecycle_peer_lost_stage_reply_recovers_from_real_grpc_postconditions() {
    let lost_api = KvClusterApi::new();
    let lost_state = lost_api.removal_state();
    let lost_previous = create_healthy_set(&lost_api, &lost_state, "remove-peer-lost", 3).await;
    let lost_proxy = lost_api.install_peer_proxy("remove-peer-lost-2").await;
    lost_proxy.set(PeerProxyMode::LoseNextStageReply);
    reconcile_set(
        &make_set("remove-peer-lost", 2, Some(lost_previous)),
        &lost_api,
        &lost_state,
    )
    .await
    .unwrap();
    let lost_completed = drive_operation_to_healthy_with_state(
        &lost_api,
        &lost_state,
        "remove-peer-lost",
        2,
        lost_api.last_status().unwrap(),
    )
    .await;
    assert_eq!(
        lost_completed
            .operation
            .as_ref()
            .unwrap()
            .remove_intent
            .as_ref()
            .unwrap()
            .last_observed_result,
        Some(RemoveReplicaTerminalResultStatus::CommittedClean)
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_lifecycle_peer_temporarily_unavailable_returns_before_expiry() {
    let reachable_api = KvClusterApi::new();
    let reachable_state = reachable_api.removal_state();
    let reachable_previous =
        create_healthy_set(&reachable_api, &reachable_state, "remove-peer-return", 3).await;
    let target = reachable_previous
        .stable_snapshot
        .as_ref()
        .unwrap()
        .members
        .iter()
        .find(|member| member.id == 3)
        .unwrap()
        .clone();
    let target_address = reachable_api
        .live_pods
        .lock()
        .unwrap()
        .get("remove-peer-return-2")
        .unwrap()
        .data_address
        .clone();
    let operation = start_remove_replica(
        "test-uid",
        reachable_previous.stable_snapshot.clone().unwrap(),
        RemoveReplicaTarget {
            replica_id: target.id,
            pod_name: "remove-peer-return-2".to_string(),
            pod_uid: target.instance_id,
            replicator_address: target_address,
            agent_generation: None,
        },
        DurableRemoveMode::Force,
        1,
        reachable_api.removal_clock.advance(0),
    )
    .unwrap();
    let authorized = KubericSetStatus {
        phase: Phase::RemovingReplica,
        operation: Some(operation),
        ..reachable_previous
    };
    let reachable_proxy = reachable_api
        .install_peer_proxy("remove-peer-return-2")
        .await;
    reachable_proxy.set(PeerProxyMode::Unavailable);
    reconcile_set(
        &make_set("remove-peer-return", 2, Some(authorized)),
        &reachable_api,
        &reachable_state,
    )
    .await
    .unwrap();
    let mut returning = reachable_api.last_status().unwrap();
    for _ in 0..200 {
        reconcile_set(
            &make_set("remove-peer-return", 2, Some(returning.clone())),
            &reachable_api,
            &reachable_state,
        )
        .await
        .unwrap();
        returning = reachable_api.last_status().unwrap();
        if live_remove_progress(&reachable_api, "remove-peer-return", 2, &returning)
            .await
            .is_some_and(|progress| {
                progress.phase == RemoveReplicaCoordinatorPhase::RetiringTarget
                    && progress.target_retirement == TargetRetirementObservation::Unavailable
            })
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        live_remove_progress(&reachable_api, "remove-peer-return", 2, &returning)
            .await
            .is_some_and(|progress| {
                progress.target_retirement == TargetRetirementObservation::Unavailable
            })
    );
    assert_eq!(
        returning.operation.as_ref().unwrap().remove_mode,
        Some(DurableRemoveMode::Force)
    );
    reachable_proxy.set(PeerProxyMode::Forward);
    let mut terminal = None;
    for _ in 0..200 {
        terminal =
            live_remove_terminal_result(&reachable_api, "remove-peer-return", 2, &returning).await;
        if terminal.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(terminal, Some(RemoveReplicaTerminalResult::CommittedClean));
    let returning_completed = drive_operation_to_healthy_with_state(
        &reachable_api,
        &reachable_state,
        "remove-peer-return",
        2,
        returning,
    )
    .await;
    assert_eq!(
        returning_completed
            .operation
            .as_ref()
            .unwrap()
            .remove_cleanup
            .as_ref()
            .unwrap()
            .target_retirement,
        Some(TargetRetirementObservationStatus::Completed)
    );
    assert_eq!(
        returning_completed
            .operation
            .as_ref()
            .unwrap()
            .remove_intent
            .as_ref()
            .unwrap()
            .last_observed_result,
        Some(RemoveReplicaTerminalResultStatus::CommittedClean)
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_lifecycle_peer_stall_expires_degraded_with_responsive_status() {
    let stalled_api = KvClusterApi::new();
    let stalled_state = stalled_api.removal_state();
    let stalled_previous =
        create_healthy_set(&stalled_api, &stalled_state, "remove-peer-stall", 3).await;
    let lifecycle = stalled_api.lifecycle_control("remove-peer-stall-2");
    lifecycle.hold(HeldLifecycleEffect::ChangeRoleNone);
    reconcile_set(
        &make_set("remove-peer-stall", 2, Some(stalled_previous)),
        &stalled_api,
        &stalled_state,
    )
    .await
    .unwrap();
    let mut stalled = stalled_api.last_status().unwrap();
    for _ in 0..200 {
        reconcile_set(
            &make_set("remove-peer-stall", 2, Some(stalled.clone())),
            &stalled_api,
            &stalled_state,
        )
        .await
        .unwrap();
        stalled = stalled_api.last_status().unwrap();
        if lifecycle.has_entered(HeldLifecycleEffect::ChangeRoleNone) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    lifecycle
        .wait_until_entered(HeldLifecycleEffect::ChangeRoleNone)
        .await;
    for _ in 0..5 {
        let status = tokio::time::timeout(
            Duration::from_millis(200),
            live_remove_progress(&stalled_api, "remove-peer-stall", 2, &stalled),
        )
        .await
        .expect("primary status must remain responsive");
        assert!(status.is_some());
    }
    let overall_deadline = stalled
        .operation
        .as_ref()
        .unwrap()
        .remove_intent
        .as_ref()
        .unwrap()
        .overall_deadline_unix_seconds;
    stalled_api.removal_clock.set(overall_deadline);
    let stalled_completed = drive_operation_to_healthy_with_state(
        &stalled_api,
        &stalled_state,
        "remove-peer-stall",
        2,
        stalled,
    )
    .await;
    assert_eq!(
        stalled_completed
            .operation
            .as_ref()
            .unwrap()
            .remove_intent
            .as_ref()
            .unwrap()
            .last_observed_result,
        Some(RemoveReplicaTerminalResultStatus::CommittedDegraded)
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_primary_process_restart_poison_is_durable_and_operator_restart_is_a_no_op() {
    let api = KvClusterApi::new();
    let state = api.removal_state();
    let previous = create_healthy_set(&api, &state, "remove-primary-poison", 3).await;
    let target_lifecycle = api.lifecycle_control("remove-primary-poison-2");
    target_lifecycle.hold(HeldLifecycleEffect::ChangeRoleNone);
    reconcile_set(
        &make_set("remove-primary-poison", 2, Some(previous.clone())),
        &api,
        &state,
    )
    .await
    .unwrap();
    let waiting = advance_remove_until_coordinator_phase(
        &api,
        &state,
        "remove-primary-poison",
        2,
        api.last_status().unwrap(),
        RemoveReplicaCoordinatorPhaseStatus::RetiringTarget,
    )
    .await;
    target_lifecycle
        .wait_until_entered(HeldLifecycleEffect::ChangeRoleNone)
        .await;
    let primary_name = previous.current_primary.clone().unwrap();

    api.crash_pod(&primary_name);
    api.restart_process_same_pod_uid(&primary_name).await;
    api.reset_operations();
    reconcile_set(
        &make_set("remove-primary-poison", 2, Some(waiting.clone())),
        &api,
        &api.removal_state(),
    )
    .await
    .unwrap();
    let poisoned = api.last_status().unwrap();
    assert_eq!(
        poisoned.operation.as_ref().unwrap().phase,
        DurableOperationPhase::Poisoned
    );
    assert!(matches!(
        poisoned.operation.as_ref().unwrap().removal_disposition,
        Some(RemoveReplicaDispositionStatus::AmbiguousPrimaryRestart { .. })
    ));
    assert!(
        poisoned
            .operation
            .as_ref()
            .unwrap()
            .remove_commit_evidence
            .is_none()
    );
    assert_eq!(poisoned.stable_snapshot, previous.stable_snapshot);

    api.reset_operations();
    let status_count = api.statuses.lock().unwrap().len();
    reconcile_set(
        &make_set("remove-primary-poison", 2, Some(poisoned.clone())),
        &api,
        &api.removal_state(),
    )
    .await
    .unwrap();
    assert_eq!(api.statuses.lock().unwrap().len(), status_count);
    assert_eq!(api.last_status().unwrap(), poisoned);
    assert!(
        api.operations()
            .iter()
            .all(|operation| *operation == ControlOperation::GetStatus)
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_post_commit_primary_restart_rolls_forward_from_committed_snapshot_then_fails_over() {
    let api = KvClusterApi::new();
    let state = api.removal_state();
    let previous = create_healthy_set(&api, &state, "remove-postcommit-restart", 3).await;
    let removed_id = 3;
    let primary_name = previous.current_primary.clone().unwrap();
    reconcile_set(
        &make_set("remove-postcommit-restart", 2, Some(previous.clone())),
        &api,
        &state,
    )
    .await
    .unwrap();
    let committed = advance_remove_until_operation_phase(
        &api,
        &state,
        "remove-postcommit-restart",
        2,
        api.last_status().unwrap(),
        DurableOperationPhase::RemoveRecordCommit,
    )
    .await;
    assert_eq!(committed.stable_snapshot, previous.stable_snapshot);
    assert_eq!(
        committed
            .operation
            .as_ref()
            .unwrap()
            .committed_snapshot
            .as_ref()
            .unwrap()
            .members
            .len(),
        2
    );

    api.crash_pod(&primary_name);
    api.restart_process_same_pod_uid(&primary_name).await;
    let removed = drive_operation_to_healthy_with_state(
        &api,
        &api.removal_state(),
        "remove-postcommit-restart",
        2,
        committed,
    )
    .await;
    assert_stable_snapshot(&api, &removed, 2);
    assert!(
        removed
            .stable_snapshot
            .as_ref()
            .unwrap()
            .members
            .iter()
            .all(|member| member.id != removed_id)
    );

    let mut status = removed;
    for _ in 0..120 {
        reconcile_set(
            &make_set("remove-postcommit-restart", 2, Some(status.clone())),
            &api,
            &ReconcilerState::default(),
        )
        .await
        .unwrap();
        status = api.last_status().unwrap();
        if status.phase == Phase::FailingOver {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(status.phase, Phase::FailingOver, "{status:?}");
    assert!(
        status
            .operation
            .as_ref()
            .unwrap()
            .previous_snapshot
            .as_ref()
            .unwrap()
            .members
            .iter()
            .all(|member| member.id != removed_id)
    );
    let recovered = drive_operation_to_healthy(&api, "remove-postcommit-restart", 2, status).await;
    assert!(
        recovered
            .stable_snapshot
            .as_ref()
            .unwrap()
            .members
            .iter()
            .all(|member| member.id != removed_id)
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_post_commit_missing_exact_primary_status_never_waives_connection_cleanup() {
    let api = KvClusterApi::new();
    let state = api.removal_state();
    let previous =
        create_healthy_set(&api, &state, "remove-postcommit-primary-status-gap", 3).await;
    let primary_name = previous.current_primary.clone().unwrap();

    reconcile_set(
        &make_set(
            "remove-postcommit-primary-status-gap",
            2,
            Some(previous.clone()),
        ),
        &api,
        &state,
    )
    .await
    .unwrap();
    let frozen = advance_remove_until_operation_phase(
        &api,
        &state,
        "remove-postcommit-primary-status-gap",
        2,
        api.last_status().unwrap(),
        DurableOperationPhase::RemoveDispatchIntent,
    )
    .await;
    let mut checkpoint = frozen;
    let operation = checkpoint.operation.as_mut().unwrap();
    let (attempt_id, action_id, primary_agent_generation) = {
        let intent = operation.remove_intent.as_ref().unwrap();
        (
            intent.attempt_id.clone(),
            intent.action_id.clone(),
            intent.primary_agent_generation.clone(),
        )
    };
    let target_id = operation.target_replica_id.unwrap();
    let target_instance_id = operation.target_instance_id.clone().unwrap();
    let primary_pod = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .find(|pod| pod.metadata.name.as_deref() == Some(primary_name.as_str()))
        .unwrap()
        .clone();
    let primary = api
        .create_replica_handle(
            operation.old_primary_id,
            &primary_pod,
            &make_set("remove-postcommit-primary-status-gap", 2, None).spec,
        )
        .await
        .unwrap();
    assert!(
        primary
            .get_status()
            .await
            .unwrap()
            .active_replica_connections
            .iter()
            .any(|connection| {
                connection.id == target_id
                    && connection.instance_id.as_str() == target_instance_id.as_str()
            }),
        "fixture must retain the exact target connection"
    );

    operation.phase = DurableOperationPhase::RemoveAwaitCleanup;
    operation.committed_snapshot = Some(operation.target_snapshot.clone());
    operation.remove_commit_evidence = Some(RemoveReplicaCommitEvidenceStatus {
        attempt_id,
        action_id,
        primary_agent_generation,
        configuration_signature: "durable-test-commit".to_string(),
        observed_unix_seconds: api.removal_clock.advance(0),
    });
    operation.remove_cleanup = Some(RemoveReplicaCleanupStatus::default());
    let checkpoint_before_gap = checkpoint.clone();
    api.statuses.lock().unwrap().push(checkpoint.clone());
    let status_count = api.statuses.lock().unwrap().len();
    api.removal_clock.advance(10_000);
    api.fail_status_after_successes(&primary_name, 0, InjectedStatusError::Unavailable);

    reconcile_set(
        &make_set("remove-postcommit-primary-status-gap", 2, Some(checkpoint)),
        &api,
        &state,
    )
    .await
    .unwrap();

    assert_eq!(
        api.statuses.lock().unwrap().len(),
        status_count,
        "temporary loss of exact primary status must not churn or advance status"
    );
    assert_eq!(api.last_status().unwrap(), checkpoint_before_gap);
    assert!(
        !checkpoint_before_gap
            .operation
            .as_ref()
            .unwrap()
            .remove_cleanup
            .as_ref()
            .unwrap()
            .connection_absent,
        "retirement or overall deadline cannot waive exact connection cleanup"
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_post_commit_retained_member_pod_churn_does_not_block_cleanup_or_publication() {
    let api = KvClusterApi::new();
    let state = api.removal_state();
    let previous = create_healthy_set(&api, &state, "remove-postcommit-retained-churn", 3).await;
    reconcile_set(
        &make_set("remove-postcommit-retained-churn", 2, Some(previous)),
        &api,
        &state,
    )
    .await
    .unwrap();
    let committed = advance_remove_until_operation_phase(
        &api,
        &state,
        "remove-postcommit-retained-churn",
        2,
        api.last_status().unwrap(),
        DurableOperationPhase::RemoveRecordCommit,
    )
    .await;
    let operation = committed.operation.as_ref().unwrap();
    let retained = operation
        .target_snapshot
        .members
        .iter()
        .find(|member| member.id != operation.old_primary_id)
        .unwrap()
        .clone();
    let retained_name = format!("remove-postcommit-retained-churn-{}", retained.id - 1);
    api.crash_pod(&retained_name);
    api.restart_pod(&retained_name).await;
    let replacement_uid = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .find(|pod| pod.metadata.name.as_deref() == Some(retained_name.as_str()))
        .and_then(|pod| pod.metadata.uid.clone())
        .unwrap();
    assert_ne!(replacement_uid, retained.instance_id);
    let history_start = api.statuses.lock().unwrap().len();

    let completed = drive_operation_to_healthy_with_state(
        &api,
        &state,
        "remove-postcommit-retained-churn",
        2,
        committed,
    )
    .await;
    assert_eq!(
        completed.operation.as_ref().unwrap().phase,
        DurableOperationPhase::Completed
    );
    let published = completed.stable_snapshot.as_ref().unwrap();
    assert_eq!(published.members.len(), 2);
    assert!(
        published.members.iter().any(|member| {
            member.id == retained.id && member.instance_id == retained.instance_id
        })
    );
    assert!(published.members.iter().all(|member| {
        Some(member.id) != completed.operation.as_ref().unwrap().target_replica_id
    }));
    assert!(
        api.statuses.lock().unwrap()[history_start..]
            .iter()
            .all(|status| {
                status
                    .operation
                    .as_ref()
                    .is_none_or(|operation| operation.phase != DurableOperationPhase::Poisoned)
                    && status
                        .conditions
                        .iter()
                        .all(|condition| condition.reason != "IncompatibleOrInvalid")
            }),
        "retained-member pod churn caused fail-closed status churn after commit"
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_target_process_restart_at_demote_and_close_boundaries_is_stale() {
    for (suffix, held) in [
        ("demote", HeldLifecycleEffect::ChangeRoleNone),
        ("close", HeldLifecycleEffect::Close),
    ] {
        let name = format!("remove-target-restart-{suffix}");
        let api = KvClusterApi::new();
        let state = api.removal_state();
        let previous = create_healthy_set(&api, &state, &name, 3).await;
        let target_name = format!("{name}-2");
        let target_uid = api
            .pods
            .lock()
            .unwrap()
            .iter()
            .find(|pod| pod.metadata.name.as_deref() == Some(target_name.as_str()))
            .unwrap()
            .metadata
            .uid
            .clone()
            .unwrap();
        let lifecycle = api.lifecycle_control(&target_name);
        lifecycle.hold(held);
        reconcile_set(&make_set(&name, 2, Some(previous)), &api, &state)
            .await
            .unwrap();
        let mut status = api.last_status().unwrap();
        for _ in 0..200 {
            reconcile_set(&make_set(&name, 2, Some(status.clone())), &api, &state)
                .await
                .unwrap();
            status = api.last_status().unwrap();
            if lifecycle.has_entered(held) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        lifecycle.wait_until_entered(held).await;
        api.crash_pod(&target_name);
        api.restart_process_same_pod_uid(&target_name).await;
        assert_eq!(
            api.pods
                .lock()
                .unwrap()
                .iter()
                .find(|pod| pod.metadata.name.as_deref() == Some(target_name.as_str()))
                .unwrap()
                .metadata
                .uid
                .as_deref(),
            Some(target_uid.as_str())
        );
        let deadline = status
            .operation
            .as_ref()
            .unwrap()
            .remove_intent
            .as_ref()
            .unwrap()
            .overall_deadline_unix_seconds;
        api.removal_clock.set(deadline);
        let completed = drive_operation_to_healthy_with_state(&api, &state, &name, 2, status).await;
        assert_eq!(
            completed
                .operation
                .as_ref()
                .unwrap()
                .remove_intent
                .as_ref()
                .unwrap()
                .last_observed_result,
            Some(RemoveReplicaTerminalResultStatus::CommittedDegraded)
        );
        assert!(
            api.pods
                .lock()
                .unwrap()
                .iter()
                .all(|pod| pod.metadata.uid.as_deref() != Some(target_uid.as_str()))
        );
    }
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_remove_commit_and_publication_conflicts_refetch_without_duplicate_mutation() {
    let api = KvClusterApi::new();
    let state = api.removal_state();
    let previous = create_healthy_set(&api, &state, "remove-status-conflict", 3).await;
    let previous_snapshot = previous.stable_snapshot.clone();
    reconcile_set(
        &make_set("remove-status-conflict", 2, Some(previous)),
        &api,
        &state,
    )
    .await
    .unwrap();
    let mut observed_terminal = api.last_status().unwrap();
    for _ in 0..200 {
        reconcile_set(
            &make_set("remove-status-conflict", 2, Some(observed_terminal.clone())),
            &api,
            &state,
        )
        .await
        .unwrap();
        observed_terminal = api.last_status().unwrap();
        if observed_terminal
            .operation
            .as_ref()
            .is_some_and(|operation| {
                operation.remove_commit_evidence.is_none()
                    && operation
                        .remove_intent
                        .as_ref()
                        .is_some_and(|intent| intent.last_observed_result.is_some())
            })
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        observed_terminal
            .operation
            .as_ref()
            .unwrap()
            .remove_commit_evidence
            .is_none()
    );
    api.fail_next_status_conflict();
    api.reset_operations();
    let error = reconcile_error(
        reconcile_set(
            &make_set("remove-status-conflict", 2, Some(observed_terminal.clone())),
            &api,
            &state,
        )
        .await,
    );
    assert_eq!(error, "resource version conflict");
    assert_eq!(api.last_status().unwrap(), observed_terminal);
    assert!(
        !api.operations()
            .contains(&ControlOperation::RemoveReplicaIntent)
    );

    reconcile_set(
        &make_set("remove-status-conflict", 2, Some(observed_terminal)),
        &api,
        &state,
    )
    .await
    .unwrap();
    let committed = api.last_status().unwrap();
    assert_eq!(
        committed.operation.as_ref().unwrap().phase,
        DurableOperationPhase::RemoveRecordCommit
    );
    assert_eq!(committed.stable_snapshot, previous_snapshot);

    let publishing = advance_remove_until_operation_phase(
        &api,
        &state,
        "remove-status-conflict",
        2,
        committed,
        DurableOperationPhase::RemovePublishTopology,
    )
    .await;
    api.fail_next_status_conflict();
    api.reset_operations();
    let error = reconcile_error(
        reconcile_set(
            &make_set("remove-status-conflict", 2, Some(publishing.clone())),
            &api,
            &state,
        )
        .await,
    );
    assert_eq!(error, "resource version conflict");
    assert_eq!(api.last_status().unwrap(), publishing);
    assert_eq!(publishing.stable_snapshot, previous_snapshot);
    assert!(
        !api.operations()
            .contains(&ControlOperation::RemoveReplicaIntent)
    );

    reconcile_set(
        &make_set("remove-status-conflict", 2, Some(publishing)),
        &api,
        &state,
    )
    .await
    .unwrap();
    let finalizing = api.last_status().unwrap();
    assert_eq!(
        finalizing.operation.as_ref().unwrap().phase,
        DurableOperationPhase::RemoveFinalize
    );
    assert_eq!(
        finalizing.stable_snapshot.as_ref().unwrap().members.len(),
        2
    );
    let completed = drive_operation_to_healthy_with_state(
        &api,
        &state,
        "remove-status-conflict",
        2,
        finalizing,
    )
    .await;
    assert_stable_snapshot(&api, &completed, 2);
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_already_absent_target_cleanup_uses_only_frozen_historical_uid() {
    let absent_api = KvClusterApi::new();
    let absent_state = absent_api.removal_state();
    let absent_previous =
        create_healthy_set(&absent_api, &absent_state, "remove-already-absent", 3).await;
    let target_name = "remove-already-absent-2";
    let frozen_uid = absent_api
        .pods
        .lock()
        .unwrap()
        .iter()
        .find(|pod| pod.metadata.name.as_deref() == Some(target_name))
        .unwrap()
        .metadata
        .uid
        .clone()
        .unwrap();
    absent_api
        .delete_pod("default", target_name, &frozen_uid)
        .await
        .unwrap();
    reconcile_set(
        &make_set("remove-already-absent", 3, Some(absent_previous)),
        &absent_api,
        &absent_state,
    )
    .await
    .unwrap();
    let started = absent_api.last_status().unwrap();
    assert_eq!(
        started
            .operation
            .as_ref()
            .unwrap()
            .target_pod_uid
            .as_deref(),
        Some(frozen_uid.as_str())
    );
    let absent_completed = drive_operation_to_healthy_with_state(
        &absent_api,
        &absent_state,
        "remove-already-absent",
        3,
        started,
    )
    .await;
    assert_stable_snapshot(&absent_api, &absent_completed, 2);
    assert!(
        absent_api
            .pods
            .lock()
            .unwrap()
            .iter()
            .all(|pod| pod.metadata.uid.as_deref() != Some(frozen_uid.as_str()))
    );
}

fn assert_no_removal(api: &KvClusterApi) {
    assert!(
        !api.operations().iter().any(|operation| {
            matches!(
                operation,
                ControlOperation::RemoveReplicaIntent | ControlOperation::RemoveReplica
            )
        }),
        "another active topology operation dispatched removal: {:?}",
        api.operations()
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_active_create_never_dispatches_removal() {
    let create_api = KvClusterApi::new();
    let create_state = create_api.removal_state();
    reconcile_set(
        &make_set("remove-blocked-create", 3, None),
        &create_api,
        &create_state,
    )
    .await
    .unwrap();
    create_api.mark_all_pods_ready();
    reconcile_set(
        &make_set(
            "remove-blocked-create",
            3,
            Some(KubericSetStatus {
                phase: Phase::Creating,
                ..Default::default()
            }),
        ),
        &create_api,
        &create_state,
    )
    .await
    .unwrap();
    let creating = create_api.last_status().unwrap();
    assert_eq!(
        creating.operation.as_ref().unwrap().kind,
        DurableOperationKind::CreatePartition
    );
    create_api.reset_operations();
    reconcile_set(
        &make_set("remove-blocked-create", 1, Some(creating)),
        &create_api,
        &create_state,
    )
    .await
    .unwrap();
    assert_no_removal(&create_api);
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_active_add_never_dispatches_removal() {
    let add_api = KvClusterApi::new();
    let add_state = add_api.removal_state();
    let add_previous = create_healthy_set(&add_api, &add_state, "remove-blocked-add", 1).await;
    reconcile_set(
        &make_set("remove-blocked-add", 2, Some(add_previous.clone())),
        &add_api,
        &add_state,
    )
    .await
    .unwrap();
    add_api.mark_all_pods_ready();
    reconcile_set(
        &make_set("remove-blocked-add", 2, Some(add_previous)),
        &add_api,
        &add_state,
    )
    .await
    .unwrap();
    let adding = add_api.last_status().unwrap();
    assert_eq!(
        adding.operation.as_ref().unwrap().kind,
        DurableOperationKind::AddReplica
    );
    add_api.reset_operations();
    reconcile_set(
        &make_set("remove-blocked-add", 1, Some(adding)),
        &add_api,
        &add_state,
    )
    .await
    .unwrap();
    assert_no_removal(&add_api);
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_active_switchover_never_dispatches_removal() {
    let switch_api = KvClusterApi::new();
    let switch_state = switch_api.removal_state();
    let mut switch_previous =
        create_healthy_set(&switch_api, &switch_state, "remove-blocked-switch", 3).await;
    let current = switch_previous.current_primary.clone().unwrap();
    switch_previous.target_primary = switch_api
        .pods
        .lock()
        .unwrap()
        .iter()
        .map(|pod| pod.metadata.name.clone().unwrap())
        .find(|name| name != &current);
    reconcile_set(
        &make_set("remove-blocked-switch", 3, Some(switch_previous)),
        &switch_api,
        &switch_state,
    )
    .await
    .unwrap();
    let switching = switch_api.last_status().unwrap();
    assert_eq!(
        switching.operation.as_ref().unwrap().kind,
        DurableOperationKind::Switchover
    );
    switch_api.reset_operations();
    reconcile_set(
        &make_set("remove-blocked-switch", 1, Some(switching)),
        &switch_api,
        &switch_state,
    )
    .await
    .unwrap();
    assert_no_removal(&switch_api);
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_active_failover_never_dispatches_removal() {
    let failover_api = KvClusterApi::new();
    let failover_state = failover_api.removal_state();
    let failover_previous =
        create_healthy_set(&failover_api, &failover_state, "remove-blocked-failover", 3).await;
    failover_api.crash_pod(failover_previous.current_primary.as_deref().unwrap());
    reconcile_set(
        &make_set("remove-blocked-failover", 3, Some(failover_previous)),
        &failover_api,
        &failover_state,
    )
    .await
    .unwrap();
    let failing = failover_api.last_status().unwrap();
    assert_eq!(
        failing.operation.as_ref().unwrap().kind,
        DurableOperationKind::Failover
    );
    failover_api.reset_operations();
    reconcile_set(
        &make_set("remove-blocked-failover", 1, Some(failing)),
        &failover_api,
        &failover_state,
    )
    .await
    .unwrap();
    assert_no_removal(&failover_api);
}

/// Reconciler test: Healthy phase detects spec.replicas > actual → scale-up.
#[test_log::test(tokio::test)]
#[serial]
async fn test_reconciler_scale_up() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();

    let status = create_healthy_set(&api, &state, "myapp", 1).await;
    let primary_name = status.current_primary.clone().unwrap();

    // Write data on primary
    let client_addr = api.client_address(&primary_name).unwrap();
    let mut kv = connect_kv(&client_addr).await;
    kv.put(proto::PutRequest {
        key: "before-scale".to_string(),
        value: "original".to_string(),
    })
    .await
    .unwrap();

    // Scale up: change spec to 3 replicas
    // First reconcile creates new pods
    let set = make_set("myapp", 3, Some(status.clone()));
    reconcile_set(&set, &api, &state).await.unwrap();
    assert_eq!(api.pods.lock().unwrap().len(), 3);

    // PVCs should also scale up
    assert_eq!(api.pvcs.lock().unwrap().len(), 3);

    // Mark new pods ready
    api.mark_all_pods_ready();

    // Subsequent reconciles durably add one replica at a time.
    let status = api.last_status().unwrap();
    let status = drive_add_replica(&api, &state, "myapp", 3, status).await;
    assert_stable_snapshot(&api, &status, 3);

    // Driver should have 3 replicas
    {
        let drivers = state.drivers.lock().await;
        let driver = drivers.get("default/myapp").unwrap();
        assert_eq!(driver.replica_ids().len(), 3);
    }

    // Primary still works
    let resp = kv
        .get(proto::GetRequest {
            key: "before-scale".to_string(),
        })
        .await
        .unwrap();
    assert!(resp.get_ref().found);
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_scale_up_replays_writes_buffered_during_copy() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let status = create_healthy_set(&api, &state, "buffered-scale", 1).await;
    let primary_name = status.current_primary.clone().unwrap();
    let mut client = connect_kv(&api.client_address(&primary_name).unwrap()).await;

    for index in 0..500 {
        client
            .put(proto::PutRequest {
                key: format!("before-{index}"),
                value: format!("value-{index}"),
            })
            .await
            .unwrap();
    }

    reconcile_set(&make_set("buffered-scale", 2, Some(status)), &api, &state)
        .await
        .unwrap();
    api.mark_all_pods_ready();
    let pending = advance_until_pending_action(
        &api,
        "buffered-scale",
        2,
        api.last_status().unwrap(),
        DurableActionKind::AddReplicaIntent,
    )
    .await;

    let dispatch_set = make_set("buffered-scale", 2, Some(pending));
    let restarted_state = ReconcilerState::default();
    let dispatch = reconcile_set(&dispatch_set, &api, &restarted_state);
    let writes = async {
        for index in 0..200 {
            client
                .put(proto::PutRequest {
                    key: format!("during-{index}"),
                    value: format!("value-{index}"),
                })
                .await
                .unwrap();
        }
    };
    let (dispatch_result, ()) = tokio::join!(dispatch, writes);
    dispatch_result.unwrap();

    let completed = drive_add_replica(
        &api,
        &state,
        "buffered-scale",
        2,
        api.last_status().unwrap(),
    )
    .await;
    assert_stable_snapshot(&api, &completed, 2);

    let secondary_name = completed
        .stable_snapshot
        .as_ref()
        .unwrap()
        .members
        .iter()
        .find(|member| member.id != completed.stable_snapshot.as_ref().unwrap().primary_id)
        .map(|member| format!("buffered-scale-{}", member.id - 1))
        .unwrap();
    let secondary_state = api
        .live_pods
        .lock()
        .unwrap()
        .get(&secondary_name)
        .unwrap()
        .state
        .clone();
    let secondary_state = secondary_state.read().await;
    assert_eq!(secondary_state.data.len(), 700);
    for index in 0..200 {
        assert_eq!(
            secondary_state.data.get(&format!("during-{index}")),
            Some(&format!("value-{index}"))
        );
    }
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_simultaneous_secondary_loss_bounds_new_and_inflight_writes() {
    async fn setup(
        name: &str,
    ) -> (
        KvClusterApi,
        ReconcilerState,
        String,
        Vec<Box<dyn ReplicaHandle>>,
    ) {
        let api = KvClusterApi::new();
        let state = ReconcilerState::default();
        let status = create_healthy_set(&api, &state, name, 3).await;
        let primary = status.current_primary.unwrap();
        let secondary_pods = api
            .pods
            .lock()
            .unwrap()
            .iter()
            .filter_map(|pod| {
                (pod.metadata.name.as_deref() != Some(primary.as_str())).then_some(pod.clone())
            })
            .collect::<Vec<_>>();
        let spec = make_set(name, 3, None).spec;
        let mut secondaries = Vec::new();
        for pod in secondary_pods {
            let id = pod
                .metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get("kuberic.io/pod-index"))
                .unwrap()
                .parse::<i64>()
                .unwrap()
                + 1;
            secondaries.push(api.create_replica_handle(id, &pod, &spec).await.unwrap());
        }
        (api, state, primary, secondaries)
    }

    let (api, _state, primary, secondaries) = setup("bounded-new-write").await;
    let mut client = connect_kv(&api.client_address(&primary).unwrap()).await;
    for (index, secondary) in secondaries.iter().enumerate() {
        execute_with_fresh_fences(
            secondary.as_ref(),
            &format!("bounded-new-write:close:{index}"),
            DurableReplicaAction::Close,
        )
        .await
        .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let result = tokio::time::timeout(
        Duration::from_secs(8),
        client.put(proto::PutRequest {
            key: "after-loss".to_string(),
            value: "bounded".to_string(),
        }),
    )
    .await
    .expect("write must not hang after quorum loss");
    assert!(
        result.is_err(),
        "write unexpectedly committed without quorum"
    );

    let (api, _state, primary, secondaries) = setup("bounded-inflight-write").await;
    let mut client = connect_kv(&api.client_address(&primary).unwrap()).await;
    let write = tokio::spawn(async move {
        client
            .put(proto::PutRequest {
                key: "racing-loss".to_string(),
                value: "bounded".to_string(),
            })
            .await
    });
    tokio::task::yield_now().await;
    for (index, secondary) in secondaries.iter().enumerate() {
        execute_with_fresh_fences(
            secondary.as_ref(),
            &format!("bounded-inflight-write:close:{index}"),
            DurableReplicaAction::Close,
        )
        .await
        .unwrap();
    }
    let _result = tokio::time::timeout(Duration::from_secs(8), write)
        .await
        .expect("in-flight write must complete or fail within the quorum bound")
        .unwrap();
}

/// Reconciler test: Healthy phase detects spec.replicas < actual → scale-down.
#[test_log::test(tokio::test)]
#[serial]
async fn test_reconciler_scale_down() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();

    let status = create_healthy_set(&api, &state, "myapp", 3).await;

    // Write data
    let primary_name = status.current_primary.clone().unwrap();
    let client_addr = api.client_address(&primary_name).unwrap();
    let mut kv = connect_kv(&client_addr).await;
    kv.put(proto::PutRequest {
        key: "before-scale-down".to_string(),
        value: "yes".to_string(),
    })
    .await
    .unwrap();

    // Scale down: change spec to 2 replicas
    let mut status = status;
    for _ in 0..10 {
        reconcile_set(&make_set("myapp", 2, Some(status.clone())), &api, &state)
            .await
            .unwrap();
        status = api.last_status().unwrap();
        if status.phase == Phase::RemovingReplica {
            break;
        }
    }
    assert_eq!(status.phase, Phase::RemovingReplica);
    let status = drive_operation_to_healthy(&api, "myapp", 2, status).await;
    assert_stable_snapshot(&api, &status, 2);
    reconcile_set(&make_set("myapp", 2, Some(status.clone())), &api, &state)
        .await
        .unwrap();

    // PVCs retained after scale-down (pod deleted, PVC kept)
    assert_eq!(api.pvcs.lock().unwrap().len(), 3);

    // Driver should have fewer replicas
    {
        let drivers = state.drivers.lock().await;
        let driver = drivers.get("default/myapp").unwrap();
        assert_eq!(driver.replica_ids().len(), 2);
    }

    // Primary still works
    let resp = kv
        .put(proto::PutRequest {
            key: "after-scale-down".to_string(),
            value: "still-works".to_string(),
        })
        .await
        .unwrap();
    assert!(resp.get_ref().lsn > 0);
}

/// Reconciler test: double failover — after the first failover, the new
/// primary also fails. The reconciler handles a second failover cycle
/// (FailingOver → Healthy again) with the last surviving replica.
/// Exercises: reconciler retry loop, driver failover, A1 best-effort epoch.
#[test_log::test(tokio::test)]
#[serial]
async fn test_reconciler_double_failover() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();

    let status = create_healthy_set(&api, &state, "myapp", 3).await;
    let first_primary = status.current_primary.clone().unwrap();

    // Write data
    let addr = api.client_address(&first_primary).unwrap();
    let mut kv = connect_kv(&addr).await;
    kv.put(proto::PutRequest {
        key: "epoch-1".into(),
        value: "data".into(),
    })
    .await
    .unwrap();

    // --- First failover: primary fails ---
    api.crash_pod(&first_primary);

    let set = make_set("myapp", 3, Some(status));
    reconcile_set(&set, &api, &state).await.unwrap();
    let status = api.last_status().unwrap();
    assert_eq!(status.phase, Phase::FailingOver);

    let status = drive_operation_to_healthy(&api, "myapp", 3, status).await;

    let second_primary = status.current_primary.clone().unwrap();
    assert_ne!(second_primary, first_primary);

    // Write on second primary
    let addr2 = api.client_address(&second_primary).unwrap();
    let mut kv2 = connect_kv(&addr2).await;
    kv2.put(proto::PutRequest {
        key: "epoch-2".into(),
        value: "survived".into(),
    })
    .await
    .unwrap();

    // --- Second failover: new primary also fails ---
    api.crash_pod(&second_primary);

    // Reconcile Healthy: health check detects both dead replicas,
    // removes stale secondaries, then transitions to FailingOver.
    let set = make_set("myapp", 3, Some(status));
    reconcile_set(&set, &api, &state).await.unwrap();
    let status = api.last_status().unwrap();
    assert_eq!(status.phase, Phase::FailingOver);

    let status = drive_operation_to_healthy(&api, "myapp", 3, status).await;

    let third_primary = status.current_primary.clone().unwrap();
    assert_ne!(third_primary, first_primary);
    assert_ne!(third_primary, second_primary);

    // Third primary should have data from both epochs.
    // Retry Get — the client gRPC server may not be ready immediately after promotion.
    let addr3 = api.client_address(&third_primary).unwrap();
    let mut kv3 = connect_kv(&addr3).await;

    let resp = retry_get(&mut kv3, "epoch-1").await;
    assert!(resp.get_ref().found, "data from first epoch should survive");

    let resp = retry_get(&mut kv3, "epoch-2").await;
    assert!(
        resp.get_ref().found,
        "data from second epoch should survive"
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_failover_recovers_lost_replies_and_restarts() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let status = create_healthy_set(&api, &state, "durable-failover", 3).await;
    let old_primary = status.current_primary.clone().unwrap();
    api.crash_pod(&old_primary);

    reconcile_set(&make_set("durable-failover", 3, Some(status)), &api, &state)
        .await
        .unwrap();
    let started = api.last_status().unwrap();
    assert_eq!(started.phase, Phase::FailingOver);
    assert_eq!(
        started.operation.as_ref().unwrap().kind,
        DurableOperationKind::Failover
    );

    let expected = [
        DurableActionKind::FailoverRecordStartingConfiguration,
        DurableActionKind::FailoverUpdateCandidateEpoch,
        DurableActionKind::FailoverPromoteCandidate,
        DurableActionKind::FailoverUpdateSecondaryEpoch,
        DurableActionKind::FailoverCatchUpConfiguration,
        DurableActionKind::FailoverWaitForCatchUpQuorum,
        DurableActionKind::FailoverCurrentConfiguration,
        DurableActionKind::FailoverRecordElectionConfiguration,
    ];
    let (completed, injected) =
        drive_operation_with_lost_replies(&api, "durable-failover", 3, started, &expected).await;
    assert_eq!(completed.phase, Phase::Healthy);
    assert!(completed.operation.is_none());
    assert_eq!(completed.epoch.configuration_number, 2);
    for kind in expected {
        assert!(injected.contains(&kind), "missing lost reply for {kind:?}");
    }
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_failover_negotiates_data_loss_after_accounted_quorum_loss() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let status = create_healthy_set(&api, &state, "data-loss-failover", 3).await;
    let old_primary = status.current_primary.clone().unwrap();
    let replaced_secondary = status
        .members
        .iter()
        .find(|member| member.name != old_primary)
        .unwrap()
        .name
        .clone();
    api.restart_pod(&replaced_secondary).await;
    api.crash_pod(&old_primary);

    reconcile_set(
        &make_set("data-loss-failover", 3, Some(status)),
        &api,
        &state,
    )
    .await
    .unwrap();
    let started = api.last_status().unwrap();
    let completed = drive_operation_to_healthy(&api, "data-loss-failover", 3, started).await;

    assert_eq!(completed.epoch.data_loss_number, 1);
    assert_eq!(completed.epoch.configuration_number, 2);
    assert_eq!(completed.stable_snapshot.as_ref().unwrap().members.len(), 1);
    assert!(api.operations().contains(&ControlOperation::OnDataLoss));
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_failover_data_loss_state_changed_and_failure() {
    let api = KvClusterApi::new_with_data_loss_behavior(service::DataLossBehavior::StateChanged);
    let state = ReconcilerState::default();
    let status = create_healthy_set(&api, &state, "data-loss-changed", 3).await;
    let old_primary = status.current_primary.clone().unwrap();
    let replaced_secondary = status
        .members
        .iter()
        .find(|member| member.name != old_primary)
        .unwrap()
        .name
        .clone();
    api.restart_pod(&replaced_secondary).await;
    api.crash_pod(&old_primary);
    reconcile_set(
        &make_set("data-loss-changed", 3, Some(status)),
        &api,
        &state,
    )
    .await
    .unwrap();
    let completed =
        drive_operation_to_healthy(&api, "data-loss-changed", 3, api.last_status().unwrap()).await;
    assert_eq!(completed.epoch.data_loss_number, 1);
    assert_eq!(completed.stable_snapshot.as_ref().unwrap().members.len(), 1);
    assert!(
        completed.stable_snapshot.as_ref().unwrap().members[0]
            .election_metadata
            .is_some()
    );

    let failed_api = KvClusterApi::new_with_data_loss_behavior(service::DataLossBehavior::Fail(
        "reject data loss".to_string(),
    ));
    let failed_state = ReconcilerState::default();
    let failed_status = create_healthy_set(&failed_api, &failed_state, "data-loss-failed", 3).await;
    let failed_primary = failed_status.current_primary.clone().unwrap();
    let failed_secondary = failed_status
        .members
        .iter()
        .find(|member| member.name != failed_primary)
        .unwrap()
        .name
        .clone();
    failed_api.restart_pod(&failed_secondary).await;
    failed_api.crash_pod(&failed_primary);
    reconcile_set(
        &make_set("data-loss-failed", 3, Some(failed_status)),
        &failed_api,
        &failed_state,
    )
    .await
    .unwrap();
    let mut status = failed_api.last_status().unwrap();
    for _ in 0..100 {
        if status
            .operation
            .as_ref()
            .is_some_and(|operation| operation.phase == DurableOperationPhase::Poisoned)
        {
            assert_eq!(status.epoch.data_loss_number, 0);
            assert!(
                status
                    .operation
                    .as_ref()
                    .unwrap()
                    .last_error
                    .as_deref()
                    .unwrap_or_default()
                    .contains("reject data loss")
            );
            return;
        }

        reconcile_set(
            &make_set("data-loss-failed", 3, Some(status.clone())),
            &failed_api,
            &ReconcilerState::default(),
        )
        .await
        .unwrap();
        status = failed_api.last_status().unwrap();
    }
    panic!("failed data-loss callback did not poison failover");
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_slow_data_loss_callback_does_not_poison_failover() {
    let api = KvClusterApi::new_with_data_loss_behavior(service::DataLossBehavior::Delay {
        duration: Duration::from_secs(12),
        state_changed: false,
    });
    let state = ReconcilerState::default();
    let status = create_healthy_set(&api, &state, "slow-data-loss", 3).await;
    let old_primary = status.current_primary.clone().unwrap();
    let replaced_secondary = status
        .members
        .iter()
        .find(|member| member.name != old_primary)
        .unwrap()
        .name
        .clone();
    api.restart_pod(&replaced_secondary).await;
    api.crash_pod(&old_primary);
    reconcile_set(&make_set("slow-data-loss", 3, Some(status)), &api, &state)
        .await
        .unwrap();

    let mut status = api.last_status().unwrap();
    for _ in 0..800 {
        reconcile_set(
            &make_set("slow-data-loss", 3, Some(status.clone())),
            &api,
            &ReconcilerState::default(),
        )
        .await
        .unwrap();
        status = api.last_status().unwrap();
        assert!(
            status
                .operation
                .as_ref()
                .is_none_or(|operation| operation.phase != DurableOperationPhase::Poisoned),
            "slow in-progress data-loss callback poisoned failover: {status:?}"
        );
        if status.phase == Phase::Healthy {
            assert_eq!(status.epoch.data_loss_number, 1);
            assert!(api.operations().contains(&ControlOperation::OnDataLoss));
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("slow data-loss callback did not complete durable failover: {status:?}");
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_failover_observes_lost_data_loss_reply() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let status = create_healthy_set(&api, &state, "data-loss-reply", 3).await;
    let primary = status.current_primary.clone().unwrap();
    let replaced = status
        .members
        .iter()
        .find(|member| member.name != primary)
        .unwrap()
        .name
        .clone();
    api.restart_pod(&replaced).await;
    api.crash_pod(&primary);
    reconcile_set(&make_set("data-loss-reply", 3, Some(status)), &api, &state)
        .await
        .unwrap();
    let (completed, injected) = drive_operation_with_lost_replies(
        &api,
        "data-loss-reply",
        3,
        api.last_status().unwrap(),
        &[DurableActionKind::FailoverOnDataLoss],
    )
    .await;
    assert_eq!(completed.phase, Phase::Healthy);
    assert_eq!(completed.epoch.data_loss_number, 1);
    assert_eq!(injected, vec![DurableActionKind::FailoverOnDataLoss]);
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_failover_waits_for_unavailable_possible_best_replica() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let status = create_healthy_set(&api, &state, "quorum-wait", 3).await;
    let old_primary = status.current_primary.clone().unwrap();
    let unavailable = status
        .members
        .iter()
        .filter(|member| member.name != old_primary)
        .map(|member| member.name.clone())
        .collect::<Vec<_>>();
    api.crash_pod(&old_primary);
    for pod in unavailable {
        api.crash_pod(&pod);
    }

    reconcile_set(&make_set("quorum-wait", 3, Some(status)), &api, &state)
        .await
        .unwrap();
    let mut status = api.last_status().unwrap();
    for _ in 0..40 {
        let phase = status.operation.as_ref().unwrap().phase;
        if matches!(
            phase,
            DurableOperationPhase::FailoverWaitForBestCandidate
                | DurableOperationPhase::FailoverWaitForReadQuorum
        ) {
            assert_eq!(status.epoch.data_loss_number, 0);
            assert_eq!(status.phase, Phase::FailingOver);
            let before = status
                .operation
                .as_ref()
                .unwrap()
                .failover
                .as_ref()
                .unwrap()
                .next_unavailable_index;
            reconcile_set(
                &make_set("quorum-wait", 3, Some(status.clone())),
                &api,
                &ReconcilerState::default(),
            )
            .await
            .unwrap();
            let after_status = api.last_status().unwrap();
            let failover = after_status
                .operation
                .as_ref()
                .unwrap()
                .failover
                .as_ref()
                .unwrap();
            assert_eq!(failover.unavailable_replicas.len(), 2);
            assert_ne!(failover.next_unavailable_index, before);
            return;
        }

        reconcile_set(
            &make_set("quorum-wait", 3, Some(status.clone())),
            &api,
            &ReconcilerState::default(),
        )
        .await
        .unwrap();
        status = api.last_status().unwrap();
    }
    panic!("failover did not enter explicit quorum/best-candidate wait: {status:?}");
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_failover_delay_requires_continuous_failure_and_rejects_negative() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let status = create_healthy_set(&api, &state, "failover-delay", 3).await;
    let primary = status.current_primary.clone().unwrap();
    api.mark_pod_not_ready(&primary);

    let mut delayed = make_set("failover-delay", 3, Some(status));
    delayed.spec.failover_delay = 5;
    reconcile_set(&delayed, &api, &state).await.unwrap();
    let waiting = api.last_status().unwrap();
    assert_eq!(waiting.phase, Phase::Healthy);
    assert!(waiting.primary_failing_since.is_some());
    assert!(
        waiting
            .operation
            .as_ref()
            .is_none_or(|operation| operation.kind != DurableOperationKind::Failover)
    );

    api.mark_all_pods_ready();
    let recovered = make_set("failover-delay", 3, Some(waiting));
    reconcile_set(&recovered, &api, &state).await.unwrap();
    let recovered = api.last_status().unwrap();
    assert_eq!(recovered.phase, Phase::Healthy);
    assert!(recovered.primary_failing_since.is_none());

    api.mark_pod_not_ready(&primary);
    let mut invalid = make_set("failover-delay", 3, Some(recovered));
    invalid.spec.failover_delay = -1;
    let error = match reconcile_set(&invalid, &api, &state).await {
        Ok(_) => panic!("negative failover delay unexpectedly reconciled"),
        Err(error) => error,
    };
    assert!(error.contains("non-negative"));
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_failover_final_status_lost_reply_reloads_applied_snapshot() {
    let api = KvClusterApi::new();
    let initial_state = ReconcilerState::default();
    let status = create_healthy_set(&api, &initial_state, "final-status-loss", 3).await;
    let primary = status.current_primary.clone().unwrap();
    api.crash_pod(&primary);
    reconcile_set(
        &make_set("final-status-loss", 3, Some(status)),
        &api,
        &initial_state,
    )
    .await
    .unwrap();
    let mut status = api.last_status().unwrap();
    for _ in 0..120 {
        let ready_to_complete = status.operation.as_ref().is_some_and(|operation| {
            operation.phase == DurableOperationPhase::FailoverAttest
                && operation.failover.as_ref().is_some_and(|failover| {
                    failover.final_attestations.len() == operation.target_snapshot.members.len()
                })
        });
        if ready_to_complete {
            break;
        }

        reconcile_set(
            &make_set("final-status-loss", 3, Some(status.clone())),
            &api,
            &ReconcilerState::default(),
        )
        .await
        .unwrap();
        status = api.last_status().unwrap();
    }
    assert_eq!(
        status.operation.as_ref().unwrap().phase,
        DurableOperationPhase::FailoverAttest
    );
    *api.fail_after_next_status_patch.lock().unwrap() = true;
    let completion_state = ReconcilerState::default();
    assert!(
        reconcile_set(
            &make_set("final-status-loss", 3, Some(status)),
            &api,
            &completion_state,
        )
        .await
        .is_err()
    );
    let applied = api.last_status().unwrap();
    assert_eq!(applied.phase, Phase::Healthy);
    assert!(applied.operation.is_none());
    let count = api.statuses.lock().unwrap().len();
    reconcile_set(
        &make_set("final-status-loss", 3, Some(applied.clone())),
        &api,
        &completion_state,
    )
    .await
    .unwrap();
    assert_eq!(api.statuses.lock().unwrap().len(), count);
}

async fn assert_active_failover_rejects_control_status(name: &str, injected: InjectedStatusError) {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let status = create_healthy_set(&api, &state, name, 3).await;
    let old_primary = status.current_primary.clone().unwrap();
    api.crash_pod(&old_primary);

    reconcile_set(&make_set(name, 3, Some(status)), &api, &state)
        .await
        .unwrap();
    let started = api.last_status().unwrap();
    reconcile_set(&make_set(name, 3, Some(started)), &api, &state)
        .await
        .unwrap();
    let before = api.last_status().unwrap();
    let status_count = api.statuses.lock().unwrap().len();

    api.reset_operations();
    api.fail_next_status(injected);
    let error = reconcile_set(&make_set(name, 3, Some(before.clone())), &api, &state)
        .await
        .err()
        .expect("incompatible control status must fail closed");

    assert!(
        error.contains("unsupported or malformed control status"),
        "unexpected fail-closed error: {error}"
    );
    assert_eq!(api.statuses.lock().unwrap().len(), status_count);
    assert_eq!(api.last_status().unwrap(), before);
    assert!(
        api.operations()
            .iter()
            .all(|operation| *operation == ControlOperation::GetStatus)
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_active_failover_rejects_unsupported_or_malformed_agent_status() {
    assert_active_failover_rejects_control_status(
        "failover-unsupported-agent",
        InjectedStatusError::UnsupportedProtocol,
    )
    .await;
    assert_active_failover_rejects_control_status(
        "failover-malformed-agent",
        InjectedStatusError::MalformedAgentStatus,
    )
    .await;
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_durable_failover_incarnation_drift_is_phase_fenced() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let status = create_healthy_set(&api, &state, "failover-drift", 3).await;
    let primary = status.current_primary.clone().unwrap();
    api.crash_pod(&primary);
    reconcile_set(&make_set("failover-drift", 3, Some(status)), &api, &state)
        .await
        .unwrap();
    let confirmed = advance_add_until_phase(
        &api,
        &ReconcilerState::default(),
        "failover-drift",
        3,
        api.last_status().unwrap(),
        DurableOperationPhase::FailoverPersistConfigurationEpoch,
    )
    .await;
    let target_id = confirmed.operation.as_ref().unwrap().target_primary_id;
    let target_name = format!("failover-drift-{}", target_id - 1);
    api.restart_pod(&target_name).await;
    reconcile_set(
        &make_set("failover-drift", 3, Some(confirmed)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        api.last_status().unwrap().operation.unwrap().phase,
        DurableOperationPhase::Poisoned
    );

    let roll_api = KvClusterApi::new();
    let roll_state = ReconcilerState::default();
    let roll_status = create_healthy_set(&roll_api, &roll_state, "failover-roll", 3).await;
    let old_primary = roll_status.current_primary.clone().unwrap();
    roll_api.crash_pod(&old_primary);
    reconcile_set(
        &make_set("failover-roll", 3, Some(roll_status)),
        &roll_api,
        &roll_state,
    )
    .await
    .unwrap();
    let post_commit = advance_add_until_phase(
        &roll_api,
        &ReconcilerState::default(),
        "failover-roll",
        3,
        roll_api.last_status().unwrap(),
        DurableOperationPhase::FailoverDistributeEpoch,
    )
    .await;
    let operation = post_commit.operation.as_ref().unwrap();
    let secondary = operation
        .target_snapshot
        .members
        .iter()
        .find(|member| member.id != operation.target_primary_id)
        .unwrap()
        .id;
    roll_api
        .restart_pod(&format!("failover-roll-{}", secondary - 1))
        .await;
    reconcile_set(
        &make_set("failover-roll", 3, Some(post_commit)),
        &roll_api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    let reduced = roll_api.last_status().unwrap();
    assert_eq!(
        reduced
            .operation
            .as_ref()
            .unwrap()
            .target_snapshot
            .members
            .len(),
        1
    );
    let completed = drive_operation_to_healthy(&roll_api, "failover-roll", 3, reduced).await;
    assert_eq!(completed.stable_snapshot.unwrap().members.len(), 1);
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_stable_metadata_refresh_records_live_configuration() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let status = create_healthy_set(&api, &state, "metadata-refresh", 3).await;
    assert!(status.stable_election_metadata_refresh.is_none());
    let snapshot = status.stable_snapshot.as_ref().unwrap();
    assert!(snapshot.members.iter().all(|member| {
        member
            .election_metadata
            .as_ref()
            .is_some_and(|metadata| metadata.deactivation_epoch == snapshot.epoch)
    }));
    assert!(
        live_replica_statuses(&api, "metadata-refresh", 3)
            .await
            .iter()
            .all(|status| status.election_configuration.is_some())
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_failover_reprobes_reachable_transient_observation() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let status = create_healthy_set(&api, &state, "transient-reprobe", 3).await;
    let primary = status.current_primary.clone().unwrap();
    api.crash_pod(&primary);
    reconcile_set(
        &make_set("transient-reprobe", 3, Some(status)),
        &api,
        &state,
    )
    .await
    .unwrap();
    let mut collecting = advance_add_until_phase(
        &api,
        &ReconcilerState::default(),
        "transient-reprobe",
        3,
        api.last_status().unwrap(),
        DurableOperationPhase::FailoverCollect,
    )
    .await;
    let operation = collecting.operation.as_mut().unwrap();
    let epoch = operation.previous_snapshot.as_ref().unwrap().epoch.clone();
    let instance_2 = operation
        .failover
        .as_ref()
        .unwrap()
        .current_configuration
        .members
        .iter()
        .find(|member| member.id == 2)
        .unwrap()
        .instance_id
        .clone();
    let instance_3 = operation
        .failover
        .as_ref()
        .unwrap()
        .current_configuration
        .members
        .iter()
        .find(|member| member.id == 3)
        .unwrap()
        .instance_id
        .clone();
    operation.failover.as_mut().unwrap().observations = vec![
        ReplicaElectionObservationStatus {
            id: 2,
            instance_id: instance_2,
            epoch: epoch.clone(),
            role: "activeSecondary".to_string(),
            healthy: true,
            current_lsn: 10,
            committed_lsn: 10,
            first_retained_lsn: Some(0),
            deactivation_epoch: Some(epoch.clone()),
            deactivation_catch_up_lsn: Some(10),
            configuration_matches: true,
        },
        ReplicaElectionObservationStatus {
            id: 3,
            instance_id: instance_3,
            epoch: epoch.clone(),
            role: "activeSecondary".to_string(),
            healthy: false,
            current_lsn: 20,
            committed_lsn: 20,
            first_retained_lsn: Some(0),
            deactivation_epoch: Some(epoch),
            deactivation_catch_up_lsn: Some(20),
            configuration_matches: true,
        },
    ];
    operation.phase = DurableOperationPhase::FailoverAssess;
    reconcile_set(
        &make_set("transient-reprobe", 3, Some(collecting)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    let waiting = api.last_status().unwrap();
    assert!(matches!(
        waiting.operation.as_ref().unwrap().phase,
        DurableOperationPhase::FailoverWaitForBestCandidate
            | DurableOperationPhase::FailoverWaitForReadQuorum
    ));

    reconcile_set(
        &make_set("transient-reprobe", 3, Some(waiting)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    let refreshed = api.last_status().unwrap();
    assert_eq!(
        refreshed.operation.as_ref().unwrap().phase,
        DurableOperationPhase::FailoverAssess
    );
    let completed = drive_operation_to_healthy(&api, "transient-reprobe", 3, refreshed).await;
    assert_eq!(completed.phase, Phase::Healthy);
}

/// Verify idempotent creation: reconciling Pending twice doesn't create duplicate PVCs.
#[test_log::test(tokio::test)]
#[serial]
async fn test_reconciler_idempotent_creation() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();

    let set = make_set("idempotent", 3, None);

    // First reconcile: Pending → Creating
    reconcile_set(&set, &api, &state).await.unwrap();
    assert_eq!(api.pods.lock().unwrap().len(), 3);
    assert_eq!(api.pvcs.lock().unwrap().len(), 3);

    // Second reconcile at Pending — should not duplicate
    reconcile_set(&set, &api, &state).await.unwrap();
    assert_eq!(api.pods.lock().unwrap().len(), 3);
    assert_eq!(api.pvcs.lock().unwrap().len(), 3);
    assert_eq!(api.services.lock().unwrap().len(), 3);
}

/// Reconciler test: secondary crash_pod → detected by epoch-based health
/// check → stale handle removed → restart_pod → re-integrated via scale-up.
#[test_log::test(tokio::test)]
#[serial]
async fn test_reconciler_secondary_crash_and_rejoin() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();

    let status = create_healthy_set(&api, &state, "myapp", 3).await;
    let primary_name = status.current_primary.clone().unwrap();

    // Write data on primary
    let client_addr = api.client_address(&primary_name).unwrap();
    let mut kv = connect_kv(&client_addr).await;
    kv.put(proto::PutRequest {
        key: "before-crash".into(),
        value: "important".into(),
    })
    .await
    .unwrap();

    // Pick a secondary to crash (any non-primary pod)
    let secondary_name = {
        let pods = api.pods.lock().unwrap();
        pods.iter()
            .map(|p| p.metadata.name.clone().unwrap())
            .find(|n| n != &primary_name)
            .unwrap()
    };

    // Crash the secondary (high fidelity — abort tasks, gRPC breaks)
    api.crash_pod(&secondary_name);

    // Restart before reconciliation. A ready replacement incarnation that is
    // still required by desired capacity enters durable rebuild; an
    // unreachable old incarnation is covered by the force-removal test.
    api.restart_pod(&secondary_name).await;

    let status = api.last_status().unwrap();
    let rejoined_status = drive_add_replica(&api, &state, "myapp", 3, status).await;
    assert_stable_snapshot(&api, &rejoined_status, 3);
    let replacement_uid = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .find(|pod| pod.metadata.name.as_deref() == Some(secondary_name.as_str()))
        .unwrap()
        .metadata
        .uid
        .clone()
        .unwrap();
    assert!(
        rejoined_status
            .stable_snapshot
            .as_ref()
            .unwrap()
            .members
            .iter()
            .any(|member| member.instance_id == replacement_uid)
    );

    // Driver should be back to 3 replicas
    {
        let drivers = state.drivers.lock().await;
        let driver = drivers.get("default/myapp").unwrap();
        assert_eq!(
            driver.replica_ids().len(),
            3,
            "restarted secondary should be re-added"
        );
    }

    // Primary still works — write new data
    kv.put(proto::PutRequest {
        key: "after-rejoin".into(),
        value: "recovered".into(),
    })
    .await
    .unwrap();

    // Verify data readable on primary
    let resp = kv
        .get(proto::GetRequest {
            key: "before-crash".into(),
        })
        .await
        .unwrap();
    assert!(resp.get_ref().found);
    assert_eq!(resp.get_ref().value, "important");

    // Promote the replacement. Reading the post-rejoin write from it proves
    // the primary replaced the old same-ID data-plane connection.
    let status = api.last_status().unwrap();
    let set = make_set(
        "myapp",
        3,
        Some(KubericSetStatus {
            phase: Phase::Healthy,
            current_primary: Some(primary_name),
            target_primary: Some(secondary_name.clone()),
            ..status
        }),
    );
    reconcile_set(&set, &api, &state).await.unwrap();
    let status = api.last_status().unwrap();
    assert_eq!(status.phase, Phase::Switchover);
    drive_switchover(&api, &state, "myapp", 3, status).await;

    let replacement_addr = api.client_address(&secondary_name).unwrap();
    let mut replacement = connect_kv(&replacement_addr).await;
    let resp = replacement
        .get(proto::GetRequest {
            key: "after-rejoin".into(),
        })
        .await
        .unwrap();
    assert!(resp.get_ref().found);
    assert_eq!(resp.get_ref().value, "recovered");
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_same_pod_process_restart_changes_agent_generation_not_incarnation() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let status = create_healthy_set(&api, &state, "same-pod-restart", 3).await;
    let primary = status.current_primary.clone().unwrap();
    let secondary = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .find(|pod| pod.metadata.name.as_deref() != Some(primary.as_str()))
        .cloned()
        .unwrap();
    let pod_name = secondary.metadata.name.clone().unwrap();
    let pod_uid = secondary.metadata.uid.clone().unwrap();
    let replica_id = secondary
        .metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get("kuberic.io/pod-index"))
        .unwrap()
        .parse::<i64>()
        .unwrap()
        + 1;
    let set = make_set("same-pod-restart", 3, None);

    let before = api
        .create_replica_handle(replica_id, &secondary, &set.spec)
        .await
        .unwrap()
        .get_status()
        .await
        .unwrap();
    let before_generation = before.agent.generation;
    assert_eq!(before.instance_id.as_str(), pod_uid);

    api.crash_pod(&pod_name);
    api.restart_process_same_pod_uid(&pod_name).await;
    let restarted_pod = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .find(|pod| pod.metadata.name.as_deref() == Some(pod_name.as_str()))
        .cloned()
        .unwrap();
    assert_eq!(
        restarted_pod.metadata.uid.as_deref(),
        Some(pod_uid.as_str())
    );

    let after = api
        .create_replica_handle(replica_id, &restarted_pod, &set.spec)
        .await
        .unwrap()
        .get_status()
        .await
        .unwrap();
    let after_agent = after.agent;
    assert_eq!(after.instance_id.as_str(), pod_uid);
    assert_ne!(after_agent.generation, before_generation);
    assert_eq!(after_agent.control_version.value(), 0);
    assert!(after_agent.current_action.is_none());
    assert!(after_agent.retained_terminal_actions.is_empty());

    api.reset_operations();
    reconcile_set(
        &make_set("same-pod-restart", 3, Some(api.last_status().unwrap())),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    let fail_closed = api.last_status().unwrap();
    assert_eq!(fail_closed.phase, Phase::RemovingReplica);
    let operation = fail_closed.operation.unwrap();
    assert_eq!(operation.kind, DurableOperationKind::RemoveReplica);
    assert_eq!(operation.target_replica_id, Some(replica_id));
    assert_eq!(
        operation.target_instance_id.as_deref(),
        Some(pod_uid.as_str())
    );
    assert!(
        api.operations()
            .iter()
            .all(|operation| *operation == ControlOperation::GetStatus),
        "operator restart must persist durable recovery intent before mutation"
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_add_target_same_pod_process_restart_invalidates_build_proof() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let previous = create_healthy_set(&api, &state, "same-pod-add", 1).await;
    reconcile_set(
        &make_set("same-pod-add", 2, Some(previous.clone())),
        &api,
        &state,
    )
    .await
    .unwrap();
    api.mark_all_pods_ready();
    reconcile_set(&make_set("same-pod-add", 2, Some(previous)), &api, &state)
        .await
        .unwrap();
    let pending = advance_until_pending_action(
        &api,
        "same-pod-add",
        2,
        api.last_status().unwrap(),
        DurableActionKind::AddReplicaIntent,
    )
    .await;
    let target_name = pending
        .operation
        .as_ref()
        .unwrap()
        .target_pod_name
        .clone()
        .unwrap();
    let target_uid = pending
        .operation
        .as_ref()
        .unwrap()
        .target_instance_id
        .clone()
        .unwrap();
    let original_generation = pending
        .operation
        .as_ref()
        .unwrap()
        .add_intent
        .as_ref()
        .unwrap()
        .target_agent_generation
        .clone();

    reconcile_set(
        &make_set("same-pod-add", 2, Some(pending)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    api.crash_pod(&target_name);
    api.restart_process_same_pod_uid(&target_name).await;
    assert_eq!(
        api.pods
            .lock()
            .unwrap()
            .iter()
            .find(|pod| pod.metadata.name.as_deref() == Some(target_name.as_str()))
            .unwrap()
            .metadata
            .uid
            .as_deref(),
        Some(target_uid.as_str())
    );

    let mut status = api.last_status().unwrap();
    for _ in 0..360 {
        reconcile_set(
            &make_set("same-pod-add", 2, Some(status.clone())),
            &api,
            &ReconcilerState::default(),
        )
        .await
        .unwrap();
        api.mark_all_pods_ready();
        status = api.last_status().unwrap();
        if status.phase == Phase::Healthy
            && status
                .stable_snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.members.len() == 2)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(status.phase, Phase::Healthy, "{status:?}");
    assert_stable_snapshot(&api, &status, 2);

    let primary_name = status.current_primary.clone().unwrap();
    let primary_pod = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .find(|pod| pod.metadata.name.as_deref() == Some(primary_name.as_str()))
        .unwrap()
        .clone();
    let final_primary = api
        .create_replica_handle(
            status.stable_snapshot.as_ref().unwrap().primary_id,
            &primary_pod,
            &make_set("same-pod-add", 2, Some(status.clone())).spec,
        )
        .await
        .unwrap()
        .get_status()
        .await
        .unwrap();
    let final_build = final_primary.build_observation.unwrap();
    assert_ne!(
        final_build.target_agent_generation.to_string(),
        original_generation,
        "same-Pod process restart must invalidate the old target-generation build proof"
    );
}

#[test_log::test(tokio::test)]
#[serial]
async fn test_missing_dispatch_fences_are_reobserved_before_dispatch() {
    let api = KvClusterApi::new();
    let state = ReconcilerState::default();
    let status = create_healthy_set(&api, &state, "rollback-fences", 3).await;
    let original_primary = status.current_primary.clone().unwrap();
    let target = api
        .pods
        .lock()
        .unwrap()
        .iter()
        .map(|pod| pod.metadata.name.clone().unwrap())
        .find(|name| name != &original_primary)
        .unwrap();
    reconcile_set(
        &make_set(
            "rollback-fences",
            3,
            Some(KubericSetStatus {
                target_primary: Some(target),
                ..status
            }),
        ),
        &api,
        &state,
    )
    .await
    .unwrap();
    let mut pending_status = advance_until_pending_action(
        &api,
        "rollback-fences",
        3,
        api.last_status().unwrap(),
        DurableActionKind::PromoteTarget,
    )
    .await;
    let pending = pending_status
        .operation
        .as_mut()
        .unwrap()
        .pending_action
        .as_mut()
        .unwrap();
    let attempts = pending.attempts;
    let deadline = pending.deadline_unix_seconds;
    pending.dispatch_agent_generation = None;
    pending.dispatch_agent_control_version = None;
    pending.dispatch_observed_runtime_epoch = None;

    api.reset_operations();
    reconcile_set(
        &make_set("rollback-fences", 3, Some(pending_status)),
        &api,
        &ReconcilerState::default(),
    )
    .await
    .unwrap();
    let refreshed = api.last_status().unwrap();
    let pending = refreshed
        .operation
        .as_ref()
        .unwrap()
        .pending_action
        .as_ref()
        .unwrap();
    assert!(pending.dispatch_agent_generation.is_some());
    assert_eq!(pending.attempts, attempts);
    assert_eq!(pending.deadline_unix_seconds, deadline);
    assert!(
        api.operations()
            .iter()
            .all(|operation| *operation == ControlOperation::GetStatus)
    );
}
