use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::{env, process::Command};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use kuberic_agent::command::admit_configuration;
use kuberic_agent::coordinator::Coordinator;
use kuberic_agent::hosting::PodRuntime;
use kuberic_agent::recovery::{RecoveryDecision, inspect_recovery, recover_pending};
use kuberic_agent::runtime_adapter::{RuntimeAdapter, RuntimeEffectExecutor};
use kuberic_agent::sqlite_store::SqliteStore;
use kuberic_agent::state::{
    AgentState, CoordinatorStage, EffectStage, SCHEMA_VERSION, StorageIdentity,
};
use kuberic_agent::store::{AgentStore, BeginConfiguration, BeginEffect};
use kuberic_agent::{AgentError, Result};
use kuberic_protocol::command::{EnsureConfiguration, PrepareSwitchover};
use kuberic_protocol::types::{
    AccessStatus, AgentGeneration, ConfigurationDescriptor, ConfigurationMember, EffectivePolicy,
    Epoch, InitializationId, OperationId, PodUid, PvcUid, ReplicaId, ReplicaIdentity,
    ReplicaInstanceId, ReplicaRole, ResourceUid, SwitchoverRequestId, TransitionKind,
};
use kuberic_runtime::application::{
    CopyChunk, DurableApplicationAck, DurableApplicationProgress, OpenContext, Operation,
    OperationDataStream, RoleChange, StateProvider, StatefulServiceReplica,
};
use kuberic_runtime::engine::{DurableState, RetainedOperationStream};
use kuberic_runtime::replicator::{DefaultReplicatorFactory, Replicator, ReplicatorSettings};
use kuberic_runtime::{Result as RuntimeResult, RuntimeError};
use kuberic_runtime_internal::authority::{
    AdmittedAuthority, ReplicaAuthorityStore, ReplicationProgress, ReplicationProgressStore,
};
use kuberic_runtime_internal::effects::{
    OpenMode, RuntimeEffect, RuntimeEffectAction, RuntimeEffectResult, RuntimePostcondition,
    RuntimeSnapshot,
};
use serde::{Deserialize, Serialize};
use tempfile::tempdir;

struct FakeRuntime {
    calls: AtomicUsize,
    result: RuntimeEffectResult,
}

struct CancelledRuntime;

struct CrashAfterRealEffect {
    runtime: Arc<PodRuntime>,
    boundary: Option<String>,
}

#[async_trait]
impl RuntimeEffectExecutor for CrashAfterRealEffect {
    async fn apply_runtime_effect(&self, effect: RuntimeEffect) -> Result<RuntimeEffectResult> {
        let boundary = match &effect.action {
            RuntimeEffectAction::AdmitAuthority(_) => "authority",
            RuntimeEffectAction::ChangeApplicationRole(_) => "application-role",
            RuntimeEffectAction::SetAccessStatus { .. } => "access",
            _ => "",
        };
        if matches!(effect.action, RuntimeEffectAction::WaitForCatchup) {
            let snapshot = self.runtime.snapshot().await;
            let authority = snapshot.authority.unwrap();
            let receiver = authority
                .current_configuration
                .members
                .iter()
                .find(|member| member.identity != authority.local_identity)
                .unwrap();
            self.runtime
                .data_plane()
                .accept_acknowledgement(kuberic_wire::proto::ReplicationAck {
                    protocol_version: kuberic_protocol::PROTOCOL_VERSION,
                    sender: Some(authority.primary_identity().clone().into()),
                    receiver: Some(receiver.identity.clone().into()),
                    epoch: Some(authority.current_configuration.epoch.into()),
                    previous_configuration_id: authority
                        .previous_configuration
                        .as_ref()
                        .map_or_else(String::new, |cc| cc.configuration_id.to_string()),
                    current_configuration_id: authority
                        .current_configuration
                        .configuration_id
                        .to_string(),
                    received_lsn: 7,
                    applied_lsn: 7,
                    committed_lsn: 7,
                    ..Default::default()
                })
                .await?;
        }
        let result = self.runtime.apply_effect(effect).await?;
        if self.boundary.as_deref() == Some(boundary) {
            // Exit before RuntimeAdapter records the result or advances the stage.
            std::process::exit(73);
        }
        Ok(result)
    }
}

fn real_handoff_fixture(scenario: &str) -> (AgentState, EnsureConfiguration) {
    let fixture = match scenario {
        "target-current-only" => "retirement-after",
        "compensation-promotion" => "compensation-admission",
        other => other,
    };
    let (mut state, mut command) = switchover_recovery_fixture(fixture);
    if scenario == "target-current-only" {
        let target = switchover_prepare_command().target;
        state.identity.local_identity = target.clone();
        state.role = ReplicaRole::Primary;
        state.prepared_switchover = None;
        command.local_replica_id = target.replica_id;
        command.expected_instance_id = target.instance_id;
        command.expected_agent_generation = target.agent_generation;
        command.retire_switchover_preparation_ids.clear();
    }
    (state, command)
}

struct SwitchoverRecoveryRuntime {
    state: Mutex<RuntimePostcondition>,
    crash_after: Option<&'static str>,
}

impl SwitchoverRecoveryRuntime {
    fn new(state: &AgentState, crash_after: Option<&'static str>) -> Self {
        let mut snapshot = result().postcondition;
        snapshot.open = true;
        snapshot.role = state.role;
        snapshot.read_status = AccessStatus::ReconfigurationPending;
        snapshot.write_status = AccessStatus::ReconfigurationPending;
        snapshot.current_progress = 7;
        snapshot.verified_replication_lsn = Some(7);
        snapshot.committed_lsn = 7;
        snapshot.current_configuration_quorum_progress = 7;
        snapshot.catch_up_complete = true;
        snapshot.authority = Some(AdmittedAuthority {
            local_identity: state.identity.local_identity.clone(),
            transition_kind: state
                .previous_configuration
                .as_ref()
                .map(|_| TransitionKind::PlannedSwitchover),
            previous_configuration: state.previous_configuration.clone(),
            current_configuration: state.current_configuration.clone().unwrap(),
            switchover_handoff: (state.highest_epoch.configuration_number > 1)
                .then(|| {
                    state
                        .prepared_switchover
                        .clone()
                        .or(state.retired_switchover.clone())
                })
                .flatten(),
        });
        Self {
            state: Mutex::new(snapshot),
            crash_after,
        }
    }
}

#[async_trait]
impl RuntimeEffectExecutor for SwitchoverRecoveryRuntime {
    async fn apply_runtime_effect(&self, effect: RuntimeEffect) -> Result<RuntimeEffectResult> {
        let mut state = self.state.lock().unwrap();
        let stage = match effect.action {
            RuntimeEffectAction::AdmitAuthority(authority) => {
                state.authority = Some(*authority);
                "admission"
            }
            RuntimeEffectAction::ChangeApplicationRole(role) => {
                state.role = role;
                "role"
            }
            RuntimeEffectAction::SetReadStatus(read) => {
                state.read_status = read;
                "read"
            }
            RuntimeEffectAction::SetWriteStatus(write) => {
                state.write_status = write;
                "write"
            }
            RuntimeEffectAction::SetAccessStatus { read, write } => {
                state.read_status = read;
                state.write_status = write;
                "access"
            }
            RuntimeEffectAction::RefreshApplicationProgress
            | RuntimeEffectAction::ChangeReplicatorRole(_)
            | RuntimeEffectAction::UpdateEpoch
            | RuntimeEffectAction::WaitForCatchup => "other",
            action => panic!("unexpected recovery effect {action:?}"),
        };
        if self.crash_after == Some(stage) {
            std::process::exit(0);
        }
        Ok(RuntimeEffectResult {
            operation_id: effect.operation_id,
            sequence: effect.sequence,
            postcondition: state.clone(),
        })
    }
}

fn switchover_recovery_fixture(boundary: &str) -> (AgentState, EnsureConfiguration) {
    let starting = switchover_configuration();
    let prepare = switchover_prepare_command();
    let requested = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        prepare.target.replica_id,
        starting
            .members
            .iter()
            .map(|member| ConfigurationMember {
                identity: member.identity.clone(),
                role: if member.identity == prepare.target {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        starting.write_quorum,
    );
    let handoff = kuberic_protocol::types::SwitchoverHandoff {
        preparation_generation: prepare.preparation_generation,
        preparation_operation_id: prepare.operation_id,
        request_id: prepare.request_id,
        source: prepare.source.clone(),
        target: prepare.target.clone(),
        starting_configuration_id: starting.configuration_id.clone(),
        starting_epoch: starting.epoch,
        handoff_lsn: 7,
    };
    let local = if boundary == "promotion" {
        prepare.target
    } else {
        prepare.source
    };
    let compensation = boundary.starts_with("compensation");
    let restoring = boundary.starts_with("restoration");
    let current_only = boundary.starts_with("retirement") || boundary == "compensation-completion";
    let current = if compensation {
        ConfigurationDescriptor::new(
            Epoch::new(0, 3),
            starting.primary_id,
            starting.members.clone(),
            starting.write_quorum,
        )
    } else if restoring {
        starting.clone()
    } else {
        requested.clone()
    };
    let previous = if compensation {
        requested.clone()
    } else {
        starting.clone()
    };
    let mut state = AgentState::new(StorageIdentity {
        local_identity: local.clone(),
        ..switchover_storage_identity()
    });
    state.current_configuration = Some(if current_only {
        current.clone()
    } else {
        previous.clone()
    });
    state.previous_configuration = current_only.then(|| previous.clone());
    state.highest_epoch = state.current_configuration.as_ref().unwrap().epoch;
    state.role = state
        .current_configuration
        .as_ref()
        .unwrap()
        .members
        .iter()
        .find(|member| member.identity == local)
        .unwrap()
        .role;
    state.write_status = AccessStatus::ReconfigurationPending;
    state.prepared_switchover =
        (local == handoff.source && !boundary.contains("unobserved")).then(|| handoff.clone());
    let command = EnsureConfiguration {
        operation_id: OperationId::new(format!("recover-{boundary}")),
        previous_configuration: (!current_only && !restoring).then(|| previous.clone()),
        previous_epoch: (!current_only && !restoring).then_some(previous.epoch),
        current_epoch: current.epoch,
        current_configuration: current,
        effective_policy: state.identity.effective_policy.clone(),
        local_replica_id: local.replica_id,
        expected_instance_id: local.instance_id,
        expected_agent_generation: local.agent_generation,
        transition_kind: TransitionKind::PlannedSwitchover,
        failover_safe_lsn: None,
        primary_write_status: AccessStatus::ReconfigurationPending,
        current_only,
        retire_build_ids: Vec::new(),
        retire_switchover_preparation_ids: if current_only || restoring {
            vec![handoff.preparation()]
        } else {
            Vec::new()
        },
        switchover_handoff: (!boundary.contains("unobserved")).then_some(handoff),
    };
    (state, command)
}

#[async_trait]
impl RuntimeEffectExecutor for CancelledRuntime {
    async fn apply_runtime_effect(&self, _effect: RuntimeEffect) -> Result<RuntimeEffectResult> {
        Err(RuntimeError::ReplicaRemoved(2).into())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct CrashPersistedState {
    applied_lsn: i64,
    committed_lsn: i64,
    operations: BTreeMap<i64, Vec<u8>>,
}

struct CrashState {
    path: PathBuf,
    state: Mutex<CrashPersistedState>,
}

impl CrashState {
    fn open(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        let state = if path.is_file() {
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap()
        } else {
            CrashPersistedState::default()
        };
        Self {
            path,
            state: Mutex::new(state),
        }
    }

    fn persist(&self, state: &CrashPersistedState) -> RuntimeResult<()> {
        let temporary = self.path.with_extension("tmp");
        let bytes = serde_json::to_vec(state)
            .map_err(|error| RuntimeError::Application(error.to_string()))?;
        std::fs::write(&temporary, bytes)
            .map_err(|error| RuntimeError::Application(error.to_string()))?;
        std::fs::File::open(&temporary)
            .and_then(|file| file.sync_all())
            .map_err(|error| RuntimeError::Application(error.to_string()))?;
        std::fs::rename(&temporary, &self.path)
            .map_err(|error| RuntimeError::Application(error.to_string()))?;
        std::fs::File::open(self.path.parent().expect("application state parent"))
            .and_then(|directory| directory.sync_all())
            .map_err(|error| RuntimeError::Application(error.to_string()))
    }

    fn progress(state: &CrashPersistedState) -> DurableApplicationProgress {
        DurableApplicationProgress {
            applied_lsn: state.applied_lsn,
            committed_lsn: state.committed_lsn,
        }
    }
}

#[async_trait]
impl StatefulServiceReplica for CrashState {
    async fn open(self: Arc<Self>, context: OpenContext) -> RuntimeResult<Arc<dyn Replicator>> {
        let partition = context
            .partition
            .with_factory(Arc::new(DefaultReplicatorFactory::new(self.clone())));
        let interfaces = partition
            .create_replicator(self.clone(), Some(ReplicatorSettings::default()))
            .await?;
        Ok(interfaces.replicator())
    }

    async fn change_role(&self, _role: ReplicaRole) -> RuntimeResult<RoleChange> {
        Ok(RoleChange {
            service_address: None,
        })
    }

    async fn close(&self) -> RuntimeResult<()> {
        Ok(())
    }

    fn abort(&self) {}
}

#[async_trait]
impl StateProvider for CrashState {
    async fn update_epoch(
        &self,
        _epoch: Epoch,
        _previous_epoch_last_lsn: i64,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    async fn last_committed_lsn(&self) -> RuntimeResult<i64> {
        Ok(self.state.lock().unwrap().committed_lsn)
    }

    async fn get_copy_context(&self) -> RuntimeResult<OperationDataStream> {
        Ok(Box::pin(stream::empty()))
    }

    async fn get_copy_state(
        &self,
        _up_to_lsn: i64,
        _copy_context: OperationDataStream,
    ) -> RuntimeResult<OperationDataStream> {
        Ok(Box::pin(stream::empty()))
    }

    async fn on_data_loss(&self) -> RuntimeResult<bool> {
        Ok(false)
    }
}

#[async_trait]
impl DurableState for CrashState {
    async fn get_replication_operations(
        &self,
        from_lsn: i64,
        to_lsn: i64,
    ) -> RuntimeResult<RetainedOperationStream> {
        let operations = self
            .state
            .lock()
            .unwrap()
            .operations
            .range(from_lsn..=to_lsn)
            .map(|(lsn, data)| {
                Ok(Operation {
                    lsn: *lsn,
                    committed_lsn: *lsn,
                    data: Bytes::copy_from_slice(data),
                })
            })
            .collect::<Vec<_>>();
        Ok(Box::pin(stream::iter(operations)))
    }

    async fn apply_copy_chunk(
        &self,
        _build_id: &OperationId,
        _sequence: u64,
        _chunk: CopyChunk,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    async fn verify_copy_chunk(
        &self,
        _build_id: &OperationId,
        _sequence: u64,
        _chunk: &CopyChunk,
    ) -> RuntimeResult<bool> {
        Ok(true)
    }

    async fn finish_copy(
        &self,
        _build_id: &OperationId,
        up_to_lsn: i64,
        committed_lsn: i64,
    ) -> RuntimeResult<DurableApplicationProgress> {
        let progress = DurableApplicationProgress {
            applied_lsn: up_to_lsn,
            committed_lsn,
        };
        let mut state = self.state.lock().unwrap();
        let mut candidate = state.clone();
        candidate.applied_lsn = up_to_lsn;
        candidate.committed_lsn = committed_lsn;
        self.persist(&candidate)?;
        *state = candidate;
        Ok(progress)
    }

    async fn apply(&self, operation: Operation) -> RuntimeResult<DurableApplicationAck> {
        let mut state = self.state.lock().unwrap();
        if let Some(existing) = state.operations.get(&operation.lsn) {
            if existing.as_slice() != operation.data.as_ref() {
                return Err(RuntimeError::AuthorityMismatch(
                    "crash fixture LSN reused with different data".into(),
                ));
            }
            return Ok(Self::progress(&state));
        }
        let mut candidate = state.clone();
        candidate
            .operations
            .insert(operation.lsn, operation.data.to_vec());
        candidate.applied_lsn = candidate.applied_lsn.max(operation.lsn);
        candidate.committed_lsn = candidate.committed_lsn.max(operation.committed_lsn);
        self.persist(&candidate)?;
        *state = candidate;
        Ok(Self::progress(&state))
    }

    async fn durable_progress(&self) -> RuntimeResult<DurableApplicationProgress> {
        Ok(Self::progress(&self.state.lock().unwrap()))
    }

    async fn verify_applied(&self, operation: &Operation) -> RuntimeResult<bool> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .operations
            .get(&operation.lsn)
            .is_some_and(|data| data.as_slice() == operation.data.as_ref()))
    }

    async fn commit(&self, committed_lsn: i64) -> RuntimeResult<DurableApplicationProgress> {
        let mut state = self.state.lock().unwrap();
        if committed_lsn > state.applied_lsn {
            return Err(RuntimeError::Application(
                "cannot commit beyond applied progress".into(),
            ));
        }
        let mut candidate = state.clone();
        candidate.committed_lsn = candidate.committed_lsn.max(committed_lsn);
        self.persist(&candidate)?;
        *state = candidate;
        if committed_lsn == 1
            && env::var("KUBERIC_LOCAL_RECOVERY_BOUNDARY").as_deref() == Ok("application-commit")
        {
            std::process::exit(73);
        }
        Ok(Self::progress(&state))
    }
}

#[async_trait]
impl RuntimeEffectExecutor for FakeRuntime {
    async fn apply_runtime_effect(&self, _effect: RuntimeEffect) -> Result<RuntimeEffectResult> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.result.clone())
    }
}

fn storage_identity() -> StorageIdentity {
    StorageIdentity {
        schema_version: SCHEMA_VERSION,
        resource_uid: ResourceUid::new("resource-1"),
        pod_uid: PodUid::new("pod-1"),
        pvc_uid: PvcUid::new("pvc-1"),
        initialization_id: InitializationId::new("init-1"),
        local_identity: ReplicaIdentity {
            replica_id: ReplicaId::new(1),
            instance_id: ReplicaInstanceId::new("instance-1"),
            agent_generation: AgentGeneration::new("generation-1"),
        },
        effective_policy: EffectivePolicy::fixed(3, 30).unwrap(),
    }
}

fn effect() -> RuntimeEffect {
    RuntimeEffect {
        operation_id: OperationId::new("effect-1"),
        sequence: 1,
        action: RuntimeEffectAction::Open(OpenMode::Existing),
    }
}

fn configuration_command() -> EnsureConfiguration {
    let local = storage_identity().local_identity;
    let effective_policy = EffectivePolicy::fixed(1, 30).unwrap();
    let current_configuration = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        local.replica_id,
        vec![ConfigurationMember {
            identity: local.clone(),
            role: ReplicaRole::Primary,
        }],
        effective_policy.write_quorum,
    );
    EnsureConfiguration {
        operation_id: OperationId::new("configuration-1"),
        previous_configuration: None,
        current_configuration,
        previous_epoch: None,
        current_epoch: Epoch::new(0, 1),
        effective_policy,
        local_replica_id: local.replica_id,
        expected_instance_id: local.instance_id,
        expected_agent_generation: local.agent_generation,
        transition_kind: TransitionKind::Bootstrap,
        failover_safe_lsn: None,
        primary_write_status: AccessStatus::ReconfigurationPending,
        current_only: false,
        retire_build_ids: Vec::new(),
        switchover_handoff: None,
        retire_switchover_preparation_ids: Vec::new(),
    }
}

fn single_storage_identity() -> StorageIdentity {
    StorageIdentity {
        effective_policy: EffectivePolicy::fixed(1, 30).unwrap(),
        ..storage_identity()
    }
}

fn real_configuration_command() -> EnsureConfiguration {
    let identity = single_storage_identity().local_identity;
    let policy = EffectivePolicy::fixed(1, 30).unwrap();
    let current_configuration = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        identity.replica_id,
        vec![ConfigurationMember {
            identity: identity.clone(),
            role: ReplicaRole::Primary,
        }],
        policy.write_quorum,
    );
    EnsureConfiguration {
        operation_id: OperationId::new("real-runtime-configuration"),
        previous_configuration: None,
        current_configuration,
        previous_epoch: None,
        current_epoch: Epoch::new(0, 1),
        effective_policy: policy,
        local_replica_id: identity.replica_id,
        expected_instance_id: identity.instance_id,
        expected_agent_generation: identity.agent_generation,
        transition_kind: TransitionKind::Bootstrap,
        failover_safe_lsn: None,
        primary_write_status: AccessStatus::ReconfigurationPending,
        current_only: false,
        retire_build_ids: Vec::new(),
        switchover_handoff: None,
        retire_switchover_preparation_ids: Vec::new(),
    }
}

fn grant_access_command() -> EnsureConfiguration {
    EnsureConfiguration {
        operation_id: OperationId::new("real-runtime-grant"),
        primary_write_status: AccessStatus::Granted,
        ..real_configuration_command()
    }
}

fn crash_application_path(database_path: &Path) -> PathBuf {
    database_path
        .parent()
        .and_then(Path::parent)
        .expect("agent database is under the data root")
        .join("crash-application.json")
}

fn real_runtime(
    store: Arc<SqliteStore>,
    application_path: &Path,
) -> (Arc<PodRuntime>, Arc<CrashState>) {
    let application = Arc::new(CrashState::open(application_path));
    (
        Arc::new(PodRuntime::new(
            single_storage_identity().local_identity,
            application.clone(),
            store,
        )),
        application,
    )
}

fn switchover_storage_identity() -> StorageIdentity {
    StorageIdentity {
        effective_policy: EffectivePolicy::fixed(2, 30).unwrap(),
        ..storage_identity()
    }
}

fn switchover_configuration() -> ConfigurationDescriptor {
    let source = switchover_storage_identity().local_identity;
    ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        source.replica_id,
        vec![
            ConfigurationMember {
                identity: source,
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: ReplicaIdentity {
                    replica_id: ReplicaId::new(2),
                    instance_id: ReplicaInstanceId::new("pod-2"),
                    agent_generation: AgentGeneration::new("generation-2"),
                },
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    )
}

fn switchover_prepare_command() -> PrepareSwitchover {
    let source = switchover_storage_identity().local_identity;
    let configuration = switchover_configuration();
    PrepareSwitchover {
        preparation_generation: 1,
        operation_id: kuberic_protocol::types::derive_switchover_preparation_operation_id(
            &switchover_storage_identity().resource_uid,
            &SwitchoverRequestId::new("real-switchover-request"),
            1,
            &configuration.configuration_id,
            &source,
            &configuration.members[1].identity,
        ),
        request_id: SwitchoverRequestId::new("real-switchover-request"),
        local_replica_id: source.replica_id,
        expected_instance_id: source.instance_id.clone(),
        expected_agent_generation: source.agent_generation.clone(),
        source,
        target: configuration.members[1].identity.clone(),
        current_configuration: configuration,
    }
}

fn switchover_runtime_effect(command: &PrepareSwitchover, sequence: u64) -> RuntimeEffect {
    RuntimeEffect {
        operation_id: command.operation_id.clone(),
        sequence,
        action: RuntimeEffectAction::PrepareSwitchover {
            preparation_generation: 1,
            request_id: command.request_id.clone(),
            source: command.source.clone(),
            target: command.target.clone(),
            starting_configuration_id: command.current_configuration.configuration_id.clone(),
            starting_epoch: command.current_configuration.epoch,
        },
    }
}

fn switchover_real_runtime(
    store: Arc<SqliteStore>,
    application_path: &Path,
) -> (Arc<PodRuntime>, Arc<CrashState>) {
    let application = Arc::new(CrashState::open(application_path));
    (
        Arc::new(PodRuntime::new(
            switchover_storage_identity().local_identity,
            application.clone(),
            store,
        )),
        application,
    )
}

fn seeded_operation() -> Operation {
    Operation {
        lsn: 1,
        committed_lsn: 1,
        data: Bytes::from_static(b"acknowledged-before-crash"),
    }
}

fn result() -> RuntimeEffectResult {
    RuntimeEffectResult {
        operation_id: OperationId::new("effect-1"),
        sequence: 1,
        postcondition: RuntimePostcondition {
            open: true,
            role: ReplicaRole::None,
            role_transition: None,
            read_status: AccessStatus::NotPrimary,
            write_status: AccessStatus::NotPrimary,
            authority: None,
            current_progress: 0,
            verified_replication_lsn: None,
            committed_lsn: 0,
            current_configuration_quorum_progress: 0,
            catch_up_boundary: None,
            catch_up_complete: false,
            builds: Vec::new(),
        },
    }
}

fn switchover_effect() -> RuntimeEffect {
    let authority = switchover_authority();
    RuntimeEffect {
        operation_id: OperationId::new("prepare-switchover-1"),
        sequence: 1,
        action: RuntimeEffectAction::PrepareSwitchover {
            preparation_generation: 1,
            request_id: SwitchoverRequestId::new("request-1"),
            source: storage_identity().local_identity,
            target: ReplicaIdentity {
                replica_id: ReplicaId::new(2),
                instance_id: ReplicaInstanceId::new("pod-2"),
                agent_generation: AgentGeneration::new("generation-2"),
            },
            starting_configuration_id: authority.current_configuration.configuration_id,
            starting_epoch: authority.current_configuration.epoch,
        },
    }
}

fn switchover_result() -> RuntimeEffectResult {
    let authority = switchover_authority();
    RuntimeEffectResult {
        operation_id: OperationId::new("prepare-switchover-1"),
        sequence: 1,
        postcondition: RuntimePostcondition {
            open: true,
            role: ReplicaRole::Primary,
            role_transition: None,
            read_status: AccessStatus::Granted,
            write_status: AccessStatus::ReconfigurationPending,
            authority: Some(authority),
            current_progress: 9,
            verified_replication_lsn: Some(9),
            committed_lsn: 7,
            current_configuration_quorum_progress: 9,
            catch_up_boundary: None,
            catch_up_complete: true,
            builds: Vec::new(),
        },
    }
}

fn switchover_authority() -> AdmittedAuthority {
    let source = storage_identity().local_identity;
    let target = ReplicaIdentity {
        replica_id: ReplicaId::new(2),
        instance_id: ReplicaInstanceId::new("pod-2"),
        agent_generation: AgentGeneration::new("generation-2"),
    };
    AdmittedAuthority {
        local_identity: source.clone(),
        transition_kind: None,
        previous_configuration: None,
        current_configuration: ConfigurationDescriptor::new(
            Epoch::new(0, 1),
            source.replica_id,
            vec![
                ConfigurationMember {
                    identity: source,
                    role: ReplicaRole::Primary,
                },
                ConfigurationMember {
                    identity: target,
                    role: ReplicaRole::ActiveSecondary,
                },
            ],
            2,
        ),
        switchover_handoff: None,
    }
}

fn snapshot(write_status: AccessStatus) -> RuntimeSnapshot {
    RuntimeSnapshot {
        identity: storage_identity().local_identity,
        open: false,
        replication_address: None,
        role: ReplicaRole::None,
        role_transition: None,
        read_status: AccessStatus::NotPrimary,
        write_status,
        authority: None,
        current_progress: 0,
        verified_replication_lsn: None,
        committed_lsn: 0,
        current_configuration_quorum_progress: 0,
        catch_up_boundary: None,
        catch_up_complete: false,
        builds: Vec::new(),
    }
}

#[tokio::test]
async fn recovery_reissues_committed_intent_without_reporting_completion() {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let store = SqliteStore::create_authorized(&path, AgentState::new(storage_identity())).unwrap();
    assert_eq!(
        store.begin_effect(&effect()).await.unwrap(),
        BeginEffect::Execute(effect())
    );
    drop(store);

    let reopened = Arc::new(SqliteStore::open_existing(&path, None).unwrap());
    assert_eq!(
        inspect_recovery(reopened.as_ref(), &snapshot(AccessStatus::NotPrimary))
            .await
            .unwrap(),
        RecoveryDecision::Reissue(Box::new(effect()))
    );
    let runtime = Arc::new(FakeRuntime {
        calls: AtomicUsize::new(0),
        result: result(),
    });
    let adapter = RuntimeAdapter::new(reopened.clone(), runtime.clone());
    assert_eq!(
        recover_pending(&adapter, &snapshot(AccessStatus::NotPrimary))
            .await
            .unwrap(),
        Some(result())
    );
    assert_eq!(runtime.calls.load(Ordering::SeqCst), 1);
    assert!(
        reopened
            .load_state()
            .await
            .unwrap()
            .pending_effect
            .is_none()
    );
}

#[tokio::test]
async fn recovery_reobserves_effect_applied_before_completion_persistence() {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let store = SqliteStore::create_authorized(&path, AgentState::new(storage_identity())).unwrap();
    store.begin_effect(&effect()).await.unwrap();
    store.mark_effect_applied(&effect()).await.unwrap();
    drop(store);

    let reopened = Arc::new(SqliteStore::open_existing(&path, None).unwrap());
    let runtime = Arc::new(FakeRuntime {
        calls: AtomicUsize::new(0),
        result: result(),
    });
    let adapter = RuntimeAdapter::new(reopened.clone(), runtime.clone());
    let recovered = recover_pending(&adapter, &snapshot(AccessStatus::NotPrimary))
        .await
        .unwrap();
    assert_eq!(recovered, Some(result()));
    assert_eq!(runtime.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        reopened.retained_result().await.unwrap().unwrap().result,
        result()
    );
}

#[tokio::test]
async fn bootstrap_terminal_result_survives_transition_clearing_and_reopen() {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let store = Arc::new(
        SqliteStore::create_authorized(&path, AgentState::new(storage_identity())).unwrap(),
    );
    let runtime = Arc::new(FakeRuntime {
        calls: AtomicUsize::new(0),
        result: result(),
    });
    let adapter = RuntimeAdapter::new(store.clone(), runtime);
    adapter.execute(effect()).await.unwrap();

    store
        .set_reconfiguration(Some("transition-stage".into()))
        .await
        .unwrap();
    store.clear_reconfiguration().await.unwrap();
    drop(adapter);
    drop(store);

    let reopened = SqliteStore::open_existing(&path, None).unwrap();
    let state = reopened.load_state().await.unwrap();
    assert!(state.reconfiguration_data.is_none());
    assert_eq!(state.retained_result.unwrap().result, result());
}

#[tokio::test]
async fn recovery_refuses_a_runtime_that_did_not_start_write_closed() {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let store = SqliteStore::create_authorized(&path, AgentState::new(storage_identity())).unwrap();
    assert!(matches!(
        inspect_recovery(&store, &snapshot(AccessStatus::Granted)).await,
        Err(AgentError::EffectConflict(_))
    ));
}

#[tokio::test]
async fn definitively_cancelled_build_effect_releases_durable_intent() {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let store = Arc::new(
        SqliteStore::create_authorized(&path, AgentState::new(storage_identity())).unwrap(),
    );
    let effect = RuntimeEffect {
        operation_id: OperationId::new("cancelled-build"),
        sequence: 1,
        action: RuntimeEffectAction::BuildReplica {
            build_id: OperationId::new("cancelled-build"),
            target: ReplicaIdentity {
                replica_id: ReplicaId::new(2),
                instance_id: ReplicaInstanceId::new("lost-target"),
                agent_generation: AgentGeneration::new("lost-generation"),
            },
            replication_address: String::new(),
        },
    };
    assert!(matches!(
        RuntimeAdapter::new(store.clone(), Arc::new(CancelledRuntime))
            .execute(effect)
            .await,
        Err(AgentError::Runtime(RuntimeError::ReplicaRemoved(2)))
    ));
    assert!(store.load_state().await.unwrap().pending_effect.is_none());
}

#[test]
fn process_session_changes_without_changing_durable_generation() {
    let first = kuberic_agent::session::ProcessSession::new();
    let second = kuberic_agent::session::ProcessSession::new();
    assert_ne!(first.id(), second.id());
    assert_eq!(first.next_report_sequence(), 1);
    assert_eq!(first.next_report_sequence(), 2);
    assert_eq!(
        storage_identity().local_identity.agent_generation,
        AgentGeneration::new("generation-1")
    );
    assert_eq!(Epoch::default(), Epoch::new(0, 0));
}

#[test]
fn sqlite_commits_survive_process_termination_without_destructors() {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let output = Command::new(env::current_exe().unwrap())
        .args(["--ignored", "--exact", "crash_writer_process"])
        .env("KUBERIC_CRASH_WRITER_PATH", &path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let reopened = SqliteStore::open_existing(&path, Some(&storage_identity())).unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let state = runtime.block_on(reopened.load_state()).unwrap();
    assert_eq!(state.pending_effect.unwrap().effect, effect());
}

#[test]
fn configuration_boundaries_survive_process_termination_without_destructors() {
    for boundary in [
        "pending-effect",
        "effect-complete-before-stage-advance",
        "enclosing-command",
        "terminal-command",
    ] {
        let directory = tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let output = Command::new(env::current_exe().unwrap())
            .args(["--ignored", "--exact", "crash_boundary_writer_process"])
            .env("KUBERIC_CRASH_WRITER_PATH", &path)
            .env("KUBERIC_CRASH_BOUNDARY", boundary)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child failed at {boundary}: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let reopened = SqliteStore::open_existing(&path, Some(&storage_identity())).unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let state = runtime.block_on(reopened.load_state()).unwrap();
        match boundary {
            "pending-effect" => {
                assert_eq!(
                    state.pending_effect.unwrap().stage,
                    EffectStage::IntentCommitted
                );
            }

            "effect-complete-before-stage-advance" => {
                assert_eq!(
                    state.reconfiguration.unwrap().stage,
                    CoordinatorStage::AdmitAuthority
                );
                assert_eq!(state.retained_result.unwrap().result, result());
            }
            "enclosing-command" => {
                assert_eq!(
                    state.reconfiguration.unwrap().stage,
                    CoordinatorStage::AdmitAuthority
                );
                assert!(state.retained_command.is_none());
            }
            "terminal-command" => {
                assert!(state.reconfiguration.is_none());
                assert_eq!(
                    state.retained_command.unwrap().command,
                    configuration_command()
                );
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn switchover_recovery_boundaries_survive_process_termination() {
    for boundary in [
        "demotion",
        "promotion",
        "compensation-allocation",
        "compensation-admission",
        "compensation-completion",
        "retirement-before",
        "retirement-after",
        "restoration-before",
        "restoration-after",
        "restoration-unobserved-before",
        "restoration-unobserved-after",
    ] {
        let directory = tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let output = Command::new(env::current_exe().unwrap())
            .args(["--ignored", "--exact", "switchover_recovery_writer_process"])
            .env("KUBERIC_RECOVERY_PATH", &path)
            .env("KUBERIC_RECOVERY_BOUNDARY", boundary)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{boundary}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let store = Arc::new(SqliteStore::open_existing(&path, None).unwrap());
            let state = store.load_state().await.unwrap();
            let (_, command) = switchover_recovery_fixture(boundary);
            assert_ne!(state.write_status, AccessStatus::Granted);
            if boundary.ends_with("-after") || boundary == "compensation-completion" {
                assert!(state.prepared_switchover.is_none());
                assert_eq!(state.retired_switchover, command.switchover_handoff);
            } else if boundary != "promotion" {
                assert_eq!(state.prepared_switchover, command.switchover_handoff);
            }
            let runtime = Arc::new(SwitchoverRecoveryRuntime::new(&state, None));
            let coordinator = Coordinator::new(store.clone(), runtime);
            let completed = coordinator
                .ensure_configuration(command.clone())
                .await
                .unwrap();
            assert_eq!(completed.command, command);
            assert_eq!(
                coordinator
                    .ensure_configuration(command.clone())
                    .await
                    .unwrap(),
                completed
            );
            let state = store.load_state().await.unwrap();
            assert!(state.pending_effect.is_none() && state.reconfiguration.is_none());
            assert_eq!(state.highest_epoch, command.current_epoch);
            assert_ne!(state.write_status, AccessStatus::Granted);
            if !command.retire_switchover_preparation_ids.is_empty() {
                assert!(state.prepared_switchover.is_none());
                assert_eq!(state.retired_switchover, command.switchover_handoff);
                assert_eq!(
                    state
                        .preparation_retirement
                        .as_ref()
                        .map(|retired| retired.generation),
                    command
                        .retire_switchover_preparation_ids
                        .first()
                        .map(|id| id.generation)
                );
                let mut writable = state.clone();
                writable.write_status = AccessStatus::Granted;
                assert!(
                    kuberic_agent::command::admit_switchover_preparation(
                        &switchover_prepare_command(),
                        &writable
                    )
                    .is_err()
                );
            }
        });
    }
}

#[test]
#[ignore = "helper process for switchover_recovery_boundaries_survive_process_termination"]
fn switchover_recovery_writer_process() {
    let (Ok(path), Ok(boundary)) = (
        env::var("KUBERIC_RECOVERY_PATH"),
        env::var("KUBERIC_RECOVERY_BOUNDARY"),
    ) else {
        return;
    };
    let (state, command) = switchover_recovery_fixture(&boundary);
    let crash_after = match boundary.as_str() {
        "demotion" | "promotion" => Some("role"),
        "compensation-admission" => Some("admission"),
        "retirement-before" | "restoration-before" | "restoration-unobserved-before" => {
            Some("access")
        }
        _ => None,
    };
    let runtime = Arc::new(SwitchoverRecoveryRuntime::new(&state, crash_after));
    let store = Arc::new(SqliteStore::create_authorized(path, state).unwrap());
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        if boundary == "compensation-allocation" {
            store.begin_configuration(&command).await.unwrap();
        } else {
            Coordinator::new(store, runtime)
                .ensure_configuration(command)
                .await
                .unwrap();
        }
    });
    std::process::exit(0);
}

#[test]
fn switchover_preparation_boundaries_survive_process_termination() {
    for boundary in ["pending-effect", "effect-applied", "effect-completed"] {
        let directory = tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let output = Command::new(env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "switchover_preparation_writer_process",
            ])
            .env("KUBERIC_SWITCHOVER_PATH", &path)
            .env("KUBERIC_SWITCHOVER_BOUNDARY", boundary)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "switchover child failed at {boundary}: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let store = Arc::new(SqliteStore::open_existing(&path, None).unwrap());
            if boundary != "effect-completed" {
                let adapter = RuntimeAdapter::new(
                    store.clone(),
                    Arc::new(FakeRuntime {
                        calls: AtomicUsize::new(0),
                        result: switchover_result(),
                    }),
                );
                adapter.resume_pending().await.unwrap();
            }
            let state = store.load_state().await.unwrap();
            assert!(state.pending_effect.is_none());
            let prepared = state.prepared_switchover.unwrap();
            assert_eq!(
                prepared.preparation_operation_id,
                switchover_effect().operation_id
            );
            assert_eq!(prepared.handoff_lsn, 9);
        });
    }
}

#[test]
fn real_runtime_switchover_preparation_recovers_after_process_termination() {
    for boundary in ["pending-effect", "effect-applied", "effect-completed"] {
        let directory = tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let output = Command::new(env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "real_switchover_preparation_writer_process",
            ])
            .env("KUBERIC_REAL_SWITCHOVER_PATH", &path)
            .env("KUBERIC_REAL_SWITCHOVER_BOUNDARY", boundary)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "real switchover child failed at {boundary}: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let store = Arc::new(
                SqliteStore::open_existing(&path, Some(&switchover_storage_identity())).unwrap(),
            );
            let state = store.load_state().await.unwrap();
            let application_path = crash_application_path(&path);
            let (pod, application) = switchover_real_runtime(store.clone(), &application_path);
            assert!(
                application
                    .verify_applied(&seeded_operation())
                    .await
                    .unwrap()
            );
            pod.reconstruct(
                OpenMode::Existing,
                state.role,
                state.read_status,
                state.write_status,
                None,
            )
            .await
            .unwrap();
            let coordinator = Coordinator::new(store.clone(), pod);
            let prepared = coordinator
                .ensure_switchover_prepared(switchover_prepare_command())
                .await
                .unwrap();
            assert!(prepared.handoff_lsn >= 1);
            let recovered = store.load_state().await.unwrap();
            assert!(recovered.pending_effect.is_none());
            assert_eq!(recovered.write_status, AccessStatus::ReconfigurationPending);
            assert_eq!(recovered.prepared_switchover, Some(prepared));
        });
    }
}

async fn acknowledge_recovery_item(
    pod: &PodRuntime,
    peer: &CrashState,
    item: kuberic_wire::proto::ReplicationItem,
) {
    let progress = peer
        .apply(Operation {
            lsn: item.lsn,
            committed_lsn: item.committed_lsn,
            data: item.data.into(),
        })
        .await
        .unwrap();
    pod.data_plane()
        .accept_acknowledgement(kuberic_wire::proto::ReplicationAck {
            protocol_version: kuberic_protocol::PROTOCOL_VERSION,
            sender: item.sender,
            receiver: item.receiver,
            epoch: item.epoch,
            previous_configuration_id: item.previous_configuration_id,
            current_configuration_id: item.current_configuration_id,
            received_lsn: progress.applied_lsn,
            applied_lsn: progress.applied_lsn,
            committed_lsn: progress.committed_lsn,
            ..Default::default()
        })
        .await
        .unwrap();
}

fn generation_preparation(generation: u64) -> PrepareSwitchover {
    let mut command = switchover_prepare_command();
    command.preparation_generation = generation;
    command.request_id = SwitchoverRequestId::new(format!("restoration-{generation}"));
    command.operation_id = kuberic_protocol::types::derive_switchover_preparation_operation_id(
        &switchover_storage_identity().resource_uid,
        &command.request_id,
        generation,
        &command.current_configuration.configuration_id,
        &command.source,
        &command.target,
    );
    command
}

#[test]
fn repeated_preparation_retirement_fences_every_delayed_command_after_process_termination() {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let output = Command::new(env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "local_write_recovery_writer_process",
        ])
        .env("KUBERIC_LOCAL_RECOVERY_PATH", &path)
        .env("KUBERIC_LOCAL_RECOVERY_BOUNDARY", "retirements")
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(73),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let store = Arc::new(SqliteStore::open_existing(&path, None).unwrap());
        let before = store.load_state().await.unwrap();
        assert_eq!(
            before.preparation_retirement.as_ref().unwrap().generation,
            3
        );
        let (pod, _) = switchover_real_runtime(store.clone(), &crash_application_path(&path));
        pod.reconstruct(
            OpenMode::Existing,
            ReplicaRole::Primary,
            before.read_status,
            before.write_status,
            None,
        )
        .await
        .unwrap();
        let runtime_before = pod.snapshot().await;
        let coordinator = Coordinator::new(store.clone(), pod.clone());
        for generation in 1..=3 {
            assert!(
                coordinator
                    .ensure_switchover_prepared(generation_preparation(generation))
                    .await
                    .is_err()
            );
            assert_eq!(store.load_state().await.unwrap(), before);
            assert_eq!(pod.snapshot().await, runtime_before);
        }
        assert_eq!(runtime_before.write_status, AccessStatus::Granted);
        assert!(
            coordinator
                .ensure_switchover_prepared(generation_preparation(4))
                .await
                .is_ok()
        );
    });
}

#[test]
fn local_write_recovery_boundaries_commit_fresh_writes_after_process_termination() {
    use kuberic_runtime_internal::authority::LocalWriteJournal;
    for boundary in ["registered", "application-commit", "grant"] {
        let directory = tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let output = Command::new(env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "local_write_recovery_writer_process",
            ])
            .env("KUBERIC_LOCAL_RECOVERY_PATH", &path)
            .env("KUBERIC_LOCAL_RECOVERY_BOUNDARY", boundary)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(73),
            "{boundary}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let store = Arc::new(SqliteStore::open_existing(&path, None).unwrap());
            let state = store.load_state().await.unwrap();
            assert_eq!(state.write_status, AccessStatus::ReconfigurationPending);
            let (pod, _) = switchover_real_runtime(store.clone(), &crash_application_path(&path));
            pod.reconstruct(OpenMode::Existing, ReplicaRole::Primary,
                AccessStatus::ReconfigurationPending, AccessStatus::ReconfigurationPending, None).await.unwrap();
            let adapter = Arc::new(RuntimeAdapter::new(store.clone(), pod.clone()));
            let recovery = adapter.clone();
            let mut task = tokio::spawn(async move { recovery.resume_pending().await });
            let plane = pod.data_plane();
            let peer = CrashState::open(path.with_extension("peer"));
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    tokio::select! {
                        result = &mut task => { result.unwrap().unwrap(); break; },
                        item = plane.next_outbound() => {
                            let Some(kuberic_agent::hosting::OutboundReplication::Replication(item)) = item else { panic!("recovery replication") };
                            assert_ne!(pod.snapshot().await.write_status, AccessStatus::Granted);
                            acknowledge_recovery_item(&pod, &peer, item).await;
                        }
                    }
                }
            }).await.unwrap();
            let old = store.load_local_write(&OperationId::new("interrupted-before-crash")).await.unwrap().unwrap();
            assert_eq!(old.phase, kuberic_runtime_internal::authority::LocalWritePhase::Committed);
            assert_eq!(old.data, Bytes::from_static(b"original-before-crash"));
            let fresh = plane.begin_write(kuberic_runtime::application::ClientWrite {
                operation_id: OperationId::new("fresh-after-process-crash"),
                data: Bytes::from_static(b"different-after-crash"),
            }).await.unwrap();
            for item in &fresh.replication_items { acknowledge_recovery_item(&pod, &peer, item.clone()).await; }
            assert_eq!(fresh.committed().await.unwrap().committed_lsn, 2);
        });
    }
}

#[test]
#[ignore = "subprocess for local_write_recovery_boundaries_commit_fresh_writes_after_process_termination"]
fn local_write_recovery_writer_process() {
    let (Ok(path), Ok(boundary)) = (
        env::var("KUBERIC_LOCAL_RECOVERY_PATH"),
        env::var("KUBERIC_LOCAL_RECOVERY_BOUNDARY"),
    ) else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let configuration = switchover_configuration();
        let mut state = AgentState::new(switchover_storage_identity());
        state.highest_epoch = configuration.epoch;
        state.current_configuration = Some(configuration.clone());
        state.role = ReplicaRole::Primary;
        state.read_status = AccessStatus::Granted;
        state.write_status = AccessStatus::Granted;
        let store = Arc::new(SqliteStore::create_authorized(&path, state).unwrap());
        store
            .admit(&AdmittedAuthority {
                local_identity: switchover_storage_identity().local_identity,
                transition_kind: None,
                previous_configuration: None,
                current_configuration: configuration.clone(),
                switchover_handoff: None,
            })
            .await
            .unwrap();
        let (pod, _) =
            switchover_real_runtime(store.clone(), &crash_application_path(Path::new(&path)));
        pod.reconstruct(
            OpenMode::Existing,
            ReplicaRole::Primary,
            AccessStatus::Granted,
            AccessStatus::Granted,
            None,
        )
        .await
        .unwrap();
        if boundary == "retirements" {
            let coordinator = Coordinator::new(store.clone(), pod.clone());
            for generation in 1..=3 {
                let command = generation_preparation(generation);
                let handoff = if generation < 3 {
                    Some(
                        coordinator
                            .ensure_switchover_prepared(command.clone())
                            .await
                            .unwrap(),
                    )
                } else {
                    None
                };
                let restore = EnsureConfiguration {
                    operation_id: OperationId::new("reused-restoration"),
                    previous_configuration: None,
                    previous_epoch: None,
                    current_epoch: configuration.epoch,
                    current_configuration: configuration.clone(),
                    effective_policy: switchover_storage_identity().effective_policy,
                    local_replica_id: command.source.replica_id,
                    expected_instance_id: command.source.instance_id,
                    expected_agent_generation: command.source.agent_generation,
                    transition_kind: TransitionKind::PlannedSwitchover,
                    failover_safe_lsn: None,
                    primary_write_status: AccessStatus::ReconfigurationPending,
                    current_only: false,
                    retire_build_ids: Vec::new(),
                    retire_switchover_preparation_ids: vec![
                        kuberic_protocol::types::SwitchoverPreparationId {
                            generation,
                            operation_id: command.operation_id,
                        },
                    ],
                    switchover_handoff: handoff,
                };
                coordinator
                    .ensure_configuration(restore.clone())
                    .await
                    .unwrap();
                coordinator
                    .ensure_configuration(EnsureConfiguration {
                        operation_id: OperationId::new("reused-grant"),
                        transition_kind: TransitionKind::Bootstrap,
                        primary_write_status: AccessStatus::Granted,
                        switchover_handoff: None,
                        retire_switchover_preparation_ids: Vec::new(),
                        ..restore
                    })
                    .await
                    .unwrap();
            }
            std::process::exit(73);
        }
        let pending = pod
            .data_plane()
            .begin_write(kuberic_runtime::application::ClientWrite {
                operation_id: OperationId::new("interrupted-before-crash"),
                data: Bytes::from_static(b"original-before-crash"),
            })
            .await
            .unwrap();
        let coordinator = Coordinator::new(store.clone(), pod.clone());
        let handoff = coordinator
            .ensure_switchover_prepared(switchover_prepare_command())
            .await
            .unwrap();
        assert!(pending.committed().await.is_err());
        let restore = EnsureConfiguration {
            operation_id: OperationId::new("recover-source"),
            previous_configuration: None,
            previous_epoch: None,
            current_epoch: configuration.epoch,
            current_configuration: configuration,
            effective_policy: switchover_storage_identity().effective_policy,
            local_replica_id: handoff.source.replica_id,
            expected_instance_id: handoff.source.instance_id.clone(),
            expected_agent_generation: handoff.source.agent_generation.clone(),
            transition_kind: TransitionKind::PlannedSwitchover,
            failover_safe_lsn: None,
            primary_write_status: AccessStatus::ReconfigurationPending,
            current_only: false,
            retire_build_ids: Vec::new(),
            retire_switchover_preparation_ids: vec![handoff.preparation()],
            switchover_handoff: Some(handoff),
        };
        coordinator.ensure_configuration(restore).await.unwrap();
        let grant = RuntimeEffect {
            operation_id: OperationId::new("grant-after-retirement"),
            sequence: store.load_state().await.unwrap().next_effect_sequence,
            action: RuntimeEffectAction::SetAccessStatus {
                read: AccessStatus::Granted,
                write: AccessStatus::Granted,
            },
        };
        store.begin_effect(&grant).await.unwrap();
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(30),
                pod.apply_effect(grant.clone())
            )
            .await
            .is_err()
        );
        if boundary == "registered" {
            std::process::exit(73);
        }
        let granting = pod.clone();
        let task = tokio::spawn(async move { granting.apply_effect(grant).await });
        let Some(kuberic_agent::hosting::OutboundReplication::Replication(item)) =
            pod.data_plane().next_outbound().await
        else {
            panic!("recovery replication")
        };
        let peer = CrashState::open(Path::new(&path).with_extension("peer"));
        acknowledge_recovery_item(&pod, &peer, item).await;
        task.await.unwrap().unwrap();
        std::process::exit(73);
    });
}

#[test]
fn real_handoff_configuration_boundaries_survive_process_termination() {
    for scenario in [
        "demotion",
        "promotion",
        "retirement-after",
        "target-current-only",
        "compensation-promotion",
        "compensation-completion",
        "restoration-after",
    ] {
        for boundary in [
            "intent",
            "authority",
            "application-role",
            "access",
            "receipt",
        ] {
            // Current-only and restoration deliberately omit role callbacks.
            if boundary == "application-role"
                && matches!(
                    scenario,
                    "retirement-after"
                        | "target-current-only"
                        | "compensation-completion"
                        | "restoration-after"
                )
            {
                continue;
            }
            if boundary == "authority" && scenario == "restoration-after" {
                continue;
            }
            let directory = tempdir().unwrap();
            let path = SqliteStore::metadata_database_path(directory.path());
            let output = Command::new(env::current_exe().unwrap())
                .args([
                    "--ignored",
                    "--exact",
                    "real_handoff_configuration_writer_process",
                ])
                .env("KUBERIC_HANDOFF_PATH", &path)
                .env("KUBERIC_HANDOFF_SCENARIO", scenario)
                .env("KUBERIC_HANDOFF_BOUNDARY", boundary)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(73),
                "{scenario}/{boundary}: {}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let store = Arc::new(SqliteStore::open_existing(&path, None).unwrap());
                let state = store.load_state().await.unwrap();
                let (_, command) = real_handoff_fixture(scenario);
                assert_ne!(state.write_status, AccessStatus::Granted);
                if boundary == "receipt" {
                    assert!(state.reconfiguration.is_none());
                    assert_eq!(state.retained_command.as_ref().unwrap().command, command);
                } else {
                    assert_eq!(state.reconfiguration.as_ref().unwrap().command, command);
                    assert_eq!(state.pending_effect.is_some(), boundary != "intent");
                }
                let application = Arc::new(CrashState::open(crash_application_path(&path)));
                assert!(
                    application
                        .verify_applied(&seeded_operation())
                        .await
                        .unwrap()
                );
                assert_eq!(
                    application.durable_progress().await.unwrap().committed_lsn,
                    7
                );
                let pod = Arc::new(PodRuntime::new(
                    state.identity.local_identity.clone(),
                    application.clone(),
                    store.clone(),
                ));
                let executor = Arc::new(CrashAfterRealEffect {
                    runtime: pod.clone(),
                    boundary: None,
                });
                let service = kuberic_agent::service::AgentService::new(
                    store.clone(),
                    pod.clone(),
                    executor.clone(),
                    Arc::<str>::from("crash-test"),
                )
                .unwrap();
                let address = || {
                    std::net::TcpListener::bind("127.0.0.1:0")
                        .unwrap()
                        .local_addr()
                        .unwrap()
                };
                let (ready_tx, mut ready_rx) = tokio::sync::watch::channel(false);
                let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
                let server =
                    tokio::spawn(service.serve(address(), address(), ready_tx, shutdown_rx));
                tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    ready_rx.wait_for(|ready| *ready),
                )
                .await
                .unwrap()
                .unwrap();
                let direct_client = pod.data_plane();
                assert!(
                    direct_client
                        .begin_write(kuberic_runtime::application::ClientWrite {
                            operation_id: OperationId::new("crash-retained-client"),
                            data: Bytes::from_static(b"forbidden"),
                        })
                        .await
                        .is_err()
                );
                tokio::time::timeout(std::time::Duration::from_secs(3), async {
                    while store.load_state().await.unwrap().reconfiguration.is_some() {
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    }
                })
                .await
                .unwrap_or_else(|_| {
                    panic!("{scenario}/{boundary}: startup recovery did not complete")
                });
                let coordinator = Coordinator::new(store.clone(), executor);
                let completed = tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    coordinator.ensure_configuration(command.clone()),
                )
                .await
                .unwrap()
                .unwrap_or_else(|error| panic!("{scenario}/{boundary}: {error}"));
                assert_eq!(
                    coordinator
                        .ensure_configuration(command.clone())
                        .await
                        .unwrap(),
                    completed
                );
                let recovered = store.load_state().await.unwrap();
                assert_eq!(recovered.highest_epoch, command.current_epoch);
                assert!(recovered.pending_effect.is_none() && recovered.reconfiguration.is_none());
                assert_ne!(pod.snapshot().await.write_status, AccessStatus::Granted);
                assert!(
                    application
                        .verify_applied(&seeded_operation())
                        .await
                        .unwrap()
                );
                assert_eq!(
                    application.durable_progress().await.unwrap().committed_lsn,
                    7
                );
                if !command.retire_switchover_preparation_ids.is_empty() {
                    assert!(recovered.prepared_switchover.is_none());
                    assert_eq!(recovered.retired_switchover, command.switchover_handoff);
                    assert_eq!(
                        recovered
                            .preparation_retirement
                            .as_ref()
                            .map(|retired| retired.generation),
                        command
                            .retire_switchover_preparation_ids
                            .first()
                            .map(|id| id.generation)
                    );
                }
                shutdown_tx.send_replace(true);
                server.await.unwrap().unwrap();
            });
        }
    }
}

#[test]
#[ignore = "helper process for real_handoff_configuration_boundaries_survive_process_termination"]
fn real_handoff_configuration_writer_process() {
    let (Ok(path), Ok(scenario), Ok(boundary)) = (
        env::var("KUBERIC_HANDOFF_PATH"),
        env::var("KUBERIC_HANDOFF_SCENARIO"),
        env::var("KUBERIC_HANDOFF_BOUNDARY"),
    ) else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let (state, command) = real_handoff_fixture(&scenario);
        let authority = AdmittedAuthority {
            local_identity: state.identity.local_identity.clone(),
            transition_kind: state
                .previous_configuration
                .as_ref()
                .map(|_| TransitionKind::PlannedSwitchover),
            previous_configuration: state.previous_configuration.clone(),
            current_configuration: state.current_configuration.clone().unwrap(),
            switchover_handoff: (state.highest_epoch.configuration_number > 1)
                .then(|| command.switchover_handoff.clone())
                .flatten(),
        };
        let store = Arc::new(SqliteStore::create_authorized(&path, state.clone()).unwrap());
        store.admit(&authority).await.unwrap();
        store
            .record_replication_progress(&ReplicationProgress {
                fence: authority.fence(),
                verified_lsn: 7,
            })
            .await
            .unwrap();
        let starting = switchover_configuration();
        store
            .record_replication_progress(&ReplicationProgress {
                fence: kuberic_runtime_internal::authority::AuthorityFence {
                    epoch: starting.epoch,
                    previous_configuration_id: None,
                    current_configuration_id: starting.configuration_id,
                },
                verified_lsn: 7,
            })
            .await
            .unwrap();
        let application = Arc::new(CrashState::open(crash_application_path(Path::new(&path))));
        for lsn in 1..=7 {
            application
                .apply(if lsn == 1 {
                    seeded_operation()
                } else {
                    Operation {
                        lsn,
                        committed_lsn: lsn,
                        data: Bytes::from(format!("acknowledged-{lsn}")),
                    }
                })
                .await
                .unwrap();
        }
        application.commit(7).await.unwrap();
        let pod = Arc::new(PodRuntime::new(
            state.identity.local_identity,
            application,
            store.clone(),
        ));
        pod.reconstruct(
            OpenMode::Existing,
            state.role,
            state.read_status,
            state.write_status,
            None,
        )
        .await
        .unwrap();
        if boundary == "intent" {
            store.begin_configuration(&command).await.unwrap();
        } else {
            let executor = Arc::new(CrashAfterRealEffect {
                runtime: pod,
                boundary: Some(boundary.clone()),
            });
            tokio::time::timeout(
                std::time::Duration::from_secs(3),
                Coordinator::new(store, executor).ensure_configuration(command),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                boundary, "receipt",
                "requested effect boundary was not reached"
            );
        }
        std::process::exit(73);
    });
}

#[test]
fn real_runtime_effect_recovers_and_completes_after_process_termination() {
    for boundary in ["pending-effect", "effect-complete-before-stage-advance"] {
        let directory = tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let output = Command::new(env::current_exe().unwrap())
            .args(["--ignored", "--exact", "real_runtime_effect_writer_process"])
            .env("KUBERIC_REAL_RUNTIME_PATH", &path)
            .env("KUBERIC_REAL_RUNTIME_BOUNDARY", boundary)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "real runtime child failed at {boundary}: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let store = Arc::new(
                SqliteStore::open_existing(&path, Some(&single_storage_identity())).unwrap(),
            );
            let state = store.load_state().await.unwrap();
            assert_eq!(
                state.reconfiguration.as_ref().map(|record| record.stage),
                Some(CoordinatorStage::AdmitAuthority)
            );
            match boundary {
                "pending-effect" => {
                    assert_eq!(
                        state.pending_effect.as_ref().map(|pending| pending.stage),
                        Some(EffectStage::IntentCommitted)
                    );
                    assert!(state.retained_result.is_some());
                }
                "effect-complete-before-stage-advance" => {
                    assert!(state.pending_effect.is_none());
                    assert!(state.retained_result.is_some());
                }
                _ => unreachable!(),
            }

            let application_path = crash_application_path(&path);
            let (pod, application) = real_runtime(store.clone(), &application_path);
            assert!(
                application
                    .verify_applied(&seeded_operation())
                    .await
                    .unwrap()
            );
            assert_eq!(
                application.durable_progress().await.unwrap(),
                DurableApplicationProgress {
                    applied_lsn: 1,
                    committed_lsn: 1,
                }
            );
            pod.reconstruct(
                OpenMode::Existing,
                state.role,
                state.read_status,
                state.write_status,
                None,
            )
            .await
            .unwrap();
            let coordinator = Coordinator::new(store.clone(), pod);
            coordinator
                .ensure_configuration(real_configuration_command())
                .await
                .unwrap();
            coordinator
                .ensure_configuration(grant_access_command())
                .await
                .unwrap();

            let recovered = store.load_state().await.unwrap();
            assert!(recovered.reconfiguration.is_none());
            assert_eq!(
                recovered.retained_command.unwrap().command,
                grant_access_command()
            );
            assert_eq!(recovered.role, ReplicaRole::Primary);
            assert_eq!(recovered.read_status, AccessStatus::Granted);
            assert_eq!(recovered.write_status, AccessStatus::Granted);
            assert!(
                application
                    .verify_applied(&seeded_operation())
                    .await
                    .unwrap()
            );
        });
    }
}

#[test]
#[ignore = "helper process for sqlite_commits_survive_process_termination_without_destructors"]
fn crash_writer_process() {
    let Ok(path) = env::var("KUBERIC_CRASH_WRITER_PATH") else {
        return;
    };
    let store = SqliteStore::create_authorized(path, AgentState::new(storage_identity())).unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(store.begin_effect(&effect())).unwrap();
    std::process::exit(0);
}

#[test]
#[ignore = "helper process for configuration_boundaries_survive_process_termination_without_destructors"]
fn crash_boundary_writer_process() {
    let (Ok(path), Ok(boundary)) = (
        env::var("KUBERIC_CRASH_WRITER_PATH"),
        env::var("KUBERIC_CRASH_BOUNDARY"),
    ) else {
        return;
    };
    let store = SqliteStore::create_authorized(path, AgentState::new(storage_identity())).unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        match boundary.as_str() {
            "pending-effect" => {
                assert_eq!(
                    store.begin_effect(&effect()).await.unwrap(),
                    BeginEffect::Execute(effect())
                );
            }

            "effect-complete-before-stage-advance" => {
                assert!(matches!(
                    store
                        .begin_configuration(&configuration_command())
                        .await
                        .unwrap(),
                    BeginConfiguration::Execute(_)
                ));
                store.begin_effect(&effect()).await.unwrap();
                store.mark_effect_applied(&effect()).await.unwrap();
                store.complete_effect(&result()).await.unwrap();
            }
            "enclosing-command" => {
                assert!(matches!(
                    store
                        .begin_configuration(&configuration_command())
                        .await
                        .unwrap(),
                    BeginConfiguration::Execute(_)
                ));
            }
            "terminal-command" => {
                let command = configuration_command();
                store.begin_configuration(&command).await.unwrap();
                store
                    .advance_configuration(
                        &command.operation_id,
                        CoordinatorStage::AdmitAuthority,
                        CoordinatorStage::Complete,
                        None,
                    )
                    .await
                    .unwrap();
                store
                    .complete_configuration(&command.operation_id)
                    .await
                    .unwrap();
            }
            _ => panic!("unknown crash boundary {boundary}"),
        }
    });
    std::process::exit(0);
}

#[test]
#[ignore = "helper process for switchover_preparation_boundaries_survive_process_termination"]
fn switchover_preparation_writer_process() {
    let (Ok(path), Ok(boundary)) = (
        env::var("KUBERIC_SWITCHOVER_PATH"),
        env::var("KUBERIC_SWITCHOVER_BOUNDARY"),
    ) else {
        return;
    };
    let store = SqliteStore::create_authorized(path, AgentState::new(storage_identity())).unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let effect = switchover_effect();
        store.begin_effect(&effect).await.unwrap();
        match boundary.as_str() {
            "pending-effect" => {}
            "effect-applied" => {
                store.mark_effect_applied(&effect).await.unwrap();
            }
            "effect-completed" => {
                store.mark_effect_applied(&effect).await.unwrap();
                store.complete_effect(&switchover_result()).await.unwrap();
            }
            _ => panic!("unknown switchover boundary {boundary}"),
        }
    });
    std::process::exit(0);
}

#[test]
#[ignore = "helper process for real_runtime_switchover_preparation_recovers_after_process_termination"]
fn real_switchover_preparation_writer_process() {
    let (Ok(path), Ok(boundary)) = (
        env::var("KUBERIC_REAL_SWITCHOVER_PATH"),
        env::var("KUBERIC_REAL_SWITCHOVER_BOUNDARY"),
    ) else {
        return;
    };
    let configuration = switchover_configuration();
    let mut state = AgentState::new(switchover_storage_identity());
    state.highest_epoch = configuration.epoch;
    state.current_configuration = Some(configuration.clone());
    state.role = ReplicaRole::Primary;
    state.read_status = AccessStatus::Granted;
    state.write_status = AccessStatus::Granted;
    let store = Arc::new(SqliteStore::create_authorized(&path, state).unwrap());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let authority = AdmittedAuthority {
            local_identity: switchover_storage_identity().local_identity,
            transition_kind: None,
            previous_configuration: None,
            current_configuration: configuration,
            switchover_handoff: None,
        };
        store.admit(&authority).await.unwrap();
        let application_path = crash_application_path(Path::new(&path));
        let (pod, application) = switchover_real_runtime(store.clone(), &application_path);
        application.apply(seeded_operation()).await.unwrap();
        application.commit(1).await.unwrap();
        pod.reconstruct(
            OpenMode::Existing,
            ReplicaRole::Primary,
            AccessStatus::Granted,
            AccessStatus::Granted,
            None,
        )
        .await
        .unwrap();
        let command = switchover_prepare_command();
        let effect = switchover_runtime_effect(
            &command,
            store.load_state().await.unwrap().next_effect_sequence,
        );
        match boundary.as_str() {
            "pending-effect" => {
                store.begin_effect(&effect).await.unwrap();
            }
            "effect-applied" => {
                store.begin_effect(&effect).await.unwrap();
                pod.apply_effect(effect.clone()).await.unwrap();
                store.mark_effect_applied(&effect).await.unwrap();
            }
            "effect-completed" => {
                Coordinator::new(store.clone(), pod)
                    .ensure_switchover_prepared(command)
                    .await
                    .unwrap();
            }
            _ => panic!("unknown real switchover boundary {boundary}"),
        }
    });
    std::process::exit(0);
}

#[test]
#[ignore = "helper process for real_runtime_effect_recovers_and_completes_after_process_termination"]
fn real_runtime_effect_writer_process() {
    let (Ok(path), Ok(boundary)) = (
        env::var("KUBERIC_REAL_RUNTIME_PATH"),
        env::var("KUBERIC_REAL_RUNTIME_BOUNDARY"),
    ) else {
        return;
    };
    let store = Arc::new(
        SqliteStore::create_authorized(&path, AgentState::new(single_storage_identity())).unwrap(),
    );
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let application_path = crash_application_path(Path::new(&path));
        let (pod, application) = real_runtime(store.clone(), &application_path);
        application.apply(seeded_operation()).await.unwrap();
        application.commit(1).await.unwrap();
        let coordinator = Coordinator::new(store.clone(), pod.clone());
        coordinator
            .open_runtime(
                OpenMode::Existing,
                &kuberic_protocol::types::ProcessSessionId::new("real-runtime-session"),
            )
            .await
            .unwrap();
        let command = real_configuration_command();
        store.begin_configuration(&command).await.unwrap();
        let authority = admit_configuration(&command, &store.load_state().await.unwrap()).unwrap();
        let sequence = store.load_state().await.unwrap().next_effect_sequence;
        let effect = RuntimeEffect {
            operation_id: OperationId::new("real-runtime-configuration:admit-authority"),
            sequence,
            action: RuntimeEffectAction::AdmitAuthority(Box::new(authority)),
        };
        match boundary.as_str() {
            "pending-effect" => {
                store.begin_effect(&effect).await.unwrap();
            }
            "effect-complete-before-stage-advance" => {
                RuntimeAdapter::new(store, pod)
                    .execute(effect)
                    .await
                    .unwrap();
            }
            _ => panic!("unknown real runtime boundary {boundary}"),
        }
    });
    std::process::exit(0);
}
