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
use kuberic_protocol::command::EnsureConfiguration;
use kuberic_protocol::types::{
    AccessStatus, AgentGeneration, ConfigurationDescriptor, ConfigurationMember, EffectivePolicy,
    Epoch, InitializationId, OperationId, PodUid, PvcUid, ReplicaId, ReplicaIdentity,
    ReplicaInstanceId, ReplicaRole, ResourceUid, TransitionKind,
};
use kuberic_runtime::application::{
    CopyChunk, DurableApplicationAck, DurableApplicationProgress, OpenContext, Operation,
    OperationDataStream, RoleChange, StateProvider, StatefulServiceReplica,
};
use kuberic_runtime::engine::{DurableState, RetainedOperationStream};
use kuberic_runtime::replicator::{DefaultReplicatorFactory, Replicator, ReplicatorSettings};
use kuberic_runtime::{Result as RuntimeResult, RuntimeError};
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
        RecoveryDecision::Reissue(effect())
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
