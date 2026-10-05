use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;

use crate::application::OpenMode;
use crate::effects::{
    RuntimeEffect, RuntimeEffectAction, RuntimeEffectResult, RuntimePostcondition, RuntimeSnapshot,
};
use crate::host::Result;
use crate::host::hosting::empty_snapshot;
use crate::host::recovery::{RecoveryDecision, inspect_recovery, recover_pending};
use crate::host::runtime_adapter::{RuntimeAdapter, RuntimeEffectExecutor};
use crate::host::session::ProcessSession;
use crate::host::sqlite_store::SqliteStore;
use crate::host::state::{AgentState, SCHEMA_VERSION, StorageIdentity};
use crate::host::store::{AgentStore, BeginEffect};
use crate::protocol::types::{
    AccessStatus, AgentGeneration, EffectivePolicy, InitializationId, OperationId, PodUid, PvcUid,
    ReplicaId, ReplicaIdentity, ReplicaInstanceId, ResourceUid,
};

use super::tempdir;

fn initial_state() -> AgentState {
    AgentState::new(StorageIdentity {
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
    })
}

fn effect() -> RuntimeEffect {
    RuntimeEffect {
        operation_id: OperationId::new("effect-1"),
        sequence: 1,
        action: RuntimeEffectAction::Open(OpenMode::Existing),
    }
}

fn snapshot() -> RuntimeSnapshot {
    empty_snapshot(initial_state().identity.local_identity)
}

fn result() -> RuntimeEffectResult {
    RuntimeEffectResult {
        operation_id: effect().operation_id,
        sequence: 1,
        topology_receipt: None,
        postcondition: RuntimePostcondition {
            prepared_secondary_removal: None,
            retired_authority: None,
            accepted_secondary_removal: None,
            open: true,
            role: snapshot().role,
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

#[derive(Default)]
struct RecoveringRuntime {
    calls: AtomicUsize,
}

#[async_trait]
impl RuntimeEffectExecutor for RecoveringRuntime {
    async fn apply_runtime_effect(&self, _: RuntimeEffect) -> Result<RuntimeEffectResult> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(result())
    }
}

#[tokio::test]
async fn committed_and_applied_intents_recover_without_losing_retained_completion() {
    for applied in [false, true] {
        let directory = tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let store = SqliteStore::create_authorized(&path, initial_state()).unwrap();
        assert_eq!(
            store.begin_effect(&effect()).await.unwrap(),
            BeginEffect::Execute(effect())
        );
        if applied {
            store.mark_effect_applied(&effect()).await.unwrap();
        }
        drop(store);

        let reopened = Arc::new(SqliteStore::open_existing(&path, None).unwrap());
        assert_eq!(
            inspect_recovery(reopened.as_ref(), &snapshot())
                .await
                .unwrap(),
            RecoveryDecision::Reissue(Box::new(effect()))
        );
        let runtime = Arc::new(RecoveringRuntime::default());
        let adapter = RuntimeAdapter::new(reopened.clone(), runtime.clone());
        assert_eq!(
            recover_pending(&adapter, &snapshot()).await.unwrap(),
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
        reopened
            .set_reconfiguration(Some("transition-stage".into()))
            .await
            .unwrap();
        reopened.clear_reconfiguration().await.unwrap();
        drop(adapter);
        drop(reopened);
        let reopened = SqliteStore::open_existing(&path, None).unwrap();
        assert!(
            reopened
                .load_state()
                .await
                .unwrap()
                .reconfiguration_data
                .is_none()
        );
        assert_eq!(
            reopened.retained_result().await.unwrap().unwrap().result,
            result()
        );
    }
}

#[tokio::test]
async fn recovery_starts_idle_and_requires_closed_writes() {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let store = Arc::new(SqliteStore::create_authorized(&path, initial_state()).unwrap());
    let runtime = Arc::new(RecoveringRuntime::default());
    let adapter = RuntimeAdapter::new(store.clone(), runtime.clone());
    assert_eq!(recover_pending(&adapter, &snapshot()).await.unwrap(), None);
    let mut writable = snapshot();
    writable.write_status = AccessStatus::Granted;
    assert!(matches!(
        inspect_recovery(store.as_ref(), &writable).await,
        Err(crate::host::HostError::DurableEffectConflict(_))
    ));
    assert_eq!(runtime.calls.load(Ordering::SeqCst), 0);
    adapter.execute(effect()).await.unwrap();
    assert_eq!(
        recover_pending(&adapter, &snapshot()).await.unwrap(),
        Some(result())
    );
    assert_eq!(runtime.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn process_sessions_fence_restarts_without_changing_durable_generation() {
    let first = ProcessSession::new();
    let second = ProcessSession::new();
    assert_ne!(first.id(), second.id());
    assert_eq!(first.next_report_sequence(), 1);
    assert_eq!(first.next_report_sequence(), 2);
    assert_eq!(
        initial_state().identity.local_identity.agent_generation,
        AgentGeneration::new("generation-1")
    );
}

#[test]
fn sqlite_completion_survives_process_exit_without_destructors() {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            &format!(
                "{}::crash_writer",
                module_path!().split_once("::").unwrap().1
            ),
        ])
        .env("KUBERIC_HOST_CRASH_WRITER_PATH", &path)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(73), "{output:?}");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let store = SqliteStore::open_existing(&path, None).unwrap();
        assert_eq!(store.identity().await.unwrap(), initial_state().identity);
        assert!(store.load_state().await.unwrap().pending_effect.is_none());
        assert_eq!(
            inspect_recovery(&store, &snapshot()).await.unwrap(),
            RecoveryDecision::ReturnRetained(Box::new(
                store.retained_result().await.unwrap().unwrap()
            ))
        );
        assert_eq!(
            store.retained_result().await.unwrap().unwrap().result,
            result()
        );
    });
}

#[test]
#[ignore = "helper process for sqlite_completion_survives_process_exit_without_destructors"]
fn crash_writer() {
    let path = std::env::var_os("KUBERIC_HOST_CRASH_WRITER_PATH").expect("crash writer path");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let store = Arc::new(SqliteStore::create_authorized(path, initial_state()).unwrap());
        RuntimeAdapter::new(store, Arc::new(RecoveringRuntime::default()))
            .execute(effect())
            .await
            .unwrap();
    });
    std::process::exit(73);
}
