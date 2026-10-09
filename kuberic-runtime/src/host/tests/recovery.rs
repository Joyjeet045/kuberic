use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;

use crate::application::OpenMode;
use crate::effects::{
    ProcessCompletion, RuntimeEffect, RuntimeEffectAction, RuntimeEffectOutcome,
    RuntimeEffectResult, RuntimeSnapshot,
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
        outcome: RuntimeEffectOutcome::Opened,
    }
}

#[test]
fn load_and_fault_observations_do_not_change_access_recovery_eligibility() {
    let state = initial_state();
    let mut observed = state.clone();
    observed
        .load_metrics
        .push(crate::protocol::types::LoadMetric {
            name: "queue-depth".into(),
            value: 9,
        });
    observed.reported_fault = Some(crate::protocol::types::FaultType::Transient);
    assert!(crate::host::recovery::same_recovery_eligibility(
        &state, &observed
    ));
}

#[derive(Default)]
struct RecoveringRuntime {
    calls: AtomicUsize,
}

struct AppliedRecoveryRuntime {
    calls: AtomicUsize,
    discards: AtomicUsize,
    result: RuntimeEffectResult,
}

#[async_trait]
impl RuntimeEffectExecutor for AppliedRecoveryRuntime {
    async fn apply_runtime_effect(&self, _: RuntimeEffect) -> Result<RuntimeEffectResult> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.result.clone())
    }

    async fn discard_runtime_effect(&self, _: &RuntimeEffect) -> Result<()> {
        self.discards.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
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
            store
                .mark_effect_applied(&effect(), &result())
                .await
                .unwrap();
        }

        drop(store);

        let reopened = Arc::new(SqliteStore::open_existing(&path, None).unwrap());
        assert_eq!(
            inspect_recovery(reopened.as_ref(), &snapshot().into())
                .await
                .unwrap(),
            RecoveryDecision::Reissue(Box::new(effect()))
        );
        let runtime = Arc::new(RecoveringRuntime::default());
        let adapter = RuntimeAdapter::new(reopened.clone(), runtime.clone());
        assert_eq!(
            recover_pending(&adapter, &snapshot().into()).await.unwrap(),
            Some(result())
        );
        assert_eq!(runtime.calls.load(Ordering::SeqCst), usize::from(!applied));
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
async fn applied_close_and_abort_reexecute_live_state_before_retention() {
    for (index, action) in [RuntimeEffectAction::Close, RuntimeEffectAction::Abort]
        .into_iter()
        .enumerate()
    {
        let directory = tempdir().unwrap();
        let path = SqliteStore::metadata_database_path(directory.path());
        let store = Arc::new(SqliteStore::create_authorized(&path, initial_state()).unwrap());
        let effect = RuntimeEffect {
            operation_id: OperationId::new(format!("terminal-{index}")),
            sequence: 1,
            action,
        };
        let completion = ProcessCompletion {
            open: false,
            role: crate::protocol::types::ReplicaRole::None,
            read_status: AccessStatus::NotPrimary,
            write_status: AccessStatus::NotPrimary,
            authority: None,
        };
        let outcome = if matches!(effect.action, RuntimeEffectAction::Close) {
            RuntimeEffectOutcome::Closed(completion)
        } else {
            RuntimeEffectOutcome::Aborted(completion)
        };
        let result = RuntimeEffectResult {
            operation_id: effect.operation_id.clone(),
            sequence: effect.sequence,
            outcome,
        };
        store.begin_effect(&effect).await.unwrap();
        store.mark_effect_applied(&effect, &result).await.unwrap();
        let runtime = Arc::new(AppliedRecoveryRuntime {
            calls: AtomicUsize::new(0),
            discards: AtomicUsize::new(0),
            result: result.clone(),
        });
        let adapter = RuntimeAdapter::new(store.clone(), runtime.clone());
        assert_eq!(adapter.execute(effect).await.unwrap(), result);
        assert_eq!(runtime.calls.load(Ordering::SeqCst), 1);
        assert!(store.load_state().await.unwrap().pending_effect.is_none());
    }
}

#[tokio::test]
async fn rejected_fresh_completion_discards_the_process_local_result() {
    use crate::authority::{AdmittedAuthority, ReplicaAuthorityStore};

    let intent = crate::removal_fixture::intent(&[1, 2, 3], 1);
    let mut state = initial_state();
    state.identity.local_identity = intent.primary.clone();
    state.current_configuration = Some(intent.previous_configuration.clone());
    state.highest_epoch = intent.previous_configuration.epoch;
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let store = Arc::new(SqliteStore::create_authorized(&path, state).unwrap());
    let durable = AdmittedAuthority {
        local_identity: intent.primary.clone(),
        transition_kind: None,
        previous_configuration: None,
        current_configuration: intent.previous_configuration.clone(),
        switchover_handoff: None,
        scale_up: None,
        secondary_removal: None,
    };
    store.admit(&durable).await.unwrap();
    let mut stale = durable.clone();
    stale.current_configuration.epoch.configuration_number += 1;
    stale.current_configuration.configuration_id = stale.current_configuration.expected_id();
    let effect = RuntimeEffect {
        operation_id: OperationId::new("stale-catch-up"),
        sequence: 1,
        action: RuntimeEffectAction::WaitForCatchup,
    };
    let result = RuntimeEffectResult {
        operation_id: effect.operation_id.clone(),
        sequence: effect.sequence,
        outcome: RuntimeEffectOutcome::CatchUpCompleted(crate::effects::CatchUpCompletion {
            authority: Some(stale),
            boundary_lsn: 9,
        }),
    };
    let runtime = Arc::new(AppliedRecoveryRuntime {
        calls: AtomicUsize::new(0),
        discards: AtomicUsize::new(0),
        result,
    });
    let adapter = RuntimeAdapter::new(store.clone(), runtime.clone());
    assert!(adapter.execute(effect).await.is_err());
    assert_eq!(runtime.calls.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.discards.load(Ordering::SeqCst), 1);
    assert!(
        store
            .load_state()
            .await
            .unwrap()
            .pending_effect
            .as_ref()
            .is_some_and(|pending| pending.applied_result.is_some())
    );
}

#[tokio::test]
async fn rejected_applied_completion_discards_the_process_local_result() {
    use crate::authority::{AdmittedAuthority, ReplicaAuthorityStore};

    let intent = crate::removal_fixture::intent(&[1, 2, 3], 1);
    let mut state = initial_state();
    state.identity.local_identity = intent.primary.clone();
    state.current_configuration = Some(intent.previous_configuration.clone());
    state.highest_epoch = intent.previous_configuration.epoch;
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let store = Arc::new(SqliteStore::create_authorized(&path, state).unwrap());
    let durable = AdmittedAuthority {
        local_identity: intent.primary.clone(),
        transition_kind: None,
        previous_configuration: None,
        current_configuration: intent.previous_configuration.clone(),
        switchover_handoff: None,
        scale_up: None,
        secondary_removal: None,
    };
    store.admit(&durable).await.unwrap();
    let mut stale = durable.clone();
    stale.current_configuration.epoch.configuration_number += 1;
    stale.current_configuration.configuration_id = stale.current_configuration.expected_id();
    let effect = RuntimeEffect {
        operation_id: OperationId::new("applied-stale-catch-up"),
        sequence: 1,
        action: RuntimeEffectAction::WaitForCatchup,
    };
    let result = RuntimeEffectResult {
        operation_id: effect.operation_id.clone(),
        sequence: effect.sequence,
        outcome: RuntimeEffectOutcome::CatchUpCompleted(crate::effects::CatchUpCompletion {
            authority: Some(stale),
            boundary_lsn: 9,
        }),
    };
    store.begin_effect(&effect).await.unwrap();
    store.mark_effect_applied(&effect, &result).await.unwrap();
    let runtime = Arc::new(AppliedRecoveryRuntime {
        calls: AtomicUsize::new(0),
        discards: AtomicUsize::new(0),
        result,
    });
    let adapter = RuntimeAdapter::new(store.clone(), runtime.clone());
    assert!(adapter.execute(effect).await.is_err());
    assert_eq!(runtime.calls.load(Ordering::SeqCst), 0);
    assert_eq!(runtime.discards.load(Ordering::SeqCst), 1);
    assert!(
        store
            .load_state()
            .await
            .unwrap()
            .pending_effect
            .as_ref()
            .is_some_and(|pending| pending.applied_result.is_some())
    );
}

#[tokio::test]
async fn recovery_starts_idle_and_requires_closed_writes() {
    let directory = tempdir().unwrap();
    let path = SqliteStore::metadata_database_path(directory.path());
    let store = Arc::new(SqliteStore::create_authorized(&path, initial_state()).unwrap());
    let runtime = Arc::new(RecoveringRuntime::default());
    let adapter = RuntimeAdapter::new(store.clone(), runtime.clone());
    assert_eq!(
        recover_pending(&adapter, &snapshot().into()).await.unwrap(),
        None
    );
    let mut writable = snapshot();
    writable.write_status = AccessStatus::Granted;
    assert!(matches!(
        inspect_recovery(store.as_ref(), &writable.into()).await,
        Err(crate::host::HostError::DurableEffectConflict(_))
    ));
    assert_eq!(runtime.calls.load(Ordering::SeqCst), 0);
    adapter.execute(effect()).await.unwrap();
    assert_eq!(
        recover_pending(&adapter, &snapshot().into()).await.unwrap(),
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
            inspect_recovery(&store, &snapshot().into()).await.unwrap(),
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
