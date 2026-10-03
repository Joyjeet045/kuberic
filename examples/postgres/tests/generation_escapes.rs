use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use kuberic_agent::store::AgentStore;
use kuberic_protocol::types::{AccessStatus, FaultType, ProcessSessionId, ResourceUid};
use kuberic_runtime_internal::effects::RuntimeEffectAction;
use postgres_replicated::access::{PgAccessController, initialize_application_role};
use postgres_replicated::durable::{PgDurableIdentity, PgDurableStore, StorageMode};
use postgres_replicated::instance::PgInstanceManager;
use postgres_replicated::native::{
    PgNativeObserver, PolicyStage, compile_synchronous_configuration,
};
use postgres_replicated::testing::{
    PgPod, ProcessProbe, TestDataDir, allocate_port, find_pg_bin, native_configuration,
    native_identity, wrapped_pg_bin,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retired_error_inside_sf_effect_cannot_abort_replacement_or_publish_success() {
    let root = TestDataDir::new("effect-old");
    let armed = root.path().join("malformed");
    let bin = wrapped_pg_bin(
        root.path(),
        "pg_controldata",
        &format!(
            "#!/bin/sh\nif test -f '{}'; then\n echo malformed\nelse\n exec '{}/pg_controldata' \"$@\"\nfi\n",
            armed.display(),
            find_pg_bin().display()
        ),
    );
    let pod =
        Arc::new(PgPod::with_bin(root.path().join("p"), native_identity(1, "effect"), bin).await);
    pod.singleton().await;
    for _ in 0..3 {
        std::fs::write(&armed, b"").unwrap();
        let instance = pod.application.instance();
        let gate = instance.pause_error_handling();
        let before = pod.store.load_state().await.unwrap();
        let old = tokio::spawn({
            let pod = pod.clone();
            async move {
                pod.effect(RuntimeEffectAction::SetAccessStatus {
                    read: AccessStatus::Granted,
                    write: AccessStatus::Granted,
                })
                .await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        std::fs::remove_file(&armed).unwrap();
        let access = PgAccessController::new(instance);
        access.close_external().await.unwrap();
        access.grant_role_access().await.unwrap();
        let (sql, connection) = instance.connect_application().await.unwrap();
        sql.simple_query("SELECT 1").await.unwrap();
        gate.release.notify_one();
        let old = old.await.unwrap();
        assert!(
            matches!(
                old,
                Err(kuberic_agent::AgentError::Runtime(
                    kuberic_runtime::RuntimeError::OperationCancelled
                ))
            ),
            "{old:?}"
        );
        sql.simple_query("SELECT 1").await.unwrap();
        assert!(instance.is_running().await);
        assert_eq!(pod.runtime.partition_report().await.reported_fault, None);
        let after = pod.store.load_state().await.unwrap();
        assert!(after.pending_effect.is_none());
        assert_eq!(after.next_effect_sequence, before.next_effect_sequence);
        assert_eq!(after.retained_result, before.retained_result);
        pod.effect(RuntimeEffectAction::RefreshApplicationProgress)
            .await
            .unwrap();
        drop(sql);
        connection.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_out_sf_effect_does_not_abort_its_retryable_successor() {
    let root = TestDataDir::new("effect-time");
    let armed = root.path().join("stalled");
    let bin = wrapped_pg_bin(
        root.path(),
        "pg_controldata",
        &format!(
            "#!/bin/sh\nif test -f '{}'; then\n sleep 30\nelse\n exec '{}/pg_controldata' \"$@\"\nfi\n",
            armed.display(),
            find_pg_bin().display()
        ),
    );
    let pod = PgPod::with_bin(root.path().join("p"), native_identity(1, "timeout"), bin).await;
    pod.singleton().await;
    let owned = ProcessProbe::postgres(pod.application.instance().data_dir());
    std::fs::write(&armed, b"").unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        pod.effect(RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::Granted,
            write: AccessStatus::Granted,
        }),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(matches!(
        error,
        kuberic_agent::AgentError::Runtime(kuberic_runtime::RuntimeError::OperationCancelled)
    ));
    owned.assert_reaped();
    assert_eq!(
        pod.runtime.partition_report().await.reported_fault,
        Some(FaultType::Transient)
    );
    std::fs::remove_file(armed).unwrap();
    for _ in 0..3 {
        pod.effect(RuntimeEffectAction::RefreshApplicationProgress)
            .await
            .unwrap();
    }
    assert_eq!(
        pod.runtime.partition_report().await.reported_fault,
        Some(FaultType::Transient)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retired_policy_cannot_mutate_or_certify_replacement_at_any_transaction_cut() {
    let root = TestDataDir::new("policy-owner");
    let local = native_identity(1, "local");
    let standby = native_identity(2, "absent");
    let instance = Arc::new(PgInstanceManager::new(
        root.path().join("pgdata"),
        find_pg_bin(),
        allocate_port().await,
    ));
    instance.init_db().await.unwrap();
    let (faults, _receiver) = tokio::sync::mpsc::channel(8);
    instance.start_native(faults).await.unwrap();
    initialize_application_role(&instance).await.unwrap();
    let access = PgAccessController::new(&instance);
    access.grant_role_access().await.unwrap();
    let store = Arc::new(
        PgDurableStore::open(
            root.path().join("meta"),
            PgDurableIdentity {
                resource_uid: ResourceUid::new("policy"),
                replica: local.clone(),
            },
            StorageMode::Fresh,
        )
        .await
        .unwrap(),
    );
    let observer = Arc::new(PgNativeObserver::with_store(instance.clone(), store.clone()).await);
    let old_policy = compile_synchronous_configuration(
        None,
        &native_configuration(&[local.clone(), standby.clone()], 0, 1),
        &local,
        &BTreeMap::from([(standby, ProcessSessionId::new("retired"))]),
        true,
    )
    .unwrap();
    let current_policy = compile_synchronous_configuration(
        None,
        &native_configuration(std::slice::from_ref(&local), 0, 2),
        &local,
        &BTreeMap::new(),
        true,
    )
    .unwrap();
    for stage in [
        PolicyStage::InvalidationStarted,
        PolicyStage::Invalidated,
        PolicyStage::Applied,
        PolicyStage::ReadBack,
        PolicyStage::Published,
    ] {
        let gate = observer.pause_policy(stage);
        let old = tokio::spawn({
            let observer = observer.clone();
            let policy = old_policy.clone();
            async move { observer.apply_synchronous(policy).await }
        });
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        access.close_external().await.unwrap();
        access.grant_role_access().await.unwrap();
        let before = store.snapshot().await;
        let (sql, connection) = instance.connect().await.unwrap();
        let setting: String = sql
            .query_one("SHOW synchronous_standby_names", &[])
            .await
            .unwrap()
            .get(0);
        if matches!(
            stage,
            PolicyStage::InvalidationStarted | PolicyStage::Invalidated
        ) {
            assert_eq!(setting, "", "{stage:?}");
        }
        let before_setting = setting;
        gate.release.notify_one();
        assert!(old.await.unwrap().is_err(), "{stage:?}");
        assert_eq!(store.revalidate().await.unwrap(), before, "{stage:?}");
        let setting: String = sql
            .query_one("SHOW synchronous_standby_names", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(setting, before_setting, "{stage:?}");
        let fresh = PgNativeObserver::with_store(instance.clone(), store.clone()).await;
        assert!(
            !fresh
                .snapshot()
                .await
                .unwrap()
                .evidence
                .unwrap()
                .synchronous
                .is_some_and(|p| p.valid)
        );
        observer
            .apply_synchronous(current_policy.clone())
            .await
            .unwrap();
        assert!(
            fresh
                .snapshot()
                .await
                .unwrap()
                .evidence
                .unwrap()
                .synchronous
                .unwrap()
                .valid
        );
        drop(sql);
        connection.await.unwrap();
    }
    instance.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_policy_readback_releases_lifecycle_for_bounded_owned_cleanup() {
    let root = TestDataDir::new("policy-stall");
    let instance = Arc::new(PgInstanceManager::new(
        root.path().join("pgdata"),
        find_pg_bin(),
        allocate_port().await,
    ));
    instance.init_db().await.unwrap();
    let (faults, _receiver) = tokio::sync::mpsc::channel(8);
    instance.start_native(faults).await.unwrap();
    let observer = Arc::new(PgNativeObserver::new(instance.clone()));
    let local = native_identity(1, "stall");
    let policy = compile_synchronous_configuration(
        None,
        &native_configuration(std::slice::from_ref(&local), 0, 1),
        &local,
        &BTreeMap::new(),
        true,
    )
    .unwrap();
    let gate = observer.pause_policy(PolicyStage::Applied);
    let apply = tokio::spawn({
        let observer = observer.clone();
        async move { observer.apply_synchronous(policy).await }
    });
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    let owned = ProcessProbe::postgres(instance.data_dir());
    owned.signal(rustix::process::Signal::STOP);
    gate.release.notify_one();
    let error = tokio::time::timeout(Duration::from_secs(7), apply)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.is_timeout(), "{error}");
    assert_eq!(error.fault_type(), FaultType::Transient);
    tokio::time::timeout(Duration::from_secs(10), instance.stop())
        .await
        .unwrap()
        .unwrap();
    owned.assert_reaped();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_generation_exhaustion_reaps_and_cannot_restart_or_wrap() {
    let root = TestDataDir::new("process-max");
    let pod = PgPod::new(root.path().join("p"), native_identity(1, "max")).await;
    pod.singleton().await;
    let instance = pod.application.instance();
    let owned = ProcessProbe::postgres(instance.data_dir());
    instance.advance_generation_for_test(u64::MAX - 1).unwrap();
    instance.stop().await.unwrap();
    owned.assert_reaped();
    assert_eq!(instance.process_generation_for_test(), u64::MAX);
    let (faults, _receiver) = tokio::sync::mpsc::channel(8);
    let error = instance.start_native(faults.clone()).await.unwrap_err();
    assert!(error.to_string().contains("generation exhausted"));
    assert_eq!(error.fault_type(), FaultType::Permanent);
    assert!(instance.start_native(faults).await.is_err());
    assert_eq!(instance.process_generation_for_test(), u64::MAX);
    assert!(!instance.is_running().await);
    assert!(instance.connect_application().await.is_err());
}
