use std::collections::BTreeMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use kuberic_protocol::types::{
    AgentGeneration, ConfigurationDescriptor, ConfigurationMember, Epoch, ProcessSessionId,
    ReplicaId, ReplicaIdentity, ReplicaInstanceId, ReplicaRole, ResourceUid,
};
use postgres_replicated::access::{
    PgAccessController, application_role_uses_remote_apply, initialize_application_role, stop_fence,
};
use postgres_replicated::durable::{
    PgDurableError, PgDurableIdentity, PgDurableStore, StorageMode,
};
use postgres_replicated::instance::PgInstanceManager;
use postgres_replicated::instance::PgProcessState;
use postgres_replicated::native::{
    PgNativeObserver, compile_synchronous_configuration, replication_application_name,
};
use postgres_replicated::testing::{TestDataDir, allocate_port, find_pg_bin};
use serial_test::serial;
use test_log::test;
use tokio::sync::mpsc;
use tokio_postgres::error::SqlState;

fn identity(id: i64) -> ReplicaIdentity {
    ReplicaIdentity {
        replica_id: ReplicaId::new(id),
        instance_id: ReplicaInstanceId::new(format!("pod-{id}")),
        agent_generation: AgentGeneration::new(format!("generation-{id}")),
    }
}

fn durable_identity(id: i64) -> PgDurableIdentity {
    PgDurableIdentity {
        resource_uid: ResourceUid::new("postgres-test"),
        replica: identity(id),
    }
}

fn commit_stages() -> [postgres_replicated::durable::CommitStage; 3] {
    use postgres_replicated::durable::CommitStage::*;
    [BeforeRename, CommittedBeforePublish, Published]
}

#[tokio::test]
async fn cancelled_metadata_commit_publishes_exactly_the_committed_state() {
    for stage in commit_stages() {
        let directory = TestDataDir::new("commit-cancel");
        let root = directory.path().join("application");
        let store = Arc::new(
            PgDurableStore::open(&root, durable_identity(1), StorageMode::Fresh)
                .await
                .unwrap(),
        );
        let gate = store.pause_commit(stage, false);
        let writing = tokio::spawn({
            let store = store.clone();
            async move {
                store
                    .update(|state| {
                        state.has_accepted_authority = true;
                        Ok(())
                    })
                    .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        writing.abort();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(2), writing)
                .await
                .unwrap()
                .unwrap_err()
                .is_cancelled()
        );
        gate.release();
        let committed = tokio::time::timeout(std::time::Duration::from_secs(5), store.revalidate())
            .await
            .unwrap()
            .unwrap();
        let committed_before_cancel =
            stage != postgres_replicated::durable::CommitStage::BeforeRename;
        assert_eq!(committed.has_accepted_authority, committed_before_cancel);
        assert_eq!(
            committed.generation,
            if committed_before_cancel { 2 } else { 1 }
        );
        assert_eq!(store.snapshot().await, committed);
        drop(store);
        let reopened = PgDurableStore::open(&root, durable_identity(1), StorageMode::Established)
            .await
            .unwrap();
        assert_eq!(reopened.revalidate().await.unwrap(), committed);
    }
}

#[test]
#[ignore = "subprocess helper for metadata_crash_boundaries_reopen_consistently"]
fn metadata_crash_writer() {
    let root = std::env::var_os("PG_METADATA_TEST_ROOT").unwrap();
    let stage: usize = std::env::var("PG_METADATA_TEST_STAGE")
        .unwrap()
        .parse()
        .unwrap();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let store = Arc::new(
                PgDurableStore::open(
                    PathBuf::from(root),
                    durable_identity(1),
                    StorageMode::Established,
                )
                .await
                .unwrap(),
            );
            let gate = store.pause_commit(commit_stages()[stage], false);
            tokio::spawn(async move {
                store
                    .update(|state| {
                        state.has_accepted_authority = true;
                        Ok(())
                    })
                    .await
                    .unwrap();
            });
            tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered.notified())
                .await
                .unwrap();
            std::process::exit(73);
        });
}

#[tokio::test]
async fn metadata_crash_boundaries_reopen_consistently() {
    for stage in 0..3 {
        let directory = TestDataDir::new("commit-crash");
        let root = directory.path().join("application");
        drop(
            PgDurableStore::open(&root, durable_identity(1), StorageMode::Fresh)
                .await
                .unwrap(),
        );
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "metadata_crash_writer",
                "--ignored",
                "--test-threads=1",
            ])
            .env("PG_METADATA_TEST_ROOT", &root)
            .env("PG_METADATA_TEST_STAGE", stage.to_string())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(73), "{output:?}");
        let reopened = PgDurableStore::open(&root, durable_identity(1), StorageMode::Established)
            .await
            .unwrap();
        let state = reopened.revalidate().await.unwrap();
        assert_eq!(state.has_accepted_authority, stage != 0);
        assert_eq!(state.generation, if stage == 0 { 1 } else { 2 });
    }
}

#[tokio::test]
async fn stalled_control_data_fence_is_bounded_reaped_and_retryable() {
    use postgres_replicated::testing::{ProcessProbe, wrapped_pg_bin};
    use std::time::Duration;
    for cancel in [false, true] {
        let directory = TestDataDir::new("control-gate");
        let armed = directory.path().join("armed");
        let marker = directory.path().join("control.pid");
        let leaf = directory.path().join("leaf.pid");
        let bin = wrapped_pg_bin(
            directory.path(),
            "pg_controldata",
            &format!(
                "#!/bin/sh\nif test -f '{}'; then\n echo $$ > '{}'\n sleep 30 &\n echo $! > '{}'\n wait\nelse\n exec '{}/pg_controldata' \"$@\"\nfi\n",
                armed.display(),
                marker.display(),
                leaf.display(),
                find_pg_bin().display()
            ),
        );
        let instance =
            PgInstanceManager::new(directory.path().join("pgdata"), bin, allocate_port().await);
        instance.init_db().await.unwrap();
        let (faults, mut reported) = mpsc::channel(8);
        instance.start_native(faults).await.unwrap();
        initialize_application_role(&instance).await.unwrap();
        let access = PgAccessController::new(&instance);
        access.grant_role_access().await.unwrap();
        let (old, _) = instance.connect_application().await.unwrap();
        old.batch_execute(
            "CREATE TABLE timeout_receipt(id int); INSERT INTO timeout_receipt VALUES (1)",
        )
        .await
        .unwrap();
        std::fs::write(&armed, b"").unwrap();
        let (helper, children) = {
            let closing = access.close_external();
            tokio::pin!(closing);
            tokio::select! {
                result = &mut closing => panic!("helper did not reach gate: {result:?}"),
                ready = tokio::time::timeout(Duration::from_secs(5), async {
                    while !marker.exists() || !leaf.exists() { tokio::time::sleep(Duration::from_millis(10)).await; }
                }) => { ready.unwrap(); }
            }
            let pid = std::fs::read_to_string(&marker)
                .unwrap()
                .trim()
                .parse()
                .unwrap();
            let helper = ProcessProbe::process(pid);
            let children = ProcessProbe::descendants(pid);
            if !cancel {
                let error = tokio::time::timeout(Duration::from_secs(8), &mut closing)
                    .await
                    .unwrap()
                    .unwrap_err();
                assert!(error.is_timeout(), "{error}");
                assert_eq!(
                    error.fault_type(),
                    kuberic_protocol::types::FaultType::Transient
                );
                assert_eq!(
                    reported.recv().await,
                    Some(kuberic_protocol::types::FaultType::Transient)
                );
            }
            (helper, children)
        };
        helper.assert_reaped();
        children.assert_reaped();
        assert!(!instance.is_running().await);
        assert!(
            old.simple_query("INSERT INTO timeout_receipt VALUES (2)")
                .await
                .is_err()
        );
        assert!(instance.connect_application().await.is_err());
        std::fs::remove_file(armed).unwrap();
        tokio::time::timeout(Duration::from_secs(8), access.close_external())
            .await
            .unwrap()
            .unwrap();
        assert!(instance.connect_application().await.is_err());
        access.grant_role_access().await.unwrap();
        let (restored, _) = instance.connect_application().await.unwrap();
        assert_eq!(
            restored
                .query_one("SELECT count(*) FROM timeout_receipt", &[])
                .await
                .unwrap()
                .get::<_, i64>(0),
            1
        );
        instance.stop().await.unwrap();
    }
}

#[test(tokio::test)]
async fn durable_state_reopens_exact_identity_and_rejects_corruption() {
    let directory = TestDataDir::new("durable");
    let root = directory.path().join("application");
    let expected = durable_identity(1);
    assert!(matches!(
        PgDurableStore::open(&root, expected.clone(), StorageMode::Established).await,
        Err(PgDurableError::Missing)
    ));
    let store = PgDurableStore::open(&root, expected.clone(), StorageMode::Fresh)
        .await
        .unwrap();
    assert!(matches!(
        PgDurableStore::open(&root, expected.clone(), StorageMode::Fresh).await,
        Err(PgDurableError::AlreadyExists)
    ));
    let fresh = store.snapshot().await;
    assert!(fresh.external_access_closed);
    assert!(fresh.postgres_stopped);
    store
        .update(|state| {
            state.system_identifier = Some("system-1".into());
            state.timeline_id = Some(1);
            state.current_lsn = 7;
            state.flush_lsn = 7;
            state.policy_certified_lsn = 7;
            Ok(())
        })
        .await
        .unwrap();
    let generation = store.snapshot().await.generation;
    let updated = store
        .update(|state| {
            state.generation = 0;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(updated.generation, generation + 1);
    assert!(
        store
            .update(|state| {
                state.current_lsn = 1;
                state.flush_lsn = 2;
                Ok(())
            })
            .await
            .is_err()
    );
    drop(store);

    let reopened = PgDurableStore::open(&root, expected.clone(), StorageMode::Established)
        .await
        .unwrap();
    assert_eq!(reopened.snapshot().await.flush_lsn, 7);
    assert!(
        PgDurableStore::open(&root, durable_identity(2), StorageMode::Established)
            .await
            .is_err()
    );
    drop(reopened);

    let state_path = root.join("state-v2.json");
    let mut envelope: serde_json::Value =
        serde_json::from_slice(&tokio::fs::read(&state_path).await.unwrap()).unwrap();
    envelope["state"]["currentLsn"] = serde_json::json!(999);
    tokio::fs::write(&state_path, serde_json::to_vec_pretty(&envelope).unwrap())
        .await
        .unwrap();
    assert!(matches!(
        PgDurableStore::open(&root, expected, StorageMode::Established).await,
        Err(PgDurableError::Checksum)
    ));
}

#[test(tokio::test)]
async fn synchronous_apply_failure_remains_durably_invalid() {
    let directory = TestDataDir::new("sync-invalidation");
    let durable = Arc::new(
        PgDurableStore::open(
            directory.path().join("application"),
            durable_identity(1),
            StorageMode::Fresh,
        )
        .await
        .unwrap(),
    );
    let instance = Arc::new(PgInstanceManager::new(
        directory.path().join("pgdata"),
        find_pg_bin(),
        allocate_port().await,
    ));
    let local = identity(1);
    let configuration = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        local.replica_id,
        vec![ConfigurationMember {
            identity: local.clone(),
            role: ReplicaRole::Primary,
        }],
        1,
    );
    let synchronous =
        compile_synchronous_configuration(None, &configuration, &local, &BTreeMap::new(), true)
            .unwrap();
    let observer = PgNativeObserver::with_store(instance, durable.clone()).await;
    assert!(observer.apply_synchronous(synchronous).await.is_err());
    assert!(
        durable
            .snapshot()
            .await
            .synchronous
            .as_ref()
            .is_some_and(|synchronous| !synchronous.valid)
    );
}

#[test(tokio::test)]
#[serial]
async fn local_postgres_reports_native_identity_and_completed_stop_fence() {
    let directory = TestDataDir::new("native-progress");
    let instance = Arc::new(PgInstanceManager::new(
        directory.path().join("pgdata"),
        find_pg_bin(),
        allocate_port().await,
    ));
    let (fault_tx, mut fault_rx) = mpsc::channel(4);
    instance.init_db().await.unwrap();
    instance.start_native(fault_tx).await.unwrap();
    initialize_application_role(&instance).await.unwrap();
    assert!(application_role_uses_remote_apply(&instance).await.unwrap());
    let access = PgAccessController::new(&instance);
    assert!(instance.connect_application().await.is_err());
    access.grant_role_access().await.unwrap();
    let (application, _application_connection) = instance.connect_application().await.unwrap();
    application.simple_query("SELECT 1").await.unwrap();
    access.close_external().await.unwrap();
    assert!(application.simple_query("SELECT 1").await.is_err());
    assert!(instance.connect_application().await.is_err());

    let local = identity(1);
    let configuration = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        local.replica_id,
        vec![ConfigurationMember {
            identity: local.clone(),
            role: ReplicaRole::Primary,
        }],
        1,
    );
    let synchronous =
        compile_synchronous_configuration(None, &configuration, &local, &BTreeMap::new(), true)
            .unwrap();
    let durable = Arc::new(
        PgDurableStore::open(
            directory.path().join("application"),
            durable_identity(1),
            StorageMode::Fresh,
        )
        .await
        .unwrap(),
    );
    let observer = PgNativeObserver::with_store(instance.clone(), durable.clone()).await;
    observer.apply_synchronous(synchronous).await.unwrap();
    let snapshot = observer.snapshot_and_persist().await.unwrap();
    let evidence = snapshot.evidence.unwrap();
    assert!(!evidence.system_identifier.is_empty());
    assert!(evidence.timeline_id > 0);
    assert!(!evidence.in_recovery);
    assert_eq!(snapshot.committed_lsn, snapshot.current_lsn);
    assert!(
        durable
            .snapshot()
            .await
            .synchronous
            .as_ref()
            .is_some_and(|synchronous| synchronous.valid)
    );

    let (client, connection) = instance.connect().await.unwrap();
    stop_fence(&instance).await.unwrap();
    assert!(client.simple_query("SELECT 1").await.is_err());
    connection.await.unwrap();
    assert!(fault_rx.try_recv().is_err());
}

#[test(tokio::test)]
#[serial]
async fn physical_standby_serves_reads_and_rejects_writes() {
    let directory = TestDataDir::new("standby-read-only");
    let primary = Arc::new(PgInstanceManager::new(
        directory.path().join("primary"),
        find_pg_bin(),
        allocate_port().await,
    ));
    let standby = Arc::new(PgInstanceManager::new(
        directory.path().join("standby"),
        find_pg_bin(),
        allocate_port().await,
    ));
    let (primary_faults, _primary_rx) = mpsc::channel(4);
    let (standby_faults, _standby_rx) = mpsc::channel(4);
    primary.init_db().await.unwrap();
    primary.start_native(primary_faults).await.unwrap();
    initialize_application_role(&primary).await.unwrap();
    PgAccessController::new(&primary)
        .grant_role_access()
        .await
        .unwrap();
    let (client, _connection) = primary.connect_application().await.unwrap();
    client
        .batch_execute("CREATE TABLE replicated_value(id int primary key); INSERT INTO replicated_value VALUES (1);")
        .await
        .unwrap();

    standby
        .base_backup(primary.listen_host(), primary.port())
        .await
        .unwrap();
    let standby_identity = identity(2);
    let standby_session = ProcessSessionId::new("standby-session-2");
    standby
        .config()
        .patch_after_clone_exact(standby.data_dir(), "old_session")
        .await
        .unwrap();
    let application_name = replication_application_name(&standby_identity, &standby_session);
    standby
        .config()
        .patch_after_clone_exact(standby.data_dir(), &application_name)
        .await
        .unwrap();
    let auto_conf = tokio::fs::read_to_string(standby.data_dir().join("postgresql.auto.conf"))
        .await
        .unwrap();
    assert!(auto_conf.contains(&application_name));
    assert!(!auto_conf.contains("application_name=old_session"));
    standby.start_native(standby_faults).await.unwrap();
    let primary_identity = identity(1);
    let configuration = ConfigurationDescriptor::new(
        Epoch::new(0, 2),
        primary_identity.replica_id,
        vec![
            ConfigurationMember {
                identity: primary_identity.clone(),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: standby_identity.clone(),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    );
    let synchronous = compile_synchronous_configuration(
        None,
        &configuration,
        &primary_identity,
        &[(standby_identity, standby_session)].into_iter().collect(),
        true,
    )
    .unwrap();
    let observer = PgNativeObserver::new(primary.clone());
    observer.apply_synchronous(synchronous).await.unwrap();
    let (standby_admin, _standby_admin_connection) = standby.connect().await.unwrap();
    standby_admin
        .simple_query("SELECT pg_wal_replay_pause()")
        .await
        .unwrap();
    let insert = tokio::spawn(async move {
        client
            .execute("INSERT INTO replicated_value VALUES (2)", &[])
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(!insert.is_finished());
    standby_admin
        .simple_query("SELECT pg_wal_replay_resume()")
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), insert)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(observer.snapshot().await.unwrap().committed_lsn > 0);

    PgAccessController::new(&standby)
        .grant_role_access()
        .await
        .unwrap();
    let (client, _connection) = standby.connect_application().await.unwrap();
    let count: i64 = client
        .query_one("SELECT count(*) FROM replicated_value", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 2);
    let error = client
        .execute("INSERT INTO replicated_value VALUES (3)", &[])
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::READ_ONLY_SQL_TRANSACTION));
    let absent: i64 = client
        .query_one("SELECT count(*) FROM replicated_value WHERE id = 3", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(absent, 0);

    stop_fence(&standby).await.unwrap();
    stop_fence(&primary).await.unwrap();
}

#[test(tokio::test)]
#[serial]
async fn unexpected_process_exit_reports_v2_permanent_fault() {
    let directory = TestDataDir::new("native-fault");
    let pg_bin = find_pg_bin();
    let instance = Arc::new(PgInstanceManager::new(
        directory.path().join("pgdata"),
        pg_bin.clone(),
        allocate_port().await,
    ));
    let (fault_tx, mut fault_rx) = mpsc::channel(4);
    instance.init_db().await.unwrap();
    instance.start_native(fault_tx).await.unwrap();
    let output = tokio::process::Command::new(pg_bin.join("pg_ctl"))
        .args([
            "stop",
            "-D",
            &instance.data_dir().to_string_lossy(),
            "-m",
            "immediate",
            "-w",
        ])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    let fault = tokio::time::timeout(std::time::Duration::from_secs(5), fault_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fault, kuberic_protocol::types::FaultType::Permanent);
    instance.stop().await.unwrap();
}

#[test(tokio::test)]
#[serial]
async fn restart_retires_old_fault_monitors_before_new_run() {
    let directory = TestDataDir::new("monitor-generation");
    let pg_bin = find_pg_bin();
    let instance = Arc::new(PgInstanceManager::new(
        directory.path().join("pgdata"),
        pg_bin.clone(),
        allocate_port().await,
    ));
    let (old_fault_tx, mut old_fault_rx) = mpsc::channel(8);
    instance.init_db().await.unwrap();
    instance.start_native(old_fault_tx).await.unwrap();
    let output = tokio::process::Command::new(pg_bin.join("pg_ctl"))
        .args([
            "stop",
            "-D",
            &instance.data_dir().to_string_lossy(),
            "-m",
            "immediate",
            "-w",
        ])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    tokio::time::timeout(std::time::Duration::from_secs(5), old_fault_rx.recv())
        .await
        .unwrap()
        .unwrap();
    while old_fault_rx.try_recv().is_ok() {}

    let (new_fault_tx, mut new_fault_rx) = mpsc::channel(8);
    instance.start_native(new_fault_tx).await.unwrap();
    stop_fence(&instance).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(750)).await;
    assert!(old_fault_rx.try_recv().is_err());
    assert!(new_fault_rx.try_recv().is_err());
}

#[test(tokio::test)]
#[serial]
async fn failed_start_reports_fault_cleans_child_and_allows_retry() {
    let directory = TestDataDir::new("failed-start");
    let instance = Arc::new(PgInstanceManager::new(
        directory.path().join("pgdata"),
        find_pg_bin(),
        allocate_port().await,
    ));
    let (fault_tx, mut fault_rx) = mpsc::channel(4);
    assert!(instance.start_native(fault_tx.clone()).await.is_err());
    assert!(!instance.is_running().await);
    assert_eq!(instance.process_state().await, PgProcessState::Faulted);
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(5), fault_rx.recv())
            .await
            .unwrap()
            .unwrap(),
        kuberic_protocol::types::FaultType::Permanent
    );
    instance.init_db().await.unwrap();
    instance.start_native(fault_tx).await.unwrap();
    assert_eq!(instance.process_state().await, PgProcessState::Running);
    stop_fence(&instance).await.unwrap();
    assert_eq!(instance.process_state().await, PgProcessState::Stopped);
}

#[test(tokio::test)]
#[serial]
async fn native_cancellation_stops_the_owned_postgres_process() {
    let directory = TestDataDir::new("native-cancellation");
    let instance = Arc::new(PgInstanceManager::new(
        directory.path().join("pgdata"),
        find_pg_bin(),
        allocate_port().await,
    ));
    let (fault_tx, _fault_rx) = mpsc::channel(4);
    instance.init_db().await.unwrap();
    let cancellation = tokio_util::sync::CancellationToken::new();
    instance
        .start_native_with_cancellation(fault_tx, cancellation.clone())
        .await
        .unwrap();
    cancellation.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            if instance.process_state().await == PgProcessState::Stopped {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert!(!instance.is_running().await);
}

#[test(tokio::test)]
#[serial]
async fn restart_closes_previous_application_access_grant() {
    let directory = TestDataDir::new("restart-closed");
    let instance = Arc::new(PgInstanceManager::new(
        directory.path().join("pgdata"),
        find_pg_bin(),
        allocate_port().await,
    ));
    let (fault_tx, _fault_rx) = mpsc::channel(4);
    instance.init_db().await.unwrap();
    instance.start_native(fault_tx.clone()).await.unwrap();
    initialize_application_role(&instance).await.unwrap();
    PgAccessController::new(&instance)
        .grant_role_access()
        .await
        .unwrap();
    assert!(instance.connect_application().await.is_ok());
    stop_fence(&instance).await.unwrap();
    instance.start_native(fault_tx).await.unwrap();
    assert!(instance.connect_application().await.is_err());
    stop_fence(&instance).await.unwrap();
}

#[test(tokio::test)]
#[serial]
async fn cancelling_an_old_run_does_not_stop_a_new_process() {
    let directory = TestDataDir::new("stale-cancellation");
    let instance = Arc::new(PgInstanceManager::new(
        directory.path().join("pgdata"),
        find_pg_bin(),
        allocate_port().await,
    ));
    let (fault_tx, _fault_rx) = mpsc::channel(4);
    instance.init_db().await.unwrap();
    let old = tokio_util::sync::CancellationToken::new();
    instance
        .start_native_with_cancellation(fault_tx.clone(), old.clone())
        .await
        .unwrap();
    stop_fence(&instance).await.unwrap();
    instance.start_native(fault_tx).await.unwrap();
    old.cancel();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(instance.is_running().await);
    stop_fence(&instance).await.unwrap();
}

#[test(tokio::test)]
#[serial]
async fn spawn_failure_reports_permanent_fault_and_faulted_state() {
    let directory = TestDataDir::new("spawn-failure");
    let instance = Arc::new(PgInstanceManager::new(
        directory.path().join("pgdata"),
        directory.path().join("missing-bin"),
        allocate_port().await,
    ));
    let (fault_tx, mut fault_rx) = mpsc::channel(4);
    assert!(instance.start_native(fault_tx).await.is_err());
    assert_eq!(instance.process_state().await, PgProcessState::Faulted);
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(5), fault_rx.recv())
            .await
            .unwrap()
            .unwrap(),
        kuberic_protocol::types::FaultType::Permanent
    );
}

#[test(tokio::test)]
#[serial]
async fn exact_name_rotation_preserves_effective_libpq_escaping() {
    let directory = TestDataDir::new("conninfo-escaping");
    let instance = Arc::new(PgInstanceManager::new(
        directory.path().join("pgdata"),
        find_pg_bin(),
        allocate_port().await,
    ));
    let (fault_tx, _fault_rx) = mpsc::channel(4);
    instance.init_db().await.unwrap();
    instance.start_native(fault_tx.clone()).await.unwrap();
    let (admin, _connection) = instance.connect().await.unwrap();
    admin
        .batch_execute(
            r#"ALTER SYSTEM SET primary_conninfo = $review$host=127.0.0.1 password=a\'b application_name='old session'$review$"#,
        )
        .await
        .unwrap();
    stop_fence(&instance).await.unwrap();
    instance
        .config()
        .patch_after_clone_exact(instance.data_dir(), "new_session")
        .await
        .unwrap();
    instance.start_native(fault_tx).await.unwrap();
    let (admin, _connection) = instance.connect().await.unwrap();
    let effective: String = admin
        .query_one("SHOW primary_conninfo", &[])
        .await
        .unwrap()
        .get(0);
    let effective = tokio_postgres::Config::from_str(&effective).unwrap();
    assert_eq!(effective.get_application_name(), Some("new_session"));
    assert_eq!(effective.get_password(), Some(&b"a'b"[..]));
    stop_fence(&instance).await.unwrap();
}

#[test(tokio::test)]
#[serial]
async fn missing_pid_file_cannot_produce_a_false_completed_fence() {
    let directory = TestDataDir::new("missing-pid-fence");
    let instance = Arc::new(PgInstanceManager::new(
        directory.path().join("pgdata"),
        find_pg_bin(),
        allocate_port().await,
    ));
    let (fault_tx, _fault_rx) = mpsc::channel(4);
    instance.init_db().await.unwrap();
    instance.start_native(fault_tx).await.unwrap();
    let (client, _connection) = instance.connect().await.unwrap();
    let pid = instance.data_dir().join("postmaster.pid");
    let hidden = instance.data_dir().join("postmaster.pid.hidden");
    tokio::fs::rename(&pid, &hidden).await.unwrap();
    assert!(stop_fence(&instance).await.is_err());
    assert!(client.simple_query("SELECT 1").await.is_err());
    assert!(!instance.is_running().await);
    assert!(instance.connect().await.is_err());
    assert!(stop_fence(&instance).await.is_err());
}

#[test]
fn postgres_errors_have_explicit_fault_classification() {
    use postgres_replicated::instance::PgError;

    assert_eq!(
        PgError::Connection("temporary".into()).fault_type(),
        kuberic_protocol::types::FaultType::Transient
    );
    assert_eq!(
        PgError::Query("temporary".into()).fault_type(),
        kuberic_protocol::types::FaultType::Transient
    );
    assert_eq!(
        PgError::Configuration("unsafe".into()).fault_type(),
        kuberic_protocol::types::FaultType::Permanent
    );
}
