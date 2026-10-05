use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use kuberic_runtime::protocol::types::{ConfigurationId, FaultType, OperationId, ResourceUid};
use kuberic_runtime::replicator::ReplicaInformation;
use kuberic_runtime::testing::copy::BuildConfiguration;
use postgres_replicated::durable::{
    CommitStage, MAX_RETAINED_BUILD_IDS, PgDurableIdentity, PgDurableStore, StorageMode,
};
use postgres_replicated::instance::PgInstanceManager;
use postgres_replicated::native::{
    PgNativeObserver, PolicyStage, compile_synchronous_configuration,
};
use postgres_replicated::testing::{
    PgPod, TestDataDir, allocate_port, find_pg_bin, native_configuration, native_identity,
    wrapped_pg_bin,
};

fn identity() -> PgDurableIdentity {
    PgDurableIdentity {
        resource_uid: ResourceUid::new("gate"),
        replica: native_identity(1, "gate"),
    }
}

#[tokio::test]
async fn ownership_pending_count_identifier_and_byte_bounds_are_enforced() {
    use kuberic_runtime::protocol::types::{BuildAuthority, BuildAuthorityKind, ProcessSessionId};
    use postgres_replicated::build::{
        BUILD_PROTOCOL_VERSION, PgBuildMethod, PgBuildProgress, PgBuildRequest, PgBuildStage,
        PgLineage,
    };
    use postgres_replicated::durable::{MAX_METADATA_BYTES, PgDurableError};
    let root = TestDataDir::new("gate-limits");
    let store = PgDurableStore::open(root.path(), identity(), StorageMode::Fresh)
        .await
        .unwrap();
    assert!(matches!(
        PgDurableStore::open(root.path(), identity(), StorageMode::Established).await,
        Err(PgDurableError::AlreadyOwned)
    ));
    let before = store.snapshot().await;
    let pending = (2..19)
        .map(|id| PgBuildProgress {
            request: PgBuildRequest {
                version: BUILD_PROTOCOL_VERSION,
                resource_uid: identity().resource_uid,
                authority: BuildAuthority {
                    build_id: OperationId::new(format!("pending-{id}")),
                    kind: BuildAuthorityKind::Provisioning,
                    source: identity().replica,
                    target: native_identity(id, "pending"),
                    current_configuration: native_configuration(&[identity().replica], 0, 1),
                    replication_boundary_lsn: 0,
                },
                source_session: ProcessSessionId::new("source-session"),
                target_session: ProcessSessionId::new(format!("target-session-{id}")),
                source_endpoint: "http://127.0.0.1:12345".into(),
                source_host: "127.0.0.1".into(),
                source_port: 5432,
                lineage: PgLineage {
                    system_identifier: "123".into(),
                    timeline: 1,
                    history: vec![],
                    history_text: String::new(),
                },
            },
            stage: PgBuildStage::Intent,
            method: PgBuildMethod::Fresh,
            sequence: 1,
            evidence: None,
        })
        .collect::<Vec<_>>();
    assert!(
        store
            .update(|s| {
                s.suspended_builds = pending;
                Ok(())
            })
            .await
            .is_err()
    );
    assert_eq!(store.revalidate().await.unwrap(), before);
    assert!(
        store
            .update(|s| {
                s.retired_builds.insert(OperationId::new("x".repeat(513)));
                Ok(())
            })
            .await
            .is_err()
    );
    assert_eq!(store.revalidate().await.unwrap(), before);
    assert!(
        store
            .update(|s| {
                s.system_identifier = Some("x".repeat(MAX_METADATA_BYTES));
                Ok(())
            })
            .await
            .is_err()
    );
    assert_eq!(store.revalidate().await.unwrap(), before);
    drop(store);
    std::fs::write(
        root.path().join("state-v2.json"),
        vec![b' '; MAX_METADATA_BYTES + 1],
    )
    .unwrap();
    assert!(
        PgDurableStore::open(root.path(), identity(), StorageMode::Established)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn cancelled_owner_cannot_consume_reopened_staging_or_reorder_generations() {
    for _ in 0..3 {
        let root = TestDataDir::new("gate-owner");
        let store = Arc::new(
            PgDurableStore::open(root.path(), identity(), StorageMode::Fresh)
                .await
                .unwrap(),
        );
        let old_gate = store.pause_commit(CommitStage::BeforeRename, false);
        let old = tokio::spawn({
            let store = store.clone();
            async move {
                store
                    .update(|s| {
                        s.has_accepted_authority = true;
                        Ok(())
                    })
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), old_gate.entered.notified())
            .await
            .unwrap();
        old.abort();
        assert!(old.await.unwrap_err().is_cancelled());
        assert_eq!(store.revalidate().await.unwrap().generation, 1);
        drop(store);
        let reopened = Arc::new(
            PgDurableStore::open(root.path(), identity(), StorageMode::Established)
                .await
                .unwrap(),
        );
        let new_gate = reopened.pause_commit(CommitStage::BeforeRename, false);
        let new = tokio::spawn({
            let store = reopened.clone();
            async move {
                store
                    .update(|s| {
                        s.catch_up = Some((ConfigurationId::new("new-owner"), 0));
                        Ok(())
                    })
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), new_gate.entered.notified())
            .await
            .unwrap();
        old_gate.release();
        assert!(!new.is_finished());
        let file = std::fs::read_to_string(root.path().join("state-v2.json")).unwrap();
        assert!(!file.contains("new-owner"));
        new_gate.release();
        let current = new.await.unwrap().unwrap();
        assert_eq!(current.generation, 2);
        assert!(!current.has_accepted_authority);
        assert_eq!(reopened.revalidate().await.unwrap(), current);
    }
}

#[tokio::test]
async fn queued_cancellation_is_noop_and_postcommit_cancellation_orders_the_next_update() {
    let root = TestDataDir::new("gate-order");
    let store = Arc::new(
        PgDurableStore::open(root.path(), identity(), StorageMode::Fresh)
            .await
            .unwrap(),
    );
    let gate = store.pause_commit(CommitStage::CommittedBeforePublish, false);
    let first = tokio::spawn({
        let store = store.clone();
        async move {
            store
                .update(|s| {
                    s.has_accepted_authority = true;
                    Ok(())
                })
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    let queued = tokio::spawn({
        let store = store.clone();
        async move {
            store
                .update(|s| {
                    s.catch_up = Some((ConfigurationId::new("cancelled"), 0));
                    Ok(())
                })
                .await
        }
    });
    tokio::task::yield_now().await;
    queued.abort();
    assert!(queued.await.unwrap_err().is_cancelled());
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    gate.release();
    let next = store
        .update(|s| {
            assert!(s.has_accepted_authority);
            assert!(s.catch_up.is_none());
            s.catch_up = Some((ConfigurationId::new("later"), 0));
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(next.generation, 3);
    assert_eq!(store.revalidate().await.unwrap(), next);
}

#[tokio::test]
async fn invalidation_cancellation_never_republishes_the_old_valid_policy() {
    let root = TestDataDir::new("gate-policy");
    let store = Arc::new(
        PgDurableStore::open(root.path().join("meta"), identity(), StorageMode::Fresh)
            .await
            .unwrap(),
    );
    let instance = Arc::new(PgInstanceManager::new(
        root.path().join("pgdata"),
        find_pg_bin(),
        allocate_port().await,
    ));
    instance.init_db().await.unwrap();
    let (faults, _rx) = tokio::sync::mpsc::channel(8);
    instance.start_native(faults).await.unwrap();
    let local = identity().replica;
    let cfg = native_configuration(std::slice::from_ref(&local), 0, 1);
    let valid =
        compile_synchronous_configuration(None, &cfg, &local, &BTreeMap::new(), true).unwrap();
    let observer = Arc::new(PgNativeObserver::with_store(instance.clone(), store.clone()).await);
    for stage in [
        CommitStage::BeforeRename,
        CommitStage::CommittedBeforePublish,
        CommitStage::Published,
    ] {
        observer.apply_synchronous(valid.clone()).await.unwrap();
        let mut invalid = valid.clone();
        invalid.valid = false;
        let gate = store.pause_commit(stage, false);
        let operation = tokio::spawn({
            let observer = observer.clone();
            async move { observer.set_synchronous(invalid).await }
        });
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        operation.abort();
        assert!(operation.await.unwrap_err().is_cancelled());
        gate.release();
        let durable = store.revalidate().await.unwrap();
        let observed = observer
            .snapshot()
            .await
            .unwrap()
            .evidence
            .unwrap()
            .synchronous
            .unwrap();
        assert!(!observed.valid, "stale valid policy after {stage:?}");
        if stage != CommitStage::BeforeRename {
            assert_eq!(Some(observed), durable.synchronous);
        }
        observer.snapshot_and_persist().await.unwrap();
        assert!(
            !observer
                .snapshot()
                .await
                .unwrap()
                .evidence
                .unwrap()
                .synchronous
                .unwrap()
                .valid
        );
        observer.apply_synchronous(valid.clone()).await.unwrap();
        assert!(
            observer
                .snapshot()
                .await
                .unwrap()
                .evidence
                .unwrap()
                .synchronous
                .unwrap()
                .valid
        );
    }
    instance.stop().await.unwrap();
}

#[tokio::test]
async fn apply_and_readback_cancellation_cuts_publish_only_committed_policy() {
    let root = TestDataDir::new("gate-apply");
    let store = Arc::new(
        PgDurableStore::open(root.path().join("meta"), identity(), StorageMode::Fresh)
            .await
            .unwrap(),
    );
    let instance = Arc::new(PgInstanceManager::new(
        root.path().join("pgdata"),
        find_pg_bin(),
        allocate_port().await,
    ));
    instance.init_db().await.unwrap();
    let (faults, _rx) = tokio::sync::mpsc::channel(8);
    instance.start_native(faults).await.unwrap();
    let local = identity().replica;
    let cfg = native_configuration(std::slice::from_ref(&local), 0, 1);
    let valid =
        compile_synchronous_configuration(None, &cfg, &local, &BTreeMap::new(), true).unwrap();
    let observer = Arc::new(PgNativeObserver::with_store(instance.clone(), store.clone()).await);
    for stage in [
        PolicyStage::InvalidationStarted,
        PolicyStage::Invalidated,
        PolicyStage::Applied,
        PolicyStage::ReadBack,
        PolicyStage::Published,
    ] {
        observer.apply_synchronous(valid.clone()).await.unwrap();
        let gate = observer.pause_policy(stage);
        let operation = tokio::spawn({
            let observer = observer.clone();
            let valid = valid.clone();
            async move { observer.apply_synchronous(valid).await }
        });
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        operation.abort();
        assert!(operation.await.unwrap_err().is_cancelled());
        let observed = observer
            .snapshot()
            .await
            .unwrap()
            .evidence
            .unwrap()
            .synchronous
            .unwrap();
        assert_eq!(observed.valid, stage == PolicyStage::Published, "{stage:?}");
        observer.apply_synchronous(valid.clone()).await.unwrap();
    }
    for stage in [
        CommitStage::BeforeRename,
        CommitStage::CommittedBeforePublish,
        CommitStage::Published,
    ] {
        let readback = observer.pause_policy(PolicyStage::ReadBack);
        let operation = tokio::spawn({
            let observer = observer.clone();
            let valid = valid.clone();
            async move { observer.apply_synchronous(valid).await }
        });
        tokio::time::timeout(Duration::from_secs(5), readback.entered.notified())
            .await
            .unwrap();
        let gate = store.pause_commit(stage, false);
        readback.release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        operation.abort();
        assert!(operation.await.unwrap_err().is_cancelled());
        gate.release();
        let durable = store.revalidate().await.unwrap().synchronous;
        let observed = observer
            .snapshot()
            .await
            .unwrap()
            .evidence
            .unwrap()
            .synchronous;
        assert_eq!(observed, durable);
        assert_eq!(observed.unwrap().valid, stage != CommitStage::BeforeRename);
    }
    instance.stop().await.unwrap();
}

#[tokio::test]
async fn actual_sf_service_retries_control_helper_without_a_permanent_fault() {
    let root = TestDataDir::new("gate-retry");
    let armed = root.path().join("armed");
    let bin = wrapped_pg_bin(
        root.path(),
        "pg_controldata",
        &format!(
            "#!/bin/sh\nif test -f '{}'; then\n sleep 30\nelse\n exec '{}/pg_controldata' \"$@\"\nfi\n",
            armed.display(),
            find_pg_bin().display()
        ),
    );
    let pod = PgPod::with_bin(root.path().join("p"), native_identity(1, "retry"), bin).await;
    pod.singleton().await;
    let control = pod.runtime.primary_replicator().await.unwrap();
    std::fs::write(&armed, b"").unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(10), control.current_progress())
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(
        pod.runtime.partition_report().await.reported_fault,
        Some(FaultType::Transient)
    );
    assert!(
        pod.application
            .instance()
            .connect_application()
            .await
            .is_err()
    );
    std::fs::remove_file(armed).unwrap();
    for _ in 0..3 {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(10), control.current_progress())
                .await
                .unwrap()
                .unwrap(),
            0
        );
        assert_ne!(
            pod.runtime.partition_report().await.reported_fault,
            Some(FaultType::Permanent)
        );
        assert!(
            pod.application
                .instance()
                .connect_application()
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn bounded_history_exhaustion_requires_epoch_advance_and_rejects_old_work_after_reopen() {
    let root = TestDataDir::new("gate-bound");
    let mut source = PgPod::new(root.path().join("s"), native_identity(1, "source")).await;
    let target = PgPod::new(root.path().join("t"), native_identity(2, "target")).await;
    source.singleton().await;
    source.refresh().await;
    source.peer(&target).await;
    let authority = source
        .runtime
        .authorize_build(
            OperationId::new("bounded-0000"),
            target.identity.clone(),
            BuildConfiguration::Current,
        )
        .await
        .unwrap();
    assert!(
        kuberic_runtime::testing::execute_build(
            &source.runtime,
            ReplicaInformation::new(
                authority.build_id.clone(),
                target.identity.clone(),
                "http://127.0.0.1:0".into()
            ),
        )
        .await
        .is_err()
    );
    source
        .runtime
        .cancel_outbound_build(&authority.build_id)
        .await
        .unwrap();
    let state = source.application.native_driver().durable_state().await;
    assert_eq!(state.retired_builds.len() + state.suspended_builds.len(), 1);
    assert!(state.outbound_builds.is_empty());
    source
        .application
        .native_driver()
        .fill_retired_build_history_for_test(MAX_RETAINED_BUILD_IDS / 2)
        .await;
    source = source.reopen().await;
    let state = source.application.native_driver().durable_state().await;
    assert_eq!(
        state.retired_builds.len() + state.suspended_builds.len(),
        MAX_RETAINED_BUILD_IDS / 2
    );
    source
        .application
        .native_driver()
        .fill_retired_build_history_for_test(MAX_RETAINED_BUILD_IDS)
        .await;
    let state = source.application.native_driver().durable_state().await;
    assert_eq!(
        state.retired_builds.len() + state.suspended_builds.len(),
        MAX_RETAINED_BUILD_IDS
    );
    source.peer(&target).await;
    let error = source
        .runtime
        .authorize_build(
            OperationId::new("over-bound"),
            target.identity.clone(),
            BuildConfiguration::Current,
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("newer admitted epoch"),
        "{error}"
    );
    assert_ne!(
        source.runtime.partition_report().await.reported_fault,
        Some(FaultType::Permanent)
    );
    source
        .admit(native_configuration(
            std::slice::from_ref(&source.identity),
            0,
            2,
        ))
        .await;
    let source = source.reopen().await;
    let state = source.application.native_driver().durable_state().await;
    assert!(state.retired_builds.is_empty());
    assert!(state.suspended_builds.is_empty());
    assert_eq!(
        state.retired_build_epoch,
        Some(kuberic_runtime::protocol::types::Epoch::new(0, 1))
    );
    assert!(
        kuberic_runtime::testing::execute_build(
            &source.runtime,
            ReplicaInformation::new(
                OperationId::new("bounded-0000"),
                target.identity.clone(),
                target.endpoint.clone()
            ),
        )
        .await
        .is_err()
    );
    let next = source.authorize(&target, "fresh-epoch-build").await;
    source.build(&target, &next).await.unwrap();
    assert!(
        std::fs::metadata(source.root.join("application/state-v2.json"))
            .unwrap()
            .len()
            < 16384
    );
}
