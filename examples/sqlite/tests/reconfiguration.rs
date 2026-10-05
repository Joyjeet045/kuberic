use futures::TryStreamExt;
use kuberic_runtime::engine::DurableState;
use kuberic_runtime::protocol::types::*;
use kuberic_runtime::testing::effects::RuntimeEffectAction;
use sqlite_replicated::state::PersistenceFault;
use sqlite_replicated::testing::*;

#[tokio::test]
async fn replacement_replays_copy_across_source_candidate_and_install_ack_crash_cuts() {
    let root = scratch();
    let source = SqlitePod::new(1, root.path().join("source"), 3).await;
    let old = SqlitePod::new(2, root.path().join("old"), 3).await;
    let witness = SqlitePod::new(3, root.path().join("witness"), 3).await;
    let previous = bootstrap(&[&source, &old, &witness]).await;
    for pod in [&old, &witness] {
        let progress = copy_progress(
            pod,
            &OperationId::new(format!("bootstrap-{}", pod.identity.replica_id)),
        )
        .await;
        assert!(progress.completed);
        assert_eq!(progress.authority.replication_boundary_lsn, 0);
    }
    let routes = route(&source, &[&old, &witness]).await;
    create_data(&source).await;
    let first = write_receipt(&source, 10).await;
    wait_applied(&[&old, &witness], first.lsn).await;
    routes.stop().await;
    old.runtime.abort();
    let replacement = ReplicaIdentity {
        instance_id: ReplicaInstanceId::new("sqlite-2-replacement"),
        agent_generation: AgentGeneration::new("replacement-generation"),
        ..identity(2)
    };
    let candidate = SqlitePod::with_identity(replacement, root.path().join("candidate"), 3).await;
    candidate.open().await;
    candidate
        .effect(RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary))
        .await
        .unwrap();
    let build_id = OperationId::new("replacement-copy");
    let frozen = authorize_copy(&source, &candidate, &build_id).await;
    assert_eq!(frozen.replication_boundary_lsn, first.lsn);
    let routes = route(&source, &[&witness]).await;
    let second = write_receipt(&source, 20).await;
    wait_applied(&[&witness], second.lsn).await;
    routes.stop().await;
    let mut copy = prepare_copy(&source, &candidate, &build_id).await;
    let original = copy_items(&mut copy, second.lsn).await;
    assert_eq!(original.len(), 3);
    assert!(original[0].snapshot_chunk);
    assert!(original[1].final_item);
    assert_eq!(original[1].committed_lsn, first.lsn);
    assert_eq!(original[1].catch_up_boundary_lsn, Some(second.lsn));
    assert_eq!(original[2].lsn, second.lsn);
    let mut transport = copy_transport(&source, &candidate).await;
    deliver_copy(&mut transport, original[0].clone())
        .await
        .unwrap();
    assert_eq!(copy_progress(&candidate, &build_id).await.last_sequence, 1);
    drop(transport);
    drop(copy);
    let source = source.reopen().await;
    let candidate = candidate.reopen().await;
    assert_eq!(persisted_build(&source, &build_id).await, frozen);
    assert!(!copy_progress(&candidate, &build_id).await.completed);
    let mut copy = prepare_copy(&source, &candidate, &build_id).await;
    assert_eq!(copy_items(&mut copy, second.lsn).await, original);
    let mut transport = copy_transport(&source, &candidate).await;
    candidate
        .application
        .persistence()
        .fail_once(PersistenceFault::AfterCopyInstall);
    assert!(
        deliver_copy(&mut transport, original[1].clone())
            .await
            .is_err()
    );
    assert_eq!(
        candidate
            .application
            .persistence()
            .progress()
            .unwrap()
            .committed_lsn,
        first.lsn
    );
    assert!(!copy_progress(&candidate, &build_id).await.completed);
    assert_durable_receipts(&candidate, std::slice::from_ref(&first));
    drop(transport);
    drop(copy);
    source
        .runtime
        .cancel_outbound_build(&build_id)
        .await
        .unwrap();
    let candidate = candidate.reopen().await;
    let mut copy = prepare_copy(&source, &candidate, &build_id).await;
    assert_eq!(copy_items(&mut copy, second.lsn).await, original);
    let mut transport = copy_transport(&source, &candidate).await;
    deliver_copy(&mut transport, original[0].clone())
        .await
        .unwrap();
    deliver_copy(&mut transport, original[1].clone())
        .await
        .unwrap();
    deliver_copy(&mut transport, original[1].clone())
        .await
        .unwrap();
    deliver_copy(&mut transport, original[2].clone())
        .await
        .unwrap();
    let before = candidate.application.persistence().progress().unwrap();
    assert_eq!(
        (before.applied_lsn, before.committed_lsn),
        (second.lsn, first.lsn)
    );
    let retained = candidate
        .application
        .persistence()
        .get_replication_operations(second.lsn, second.lsn)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(retained[0].data.as_ref(), original[2].data.as_slice());
    assert_eq!(retained[0].committed_lsn, original[2].committed_lsn);
    let mut receipts = vec![first, second];
    assert_durable_receipts(&candidate, &receipts);
    assert_closed(&candidate).await;
    deliver_copy(&mut transport, original[1].clone())
        .await
        .unwrap();
    for both in [false, true] {
        let mut conflict = original[1].clone();
        conflict.committed_lsn += 1;
        if both {
            conflict.lsn += 1;
            conflict.replication_boundary_lsn += 1;
        }
        assert!(deliver_copy(&mut transport, conflict).await.is_err());
        assert_eq!(
            candidate.application.persistence().progress().unwrap(),
            before
        );
    }
    drop(transport);
    drop(copy);
    source
        .runtime
        .cancel_outbound_build(&build_id)
        .await
        .unwrap();
    let candidate = candidate.reopen().await;
    let mut copy = prepare_copy(&source, &candidate, &build_id).await;
    assert_eq!(copy_items(&mut copy, receipts[1].lsn).await, original);
    let mut transport = copy_transport(&source, &candidate).await;
    deliver_copy(&mut transport, original[1].clone())
        .await
        .unwrap();
    assert_eq!(
        candidate.application.persistence().progress().unwrap(),
        before
    );
    let mut conflicting_after_restart = original[1].clone();
    conflicting_after_restart.committed_lsn += 1;
    assert!(
        deliver_copy(&mut transport, conflicting_after_restart)
            .await
            .is_err()
    );
    assert_eq!(
        candidate.application.persistence().progress().unwrap(),
        before
    );
    assert_durable_receipts(&candidate, &receipts);
    drop(transport);

    let current = configuration(
        &[
            source.identity.clone(),
            candidate.identity.clone(),
            witness.identity.clone(),
        ],
        0,
        2,
    );
    for pod in [&candidate, &witness, &source] {
        let mut admitted = authority(pod.identity.clone(), current.clone());
        admitted.previous_configuration = Some(previous.clone());
        admitted.transition_kind = Some(TransitionKind::Replacement);
        pod.effect(RuntimeEffectAction::AdmitAuthority(Box::new(admitted)))
            .await
            .unwrap();
        pod.effect(RuntimeEffectAction::ChangeRole(
            if pod.identity == source.identity {
                ReplicaRole::Primary
            } else {
                ReplicaRole::ActiveSecondary
            },
        ))
        .await
        .unwrap();
    }
    let routes = route(&source, &[&candidate, &witness]).await;
    for peer in [&candidate, &witness] {
        source
            .runtime
            .repair_peer(peer.identity.clone(), frozen.replication_boundary_lsn)
            .await
            .unwrap();
    }
    wait_catchup(&source).await;
    for pod in [&source, &candidate, &witness] {
        pod.effect(RuntimeEffectAction::AdmitAuthority(Box::new(authority(
            pod.identity.clone(),
            current.clone(),
        ))))
        .await
        .unwrap();
    }
    source.grant().await;
    routes.stop().await;
    drop(copy);
    retire_copy(&source, &candidate, &build_id).await;
    assert_closed(&old).await;
    assert_receipts(&source, &receipts).await;
    // A copied applied suffix is certified by a real planned handoff, then made
    // SQL-visible before any further SQL write on the candidate.
    let _current = change_primary(
        &[&source, &candidate, &witness],
        &current,
        &candidate,
        true,
        receipts[1].lsn,
    )
    .await;
    assert_receipts(&candidate, &receipts).await;
    assert_closed(&source).await;
    let candidate = candidate.reopen().await;
    assert_receipts(&candidate, &receipts).await;
    let routes = route(&candidate, &[&source, &witness]).await;
    let next = write_receipt(&candidate, 30).await;
    wait_applied(&[&source, &witness], next.lsn).await;
    receipts.push(next);
    assert_receipts(&candidate, &receipts).await;
    for stale in [&source, &witness, &old] {
        assert_closed(stale).await;
    }
    routes.stop().await;
}

#[tokio::test]
async fn sequential_scale_up_admits_only_durable_sqlite_candidates_then_retires_a_secondary() {
    let root = scratch();
    let source = SqlitePod::singleton(root.path().join("source")).await;
    create_data(&source).await;
    let mut receipts = vec![write_receipt(&source, 1).await];
    let previous = source
        .runtime
        .snapshot()
        .await
        .authority
        .unwrap()
        .current_configuration;
    let second = SqlitePod::new(2, root.path().join("second"), 2).await;
    second.open().await;
    second
        .effect(RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary))
        .await
        .unwrap();
    let id = OperationId::new("scale-up-2");
    let build = authorize_copy(&source, &second, &id).await;
    let mut copy = prepare_copy(&source, &second, &id).await;
    let items = copy_items(&mut copy, receipts.last().unwrap().lsn).await;
    let mut transport = copy_transport(&source, &second).await;
    for item in items {
        deliver_copy(&mut transport, item).await.unwrap();
    }
    drop(transport);
    drop(copy);
    let second = second.reopen().await;
    assert_closed(&second).await;
    assert_durable_receipts(&second, &receipts);
    let intent = scale_up_intent(
        &previous,
        &second,
        &build,
        copy_progress(&second, &id)
            .await
            .catch_up_boundary_lsn
            .unwrap(),
    );
    admit_expansion(&[&source, &second], &intent).await;
    retire_copy(&source, &second, &id).await;
    let routes = route(&source, &[&second]).await;
    receipts.push(write_receipt(&source, 2).await);
    wait_applied(&[&second], receipts.last().unwrap().lsn).await;
    routes.stop().await;
    let source = source.reopen().await;
    assert_receipts(&source, &receipts).await;
    let previous = intent.current_configuration;

    let third = SqlitePod::new(3, root.path().join("third"), 3).await;
    third.open().await;
    third
        .effect(RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary))
        .await
        .unwrap();
    let id = OperationId::new("scale-up-3");
    let build = authorize_copy(&source, &third, &id).await;
    let mut copy = prepare_copy(&source, &third, &id).await;
    let items = copy_items(&mut copy, receipts.last().unwrap().lsn).await;
    let mut transport = copy_transport(&source, &third).await;
    for item in items {
        deliver_copy(&mut transport, item).await.unwrap();
    }
    drop(transport);
    assert_closed(&third).await;
    assert_durable_receipts(&third, &receipts);
    let intent = scale_up_intent(
        &previous,
        &third,
        &build,
        copy_progress(&third, &id)
            .await
            .catch_up_boundary_lsn
            .unwrap(),
    );
    admit_expansion(&[&source, &second, &third], &intent).await;
    drop(copy);
    retire_copy(&source, &third, &id).await;
    let routes = route(&source, &[&second, &third]).await;
    receipts.push(write_receipt(&source, 3).await);
    wait_applied(&[&second, &third], receipts.last().unwrap().lsn).await;
    routes.stop().await;
    assert_durable_receipts(&third, &receipts);
    assert_closed(&second).await;
    assert_closed(&third).await;
    let committed = scale_down(&[&source, &second], &third, &intent.current_configuration).await;
    assert_eq!(committed.evidence.preparation.intent.target, third.identity);
    assert!(third.runtime.snapshot().await.retired_authority.is_some());
    assert_closed(&third).await;
    let third = third.reopen().await;
    assert!(!third.runtime.snapshot().await.open);
    assert!(third.runtime.snapshot().await.retired_authority.is_some());
    third
        .runtime
        .reconstruct(
            kuberic_runtime::application::OpenMode::Existing,
            ReplicaRole::Primary,
            AccessStatus::Granted,
            AccessStatus::Granted,
            None,
        )
        .await
        .unwrap();
    assert!(!third.runtime.snapshot().await.open);
    assert_eq!(third.runtime.snapshot().await.role, ReplicaRole::None);
    assert_closed(&third).await;
    let routes = route(&source, &[&second]).await;
    receipts.push(write_receipt(&source, 4).await);
    wait_applied(&[&second], receipts.last().unwrap().lsn).await;
    assert_receipts(&source, &receipts).await;
    routes.stop().await;
    assert_durable_receipts(&second, &receipts);
}

#[tokio::test]
async fn repeated_last_acknowledgement_failover_and_restart_preserve_every_receipt() {
    let root = scratch();
    let first = SqlitePod::new(1, root.path().join("one"), 3).await;
    let second = SqlitePod::new(2, root.path().join("two"), 3).await;
    let third = SqlitePod::new(3, root.path().join("three"), 3).await;
    let previous = bootstrap(&[&first, &second, &third]).await;
    let routes = route(&first, &[&second, &third]).await;
    create_data(&first).await;
    let first_receipt = write_receipt(&first, 1).await;
    wait_applied(&[&second, &third], first_receipt.lsn).await;
    assert_eq!(
        second
            .application
            .persistence()
            .progress()
            .unwrap()
            .committed_lsn,
        first_receipt.lsn - 1
    );
    routes.stop().await;
    let current = change_primary(
        &[&first, &second, &third],
        &previous,
        &second,
        false,
        first_receipt.lsn,
    )
    .await;
    assert_eq!(
        second
            .application
            .persistence()
            .progress()
            .unwrap()
            .applied_lsn,
        first_receipt.lsn
    );
    assert_receipts(&second, std::slice::from_ref(&first_receipt)).await;
    assert_closed(&first).await;
    let second = second.reopen().await;
    assert_eq!(
        second
            .application
            .persistence()
            .progress()
            .unwrap()
            .committed_lsn,
        first_receipt.lsn
    );
    assert_receipts(&second, std::slice::from_ref(&first_receipt)).await;
    // Restore the old process as an admitted secondary from durable state.
    let first = first.reopen_closed().await;
    first
        .effect(RuntimeEffectAction::AdmitAuthority(Box::new(authority(
            first.identity.clone(),
            current.clone(),
        ))))
        .await
        .unwrap();
    first
        .effect(RuntimeEffectAction::ChangeRole(
            ReplicaRole::ActiveSecondary,
        ))
        .await
        .unwrap();
    first
        .effect(RuntimeEffectAction::AuthorizeFailoverPrefix(
            first_receipt.lsn,
        ))
        .await
        .unwrap();
    let routes = route(&second, &[&first, &third]).await;
    let next = write_receipt(&second, 2).await;
    wait_applied(&[&first, &third], next.lsn).await;
    assert_eq!(
        third
            .application
            .persistence()
            .progress()
            .unwrap()
            .committed_lsn,
        next.lsn - 1
    );
    routes.stop().await;
    let _current = change_primary(
        &[&first, &second, &third],
        &current,
        &third,
        false,
        next.lsn,
    )
    .await;
    let receipts = [first_receipt, next];
    assert_eq!(
        third
            .application
            .persistence()
            .progress()
            .unwrap()
            .applied_lsn,
        receipts[1].lsn
    );
    assert_receipts(&third, &receipts).await;
    assert_closed(&second).await;
    assert_closed(&first).await;
    let third = third.reopen().await;
    assert_eq!(
        third
            .application
            .persistence()
            .progress()
            .unwrap()
            .committed_lsn,
        receipts[1].lsn
    );
    assert_receipts(&third, &receipts).await;
}
