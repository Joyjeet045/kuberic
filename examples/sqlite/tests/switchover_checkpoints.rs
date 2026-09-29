use kuberic_protocol::types::{AccessStatus, ReplicaRole};
use sqlite_replicated::proto;
use sqlite_replicated::proto::sqlite_store_server::SqliteStore as _;
use sqlite_replicated::testing::*;
use std::collections::BTreeSet;

#[derive(Debug)]
enum NewWrite {
    Accepted(SqlReceipt),
    Fenced(FencedProbe),
}

fn check_interval(
    outcomes: &[(PrimaryCheckpoint, i64, NewWrite)],
    checkpoint: PrimaryCheckpoint,
    expected_writer: Option<i64>,
) {
    let successful = outcomes
        .iter()
        .filter(|(at, _, _)| *at == checkpoint)
        .filter_map(|(_, replica, outcome)| {
            matches!(outcome, NewWrite::Accepted(_)).then_some(*replica)
        })
        .collect::<BTreeSet<_>>();
    assert!(
        successful.len() <= 1,
        "two replicas accepted new writes at {checkpoint:?}: {outcomes:?}"
    );
    assert_eq!(successful, expected_writer.into_iter().collect());
}

async fn both_closed(
    outcomes: &mut Vec<(PrimaryCheckpoint, i64, NewWrite)>,
    stage: PrimaryCheckpoint,
    source: &SqlitePod,
    target: &SqlitePod,
) {
    let (old, new) = tokio::join!(assert_closed(source), assert_closed(target));
    outcomes.push((
        stage,
        source.identity.replica_id.value(),
        NewWrite::Fenced(old),
    ));
    outcomes.push((
        stage,
        target.identity.replica_id.value(),
        NewWrite::Fenced(new),
    ));
    check_interval(outcomes, stage, None);
}

#[tokio::test]
async fn every_handoff_checkpoint_fences_new_old_primary_writes_while_delayed_success_is_separate()
{
    let root = scratch();
    let source = SqlitePod::new(1, root.path().join("source"), 3).await;
    let target = SqlitePod::new(2, root.path().join("target"), 3).await;
    let witness = SqlitePod::new(3, root.path().join("witness"), 3).await;
    let previous = bootstrap(&[&source, &target, &witness]).await;
    let routes = route(&source, &[&target, &witness]).await;
    create_data(&source).await;
    let baseline = write_receipt(&source, 10).await;
    wait_applied(&[&target, &witness], baseline.lsn).await;

    source.application.barrier().after_quorum.arm();
    let server = source.server.clone();
    let delayed = tokio::spawn(async move {
        server
            .execute(tonic::Request::new(proto::ExecuteRequest {
                sql: "INSERT INTO data VALUES(99,'delayed-before-handoff')".into(),
                params: Vec::new(),
            }))
            .await
    });
    source
        .application
        .barrier()
        .after_quorum
        .wait_entered()
        .await;
    let boundary = baseline.lsn + 1;
    wait_applied(&[&target, &witness], boundary).await;
    assert!(!delayed.is_finished());
    let delayed_receipt = SqlReceipt {
        id: 99,
        value: "delayed-before-handoff".into(),
        lsn: boundary,
    };
    let mut expected = vec![baseline, delayed_receipt.clone()];
    routes.stop().await;
    let mut transition = begin_primary_change(
        &[&source, &target, &witness],
        &previous,
        &target,
        true,
        boundary,
    )
    .await;
    let mut outcomes = Vec::new();
    assert!(source.application.primary_application_active_for_test());
    assert!(source.runtime.snapshot().await.open);
    assert_eq!(source.runtime.snapshot().await.role, ReplicaRole::Primary);
    assert_ne!(
        source.runtime.snapshot().await.write_status,
        AccessStatus::Granted
    );
    both_closed(&mut outcomes, PrimaryCheckpoint::Prepared, &source, &target).await;
    assert!(!delayed.is_finished());

    transition.install_authority().await;
    assert!(source.application.primary_application_active_for_test());
    both_closed(
        &mut outcomes,
        PrimaryCheckpoint::AuthorityInstalled,
        &source,
        &target,
    )
    .await;
    transition.activate_target().await;
    assert!(source.application.primary_application_active_for_test());
    assert!(target.application.primary_application_active_for_test());
    both_closed(
        &mut outcomes,
        PrimaryCheckpoint::TargetActivated,
        &source,
        &target,
    )
    .await;
    transition.grant_target().await;
    assert_eq!(
        transition.checkpoint,
        PrimaryCheckpoint::TargetGrantedOldAlive
    );
    assert!(source.application.primary_application_active_for_test());
    assert_eq!(source.runtime.snapshot().await.role, ReplicaRole::Primary);
    assert!(
        source.runtime.snapshot().await.open,
        "do not mistake death for fencing"
    );
    assert_receipts(&target, &expected).await; // Before any new SQL write on target.
    let (old, new) = tokio::join!(assert_closed(&source), write_receipt(&target, 200));
    expected.push(new.clone());
    outcomes.push((transition.checkpoint, 1, NewWrite::Fenced(old)));
    outcomes.push((transition.checkpoint, 2, NewWrite::Accepted(new)));
    check_interval(&outcomes, transition.checkpoint, Some(2));
    assert!(
        !delayed.is_finished(),
        "old response remains withheld during new-primary success"
    );
    assert_durable_receipts(&source, &expected[..2]);

    // Release only the already-quorum-committed old transaction. Its response
    // is deliberately not recorded as a successful *new* write in any interval.
    source.application.barrier().after_quorum.release();
    let delayed_response = tokio::time::timeout(std::time::Duration::from_secs(5), delayed)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_inner();
    assert_eq!(delayed_response.lsn, delayed_receipt.lsn);
    assert_eq!(delayed_response.rows_affected, 1);
    assert!(source.application.primary_application_active_for_test());
    assert_closed(&source).await;
    transition.demote_old().await;
    assert!(!source.application.primary_application_active_for_test());
    outcomes.push((
        transition.checkpoint,
        1,
        NewWrite::Fenced(assert_closed(&source).await),
    ));
    let receipt = write_receipt(&target, 201).await;
    expected.push(receipt.clone());
    outcomes.push((transition.checkpoint, 2, NewWrite::Accepted(receipt)));
    check_interval(&outcomes, transition.checkpoint, Some(2));
    transition.complete().await;
    assert_receipts(&target, &expected).await;
    assert_durable_receipts(&source, &expected);
    assert_durable_receipts(&witness, &expected);
    assert_closed(&source).await;
    assert_closed(&witness).await;
    let routes = route(&target, &[&source, &witness]).await;
    // The demoted replica now receives legitimate replication, so complete its
    // no-side-effect probe before sending the next accepted transaction.
    let old = assert_closed(&source).await;
    let new = write_receipt(&target, 202).await;
    expected.push(new.clone());
    outcomes.push((PrimaryCheckpoint::Complete, 1, NewWrite::Fenced(old)));
    outcomes.push((PrimaryCheckpoint::Complete, 2, NewWrite::Accepted(new)));
    check_interval(&outcomes, PrimaryCheckpoint::Complete, Some(2));
    wait_applied(&[&source, &witness], expected.last().unwrap().lsn).await;
    routes.stop().await;
    assert_receipts(&target, &expected).await;
    assert_durable_receipts(&source, &expected);
    assert_durable_receipts(&witness, &expected);

    let probes = outcomes
        .iter()
        .filter_map(|(_, _, outcome)| match outcome {
            NewWrite::Fenced(probe) => Some(probe.id),
            NewWrite::Accepted(receipt) => {
                assert!(receipt.lsn > delayed_receipt.lsn);
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(probes.iter().collect::<BTreeSet<_>>().len(), probes.len());
    let target = target.reopen().await;
    assert_receipts(&target, &expected).await;
    let source = source.reopen().await;
    assert_closed(&source).await;
    assert_durable_receipts(&source, &expected);
    assert_durable_receipts(&witness, &expected);
}
