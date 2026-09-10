use std::time::Duration;

use kuberic_core::driver::ReplicaHandle;
use kuberic_core::types::{
    CorrelatedControlActionRequest, DurableActionState, DurableReplicaAction, Epoch, OpenMode, Role,
};
use kvstore::proto::{self, kv_store_client::KvStoreClient};
use kvstore::state::{COMMIT_HISTORY_LIMIT, KvMutation, KvOp, KvState};
use kvstore::testing::{KvPod, connect_kv_client};
use tonic::Code;
use tonic::transport::Channel;

async fn primary() -> (KvPod, KvStoreClient<Channel>) {
    let pod = KvPod::start(1).await;
    let handle = pod.replica_handle(1).await;
    for (action_id, action) in [
        (
            "open",
            DurableReplicaAction::Open {
                mode: OpenMode::New,
            },
        ),
        (
            "primary",
            DurableReplicaAction::ChangeRole {
                epoch: Epoch::new(0, 1),
                role: Role::Primary,
            },
        ),
        (
            "configuration",
            DurableReplicaAction::UpdateCurrentConfiguration {
                current: kuberic_core::types::ReplicaSetConfig {
                    members: Vec::new(),
                    write_quorum: 1,
                },
            },
        ),
    ] {
        let status = handle.get_status().await.unwrap();
        let result = handle
            .execute_correlated_control_action(CorrelatedControlActionRequest {
                protocol_version: kuberic_core::replica_agent::CORRELATED_CONTROL_PROTOCOL_VERSION,
                action_id: action_id.into(),
                input_signature: action.signature(),
                target_replica_id: 1,
                target_instance_id: status.instance_id,
                expected_agent_generation: status.agent.generation,
                expected_control_version: status.agent.control_version,
                observed_runtime_epoch: status.epoch,
                action,
            })
            .await
            .unwrap();
        assert_ne!(result.observation.action.state, DurableActionState::Failed);
    }
    let client = connect_kv_client(&pod.client_address).await;
    (pod, client)
}

async fn begin(client: &mut KvStoreClient<Channel>) -> String {
    client
        .begin_transaction(proto::BeginTransactionRequest::default())
        .await
        .unwrap()
        .into_inner()
        .transaction_id
}

async fn stage(client: &mut KvStoreClient<Channel>, transaction_id: &str, key: &str, value: &str) {
    client
        .transaction_put(proto::TransactionPutRequest {
            transaction_id: transaction_id.into(),
            key: key.into(),
            value: value.into(),
        })
        .await
        .unwrap();
}

fn handle(transaction_id: &str) -> proto::TransactionRequest {
    proto::TransactionRequest {
        transaction_id: transaction_id.into(),
    }
}

fn batch(transaction_id: &str, value: &str) -> proto::ExecuteTransactionRequest {
    proto::ExecuteTransactionRequest {
        transaction_id: transaction_id.into(),
        mutations: ["left", "right"]
            .into_iter()
            .map(|key| proto::TransactionMutation {
                mutation: Some(proto::transaction_mutation::Mutation::Put(
                    proto::PutRequest {
                        key: key.into(),
                        value: value.into(),
                    },
                )),
            })
            .collect(),
    }
}

#[tokio::test]
async fn interactive_commit_is_one_wal_record_with_read_your_own_writes() {
    let (pod, mut client) = primary().await;
    client
        .put(proto::PutRequest {
            key: "removed".into(),
            value: "old".into(),
        })
        .await
        .unwrap();
    let transaction_id = begin(&mut client).await;
    stage(&mut client, &transaction_id, "first", "one").await;
    stage(&mut client, &transaction_id, "second", "two").await;
    client
        .transaction_delete(proto::TransactionDeleteRequest {
            transaction_id: transaction_id.clone(),
            key: "removed".into(),
        })
        .await
        .unwrap();
    for (key, expected) in [
        ("first", Some("one")),
        ("second", Some("two")),
        ("removed", None),
    ] {
        let result = client
            .transaction_get(proto::TransactionGetRequest {
                transaction_id: transaction_id.clone(),
                key: key.into(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(result.found, expected.is_some());
        assert_eq!(result.value, expected.unwrap_or_default());
    }
    assert_eq!(pod.state.read().await.data.len(), 1);
    let lsn = client
        .commit_transaction(handle(&transaction_id))
        .await
        .unwrap()
        .into_inner()
        .lsn;
    assert_eq!(lsn, 2);
    assert_eq!(
        client
            .commit_transaction(handle(&transaction_id))
            .await
            .unwrap()
            .into_inner()
            .lsn,
        lsn
    );
    assert_eq!(
        client
            .abort_transaction(handle(&transaction_id))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let wal = tokio::fs::read_to_string(pod.data_dir.join("wal.log"))
        .await
        .unwrap();
    assert_eq!(wal.lines().count(), 2);
    let record: kvstore::persistence::WalEntry =
        serde_json::from_str(wal.lines().last().unwrap()).unwrap();
    assert_eq!(record.lsn, lsn);
    assert!(matches!(record.op, KvOp::Transaction { mutations, .. } if mutations.len() == 3));
    let recovered = KvState::open(pod.data_dir.clone()).await.unwrap();
    assert_eq!(recovered.data.len(), 2);
    assert_eq!(recovered.transaction_lsn(&transaction_id), Some(lsn));
    assert!(!recovered.data.contains_key("removed"));
}

#[tokio::test]
async fn conflicting_commits_abort_but_disjoint_commits_succeed() {
    let (_pod, mut client) = primary().await;
    let first = begin(&mut client).await;
    let second = begin(&mut client).await;
    stage(&mut client, &first, "shared", "first").await;
    stage(&mut client, &second, "shared", "second").await;
    let mut other = client.clone();
    let (first_result, second_result) = tokio::join!(
        client.commit_transaction(handle(&first)),
        other.commit_transaction(handle(&second)),
    );
    assert_ne!(first_result.is_ok(), second_result.is_ok());
    assert_eq!(
        first_result.err().or(second_result.err()).unwrap().code(),
        Code::Aborted
    );

    let first = begin(&mut client).await;
    let second = begin(&mut client).await;
    stage(&mut client, &first, "left", "one").await;
    stage(&mut client, &second, "right", "two").await;
    let (first_result, second_result) = tokio::join!(
        client.commit_transaction(handle(&first)),
        other.commit_transaction(handle(&second)),
    );
    assert!(first_result.is_ok());
    assert!(second_result.is_ok());
}

#[tokio::test]
async fn ordinary_writes_conflict_with_reads_and_blind_writes_including_absent_key_aba() {
    let (_pod, mut client) = primary().await;
    for blind_write in [false, true] {
        let transaction_id = begin(&mut client).await;
        if blind_write {
            stage(&mut client, &transaction_id, "absent", "staged").await;
        } else {
            assert!(
                !client
                    .transaction_get(proto::TransactionGetRequest {
                        transaction_id: transaction_id.clone(),
                        key: "absent".into(),
                    })
                    .await
                    .unwrap()
                    .into_inner()
                    .found
            );
        }
        client
            .put(proto::PutRequest {
                key: "absent".into(),
                value: "intervening".into(),
            })
            .await
            .unwrap();
        assert!(
            client
                .delete(proto::DeleteRequest {
                    key: "absent".into()
                })
                .await
                .unwrap()
                .into_inner()
                .existed
        );
        assert_eq!(
            client
                .commit_transaction(handle(&transaction_id))
                .await
                .unwrap_err()
                .code(),
            Code::Aborted
        );
    }
}

#[tokio::test]
async fn repeatable_reads_and_write_skew_are_validated_at_commit() {
    let (_pod, mut client) = primary().await;
    client
        .execute_transaction(batch("initial", "on"))
        .await
        .unwrap();
    let first = begin(&mut client).await;
    let second = begin(&mut client).await;
    for transaction_id in [&first, &second] {
        for key in ["left", "right"] {
            assert_eq!(
                client
                    .transaction_get(proto::TransactionGetRequest {
                        transaction_id: transaction_id.clone(),
                        key: key.into(),
                    })
                    .await
                    .unwrap()
                    .into_inner()
                    .value,
                "on"
            );
        }
    }
    stage(&mut client, &first, "left", "off").await;
    stage(&mut client, &second, "right", "off").await;
    client.commit_transaction(handle(&first)).await.unwrap();
    assert_eq!(
        client
            .transaction_get(proto::TransactionGetRequest {
                transaction_id: second.clone(),
                key: "left".into(),
            })
            .await
            .unwrap()
            .into_inner()
            .value,
        "on"
    );
    assert_eq!(
        client
            .commit_transaction(handle(&second))
            .await
            .unwrap_err()
            .code(),
        Code::Aborted
    );
}

#[tokio::test]
async fn abort_expiry_and_invalid_batches_do_not_publish_mutations() {
    let (pod, mut client) = primary().await;
    let transaction_id = begin(&mut client).await;
    stage(&mut client, &transaction_id, "aborted", "value").await;
    client
        .abort_transaction(handle(&transaction_id))
        .await
        .unwrap();
    client
        .abort_transaction(handle(&transaction_id))
        .await
        .unwrap();
    assert_eq!(
        client
            .commit_transaction(handle(&transaction_id))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    let transaction_id = client
        .begin_transaction(proto::BeginTransactionRequest { timeout_ms: 20 })
        .await
        .unwrap()
        .into_inner()
        .transaction_id;
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(
        client
            .commit_transaction(handle(&transaction_id))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    assert_eq!(
        client
            .begin_transaction(proto::BeginTransactionRequest { timeout_ms: 60_001 })
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let mut invalid = batch("invalid", "value");
    invalid
        .mutations
        .push(proto::TransactionMutation::default());
    assert_eq!(
        client
            .execute_transaction(invalid)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert!(pod.state.read().await.data.is_empty());
    assert_eq!(pod.state.read().await.last_applied_lsn, 0);
}

#[tokio::test]
async fn concurrent_batch_retries_and_readers_observe_only_whole_commits() {
    let (pod, mut client) = primary().await;
    let state = pod.state.clone();
    let reader = tokio::spawn(async move {
        for _ in 0..5000 {
            let state = state.read().await;
            assert_eq!(state.data.get("left"), state.data.get("right"));
            drop(state);
            tokio::task::yield_now().await;
        }
    });
    let mut other = client.clone();
    let (first, second) = tokio::join!(
        client.execute_transaction(batch("retry", "one")),
        other.execute_transaction(batch("retry", "one")),
    );
    assert_eq!(
        first.unwrap().into_inner().lsn,
        second.unwrap().into_inner().lsn
    );
    for index in 0..30 {
        client
            .execute_transaction(batch(&format!("next-{index}"), &index.to_string()))
            .await
            .unwrap();
    }
    assert_eq!(
        client
            .execute_transaction(batch("retry", "must-not-overwrite"))
            .await
            .unwrap()
            .into_inner()
            .lsn,
        1
    );
    assert_eq!(pod.state.read().await.data["left"], "29");
    assert_eq!(pod.state.read().await.last_applied_lsn, 31);
    reader.await.unwrap();
}

#[tokio::test]
async fn checkpoint_copy_rollback_and_dedup_retention_preserve_transaction_boundaries() {
    let (pod, _client) = primary().await;
    let mut state = pod.state.write().await;
    for lsn in 1..=3 {
        state
            .apply_op(
                lsn,
                &KvOp::Transaction {
                    transaction_id: format!("transaction-{lsn}"),
                    mutations: vec![
                        KvMutation::Put {
                            key: "left".into(),
                            value: lsn.to_string(),
                        },
                        KvMutation::Put {
                            key: "right".into(),
                            value: lsn.to_string(),
                        },
                    ],
                },
            )
            .await
            .unwrap();
    }
    let snapshot = state.snapshot_at(2).await.unwrap();
    assert_eq!(snapshot.data["left"], "2");
    assert_eq!(snapshot.data["right"], "2");
    assert!(
        !snapshot
            .committed_transactions
            .contains_key("transaction-3")
    );
    state.rollback_to(2).await.unwrap();
    assert_eq!(state.data["left"], "2");
    assert_eq!(state.data["right"], "2");
    assert_eq!(state.transaction_lsn("transaction-3"), None);
    state.set_committed_lsn(2);
    state.checkpoint().await.unwrap();
    let mut recovered = KvState::open(pod.data_dir.clone()).await.unwrap();
    assert_eq!(recovered.transaction_lsn("transaction-2"), Some(2));
    for index in 0..COMMIT_HISTORY_LIMIT {
        recovered
            .apply_op(
                3 + index as i64,
                &KvOp::Transaction {
                    transaction_id: format!("retained-{index}"),
                    mutations: Vec::new(),
                },
            )
            .await
            .unwrap();
    }
    assert_eq!(recovered.transaction_lsn("transaction-2"), None);
    assert_eq!(
        recovered.snapshot().committed_transactions.len(),
        COMMIT_HISTORY_LIMIT
    );
    recovered.set_committed_lsn(recovered.last_applied_lsn);
    recovered.checkpoint().await.unwrap();
    let recovered = KvState::open(pod.data_dir.clone()).await.unwrap();
    assert_eq!(
        recovered.snapshot().committed_transactions.len(),
        COMMIT_HISTORY_LIMIT
    );
    assert_eq!(recovered.transaction_lsn("retained-0"), Some(3));
}
