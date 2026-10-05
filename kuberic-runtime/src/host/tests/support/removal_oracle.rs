use super::*;

#[derive(Default)]
struct SuccessfulWrites(BTreeMap<OperationId, (i64, Bytes)>);

impl SuccessfulWrites {
    fn observe(&mut self, write: &ClientWrite, result: Result<crate::application::WriteReceipt>) {
        if let Ok(receipt) = result {
            assert!(receipt.committed_lsn >= receipt.lsn);
            let value = (receipt.lsn, write.data.clone());
            if let Some(previous) = self.0.insert(write.operation_id.clone(), value.clone()) {
                assert_eq!(
                    previous, value,
                    "retry changed the successful operation identity"
                );
            }
        }
    }

    async fn assert_preserved(&self, app: &TestApplication, store: &MemoryAuthorityStore) {
        for (id, (lsn, data)) in &self.0 {
            assert_eq!(app.applied.lock().unwrap()[lsn].data, *data, "{id}");
            assert!(app.progress.lock().unwrap().committed_lsn >= *lsn, "{id}");
            let original = store.load_local_write(id).await.unwrap().unwrap();
            assert_eq!(original.operation_id, *id);
            assert_eq!(original.lsn, *lsn);
            assert_eq!(original.data, *data);
        }
    }
}

async fn write_success(runtime: &PodRuntime, oracle: &mut SuccessfulWrites, label: &str) {
    eprintln!("write {label}");
    let write = ClientWrite {
        operation_id: OperationId::new(label),
        data: Bytes::from(format!("value:{label}")),
    };
    let pending = runtime
        .data_plane()
        .begin_write(write.clone())
        .await
        .unwrap();
    let authority = runtime.snapshot().await.authority.unwrap();
    for member in authority.current_configuration.members.iter().skip(1) {
        runtime
            .data_plane()
            .accept_acknowledgement(acknowledgement(
                &authority,
                member.identity.clone(),
                pending.lsn,
            ))
            .await
            .unwrap();
    }
    let result = timeout(Duration::from_secs(5), pending.committed())
        .await
        .unwrap_or_else(|_| panic!("write completion stalled: {label}"));
    assert!(result.is_ok(), "{label}: {result:?}");
    oracle.observe(&write, result);
}

#[tokio::test]
async fn successful_write_oracle_survives_each_sequential_removal_and_singleton_restart() {
    let mut intent = removal_fixture::intent(&[1, 2, 3, 4, 5], 1);
    let store = Arc::new(MemoryAuthorityStore::default());
    let mut app = Arc::new(TestApplication::default());
    let mut runtime =
        open_removal_member(&intent, intent.primary.clone(), app.clone(), store.clone()).await;
    let mut oracle = SuccessfulWrites::default();
    let mut sequence = 5;
    for size in (2..=5).rev() {
        eprintln!("oracle reduction {size}");
        let old = runtime.snapshot().await.authority.unwrap();
        intent = removal_fixture::intent(&(1..=size).collect::<Vec<_>>(), 1);
        intent.previous_configuration = old.current_configuration.clone();
        intent.current_configuration = ConfigurationDescriptor::new(
            Epoch::new(
                old.current_configuration.epoch.data_loss_number,
                old.current_configuration.epoch.configuration_number + 1,
            ),
            old.current_configuration.primary_id,
            intent.current_configuration.members.clone(),
            intent.current_policy.write_quorum,
        );
        intent.operation_id = intent.expected_operation_id();
        write_success(&runtime, &mut oracle, &format!("before-{size}")).await;
        let unknown = ClientWrite {
            operation_id: OperationId::new(format!("unknown-{size}")),
            data: Bytes::from(format!("unknown-value-{size}")),
        };
        let pending = runtime
            .data_plane()
            .begin_write(unknown.clone())
            .await
            .unwrap();
        let unknown_lsn = pending.lsn;
        let successes = oracle.0.len();
        timeout(
            Duration::from_secs(5),
            recovery_action(&runtime, &mut sequence, prepare_removal(&intent)),
        )
        .await
        .unwrap();
        let failure = pending.committed().await;
        assert!(failure.is_err());
        oracle.observe(&unknown, failure);
        assert_eq!(
            oracle.0.len(),
            successes,
            "failed client is not acknowledged"
        );
        assert!(!oracle.0.contains_key(&unknown.operation_id));
        // The application fixture has no network dispatcher. Supply fair exact
        // peer ACKs while access recovery resolves the original unknown write.
        let acknowledgements = {
            let runtime = runtime.clone();
            tokio::spawn(async move {
                loop {
                    let authority = runtime.snapshot().await.authority.unwrap();
                    for member in authority.current_configuration.members.iter().skip(1) {
                        let _ = runtime
                            .data_plane()
                            .accept_acknowledgement(acknowledgement(
                                &authority,
                                member.identity.clone(),
                                unknown_lsn,
                            ))
                            .await;
                    }
                    tokio::task::yield_now().await;
                }
            })
        };
        timeout(
            Duration::from_secs(5),
            converge_removal(&runtime, &mut sequence),
        )
        .await
        .unwrap();
        acknowledgements.abort();
        let _ = acknowledgements.await;
        oracle.assert_preserved(&app, &store).await;
        assert_eq!(
            store
                .load_local_write(&unknown.operation_id)
                .await
                .unwrap()
                .unwrap()
                .lsn,
            unknown_lsn
        );
        assert_eq!(app.applied.lock().unwrap()[&unknown_lsn].data, unknown.data);
        write_success(&runtime, &mut oracle, &format!("after-{size}")).await;
        oracle.assert_preserved(&app, &store).await;

        let committed = runtime.snapshot().await.accepted_secondary_removal.unwrap();
        let operations = app.applied.lock().unwrap().clone();
        let progress = *app.progress.lock().unwrap();
        runtime.abort();
        drop(runtime);
        app = Arc::new(TestApplication::default());
        *app.applied.lock().unwrap() = operations;
        *app.progress.lock().unwrap() = progress;
        runtime = Arc::new(PodRuntime::new(
            intent.primary.clone(),
            app.clone(),
            store.clone(),
        ));
        runtime
            .reconstruct(
                OpenMode::Existing,
                ReplicaRole::Primary,
                AccessStatus::ReconfigurationPending,
                AccessStatus::ReconfigurationPending,
                None,
            )
            .await
            .unwrap();
        sequence = 1;
        recovery_action(
            &runtime,
            &mut sequence,
            RuntimeEffectAction::AcceptSecondaryRemovalCommit(Box::new(committed)),
        )
        .await;
        recovery_action(
            &runtime,
            &mut sequence,
            RuntimeEffectAction::SetAccessStatus {
                read: AccessStatus::Granted,
                write: AccessStatus::Granted,
            },
        )
        .await;
        oracle.assert_preserved(&app, &store).await;
        write_success(&runtime, &mut oracle, &format!("restart-{size}")).await;
    }
    assert_eq!(
        runtime
            .snapshot()
            .await
            .authority
            .unwrap()
            .current_configuration
            .members
            .len(),
        1
    );
    assert_eq!(oracle.0.len(), 12);
    oracle.assert_preserved(&app, &store).await;
}

#[tokio::test]
async fn successful_write_oracle_covers_application_ack_preparation_race() {
    for ack_first in [false, true] {
        let intent = removal_fixture::intent(&[1, 2], 1);
        let store = Arc::new(MemoryAuthorityStore::default());
        let app = Arc::new(TestApplication::default());
        let runtime =
            open_removal_member(&intent, intent.primary.clone(), app.clone(), store.clone()).await;
        let mut oracle = SuccessfulWrites::default();
        write_success(&runtime, &mut oracle, "known-success").await;
        let write = ClientWrite {
            operation_id: OperationId::new("original-racing-id"),
            data: Bytes::from_static(b"original-racing-data"),
        };
        let pending = runtime
            .data_plane()
            .begin_write(write.clone())
            .await
            .unwrap();
        let old = runtime.snapshot().await.authority.unwrap();
        let ack = acknowledgement(&old, intent.target.clone(), pending.lsn);
        if ack_first {
            runtime
                .data_plane()
                .accept_acknowledgement(ack.clone())
                .await
                .unwrap();
        }
        let mut sequence = 5;
        recovery_action(&runtime, &mut sequence, prepare_removal(&intent)).await;
        if !ack_first {
            runtime
                .data_plane()
                .accept_acknowledgement(ack)
                .await
                .unwrap();
        }
        let outcome = pending.committed().await;
        assert_eq!(outcome.is_ok(), ack_first);
        oracle.observe(&write, outcome);
        assert_eq!(oracle.0.len(), if ack_first { 2 } else { 1 });
        converge_removal(&runtime, &mut sequence).await;
        oracle.assert_preserved(&app, &store).await;
        write_success(&runtime, &mut oracle, "fresh-singleton").await;
        oracle.assert_preserved(&app, &store).await;
    }
}
