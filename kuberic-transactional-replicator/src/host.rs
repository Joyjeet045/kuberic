use super::*;
use kuberic_core::events::{LifecycleEvent, StateProviderEvent};
use kuberic_core::replicator::WalReplicator;
use kuberic_core::types::{Operation, OperationStream};
use tokio::sync::mpsc;

impl<State: TransactionalStateProvider> TransactionalReplicator<State> {
    async fn drain(
        &self,
        stream: &mut OperationStream,
        token: CancellationToken,
        copy: bool,
    ) -> Result<()> {
        let mut contents = Vec::new();
        loop {
            let operation = tokio::select! {
                _ = token.cancelled() => return if copy { Err(Error::RecoveryRequired) } else { Ok(()) },
                operation = stream.get_operation() => operation,
            };
            let Some(operation) = operation else { break };
            if copy {
                if contents.len() + operation.data.len() > MAX_SNAPSHOT_BYTES + 32 {
                    return Err(Error::ResourceExhausted);
                }
                contents.extend_from_slice(&operation.data);
            } else {
                let payload = operation.data.to_vec();
                let lsn = operation.lsn;
                self.access(move |inner| {
                    if inner.failed {
                        return Err(Error::RecoveryRequired);
                    }
                    if lsn <= inner.snapshot.applied_lsn {
                        if let Some(record) =
                            inner.log.records().iter().find(|record| record.lsn == lsn)
                        {
                            return if record.payload == payload {
                                Ok(())
                            } else {
                                Err(Error::DuplicateRequest)
                            };
                        }
                        let envelope: Envelope<State::Command> =
                            decode(&payload, MAX_TRANSACTION_BYTES)?;
                        let digest =
                            Sha256::digest(postcard::to_allocvec(&envelope.command)?).into();
                        if envelope.format != FORMAT
                            || envelope.provider_format != State::FORMAT_ID
                            || envelope.confirmed_lsn < 0
                            || envelope.confirmed_lsn >= lsn
                            || retained_result(&inner.snapshot, &envelope.identity, digest)?
                                != Some(CommitVersion(lsn))
                        {
                            return Err(Error::DuplicateRequest);
                        }
                        return Ok(());
                    }
                    let snapshot = next_snapshot(&inner.snapshot, lsn, &payload)?;
                    let envelope: Envelope<State::Command> =
                        decode(&payload, MAX_TRANSACTION_BYTES)?;
                    inner.confirmed_lsn = inner.confirmed_lsn.max(envelope.confirmed_lsn);
                    reclaim_if_needed(inner, payload.len())?;
                    inner.log.append(Record { lsn, payload })?;
                    inner.snapshot = snapshot;
                    Ok(())
                })
                .await?;
            }
            operation.acknowledge();
        }
        if copy {
            let lsn = stream
                .copy_lsn()
                .ok_or_else(|| Error::Invalid("copy boundary missing".into()))?;
            self.access(move |inner| {
                let snapshot = checked_snapshot::<State>(&contents)?;
                if snapshot.applied_lsn != lsn {
                    return Err(Error::Invalid("copy LSN mismatch".into()));
                }
                inner.failed = true;
                inner.log.install_checkpoint(Record {
                    lsn,
                    payload: contents,
                })?;
                inner.snapshot = snapshot;
                inner.confirmed_lsn = lsn;
                inner.failed = false;
                Ok(())
            })
            .await?;
            stream.acknowledge_completion()?;
        }
        Ok(())
    }

    async fn provider(&self, event: StateProviderEvent) {
        match event {
            StateProviderEvent::GetLastCommittedLsn { reply } => {
                let _ = reply.send(self.applied_lsn().await.map_err(core_error));
            }
            StateProviderEvent::GetCopyContext { reply } => {
                let (sender, stream) = OperationStream::channel(1);
                drop(sender);
                let _ = reply.send(Ok(stream));
            }
            StateProviderEvent::GetCopyState {
                up_to_lsn, reply, ..
            } => {
                let replica = self.clone();
                tokio::spawn(async move {
                    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
                        while replica.applied_lsn().await? < up_to_lsn {
                            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                        }
                        replica
                            .access(move |inner| {
                                encode(
                                    &recover::<State>(&inner.log, up_to_lsn)?,
                                    MAX_SNAPSHOT_BYTES,
                                )
                            })
                            .await
                    })
                    .await
                    .unwrap_or(Err(Error::RecoveryRequired));
                    match result {
                        Ok(contents) => {
                            let (sender, stream) = OperationStream::channel(4);
                            let _ = reply.send(Ok(stream));
                            for chunk in contents.chunks(256 * 1024) {
                                if sender
                                    .send(Operation::new(
                                        up_to_lsn,
                                        Bytes::copy_from_slice(chunk),
                                        None,
                                    ))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                        }
                        Err(error) => {
                            let _ = reply.send(Err(core_error(error)));
                        }
                    }
                });
            }
            StateProviderEvent::UpdateEpoch {
                previous_epoch_last_lsn,
                reply,
                ..
            } => {
                let result = self
                    .access(move |inner| {
                        inner.generation += 1;
                        if previous_epoch_last_lsn < inner.snapshot.applied_lsn {
                            let snapshot = recover::<State>(&inner.log, previous_epoch_last_lsn)?;
                            inner.log.rollback(previous_epoch_last_lsn)?;
                            inner.snapshot = snapshot;
                            inner.confirmed_lsn = inner.confirmed_lsn.min(previous_epoch_last_lsn);
                        }
                        Ok(())
                    })
                    .await;
                if result.is_err() {
                    self.fault().await;
                }
                let _ = reply.send(result.map_err(core_error));
            }
            StateProviderEvent::OnDataLoss { reply } => {
                let _ = reply.send(Ok(false));
            }
        }
    }

    pub async fn run(self, mut lifecycle: mpsc::Receiver<LifecycleEvent>) {
        let mut provider: Option<mpsc::UnboundedReceiver<StateProviderEvent>> = None;
        let mut copy_stream = None;
        let mut replication_stream = None;
        let mut drain: Option<tokio::task::JoinHandle<(Option<OperationStream>, Result<()>)>> =
            None;
        let mut drain_token = CancellationToken::new();
        let mut role = Role::Unknown;
        loop {
            tokio::select! {
                biased;
                event = lifecycle.recv() => {
                    let Some(event) = event else { break };
                    match event {
                        LifecycleEvent::Open { ctx, reply } => {
                            let (sender, receiver) = mpsc::unbounded_channel();
                            match WalReplicator::create(ctx.replica_id, &ctx.data_bind, ctx.fault_tx, sender).await {
                                Ok((handle, handles)) => {
                                    copy_stream = handles.copy_stream;
                                    replication_stream = handles.replication_stream;
                                    let result = self.access(move |inner| {
                                        inner.partition = Some(handles.partition);
                                        inner.replicator = Some(handles.replicator);
                                        inner.token = ctx.token;
                                        Ok(())
                                    }).await;
                                    provider = Some(receiver);
                                    let _ = reply.send(result.map(|()| handle).map_err(core_error));
                                }
                                Err(error) => { let _ = reply.send(Err(error)); }
                            }
                        }
                        LifecycleEvent::ChangeRole { new_role, reply } => {
                            let result = async {
                                if role == new_role { return Ok(()); }
                                if role != Role::IdleSecondary || !matches!(new_role, Role::ActiveSecondary | Role::Primary) { drain_token.cancel(); }
                                if let Some(handle) = drain.take() {
                                    let (stream, result) = handle.await.map_err(|_| Error::RecoveryRequired)?;
                                    if role == Role::ActiveSecondary { replication_stream = stream; }
                                    if matches!(new_role, Role::ActiveSecondary | Role::Primary) { result?; }
                                }
                                self.access(move |inner| {
                                    if inner.failed && matches!(new_role, Role::Primary | Role::ActiveSecondary) { return Err(Error::RecoveryRequired); }
                                    inner.generation += 1;
                                    inner.role = new_role;
                                    Ok(())
                                }).await?;
                                drain_token = CancellationToken::new();
                                let stream = match new_role { Role::IdleSecondary => copy_stream.take(), Role::ActiveSecondary => replication_stream.take(), _ => None };
                                if let Some(mut stream) = stream {
                                    let replica = self.clone();
                                    let token = drain_token.clone();
                                    drain = Some(tokio::spawn(async move {
                                        let result = replica.drain(&mut stream, token.clone(), new_role == Role::IdleSecondary).await;
                                        if result.is_err() && (new_role == Role::ActiveSecondary || !token.is_cancelled()) { replica.fault().await; }
                                        ((new_role == Role::ActiveSecondary).then_some(stream), result)
                                    }));
                                }
                                role = new_role;
                                Ok(())
                            }.await;
                            if result.is_err() { self.fault().await; }
                            let _ = reply.send(result.map(|()| String::new()).map_err(core_error));
                        }
                        LifecycleEvent::Close { reply } => {
                            drain_token.cancel();
                            if let Some(handle) = drain.take() { let _ = handle.await; }
                            let result = self.access(|inner| { inner.role = Role::None; inner.generation += 1; inner.token.cancel(); Ok(()) }).await;
                            let _ = reply.send(result.map_err(core_error));
                            break;
                        }
                        LifecycleEvent::Abort => break,
                    }
                }
                Some(event) = async { match provider.as_mut() {
                    Some(receiver) => receiver.recv().await,
                    None => std::future::pending().await,
                }} => self.provider(event).await,
            }
        }
        drain_token.cancel();
        if let Some(handle) = drain {
            let _ = handle.await;
        }
        let _ = self
            .access(|inner| {
                inner.role = Role::None;
                inner.token.cancel();
                Ok(())
            })
            .await;
    }
}

fn core_error(error: Error) -> kuberic_core::KubericError {
    kuberic_core::KubericError::Internal(error.to_string().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kuberic_core::events::ReplicateRequest;
    use kuberic_core::handles::PartitionState;

    #[derive(Clone, Default, Serialize, Deserialize)]
    struct Counter(u64);

    impl TransactionalStateProvider for Counter {
        const FORMAT_ID: &'static str = "counter/1";
        type Command = u64;
        type Observations = u64;
        fn validate(&self, observed: &u64, _command: &u64) -> Result<()> {
            if self.0 != *observed {
                return Err(Error::Conflict("counter changed".into()));
            }
            Ok(())
        }
        fn apply(&mut self, command: &u64, _version: CommitVersion) -> Result<()> {
            self.0 = self
                .0
                .checked_add(*command)
                .ok_or_else(|| Error::Invalid("counter overflow".into()))?;
            Ok(())
        }
        fn validate_snapshot(&self, _version: CommitVersion) -> Result<()> {
            Ok(())
        }
    }

    async fn primary(
        path: PathBuf,
    ) -> (
        TransactionalReplicator<Counter>,
        mpsc::Receiver<ReplicateRequest>,
    ) {
        let replica = TransactionalReplicator::open(path).await.unwrap();
        let partition = Arc::new(PartitionState::new());
        partition.set_write_status(AccessStatus::Granted);
        let (fault_tx, _fault_rx) = mpsc::channel(1);
        let (data_tx, data_rx) = mpsc::channel(1);
        replica
            .access(move |inner| {
                inner.role = Role::Primary;
                inner.partition = Some(Arc::new(PartitionHandle::new(partition.clone(), fault_tx)));
                inner.replicator = Some(StateReplicatorHandle::new(data_tx, partition));
                Ok(())
            })
            .await
            .unwrap();
        (replica, data_rx)
    }

    fn identity(transaction: u128) -> TransactionId {
        TransactionId {
            transaction,
            request: format!("request-{transaction}"),
        }
    }

    #[tokio::test]
    async fn cancelled_replication_drain_can_resume() {
        let directory = tempfile::tempdir().unwrap();
        let replica = TransactionalReplicator::<Counter>::open(directory.path().into())
            .await
            .unwrap();
        let token = CancellationToken::new();
        token.cancel();
        let (sender, mut stream) = OperationStream::channel(1);
        replica.drain(&mut stream, token, false).await.unwrap();
        let payload = encode(
            &Envelope {
                format: FORMAT,
                provider_format: Counter::FORMAT_ID.into(),
                confirmed_lsn: 0,
                identity: identity(1),
                command: 7u64,
            },
            MAX_TRANSACTION_BYTES,
        )
        .unwrap();
        sender
            .send(Operation::new(1, payload.into(), None))
            .await
            .unwrap();
        drop(sender);
        replica
            .drain(&mut stream, CancellationToken::new(), false)
            .await
            .unwrap();
        assert_eq!(replica.applied_lsn().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn crash_writer() {
        let Ok(path) = std::env::var("KUBERIC_RC_CRASH_PATH") else {
            return;
        };
        let stage = std::env::var("KUBERIC_RC_CRASH_STAGE").unwrap();
        let (replica, mut requests) = primary(path.into()).await;
        let (context, state) = replica.begin(TransactionOptions::default()).await.unwrap();
        let writer = replica.clone();
        let commit =
            tokio::spawn(async move { writer.commit(context, identity(1), state.0, 7).await });
        let request = requests.recv().await.unwrap();
        if stage == "accepted" {
            std::process::exit(0);
        }
        if stage == "durable" {
            replica
                .access(move |inner| {
                    inner.log.append(Record {
                        lsn: 1,
                        payload: request.data.to_vec(),
                    })?;
                    assert_eq!(inner.snapshot.applied_lsn, 0);
                    Ok(())
                })
                .await
                .unwrap();
            std::process::exit(0);
        }
        request.reply.send(Ok(1)).unwrap();
        if stage == "quorum" {
            std::process::exit(0);
        }
        assert_eq!(commit.await.unwrap().unwrap(), CommitVersion(1));
        if stage == "checkpointed" {
            replica.checkpoint().await.unwrap();
        }
        std::process::exit(0);
    }

    #[tokio::test]
    async fn abrupt_exit_recovers_transaction_at_each_commit_boundary() {
        for stage in ["accepted", "quorum", "durable", "applied", "checkpointed"] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().to_path_buf();
            let status = tokio::task::spawn_blocking(move || {
                std::process::Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", "host::tests::crash_writer", "--nocapture"])
                    .env("KUBERIC_RC_CRASH_PATH", path)
                    .env("KUBERIC_RC_CRASH_STAGE", stage)
                    .status()
                    .unwrap()
            })
            .await
            .unwrap();
            assert!(status.success(), "crash helper failed at {stage}");
            let (replica, mut requests) = primary(directory.path().into()).await;
            let durable = matches!(stage, "durable" | "applied" | "checkpointed");
            let (context, state) = replica.begin(TransactionOptions::default()).await.unwrap();
            assert_eq!(state.0, if durable { 7 } else { 0 }, "{stage}");
            assert_eq!(
                replica.applied_lsn().await.unwrap(),
                i64::from(durable),
                "{stage}"
            );
            if durable {
                let writer = replica.clone();
                let retry =
                    tokio::spawn(
                        async move { writer.commit(context, identity(1), state.0, 7).await },
                    );
                if stage != "checkpointed" {
                    let request =
                        tokio::time::timeout(std::time::Duration::from_secs(2), requests.recv())
                            .await
                            .unwrap()
                            .unwrap();
                    request.reply.send(Ok(2)).unwrap();
                }
                assert_eq!(retry.await.unwrap().unwrap(), CommitVersion(1));
                assert!(requests.try_recv().is_err());
            } else {
                assert_eq!(replica.committed_result(identity(1)).await.unwrap(), None);
            }
        }
    }

    #[tokio::test]
    async fn recovered_unconfirmed_retry_requires_quorum() {
        for quorum_succeeds in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let payload = encode(
                &Envelope {
                    format: FORMAT,
                    provider_format: Counter::FORMAT_ID.into(),
                    confirmed_lsn: 0,
                    identity: identity(1),
                    command: 7u64,
                },
                MAX_TRANSACTION_BYTES,
            )
            .unwrap();
            let mut log = TransactionLog::open(directory.path().into()).unwrap();
            log.append(Record { lsn: 1, payload }).unwrap();
            drop(log);
            let (replica, mut requests) = primary(directory.path().into()).await;
            assert!(matches!(
                replica.committed_result(identity(1)).await,
                Err(Error::UnconfirmedCommit)
            ));
            let (context, state) = replica.begin(TransactionOptions::default()).await.unwrap();
            assert_eq!(state.0, 7);
            let writer = replica.clone();
            let commit =
                tokio::spawn(async move { writer.commit(context, identity(1), 0, 7).await });
            let request = tokio::time::timeout(std::time::Duration::from_secs(2), requests.recv())
                .await
                .expect("unconfirmed retry must reach quorum replication")
                .unwrap();
            if quorum_succeeds {
                request.reply.send(Ok(2)).unwrap();
                assert_eq!(commit.await.unwrap().unwrap(), CommitVersion(1));
                assert_eq!(replica.applied_lsn().await.unwrap(), 2);
                assert_eq!(
                    replica
                        .begin(TransactionOptions::default())
                        .await
                        .unwrap()
                        .1
                        .0,
                    7
                );
                assert_eq!(
                    replica.committed_result(identity(1)).await.unwrap(),
                    Some(CommitVersion(1))
                );
            } else {
                request
                    .reply
                    .send(Err(kuberic_core::KubericError::NoWriteQuorum))
                    .unwrap();
                assert!(matches!(commit.await.unwrap(), Err(Error::Replication(_))));
                assert_eq!(replica.applied_lsn().await.unwrap(), 1);
            }
        }
    }

    #[tokio::test]
    async fn lost_commit_reply_is_durable_and_idempotent_after_checkpoint_restart() {
        let directory = tempfile::tempdir().unwrap();
        let (replica, mut requests) = primary(directory.path().into()).await;
        let (context, snapshot) = replica.begin(TransactionOptions::default()).await.unwrap();
        let writer = replica.clone();
        let rpc =
            tokio::spawn(async move { writer.commit(context, identity(1), snapshot.0, 7).await });
        let request = requests.recv().await.unwrap();
        assert_eq!(replica.applied_lsn().await.unwrap(), 0);
        rpc.abort();
        let _ = rpc.await;
        request.reply.send(Ok(1)).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while replica.applied_lsn().await.unwrap() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        replica.checkpoint().await.unwrap();
        drop(replica);
        let (replica, mut requests) = primary(directory.path().into()).await;
        let (context, state) = replica.begin(TransactionOptions::default()).await.unwrap();
        assert_eq!(state.0, 7);
        assert_eq!(
            replica
                .commit(context, identity(1), state.0, 7)
                .await
                .unwrap(),
            CommitVersion(1)
        );
        assert!(requests.try_recv().is_err());
        assert_eq!(
            replica.committed_result(identity(1)).await.unwrap(),
            Some(CommitVersion(1))
        );
        let (context, _) = replica.begin(TransactionOptions::default()).await.unwrap();
        assert!(matches!(
            replica.commit(context, identity(1), 7, 8).await,
            Err(Error::DuplicateRequest)
        ));
    }

    #[tokio::test]
    async fn uncertain_commit_and_stale_context_cannot_continue_writing() {
        let directory = tempfile::tempdir().unwrap();
        let (replica, mut requests) = primary(directory.path().into()).await;
        let (context, _) = replica.begin(TransactionOptions::default()).await.unwrap();
        replica
            .access(|inner| {
                inner.generation += 1;
                Ok(())
            })
            .await
            .unwrap();
        assert!(matches!(
            replica.commit(context, identity(1), 0, 1).await,
            Err(Error::StaleEpoch)
        ));
        assert!(requests.try_recv().is_err());
        let (context, _) = replica.begin(TransactionOptions::default()).await.unwrap();
        let writer = replica.clone();
        let commit = tokio::spawn(async move { writer.commit(context, identity(2), 0, 1).await });
        let request = requests.recv().await.unwrap();
        request
            .reply
            .send(Err(kuberic_core::KubericError::NoWriteQuorum))
            .unwrap();
        assert!(commit.await.unwrap().is_err());
        assert_eq!(replica.applied_lsn().await.unwrap(), 0);
        replica
            .access(|inner| {
                inner.generation += 1;
                Ok(())
            })
            .await
            .unwrap();
        assert!(matches!(
            replica.begin(TransactionOptions::default()).await,
            Err(Error::RecoveryRequired)
        ));
    }

    #[tokio::test]
    async fn secondary_persists_before_ack_and_rollback_removes_result_with_state() {
        let directory = tempfile::tempdir().unwrap();
        let replica = TransactionalReplicator::<Counter>::open(directory.path().into())
            .await
            .unwrap();
        let payload = encode(
            &Envelope {
                format: FORMAT,
                provider_format: Counter::FORMAT_ID.into(),
                confirmed_lsn: 0,
                identity: identity(1),
                command: 7u64,
            },
            MAX_TRANSACTION_BYTES,
        )
        .unwrap();
        let (sender, mut stream) = OperationStream::channel(1);
        let (ack, acknowledged) = tokio::sync::oneshot::channel();
        sender
            .send(Operation::new(1, payload.into(), Some(ack)))
            .await
            .unwrap();
        drop(sender);
        replica
            .drain(&mut stream, CancellationToken::new(), false)
            .await
            .unwrap();
        acknowledged.await.unwrap();
        drop(replica);
        let replica = TransactionalReplicator::<Counter>::open(directory.path().into())
            .await
            .unwrap();
        assert_eq!(
            replica
                .access(|inner| Ok(inner.snapshot.state.0))
                .await
                .unwrap(),
            7
        );
        let (reply, result) = tokio::sync::oneshot::channel();
        replica
            .provider(StateProviderEvent::UpdateEpoch {
                epoch: kuberic_core::types::Epoch::new(1, 1),
                previous_epoch_last_lsn: 0,
                reply,
            })
            .await;
        result.await.unwrap().unwrap();
        assert!(
            replica
                .access(
                    |inner| Ok(inner.snapshot.results.is_empty() && inner.snapshot.state.0 == 0)
                )
                .await
                .unwrap()
        );
        let (sender, mut stream) = OperationStream::channel(1);
        let (ack, acknowledged) = tokio::sync::oneshot::channel();
        sender
            .send(Operation::new(
                1,
                Bytes::from_static(b"truncated"),
                Some(ack),
            ))
            .await
            .unwrap();
        drop(sender);
        assert!(
            replica
                .drain(&mut stream, CancellationToken::new(), false)
                .await
                .is_err()
        );
        assert!(acknowledged.await.is_err());
        assert_eq!(replica.applied_lsn().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn rejected_checkpoint_does_not_fence_a_healthy_replica() {
        let directory = tempfile::tempdir().unwrap();
        let (replica, mut requests) = primary(directory.path().into()).await;
        replica
            .access(|inner| {
                inner.role = Role::IdleSecondary;
                Ok(())
            })
            .await
            .unwrap();
        assert!(matches!(replica.checkpoint().await, Err(Error::NotPrimary)));
        replica
            .access(|inner| {
                inner.role = Role::Primary;
                inner.confirmed_lsn = 1;
                Ok(())
            })
            .await
            .unwrap();
        assert!(matches!(replica.checkpoint().await, Err(Error::Invalid(_))));
        replica
            .access(|inner| {
                inner.confirmed_lsn = 0;
                Ok(())
            })
            .await
            .unwrap();
        let (context, state) = replica.begin(TransactionOptions::default()).await.unwrap();
        let writer = replica.clone();
        let commit =
            tokio::spawn(async move { writer.commit(context, identity(1), state.0, 7).await });
        requests.recv().await.unwrap().reply.send(Ok(1)).unwrap();
        assert_eq!(commit.await.unwrap().unwrap(), CommitVersion(1));
        replica.checkpoint().await.unwrap();
    }

    #[tokio::test]
    async fn secondary_checkpoints_only_confirmed_prefix_and_retains_suffix() {
        let directory = tempfile::tempdir().unwrap();
        let replica = TransactionalReplicator::<Counter>::open(directory.path().into())
            .await
            .unwrap();
        replica
            .access(|inner| {
                inner.role = Role::ActiveSecondary;
                Ok(())
            })
            .await
            .unwrap();
        let (sender, mut stream) = OperationStream::channel(2);
        for lsn in 1..=2 {
            let payload = encode(
                &Envelope {
                    format: FORMAT,
                    provider_format: Counter::FORMAT_ID.into(),
                    confirmed_lsn: lsn - 1,
                    identity: identity(lsn as u128),
                    command: 7u64,
                },
                MAX_TRANSACTION_BYTES,
            )
            .unwrap();
            sender
                .send(Operation::new(lsn, payload.into(), None))
                .await
                .unwrap();
        }
        drop(sender);
        replica
            .drain(&mut stream, CancellationToken::new(), false)
            .await
            .unwrap();
        replica
            .access(|inner| {
                reclaim_if_needed(
                    inner,
                    kuberic_transaction_log::MAX_RETAINED_LOG as usize / 2,
                )?;
                assert_eq!(inner.log.checkpoint_record().unwrap().lsn, 1);
                Ok(())
            })
            .await
            .unwrap();
        replica.checkpoint().await.unwrap();
        replica
            .access(|inner| {
                assert_eq!(inner.log.checkpoint_record().unwrap().lsn, 1);
                assert_eq!(inner.log.records()[0].lsn, 2);
                assert_eq!(inner.snapshot.state.0, 14);
                Ok(())
            })
            .await
            .unwrap();
        drop(replica);
        let replica = TransactionalReplicator::<Counter>::open(directory.path().into())
            .await
            .unwrap();
        replica
            .access(|inner| {
                assert_eq!(inner.confirmed_lsn, 1);
                assert_eq!(inner.snapshot.state.0, 14);
                Ok(())
            })
            .await
            .unwrap();
        let (reply, result) = tokio::sync::oneshot::channel();
        replica
            .provider(StateProviderEvent::UpdateEpoch {
                epoch: kuberic_core::types::Epoch::new(1, 1),
                previous_epoch_last_lsn: 1,
                reply,
            })
            .await;
        result.await.unwrap().unwrap();
        assert_eq!(
            replica
                .access(|inner| Ok(inner.snapshot.state.0))
                .await
                .unwrap(),
            7
        );
    }

    #[tokio::test]
    async fn replay_rejects_conflicting_lsn_and_invalid_snapshot_metadata() {
        let directory = tempfile::tempdir().unwrap();
        let replica = TransactionalReplicator::<Counter>::open(directory.path().into())
            .await
            .unwrap();
        for (transaction, expected) in [(1, true), (1, true), (2, false)] {
            let payload = encode(
                &Envelope {
                    format: FORMAT,
                    provider_format: Counter::FORMAT_ID.into(),
                    confirmed_lsn: 0,
                    identity: identity(transaction),
                    command: 7u64,
                },
                MAX_TRANSACTION_BYTES,
            )
            .unwrap();
            let (sender, mut stream) = OperationStream::channel(1);
            let (ack, acknowledged) = tokio::sync::oneshot::channel();
            sender
                .send(Operation::new(1, payload.into(), Some(ack)))
                .await
                .unwrap();
            drop(sender);
            assert_eq!(
                replica
                    .drain(&mut stream, CancellationToken::new(), false)
                    .await
                    .is_ok(),
                expected
            );
            assert_eq!(acknowledged.await.is_ok(), expected);
        }
        let mut snapshot = replica
            .access(|inner| Ok(inner.snapshot.clone()))
            .await
            .unwrap();
        let mut duplicate = snapshot.results["request-1"].clone();
        duplicate.identity.request = "another-request".into();
        snapshot
            .results
            .insert(duplicate.identity.request.clone(), duplicate);
        assert!(
            checked_snapshot::<Counter>(&encode(&snapshot, MAX_SNAPSHOT_BYTES).unwrap()).is_err()
        );
        let mut payload = postcard::to_allocvec(&7u64).unwrap();
        payload.push(0);
        let mut bytes = Sha256::digest(&payload).to_vec();
        bytes.extend_from_slice(&payload);
        assert!(decode::<u64>(&bytes, MAX_TRANSACTION_BYTES).is_err());
    }

    #[tokio::test]
    async fn retained_outcome_eviction_is_bounded_and_explicit() {
        let mut snapshot = Snapshot::<Counter>::default();
        for lsn in 1..=RETAINED_RESULTS as i64 + 1 {
            let payload = encode(
                &Envelope {
                    format: FORMAT,
                    provider_format: Counter::FORMAT_ID.into(),
                    confirmed_lsn: lsn - 1,
                    identity: identity(lsn as u128),
                    command: 1u64,
                },
                MAX_TRANSACTION_BYTES,
            )
            .unwrap();
            snapshot = next_snapshot(&snapshot, lsn, &payload).unwrap();
        }
        let snapshot =
            checked_snapshot::<Counter>(&encode(&snapshot, MAX_SNAPSHOT_BYTES).unwrap()).unwrap();
        let digest = Sha256::digest(postcard::to_allocvec(&1u64).unwrap()).into();
        assert_eq!(snapshot.results.len(), RETAINED_RESULTS);
        assert_eq!(
            retained_result(&snapshot, &identity(1), digest).unwrap(),
            None
        );
        assert_eq!(
            retained_result(&snapshot, &identity(2), digest).unwrap(),
            Some(CommitVersion(2))
        );
    }

    #[tokio::test]
    async fn contexts_are_owner_bound_and_admission_is_released_on_abort() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let (replica, _) = primary(first.path().into()).await;
        let (other, _) = primary(second.path().into()).await;
        let (context, _) = replica.begin(TransactionOptions::default()).await.unwrap();
        assert!(matches!(
            other.commit(context, identity(1), 0, 1).await,
            Err(Error::Invalid(_))
        ));
        let mut transactions = Vec::new();
        for _ in 0..16 {
            transactions.push(replica.begin(TransactionOptions::default()).await.unwrap());
        }
        assert!(matches!(
            replica.begin(TransactionOptions::default()).await,
            Err(Error::ResourceExhausted)
        ));
        transactions.clear();
        assert!(replica.begin(TransactionOptions::default()).await.is_ok());
    }
}
