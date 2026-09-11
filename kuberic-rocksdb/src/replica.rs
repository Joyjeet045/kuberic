use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use kuberic_core::events::{LifecycleEvent, StateProviderEvent};
use kuberic_core::handles::{PartitionHandle, StateReplicatorHandle};
use kuberic_core::replicator::WalReplicator;
use kuberic_core::types::{
    AccessStatus, CancellationToken, FaultType, Operation, OperationStream, Role,
};
use tokio::sync::mpsc;

use crate::{
    Error, MAX_COPY, Mutation, Result, Store, filename, publish_database, record, restore, user_key,
};

struct Inner {
    store: Store,
    root: PathBuf,
    role: Role,
    generation: u64,
    failed: bool,
    partition: Option<Arc<PartitionHandle>>,
    replicator: Option<StateReplicatorHandle>,
    token: CancellationToken,
    _lock: std::fs::File,
}

#[derive(Clone)]
pub struct RocksReplica {
    inner: Arc<Mutex<Inner>>,
    gate: Arc<tokio::sync::Mutex<()>>,
    admission: Arc<tokio::sync::Semaphore>,
}

impl RocksReplica {
    pub async fn open(root: PathBuf) -> Result<Self> {
        tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&root)?;
            let lock = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(root.join("replica.lock"))?;
            fs2::FileExt::try_lock_exclusive(&lock)?;
            let name = match std::fs::read_to_string(root.join("active")) {
                Ok(name) => {
                    filename(&name)?;
                    if !root.join(&name).join("CURRENT").is_file() {
                        return Err(Error::Invalid(
                            "active database is missing; rebuild required".into(),
                        ));
                    }
                    name
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => "initial".into(),
                Err(error) => return Err(error.into()),
            };
            filename(&name)?;
            let store = Store::open(&root.join(&name))?;
            publish_database(&root, &name)?;
            Ok(Self {
                inner: Arc::new(Mutex::new(Inner {
                    store,
                    root,
                    role: Role::Unknown,
                    generation: 0,
                    failed: false,
                    partition: None,
                    replicator: None,
                    token: CancellationToken::new(),
                    _lock: lock,
                })),
                gate: Arc::new(tokio::sync::Mutex::new(())),
                admission: Arc::new(tokio::sync::Semaphore::new(64)),
            })
        })
        .await
        .map_err(|_| Error::RecoveryRequired)?
    }

    async fn access<T: Send + 'static>(
        &self,
        action: impl FnOnce(&mut Inner) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let mut inner = inner.lock().map_err(|_| Error::RecoveryRequired)?;
            action(&mut inner)
        })
        .await
        .map_err(|_| Error::RecoveryRequired)?
    }

    async fn fault(&self) {
        let _ = self
            .access(|inner| {
                inner.failed = true;
                if let Some(partition) = &inner.partition {
                    partition.report_fault(FaultType::Transient);
                }
                Ok(())
            })
            .await;
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        self.access(move |inner| {
            if inner.failed {
                return Err(Error::RecoveryRequired);
            }
            if inner.role != Role::Primary
                || !inner.partition.as_ref().is_some_and(|partition| {
                    matches!(
                        partition.read_status(),
                        AccessStatus::Granted | AccessStatus::NoWriteQuorum
                    )
                })
            {
                return Err(Error::NotPrimary);
            }
            Ok(inner.store.database.get(user_key(&key))?)
        })
        .await
    }

    pub async fn applied_lsn(&self) -> Result<i64> {
        self.access(|inner| Ok(inner.store.lsn)).await
    }

    pub async fn write(&self, mutations: Vec<Mutation>) -> Result<i64> {
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        let record = record(mutations)?;
        let replica = self.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let _gate = replica.gate.lock().await;
            let (replicator, token, generation) = replica
                .access(|inner| {
                    if inner.failed {
                        return Err(Error::RecoveryRequired);
                    }
                    if inner.role != Role::Primary
                        || !inner.partition.as_ref().is_some_and(|partition| {
                            partition.write_status() == AccessStatus::Granted
                        })
                    {
                        return Err(Error::NotPrimary);
                    }
                    Ok((
                        inner.replicator.clone().ok_or(Error::NotPrimary)?,
                        inner.token.clone(),
                        inner.generation,
                    ))
                })
                .await?;
            let lsn = match replicator
                .replicate(Bytes::copy_from_slice(&record), token)
                .await
            {
                Ok(lsn) => lsn,
                Err(error) => {
                    replica.fault().await;
                    return Err(error.into());
                }
            };
            let result = replica
                .access(move |inner| {
                    if inner.generation != generation || inner.role != Role::Primary || inner.failed
                    {
                        return Err(Error::RecoveryRequired);
                    }
                    inner.store.apply_record(lsn, &record)
                })
                .await;
            if result.is_err() {
                replica.fault().await;
            }
            result.map(|()| lsn)
        })
        .await
        .map_err(|_| Error::RecoveryRequired)?
    }

    pub async fn write_column_family(
        &self,
        column_family: &str,
        mutations: Vec<Mutation>,
    ) -> Result<i64> {
        if column_family != "default" {
            return Err(Error::Invalid(
                "only the default column family is supported".into(),
            ));
        }
        self.write(mutations).await
    }

    async fn drain(
        &self,
        mut stream: OperationStream,
        token: CancellationToken,
        copy: bool,
    ) -> Result<()> {
        let mut contents = Vec::new();
        loop {
            let operation = tokio::select! {
                _ = token.cancelled() => return Err(Error::RecoveryRequired),
                operation = stream.get_operation() => operation,
            };
            let Some(operation) = operation else { break };
            if copy {
                if contents.len() + operation.data.len() > MAX_COPY + 4 {
                    return Err(Error::Invalid("copy exceeds limit".into()));
                }
                contents.extend_from_slice(&operation.data);
            } else {
                let data = operation.data.clone();
                let lsn = operation.lsn;
                self.access(move |inner| inner.store.apply_record(lsn, &data))
                    .await?;
            }
            operation.acknowledge();
        }
        if copy {
            let lsn = stream
                .copy_lsn()
                .ok_or_else(|| Error::Invalid("copy completion missing".into()))?;
            self.access(move |inner| {
                let store = restore(&inner.root, &contents, lsn)?;
                let previous = std::mem::replace(&mut inner.store, store);
                let path = previous.path.clone();
                drop(previous);
                if let Err(error) = std::fs::remove_dir_all(path) {
                    tracing::warn!(%error, "old RocksDB generation cleanup failed");
                }
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
                    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
                    let result = async {
                        while replica.applied_lsn().await? < up_to_lsn {
                            if tokio::time::Instant::now() >= deadline {
                                return Err(Error::RecoveryRequired);
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                        }
                        replica
                            .access(move |inner| inner.store.copy_at(up_to_lsn))
                            .await
                    }
                    .await;
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
                        if previous_epoch_last_lsn < inner.store.lsn {
                            inner.store.rollback(previous_epoch_last_lsn)?;
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
        let mut drain: Option<tokio::task::JoinHandle<Result<()>>> = None;
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
                                    let result = handle.await.map_err(|_| Error::RecoveryRequired)?;
                                    if role == Role::IdleSecondary && matches!(new_role, Role::ActiveSecondary | Role::Primary) { result?; }
                                }
                                self.access(move |inner| {
                                    if inner.failed && matches!(new_role, Role::Primary | Role::ActiveSecondary) { return Err(Error::RecoveryRequired); }
                                    inner.generation += 1;
                                    inner.role = new_role;
                                    Ok(())
                                }).await?;
                                drain_token = CancellationToken::new();
                                let stream = match new_role { Role::IdleSecondary => copy_stream.take(), Role::ActiveSecondary => replication_stream.take(), _ => None };
                                if let Some(stream) = stream {
                                    let replica = self.clone();
                                    let token = drain_token.clone();
                                    drain = Some(tokio::spawn(async move {
                                        let result = replica.drain(stream, token.clone(), new_role == Role::IdleSecondary).await;
                                        if result.is_err() && !token.is_cancelled() { replica.fault().await; }
                                        result
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
    use kuberic_core::handles::PartitionState;

    #[tokio::test]
    async fn missing_active_database_and_concurrent_open_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let replica = RocksReplica::open(directory.path().into()).await.unwrap();
        assert!(RocksReplica::open(directory.path().into()).await.is_err());
        drop(replica);
        std::fs::remove_dir_all(directory.path().join("initial")).unwrap();
        assert!(RocksReplica::open(directory.path().into()).await.is_err());
    }

    #[tokio::test]
    async fn replication_failure_never_applies_and_persisted_ack_survives_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let replica = RocksReplica::open(directory.path().into()).await.unwrap();
        let state = Arc::new(PartitionState::new());
        state.set_write_status(AccessStatus::Granted);
        let (fault_tx, _faults) = mpsc::channel(1);
        let (sender, mut requests) = mpsc::channel::<kuberic_core::events::ReplicateRequest>(1);
        replica
            .access(move |inner| {
                inner.role = Role::Primary;
                inner.partition = Some(Arc::new(PartitionHandle::new(state.clone(), fault_tx)));
                inner.replicator = Some(StateReplicatorHandle::new(sender, state));
                Ok(())
            })
            .await
            .unwrap();
        let writer = replica.clone();
        let write = tokio::spawn(async move {
            writer
                .write(vec![Mutation::Put {
                    key: b"key".to_vec(),
                    value: b"value".to_vec(),
                }])
                .await
        });
        let request = requests.recv().await.unwrap();
        assert_eq!(replica.applied_lsn().await.unwrap(), 0);
        request
            .reply
            .send(Err(kuberic_core::KubericError::NoWriteQuorum))
            .unwrap();
        assert!(write.await.unwrap().is_err());
        assert_eq!(replica.applied_lsn().await.unwrap(), 0);
        assert!(replica.write(vec![]).await.is_err());
        drop(replica);
        let replica = RocksReplica::open(directory.path().into()).await.unwrap();
        let payload = record(vec![Mutation::Put {
            key: b"key".to_vec(),
            value: b"durable".to_vec(),
        }])
        .unwrap();
        let (sender, stream) = OperationStream::channel(1);
        let (ack, acknowledged) = tokio::sync::oneshot::channel();
        sender
            .send(Operation::new(1, payload.into(), Some(ack)))
            .await
            .unwrap();
        drop(sender);
        replica
            .drain(stream, CancellationToken::new(), false)
            .await
            .unwrap();
        acknowledged.await.unwrap();
        drop(replica);
        let replica = RocksReplica::open(directory.path().into()).await.unwrap();
        assert_eq!(replica.applied_lsn().await.unwrap(), 1);
        let (sender, stream) = OperationStream::channel(1);
        let (ack, acknowledged) = tokio::sync::oneshot::channel();
        sender
            .send(Operation::new(2, Bytes::from_static(b"invalid"), Some(ack)))
            .await
            .unwrap();
        drop(sender);
        assert!(
            replica
                .drain(stream, CancellationToken::new(), false)
                .await
                .is_err()
        );
        assert!(acknowledged.await.is_err());
        assert_eq!(replica.applied_lsn().await.unwrap(), 1);
    }
}
