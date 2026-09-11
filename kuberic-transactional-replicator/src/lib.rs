use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use kuberic_core::handles::{PartitionHandle, StateReplicatorHandle};
use kuberic_core::types::{AccessStatus, CancellationToken, FaultType, Role};
use kuberic_transaction_log::{Record, TransactionLog};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

mod host;

pub type Result<T> = std::result::Result<T, Error>;
pub const MAX_TRANSACTION_BYTES: usize = 1024 * 1024;
pub const RETAINED_RESULTS: usize = 1024;
pub const MAX_SNAPSHOT_BYTES: usize = 8 * 1024 * 1024;
const FORMAT: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("transaction conflict: {0}")]
    Conflict(String),
    #[error("invalid transaction: {0}")]
    Invalid(String),
    #[error("transaction expired")]
    Expired,
    #[error("transaction belongs to a stale replica epoch")]
    StaleEpoch,
    #[error("primary write access is not granted")]
    NotPrimary,
    #[error("replica requires recovery")]
    RecoveryRequired,
    #[error("transaction resource limit exceeded")]
    ResourceExhausted,
    #[error("request identity was reused with different transaction data")]
    DuplicateRequest,
    #[error("storage: {0}")]
    Storage(#[from] std::io::Error),
    #[error("encoding: {0}")]
    Encoding(#[from] postcard::Error),
    #[error("replication: {0}")]
    Replication(#[from] kuberic_core::KubericError),
}

pub trait TransactionalStateProvider:
    Clone + Default + Serialize + DeserializeOwned + Send + 'static
{
    const FORMAT_ID: &'static str;
    type Command: Clone + Serialize + DeserializeOwned + Send + 'static;
    type Observations: Send + 'static;

    fn validate(&self, observed: &Self::Observations, command: &Self::Command) -> Result<()>;
    fn apply(&mut self, command: &Self::Command, version: CommitVersion) -> Result<()>;
    fn validate_snapshot(&self) -> Result<()>;
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitVersion(pub i64);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransactionId {
    pub transaction: u128,
    pub request: String,
}

#[derive(Clone, Copy, Debug)]
pub enum IsolationLevel {
    OptimisticSerializable,
}

#[derive(Clone, Copy, Debug)]
pub struct TransactionOptions {
    pub timeout: std::time::Duration,
    pub isolation: IsolationLevel,
}

impl Default for TransactionOptions {
    fn default() -> Self {
        Self {
            timeout: std::time::Duration::from_secs(60),
            isolation: IsolationLevel::OptimisticSerializable,
        }
    }
}

pub struct TransactionContext {
    generation: u64,
    deadline: Instant,
    _permit: OwnedSemaphorePermit,
    owner: Arc<Semaphore>,
}

impl TransactionContext {
    pub fn ensure_active(&self) -> Result<()> {
        if Instant::now() >= self.deadline {
            Err(Error::Expired)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Outcome {
    identity: TransactionId,
    digest: [u8; 32],
    version: CommitVersion,
}

#[derive(Clone, Serialize, Deserialize)]
struct Snapshot<State> {
    format: u32,
    provider_format: String,
    state: State,
    results: BTreeMap<String, Outcome>,
    applied_lsn: i64,
}

impl<State: TransactionalStateProvider> Default for Snapshot<State> {
    fn default() -> Self {
        Self {
            format: FORMAT,
            provider_format: State::FORMAT_ID.into(),
            state: State::default(),
            results: BTreeMap::new(),
            applied_lsn: 0,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Envelope<Command> {
    format: u32,
    provider_format: String,
    identity: TransactionId,
    command: Command,
}

pub(crate) fn encode<T: Serialize>(value: &T, limit: usize) -> Result<Vec<u8>> {
    let payload = postcard::to_allocvec(value)?;
    if payload.len() > limit {
        return Err(Error::ResourceExhausted);
    }
    let mut bytes = Sha256::digest(&payload).to_vec();
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8], limit: usize) -> Result<T> {
    if bytes.len() < 32
        || bytes.len() > limit + 32
        || Sha256::digest(&bytes[32..])[..] != bytes[..32]
    {
        return Err(Error::Invalid("invalid record length or checksum".into()));
    }
    Ok(postcard::from_bytes(&bytes[32..])?)
}

struct Inner<State> {
    log: TransactionLog,
    snapshot: Snapshot<State>,
    generation: u64,
    role: Role,
    failed: bool,
    confirmed_lsn: i64,
    partition: Option<Arc<PartitionHandle>>,
    replicator: Option<StateReplicatorHandle>,
    token: CancellationToken,
}

pub struct TransactionalReplicator<State> {
    inner: Arc<Mutex<Inner<State>>>,
    gate: Arc<tokio::sync::Mutex<()>>,
    admission: Arc<Semaphore>,
}

impl<State> Clone for TransactionalReplicator<State> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            gate: self.gate.clone(),
            admission: self.admission.clone(),
        }
    }
}

impl<State: TransactionalStateProvider> TransactionalReplicator<State> {
    pub async fn open(path: PathBuf) -> Result<Self> {
        tokio::task::spawn_blocking(move || {
            let log = TransactionLog::open(path)?;
            let snapshot = recover::<State>(&log, log.last_lsn())?;
            let confirmed_lsn = log.checkpoint_record().map_or(0, |record| record.lsn);
            Ok(Self {
                inner: Arc::new(Mutex::new(Inner {
                    log,
                    snapshot,
                    generation: 0,
                    role: Role::Unknown,
                    failed: false,
                    confirmed_lsn,
                    partition: None,
                    replicator: None,
                    token: CancellationToken::new(),
                })),
                gate: Arc::new(tokio::sync::Mutex::new(())),
                admission: Arc::new(Semaphore::new(16)),
            })
        })
        .await
        .map_err(|_| Error::RecoveryRequired)?
    }

    async fn access<T: Send + 'static>(
        &self,
        action: impl FnOnce(&mut Inner<State>) -> Result<T> + Send + 'static,
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

    pub async fn begin(&self, options: TransactionOptions) -> Result<(TransactionContext, State)> {
        if options.timeout.is_zero() || options.timeout > std::time::Duration::from_secs(60) {
            return Err(Error::Invalid(
                "timeout must be between zero and 60 seconds".into(),
            ));
        }
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::ResourceExhausted)?;
        let (generation, state) = self
            .access(|inner| {
                ensure_writable(inner)?;
                Ok((inner.generation, inner.snapshot.state.clone()))
            })
            .await?;
        Ok((
            TransactionContext {
                generation,
                deadline: Instant::now() + options.timeout,
                _permit: permit,
                owner: self.admission.clone(),
            },
            state,
        ))
    }

    pub async fn commit(
        &self,
        context: TransactionContext,
        identity: TransactionId,
        observations: State::Observations,
        command: State::Command,
    ) -> Result<CommitVersion> {
        context.ensure_active()?;
        if !Arc::ptr_eq(&context.owner, &self.admission) {
            return Err(Error::Invalid(
                "transaction belongs to another coordinator".into(),
            ));
        }
        if identity.request.is_empty() || identity.request.len() > 128 {
            return Err(Error::Invalid("request ID must be 1-128 bytes".into()));
        }
        let payload = encode(
            &Envelope {
                format: FORMAT,
                provider_format: State::FORMAT_ID.into(),
                identity: identity.clone(),
                command: command.clone(),
            },
            MAX_TRANSACTION_BYTES,
        )?;
        let replica = self.clone();
        tokio::spawn(async move {
            let _gate = tokio::time::timeout_at(context.deadline, replica.gate.lock())
                .await
                .map_err(|_| Error::Expired)?;
            context.ensure_active()?;
            let envelope = decode::<Envelope<State::Command>>(&payload, MAX_TRANSACTION_BYTES)?;
            let digest: [u8; 32] = Sha256::digest(postcard::to_allocvec(&envelope.command)?).into();
            let payload_for_check = payload.clone();
            let preparation = replica
                .access(move |inner| {
                    ensure_writable(inner)?;
                    if context.generation != inner.generation {
                        return Err(Error::StaleEpoch);
                    }
                    if let Some(version) = retained_result(&inner.snapshot, &identity, digest)? {
                        return Ok(Err(version));
                    }
                    inner.log.check_capacity(payload_for_check.len())?;
                    inner.snapshot.state.validate(&observations, &command)?;
                    let _ = next_snapshot(
                        &inner.snapshot,
                        inner.snapshot.applied_lsn + 1,
                        &payload_for_check,
                    )?;
                    Ok(Ok((
                        inner.replicator.clone().ok_or(Error::NotPrimary)?,
                        inner.token.clone(),
                        inner.generation,
                    )))
                })
                .await?;
            let (replicator, token, generation) = match preparation {
                Ok(preparation) => preparation,
                Err(version) => return Ok(version),
            };
            let lsn = match replicator
                .replicate(Bytes::copy_from_slice(&payload), token)
                .await
            {
                Ok(lsn) => lsn,
                Err(error) => {
                    replica.fault().await;
                    return Err(error.into());
                }
            };
            let applied = replica
                .access(move |inner| {
                    if inner.failed || inner.generation != generation || inner.role != Role::Primary
                    {
                        return Err(Error::StaleEpoch);
                    }
                    let snapshot = next_snapshot(&inner.snapshot, lsn, &payload)?;
                    inner.log.append(Record { lsn, payload })?;
                    inner.snapshot = snapshot;
                    inner.confirmed_lsn = lsn;
                    Ok(CommitVersion(lsn))
                })
                .await;
            if applied.is_err() {
                replica.fault().await;
            }
            applied
        })
        .await
        .map_err(|_| Error::RecoveryRequired)?
    }

    pub async fn applied_lsn(&self) -> Result<i64> {
        self.access(|inner| Ok(inner.snapshot.applied_lsn)).await
    }

    pub async fn committed_result(&self, identity: TransactionId) -> Result<Option<CommitVersion>> {
        self.access(move |inner| {
            ensure_writable(inner)?;
            match inner.snapshot.results.get(&identity.request) {
                Some(outcome) if outcome.identity == identity => Ok(Some(outcome.version)),
                Some(_) => Err(Error::DuplicateRequest),
                None => Ok(None),
            }
        })
        .await
    }

    pub async fn checkpoint(&self) -> Result<()> {
        let _gate = self.gate.lock().await;
        let result = self
            .access(|inner| {
                ensure_writable(inner)?;
                if inner.confirmed_lsn != inner.snapshot.applied_lsn {
                    return Err(Error::RecoveryRequired);
                }
                inner.log.checkpoint(Record {
                    lsn: inner.snapshot.applied_lsn,
                    payload: encode(&inner.snapshot, MAX_SNAPSHOT_BYTES)?,
                })?;
                Ok(())
            })
            .await;
        if result.is_err() {
            self.fault().await;
        }
        result
    }

    pub async fn backup(&self, destination: PathBuf) -> Result<()> {
        let _gate = self.gate.lock().await;
        self.access(move |inner| {
            ensure_writable(inner)?;
            if inner.confirmed_lsn != inner.snapshot.applied_lsn {
                return Err(Error::RecoveryRequired);
            }
            kuberic_transaction_log::atomic_write(
                &destination,
                &encode(&inner.snapshot, MAX_SNAPSHOT_BYTES)?,
            )?;
            Ok(())
        })
        .await
    }

    pub async fn restore_backup(&self, source: PathBuf) -> Result<()> {
        let _gate = self.gate.lock().await;
        self.access(move |inner| {
            if inner.role != Role::Unknown {
                return Err(Error::Invalid(
                    "restore requires an unopened replica".into(),
                ));
            }
            if std::fs::metadata(&source)?.len() > (MAX_SNAPSHOT_BYTES + 32) as u64 {
                return Err(Error::ResourceExhausted);
            }
            let bytes = std::fs::read(source)?;
            let snapshot = checked_snapshot::<State>(&bytes)?;
            inner.failed = true;
            inner.log.install_checkpoint(Record {
                lsn: snapshot.applied_lsn,
                payload: bytes,
            })?;
            inner.confirmed_lsn = snapshot.applied_lsn;
            inner.snapshot = snapshot;
            inner.generation += 1;
            inner.failed = false;
            Ok(())
        })
        .await
    }
}

fn ensure_writable<State>(inner: &Inner<State>) -> Result<()> {
    if inner.failed {
        return Err(Error::RecoveryRequired);
    }
    if inner.role != Role::Primary
        || inner.token.is_cancelled()
        || !inner
            .partition
            .as_ref()
            .is_some_and(|partition| partition.write_status() == AccessStatus::Granted)
    {
        return Err(Error::NotPrimary);
    }
    Ok(())
}

fn retained_result<State>(
    snapshot: &Snapshot<State>,
    identity: &TransactionId,
    digest: [u8; 32],
) -> Result<Option<CommitVersion>> {
    if let Some(outcome) = snapshot.results.get(&identity.request) {
        return if &outcome.identity == identity && outcome.digest == digest {
            Ok(Some(outcome.version))
        } else {
            Err(Error::DuplicateRequest)
        };
    }
    if snapshot
        .results
        .values()
        .any(|outcome| outcome.identity.transaction == identity.transaction)
    {
        return Err(Error::DuplicateRequest);
    }
    Ok(None)
}

fn next_snapshot<State: TransactionalStateProvider>(
    previous: &Snapshot<State>,
    lsn: i64,
    payload: &[u8],
) -> Result<Snapshot<State>> {
    if lsn != previous.applied_lsn + 1 {
        return Err(Error::Invalid("transaction LSN gap".into()));
    }
    let envelope: Envelope<State::Command> = decode(payload, MAX_TRANSACTION_BYTES)?;
    if envelope.format != FORMAT
        || envelope.provider_format != State::FORMAT_ID
        || envelope.identity.request.is_empty()
        || envelope.identity.request.len() > 128
    {
        return Err(Error::Invalid(
            "transaction format or identity mismatch".into(),
        ));
    }
    let digest = Sha256::digest(postcard::to_allocvec(&envelope.command)?).into();
    let mut snapshot = previous.clone();
    if retained_result(&snapshot, &envelope.identity, digest)?.is_none() {
        snapshot
            .state
            .apply(&envelope.command, CommitVersion(lsn))?;
        snapshot.results.insert(
            envelope.identity.request.clone(),
            Outcome {
                identity: envelope.identity,
                digest,
                version: CommitVersion(lsn),
            },
        );
        if snapshot.results.len() > RETAINED_RESULTS {
            let oldest = snapshot
                .results
                .iter()
                .min_by_key(|(_, outcome)| outcome.version.0)
                .map(|(key, _)| key.clone())
                .unwrap();
            snapshot.results.remove(&oldest);
        }
    }
    snapshot.applied_lsn = lsn;
    snapshot.state.validate_snapshot()?;
    let _ = encode(&snapshot, MAX_SNAPSHOT_BYTES)?;
    Ok(snapshot)
}

fn checked_snapshot<State: TransactionalStateProvider>(payload: &[u8]) -> Result<Snapshot<State>> {
    let snapshot: Snapshot<State> = decode(payload, MAX_SNAPSHOT_BYTES)?;
    if snapshot.format != FORMAT
        || snapshot.provider_format != State::FORMAT_ID
        || snapshot.applied_lsn < 0
        || snapshot.results.len() > RETAINED_RESULTS
    {
        return Err(Error::Invalid("invalid checkpoint format".into()));
    }
    for (request, outcome) in &snapshot.results {
        if request != &outcome.identity.request
            || request.is_empty()
            || request.len() > 128
            || outcome.version.0 <= 0
            || outcome.version.0 > snapshot.applied_lsn
        {
            return Err(Error::Invalid("invalid retained transaction result".into()));
        }
    }
    snapshot.state.validate_snapshot()?;
    Ok(snapshot)
}

fn recover<State: TransactionalStateProvider>(
    log: &TransactionLog,
    target: i64,
) -> Result<Snapshot<State>> {
    let mut snapshot = match log.checkpoint_record() {
        Some(record) => {
            let snapshot = checked_snapshot::<State>(&record.payload)?;
            if snapshot.applied_lsn != record.lsn {
                return Err(Error::Invalid("checkpoint LSN mismatch".into()));
            }
            snapshot
        }
        None => Snapshot::default(),
    };
    if target < snapshot.applied_lsn || target > log.last_lsn() {
        return Err(Error::Invalid(
            "history unavailable; full copy required".into(),
        ));
    }
    for record in log.records().iter().filter(|record| record.lsn <= target) {
        snapshot = next_snapshot(&snapshot, record.lsn, &record.payload)?;
    }
    Ok(snapshot)
}
