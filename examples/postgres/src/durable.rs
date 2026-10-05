use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Path, PathBuf};
#[cfg(feature = "testing")]
use std::sync::Condvar;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Weak};

use crate::native::AcknowledgementPolicy;
use kuberic_runtime::protocol::types::{
    BuildAuthority, Epoch, OperationId, ReplicaIdentity, ResourceUid,
};
use kuberic_runtime::replicator::ReplicaSetConfiguration;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

const STATE_VERSION: u32 = 1;
const STATE_FILE: &str = "state-v2.json";
pub const MAX_RETAINED_BUILD_IDS: usize = 64;
pub const MAX_METADATA_BYTES: usize = 4 * 1024 * 1024;
const MAX_BUILD_ID_BYTES: usize = 512;

fn is_false(value: &bool) -> bool {
    !value
}
fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageMode {
    Fresh,
    Established,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PgDurableRole {
    None,
    Primary,
    Standby,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PgRecoveryState {
    Ready,
    Rebuilding,
    Unsafe,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PgDurableIdentity {
    pub resource_uid: ResourceUid,
    pub replica: ReplicaIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PgDurableState {
    pub version: u32,
    pub identity: PgDurableIdentity,
    pub generation: u64,
    #[serde(default, skip_serializing_if = "is_false")]
    pub process_clock_initialized: bool,
    pub system_identifier: Option<String>,
    pub timeline_id: Option<u32>,
    pub timeline_history_digest: Option<String>,
    pub role: PgDurableRole,
    pub recovery_state: PgRecoveryState,
    pub external_access_closed: bool,
    pub postgres_stopped: bool,
    pub current_lsn: i64,
    pub flush_lsn: i64,
    pub received_lsn: Option<i64>,
    pub replay_lsn: Option<i64>,
    pub policy_certified_lsn: i64,
    pub synchronous: Option<AcknowledgementPolicy>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub synchronous_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synchronous_process_generation: Option<u64>,
    pub accepted_build: Option<BuildAuthority>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub has_accepted_authority: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_build: Option<crate::build::PgBuildProgress>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outbound_builds: Vec<crate::build::PgBuildProgress>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catch_up: Option<(kuberic_runtime::protocol::types::ConfigurationId, i64)>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub suspended_builds: Vec<crate::build::PgBuildProgress>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub retired_builds: BTreeSet<OperationId>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub outbound_attempts: BTreeMap<OperationId, u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_epoch: Option<Epoch>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retired_build_epoch: Option<Epoch>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) recovery: Option<crate::adapter::recovery::Recovery>,
}

impl PgDurableState {
    fn new(identity: PgDurableIdentity) -> Self {
        Self {
            version: STATE_VERSION,
            identity,
            generation: 1,
            process_clock_initialized: false,
            system_identifier: None,
            timeline_id: None,
            timeline_history_digest: None,
            role: PgDurableRole::None,
            recovery_state: PgRecoveryState::Ready,
            external_access_closed: true,
            postgres_stopped: true,
            current_lsn: 0,
            flush_lsn: 0,
            received_lsn: None,
            replay_lsn: None,
            policy_certified_lsn: 0,
            synchronous: None,
            synchronous_generation: 0,
            synchronous_process_generation: None,
            accepted_build: None,
            has_accepted_authority: false,
            native_build: None,
            outbound_builds: Vec::new(),
            catch_up: None,
            suspended_builds: Vec::new(),
            retired_builds: BTreeSet::new(),
            outbound_attempts: BTreeMap::new(),
            build_epoch: None,
            retired_build_epoch: None,
            recovery: None,
        }
    }

    fn validate(&self, expected: &PgDurableIdentity) -> Result<(), PgDurableError> {
        if self.version != STATE_VERSION {
            return Err(PgDurableError::Version {
                expected: STATE_VERSION,
                observed: self.version,
            });
        }
        if &self.identity != expected {
            return Err(PgDurableError::IdentityMismatch);
        }
        if self.generation == 0
            || self.synchronous_generation > self.generation
            || self.current_lsn < 0
            || self.flush_lsn < 0
            || self.flush_lsn > self.current_lsn
            || self.policy_certified_lsn < 0
            || self.policy_certified_lsn > self.flush_lsn
            || self.received_lsn.is_some_and(|lsn| lsn < 0)
            || self.replay_lsn.is_some_and(|lsn| lsn < 0)
            || self
                .replay_lsn
                .zip(self.received_lsn)
                .is_some_and(|(replay, received)| replay > received)
        {
            return Err(PgDurableError::Invalid(
                "durable PostgreSQL progress is inconsistent".into(),
            ));
        }
        if let Some(synchronous) = &self.synchronous {
            synchronous.validate().map_err(PgDurableError::Invalid)?;
        }
        if let Some(recovery) = &self.recovery {
            recovery
                .validate(&expected.replica)
                .map_err(PgDurableError::Invalid)?;
        }
        if self.outbound_builds.len() + self.suspended_builds.len() > crate::build::MAX_BUILDS {
            return Err(PgDurableError::Invalid("too many native builds".into()));
        }
        if self.retired_builds.len() > MAX_RETAINED_BUILD_IDS
            || self.retired_builds.iter().any(|id| !bounded_build_id(id))
            || self
                .retired_build_epoch
                .is_some_and(|retired| self.build_epoch.is_none_or(|current| retired >= current))
        {
            return Err(PgDurableError::Invalid(
                "invalid bounded build history".into(),
            ));
        }
        let mut ids = std::collections::BTreeSet::new();
        for build in self.outbound_builds.iter().chain(&self.suspended_builds) {
            build.validate().map_err(PgDurableError::Invalid)?;
            if build.request.resource_uid != self.identity.resource_uid
                || build.request.authority.source != self.identity.replica
                || !ids.insert(build.request.authority.build_id.clone())
                || self.build_is_terminal(&build.request.authority)
            {
                return Err(PgDurableError::Invalid(
                    "invalid outbound native build".into(),
                ));
            }
        }
        if self.outbound_attempts.iter().any(|(id, generation)| {
            *generation == 0
                || *generation > self.generation
                || !self
                    .outbound_builds
                    .iter()
                    .any(|b| &b.request.authority.build_id == id)
        }) {
            return Err(PgDurableError::Invalid(
                "invalid outbound build attempt".into(),
            ));
        }
        ids.extend(self.retired_builds.iter().cloned());
        if ids.len() > MAX_RETAINED_BUILD_IDS {
            return Err(PgDurableError::Invalid(
                "build history bound exceeded".into(),
            ));
        }
        if let Some(build) = &self.native_build {
            build.validate().map_err(PgDurableError::Invalid)?;
            if build.request.resource_uid != self.identity.resource_uid
                || build.request.authority.target != self.identity.replica
                || self.accepted_build.as_ref() != Some(&build.request.authority)
                || !self.external_access_closed
                    && build.stage != crate::build::PgBuildStage::Complete
            {
                return Err(PgDurableError::Invalid(
                    "invalid target native build".into(),
                ));
            }
        }
        if self.postgres_stopped && !self.external_access_closed {
            return Err(PgDurableError::Invalid(
                "stopped PostgreSQL cannot have external access granted".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn build_is_terminal(&self, authority: &BuildAuthority) -> bool {
        self.retired_builds.contains(&authority.build_id)
            || self
                .retired_build_epoch
                .is_some_and(|epoch| authority.current_configuration.epoch <= epoch)
    }

    pub(crate) fn suspend_build(&mut self, id: &OperationId) {
        if let Some(index) = self
            .outbound_builds
            .iter()
            .position(|b| &b.request.authority.build_id == id)
        {
            let build = self.outbound_builds.remove(index);
            self.suspended_builds
                .retain(|b| &b.request.authority.build_id != id);
            self.suspended_builds.push(build);
        }
        self.outbound_attempts.remove(id);
    }

    pub(crate) fn reconcile_builds(
        &mut self,
        current: &ReplicaSetConfiguration,
    ) -> Result<(), PgDurableError> {
        let epoch = current.configuration.epoch;
        if self.build_epoch.is_some_and(|previous| epoch < previous) {
            return Err(PgDurableError::Invalid(
                "build configuration is older than its durable epoch".into(),
            ));
        }
        if let Some(previous) = self.build_epoch
            && epoch > previous
        {
            self.retired_build_epoch = Some(previous);
            self.retired_builds.clear();
            self.outbound_builds
                .retain(|build| build.request.authority.current_configuration.epoch > previous);
            self.suspended_builds
                .retain(|build| build.request.authority.current_configuration.epoch > previous);
        }
        self.build_epoch = Some(epoch);
        let selected = current
            .replicas
            .iter()
            .filter(|r| !r.build_id.is_empty())
            .map(|r| &r.build_id)
            .collect::<BTreeSet<_>>();
        if selected.len() > crate::build::MAX_BUILDS
            || selected.iter().any(|id| !bounded_build_id(id))
        {
            return Err(PgDurableError::Invalid(
                "selected build descriptions exceed their bound".into(),
            ));
        }
        for build in &self.outbound_builds {
            if !selected.contains(&build.request.authority.build_id)
                && !self.retired_build_epoch.is_some_and(|floor| {
                    build.request.authority.current_configuration.epoch <= floor
                })
            {
                self.retired_builds
                    .insert(build.request.authority.build_id.clone());
            }
        }
        for build in &self.suspended_builds {
            if !selected.contains(&build.request.authority.build_id)
                && !self.retired_build_epoch.is_some_and(|floor| {
                    build.request.authority.current_configuration.epoch <= floor
                })
            {
                self.retired_builds
                    .insert(build.request.authority.build_id.clone());
            }
        }
        if let Some(build) = &self.native_build
            && !selected.contains(&build.request.authority.build_id)
            && !self
                .retired_build_epoch
                .is_some_and(|floor| build.request.authority.current_configuration.epoch <= floor)
        {
            self.retired_builds
                .insert(build.request.authority.build_id.clone());
        }
        self.outbound_builds
            .retain(|b| !self.retired_builds.contains(&b.request.authority.build_id));
        self.suspended_builds
            .retain(|b| !self.retired_builds.contains(&b.request.authority.build_id));
        self.outbound_attempts.retain(|id, _| {
            self.outbound_builds
                .iter()
                .any(|b| &b.request.authority.build_id == id)
        });
        let mut known = self.retired_builds.clone();
        known.extend(selected.into_iter().cloned());
        known.extend(
            self.outbound_builds
                .iter()
                .chain(&self.suspended_builds)
                .map(|build| build.request.authority.build_id.clone()),
        );
        if known.len() > MAX_RETAINED_BUILD_IDS {
            return Err(PgDurableError::Invalid(
                "build history bound reached; a newer admitted epoch is required".into(),
            ));
        }
        Ok(())
    }
}

fn bounded_build_id(id: &OperationId) -> bool {
    !id.is_empty()
        && id.as_str().len() <= MAX_BUILD_ID_BYTES
        && !id.as_str().chars().any(char::is_control)
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StateEnvelope {
    state: PgDurableState,
    checksum: String,
}

pub struct PgDurableStore {
    root: PathBuf,
    state: Arc<Mutex<PgDurableState>>,
    workers: StdMutex<Vec<Weak<CommitWorker>>>,
    ownership: Arc<File>,
    pub(crate) policy_lock: Arc<Mutex<()>>,
    pub(crate) policy_fence: std::sync::atomic::AtomicU64,
    #[cfg(feature = "testing")]
    commit_hook: std::sync::Mutex<Option<CommitHook>>,
}

#[cfg(feature = "testing")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitStage {
    BeforeRename,
    CommittedBeforePublish,
    Published,
}

#[cfg(feature = "testing")]
pub struct CommitGate {
    pub entered: Arc<tokio::sync::Notify>,
    state: Arc<GateState>,
}

#[cfg(feature = "testing")]
impl CommitGate {
    pub fn release(&self) {
        *self.state.released.lock().unwrap() = true;
        self.state.changed.notify_all();
    }
}

#[cfg(feature = "testing")]
impl Drop for CommitGate {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(feature = "testing")]
struct GateState {
    released: StdMutex<bool>,
    changed: Condvar,
}

#[cfg(feature = "testing")]
struct CommitHook {
    stage: CommitStage,
    catch_up_only: bool,
    entered: Arc<tokio::sync::Notify>,
    state: Arc<GateState>,
}

#[cfg(feature = "testing")]
impl CommitHook {
    fn checkpoint(&self, stage: CommitStage, control: &CommitControl) {
        if self.stage == stage {
            self.entered.notify_one();
            let mut released = self.state.released.lock().unwrap();
            while !*released && !control.cancel_requested.load(Ordering::Acquire) {
                released = self.state.changed.wait(released).unwrap();
            }
        }
    }
}

struct CommitControl {
    // Cancellation and the non-cancellable rename/publication section arbitrate
    // once: 0=pending, 1=committing, 2=cancelled.
    phase: AtomicU8,
    cancel_requested: AtomicBool,
    #[cfg(feature = "testing")]
    gate: Option<Arc<GateState>>,
}

impl CommitControl {
    fn cancel(&self) {
        self.cancel_requested.store(true, Ordering::Release);
        let _ = self
            .phase
            .compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire);
        #[cfg(feature = "testing")]
        if let Some(gate) = &self.gate {
            let _released = gate.released.lock().unwrap();
            gate.changed.notify_all();
        }
    }

    fn begin_commit(&self) -> Result<(), PgDurableError> {
        self.phase
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| PgDurableError::Cancelled)
    }
}

struct CommitWorker {
    control: Arc<CommitControl>,
    thread: StdMutex<Option<std::thread::JoinHandle<()>>>,
}

impl CommitWorker {
    fn cancel_and_join(&self) {
        self.control.cancel();
        if let Some(thread) = self.thread.lock().unwrap().take() {
            let _ = thread.join();
        }
    }
}

impl Drop for CommitWorker {
    fn drop(&mut self) {
        self.cancel_and_join();
    }
}

struct CommitWaiter(Arc<CommitWorker>);

impl Drop for CommitWaiter {
    fn drop(&mut self) {
        self.0.cancel_and_join();
    }
}

impl Drop for PgDurableStore {
    fn drop(&mut self) {
        for worker in self
            .workers
            .get_mut()
            .unwrap()
            .iter()
            .filter_map(Weak::upgrade)
        {
            worker.cancel_and_join();
        }
    }
}

impl PgDurableStore {
    pub(crate) async fn prepare_process_clock(&self) -> Result<u64, crate::instance::PgError> {
        let initialized = self.snapshot().await.process_clock_initialized;
        let path = self.root.join("process-generation-v2");
        if !initialized && !path.exists() {
            self.write_process_clock(0)?;
        }
        let value = self.read_process_clock()?;
        if !initialized {
            self.update(|state| {
                state.process_clock_initialized = true;
                Ok(())
            })
            .await
            .map_err(|error| crate::instance::PgError::Process(error.to_string()))?;
        }
        Ok(value)
    }

    fn read_process_clock(&self) -> Result<u64, crate::instance::PgError> {
        use std::io::Read;
        let read = || -> std::io::Result<u64> {
            let mut file = File::open(self.root.join("process-generation-v2"))?;
            let mut data = [0; 40];
            file.read_exact(&mut data)?;
            let mut extra = [0];
            if file.read(&mut extra)? != 0 || Sha256::digest(&data[..8])[..] != data[8..] {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid process generation checksum/length",
                ));
            }
            Ok(u64::from_be_bytes(data[..8].try_into().unwrap()))
        };
        read().map_err(|error| {
            crate::instance::PgError::Process(format!("read process generation: {error}"))
        })
    }

    fn write_process_clock(&self, value: u64) -> Result<(), crate::instance::PgError> {
        use std::io::Write;
        let write = || -> std::io::Result<()> {
            let bytes = value.to_be_bytes();
            let path = self.root.join("process-generation-v2.next");
            let mut file = File::create(&path)?;
            file.write_all(&bytes)?;
            file.write_all(&Sha256::digest(bytes))?;
            file.sync_all()?;
            std::fs::rename(path, self.root.join("process-generation-v2"))?;
            File::open(&self.root)?.sync_all()
        };
        write().map_err(|error| {
            crate::instance::PgError::Process(format!("persist process generation: {error}"))
        })
    }

    pub(crate) fn advance_process_clock(
        &self,
        expected: u64,
        next: u64,
    ) -> Result<(), crate::instance::PgError> {
        if self.read_process_clock()? != expected || next <= expected {
            return Err(crate::instance::PgError::Process(
                "process generation changed outside its owner".into(),
            ));
        }
        self.write_process_clock(next)
    }

    #[cfg(feature = "testing")]
    pub fn pause_commit(&self, stage: CommitStage, catch_up_only: bool) -> CommitGate {
        let entered = Arc::new(tokio::sync::Notify::new());
        let state = Arc::new(GateState {
            released: StdMutex::new(false),
            changed: Condvar::new(),
        });
        *self.commit_hook.lock().unwrap() = Some(CommitHook {
            stage,
            catch_up_only,
            entered: entered.clone(),
            state: state.clone(),
        });
        CommitGate { entered, state }
    }

    pub async fn open(
        root: impl AsRef<Path>,
        expected: PgDurableIdentity,
        mode: StorageMode,
    ) -> Result<Self, PgDurableError> {
        let root = root.as_ref().to_path_buf();
        if mode == StorageMode::Fresh {
            tokio::fs::create_dir_all(&root)
                .await
                .map_err(PgDurableError::Io)?;
        }
        let path = root.join(STATE_FILE);
        let state = match (
            tokio::fs::try_exists(&path)
                .await
                .map_err(PgDurableError::Io)?,
            mode,
        ) {
            (true, StorageMode::Fresh) => return Err(PgDurableError::AlreadyExists),
            (false, StorageMode::Established) => return Err(PgDurableError::Missing),
            (false, StorageMode::Fresh) => PgDurableState::new(expected.clone()),
            (true, StorageMode::Established) => read_state(&path).await?,
        };
        state.validate(&expected)?;
        let ownership = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("state-v2.owner"))
            .map_err(PgDurableError::Io)?;
        ownership.try_lock().map_err(|error| {
            let error: std::io::Error = error.into();
            if error.kind() == std::io::ErrorKind::WouldBlock {
                PgDurableError::AlreadyOwned
            } else {
                PgDurableError::Io(error)
            }
        })?;
        let state = if mode == StorageMode::Established {
            let current = read_state(&path).await?;
            current.validate(&expected)?;
            current
        } else {
            state
        };
        let policy_generation = state.synchronous_generation;
        let store = Self {
            root,
            state: Arc::new(Mutex::new(state)),
            workers: StdMutex::new(Vec::new()),
            ownership: Arc::new(ownership),
            policy_lock: Arc::new(Mutex::new(())),
            policy_fence: std::sync::atomic::AtomicU64::new(policy_generation),
            #[cfg(feature = "testing")]
            commit_hook: std::sync::Mutex::new(None),
        };
        if mode == StorageMode::Fresh {
            store.persist().await?;
        }
        Ok(store)
    }

    pub async fn snapshot(&self) -> PgDurableState {
        self.state.lock().await.clone()
    }

    pub async fn revalidate(&self) -> Result<PgDurableState, PgDurableError> {
        let expected = self.state.lock().await;
        let actual = read_state(&self.root.join(STATE_FILE)).await?;
        actual.validate(&expected.identity)?;
        if actual != *expected {
            return Err(PgDurableError::Invalid(
                "durable PostgreSQL evidence changed outside its owner".into(),
            ));
        }
        Ok(actual)
    }

    pub async fn update(
        &self,
        update: impl FnOnce(&mut PgDurableState) -> Result<(), PgDurableError>,
    ) -> Result<PgDurableState, PgDurableError> {
        let mut state = self.state.clone().lock_owned().await;
        let mut next = state.clone();
        update(&mut next)?;
        next.generation = state
            .generation
            .checked_add(1)
            .ok_or_else(|| PgDurableError::Invalid("durable generation overflow".into()))?;
        next.validate(&state.identity)?;
        if next.system_identifier == state.system_identifier
            && next.recovery_state != PgRecoveryState::Rebuilding
            && (next.current_lsn < state.current_lsn
                || next.flush_lsn < state.flush_lsn
                || next.received_lsn.zip(state.received_lsn).is_some_and(
                    |(next_received, previous_received)| next_received < previous_received,
                )
                || next
                    .replay_lsn
                    .zip(state.replay_lsn)
                    .is_some_and(|(next_replay, previous_replay)| next_replay < previous_replay)
                || next.policy_certified_lsn < state.policy_certified_lsn)
        {
            return Err(PgDurableError::Invalid(
                "durable PostgreSQL progress cannot regress outside rebuilding".into(),
            ));
        }
        let path = self.root.join(STATE_FILE);
        #[cfg(feature = "testing")]
        let hook = {
            let mut pending = self.commit_hook.lock().unwrap();
            if pending
                .as_ref()
                .is_some_and(|hook| !hook.catch_up_only || next.catch_up != state.catch_up)
            {
                pending.take()
            } else {
                None
            }
        };
        let control = Arc::new(CommitControl {
            phase: AtomicU8::new(0),
            cancel_requested: AtomicBool::new(false),
            #[cfg(feature = "testing")]
            gate: hook.as_ref().map(|hook| hook.state.clone()),
        });
        let worker = Arc::new(CommitWorker {
            control: control.clone(),
            thread: StdMutex::new(None),
        });
        let waiter = CommitWaiter(worker.clone());
        let (completed, result) = tokio::sync::oneshot::channel();
        let ownership = self.ownership.clone();
        let thread = std::thread::Builder::new()
            .name("postgres-metadata-commit".into())
            .spawn(move || {
                let _ownership = ownership;
                let committed = (|| {
                    let _file_lock = lock_metadata(&path, Some(&control))?;
                    if read_state_sync(&path)? != *state {
                        return Err(PgDurableError::Invalid(
                            "metadata generation changed outside its owner".into(),
                        ));
                    }
                    let mut renamed = false;
                    let result = write_state(
                        &path,
                        &next,
                        &mut renamed,
                        || control.begin_commit(),
                        |stage| {
                            #[cfg(feature = "testing")]
                            if let Some(hook) = &hook {
                                hook.checkpoint(stage, &control);
                            }
                            #[cfg(not(feature = "testing"))]
                            let _ = stage;
                        },
                    );
                    if renamed {
                        *state = next.clone();
                        #[cfg(feature = "testing")]
                        if let Some(hook) = &hook {
                            hook.checkpoint(CommitStage::Published, &control);
                        }
                    }
                    result?;
                    Ok(next)
                })();
                drop(state);
                let _ = completed.send(committed);
            })
            .map_err(PgDurableError::Io)?;
        *worker.thread.lock().unwrap() = Some(thread);
        {
            let mut workers = self.workers.lock().unwrap();
            workers.retain(|worker| worker.strong_count() != 0);
            workers.push(Arc::downgrade(&worker));
        }
        let committed = result.await.map_err(|_| {
            PgDurableError::Invalid("metadata worker did not publish an outcome".into())
        })?;
        drop(waiter);
        committed
    }

    async fn persist(&self) -> Result<(), PgDurableError> {
        let state = self.state.lock().await.clone();
        let path = self.root.join(STATE_FILE);
        let _file_lock = lock_metadata(&path, None)?;
        if path.exists() {
            return Err(PgDurableError::AlreadyExists);
        }
        write_state(&path, &state, &mut false, || Ok(()), |_| {})
    }

    #[cfg(test)]
    pub(crate) async fn persist_unchecked_for_test(
        &self,
        state: PgDurableState,
    ) -> Result<(), PgDurableError> {
        let path = self.root.join(STATE_FILE);
        let _file_lock = lock_metadata(&path, None)?;
        write_state(&path, &state, &mut false, || Ok(()), |_| {})?;
        *self.state.lock().await = state;
        Ok(())
    }
}

async fn read_state(path: &Path) -> Result<PgDurableState, PgDurableError> {
    use tokio::io::AsyncReadExt;
    let file = tokio::fs::File::open(path)
        .await
        .map_err(PgDurableError::Io)?;
    let mut bytes = Vec::new();
    file.take(MAX_METADATA_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(PgDurableError::Io)?;
    decode_state(&bytes)
}

fn read_state_sync(path: &Path) -> Result<PgDurableState, PgDurableError> {
    use std::io::Read;
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(PgDurableError::Io)?
        .take(MAX_METADATA_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(PgDurableError::Io)?;
    decode_state(&bytes)
}

fn decode_state(bytes: &[u8]) -> Result<PgDurableState, PgDurableError> {
    if bytes.len() > MAX_METADATA_BYTES {
        return Err(PgDurableError::Invalid(
            "metadata byte bound exceeded".into(),
        ));
    }
    let envelope: StateEnvelope = serde_json::from_slice(bytes).map_err(PgDurableError::Json)?;
    let checksum = checksum(&envelope.state)?;
    if checksum != envelope.checksum {
        return Err(PgDurableError::Checksum);
    }
    Ok(envelope.state)
}

fn lock_metadata(path: &Path, control: Option<&CommitControl>) -> Result<File, PgDurableError> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path.with_extension("lock"))
        .map_err(PgDurableError::Io)?;
    loop {
        if control.is_some_and(|control| control.cancel_requested.load(Ordering::Acquire)) {
            return Err(PgDurableError::Cancelled);
        }
        match file.try_lock() {
            Ok(()) => return Ok(file),
            Err(error) => {
                let error: std::io::Error = error.into();
                if error.kind() != std::io::ErrorKind::WouldBlock {
                    return Err(PgDurableError::Io(error));
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
    }
}

#[cfg(not(feature = "testing"))]
enum CommitStage {
    BeforeRename,
    CommittedBeforePublish,
}

fn write_state(
    path: &Path,
    state: &PgDurableState,
    renamed: &mut bool,
    begin_commit: impl FnOnce() -> Result<(), PgDurableError>,
    checkpoint: impl Fn(CommitStage),
) -> Result<(), PgDurableError> {
    let envelope = StateEnvelope {
        state: state.clone(),
        checksum: checksum(state)?,
    };
    let bytes = serde_json::to_vec_pretty(&envelope).map_err(PgDurableError::Json)?;
    if bytes.len() > MAX_METADATA_BYTES {
        return Err(PgDurableError::Invalid(
            "metadata byte bound exceeded".into(),
        ));
    }
    let temporary = path.with_extension("json.tmp");
    std::fs::write(&temporary, bytes).map_err(PgDurableError::Io)?;
    File::open(&temporary)
        .and_then(|file| file.sync_all())
        .map_err(PgDurableError::Io)?;
    checkpoint(CommitStage::BeforeRename);
    if let Err(error) = begin_commit() {
        std::fs::remove_file(&temporary).map_err(PgDurableError::Io)?;
        return Err(error);
    }
    std::fs::rename(&temporary, path).map_err(PgDurableError::Io)?;
    *renamed = true;
    if let Some(parent) = path.parent() {
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(PgDurableError::Io)?;
    }
    checkpoint(CommitStage::CommittedBeforePublish);
    Ok(())
}

fn checksum(state: &PgDurableState) -> Result<String, PgDurableError> {
    let bytes = serde_json::to_vec(state).map_err(PgDurableError::Json)?;
    let digest = Sha256::digest(bytes);
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

#[derive(Debug, thiserror::Error)]
pub enum PgDurableError {
    #[error("PostgreSQL durable state already has a live owner")]
    AlreadyOwned,
    #[error("PostgreSQL metadata update cancelled before commit")]
    Cancelled,
    #[error("PostgreSQL durable state already exists")]
    AlreadyExists,
    #[error("PostgreSQL durable state is missing")]
    Missing,
    #[error("PostgreSQL durable identity mismatch")]
    IdentityMismatch,
    #[error("PostgreSQL durable state checksum mismatch")]
    Checksum,
    #[error("PostgreSQL durable state version mismatch: expected {expected}, observed {observed}")]
    Version { expected: u32, observed: u32 },
    #[error("invalid PostgreSQL durable state: {0}")]
    Invalid(String),
    #[error("PostgreSQL durable state I/O failed: {0}")]
    Io(std::io::Error),
    #[error("PostgreSQL durable state JSON failed: {0}")]
    Json(serde_json::Error),
}

#[cfg(test)]
mod tests {
    use kuberic_runtime::protocol::types::{AgentGeneration, ReplicaId, ReplicaInstanceId};

    use super::*;

    fn identity() -> PgDurableIdentity {
        PgDurableIdentity {
            resource_uid: ResourceUid::new("resource"),
            replica: ReplicaIdentity {
                replica_id: ReplicaId::new(1),
                instance_id: ReplicaInstanceId::new("pod-1"),
                agent_generation: AgentGeneration::new("generation-1"),
            },
        }
    }

    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn exhausted_policy_and_metadata_generations_remain_fenced_after_reopen() {
        use crate::instance::PgInstanceManager;
        use crate::native::{PgNativeObserver, compile_synchronous_configuration};
        use crate::testing::{TestDataDir, allocate_port, find_pg_bin, native_configuration};
        let root = TestDataDir::new("policy-max");
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
        let policy = compile_synchronous_configuration(
            None,
            &native_configuration(std::slice::from_ref(&identity().replica), 0, 1),
            &identity().replica,
            &BTreeMap::new(),
            true,
        )
        .unwrap();
        let observer = PgNativeObserver::with_store(instance.clone(), store.clone()).await;
        observer.apply_synchronous(policy.clone()).await.unwrap();
        {
            let mut state = store.state.lock().await;
            state.generation = u64::MAX;
            state.synchronous_generation = u64::MAX;
            write_state(
                &store.root.join(STATE_FILE),
                &state,
                &mut false,
                || Ok(()),
                |_| {},
            )
            .unwrap();
        }
        assert!(observer.apply_synchronous(policy.clone()).await.is_err());
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
        assert_eq!(store.revalidate().await.unwrap().generation, u64::MAX);
        drop(observer);
        drop(store);
        let store = Arc::new(
            PgDurableStore::open(
                root.path().join("meta"),
                identity(),
                StorageMode::Established,
            )
            .await
            .unwrap(),
        );
        let observer = PgNativeObserver::with_store(instance.clone(), store.clone()).await;
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
        assert!(observer.apply_synchronous(policy).await.is_err());
        assert_eq!(
            store.revalidate().await.unwrap().synchronous_generation,
            u64::MAX
        );
        instance.stop().await.unwrap();
    }

    #[test]
    fn durable_validation_rejects_version_progress_and_fence_mismatches() {
        let expected = identity();
        let mut state = PgDurableState::new(expected.clone());
        state.version = 0;
        assert!(matches!(
            state.validate(&expected),
            Err(PgDurableError::Version { .. })
        ));

        let mut state = PgDurableState::new(expected.clone());
        state.current_lsn = 1;
        state.flush_lsn = 2;
        assert!(matches!(
            state.validate(&expected),
            Err(PgDurableError::Invalid(_))
        ));

        let mut state = PgDurableState::new(expected.clone());
        state.postgres_stopped = true;
        state.external_access_closed = false;
        assert!(matches!(
            state.validate(&expected),
            Err(PgDurableError::Invalid(_))
        ));

        let mut state = PgDurableState::new(expected.clone());
        state.received_lsn = Some(2);
        state.replay_lsn = Some(3);
        assert!(matches!(
            state.validate(&expected),
            Err(PgDurableError::Invalid(_))
        ));
    }
}
