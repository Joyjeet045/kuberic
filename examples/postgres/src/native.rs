use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use kuberic_runtime::protocol::types::{
    ConfigurationDescriptor, ConfigurationId, ProcessSessionId, ReplicaIdentity,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, RwLock};

use crate::durable::{PgDurableStore, PgRecoveryState};
use crate::instance::{GenerationLease, PgError, PgInstanceManager};
use crate::monitor::parse_pg_lsn;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AcknowledgingReplica {
    pub identity: ReplicaIdentity,
    pub process_session_id: ProcessSessionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AcknowledgementPolicy {
    pub configuration_generation: u64,
    pub configuration_id: ConfigurationId,
    pub valid: bool,
    pub write_acknowledgements: u32,
    pub eligible_standbys: Vec<AcknowledgingReplica>,
}

impl AcknowledgementPolicy {
    pub fn validate(&self) -> Result<(), String> {
        if self.configuration_generation == 0
            || self.configuration_id.is_empty()
            || self.configuration_id.as_str().len() > 128
            || self.eligible_standbys.len() > 16
        {
            return Err("invalid PostgreSQL synchronous configuration".into());
        }
        let mut identities = BTreeSet::new();
        let mut sessions = BTreeSet::new();
        for replica in &self.eligible_standbys {
            if replica.process_session_id.is_empty()
                || replica.process_session_id.as_str().len() > 128
                || replica.identity.instance_id.as_str().len() > 128
                || replica.identity.agent_generation.as_str().len() > 128
                || !identities.insert(&replica.identity)
                || !sessions.insert(&replica.process_session_id)
            {
                return Err("invalid PostgreSQL synchronous replica session".into());
            }
        }
        if self.valid {
            if self.write_acknowledgements as usize > self.eligible_standbys.len()
                || (self.write_acknowledgements == 0 && !self.eligible_standbys.is_empty())
            {
                return Err("invalid PostgreSQL synchronous acknowledgement count".into());
            }
        } else if self.write_acknowledgements != 0 {
            return Err("invalid PostgreSQL policy cannot grant acknowledgements".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PgReplicationEvidence {
    pub engine: String,
    pub system_identifier: String,
    pub timeline_id: u32,
    pub in_recovery: bool,
    pub flush_lsn: i64,
    pub received_lsn: Option<i64>,
    pub replay_lsn: Option<i64>,
    pub metadata_generation: u64,
    pub synchronous: Option<AcknowledgementPolicy>,
    #[serde(default)]
    pub wal_receiver_stopped: bool,
}

impl PgReplicationEvidence {
    pub fn validate(&self) -> Result<(), String> {
        if self.engine != "postgres-physical"
            || self.system_identifier.is_empty()
            || self.system_identifier.len() > 128
            || self.timeline_id == 0
            || self.metadata_generation == 0
            || self.flush_lsn < 0
            || self.received_lsn.is_some_and(|p| p < 0)
            || self.replay_lsn.is_some_and(|p| p < 0)
            || self
                .received_lsn
                .zip(self.replay_lsn)
                .is_some_and(|(received, replay)| replay > received)
            || !self.in_recovery
                && (self.received_lsn.is_some()
                    || self.replay_lsn.is_some()
                    || self.wal_receiver_stopped)
        {
            return Err("invalid PostgreSQL recovery evidence".into());
        }
        if let Some(policy) = &self.synchronous {
            policy.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct PgObservation {
    pub current_lsn: i64,
    pub committed_lsn: i64,
    pub current_configuration_quorum_lsn: i64,
    pub catch_up_boundary: Option<i64>,
    pub catch_up_complete: bool,
    pub evidence: Option<PgReplicationEvidence>,
}

pub struct PgNativeObserver {
    instance: Arc<PgInstanceManager>,
    policy: RwLock<(u64, Option<AcknowledgementPolicy>, Option<u64>)>,
    policy_fence: AtomicU64,
    durable: Option<Arc<PgDurableStore>>,
    configuration_lock: Arc<Mutex<()>>,
    #[cfg(feature = "testing")]
    policy_hook: std::sync::Mutex<Option<(PolicyStage, Arc<PolicyGate>)>>,
}

#[cfg(feature = "testing")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyStage {
    InvalidationStarted,
    Invalidated,
    Applied,
    ReadBack,
    Published,
}

#[cfg(feature = "testing")]
pub struct PolicyGate {
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}

impl PgNativeObserver {
    pub(crate) async fn write_quorum_present(
        &self,
        policy: &AcknowledgementPolicy,
    ) -> Result<bool, PgError> {
        let lease = self.instance.generation_lease();
        self.instance.generation_step(&lease, async {
            let expected = policy.eligible_standbys.iter().map(|p| replication_application_name(&p.identity, &p.process_session_id))
                .collect::<BTreeSet<_>>();
            let (client, connection) = self.instance.connect().await?;
            let rows = tokio::time::timeout(std::time::Duration::from_secs(5),
                client.query("SELECT application_name FROM pg_stat_replication WHERE state = 'streaming' AND replay_lsn IS NOT NULL", &[]))
                .await.map_err(|_| PgError::Timeout("write quorum observation".into()))?
                .map_err(|error| PgError::Query(error.to_string()))?;
            let matched = rows.iter().map(|row| row.get::<_, String>(0)).filter(|name| expected.contains(name)).collect::<BTreeSet<_>>();
            drop(client);
            connection.await.map_err(|error| PgError::Connection(error.to_string()))?;
            Ok(matched.len() >= policy.write_acknowledgements as usize)
        }).await
    }

    #[cfg(feature = "testing")]
    pub fn pause_policy(&self, stage: PolicyStage) -> Arc<PolicyGate> {
        let gate = Arc::new(PolicyGate {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        *self.policy_hook.lock().unwrap() = Some((stage, gate.clone()));
        gate
    }

    #[cfg(feature = "testing")]
    async fn policy_checkpoint(&self, stage: PolicyStage) {
        let gate = {
            let mut hook = self.policy_hook.lock().unwrap();
            if hook
                .as_ref()
                .is_some_and(|(expected, _)| *expected == stage)
            {
                hook.take().map(|(_, gate)| gate)
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
    }

    pub(crate) async fn reconcile_replication_slots(
        &self,
        local: &ReplicaIdentity,
        configuration: &kuberic_runtime::replicator::ReplicaSetConfiguration,
    ) -> Result<(), PgError> {
        self.instance
            .generation_step(
                &self.instance.generation_lease(),
                self.reconcile_slots_inner(local, configuration),
            )
            .await
    }

    async fn reconcile_slots_inner(
        &self,
        local: &ReplicaIdentity,
        configuration: &kuberic_runtime::replicator::ReplicaSetConfiguration,
    ) -> Result<(), PgError> {
        let expected = configuration
            .replicas
            .iter()
            .filter(|r| &r.identity != local)
            .map(|r| replication_slot_name(&r.identity))
            .collect::<BTreeSet<_>>();
        let (client, _connection) = self.instance.connect().await?;
        let existing = client
            .query(
                "SELECT slot_name, active FROM pg_replication_slots \
             WHERE slot_type = 'physical' AND slot_name ~ '^kuberic_[0-9a-f]{32}$'",
                &[],
            )
            .await
            .map_err(|error| PgError::Query(format!("read managed physical slots: {error}")))?;
        let mut present = BTreeSet::new();
        for row in existing {
            let slot: String = row.get(0);
            present.insert(slot.clone());
            if !expected.contains(&slot) {
                retire_slot(&client, &slot).await?;
            }
        }
        for slot in expected.difference(&present) {
            client
                .query_one(
                    "SELECT pg_create_physical_replication_slot($1::name, true)",
                    &[slot],
                )
                .await
                .map_err(|error| {
                    PgError::Query(format!("reserve physical replica WAL: {error}"))
                })?;
        }
        Ok(())
    }

    pub(crate) async fn all_replayed(
        &self,
        required: &BTreeMap<ReplicaIdentity, ProcessSessionId>,
        boundary: i64,
    ) -> Result<bool, PgError> {
        self.instance
            .generation_step(
                &self.instance.generation_lease(),
                self.all_replayed_inner(required, boundary),
            )
            .await
    }

    async fn all_replayed_inner(
        &self,
        required: &BTreeMap<ReplicaIdentity, ProcessSessionId>,
        boundary: i64,
    ) -> Result<bool, PgError> {
        let expected = required
            .iter()
            .map(|(identity, session)| replication_application_name(identity, session))
            .collect::<BTreeSet<_>>();
        let (client, _connection) = self.instance.connect().await?;
        let rows = client
            .query(
                "SELECT application_name, replay_lsn::text FROM pg_stat_replication",
                &[],
            )
            .await
            .map_err(|error| PgError::Query(format!("all-replica replay: {error}")))?;
        let mut observed = BTreeSet::new();
        let mut complete = true;
        for row in rows {
            let name: String = row.get(0);
            if !expected.contains(&name) {
                continue;
            }
            if !observed.insert(name) {
                return Err(PgError::Query(
                    "duplicate exact replica session during All catch-up".into(),
                ));
            }
            let replay: Option<&str> = row.get(1);
            complete &= replay
                .map(parse_pg_lsn)
                .transpose()?
                .is_some_and(|lsn| lsn >= boundary);
        }
        Ok(complete && observed == expected)
    }

    pub fn new(instance: Arc<PgInstanceManager>) -> Self {
        Self {
            instance,
            policy: RwLock::new((1, None, None)),
            policy_fence: AtomicU64::new(0),
            durable: None,
            configuration_lock: Arc::new(Mutex::new(())),
            #[cfg(feature = "testing")]
            policy_hook: std::sync::Mutex::new(None),
        }
    }

    pub async fn with_store(
        instance: Arc<PgInstanceManager>,
        durable: Arc<PgDurableStore>,
    ) -> Self {
        Self {
            instance,
            policy: RwLock::new((1, None, None)),
            policy_fence: AtomicU64::new(0),
            configuration_lock: durable.policy_lock.clone(),
            #[cfg(feature = "testing")]
            policy_hook: std::sync::Mutex::new(None),
            durable: Some(durable),
        }
    }

    pub async fn set_synchronous(&self, synchronous: AcknowledgementPolicy) -> Result<(), PgError> {
        let lease = self.instance.generation_lease();
        self.instance
            .generation_operation(self.set_synchronous_inner(&lease, synchronous))
            .await
    }

    async fn set_synchronous_inner(
        &self,
        lease: &GenerationLease,
        synchronous: AcknowledgementPolicy,
    ) -> Result<(), PgError> {
        let _configuration = self.configuration_lock.lock().await;
        if synchronous.valid {
            return Err(PgError::Configuration(
                "valid synchronous metadata requires native apply/read-back".into(),
            ));
        }
        let version = self
            .instance
            .generation_step(lease, async { Ok(self.fence_policy().await) })
            .await?;
        #[cfg(feature = "testing")]
        self.policy_checkpoint(PolicyStage::InvalidationStarted)
            .await;
        self.instance
            .generation_step(lease, self.publish_synchronous(lease, synchronous, version))
            .await?;
        #[cfg(feature = "testing")]
        self.policy_checkpoint(PolicyStage::Invalidated).await;
        Ok(())
    }

    async fn committed_policy(&self) -> (u64, u64, Option<AcknowledgementPolicy>, Option<u64>) {
        if let Some(durable) = &self.durable {
            let state = durable.snapshot().await;
            (
                state.synchronous_generation,
                state.generation,
                state.synchronous,
                state.synchronous_process_generation,
            )
        } else {
            let policy = self.policy.read().await;
            (policy.0, policy.0, policy.1.clone(), policy.2)
        }
    }

    async fn fence_policy(&self) -> u64 {
        let (version, _, _, _) = self.committed_policy().await;
        if let Some(durable) = &self.durable {
            durable.policy_fence.store(version, Ordering::Release);
        } else {
            self.policy_fence.store(version, Ordering::Release);
        }
        version
    }

    async fn publish_synchronous(
        &self,
        lease: &GenerationLease,
        synchronous: AcknowledgementPolicy,
        expected: u64,
    ) -> Result<u64, PgError> {
        synchronous.validate().map_err(PgError::Configuration)?;
        if let Some(durable) = &self.durable {
            let committed = durable
                .update(|state| {
                    if state.synchronous_generation != expected {
                        return Err(crate::durable::PgDurableError::Invalid(
                            "synchronous policy generation changed".into(),
                        ));
                    }
                    state.synchronous = Some(synchronous.clone());
                    state.synchronous_process_generation = synchronous.valid.then_some(lease.id());
                    state.synchronous_generation = expected.checked_add(1).ok_or_else(|| {
                        crate::durable::PgDurableError::Invalid(
                            "policy generation exhausted".into(),
                        )
                    })?;
                    Ok(())
                })
                .await
                .map_err(|error| PgError::Configuration(error.to_string()))?;
            return Ok(committed.synchronous_generation);
        }
        let mut policy = self.policy.write().await;
        if policy.0 != expected {
            return Err(PgError::Configuration(
                "synchronous policy generation changed".into(),
            ));
        }
        let version = expected
            .checked_add(1)
            .ok_or_else(|| PgError::Configuration("policy generation exhausted".into()))?;
        let owner = synchronous.valid.then_some(lease.id());
        *policy = (version, Some(synchronous), owner);
        Ok(version)
    }

    pub async fn apply_synchronous(
        &self,
        synchronous: AcknowledgementPolicy,
    ) -> Result<(), PgError> {
        let lease = self.instance.generation_lease();
        self.instance
            .generation_operation(self.apply_synchronous_inner(&lease, synchronous))
            .await
    }

    async fn apply_synchronous_inner(
        &self,
        lease: &GenerationLease,
        synchronous: AcknowledgementPolicy,
    ) -> Result<(), PgError> {
        let _configuration = self.configuration_lock.lock().await;
        synchronous.validate().map_err(PgError::Configuration)?;
        let version = self
            .instance
            .generation_step(lease, async { Ok(self.fence_policy().await) })
            .await?;
        if !synchronous.valid {
            return self
                .instance
                .generation_step(lease, self.publish_synchronous(lease, synchronous, version))
                .await
                .map(|_| ());
        }
        #[cfg(feature = "testing")]
        self.policy_checkpoint(PolicyStage::InvalidationStarted)
            .await;
        let mut invalid = synchronous.clone();
        invalid.valid = false;
        invalid.write_acknowledgements = 0;
        let version = self
            .instance
            .generation_step(lease, self.publish_synchronous(lease, invalid, version))
            .await?;
        #[cfg(feature = "testing")]
        self.policy_checkpoint(PolicyStage::Invalidated).await;
        let (client, _connection) = self
            .policy_step(lease, version, self.instance.connect())
            .await?;
        // Acquire the SQL session lock before any native mutation. Invalidation
        // is already durable, including when connection or lock acquisition fails.
        self.policy_step(lease, version, async {
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                client.query_one("SELECT pg_advisory_lock(1729364819)", &[]),
            )
            .await
            .map_err(|_| PgError::Timeout("synchronous configuration lock".into()))?
            .map_err(|error| PgError::Query(format!("lock synchronous configuration: {error}")))?;
            Ok(())
        })
        .await?;
        let setting = if synchronous.write_acknowledgements == 0 {
            String::new()
        } else {
            let names = synchronous
                .eligible_standbys
                .iter()
                .map(|standby| {
                    replication_application_name(&standby.identity, &standby.process_session_id)
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("ANY {} ({names})", synchronous.write_acknowledgements)
        };
        self.policy_step(lease, version, async {
            client
                .simple_query(&format!(
                    "ALTER SYSTEM SET synchronous_standby_names = '{}'",
                    setting.replace('\'', "''")
                ))
                .await
                .map_err(|error| {
                    PgError::Query(format!("configure synchronous_standby_names: {error}"))
                })?;
            Ok(())
        })
        .await?;
        self.policy_step(lease, version, async {
            client
                .query_one("SELECT pg_reload_conf()", &[])
                .await
                .map_err(|error| PgError::Query(format!("reload PostgreSQL config: {error}")))?;
            Ok(())
        })
        .await?;
        #[cfg(feature = "testing")]
        self.policy_checkpoint(PolicyStage::Applied).await;
        let mut observed = String::new();
        for _ in 0..40 {
            observed = self
                .policy_step(lease, version, async {
                    Ok(client
                        .query_one("SHOW synchronous_standby_names", &[])
                        .await
                        .map_err(|error| {
                            PgError::Query(format!("read synchronous_standby_names: {error}"))
                        })?
                        .get(0))
                })
                .await?;
            if observed == setting {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        if observed != setting {
            return Err(PgError::Configuration(format!(
                "effective synchronous_standby_names differs: expected {setting:?}, observed {observed:?}"
            )));
        }
        #[cfg(feature = "testing")]
        self.policy_checkpoint(PolicyStage::ReadBack).await;
        self.instance
            .generation_step(lease, self.publish_synchronous(lease, synchronous, version))
            .await?;
        #[cfg(feature = "testing")]
        self.policy_checkpoint(PolicyStage::Published).await;
        Ok(())
    }

    async fn policy_step<T>(
        &self,
        lease: &GenerationLease,
        version: u64,
        operation: impl std::future::Future<Output = Result<T, PgError>>,
    ) -> Result<T, PgError> {
        self.instance
            .generation_step(lease, async {
                if self.committed_policy().await.0 != version {
                    return Err(PgError::Configuration(
                        "synchronous policy generation changed".into(),
                    ));
                }
                tokio::time::timeout(std::time::Duration::from_secs(5), operation)
                    .await
                    .map_err(|_| PgError::Timeout("synchronous policy step".into()))?
            })
            .await
    }

    pub async fn snapshot(&self) -> Result<PgObservation, PgError> {
        let _configuration = self.configuration_lock.lock().await;
        self.snapshot_locked().await
    }

    async fn snapshot_locked(&self) -> Result<PgObservation, PgError> {
        let generation = self.instance.generation_id();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.instance.generation_operation(self.snapshot_inner()),
        )
        .await
        .map_err(|_| {
            PgError::Timeout("PostgreSQL observation".into()).with_generation(generation)
        })?
    }

    async fn snapshot_inner(&self) -> Result<PgObservation, PgError> {
        let (client, _connection) = self.instance.connect().await?;
        let system_identifier: String = client
            .query_one(
                "SELECT system_identifier::text FROM pg_control_system()",
                &[],
            )
            .await
            .map_err(|error| PgError::Query(format!("pg_control_system: {error}")))?
            .get(0);
        let timeline_id: i32 = client
            .query_one(
                "SELECT GREATEST(c.timeline_id, r.min_recovery_end_timeline, \
                    COALESCE((SELECT received_tli FROM pg_stat_wal_receiver LIMIT 1), 0))::int \
                        FROM pg_control_checkpoint() c, pg_control_recovery() r",
                &[],
            )
            .await
            .map_err(|error| PgError::Query(format!("pg_control_checkpoint: {error}")))?
            .get(0);
        let in_recovery: bool = client
            .query_one("SELECT pg_is_in_recovery()", &[])
            .await
            .map_err(|error| PgError::Query(format!("pg_is_in_recovery: {error}")))?
            .get(0);
        let (policy_generation, metadata_generation, mut synchronous, owner) =
            self.committed_policy().await;
        let fence = self.durable.as_ref().map_or_else(
            || self.policy_fence.load(Ordering::Acquire),
            |durable| durable.policy_fence.load(Ordering::Acquire),
        );
        let current_owner = owner == Some(self.instance.generation_id());
        if (policy_generation <= fence || !current_owner)
            && let Some(policy) = &mut synchronous
        {
            policy.valid = false;
            policy.write_acknowledgements = 0;
        }
        let synchronous = if in_recovery { None } else { synchronous };
        let (current_lsn, flush_lsn, received_lsn, replay_lsn, policy_certified_lsn) =
            if in_recovery {
                let row = client
                    .query_one(
                        "SELECT pg_last_wal_replay_lsn()::text, pg_last_wal_receive_lsn()::text",
                        &[],
                    )
                    .await
                    .map_err(|error| PgError::Query(format!("standby WAL progress: {error}")))?;
                let received = row
                    .get::<_, Option<&str>>(1)
                    .map(parse_pg_lsn)
                    .transpose()?;
                let replay = row
                    .get::<_, Option<&str>>(0)
                    .map(parse_pg_lsn)
                    .transpose()?;
                // The streaming receiver's position resets on restart, whereas
                // replayed local WAL is already durable and recoverable.
                let received = received.into_iter().chain(replay).max();
                let current = received.unwrap_or_default();
                (
                    current,
                    received.unwrap_or_default(),
                    received,
                    replay,
                    replay.unwrap_or_default(),
                )
            } else {
                let row = client
                    .query_one(
                        "SELECT pg_current_wal_lsn()::text, pg_current_wal_flush_lsn()::text",
                        &[],
                    )
                    .await
                    .map_err(|error| PgError::Query(format!("primary WAL progress: {error}")))?;
                let current: &str = row.get(0);
                let flush: &str = row.get(1);
                let current = parse_pg_lsn(current)?;
                let flush = parse_pg_lsn(flush)?;
                let certified =
                    certify_primary_progress(&client, flush, synchronous.as_ref()).await?;
                (current, flush, None, None, certified)
            };
        let wal_receiver_stopped = if in_recovery {
            let count: i64 = client
                .query_one("SELECT count(*) FROM pg_stat_wal_receiver", &[])
                .await
                .map_err(|error| PgError::Query(format!("pg_stat_wal_receiver: {error}")))?
                .get(0);
            count == 0
        } else {
            false
        };
        Ok(PgObservation {
            current_lsn,
            committed_lsn: policy_certified_lsn,
            current_configuration_quorum_lsn: policy_certified_lsn,
            catch_up_boundary: None,
            catch_up_complete: false,
            evidence: Some(PgReplicationEvidence {
                engine: "postgres-physical".into(),
                system_identifier,
                timeline_id: u32::try_from(timeline_id)
                    .map_err(|_| PgError::Query("negative PostgreSQL timeline".into()))?,
                in_recovery,
                flush_lsn,
                received_lsn,
                replay_lsn,
                metadata_generation,
                synchronous,
                wal_receiver_stopped,
            }),
        })
    }

    pub async fn snapshot_and_persist(&self) -> Result<PgObservation, PgError> {
        let _configuration = self.configuration_lock.lock().await;
        self.instance
            .generation_step(
                &self.instance.generation_lease(),
                self.snapshot_and_persist_inner(),
            )
            .await
    }

    async fn snapshot_and_persist_inner(&self) -> Result<PgObservation, PgError> {
        let snapshot = self.snapshot_locked().await?;
        if let (Some(durable), Some(evidence)) = (&self.durable, snapshot.evidence.as_ref()) {
            let history_digest =
                timeline_history_digest(self.instance.data_dir(), evidence).await?;
            durable
                .update(|state| {
                    state.system_identifier = Some(evidence.system_identifier.clone());
                    state.timeline_id = Some(evidence.timeline_id);
                    state.timeline_history_digest = Some(history_digest);
                    state.recovery_state = PgRecoveryState::Ready;
                    state.role = if evidence.in_recovery {
                        crate::durable::PgDurableRole::Standby
                    } else {
                        crate::durable::PgDurableRole::Primary
                    };
                    state.current_lsn = snapshot.current_lsn;
                    state.flush_lsn = evidence.flush_lsn;
                    state.received_lsn = evidence.received_lsn;
                    state.replay_lsn = evidence.replay_lsn;
                    state.policy_certified_lsn =
                        state.policy_certified_lsn.max(snapshot.committed_lsn);
                    state.postgres_stopped = false;
                    Ok(())
                })
                .await
                .map_err(|error| PgError::Configuration(error.to_string()))?;
        }

        Ok(snapshot)
    }
}

pub(crate) async fn timeline_history_digest(
    data_dir: &std::path::Path,
    evidence: &PgReplicationEvidence,
) -> Result<String, PgError> {
    let mut entries = tokio::fs::read_dir(data_dir.join("pg_wal"))
        .await
        .map_err(|error| PgError::Process(format!("read pg_wal: {error}")))?;
    let mut history = Vec::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|error| PgError::Process(format!("read pg_wal entry: {error}")))?
    {
        if entry
            .path()
            .extension()
            .is_some_and(|extension| extension == "history")
        {
            history.push(entry.path());
        }
    }
    history.sort();
    let mut hasher = Sha256::new();
    hasher.update((evidence.system_identifier.len() as u64).to_be_bytes());
    hasher.update(evidence.system_identifier.as_bytes());
    hasher.update(evidence.timeline_id.to_be_bytes());
    for path in history {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| PgError::Process("non-UTF8 PostgreSQL history filename".into()))?;
        let bytes = tokio::fs::read(&path)
            .await
            .map_err(|error| PgError::Process(format!("read timeline history: {error}")))?;
        hasher.update((name.len() as u64).to_be_bytes());
        hasher.update(name.as_bytes());
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

pub fn compile_synchronous_configuration(
    previous: Option<&ConfigurationDescriptor>,
    current: &ConfigurationDescriptor,
    local: &ReplicaIdentity,
    sessions: &BTreeMap<ReplicaIdentity, ProcessSessionId>,
    valid: bool,
) -> Result<AcknowledgementPolicy, PgError> {
    let current_requirement = requirement(current, local, true)?;
    let expected = match previous {
        Some(previous) => {
            let previous_requirement = requirement(previous, local, false)?;
            match (previous_requirement.1, current_requirement.1) {
                (0, _) => current_requirement,
                (_, 0) => previous_requirement,
                (previous_count, current_count) => (
                    previous_requirement
                        .0
                        .intersection(&current_requirement.0)
                        .cloned()
                        .collect(),
                    previous_count.max(current_count),
                ),
            }
        }
        None => current_requirement,
    };
    if expected.0.len() < expected.1 {
        return Err(PgError::Configuration(
            "PC/CC policies have no safe PostgreSQL synchronous intersection".into(),
        ));
    }
    let eligible_standbys = expected
        .0
        .into_iter()
        .map(|identity| {
            let process_session_id = sessions.get(&identity).cloned().ok_or_else(|| {
                PgError::Configuration(format!(
                    "missing process session for synchronous standby {}",
                    identity.replica_id.value()
                ))
            })?;
            Ok(AcknowledgingReplica {
                identity,
                process_session_id,
            })
        })
        .collect::<Result<Vec<_>, PgError>>()?;
    let configuration = AcknowledgementPolicy {
        configuration_generation: u64::try_from(current.epoch.configuration_number)
            .map_err(|_| PgError::Configuration("negative configuration generation".into()))?,
        configuration_id: current.configuration_id.clone(),
        valid,
        write_acknowledgements: if valid {
            u32::try_from(expected.1)
                .map_err(|_| PgError::Configuration("write quorum overflow".into()))?
        } else {
            0
        },
        eligible_standbys,
    };
    configuration.validate().map_err(PgError::Configuration)?;
    Ok(configuration)
}

async fn retire_slot(client: &tokio_postgres::Client, slot: &str) -> Result<(), PgError> {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            // A walreceiver can reconnect after termination. Keep drain/drop in
            // one server round trip and retry that race, never acknowledge it.
            let result = client
                .query(
                    "WITH drained AS MATERIALIZED ( \
                   SELECT slot_name, CASE WHEN active_pid IS NULL THEN true \
                     ELSE pg_terminate_backend(active_pid, 1000) END AS stopped \
                   FROM pg_replication_slots WHERE slot_name = $1::name) \
                 SELECT stopped, CASE WHEN stopped THEN pg_drop_replication_slot(slot_name) END \
                 FROM drained",
                    &[&slot],
                )
                .await;
            match result {
                Ok(rows) if rows.iter().all(|row| row.get::<_, bool>(0)) => return Ok(()),
                Ok(_) => {}
                Err(error)
                    if error.code() == Some(&tokio_postgres::error::SqlState::OBJECT_IN_USE) => {}
                Err(error)
                    if error.code() == Some(&tokio_postgres::error::SqlState::UNDEFINED_OBJECT) =>
                {
                    return Ok(());
                }
                Err(error) => {
                    return Err(PgError::Query(format!(
                        "retire physical slot {slot}: {error:?}"
                    )));
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| PgError::Timeout(format!("retire physical slot {slot}")))?
}

pub(crate) fn replication_slot_name(identity: &ReplicaIdentity) -> String {
    let name = replication_application_name(identity, &ProcessSessionId::new("physical-slot"));
    format!(
        "kuberic_{}",
        name.rsplit_once('_').expect("hashed replication name").1
    )
}

pub fn replication_application_name(
    identity: &ReplicaIdentity,
    session: &ProcessSessionId,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(identity.replica_id.value().to_be_bytes());
    for value in [
        identity.instance_id.as_str(),
        identity.agent_generation.as_str(),
        session.as_str(),
    ] {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value.as_bytes());
    }
    let digest = hasher.finalize();
    let suffix = digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("kr{}_{}", identity.replica_id.value(), suffix)
}

fn requirement(
    configuration: &ConfigurationDescriptor,
    local: &ReplicaIdentity,
    current: bool,
) -> Result<(BTreeSet<ReplicaIdentity>, usize), PgError> {
    if (current && configuration.primary_id != local.replica_id) || configuration.write_quorum == 0
    {
        return Err(PgError::Configuration(
            "synchronous policy requires the exact local primary".into(),
        ));
    }
    if current
        && !configuration.members.iter().any(|member| {
            member.identity == *local
                && member.role == kuberic_runtime::protocol::types::ReplicaRole::Primary
        })
    {
        return Err(PgError::Configuration(
            "synchronous policy primary incarnation differs from the local replica".into(),
        ));
    }
    Ok((
        configuration
            .members
            .iter()
            .filter(|member| member.identity != *local)
            .map(|member| member.identity.clone())
            .collect(),
        (configuration.write_quorum
            - u32::from(configuration.members.iter().any(|m| m.identity == *local)))
            as usize,
    ))
}

async fn certify_primary_progress(
    client: &tokio_postgres::Client,
    local_flush_lsn: i64,
    synchronous: Option<&AcknowledgementPolicy>,
) -> Result<i64, PgError> {
    let Some(synchronous) = synchronous else {
        return Ok(0);
    };
    if !synchronous.valid {
        return Ok(0);
    }
    if synchronous.write_acknowledgements == 0 {
        return Ok(local_flush_lsn);
    }
    let expected = synchronous
        .eligible_standbys
        .iter()
        .map(|standby| {
            (
                replication_application_name(&standby.identity, &standby.process_session_id),
                standby,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let rows = client
        .query(
            "SELECT application_name, replay_lsn::text FROM pg_stat_replication \
             WHERE replay_lsn IS NOT NULL",
            &[],
        )
        .await
        .map_err(|error| PgError::Query(format!("pg_stat_replication: {error}")))?;
    let mut replay = BTreeMap::new();
    for row in rows {
        let name: &str = row.get(0);
        let lsn: &str = row.get(1);
        if expected.contains_key(name)
            && replay
                .insert(name.to_string(), parse_pg_lsn(lsn)?)
                .is_some()
        {
            return Err(PgError::Query(format!(
                "duplicate synchronous application_name {name}"
            )));
        }
    }
    let mut replay = replay.into_values().collect::<Vec<_>>();
    replay.sort_unstable_by(|left, right| right.cmp(left));
    let required = synchronous.write_acknowledgements as usize;
    let certified = replay.get(required - 1).copied().unwrap_or_default();
    Ok(local_flush_lsn.min(certified))
}

#[cfg(test)]
mod tests {
    use kuberic_runtime::protocol::types::{
        AgentGeneration, ConfigurationMember, Epoch, ReplicaId, ReplicaInstanceId, ReplicaRole,
    };

    use super::*;

    fn identity(id: i64) -> ReplicaIdentity {
        ReplicaIdentity {
            replica_id: ReplicaId::new(id),
            instance_id: ReplicaInstanceId::new(format!("pod-{id}")),
            agent_generation: AgentGeneration::new(format!("generation-{id}")),
        }
    }

    fn configuration(
        ids: &[i64],
        primary: i64,
        write_quorum: u32,
        epoch: i64,
    ) -> ConfigurationDescriptor {
        ConfigurationDescriptor::new(
            Epoch::new(0, epoch),
            ReplicaId::new(primary),
            ids.iter()
                .map(|id| ConfigurationMember {
                    identity: identity(*id),
                    role: if *id == primary {
                        ReplicaRole::Primary
                    } else {
                        ReplicaRole::ActiveSecondary
                    },
                })
                .collect(),
            write_quorum,
        )
    }

    fn sessions(ids: &[i64]) -> BTreeMap<ReplicaIdentity, ProcessSessionId> {
        ids.iter()
            .map(|id| {
                (
                    identity(*id),
                    ProcessSessionId::new(format!("session-{id}")),
                )
            })
            .collect()
    }

    #[test]
    fn application_names_are_bounded_and_session_unique() {
        let first = replication_application_name(&identity(2), &ProcessSessionId::new("session-a"));
        let second =
            replication_application_name(&identity(2), &ProcessSessionId::new("session-b"));
        assert!(first.len() <= 63);
        assert_ne!(first, second);
        let mut ambiguous_left = identity(2);
        ambiguous_left.instance_id = ReplicaInstanceId::new("pod:a");
        ambiguous_left.agent_generation = AgentGeneration::new("b");
        let mut ambiguous_right = identity(2);
        ambiguous_right.instance_id = ReplicaInstanceId::new("pod");
        ambiguous_right.agent_generation = AgentGeneration::new("a");
        assert_ne!(
            replication_application_name(&ambiguous_left, &ProcessSessionId::new("c")),
            replication_application_name(&ambiguous_right, &ProcessSessionId::new("b:c"))
        );
    }

    #[test]
    fn zero_secondary_policy_is_identity_and_joint_policy_is_conservative() {
        let singleton = configuration(&[1], 1, 1, 1);
        let expanded = configuration(&[1, 2], 1, 2, 2);
        let compiled = compile_synchronous_configuration(
            Some(&singleton),
            &expanded,
            &identity(1),
            &sessions(&[2]),
            true,
        )
        .unwrap();
        assert_eq!(compiled.write_acknowledgements, 1);
        assert_eq!(compiled.eligible_standbys[0].identity, identity(2));

        let previous = configuration(&[1, 2, 3], 1, 2, 3);
        let current = configuration(&[1, 2, 4], 1, 2, 4);
        let compiled = compile_synchronous_configuration(
            Some(&previous),
            &current,
            &identity(1),
            &sessions(&[2, 3, 4]),
            true,
        )
        .unwrap();
        assert_eq!(compiled.write_acknowledgements, 1);
        assert_eq!(
            compiled
                .eligible_standbys
                .iter()
                .map(|standby| standby.identity.clone())
                .collect::<Vec<_>>(),
            vec![identity(2)]
        );

        let reduced = configuration(&[1], 1, 1, 5);
        let compiled = compile_synchronous_configuration(
            Some(&expanded),
            &reduced,
            &identity(1),
            &sessions(&[2]),
            true,
        )
        .unwrap();
        assert_eq!(compiled.write_acknowledgements, 1);
        assert_eq!(compiled.eligible_standbys[0].identity, identity(2));

        let incompatible_previous = configuration(&[1, 2], 1, 2, 6);
        let incompatible_current = configuration(&[1, 3], 1, 2, 7);
        assert!(
            compile_synchronous_configuration(
                Some(&incompatible_previous),
                &incompatible_current,
                &identity(1),
                &sessions(&[2, 3]),
                true,
            )
            .is_err()
        );
        assert!(
            compile_synchronous_configuration(
                None,
                &expanded,
                &identity(1),
                &BTreeMap::new(),
                true,
            )
            .is_err()
        );

        let mut wrong_primary = identity(1);
        wrong_primary.instance_id = ReplicaInstanceId::new("other-primary");
        let wrong = ConfigurationDescriptor::new(
            Epoch::new(0, 8),
            ReplicaId::new(1),
            vec![
                ConfigurationMember {
                    identity: wrong_primary,
                    role: ReplicaRole::Primary,
                },
                ConfigurationMember {
                    identity: identity(2),
                    role: ReplicaRole::ActiveSecondary,
                },
            ],
            2,
        );
        assert!(
            compile_synchronous_configuration(None, &wrong, &identity(1), &sessions(&[2]), true,)
                .is_err()
        );
    }
}
