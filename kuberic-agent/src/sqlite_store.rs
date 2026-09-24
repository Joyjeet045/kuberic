//! SQLite-backed agent persistence.

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use async_trait::async_trait;
use kuberic_protocol::command::EnsureConfiguration;
use kuberic_protocol::types::{ConfigurationId, Epoch, FaultType, LoadMetric, OperationId};
use kuberic_runtime_internal::authority::{
    AdmittedAuthority, AuthorityFence, BuildAuthority, BuildAuthorityStore, BuildProgressStore,
    DurableBuildProgress, DurableLocalWrite, LocalWriteJournal, LocalWritePhase,
    ReplicaAuthorityStore, ReplicationProgress, ReplicationProgressStore,
};
use kuberic_runtime_internal::effects::{RuntimeEffect, RuntimeEffectResult};
use kuberic_runtime_internal::{ContractError, Result as ContractResult};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, params};

use crate::command::is_access_only_configuration;
use crate::state::{
    AgentState, CoordinatorStage, DeactivationState, EffectStage, PendingEffect,
    ReconfigurationRecord, RetainedCommandResult, RetainedResult, SCHEMA_VERSION, StorageIdentity,
};
use crate::store::{AgentStore, BeginConfiguration, BeginEffect};
use crate::{AgentError, Result};

const DATABASE_FILE: &str = "agent.sqlite3";

pub struct SqliteStore {
    path: PathBuf,
    connection: Mutex<Connection>,
}

impl SqliteStore {
    pub fn metadata_database_path(pvc_root: &Path) -> PathBuf {
        pvc_root.join(".kuberic").join(DATABASE_FILE)
    }

    pub fn create_authorized(path: impl AsRef<Path>, state: AgentState) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if state.identity.schema_version != SCHEMA_VERSION {
            return Err(AgentError::SchemaMismatch {
                expected: SCHEMA_VERSION,
                observed: state.identity.schema_version,
            });
        }
        let parent = path.parent().ok_or_else(|| {
            AgentError::Corrupt("agent database path has no metadata directory".into())
        })?;
        std::fs::create_dir_all(parent)?;
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let result = (|| {
            let mut connection = open_connection(&path)?;
            configure_durability(&connection)?;
            create_schema(&mut connection, &state)?;
            Ok(Self {
                path: path.clone(),
                connection: Mutex::new(connection),
            })
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&path);
        }
        result
    }

    pub fn open_existing(
        path: impl AsRef<Path>,
        expected_identity: Option<&StorageIdentity>,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if !path.is_file() {
            return Err(AgentError::MissingEstablishedStore);
        }
        let connection = open_connection(&path).map_err(|error| {
            AgentError::Corrupt(format!("cannot open SQLite metadata: {error}"))
        })?;
        configure_durability(&connection)
            .map_err(|error| AgentError::Corrupt(error.to_string()))?;
        validate_integrity(&connection).map_err(|error| AgentError::Corrupt(error.to_string()))?;
        let version: u32 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if version != SCHEMA_VERSION {
            return Err(AgentError::SchemaMismatch {
                expected: SCHEMA_VERSION,
                observed: version,
            });
        }
        let state = load_state_from_connection(&connection)?;
        if state.identity.schema_version != version {
            return Err(AgentError::SchemaMismatch {
                expected: version,
                observed: state.identity.schema_version,
            });
        }
        if let Some(expected) = expected_identity
            && &state.identity != expected
        {
            return Err(AgentError::IdentityMismatch(
                "resource, Pod, PVC, replica incarnation, generation, or initialization changed"
                    .into(),
            ));
        }
        Ok(Self {
            path,
            connection: Mutex::new(connection),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn with_transaction<T>(&self, action: impl FnOnce(&Transaction<'_>) -> Result<T>) -> Result<T> {
        let mut connection = self.connection.lock().map_err(|_| {
            AgentError::Corrupt("agent database connection mutex was poisoned".into())
        })?;
        let transaction = connection.transaction()?;
        let result = action(&transaction)?;
        transaction.commit()?;
        Ok(result)
    }

    fn contract_transaction<T>(
        &self,
        action: impl FnOnce(&Transaction<'_>) -> ContractResult<T>,
    ) -> ContractResult<T> {
        let mut connection = self.connection.lock().map_err(|_| {
            ContractError::Persistence("agent database connection mutex was poisoned".into())
        })?;
        let transaction = connection.transaction().map_err(contract_sqlite_error)?;
        let result = action(&transaction)?;
        transaction.commit().map_err(contract_sqlite_error)?;
        Ok(result)
    }
}

#[async_trait]
impl AgentStore for SqliteStore {
    async fn identity(&self) -> Result<StorageIdentity> {
        Ok(self.load_state().await?.identity)
    }

    async fn load_state(&self) -> Result<AgentState> {
        let connection = self.connection.lock().map_err(|_| {
            AgentError::Corrupt("agent database connection mutex was poisoned".into())
        })?;
        load_state_from_connection(&connection)
    }

    async fn begin_effect(&self, effect: &RuntimeEffect) -> Result<BeginEffect> {
        self.with_transaction(|transaction| {
            let mut state = load_state_from_connection(transaction)?;
            if let Some(retained) = state.retained_result.as_ref()
                && retained.operation_id == effect.operation_id
            {
                if retained.effect != *effect {
                    return Err(AgentError::EffectConflict(
                        "operation ID was reused with a different effect".into(),
                    ));
                }
                return Ok(BeginEffect::Completed(Box::new(retained.result.clone())));
            }
            if let Some(pending) = state.pending_effect.as_ref() {
                if pending.effect != *effect {
                    return Err(AgentError::EffectConflict(
                        "another durable effect is pending".into(),
                    ));
                }
                return Ok(BeginEffect::Pending(pending.effect.clone()));
            }
            state.pending_effect = Some(PendingEffect {
                effect: effect.clone(),
                stage: EffectStage::IntentCommitted,
            });
            write_agent_state(transaction, &state)?;
            Ok(BeginEffect::Execute(effect.clone()))
        })
    }

    async fn mark_effect_applied(&self, effect: &RuntimeEffect) -> Result<()> {
        self.with_transaction(|transaction| {
            let mut state = load_state_from_connection(transaction)?;
            let pending = state.pending_effect.as_mut().ok_or_else(|| {
                AgentError::EffectConflict("runtime effect has no durable intent".into())
            })?;
            if pending.effect != *effect {
                return Err(AgentError::EffectConflict(
                    "runtime effect does not match durable intent".into(),
                ));
            }
            pending.stage = EffectStage::EffectApplied;
            write_agent_state(transaction, &state)
        })
    }

    async fn complete_effect(&self, result: &RuntimeEffectResult) -> Result<()> {
        self.with_transaction(|transaction| {
            let mut state = load_state_from_connection(transaction)?;
            let pending = state.pending_effect.take().ok_or_else(|| {
                AgentError::EffectConflict("effect completion has no durable intent".into())
            })?;
            if pending.effect.operation_id != result.operation_id
                || pending.effect.sequence != result.sequence
            {
                return Err(AgentError::EffectConflict(
                    "effect completion does not match durable intent".into(),
                ));
            }
            state.role = result.postcondition.role;
            state.read_status = result.postcondition.read_status;
            state.write_status = result.postcondition.write_status;
            state.previous_configuration = result
                .postcondition
                .authority
                .as_ref()
                .and_then(|authority| authority.previous_configuration.clone());
            state.current_configuration = result
                .postcondition
                .authority
                .as_ref()
                .map(|authority| authority.current_configuration.clone());
            if let Some(configuration) = state.current_configuration.as_ref() {
                state.highest_epoch = state.highest_epoch.max(configuration.epoch);
            }
            state.retained_result = Some(RetainedResult {
                operation_id: result.operation_id.clone(),
                effect: pending.effect,
                result: result.clone(),
            });
            state.next_effect_sequence = state.next_effect_sequence.max(result.sequence + 1);
            write_agent_state(transaction, &state)
        })
    }

    async fn cancel_effect(&self, effect: &RuntimeEffect) -> Result<()> {
        self.with_transaction(|transaction| {
            let mut state = load_state_from_connection(transaction)?;
            let pending = state.pending_effect.take().ok_or_else(|| {
                AgentError::EffectConflict("runtime effect has no durable intent".into())
            })?;
            if pending.effect != *effect {
                return Err(AgentError::EffectConflict(
                    "cancelled runtime effect does not match durable intent".into(),
                ));
            }
            write_agent_state(transaction, &state)
        })
    }

    async fn begin_configuration(
        &self,
        command: &EnsureConfiguration,
    ) -> Result<BeginConfiguration> {
        self.with_transaction(|transaction| {
            let mut state = load_state_from_connection(transaction)?;
            if let Some(retained) = state.retained_command.as_ref()
                && retained.command.operation_id == command.operation_id
            {
                if retained.command != *command {
                    return Err(AgentError::EffectConflict(
                        "operation ID was reused with different configuration authority".into(),
                    ));
                }
                return Ok(BeginConfiguration::Completed(retained.clone()));
            }
            if let Some(pending) = state.reconfiguration.as_ref() {
                if pending.command != *command {
                    if command.current_epoch <= pending.command.current_epoch {
                        return Err(AgentError::EffectConflict(
                            "another configuration command is pending".into(),
                        ));
                    }
                    if let Some(effect) = state.pending_effect.as_ref()
                        && !effect
                            .effect
                            .operation_id
                            .as_str()
                            .starts_with(&format!("{}:", pending.command.operation_id.as_str()))
                    {
                        return Err(AgentError::EffectConflict(
                            "pending runtime effect is not owned by the superseded command".into(),
                        ));
                    }
                    state.pending_effect = None;
                    let record = ReconfigurationRecord {
                        command: command.clone(),
                        stage: CoordinatorStage::AdmitAuthority,
                        observed_lsn: None,
                    };
                    state.reconfiguration = Some(record.clone());
                    write_agent_state(transaction, &state)?;
                    return Ok(BeginConfiguration::Superseded(record));
                }
                return Ok(BeginConfiguration::Pending(pending.clone()));
            }
            let record = ReconfigurationRecord {
                command: command.clone(),
                stage: if is_access_only_configuration(command, &state) {
                    CoordinatorStage::Activate
                } else {
                    CoordinatorStage::AdmitAuthority
                },
                observed_lsn: None,
            };
            state.reconfiguration = Some(record.clone());
            write_agent_state(transaction, &state)?;
            Ok(BeginConfiguration::Execute(record))
        })
    }

    async fn advance_configuration(
        &self,
        operation_id: &OperationId,
        expected: CoordinatorStage,
        next: CoordinatorStage,
        observed_lsn: Option<i64>,
    ) -> Result<ReconfigurationRecord> {
        self.with_transaction(|transaction| {
            let mut state = load_state_from_connection(transaction)?;
            let record = state.reconfiguration.as_mut().ok_or_else(|| {
                AgentError::EffectConflict("configuration command is not pending".into())
            })?;
            if &record.command.operation_id != operation_id || record.stage != expected {
                return Err(AgentError::EffectConflict(
                    "configuration stage does not match durable command".into(),
                ));
            }
            record.stage = next;
            if observed_lsn.is_some() {
                record.observed_lsn = observed_lsn;
            }
            let result = record.clone();
            if expected == CoordinatorStage::Deactivate {
                state.deactivation = Some(DeactivationState {
                    epoch: result.command.current_epoch,
                    deactivated_lsn: result.observed_lsn.unwrap_or(0),
                });
            }
            write_agent_state(transaction, &state)?;
            Ok(result)
        })
    }

    async fn complete_configuration(
        &self,
        operation_id: &OperationId,
    ) -> Result<RetainedCommandResult> {
        self.with_transaction(|transaction| {
            let mut state = load_state_from_connection(transaction)?;
            let record = state.reconfiguration.take().ok_or_else(|| {
                AgentError::EffectConflict("configuration command is not pending".into())
            })?;
            if &record.command.operation_id != operation_id
                || record.stage != CoordinatorStage::Complete
            {
                return Err(AgentError::EffectConflict(
                    "configuration command has not reached its terminal stage".into(),
                ));
            }
            let result = RetainedCommandResult {
                command: record.command,
                role: state.role,
                epoch: state.highest_epoch,
            };
            state.retained_command = Some(result.clone());
            write_agent_state(transaction, &state)?;
            Ok(result)
        })
    }

    async fn retained_result(&self) -> Result<Option<RetainedResult>> {
        Ok(self.load_state().await?.retained_result)
    }

    async fn set_reconfiguration(&self, data: Option<String>) -> Result<()> {
        self.with_transaction(|transaction| {
            let mut state = load_state_from_connection(transaction)?;
            state.reconfiguration_data = data;
            write_agent_state(transaction, &state)
        })
    }

    async fn clear_reconfiguration(&self) -> Result<()> {
        self.with_transaction(|transaction| {
            let mut state = load_state_from_connection(transaction)?;
            state.reconfiguration_data = None;
            write_agent_state(transaction, &state)
        })
    }

    async fn migrate_schema(&self, expected_version: u32, target_version: u32) -> Result<()> {
        self.with_transaction(|transaction| {
            let observed: u32 =
                transaction.query_row("PRAGMA user_version", [], |row| row.get(0))?;
            if observed != expected_version
                || expected_version != SCHEMA_VERSION
                || target_version != SCHEMA_VERSION
            {
                return Err(AgentError::SchemaMismatch {
                    expected: target_version,
                    observed,
                });
            }
            transaction.execute(
                "INSERT INTO schema_migrations(version) VALUES(?1)
                 ON CONFLICT(version) DO NOTHING",
                [target_version],
            )?;
            Ok(())
        })
    }

    async fn record_partition_reports(
        &self,
        load_metrics: Vec<LoadMetric>,
        reported_fault: Option<FaultType>,
    ) -> Result<()> {
        self.with_transaction(|transaction| {
            let mut names = std::collections::BTreeSet::new();
            if load_metrics.iter().any(|metric| {
                metric.name.is_empty() || metric.value < 0 || !names.insert(metric.name.clone())
            }) {
                return Err(AgentError::CommandRejected(
                    "load metrics require unique nonempty names and nonnegative values".into(),
                ));
            }
            let mut state = load_state_from_connection(transaction)?;
            state.load_metrics = load_metrics;
            state.reported_fault = reported_fault;
            write_agent_state(transaction, &state)
        })
    }
}

#[async_trait]
impl ReplicaAuthorityStore for SqliteStore {
    async fn load(&self) -> ContractResult<Option<AdmittedAuthority>> {
        let connection = self.connection.lock().map_err(|_| {
            ContractError::Persistence("agent database connection mutex was poisoned".into())
        })?;
        load_json_optional(
            &connection,
            "SELECT authority_json FROM replica_authority WHERE singleton = 1",
            [],
        )
    }

    async fn admit(&self, authority: &AdmittedAuthority) -> ContractResult<()> {
        authority.validate()?;
        self.contract_transaction(|transaction| {
            let existing: Option<AdmittedAuthority> = load_json_optional(
                transaction,
                "SELECT authority_json FROM replica_authority WHERE singleton = 1",
                [],
            )?;
            if let Some(existing) = existing {
                if existing == *authority {
                    return Ok(());
                }
                if authority.current_configuration.epoch < existing.current_configuration.epoch {
                    return Err(ContractError::AuthorityMismatch(
                        "replica authority epoch regressed".into(),
                    ));
                }
                if authority.current_configuration.epoch == existing.current_configuration.epoch
                    && existing != *authority
                    && !authority.is_current_only_completion_of(&existing)
                {
                    return Err(ContractError::AuthorityMismatch(
                        "replica authority changed without a newer epoch".into(),
                    ));
                }
            }
            let json = contract_json(authority)?;
            transaction
                .execute(
                    "INSERT INTO replica_authority(singleton, authority_json) VALUES(1, ?1)
                     ON CONFLICT(singleton) DO UPDATE SET authority_json = excluded.authority_json",
                    [json],
                )
                .map_err(contract_sqlite_error)?;
            Ok(())
        })
    }
}

#[async_trait]
impl ReplicationProgressStore for SqliteStore {
    async fn load_replication_progress(
        &self,
        fence: &AuthorityFence,
    ) -> ContractResult<Option<ReplicationProgress>> {
        let connection = self.connection.lock().map_err(|_| {
            ContractError::Persistence("agent database connection mutex was poisoned".into())
        })?;
        let fence_json = contract_json(fence)?;
        load_json_optional(
            &connection,
            "SELECT progress_json FROM replication_progress WHERE fence_json = ?1",
            [fence_json],
        )
    }

    async fn load_configuration_progress(
        &self,
        epoch: Epoch,
        current_configuration_id: &ConfigurationId,
    ) -> ContractResult<Option<ReplicationProgress>> {
        let connection = self.connection.lock().map_err(|_| {
            ContractError::Persistence("agent database connection mutex was poisoned".into())
        })?;
        let mut statement = connection
            .prepare(
                "SELECT progress_json FROM replication_progress
                 WHERE data_loss_number = ?1 AND configuration_number = ?2
                   AND current_configuration_id = ?3
                 ORDER BY verified_lsn DESC LIMIT 1",
            )
            .map_err(contract_sqlite_error)?;
        let json: Option<String> = statement
            .query_row(
                params![
                    epoch.data_loss_number,
                    epoch.configuration_number,
                    current_configuration_id.as_str()
                ],
                |row| row.get(0),
            )
            .optional()
            .map_err(contract_sqlite_error)?;
        json.map(|value| contract_from_json(&value)).transpose()
    }

    async fn record_replication_progress(
        &self,
        progress: &ReplicationProgress,
    ) -> ContractResult<()> {
        self.contract_transaction(|transaction| {
            let fence_json = contract_json(&progress.fence)?;
            let existing: Option<ReplicationProgress> = load_json_optional(
                transaction,
                "SELECT progress_json FROM replication_progress WHERE fence_json = ?1",
                [fence_json.clone()],
            )?;
            if existing
                .as_ref()
                .is_some_and(|existing| existing.verified_lsn > progress.verified_lsn)
            {
                return Err(ContractError::AuthorityMismatch(
                    "replication progress regressed".into(),
                ));
            }
            let progress_json = contract_json(progress)?;
            transaction
                .execute(
                    "INSERT INTO replication_progress(
                        fence_json, data_loss_number, configuration_number,
                        current_configuration_id, verified_lsn, progress_json
                     ) VALUES(?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT(fence_json) DO UPDATE SET
                        verified_lsn = excluded.verified_lsn,
                        progress_json = excluded.progress_json",
                    params![
                        fence_json,
                        progress.fence.epoch.data_loss_number,
                        progress.fence.epoch.configuration_number,
                        progress.fence.current_configuration_id.as_str(),
                        progress.verified_lsn,
                        progress_json
                    ],
                )
                .map_err(contract_sqlite_error)?;
            Ok(())
        })
    }
}

#[async_trait]
impl LocalWriteJournal for SqliteStore {
    async fn load_local_write(
        &self,
        operation_id: &OperationId,
    ) -> ContractResult<Option<DurableLocalWrite>> {
        let connection = self.connection.lock().map_err(|_| {
            ContractError::Persistence("agent database connection mutex was poisoned".into())
        })?;
        load_json_optional(
            &connection,
            "SELECT write_json FROM local_writes WHERE operation_id = ?1",
            [operation_id.as_str()],
        )
    }

    async fn load_local_writes(&self) -> ContractResult<Vec<DurableLocalWrite>> {
        let connection = self.connection.lock().map_err(|_| {
            ContractError::Persistence("agent database connection mutex was poisoned".into())
        })?;
        let mut statement = connection
            .prepare(
                "SELECT write_json FROM local_writes WHERE phase != ?1 ORDER BY lsn, operation_id",
            )
            .map_err(contract_sqlite_error)?;
        let rows = statement
            .query_map([phase_tag(LocalWritePhase::Committed)], |row| {
                row.get::<_, String>(0)
            })
            .map_err(contract_sqlite_error)?;
        rows.map(|row| {
            let json = row.map_err(contract_sqlite_error)?;
            contract_from_json(&json)
        })
        .collect()
    }

    async fn record_local_write(&self, write: &DurableLocalWrite) -> ContractResult<()> {
        self.contract_transaction(|transaction| {
            let existing: Option<DurableLocalWrite> = load_json_optional(
                transaction,
                "SELECT write_json FROM local_writes WHERE operation_id = ?1",
                [write.operation_id.as_str()],
            )?;
            if let Some(existing) = existing {
                if existing.lsn != write.lsn
                    || existing.committed_lsn != write.committed_lsn
                    || existing.data != write.data
                {
                    return Err(ContractError::AuthorityMismatch(
                        "local write identity or payload changed".into(),
                    ));
                }
                if phase_tag(write.phase) < phase_tag(existing.phase) {
                    return Err(ContractError::AuthorityMismatch(
                        "local write phase regressed".into(),
                    ));
                }
            }
            let json = contract_json(write)?;
            transaction
                .execute(
                    "INSERT INTO local_writes(operation_id, lsn, phase, write_json)
                     VALUES(?1, ?2, ?3, ?4)
                     ON CONFLICT(operation_id) DO UPDATE SET
                        lsn = excluded.lsn,
                        phase = excluded.phase,
                        write_json = excluded.write_json",
                    params![
                        write.operation_id.as_str(),
                        write.lsn,
                        phase_tag(write.phase),
                        json
                    ],
                )
                .map_err(contract_sqlite_error)?;
            Ok(())
        })
    }

    async fn reset_local_writes_after_data_loss(&self, committed_lsn: i64) -> ContractResult<()> {
        self.contract_transaction(|transaction| {
            transaction
                .execute(
                    "DELETE FROM local_writes WHERE phase != ?1 OR lsn > ?2",
                    params![phase_tag(LocalWritePhase::Committed), committed_lsn],
                )
                .map_err(contract_sqlite_error)?;
            Ok(())
        })
    }
}

#[async_trait]
impl BuildAuthorityStore for SqliteStore {
    async fn load_build(&self, build_id: &OperationId) -> ContractResult<Option<BuildAuthority>> {
        let connection = self.connection.lock().map_err(|_| {
            ContractError::Persistence("agent database connection mutex was poisoned".into())
        })?;
        load_json_optional(
            &connection,
            "SELECT authority_json FROM build_authority WHERE build_id = ?1",
            [build_id.as_str()],
        )
    }

    async fn admit_build(&self, authority: &BuildAuthority) -> ContractResult<()> {
        authority
            .validate()
            .map_err(|error| ContractError::AuthorityMismatch(error.to_string()))?;
        self.contract_transaction(|transaction| {
            let existing: Option<BuildAuthority> = load_json_optional(
                transaction,
                "SELECT authority_json FROM build_authority WHERE build_id = ?1",
                [authority.build_id.as_str()],
            )?;
            if let Some(existing) = existing
                && existing != *authority
            {
                return Err(ContractError::AuthorityMismatch(
                    "build ID was reused with different authority".into(),
                ));
            }
            let json = contract_json(authority)?;
            transaction
                .execute(
                    "INSERT INTO build_authority(build_id, authority_json) VALUES(?1, ?2)
                     ON CONFLICT(build_id) DO NOTHING",
                    params![authority.build_id.as_str(), json],
                )
                .map_err(contract_sqlite_error)?;
            Ok(())
        })
    }
}

#[async_trait]
impl BuildProgressStore for SqliteStore {
    async fn load_build_progress(
        &self,
        build_id: &OperationId,
    ) -> ContractResult<Option<DurableBuildProgress>> {
        let connection = self.connection.lock().map_err(|_| {
            ContractError::Persistence("agent database connection mutex was poisoned".into())
        })?;
        load_json_optional(
            &connection,
            "SELECT progress_json FROM build_progress WHERE build_id = ?1",
            [build_id.as_str()],
        )
    }

    async fn record_build_progress(&self, progress: &DurableBuildProgress) -> ContractResult<()> {
        self.contract_transaction(|transaction| {
            let authority: Option<BuildAuthority> = load_json_optional(
                transaction,
                "SELECT authority_json FROM build_authority WHERE build_id = ?1",
                [progress.authority.build_id.as_str()],
            )?;
            if authority.as_ref() != Some(&progress.authority) {
                return Err(ContractError::AuthorityMismatch(
                    "build progress lacks matching durable authorization".into(),
                ));
            }
            let existing: Option<DurableBuildProgress> = load_json_optional(
                transaction,
                "SELECT progress_json FROM build_progress WHERE build_id = ?1",
                [progress.authority.build_id.as_str()],
            )?;
            if existing.as_ref().is_some_and(|existing| {
                progress.last_sequence < existing.last_sequence
                    || progress.durable_lsn < existing.durable_lsn
                    || (existing.completed && !progress.completed)
            }) {
                return Err(ContractError::AuthorityMismatch(
                    "build progress regressed".into(),
                ));
            }
            let json = contract_json(progress)?;
            transaction
                .execute(
                    "INSERT INTO build_progress(build_id, progress_json) VALUES(?1, ?2)
                     ON CONFLICT(build_id) DO UPDATE SET progress_json = excluded.progress_json",
                    params![progress.authority.build_id.as_str(), json],
                )
                .map_err(contract_sqlite_error)?;
            Ok(())
        })
    }
}

fn open_connection(path: &Path) -> Result<Connection> {
    Ok(Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?)
}

fn configure_durability(connection: &Connection) -> Result<()> {
    let journal_mode: String =
        connection.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(AgentError::Corrupt(format!(
            "SQLite refused WAL journaling and selected {journal_mode}"
        )));
    }
    connection.execute_batch(
        "PRAGMA synchronous = FULL;
         PRAGMA foreign_keys = ON;
         PRAGMA wal_autocheckpoint = 0;
         PRAGMA busy_timeout = 5000;",
    )?;
    let synchronous: i64 = connection.query_row("PRAGMA synchronous", [], |row| row.get(0))?;
    if synchronous != 2 {
        return Err(AgentError::Corrupt(
            "SQLite synchronous mode is not FULL".into(),
        ));
    }
    Ok(())
}

fn create_schema(connection: &mut Connection, state: &AgentState) -> Result<()> {
    let transaction = connection.transaction()?;
    transaction.execute_batch(
        "CREATE TABLE agent_state (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            state_json TEXT NOT NULL
         );
         CREATE TABLE schema_migrations (
            version INTEGER PRIMARY KEY
         );
         CREATE TABLE replica_authority (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            authority_json TEXT NOT NULL
         );
         CREATE TABLE replication_progress (
            fence_json TEXT PRIMARY KEY,
            data_loss_number INTEGER NOT NULL,
            configuration_number INTEGER NOT NULL,
            current_configuration_id TEXT NOT NULL,
            verified_lsn INTEGER NOT NULL,
            progress_json TEXT NOT NULL
         );
         CREATE INDEX replication_progress_configuration
           ON replication_progress(
             data_loss_number, configuration_number, current_configuration_id, verified_lsn
           );
         CREATE TABLE local_writes (
            operation_id TEXT PRIMARY KEY,
            lsn INTEGER NOT NULL,
            phase INTEGER NOT NULL,
            write_json TEXT NOT NULL
         );
         CREATE TABLE build_authority (
            build_id TEXT PRIMARY KEY,
            authority_json TEXT NOT NULL
         );
         CREATE TABLE build_progress (
            build_id TEXT PRIMARY KEY REFERENCES build_authority(build_id),
            progress_json TEXT NOT NULL
         );",
    )?;
    transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    transaction.execute(
        "INSERT INTO schema_migrations(version) VALUES(?1)",
        [SCHEMA_VERSION],
    )?;
    write_agent_state(&transaction, state)?;
    transaction.commit()?;
    Ok(())
}

fn validate_integrity(connection: &Connection) -> Result<()> {
    let result: String = connection.query_row("PRAGMA quick_check(1)", [], |row| row.get(0))?;
    if result != "ok" {
        return Err(AgentError::Corrupt(result));
    }
    Ok(())
}

fn load_state_from_connection(connection: &Connection) -> Result<AgentState> {
    let json: String = connection.query_row(
        "SELECT state_json FROM agent_state WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?;
    Ok(serde_json::from_str(&json)?)
}

fn write_agent_state(transaction: &Transaction<'_>, state: &AgentState) -> Result<()> {
    let json = serde_json::to_string(state)?;
    transaction.execute(
        "INSERT INTO agent_state(singleton, state_json) VALUES(1, ?1)
         ON CONFLICT(singleton) DO UPDATE SET state_json = excluded.state_json",
        [json],
    )?;
    Ok(())
}

fn load_json_optional<T, P>(
    connection: &Connection,
    sql: &str,
    params: P,
) -> ContractResult<Option<T>>
where
    T: serde::de::DeserializeOwned,
    P: rusqlite::Params,
{
    let json: Option<String> = connection
        .query_row(sql, params, |row| row.get(0))
        .optional()
        .map_err(contract_sqlite_error)?;
    json.map(|value| contract_from_json(&value)).transpose()
}

fn contract_json<T: serde::Serialize>(value: &T) -> ContractResult<String> {
    serde_json::to_string(value).map_err(|error| ContractError::Persistence(error.to_string()))
}

fn contract_from_json<T: serde::de::DeserializeOwned>(value: &str) -> ContractResult<T> {
    serde_json::from_str(value).map_err(|error| ContractError::Persistence(error.to_string()))
}

fn contract_sqlite_error(error: rusqlite::Error) -> ContractError {
    ContractError::Persistence(error.to_string())
}

fn phase_tag(phase: LocalWritePhase) -> i64 {
    match phase {
        LocalWritePhase::Reserved => 0,
        LocalWritePhase::Registered => 1,
        LocalWritePhase::Committed => 2,
    }
}
